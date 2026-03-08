import socket
import sys
import threading
import csv
import time

# math stuff for the hash table
def is_prime(num):
    # checks if a number is prime
    if num <= 1: return False
    if num <= 3: return True
    if num % 2 == 0 or num % 3 == 0: return False
    i = 5
    while i * i <= num:
        if num % i == 0 or num % (i + 2) == 0: return False
        i += 6
    return True

def get_table_size(l):
    # getting the table size s, needs to be the next prime number after 2 * l
    s = (2 * l) + 1
    while not is_prime(s):
        s += 1
    return s

# global variables so the threads can share data
peer_name = ""
my_id = -1
ring_size = 0
right_neighbor = None 
local_hash_table = {}
records_stored_count = 0
manager_address = None
m_socket = None
p_socket = None

# thread 1: listening to the manager
def listen_manager():
    global peer_name, my_id, ring_size, right_neighbor, records_stored_count, local_hash_table
    while True:
        try:
            message, _ = m_socket.recvfrom(4096)
            decoded_msg = message.decode('utf-8')
            
            # keep the terminal looking clean with the > prompt
            print(f"\n[Manager]: {decoded_msg}\n> ", end="") 
            
            parts = decoded_msg.split()
            if not parts:
                continue
                
            # if we get success and it has our name, we are the leader
            if parts[0] == "SUCCESS" and len(parts) > 1 and parts[1].startswith(peer_name):
                print(f"\nI am the Leader! Initiating ring setup and reading dataset...")
                
                # grab the peers and set my id to 0
                peer_tuples = parts[1:]
                n = len(peer_tuples)
                ring_size = n
                my_id = 0 
                
                # set my right neighbor
                if n > 1:
                    neighbor_info = peer_tuples[1].split(',')
                    right_neighbor = (neighbor_info[1], int(neighbor_info[2]))

                # tell everyone else their id and the ring info
                tuples_str = " ".join(peer_tuples)
                for i in range(1, n):
                    target_info = peer_tuples[i].split(',')
                    target_ip = target_info[1]
                    target_port = int(target_info[2])
                    
                    set_id_msg = f"set-id {i} {n} {tuples_str}"
                    p_socket.sendto(set_id_msg.encode('utf-8'), (target_ip, target_port))

                # open the 1950 data
                file_name = "details-1950.csv" 
                try:
                    with open(file_name, mode='r', encoding='utf-8') as file:
                        csv_reader = csv.reader(file)
                        next(csv_reader) # skip the header
                        records = list(csv_reader)
                        
                        # get l and s
                        l = len(records) 
                        s = get_table_size(l) 
                        
                        for record in records:
                            event_id = int(record[0])
                            
                            # project formulas for pos and id
                            pos = event_id % s 
                            peer_id = pos % n  
                            
                            record_string = ",".join(record)
                            store_msg = f"store {peer_id} {pos} {record_string}"
                            
                            # if it's mine, save it. otherwise send it to the right
                            if peer_id == 0:
                                local_hash_table[pos] = record_string
                                records_stored_count += 1
                            else:
                                if right_neighbor:
                                    p_socket.sendto(store_msg.encode('utf-8'), right_neighbor)
                                    
                    # wait a sec for the network to finish passing stuff around
                    time.sleep(2)
                    
                    # let the manager know we finished
                    print(f"\nDHT Setup Complete. I am storing {records_stored_count} records.\n> ", end="")
                    complete_msg = f"dht-complete {peer_name}"
                    m_socket.sendto(complete_msg.encode('utf-8'), manager_address)
                    
                except FileNotFoundError:
                    print(f"\nError: '{file_name}' not found.\n> ", end="")
        except Exception:
            pass

# thread 2: listening to other peers
def listen_peers():
    global my_id, ring_size, right_neighbor, local_hash_table, records_stored_count
    while True:
        try:
            message, sender_address = p_socket.recvfrom(4096)
            decoded_msg = message.decode('utf-8')
            parts = decoded_msg.split(maxsplit=3)
            
            if not parts:
                continue
                
            cmd = parts[0]
            
            # someone sent us our id layout
            if cmd == "set-id":
                my_id = int(parts[1]) 
                ring_size = int(parts[2]) 
                
                # find right neighbor using modulo
                peer_tuples = parts[3:]
                neighbor_index = (my_id + 1) % ring_size
                neighbor_info = peer_tuples[neighbor_index].split(',')
                right_neighbor = (neighbor_info[1], int(neighbor_info[2]))
                
                print(f"\nRing setup: I am node {my_id} of {ring_size}.")
                print(f"My right neighbor is {neighbor_info[0]} at {right_neighbor[0]}:{right_neighbor[1]}\n> ", end="")
                
            # someone sent us data to store
            elif cmd == "store":
                target_id = int(parts[1])
                pos = int(parts[2])
                record_data = parts[3]
                
                # belongs to me, save it
                if target_id == my_id:
                    local_hash_table[pos] = record_data
                    records_stored_count += 1
                else:
                    # not mine, forward it to the right
                    if right_neighbor:
                        p_socket.sendto(message, right_neighbor)
        except Exception:
            pass

# main code
def main():
    global peer_name, manager_address, m_socket, p_socket
    
    # need manager ip and port
    if len(sys.argv) != 3:
        print("Usage: python peer.py <manager_ip> <manager_port>")
        sys.exit(1)

    manager_address = (sys.argv[1], int(sys.argv[2]))
    m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    p_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    is_registered = False
    print("Peer started. Type commands below:")

    # main loop reading keyboard input
    while True:
        command = sys.stdin.readline().strip()
        if not command:
            continue
        
        parts = command.split()
        cmd_type = parts[0]

        if cmd_type == "register" and not is_registered:
            if len(parts) == 5:
                peer_name = parts[1]
                my_ip = parts[2]
                m_port = int(parts[3])
                p_port = int(parts[4])
                
                # bind the sockets
                m_socket.bind((my_ip, m_port))
                p_socket.bind((my_ip, p_port))
                is_registered = True
                
                # start the threads now that sockets are good
                threading.Thread(target=listen_manager, daemon=True).start()
                threading.Thread(target=listen_peers, daemon=True).start()
                
                m_socket.sendto(command.encode('utf-8'), manager_address)
            else:
                print("Invalid register format.")
        
        # pass all these straight to the manager
        elif cmd_type in ["setup-dht", "query-dht", "leave-dht", "join-dht", "teardown-dht", "deregister"]:
            if is_registered:
                m_socket.sendto(command.encode('utf-8'), manager_address)
            else:
                print("Please register first.")

if __name__ == "__main__":
    main()