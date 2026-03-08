import socket
import sys
import select
import csv
import time

def is_prime(num):
    """Helper function to check if a number is prime."""
    if num <= 1:
        return False
    if num <= 3:
        return True
    if num % 2 == 0 or num % 3 == 0:
        return False
    i = 5
    while i * i <= num:
        if num % i == 0 or num % (i + 2) == 0:
            return False
        i += 6
    return True

def get_table_size(l):
    # The hash table size, s, is the first prime number larger than 2 * l [cite: 184]
    s = (2 * l) + 1
    while not is_prime(s):
        s += 1
    return s

def main():
    # Peer should read two command line parameters: the IPv4 address of the manager and the port number 
    if len(sys.argv) != 3:
        print("Usage: python peer.py <manager_ip> <manager_port>")
        sys.exit(1)

    manager_ip = sys.argv[1]
    manager_port = int(sys.argv[2])
    manager_address = (manager_ip, manager_port)

    # A peer reads in a command and constructs a message to be sent over a UDP socket [cite: 73]
    m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    p_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    peer_name = ""
    my_id = -1
    ring_size = 0
    right_neighbor = None 
    local_hash_table = {}
    records_stored_count = 0
    is_registered = False

    print("Peer started. Type commands below:")

    while True:
        sockets_to_watch = [sys.stdin]
        if is_registered:
            sockets_to_watch.extend([m_socket, p_socket])

        ready_to_read, _, _ = select.select(sockets_to_watch, [], [])

        for sock in ready_to_read:
            # 1. HANDLE KEYBOARD INPUT (Commands from the user) [cite: 72]
            if sock == sys.stdin:
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
                        
                        m_socket.bind((my_ip, m_port))
                        p_socket.bind((my_ip, p_port))
                        is_registered = True
                        
                        m_socket.sendto(command.encode('utf-8'), manager_address)
                    else:
                        print("Invalid register format.")
                
                elif cmd_type in ["setup-dht", "query-dht", "leave-dht", "join-dht", "teardown-dht", "deregister"]:
                    m_socket.sendto(command.encode('utf-8'), manager_address)
                    
            # 2. HANDLE MESSAGES FROM THE MANAGER (m-port) [cite: 80]
            elif sock == m_socket:
                message, _ = m_socket.recvfrom(4096)
                decoded_msg = message.decode('utf-8')
                print(f"Manager response: {decoded_msg}")
                
                parts = decoded_msg.split()
                if not parts:
                    continue
                    
                if parts[0] == "SUCCESS" and len(parts) > 1 and parts[1].startswith(peer_name):
                    print("I am the Leader! Initiating ring setup and reading dataset...")
                    
                    peer_tuples = parts[1:]
                    n = len(peer_tuples)
                    ring_size = n
                    my_id = 0 # The leader has identifier 0 [cite: 141, 146]
                    
                    if n > 1:
                        neighbor_info = peer_tuples[1].split(',')
                        right_neighbor = (neighbor_info[1], int(neighbor_info[2]))

                    tuples_str = " ".join(peer_tuples)
                    for i in range(1, n):
                        target_info = peer_tuples[i].split(',')
                        target_ip = target_info[1]
                        target_port = int(target_info[2])
                        
                        # The leader sends a command set-id to peer_i [cite: 147]
                        set_id_msg = f"set-id {i} {n} {tuples_str}"
                        p_socket.sendto(set_id_msg.encode('utf-8'), (target_ip, target_port))

                    file_name = "details-1950.csv" 
                    try:
                        with open(file_name, mode='r', encoding='utf-8') as file:
                            csv_reader = csv.reader(file)
                            next(csv_reader) # The first line contains the field names and should be skipped [cite: 157]
                            records = list(csv_reader)
                            l = len(records) # Compute the number of storm events, l [cite: 178]
                            s = get_table_size(l) 
                            
                            for record in records:
                                event_id = int(record[0])
                                pos = event_id % s # First hash function computes a position [cite: 183]
                                peer_id = pos % n  # Second hash function is the position modulo the ring size n [cite: 185]
                                
                                record_string = ",".join(record)
                                store_msg = f"store {peer_id} {pos} {record_string}"
                                
                                if peer_id == 0:
                                    local_hash_table[pos] = record_string
                                    records_stored_count += 1
                                else:
                                    # The leader sends a store command to its right neighbour on the ring [cite: 191]
                                    if right_neighbor:
                                        p_socket.sendto(store_msg.encode('utf-8'), right_neighbor)
                                        
                        # Wait for the network to finish routing
                        time.sleep(2)
                        
                        # The leader outputs the number of records stored at each node [cite: 197]
                        print(f"DHT Setup Complete. I am storing {records_stored_count} records.")
                        # Sends a message for the dht-complete command to the manager [cite: 197]
                        complete_msg = f"dht-complete {peer_name}"
                        m_socket.sendto(complete_msg.encode('utf-8'), manager_address)
                        
                    except FileNotFoundError:
                        print(f"Error: '{file_name}' not found.")

            # 3. HANDLE MESSAGES FROM OTHER PEERS (p-port) [cite: 80]
            elif sock == p_socket:
                message, sender_address = p_socket.recvfrom(4096)
                decoded_msg = message.decode('utf-8')
                parts = decoded_msg.split(maxsplit=3)
                
                if not parts:
                    continue
                    
                cmd = parts[0]
                
                if cmd == "set-id":
                    my_id = int(parts[1]) # On receipt of a set-id, peer_i sets its identifier to i [cite: 149]
                    ring_size = int(parts[2]) # and the ring size to n [cite: 149]
                    
                    peer_tuples = parts[3:]
                    neighbor_index = (my_id + 1) % ring_size
                    neighbor_info = peer_tuples[neighbor_index].split(',')
                    
                    # It stores the 3-tuple of peer_(i+1) mod n to use as the address of its right neighbour [cite: 150]
                    right_neighbor = (neighbor_info[1], int(neighbor_info[2]))
                    
                    print(f"Ring setup: I am node {my_id} of {ring_size}.")
                    print(f"My right neighbor is {neighbor_info[0]} at {right_neighbor[0]}:{right_neighbor[1]}")
                    
                elif cmd == "store":
                    target_id = int(parts[1])
                    pos = int(parts[2])
                    record_data = parts[3]
                    
                    if target_id == my_id:
                        # Where it is stored in the local hash table [cite: 192]
                        local_hash_table[pos] = record_data
                        records_stored_count += 1
                    else:
                        if right_neighbor:
                            p_socket.sendto(message, right_neighbor)

if __name__ == "__main__":
    main()