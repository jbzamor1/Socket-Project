import socket
import sys
import select
import csv
import time

def is_prime(num):
    # Just a math helper to see if a number is prime
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
    # Getting the table size (s) using the formula from the project [cite: 184]
    s = (2 * l) + 1
    while not is_prime(s):
        s += 1
    return s

def main():
    # We need the manager's IP and port to start [cite: 60]
    if len(sys.argv) != 3:
        print("Usage: python peer.py <manager_ip> <manager_port>")
        sys.exit(1)

    manager_ip = sys.argv[1]
    manager_port = int(sys.argv[2])
    manager_address = (manager_ip, manager_port)

    # Setup our two sockets (one for manager, one for peers)
    m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    p_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    # Variables to remember who we are and our spot in the ring
    peer_name = ""
    my_id = -1
    ring_size = 0
    right_neighbor = None 
    local_hash_table = {}
    records_stored_count = 0
    is_registered = False

    print("Peer started. Type commands below:")

    # Use select so we can listen to the keyboard and the network at the same time
    while True:
        sockets_to_watch = [sys.stdin]
        if is_registered:
            sockets_to_watch.extend([m_socket, p_socket])

        ready_to_read, _, _ = select.select(sockets_to_watch, [], [])

        for sock in ready_to_read:
            # 1. Handle stuff typed in the terminal
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
                        
                        # Bind our sockets to the ports we picked
                        m_socket.bind((my_ip, m_port))
                        p_socket.bind((my_ip, p_port))
                        is_registered = True
                        
                        # Send the command to the manager
                        m_socket.sendto(command.encode('utf-8'), manager_address)
                    else:
                        print("Invalid register format.")
                
                elif cmd_type in ["setup-dht", "query-dht", "leave-dht", "join-dht", "teardown-dht", "deregister"]:
                    # Forward these other commands straight to the manager
                    m_socket.sendto(command.encode('utf-8'), manager_address)
                    
            # 2. Handle messages coming back from the manager
            elif sock == m_socket:
                message, _ = m_socket.recvfrom(4096)
                decoded_msg = message.decode('utf-8')
                print(f"Manager response: {decoded_msg}")
                
                parts = decoded_msg.split()
                if not parts:
                    continue
                    
                # If the manager says SUCCESS to our setup, that means we are the leader!
                if parts[0] == "SUCCESS" and len(parts) > 1 and parts[1].startswith(peer_name):
                    print("I am the Leader! Initiating ring setup and reading dataset...")
                    
                    # Grab the list of peers and figure out the ring size
                    peer_tuples = parts[1:]
                    n = len(peer_tuples)
                    ring_size = n
                    my_id = 0 
                    
                    # Figure out who my right neighbor is
                    if n > 1:
                        neighbor_info = peer_tuples[1].split(',')
                        right_neighbor = (neighbor_info[1], int(neighbor_info[2]))

                    tuples_str = " ".join(peer_tuples)
                    for i in range(1, n):
                        target_info = peer_tuples[i].split(',')
                        target_ip = target_info[1]
                        target_port = int(target_info[2])
                        
                        # Send the set-id message to tell everyone else their ID and the whole ring layout [cite: 147, 148]
                        set_id_msg = f"set-id {i} {n} {tuples_str}"
                        p_socket.sendto(set_id_msg.encode('utf-8'), (target_ip, target_port))

                    # Open the dataset
                    file_name = "details-1950.csv" 
                    try:
                        with open(file_name, mode='r', encoding='utf-8') as file:
                            csv_reader = csv.reader(file)
                            
                            # Skip the header row [cite: 157]
                            next(csv_reader) 
                            records = list(csv_reader)
                            
                            # Figure out how many rows we have to get our table size
                            l = len(records) 
                            s = get_table_size(l) 
                            
                            for record in records:
                                event_id = int(record[0])
                                
                                # Do the math to find the table spot (pos) and the peer ID [cite: 183, 185]
                                pos = event_id % s 
                                peer_id = pos % n  
                                
                                # Put the row back together as a string
                                record_string = ",".join(record)
                                store_msg = f"store {peer_id} {pos} {record_string}"
                                
                                # If the peer ID is 0, it belongs to me (the leader), so save it locally [cite: 190]
                                if peer_id == 0:
                                    local_hash_table[pos] = record_string
                                    records_stored_count += 1
                                else:
                                    # Otherwise, pass it down the ring to my right neighbor [cite: 191]
                                    if right_neighbor:
                                        p_socket.sendto(store_msg.encode('utf-8'), right_neighbor)
                                        
                        # Give the network a second to finish passing the data around
                        time.sleep(2)
                        
                        # Tell the manager we're all done [cite: 197]
                        print(f"DHT Setup Complete. I am storing {records_stored_count} records.")
                        complete_msg = f"dht-complete {peer_name}"
                        m_socket.sendto(complete_msg.encode('utf-8'), manager_address)
                        
                    except FileNotFoundError:
                        print(f"Error: '{file_name}' not found.")

            # 3. Handle messages from other peers in the ring
            elif sock == p_socket:
                message, sender_address = p_socket.recvfrom(4096)
                decoded_msg = message.decode('utf-8')
                parts = decoded_msg.split(maxsplit=3)
                
                if not parts:
                    continue
                    
                cmd = parts[0]
                
                if cmd == "set-id":
                    # We got assigned an ID and the ring layout [cite: 149]
                    my_id = int(parts[1]) 
                    ring_size = int(parts[2]) 
                    
                    peer_tuples = parts[3:]
                    
                    # Find my right neighbor using modulo math [cite: 150]
                    neighbor_index = (my_id + 1) % ring_size
                    neighbor_info = peer_tuples[neighbor_index].split(',')
                    right_neighbor = (neighbor_info[1], int(neighbor_info[2]))
                    
                    print(f"Ring setup: I am node {my_id} of {ring_size}.")
                    print(f"My right neighbor is {neighbor_info[0]} at {right_neighbor[0]}:{right_neighbor[1]}")
                    
                elif cmd == "store":
                    # Someone sent us a record to store
                    target_id = int(parts[1])
                    pos = int(parts[2])
                    record_data = parts[3]
                    
                    if target_id == my_id:
                        # It belongs to me! Save it in my dictionary [cite: 192]
                        local_hash_table[pos] = record_data
                        records_stored_count += 1
                    else:
                        # Not mine, pass it to my right neighbor [cite: 192]
                        if right_neighbor:
                            p_socket.sendto(message, right_neighbor)

if __name__ == "__main__":
    main()