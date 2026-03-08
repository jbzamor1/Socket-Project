import socket
import sys
import random

def main():
    # Grab the port number from the command line
    if len(sys.argv) != 2:
        print("Usage: python manager.py <port>")
        sys.exit(1)

    port = int(sys.argv[1])
    manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    manager_socket.bind(('', port))
    
    print(f"Manager listening on port {port}...")
    
    # Dictionary to keep track of everyone connected
    registered_peers = {} 
    
    # Flags to track if we're building the DHT right now
    dht_exists = False
    waiting_for_dht_complete = False
    dht_leader = ""

    # Infinite loop to keep the server awake
    while True:
        message, client_address = manager_socket.recvfrom(2048)
        decoded_msg = message.decode('utf-8').strip()
        parts = decoded_msg.split()
        
        if not parts:
            continue
            
        command = parts[0]
        
        # If we are currently building the table, block any other commands until it's done [cite: 98]
        if waiting_for_dht_complete and command != "dht-complete":
            manager_socket.sendto("FAILURE".encode('utf-8'), client_address)
            continue
        
        if command == "register":
            # Check if they sent the right amount of info for registering
            if len(parts) == 5:
                peer_name = parts[1]
                if peer_name not in registered_peers:
                    # Save their info and mark them as Free so they can be picked later [cite: 81]
                    registered_peers[peer_name] = {
                        "ip": parts[2],
                        "m_port": parts[3],
                        "p_port": parts[4],
                        "state": "Free"
                    }
                    # Tell them it worked [cite: 82]
                    response = "SUCCESS" 
                else:
                    # Send an error if they are already registered [cite: 85]
                    response = "FAILURE" 
            else:
                response = "FAILURE"
                
            manager_socket.sendto(response.encode('utf-8'), client_address)

        elif command == "setup-dht":
            if len(parts) == 4:
                peer_name = parts[1]
                
                # Make sure n is actually a number
                try:
                    n = int(parts[2])
                except ValueError:
                    manager_socket.sendto("FAILURE".encode('utf-8'), client_address)
                    continue
                    
                yyyy = parts[3]
                
                # Get a list of everyone who is Free
                free_peers = [name for name, info in registered_peers.items() if info["state"] == "Free"]

                # A bunch of checks to make sure we can actually build the DHT [cite: 88, 89, 90, 91, 92]
                if peer_name not in registered_peers:
                    response = "FAILURE" 
                elif n < 3:
                    response = "FAILURE" 
                elif len(registered_peers) < n:
                    response = "FAILURE" 
                elif dht_exists:
                    response = "FAILURE" 
                elif peer_name not in free_peers:
                    response = "FAILURE"
                else:
                    free_peers.remove(peer_name)
                    if len(free_peers) < n - 1:
                        response = "FAILURE" 
                    else:
                        # Upgrade the sender to Leader and lock the server [cite: 94, 98]
                        registered_peers[peer_name]["state"] = "Leader"
                        dht_leader = peer_name
                        dht_exists = True 
                        waiting_for_dht_complete = True 
                        
                        # Pick the other peers randomly [cite: 94]
                        selected_peers = random.sample(free_peers, n - 1)
                        
                        for p in selected_peers:
                            registered_peers[p]["state"] = "InDHT"
                            
                        leader_info = registered_peers[peer_name]
                        
                        # Put the leader first in the response string [cite: 96]
                        response = f"SUCCESS {peer_name},{leader_info['ip']},{leader_info['p_port']}"
                        
                        # Add the rest of the chosen peers
                        for p in selected_peers:
                            p_info = registered_peers[p]
                            response += f" {p},{p_info['ip']},{p_info['p_port']}"
            else:
                response = "FAILURE"
                
            manager_socket.sendto(response.encode('utf-8'), client_address)

        elif command == "dht-complete":
            if len(parts) == 2:
                peer_name = parts[1]
                
                # Only the leader is allowed to tell us it's complete [cite: 100]
                if peer_name == dht_leader:
                    # Unlock the server
                    waiting_for_dht_complete = False
                    response = "SUCCESS" 
                else:
                    response = "FAILURE"
            else:
                response = "FAILURE"
                
            manager_socket.sendto(response.encode('utf-8'), client_address)

if __name__ == "__main__":
    main()