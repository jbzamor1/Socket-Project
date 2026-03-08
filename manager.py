import socket
import sys
import random

def main():
    # The manager should read one command line parameter, an integer giving the port number 
    if len(sys.argv) != 2:
        print("Usage: python manager.py <port>")
        sys.exit(1)

    port = int(sys.argv[1])
    manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    manager_socket.bind(('', port))
    
    print(f"Manager listening on port {port}...")
    
    # State information base [cite: 66]
    registered_peers = {} 
    dht_exists = False
    waiting_for_dht_complete = False
    dht_leader = ""

    # Once started it runs in an infinite loop, listening to the given port [cite: 65]
    while True:
        message, client_address = manager_socket.recvfrom(2048)
        decoded_msg = message.decode('utf-8').strip()
        parts = decoded_msg.split()
        
        if not parts:
            continue
            
        command = parts[0]
        
        # After responding with SUCCESS to a setup-dht, the manager waits for dht-complete, returning FAILURE to any other incoming messages [cite: 98]
        if waiting_for_dht_complete and command != "dht-complete":
            manager_socket.sendto("FAILURE".encode('utf-8'), client_address)
            continue
        
        if command == "register":
            # Format: register <peer-name> <IPv4-address> <m-port> <p-port> [cite: 77]
            if len(parts) == 5:
                peer_name = parts[1]
                if peer_name not in registered_peers:
                    # The state of the peer is set to Free [cite: 81]
                    registered_peers[peer_name] = {
                        "ip": parts[2],
                        "m_port": parts[3],
                        "p_port": parts[4],
                        "state": "Free"  # Peer is able to participate [cite: 68]
                    }
                    response = "SUCCESS" # If the parameters are unique, the manager responds with SUCCESS [cite: 82]
                else:
                    response = "FAILURE" # Returns FAILURE in the case of a duplicate registration [cite: 85]
            else:
                response = "FAILURE"
                
            manager_socket.sendto(response.encode('utf-8'), client_address)

        elif command == "setup-dht":
            # Format: setup-dht <peer-name> <n> <YYYY> [cite: 86]
            if len(parts) == 4:
                peer_name = parts[1]
                try:
                    n = int(parts[2])
                except ValueError:
                    manager_socket.sendto("FAILURE".encode('utf-8'), client_address)
                    continue
                    
                yyyy = parts[3]
                free_peers = [name for name, info in registered_peers.items() if info["state"] == "Free"]

                if peer_name not in registered_peers:
                    response = "FAILURE" # The peer-name is not registered [cite: 89]
                elif n < 3:
                    response = "FAILURE" # n is not at least three [cite: 90]
                elif len(registered_peers) < n:
                    response = "FAILURE" # Fewer than n users are registered with the manager [cite: 91]
                elif dht_exists:
                    response = "FAILURE" # A DHT has already been set up [cite: 92]
                elif peer_name not in free_peers:
                    response = "FAILURE"
                else:
                    free_peers.remove(peer_name)
                    if len(free_peers) < n - 1:
                        response = "FAILURE" 
                    else:
                        # The manager sets the state of peer-name to Leader [cite: 94]
                        registered_peers[peer_name]["state"] = "Leader"
                        dht_leader = peer_name
                        dht_exists = True 
                        waiting_for_dht_complete = True 
                        
                        # Selects at random n-1 Free users [cite: 94]
                        selected_peers = random.sample(free_peers, n - 1)
                        
                        # Updating each one's state to InDHT [cite: 94]
                        for p in selected_peers:
                            registered_peers[p]["state"] = "InDHT"
                            
                        leader_info = registered_peers[peer_name]
                        # 3-tuple of the leader is given first [cite: 96]
                        response = f"SUCCESS {peer_name},{leader_info['ip']},{leader_info['p_port']}"
                        
                        # The other tuples can follow in any order [cite: 96]
                        for p in selected_peers:
                            p_info = registered_peers[p]
                            response += f" {p},{p_info['ip']},{p_info['p_port']}"
            else:
                response = "FAILURE"
                
            manager_socket.sendto(response.encode('utf-8'), client_address)

        elif command == "dht-complete":
            # Format: dht-complete <peer-name> [cite: 99]
            if len(parts) == 2:
                peer_name = parts[1]
                # If the peer-name is not the leader of the DHT then this command returns FAILURE [cite: 100]
                if peer_name == dht_leader:
                    waiting_for_dht_complete = False
                    response = "SUCCESS" # Otherwise, the manager responds to the leader with SUCCESS [cite: 100]
                else:
                    response = "FAILURE"
            else:
                response = "FAILURE"
                
            manager_socket.sendto(response.encode('utf-8'), client_address)

if __name__ == "__main__":
    main()