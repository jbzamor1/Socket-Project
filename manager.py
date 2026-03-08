import socket
import sys
import random

def main():
    # The manager requires exactly one argument: the port number 
    if len(sys.argv) != 2:
        print("Usage: python manager.py <port>")
        sys.exit(1)

    port = int(sys.argv[1])
    
    # Create a UDP socket 
    manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    manager_socket.bind(('', port))
    
    print(f"Manager listening on port {port}...")
    
    # State information base [cite: 79]
    registered_peers = {} 

    # Infinite loop listening for incoming messages 
    while True:
        message, client_address = manager_socket.recvfrom(2048)
        decoded_msg = message.decode('utf-8').strip()
        print(f"Received from {client_address}: {decoded_msg}")
        
        parts = decoded_msg.split()
        command = parts[0]
        
        if command == "register":
            # Expected format: register <peer-name> <IPv4-address> <m-port> <p-port> 
            if len(parts) == 5:
                peer_name = parts[1]
                if peer_name not in registered_peers:
                    # Store peer state as Free [cite: 81]
                    registered_peers[peer_name] = {
                        "ip": parts[2],
                        "m_port": parts[3],
                        "p_port": parts[4],
                        "state": "Free"
                    }
                    response = "SUCCESS"
                else:
                    response = "FAILURE" # Duplicate registration [cite: 85]
            else:
                response = "FAILURE"
                
            manager_socket.sendto(response.encode('utf-8'), client_address)

        elif command == "setup-dht":
            # Milestone logic for setting up the DHT goes here [cite: 286]
            # You will need to select n-1 Free peers, upgrade the sender to Leader, 
            # and return the 3-tuples [cite: 94, 95]
            response = "SUCCESS" # Placeholder for testing
            manager_socket.sendto(response.encode('utf-8'), client_address)

        elif command == "dht-complete":
            # Milestone logic for acknowledging table completion [cite: 286]
            response = "SUCCESS"
            manager_socket.sendto(response.encode('utf-8'), client_address)

if __name__ == "__main__":
    main()