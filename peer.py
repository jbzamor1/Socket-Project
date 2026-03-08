import socket
import sys
import pandas as pd # Highly recommended for reading details-YYYY.csv later [cite: 155]

def main():
    # The peer requires two arguments: manager IP and manager port 
    if len(sys.argv) != 3:
        print("Usage: python peer.py <manager_ip> <manager_port>")
        sys.exit(1)

    manager_ip = sys.argv[1]
    manager_port = int(sys.argv[2])
    manager_address = (manager_ip, manager_port)

    # Create a UDP socket 
    peer_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    print("Peer started. Type commands below:")

    # Read commands from stdin 
    for line in sys.stdin:
        command = line.strip()
        if not command:
            continue
            
        # Send message to the manager over the UDP socket 
        peer_socket.sendto(command.encode('utf-8'), manager_address)
        
        # Messages to the manager always come in pairs; wait for response [cite: 74]
        response, server = peer_socket.recvfrom(2048)
        print(f"Manager response: {response.decode('utf-8')}")
        
        # If the command was setup-dht, the leader will need to kick off the ring creation
        if command.startswith("setup-dht") and response.decode('utf-8').startswith("SUCCESS"):
            print("Initiating ring setup and reading dataset...")
            # Note: Ensure details-YYYY.csv is in your exact working directory to avoid a FileNotFoundError.
            # df = pd.read_csv('details-1950.csv') 

if __name__ == "__main__":
    main()