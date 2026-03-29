import sys
import socket
import random

# Helper functions to pack/unpack messages so I don't have to keep doing it manually
def encode_msg(command, *args):
    return "|".join([str(command)] + [str(a) for a in args]).encode('utf-8')

def decode_msg(data):
    return data.decode('utf-8').split("|")

def main():
    # Make sure we actually passed the port number when running this in the terminal
    if len(sys.argv) != 2:
        print("Usage: python3 manager.py <listen_port>")
        sys.exit(1)
        
    m_port = int(sys.argv[1])
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.bind(("", m_port))
    
    print(f"Manager listening on port {m_port}...")

    # Big dictionary to keep track of everyone
    # Format: peer_name -> {'ip': ip, 'm_port': m, 'p_port': p, 'state': 'Free'/'Leader'/'InDHT'}
    peers = {} 
    
    # Keep track of the DHT itself so we don't accidentally make two
    dht_state = {'exists': False, 'leader': None, 'size': 0}
    
    # Sometimes we need to lock the server and only wait for a specific message to finish a process
    waiting_for = None 

    # Infinite loop to keep the server awake forever
    while True:
        data, addr = server_socket.recvfrom(4096)
        args = decode_msg(data)
        cmd = args[0]

        # Assume it fails by default, makes the code shorter
        response = encode_msg("FAILURE") 

        # If we are locked waiting for something specific, ignore everything else
        if waiting_for and cmd != waiting_for and cmd not in ["teardown-complete"]:
            server_socket.sendto(encode_msg("FAILURE"), addr)
            continue

        if cmd == "register":
            # Grab all the registration info
            p_name, p_ip, pm_port, pp_port = args[1], args[2], args[3], args[4]
            # Only register if the name isn't taken already
            if p_name not in peers:
                # They start as Free
                peers[p_name] = {'ip': p_ip, 'm_port': pm_port, 'p_port': pp_port, 'state': 'Free'}
                response = encode_msg("SUCCESS")

        elif cmd == "setup-dht":
            p_name, n_str, year = args[1], args[2], args[3]
            n = int(n_str)
            # Lots of rules here: peer must exist, n >= 3, enough users registered, and no existing DHT
            if p_name in peers and n >= 3 and len(peers) >= n and not dht_state['exists']:
                # Make this guy the leader
                peers[p_name]['state'] = 'Leader'
                dht_state['leader'] = p_name
                dht_state['size'] = n
                
                # Find all the Free peers to pick from
                free_peers = [name for name, info in peers.items() if info['state'] == 'Free' and name != p_name]
                selected = random.sample(free_peers, n - 1)
                
                # Pack the leader's info first
                tuples = [f"{p_name},{peers[p_name]['ip']},{peers[p_name]['p_port']}"]
                # Add the rest of the randomly picked guys and update their state
                for sp in selected:
                    peers[sp]['state'] = 'InDHT'
                    tuples.append(f"{sp},{peers[sp]['ip']},{peers[sp]['p_port']}")
                
                # Lock the server until the leader says the DHT is done
                waiting_for = "dht-complete"
                response = encode_msg("SUCCESS", *tuples)

        elif cmd == "dht-complete":
            p_name = args[1]
            # Gotta make sure only the leader can finish this
            if p_name == dht_state['leader']:
                dht_state['exists'] = True
                waiting_for = None # Unlock the server
                response = encode_msg("SUCCESS")

        elif cmd == "query-dht":
            p_name = args[1]
            # Only Free peers can query an existing DHT
            if dht_state['exists'] and p_name in peers and peers[p_name]['state'] == 'Free':
                # Pick a random node that's actually in the DHT to give to the client
                active_dht = [name for name, info in peers.items() if info['state'] in ['Leader', 'InDHT']]
                target = random.choice(active_dht)
                t_info = peers[target]
                response = encode_msg("SUCCESS", target, t_info['ip'], t_info['p_port'])

        elif cmd == "leave-dht":
            p_name = args[1]
            # Check if they are actually in the DHT
            if dht_state['exists'] and peers.get(p_name, {}).get('state') in ['Leader', 'InDHT']:
                waiting_for = "dht-rebuilt" # Lock until rebuilt
                response = encode_msg("SUCCESS")

        elif cmd == "join-dht":
            p_name = args[1]
            # Only Free peers can join
            if dht_state['exists'] and peers.get(p_name, {}).get('state') == 'Free':
                waiting_for = "dht-rebuilt" # Lock it down
                l_info = peers[dht_state['leader']]
                # I'm giving them the leader's info so they know who to talk to on the p-port
                response = encode_msg("SUCCESS", dht_state['leader'], l_info['ip'], l_info['p_port'])

        elif cmd == "dht-rebuilt":
            p_name, new_leader = args[1], args[2]
            waiting_for = None # Unlock
            dht_state['leader'] = new_leader
            peers[new_leader]['state'] = 'Leader'
            
            # If they were free, they just joined. If they weren't, they just left.
            if peers[p_name]['state'] == 'Free': 
                peers[p_name]['state'] = 'InDHT'
            else:
                peers[p_name]['state'] = 'Free'
                
            response = encode_msg("SUCCESS")

        elif cmd == "deregister":
            p_name = args[1]
            # You can't deregister if you are in the DHT
            if p_name in peers and peers[p_name]['state'] == 'Free':
                del peers[p_name] # Bye
                response = encode_msg("SUCCESS")

        elif cmd == "teardown-dht":
            p_name = args[1]
            # Only the leader can destroy everything
            if p_name == dht_state['leader']:
                waiting_for = "teardown-complete"
                response = encode_msg("SUCCESS")

        elif cmd == "teardown-complete":
            p_name = args[1]
            if p_name == dht_state['leader']:
                # Set everyone who was in the DHT back to Free
                for p in peers:
                    if peers[p]['state'] in ['Leader', 'InDHT']:
                        peers[p]['state'] = 'Free'
                dht_state['exists'] = False
                dht_state['leader'] = None
                waiting_for = None # Unlock
                response = encode_msg("SUCCESS")

        # Send the response back to whoever asked
        server_socket.sendto(response, addr)

if __name__ == "__main__":
    main()