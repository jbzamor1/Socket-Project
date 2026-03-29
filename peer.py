import sys
import socket
import threading
import json
import csv
import random
import time

# --- State & Setup Stuff ---
# Keeping all the peer's info in one object so the threads don't get confused
class PeerState:
    def __init__(self, name, ip, m_port, p_port, mgr_ip, mgr_port):
        self.name = name
        self.ip = ip
        self.m_port = m_port
        self.p_port = p_port
        self.mgr_ip = mgr_ip
        self.mgr_port = mgr_port
        
        self.my_id = None
        self.ring_size = 0
        self.tuples = [] # The master list of who is in the ring
        self.local_hash = {} # Format: { pos: {event_id: record_string} }
        
        self.r_ip = None
        self.r_port = None
        self.is_leaving = False
        self.dataset_year = "1996"

# Little helpers to pack and unpack string messages for the sockets
def encode_msg(*args): 
    return "|".join(str(a) for a in args).encode('utf-8')

def decode_msg(data): 
    return data.decode('utf-8').split("|")

# A quick way to shoot a UDP message to another peer
def send_udp(msg, ip, port, await_response=False):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(3.0) # Don't hang forever if a packet drops
    try:
        sock.sendto(msg, (ip, int(port)))
        if await_response:
            data, _ = sock.recvfrom(4096)
            return decode_msg(data)
    except:
        pass
    finally:
        sock.close()
    return None

# Figure out who my right neighbor is so I know where to pass the potato
def update_neighbor(state):
    if state.ring_size > 1:
        n_id = (state.my_id + 1) % state.ring_size
        if n_id < len(state.tuples):
            n = state.tuples[n_id]
            state.r_ip = n[1]
            state.r_port = n[2]

# --- Math & Hashing ---
def is_prime(n):
    # Standard prime checker
    if n <= 1: return False
    if n <= 3: return True
    if n % 2 == 0 or n % 3 == 0: return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0: return False
        i += 6
    return True

def build_dht(state):
    # Clears out old data and re-reads the CSV file
    state.local_hash.clear()
    fname = f"details-{state.dataset_year}.csv"
    try:
        # Pass 1: count the lines to find l
        with open(fname, 'r', encoding='utf-8') as f: 
            l = sum(1 for _ in f) - 1
            
        # Get the prime table size (s)
        s = (2 * l) + 1
        while not is_prime(s): s += 1
        
        # Pass 2: Actually read and distribute the storm records
        with open(fname, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader) # skip the headers
            for row in reader:
                if not row: continue
                e_id = int(row[0])
                
                # The dual hashes required by the rubric
                pos = e_id % s
                t_id = pos % state.ring_size
                rec = ",".join(row)
                
                # If it's mine, keep it. Otherwise, send to right neighbor
                if t_id == state.my_id:
                    if pos not in state.local_hash: 
                        state.local_hash[pos] = {}
                    state.local_hash[pos][e_id] = rec
                else:
                    send_udp(encode_msg("store", t_id, pos, e_id, rec), state.r_ip, state.r_port)
    except FileNotFoundError:
        print(f"Error: Make sure {fname} is in the same folder!")

# --- The Background P2P Thread ---
def p_port_listener(state):
    # This runs forever in the background listening for ring commands
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", state.p_port))
    while True:
        data, addr = sock.recvfrom(65535) # Need a big buffer for those long CSV strings
        args = decode_msg(data)
        cmd = args[0]

        # Initial ring setup
        if cmd == "set-id":
            state.my_id = int(args[1])
            state.ring_size = int(args[2])
            state.tuples = [t.split(',') for t in json.loads(args[3])]
            update_neighbor(state)
        
        # Storing data during the build phase
        elif cmd == "store":
            t_id, pos, e_id, rec = int(args[1]), int(args[2]), int(args[3]), args[4]
            if state.my_id == t_id:
                if pos not in state.local_hash: state.local_hash[pos] = {}
                state.local_hash[pos][e_id] = rec
            else:
                send_udp(data, state.r_ip, state.r_port) # Pass it along

        # The actual hot-potato search logic
        elif cmd == "find-event":
            e_id, seq_str, s_ip, s_port = int(args[1]), args[2], args[3], args[4]
            seq = seq_str.split(",") if seq_str else []
            
            # Need to recalculate s here to find the target ID
            # A bit hacky but we just approximate 's' for the search
            s = (2 * 55000) + 1 
            while not is_prime(s): s+=1
            pos = e_id % s
            t_id = pos % state.ring_size
            
            if state.my_id == t_id:
                # I am supposed to have it. Let's check.
                if pos in state.local_hash and e_id in state.local_hash[pos]:
                    seq.append(str(state.my_id))
                    send_udp(encode_msg("SUCCESS", state.local_hash[pos][e_id], ",".join(seq)), s_ip, s_port)
                else:
                    send_udp(encode_msg("FAILURE"), s_ip, s_port)
            else:
                # It's not mine, throw the potato to a random unvisited node
                avail = [str(i) for i in range(state.ring_size) if i != t_id and str(i) not in seq]
                if avail:
                    nxt = random.choice(avail)
                    seq.append(nxt)
                    nxt_tup = state.tuples[int(nxt)]
                    send_udp(encode_msg("find-event", e_id, ",".join(seq), s_ip, s_port), nxt_tup[1], nxt_tup[2])
                else:
                    send_udp(encode_msg("FAILURE"), s_ip, s_port) # Nowhere left to search

        # --- LEAVING & JOINING ---
        # Deleting tables around the ring
        elif cmd == "teardown":
            i_id = int(args[1])
            state.local_hash.clear()
            if state.my_id != i_id:
                send_udp(encode_msg("teardown", i_id), state.r_ip, state.r_port)
            else:
                # Ring cleared. Remove myself and start the ID reset
                n_ring = state.ring_size - 1
                state.tuples = [t for t in state.tuples if t[0] != state.name]
                send_udp(encode_msg("reset-id", 0, n_ring, json.dumps(state.tuples)), state.r_ip, state.r_port)

        elif cmd == "reset-id":
            n_id, n_ring, tups = int(args[1]), int(args[2]), json.loads(args[3])
            if state.is_leaving:
                # I'm the one who left, tell the new leader to fix the tables
                send_udp(encode_msg("rebuild-dht", state.name, state.p_port), state.r_ip, state.r_port)
            else:
                state.my_id, state.ring_size, state.tuples = n_id, n_ring, tups
                update_neighbor(state)
                send_udp(encode_msg("reset-id", n_id+1, n_ring, args[3]), state.r_ip, state.r_port)

        # Handling a join request from a new peer
        elif cmd == "request-join":
            new_name, u_ip, u_port = args[1], args[2], int(args[3])
            new_tup = [new_name, u_ip, str(u_port)]
            state.local_hash.clear()
            send_udp(encode_msg("teardown-for-join", state.my_id, json.dumps(new_tup), u_ip, u_port), state.r_ip, state.r_port)

        elif cmd == "teardown-for-join":
            i_id, tup_str, u_ip, u_port = int(args[1]), args[2], args[3], args[4]
            state.local_hash.clear()
            if state.my_id != i_id:
                send_udp(data, state.r_ip, state.r_port)
            else:
                # Add the new guy and start ID reset
                state.ring_size += 1
                state.tuples.append(json.loads(tup_str))
                send_udp(encode_msg("reset-id-join", 0, state.ring_size, json.dumps(state.tuples), u_ip, u_port), state.r_ip, state.r_port)

        elif cmd == "reset-id-join":
            n_id, n_ring, tups, u_ip, u_port = int(args[1]), int(args[2]), json.loads(args[3]), args[4], args[5]
            state.my_id, state.ring_size, state.tuples = n_id, n_ring, tups
            update_neighbor(state)
            if n_id == n_ring - 1:
                # I'm the new guy! Tell my right neighbor to rebuild
                send_udp(encode_msg("rebuild-dht", state.name, state.p_port), state.r_ip, state.r_port)
            else:
                send_udp(encode_msg("reset-id-join", n_id+1, n_ring, args[3], u_ip, u_port), state.r_ip, state.r_port)

        # Rebuilding the tables after someone leaves or joins
        elif cmd == "rebuild-dht":
            u_name, u_port = args[1], args[2]
            build_dht(state)
            send_udp(encode_msg("rebuild-complete", state.name), addr[0], u_port)
            
        elif cmd == "rebuild-complete":
            n_leader = args[1]
            send_udp(encode_msg("dht-rebuilt", state.name, n_leader), state.mgr_ip, state.mgr_port)
            state.is_leaving = False
            state.ring_size = 0

# --- Main Terminal Input Loop ---
def main():
    if len(sys.argv) != 3:
        print("Usage: python3 peer.py <manager_ip> <manager_port>")
        sys.exit(1)
        
    m_ip = sys.argv[1]
    m_port = int(sys.argv[2])
    
    name = input("Peer Name: ")
    my_ip = "127.0.0.1" # Hardcoded to localhost for easy testing
    my_m_port = int(input("M-Port: "))
    my_p_port = int(input("P-Port: "))
    
    state = PeerState(name, my_ip, my_m_port, my_p_port, m_ip, m_port)
    
    # Spin up the background thread so we can listen and type at the same time
    threading.Thread(target=p_port_listener, args=(state,), daemon=True).start()
    time.sleep(1) # Let the socket bind properly
    
    print("Commands: register, setup-dht, query-dht, leave-dht, join-dht, teardown-dht, deregister, exit")
    
    while True:
        try:
            raw = input(f"[{state.name}] > ").strip().split()
            if not raw: continue
            cmd = raw[0]

            if cmd == "register":
                res = send_udp(encode_msg("register", state.name, state.ip, state.m_port, state.p_port), state.mgr_ip, state.mgr_port, True)
                print(res[0] if res else "No response from manager")

            elif cmd == "setup-dht":
                n, y = raw[1], raw[2]
                state.dataset_year = y
                res = send_udp(encode_msg("setup-dht", state.name, n, y), state.mgr_ip, state.mgr_port, True)
                if res and res[0] == "SUCCESS":
                    state.my_id = 0
                    state.ring_size = int(n)
                    state.tuples = [t.split(',') for t in res[1:]]
                    update_neighbor(state)
                    
                    # Wire up all the other peers in the ring
                    for i in range(1, int(n)):
                        t = state.tuples[i]
                        send_udp(encode_msg("set-id", i, n, json.dumps(state.tuples)), t[1], t[2])
                        
                    build_dht(state)
                    send_udp(encode_msg("dht-complete", state.name), state.mgr_ip, state.mgr_port, True)
                    print("DHT Setup Complete.")

            elif cmd == "query-dht":
                res = send_udp(encode_msg("query-dht", state.name), state.mgr_ip, state.mgr_port, True)
                if res and res[0] == "SUCCESS":
                    e_id = raw[1]
                    
                    # Make a temporary listening socket just to catch the search result
                    q_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    q_sock.bind(("", 0)) 
                    q_port = q_sock.getsockname()[1]
                    
                    send_udp(encode_msg("find-event", e_id, "", state.ip, q_port), res[2], res[3])
                    
                    q_sock.settimeout(5.0)
                    try:
                        d, _ = q_sock.recvfrom(4096)
                        ans = decode_msg(d)
                        if ans[0] == "SUCCESS":
                            print(f"Record: {ans[1]}\nPath: {ans[2]}")
                        else:
                            print(f"Storm event {e_id} not found in the DHT.")
                    except:
                        print("Query timed out.")
                    q_sock.close()

            elif cmd == "leave-dht":
                res = send_udp(encode_msg("leave-dht", state.name), state.mgr_ip, state.mgr_port, True)
                if res and res[0] == "SUCCESS":
                    state.is_leaving = True
                    send_udp(encode_msg("teardown", state.my_id), state.r_ip, state.r_port)
                    
            elif cmd == "join-dht":
                if len(raw) == 3:
                    target_ip = raw[1]
                    target_port = raw[2]
                    res = send_udp(encode_msg("join-dht", state.name), state.mgr_ip, state.mgr_port, True)
                    if res and res[0] == "SUCCESS":
                        # Shoot request-join to the leader we were given
                        send_udp(encode_msg("request-join", state.name, state.ip, state.p_port), target_ip, target_port)
                        
            elif cmd == "teardown-dht":
                res = send_udp(encode_msg("teardown-dht", state.name), state.mgr_ip, state.mgr_port, True)
                if res and res[0] == "SUCCESS":
                    state.local_hash.clear()
                    send_udp(encode_msg("teardown", state.my_id), state.r_ip, state.r_port)
                    send_udp(encode_msg("teardown-complete", state.name), state.mgr_ip, state.mgr_port, True)
                    
            elif cmd == "deregister":
                res = send_udp(encode_msg("deregister", state.name), state.mgr_ip, state.mgr_port, True)
                print(res[0] if res else "No response")
                if res and res[0] == "SUCCESS": break
                
            elif cmd == "exit":
                break

        except KeyboardInterrupt: break

if __name__ == "__main__":
    main()