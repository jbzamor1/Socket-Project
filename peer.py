import sys
import socket
import threading
import json
import csv
import random
import time

# --- Global State ---
class PeerState:
    def __init__(self, name, ip, m_port, p_port, mgr_ip, mgr_port):
        # Keeps track of all my info and where I am in the ring
        self.name, self.ip, self.m_port, self.p_port = name, ip, m_port, p_port
        self.mgr_ip, self.mgr_port = mgr_ip, mgr_port
        self.my_id, self.ring_size = None, 0
        self.tuples, self.local_hash = [], {}
        self.r_ip, self.r_port = None, None # The next node in the ring
        self.is_leaving = False
        self.dataset_year = "1996"

# --- Networking Helpers ---
def encode_msg(*args): return "|".join(str(a) for a in args).encode('utf-8')
def decode_msg(data): return data.decode('utf-8').split("|")

def send_udp(msg, ip, port, await_response=False):
    # Basic UDP sender. Added a timeout so my terminal doesn't hang forever if a packet drops
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(2.0)
    try:
        sock.sendto(msg, (ip, int(port)))
        if await_response:
            data, _ = sock.recvfrom(4096)
            return decode_msg(data)
    except: pass
    finally: sock.close()
    return None

def is_prime(n):
    if n < 2: return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0: return False
    return True

def get_hash_s(year):
    # The assignment said to find a prime number based on the dataset size for hashing
    fname = f"details-{year}.csv"
    try:
        with open(fname, 'r', encoding='utf-8') as f:
            l = sum(1 for _ in f) - 1 # count lines minus header
        s = (2 * l) + 1
        while not is_prime(s): s += 1
        return s
    except: return 11 # Fallback just in case the file is missing

# --- Background Command Listener ---
def p_port_listener(state):
    # This runs in a separate thread so we can receive commands while typing in the terminal
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", state.p_port))
    while True:
        try:
            data, addr = sock.recvfrom(65535)
            args = decode_msg(data)
            cmd = args[0]

            if cmd == "SUCCESS":
                print(f"\n[QUERY RESULT] Found: {args[1]}\nPath: {args[2]}", flush=True)
                print(f"[{state.name}] > ", end="", flush=True)
            elif cmd == "FAILURE":
                print(f"\n[QUERY RESULT] Event not found.", flush=True)
                print(f"[{state.name}] > ", end="", flush=True)
                
            elif cmd == "set-id":
                # Manager tells us our ID and who is next in the ring
                state.my_id, state.ring_size = int(args[1]), int(args[2])
                state.tuples = json.loads(args[3])
                nxt = state.tuples[(state.my_id + 1) % state.ring_size]
                state.r_ip, state.r_port = nxt[1], int(nxt[2])
                
            elif cmd == "store":
                t_id, pos, e_id, rec = int(args[1]), int(args[2]), int(args[3]), args[4]
                if state.my_id == t_id:
                    # It belongs to me, save it
                    if pos not in state.local_hash: state.local_hash[pos] = {}
                    state.local_hash[pos][e_id] = rec
                else: 
                    # Not mine, pass it to the next guy
                    send_udp(data, state.r_ip, state.r_port)
                    
            elif cmd == "find-event":
                # Searching for data. Hash it to figure out whose ID it belongs to
                e_id, seq_str, s_ip, s_port = int(args[1]), args[2], args[3], int(args[4])
                seq = seq_str.split(",") if seq_str else []
                s = get_hash_s(state.dataset_year)
                pos, t_id = e_id % s, (e_id % s) % state.ring_size
                
                if state.my_id == t_id:
                    if pos in state.local_hash and e_id in state.local_hash[pos]:
                        seq.append(state.name)
                        send_udp(encode_msg("SUCCESS", state.local_hash[pos][e_id], ",".join(seq)), s_ip, s_port)
                    else: send_udp(encode_msg("FAILURE"), s_ip, s_port)
                else:
                    # Forward the query to a random node we haven't visited yet
                    avail = [str(i) for i in range(state.ring_size) if state.tuples[i][0] not in seq]
                    if avail:
                        nxt = int(random.choice(avail))
                        seq.append(state.name)
                        send_udp(encode_msg("find-event", e_id, ",".join(seq), s_ip, s_port), state.tuples[nxt][1], state.tuples[nxt][2])
                    else: send_udp(encode_msg("FAILURE"), s_ip, s_port)
            
            # --- FIXED LEAVE/TEARDOWN LOGIC ---
            elif cmd == "teardown":
                i_id = int(args[1])
                state.local_hash.clear() # Dump all my data
                if state.my_id != i_id:
                    send_udp(data, state.r_ip, state.r_port)
                else:
                    # Teardown completed the circle. Remove myself and start reset-id.
                    n_ring = state.ring_size - 1
                    state.tuples = [t for t in state.tuples if t[0] != state.name]
                    send_udp(encode_msg("reset-id", 0, n_ring, json.dumps(state.tuples)), state.r_ip, state.r_port)
                    
            elif cmd == "reset-id":
                n_id, n_ring, tups = int(args[1]), int(args[2]), json.loads(args[3])
                state.my_id, state.ring_size, state.tuples = n_id, n_ring, tups
                nxt = state.tuples[(state.my_id + 1) % state.ring_size]
                state.r_ip, state.r_port = nxt[1], int(nxt[2])
                
                # If I am the last node in the new ring, trigger the rebuild
                if state.my_id == state.ring_size - 1:
                    send_udp(encode_msg("rebuild-dht", state.name, state.p_port), state.r_ip, state.r_port)
                else:
                    send_udp(encode_msg("reset-id", n_id+1, n_ring, args[3]), state.r_ip, state.r_port)
            # -----------------------------------
            
            # These handle dynamic joining and rebuilding the ring
            elif cmd == "request-join":
                new_tup = [args[1], args[2], args[3]]
                state.local_hash.clear()
                send_udp(encode_msg("teardown-for-join", state.my_id, json.dumps(new_tup), args[2], args[3]), state.r_ip, state.r_port)
            elif cmd == "teardown-for-join":
                state.local_hash.clear()
                if int(args[1]) != state.my_id: send_udp(data, state.r_ip, state.r_port)
                else:
                    state.ring_size += 1
                    state.tuples.append(json.loads(args[2]))
                    send_udp(encode_msg("reset-id-join", 0, state.ring_size, json.dumps(state.tuples), args[3], args[4]), state.r_ip, state.r_port)
            elif cmd == "reset-id-join":
                state.my_id, state.ring_size, state.tuples = int(args[1]), int(args[2]), json.loads(args[3])
                nxt = state.tuples[(state.my_id + 1) % state.ring_size]
                state.r_ip, state.r_port = nxt[1], int(nxt[2])
                if state.my_id == state.ring_size - 1: send_udp(encode_msg("rebuild-dht", state.name, state.p_port), state.r_ip, state.r_port)
                else: send_udp(encode_msg("reset-id-join", state.my_id+1, state.ring_size, args[3], args[4], args[5]), state.r_ip, state.r_port)
            
            elif cmd == "rebuild-dht":
                # Reload the dataset from scratch. 
                s = get_hash_s(state.dataset_year)
                with open(f"details-{state.dataset_year}.csv", 'r', encoding='utf-8') as f:
                    r = csv.reader(f); next(r)
                    count = 0
                    for row in r:
                        try: eid = int(row[7])
                        except: continue
                        pos, tid = eid % s, (eid % s) % state.ring_size
                        if tid == state.my_id:
                            if pos not in state.local_hash: state.local_hash[pos] = {}
                            state.local_hash[pos][eid] = ",".join(row)
                        else: send_udp(encode_msg("store", tid, pos, eid, ",".join(row)), state.r_ip, state.r_port)
                        count += 1
                        # Added sleep here because sending too fast drops UDP packets
                        if count % 100 == 0: time.sleep(0.01) 
                send_udp(encode_msg("rebuild-complete", state.name), addr[0], int(args[2]))
            elif cmd == "rebuild-complete":
                send_udp(encode_msg("dht-rebuilt", state.name), state.mgr_ip, state.mgr_port)
                state.is_leaving = False
        except: pass

# --- User Interface ---
def main():
    m_ip, m_port = sys.argv[1], int(sys.argv[2])
    print("--- Peer Startup ---")
    name = input("Peer Name: ")
    my_ip = input("Enter THIS Node's LAN IP: ")
    m_p, p_p = int(input("M-Port: ")), int(input("P-Port: "))
    
    state = PeerState(name, my_ip, m_p, p_p, m_ip, m_port)
    # Start the listener thread in the background
    threading.Thread(target=p_port_listener, args=(state,), daemon=True).start()
    time.sleep(1)
    print("Ready. Commands: register, setup-dht, query-dht, leave-dht, join-dht, teardown-dht, status-dht, deregister, exit")

    # The main CLI loop
    while True:
        try:
            raw = input(f"[{state.name}] > ").strip().split()
            if not raw: continue
            cmd = raw[0]

            if cmd == "register":
                res = send_udp(encode_msg("register", name, my_ip, m_p, p_p), m_ip, m_port, True)
                if res: print(res[0], flush=True)
                
            elif cmd == "status-dht":
                total = sum(len(v) for v in state.local_hash.values())
                print(f"Holding {total} records.", flush=True)
                
            elif cmd == "setup-dht":
                n, y = int(raw[1]), raw[2]
                state.dataset_year = y
                res = send_udp(encode_msg("setup-dht", name, n, y), m_ip, m_port, True)
                if res and res[0] == "SUCCESS":
                    state.my_id, state.ring_size, state.tuples = 0, n, [t.split(',') for t in res[1:]]
                    nxt = state.tuples[1 % n]
                    state.r_ip, state.r_port = nxt[1], int(nxt[2])
                    
                    # Tell everyone their IDs
                    for i in range(1, n): send_udp(encode_msg("set-id", i, n, json.dumps(state.tuples)), state.tuples[i][1], state.tuples[i][2])
                    print("Syncing ring... (Wait 2s)", flush=True); time.sleep(2.0)
                    s = get_hash_s(y)
                    print(f"Distributing records... (Approx 10s)", flush=True)
                    
                    # Parse the CSV and send records to the right peer based on the hash
                    with open(f"details-{y}.csv", 'r', encoding='utf-8') as f:
                        r = csv.reader(f); next(r)
                        count = 0
                        for row in r:
                            try: eid = int(row[7])
                            except: continue
                            pos, tid = eid % s, (eid % s) % n
                            if tid == 0:
                                if pos not in state.local_hash: state.local_hash[pos] = {}
                                state.local_hash[pos][eid] = ",".join(row)
                            else: send_udp(encode_msg("store", tid, pos, eid, ",".join(row)), state.r_ip, state.r_port)
                            count += 1
                            if count % 100 == 0: time.sleep(0.01) # Sleep to prevent UDP buffer overflow
                    send_udp(encode_msg("dht-complete", name), m_ip, m_port)
                    print("DHT is up and running.", flush=True)
                    
            elif cmd == "query-dht":
                res = send_udp(encode_msg("query-dht", name), m_ip, m_port, True)
                if res and res[0] == "SUCCESS":
                    print(f"Entry point identified: {res[1]}. Sending query...", flush=True)
                    send_udp(encode_msg("find-event", raw[1], "", my_ip, p_p), res[2], res[3])
                    
            elif cmd == "leave-dht":
                res = send_udp(encode_msg("leave-dht", name), m_ip, m_port, True)
                if res and res[0] == "SUCCESS":
                    print("SUCCESS", flush=True); state.is_leaving = True
                    send_udp(encode_msg("teardown", state.my_id), state.r_ip, state.r_port)
                    
            elif cmd == "join-dht":
                res = send_udp(encode_msg("join-dht", name), m_ip, m_port, True)
                if res and res[0] == "SUCCESS":
                    print("SUCCESS", flush=True); send_udp(encode_msg("request-join", name, my_ip, p_p), raw[1], raw[2])
                    
            elif cmd == "teardown-dht":
                res = send_udp(encode_msg("teardown-dht", name), m_ip, m_port, True)
                if res and res[0] == "SUCCESS":
                    print("SUCCESS", flush=True); state.local_hash.clear()
                    send_udp(encode_msg("teardown", state.my_id), state.r_ip, state.r_port)
                    send_udp(encode_msg("teardown-complete", name), m_ip, m_port, True)
                    
            elif cmd == "deregister":
                res = send_udp(encode_msg("deregister", name), m_ip, m_port, True)
                if res and res[0] == "SUCCESS": print("SUCCESS", flush=True); break
            elif cmd == "exit": break
        except Exception: break

if __name__ == "__main__": main()