import sys
import socket
import threading
import json
import csv
import random
import time

class PeerState:
    def __init__(self, name, ip, m_port, p_port, mgr_ip, mgr_port):
        self.name, self.ip, self.m_port, self.p_port = name, ip, m_port, p_port
        self.mgr_ip, self.mgr_port = mgr_ip, mgr_port
        self.my_id, self.ring_size = None, 0
        self.tuples, self.local_hash = [], {}
        self.r_ip, self.r_port = None, None
        self.dataset_year = "1996"

def encode_msg(*args): return "|".join(str(a) for a in args).encode('utf-8')
def decode_msg(data): return data.decode('utf-8').split("|")

def send_udp(msg, ip, port, await_response=False):
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
    fname = f"details-{year}.csv"
    try:
        with open(fname, 'r') as f:
            l = sum(1 for _ in f) - 1
        s = (2 * l) + 1
        while not is_prime(s): s += 1
        return s
    except: return 11

def p_port_listener(state):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", state.p_port))
    while True:
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
            state.my_id, state.ring_size = int(args[1]), int(args[2])
            state.tuples = json.loads(args[3])
            nxt = state.tuples[(state.my_id + 1) % state.ring_size]
            state.r_ip, state.r_port = nxt[1], int(nxt[2])
        elif cmd == "store":
            t_id, pos, e_id, rec = int(args[1]), int(args[2]), int(args[3]), args[4]
            if state.my_id == t_id:
                if pos not in state.local_hash: state.local_hash[pos] = {}
                state.local_hash[pos][e_id] = rec
            else: send_udp(data, state.r_ip, state.r_port)
        elif cmd == "find-event":
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
                avail = [str(i) for i in range(state.ring_size) if state.tuples[i][0] not in seq]
                if avail:
                    nxt = int(random.choice(avail))
                    seq.append(state.name)
                    send_udp(encode_msg("find-event", e_id, ",".join(seq), s_ip, s_port), state.tuples[nxt][1], state.tuples[nxt][2])
                else: send_udp(encode_msg("FAILURE"), s_ip, s_port)

def main():
    m_ip, m_port = sys.argv[1], int(sys.argv[2])
    name = input("Peer Name: ")
    my_ip = input("Enter THIS Node's LAN IP: ")
    m_p, p_p = int(input("M-Port: ")), int(input("P-Port: "))
    state = PeerState(name, my_ip, m_p, p_p, m_ip, m_port)
    threading.Thread(target=p_port_listener, args=(state,), daemon=True).start()
    while True:
        raw = input(f"[{state.name}] > ").strip().split()
        if not raw: continue
        cmd = raw[0]
        if cmd == "register":
            res = send_udp(encode_msg("register", name, my_ip, m_p, p_p), m_ip, m_port, True)
            if res: print(res[0], flush=True)
        elif cmd == "setup-dht":
            n, y = raw[1], raw[2]
            state.dataset_year = y
            res = send_udp(encode_msg("setup-dht", name, n, y), m_ip, m_port, True)
            if res and res[0] == "SUCCESS":
                state.my_id, state.ring_size = 0, int(n)
                state.tuples = [t.split(',') for t in res[1:]]
                nxt = state.tuples[1 % state.ring_size]
                state.r_ip, state.r_port = nxt[1], int(nxt[2])
                for i in range(1, int(n)):
                    send_udp(encode_msg("set-id", i, n, json.dumps(state.tuples)), state.tuples[i][1], state.tuples[i][2])
                s = get_hash_s(y)
                with open(f"details-{y}.csv", 'r') as f:
                    r = csv.reader(f); next(r)
                    for row in r:
                        eid = int(row[0])
                        pos, tid = eid % s, (eid % s) % state.ring_size
                        if tid == 0:
                            if pos not in state.local_hash: state.local_hash[pos] = {}
                            state.local_hash[pos][eid] = ",".join(row)
                        else: send_udp(encode_msg("store", tid, pos, eid, ",".join(row)), state.r_ip, state.r_port)
                send_udp(encode_msg("dht-complete", name), m_ip, m_port)
                print("DHT initialized and records distributed.", flush=True)
        elif cmd == "query-dht":
            res = send_udp(encode_msg("query-dht", name), m_ip, m_port, True)
            if res and res[0] == "SUCCESS":
                print(f"Entry point identified: {res[1]}. Sending query...", flush=True)
                send_udp(encode_msg("find-event", raw[1], "", my_ip, p_p), res[2], res[3])
        elif cmd == "exit": break

if __name__ == "__main__": main()