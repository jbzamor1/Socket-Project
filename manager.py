import socket
import sys
import json
import random

def encode_msg(*args):
    return "|".join(str(a) for a in args).encode('utf-8')

def decode_msg(data):
    return data.decode('utf-8').split("|")

class Manager:
    def __init__(self):
        self.peers = {} # {name: PeerInfo}

    def handle_msg(self, data, addr, sock):
        args = decode_msg(data)
        cmd = args[0]

        if cmd == "register":
            name, ip, m_port, p_port = args[1], args[2], args[3], args[4]
            self.peers[name] = {"ip": ip, "m_port": m_port, "p_port": p_port, "name": name}
            print(f"Registered {name} [{ip}:{m_port}]", flush=True)
            sock.sendto(encode_msg("SUCCESS"), addr)

        elif cmd == "setup-dht":
            leader_name, n, year = args[1], int(args[2]), args[3]
            if len(self.peers) < n:
                sock.sendto(encode_msg("FAILURE", "Not enough peers"), addr)
            else:
                selected = list(self.peers.values())[:n]
                tup_list = [[p["name"], p["ip"], p["p_port"]] for p in selected]
                msg = ["SUCCESS"] + [",".join(map(str, t)) for t in tup_list]
                sock.sendto(encode_msg(*msg), addr)
                print(f"DHT setup initiated by {leader_name} for {n} nodes using {year} data.", flush=True)

        elif cmd == "query-dht":
            if not self.peers:
                sock.sendto(encode_msg("FAILURE"), addr)
            else:
                # Pick a random peer to act as the entry point for the query
                p = random.choice(list(self.peers.values()))
                sock.sendto(encode_msg("SUCCESS", p["name"], p["ip"], p["p_port"]), addr)

        elif cmd == "dht-complete":
            print(f"DHT distribution completed by {args[1]}.", flush=True)
            sock.sendto(encode_msg("SUCCESS"), addr)

        elif cmd == "deregister":
            name = args[1]
            if name in self.peers:
                del self.peers[name]
                print(f"Deregistered {name}.", flush=True)
            sock.sendto(encode_msg("SUCCESS"), addr)

def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", port))
    mgr = Manager()
    print(f"Manager listening on port {port}...", flush=True)

    while True:
        data, addr = sock.recvfrom(65535)
        mgr.handle_msg(data, addr, sock)

if __name__ == "__main__":
    main()