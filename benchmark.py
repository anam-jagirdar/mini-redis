"""Measure throughput:  python benchmark.py

Runs a server in a background thread and times SET/GET with and without pipelining.
Client and server share one Python process (and one CPU's worth of GIL), so the numbers are
conservative; treat them as a relative comparison, not an absolute record.
"""
import socket
import time

from miniredis import ThreadedServer
from miniredis.protocol import encode, parse_value

N = 20_000
BATCH = 100


def read_replies(sock, buf, count):
    seen = 0
    while seen < count:
        while True:
            parsed = parse_value(buf)
            if parsed is None:
                break
            del buf[: parsed[1]]
            seen += 1
            if seen == count:
                return
        buf += sock.recv(262144)


def bench(sock, make_command, pipeline):
    buf = bytearray()
    start = time.perf_counter()
    for base in range(0, N, pipeline):
        n = min(pipeline, N - base)
        sock.sendall(b"".join(encode(make_command(base + i)) for i in range(n)))
        read_replies(sock, buf, n)
    return N / (time.perf_counter() - start)


def main():
    with ThreadedServer() as server:
        sock = socket.create_connection((server.host, server.port))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        set_cmd = lambda i: [b"SET", b"key:%d" % i, b"value"]
        get_cmd = lambda i: [b"GET", b"key:%d" % i]
        print(f"{N:,} commands per test, one connection\n")
        print(f"{'':<26}{'ops/sec':>12}")
        for label, cmd, pipe in (
            ("SET, one at a time", set_cmd, 1),
            ("GET, one at a time", get_cmd, 1),
            (f"SET, pipelined x{BATCH}", set_cmd, BATCH),
            (f"GET, pipelined x{BATCH}", get_cmd, BATCH),
        ):
            print(f"{label:<26}{bench(sock, cmd, pipe):>12,.0f}")


if __name__ == "__main__":
    main()
