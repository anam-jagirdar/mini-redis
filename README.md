# mini-redis

A Redis-compatible in-memory database server written from scratch in Python, using only the standard library. It speaks the real Redis wire protocol (RESP), so the official `redis-cli` and standard Redis client libraries can talk to it.

```
$ python -m miniredis --port 6380 --dump data.json
mini-redis listening on 127.0.0.1:6380  (Ctrl+C to stop)

$ redis-cli -p 6380
127.0.0.1:6380> SET session:42 anam EX 30
OK
127.0.0.1:6380> TTL session:42
(integer) 30
127.0.0.1:6380> RPUSH jobs build test deploy
(integer) 3
127.0.0.1:6380> LRANGE jobs 0 -1
1) "build"
2) "test"
3) "deploy"
```

## What it does

| Area | Commands |
|---|---|
| Strings | `SET` (`EX` `PX` `NX` `XX` `KEEPTTL`), `GET`, `MSET`, `MGET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `APPEND`, `STRLEN` |
| Keys | `DEL`, `EXISTS`, `KEYS` (glob patterns), `TYPE`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST` |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX` |
| Hashes | `HSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS`, `HLEN` |
| Sets | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD` |
| Server | `PING`, `ECHO`, `DBSIZE`, `FLUSHALL`, `SAVE`, `INFO`, `QUIT` |

Error messages match real Redis (`WRONGTYPE ...`, `ERR value is not an integer or out of range`, and so on).

## How it works

```
 client bytes ──► protocol.py ──► commands.py ──► store.py
   (TCP)          parse RESP      look up &        dicts + expiry
                  into commands   run command      deadlines
                       ▲                │
                       └── server.py ◄──┘  asyncio event loop, one thread
                              │
                       persistence.py   atomic JSON snapshots
```

- **`protocol.py`**: an incremental RESP parser. TCP delivers bytes in arbitrary chunks, so a command may arrive split across reads or many commands may arrive glued together (pipelining). The parser buffers partial input, handles both real RESP and plain `PING\r\n` inline commands (so you can use `telnet`/`nc`), and rejects malformed or oversized input instead of trusting it.
- **`store.py`**: the data. Keys and string values are `bytes` (binary safe). Expiry uses the same two layers as real Redis: *lazy* (an expired key is deleted the moment something touches it) and *active* (a background task samples keys with deadlines and sweeps them).
- **`commands.py`**: a registry of commands, each declared with its arity. Adding a command is one decorated function.
- **`server.py`**: an `asyncio` server. Like real Redis it executes commands on a single thread, so every command is atomic and the store needs no locks. A concurrency test with 8 clients incrementing one counter checks this.
- **`persistence.py`**: `SAVE` and shutdown write a snapshot. The file is written to a temporary file and then moved over the old one in a single step, so a crash mid-save cannot corrupt the existing snapshot. Keys that expired while the server was off are skipped on load.

## Run it

Python 3.10+ and nothing to install.

```bash
python -m miniredis                         # listens on 127.0.0.1:6380
python -m miniredis --port 7000 --dump data.json   # snapshot loaded at start, saved on exit and on SAVE
```

Talk to it with `redis-cli -p 6380`, with `nc 127.0.0.1 6380`, or from Python:

```python
from miniredis import Client

with Client("127.0.0.1", 6380) as r:
    r.set("greeting", "hello")
    print(r.get("greeting"))          # b'hello'
    r.execute("LRANGE", "jobs", 0, -1)
```

## Tests

```bash
python -m unittest discover -s tests -v
```

44 tests cover the protocol (including input delivered one byte at a time and malformed input), every command, expiry using a fake clock, snapshots, and the real server over real sockets (concurrency, pipelining, 2 MB values, restart persistence).

I also checked the server by hand with the real `redis-cli` (`PING`, `SET`, `GET`, `RPUSH`, `LRANGE`, `HSET`, `HGETALL`, `TTL`, error replies).

## Performance

`python benchmark.py` on the machine this was developed on, one connection, client and server in the same Python process:

| | ops/sec |
|---|---|
| `SET`, one at a time | ~11,000 |
| `GET`, one at a time | ~12,000 |
| `SET`, pipelined (100 per batch) | ~149,000 |
| `GET`, pipelined (100 per batch) | ~167,000 |

One-at-a-time speed is limited by network round trips; pipelining shows what the server itself can handle. Real Redis is far faster (it is written in C). Your numbers will differ by machine.

## Limitations

This is a learning project, not a production database.

- Only the commands in the table above exist. No transactions (`MULTI`), pub/sub, blocking pops, sorted sets, or replication.
- One database, no authentication, no TLS. It binds to `127.0.0.1` by default; don't expose it to a network.
- Persistence is snapshot-only, so writes since the last `SAVE` are lost if the process is killed (a clean shutdown saves).
- `KEYS` and `SAVE` scan everything and block the server while they run, as in real Redis.
- No memory limit or eviction.
