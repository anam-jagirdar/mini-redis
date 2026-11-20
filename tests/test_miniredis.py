import os
import socket
import tempfile
import time
import unittest

from miniredis import Client, ResponseError, Store, ThreadedServer
from miniredis import persistence
from miniredis.commands import Context, execute
from miniredis.protocol import CommandParser, Err, ProtocolError, Simple, encode, parse_value


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def advance(self, seconds):
        self.t += seconds


def run(ctx, line):
    return execute(ctx, [w.encode() for w in line.split()])


class ProtocolTests(unittest.TestCase):
    def test_encode_each_type(self):
        self.assertEqual(encode(Simple("OK")), b"+OK\r\n")
        self.assertEqual(encode(Err("ERR x")), b"-ERR x\r\n")
        self.assertEqual(encode(42), b":42\r\n")
        self.assertEqual(encode(b"foo"), b"$3\r\nfoo\r\n")
        self.assertEqual(encode(None), b"$-1\r\n")
        self.assertEqual(encode([b"a", 1]), b"*2\r\n$1\r\na\r\n:1\r\n")

    def test_parse_round_trip(self):
        value = [b"a", 7, [b"nested", None]]
        parsed, used = parse_value(bytearray(encode(value)))
        self.assertEqual(parsed, value)
        self.assertEqual(used, len(encode(value)))

    def test_incomplete_returns_none(self):
        self.assertIsNone(parse_value(bytearray(b"$5\r\nhel")))

    def test_null_is_distinct_from_incomplete(self):
        self.assertEqual(parse_value(bytearray(b"$-1\r\n")), (None, 5))

    def test_parser_handles_split_chunks(self):
        parser = CommandParser()
        data = encode([b"SET", b"k", b"value"])
        out = []
        for i in range(len(data)):  # one byte at a time
            parser.feed(data[i : i + 1])
            out.extend(parser.commands())
        self.assertEqual(out, [[b"SET", b"k", b"value"]])

    def test_parser_pipelined_and_inline(self):
        parser = CommandParser()
        parser.feed(encode([b"PING"]) + b"ECHO hi\r\n" + encode([b"GET", b"x"]))
        self.assertEqual(list(parser.commands()), [[b"PING"], [b"ECHO", b"hi"], [b"GET", b"x"]])

    def test_binary_safe_values(self):
        parser = CommandParser()
        payload = b"a\r\nb\x00c"
        parser.feed(encode([b"SET", b"k", payload]))
        self.assertEqual(list(parser.commands())[0][2], payload)

    def test_malformed_input_raises(self):
        for bad in (b"*x\r\n", b"*1\r\n+oops\r\n", b"*1\r\n$3\r\nabcde\r\n", b"*1\r\n$-5\r\n"):
            parser = CommandParser()
            parser.feed(bad)
            with self.assertRaises(ProtocolError, msg=bad):
                list(parser.commands())

    def test_huge_declared_sizes_rejected(self):
        parser = CommandParser()
        parser.feed(b"*999999999999\r\n")
        with self.assertRaises(ProtocolError):
            list(parser.commands())


class CommandTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.ctx = Context(Store(clock=self.clock))

    def test_set_get_and_missing(self):
        self.assertEqual(run(self.ctx, "SET a 1"), "OK")
        self.assertEqual(run(self.ctx, "GET a"), b"1")
        self.assertIsNone(run(self.ctx, "GET nope"))

    def test_commands_are_case_insensitive(self):
        run(self.ctx, "set a 1")
        self.assertEqual(run(self.ctx, "gEt a"), b"1")

    def test_unknown_command_and_arity(self):
        self.assertTrue(run(self.ctx, "NOPE").startswith("ERR unknown command"))
        self.assertTrue(run(self.ctx, "GET").startswith("ERR wrong number of arguments for 'get'"))
        self.assertTrue(run(self.ctx, "GET a b").startswith("ERR wrong number"))

    def test_set_nx_xx(self):
        self.assertEqual(run(self.ctx, "SET a 1 NX"), "OK")
        self.assertIsNone(run(self.ctx, "SET a 2 NX"))
        self.assertEqual(run(self.ctx, "GET a"), b"1")
        self.assertIsNone(run(self.ctx, "SET b 1 XX"))
        self.assertEqual(run(self.ctx, "SET a 3 XX"), "OK")
        self.assertTrue(run(self.ctx, "SET a 1 NX XX").startswith("ERR syntax"))

    def test_set_ex_and_expiry(self):
        run(self.ctx, "SET a 1 EX 10")
        self.assertEqual(run(self.ctx, "TTL a"), 10)
        self.clock.advance(9.9)
        self.assertEqual(run(self.ctx, "GET a"), b"1")
        self.clock.advance(0.2)
        self.assertIsNone(run(self.ctx, "GET a"))
        self.assertEqual(run(self.ctx, "TTL a"), -2)

    def test_set_rejects_bad_expire(self):
        self.assertTrue(run(self.ctx, "SET a 1 EX 0").startswith("ERR invalid expire"))
        self.assertTrue(run(self.ctx, "SET a 1 EX abc").startswith("ERR value is not an integer"))
        self.assertTrue(run(self.ctx, "SET a 1 EX").startswith("ERR syntax"))

    def test_plain_set_clears_ttl_but_keepttl_keeps_it(self):
        run(self.ctx, "SET a 1 EX 100")
        run(self.ctx, "SET a 2 KEEPTTL")
        self.assertEqual(run(self.ctx, "TTL a"), 100)
        run(self.ctx, "SET a 3")
        self.assertEqual(run(self.ctx, "TTL a"), -1)

    def test_expire_persist_ttl(self):
        run(self.ctx, "SET a 1")
        self.assertEqual(run(self.ctx, "TTL a"), -1)
        self.assertEqual(run(self.ctx, "EXPIRE a 5"), 1)
        self.assertEqual(run(self.ctx, "PERSIST a"), 1)
        self.assertEqual(run(self.ctx, "TTL a"), -1)
        self.assertEqual(run(self.ctx, "EXPIRE missing 5"), 0)
        self.assertEqual(run(self.ctx, "EXPIRE a -1"), 1)  # past deadline deletes
        self.assertEqual(run(self.ctx, "EXISTS a"), 0)

    def test_incr_family(self):
        self.assertEqual(run(self.ctx, "INCR n"), 1)
        self.assertEqual(run(self.ctx, "INCRBY n 10"), 11)
        self.assertEqual(run(self.ctx, "DECR n"), 10)
        self.assertEqual(run(self.ctx, "DECRBY n 15"), -5)
        run(self.ctx, "SET s abc")
        self.assertTrue(run(self.ctx, "INCR s").startswith("ERR value is not an integer"))
        run(self.ctx, "SET big 9223372036854775807")
        self.assertTrue(run(self.ctx, "INCR big").startswith("ERR increment or decrement would overflow"))

    def test_incr_keeps_ttl(self):
        run(self.ctx, "SET n 1 EX 50")
        run(self.ctx, "INCR n")
        self.assertEqual(run(self.ctx, "TTL n"), 50)

    def test_append_strlen_mset_mget(self):
        self.assertEqual(run(self.ctx, "APPEND a hel"), 3)
        self.assertEqual(run(self.ctx, "APPEND a lo"), 5)
        self.assertEqual(run(self.ctx, "STRLEN a"), 5)
        run(self.ctx, "MSET x 1 y 2")
        self.assertEqual(run(self.ctx, "MGET x nope y"), [b"1", None, b"2"])
        self.assertTrue(run(self.ctx, "MSET x 1 y").startswith("ERR wrong number"))

    def test_del_exists_type_keys(self):
        run(self.ctx, "MSET user:1 a user:2 b other c")
        run(self.ctx, "RPUSH l 1")
        self.assertEqual(run(self.ctx, "EXISTS user:1 user:1 nope"), 2)
        self.assertEqual(sorted(run(self.ctx, "KEYS user:*")), [b"user:1", b"user:2"])
        self.assertEqual(run(self.ctx, "TYPE l"), "list")
        self.assertEqual(run(self.ctx, "TYPE nope"), "none")
        self.assertEqual(run(self.ctx, "DEL user:1 nope"), 1)
        self.assertEqual(run(self.ctx, "DBSIZE"), 3)

    def test_lists(self):
        self.assertEqual(run(self.ctx, "RPUSH l a b c"), 3)
        self.assertEqual(run(self.ctx, "LPUSH l z"), 4)
        self.assertEqual(run(self.ctx, "LRANGE l 0 -1"), [b"z", b"a", b"b", b"c"])
        self.assertEqual(run(self.ctx, "LRANGE l -2 -1"), [b"b", b"c"])
        self.assertEqual(run(self.ctx, "LRANGE l 5 10"), [])
        self.assertEqual(run(self.ctx, "LRANGE l 0 100"), [b"z", b"a", b"b", b"c"])
        self.assertEqual(run(self.ctx, "LINDEX l -1"), b"c")
        self.assertIsNone(run(self.ctx, "LINDEX l 9"))
        self.assertEqual(run(self.ctx, "LPOP l"), b"z")
        self.assertEqual(run(self.ctx, "RPOP l"), b"c")
        self.assertEqual(run(self.ctx, "LLEN l"), 2)

    def test_empty_list_key_disappears(self):
        run(self.ctx, "RPUSH l a")
        run(self.ctx, "LPOP l")
        self.assertEqual(run(self.ctx, "EXISTS l"), 0)
        self.assertIsNone(run(self.ctx, "LPOP l"))

    def test_hashes(self):
        self.assertEqual(run(self.ctx, "HSET h a 1 b 2"), 2)
        self.assertEqual(run(self.ctx, "HSET h a 9"), 0)
        self.assertEqual(run(self.ctx, "HGET h a"), b"9")
        self.assertEqual(run(self.ctx, "HLEN h"), 2)
        self.assertEqual(run(self.ctx, "HEXISTS h b"), 1)
        self.assertEqual(sorted(run(self.ctx, "HGETALL h")), [b"1" if False else b"2", b"9", b"a", b"b"])
        self.assertEqual(run(self.ctx, "HDEL h a b"), 2)
        self.assertEqual(run(self.ctx, "EXISTS h"), 0)

    def test_sets(self):
        self.assertEqual(run(self.ctx, "SADD s a b a"), 2)
        self.assertEqual(run(self.ctx, "SISMEMBER s a"), 1)
        self.assertEqual(run(self.ctx, "SCARD s"), 2)
        self.assertEqual(sorted(run(self.ctx, "SMEMBERS s")), [b"a", b"b"])
        self.assertEqual(run(self.ctx, "SREM s a b"), 2)
        self.assertEqual(run(self.ctx, "EXISTS s"), 0)

    def test_wrong_type(self):
        run(self.ctx, "RPUSH l a")
        for line in ("GET l", "HGET l a", "SADD l x", "INCR l", "APPEND l x"):
            self.assertTrue(run(self.ctx, line).startswith("WRONGTYPE"), line)
        run(self.ctx, "SET s 1")
        self.assertTrue(run(self.ctx, "LPUSH s a").startswith("WRONGTYPE"))

    def test_active_expiry_sweeps_untouched_keys(self):
        for i in range(50):
            run(self.ctx, f"SET k{i} v PX 100")
        self.clock.advance(1)
        removed = self.ctx.store.purge_expired(sample=100)
        self.assertEqual(removed, 50)
        self.assertEqual(len(self.ctx.store.data), 0)

    def test_flushall_and_info(self):
        run(self.ctx, "SET a 1")
        run(self.ctx, "FLUSHALL")
        self.assertEqual(run(self.ctx, "DBSIZE"), 0)
        self.assertIn(b"db_keys:0", run(self.ctx, "INFO"))

    def test_ping_echo(self):
        self.assertEqual(run(self.ctx, "PING"), "PONG")
        self.assertEqual(run(self.ctx, "PING hi"), b"hi")
        self.assertEqual(run(self.ctx, "ECHO hi"), b"hi")


class PersistenceTests(unittest.TestCase):
    def test_round_trip_all_types_with_binary_data(self):
        clock = FakeClock()
        ctx = Context(Store(clock=clock))
        execute(ctx, [b"SET", b"s", b"a\r\n\x00\xff"])
        execute(ctx, [b"SET", b"t", b"1", b"EX", b"100"])
        execute(ctx, [b"RPUSH", b"l", b"1", b"2"])
        execute(ctx, [b"HSET", b"h", b"f", b"v"])
        execute(ctx, [b"SADD", b"z", b"m"])
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "dump.json")
            self.assertEqual(persistence.save(ctx.store, path), 5)
            fresh = Context(Store(clock=clock))
            self.assertEqual(persistence.load(fresh.store, path), 5)
            self.assertEqual(execute(fresh, [b"GET", b"s"]), b"a\r\n\x00\xff")
            self.assertEqual(execute(fresh, [b"LRANGE", b"l", b"0", b"-1"]), [b"1", b"2"])
            self.assertEqual(execute(fresh, [b"HGET", b"h", b"f"]), b"v")
            self.assertEqual(execute(fresh, [b"SISMEMBER", b"z", b"m"]), 1)
            self.assertEqual(execute(fresh, [b"TTL", b"t"]), 100)

    def test_keys_expired_while_offline_are_skipped(self):
        clock = FakeClock()
        store = Store(clock=clock)
        store.set(b"a", b"1", expire_at_ms=store.now_ms() + 5000)
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "dump.json")
            persistence.save(store, path)
            clock.advance(10)
            self.assertEqual(persistence.load(Store(clock=clock), path), 0)

    def test_missing_file_is_fine_and_corrupt_file_errors(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(persistence.load(Store(), os.path.join(d, "nope.json")), 0)
            bad = os.path.join(d, "bad.json")
            with open(bad, "w") as f:
                f.write("{not json")
            with self.assertRaises(persistence.PersistenceError):
                persistence.load(Store(), bad)

    def test_no_temp_files_left_behind(self):
        with tempfile.TemporaryDirectory() as d:
            persistence.save(Store(), os.path.join(d, "dump.json"))
            self.assertEqual(os.listdir(d), ["dump.json"])


class ServerTests(unittest.TestCase):
    def test_client_round_trip_over_real_socket(self):
        with ThreadedServer() as server, Client(server.host, server.port) as c:
            self.assertEqual(c.ping(), "PONG")
            self.assertEqual(c.set("k", "v"), "OK")
            self.assertEqual(c.get("k"), b"v")
            self.assertEqual(c.incr("n"), 1)
            self.assertEqual(c.rpush("l", "a", "b"), 2)
            self.assertEqual(c.lrange("l", 0, -1), [b"a", b"b"])
            with self.assertRaises(ResponseError):
                c.execute("LPUSH", "k", "x")

    def test_clients_share_one_dataset(self):
        with ThreadedServer() as server:
            with Client(server.host, server.port) as a, Client(server.host, server.port) as b:
                a.set("shared", "1")
                self.assertEqual(b.get("shared"), b"1")

    def test_concurrent_increments_are_atomic(self):
        import threading

        with ThreadedServer() as server:
            def worker():
                with Client(server.host, server.port) as c:
                    for _ in range(200):
                        c.incr("counter")

            threads = [threading.Thread(target=worker) for _ in range(8)]
            [t.start() for t in threads]
            [t.join() for t in threads]
            with Client(server.host, server.port) as c:
                self.assertEqual(c.get("counter"), b"1600")

    def test_pipelining(self):
        with ThreadedServer() as server:
            sock = socket.create_connection((server.host, server.port), timeout=5)
            sock.sendall(b"".join(encode([b"INCR", b"n"]) for _ in range(100)))
            buf = bytearray()
            while buf.count(b"\r\n") < 100:
                buf += sock.recv(65536)
            self.assertTrue(bytes(buf).endswith(b":100\r\n"))
            sock.close()

    def test_inline_commands_like_telnet(self):
        with ThreadedServer() as server:
            sock = socket.create_connection((server.host, server.port), timeout=5)
            sock.sendall(b"SET greeting hello\r\nGET greeting\r\n")
            time.sleep(0.2)
            self.assertEqual(sock.recv(1024), b"+OK\r\n$5\r\nhello\r\n")
            sock.close()

    def test_protocol_error_closes_connection_but_server_survives(self):
        with ThreadedServer() as server:
            sock = socket.create_connection((server.host, server.port), timeout=5)
            sock.sendall(b"*1\r\n+bad\r\n")
            self.assertTrue(sock.recv(1024).startswith(b"-ERR Protocol error"))
            self.assertEqual(sock.recv(1024), b"")  # closed
            sock.close()
            with Client(server.host, server.port) as c:
                self.assertEqual(c.ping(), "PONG")

    def test_quit_closes(self):
        with ThreadedServer() as server:
            sock = socket.create_connection((server.host, server.port), timeout=5)
            sock.sendall(encode([b"QUIT"]))
            self.assertEqual(sock.recv(100), b"+OK\r\n")
            self.assertEqual(sock.recv(100), b"")
            sock.close()

    def test_large_value(self):
        with ThreadedServer() as server, Client(server.host, server.port) as c:
            blob = os.urandom(2_000_000)
            c.set("blob", blob)
            self.assertEqual(c.get("blob"), blob)

    def test_expiry_in_real_time(self):
        with ThreadedServer() as server, Client(server.host, server.port) as c:
            c.execute("SET", "k", "v", "PX", "100")
            self.assertEqual(c.get("k"), b"v")
            time.sleep(0.25)
            self.assertIsNone(c.get("k"))

    def test_snapshot_survives_restart(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "dump.json")
            with ThreadedServer(snapshot_path=path) as server, Client(server.host, server.port) as c:
                c.set("persisted", "yes")
            self.assertTrue(os.path.exists(path))  # written on shutdown
            with ThreadedServer(snapshot_path=path) as server, Client(server.host, server.port) as c:
                self.assertEqual(c.get("persisted"), b"yes")
                self.assertEqual(c.save(), "OK")

    def test_save_without_dump_file_is_an_error(self):
        with ThreadedServer() as server, Client(server.host, server.port) as c:
            with self.assertRaises(ResponseError):
                c.save()


if __name__ == "__main__":
    unittest.main()
