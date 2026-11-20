"""Every command the server understands.

A command is a function `fn(ctx, *args)` that returns a reply (see protocol.py).
The `@command(name, arity)` decorator registers it. Arity counts the command name too:
    arity 3   means exactly 3 words, e.g. `GET`-style `SETNX key value` has arity 3
    arity -3  means at least 3 words, e.g. `SET key value [options...]`
Error messages copy real Redis, so existing clients and habits keep working.
"""
from __future__ import annotations

import re
import time
from collections import deque
from dataclasses import dataclass, field
from itertools import islice

from . import persistence
from .protocol import Err, Simple
from .store import Store, WrongType

OK = Simple("OK")
INT_MIN, INT_MAX = -(2**63), 2**63 - 1
_INTEGER = re.compile(rb"0|-?[1-9]\d*")  # no "+5", no "007", like Redis


class CommandError(Exception):
    """Becomes an error reply such as `-ERR syntax error`."""

    def __init__(self, message: str, prefix: str = "ERR") -> None:
        super().__init__(message)
        self.prefix = prefix


def _syntax_error() -> CommandError:
    return CommandError("syntax error")


@dataclass
class Context:
    store: Store
    snapshot_path: str | None = None
    started_at: float = field(default_factory=time.time)
    commands_processed: int = 0
    connected_clients: int = 0


@dataclass(frozen=True)
class Command:
    name: str
    fn: object
    arity: int


COMMANDS: dict[str, Command] = {}


def command(name: str, arity: int):
    def register(fn):
        COMMANDS[name] = Command(name, fn, arity)
        return fn

    return register


def execute(ctx: Context, args: list[bytes]):
    """Run one command line (already split into words) and return the reply."""
    ctx.commands_processed += 1
    name = args[0].decode("latin-1").upper()
    cmd = COMMANDS.get(name)
    if cmd is None:
        return Err(f"ERR unknown command '{args[0].decode('utf-8', 'replace')[:64]}'")
    count = len(args)
    if (cmd.arity >= 0 and count != cmd.arity) or (cmd.arity < 0 and count < -cmd.arity):
        return Err(f"ERR wrong number of arguments for '{name.lower()}' command")
    try:
        return cmd.fn(ctx, *args[1:])
    except WrongType:
        return Err("WRONGTYPE Operation against a key holding the wrong kind of value")
    except CommandError as exc:
        return Err(f"{exc.prefix} {exc}")


def to_int(raw: bytes) -> int:
    if not _INTEGER.fullmatch(raw) or not INT_MIN <= int(raw) <= INT_MAX:
        raise CommandError("value is not an integer or out of range")
    return int(raw)


# ------------------------------------------------------------------------------ connection
@command("PING", -1)
def ping(ctx, *message):
    if len(message) > 1:
        raise CommandError("wrong number of arguments for 'ping' command")
    return message[0] if message else Simple("PONG")


@command("ECHO", 2)
def echo(ctx, message):
    return message


@command("QUIT", -1)
def quit_(ctx, *_):
    return OK  # the server closes the connection after sending this


# ---------------------------------------------------------------------------------- strings
@command("SET", -3)
def set_(ctx, key, value, *options):
    nx = xx = keep_ttl = False
    ttl_ms = None
    i = 0
    while i < len(options):
        option = options[i].upper()
        if option == b"NX":
            nx = True
        elif option == b"XX":
            xx = True
        elif option == b"KEEPTTL":
            keep_ttl = True
        elif option in (b"EX", b"PX") and ttl_ms is None:
            i += 1
            if i == len(options):
                raise _syntax_error()
            amount = to_int(options[i])
            if amount <= 0:
                raise CommandError("invalid expire time in 'set' command")
            ttl_ms = amount * (1000 if option == b"EX" else 1)
        else:
            raise _syntax_error()
        i += 1
    if (nx and xx) or (keep_ttl and ttl_ms is not None):
        raise _syntax_error()

    store = ctx.store
    present = store.exists(key)
    if (nx and present) or (xx and not present):
        return None
    deadline = store.now_ms() + ttl_ms if ttl_ms else None
    store.set(key, value, expire_at_ms=deadline, keep_ttl=keep_ttl)
    return OK


@command("GET", 2)
def get(ctx, key):
    return ctx.store.get(key, bytes)


@command("MSET", -3)
def mset(ctx, *pairs):
    if len(pairs) % 2:
        raise CommandError("wrong number of arguments for 'mset' command")
    for key, value in zip(pairs[::2], pairs[1::2]):
        ctx.store.set(key, value)
    return OK


@command("MGET", -2)
def mget(ctx, *keys):
    values = (ctx.store.get(key) for key in keys)
    return [value if isinstance(value, bytes) else None for value in values]  # wrong types read as nil


def _add(ctx, key, delta):
    raw = ctx.store.get(key, bytes)
    new = (0 if raw is None else to_int(raw)) + delta
    if not INT_MIN <= new <= INT_MAX:
        raise CommandError("increment or decrement would overflow")
    ctx.store.set(key, str(new).encode(), keep_ttl=True)
    return new


@command("INCR", 2)
def incr(ctx, key):
    return _add(ctx, key, 1)


@command("DECR", 2)
def decr(ctx, key):
    return _add(ctx, key, -1)


@command("INCRBY", 3)
def incrby(ctx, key, amount):
    return _add(ctx, key, to_int(amount))


@command("DECRBY", 3)
def decrby(ctx, key, amount):
    return _add(ctx, key, -to_int(amount))


@command("APPEND", 3)
def append(ctx, key, value):
    new = (ctx.store.get(key, bytes) or b"") + value
    ctx.store.set(key, new, keep_ttl=True)
    return len(new)


@command("STRLEN", 2)
def strlen(ctx, key):
    value = ctx.store.get(key, bytes)
    return 0 if value is None else len(value)


# ------------------------------------------------------------------------------------ keys
@command("DEL", -2)
def del_(ctx, *keys):
    deleted = 0
    for key in keys:
        if ctx.store.exists(key):
            ctx.store.delete(key)
            deleted += 1
    return deleted


@command("EXISTS", -2)
def exists(ctx, *keys):
    return sum(ctx.store.exists(key) for key in keys)  # repeated keys count repeatedly, like Redis


def _expire(ctx, key, amount, unit_ms):
    ms = to_int(amount) * unit_ms
    if not ctx.store.exists(key):
        return 0
    if ms <= 0:  # a deadline in the past deletes the key immediately
        ctx.store.delete(key)
        return 1
    return int(ctx.store.set_expiry(key, ctx.store.now_ms() + ms))


@command("EXPIRE", 3)
def expire(ctx, key, seconds):
    return _expire(ctx, key, seconds, 1000)


@command("PEXPIRE", 3)
def pexpire(ctx, key, milliseconds):
    return _expire(ctx, key, milliseconds, 1)


@command("TTL", 2)
def ttl(ctx, key):
    ms = ctx.store.ttl_ms(key)
    return ms if ms < 0 else (ms + 500) // 1000  # rounds to the nearest second, like Redis


@command("PTTL", 2)
def pttl(ctx, key):
    return ctx.store.ttl_ms(key)


@command("PERSIST", 2)
def persist(ctx, key):
    return int(ctx.store.persist(key))


@command("KEYS", 2)
def keys(ctx, pattern):
    return ctx.store.keys(pattern)


@command("TYPE", 2)
def type_(ctx, key):
    return Simple(ctx.store.type_of(key))


# ----------------------------------------------------------------------------------- lists
def _push(ctx, key, values, left):
    items = ctx.store.get(key, deque)
    if items is None:
        items = deque()
    for value in values:
        (items.appendleft if left else items.append)(value)
    ctx.store.set(key, items, keep_ttl=True)
    return len(items)


def _pop(ctx, key, left):
    items = ctx.store.get(key, deque)
    if items is None:
        return None
    value = items.popleft() if left else items.pop()
    if not items:
        ctx.store.delete(key)
    return value


@command("LPUSH", -3)
def lpush(ctx, key, *values):
    return _push(ctx, key, values, left=True)


@command("RPUSH", -3)
def rpush(ctx, key, *values):
    return _push(ctx, key, values, left=False)


@command("LPOP", 2)
def lpop(ctx, key):
    return _pop(ctx, key, left=True)


@command("RPOP", 2)
def rpop(ctx, key):
    return _pop(ctx, key, left=False)


@command("LLEN", 2)
def llen(ctx, key):
    items = ctx.store.get(key, deque)
    return 0 if items is None else len(items)


@command("LRANGE", 4)
def lrange(ctx, key, start, stop):
    start, stop = to_int(start), to_int(stop)
    items = ctx.store.get(key, deque)
    if items is None:
        return []
    length = len(items)
    if start < 0:
        start = max(length + start, 0)
    if stop < 0:
        stop = length + stop
    stop = min(stop, length - 1)
    if start > stop:
        return []
    return list(islice(items, start, stop + 1))


@command("LINDEX", 3)
def lindex(ctx, key, index):
    index = to_int(index)
    items = ctx.store.get(key, deque)
    if items is None:
        return None
    if index < 0:
        index += len(items)
    return items[index] if 0 <= index < len(items) else None


# ---------------------------------------------------------------------------------- hashes
@command("HSET", -4)
def hset(ctx, key, *pairs):
    if len(pairs) % 2:
        raise CommandError("wrong number of arguments for 'hset' command")
    table = ctx.store.get(key, dict)
    if table is None:
        table = {}
    added = 0
    for name, value in zip(pairs[::2], pairs[1::2]):
        added += name not in table
        table[name] = value
    ctx.store.set(key, table, keep_ttl=True)
    return added


@command("HGET", 3)
def hget(ctx, key, name):
    table = ctx.store.get(key, dict)
    return None if table is None else table.get(name)


@command("HDEL", -3)
def hdel(ctx, key, *names):
    table = ctx.store.get(key, dict)
    if table is None:
        return 0
    removed = sum(table.pop(name, None) is not None for name in names)
    if not table:
        ctx.store.delete(key)
    return removed


@command("HGETALL", 2)
def hgetall(ctx, key):
    table = ctx.store.get(key, dict)
    return [] if table is None else [item for pair in table.items() for item in pair]


@command("HEXISTS", 3)
def hexists(ctx, key, name):
    table = ctx.store.get(key, dict)
    return int(table is not None and name in table)


@command("HLEN", 2)
def hlen(ctx, key):
    table = ctx.store.get(key, dict)
    return 0 if table is None else len(table)


# ------------------------------------------------------------------------------------ sets
@command("SADD", -3)
def sadd(ctx, key, *members):
    members_set = ctx.store.get(key, set)
    if members_set is None:
        members_set = set()
    before = len(members_set)
    members_set.update(members)
    ctx.store.set(key, members_set, keep_ttl=True)
    return len(members_set) - before


@command("SREM", -3)
def srem(ctx, key, *members):
    members_set = ctx.store.get(key, set)
    if members_set is None:
        return 0
    before = len(members_set)
    members_set.difference_update(members)
    if not members_set:
        ctx.store.delete(key)
    return before - len(members_set)


@command("SMEMBERS", 2)
def smembers(ctx, key):
    members_set = ctx.store.get(key, set)
    return [] if members_set is None else list(members_set)


@command("SISMEMBER", 3)
def sismember(ctx, key, member):
    members_set = ctx.store.get(key, set)
    return int(members_set is not None and member in members_set)


@command("SCARD", 2)
def scard(ctx, key):
    members_set = ctx.store.get(key, set)
    return 0 if members_set is None else len(members_set)


# ------------------------------------------------------------------------------ server admin
@command("DBSIZE", 1)
def dbsize(ctx):
    return ctx.store.dbsize()


@command("FLUSHALL", 1)
@command("FLUSHDB", 1)
def flushall(ctx):
    ctx.store.clear()
    return OK


@command("SAVE", 1)
def save(ctx):
    if not ctx.snapshot_path:
        raise CommandError("no snapshot file configured (start the server with --dump FILE)")
    persistence.save(ctx.store, ctx.snapshot_path)
    return OK


@command("INFO", -1)
def info(ctx, *_):
    lines = [
        "# Server",
        "mini_redis_version:1.0.0",
        f"uptime_in_seconds:{int(time.time() - ctx.started_at)}",
        "# Clients",
        f"connected_clients:{ctx.connected_clients}",
        "# Stats",
        f"total_commands_processed:{ctx.commands_processed}",
        "# Keyspace",
        f"db_keys:{ctx.store.dbsize()}",
    ]
    return ("\r\n".join(lines) + "\r\n").encode()
