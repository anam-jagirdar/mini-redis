"""RESP, the Redis wire protocol: encoding replies and incrementally parsing bytes.

Replies are plain Python values:
    None            -> null            ($-1)
    int             -> integer         (:42)
    bytes / str     -> bulk string     ($3 foo)
    list / tuple    -> array           (*2 ...)
    Simple("OK")    -> simple string   (+OK)
    Err("ERR ...")  -> error           (-ERR ...)
"""
from __future__ import annotations

import re
from collections.abc import Iterator

CRLF = b"\r\n"
MAX_BULK = 64 * 1024 * 1024  # largest single value accepted
MAX_ARGS = 1_000_000  # most arguments in one command
MAX_LINE = 64 * 1024  # longest header line or inline command
MAX_DEPTH = 32  # deepest nesting of arrays the client-side parser accepts

_INTEGER = re.compile(rb"-?\d+")


class ProtocolError(Exception):
    """The peer sent bytes that are not valid RESP."""


class Simple(str):
    """A RESP simple string such as +OK."""


class Err(str):
    """A RESP error such as -ERR unknown command."""


def encode(value) -> bytes:
    if value is None:
        return b"$-1\r\n"
    if isinstance(value, Simple):  # these two subclass str, so they must be checked first
        return b"+" + value.encode() + CRLF
    if isinstance(value, Err):
        return b"-" + value.encode() + CRLF
    if isinstance(value, int):
        return b":%d\r\n" % value
    if isinstance(value, str):
        value = value.encode()
    if isinstance(value, (bytes, bytearray)):
        return b"$%d\r\n%s\r\n" % (len(value), value)
    if isinstance(value, (list, tuple)):
        return b"*%d\r\n" % len(value) + b"".join(encode(item) for item in value)
    raise TypeError(f"cannot encode {type(value).__name__}")


def _integer(line: bytes) -> int:
    if not _INTEGER.fullmatch(line):
        raise ProtocolError("invalid integer")
    return int(line)


def parse_value(buf, pos: int = 0, depth: int = 0):
    """Parse one RESP value that starts at `pos`.

    Returns (value, next_pos), or None when the buffer does not yet hold a whole value.
    (A null reply comes back as (None, next_pos), so the two cases cannot be confused.)
    """
    if depth > MAX_DEPTH:
        raise ProtocolError("nesting too deep")
    if pos >= len(buf):
        return None
    kind = bytes(buf[pos : pos + 1])
    end = buf.find(CRLF, pos)
    if end == -1:
        if len(buf) - pos > MAX_LINE:
            raise ProtocolError("line too long")
        return None
    line, after = bytes(buf[pos + 1 : end]), end + 2

    if kind == b"+":
        return Simple(line.decode()), after
    if kind == b"-":
        return Err(line.decode()), after
    if kind == b":":
        return _integer(line), after
    if kind == b"$":
        size = _integer(line)
        if size == -1:
            return None, after
        if size < 0 or size > MAX_BULK:
            raise ProtocolError("invalid bulk length")
        if len(buf) < after + size + 2:
            return None
        if buf[after + size : after + size + 2] != CRLF:
            raise ProtocolError("bulk string not terminated by CRLF")
        return bytes(buf[after : after + size]), after + size + 2
    if kind == b"*":
        count = _integer(line)
        if count == -1:
            return None, after
        if count < 0 or count > MAX_ARGS:
            raise ProtocolError("invalid multibulk length")
        items, cursor = [], after
        for _ in range(count):
            result = parse_value(buf, cursor, depth + 1)
            if result is None:
                return None
            value, cursor = result
            items.append(value)
        return items, cursor
    raise ProtocolError(f"unknown type byte {kind!r}")


def _parse_command(buf):
    """Parse one `*N` array of bulk strings. Returns (args, bytes_used) or None if incomplete."""
    end = buf.find(CRLF)
    if end == -1:
        if len(buf) > MAX_LINE:
            raise ProtocolError("invalid multibulk length")
        return None
    count = _integer(bytes(buf[1:end]))
    if count > MAX_ARGS:
        raise ProtocolError("invalid multibulk length")
    pos = end + 2
    args = []
    for _ in range(max(count, 0)):
        if pos >= len(buf):
            return None
        if buf[pos : pos + 1] != b"$":
            raise ProtocolError(f"expected '$', got '{chr(buf[pos])}'")
        end = buf.find(CRLF, pos)
        if end == -1:
            if len(buf) - pos > MAX_LINE:
                raise ProtocolError("invalid bulk length")
            return None
        size = _integer(bytes(buf[pos + 1 : end]))
        if size < 0 or size > MAX_BULK:
            raise ProtocolError("invalid bulk length")
        start = end + 2
        if len(buf) < start + size + 2:
            return None
        if buf[start + size : start + size + 2] != CRLF:
            raise ProtocolError("bulk string not terminated by CRLF")
        args.append(bytes(buf[start : start + size]))
        pos = start + size + 2
    return args, pos


class CommandParser:
    """Turns a stream of bytes from a client into commands, however the bytes are chunked.

    Feed it whatever `recv` returned, then iterate `commands()`. Partial commands stay
    buffered until the rest arrives. Both the real protocol (`*2\\r\\n$4\\r\\nPING...`)
    and plain "inline" commands typed into telnet (`PING\\r\\n`) are understood.
    """

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, data: bytes) -> None:
        self._buf += data

    def commands(self) -> Iterator[list[bytes]]:
        buf = self._buf
        while buf:
            if buf[0:1] == b"*":
                parsed = _parse_command(buf)
                if parsed is None:
                    return
                args, used = parsed
                del buf[:used]
                if args:
                    yield args
            else:
                end = buf.find(b"\n")
                if end == -1:
                    if len(buf) > MAX_LINE:
                        raise ProtocolError("too big inline request")
                    return
                line = bytes(buf[:end])
                del buf[: end + 1]
                args = line.split()
                if args:
                    yield args
