"""A tiny blocking client, useful for scripts and tests.

    client = Client("127.0.0.1", 6380)
    client.set("greeting", "hello")        # any command name works as a method
    client.get("greeting")                 # b'hello'
    client.execute("LRANGE", "queue", 0, -1)
"""
from __future__ import annotations

import socket

from .protocol import Err, encode, parse_value


class ResponseError(Exception):
    """The server replied with an error, such as WRONGTYPE or ERR syntax error."""


class Client:
    def __init__(self, host: str = "127.0.0.1", port: int = 6380, timeout: float = 5.0) -> None:
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._buf = bytearray()

    def execute(self, *args):
        words = [str(arg) if isinstance(arg, int) else arg for arg in args]
        self._sock.sendall(encode(words))
        return self._read_reply()

    def _read_reply(self):
        while True:
            parsed = parse_value(self._buf)
            if parsed is not None:
                value, used = parsed
                del self._buf[:used]
                if isinstance(value, Err):
                    raise ResponseError(str(value))
                return value
            chunk = self._sock.recv(65536)
            if not chunk:
                raise ConnectionError("server closed the connection")
            self._buf += chunk

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return lambda *args: self.execute(name.upper(), *args)

    def close(self) -> None:
        self._sock.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
