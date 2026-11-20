"""The network layer: an asyncio TCP server.

Like real Redis, everything runs on a single event loop thread. Commands from all clients are
executed one at a time, so the data store needs no locks and every command is atomic.
"""
from __future__ import annotations

import asyncio
import logging
import threading

from . import persistence
from .commands import Context, execute
from .protocol import CommandParser, Err, ProtocolError, encode
from .store import Store

log = logging.getLogger("miniredis")


class MiniRedisServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 6380, store: Store | None = None,
                 snapshot_path: str | None = None, expiry_interval: float = 0.1) -> None:
        self.host = host
        self.port = port
        self.ctx = Context(store or Store(), snapshot_path)
        self._expiry_interval = expiry_interval
        self._server: asyncio.Server | None = None
        self._expiry_task: asyncio.Task | None = None
        self._writers: set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        if self.ctx.snapshot_path:
            loaded = persistence.load(self.ctx.store, self.ctx.snapshot_path)
            log.info("loaded %d keys from %s", loaded, self.ctx.snapshot_path)
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]  # the real port, if 0 was requested
        self._expiry_task = asyncio.create_task(self._expire_loop())

    async def serve_forever(self) -> None:
        assert self._server is not None, "call start() first"
        await self._server.serve_forever()

    async def stop(self) -> None:
        """Stop accepting connections, close the open ones, and save a final snapshot."""
        if self._server is None:
            return
        server, self._server = self._server, None
        if self._expiry_task:
            self._expiry_task.cancel()
        server.close()
        for writer in list(self._writers):
            writer.close()
        await server.wait_closed()
        if self.ctx.snapshot_path:
            count = persistence.save(self.ctx.store, self.ctx.snapshot_path)
            log.info("saved %d keys to %s", count, self.ctx.snapshot_path)

    async def _expire_loop(self) -> None:
        while True:
            await asyncio.sleep(self._expiry_interval)
            self.ctx.store.purge_expired()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        parser = CommandParser()
        self._writers.add(writer)
        self.ctx.connected_clients += 1
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                parser.feed(data)
                replies, closing = [], False
                try:
                    for args in parser.commands():  # a pipelined batch is handled in one go
                        replies.append(self._run(args))
                        if args[0].upper() == b"QUIT":
                            closing = True
                            break
                except ProtocolError as exc:
                    replies.append(encode(Err(f"ERR Protocol error: {exc}")))
                    closing = True
                if replies:
                    writer.write(b"".join(replies))
                    await writer.drain()
                if closing:
                    break
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            self.ctx.connected_clients -= 1
            self._writers.discard(writer)
            writer.close()

    def _run(self, args: list[bytes]) -> bytes:
        try:
            return encode(execute(self.ctx, args))
        except Exception:  # a bug in one command must not take the server down
            log.exception("internal error running %r", args[:1])
            return encode(Err("ERR internal server error"))


class ThreadedServer:
    """Runs a server on a background thread. Handy for tests, benchmarks and demos.

        with ThreadedServer() as server:
            client = Client(server.host, server.port)
    """

    def __init__(self, **options) -> None:
        options.setdefault("port", 0)  # 0 asks the OS for any free port
        self.server = MiniRedisServer(**options)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._error: BaseException | None = None

    @property
    def host(self) -> str:
        return self.server.host

    @property
    def port(self) -> int:
        return self.server.port

    def _run(self) -> None:
        loop = self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.server.start())
        except BaseException as exc:  # report start-up failures to the caller
            self._error = exc
            self._ready.set()
            loop.close()
            return
        self._ready.set()
        try:
            loop.run_forever()
        finally:
            loop.run_until_complete(self.server.stop())
            loop.close()

    def start(self) -> "ThreadedServer":
        self._thread = threading.Thread(target=self._run, name="miniredis", daemon=True)
        self._thread.start()
        self._ready.wait(timeout=5)
        if self._error:
            raise self._error
        return self

    def stop(self) -> None:
        if self._loop and self._thread and self._thread.is_alive():
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5)

    def __enter__(self) -> "ThreadedServer":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()
