"""Run the server:  python -m miniredis --port 6380 --dump data.json"""
import argparse
import asyncio
import logging
import signal

from .server import MiniRedisServer


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="miniredis", description="A small Redis-compatible server.")
    parser.add_argument("--host", default="127.0.0.1", help="address to listen on (default 127.0.0.1)")
    parser.add_argument("--port", type=int, default=6380, help="port to listen on (default 6380)")
    parser.add_argument("--dump", metavar="FILE", help="snapshot file: loaded at start, saved on SAVE and on exit")
    return parser.parse_args(argv)


async def run(args) -> None:
    server = MiniRedisServer(args.host, args.port, snapshot_path=args.dump)
    await server.start()
    print(f"mini-redis listening on {server.host}:{server.port}  (Ctrl+C to stop)")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows has no signal handlers on the event loop
            pass
    try:
        await stop.wait()
    finally:
        await server.stop()
        print("stopped" + (f", snapshot saved to {args.dump}" if args.dump else ""))


def main(argv=None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        asyncio.run(run(parse_args(argv)))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
