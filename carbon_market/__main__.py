from __future__ import annotations

import argparse

from .server import serve


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(prog="carbon_market")
    subcommands = command.add_subparsers(dest="command", required=True)
    server = subcommands.add_parser("serve", help="start the HTTP service")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8000)
    return command


def main() -> None:
    args = parser().parse_args()
    if args.command == "serve":
        serve(args.host, args.port)


if __name__ == "__main__":
    main()
