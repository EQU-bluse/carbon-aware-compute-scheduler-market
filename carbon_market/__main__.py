from __future__ import annotations

import argparse

from .server import serve


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(prog="carbon_market")
    subcommands = command.add_subparsers(dest="command", required=True)
    server = subcommands.add_parser("serve", help="start the HTTP service")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8000)
    # The audit route is enabled only when both are supplied; collecting
    # every occurrence lets us reject a repeated flag instead of silently
    # keeping the last value.
    server.add_argument("--audit", action="append", default=[],
                        metavar="PATH", help="audit journal served at /audit")
    server.add_argument("--token", action="append", default=[],
                        metavar="TOKEN", help="token required for /audit")
    return command


def _audit_pair(
    command: argparse.ArgumentParser,
    args: argparse.Namespace,
) -> tuple[str | None, str | None]:
    # Both flags together, each at most once, both non-empty; any other
    # combination is a usage error and exits with argparse's code 2.
    if not args.audit and not args.token:
        return None, None
    if len(args.audit) > 1 or len(args.token) > 1:
        command.error("--audit and --token may each be given at most once")
    if len(args.audit) != 1 or len(args.token) != 1 \
            or not args.audit[0] or not args.token[0]:
        command.error("--audit PATH and --token TOKEN must be given "
                      "together as non-empty values")
    return args.audit[0], args.token[0]


def main() -> None:
    command = parser()
    args = command.parse_args()
    if args.command == "serve":
        audit_path, audit_token = _audit_pair(command, args)
        serve(args.host, args.port, audit_path, audit_token)


if __name__ == "__main__":
    main()
