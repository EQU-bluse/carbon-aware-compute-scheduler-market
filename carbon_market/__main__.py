from __future__ import annotations

import argparse

from . import auth
from .server import serve


class _Once(argparse.Action):
    """Optional argument that may be given at most once."""

    def __call__(self, parser, namespace, values, option_string=None):
        if getattr(namespace, self.dest, None) is not None:
            parser.error(f"{option_string} must not be given more than once")
        setattr(namespace, self.dest, values)


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(prog="carbon_market")
    subcommands = command.add_subparsers(dest="command", required=True)
    server = subcommands.add_parser("serve", help="start the HTTP service")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8000)
    server.add_argument(
        "--audit", action=_Once,
        help="audit journal file to expose at GET /audit")
    server.add_argument(
        "--token", action=_Once,
        help="token clients must send in the X-Audit-Token header")
    server.add_argument(
        "--auth", action=_Once,
        help="hot-rotatable multi-token scoped authorization file; "
             "mutually exclusive with --token")
    return command


def main() -> None:
    command = parser()
    args = command.parse_args()
    if args.command == "serve":
        # --token and --auth are mutually exclusive authorization methods;
        # enabling the audit requires exactly one of them and omitting the
        # audit requires neither. Any mismatch, an empty value or a repeated
        # option is a usage error and exits with argparse's status 2.
        token_given = args.token is not None
        auth_given = args.auth is not None
        audit_given = args.audit is not None
        if token_given and auth_given:
            command.error("--auth and --token are mutually exclusive")
        if audit_given != (token_given or auth_given):
            command.error(
                "--audit requires exactly one of --token or --auth")
        if args.audit == "" or args.token == "" or args.auth == "":
            command.error("--audit, --token and --auth must be non-empty")

        # The whole authorization file is validated before the port is
        # ever bound: a missing, empty or malformed file exits 2 without
        # listening. It is re-read per request afterwards, so a rotation
        # needs no restart but a broken file can never have served.
        if auth_given:
            try:
                auth.load_config(args.auth)
            except auth.AuthConfigError as exc:
                command.error(f"invalid --auth configuration: {exc}")

        serve(args.host, args.port, audit_path=args.audit, token=args.token,
              auth_path=args.auth)


if __name__ == "__main__":
    main()
