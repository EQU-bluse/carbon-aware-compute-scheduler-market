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
        help="multi-token scoped authorization file for GET /audit, "
             "exclusive with --token")
    server.add_argument(
        "--checkpoint", action=_Once,
        help="proof checkpoint file to expose at GET /audit/proof, "
             "requires --audit")
    return command


def main() -> None:
    command = parser()
    args = command.parse_args()
    if args.command == "serve":
        # --audit pairs with exactly one authorization method: the
        # single --token or the multi-token --auth file. Both omitted
        # keeps GET /audit a plain 404; --audit without a method, a
        # method without --audit, both methods at once, an empty value
        # or a repeated option is a usage error and exits with
        # argparse's status 2. --checkpoint only ever accompanies
        # --audit: alone, empty or repeated it is a usage error too.
        if args.audit is None:
            if args.token is not None or args.auth is not None \
                    or args.checkpoint is not None:
                command.error(
                    "--token, --auth and --checkpoint require --audit")
        else:
            if not args.audit:
                command.error("--audit must be non-empty")
            if (args.token is None) == (args.auth is None):
                command.error(
                    "--audit requires exactly one of --token and --auth")
            if args.token is not None and not args.token:
                command.error("--token must be non-empty")
            if args.auth is not None and not args.auth:
                command.error("--auth must be non-empty")
            if args.checkpoint is not None and not args.checkpoint:
                command.error("--checkpoint must be non-empty")
        if args.auth:
            # The whole configuration is validated before the port is
            # bound; any failure is a usage error with status 2.
            try:
                auth.load(args.auth)
            except (OSError, ValueError) as exc:
                command.error(f"invalid --auth file: {exc}")
        serve(args.host, args.port, audit_path=args.audit, token=args.token,
              auth=args.auth, checkpoint=args.checkpoint)


if __name__ == "__main__":
    main()
