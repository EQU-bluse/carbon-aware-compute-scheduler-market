from __future__ import annotations

import argparse
import json
import sys
from typing import NoReturn

from . import audit_proof
from . import auth
from .server import serve


class _Once(argparse.Action):
    """Optional argument that may be given at most once."""

    def __call__(self, parser, namespace, values, option_string=None):
        if getattr(namespace, self.dest, None) is not None:
            parser.error(f"{option_string} must not be given more than once")
        setattr(namespace, self.dest, values)


def _emit_failure(code: str, status: int) -> NoReturn:
    # The failure object is compact and single-line and carries only the
    # error code: no paths, system messages or input content ever leak.
    body = json.dumps({"error": code}, ensure_ascii=False,
                      separators=(",", ":"))
    sys.stderr.write(body + "\n")
    raise SystemExit(status)


def _invalid_request(message: str) -> NoReturn:
    # argparse error hook of the verify-bundle subparser: every usage
    # error -- unknown, missing, repeated or empty options -- is the
    # same compact invalid_request object with exit status 2, and no
    # input file has been read at this point.
    _emit_failure("invalid_request", 2)


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
    bundle = subcommands.add_parser(
        "verify-bundle",
        help="verify a downloaded checkpoint and proof bundle offline")
    bundle.add_argument(
        "--checkpoint", action=_Once, required=True,
        help="downloaded checkpoint file to verify against")
    bundle.add_argument(
        "--proof", action=_Once, required=True,
        help="exported proof file to verify")
    bundle.add_argument(
        "--etag", action=_Once, required=True,
        help='strong ETag of the checkpoint download ("<64 hex>")')
    bundle.error = _invalid_request  # type: ignore[method-assign]
    return command


def main() -> None:
    command = parser()
    args, extras = command.parse_known_args()
    if extras:
        # Unrecognized arguments surface through the main parser even
        # when they belong to a subcommand; verify-bundle reports every
        # usage error as the compact invalid_request object.
        if args.command == "verify-bundle":
            _invalid_request(" ".join(extras))
        command.error(f"unrecognized arguments: {' '.join(extras)}")
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
    elif args.command == "verify-bundle":
        # Argument and tag format errors (ValueError raised before any
        # file is read) exit 2; encoding, JSON or public-structure
        # errors exit 3; tag, chain, generation, anchor, state or page
        # mismatches exit 4; a missing file or any other locking, open
        # or read failure exits 5. A failure writes nothing to stdout.
        try:
            result = audit_proof.verify_bundle(
                args.checkpoint, args.proof, args.etag)
        except audit_proof.InvalidBundleError:
            _emit_failure("invalid_bundle", 3)
        except audit_proof.VerificationFailedError:
            _emit_failure("verification_failed", 4)
        except OSError:
            _emit_failure("bundle_unavailable", 5)
        except ValueError:
            _emit_failure("invalid_request", 2)
        # Compact UTF-8 JSON with non-ASCII written through and exactly
        # one trailing newline; stderr stays empty on success.
        body = json.dumps(result, ensure_ascii=False,
                          separators=(",", ":"), allow_nan=False)
        sys.stdout.write(body + "\n")


if __name__ == "__main__":
    main()
