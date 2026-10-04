from __future__ import annotations

import argparse
import json
import sys

from . import audit_proof, auth
from .server import serve


class _Once(argparse.Action):
    """Optional argument that may be given at most once."""

    def __call__(self, parser, namespace, values, option_string=None):
        if getattr(namespace, self.dest, None) is not None:
            parser.error(f"{option_string} must not be given more than once")
        setattr(namespace, self.dest, values)


class _FlagOnce(argparse.Action):
    """Valueless switch that may be given at most once."""

    def __init__(self, option_strings, dest, **kwargs):
        # nargs=0 makes the switch take no value and turns an attached
        # "--flag=value" into argparse's usage error; const carries the
        # value setting the switch implies. argparse itself supplies
        # const and default, so override what it passed.
        kwargs["nargs"] = 0
        kwargs["const"] = True
        kwargs["default"] = False
        super().__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if getattr(namespace, self.dest, False):
            parser.error(f"{option_string} must not be given more than once")
        setattr(namespace, self.dest, True)


class _UsageError(Exception):
    """A verify-bundle usage error, reported as invalid_request."""


def _raise_usage(message: str) -> None:
    raise _UsageError(message)


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
        "--metrics", action=_FlagOnce,
        help="expose the read-only process metrics at GET /metrics; "
             "requires --audit and --auth")
    server.add_argument(
        "--checkpoint", action=_Once,
        help="proof checkpoint file to expose at GET /audit/proof, "
             "requires --audit")
    server.add_argument(
        "--acceptance", action=_Once,
        help="acceptance ledger directory to expose at GET /acceptance, "
             "requires --audit and --auth")
    server.add_argument(
        "--migration-batches", action=_Once, metavar="PATH",
        help="migration batch coordination ledger to expose at "
             "GET /migration-batches, requires --audit and --auth")
    server.add_argument(
        "--migration-consumers", action=_Once, metavar="PATH",
        help="independent consumer ledger for "
             "POST /migration-consumers/{claim,pull,ack,reject} and "
             "GET /migration-consumers/{status,dead-letters}, requires "
             "--migration-batches")
    server.add_argument(
        "--completions", action=_Once, metavar="PATH",
        help="completion ledger to expose at GET /completions, "
             "requires --audit and --auth")
    server.add_argument(
        "--signals", action=_Once, metavar="PATH",
        help="live signal ledger to accept pushes at POST /signals/ingest, "
             "requires --signal-trust and --signal-receipts")
    server.add_argument(
        "--signal-trust", action=_Once, metavar="PATH",
        help="source trust file authenticating POST /signals/ingest, "
             "requires --signals and --signal-receipts")
    server.add_argument(
        "--signal-receipts", action=_Once, metavar="PATH",
        help="ingest receipt ledger written by POST /signals/ingest, "
             "requires --signals and --signal-trust")
    bundle = subcommands.add_parser(
        "verify-bundle",
        help="verify a downloaded checkpoint/proof pair offline")
    bundle.add_argument(
        "--checkpoint", action=_Once, required=True,
        help="downloaded checkpoint file")
    bundle.add_argument(
        "--proof", action=_Once, required=True,
        help="downloaded proof file")
    bundle.add_argument(
        "--etag", action=_Once, required=True,
        help="strong ETag the checkpoint download carried")
    bundle.add_argument(
        "--trust-dir", action=_Once,
        help="directory retaining the persistent checkpoint sequence "
             "that rejects whole-bundle replays of older versions")
    # Usage errors of this command surface as the compact invalid_request
    # failure object on stderr, not as argparse's usage text.
    bundle.error = _raise_usage
    return command


def _fail(code: str, status: int) -> None:
    # Failure objects are a single compact JSON line on stderr and never
    # carry paths, system messages or input content; stdout stays empty.
    sys.stderr.write(json.dumps({"error": code}, separators=(",", ":"))
                     + "\n")
    raise SystemExit(status)


def _verify_bundle(args: argparse.Namespace) -> None:
    # The three mandatory options are required and may appear at most
    # once (argparse enforces both); an empty value, an empty
    # --trust-dir or a tag that is not one quoted 64-digit lowercase
    # digest is a usage error too, and no argument error may read the
    # input files.
    if not args.checkpoint or not args.proof or not args.etag \
            or (args.trust_dir is not None and not args.trust_dir) \
            or not audit_proof._ETAG_RE.fullmatch(args.etag):
        _fail("invalid_request", 2)
    try:
        result = audit_proof.verify_bundle(
            args.checkpoint, args.proof, args.etag,
            trust_dir=args.trust_dir)
        payload = json.dumps(
            {"valid": True, "etag": result["etag"],
             "generation": result["generation"],
             "closed": result["closed"],
             "events": result["events"], "next": result["next"]},
            ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    except audit_proof.BundleMismatchError:
        _fail("verification_failed", 4)
    except ValueError:
        _fail("invalid_bundle", 3)
    except OSError:
        _fail("bundle_unavailable", 5)
    # Success: one compact UTF-8 JSON line on stdout; stderr stays empty.
    sys.stdout.write(payload + "\n")


def main() -> None:
    command = parser()
    try:
        if sys.argv[1:2] == ["verify-bundle"]:
            # Extras left over after the subcommand are reported by the
            # top-level parser; route them to the same failure object.
            command.error = _raise_usage
        args = command.parse_args()
    except _UsageError:
        _fail("invalid_request", 2)
    if args.command == "serve":
        # --audit pairs with exactly one authorization method: the
        # single --token or the multi-token --auth file. Both omitted
        # keeps GET /audit a plain 404; --audit without a method, a
        # method without --audit, both methods at once, an empty value
        # or a repeated option is a usage error and exits with
        # argparse's status 2. --checkpoint only ever accompanies
        # --audit: alone, empty or repeated it is a usage error too.
        # --acceptance and --migration-batches are stricter still: they
        # require --audit together with the multi-token --auth method,
        # never the single --token. --completions follows the same rule.
        # --metrics is a valueless switch that likewise requires the
        # --audit/--auth pair and may never accompany --token.
        if args.audit is None:
            if args.token is not None or args.auth is not None \
                    or args.checkpoint is not None \
                    or args.acceptance is not None \
                    or args.migration_batches is not None \
                    or args.migration_consumers is not None \
                    or args.completions is not None \
                    or args.metrics:
                command.error(
                    "--token, --auth, --checkpoint, --acceptance, "
                    "--migration-batches, --migration-consumers, "
                    "--completions and --metrics require --audit")
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
            if args.metrics and args.auth is None:
                command.error("--metrics requires --auth")
            if args.checkpoint is not None and not args.checkpoint:
                command.error("--checkpoint must be non-empty")
            if args.acceptance is not None:
                if not args.acceptance:
                    command.error("--acceptance must be non-empty")
                if args.auth is None:
                    command.error("--acceptance requires --auth")
            if args.migration_batches is not None:
                if not args.migration_batches:
                    command.error("--migration-batches must be non-empty")
                if args.auth is None:
                    command.error("--migration-batches requires --auth")
            if args.migration_consumers is not None:
                if not args.migration_consumers:
                    command.error("--migration-consumers must be non-empty")
                if args.migration_batches is None:
                    command.error(
                        "--migration-consumers requires --migration-batches")
                if args.auth is None:
                    command.error("--migration-consumers requires --auth")
            if args.completions is not None:
                if not args.completions:
                    command.error("--completions must be non-empty")
                if args.auth is None:
                    command.error("--completions requires --auth")
        # --signals, --signal-trust and --signal-receipts form one
        # independent group: they only ever appear together, each at
        # most once and with a non-empty value, and need neither
        # --audit nor an authorization method. Any subset, an empty
        # value or a repeated option is a usage error and nothing
        # listens.
        signal_group = (args.signals, args.signal_trust,
                        args.signal_receipts)
        if any(value is not None for value in signal_group):
            if any(value is None for value in signal_group):
                command.error("--signals, --signal-trust and "
                              "--signal-receipts must be given together")
            if any(not value for value in signal_group):
                command.error("--signals, --signal-trust and "
                              "--signal-receipts must be non-empty")
        if args.auth:
            # The whole configuration is validated before the port is
            # bound; any failure is a usage error with status 2.
            try:
                auth.load(args.auth)
            except (OSError, ValueError) as exc:
                command.error(f"invalid --auth file: {exc}")
        serve(args.host, args.port, audit_path=args.audit, token=args.token,
              auth=args.auth, checkpoint=args.checkpoint,
              acceptance=args.acceptance,
              migration_batches=args.migration_batches,
              migration_consumers=args.migration_consumers,
              completions=args.completions,
              signals=args.signals, signal_trust=args.signal_trust,
              signal_receipts=args.signal_receipts, metrics=args.metrics)
    elif args.command == "verify-bundle":
        _verify_bundle(args)


if __name__ == "__main__":
    main()
