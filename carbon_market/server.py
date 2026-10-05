from __future__ import annotations

import hmac
import json
import re
import time
import urllib.parse
from collections.abc import Callable
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, NamedTuple

from . import acceptance, audit, audit_proof, auth, completion, \
    metrics as metrics_mod, migration_batch, signal_ingest

# Query parameters GET /audit accepts; anything else is an invalid request.
_AUDIT_PARAMS = ("cursor", "limit", "op", "stage", "key")
# GET /audit/proof additionally takes the mandatory generation name and
# the optional closing flag; the audit and checkpoint paths are fixed by
# the server and can never be selected through the query.
_PROOF_PARAMS = ("generation", "final") + _AUDIT_PARAMS
# GET /acceptance takes either the exact-lookup key alone or the
# paginated exclusive cursor, page size and state filter.
_ACCEPTANCE_PARAMS = ("key", "cursor", "limit", "state")
# GET /migration-batches takes either the exact-lookup key alone or the
# paginated exclusive cursor and page size; the coordination ledger is
# fixed at startup and can never be selected through the query.
_MIGRATION_BATCHES_PARAMS = ("key", "cursor", "limit")
# GET /migration-batches/events paginates the incremental progress
# stream by the exclusive zero-based position cursor with an optional
# batch key and related job filter, combined by logical AND.
_MIGRATION_EVENTS_PARAMS = ("cursor", "limit", "key", "job")
# POST /migration-consumers/{claim,pull,ack,reject} each accept one
# fixed JSON field set; the client never names a ledger path, which is
# fixed at startup alongside --migration-batches.
_MIGRATION_CONSUMER_PATHS = (
    "/migration-consumers/claim", "/migration-consumers/pull",
    "/migration-consumers/ack", "/migration-consumers/reject")
# GET /migration-consumers/status takes exactly the named consumer and
# the query moment; GET /migration-consumers/dead-letters pages one
# consumer's dead letters by an exclusive position cursor. The ledger
# paths stay fixed at startup and each parameter may appear at most
# once.
_MIGRATION_CONSUMER_STATUS_PARAMS = ("consumer", "now")
_MIGRATION_DEAD_LETTERS_PARAMS = ("consumer", "cursor", "limit")
# GET /completions takes either the exact-lookup job id alone or the
# paginated exclusive cursor, page size and the outcome/exceeded
# filters; the completion ledger is fixed at startup and can never be
# selected through the query.
_COMPLETIONS_PARAMS = ("job", "cursor", "limit", "outcome", "exceeded")
_COMPLETION_OUTCOMES = ("succeeded", "failed")
_COMPLETION_EXCEEDED = ("cost", "carbon", "any", "none")
_MIGRATION_CLAIM_FIELDS = frozenset(
    ("consumer", "owner", "now", "lease", "idem", "key", "job_id"))
_MIGRATION_CLAIM_REQUIRED = frozenset(
    ("consumer", "owner", "now", "lease", "idem"))
_MIGRATION_PULL_FIELDS = frozenset(("consumer", "owner", "now", "limit"))
_MIGRATION_PULL_REQUIRED = frozenset(("consumer", "owner", "now"))
_MIGRATION_ACK_FIELDS = frozenset(
    ("consumer", "owner", "now", "position", "idem"))
_MIGRATION_REJECT_FIELDS = frozenset(
    ("consumer", "owner", "now", "position", "reason", "idem"))
_MIGRATION_MAX_BODY = 1 << 20
# POST /signals/ingest takes one JSON object with exactly the signed
# envelope and the ingest idempotency key; the signal, trust and receipt
# ledger paths are fixed at startup as a group and can never be selected
# through the request.
_SIGNAL_INGEST_PATH = "/signals/ingest"
_SIGNAL_INGEST_BODY_FIELDS = frozenset(("envelope", "key"))
_SIGNAL_INGEST_MAX_BODY = 1 << 20
_ACCEPTANCE_STATES = ("pending", "active", "quarantined")
_OPS = ("copy", "restore")
_STAGES = ("成功", "校验", "执行", "同步", "回滚")
_MAX_LIMIT = 1000
# A conditional download accepts exactly one strong entity tag: one
# double-quoted string of 64 lowercase hexadecimal digits, the SHA-256
# of the full validated response bytes. Weak tags, lists, wildcards and
# surrounding whitespace are invalid requests.
_ETAG_RE = re.compile(r'"[0-9a-f]{64}"')
# Every fixed-path entry maps the exceptions of its scope and snapshot
# reads through one ordered rule list: the first rule whose exception
# types match decides the status and the entry's own error name,
# exactly like a chain of except clauses (a subclass rule therefore
# always precedes its base). "missing" is the absent ledger or
# checkpoint, "unknown" the absent exact-lookup key, "invalid" the
# non-canonical content and "unavailable" any other I/O failure; no
# rule ever leaks a configured path, a token or a system message.
_CHECKPOINT_ERRORS = (
    (FileNotFoundError, HTTPStatus.NOT_FOUND, "checkpoint_not_found"),
    (ValueError, HTTPStatus.CONFLICT, "checkpoint_invalid"),
    (OSError, HTTPStatus.SERVICE_UNAVAILABLE, "checkpoint_unavailable"),
)
_ACCEPTANCE_ERRORS = (
    (FileNotFoundError, HTTPStatus.NOT_FOUND, "acceptance_not_found"),
    (KeyError, HTTPStatus.NOT_FOUND, "acceptance_key_not_found"),
    (ValueError, HTTPStatus.CONFLICT, "acceptance_invalid"),
    (OSError, HTTPStatus.SERVICE_UNAVAILABLE, "acceptance_unavailable"),
)
_COMPLETION_ERRORS = (
    (FileNotFoundError, HTTPStatus.NOT_FOUND, "completion_not_found"),
    (KeyError, HTTPStatus.NOT_FOUND, "completion_job_not_found"),
    (ValueError, HTTPStatus.CONFLICT, "completion_invalid"),
    (OSError, HTTPStatus.SERVICE_UNAVAILABLE, "completion_unavailable"),
)
_AUDIT_ERRORS = (
    (FileNotFoundError, HTTPStatus.NOT_FOUND, "audit_not_found"),
    (ValueError, HTTPStatus.CONFLICT, "audit_invalid"),
    (OSError, HTTPStatus.SERVICE_UNAVAILABLE, "audit_unavailable"),
)
_PROOF_ERRORS = (
    (FileNotFoundError, HTTPStatus.NOT_FOUND, "proof_not_found"),
    (ValueError, HTTPStatus.CONFLICT, "proof_invalid"),
    (OSError, HTTPStatus.SERVICE_UNAVAILABLE, "proof_unavailable"),
)
_MIGRATION_BATCHES_ERRORS = (
    (FileNotFoundError, HTTPStatus.NOT_FOUND,
     "migration_batches_not_found"),
    (KeyError, HTTPStatus.NOT_FOUND, "migration_batch_not_found"),
    (ValueError, HTTPStatus.CONFLICT, "migration_batches_invalid"),
    (OSError, HTTPStatus.SERVICE_UNAVAILABLE,
     "migration_batches_unavailable"),
)
_MIGRATION_EVENTS_ERRORS = (
    (FileNotFoundError, HTTPStatus.NOT_FOUND,
     "migration_batches_not_found"),
    (ValueError, HTTPStatus.CONFLICT, "migration_batches_invalid"),
    (OSError, HTTPStatus.SERVICE_UNAVAILABLE,
     "migration_batches_unavailable"),
)
# The migration consumer entries share one vocabulary; each keeps only
# the rules its own reads can raise, in matching order. The dedicated
# ownership subclasses precede the OSError rule so a filesystem
# PermissionError from a commit can never fall through as 503 instead
# of 409, and KeyError precedes its LookupError base.
_MIGRATION_CONSUMER_ERRORS = (
    (KeyError, HTTPStatus.NOT_FOUND, "migration_consumer_not_found"),
    ((migration_batch.ConsumerOwnershipError,
      migration_batch.ConsumerLeaseExpired),
     HTTPStatus.CONFLICT, "migration_consumer_ownership"),
    (LookupError, HTTPStatus.CONFLICT, "migration_consumer_checkpoint"),
    (migration_batch.CoordinationLedgerInvalid,
     HTTPStatus.CONFLICT, "migration_batches_invalid"),
    (migration_batch.ConsumerLedgerInvalid,
     HTTPStatus.CONFLICT, "migration_consumers_invalid"),
    (migration_batch.CoordinationLedgerMissing,
     HTTPStatus.NOT_FOUND, "migration_batches_not_found"),
    (migration_batch.ConsumersLedgerMissing,
     HTTPStatus.NOT_FOUND, "migration_consumers_not_found"),
    (FileNotFoundError, HTTPStatus.NOT_FOUND,
     "migration_consumers_not_found"),
    (ValueError, HTTPStatus.BAD_REQUEST, "invalid_request"),
    (OSError, HTTPStatus.SERVICE_UNAVAILABLE,
     "migration_consumers_unavailable"),
)
_MIGRATION_CONSUMER_STATUS_ERRORS = (
    (KeyError, HTTPStatus.NOT_FOUND, "migration_consumer_not_found"),
    (LookupError, HTTPStatus.CONFLICT, "migration_consumer_checkpoint"),
    (migration_batch.CoordinationLedgerInvalid,
     HTTPStatus.CONFLICT, "migration_batches_invalid"),
    (migration_batch.ConsumerLedgerInvalid,
     HTTPStatus.CONFLICT, "migration_consumers_invalid"),
    (migration_batch.CoordinationLedgerMissing,
     HTTPStatus.NOT_FOUND, "migration_batches_not_found"),
    (migration_batch.ConsumersLedgerMissing,
     HTTPStatus.NOT_FOUND, "migration_consumers_not_found"),
    (FileNotFoundError, HTTPStatus.NOT_FOUND,
     "migration_consumers_not_found"),
    (ValueError, HTTPStatus.BAD_REQUEST, "invalid_request"),
    (OSError, HTTPStatus.SERVICE_UNAVAILABLE,
     "migration_consumers_unavailable"),
)
_MIGRATION_DEAD_LETTERS_ERRORS = (
    (KeyError, HTTPStatus.NOT_FOUND, "migration_consumer_not_found"),
    (migration_batch.ConsumerLedgerInvalid,
     HTTPStatus.CONFLICT, "migration_consumers_invalid"),
    (migration_batch.ConsumersLedgerMissing,
     HTTPStatus.NOT_FOUND, "migration_consumers_not_found"),
    (FileNotFoundError, HTTPStatus.NOT_FOUND,
     "migration_consumers_not_found"),
    (ValueError, HTTPStatus.BAD_REQUEST, "invalid_request"),
    (OSError, HTTPStatus.SERVICE_UNAVAILABLE,
     "migration_consumers_unavailable"),
)
# The process metrics classify requests by the public fixed path with
# the query string removed. Only these routes can open their own
# dimension; every other path -- disabled entries included -- is
# "other", so an arbitrary URL can never create an unbounded set of keys.
_METRICS_ROUTES = frozenset((
    "/health",
    "/audit",
    "/audit/proof",
    "/audit/checkpoint",
    "/acceptance",
    "/migration-batches",
    "/migration-batches/events",
    "/migration-consumers/status",
    "/migration-consumers/dead-letters",
    "/migration-consumers/claim",
    "/migration-consumers/pull",
    "/migration-consumers/ack",
    "/migration-consumers/reject",
    "/completions",
    "/signals/ingest",
    "/metrics",
))
_METRICS_PATH = "/metrics"


def _decode_component(text: str) -> str:
    # Form decoding: '+' is a space, percent-triplets must decode as UTF-8.
    try:
        return urllib.parse.unquote_to_bytes(
            text.replace("+", " ")).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("query component is not valid UTF-8") from exc


def _parse_query(query: str, allowed: tuple[str, ...]) -> dict[str, str]:
    # Every parameter must be one of the allowed names, appear at most once
    # and carry a non-empty value; anything else is an invalid request.
    params: dict[str, str] = {}
    if not query:
        return params
    for piece in query.split("&"):
        name, _, value = piece.partition("=")
        name = _decode_component(name)
        value = _decode_component(value)
        if name not in allowed or name in params or not value:
            raise ValueError(f"invalid query parameter: {name!r}")
        params[name] = value
    return params


def _check_filters(params: dict[str, str]) -> None:
    limit = params.get("limit")
    if limit is not None:
        if not all("0" <= char <= "9" for char in limit):
            raise ValueError("limit must be a decimal integer")
        if not 1 <= int(limit) <= _MAX_LIMIT:
            raise ValueError("limit must be between 1 and 1000")
    if "op" in params and params["op"] not in _OPS:
        raise ValueError('op must be "copy" or "restore"')
    if "stage" in params and params["stage"] not in _STAGES:
        raise ValueError("stage must be one of 成功, 校验, 执行, 同步, 回滚")


def _parse_audit_params(query: str) -> dict[str, str]:
    params = _parse_query(query, _AUDIT_PARAMS)
    _check_filters(params)
    return params


def _parse_proof_params(query: str) -> dict[str, str]:
    params = _parse_query(query, _PROOF_PARAMS)
    # The generation name is mandatory; the closing flag is exactly
    # "true" or "false" and defaults to false when omitted.
    if "generation" not in params:
        raise ValueError("generation is required")
    if "final" in params and params["final"] not in ("true", "false"):
        raise ValueError('final must be "true" or "false"')
    _check_filters(params)
    return params


def _parse_acceptance_params(query: str) -> dict[str, str]:
    params = _parse_query(query, _ACCEPTANCE_PARAMS)
    if "key" in params:
        # An exact lookup stands alone: no cursor, page size or state
        # may accompany the key.
        if len(params) != 1:
            raise ValueError("key cannot be combined with cursor, "
                             "limit or state")
        return params
    limit = params.get("limit")
    if limit is not None:
        if not all("0" <= char <= "9" for char in limit):
            raise ValueError("limit must be a decimal integer")
        if not 1 <= int(limit) <= _MAX_LIMIT:
            raise ValueError("limit must be between 1 and 1000")
    if "state" in params and params["state"] not in _ACCEPTANCE_STATES:
        raise ValueError("state must be pending, active or quarantined")
    return params


def _parse_completions_params(query: str) -> dict[str, str]:
    params = _parse_query(query, _COMPLETIONS_PARAMS)
    if "job" in params:
        # An exact lookup stands alone: no cursor, page size or filter
        # may accompany the job id.
        if len(params) != 1:
            raise ValueError("job cannot be combined with cursor, "
                             "limit, outcome or exceeded")
        return params
    limit = params.get("limit")
    if limit is not None:
        if not all("0" <= char <= "9" for char in limit):
            raise ValueError("limit must be a decimal integer")
        if not 1 <= int(limit) <= _MAX_LIMIT:
            raise ValueError("limit must be between 1 and 1000")
    if "outcome" in params and params["outcome"] not in _COMPLETION_OUTCOMES:
        raise ValueError("outcome must be succeeded or failed")
    if "exceeded" in params \
            and params["exceeded"] not in _COMPLETION_EXCEEDED:
        raise ValueError("exceeded must be cost, carbon, any or none")
    return params


def _parse_migration_batches_params(query: str) -> dict[str, str]:
    params = _parse_query(query, _MIGRATION_BATCHES_PARAMS)
    if "key" in params:
        # An exact lookup stands alone: no cursor or page size may
        # accompany the key.
        if len(params) != 1:
            raise ValueError("key cannot be combined with cursor or limit")
        return params
    limit = params.get("limit")
    if limit is not None:
        if not all("0" <= char <= "9" for char in limit):
            raise ValueError("limit must be a decimal integer")
        if not 1 <= int(limit) <= _MAX_LIMIT:
            raise ValueError("limit must be between 1 and 1000")
    return params


def _parse_migration_events_params(query: str) -> dict[str, str]:
    params = _parse_query(query, _MIGRATION_EVENTS_PARAMS)
    # The cursor is a decimal non-negative integer (not a batch key),
    # page size a decimal integer from 1 to 1000.
    cursor = params.get("cursor")
    if cursor is not None and not all("0" <= char <= "9" for char in cursor):
        raise ValueError("cursor must be a decimal non-negative integer")
    limit = params.get("limit")
    if limit is not None:
        if not all("0" <= char <= "9" for char in limit):
            raise ValueError("limit must be a decimal integer")
        if not 1 <= int(limit) <= _MAX_LIMIT:
            raise ValueError("limit must be between 1 and 1000")
    return params


def _parse_migration_consumer_status_params(query: str) -> dict[str, str]:
    params = _parse_query(query, _MIGRATION_CONSUMER_STATUS_PARAMS)
    # Both the consumer id and the query moment are mandatory, each at
    # most once; now is decimal non-negative-integer text (no sign, no
    # whitespace, booleans have no textual spelling to reject).
    if "consumer" not in params or "now" not in params:
        raise ValueError("consumer and now are required")
    now = params["now"]
    if not all("0" <= char <= "9" for char in now):
        raise ValueError("now must be a decimal non-negative integer")
    return params


def _parse_migration_dead_letters_params(query: str) -> dict[str, str]:
    params = _parse_query(query, _MIGRATION_DEAD_LETTERS_PARAMS)
    # The consumer id is mandatory; cursor is an exclusive decimal
    # non-negative integer and limit a decimal integer from 1 to 1000,
    # each at most once. Defaults (no cursor, limit 100) are applied by
    # the caller.
    if "consumer" not in params:
        raise ValueError("consumer is required")
    cursor = params.get("cursor")
    if cursor is not None and not all("0" <= char <= "9" for char in cursor):
        raise ValueError("cursor must be a decimal non-negative integer")
    limit = params.get("limit")
    if limit is not None:
        if not all("0" <= char <= "9" for char in limit):
            raise ValueError("limit must be a decimal integer")
        if not 1 <= int(limit) <= _MAX_LIMIT:
            raise ValueError("limit must be between 1 and 1000")
    return params


class _Entry(NamedTuple):
    """One fixed-path entry's plug points in the shared request pipeline.

    ``parse`` validates the raw query string (and, for the consumer
    operations, the request body) into the entry's parameter mapping,
    raising ``ValueError`` on any invalid request. ``scope`` decides
    whether the identified token may run the query, reading a persisted
    subscription when the scope depends on one. ``fetch`` reads the
    validated snapshot under its locks and returns the response payload
    (a conditional entry returns ``(body, etag)`` with ``body`` ``None``
    on a conditional hit). ``respond`` writes the success response.
    ``errors`` is the ordered exception mapping of the scope and
    snapshot reads; ``conditional`` enables the If-None-Match
    validation stage between parameter validation and the scope
    decision.
    """

    parse: Callable[[str], dict[str, Any]]
    scope: Callable[[auth.Record | None, dict[str, Any]], bool]
    fetch: Callable[[dict[str, Any], str | None], Any]
    respond: Callable[[Any], None]
    errors: tuple = ()
    conditional: bool = False


class Handler(BaseHTTPRequestHandler):
    server_version = "CarbonMarket/0.1"

    # Per-request count guard (reset in handle_one_request, which runs
    # once per keep-alive request on the same handler instance).
    _metrics_counted = False

    def handle_one_request(self) -> None:
        # One handler instance serves several keep-alive requests, so
        # the per-request count guard and the parsed target are reset
        # before each request is parsed; a malformed follow-up request
        # line must not be classified (or counted) under the previous
        # request's path.
        self._metrics_counted = False
        self.path = None
        super().handle_one_request()

    def send_response_only(self, code, message=None) -> None:  # type: ignore[override]
        # Every final response funnels through here: the normal
        # _json/_bytes/_raw path arrives via send_response, and the base
        # class's own send_error (an unsupported method's 501, a
        # malformed request line's 400) calls this directly. Counting
        # here -- guarded so one request is counted exactly once, and
        # ignoring the provisional 100 Continue of Expect handling --
        # covers every decided status. A successful GET /metrics reads
        # its snapshot in the handler before this runs, so its own count
        # only shows up in a later snapshot.
        if not (100 <= int(code) < 200):
            self._count_metrics(int(code))
        super().send_response_only(code, message)

    def _count_metrics(self, status: int) -> None:
        store = getattr(self.server, "metrics", None)
        if store is None or self._metrics_counted:
            return
        self._metrics_counted = True
        # Classify by the fixed public path with the query removed; a
        # request line that never parsed has no self.path at all and is
        # an "other" count like any unknown path.
        raw_path = getattr(self, "path", "") or ""
        path = raw_path.partition("?")[0]
        route = path if path in _METRICS_ROUTES else metrics_mod.OTHER
        store.record(route, status)

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        if self.path == "/health":
            self._json(HTTPStatus.OK, {"status": "ok"})
            return
        path, _, query = self.path.partition("?")
        if path == _METRICS_PATH \
                and getattr(self.server, "metrics", None) is not None:
            self._metrics()
            return
        if path == "/audit" \
                and getattr(self.server, "audit_path", None) is not None:
            self._audit(query)
            return
        if path == "/audit/proof" \
                and getattr(self.server, "audit_checkpoint", None) is not None:
            self._proof(query)
            return
        if path == "/audit/checkpoint" \
                and getattr(self.server, "audit_checkpoint", None) is not None:
            # The endpoint takes no parameters: the snapshot is always
            # the one checkpoint file fixed at startup.
            self._checkpoint(query)
            return
        if path == "/acceptance" \
                and getattr(self.server, "acceptance_dir", None) is not None:
            self._acceptance(query)
            return
        if path == "/migration-batches" \
                and getattr(self.server, "migration_batches", None) \
                is not None:
            self._migration_batches(query)
            return
        if path == "/migration-batches/events" \
                and getattr(self.server, "migration_batches", None) \
                is not None:
            self._migration_batch_events(query)
            return
        if path == "/migration-consumers/status" \
                and getattr(self.server, "migration_batches", None) \
                is not None \
                and getattr(self.server, "migration_consumers", None) \
                is not None:
            self._migration_consumer_status(query)
            return
        if path == "/migration-consumers/dead-letters" \
                and getattr(self.server, "migration_batches", None) \
                is not None \
                and getattr(self.server, "migration_consumers", None) \
                is not None:
            self._migration_consumer_dead_letters(query)
            return
        if path == "/completions" \
                and getattr(self.server, "completions_path", None) \
                is not None:
            self._completions(query)
            return
        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        path, _, query = self.path.partition("?")
        if path == _SIGNAL_INGEST_PATH \
                and getattr(self.server, "signals_path", None) is not None \
                and getattr(self.server, "signal_trust", None) is not None \
                and getattr(self.server, "signal_receipts", None) is not None:
            self._signal_ingest(query)
            return
        configured = getattr(self.server, "migration_batches", None) \
            is not None and getattr(self.server, "migration_consumers",
                                   None) is not None
        if path in _MIGRATION_CONSUMER_PATHS and configured:
            operation = path.rsplit("/", 1)[-1]
            self._migration_consumer(operation, query)
            return
        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def _method_not_post(self) -> None:
        # A non-POST method on the ingest path or the metrics path is a
        # plain 404 like any other unknown route and never opens a
        # business file; every other path keeps the default 501 for
        # unsupported methods.
        path, _, _query = self.path.partition("?")
        if path == _SIGNAL_INGEST_PATH or path == _METRICS_PATH:
            self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        self.send_error(HTTPStatus.NOT_IMPLEMENTED,
                        f"Unsupported method ({self.command!r})")

    def do_HEAD(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        self._method_not_post()

    def do_PUT(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        self._method_not_post()

    def do_DELETE(self) -> None:  # noqa: N802
        self._method_not_post()

    def do_PATCH(self) -> None:  # noqa: N802
        self._method_not_post()

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._method_not_post()

    def _read_signal_body(self) -> dict[str, object] | None:
        # One UTF-8 JSON object of at most 1 MiB carrying exactly the
        # signed envelope and the non-empty ingest idempotency key: no
        # duplicate members, no non-finite numbers, no other fields.
        # Anything else is a 400 and never reaches a ledger.
        def reject_duplicates(pairs):
            keys = [name for name, _value in pairs]
            if len(keys) != len(set(keys)):
                raise ValueError("duplicate JSON object member")
            return dict(pairs)

        def reject_constant(_token: str) -> float:
            # NaN/Infinity are never valid request literals.
            raise ValueError("non-finite JSON literal")

        def reject_nonfinite(token: str) -> float:
            # A finite literal like 1e999 still overflows to infinity.
            value = float(token)
            if value != value or value in (float("inf"), float("-inf")):
                raise ValueError("non-finite JSON literal")
            return value

        def invalid() -> None:
            # The request body may not have been consumed, so this
            # connection cannot serve a further request.
            self.close_connection = True
            self._json(HTTPStatus.BAD_REQUEST,
                       {"error": "signal_ingest_invalid"})

        # Only application/json is accepted; a charset or other
        # parameters ride along, but any other media type is a 400.
        if self.headers.get_content_type() != "application/json":
            invalid()
            return None
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length < 1 or length > _SIGNAL_INGEST_MAX_BODY:
                raise ValueError("bad content length")
            raw = self.rfile.read(length)
            body = json.loads(raw.decode("utf-8"),
                              parse_float=reject_nonfinite,
                              parse_constant=reject_constant,
                              object_pairs_hook=reject_duplicates)
        except (ValueError, UnicodeDecodeError):
            invalid()
            return None
        if not isinstance(body, dict) \
                or set(body) != _SIGNAL_INGEST_BODY_FIELDS:
            invalid()
            return None
        return body

    def _signal_ingest(self, query: str) -> None:
        # No endpoint parameter exists: any query string is an invalid
        # request and, like the body validation, is answered before any
        # ledger or trust file is opened.
        if query:
            self.close_connection = True
            self._json(HTTPStatus.BAD_REQUEST,
                       {"error": "signal_ingest_invalid"})
            return
        body = self._read_signal_body()
        if body is None:
            return
        try:
            receipt, created = signal_ingest.ingest(
                getattr(self.server, "signals_path"),
                getattr(self.server, "signal_trust"),
                getattr(self.server, "signal_receipts"),
                body["envelope"], body["key"])
        except PermissionError:
            # An unknown source or key_id, or a mismatched signature;
            # the response never says which and carries no key material.
            self._json(HTTPStatus.FORBIDDEN,
                       {"error": "signal_ingest_forbidden"})
        except FileNotFoundError:
            # A missing trust file or a missing parent directory of
            # either written file; the configured paths never leak.
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "signal_ingest_not_found"})
        except ValueError:
            # A malformed envelope or key, an expired key, an
            # unauthorized region, a non-increasing sequence, a
            # conflicting idempotency key or a non-canonical file.
            self._json(HTTPStatus.BAD_REQUEST,
                       {"error": "signal_ingest_invalid"})
        except OSError:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                       {"error": "signal_ingest_unavailable"})
        else:
            # A first acceptance and a completed pending resume both
            # answer 201 with created true; an active replay answers 200
            # with created false and the stored receipt.
            status = HTTPStatus.CREATED if created else HTTPStatus.OK
            self._json(status, {"receipt": receipt, "created": created})

    def _read_consumer_body(self, operation: str) -> dict[str, object]:
        # One fixed JSON object per operation: exact field set, no
        # duplicates, non-boolean integer moments, no nulls beyond the
        # optional claim filters and no client-selected paths. Anything
        # else raises ValueError -- the shared pipeline's 400 -- and
        # never reaches a ledger.
        allowed, required = {
            "claim": (_MIGRATION_CLAIM_FIELDS, _MIGRATION_CLAIM_REQUIRED),
            "pull": (_MIGRATION_PULL_FIELDS, _MIGRATION_PULL_REQUIRED),
            "ack": (_MIGRATION_ACK_FIELDS, _MIGRATION_ACK_FIELDS),
            "reject": (_MIGRATION_REJECT_FIELDS, _MIGRATION_REJECT_FIELDS),
        }[operation]

        def reject_duplicates(pairs):
            keys = [name for name, _value in pairs]
            if len(keys) != len(set(keys)):
                raise ValueError("duplicate JSON object member")
            return dict(pairs)

        def reject_constant(_token: str) -> float:
            # NaN/Infinity are never valid request literals.
            raise ValueError("non-finite JSON literal")

        length = int(self.headers.get("Content-Length", "0"))
        if length < 0 or length > _MIGRATION_MAX_BODY:
            raise ValueError("bad content length")
        raw = self.rfile.read(length) if length else b""
        body = json.loads(raw.decode("utf-8"),
                         parse_constant=reject_constant,
                         object_pairs_hook=reject_duplicates)
        if not isinstance(body, dict) \
                or not required.issubset(body) \
                or not set(body).issubset(allowed):
            raise ValueError("consumer request has invalid fields")
        for name in ("consumer", "owner"):
            value = body[name]
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be a non-empty string")
        now = body["now"]
        if not isinstance(now, int) or isinstance(now, bool) or now < 0:
            raise ValueError("now must be a non-boolean non-negative integer")
        if operation == "claim":
            lease = body["lease"]
            if not isinstance(lease, int) or isinstance(lease, bool) \
                    or lease < 1:
                raise ValueError(
                    "lease must be a non-boolean positive integer")
            for name in ("key", "job_id"):
                if name in body and body[name] is not None \
                        and (not isinstance(body[name], str)
                             or not body[name]):
                    raise ValueError(f"{name} must be null or a non-empty "
                                     "string")
            if not isinstance(body["idem"], str) or not body["idem"]:
                raise ValueError("idem must be a non-empty string")
        elif operation == "ack":
            position = body["position"]
            if not isinstance(position, int) or isinstance(position, bool) \
                    or position < 0:
                raise ValueError("position must be a non-boolean "
                                 "non-negative integer")
            if not isinstance(body["idem"], str) or not body["idem"]:
                raise ValueError("idem must be a non-empty string")
        elif operation == "reject":
            position = body["position"]
            if not isinstance(position, int) or isinstance(position, bool) \
                    or position < 0:
                raise ValueError("position must be a non-boolean "
                                 "non-negative integer")
            # The content rule is a parameter validity check, enforced
            # here like position's range so an invalid reason answers
            # 400 before the subscription scope is read or the consumer
            # is looked up: surrounding whitespace is stripped and the
            # remainder must span 1..512 Unicode code points (code
            # points, not UTF-16 units or bytes).
            reason = body["reason"]
            if not isinstance(reason, str) \
                    or not 1 <= len(reason.strip()) <= 512:
                raise ValueError("reason must contain between 1 and 512 "
                                 "Unicode code points")
            if not isinstance(body["idem"], str) or not body["idem"]:
                raise ValueError("idem must be a non-empty string")
        elif "limit" in body:
            limit = body["limit"]
            if not isinstance(limit, int) or isinstance(limit, bool) \
                    or not 1 <= limit <= _MAX_LIMIT:
                raise ValueError("limit must be a non-boolean integer "
                                 "between 1 and 1000")
        return body

    def _bad_request(self) -> None:
        self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})

    def _authorize(self) -> tuple[bool, auth.Record | None]:
        # Authorization comes first: an unauthorized request learns nothing
        # about the query, the configured paths or the journal's state.
        # Returns (True, record) once the caller may proceed; on failure
        # the error response is already sent and the result is (False, _).
        tokens = self.headers.get_all("X-Audit-Token")
        if tokens is None or len(tokens) != 1 or not tokens[0].strip():
            self._json(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
            return False, None

        record: auth.Record | None = None
        auth_path = getattr(self.server, "audit_auth", None)
        if auth_path is not None:
            # Multi-token mode: the configuration is re-read on every
            # request, so a same-directory atomic replacement rotates
            # tokens without a restart and each request sees either the
            # complete old or the complete new configuration. A file
            # that can no longer be read or validated makes
            # authorization itself unavailable.
            try:
                records = auth.load(auth_path)
            except (OSError, ValueError):
                self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                           {"error": "auth_unavailable"})
                return False, None
            # An unknown digest and a token past its grace cutoff are
            # indistinguishable: both are a plain 403.
            record = auth.identify(records, tokens[0])
            if record is None:
                self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
                return False, None
        else:
            expected = getattr(self.server, "audit_token").encode("utf-8")
            if not hmac.compare_digest(tokens[0].encode("utf-8"), expected):
                self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
                return False, None
        return True, record

    def _condition(self) -> tuple[bool, str | None]:
        # The conditional header is validated together with the query,
        # after authorization and before the scope decision and any
        # file access: absent (allowed) or exactly one strong tag of
        # the documented shape. A blank value, a repeated header, a
        # weak tag, a list, a wildcard or any other shape is an invalid
        # request that never opens a file. Returns (True, digest) with
        # the unquoted tag -- or (True, None) when the header is absent
        # -- once the caller may proceed; on failure the 400 is already
        # sent and the result is (False, _).
        conditions = self.headers.get_all("If-None-Match")
        if conditions is None:
            return True, None
        if len(conditions) != 1 or not _ETAG_RE.fullmatch(conditions[0]):
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
            return False, None
        return True, conditions[0][1:-1]

    @staticmethod
    def _scope_allows(record: auth.Record | None, exact: str | None) -> bool:
        # The shared scope rule of the exact-or-paginated queries: an
        # exact lookup requires unrestricted operation and stage scopes
        # and a key scope that is unrestricted or names the requested
        # target; a paginated query -- and the checkpoint download,
        # which never names a key -- requires all three scopes
        # unrestricted.
        if record is None:
            return True
        if record.ops is not None or record.stages is not None:
            return False
        if exact is not None:
            return record.keys is None or exact in record.keys
        return record.keys is None

    def _serve(self, query: str, entry: _Entry) -> None:
        # The shared request pipeline of every fixed-path entry:
        # authorize, validate the entry's own parameters (and the
        # conditional header of a conditional entry), decide the token
        # scope, and only then open the snapshot -- a failure at one
        # stage never reaches a later stage's files. Each entry keeps
        # its own parameter shapes, scope target, snapshot read,
        # response body and error names through the _Entry hooks.
        authorized, record = self._authorize()
        if not authorized:
            return
        try:
            params = entry.parse(query)
        except ValueError:
            self._bad_request()
            return
        condition: str | None = None
        if entry.conditional:
            ok, condition = self._condition()
            if not ok:
                return
        try:
            # Scope checks follow parameter validation and precede the
            # snapshot read; a scope that depends on a persisted
            # subscription reads it here, under the same error mapping
            # as the snapshot itself, so a forbidden request still
            # never opens a business file.
            if not entry.scope(record, params):
                self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
                return
            result = entry.fetch(params, condition)
        except Exception as exc:
            # The entry's ordered rules decide the status and error
            # name; an unmapped exception propagates like an unhandled
            # one always did.
            for types, status, name in entry.errors:
                if isinstance(exc, types):
                    self._json(status, {"error": name})
                    return
            raise
        entry.respond(result)

    def _respond_snapshot(self, result: tuple[bytes | None, str]) -> None:
        # The 304 decision was made under the snapshot's lock against
        # the same serialized bytes a 200 carries, so a concurrent
        # write can never mix an old tag with a new body.
        body, etag = result
        if body is None:
            self._raw(HTTPStatus.NOT_MODIFIED, b"", etag)
        else:
            self._raw(HTTPStatus.OK, body, etag)

    def _subscription_key(self, consumer: str) -> str | None:
        # One consumer's persisted fixed subscription key, read at the
        # scope stage from this entry's own control ledger; its
        # failures share the entry's ordered error mapping.
        subscription = migration_batch.consumer_subscription(
            getattr(self.server, "migration_batches"),
            getattr(self.server, "migration_consumers"), consumer)
        return subscription["key"]

    def _audit(self, query: str) -> None:
        def fetch(params: dict[str, Any], _condition: str | None):
            kwargs: dict[str, object] = {}
            for name in ("cursor", "op", "stage", "key"):
                if name in params:
                    kwargs[name] = params[name]
            if "limit" in params:
                kwargs["limit"] = int(params["limit"])
            return audit.search(getattr(self.server, "audit_path"), **kwargs)

        self._serve(query, _Entry(
            parse=_parse_audit_params,
            scope=lambda record, params: record is None
            or auth.scope_allows(record, params),
            fetch=fetch,
            respond=lambda result: self._json(HTTPStatus.OK, result),
            errors=_AUDIT_ERRORS))

    def _metrics(self) -> None:
        # The read-only process snapshot runs the same shared pipeline:
        # identity first (401/503/403), then this entry's parameter and
        # scope checks, and only then is the snapshot read. The entry
        # takes no parameters, so any query string -- even an empty one
        # after a bare '?' -- is an invalid request.
        def parse(_query: str) -> dict[str, Any]:
            if "?" in self.path:
                raise ValueError("metrics takes no query string")
            return {}

        def scope(record: auth.Record | None, _params: dict[str, Any]) \
                -> bool:
            # A metrics token must be unrestricted on every audit axis:
            # operation, stage and history key all have to be wildcards.
            return record is None or (record.ops is None
                                      and record.stages is None
                                      and record.keys is None)

        def fetch(_params: dict[str, Any], _condition: str | None):
            # The snapshot is taken before the response is sent (and
            # this request counted), so a successful GET /metrics never
            # appears in its own snapshot but is counted exactly once
            # afterwards and shows up in the next one.
            return getattr(self.server, "metrics").snapshot()

        self._serve("", _Entry(
            parse=parse,
            scope=scope,
            fetch=fetch,
            respond=lambda result: self._json(HTTPStatus.OK, result)))

    def _proof(self, query: str) -> None:
        def fetch(params: dict[str, Any], _condition: str | None):
            kwargs: dict[str, object] = {}
            for name in ("cursor", "op", "stage", "key"):
                if name in params:
                    kwargs[name] = params[name]
            if "limit" in params:
                kwargs["limit"] = int(params["limit"])
            return audit_proof.export(
                getattr(self.server, "audit_path"),
                getattr(self.server, "audit_checkpoint"),
                params["generation"],
                final=params.get("final") == "true",
                **kwargs)

        self._serve(query, _Entry(
            parse=_parse_proof_params,
            scope=lambda record, params: record is None
            or auth.scope_allows(record, params),
            fetch=fetch,
            respond=lambda result: self._json(HTTPStatus.OK, result),
            errors=_PROOF_ERRORS))

    def _checkpoint(self, query: str) -> None:
        # The snapshot entry takes no query parameters; any query string
        # fails the same "unknown, repeated or empty parameter" parsing
        # used by the other endpoints, and the snapshot is always the
        # one checkpoint file fixed at startup.
        def fetch(_params: dict[str, Any], condition: str | None):
            body, etag = audit_proof.read_snapshot(
                getattr(self.server, "audit_checkpoint"))
            # The 304 decision uses the ETag of the same validated bytes
            # a 200 would serve, so a concurrent export can never mix an
            # old tag with a new body.
            if condition == etag:
                return None, etag
            return body, etag

        self._serve(query, _Entry(
            parse=lambda q: _parse_query(q, ()),
            scope=lambda record, _params: self._scope_allows(record, None),
            fetch=fetch,
            respond=self._respond_snapshot,
            errors=_CHECKPOINT_ERRORS,
            conditional=True))

    def _acceptance(self, query: str) -> None:
        ledger_dir = getattr(self.server, "acceptance_dir")

        def fetch(params: dict[str, Any], condition: str | None):
            if "key" in params:
                return acceptance.get_response(
                    ledger_dir, params["key"], condition)
            kwargs: dict[str, object] = {}
            for name in ("cursor", "state"):
                if name in params:
                    kwargs[name] = params[name]
            if "limit" in params:
                kwargs["limit"] = int(params["limit"])
            return acceptance.search_response(
                ledger_dir, condition=condition, **kwargs)

        self._serve(query, _Entry(
            parse=_parse_acceptance_params,
            scope=lambda record, params: self._scope_allows(
                record, params.get("key")),
            fetch=fetch,
            respond=self._respond_snapshot,
            errors=_ACCEPTANCE_ERRORS,
            conditional=True))

    def _completions(self, query: str) -> None:
        ledger = getattr(self.server, "completions_path")

        def fetch(params: dict[str, Any], condition: str | None):
            if "job" in params:
                return completion.get_response(
                    ledger, params["job"], condition)
            kwargs: dict[str, object] = {}
            for name in ("cursor", "outcome", "exceeded"):
                if name in params:
                    kwargs[name] = params[name]
            if "limit" in params:
                kwargs["limit"] = int(params["limit"])
            return completion.search_response(
                ledger, condition=condition, **kwargs)

        self._serve(query, _Entry(
            parse=_parse_completions_params,
            scope=lambda record, params: self._scope_allows(
                record, params.get("job")),
            fetch=fetch,
            respond=self._respond_snapshot,
            errors=_COMPLETION_ERRORS,
            conditional=True))

    def _migration_batches(self, query: str) -> None:
        ledger = getattr(self.server, "migration_batches")

        def fetch(params: dict[str, Any], _condition: str | None):
            if "key" in params:
                # The exact lookup returns only the batch snapshot; the
                # shared locks cover the coordination file, the business
                # snapshots and the serialization, so the body is one
                # complete version.
                return migration_batch.get_response(ledger, params["key"])
            kwargs: dict[str, object] = {}
            if "cursor" in params:
                kwargs["cursor"] = params["cursor"]
            if "limit" in params:
                kwargs["limit"] = int(params["limit"])
            return migration_batch.search_response(ledger, **kwargs)

        # The scope rule is the shared exact-or-paginated one: an exact
        # lookup requires unrestricted operation and stage scopes and a
        # key scope that is unrestricted or names the requested batch; a
        # paginated query requires all three scopes unrestricted.
        self._serve(query, _Entry(
            parse=_parse_migration_batches_params,
            scope=lambda record, params: self._scope_allows(
                record, params.get("key")),
            fetch=fetch,
            respond=lambda body: self._bytes(HTTPStatus.OK, body),
            errors=_MIGRATION_BATCHES_ERRORS))

    def _migration_batch_events(self, query: str) -> None:
        ledger = getattr(self.server, "migration_batches")

        def fetch(params: dict[str, Any], _condition: str | None):
            kwargs: dict[str, object] = {}
            if "cursor" in params:
                kwargs["cursor"] = int(params["cursor"])
            if "limit" in params:
                kwargs["limit"] = int(params["limit"])
            if "key" in params:
                kwargs["key"] = params["key"]
            if "job" in params:
                kwargs["job_id"] = params["job"]
            return migration_batch.events_response(ledger, **kwargs)

        # Same scope rule as the snapshot query and before any file is
        # opened. A batch-key filter needs unrestricted operation and
        # stage scopes and a key scope that is unrestricted or names the
        # requested batch; cross-batch reads need all three scopes
        # unrestricted. The job filter alone never lifts the
        # cross-batch requirement.
        self._serve(query, _Entry(
            parse=_parse_migration_events_params,
            scope=lambda record, params: self._scope_allows(
                record, params.get("key")),
            fetch=fetch,
            respond=lambda body: self._bytes(HTTPStatus.OK, body),
            errors=_MIGRATION_EVENTS_ERRORS))

    def _migration_consumer(self, operation: str, query: str) -> None:
        coordination = getattr(self.server, "migration_batches")
        consumers = getattr(self.server, "migration_consumers")

        def parse(raw_query: str) -> dict[str, Any]:
            # No endpoint takes a query string; checked after identity
            # like the other parameter validations, before the body and
            # any file.
            if raw_query:
                raise ValueError("unexpected query string")
            return self._read_consumer_body(operation)

        def scope(record: auth.Record | None, body: dict[str, Any]) -> bool:
            # Scope follows body validation and precedes the
            # coordination stream read. The operation and stage axes
            # need no file and are decided first. A claim carries the
            # batch key in the request; a pull or ack never names it, so
            # the key axis reads the consumer's persisted fixed
            # subscription from this endpoint's own control ledger (at
            # the same stage the auth configuration is re-read). The
            # data-plane coordination and business ledgers open only
            # after the scope is allowed.
            if record is not None and (record.ops is not None
                                       or record.stages is not None):
                return False
            batch_key: str | None
            if operation == "claim":
                batch_key = body.get("key")
            elif record is not None and record.keys is None:
                batch_key = None
            else:
                batch_key = self._subscription_key(body["consumer"])
            if record is not None and record.keys is not None:
                return batch_key is not None and batch_key in record.keys
            return True

        def fetch(body: dict[str, Any], _condition: str | None):
            kwargs: dict[str, object] = {}
            if operation == "claim":
                if "key" in body:
                    kwargs["key"] = body["key"]
                if "job_id" in body:
                    kwargs["job_id"] = body["job_id"]
                kwargs["lease"] = body["lease"]
                kwargs["idem"] = body["idem"]
            elif operation == "ack":
                kwargs["position"] = body["position"]
                kwargs["idem"] = body["idem"]
            elif operation == "reject":
                kwargs["position"] = body["position"]
                kwargs["reason"] = body["reason"]
                kwargs["idem"] = body["idem"]
            elif "limit" in body:
                kwargs["limit"] = body["limit"]
            return migration_batch.consume_response(
                coordination, consumers, operation, body["consumer"],
                body["owner"], body["now"], **kwargs)

        self._serve(query, _Entry(
            parse=parse,
            scope=scope,
            fetch=fetch,
            respond=lambda body: self._bytes(HTTPStatus.OK, body),
            errors=_MIGRATION_CONSUMER_ERRORS))

    def _migration_consumer_status(self, query: str) -> None:
        # Read-only consumer status: identity, parameters, the
        # persisted subscription read for scope, and only then the
        # coordination/business snapshot -- a failure at one stage
        # never opens a later stage's files.
        def scope(record: auth.Record | None, params: dict[str, Any]) \
                -> bool:
            # Operation- and stage-scoped tokens can never read a
            # consumer; an unrestricted key scope reads any
            # subscription. A key-scoped token reads the consumer's
            # fixed subscription from the consumer ledger at the scope
            # stage, exactly like a pull or ack: only a fixed-batch
            # subscription naming an allowed batch passes, while a
            # cross-batch or job-only range needs all three scopes
            # unrestricted. The data-plane coordination and business
            # ledgers open only after the scope is allowed.
            if record is not None and (record.ops is not None
                                       or record.stages is not None):
                return False
            if record is not None and record.keys is not None:
                batch_key = self._subscription_key(params["consumer"])
                if batch_key is None or batch_key not in record.keys:
                    return False
            return True

        def fetch(params: dict[str, Any], _condition: str | None):
            return migration_batch.consumer_status_response(
                getattr(self.server, "migration_batches"),
                getattr(self.server, "migration_consumers"),
                params["consumer"], int(params["now"]))

        self._serve(query, _Entry(
            parse=_parse_migration_consumer_status_params,
            scope=scope,
            fetch=fetch,
            respond=lambda body: self._bytes(HTTPStatus.OK, body),
            errors=_MIGRATION_CONSUMER_STATUS_ERRORS))

    def _migration_consumer_dead_letters(self, query: str) -> None:
        # Read-only dead-letter query: identity, parameters, the
        # persisted subscription read for scope, and only then the
        # consumer ledger's dead-letter section. The page is a
        # self-contained historical record, so the coordination and
        # business ledgers are never opened; a failure at one stage
        # still never opens a later stage's files.
        def scope(record: auth.Record | None, params: dict[str, Any]) \
                -> bool:
            # Same scope order as status: operation- and stage-scoped
            # tokens are rejected before the subscription is read; a
            # key-scoped token reads the fixed subscription and passes
            # only a fixed-batch range naming an allowed batch.
            if record is not None and (record.ops is not None
                                       or record.stages is not None):
                return False
            if record is not None and record.keys is not None:
                batch_key = self._subscription_key(params["consumer"])
                if batch_key is None or batch_key not in record.keys:
                    return False
            return True

        def fetch(params: dict[str, Any], _condition: str | None):
            cursor = int(params["cursor"]) if "cursor" in params else None
            limit = int(params["limit"]) if "limit" in params else 100
            return migration_batch.consumer_dead_letters_response(
                getattr(self.server, "migration_batches"),
                getattr(self.server, "migration_consumers"),
                params["consumer"], cursor=cursor, limit=limit)

        self._serve(query, _Entry(
            parse=_parse_migration_dead_letters_params,
            scope=scope,
            fetch=fetch,
            respond=lambda body: self._bytes(HTTPStatus.OK, body),
            errors=_MIGRATION_DEAD_LETTERS_ERRORS))

    def _raw(self, status: HTTPStatus, body: bytes, etag: str) -> None:
        # The snapshot bytes are served verbatim -- the original UTF-8
        # written by the exporter, with its field order intact -- and the
        # tag is the strong SHA-256 of those exact bytes.
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("ETag", f'"{etag}"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _bytes(self, status: HTTPStatus, body: bytes) -> None:
        # Serve already-serialized compact UTF-8 JSON bytes verbatim, as
        # the read-only library builder formed them under its locks.
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def _json(self, status: HTTPStatus, payload: object) -> None:
        # Compact JSON with non-ASCII written through as direct UTF-8 and
        # no trailing newline, matching the persistent files' convention.
        body = json.dumps(payload, ensure_ascii=False,
                          separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def serve(host: str, port: int, audit_path: str | None = None,
          token: str | None = None, auth: str | None = None,
          checkpoint: str | None = None,
          acceptance: str | None = None,
          migration_batches: str | None = None,
          migration_consumers: str | None = None,
          completions: str | None = None,
          signals: str | None = None,
          signal_trust: str | None = None,
          signal_receipts: str | None = None,
          enable_metrics: bool = False) -> None:
    with ThreadingHTTPServer((host, port), Handler) as server:
        server.audit_path = audit_path  # type: ignore[attr-defined]
        server.audit_token = token  # type: ignore[attr-defined]
        server.audit_auth = auth  # type: ignore[attr-defined]
        server.audit_checkpoint = checkpoint  # type: ignore[attr-defined]
        server.acceptance_dir = acceptance  # type: ignore[attr-defined]
        server.migration_batches = migration_batches  # type: ignore[attr-defined]
        server.migration_consumers = migration_consumers  # type: ignore[attr-defined]
        server.completions_path = completions  # type: ignore[attr-defined]
        server.signals_path = signals  # type: ignore[attr-defined]
        server.signal_trust = signal_trust  # type: ignore[attr-defined]
        server.signal_receipts = signal_receipts  # type: ignore[attr-defined]
        # The counters are process-local: they start at zero exactly
        # when listening begins, are reset by a restart and never touch
        # a file. When the entry is disabled it stays None and
        # GET /metrics is an ordinary unknown path.
        server.metrics = (  # type: ignore[attr-defined]
            metrics_mod.Metrics(int(time.time())) if enable_metrics else None)
        server.serve_forever()
