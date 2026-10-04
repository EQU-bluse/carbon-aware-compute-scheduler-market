from __future__ import annotations

import hmac
import json
import re
import time
import urllib.parse
from collections.abc import Callable
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

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
# The read-only conditional queries -- the checkpoint download, GET
# /acceptance and GET /completions -- share one request pipeline (see
# Handler._snapshot_query); each entry keeps only its own error names
# here. "missing" is the absent ledger or checkpoint, "unknown" the
# absent exact-lookup key (the checkpoint download has no exact lookup
# and its snapshot read never raises KeyError), "invalid" the
# non-canonical content and "unavailable" any other I/O failure.
_CHECKPOINT_ERRORS = {
    "missing": "checkpoint_not_found",
    "invalid": "checkpoint_invalid",
    "unavailable": "checkpoint_unavailable",
}
_ACCEPTANCE_ERRORS = {
    "missing": "acceptance_not_found",
    "unknown": "acceptance_key_not_found",
    "invalid": "acceptance_invalid",
    "unavailable": "acceptance_unavailable",
}
_COMPLETION_ERRORS = {
    "missing": "completion_not_found",
    "unknown": "completion_job_not_found",
    "invalid": "completion_invalid",
    "unavailable": "completion_unavailable",
}
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

    def _read_consumer_body(self, operation: str) -> dict[str, object] | None:
        # One fixed JSON object per operation: exact field set, no
        # duplicates, non-boolean integer moments, no nulls beyond the
        # optional claim filters and no client-selected paths. Anything
        # else is a 400 and never reaches a ledger.
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

        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length < 0 or length > _MIGRATION_MAX_BODY:
                raise ValueError("bad content length")
            raw = self.rfile.read(length) if length else b""
            body = json.loads(raw.decode("utf-8"),
                             parse_constant=reject_constant,
                             object_pairs_hook=reject_duplicates)
        except (ValueError, UnicodeDecodeError):
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
            return None
        if not isinstance(body, dict) \
                or not required.issubset(body) \
                or not set(body).issubset(allowed):
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
            return None
        for name in ("consumer", "owner"):
            value = body[name]
            if not isinstance(value, str) or not value:
                self._bad_request()
                return None
        now = body["now"]
        if not isinstance(now, int) or isinstance(now, bool) or now < 0:
            self._bad_request()
            return None
        if operation == "claim":
            lease = body["lease"]
            if not isinstance(lease, int) or isinstance(lease, bool) \
                    or lease < 1:
                self._bad_request()
                return None
            for name in ("key", "job_id"):
                if name in body and body[name] is not None \
                        and (not isinstance(body[name], str)
                             or not body[name]):
                    self._bad_request()
                    return None
            if not isinstance(body["idem"], str) or not body["idem"]:
                self._bad_request()
                return None
        elif operation == "ack":
            position = body["position"]
            if not isinstance(position, int) or isinstance(position, bool) \
                    or position < 0:
                self._bad_request()
                return None
            if not isinstance(body["idem"], str) or not body["idem"]:
                self._bad_request()
                return None
        elif operation == "reject":
            position = body["position"]
            if not isinstance(position, int) or isinstance(position, bool) \
                    or position < 0:
                self._bad_request()
                return None
            # The content rule is a parameter validity check, enforced
            # here like position's range so an invalid reason answers
            # 400 before the subscription scope is read or the consumer
            # is looked up: surrounding whitespace is stripped and the
            # remainder must span 1..512 Unicode code points (code
            # points, not UTF-16 units or bytes).
            reason = body["reason"]
            if not isinstance(reason, str) \
                    or not 1 <= len(reason.strip()) <= 512:
                self._bad_request()
                return None
            if not isinstance(body["idem"], str) or not body["idem"]:
                self._bad_request()
                return None
        elif "limit" in body:
            limit = body["limit"]
            if not isinstance(limit, int) or isinstance(limit, bool) \
                    or not 1 <= limit <= _MAX_LIMIT:
                self._bad_request()
                return None
        return body

    def _bad_request(self) -> None:
        self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})

    def _migration_consumer(self, operation: str, query: str) -> None:
        coordination = getattr(self.server, "migration_batches")
        consumers = getattr(self.server, "migration_consumers")
        authorized, record = self._authorize()
        if not authorized:
            return
        # No endpoint takes a query string; checked after identity like
        # the other parameter validations, before the body and any file.
        if query:
            self._bad_request()
            return
        body = self._read_consumer_body(operation)
        if body is None:
            return
        consumer = body["consumer"]
        owner = body["owner"]
        now = body["now"]

        # Scope follows body validation and precedes the coordination
        # stream read. The operation and stage axes need no file and are
        # decided first. A claim carries the batch key in the request; a
        # pull or ack never names it, so the key axis reads the
        # consumer's persisted fixed subscription from this endpoint's
        # own control ledger (at the same stage the auth configuration
        # is re-read). The data-plane coordination and business ledgers
        # open only after the scope is allowed.
        if record is not None and (record.ops is not None
                                   or record.stages is not None):
            self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
            return
        batch_key: str | None
        if operation == "claim":
            batch_key = body.get("key")
        elif record is not None and record.keys is None:
            batch_key = None
        else:
            try:
                subscription = migration_batch.consumer_subscription(
                    coordination, consumers, consumer)
            except KeyError:
                self._json(HTTPStatus.NOT_FOUND,
                           {"error": "migration_consumer_not_found"})
                return
            except migration_batch.ConsumerLedgerInvalid:
                self._json(HTTPStatus.CONFLICT,
                           {"error": "migration_consumers_invalid"})
                return
            except FileNotFoundError:
                self._json(HTTPStatus.NOT_FOUND,
                           {"error": "migration_consumers_not_found"})
                return
            except OSError:
                self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                           {"error": "migration_consumers_unavailable"})
                return
            batch_key = subscription["key"]
        if record is not None and record.keys is not None:
            allowed = batch_key is not None and batch_key in record.keys
            if not allowed:
                self._json(HTTPStatus.FORBIDDEN,
                           {"error": "forbidden"})
                return

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

        try:
            payload = migration_batch.consume_response(
                coordination, consumers, operation, consumer, owner, now,
                **kwargs)
        except KeyError:
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_consumer_not_found"})
        except (migration_batch.ConsumerOwnershipError,
                migration_batch.ConsumerLeaseExpired):
            # A live lease owned by another caller, or an expired lease
            # the caller no longer holds: an ownership conflict. These
            # dedicated subclasses keep a filesystem PermissionError
            # from the commit falling through as 503 instead of 409.
            self._json(HTTPStatus.CONFLICT,
                       {"error": "migration_consumer_ownership"})
        except LookupError:
            # The confirmed cursor points at an event the current stream
            # truncated, rewrote or reused: a stream regression.
            self._json(HTTPStatus.CONFLICT,
                       {"error": "migration_consumer_checkpoint"})
        except migration_batch.CoordinationLedgerInvalid:
            self._json(HTTPStatus.CONFLICT,
                       {"error": "migration_batches_invalid"})
        except migration_batch.ConsumerLedgerInvalid:
            self._json(HTTPStatus.CONFLICT,
                       {"error": "migration_consumers_invalid"})
        except migration_batch.CoordinationLedgerMissing:
            # The fixed coordination ledger itself is missing; it keeps
            # the read-only endpoint's 404 and never leaks its path.
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_batches_not_found"})
        except migration_batch.ConsumersLedgerMissing:
            # The configured consumer ledger parent does not exist.
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_consumers_not_found"})
        except FileNotFoundError:
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_consumers_not_found"})
        except ValueError:
            # A changed idempotent request, a backward or past-tail ack,
            # a non-matching ack target or any other invalid argument.
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
        except OSError:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                       {"error": "migration_consumers_unavailable"})
        else:
            self._bytes(HTTPStatus.OK, payload)

    def _migration_consumer_status(self, query: str) -> None:
        # Read-only consumer status: identity, parameters, the
        # persisted subscription read for scope, and only then the
        # coordination/business snapshot -- a failure at one stage
        # never opens a later stage's files.
        coordination = getattr(self.server, "migration_batches")
        consumers = getattr(self.server, "migration_consumers")
        authorized, record = self._authorize()
        if not authorized:
            return
        try:
            params = _parse_migration_consumer_status_params(query)
        except ValueError:
            self._bad_request()
            return
        consumer = params["consumer"]
        now = int(params["now"])

        # Operation- and stage-scoped tokens can never read a consumer;
        # an unrestricted key scope reads any subscription. A key-scoped
        # token reads the consumer's fixed subscription from the
        # consumer ledger at the scope stage, exactly like a pull or
        # ack: only a fixed-batch subscription naming an allowed batch
        # passes, while a cross-batch or job-only range needs all three
        # scopes unrestricted. The data-plane coordination and business
        # ledgers open only after the scope is allowed.
        if record is not None and (record.ops is not None
                                   or record.stages is not None):
            self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
            return
        if record is not None and record.keys is not None:
            try:
                subscription = migration_batch.consumer_subscription(
                    coordination, consumers, consumer)
            except KeyError:
                self._json(HTTPStatus.NOT_FOUND,
                           {"error": "migration_consumer_not_found"})
                return
            except migration_batch.ConsumerLedgerInvalid:
                self._json(HTTPStatus.CONFLICT,
                           {"error": "migration_consumers_invalid"})
                return
            except FileNotFoundError:
                self._json(HTTPStatus.NOT_FOUND,
                           {"error": "migration_consumers_not_found"})
                return
            except OSError:
                self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                           {"error": "migration_consumers_unavailable"})
                return
            batch_key = subscription["key"]
            if batch_key is None or batch_key not in record.keys:
                self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
                return

        try:
            payload = migration_batch.consumer_status_response(
                coordination, consumers, consumer, now)
        except KeyError:
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_consumer_not_found"})
        except LookupError:
            # The confirmed cursor points at an event the current
            # stream truncated, rewrote or reused: a stream regression.
            self._json(HTTPStatus.CONFLICT,
                       {"error": "migration_consumer_checkpoint"})
        except migration_batch.CoordinationLedgerInvalid:
            self._json(HTTPStatus.CONFLICT,
                       {"error": "migration_batches_invalid"})
        except migration_batch.ConsumerLedgerInvalid:
            self._json(HTTPStatus.CONFLICT,
                       {"error": "migration_consumers_invalid"})
        except migration_batch.CoordinationLedgerMissing:
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_batches_not_found"})
        except migration_batch.ConsumersLedgerMissing:
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_consumers_not_found"})
        except FileNotFoundError:
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_consumers_not_found"})
        except ValueError:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
        except OSError:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                       {"error": "migration_consumers_unavailable"})
        else:
            self._bytes(HTTPStatus.OK, payload)

    def _migration_consumer_dead_letters(self, query: str) -> None:
        # Read-only dead-letter query: identity, parameters, the
        # persisted subscription read for scope, and only then the
        # consumer ledger's dead-letter section. The page is a
        # self-contained historical record, so the coordination and
        # business ledgers are never opened; a failure at one stage
        # still never opens a later stage's files.
        coordination = getattr(self.server, "migration_batches")
        consumers = getattr(self.server, "migration_consumers")
        authorized, record = self._authorize()
        if not authorized:
            return
        try:
            params = _parse_migration_dead_letters_params(query)
        except ValueError:
            self._bad_request()
            return
        consumer = params["consumer"]
        cursor = int(params["cursor"]) if "cursor" in params else None
        limit = int(params["limit"]) if "limit" in params else 100

        # Same scope order as status: operation- and stage-scoped
        # tokens are rejected before the subscription is read; a
        # key-scoped token reads the fixed subscription and passes only
        # a fixed-batch range naming an allowed batch.
        if record is not None and (record.ops is not None
                                   or record.stages is not None):
            self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
            return
        if record is not None and record.keys is not None:
            try:
                subscription = migration_batch.consumer_subscription(
                    coordination, consumers, consumer)
            except KeyError:
                self._json(HTTPStatus.NOT_FOUND,
                           {"error": "migration_consumer_not_found"})
                return
            except migration_batch.ConsumerLedgerInvalid:
                self._json(HTTPStatus.CONFLICT,
                           {"error": "migration_consumers_invalid"})
                return
            except FileNotFoundError:
                self._json(HTTPStatus.NOT_FOUND,
                           {"error": "migration_consumers_not_found"})
                return
            except OSError:
                self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                           {"error": "migration_consumers_unavailable"})
                return
            batch_key = subscription["key"]
            if batch_key is None or batch_key not in record.keys:
                self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
                return

        try:
            payload = migration_batch.consumer_dead_letters_response(
                coordination, consumers, consumer, cursor=cursor,
                limit=limit)
        except KeyError:
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_consumer_not_found"})
        except migration_batch.ConsumerLedgerInvalid:
            self._json(HTTPStatus.CONFLICT,
                       {"error": "migration_consumers_invalid"})
        except migration_batch.ConsumersLedgerMissing:
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_consumers_not_found"})
        except FileNotFoundError:
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_consumers_not_found"})
        except ValueError:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
        except OSError:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                       {"error": "migration_consumers_unavailable"})
        else:
            self._bytes(HTTPStatus.OK, payload)

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
        # The shared scope rule of the conditional queries: an exact
        # lookup requires unrestricted operation and stage scopes and a
        # key scope that is unrestricted or names the requested target;
        # a paginated query -- and the checkpoint download, which never
        # names a key -- requires all three scopes unrestricted.
        if record is None:
            return True
        if record.ops is not None or record.stages is not None:
            return False
        if exact is not None:
            return record.keys is None or exact in record.keys
        return record.keys is None

    def _snapshot_query(
            self, query: str,
            parse: Callable[[str], dict[str, str]],
            target: Callable[[dict[str, str]], str | None],
            fetch: Callable[[dict[str, str], str | None],
                            tuple[bytes | None, str]],
            errors: dict[str, str]) -> None:
        # The shared pipeline of the read-only conditional queries (the
        # checkpoint download, GET /acceptance and GET /completions):
        # authorize, validate the entry's own query string and the
        # conditional header, decide the scope, and only then open the
        # snapshot -- a failure at one stage never reaches a later
        # stage's files. ``parse`` validates the query string into the
        # entry's parameter dict; ``target`` maps the parameters to the
        # exact-lookup key (None for a paginated or parameterless
        # query); ``fetch`` reads the validated snapshot under its lock
        # and returns (body, etag) with body None on a conditional hit;
        # ``errors`` carries the entry's own error names.
        authorized, record = self._authorize()
        if not authorized:
            return
        try:
            params = parse(query)
        except ValueError:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
            return
        ok, condition = self._condition()
        if not ok:
            return
        # Scope checks follow parameter validation and precede any
        # ledger or checkpoint access: a forbidden request never opens
        # a file and never learns about records or the configuration.
        if not self._scope_allows(record, target(params)):
            self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
            return
        try:
            body, etag = fetch(params, condition)
        except FileNotFoundError:
            # Never leak the configured path or a system message.
            self._json(HTTPStatus.NOT_FOUND, {"error": errors["missing"]})
        except KeyError:
            self._json(HTTPStatus.NOT_FOUND, {"error": errors["unknown"]})
        except ValueError:
            self._json(HTTPStatus.CONFLICT, {"error": errors["invalid"]})
        except OSError:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                       {"error": errors["unavailable"]})
        else:
            # The 304 decision was made under the snapshot's lock
            # against the same serialized bytes a 200 carries, so a
            # concurrent write can never mix an old tag with a new
            # body.
            if body is None:
                self._raw(HTTPStatus.NOT_MODIFIED, b"", etag)
            else:
                self._raw(HTTPStatus.OK, body, etag)

    def _audit(self, query: str) -> None:
        authorized, record = self._authorize()
        if not authorized:
            return

        try:
            params = _parse_audit_params(query)
        except ValueError:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
            return

        # Scope checks follow parameter validation and precede any access
        # to the journal: a forbidden request never opens the audit file
        # and never learns about records, token names or the configuration.
        if record is not None and not auth.scope_allows(record, params):
            self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
            return

        kwargs: dict[str, object] = {}
        for name in ("cursor", "op", "stage", "key"):
            if name in params:
                kwargs[name] = params[name]
        if "limit" in params:
            kwargs["limit"] = int(params["limit"])

        try:
            result = audit.search(getattr(self.server, "audit_path"), **kwargs)
        except FileNotFoundError:
            # Never leak the configured path or the system message.
            self._json(HTTPStatus.NOT_FOUND, {"error": "audit_not_found"})
        except ValueError:
            self._json(HTTPStatus.CONFLICT, {"error": "audit_invalid"})
        except OSError:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                       {"error": "audit_unavailable"})
        else:
            self._json(HTTPStatus.OK, result)

    def _metrics(self) -> None:
        # The read-only process snapshot reuses the audit entry's
        # verification order: header presence and identity first
        # (401/503/403), then this entry's parameter and scope checks,
        # and only then is the snapshot read. The entry takes no
        # parameters, so any query string -- even an empty one after a
        # bare '?' -- is an invalid request.
        authorized, record = self._authorize()
        if not authorized:
            return
        if "?" in self.path:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
            return
        # A metrics token must be unrestricted on every audit axis:
        # operation, stage and history key all have to be wildcards.
        if record is not None and (record.ops is not None
                                   or record.stages is not None
                                   or record.keys is not None):
            self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
            return
        # The snapshot is taken before the response is sent (and this
        # request counted), so a successful GET /metrics never appears
        # in its own snapshot but is counted exactly once afterwards and
        # shows up in the next one.
        store = getattr(self.server, "metrics")
        self._json(HTTPStatus.OK, store.snapshot())

    def _proof(self, query: str) -> None:
        authorized, record = self._authorize()
        if not authorized:
            return

        try:
            params = _parse_proof_params(query)
        except ValueError:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
            return

        # Same order as the plain audit query: scopes are checked against
        # the validated filters before any file is opened.
        if record is not None and not auth.scope_allows(record, params):
            self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
            return

        kwargs: dict[str, object] = {}
        for name in ("cursor", "op", "stage", "key"):
            if name in params:
                kwargs[name] = params[name]
        if "limit" in params:
            kwargs["limit"] = int(params["limit"])

        try:
            proof = audit_proof.export(
                getattr(self.server, "audit_path"),
                getattr(self.server, "audit_checkpoint"),
                params["generation"],
                final=params.get("final") == "true",
                **kwargs)
        except FileNotFoundError:
            # A missing journal or checkpoint parent directory; the
            # configured paths never leak into the response.
            self._json(HTTPStatus.NOT_FOUND, {"error": "proof_not_found"})
        except ValueError:
            self._json(HTTPStatus.CONFLICT, {"error": "proof_invalid"})
        except OSError:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                       {"error": "proof_unavailable"})
        else:
            self._json(HTTPStatus.OK, proof)

    def _checkpoint(self, query: str) -> None:
        # The snapshot entry takes no query parameters; any query string
        # fails the same "unknown, repeated or empty parameter" parsing
        # used by the other endpoints, and the snapshot is always the
        # one checkpoint file fixed at startup.
        def fetch(_params: dict[str, str], condition: str | None):
            body, etag = audit_proof.read_snapshot(
                getattr(self.server, "audit_checkpoint"))
            # The 304 decision uses the ETag of the same validated bytes
            # a 200 would serve, so a concurrent export can never mix an
            # old tag with a new body.
            if condition == etag:
                return None, etag
            return body, etag

        self._snapshot_query(query, lambda q: _parse_query(q, ()),
                             lambda _params: None, fetch,
                             _CHECKPOINT_ERRORS)

    def _acceptance(self, query: str) -> None:
        ledger_dir = getattr(self.server, "acceptance_dir")

        def fetch(params: dict[str, str], condition: str | None):
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

        self._snapshot_query(query, _parse_acceptance_params,
                             lambda params: params.get("key"), fetch,
                             _ACCEPTANCE_ERRORS)

    def _completions(self, query: str) -> None:
        ledger = getattr(self.server, "completions_path")

        def fetch(params: dict[str, str], condition: str | None):
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

        self._snapshot_query(query, _parse_completions_params,
                             lambda params: params.get("job"), fetch,
                             _COMPLETION_ERRORS)

    def _migration_batches(self, query: str) -> None:
        ledger = getattr(self.server, "migration_batches")
        authorized, record = self._authorize()
        if not authorized:
            return

        try:
            params = _parse_migration_batches_params(query)
        except ValueError:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
            return

        # Scope checks follow parameter validation and precede any
        # ledger access: an exact lookup requires unrestricted operation
        # and stage scopes and a key scope that is unrestricted or names
        # the requested batch; a paginated query requires all three
        # scopes unrestricted. A forbidden request never opens the
        # coordination ledger or a business ledger.
        if record is not None:
            if record.ops is not None or record.stages is not None:
                self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
                return
            if "key" in params:
                if record.keys is not None \
                        and params["key"] not in record.keys:
                    self._json(
                        HTTPStatus.FORBIDDEN, {"error": "forbidden"})
                    return
            elif record.keys is not None:
                self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
                return

        try:
            if "key" in params:
                # The exact lookup returns only the batch snapshot; the
                # shared locks cover the coordination file, the business
                # snapshots and the serialization, so the body is one
                # complete version.
                body = migration_batch.get_response(ledger, params["key"])
            else:
                kwargs: dict[str, object] = {}
                if "cursor" in params:
                    kwargs["cursor"] = params["cursor"]
                if "limit" in params:
                    kwargs["limit"] = int(params["limit"])
                body = migration_batch.search_response(ledger, **kwargs)
        except FileNotFoundError:
            # The fixed coordination ledger itself is missing. Never
            # leak its path or a system message.
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_batches_not_found"})
        except KeyError:
            # A canonical coordination ledger that lacks the key.
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_batch_not_found"})
        except ValueError:
            # Non-canonical content, a broken reference or a referenced
            # business ledger that is missing.
            self._json(HTTPStatus.CONFLICT,
                       {"error": "migration_batches_invalid"})
        except OSError:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                       {"error": "migration_batches_unavailable"})
        else:
            self._bytes(HTTPStatus.OK, body)

    def _migration_batch_events(self, query: str) -> None:
        ledger = getattr(self.server, "migration_batches")
        authorized, record = self._authorize()
        if not authorized:
            return

        try:
            params = _parse_migration_events_params(query)
        except ValueError:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
            return

        # Same scope order as the snapshot query and before any file is
        # opened. A batch-key filter needs unrestricted operation and
        # stage scopes and a key scope that is unrestricted or names the
        # requested batch; cross-batch reads need all three scopes
        # unrestricted. The job filter alone never lifts the
        # cross-batch requirement.
        if record is not None:
            if record.ops is not None or record.stages is not None:
                self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
                return
            if "key" in params:
                if record.keys is not None \
                        and params["key"] not in record.keys:
                    self._json(
                        HTTPStatus.FORBIDDEN, {"error": "forbidden"})
                    return
            elif record.keys is not None:
                self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
                return

        kwargs: dict[str, object] = {}
        if "cursor" in params:
            kwargs["cursor"] = int(params["cursor"])
        if "limit" in params:
            kwargs["limit"] = int(params["limit"])
        if "key" in params:
            kwargs["key"] = params["key"]
        if "job" in params:
            kwargs["job_id"] = params["job"]
        try:
            body = migration_batch.events_response(ledger, **kwargs)
        except FileNotFoundError:
            # The fixed coordination ledger itself is missing; never
            # leak its path or a system message.
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_batches_not_found"})
        except ValueError:
            # Non-canonical content, a broken reference or a referenced
            # business ledger that is missing.
            self._json(HTTPStatus.CONFLICT,
                       {"error": "migration_batches_invalid"})
        except OSError:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                       {"error": "migration_batches_unavailable"})
        else:
            self._bytes(HTTPStatus.OK, body)

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