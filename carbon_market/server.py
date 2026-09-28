from __future__ import annotations

import hmac
import json
import re
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import acceptance, audit, audit_proof, auth, migration_batch
from ._jsonio import _parse_float, _parse_int, _reject_constant


def _loads_consumer_body(text: str) -> object:
    # Finite JSON (no negative-zero or NaN/Infinity literals) with no
    # duplicate object names: a body carrying the same field twice is
    # ambiguous and rejected instead of silently taking the last value.
    def pairs_hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        names = {name for name, _value in pairs}
        if len(names) != len(pairs):
            raise ValueError("duplicate JSON object member")
        return dict(pairs)

    return json.loads(text, parse_int=_parse_int, parse_float=_parse_float,
                      parse_constant=_reject_constant,
                      object_pairs_hook=pairs_hook)


class _InvalidConsumerRequest(Exception):
    """A consumer POST body that is not the fixed JSON object shape."""


class _ConsumerScopeForbidden(PermissionError):
    """The token's scopes do not cover the effective batch range."""


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
# POST /migration-consumers/{claim,fetch,confirm}: the write requests
# accept only a fixed JSON object each; the coordination and consumer
# ledger paths are fixed at startup and can never be named in a body.
_CONSUMER_CLAIM_FIELDS = frozenset(
    ("consumer", "owner", "lease", "now", "key", "job",
     "idempotency_key"))
_CONSUMER_FETCH_FIELDS = frozenset(
    ("consumer", "owner", "now", "cursor", "limit"))
_CONSUMER_CONFIRM_FIELDS = frozenset(
    ("consumer", "owner", "now", "position", "idempotency_key"))
_CONSUMER_PATHS = {
    "/migration-consumers/claim": "claim",
    "/migration-consumers/fetch": "fetch",
    "/migration-consumers/confirm": "confirm",
}
_ACCEPTANCE_STATES = ("pending", "active", "quarantined")
_OPS = ("copy", "restore")
_STAGES = ("成功", "校验", "执行", "同步", "回滚")
_MAX_LIMIT = 1000
# A conditional download accepts exactly one strong entity tag: one
# double-quoted string of 64 lowercase hexadecimal digits, the SHA-256
# of the full validated response bytes. Weak tags, lists, wildcards and
# surrounding whitespace are invalid requests.
_ETAG_RE = re.compile(r'"[0-9a-f]{64}"')


def _parse_consumer_body(handler: "Handler", op: str) -> dict[str, object]:
    # Each consumer op accepts exactly one JSON object with that op's
    # fixed field set: no unknown fields, no duplicates possible in a
    # JSON object, no file paths the client could choose, no NaN or
    # negative-zero numeric literals. The body is length-bounded so a
    # client can never make the service buffer without bound.
    allowed = {
        "claim": _CONSUMER_CLAIM_FIELDS,
        "fetch": _CONSUMER_FETCH_FIELDS,
        "confirm": _CONSUMER_CONFIRM_FIELDS,
    }[op]
    required = {
        "claim": ("consumer", "owner", "now", "lease"),
        "fetch": ("consumer", "owner", "now"),
        "confirm": ("consumer", "owner", "now", "position"),
    }[op]
    try:
        length = int(handler.headers.get("Content-Length", "0"))
    except ValueError:
        raise _InvalidConsumerRequest("bad content length")
    if length <= 0 or length > 65536:
        raise _InvalidConsumerRequest("bad content length")
    raw = handler.rfile.read(length)
    if len(raw) != length:
        raise _InvalidConsumerRequest("short body")
    try:
        data = _loads_consumer_body(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        raise _InvalidConsumerRequest("body is not finite JSON") from None
    if not isinstance(data, dict):
        raise _InvalidConsumerRequest("body must be a JSON object")
    fields = set(data.keys())
    if not fields.issubset(allowed) or not fields.issuperset(required):
        raise _InvalidConsumerRequest("invalid field set")
    for name in ("consumer", "owner"):
        if not isinstance(data[name], str) or not data[name]:
            raise _InvalidConsumerRequest(f"{name} must be a non-empty string")
    now = data["now"]
    if not isinstance(now, int) or isinstance(now, bool) or now < 0:
        raise _InvalidConsumerRequest("now must be a non-negative integer")
    if op == "claim":
        lease = data["lease"]
        if not isinstance(lease, int) or isinstance(lease, bool) \
                or lease < 1:
            raise _InvalidConsumerRequest(
                "lease must be a positive integer")
        for name in ("key", "job", "idempotency_key"):
            if name in data and (not isinstance(data[name], str)
                                 or not data[name]):
                raise _InvalidConsumerRequest(
                    f"{name} must be a non-empty string")
    elif op == "fetch":
        if "cursor" in data:
            cursor = data["cursor"]
            if not isinstance(cursor, int) or isinstance(cursor, bool) \
                    or cursor < 0:
                raise _InvalidConsumerRequest(
                    "cursor must be a non-negative integer")
        if "limit" in data:
            limit = data["limit"]
            if not isinstance(limit, int) or isinstance(limit, bool) \
                    or not 1 <= limit <= _MAX_LIMIT:
                raise _InvalidConsumerRequest(
                    "limit must be between 1 and 1000")
    else:
        position = data["position"]
        if not isinstance(position, int) or isinstance(position, bool) \
                or position < 0:
            raise _InvalidConsumerRequest(
                "position must be a non-negative integer")
        if "idempotency_key" in data \
                and (not isinstance(data["idempotency_key"], str)
                     or not data["idempotency_key"]):
            raise _InvalidConsumerRequest(
                "idempotency_key must be a non-empty string")
    return data


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


class Handler(BaseHTTPRequestHandler):
    server_version = "CarbonMarket/0.1"

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        if self.path == "/health":
            self._json(HTTPStatus.OK, {"status": "ok"})
            return
        path, _, query = self.path.partition("?")
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
        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        path, _, query = self.path.partition("?")
        op = _CONSUMER_PATHS.get(path)
        if op is not None \
                and getattr(self.server, "migration_consumers", None) \
                is not None:
            self._migration_consume(op, query)
            return
        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

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
        authorized, record = self._authorize()
        if not authorized:
            return

        # The snapshot entry takes no query parameters; any query string
        # fails the same "unknown, repeated or empty parameter" parsing
        # used by the other endpoints.
        try:
            _parse_query(query, ())
        except ValueError:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
            return

        # The conditional header is validated before the checkpoint is
        # ever opened: absent (allowed) or exactly one strong tag of the
        # documented shape. A blank value, a repeated header, a weak tag,
        # a list, a wildcard or any other shape is an invalid request.
        conditions = self.headers.get_all("If-None-Match")
        condition: str | None = None
        if conditions is not None:
            if len(conditions) != 1 or not _ETAG_RE.fullmatch(conditions[0]):
                self._json(HTTPStatus.BAD_REQUEST,
                           {"error": "invalid_request"})
                return
            condition = conditions[0][1:-1]

        # A scoped token may only audit through its filters; downloading
        # the whole snapshot requires unrestricted scope on every axis.
        if record is not None and (record.ops is not None
                                   or record.stages is not None
                                   or record.keys is not None):
            self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
            return

        try:
            body, etag = audit_proof.read_snapshot(
                getattr(self.server, "audit_checkpoint"))
        except FileNotFoundError:
            # Never leak the configured path or a system message.
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "checkpoint_not_found"})
        except ValueError:
            self._json(HTTPStatus.CONFLICT, {"error": "checkpoint_invalid"})
        except OSError:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                       {"error": "checkpoint_unavailable"})
        else:
            # The 304 decision uses the ETag of the same validated bytes
            # a 200 would serve, so a concurrent export can never mix an
            # old tag with a new body.
            if condition == etag:
                self._raw(HTTPStatus.NOT_MODIFIED, b"", etag)
            else:
                self._raw(HTTPStatus.OK, body, etag)

    def _acceptance(self, query: str) -> None:
        authorized, record = self._authorize()
        if not authorized:
            return

        try:
            params = _parse_acceptance_params(query)
        except ValueError:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
            return

        # The conditional header is validated together with the query,
        # before the scope decision and any ledger access: absent
        # (allowed) or exactly one strong tag of the documented shape.
        # A blank value, a repeated header, a weak tag, a list, a
        # wildcard or any other shape is an invalid request that never
        # opens the ledger.
        conditions = self.headers.get_all("If-None-Match")
        condition: str | None = None
        if conditions is not None:
            if len(conditions) != 1 or not _ETAG_RE.fullmatch(conditions[0]):
                self._json(HTTPStatus.BAD_REQUEST,
                           {"error": "invalid_request"})
                return
            condition = conditions[0][1:-1]

        # Scope checks follow parameter validation and precede any
        # ledger access: an exact lookup requires unrestricted operation
        # and stage scopes and a key scope that is unrestricted or names
        # the requested key; a paginated query requires all three scopes
        # unrestricted. A forbidden request never opens the ledger.
        if record is not None:
            if record.ops is not None or record.stages is not None:
                self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
                return
            if "key" in params:
                if record.keys is not None \
                        and params["key"] not in record.keys:
                    self._json(HTTPStatus.FORBIDDEN,
                               {"error": "forbidden"})
                    return
            elif record.keys is not None:
                self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
                return

        ledger_dir = getattr(self.server, "acceptance_dir")
        try:
            if "key" in params:
                body, etag = acceptance.get_response(
                    ledger_dir, params["key"], condition)
            else:
                kwargs: dict[str, object] = {}
                for name in ("cursor", "state"):
                    if name in params:
                        kwargs[name] = params[name]
                if "limit" in params:
                    kwargs["limit"] = int(params["limit"])
                body, etag = acceptance.search_response(
                    ledger_dir, condition=condition, **kwargs)
        except FileNotFoundError:
            # Never leak the configured path or the system message.
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "acceptance_not_found"})
        except KeyError:
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "acceptance_key_not_found"})
        except ValueError:
            self._json(HTTPStatus.CONFLICT, {"error": "acceptance_invalid"})
        except OSError:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                       {"error": "acceptance_unavailable"})
        else:
            # The 304 decision was made under the ledger's shared lock
            # against the same serialized bytes a 200 carries, so a
            # concurrent submission can never mix an old tag with a new
            # body.
            if body is None:
                self._raw(HTTPStatus.NOT_MODIFIED, b"", etag)
            else:
                self._raw(HTTPStatus.OK, body, etag)

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

    def _migration_consume(self, op: str, query: str = "") -> None:
        ledger = getattr(self.server, "migration_batches")
        consumers = getattr(self.server, "migration_consumers")
        authorized, record = self._authorize()
        if not authorized:
            return

        # The consumer endpoints take no query parameters; any query
        # string is an illegal request, validated after identity and
        # before the body is parsed (the same order as the GET
        # endpoints).
        try:
            _parse_query(query, ())
        except ValueError:
            self._json(HTTPStatus.BAD_REQUEST,
                       {"error": "invalid_request"})
            return

        try:
            request = _parse_consumer_body(self, op)
        except _InvalidConsumerRequest:
            self._json(HTTPStatus.BAD_REQUEST,
                       {"error": "invalid_request"})
            return

        # The scope gate runs inside the library call at the exact
        # point the effective batch key is known: the claim's own key
        # for a claim, the stored subscription's key for a fetch or a
        # confirm. A fixed-batch subscription needs unrestricted
        # operation and stage scopes and a key scope that names that
        # batch; a cross-batch or job-only subscription needs all three
        # scopes unrestricted. A forbidden result is decided there,
        # before any write.
        def scope_gate(effective_key: str | None) -> None:
            if record is None:
                return
            if record.ops is not None or record.stages is not None:
                raise _ConsumerScopeForbidden()
            if effective_key is None:
                if record.keys is not None:
                    raise _ConsumerScopeForbidden()
            elif record.keys is not None \
                    and effective_key not in record.keys:
                raise _ConsumerScopeForbidden()

        kwargs: dict[str, object] = {
            "now": request["now"], "owner": request["owner"],
            "scope": scope_gate,
        }
        if "lease" in request:
            kwargs["lease"] = request["lease"]
        if "key" in request:
            kwargs["key"] = request["key"]
        if "job" in request:
            kwargs["job_id"] = request["job"]
        if "position" in request:
            kwargs["position"] = request["position"]
        if "cursor" in request:
            kwargs["cursor"] = request["cursor"]
        if "limit" in request:
            kwargs["limit"] = request["limit"]
        if "idempotency_key" in request:
            kwargs["idempotency_key"] = request["idempotency_key"]

        try:
            body = migration_batch.consume_response(
                ledger, consumers, op, request["consumer"], **kwargs)
        except _ConsumerScopeForbidden:
            self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
        except KeyError:
            # A canonical consumer ledger that does not name the
            # consumer.
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_consumer_not_found"})
        except migration_batch._ConsumersParentMissing:
            # The consumer ledger's fixed parent directory is absent.
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_consumers_not_found"})
        except FileNotFoundError:
            # The fixed coordination ledger is missing. Never leak a
            # path.
            self._json(HTTPStatus.NOT_FOUND,
                       {"error": "migration_batches_not_found"})
        except migration_batch._ConsumerLedgerInvalid as exc:
            # Non-canonical stored bytes; each fixed ledger keeps its
            # own error name.
            if exc.source == "coordination":
                self._json(
                    HTTPStatus.CONFLICT,
                    {"error": "migration_batches_invalid"})
            else:
                self._json(
                    HTTPStatus.CONFLICT,
                    {"error": "migration_consumers_invalid"})
        except LookupError:
            # The stream truncated or rewrote the confirmed event.
            self._json(HTTPStatus.CONFLICT,
                       {"error": "migration_consumer_conflict"})
        except PermissionError:
            # Another owner holds the subscription inside its lease.
            self._json(HTTPStatus.CONFLICT,
                       {"error": "migration_consumer_conflict"})
        except TimeoutError:
            # The acting owner's lease has strictly expired.
            self._json(HTTPStatus.CONFLICT,
                       {"error": "migration_consumer_conflict"})
        except ValueError:
            # A bad request the library refused (changed filter or
            # idempotency key, a backwards or out-of-stream
            # confirmation, ...).
            self._json(HTTPStatus.BAD_REQUEST,
                       {"error": "invalid_request"})
        except OSError:
            self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                       {"error": "migration_consumers_unavailable"})
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
          migration_consumers: str | None = None) -> None:
    with ThreadingHTTPServer((host, port), Handler) as server:
        server.audit_path = audit_path  # type: ignore[attr-defined]
        server.audit_token = token  # type: ignore[attr-defined]
        server.audit_auth = auth  # type: ignore[attr-defined]
        server.audit_checkpoint = checkpoint  # type: ignore[attr-defined]
        server.acceptance_dir = acceptance  # type: ignore[attr-defined]
        server.migration_batches = migration_batches  # type: ignore[attr-defined]
        server.migration_consumers = migration_consumers  # type: ignore[attr-defined]
        server.serve_forever()