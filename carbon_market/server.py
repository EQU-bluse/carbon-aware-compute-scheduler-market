from __future__ import annotations

import hmac
import json
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import audit, audit_proof, auth

# Query parameters GET /audit accepts; anything else is an invalid request.
_AUDIT_PARAMS = ("cursor", "limit", "op", "stage", "key")
# GET /audit/proof additionally takes the mandatory generation name and
# the optional closing flag; the audit and checkpoint paths are fixed by
# the server and can never be selected through the query.
_PROOF_PARAMS = ("generation", "final") + _AUDIT_PARAMS
_OPS = ("copy", "restore")
_STAGES = ("成功", "校验", "执行", "同步", "回滚")
_MAX_LIMIT = 1000


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

    def _json(self, status: HTTPStatus, payload: object) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def serve(host: str, port: int, audit_path: str | None = None,
          token: str | None = None, auth: str | None = None,
          checkpoint: str | None = None) -> None:
    with ThreadingHTTPServer((host, port), Handler) as server:
        server.audit_path = audit_path  # type: ignore[attr-defined]
        server.audit_token = token  # type: ignore[attr-defined]
        server.audit_auth = auth  # type: ignore[attr-defined]
        server.audit_checkpoint = checkpoint  # type: ignore[attr-defined]
        server.serve_forever()
