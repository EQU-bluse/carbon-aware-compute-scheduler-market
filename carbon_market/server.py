from __future__ import annotations

import hmac
import json
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import audit

# Query parameters GET /audit accepts; anything else is an invalid request.
_AUDIT_PARAMS = ("cursor", "limit", "op", "stage", "key")
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


def _parse_audit_params(query: str) -> dict[str, str]:
    # Every parameter must be one of the allowed names, appear at most once
    # and carry a non-empty value; anything else is an invalid request.
    params: dict[str, str] = {}
    if not query:
        return params
    for piece in query.split("&"):
        name, _, value = piece.partition("=")
        name = _decode_component(name)
        value = _decode_component(value)
        if name not in _AUDIT_PARAMS or name in params or not value:
            raise ValueError(f"invalid query parameter: {name!r}")
        params[name] = value

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
        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def _audit(self, query: str) -> None:
        # Authorization comes first: an unauthorized request learns nothing
        # about the query, the configured path or the journal's state.
        tokens = self.headers.get_all("X-Audit-Token")
        if tokens is None or len(tokens) != 1 or not tokens[0].strip():
            self._json(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
            return
        expected = getattr(self.server, "audit_token").encode("utf-8")
        if not hmac.compare_digest(tokens[0].encode("utf-8"), expected):
            self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
            return

        try:
            params = _parse_audit_params(query)
        except ValueError:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
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
          token: str | None = None) -> None:
    with ThreadingHTTPServer((host, port), Handler) as server:
        server.audit_path = audit_path  # type: ignore[attr-defined]
        server.audit_token = token  # type: ignore[attr-defined]
        server.serve_forever()
