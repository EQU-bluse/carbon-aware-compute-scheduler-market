from __future__ import annotations

import hmac
import json
import re
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qsl, urlsplit

from . import audit

# Query parameters accepted by GET /audit, mapped straight onto
# audit.search. Anything else -- duplicated names, blank values, unknown
# names -- makes the request invalid.
_QUERY_FIELDS = frozenset(("cursor", "limit", "op", "stage", "key"))
# The page size is a decimal integer text from 1 to 1000 with no sign,
# no fraction and no leading zero; audit.search keeps the authoritative
# bound, but the textual form is an HTTP-layer concern.
_LIMIT_RE = re.compile(r"[1-9][0-9]*")
_OPS = ("copy", "restore")
_SEARCH_STAGES = ("成功", "校验", "执行", "同步", "回滚")
_TOKEN_HEADER = "X-Audit-Token"


class Handler(BaseHTTPRequestHandler):
    server_version = "CarbonMarket/0.1"

    # When both are set the authenticated GET /audit route is enabled and
    # queries this fixed audit file; clients can never name a path. The
    # base class leaves auditing off, so /audit falls through to 404.
    audit_path: str | None = None
    audit_token: str | None = None

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
        target = urlsplit(self.path)
        if target.path == "/health":
            self._json(HTTPStatus.OK, {"status": "ok"})
            return
        if self.audit_path is not None and target.path == "/audit":
            self._handle_audit(target.query)
            return
        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def _handle_audit(self, query: str) -> None:
        # Authorization runs before any parameter parsing or file access:
        # an unauthorized request must learn nothing about the journal's
        # existence or state.
        tokens = self.headers.get_all(_TOKEN_HEADER)
        if tokens is None or len(tokens) != 1 or not tokens[0].strip():
            # Missing, blank or repeated token: the request lacks usable
            # credentials at all.
            self._json(HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
            return
        # Constant-time comparison of the whole value, so a well-formed
        # but wrong token cannot be discovered byte by byte.
        if not hmac.compare_digest(self.audit_token or "", tokens[0]):
            self._json(HTTPStatus.FORBIDDEN, {"error": "forbidden"})
            return

        kwargs = self._parse_query(query)
        if kwargs is None:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_request"})
            return

        try:
            result = audit.search(self.audit_path, **kwargs)
        except FileNotFoundError:
            # Never echo the configured path or any system detail.
            self._json(HTTPStatus.NOT_FOUND, {"error": "audit_not_found"})
            return
        except ValueError:
            # Bad UTF-8/JSON, negative-zero literals, wrong version,
            # structure or field ordering in the journal itself.
            self._json(HTTPStatus.CONFLICT, {"error": "audit_invalid"})
            return
        except OSError:
            # Locking, opening, reading and every other I/O failure.
            self._json(HTTPStatus.SERVICE_UNAVAILABLE,
                       {"error": "audit_unavailable"})
            return
        self._json(HTTPStatus.OK, result)

    def _parse_query(self, query: str) -> dict[str, object] | None:
        # parse_qsl drops empty segments (a trailing or doubled "&"), so
        # reject them structurally first: an unnamed/blank pair is still
        # an unknown parameter.
        if query and any(segment == "" for segment in query.split("&")):
            return None
        # keep_blank_values lets a blank value surface as "" so it can be
        # rejected explicitly; errors="strict" turns a percent-encoded
        # byte sequence that is not valid UTF-8 into a decode failure.
        try:
            pairs = parse_qsl(query, keep_blank_values=True,
                             errors="strict")
        except (UnicodeDecodeError, ValueError):
            return None

        seen: set[str] = set()
        kwargs: dict[str, object] = {}
        for name, value in pairs:
            if name not in _QUERY_FIELDS or name in seen or value == "":
                return None
            seen.add(name)
            if name == "limit":
                if _LIMIT_RE.fullmatch(value) is None:
                    return None
                limit = int(value)
                if not 1 <= limit <= 1000:
                    return None
                kwargs["limit"] = limit
            elif name == "op":
                if value not in _OPS:
                    return None
                kwargs["op"] = value
            elif name == "stage":
                if value not in _SEARCH_STAGES:
                    return None
                kwargs["stage"] = value
            else:  # cursor / key: any non-empty text, rules as in search
                kwargs[name] = value
        return kwargs

    def _json(self, status: HTTPStatus, payload: object) -> None:
        # Compact JSON; non-ASCII is written through as UTF-8, matching
        # the audit journal's own serialization rather than escaping it.
        body = json.dumps(payload, ensure_ascii=False,
                          separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def _handler_for(audit_path: str, audit_token: str) -> type[Handler]:
    """Build a Handler class pinned to one audit file and token."""
    return type("AuditHandler", (Handler,), {
        "audit_path": audit_path,
        "audit_token": audit_token,
    })


def serve(host: str, port: int, audit_path: str | None = None,
          audit_token: str | None = None) -> None:
    if (audit_path is None) != (audit_token is None):
        raise ValueError("audit_path and audit_token must be given together")
    handler = Handler
    if audit_path is not None and audit_token is not None:
        if not audit_path or not audit_token:
            raise ValueError("audit_path and audit_token must be non-empty")
        handler = _handler_for(audit_path, audit_token)
    with ThreadingHTTPServer((host, port), handler) as server:
        server.serve_forever()
