"""HTTP surface for the authenticated audit search endpoint.

Covers GET /audit served only when --audit/--token are both configured:
the header-token contract (missing/blank/repeated -> 401, wrong -> 403,
checked before parameters or the file are touched), query parameter
mapping onto audit.search (cursor/limit/op/stage/key, defaults and
invalid forms -> 400 invalid_request, including non-UTF-8 percent
encoding), the 200 response carrying the search result verbatim, and the
file-state errors 404 audit_not_found / 409 audit_invalid / 503
audit_unavailable with no path or exception detail leaked. Also covers
the unchanged /health and generic 404, and the CLI pair validation.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from tempfile import TemporaryDirectory
from unittest import mock

from carbon_market import audit
from carbon_market.server import Handler, _handler_for


def _event(op: str = "copy", target: str = "t.history", key: str = "alpha",
           changed: bool = True, error: str | None = None,
           stage: str | None = None) -> dict[str, object]:
    return {"op": op, "target": target, "key": key, "changed": changed,
            "error": error, "stage": stage}


class _Server:
    def __init__(self, handler: type[Handler]) -> None:
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       daemon=True)
        self.thread.start()

    @property
    def port(self) -> int:
        return self.server.server_port

    def request(self, path: str, token: str | object = ...,
                raw_headers: list[tuple[str, str]] | None = None
                ) -> tuple[int, dict[str, object], bytes, dict[str, str]]:
        connection = HTTPConnection("127.0.0.1", self.port, timeout=2)
        headers: dict[str, str]
        if raw_headers is not None:
            # Bypass http.client's header dict so repeated or odd headers
            # can be sent exactly as given.
            connection.connect()
            lines = [f"GET {path} HTTP/1.1", "Host: test", "Connection: close"]
            lines.extend(f"{name}: {value}" for name, value in raw_headers)
            connection.sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode())
            raw = b""
            while True:
                chunk = connection.sock.recv(4096)
                if not chunk:
                    break
                raw += chunk
            head, _, body = raw.partition(b"\r\n\r\n")
            status = int(head.split(b"\r\n", 1)[0].split()[1])
            response_headers = {}
            for line in head.split(b"\r\n")[1:]:
                name, _, value = line.partition(b":")
                response_headers[name.decode().strip().lower()] = (
                    value.decode().strip())
            connection.close()
            parsed = json.loads(body) if body else {}
            return status, parsed, body, response_headers
        headers = {}
        if token is not ...:
            headers["X-Audit-Token"] = token  # type: ignore[assignment]
        connection.request("GET", path, headers=headers)
        response = connection.getresponse()
        body = response.read()
        response_headers = {k.lower(): v for k, v in response.getheaders()}
        connection.close()
        parsed: dict[str, object] = {}
        if body:
            parsed = json.loads(body)
        return response.status, parsed, body, response_headers

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class _Fixture(unittest.TestCase):
    token = "test-token"

    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        events = {
            "k-01": _event(op="copy", key="alpha"),
            "k-02": _event(op="restore", key="beta", changed=False,
                           error="ValueError", stage="校验"),
            "k-03": _event(op="copy", key="alpha", changed=False),
        }
        for audit_key, event in events.items():
            audit.record(self.journal, audit_key, event)
        self.handler = _handler_for(self.journal, self.token)
        self.server = _Server(self.handler)
        self.addCleanup(self.server.close)

    def get(self, path: str, token: str | object = ...):
        return self.server.request(path, token)


class RoutingTest(unittest.TestCase):
    def test_health_and_404_unchanged_with_audit_disabled(self) -> None:
        server = _Server(Handler)
        try:
            status, body, _, _ = server.request("/health")
            self.assertEqual(status, 200)
            self.assertEqual(body, {"status": "ok"})
            for path in ("/audit", "/audit/", "/auditx", "/missing"):
                status, body, raw, headers = server.request(path, "anything")
                self.assertEqual(status, 404)
                self.assertEqual(body, {"error": "not_found"})
                self.assertEqual(headers["content-type"], "application/json")
                self.assertEqual(raw, b'{"error":"not_found"}')
        finally:
            server.close()


class TokenAuthTest(_Fixture):
    def test_missing_blank_and_repeated_token_are_401(self) -> None:
        status, body, raw, headers = self.get("/audit")
        self.assertEqual((status, body), (401, {"error": "unauthorized"}))
        self.assertEqual(headers["content-type"], "application/json")
        self.assertEqual(set(body), {"error"})
        # A value that is all whitespace carries no token. Leading OWS is
        # stripped by HTTP parsing, so it only reads as blank when the
        # whole value is whitespace.
        for value in ("", "   ", "\t"):
            status, body, _, _ = self.server.request(
                "/audit", raw_headers=[("X-Audit-Token", value)])
            self.assertEqual(status, 401, repr(value))
            self.assertEqual(body, {"error": "unauthorized"})
        status, body, _, _ = self.server.request(
            "/audit", raw_headers=[("X-Audit-Token", "a"),
                                   ("X-Audit-Token", self.token)])
        self.assertEqual((status, body), (401, {"error": "unauthorized"}))

    def test_well_formed_wrong_token_is_403(self) -> None:
        # Trailing whitespace is part of the field value and mismatches;
        # leading OWS is stripped by HTTP parsing and therefore matches.
        for value in ("wrong", "test-token ", " test-token ", "TEST-TOKEN"):
            status, body, _, _ = self.get("/audit", value)
            self.assertEqual(status, 403, value)
            self.assertEqual(body, {"error": "forbidden"})
        status, _, _, _ = self.get("/audit", " test-token")
        self.assertEqual(status, 200)

    def test_correct_token_returns_200(self) -> None:
        status, body, _, _ = self.get("/audit", self.token)
        self.assertEqual(status, 200)
        self.assertEqual([k for k, _ in body["events"]],
                         ["k-01", "k-02", "k-03"])
        self.assertIsNone(body["next"])

    def test_authorization_precedes_parameters_and_file(self) -> None:
        # A bad query string and even an unreadable/missing file must not
        # change the response for an unauthorized caller.
        missing = _handler_for(os.path.join(self.tmp.name, "missing.json"),
                               self.token)
        server = _Server(missing)
        try:
            status, body, _, _ = server.request("/audit?bogus=1")
            self.assertEqual((status, body), (401, {"error": "unauthorized"}))
            status, body, _, _ = server.request("/audit?bogus=1", "nope")
            self.assertEqual((status, body), (403, {"error": "forbidden"}))
        finally:
            server.close()


class QueryMappingTest(_Fixture):
    def test_result_shape_and_field_and_event_order_verbatim(self) -> None:
        status, body, raw, _ = self.get("/audit", self.token)
        self.assertEqual(status, 200)
        self.assertEqual(list(body), ["events", "next"])
        self.assertEqual([k for k, _ in body["events"]],
                         ["k-01", "k-02", "k-03"])
        for _, event in body["events"]:
            self.assertEqual(
                list(event),
                ["op", "target", "key", "changed", "error", "stage"])
        # Compact JSON with the Chinese stage written through as UTF-8,
        # not escaped, and events ahead of next.
        self.assertEqual(
            raw,
            b'{"events":[["k-01",{"op":"copy","target":"t.history",'
            b'"key":"alpha","changed":true,"error":null,"stage":null}],'
            b'["k-02",{"op":"restore","target":"t.history","key":"beta",'
            b'"changed":false,"error":"ValueError","stage":"\xe6\xa0\xa1'
            b'\xe9\xaa\x8c"}],["k-03",{"op":"copy","target":"t.history",'
            b'"key":"alpha","changed":false,"error":null,"stage":null}]],'
            b'"next":null}')

    def test_cursor_limit_and_filters_map_to_search(self) -> None:
        status, body, _, _ = self.get("/audit?cursor=k-01", self.token)
        self.assertEqual(status, 200)
        self.assertEqual([k for k, _ in body["events"]], ["k-02", "k-03"])

        status, body, _, _ = self.get("/audit?limit=1", self.token)
        self.assertEqual([k for k, _ in body["events"]], ["k-01"])
        self.assertEqual(body["next"], "k-01")

        status, body, _, _ = self.get("/audit?op=copy&key=alpha", self.token)
        self.assertEqual([k for k, _ in body["events"]], ["k-01", "k-03"])

        status, body, _, _ = self.get("/audit?stage=%E6%A0%A1%E9%AA%8C",
                                      self.token)
        self.assertEqual([k for k, _ in body["events"]], ["k-02"])

        status, body, _, _ = self.get("/audit?stage=%E6%88%90%E5%8A%9F",
                                      self.token)
        self.assertEqual([k for k, _ in body["events"]], ["k-01", "k-03"])

    def test_limit_boundaries_and_decimal_text(self) -> None:
        for value, ok in (("1", True), ("1000", True), ("100", True),
                          ("0", False), ("1001", False), ("01", False),
                          ("1.0", False), ("-1", False), ("1e2", False),
                          ("true", False), ("%201", False), ("+1", False),
                          ("", False)):
            status, _, _, _ = self.get(f"/audit?limit={value}", self.token)
            self.assertEqual(status, 200 if ok else 400, value)

    def test_invalid_queries_are_400_invalid_request(self) -> None:
        for query in (
            "op=delete", "stage=nope", "op=", "key=", "cursor=",
            "unknown=1", "op=copy&op=restore", "limit=1&limit=2",
            "op=copy&op=copy", "=copy", "op=copy&", "limit=01",
        ):
            status, body, _, _ = self.get(f"/audit?{query}", self.token)
            self.assertEqual(status, 400, query)
            self.assertEqual(body, {"error": "invalid_request"}, query)

    def test_non_utf8_percent_encoding_is_400(self) -> None:
        status, body, _, _ = self.get("/audit?key=%ff", self.token)
        self.assertEqual((status, body),
                         (400, {"error": "invalid_request"}))

    def test_pagination_cursor_round_trips_through_http(self) -> None:
        seen: list[str] = []
        cursor = ""
        pages = 0
        while True:
            path = "/audit?limit=2" + (f"&cursor={cursor}" if cursor else "")
            status, body, _, _ = self.get(path, self.token)
            self.assertEqual(status, 200)
            pages += 1
            seen.extend(k for k, _ in body["events"])
            if body["next"] is None:
                break
            cursor = body["next"]
        self.assertEqual(seen, ["k-01", "k-02", "k-03"])
        self.assertEqual(pages, 2)


class FileStateTest(_Fixture):
    def _start(self, path: str) -> _Server:
        return _Server(_handler_for(path, self.token))

    def test_missing_audit_file_is_404_without_detail(self) -> None:
        server = self._start(os.path.join(self.tmp.name, "missing.json"))
        try:
            status, body, raw, _ = server.request("/audit", self.token)
            self.assertEqual((status, body),
                             (404, {"error": "audit_not_found"}))
            self.assertNotIn(self.tmp.name.encode(), raw)
        finally:
            server.close()

    def test_invalid_audit_content_is_409(self) -> None:
        for index, content in enumerate((
            b"{not json",
            b'{"events":{},"version":1}',
            b'{"version":-0,"events":{}}',
            b"\xff\xfe",
        )):
            path = os.path.join(self.tmp.name, f"bad-{index}.json")
            with open(path, "wb") as handle:
                handle.write(content)
            server = self._start(path)
            try:
                status, body, raw, _ = server.request("/audit", self.token)
                self.assertEqual(status, 409, content)
                self.assertEqual(body, {"error": "audit_invalid"})
                self.assertNotIn(b"Traceback", raw)
            finally:
                server.close()

    def test_other_oserror_is_503_without_detail(self) -> None:
        with mock.patch("carbon_market.audit._read_existing_document",
                        side_effect=OSError("secret /dev/sdX failure")):
            status, body, raw, _ = self.get("/audit", self.token)
        self.assertEqual((status, body),
                         (503, {"error": "audit_unavailable"}))
        self.assertNotIn(b"secret", raw)
        self.assertNotIn(b"sdX", raw)

    def test_path_cannot_be_supplied_by_the_client(self) -> None:
        for query in ("path=/etc/passwd", "audit=/etc/passwd",
                      "file=/etc/passwd"):
            status, body, _, _ = self.get(f"/audit?{query}", self.token)
            self.assertEqual((status, body),
                             (400, {"error": "invalid_request"}))

    def test_search_stays_read_only(self) -> None:
        with open(self.journal, "rb") as handle:
            before = handle.read()
        self.get("/audit?op=copy&stage=%E6%88%90%E5%8A%9F&key=alpha",
                 self.token)
        with open(self.journal, "rb") as handle:
            self.assertEqual(handle.read(), before)


class CliPairTest(unittest.TestCase):
    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "carbon_market", "serve", *args],
            capture_output=True, text=True, timeout=10)

    def test_invalid_combinations_exit_2(self) -> None:
        for args in (
            ("--audit", "/tmp/a.json"),
            ("--token", "secret"),
            ("--audit", "", "--token", "secret"),
            ("--audit", "/tmp/a.json", "--token", ""),
            ("--audit", "/a", "--audit", "/b", "--token", "s"),
            ("--audit", "/a", "--token", "s", "--token", "t"),
        ):
            result = self._run(*args)
            self.assertEqual(result.returncode, 2, args)

    def test_valid_pair_starts_the_server(self) -> None:
        # A valid pair gets past argument validation far enough to bind
        # the port; an already-in-use port proves serve() was reached.
        first = socket.socket()
        first.bind(("127.0.0.1", 0))
        port = first.getsockname()[1]
        try:
            result = self._run("--host", "127.0.0.1", "--port", str(port),
                               "--audit", "/tmp/a.json", "--token", "secret")
            self.assertNotEqual(result.returncode, 2)
            self.assertIn("address already in use",
                          (result.stderr or "").lower())
        finally:
            first.close()


if __name__ == "__main__":
    unittest.main()
