"""HTTP tests for the read-only process metrics endpoint."""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from tempfile import TemporaryDirectory

from carbon_market.server import Handler

FULL_TOKEN = "full-token"
OPS_TOKEN = "ops-token"
KEYS_TOKEN = "keys-token"
STAGE_TOKEN = "stage-token"
OLD_TOKEN = "old-token"


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _line(name: str, token: str, grace_until=None,
          ops="*", stages="*", keys="*") -> str:
    return json.dumps([name, _digest(token), grace_until, ops, stages, keys],
                      ensure_ascii=False)


class MetricsHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        with open(self.config, "w", encoding="utf-8") as handle:
            handle.write(
                _line("full", FULL_TOKEN) + "\n"
                + _line("ops", OPS_TOKEN, None,
                        ops=["copy"], stages=["成功"], keys=["k"]) + "\n"
                + _line("keys", KEYS_TOKEN, None, keys=["k"]) + "\n"
                + _line("stage", STAGE_TOKEN, None, stages=["成功"]) + "\n")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.audit_path = self.journal
        self.server.audit_token = None
        self.server.audit_auth = self.config
        from carbon_market.metrics import Metrics
        self.server.metrics = Metrics(int(time.time()))
        thread = threading.Thread(target=self.server.serve_forever,
                                  daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)

    def _request(self, method: str, path: str,
                 headers: dict[str, str] | None = None,
                 raw_headers: list[tuple[str, str]] | None = None):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        if raw_headers is not None:
            connection.putrequest(method, path)
            for name, value in raw_headers:
                connection.putheader(name, value)
            connection.endheaders()
        else:
            connection.request(method, path, headers=headers or {})
        response = connection.getresponse()
        body = response.read()
        self.addCleanup(connection.close)
        return response.status, body, dict(response.getheaders())

    def _get(self, path: str, token: str | None = None,
             raw_headers=None):
        headers = None if raw_headers is not None else {}
        if token is not None:
            headers = {"X-Audit-Token": token}
        return self._request("GET", path, headers=headers,
                             raw_headers=raw_headers)

    def _metrics(self, token: str = FULL_TOKEN):
        status, body, _headers = self._get("/metrics", token)
        return status, json.loads(body)

    # -- authentication order ----------------------------------------------

    def test_missing_blank_and_duplicate_token_are_401(self) -> None:
        self.assertEqual(self._get("/metrics")[0], 401)
        self.assertEqual(self._get("/metrics", "   ")[0], 401)
        status, body, _ = self._get(
            "/metrics", raw_headers=[("X-Audit-Token", FULL_TOKEN),
                                     ("X-Audit-Token", FULL_TOKEN)])
        self.assertEqual(status, 401)
        self.assertEqual(json.loads(body), {"error": "unauthorized"})

    def test_unknown_and_expired_token_are_403(self) -> None:
        self.assertEqual(self._get("/metrics", "wrong")[0], 403)
        cutoff = int(time.time()) - 1
        with open(self.config, "w", encoding="utf-8") as handle:
            handle.write(_line("old", OLD_TOKEN, cutoff) + "\n")
        self.assertEqual(self._get("/metrics", OLD_TOKEN)[0], 403)

    def test_runtime_unreadable_or_invalid_auth_is_503(self) -> None:
        os.unlink(self.config)
        status, body, _ = self._get("/metrics", FULL_TOKEN)
        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body), {"error": "auth_unavailable"})
        with open(self.config, "w", encoding="utf-8") as handle:
            handle.write("{not json\n")
        status, body, _ = self._get("/metrics", FULL_TOKEN)
        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body), {"error": "auth_unavailable"})

    # -- query and method ---------------------------------------------------

    def test_any_query_string_is_400(self) -> None:
        for path in ("/metrics?x=1", "/metrics?a=b&c=d", "/metrics?x="):
            with self.subTest(path=path):
                status, body, _ = self._get(path, FULL_TOKEN)
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(body),
                                 {"error": "invalid_request"})

    def test_bare_question_mark_is_no_query_like_the_other_entries(self) -> None:
        # An empty query is treated exactly as no query, matching the
        # parameterless /signals/ingest and checkpoint entries.
        status, _body, _ = self._get("/metrics?", FULL_TOKEN)
        self.assertEqual(status, 200)

    def test_parameter_check_precedes_snapshot_read(self) -> None:
        # A 400 must still be counted afterwards; the point is that no
        # snapshot is served, but first establish a baseline and confirm
        # the query never returns one.
        status, body, _ = self._get("/metrics?x=1", FULL_TOKEN)
        self.assertEqual(status, 400)
        self.assertNotIn(b"version", body)

    def test_non_get_methods_are_404(self) -> None:
        for method in ("POST", "PUT", "DELETE", "PATCH", "HEAD",
                       "OPTIONS"):
            with self.subTest(method=method):
                status, _body, _headers = self._request(
                    method, "/metrics", {"X-Audit-Token": FULL_TOKEN})
                self.assertEqual(status, 404)

    # -- scope ---------------------------------------------------------------

    def test_only_a_fully_wildcard_token_may_read(self) -> None:
        status, body, _ = self._get("/metrics", FULL_TOKEN)
        self.assertEqual(status, 200)
        self.assertIn(b"version", body)
        for token in (OPS_TOKEN, KEYS_TOKEN, STAGE_TOKEN):
            with self.subTest(token=token):
                status, body, _ = self._get("/metrics", token)
                self.assertEqual(status, 403)
                self.assertEqual(json.loads(body),
                                 {"error": "forbidden"})

    # -- snapshot semantics --------------------------------------------------

    def test_snapshot_shape_and_ordering(self) -> None:
        status, body, headers = self._get("/metrics", FULL_TOKEN)
        self.assertEqual(status, 200)
        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertFalse(body.endswith(b"\n"))
        parsed = json.loads(body)
        self.assertEqual(list(parsed), ["version", "started", "total",
                                        "routes"])
        self.assertEqual(parsed["version"], 1)
        self.assertIsInstance(parsed["started"], int)
        self.assertGreaterEqual(parsed["started"], 0)
        for route, value in parsed["routes"].items():
            self.assertEqual(list(value), ["total", "statuses"])

    def test_current_request_is_not_in_its_own_snapshot(self) -> None:
        # This is the first request of the test, so it observes zero.
        _status, first = self._metrics(FULL_TOKEN)
        self.assertEqual(first["total"], 0)
        self.assertEqual(first["routes"], {})
        # The second snapshot sees the first (and only the prior) one.
        _status, second = self._metrics(FULL_TOKEN)
        self.assertEqual(second["total"], 1)
        self.assertEqual(second["routes"]["/metrics"]["total"], 1)
        self.assertEqual(
            second["routes"]["/metrics"]["statuses"], {"200": 1})

    def test_every_decided_response_is_counted_once_by_route(self) -> None:
        # 200 health, 404 unknown, 401 and 403 against /metrics, 400
        # query, and a 404 on an enabled-but-unconfigured fixed path.
        self._get("/health")
        self._get("/totally/unknown/path")
        self._get("/another-unknown?with=query")
        self._get("/metrics")                 # 401
        self._get("/metrics", "wrong")        # 403
        self._get("/metrics?x=1", FULL_TOKEN)  # 400
        # /completions is a fixed public path even though it is not
        # configured on this server: its 404 still groups under its own
        # path, never under "other".
        self._get("/completions", FULL_TOKEN)

        _status, snapshot = self._metrics(FULL_TOKEN)
        self.assertEqual(snapshot["total"], 7)
        routes = snapshot["routes"]
        self.assertEqual(routes["/health"],
                         {"total": 1, "statuses": {"200": 1}})
        self.assertEqual(routes["/metrics"]["total"], 3)
        self.assertEqual(routes["/metrics"]["statuses"],
                         {"400": 1, "401": 1, "403": 1})
        self.assertEqual(routes["/completions"],
                         {"total": 1, "statuses": {"404": 1}})
        # Both arbitrary paths collapse into a single "other" bucket.
        self.assertEqual(routes["other"],
                         {"total": 2, "statuses": {"404": 2}})
        # Totals reconcile at every layer.
        self.assertEqual(
            sum(r["total"] for r in routes.values()), snapshot["total"])
        self.assertEqual(
            sum(c for r in routes.values()
                for c in r["statuses"].values()), snapshot["total"])

    def test_business_rejections_and_auth_failures_are_counted(self) -> None:
        # /audit is configured and backed by a missing journal: a valid
        # token reaches the journal and gets 404 audit_not_found; a bad
        # token gets 403 before the file is opened. Both count once.
        self._get("/audit", FULL_TOKEN)
        self._get("/audit", "wrong")
        self._request("POST", "/metrics", {"X-Audit-Token": FULL_TOKEN})
        _status, snapshot = self._metrics(FULL_TOKEN)
        routes = snapshot["routes"]
        self.assertEqual(routes["/audit"]["statuses"]["404"], 1)
        self.assertEqual(routes["/audit"]["statuses"]["403"], 1)
        self.assertEqual(routes["/metrics"]["statuses"]["404"], 1)

    def test_routes_sorted_by_code_point_and_statuses_numerically(self) -> None:
        for code_path in ("/health",):
            self._get(code_path)
        # Produce several statuses on /metrics out of numeric order.
        self._get("/metrics", "wrong")       # 403
        self._get("/metrics")                # 401
        self._get("/metrics?x=1", FULL_TOKEN)  # 400
        _status, snapshot = self._metrics(FULL_TOKEN)
        self.assertEqual(list(snapshot["routes"]),
                         sorted(snapshot["routes"]))
        statuses = list(
            snapshot["routes"]["/metrics"]["statuses"])
        self.assertEqual(statuses, sorted(statuses, key=int))
        self.assertEqual(statuses, ["400", "401", "403"])

    def test_error_bodies_do_not_leak_paths_or_tokens(self) -> None:
        for path in ("/metrics", "/metrics?x=1"):
            _status, body, _ = self._get(path, "wrong-token-value")
            self.assertNotIn(b"wrong-token-value", body)
            self.assertNotIn(self.config.encode(), body)
        os.unlink(self.config)
        _status, body, _ = self._get("/metrics?x=1", FULL_TOKEN)
        self.assertNotIn(self.config.encode(), body)

    # -- existing surface unchanged -----------------------------------------

    def test_health_unchanged(self) -> None:
        status, body, headers = self._get("/health")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"status": "ok"})
        self.assertFalse(body.endswith(b"\n"))


class MetricsDisabledHttpTest(unittest.TestCase):
    def test_metrics_path_is_plain_404_when_disabled(self) -> None:
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        config = os.path.join(tmp.name, "auth.jsonl")
        with open(config, "w", encoding="utf-8") as handle:
            handle.write(_line("full", FULL_TOKEN) + "\n")
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        server.audit_path = os.path.join(tmp.name, "audit.json")
        server.audit_token = None
        server.audit_auth = config
        # No server.metrics attribute at all.
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 2)
        connection = HTTPConnection("127.0.0.1", server.server_port,
                                    timeout=2)
        connection.request("GET", "/metrics",
                           headers={"X-Audit-Token": FULL_TOKEN})
        response = connection.getresponse()
        self.assertEqual(response.status, 404)
        self.assertEqual(json.loads(response.read()),
                         {"error": "not_found"})
        connection.close()


if __name__ == "__main__":
    unittest.main()
