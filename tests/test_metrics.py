"""Tests for the process-local read-only GET /metrics snapshot."""

from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import sys
import threading
import time
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from tempfile import TemporaryDirectory

from carbon_market.metrics import Metrics
from carbon_market.server import Handler, _METRICS_ROUTES

FULL_TOKEN = "full-token"
OPS_TOKEN = "ops-token"
OLD_TOKEN = "old-token"


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _line(name: str, token: str, grace_until,
          ops="*", stages="*", keys="*") -> str:
    return json.dumps([name, _digest(token), grace_until, ops, stages, keys],
                      ensure_ascii=False)


def _assert_invariants(testcase: unittest.TestCase,
                       snapshot: dict) -> None:
    testcase.assertEqual(list(snapshot),
                         ["version", "started", "total", "routes"])
    testcase.assertEqual(snapshot["version"], 1)
    routes = snapshot["routes"]
    testcase.assertEqual(list(routes), sorted(routes))
    route_sum = 0
    for value in routes.values():
        testcase.assertEqual(list(value), ["total", "statuses"])
        testcase.assertEqual(value["total"], sum(value["statuses"].values()))
        codes = list(value["statuses"])
        testcase.assertEqual(codes, sorted(codes, key=int))
        route_sum += value["total"]
    testcase.assertEqual(route_sum, snapshot["total"])


class _Server:
    def __init__(self, config: str | None, *, enable: bool = True,
                 audit_path=None) -> None:
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.audit_path = audit_path
        self.server.audit_token = None
        self.server.audit_auth = config
        self.server.metrics = Metrics(int(time.time())) if enable else None
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       daemon=True)
        self.thread.start()
        self.port = self.server.server_port

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class MetricsHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        self._write_config(
            _line("full", FULL_TOKEN, None) + "\n"
            + _line("ops", OPS_TOKEN, None,
                    ops=["copy"], stages=["成功"], keys=["k1"]) + "\n")
        self.started_before = int(time.time())
        self.live = _Server(self.config)
        self.addCleanup(self.live.close)

    def _write_config(self, text: str) -> None:
        tmp_path = self.config + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp_path, self.config)

    def _request(self, method: str, path: str,
                 headers: dict[str, str] | None = None,
                 raw_headers: list[tuple[str, str]] | None = None):
        connection = HTTPConnection("127.0.0.1", self.live.port, timeout=5)
        if raw_headers is not None:
            connection.putrequest(method, path)
            for name, value in raw_headers:
                connection.putheader(name, value)
            connection.endheaders()
        else:
            connection.request(method, path, headers=headers or {})
        response = connection.getresponse()
        body = response.read()
        connection.close()
        return response.status, body

    def _get_metrics(self, token: str | None = FULL_TOKEN,
                     path: str = "/metrics"):
        headers = {} if token is None else {"X-Audit-Token": token}
        return self._request("GET", path, headers=headers)

    # -- authorization order -------------------------------------------------

    def test_missing_blank_and_duplicate_token_are_401(self) -> None:
        status, body = self._get_metrics(token=None)
        self.assertEqual((status, json.loads(body)),
                         (401, {"error": "unauthorized"}))
        status, body = self._request(
            "GET", "/metrics", headers={"X-Audit-Token": "  "})
        self.assertEqual(status, 401)
        status, body = self._request(
            "GET", "/metrics",
            raw_headers=[("X-Audit-Token", FULL_TOKEN),
                         ("X-Audit-Token", FULL_TOKEN)])
        self.assertEqual((status, json.loads(body)),
                         (401, {"error": "unauthorized"}))

    def test_unknown_and_expired_token_are_403(self) -> None:
        status, body = self._get_metrics("wrong-token")
        self.assertEqual((status, json.loads(body)),
                         (403, {"error": "forbidden"}))
        cutoff = int(time.time()) - 1
        self._write_config(_line("old", OLD_TOKEN, cutoff) + "\n")
        status, body = self._get_metrics(OLD_TOKEN)
        self.assertEqual((status, json.loads(body)),
                         (403, {"error": "forbidden"}))

    def test_any_restricted_scope_is_403(self) -> None:
        for spec in ((["copy"], "*", "*"),
                     ("*", ["成功"], "*"),
                     ("*", "*", ["k1"])):
            with self.subTest(spec=spec):
                self._write_config(
                    _line("scoped", OPS_TOKEN, None, *spec) + "\n")
                status, body = self._get_metrics(OPS_TOKEN)
                self.assertEqual((status, json.loads(body)),
                                 (403, {"error": "forbidden"}))

    def test_unreadable_or_invalid_config_is_503(self) -> None:
        os.unlink(self.config)
        status, body = self._get_metrics(FULL_TOKEN)
        self.assertEqual((status, json.loads(body)),
                         (503, {"error": "auth_unavailable"}))
        self._write_config("{not json\n")
        status, body = self._get_metrics(FULL_TOKEN)
        self.assertEqual((status, json.loads(body)),
                         (503, {"error": "auth_unavailable"}))

    # -- parameter and method handling --------------------------------------

    def test_query_string_is_400_after_identity_before_scope(self) -> None:
        # Identity precedes the parameter check: an unknown token with a
        # query is still 403.
        status, _ = self._get_metrics("wrong-token", "/metrics?x=1")
        self.assertEqual(status, 403)
        # A valid but scoped token with a query gets the 400 before the
        # scope decision.
        status, body = self._get_metrics(OPS_TOKEN, "/metrics?x=1")
        self.assertEqual((status, json.loads(body)),
                         (400, {"error": "invalid_request"}))
        for path in ("/metrics?", "/metrics?x", "/metrics?x="):
            with self.subTest(path=path):
                status, body = self._get_metrics(FULL_TOKEN, path)
                self.assertEqual((status, json.loads(body)),
                                 (400, {"error": "invalid_request"}))

    def test_other_methods_are_404(self) -> None:
        for method in ("POST", "HEAD", "PUT", "DELETE", "PATCH", "OPTIONS"):
            with self.subTest(method=method):
                status, body = self._request(
                    method, "/metrics",
                    headers={"X-Audit-Token": FULL_TOKEN})
                self.assertEqual(status, 404)
                if method != "HEAD":
                    self.assertEqual(json.loads(body),
                                     {"error": "not_found"})

    # -- snapshot content ----------------------------------------------------

    def test_success_shape_and_bytes(self) -> None:
        status, body = self._get_metrics()
        self.assertEqual(status, 200)
        self.assertFalse(body.endswith(b"\n"))
        snapshot = json.loads(body)
        self.assertEqual(snapshot["version"], 1)
        self.assertIsInstance(snapshot["started"], int)
        self.assertGreaterEqual(snapshot["started"], 0)
        self.assertGreaterEqual(snapshot["started"], self.started_before)
        self.assertLessEqual(snapshot["started"], int(time.time()) + 1)
        # Nothing else happened on this fresh server, and this read is
        # not in its own snapshot: zero counts.
        self.assertEqual(snapshot["total"], 0)
        self.assertEqual(snapshot["routes"], {})
        _assert_invariants(self, snapshot)

    def test_snapshot_excludes_itself_but_counts_it_afterwards(self) -> None:
        status, first_body = self._get_metrics()
        first = json.loads(first_body)
        self.assertEqual(status, 200)
        self.assertEqual(first["total"], 0)
        status, second_body = self._get_metrics()
        second = json.loads(second_body)
        self.assertEqual(status, 200)
        # The first successful read appears in the second snapshot,
        # exactly once; the second read itself stays out of its own view.
        self.assertEqual(second["total"], 1)
        self.assertEqual(second["routes"],
                         {"/metrics": {"total": 1,
                                       "statuses": {"200": 1}}})
        _assert_invariants(self, first)
        _assert_invariants(self, second)

    def test_routes_are_classified_and_sorted(self) -> None:
        self._request("GET", "/health")
        self._request("GET", "/missing")
        self._request("GET", "/nope%2Fpath")
        self._get_metrics(token=None)
        status, body = self._get_metrics()
        self.assertEqual(status, 200)
        snapshot = json.loads(body)
        routes = snapshot["routes"]
        self.assertEqual(list(routes), sorted(routes))
        self.assertEqual(routes["/health"],
                         {"total": 1, "statuses": {"200": 1}})
        # Every unknown path collapses into the single "other" bucket.
        self.assertEqual(routes["other"],
                         {"total": 2, "statuses": {"404": 2}})
        # The unauthenticated rejection is counted under /metrics.
        self.assertEqual(routes["/metrics"],
                         {"total": 1, "statuses": {"401": 1}})
        _assert_invariants(self, snapshot)

    def test_statuses_sorted_numerically(self) -> None:
        # One 401 rejection, then one 200 read observed by a second read:
        # numeric order puts "200" before "401", not lexicographic.
        self._get_metrics(token=None)
        self._get_metrics()
        status, body = self._get_metrics()
        self.assertEqual(status, 200)
        snapshot = json.loads(body)
        statuses = snapshot["routes"]["/metrics"]["statuses"]
        self.assertEqual(list(statuses), ["200", "401"])
        self.assertEqual(statuses, {"200": 1, "401": 1})
        self.assertEqual(snapshot["routes"]["/metrics"]["total"], 2)
        _assert_invariants(self, snapshot)

    def test_rejections_and_other_methods_are_counted(self) -> None:
        self._get_metrics("wrong-token")
        self._get_metrics(FULL_TOKEN, "/metrics?x=1")
        self._request("POST", "/metrics")
        status, body = self._get_metrics()
        self.assertEqual(status, 200)
        snapshot = json.loads(body)
        metrics_route = snapshot["routes"]["/metrics"]
        self.assertEqual(metrics_route["statuses"],
                         {"400": 1, "403": 1, "404": 1})
        self.assertEqual(metrics_route["total"], 3)
        _assert_invariants(self, snapshot)

    def test_health_bytes_unchanged(self) -> None:
        status, body = self._request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body, b'{"status":"ok"}')


class MetricsDisabledTest(unittest.TestCase):
    def setUp(self) -> None:
        self.live = _Server(None, enable=False)
        self.addCleanup(self.live.close)

    def test_metrics_is_unknown_path_when_disabled(self) -> None:
        connection = HTTPConnection("127.0.0.1", self.live.port, timeout=2)
        connection.request("GET", "/metrics")
        response = connection.getresponse()
        self.assertEqual(response.status, 404)
        self.assertEqual(json.loads(response.read()),
                         {"error": "not_found"})
        connection.close()
        connection = HTTPConnection("127.0.0.1", self.live.port, timeout=2)
        connection.request("GET", "/health")
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        self.assertEqual(response.read(), b'{"status":"ok"}')
        connection.close()


class MetricsConcurrencyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        with open(self.config, "w", encoding="utf-8") as handle:
            handle.write(_line("full", FULL_TOKEN, None) + "\n")
        self.live = _Server(self.config)
        self.addCleanup(self.live.close)

    def test_no_lost_or_double_count_under_concurrency(self) -> None:
        observed: dict[tuple[str, int], int] = {}
        lock = threading.Lock()
        fixed_paths = ("/health", "/metrics", "/missing")

        def worker(worker_id: int) -> None:
            local: dict[tuple[str, int], int] = {}
            for i in range(40):
                path = fixed_paths[(worker_id + i) % len(fixed_paths)]
                headers = ({"X-Audit-Token": FULL_TOKEN}
                           if path == "/metrics" else None)
                connection = HTTPConnection("127.0.0.1", self.live.port,
                                            timeout=5)
                connection.request("GET", path, headers=headers or {})
                response = connection.getresponse()
                response.read()
                connection.close()
                route = path if path in _METRICS_ROUTES else "other"
                key = (route, response.status)
                local[key] = local.get(key, 0) + 1
            with lock:
                for key, count in local.items():
                    observed[key] = observed.get(key, 0) + count

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
            self.assertFalse(thread.is_alive())

        # All worker responses are committed before this read; the
        # snapshot is taken before this request is counted, so its totals
        # match the client-observed counts exactly.
        connection = HTTPConnection("127.0.0.1", self.live.port, timeout=5)
        connection.request("GET", "/metrics",
                           headers={"X-Audit-Token": FULL_TOKEN})
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        snapshot = json.loads(response.read())
        connection.close()
        self.assertEqual(snapshot["total"], sum(observed.values()))
        for (route, code), count in observed.items():
            self.assertEqual(snapshot["routes"][route]["statuses"][str(code)],
                             count)
        _assert_invariants(self, snapshot)


class ServeMetricsArgumentsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        with open(self.config, "w", encoding="utf-8") as handle:
            handle.write(_line("full", FULL_TOKEN, None) + "\n")

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "carbon_market", "serve", *args],
            capture_output=True, timeout=10)

    def test_usage_errors_exit_2_without_listening(self) -> None:
        for args in (
            ("--metrics",),                                   # nothing else
            ("--metrics", "--audit", "j"),                   # no method
            ("--metrics", "--audit", "j", "--token", "t"),   # single token
            ("--audit", "j", "--auth", self.config,
             "--metrics", "--metrics"),                      # repeated
            ("--metrics=true", "--audit", "j", "--auth",
             self.config),                                   # carries a value
        ):
            with self.subTest(args=args):
                self.assertEqual(self._run(*args).returncode, 2)

    def test_valid_metrics_server_starts_with_empty_snapshot(self) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        process = subprocess.Popen(
            [sys.executable, "-m", "carbon_market", "serve",
             "--host", "127.0.0.1", "--port", str(port),
             "--audit", os.path.join(self.tmp.name, "audit.json"),
             "--auth", self.config, "--metrics"])
        try:
            deadline = time.time() + 10
            snapshot = None
            while time.time() < deadline:
                try:
                    connection = HTTPConnection("127.0.0.1", port, timeout=1)
                    connection.request("GET", "/metrics",
                                       headers={"X-Audit-Token": FULL_TOKEN})
                    response = connection.getresponse()
                    body = response.read()
                    connection.close()
                    if response.status == 200:
                        snapshot = json.loads(body)
                        break
                except OSError:
                    time.sleep(0.05)
            self.assertIsNotNone(snapshot)
            self.assertEqual(snapshot["version"], 1)
            self.assertEqual(snapshot["total"], 0)
            self.assertEqual(snapshot["routes"], {})
            self.assertGreaterEqual(snapshot["started"], 0)
        finally:
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
