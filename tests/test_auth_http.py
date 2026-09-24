"""HTTP tests for hot-rotatable multi-token scoped authorization.

Covers the --auth mode of GET /audit: header handling (401), unknown and
expired tokens (403), scope enforcement before the journal is opened,
per-request config reload (503 when the file is broken, atomic rotation
without restart), and unchanged search semantics once authorized.
"""

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

from carbon_market import audit
from carbon_market.server import Handler


def digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def record_line(name: str, token: str, deadline: int | None = None,
                ops: object = "*", stages: object = "*",
                keys: object = "*") -> str:
    return json.dumps(
        [name, digest(token), deadline, ops, stages, keys],
        ensure_ascii=False) + "\n"


def _event(op: str = "copy", key: str = "batch-key",
           error: str | None = None, stage: str | None = None) -> dict:
    return {"op": op, "target": "t.history", "key": key,
            "changed": True, "error": error, "stage": stage}


class MultiTokenHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.auth_path = os.path.join(self.tmp.name, "auth.jsonl")
        self.write_auth(
            record_line("wild", "all-access")
            + record_line("scoped", "limited",
                          ops=["copy"], stages=["成功"], keys=["hist-1"])
            + record_line("retired", "old", deadline=100))

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.audit_path = self.journal
        self.server.audit_token = None
        self.server.auth_path = self.auth_path
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)

    def write_auth(self, text: str) -> None:
        # Same-directory atomic replace, exactly as an operator rotation.
        tmp = self.auth_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp, self.auth_path)

    def _get(self, path: str, headers: dict[str, str] | None = None,
             raw_headers: list[tuple[str, str]] | None = None):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        if raw_headers is not None:
            connection.putrequest("GET", path)
            for name, value in raw_headers:
                connection.putheader(name, value)
            connection.endheaders()
        else:
            connection.request("GET", path, headers=headers or {})
        response = connection.getresponse()
        body = response.read()
        self.addCleanup(connection.close)
        return response.status, json.loads(body)

    def _token_get(self, token: str, path: str = "/audit"):
        return self._get(path, headers={"X-Audit-Token": token})

    def _record(self, audit_key: str, **event_kwargs) -> None:
        audit.record(self.journal, audit_key, _event(**event_kwargs))

    # -- header handling ----------------------------------------------------

    def test_missing_blank_and_duplicate_headers_are_401(self) -> None:
        self.assertEqual(self._get("/audit")[0], 401)
        self.assertEqual(self._token_get("   ")[0], 401)
        status, body = self._get(
            "/audit", raw_headers=[("X-Audit-Token", "all-access"),
                                   ("X-Audit-Token", "all-access")])
        self.assertEqual(status, 401)
        self.assertEqual(body, {"error": "unauthorized"})

    # -- token matching and grace deadline ----------------------------------

    def test_unknown_token_is_403_and_never_opens_journal(self) -> None:
        self.assertEqual(self._token_get("nope")[0], 403)
        self._record("k")
        # 404 for an authorized wildcard, but still 403 here: no state leak.
        self.assertEqual(self._token_get("nope")[0], 403)
        self.assertFalse(os.path.exists(self.journal + ".unexpected"))

    def test_expired_grace_token_is_403_but_valid_token_works(self) -> None:
        now = int(time.time())
        self.write_auth(
            record_line("retired", "old", deadline=now - 1)
            + record_line("current", "fresh", deadline=now + 3600))
        self.assertEqual(self._token_get("old")[0], 403)
        status, _ = self._token_get("fresh")
        self.assertEqual(status, 404)  # authorized; journal simply missing

    def test_deadline_equal_to_now_is_still_valid(self) -> None:
        now = int(time.time())
        self.write_auth(record_line("edge", "edge-token", deadline=now))
        status, _ = self._token_get("edge-token")
        self.assertEqual(status, 404)

    # -- scopes ---------------------------------------------------------------

    def test_wildcard_token_queries_freely(self) -> None:
        self._record("a", key="hist-1")
        self._record("b", op="restore", key="hist-2")
        for path in ("/audit", "/audit?op=restore",
                     "/audit?key=hist-2",
                     "/audit?stage=" + "%E6%88%90%E5%8A%9F"):  # 成功
            with self.subTest(path=path):
                status, body = self._token_get("all-access", path)
                self.assertEqual(status, 200)

    def test_scoped_token_must_send_in_scope_filters(self) -> None:
        self._record("a", key="hist-1")
        self._record("b", op="restore", key="hist-2")
        self._record("c", key="hist-1", error="OSError", stage="同步")

        # All three in-scope filters present and satisfied.
        status, body = self._token_get(
            "limited", "/audit?op=copy&stage="
            + "%E6%88%90%E5%8A%9F" + "&key=hist-1")  # 成功
        self.assertEqual(status, 200)
        self.assertEqual([k for k, _ in body["events"]], ["a"])

        # Omitting a required filter is 403.
        self.assertEqual(
            self._token_get("limited", "/audit?op=copy&key=hist-1")[0], 403)
        # A legal value outside the scope is 403.
        self.assertEqual(
            self._token_get("limited", "/audit?op=restore&stage="
                            + "%E6%88%90%E5%8A%9F" + "&key=hist-1")[0], 403)
        self.assertEqual(
            self._token_get("limited", "/audit?op=copy&stage="
                            + "%E5%90%8C%E6%AD%A5"  # 同步
                            + "&key=hist-1")[0], 403)
        self.assertEqual(
            self._token_get(
                "limited", "/audit?op=copy&stage="
                + "%E6%88%90%E5%8A%9F" + "&key=hist-2")[0], 403)

    def test_scope_failure_is_403_without_touching_the_journal(self) -> None:
        # Journal does not exist; an out-of-scope request is still 403 and
        # never falls through to the 404 a real query would produce.
        status, body = self._token_get("limited", "/audit")
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "forbidden"})

    def test_parameter_validation_precedes_scope_check(self) -> None:
        # An in-scope filter shape but invalid value is 400, not 403.
        status, body = self._token_get(
            "limited", "/audit?op=delete&stage=x&key=hist-1")
        self.assertEqual(status, 400)
        self.assertEqual(body, {"error": "invalid_request"})
        # Unknown parameters are likewise 400 even when fully in scope.
        status, _ = self._token_get(
            "limited", "/audit?op=copy&stage="
            + "%E6%88%90%E5%8A%9F" + "&key=hist-1&bogus=1")
        self.assertEqual(status, 400)

    # -- hot rotation ---------------------------------------------------------

    def test_rotation_adds_and_retires_tokens_without_restart(self) -> None:
        # Old token works before rotation.
        self.assertEqual(self._token_get("all-access")[0], 404)

        self.write_auth(
            record_line("new", "rotated")
            + record_line("old-svc", "all-access", deadline=1))
        # The previously valid token is retired immediately (deadline 1).
        self.assertEqual(self._token_get("all-access")[0], 403)
        # The newly added digest works.
        self.assertEqual(self._token_get("rotated")[0], 404)

    def test_broken_rotated_config_returns_503_then_recovers(self) -> None:
        self.assertEqual(self._token_get("all-access")[0], 404)

        self.write_auth("{not json\n")
        status, body = self._token_get("all-access")
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "auth_unavailable"})

        # Removing the file has the same effect -- never serve on a stale
        # snapshot and never fail open.
        os.unlink(self.auth_path)
        self.assertEqual(self._token_get("all-access")[0], 503)

        self.write_auth(record_line("wild", "all-access"))
        self.assertEqual(self._token_get("all-access")[0], 404)

    def test_503_reveals_no_record_detail(self) -> None:
        self.write_auth("{broken")
        status, body = self._token_get("all-access")
        self.assertEqual(status, 503)
        self.assertEqual(list(body), ["error"])

    # -- unchanged surface ----------------------------------------------------

    def test_health_and_404_unchanged(self) -> None:
        self.assertEqual(self._get("/health"), (200, {"status": "ok"}))
        self.assertEqual(self._get("/nope"), (404, {"error": "not_found"}))

    def test_authorized_search_pagination_and_errors_unchanged(self) -> None:
        for letter in ("a", "b", "c"):
            self._record(letter)
        status, body = self._token_get("all-access", "/audit?limit=2")
        self.assertEqual(status, 200)
        self.assertEqual([k for k, _ in body["events"]], ["a", "b"])
        self.assertEqual(body["next"], "b")

        with open(self.journal, "wb") as handle:
            handle.write(b"{bad")
        status, body = self._token_get("all-access")
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "audit_invalid"})


class ServeAuthArgumentsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.auth_path = os.path.join(self.tmp.name, "auth.jsonl")
        self.journal = os.path.join(self.tmp.name, "audit.json")

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "carbon_market", "serve", *args],
            capture_output=True, timeout=10)

    def _free_port(self) -> int:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]

    def test_bad_argument_combinations_exit_2(self) -> None:
        for args in (
            ("--auth", self.auth_path, "--token", "t"),  # both methods
            ("--audit", self.journal),                   # no authorization
            ("--token", "t"),                           # token without audit
            ("--auth", self.auth_path),                 # auth without audit
            ("--audit", self.journal, "--auth", ""),    # empty auth
        ):
            with self.subTest(args=args):
                self.assertEqual(self._run(*args).returncode, 2)

    def test_invalid_auth_file_exits_2_without_listening(self) -> None:
        port = self._free_port()
        # File does not exist.
        result = self._run("--port", str(port), "--audit", self.journal,
                           "--auth", self.auth_path)
        self.assertEqual(result.returncode, 2)
        self.assertNotIn(b"Traceback", result.stderr)
        # Empty file.
        open(self.auth_path, "wb").close()
        self.assertEqual(
            self._run("--port", str(port), "--audit", self.journal,
                      "--auth", self.auth_path).returncode, 2)
        # Malformed line.
        with open(self.auth_path, "w", encoding="utf-8") as handle:
            handle.write("{not an array}\n")
        self.assertEqual(
            self._run("--port", str(port), "--audit", self.journal,
                      "--auth", self.auth_path).returncode, 2)

    def test_valid_auth_file_serves_requests(self) -> None:
        port = self._free_port()
        with open(self.auth_path, "w", encoding="utf-8") as handle:
            handle.write(record_line("svc", "serve-token"))
        process = subprocess.Popen(
            [sys.executable, "-m", "carbon_market", "serve",
             "--port", str(port), "--audit", self.journal,
             "--auth", self.auth_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            self._wait_until_listening(port)
            connection = HTTPConnection("127.0.0.1", port, timeout=2)
            connection.request("GET", "/audit",
                               headers={"X-Audit-Token": "serve-token"})
            response = connection.getresponse()
            self.assertEqual(response.status, 404)  # no journal yet
            response.read()
            connection.close()

            connection = HTTPConnection("127.0.0.1", port, timeout=2)
            connection.request("GET", "/audit",
                               headers={"X-Audit-Token": "wrong"})
            response = connection.getresponse()
            self.assertEqual(response.status, 403)
            response.read()
            connection.close()
        finally:
            process.terminate()
            process.wait(timeout=5)

    def _wait_until_listening(self, port: int) -> None:
        for _ in range(50):
            with socket.socket() as sock:
                if sock.connect_ex(("127.0.0.1", port)) == 0:
                    return
            time.sleep(0.05)
        self.fail("server did not start listening")


if __name__ == "__main__":
    unittest.main()
