"""HTTP tests for the token-protected GET /audit endpoint."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from tempfile import TemporaryDirectory

from carbon_market import audit
from carbon_market.server import Handler

TOKEN = "s3cret"


def _event(op: str = "copy", key: str = "batch-key",
           error: str | None = None, stage: str | None = None) -> dict:
    return {"op": op, "target": "t.history", "key": key,
            "changed": True, "error": error, "stage": stage}


class AuditHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.audit_path = self.journal
        self.server.audit_token = TOKEN
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)

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

    def _auth_get(self, path: str):
        return self._get(path, headers={"X-Audit-Token": TOKEN})

    def _record(self, audit_key: str, **event_kwargs) -> None:
        audit.record(self.journal, audit_key, _event(**event_kwargs))

    # -- authorization ----------------------------------------------------

    def test_missing_blank_and_duplicate_token_are_401(self) -> None:
        self.assertEqual(self._get("/audit")[0], 401)
        self.assertEqual(
            self._get("/audit", headers={"X-Audit-Token": "  "})[0], 401)
        status, body = self._get(
            "/audit", raw_headers=[("X-Audit-Token", TOKEN),
                                   ("X-Audit-Token", TOKEN)])
        self.assertEqual(status, 401)
        self.assertEqual(list(body), ["error"])

    def test_wrong_token_is_403_and_never_touches_the_file(self) -> None:
        # 403 whether the journal exists or not: no existence leak.
        self.assertEqual(
            self._get("/audit", headers={"X-Audit-Token": "nope"})[0], 403)
        self._record("k")
        status, body = self._get("/audit", headers={"X-Audit-Token": "nope"})
        self.assertEqual(status, 403)
        self.assertEqual(list(body), ["error"])

    def test_auth_precedes_parameter_validation(self) -> None:
        self.assertEqual(self._get("/audit?bogus=1")[0], 401)
        self.assertEqual(
            self._get("/audit?bogus=1",
                      headers={"X-Audit-Token": "nope"})[0], 403)

    # -- query parameter validation ----------------------------------------

    def test_invalid_parameters_are_400(self) -> None:
        for path in (
            "/audit?bogus=1",            # unknown name
            "/audit?cursor=a&cursor=b",  # repeated name
            "/audit?cursor=",            # empty value
            "/audit?cursor",             # missing value
            "/audit?key=",               # empty filter value
            "/audit?limit=0",
            "/audit?limit=1001",
            "/audit?limit=-1",
            "/audit?limit=1.5",
            "/audit?limit=abc",
            "/audit?limit=+5",
            "/audit?op=delete",
            "/audit?stage=unknown",
            "/audit?cursor=%FF%FE",      # not UTF-8
        ):
            with self.subTest(path=path):
                status, body = self._auth_get(path)
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})

    # -- successful queries -------------------------------------------------

    def test_search_maps_parameters_and_preserves_order(self) -> None:
        self._record("a", key="hist-1")
        self._record("b", op="restore", key="hist-2")
        self._record("c", key="hist-1", error="OSError", stage="同步")

        status, body = self._auth_get("/audit")
        self.assertEqual(status, 200)
        self.assertEqual(list(body), ["events", "next"])
        self.assertEqual([key for key, _ in body["events"]], ["a", "b", "c"])
        self.assertEqual(list(body["events"][0][1]),
                         ["op", "target", "key", "changed", "error", "stage"])
        self.assertIsNone(body["next"])

        status, body = self._auth_get("/audit?op=restore")
        self.assertEqual([key for key, _ in body["events"]], ["b"])

        status, body = self._auth_get(
            "/audit?stage=" + "%E5%90%8C%E6%AD%A5")  # 同步
        self.assertEqual([key for key, _ in body["events"]], ["c"])

        status, body = self._auth_get("/audit?key=hist-1&op=copy")
        self.assertEqual([key for key, _ in body["events"]], ["a", "c"])

    def test_limit_and_exclusive_cursor_paginate(self) -> None:
        for letter in ("a", "b", "c"):
            self._record(letter)
        status, body = self._auth_get("/audit?limit=2")
        self.assertEqual(status, 200)
        self.assertEqual([key for key, _ in body["events"]], ["a", "b"])
        self.assertEqual(body["next"], "b")
        status, body = self._auth_get("/audit?limit=2&cursor=b")
        self.assertEqual([key for key, _ in body["events"]], ["c"])
        self.assertIsNone(body["next"])

    # -- journal state errors ------------------------------------------------

    def test_missing_journal_is_404_without_leaking_path(self) -> None:
        status, body = self._auth_get("/audit")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "audit_not_found"})

    def test_malformed_journal_is_409(self) -> None:
        with open(self.journal, "wb") as handle:
            handle.write(b"{not json")
        status, body = self._auth_get("/audit")
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "audit_invalid"})

    def test_io_failure_is_503(self) -> None:
        self.server.audit_path = self.tmp.name  # a directory: open fails
        status, body = self._auth_get("/audit")
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "audit_unavailable"})

    # -- unchanged surface ----------------------------------------------------

    def test_health_and_unknown_paths_unchanged(self) -> None:
        self.assertEqual(self._get("/health"), (200, {"status": "ok"}))
        self.assertEqual(self._get("/missing"), (404, {"error": "not_found"}))
        self.assertEqual(self._get("/audit/"), (404, {"error": "not_found"}))


class AuditDisabledTest(unittest.TestCase):
    def test_audit_path_is_plain_404_without_configuration(self) -> None:
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connection = HTTPConnection("127.0.0.1", server.server_port,
                                        timeout=2)
            connection.request("GET", "/audit")
            response = connection.getresponse()
            self.assertEqual(response.status, 404)
            self.assertEqual(json.loads(response.read()),
                             {"error": "not_found"})
            connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


class ServeArgumentsTest(unittest.TestCase):
    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "carbon_market", "serve", *args],
            capture_output=True, timeout=10)

    def test_incomplete_empty_or_repeated_pair_exits_2(self) -> None:
        for args in (
            ("--audit", "journal.json"),
            ("--token", "tok"),
            ("--audit", "", "--token", "tok"),
            ("--audit", "journal.json", "--token", ""),
            ("--audit", "a", "--audit", "b", "--token", "tok"),
            ("--audit", "a", "--token", "tok", "--token", "tok2"),
        ):
            with self.subTest(args=args):
                self.assertEqual(self._run(*args).returncode, 2)


if __name__ == "__main__":
    unittest.main()
