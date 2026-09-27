"""HTTP tests for the read-only GET /migration-batches endpoint.

Covers the multi-token authorization (401/403/503 auth_unavailable),
the exact-lookup and paginated query shapes, the scope rules (an exact
lookup needs unrestricted operation and stage scopes and a key scope
that names the requested batch; pagination needs all three scopes
unrestricted), the validation priority (bad coordination bytes are 409
even for an unknown key), the completion information, and the error
mapping (400 invalid_request, 404 migration_batches_not_found, 404
migration_batch_not_found, 409 migration_batches_invalid, 503
migration_batches_unavailable). The endpoint only exists with
``--migration-batches`` alongside ``--audit`` and the multi-token
``--auth``; without it the path is a plain 404 like any unknown path.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import threading
import time
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from tempfile import TemporaryDirectory

from carbon_market import migration_batch
from carbon_market.server import Handler
from tests.test_migration_batch import MigrationBatchTest

FULL_TOKEN = "full-token"
KEY_TOKEN = "key-token"
OPS_TOKEN = "ops-token"
OLD_TOKEN = "old-token"


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _line(name: str, token: str, grace_until,
          ops="*", stages="*", keys="*") -> str:
    return json.dumps([name, _digest(token), grace_until, ops, stages, keys],
                      ensure_ascii=False)


class MigrationBatchesHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        # Reuse the library fixture that builds the nine business
        # ledgers and a coordination ledger with one active and one
        # completed batch.
        self.fixture = MigrationBatchTest(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture._prepare_migrate()
        self.fixture._run(now=30)               # bk-1 stays active
        self.fixture._run(key="bk-2", now=31)   # empty scan completes bk-2
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        self._write_config(
            _line("full", FULL_TOKEN, None) + "\n"
            + _line("key", KEY_TOKEN, None, keys=["bk-1"]) + "\n"
            + _line("ops", OPS_TOKEN, None, ops=["copy"]) + "\n")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.audit_path = self.journal
        self.server.audit_token = None
        self.server.audit_auth = self.config
        self.server.migration_batches_path = self.fixture.paths["coord"]
        thread = threading.Thread(target=self.server.serve_forever,
                                  daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)

    def _write_config(self, text: str) -> None:
        tmp_path = self.config + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp_path, self.config)

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
        return response.status, body

    def _json_get(self, path: str, **kwargs):
        status, body = self._get(path, **kwargs)
        return status, json.loads(body)

    def _token_get(self, token: str, path: str = "/migration-batches"):
        return self._json_get(path, headers={"X-Audit-Token": token})

    # -- authorization -------------------------------------------------------

    def test_missing_blank_and_duplicate_token_are_401(self) -> None:
        self.assertEqual(self._json_get("/migration-batches")[0], 401)
        self.assertEqual(
            self._json_get("/migration-batches",
                           headers={"X-Audit-Token": "  "})[0], 401)
        status, body = self._json_get(
            "/migration-batches", raw_headers=[("X-Audit-Token", FULL_TOKEN),
                                               ("X-Audit-Token", FULL_TOKEN)])
        self.assertEqual(status, 401)
        self.assertEqual(body, {"error": "unauthorized"})

    def test_unknown_and_expired_token_are_403(self) -> None:
        status, body = self._token_get("wrong-token")
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "forbidden"})
        cutoff = int(time.time()) - 1
        self._write_config(_line("old", OLD_TOKEN, cutoff) + "\n")
        status, body = self._token_get(OLD_TOKEN)
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "forbidden"})

    def test_unreadable_or_invalid_config_is_503(self) -> None:
        os.unlink(self.config)
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "auth_unavailable"})
        self._write_config("{not json\n")
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "auth_unavailable"})

    def test_authorization_precedes_parameter_validation(self) -> None:
        self.assertEqual(
            self._json_get("/migration-batches?bogus=1")[0], 401)
        self.assertEqual(
            self._token_get("wrong-token",
                            "/migration-batches?bogus=1")[0], 403)

    # -- query parameter validation ------------------------------------------

    def test_invalid_parameters_are_400(self) -> None:
        for path in (
            "/migration-batches?bogus=1",          # unknown name
            "/migration-batches?key=bk-1&key=bk-2",  # repeated name
            "/migration-batches?key=",             # empty key
            "/migration-batches?cursor=",          # empty cursor
            "/migration-batches?key=bk-1&cursor=z",  # key with cursor
            "/migration-batches?key=bk-1&limit=2",   # key with page size
            "/migration-batches?limit=0",
            "/migration-batches?limit=1001",
            "/migration-batches?limit=-1",
            "/migration-batches?limit=abc",
            "/migration-batches?cursor=%FF%FE",    # not UTF-8
        ):
            with self.subTest(path=path):
                status, body = self._token_get(FULL_TOKEN, path)
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})

    # -- scopes ---------------------------------------------------------------

    def test_exact_lookup_scopes(self) -> None:
        self.assertEqual(
            self._token_get(OPS_TOKEN,
                            "/migration-batches?key=bk-1")[0], 403)
        status, body = self._token_get(KEY_TOKEN,
                                       "/migration-batches?key=bk-1")
        self.assertEqual(status, 200)
        self.assertEqual(body["key"], "bk-1")
        self.assertEqual(
            self._token_get(KEY_TOKEN,
                            "/migration-batches?key=bk-2")[0], 403)

    def test_pagination_requires_unrestricted_scopes(self) -> None:
        for token in (KEY_TOKEN, OPS_TOKEN):
            with self.subTest(token=token):
                status, body = self._token_get(token)
                self.assertEqual(status, 403)
                self.assertEqual(body, {"error": "forbidden"})
        self.assertEqual(self._token_get(FULL_TOKEN)[0], 200)

    # -- exact lookup ---------------------------------------------------------

    def test_exact_lookup_returns_the_snapshot(self) -> None:
        status, body = self._token_get(FULL_TOKEN,
                                       "/migration-batches?key=bk-1")
        self.assertEqual(status, 200)
        self.assertEqual(
            list(body), ["key", "owner", "until", "status", "inputs",
                         "items"])
        self.assertEqual(body, migration_batch.get(
            self.fixture.paths["coord"], "bk-1"))

    def test_exact_lookup_is_compact_utf8_without_newline(self) -> None:
        status, raw = self._get(
            "/migration-batches?key=bk-1",
            headers={"X-Audit-Token": FULL_TOKEN})
        self.assertEqual(status, 200)
        self.assertEqual(
            raw, json.dumps(
                migration_batch.get(self.fixture.paths["coord"], "bk-1"),
                ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        self.assertFalse(raw.endswith(b"\n"))

    # -- pagination ------------------------------------------------------------

    def test_pagination_page_object_and_completion_info(self) -> None:
        status, body = self._token_get(FULL_TOKEN,
                                       "/migration-batches?limit=1")
        self.assertEqual(status, 200)
        self.assertEqual(list(body), ["entries", "next"])
        first = body["entries"][0]
        self.assertEqual([first[0]], ["bk-1"])
        self.assertEqual(first[1]["key"], "bk-1")
        self.assertIsNone(first[2])  # active batch
        self.assertEqual(body["next"], "bk-1")

        status, body = self._token_get(
            FULL_TOKEN, "/migration-batches?limit=1&cursor=bk-1")
        self.assertEqual(status, 200)
        entry = body["entries"][0]
        self.assertEqual(entry[0], "bk-2")
        self.assertEqual(entry[1]["status"], "completed")
        # bk-2 was the first (zero-based) batch to complete, at 31.
        self.assertEqual(entry[2], {"index": 0, "at": 31})
        self.assertIsNone(body["next"])

    def test_default_page_size_and_order(self) -> None:
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual(status, 200)
        self.assertEqual([entry[0] for entry in body["entries"]],
                         ["bk-1", "bk-2"])
        self.assertIsNone(body["next"])

    # -- ledger state errors ---------------------------------------------------

    def test_missing_ledger_is_404(self) -> None:
        self.server.migration_batches_path = os.path.join(
            self.tmp.name, "missing.json")
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "migration_batches_not_found"})
        status, body = self._token_get(FULL_TOKEN,
                                       "/migration-batches?key=bk-1")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "migration_batches_not_found"})

    def test_unknown_key_is_404(self) -> None:
        status, body = self._token_get(FULL_TOKEN,
                                       "/migration-batches?key=nope")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "migration_batch_not_found"})

    def test_invalid_ledger_is_409_even_for_unknown_key(self) -> None:
        bad = os.path.join(self.tmp.name, "bad.json")
        with open(bad, "wb") as handle:
            handle.write(b"{not json\n")
        self.server.migration_batches_path = bad
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "migration_batches_invalid"})
        # Validation priority: the bad format wins over the unknown key.
        status, body = self._token_get(FULL_TOKEN,
                                       "/migration-batches?key=zzz")
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "migration_batches_invalid"})

    def test_missing_business_ledger_is_409(self) -> None:
        with open(self.fixture.paths["supply"], "rb") as handle:
            supply_bytes = handle.read()
        os.unlink(self.fixture.paths["supply"])
        try:
            status, body = self._token_get(FULL_TOKEN)
            self.assertEqual(status, 409)
            self.assertEqual(body, {"error": "migration_batches_invalid"})
        finally:
            with open(self.fixture.paths["supply"], "wb") as handle:
                handle.write(supply_bytes)

    def test_io_failure_is_503(self) -> None:
        directory = os.path.join(self.tmp.name, "adir")
        os.mkdir(directory)
        self.server.migration_batches_path = directory
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "migration_batches_unavailable"})

    def test_error_bodies_never_leak_paths(self) -> None:
        bad = os.path.join(self.tmp.name, "bad.json")
        with open(bad, "wb") as handle:
            handle.write(b"{not json\n")
        self.server.migration_batches_path = bad
        _status, body = self._token_get(FULL_TOKEN)
        self.assertEqual(list(body), ["error"])
        self.assertNotIn("bad.json", json.dumps(body))

    # -- unchanged surface ------------------------------------------------------

    def test_health_and_unknown_paths_unchanged(self) -> None:
        self.assertEqual(self._json_get("/health"), (200, {"status": "ok"}))
        self.assertEqual(self._json_get("/missing"),
                         (404, {"error": "not_found"}))
        self.assertEqual(self._json_get("/migration-batches/"),
                         (404, {"error": "not_found"}))


class MigrationBatchesDisabledTest(unittest.TestCase):
    def test_path_is_plain_404_without_configuration(self) -> None:
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connection = HTTPConnection("127.0.0.1", server.server_port,
                                        timeout=2)
            connection.request("GET", "/migration-batches")
            response = connection.getresponse()
            self.assertEqual(response.status, 404)
            self.assertEqual(json.loads(response.read()),
                             {"error": "not_found"})
            connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


class ServeMigrationBatchesArgumentsTest(unittest.TestCase):
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

    def test_misplaced_empty_or_repeated_option_exits_2(self) -> None:
        ledger = os.path.join(self.tmp.name, "coord.json")
        for args in (
            ("--migration-batches", ledger),                  # no --audit
            ("--audit", "j", "--migration-batches", ledger),  # no method
            ("--audit", "j", "--token", "t",
             "--migration-batches", ledger),                   # single token
            ("--audit", "j", "--auth", self.config,
             "--migration-batches", ""),                       # empty value
            ("--audit", "j", "--auth", self.config,
             "--migration-batches", ledger,
             "--migration-batches", ledger),                   # repeated
        ):
            with self.subTest(args=args):
                self.assertEqual(self._run(*args).returncode, 2)

    def test_valid_option_starts_and_serves(self) -> None:
        import socket
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        process = subprocess.Popen(
            [sys.executable, "-m", "carbon_market", "serve",
             "--host", "127.0.0.1", "--port", str(port),
             "--audit", os.path.join(self.tmp.name, "audit.json"),
             "--auth", self.config,
             "--migration-batches", os.path.join(self.tmp.name, "coord.json")])
        try:
            deadline = time.time() + 10
            statuses = []
            while time.time() < deadline:
                try:
                    connection = HTTPConnection("127.0.0.1", port, timeout=1)
                    connection.request(
                        "GET", "/migration-batches",
                        headers={"X-Audit-Token": FULL_TOKEN})
                    response = connection.getresponse()
                    body = json.loads(response.read())
                    statuses.append((response.status, body))
                    connection.close()
                    break
                except OSError:
                    time.sleep(0.05)
            # The coordination ledger does not exist yet, but
            # authorization worked.
            self.assertEqual(statuses,
                             [(404, {"error":
                                     "migration_batches_not_found"})])
        finally:
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
