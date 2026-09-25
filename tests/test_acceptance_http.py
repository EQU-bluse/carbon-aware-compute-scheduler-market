"""HTTP tests for the read-only GET /acceptance ledger query."""

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

from carbon_market import acceptance, audit, audit_proof
from carbon_market.server import Handler

FULL_TOKEN = "full-token"
KEY_TOKEN = "key-token"
SCOPED_TOKEN = "scoped-token"


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _event(op: str = "copy", key: str = "batch-key",
           error: str | None = None, stage: str | None = None) -> dict:
    return {"op": op, "target": "t.history", "key": key,
            "changed": True, "error": error, "stage": stage}


class _HttpFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.checkpoint = os.path.join(self.tmp.name, "checkpoint.json")
        self.proof_path = os.path.join(self.tmp.name, "proof.json")
        self.ledger_dir = os.path.join(self.tmp.name, "ledger")
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=self.server.serve_forever,
                                  daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)

    @staticmethod
    def _line(name: str, token: str, grace_until,
              ops="*", stages="*", keys="*") -> str:
        return json.dumps([name, _digest(token), grace_until, ops, stages,
                           keys], ensure_ascii=False)

    def _write_config(self, text: str) -> None:
        tmp_path = self.config + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp_path, self.config)

    def _configure(self) -> None:
        self.server.audit_path = self.journal
        self.server.audit_token = None
        self.server.audit_auth = self.config
        self.server.acceptance_dir = self.ledger_dir

    def _request(self, path: str = "/acceptance",
                 headers: dict[str, str] | None = None,
                 raw_headers: list[tuple[str, str]] | None = None):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=5)
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
        return response.status, response.headers, body

    def _get(self, **kwargs):
        status, _, body = self._request(**kwargs)
        return status, json.loads(body) if body else None

    def _auth_headers(self, token: str = FULL_TOKEN) -> dict[str, str]:
        return {"X-Audit-Token": token}

    def _submit(self, key: str, etag: str | None = None) -> None:
        audit.record(self.journal, key, _event())
        proof = audit_proof.export(self.journal, self.checkpoint, "g1")
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False))
        if etag is None:
            with open(self.checkpoint, "rb") as handle:
                etag = '"' + hashlib.sha256(handle.read()).hexdigest() + '"'
        try:
            acceptance.submit(self.checkpoint, self.proof_path, etag,
                              None, self.ledger_dir, key)
        except audit_proof.BundleMismatchError:
            pass


class AcceptanceHttpTest(_HttpFixture):
    def setUp(self) -> None:
        super().setUp()
        self._write_config(
            self._line("full", FULL_TOKEN, None) + "\n"
            + self._line("key", KEY_TOKEN, None, keys=["k1"]) + "\n"
            + self._line("scoped", SCOPED_TOKEN, None, ops=["copy"],
                         stages=["成功"], keys=["k1"]) + "\n")
        self._configure()

    def _populate(self) -> None:
        self._submit("k1")
        self._submit("k2")
        # k3 quarantines on a claimed-tag mismatch.
        self._submit("k3", etag='"' + "0" * 64 + '"')

    # -- authorization ----------------------------------------------------

    def test_missing_blank_and_duplicate_token_are_401(self) -> None:
        self.assertEqual(self._get()[0], 401)
        self.assertEqual(
            self._get(headers={"X-Audit-Token": " "})[0], 401)
        status, body = self._get(raw_headers=[
            ("X-Audit-Token", FULL_TOKEN), ("X-Audit-Token", FULL_TOKEN)])
        self.assertEqual(status, 401)
        self.assertEqual(body, {"error": "unauthorized"})
        # The ledger was never opened or created.
        self.assertFalse(os.path.exists(self.ledger_dir))

    def test_unknown_and_expired_token_are_403(self) -> None:
        status, body = self._get(headers=self._auth_headers("nope"))
        self.assertEqual((status, body), (403, {"error": "forbidden"}))
        self._write_config(self._line("full", FULL_TOKEN, 1) + "\n")
        status, body = self._get(headers=self._auth_headers())
        self.assertEqual((status, body), (403, {"error": "forbidden"}))
        self.assertFalse(os.path.exists(self.ledger_dir))

    def test_unreadable_or_invalid_config_is_503(self) -> None:
        os.unlink(self.config)
        status, body = self._get(headers=self._auth_headers())
        self.assertEqual((status, body), (503, {"error": "auth_unavailable"}))
        self._write_config("{not json\n")
        status, body = self._get(headers=self._auth_headers())
        self.assertEqual((status, body), (503, {"error": "auth_unavailable"}))
        self.assertFalse(os.path.exists(self.ledger_dir))

    # -- parameter validation ----------------------------------------------

    def test_unknown_repeated_and_empty_params_are_400(self) -> None:
        for path in ("/acceptance?x=1", "/acceptance?key=k1&key=k1",
                     "/acceptance?key=", "/acceptance?cursor=",
                     "/acceptance?limit=", "/acceptance?state="):
            with self.subTest(path=path):
                status, body = self._get(path=path,
                                         headers=self._auth_headers())
                self.assertEqual((status, body),
                                 (400, {"error": "invalid_request"}))

    def test_key_combinations_are_400(self) -> None:
        for path in ("/acceptance?key=k1&cursor=k0",
                     "/acceptance?key=k1&limit=10",
                     "/acceptance?key=k1&state=active"):
            with self.subTest(path=path):
                status, body = self._get(path=path,
                                         headers=self._auth_headers())
                self.assertEqual((status, body),
                                 (400, {"error": "invalid_request"}))

    def test_invalid_state_and_limit_are_400(self) -> None:
        for path in ("/acceptance?state=unknown", "/acceptance?state=ACTIVE",
                     "/acceptance?limit=0", "/acceptance?limit=1001",
                     "/acceptance?limit=abc", "/acceptance?limit=1.5"):
            with self.subTest(path=path):
                status, body = self._get(path=path,
                                         headers=self._auth_headers())
                self.assertEqual((status, body),
                                 (400, {"error": "invalid_request"}))

    def test_param_validation_follows_auth_and_precedes_scope(self) -> None:
        # Without a token the malformed query is still masked by 401.
        self.assertEqual(self._get(path="/acceptance?x=1")[0], 401)
        # A scoped token meets 400 before its 403.
        status, body = self._get(path="/acceptance?state=nope",
                                 headers=self._auth_headers(SCOPED_TOKEN))
        self.assertEqual((status, body), (400, {"error": "invalid_request"}))

    # -- scope rules --------------------------------------------------------

    def test_exact_query_scope_rules(self) -> None:
        self._populate()
        # Operation- or stage-restricted tokens may not query exactly.
        status, body = self._get(path="/acceptance?key=k1",
                                 headers=self._auth_headers(SCOPED_TOKEN))
        self.assertEqual((status, body), (403, {"error": "forbidden"}))
        # A key-scoped token reads only the keys its scope names.
        status, body = self._get(path="/acceptance?key=k2",
                                 headers=self._auth_headers(KEY_TOKEN))
        self.assertEqual((status, body), (403, {"error": "forbidden"}))
        status, body = self._get(path="/acceptance?key=k1",
                                 headers=self._auth_headers(KEY_TOKEN))
        self.assertEqual(status, 200)
        self.assertEqual(body["key"], "k1")

    def test_paginated_query_requires_all_star_scopes(self) -> None:
        self._populate()
        for token in (KEY_TOKEN, SCOPED_TOKEN):
            with self.subTest(token=token):
                status, body = self._get(
                    headers=self._auth_headers(token))
                self.assertEqual((status, body),
                                 (403, {"error": "forbidden"}))

    def test_scope_check_precedes_ledger_read(self) -> None:
        # No ledger exists: 403 (not 404) proves it stayed closed.
        status, body = self._get(path="/acceptance?key=k1",
                                 headers=self._auth_headers(SCOPED_TOKEN))
        self.assertEqual((status, body), (403, {"error": "forbidden"}))
        status, body = self._get(headers=self._auth_headers(KEY_TOKEN))
        self.assertEqual((status, body), (403, {"error": "forbidden"}))
        self.assertFalse(os.path.exists(self.ledger_dir))

    # -- ledger state errors -------------------------------------------------

    def test_missing_ledger_is_404(self) -> None:
        status, body = self._get(headers=self._auth_headers())
        self.assertEqual((status, body),
                         (404, {"error": "acceptance_not_found"}))
        status, body = self._get(path="/acceptance?key=k1",
                                 headers=self._auth_headers())
        self.assertEqual((status, body),
                         (404, {"error": "acceptance_not_found"}))

    def test_unknown_key_is_404(self) -> None:
        self._populate()
        status, body = self._get(path="/acceptance?key=unknown",
                                 headers=self._auth_headers())
        self.assertEqual((status, body),
                         (404, {"error": "acceptance_key_not_found"}))

    def test_invalid_ledger_is_409(self) -> None:
        self._populate()
        with open(os.path.join(self.ledger_dir, "ledger.json"),
                  "wb") as handle:
            handle.write(b"{not json")
        for path in ("/acceptance", "/acceptance?key=k1"):
            with self.subTest(path=path):
                status, body = self._get(path=path,
                                         headers=self._auth_headers())
                self.assertEqual((status, body),
                                 (409, {"error": "acceptance_invalid"}))

    def test_io_failure_is_503(self) -> None:
        # The ledger path being a directory opens with an OSError that
        # is not FileNotFoundError.
        self._populate()
        ledger = os.path.join(self.ledger_dir, "ledger.json")
        os.unlink(ledger)
        os.mkdir(ledger)
        status, body = self._get(headers=self._auth_headers())
        self.assertEqual((status, body),
                         (503, {"error": "acceptance_unavailable"}))

    # -- successful queries ---------------------------------------------------

    def test_exact_query_returns_the_record(self) -> None:
        self._populate()
        status, headers, body = self._request(
            path="/acceptance?key=k3", headers=self._auth_headers())
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("Content-Type"), "application/json")
        record = json.loads(body)
        self.assertEqual(list(record),
                         ["key", "etag", "checkpoint_digest",
                          "proof_digest", "state", "error"])
        self.assertEqual(record["key"], "k3")
        self.assertEqual(record["state"], "quarantined")
        self.assertEqual(record["error"], "BundleMismatchError")
        self.assertEqual(record, acceptance.get(self.ledger_dir, "k3"))
        # Compact UTF-8 with no trailing newline.
        self.assertFalse(body.endswith(b"\n"))
        self.assertNotIn(b": ", body)
        self.assertNotIn(b", ", body)

    def test_paginated_query_pages_in_key_order(self) -> None:
        self._populate()
        status, body = self._get(path="/acceptance?limit=2",
                                 headers=self._auth_headers())
        self.assertEqual(status, 200)
        self.assertEqual([key for key, _ in body["entries"]], ["k1", "k2"])
        self.assertEqual(body["next"], "k2")
        status, body = self._get(
            path="/acceptance?cursor=k2&limit=2",
            headers=self._auth_headers())
        self.assertEqual([key for key, _ in body["entries"]], ["k3"])
        self.assertIsNone(body["next"])
        # The state filter composes with the cursor.
        status, body = self._get(path="/acceptance?state=quarantined",
                                 headers=self._auth_headers())
        self.assertEqual([key for key, _ in body["entries"]], ["k3"])
        self.assertIsNone(body["next"])
        status, body = self._get(path="/acceptance?state=pending",
                                 headers=self._auth_headers())
        self.assertEqual(body, {"entries": [], "next": None})

    def test_non_ascii_key_ships_as_direct_utf8(self) -> None:
        self._submit("键-1")
        status, headers, body = self._request(
            path="/acceptance?key=%E9%94%AE-1",
            headers=self._auth_headers())
        self.assertEqual(status, 200)
        self.assertIn("键-1".encode("utf-8"), body)
        self.assertNotIn(b"\\u", body)
        record = json.loads(body)
        self.assertEqual(record["key"], "键-1")

    # -- unchanged surface ---------------------------------------------------

    def test_other_paths_unchanged(self) -> None:
        status, body = self._get(path="/health")
        self.assertEqual((status, body), (200, {"status": "ok"}))
        status, body = self._get(path="/acceptance/",
                                 headers=self._auth_headers())
        self.assertEqual((status, body), (404, {"error": "not_found"}))
        status, body = self._get(path="/missing",
                                 headers=self._auth_headers())
        self.assertEqual((status, body), (404, {"error": "not_found"}))


class AcceptanceDisabledTest(_HttpFixture):
    def test_path_is_plain_404_without_acceptance(self) -> None:
        self._write_config(self._line("full", FULL_TOKEN, None) + "\n")
        self.server.audit_path = self.journal
        self.server.audit_token = None
        self.server.audit_auth = self.config
        status, body = self._get(headers=self._auth_headers())
        self.assertEqual((status, body), (404, {"error": "not_found"}))
        status, body = self._get(path="/acceptance?key=k1",
                                 headers=self._auth_headers())
        self.assertEqual((status, body), (404, {"error": "not_found"}))


class ServeAcceptanceArgumentsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        with open(self.config, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(
                ["full", _digest(FULL_TOKEN), None, "*", "*", "*"]) + "\n")

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "carbon_market", "serve", *args],
            capture_output=True, timeout=10)

    def test_acceptance_pairing_errors_exit_2(self) -> None:
        ledger = os.path.join(self.tmp.name, "ledger")
        for args in (
            ("--acceptance", ledger),                       # alone
            ("--token", "t", "--acceptance", ledger),       # no --audit
            ("--audit", "j", "--acceptance", ledger),       # no method
            ("--audit", "j", "--token", "t",
             "--acceptance", ledger),                       # single token
            ("--audit", "j", "--auth", self.config,
             "--acceptance", ""),                           # empty
            ("--audit", "j", "--auth", self.config,
             "--acceptance", "a", "--acceptance", "b"),     # repeated
        ):
            with self.subTest(args=args):
                self.assertEqual(self._run(*args).returncode, 2)

    def test_valid_acceptance_starts_and_serves_queries(self) -> None:
        journal = os.path.join(self.tmp.name, "audit.json")
        ledger = os.path.join(self.tmp.name, "ledger")
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        process = subprocess.Popen(
            [sys.executable, "-m", "carbon_market", "serve",
             "--host", "127.0.0.1", "--port", str(port),
             "--audit", journal, "--auth", self.config,
             "--acceptance", ledger])
        try:
            deadline = time.time() + 10
            status = None
            while time.time() < deadline:
                try:
                    connection = HTTPConnection("127.0.0.1", port,
                                                timeout=1)
                    connection.request(
                        "GET", "/acceptance",
                        headers={"X-Audit-Token": FULL_TOKEN})
                    response = connection.getresponse()
                    status = response.status
                    body = json.loads(response.read())
                    connection.close()
                    break
                except OSError:
                    time.sleep(0.05)
            # The ledger does not exist yet, but the endpoint is live
            # and authorization worked.
            self.assertEqual(status, 404)
            self.assertEqual(body, {"error": "acceptance_not_found"})
        finally:
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
