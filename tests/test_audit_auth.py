"""Tests for the multi-token scoped authorization mode of GET /audit."""

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

from carbon_market import audit, auth
from carbon_market.server import Handler

FULL_TOKEN = "full-token"
OPS_TOKEN = "ops-token"
KEYS_TOKEN = "keys-token"
OLD_TOKEN = "old-token"
NEW_TOKEN = "new-token"


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _line(name: str, token: str, grace_until,
          ops="*", stages="*", keys="*") -> str:
    return json.dumps([name, _digest(token), grace_until, ops, stages, keys],
                      ensure_ascii=False)


def _event(op: str = "copy", key: str = "batch-key",
           error: str | None = None, stage: str | None = None) -> dict:
    return {"op": op, "target": "t.history", "key": key,
            "changed": True, "error": error, "stage": stage}


class AuthLoadTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "auth.jsonl")

    def _write(self, text: str | bytes) -> None:
        mode = "wb" if isinstance(text, bytes) else "w"
        with open(self.path, mode, encoding=None if "b" in mode else "utf-8") \
                as handle:
            handle.write(text)

    def test_valid_file_loads_records(self) -> None:
        self._write(
            _line("full", FULL_TOKEN, None) + "\n"
            + "\n"  # blank lines are skipped
            + _line("scoped", OPS_TOKEN, 2000000000,
                    ops=["copy"], stages=["成功", "同步"], keys=["k1"]) + "\n")
        records = auth.load(self.path)
        self.assertEqual([r.name for r in records], ["full", "scoped"])
        full, scoped = records
        self.assertEqual(full.digest, bytes.fromhex(_digest(FULL_TOKEN)))
        self.assertIsNone(full.grace_until)
        self.assertIsNone(full.ops)
        self.assertIsNone(full.stages)
        self.assertIsNone(full.keys)
        self.assertEqual(scoped.grace_until, 2000000000)
        self.assertEqual(scoped.ops, frozenset({"copy"}))
        self.assertEqual(scoped.stages, frozenset({"成功", "同步"}))
        self.assertEqual(scoped.keys, frozenset({"k1"}))

    def test_missing_and_empty_file_are_rejected(self) -> None:
        with self.assertRaises(FileNotFoundError):
            auth.load(self.path)
        for text in ("", "  \n\n\t\n"):
            with self.subTest(text=text):
                self._write(text)
                with self.assertRaises(ValueError):
                    auth.load(self.path)

    def test_invalid_utf8_and_json_are_rejected(self) -> None:
        self._write(b'["a", "' + b"x" * 64 + b'", null, "*", "*", "*"]\n\xff')
        with self.assertRaises(ValueError):
            auth.load(self.path)
        for line in ("{not json", '["a", "b"', '"just a string"',
                     '["a", "' + "0" * 64 + '", -0, "*", "*", "*"]'):
            with self.subTest(line=line):
                self._write(line + "\n")
                with self.assertRaises(ValueError):
                    auth.load(self.path)

    def test_wrong_shape_is_rejected(self) -> None:
        good = _digest(FULL_TOKEN)
        for line in (
                "[]",
                f'["n", "{good}"]',
                f'["n", "{good}", null, "*", "*"]',
                f'["n", "{good}", null, "*", "*", "*", "extra"]',
                '{"name": "n"}',
                f'[1, "{good}", null, "*", "*", "*"]',
                f'["", "{good}", null, "*", "*", "*"]'):
            with self.subTest(line=line):
                self._write(line + "\n")
                with self.assertRaises(ValueError):
                    auth.load(self.path)

    def test_bad_digest_is_rejected(self) -> None:
        for digest in ("", "0" * 63, "0" * 65, "A" + "0" * 63,
                       "g" * 64, 123, None):
            with self.subTest(digest=digest):
                self._write(
                    json.dumps(["n", digest, None, "*", "*", "*"]) + "\n")
                with self.assertRaises(ValueError):
                    auth.load(self.path)

    def test_bad_grace_cutoff_is_rejected(self) -> None:
        for cutoff in (True, False, -1, 1.5, "100"):
            with self.subTest(cutoff=cutoff):
                self._write(
                    json.dumps(["n", _digest(FULL_TOKEN), cutoff,
                                "*", "*", "*"]) + "\n")
                with self.assertRaises(ValueError):
                    auth.load(self.path)

    def test_bad_scopes_are_rejected(self) -> None:
        token = _digest(FULL_TOKEN)
        bad_scopes = (
            [0, "*", "*"],                 # not "*" and not an array
            [["copy", "copy"], "*", "*"],  # duplicates
            [[""], "*", "*"],              # empty string
            [[1], "*", "*"],               # non-string
            [["delete"], "*", "*"],        # unknown operation
            ["*", ["unknown"], "*"],       # unknown stage
            ["*", "*", [""]],              # empty history key
        )
        for ops, stages, keys in bad_scopes:
            with self.subTest(ops=ops, stages=stages, keys=keys):
                self._write(
                    json.dumps(["n", token, None, ops, stages, keys],
                               ensure_ascii=False) + "\n")
                with self.assertRaises(ValueError):
                    auth.load(self.path)

    def test_duplicate_name_or_digest_is_rejected(self) -> None:
        self._write(_line("dup", FULL_TOKEN, None) + "\n"
                    + _line("dup", NEW_TOKEN, None) + "\n")
        with self.assertRaises(ValueError):
            auth.load(self.path)
        self._write(_line("one", FULL_TOKEN, None) + "\n"
                    + _line("two", FULL_TOKEN, None) + "\n")
        with self.assertRaises(ValueError):
            auth.load(self.path)


class IdentifyTest(unittest.TestCase):
    def _records(self) -> list[auth.Record]:
        return [
            auth.Record("full", bytes.fromhex(_digest(FULL_TOKEN)), None,
                        None, None, None),
            auth.Record("grace", bytes.fromhex(_digest(OLD_TOKEN)), 1000,
                        None, None, None),
        ]

    def test_known_token_identifies_unknown_is_none(self) -> None:
        records = self._records()
        self.assertEqual(auth.identify(records, FULL_TOKEN).name, "full")
        self.assertIsNone(auth.identify(records, "no-such-token"))

    def test_grace_cutoff_boundary(self) -> None:
        records = self._records()
        # Valid while the current second is not greater than the cutoff.
        self.assertEqual(auth.identify(records, OLD_TOKEN, now=999).name,
                         "grace")
        self.assertEqual(auth.identify(records, OLD_TOKEN, now=1000).name,
                         "grace")
        self.assertIsNone(auth.identify(records, OLD_TOKEN, now=1001))

    def test_scope_allows(self) -> None:
        record = auth.Record("scoped", b"\0" * 32, None,
                             frozenset({"copy"}), None, frozenset({"k1"}))
        self.assertTrue(auth.scope_allows(record, {"op": "copy", "key": "k1"}))
        self.assertFalse(auth.scope_allows(record, {"key": "k1"}))
        self.assertFalse(auth.scope_allows(record, {"op": "restore",
                                                    "key": "k1"}))
        self.assertFalse(auth.scope_allows(record, {"op": "copy"}))
        self.assertFalse(auth.scope_allows(record, {"op": "copy", "key": "k2"}))
        unrestricted = auth.Record("full", b"\0" * 32, None, None, None, None)
        self.assertTrue(auth.scope_allows(unrestricted, {}))
        self.assertTrue(auth.scope_allows(
            unrestricted, {"op": "restore", "stage": "成功", "key": "any"}))


class AuditAuthHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        self._write_config(
            _line("full", FULL_TOKEN, None) + "\n"
            + _line("ops", OPS_TOKEN, None,
                    ops=["copy"], stages=["成功"], keys=["hist-1"]) + "\n")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.audit_path = self.journal
        self.server.audit_token = None
        self.server.audit_auth = self.config
        thread = threading.Thread(target=self.server.serve_forever,
                                  daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)

    def _write_config(self, text: str) -> None:
        # Same-directory atomic replacement, as a rotation would do.
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
        return response.status, json.loads(body)

    def _token_get(self, token: str, path: str = "/audit"):
        return self._get(path, headers={"X-Audit-Token": token})

    def _record(self, audit_key: str, **event_kwargs) -> None:
        audit.record(self.journal, audit_key, _event(**event_kwargs))

    # -- header and identity handling --------------------------------------

    def test_missing_blank_and_duplicate_token_are_401(self) -> None:
        self.assertEqual(self._get("/audit")[0], 401)
        self.assertEqual(
            self._get("/audit", headers={"X-Audit-Token": "  "})[0], 401)
        status, body = self._get(
            "/audit", raw_headers=[("X-Audit-Token", FULL_TOKEN),
                                   ("X-Audit-Token", FULL_TOKEN)])
        self.assertEqual(status, 401)
        self.assertEqual(list(body), ["error"])

    def test_unknown_token_is_403_without_touching_the_journal(self) -> None:
        # The journal does not exist; a 403 (not 404) proves it was never
        # opened.
        status, body = self._token_get("wrong-token")
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "forbidden"})

    def test_expired_token_is_403_like_an_unknown_one(self) -> None:
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

    # -- validation order ---------------------------------------------------

    def test_identity_precedes_parameter_validation(self) -> None:
        self.assertEqual(self._get("/audit?bogus=1")[0], 401)
        self.assertEqual(self._token_get("wrong-token", "/audit?bogus=1")[0],
                         403)

    def test_parameter_validation_precedes_scope_check(self) -> None:
        # OPS_TOKEN lacks the op filter its scope requires, but the
        # invalid parameter surfaces first.
        status, body = self._token_get(OPS_TOKEN, "/audit?limit=0")
        self.assertEqual(status, 400)
        self.assertEqual(body, {"error": "invalid_request"})

    # -- scopes ---------------------------------------------------------------

    def test_full_token_behaves_like_the_single_token_mode(self) -> None:
        self._record("a", key="hist-1")
        self._record("b", op="restore", key="hist-2")
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual(status, 200)
        self.assertEqual([key for key, _ in body["events"]], ["a", "b"])
        status, body = self._token_get(FULL_TOKEN, "/audit?op=restore")
        self.assertEqual([key for key, _ in body["events"]], ["b"])

    def test_scoped_token_must_filter_within_its_scope(self) -> None:
        self._record("a", key="hist-1")
        self._record("b", op="restore", key="hist-2")
        ok = ("/audit?op=copy&key=hist-1&stage=" + "%E6%88%90%E5%8A%9F",)
        for path in ok:
            with self.subTest(path=path):
                status, body = self._token_get(OPS_TOKEN, path)
                self.assertEqual(status, 200)
                self.assertEqual([key for key, _ in body["events"]], ["a"])
        # Missing any of the required filters, or values outside the
        # scope, are a plain 403 -- and never reach the journal.
        for path in ("/audit",
                     "/audit?op=copy",
                     "/audit?op=copy&key=hist-1",
                     "/audit?op=restore&key=hist-1&stage=%E6%88%90%E5%8A%9F",
                     "/audit?op=copy&key=hist-2&stage=%E6%88%90%E5%8A%9F",
                     "/audit?op=copy&key=hist-1&stage=%E5%90%8C%E6%AD%A5"):
            with self.subTest(path=path):
                status, body = self._token_get(OPS_TOKEN, path)
                self.assertEqual(status, 403)
                self.assertEqual(body, {"error": "forbidden"})

    def test_scope_403_never_opens_the_journal(self) -> None:
        # No journal exists: a scoped-out request still gets 403, not 404.
        status, body = self._token_get(OPS_TOKEN, "/audit")
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "forbidden"})

    # -- rotation -------------------------------------------------------------

    def test_rotation_swaps_the_whole_configuration_without_restart(self) -> None:
        self._record("a", key="hist-1")
        self.assertEqual(self._token_get(OLD_TOKEN)[0], 403)
        cutoff = int(time.time()) - 1
        self._write_config(
            _line("old", OLD_TOKEN, cutoff) + "\n"
            + _line("new", NEW_TOKEN, None) + "\n")
        # The expired old token is rejected immediately, the new one
        # works -- all without restarting the server.
        self.assertEqual(self._token_get(OLD_TOKEN)[0], 403)
        status, body = self._token_get(NEW_TOKEN)
        self.assertEqual(status, 200)
        self.assertEqual([key for key, _ in body["events"]], ["a"])

    # -- unchanged surface ----------------------------------------------------

    def test_health_and_unknown_paths_unchanged(self) -> None:
        self.assertEqual(self._get("/health"), (200, {"status": "ok"}))
        self.assertEqual(self._get("/missing"), (404, {"error": "not_found"}))


class ServeAuthArgumentsTest(unittest.TestCase):
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

    def test_conflicting_incomplete_or_empty_methods_exit_2(self) -> None:
        for args in (
            ("--auth", self.config),                       # no --audit
            ("--audit", "journal.json"),                   # no method
            ("--audit", "j", "--token", "t", "--auth", self.config),
            ("--audit", "", "--auth", self.config),
            ("--audit", "j", "--auth", ""),
            ("--audit", "a", "--auth", self.config,
             "--auth", self.config),                       # repeated option
        ):
            with self.subTest(args=args):
                self.assertEqual(self._run(*args).returncode, 2)

    def test_invalid_auth_config_exits_2(self) -> None:
        missing = os.path.join(self.tmp.name, "missing.jsonl")
        self.assertEqual(
            self._run("--audit", "j", "--auth", missing).returncode, 2)
        bad = os.path.join(self.tmp.name, "bad.jsonl")
        with open(bad, "w", encoding="utf-8") as handle:
            handle.write("{not json\n")
        self.assertEqual(
            self._run("--audit", "j", "--auth", bad).returncode, 2)

    def test_valid_auth_config_starts_and_serves(self) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        process = subprocess.Popen(
            [sys.executable, "-m", "carbon_market", "serve",
             "--host", "127.0.0.1", "--port", str(port),
             "--audit", os.path.join(self.tmp.name, "audit.json"),
             "--auth", self.config])
        try:
            deadline = time.time() + 10
            status = None
            while time.time() < deadline:
                try:
                    connection = HTTPConnection("127.0.0.1", port, timeout=1)
                    connection.request("GET", "/audit",
                                       headers={"X-Audit-Token": FULL_TOKEN})
                    status = connection.getresponse().status
                    connection.close()
                    break
                except OSError:
                    time.sleep(0.05)
            # The journal does not exist yet, but authorization worked.
            self.assertEqual(status, 404)
        finally:
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
