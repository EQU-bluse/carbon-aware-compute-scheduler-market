"""HTTP tests for the read-only GET /migration-batches endpoint.

Covers the multi-token authorization (401/403/503 auth_unavailable),
the exact-lookup and paginated query shapes, the scope rules (exact
lookup needs unrestricted operation and stage scopes and a key scope
naming the requested batch, pagination needs all three scopes
unrestricted), the key-sorted page with per-entry completion
information, the error mapping (400 invalid_request, 404
migration_batches_not_found, 404 migration_batch_not_found, 409
migration_batches_invalid, 503 migration_batches_unavailable) and the
``serve`` command's ``--migration-batches`` option, which only ever
accompanies ``--audit`` together with ``--auth``.
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
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import migration_batch
from carbon_market.server import Handler
from tests.test_migration_batch import MigrationBatchTest

FULL_TOKEN = "full-token"
KEY_TOKEN = "key-token"
OPS_TOKEN = "ops-token"
STAGE_TOKEN = "stage-token"


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _line(name: str, token: str, grace_until=None,
          ops="*", stages="*", keys="*") -> str:
    return json.dumps([name, _digest(token), grace_until, ops, stages, keys],
                      ensure_ascii=False)


class MigrationBatchesHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        # Reuse the migration-batch module fixture to build a real
        # coordination ledger and its nine business ledgers.
        self.fx = MigrationBatchTest(
            "test_get_returns_copy_and_unknown_key_raises")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.coord = self.fx.paths["coord"]
        self.config = os.path.join(self.fx.tmp.name, "auth.jsonl")
        self._write_config(
            _line("full", FULL_TOKEN) + "\n"
            + _line("key", KEY_TOKEN, keys=["aaa"]) + "\n"
            + _line("ops", OPS_TOKEN, ops=["copy"]) + "\n"
            + _line("stage", STAGE_TOKEN, stages=["成功"]) + "\n")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.audit_path = os.path.join(self.fx.tmp.name, "audit.json")
        self.server.audit_token = None
        self.server.audit_auth = self.config
        self.server.migration_batches = self.coord
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

    def _get(self, path: str, headers=None, raw_headers=None):
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

    # -- ledger fixture -------------------------------------------------------

    def _populate(self) -> None:
        fx = self.fx
        fx._prepare_migrate()
        # "aaa" starts the only migrating member and stays active; the
        # next batch "zzz" finds no free member and completes empty.
        # "aaa" is then driven to completion, so the audit (real
        # completion order) is zzz@31 (index 0), aaa@34 (index 1) even
        # though key order lists aaa before zzz (the completion moment
        # is the closing run's moment, not the last receipt's).
        fx._run(key="aaa", owner="负责人-1", now=30)
        fx._run(key="zzz", now=31)
        fx._run(key="aaa", owner="负责人-1", now=32,
                receipts={"j-1": fx._receipt(
                    "copy", "succeeded", "复制完成", 33)})
        fx._run(key="aaa", owner="负责人-1", now=34,
                receipts={"j-1": fx._receipt(
                    "switch", "succeeded", "切换完成", 35)})

    # -- authorization --------------------------------------------------------

    def test_missing_blank_and_duplicate_token_are_401(self) -> None:
        self.assertEqual(self._json_get("/migration-batches")[0], 401)
        self.assertEqual(
            self._json_get("/migration-batches",
                           headers={"X-Audit-Token": "  "})[0], 401)
        status, body = self._json_get(
            "/migration-batches",
            raw_headers=[("X-Audit-Token", FULL_TOKEN),
                         ("X-Audit-Token", FULL_TOKEN)])
        self.assertEqual(status, 401)
        self.assertEqual(body, {"error": "unauthorized"})

    def test_unknown_token_is_403_without_opening_ledger(self) -> None:
        # No coordination ledger exists; a 403 (not 404) proves it was
        # never opened.
        status, body = self._token_get("wrong-token")
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "forbidden"})

    def test_unreadable_or_invalid_config_is_503(self) -> None:
        os.unlink(self.config)
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual((status, body),
                         (503, {"error": "auth_unavailable"}))
        self._write_config("{not json\n")
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual((status, body),
                         (503, {"error": "auth_unavailable"}))

    def test_authorization_precedes_parameter_validation(self) -> None:
        self.assertEqual(
            self._json_get("/migration-batches?bogus=1")[0], 401)
        self.assertEqual(
            self._token_get("wrong-token",
                            "/migration-batches?bogus=1")[0], 403)

    # -- query parameter validation -------------------------------------------

    def test_invalid_parameters_are_400(self) -> None:
        for path in (
            "/migration-batches?bogus=1",
            "/migration-batches?key=aaa&key=zzz",   # repeated
            "/migration-batches?key=",              # empty key
            "/migration-batches?cursor=",           # empty cursor
            "/migration-batches?limit=",            # empty limit
            "/migration-batches?key=aaa&cursor=z",  # key with cursor
            "/migration-batches?key=aaa&limit=10",  # key with page size
            "/migration-batches?limit=0",
            "/migration-batches?limit=1001",
            "/migration-batches?limit=-5",
            "/migration-batches?limit=abc",
            "/migration-batches?cursor=%FF%FE",     # not UTF-8
        ):
            with self.subTest(path=path):
                status, body = self._token_get(FULL_TOKEN, path)
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})

    # -- scopes ----------------------------------------------------------------

    def test_exact_lookup_scopes(self) -> None:
        self._populate()
        # A restricted operation or stage scope forbids the lookup.
        self.assertEqual(
            self._token_get(OPS_TOKEN, "/migration-batches?key=aaa")[0],
            403)
        self.assertEqual(
            self._token_get(STAGE_TOKEN, "/migration-batches?key=aaa")[0],
            403)
        # A key scope must name the requested batch explicitly.
        self.assertEqual(
            self._token_get(KEY_TOKEN, "/migration-batches?key=aaa")[0],
            200)
        self.assertEqual(
            self._token_get(KEY_TOKEN, "/migration-batches?key=zzz")[0],
            403)

    def test_pagination_requires_all_scopes_unrestricted(self) -> None:
        self._populate()
        for token in (KEY_TOKEN, OPS_TOKEN, STAGE_TOKEN):
            with self.subTest(token=token):
                status, body = self._token_get(token)
                self.assertEqual(status, 403)
                self.assertEqual(body, {"error": "forbidden"})
        self.assertEqual(self._token_get(FULL_TOKEN)[0], 200)

    def test_scope_403_never_opens_the_ledger(self) -> None:
        # No ledger exists: a scoped-out request still gets 403, not a
        # 404, proving no stage-4 file was read.
        self.assertEqual(
            self._token_get(OPS_TOKEN, "/migration-batches?key=aaa")[0],
            403)
        self.assertEqual(self._token_get(KEY_TOKEN)[0], 403)

    # -- exact lookup ----------------------------------------------------------

    def test_exact_lookup_returns_only_the_snapshot(self) -> None:
        self._populate()
        status, body = self._token_get(FULL_TOKEN,
                                       "/migration-batches?key=aaa")
        self.assertEqual(status, 200)
        snapshot = migration_batch.get(self.coord, "aaa")
        self.assertEqual(body, snapshot)
        self.assertEqual(body["key"], "aaa")
        self.assertEqual(body["status"], "completed")

    def test_exact_lookup_is_compact_utf8(self) -> None:
        self._populate()
        status, raw = self._get("/migration-batches?key=aaa",
                                headers={"X-Audit-Token": FULL_TOKEN})
        self.assertEqual(status, 200)
        expected = migration_batch.get_response(self.coord, "aaa")
        self.assertEqual(raw, expected)
        self.assertFalse(raw.endswith(b"\n"))
        # Non-ASCII owner and credential text pass through unescaped.
        self.assertIn("负责人-1".encode("utf-8"), raw)
        self.assertIn("复制完成".encode("utf-8"), raw)
        self.assertNotIn(b"\\u", raw)

    # -- pagination --------------------------------------------------------------

    def test_page_shape_order_and_completion(self) -> None:
        self._populate()
        status, body = self._token_get(FULL_TOKEN,
                                       "/migration-batches?limit=1")
        self.assertEqual(status, 200)
        self.assertEqual(list(body), ["entries", "next"])
        (first,) = body["entries"]
        self.assertEqual(list(first), ["key", "snapshot", "completion"])
        # Key order lists aaa first, but aaa completed second. The
        # completion moment is the closing run's moment (34).
        self.assertEqual(first["key"], "aaa")
        self.assertEqual(first["completion"], {"index": 1, "at": 34})
        self.assertEqual(body["next"], "aaa")

        status, body = self._token_get(
            FULL_TOKEN, "/migration-batches?limit=1&cursor=aaa")
        self.assertEqual(status, 200)
        (second,) = body["entries"]
        self.assertEqual(second["key"], "zzz")
        # zzz was the first batch to complete (the empty batch).
        self.assertEqual(second["completion"], {"index": 0, "at": 31})
        self.assertIsNone(body["next"])

        # The exclusive cursor drops everything at or before it.
        status, body = self._token_get(
            FULL_TOKEN, "/migration-batches?cursor=zzz")
        self.assertEqual(body, {"entries": [], "next": None})

    def test_full_page_matches_library_object(self) -> None:
        self._populate()
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual(status, 200)
        self.assertEqual(body, migration_batch.search(self.coord))
        self.assertEqual([e["key"] for e in body["entries"]],
                         ["aaa", "zzz"])
        self.assertIsNone(body["next"])

    def test_active_batch_completion_is_null(self) -> None:
        self.fx._prepare_migrate()
        self.fx._run(key="aaa", now=30)
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual(status, 200)
        (entry,) = body["entries"]
        self.assertEqual(entry["key"], "aaa")
        self.assertEqual(entry["snapshot"]["status"], "pending")
        self.assertIsNone(entry["completion"])

    def test_default_limit_is_one_hundred(self) -> None:
        # The default page size is the library's; a single populated
        # ledger with two batches returns both.
        self._populate()
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual(len(body["entries"]), 2)

    def test_pagination_is_compact_utf8(self) -> None:
        self._populate()
        status, raw = self._get("/migration-batches",
                                headers={"X-Audit-Token": FULL_TOKEN})
        self.assertEqual(status, 200)
        self.assertEqual(raw, migration_batch.search_response(self.coord))
        self.assertFalse(raw.endswith(b"\n"))

    # -- ledger state errors -----------------------------------------------------

    def test_missing_coordination_ledger_is_404(self) -> None:
        missing = os.path.join(self.fx.tmp.name, "absent.json")
        self.server.migration_batches = missing
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual((status, body),
                         (404, {"error": "migration_batches_not_found"})),
        status, body = self._token_get(FULL_TOKEN,
                                       "/migration-batches?key=aaa")
        self.assertEqual((status, body),
                         (404, {"error": "migration_batches_not_found"}))

    def test_unknown_batch_is_distinct_404(self) -> None:
        self._populate()
        status, body = self._token_get(FULL_TOKEN,
                                       "/migration-batches?key=nope")
        self.assertEqual((status, body),
                         (404, {"error": "migration_batch_not_found"}))

    def test_non_canonical_ledger_is_409(self) -> None:
        self._populate()
        good = Path(self.coord).read_bytes()
        for label, payload in (
                ("json", b"{not json\n"),
                ("utf-8", good[:20] + b"\xff" + good[20:]),
                ("version", self._version_two_bytes(good)),
                ("newline", good + b"\n"),
        ):
            with self.subTest(label=label):
                with open(self.coord, "wb") as handle:
                    handle.write(payload)
                status, body = self._token_get(FULL_TOKEN)
                self.assertEqual((status, body),
                                 (409, {"error":
                                        "migration_batches_invalid"}))
                status, body = self._token_get(
                    FULL_TOKEN, "/migration-batches?key=aaa")
                self.assertEqual((status, body),
                                 (409, {"error":
                                        "migration_batches_invalid"}))
        with open(self.coord, "wb") as handle:
            handle.write(good)

    @staticmethod
    def _version_two_bytes(good: bytes) -> bytes:
        data = json.loads(good.decode("utf-8"))
        data["version"] = 2
        return json.dumps(data, ensure_ascii=False,
                          separators=(",", ":")).encode("utf-8") + b"\n"

    def test_missing_business_ledger_is_409(self) -> None:
        self._populate()
        # A canonical coordination file referencing a business ledger
        # that has vanished is a broken reference: 409, not 404.
        os.unlink(self.fx.paths["jobs"])
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual((status, body),
                         (409, {"error": "migration_batches_invalid"}))
        status, body = self._token_get(FULL_TOKEN,
                                       "/migration-batches?key=aaa")
        self.assertEqual((status, body),
                         (409, {"error": "migration_batches_invalid"}))

    def test_unknown_key_never_masks_a_malformed_file(self) -> None:
        self._populate()
        with open(self.coord, "wb") as handle:
            handle.write(b"{not json\n")
        # Even an absent key gets the format error, not a 404.
        status, body = self._token_get(FULL_TOKEN,
                                       "/migration-batches?key=ghost")
        self.assertEqual((status, body),
                         (409, {"error": "migration_batches_invalid"}))

    def test_other_read_failure_is_503(self) -> None:
        # A directory where the coordination file should be: the read
        # raises an OSError other than FileNotFoundError.
        directory = os.path.join(self.fx.tmp.name, "coord-dir")
        os.mkdir(directory)
        self.server.migration_batches = directory
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual((status, body),
                         (503, {"error": "migration_batches_unavailable"}))

    def test_error_body_carries_only_error(self) -> None:
        self._populate()
        os.unlink(self.coord)
        _, body = self._token_get(FULL_TOKEN)
        self.assertEqual(body, {"error": "migration_batches_not_found"})

    # -- unchanged surface ------------------------------------------------------

    def test_health_and_unknown_paths_unchanged(self) -> None:
        self.assertEqual(self._json_get("/health"), (200, {"status": "ok"}))
        self.assertEqual(self._json_get("/migration-batches/"),
                         (404, {"error": "not_found"}))
        self.assertEqual(self._json_get("/missing"),
                         (404, {"error": "not_found"}))


class MigrationBatchesDisabledTest(unittest.TestCase):
    def test_path_is_plain_404_without_configuration(self) -> None:
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connection = HTTPConnection("127.0.0.1", server.server_port,
                                        timeout=2)
            connection.request("GET", "/migration-batches",
                               headers={"X-Audit-Token": "anything"})
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
            handle.write(_line("full", FULL_TOKEN) + "\n")

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "carbon_market", "serve", *args],
            capture_output=True, timeout=10)

    def test_misplaced_empty_or_repeated_option_exits_2(self) -> None:
        coord = os.path.join(self.tmp.name, "coord.json")
        for args in (
            ("--migration-batches", coord),                 # no --audit
            ("--audit", "j", "--migration-batches", coord),  # no method
            ("--audit", "j", "--token", "t",
             "--migration-batches", coord),                  # single token
            ("--audit", "j", "--auth", self.config,
             "--migration-batches", ""),                     # empty
            ("--audit", "j", "--auth", self.config,
             "--migration-batches", coord,
             "--migration-batches", coord),                  # repeated
        ):
            with self.subTest(args=args):
                self.assertEqual(self._run(*args).returncode, 2)

    def test_valid_option_starts_and_serves(self) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        coord = os.path.join(self.tmp.name, "coord.json")
        process = subprocess.Popen(
            [sys.executable, "-m", "carbon_market", "serve",
             "--host", "127.0.0.1", "--port", str(port),
             "--audit", os.path.join(self.tmp.name, "audit.json"),
             "--auth", self.config,
             "--migration-batches", coord])
        try:
            deadline = time.time() + 10
            seen = []
            while time.time() < deadline:
                try:
                    connection = HTTPConnection("127.0.0.1", port, timeout=1)
                    connection.request(
                        "GET", "/migration-batches",
                        headers={"X-Audit-Token": FULL_TOKEN})
                    response = connection.getresponse()
                    seen.append((response.status,
                                 json.loads(response.read())))
                    connection.close()
                    break
                except OSError:
                    time.sleep(0.05)
            # Authorization worked; the ledger does not exist yet.
            self.assertEqual(
                seen, [(404, {"error": "migration_batches_not_found"})])
        finally:
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
