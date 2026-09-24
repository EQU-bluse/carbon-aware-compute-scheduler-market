"""HTTP tests for the checkpoint-backed GET /audit/proof endpoint."""

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
from unittest import mock

from carbon_market import audit, audit_proof
from carbon_market.server import Handler

TOKEN = "s3cret"
FULL_TOKEN = "full-token"
OPS_TOKEN = "ops-token"


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _event(op: str = "copy", key: str = "batch-key",
           error: str | None = None, stage: str | None = None) -> dict:
    return {"op": op, "target": "t.history", "key": key,
            "changed": True, "error": error, "stage": stage}


def _sealed(events: dict[str, dict]) -> bytes:
    # Build a sealed version 2 document with the same compact convention
    # audit.record uses, allowing hand-crafted journals.
    chain: dict[str, list] = {}
    previous = None
    for audit_key in sorted(events):
        event = events[audit_key]
        payload = json.dumps([audit_key, event, previous],
                             ensure_ascii=False, separators=(",", ":"),
                             allow_nan=False)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        chain[audit_key] = [event, previous, digest]
        previous = digest
    doc = {"version": 2, "events": chain, "head": previous}
    return (json.dumps(doc, ensure_ascii=False, separators=(",", ":"),
                       allow_nan=False) + "\n").encode("utf-8")


class _HttpFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.checkpoint = os.path.join(self.tmp.name, "checkpoint.json")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=self.server.serve_forever,
                                  daemon=True)
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

    def _auth_get(self, path: str, token: str = TOKEN):
        return self._get(path, headers={"X-Audit-Token": token})

    def _record(self, audit_key: str, **event_kwargs) -> None:
        audit.record(self.journal, audit_key, _event(**event_kwargs))

    def _anchors(self, generation: str = "g1") -> list[dict]:
        with open(self.checkpoint, encoding="utf-8") as handle:
            doc = json.load(handle)
        return next(g["anchors"] for g in doc["generations"]
                    if g["name"] == generation)


class ProofHttpTest(_HttpFixture):
    def setUp(self) -> None:
        super().setUp()
        self.server.audit_path = self.journal
        self.server.audit_token = TOKEN
        self.server.audit_checkpoint = self.checkpoint

    # -- authorization ----------------------------------------------------

    def test_missing_blank_and_duplicate_token_are_401(self) -> None:
        self.assertEqual(self._get("/audit/proof?generation=g1")[0], 401)
        self.assertEqual(
            self._get("/audit/proof?generation=g1",
                      headers={"X-Audit-Token": "  "})[0], 401)
        status, body = self._get(
            "/audit/proof?generation=g1",
            raw_headers=[("X-Audit-Token", TOKEN),
                         ("X-Audit-Token", TOKEN)])
        self.assertEqual(status, 401)
        self.assertEqual(list(body), ["error"])

    def test_wrong_token_is_403_and_never_touches_files(self) -> None:
        # No journal exists: a 403 (not 404) proves nothing was opened.
        status, body = self._get("/audit/proof?generation=g1",
                                 headers={"X-Audit-Token": "nope"})
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "forbidden"})
        self.assertFalse(os.path.exists(self.checkpoint))

    def test_auth_precedes_parameter_validation(self) -> None:
        self.assertEqual(self._get("/audit/proof?bogus=1")[0], 401)
        self.assertEqual(
            self._get("/audit/proof?bogus=1",
                      headers={"X-Audit-Token": "nope"})[0], 403)

    # -- query parameter validation ----------------------------------------

    def test_generation_is_required_and_non_empty(self) -> None:
        for path in ("/audit/proof", "/audit/proof?limit=5",
                     "/audit/proof?generation="):
            with self.subTest(path=path):
                status, body = self._auth_get(path)
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})

    def test_final_accepts_only_true_or_false(self) -> None:
        self._record("a")
        for value in ("yes", "1", "True", "TRUE", ""):
            with self.subTest(value=value):
                status, body = self._auth_get(
                    f"/audit/proof?generation=g1&final={value}")
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})
        self.assertFalse(os.path.exists(self.checkpoint))
        status, body = self._auth_get("/audit/proof?generation=g1&final=true")
        self.assertEqual(status, 200)
        self.assertIs(body["closed"], True)

    def test_invalid_parameters_are_400(self) -> None:
        for path in (
            "/audit/proof?generation=g1&bogus=1",     # unknown name
            "/audit/proof?generation=g1&generation=g2",  # repeated name
            "/audit/proof?generation=g1&cursor=",     # empty value
            "/audit/proof?generation=g1&cursor",      # missing value
            "/audit/proof?generation=g1&limit=0",
            "/audit/proof?generation=g1&limit=1001",
            "/audit/proof?generation=g1&limit=1.5",
            "/audit/proof?generation=g1&limit=+5",
            "/audit/proof?generation=g1&op=delete",
            "/audit/proof?generation=g1&stage=unknown",
            "/audit/proof?generation=g1&cursor=%FF%FE",  # not UTF-8
        ):
            with self.subTest(path=path):
                status, body = self._auth_get(path)
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})

    def test_client_cannot_select_audit_or_checkpoint_paths(self) -> None:
        self._record("a")
        for name in ("audit", "audit_path", "checkpoint",
                     "checkpoint_path", "path"):
            with self.subTest(name=name):
                status, body = self._auth_get(
                    f"/audit/proof?generation=g1&{name}=other.json")
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})

    # -- successful exports -------------------------------------------------

    def test_export_returns_proof_and_creates_checkpoint(self) -> None:
        self._record("a", key="hist-1")
        self._record("b", op="restore", key="hist-2")
        status, body = self._auth_get("/audit/proof?generation=g1")
        self.assertEqual(status, 200)
        self.assertEqual(list(body),
                         ["version", "generation", "params", "result",
                          "log_bytes", "log_digest", "head", "closed",
                          "anchor_digest"])
        self.assertEqual(body["generation"], "g1")
        self.assertIs(body["closed"], False)
        self.assertEqual(body["params"],
                         {"cursor": None, "limit": 100, "op": None,
                          "stage": None, "key": None})
        self.assertEqual([key for key, _ in body["result"]["events"]],
                         ["a", "b"])
        self.assertEqual(body["log_bytes"],
                         open(self.journal, encoding="utf-8").read())
        # The checkpoint was created and the proof verifies offline.
        self.assertEqual(len(self._anchors()), 1)
        result = audit_proof.verify(self.checkpoint, body)
        self.assertEqual(result["generation"], "g1")
        self.assertEqual([key for key, _ in result["events"]], ["a", "b"])

    def test_page_matches_audit_search_with_filters_and_cursor(self) -> None:
        self._record("a", key="hist-1")
        self._record("b", op="restore", key="hist-2")
        self._record("c", key="hist-1", error="OSError", stage="同步")
        status, body = self._auth_get("/audit/proof?generation=g1&limit=2")
        self.assertEqual(status, 200)
        self.assertEqual(body["result"],
                         audit.search(self.journal, limit=2))
        self.assertEqual(body["result"]["next"], "b")
        status, body = self._auth_get(
            "/audit/proof?generation=g1&limit=2&cursor=b&key=hist-1")
        self.assertEqual([key for key, _ in body["result"]["events"]], ["c"])
        self.assertIsNone(body["result"]["next"])
        self.assertEqual(body["params"]["cursor"], "b")
        self.assertEqual(body["params"]["key"], "hist-1")

    def test_final_closes_generation_and_replay_is_idempotent(self) -> None:
        self._record("a")
        status, body = self._auth_get("/audit/proof?generation=g1")
        self.assertEqual((status, body["closed"]), (200, False))
        status, body = self._auth_get("/audit/proof?generation=g1&final=true")
        self.assertEqual((status, body["closed"]), (200, True))
        self.assertEqual(len(self._anchors()), 2)
        before = open(self.checkpoint, "rb").read()
        # The identical idempotent export appends nothing.
        status, again = self._auth_get(
            "/audit/proof?generation=g1&final=true&limit=5")
        self.assertEqual(status, 200)
        self.assertEqual(again["anchor_digest"], body["anchor_digest"])
        self.assertEqual(open(self.checkpoint, "rb").read(), before)
        # A closed generation rejects anything but the identical replay.
        status, body = self._auth_get("/audit/proof?generation=g1")
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "proof_invalid"})
        self.assertEqual(open(self.checkpoint, "rb").read(), before)

    def test_concurrent_exports_observe_complete_checkpoints(self) -> None:
        for index in range(20):
            self._record(f"k{index}")
        results: list[tuple[int, dict]] = []

        def worker(index: int) -> None:
            results.append(self._auth_get(
                f"/audit/proof?generation=g1&limit={index + 1}"))

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual([status for status, _ in results], [200] * 8)
        # Every export pinned the same 20-event snapshot: one anchor.
        self.assertEqual(len(self._anchors()), 1)
        for _, body in results:
            audit_proof.verify(self.checkpoint, body)

    # -- journal and checkpoint state errors --------------------------------

    def test_missing_journal_is_404_without_leaking_path(self) -> None:
        status, body = self._auth_get("/audit/proof?generation=g1")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "proof_not_found"})
        self.assertNotIn(self.tmp.name, json.dumps(body))
        self.assertFalse(os.path.exists(self.checkpoint))

    def test_missing_checkpoint_parent_is_404_without_leaking_path(self) -> None:
        self._record("a")
        self.server.audit_checkpoint = os.path.join(
            self.tmp.name, "nodir", "checkpoint.json")
        status, body = self._auth_get("/audit/proof?generation=g1")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "proof_not_found"})
        self.assertNotIn("nodir", json.dumps(body))

    def test_malformed_or_unsealed_journal_is_409(self) -> None:
        with open(self.journal, "wb") as handle:
            handle.write(b"{not json")
        status, body = self._auth_get("/audit/proof?generation=g1")
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "proof_invalid"})
        self.assertFalse(os.path.exists(self.checkpoint))
        # A version 1 journal carries no chain to anchor.
        with open(self.journal, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(
                {"version": 1, "events": {"a": _event()}},
                separators=(",", ":")) + "\n")
        status, body = self._auth_get("/audit/proof?generation=g1")
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "proof_invalid"})

    def test_growth_violation_is_409_and_appends_no_anchor(self) -> None:
        self._record("a")
        self._record("b")
        self.assertEqual(
            self._auth_get("/audit/proof?generation=g1")[0], 200)
        before = open(self.checkpoint, "rb").read()
        # Delete event "b" from the journal: a retreat within the
        # generation must be rejected without touching the checkpoint.
        with open(self.journal, "wb") as handle:
            handle.write(_sealed({"a": _event()}))
        status, body = self._auth_get("/audit/proof?generation=g1")
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "proof_invalid"})
        self.assertEqual(open(self.checkpoint, "rb").read(), before)
        self.assertEqual(len(self._anchors()), 1)

    def test_commit_failure_is_503_and_preserves_old_checkpoint(self) -> None:
        self._record("a")
        self.assertEqual(
            self._auth_get("/audit/proof?generation=g1")[0], 200)
        before = open(self.checkpoint, "rb").read()
        self._record("b")

        def fail(_directory: str) -> None:
            raise OSError("injected directory fsync failure")

        with mock.patch.object(audit_proof, "_fsync_dir", fail):
            status, body = self._auth_get("/audit/proof?generation=g1")
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "proof_unavailable"})
        self.assertEqual(open(self.checkpoint, "rb").read(), before)
        self.assertEqual(len(self._anchors()), 1)
        # The failure is fully retryable.
        status, body = self._auth_get("/audit/proof?generation=g1")
        self.assertEqual(status, 200)
        self.assertEqual(len(self._anchors()), 2)

    def test_journal_io_failure_is_503(self) -> None:
        self.server.audit_path = self.tmp.name  # a directory: open fails
        status, body = self._auth_get("/audit/proof?generation=g1")
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "proof_unavailable"})

    # -- unchanged surface ----------------------------------------------------

    def test_health_audit_and_unknown_paths_unchanged(self) -> None:
        self.assertEqual(self._get("/health"), (200, {"status": "ok"}))
        self.assertEqual(self._get("/missing"), (404, {"error": "not_found"}))
        self.assertEqual(self._get("/audit/proof/"),
                         (404, {"error": "not_found"}))
        self._record("a")
        status, body = self._auth_get("/audit")
        self.assertEqual(status, 200)
        self.assertEqual([key for key, _ in body["events"]], ["a"])


class ProofDisabledTest(_HttpFixture):
    def test_proof_path_is_plain_404_without_checkpoint(self) -> None:
        # Audit is fully configured; only --checkpoint is missing.
        self.server.audit_path = self.journal
        self.server.audit_token = TOKEN
        status, body = self._auth_get("/audit/proof?generation=g1")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "not_found"})
        # The plain audit query keeps working.
        self._record("a")
        status, body = self._auth_get("/audit")
        self.assertEqual(status, 200)
        self.assertEqual([key for key, _ in body["events"]], ["a"])


class ProofAuthHttpTest(_HttpFixture):
    def setUp(self) -> None:
        super().setUp()
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        self._write_config(
            self._line("full", FULL_TOKEN, None) + "\n"
            + self._line("ops", OPS_TOKEN, None, ops=["copy"],
                         stages=["成功"], keys=["hist-1"]) + "\n")
        self.server.audit_path = self.journal
        self.server.audit_token = None
        self.server.audit_auth = self.config
        self.server.audit_checkpoint = self.checkpoint

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

    def test_scoped_token_must_filter_within_its_scope(self) -> None:
        self._record("a", key="hist-1")
        self._record("b", op="restore", key="hist-2")
        ok = ("/audit/proof?generation=g1&op=copy&key=hist-1&stage="
              "%E6%88%90%E5%8A%9F")  # 成功
        status, body = self._auth_get(ok, token=OPS_TOKEN)
        self.assertEqual(status, 200)
        self.assertEqual([key for key, _ in body["result"]["events"]], ["a"])
        for path in ("/audit/proof?generation=g1",
                     "/audit/proof?generation=g1&op=copy",
                     "/audit/proof?generation=g1&op=restore&key=hist-1"
                     "&stage=%E6%88%90%E5%8A%9F"):
            with self.subTest(path=path):
                status, body = self._auth_get(path, token=OPS_TOKEN)
                self.assertEqual(status, 403)
                self.assertEqual(body, {"error": "forbidden"})

    def test_scope_403_never_opens_files(self) -> None:
        # No journal exists: a scoped-out request still gets 403, not
        # 404, and no checkpoint is created.
        status, body = self._auth_get("/audit/proof?generation=g1",
                                      token=OPS_TOKEN)
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "forbidden"})
        self.assertFalse(os.path.exists(self.checkpoint))

    def test_parameter_validation_precedes_scope_check(self) -> None:
        status, body = self._auth_get("/audit/proof?limit=0", token=OPS_TOKEN)
        self.assertEqual(status, 400)
        self.assertEqual(body, {"error": "invalid_request"})

    def test_unknown_token_is_403(self) -> None:
        status, body = self._auth_get("/audit/proof?generation=g1",
                                      token="wrong-token")
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "forbidden"})

    def test_unreadable_or_invalid_config_is_503(self) -> None:
        os.unlink(self.config)
        status, body = self._auth_get("/audit/proof?generation=g1",
                                      token=FULL_TOKEN)
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "auth_unavailable"})
        self._write_config("{not json\n")
        status, body = self._auth_get("/audit/proof?generation=g1",
                                      token=FULL_TOKEN)
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "auth_unavailable"})


class ServeCheckpointArgumentsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "carbon_market", "serve", *args],
            capture_output=True, timeout=10)

    def test_checkpoint_pairing_errors_exit_2(self) -> None:
        for args in (
            ("--checkpoint", "cp.json"),                      # alone
            ("--token", "t", "--checkpoint", "cp.json"),      # no --audit
            ("--audit", "j", "--checkpoint", "cp.json"),      # no method
            ("--audit", "j", "--token", "t", "--checkpoint", ""),
            ("--audit", "j", "--token", "t",
             "--checkpoint", "a", "--checkpoint", "b"),       # repeated
        ):
            with self.subTest(args=args):
                self.assertEqual(self._run(*args).returncode, 2)

    def test_valid_checkpoint_starts_and_serves_proof_export(self) -> None:
        journal = os.path.join(self.tmp.name, "audit.json")
        checkpoint = os.path.join(self.tmp.name, "checkpoint.json")
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        process = subprocess.Popen(
            [sys.executable, "-m", "carbon_market", "serve",
             "--host", "127.0.0.1", "--port", str(port),
             "--audit", journal, "--token", TOKEN,
             "--checkpoint", checkpoint])
        try:
            deadline = time.time() + 10
            status = None
            while time.time() < deadline:
                try:
                    connection = HTTPConnection("127.0.0.1", port, timeout=1)
                    connection.request(
                        "GET", "/audit/proof?generation=g1",
                        headers={"X-Audit-Token": TOKEN})
                    response = connection.getresponse()
                    status = response.status
                    body = json.loads(response.read())
                    connection.close()
                    break
                except OSError:
                    time.sleep(0.05)
            # The journal does not exist yet, but the endpoint is live
            # and authorization worked.
            self.assertEqual(status, 404)
            self.assertEqual(body, {"error": "proof_not_found"})
        finally:
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
