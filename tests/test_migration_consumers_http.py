"""HTTP tests for the POST /migration-consumers endpoints.

Covers the claim/fetch/confirm endpoints enabled by
``--migration-consumers``: multi-token authorization (401/403/503),
fixed-field JSON request bodies, the batch-key scope rules (a fixed
batch subscription is authorized by that key, a cross-batch or
job-only subscription needs every scope unrestricted), the consumer
lifecycle and ownership mapping (400 invalid_request, 404
migration_consumer_not_found, 409 migration_consumer_conflict), the
stored-state mapping (404 for the fixed ledgers, 409 for invalid
ledgers) and the serve option gating.
"""

from __future__ import annotations

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

from carbon_market.server import Handler
from tests import test_migration_batch as _batch_tests
from tests.test_migration_batches_http import _line

FULL_TOKEN = "full-token"
KEY_TOKEN = "key-token"
OPS_TOKEN = "ops-token"
STAGE_TOKEN = "stage-token"


class MigrationConsumersHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fx = _batch_tests.MigrationBatchTest(
            "test_claimed_active_job_is_kept")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.coord = self.fx.paths["coord"]
        self.config = os.path.join(self.fx.tmp.name, "auth.jsonl")
        self.consumers = os.path.join(self.fx.tmp.name, "consumers.json")
        self._write_config(
            _line("full", FULL_TOKEN) + "\n"
            + _line("key", KEY_TOKEN, keys=["zzz"]) + "\n"
            + _line("ops", OPS_TOKEN, ops=["copy"]) + "\n"
            + _line("stage", STAGE_TOKEN, stages=["成功"]) + "\n")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.audit_path = os.path.join(self.fx.tmp.name, "audit.json")
        self.server.audit_token = None
        self.server.audit_auth = self.config
        self.server.migration_batches = self.coord
        self.server.migration_consumers = self.consumers
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

    def _populate(self) -> None:
        fx = self.fx
        fx._prepare_migrate()
        fx._run(key="aaa", owner="o1", now=30)
        fx._run(key="zzz", now=31)

    def _post(self, path: str, token=None, body=None, *, raw=None,
              content_type="application/json"):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        headers = {}
        if token is not None:
            headers["X-Audit-Token"] = token
        data: bytes | None
        if raw is not None:
            data = raw
        elif body is not None:
            data = json.dumps(body).encode("utf-8")
        else:
            data = b""
        if data:
            headers["Content-Type"] = content_type
        connection.request("POST", path, body=data, headers=headers)
        response = connection.getresponse()
        raw_body = response.read()
        self.addCleanup(connection.close)
        return response.status, raw_body

    def _json_post(self, path: str, token, body):
        status, raw_body = self._post(path, token, body)
        return status, json.loads(raw_body)

    def _claim(self, token=FULL_TOKEN, **body):
        payload = {"consumer": "c1", "owner": "o1", "now": 40,
                   "lease": 100}
        payload.update(body)
        return self._json_post("/migration-consumers/claim", token, payload)

    # -- authorization --------------------------------------------------------

    def test_missing_blank_and_duplicate_tokens_are_401(self) -> None:
        self._populate()
        self.assertEqual(self._json_post(
            "/migration-consumers/fetch", None,
            {"consumer": "c1", "owner": "o1", "now": 40})[0], 401)
        self.assertEqual(self._json_post(
            "/migration-consumers/fetch", "   ",
            {"consumer": "c1", "owner": "o1", "now": 40})[0], 401)
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        connection.putrequest("POST", "/migration-consumers/fetch")
        connection.putheader("X-Audit-Token", FULL_TOKEN)
        connection.putheader("X-Audit-Token", FULL_TOKEN)
        connection.endheaders()
        response = connection.getresponse()
        response.read()
        self.addCleanup(connection.close)
        self.assertEqual(response.status, 401)

    def test_unknown_token_is_403(self) -> None:
        self._populate()
        status, body = self._json_post(
            "/migration-consumers/fetch", "wrong",
            {"consumer": "c1", "owner": "o1", "now": 40})
        self.assertEqual((status, body), (403, {"error": "forbidden"}))

    def test_bad_auth_config_is_503(self) -> None:
        self._populate()
        self._write_config("{not json\n")
        status, body = self._json_post(
            "/migration-consumers/fetch", FULL_TOKEN,
            {"consumer": "c1", "owner": "o1", "now": 40})
        self.assertEqual((status, body),
                         (503, {"error": "auth_unavailable"}))

    def test_authorization_precedes_body_validation(self) -> None:
        self.assertEqual(self._json_post(
            "/migration-consumers/claim", None, {"bogus": True})[0], 401)
        self.assertEqual(self._json_post(
            "/migration-consumers/claim", "wrong", {"bogus": True})[0],
            403)

    # -- request bodies --------------------------------------------------------

    def test_invalid_bodies_are_400(self) -> None:
        self._populate()
        bad_bodies = [
            b"not json",
            b"[1,2]",
            b"",
            json.dumps({"consumer": "c1", "owner": "o1",
                        "now": 40}).encode(),          # missing lease
            json.dumps({"consumer": "c1", "owner": "o1",
                        "lease": 100}).encode(),        # missing now
            json.dumps({"consumer": "c1", "owner": "o1", "now": 40,
                        "lease": 100,
                        "consumers": "/tmp/x"}).encode(),  # path field
            json.dumps({"consumer": "c1", "owner": "o1", "now": -1,
                        "lease": 100}).encode(),
            json.dumps({"consumer": "c1", "owner": "o1", "now": True,
                        "lease": 100}).encode(),
            json.dumps({"consumer": "c1", "owner": "o1", "now": 40,
                        "lease": 0}).encode(),
            json.dumps({"consumer": "", "owner": "o1", "now": 40,
                        "lease": 100}).encode(),
            json.dumps({"consumer": "c1", "owner": 3, "now": 40,
                        "lease": 100}).encode(),
            json.dumps({"consumer": "c1", "owner": "o1", "now": 40,
                        "lease": 100,
                        "key": ""}).encode(),
            b'{"consumer":"c1","owner":"o1","now":40,"lease":100,'
            b'"now":41}',                                # duplicate member
            b'{"consumer":"c1","owner":"o1","now":NaN,'
            b'"lease":100}',                             # non-finite
        ]
        for raw_body in bad_bodies:
            with self.subTest(raw_body=raw_body):
                status, body = self._post(
                    "/migration-consumers/claim", FULL_TOKEN, raw=raw_body)
                self.assertEqual((status, json.loads(body)),
                                 (400, {"error": "invalid_request"}))

    def test_fetch_and_confirm_bodies_are_400_for_wrong_fields(self) -> None:
        self._populate()
        self._claim()
        status, _ = self._json_post(
            "/migration-consumers/fetch", FULL_TOKEN,
            {"consumer": "c1", "owner": "o1", "now": 40,
             "lease": 5})
        self.assertEqual(status, 400)
        status, _ = self._json_post(
            "/migration-consumers/fetch", FULL_TOKEN,
            {"consumer": "c1", "owner": "o1", "now": 40, "limit": 0})
        self.assertEqual(status, 400)
        status, _ = self._json_post(
            "/migration-consumers/fetch", FULL_TOKEN,
            {"consumer": "c1", "owner": "o1", "now": 40,
             "cursor": -1})
        self.assertEqual(status, 400)
        status, _ = self._json_post(
            "/migration-consumers/confirm", FULL_TOKEN,
            {"consumer": "c1", "owner": "o1", "now": 40})
        self.assertEqual(status, 400)
        status, _ = self._json_post(
            "/migration-consumers/confirm", FULL_TOKEN,
            {"consumer": "c1", "owner": "o1", "now": 40,
             "position": -1})
        self.assertEqual(status, 400)

    def test_query_string_is_rejected(self) -> None:
        self._populate()
        status, body = self._json_post(
            "/migration-consumers/claim?x=1", FULL_TOKEN,
            {"consumer": "c1", "owner": "o1", "now": 40, "lease": 100})
        self.assertEqual((status, body),
                         (400, {"error": "invalid_request"}))

    # -- scopes ----------------------------------------------------------------

    def test_scope_rules_for_claim(self) -> None:
        self._populate()
        # A fixed-batch subscription for zzz passes with a matching key
        # scope; anything else fails.
        self.assertEqual(self._claim(KEY_TOKEN, key="zzz")[0], 200)
        self.assertEqual(self._claim(KEY_TOKEN, consumer="c2",
                                     key="aaa")[0], 403)
        self.assertEqual(self._claim(KEY_TOKEN, consumer="c3")[0], 403)
        self.assertEqual(self._claim(KEY_TOKEN, consumer="c4",
                                     job="j-1")[0], 403)
        self.assertEqual(self._claim(OPS_TOKEN, consumer="c5")[0], 403)
        self.assertEqual(self._claim(STAGE_TOKEN, consumer="c6",
                                     key="zzz")[0], 403)

    def test_fetch_and_confirm_use_the_stored_subscription_scope(self) -> None:
        self._populate()
        self._claim(KEY_TOKEN, consumer="fixed", key="zzz")
        self._claim(FULL_TOKEN, consumer="wide")
        # The key-scoped token may drive its own fixed subscription...
        status, _ = self._json_post(
            "/migration-consumers/fetch", KEY_TOKEN,
            {"consumer": "fixed", "owner": "o1", "now": 40})
        self.assertEqual(status, 200)
        status, _ = self._json_post(
            "/migration-consumers/confirm", KEY_TOKEN,
            {"consumer": "fixed", "owner": "o1", "now": 40,
             "position": 4})
        self.assertEqual(status, 200)
        # ...but never the cross-batch one.
        status, _ = self._json_post(
            "/migration-consumers/fetch", KEY_TOKEN,
            {"consumer": "wide", "owner": "o1", "now": 40})
        self.assertEqual(status, 403)

    # -- lifecycle --------------------------------------------------------------

    def test_claim_fetch_confirm_lifecycle(self) -> None:
        self._populate()
        status, claimed = self._claim()
        self.assertEqual(status, 200)
        self.assertEqual(claimed["position"], -1)
        status, page = self._json_post(
            "/migration-consumers/fetch", FULL_TOKEN,
            {"consumer": "c1", "owner": "o1", "now": 41, "limit": 2})
        self.assertEqual(status, 200)
        self.assertEqual([event["position"] for event in page["events"]],
                         [0, 1])
        self.assertEqual(page["next"], 1)
        status, confirmed = self._json_post(
            "/migration-consumers/confirm", FULL_TOKEN,
            {"consumer": "c1", "owner": "o1", "now": 42, "position": 1})
        self.assertEqual((status, confirmed),
                         (200, {"consumer": "c1", "position": 1}))
        # The next fetch starts strictly after the checkpoint.
        status, page = self._json_post(
            "/migration-consumers/fetch", FULL_TOKEN,
            {"consumer": "c1", "owner": "o1", "now": 43})
        self.assertEqual(page["events"][0]["position"], 2)

    def test_unknown_consumer_is_404(self) -> None:
        self._populate()
        status, body = self._json_post(
            "/migration-consumers/fetch", FULL_TOKEN,
            {"consumer": "ghost", "owner": "o1", "now": 40})
        self.assertEqual((status, body),
                         (404, {"error": "migration_consumer_not_found"}))

    def test_ownership_conflict_and_takeover_are_409_then_200(self) -> None:
        self._populate()
        self._claim()
        status, body = self._claim(owner="o2", now=50)
        self.assertEqual((status, body),
                         (409, {"error": "migration_consumer_conflict"}))
        # Expired: the old owner gets 409, the new owner takes over.
        status, _ = self._json_post(
            "/migration-consumers/fetch", FULL_TOKEN,
            {"consumer": "c1", "owner": "o1", "now": 141})
        self.assertEqual(status, 409)
        status, result = self._claim(owner="o2", now=141, lease=100)
        self.assertEqual(status, 200)
        self.assertEqual(result["owner"], "o2")

    def test_bad_confirmations_are_400(self) -> None:
        self._populate()
        self._claim()
        for position in (999,):
            status, body = self._json_post(
                "/migration-consumers/confirm", FULL_TOKEN,
                {"consumer": "c1", "owner": "o1", "now": 40,
                 "position": position})
            self.assertEqual((status, body),
                             (400, {"error": "invalid_request"}))

    def test_stream_rollback_is_409(self) -> None:
        self._populate()
        self._claim()
        from carbon_market import migration_batch
        positions = [event["position"] for event in
                     migration_batch.events(self.coord, limit=1000)["events"]]
        self._json_post(
            "/migration-consumers/confirm", FULL_TOKEN,
            {"consumer": "c1", "owner": "o1", "now": 40,
             "position": positions[-1]})
        data = json.loads(Path(self.coord).read_text("utf-8"))
        data["events"][positions[-1]]["at"] = 999
        Path(self.coord).write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"))
            + "\n", encoding="utf-8")
        status, body = self._json_post(
            "/migration-consumers/fetch", FULL_TOKEN,
            {"consumer": "c1", "owner": "o1", "now": 50})
        self.assertEqual((status, body),
                         (409, {"error": "migration_consumer_conflict"}))

    def test_idempotent_claim_returns_same_result(self) -> None:
        self._populate()
        first_status, first = self._claim(idempotency_key="ik", now=40)
        second_status, second = self._claim(
            idempotency_key="ik", now=99, lease=5)
        self.assertEqual(first_status, 200)
        self.assertEqual((second_status, second), (200, first))

    # -- stored-state errors ---------------------------------------------------

    def test_missing_coordination_ledger_is_404(self) -> None:
        missing = os.path.join(self.fx.tmp.name, "absent.json")
        self.server.migration_batches = missing
        status, body = self._claim()
        self.assertEqual((status, body),
                         (404, {"error": "migration_batches_not_found"}))

    def test_missing_consumer_parent_is_404(self) -> None:
        self._populate()
        self.server.migration_consumers = os.path.join(
            self.fx.tmp.name, "no-such-dir", "consumers.json")
        status, body = self._claim()
        self.assertEqual((status, body),
                         (404, {"error": "migration_consumers_not_found"}))

    def test_invalid_consumer_ledger_is_409(self) -> None:
        self._populate()
        self._claim()
        with open(self.consumers, "wb") as handle:
            handle.write(b"{broken")
        status, body = self._json_post(
            "/migration-consumers/fetch", FULL_TOKEN,
            {"consumer": "c1", "owner": "o1", "now": 50})
        self.assertEqual((status, body),
                         (409, {"error": "migration_consumers_invalid"}))

    def test_invalid_coordination_ledger_is_409(self) -> None:
        self._populate()
        with open(self.coord, "wb") as handle:
            handle.write(b"{broken")
        status, body = self._claim()
        self.assertEqual((status, body),
                         (409, {"error": "migration_batches_invalid"}))

    # -- routing ----------------------------------------------------------------

    def test_endpoints_are_plain_404_when_unconfigured(self) -> None:
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            for path in ("/migration-consumers/claim",
                         "/migration-consumers/fetch",
                         "/migration-consumers/confirm",
                         "/migration-consumers",
                         "/migration-consumers/other"):
                connection = HTTPConnection("127.0.0.1", server.server_port,
                                            timeout=2)
                connection.request("POST", path, body=b"{}",
                                   headers={"X-Audit-Token": "x"})
                response = connection.getresponse()
                self.assertEqual(response.status, 404, path)
                self.assertEqual(json.loads(response.read()),
                                 {"error": "not_found"})
                connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_get_endpoints_and_other_posts_are_unchanged(self) -> None:
        self._populate()
        status, _ = self._post("/migration-batches", FULL_TOKEN,
                               body=None)
        self.assertEqual(status, 404)  # GET-only path has no POST handler
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        connection.request("GET", "/migration-batches",
                           headers={"X-Audit-Token": FULL_TOKEN})
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        response.read()
        connection.close()


class ServeMigrationConsumersArgumentsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        with open(self.config, "w", encoding="utf-8") as handle:
            handle.write(_line("full", FULL_TOKEN) + "\n")
        # Provision a real (empty-completed) migration batch so the
        # coordination ledger exists and the new endpoints are fully
        # routable in the subprocess test below.
        self.fx = _batch_tests.MigrationBatchTest(
            "test_claimed_active_job_is_kept")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.fx._prepare_migrate()
        self.fx._run(key="only", now=30)
        self.coord = self.fx.paths["coord"]
        self.consumers = os.path.join(self.tmp.name, "consumers.json")

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "carbon_market", "serve", *args],
            capture_output=True, timeout=10)

    def test_misplaced_empty_or_repeated_option_exits_2(self) -> None:
        for args in (
            ("--audit", "j", "--auth", self.config,
             "--migration-consumers", self.consumers),  # no batches
            ("--audit", "j", "--token", "t",
             "--migration-batches", self.coord,
             "--migration-consumers", self.consumers),  # single token
            ("--audit", "j", "--auth", self.config,
             "--migration-batches", self.coord,
             "--migration-consumers", ""),               # empty
            ("--audit", "j", "--auth", self.config,
             "--migration-batches", self.coord,
             "--migration-consumers", self.consumers,
             "--migration-consumers", self.consumers),   # repeated
        ):
            with self.subTest(args=args):
                self.assertEqual(self._run(*args).returncode, 2)

    def test_valid_option_starts_and_serves(self) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        process = subprocess.Popen(
            [sys.executable, "-m", "carbon_market", "serve",
             "--host", "127.0.0.1", "--port", str(port),
             "--audit", os.path.join(self.tmp.name, "audit.json"),
             "--auth", self.config,
             "--migration-batches", self.coord,
             "--migration-consumers", self.consumers],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.time() + 10
            status = None
            while time.time() < deadline:
                try:
                    connection = HTTPConnection("127.0.0.1", port,
                                                timeout=1)
                    connection.request(
                        "POST", "/migration-consumers/claim",
                        body=json.dumps({"consumer": "c1", "owner": "o1",
                                         "now": 1, "lease": 100}),
                        headers={"X-Audit-Token": FULL_TOKEN,
                                 "Content-Type": "application/json"})
                    response = connection.getresponse()
                    status = response.status
                    body = json.loads(response.read())
                    connection.close()
                    break
                except OSError:
                    time.sleep(0.05)
            self.assertEqual(status, 200)
            self.assertEqual(body["consumer"], "c1")
        finally:
            process.terminate()
            process.wait(timeout=10)
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()


if __name__ == "__main__":
    unittest.main()
