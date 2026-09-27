"""HTTP tests for the persistent migration consumer endpoints.

Covers POST /migration-consumers/{claim,pull,ack}: the fixed-field
JSON bodies and 400 invalid_request, the shared audit authorization
(401/403/503), the fixed-batch vs cross-batch/job-only scope rules,
the happy claim/pull/ack/takeover flow and its compact wire bodies,
the error mapping (404 migration_consumers_not_found /
migration_consumer_not_found, 409 migration_consumers_invalid /
migration_consumer_ownership / migration_consumer_checkpoint /
migration_batches_invalid, 503 migration_consumers_unavailable), the
plain 404 for unconfigured or unknown paths, and the
``--migration-consumers`` serve option (only with --migration-batches
and --auth).
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


class MigrationConsumersHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fx = MigrationBatchTest(
            "test_get_returns_copy_and_unknown_key_raises")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.fx._prepare_migrate()
        self.coord = self.fx.paths["coord"]
        self.consumers = os.path.join(self.fx.tmp.name, "consumers.json")
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
        self.server.migration_consumers = self.consumers
        thread = threading.Thread(target=self.server.serve_forever,
                                  daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)
        self._populate()

    def _write_config(self, text: str) -> None:
        tmp_path = self.config + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp_path, self.config)

    def _populate(self) -> None:
        fx = self.fx
        fx._run(key="aaa", owner="负责人-1", now=30)
        fx._run(key="zzz", now=31)
        fx._run(key="aaa", owner="负责人-1", now=32,
                receipts={"j-1": fx._receipt(
                    "copy", "succeeded", "复制完成", 33)})
        fx._run(key="aaa", owner="负责人-1", now=34,
                receipts={"j-1": fx._receipt(
                    "switch", "succeeded", "切换完成", 35)})

    def _post(self, operation, body=None, token=FULL_TOKEN, raw=None,
              query="", headers=None):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        all_headers = {"Content-Type": "application/json"}
        if token is not None:
            all_headers["X-Audit-Token"] = token
        if headers:
            all_headers.update(headers)
        data = raw if raw is not None else json.dumps(body or {}).encode()
        connection.request(
            "POST", f"/migration-consumers/{operation}{query}", data,
            all_headers)
        response = connection.getresponse()
        raw_body = response.read()
        connection.close()
        try:
            return response.status, json.loads(raw_body), raw_body
        except ValueError:
            return response.status, None, raw_body

    def _claim(self, consumer="c1", owner="o1", now=40, lease=100,
               idem="k1", token=FULL_TOKEN, **filters):
        body = {"consumer": consumer, "owner": owner, "now": now,
                "lease": lease, "idem": idem, **filters}
        return self._post("claim", body, token=token)

    # -- authorization --------------------------------------------------------

    def test_identity_is_checked_first(self) -> None:
        status, body, _ = self._post("claim", {"bogus": 1}, token=None)
        self.assertEqual((status, body), (401, {"error": "unauthorized"}))
        status, body, _ = self._post("claim", {"bogus": 1}, token="wrong")
        self.assertEqual((status, body), (403, {"error": "forbidden"}))

    def test_duplicate_and_blank_tokens(self) -> None:
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        connection.putrequest("POST", "/migration-consumers/claim")
        connection.putheader("X-Audit-Token", FULL_TOKEN)
        connection.putheader("X-Audit-Token", FULL_TOKEN)
        connection.endheaders()
        response = connection.getresponse()
        self.assertEqual(response.status, 401)
        self.assertEqual(json.loads(response.read()),
                         {"error": "unauthorized"})
        connection.close()

    def test_auth_unavailable_is_503(self) -> None:
        os.unlink(self.config)
        status, body, _ = self._claim()
        self.assertEqual((status, body),
                         (503, {"error": "auth_unavailable"}))

    # -- request validation ---------------------------------------------------

    def test_invalid_bodies_are_400(self) -> None:
        good = {"consumer": "c", "owner": "o", "now": 1, "lease": 2,
                "idem": "i"}
        cases = [
            ("claim", b"{not json"),
            ("claim", b'{"consumer":"a","consumer":"b","owner":"o",'
                       b'"now":1,"lease":2,"idem":"i"}'),
            ("claim", None),  # replaced per case below
        ]
        for operation, raw in cases:
            if raw is None:
                continue
            with self.subTest(raw=raw):
                status, body, _ = self._post(operation, raw=raw)
                self.assertEqual((status, body),
                                 (400, {"error": "invalid_request"}))
        for bad_body in (
            {**good, "extra": 1},          # unknown field
            {**good, "consumer": ""},      # empty consumer
            {**good, "consumer": 5},       # wrong type
            {**good, "consumer": True},    # boolean
            {**good, "owner": ""},
            {**good, "now": -1},
            {**good, "now": True},
            {**good, "now": 1.5},
            {**good, "lease": 0},
            {**good, "lease": True},
            {**good, "idem": ""},
            {**good, "key": ""},
            {**good, "job_id": ""},
        ):
            with self.subTest(bad=bad_body):
                status, body, _ = self._post("claim", bad_body)
                self.assertEqual((status, body),
                                 (400, {"error": "invalid_request"}))
        # pull and ack field sets
        status, _, _ = self._post(
            "pull", {"consumer": "c", "owner": "o", "now": 1, "idem": "i"})
        self.assertEqual(status, 400)
        status, _, _ = self._post(
            "pull", {"consumer": "c", "owner": "o", "now": 1, "limit": 0})
        self.assertEqual(status, 400)
        status, _, _ = self._post(
            "pull", {"consumer": "c", "owner": "o", "now": 1, "key": "a"})
        self.assertEqual(status, 400)
        status, _, _ = self._post(
            "ack", {"consumer": "c", "owner": "o", "now": 1, "idem": "i"})
        self.assertEqual(status, 400)
        status, _, _ = self._post(
            "ack", {"consumer": "c", "owner": "o", "now": 1,
                    "position": -1, "idem": "i"})
        self.assertEqual(status, 400)
        status, _, _ = self._post(
            "ack", {"consumer": "c", "owner": "o", "now": 1,
                    "position": True, "idem": "i"})
        self.assertEqual(status, 400)

    def test_non_finite_literal_is_400(self) -> None:
        status, body, _ = self._post(
            "claim", raw=b'{"consumer":"c","owner":"o","now":NaN,'
                         b'"lease":2,"idem":"i"}')
        self.assertEqual((status, body),
                         (400, {"error": "invalid_request"}))

    def test_query_string_is_400(self) -> None:
        status, body, _ = self._post(
            "claim", {"consumer": "c", "owner": "o", "now": 1, "lease": 2,
                      "idem": "i"}, query="?x=1")
        self.assertEqual((status, body),
                         (400, {"error": "invalid_request"}))

    def test_client_cannot_select_a_path(self) -> None:
        # Neither the coordination nor the consumer ledger path is a
        # request field; an extra path-ish field is a 400.
        status, _, _ = self._post(
            "claim", {"consumer": "c", "owner": "o", "now": 1, "lease": 2,
                      "idem": "i", "ledger": "/etc/passwd"})
        self.assertEqual(status, 400)

    # -- scopes ---------------------------------------------------------------

    def test_claim_scope_rules(self) -> None:
        # Ops/stage-scoped tokens can never claim.
        self.assertEqual(self._claim(token=OPS_TOKEN)[0], 403)
        self.assertEqual(self._claim(token=STAGE_TOKEN)[0], 403)
        # A key-scoped token may claim its one batch...
        self.assertEqual(
            self._claim(consumer="ka", idem="ka", key="aaa",
                        token=KEY_TOKEN)[0], 200)
        # ...but not a cross-batch or job-only subscription, even when
        # the job happens to live inside an allowed batch.
        self.assertEqual(self._claim(consumer="kb", idem="kb",
                                     token=KEY_TOKEN)[0], 403)
        self.assertEqual(
            self._claim(consumer="kc", idem="kc", job_id="j-1",
                        token=KEY_TOKEN)[0], 403)
        # A mismatching fixed key is forbidden too.
        self.assertEqual(
            self._claim(consumer="kd", idem="kd", key="zzz",
                        token=KEY_TOKEN)[0], 403)

    def test_pull_and_ack_scope_uses_persisted_subscription(self) -> None:
        # Fixed-key subscription, claimed with the full token, is then
        # readable through the matching key-scoped token.
        self._claim(consumer="fixed", idem="f1", key="aaa")
        status, _, _ = self._post(
            "pull", {"consumer": "fixed", "owner": "o1", "now": 40},
            token=KEY_TOKEN)
        self.assertEqual(status, 200)
        # A cross-batch subscription stays forbidden to that token even
        # though the request body never names a key.
        self._claim(consumer="wide", idem="w1")
        status, body, _ = self._post(
            "pull", {"consumer": "wide", "owner": "o1", "now": 40},
            token=KEY_TOKEN)
        self.assertEqual((status, body), (403, {"error": "forbidden"}))
        # An ops-scoped token is rejected before the subscription is even
        # consulted (unknown consumer still answers 403, not 404).
        status, _, _ = self._post(
            "pull", {"consumer": "ghost", "owner": "o", "now": 1},
            token=OPS_TOKEN)
        self.assertEqual(status, 403)

    # -- happy path -----------------------------------------------------------

    def test_claim_pull_ack_takeover_flow(self) -> None:
        status, claim, _ = self._claim()
        self.assertEqual(status, 200)
        self.assertEqual(list(claim),
                         ["consumer", "key", "job_id", "owner", "until",
                          "position", "taken_over"])
        self.assertIsNone(claim["position"])

        status, page, raw = self._post(
            "pull", {"consumer": "c1", "owner": "o1", "now": 40, "limit": 2})
        self.assertEqual(status, 200)
        self.assertEqual(list(page),
                         ["consumer", "owner", "until", "position",
                          "events", "next"])
        self.assertEqual([e["position"] for e in page["events"]], [0, 1])
        self.assertEqual(page["next"], 1)
        self.assertFalse(raw.endswith(b"\n"))
        self.assertIn("负责人-1".encode("utf-8"), raw)
        self.assertNotIn(b"\\u", raw)
        # A full page carries the non-ASCII step receipt text verbatim.
        _, full_page, full_raw = self._post(
            "pull", {"consumer": "c1", "owner": "o1", "now": 40, "limit": 1000})
        self.assertTrue(full_page["events"])
        self.assertIn("复制完成".encode("utf-8"), full_raw)
        # The wire body is exactly the lock-holding serializer's.
        self.assertEqual(
            raw, migration_batch.consume_response(
                self.coord, self.consumers, "pull", "c1", "o1", 40,
                limit=2))

        # A pull never advances the checkpoint.
        _, again, _ = self._post(
            "pull", {"consumer": "c1", "owner": "o1", "now": 40, "limit": 2})
        self.assertEqual([e["position"] for e in again["events"]], [0, 1])

        # Acknowledge position 0; the next pull starts at 1.
        status, ack, _ = self._post(
            "ack", {"consumer": "c1", "owner": "o1", "now": 40,
                    "position": 0, "idem": "a0"})
        self.assertEqual(status, 200)
        self.assertEqual(list(ack), ["consumer", "owner", "until",
                                      "position"])
        status, page, _ = self._post(
            "pull", {"consumer": "c1", "owner": "o1", "now": 40, "limit": 2})
        self.assertEqual([e["position"] for e in page["events"]], [1, 2])

        # The same idempotency key replays the original answer.
        status, replay, _ = self._post(
            "ack", {"consumer": "c1", "owner": "o1", "now": 40,
                    "position": 0, "idem": "a0"})
        self.assertEqual(status, 200)
        self.assertEqual(replay, ack)

        # Another owner during the lease is a conflict.
        status, body, _ = self._post(
            "pull", {"consumer": "c1", "owner": "o2", "now": 40})
        self.assertEqual((status, body),
                         (409, {"error": "migration_consumer_ownership"}))
        # After strict expiry the old owner conflicts too; a takeover
        # claim then succeeds and keeps the acknowledged position.
        self.assertEqual(
            self._post("pull", {"consumer": "c1", "owner": "o1",
                                "now": 141})[0], 409)
        status, takeover, _ = self._claim(
            owner="o2", now=141, lease=100, idem="k2")
        self.assertEqual(status, 200)
        self.assertTrue(takeover["taken_over"])
        self.assertEqual(takeover["position"], 0)
        status, page, _ = self._post(
            "pull", {"consumer": "c1", "owner": "o2", "now": 141, "limit": 2})
        self.assertEqual([e["position"] for e in page["events"]], [1, 2])

    def test_bad_ack_is_400_or_conflict(self) -> None:
        self._claim()
        total = len(migration_batch.events(self.coord, limit=1000)["events"])
        status, body, _ = self._post(
            "ack", {"consumer": "c1", "owner": "o1", "now": 40,
                    "position": total + 10, "idem": "a"})
        self.assertEqual((status, body),
                         (400, {"error": "invalid_request"}))
        # Changed request under a used idempotency key.
        self._post("ack", {"consumer": "c1", "owner": "o1", "now": 40,
                           "position": 0, "idem": "a0"})
        status, _, _ = self._post(
            "ack", {"consumer": "c1", "owner": "o1", "now": 40,
                    "position": 1, "idem": "a0"})
        self.assertEqual(status, 400)

    def test_checkpoint_regression_is_409(self) -> None:
        total = len(migration_batch.events(self.coord, limit=1000)["events"])
        self._claim()
        self._post("ack", {"consumer": "c1", "owner": "o1", "now": 40,
                           "position": total - 1, "idem": "a-last"})
        # Rebind the consumer ledger to a shorter canonical coordination
        # stream: the confirmed position no longer exists, so no reset.
        other = MigrationBatchTest(
            "test_get_returns_copy_and_unknown_key_raises")
        other.setUp()
        self.addCleanup(other.doCleanups)
        other._prepare_migrate()
        other._run(key="aaa", owner="o1", now=30)
        doc = json.loads(Path(self.consumers).read_text("utf-8"))
        doc["coordination"] = os.path.realpath(other.paths["coord"])
        other_cons = os.path.join(other.tmp.name, "consumers.json")
        Path(other_cons).write_text(
            json.dumps(doc, ensure_ascii=False,
                       separators=(",", ":")) + "\n",
            encoding="utf-8")
        self.server.migration_batches = other.paths["coord"]
        self.server.migration_consumers = other_cons
        status, body, _ = self._post(
            "pull", {"consumer": "c1", "owner": "o1", "now": 40})
        self.assertEqual((status, body),
                         (409, {"error": "migration_consumer_checkpoint"}))

    # -- ledger state errors --------------------------------------------------

    def test_unknown_consumer_is_404(self) -> None:
        status, body, _ = self._post(
            "pull", {"consumer": "ghost", "owner": "o", "now": 1})
        self.assertEqual((status, body),
                         (404, {"error": "migration_consumer_not_found"}))
        status, _, _ = self._post(
            "ack", {"consumer": "ghost", "owner": "o", "now": 1,
                    "position": 0, "idem": "i"})
        self.assertEqual(status, 404)

    def test_missing_coordination_is_404(self) -> None:
        self.server.migration_batches = os.path.join(
            self.fx.tmp.name, "absent.json")
        status, body, _ = self._claim()
        self.assertEqual((status, body),
                         (404, {"error": "migration_batches_not_found"}))

    def test_missing_consumers_parent_is_404(self) -> None:
        self.server.migration_consumers = os.path.join(
            self.fx.tmp.name, "no-dir", "consumers.json")
        status, body, _ = self._claim()
        self.assertEqual((status, body),
                         (404, {"error": "migration_consumers_not_found"}))

    def test_invalid_consumers_ledger_is_409(self) -> None:
        self._claim()
        with open(self.consumers, "wb") as handle:
            handle.write(b"{broken\n")
        status, body, _ = self._post(
            "pull", {"consumer": "c1", "owner": "o1", "now": 40})
        self.assertEqual((status, body),
                         (409, {"error": "migration_consumers_invalid"}))

    def test_invalid_coordination_ledger_is_409(self) -> None:
        with open(self.coord, "wb") as handle:
            handle.write(b"{broken\n")
        status, body, _ = self._claim()
        self.assertEqual((status, body),
                         (409, {"error": "migration_batches_invalid"}))

    def test_other_io_failure_is_503(self) -> None:
        directory = os.path.join(self.fx.tmp.name, "consumers-dir")
        os.mkdir(directory)
        self.server.migration_consumers = directory
        status, body, _ = self._claim()
        self.assertEqual((status, body),
                         (503, {"error": "migration_consumers_unavailable"}))

    def test_error_bodies_carry_only_error(self) -> None:
        for status, body, _ in (
            self._post("pull", {"consumer": "ghost", "owner": "o",
                                "now": 1}),
            self._post("pull", {"consumer": "c1", "owner": "o2",
                                "now": 40}),
        ):
            self.assertEqual(set(body), {"error"})

    # -- unconfigured and unknown paths --------------------------------------

    def test_get_method_and_unknown_paths_are_plain_404(self) -> None:
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        connection.request("GET", "/migration-consumers/claim",
                           headers={"X-Audit-Token": FULL_TOKEN})
        response = connection.getresponse()
        self.assertEqual(response.status, 404)
        self.assertEqual(json.loads(response.read()),
                         {"error": "not_found"})
        connection.close()
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        connection.request("POST", "/migration-consumers/nope", b"{}",
                           headers={"X-Audit-Token": FULL_TOKEN})
        response = connection.getresponse()
        self.assertEqual(response.status, 404)
        self.assertEqual(json.loads(response.read()),
                         {"error": "not_found"})
        connection.close()


class MigrationConsumersDisabledTest(unittest.TestCase):
    def _server(self, **attrs):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        for name, value in attrs.items():
            setattr(server, name, value)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        return server, thread

    def _post_status(self, server, path="/migration-consumers/claim"):
        connection = HTTPConnection("127.0.0.1", server.server_port,
                                    timeout=2)
        connection.request("POST", path, b"{}",
                           headers={"X-Audit-Token": "anything"})
        response = connection.getresponse()
        body = response.read()
        connection.close()
        return response.status, json.loads(body)

    def test_unconfigured_is_plain_404(self) -> None:
        server, thread = self._server()
        try:
            self.assertEqual(self._post_status(server),
                             (404, {"error": "not_found"}))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_without_consumers_option_is_plain_404(self) -> None:
        server, thread = self._server(migration_batches="/tmp/whatever")
        try:
            self.assertEqual(self._post_status(server),
                             (404, {"error": "not_found"}))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


class ServeMigrationConsumersArgumentsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        with open(self.config, "w", encoding="utf-8") as handle:
            handle.write(_line("full", FULL_TOKEN) + "\n")
        self.coord = os.path.join(self.tmp.name, "coord.json")
        self.consumers = os.path.join(self.tmp.name, "consumers.json")

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "carbon_market", "serve", *args],
            capture_output=True, timeout=10)

    def test_misplaced_empty_or_repeated_option_exits_2(self) -> None:
        for args in (
            ("--migration-consumers", self.consumers),
            ("--audit", "j", "--auth", self.config,
             "--migration-consumers", self.consumers),
            ("--audit", "j", "--token", "t",
             "--migration-batches", self.coord,
             "--migration-consumers", self.consumers),
            ("--audit", "j", "--auth", self.config,
             "--migration-batches", self.coord,
             "--migration-consumers", ""),
            ("--audit", "j", "--auth", self.config,
             "--migration-batches", self.coord,
             "--migration-consumers", self.consumers,
             "--migration-consumers", self.consumers),
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
             "--migration-consumers", self.consumers])
        try:
            deadline = time.time() + 10
            seen = []
            while time.time() < deadline:
                try:
                    connection = HTTPConnection("127.0.0.1", port, timeout=1)
                    body = json.dumps({
                        "consumer": "c", "owner": "o", "now": 1,
                        "lease": 2, "idem": "i"})
                    connection.request(
                        "POST", "/migration-consumers/claim", body,
                        {"X-Audit-Token": FULL_TOKEN,
                         "Content-Type": "application/json"})
                    response = connection.getresponse()
                    seen.append((response.status,
                                 json.loads(response.read())))
                    connection.close()
                    break
                except OSError:
                    time.sleep(0.05)
            # The coordination ledger does not exist yet: 404.
            self.assertEqual(seen,
                             [(404, {"error":
                                     "migration_batches_not_found"})])
        finally:
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
