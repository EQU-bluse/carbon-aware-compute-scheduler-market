"""HTTP tests for the persistent migration consumer endpoints.

Covers POST /migration-consumers/{claim,pull,ack}: the fixed-field
JSON bodies and 400 invalid_request, the shared audit authorization
(401/403/503), the fixed-batch vs cross-batch/job-only scope rules,
the happy claim/pull/ack/takeover flow and its compact wire bodies,
the error mapping (404 migration_consumers_not_found /
migration_consumer_not_found, 409 migration_consumers_invalid /
migration_consumer_ownership / migration_consumer_checkpoint /
migration_batches_invalid, 503 migration_consumers_unavailable), the
read-only GET /migration-consumers/status query and its parameter,
scope and error mapping, the plain 404 for unconfigured or unknown
paths, and the ``--migration-consumers`` serve option (only with
--migration-batches and --auth).
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

    def _ack(self, position, consumer="c1", owner="o1", now=40, idem="a",
             token=FULL_TOKEN):
        return self._post(
            "ack", {"consumer": consumer, "owner": owner, "now": now,
                    "position": position, "idem": idem}, token=token)

    def _reject(self, position, reason="bad", consumer="c1", owner="o1",
                now=40, idem="r", token=FULL_TOKEN):
        return self._post(
            "reject", {"consumer": consumer, "owner": owner, "now": now,
                       "position": position, "reason": reason, "idem": idem},
            token=token)

    def _get_status(self, query, token=FULL_TOKEN, raw_path=None):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        headers = {}
        if token is not None:
            headers["X-Audit-Token"] = token
        path = (raw_path if raw_path is not None
                else f"/migration-consumers/status?{query}")
        connection.request("GET", path, headers=headers)
        response = connection.getresponse()
        raw_body = response.read()
        connection.close()
        try:
            return response.status, json.loads(raw_body), raw_body
        except ValueError:
            return response.status, None, raw_body

    def _get_dead_letters(self, query, token=FULL_TOKEN, raw_path=None):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        headers = {}
        if token is not None:
            headers["X-Audit-Token"] = token
        path = (raw_path if raw_path is not None
                else f"/migration-consumers/dead-letters?{query}")
        connection.request("GET", path, headers=headers)
        response = connection.getresponse()
        raw_body = response.read()
        connection.close()
        try:
            return response.status, json.loads(raw_body), raw_body
        except ValueError:
            return response.status, None, raw_body

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

    # -- read-only status -----------------------------------------------------

    def test_status_returns_lease_state_and_backlog(self) -> None:
        self._claim(consumer="fixed", idem="f1", key="aaa")
        aaa = [event for event
               in migration_batch.events(self.coord, limit=1000)["events"]
               if event["key"] == "aaa"]
        status, body, raw = self._get_status("consumer=fixed&now=40")
        self.assertEqual(status, 200)
        self.assertEqual(list(body),
                         ["consumer", "key", "job_id", "owner", "until",
                          "lease", "position", "pending", "oldest"])
        self.assertEqual(body["consumer"], "fixed")
        self.assertEqual(body["key"], "aaa")
        self.assertIsNone(body["job_id"])
        self.assertEqual(body["owner"], "o1")
        self.assertEqual(body["until"], 140)
        self.assertEqual(body["lease"], "active")
        self.assertIsNone(body["position"])
        self.assertEqual(body["pending"], len(aaa))
        self.assertEqual(body["oldest"]["position"], aaa[0]["position"])
        self.assertEqual(body["oldest"], aaa[0])
        self.assertFalse(raw.endswith(b"\n"))
        # Compact UTF-8 with non-ASCII written straight through; the
        # oldest event's batch snapshot carries the non-ASCII owner.
        self.assertNotIn(b"\\u", raw)
        self.assertIn("负责人-1".encode("utf-8"), raw)
        # The wire bytes are exactly the lock-holding serializer's.
        self.assertEqual(
            raw, migration_batch.consumer_status_response(
                self.coord, self.consumers, "fixed", 40))

    def test_status_lease_boundary_and_caught_up_cursor(self) -> None:
        total = len(migration_batch.events(self.coord, limit=1000)["events"])
        self._claim()
        self.assertEqual(
            self._get_status("consumer=c1&now=140")[1]["lease"], "active")
        self.assertEqual(
            self._get_status("consumer=c1&now=141")[1]["lease"], "expired")
        self._post("ack", {"consumer": "c1", "owner": "o1", "now": 40,
                           "position": total - 1, "idem": "a-last"})
        status, body, _ = self._get_status("consumer=c1&now=40")
        self.assertEqual(status, 200)
        self.assertEqual(body["position"], total - 1)
        self.assertEqual(body["pending"], 0)
        self.assertIsNone(body["oldest"])

    def test_status_bad_queries_are_400(self) -> None:
        self._claim()
        for query in (
            "",
            "consumer=c1",
            "now=40",
            "consumer=c1&now=40&now=41",
            "consumer=c1&consumer=c2&now=40",
            "consumer=&now=40",
            "consumer=c1&now=",
            "consumer=c1&now=-1",
            "consumer=c1&now=1.5",
            "consumer=c1&now=0x10",
            "consumer=c1&now=1e1",
            "consumer=c1&now=true",
            "consumer=c1&now=40&extra=1",
            "CONSUMER=c1&now=40",
        ):
            with self.subTest(query=query):
                status, body, _ = self._get_status(query)
                self.assertEqual((status, body),
                                 (400, {"error": "invalid_request"}))

    def test_status_identity_is_checked_first(self) -> None:
        status, body, _ = self._get_status("nonsense", token=None)
        self.assertEqual((status, body), (401, {"error": "unauthorized"}))
        status, body, _ = self._get_status("nonsense", token="wrong")
        self.assertEqual((status, body), (403, {"error": "forbidden"}))

    def test_status_unknown_consumer_is_404(self) -> None:
        status, body, _ = self._get_status("consumer=ghost&now=40")
        self.assertEqual((status, body),
                         (404, {"error": "migration_consumer_not_found"}))

    def test_status_scope_uses_persisted_subscription(self) -> None:
        # Fixed-key subscription readable through the key-scoped token.
        self._claim(consumer="fixed", idem="f1", key="aaa")
        self.assertEqual(
            self._get_status("consumer=fixed&now=40",
                             token=KEY_TOKEN)[0], 200)
        self.assertEqual(
            self._get_status("consumer=fixed&now=40&now=41",
                             token=KEY_TOKEN)[0], 400)
        # A cross-batch and a job-only subscription stay forbidden.
        self._claim(consumer="wide", idem="w1")
        self._claim(consumer="jobonly", idem="j1", job_id="j-1")
        for consumer in ("wide", "jobonly"):
            status, body, _ = self._get_status(
                f"consumer={consumer}&now=40", token=KEY_TOKEN)
            self.assertEqual((status, body),
                             (403, {"error": "forbidden"}))
        # Ops- and stage-scoped tokens are rejected before the consumer
        # is even looked up; an unknown consumer still answers 403.
        for token in (OPS_TOKEN, STAGE_TOKEN):
            self.assertEqual(
                self._get_status("consumer=ghost&now=40",
                                 token=token)[0], 403)
        # The key-scoped token reads the subscription to decide scope,
        # so an unknown consumer is 404, not 403.
        self.assertEqual(
            self._get_status("consumer=ghost&now=40",
                             token=KEY_TOKEN)[0], 404)

    def test_status_checkpoint_regression_is_409(self) -> None:
        total = len(migration_batch.events(self.coord, limit=1000)["events"])
        self._claim()
        self._post("ack", {"consumer": "c1", "owner": "o1", "now": 40,
                           "position": total - 1, "idem": "a-last"})
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
        status, body, _ = self._get_status("consumer=c1&now=40")
        self.assertEqual((status, body),
                         (409, {"error": "migration_consumer_checkpoint"}))

    def test_status_ledger_errors(self) -> None:
        from tests.test_migration_batch import MigrationBatchTest as _Fx
        # A malformed consumer ledger is 409.
        self._claim()
        with open(self.consumers, "wb") as handle:
            handle.write(b"{broken\n")
        status, body, _ = self._get_status("consumer=c1&now=40")
        self.assertEqual((status, body),
                         (409, {"error": "migration_consumers_invalid"}))
        # A malformed bound coordination ledger is 409.
        fx = _Fx("test_get_returns_copy_and_unknown_key_raises")
        fx.setUp()
        self.addCleanup(fx.doCleanups)
        fx._prepare_migrate()
        fx._run(key="aaa", owner="o1", now=30)
        cons = os.path.join(fx.tmp.name, "consumers.json")
        migration_batch.consume(fx.paths["coord"], cons, "claim",
                                "c1", "o1", 40, lease=100, idem="k1")
        self.server.migration_batches = fx.paths["coord"]
        self.server.migration_consumers = cons
        with open(fx.paths["coord"], "wb") as handle:
            handle.write(b"{broken\n")
        status, body, _ = self._get_status("consumer=c1&now=40")
        self.assertEqual((status, body),
                         (409, {"error": "migration_batches_invalid"}))
        # A bound coordination ledger that has vanished is 404.
        fx2 = _Fx("test_get_returns_copy_and_unknown_key_raises")
        fx2.setUp()
        self.addCleanup(fx2.doCleanups)
        fx2._prepare_migrate()
        fx2._run(key="aaa", owner="o1", now=30)
        cons2 = os.path.join(fx2.tmp.name, "consumers.json")
        migration_batch.consume(fx2.paths["coord"], cons2, "claim",
                                "c1", "o1", 40, lease=100, idem="k1")
        self.server.migration_batches = fx2.paths["coord"]
        self.server.migration_consumers = cons2
        os.unlink(fx2.paths["coord"])
        status, body, _ = self._get_status("consumer=c1&now=40")
        self.assertEqual((status, body),
                         (404, {"error": "migration_batches_not_found"}))

    # -- reject ---------------------------------------------------------------

    def test_reject_parks_event_and_advances_cursor(self) -> None:
        total = len(migration_batch.events(
            self.coord, limit=1000)["events"])
        self._claim()
        status, body, raw = self._reject(0, reason="  拒绝理由 \t")
        self.assertEqual(status, 200)
        self.assertEqual(list(body),
                         ["consumer", "owner", "until", "position",
                          "dead_letter"])
        self.assertEqual(body["consumer"], "c1")
        self.assertEqual(body["owner"], "o1")
        self.assertEqual(body["until"], 140)
        self.assertEqual(body["position"], 0)
        letter = body["dead_letter"]
        self.assertEqual(list(letter),
                         ["position", "event", "reason", "rejected_at",
                          "owner"])
        self.assertEqual(letter["position"], 0)
        self.assertEqual(letter["reason"], "拒绝理由")
        self.assertEqual(letter["rejected_at"], 40)
        self.assertEqual(letter["owner"], "o1")
        stream = migration_batch.events(self.coord, limit=1000)["events"]
        self.assertEqual(letter["event"], stream[0])
        self.assertFalse(raw.endswith(b"\n"))
        self.assertNotIn(b"\\u", raw)
        self.assertIn("拒绝理由".encode("utf-8"), raw)
        self.assertEqual(
            raw, migration_batch.consume_response(
                self.coord, self.consumers, "reject", "c1", "o1", 40,
                position=0, reason="  拒绝理由 \t", idem="r"))
        # The rejected event no longer pulls; the cursor is at 0.
        status, page, _ = self._post(
            "pull", {"consumer": "c1", "owner": "o1", "now": 40,
                     "limit": 1000})
        self.assertEqual(status, 200)
        self.assertEqual([e["position"] for e in page["events"]],
                         list(range(1, total)))
        # The first reject upgraded the on-disk ledger with the
        # dead_letters section seeded for every existing consumer.
        self._claim(consumer="c2", idem="k2", key="aaa")
        # c2 was claimed after the upgrade, so a reject there appends to
        # its already-present empty list.
        doc = json.loads(Path(self.consumers).read_text("utf-8"))
        self.assertEqual(list(doc),
                         ["version", "coordination", "subscriptions",
                          "consumers", "idempotency", "audit",
                          "dead_letters"])
        self.assertEqual(sorted(doc["dead_letters"]), ["c1", "c2"])
        self.assertEqual(len(doc["dead_letters"]["c1"]), 1)
        self.assertEqual(doc["dead_letters"]["c2"], [])

    def test_reject_bad_bodies_are_400(self) -> None:
        self._claim()
        good = {"consumer": "c1", "owner": "o1", "now": 40, "position": 0,
                "reason": "r", "idem": "i"}
        for bad in (
            {**good, "extra": 1},
            {**good, "position": -1},
            {**good, "position": True},
            {**good, "position": 1.5},
            {**good, "position": None},
            {**good, "reason": ""},
            {**good, "reason": "   "},
            {**good, "reason": "\t\n "},
            {**good, "reason": 5},
            {**good, "reason": True},
            {**good, "reason": None},
            {**good, "reason": ["r"]},
            {**good, "reason": "x" * 513},
            {**good, "reason": "あ" * 513},
            {**good, "idem": ""},
            {"consumer": "c1", "owner": "o1", "now": 40, "position": 0,
             "idem": "i"},
        ):
            with self.subTest(bad=bad):
                status, body, _ = self._post("reject", bad)
                self.assertEqual((status, body),
                                 (400, {"error": "invalid_request"}))
        # 512 code points after trimming are accepted.
        status, _, _ = self._post(
            "reject", {**good, "reason": " " + "あ" * 512, "idem": "i512"})
        self.assertEqual(status, 200)
        # Malformed JSON and duplicate members.
        status, _, _ = self._post(
            "reject", raw=b'{"consumer":"c1","consumer":"c2","owner":"o1",'
                          b'"now":40,"position":0,"reason":"r","idem":"i"}')
        self.assertEqual(status, 400)
        # A query string is a 400 on the write path.
        status, _, _ = self._post("reject", good, query="?x=1")
        self.assertEqual(status, 400)

    def test_reject_skipping_past_tail_or_in_place_is_400(self) -> None:
        total = len(migration_batch.events(
            self.coord, limit=1000)["events"])
        self._claim()
        for bad_position in (1, 2, total + 10):
            status, body, _ = self._reject(
                bad_position, idem=f"r{bad_position}")
            self.assertEqual((status, body),
                             (400, {"error": "invalid_request"}))
        status, _, _ = self._reject(0, idem="r0")
        self.assertEqual(status, 200)
        # No in-place reject: position 0 is at the cursor now.
        status, body, _ = self._reject(0, idem="r0b")
        self.assertEqual((status, body),
                         (400, {"error": "invalid_request"}))
        # A non-matching position for a fixed-key subscription is 400.
        self._claim(consumer="ck", idem="kb", key="zzz")
        status, _, _ = self._reject(0, consumer="ck", idem="rk0")
        self.assertEqual(status, 400)

    def test_reject_ownership_is_409_and_unknown_consumer_404(self) -> None:
        self._claim()
        status, body, _ = self._reject(0, owner="o2", now=40, idem="r0")
        self.assertEqual((status, body),
                         (409, {"error": "migration_consumer_ownership"}))
        status, body, _ = self._reject(0, owner="o1", now=141, idem="r0")
        self.assertEqual((status, body),
                         (409, {"error": "migration_consumer_ownership"}))
        status, body, _ = self._reject(0, consumer="ghost", idem="r0")
        self.assertEqual((status, body),
                         (404, {"error": "migration_consumer_not_found"}))

    def test_reject_checkpoint_regression_is_409(self) -> None:
        total = len(migration_batch.events(
            self.coord, limit=1000)["events"])
        self._claim()
        self._ack(total - 1)
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
        status, body, _ = self._reject(0, idem="r0")
        self.assertEqual((status, body),
                         (409, {"error": "migration_consumer_checkpoint"}))

    def test_reject_idempotent_replay_and_changed_request(self) -> None:
        self._claim()
        status, first, raw = self._reject(0, reason="r", idem="r0")
        self.assertEqual(status, 200)
        before = Path(self.consumers).read_bytes()
        # Identical replay returns the same bytes and writes nothing.
        status, replay, replay_raw = self._reject(0, reason="r", idem="r0")
        self.assertEqual(status, 200)
        self.assertEqual(replay, first)
        self.assertEqual(replay_raw, raw)
        self.assertEqual(Path(self.consumers).read_bytes(), before)
        # The same key with a changed request is 400.
        status, body, _ = self._reject(0, reason="other", idem="r0")
        self.assertEqual((status, body),
                         (400, {"error": "invalid_request"}))
        status, _, _ = self._reject(1, reason="r", idem="r0")
        self.assertEqual(status, 400)

    def test_reject_scope_uses_persisted_subscription(self) -> None:
        # The fixed-aaa subscription is rejectable through the aaa key
        # token: position 0 is an aaa event.
        self._claim(consumer="fixed", idem="f1", key="aaa")
        status, _, _ = self._reject(
            0, consumer="fixed", idem="r0", token=KEY_TOKEN)
        self.assertEqual(status, 200)
        # A cross-batch subscription stays forbidden.
        self._claim(consumer="wide", idem="w1")
        status, body, _ = self._reject(
            0, consumer="wide", idem="rw", token=KEY_TOKEN)
        self.assertEqual((status, body), (403, {"error": "forbidden"}))
        # Ops/stage tokens are rejected before the consumer is looked up.
        for token in (OPS_TOKEN, STAGE_TOKEN):
            status, _, _ = self._reject(
                0, consumer="ghost", idem="rg", token=token)
            self.assertEqual(status, 403)

    # -- dead-letter endpoint -------------------------------------------------

    def test_dead_letters_returns_paged_entries(self) -> None:
        total = len(migration_batch.events(
            self.coord, limit=1000)["events"])
        self._claim()
        self._reject(0, reason="理由", idem="r0")
        self._ack(1, idem="a1")
        self._reject(2, reason="two", idem="r2")
        status, page, raw = self._get_dead_letters(
            "consumer=c1&limit=1")
        self.assertEqual(status, 200)
        self.assertEqual(list(page), ["consumer", "entries", "next"])
        self.assertEqual([e["position"] for e in page["entries"]], [0])
        self.assertEqual(page["next"], 0)
        letter = page["entries"][0]
        self.assertEqual(list(letter),
                         ["position", "event", "reason", "rejected_at",
                          "owner"])
        self.assertEqual(letter["reason"], "理由")
        stream = migration_batch.events(self.coord, limit=1000)["events"]
        self.assertEqual(letter["event"], stream[0])
        status, page, _ = self._get_dead_letters("consumer=c1&cursor=0&limit=1")
        self.assertEqual(status, 200)
        self.assertEqual([e["position"] for e in page["entries"]], [2])
        self.assertIsNone(page["next"])
        # Default limit 100 returns the complete page with next null.
        status, full, _ = self._get_dead_letters("consumer=c1")
        self.assertEqual(status, 200)
        self.assertEqual([e["position"] for e in full["entries"]], [0, 2])
        self.assertIsNone(full["next"])
        self.assertFalse(raw.endswith(b"\n"))
        self.assertNotIn(b"\\u", raw)
        self.assertEqual(
            raw, migration_batch.consumer_dead_letters_response(
                self.coord, self.consumers, "c1", None, 1))
        # An empty page past the cursor carries next null.
        status, page, _ = self._get_dead_letters("consumer=c1&cursor=2")
        self.assertEqual((page["entries"], page["next"]), ([], None))
        # A consumer that never rejected has an empty page.
        self._claim(consumer="empty", idem="ke")
        status, page, _ = self._get_dead_letters("consumer=empty")
        self.assertEqual(status, 200)
        self.assertEqual(page, {"consumer": "empty", "entries": [],
                                "next": None})

    def test_dead_letters_bad_queries_are_400(self) -> None:
        self._claim()
        for query in (
            "",
            "consumer=c1&limit=0",
            "consumer=c1&limit=1001",
            "consumer=c1&limit=x",
            "consumer=c1&cursor=-1",
            "consumer=c1&cursor=1.5",
            "consumer=c1&cursor=x",
            "consumer=c1&cursor=0&extra=1",
            "consumer=c1&consumer=c2",
            "consumer=",
            "CONSUMER=c1",
        ):
            with self.subTest(query=query):
                status, body, _ = self._get_dead_letters(query)
                self.assertEqual((status, body),
                                 (400, {"error": "invalid_request"}))

    def test_dead_letters_identity_unknown_and_scope(self) -> None:
        status, body, _ = self._get_dead_letters("consumer=c1", token=None)
        self.assertEqual((status, body), (401, {"error": "unauthorized"}))
        status, body, _ = self._get_dead_letters("consumer=c1",
                                                 token="wrong")
        self.assertEqual((status, body), (403, {"error": "forbidden"}))
        status, body, _ = self._get_dead_letters("consumer=ghost")
        self.assertEqual((status, body),
                         (404, {"error": "migration_consumer_not_found"}))
        # Scopes mirror the status query.
        self._claim(consumer="fixed", idem="f1", key="aaa")
        self._reject(0, consumer="fixed", idem="r0")
        self.assertEqual(
            self._get_dead_letters("consumer=fixed",
                                   token=KEY_TOKEN)[0], 200)
        self._claim(consumer="wide", idem="w1")
        status, body, _ = self._get_dead_letters(
            "consumer=wide", token=KEY_TOKEN)
        self.assertEqual((status, body), (403, {"error": "forbidden"}))
        for token in (OPS_TOKEN, STAGE_TOKEN):
            self.assertEqual(
                self._get_dead_letters("consumer=ghost",
                                       token=token)[0], 403)
        self.assertEqual(
            self._get_dead_letters("consumer=ghost",
                                   token=KEY_TOKEN)[0], 404)

    def test_dead_letters_ignore_a_broken_coordination_ledger(self) -> None:
        # The query reads only the consumer ledger, so a coordination
        # ledger that became malformed does not affect it -- while the
        # reject write still maps that to 409.
        self._claim()
        self._reject(0, idem="r0")
        with open(self.coord, "wb") as handle:
            handle.write(b"{broken\n")
        status, body, _ = self._get_dead_letters("consumer=c1")
        self.assertEqual(status, 200)
        self.assertEqual([e["position"] for e in body["entries"]], [0])
        status, resp, _ = self._reject(1, idem="r1")
        self.assertEqual((status, resp),
                         (409, {"error": "migration_batches_invalid"}))

    def test_dead_letters_ledger_errors(self) -> None:
        self._claim()
        self._reject(0, idem="r0")
        with open(self.consumers, "wb") as handle:
            handle.write(b"{broken\n")
        status, body, _ = self._get_dead_letters("consumer=c1")
        self.assertEqual((status, body),
                         (409, {"error": "migration_consumers_invalid"}))
        self.server.migration_consumers = os.path.join(
            self.fx.tmp.name, "no-dir", "consumers.json")
        status, body, _ = self._get_dead_letters("consumer=c1")
        self.assertEqual((status, body),
                         (404, {"error": "migration_consumers_not_found"}))

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

    def _get_status_code(self, server):
        connection = HTTPConnection("127.0.0.1", server.server_port,
                                    timeout=2)
        connection.request("GET", "/migration-consumers/status?consumer=c&now=1",
                           headers={"X-Audit-Token": "anything"})
        response = connection.getresponse()
        body = response.read()
        connection.close()
        return response.status, json.loads(body)

    def test_get_status_unconfigured_is_plain_404(self) -> None:
        server, thread = self._server()
        try:
            self.assertEqual(self._get_status_code(server),
                             (404, {"error": "not_found"}))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

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
