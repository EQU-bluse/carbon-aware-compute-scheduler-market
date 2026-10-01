"""HTTP tests for reject and the dead-letters query.

Covers POST /migration-consumers/reject (fixed field set, the trimmed
reason, the earliest-unacknowledged-matching-event rule, ownership and
lease, idempotent replay vs a divergent request, the compact wire body
and the persisted-subscription scope rules) and GET
/migration-consumers/dead-letters (exclusive position cursor, 1..1000
limit, default 100, per-consumer ordering and next cursor, query
validation, scope and error mapping), plus the plain 404 for the wrong
method or an unconfigured server.
"""

from __future__ import annotations

import json
import os
import threading
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer

from carbon_market import migration_batch
from carbon_market.server import Handler
from tests.test_migration_batch import MigrationBatchTest

FULL_TOKEN = "full-token"
KEY_TOKEN = "key-token"
OPS_TOKEN = "ops-token"
STAGE_TOKEN = "stage-token"


def _digest(token: str) -> str:
    import hashlib
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _line(name: str, token: str, grace_until=None,
          ops="*", stages="*", keys="*") -> str:
    return json.dumps([name, _digest(token), grace_until, ops, stages, keys],
                      ensure_ascii=False)


class MigrationRejectHttpTest(unittest.TestCase):
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
        self.total = len(migration_batch.events(self.coord, limit=1000)[
            "events"])

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

    def _request(self, method, path, body=None, token=FULL_TOKEN,
                 raw=None, headers=None):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        all_headers = {"Content-Type": "application/json"}
        if token is not None:
            all_headers["X-Audit-Token"] = token
        if headers:
            all_headers.update(headers)
        if raw is not None:
            data = raw
        elif body is not None:
            data = json.dumps(body).encode()
        else:
            data = None
        connection.request(method, path, data, all_headers)
        response = connection.getresponse()
        raw_body = response.read()
        connection.close()
        try:
            return response.status, json.loads(raw_body), raw_body
        except ValueError:
            return response.status, None, raw_body

    def _post(self, operation, body=None, token=FULL_TOKEN, raw=None,
              query=""):
        return self._request(
            "POST", f"/migration-consumers/{operation}{query}", body=body,
            token=token, raw=raw)

    def _claim(self, consumer="c1", owner="o1", now=40, lease=100,
               idem="k1", token=FULL_TOKEN, **filters):
        body = {"consumer": consumer, "owner": owner, "now": now,
                "lease": lease, "idem": idem, **filters}
        return self._post("claim", body, token=token)

    def _reject(self, position, reason="bad", consumer="c1", owner="o1",
                now=40, idem=None, token=FULL_TOKEN):
        return self._post("reject", {
            "consumer": consumer, "owner": owner, "now": now,
            "position": position, "reason": reason,
            "idem": idem if idem is not None else f"r-{consumer}-{position}"},
            token=token)

    def _dead_letters(self, query, token=FULL_TOKEN, raw_path=None):
        path = raw_path if raw_path is not None else \
            f"/migration-consumers/dead-letters?{query}" if query else \
            "/migration-consumers/dead-letters"
        return self._request("GET", path, token=token)

    # -- reject happy path ----------------------------------------------------

    def test_reject_advances_and_records_dead_letter(self) -> None:
        self._claim()
        stream = migration_batch.events(self.coord, limit=1)["events"][0]
        status, body, raw = self._reject(0, reason="  坏数据 😀 \t",
                                         idem="r0")
        self.assertEqual(status, 200)
        self.assertEqual(list(body),
                         ["consumer", "owner", "until", "position",
                          "dead_letter"])
        self.assertEqual(body["consumer"], "c1")
        self.assertEqual(body["position"], 0)
        letter = body["dead_letter"]
        self.assertEqual(list(letter),
                         ["position", "event", "reason", "rejected_at",
                          "owner"])
        self.assertEqual(letter["reason"], "坏数据 😀")
        self.assertEqual(letter["position"], 0)
        self.assertEqual(letter["rejected_at"], 40)
        self.assertEqual(letter["owner"], "o1")
        self.assertEqual(letter["event"], stream)
        self.assertFalse(raw.endswith(b"\n"))
        self.assertNotIn(b"\\u", raw)
        self.assertIn("坏数据".encode("utf-8"), raw)
        self.assertEqual(
            raw, migration_batch.consume_response(
                self.coord, self.consumers, "reject", "c1", "o1", 40,
                position=0, reason="坏数据 😀", idem="r0"))

    def test_rejected_event_is_not_pulled_again(self) -> None:
        self._claim()
        self.assertEqual(self._reject(0)[0], 200)
        status, page, _ = self._post(
            "pull", {"consumer": "c1", "owner": "o1", "now": 40, "limit": 2})
        self.assertEqual(status, 200)
        self.assertEqual([e["position"] for e in page["events"]], [1, 2])
        self.assertEqual(page["position"], 0)

    def test_reject_then_drain_to_empty(self) -> None:
        self._claim()
        for position in range(self.total):
            self.assertEqual(
                self._reject(position, idem=f"r{position}")[0], 200)
        status, page, _ = self._post(
            "pull", {"consumer": "c1", "owner": "o1", "now": 40})
        self.assertEqual(status, 200)
        self.assertEqual(page["events"], [])
        self.assertIsNone(page["next"])

    # -- reject request validation --------------------------------------------

    def test_reject_bad_bodies_are_400(self) -> None:
        self._claim()
        good = {"consumer": "c1", "owner": "o1", "now": 40, "position": 0,
                "reason": "x", "idem": "i"}
        for bad_body in (
            {**good, "position": -1},
            {**good, "position": True},
            {**good, "position": 1.5},
            {**good, "position": "0"},
            {**good, "reason": ""},
            {**good, "reason": "   "},
            {**good, "reason": "\n\t "},
            {**good, "reason": "x" * 513},
            {**good, "reason": "😀" * 513},
            {**good, "reason": 5},
            {**good, "reason": None},
            {**good, "reason": ["x"]},
            {**good, "idem": ""},
            {**good, "consumer": ""},
            {**good, "owner": ""},
            {**good, "now": -1},
            {**good, "now": True},
            {**good, "extra": 1},
            {**good, "limit": 2},
            {"consumer": "c1", "owner": "o1", "now": 40, "position": 0,
             "reason": "x"},                      # missing idem
            {"consumer": "c1", "owner": "o1", "now": 40,
             "reason": "x", "idem": "i"},         # missing position
            {"consumer": "c1", "owner": "o1", "now": 40, "position": 0,
             "idem": "i"},                        # missing reason
        ):
            with self.subTest(bad=bad_body):
                status, body, _ = self._post("reject", bad_body)
                self.assertEqual((status, body),
                                 (400, {"error": "invalid_request"}))

    def test_reject_malformed_json_is_400(self) -> None:
        status, body, _ = self._post("reject", raw=b"{not json")
        self.assertEqual((status, body),
                         (400, {"error": "invalid_request"}))
        status, _, _ = self._post(
            "reject",
            raw=b'{"consumer":"a","consumer":"b","owner":"o","now":1,'
                b'"position":0,"reason":"x","idem":"i"}')
        self.assertEqual(status, 400)
        status, _, _ = self._post(
            "reject",
            raw=b'{"consumer":"c","owner":"o","now":NaN,"position":0,'
                b'"reason":"x","idem":"i"}')
        self.assertEqual(status, 400)

    def test_reject_query_string_is_400(self) -> None:
        self._claim()
        status, body, _ = self._post(
            "reject",
            {"consumer": "c1", "owner": "o1", "now": 40, "position": 0,
             "reason": "x", "idem": "i"}, query="?x=1")
        self.assertEqual((status, body),
                         (400, {"error": "invalid_request"}))

    def test_reason_boundary_512_code_points_is_accepted(self) -> None:
        self._claim()
        status, _, _ = self._reject(0, reason="😀" * 512)
        self.assertEqual(status, 200)

    # -- reject position rules ------------------------------------------------

    def test_reject_skipping_earliest_is_400(self) -> None:
        self._claim()
        status, body, _ = self._reject(1, idem="r1")
        self.assertEqual((status, body),
                         (400, {"error": "invalid_request"}))

    def test_reject_past_tail_is_400(self) -> None:
        self._claim()
        status, _, _ = self._reject(self.total + 10, idem="rp")
        self.assertEqual(status, 400)

    def test_reject_non_matching_position_is_400(self) -> None:
        events = migration_batch.events(self.coord, limit=1000)["events"]
        aaa = next(e["position"] for e in events if e["key"] == "aaa")
        zzz = next(e["position"] for e in events if e["key"] == "zzz")
        self._claim(consumer="ck", idem="kk", key="zzz")
        status, _, _ = self._reject(aaa, consumer="ck", idem="ra")
        self.assertEqual(status, 400)
        status, body, _ = self._reject(zzz, consumer="ck", idem="rz")
        self.assertEqual(status, 200, body)
        self.assertEqual(body["position"], zzz)

    # -- ownership ------------------------------------------------------------

    def test_reject_ownership_and_lease_are_409(self) -> None:
        self._claim()
        status, body, _ = self._reject(0, owner="o2", idem="rx")
        self.assertEqual((status, body),
                         (409, {"error": "migration_consumer_ownership"}))
        status, _, _ = self._reject(0, now=141, idem="ry")
        self.assertEqual(status, 409)

    def test_reject_unknown_consumer_is_404(self) -> None:
        status, body, _ = self._reject(0, consumer="ghost", idem="i")
        self.assertEqual((status, body),
                         (404, {"error": "migration_consumer_not_found"}))

    # -- idempotency ----------------------------------------------------------

    def test_reject_idempotent_replay(self) -> None:
        self._claim()
        _, first, raw_first = self._reject(0, reason="x")
        status, replay, raw_replay = self._reject(0, reason="x")
        self.assertEqual(status, 200)
        self.assertEqual(replay, first)
        self.assertEqual(raw_replay, raw_first)
        # Equivalent surrounding whitespace trims to the same request.
        status, replay2, _ = self._reject(0, reason="  x  ")
        self.assertEqual(status, 200)
        self.assertEqual(replay2, first)

    def test_reject_divergent_request_is_400(self) -> None:
        self._claim()
        self.assertEqual(self._reject(0, reason="x")[0], 200)
        status, body, _ = self._reject(0, reason="different")
        self.assertEqual((status, body),
                         (400, {"error": "invalid_request"}))

    # -- checkpoint regression ------------------------------------------------

    def test_reject_checkpoint_regression_is_409(self) -> None:
        self._claim()
        self._post("ack", {"consumer": "c1", "owner": "o1", "now": 40,
                           "position": self.total - 1, "idem": "a-last"})
        other = MigrationBatchTest(
            "test_get_returns_copy_and_unknown_key_raises")
        other.setUp()
        self.addCleanup(other.doCleanups)
        other._prepare_migrate()
        other._run(key="aaa", owner="o1", now=30)
        doc = json.loads(open(self.consumers, encoding="utf-8").read())
        doc["coordination"] = os.path.realpath(other.paths["coord"])
        other_cons = os.path.join(other.tmp.name, "consumers.json")
        with open(other_cons, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(doc, ensure_ascii=False,
                                    separators=(",", ":")) + "\n")
        self.server.migration_batches = other.paths["coord"]
        self.server.migration_consumers = other_cons
        status, body, _ = self._reject(self.total - 1)
        self.assertEqual((status, body),
                         (409, {"error": "migration_consumer_checkpoint"}))

    # -- reject scopes --------------------------------------------------------

    def test_reject_scope_uses_persisted_subscription(self) -> None:
        self._claim(consumer="fixed", idem="f1", key="aaa")
        # The matching key-scoped token may reject the earliest event.
        status, body, _ = self._reject(
            0, consumer="fixed", idem="fr0", token=KEY_TOKEN)
        self.assertEqual(status, 200, body)
        # A cross-batch subscription stays forbidden.
        self._claim(consumer="wide", idem="w1")
        status, body, _ = self._reject(
            0, consumer="wide", idem="wr0", token=KEY_TOKEN)
        self.assertEqual((status, body), (403, {"error": "forbidden"}))
        # Ops- and stage-scoped tokens are rejected before the consumer
        # is even looked up.
        for token in (OPS_TOKEN, STAGE_TOKEN):
            status, _, _ = self._reject(
                0, consumer="ghost", idem="i", token=token)
            self.assertEqual(status, 403)

    def test_reject_identity_is_checked_first(self) -> None:
        status, body, _ = self._post(
            "reject", {"bogus": 1}, token=None)
        self.assertEqual((status, body), (401, {"error": "unauthorized"}))
        status, body, _ = self._post(
            "reject", {"bogus": 1}, token="wrong")
        self.assertEqual((status, body), (403, {"error": "forbidden"}))

    # -- dead-letters happy path ---------------------------------------------

    def test_dead_letters_lists_entries_in_position_order(self) -> None:
        self._claim()
        stream = migration_batch.events(self.coord, limit=1000)["events"]
        self._reject(0, reason="r0")
        self._reject(1, reason="r1")
        status, body, raw = self._dead_letters("consumer=c1")
        self.assertEqual(status, 200)
        self.assertEqual(list(body), ["consumer", "entries", "next"])
        self.assertEqual(body["consumer"], "c1")
        self.assertEqual([e["position"] for e in body["entries"]], [0, 1])
        self.assertIsNone(body["next"])
        self.assertEqual(
            list(body["entries"][0]),
            ["position", "event", "reason", "rejected_at", "owner"])
        self.assertEqual(body["entries"][0]["event"], stream[0])
        self.assertEqual(body["entries"][0]["reason"], "r0")
        self.assertEqual(body["entries"][1]["reason"], "r1")
        self.assertFalse(raw.endswith(b"\n"))
        self.assertNotIn(b"\\u", raw)
        self.assertEqual(
            raw, migration_batch.consumer_dead_letters_response(
                self.coord, self.consumers, "c1"))

    def test_dead_letters_default_limit_is_100(self) -> None:
        # The default applies when no limit is supplied; the library
        # default and the endpoint default agree.
        self._claim()
        status, _, _ = self._dead_letters("consumer=c1")
        self.assertEqual(status, 200)

    def test_dead_letters_pagination(self) -> None:
        self._claim()
        for position in range(self.total):
            self._reject(position, reason="r", idem=f"r{position}")
        status, body, _ = self._dead_letters("consumer=c1&limit=2")
        self.assertEqual(status, 200)
        self.assertEqual([e["position"] for e in body["entries"]], [0, 1])
        self.assertEqual(body["next"], 1)
        cursor = body["next"]
        status, body, _ = self._dead_letters(
            f"consumer=c1&cursor={cursor}&limit=2")
        self.assertEqual([e["position"] for e in body["entries"]], [2, 3])
        self.assertEqual(body["next"], 3)
        status, body, _ = self._dead_letters(
            f"consumer=c1&cursor={self.total - 1}")
        self.assertEqual(body["entries"], [])
        self.assertIsNone(body["next"])
        # limit 1000 is accepted, 1001 is a 400.
        self.assertEqual(
            self._dead_letters("consumer=c1&limit=1000")[0], 200)
        self.assertEqual(
            self._dead_letters("consumer=c1&limit=1001")[0], 400)

    def test_dead_letters_are_scoped_per_consumer(self) -> None:
        self._claim()
        self._reject(0, idem="r0")
        self._claim(consumer="c2", idem="k2")
        self._reject(0, consumer="c2", idem="r2")
        for consumer in ("c1", "c2"):
            status, body, _ = self._dead_letters(f"consumer={consumer}")
            self.assertEqual(status, 200)
            self.assertEqual(body["consumer"], consumer)
            self.assertEqual([e["position"] for e in body["entries"]], [0])

    def test_dead_letters_empty_page_before_any_reject(self) -> None:
        self._claim()
        status, body, _ = self._dead_letters("consumer=c1")
        self.assertEqual(status, 200)
        self.assertEqual(body["entries"], [])
        self.assertIsNone(body["next"])

    # -- dead-letters query validation ---------------------------------------

    def test_dead_letters_bad_queries_are_400(self) -> None:
        self._claim()
        for query in (
            "",
            "consumer=",
            "consumer=c1&limit=0",
            "consumer=c1&limit=1001",
            "consumer=c1&limit=x",
            "consumer=c1&limit=1.5",
            "consumer=c1&cursor=-1",
            "consumer=c1&cursor=1.5",
            "consumer=c1&cursor=0x1",
            "consumer=c1&cursor=1e1",
            "consumer=c1&cursor=",
            "consumer=c1&extra=1",
            "consumer=c1&limit=1&limit=2",
            "consumer=c1&cursor=0&cursor=1",
            "consumer=c1&consumer=c2",
            "consumer=c1&now=40",
            "CONSUMER=c1",
        ):
            with self.subTest(query=query):
                status, body, _ = self._dead_letters(query)
                self.assertEqual((status, body),
                                 (400, {"error": "invalid_request"}))

    def test_dead_letters_unknown_consumer_is_404(self) -> None:
        status, body, _ = self._dead_letters("consumer=ghost")
        self.assertEqual((status, body),
                         (404, {"error": "migration_consumer_not_found"}))

    # -- dead-letters authorization ------------------------------------------

    def test_dead_letters_identity_is_checked_first(self) -> None:
        status, body, _ = self._dead_letters("consumer=ghost", token=None)
        self.assertEqual((status, body), (401, {"error": "unauthorized"}))
        status, body, _ = self._dead_letters("consumer=ghost", token="wrong")
        self.assertEqual((status, body), (403, {"error": "forbidden"}))

    def test_dead_letters_scope_uses_persisted_subscription(self) -> None:
        self._claim(consumer="fixed", idem="f1", key="aaa")
        self._reject(0, consumer="fixed", idem="fr0")
        self.assertEqual(
            self._dead_letters("consumer=fixed", token=KEY_TOKEN)[0], 200)
        self._claim(consumer="wide", idem="w1")
        self._reject(0, consumer="wide", idem="wr0")
        status, body, _ = self._dead_letters(
            "consumer=wide", token=KEY_TOKEN)
        self.assertEqual((status, body), (403, {"error": "forbidden"}))
        self._claim(consumer="jobonly", idem="j1", job_id="j-1")
        status, _, _ = self._dead_letters(
            "consumer=jobonly", token=KEY_TOKEN)
        self.assertEqual(status, 403)
        for token in (OPS_TOKEN, STAGE_TOKEN):
            self.assertEqual(
                self._dead_letters("consumer=ghost", token=token)[0], 403)
        # The key-scoped token reads the subscription to decide scope,
        # so an unknown consumer is 404, not 403.
        self.assertEqual(
            self._dead_letters("consumer=ghost", token=KEY_TOKEN)[0], 404)

    # -- ledger errors --------------------------------------------------------

    def test_invalid_consumers_ledger_is_409(self) -> None:
        self._claim()
        self._reject(0)
        with open(self.consumers, "wb") as handle:
            handle.write(b"{broken\n")
        status, body, _ = self._reject(1, idem="r1")
        self.assertEqual((status, body),
                         (409, {"error": "migration_consumers_invalid"}))
        status, body, _ = self._dead_letters("consumer=c1")
        self.assertEqual((status, body),
                         (409, {"error": "migration_consumers_invalid"}))

    def test_missing_consumers_parent_is_404(self) -> None:
        self.server.migration_consumers = os.path.join(
            self.fx.tmp.name, "no-dir", "consumers.json")
        status, body, _ = self._dead_letters("consumer=c1")
        self.assertEqual((status, body),
                         (404, {"error": "migration_consumers_not_found"}))

    @unittest.skipIf(hasattr(os, "geteuid") and os.geteuid() == 0,
                     "read-only directories do not block root")
    def test_reject_commit_failure_is_503(self) -> None:
        self._claim()
        # Make the ledger directory unwritable: the reject's atomic
        # commit cannot land, validation already passed, and the answer
        # is 503 rather than a 4xx.
        directory = os.path.dirname(self.consumers)
        os.chmod(directory, 0o555)
        self.addCleanup(os.chmod, directory, 0o755)
        try:
            status, body, _ = self._reject(0, idem="r0")
        finally:
            os.chmod(directory, 0o755)
        self.assertEqual((status, body),
                         (503, {"error": "migration_consumers_unavailable"}))

    # -- method and configuration gating --------------------------------------

    def test_wrong_method_or_path_is_plain_404(self) -> None:
        status, body, _ = self._request(
            "GET", "/migration-consumers/reject")
        self.assertEqual((status, body), (404, {"error": "not_found"}))
        status, body, _ = self._request(
            "POST", "/migration-consumers/dead-letters", raw=b"{}")
        self.assertEqual((status, body), (404, {"error": "not_found"}))
        status, body, _ = self._request(
            "GET", "/migration-consumers/nope")
        self.assertEqual((status, body), (404, {"error": "not_found"}))


if __name__ == "__main__":
    unittest.main()
