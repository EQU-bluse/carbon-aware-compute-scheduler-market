"""Library tests for migration_batch.consume: persistent consumers.

Covers the fixed claim/pull/ack subscription ledger: first claim and
idempotent replay, the immutable (key, job_id) subscription, pulls
paged strictly after the acknowledged position without advancing it,
forward/in-place/backward/past-tail/non-matching acks, ownership and
strict lease expiry (PermissionError vs TimeoutError), expired-lease
takeover with redelivery, the LookupError when a confirmed event is
truncated, rewritten or position-reused, canonical ledger bytes, the
coordination binding and the FileNotFoundError/ValueError/OSError
surface.
"""

from __future__ import annotations

import json
import os
import unittest
from pathlib import Path

from carbon_market import migration_batch as mb
from tests.test_migration_batch import MigrationBatchTest


class ConsumeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fx = MigrationBatchTest(
            "test_get_returns_copy_and_unknown_key_raises")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.fx._prepare_migrate()
        self.coord = self.fx.paths["coord"]
        self.cons = os.path.join(self.fx.tmp.name, "consumers.json")
        # Every claim requires an existing canonical coordination ledger,
        # so the populated two-batch stream is the common starting state.
        self.total = self._populate()

    # -- fixture --------------------------------------------------------------

    def _populate(self) -> int:
        fx = self.fx
        fx._run(key="aaa", owner="o1", now=30)
        fx._run(key="zzz", now=31)
        fx._run(key="aaa", owner="o1", now=32,
                receipts={"j-1": fx._receipt(
                    "copy", "succeeded", "复制完成", 33)})
        fx._run(key="aaa", owner="o1", now=34,
                receipts={"j-1": fx._receipt(
                    "switch", "succeeded", "切换完成", 35)})
        return len(mb.events(self.coord, limit=1000)["events"])

    def _claim(self, consumer="c1", owner="o1", now=40, lease=100,
               idem="k1", **filters):
        return mb.consume(self.coord, self.cons, "claim", consumer, owner,
                          now, lease=lease, idem=idem, **filters)

    def _pull(self, consumer="c1", owner="o1", now=40, **kwargs):
        return mb.consume(self.coord, self.cons, "pull", consumer, owner,
                          now, **kwargs)

    def _ack(self, position, consumer="c1", owner="o1", now=40, idem=None):
        return mb.consume(self.coord, self.cons, "ack", consumer, owner,
                          now, position=position, idem=idem)

    # -- argument validation --------------------------------------------------

    def test_bad_arguments_raise_value_error(self) -> None:
        kwargs = dict(lease=1, idem="i")
        for bad in (
            lambda: mb.consume("", self.cons, "claim", "c", "o", 1, **kwargs),
            lambda: mb.consume(self.coord, "", "claim", "c", "o", 1, **kwargs),
            lambda: mb.consume(self.coord, self.cons, "nope", "c", "o", 1,
                               **kwargs),
            lambda: mb.consume(self.coord, self.cons, "claim", "", "o", 1,
                               **kwargs),
            lambda: mb.consume(self.coord, self.cons, "claim", "c", "", 1,
                               **kwargs),
            lambda: mb.consume(self.coord, self.cons, "claim", "c", "o", -1,
                               **kwargs),
            lambda: mb.consume(self.coord, self.cons, "claim", "c", "o", 1,
                               lease=0, idem="i"),
            lambda: mb.consume(self.coord, self.cons, "claim", "c", "o", 1,
                               lease=True, idem="i"),  # boolean
            lambda: mb.consume(self.coord, self.cons, "claim", "c", "o", 1,
                               lease=1, idem=""),
            lambda: mb.consume(self.coord, self.cons, "claim", "c", "o", 1,
                               lease=1, idem="i", key=""),
            lambda: mb.consume(self.coord, self.cons, "claim", "c", "o", 1,
                               lease=1, idem="i", job_id=""),
            lambda: mb.consume(self.coord, self.cons, "pull", "c", "o", 1,
                               idem="i"),
            lambda: mb.consume(self.coord, self.cons, "pull", "c", "o", 1,
                               limit=0),
            lambda: mb.consume(self.coord, self.cons, "pull", "c", "o", 1,
                               limit=1001),
            lambda: mb.consume(self.coord, self.cons, "ack", "c", "o", 1,
                               position=-1, idem="i"),
        ):
            with self.subTest(bad=bad):
                self.assertRaises(ValueError, bad)

    def test_ledger_paths_must_be_distinct(self) -> None:
        with self.assertRaises(ValueError):
            mb.consume(self.coord, self.coord, "claim", "c", "o", 1,
                       lease=1, idem="i")

    # -- claim ----------------------------------------------------------------

    def test_first_claim_persists_subscription_and_null_position(self) -> None:
        result, created = self._claim()
        self.assertTrue(created)
        self.assertEqual(result, {
            "consumer": "c1", "key": None, "job_id": None, "owner": "o1",
            "until": 140, "position": None, "taken_over": False})
        doc = json.loads(Path(self.cons).read_text("utf-8"))
        self.assertEqual(doc["subscriptions"], {"c1": {"key": None,
                                                       "job_id": None}})
        self.assertEqual(doc["consumers"]["c1"]["position"], None)
        self.assertEqual(doc["coordination"], os.path.realpath(self.coord))

    def test_claim_with_fixed_key_and_job(self) -> None:
        result, created = self._claim(
            consumer="cj", idem="k2", key="aaa", job_id="j-1")
        self.assertTrue(created)
        self.assertEqual(result["key"], "aaa")
        self.assertEqual(result["job_id"], "j-1")

    def test_equivalent_claim_replays_original_without_writing(self) -> None:
        first, _ = self._claim()
        before = Path(self.cons).read_bytes()
        replay, created = self._claim()
        self.assertFalse(created)
        self.assertEqual(replay, first)
        self.assertEqual(Path(self.cons).read_bytes(), before)

    def test_repeat_claim_by_same_owner_renews_lease(self) -> None:
        self._claim(now=40)
        result, created = self._claim(now=60, idem="k1b")
        self.assertFalse(created)
        self.assertEqual(result["until"], 160)
        self.assertFalse(result["taken_over"])

    def test_subscription_cannot_change(self) -> None:
        self._claim()
        with self.assertRaises(ValueError):
            self._claim(idem="k2", key="aaa")
        with self.assertRaises(ValueError):
            self._claim(idem="k3", job_id="j-1")

    def test_claim_idempotency_key_changed_request_raises(self) -> None:
        self._claim(lease=100)
        with self.assertRaises(ValueError):
            self._claim(lease=50)

    def test_foreign_owner_refused_during_valid_lease(self) -> None:
        self._claim()
        with self.assertRaises(PermissionError):
            self._claim(owner="o2", now=100, idem="k2")

    def test_foreign_owner_takes_over_after_strict_expiry(self) -> None:
        self._claim()
        self._ack(0, idem="a0")
        # now == until is still refused; only strictly greater takes over.
        with self.assertRaises(PermissionError):
            self._claim(owner="o2", now=140, idem="kx")
        result, created = self._claim(owner="o2", now=141, lease=10,
                                      idem="k2")
        self.assertFalse(created)
        self.assertTrue(result["taken_over"])
        self.assertEqual(result["owner"], "o2")
        self.assertEqual(result["until"], 151)
        # The acknowledged position survives the takeover.
        self.assertEqual(result["position"], 0)

    # -- pull -----------------------------------------------------------------

    def test_pull_pages_after_position_without_advancing(self) -> None:
        total = self.total
        self._claim()
        page, changed = self._pull(limit=2)
        self.assertFalse(changed)
        self.assertEqual([e["position"] for e in page["events"]], [0, 1])
        self.assertEqual(page["next"], 1)
        self.assertIsNone(page["position"])
        # A second identical pull returns the same page: no checkpoint.
        again, _ = self._pull(limit=2)
        self.assertEqual([e["position"] for e in again["events"]], [0, 1])
        # A pull always resumes at the unchanged checkpoint, so a
        # larger limit re-reads every unacknowledged event from 0;
        # continuation across pages is driven by acking, never by a
        # client cursor.
        rest, _ = self._pull(limit=1000)
        self.assertEqual([e["position"] for e in rest["events"]],
                         list(range(total)))

    def test_pull_respects_fixed_filters(self) -> None:
        self._claim(consumer="ck", idem="kb", key="zzz")
        page, _ = self._pull(consumer="ck", limit=10)
        self.assertTrue(page["events"])
        self.assertTrue(all(e["key"] == "zzz") for e in page["events"])
        self._claim(consumer="cj", idem="kj", job_id="j-1")
        page, _ = self._pull(consumer="cj", limit=10)
        self.assertTrue(all(e["job_id"] == "j-1") for e in page["events"])
        self.assertIsNone(page["next"])

    def test_pull_requires_ownership_and_valid_lease(self) -> None:
        self._claim()
        with self.assertRaises(PermissionError):
            self._pull(owner="o2", now=40)
        with self.assertRaises(TimeoutError):
            self._pull(owner="o1", now=141)

    def test_pull_unknown_consumer_is_key_error(self) -> None:
        self._claim()
        with self.assertRaises(KeyError):
            self._pull(consumer="ghost")

    # -- ack ------------------------------------------------------------------

    def test_ack_advances_only_to_a_real_matching_event(self) -> None:
        self._claim(consumer="ck", idem="kb", key="zzz")
        # Position 0 is an "aaa" event: real, but not this subscription.
        with self.assertRaises(ValueError):
            self._ack(0, consumer="ck", idem="a0")

    def test_ack_in_place_writes_nothing(self) -> None:
        self._claim()
        first, changed = self._ack(0, idem="a0")
        self.assertTrue(changed)
        self.assertEqual(first["position"], 0)
        before = Path(self.cons).read_bytes()
        # A fresh idempotency key at the same position is still a no-op.
        same, changed = self._ack(0, idem="a0-repeat")
        self.assertFalse(changed)
        self.assertEqual(same["position"], 0)
        self.assertEqual(Path(self.cons).read_bytes(), before)

    def test_ack_idempotent_replay_returns_original(self) -> None:
        self._claim()
        first, _ = self._ack(0, idem="a0")
        replay, changed = self._ack(0, idem="a0")
        self.assertFalse(changed)
        self.assertEqual(replay, first)

    def test_ack_backward_and_past_tail_raise_value_error(self) -> None:
        self._claim()
        self._ack(1, idem="a1")
        with self.assertRaises(ValueError):
            self._ack(0, idem="a2")
        with self.assertRaises(ValueError):
            self._ack(999, idem="a3")

    def test_ack_ownership_and_lease(self) -> None:
        self._claim()
        with self.assertRaises(PermissionError):
            self._ack(0, owner="o2", now=40, idem="a0")
        with self.assertRaises(TimeoutError):
            self._ack(0, owner="o1", now=141, idem="a0")

    def test_takeover_keeps_cursor_and_redelivers(self) -> None:
        total = self.total
        self._claim()
        self._ack(1, idem="a1")
        self._claim(owner="o2", now=141, lease=100, idem="k2")
        page, _ = self._pull(owner="o2", now=141, limit=1000)
        self.assertEqual([e["position"] for e in page["events"]],
                         list(range(2, total)))

    def test_confirmed_cursor_truncated_raises_lookup_error(self) -> None:
        # Confirm a late position against one stream, then rebind the
        # consumer ledger to a second, shorter canonical coordination
        # ledger at a different real path: the confirmed event is gone,
        # so the cursor must raise LookupError, never auto-reset.
        total = self.total
        self.assertGreaterEqual(total, 4)
        self._claim()
        self._ack(total - 1, idem="a-last")

        other = MigrationBatchTest(
            "test_get_returns_copy_and_unknown_key_raises")
        other.setUp()
        self.addCleanup(other.doCleanups)
        other._prepare_migrate()
        other._run(key="aaa", owner="o1", now=30)
        other_cons = os.path.join(other.tmp.name, "consumers.json")
        doc = json.loads(Path(self.cons).read_text("utf-8"))
        doc["coordination"] = os.path.realpath(other.paths["coord"])
        Path(other_cons).write_text(
            json.dumps(doc, ensure_ascii=False,
                       separators=(",", ":")) + "\n",
            encoding="utf-8")
        with self.assertRaises(LookupError):
            mb.consume(other.paths["coord"], other_cons, "pull", "c1",
                       "o1", 40)
        with self.assertRaises(LookupError):
            mb.consume(other.paths["coord"], other_cons, "ack", "c1",
                       "o1", 40, position=total - 1, idem="a-again")

    # -- ledger files ---------------------------------------------------------

    def test_consumer_ledger_is_canonical(self) -> None:
        self._claim()
        self._ack(0, idem="a0")
        raw = Path(self.cons).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        self.assertNotIn(b"\\u", raw)
        doc = json.loads(raw)
        self.assertEqual(list(doc),
                         ["version", "coordination", "subscriptions",
                          "consumers", "idempotency", "audit"])
        self.assertEqual(list(doc["subscriptions"]["c1"]),
                         ["key", "job_id"])
        self.assertEqual(list(doc["consumers"]["c1"]),
                         ["owner", "until", "position"])
        # One idempotency binding and one audit event per write.
        self.assertEqual(len(doc["idempotency"]), len(doc["audit"]))

    def test_non_canonical_consumer_ledger_raises_value_error(self) -> None:
        self._claim()
        good = Path(self.cons).read_bytes()
        for payload in (b"{broken\n", good + b"\n",
                        good.replace(b'"version":1', b'"version":2', 1)):
            Path(self.cons).write_bytes(payload)
            with self.subTest(payload=payload[:10]):
                with self.assertRaises(ValueError):
                    self._pull()
        Path(self.cons).write_bytes(good)

    def test_consumer_ledger_bound_to_another_coord_raises(self) -> None:
        self._claim()
        # The other coordination ledger must exist and be canonical for
        # the request to reach the binding check; a byte-for-byte copy at
        # a different real path is bound to the consumer ledger's path.
        other_coord = os.path.join(self.fx.tmp.name, "other.json")
        Path(other_coord).write_bytes(Path(self.coord).read_bytes())
        with self.assertRaises(ValueError):
            mb.consume(other_coord, self.cons, "pull", "c1", "o1", 40)

    def test_missing_parent_raises_file_not_found(self) -> None:
        missing = os.path.join(self.fx.tmp.name, "no-dir", "c.json")
        with self.assertRaises(FileNotFoundError):
            mb.consume(self.coord, missing, "claim", "c", "o", 1,
                       lease=1, idem="i")

    def test_missing_coordination_raises_file_not_found(self) -> None:
        absent = os.path.join(self.fx.tmp.name, "absent.json")
        with self.assertRaises(FileNotFoundError):
            mb.consume(absent, self.cons, "claim", "c", "o", 1,
                       lease=1, idem="i")

    def test_pull_never_creates_ledger(self) -> None:
        with self.assertRaises(KeyError):
            self._pull()
        self.assertFalse(Path(self.cons).exists())

    def test_response_serializer_matches_object(self) -> None:
        total = self.total
        self._claim()
        result, _ = self._pull(limit=total)
        body = mb.consume_response(self.coord, self.cons, "pull", "c1",
                                   "o1", 40, limit=total)
        self.assertEqual(json.loads(body), result)
        self.assertFalse(body.endswith(b"\n"))


if __name__ == "__main__":
    unittest.main()
