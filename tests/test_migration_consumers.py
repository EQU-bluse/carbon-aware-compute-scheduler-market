"""Tests for the persistent migration-event consumer ledger.

Covers the three ``migration_batch.consume`` operations (claim, fetch,
confirm): the fixed subscription filters, leases and ownership,
idempotent replays, checkpoint anchoring against stream truncation or
rewrite, the exception vocabulary (KeyError, PermissionError,
TimeoutError, ValueError, LookupError, FileNotFoundError, OSError) and
the canonical consumer ledger bytes.
"""

from __future__ import annotations

import json
import os
import unittest
from pathlib import Path

from carbon_market import migration_batch as mb

from tests import test_migration_batch as _batch_tests


class MigrationConsumersTest(unittest.TestCase):
    def setUp(self) -> None:
        # Reuse the migration-batch fixture (real coordination ledger
        # plus its nine business ledgers) by composition.
        self.fx = _batch_tests.MigrationBatchTest(
            "test_claimed_active_job_is_kept")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.tmp = self.fx.tmp
        self.paths = self.fx.paths
        self.consumers = os.path.join(self.tmp.name, "consumers.json")

    def _populate(self) -> None:
        self.fx._prepare_migrate()
        # Two batches with interleaved events:
        # aaa: created@30 with an item, zzz: created and completed,
        # then aaa drives the copy receipt through.
        self.fx._run(key="aaa", owner="o1", now=30)
        self.fx._run(key="zzz", now=31)
        self.fx._run(key="aaa", owner="o1", now=32,
                     receipts={"j-1": self.fx._receipt(
                         "copy", "succeeded", "复制完成", 33)})

    def _consume(self, op: str, consumer: str, **kwargs):
        return mb.consume(self.paths["coord"], self.consumers, op,
                          consumer, **kwargs)

    def _claim(self, consumer: str = "c1", owner: str = "o1", now: int = 40,
               lease: int = 100, **kwargs):
        return self._consume("claim", consumer, owner=owner, now=now,
                             lease=lease, **kwargs)

    def _ledger(self) -> dict:
        return json.loads(Path(self.consumers).read_text("utf-8"))

    # -- claim ---------------------------------------------------------------

    def test_first_claim_returns_true_and_persists_subscription(self) -> None:
        self._populate()
        result, created = self._claim()
        self.assertTrue(created)
        self.assertEqual(result, {
            "consumer": "c1", "key": None, "job_id": None,
            "owner": "o1", "until": 140, "position": -1})
        ledger = self._ledger()
        self.assertEqual(list(ledger), ["version", "consumers",
                                        "idempotency", "audit"])
        record = ledger["consumers"]["c1"]
        self.assertEqual(record["key"], None)
        self.assertEqual(record["anchor"], None)
        self.assertEqual(ledger["audit"][0]["action"], "created")
        self.assertTrue(Path(self.consumers).read_bytes().endswith(b"\n"))

    def test_renewal_by_owner_returns_false_and_extends_lease(self) -> None:
        self._populate()
        first, created_first = self._claim(now=40, lease=100)
        second, created_second = self._claim(now=50, lease=200)
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(second["until"], 250)
        self.assertEqual(self._ledger()["audit"][1]["action"], "renewed")
        self.assertNotEqual(first["until"], second["until"])

    def test_filter_is_fixed_and_cannot_change(self) -> None:
        self._populate()
        self._claim(key="aaa")
        for kwargs in ({"key": "zzz"}, {"job_id": "j-1"}):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    self._claim(**kwargs)
        # The bytes are untouched after a refused change.
        before = Path(self.consumers).read_bytes()
        with self.assertRaises(ValueError):
            self._claim(key="zzz")
        self.assertEqual(Path(self.consumers).read_bytes(), before)

    def test_other_owner_within_lease_raises_permission(self) -> None:
        self._populate()
        self._claim(now=40, lease=100)
        with self.assertRaises(PermissionError):
            self._claim(owner="o2", now=90)

    def test_same_owner_after_strict_expiry_raises_timeout(self) -> None:
        self._populate()
        self._claim(now=40, lease=100)
        with self.assertRaises(TimeoutError):
            self._claim(now=141)

    def test_takeover_after_expiry_keeps_cursor(self) -> None:
        self._populate()
        self._claim(now=40, lease=100)
        self._consume("confirm", "c1", owner="o1", now=41, position=0)
        result, created = self._claim(owner="o2", now=141, lease=100)
        self.assertFalse(created)
        self.assertEqual(result["owner"], "o2")
        self.assertEqual(result["position"], 0)
        self.assertEqual(self._ledger()["audit"][-1]["action"],
                         "taken_over")

    # -- fetch ---------------------------------------------------------------

    def test_fetch_returns_matching_events_after_position(self) -> None:
        self._populate()
        self._claim()
        page, created = self._consume("fetch", "c1", owner="o1", now=41)
        self.assertFalse(created)
        self.assertEqual(list(page), ["events", "next"])
        positions = [event["position"] for event in page["events"]]
        self.assertEqual(positions, list(range(len(positions))))
        self.assertIsNone(page["next"])
        # Fetch never advances the checkpoint.
        self.assertEqual(self._ledger()["consumers"]["c1"]["position"], -1)

    def test_fetch_filters_by_fixed_batch_and_job(self) -> None:
        self._populate()
        self._claim("fixed", key="zzz")
        self._claim("jobonly", job_id="j-1")
        page, _ = self._consume("fetch", "fixed", owner="o1", now=41)
        self.assertTrue(page["events"])
        self.assertTrue(all(event["key"] == "zzz"
                            for event in page["events"]))
        page, _ = self._consume("fetch", "jobonly", owner="o1", now=41)
        self.assertTrue(page["events"])
        self.assertTrue(all(event["job_id"] == "j-1"
                            for event in page["events"]))

    def test_fetch_pagination_matches_events_semantics(self) -> None:
        self._populate()
        self._claim()
        page, _ = self._consume("fetch", "c1", owner="o1", now=41, limit=2)
        self.assertEqual([event["position"] for event in page["events"]],
                         [0, 1])
        self.assertEqual(page["next"], 1)
        page, _ = self._consume("fetch", "c1", owner="o1", now=41,
                                cursor=1, limit=2)
        self.assertEqual([event["position"] for event in page["events"]],
                         [2, 3])
        self.assertEqual(page["next"], 3)

    def test_fetch_after_confirm_starts_after_checkpoint(self) -> None:
        self._populate()
        self._claim()
        self._consume("confirm", "c1", owner="o1", now=41, position=1)
        page, _ = self._consume("fetch", "c1", owner="o1", now=42)
        self.assertEqual(page["events"][0]["position"], 2)

    def test_fetch_cursor_cannot_page_behind_checkpoint(self) -> None:
        self._populate()
        self._claim()
        self._consume("confirm", "c1", owner="o1", now=41, position=3)
        with self.assertRaises(ValueError):
            self._consume("fetch", "c1", owner="o1", now=42, cursor=1)

    def test_fetch_unknown_consumer_is_key_error(self) -> None:
        self._populate()
        with self.assertRaises(KeyError):
            self._consume("fetch", "ghost", owner="o1", now=41)

    def test_fetch_ownership_and_lease(self) -> None:
        self._populate()
        self._claim(now=40, lease=100)
        with self.assertRaises(PermissionError):
            self._consume("fetch", "c1", owner="o2", now=50)
        with self.assertRaises(TimeoutError):
            self._consume("fetch", "c1", owner="o1", now=141)

    # -- confirm -------------------------------------------------------------

    def test_confirm_advances_to_real_matching_event(self) -> None:
        self._populate()
        self._claim("fixed", key="zzz")
        zzz_positions = [
            event["position"] for event in mb.events(
                self.paths["coord"], limit=1000, key="zzz")["events"]]
        result, created = self._consume(
            "confirm", "fixed", owner="o1", now=41,
            position=zzz_positions[0])
        self.assertTrue(created)
        self.assertEqual(result, {"consumer": "fixed",
                                  "position": zzz_positions[0]})
        self.assertEqual(self._ledger()["audit"][-1]["action"],
                         "confirmed")

    def test_in_place_confirm_replay_writes_nothing(self) -> None:
        self._populate()
        self._claim()
        self._consume("confirm", "c1", owner="o1", now=41, position=0)
        before = Path(self.consumers).read_bytes()
        result, created = self._consume(
            "confirm", "c1", owner="o1", now=42, position=0)
        self.assertFalse(created)
        self.assertEqual(result, {"consumer": "c1", "position": 0})
        self.assertEqual(Path(self.consumers).read_bytes(), before)

    def test_confirm_backwards_is_value_error(self) -> None:
        self._populate()
        self._claim()
        self._consume("confirm", "c1", owner="o1", now=41, position=2)
        with self.assertRaises(ValueError):
            self._consume("confirm", "c1", owner="o1", now=42, position=1)

    def test_confirm_past_tail_is_value_error(self) -> None:
        self._populate()
        self._claim()
        total = len(mb.events(self.paths["coord"], limit=1000)["events"])
        with self.assertRaises(ValueError):
            self._consume("confirm", "c1", owner="o1", now=41,
                          position=total)

    def test_confirm_non_matching_event_is_value_error(self) -> None:
        self._populate()
        self._claim("fixed", key="zzz")
        with self.assertRaises(ValueError):
            self._consume("confirm", "fixed", owner="o1", now=41,
                          position=0)

    def test_confirm_requires_owner_and_valid_lease(self) -> None:
        self._populate()
        self._claim(now=40, lease=100)
        with self.assertRaises(PermissionError):
            self._consume("confirm", "c1", owner="o2", now=50, position=0)
        with self.assertRaises(TimeoutError):
            self._consume("confirm", "c1", owner="o1", now=141,
                          position=0)

    # -- idempotency ---------------------------------------------------------

    def test_equivalent_claim_key_returns_stored_result_and_false(self) -> None:
        self._populate()
        first, created_first = self._claim(idempotency_key="k1", now=40)
        second, created_second = self._claim(
            idempotency_key="k1", now=99, lease=5)
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first, second)

    def test_changed_claim_request_under_same_key_is_value_error(self) -> None:
        self._populate()
        self._claim(idempotency_key="k1", key="aaa")
        with self.assertRaises(ValueError):
            self._claim(idempotency_key="k1", key="zzz")
        with self.assertRaises(ValueError):
            self._claim(idempotency_key="k1", owner="o2")

    def test_equivalent_confirm_key_returns_stored_result_and_false(self) -> None:
        self._populate()
        self._claim()
        first, created_first = self._consume(
            "confirm", "c1", owner="o1", now=41, position=2,
            idempotency_key="ck")
        second, created_second = self._consume(
            "confirm", "c1", owner="o1", now=42, position=2,
            idempotency_key="ck")
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first, second)

    def test_confirm_key_reused_with_changed_position_is_value_error(self):
        self._populate()
        self._claim()
        self._consume("confirm", "c1", owner="o1", now=41, position=2,
                      idempotency_key="ck")
        with self.assertRaises(ValueError):
            self._consume("confirm", "c1", owner="o1", now=42, position=3,
                          idempotency_key="ck")

    def test_same_idempotency_key_across_ops_is_value_error(self) -> None:
        self._populate()
        self._claim(idempotency_key="shared")
        with self.assertRaises(ValueError):
            self._consume("confirm", "c1", owner="o1", now=41, position=0,
                          idempotency_key="shared")

    def test_lease_gate_runs_before_claim_idempotent_replay(self) -> None:
        self._populate()
        self._claim(idempotency_key="k1", now=40, lease=100)
        # The old owner cannot turn an expired claim into an idempotent
        # replay.
        with self.assertRaises(TimeoutError):
            self._claim(now=141, idempotency_key="k1")
        # A different owner inside the lease gets PermissionError even
        # with the bound key.
        with self.assertRaises(PermissionError):
            self._claim(owner="o2", now=50, idempotency_key="k1")
        # The equivalent replay by the still-current owner returns the
        # stored result and extends nothing in the response object.
        before = self._claim(now=55, idempotency_key="k1")
        self.assertFalse(before[1])
        self.assertEqual(before[0]["until"], 140)

    def test_claim_idempotent_replay_after_crash_returns_first_result(
            self) -> None:
        self._populate()
        # A retry at a later moment within the lease returns the exact
        # first-claim snapshot (until 140, not 199).
        self._claim(idempotency_key="k1", now=40, lease=100)
        result, created = self._claim(now=99, lease=99,
                                      idempotency_key="k1")
        self.assertFalse(created)
        self.assertEqual(result["until"], 140)

    # -- stream rollback detection -------------------------------------------

    def test_truncated_confirmed_event_is_lookup_error(self) -> None:
        self._populate()
        self._claim()
        total = len(mb.events(self.paths["coord"], limit=1000)["events"])
        self._consume("confirm", "c1", owner="o1", now=41,
                      position=total - 1)
        # Run a later batch that appends, then truncate the events
        # section to simulate history loss, rebuilding canonical bytes.
        data = json.loads(Path(self.paths["coord"]).read_text("utf-8"))
        data["events"] = data["events"][: total - 1]
        for index, event in enumerate(data["events"]):
            event["position"] = index
        Path(self.paths["coord"]).write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"))
            + "\n", encoding="utf-8")
        with self.assertRaises(LookupError):
            self._consume("fetch", "c1", owner="o1", now=42)
        # The cursor is not reset by the failed read.
        self.assertEqual(self._ledger()["consumers"]["c1"]["position"],
                         total - 1)

    def test_rewritten_confirmed_event_is_lookup_error(self) -> None:
        self._populate()
        self._claim()
        self._consume("confirm", "c1", owner="o1", now=41, position=2)
        data = json.loads(Path(self.paths["coord"]).read_text("utf-8"))
        data["events"][2]["at"] = 999
        Path(self.paths["coord"]).write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"))
            + "\n", encoding="utf-8")
        with self.assertRaises(LookupError):
            self._consume("fetch", "c1", owner="o1", now=42)

    # -- files and arguments -------------------------------------------------

    def test_missing_coordination_ledger_is_file_not_found(self) -> None:
        self._populate()
        missing = os.path.join(self.tmp.name, "absent.json")
        with self.assertRaises(FileNotFoundError):
            mb.consume(missing, self.consumers, "claim", "c1",
                       owner="o1", now=1, lease=10)

    def test_missing_consumer_parent_is_file_not_found(self) -> None:
        self._populate()
        nested = os.path.join(self.tmp.name, "no-dir", "consumers.json")
        with self.assertRaises(FileNotFoundError):
            mb.consume(self.paths["coord"], nested, "claim", "c1",
                       owner="o1", now=1, lease=10)
        self.assertFalse(os.path.exists(os.path.dirname(nested)))

    def test_missing_consumer_ledger_starts_empty(self) -> None:
        self._populate()
        result, created = self._claim()
        self.assertTrue(created)
        self.assertEqual(result["consumer"], "c1")

    def test_bad_arguments_are_value_errors(self) -> None:
        self._populate()
        with self.assertRaises(ValueError):
            mb.consume(self.paths["coord"], self.consumers, "rewind", "c1",
                       owner="o1", now=1, lease=10)
        with self.assertRaises(ValueError):
            mb.consume(self.paths["coord"], self.consumers, "claim", "",
                       owner="o1", now=1, lease=10)
        with self.assertRaises(ValueError):
            mb.consume(self.paths["coord"], self.consumers, "claim", "c1",
                       owner="o1", now=-1, lease=10)
        with self.assertRaises(ValueError):
            mb.consume(self.paths["coord"], self.consumers, "claim", "c1",
                       owner="o1", now=1, lease=0)
        with self.assertRaises(ValueError):
            mb.consume(self.paths["coord"], self.paths["coord"],
                       "claim", "c1", owner="o1", now=1, lease=10)
        with self.assertRaises(ValueError):
            mb.consume(self.paths["coord"], self.consumers, "fetch", "c1",
                       owner="o1", now=1, limit=0)

    def test_invalid_consumer_ledger_bytes_are_value_error(self) -> None:
        self._populate()
        self._claim()
        Path(self.consumers).write_bytes(b"{not json\n")
        with self.assertRaises(ValueError):
            self._consume("fetch", "c1", owner="o1", now=42)

    def test_response_serializer_returns_compact_json(self) -> None:
        self._populate()
        self._claim()
        body = mb.consume_response(
            self.paths["coord"], self.consumers, "fetch", "c1",
            owner="o1", now=41, limit=2)
        self.assertFalse(body.endswith(b"\n"))
        decoded = json.loads(body)
        self.assertEqual(list(decoded), ["events", "next"])
        self.assertEqual([event["position"] for event in decoded["events"]],
                         [0, 1])

    def test_reclaimed_subscription_redelivers_unconfirmed_events(self) -> None:
        self._populate()
        self._claim(now=40, lease=100)
        self._consume("confirm", "c1", owner="o1", now=41, position=1)
        self._claim(owner="o2", now=141, lease=100)
        page, _ = self._consume("fetch", "c1", owner="o2", now=142)
        self.assertEqual(page["events"][0]["position"], 2)


if __name__ == "__main__":
    unittest.main()
