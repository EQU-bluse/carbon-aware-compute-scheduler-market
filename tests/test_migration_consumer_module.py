"""Regression tests for the migration_batch/migration_consumer split.

The persistent consumer domain (ledger validation, canonical
serialization, file locking and atomic commits, claims and renewals,
pulls and acks, rejects and dead letters, status and subscription
reads) now lives in :mod:`carbon_market.migration_consumer`, while
:mod:`carbon_market.migration_batch` keeps the batch orchestration and
the narrow coordination-snapshot interface, re-exporting every public
consumer name for compatibility.

These tests pin that contract: the compat exports resolve to the very
same objects from both modules and the batch entry points stay put, a
pre-dead-letter consumer ledger keeps its exact bytes through every
read-only and non-reject path and upgrades only on the first
successful reject, and consumer operations raced against batch writes
still observe and commit one complete version at a time.
"""

from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path

from carbon_market import migration_batch as mb
from carbon_market import migration_consumer as mc
from tests.test_migration_batch import MigrationBatchTest

_WAIT = 15.0  # Bounded wait (seconds): a deadlock fails, never hangs.

_CONSUMER_NAMES = (
    "consume", "consume_response", "consumer_subscription",
    "consumer_status", "consumer_status_response",
    "consumer_dead_letters", "consumer_dead_letters_response",
    "ConsumerLedgerInvalid", "ConsumerOwnershipError",
    "ConsumerLeaseExpired", "CoordinationLedgerInvalid",
    "ConsumersLedgerMissing",
)

_BATCH_NAMES = (
    "run", "get", "search", "get_response", "search_response",
    "events", "events_response", "CoordinationLedgerMissing",
)


class CompatExportTest(unittest.TestCase):
    """Every public consumer name imports from migration_batch as before."""

    def test_consumer_names_are_reexported_identically(self) -> None:
        for name in _CONSUMER_NAMES:
            self.assertIn(name, mb.__all__)
            self.assertIs(getattr(mb, name), getattr(mc, name), name)
            self.assertEqual(getattr(mc, name).__module__,
                             "carbon_market.migration_consumer", name)

    def test_from_import_still_works(self) -> None:
        namespace: dict[str, object] = {}
        exec("from carbon_market.migration_batch import "
             + ", ".join(_CONSUMER_NAMES), namespace)
        for name in _CONSUMER_NAMES:
            self.assertIs(namespace[name], getattr(mc, name), name)

    def test_batch_entry_points_stay_in_migration_batch(self) -> None:
        for name in _BATCH_NAMES:
            self.assertIn(name, mb.__all__)
            self.assertEqual(getattr(mb, name).__module__,
                             "carbon_market.migration_batch", name)

    def test_exception_hierarchy_unchanged(self) -> None:
        self.assertTrue(issubclass(mb.ConsumerLedgerInvalid, ValueError))
        self.assertTrue(issubclass(mb.ConsumerOwnershipError,
                                   PermissionError))
        self.assertTrue(issubclass(mb.ConsumerLeaseExpired, TimeoutError))
        self.assertTrue(issubclass(mb.CoordinationLedgerInvalid,
                                   mb.ConsumerLedgerInvalid))
        self.assertTrue(issubclass(mb.ConsumersLedgerMissing,
                                   FileNotFoundError))
        self.assertTrue(issubclass(mb.CoordinationLedgerMissing,
                                   FileNotFoundError))

    def test_unknown_attribute_still_raises(self) -> None:
        with self.assertRaises(AttributeError):
            mb.no_such_consumer_thing


class _ConsumerFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.fx = MigrationBatchTest(
            "test_get_returns_copy_and_unknown_key_raises")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.fx._prepare_migrate()
        self.coord = self.fx.paths["coord"]
        self.cons = os.path.join(self.fx.tmp.name, "consumers.json")
        self._populate()

    def _populate(self) -> None:
        fx = self.fx
        fx._run(key="aaa", owner="o1", now=30)
        fx._run(key="zzz", now=31)
        fx._run(key="aaa", owner="o1", now=32,
                receipts={"j-1": fx._receipt(
                    "copy", "succeeded", "复制完成", 33)})
        fx._run(key="aaa", owner="o1", now=34,
                receipts={"j-1": fx._receipt(
                    "switch", "succeeded", "切换完成", 35)})

    def _claim(self, consumer="c1", owner="o1", now=40, lease=10 ** 6,
               idem="k1", **filters):
        return mb.consume(self.coord, self.cons, "claim", consumer, owner,
                          now, lease=lease, idem=idem, **filters)

    def _bytes(self) -> bytes:
        return Path(self.cons).read_bytes()

    def _root_fields(self) -> list[str]:
        return list(json.loads(self._bytes().decode("utf-8")))


class LegacyUpgradeTest(_ConsumerFixture):
    """A pre-dead-letter ledger upgrades only on its first real reject."""

    def test_reads_and_non_reject_writes_keep_legacy_bytes(self) -> None:
        self._claim()
        born = self._bytes()
        self.assertEqual(self._root_fields(),
                         ["version", "coordination", "subscriptions",
                          "consumers", "idempotency", "audit"])
        self.assertTrue(born.endswith(b"\n"))
        self.assertFalse(born.endswith(b"\n\n"))

        # Every read-only query leaves the file byte-for-byte untouched.
        mb.consume(self.coord, self.cons, "pull", "c1", "o1", 41)
        mb.consumer_status(self.coord, self.cons, "c1", 41)
        mb.consumer_subscription(self.coord, self.cons, "c1")
        page = mb.consumer_dead_letters(self.coord, self.cons, "c1")
        self.assertEqual(page, {"consumer": "c1", "entries": [],
                                "next": None})
        self.assertEqual(self._bytes(), born)

        # A lease-renewing repeat claim and a real ack rewrite the
        # ledger but keep the original six-field shape.
        self._claim(now=42, idem="k2")
        renewed = self._bytes()
        self.assertEqual(self._root_fields(),
                         ["version", "coordination", "subscriptions",
                          "consumers", "idempotency", "audit"])
        result, changed = mb.consume(self.coord, self.cons, "ack", "c1",
                                     "o1", 43, position=0, idem="k3")
        self.assertTrue(changed)
        self.assertEqual(result["position"], 0)
        self.assertEqual(self._root_fields(),
                         ["version", "coordination", "subscriptions",
                          "consumers", "idempotency", "audit"])
        self.assertNotEqual(renewed, born)

    def test_first_reject_upgrades_and_replays_without_writing(self) -> None:
        self._claim()
        mb.consume(self.coord, self.cons, "ack", "c1", "o1", 43,
                   position=0, idem="k3")
        before = self._bytes()

        # A failed reject leaves the pre-call bytes exactly in place.
        with self.assertRaises(ValueError):
            mb.consume(self.coord, self.cons, "reject", "c1", "o1", 44,
                       position=10 ** 6, reason="太晚", idem="k4")
        self.assertEqual(self._bytes(), before)

        # The first successful reject upgrades the document once: the
        # dead_letters section is appended last, the reason's non-ASCII
        # text is written through and the single trailing newline stays.
        result, changed = mb.consume(
            self.coord, self.cons, "reject", "c1", "o1", 45,
            position=1, reason=" 数据损坏 ", idem="k4")
        self.assertTrue(changed)
        self.assertEqual(result["position"], 1)
        self.assertEqual(result["dead_letter"]["reason"], "数据损坏")
        upgraded = self._bytes()
        self.assertEqual(self._root_fields(),
                         ["version", "coordination", "subscriptions",
                          "consumers", "idempotency", "audit",
                          "dead_letters"])
        self.assertIn("数据损坏".encode("utf-8"), upgraded)
        self.assertTrue(upgraded.endswith(b"\n"))
        self.assertFalse(upgraded.endswith(b"\n\n"))

        # The same idempotency key replays the first result and writes
        # nothing; a changed request under it is a ValueError that also
        # writes nothing.
        replay, changed = mb.consume(
            self.coord, self.cons, "reject", "c1", "o1", 45,
            position=1, reason=" 数据损坏 ", idem="k4")
        self.assertFalse(changed)
        self.assertEqual(replay, result)
        self.assertEqual(self._bytes(), upgraded)
        with self.assertRaises(ValueError):
            mb.consume(self.coord, self.cons, "reject", "c1", "o1", 46,
                       position=2, reason="变了", idem="k4")
        self.assertEqual(self._bytes(), upgraded)

        # The letter is observable through the dead-letter query and
        # the cursor advanced with it in the same commit.
        page = mb.consumer_dead_letters(self.coord, self.cons, "c1")
        self.assertEqual([entry["position"] for entry in page["entries"]],
                         [1])
        self.assertEqual(page["entries"][0]["reason"], "数据损坏")
        status = mb.consumer_status(self.coord, self.cons, "c1", 46)
        self.assertEqual(status["position"], 1)

    def test_response_serializers_match_result_objects(self) -> None:
        self._claim()
        result, _ = mb.consume(self.coord, self.cons, "pull", "c1", "o1",
                               41)
        raw = mb.consume_response(self.coord, self.cons, "pull", "c1",
                                  "o1", 41)
        self.assertIsInstance(raw, bytes)
        self.assertEqual(raw, json.dumps(
            result, ensure_ascii=False, separators=(",", ":"),
            allow_nan=False).encode("utf-8"))
        status = mb.consumer_status(self.coord, self.cons, "c1", 41)
        self.assertEqual(
            mb.consumer_status_response(self.coord, self.cons, "c1", 41),
            json.dumps(status, ensure_ascii=False, separators=(",", ":"),
                       allow_nan=False).encode("utf-8"))
        letters = mb.consumer_dead_letters(self.coord, self.cons, "c1")
        self.assertEqual(
            mb.consumer_dead_letters_response(self.coord, self.cons, "c1"),
            json.dumps(letters, ensure_ascii=False, separators=(",", ":"),
                       allow_nan=False).encode("utf-8"))


class BatchConsumerConcurrencyTest(_ConsumerFixture):
    """Consumer reads/acks raced against batch writes stay consistent."""

    def test_consumer_operations_race_batch_writes(self) -> None:
        self._claim()
        errors: list[BaseException] = []
        pulled: list[dict] = []
        writer_done = threading.Event()
        barrier = threading.Barrier(3)

        def guard(callable_) -> None:
            try:
                callable_()
            except BaseException as exc:  # noqa: BLE001 - reported below
                errors.append(exc)

        def writer() -> None:
            barrier.wait()
            fx = self.fx
            fx._run(key="bbb", owner="o1", now=50)
            fx._run(key="ccc", now=51)
            fx._run(key="bbb", owner="o1", now=52,
                    receipts={"j-1": fx._receipt(
                        "copy", "succeeded", "再次复制", 53)})
            fx._run(key="bbb", owner="o1", now=54,
                    receipts={"j-1": fx._receipt(
                        "switch", "succeeded", "再次切换", 55)})
            writer_done.set()

        def puller() -> None:
            barrier.wait()
            idem = 0
            while True:
                page, _ = mb.consume(self.coord, self.cons, "pull", "c1",
                                     "o1", 60, limit=2)
                if not page["events"]:
                    if writer_done.is_set():
                        return
                    continue
                pulled.extend(page["events"])
                idem += 1
                # Ack the page's last position; the pull/ack pair races
                # the writer's own commits under the shared lock group.
                mb.consume(self.coord, self.cons, "ack", "c1", "o1", 60,
                           position=page["events"][-1]["position"],
                           idem=f"ack-{idem}")

        def status_reader() -> None:
            barrier.wait()
            while not writer_done.is_set():
                status = mb.consumer_status(self.coord, self.cons, "c1", 60)
                self.assertEqual(status["lease"], "active")

        threads = [
            threading.Thread(target=guard, args=(writer,)),
            threading.Thread(target=guard, args=(puller,)),
            threading.Thread(target=guard, args=(status_reader,)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(_WAIT)
            self.assertFalse(thread.is_alive())

        self.assertEqual(errors, [])
        # The pulled events form one strictly increasing run of real
        # stream positions, and the final ack left the cursor on the
        # stream's tail with an empty backlog.
        positions = [event["position"] for event in pulled]
        self.assertEqual(positions, sorted(positions))
        self.assertEqual(len(positions), len(set(positions)))
        total = len(mb.events(self.coord, limit=1000)["events"])
        self.assertEqual(positions, list(range(total)))
        status = mb.consumer_status(self.coord, self.cons, "c1", 61)
        self.assertEqual(status["position"], total - 1)
        self.assertEqual(status["pending"], 0)
        self.assertIsNone(status["oldest"])
        # No reject happened, so the consumer ledger keeps its original
        # six-field shape and still loads as canonical bytes.
        self.assertEqual(self._root_fields(),
                         ["version", "coordination", "subscriptions",
                          "consumers", "idempotency", "audit"])
        self.assertTrue(self._bytes().endswith(b"\n"))


if __name__ == "__main__":
    unittest.main()
