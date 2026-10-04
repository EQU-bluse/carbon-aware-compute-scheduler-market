"""Regression tests for the migration_consumer module split.

The persistent consumer domain (claim, pull, ack, reject, dead
letters, status and subscription reads, the consumer ledger's
validation, canonical serialization, locking and atomic commits) now
lives in :mod:`carbon_market.migration_consumer`, while
:mod:`carbon_market.migration_batch` stays the compatibility entry
point and re-exports the consumer public surface unchanged.

Covered here:

* the compatibility exports: every documented consumer name imports
  from ``carbon_market.migration_batch`` exactly as before and is the
  same object the new module defines, in either module import order,
  with the exception hierarchy and ``__all__`` unchanged;
* identical requests through either entry point produce identical
  return objects, identical compact UTF-8 response bytes and identical
  canonical ledger bytes;
* the pre-dead-letter consumer ledger keeps its exact six-field bytes
  through claim, pull, ack and every read-only query and is upgraded
  only by the first successful reject, and a baseline coordination
  ledger without an events section stays read-only compatible and is
  never rewritten by a consumer call;
* consumer operations raced against batch writes (``run`` calls
  appending progress events) stay linearizable: the consumer ledger
  and the coordination ledger both remain canonical, the cursor never
  regresses, and a reject's cursor advance, dead letter and
  idempotency binding still commit atomically.
"""

from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
import threading
import unittest
from pathlib import Path

from carbon_market import migration_batch as mb
from carbon_market import migration_consumer as mc
from tests.test_migration_batch import MigrationBatchTest

_WAIT = 30.0  # Bounded wait (seconds): a deadlock fails, never hangs.

_CONSUMER_NAMES = ("consume", "consume_response", "consumer_subscription",
                   "consumer_status", "consumer_status_response",
                   "consumer_dead_letters", "consumer_dead_letters_response",
                   "ConsumerLedgerInvalid", "ConsumerOwnershipError",
                   "ConsumerLeaseExpired", "CoordinationLedgerInvalid",
                   "ConsumersLedgerMissing")

_BATCH_NAMES = ("run", "get", "search", "get_response", "search_response",
                "events", "events_response", "CoordinationLedgerMissing")


class CompatExportTest(unittest.TestCase):
    """The compat entry re-exports the consumer surface unchanged."""

    def test_consumer_names_are_the_same_objects(self) -> None:
        for name in _CONSUMER_NAMES:
            self.assertIs(getattr(mb, name), getattr(mc, name), name)

    def test_batch_names_stay_batch_defined(self) -> None:
        for name in ("run", "get", "search", "get_response",
                     "search_response", "events", "events_response"):
            self.assertFalse(hasattr(mc, name), name)
            self.assertEqual(getattr(mb, name).__module__,
                             "carbon_market.migration_batch", name)
        for name in ("consume", "consume_response", "consumer_status",
                     "consumer_dead_letters"):
            self.assertEqual(getattr(mb, name).__module__,
                             "carbon_market.migration_consumer", name)

    def test_all_is_unchanged(self) -> None:
        self.assertEqual(
            mb.__all__,
            ["run", "get", "search", "get_response", "search_response",
             "events", "events_response", "consume", "consume_response",
             "consumer_subscription", "consumer_status",
             "consumer_status_response", "consumer_dead_letters",
             "consumer_dead_letters_response", "ConsumerLedgerInvalid",
             "ConsumerOwnershipError", "ConsumerLeaseExpired",
             "CoordinationLedgerInvalid", "CoordinationLedgerMissing",
             "ConsumersLedgerMissing"])
        for name in _CONSUMER_NAMES:
            self.assertIn(name, mc.__all__)

    def test_exception_hierarchy_is_preserved(self) -> None:
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
        self.assertFalse(issubclass(mb.CoordinationLedgerMissing,
                                    mb.ConsumerLedgerInvalid))

    def test_import_order_and_from_import(self) -> None:
        root = Path(__file__).resolve().parent.parent
        names = ", ".join(_CONSUMER_NAMES + _BATCH_NAMES)
        programs = [
            "from carbon_market import migration_consumer\n"
            "from carbon_market import migration_batch as mb\n"
            f"for n in {(_CONSUMER_NAMES + _BATCH_NAMES)!r}: getattr(mb, n)\n",
            "from carbon_market.migration_batch import " + names + "\n",
            "from carbon_market.migration_consumer import "
            + ", ".join(_CONSUMER_NAMES) + "\n",
        ]
        for program in programs:
            result = subprocess.run(
                [sys.executable, "-c", program], cwd=root,
                capture_output=True, text=True)
            self.assertEqual(result.returncode, 0,
                             f"{program!r}: {result.stderr}")


class _ConsumerFixture(unittest.TestCase):
    """One populated coordination stream and an empty consumer path."""

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

    def _claim(self, module=mb, consumer="c1", owner="o1", now=40,
               lease=100, idem="k1", **filters):
        return module.consume(self.coord, self.cons, "claim", consumer,
                              owner, now, lease=lease, idem=idem, **filters)

    def _pull(self, module=mb, consumer="c1", owner="o1", now=40,
              **kwargs):
        return module.consume(self.coord, self.cons, "pull", consumer,
                              owner, now, **kwargs)

    def _ack(self, position, module=mb, consumer="c1", owner="o1", now=40,
             idem=None):
        return module.consume(self.coord, self.cons, "ack", consumer,
                              owner, now, position=position, idem=idem)

    def _reject(self, position, reason="坏", module=mb, consumer="c1",
                owner="o1", now=40, idem="r"):
        return module.consume(self.coord, self.cons, "reject", consumer,
                              owner, now, position=position, reason=reason,
                              idem=idem)


class ModuleEquivalenceTest(_ConsumerFixture):
    """The same requests through either entry point agree byte-for-byte."""

    def test_identical_requests_identical_results_and_bytes(self) -> None:
        def sequence(module) -> tuple[list, list[bytes]]:
            results: list = []
            results.append(module.consume(
                self.coord, self.cons, "claim", "c1", "o1", 40,
                key="aaa", lease=100, idem="k1"))
            results.append(module.consume(
                self.coord, self.cons, "pull", "c1", "o1", 41, limit=2))
            results.append(module.consume(
                self.coord, self.cons, "ack", "c1", "o1", 42, position=0,
                idem="a0"))
            results.append(module.consume(
                self.coord, self.cons, "reject", "c1", "o1", 43,
                position=1, reason=" 拒绝：凭证过期 ", idem="r1"))
            results.append(module.consumer_status(
                self.coord, self.cons, "c1", 44))
            results.append(module.consumer_subscription(
                self.coord, self.cons, "c1"))
            results.append(module.consumer_dead_letters(
                self.coord, self.cons, "c1"))
            responses = [
                module.consume_response(
                    self.coord, self.cons, "pull", "c1", "o1", 45),
                module.consumer_status_response(
                    self.coord, self.cons, "c1", 45),
                module.consumer_dead_letters_response(
                    self.coord, self.cons, "c1"),
            ]
            return results, responses

        first_results, first_responses = sequence(mb)
        first_bytes = Path(self.cons).read_bytes()
        os.unlink(self.cons)
        second_results, second_responses = sequence(mc)
        second_bytes = Path(self.cons).read_bytes()
        self.assertEqual(first_results, second_results)
        self.assertEqual(first_responses, second_responses)
        self.assertEqual(first_bytes, second_bytes)
        # The canonical consumer ledger: compact, non-ASCII written
        # through, exactly one trailing newline.
        self.assertTrue(first_bytes.endswith(b"\n"))
        self.assertFalse(first_bytes.endswith(b"\n\n"))
        self.assertNotIn(b"\\u", first_bytes)
        self.assertNotIn(b": ", first_bytes)


class LegacyLedgerUpgradeTest(_ConsumerFixture):
    """Old ledger shapes stay read-only compatible and upgrade once."""

    def test_read_only_queries_never_create_the_ledger(self) -> None:
        for call in (
                lambda: self._pull(),
                lambda: mb.consumer_status(self.coord, self.cons, "c1", 40),
                lambda: mb.consumer_subscription(
                    self.coord, self.cons, "c1"),
                lambda: mb.consumer_dead_letters(
                    self.coord, self.cons, "c1")):
            with self.assertRaises(KeyError):
                call()
            self.assertFalse(Path(self.cons).exists())

    def test_legacy_ledger_upgrades_only_on_first_reject(self) -> None:
        self._claim()
        self._ack(0, idem="a0")
        before = Path(self.cons).read_bytes()
        self.assertEqual(
            list(json.loads(before)),
            ["version", "coordination", "subscriptions", "consumers",
             "idempotency", "audit"])
        # Every read-only query and every no-write replay keeps the
        # exact pre-dead-letter bytes.
        self._pull()
        mb.consumer_status(self.coord, self.cons, "c1", 40)
        mb.consumer_subscription(self.coord, self.cons, "c1")
        self.assertEqual(
            mb.consumer_dead_letters(self.coord, self.cons, "c1"),
            {"consumer": "c1", "entries": [], "next": None})
        self._claim()  # idempotent replay
        self._ack(0, idem="a0")  # idempotent replay
        self.assertEqual(Path(self.cons).read_bytes(), before)
        # The first successful reject upgrades the document once.
        self._reject(1, idem="r1")
        upgraded = Path(self.cons).read_bytes()
        self.assertEqual(
            list(json.loads(upgraded)),
            ["version", "coordination", "subscriptions", "consumers",
             "idempotency", "audit", "dead_letters"])
        self.assertTrue(upgraded.endswith(b"\n"))
        self.assertNotIn(b"\\u", upgraded)
        # Later writes keep the seven-field shape; readers accept it.
        self._ack(2, idem="a2")
        self.assertIn("dead_letters",
                      json.loads(Path(self.cons).read_text("utf-8")))
        self._pull()
        mb.consumer_dead_letters_response(self.coord, self.cons, "c1")

    def test_legacy_coordination_ledger_stays_read_only(self) -> None:
        # A baseline coordination ledger without the events section
        # keeps validating byte-for-byte and a consumer never rewrites
        # a coordination byte.
        doc = json.loads(Path(self.coord).read_text("utf-8"))
        del doc["events"]
        legacy = (json.dumps(doc, ensure_ascii=False,
                             separators=(",", ":")) + "\n").encode("utf-8")
        Path(self.coord).write_bytes(legacy)
        self.assertEqual(mb.events(self.coord),
                         {"events": [], "next": None})
        self._claim()
        result, changed = self._pull()
        self.assertEqual(result["events"], [])
        self.assertIsNone(result["next"])
        self.assertFalse(changed)
        status = mb.consumer_status(self.coord, self.cons, "c1", 40)
        self.assertEqual(status["pending"], 0)
        self.assertIsNone(status["oldest"])
        self.assertEqual(Path(self.coord).read_bytes(), legacy)


class ConsumerBatchConcurrencyTest(_ConsumerFixture):
    """Consumer operations raced against batch writes stay consistent."""

    def _race(self, consumer_body, *, writers=2, rounds=12) -> list:
        errors: list[BaseException] = []
        barrier = threading.Barrier(writers + 1)
        done = threading.Event()

        def writer(index: int) -> None:
            try:
                barrier.wait(timeout=_WAIT)
                for i in range(rounds):
                    self.fx._run(key=f"w-{index}-{i}", owner="wo",
                                 now=1000 + index * rounds + i)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)
            finally:
                done.set()

        def consumer() -> None:
            try:
                barrier.wait(timeout=_WAIT)
                consumer_body(done)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=writer, args=(i,))
                   for i in range(writers)]
        threads.append(threading.Thread(target=consumer))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(_WAIT)
            self.assertFalse(thread.is_alive(), "deadlocked thread")
        return errors

    def test_pull_and_ack_race_batch_writes(self) -> None:
        self._claim(lease=100000)
        acked: list[int] = []

        def body(done: threading.Event) -> None:
            while not done.is_set():
                page, _ = self._pull(now=41, limit=3)
                for event in page["events"]:
                    result, changed = self._ack(
                        event["position"], now=42,
                        idem=f"a-{event['position']}")
                    self.assertTrue(changed)
                    acked.append(event["position"])
                page, _ = self._pull(now=43, limit=3)

        errors = self._race(body)
        self.assertEqual(errors, [])
        # The cursor advanced monotonically through real stream
        # positions and the persisted state matches the last ack.
        self.assertEqual(acked, sorted(acked))
        status = mb.consumer_status(self.coord, self.cons, "c1", 50)
        if acked:
            self.assertEqual(status["position"], acked[-1])
        stream = mb.events(self.coord, limit=1000)["events"]
        matching = [event for event in stream
                    if status["position"] is None
                    or event["position"] > status["position"]]
        self.assertEqual(status["pending"], len(matching))
        # Both ledgers still load through the fully validating public
        # readers, and the consumer ledger is canonical compact UTF-8.
        raw = Path(self.cons).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        self.assertNotIn(b"\\u", raw)
        self._pull(now=44)
        mb.events_response(self.coord)

    def test_reject_commits_atomically_racing_batch_writes(self) -> None:
        self._claim(lease=100000)
        letters: list[dict] = []

        def body(done: threading.Event) -> None:
            while not done.is_set():
                page, _ = self._pull(now=41, limit=1)
                if not page["events"]:
                    continue
                target = page["events"][0]["position"]
                result, changed = self._reject(
                    target, reason=" 坏事件 ", now=42, idem=f"r-{target}")
                self.assertTrue(changed)
                self.assertEqual(result["position"], target)
                letters.append(result["dead_letter"])

        errors = self._race(body)
        self.assertEqual(errors, [])
        # Every successful reject recorded exactly one dead letter in
        # original-position order and moved the cursor to it, all in
        # one commit: the persisted section matches the observed
        # results one to one.
        page = mb.consumer_dead_letters(self.coord, self.cons, "c1",
                                        limit=1000)
        self.assertEqual(page["entries"], letters)
        positions = [letter["position"] for letter in letters]
        self.assertEqual(positions, sorted(positions))
        status = mb.consumer_status(self.coord, self.cons, "c1", 50)
        if positions:
            self.assertEqual(status["position"], positions[-1])
        # A rejected event is never pulled again and the reasons kept
        # their trimmed non-ASCII form in the canonical bytes.
        for letter in letters:
            self.assertEqual(letter["reason"], "坏事件")
        raw = Path(self.cons).read_bytes()
        self.assertNotIn(b"\\u", raw)
        self.assertIn("坏事件".encode("utf-8"), raw)

    def test_read_only_queries_race_claims_and_writes(self) -> None:
        self._claim(lease=100000)
        self._ack(0, idem="a0")
        snapshot: list = []

        def body(done: threading.Event) -> None:
            while not done.is_set():
                status = mb.consumer_status(self.coord, self.cons, "c1",
                                            41)
                self.assertEqual(status["lease"], "active")
                self.assertEqual(status["position"], 0)
                snapshot.append(copy.deepcopy(status))
                mb.consumer_subscription(self.coord, self.cons, "c1")
                mb.consumer_dead_letters(self.coord, self.cons, "c1")

        ledger_before = Path(self.cons).read_bytes()
        errors = self._race(body)
        self.assertEqual(errors, [])
        self.assertTrue(snapshot)
        # The read-only queries never wrote: the consumer ledger keeps
        # its exact bytes through the whole race.
        self.assertEqual(Path(self.cons).read_bytes(), ledger_before)


if __name__ == "__main__":
    unittest.main()
