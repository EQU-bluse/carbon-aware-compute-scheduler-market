"""Deterministic concurrency and commit-failure tests for completion.

These tests exercise the public completion-driven capacity handoff:
``completion.complete`` racing ``market.clear_live``, ``rebalance.evaluate``
and ``rebalance.apply`` calls that carry the same completion ledger, plus
injected I/O failures inside the completion commit. Every race is
controlled by an explicit one-shot gate installed on an internal hook
point (never by sleeps or lucky scheduling), every wait is bounded so a
lock-ordering regression fails as a deadlock instead of hanging, and all
result verification goes through the public read entries
(``completion.get``, ``market.clear_live`` probes) or the persisted
ledger bytes -- never private in-memory state.

The fixture fills one resource version exactly (capacity 10, work 10):
job j-1 occupies all of ``r-1`` version 1 and sits in a stable terminal
state, ready to complete. Job j-9 (already completed on another
resource) guarantees the completion ledger exists before the raced
commit, so a pre-commit reader observes a complete pre-commit snapshot
rather than a missing file.
"""

from __future__ import annotations

import errno
import json
import os
import stat
import tempfile
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from carbon_market import completion as completion_module
from carbon_market import dispatch as dispatch_module
from carbon_market import execution as execution_module
from carbon_market import execution_sync as execution_sync_module
from carbon_market import jobs as jobs_module
from carbon_market import market as market_module
from carbon_market import rebalance as rebalance_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.completion import complete, get
from carbon_market.market import clear_live
from carbon_market.rebalance import apply, evaluate

# Every synchronization point waits at most this long; a lock-ordering
# regression surfaces as a failed bounded wait, never as a hung suite.
_TIMEOUT = 30.0


def _job(job_id: str, regions: list[str], residency: list[str],
         **overrides: object) -> dict[str, object]:
    job: dict[str, object] = {
        "job_id": job_id,
        "work": 10,
        "deadline": 100,
        "regions": regions,
        "residency": residency,
        "max_cost": 1000,
        "carbon_cap": 1000,
    }
    job.update(overrides)
    return job


def _resource(resource_id: str, region: str, capacity: int,
              residency: list[str], **overrides: object) -> dict[str, object]:
    resource: dict[str, object] = {
        "resource_id": resource_id,
        "region": region,
        "capacity": capacity,
        "start": 0,
        "end": 500,
        "unit_cost": 5,
        "carbon_intensity": 8,
        "residency": residency,
    }
    resource.update(overrides)
    return resource


def _signal(region: str, **overrides: object) -> dict[str, object]:
    signal: dict[str, object] = {
        "region": region,
        "observed": 0,
        "expires": 500,
        "mix": {"solar": 10000},
        "unit_cost": 5,
        "carbon_intensity": 8,
    }
    signal.update(overrides)
    return signal


class _Outcome:
    """The result or error one worker thread produced."""

    def __init__(self) -> None:
        self.result: object = None
        self.error: BaseException | None = None


def _spawn(call) -> tuple[threading.Thread, _Outcome]:
    outcome = _Outcome()

    def run() -> None:
        try:
            outcome.result = call()
        except BaseException as exc:  # reported by the main thread
            outcome.error = exc

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, outcome


def _await(thread: threading.Thread, what: str) -> None:
    thread.join(_TIMEOUT)
    if thread.is_alive():
        raise AssertionError(
            f"{what} did not finish within {_TIMEOUT}s; possible deadlock")


class _OneShotGate:
    """Deterministic barrier inside a patched internal hook.

    The first call blocks until the main thread releases the gate; later
    calls pass through. The gated thread already holds every lock the
    patched entry acquired, so the competing thread is forced to observe
    either the complete pre-commit or the complete post-commit state.
    """

    def __init__(self, wrapped) -> None:
        self._wrapped = wrapped
        self._fired = False
        self._lock = threading.Lock()
        self.entered = threading.Event()
        self.release = threading.Event()

    def __call__(self, *args, **kwargs):
        with self._lock:
            fire = not self._fired
            self._fired = True
        if fire:
            self.entered.set()
            if not self.release.wait(_TIMEOUT):
                raise AssertionError("the gate was never released")
        return self._wrapped(*args, **kwargs)


class _RaceFixtureBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        base = self.tmp.name
        self.jobs = os.path.join(base, "jobs.json")
        self.supply = os.path.join(base, "supply.json")
        self.signals = os.path.join(base, "signals.json")
        self.trades = os.path.join(base, "trades.json")
        self.dispatch = os.path.join(base, "dispatch.json")
        self.execution = os.path.join(base, "execution.json")
        self.sync = os.path.join(base, "sync.json")
        self.advice = os.path.join(base, "advice.json")
        self.intents = os.path.join(base, "intents.json")
        self.completions = os.path.join(base, "completions.json")

    # -- fixture ---------------------------------------------------------

    def _build_fixture(self, *, bootstrap_completion: bool = True) -> None:
        """One resource version exactly filled by a finished job.

        r-1 (eu-north) has capacity 10 and every job has work 10, so
        j-1's trade occupies the whole version. j-2 and j-4 trade on the
        large r-9 (zz) and receive migrate advice targeting r-1 while it
        is still free; j-1 then fills r-1 and runs to a stable terminal
        state. j-9's completion (when requested) creates the completion
        ledger before the raced commit.
        """
        jobs_module.submit(
            self.jobs, _job("j-1", ["eu-north", "us-west"], ["eu-north"]),
            "jk-1")
        jobs_module.submit(
            self.jobs, _job("j-2", ["eu-north", "zz"], ["eu-north"]),
            "jk-2")
        jobs_module.submit(
            self.jobs, _job("j-3", ["eu-north"], ["eu-north"]), "jk-3")
        jobs_module.submit(
            self.jobs, _job("j-4", ["eu-north", "zz"], ["eu-north"]),
            "jk-4")
        jobs_module.submit(
            self.jobs, _job("j-9", ["zz"], ["zz"]), "jk-9")
        resources_module.publish(
            self.supply, _resource("r-1", "eu-north", 10, ["eu-north"]),
            "rk-1")
        resources_module.publish(
            self.supply,
            _resource("r-9", "zz", 1000, ["eu-north", "zz"], unit_cost=2,
                      carbon_intensity=2), "rk-9")
        signals_module.publish(self.signals, _signal("eu-north"), "sk-1")
        signals_module.publish(
            self.signals, _signal("zz", unit_cost=2, carbon_intensity=2),
            "sk-zz")
        # j-9 trades and runs to a stable terminal state first, so the
        # dispatch and execution ledgers exist for the evaluations below.
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-9", "t-9", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-9", "d-9", 10)
        self._finish("j-9", "9")
        # j-2 and j-4 trade while zz is the cheaper region.
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-2", "t-2", 12)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-2", "d-2", 12)
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-4", "t-4", 13)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-4", "d-4", 13)
        # eu-north becomes the cleanest and cheapest region from 15 on.
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=15, unit_cost=1,
                    carbon_intensity=1), "sk-2")
        # Migrate advice towards r-1 recorded while r-1 is still free.
        evaluate(self.jobs, self.supply, self.signals, self.trades,
                 self.dispatch, self.execution, self.advice, "j-2", "a-2",
                 30)
        evaluate(self.jobs, self.supply, self.signals, self.trades,
                 self.dispatch, self.execution, self.advice, "j-4", "a-4",
                 31)
        # j-1 fills r-1 version 1 exactly and reaches a stable terminal
        # state; its last evidence is the synchronization finish at 70.
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-1", "t-1", 40)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-1", "d-1", 40)
        self._finish("j-1", "1")
        if bootstrap_completion:
            complete(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.completions,
                     "j-9", "x-9", 70, "succeeded", 1, 1)

    def _finish(self, job_id: str, suffix: str) -> None:
        dispatch_module.claim(self.dispatch, job_id, f"c-{suffix}",
                              f"owner-{suffix}", 30, 60)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, job_id,
                              f"p-{suffix}", f"owner-{suffix}", None, 61)
        execution_module.record(self.execution, job_id, 1, f"s1-{suffix}",
                                f"owner-{suffix}", "stage", "succeeded",
                                "staged", 62)
        execution_module.record(self.execution, job_id, 1, f"s2-{suffix}",
                                f"owner-{suffix}", "start", "succeeded",
                                "started", 63)
        execution_sync_module.run(self.execution, self.dispatch, self.sync,
                                  "sync-1", f"b-{suffix}", 70, 50)

    # -- raced calls -------------------------------------------------------

    def _complete_j1(self):
        return complete(self.jobs, self.supply, self.signals, self.trades,
                        self.dispatch, self.execution, self.completions,
                        "j-1", "x-1", 70, "succeeded", 90, 120)

    def _clear_j3(self):
        return clear_live(self.jobs, self.supply, self.signals, self.trades,
                          "j-3", "t-3", 72, self.completions)

    def _evaluate_j1(self):
        return evaluate(self.jobs, self.supply, self.signals, self.trades,
                        self.dispatch, self.execution, self.advice, "j-1",
                        "a-1", 72, self.completions)

    def _apply_j2(self):
        return apply(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.advice,
                     self.intents, "j-2", "a-2", "k-2", 72,
                     self.completions)

    def _apply_j4(self):
        return apply(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.advice,
                     self.intents, "j-4", "a-4", "k-4", 81,
                     self.completions)

    # -- public-state assertions ------------------------------------------

    def _read(self, path: str) -> dict:
        return json.loads(Path(path).read_bytes().decode("utf-8"))

    def _assert_single_completion(self, record: dict) -> None:
        # The completion record, its idempotency binding and its audit
        # event each appear exactly once, readable through the public
        # entries and the persisted ledger bytes.
        self.assertEqual(get(self.completions, "j-1"), record)
        data = self._read(self.completions)
        for section in ("completions", "idempotency", "audit"):
            self.assertEqual(sorted(data[section]), ["x-1", "x-9"],
                             section)
        self.assertEqual(data["completions"]["x-1"], record)
        self.assertEqual(data["audit"]["x-1"]["result"], record)
        self.assertEqual(data["idempotency"]["x-1"], {
            "job_id": "j-1", "at": 70, "outcome": "succeeded",
            "actual_cost": 90, "actual_carbon": 120})

    def _assert_capacity_stays_full(self, job_id: str, submit_key: str,
                                    trade_key: str, at: int) -> None:
        # A further job still finds r-1 version 1 fully occupied: the
        # handoff never oversold the published capacity.
        jobs_module.submit(
            self.jobs, _job(job_id, ["eu-north"], ["eu-north"]),
            submit_key)
        with self.assertRaises(LookupError):
            clear_live(self.jobs, self.supply, self.signals, self.trades,
                       job_id, trade_key, at, self.completions)


class CompletionClearRaceTest(_RaceFixtureBase):
    """completion.complete racing market.clear_live on one full version."""

    def test_complete_committing_first_then_clear_creates_unique_trade(
            self) -> None:
        self._build_fixture()
        # The gate holds complete() inside its commit, with every lock
        # taken and the completion ledger locked exclusively; the clear
        # can only proceed after the commit is durable.
        gate = _OneShotGate(completion_module._commit_file)
        with mock.patch.object(completion_module, "_commit_file", gate):
            complete_thread, complete_out = _spawn(self._complete_j1)
            self.assertTrue(gate.entered.wait(_TIMEOUT),
                            "complete did not reach the commit barrier")
            try:
                clear_thread, clear_out = _spawn(self._clear_j3)
            finally:
                gate.release.set()
            _await(complete_thread, "completion.complete")
            _await(clear_thread, "market.clear_live")

        self.assertIsNone(complete_out.error, repr(complete_out.error))
        self.assertIsNone(clear_out.error, repr(clear_out.error))
        record, created = complete_out.result
        self.assertTrue(created)
        self.assertEqual(record["current"],
                         {"resource_id": "r-1", "version": 1})
        trade, trade_created = clear_out.result
        self.assertTrue(trade_created)
        self.assertEqual(trade["resource_id"], "r-1")
        self.assertEqual(trade["version"], 1)

        self._assert_single_completion(record)
        trades = self._read(self.trades)
        self.assertEqual(trades["idempotency"]["t-3"],
                         {"job_id": "j-3", "at": 72})
        self.assertEqual(trades["audit"]["t-3"]["job_id"], "j-3")
        self.assertEqual(trades["trades"]["j-3"]["resource_id"], "r-1")

        # Replays of both raced calls return the stored results without
        # rewriting a byte: one creation, equivalent replays afterwards.
        completions_bytes = Path(self.completions).read_bytes()
        replayed, again = self._complete_j1()
        self.assertFalse(again)
        self.assertEqual(replayed, record)
        self.assertEqual(Path(self.completions).read_bytes(),
                         completions_bytes)
        trades_bytes = Path(self.trades).read_bytes()
        replayed_trade, again = self._clear_j3()
        self.assertFalse(again)
        self.assertEqual(replayed_trade, trade)
        self.assertEqual(Path(self.trades).read_bytes(), trades_bytes)

        # j-1 released exactly its own booking and j-3 took it: the
        # version is full again, never oversold.
        self._assert_capacity_stays_full("j-5", "jk-5", "t-5", 73)

    def test_clear_first_observes_pre_commit_state_then_lookup_error(
            self) -> None:
        self._build_fixture()
        trades_before = Path(self.trades).read_bytes()
        # The gate holds clear_live() after it acquired every lock but
        # before it reads the completion union; complete() then blocks on
        # the completion ledger's exclusive lock until the clear has
        # finished with the pre-commit snapshot.
        gate = _OneShotGate(market_module._load_completion_union)
        with mock.patch.object(market_module, "_load_completion_union",
                               gate):
            clear_thread, clear_out = _spawn(self._clear_j3)
            self.assertTrue(gate.entered.wait(_TIMEOUT),
                            "clear_live did not reach the snapshot barrier")
            try:
                complete_thread, complete_out = _spawn(self._complete_j1)
            finally:
                gate.release.set()
            _await(clear_thread, "market.clear_live")
            _await(complete_thread, "completion.complete")

        # Pre-commit the version is still full: the clear is refused and
        # leaves no trade, binding or audit event behind.
        self.assertIsInstance(clear_out.error, LookupError)
        self.assertEqual(Path(self.trades).read_bytes(), trades_before)
        trades = self._read(self.trades)
        self.assertNotIn("j-3", trades["trades"])
        self.assertNotIn("t-3", trades["idempotency"])
        self.assertNotIn("t-3", trades["audit"])

        self.assertIsNone(complete_out.error, repr(complete_out.error))
        record, created = complete_out.result
        self.assertTrue(created)
        self._assert_single_completion(record)

        # After the commit the same clear request succeeds exactly once.
        trade, created = self._clear_j3()
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-1")
        replayed, again = self._clear_j3()
        self.assertFalse(again)
        self.assertEqual(replayed, trade)
        self._assert_capacity_stays_full("j-5", "jk-5", "t-5", 73)


class CompletionEvaluateRaceTest(_RaceFixtureBase):
    """completion.complete racing rebalance.evaluate."""

    def test_complete_committing_first_then_evaluate_rejects_completed(
            self) -> None:
        self._build_fixture()
        advice_before = Path(self.advice).read_bytes()
        gate = _OneShotGate(completion_module._commit_file)
        with mock.patch.object(completion_module, "_commit_file", gate):
            complete_thread, complete_out = _spawn(self._complete_j1)
            self.assertTrue(gate.entered.wait(_TIMEOUT),
                            "complete did not reach the commit barrier")
            try:
                eval_thread, eval_out = _spawn(self._evaluate_j1)
            finally:
                gate.release.set()
            _await(complete_thread, "completion.complete")
            _await(eval_thread, "rebalance.evaluate")

        self.assertIsNone(complete_out.error, repr(complete_out.error))
        record, created = complete_out.result
        self.assertTrue(created)
        self._assert_single_completion(record)

        # The completed job is refused new advice with ValueError and
        # the advice ledger never gained a record for it.
        self.assertIsInstance(eval_out.error, ValueError)
        self.assertIn("already completed", str(eval_out.error))
        self.assertEqual(Path(self.advice).read_bytes(), advice_before)

        # Another job may still evaluate and sees the released capacity:
        # r-1 is feasible again and the advice is to migrate onto it.
        advice, advice_created = evaluate(
            self.jobs, self.supply, self.signals, self.trades,
            self.dispatch, self.execution, self.advice, "j-2", "a-2b", 73,
            self.completions)
        self.assertTrue(advice_created)
        self.assertEqual(advice["recommendation"], "migrate")
        self.assertEqual(advice["target"],
                         {"resource_id": "r-1", "version": 1})

    def test_evaluate_first_observes_unfinished_snapshot(self) -> None:
        self._build_fixture()
        advice_before = Path(self.advice).read_bytes()
        # The gate holds evaluate() with every lock taken before it reads
        # the completion snapshot; complete() blocks until evaluate has
        # refused on the pre-commit (unfinished) state.
        gate = _OneShotGate(rebalance_module._load_completion_snapshot)
        with mock.patch.object(rebalance_module,
                               "_load_completion_snapshot", gate):
            eval_thread, eval_out = _spawn(self._evaluate_j1)
            self.assertTrue(gate.entered.wait(_TIMEOUT),
                            "evaluate did not reach the snapshot barrier")
            try:
                complete_thread, complete_out = _spawn(self._complete_j1)
            finally:
                gate.release.set()
            _await(eval_thread, "rebalance.evaluate")
            _await(complete_thread, "completion.complete")

        # Pre-commit the job is merely finished, not completed: a
        # different ValueError, and still no advice record.
        self.assertIsInstance(eval_out.error, ValueError)
        self.assertIn("finished booking", str(eval_out.error))
        self.assertEqual(Path(self.advice).read_bytes(), advice_before)

        self.assertIsNone(complete_out.error, repr(complete_out.error))
        record, created = complete_out.result
        self.assertTrue(created)
        self._assert_single_completion(record)

        # The committed snapshot flips the refusal reason atomically.
        with self.assertRaisesRegex(ValueError, "already completed"):
            evaluate(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.advice, "j-1",
                     "a-1b", 73, self.completions)
        self.assertEqual(Path(self.advice).read_bytes(), advice_before)


class CompletionApplyRaceTest(_RaceFixtureBase):
    """completion.complete racing rebalance.apply reservations."""

    def test_complete_committing_first_then_apply_reserves_released(
            self) -> None:
        self._build_fixture()
        gate = _OneShotGate(completion_module._commit_file)
        with mock.patch.object(completion_module, "_commit_file", gate):
            complete_thread, complete_out = _spawn(self._complete_j1)
            self.assertTrue(gate.entered.wait(_TIMEOUT),
                            "complete did not reach the commit barrier")
            try:
                apply_thread, apply_out = _spawn(self._apply_j2)
            finally:
                gate.release.set()
            _await(complete_thread, "completion.complete")
            _await(apply_thread, "rebalance.apply")

        self.assertIsNone(complete_out.error, repr(complete_out.error))
        record, created = complete_out.result
        self.assertTrue(created)
        self._assert_single_completion(record)

        # The reservation observes the post-commit snapshot: j-1's
        # occupancy is released and j-2 migrates onto r-1 version 1.
        self.assertIsNone(apply_out.error, repr(apply_out.error))
        intent, intent_created = apply_out.result
        self.assertTrue(intent_created)
        self.assertEqual(intent["source"],
                         {"resource_id": "r-9", "version": 1})
        self.assertEqual(intent["target"],
                         {"resource_id": "r-1", "version": 1})
        self.assertEqual(intent["reserved"], "reserved")

        # One intent, one idempotency binding, one audit event; a replay
        # returns the stored record without rewriting a byte.
        intents = self._read(self.intents)
        self.assertEqual(sorted(intents["intents"]), ["j-2"])
        self.assertEqual(sorted(intents["idempotency"]), ["k-2"])
        self.assertEqual(sorted(intents["audit"]), ["k-2"])
        intents_bytes = Path(self.intents).read_bytes()
        replayed, again = self._apply_j2()
        self.assertFalse(again)
        self.assertEqual(replayed, intent)
        self.assertEqual(Path(self.intents).read_bytes(), intents_bytes)

        # The released capacity is reserved exactly once: j-4's migrate
        # reservation for the same version is refused.
        with self.assertRaises(LookupError):
            self._apply_j4()

    def test_apply_first_full_version_then_lookup_error(self) -> None:
        self._build_fixture()
        # The gate holds apply() with every lock taken before it reads
        # the completion snapshot; complete() blocks until apply has been
        # refused against the still-full version.
        gate = _OneShotGate(rebalance_module._load_completion_snapshot)
        with mock.patch.object(rebalance_module,
                               "_load_completion_snapshot", gate):
            apply_thread, apply_out = _spawn(self._apply_j2)
            self.assertTrue(gate.entered.wait(_TIMEOUT),
                            "apply did not reach the snapshot barrier")
            try:
                complete_thread, complete_out = _spawn(self._complete_j1)
            finally:
                gate.release.set()
            _await(apply_thread, "rebalance.apply")
            _await(complete_thread, "completion.complete")

        # Pre-commit the version is full: the reservation is refused and
        # the intent ledger is never created.
        self.assertIsInstance(apply_out.error, LookupError)
        self.assertFalse(Path(self.intents).exists())

        self.assertIsNone(complete_out.error, repr(complete_out.error))
        record, created = complete_out.result
        self.assertTrue(created)
        self._assert_single_completion(record)

        # Retrying the same request after the commit reserves exactly
        # once; the released capacity cannot be reserved twice.
        intent, created = self._apply_j2()
        self.assertTrue(created)
        self.assertEqual(intent["target"],
                         {"resource_id": "r-1", "version": 1})
        replayed, again = self._apply_j2()
        self.assertFalse(again)
        self.assertEqual(replayed, intent)
        with self.assertRaises(LookupError):
            self._apply_j4()


class CompletionDefaultBehaviorTest(_RaceFixtureBase):
    """Omitting the completions argument keeps the historical behavior."""

    def test_entries_without_completions_argument_are_unchanged(self) -> None:
        self._build_fixture()
        self._complete_j1()
        # clear_live without the completion ledger still sees j-1's
        # booking occupying the full version.
        with self.assertRaises(LookupError):
            clear_live(self.jobs, self.supply, self.signals, self.trades,
                       "j-3", "t-3", 72)
        # evaluate without it refuses the finished booking, not the
        # completed job.
        with self.assertRaisesRegex(ValueError, "finished booking"):
            evaluate(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.advice, "j-1",
                     "a-1", 72)
        # apply without it still finds r-1 fully reserved by j-1.
        with self.assertRaises(LookupError):
            apply(self.jobs, self.supply, self.signals, self.trades,
                  self.dispatch, self.execution, self.advice, self.intents,
                  "j-2", "a-2", "k-2", 72)
        # None of the refusals wrote anything.
        self.assertFalse(Path(self.intents).exists())
        trades = self._read(self.trades)
        self.assertNotIn("j-3", trades["trades"])


class CompletionCommitFaultTest(_RaceFixtureBase):
    """Injected commit failures leave only pre- or post-commit bytes."""

    # -- fault injection points -------------------------------------------

    @staticmethod
    def _fault_temp_write():
        # The same-directory temporary cannot be written: the file
        # descriptor comes back read-only and the write/flush fails.
        real_mkstemp = tempfile.mkstemp

        def bad_mkstemp(*args, **kwargs):
            fd, path = real_mkstemp(*args, **kwargs)
            os.close(fd)
            return os.open(path, os.O_RDONLY), path

        return mock.patch.object(completion_module.tempfile, "mkstemp",
                                 bad_mkstemp)

    @staticmethod
    def _fault_file_fsync():
        real_fsync = os.fsync

        def bad_fsync(fd: int) -> None:
            if stat.S_ISREG(os.fstat(fd).st_mode):
                raise OSError(errno.EIO, "injected file fsync failure")
            return real_fsync(fd)

        return mock.patch.object(completion_module.os, "fsync", bad_fsync)

    @staticmethod
    def _fault_replace():
        def bad_replace(src, dst) -> None:
            raise OSError(errno.EIO, "injected atomic replace failure")

        return mock.patch.object(completion_module.os, "replace",
                                 bad_replace)

    @staticmethod
    def _fault_dir_fsync():
        # Only the commit's own directory fsync fails; the rollback's
        # directory fsync succeeds so the recovery can complete.
        real_fsync = os.fsync
        fired = []

        def bad_fsync(fd: int) -> None:
            if not fired and stat.S_ISDIR(os.fstat(fd).st_mode):
                fired.append(True)
                raise OSError(errno.EIO, "injected directory fsync "
                                        "failure")
            return real_fsync(fd)

        return mock.patch.object(completion_module.os, "fsync", bad_fsync)

    # -- shared assertions --------------------------------------------------

    def _assert_fault_then_retry_once(self, fault, *,
                                      preexisting: bool) -> None:
        before = (Path(self.completions).read_bytes()
                  if preexisting else None)
        with fault():
            with self.assertRaises(OSError):
                self._complete_j1()

        # The ledger holds either the exact pre-call bytes or one
        # complete commit -- here the commit failed, so only the
        # pre-call state may remain, with no temporary fragment and no
        # half record publicly readable.
        if before is None:
            self.assertFalse(Path(self.completions).exists())
        else:
            self.assertEqual(Path(self.completions).read_bytes(), before)
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.startswith(".completion-")]
        self.assertEqual(leftovers, [])
        with self.assertRaises(KeyError):
            get(self.completions, "j-1")

        # Retrying with the same idempotency key yields exactly one
        # creation; further calls are equivalent replays.
        record, created = self._complete_j1()
        self.assertTrue(created)
        replayed, again = self._complete_j1()
        self.assertFalse(again)
        self.assertEqual(replayed, record)

        expected_keys = ["x-1"] + (["x-9"] if preexisting else [])
        data = self._read(self.completions)
        for section in ("completions", "idempotency", "audit"):
            self.assertEqual(sorted(data[section]), sorted(expected_keys),
                             section)
        self.assertEqual(get(self.completions, "j-1"), record)

        # The capacity release took effect exactly once: j-3 takes the
        # freed slot and the version is full again afterwards.
        trade, created = self._clear_j3()
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-1")
        self._assert_capacity_stays_full("j-5", "jk-5", "t-5", 73)

    # -- fresh ledger (no pre-call bytes) -----------------------------------

    def test_temp_write_failure_on_fresh_ledger(self) -> None:
        self._build_fixture(bootstrap_completion=False)
        self._assert_fault_then_retry_once(self._fault_temp_write,
                                           preexisting=False)

    def test_file_fsync_failure_on_fresh_ledger(self) -> None:
        self._build_fixture(bootstrap_completion=False)
        self._assert_fault_then_retry_once(self._fault_file_fsync,
                                           preexisting=False)

    def test_atomic_replace_failure_on_fresh_ledger(self) -> None:
        self._build_fixture(bootstrap_completion=False)
        self._assert_fault_then_retry_once(self._fault_replace,
                                           preexisting=False)

    def test_directory_fsync_failure_on_fresh_ledger(self) -> None:
        self._build_fixture(bootstrap_completion=False)
        self._assert_fault_then_retry_once(self._fault_dir_fsync,
                                           preexisting=False)

    # -- existing ledger (pre-call bytes must survive) ----------------------

    def test_temp_write_failure_preserves_existing_ledger(self) -> None:
        self._build_fixture()
        self._assert_fault_then_retry_once(self._fault_temp_write,
                                           preexisting=True)

    def test_file_fsync_failure_preserves_existing_ledger(self) -> None:
        self._build_fixture()
        self._assert_fault_then_retry_once(self._fault_file_fsync,
                                           preexisting=True)

    def test_atomic_replace_failure_preserves_existing_ledger(self) -> None:
        self._build_fixture()
        self._assert_fault_then_retry_once(self._fault_replace,
                                           preexisting=True)

    def test_directory_fsync_failure_preserves_existing_ledger(
            self) -> None:
        self._build_fixture()
        self._assert_fault_then_retry_once(self._fault_dir_fsync,
                                           preexisting=True)


if __name__ == "__main__":
    unittest.main()
