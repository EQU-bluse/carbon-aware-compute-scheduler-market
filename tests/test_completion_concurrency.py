"""Deterministic concurrency and commit-failure tests for completion.

These tests exercise the public completion handover only:
``completion.complete`` racing ``market.clear_live`` and
``rebalance.evaluate``/``rebalance.apply`` over one shared completion
snapshot, plus injected failures of the completion commit (temporary
file write, file fsync, atomic replace, directory fsync). Every race is
staged with explicit gates and lock-attempt probes patched into the
production modules, so each interleaving is decided by the test, not by
the scheduler; every wait is bounded so a lock-ordering regression
fails loudly instead of hanging. All result verification goes through
the public entry points (``completion.get``/``search``,
``market.clear_live``, ``rebalance.evaluate``/``apply``) and the
on-disk ledger bytes -- never through private in-memory state. No
interface, ledger format, exception type or sequential-call result is
changed.
"""

from __future__ import annotations

import contextlib
import json
import os
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
from carbon_market.completion import complete, get, search
from carbon_market.market import clear_live
from carbon_market.rebalance import apply, evaluate

_WAIT = 15.0  # Bounded wait (seconds): a deadlock fails, never hangs.


def _resource(resource_id: str = "r-1", region: str = "eu-north",
              **overrides: object) -> dict[str, object]:
    resource: dict[str, object] = {
        "resource_id": resource_id,
        "region": region,
        "capacity": 100,
        "start": 0,
        "end": 500,
        "unit_cost": 5,
        "carbon_intensity": 8,
        "residency": ["eu-north", "us-west"] if region == "us-west"
        else [region],
    }
    resource.update(overrides)
    return resource


def _signal(region: str = "eu-north", **overrides: object) -> dict[str, object]:
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


def _job(job_id: str = "j-1", **overrides: object) -> dict[str, object]:
    job: dict[str, object] = {
        "job_id": job_id,
        "work": 10,
        "deadline": 100,
        "regions": ["eu-north", "us-west"],
        "residency": ["eu-north"],
        "max_cost": 1000,
        "carbon_cap": 1000,
    }
    job.update(overrides)
    return job


class _Gate:
    """One-shot rendezvous between a patched production call and the
    main test thread: the call arrives and blocks until the main thread
    opens the gate. Bounded waits turn a missed rendezvous or a
    deadlock into a loud failure instead of a hang."""

    def __init__(self) -> None:
        self.arrived = threading.Event()
        self.released = threading.Event()

    def wait_inside(self) -> None:
        self.arrived.set()
        if not self.released.wait(_WAIT):
            raise RuntimeError("test gate was never released")

    def await_arrival(self, test: unittest.TestCase, label: str) -> None:
        test.assertTrue(self.arrived.wait(_WAIT),
                        f"timed out waiting for {label} to reach the gate")

    def open(self) -> None:
        self.released.set()


def _lock_probe(real_lock, realpath: str, event: threading.Event,
                thread_name: str | None = None):
    """Wrap a module's lock context manager so ``event`` fires when the
    given thread is about to block on ``realpath``'s flock. The probe
    only observes; the underlying lock behavior is unchanged."""
    @contextlib.contextmanager
    def probed(path: str, *, shared: bool = False):
        if path == realpath and (thread_name is None
                                 or threading.current_thread().name
                                 == thread_name):
            event.set()
        with real_lock(path, shared=shared):
            yield
    return probed


class _RaceTestBase(unittest.TestCase):
    """Shared paths, thread harness and lifecycle helpers."""

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
        self._outcomes: dict[str, object] = {}

    # -- thread harness -------------------------------------------------

    def _spawn(self, name: str, fn) -> threading.Thread:
        def target() -> None:
            try:
                self._outcomes[name] = fn()
            except BaseException as exc:  # asserted in the main thread
                self._outcomes[name] = exc

        thread = threading.Thread(target=target, name=name, daemon=True)
        thread.start()
        return thread

    def _join(self, *threads: threading.Thread) -> None:
        for thread in threads:
            thread.join(_WAIT)
        for thread in threads:
            self.assertFalse(thread.is_alive(),
                             f"{thread.name} is stuck (possible deadlock)")

    def _result(self, name: str):
        outcome = self._outcomes[name]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    # -- business lifecycle helpers --------------------------------------

    def _finish_job(self, job_id: str, index: int, trade_at: int = 10,
                    sync_now: int = 70) -> None:
        """Trade, dispatch, execute and synchronize one job to a stable
        terminal state, exactly as the sequential completion tests do."""
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   job_id, f"t-{index}", trade_at)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, job_id, f"d-{index}", trade_at)
        dispatch_module.claim(self.dispatch, job_id, f"c-{index}",
                              f"owner-{index}", 30, 60)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, job_id,
                              f"p-{index}", f"owner-{index}", None, 61)
        execution_module.record(self.execution, job_id, 1, f"s1-{index}",
                                f"owner-{index}", "stage", "succeeded",
                                "staged", 62)
        execution_module.record(self.execution, job_id, 1, f"s2-{index}",
                                f"owner-{index}", "start", "succeeded",
                                "started", 63)
        execution_sync_module.run(self.execution, self.dispatch, self.sync,
                                  "sync-1", f"b-{index}", sync_now, 50)

    def _complete(self, job_id: str, key: str, at: int = 70,
                  completions: str | None = None):
        return complete(self.jobs, self.supply, self.signals, self.trades,
                        self.dispatch, self.execution,
                        completions or self.completions, job_id, key, at,
                        "succeeded", 1, 1)

    def _clear(self, job_id: str, key: str, at: int = 70,
               completions: str | None = None):
        return clear_live(self.jobs, self.supply, self.signals, self.trades,
                          job_id, key, at,
                          self.completions if completions is None
                          else completions)

    # -- shared assertions ------------------------------------------------

    def _assert_completion_recorded_once(self, job_id: str, key: str,
                                         expected_keys: list[str],
                                         completions: str | None = None
                                         ) -> None:
        """The completion record, its idempotency binding and its audit
        event each appear exactly once, read back through the public
        query entries and the canonical ledger bytes."""
        path = completions or self.completions
        record = get(path, job_id)
        self.assertEqual(record["job_id"], job_id)
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        for section in ("completions", "idempotency", "audit"):
            self.assertEqual(list(data[section]), expected_keys)
        # The public paginated query returns exactly the recorded jobs.
        page = search(path)
        self.assertEqual(
            [entry["job_id"] for entry in page["entries"]],
            sorted(record["job_id"]
                   for record in data["completions"].values()))
        self.assertIsNone(page["next"])
        self.assertEqual(data["completions"][key]["job_id"], job_id)
        self.assertEqual(data["audit"][key]["result"],
                         data["completions"][key])
        self.assertEqual(data["audit"][key]["request"],
                         data["idempotency"][key])

    def _assert_capacity_not_oversold(self, job_id: str, key: str,
                                      at: int = 70) -> None:
        """One more job wanting the released version finds it exactly
        full again: the release happened once, never twice."""
        with self.assertRaises(LookupError):
            self._clear(job_id, key, at)


class CompletionClearLiveRaceTest(_RaceTestBase):
    """``completion.complete`` vs ``market.clear_live`` carrying the
    same completion ledger, over a resource version exactly filled by
    the completing job (capacity 10, work 10)."""

    def setUp(self) -> None:
        super().setUp()
        # j-0 occupies its own resource and completes first, so the
        # completion ledger already exists when the race starts.
        jobs_module.submit(self.jobs,
                           _job("j-0", regions=["zz-boot"],
                                residency=["zz-boot"]), "jk-0")
        for index in (1, 2, 3):
            jobs_module.submit(self.jobs, _job(f"j-{index}"), f"jk-{index}")
        resources_module.publish(
            self.supply,
            _resource("r-0", region="zz-boot", capacity=10,
                      residency=["zz-boot"]), "rk-0")
        resources_module.publish(
            self.supply, _resource("r-1", capacity=10), "rk-1")
        signals_module.publish(self.signals, _signal(), "sk-1")
        signals_module.publish(self.signals, _signal("zz-boot"), "sk-0")
        self._finish_job("j-0", 0)
        self._complete("j-0", "x-0")
        # j-1 fills r-1's version exactly and reaches its terminal state.
        self._finish_job("j-1", 1)

    def test_complete_first_then_clear_observes_release(self) -> None:
        # The completion commits while a concurrent clear is queued on
        # the completion ledger's shared lock: the clear must observe
        # the complete post-commit snapshot and book the freed capacity.
        gate = _Gate()
        clear_waiting = threading.Event()
        completions_real = os.path.realpath(self.completions)

        real_commit = completion_module._commit_file

        def gated_commit(realpath, payload, old_bytes):
            gate.wait_inside()
            return real_commit(realpath, payload, old_bytes)

        probed_clear_lock = _lock_probe(market_module._clear_lock,
                                        completions_real, clear_waiting)
        with mock.patch.object(completion_module, "_commit_file",
                               gated_commit), \
                mock.patch.object(market_module, "_clear_lock",
                                  probed_clear_lock):
            t_complete = self._spawn(
                "complete", lambda: self._complete("j-1", "x-1"))
            gate.await_arrival(self, "completion commit")
            t_clear = self._spawn(
                "clear", lambda: self._clear("j-2", "t-2"))
            self.assertTrue(clear_waiting.wait(_WAIT),
                            "clear_live never queued on the completion "
                            "lock")
            gate.open()
            self._join(t_complete, t_clear)

        record, created = self._result("complete")
        self.assertTrue(created)
        self.assertEqual(record["current"],
                         {"resource_id": "r-1", "version": 1})
        trade, created = self._result("clear")
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-1")
        self.assertEqual(trade["version"], 1)

        self._assert_completion_recorded_once("j-1", "x-1", ["x-0", "x-1"])
        # The trade's idempotency binding replays; a second binding for
        # the same job is refused; the released capacity is spent once.
        replayed, created = self._clear("j-2", "t-2")
        self.assertFalse(created)
        self.assertEqual(replayed, trade)
        with self.assertRaises(ValueError):
            self._clear("j-2", "t-2-other")
        self._assert_capacity_not_oversold("j-3", "t-3")

    def test_clear_first_observes_uncommitted_snapshot(self) -> None:
        # The clear reads the completion union while the completion is
        # queued on the ledger's exclusive lock: it must observe the
        # complete pre-commit snapshot (capacity still sold out) and be
        # refused, then the completion commits and exactly one retry
        # books the freed capacity.
        gate = _Gate()
        complete_waiting = threading.Event()
        completions_real = os.path.realpath(self.completions)
        trades_before = Path(self.trades).read_bytes()

        real_union = market_module._load_completion_union
        gated_once = {"done": False}

        def gated_union(paths, accepted, history):
            if not gated_once["done"]:
                gated_once["done"] = True
                gate.wait_inside()
            return real_union(paths, accepted, history)

        probed_lock = _lock_probe(completion_module._lock, completions_real,
                                  complete_waiting)
        with mock.patch.object(market_module, "_load_completion_union",
                               gated_union), \
                mock.patch.object(completion_module, "_lock", probed_lock):
            t_clear = self._spawn(
                "clear", lambda: self._clear("j-2", "t-2"))
            gate.await_arrival(self, "clear_live snapshot read")
            t_complete = self._spawn(
                "complete", lambda: self._complete("j-1", "x-1"))
            self.assertTrue(complete_waiting.wait(_WAIT),
                            "complete never queued on the completion "
                            "lock")
            gate.open()
            self._join(t_clear, t_complete)

        self.assertIsInstance(self._outcomes["clear"], LookupError)
        record, created = self._result("complete")
        self.assertTrue(created)
        self.assertEqual(record["job_id"], "j-1")
        # The refused clear wrote nothing: the trades ledger is
        # byte-for-byte the pre-race one, so the same idempotency key is
        # still free and books exactly once on retry.
        self.assertEqual(Path(self.trades).read_bytes(), trades_before)
        trade, created = self._clear("j-2", "t-2")
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-1")
        replayed, created = self._clear("j-2", "t-2")
        self.assertFalse(created)
        self.assertEqual(replayed, trade)

        self._assert_completion_recorded_once("j-1", "x-1", ["x-0", "x-1"])
        self._assert_capacity_not_oversold("j-3", "t-3")

    def test_concurrent_duplicate_complete_records_once(self) -> None:
        # Two completions of the same job under the same idempotency
        # key: exactly one creates, the other replays the stored record.
        barrier = threading.Barrier(2)

        def call():
            barrier.wait(_WAIT)
            return self._complete("j-1", "x-1")

        first = self._spawn("complete-1", call)
        second = self._spawn("complete-2", call)
        self._join(first, second)

        outcomes = [self._result("complete-1"), self._result("complete-2")]
        created_flags = sorted(created for _record, created in outcomes)
        self.assertEqual(created_flags, [False, True])
        self.assertEqual(outcomes[0][0], outcomes[1][0])
        self._assert_completion_recorded_once("j-1", "x-1", ["x-0", "x-1"])
        # The capacity released by the single record is spent once.
        trade, created = self._clear("j-2", "t-2")
        self.assertTrue(created)
        self._assert_capacity_not_oversold("j-3", "t-3")


class CompletionRebalanceRaceTest(_RaceTestBase):
    """``completion.complete`` vs ``rebalance.apply``/``evaluate`` over
    the capacity the completing job releases."""

    def setUp(self) -> None:
        super().setUp()
        # j-0 completes on its own resource so the completion ledger
        # exists before the race.
        jobs_module.submit(self.jobs,
                           _job("j-0", regions=["zz-boot"],
                                residency=["zz-boot"]), "jk-0")
        jobs_module.submit(self.jobs, _job("j-1"), "jk-1")
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        jobs_module.submit(self.jobs,
                           _job("j-3", regions=["eu-north"]), "jk-3")
        jobs_module.submit(self.jobs,
                           _job("j-4", regions=["us-west"],
                                residency=["us-west"]), "jk-4")
        resources_module.publish(
            self.supply,
            _resource("r-0", region="zz-boot", capacity=10,
                      residency=["zz-boot"]), "rk-0")
        resources_module.publish(
            self.supply, _resource("r-1", capacity=10), "rk-1")
        resources_module.publish(
            self.supply, _resource("r-2", region="us-west", capacity=10),
            "rk-2")
        resources_module.publish(
            self.supply,
            _resource("r-3", region="us-west", capacity=10,
                      residency=["us-west"]), "rk-3")
        # eu-north starts unattractive (v1) and turns cheapest at 5 (v2);
        # us-west is mid-priced from moment 4 on.
        signals_module.publish(
            self.signals, _signal(expires=15), "sk-1a")
        signals_module.publish(
            self.signals,
            _signal("us-west", observed=4, unit_cost=3,
                    carbon_intensity=2), "sk-2")
        signals_module.publish(self.signals, _signal("zz-boot"), "sk-0")
        self._finish_job("j-0", 0)
        self._complete("j-0", "x-0")
        # j-2 books r-2 while eu-north is still expensive, then earns a
        # migrate-to-r-1 advice while r-1 is still empty.
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-2", "t-2", 4)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-2", "d-2", 4)
        signals_module.publish(
            self.signals,
            _signal(observed=5, unit_cost=1, carbon_intensity=1), "sk-1b")
        advice, created = evaluate(self.jobs, self.supply, self.signals,
                                   self.trades, self.dispatch,
                                   self.execution, self.advice, "j-2",
                                   "a-1", 6)
        self.assertTrue(created)
        self.assertEqual(advice["recommendation"], "migrate")
        self.assertEqual(advice["target"],
                         {"resource_id": "r-1", "version": 1})
        # j-1 fills r-1's version exactly and reaches its terminal state.
        self._finish_job("j-1", 1)

    def _apply(self, key: str = "r-1res", at: int = 80):
        return apply(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.advice,
                     self.intents, "j-2", "a-1", key, at, self.completions)

    def _evaluate(self, key: str = "a-2", at: int = 80):
        return evaluate(self.jobs, self.supply, self.signals, self.trades,
                        self.dispatch, self.execution, self.advice, "j-2",
                        key, at, self.completions)

    def test_apply_first_observes_uncompleted_snapshot(self) -> None:
        # The reservation reads the completion snapshot while the
        # completion is queued behind it: r-1 is still sold out, so the
        # migrate advice cannot be applied; the completion then commits
        # and one retry reserves the freed capacity.
        gate = _Gate()
        complete_waiting = threading.Event()
        advice_real = os.path.realpath(self.advice)

        real_snapshot = rebalance_module._load_completion_snapshot
        gated_once = {"done": False}

        def gated_snapshot(realpath, accepted, history):
            if not gated_once["done"]:
                gated_once["done"] = True
                gate.wait_inside()
            return real_snapshot(realpath, accepted, history)

        probed_lock = _lock_probe(completion_module._lock, advice_real,
                                  complete_waiting)
        with mock.patch.object(rebalance_module, "_load_completion_snapshot",
                               gated_snapshot), \
                mock.patch.object(completion_module, "_lock", probed_lock):
            t_apply = self._spawn("apply", self._apply)
            gate.await_arrival(self, "apply completion snapshot read")
            t_complete = self._spawn(
                "complete", lambda: self._complete("j-1", "x-1"))
            self.assertTrue(complete_waiting.wait(_WAIT),
                            "complete never queued on the advice lock")
            gate.open()
            self._join(t_apply, t_complete)

        self.assertIsInstance(self._outcomes["apply"], LookupError)
        record, created = self._result("complete")
        self.assertTrue(created)
        # The refused reservation left no intent ledger behind; the same
        # idempotency key reserves exactly once on retry.
        self.assertFalse(Path(self.intents).exists())
        reserved, created = self._apply()
        self.assertTrue(created)
        self.assertEqual(reserved["target"],
                         {"resource_id": "r-1", "version": 1})
        self.assertEqual(reserved["reserved"], "reserved")
        replayed, created = self._apply()
        self.assertFalse(created)
        self.assertEqual(replayed, reserved)
        with self.assertRaises(ValueError):
            self._apply(key="r-1res-other")
        self._assert_completion_recorded_once("j-1", "x-1", ["x-0", "x-1"])

    def test_complete_first_releases_capacity_for_reservation(self) -> None:
        # The completion commits while the reservation is queued on the
        # advice lock: the reservation must observe the post-commit
        # snapshot and reserve the freed capacity.
        gate = _Gate()
        apply_waiting = threading.Event()
        advice_real = os.path.realpath(self.advice)

        real_commit = completion_module._commit_file

        def gated_commit(realpath, payload, old_bytes):
            gate.wait_inside()
            return real_commit(realpath, payload, old_bytes)

        probed_lock = _lock_probe(rebalance_module._lock, advice_real,
                                  apply_waiting, thread_name="apply")
        with mock.patch.object(completion_module, "_commit_file",
                               gated_commit), \
                mock.patch.object(rebalance_module, "_lock", probed_lock):
            t_complete = self._spawn(
                "complete", lambda: self._complete("j-1", "x-1"))
            gate.await_arrival(self, "completion commit")
            t_apply = self._spawn("apply", self._apply)
            self.assertTrue(apply_waiting.wait(_WAIT),
                            "apply never queued on the advice lock")
            gate.open()
            self._join(t_complete, t_apply)

        record, created = self._result("complete")
        self.assertTrue(created)
        reserved, created = self._result("apply")
        self.assertTrue(created)
        self.assertEqual(reserved["target"],
                         {"resource_id": "r-1", "version": 1})
        replayed, created = self._apply()
        self.assertFalse(created)
        self.assertEqual(replayed, reserved)
        self._assert_completion_recorded_once("j-1", "x-1", ["x-0", "x-1"])

    def test_four_way_lock_ordering_never_deadlocks(self) -> None:
        # complete (completion ledger exclusive) commits while a clear,
        # an evaluation and a reservation queue on the shared locks in
        # the one global resolved-path order; every party observes the
        # post-commit snapshot and finishes within bounded waits.
        gate = _Gate()
        evaluate_waiting = threading.Event()
        apply_waiting = threading.Event()
        clear_waiting = threading.Event()
        advice_real = os.path.realpath(self.advice)
        completions_real = os.path.realpath(self.completions)

        real_commit = completion_module._commit_file

        def gated_commit(realpath, payload, old_bytes):
            gate.wait_inside()
            return real_commit(realpath, payload, old_bytes)

        real_rebalance_lock = rebalance_module._lock

        @contextlib.contextmanager
        def probed_rebalance_lock(realpath, *, shared=False):
            name = threading.current_thread().name
            if realpath == advice_real and name == "evaluate":
                evaluate_waiting.set()
            if realpath == advice_real and name == "apply":
                apply_waiting.set()
            with real_rebalance_lock(realpath, shared=shared):
                yield

        probed_clear_lock = _lock_probe(market_module._clear_lock,
                                        completions_real, clear_waiting,
                                        thread_name="clear")
        with mock.patch.object(completion_module, "_commit_file",
                               gated_commit), \
                mock.patch.object(rebalance_module, "_lock",
                                  probed_rebalance_lock), \
                mock.patch.object(market_module, "_clear_lock",
                                  probed_clear_lock):
            t_complete = self._spawn(
                "complete", lambda: self._complete("j-1", "x-1"))
            gate.await_arrival(self, "completion commit")
            # Staged starts: each party must be queued on its lock
            # before the next one enters, so the lock-ordering chain is
            # fully assembled before the commit is released.
            t_evaluate = self._spawn("evaluate", self._evaluate)
            self.assertTrue(evaluate_waiting.wait(_WAIT),
                            "evaluate never queued on the advice lock")
            t_apply = self._spawn("apply", self._apply)
            self.assertTrue(apply_waiting.wait(_WAIT),
                            "apply never queued on the advice lock")
            t_clear = self._spawn(
                "clear", lambda: self._clear("j-4", "t-4", at=80))
            self.assertTrue(clear_waiting.wait(_WAIT),
                            "clear never queued on the completion lock")
            gate.open()
            self._join(t_complete, t_evaluate, t_apply, t_clear)

        record, created = self._result("complete")
        self.assertTrue(created)
        advice, created = self._result("evaluate")
        self.assertTrue(created)
        self.assertEqual(advice["recommendation"], "migrate")
        self.assertEqual(advice["target"],
                         {"resource_id": "r-1", "version": 1})
        reserved, created = self._result("apply")
        self.assertTrue(created)
        self.assertEqual(reserved["target"],
                         {"resource_id": "r-1", "version": 1})
        trade, created = self._result("clear")
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-3")
        self._assert_completion_recorded_once("j-1", "x-1", ["x-0", "x-1"])

    def test_entries_without_completions_keep_historical_behavior(self) -> None:
        # Omitting the completion argument keeps the pre-completion
        # results even though the ledger sits beside the snapshots.
        self._complete("j-1", "x-1")
        # clear_live: the released version still counts as occupied.
        with self.assertRaises(LookupError):
            clear_live(self.jobs, self.supply, self.signals, self.trades,
                       "j-3", "t-3", 80)
        # evaluate: r-1 is not a candidate, so the advice stays "keep".
        advice, created = evaluate(self.jobs, self.supply, self.signals,
                                   self.trades, self.dispatch,
                                   self.execution, self.advice, "j-2",
                                   "a-9", 80)
        self.assertTrue(created)
        self.assertEqual(advice["recommendation"], "keep")
        # apply: the migrate advice's target is not feasible.
        with self.assertRaises(LookupError):
            apply(self.jobs, self.supply, self.signals, self.trades,
                  self.dispatch, self.execution, self.advice, self.intents,
                  "j-2", "a-1", "r-1res", 80)
        # Passing the completion ledger flips each result exactly as the
        # sequential contract documents.
        advice, created = self._evaluate(key="a-10")
        self.assertTrue(created)
        self.assertEqual(advice["recommendation"], "migrate")
        reserved, created = self._apply()
        self.assertTrue(created)
        self.assertEqual(reserved["target"],
                         {"resource_id": "r-1", "version": 1})
        trade, created = self._clear("j-3", "t-3", at=80)
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-1")


class CompletedJobRejectionRaceTest(_RaceTestBase):
    """A complete completion snapshot refuses new advice and new
    reservations for the completed job while it is being committed."""

    def setUp(self) -> None:
        super().setUp()
        jobs_module.submit(self.jobs, _job("j-1"), "jk-1")
        # A bootstrap job on its own resource brings the dispatch and
        # execution ledgers into existence, as in the sequential tests.
        region = "zz-bootstrap"
        jobs_module.submit(
            self.jobs,
            _job("j-9", regions=[region, "eu-north"], residency=[region]),
            "jk-9")
        resources_module.publish(
            self.supply, _resource("r-1", capacity=10), "rk-1")
        resources_module.publish(
            self.supply, _resource("r-2", region="us-west", capacity=10),
            "rk-2")
        resources_module.publish(
            self.supply,
            _resource("r-9", region=region, capacity=1000,
                      residency=[region]), "rk-9")
        signals_module.publish(
            self.signals, _signal(expires=15), "sk-1a")
        signals_module.publish(
            self.signals,
            _signal("us-west", observed=4, unit_cost=3,
                    carbon_intensity=2), "sk-2")
        signals_module.publish(
            self.signals, _signal(region, carbon_intensity=1), "sk-9")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-9", "t-9", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-9", "d-9", 10)
        dispatch_module.claim(self.dispatch, "j-9", "c-9", "owner-9", 80,
                              10)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-9", "e-9",
                              "owner-9", None, 10)
        # j-1 books r-1 (the us-west signal is not observed yet), then
        # earns a migrate-to-r-2 advice once eu-north turns expensive.
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-1", "t-1", 3)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-1", "d-1", 3)
        signals_module.publish(
            self.signals,
            _signal(observed=5, unit_cost=9, carbon_intensity=9), "sk-1b")
        advice, created = evaluate(self.jobs, self.supply, self.signals,
                                   self.trades, self.dispatch,
                                   self.execution, self.advice, "j-1",
                                   "a-j1", 6)
        self.assertTrue(created)
        self.assertEqual(advice["recommendation"], "migrate")
        self.assertEqual(advice["target"],
                         {"resource_id": "r-2", "version": 1})
        # j-1 finishes its booking and becomes completable.
        dispatch_module.claim(self.dispatch, "j-1", "c-1", "owner-1", 30,
                              60)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "p-1",
                              "owner-1", None, 61)
        execution_module.record(self.execution, "j-1", 1, "s-1",
                                "owner-1", "stage", "succeeded", "staged",
                                62)
        execution_module.record(self.execution, "j-1", 1, "s-2",
                                "owner-1", "start", "succeeded", "started",
                                63)
        execution_sync_module.run(self.execution, self.dispatch, self.sync,
                                  "sync-1", "b-1", 70, 50)

    def test_completed_job_gets_no_new_advice_or_reservation(self) -> None:
        # evaluate and apply for the completing job queue on the advice
        # lock while the completion commits; both must observe the
        # complete post-commit snapshot and refuse with ValueError,
        # writing nothing.
        gate = _Gate()
        evaluate_waiting = threading.Event()
        apply_waiting = threading.Event()
        advice_real = os.path.realpath(self.advice)
        advice_before = Path(self.advice).read_bytes()

        real_commit = completion_module._commit_file

        def gated_commit(realpath, payload, old_bytes):
            gate.wait_inside()
            return real_commit(realpath, payload, old_bytes)

        real_rebalance_lock = rebalance_module._lock

        @contextlib.contextmanager
        def probed_rebalance_lock(realpath, *, shared=False):
            name = threading.current_thread().name
            if realpath == advice_real and name == "evaluate":
                evaluate_waiting.set()
            if realpath == advice_real and name == "apply":
                apply_waiting.set()
            with real_rebalance_lock(realpath, shared=shared):
                yield

        def run_evaluate():
            return evaluate(self.jobs, self.supply, self.signals,
                            self.trades, self.dispatch, self.execution,
                            self.advice, "j-1", "a-2", 80, self.completions)

        def run_apply():
            return apply(self.jobs, self.supply, self.signals, self.trades,
                         self.dispatch, self.execution, self.advice,
                         self.intents, "j-1", "a-j1", "r-2res", 80,
                         self.completions)

        with mock.patch.object(completion_module, "_commit_file",
                               gated_commit), \
                mock.patch.object(rebalance_module, "_lock",
                                  probed_rebalance_lock):
            t_complete = self._spawn(
                "complete", lambda: self._complete("j-1", "x-1"))
            gate.await_arrival(self, "completion commit")
            t_evaluate = self._spawn("evaluate", run_evaluate)
            self.assertTrue(evaluate_waiting.wait(_WAIT),
                            "evaluate never queued on the advice lock")
            t_apply = self._spawn("apply", run_apply)
            self.assertTrue(apply_waiting.wait(_WAIT),
                            "apply never queued on the advice lock")
            gate.open()
            self._join(t_complete, t_evaluate, t_apply)

        record, created = self._result("complete")
        self.assertTrue(created)
        self.assertEqual(record["job_id"], "j-1")
        self.assertIsInstance(self._outcomes["evaluate"], ValueError)
        self.assertIsInstance(self._outcomes["apply"], ValueError)
        # Neither refusal wrote: the advice ledger is byte-for-byte the
        # pre-race one and no intent ledger was created.
        self.assertEqual(Path(self.advice).read_bytes(), advice_before)
        self.assertFalse(Path(self.intents).exists())
        self._assert_completion_recorded_once("j-1", "x-1", ["x-1"])


class CompletionCommitFailureTest(_RaceTestBase):
    """Injected failures of every stage of the completion commit: each
    surfaces as OSError, leaves the ledger with exactly the pre-call
    bytes (or absent) and no readable half-record, and one retry with
    the same idempotency key creates exactly one record and releases
    the capacity exactly once."""

    def setUp(self) -> None:
        super().setUp()
        jobs_module.submit(self.jobs,
                           _job("j-0", regions=["zz-boot"],
                                residency=["zz-boot"]), "jk-0")
        for index in (1, 2, 3):
            jobs_module.submit(self.jobs, _job(f"j-{index}"), f"jk-{index}")
        resources_module.publish(
            self.supply,
            _resource("r-0", region="zz-boot", capacity=10,
                      residency=["zz-boot"]), "rk-0")
        resources_module.publish(
            self.supply, _resource("r-1", capacity=10), "rk-1")
        signals_module.publish(self.signals, _signal(), "sk-1")
        signals_module.publish(self.signals, _signal("zz-boot"), "sk-0")
        self._finish_job("j-0", 0)
        self._complete("j-0", "x-0")
        self._finish_job("j-1", 1)

    # -- shared assertions ------------------------------------------------

    def _assert_no_trace_of(self, job_id: str, ledger: str,
                            before: bytes | None) -> None:
        if before is None:
            self.assertFalse(Path(ledger).exists())
        else:
            self.assertEqual(Path(ledger).read_bytes(), before)
        with self.assertRaises(KeyError):
            get(ledger, job_id)
        fragments = [name for name in os.listdir(self.tmp.name)
                     if name.startswith(".completion-")]
        self.assertEqual(fragments, [])

    def _assert_single_retry_effect(self, job_id: str, key: str,
                                    expected_keys: list[str],
                                    completions: str | None = None) -> None:
        # The retry with the same idempotency key creates exactly once;
        # an equivalent replay rewrites nothing.
        ledger = completions or self.completions
        record, created = self._complete(job_id, key,
                                         completions=completions)
        self.assertTrue(created)
        raw = Path(ledger).read_bytes()
        replayed, created = self._complete(job_id, key,
                                           completions=completions)
        self.assertFalse(created)
        self.assertEqual(replayed, record)
        self.assertEqual(Path(ledger).read_bytes(), raw)
        self._assert_completion_recorded_once(job_id, key, expected_keys,
                                              completions=completions)
        # The capacity release takes effect exactly once: one new trade
        # books the freed version, the next one finds it full.
        trade, created = clear_live(self.jobs, self.supply, self.signals,
                                    self.trades, "j-2", "t-2", 70, ledger)
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-1")
        replayed_trade, created = clear_live(self.jobs, self.supply,
                                             self.signals, self.trades,
                                             "j-2", "t-2", 70, ledger)
        self.assertFalse(created)
        self.assertEqual(replayed_trade, trade)
        with self.assertRaises(LookupError):
            clear_live(self.jobs, self.supply, self.signals, self.trades,
                       "j-3", "t-3", 70, ledger)

    # -- injected commit failures -----------------------------------------

    def test_temporary_file_write_failure(self) -> None:
        before = Path(self.completions).read_bytes()
        real_fdopen = os.fdopen
        injected = {"done": False}

        class FailingWriter:
            def __init__(self, handle):
                self._handle = handle

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                self._handle.close()
                return False

            def write(self, data):
                raise OSError("injected temporary-file write failure")

        def flaky_fdopen(fd, mode="r", *args, **kwargs):
            handle = real_fdopen(fd, mode, *args, **kwargs)
            if not injected["done"] and "w" in mode:
                injected["done"] = True
                return FailingWriter(handle)
            return handle

        with mock.patch("os.fdopen", flaky_fdopen):
            with self.assertRaises(OSError):
                self._complete("j-1", "x-1")
        self.assertTrue(injected["done"])
        self._assert_no_trace_of("j-1", self.completions, before)
        self._assert_single_retry_effect("j-1", "x-1", ["x-0", "x-1"])

    def test_file_fsync_failure(self) -> None:
        before = Path(self.completions).read_bytes()
        real_fsync = os.fsync
        injected = {"done": False}

        def flaky_fsync(fd):
            if not injected["done"]:
                injected["done"] = True
                raise OSError("injected file fsync failure")
            return real_fsync(fd)

        with mock.patch("os.fsync", flaky_fsync):
            with self.assertRaises(OSError):
                self._complete("j-1", "x-1")
        self.assertTrue(injected["done"])
        self._assert_no_trace_of("j-1", self.completions, before)
        self._assert_single_retry_effect("j-1", "x-1", ["x-0", "x-1"])

    def test_atomic_replace_failure(self) -> None:
        before = Path(self.completions).read_bytes()
        real_replace = os.replace
        injected = {"done": False}

        def flaky_replace(src, dst):
            if not injected["done"]:
                injected["done"] = True
                raise OSError("injected atomic replace failure")
            return real_replace(src, dst)

        with mock.patch("os.replace", flaky_replace):
            with self.assertRaises(OSError):
                self._complete("j-1", "x-1")
        self.assertTrue(injected["done"])
        self._assert_no_trace_of("j-1", self.completions, before)
        self._assert_single_retry_effect("j-1", "x-1", ["x-0", "x-1"])

    def test_directory_fsync_failure(self) -> None:
        before = Path(self.completions).read_bytes()
        real_dirsync = completion_module._fsync_directory
        injected = {"done": False}

        def flaky_dirsync(directory):
            if not injected["done"]:
                injected["done"] = True
                raise OSError("injected directory fsync failure")
            return real_dirsync(directory)

        with mock.patch.object(completion_module, "_fsync_directory",
                               flaky_dirsync):
            with self.assertRaises(OSError):
                self._complete("j-1", "x-1")
        self.assertTrue(injected["done"])
        # The replace had already landed, so the rollback restored the
        # exact pre-call bytes.
        self._assert_no_trace_of("j-1", self.completions, before)
        self._assert_single_retry_effect("j-1", "x-1", ["x-0", "x-1"])

    def test_directory_fsync_failure_without_existing_ledger(self) -> None:
        # The same failure on a first completion rolls the brand-new
        # ledger back to absence.
        ledger = os.path.join(self.tmp.name, "completions-two.json")
        real_dirsync = completion_module._fsync_directory
        injected = {"done": False}

        def flaky_dirsync(directory):
            if not injected["done"]:
                injected["done"] = True
                raise OSError("injected directory fsync failure")
            return real_dirsync(directory)

        with mock.patch.object(completion_module, "_fsync_directory",
                               flaky_dirsync):
            with self.assertRaises(OSError):
                self._complete("j-1", "x-1", completions=ledger)
        self.assertTrue(injected["done"])
        self._assert_no_trace_of("j-1", ledger, None)
        # The pre-existing sibling ledger is untouched.
        with self.assertRaises(KeyError):
            get(self.completions, "j-1")
        self._assert_single_retry_effect("j-1", "x-1", ["x-1"],
                                         completions=ledger)


if __name__ == "__main__":
    unittest.main()
