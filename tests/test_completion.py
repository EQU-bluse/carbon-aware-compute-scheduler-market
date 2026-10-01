from __future__ import annotations

import json
import os
import unittest
from multiprocessing import Process, Queue
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from carbon_market import completion as completion_module
from carbon_market import dispatch as dispatch_module
from carbon_market import execution as execution_module
from carbon_market import jobs as jobs_module
from carbon_market import rebalance as rebalance_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.completion import get, register
from carbon_market.market import clear, clear_live
from carbon_market.rebalance import apply, evaluate, record as mig_record
from carbon_market.rebalance import recover as mig_recover
from carbon_market.rebalance import settle, start


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


def _resource(resource_id: str = "r-1", **overrides: object) -> dict[str, object]:
    resource: dict[str, object] = {
        "resource_id": resource_id,
        "region": "eu-north",
        "capacity": 100,
        "start": 0,
        "end": 500,
        "unit_cost": 3,
        "carbon_intensity": 7,
        "residency": ["eu-north"],
    }
    resource.update(overrides)
    return resource


class SimpleCompletionTest(unittest.TestCase):
    """A traded, claimed and completed launch job with no migration."""

    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        t = self.tmp.name
        self.jobs = os.path.join(t, "jobs.json")
        self.supply = os.path.join(t, "supply.json")
        self.trades = os.path.join(t, "clear.json")
        self.dispatch = os.path.join(t, "dispatch.json")
        self.execution = os.path.join(t, "execution.json")
        self.completions = os.path.join(t, "completions.json")
        jobs_module.submit(self.jobs, _job(), "jk-1")
        resources_module.publish(self.supply, _resource(), "rk-1")
        clear(self.jobs, self.supply, self.trades, "j-1", "tk-1", 40)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-1", "ck-1", 50)
        dispatch_module.claim(self.dispatch, "j-1", "lk-1", "worker-1",
                              30, 60)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "pk-1",
                              "worker-1", None, 61)
        execution_module.record(self.execution, "j-1", 1, "s-1",
                                "worker-1", "stage", "succeeded", "staged",
                                62)
        execution_module.record(self.execution, "j-1", 1, "s-2",
                                "worker-1", "start", "succeeded", "started",
                                63)
        dispatch_module.finish(self.dispatch, "j-1", "fk-1", "worker-1",
                               "succeeded", 64)

    def _register(self, at: int = 70, key: str = "x-1",
                  outcome: str = "succeeded", actual_cost: int = 300,
                  actual_carbon: int = 700, **kwargs: object):
        return register(self.jobs, self.supply, self.trades, self.dispatch,
                        self.execution, self.completions, "j-1", key, at,
                        outcome, actual_cost, actual_carbon, **kwargs)

    def test_first_registration_freezes_trade_generation(self) -> None:
        record, created = self._register()
        self.assertTrue(created)
        self.assertEqual(list(record.keys()),
                         ["job_id", "key", "at", "outcome", "actual_cost",
                          "actual_carbon", "generation", "current",
                          "cost_exceeded", "carbon_exceeded"])
        self.assertEqual(record["job_id"], "j-1")
        self.assertEqual(record["key"], "x-1")
        self.assertEqual(record["at"], 70)
        self.assertEqual(record["outcome"], "succeeded")
        self.assertEqual(record["generation"], 0)
        self.assertEqual(record["current"],
                         {"resource_id": "r-1", "version": 1})
        self.assertFalse(record["cost_exceeded"])
        self.assertFalse(record["carbon_exceeded"])

    def test_cost_exceeded_follows_original_limit(self) -> None:
        record, _ = self._register(key="x-2", actual_cost=1001,
                                   actual_carbon=1000)
        self.assertTrue(record["cost_exceeded"])
        self.assertFalse(record["carbon_exceeded"])

    def test_carbon_exceeded_follows_original_limit(self) -> None:
        record, _ = self._register(key="x-3", actual_cost=1000,
                                   actual_carbon=1001)
        self.assertFalse(record["cost_exceeded"])
        self.assertTrue(record["carbon_exceeded"])

    def test_limits_are_inclusive_at_the_boundary(self) -> None:
        record, _ = self._register(key="x-4", actual_cost=1000,
                                   actual_carbon=1000)
        self.assertFalse(record["cost_exceeded"])
        self.assertFalse(record["carbon_exceeded"])

    def test_failed_outcome_freezes_the_same_binding(self) -> None:
        record, created = self._register(key="x-4", outcome="failed")
        self.assertTrue(created)
        self.assertEqual(record["outcome"], "failed")
        self.assertEqual(record["generation"], 0)
        self.assertEqual(record["current"],
                         {"resource_id": "r-1", "version": 1})

    def test_get_returns_the_record(self) -> None:
        first, _ = self._register()
        again = get(self.completions, "j-1")
        self.assertEqual(again, first)

    def test_get_unknown_job_is_key_error(self) -> None:
        self._register()
        with self.assertRaises(KeyError):
            get(self.completions, "j-other")

    def test_get_missing_ledger_is_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            get(os.path.join(self.tmp.name, "missing.json"), "j-1")

    def test_replay_returns_false_without_rewriting_a_byte(self) -> None:
        first, created = self._register()
        raw = Path(self.completions).read_bytes()
        again, created_again = self._register()
        self.assertEqual(again, first)
        self.assertTrue(created)
        self.assertFalse(created_again)
        self.assertEqual(Path(self.completions).read_bytes(), raw)

    def test_same_key_with_changed_request_is_value_error(self) -> None:
        self._register()
        with self.assertRaises(ValueError):
            self._register(at=71)
        with self.assertRaises(ValueError):
            self._register(outcome="failed")
        with self.assertRaises(ValueError):
            self._register(actual_cost=1)
        with self.assertRaises(ValueError):
            self._register(actual_carbon=1)

    def test_same_job_under_another_key_is_value_error(self) -> None:
        self._register()
        with self.assertRaises(ValueError):
            self._register(key="x-other")

    def test_illegal_arguments_are_value_errors(self) -> None:
        with self.assertRaises(ValueError):
            register(self.jobs, self.supply, self.trades, self.dispatch,
                     self.execution, self.completions, "j-1", "x", 70,
                     "abandoned", 1, 1)
        for bad_at in (True, -1, 1.0, "70"):
            with self.assertRaises(ValueError):
                register(self.jobs, self.supply, self.trades, self.dispatch,
                         self.execution, self.completions, "j-1", "x",
                         bad_at, "succeeded", 1, 1)
        for bad in (-1, True, 1.0, "1"):
            with self.assertRaises(ValueError):
                register(self.jobs, self.supply, self.trades, self.dispatch,
                         self.execution, self.completions, "j-1", "x", 70,
                         "succeeded", bad, 1)
            with self.assertRaises(ValueError):
                register(self.jobs, self.supply, self.trades, self.dispatch,
                         self.execution, self.completions, "j-1", "x", 70,
                         "succeeded", 1, bad)
        with self.assertRaises(ValueError):
            register(self.jobs, self.supply, self.trades, self.dispatch,
                     self.execution, self.jobs, "j-1", "x", 70,
                     "succeeded", 1, 1)
        with self.assertRaises(ValueError):
            register(self.jobs, self.supply, self.trades, self.dispatch,
                     self.execution, self.completions, "j-1", "x", 70,
                     "succeeded", 1, 1, signals=self.jobs)

    def test_moment_before_last_evidence_is_value_error(self) -> None:
        with self.assertRaises(ValueError):
            self._register(at=62)

    def test_dispatch_not_succeeded_is_permission_error(self) -> None:
        # Roll the dispatch decision back to claimed through a second
        # job: j-2 has a completed plan but never finishes dispatch.
        self._make_second_job_finished_plan(finish_dispatch=False)
        with self.assertRaises(PermissionError):
            register(self.jobs, self.supply, self.trades, self.dispatch,
                     self.execution, self.completions, "j-2", "y-1", 80,
                     "succeeded", 1, 1)

    def test_traded_but_not_executed_is_permission_error(self) -> None:
        jobs_module.submit(self.jobs, _job("j-3"), "jk-3")
        clear(self.jobs, self.supply, self.trades, "j-3", "tk-3", 40)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-3", "ck-3", 50)
        with self.assertRaises(PermissionError):
            register(self.jobs, self.supply, self.trades, self.dispatch,
                     self.execution, self.completions, "j-3", "z-1", 80,
                     "succeeded", 1, 1)

    def test_unknown_job_is_key_error(self) -> None:
        with self.assertRaises(KeyError):
            register(self.jobs, self.supply, self.trades, self.dispatch,
                     self.execution, self.completions, "j-nope", "n-1", 80,
                     "succeeded", 1, 1)

    def _make_second_job_finished_plan(self, finish_dispatch: bool) -> None:
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        clear(self.jobs, self.supply, self.trades, "j-2", "tk-2", 40)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-2", "ck-2", 50)
        dispatch_module.claim(self.dispatch, "j-2", "lk-2", "worker-2",
                              25, 60)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-2", "pk-2",
                              "worker-2", None, 61)
        execution_module.record(self.execution, "j-2", 1, "t-1",
                                "worker-2", "stage", "succeeded", "staged",
                                62)
        execution_module.record(self.execution, "j-2", 1, "t-2",
                                "worker-2", "start", "succeeded", "started",
                                63)
        if finish_dispatch:
            dispatch_module.finish(self.dispatch, "j-2", "fk-2", "worker-2",
                                  "succeeded", 64)

    def test_missing_execution_ledger_is_file_not_found(self) -> None:
        os.unlink(self.execution)
        with self.assertRaises(FileNotFoundError):
            self._register()

    def test_missing_completion_parent_is_file_not_found(self) -> None:
        missing = os.path.join(self.tmp.name, "nope", "c.json")
        with self.assertRaises(FileNotFoundError):
            register(self.jobs, self.supply, self.trades, self.dispatch,
                     self.execution, missing, "j-1", "x-1", 70,
                     "succeeded", 1, 1)
        self.assertFalse(os.path.exists(os.path.dirname(missing)))

    def test_commit_io_failure_leaves_no_ledger(self) -> None:
        with mock.patch.object(completion_module, "_fsync_directory",
                               side_effect=OSError("disk gone")):
            with self.assertRaises(OSError):
                self._register()
        # The failed commit rolled the completion ledger away; the
        # business ledgers are untouched and a retry on a healthy disk
        # still creates the record exactly once.
        self.assertFalse(Path(self.completions).exists())
        record, created = self._register()
        self.assertTrue(created)
        self.assertEqual(record["job_id"], "j-1")

    def test_failed_request_preserves_the_existing_ledger(self) -> None:
        self._register()
        raw = Path(self.completions).read_bytes()
        with self.assertRaises(ValueError):
            self._register(key="x-other", at=1)
        # j-2 has a completed launch plan but no succeeded dispatch
        # decision: not a stable terminal state.
        self._make_second_job_finished_plan(finish_dispatch=False)
        with self.assertRaises(PermissionError):
            register(self.jobs, self.supply, self.trades, self.dispatch,
                     self.execution, self.completions, "j-2", "z-9", 80,
                     "succeeded", 1, 1)
        self.assertEqual(Path(self.completions).read_bytes(), raw)

    def test_ledger_is_canonical_compact_json(self) -> None:
        self._register(key="b")
        raw = Path(self.completions).read_bytes()
        text = raw.decode("utf-8")
        self.assertTrue(text.endswith("\n"))
        self.assertFalse(text.endswith("\n\n"))
        self.assertNotIn(" ", text)
        data = json.loads(text)
        self.assertEqual(list(data.keys()),
                         ["version", "completions", "idempotency", "audit"])
        self.assertEqual(data["version"], 1)
        request = data["idempotency"]["b"]
        self.assertEqual(request, {
            "job_id": "j-1", "at": 70, "outcome": "succeeded",
            "actual_cost": 300, "actual_carbon": 700})
        self.assertEqual(data["audit"]["b"]["request"], request)
        self.assertEqual(data["audit"]["b"]["result"],
                         data["completions"]["b"])

    def test_non_canonical_ledger_is_value_error(self) -> None:
        self._register()
        raw = Path(self.completions).read_bytes()
        Path(self.completions).write_bytes(raw[:-1] + b" \n")
        with self.assertRaises(ValueError):
            get(self.completions, "j-1")
        with self.assertRaises(ValueError):
            self._register()


class CapacityReleaseTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        t = self.tmp.name
        self.jobs = os.path.join(t, "jobs.json")
        self.supply = os.path.join(t, "supply.json")
        self.trades = os.path.join(t, "clear.json")
        self.dispatch = os.path.join(t, "dispatch.json")
        self.execution = os.path.join(t, "execution.json")
        self.completions = os.path.join(t, "completions.json")
        jobs_module.submit(self.jobs, _job("j-1", work=60), "jk-1")
        jobs_module.submit(self.jobs, _job("j-2", work=60), "jk-2")
        resources_module.publish(
            self.supply, _resource(capacity=100), "rk-1")

    def _finish_job_one(self) -> None:
        clear(self.jobs, self.supply, self.trades, "j-1", "tk-1", 40)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-1", "ck-1", 50)
        dispatch_module.claim(self.dispatch, "j-1", "lk-1", "worker-1",
                              30, 60)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "pk-1",
                              "worker-1", None, 61)
        execution_module.record(self.execution, "j-1", 1, "s-1",
                                "worker-1", "stage", "succeeded", "staged",
                                62)
        execution_module.record(self.execution, "j-1", 1, "s-2",
                                "worker-1", "start", "succeeded", "started",
                                63)
        dispatch_module.finish(self.dispatch, "j-1", "fk-1", "worker-1",
                               "succeeded", 64)
        register(self.jobs, self.supply, self.trades, self.dispatch,
                 self.execution, self.completions, "j-1", "x-1", 70,
                 "succeeded", 1, 1)

    def test_clear_without_completion_keeps_booking(self) -> None:
        self._finish_job_one()
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.trades, "j-2", "tk-2a", 80)

    def test_clear_releases_capacity_at_and_after_completion(self) -> None:
        self._finish_job_one()
        # At 69 the completion (at 70) is not visible yet.
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.trades, "j-2", "tk-2b", 69,
                  completion=self.completions)
        trade, created = clear(self.jobs, self.supply, self.trades, "j-2",
                               "tk-2c", 70,
                               completion=self.completions)
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-1")

    def test_clear_live_releases_capacity_too(self) -> None:
        signals = os.path.join(self.tmp.name, "signals.json")
        signals_module.publish(signals, _signal(), "sg-1")
        # Re-book j-1 through live clearing so the shared ledger holds a
        # live trade, then run it to completion the same way.
        trades_live = os.path.join(self.tmp.name, "clear-live.json")
        clear_live(self.jobs, self.supply, signals, trades_live, "j-1",
                   "lk-1", 40)
        dispatch_live = os.path.join(self.tmp.name, "dispatch-live.json")
        execution_live = os.path.join(self.tmp.name, "execution-live.json")
        dispatch_module.commit(self.jobs, self.supply, trades_live,
                               dispatch_live, "j-1", "ck-1", 50)
        dispatch_module.claim(dispatch_live, "j-1", "q-1", "worker-1", 30,
                              60)
        execution_module.plan(self.jobs, self.supply, trades_live,
                              dispatch_live, execution_live, "j-1", "pk-1",
                              "worker-1", None, 61)
        execution_module.record(execution_live, "j-1", 1, "s-1",
                                "worker-1", "stage", "succeeded", "staged",
                                62)
        execution_module.record(execution_live, "j-1", 1, "s-2",
                                "worker-1", "start", "succeeded", "started",
                                63)
        dispatch_module.finish(dispatch_live, "j-1", "fk-1", "worker-1",
                               "succeeded", 64)
        completions_live = os.path.join(self.tmp.name, "comp-live.json")
        register(self.jobs, self.supply, trades_live, dispatch_live,
                 execution_live, completions_live, "j-1", "x-1", 70,
                 "succeeded", 1, 1, signals=signals)
        with self.assertRaises(LookupError):
            clear_live(self.jobs, self.supply, signals, trades_live, "j-2",
                       "tk-2a", 69, completion=completions_live)
        trade, created = clear_live(self.jobs, self.supply, signals,
                                    trades_live, "j-2", "tk-2b", 80,
                                    completion=completions_live)
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-1")


def _signal(region: str = "eu-north", **overrides: object) -> dict[str, object]:
    signal: dict[str, object] = {
        "region": region,
        "observed": 0,
        "expires": 500,
        "mix": {"solar": 10000},
        "unit_cost": 3,
        "carbon_intensity": 7,
    }
    signal.update(overrides)
    return signal


class MigrationCompletionTest(unittest.TestCase):
    """j-1 migrates r-1 -> r-2, settles, relaunches and completes."""

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
        self.advice = os.path.join(base, "advice.json")
        self.intents = os.path.join(base, "intents.json")
        self.settlements = os.path.join(base, "settlements.json")
        self.completions = os.path.join(base, "completions.json")

        jobs_module.submit(self.jobs, _job("j-1", work=60), "jk-1")
        jobs_module.submit(self.jobs, _job("j-3", work=80), "jk-3")
        resources_module.publish(
            self.supply,
            # 139 is the deliberate boundary: j-1's compensated 60
            # leaves 79 for an 80-work migrant, but the full 139 once
            # j-1's completion releases that current occupancy.
            _resource("r-1", region="eu-north", capacity=139), "k1")
        resources_module.publish(
            self.supply,
            _resource("r-2", region="us-west", capacity=200,
                      residency=["eu-north", "us-west"], unit_cost=1,
                      carbon_intensity=2), "k2")
        signals_module.publish(
            self.signals, _signal("eu-north", unit_cost=3,
                                  carbon_intensity=8), "s1")

        # j-1 trades r-1 at 10 before the us-west signal exists; once
        # the cleaner r-2 signal is published, j-3 trades r-2 at 17.
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-1", "t1", 10)
        signals_module.publish(
            self.signals, _signal("us-west", unit_cost=1,
                                  carbon_intensity=2, observed=15), "s2")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-3", "t3", 17)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-1", "d1", 12)
        # j-3 stays at a ready decision; advice only requires the
        # decision to exist.
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-3", "d3", 18)
        self._bootstrap_execution_ledger()

    def _bootstrap_execution_ledger(self) -> None:
        # execution.plan requires the execution ledger to already exist;
        # a throwaway j-9 migration-free run creates it.
        jobs_module.submit(self.jobs, _job("j-9", regions=["zz"],
                                          residency=["zz"]), "jk-9")
        resources_module.publish(
            self.supply,
            _resource("r-9", region="zz", capacity=1000,
                      residency=["zz"], carbon_intensity=1), "k9")
        signals_module.publish(
            self.signals, _signal("zz", carbon_intensity=1), "s9")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-9", "t9", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-9", "d9", 10)
        dispatch_module.claim(self.dispatch, "j-9", "c9", "owner-9", 80,
                              10)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-9", "e9",
                              "owner-9", None, 10)

    def _seven(self):
        return (self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice)

    def _eight(self):
        return self._seven() + (self.intents,)

    def _nine(self):
        return self._eight() + (self.settlements,)

    def _run_j1_to_completion(self, completion_at: int = 40) -> dict:
        # Round: the migration to cleaner r-2 fails on the copy step, so
        # the settlement compensates and keeps j-1 on traded r-1 at
        # generation 1. The relaunch on r-1 then finishes the run.
        evaluate(*self._seven(), "j-1", "a1", 20)
        apply(*self._eight(), "j-1", "a1", "r1", 21)
        start(*self._eight(), "j-1", "m1", "owner-1", 90, 22)
        mig_record(*self._eight(), "j-1", "m2", "owner-1", "copy",
                   "failed", "copy-broke", 23)
        settled, _ = settle(*self._nine(), "j-1", "m1", "s1", 25)
        # Relaunch on the compensated source and finish dispatch.
        dispatch_module.claim(self.dispatch, "j-1", "l1", "owner-1", 30,
                              26)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "p1",
                              "owner-1", None, 27)
        execution_module.record(self.execution, "j-1", 1, "e1",
                                "owner-1", "stage", "succeeded", "staged",
                                28)
        execution_module.record(self.execution, "j-1", 1, "e2",
                                "owner-1", "start", "succeeded", "started",
                                29)
        dispatch_module.finish(self.dispatch, "j-1", "f1", "owner-1",
                               "succeeded", 30)
        record, created = register(
            self.jobs, self.supply, self.trades, self.dispatch,
            self.execution, self.completions, "j-1", "x1",
            completion_at, "succeeded", 500, 120)
        self.assertTrue(created)
        return {"settled": settled, "record": record}

    def test_completion_freezes_settled_generation(self) -> None:
        out = self._run_j1_to_completion()
        self.assertEqual(out["settled"]["generation"], 1)
        self.assertEqual(out["settled"]["migration"], "failed")
        self.assertEqual(out["settled"]["state"], "compensated")
        self.assertEqual(out["settled"]["before"],
                         {"resource_id": "r-1", "version": 1})
        self.assertEqual(out["settled"]["after"],
                         {"resource_id": "r-1", "version": 1})
        record = out["record"]
        self.assertEqual(record["generation"], 1)
        self.assertEqual(record["current"],
                         {"resource_id": "r-1", "version": 1})
        self.assertFalse(record["cost_exceeded"])
        self.assertFalse(record["carbon_exceeded"])
        self.assertEqual(get(self.completions, "j-1"), record)

    def test_completion_before_last_evidence_is_value_error(self) -> None:
        # Build the full lineage but relaunch-finish at 30; completion
        # at 29 precedes the last launch receipt.
        evaluate(*self._seven(), "j-1", "a1", 20)
        apply(*self._eight(), "j-1", "a1", "r1", 21)
        start(*self._eight(), "j-1", "m1", "owner-1", 90, 22)
        mig_record(*self._eight(), "j-1", "m2", "owner-1", "copy",
                   "succeeded", "copy-receipt", 23)
        mig_record(*self._eight(), "j-1", "m4", "owner-1", "switch",
                   "succeeded", "switch-receipt", 24)
        settle(*self._nine(), "j-1", "m1", "s1", 25)
        dispatch_module.claim(self.dispatch, "j-1", "l1", "owner-1", 30,
                              26)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "p1",
                              "owner-1", None, 27)
        execution_module.record(self.execution, "j-1", 1, "e1",
                                "owner-1", "stage", "succeeded", "staged",
                                28)
        execution_module.record(self.execution, "j-1", 1, "e2",
                                "owner-1", "start", "succeeded", "started",
                                29)
        dispatch_module.finish(self.dispatch, "j-1", "f1", "owner-1",
                               "succeeded", 30)
        with self.assertRaises(ValueError):
            register(self.jobs, self.supply, self.trades, self.dispatch,
                     self.execution, self.completions, "j-1", "x1", 28,
                     "succeeded", 1, 1)

    def test_completed_job_cannot_evaluate_or_reserve_again(self) -> None:
        self._run_j1_to_completion()
        with self.assertRaises(ValueError):
            evaluate(*self._seven(), "j-1", "a-late", 45,
                     completion=self.completions)
        with self.assertRaises(ValueError):
            apply(*self._eight(), "j-1", "a1", "r-late", 45,
                  completion=self.completions)

    def test_completion_releases_settled_target_for_other_advice(self) -> None:
        self._run_j1_to_completion()
        # After the completion the traded r-1 booking is released; a
        # fresh cleaner eu-north signal makes r-1 the winning migrant.
        signals_module.publish(
            self.signals, _signal("eu-north", observed=41, expires=300,
                                  unit_cost=1, carbon_intensity=1), "s3")

        # Without the completion ledger r-1 holds j-1's 60: 79 remain,
        # below j-3's 80, so the advice stays on traded r-2.
        without = evaluate(*self._seven(), "j-3", "a3a", 50)
        self.assertEqual(without[0]["recommendation"], "keep")

        # With j-1 completed at 40 the booking is released: all 139 of
        # r-1 remain and the cleaner region wins.
        with_completion = evaluate(
            *self._seven(), "j-3", "a3b", 50,
            completion=self.completions)
        self.assertEqual(with_completion[0]["recommendation"], "migrate")
        self.assertEqual(with_completion[0]["target"],
                         {"resource_id": "r-1", "version": 1})

        # At 39 the completion (at 40) is not yet visible; the fresh
        # signal (observed 41) is not visible either, so the advice
        # still keeps the traded r-2.
        before = evaluate(*self._seven(), "j-3", "a3c", 39,
                          completion=self.completions)
        self.assertEqual(before[0]["recommendation"], "keep")

    def test_completion_releases_target_for_reservation(self) -> None:
        self._run_j1_to_completion()
        signals_module.publish(
            self.signals, _signal("eu-north", observed=41, expires=300,
                                  unit_cost=1, carbon_intensity=1), "s3")
        evaluate(*self._seven(), "j-3", "a3", 50,
                 completion=self.completions)
        # Without the completion ledger the advice target no longer has
        # room and the reservation is refused.
        with self.assertRaises(LookupError):
            apply(*self._eight(), "j-3", "a3", "r3a", 51)
        # With it the target reservation lands.
        reserved, created = apply(*self._eight(), "j-3", "a3", "r3b", 51,
                                  completion=self.completions)
        self.assertTrue(created)
        self.assertEqual(reserved["target"],
                         {"resource_id": "r-1", "version": 1})

    def test_reserved_intent_blocks_completion(self) -> None:
        # j-1 only reserves, never starts: the open reservation keeps
        # the run in flight.
        evaluate(*self._seven(), "j-1", "a1", 20)
        apply(*self._eight(), "j-1", "a1", "r1", 21)
        dispatch_module.claim(self.dispatch, "j-1", "l1", "owner-1", 30,
                              26)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "p1",
                              "owner-1", None, 27)
        execution_module.record(self.execution, "j-1", 1, "e1",
                                "owner-1", "stage", "succeeded", "staged",
                                28)
        execution_module.record(self.execution, "j-1", 1, "e2",
                                "owner-1", "start", "succeeded", "started",
                                29)
        dispatch_module.finish(self.dispatch, "j-1", "f1", "owner-1",
                               "succeeded", 30)
        with self.assertRaises(PermissionError):
            register(self.jobs, self.supply, self.trades, self.dispatch,
                     self.execution, self.completions, "j-1", "x1", 40,
                     "succeeded", 1, 1)

    def test_active_migration_plan_blocks_completion(self) -> None:
        evaluate(*self._seven(), "j-1", "a1", 20)
        apply(*self._eight(), "j-1", "a1", "r1", 21)
        start(*self._eight(), "j-1", "m1", "owner-1", 90, 22)
        mig_record(*self._eight(), "j-1", "m2", "owner-1", "copy",
                   "succeeded", "copy-receipt", 23)
        with self.assertRaises(PermissionError):
            register(self.jobs, self.supply, self.trades, self.dispatch,
                     self.execution, self.completions, "j-1", "x1", 40,
                     "succeeded", 1, 1)

    def test_unsettled_terminal_migration_blocks_completion(self) -> None:
        evaluate(*self._seven(), "j-1", "a1", 20)
        apply(*self._eight(), "j-1", "a1", "r1", 21)
        start(*self._eight(), "j-1", "m1", "owner-1", 90, 22)
        mig_record(*self._eight(), "j-1", "m2", "owner-1", "copy",
                   "succeeded", "copy-receipt", 23)
        mig_record(*self._eight(), "j-1", "m4", "owner-1", "switch",
                   "succeeded", "switch-receipt", 24)
        with self.assertRaises(PermissionError):
            register(self.jobs, self.supply, self.trades, self.dispatch,
                     self.execution, self.completions, "j-1", "x1", 40,
                     "succeeded", 1, 1)

    def test_interrupted_unsettled_migration_blocks_completion(self) -> None:
        evaluate(*self._seven(), "j-1", "a1", 20)
        apply(*self._eight(), "j-1", "a1", "r1", 21)
        start(*self._eight(), "j-1", "m1", "owner-1", 90, 22)
        mig_record(*self._eight(), "j-1", "m2", "owner-1", "copy",
                   "succeeded", "copy-receipt", 23)
        mig_recover(*self._eight(), "j-1", "m3", "owner-1", 91)
        with self.assertRaises(PermissionError):
            register(self.jobs, self.supply, self.trades, self.dispatch,
                     self.execution, self.completions, "j-1", "x1", 95,
                     "succeeded", 1, 1)


def _register_worker(payload: tuple[str, ...], job_id: str, key: str,
                     at: int, queue: Queue) -> None:
    jobs, supply, trades, dispatch, execution, completions = payload
    try:
        record, created = register(jobs, supply, trades, dispatch,
                                   execution, completions, job_id, key, at,
                                   "succeeded", 1, 1)
        queue.put(("ok", created, record))
    except Exception as exc:  # noqa: BLE001 - relay the public class
        queue.put((type(exc).__name__, str(exc)))


class CompletionConcurrencyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        t = self.tmp.name
        self.jobs = os.path.join(t, "jobs.json")
        self.supply = os.path.join(t, "supply.json")
        self.trades = os.path.join(t, "clear.json")
        self.dispatch = os.path.join(t, "dispatch.json")
        self.execution = os.path.join(t, "execution.json")
        self.completions = os.path.join(t, "completions.json")
        jobs_module.submit(self.jobs, _job(), "jk-1")
        resources_module.publish(self.supply, _resource(), "rk-1")
        clear(self.jobs, self.supply, self.trades, "j-1", "tk-1", 40)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-1", "ck-1", 50)
        dispatch_module.claim(self.dispatch, "j-1", "lk-1", "worker-1",
                              30, 60)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "pk-1",
                              "worker-1", None, 61)
        execution_module.record(self.execution, "j-1", 1, "s-1",
                                "worker-1", "stage", "succeeded", "staged",
                                62)
        execution_module.record(self.execution, "j-1", 1, "s-2",
                                "worker-1", "start", "succeeded", "started",
                                63)
        dispatch_module.finish(self.dispatch, "j-1", "fk-1", "worker-1",
                               "succeeded", 64)

    def _payload(self) -> tuple[str, ...]:
        return (self.jobs, self.supply, self.trades, self.dispatch,
                self.execution, self.completions)

    def test_concurrent_same_key_registers_once(self) -> None:
        queue: Queue = Queue()
        procs = [
            Process(target=_register_worker,
                    args=(self._payload(), "j-1", "x-1", 70, queue))
            for _ in range(4)]
        for proc in procs:
            proc.start()
        results = [queue.get() for _ in procs]
        for proc in procs:
            proc.join()
        self.assertEqual(sorted(kind for kind, *_ in results),
                         ["ok", "ok", "ok", "ok"])
        created_flags = [created for _kind, created, _record in results]
        self.assertEqual(sorted(created_flags), [False, False, False, True])
        records = [record for _kind, _created, record in results]
        self.assertTrue(all(item == records[0] for item in records))
        self.assertEqual(get(self.completions, "j-1"), records[0])

    def test_concurrent_distinct_keys_one_wins(self) -> None:
        queue: Queue = Queue()
        procs = [
            Process(target=_register_worker,
                    args=(self._payload(), "j-1", f"x-{i}", 70, queue))
            for i in range(4)]
        for proc in procs:
            proc.start()
        results = [queue.get() for _ in procs]
        for proc in procs:
            proc.join()
        kinds = [kind for kind, *_ in results]
        self.assertEqual(kinds.count("ok"), 1)
        self.assertEqual(kinds.count("ValueError"), 3)
        self.assertTrue(get(self.completions, "j-1"))


class LiveCompletionWithoutSignalsTest(unittest.TestCase):
    """A live trade with no migration completes without a signals path."""

    def test_register_without_signals_path(self) -> None:
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        t = tmp.name
        jobs = os.path.join(t, "jobs.json")
        supply = os.path.join(t, "supply.json")
        signals = os.path.join(t, "signals.json")
        trades = os.path.join(t, "trades.json")
        dispatch_p = os.path.join(t, "dispatch.json")
        execution_p = os.path.join(t, "execution.json")
        completions = os.path.join(t, "completions.json")
        jobs_module.submit(jobs, _job(), "jk-1")
        resources_module.publish(supply, _resource(), "rk-1")
        signals_module.publish(signals, _signal(), "sg-1")
        clear_live(jobs, supply, signals, trades, "j-1", "tk-1", 40)
        dispatch_module.commit(jobs, supply, trades, dispatch_p, "j-1",
                               "ck-1", 50)
        dispatch_module.claim(dispatch_p, "j-1", "lk-1", "worker-1", 30,
                              60)
        execution_module.plan(jobs, supply, trades, dispatch_p,
                              execution_p, "j-1", "pk-1", "worker-1", None,
                              61)
        execution_module.record(execution_p, "j-1", 1, "s-1", "worker-1",
                                "stage", "succeeded", "staged", 62)
        execution_module.record(execution_p, "j-1", 1, "s-2", "worker-1",
                                "start", "succeeded", "started", 63)
        dispatch_module.finish(dispatch_p, "j-1", "fk-1", "worker-1",
                               "succeeded", 64)
        # No signals argument: there is no advice/intent lineage to
        # resolve frozen signals against.
        record, created = register(jobs, supply, trades, dispatch_p,
                                   execution_p, completions, "j-1", "x-1",
                                   70, "succeeded", 300, 700)
        self.assertTrue(created)
        self.assertEqual(record["current"],
                         {"resource_id": "r-1", "version": 1})


if __name__ == "__main__":
    unittest.main()
