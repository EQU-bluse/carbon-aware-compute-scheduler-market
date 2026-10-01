from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import dispatch as dispatch_module
from carbon_market import execution as execution_module
from carbon_market import execution_sync as execution_sync_module
from carbon_market import jobs as jobs_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.completion import complete, get
from carbon_market.market import clear_live
from carbon_market.rebalance import (apply, evaluate, record as mig_record,
                                     settle, start)


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


class CompletionTest(unittest.TestCase):
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
        self.settlements = os.path.join(base, "settlements.json")
        self.completions = os.path.join(base, "completions.json")
        jobs_module.submit(self.jobs, _job(), "jk-1")
        resources_module.publish(self.supply, _resource(), "rk-1")
        signals_module.publish(self.signals, _signal(), "sk-1")

    # -- fixture helpers -----------------------------------------------------

    def _bootstrap_ledgers(self) -> None:
        # A second, unrelated job brings every shared ledger into
        # existence the way the settle fixtures do, so error cases can
        # distinguish a missing input file from a failed predicate.
        region = "zz-bootstrap"
        jobs_module.submit(
            self.jobs,
            _job("j-9", regions=[region, "eu-north"],
                 residency=[region]), "jk-9")
        resources_module.publish(
            self.supply,
            _resource("r-9", region=region, capacity=1000,
                      residency=[region]), "rk-9")
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

    def _trade(self) -> None:
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-1", "t-1", 10)

    def _commit(self) -> None:
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-1", "d-1", 10)

    def _claim(self, owner: str = "owner-1", lease: int = 30,
               at: int = 60) -> None:
        dispatch_module.claim(self.dispatch, "j-1", "c-1", owner, lease,
                              at)

    def _plan(self, key: str = "p-1", owner: str = "owner-1",
              at: int = 61) -> None:
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", key,
                              owner, None, at)

    def _launch_completed(self) -> None:
        self._trade()
        self._commit()
        self._claim()
        self._plan()
        execution_module.record(self.execution, "j-1", 1, "s-1",
                                "owner-1", "stage", "succeeded", "staged",
                                62)
        execution_module.record(self.execution, "j-1", 1, "s-2",
                                "owner-1", "start", "succeeded", "started",
                                63)

    def _synchronize(self, key: str = "b-1", now: int = 70) -> None:
        execution_sync_module.run(self.execution, self.dispatch, self.sync,
                                  "sync-1", key, now, 50)

    def _finished(self) -> None:
        self._bootstrap_ledgers()
        self._launch_completed()
        self._synchronize()

    def _complete(self, at: int = 70, *, outcome: str = "succeeded",
                  actual_cost: int = 90, actual_carbon: int = 120,
                  key: str = "x-1"):
        return complete(self.jobs, self.supply, self.signals, self.trades,
                        self.dispatch, self.execution, self.completions,
                        "j-1", key, at, outcome, actual_cost,
                        actual_carbon)

    # -- success -------------------------------------------------------------

    def test_complete_freezes_generation_zero_binding(self) -> None:
        self._finished()
        record, created = self._complete()
        self.assertTrue(created)
        self.assertEqual(list(record.keys()),
                         ["job_id", "at", "outcome", "actual_cost",
                          "actual_carbon", "generation", "current",
                          "cost_exceeded", "carbon_exceeded"])
        self.assertEqual(record["job_id"], "j-1")
        self.assertEqual(record["at"], 70)
        self.assertEqual(record["outcome"], "succeeded")
        self.assertEqual(record["actual_cost"], 90)
        self.assertEqual(record["actual_carbon"], 120)
        self.assertEqual(record["generation"], 0)
        self.assertEqual(record["current"],
                         {"resource_id": "r-1", "version": 1})
        self.assertFalse(record["cost_exceeded"])
        self.assertFalse(record["carbon_exceeded"])

    def test_complete_failed_outcome_and_exceeded_flags(self) -> None:
        self._finished()
        record, created = self._complete(outcome="failed",
                                         actual_cost=1001,
                                         actual_carbon=1000)
        self.assertTrue(created)
        self.assertEqual(record["outcome"], "failed")
        self.assertTrue(record["cost_exceeded"])
        self.assertFalse(record["carbon_exceeded"])

    def test_limits_are_strict_inequalities(self) -> None:
        self._finished()
        record, _ = self._complete(actual_cost=1000, actual_carbon=1000)
        self.assertFalse(record["cost_exceeded"])
        self.assertFalse(record["carbon_exceeded"])

    def test_complete_writes_canonical_ledger(self) -> None:
        self._finished()
        self._complete()
        raw = Path(self.completions).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        self.assertNotIn(b"\\u", raw)
        data = json.loads(raw.decode("utf-8"))
        self.assertEqual(list(data.keys()),
                         ["version", "completions", "idempotency", "audit"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["completions"]), ["x-1"])
        self.assertEqual(list(data["idempotency"]), ["x-1"])
        self.assertEqual(list(data["audit"]), ["x-1"])
        self.assertEqual(data["idempotency"]["x-1"], {
            "job_id": "j-1", "at": 70, "outcome": "succeeded",
            "actual_cost": 90, "actual_carbon": 120})
        self.assertEqual(data["audit"]["x-1"]["result"],
                         data["completions"]["x-1"])

    def test_replay_returns_record_without_rewrite(self) -> None:
        self._finished()
        record, created = self._complete()
        raw = Path(self.completions).read_bytes()
        replayed, created2 = self._complete()
        self.assertFalse(created2)
        self.assertEqual(replayed, record)
        self.assertEqual(Path(self.completions).read_bytes(), raw)

    def test_same_key_with_another_request_is_value_error(self) -> None:
        self._finished()
        self._complete()
        with self.assertRaises(ValueError):
            self._complete(at=71)
        with self.assertRaises(ValueError):
            self._complete(outcome="failed")
        with self.assertRaises(ValueError):
            self._complete(actual_cost=91)
        with self.assertRaises(ValueError):
            self._complete(actual_carbon=121)
        # Nothing was written after the first commit.
        self.assertEqual(
            list(json.loads(Path(self.completions).read_text())[
                "completions"]),
            ["x-1"])

    def test_same_job_under_another_key_is_value_error(self) -> None:
        self._finished()
        self._complete()
        with self.assertRaises(ValueError):
            self._complete(key="x-2")

    def test_get_returns_record_and_raises_key_error(self) -> None:
        self._finished()
        with self.assertRaises(KeyError):
            get(self.completions, "j-1")
        self._complete()
        record = get(self.completions, "j-1")
        self.assertEqual(record["job_id"], "j-1")
        self.assertEqual(record["generation"], 0)
        with self.assertRaises(KeyError):
            get(self.completions, "j-nope")
        # A missing completion ledger is an unknown terminal state too.
        with self.assertRaises(KeyError):
            get(os.path.join(self.tmp.name, "missing.json"), "j-1")

    # -- stable terminal predicate ------------------------------------------

    def test_unknown_job_is_key_error(self) -> None:
        self._finished()
        with self.assertRaises(KeyError):
            complete(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.completions,
                     "j-nope", "x-1", 70, "succeeded", 1, 1)

    def test_dispatch_not_succeeded_is_permission_error(self) -> None:
        self._bootstrap_ledgers()
        self._launch_completed()
        # The plan is completed but the decision is still claimed until
        # the synchronization finishes it.
        with self.assertRaises(PermissionError):
            self._complete()

    def test_launch_plan_not_completed_is_permission_error(self) -> None:
        self._bootstrap_ledgers()
        self._trade()
        self._commit()
        self._claim()
        self._plan()
        execution_module.record(self.execution, "j-1", 1, "s-1",
                                "owner-1", "stage", "succeeded", "staged",
                                62)
        with self.assertRaises(PermissionError):
            self._complete(at=65)

    def test_no_launch_plan_is_permission_error(self) -> None:
        self._bootstrap_ledgers()
        self._trade()
        self._commit()
        with self.assertRaises(PermissionError):
            self._complete(at=70)

    def test_untraded_job_is_permission_error(self) -> None:
        # All ledgers exist (bootstrapped by j-9), but j-1 was accepted
        # and never traded: the stable-terminal predicate fails.
        self._bootstrap_ledgers()
        with self.assertRaises(PermissionError):
            self._complete()

    def test_completion_before_last_evidence_is_value_error(self) -> None:
        self._finished()
        with self.assertRaisesRegex(ValueError, "evidence"):
            self._complete(at=62)

    def test_active_migration_blocks_completion(self) -> None:
        self._prepare_migrated_round(terminal=False)
        with self.assertRaises(PermissionError):
            complete(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.completions,
                     "j-1", "x-1", 95, "succeeded", 1, 1)

    def test_unsettled_terminal_migration_blocks_completion(self) -> None:
        self._prepare_migrated_round(terminal=True, settled=False)
        with self.assertRaises(PermissionError):
            complete(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.completions,
                     "j-1", "x-1", 95, "succeeded", 1, 1)

    def test_settled_migration_freezes_generation_one(self) -> None:
        self._prepare_migrated_round(terminal=True, settled=True)
        # The migration settled at 55; the job now actually runs on its
        # new binding, finishes it, and completes there.
        dispatch_module.claim(self.dispatch, "j-1", "c-2", "owner-1", 20,
                              80)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "p-2",
                              "owner-1", None, 81)
        execution_module.record(self.execution, "j-1", 1, "s-3",
                                "owner-1", "stage", "succeeded", "staged",
                                82)
        execution_module.record(self.execution, "j-1", 1, "s-4",
                                "owner-1", "start", "succeeded", "started",
                                83)
        self._synchronize(key="b-2", now=90)
        record, created = complete(
            self.jobs, self.supply, self.signals, self.trades,
            self.dispatch, self.execution, self.completions, "j-1", "x-1",
            90, "succeeded", 5, 5)
        self.assertTrue(created)
        self.assertEqual(record["generation"], 1)
        # The round migrated r-2 (trade) onto r-1.
        self.assertEqual(record["current"],
                         {"resource_id": "r-1", "version": 1})

    # -- argument and file errors -------------------------------------------

    def test_invalid_arguments(self) -> None:
        self._finished()
        kwargs = dict(jobs=self.jobs, supply=self.supply,
                      signals=self.signals, trades=self.trades,
                      dispatch=self.dispatch, execution=self.execution,
                      completions=self.completions, job_id="j-1",
                      key="x-1", at=70, outcome="succeeded", actual_cost=1,
                      actual_carbon=1)
        for field, value in (("at", -1), ("at", True), ("outcome", "nope"),
                             ("actual_cost", -1), ("actual_cost", True),
                             ("actual_cost", 1.5), ("actual_carbon", -1),
                             ("job_id", ""), ("key", ""),
                             ("completions", "")):
            with self.subTest(field=field):
                bad = dict(kwargs)
                bad[field] = value
                with self.assertRaises(ValueError):
                    complete(**bad)

    def test_paths_must_be_distinct(self) -> None:
        self._finished()
        with self.assertRaises(ValueError):
            complete(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.trades, "j-1",
                     "x-1", 70, "succeeded", 1, 1)

    def test_missing_input_is_file_not_found(self) -> None:
        self._finished()
        missing = os.path.join(self.tmp.name, "missing.json")
        with self.assertRaises(FileNotFoundError):
            complete(missing, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.completions,
                     "j-1", "x-1", 70, "succeeded", 1, 1)

    def test_missing_parent_is_file_not_found_and_leaves_no_trace(
            self) -> None:
        self._finished()
        target = os.path.join(self.tmp.name, "nope", "completions.json")
        with self.assertRaises(FileNotFoundError):
            complete(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, target, "j-1", "x-1",
                     70, "succeeded", 1, 1)
        self.assertFalse(Path(target).exists())

    def test_failed_request_changes_nothing(self) -> None:
        self._finished()
        before = {p: Path(p).read_bytes() for p in (
            self.jobs, self.supply, self.signals, self.trades,
            self.dispatch, self.execution, self.sync)}
        with self.assertRaises(ValueError):
            self._complete(at=1)
        for path, raw in before.items():
            self.assertEqual(Path(path).read_bytes(), raw)
        self.assertFalse(Path(self.completions).exists())

    # -- migration round fixture (r-2 trade -> r-1 migration) ----------------

    def _prepare_migrated_round(self, *, terminal: bool,
                                settled: bool = False) -> None:
        resources_module.publish(
            self.supply,
            _resource("r-2", region="us-west",
                      residency=["eu-north", "us-west"], unit_cost=3,
                      carbon_intensity=2), "rk-2")
        signals_module.publish(
            self.signals, _signal("us-west", unit_cost=3,
                                  carbon_intensity=2), "sk-2")
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=15, expires=300, unit_cost=1,
                    carbon_intensity=1), "sk-3")
        self._bootstrap_ledgers()
        # Trade at 10 lands on the cleaner r-2 region.
        self._trade()
        self._commit()
        seven = (self.jobs, self.supply, self.signals, self.trades,
                 self.dispatch, self.execution, self.advice)
        eight = seven + (self.intents,)
        nine = eight + (self.settlements,)
        evaluate(*seven, "j-1", "a-1", 30)
        apply(*eight, "j-1", "a-1", "r-1", 40)
        start(*eight, "j-1", "m-1", "owner-2", 85, 45)
        mig_record(*eight, "j-1", "m-2", "owner-2", "copy", "succeeded",
                   "copy-ok", 50)
        if terminal:
            mig_record(*eight, "j-1", "m-4", "owner-2", "switch",
                       "succeeded", "switch-ok", 52)
            if settled:
                settle(*nine, "j-1", "m-1", "s-1", 55)
        self.migration_eight = eight
        self.migration_nine = nine


class CompletionCapacityTest(unittest.TestCase):
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
        self.completions = os.path.join(base, "completions.json")
        jobs_module.submit(self.jobs, _job("j-1"), "jk-1")
        # Capacity 10, work 10: one job fills the version completely.
        resources_module.publish(
            self.supply, _resource(capacity=10), "rk-1")
        signals_module.publish(self.signals, _signal(), "sk-1")

    def _finish_job(self, job_id: str, index: int, last_at: int = 63) -> None:
        jobs_module.submit(self.jobs, _job(job_id), f"jk-{index}")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   job_id, f"t-{index}", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, job_id, f"d-{index}", 10)
        dispatch_module.claim(self.dispatch, job_id, f"c-{index}",
                              f"owner-{index}", 30, 60)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, job_id,
                              f"p-{index}", f"owner-{index}", None, 61)
        execution_module.record(self.execution, job_id, 1, f"s1-{index}",
                                f"owner-{index}", "stage", "succeeded",
                                "staged", last_at - 1)
        execution_module.record(self.execution, job_id, 1, f"s2-{index}",
                                f"owner-{index}", "start", "succeeded",
                                "started", last_at)
        execution_sync_module.run(self.execution, self.dispatch, self.sync,
                                  "sync-1", f"b-{index}", 70 + index, 50)

    def _complete(self, job_id: str, at: int) -> None:
        complete(self.jobs, self.supply, self.signals, self.trades,
                 self.dispatch, self.execution, self.completions, job_id,
                 f"x-{job_id}", at, "succeeded", 1, 1)

    def test_completion_releases_capacity_for_later_clear(self) -> None:
        self._finish_job("j-1", 1)
        # No room for a second trade while j-1 still occupies capacity.
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        with self.assertRaises(LookupError):
            clear_live(self.jobs, self.supply, self.signals, self.trades,
                       "j-2", "t-2", 70)
        self._complete("j-1", 70)
        # Without the completion ledger the result is unchanged: still
        # no room even though the ledger sits next to the trades.
        with self.assertRaises(LookupError):
            clear_live(self.jobs, self.supply, self.signals, self.trades,
                       "j-2", "t-2", 71)
        # With it, j-1's booking no longer occupies the version.
        trade, created = clear_live(
            self.jobs, self.supply, self.signals, self.trades, "j-2",
            "t-2b", 72, self.completions)
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-1")

    def test_future_completion_does_not_release_yet(self) -> None:
        self._finish_job("j-1", 1)
        self._complete("j-1", 80)
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        # The completion lies in the future of the evaluation moment.
        with self.assertRaises(LookupError):
            clear_live(self.jobs, self.supply, self.signals, self.trades,
                       "j-2", "t-2", 70, self.completions)
        trade, _ = clear_live(self.jobs, self.supply, self.signals,
                              self.trades, "j-2", "t-2b", 80,
                              self.completions)
        self.assertEqual(trade["resource_id"], "r-1")

    def test_reused_capacity_keeps_other_layers_readable(self) -> None:
        # After completion frees the version and a new trade reuses it,
        # the trades ledger's aggregate work exceeds the published
        # capacity; every other reader must accept it as a legal
        # book-release-book envelope.
        self._finish_job("j-1", 1)
        self._complete("j-1", 70)
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-2", "t-2b", 72, self.completions)
        # dispatch.commit reads the trades ledger for j-2.
        decision = dispatch_module.commit(
            self.jobs, self.supply, self.trades, self.dispatch, "j-2",
            "d-2", 73)
        self.assertEqual(decision[0]["job_id"], "j-2")

    def test_completed_job_cannot_be_advised(self) -> None:
        self._finish_job("j-1", 1)
        self._complete("j-1", 70)
        with self.assertRaises(ValueError):
            evaluate(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.advice, "j-1",
                     "a-1", 71, self.completions)
        # The advice ledger never comes into existence on the refused
        # path.
        self.assertFalse(Path(self.advice).exists())


if __name__ == "__main__":
    unittest.main()
