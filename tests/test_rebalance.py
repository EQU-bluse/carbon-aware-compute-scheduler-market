from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import dispatch as dispatch_module
from carbon_market import execution as execution_module
from carbon_market import jobs as jobs_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.market import clear, clear_live
from carbon_market.rebalance import evaluate


def _resource(resource_id: str = "r-1", **overrides: object) -> dict[str, object]:
    resource: dict[str, object] = {
        "resource_id": resource_id,
        "region": "eu-north",
        "capacity": 100,
        "start": 0,
        "end": 500,
        "unit_cost": 5,
        "carbon_intensity": 8,
        "residency": ["eu-north"],
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


class RebalanceTest(unittest.TestCase):
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
        self.ledger = os.path.join(base, "advice.json")
        self.paths = (self.jobs, self.supply, self.signals, self.trades,
                      self.dispatch, self.execution, self.ledger)
        jobs_module.submit(self.jobs, _job(), "jk-1")

    def _seed_single(self) -> None:
        publish_resource = resources_module.publish
        publish_resource(self.supply, _resource("r-1"), "k1")
        signals_module.publish(self.signals, _signal("eu-north"), "s1")

    def _seed_two_regions(self) -> None:
        resources_module.publish(self.supply, _resource("r-1"), "k1")
        resources_module.publish(
            self.supply,
            _resource("r-2", region="us-west",
                      residency=["eu-north", "us-west"]), "k2")
        signals_module.publish(
            self.signals,
            _signal("eu-north", unit_cost=5, carbon_intensity=8), "s1")
        signals_module.publish(
            self.signals,
            _signal("us-west", unit_cost=3, carbon_intensity=2), "s2")

    def _commit_job(self, job_id: str = "j-1", at: int = 10,
                    live: bool = True, key_trade: str = "t1",
                    key_dispatch: str = "d1") -> None:
        if live:
            clear_live(self.jobs, self.supply, self.signals, self.trades,
                       job_id, key_trade, at)
        else:
            clear(self.jobs, self.supply, self.trades, job_id,
                  key_trade, at)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, job_id, key_dispatch, at)
        self._ensure_execution_ledger()

    def _ensure_execution_ledger(self) -> None:
        # A fully isolated bootstrap job on its own region creates the
        # execution ledger without affecting the evaluated job's
        # candidates, capacity or dispatch: it clears onto its own
        # resource and is left with an active launch plan. The evaluated
        # job then reads with no plans of its own (state "none").
        region = "zz-bootstrap"
        jobs_module.submit(
            self.jobs,
            _job("j-9", regions=[region], residency=[region]), "jk-9")
        resources_module.publish(
            self.supply,
            _resource("r-9", region=region, capacity=1000,
                      residency=[region]), "k-9")
        signals_module.publish(
            self.signals, _signal(region, carbon_intensity=1), "s-9")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-9", "t-9", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-9", "d-9", 10)
        dispatch_module.claim(self.dispatch, "j-9", "c-9", "owner-9", 80,
                              10)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-9", "e-9",
                              "owner-9", None, 10)

    def _evaluate(self, job_id: str = "j-1", key: str = "a1",
                  at: int = 20):
        return evaluate(self.jobs, self.supply, self.signals, self.trades,
                        self.dispatch, self.execution, self.ledger,
                        job_id, key, at)

    # -- basic outcomes ----------------------------------------------------

    def test_keeps_when_current_is_greenest(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        record, created = self._evaluate()
        self.assertTrue(created)
        self.assertEqual(record["recommendation"], "keep")
        self.assertEqual(record["target"], record["current"])
        self.assertEqual(record["current"],
                         {"resource_id": "r-2", "version": 1})
        self.assertEqual(record["dispatch"], "ready")
        self.assertEqual(record["execution"], "none")
        self.assertEqual(list(record.keys()),
                         ["job_id", "at", "current", "dispatch",
                          "execution", "candidates", "recommendation",
                          "target"])
        self.assertEqual(
            [c["resource"]["resource_id"] for c in record["candidates"]],
            ["r-2", "r-1"])
        for candidate in record["candidates"]:
            self.assertEqual(list(candidate.keys()),
                             ["resource", "signal", "total_cost",
                              "total_carbon"])

    def test_single_resource_advises_keep(self) -> None:
        self._seed_single()
        self._commit_job()
        record, created = self._evaluate()
        self.assertTrue(created)
        self.assertEqual(record["recommendation"], "keep")
        self.assertEqual(
            [c["resource"]["resource_id"] for c in record["candidates"]],
            ["r-1"])

    def test_migrates_when_later_signal_makes_another_resource_greener(
            self) -> None:
        self._seed_two_regions()
        self._commit_job()
        # A newer, cleaner eu-north signal observed after the trade.
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=15, expires=300, unit_cost=1,
                    carbon_intensity=1), "s3")
        record, created = self._evaluate(at=30)
        self.assertTrue(created)
        self.assertEqual(record["recommendation"], "migrate")
        self.assertEqual(record["target"],
                         {"resource_id": "r-1", "version": 1})
        self.assertEqual(record["current"],
                         {"resource_id": "r-2", "version": 1})
        ordered = [(c["resource"]["resource_id"],
                    c["signal"]["version"]) for c in record["candidates"]]
        self.assertEqual(ordered, [("r-1", 2), ("r-2", 1)])

    def test_static_trade_is_repriced_on_live_signals(self) -> None:
        # Supply figures make eu/r-1 the static winner (carbon 1), while
        # live signals later show us/r-2 cleaner (carbon 2 vs 8).
        resources_module.publish(
            self.supply,
            _resource("r-1", unit_cost=1, carbon_intensity=1), "k1")
        resources_module.publish(
            self.supply,
            _resource("r-2", region="us-west", unit_cost=9,
                      carbon_intensity=9,
                      residency=["eu-north", "us-west"]), "k2")
        signals_module.publish(
            self.signals,
            _signal("eu-north", unit_cost=5, carbon_intensity=8), "s1")
        signals_module.publish(
            self.signals,
            _signal("us-west", unit_cost=3, carbon_intensity=2), "s2")
        self._commit_job(live=False)
        record, _ = self._evaluate()
        self.assertTrue(all("signal" in c for c in record["candidates"]))
        self.assertEqual(
            [c["resource"]["resource_id"] for c in record["candidates"]],
            ["r-2", "r-1"])
        self.assertEqual(record["current"],
                         {"resource_id": "r-1", "version": 1})
        self.assertEqual(record["recommendation"], "migrate")

    def test_signal_versions_freeze_in_record(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        record, _ = self._evaluate(at=20)
        # Republishing signals must not change the stored record bytes.
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=15, expires=400, unit_cost=0,
                    carbon_intensity=0), "s9")
        replayed, created = self._evaluate(at=20)
        self.assertFalse(created)
        self.assertEqual(replayed, record)

    # -- retained vs migration version rules ------------------------------

    def test_retained_item_uses_frozen_version_not_a_newer_one(self) -> None:
        # The trade freezes r-1 v1; a later v2 valid at the evaluation
        # moment must never replace or accompany it as the retained item.
        resources_module.publish(
            self.supply, _resource("r-1", start=0, end=500), "k1")
        signals_module.publish(self.signals, _signal("eu-north"), "s1")
        self._commit_job()
        resources_module.publish(
            self.supply, _resource("r-1", start=60, end=600), "k9")
        record, created = self._evaluate(at=100, key="a9")
        self.assertTrue(created)
        self.assertEqual(record["current"],
                         {"resource_id": "r-1", "version": 1})
        self.assertEqual(
            [(c["resource"]["resource_id"], c["resource"]["version"])
             for c in record["candidates"]],
            [("r-1", 1)])

    def test_migration_item_uses_highest_valid_version(self) -> None:
        # The trade books r-1 at t=10; another resource r-3 has v1 valid
        # all along and a v2 valid from 60. At t=100 the migration
        # candidate for r-3 must be its highest valid version, v2.
        resources_module.publish(self.supply, _resource("r-1"), "k1")
        resources_module.publish(
            self.supply,
            _resource("r-3", region="us-west", end=500, unit_cost=9,
                      carbon_intensity=9,
                      residency=["eu-north", "us-west"]), "k3a")
        resources_module.publish(
            self.supply,
            _resource("r-3", region="us-west", start=60, end=600,
                      unit_cost=1, carbon_intensity=1,
                      residency=["eu-north", "us-west"]), "k3b")
        signals_module.publish(
            self.signals,
            _signal("eu-north", unit_cost=8, carbon_intensity=8), "s1")
        signals_module.publish(
            self.signals,
            _signal("us-west", observed=0, expires=30, unit_cost=9,
                    carbon_intensity=9), "s2")
        # At trade time us-west is dirtier (9 vs 8), so r-1 wins.
        self._commit_job()
        # A later, clean us-west signal makes r-3 the advice winner.
        signals_module.publish(
            self.signals,
            _signal("us-west", observed=20, expires=400, unit_cost=1,
                    carbon_intensity=1), "s3")
        record, _ = self._evaluate(at=100, key="a9")
        self.assertEqual(record["recommendation"], "migrate")
        self.assertEqual(record["target"],
                         {"resource_id": "r-3", "version": 2})

    def test_migration_never_uses_another_version_of_current_resource(
            self) -> None:
        resources_module.publish(self.supply, _resource("r-1"), "k1")
        signals_module.publish(self.signals, _signal("eu-north"), "s1")
        self._commit_job()
        # A cleaner r-1 v2 must not appear as a migration candidate: only
        # the frozen v1 is retained, even once v2 is valid.
        resources_module.publish(
            self.supply,
            _resource("r-1", start=60, end=500, unit_cost=0,
                      carbon_intensity=0), "k9")
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=60, expires=400, unit_cost=0,
                    carbon_intensity=0), "s9")
        record, _ = self._evaluate(at=100, key="a9")
        self.assertEqual(
            [c["resource"]["version"] for c in record["candidates"]], [1])
        self.assertEqual(record["recommendation"], "keep")

    def test_region_without_current_signal_removes_alternative(self) -> None:
        # The trade books r-1; r-2 is an alternative whose signal window
        # closes before the evaluation. At evaluation r-2 simply drops
        # out and the retained r-1 stays -- a missing signal is never an
        # error by itself.
        resources_module.publish(self.supply, _resource("r-1"), "k1")
        resources_module.publish(
            self.supply,
            _resource("r-2", region="us-west", carbon_intensity=1,
                      residency=["eu-north", "us-west"]), "k2")
        signals_module.publish(
            self.signals,
            _signal("eu-north", carbon_intensity=8), "s1")
        signals_module.publish(
            self.signals,
            _signal("us-west", observed=0, expires=19, carbon_intensity=20),
            "s2")
        self._commit_job()
        record, _ = self._evaluate(at=50, key="a9")
        self.assertEqual(
            [c["resource"]["resource_id"] for c in record["candidates"]],
            ["r-1"])
        self.assertEqual(record["recommendation"], "keep")

    def test_expired_signal_on_booked_region_leaves_empty_set(self) -> None:
        # When the booked region itself loses its signal, even the
        # retained item cannot be priced and the set is empty.
        resources_module.publish(self.supply, _resource("r-1"), "k1")
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=0, expires=19), "s1")
        self._commit_job()
        with self.assertRaises(LookupError):
            self._evaluate(at=50, key="a9")
        self.assertFalse(os.path.exists(self.ledger))

    def test_no_feasible_item_raises_lookup_error(self) -> None:
        resources_module.publish(self.supply, _resource("r-1"), "k1")
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=0, expires=19), "s1")
        self._commit_job()
        with self.assertRaises(LookupError):
            self._evaluate(at=100, key="a9")
        self.assertFalse(os.path.exists(self.ledger))

    # -- ordering and constraints ------------------------------------------

    def test_ordering_by_carbon_then_cost_then_id(self) -> None:
        resources_module.publish(
            self.supply,
            _resource("r-1", region="eu-a", unit_cost=9, carbon_intensity=4,
                      residency=["eu-a", "eu-b", "eu-c"]), "k1")
        resources_module.publish(
            self.supply,
            _resource("r-2", region="eu-b", unit_cost=3, carbon_intensity=2,
                      residency=["eu-a", "eu-b", "eu-c"]), "k2")
        resources_module.publish(
            self.supply,
            _resource("r-3", region="eu-c", unit_cost=1, carbon_intensity=2,
                      residency=["eu-a", "eu-b", "eu-c"]), "k3")
        for region, cost, carbon in (
                ("eu-a", 9, 4), ("eu-b", 3, 2), ("eu-c", 1, 2)):
            signals_module.publish(
                self.signals,
                _signal(region, unit_cost=cost, carbon_intensity=carbon),
                f"s-{region}")
        jobs_module.submit(
            self.jobs,
            _job("j-x", regions=["eu-a", "eu-b", "eu-c"],
                 residency=["eu-a"]), "jk-x")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-x", "tx", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-x", "dx", 10)
        self._ensure_execution_ledger()
        record, _ = evaluate(
            self.jobs, self.supply, self.signals, self.trades,
            self.dispatch, self.execution,
            os.path.join(self.tmp.name, "advice-x.json"),
            "j-x", "ax", 20)
        # Carbon 2 first (r-3 by lower cost, then r-2), then carbon 4.
        self.assertEqual(
            [c["resource"]["resource_id"] for c in record["candidates"]],
            ["r-3", "r-2", "r-1"])

    def test_budget_exclusion_uses_current_signal(self) -> None:
        self._seed_single()
        self._commit_job()
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=15, expires=400,
                    unit_cost=1000, carbon_intensity=1), "s9")
        with self.assertRaises(LookupError):
            self._evaluate(at=100, key="a9")

    def test_capacity_deducts_other_jobs_trades_but_not_own(self) -> None:
        # Each resource holds exactly the job's 10 capacity. j-1 clears
        # to r-2 (us-west signal is cleaner); j-2, eu-only, fills r-1.
        resources_module.publish(
            self.supply, _resource("r-1", capacity=10), "k1")
        resources_module.publish(
            self.supply,
            _resource("r-2", region="us-west", capacity=10,
                      residency=["eu-north", "us-west"]), "k2")
        signals_module.publish(
            self.signals,
            _signal("eu-north", unit_cost=1, carbon_intensity=5), "s1")
        signals_module.publish(
            self.signals,
            _signal("us-west", unit_cost=1, carbon_intensity=1), "s2")
        jobs_module.submit(
            self.jobs,
            _job("j-2", regions=["eu-north"], residency=["eu-north"]),
            "jk-2")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-1", "t1", 10)
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-2", "t2", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-1", "d1", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-2", "d2", 10)
        self._ensure_execution_ledger()
        record, _ = self._evaluate(at=20)
        # j-1's frozen r-2 survives its own 10/10 occupancy (never
        # deducted twice); r-1 is full with j-2's trade and drops out.
        ids = [c["resource"]["resource_id"] for c in record["candidates"]]
        self.assertEqual(ids, ["r-2"])
        self.assertEqual(record["recommendation"], "keep")

    # -- lifecycle guards --------------------------------------------------

    def test_succeeded_dispatch_raises_value_error(self) -> None:
        self._seed_single()
        self._commit_job()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 80, 10)
        dispatch_module.finish(self.dispatch, "j-1", "f1", "owner",
                               "succeeded", 20)
        with self.assertRaises(ValueError):
            self._evaluate(at=30, key="a9")
        self.assertFalse(os.path.exists(self.ledger))

    def test_completed_plan_raises_value_error(self) -> None:
        self._seed_single()
        self._commit_job()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 80, 10)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "e1",
                              "owner", None, 20)
        execution_module.record(self.execution, "j-1", 1, "e2", "owner",
                                "stage", "succeeded", "rc1", 25)
        execution_module.record(self.execution, "j-1", 1, "e3", "owner",
                                "start", "succeeded", "rc2", 30)
        with self.assertRaises(ValueError):
            self._evaluate(at=40, key="a9")
        self.assertFalse(os.path.exists(self.ledger))

    def test_active_plan_raises_permission_error(self) -> None:
        self._seed_single()
        self._commit_job()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 80, 10)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "e1",
                              "owner", None, 20)
        with self.assertRaises(PermissionError):
            self._evaluate(at=30, key="a9")
        self.assertFalse(os.path.exists(self.ledger))

    def test_past_deadline_raises_timeout_error(self) -> None:
        self._seed_single()
        self._commit_job()
        with self.assertRaises(TimeoutError):
            self._evaluate(at=101, key="a9")
        self.assertFalse(os.path.exists(self.ledger))

    def test_failed_and_interrupted_plans_allow_re_evaluation(self) -> None:
        self._seed_single()
        self._commit_job()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 20, 10)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "e1",
                              "owner", None, 12)
        execution_module.record(self.execution, "j-1", 1, "e2", "owner",
                                "stage", "failed", "boom", 13)
        record, created = self._evaluate(at=30, key="a-failed")
        self.assertTrue(created)
        self.assertEqual(record["execution"], "failed")

        # Settle the dispatch decision as failed, then start a second
        # attempt and interrupt it after its short lease expires.
        dispatch_module.finish(self.dispatch, "j-1", "f1", "owner",
                               "failed", 13)
        dispatch_module.claim(self.dispatch, "j-1", "c2", "owner", 5, 30)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "e3",
                              "owner", None, 31)
        execution_module.recover(self.execution, "j-1", 2, "e4", 40)
        record, created = self._evaluate(at=50, key="a-interrupted")
        self.assertTrue(created)
        self.assertEqual(record["execution"], "interrupted")

    def test_claimed_decision_without_plan_is_evaluable(self) -> None:
        self._seed_single()
        self._commit_job()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 80, 10)
        record, created = self._evaluate(at=20, key="a9")
        self.assertTrue(created)
        self.assertEqual(record["dispatch"], "claimed")
        self.assertEqual(record["execution"], "none")

    # -- idempotency --------------------------------------------------------

    def test_replay_returns_stored_record_without_write(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        first, created_first = self._evaluate()
        self.assertTrue(created_first)
        before = Path(self.ledger).read_bytes()
        second, created_second = self._evaluate()
        self.assertFalse(created_second)
        self.assertEqual(second, first)
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_same_key_changed_request_raises(self) -> None:
        self._seed_single()
        self._commit_job()
        self._evaluate(at=20)
        before = Path(self.ledger).read_bytes()
        with self.assertRaises(ValueError):
            self._evaluate(at=21)
        self.assertEqual(Path(self.ledger).read_bytes(), before)
        jobs_module.submit(
            self.jobs,
            _job("j-2", regions=["eu-north"], residency=["eu-north"]),
            "jk-2")
        with self.assertRaises(ValueError):
            evaluate(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.ledger,
                     "j-2", "a1", 20)

    # -- errors that must not create the ledger ----------------------------

    def test_unknown_job_raises_key_error(self) -> None:
        self._seed_single()
        self._commit_job()
        with self.assertRaises(KeyError):
            self._evaluate(job_id="j-nope", key="a9")
        self.assertFalse(os.path.exists(self.ledger))

    def test_trade_without_dispatch_raises_key_error(self) -> None:
        self._seed_single()
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-1", "t1", 10)
        # The bootstrap creates the dispatch and execution ledgers for
        # another job, so j-1's missing decision is the error surfaced.
        self._ensure_execution_ledger()
        with self.assertRaises(KeyError):
            self._evaluate()
        self.assertFalse(os.path.exists(self.ledger))

    def test_accepted_job_without_trade_raises_lookup_error(self) -> None:
        # j-9 creates every ledger, while j-1 is merely committed-less
        # (only accepted), so the ledgers exist but j-1 has no trade.
        self._seed_single()
        self._ensure_execution_ledger()
        with self.assertRaises(LookupError):
            self._evaluate()
        self.assertFalse(os.path.exists(self.ledger))

    def test_missing_inputs_raise_file_not_found(self) -> None:
        self._seed_single()
        self._commit_job()
        missing = [os.path.join(self.tmp.name, name) for name in (
            "no-jobs.json", "no-supply.json", "no-signals.json",
            "no-trades.json", "no-dispatch.json", "no-execution.json")]
        replacements = [
            (missing[0], self.supply, self.signals, self.trades,
             self.dispatch, self.execution),
            (self.jobs, missing[1], self.signals, self.trades,
             self.dispatch, self.execution),
            (self.jobs, self.supply, missing[2], self.trades,
             self.dispatch, self.execution),
            (self.jobs, self.supply, self.signals, missing[3],
             self.dispatch, self.execution),
            (self.jobs, self.supply, self.signals, self.trades,
             missing[4], self.execution),
            (self.jobs, self.supply, self.signals, self.trades,
             self.dispatch, missing[5]),
        ]
        for replaced in replacements:
            with self.subTest(replaced=replaced):
                with self.assertRaises(FileNotFoundError):
                    evaluate(*replaced, self.ledger, "j-1", "a1", 20)

    def test_missing_ledger_parent_raises_file_not_found(self) -> None:
        self._seed_single()
        self._commit_job()
        missing = os.path.join(self.tmp.name, "no-such-dir", "advice.json")
        with self.assertRaises(FileNotFoundError):
            evaluate(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, missing,
                     "j-1", "a1", 20)
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    # -- argument validation -----------------------------------------------

    def test_invalid_arguments(self) -> None:
        with self.assertRaises(ValueError):
            evaluate("", self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.ledger,
                     "j-1", "a1", 20)
        with self.assertRaises(ValueError):
            evaluate(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.ledger,
                     "", "a1", 20)
        with self.assertRaises(ValueError):
            evaluate(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.ledger,
                     "j-1", "", 20)
        with self.assertRaises(ValueError):
            evaluate(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.ledger,
                     "j-1", "a1", -1)
        with self.assertRaises(ValueError):
            evaluate(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.ledger,
                     "j-1", "a1", True)  # type: ignore[arg-type]

    def test_paths_must_be_distinct(self) -> None:
        args = [self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.ledger, "j-1", "a1", 20]
        for index in range(7):
            broken = list(args)
            # Collide this position with the next path so two positions
            # resolve to the same real location while all others differ.
            broken[index] = args[(index + 1) % 7]
            with self.assertRaises(ValueError):
                evaluate(*broken)

    def test_replay_still_returns_record_after_booking_finishes(self) -> None:
        self._seed_single()
        self._commit_job()
        first, _ = self._evaluate(at=20)
        # Run the bootstrap-independent lifecycle of j-1 to completion.
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 70, 20)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "e1",
                              "owner", None, 25)
        execution_module.record(self.execution, "j-1", 1, "e2", "owner",
                                "stage", "succeeded", "r1", 30)
        execution_module.record(self.execution, "j-1", 1, "e3", "owner",
                                "start", "succeeded", "r2", 35)
        dispatch_module.finish(self.dispatch, "j-1", "f1", "owner",
                               "succeeded", 35)
        # A fresh evaluation is now refused, but the key still replays its
        # frozen record without writing.
        with self.assertRaises(ValueError):
            self._evaluate(at=40, key="a2")
        before = Path(self.ledger).read_bytes()
        replayed, created = self._evaluate(at=20)
        self.assertFalse(created)
        self.assertEqual(replayed, first)
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    # -- ledger form --------------------------------------------------------

    def test_ledger_is_canonical_with_sorted_sections(self) -> None:
        self._seed_single()
        self._commit_job()
        # Keys chosen to sort independently of insertion order, including
        # a non-ASCII idempotency key that must be written through.
        for job_id, key, trade_key, dispatch_key in (
                ("j-1", "z9", "t1", "d1"),
                ("j-2", "é-mid", "t-2", "d-2"),
                ("j-3", "a-first", "t-3", "d-3")):
            if job_id != "j-1":
                jobs_module.submit(
                    self.jobs,
                    _job(job_id, regions=["eu-north"],
                         residency=["eu-north"]), f"jk-{job_id}")
                clear_live(self.jobs, self.supply, self.signals,
                           self.trades, job_id, trade_key, 10)
                dispatch_module.commit(self.jobs, self.supply,
                                       self.trades, self.dispatch, job_id,
                                       dispatch_key, 10)
            evaluate(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.ledger,
                     job_id, key, 20)

        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        # Non-ASCII key written through, never \u-escaped.
        self.assertIn("é-mid".encode("utf-8"), raw)
        self.assertNotIn(b"\\u", raw)
        text = raw.decode("utf-8")
        data = json.loads(text)
        self.assertEqual(list(data.keys()),
                         ["version", "records", "audit"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["records"]), sorted(data["records"]))
        self.assertEqual(list(data["audit"]), sorted(data["audit"]))
        self.assertEqual(set(data["records"]), set(data["audit"]))

    def test_tampered_ledger_raises_value_error(self) -> None:
        self._seed_single()
        self._commit_job()
        self._evaluate()
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        data["records"]["a1"]["recommendation"] = "migrate"
        Path(self.ledger).write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"))
            + "\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            self._evaluate(job_id="j-1", key="a2", at=30)

    def test_no_temp_fragments_left_after_guard_failures(self) -> None:
        self._seed_single()
        self._commit_job()
        with self.assertRaises(TimeoutError):
            self._evaluate(at=200, key="a9")
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    def test_second_evaluation_freezes_its_own_moment(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        first, _ = self._evaluate(key="a1", at=20)
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=25, expires=400, unit_cost=1,
                    carbon_intensity=1), "s9")
        second, created = self._evaluate(key="a2", at=50)
        self.assertTrue(created)
        self.assertNotEqual(second["recommendation"],
                            first["recommendation"])
        self.assertEqual(second["recommendation"], "migrate")


if __name__ == "__main__":
    unittest.main()
