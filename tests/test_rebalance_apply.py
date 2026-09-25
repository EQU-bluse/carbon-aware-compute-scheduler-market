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
from carbon_market.market import clear_live
from carbon_market.rebalance import apply, evaluate


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


class RebalanceApplyTest(unittest.TestCase):
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
        self.ledger = os.path.join(base, "intents.json")
        jobs_module.submit(self.jobs, _job(), "jk-1")

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
                    key_trade: str = "t1", key_dispatch: str = "d1") -> None:
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   job_id, key_trade, at)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, job_id, key_dispatch, at)
        self._ensure_execution_ledger()

    def _ensure_execution_ledger(self) -> None:
        # A fully isolated bootstrap job on its own region creates the
        # execution ledger without affecting the applied job's
        # candidates, capacity or dispatch.
        if os.path.exists(self.execution):
            return
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

    def _green_eu_north(self) -> None:
        # A newer, cleaner eu-north signal observed after the trade, so
        # the advice becomes "migrate" from r-2 to r-1.
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=15, expires=300, unit_cost=1,
                    carbon_intensity=1), "s3")

    def _evaluate(self, job_id: str = "j-1", key: str = "a1",
                  at: int = 20):
        return evaluate(self.jobs, self.supply, self.signals, self.trades,
                        self.dispatch, self.execution, self.advice,
                        job_id, key, at)

    def _migrate_advice(self, job_id: str = "j-1", key: str = "a1",
                        at: int = 30) -> dict[str, object]:
        record, created = self._evaluate(job_id=job_id, key=key, at=at)
        self.assertTrue(created)
        self.assertEqual(record["recommendation"], "migrate")
        return record

    def _apply(self, job_id: str = "j-1", advice_key: str = "a1",
               key: str = "r1", at: int = 40):
        return apply(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.advice,
                     self.ledger, job_id, advice_key, key, at)

    def _prepare_migrate(self) -> dict[str, object]:
        self._seed_two_regions()
        self._commit_job()
        self._green_eu_north()
        return self._migrate_advice()

    def _commit_second_job(self) -> None:
        # j-2 trades at the same moment as j-1, before the clean
        # eu-north signal, so its later advice is a migrate as well.
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-2", "t2", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-2", "d2", 10)
        self._ensure_execution_ledger()

    # -- basic outcomes ----------------------------------------------------

    def test_apply_freezes_full_record(self) -> None:
        advice = self._prepare_migrate()
        record, created = self._apply()
        self.assertTrue(created)
        self.assertEqual(list(record.keys()),
                         ["job_id", "advice_key", "at", "source", "target",
                          "supply", "signal", "dispatch", "execution",
                          "reserved"])
        self.assertEqual(record["job_id"], "j-1")
        self.assertEqual(record["advice_key"], "a1")
        self.assertEqual(record["at"], 40)
        self.assertEqual(record["source"],
                         {"resource_id": "r-2", "version": 1})
        self.assertEqual(record["target"], advice["target"])
        self.assertEqual(record["target"],
                         {"resource_id": "r-1", "version": 1})
        self.assertEqual(record["supply"]["resource_id"], "r-1")
        self.assertEqual(record["supply"]["version"], 1)
        self.assertEqual(record["signal"]["region"], "eu-north")
        self.assertEqual(record["signal"]["version"], 2)
        self.assertEqual(record["dispatch"], "ready")
        self.assertEqual(record["execution"], "none")
        self.assertEqual(record["reserved"], "reserved")

    def test_apply_at_advice_moment_is_allowed(self) -> None:
        self._prepare_migrate()
        record, created = self._apply(at=30)
        self.assertTrue(created)
        self.assertEqual(record["at"], 30)

    def test_failed_dispatch_and_interrupted_plan_allow_apply(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 20, 10)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "e1",
                              "owner", None, 12)
        execution_module.record(self.execution, "j-1", 1, "e2", "owner",
                                "stage", "failed", "boom", 13)
        dispatch_module.finish(self.dispatch, "j-1", "f1", "owner",
                               "failed", 13)
        self._green_eu_north()
        self._migrate_advice()
        record, created = self._apply()
        self.assertTrue(created)
        self.assertEqual(record["dispatch"], "failed")
        self.assertEqual(record["execution"], "failed")

    # -- advice gate --------------------------------------------------------

    def test_keep_advice_raises_value_error(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        record, _ = self._evaluate()
        self.assertEqual(record["recommendation"], "keep")
        with self.assertRaises(ValueError):
            self._apply()
        self.assertFalse(os.path.exists(self.ledger))

    def test_advice_of_another_job_raises_value_error(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._commit_second_job()
        self._green_eu_north()
        self._migrate_advice(job_id="j-1", key="a1")
        self._migrate_advice(job_id="j-2", key="a2")
        with self.assertRaises(ValueError):
            self._apply(job_id="j-1", advice_key="a2")
        self.assertFalse(os.path.exists(self.ledger))

    def test_moment_regression_raises_value_error(self) -> None:
        self._prepare_migrate()
        with self.assertRaises(ValueError):
            self._apply(at=20)
        self.assertFalse(os.path.exists(self.ledger))

    def test_unknown_advice_key_raises_key_error(self) -> None:
        self._prepare_migrate()
        with self.assertRaises(KeyError):
            self._apply(advice_key="a-nope")
        self.assertFalse(os.path.exists(self.ledger))

    # -- lifecycle guards --------------------------------------------------

    def test_claimed_decision_raises_permission_error(self) -> None:
        self._prepare_migrate()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 60, 30)
        with self.assertRaises(PermissionError):
            self._apply()
        self.assertFalse(os.path.exists(self.ledger))

    def test_active_plan_raises_permission_error(self) -> None:
        self._prepare_migrate()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 60, 30)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "e1",
                              "owner", None, 35)
        with self.assertRaises(PermissionError):
            self._apply()
        self.assertFalse(os.path.exists(self.ledger))

    def test_succeeded_decision_raises_value_error(self) -> None:
        self._prepare_migrate()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 60, 30)
        dispatch_module.finish(self.dispatch, "j-1", "f1", "owner",
                               "succeeded", 35)
        with self.assertRaises(ValueError):
            self._apply()
        self.assertFalse(os.path.exists(self.ledger))

    def test_completed_plan_raises_value_error(self) -> None:
        self._prepare_migrate()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 60, 30)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "e1",
                              "owner", None, 32)
        execution_module.record(self.execution, "j-1", 1, "e2", "owner",
                                "stage", "succeeded", "rc1", 33)
        execution_module.record(self.execution, "j-1", 1, "e3", "owner",
                                "start", "succeeded", "rc2", 34)
        with self.assertRaises(ValueError):
            self._apply()
        self.assertFalse(os.path.exists(self.ledger))

    def test_past_deadline_raises_timeout_error(self) -> None:
        self._prepare_migrate()
        with self.assertRaises(TimeoutError):
            self._apply(at=101)
        self.assertFalse(os.path.exists(self.ledger))

    # -- re-reservation feasibility ----------------------------------------

    def test_target_change_raises_lookup_error(self) -> None:
        self._prepare_migrate()
        # A still cleaner us-west signal makes the retained r-2 the
        # first candidate again, so the advice target is stale.
        signals_module.publish(
            self.signals,
            _signal("us-west", observed=35, expires=400, unit_cost=0,
                    carbon_intensity=0), "s9")
        with self.assertRaises(LookupError):
            self._apply()
        self.assertFalse(os.path.exists(self.ledger))

    def test_no_candidates_raises_lookup_error(self) -> None:
        self._prepare_migrate()
        # Every region's latest signal at the reservation moment prices
        # the job out of its budgets.
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=35, expires=400,
                    unit_cost=1000, carbon_intensity=1), "s8")
        signals_module.publish(
            self.signals,
            _signal("us-west", observed=35, expires=400,
                    unit_cost=1000, carbon_intensity=1), "s9")
        with self.assertRaises(LookupError):
            self._apply()
        self.assertFalse(os.path.exists(self.ledger))

    def test_capacity_filled_by_later_trade_raises_lookup_error(
            self) -> None:
        # r-1 holds exactly the job's 10 units; once another job books
        # them after the advice, the reservation cannot be taken.
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
        self._commit_job()
        self._green_eu_north()
        self._migrate_advice()
        jobs_module.submit(
            self.jobs,
            _job("j-2", regions=["eu-north"], residency=["eu-north"]),
            "jk-2")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-2", "t2", 35)
        with self.assertRaises(LookupError):
            self._apply()
        self.assertFalse(os.path.exists(self.ledger))

    def test_other_job_intent_deducts_target_capacity(self) -> None:
        # Both jobs migrate to r-1 (capacity 10, work 10 each). The
        # first reservation succeeds; the second recomputation sees the
        # first intent's occupancy and refuses.
        resources_module.publish(
            self.supply, _resource("r-1", capacity=10), "k1")
        resources_module.publish(
            self.supply,
            _resource("r-2", region="us-west", capacity=100,
                      residency=["eu-north", "us-west"]), "k2")
        signals_module.publish(
            self.signals,
            _signal("eu-north", unit_cost=1, carbon_intensity=5), "s1")
        signals_module.publish(
            self.signals,
            _signal("us-west", unit_cost=1, carbon_intensity=1), "s2")
        self._commit_job()
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-2", "t2", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-2", "d2", 10)
        self._green_eu_north()
        self._migrate_advice(job_id="j-1", key="a1")
        self._migrate_advice(job_id="j-2", key="a2")
        record, created = self._apply(job_id="j-1", advice_key="a1",
                                      key="r1")
        self.assertTrue(created)
        with self.assertRaises(LookupError):
            self._apply(job_id="j-2", advice_key="a2", key="r2")

    def test_own_source_occupancy_is_never_deducted(self) -> None:
        # The source version holds exactly the job's 10 units; the
        # reservation on the target must not be disturbed by it.
        resources_module.publish(
            self.supply, _resource("r-1", capacity=100), "k1")
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
        self._commit_job()
        self._green_eu_north()
        self._migrate_advice()
        record, created = self._apply()
        self.assertTrue(created)
        self.assertEqual(record["source"],
                         {"resource_id": "r-2", "version": 1})

    # -- idempotency --------------------------------------------------------

    def test_replay_returns_stored_record_without_write(self) -> None:
        self._prepare_migrate()
        first, created_first = self._apply()
        self.assertTrue(created_first)
        before = Path(self.ledger).read_bytes()
        second, created_second = self._apply()
        self.assertFalse(created_second)
        self.assertEqual(second, first)
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_same_key_changed_request_raises(self) -> None:
        self._prepare_migrate()
        self._apply()
        before = Path(self.ledger).read_bytes()
        with self.assertRaises(ValueError):
            self._apply(at=41)
        with self.assertRaises(ValueError):
            self._apply(advice_key="a-other")
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_same_job_under_another_key_raises(self) -> None:
        self._prepare_migrate()
        self._migrate_advice(job_id="j-1", key="a-second", at=35)
        self._apply(advice_key="a1", key="r1")
        before = Path(self.ledger).read_bytes()
        with self.assertRaises(ValueError):
            self._apply(advice_key="a-second", key="r2", at=45)
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_replay_still_returns_record_after_booking_finishes(
            self) -> None:
        self._prepare_migrate()
        first, _ = self._apply()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 50, 41)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "e1",
                              "owner", None, 45)
        execution_module.record(self.execution, "j-1", 1, "e2", "owner",
                                "stage", "succeeded", "rc1", 46)
        execution_module.record(self.execution, "j-1", 1, "e3", "owner",
                                "start", "succeeded", "rc2", 47)
        dispatch_module.finish(self.dispatch, "j-1", "f1", "owner",
                               "succeeded", 48)
        # A fresh reservation is now refused, but the key still replays
        # its frozen record without writing.
        with self.assertRaises(ValueError):
            self._apply(key="r2", at=50)
        before = Path(self.ledger).read_bytes()
        replayed, created = self._apply()
        self.assertFalse(created)
        self.assertEqual(replayed, first)
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    # -- errors that must not create the ledger ----------------------------

    def test_unknown_job_raises_key_error(self) -> None:
        self._prepare_migrate()
        with self.assertRaises(KeyError):
            self._apply(job_id="j-nope")
        self.assertFalse(os.path.exists(self.ledger))

    def test_trade_without_dispatch_raises_key_error(self) -> None:
        self._seed_two_regions()
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-1", "t1", 10)
        self._commit_second_job()
        self._green_eu_north()
        # j-2's advice creates the advice ledger; j-1's missing
        # dispatch decision is the error surfaced.
        self._migrate_advice(job_id="j-2", key="a2")
        with self.assertRaises(KeyError):
            self._apply()
        self.assertFalse(os.path.exists(self.ledger))

    def test_accepted_job_without_trade_raises_lookup_error(self) -> None:
        self._seed_two_regions()
        self._commit_second_job()
        self._green_eu_north()
        # j-2 creates every ledger, while j-1 is merely accepted, so
        # the ledgers exist but j-1 has no trade.
        self._migrate_advice(job_id="j-2", key="a2")
        with self.assertRaises(LookupError):
            self._apply()
        self.assertFalse(os.path.exists(self.ledger))

    def test_missing_inputs_raise_file_not_found(self) -> None:
        self._prepare_migrate()
        missing = [os.path.join(self.tmp.name, name) for name in (
            "no-jobs.json", "no-supply.json", "no-signals.json",
            "no-trades.json", "no-dispatch.json", "no-execution.json",
            "no-advice.json")]
        replacements = [
            (missing[0], self.supply, self.signals, self.trades,
             self.dispatch, self.execution, self.advice),
            (self.jobs, missing[1], self.signals, self.trades,
             self.dispatch, self.execution, self.advice),
            (self.jobs, self.supply, missing[2], self.trades,
             self.dispatch, self.execution, self.advice),
            (self.jobs, self.supply, self.signals, missing[3],
             self.dispatch, self.execution, self.advice),
            (self.jobs, self.supply, self.signals, self.trades,
             missing[4], self.execution, self.advice),
            (self.jobs, self.supply, self.signals, self.trades,
             self.dispatch, missing[5], self.advice),
            (self.jobs, self.supply, self.signals, self.trades,
             self.dispatch, self.execution, missing[6]),
        ]
        for replaced in replacements:
            with self.subTest(replaced=replaced):
                with self.assertRaises(FileNotFoundError):
                    apply(*replaced, self.ledger, "j-1", "a1", "r1", 40)

    def test_missing_ledger_parent_raises_file_not_found(self) -> None:
        self._prepare_migrate()
        missing = os.path.join(self.tmp.name, "no-such-dir",
                               "intents.json")
        with self.assertRaises(FileNotFoundError):
            apply(self.jobs, self.supply, self.signals, self.trades,
                  self.dispatch, self.execution, self.advice, missing,
                  "j-1", "a1", "r1", 40)
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    # -- argument validation ------------------------------------------------

    def test_invalid_arguments(self) -> None:
        base_args = [self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.advice,
                     self.ledger, "j-1", "a1", "r1", 40]
        for index in range(11):
            broken = list(base_args)
            broken[index] = ""
            with self.subTest(index=index):
                with self.assertRaises(ValueError):
                    apply(*broken)
        with self.assertRaises(ValueError):
            apply(*base_args[:11], -1)
        with self.assertRaises(ValueError):
            apply(*base_args[:11], True)  # type: ignore[arg-type]

    def test_paths_must_be_distinct(self) -> None:
        args = [self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, self.ledger,
                "j-1", "a1", "r1", 40]
        for index in range(8):
            broken = list(args)
            # Collide this position with the next path so two positions
            # resolve to the same real location while all others differ.
            broken[index] = args[(index + 1) % 8]
            with self.assertRaises(ValueError):
                apply(*broken)

    # -- ledger form --------------------------------------------------------

    def test_ledger_is_canonical_with_sorted_sections(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._commit_second_job()
        self._green_eu_north()
        self._migrate_advice(job_id="j-1", key="a1")
        self._migrate_advice(job_id="j-2", key="a2")
        # Keys chosen to sort independently of insertion order,
        # including a non-ASCII idempotency key written through.
        self._apply(job_id="j-2", advice_key="a2", key="z9")
        self._apply(job_id="j-1", advice_key="a1", key="é-mid")

        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        # Non-ASCII key written through, never \u-escaped.
        self.assertIn("é-mid".encode("utf-8"), raw)
        self.assertNotIn(b"\\u", raw)
        data = json.loads(raw.decode("utf-8"))
        self.assertEqual(list(data.keys()),
                         ["version", "intents", "idempotency", "audit"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["intents"]), sorted(data["intents"]))
        self.assertEqual(list(data["intents"]), ["j-1", "j-2"])
        self.assertEqual(list(data["idempotency"]),
                         sorted(data["idempotency"]))
        self.assertEqual(list(data["audit"]), sorted(data["audit"]))
        self.assertEqual(set(data["idempotency"]), set(data["audit"]))
        self.assertEqual(
            {entry["job_id"] for entry in data["idempotency"].values()},
            set(data["intents"]))
        event = data["audit"]["é-mid"]
        self.assertEqual(event["request"],
                         {"job_id": "j-1", "advice_key": "a1", "at": 40})
        self.assertEqual(event["result"], data["intents"]["j-1"])

    def test_tampered_ledger_raises_value_error(self) -> None:
        self._prepare_migrate()
        self._apply()
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        data["intents"]["j-1"]["reserved"] = "released"
        Path(self.ledger).write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"))
            + "\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            self._apply(job_id="j-1", advice_key="a1", key="r2", at=45)

    def test_no_temp_fragments_left_after_guard_failures(self) -> None:
        self._prepare_migrate()
        with self.assertRaises(TimeoutError):
            self._apply(at=200)
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])


if __name__ == "__main__":
    unittest.main()
