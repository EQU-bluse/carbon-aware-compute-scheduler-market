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
from carbon_market.rebalance import apply, evaluate, record, recover, start


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


class RebalanceMigrateTest(unittest.TestCase):
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

    def _args(self) -> tuple[str, ...]:
        return (self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, self.ledger)

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

    def _prepare_reserved(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._green_eu_north()
        record, created = evaluate(self.jobs, self.supply, self.signals,
                                   self.trades, self.dispatch,
                                   self.execution, self.advice, "j-1",
                                   "a1", 30)
        self.assertTrue(created)
        self.assertEqual(record["recommendation"], "migrate")
        intent, created = apply(self.jobs, self.supply, self.signals,
                                self.trades, self.dispatch, self.execution,
                                self.advice, self.ledger, "j-1", "a1",
                                "r1", 40)
        self.assertTrue(created)
        self.assertEqual(intent["reserved"], "reserved")

    def _start(self, job_id: str = "j-1", key: str = "m1",
               owner: str = "owner-1", lease_end: int = 90, at: int = 45):
        return start(*self._args(), job_id, key, owner, lease_end, at)

    def _record(self, job_id: str = "j-1", key: str = "m2",
                owner: str = "owner-1", step: str = "copy",
                result: str = "succeeded", receipt: str = "rc1",
                at: int = 50):
        return record(*self._args(), job_id, key, owner, step, result,
                      receipt, at)

    def _recover(self, job_id: str = "j-1", key: str = "m3",
                 owner: str = "owner-1", at: int = 91):
        return recover(*self._args(), job_id, key, owner, at)

    def _migrate(self) -> dict[str, object]:
        self._start()
        plan, created = self._record(key="m2", step="copy", receipt="rc1",
                                     at=50)
        self.assertTrue(created)
        plan, created = self._record(key="m4", step="switch",
                                     receipt="rc2", at=52)
        self.assertTrue(created)
        self.assertEqual(plan["state"], "migrated")
        return plan

    # -- start --------------------------------------------------------------

    def test_start_freezes_full_plan(self) -> None:
        self._prepare_reserved()
        plan, created = self._start()
        self.assertTrue(created)
        self.assertEqual(list(plan.keys()),
                         ["job_id", "source", "target", "owner",
                          "lease_end", "at", "state", "steps"])
        self.assertEqual(plan["job_id"], "j-1")
        self.assertEqual(plan["source"],
                         {"resource_id": "r-2", "version": 1})
        self.assertEqual(plan["target"],
                         {"resource_id": "r-1", "version": 1})
        self.assertEqual(plan["owner"], "owner-1")
        self.assertEqual(plan["lease_end"], 90)
        self.assertEqual(plan["at"], 45)
        self.assertEqual(plan["state"], "active")
        self.assertEqual(plan["steps"], [])

    def test_start_upgrades_ledger_keeping_intents(self) -> None:
        self._prepare_reserved()
        before = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(before["version"], 1)
        self.assertEqual(list(before.keys()),
                         ["version", "intents", "idempotency", "audit"])
        self._start()
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(data["version"], 2)
        self.assertEqual(list(data.keys()),
                         ["version", "intents", "plans", "idempotency",
                          "audit"])
        self.assertEqual(data["intents"], before["intents"])
        self.assertEqual(list(data["plans"]), ["j-1"])
        self.assertEqual(set(data["idempotency"]), {"r1", "m1"})
        self.assertEqual(set(data["audit"]), {"r1", "m1"})
        event = data["audit"]["m1"]
        self.assertEqual(event["request"],
                         {"action": "start", "job_id": "j-1",
                          "owner": "owner-1", "lease_end": 90, "at": 45})
        self.assertEqual(event["result"], data["plans"]["j-1"])

    def test_start_replay_returns_current_snapshot(self) -> None:
        self._prepare_reserved()
        first, created = self._start()
        self.assertTrue(created)
        self._migrate()
        before = Path(self.ledger).read_bytes()
        replayed, created = self._start()
        self.assertFalse(created)
        self.assertEqual(replayed["state"], "migrated")
        self.assertNotEqual(replayed, first)
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_start_same_key_changed_request_raises(self) -> None:
        self._prepare_reserved()
        self._start()
        before = Path(self.ledger).read_bytes()
        with self.assertRaises(ValueError):
            self._start(lease_end=80)
        with self.assertRaises(ValueError):
            self._start(owner="owner-x")
        with self.assertRaises(ValueError):
            self._start(at=46)
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_start_active_plan_raises_permission_error(self) -> None:
        self._prepare_reserved()
        self._start()
        before = Path(self.ledger).read_bytes()
        with self.assertRaises(PermissionError):
            self._start(key="m2", at=46)
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_start_terminal_plan_raises_value_error(self) -> None:
        self._prepare_reserved()
        self._migrate()
        before = Path(self.ledger).read_bytes()
        with self.assertRaises(ValueError):
            self._start(key="m5", at=60)
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_start_lease_past_deadline_raises_value_error(self) -> None:
        self._prepare_reserved()
        with self.assertRaises(ValueError):
            self._start(lease_end=101)
        with self.assertRaises(ValueError):
            self._start(lease_end=0)
        self.assertNotIn("plans", json.loads(
            Path(self.ledger).read_text(encoding="utf-8")))

    def test_start_past_deadline_raises_timeout_error(self) -> None:
        self._prepare_reserved()
        with self.assertRaises(TimeoutError):
            self._start(lease_end=100, at=101)
        with self.assertRaises(TimeoutError):
            self._start(lease_end=40, at=46)

    def test_start_claimed_decision_raises_permission_error(self) -> None:
        self._prepare_reserved()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 40, 45)
        with self.assertRaises(PermissionError):
            self._start(at=46)

    def test_start_active_execution_plan_raises_permission_error(
            self) -> None:
        self._prepare_reserved()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 40, 45)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "e1",
                              "owner", None, 46)
        with self.assertRaises(PermissionError):
            self._start(at=47)

    def test_start_succeeded_decision_raises_value_error(self) -> None:
        self._prepare_reserved()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 40, 45)
        dispatch_module.finish(self.dispatch, "j-1", "f1", "owner",
                               "succeeded", 46)
        with self.assertRaises(ValueError):
            self._start(at=47)

    def test_start_completed_plan_raises_value_error(self) -> None:
        self._prepare_reserved()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 40, 45)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "e1",
                              "owner", None, 46)
        execution_module.record(self.execution, "j-1", 1, "e2", "owner",
                                "stage", "succeeded", "rc1", 47)
        execution_module.record(self.execution, "j-1", 1, "e3", "owner",
                                "start", "succeeded", "rc2", 48)
        with self.assertRaises(ValueError):
            self._start(at=49)

    def test_start_unknown_job_and_intent_raise_key_error(self) -> None:
        self._prepare_reserved()
        with self.assertRaises(KeyError):
            self._start(job_id="j-nope")
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        with self.assertRaises(KeyError):
            self._start(job_id="j-2")

    # -- record -------------------------------------------------------------

    def test_record_copy_then_switch_migrates(self) -> None:
        self._prepare_reserved()
        self._start()
        plan, created = self._record()
        self.assertTrue(created)
        self.assertEqual(plan["state"], "active")
        self.assertEqual(plan["steps"],
                         [{"step": "copy", "result": "succeeded",
                           "receipt": "rc1", "at": 50}])
        plan, created = self._record(key="m4", step="switch",
                                     receipt="rc2", at=52)
        self.assertTrue(created)
        self.assertEqual(plan["state"], "migrated")
        self.assertEqual(len(plan["steps"]), 2)

    def test_record_failed_step_fails_plan(self) -> None:
        self._prepare_reserved()
        self._start()
        plan, created = self._record(result="failed", receipt="boom")
        self.assertTrue(created)
        self.assertEqual(plan["state"], "failed")
        with self.assertRaises(ValueError):
            self._record(key="m4", step="switch", at=51)

    def test_record_failed_switch_fails_plan(self) -> None:
        self._prepare_reserved()
        self._start()
        self._record()
        plan, created = self._record(key="m4", step="switch",
                                     result="failed", receipt="boom",
                                     at=51)
        self.assertTrue(created)
        self.assertEqual(plan["state"], "failed")

    def test_record_step_skip_or_repeat_raises_value_error(self) -> None:
        self._prepare_reserved()
        self._start()
        with self.assertRaises(ValueError):
            self._record(step="switch")
        self._record()
        with self.assertRaises(ValueError):
            self._record(key="m4", step="copy", at=51)

    def test_record_terminal_plan_raises_value_error(self) -> None:
        self._prepare_reserved()
        self._migrate()
        with self.assertRaises(ValueError):
            self._record(key="m5", step="copy", at=60)

    def test_record_wrong_owner_raises_permission_error(self) -> None:
        self._prepare_reserved()
        self._start()
        with self.assertRaises(PermissionError):
            self._record(owner="owner-x")

    def test_record_outside_lease_raises_timeout_error(self) -> None:
        self._prepare_reserved()
        self._start()
        with self.assertRaises(TimeoutError):
            self._record(at=91)
        # The lease boundary itself is still inside the lease.
        plan, created = self._record(at=90)
        self.assertTrue(created)

    def test_record_without_plan_raises_value_error(self) -> None:
        self._prepare_reserved()
        with self.assertRaises(ValueError):
            self._record()

    def test_record_replay_returns_current_snapshot(self) -> None:
        self._prepare_reserved()
        self._migrate()
        before = Path(self.ledger).read_bytes()
        replayed, created = self._record()
        self.assertFalse(created)
        self.assertEqual(replayed["state"], "migrated")
        self.assertEqual(Path(self.ledger).read_bytes(), before)
        with self.assertRaises(ValueError):
            self._record(receipt="other")

    def test_record_invalid_arguments(self) -> None:
        self._prepare_reserved()
        self._start()
        with self.assertRaises(ValueError):
            self._record(step="stage")
        with self.assertRaises(ValueError):
            self._record(result="done")
        with self.assertRaises(ValueError):
            self._record(receipt="")
        with self.assertRaises(ValueError):
            self._record(at=-1)

    # -- recover --------------------------------------------------------------

    def test_recover_interrupts_expired_plan(self) -> None:
        self._prepare_reserved()
        self._start()
        self._record()
        plan, created = self._recover()
        self.assertTrue(created)
        self.assertEqual(plan["state"], "interrupted")
        self.assertEqual(len(plan["steps"]), 1)

    def test_recover_unexpired_lease_raises_permission_error(self) -> None:
        self._prepare_reserved()
        self._start()
        with self.assertRaises(PermissionError):
            self._recover(at=90)
        with self.assertRaises(PermissionError):
            self._recover(at=45)

    def test_recover_wrong_owner_raises_permission_error(self) -> None:
        self._prepare_reserved()
        self._start()
        with self.assertRaises(PermissionError):
            self._recover(owner="owner-x")

    def test_recover_terminal_plan_raises_value_error(self) -> None:
        self._prepare_reserved()
        self._migrate()
        with self.assertRaises(ValueError):
            self._recover(at=95)

    def test_recover_without_plan_raises_value_error(self) -> None:
        self._prepare_reserved()
        with self.assertRaises(ValueError):
            self._recover()

    def test_recover_replay_returns_current_snapshot(self) -> None:
        self._prepare_reserved()
        self._start()
        first, created = self._recover()
        self.assertTrue(created)
        before = Path(self.ledger).read_bytes()
        replayed, created = self._recover()
        self.assertFalse(created)
        self.assertEqual(replayed, first)
        self.assertEqual(Path(self.ledger).read_bytes(), before)
        with self.assertRaises(ValueError):
            self._recover(at=92)

    # -- shared idempotency key space ----------------------------------------

    def test_lifecycle_keys_clash_with_apply_key(self) -> None:
        self._prepare_reserved()
        with self.assertRaises(ValueError):
            self._start(key="r1")
        with self.assertRaises(ValueError):
            self._record(key="r1")
        with self.assertRaises(ValueError):
            self._recover(key="r1")

    # -- capacity accounting ---------------------------------------------------

    def _prepare_capacity_race(self) -> None:
        # r-1 (target) and r-2 (source) both hold exactly j-1's and
        # j-2's work; j-2's advice targets r-1 as well.
        resources_module.publish(
            self.supply, _resource("r-1", capacity=20), "k1")
        resources_module.publish(
            self.supply,
            _resource("r-2", region="us-west", capacity=20,
                      residency=["eu-north", "us-west"]), "k2")
        signals_module.publish(
            self.signals,
            _signal("eu-north", unit_cost=5, carbon_intensity=8), "s1")
        signals_module.publish(
            self.signals,
            _signal("us-west", unit_cost=3, carbon_intensity=2), "s2")
        self._commit_job()
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-2", "t2", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-2", "d2", 10)
        self._green_eu_north()
        for job_id, advice_key in (("j-1", "a1"), ("j-2", "a2")):
            record, created = evaluate(self.jobs, self.supply,
                                       self.signals, self.trades,
                                       self.dispatch, self.execution,
                                       self.advice, job_id, advice_key,
                                       30)
            self.assertTrue(created)
            self.assertEqual(record["recommendation"], "migrate")
        intent, created = apply(self.jobs, self.supply, self.signals,
                                self.trades, self.dispatch, self.execution,
                                self.advice, self.ledger, "j-1", "a1",
                                "r1", 40)
        self.assertTrue(created)

    def _apply_j2(self):
        return apply(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.advice,
                     self.ledger, "j-2", "a2", "r2", 45)

    def test_active_intent_holds_target_capacity(self) -> None:
        self._prepare_capacity_race()
        self._start()
        # r-1 still holds exactly one free slot; j-2 takes it.
        _, created = self._apply_j2()
        self.assertTrue(created)

    def test_failed_intent_releases_target_capacity(self) -> None:
        resources_module.publish(
            self.supply, _resource("r-1", capacity=10), "k1")
        resources_module.publish(
            self.supply,
            _resource("r-2", region="us-west", capacity=100,
                      residency=["eu-north", "us-west"]), "k2")
        signals_module.publish(
            self.signals,
            _signal("eu-north", unit_cost=5, carbon_intensity=8), "s1")
        signals_module.publish(
            self.signals,
            _signal("us-west", unit_cost=3, carbon_intensity=2), "s2")
        self._commit_job()
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-2", "t2", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-2", "d2", 10)
        self._green_eu_north()
        for job_id, advice_key in (("j-1", "a1"), ("j-2", "a2")):
            evaluate(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.advice, job_id,
                     advice_key, 30)
        apply(self.jobs, self.supply, self.signals, self.trades,
              self.dispatch, self.execution, self.advice, self.ledger,
              "j-1", "a1", "r1", 40)
        # j-1's reservation fills r-1 (capacity 10): j-2 cannot apply.
        with self.assertRaises(LookupError):
            self._apply_j2()
        self._start()
        self._record(result="failed", receipt="boom")
        # The failed plan released the r-1 reservation: j-2 applies.
        _, created = self._apply_j2()
        self.assertTrue(created)

    def test_interrupted_intent_releases_target_capacity(self) -> None:
        resources_module.publish(
            self.supply, _resource("r-1", capacity=10), "k1")
        resources_module.publish(
            self.supply,
            _resource("r-2", region="us-west", capacity=100,
                      residency=["eu-north", "us-west"]), "k2")
        signals_module.publish(
            self.signals,
            _signal("eu-north", unit_cost=5, carbon_intensity=8), "s1")
        signals_module.publish(
            self.signals,
            _signal("us-west", unit_cost=3, carbon_intensity=2), "s2")
        self._commit_job()
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-2", "t2", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-2", "d2", 10)
        self._green_eu_north()
        for job_id, advice_key in (("j-1", "a1"), ("j-2", "a2")):
            evaluate(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.advice, job_id,
                     advice_key, 30)
        apply(self.jobs, self.supply, self.signals, self.trades,
              self.dispatch, self.execution, self.advice, self.ledger,
              "j-1", "a1", "r1", 40)
        with self.assertRaises(LookupError):
            self._apply_j2()
        self._start()
        self._recover()
        _, created = self._apply_j2()
        self.assertTrue(created)

    # -- ledger form and robustness -------------------------------------------

    def test_ledger_is_canonical_with_sorted_sections(self) -> None:
        self._prepare_reserved()
        self._migrate()
        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        self.assertNotIn(b"\\u", raw)
        data = json.loads(raw.decode("utf-8"))
        self.assertEqual(list(data.keys()),
                         ["version", "intents", "plans", "idempotency",
                          "audit"])
        self.assertEqual(list(data["intents"]), sorted(data["intents"]))
        self.assertEqual(list(data["plans"]), sorted(data["plans"]))
        self.assertEqual(list(data["idempotency"]),
                         sorted(data["idempotency"]))
        self.assertEqual(list(data["audit"]), sorted(data["audit"]))
        self.assertEqual(set(data["idempotency"]), set(data["audit"]))

    def test_tampered_plan_raises_value_error(self) -> None:
        self._prepare_reserved()
        self._migrate()
        good = Path(self.ledger).read_bytes()
        data = json.loads(good.decode("utf-8"))
        data["plans"]["j-1"]["state"] = "active"
        Path(self.ledger).write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"))
            + "\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            self._recover(at=95)
        Path(self.ledger).write_bytes(good)
        data = json.loads(good.decode("utf-8"))
        data["plans"]["j-1"]["steps"][0]["receipt"] = "forged"
        Path(self.ledger).write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"))
            + "\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            self._recover(at=95)

    def test_missing_inputs_raise_file_not_found(self) -> None:
        self._prepare_reserved()
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
                    start(*replaced, self.ledger, "j-1", "m1", "o", 90, 45)
                with self.assertRaises(FileNotFoundError):
                    record(*replaced, self.ledger, "j-1", "m1", "o",
                           "copy", "succeeded", "rc", 45)
                with self.assertRaises(FileNotFoundError):
                    recover(*replaced, self.ledger, "j-1", "m1", "o", 45)

    def test_paths_must_be_distinct(self) -> None:
        self._prepare_reserved()
        args = [self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, self.ledger]
        for index in range(8):
            broken = list(args)
            broken[index] = args[(index + 1) % 8]
            with self.subTest(index=index):
                with self.assertRaises(ValueError):
                    start(*broken, "j-1", "m1", "o", 90, 45)
                with self.assertRaises(ValueError):
                    record(*broken, "j-1", "m1", "o", "copy",
                           "succeeded", "rc", 45)
                with self.assertRaises(ValueError):
                    recover(*broken, "j-1", "m1", "o", 45)

    def test_invalid_arguments(self) -> None:
        self._prepare_reserved()
        base_args = [self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.advice,
                     self.ledger, "j-1", "m1", "owner-1"]
        for index in range(11):
            broken = list(base_args)
            broken[index] = ""
            with self.subTest(index=index):
                with self.assertRaises(ValueError):
                    start(*broken, 90, 45)
                with self.assertRaises(ValueError):
                    recover(*broken, 45)
        with self.assertRaises(ValueError):
            start(*base_args, True, 45)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            start(*base_args, 90, -1)
        with self.assertRaises(ValueError):
            recover(*base_args, True)  # type: ignore[arg-type]

    def test_no_temp_fragments_left_after_guard_failures(self) -> None:
        self._prepare_reserved()
        with self.assertRaises(TimeoutError):
            self._start(lease_end=100, at=200)
        self._start()
        with self.assertRaises(PermissionError):
            self._recover(at=50)
        with self.assertRaises(ValueError):
            self._record(step="switch")
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])


if __name__ == "__main__":
    unittest.main()
