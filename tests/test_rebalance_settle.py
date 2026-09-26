from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from carbon_market import dispatch as dispatch_module
from carbon_market import execution as execution_module
from carbon_market import jobs as jobs_module
from carbon_market import rebalance as rebalance_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.market import clear_live
from carbon_market.rebalance import (apply, current, evaluate, record,
                                     recover, settle, start)


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


class RebalanceSettleTest(unittest.TestCase):
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
        self.settlements = os.path.join(base, "settlements.json")
        jobs_module.submit(self.jobs, _job(), "jk-1")

    def _args(self) -> tuple[str, ...]:
        return (self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, self.ledger)

    def _all(self) -> tuple[str, ...]:
        return self._args() + (self.settlements,)

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

    def _ensure_execution_ledger(self) -> None:
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

    def _commit_job(self) -> None:
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-1", "t1", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-1", "d1", 10)
        self._ensure_execution_ledger()

    def _green_eu_north(self) -> None:
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=15, expires=300, unit_cost=1,
                    carbon_intensity=1), "s3")

    def _prepare_reserved(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._green_eu_north()
        evaluate(self.jobs, self.supply, self.signals, self.trades,
                 self.dispatch, self.execution, self.advice, "j-1",
                 "a1", 30)
        apply(self.jobs, self.supply, self.signals, self.trades,
              self.dispatch, self.execution, self.advice, self.ledger,
              "j-1", "a1", "r1", 40)

    def _start(self) -> None:
        start(*self._args(), "j-1", "m1", "owner-1", 90, 45)

    def _finish_migrated(self) -> None:
        self._start()
        record(*self._args(), "j-1", "m2", "owner-1", "copy",
               "succeeded", "rc1", 50)
        record(*self._args(), "j-1", "m4", "owner-1", "switch",
               "succeeded", "rc2", 52)

    def _finish_failed(self) -> None:
        self._start()
        record(*self._args(), "j-1", "m2", "owner-1", "copy", "failed",
               "boom", 50)

    def _finish_interrupted(self) -> None:
        self._start()
        record(*self._args(), "j-1", "m2", "owner-1", "copy",
               "succeeded", "rc1", 50)
        recover(*self._args(), "j-1", "m3", "owner-1", 91)

    def _settle(self, plan_key: str = "m1", key: str = "s1",
                at: int = 55):
        return settle(*self._all(), "j-1", plan_key, key, at)

    # -- migrated ------------------------------------------------------------

    def test_migrated_settle_confirms_target(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        result, created = self._settle()
        self.assertTrue(created)
        self.assertEqual(list(result.keys()),
                         ["job_id", "plan_key", "generation", "migration",
                          "before", "after", "at", "state", "audit"])
        self.assertEqual(result["job_id"], "j-1")
        self.assertEqual(result["plan_key"], "m1")
        self.assertEqual(result["generation"], 1)
        self.assertEqual(result["migration"], "migrated")
        self.assertEqual(result["before"],
                         {"resource_id": "r-2", "version": 1})
        self.assertEqual(result["after"],
                         {"resource_id": "r-1", "version": 1})
        self.assertEqual(result["at"], 55)
        self.assertEqual(result["state"], "active")
        self.assertEqual(result["audit"], ["m1", "m2", "m4", "r1"])

    def test_settle_writes_only_independent_ledger(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        snapshots = {path: Path(path).read_bytes()
                     for path in self._args()}
        self._settle()
        for path, raw in snapshots.items():
            self.assertEqual(Path(path).read_bytes(), raw,
                             f"{path} must not be rewritten")
        self.assertTrue(Path(self.settlements).exists())

    def test_settle_creates_canonical_ledger(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        self._settle()
        raw = Path(self.settlements).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        self.assertNotIn(b"\\u", raw)
        data = json.loads(raw.decode("utf-8"))
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data.keys()),
                         ["version", "records", "idempotency", "audit"])
        for section in ("records", "idempotency", "audit"):
            self.assertEqual(list(data[section]), sorted(data[section]))
        self.assertEqual(
            data["audit"]["s1"]["result"], data["records"]["s1"])
        self.assertEqual(
            data["audit"]["s1"]["request"],
            {"job_id": "j-1", "plan_key": "m1", "at": 55})

    # -- failed / interrupted compensation ----------------------------------

    def test_failed_settle_compensates_on_source(self) -> None:
        self._prepare_reserved()
        self._finish_failed()
        result, created = self._settle()
        self.assertTrue(created)
        self.assertEqual(result["migration"], "failed")
        self.assertEqual(result["state"], "compensated")
        self.assertEqual(result["before"], result["after"])
        self.assertEqual(result["after"],
                         {"resource_id": "r-2", "version": 1})

    def test_interrupted_settle_compensates_on_source(self) -> None:
        self._prepare_reserved()
        self._finish_interrupted()
        result, created = self._settle(at=92)
        self.assertTrue(created)
        self.assertEqual(result["migration"], "interrupted")
        self.assertEqual(result["state"], "compensated")
        self.assertEqual(result["after"],
                         {"resource_id": "r-2", "version": 1})

    def test_late_recovery_past_deadline_still_settles(self) -> None:
        self._prepare_reserved()
        self._finish_interrupted()
        result, created = self._settle(at=150)
        self.assertTrue(created)
        self.assertEqual(result["state"], "compensated")

    def test_interrupted_moment_before_recovery_raises(self) -> None:
        self._prepare_reserved()
        self._finish_interrupted()
        with self.assertRaises(ValueError):
            self._settle(at=90)
        result, _ = self._settle(at=91)
        self.assertEqual(result["state"], "compensated")

    def test_migrated_moment_before_last_receipt_raises(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        with self.assertRaises(ValueError):
            self._settle(at=51)
        result, _ = self._settle(at=52)
        self.assertEqual(result["state"], "active")

    # -- refusal rules -------------------------------------------------------

    def test_active_plan_raises_permission_error(self) -> None:
        self._prepare_reserved()
        self._start()
        with self.assertRaises(PermissionError):
            self._settle()
        self.assertFalse(Path(self.settlements).exists())

    def test_reserved_intent_without_plan_raises_key_error(self) -> None:
        self._prepare_reserved()
        with self.assertRaises(KeyError):
            self._settle()

    def test_unknown_job_raises_key_error(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        with self.assertRaises(KeyError):
            settle(*self._all(), "j-nope", "m1", "s1", 55)

    def test_unknown_plan_key_raises_key_error(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        with self.assertRaises(KeyError):
            self._settle(plan_key="m-nope")

    def test_another_key_settling_same_plan_raises(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        self._settle()
        with self.assertRaises(ValueError):
            self._settle(key="s2", at=56)

    def test_same_key_changed_request_raises(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        self._settle()
        before = Path(self.settlements).read_bytes()
        with self.assertRaises(ValueError):
            self._settle(at=56)
        with self.assertRaises(ValueError):
            self._settle(plan_key="m2")
        self.assertEqual(Path(self.settlements).read_bytes(), before)

    # -- idempotency ---------------------------------------------------------

    def test_replay_completed_returns_record_no_write(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        first, created_first = self._settle()
        self.assertTrue(created_first)
        before = Path(self.settlements).read_bytes()
        second, created_second = self._settle()
        self.assertFalse(created_second)
        self.assertEqual(second, first)
        self.assertEqual(Path(self.settlements).read_bytes(), before)

    # -- current -------------------------------------------------------------

    def test_current_before_settlement_is_trade(self) -> None:
        self._prepare_reserved()
        binding = current(*self._all(), "j-1")
        self.assertEqual(binding, {
            "job_id": "j-1", "resource_id": "r-2", "version": 1,
            "generation": 0, "state": "traded", "at": 10})

    def test_current_without_ledger_file_is_trade(self) -> None:
        self._prepare_reserved()
        self.assertFalse(Path(self.settlements).exists())
        binding = current(*self._all(), "j-1")
        self.assertEqual(binding["state"], "traded")

    def test_current_after_migrated_is_target(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        self._settle()
        binding = current(*self._all(), "j-1")
        self.assertEqual(binding["resource_id"], "r-1")
        self.assertEqual(binding["version"], 1)
        self.assertEqual(binding["generation"], 1)
        self.assertEqual(binding["state"], "active")
        self.assertEqual(binding["at"], 55)

    def test_current_after_compensation_is_source(self) -> None:
        self._prepare_reserved()
        self._finish_failed()
        self._settle()
        binding = current(*self._all(), "j-1")
        self.assertEqual(binding["resource_id"], "r-2")
        self.assertEqual(binding["state"], "compensated")

    def test_current_unknown_job_raises_key_error(self) -> None:
        self._prepare_reserved()
        with self.assertRaises(KeyError):
            current(*self._all(), "j-nope")

    def test_current_is_read_only(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        input_before = {path: Path(path).read_bytes()
                        for path in self._args()}
        current(*self._all(), "j-1")
        self.assertFalse(Path(self.settlements).exists())
        for path, raw in input_before.items():
            self.assertEqual(Path(path).read_bytes(), raw)

    # -- record receipt timing ------------------------------------------------

    def test_record_before_plan_start_raises_value_error(self) -> None:
        self._prepare_reserved()
        self._start()
        with self.assertRaises(ValueError):
            record(*self._args(), "j-1", "m2", "owner-1", "copy",
                   "succeeded", "rc1", 44)

    def test_record_before_previous_receipt_raises_value_error(self) -> None:
        self._prepare_reserved()
        self._start()
        record(*self._args(), "j-1", "m2", "owner-1", "copy",
               "succeeded", "rc1", 50)
        with self.assertRaises(ValueError):
            record(*self._args(), "j-1", "m4", "owner-1", "switch",
                   "succeeded", "rc2", 49)

    # -- staged crash recovery ------------------------------------------------

    def test_pending_settle_resumes_after_final_write_failure(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        original = rebalance_module._commit_file

        def fail_final(realpath, payload, old_bytes,
                       prefix=".rebalance-"):
            if prefix == ".rebalance-settle-final-":
                raise OSError("injected final failure")
            return original(realpath, payload, old_bytes, prefix=prefix)

        with mock.patch.object(rebalance_module, "_commit_file",
                               fail_final):
            with self.assertRaises(OSError):
                self._settle()
        data = json.loads(
            Path(self.settlements).read_text(encoding="utf-8"))
        self.assertEqual(data["records"]["s1"]["state"], "pending")

        result, created = self._settle()
        self.assertTrue(created)
        self.assertEqual(result["state"], "active")
        data = json.loads(
            Path(self.settlements).read_text(encoding="utf-8"))
        self.assertEqual(data["records"]["s1"]["state"], "active")

        # The resumed settlement is now complete: another call replays.
        _again, again_created = self._settle()
        self.assertFalse(again_created)

    def test_resume_with_changed_request_raises(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        original = rebalance_module._commit_file

        def fail_final(realpath, payload, old_bytes,
                       prefix=".rebalance-"):
            if prefix == ".rebalance-settle-final-":
                raise OSError("injected final failure")
            return original(realpath, payload, old_bytes, prefix=prefix)

        with mock.patch.object(rebalance_module, "_commit_file",
                               fail_final):
            with self.assertRaises(OSError):
                self._settle()
        with self.assertRaises(ValueError):
            self._settle(at=56)

    # -- ledger robustness ----------------------------------------------------

    def test_tampered_settlement_raises_value_error(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        self._settle()
        good = Path(self.settlements).read_bytes()
        data = json.loads(good.decode("utf-8"))
        data["records"]["s1"]["state"] = "compensated"
        Path(self.settlements).write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"))
            + "\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            current(*self._all(), "j-1")

    def test_contradictory_snapshot_raises_value_error(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        self._settle()
        good = Path(self.ledger).read_bytes()
        data = json.loads(good.decode("utf-8"))
        data["plans"]["j-1"]["state"] = "failed"
        Path(self.ledger).write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"))
            + "\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            current(*self._all(), "j-1")

    def test_missing_inputs_raise_file_not_found(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        names = ("no-jobs.json", "no-supply.json", "no-signals.json",
                 "no-trades.json", "no-dispatch.json",
                 "no-execution.json", "no-advice.json", "no-intent.json")
        missing = [os.path.join(self.tmp.name, name) for name in names]
        replacements = [
            (missing[0], self.supply, self.signals, self.trades,
             self.dispatch, self.execution, self.advice, self.ledger),
            (self.jobs, missing[1], self.signals, self.trades,
             self.dispatch, self.execution, self.advice, self.ledger),
            (self.jobs, self.supply, missing[2], self.trades,
             self.dispatch, self.execution, self.advice, self.ledger),
            (self.jobs, self.supply, self.signals, missing[3],
             self.dispatch, self.execution, self.advice, self.ledger),
            (self.jobs, self.supply, self.signals, self.trades,
             missing[4], self.execution, self.advice, self.ledger),
            (self.jobs, self.supply, self.signals, self.trades,
             self.dispatch, missing[5], self.advice, self.ledger),
            (self.jobs, self.supply, self.signals, self.trades,
             self.dispatch, self.execution, missing[6], self.ledger),
            (self.jobs, self.supply, self.signals, self.trades,
             self.dispatch, self.execution, self.advice, missing[7]),
        ]
        for replaced in replacements:
            with self.subTest(replaced=replaced):
                with self.assertRaises(FileNotFoundError):
                    settle(*replaced, self.settlements, "j-1", "m1",
                           "s1", 55)
                with self.assertRaises(FileNotFoundError):
                    current(*replaced, self.settlements, "j-1")

    def test_missing_ledger_parent_raises_file_not_found(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        missing = os.path.join(self.tmp.name, "no-dir", "settlements.json")
        with self.assertRaises(FileNotFoundError):
            settle(self.jobs, self.supply, self.signals, self.trades,
                   self.dispatch, self.execution, self.advice, self.ledger,
                   missing, "j-1", "m1", "s1", 55)

    def test_paths_must_be_distinct(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        args = list(self._all())
        for index in range(9):
            broken = list(args)
            broken[index] = args[(index + 1) % 9]
            with self.subTest(index=index):
                with self.assertRaises(ValueError):
                    settle(*broken, "j-1", "m1", "s1", 55)
                with self.assertRaises(ValueError):
                    current(*broken, "j-1")

    def test_invalid_arguments(self) -> None:
        self._prepare_reserved()
        self._finish_migrated()
        base = list(self._all()) + ["j-1", "m1", "s1"]
        for index in range(12):
            broken = list(base)
            broken[index] = ""
            with self.subTest(index=index):
                with self.assertRaises(ValueError):
                    settle(*broken, 55)
        with self.assertRaises(ValueError):
            settle(*self._all(), "j-1", "m1", "s1", True)
        with self.assertRaises(ValueError):
            settle(*self._all(), "j-1", "m1", "s1", -1)
        current_base = list(self._all()) + ["j-1"]
        for index in range(10):
            broken = list(current_base)
            broken[index] = ""
            with self.subTest(current_index=index):
                with self.assertRaises(ValueError):
                    current(*broken)

    def test_no_temp_fragments_after_guard_failures(self) -> None:
        self._prepare_reserved()
        self._start()
        with self.assertRaises(PermissionError):
            self._settle()
        self._finish_migrated()
        with self.assertRaises(ValueError):
            self._settle(at=1)
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])


if __name__ == "__main__":
    unittest.main()
