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


def _resource(resource_id: str, region: str = "eu-north",
              **overrides: object) -> dict[str, object]:
    resource: dict[str, object] = {
        "resource_id": resource_id,
        "region": region,
        "capacity": 1000,
        "start": 0,
        "end": 3000,
        "unit_cost": 5,
        "carbon_intensity": 8,
        "residency": sorted({region, "eu-north"}),
    }
    resource.update(overrides)
    return resource


def _signal(region: str, **overrides: object) -> dict[str, object]:
    signal: dict[str, object] = {
        "region": region,
        "observed": 0,
        "expires": 3000,
        "mix": {"solar": 10000},
        "unit_cost": 5,
        "carbon_intensity": 8,
    }
    signal.update(overrides)
    return signal


def _job() -> dict[str, object]:
    return {
        "job_id": "j-1",
        "work": 10,
        "deadline": 2000,
        "regions": ["eu-north", "us-west", "ap-east"],
        "residency": ["eu-north"],
        "max_cost": 100000,
        "carbon_cap": 100000,
    }


class ContinuousRoundsTest(unittest.TestCase):
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
        resources_module.publish(
            self.supply, _resource("r-a", "eu-north", carbon_intensity=9),
            "ra")
        resources_module.publish(
            self.supply, _resource("r-b", "us-west", carbon_intensity=5),
            "rb")
        resources_module.publish(
            self.supply, _resource("r-c", "ap-east", carbon_intensity=4),
            "rc")
        # Only eu-north has a signal at trade time, so the trade lands
        # on r-a; cleaner regions light up later.
        signals_module.publish(
            self.signals, _signal("eu-north", carbon_intensity=9), "sa")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-1", "t1", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-1", "d1", 10)
        self._bootstrap_execution()

    def _bootstrap_execution(self) -> None:
        region = "zz-bootstrap"
        jobs_module.submit(
            self.jobs,
            {"job_id": "j-9", "work": 1, "deadline": 3000,
             "regions": [region], "residency": [region],
             "max_cost": 10, "carbon_cap": 10}, "jk-9")
        resources_module.publish(
            self.supply, _resource("r-9", region=region,
                                   residency=[region], carbon_intensity=1),
            "r9")
        signals_module.publish(
            self.signals, _signal(region, carbon_intensity=1), "s9")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-9", "t9", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-9", "d9", 10)
        dispatch_module.claim(self.dispatch, "j-9", "c9", "o9", 500, 10)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-9", "e9",
                              "o9", None, 10)

    @property
    def _args(self) -> tuple[str, ...]:
        return (self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, self.ledger)

    @property
    def _all(self) -> tuple[str, ...]:
        return self._args + (self.settlements,)

    def _migrate_round(self, prefix: str, target_region: str,
                       base: int) -> None:
        signals_module.publish(
            self.signals,
            _signal(target_region, observed=base, expires=3000,
                    carbon_intensity=1, unit_cost=1),
            f"{prefix}-sig")
        evaluate(*self._args[:7], "j-1", f"{prefix}-adv", base + 10)
        apply(*self._args, "j-1", f"{prefix}-adv", f"{prefix}-res",
              base + 20)
        start(*self._args, "j-1", f"{prefix}-start", "own",
              base + 120, base + 30)
        record(*self._args, "j-1", f"{prefix}-copy", "own", "copy",
               "succeeded", "r", base + 40)
        record(*self._args, "j-1", f"{prefix}-switch", "own", "switch",
               "succeeded", "r", base + 50)

    def _current(self) -> dict[str, object]:
        return current(*self._all, "j-1")

    def test_two_migrated_rounds_chain_generations(self) -> None:
        self._migrate_round("r1", "ap-east", 100)
        first, created = settle(*self._all, "j-1", "r1-start", "s1", 160)
        self.assertTrue(created)
        self.assertEqual(first["generation"], 1)
        self.assertEqual(first["before"],
                         {"resource_id": "r-a", "version": 1})
        self.assertEqual(first["after"],
                         {"resource_id": "r-c", "version": 1})
        self.assertEqual(first["state"], "active")

        self._migrate_round("r2", "us-west", 300)
        second, created = settle(*self._all, "j-1", "r2-start", "s2", 360)
        self.assertTrue(created)
        self.assertEqual(second["generation"], 2)
        self.assertEqual(second["before"],
                         {"resource_id": "r-c", "version": 1})
        self.assertEqual(second["after"],
                         {"resource_id": "r-b", "version": 1})

        binding = self._current()
        self.assertEqual(binding["resource_id"], "r-b")
        self.assertEqual(binding["generation"], 2)
        self.assertEqual(binding["state"], "active")

    def test_round_two_reservation_leaves_latest_settled_binding(self) -> None:
        self._migrate_round("r1", "ap-east", 100)
        settle(*self._all, "j-1", "r1-start", "s1", 160)
        signals_module.publish(
            self.signals,
            _signal("us-west", observed=300, expires=3000,
                    carbon_intensity=1, unit_cost=1), "x")
        evaluate(*self._args[:7], "j-1", "a2", 310)
        intent, created = apply(*self._args, "j-1", "a2", "r2", 320)
        self.assertTrue(created)
        self.assertEqual(intent["source"],
                         {"resource_id": "r-c", "version": 1})

    def test_compensated_round_increments_generation_on_source(self) -> None:
        self._migrate_round("r1", "ap-east", 100)
        settle(*self._all, "j-1", "r1-start", "s1", 160)
        signals_module.publish(
            self.signals,
            _signal("us-west", observed=300, expires=3000,
                    carbon_intensity=1, unit_cost=1), "x")
        evaluate(*self._args[:7], "j-1", "a2", 310)
        apply(*self._args, "j-1", "a2", "r2", 320)
        start(*self._args, "j-1", "r2-start", "own", 460, 330)
        record(*self._args, "j-1", "r2-fail", "own", "copy", "failed",
               "boom", 340)
        result, created = settle(*self._all, "j-1", "r2-start", "s2", 360)
        self.assertTrue(created)
        self.assertEqual(result["generation"], 2)
        self.assertEqual(result["state"], "compensated")
        self.assertEqual(result["before"],
                         {"resource_id": "r-c", "version": 1})
        self.assertEqual(result["after"],
                         {"resource_id": "r-c", "version": 1})
        binding = self._current()
        self.assertEqual(binding["resource_id"], "r-c")
        self.assertEqual(binding["generation"], 2)
        self.assertEqual(binding["state"], "compensated")

    def test_another_key_same_plan_raises_without_write(self) -> None:
        self._migrate_round("r1", "ap-east", 100)
        settle(*self._all, "j-1", "r1-start", "s1", 160)
        self._migrate_round("r2", "us-west", 300)
        settle(*self._all, "j-1", "r2-start", "s2", 360)
        raw = Path(self.settlements).read_bytes()
        with self.assertRaises(ValueError):
            settle(*self._all, "j-1", "r2-start", "s-other", 370)
        self.assertEqual(Path(self.settlements).read_bytes(), raw)

    def test_pending_blocks_and_resumes(self) -> None:
        self._migrate_round("r1", "ap-east", 100)
        original = rebalance_module._commit_file

        def fail_final(realpath, payload, old_bytes,
                       prefix=".rebalance-"):
            if prefix == ".rebalance-settle-final-":
                raise OSError("injected")
            return original(realpath, payload, old_bytes, prefix=prefix)

        with mock.patch.object(rebalance_module, "_commit_file",
                               fail_final):
            with self.assertRaises(OSError):
                settle(*self._all, "j-1", "r1-start", "s1", 160)

        # Pending is not a completed generation.
        self.assertEqual(self._current()["state"], "traded")
        # Another settlement key conflicts while one is pending.
        with self.assertRaises(ValueError):
            settle(*self._all, "j-1", "r1-start", "s-other", 161)
        # A later migration cannot start while the settlement is pending.
        with self.assertRaises(ValueError):
            start(*self._args, "j-1", "late", "own", 900, 170)

        result, created = settle(*self._all, "j-1", "r1-start", "s1", 160)
        self.assertTrue(created)
        self.assertEqual(result["state"], "active")

        raw = Path(self.settlements).read_bytes()
        _again, again_created = settle(*self._all, "j-1", "r1-start",
                                       "s1", 160)
        self.assertFalse(again_created)
        self.assertEqual(Path(self.settlements).read_bytes(), raw)

    def test_round_audit_excludes_other_round_keys(self) -> None:
        self._migrate_round("r1", "ap-east", 100)
        first, _ = settle(*self._all, "j-1", "r1-start", "s1", 160)
        self.assertEqual(first["audit"],
                         ["r1-copy", "r1-res", "r1-start", "r1-switch"])
        self._migrate_round("r2", "us-west", 300)
        second, _ = settle(*self._all, "j-1", "r2-start", "s2", 360)
        self.assertEqual(second["audit"],
                         ["r2-copy", "r2-res", "r2-start", "r2-switch"])

    def test_history_sections_nest_by_job_and_plan_key(self) -> None:
        self._migrate_round("r1", "ap-east", 100)
        settle(*self._all, "j-1", "r1-start", "s1", 160)
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(data["version"], 3)
        self.assertEqual(list(data.keys()),
                         ["version", "intents", "plans", "settled",
                          "idempotency", "audit"])
        self.assertIn("r1-start", data["plans"]["j-1"])
        self.assertEqual(data["settled"]["r1-start"],
                         {"job_id": "j-1", "key": "s1", "state": "settled"})


if __name__ == "__main__":
    unittest.main()
