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
from carbon_market.rebalance import (apply, current, evaluate, record,
                                     recover, settle, start)


def _resource(resource_id: str, region: str, **overrides: object):
    resource: dict[str, object] = {
        "resource_id": resource_id, "region": region, "capacity": 100,
        "start": 0, "end": 500, "unit_cost": 5, "carbon_intensity": 8,
        "residency": (["eu-north", "us-west"] if region == "us-west"
                      else [region]),
    }
    resource.update(overrides)
    return resource


def _signal(region: str, **overrides: object):
    signal: dict[str, object] = {
        "region": region, "observed": 0, "expires": 500,
        "mix": {"solar": 10000}, "unit_cost": 5, "carbon_intensity": 8,
    }
    signal.update(overrides)
    return signal


def _job(job_id: str = "j-1"):
    return {"job_id": job_id, "work": 10, "deadline": 100,
            "regions": ["eu-north", "us-west"], "residency": ["eu-north"],
            "max_cost": 1000, "carbon_cap": 1000}


class RebalanceLineageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        base = self.tmp.name
        self.paths = {n: os.path.join(base, n + ".json") for n in (
            "jobs", "supply", "signals", "trades", "dispatch",
            "execution", "advice", "intents", "settlements")}
        jobs_module.submit(self.paths["jobs"], _job(), "jk-1")
        resources_module.publish(self.paths["supply"],
                                 _resource("r-1", "eu-north"), "k1")
        resources_module.publish(self.paths["supply"],
                                 _resource("r-2", "us-west", unit_cost=3,
                                           carbon_intensity=2), "k2")
        signals_module.publish(self.paths["signals"],
                               _signal("eu-north"), "s1")
        signals_module.publish(self.paths["signals"],
                               _signal("us-west", unit_cost=3,
                                       carbon_intensity=2), "s2")
        clear_live(self.paths["jobs"], self.paths["supply"],
                   self.paths["signals"], self.paths["trades"], "j-1",
                   "t1", 10)
        dispatch_module.commit(self.paths["jobs"], self.paths["supply"],
                               self.paths["trades"], self.paths["dispatch"],
                               "j-1", "d1", 10)
        self._bootstrap_execution_ledger()

    def _bootstrap_execution_ledger(self) -> None:
        if os.path.exists(self.paths["execution"]):
            return
        jobs_module.submit(self.paths["jobs"], _job("j-9"), "jk-9")
        resources_module.publish(
            self.paths["supply"],
            _resource("r-9", "zz", capacity=1000, unit_cost=1,
                      carbon_intensity=1), "k9")
        signals_module.publish(self.paths["signals"],
                               _signal("zz", unit_cost=1,
                                       carbon_intensity=1), "s9")
        clear_live(self.paths["jobs"], self.paths["supply"],
                   self.paths["signals"], self.paths["trades"], "j-9",
                   "t-9", 10)
        dispatch_module.commit(self.paths["jobs"], self.paths["supply"],
                               self.paths["trades"], self.paths["dispatch"],
                               "j-9", "d-9", 10)
        dispatch_module.claim(self.paths["dispatch"], "j-9", "c-9",
                              "owner-9", 80, 10)
        execution_module.plan(self.paths["jobs"], self.paths["supply"],
                              self.paths["trades"], self.paths["dispatch"],
                              self.paths["execution"], "j-9", "e-9",
                              "owner-9", None, 10)

    @property
    def _seven(self):
        return tuple(self.paths[n] for n in (
            "jobs", "supply", "signals", "trades", "dispatch",
            "execution", "advice"))

    @property
    def _eight(self):
        return self._seven + (self.paths["intents"],)

    @property
    def _nine(self):
        return self._eight + (self.paths["settlements"],)

    def _round(self, advice_key, apply_key, start_key, copy_key,
               switch_key, at_advice, at_apply, at_start, at_copy,
               at_switch, region, region_cost=0, region_ci=0,
               lease_end=90, owner="owner-1"):
        signals_module.publish(
            self.paths["signals"],
            _signal(region, observed=at_advice, expires=300,
                    unit_cost=region_cost, carbon_intensity=region_ci),
            advice_key + "-sig")
        evaluate(*self._seven, "j-1", advice_key, at_advice)
        apply(*self._eight, "j-1", advice_key, apply_key, at_apply)
        start(*self._eight, "j-1", start_key, owner, lease_end, at_start)
        record(*self._eight, "j-1", copy_key, owner, "copy", "succeeded",
               "rc-" + copy_key, at_copy)
        plan, _ = record(*self._eight, "j-1", switch_key, owner, "switch",
                         "succeeded", "rc-" + switch_key, at_switch)
        return plan

    def test_first_start_writes_history_layout(self) -> None:
        signals_module.publish(
            self.paths["signals"],
            _signal("eu-north", observed=15, expires=300, unit_cost=1,
                    carbon_intensity=1), "s3")
        evaluate(*self._seven, "j-1", "a1", 30)
        apply(*self._eight, "j-1", "a1", "r1", 40)
        self.assertEqual(
            json.loads(Path(self.paths["intents"]).read_text(
                encoding="utf-8"))["version"], 1)
        plan, created = start(*self._eight, "j-1", "m1", "owner-1", 90, 45)
        self.assertTrue(created)
        data = json.loads(Path(self.paths["intents"]).read_text(
            encoding="utf-8"))
        self.assertEqual(data["version"], 3)
        self.assertEqual(list(data["intents"]), ["r1"])
        self.assertEqual(list(data["plans"]), ["m1"])
        self.assertEqual(data["idempotency"]["m1"]["intent_key"], "r1")
        self.assertEqual(data["plans"]["m1"], plan)

    def test_two_successive_migrations_chain_generations(self) -> None:
        # Round 1: r-2 (trade) -> r-1.
        self._round("a1", "r1", "m1", "m2", "m4", 30, 40, 45, 50, 52,
                    "eu-north", region_cost=1, region_ci=1)
        s1, created = settle(*self._nine, "j-1", "m1", "s1", 55)
        self.assertTrue(created)
        self.assertEqual(s1["generation"], 1)
        self.assertEqual(s1["before"], {"resource_id": "r-2", "version": 1})
        self.assertEqual(s1["after"], {"resource_id": "r-1", "version": 1})
        self.assertEqual(s1["audit"], ["m1", "m2", "m4", "r1"])
        self.assertEqual(current(*self._nine, "j-1")["resource_id"], "r-1")

        # Round 2: r-1 -> r-2.
        self._round("a2", "r2", "m5", "m6", "m7", 60, 65, 66, 67, 68,
                    "us-west")
        s2, created = settle(*self._nine, "j-1", "m5", "s2", 70)
        self.assertTrue(created)
        self.assertEqual(s2["generation"], 2)
        # The new before-binding equals the previous real after-binding.
        self.assertEqual(s2["before"], {"resource_id": "r-1", "version": 1})
        self.assertEqual(s2["after"], {"resource_id": "r-2", "version": 1})
        binding = current(*self._nine, "j-1")
        self.assertEqual(binding["resource_id"], "r-2")
        self.assertEqual(binding["generation"], 2)

        # The intent ledger keeps every generation keyed distinctly.
        data = json.loads(Path(self.paths["intents"]).read_text(
            encoding="utf-8"))
        self.assertEqual(set(data["intents"]), {"r1", "r2"})
        self.assertEqual(set(data["plans"]), {"m1", "m5"})
        self.assertEqual(data["idempotency"]["m6"]["plan_key"], "m5")
        self.assertEqual(data["idempotency"]["m2"]["plan_key"], "m1")

    def test_old_action_keys_replay_against_their_own_round(self) -> None:
        self._round("a1", "r1", "m1", "m2", "m4", 30, 40, 45, 50, 52,
                    "eu-north", region_cost=1, region_ci=1)
        settle(*self._nine, "j-1", "m1", "s1", 55)
        self._round("a2", "r2", "m5", "m6", "m7", 60, 65, 66, 67, 68,
                    "us-west")
        settle(*self._nine, "j-1", "m5", "s2", 70)
        before = Path(self.paths["intents"]).read_bytes()

        replay_start, created = start(*self._eight, "j-1", "m1", "owner-1",
                                     90, 45)
        self.assertFalse(created)
        self.assertEqual(replay_start["target"],
                         {"resource_id": "r-1", "version": 1})
        replay_copy, created = record(*self._eight, "j-1", "m2", "owner-1",
                                      "copy", "succeeded", "rc-m2", 50)
        self.assertFalse(created)
        # The replay returns the bound plan's current snapshot: m1
        # finished migrated with both receipts, never round 2's plan.
        self.assertEqual(replay_copy["state"], "migrated")
        self.assertEqual(len(replay_copy["steps"]), 2)
        self.assertEqual(replay_copy["target"],
                         {"resource_id": "r-1", "version": 1})
        replay_round2, created = record(*self._eight, "j-1", "m6",
                                        "owner-1", "copy", "succeeded",
                                        "rc-m6", 67)
        self.assertFalse(created)
        self.assertEqual(replay_round2["target"],
                         {"resource_id": "r-2", "version": 1})
        replay_settle, created = settle(*self._nine, "j-1", "m1", "s1", 55)
        self.assertFalse(created)
        self.assertEqual(replay_settle["generation"], 1)
        self.assertEqual(Path(self.paths["intents"]).read_bytes(), before)

    def test_compensation_then_migration_chains_from_source(self) -> None:
        # Round 1 fails on the copy step: compensation keeps r-2.
        signals_module.publish(
            self.paths["signals"],
            _signal("eu-north", observed=15, expires=300, unit_cost=1,
                    carbon_intensity=1), "s3")
        evaluate(*self._seven, "j-1", "a1", 30)
        apply(*self._eight, "j-1", "a1", "r1", 40)
        start(*self._eight, "j-1", "m1", "owner-1", 90, 45)
        failed, _ = record(*self._eight, "j-1", "m2", "owner-1", "copy",
                           "failed", "boom", 50)
        self.assertEqual(failed["state"], "failed")
        s1, _ = settle(*self._nine, "j-1", "m1", "s1", 55)
        self.assertEqual(s1["state"], "compensated")
        self.assertEqual(s1["before"], s1["after"])
        self.assertEqual(current(*self._nine, "j-1")["state"],
                         "compensated")

        # Round 2 leaves the unchanged source r-2 and migrates to r-1.
        self._round("a2", "r2", "m5", "m6", "m7", 60, 65, 66, 67, 68,
                    "eu-north", region_cost=1, region_ci=1)
        s2, _ = settle(*self._nine, "j-1", "m5", "s2", 70)
        self.assertEqual(s2["generation"], 2)
        self.assertEqual(s2["before"], {"resource_id": "r-2", "version": 1})
        self.assertEqual(s2["after"], {"resource_id": "r-1", "version": 1})

    def test_interrupted_round_compensates_then_chain_continues(self) -> None:
        signals_module.publish(
            self.paths["signals"],
            _signal("eu-north", observed=15, expires=300, unit_cost=1,
                    carbon_intensity=1), "s3")
        evaluate(*self._seven, "j-1", "a1", 30)
        apply(*self._eight, "j-1", "a1", "r1", 40)
        start(*self._eight, "j-1", "m1", "owner-1", 90, 45)
        record(*self._eight, "j-1", "m2", "owner-1", "copy", "succeeded",
               "rc1", 50)
        interrupted, created = recover(*self._eight, "j-1", "m3",
                                       "owner-1", 91)
        self.assertTrue(created)
        self.assertEqual(interrupted["state"], "interrupted")
        s1, _ = settle(*self._nine, "j-1", "m1", "s1", 92)
        self.assertEqual(s1["state"], "compensated")
        # Round 2 follows from the source after the compensation.
        self._round("a2", "r2", "m5", "m6", "m7", 94, 95, 96, 97, 98,
                    "eu-north", region_cost=1, region_ci=1, lease_end=100)
        s2, _ = settle(*self._nine, "j-1", "m5", "s2", 99)
        self.assertEqual(s2["generation"], 2)
        self.assertEqual(s2["before"], {"resource_id": "r-2", "version": 1})

    def test_each_plan_settles_once(self) -> None:
        self._round("a1", "r1", "m1", "m2", "m4", 30, 40, 45, 50, 52,
                    "eu-north", region_cost=1, region_ci=1)
        settle(*self._nine, "j-1", "m1", "s1", 55)
        with self.assertRaises(ValueError):
            settle(*self._nine, "j-1", "m1", "s-other", 56)
        # A pending settlement also blocks a second settlement key.
        self._round("a2", "r2", "m5", "m6", "m7", 60, 65, 66, 67, 68,
                    "us-west")
        from carbon_market import rebalance as rebalance_module
        from unittest import mock
        original = rebalance_module._commit_file

        def fail_final(realpath, payload, old_bytes,
                       prefix=".rebalance-"):
            if prefix == ".rebalance-settle-final-":
                raise OSError("injected")
            return original(realpath, payload, old_bytes, prefix=prefix)

        with mock.patch.object(rebalance_module, "_commit_file",
                               fail_final):
            with self.assertRaises(OSError):
                settle(*self._nine, "j-1", "m5", "s2", 70)
        with self.assertRaises(ValueError):
            settle(*self._nine, "j-1", "m5", "s-other2", 70)
        # Resuming the pending settlement completes it once.
        resumed, created = settle(*self._nine, "j-1", "m5", "s2", 70)
        self.assertTrue(created)
        self.assertEqual(resumed["state"], "active")
        again, created = settle(*self._nine, "j-1", "m5", "s2", 70)
        self.assertFalse(created)
        self.assertEqual(again["generation"], 2)

    def test_pending_settlement_blocks_follow_up_migration(self) -> None:
        self._round("a1", "r1", "m1", "m2", "m4", 30, 40, 45, 50, 52,
                    "eu-north", region_cost=1, region_ci=1)
        from carbon_market import rebalance as rebalance_module
        from unittest import mock
        original = rebalance_module._commit_file

        def fail_final(realpath, payload, old_bytes,
                       prefix=".rebalance-"):
            if prefix == ".rebalance-settle-final-":
                raise OSError("injected")
            return original(realpath, payload, old_bytes, prefix=prefix)

        with mock.patch.object(rebalance_module, "_commit_file",
                               fail_final):
            with self.assertRaises(OSError):
                settle(*self._nine, "j-1", "m1", "s1", 55)
        # current ignores the pending record: still the traded generation.
        self.assertEqual(current(*self._nine, "j-1")["generation"], 0)
        # The migration cannot be re-opened while settlement is pending.
        evaluate(*self._seven, "j-1", "a2", 56)
        with self.assertRaises(ValueError):
            apply(*self._eight, "j-1", "a2", "r2", 57)
        with self.assertRaises(ValueError):
            start(*self._eight, "j-1", "m9", "owner-1", 90, 58)


if __name__ == "__main__":
    unittest.main()
