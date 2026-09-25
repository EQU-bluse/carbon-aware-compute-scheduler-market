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
        self.ledger = os.path.join(base, "advice.json")
        self.intents = os.path.join(base, "intents.json")
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

    def _commit_job(self, job_id: str = "j-1", at: int = 10) -> None:
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   job_id, f"t-{job_id}", at)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, job_id, f"d-{job_id}", at)
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

    def _ensure_advice_ledger(self) -> None:
        # A second isolated bootstrap job on its own region creates the
        # advice ledger with a keep record, without touching the applied
        # job's candidates, capacity or dispatch.
        if os.path.exists(self.ledger):
            return
        self._ensure_execution_ledger()
        region = "zz-advice"
        jobs_module.submit(
            self.jobs,
            _job("j-8", regions=[region], residency=[region]), "jk-8")
        resources_module.publish(
            self.supply,
            _resource("r-8", region=region, capacity=1000,
                      residency=[region]), "k-8")
        signals_module.publish(
            self.signals, _signal(region, carbon_intensity=1), "s-8")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-8", "t-8", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-8", "d-8", 10)
        evaluate(self.jobs, self.supply, self.signals, self.trades,
                 self.dispatch, self.execution, self.ledger,
                 "j-8", "a-8", 20)

    def _evaluate(self, job_id: str = "j-1", key: str = "a1",
                  at: int = 30):
        return evaluate(self.jobs, self.supply, self.signals, self.trades,
                        self.dispatch, self.execution, self.ledger,
                        job_id, key, at)

    def _apply(self, job_id: str = "j-1", advice_key: str = "a1",
               key: str = "i1", at: int = 40):
        return apply(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.ledger,
                     self.intents, job_id, advice_key, key, at)

    def _publish_clean_eu_north(self, observed: int = 15) -> None:
        # A newer, cleaner eu-north signal observed after the trades
        # makes r-1 the advice target.
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=observed, expires=300,
                    unit_cost=1, carbon_intensity=1), "s-clean")

    def _migrate_advice(self, job_id: str = "j-1", key: str = "a1",
                        at: int = 30) -> dict[str, object]:
        record, created = self._evaluate(job_id=job_id, key=key, at=at)
        self.assertTrue(created)
        self.assertEqual(record["recommendation"], "migrate")
        return record

    # -- basic outcomes ----------------------------------------------------

    def test_apply_reserves_the_advice_target(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        record, created = self._apply()
        self.assertTrue(created)
        self.assertEqual(list(record.keys()),
                         ["job_id", "advice", "at", "source", "target",
                          "supply", "signal", "dispatch", "execution",
                          "state"])
        self.assertEqual(record["job_id"], "j-1")
        self.assertEqual(record["advice"], "a1")
        self.assertEqual(record["at"], 40)
        self.assertEqual(record["source"],
                         {"resource_id": "r-2", "version": 1})
        self.assertEqual(record["target"],
                         {"resource_id": "r-1", "version": 1})
        self.assertEqual(record["supply"]["resource_id"], "r-1")
        self.assertEqual(record["supply"]["version"], 1)
        self.assertEqual(record["signal"]["region"], "eu-north")
        self.assertEqual(record["signal"]["version"], 2)
        self.assertEqual(record["dispatch"], "ready")
        self.assertEqual(record["execution"], "none")
        self.assertEqual(record["state"], "reserved")

    def test_apply_at_the_advice_moment(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        record, created = self._apply(at=30)
        self.assertTrue(created)
        self.assertEqual(record["at"], 30)

    def test_failed_and_interrupted_plans_allow_apply(self) -> None:
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
        self._publish_clean_eu_north()
        self._migrate_advice()
        record, created = self._apply()
        self.assertTrue(created)
        self.assertEqual(record["dispatch"], "failed")
        self.assertEqual(record["execution"], "failed")

    # -- advice entry checks -------------------------------------------------

    def test_keep_advice_raises_value_error(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        record, _ = self._evaluate()
        self.assertEqual(record["recommendation"], "keep")
        with self.assertRaises(ValueError):
            self._apply()
        self.assertFalse(os.path.exists(self.intents))

    def test_advice_of_another_job_raises_value_error(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        self._commit_job("j-2")
        with self.assertRaises(ValueError):
            self._apply(job_id="j-2", advice_key="a1")
        self.assertFalse(os.path.exists(self.intents))

    def test_moment_regression_raises_value_error(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        with self.assertRaises(ValueError):
            self._apply(at=29)
        self.assertFalse(os.path.exists(self.intents))

    # -- lifecycle guards --------------------------------------------------

    def test_claimed_decision_raises_permission_error(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 80, 10)
        with self.assertRaises(PermissionError):
            self._apply()
        self.assertFalse(os.path.exists(self.intents))

    def test_active_plan_raises_permission_error(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 80, 10)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "e1",
                              "owner", None, 20)
        with self.assertRaises(PermissionError):
            self._apply()
        self.assertFalse(os.path.exists(self.intents))

    def test_succeeded_dispatch_raises_value_error(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 80, 10)
        dispatch_module.finish(self.dispatch, "j-1", "f1", "owner",
                               "succeeded", 20)
        with self.assertRaises(ValueError):
            self._apply()
        self.assertFalse(os.path.exists(self.intents))

    def test_completed_plan_raises_value_error(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        dispatch_module.claim(self.dispatch, "j-1", "c1", "owner", 80, 10)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, "j-1", "e1",
                              "owner", None, 20)
        execution_module.record(self.execution, "j-1", 1, "e2", "owner",
                                "stage", "succeeded", "rc1", 25)
        execution_module.record(self.execution, "j-1", 1, "e3", "owner",
                                "start", "succeeded", "rc2", 30)
        # Settle the decision so only the completed plan guards.
        dispatch_module.finish(self.dispatch, "j-1", "f1", "owner",
                               "failed", 35)
        with self.assertRaises(ValueError):
            self._apply()
        self.assertFalse(os.path.exists(self.intents))

    def test_past_deadline_raises_timeout_error(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        with self.assertRaises(TimeoutError):
            self._apply(at=101)
        self.assertFalse(os.path.exists(self.intents))

    # -- recomputation ------------------------------------------------------

    def test_target_changed_raises_lookup_error(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        # An even cleaner us-west signal makes the retained r-2 the
        # first candidate again, so the advice target is stale.
        signals_module.publish(
            self.signals,
            _signal("us-west", observed=35, expires=400, unit_cost=0,
                    carbon_intensity=0), "s-late")
        with self.assertRaises(LookupError):
            self._apply()
        self.assertFalse(os.path.exists(self.intents))

    def test_no_feasible_candidate_raises_lookup_error(self) -> None:
        resources_module.publish(self.supply, _resource("r-1"), "k1")
        resources_module.publish(
            self.supply,
            _resource("r-2", region="us-west",
                      residency=["eu-north", "us-west"]), "k2")
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=0, expires=19, unit_cost=5,
                    carbon_intensity=8), "s1")
        signals_module.publish(
            self.signals,
            _signal("us-west", observed=0, expires=19, unit_cost=3,
                    carbon_intensity=2), "s2")
        self._commit_job()
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=15, expires=25, unit_cost=1,
                    carbon_intensity=1), "s3")
        record, _ = self._evaluate(at=20)
        self.assertEqual(record["recommendation"], "migrate")
        # At 30 every signal window has closed: the feasible set is
        # empty.
        with self.assertRaises(LookupError):
            self._apply(at=30)
        self.assertFalse(os.path.exists(self.intents))

    def test_other_job_intent_deducts_target_capacity(self) -> None:
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
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        self._commit_job()
        self._commit_job("j-2")
        # Both jobs are advised to migrate to r-1, which holds exactly
        # one job's work.
        self._publish_clean_eu_north()
        self._migrate_advice("j-1", key="a1")
        self._migrate_advice("j-2", key="a2")
        record, created = self._apply(job_id="j-2", advice_key="a2",
                                      key="i2")
        self.assertTrue(created)
        self.assertEqual(record["target"],
                         {"resource_id": "r-1", "version": 1})
        # j-2's intent now occupies r-1 entirely; j-1's target is no
        # longer feasible.
        with self.assertRaises(LookupError):
            self._apply(job_id="j-1", advice_key="a1", key="i1")
        data = json.loads(Path(self.intents).read_text(encoding="utf-8"))
        self.assertEqual(list(data["intents"]), ["j-2"])

    # -- idempotency --------------------------------------------------------

    def test_replay_returns_stored_record_without_write(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        first, created_first = self._apply()
        self.assertTrue(created_first)
        before = Path(self.intents).read_bytes()
        second, created_second = self._apply()
        self.assertFalse(created_second)
        self.assertEqual(second, first)
        self.assertEqual(Path(self.intents).read_bytes(), before)

    def test_replay_survives_later_signal_publication(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        first, _ = self._apply()
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=50, expires=400, unit_cost=0,
                    carbon_intensity=0), "s-later")
        before = Path(self.intents).read_bytes()
        replayed, created = self._apply()
        self.assertFalse(created)
        self.assertEqual(replayed, first)
        self.assertEqual(Path(self.intents).read_bytes(), before)

    def test_same_key_changed_request_raises(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        self._apply()
        before = Path(self.intents).read_bytes()
        with self.assertRaises(ValueError):
            self._apply(at=41)
        self.assertEqual(Path(self.intents).read_bytes(), before)

    def test_same_job_another_key_raises(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        self._apply()
        before = Path(self.intents).read_bytes()
        with self.assertRaises(ValueError):
            self._apply(key="i2")
        self.assertEqual(Path(self.intents).read_bytes(), before)

    # -- errors that must not create the ledger ----------------------------

    def test_unknown_job_raises_key_error(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        with self.assertRaises(KeyError):
            self._apply(job_id="j-nope")
        self.assertFalse(os.path.exists(self.intents))

    def test_unknown_advice_key_raises_key_error(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        with self.assertRaises(KeyError):
            self._apply(advice_key="a-nope")
        self.assertFalse(os.path.exists(self.intents))

    def test_trade_without_dispatch_raises_key_error(self) -> None:
        self._seed_two_regions()
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-1", "t1", 10)
        # The bootstrap creates the dispatch, execution and advice
        # layers for other jobs, so j-1's missing decision is the
        # error surfaced.
        self._ensure_execution_ledger()
        self._ensure_advice_ledger()
        with self.assertRaises(KeyError):
            self._apply()
        self.assertFalse(os.path.exists(self.intents))

    def test_accepted_job_without_trade_raises_lookup_error(self) -> None:
        self._seed_two_regions()
        self._ensure_execution_ledger()
        self._ensure_advice_ledger()
        with self.assertRaises(LookupError):
            self._apply()
        self.assertFalse(os.path.exists(self.intents))

    def test_missing_inputs_raise_file_not_found(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        missing = [os.path.join(self.tmp.name, name) for name in (
            "no-jobs.json", "no-supply.json", "no-signals.json",
            "no-trades.json", "no-dispatch.json", "no-execution.json",
            "no-advice.json")]
        replacements = [
            (missing[0], self.supply, self.signals, self.trades,
             self.dispatch, self.execution, self.ledger),
            (self.jobs, missing[1], self.signals, self.trades,
             self.dispatch, self.execution, self.ledger),
            (self.jobs, self.supply, missing[2], self.trades,
             self.dispatch, self.execution, self.ledger),
            (self.jobs, self.supply, self.signals, missing[3],
             self.dispatch, self.execution, self.ledger),
            (self.jobs, self.supply, self.signals, self.trades,
             missing[4], self.execution, self.ledger),
            (self.jobs, self.supply, self.signals, self.trades,
             self.dispatch, missing[5], self.ledger),
            (self.jobs, self.supply, self.signals, self.trades,
             self.dispatch, self.execution, missing[6]),
        ]
        for replaced in replacements:
            with self.subTest(replaced=replaced):
                with self.assertRaises(FileNotFoundError):
                    apply(*replaced, self.intents, "j-1", "a1", "i1", 40)

    def test_missing_ledger_parent_raises_file_not_found(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        missing = os.path.join(self.tmp.name, "no-such-dir",
                               "intents.json")
        with self.assertRaises(FileNotFoundError):
            apply(self.jobs, self.supply, self.signals, self.trades,
                  self.dispatch, self.execution, self.ledger, missing,
                  "j-1", "a1", "i1", 40)
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    # -- argument validation -------------------------------------------------

    def test_invalid_arguments(self) -> None:
        base_args = [self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.ledger,
                     self.intents, "j-1", "a1", "i1", 40]
        for index in range(11):
            broken = list(base_args)
            broken[index] = ""
            with self.assertRaises(ValueError):
                apply(*broken)
        with self.assertRaises(ValueError):
            apply(*base_args[:11], -1)
        with self.assertRaises(ValueError):
            apply(*base_args[:11], True)  # type: ignore[arg-type]

    def test_paths_must_be_distinct(self) -> None:
        args = [self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.ledger, self.intents,
                "j-1", "a1", "i1", 40]
        for index in range(8):
            broken = list(args)
            # Collide this position with the next path so two positions
            # resolve to the same real location while all others differ.
            broken[index] = args[(index + 1) % 8]
            with self.assertRaises(ValueError):
                apply(*broken)

    # -- ledger form ---------------------------------------------------------

    def test_ledger_is_canonical_with_sorted_sections(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        # A second migrating job with ids chosen to sort against j-1,
        # including a non-ASCII idempotency key that must be written
        # through.
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        self._commit_job("j-2")
        self._publish_clean_eu_north()
        self._migrate_advice("j-1", key="a1")
        self._migrate_advice("j-2", key="a2")
        apply(self.jobs, self.supply, self.signals, self.trades,
              self.dispatch, self.execution, self.ledger, self.intents,
              "j-1", "a1", "z9", 40)
        apply(self.jobs, self.supply, self.signals, self.trades,
              self.dispatch, self.execution, self.ledger, self.intents,
              "j-2", "a2", "é-mid", 40)

        raw = Path(self.intents).read_bytes()
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
        self.assertEqual(list(data["idempotency"]),
                         sorted(data["idempotency"]))
        self.assertEqual(list(data["audit"]), sorted(data["audit"]))
        self.assertEqual(set(data["intents"]),
                         {request["job_id"]
                          for request in data["idempotency"].values()})
        self.assertEqual(set(data["idempotency"]), set(data["audit"]))

    def test_tampered_ledger_raises_value_error(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        self._apply()
        data = json.loads(Path(self.intents).read_text(encoding="utf-8"))
        data["intents"]["j-1"]["state"] = "released"
        Path(self.intents).write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"))
            + "\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            self._apply(job_id="j-2", advice_key="a1", key="i2")

    def test_no_temp_fragments_left_after_guard_failures(self) -> None:
        self._seed_two_regions()
        self._commit_job()
        self._publish_clean_eu_north()
        self._migrate_advice()
        with self.assertRaises(TimeoutError):
            self._apply(at=200)
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])


if __name__ == "__main__":
    unittest.main()
