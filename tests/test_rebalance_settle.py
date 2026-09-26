from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import dispatch as dispatch_module
from carbon_market import execution as execution_module
from carbon_market import jobs as jobs_module
from carbon_market import rebalance as rebalance_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.market import clear_live
from carbon_market.rebalance import (apply, current, evaluate, record,
                                     recover, settle, start)
from carbon_market.rebalance import _settlement_canonical_bytes


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
        self.intents = os.path.join(base, "intents.json")
        self.ledger = os.path.join(base, "settle.json")
        jobs_module.submit(self.jobs, _job(), "jk-1")

    def _args8(self) -> tuple[str, ...]:
        return (self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, self.intents)

    def _args9(self) -> tuple[str, ...]:
        return self._args8() + (self.ledger,)

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
        self._seed_two_regions()
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-1", "t1", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-1", "d1", 10)
        self._ensure_execution_ledger()
        signals_module.publish(
            self.signals,
            _signal("eu-north", observed=15, expires=300, unit_cost=1,
                    carbon_intensity=1), "s3")
        advice, created = evaluate(self.jobs, self.supply, self.signals,
                                   self.trades, self.dispatch,
                                   self.execution, self.advice, "j-1",
                                   "a1", 30)
        self.assertTrue(created)
        self.assertEqual(advice["recommendation"], "migrate")
        intent, created = apply(self.jobs, self.supply, self.signals,
                                self.trades, self.dispatch, self.execution,
                                self.advice, self.intents, "j-1", "a1",
                                "r1", 40)
        self.assertTrue(created)

    def _start(self) -> None:
        plan, created = start(*self._args8(), "j-1", "m1", "owner-1", 90, 45)
        self.assertTrue(created)
        self.assertEqual(plan["state"], "active")

    def _copy(self, result: str = "succeeded", at: int = 50,
              receipt: str = "rc1", key: str = "m2") -> None:
        plan, created = record(*self._args8(), "j-1", key, "owner-1",
                               "copy", result, receipt, at)
        self.assertTrue(created)

    def _switch(self, result: str = "succeeded", at: int = 52,
                receipt: str = "rc2", key: str = "m4") -> None:
        plan, created = record(*self._args8(), "j-1", key, "owner-1",
                               "switch", result, receipt, at)
        self.assertTrue(created)
        return plan

    def _migrate(self) -> None:
        self._start()
        self._copy()
        plan = self._switch()
        self.assertEqual(plan["state"], "migrated")

    def _fail(self) -> None:
        self._start()
        self._copy(result="failed", receipt="boom")

    def _interrupt(self) -> None:
        self._start()
        self._copy()
        plan, created = recover(*self._args8(), "j-1", "m3", "owner-1", 95)
        self.assertTrue(created)
        self.assertEqual(plan["state"], "interrupted")

    def _settle(self, at: int = 60, key: str = "x1",
                plan_key: str = "m1"):
        return settle(*self._args9(), "j-1", plan_key, key, at)

    # -- migrated ------------------------------------------------------------

    def test_settle_migrated_confirms_target(self) -> None:
        self._commit_job()
        self._migrate()
        record_, created = self._settle()
        self.assertTrue(created)
        self.assertEqual(list(record_.keys()),
                         ["job_id", "plan_key", "generation",
                          "source_state", "before", "after", "at", "state",
                          "audit"])
        self.assertEqual(record_["job_id"], "j-1")
        self.assertEqual(record_["plan_key"], "m1")
        self.assertEqual(record_["generation"], 1)
        self.assertEqual(record_["source_state"], "migrated")
        self.assertEqual(record_["before"],
                         {"resource_id": "r-2", "version": 1})
        self.assertEqual(record_["after"],
                         {"resource_id": "r-1", "version": 1})
        self.assertEqual(record_["at"], 60)
        self.assertEqual(record_["state"], "active")
        self.assertEqual(record_["audit"], {"intent": "r1", "plan": "m1"})

    def test_settle_writes_canonical_ledger_with_audit(self) -> None:
        self._commit_job()
        self._migrate()
        self._settle()
        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        self.assertNotIn(b"\\u", raw)
        data = json.loads(raw.decode("utf-8"))
        self.assertEqual(list(data.keys()),
                         ["version", "settlements", "idempotency", "audit"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(set(data["settlements"]), {"x1"})
        self.assertEqual(set(data["idempotency"]), {"x1"})
        self.assertEqual(set(data["audit"]), {"x1"})
        self.assertEqual(data["idempotency"]["x1"],
                         {"job_id": "j-1", "plan_key": "m1", "at": 60})
        self.assertEqual(data["audit"]["x1"]["request"],
                         data["idempotency"]["x1"])
        self.assertEqual(data["audit"]["x1"]["result"],
                         data["settlements"]["x1"])

    def test_settle_replay_returns_false_without_writing(self) -> None:
        self._commit_job()
        self._migrate()
        first, created = self._settle()
        self.assertTrue(created)
        before = Path(self.ledger).read_bytes()
        replayed, created = self._settle()
        self.assertFalse(created)
        self.assertEqual(replayed, first)
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    # -- compensation ---------------------------------------------------------

    def test_settle_failed_compensates_keeping_source(self) -> None:
        self._commit_job()
        self._fail()
        record_, created = self._settle()
        self.assertTrue(created)
        self.assertEqual(record_["state"], "compensated")
        self.assertEqual(record_["source_state"], "failed")
        self.assertEqual(record_["before"],
                         {"resource_id": "r-2", "version": 1})
        self.assertEqual(record_["after"],
                         {"resource_id": "r-1", "version": 1})
        self.assertEqual(current(*self._args9(), "j-1"),
                         {"resource_id": "r-2", "version": 1})

    def test_settle_interrupted_compensates(self) -> None:
        self._commit_job()
        self._interrupt()
        record_, created = self._settle(at=96)
        self.assertTrue(created)
        self.assertEqual(record_["state"], "compensated")
        self.assertEqual(record_["source_state"], "interrupted")
        self.assertEqual(current(*self._args9(), "j-1"),
                         {"resource_id": "r-2", "version": 1})

    def test_settle_late_recovery_past_deadline_still_completes(self) -> None:
        self._commit_job()
        self._interrupt()
        record_, created = self._settle(at=500)
        self.assertTrue(created)
        self.assertEqual(record_["state"], "compensated")
        self.assertEqual(record_["at"], 500)

    # -- two phases and crash recovery ----------------------------------------

    def _write_pending_ledger(self) -> None:
        pending = {
            "job_id": "j-1",
            "plan_key": "m1",
            "generation": 1,
            "source_state": "migrated",
            "before": {"resource_id": "r-2", "version": 1},
            "after": {"resource_id": "r-1", "version": 1},
            "at": 60,
            "state": "pending",
            "audit": {"intent": "r1", "plan": "m1"},
        }
        payload = _settlement_canonical_bytes(
            {"x1": pending},
            {"x1": {"job_id": "j-1", "plan_key": "m1", "at": 60}},
            {})
        Path(self.ledger).write_bytes(payload)

    def test_pending_then_terminal_are_two_commits(self) -> None:
        # A crash between the durable pending write and phase two leaves
        # exactly a binding without an audit event.
        self._commit_job()
        self._migrate()
        self._write_pending_ledger()
        pending_raw = Path(self.ledger).read_bytes()
        pending = json.loads(pending_raw.decode("utf-8"))
        self.assertEqual(pending["settlements"]["x1"]["state"], "pending")
        self.assertNotIn("x1", pending["audit"])
        record_, created = self._settle()
        self.assertTrue(created)
        self.assertEqual(record_["state"], "active")
        self.assertNotEqual(Path(self.ledger).read_bytes(), pending_raw)
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(set(data["audit"]), {"x1"})
        # The resumed run committed the audit exactly once; a replay now
        # is a completed replay and writes nothing.
        before = Path(self.ledger).read_bytes()
        replayed, created = self._settle()
        self.assertFalse(created)
        self.assertEqual(replayed["state"], "active")
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_pending_resume_with_changed_request_raises(self) -> None:
        self._commit_job()
        self._migrate()
        self._write_pending_ledger()
        before = Path(self.ledger).read_bytes()
        with self.assertRaises(ValueError):
            self._settle(at=61)
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    # -- refusals --------------------------------------------------------------

    def test_settle_active_plan_raises_permission_error(self) -> None:
        self._commit_job()
        self._start()
        with self.assertRaises(PermissionError):
            self._settle()
        self.assertFalse(os.path.exists(self.ledger))

    def test_settle_active_after_copy_still_raises_permission_error(
            self) -> None:
        self._commit_job()
        self._start()
        self._copy()
        with self.assertRaises(PermissionError):
            self._settle()
        self.assertFalse(os.path.exists(self.ledger))

    def test_settle_unknown_job_intent_and_plan_raise_key_error(self) -> None:
        self._commit_job()
        self._migrate()
        with self.assertRaises(KeyError):
            settle(*self._args9(), "j-nope", "m1", "x1", 60)
        with self.assertRaises(KeyError):
            self._settle(plan_key="m-nope")
        # A job that exists but never reserved an intent is unknown to
        # settlement as well.
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        with self.assertRaises(KeyError):
            settle(*self._args9(), "j-2", "m1", "x1", 60)

    def test_settle_same_key_changed_request_raises(self) -> None:
        self._commit_job()
        self._migrate()
        self._settle()
        before = Path(self.ledger).read_bytes()
        with self.assertRaises(ValueError):
            self._settle(at=61)
        with self.assertRaises(ValueError):
            self._settle(plan_key="m9")
        with self.assertRaises(ValueError):
            settle(*self._args9(), "j-1", "m1", "x1", True)  # type: ignore
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_settle_other_key_repeat_raises(self) -> None:
        self._commit_job()
        self._migrate()
        self._settle()
        before = Path(self.ledger).read_bytes()
        with self.assertRaises(ValueError):
            self._settle(key="x2")
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_settle_moment_before_migrated_evidence_raises(self) -> None:
        self._commit_job()
        self._migrate()
        with self.assertRaises(ValueError):
            self._settle(at=51)
        self.assertFalse(os.path.exists(self.ledger))

    def test_settle_moment_before_failed_evidence_raises(self) -> None:
        self._commit_job()
        self._fail()
        with self.assertRaises(ValueError):
            self._settle(at=49)
        self.assertFalse(os.path.exists(self.ledger))

    def test_settle_moment_before_recovery_evidence_raises(self) -> None:
        self._commit_job()
        self._interrupt()
        with self.assertRaises(ValueError):
            self._settle(at=94)
        self.assertFalse(os.path.exists(self.ledger))

    def test_settle_moment_equal_to_evidence_completes(self) -> None:
        self._commit_job()
        self._migrate()
        record_, created = self._settle(at=52)
        self.assertTrue(created)
        self.assertEqual(record_["at"], 52)

    # -- current ---------------------------------------------------------------

    def test_current_defaults_to_the_trade(self) -> None:
        self._commit_job()
        # No migration plan, no settlement ledger at all.
        self.assertFalse(os.path.exists(self.ledger))
        self.assertEqual(current(*self._args9(), "j-1"),
                         {"resource_id": "r-2", "version": 1})

    def test_current_follows_active_and_compensated_settlements(self) -> None:
        self._commit_job()
        self._fail()
        self._settle()
        self.assertEqual(current(*self._args9(), "j-1"),
                         {"resource_id": "r-2", "version": 1})

    def test_current_ignores_pending_settlement(self) -> None:
        self._commit_job()
        self._migrate()
        self._write_pending_ledger()
        self.assertEqual(current(*self._args9(), "j-1"),
                         {"resource_id": "r-2", "version": 1})
        self._settle()
        self.assertEqual(current(*self._args9(), "j-1"),
                         {"resource_id": "r-1", "version": 1})

    def test_current_unknown_job_raises_key_error(self) -> None:
        self._commit_job()
        with self.assertRaises(KeyError):
            current(*self._args9(), "j-nope")

    def test_current_known_job_without_trade_raises_lookup_error(self) -> None:
        self._commit_job()
        jobs_module.submit(self.jobs, _job("j-3"), "jk-3")
        with self.assertRaises(LookupError):
            current(*self._args9(), "j-3")

    def test_concurrent_same_key_settles_exactly_once(self) -> None:
        import threading
        self._commit_job()
        self._migrate()
        results: list[tuple[dict[str, object], bool]] = []
        errors: list[BaseException] = []

        def run() -> None:
            try:
                results.append(self._settle())
            except BaseException as exc:  # pragma: no cover - debug aid
                errors.append(exc)

        threads = [threading.Thread(target=run) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 4)
        created = sorted(flag for _record, flag in results)
        self.assertEqual(created, [False, False, False, True])
        records = {json.dumps(record, sort_keys=True)
                   for record, _flag in results}
        self.assertEqual(len(records), 1)
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(list(data["audit"]), ["x1"])

    # -- generation chain and contradictory evidence ---------------------------

    def _settled_ledger_bytes(self, records: dict[str, object],
                              requests: dict[str, object]) -> bytes:
        return _settlement_canonical_bytes(records, requests, {})  # type: ignore

    def test_broken_generation_chain_raises_value_error(self) -> None:
        self._commit_job()
        self._migrate()
        self._settle()
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        first = data["settlements"]["x1"]
        second = dict(first)
        second["generation"] = 3  # skips generation 2
        second["at"] = 70
        records = dict(data["settlements"])
        records["x2"] = second
        requests = dict(data["idempotency"])
        requests["x2"] = {"job_id": "j-1", "plan_key": "m1", "at": 70}
        events = {
            "x1": {"key": "x1", "request": requests["x1"],
                   "result": first},
            "x2": {"key": "x2", "request": requests["x2"],
                   "result": second},
        }
        Path(self.ledger).write_bytes(_settlement_canonical_bytes(
            records, requests, events))
        with self.assertRaises(ValueError):
            self._settle(key="x3", at=80)
        with self.assertRaises(ValueError):
            current(*self._args9(), "j-1")

    def test_snapshot_reference_contradiction_raises_value_error(self) -> None:
        self._commit_job()
        self._migrate()
        # A settlement claiming a failed outcome while the recorded plan
        # is migrated is contradictory terminal evidence.
        record_ = {
            "job_id": "j-1",
            "plan_key": "m1",
            "generation": 1,
            "source_state": "failed",
            "before": {"resource_id": "r-2", "version": 1},
            "after": {"resource_id": "r-1", "version": 1},
            "at": 60,
            "state": "compensated",
            "audit": {"intent": "r1", "plan": "m1"},
        }
        Path(self.ledger).write_bytes(self._settled_ledger_bytes(
            {"x1": record_},
            {"x1": {"job_id": "j-1", "plan_key": "m1", "at": 60}}))
        with self.assertRaises(ValueError):
            current(*self._args9(), "j-1")

    def test_wrong_audit_association_raises_value_error(self) -> None:
        self._commit_job()
        self._migrate()
        self._settle()
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        data["settlements"]["x1"]["audit"]["intent"] = "forged"
        # Drop the audit section so the structure validator reaches the
        # reference check rather than the event mismatch first.
        Path(self.ledger).write_bytes(self._settled_ledger_bytes(
            data["settlements"], data["idempotency"]))
        with self.assertRaises(ValueError):
            current(*self._args9(), "j-1")

    def test_tampered_terminal_state_raises_value_error(self) -> None:
        self._commit_job()
        self._migrate()
        self._settle()
        good = Path(self.ledger).read_bytes()
        data = json.loads(good.decode("utf-8"))
        # An active confirmation paired with a non-migrated outcome is a
        # state contradiction, not merely a pending crash record.
        data["settlements"]["x1"]["state"] = "active"
        data["settlements"]["x1"]["source_state"] = "failed"
        Path(self.ledger).write_bytes(self._settled_ledger_bytes(
            data["settlements"], data["idempotency"]))
        with self.assertRaises(ValueError):
            current(*self._args9(), "j-1")

    # -- arguments, files and robustness ---------------------------------------

    def test_invalid_arguments_raise_value_error(self) -> None:
        self._commit_job()
        self._migrate()
        args = list(self._args9())
        for index in range(9):
            broken = list(args)
            broken[index] = ""
            with self.subTest(index=index):
                with self.assertRaises(ValueError):
                    settle(*broken, "j-1", "m1", "x1", 60)
                with self.assertRaises(ValueError):
                    current(*broken, "j-1")
        for value in ("", 123):
            with self.assertRaises(ValueError):
                settle(*args, value, "m1", "x1", 60)  # type: ignore
            with self.assertRaises(ValueError):
                settle(*args, "j-1", value, "x1", 60)  # type: ignore
            with self.assertRaises(ValueError):
                settle(*args, "j-1", "m1", value, 60)  # type: ignore
        with self.assertRaises(ValueError):
            settle(*args, "j-1", "m1", "x1", True)  # type: ignore
        with self.assertRaises(ValueError):
            settle(*args, "j-1", "m1", "x1", -1)
        with self.assertRaises(ValueError):
            current(*args, "")

    def test_paths_must_be_distinct(self) -> None:
        self._commit_job()
        self._migrate()
        args = list(self._args9())
        for index in range(9):
            broken = list(args)
            broken[index] = args[(index + 1) % 9]
            with self.subTest(index=index):
                with self.assertRaises(ValueError):
                    settle(*broken, "j-1", "m1", "x1", 60)
                with self.assertRaises(ValueError):
                    current(*broken, "j-1")

    def test_missing_inputs_raise_file_not_found(self) -> None:
        self._commit_job()
        self._migrate()
        names = ("no-jobs.json", "no-supply.json", "no-signals.json",
                 "no-trades.json", "no-dispatch.json", "no-execution.json",
                 "no-advice.json", "no-intents.json")
        for index, name in enumerate(names):
            broken = list(self._args9())
            broken[index] = os.path.join(self.tmp.name, name)
            with self.subTest(name=name):
                with self.assertRaises(FileNotFoundError):
                    settle(*broken, "j-1", "m1", "x1", 60)
                with self.assertRaises(FileNotFoundError):
                    current(*broken, "j-1")

    def test_missing_ledger_parent_raises_file_not_found(self) -> None:
        self._commit_job()
        self._migrate()
        missing = os.path.join(self.tmp.name, "no-dir", "settle.json")
        args = self._args8() + (missing,)
        with self.assertRaises(FileNotFoundError):
            settle(*args, "j-1", "m1", "x1", 60)

    def test_non_canonical_ledger_raises_value_error(self) -> None:
        self._commit_job()
        self._migrate()
        self._settle()
        good = Path(self.ledger).read_bytes()
        Path(self.ledger).write_bytes(good + b"\n")
        with self.assertRaises(ValueError):
            current(*self._args9(), "j-1")
        with self.assertRaises(ValueError):
            self._settle(key="x2", at=70)
        Path(self.ledger).write_bytes(good)

    def test_no_temp_fragments_after_guard_failures(self) -> None:
        self._commit_job()
        self._start()
        with self.assertRaises(PermissionError):
            self._settle()
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    # -- record fix: receipt monotonicity --------------------------------------

    def test_record_before_plan_start_raises_value_error(self) -> None:
        self._commit_job()
        self._start()
        with self.assertRaises(ValueError):
            self._copy(at=44)
        # The lease boundary still governs TimeoutError.
        with self.assertRaises(TimeoutError):
            self._copy(at=91)

    def test_record_before_previous_receipt_raises_value_error(self) -> None:
        self._commit_job()
        self._start()
        self._copy(at=55)
        with self.assertRaises(ValueError):
            self._switch(at=54, key="m4")
        # The same moment as the previous receipt is allowed.
        plan = self._switch(at=55, key="m5")
        self.assertEqual(plan["state"], "migrated")


if __name__ == "__main__":
    unittest.main()
