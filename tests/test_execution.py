from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market.dispatch import claim, commit, finish
from carbon_market.dispatch import recover as dispatch_recover
from carbon_market.execution import plan, record, recover
from carbon_market.jobs import submit
from carbon_market.market import clear
from carbon_market.resources import publish


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


class ExecutionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs = os.path.join(self.tmp.name, "jobs.json")
        self.supply = os.path.join(self.tmp.name, "supply.json")
        self.trades = os.path.join(self.tmp.name, "clear.json")
        self.dispatch = os.path.join(self.tmp.name, "dispatch.json")
        self.ledger = os.path.join(self.tmp.name, "execution.json")

    def _seed(self, *resources: dict[str, object]) -> None:
        submit(self.jobs, _job(), "jk-1")
        for index, resource in enumerate(resources or [_resource()], 1):
            publish(self.supply, resource, f"rk-{index}")

    def _claimed(self, owner: str = "worker-1", lease: int = 10,
                 at: int = 60) -> None:
        self._seed()
        clear(self.jobs, self.supply, self.trades, "j-1", "tk-1", 40)
        commit(self.jobs, self.supply, self.trades, self.dispatch,
               "j-1", "ck-1", 50)
        claim(self.dispatch, "j-1", "lk-1", owner, lease, at)

    def _plan(self, key: str = "pk-1", owner: str = "worker-1",
              source: str | None = None, at: int = 65,
              job_id: str = "j-1") -> tuple[dict[str, object], bool]:
        return plan(self.jobs, self.supply, self.trades, self.dispatch,
                    self.ledger, job_id, key, owner, source, at)

    # -- plan ----------------------------------------------------------

    def test_plan_creates_ledger_and_launch_plan(self) -> None:
        self._claimed()
        created_plan, created = self._plan()
        self.assertTrue(created)
        self.assertEqual(created_plan, {
            "job_id": "j-1", "attempt": 1, "kind": "launch",
            "source": None, "resource_id": "r-1", "version": 1,
            "deadline": 100, "owner": "worker-1", "lease_end": 70,
            "state": "active", "steps": [],
        })
        self.assertEqual(list(created_plan.keys()),
                         ["job_id", "attempt", "kind", "source",
                          "resource_id", "version", "deadline", "owner",
                          "lease_end", "state", "steps"])

        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()),
                         ["version", "plans", "idempotency", "audit"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["plans"].keys()), ["j-1"])
        self.assertEqual(list(data["plans"]["j-1"].keys()), ["1"])
        self.assertEqual(data["idempotency"],
                         {"pk-1": {"action": "plan", "job_id": "j-1",
                                   "owner": "worker-1", "source": None,
                                   "at": 65}})
        self.assertEqual(data["audit"], {"pk-1": {
            "key": "pk-1",
            "request": {"action": "plan", "job_id": "j-1",
                        "owner": "worker-1", "source": None, "at": 65},
            "result": created_plan,
        }})

    def test_plan_replay_returns_current_plan_without_write(self) -> None:
        self._claimed()
        self._plan()
        raw = Path(self.ledger).read_bytes()
        replayed, created = self._plan()
        self.assertFalse(created)
        self.assertEqual(replayed["state"], "active")
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        # The replay returns the current plan, not the create snapshot.
        record(self.ledger, "j-1", 1, "rk-1", "worker-1", "stage",
               "succeeded", "staged", 66)
        replayed, created = self._plan()
        self.assertFalse(created)
        self.assertEqual(len(replayed["steps"]), 1)

    def test_plan_key_conflict_and_double_plan(self) -> None:
        self._claimed()
        self._plan()
        with self.assertRaises(ValueError):
            self._plan(at=66)
        with self.assertRaises(ValueError):
            self._plan(owner="worker-2")
        # One dispatch attempt yields at most one plan.
        with self.assertRaises(ValueError):
            self._plan(key="pk-2")

    def test_plan_requires_claimed_decision_owner_and_lease(self) -> None:
        self._seed()
        clear(self.jobs, self.supply, self.trades, "j-1", "tk-1", 40)
        commit(self.jobs, self.supply, self.trades, self.dispatch,
               "j-1", "ck-1", 50)
        # The decision is still ready, not claimed.
        with self.assertRaises(ValueError):
            self._plan()
        claim(self.dispatch, "j-1", "lk-1", "worker-1", 10, 60)
        with self.assertRaises(PermissionError):
            self._plan(owner="worker-2")
        # The lease ends at 70; planning later than that is too late.
        with self.assertRaises(TimeoutError):
            self._plan(at=71)
        created_plan, created = self._plan(at=70)
        self.assertTrue(created)
        self.assertEqual(created_plan["lease_end"], 70)

    def test_plan_unknown_job_and_missing_inputs(self) -> None:
        self._claimed()
        with self.assertRaises(KeyError):
            self._plan(job_id="j-2")
        with self.assertRaises(FileNotFoundError):
            plan(self.jobs, self.supply, self.trades, self.dispatch,
                 os.path.join(self.tmp.name, "none", "exec.json"),
                 "j-1", "pk-9", "worker-1", None, 65)
        with self.assertRaises(FileNotFoundError):
            plan(os.path.join(self.tmp.name, "no-jobs.json"),
                 self.supply, self.trades, self.dispatch, self.ledger,
                 "j-1", "pk-9", "worker-1", None, 65)

    def test_plan_argument_validation(self) -> None:
        self._claimed()
        with self.assertRaises(ValueError):
            self._plan(owner="")
        with self.assertRaises(ValueError):
            self._plan(at=-1)
        with self.assertRaises(ValueError):
            self._plan(at=True)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            plan(self.jobs, self.supply, self.trades, self.dispatch,
                 self.ledger, "j-1", "pk-9", "worker-1", 5, 65)
        # The five paths must be distinct real locations.
        with self.assertRaises(ValueError):
            plan(self.jobs, self.supply, self.trades, self.dispatch,
                 self.dispatch, "j-1", "pk-9", "worker-1", None, 65)

    def test_plan_migrate(self) -> None:
        self._claimed()
        publish(self.supply, _resource("r-2", region="us-west",
                                       residency=["eu-north", "us-west"]),
                "rk-2")
        created_plan, created = self._plan(source="r-2")
        self.assertTrue(created)
        self.assertEqual(created_plan["kind"], "migrate")
        self.assertEqual(created_plan["source"], "r-2")
        self.assertEqual(created_plan["resource_id"], "r-1")

    def test_plan_migrate_source_constraints(self) -> None:
        self._claimed()
        # An unpublished source is unavailable.
        with self.assertRaises(LookupError):
            self._plan(source="r-9")
        # The source must differ from the traded target.
        with self.assertRaises(LookupError):
            self._plan(source="r-1")
        # The source must cover the job's residency regions.
        publish(self.supply, _resource("r-3", region="us-west",
                                       residency=["us-west"]), "rk-3")
        with self.assertRaises(LookupError):
            self._plan(source="r-3")

    # -- record --------------------------------------------------------

    def test_record_launch_sequence_to_completed(self) -> None:
        self._claimed()
        self._plan()
        current, created = record(self.ledger, "j-1", 1, "rk-1",
                                  "worker-1", "stage", "succeeded",
                                  "暂存完成", 66)
        self.assertTrue(created)
        self.assertEqual(current["state"], "active")
        self.assertEqual(current["steps"], [
            {"step": "stage", "result": "succeeded",
             "receipt": "暂存完成", "at": 66},
        ])
        current, created = record(self.ledger, "j-1", 1, "rk-2",
                                  "worker-1", "start", "succeeded",
                                  "started", 67)
        self.assertTrue(created)
        self.assertEqual(current["state"], "completed")
        self.assertEqual(len(current["steps"]), 2)
        # A terminal plan accepts no further receipts.
        with self.assertRaises(ValueError):
            record(self.ledger, "j-1", 1, "rk-3", "worker-1", "start",
                   "succeeded", "again", 68)
        # Non-ASCII receipts are written through.
        raw = Path(self.ledger).read_bytes()
        self.assertIn("暂存完成".encode("utf-8"), raw)

    def test_record_migrate_sequence_and_failure(self) -> None:
        self._claimed()
        publish(self.supply, _resource("r-2", region="us-west",
                                       residency=["eu-north", "us-west"]),
                "rk-2")
        self._plan(source="r-2")
        with self.assertRaises(ValueError):
            # A migrate starts with copy, not start.
            record(self.ledger, "j-1", 1, "rk-x", "worker-1", "start",
                   "succeeded", "nope", 66)
        current, created = record(self.ledger, "j-1", 1, "rk-1",
                                  "worker-1", "copy", "failed",
                                  "copy broke", 66)
        self.assertTrue(created)
        self.assertEqual(current["state"], "failed")
        self.assertEqual(len(current["steps"]), 1)
        with self.assertRaises(ValueError):
            record(self.ledger, "j-1", 1, "rk-2", "worker-1", "switch",
                   "succeeded", "too late", 67)

    def test_record_step_order_and_validation(self) -> None:
        self._claimed()
        self._plan()
        with self.assertRaises(ValueError):
            # The first step of a launch is stage, not start.
            record(self.ledger, "j-1", 1, "rk-1", "worker-1", "start",
                   "succeeded", "skipped", 66)
        with self.assertRaises(ValueError):
            record(self.ledger, "j-1", 1, "rk-1", "worker-1", "stage",
                   "maybe", "bad result", 66)
        with self.assertRaises(ValueError):
            record(self.ledger, "j-1", 1, "rk-1", "worker-1", "stage",
                   "succeeded", "", 66)
        with self.assertRaises(ValueError):
            record(self.ledger, "j-1", 1, "rk-1", "worker-1", "stage",
                   "succeeded", "staged", -1)
        with self.assertRaises(ValueError):
            record(self.ledger, "j-1", 0, "rk-1", "worker-1", "stage",
                   "succeeded", "staged", 66)

    def test_record_owner_lease_and_lookup(self) -> None:
        self._claimed()
        self._plan()
        with self.assertRaises(PermissionError):
            record(self.ledger, "j-1", 1, "rk-1", "worker-2", "stage",
                   "succeeded", "staged", 66)
        with self.assertRaises(TimeoutError):
            record(self.ledger, "j-1", 1, "rk-1", "worker-1", "stage",
                   "succeeded", "staged", 71)
        with self.assertRaises(KeyError):
            record(self.ledger, "j-2", 1, "rk-1", "worker-1", "stage",
                   "succeeded", "staged", 66)
        with self.assertRaises(KeyError):
            record(self.ledger, "j-1", 2, "rk-1", "worker-1", "stage",
                   "succeeded", "staged", 66)
        with self.assertRaises(FileNotFoundError):
            record(os.path.join(self.tmp.name, "missing.json"),
                   "j-1", 1, "rk-1", "worker-1", "stage", "succeeded",
                   "staged", 66)

    def test_record_replay_and_key_conflict(self) -> None:
        self._claimed()
        self._plan()
        record(self.ledger, "j-1", 1, "rk-1", "worker-1", "stage",
               "succeeded", "staged", 66)
        raw = Path(self.ledger).read_bytes()
        replayed, created = record(self.ledger, "j-1", 1, "rk-1",
                                   "worker-1", "stage", "succeeded",
                                   "staged", 66)
        self.assertFalse(created)
        self.assertEqual(len(replayed["steps"]), 1)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        with self.assertRaises(ValueError):
            record(self.ledger, "j-1", 1, "rk-1", "worker-1", "stage",
                   "succeeded", "staged", 67)
        # The three entries share one idempotency key space.
        with self.assertRaises(ValueError):
            record(self.ledger, "j-1", 1, "pk-1", "worker-1", "start",
                   "succeeded", "started", 67)

    # -- recover -------------------------------------------------------

    def test_recover_interrupts_after_strict_expiry(self) -> None:
        self._claimed()
        self._plan()
        record(self.ledger, "j-1", 1, "rk-1", "worker-1", "stage",
               "succeeded", "staged", 66)
        # The lease ends at 70; recovery needs a strictly later moment.
        with self.assertRaises(PermissionError):
            recover(self.ledger, "j-1", 1, "xk-1", 70)
        current, created = recover(self.ledger, "j-1", 1, "xk-1", 71)
        self.assertTrue(created)
        self.assertEqual(current["state"], "interrupted")
        # The recorded receipts are preserved in full.
        self.assertEqual(current["steps"], [
            {"step": "stage", "result": "succeeded",
             "receipt": "staged", "at": 66},
        ])
        raw = Path(self.ledger).read_bytes()
        replayed, created = recover(self.ledger, "j-1", 1, "xk-1", 71)
        self.assertFalse(created)
        self.assertEqual(replayed["state"], "interrupted")
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        # An interrupted plan is terminal for both record and recover.
        with self.assertRaises(ValueError):
            recover(self.ledger, "j-1", 1, "xk-2", 72)
        with self.assertRaises(ValueError):
            record(self.ledger, "j-1", 1, "rk-2", "worker-1", "start",
                   "succeeded", "started", 72)

    def test_recover_validation_and_lookup(self) -> None:
        self._claimed()
        self._plan()
        with self.assertRaises(KeyError):
            recover(self.ledger, "j-2", 1, "xk-1", 71)
        with self.assertRaises(KeyError):
            recover(self.ledger, "j-1", 9, "xk-1", 71)
        with self.assertRaises(ValueError):
            recover(self.ledger, "j-1", 0, "xk-1", 71)
        with self.assertRaises(FileNotFoundError):
            recover(os.path.join(self.tmp.name, "missing.json"),
                    "j-1", 1, "xk-1", 71)

    # -- ledger lifecycle ----------------------------------------------

    def test_second_attempt_after_recovery_plans_again(self) -> None:
        self._claimed()
        self._plan()
        recover(self.ledger, "j-1", 1, "xk-1", 71)
        # The dispatch layer recovers the expired claim and a new claim
        # counts the second attempt, which accepts its own plan.
        dispatch_recover(self.dispatch, "j-1", "dk-1", 71)
        claim(self.dispatch, "j-1", "lk-2", "worker-2", 10, 72)
        created_plan, created = self._plan(key="pk-2", owner="worker-2",
                                           at=73)
        self.assertTrue(created)
        self.assertEqual(created_plan["attempt"], 2)
        self.assertEqual(created_plan["owner"], "worker-2")
        data = json.loads(Path(self.ledger).read_bytes())
        self.assertEqual(list(data["plans"]["j-1"].keys()), ["1", "2"])

    def test_failed_attempt_and_reclaim(self) -> None:
        self._claimed()
        self._plan()
        record(self.ledger, "j-1", 1, "rk-1", "worker-1", "stage",
               "failed", "stage broke", 66)
        finish(self.dispatch, "j-1", "fk-1", "worker-1", "failed", 67)
        claim(self.dispatch, "j-1", "lk-2", "worker-1", 10, 68)
        created_plan, created = self._plan(key="pk-2", at=69)
        self.assertTrue(created)
        self.assertEqual(created_plan["attempt"], 2)

    def test_canonical_form_rejected_when_altered(self) -> None:
        self._claimed()
        self._plan()
        raw = Path(self.ledger).read_bytes()
        # Extra trailing newline breaks the canonical form.
        Path(self.ledger).write_bytes(raw + b"\n")
        with self.assertRaises(ValueError):
            record(self.ledger, "j-1", 1, "rk-1", "worker-1", "stage",
                   "succeeded", "staged", 66)
        # A negative-zero literal is a format error.
        Path(self.ledger).write_bytes(raw.replace(b'"attempt":1',
                                                  b'"attempt":-0', 1))
        with self.assertRaises(ValueError):
            recover(self.ledger, "j-1", 1, "xk-1", 71)

    def test_audit_trail_covers_all_actions(self) -> None:
        self._claimed()
        self._plan()
        record(self.ledger, "j-1", 1, "rk-1", "worker-1", "stage",
               "succeeded", "staged", 66)
        recover(self.ledger, "j-1", 1, "xk-1", 71)
        data = json.loads(Path(self.ledger).read_bytes())
        self.assertEqual(list(data["audit"].keys()),
                         ["pk-1", "rk-1", "xk-1"])
        self.assertEqual(list(data["idempotency"].keys()),
                         ["pk-1", "rk-1", "xk-1"])
        actions = [event["request"]["action"]
                   for event in data["audit"].values()]
        self.assertEqual(actions, ["plan", "record", "recover"])
        # Every event carries the complete request and result snapshot.
        self.assertEqual(data["audit"]["xk-1"]["result"]["state"],
                         "interrupted")
        self.assertEqual(data["audit"]["rk-1"]["result"]["steps"], [
            {"step": "stage", "result": "succeeded",
             "receipt": "staged", "at": 66},
        ])


if __name__ == "__main__":
    unittest.main()
