from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market.dispatch import claim, commit, finish
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


def _resource(resource_id: str = "r-1", region: str = "eu-north",
              residency: list[str] | None = None,
              **overrides: object) -> dict[str, object]:
    resource: dict[str, object] = {
        "resource_id": resource_id,
        "region": region,
        "capacity": 100,
        "start": 0,
        "end": 500,
        "unit_cost": 3,
        "carbon_intensity": 7,
        "residency": residency if residency is not None else [region],
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

    def _seed(self, job: dict[str, object] | None = None,
              resource: dict[str, object] | None = None) -> None:
        submit(self.jobs, job if job is not None else _job(), "jk-1")
        publish(self.supply,
                resource if resource is not None else _resource(), "rk-1")

    def _trade(self, job_id: str = "j-1", key: str = "tk-1",
               at: int = 40) -> None:
        clear(self.jobs, self.supply, self.trades, job_id, key, at)

    def _commit(self, job_id: str = "j-1", key: str = "ck-1",
                at: int = 50) -> None:
        commit(self.jobs, self.supply, self.trades, self.dispatch,
               job_id, key, at)

    def _claim(self, job_id: str = "j-1", key: str = "lk-1",
               owner: str = "w-1", lease: int = 20, at: int = 60
               ) -> dict[str, object]:
        decision, _ = claim(self.dispatch, job_id, key, owner, lease, at)
        return decision

    def _seed_claimed(self) -> None:
        self._seed()
        self._trade()
        self._commit()
        self._claim()

    def _plan(self, job_id: str = "j-1", key: str = "pk-1",
              source: object = None, target: object = None,
              at: int = 61) -> tuple[dict[str, object], bool]:
        return plan(self.jobs, self.supply, self.trades, self.dispatch,
                    self.ledger, job_id, key, source, target, at)

    def _record(self, step: str = "stage", result: str = "succeeded",
                receipt: str = "ok-1", at: int = 62, *,
                job_id: str = "j-1", attempt: int = 1, key: str = "ek-1",
                owner: str = "w-1") -> tuple[dict[str, object], bool]:
        return record(self.ledger, job_id, attempt, key, owner, step,
                      result, receipt, at)

    # -- plan -----------------------------------------------------------

    def test_plan_launch_creates_ledger_and_running_plan(self) -> None:
        self._seed_claimed()
        planned, created = self._plan()
        self.assertTrue(created)
        self.assertEqual(planned, {
            "job_id": "j-1", "attempt": 1, "kind": "launch",
            "source": None,
            "target": {"resource_id": "r-1", "version": 1},
            "deadline": 100, "owner": "w-1", "lease_end": 80,
            "state": "running", "receipts": [],
        })
        self.assertEqual(list(planned.keys()),
                         ["job_id", "attempt", "kind", "source", "target",
                          "deadline", "owner", "lease_end", "state",
                          "receipts"])

        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()),
                         ["version", "plans", "idempotency", "audit"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(data["plans"], {"j-1": [planned]})
        self.assertEqual(data["idempotency"], {"pk-1": {
            "action": "plan", "job_id": "j-1", "attempt": 1,
            "source": None, "target": {"resource_id": "r-1", "version": 1},
            "at": 61,
        }})
        self.assertEqual(data["audit"], {"pk-1": {
            "key": "pk-1",
            "request": data["idempotency"]["pk-1"],
            "result": planned,
        }})

    def test_plan_migrate_freezes_source_and_target(self) -> None:
        self._seed(resource=_resource())
        publish(self.supply, _resource("r-2", "eu-north",
                                       residency=["eu-north"]), "rk-2")
        self._trade()
        self._commit()
        self._claim()
        planned, created = self._plan(
            source={"resource_id": "r-1", "version": 1},
            target={"resource_id": "r-2", "version": 1})
        self.assertTrue(created)
        self.assertEqual(planned["kind"], "migrate")
        self.assertEqual(planned["source"],
                         {"resource_id": "r-1", "version": 1})
        self.assertEqual(planned["target"],
                         {"resource_id": "r-2", "version": 1})
        self.assertEqual(planned["state"], "running")
        self.assertEqual(planned["receipts"], [])

    def test_plan_replay_returns_current_plan_without_write(self) -> None:
        self._seed_claimed()
        planned, _ = self._plan()
        raw = Path(self.ledger).read_bytes()
        replayed, created = self._plan()
        self.assertFalse(created)
        self.assertEqual(replayed, planned)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        # The replay returns the current plan, not the plan snapshot.
        self._record("stage", "succeeded", "ok", 62, key="ek-1")
        replayed, created = self._plan()
        self.assertFalse(created)
        self.assertEqual(replayed["state"], "running")
        self.assertEqual(len(replayed["receipts"]), 1)

    def test_plan_conflicts(self) -> None:
        self._seed(resource=_resource())
        publish(self.supply, _resource("r-2", "eu-north"), "rk-2")
        self._trade()
        self._commit()
        self._claim()
        self._plan(source={"resource_id": "r-1", "version": 1},
                   target={"resource_id": "r-2", "version": 1})
        raw = Path(self.ledger).read_bytes()
        # Same key, different moment.
        with self.assertRaises(ValueError):
            self._plan(at=62,
                       source={"resource_id": "r-1", "version": 1},
                       target={"resource_id": "r-2", "version": 1})
        # Same key, different target.
        with self.assertRaises(ValueError):
            self._plan(source={"resource_id": "r-1", "version": 1},
                       target={"resource_id": "r-1", "version": 1})
        # Same key, launch versus migrate.
        with self.assertRaises(ValueError):
            self._plan(source=None, target=None)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)

    def test_one_plan_per_dispatch_attempt(self) -> None:
        self._seed_claimed()
        self._plan()
        with self.assertRaises(ValueError):
            self._plan(key="pk-2")
        with self.assertRaises(ValueError):
            self._plan(key="pk-2", source=None, target=None)

    def test_plan_is_numbered_by_the_dispatch_attempt(self) -> None:
        self._seed_claimed()
        planned, _ = self._plan()
        self.assertEqual(planned["attempt"], 1)
        # The first attempt fails at the dispatch layer; a new claim
        # counts attempt 2 and freezes a second plan.
        finish(self.dispatch, "j-1", "fk-1", "w-1", "failed", 65)
        self._claim(key="lk-2", owner="w-2", lease=20, at=66)
        planned, created = self._plan(key="pk-2", at=67)
        self.assertTrue(created)
        self.assertEqual(planned["attempt"], 2)
        self.assertEqual(planned["owner"], "w-2")
        self.assertEqual(planned["lease_end"], 86)
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual([p["attempt"] for p in data["plans"]["j-1"]],
                         [1, 2])

    def test_plan_requires_a_claimed_decision(self) -> None:
        self._seed()
        self._trade()
        self._commit()
        # ready, never claimed
        with self.assertRaises(ValueError):
            self._plan()
        self.assertFalse(os.path.exists(self.ledger))
        self._claim()
        finish(self.dispatch, "j-1", "fk-1", "w-1", "succeeded", 65)
        with self.assertRaises(ValueError):
            self._plan()

    def test_plan_inside_lease_boundary(self) -> None:
        self._seed_claimed()
        # The lease ends at 80; the plan may be frozen exactly then.
        planned, created = self._plan(at=80)
        self.assertTrue(created)
        self.assertEqual(planned["lease_end"], 80)
        with self.assertRaises(TimeoutError):
            self._plan(key="pk-late", at=81)

    def test_plan_requires_job_decision_and_trade(self) -> None:
        self._seed()
        # No trade, no dispatch ledger at all: the missing input file is
        # a FileNotFoundError before any decision is consulted.
        with self.assertRaises(FileNotFoundError):
            self._plan()
        self.assertFalse(os.path.exists(self.ledger))
        self._trade()
        # Still no dispatch ledger.
        with self.assertRaises(FileNotFoundError):
            self._plan()
        self.assertFalse(os.path.exists(self.ledger))
        # A dispatch ledger holding another job's decision but none for
        # this job is a missing decision.
        submit(self.jobs, _job("j-2"), "jk-2")
        clear(self.jobs, self.supply, self.trades, "j-2", "tk-2", 40)
        commit(self.jobs, self.supply, self.trades, self.dispatch,
               "j-2", "ck-2", 50)
        with self.assertRaises(LookupError):
            self._plan()
        self._commit()
        self._claim()
        with self.assertRaises(KeyError):
            self._plan(job_id="j-unknown")

    def test_plan_source_validation(self) -> None:
        self._seed(resource=_resource())
        publish(self.supply, _resource("r-2", "us-west",
                                       residency=["us-west"]), "rk-2")
        publish(self.supply, _resource("r-3", "eu-north"), "rk-3")
        self._trade()
        self._commit()
        self._claim()
        claimed = {"resource_id": "r-1", "version": 1}
        # Source must be the claimed resource version.
        with self.assertRaises(ValueError):
            self._plan(key="p-a",
                       source={"resource_id": "r-3", "version": 1},
                       target={"resource_id": "r-1", "version": 1})
        # Source and target must differ.
        with self.assertRaises(ValueError):
            self._plan(key="p-b", source=dict(claimed), target=dict(claimed))
        # The target must satisfy residency.
        with self.assertRaises(ValueError):
            self._plan(key="p-c", source=dict(claimed),
                       target={"resource_id": "r-2", "version": 1})
        # An unpublished target version is unavailable.
        with self.assertRaises(LookupError):
            self._plan(key="p-d", source=dict(claimed),
                       target={"resource_id": "r-9", "version": 1})
        with self.assertRaises(LookupError):
            self._plan(key="p-e", source=dict(claimed),
                       target={"resource_id": "r-1", "version": 9})

    def test_plan_target_region_must_be_permitted(self) -> None:
        self._seed(resource=_resource())
        publish(self.supply, _resource("r-2", "asia",
                                       residency=["asia"]), "rk-2")
        self._trade()
        self._commit()
        self._claim()
        with self.assertRaises(ValueError):
            self._plan(key="p",
                       source={"resource_id": "r-1", "version": 1},
                       target={"resource_id": "r-2", "version": 1})

    def test_plan_launch_rejects_explicit_target(self) -> None:
        self._seed_claimed()
        with self.assertRaises(ValueError):
            self._plan(target={"resource_id": "r-1", "version": 1})
        with self.assertRaises(ValueError):
            self._plan(source={"resource_id": "r-1", "version": 1})

    def test_plan_argument_validation(self) -> None:
        self._seed_claimed()
        for bad in ("", None, 1):
            with self.assertRaises(ValueError):
                plan(bad, self.supply, self.trades, self.dispatch,
                     self.ledger, "j-1", "k", None, None, 1)
        with self.assertRaises(ValueError):
            self._plan(at=-1)
        with self.assertRaises(ValueError):
            self._plan(at=True)
        with self.assertRaises(ValueError):
            self._plan(source={"resource_id": "r-1"})
        with self.assertRaises(ValueError):
            self._plan(source={"resource_id": "", "version": 1},
                       target={"resource_id": "r-2", "version": 1})
        with self.assertRaises(ValueError):
            self._plan(source={"resource_id": "r-1", "version": 0},
                       target={"resource_id": "r-2", "version": 1})
        with self.assertRaises(ValueError):
            plan(self.jobs, self.supply, self.trades, self.dispatch,
                 self.dispatch, "j-1", "k", None, None, 1)

    def test_plan_missing_inputs(self) -> None:
        self._seed_claimed()
        missing = os.path.join(self.tmp.name, "missing.json")
        with self.assertRaises(FileNotFoundError):
            plan(missing, self.supply, self.trades, self.dispatch,
                 self.ledger, "j-1", "k", None, None, 1)
        with self.assertRaises(FileNotFoundError):
            plan(self.jobs, missing, self.trades, self.dispatch,
                 self.ledger, "j-1", "k", None, None, 1)
        with self.assertRaises(FileNotFoundError):
            plan(self.jobs, self.supply, missing, self.dispatch,
                 self.ledger, "j-1", "k", None, None, 1)
        with self.assertRaises(FileNotFoundError):
            plan(self.jobs, self.supply, self.trades, missing,
                 self.ledger, "j-1", "k", None, None, 1)
        with self.assertRaises(FileNotFoundError):
            plan(self.jobs, self.supply, self.trades, self.dispatch,
                 os.path.join(self.tmp.name, "no-dir", "e.json"),
                 "j-1", "k", None, None, 1)

    # -- record ---------------------------------------------------------

    def test_record_launch_steps_complete_in_order(self) -> None:
        self._seed_claimed()
        self._plan()
        first, created = self._record("stage", "succeeded", "staged", 62)
        self.assertTrue(created)
        self.assertEqual(first["state"], "running")
        self.assertEqual(first["receipts"], [
            {"step": "stage", "result": "succeeded", "receipt": "staged",
             "at": 62},
        ])
        second, created = self._record("start", "succeeded", "started", 63,
                                       key="ek-2")
        self.assertTrue(created)
        self.assertEqual(second["state"], "completed")
        self.assertEqual([r["step"] for r in second["receipts"]],
                         ["stage", "start"])

    def test_record_migrate_steps(self) -> None:
        self._seed(resource=_resource())
        publish(self.supply, _resource("r-2", "eu-north"), "rk-2")
        self._trade()
        self._commit()
        self._claim()
        self._plan(source={"resource_id": "r-1", "version": 1},
                   target={"resource_id": "r-2", "version": 1})
        first, _ = self._record("copy", "succeeded", "copied", 62)
        self.assertEqual(first["state"], "running")
        second, _ = self._record("switch", "succeeded", "switched", 63,
                                 key="ek-2")
        self.assertEqual(second["state"], "completed")

    def test_record_step_order_is_strict(self) -> None:
        self._seed(resource=_resource())
        publish(self.supply, _resource("r-2", "eu-north"), "rk-2")
        self._trade()
        self._commit()
        self._claim()
        self._plan(source={"resource_id": "r-1", "version": 1},
                   target={"resource_id": "r-2", "version": 1})
        before = Path(self.ledger).read_bytes()
        # launch steps do not belong to a migrate
        with self.assertRaises(ValueError):
            self._record("stage", "succeeded", "x", 62)
        # cannot skip copy
        with self.assertRaises(ValueError):
            self._record("switch", "succeeded", "x", 62)
        self.assertEqual(Path(self.ledger).read_bytes(), before)
        # copy cannot repeat and stage still does not belong
        self._record("copy", "succeeded", "copied", 62)
        after_copy = Path(self.ledger).read_bytes()
        with self.assertRaises(ValueError):
            self._record("copy", "succeeded", "again", 63, key="ek-x")
        with self.assertRaises(ValueError):
            self._record("stage", "succeeded", "x", 63, key="ek-y")
        self.assertEqual(Path(self.ledger).read_bytes(), after_copy)

    def test_record_failed_step_ends_attempt_failed(self) -> None:
        self._seed_claimed()
        self._plan()
        failed, created = self._record("stage", "failed", "boom", 62)
        self.assertTrue(created)
        self.assertEqual(failed["state"], "failed")
        self.assertEqual(len(failed["receipts"]), 1)
        # A terminal attempt accepts no further receipts, even start.
        with self.assertRaises(ValueError):
            self._record("start", "succeeded", "x", 63, key="ek-2")
        with self.assertRaises(ValueError):
            self._record("stage", "succeeded", "x", 63, key="ek-3")

    def test_record_failed_final_step_is_failed_not_completed(self) -> None:
        self._seed_claimed()
        self._plan()
        self._record("stage", "succeeded", "ok", 62)
        failed, _ = self._record("start", "failed", "boom", 63,
                                 key="ek-2")
        self.assertEqual(failed["state"], "failed")
        self.assertEqual(len(failed["receipts"]), 2)

    def test_record_requires_owner_within_lease(self) -> None:
        self._seed_claimed()
        self._plan()
        with self.assertRaises(PermissionError):
            self._record("stage", "succeeded", "x", 62, owner="w-2")
        with self.assertRaises(TimeoutError):
            self._record("stage", "succeeded", "x", 81)
        recorded, created = self._record("stage", "succeeded", "x", 80)
        self.assertTrue(created)
        self.assertEqual(recorded["state"], "running")

    def test_record_arguments_validated(self) -> None:
        self._seed_claimed()
        self._plan()
        with self.assertRaises(ValueError):
            record(self.ledger, "j-1", 0, "k", "w-1", "stage",
                   "succeeded", "x", 62)
        with self.assertRaises(ValueError):
            record(self.ledger, "j-1", True, "k", "w-1", "stage",
                   "succeeded", "x", 62)
        with self.assertRaises(ValueError):
            record(self.ledger, "j-1", 1, "k", "w-1", "stage",
                   "done", "x", 62)
        with self.assertRaises(ValueError):
            record(self.ledger, "j-1", 1, "k", "w-1", "stage",
                   "succeeded", "", 62)
        with self.assertRaises(ValueError):
            record(self.ledger, "j-1", 1, "k", "w-1", "",
                   "succeeded", "x", 62)
        with self.assertRaises(ValueError):
            record(self.ledger, "j-1", 1, "k", "w-1", "stage",
                   "succeeded", "x", -1)

    def test_record_unknown_job_or_attempt_raises_key_error(self) -> None:
        self._seed_claimed()
        self._plan()
        with self.assertRaises(KeyError):
            self._record(job_id="j-x")
        with self.assertRaises(KeyError):
            self._record(attempt=2)

    def test_record_replay_and_conflicts(self) -> None:
        self._seed_claimed()
        self._plan()
        self._record("stage", "succeeded", "ok", 62)
        raw = Path(self.ledger).read_bytes()
        replayed, created = self._record("stage", "succeeded", "ok", 62)
        self.assertFalse(created)
        self.assertEqual(len(replayed["receipts"]), 1)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        with self.assertRaises(ValueError):
            self._record("stage", "failed", "ok", 62)
        with self.assertRaises(ValueError):
            self._record("stage", "succeeded", "other", 62)
        with self.assertRaises(ValueError):
            self._record("stage", "succeeded", "ok", 63)
        with self.assertRaises(ValueError):
            self._record("start", "succeeded", "ok", 62, owner="w-1")

    def test_record_requires_existing_ledger(self) -> None:
        with self.assertRaises(FileNotFoundError):
            record(os.path.join(self.tmp.name, "nope.json"),
                   "j-1", 1, "k", "w", "stage", "succeeded", "x", 1)

    # -- recover --------------------------------------------------------

    def test_recover_interrupts_after_strict_lease_expiry(self) -> None:
        self._seed_claimed()
        self._plan()
        self._record("stage", "succeeded", "ok", 62)
        with self.assertRaises(PermissionError):
            recover(self.ledger, "j-1", 1, "rk", 80)
        interrupted, created = recover(self.ledger, "j-1", 1, "rk", 81)
        self.assertTrue(created)
        self.assertEqual(interrupted["state"], "interrupted")
        self.assertEqual(len(interrupted["receipts"]), 1)
        self.assertEqual(interrupted["receipts"][0]["receipt"], "ok")

    def test_recover_preserves_receipts_and_blocks_further_records(self) -> None:
        self._seed_claimed()
        self._plan()
        self._record("stage", "succeeded", "ok", 62)
        interrupted, _ = recover(self.ledger, "j-1", 1, "rk", 81)
        self.assertEqual([r["step"] for r in interrupted["receipts"]],
                         ["stage"])
        with self.assertRaises(ValueError):
            self._record("start", "succeeded", "x", 82, key="ek-2")

    def test_recover_requires_running_plan(self) -> None:
        self._seed_claimed()
        self._plan()
        self._record("stage", "failed", "boom", 62)
        with self.assertRaises(ValueError):
            recover(self.ledger, "j-1", 1, "rk", 81)

    def test_recover_completed_plan_rejected(self) -> None:
        self._seed_claimed()
        self._plan()
        self._record("stage", "succeeded", "a", 62)
        self._record("start", "succeeded", "b", 63, key="ek-2")
        with self.assertRaises(ValueError):
            recover(self.ledger, "j-1", 1, "rk", 81)

    def test_recover_replay_and_conflicts(self) -> None:
        self._seed_claimed()
        self._plan()
        recover(self.ledger, "j-1", 1, "rk", 81)
        raw = Path(self.ledger).read_bytes()
        replayed, created = recover(self.ledger, "j-1", 1, "rk", 81)
        self.assertFalse(created)
        self.assertEqual(replayed["state"], "interrupted")
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        with self.assertRaises(ValueError):
            recover(self.ledger, "j-1", 1, "rk", 82)
        with self.assertRaises(ValueError):
            recover(self.ledger, "j-1", 2, "rk", 81)

    def test_recover_unknown_job_or_attempt_raises_key_error(self) -> None:
        self._seed_claimed()
        self._plan()
        with self.assertRaises(KeyError):
            recover(self.ledger, "j-x", 1, "rk", 81)
        with self.assertRaises(KeyError):
            recover(self.ledger, "j-1", 2, "rk", 81)

    def test_recover_requires_existing_ledger(self) -> None:
        with self.assertRaises(FileNotFoundError):
            recover(os.path.join(self.tmp.name, "nope.json"),
                    "j-1", 1, "k", 1)

    # -- shared ledger properties --------------------------------------

    def test_key_namespace_is_shared_across_entries(self) -> None:
        self._seed_claimed()
        self._plan()
        self._record("stage", "succeeded", "ok", 62)
        with self.assertRaises(ValueError):
            self._plan(key="ek-1")
        with self.assertRaises(ValueError):
            recover(self.ledger, "j-1", 1, "ek-1", 81)
        with self.assertRaises(ValueError):
            self._record("start", "succeeded", "x", 63, key="pk-1")

    def test_audit_trail_carries_full_request_and_snapshot(self) -> None:
        self._seed_claimed()
        self._plan()
        self._record("stage", "succeeded", "ok", 62)
        recover(self.ledger, "j-1", 1, "rk", 81)
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(list(data["idempotency"].keys()),
                         ["ek-1", "pk-1", "rk"])
        self.assertEqual(
            data["audit"]["ek-1"]["request"],
            {"action": "record", "job_id": "j-1", "attempt": 1,
             "owner": "w-1", "step": "stage", "result": "succeeded",
             "receipt": "ok", "at": 62})
        self.assertEqual(data["audit"]["ek-1"]["result"]["state"],
                         "running")
        self.assertEqual(data["audit"]["rk"]["result"]["state"],
                         "interrupted")
        self.assertEqual(data["audit"]["rk"]["result"]["receipts"],
                         data["plans"]["j-1"][0]["receipts"])

    def test_sorted_sections_and_non_ascii(self) -> None:
        submit(self.jobs, _job("j-b"), "jk-b")
        submit(self.jobs, _job("j-a"), "jk-a")
        publish(self.supply, _resource("r-中"), "rk")
        clear(self.jobs, self.supply, self.trades, "j-b", "tk-b", 40)
        clear(self.jobs, self.supply, self.trades, "j-a", "tk-a", 40)
        commit(self.jobs, self.supply, self.trades, self.dispatch,
               "j-b", "ck-b", 50)
        commit(self.jobs, self.supply, self.trades, self.dispatch,
               "j-a", "ck-a", 50)
        claim(self.dispatch, "j-b", "lk-b", "w", 20, 60)
        claim(self.dispatch, "j-a", "lk-a", "w", 20, 60)
        plan(self.jobs, self.supply, self.trades, self.dispatch,
             self.ledger, "j-b", "键-b", None, None, 61)
        plan(self.jobs, self.supply, self.trades, self.dispatch,
             self.ledger, "j-a", "键-a", None, None, 61)
        raw = Path(self.ledger).read_text(encoding="utf-8")
        self.assertIn("r-中", raw)
        self.assertIn("键-b", raw)
        self.assertNotIn(" ", raw.split("\n", 1)[0])
        data = json.loads(raw)
        self.assertEqual(list(data["plans"].keys()), ["j-a", "j-b"])
        self.assertEqual(list(data["idempotency"].keys()),
                         ["键-a", "键-b"])

    def test_invalid_ledger_bytes_raise_value_error(self) -> None:
        self._seed_claimed()
        self._plan()
        raw = Path(self.ledger).read_bytes()
        for bad in (raw + b"\n", raw[:-1],
                    raw.replace(b'"running"', b'"Running"', 1),
                    b"not json", json.dumps({"version": 2}).encode()):
            Path(self.ledger).write_bytes(bad)
            with self.assertRaises(ValueError):
                self._record("stage", "succeeded", "x", 62, key="k-x")
            with self.assertRaises(ValueError):
                recover(self.ledger, "j-1", 1, "k-r", 81)
        Path(self.ledger).write_bytes(raw)
        recorded, created = self._record("stage", "succeeded", "x", 62)
        self.assertTrue(created)
        self.assertEqual(recorded["state"], "running")

    def test_failed_call_leaves_ledger_untouched(self) -> None:
        self._seed(resource=_resource())
        publish(self.supply, _resource("r-2", "eu-north"), "rk-2")
        self._trade()
        self._commit()
        self._claim()
        self._plan(source={"resource_id": "r-1", "version": 1},
                   target={"resource_id": "r-2", "version": 1})
        raw = Path(self.ledger).read_bytes()
        for call in (
            lambda: self._plan(key="pk-2",
                               source={"resource_id": "r-1", "version": 1},
                               target={"resource_id": "r-2", "version": 1}),
            lambda: self._record("switch", "succeeded", "x", 62),
            lambda: self._record("copy", "succeeded", "", 62, key="k2"),
            lambda: self._record("copy", "nope", "x", 62, key="k3"),
            lambda: self._record("copy", "succeeded", "x", 62,
                                 owner="w-2", key="k4"),
            lambda: self._record("copy", "succeeded", "x", 81, key="k5"),
            lambda: recover(self.ledger, "j-1", 1, "k6", 80),
            lambda: recover(self.ledger, "j-x", 1, "k7", 81),
        ):
            with self.assertRaises((ValueError, KeyError, TimeoutError,
                                    PermissionError)):
                call()
            self.assertEqual(Path(self.ledger).read_bytes(), raw)

    def test_concurrent_records_serialize_to_one_first_step(self) -> None:
        self._seed_claimed()
        self._plan()
        results: list[object] = []

        def worker(index: int) -> None:
            try:
                results.append(record(
                    self.ledger, "j-1", 1, f"ek-{index}", "w-1",
                    "stage", "succeeded", f"receipt-{index}", 62))
            except (ValueError, TimeoutError, PermissionError) as exc:
                results.append(exc)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        winners = [r for r in results if isinstance(r, tuple)]
        self.assertEqual(len(winners), 1)
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(len(data["plans"]["j-1"][0]["receipts"]), 1)


if __name__ == "__main__":
    unittest.main()
