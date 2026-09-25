from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from carbon_market.dispatch import claim, commit, finish
from carbon_market.dispatch import recover as dispatch_recover
from carbon_market.execution import plan, record, recover
from carbon_market.execution_sync import run
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


def _dispatch_key(batch: str, job_id: str, attempt: int) -> str:
    return json.dumps([batch, job_id, attempt],
                      separators=(",", ":"))


class ExecutionSyncTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs = os.path.join(self.tmp.name, "jobs.json")
        self.supply = os.path.join(self.tmp.name, "supply.json")
        self.trades = os.path.join(self.tmp.name, "clear.json")
        self.dispatch = os.path.join(self.tmp.name, "dispatch.json")
        self.execution = os.path.join(self.tmp.name, "execution.json")
        self.ledger = os.path.join(self.tmp.name, "sync.json")

    def _seed(self, *job_ids: str) -> None:
        for index, job_id in enumerate(job_ids or ("j-1",), 1):
            submit(self.jobs, _job(job_id), f"jk-{index}")
        publish(self.supply, _resource(), "rk-1")

    def _claimed(self, job_id: str = "j-1", owner: str = "worker-1",
                 lease: int = 10, at: int = 60,
                 key_prefix: str = "") -> None:
        clear(self.jobs, self.supply, self.trades, job_id,
              f"{key_prefix}tk-{job_id}", 40)
        commit(self.jobs, self.supply, self.trades, self.dispatch,
               job_id, f"{key_prefix}ck-{job_id}", 50)
        claim(self.dispatch, job_id, f"{key_prefix}lk-{job_id}",
              owner, lease, at)

    def _plan(self, job_id: str = "j-1", owner: str = "worker-1",
              at: int = 65, key_prefix: str = "") -> None:
        plan(self.jobs, self.supply, self.trades, self.dispatch,
             self.execution, job_id, f"{key_prefix}pk-{job_id}",
             owner, None, at)

    def _completed(self, job_id: str = "j-1", owner: str = "worker-1",
                   attempt: int = 1, key_prefix: str = "") -> None:
        self._claimed(job_id, owner, key_prefix=key_prefix)
        self._plan(job_id, owner, key_prefix=key_prefix)
        record(self.execution, job_id, attempt,
               f"{key_prefix}rk-a-{job_id}", owner, "stage",
               "succeeded", "rcpt-1", 66)
        record(self.execution, job_id, attempt,
               f"{key_prefix}rk-b-{job_id}", owner, "start",
               "succeeded", "rcpt-2", 67)

    def _run(self, owner: str = "sync-1", key: str = "batch-1",
             at: int = 70, lease: int = 30
             ) -> tuple[dict[str, object], bool]:
        return run(self.dispatch, self.execution, self.ledger,
                   owner, key, at, lease)

    # -- argument validation -------------------------------------------

    def test_invalid_arguments_raise_before_any_read(self) -> None:
        self._seed()
        for kwargs in (
            {"owner": ""}, {"key": ""}, {"at": -1}, {"at": True},
            {"lease": 0}, {"lease": True},
        ):
            with self.subTest(**kwargs):
                arguments = {"owner": "sync-1", "key": "batch-1",
                             "at": 70, "lease": 30}
                arguments.update(kwargs)
                with self.assertRaises(ValueError):
                    run(self.dispatch, self.execution, self.ledger,
                        **arguments)
        with self.assertRaises(ValueError):
            run("", self.execution, self.ledger, "sync-1", "batch-1",
                70, 30)
        with self.assertRaises(ValueError):
            run(self.dispatch, self.dispatch, self.ledger, "sync-1",
                "batch-1", 70, 30)
        self.assertFalse(os.path.exists(self.ledger))

    def test_missing_inputs_raise_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            self._run()
        self._seed()
        self._claimed()
        self._plan()
        with self.assertRaises(FileNotFoundError):
            run(self.dispatch, self.execution,
                os.path.join(self.tmp.name, "missing", "sync.json"),
                "sync-1", "batch-1", 70, 30)

    # -- first scan ------------------------------------------------------

    def test_completed_plan_finishes_dispatch_as_succeeded(self) -> None:
        self._seed()
        self._completed()
        batch, created = self._run()
        self.assertTrue(created)
        self.assertEqual(batch["state"], "completed")
        self.assertEqual(list(batch.keys()),
                         ["key", "owner", "lease_end", "state", "items"])
        self.assertEqual(batch["owner"], "sync-1")
        self.assertEqual(batch["lease_end"], 100)
        self.assertEqual(len(batch["items"]), 1)
        item = batch["items"][0]
        self.assertEqual(list(item.keys()),
                         ["job_id", "attempt", "action", "result",
                          "owner", "lease_end", "at", "key", "state",
                          "decision", "error"])
        self.assertEqual(item["job_id"], "j-1")
        self.assertEqual(item["attempt"], 1)
        self.assertEqual(item["action"], "finish")
        self.assertEqual(item["result"], "succeeded")
        self.assertEqual(item["owner"], "worker-1")
        self.assertEqual(item["lease_end"], 70)
        # The action moment is the last step receipt's original value.
        self.assertEqual(item["at"], 67)
        self.assertEqual(item["key"], _dispatch_key("batch-1", "j-1", 1))
        self.assertEqual(item["state"], "applied")
        self.assertIsNone(item["error"])
        decision = item["decision"]
        self.assertEqual(decision["state"], "succeeded")
        self.assertEqual(decision["attempts"], 1)
        self.assertIsNone(decision["owner"])
        self.assertIsNone(decision["lease_end"])

        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()), ["version", "batches", "audit"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["batches"].keys()), ["batch-1"])
        self.assertEqual(data["batches"]["batch-1"], batch)
        self.assertEqual([event["key"] for event in data["audit"]],
                         ["batch-1", "batch-1"])
        self.assertEqual(data["audit"][-1]["result"], batch)
        self.assertEqual(data["audit"][-1]["request"],
                         {"owner": "sync-1", "at": 70, "lease": 30})

    def test_failed_plan_finishes_dispatch_as_failed(self) -> None:
        self._seed()
        self._claimed()
        self._plan()
        record(self.execution, "j-1", 1, "rk-a", "worker-1", "stage",
               "failed", "rcpt-1", 66)
        batch, created = self._run()
        self.assertTrue(created)
        item = batch["items"][0]
        self.assertEqual(item["action"], "finish")
        self.assertEqual(item["result"], "failed")
        self.assertEqual(item["at"], 66)
        self.assertEqual(item["decision"]["state"], "failed")

    def test_interrupted_plan_recovers_expired_dispatch_lease(self) -> None:
        self._seed()
        self._claimed()
        self._plan()
        record(self.execution, "j-1", 1, "rk-a", "worker-1", "stage",
               "succeeded", "rcpt-1", 66)
        recover(self.execution, "j-1", 1, "xk-1", 75)
        batch, created = self._run(at=80)
        self.assertTrue(created)
        item = batch["items"][0]
        self.assertEqual(item["action"], "recover")
        self.assertIsNone(item["result"])
        # The action moment is the interruption audit's original value.
        self.assertEqual(item["at"], 75)
        self.assertEqual(item["state"], "applied")
        self.assertEqual(item["decision"]["state"], "ready")
        self.assertEqual(item["decision"]["attempts"], 1)

    def test_active_plans_are_not_collected(self) -> None:
        self._seed()
        self._claimed()
        self._plan()
        batch, created = self._run(at=66)
        self.assertTrue(created)
        self.assertEqual(batch["state"], "completed")
        self.assertEqual(batch["items"], [])
        data = json.loads(Path(self.ledger).read_bytes())
        self.assertEqual(len(data["audit"]), 1)
        self.assertEqual(data["audit"][0]["result"], batch)

    def test_items_are_sorted_by_job_id_and_attempt(self) -> None:
        self._seed("j-1", "j-2")
        self._completed("j-2", key_prefix="b-")
        self._completed("j-1", key_prefix="a-")
        batch, _ = self._run()
        self.assertEqual(
            [(item["job_id"], item["attempt"]) for item in batch["items"]],
            [("j-1", 1), ("j-2", 1)])

    # -- replay and continuation -----------------------------------------

    def test_completed_batch_replays_without_write(self) -> None:
        self._seed()
        self._completed()
        batch, _ = self._run()
        raw = Path(self.ledger).read_bytes()
        replayed, created = self._run(at=80, lease=5, owner="other")
        self.assertFalse(created)
        self.assertEqual(replayed, batch)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)

    def test_later_terminal_plans_need_a_new_batch_key(self) -> None:
        self._seed()
        self._claimed()
        self._plan()
        record(self.execution, "j-1", 1, "rk-a", "worker-1", "stage",
               "failed", "rcpt-1", 66)
        first, _ = self._run()
        self.assertEqual(first["items"][0]["decision"]["state"], "failed")
        # A second dispatch attempt reaches a terminal plan afterwards.
        claim(self.dispatch, "j-1", "lk-2", "worker-2", 10, 71)
        plan(self.jobs, self.supply, self.trades, self.dispatch,
             self.execution, "j-1", "pk-2", "worker-2", None, 72)
        record(self.execution, "j-1", 2, "rk-c", "worker-2", "stage",
               "succeeded", "rcpt-3", 73)
        record(self.execution, "j-1", 2, "rk-d", "worker-2", "start",
               "succeeded", "rcpt-4", 74)
        # The old batch key does not pick the new plan up.
        replayed, created = self._run(at=80)
        self.assertFalse(created)
        self.assertEqual(replayed, first)
        second, created = self._run(key="batch-2", at=80)
        self.assertTrue(created)
        self.assertEqual(
            [(item["job_id"], item["attempt"]) for item in second["items"]],
            [("j-1", 2)])
        self.assertEqual(second["items"][0]["decision"]["state"],
                         "succeeded")

    def test_crash_reentry_replays_the_same_dispatch_request(self) -> None:
        self._seed()
        self._completed()
        import carbon_market.execution_sync as execution_sync

        real_commit = execution_sync._commit_file
        calls = {"count": 0}

        def flaky(realpath: str, payload: bytes,
                  old_bytes: bytes | None) -> None:
            calls["count"] += 1
            if calls["count"] == 2:
                raise OSError("simulated crash")
            real_commit(realpath, payload, old_bytes)

        with mock.patch.object(execution_sync, "_commit_file", flaky):
            with self.assertRaises(OSError):
                self._run()
        data = json.loads(Path(self.ledger).read_bytes())
        item = data["batches"]["batch-1"]["items"][0]
        self.assertEqual(item["state"], "pending")
        # The dispatch request was committed before the crash.
        dispatch_data = json.loads(Path(self.dispatch).read_bytes())
        self.assertIn(_dispatch_key("batch-1", "j-1", 1),
                      dispatch_data["idempotency"])

        batch, created = self._run(at=71)
        self.assertFalse(created)
        self.assertEqual(batch["state"], "completed")
        self.assertEqual(batch["items"][0]["state"], "applied")
        self.assertEqual(batch["items"][0]["decision"]["state"],
                         "succeeded")
        # The dispatch ledger saw exactly one finish for the plan, and
        # the reentry replayed it instead of committing a new one.
        self.assertEqual(
            sum(1 for request in dispatch_data["idempotency"].values()
                if request["action"] == "finish"), 1)
        dispatch_data = json.loads(Path(self.dispatch).read_bytes())
        self.assertEqual(
            sum(1 for request in dispatch_data["idempotency"].values()
                if request["action"] == "finish"), 1)

    def test_active_batch_rejects_another_owner_before_expiry(self) -> None:
        self._seed()
        self._completed()
        with mock.patch("carbon_market.dispatch.finish",
                        side_effect=TimeoutError("boom")):
            with self.assertRaises(TimeoutError):
                self._run(lease=10)
        with self.assertRaises(PermissionError):
            self._run(owner="other", at=75)
        # The same owner may continue inside the lease.
        with mock.patch("carbon_market.dispatch.finish",
                        side_effect=TimeoutError("boom")):
            with self.assertRaises(TimeoutError):
                self._run(at=75)

    def test_expired_batch_allows_takeover(self) -> None:
        self._seed()
        self._completed()
        with mock.patch("carbon_market.dispatch.finish",
                        side_effect=TimeoutError("boom")):
            with self.assertRaises(TimeoutError):
                self._run(lease=10)
        batch, created = self._run(owner="other", at=81, lease=5)
        self.assertFalse(created)
        self.assertEqual(batch["owner"], "other")
        self.assertEqual(batch["lease_end"], 86)
        self.assertEqual(batch["state"], "completed")
        self.assertEqual(batch["items"][0]["state"], "applied")

    # -- failure handling --------------------------------------------------

    def test_dispatch_failure_records_error_and_reraises(self) -> None:
        self._seed()
        self._completed()
        # Pre-bind the item's dispatch key to a different request.
        finish(self.dispatch, "j-1", _dispatch_key("batch-1", "j-1", 1),
               "worker-1", "failed", 68)
        with self.assertRaises(ValueError):
            self._run()
        data = json.loads(Path(self.ledger).read_bytes())
        batch = data["batches"]["batch-1"]
        self.assertEqual(batch["state"], "active")
        item = batch["items"][0]
        self.assertEqual(item["state"], "pending")
        self.assertEqual(item["error"], "ValueError")
        self.assertIsNone(item["decision"])

    def test_mismatched_dispatch_state_raises_without_advancing(self) -> None:
        self._seed()
        self._completed()
        # The decision's lease expires and is recovered outside the sync.
        dispatch_recover(self.dispatch, "j-1", "dk-1", 71)
        with self.assertRaises(ValueError):
            self._run(at=72)
        data = json.loads(Path(self.ledger).read_bytes())
        item = data["batches"]["batch-1"]["items"][0]
        self.assertEqual(item["state"], "pending")
        self.assertIsNone(item["error"])

    # -- ledger validation -------------------------------------------------

    def test_non_canonical_ledger_is_rejected(self) -> None:
        self._seed()
        self._completed()
        self._run()
        raw = Path(self.ledger).read_bytes()
        Path(self.ledger).write_bytes(raw + b"\n")
        with self.assertRaises(ValueError):
            self._run(key="batch-2")

    def test_invalid_ledger_structure_is_rejected(self) -> None:
        self._seed()
        self._completed()
        self._run()
        data = json.loads(Path(self.ledger).read_bytes())
        data["batches"]["batch-1"]["state"] = "active"
        Path(self.ledger).write_text(json.dumps(data), encoding="utf-8")
        with self.assertRaises(ValueError):
            self._run(key="batch-2")


if __name__ == "__main__":
    unittest.main()
