from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import execution_sync
from carbon_market.dispatch import claim, commit
from carbon_market.dispatch import _load_ledger as load_dispatch
from carbon_market.execution import plan, record, recover as exec_recover
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


class ExecutionSyncTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        t = self.tmp.name
        self.jobs = os.path.join(t, "jobs.json")
        self.supply = os.path.join(t, "supply.json")
        self.trades = os.path.join(t, "clear.json")
        self.dispatch = os.path.join(t, "dispatch.json")
        self.execution = os.path.join(t, "execution.json")
        self.ledger = os.path.join(t, "sync.json")
        submit(self.jobs, _job(), "jk-1")
        publish(self.supply, _resource(), "rk-1")
        clear(self.jobs, self.supply, self.trades, "j-1", "tk-1", 40)
        commit(self.jobs, self.supply, self.trades, self.dispatch,
               "j-1", "ck-1", 50)
        claim(self.dispatch, "j-1", "lk-1", "worker-1", 30, 60)

    def _plan(self, owner: str = "worker-1", at: int = 61) -> None:
        plan(self.jobs, self.supply, self.trades, self.dispatch,
             self.execution, "j-1", "pk-1", owner, None, at)

    def _completed(self) -> None:
        self._plan()
        record(self.execution, "j-1", 1, "s-1", "worker-1", "stage",
               "succeeded", "暂存完成", 62)
        record(self.execution, "j-1", 1, "s-2", "worker-1", "start",
               "succeeded", "started", 63)

    def _failed(self) -> None:
        self._plan()
        record(self.execution, "j-1", 1, "s-1", "worker-1", "stage",
               "failed", "stage broke", 62)

    def _interrupted(self) -> None:
        self._plan()
        record(self.execution, "j-1", 1, "s-1", "worker-1", "stage",
               "succeeded", "staged", 62)
        # The claim lease ends at 90; recovery needs a strict later
        # moment.
        exec_recover(self.execution, "j-1", 1, "x-1", 91)

    def _run(self, owner: str = "sync-1", key: str = "bk-1",
             now: int = 70, lease: int = 50):
        return execution_sync.run(self.execution, self.dispatch, self.ledger,
                                  owner, key, now, lease)

    def _coord(self) -> dict[str, object]:
        return json.loads(Path(self.ledger).read_text(encoding="utf-8"))

    def test_multi_item_batch_is_ordered_and_resumes_mid_batch(self) -> None:
        # The first job completed; a second job failed.
        submit(self.jobs, _job("j-2"), "jk-2")
        clear(self.jobs, self.supply, self.trades, "j-2", "tk-2", 41)
        commit(self.jobs, self.supply, self.trades, self.dispatch,
               "j-2", "ck-2", 51)
        claim(self.dispatch, "j-2", "lk-2", "worker-2", 30, 60)
        plan(self.jobs, self.supply, self.trades, self.dispatch,
             self.execution, "j-2", "pk-2", "worker-2", None, 61)
        record(self.execution, "j-2", 1, "s-3", "worker-2", "stage",
               "failed", "broke", 62)
        self._completed()

        original_finish = execution_sync._dispatch.finish
        calls: list[str] = []

        def selective(ledger, job_id, dkey, owner, result, at):
            calls.append(job_id)
            if job_id == "j-1":
                raise RuntimeError("j-1 down")
            return original_finish(ledger, job_id, dkey, owner, result, at)

        execution_sync._dispatch.finish = selective
        try:
            with self.assertRaises(RuntimeError):
                self._run()
        finally:
            execution_sync._dispatch.finish = original_finish
        # Items process by job id; j-1 failed first and j-2 never ran.
        self.assertEqual(calls, ["j-1"])
        data = self._coord()
        items = data["batches"]["bk-1"]["items"]
        self.assertEqual(list(items), ["j-1\t1", "j-2\t1"])
        self.assertEqual(items["j-1\t1"]["status"], "pending")
        self.assertEqual(items["j-1\t1"]["error"], "RuntimeError")
        self.assertEqual(items["j-2\t1"]["status"], "pending")
        self.assertIsNone(items["j-2\t1"]["error"])

        batch, created = self._run()
        self.assertFalse(created)
        self.assertEqual(batch["status"], "completed")
        self.assertTrue(all(item["status"] == "applied"
                            for item in batch["items"].values()))

    # -- empty batch -----------------------------------------------------

    def test_empty_batch_completes_with_audit_snapshot(self) -> None:
        # An execution ledger whose only plan is still active yields an
        # empty first scan: nothing to sync yet.
        self._plan()
        record(self.execution, "j-1", 1, "s-1", "worker-1", "stage",
               "succeeded", "staged", 62)
        batch, created = self._run()
        self.assertTrue(created)
        self.assertEqual(list(batch.keys()),
                         ["key", "owner", "until", "status", "items"])
        self.assertEqual(batch["key"], "bk-1")
        self.assertEqual(batch["owner"], "sync-1")
        self.assertEqual(batch["until"], 120)
        self.assertEqual(batch["status"], "completed")
        self.assertEqual(batch["items"], {})
        data = self._coord()
        self.assertEqual(list(data.keys()),
                         ["version", "batches", "audit"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["batches"].keys()), ["bk-1"])
        self.assertEqual(data["audit"]["bk-1"]["batch"], batch)
        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))

    def test_empty_batch_replay_writes_nothing(self) -> None:
        self._plan()
        record(self.execution, "j-1", 1, "s-1", "worker-1", "stage",
               "succeeded", "staged", 62)
        self._run()
        raw = Path(self.ledger).read_bytes()
        batch, created = self._run(owner="sync-2", now=500, lease=3)
        self.assertFalse(created)
        self.assertEqual(batch["until"], 120)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)

    # -- terminal plan reconciliation ------------------------------------

    def test_completed_plan_finishes_decision_as_succeeded(self) -> None:
        self._completed()
        batch, created = self._run()
        self.assertTrue(created)
        self.assertEqual(batch["status"], "completed")
        self.assertEqual(list(batch["items"].keys()), ["j-1\t1"])
        item = batch["items"]["j-1\t1"]
        self.assertEqual(list(item.keys()),
                         ["job_id", "attempt", "state", "action", "at",
                          "status", "error", "decision"])
        self.assertEqual(item["state"], "completed")
        self.assertEqual(item["action"], "finish")
        # The action moment is the last receipt's historical value, not
        # the current `now`.
        self.assertEqual(item["at"], 63)
        self.assertEqual(item["status"], "applied")
        self.assertIsNone(item["error"])
        self.assertEqual(item["decision"]["state"], "succeeded")
        self.assertIsNone(item["decision"]["owner"])
        decision = load_dispatch(self.dispatch)[0]["j-1"]
        self.assertEqual(decision["state"], "succeeded")
        # The stable dispatch idempotency key was served exactly once.
        bindings = load_dispatch(self.dispatch)[1]
        self.assertIn("4:bk-1j-1/1", bindings)
        self.assertEqual(bindings["4:bk-1j-1/1"],
                         {"action": "finish", "job_id": "j-1",
                          "owner": "worker-1", "result": "succeeded",
                          "at": 63})

    def test_failed_plan_finishes_decision_as_failed(self) -> None:
        self._failed()
        batch, _ = self._run()
        item = batch["items"]["j-1\t1"]
        self.assertEqual(item["state"], "failed")
        self.assertEqual(item["decision"]["state"], "failed")
        self.assertEqual(load_dispatch(self.dispatch)[0]["j-1"]["state"],
                         "failed")

    def test_interrupted_plan_recovers_expired_claim(self) -> None:
        self._interrupted()
        batch, _ = self._run(now=95)
        item = batch["items"]["j-1\t1"]
        self.assertEqual(item["action"], "recover")
        # The recover uses the recorded recovery moment, not `now`.
        self.assertEqual(item["at"], 91)
        self.assertEqual(item["decision"]["state"], "ready")
        self.assertEqual(load_dispatch(self.dispatch)[0]["j-1"]["state"],
                         "ready")
        bindings = load_dispatch(self.dispatch)[1]
        self.assertEqual(bindings["4:bk-1j-1/1"],
                         {"action": "recover", "job_id": "j-1", "at": 91})

    def test_active_plan_is_not_collected(self) -> None:
        self._plan()
        record(self.execution, "j-1", 1, "s-1", "worker-1", "stage",
               "succeeded", "staged", 62)
        batch, created = self._run()
        self.assertTrue(created)
        self.assertEqual(batch["status"], "completed")
        self.assertEqual(batch["items"], {})
        self.assertEqual(load_dispatch(self.dispatch)[0]["j-1"]["state"],
                         "claimed")

    # -- pending-first persistence and failures --------------------------

    def test_item_is_persisted_pending_before_dispatch_is_called(self) -> None:
        self._completed()
        seen = {}
        original = execution_sync._dispatch.finish

        def observe(ledger, job_id, dkey, owner, result, at):
            data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
            item = data["batches"]["bk-1"]["items"]["j-1\t1"]
            seen["pending"] = item
            return original(ledger, job_id, dkey, owner, result, at)

        execution_sync._dispatch.finish = observe
        try:
            self._run()
        finally:
            execution_sync._dispatch.finish = original
        self.assertEqual(seen["pending"]["status"], "pending")
        self.assertIsNone(seen["pending"]["error"])
        self.assertIsNone(seen["pending"]["decision"])

    def test_failure_records_public_class_keeps_pending_and_reraises(self) -> None:
        self._completed()
        original_finish = execution_sync._dispatch.finish

        def boom(*_args, **_kwargs):
            try:
                raise ValueError("inner")
            except ValueError as inner:
                raise RuntimeError("dispatch down") from inner

        execution_sync._dispatch.finish = boom
        try:
            with self.assertRaises(RuntimeError) as caught:
                self._run()
        finally:
            execution_sync._dispatch.finish = original_finish
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        data = self._coord()
        batch = data["batches"]["bk-1"]
        self.assertEqual(batch["status"], "pending")
        item = batch["items"]["j-1\t1"]
        self.assertEqual(item["status"], "pending")
        self.assertEqual(item["error"], "RuntimeError")
        self.assertIsNone(item["decision"])
        self.assertNotIn("bk-1", data["audit"])
        # The dispatch key was never served before the failure.
        self.assertNotIn("4:bk-1j-1/1", load_dispatch(self.dispatch)[1])

    def test_retry_after_failure_applies_with_same_dispatch_key(self) -> None:
        self._completed()
        calls = {"n": 0}
        original = execution_sync._dispatch.finish

        def flaky(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("dispatch down")
            return original(*args, **kwargs)

        execution_sync._dispatch.finish = flaky
        try:
            with self.assertRaises(RuntimeError):
                self._run()
            batch, created = self._run()
        finally:
            execution_sync._dispatch.finish = original
        self.assertFalse(created)
        self.assertEqual(batch["items"]["j-1\t1"]["status"], "applied")
        self.assertIsNone(batch["items"]["j-1\t1"]["error"])
        self.assertEqual(calls["n"], 2)
        self.assertEqual(
            [key for key in load_dispatch(self.dispatch)[1]
             if key.endswith("j-1/1")],
            ["4:bk-1j-1/1"])

    # -- replay and crash re-entry ---------------------------------------

    def test_completed_batch_replay_is_byte_identical(self) -> None:
        self._completed()
        self._run()
        raw = Path(self.ledger).read_bytes()
        batch, created = self._run(now=999)
        self.assertFalse(created)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        self.assertEqual(batch["status"], "completed")

    def test_crash_after_dispatch_commit_converges_byte_identical(self) -> None:
        self._completed()
        _, created = self._run()
        self.assertTrue(created)
        final_bytes = Path(self.ledger).read_bytes()
        data = json.loads(final_bytes.decode("utf-8"))
        item = data["batches"]["bk-1"]["items"]["j-1\t1"]
        item["status"] = "pending"
        item["error"] = None
        item["decision"] = None
        data["batches"]["bk-1"]["status"] = "pending"
        Path(self.ledger).write_bytes(
            execution_sync._canonical_bytes(data["batches"], {}))

        dispatch_calls = {"n": 0}
        original = execution_sync._dispatch.finish

        def counting(*args, **kwargs):
            dispatch_calls["n"] += 1
            return original(*args, **kwargs)

        execution_sync._dispatch.finish = counting
        try:
            batch, created = self._run()
        finally:
            execution_sync._dispatch.finish = original
        self.assertFalse(created)
        # The dispatch call replayed rather than creating a second
        # decision, and the batch converged to the exact original bytes.
        self.assertEqual(dispatch_calls["n"], 1)
        self.assertEqual(batch["items"]["j-1\t1"]["status"], "applied")
        self.assertEqual(Path(self.ledger).read_bytes(), final_bytes)

    # -- ownership and takeover ------------------------------------------

    def _make_pending_failed_batch(self, owner: str = "owner-a",
                                   now: int = 70, lease: int = 5) -> None:
        self._completed()
        original = execution_sync._dispatch.finish

        def boom(*_args, **_kwargs):
            raise RuntimeError("dispatch down")

        execution_sync._dispatch.finish = boom
        try:
            with self.assertRaises(RuntimeError):
                self._run(owner=owner, now=now, lease=lease)
        finally:
            execution_sync._dispatch.finish = original

    def test_other_owner_before_expiry_is_blocked(self) -> None:
        self._make_pending_failed_batch()
        raw = Path(self.ledger).read_bytes()
        with self.assertRaises(PermissionError):
            self._run(owner="owner-b", now=75, lease=5)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)

    def test_takeover_after_expiry_completes_without_duplicates(self) -> None:
        self._make_pending_failed_batch()
        batch, created = self._run(owner="owner-b", now=76, lease=10)
        self.assertFalse(created)
        self.assertEqual(batch["owner"], "owner-b")
        self.assertEqual(batch["until"], 86)
        self.assertEqual(batch["status"], "completed")
        self.assertEqual(
            [key for key in load_dispatch(self.dispatch)[1]
             if key.endswith("j-1/1")],
            ["4:bk-1j-1/1"])

    # -- fixed scan and later batches ------------------------------------

    def test_reentry_does_not_pick_up_later_terminal_plans(self) -> None:
        # A failed attempt is finished as failed, then reclaimed: the
        # second dispatch attempt gets its own terminal plan.
        self._failed()
        self._run()
        raw = Path(self.ledger).read_bytes()
        claim(self.dispatch, "j-1", "lk-2", "worker-1", 30, 65)
        plan(self.jobs, self.supply, self.trades, self.dispatch,
             self.execution, "j-1", "pk-2", "worker-1", None, 66)
        record(self.execution, "j-1", 2, "s-3", "worker-1", "stage",
               "failed", "broke again", 67)
        batch, created = self._run()
        self.assertFalse(created)
        self.assertEqual(list(batch["items"]), ["j-1\t1"])
        self.assertEqual(Path(self.ledger).read_bytes(), raw)

    def test_new_batch_scans_only_unsynced_plans(self) -> None:
        self._failed()
        self._run(key="bk-1")
        claim(self.dispatch, "j-1", "lk-2", "worker-1", 30, 65)
        plan(self.jobs, self.supply, self.trades, self.dispatch,
             self.execution, "j-1", "pk-2", "worker-1", None, 66)
        record(self.execution, "j-1", 2, "s-3", "worker-1", "stage",
               "failed", "broke again", 67)
        batch, created = self._run(key="bk-2")
        self.assertTrue(created)
        self.assertEqual(list(batch["items"]), ["j-1\t2"])
        data = self._coord()
        self.assertEqual(list(data["batches"].keys()), ["bk-1", "bk-2"])
        self.assertEqual(list(data["audit"].keys()), ["bk-1", "bk-2"])

    def test_dispatch_key_bound_elsewhere_raises_value_error_no_advance(self) -> None:
        self._completed()
        # Pre-bind the stable dispatch key to a different request.
        from carbon_market.dispatch import finish as dispatch_finish
        dispatch_finish(self.dispatch, "j-1", "4:bk-1j-1/1", "worker-1",
                        "failed", 63)
        self.assertFalse(Path(self.ledger).exists())
        with self.assertRaises(ValueError):
            self._run()
        # No coordination ledger was created, no decision rewritten.
        self.assertFalse(Path(self.ledger).exists())
        self.assertEqual(load_dispatch(self.dispatch)[0]["j-1"]["state"],
                         "failed")

    # -- canonical form and validation -----------------------------------

    def test_snapshot_orders_items_by_job_then_numeric_attempt(self) -> None:
        items = {
            "j-2\t1": {"job_id": "j-2", "attempt": 1},
            "j-1\t10": {"job_id": "j-1", "attempt": 10},
            "j-1\t2": {"job_id": "j-1", "attempt": 2},
        }
        batch = {"items": items}
        self.assertEqual(
            list(execution_sync._ordered_items(batch)),
            [items["j-1\t2"], items["j-1\t10"], items["j-2\t1"]])

    def test_mismatched_dispatch_attempt_raises_value_error_no_advance(self) -> None:
        # A pending batch from a failed dispatch call...
        self._make_pending_failed_batch()
        raw = Path(self.ledger).read_bytes()
        # ...then the dispatch decision moves on externally: recovered to
        # ready and reclaimed under a new owner with a new attempt.
        from carbon_market.dispatch import recover as dispatch_recover
        # The claim lease ends at 90, so it is recoverable strictly
        # after that.
        dispatch_recover(self.dispatch, "j-1", "dr-1", 91)
        claim(self.dispatch, "j-1", "lk-9", "worker-9", 5, 92)
        with self.assertRaises(ValueError):
            self._run(owner="owner-a", now=93, lease=5)
        # The failed attempt did not advance anything.
        self.assertEqual(Path(self.ledger).read_bytes(), raw)

    def test_mismatched_dispatch_owner_raises_value_error_no_advance(self) -> None:
        self._make_pending_failed_batch()
        raw = Path(self.ledger).read_bytes()
        # Rewrite the dispatch ledger so the still-claimed decision names
        # another owner while keeping attempt 1.
        decisions, bindings, events, dispatch_raw = load_dispatch(
            self.dispatch)
        decisions["j-1"]["owner"] = "worker-other"
        from carbon_market import dispatch as dispatch_module
        payload = dispatch_module._canonical_bytes(
            decisions, bindings, events)
        Path(self.dispatch).write_bytes(payload)
        with self.assertRaises(ValueError):
            self._run(owner="owner-a", now=71, lease=5)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        Path(self.dispatch).write_bytes(dispatch_raw)

    def test_non_ascii_and_canonical_form(self) -> None:
        self._completed()
        self._run()
        # The batch itself carries no receipt; tampering with the bytes
        # breaks canonical acceptance on re-entry.
        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        Path(self.ledger).write_bytes(raw + b"\n")
        with self.assertRaises(ValueError):
            self._run(key="bk-9")

    def test_corrupt_ledger_raises_value_error(self) -> None:
        self._completed()
        self._run()
        raw = Path(self.ledger).read_bytes()
        data = json.loads(raw.decode("utf-8"))
        data["version"] = 2
        Path(self.ledger).write_text(json.dumps(data) + "\n",
                                     encoding="utf-8")
        with self.assertRaises(ValueError):
            self._run(key="bk-9")

    def test_item_state_mismatch_raises_value_error(self) -> None:
        self._completed()
        self._run()
        raw = Path(self.ledger).read_bytes()
        data = json.loads(raw.decode("utf-8"))
        item = data["batches"]["bk-1"]["items"]["j-1\t1"]
        item["state"] = "failed"
        item["action"] = "finish"
        Path(self.ledger).write_bytes(
            execution_sync._canonical_bytes(data["batches"], data["audit"]))
        with self.assertRaises(ValueError):
            self._run(key="bk-9")

    # -- arguments and missing files -------------------------------------

    def test_invalid_arguments(self) -> None:
        good = [self.execution, self.dispatch, self.ledger,
                "owner", "bk", 70, 50]
        for index in range(5):
            for bad in ("", 123, None):
                args = list(good)
                args[index] = bad
                with self.subTest(index=index, bad=bad):
                    with self.assertRaises(ValueError):
                        execution_sync.run(*args)  # type: ignore[arg-type]
        for bad in (-1, True, False, 1.5, "70", None):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    execution_sync.run(
                        self.execution, self.dispatch, self.ledger,
                        "owner", "bk", bad, 50)  # type: ignore[arg-type]
        for bad in (0, -1, True, False, 1.5, "50", None):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    execution_sync.run(
                        self.execution, self.dispatch, self.ledger,
                        "owner", "bk", 70, bad)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            execution_sync.run(self.execution, self.dispatch, self.dispatch,
                               "owner", "bk", 70, 50)

    def test_missing_inputs_and_parent_raise_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            execution_sync.run(
                os.path.join(self.tmp.name, "missing.json"),
                self.dispatch, self.ledger, "owner", "bk", 70, 50)
        with self.assertRaises(FileNotFoundError):
            execution_sync.run(
                self.execution,
                os.path.join(self.tmp.name, "missing-dispatch.json"),
                self.ledger, "owner", "bk", 70, 50)
        with self.assertRaises(FileNotFoundError):
            execution_sync.run(
                self.execution, self.dispatch,
                os.path.join(self.tmp.name, "nested", "sync.json"),
                "owner", "bk", 70, 50)


if __name__ == "__main__":
    unittest.main()
