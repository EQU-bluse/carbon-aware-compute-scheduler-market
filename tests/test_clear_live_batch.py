"""Tests for market.clear_live_batch, the atomic batch live clearing."""

from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import dispatch as dispatch_module
from carbon_market import execution as execution_module
from carbon_market import execution_sync as execution_sync_module
from carbon_market import jobs as jobs_module
from carbon_market import market
from carbon_market.cancellation import cancel
from carbon_market.completion import complete
from carbon_market.market import clear, clear_live, clear_live_batch
from carbon_market.resources import publish
from carbon_market.signals import publish as publish_signal


def _resource(resource_id: str = "r-1", **overrides: object) -> dict:
    resource: dict = {
        "resource_id": resource_id,
        "region": "eu-north",
        "capacity": 100,
        "start": 10,
        "end": 500,
        "unit_cost": 3,
        "carbon_intensity": 7,
        "residency": ["eu-north"],
    }
    resource.update(overrides)
    return resource


def _signal(region: str = "eu-north", **overrides: object) -> dict:
    signal: dict = {
        "region": region,
        "observed": 0,
        "expires": 500,
        "mix": {"solar": 10000},
        "unit_cost": 3,
        "carbon_intensity": 7,
    }
    signal.update(overrides)
    return signal


def _job(job_id: str = "j-1", **overrides: object) -> dict:
    job: dict = {
        "job_id": job_id,
        "work": 10,
        "deadline": 100,
        "regions": ["eu-north"],
        "residency": ["eu-north"],
        "max_cost": 1000,
        "carbon_cap": 1000,
    }
    job.update(overrides)
    return job


class ClearLiveBatchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        base = self.tmp.name
        self.jobs = os.path.join(base, "jobs.json")
        self.supply = os.path.join(base, "supply.json")
        self.signals = os.path.join(base, "signals.json")
        self.ledger = os.path.join(base, "ledger.json")
        self._submit_count = 0

    def _submit(self, *jobs: dict) -> None:
        for job in jobs:
            jobs_module.submit(self.jobs, job, f"jk-{self._submit_count}")
            self._submit_count += 1

    def _batch(self, job_ids, key="b-1", at=50, **kwargs):
        return clear_live_batch(self.jobs, self.supply, self.signals,
                                self.ledger, job_ids, key, at, **kwargs)

    # -- basic clearing and replay --------------------------------------

    def test_clears_batch_and_returns_snapshot(self) -> None:
        self._submit(_job("j-1"), _job("j-2", work=20))
        publish(self.supply, _resource("r-1"), "k1")
        publish_signal(self.signals, _signal(), "s1")
        result, created = self._batch(["j-2", "j-1"])
        self.assertTrue(created)
        self.assertEqual(result["key"], "b-1")
        self.assertEqual(result["at"], 50)
        self.assertEqual(list(result["trades"]), ["j-1", "j-2"])
        self.assertEqual(result["unassigned"], [])
        trade = result["trades"]["j-1"]
        self.assertEqual(trade["resource_id"], "r-1")
        self.assertEqual(trade["version"], 1)
        self.assertEqual(trade["at"], 50)
        self.assertEqual(trade["work"], 10)
        self.assertEqual(trade["selection"],
                         {"resource_id": "r-1", "version": 1})
        # The recorded candidates keep the shared trade shape: the
        # selection is the first ordered candidate.
        self.assertEqual(trade["candidates"][0]["resource"]["resource_id"],
                         "r-1")

    def test_replay_returns_recorded_outcome_without_writing(self) -> None:
        self._submit(_job("j-1"), _job("j-2"))
        publish(self.supply, _resource("r-1"), "k1")
        publish_signal(self.signals, _signal(), "s1")
        result, created = self._batch(["j-1", "j-2"])
        self.assertTrue(created)
        raw = Path(self.ledger).read_bytes()
        # The request order does not matter for an equivalent replay.
        again, created = self._batch(["j-2", "j-1"])
        self.assertFalse(created)
        self.assertEqual(again, result)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)

    def test_same_key_with_changed_request_raises(self) -> None:
        self._submit(_job("j-1"), _job("j-2"))
        publish(self.supply, _resource("r-1"), "k1")
        publish_signal(self.signals, _signal(), "s1")
        self._batch(["j-1", "j-2"])
        raw = Path(self.ledger).read_bytes()
        with self.assertRaises(ValueError):
            self._batch(["j-1"])
        with self.assertRaises(ValueError):
            self._batch(["j-1", "j-2"], at=51)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)

    def test_empty_assignment_still_commits_replayable_result(self) -> None:
        self._submit(_job("j-1"))
        publish(self.supply, _resource("r-1", start=100), "k1")
        publish_signal(self.signals, _signal(), "s1")
        result, created = self._batch(["j-1"])
        self.assertTrue(created)
        self.assertEqual(result["trades"], {})
        self.assertEqual(result["unassigned"], ["j-1"])
        self.assertTrue(Path(self.ledger).exists())
        again, created = self._batch(["j-1"])
        self.assertFalse(created)
        self.assertEqual(again, result)

    # -- global selection objectives -------------------------------------

    def test_total_work_beats_job_count(self) -> None:
        # Capacity 10: one work-10 job against two work-5 jobs ties on
        # total work; the higher job count wins.
        self._submit(_job("j-big", work=10), _job("j-s1", work=5),
                     _job("j-s2", work=5))
        publish(self.supply, _resource("r-1", capacity=10), "k1")
        publish_signal(self.signals, _signal(), "s1")
        result, _ = self._batch(["j-big", "j-s1", "j-s2"])
        self.assertEqual(sorted(result["trades"]), ["j-s1", "j-s2"])
        self.assertEqual(result["unassigned"], ["j-big"])

    def test_total_work_is_the_primary_objective(self) -> None:
        # Capacity 10: the work-10 job alone beats the two work-4 jobs.
        self._submit(_job("j-big", work=10), _job("j-s1", work=4),
                     _job("j-s2", work=4))
        publish(self.supply, _resource("r-1", capacity=10), "k1")
        publish_signal(self.signals, _signal(), "s1")
        result, _ = self._batch(["j-big", "j-s1", "j-s2"])
        self.assertEqual(sorted(result["trades"]), ["j-big"])

    def test_global_selection_beats_per_job_greedy(self) -> None:
        # j-1 can use r-a or r-b, j-2 only r-a; clearing j-1 onto its
        # best slot alone would leave j-2 unserved.
        self._submit(_job("j-1", regions=["eu-north", "eu-west"]),
                     _job("j-2"))
        publish(self.supply, _resource("r-a", capacity=10), "k1")
        publish(self.supply,
                _resource("r-b", capacity=10, region="eu-west",
                          residency=["eu-north", "eu-west"]), "k2")
        publish_signal(self.signals,
                       _signal("eu-north", carbon_intensity=1), "s1")
        publish_signal(self.signals,
                       _signal("eu-west", carbon_intensity=9), "s2")
        result, _ = self._batch(["j-1", "j-2"])
        self.assertEqual(result["unassigned"], [])
        slots = {job: trade["resource_id"]
                 for job, trade in result["trades"].items()}
        self.assertEqual(slots, {"j-1": "r-b", "j-2": "r-a"})

    def test_carbon_is_minimized_before_cost(self) -> None:
        # Both jobs fit both slots: the greener slot costs more, and
        # carbon is minimized before cost, so both jobs land on the
        # green slot even though the dirty one is cheaper.
        self._submit(_job("j-1", regions=["eu-north", "eu-west"]),
                     _job("j-2", regions=["eu-north", "eu-west"]))
        publish(self.supply, _resource("r-green", capacity=20), "k1")
        publish(self.supply,
                _resource("r-cheap", capacity=20, region="eu-west",
                          residency=["eu-north", "eu-west"]), "k2")
        publish_signal(self.signals,
                       _signal("eu-north", carbon_intensity=1,
                               unit_cost=9), "s1")
        publish_signal(self.signals,
                       _signal("eu-west", carbon_intensity=9,
                               unit_cost=1), "s2")
        result, _ = self._batch(["j-1", "j-2"])
        self.assertEqual(result["unassigned"], [])
        for trade in result["trades"].values():
            self.assertEqual(trade["resource_id"], "r-green")

    def test_lexicographic_sequence_breaks_final_ties(self) -> None:
        # Identical slots and jobs: the smaller job id takes the
        # smaller resource id, regardless of the request order.
        self._submit(_job("j-1"), _job("j-2"))
        publish(self.supply, _resource("r-a", capacity=10), "k1")
        publish(self.supply, _resource("r-b", capacity=10), "k2")
        publish_signal(self.signals, _signal(), "s1")
        result, _ = self._batch(["j-2", "j-1"])
        slots = {job: trade["resource_id"]
                 for job, trade in result["trades"].items()}
        self.assertEqual(slots, {"j-1": "r-a", "j-2": "r-b"})

    def test_capacity_contention_leaves_loser_unassigned(self) -> None:
        self._submit(_job("j-a"), _job("j-b"))
        publish(self.supply, _resource("r-1", capacity=10), "k1")
        publish_signal(self.signals, _signal(), "s1")
        result, created = self._batch(["j-b", "j-a"])
        self.assertTrue(created)
        self.assertEqual(sorted(result["trades"]), ["j-a"])
        self.assertEqual(result["unassigned"], ["j-b"])

    def test_missing_signal_leaves_job_unassigned(self) -> None:
        self._submit(_job("j-1"), _job("j-2", regions=["eu-west"],
                                       residency=["eu-west"]))
        publish(self.supply, _resource("r-1"), "k1")
        publish(self.supply,
                _resource("r-2", region="eu-west",
                          residency=["eu-west"]), "k2")
        publish_signal(self.signals, _signal("eu-north"), "s1")
        result, _ = self._batch(["j-1", "j-2"])
        self.assertEqual(sorted(result["trades"]), ["j-1"])
        self.assertEqual(result["unassigned"], ["j-2"])

    # -- argument and state validation -----------------------------------

    def test_invalid_arguments_raise_before_files_are_read(self) -> None:
        with self.assertRaises(ValueError):
            self._batch([])
        with self.assertRaises(ValueError):
            self._batch(["j-1", "j-1"])
        with self.assertRaises(ValueError):
            self._batch(["j-1", ""])
        with self.assertRaises(ValueError):
            self._batch(["j-1", 7])
        with self.assertRaises(ValueError):
            self._batch("j-1")
        with self.assertRaises(ValueError):
            self._batch(["j-1"], key="")
        with self.assertRaises(ValueError):
            self._batch(["j-1"], at=-1)
        with self.assertRaises(ValueError):
            self._batch(["j-1"], at=True)
        with self.assertRaises(ValueError):
            clear_live_batch(self.jobs, self.supply, self.signals,
                             self.signals, ["j-1"], "b-1", 50)
        self.assertFalse(Path(self.ledger).exists())

    def test_unknown_job_raises_key_error_without_ledger(self) -> None:
        self._submit(_job("j-1"))
        publish(self.supply, _resource("r-1"), "k1")
        publish_signal(self.signals, _signal(), "s1")
        with self.assertRaises(KeyError):
            self._batch(["j-1", "j-unknown"])
        self.assertFalse(Path(self.ledger).exists())

    def test_already_traded_job_fails_the_whole_batch(self) -> None:
        self._submit(_job("j-1"), _job("j-2"))
        publish(self.supply, _resource("r-1"), "k1")
        publish_signal(self.signals, _signal(), "s1")
        clear_live(self.jobs, self.supply, self.signals, self.ledger,
                   "j-1", "k-single", 50)
        raw = Path(self.ledger).read_bytes()
        with self.assertRaises(ValueError):
            self._batch(["j-1", "j-2"])
        self.assertEqual(Path(self.ledger).read_bytes(), raw)

    def test_cancelled_job_fails_the_whole_batch(self) -> None:
        self._submit(_job("j-1"), _job("j-2"))
        publish(self.supply, _resource("r-1"), "k1")
        publish_signal(self.signals, _signal(), "s1")
        clear_live(self.jobs, self.supply, self.signals, self.ledger,
                   "j-1", "k-single", 50)
        cancels = os.path.join(self.tmp.name, "cancel.json")
        dispatch = os.path.join(self.tmp.name, "dispatch.json")
        cancel(self.jobs, self.supply, self.ledger, dispatch, cancels,
               "j-1", "ck-1", 60, "no longer needed")
        raw = Path(self.ledger).read_bytes()
        with self.assertRaises(ValueError):
            self._batch(["j-1", "j-2"], at=70, cancellations=cancels)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)

    def test_missing_inputs_raise_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            self._batch(["j-1"])
        self._submit(_job("j-1"))
        with self.assertRaises(FileNotFoundError):
            self._batch(["j-1"])
        publish(self.supply, _resource("r-1"), "k1")
        with self.assertRaises(FileNotFoundError):
            self._batch(["j-1"])

    # -- ledger format and interop ----------------------------------------

    def test_ledger_carries_batch_binding_and_audit(self) -> None:
        self._submit(_job("j-1"), _job("j-2"))
        publish(self.supply, _resource("r-1"), "k1")
        publish_signal(self.signals, _signal(), "s1")
        self._batch(["j-1", "j-2"])
        with open(self.ledger, encoding="utf-8") as handle:
            data = json.load(handle)
        self.assertEqual(list(data), ["version", "trades", "idempotency",
                                      "audit", "batches"])
        self.assertEqual(data["batches"],
                         {"b-1": {"at": 50, "jobs": ["j-1", "j-2"],
                                  "unassigned": []}})
        self.assertEqual(sorted(data["idempotency"]), ["b-1#j-1",
                                                       "b-1#j-2"])
        self.assertEqual(sorted(data["audit"]), ["b-1#j-1", "b-1#j-2"])
        for key in ("b-1#j-1", "b-1#j-2"):
            event = data["audit"][key]
            self.assertEqual(event["key"], key)
            self.assertEqual(event["resource_id"], "r-1")

    def test_batch_trades_deduct_capacity_for_single_clears(self) -> None:
        self._submit(_job("j-1"), _job("j-2"), _job("j-3"))
        publish(self.supply, _resource("r-1", capacity=25), "k1")
        publish_signal(self.signals, _signal(), "s1")
        self._batch(["j-1", "j-2"])
        with self.assertRaises(LookupError):
            clear_live(self.jobs, self.supply, self.signals, self.ledger,
                       "j-3", "k-3", 50)
        # A static clear over the same ledger sees the same occupancy.
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.ledger, "j-3", "k-3", 50)

    def test_single_clears_deduct_capacity_for_the_batch(self) -> None:
        self._submit(_job("j-1"), _job("j-2"))
        publish(self.supply, _resource("r-1", capacity=15), "k1")
        publish_signal(self.signals, _signal(), "s1")
        clear_live(self.jobs, self.supply, self.signals, self.ledger,
                   "j-1", "k-1", 50)
        result, _ = self._batch(["j-2"])
        self.assertEqual(result["unassigned"], ["j-2"])

    def test_later_single_clear_preserves_batch_records(self) -> None:
        self._submit(_job("j-1"), _job("j-2"))
        publish(self.supply, _resource("r-1"), "k1")
        publish_signal(self.signals, _signal(), "s1")
        self._batch(["j-1"])
        trade, created = clear_live(self.jobs, self.supply, self.signals,
                                    self.ledger, "j-2", "k-2", 50)
        self.assertTrue(created)
        again, created = self._batch(["j-1"])
        self.assertFalse(created)
        self.assertEqual(sorted(again["trades"]), ["j-1", "j-2"])
        self.assertEqual(again["unassigned"], [])

    def test_unassigned_job_can_be_cleared_later(self) -> None:
        self._submit(_job("j-1"), _job("j-2"))
        publish(self.supply, _resource("r-1", capacity=10), "k1")
        publish_signal(self.signals, _signal(), "s1")
        result, _ = self._batch(["j-1", "j-2"])
        self.assertEqual(result["unassigned"], ["j-2"])
        # The batch left j-2 untraded, so a later clear may trade it;
        # here the version is full, so the refusal is a LookupError
        # from capacity, not a ValueError from the batch record.
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.ledger, "j-2", "k-2", 50)
        # Freeing the version lets the very same job clear afterwards.
        cancels = os.path.join(self.tmp.name, "cancel.json")
        dispatch = os.path.join(self.tmp.name, "dispatch.json")
        cancel(self.jobs, self.supply, self.ledger, dispatch, cancels,
               "j-1", "ck-1", 60, "no longer needed")
        trade, created = clear(self.jobs, self.supply, self.ledger,
                               "j-2", "k-2", 70, cancellations=cancels)
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-1")

    def test_batch_trades_are_visible_to_dispatch(self) -> None:
        self._submit(_job("j-1"), _job("j-2"))
        publish(self.supply, _resource("r-1"), "k1")
        publish_signal(self.signals, _signal(), "s1")
        self._batch(["j-1", "j-2"])
        dispatch = os.path.join(self.tmp.name, "dispatch.json")
        decision, created = dispatch_module.commit(
            self.jobs, self.supply, self.ledger, dispatch, "j-1",
            "dk-1", 60)
        self.assertTrue(created)
        self.assertEqual(decision["resource_id"], "r-1")

    def test_completion_releases_capacity_for_a_later_batch(self) -> None:
        self._submit(_job("j-1"), _job("j-2"))
        publish(self.supply, _resource("r-1", capacity=10), "k1")
        publish_signal(self.signals, _signal(), "s1")
        clear_live(self.jobs, self.supply, self.signals, self.ledger,
                   "j-1", "k-1", 50)
        result, _ = self._batch(["j-2"], at=55)
        self.assertEqual(result["unassigned"], ["j-2"])
        # Drive j-1 through the lifecycle to a completion at 58.
        dispatch = os.path.join(self.tmp.name, "dispatch.json")
        execution = os.path.join(self.tmp.name, "execution.json")
        completions = os.path.join(self.tmp.name, "completions.json")
        sync = os.path.join(self.tmp.name, "sync.json")
        dispatch_module.commit(self.jobs, self.supply, self.ledger,
                               dispatch, "j-1", "dk-1", 55)
        dispatch_module.claim(dispatch, "j-1", "dk-2", "owner", 40, 56)
        execution_module.plan(self.jobs, self.supply, self.ledger,
                              dispatch, execution, "j-1", "ek-1", "owner",
                              None, 57)
        execution_module.record(execution, "j-1", 1, "s-1", "owner",
                                "stage", "succeeded", "staged", 57)
        execution_module.record(execution, "j-1", 1, "s-2", "owner",
                                "start", "succeeded", "started", 57)
        execution_sync_module.run(execution, dispatch, sync, "sync-1",
                                  "bk-1", 58, 50)
        complete(self.jobs, self.supply, self.signals, self.ledger,
                 dispatch, execution, completions, "j-1", "ck-1", 58,
                 "succeeded", 1, 1)
        result, created = self._batch(["j-2"], key="b-2", at=59,
                                      completions=completions)
        self.assertTrue(created)
        self.assertEqual(result["unassigned"], [])
        self.assertEqual(sorted(result["trades"]), ["j-1", "j-2"])

    # -- atomicity and concurrency ----------------------------------------

    def test_failed_commit_restores_previous_bytes(self) -> None:
        self._submit(_job("j-1"), _job("j-2"))
        publish(self.supply, _resource("r-1"), "k1")
        publish_signal(self.signals, _signal(), "s1")
        clear_live(self.jobs, self.supply, self.signals, self.ledger,
                   "j-1", "k-1", 50)
        before = Path(self.ledger).read_bytes()
        real_commit = market._commit_clear

        def failing_commit(realpath, payload, old_bytes):
            raise OSError("injected commit failure")

        market._commit_clear = failing_commit
        try:
            with self.assertRaises(OSError):
                self._batch(["j-2"])
        finally:
            market._commit_clear = real_commit
        self.assertEqual(Path(self.ledger).read_bytes(), before)
        result, created = self._batch(["j-2"])
        self.assertTrue(created)
        self.assertEqual(sorted(result["trades"]), ["j-1", "j-2"])

    def test_concurrent_batch_and_single_clear_never_oversell(self) -> None:
        self._submit(_job("j-1"), _job("j-2"), _job("j-3"))
        publish(self.supply, _resource("r-1", capacity=15), "k1")
        publish_signal(self.signals, _signal(), "s1")
        errors: list[BaseException] = []

        def run_batch() -> None:
            try:
                self._batch(["j-1", "j-2"])
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def run_single() -> None:
            try:
                clear_live(self.jobs, self.supply, self.signals,
                           self.ledger, "j-3", "k-3", 50)
            except LookupError:
                pass
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=run_batch),
                   threading.Thread(target=run_single)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        with open(self.ledger, encoding="utf-8") as handle:
            data = json.load(handle)
        total = sum(trade["work"] for trade in data["trades"].values())
        self.assertLessEqual(total, 15)

    def test_concurrent_batches_serialize(self) -> None:
        self._submit(_job("j-1"), _job("j-2"))
        publish(self.supply, _resource("r-1", capacity=25), "k1")
        publish_signal(self.signals, _signal(), "s1")
        errors: list[BaseException] = []

        def run_batch(job_id: str, key: str) -> None:
            try:
                self._batch([job_id], key=key)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=run_batch, args=("j-1", "b-1")),
                   threading.Thread(target=run_batch, args=("j-2", "b-2"))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        with open(self.ledger, encoding="utf-8") as handle:
            data = json.load(handle)
        self.assertEqual(sorted(data["trades"]), ["j-1", "j-2"])
        self.assertEqual(sorted(data["batches"]), ["b-1", "b-2"])


if __name__ == "__main__":
    unittest.main()
