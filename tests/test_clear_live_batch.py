from __future__ import annotations

import json
import os
import threading
import unittest
import unittest.mock
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import dispatch as dispatch_module
from carbon_market import execution as execution_module
from carbon_market import execution_sync as execution_sync_module
from carbon_market import jobs as jobs_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.cancellation import cancel
from carbon_market.completion import complete
from carbon_market.market import clear, clear_live, clear_live_batch
from carbon_market.resources import publish as publish_resource
from carbon_market.signals import publish as publish_signal


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


def _signal(region: str = "eu-north", **overrides: object) -> dict[str, object]:
    signal: dict[str, object] = {
        "region": region,
        "observed": 0,
        "expires": 500,
        "mix": {"solar": 10000},
        "unit_cost": 3,
        "carbon_intensity": 7,
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


class ClearLiveBatchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        base = self.tmp.name
        self.jobs = os.path.join(base, "jobs.json")
        self.supply = os.path.join(base, "supply.json")
        self.signals = os.path.join(base, "signals.json")
        self.ledger = os.path.join(base, "ledger.json")
        self.dispatch = os.path.join(base, "dispatch.json")
        self.execution = os.path.join(base, "execution.json")
        self.sync = os.path.join(base, "sync.json")
        self.completions = os.path.join(base, "completions.json")
        self.cancellations = os.path.join(base, "cancellations.json")
        self.sidecar = self.ledger + ".batches"

    # -- fixture helpers -----------------------------------------------------

    def _submit(self, job_id: str, **overrides: object) -> None:
        jobs_module.submit(self.jobs, _job(job_id, **overrides),
                           f"jk-{job_id}")

    def _seed_two_regions(self, cap1: int = 10, cap2: int = 20) -> None:
        publish_resource(self.supply, _resource("r-1", capacity=cap1), "k1")
        publish_resource(self.supply,
                         _resource("r-2", region="us-west", capacity=cap2,
                                   residency=["eu-north", "us-west"]), "k2")
        publish_signal(self.signals,
                       _signal("eu-north", unit_cost=3, carbon_intensity=3),
                       "s1")
        publish_signal(self.signals,
                       _signal("us-west", unit_cost=5, carbon_intensity=5),
                       "s2")

    def _batch(self, job_ids: list[str], key: str = "b-1", at: int = 50,
               **kwargs: object):
        return clear_live_batch(self.jobs, self.supply, self.signals,
                                self.ledger, job_ids, key, at, **kwargs)

    def _read(self, path: str) -> bytes:
        return Path(path).read_bytes()

    def _ledger_json(self) -> dict[str, object]:
        return json.loads(self._read(self.ledger))

    # -- global selection ----------------------------------------------------

    def test_global_optimum_shares_capacity_across_jobs(self) -> None:
        self._submit("j-1")
        self._submit("j-2")
        self._seed_two_regions()
        result, created = self._batch(["j-2", "j-1"])
        self.assertTrue(created)
        self.assertEqual(result["key"], "b-1")
        self.assertEqual(result["at"], 50)
        self.assertEqual(result["unallocated"], [])
        # Both jobs fit r-2, but j-1 on r-1 and j-2 on r-2 adds less
        # carbon (80) than both on r-2 (100); the single-job ranking
        # would have sent both to r-1 and failed the second.
        self.assertEqual(list(result["trades"]), ["j-1", "j-2"])
        self.assertEqual(result["trades"]["j-1"]["resource_id"], "r-1")
        self.assertEqual(result["trades"]["j-2"]["resource_id"], "r-2")
        for job_id, resource_id in (("j-1", "r-1"), ("j-2", "r-2")):
            trade = result["trades"][job_id]
            self.assertEqual(
                set(trade), {"job_id", "at", "work", "resource_id",
                             "version", "candidates", "selection"})
            self.assertEqual(trade["job_id"], job_id)
            self.assertEqual(trade["at"], 50)
            self.assertEqual(trade["work"], 10)
            self.assertEqual(trade["version"], 1)
            self.assertEqual(len(trade["candidates"]), 1)
            candidate = trade["candidates"][0]
            self.assertEqual(candidate["resource"]["resource_id"],
                             resource_id)
            self.assertEqual(trade["selection"],
                             {"resource_id": resource_id, "version": 1})

        # The clearing ledger carries both trades, each bound to a
        # derived idempotency sub-key with its audit event.
        ledger = self._ledger_json()
        self.assertEqual(sorted(ledger["trades"]), ["j-1", "j-2"])
        self.assertEqual(sorted(ledger["idempotency"]),
                         ['["b-1","j-1"]', '["b-1","j-2"]'])
        self.assertEqual(sorted(ledger["audit"]),
                         sorted(ledger["idempotency"]))
        event = ledger["audit"]['["b-1","j-1"]']
        self.assertEqual(event["job_id"], "j-1")
        self.assertEqual(event["resource_id"], "r-1")

        # The batch ledger binds the complete request and the outcome.
        sidecar = json.loads(self._read(self.sidecar))
        self.assertEqual(set(sidecar), {"version", "batches"})
        record = sidecar["batches"]["b-1"]
        self.assertEqual(record["jobs"], ["j-1", "j-2"])
        self.assertEqual(record["unallocated"], [])
        self.assertEqual(record["allocated"], {
            "j-1": {"resource_id": "r-1", "version": 1},
            "j-2": {"resource_id": "r-2", "version": 1},
        })
        self.assertEqual(record["snapshot"], ["j-1", "j-2"])

    def test_total_work_beats_job_count(self) -> None:
        self._submit("j-1", work=10)
        self._submit("j-2", work=10)
        self._submit("j-3", work=25)
        publish_resource(self.supply, _resource("r-1", capacity=30), "k1")
        publish_signal(self.signals, _signal(), "s1")
        result, created = self._batch(["j-1", "j-2", "j-3"])
        self.assertTrue(created)
        self.assertEqual(sorted(result["trades"]), ["j-3"])
        self.assertEqual(result["unallocated"], ["j-1", "j-2"])

    def test_job_count_breaks_work_tie(self) -> None:
        self._submit("j-1", work=15)
        self._submit("j-2", work=15)
        self._submit("j-3", work=30)
        publish_resource(self.supply, _resource("r-1", capacity=30), "k1")
        publish_signal(self.signals, _signal(), "s1")
        result, _ = self._batch(["j-1", "j-2", "j-3"])
        self.assertEqual(sorted(result["trades"]), ["j-1", "j-2"])
        self.assertEqual(result["unallocated"], ["j-3"])

    def test_carbon_then_cost_minimized(self) -> None:
        self._submit("j-1")
        self._submit("j-2")
        self._submit("j-3")
        self._submit("j-4")
        publish_resource(self.supply, _resource("r-1", capacity=20), "k1")
        publish_resource(self.supply,
                         _resource("r-2", region="us-west", capacity=20,
                                   residency=["eu-north", "us-west"]), "k2")
        publish_signal(self.signals,
                       _signal("eu-north", unit_cost=9, carbon_intensity=3),
                       "s1")
        publish_signal(self.signals,
                       _signal("us-west", unit_cost=1, carbon_intensity=3),
                       "s2")
        # Equal carbon per resource: cost decides, and r-2 is cheaper.
        result, _ = self._batch(["j-1", "j-2"])
        self.assertEqual(result["unallocated"], [])
        for trade in result["trades"].values():
            self.assertEqual(trade["resource_id"], "r-2")

        # A cleaner eu-north signal wins over the cheaper r-2 for the
        # next batch.
        publish_signal(self.signals,
                       _signal("eu-north", observed=10, unit_cost=9,
                               carbon_intensity=1), "s3")
        result, _ = self._batch(["j-3", "j-4"], key="b-2")
        self.assertEqual(result["unallocated"], [])
        for job_id in ("j-3", "j-4"):
            self.assertEqual(result["trades"][job_id]["resource_id"],
                             "r-1")

    def test_lexicographic_tie_break_ignores_input_order(self) -> None:
        self._submit("j-1")
        self._submit("j-2")
        # Two identical resources in one region under one signal.
        publish_resource(self.supply, _resource("r-1", capacity=10), "k1")
        publish_resource(self.supply, _resource("r-2", capacity=10), "k2")
        publish_signal(self.signals, _signal(), "s1")
        first, _ = self._batch(["j-1", "j-2"])
        self.assertEqual(first["trades"]["j-1"]["resource_id"], "r-1")
        self.assertEqual(first["trades"]["j-2"]["resource_id"], "r-2")

        # A fresh but identical setup with the reversed request order
        # selects exactly the same assignment.
        other = TemporaryDirectory()
        self.addCleanup(other.cleanup)
        saved = (self.jobs, self.supply, self.signals, self.ledger)
        self.jobs = os.path.join(other.name, "jobs.json")
        self.supply = os.path.join(other.name, "supply.json")
        self.signals = os.path.join(other.name, "signals.json")
        self.ledger = os.path.join(other.name, "ledger.json")
        try:
            self._submit("j-1")
            self._submit("j-2")
            publish_resource(self.supply, _resource("r-1", capacity=10),
                             "k1")
            publish_resource(self.supply, _resource("r-2", capacity=10),
                             "k2")
            publish_signal(self.signals, _signal(), "s1")
            second, _ = self._batch(["j-2", "j-1"])
        finally:
            self.jobs, self.supply, self.signals, self.ledger = saved
        self.assertEqual(second, first)

    def test_result_snapshot_includes_preexisting_trades(self) -> None:
        self._submit("j-1")
        self._submit("j-2")
        self._seed_two_regions()
        clear_live(self.jobs, self.supply, self.signals, self.ledger,
                   "j-1", "t-1", 40)
        result, _ = self._batch(["j-2"])
        self.assertEqual(list(result["trades"]), ["j-1", "j-2"])
        self.assertEqual(result["trades"]["j-1"]["resource_id"], "r-1")
        self.assertEqual(result["trades"]["j-2"]["resource_id"], "r-2")

    # -- feasibility rules ---------------------------------------------------

    def test_infeasible_jobs_are_unallocated_never_errors(self) -> None:
        # residency uncovered by every resource
        self._submit("j-1", regions=["ap-south", "eu-north"],
                     residency=["ap-south"])
        self._submit("j-2", deadline=600)            # deadline uncovered
        self._submit("j-3", carbon_cap=1)            # carbon budget
        self._submit("j-4", max_cost=1)              # cost budget
        self._submit("j-5", regions=["ap-south"], residency=["ap-south"])
        self._submit("j-6")
        publish_resource(self.supply, _resource("r-1", capacity=100), "k1")
        publish_signal(self.signals, _signal(), "s1")
        # j-5 has no resource in its regions at all; the others violate
        # one rule each. Only j-6 clears.
        result, created = self._batch(
            ["j-1", "j-2", "j-3", "j-4", "j-5", "j-6"])
        self.assertTrue(created)
        self.assertEqual(sorted(result["trades"]), ["j-6"])
        self.assertEqual(result["unallocated"],
                         ["j-1", "j-2", "j-3", "j-4", "j-5"])

    def test_region_without_live_signal_is_unallocated(self) -> None:
        self._submit("j-1")
        publish_resource(self.supply, _resource("r-1", capacity=100), "k1")
        # The signal file exists but covers only another region.
        publish_signal(self.signals, _signal("us-west"), "s1")
        result, created = self._batch(["j-1"])
        self.assertTrue(created)
        self.assertEqual(result["unallocated"], ["j-1"])
        self.assertEqual(result["trades"], {})

    def test_highest_valid_version_carries_the_capacity(self) -> None:
        self._submit("j-1", work=40)
        publish_resource(self.supply, _resource("r-1", capacity=10,
                                                end=100), "k1")
        publish_resource(self.supply, _resource("r-1", capacity=50,
                                                start=50), "k2")
        publish_signal(self.signals, _signal(), "s1")
        result, _ = self._batch(["j-1"], at=50)
        trade = result["trades"]["j-1"]
        self.assertEqual(trade["version"], 2)
        self.assertEqual(trade["candidates"][0]["resource"]["capacity"], 50)

    def test_zero_allocation_batch_still_persists_replayable_result(
            self) -> None:
        self._submit("j-1", work=999)
        publish_resource(self.supply, _resource("r-1", capacity=10), "k1")
        publish_signal(self.signals, _signal(), "s1")
        result, created = self._batch(["j-1"])
        self.assertTrue(created)
        self.assertEqual(result["unallocated"], ["j-1"])
        self.assertEqual(result["trades"], {})
        # No trade means no clearing ledger, but the binding persists.
        self.assertFalse(os.path.exists(self.ledger))
        self.assertTrue(os.path.exists(self.sidecar))
        replay, created = self._batch(["j-1"])
        self.assertFalse(created)
        self.assertEqual(replay, result)

    # -- idempotent replay ---------------------------------------------------

    def test_replay_returns_stored_result_without_rewriting(self) -> None:
        self._submit("j-1")
        self._submit("j-2")
        self._seed_two_regions()
        result, created = self._batch(["j-1", "j-2"])
        self.assertTrue(created)
        ledger_bytes = self._read(self.ledger)
        sidecar_bytes = self._read(self.sidecar)

        # The same key with the same job set in any order and the same
        # moment replays the original result without reselecting.
        replay, created = self._batch(["j-2", "j-1"])
        self.assertFalse(created)
        self.assertEqual(replay, result)
        self.assertEqual(self._read(self.ledger), ledger_bytes)
        self.assertEqual(self._read(self.sidecar), sidecar_bytes)

        # A later unrelated trade never changes the replayed snapshot.
        self._submit("j-3", work=5)
        clear_live(self.jobs, self.supply, self.signals, self.ledger,
                   "j-3", "t-3", 60)
        replay, created = self._batch(["j-1", "j-2"])
        self.assertFalse(created)
        self.assertEqual(replay, result)
        self.assertEqual(sorted(replay["trades"]), ["j-1", "j-2"])

    def test_same_key_with_changed_request_raises(self) -> None:
        self._submit("j-1")
        self._submit("j-2")
        self._seed_two_regions()
        self._batch(["j-1", "j-2"])
        ledger_bytes = self._read(self.ledger)
        sidecar_bytes = self._read(self.sidecar)
        with self.assertRaises(ValueError):
            self._batch(["j-1"], key="b-1")
        with self.assertRaises(ValueError):
            self._batch(["j-1", "j-2"], key="b-1", at=51)
        with self.assertRaises(ValueError):
            self._batch(["j-2", "j-1", "j-1"], key="b-1")
        self.assertEqual(self._read(self.ledger), ledger_bytes)
        self.assertEqual(self._read(self.sidecar), sidecar_bytes)

    # -- request validation --------------------------------------------------

    def test_invalid_arguments(self) -> None:
        self._submit("j-1")
        self._seed_two_regions()
        bad_calls = [
            lambda: self._batch([]),
            lambda: self._batch(["j-1", "j-1"]),
            lambda: self._batch(["j-1", ""]),
            lambda: self._batch(["j-1", 7]),
            lambda: self._batch("j-1"),
            lambda: self._batch(None),
            lambda: self._batch(["j-1"], key=""),
            lambda: self._batch(["j-1"], key=None),
            lambda: self._batch(["j-1"], at=-1),
            lambda: self._batch(["j-1"], at=True),
            lambda: self._batch(["j-1"], at="50"),
            lambda: self._batch(["j-1"], completions=""),
            lambda: self._batch(["j-1"], cancellations=""),
        ]
        for call in bad_calls:
            with self.assertRaises(ValueError):
                call()
        # Paths that coincide after resolution are rejected.
        with self.assertRaises(ValueError):
            clear_live_batch(self.jobs, self.jobs, self.signals,
                             self.ledger, ["j-1"], "b-1", 50)
        with self.assertRaises(ValueError):
            clear_live_batch(self.jobs, self.supply, self.ledger,
                             self.ledger, ["j-1"], "b-1", 50)
        with self.assertRaises(ValueError):
            clear_live_batch(self.jobs, self.supply, self.signals,
                             self.ledger, ["j-1"], "b-1", 50,
                             completions=self.ledger)
        with self.assertRaises(ValueError):
            clear_live_batch(self.jobs, self.supply, self.signals,
                             self.ledger, ["j-1"], "b-1", 50,
                             completions=self.completions,
                             cancellations=self.completions)
        self.assertFalse(os.path.exists(self.ledger))
        self.assertFalse(os.path.exists(self.sidecar))

    def test_unknown_job_raises_key_error(self) -> None:
        self._submit("j-1")
        self._seed_two_regions()
        with self.assertRaises(KeyError):
            self._batch(["j-1", "j-unknown"])
        self.assertFalse(os.path.exists(self.ledger))
        self.assertFalse(os.path.exists(self.sidecar))

    def test_missing_inputs_raise_file_not_found(self) -> None:
        self._submit("j-1")
        self._seed_two_regions()
        with self.assertRaises(FileNotFoundError):
            clear_live_batch(os.path.join(self.tmp.name, "gone.json"),
                             self.supply, self.signals, self.ledger,
                             ["j-1"], "b-1", 50)
        with self.assertRaises(FileNotFoundError):
            clear_live_batch(self.jobs, os.path.join(self.tmp.name,
                                                     "gone.json"),
                             self.signals, self.ledger, ["j-1"], "b-1", 50)
        with self.assertRaises(FileNotFoundError):
            clear_live_batch(self.jobs, self.supply,
                             os.path.join(self.tmp.name, "gone.json"),
                             self.ledger, ["j-1"], "b-1", 50)
        with self.assertRaises(FileNotFoundError):
            self._batch(["j-1"], cancellations=self.cancellations)
        # A missing ledger parent surfaces only when the first commit
        # is attempted and leaves no fragment behind.
        nested = os.path.join(self.tmp.name, "missing", "ledger.json")
        with self.assertRaises(FileNotFoundError):
            clear_live_batch(self.jobs, self.supply, self.signals,
                             nested, ["j-1"], "b-1", 50)
        self.assertFalse(os.path.exists(nested))
        self.assertFalse(os.path.exists(nested + ".batches"))

    def test_invalid_ledgers_raise_value_error(self) -> None:
        self._submit("j-1")
        self._seed_two_regions()
        self._batch(["j-1"])
        for content in (b"not json", b"{}", b'{"version":1,"batches":{}}'):
            Path(self.sidecar).write_bytes(content)
            with self.assertRaises(ValueError):
                self._batch(["j-1"])
        # A structurally valid but non-canonical sidecar is rejected.
        record = {"key": "b-1", "at": 50, "jobs": ["j-1"],
                  "allocated": {"j-1": {"resource_id": "r-1",
                                        "version": 1}},
                  "unallocated": [], "snapshot": ["j-1"]}
        payload = json.dumps({"version": 1, "batches": {"b-1": record}},
                             indent=2).encode()
        Path(self.sidecar).write_bytes(payload)
        with self.assertRaises(ValueError):
            self._batch(["j-1"])

    # -- conflicts with other requests ---------------------------------------

    def test_job_traded_by_another_request_raises(self) -> None:
        self._submit("j-1")
        self._submit("j-2")
        self._seed_two_regions()
        clear_live(self.jobs, self.supply, self.signals, self.ledger,
                   "j-1", "t-1", 40)
        ledger_bytes = self._read(self.ledger)
        with self.assertRaises(ValueError):
            self._batch(["j-1", "j-2"])
        # The whole batch fails: j-2 was not traded and no binding was
        # persisted.
        self.assertEqual(self._read(self.ledger), ledger_bytes)
        self.assertFalse(os.path.exists(self.sidecar))

    def test_batch_trade_bars_later_requests(self) -> None:
        self._submit("j-1")
        self._submit("j-2")
        self._seed_two_regions()
        self._batch(["j-1"])
        # A single-job clearing of a batch-traded job is refused.
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.supply, self.signals, self.ledger,
                       "j-1", "t-1", 50)
        # So is another batch naming the same job under a new key.
        with self.assertRaises(ValueError):
            self._batch(["j-1", "j-2"], key="b-2")
        # But j-2 alone still clears.
        result, created = self._batch(["j-2"], key="b-2")
        self.assertTrue(created)
        self.assertEqual(sorted(result["trades"]), ["j-1", "j-2"])

    def test_cancelled_job_raises_and_releases_capacity(self) -> None:
        self._submit("j-1")
        self._submit("j-2")
        publish_resource(self.supply, _resource("r-1", capacity=15), "k1")
        publish_signal(self.signals, _signal(), "s1")
        clear_live(self.jobs, self.supply, self.signals, self.ledger,
                   "j-1", "t-1", 10)
        cancel(self.jobs, self.supply, self.ledger, self.dispatch,
               self.cancellations, "j-1", "c-1", 20, "no longer needed")
        # A cancelled job cannot be traded again by a batch either.
        with self.assertRaises(ValueError):
            self._batch(["j-1"], cancellations=self.cancellations)
        # Without the cancellation ledger the occupancy still counts.
        result, _ = self._batch(["j-2"], key="b-2")
        self.assertEqual(result["unallocated"], ["j-2"])
        # With it, the released capacity is usable again.
        result, _ = self._batch(["j-2"], key="b-3",
                                cancellations=self.cancellations)
        self.assertEqual(result["unallocated"], [])
        self.assertEqual(result["trades"]["j-2"]["resource_id"], "r-1")

    # -- completion lifecycle ------------------------------------------------

    def _finish_job(self) -> None:
        clear_live(self.jobs, self.supply, self.signals, self.ledger,
                   "j-1", "t-1", 10)
        dispatch_module.commit(self.jobs, self.supply, self.ledger,
                               self.dispatch, "j-1", "d-1", 10)
        dispatch_module.claim(self.dispatch, "j-1", "c-1", "owner-1", 30,
                              60)
        execution_module.plan(self.jobs, self.supply, self.ledger,
                              self.dispatch, self.execution, "j-1", "p-1",
                              "owner-1", None, 61)
        execution_module.record(self.execution, "j-1", 1, "s-1",
                                "owner-1", "stage", "succeeded", "staged",
                                62)
        execution_module.record(self.execution, "j-1", 1, "s-2",
                                "owner-1", "start", "succeeded", "started",
                                63)
        execution_sync_module.run(self.execution, self.dispatch, self.sync,
                                  "sync-1", "b-1", 70, 50)
        record, created = complete(self.jobs, self.supply, self.signals,
                                   self.ledger, self.dispatch,
                                   self.execution, self.completions,
                                   "j-1", "x-1", 70, "succeeded", 90, 120)
        self.assertTrue(created)
        return record

    def test_completed_job_is_terminated_and_releases_capacity(self) -> None:
        self._submit("j-1")
        self._submit("j-2")
        publish_resource(self.supply, _resource("r-1", capacity=15), "k1")
        publish_signal(self.signals, _signal(), "s1")
        self._finish_job()
        # A completed job is terminated for any later batch.
        with self.assertRaises(ValueError):
            self._batch(["j-1"], at=80, completions=self.completions)
        # Without the completion ledger the occupancy still counts.
        result, _ = self._batch(["j-2"], key="b-2", at=80)
        self.assertEqual(result["unallocated"], ["j-2"])
        # With it, the completed job's capacity is free again.
        result, _ = self._batch(["j-2"], key="b-3", at=80,
                                completions=self.completions)
        self.assertEqual(result["unallocated"], [])
        self.assertEqual(result["trades"]["j-2"]["resource_id"], "r-1")

    # -- atomicity -----------------------------------------------------------

    def test_failed_batch_commit_restores_the_clearing_ledger(self) -> None:
        self._submit("j-1")
        self._submit("j-2")
        self._seed_two_regions()
        clear_live(self.jobs, self.supply, self.signals, self.ledger,
                   "j-1", "t-1", 40)
        ledger_bytes = self._read(self.ledger)

        import carbon_market.market as market_module
        real_commit = market_module._commit_clear

        def failing_commit(realpath: str, payload: bytes,
                           old_bytes: bytes | None) -> None:
            if realpath.endswith(".batches"):
                raise OSError("injected sync failure")
            real_commit(realpath, payload, old_bytes)

        with unittest.mock.patch.object(market_module, "_commit_clear",
                                        failing_commit):
            with self.assertRaises(OSError):
                self._batch(["j-2"])
        # The trades commit was rolled back and no batch binding
        # survived, so the pre-call content is exactly preserved.
        self.assertEqual(self._read(self.ledger), ledger_bytes)
        self.assertFalse(os.path.exists(self.sidecar))
        # The batch can be retried successfully afterwards.
        result, created = self._batch(["j-2"])
        self.assertTrue(created)
        self.assertEqual(result["trades"]["j-2"]["resource_id"], "r-2")

    # -- coexistence with the existing readers -------------------------------

    def test_single_clears_share_one_capacity_pool(self) -> None:
        self._submit("j-1")
        self._submit("j-2", work=15)
        self._submit("j-3", work=15)
        self._seed_two_regions(cap1=10, cap2=40)
        self._batch(["j-1"])  # j-1 -> r-1 (cleaner), 10 of 10 used
        # r-1 is full; j-2 only fits r-2.
        trade, created = clear_live(self.jobs, self.supply, self.signals,
                                    self.ledger, "j-2", "t-2", 50)
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-2")
        # The static clear reads the same ledger with the live trades.
        trade, created = clear(self.jobs, self.supply, self.ledger,
                               "j-3", "t-3", 50)
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-2")

    def test_dispatch_commits_over_a_batch_trade(self) -> None:
        self._submit("j-1")
        self._seed_two_regions()
        self._batch(["j-1"])
        decision, created = dispatch_module.commit(
            self.jobs, self.supply, self.ledger, self.dispatch,
            "j-1", "d-1", 60)
        self.assertTrue(created)
        self.assertEqual(decision["job_id"], "j-1")

    def test_concurrent_batch_and_single_clear_never_oversell(self) -> None:
        for i in range(1, 6):
            self._submit(f"j-{i}")
        publish_resource(self.supply, _resource("r-1", capacity=25), "k1")
        publish_signal(self.signals, _signal(), "s1")
        errors: list[BaseException] = []
        barrier = threading.Barrier(3)

        def run_batch() -> None:
            try:
                barrier.wait()
                self._batch(["j-1", "j-2", "j-3", "j-4"])
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def run_single(job_id: str) -> None:
            try:
                barrier.wait()
                clear_live(self.jobs, self.supply, self.signals,
                           self.ledger, job_id, f"t-{job_id}", 50)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=run_batch),
                   threading.Thread(target=run_single, args=("j-5",)),
                   threading.Thread(target=run_single, args=("j-5",))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        # The duplicate single clear of j-5 may fail with ValueError
        # (already traded) and either clear may find the capacity gone
        # (LookupError); nothing else may go wrong.
        for exc in errors:
            self.assertIsInstance(exc, (ValueError, LookupError))
        ledger = self._ledger_json()
        trades = ledger["trades"]
        self.assertEqual(len(trades), len(set(trades)))
        total = sum(trade["work"] for trade in trades.values())
        self.assertLessEqual(total, 25)
        self.assertGreaterEqual(len(trades), 2)
        # The ledger stays canonical and readable for both clear paths.
        self._submit("j-6", work=1)
        clear_live(self.jobs, self.supply, self.signals, self.ledger,
                   "j-6", "t-6", 50)


if __name__ == "__main__":
    unittest.main()
