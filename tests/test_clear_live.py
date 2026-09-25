from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import jobs as jobs_module
from carbon_market import market as market_module
from carbon_market.jobs import submit
from carbon_market.market import clear_live
from carbon_market.resources import feasible_live, publish
from carbon_market.signals import publish as publish_signal


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


def _signal(region: str = "eu-north", **overrides: object) -> dict[str, object]:
    signal: dict[str, object] = {
        "region": region,
        "observed_at": 0,
        "expires_at": 500,
        "energy_mix": {"a": 10000},
        "unit_cost": 3,
        "carbon_intensity": 7,
    }
    signal.update(overrides)
    return signal


class ClearLiveTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs = os.path.join(self.tmp.name, "jobs.json")
        self.supply = os.path.join(self.tmp.name, "supply.json")
        self.signals = os.path.join(self.tmp.name, "signals.json")
        self.ledger = os.path.join(self.tmp.name, "live.json")

    def _seed_job(self, job: dict[str, object] | None = None,
                  key: str = "jk-1") -> None:
        submit(self.jobs, job if job is not None else _job(), key)

    def _seed_resource(self, resource: dict[str, object] | None = None,
                       key: str = "rk-1") -> None:
        publish(self.supply,
                resource if resource is not None else _resource(), key)

    def _seed_signal(self, signal: dict[str, object] | None = None,
                     key: str = "sk-1") -> None:
        publish_signal(self.signals,
                       signal if signal is not None else _signal(), key)

    def _seed(self) -> None:
        self._seed_job()
        self._seed_resource()
        self._seed_signal()

    def test_creates_ledger_and_returns_trade(self) -> None:
        self._seed()
        trade, created = clear_live(self.jobs, self.supply, self.signals,
                                    self.ledger, "j-1", "k-1", 50)
        self.assertTrue(created)
        self.assertEqual(trade["job_id"], "j-1")
        self.assertEqual(trade["at"], 50)
        self.assertEqual(trade["work"], 10)
        self.assertEqual(trade["resource_id"], "r-1")
        self.assertEqual(trade["version"], 1)
        self.assertEqual(trade["selection"],
                         {"resource_id": "r-1", "version": 1})
        self.assertEqual(list(trade.keys()),
                         ["job_id", "at", "work", "resource_id", "version",
                          "candidates", "selection"])
        candidate = trade["candidates"][0]
        self.assertEqual(list(candidate.keys()),
                         ["resource", "signal", "total_cost",
                          "total_carbon"])
        self.assertEqual(candidate["total_cost"], 30)
        self.assertEqual(candidate["total_carbon"], 70)
        self.assertEqual(candidate["resource"]["resource_id"], "r-1")
        self.assertEqual(candidate["signal"]["version"], 1)
        self.assertEqual(candidate["signal"]["region"], "eu-north")

        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()),
                         ["version", "trades", "idempotency", "audit"])
        self.assertEqual(data["version"], 3)
        self.assertEqual(data["idempotency"],
                         {"k-1": {"job_id": "j-1", "at": 50}})
        self.assertEqual(data["audit"], {"k-1": {
            "key": "k-1", "job_id": "j-1", "at": 50,
            "resource_id": "r-1", "version": 1, "signal_version": 1}})

    def test_candidates_match_feasible_live_semantics(self) -> None:
        self._seed()
        ranked = feasible_live(self.jobs, self.supply, self.signals,
                               "j-1", 50)
        trade, _ = clear_live(self.jobs, self.supply, self.signals,
                              self.ledger, "j-1", "k", 50)
        self.assertEqual(trade["candidates"], ranked)

    def test_picks_greenest_live_signal_then_cheapest(self) -> None:
        self._seed_job()
        # Every resource is statically dirtier/more expensive than the
        # next; the regional live signals decide the winner instead.
        publish(self.supply,
                _resource("r-a", region="eu-north",
                          residency=["eu-north"],
                          unit_cost=99, carbon_intensity=99), "o1")
        publish(self.supply,
                _resource("r-b", region="us-west",
                          residency=["eu-north", "us-west"],
                          unit_cost=50, carbon_intensity=50), "o2")
        publish_signal(self.signals,
                       _signal("eu-north", unit_cost=4, carbon_intensity=2),
                       "s1")
        publish_signal(self.signals,
                       _signal("us-west", unit_cost=1, carbon_intensity=5),
                       "s2")
        trade, _ = clear_live(self.jobs, self.supply, self.signals,
                              self.ledger, "j-1", "k", 50)
        # eu-north's signal carbon 2 beats us-west's 5 despite the static
        # supply records saying the opposite.
        self.assertEqual(trade["resource_id"], "r-a")
        self.assertEqual(
            [c["resource"]["resource_id"] for c in trade["candidates"]],
            ["r-a", "r-b"])

    def test_capacity_deducts_booked_work_per_version(self) -> None:
        submit(self.jobs, _job("j-1"), "jk1")
        submit(self.jobs, _job("j-2"), "jk2")
        publish(self.supply,
                _resource("r-small", region="eu-north",
                          residency=["eu-north"], capacity=15), "o1")
        publish(self.supply,
                _resource("r-big", region="us-west",
                          residency=["eu-north", "us-west"],
                          capacity=500), "o2")
        self._seed_signal(_signal("eu-north", carbon_intensity=1,
                                  unit_cost=1))
        publish_signal(self.signals,
                       _signal("us-west", carbon_intensity=2, unit_cost=1),
                       "sk2")
        first, _ = clear_live(self.jobs, self.supply, self.signals,
                              self.ledger, "j-1", "k1", 50)
        self.assertEqual(first["resource_id"], "r-small")
        # r-small has 5 capacity left, so j-2 spills to r-big.
        second, _ = clear_live(self.jobs, self.supply, self.signals,
                               self.ledger, "j-2", "k2", 50)
        self.assertEqual(second["resource_id"], "r-big")

    def test_replay_returns_stored_trade_without_writing(self) -> None:
        self._seed()
        first, created_first = clear_live(
            self.jobs, self.supply, self.signals, self.ledger, "j-1", "k", 50)
        raw = Path(self.ledger).read_bytes()
        replay, created_replay = clear_live(
            self.jobs, self.supply, self.signals, self.ledger, "j-1", "k", 50)
        self.assertTrue(created_first)
        self.assertFalse(created_replay)
        self.assertEqual(replay, first)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)

    def test_later_signal_publication_does_not_change_trade(self) -> None:
        self._seed()
        first, _ = clear_live(self.jobs, self.supply, self.signals,
                              self.ledger, "j-1", "k", 50)
        publish_signal(self.signals,
                       _signal(observed_at=20, expires_at=400,
                               unit_cost=0, carbon_intensity=0), "sk2")
        replay, created = clear_live(
            self.jobs, self.supply, self.signals, self.ledger, "j-1", "k", 50)
        self.assertFalse(created)
        self.assertEqual(replay, first)
        self.assertEqual(replay["candidates"][0]["signal"]["version"], 1)

    def test_same_key_with_changed_request_raises(self) -> None:
        self._seed()
        clear_live(self.jobs, self.supply, self.signals,
                   self.ledger, "j-1", "k", 50)
        submit(self.jobs, _job("j-2"), "jk2")
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.supply, self.signals,
                       self.ledger, "j-2", "k", 50)
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.supply, self.signals,
                       self.ledger, "j-1", "k", 51)

    def test_job_traded_under_other_key_raises(self) -> None:
        self._seed()
        clear_live(self.jobs, self.supply, self.signals,
                   self.ledger, "j-1", "k1", 50)
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.supply, self.signals,
                       self.ledger, "j-1", "k2", 50)

    def test_unknown_job_raises_key_error_without_ledger(self) -> None:
        self._seed()
        with self.assertRaises(KeyError):
            clear_live(self.jobs, self.supply, self.signals,
                       self.ledger, "ghost", "k", 50)
        self.assertFalse(os.path.exists(self.ledger))

    def test_no_remaining_resource_raises_lookup_error_without_ledger(
            self) -> None:
        submit(self.jobs, _job(work=1000), "jk")
        self._seed_resource()
        self._seed_signal()
        with self.assertRaises(LookupError):
            clear_live(self.jobs, self.supply, self.signals,
                       self.ledger, "j-1", "k", 50)
        self.assertFalse(os.path.exists(self.ledger))

    def test_no_region_signal_raises_lookup_error_without_ledger(
            self) -> None:
        self._seed_job()
        self._seed_resource()
        # The ledger exists but carries no signal for the resource's
        # region, so no candidate survives and no ledger is created.
        self._seed_signal(_signal("ap-south"), "sk-other")
        with self.assertRaises(LookupError):
            clear_live(self.jobs, self.supply, self.signals,
                       self.ledger, "j-1", "k", 50)
        self.assertFalse(os.path.exists(self.ledger))

    def test_missing_signal_file_raises_file_not_found(self) -> None:
        self._seed_job()
        self._seed_resource()
        with self.assertRaises(FileNotFoundError):
            clear_live(self.jobs, self.supply, self.signals,
                       self.ledger, "j-1", "k", 50)
        self.assertFalse(os.path.exists(self.ledger))

    def test_missing_files_and_parent_raise_file_not_found(self) -> None:
        self._seed()
        with self.assertRaises(FileNotFoundError):
            clear_live(os.path.join(self.tmp.name, "no-jobs.json"),
                       self.supply, self.signals, self.ledger, "j-1", "k", 50)
        with self.assertRaises(FileNotFoundError):
            clear_live(self.jobs, os.path.join(self.tmp.name, "no-supply.json"),
                       self.signals, self.ledger, "j-1", "k", 50)
        with self.assertRaises(FileNotFoundError):
            clear_live(self.jobs, self.supply,
                       os.path.join(self.tmp.name, "no-signals.json"),
                       self.ledger, "j-1", "k", 50)

    def test_missing_ledger_parent_raises_file_not_found(self) -> None:
        self._seed()
        ledger = os.path.join(self.tmp.name, "no-such-dir", "live.json")
        with self.assertRaises(FileNotFoundError):
            clear_live(self.jobs, self.supply, self.signals,
                       ledger, "j-1", "k", 50)
        self.assertFalse(os.path.exists(ledger))

    def test_invalid_arguments_before_files_are_read(self) -> None:
        good = (self.jobs, self.supply, self.signals, self.ledger,
                "j-1", "k", 50)
        for index in range(6):
            for bad in ("", 123, None):
                args = list(good)
                args[index] = bad
                with self.subTest(index=index, bad=bad):
                    with self.assertRaises(ValueError):
                        clear_live(*args)  # type: ignore[arg-type]
        for bad in (-1, True, False, 1.5, None, ""):
            with self.subTest(at=bad):
                with self.assertRaises(ValueError):
                    clear_live(self.jobs, self.supply, self.signals,
                               self.ledger, "j-1", "k", bad)  # type: ignore[arg-type]

    def test_paths_must_be_distinct_real_locations(self) -> None:
        self._seed()
        aliases = [
            (self.jobs, self.supply, self.signals, self.jobs),
            (self.jobs, self.supply, self.signals, self.supply),
            (self.jobs, self.supply, self.signals, self.signals),
            (self.jobs, self.supply, self.jobs, self.ledger),
        ]
        for paths in aliases:
            with self.subTest(paths=paths):
                with self.assertRaises(ValueError):
                    clear_live(paths[0], paths[1], paths[2], paths[3],
                               "j-1", "k", 50)

    def test_invalid_signal_file_raises_value_error(self) -> None:
        self._seed_job()
        self._seed_resource()
        Path(self.signals).write_text("{not json", encoding="utf-8")
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.supply, self.signals,
                       self.ledger, "j-1", "k", 50)

    def test_non_canonical_signal_file_raises_value_error(self) -> None:
        self._seed()
        data = json.loads(Path(self.signals).read_text(encoding="utf-8"))
        Path(self.signals).write_text(json.dumps(data, indent=2),
                                      encoding="utf-8")
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.supply, self.signals,
                       self.ledger, "j-1", "k", 50)

    def test_invalid_ledger_structures_raise_value_error(self) -> None:
        self._seed()
        good, _ = clear_live(self.jobs, self.supply, self.signals,
                             self.ledger, "j-1", "k", 50)
        winner = good["resource_id"]
        wver = good["version"]
        sver = good["candidates"][0]["signal"]["version"]
        trade = {
            "job_id": "j-1", "at": 50, "work": 10,
            "resource_id": winner, "version": wver,
            "candidates": good["candidates"],
            "selection": {"resource_id": winner, "version": wver},
        }
        event = {"key": "k", "job_id": "j-1", "at": 50,
                 "resource_id": winner, "version": wver,
                 "signal_version": sver}
        binding = {"job_id": "j-1", "at": 50}
        bad_payloads = [
            [],
            {},
            {"version": 3, "trades": {}, "idempotency": {}, "audit": {},
             "extra": 1},
            {"version": 2, "trades": {}, "idempotency": {}, "audit": {}},
            {"version": True, "trades": {}, "idempotency": {}, "audit": {}},
            {"version": 3, "trades": {"j-1": trade}, "idempotency": {},
             "audit": {}},
            {"version": 3, "trades": {"j-1": dict(trade, at=51)},
             "idempotency": {"k": binding}, "audit": {"k": event}},
            {"version": 3, "trades": {"j-1": trade},
             "idempotency": {"k": dict(binding, at=51)},
             "audit": {"k": event}},
            {"version": 3, "trades": {"j-1": trade},
             "idempotency": {"k": binding},
             "audit": {"k": dict(event, signal_version=sver + 1)}},
            {"version": 3, "trades": {"j-1": trade},
             "idempotency": {"k": binding}, "audit": {}},
        ]
        for payload in bad_payloads:
            with self.subTest(payload=payload):
                Path(self.ledger).write_bytes(
                    json.dumps(payload).encode("utf-8"))
                with self.assertRaises(ValueError):
                    clear_live(self.jobs, self.supply, self.signals,
                               self.ledger, "j-1", "k2", 50)

    def test_replay_keeps_frozen_signal_while_live_query_sees_new_one(
            self) -> None:
        # The frozen signal record is inside the ledger; replaying does
        # not re-resolve a live signal even when a newer one is now
        # observable at the same evaluation moment.
        self._seed()
        clear_live(self.jobs, self.supply, self.signals,
                   self.ledger, "j-1", "k", 50)
        publish_signal(self.signals,
                       _signal(observed_at=20, expires_at=400,
                               unit_cost=1, carbon_intensity=1), "sk2")
        live = feasible_live(self.jobs, self.supply, self.signals, "j-1", 50)
        self.assertEqual(live[0]["signal"]["version"], 2)
        replay, created = clear_live(
            self.jobs, self.supply, self.signals, self.ledger, "j-1", "k", 50)
        self.assertFalse(created)
        self.assertEqual(replay["candidates"][0]["signal"]["version"], 1)

    def test_concurrent_clears_serialize_without_overselling(self) -> None:
        publish(self.supply, _resource(capacity=10_000), "rk")
        self._seed_signal(_signal())
        for index in range(30):
            submit(self.jobs, _job(f"j{index:03d}"), f"jk{index}")
        relative = "live.json"
        cwd = os.getcwd()
        os.chdir(self.tmp.name)
        try:
            paths = [
                relative,
                os.path.abspath(relative),
                os.path.join(self.tmp.name, ".", "live.json"),
            ]
            results: list[tuple[dict[str, object], bool]] = []
            errors: list[BaseException] = []
            lock = threading.Lock()

            def worker(index: int) -> None:
                try:
                    outcome = clear_live(self.jobs, self.supply, self.signals,
                                         paths[index % len(paths)],
                                         f"j{index:03d}", f"k{index:03d}", 50)
                    with lock:
                        results.append(outcome)
                except BaseException as exc:  # noqa: BLE001 - report all
                    with lock:
                        errors.append(exc)

            threads = [threading.Thread(target=worker, args=(i,))
                       for i in range(30)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
            self.assertEqual(errors, [])
            self.assertEqual(len(results), 30)
            self.assertTrue(all(created for _, created in results))
            data = json.loads(Path(relative).read_text(encoding="utf-8"))
            self.assertEqual(len(data["trades"]), 30)
            self.assertEqual(len(data["idempotency"]), 30)
            self.assertEqual(len(data["audit"]), 30)
        finally:
            os.chdir(cwd)

    def test_failed_directory_sync_restores_previous_bytes(self) -> None:
        self._seed()
        clear_live(self.jobs, self.supply, self.signals,
                   self.ledger, "j-1", "k1", 50)
        before = Path(self.ledger).read_bytes()
        original = market_module._fsync_directory

        def failing(directory: str) -> None:
            raise OSError("simulated sync failure")

        market_module._fsync_directory = failing
        try:
            submit(self.jobs, _job("j-2"), "jk2")
            with self.assertRaises(OSError):
                clear_live(self.jobs, self.supply, self.signals,
                           self.ledger, "j-2", "k2", 50)
        finally:
            market_module._fsync_directory = original
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_failed_first_commit_leaves_no_file(self) -> None:
        self._seed()
        original = market_module._fsync_directory

        def failing(directory: str) -> None:
            raise OSError("simulated sync failure")

        market_module._fsync_directory = failing
        try:
            with self.assertRaises(OSError):
                clear_live(self.jobs, self.supply, self.signals,
                           self.ledger, "j-1", "k1", 50)
        finally:
            market_module._fsync_directory = original
        self.assertFalse(os.path.exists(self.ledger))
        self.assertEqual([n for n in os.listdir(self.tmp.name)
                          if n.endswith(".tmp")], [])


if __name__ == "__main__":
    unittest.main()
