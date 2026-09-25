from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import market as market_module
from carbon_market.jobs import submit as submit_job
from carbon_market.market import clear
from carbon_market.resources import feasible, publish


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


class ClearTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs = os.path.join(self.tmp.name, "jobs.json")
        self.supply = os.path.join(self.tmp.name, "supply.json")
        self.ledger = os.path.join(self.tmp.name, "ledger.json")

    def _seed(self, job: dict[str, object] | None = None,
              resource: dict[str, object] | None = None,
              job_key: str = "jk-1",
              resource_key: str = "rk-1") -> None:
        submit_job(self.jobs, job if job is not None else _job(), job_key)
        publish(self.supply,
                resource if resource is not None else _resource(),
                resource_key)

    def test_creates_ledger_and_returns_record(self) -> None:
        self._seed()
        record, created = clear(self.jobs, self.supply, self.ledger,
                                "j-1", "k-1", 50)
        self.assertTrue(created)
        self.assertEqual(record["job_id"], "j-1")
        self.assertEqual(record["at"], 50)
        self.assertEqual(record["work"], 10)
        self.assertEqual(record["selected"],
                         {"resource_id": "r-1", "version": 1})
        self.assertEqual(list(record.keys()),
                         ["job_id", "at", "work", "candidates", "selected"])
        self.assertEqual(len(record["candidates"]), 1)
        entry = record["candidates"][0]
        self.assertEqual(list(entry.keys()),
                         ["resource", "total_cost", "total_carbon"])
        self.assertEqual(entry["resource"]["resource_id"], "r-1")
        self.assertEqual(entry["resource"]["version"], 1)
        self.assertEqual(entry["total_cost"], 30)
        self.assertEqual(entry["total_carbon"], 70)

        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()),
                         ["version", "cleared", "idempotency", "audit"])
        self.assertEqual(data["version"], 1)
        stored = data["cleared"]["j-1"]
        self.assertEqual(list(stored.keys()),
                         ["job_id", "at", "work", "candidates", "selected"])
        self.assertEqual(data["idempotency"], {"k-1": "j-1"})
        self.assertEqual(data["audit"], {"k-1": {
            "key": "k-1", "job_id": "j-1",
            "resource_id": "r-1", "version": 1}})

    def test_compact_utf8_and_sorted_keys(self) -> None:
        submit_job(self.jobs, _job("j-b"), "jk-b")
        submit_job(self.jobs, _job("j-a"), "jk-a")
        publish(self.supply, _resource("r-中", capacity=10_000), "rk")
        clear(self.jobs, self.supply, self.ledger, "j-b", "键-b", 50)
        clear(self.jobs, self.supply, self.ledger, "j-a", "键-a", 50)
        raw = Path(self.ledger).read_text(encoding="utf-8")
        self.assertIn("r-中", raw)
        self.assertIn("键-b", raw)
        self.assertNotIn(" ", raw.split("\n", 1)[0])
        self.assertNotIn("\\u", raw)
        data = json.loads(raw)
        self.assertEqual(list(data["cleared"].keys()), ["j-a", "j-b"])
        self.assertEqual(list(data["idempotency"].keys()), ["键-a", "键-b"])
        self.assertEqual(list(data["audit"].keys()), ["键-a", "键-b"])

    def test_candidates_match_feasible_ranking(self) -> None:
        submit_job(self.jobs, _job(), "jk")
        publish(self.supply,
                _resource("r-1", carbon_intensity=7, unit_cost=3), "o1")
        publish(self.supply,
                _resource("r-2", carbon_intensity=2, unit_cost=9), "o2")
        publish(self.supply,
                _resource("r-3", carbon_intensity=2, unit_cost=4), "o3")
        record, _ = clear(self.jobs, self.supply, self.ledger,
                          "j-1", "k", 50)
        expected = feasible(self.jobs, self.supply, "j-1", 50)
        self.assertEqual(record["candidates"], expected)
        self.assertEqual([e["resource"]["resource_id"]
                          for e in record["candidates"]],
                         ["r-3", "r-2", "r-1"])
        self.assertEqual(record["selected"],
                         {"resource_id": "r-3", "version": 1})

    def test_uses_highest_version_valid_at_evaluation_time(self) -> None:
        submit_job(self.jobs, _job(), "jk")
        publish(self.supply,
                _resource("r-1", start=0, end=100, carbon_intensity=9), "o1")
        publish(self.supply,
                _resource("r-1", start=200, end=300, carbon_intensity=1), "o2")
        early, _ = clear(self.jobs, self.supply, self.ledger,
                         "j-1", "k1", 50)
        self.assertEqual(early["selected"],
                         {"resource_id": "r-1", "version": 1})
        submit_job(self.jobs, _job("j-2"), "jk2")
        late, _ = clear(self.jobs, self.supply, self.ledger,
                        "j-2", "k2", 250)
        self.assertEqual(late["selected"],
                         {"resource_id": "r-1", "version": 2})
        # A moment outside every window leaves no resource at all.
        submit_job(self.jobs, _job("j-3"), "jk3")
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.ledger, "j-3", "k3", 150)

    def test_capacity_deducts_per_resource_version(self) -> None:
        submit_job(self.jobs, _job("j-1", work=100), "jk1")
        submit_job(self.jobs, _job("j-2", work=100), "jk2")
        publish(self.supply,
                _resource("r-small", capacity=150, carbon_intensity=1), "o1")
        publish(self.supply,
                _resource("r-big", capacity=500, carbon_intensity=2), "o2")
        first, _ = clear(self.jobs, self.supply, self.ledger,
                         "j-1", "k1", 50)
        self.assertEqual(first["selected"]["resource_id"], "r-small")
        # r-small has only 50 left, so j-2 spills to r-big.
        second, _ = clear(self.jobs, self.supply, self.ledger,
                          "j-2", "k2", 50)
        self.assertEqual(second["selected"]["resource_id"], "r-big")

    def test_capacity_does_not_cross_versions(self) -> None:
        submit_job(self.jobs, _job("j-1", work=100), "jk1")
        submit_job(self.jobs, _job("j-2", work=100), "jk2")
        publish(self.supply,
                _resource("r-1", capacity=100, start=0, end=100), "o1")
        publish(self.supply,
                _resource("r-1", capacity=100, start=200, end=300), "o2")
        first, _ = clear(self.jobs, self.supply, self.ledger,
                         "j-1", "k1", 50)
        self.assertEqual(first["selected"]["version"], 1)
        # Version 2 has its own full capacity even though version 1 is full.
        second, _ = clear(self.jobs, self.supply, self.ledger,
                          "j-2", "k2", 250)
        self.assertEqual(second["selected"],
                         {"resource_id": "r-1", "version": 2})
        # Booking version 2 did not free or consume version 1.
        submit_job(self.jobs, _job("j-3", work=1), "jk3")
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.ledger, "j-3", "k3", 50)

    def test_oversell_is_rejected(self) -> None:
        submit_job(self.jobs, _job(work=100), "jk")
        publish(self.supply, _resource(capacity=150), "rk")
        first, created_first = clear(self.jobs, self.supply, self.ledger,
                                     "j-1", "k", 50)
        self.assertTrue(created_first)
        self.assertEqual(first["work"], 100)
        submit_job(self.jobs, _job("j-2", work=100), "jk2")
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.ledger, "j-2", "k2", 50)

    def test_replay_returns_record_without_reselecting_or_rewriting(self) -> None:
        self._seed()
        first, created_first = clear(self.jobs, self.supply, self.ledger,
                                     "j-1", "k", 50)
        before = Path(self.ledger).read_bytes()
        # Publish a strictly greener version; a fresh decision would pick
        # it, but the replay must return the frozen snapshot unchanged.
        publish(self.supply,
                _resource("r-1", start=10, end=500, carbon_intensity=0,
                          unit_cost=0), "rk2")
        replay, created_replay = clear(self.jobs, self.supply, self.ledger,
                                       "j-1", "k", 50)
        self.assertFalse(created_replay)
        self.assertEqual(replay, first)
        self.assertEqual(replay["candidates"][0]["resource"]["carbon_intensity"],
                         7)
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_replay_consumes_no_capacity(self) -> None:
        submit_job(self.jobs, _job("j-1", work=100), "jk1")
        submit_job(self.jobs, _job("j-2", work=50), "jk2")
        publish(self.supply, _resource(capacity=150), "rk")
        clear(self.jobs, self.supply, self.ledger, "j-1", "k1", 50)
        replay, created = clear(self.jobs, self.supply, self.ledger,
                                "j-1", "k1", 50)
        self.assertFalse(created)
        self.assertEqual(replay["selected"]["resource_id"], "r-1")
        second, _ = clear(self.jobs, self.supply, self.ledger,
                          "j-2", "k2", 50)
        self.assertEqual(second["selected"]["resource_id"], "r-1")

    def test_same_key_different_request_raises(self) -> None:
        submit_job(self.jobs, _job("j-1"), "jk1")
        submit_job(self.jobs, _job("j-2"), "jk2")
        publish(self.supply, _resource(capacity=10_000), "rk")
        clear(self.jobs, self.supply, self.ledger, "j-1", "k", 50)
        before = Path(self.ledger).read_bytes()
        with self.assertRaises(ValueError):
            clear(self.jobs, self.supply, self.ledger, "j-2", "k", 50)
        with self.assertRaises(ValueError):
            clear(self.jobs, self.supply, self.ledger, "j-1", "k", 60)
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_job_cleared_under_other_key_raises(self) -> None:
        self._seed()
        clear(self.jobs, self.supply, self.ledger, "j-1", "k1", 50)
        with self.assertRaises(ValueError):
            clear(self.jobs, self.supply, self.ledger, "j-1", "k2", 50)

    def test_unknown_job_raises_key_error_without_ledger(self) -> None:
        self._seed()
        with self.assertRaises(KeyError):
            clear(self.jobs, self.supply, self.ledger, "j-ghost", "k", 50)
        self.assertFalse(os.path.exists(self.ledger))

    def test_no_feasible_resource_raises_lookup_error_without_ledger(self) -> None:
        submit_job(self.jobs, _job(work=1000), "jk")
        publish(self.supply, _resource(), "rk")
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.ledger, "j-1", "k", 50)
        self.assertFalse(os.path.exists(self.ledger))

    def test_missing_inputs_raise_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            clear(self.jobs, self.supply, self.ledger, "j-1", "k", 50)
        submit_job(self.jobs, _job(), "jk")
        with self.assertRaises(FileNotFoundError):
            clear(self.jobs, self.supply, self.ledger, "j-1", "k", 50)
        publish(self.supply, _resource(), "rk")
        missing = os.path.join(self.tmp.name, "no-such-dir", "jobs.json")
        with self.assertRaises(FileNotFoundError):
            clear(missing, self.supply, self.ledger, "j-1", "k", 50)

    def test_missing_ledger_parent_raises_file_not_found(self) -> None:
        self._seed()
        ledger = os.path.join(self.tmp.name, "no-such-dir", "ledger.json")
        with self.assertRaises(FileNotFoundError):
            clear(self.jobs, self.supply, ledger, "j-1", "k", 50)
        self.assertFalse(os.path.exists(ledger))

    def test_invalid_arguments(self) -> None:
        self._seed()
        good = (self.jobs, self.supply, self.ledger, "j-1", "k", 50)
        for index in range(5):
            for bad in ("", 123, None):
                args = list(good)
                args[index] = bad
                with self.subTest(index=index, bad=bad):
                    with self.assertRaises(ValueError):
                        clear(*args)  # type: ignore[arg-type]
        for bad_at in (-1, True, False, 1.5, "50", None):
            with self.subTest(bad_at=bad_at):
                with self.assertRaises(ValueError):
                    clear(self.jobs, self.supply, self.ledger,
                          "j-1", "k", bad_at)  # type: ignore[arg-type]

    def test_paths_must_be_distinct_real_paths(self) -> None:
        self._seed()
        here = os.getcwd()
        os.chdir(self.tmp.name)
        try:
            cases = [
                # jobs and supply name the same real file
                ("jobs.json", self.jobs, self.ledger),
                # supply and ledger name the same real file
                (self.jobs, "supply.json", self.supply),
                # ledger and jobs name the same real file
                ("jobs.json", self.supply, self.jobs),
                # relative/absolute spellings of the same jobs file
                (os.path.join(".", "jobs.json"),
                 os.path.abspath("jobs.json"), self.ledger),
            ]
            for jobs, supply, ledger in cases:
                with self.subTest(case=(jobs, supply, ledger)):
                    with self.assertRaises(ValueError):
                        clear(jobs, supply, ledger, "j-1", "k", 50)
        finally:
            os.chdir(here)

    def test_invalid_files_raise_value_error(self) -> None:
        self._seed()
        for bad in ("{not json", '{"version": -0}', '{"version": NaN}'):
            Path(self.supply).write_text(bad, encoding="utf-8")
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    clear(self.jobs, self.supply, self.ledger,
                          "j-1", "k", 50)
            # Restore a valid supply file for the next iteration.
            os.unlink(self.supply)
            publish(self.supply, _resource(), "rk")
        Path(self.jobs).write_text('{"version": -0}', encoding="utf-8")
        with self.assertRaises(ValueError):
            clear(self.jobs, self.supply, self.ledger, "j-1", "k", 50)

    def test_non_canonical_supply_bytes_raise_value_error(self) -> None:
        self._seed()
        raw = Path(self.supply).read_bytes()
        Path(self.supply).write_bytes(raw + b"\n")
        with self.assertRaises(ValueError):
            clear(self.jobs, self.supply, self.ledger, "j-1", "k", 50)

    def test_register_file_is_invalid_structure(self) -> None:
        from carbon_market.jobs import register as register_job

        register_path = os.path.join(self.tmp.name, "registry.json")
        register_job(register_path, {
            "job_id": "j-1", "deadline": 100, "energy_wh": 250,
            "residency_regions": ["eu-north"],
        }, "rk-1")
        publish(self.supply, _resource(), "rk")
        with self.assertRaises(ValueError):
            clear(register_path, self.supply, self.ledger, "j-1", "k", 50)

    def test_corrupt_ledger_raises_and_keeps_bytes(self) -> None:
        self._seed()
        clear(self.jobs, self.supply, self.ledger, "j-1", "k1", 50)
        submit_job(self.jobs, _job("j-2"), "jk2")
        for bad in ("{not json", '{"version": -0}'):
            Path(self.ledger).write_text(bad, encoding="utf-8")
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    clear(self.jobs, self.supply, self.ledger,
                          "j-2", "k2", 50)
                self.assertEqual(Path(self.ledger).read_text(encoding="utf-8"),
                                 bad)

    def test_invalid_ledger_structures_raise_value_error(self) -> None:
        submit_job(self.jobs, _job(), "jk")
        publish(self.supply, _resource(), "rk")
        record9 = {"resource_id": "r-1", "version": 1, "region": "eu-north",
                   "capacity": 100, "start": 0, "end": 500, "unit_cost": 3,
                   "carbon_intensity": 7, "residency": ["eu-north"]}
        candidate = {"resource": record9, "total_cost": 30,
                     "total_carbon": 70}
        good_record = {"job_id": "j-1", "at": 50, "work": 10,
                       "candidates": [candidate],
                       "selected": {"resource_id": "r-1", "version": 1}}
        event = {"key": "k", "job_id": "j-1", "resource_id": "r-1",
                 "version": 1}
        bad_payloads = [
            [],
            {},
            {"version": 1, "cleared": {}, "idempotency": {}, "audit": {},
             "extra": 1},
            {"version": 2, "cleared": {}, "idempotency": {}, "audit": {}},
            {"version": 1, "cleared": [], "idempotency": {}, "audit": {}},
            {"version": 1,
             "cleared": {"j-1": dict(good_record, at=True)},
             "idempotency": {"k": "j-1"}, "audit": {"k": event}},
            {"version": 1,
             "cleared": {"j-1": dict(good_record, candidates=[])},
             "idempotency": {"k": "j-1"}, "audit": {"k": event}},
            {"version": 1,
             "cleared": {"j-1": dict(
                 good_record,
                 selected={"resource_id": "r-1", "version": 2})},
             "idempotency": {"k": "j-1"}, "audit": {"k": event}},
            {"version": 1,
             "cleared": {"j-1": dict(
                 good_record,
                 candidates=[dict(candidate, total_cost=31)])},
             "idempotency": {"k": "j-1"}, "audit": {"k": event}},
            {"version": 1, "cleared": {"j-1": good_record},
             "idempotency": {"other": "j-1"}, "audit": {"other": event}},
            {"version": 1, "cleared": {"j-1": good_record},
             "idempotency": {"k": "j-1"},
             "audit": {"k": dict(event, version=2)}},
            {"version": 1, "cleared": {"ghost": good_record},
             "idempotency": {"k": "ghost"},
             "audit": {"k": dict(event, job_id="ghost")}},
        ]
        for payload in bad_payloads:
            with self.subTest(payload=payload):
                Path(self.ledger).write_text(
                    json.dumps(payload, ensure_ascii=False) + "\n",
                    encoding="utf-8")
                with self.assertRaises(ValueError):
                    clear(self.jobs, self.supply, self.ledger,
                          "j-1", "k2", 50)

    def test_seeded_ledger_replays_and_counts_capacity(self) -> None:
        submit_job(self.jobs, _job("j-1", work=100), "jk1")
        submit_job(self.jobs, _job("j-2", work=100), "jk2")
        publish(self.supply, _resource(capacity=150), "rk")
        record9 = {"resource_id": "r-1", "version": 1, "region": "eu-north",
                   "capacity": 150, "start": 0, "end": 500, "unit_cost": 3,
                   "carbon_intensity": 7, "residency": ["eu-north"]}
        Path(self.ledger).write_text(
            json.dumps({
                "version": 1,
                "cleared": {"j-1": {
                    "job_id": "j-1", "at": 50, "work": 100,
                    "candidates": [{"resource": record9, "total_cost": 300,
                                    "total_carbon": 700}],
                    "selected": {"resource_id": "r-1", "version": 1}}},
                "idempotency": {"k1": "j-1"},
                "audit": {"k1": {"key": "k1", "job_id": "j-1",
                                  "resource_id": "r-1", "version": 1}},
            }, ensure_ascii=False, separators=(",", ":")) + "\n",
            encoding="utf-8")
        replay, created = clear(self.jobs, self.supply, self.ledger,
                                "j-1", "k1", 50)
        self.assertFalse(created)
        self.assertEqual(replay["selected"]["resource_id"], "r-1")
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.ledger, "j-2", "k2", 50)

    def test_concurrent_clearings_never_oversell(self) -> None:
        publish(self.supply, _resource(capacity=1000), "rk")
        for index in range(30):
            submit_job(self.jobs, _job(f"j{index:03d}", work=100),
                       f"jk{index:03d}")
        outcomes: list[tuple[str, bool]] = []
        sold_out: list[str] = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def worker(index: int) -> None:
            try:
                _record, created = clear(
                    self.jobs, self.supply, self.ledger,
                    f"j{index:03d}", f"k{index:03d}", 50)
                with lock:
                    outcomes.append((f"j{index:03d}", created))
            except LookupError:
                # Capacity exhausted by another worker is the expected
                # losing outcome, not an error.
                with lock:
                    sold_out.append(f"j{index:03d}")
            except BaseException as exc:  # noqa: BLE001 - report all
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(30)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        self.assertEqual(errors, [])
        # Capacity 1000 at work 100 each: exactly ten clear, the other
        # twenty observe no remaining capacity.
        winners = sorted(job_id for job_id, created in outcomes if created)
        self.assertEqual(len(winners), 10)
        self.assertEqual(len(sold_out), 20)
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(len(data["cleared"]), 10)
        self.assertEqual(len(data["idempotency"]), 10)
        self.assertEqual(len(data["audit"]), 10)

    def test_returned_record_is_a_copy(self) -> None:
        self._seed()
        record, _ = clear(self.jobs, self.supply, self.ledger,
                          "j-1", "k", 50)
        record["candidates"][0]["resource"]["capacity"] = 1
        again, created = clear(self.jobs, self.supply, self.ledger,
                               "j-1", "k", 50)
        self.assertFalse(created)
        self.assertEqual(again["candidates"][0]["resource"]["capacity"], 100)

    def test_no_tmp_files_left_behind(self) -> None:
        self._seed()
        clear(self.jobs, self.supply, self.ledger, "j-1", "k", 50)
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    def test_failed_directory_sync_restores_previous_bytes(self) -> None:
        self._seed()
        clear(self.jobs, self.supply, self.ledger, "j-1", "k1", 50)
        submit_job(self.jobs, _job("j-2"), "jk2")
        before = Path(self.ledger).read_bytes()
        original = market_module._fsync_directory_clear

        def failing(directory: str) -> None:
            raise OSError("simulated sync failure")

        market_module._fsync_directory_clear = failing
        try:
            with self.assertRaises(OSError):
                clear(self.jobs, self.supply, self.ledger, "j-2", "k2", 50)
        finally:
            market_module._fsync_directory_clear = original
        self.assertEqual(Path(self.ledger).read_bytes(), before)
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    def test_failed_first_commit_leaves_no_ledger(self) -> None:
        self._seed()
        original = market_module._fsync_directory_clear

        def failing(directory: str) -> None:
            raise OSError("simulated sync failure")

        market_module._fsync_directory_clear = failing
        try:
            with self.assertRaises(OSError):
                clear(self.jobs, self.supply, self.ledger, "j-1", "k1", 50)
        finally:
            market_module._fsync_directory_clear = original
        self.assertFalse(os.path.exists(self.ledger))
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])


if __name__ == "__main__":
    unittest.main()
