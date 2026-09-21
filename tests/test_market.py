from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market.jobs import register as register_job
from carbon_market.market import match
from carbon_market.offers import register as register_offer


def _job(job_id: str = "j-1", **overrides: object) -> dict[str, object]:
    job: dict[str, object] = {
        "job_id": job_id,
        "deadline": 100,
        "energy_wh": 100,
        "residency_regions": ["eu-north"],
    }
    job.update(overrides)
    return job


def _offer(resource_id: str = "r-1", **overrides: object) -> dict[str, object]:
    offer: dict[str, object] = {
        "resource_id": resource_id,
        "region": "eu-north",
        "capacity_wh": 250,
        "unit_cost": 1000,
        "carbon_intensity": 42,
    }
    offer.update(overrides)
    return offer


class MatchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs = os.path.join(self.tmp.name, "jobs.json")
        self.offers = os.path.join(self.tmp.name, "offers.json")
        self.ledger = os.path.join(self.tmp.name, "ledger.json")

    def _seed(self, job: dict[str, object] | None = None,
              offer: dict[str, object] | None = None) -> None:
        register_job(self.jobs, job if job is not None else _job(), "job-key")
        register_offer(self.offers,
                       offer if offer is not None else _offer(), "offer-key")

    def test_creates_ledger_and_returns_record(self) -> None:
        self._seed()
        record, created = match(self.jobs, self.offers, self.ledger,
                                "j-1", "k-1")
        self.assertTrue(created)
        self.assertEqual(record, {"job_id": "j-1", "resource_id": "r-1"})
        self.assertEqual(list(record.keys()), ["job_id", "resource_id"])
        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()),
                         ["version", "matches", "idempotency"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(data["matches"],
                         {"j-1": {"job_id": "j-1", "resource_id": "r-1"}})
        self.assertEqual(list(data["matches"]["j-1"].keys()),
                         ["job_id", "resource_id"])
        self.assertEqual(data["idempotency"], {"k-1": "j-1"})

    def test_compact_utf8_json_and_sorted_keys(self) -> None:
        register_job(self.jobs, _job(job_id="j-b"), "jk-b")
        register_job(self.jobs, _job(job_id="j-a"), "jk-a")
        register_offer(self.offers, _offer(resource_id="r-中"), "ok")
        match(self.jobs, self.offers, self.ledger, "j-b", "键-b")
        match(self.jobs, self.offers, self.ledger, "j-a", "键-a")
        raw = Path(self.ledger).read_text(encoding="utf-8")
        self.assertIn("r-中", raw)
        self.assertIn("键-b", raw)
        self.assertNotIn(" ", raw.split("\n", 1)[0])
        data = json.loads(raw)
        self.assertEqual(list(data["matches"].keys()), ["j-a", "j-b"])
        self.assertEqual(list(data["idempotency"].keys()), ["键-a", "键-b"])

    def test_picks_lowest_carbon_then_cost_then_resource_id(self) -> None:
        register_job(self.jobs, _job(), "jk")
        register_offer(self.offers, _offer(resource_id="r-cheap",
                                           carbon_intensity=9, unit_cost=0),
                       "o1")
        register_offer(self.offers, _offer(resource_id="r-b",
                                           carbon_intensity=5, unit_cost=4),
                       "o2")
        register_offer(self.offers, _offer(resource_id="r-a",
                                           carbon_intensity=5, unit_cost=4),
                       "o3")
        register_offer(self.offers, _offer(resource_id="r-0",
                                           carbon_intensity=5, unit_cost=7),
                       "o4")
        record, _ = match(self.jobs, self.offers, self.ledger, "j-1", "k")
        self.assertEqual(record["resource_id"], "r-a")

    def test_region_restricted(self) -> None:
        self._seed(offer=_offer(region="us-west"))
        with self.assertRaises(LookupError):
            match(self.jobs, self.offers, self.ledger, "j-1", "k")
        self.assertFalse(os.path.exists(self.ledger))

    def test_capacity_deducts_allocated_energy(self) -> None:
        register_job(self.jobs, _job(job_id="j-1"), "jk1")
        register_job(self.jobs, _job(job_id="j-2"), "jk2")
        register_offer(self.offers, _offer(resource_id="r-small",
                                           capacity_wh=150,
                                           carbon_intensity=1), "o1")
        register_offer(self.offers, _offer(resource_id="r-big",
                                           capacity_wh=500,
                                           carbon_intensity=2), "o2")
        first, _ = match(self.jobs, self.offers, self.ledger, "j-1", "k1")
        self.assertEqual(first["resource_id"], "r-small")
        # r-small has only 50 Wh left, so j-2 spills to r-big.
        second, _ = match(self.jobs, self.offers, self.ledger, "j-2", "k2")
        self.assertEqual(second["resource_id"], "r-big")

    def test_replay_allocates_no_further_capacity(self) -> None:
        register_job(self.jobs, _job(job_id="j-1"), "jk1")
        register_job(self.jobs, _job(job_id="j-2"), "jk2")
        register_offer(self.offers, _offer(capacity_wh=150), "o1")
        first, created_first = match(self.jobs, self.offers, self.ledger,
                                     "j-1", "k")
        replay, created_replay = match(self.jobs, self.offers, self.ledger,
                                       "j-1", "k")
        self.assertTrue(created_first)
        self.assertFalse(created_replay)
        self.assertEqual(first, replay)
        # The replay did not consume capacity: 150 - 100 leaves no room.
        with self.assertRaises(LookupError):
            match(self.jobs, self.offers, self.ledger, "j-2", "k2")
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(len(data["matches"]), 1)
        self.assertEqual(len(data["idempotency"]), 1)

    def test_same_key_different_job_raises(self) -> None:
        register_job(self.jobs, _job(job_id="j-1"), "jk1")
        register_job(self.jobs, _job(job_id="j-2"), "jk2")
        register_offer(self.offers, _offer(), "o")
        match(self.jobs, self.offers, self.ledger, "j-1", "k")
        with self.assertRaises(ValueError):
            match(self.jobs, self.offers, self.ledger, "j-2", "k")

    def test_job_matched_under_other_key_raises(self) -> None:
        self._seed()
        match(self.jobs, self.offers, self.ledger, "j-1", "k1")
        with self.assertRaises(ValueError):
            match(self.jobs, self.offers, self.ledger, "j-1", "k2")

    def test_unknown_job_raises_key_error(self) -> None:
        self._seed()
        with self.assertRaises(KeyError):
            match(self.jobs, self.offers, self.ledger, "j-ghost", "k")

    def test_no_feasible_offer_raises_lookup_error_without_write(self) -> None:
        register_job(self.jobs, _job(energy_wh=1000), "jk")
        register_offer(self.offers, _offer(), "o")
        with self.assertRaises(LookupError):
            match(self.jobs, self.offers, self.ledger, "j-1", "k")
        self.assertFalse(os.path.exists(self.ledger))

    def test_missing_jobs_or_offers_raise_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            match(self.jobs, self.offers, self.ledger, "j-1", "k")
        register_job(self.jobs, _job(), "jk")
        with self.assertRaises(FileNotFoundError):
            match(self.jobs, self.offers, self.ledger, "j-1", "k")
        missing = os.path.join(self.tmp.name, "no-such-dir", "jobs.json")
        with self.assertRaises(FileNotFoundError):
            match(missing, self.offers, self.ledger, "j-1", "k")

    def test_missing_ledger_parent_raises_file_not_found(self) -> None:
        self._seed()
        ledger = os.path.join(self.tmp.name, "no-such-dir", "ledger.json")
        with self.assertRaises(FileNotFoundError):
            match(self.jobs, self.offers, ledger, "j-1", "k")

    def test_invalid_arguments(self) -> None:
        self._seed()
        good = (self.jobs, self.offers, self.ledger, "j-1", "k")
        for index in range(len(good)):
            for bad in ("", 123, None):
                args = list(good)
                args[index] = bad
                with self.subTest(index=index, bad=bad):
                    with self.assertRaises(ValueError):
                        match(*args)  # type: ignore[arg-type]

    def test_corrupt_files_raise_value_error(self) -> None:
        self._seed()
        register_job(self.jobs, _job(job_id="j-2"), "jk2")
        for path in (self.jobs, self.offers, self.ledger):
            if path == self.ledger:
                match(self.jobs, self.offers, self.ledger, "j-1", "k")
            original = Path(path).read_bytes()
            Path(path).write_text("{not json", encoding="utf-8")
            with self.subTest(path=path):
                with self.assertRaises(ValueError):
                    match(self.jobs, self.offers, self.ledger, "j-2", "k2")
            Path(path).write_bytes(original)

    def test_invalid_ledger_structures_raise_value_error(self) -> None:
        self._seed()
        bad_payloads = [
            [],
            {},
            {"version": 1, "matches": {}, "idempotency": {}, "extra": 1},
            {"version": 2, "matches": {}, "idempotency": {}},
            {"version": "1", "matches": {}, "idempotency": {}},
            {"version": 1, "matches": [], "idempotency": {}},
            {"version": 1, "matches": {}, "idempotency": []},
            {"version": 1,
             "matches": {"j-1": {"job_id": "other", "resource_id": "r-1"}},
             "idempotency": {}},
            {"version": 1,
             "matches": {"j-1": {"job_id": "j-1"}},
             "idempotency": {}},
            {"version": 1,
             "matches": {"j-1": {"job_id": "j-1", "resource_id": "ghost"}},
             "idempotency": {}},
            {"version": 1,
             "matches": {"ghost": {"job_id": "ghost", "resource_id": "r-1"}},
             "idempotency": {}},
            {"version": 1, "matches": {},
             "idempotency": {"k": "missing"}},
            {"version": 1,
             "matches": {"j-1": {"job_id": "j-1", "resource_id": "r-1"}},
             "idempotency": {"k1": "j-1", "k2": "j-1"}},
        ]
        for payload in bad_payloads:
            with self.subTest(payload=payload):
                Path(self.ledger).write_text(json.dumps(payload),
                                             encoding="utf-8")
                with self.assertRaises(ValueError):
                    match(self.jobs, self.offers, self.ledger, "j-1", "k")

    def test_negative_zero_literal_in_ledger_raises_value_error(self) -> None:
        self._seed()
        Path(self.ledger).write_text(
            '{"version":-0,"matches":{},"idempotency":{}}', encoding="utf-8")
        with self.assertRaises(ValueError):
            match(self.jobs, self.offers, self.ledger, "j-1", "k")

    def test_seeded_ledger_replays_and_counts_capacity(self) -> None:
        register_job(self.jobs, _job(job_id="j-1"), "jk1")
        register_job(self.jobs, _job(job_id="j-2"), "jk2")
        register_offer(self.offers, _offer(capacity_wh=150), "o")
        Path(self.ledger).write_text(json.dumps({
            "version": 1,
            "matches": {"j-1": {"job_id": "j-1", "resource_id": "r-1"}},
            "idempotency": {"k1": "j-1"},
        }), encoding="utf-8")
        replay, created = match(self.jobs, self.offers, self.ledger,
                                "j-1", "k1")
        self.assertFalse(created)
        self.assertEqual(replay, {"job_id": "j-1", "resource_id": "r-1"})
        with self.assertRaises(LookupError):
            match(self.jobs, self.offers, self.ledger, "j-2", "k2")

    def test_equivalent_paths_share_lock_and_concurrency(self) -> None:
        register_offer(self.offers, _offer(capacity_wh=10_000), "o")
        for index in range(30):
            register_job(self.jobs, _job(job_id=f"j{index:03d}"), f"jk{index}")
        relative = os.path.join("ledger.json")
        cwd = os.getcwd()
        os.chdir(self.tmp.name)
        try:
            paths = [
                relative,
                os.path.abspath(relative),
                os.path.join(self.tmp.name, ".", "ledger.json"),
            ]
            results: list[tuple[dict[str, object], bool]] = []
            errors: list[BaseException] = []
            lock = threading.Lock()

            def worker(path: str, index: int) -> None:
                try:
                    outcome = match(self.jobs, self.offers, path,
                                    f"j{index:03d}", f"k{index:03d}")
                    with lock:
                        results.append(outcome)
                except BaseException as exc:  # noqa: BLE001 - report all
                    with lock:
                        errors.append(exc)

            threads = [threading.Thread(target=worker,
                                        args=(paths[i % len(paths)], i))
                       for i in range(30)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
            self.assertEqual(errors, [])
            self.assertEqual(len(results), 30)
            self.assertTrue(all(created for _, created in results))
            data = json.loads(Path(relative).read_text(encoding="utf-8"))
            self.assertEqual(len(data["matches"]), 30)
            self.assertEqual(len(data["idempotency"]), 30)
        finally:
            os.chdir(cwd)

    def test_no_tmp_files_left_behind(self) -> None:
        self._seed()
        match(self.jobs, self.offers, self.ledger, "j-1", "k")
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])


if __name__ == "__main__":
    unittest.main()
