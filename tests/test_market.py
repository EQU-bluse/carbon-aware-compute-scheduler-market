from __future__ import annotations

import contextlib
import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import market as market_module
from carbon_market.jobs import register as register_job
from carbon_market.market import match
from carbon_market.offers import register as register_offer


def _offer(resource_id: str, region: str = "a", *, capacity_wh: int = 1000,
           unit_cost: int = 100, carbon_intensity: int = 50) -> dict[str, object]:
    return {
        "resource_id": resource_id,
        "region": region,
        "capacity_wh": capacity_wh,
        "unit_cost": unit_cost,
        "carbon_intensity": carbon_intensity,
    }


def _job(job_id: str, *, energy_wh: int = 100,
         residency_regions: list[str] | None = None) -> dict[str, object]:
    return {
        "job_id": job_id,
        "deadline": 100,
        "energy_wh": energy_wh,
        "residency_regions": residency_regions or ["a"],
    }


class MatchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs_path = os.path.join(self.tmp.name, "jobs.json")
        self.offers_path = os.path.join(self.tmp.name, "offers.json")
        self.ledger_path = os.path.join(self.tmp.name, "ledger.json")

    def _register_offer(self, resource_id: str = "r1", **kwargs: object) -> str:
        register_offer(self.offers_path, _offer(resource_id, **kwargs),
                       f"k-offer-{resource_id}")
        return self.offers_path

    def _register_job(self, job_id: str = "j1", **kwargs: object) -> str:
        register_job(self.jobs_path, _job(job_id, **kwargs), f"k-job-{job_id}")
        return self.jobs_path

    def test_creates_ledger_and_returns_record(self) -> None:
        self._register_offer("r1")
        self._register_job()
        record, created = match(self.jobs_path, self.offers_path,
                                self.ledger_path, "j1", "key-1")
        self.assertTrue(created)
        self.assertEqual(record, {"job_id": "j1", "resource_id": "r1"})
        self.assertEqual(list(record.keys()), ["job_id", "resource_id"])

        raw = Path(self.ledger_path).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()),
                         ["version", "matches", "idempotency"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["matches"].keys()), ["j1"])
        self.assertEqual(list(data["matches"]["j1"].keys()),
                         ["job_id", "resource_id"])
        self.assertEqual(data["idempotency"], {"key-1": "j1"})

    def test_compact_utf8_json(self) -> None:
        register_offer(self.offers_path,
                       _offer("r-中", region="北"), "键o")
        register_job(self.jobs_path,
                     _job("j-中", residency_regions=["北"]), "键j")
        match(self.jobs_path, self.offers_path, self.ledger_path, "j-中", "键")
        raw = Path(self.ledger_path).read_text(encoding="utf-8")
        self.assertIn("j-中", raw)
        self.assertIn("r-中", raw)
        self.assertIn("键", raw)
        self.assertNotIn(" ", raw.split("\n", 1)[0])

    def test_prefers_lower_carbon_then_cost_then_resource_id(self) -> None:
        self._register_offer("r1", carbon_intensity=50, unit_cost=1)
        self._register_offer("r2", carbon_intensity=10, unit_cost=999)
        self._register_offer("r3", carbon_intensity=10, unit_cost=5)
        self._register_offer("r4", carbon_intensity=10, unit_cost=5)
        self._register_job()
        record, _ = match(self.jobs_path, self.offers_path,
                          self.ledger_path, "j1", "k")
        self.assertEqual(record["resource_id"], "r3")

    def test_region_must_be_allowed(self) -> None:
        self._register_offer("r1", region="b")
        self._register_offer("r2", region="c", carbon_intensity=1)
        self._register_job(residency_regions=["a", "z"])
        with self.assertRaises(LookupError):
            match(self.jobs_path, self.offers_path, self.ledger_path, "j1", "k")
        self.assertFalse(os.path.exists(self.ledger_path))

    def test_consumed_capacity_is_respected(self) -> None:
        self._register_offer("r1", capacity_wh=100, carbon_intensity=1)
        self._register_offer("r2", capacity_wh=1000, carbon_intensity=20)
        self._register_job("j1", energy_wh=100)
        self._register_job("j2", energy_wh=900)
        first, created_first = match(self.jobs_path, self.offers_path,
                                     self.ledger_path, "j1", "k1")
        second, created_second = match(self.jobs_path, self.offers_path,
                                       self.ledger_path, "j2", "k2")
        self.assertTrue(created_first)
        self.assertTrue(created_second)
        self.assertEqual(first["resource_id"], "r1")
        self.assertEqual(second["resource_id"], "r2")

    def test_capacity_exact_fit_is_feasible(self) -> None:
        self._register_offer("r1", capacity_wh=100)
        self._register_job(energy_wh=100)
        record, created = match(self.jobs_path, self.offers_path,
                                self.ledger_path, "j1", "k")
        self.assertTrue(created)
        self.assertEqual(record["resource_id"], "r1")

    def test_replay_does_not_consume_capacity(self) -> None:
        self._register_offer("r1", capacity_wh=100, carbon_intensity=1)
        self._register_job(energy_wh=100)
        first, created_first = match(self.jobs_path, self.offers_path,
                                     self.ledger_path, "j1", "k")
        second, created_second = match(self.jobs_path, self.offers_path,
                                       self.ledger_path, "j1", "k")
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first, second)
        data = json.loads(Path(self.ledger_path).read_text(encoding="utf-8"))
        self.assertEqual(len(data["matches"]), 1)
        self.assertEqual(len(data["idempotency"]), 1)

    def test_same_key_different_job_raises(self) -> None:
        self._register_offer("r1", capacity_wh=10000)
        self._register_job("j1", energy_wh=10)
        self._register_job("j2", energy_wh=10)
        match(self.jobs_path, self.offers_path, self.ledger_path, "j1", "k")
        with self.assertRaises(ValueError):
            match(self.jobs_path, self.offers_path, self.ledger_path, "j2", "k")

    def test_job_matched_under_another_key_raises(self) -> None:
        self._register_offer("r1", capacity_wh=10000)
        self._register_job()
        match(self.jobs_path, self.offers_path, self.ledger_path, "j1", "k1")
        with self.assertRaises(ValueError):
            match(self.jobs_path, self.offers_path, self.ledger_path, "j1", "k2")

    def test_unknown_job_raises_key_error(self) -> None:
        self._register_offer("r1")
        self._register_job()
        with self.assertRaises(KeyError):
            match(self.jobs_path, self.offers_path, self.ledger_path, "nope", "k")

    def test_no_feasible_offer_lookup_error_and_no_write(self) -> None:
        self._register_offer("r1", capacity_wh=10)
        self._register_job(energy_wh=100)
        with self.assertRaises(LookupError):
            match(self.jobs_path, self.offers_path, self.ledger_path, "j1", "k")
        self.assertFalse(os.path.exists(self.ledger_path))

    def test_missing_input_files_raise_file_not_found(self) -> None:
        self._register_offer("r1")
        self._register_job()
        with self.assertRaises(FileNotFoundError):
            match(os.path.join(self.tmp.name, "missing-jobs.json"),
                  self.offers_path, self.ledger_path, "j1", "k")
        with self.assertRaises(FileNotFoundError):
            match(self.jobs_path,
                  os.path.join(self.tmp.name, "missing-offers.json"),
                  self.ledger_path, "j1", "k")

    def test_missing_ledger_parent_directory_raises_file_not_found(self) -> None:
        self._register_offer("r1")
        self._register_job()
        missing = os.path.join(self.tmp.name, "no-such-dir", "ledger.json")
        with self.assertRaises(FileNotFoundError):
            match(self.jobs_path, self.offers_path, missing, "j1", "k")

    def test_invalid_arguments(self) -> None:
        self._register_offer("r1")
        self._register_job()
        for kwargs in (
            dict(jobs="", offers=self.offers_path, ledger=self.ledger_path,
                 job_id="j1", key="k"),
            dict(jobs=self.jobs_path, offers="", ledger=self.ledger_path,
                 job_id="j1", key="k"),
            dict(jobs=self.jobs_path, offers=self.offers_path, ledger="",
                 job_id="j1", key="k"),
            dict(jobs=self.jobs_path, offers=self.offers_path,
                 ledger=self.ledger_path, job_id="", key="k"),
            dict(jobs=self.jobs_path, offers=self.offers_path,
                 ledger=self.ledger_path, job_id="j1", key=""),
            dict(jobs=1, offers=self.offers_path, ledger=self.ledger_path,
                 job_id="j1", key="k"),
            dict(jobs=self.jobs_path, offers=self.offers_path,
                 ledger=self.ledger_path, job_id=1, key="k"),
            dict(jobs=self.jobs_path, offers=self.offers_path,
                 ledger=self.ledger_path, job_id="j1", key=2),
        ):
            with self.subTest(kwargs):
                with self.assertRaises(ValueError):
                    match(**kwargs)  # type: ignore[arg-type]

    def test_corrupt_registries_raise_value_error(self) -> None:
        for target in ("jobs", "offers", "ledger"):
            for stale in (self.jobs_path, self.offers_path, self.ledger_path):
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(stale)
            self._register_offer("r1")
            self._register_job()
            if target == "ledger":
                # Seed a valid ledger before corrupting it.
                match(self.jobs_path, self.offers_path, self.ledger_path,
                      "j1", "seed")
            path = {"jobs": self.jobs_path, "offers": self.offers_path,
                    "ledger": self.ledger_path}[target]
            Path(path).write_text("{not json", encoding="utf-8")
            with self.subTest(target):
                with self.assertRaises(ValueError):
                    match(self.jobs_path, self.offers_path, self.ledger_path,
                          "j1", "k")

    def test_bad_version_raises_value_error(self) -> None:
        self._register_offer("r1")
        self._register_job()
        for name, path in (("jobs", self.jobs_path), ("offers", self.offers_path)):
            data = json.loads(Path(path).read_text(encoding="utf-8"))
            data["version"] = 2
            Path(path).write_text(json.dumps(data), encoding="utf-8")
            with self.subTest(name):
                with self.assertRaises(ValueError):
                    match(self.jobs_path, self.offers_path,
                          self.ledger_path, "j1", "k")
            data["version"] = 1
            Path(path).write_text(json.dumps(data), encoding="utf-8")

    def test_dangling_references_raise_value_error(self) -> None:
        self._register_offer("r1")
        self._register_job()
        # Ledger matches a job that the jobs registry no longer knows.
        Path(self.ledger_path).write_text(
            json.dumps({"version": 1,
                        "matches": {"ghost": {"job_id": "ghost",
                                              "resource_id": "r1"}},
                        "idempotency": {}}),
            encoding="utf-8")
        with self.assertRaises(ValueError):
            match(self.jobs_path, self.offers_path, self.ledger_path, "j1", "k")

        # Ledger matches onto an unknown offer.
        Path(self.ledger_path).write_text(
            json.dumps({"version": 1,
                        "matches": {"j1": {"job_id": "j1",
                                           "resource_id": "ghost"}},
                        "idempotency": {}}),
            encoding="utf-8")
        with self.assertRaises(ValueError):
            match(self.jobs_path, self.offers_path, self.ledger_path, "j1", "k")

    def test_invalid_ledger_structures_raise_value_error(self) -> None:
        self._register_offer("r1")
        self._register_job()
        bad_payloads = [
            [],
            {},
            {"version": 2, "matches": {}, "idempotency": {}},
            {"version": "1", "matches": {}, "idempotency": {}},
            {"version": 1, "matches": [], "idempotency": {}},
            {"version": 1, "matches": {}, "idempotency": []},
            {"version": 1, "matches": {}, "idempotency": {}, "extra": 1},
            {"version": 1,
             "matches": {"j1": {"job_id": "other", "resource_id": "r1"}},
             "idempotency": {}},
            {"version": 1,
             "matches": {"j1": {"job_id": "j1", "resource_id": ""}},
             "idempotency": {}},
            {"version": 1,
             "matches": {"j1": {"job_id": "j1"}},
             "idempotency": {}},
            {"version": 1, "matches": {}, "idempotency": {"k": "missing"}},
            {"version": 1,
             "matches": {"j1": {"job_id": "j1", "resource_id": "r1"}},
             "idempotency": {"k1": "j1", "k2": "j1"}},
        ]
        for payload in bad_payloads:
            with self.subTest(payload=payload):
                Path(self.ledger_path).write_text(json.dumps(payload),
                                                  encoding="utf-8")
                with self.assertRaises(ValueError):
                    match(self.jobs_path, self.offers_path,
                          self.ledger_path, "j1", "k")

    def test_shuffled_ledger_keys_rewritten_in_canonical_order(self) -> None:
        self._register_offer("r1", capacity_wh=10_000)
        self._register_job()
        self._register_job("j2", energy_wh=1)
        Path(self.ledger_path).write_text(
            json.dumps({"idempotency": {"k": "j1"},
                        "matches": {"j1": {"resource_id": "r1",
                                           "job_id": "j1"}},
                        "version": 1}),
            encoding="utf-8")
        record, created = match(self.jobs_path, self.offers_path,
                                self.ledger_path, "j1", "k")
        self.assertFalse(created)
        self.assertEqual(list(record.keys()), ["job_id", "resource_id"])
        # A replay does not write; the next new registration rewrites the
        # whole ledger in canonical order.
        match(self.jobs_path, self.offers_path, self.ledger_path, "j2", "k2")
        raw = json.loads(Path(self.ledger_path).read_text(encoding="utf-8"))
        self.assertEqual(list(raw.keys()), ["version", "matches", "idempotency"])
        self.assertEqual(list(raw["matches"]["j1"].keys()),
                         ["job_id", "resource_id"])
        self.assertEqual(list(raw["matches"]["j2"].keys()),
                         ["job_id", "resource_id"])

    def test_objects_sorted_by_code_point(self) -> None:
        self._register_offer("r3", carbon_intensity=1)
        self._register_offer("r1", carbon_intensity=1)
        self._register_offer("r2", carbon_intensity=1)
        for job_id in ("j3", "j1", "j2"):
            self._register_job(job_id, energy_wh=1)
        for job_id, key in (("j3", "k3"), ("j1", "k1"), ("j2", "k2")):
            match(self.jobs_path, self.offers_path, self.ledger_path,
                  job_id, key)
        data = json.loads(Path(self.ledger_path).read_text(encoding="utf-8"))
        self.assertEqual(list(data["matches"].keys()), ["j1", "j2", "j3"])
        self.assertEqual(list(data["idempotency"].keys()),
                         ["k1", "k2", "k3"])

    def test_no_tmp_files_left_behind(self) -> None:
        self._register_offer("r1")
        self._register_job()
        match(self.jobs_path, self.offers_path, self.ledger_path, "j1", "k")
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    def test_returned_record_is_a_copy(self) -> None:
        self._register_offer("r1")
        self._register_job()
        record, _ = match(self.jobs_path, self.offers_path,
                          self.ledger_path, "j1", "k")
        record["resource_id"] = "tampered"
        replay, _ = match(self.jobs_path, self.offers_path,
                          self.ledger_path, "j1", "k")
        self.assertEqual(replay["resource_id"], "r1")

    def test_equivalent_paths_share_lock_and_concurrency(self) -> None:
        register_offer(self.offers_path,
                       _offer("r1", capacity_wh=10_000), "offer-key")
        for i in range(60):
            register_job(self.jobs_path,
                         _job(f"j{i:03d}", energy_wh=1), f"job-key-{i:03d}")

        relative = os.path.join("ledger.json")
        cwd = os.getcwd()
        os.chdir(self.tmp.name)
        try:
            paths = [
                relative,
                os.path.abspath(relative),
                os.path.join(self.tmp.name, ".", "ledger.json"),
            ]
            errors: list[BaseException] = []

            def worker(path: str, index: int) -> None:
                try:
                    match(self.jobs_path, self.offers_path, path,
                          f"j{index:03d}", f"k{index:03d}")
                except BaseException as exc:  # noqa: BLE001 - report all
                    errors.append(exc)

            threads = [
                threading.Thread(target=worker, args=(paths[i % 3], i))
                for i in range(60)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
            self.assertEqual(errors, [])
            data = json.loads(Path("ledger.json").read_text(encoding="utf-8"))
            self.assertEqual(len(data["matches"]), 60)
            self.assertEqual(len(data["idempotency"]), 60)
        finally:
            os.chdir(cwd)

    def test_concurrent_replays(self) -> None:
        self._register_offer("r1")
        self._register_job()
        results: list[tuple[bool, dict[str, object]]] = []
        lock = threading.Lock()

        def worker() -> None:
            record, created = match(self.jobs_path, self.offers_path,
                                    self.ledger_path, "j1", "k")
            with lock:
                results.append((created, record))

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        self.assertEqual(len(results), 10)
        created_flags = [created for created, _ in results]
        self.assertEqual(sorted(created_flags), [False] * 9 + [True])
        data = json.loads(Path(self.ledger_path).read_text(encoding="utf-8"))
        self.assertEqual(len(data["matches"]), 1)

    def test_store_cache_keyed_by_realpath(self) -> None:
        alt = os.path.join(self.tmp.name, "sub", "..", "ledger.json")
        self.assertIs(market_module._get_store(alt),
                      market_module._get_store(self.ledger_path))


if __name__ == "__main__":
    unittest.main()
