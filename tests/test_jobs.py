from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import jobs as jobs_module
from carbon_market.jobs import register


def _job(job_id: str = "j-1", **overrides: object) -> dict[str, object]:
    job: dict[str, object] = {
        "job_id": job_id,
        "deadline": 100,
        "energy_wh": 250,
        "residency_regions": ["eu-north", "us-west"],
    }
    job.update(overrides)
    return job


class RegisterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "registry.json")

    def test_creates_file_and_returns_record(self) -> None:
        record, created = register(self.path, _job(), "key-1")
        self.assertTrue(created)
        self.assertEqual(record, {
            "job_id": "j-1",
            "deadline": 100,
            "energy_wh": 250,
            "residency_regions": ["eu-north", "us-west"],
            "state": "queued",
        })
        self.assertTrue(os.path.exists(self.path))
        raw = Path(self.path).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()), ["version", "jobs", "idempotency"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["jobs"].keys()), ["j-1"])
        self.assertEqual(list(data["jobs"]["j-1"].keys()),
                         ["job_id", "deadline", "energy_wh",
                          "residency_regions", "state"])
        self.assertEqual(data["idempotency"], {"key-1": "j-1"})

    def test_regions_sorted_by_code_point_and_dedup_rejected(self) -> None:
        record, _ = register(self.path,
                             _job(residency_regions=["z", "a", "中", "A"]),
                             "k")
        self.assertEqual(record["residency_regions"], ["A", "a", "z", "中"])
        with self.assertRaises(ValueError):
            register(self.path,
                     _job(job_id="j-2", residency_regions=["a", "a"]), "k2")

    def test_compact_utf8_json(self) -> None:
        register(self.path, _job(job_id="j-中"), "键")
        raw = Path(self.path).read_text(encoding="utf-8")
        self.assertIn("j-中", raw)
        self.assertIn("键", raw)
        self.assertNotIn(" ", raw.split("\n", 1)[0])

    def test_idempotent_replay(self) -> None:
        first, created_first = register(
            self.path, _job(residency_regions=["b", "a"]), "key")
        second, created_second = register(
            self.path, _job(residency_regions=["a", "b"]), "key")
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first, second)
        data = json.loads(Path(self.path).read_text(encoding="utf-8"))
        self.assertEqual(len(data["jobs"]), 1)
        self.assertEqual(len(data["idempotency"]), 1)

    def test_same_key_different_job_raises(self) -> None:
        register(self.path, _job(), "key")
        with self.assertRaises(ValueError):
            register(self.path, _job(deadline=101), "key")
        with self.assertRaises(ValueError):
            register(self.path, _job(energy_wh=1), "key")
        with self.assertRaises(ValueError):
            register(self.path, _job(residency_regions=["x"]), "key")

    def test_job_id_taken_by_other_key_raises(self) -> None:
        register(self.path, _job(), "key-1")
        with self.assertRaises(ValueError):
            register(self.path, _job(), "key-2")

    def test_distinct_jobs_and_keys_persist_sorted(self) -> None:
        register(self.path, _job(job_id="j3"), "k3")
        register(self.path, _job(job_id="j1"), "k1")
        register(self.path, _job(job_id="j2"), "k2")
        data = json.loads(Path(self.path).read_text(encoding="utf-8"))
        self.assertEqual(list(data["jobs"].keys()), ["j1", "j2", "j3"])
        self.assertEqual(list(data["idempotency"].keys()), ["k1", "k2", "k3"])

    def test_invalid_arguments(self) -> None:
        cases = [
            ("bad id", _job(job_id="")),
            ("bool deadline", _job(deadline=True)),
            ("negative deadline", _job(deadline=-1)),
            ("float deadline", _job(deadline=1.5)),
            ("zero energy", _job(energy_wh=0)),
            ("negative energy", _job(energy_wh=-3)),
            ("bool energy", _job(energy_wh=False)),
            ("empty regions", _job(residency_regions=[])),
            ("non-string region", _job(residency_regions=["a", 1])),
            ("empty region", _job(residency_regions=["a", ""])),
            ("extra field", _job(state="queued")),
            ("missing field", {"job_id": "x", "deadline": 1,
                               "energy_wh": 2}),
            ("not a dict", ["j", 1, 2, ["a"]]),  # type: ignore[arg-type]
        ]
        for label, bad_job in cases:
            with self.subTest(label):
                with self.assertRaises(ValueError):
                    register(self.path, bad_job, "key")  # type: ignore[arg-type]

        with self.assertRaises(ValueError):
            register(self.path, _job(), "")
        with self.assertRaises(ValueError):
            register(self.path, _job(), 123)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            register(123, _job(), "key")  # type: ignore[arg-type]

    def test_missing_parent_directory_raises_file_not_found(self) -> None:
        missing = os.path.join(self.tmp.name, "no-such-dir", "registry.json")
        with self.assertRaises(FileNotFoundError):
            register(missing, _job(), "key")

    def test_equivalent_paths_share_lock_and_concurrency(self) -> None:
        relative = os.path.join("reg.json")
        cwd = os.getcwd()
        os.chdir(self.tmp.name)
        try:
            paths = [
                relative,
                os.path.abspath(relative),
                os.path.join(self.tmp.name, ".", "reg.json"),
            ]
            errors: list[BaseException] = []

            def worker(path: str, index: int) -> None:
                try:
                    register(path, _job(job_id=f"j{index:03d}"), f"k{index:03d}")
                except BaseException as exc:  # noqa: BLE001 - report all
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(path, i))
                       for i, path in enumerate(paths * 20)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
            self.assertEqual(errors, [])
            data = json.loads(Path("reg.json").read_text(encoding="utf-8"))
            self.assertEqual(len(data["jobs"]), 60)
            self.assertEqual(len(data["idempotency"]), 60)
        finally:
            os.chdir(cwd)

    def test_concurrent_replays(self) -> None:
        register(self.path, _job(), "key")
        results: list[tuple[bool, dict[str, object]]] = []
        lock = threading.Lock()

        def worker() -> None:
            record, created = register(
                self.path, _job(residency_regions=["us-west", "eu-north"]),
                "key")
            with lock:
                results.append((created, record))

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        self.assertEqual(len(results), 10)
        self.assertTrue(all(not created for created, _ in results))
        data = json.loads(Path(self.path).read_text(encoding="utf-8"))
        self.assertEqual(len(data["jobs"]), 1)

    def test_corrupt_file_raises_value_error(self) -> None:
        Path(self.path).write_text("{not json", encoding="utf-8")
        with self.assertRaises(ValueError):
            register(self.path, _job(), "key")

    def test_invalid_structures_raise_value_error(self) -> None:
        bad_payloads = [
            [],
            {},
            {"version": 1, "jobs": {}, "idempotency": {}, "extra": 1},
            {"version": 2, "jobs": {}, "idempotency": {}},
            {"version": "1", "jobs": {}, "idempotency": {}},
            {"version": 1, "jobs": [], "idempotency": {}},
            {"version": 1, "jobs": {}, "idempotency": []},
            {"version": 1,
             "jobs": {"j": {"job_id": "j", "deadline": 1, "energy_wh": 2,
                            "residency_regions": ["a"], "state": "running"}},
             "idempotency": {}},
            {"version": 1,
             "jobs": {"j": {"job_id": "other", "deadline": 1, "energy_wh": 2,
                            "residency_regions": ["a"], "state": "queued"}},
             "idempotency": {}},
            {"version": 1,
             "jobs": {"j": {"job_id": "j", "deadline": -1, "energy_wh": 2,
                            "residency_regions": ["a"], "state": "queued"}},
             "idempotency": {}},
            {"version": 1,
             "jobs": {"j": {"job_id": "j", "deadline": 1, "energy_wh": 0,
                            "residency_regions": ["a"], "state": "queued"}},
             "idempotency": {}},
            {"version": 1,
             "jobs": {"j": {"job_id": "j", "deadline": 1, "energy_wh": 2,
                            "residency_regions": [], "state": "queued"}},
             "idempotency": {}},
            {"version": 1,
             "jobs": {"j": {"job_id": "j", "deadline": 1, "energy_wh": 2,
                            "residency_regions": ["b", "a"],
                            "state": "queued"}},
             "idempotency": {}},
            {"version": 1, "jobs": {}, "idempotency": {"k": "missing"}},
        ]
        for payload in bad_payloads:
            with self.subTest(payload=payload):
                Path(self.path).write_text(json.dumps(payload),
                                           encoding="utf-8")
                with self.assertRaises(ValueError):
                    register(self.path, _job(), "key")

    def test_valid_existing_file_loads_and_registers(self) -> None:
        seed = {
            "version": 1,
            "jobs": {
                "a": {"job_id": "a", "deadline": 0, "energy_wh": 1,
                      "residency_regions": ["r1"], "state": "queued"},
            },
            "idempotency": {"k-a": "a"},
        }
        Path(self.path).write_text(json.dumps(seed), encoding="utf-8")
        record, created = register(self.path, _job(job_id="b"), "k-b")
        self.assertTrue(created)
        self.assertEqual(record["job_id"], "b")
        replay, replayed = register(
            self.path,
            {"job_id": "a", "deadline": 0, "energy_wh": 1,
             "residency_regions": ["r1"]}, "k-a")
        self.assertFalse(replayed)
        self.assertEqual(replay["state"], "queued")

    def test_reordered_keys_in_existing_file_still_load(self) -> None:
        seed = {
            "idempotency": {"k-a": "a"},
            "version": 1,
            "jobs": {
                "a": {"state": "queued", "residency_regions": ["r1"],
                      "energy_wh": 1, "deadline": 0, "job_id": "a"},
            },
        }
        Path(self.path).write_text(json.dumps(seed), encoding="utf-8")
        record, created = register(self.path, _job(job_id="b"), "k-b")
        self.assertTrue(created)
        self.assertEqual(record["job_id"], "b")
        replay, replayed = register(
            self.path,
            {"job_id": "a", "deadline": 0, "energy_wh": 1,
             "residency_regions": ["r1"]}, "k-a")
        self.assertFalse(replayed)
        # File is rewritten in canonical order on the next new registration.
        raw = json.loads(Path(self.path).read_text(encoding="utf-8"))
        self.assertEqual(list(raw.keys()), ["version", "jobs", "idempotency"])

    def test_zero_deadline_accepted(self) -> None:
        record, created = register(self.path, _job(deadline=0), "k")
        self.assertTrue(created)
        self.assertEqual(record["deadline"], 0)

    def test_returned_record_is_a_copy(self) -> None:
        record, _ = register(self.path, _job(), "k")
        record["deadline"] = 999
        replay, _ = register(self.path, _job(), "k")
        self.assertEqual(replay["deadline"], 100)

    def test_no_tmp_files_left_behind(self) -> None:
        register(self.path, _job(), "k")
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    def test_store_cache_keyed_by_realpath(self) -> None:
        register(self.path, _job(), "k")
        alt = os.path.join(self.tmp.name, "sub", "..", "registry.json")
        self.assertEqual(jobs_module._get_store(alt).realpath,
                         jobs_module._get_store(self.path).realpath)
        self.assertIs(jobs_module._get_store(alt),
                      jobs_module._get_store(self.path))


if __name__ == "__main__":
    unittest.main()
