from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
from unittest import mock

from carbon_market import jobs


def _job(job_id: str = "j1", **overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "job_id": job_id,
        "deadline": 100,
        "energy_wh": 50,
        "residency_regions": ["eu-north", "jp-east"],
    }
    value.update(overrides)
    return value


class RegisterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "store.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_creates_file_with_canonical_format(self) -> None:
        entry, created = jobs.register(self.path, _job(), "k1")
        self.assertTrue(created)
        self.assertEqual(
            entry,
            {
                "job_id": "j1",
                "deadline": 100,
                "energy_wh": 50,
                "residency_regions": ["eu-north", "jp-east"],
                "state": "queued",
            },
        )
        self.assertEqual(list(entry), [
            "job_id",
            "deadline",
            "energy_wh",
            "residency_regions",
            "state",
        ])

        with open(self.path, "rb") as handle:
            raw = handle.read()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        payload = json.loads(raw)
        self.assertEqual(list(payload), ["version", "jobs", "idempotency"])
        self.assertEqual(payload["version"], 1)
        self.assertEqual(payload["idempotency"], {"k1": "j1"})
        self.assertEqual(list(payload["jobs"]), ["j1"])

        expected = (
            '{"version":1,"jobs":{"j1":{"job_id":"j1","deadline":100,'
            '"energy_wh":50,"residency_regions":["eu-north","jp-east"],'
            '"state":"queued"}},"idempotency":{"k1":"j1"}}\n'
        )
        self.assertEqual(raw.decode("utf-8"), expected)

    def test_regions_sorted_by_codepoint_and_dedup_required(self) -> None:
        entry, _ = jobs.register(
            self.path,
            _job(residency_regions=["z", "ä", "a", "水"]),
            "k1",
        )
        self.assertEqual(entry["residency_regions"], ["a", "z", "ä", "水"])

        with self.assertRaises(ValueError):
            jobs.register(
                self.path,
                _job(job_id="j2", residency_regions=["a", "a"]),
                "k2",
            )

    def test_replay_returns_stored_entry_and_false(self) -> None:
        first, created_first = jobs.register(
            self.path, _job(residency_regions=["jp-east", "eu-north"]), "k1"
        )
        self.assertTrue(created_first)

        second, created_second = jobs.register(
            self.path, _job(), "k1"  # regions reordered, same normalized job
        )
        self.assertFalse(created_second)
        self.assertEqual(second, first)

        with open(self.path, encoding="utf-8") as handle:
            payload = json.load(handle)
        self.assertEqual(len(payload["jobs"]), 1)
        self.assertEqual(len(payload["idempotency"]), 1)

    def test_same_key_different_job_rejected(self) -> None:
        jobs.register(self.path, _job(), "k1")
        with self.assertRaises(ValueError):
            jobs.register(self.path, _job(deadline=999), "k1")
        with self.assertRaises(ValueError):
            jobs.register(self.path, _job(energy_wh=51), "k1")
        with self.assertRaises(ValueError):
            jobs.register(self.path, _job(residency_regions=["us-west"]), "k1")

    def test_job_id_taken_by_other_key_rejected(self) -> None:
        jobs.register(self.path, _job(), "k1")
        with self.assertRaises(ValueError):
            jobs.register(self.path, _job(), "k2")
        # File must stay consistent after the rejected attempt.
        entry, created = jobs.register(self.path, _job(), "k1")
        self.assertFalse(created)
        self.assertEqual(entry["state"], "queued")

    def test_persistence_across_calls_and_sorted_keys(self) -> None:
        jobs.register(self.path, _job("j3"), "k3")
        jobs.register(self.path, _job("j1"), "k1")
        jobs.register(self.path, _job("j2"), "k2")
        with open(self.path, encoding="utf-8") as handle:
            payload = json.load(handle)
        self.assertEqual(list(payload["jobs"]), ["j1", "j2", "j3"])
        self.assertEqual(list(payload["idempotency"]), ["k1", "k2", "k3"])

    def test_missing_parent_directory_raises_file_not_found(self) -> None:
        path = os.path.join(self.tmp.name, "missing", "store.json")
        with self.assertRaises(FileNotFoundError):
            jobs.register(path, _job(), "k1")

    def test_invalid_arguments(self) -> None:
        def expect_error(job: object = _job(), key: object = "k1",
                         path: object = None) -> None:
            with self.assertRaises(ValueError):
                jobs.register(path if path is not None else self.path, job, key)

        expect_error(key="")
        expect_error(key=5)
        expect_error(job=[])
        bad_jobs = [
            {},
            {**_job(), "extra": 1},
            {"job_id": "", "deadline": 0, "energy_wh": 1,
             "residency_regions": ["a"]},
            _job(deadline=True),
            _job(deadline=-1),
            _job(deadline=1.5),
            _job(energy_wh=True),
            _job(energy_wh=0),
            _job(energy_wh=1.0),
            _job(residency_regions=[]),
            _job(residency_regions="ab"),
            _job(residency_regions=[""]),
            _job(residency_regions=["a", "a"]),
        ]
        for bad in bad_jobs:
            expect_error(job=bad)
        with self.assertRaises(ValueError):
            jobs.register("", _job(), "k1")

    def test_corrupt_or_malformed_file_raises_value_error(self) -> None:
        for content in (
            "not json",
            "[]",
            "{}",
            '{"version":2,"jobs":{},"idempotency":{}}',
            '{"version":1,"jobs":[],"idempotency":{}}',
            '{"version":1,"jobs":{},"idempotency":[]}',
            '{"version":1,"jobs":{"j1":{}},"idempotency":{}}',
            '{"version":1,"jobs":{"j1":{"job_id":"j1","deadline":-1,'
            '"energy_wh":1,"residency_regions":["a"],"state":"queued"}},'
            '"idempotency":{}}',
            '{"version":1,"jobs":{"j1":{"job_id":"j2","deadline":1,'
            '"energy_wh":1,"residency_regions":["a"],"state":"queued"}},'
            '"idempotency":{}}',
            '{"version":1,"jobs":{"j1":{"job_id":"j1","deadline":1,'
            '"energy_wh":1,"residency_regions":["a"],"state":"running"}},'
            '"idempotency":{}}',
            '{"version":1,"jobs":{"j1":{"job_id":"j1","deadline":1,'
            '"energy_wh":1,"residency_regions":["a"],"state":"queued"}},'
            '"idempotency":{"k1":"missing"}}',
        ):
            with open(self.path, "w", encoding="utf-8") as handle:
                handle.write(content)
            with self.assertRaises(ValueError):
                jobs.register(self.path, _job(), "k1")

    def test_equivalent_paths_share_lock_and_linearize(self) -> None:
        cwd = os.getcwd()
        os.chdir(self.tmp.name)
        try:
            winners: list[str] = []
            errors: list[BaseException] = []

            def worker(path: str, key: str, job_id: str) -> None:
                try:
                    _, created = jobs.register(path, _job(job_id), key)
                    if created:
                        winners.append(job_id)
                except BaseException as exc:  # pragma: no cover - failure path
                    errors.append(exc)

            absolute = os.path.abspath("store.json")
            redundant = os.path.join(
                os.path.dirname(absolute), ".", os.path.basename(absolute)
            )
            threads = [
                threading.Thread(
                    target=worker, args=("store.json", "shared", "same-id")
                ),
                threading.Thread(
                    target=worker, args=(absolute, "shared", "same-id")
                ),
                threading.Thread(
                    target=worker, args=(redundant, "shared", "same-id")
                ),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
            self.assertEqual(errors, [])
            self.assertEqual(winners, ["same-id"])
            with open("store.json", encoding="utf-8") as handle:
                payload = json.load(handle)
            self.assertEqual(len(payload["jobs"]), 1)
            self.assertEqual(len(payload["idempotency"]), 1)
        finally:
            os.chdir(cwd)

    def test_concurrent_distinct_keys_are_all_registered(self) -> None:
        count = 20
        errors: list[BaseException] = []

        def worker(index: int) -> None:
            try:
                jobs.register(self.path, _job(f"j{index}"), f"k{index}")
            except BaseException as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        self.assertEqual(errors, [])
        with open(self.path, encoding="utf-8") as handle:
            payload = json.load(handle)
        self.assertEqual(len(payload["jobs"]), count)
        self.assertEqual(len(payload["idempotency"]), count)

    def test_write_failure_does_not_corrupt_store(self) -> None:
        jobs.register(self.path, _job(), "k1")
        with open(self.path, "rb") as handle:
            before = handle.read()
        with mock.patch("carbon_market.jobs.os.replace", side_effect=OSError):
            with self.assertRaises(OSError):
                jobs.register(self.path, _job("j2"), "k2")
        with open(self.path, "rb") as handle:
            self.assertEqual(handle.read(), before)

    def test_returned_entry_is_a_copy(self) -> None:
        entry, _ = jobs.register(self.path, _job(), "k1")
        entry["residency_regions"].append("hack")
        entry["deadline"] = 999
        again, created = jobs.register(self.path, _job(), "k1")
        self.assertFalse(created)
        self.assertEqual(again["deadline"], 100)
        self.assertEqual(again["residency_regions"], ["eu-north", "jp-east"])


if __name__ == "__main__":
    unittest.main()
