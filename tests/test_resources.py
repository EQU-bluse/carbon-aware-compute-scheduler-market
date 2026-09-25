from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import jobs as jobs_module
from carbon_market import resources as resources_module
from carbon_market.resources import feasible, get, publish


def _resource(resource_id: str = "r-1", **overrides: object) -> dict[str, object]:
    resource: dict[str, object] = {
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


class PublishTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "supply.json")

    def test_creates_file_and_returns_record(self) -> None:
        record, created = publish(self.path, _resource(), "key-1")
        self.assertTrue(created)
        self.assertEqual(record, {
            "resource_id": "r-1",
            "version": 1,
            "region": "eu-north",
            "capacity": 100,
            "start": 10,
            "end": 500,
            "unit_cost": 3,
            "carbon_intensity": 7,
            "residency": ["eu-north"],
        })
        raw = Path(self.path).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()),
                         ["version", "history", "idempotency", "audit"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["history"].keys()), ["r-1"])
        self.assertEqual(list(data["history"]["r-1"][0].keys()),
                         ["resource_id", "version", "region", "capacity",
                          "start", "end", "unit_cost", "carbon_intensity",
                          "residency"])
        self.assertEqual(data["idempotency"], {"key-1": "r-1"})
        self.assertEqual(data["audit"], {"key-1": {
            "key": "key-1", "resource_id": "r-1", "version": 1}})

    def test_compact_utf8_json(self) -> None:
        publish(self.path, _resource(resource_id="r-中"), "键")
        raw = Path(self.path).read_text(encoding="utf-8")
        self.assertIn("r-中", raw)
        self.assertIn("键", raw)
        self.assertNotIn(" ", raw.split("\n", 1)[0])

    def test_versions_increase_per_resource(self) -> None:
        first, _ = publish(self.path, _resource(start=10), "k1")
        second, created = publish(self.path, _resource(start=20), "k2")
        self.assertTrue(created)
        self.assertEqual(first["version"], 1)
        self.assertEqual(second["version"], 2)
        other, _ = publish(self.path, _resource("r-2", start=0), "k3")
        self.assertEqual(other["version"], 1)
        data = json.loads(Path(self.path).read_text(encoding="utf-8"))
        self.assertEqual(list(data["history"].keys()), ["r-1", "r-2"])
        self.assertEqual([r["version"] for r in data["history"]["r-1"]],
                         [1, 2])
        self.assertEqual(data["audit"]["k2"],
                         {"key": "k2", "resource_id": "r-1", "version": 2})

    def test_start_must_increase_across_versions(self) -> None:
        publish(self.path, _resource(start=10), "k1")
        with self.assertRaises(ValueError):
            publish(self.path, _resource(start=10), "k2")
        with self.assertRaises(ValueError):
            publish(self.path, _resource(start=5), "k3")
        record, created = publish(self.path, _resource(start=11), "k4")
        self.assertTrue(created)
        self.assertEqual(record["version"], 2)

    def test_idempotent_replay_returns_original_without_write(self) -> None:
        first, created_first = publish(self.path, _resource(), "key")
        before = Path(self.path).read_bytes()
        second, created_second = publish(self.path, _resource(), "key")
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first, second)
        self.assertEqual(Path(self.path).read_bytes(), before)

    def test_same_key_different_resource_raises(self) -> None:
        publish(self.path, _resource(), "key")
        for changed in (_resource(region="us-west",
                                  residency=["us-west"]),
                        _resource(capacity=50),
                        _resource(start=11),
                        _resource(end=400),
                        _resource(unit_cost=4),
                        _resource(carbon_intensity=8),
                        _resource("r-2")):
            with self.subTest(changed=changed):
                with self.assertRaises(ValueError):
                    publish(self.path, changed, "key")

    def test_distinct_resources_and_keys_persist_sorted(self) -> None:
        publish(self.path, _resource("r3"), "k3")
        publish(self.path, _resource("r1"), "k1")
        publish(self.path, _resource("r2"), "k2")
        data = json.loads(Path(self.path).read_text(encoding="utf-8"))
        self.assertEqual(list(data["history"].keys()), ["r1", "r2", "r3"])
        self.assertEqual(list(data["idempotency"].keys()), ["k1", "k2", "k3"])
        self.assertEqual(list(data["audit"].keys()), ["k1", "k2", "k3"])

    def test_invalid_arguments(self) -> None:
        cases = [
            ("empty id", _resource(resource_id="")),
            ("non-string id", _resource(resource_id=1)),
            ("empty region", _resource(region="", residency=[""])),
            ("zero capacity", _resource(capacity=0)),
            ("negative capacity", _resource(capacity=-1)),
            ("bool capacity", _resource(capacity=True)),
            ("float capacity", _resource(capacity=1.5)),
            ("negative start", _resource(start=-1)),
            ("bool start", _resource(start=False)),
            ("start after end", _resource(start=10, end=9)),
            ("negative unit_cost", _resource(unit_cost=-1)),
            ("bool carbon", _resource(carbon_intensity=True)),
            ("empty residency", _resource(residency=[])),
            ("duplicate residency",
             _resource(residency=["eu-north", "eu-north"])),
            ("unsorted residency",
             _resource(region="a", residency=["a", "A"])),
            ("residency misses region", _resource(residency=["us-west"])),
            ("empty residency entry", _resource(residency=["eu-north", ""])),
            ("extra field", _resource(state="queued")),
            ("missing field", {"resource_id": "r", "region": "a",
                               "capacity": 1, "start": 0, "end": 1,
                               "unit_cost": 0, "carbon_intensity": 0}),
            ("not a dict", ["r", "a", 1, 0, 1, 0, 0, ["a"]]),
        ]
        for label, bad_resource in cases:
            with self.subTest(label):
                with self.assertRaises(ValueError):
                    publish(self.path, bad_resource, "key")  # type: ignore[arg-type]

        with self.assertRaises(ValueError):
            publish(self.path, _resource(), "")
        with self.assertRaises(ValueError):
            publish(self.path, _resource(), 123)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            publish("", _resource(), "key")
        with self.assertRaises(ValueError):
            publish(123, _resource(), "key")  # type: ignore[arg-type]
        self.assertFalse(os.path.exists(self.path))

    def test_missing_parent_directory_raises_file_not_found(self) -> None:
        missing = os.path.join(self.tmp.name, "no-such-dir", "supply.json")
        with self.assertRaises(FileNotFoundError):
            publish(missing, _resource(), "key")

    def test_corrupt_file_raises_value_error_and_keeps_bytes(self) -> None:
        for bad in ("{not json", '{"version": -0}', '{"version": NaN}'):
            with self.subTest(bad=bad):
                Path(self.path).write_text(bad, encoding="utf-8")
                with self.assertRaises(ValueError):
                    publish(self.path, _resource(), "key")
                self.assertEqual(Path(self.path).read_text(encoding="utf-8"),
                                 bad)

    def test_invalid_structures_raise_value_error(self) -> None:
        record = {"resource_id": "r", "version": 1, "region": "a",
                  "capacity": 1, "start": 0, "end": 1, "unit_cost": 0,
                  "carbon_intensity": 0, "residency": ["a"]}
        event = {"key": "k", "resource_id": "r", "version": 1}
        bad_payloads = [
            [],
            {},
            {"version": 1, "history": {}, "idempotency": {}, "audit": {},
             "extra": 1},
            {"version": 2, "history": {}, "idempotency": {}, "audit": {}},
            {"version": True, "history": {}, "idempotency": {}, "audit": {}},
            {"version": 1, "history": [], "idempotency": {}, "audit": {}},
            {"version": 1, "history": {"r": []}, "idempotency": {},
             "audit": {}},
            {"version": 1, "history": {"r": [dict(record, version=2)]},
             "idempotency": {"k": "r"}, "audit": {"k": event}},
            {"version": 1, "history": {"r": [dict(record, region="")]},
             "idempotency": {"k": "r"}, "audit": {"k": event}},
            {"version": 1,
             "history": {"r": [dict(record, residency=["b"])]},
             "idempotency": {"k": "r"}, "audit": {"k": event}},
            {"version": 1,
             "history": {"r": [record, dict(record, version=2, start=0)]},
             "idempotency": {"k": "r"},
             "audit": {"k": event}},
            {"version": 1, "history": {"r": [record]},
             "idempotency": {"k": "missing"}, "audit": {"k": event}},
            {"version": 1, "history": {"r": [record]},
             "idempotency": {"k": "r"}, "audit": {}},
            {"version": 1, "history": {"r": [record]},
             "idempotency": {"k": "r"},
             "audit": {"k": dict(event, version=2)}},
            {"version": 1, "history": {"r": [record]},
             "idempotency": {"k": "r"},
             "audit": {"k": dict(event, resource_id="other")}},
            {"version": 1,
             "history": {"b": [dict(record, resource_id="b")],
                         "a": [dict(record, resource_id="a")]},
             "idempotency": {"k": "a"},
             "audit": {"k": dict(event, resource_id="a")}},
        ]
        for payload in bad_payloads:
            with self.subTest(payload=payload):
                Path(self.path).write_text(json.dumps(payload),
                                           encoding="utf-8")
                with self.assertRaises(ValueError):
                    publish(self.path, _resource(), "key2")

    def test_concurrent_first_publications_serialize(self) -> None:
        errors: list[BaseException] = []

        def worker(index: int) -> None:
            try:
                publish(self.path, _resource(f"r{index:03d}"), f"k{index:03d}")
            except BaseException as exc:  # noqa: BLE001 - report all
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(30)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        self.assertEqual(errors, [])
        data = json.loads(Path(self.path).read_text(encoding="utf-8"))
        self.assertEqual(len(data["history"]), 30)
        self.assertEqual(len(data["idempotency"]), 30)
        self.assertEqual(len(data["audit"]), 30)

    def test_no_tmp_files_left_behind(self) -> None:
        publish(self.path, _resource(), "k")
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    def test_failed_directory_sync_restores_previous_bytes(self) -> None:
        publish(self.path, _resource(), "k1")
        before = Path(self.path).read_bytes()
        original = resources_module._fsync_directory

        def failing(directory: str) -> None:
            raise OSError("simulated sync failure")

        resources_module._fsync_directory = failing
        try:
            with self.assertRaises(OSError):
                publish(self.path, _resource("r-2"), "k2")
        finally:
            resources_module._fsync_directory = original
        self.assertEqual(Path(self.path).read_bytes(), before)
        record = get(self.path, "r-1")
        self.assertEqual(record["version"], 1)
        with self.assertRaises(KeyError):
            get(self.path, "r-2")

    def test_failed_first_commit_leaves_no_file(self) -> None:
        original = resources_module._fsync_directory

        def failing(directory: str) -> None:
            raise OSError("simulated sync failure")

        resources_module._fsync_directory = failing
        try:
            with self.assertRaises(OSError):
                publish(self.path, _resource(), "k1")
        finally:
            resources_module._fsync_directory = original
        self.assertFalse(os.path.exists(self.path))
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])


class GetTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "supply.json")
        publish(self.path, _resource("r-1", start=10, unit_cost=3), "k1")
        publish(self.path, _resource("r-1", start=20, unit_cost=5), "k2")
        publish(self.path, _resource("r-2", start=0), "k3")

    def test_latest_version_by_default(self) -> None:
        record = get(self.path, "r-1")
        self.assertEqual(record["version"], 2)
        self.assertEqual(record["unit_cost"], 5)

    def test_specific_version(self) -> None:
        record = get(self.path, "r-1", 1)
        self.assertEqual(record["version"], 1)
        self.assertEqual(record["unit_cost"], 3)

    def test_unknown_resource_and_version_raise_key_error(self) -> None:
        with self.assertRaises(KeyError):
            get(self.path, "r-9")
        with self.assertRaises(KeyError):
            get(self.path, "r-1", 3)
        with self.assertRaises(KeyError):
            get(self.path, "r-2", 2)

    def test_missing_file_raises_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            get(os.path.join(self.tmp.name, "missing.json"), "r-1")

    def test_invalid_arguments(self) -> None:
        with self.assertRaises(ValueError):
            get("", "r-1")
        with self.assertRaises(ValueError):
            get(self.path, "")
        with self.assertRaises(ValueError):
            get(self.path, "r-1", 0)
        with self.assertRaises(ValueError):
            get(self.path, "r-1", -1)
        with self.assertRaises(ValueError):
            get(self.path, "r-1", True)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            get(self.path, "r-1", 1.5)  # type: ignore[arg-type]

    def test_invalid_file_raises_value_error(self) -> None:
        Path(self.path).write_text("{not json", encoding="utf-8")
        with self.assertRaises(ValueError):
            get(self.path, "r-1")

    def test_returned_record_is_a_copy(self) -> None:
        record = get(self.path, "r-1")
        record["unit_cost"] = 999
        record["residency"].append("x")  # type: ignore[attr-defined]
        again = get(self.path, "r-1")
        self.assertEqual(again["unit_cost"], 5)
        self.assertEqual(again["residency"], ["eu-north"])


class FeasibleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs_path = os.path.join(self.tmp.name, "jobs.json")
        self.supply_path = os.path.join(self.tmp.name, "supply.json")
        jobs_module.submit(self.jobs_path, _job(), "jk-1")

    def test_ranks_candidates_and_returns_totals(self) -> None:
        publish(self.supply_path,
                _resource("r-1", carbon_intensity=7, unit_cost=3), "k1")
        publish(self.supply_path,
                _resource("r-2", carbon_intensity=2, unit_cost=9), "k2")
        publish(self.supply_path,
                _resource("r-3", carbon_intensity=2, unit_cost=4), "k3")
        result = feasible(self.jobs_path, self.supply_path, "j-1", 50)
        self.assertEqual([entry["resource"]["resource_id"]
                          for entry in result], ["r-3", "r-2", "r-1"])
        first = result[0]
        self.assertEqual(list(first.keys()),
                         ["resource", "total_cost", "total_carbon"])
        self.assertEqual(first["total_cost"], 10 * 4)
        self.assertEqual(first["total_carbon"], 10 * 2)

    def test_tie_breaks_on_unit_cost_then_resource_id(self) -> None:
        publish(self.supply_path,
                _resource("r-b", carbon_intensity=5, unit_cost=5), "k1")
        publish(self.supply_path,
                _resource("r-a", carbon_intensity=5, unit_cost=5), "k2")
        publish(self.supply_path,
                _resource("r-c", carbon_intensity=5, unit_cost=1), "k3")
        result = feasible(self.jobs_path, self.supply_path, "j-1", 50)
        self.assertEqual([entry["resource"]["resource_id"]
                          for entry in result], ["r-c", "r-a", "r-b"])

    def test_uses_highest_version_valid_at_evaluation_time(self) -> None:
        publish(self.supply_path,
                _resource("r-1", start=10, end=100, carbon_intensity=9),
                "k1")
        publish(self.supply_path,
                _resource("r-1", start=200, end=300, carbon_intensity=1),
                "k2")
        early = feasible(self.jobs_path, self.supply_path, "j-1", 50)
        self.assertEqual(early[0]["resource"]["version"], 1)
        late = feasible(self.jobs_path, self.supply_path, "j-1", 250)
        self.assertEqual(late[0]["resource"]["version"], 2)
        self.assertEqual(late[0]["total_carbon"], 10 * 1)
        # Outside every validity window the resource contributes nothing.
        self.assertEqual(feasible(self.jobs_path, self.supply_path, "j-1", 5),
                         [])
        self.assertEqual(feasible(self.jobs_path, self.supply_path, "j-1",
                                  150), [])

    def test_filters_region_capacity_residency_and_deadline(self) -> None:
        publish(self.supply_path, _resource("ok"), "k1")
        publish(self.supply_path,
                _resource("wrong-region", region="ap-south",
                          residency=["ap-south"]), "k2")
        publish(self.supply_path, _resource("small", capacity=9), "k3")
        publish(self.supply_path,
                _resource("no-residency",
                          residency=["eu-north", "us-west"],
                          region="us-west"), "k4")
        publish(self.supply_path, _resource("short", end=99), "k5")
        result = feasible(self.jobs_path, self.supply_path, "j-1", 50)
        # Equal carbon intensity and unit cost: resource id breaks the tie.
        self.assertEqual([entry["resource"]["resource_id"]
                          for entry in result], ["no-residency", "ok"])

        jobs_module.submit(self.jobs_path,
                           _job("j-2", residency=["eu-north", "us-west"]),
                           "jk-2")
        stricter = feasible(self.jobs_path, self.supply_path, "j-2", 50)
        self.assertEqual([entry["resource"]["resource_id"]
                          for entry in stricter], ["no-residency"])

    def test_budget_exclusion_and_boundary(self) -> None:
        publish(self.supply_path, _resource("cheap", unit_cost=100,
                                            carbon_intensity=100), "k1")
        publish(self.supply_path, _resource("costly", unit_cost=101), "k2")
        publish(self.supply_path, _resource("dirty", carbon_intensity=101),
                "k3")
        result = feasible(self.jobs_path, self.supply_path, "j-1", 50)
        # work is 10 and both budgets are 1000: exactly at the budget
        # stays, anything above is excluded.
        self.assertEqual([entry["resource"]["resource_id"]
                          for entry in result], ["cheap"])

    def test_no_candidates_returns_empty_list(self) -> None:
        publish(self.supply_path, _resource("small", capacity=1), "k1")
        self.assertEqual(
            feasible(self.jobs_path, self.supply_path, "j-1", 50), [])

    def test_unknown_job_raises_key_error(self) -> None:
        publish(self.supply_path, _resource(), "k1")
        with self.assertRaises(KeyError):
            feasible(self.jobs_path, self.supply_path, "j-9", 50)

    def test_register_file_is_invalid_structure(self) -> None:
        register_path = os.path.join(self.tmp.name, "registry.json")
        jobs_module.register(register_path, {
            "job_id": "j-1", "deadline": 100, "energy_wh": 250,
            "residency_regions": ["eu-north"],
        }, "rk-1")
        publish(self.supply_path, _resource(), "k1")
        with self.assertRaises(ValueError):
            feasible(register_path, self.supply_path, "j-1", 50)

    def test_missing_files_raise_file_not_found(self) -> None:
        publish(self.supply_path, _resource(), "k1")
        with self.assertRaises(FileNotFoundError):
            feasible(os.path.join(self.tmp.name, "no-jobs.json"),
                     self.supply_path, "j-1", 50)
        with self.assertRaises(FileNotFoundError):
            feasible(self.jobs_path,
                     os.path.join(self.tmp.name, "no-supply.json"),
                     "j-1", 50)

    def test_invalid_files_raise_value_error(self) -> None:
        Path(self.supply_path).write_text("{not json", encoding="utf-8")
        with self.assertRaises(ValueError):
            feasible(self.jobs_path, self.supply_path, "j-1", 50)
        os.unlink(self.supply_path)
        publish(self.supply_path, _resource(), "k1")
        Path(self.jobs_path).write_text('{"version": -0}', encoding="utf-8")
        with self.assertRaises(ValueError):
            feasible(self.jobs_path, self.supply_path, "j-1", 50)

    def test_invalid_arguments(self) -> None:
        publish(self.supply_path, _resource(), "k1")
        with self.assertRaises(ValueError):
            feasible("", self.supply_path, "j-1", 50)
        with self.assertRaises(ValueError):
            feasible(self.jobs_path, "", "j-1", 50)
        with self.assertRaises(ValueError):
            feasible(self.jobs_path, self.supply_path, "", 50)
        with self.assertRaises(ValueError):
            feasible(self.jobs_path, self.supply_path, "j-1", -1)
        with self.assertRaises(ValueError):
            feasible(self.jobs_path, self.supply_path, "j-1", True)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            feasible(self.jobs_path, self.supply_path, "j-1", 1.5)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
