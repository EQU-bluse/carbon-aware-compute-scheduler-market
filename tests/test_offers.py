from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import offers as offers_module
from carbon_market.offers import register


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


class RegisterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "registry.json")

    def test_creates_file_and_returns_record(self) -> None:
        record, created = register(self.path, _offer(), "key-1")
        self.assertTrue(created)
        self.assertEqual(record, {
            "resource_id": "r-1",
            "region": "eu-north",
            "capacity_wh": 250,
            "unit_cost": 1000,
            "carbon_intensity": 42,
        })
        self.assertEqual(list(record.keys()),
                         ["resource_id", "region", "capacity_wh",
                          "unit_cost", "carbon_intensity"])
        self.assertTrue(os.path.exists(self.path))
        raw = Path(self.path).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()), ["version", "offers", "idempotency"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["offers"].keys()), ["r-1"])
        self.assertEqual(list(data["offers"]["r-1"].keys()),
                         ["resource_id", "region", "capacity_wh",
                          "unit_cost", "carbon_intensity"])
        self.assertEqual(data["idempotency"],
                         {"key-1": {"resource_id": "r-1"}})

    def test_compact_utf8_json(self) -> None:
        register(self.path, _offer(resource_id="r-中", region="北"), "键")
        raw = Path(self.path).read_text(encoding="utf-8")
        self.assertIn("r-中", raw)
        self.assertIn("北", raw)
        self.assertIn("键", raw)
        self.assertNotIn(" ", raw.split("\n", 1)[0])

    def test_idempotent_replay(self) -> None:
        first, created_first = register(self.path, _offer(), "key")
        second, created_second = register(self.path, _offer(), "key")
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first, second)
        data = json.loads(Path(self.path).read_text(encoding="utf-8"))
        self.assertEqual(len(data["offers"]), 1)
        self.assertEqual(len(data["idempotency"]), 1)

    def test_same_key_different_offer_raises(self) -> None:
        register(self.path, _offer(), "key")
        with self.assertRaises(ValueError):
            register(self.path, _offer(region="other"), "key")
        with self.assertRaises(ValueError):
            register(self.path, _offer(capacity_wh=1), "key")
        with self.assertRaises(ValueError):
            register(self.path, _offer(unit_cost=1), "key")
        with self.assertRaises(ValueError):
            register(self.path, _offer(carbon_intensity=1), "key")

    def test_resource_id_taken_by_other_key_raises(self) -> None:
        register(self.path, _offer(), "key-1")
        with self.assertRaises(ValueError):
            register(self.path, _offer(), "key-2")

    def test_distinct_offers_and_keys_persist_sorted(self) -> None:
        register(self.path, _offer(resource_id="r3"), "k3")
        register(self.path, _offer(resource_id="r1"), "k1")
        register(self.path, _offer(resource_id="r2"), "k2")
        data = json.loads(Path(self.path).read_text(encoding="utf-8"))
        self.assertEqual(list(data["offers"].keys()), ["r1", "r2", "r3"])
        self.assertEqual(list(data["idempotency"].keys()),
                         ["k1", "k2", "k3"])

    def test_zero_boundaries_accepted(self) -> None:
        record, created = register(
            self.path,
            _offer(capacity_wh=1, unit_cost=0, carbon_intensity=0), "k")
        self.assertTrue(created)
        self.assertEqual(record["unit_cost"], 0)
        self.assertEqual(record["carbon_intensity"], 0)

    def test_invalid_arguments(self) -> None:
        cases = [
            ("bad resource id", _offer(resource_id="")),
            ("bad region", _offer(region="")),
            ("bool capacity", _offer(capacity_wh=True)),
            ("zero capacity", _offer(capacity_wh=0)),
            ("negative capacity", _offer(capacity_wh=-3)),
            ("float capacity", _offer(capacity_wh=1.5)),
            ("bool unit_cost", _offer(unit_cost=False)),
            ("negative unit_cost", _offer(unit_cost=-1)),
            ("float unit_cost", _offer(unit_cost=1.5)),
            ("bool carbon", _offer(carbon_intensity=True)),
            ("negative carbon", _offer(carbon_intensity=-1)),
            ("extra field", _offer(resource_id="x", state="open")),
            ("missing field", {"resource_id": "x", "region": "r",
                               "capacity_wh": 1, "unit_cost": 2}),
            ("not a dict", ["x", "r", 1, 2, 3]),
        ]
        for label, bad_offer in cases:
            with self.subTest(label):
                with self.assertRaises(ValueError):
                    register(self.path, bad_offer, "key")  # type: ignore[arg-type]

        with self.assertRaises(ValueError):
            register(self.path, _offer(), "")
        with self.assertRaises(ValueError):
            register(self.path, _offer(), 123)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            register(123, _offer(), "key")  # type: ignore[arg-type]

    def test_missing_parent_directory_raises_file_not_found(self) -> None:
        missing = os.path.join(self.tmp.name, "no-such-dir", "registry.json")
        with self.assertRaises(FileNotFoundError):
            register(missing, _offer(), "key")

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
                    register(path,
                             _offer(resource_id=f"r{index:03d}"), f"k{index:03d}")
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
            self.assertEqual(len(data["offers"]), 60)
            self.assertEqual(len(data["idempotency"]), 60)
        finally:
            os.chdir(cwd)

    def test_concurrent_replays(self) -> None:
        register(self.path, _offer(), "key")
        results: list[tuple[bool, dict[str, object]]] = []
        lock = threading.Lock()

        def worker() -> None:
            record, created = register(self.path, _offer(), "key")
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
        self.assertEqual(len(data["offers"]), 1)

    def test_corrupt_file_raises_value_error(self) -> None:
        Path(self.path).write_text("{not json", encoding="utf-8")
        with self.assertRaises(ValueError):
            register(self.path, _offer(), "key")

    def test_negative_zero_literals_raise_value_error(self) -> None:
        for literal in ["-0", "-0.0", "-0e2", "-0E2", "-0.0e0", "-00"]:
            with self.subTest(literal):
                Path(self.path).write_text(
                    f'{{"version":1,"offers":{{"r":{{"resource_id":"r",'
                    f'"region":"x","capacity_wh":{literal},"unit_cost":1,'
                    f'"carbon_intensity":2}}}},"idempotency":{{}}}}',
                    encoding="utf-8")
                with self.assertRaises(ValueError):
                    register(self.path, _offer(resource_id="other"), "k")

    def test_negative_zero_inside_string_is_allowed(self) -> None:
        seed = {
            "version": 1,
            "offers": {
                "r": {"resource_id": "r", "region": "-0", "capacity_wh": 1,
                      "unit_cost": 0, "carbon_intensity": 0},
            },
            "idempotency": {"-0": {"resource_id": "r"}},
        }
        Path(self.path).write_text(json.dumps(seed), encoding="utf-8")
        record, created = register(self.path, _offer(resource_id="r2"), "k2")
        self.assertTrue(created)
        self.assertEqual(record["resource_id"], "r2")
        replay, replayed = register(
            self.path,
            {"resource_id": "r", "region": "-0", "capacity_wh": 1,
             "unit_cost": 0, "carbon_intensity": 0}, "-0")
        self.assertFalse(replayed)

    def test_non_negative_zero_literals_are_fine(self) -> None:
        for literal in ["0", "0.0", "0e2"]:
            # 0.0 would fail integer-field validation, not JSON parsing:
            # ensure it surfaces as ValueError either way without crashing.
            with self.subTest(literal):
                Path(self.path).write_text(
                    f'{{"version":1,"offers":{{"r":{{"resource_id":"r",'
                    f'"region":"x","capacity_wh":{literal},"unit_cost":1,'
                    f'"carbon_intensity":2}}}},"idempotency":{{}}}}',
                    encoding="utf-8")
                with self.assertRaises(ValueError):
                    register(self.path, _offer(resource_id="other"), "k")

    def test_invalid_structures_raise_value_error(self) -> None:
        bad_payloads = [
            [],
            {},
            {"version": 1, "offers": {}, "idempotency": {}, "extra": 1},
            {"version": 2, "offers": {}, "idempotency": {}},
            {"version": "1", "offers": {}, "idempotency": {}},
            {"version": 1, "offers": [], "idempotency": {}},
            {"version": 1, "offers": {}, "idempotency": []},
            {"version": 1,
             "offers": {"r": {"resource_id": "other", "region": "x",
                               "capacity_wh": 1, "unit_cost": 2,
                               "carbon_intensity": 3}},
             "idempotency": {}},
            {"version": 1,
             "offers": {"r": {"resource_id": "r", "region": "",
                               "capacity_wh": 1, "unit_cost": 2,
                               "carbon_intensity": 3}},
             "idempotency": {}},
            {"version": 1,
             "offers": {"r": {"resource_id": "r", "region": "x",
                               "capacity_wh": 0, "unit_cost": 2,
                               "carbon_intensity": 3}},
             "idempotency": {}},
            {"version": 1,
             "offers": {"r": {"resource_id": "r", "region": "x",
                               "capacity_wh": 1, "unit_cost": -1,
                               "carbon_intensity": 3}},
             "idempotency": {}},
            {"version": 1,
             "offers": {"r": {"resource_id": "r", "region": "x",
                               "capacity_wh": 1, "unit_cost": 2,
                               "carbon_intensity": -3}},
             "idempotency": {}},
            {"version": 1,
             "offers": {"r": {"resource_id": "r", "region": "x",
                               "capacity_wh": True, "unit_cost": 2,
                               "carbon_intensity": 3}},
             "idempotency": {}},
            {"version": 1, "offers": {}, "idempotency": {"k": "missing"}},
            {"version": 1, "offers": {},
             "idempotency": {"k": {"resource_id": "missing"}}},
            {"version": 1,
             "offers": {"r": {"resource_id": "r", "region": "x",
                               "capacity_wh": 1, "unit_cost": 2,
                               "carbon_intensity": 3}},
             "idempotency": {"k1": {"resource_id": "r"},
                             "k2": {"resource_id": "r"}}},
            {"version": 1,
             "offers": {"r": {"resource_id": "r", "region": "x",
                               "capacity_wh": 1, "unit_cost": 2,
                               "carbon_intensity": 3}},
             "idempotency": {"k": {"resource_id": "r", "extra": 1}}},
        ]
        for payload in bad_payloads:
            with self.subTest(payload=payload):
                Path(self.path).write_text(json.dumps(payload),
                                           encoding="utf-8")
                with self.assertRaises(ValueError):
                    register(self.path, _offer(), "key")

    def test_valid_existing_file_loads_and_registers(self) -> None:
        seed = {
            "version": 1,
            "offers": {
                "a": {"resource_id": "a", "region": "r1", "capacity_wh": 1,
                      "unit_cost": 0, "carbon_intensity": 0},
            },
            "idempotency": {"k-a": {"resource_id": "a"}},
        }
        Path(self.path).write_text(json.dumps(seed), encoding="utf-8")
        record, created = register(self.path, _offer(resource_id="b"), "k-b")
        self.assertTrue(created)
        self.assertEqual(record["resource_id"], "b")
        replay, replayed = register(
            self.path,
            {"resource_id": "a", "region": "r1", "capacity_wh": 1,
             "unit_cost": 0, "carbon_intensity": 0}, "k-a")
        self.assertFalse(replayed)
        self.assertEqual(replay["region"], "r1")

    def test_reordered_keys_in_existing_file_still_load(self) -> None:
        seed = {
            "idempotency": {"k-a": {"resource_id": "a"}},
            "version": 1,
            "offers": {
                "a": {"carbon_intensity": 0, "unit_cost": 0,
                      "capacity_wh": 1, "region": "r1", "resource_id": "a"},
            },
        }
        Path(self.path).write_text(json.dumps(seed), encoding="utf-8")
        record, created = register(self.path, _offer(resource_id="b"), "k-b")
        self.assertTrue(created)
        self.assertEqual(record["resource_id"], "b")
        raw = json.loads(Path(self.path).read_text(encoding="utf-8"))
        self.assertEqual(list(raw.keys()), ["version", "offers", "idempotency"])

    def test_out_of_order_records_normalized_on_replay_and_rewrite(self) -> None:
        seed = {
            "version": 1,
            "offers": {
                "a": {"carbon_intensity": 0, "unit_cost": 0,
                      "capacity_wh": 1, "region": "r1", "resource_id": "a"},
            },
            "idempotency": {"k-a": {"resource_id": "a"}},
        }
        Path(self.path).write_text(json.dumps(seed), encoding="utf-8")
        replay, replayed = register(
            self.path,
            {"resource_id": "a", "region": "r1", "capacity_wh": 1,
             "unit_cost": 0, "carbon_intensity": 0}, "k-a")
        self.assertFalse(replayed)
        self.assertEqual(list(replay.keys()),
                         ["resource_id", "region", "capacity_wh",
                          "unit_cost", "carbon_intensity"])
        register(self.path, _offer(resource_id="b"), "k-b")
        raw = json.loads(Path(self.path).read_text(encoding="utf-8"))
        self.assertEqual(list(raw["offers"]["a"].keys()),
                         ["resource_id", "region", "capacity_wh",
                          "unit_cost", "carbon_intensity"])

    def test_returned_record_is_a_copy(self) -> None:
        record, _ = register(self.path, _offer(), "k")
        record["unit_cost"] = 999
        replay, _ = register(self.path, _offer(), "k")
        self.assertEqual(replay["unit_cost"], 1000)

    def test_no_tmp_files_left_behind(self) -> None:
        register(self.path, _offer(), "k")
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    def test_store_cache_keyed_by_realpath(self) -> None:
        register(self.path, _offer(), "k")
        alt = os.path.join(self.tmp.name, "sub", "..", "registry.json")
        self.assertEqual(offers_module._get_store(alt).realpath,
                         offers_module._get_store(self.path).realpath)
        self.assertIs(offers_module._get_store(alt),
                      offers_module._get_store(self.path))


if __name__ == "__main__":
    unittest.main()
