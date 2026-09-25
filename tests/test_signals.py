from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market.signals import get, publish


def _signal(region: str = "eu-north", **overrides: object) -> dict[str, object]:
    signal: dict[str, object] = {
        "region": region,
        "observed_at": 10,
        "expires_at": 500,
        "energy_mix": {"solar": 4000, "wind": 6000},
        "unit_cost": 3,
        "carbon_intensity": 7,
    }
    signal.update(overrides)
    return signal


class PublishTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "signals.json")

    def test_creates_file_and_returns_record(self) -> None:
        record, created = publish(self.path, _signal(), "key-1")
        self.assertTrue(created)
        self.assertEqual(record, {
            "version": 1,
            "region": "eu-north",
            "observed_at": 10,
            "expires_at": 500,
            "energy_mix": {"solar": 4000, "wind": 6000},
            "unit_cost": 3,
            "carbon_intensity": 7,
        })
        self.assertEqual(list(record.keys()),
                         ["version", "region", "observed_at", "expires_at",
                          "energy_mix", "unit_cost", "carbon_intensity"])
        raw = Path(self.path).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()),
                         ["version", "history", "idempotency", "audit"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["history"]["eu-north"][0].keys()),
                         ["version", "region", "observed_at", "expires_at",
                          "energy_mix", "unit_cost", "carbon_intensity"])
        self.assertEqual(data["idempotency"], {"key-1": "eu-north"})
        self.assertEqual(data["audit"], {"key-1": {
            "key": "key-1", "region": "eu-north", "version": 1}})

    def test_mix_sources_are_sorted_by_code_point(self) -> None:
        record, _ = publish(self.path,
                            _signal(energy_mix={"wind": 1, "solar": 9999}),
                            "k")
        self.assertEqual(list(record["energy_mix"]), ["solar", "wind"])

    def test_compact_utf8_json(self) -> None:
        publish(self.path, _signal(region="eu-中",
                                   energy_mix={"风": 5000, "光": 5000}), "键")
        raw = Path(self.path).read_text(encoding="utf-8")
        self.assertIn("eu-中", raw)
        self.assertIn("风", raw)
        self.assertNotIn(" ", raw.split("\n", 1)[0])

    def test_versions_increase_per_region(self) -> None:
        first, _ = publish(self.path, _signal(observed_at=10), "k1")
        second, created = publish(self.path, _signal(observed_at=20), "k2")
        self.assertTrue(created)
        self.assertEqual(first["version"], 1)
        self.assertEqual(second["version"], 2)
        other, _ = publish(self.path, _signal("us-west", observed_at=0), "k3")
        self.assertEqual(other["version"], 1)
        data = json.loads(Path(self.path).read_text(encoding="utf-8"))
        self.assertEqual(list(data["history"].keys()),
                         ["eu-north", "us-west"])
        self.assertEqual([r["version"] for r in data["history"]["eu-north"]],
                         [1, 2])

    def test_observed_at_must_strictly_increase(self) -> None:
        publish(self.path, _signal(observed_at=10), "k1")
        with self.assertRaises(ValueError):
            publish(self.path, _signal(observed_at=10), "k2")
        with self.assertRaises(ValueError):
            publish(self.path, _signal(observed_at=5), "k3")
        record, created = publish(self.path, _signal(observed_at=11), "k4")
        self.assertTrue(created)
        self.assertEqual(record["version"], 2)

    def test_expires_may_equal_observed(self) -> None:
        record, created = publish(
            self.path, _signal(observed_at=5, expires_at=5), "k")
        self.assertTrue(created)
        self.assertEqual(get(self.path, "eu-north", 5)["version"],
                         record["version"])

    def test_idempotent_replay_returns_original_without_write(self) -> None:
        first, created_first = publish(self.path, _signal(), "key")
        before = Path(self.path).read_bytes()
        second, created_second = publish(self.path, _signal(), "key")
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first, second)
        self.assertEqual(Path(self.path).read_bytes(), before)

    def test_replay_equivalent_regardless_of_mix_order(self) -> None:
        first, created_first = publish(
            self.path, _signal(energy_mix={"a": 1, "b": 9999}), "key")
        second, created_second = publish(
            self.path, _signal(energy_mix={"b": 9999, "a": 1}), "key")
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first, second)

    def test_same_key_different_signal_raises(self) -> None:
        publish(self.path, _signal(), "key")
        for changed in (_signal(region="us-west"),
                        _signal(observed_at=11),
                        _signal(expires_at=499),
                        _signal(unit_cost=4),
                        _signal(carbon_intensity=8),
                        _signal(energy_mix={"a": 5000, "b": 5000})):
            with self.subTest(changed=changed):
                with self.assertRaises(ValueError):
                    publish(self.path, changed, "key")

    def test_invalid_arguments(self) -> None:
        cases = [
            ("empty region", _signal(region="")),
            ("non-string region", _signal(region=1)),
            ("negative observed", _signal(observed_at=-1)),
            ("bool observed", _signal(observed_at=True)),
            ("float observed", _signal(observed_at=1.5)),
            ("negative expires", _signal(observed_at=4, expires_at=-1)),
            ("expires before observed",
             _signal(observed_at=10, expires_at=9)),
            ("negative unit_cost", _signal(unit_cost=-1)),
            ("bool cost", _signal(unit_cost=False)),
            ("negative carbon", _signal(carbon_intensity=-1)),
            ("bool carbon", _signal(carbon_intensity=True)),
            ("empty mix", _signal(energy_mix={})),
            ("mix not object", _signal(energy_mix=[10000])),
            ("empty source", _signal(energy_mix={"": 10000})),
            ("negative share", _signal(energy_mix={"a": -1, "b": 10001})),
            ("bool share", _signal(energy_mix={"a": True, "b": 9999})),
            ("wrong total", _signal(energy_mix={"a": 1})),
            ("zero total", _signal(energy_mix={"a": 0})),
            ("extra field", _signal(extra=1)),
            ("missing field", {"region": "r", "observed_at": 0,
                               "expires_at": 1, "unit_cost": 0,
                               "carbon_intensity": 0}),
            ("not a dict", ["r", 0, 1, {}, 0, 0]),
        ]
        for label, bad in cases:
            with self.subTest(label):
                with self.assertRaises(ValueError):
                    publish(self.path, bad, "key")  # type: ignore[arg-type]

        with self.assertRaises(ValueError):
            publish(self.path, _signal(), "")
        with self.assertRaises(ValueError):
            publish(self.path, _signal(), 123)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            publish("", _signal(), "key")
        with self.assertRaises(ValueError):
            publish(123, _signal(), "key")  # type: ignore[arg-type]
        self.assertFalse(os.path.exists(self.path))

    def test_missing_parent_directory_raises_file_not_found(self) -> None:
        missing = os.path.join(self.tmp.name, "no-such-dir", "signals.json")
        with self.assertRaises(FileNotFoundError):
            publish(missing, _signal(), "key")

    def test_corrupt_file_raises_value_error_and_keeps_bytes(self) -> None:
        for bad in ("{not json", '{"version": -0}', '{"version": NaN}'):
            with self.subTest(bad=bad):
                Path(self.path).write_text(bad, encoding="utf-8")
                with self.assertRaises(ValueError):
                    publish(self.path, _signal(), "key")
                self.assertEqual(Path(self.path).read_text(encoding="utf-8"),
                                 bad)

    def test_concurrent_first_publications_serialize(self) -> None:
        errors: list[BaseException] = []

        def worker(index: int) -> None:
            try:
                publish(self.path,
                        _signal(f"r{index:03d}", observed_at=index),
                        f"k{index:03d}")
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

    def test_failed_first_commit_leaves_no_file(self) -> None:
        from carbon_market import signals as signals_module
        original = signals_module._fsync_directory

        def failing(directory: str) -> None:
            raise OSError("simulated sync failure")

        signals_module._fsync_directory = failing
        try:
            with self.assertRaises(OSError):
                publish(self.path, _signal(), "k1")
        finally:
            signals_module._fsync_directory = original
        self.assertFalse(os.path.exists(self.path))
        self.assertEqual([n for n in os.listdir(self.tmp.name)
                          if n.endswith(".tmp")], [])


class GetTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "signals.json")
        publish(self.path, _signal("eu-north", observed_at=10,
                                   expires_at=100), "k1")
        publish(self.path, _signal("eu-north", observed_at=200,
                                   expires_at=300), "k2")
        publish(self.path, _signal("us-west", observed_at=0,
                                   expires_at=1000), "k3")

    def test_returns_latest_unexpired_version(self) -> None:
        self.assertEqual(get(self.path, "eu-north", 50)["version"], 1)
        self.assertEqual(get(self.path, "eu-north", 100)["version"], 1)
        self.assertEqual(get(self.path, "eu-north", 200)["version"], 2)
        self.assertEqual(get(self.path, "eu-north", 250)["version"], 2)

    def test_before_observation_is_not_valid(self) -> None:
        # At 5 no eu-north version has been observed yet.
        with self.assertRaises(LookupError):
            get(self.path, "eu-north", 5)

    def test_expired_gap_raises_lookup_error(self) -> None:
        with self.assertRaises(LookupError):
            get(self.path, "eu-north", 150)
        with self.assertRaises(LookupError):
            get(self.path, "eu-north", 301)

    def test_unknown_region_raises_key_error(self) -> None:
        with self.assertRaises(KeyError):
            get(self.path, "mars", 50)

    def test_missing_file_raises_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            get(os.path.join(self.tmp.name, "missing.json"), "eu-north", 50)

    def test_invalid_arguments(self) -> None:
        with self.assertRaises(ValueError):
            get("", "eu-north", 50)
        with self.assertRaises(ValueError):
            get(self.path, "", 50)
        with self.assertRaises(ValueError):
            get(self.path, "eu-north", -1)
        with self.assertRaises(ValueError):
            get(self.path, "eu-north", True)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            get(self.path, "eu-north", 1.5)  # type: ignore[arg-type]

    def test_invalid_file_raises_value_error(self) -> None:
        Path(self.path).write_text("{not json", encoding="utf-8")
        with self.assertRaises(ValueError):
            get(self.path, "eu-north", 50)

    def test_non_canonical_bytes_rejected(self) -> None:
        data = json.loads(Path(self.path).read_text(encoding="utf-8"))
        Path(self.path).write_text(json.dumps(data, indent=2),
                                   encoding="utf-8")
        with self.assertRaises(ValueError):
            get(self.path, "eu-north", 50)

    def test_returned_record_is_a_copy(self) -> None:
        record = get(self.path, "eu-north", 50)
        record["unit_cost"] = 999
        record["energy_mix"]["x"] = 1
        again = get(self.path, "eu-north", 50)
        self.assertEqual(again["unit_cost"], 3)
        self.assertNotIn("x", again["energy_mix"])


if __name__ == "__main__":
    unittest.main()
