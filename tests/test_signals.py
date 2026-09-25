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
        "observed": 10,
        "expires": 100,
        "mix": {"solar": 6000, "wind": 4000},
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
            "region": "eu-north",
            "version": 1,
            "observed": 10,
            "expires": 100,
            "mix": {"solar": 6000, "wind": 4000},
            "unit_cost": 3,
            "carbon_intensity": 7,
        })
        raw = Path(self.path).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()),
                         ["version", "history", "idempotency", "audit"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["history"].keys()), ["eu-north"])
        self.assertEqual(list(data["history"]["eu-north"][0].keys()),
                         ["region", "version", "observed", "expires", "mix",
                          "unit_cost", "carbon_intensity"])
        self.assertEqual(data["idempotency"], {"key-1": "eu-north"})
        self.assertEqual(data["audit"], {"key-1": {
            "key": "key-1", "region": "eu-north", "version": 1}})

    def test_compact_utf8_json(self) -> None:
        publish(self.path, _signal(region="欧北",
                                   mix={"太阳能": 10000}), "键")
        raw = Path(self.path).read_text(encoding="utf-8")
        self.assertIn("欧北", raw)
        self.assertIn("太阳能", raw)
        self.assertNotIn(" ", raw.split("\n", 1)[0])

    def test_versions_increase_per_region(self) -> None:
        first, _ = publish(self.path, _signal(observed=10), "k1")
        second, created = publish(self.path, _signal(observed=20), "k2")
        self.assertTrue(created)
        self.assertEqual(first["version"], 1)
        self.assertEqual(second["version"], 2)
        other, _ = publish(self.path, _signal("us-west", observed=0), "k3")
        self.assertEqual(other["version"], 1)
        data = json.loads(Path(self.path).read_text(encoding="utf-8"))
        self.assertEqual(list(data["history"].keys()),
                         ["eu-north", "us-west"])
        self.assertEqual([r["version"]
                          for r in data["history"]["eu-north"]], [1, 2])
        self.assertEqual(data["audit"]["k2"],
                         {"key": "k2", "region": "eu-north", "version": 2})

    def test_observation_must_strictly_increase(self) -> None:
        publish(self.path, _signal(observed=10), "k1")
        with self.assertRaises(ValueError):
            publish(self.path, _signal(observed=10), "k2")
        with self.assertRaises(ValueError):
            publish(self.path, _signal(observed=5), "k3")
        record, created = publish(self.path, _signal(observed=11), "k4")
        self.assertTrue(created)
        self.assertEqual(record["version"], 2)

    def test_idempotent_replay_returns_original_without_write(self) -> None:
        first, created_first = publish(self.path, _signal(), "key")
        before = Path(self.path).read_bytes()
        second, created_second = publish(self.path, _signal(), "key")
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first, second)
        self.assertEqual(Path(self.path).read_bytes(), before)

    def test_same_key_different_signal_raises(self) -> None:
        publish(self.path, _signal(), "key")
        for changed in (_signal(region="us-west"),
                        _signal(observed=11),
                        _signal(expires=99),
                        _signal(unit_cost=4),
                        _signal(carbon_intensity=8),
                        _signal(mix={"solar": 5999, "wind": 4001})):
            with self.subTest(changed=changed):
                with self.assertRaises(ValueError):
                    publish(self.path, changed, "key")

    def test_distinct_regions_and_keys_persist_sorted(self) -> None:
        publish(self.path, _signal("r3"), "k3")
        publish(self.path, _signal("r1"), "k1")
        publish(self.path, _signal("r2"), "k2")
        data = json.loads(Path(self.path).read_text(encoding="utf-8"))
        self.assertEqual(list(data["history"].keys()), ["r1", "r2", "r3"])
        self.assertEqual(list(data["idempotency"].keys()),
                         ["k1", "k2", "k3"])
        self.assertEqual(list(data["audit"].keys()), ["k1", "k2", "k3"])

    def test_invalid_arguments(self) -> None:
        cases = [
            ("empty region", _signal(region="")),
            ("non-string region", _signal(region=1)),
            ("negative observed", _signal(observed=-1)),
            ("bool observed", _signal(observed=True)),
            ("float observed", _signal(observed=1.5)),
            ("negative expires", _signal(expires=-1)),
            ("expires before observed", _signal(observed=10, expires=9)),
            ("negative unit_cost", _signal(unit_cost=-1)),
            ("bool unit_cost", _signal(unit_cost=False)),
            ("bool carbon", _signal(carbon_intensity=True)),
            ("empty mix", _signal(mix={})),
            ("mix not a dict", _signal(mix=[("solar", 10000)])),
            ("empty mix source", _signal(mix={"": 10000})),
            ("negative share", _signal(mix={"solar": -1, "wind": 10001})),
            ("bool share", _signal(mix={"solar": True, "wind": 9999})),
            ("float share", _signal(mix={"solar": 0.5, "wind": 9999.5})),
            ("mix wrong sum", _signal(mix={"solar": 9999})),
            ("extra field", _signal(state="x")),
            ("missing field", {"region": "r", "observed": 0, "expires": 1,
                               "mix": {"solar": 10000}, "unit_cost": 0}),
            ("not a dict", ["r", 0, 1, {"solar": 10000}, 0, 0]),
        ]
        for label, bad_signal in cases:
            with self.subTest(label):
                with self.assertRaises(ValueError):
                    publish(self.path, bad_signal, "key")  # type: ignore[arg-type]

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

    def test_invalid_structures_raise_value_error(self) -> None:
        record = {"region": "r", "version": 1, "observed": 0, "expires": 1,
                  "mix": {"solar": 10000}, "unit_cost": 0,
                  "carbon_intensity": 0}
        event = {"key": "k", "region": "r", "version": 1}
        bad_payloads = [
            [],
            {},
            {"version": 1, "history": {}, "idempotency": {}, "audit": {},
             "extra": 1},
            {"version": 2, "history": {}, "idempotency": {}, "audit": {}},
            {"version": 1, "history": [], "idempotency": {}, "audit": {}},
            {"version": 1, "history": {"r": []}, "idempotency": {},
             "audit": {}},
            {"version": 1, "history": {"r": [dict(record, version=2)]},
             "idempotency": {"k": "r"}, "audit": {"k": event}},
            {"version": 1,
             "history": {"r": [record, dict(record, version=2,
                                             observed=0)]},
             "idempotency": {"k": "r"}, "audit": {"k": event}},
            {"version": 1, "history": {"r": [record]},
             "idempotency": {"k": "missing"}, "audit": {"k": event}},
            {"version": 1, "history": {"r": [record]},
             "idempotency": {"k": "r"}, "audit": {}},
            {"version": 1, "history": {"r": [record]},
             "idempotency": {"k": "r"},
             "audit": {"k": dict(event, version=2)}},
            {"version": 1, "history": {"r": [record]},
             "idempotency": {"k": "r"},
             "audit": {"k": dict(event, region="other")}},
        ]
        for payload in bad_payloads:
            with self.subTest(payload=payload):
                Path(self.path).write_text(json.dumps(payload),
                                           encoding="utf-8")
                with self.assertRaises(ValueError):
                    publish(self.path, _signal("other", observed=5), "key2")

    def test_concurrent_first_publications_serialize(self) -> None:
        errors: list[BaseException] = []

        def worker(index: int) -> None:
            try:
                publish(self.path,
                        _signal(f"r{index:03d}", observed=index),
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

    def test_no_tmp_files_left_behind(self) -> None:
        publish(self.path, _signal(), "k")
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])


class GetTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "signals.json")
        publish(self.path, _signal("eu-north", observed=10, expires=20,
                                   unit_cost=3), "k1")
        publish(self.path, _signal("eu-north", observed=30, expires=40,
                                   unit_cost=5), "k2")
        publish(self.path, _signal("us-west", observed=0, expires=100), "k3")

    def test_latest_valid_version(self) -> None:
        self.assertEqual(get(self.path, "eu-north", 15)["version"], 1)
        self.assertEqual(get(self.path, "eu-north", 30)["unit_cost"], 5)
        self.assertEqual(get(self.path, "eu-north", 40)["version"], 2)
        self.assertEqual(get(self.path, "us-west", 50)["version"], 1)

    def test_boundary_windows(self) -> None:
        self.assertEqual(get(self.path, "eu-north", 10)["version"], 1)
        self.assertEqual(get(self.path, "eu-north", 20)["version"], 1)
        self.assertEqual(get(self.path, "eu-north", 30)["version"], 2)

    def test_unknown_region_raises_key_error(self) -> None:
        with self.assertRaises(KeyError):
            get(self.path, "ap-south", 10)

    def test_no_valid_version_raises_lookup_error(self) -> None:
        # Between two windows and after the last expiration.
        with self.assertRaises(LookupError):
            get(self.path, "eu-north", 25)
        with self.assertRaises(LookupError):
            get(self.path, "eu-north", 41)
        # Before the first observation.
        with self.assertRaises(LookupError):
            get(self.path, "eu-north", 5)

    def test_missing_file_raises_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            get(os.path.join(self.tmp.name, "missing.json"), "eu-north", 10)

    def test_invalid_arguments(self) -> None:
        with self.assertRaises(ValueError):
            get("", "eu-north", 10)
        with self.assertRaises(ValueError):
            get(self.path, "", 10)
        with self.assertRaises(ValueError):
            get(self.path, "eu-north", -1)
        with self.assertRaises(ValueError):
            get(self.path, "eu-north", True)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            get(self.path, "eu-north", 1.5)  # type: ignore[arg-type]

    def test_invalid_file_raises_value_error(self) -> None:
        Path(self.path).write_text("{not json", encoding="utf-8")
        with self.assertRaises(ValueError):
            get(self.path, "eu-north", 10)

    def test_returned_record_is_a_copy(self) -> None:
        record = get(self.path, "us-west", 10)
        record["unit_cost"] = 999
        record["mix"]["solar"] = 1  # type: ignore[index]
        again = get(self.path, "us-west", 10)
        self.assertEqual(again["unit_cost"], 3)
        self.assertEqual(again["mix"], {"solar": 6000, "wind": 4000})

    def test_non_canonical_bytes_rejected(self) -> None:
        raw = json.loads(Path(self.path).read_text(encoding="utf-8"))
        reordered = json.dumps(raw, indent=2)
        Path(self.path).write_text(reordered, encoding="utf-8")
        with self.assertRaises(ValueError):
            get(self.path, "eu-north", 10)


if __name__ == "__main__":
    unittest.main()
