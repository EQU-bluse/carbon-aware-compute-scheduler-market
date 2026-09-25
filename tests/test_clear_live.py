from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import jobs as jobs_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.market import clear, clear_live
from carbon_market.resources import publish as publish_resource
from carbon_market.signals import publish as publish_signal


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
        "observed": 0,
        "expires": 500,
        "mix": {"solar": 10000},
        "unit_cost": 3,
        "carbon_intensity": 7,
    }
    signal.update(overrides)
    return signal


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


class ClearLiveTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs = os.path.join(self.tmp.name, "jobs.json")
        self.supply = os.path.join(self.tmp.name, "supply.json")
        self.signals = os.path.join(self.tmp.name, "signals.json")
        self.ledger = os.path.join(self.tmp.name, "ledger.json")
        jobs_module.submit(self.jobs, _job(), "jk-1")

    def _seed_two_regions(self) -> None:
        publish_resource(self.supply,
                         _resource("r-1", unit_cost=99, carbon_intensity=99),
                         "k1")
        publish_resource(self.supply,
                         _resource("r-2", region="us-west", unit_cost=99,
                                   carbon_intensity=99,
                                   residency=["eu-north", "us-west"]), "k2")
        publish_signal(self.signals,
                       _signal("eu-north", unit_cost=4, carbon_intensity=5),
                       "s1")
        publish_signal(self.signals,
                       _signal("us-west", unit_cost=9, carbon_intensity=2),
                       "s2")

    def test_clears_on_live_signal_and_freezes_candidates(self) -> None:
        self._seed_two_regions()
        trade, created = clear_live(self.jobs, self.supply, self.signals,
                                    self.ledger, "j-1", "t1", 50)
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-2")
        self.assertEqual(trade["version"], 1)
        self.assertEqual(trade["selection"],
                         {"resource_id": "r-2", "version": 1})
        self.assertEqual([c["resource"]["resource_id"]
                          for c in trade["candidates"]], ["r-2", "r-1"])
        for candidate in trade["candidates"]:
            self.assertEqual(list(candidate.keys()),
                             ["resource", "signal", "total_cost",
                              "total_carbon"])
            self.assertEqual(set(candidate["signal"].keys()),
                             {"region", "version", "observed", "expires",
                              "mix", "unit_cost", "carbon_intensity"})
        winner = trade["candidates"][0]
        self.assertEqual(winner["total_cost"], 10 * 9)
        self.assertEqual(winner["total_carbon"], 10 * 2)
        self.assertEqual(winner["signal"]["version"], 1)

        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(data["version"], 2)
        self.assertEqual(list(data.keys()),
                         ["version", "trades", "idempotency", "audit"])
        self.assertEqual(data["idempotency"],
                         {"t1": {"job_id": "j-1", "at": 50}})
        self.assertEqual(data["audit"]["t1"], {
            "key": "t1", "job_id": "j-1", "at": 50,
            "resource_id": "r-2", "version": 1})
        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))

    def test_replay_returns_stored_trade_without_write(self) -> None:
        self._seed_two_regions()
        first, created_first = clear_live(self.jobs, self.supply,
                                          self.signals, self.ledger,
                                          "j-1", "t1", 50)
        before = Path(self.ledger).read_bytes()
        second, created_second = clear_live(self.jobs, self.supply,
                                            self.signals, self.ledger,
                                            "j-1", "t1", 50)
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(second, first)
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_later_signal_publish_does_not_change_trade(self) -> None:
        self._seed_two_regions()
        first, _ = clear_live(self.jobs, self.supply, self.signals,
                              self.ledger, "j-1", "t1", 50)
        publish_signal(self.signals,
                       _signal("us-west", observed=60, expires=900,
                               unit_cost=0, carbon_intensity=0), "s3")
        replayed, created = clear_live(self.jobs, self.supply, self.signals,
                                       self.ledger, "j-1", "t1", 50)
        self.assertFalse(created)
        self.assertEqual(replayed, first)

    def test_same_key_changed_request_raises(self) -> None:
        self._seed_two_regions()
        clear_live(self.jobs, self.supply, self.signals, self.ledger,
                   "j-1", "t1", 50)
        before = Path(self.ledger).read_bytes()
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.supply, self.signals, self.ledger,
                       "j-1", "t1", 51)
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_job_already_traded_under_another_key_raises(self) -> None:
        self._seed_two_regions()
        clear_live(self.jobs, self.supply, self.signals, self.ledger,
                   "j-1", "t1", 50)
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.supply, self.signals, self.ledger,
                       "j-1", "t2", 50)

    def test_unknown_job_raises_key_error(self) -> None:
        self._seed_two_regions()
        with self.assertRaises(KeyError):
            clear_live(self.jobs, self.supply, self.signals, self.ledger,
                       "j-9", "t1", 50)
        self.assertFalse(os.path.exists(self.ledger))

    def test_no_candidate_raises_lookup_error(self) -> None:
        publish_resource(self.supply, _resource("r-1"), "k1")
        # Signal exists only for an unrelated region.
        publish_signal(self.signals, _signal("us-west"), "s1")
        with self.assertRaises(LookupError):
            clear_live(self.jobs, self.supply, self.signals, self.ledger,
                       "j-1", "t1", 50)
        self.assertFalse(os.path.exists(self.ledger))

    def test_no_signal_file_candidate_raises_lookup_error(self) -> None:
        publish_resource(self.supply, _resource("r-1"), "k1")
        with self.assertRaises(FileNotFoundError):
            clear_live(self.jobs, self.supply, self.signals, self.ledger,
                       "j-1", "t1", 50)

    def test_capacity_deduction_shared_with_static_clear(self) -> None:
        jobs_module.submit(self.jobs, _job(
            "j-2", work=95, max_cost=100000, carbon_cap=100000), "jk-2")
        publish_resource(self.supply, _resource("r-1", capacity=100), "k1")
        publish_signal(self.signals,
                       _signal("eu-north", unit_cost=1, carbon_intensity=1),
                       "s1")
        # A static trade books 95 of r-1's 100 capacity.
        static_trade, static_created = clear(
            self.jobs, self.supply, self.ledger, "j-2", "ts", 50)
        self.assertTrue(static_created)
        self.assertNotIn("signal", static_trade["candidates"][0])
        # The live job needs 10: r-1 has only 5 left, so it cannot win.
        with self.assertRaises(LookupError):
            clear_live(self.jobs, self.supply, self.signals, self.ledger,
                       "j-1", "t1", 50)

    def test_live_then_static_capacity_deduction(self) -> None:
        jobs_module.submit(self.jobs, _job(
            "j-2", work=95, max_cost=100000, carbon_cap=100000), "jk-2")
        publish_resource(self.supply, _resource("r-1", capacity=100), "k1")
        publish_signal(self.signals,
                       _signal("eu-north", unit_cost=1, carbon_intensity=1),
                       "s1")
        live_trade, live_created = clear_live(
            self.jobs, self.supply, self.signals, self.ledger,
            "j-1", "t1", 50)
        self.assertTrue(live_created)
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.ledger, "j-2", "ts", 50)

    def test_mixed_ledger_is_canonical_and_readable(self) -> None:
        jobs_module.submit(self.jobs, _job(
            "j-2", work=1, max_cost=100000, carbon_cap=100000), "jk-2")
        publish_resource(self.supply, _resource("r-1", capacity=100), "k1")
        publish_signal(self.signals,
                       _signal("eu-north", unit_cost=1, carbon_intensity=1),
                       "s1")
        clear(self.jobs, self.supply, self.ledger, "j-2", "ts", 50)
        clear_live(self.jobs, self.supply, self.signals, self.ledger,
                   "j-1", "t1", 50)
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(set(data["trades"]), {"j-1", "j-2"})
        self.assertIn("signal", data["trades"]["j-1"]["candidates"][0])
        self.assertNotIn("signal", data["trades"]["j-2"]["candidates"][0])

    def test_tampered_frozen_signal_raises_value_error(self) -> None:
        self._seed_two_regions()
        clear_live(self.jobs, self.supply, self.signals, self.ledger,
                   "j-1", "t1", 50)
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        data["trades"]["j-1"]["candidates"][0]["signal"]["unit_cost"] = 1
        Path(self.ledger).write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"))
            + "\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.supply, self.signals, self.ledger,
                       "j-2", "t2", 50)

    def test_invalid_arguments(self) -> None:
        for value in ("", None, 123):
            with self.assertRaises(ValueError):
                clear_live(value if isinstance(value, str) else "",
                           self.supply, self.signals, self.ledger,
                           "j-1", "t1", 50)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.supply, self.signals, self.ledger,
                       "", "t1", 50)
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.supply, self.signals, self.ledger,
                       "j-1", "", 50)
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.supply, self.signals, self.ledger,
                       "j-1", "t1", -1)
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.supply, self.signals, self.ledger,
                       "j-1", "t1", True)  # type: ignore[arg-type]

    def test_paths_must_be_distinct(self) -> None:
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.jobs, self.signals, self.ledger,
                       "j-1", "t1", 50)
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.supply, self.supply, self.ledger,
                       "j-1", "t1", 50)
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.supply, self.signals, self.supply,
                       "j-1", "t1", 50)

    def test_missing_inputs_raise_file_not_found(self) -> None:
        publish_resource(self.supply, _resource(), "k1")
        publish_signal(self.signals, _signal(), "s1")
        with self.assertRaises(FileNotFoundError):
            clear_live(os.path.join(self.tmp.name, "no-jobs.json"),
                       self.supply, self.signals, self.ledger, "j-1",
                       "t1", 50)
        with self.assertRaises(FileNotFoundError):
            clear_live(self.jobs, os.path.join(self.tmp.name, "no-supply.json"),
                       self.signals, self.ledger, "j-1", "t1", 50)
        with self.assertRaises(FileNotFoundError):
            clear_live(self.jobs, self.supply,
                       os.path.join(self.tmp.name, "no-signals.json"),
                       self.ledger, "j-1", "t1", 50)

    def test_missing_ledger_parent_raises_file_not_found(self) -> None:
        self._seed_two_regions()
        missing = os.path.join(self.tmp.name, "no-such-dir", "ledger.json")
        with self.assertRaises(FileNotFoundError):
            clear_live(self.jobs, self.supply, self.signals, missing,
                       "j-1", "t1", 50)
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])


if __name__ == "__main__":
    unittest.main()
