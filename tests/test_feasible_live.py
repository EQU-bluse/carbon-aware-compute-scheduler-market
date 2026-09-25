from __future__ import annotations

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import jobs as jobs_module
from carbon_market import signals as signals_module
from carbon_market.resources import feasible_live, publish
from carbon_market.signals import publish as publish_signal


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


def _signal(region: str = "eu-north", **overrides: object) -> dict[str, object]:
    signal: dict[str, object] = {
        "region": region,
        "observed_at": 0,
        "expires_at": 500,
        "energy_mix": {"a": 10000},
        "unit_cost": 3,
        "carbon_intensity": 7,
    }
    signal.update(overrides)
    return signal


class FeasibleLiveTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs_path = os.path.join(self.tmp.name, "jobs.json")
        self.supply_path = os.path.join(self.tmp.name, "supply.json")
        self.signal_path = os.path.join(self.tmp.name, "signals.json")
        jobs_module.submit(self.jobs_path, _job(), "jk-1")

    def test_prices_and_ranks_come_from_the_signal(self) -> None:
        # Static prices would rank r-1 (carbon 7) over r-2 (carbon 2 is
        # the greener one); the live regional signals rank them instead.
        publish(self.supply_path,
                _resource("r-1", region="eu-north",
                          residency=["eu-north"],
                          unit_cost=3, carbon_intensity=7), "k1")
        publish(self.supply_path,
                _resource("r-2", region="us-west",
                          residency=["eu-north", "us-west"],
                          unit_cost=9, carbon_intensity=2), "k2")
        publish_signal(self.signal_path,
                       _signal("eu-north", unit_cost=5, carbon_intensity=4),
                       "s1")
        publish_signal(self.signal_path,
                       _signal("us-west", unit_cost=5, carbon_intensity=2),
                       "s2")
        result = feasible_live(self.jobs_path, self.supply_path,
                               self.signal_path, "j-1", 50)
        self.assertEqual([entry["resource"]["resource_id"]
                          for entry in result], ["r-2", "r-1"])
        first = result[0]
        self.assertEqual(list(first.keys()),
                         ["resource", "signal", "total_cost",
                          "total_carbon"])
        self.assertEqual(first["total_cost"], 10 * 5)
        self.assertEqual(first["total_carbon"], 10 * 2)
        self.assertEqual(first["signal"]["version"], 1)
        self.assertEqual(first["signal"]["region"], "us-west")

    def test_uses_latest_unexpired_signal(self) -> None:
        publish(self.supply_path, _resource("r-1"), "k1")
        publish_signal(self.signal_path,
                       _signal(observed_at=0, expires_at=100,
                               unit_cost=9, carbon_intensity=9), "s1")
        publish_signal(self.signal_path,
                       _signal(observed_at=60, expires_at=200,
                               unit_cost=1, carbon_intensity=1), "s2")
        early = feasible_live(self.jobs_path, self.supply_path,
                              self.signal_path, "j-1", 50)
        self.assertEqual(early[0]["signal"]["version"], 1)
        self.assertEqual(early[0]["total_cost"], 90)
        late = feasible_live(self.jobs_path, self.supply_path,
                             self.signal_path, "j-1", 60)
        self.assertEqual(late[0]["signal"]["version"], 2)
        self.assertEqual(late[0]["total_cost"], 10)

    def test_resource_without_valid_signal_is_excluded(self) -> None:
        publish(self.supply_path, _resource("r-1"), "k1")
        publish(self.supply_path,
                _resource("r-2", region="us-west", residency=["us-west"]),
                "k2")
        # Only eu-north carries a live signal; the us-west resource is
        # structurally fine but silently excluded.
        publish_signal(self.signal_path, _signal("eu-north"), "s1")
        result = feasible_live(self.jobs_path, self.supply_path,
                               self.signal_path, "j-1", 50)
        self.assertEqual([entry["resource"]["resource_id"]
                          for entry in result], ["r-1"])

    def test_expired_signal_excludes_resource(self) -> None:
        publish(self.supply_path, _resource("r-1"), "k1")
        publish_signal(self.signal_path,
                       _signal(observed_at=0, expires_at=40), "s1")
        self.assertEqual(feasible_live(self.jobs_path, self.supply_path,
                                       self.signal_path, "j-1", 50), [])

    def test_signal_budget_exclusion(self) -> None:
        publish(self.supply_path, _resource("r-1"), "k1")
        publish_signal(self.signal_path, _signal(unit_cost=101), "s-cost")
        self.assertEqual(feasible_live(self.jobs_path, self.supply_path,
                                       self.signal_path, "j-1", 50), [])
        publish_signal(self.signal_path,
                       _signal(observed_at=1, carbon_intensity=101), "s-co2")
        self.assertEqual(feasible_live(self.jobs_path, self.supply_path,
                                       self.signal_path, "j-1", 50), [])

    def test_region_specific_signals(self) -> None:
        publish(self.supply_path, _resource("r-north", region="eu-north",
                                            residency=["eu-north"]), "k1")
        publish(self.supply_path,
                _resource("r-west", region="us-west",
                          residency=["eu-north", "us-west"]), "k2")
        publish_signal(self.signal_path,
                       _signal("eu-north", unit_cost=1, carbon_intensity=9),
                       "s1")
        publish_signal(self.signal_path,
                       _signal("us-west", unit_cost=1, carbon_intensity=1),
                       "s2")
        result = feasible_live(self.jobs_path, self.supply_path,
                               self.signal_path, "j-1", 50)
        self.assertEqual([entry["resource"]["resource_id"]
                          for entry in result], ["r-west", "r-north"])
        self.assertEqual(result[0]["signal"]["region"], "us-west")
        self.assertEqual(result[1]["signal"]["region"], "eu-north")

    def test_empty_when_no_candidate(self) -> None:
        publish(self.supply_path, _resource(capacity=1), "k1")
        publish_signal(self.signal_path, _signal(), "s1")
        self.assertEqual(feasible_live(self.jobs_path, self.supply_path,
                                       self.signal_path, "j-1", 50), [])

    def test_unknown_job_raises_key_error(self) -> None:
        publish(self.supply_path, _resource(), "k1")
        publish_signal(self.signal_path, _signal(), "s1")
        with self.assertRaises(KeyError):
            feasible_live(self.jobs_path, self.supply_path,
                          self.signal_path, "j-9", 50)

    def test_missing_files_raise_file_not_found(self) -> None:
        publish(self.supply_path, _resource(), "k1")
        publish_signal(self.signal_path, _signal(), "s1")
        with self.assertRaises(FileNotFoundError):
            feasible_live(os.path.join(self.tmp.name, "no-jobs.json"),
                          self.supply_path, self.signal_path, "j-1", 50)
        with self.assertRaises(FileNotFoundError):
            feasible_live(self.jobs_path,
                          os.path.join(self.tmp.name, "no-supply.json"),
                          self.signal_path, "j-1", 50)
        with self.assertRaises(FileNotFoundError):
            feasible_live(self.jobs_path, self.supply_path,
                          os.path.join(self.tmp.name, "no-signals.json"),
                          "j-1", 50)

    def test_invalid_signal_file_raises_value_error(self) -> None:
        publish(self.supply_path, _resource(), "k1")
        Path(self.signal_path).write_text("{not json", encoding="utf-8")
        with self.assertRaises(ValueError):
            feasible_live(self.jobs_path, self.supply_path,
                          self.signal_path, "j-1", 50)

    def test_invalid_arguments(self) -> None:
        publish(self.supply_path, _resource(), "k1")
        publish_signal(self.signal_path, _signal(), "s1")
        with self.assertRaises(ValueError):
            feasible_live("", self.supply_path, self.signal_path, "j-1", 50)
        with self.assertRaises(ValueError):
            feasible_live(self.jobs_path, "", self.signal_path, "j-1", 50)
        with self.assertRaises(ValueError):
            feasible_live(self.jobs_path, self.supply_path, "", "j-1", 50)
        with self.assertRaises(ValueError):
            feasible_live(self.jobs_path, self.supply_path,
                          self.signal_path, "", 50)
        with self.assertRaises(ValueError):
            feasible_live(self.jobs_path, self.supply_path,
                          self.signal_path, "j-1", -1)
        with self.assertRaises(ValueError):
            feasible_live(self.jobs_path, self.supply_path,
                          self.signal_path, "j-1", True)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
