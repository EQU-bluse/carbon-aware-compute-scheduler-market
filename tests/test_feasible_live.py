from __future__ import annotations

import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import jobs as jobs_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.resources import feasible_live, publish
from carbon_market.signals import publish as publish_signal


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


class FeasibleLiveTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs_path = os.path.join(self.tmp.name, "jobs.json")
        self.supply_path = os.path.join(self.tmp.name, "supply.json")
        self.signal_path = os.path.join(self.tmp.name, "signals.json")
        jobs_module.submit(self.jobs_path, _job(), "jk-1")

    def test_signal_figures_drive_totals_and_order(self) -> None:
        publish(self.supply_path,
                _resource("r-1", carbon_intensity=99, unit_cost=99), "k1")
        publish(self.supply_path,
                _resource("r-2", carbon_intensity=99, unit_cost=99,
                          region="us-west", residency=["eu-north", "us-west"]),
                "k2")
        publish_signal(self.signal_path,
                       _signal("eu-north", carbon_intensity=5, unit_cost=4),
                       "s1")
        publish_signal(self.signal_path,
                       _signal("us-west", carbon_intensity=2, unit_cost=9),
                       "s2")
        result = feasible_live(self.jobs_path, self.supply_path,
                               self.signal_path, "j-1", 50)
        # Ordered by live carbon, not the static 99 the versions carry.
        self.assertEqual([entry["resource"]["resource_id"]
                          for entry in result], ["r-2", "r-1"])
        first = result[0]
        self.assertEqual(list(first.keys()),
                         ["resource", "signal", "total_cost",
                          "total_carbon"])
        self.assertEqual(first["total_cost"], 10 * 9)
        self.assertEqual(first["total_carbon"], 10 * 2)
        self.assertEqual(first["signal"]["region"], "us-west")
        self.assertEqual(first["signal"]["version"], 1)

    def test_latest_unexpired_signal_wins(self) -> None:
        publish(self.supply_path, _resource("r-1"), "k1")
        publish_signal(self.signal_path,
                       _signal(observed=0, expires=40, unit_cost=1,
                               carbon_intensity=9), "s1")
        publish_signal(self.signal_path,
                       _signal(observed=50, expires=200, unit_cost=1,
                               carbon_intensity=1), "s2")
        self.assertEqual(
            feasible_live(self.jobs_path, self.supply_path,
                          self.signal_path, "j-1", 40)[0]["signal"]["version"],
            1)
        self.assertEqual(
            feasible_live(self.jobs_path, self.supply_path,
                          self.signal_path, "j-1", 50)[0]["signal"]["version"],
            2)

    def test_missing_or_expired_signal_excludes_resource(self) -> None:
        publish(self.supply_path,
                _resource("r-1", region="eu-north"), "k1")
        publish(self.supply_path,
                _resource("r-2", region="us-west",
                          residency=["eu-north", "us-west"]), "k2")
        # An existing but empty signal ledger: no candidates, no error.
        Path(self.signal_path).write_text(
            '{"version":1,"history":{},"idempotency":{},"audit":{}}\n',
            encoding="utf-8")
        self.assertEqual(feasible_live(self.jobs_path, self.supply_path,
                                       self.signal_path, "j-1", 50), [])
        publish_signal(self.signal_path,
                       _signal("eu-north", observed=0, expires=49), "s1")
        result = feasible_live(self.jobs_path, self.supply_path,
                               self.signal_path, "j-1", 50)
        # eu signal expired at 49; us has no signal: empty again.
        self.assertEqual(result, [])
        publish_signal(self.signal_path,
                       _signal("us-west", observed=0, expires=49), "s2")
        self.assertEqual(feasible_live(self.jobs_path, self.supply_path,
                                       self.signal_path, "j-1", 50), [])

    def test_static_constraints_still_apply(self) -> None:
        # Region, capacity, residency and deadline come from the supply
        # snapshot and keep excluding candidates even with a live signal.
        publish(self.supply_path,
                _resource("ok", region="eu-north"), "k1")
        publish(self.supply_path,
                _resource("small", region="eu-north", capacity=9), "k2")
        publish(self.supply_path,
                _resource("short", region="eu-north", end=99), "k3")
        publish_signal(self.signal_path,
                       _signal("eu-north", carbon_intensity=0, unit_cost=0),
                       "s1")
        result = feasible_live(self.jobs_path, self.supply_path,
                               self.signal_path, "j-1", 50)
        self.assertEqual([entry["resource"]["resource_id"]
                          for entry in result], ["ok"])

    def test_live_budgets_exclude_at_boundary(self) -> None:
        publish(self.supply_path,
                _resource("r", unit_cost=0, carbon_intensity=0), "k1")
        publish_signal(self.signal_path,
                       _signal(unit_cost=100, carbon_intensity=100), "s1")
        # work 10, budgets 1000: exactly at the budget stays.
        self.assertEqual(len(feasible_live(
            self.jobs_path, self.supply_path, self.signal_path, "j-1", 50)),
            1)
        publish_signal(self.signal_path,
                       _signal(unit_cost=101, carbon_intensity=101,
                               observed=1), "s2")
        self.assertEqual(feasible_live(
            self.jobs_path, self.supply_path, self.signal_path, "j-1", 50),
            [])

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

    def test_invalid_files_raise_value_error(self) -> None:
        publish(self.supply_path, _resource(), "k1")
        publish_signal(self.signal_path, _signal(), "s1")
        Path(self.signal_path).write_text("{not json", encoding="utf-8")
        with self.assertRaises(ValueError):
            feasible_live(self.jobs_path, self.supply_path,
                          self.signal_path, "j-1", 50)

    def test_invalid_arguments(self) -> None:
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

    def test_distinct_real_paths_required(self) -> None:
        with self.assertRaises(ValueError):
            feasible_live(self.jobs_path, self.jobs_path, self.signal_path,
                          "j-1", 50)


if __name__ == "__main__":
    unittest.main()
