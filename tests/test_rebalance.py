from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market.dispatch import claim, commit, finish
from carbon_market.dispatch import recover as dispatch_recover
from carbon_market.execution import plan, record, recover
from carbon_market.jobs import submit
from carbon_market.market import clear, clear_live
from carbon_market.rebalance import evaluate
from carbon_market.resources import publish as publish_resource
from carbon_market.signals import publish as publish_signal

_EMPTY_EXECUTION = (
    '{"version":1,"plans":{},"idempotency":{},"audit":{}}\n')


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


def _resource(resource_id: str = "r-1", region: str = "eu-north",
              **overrides: object) -> dict[str, object]:
    resource: dict[str, object] = {
        "resource_id": resource_id,
        "region": region,
        "capacity": 100,
        "start": 0,
        "end": 500,
        "unit_cost": 99,
        "carbon_intensity": 99,
        "residency": (["eu-north"] if region == "eu-north"
                      else ["eu-north", "us-west"]),
    }
    resource.update(overrides)
    return resource


def _signal(region: str = "eu-north", **overrides: object) -> dict[str, object]:
    signal: dict[str, object] = {
        "region": region,
        "observed": 0,
        "expires": 500,
        "mix": {"solar": 10000},
        "unit_cost": 5,
        "carbon_intensity": 7,
    }
    signal.update(overrides)
    return signal


class RebalanceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs = os.path.join(self.tmp.name, "jobs.json")
        self.supply = os.path.join(self.tmp.name, "supply.json")
        self.signals = os.path.join(self.tmp.name, "signals.json")
        self.trades = os.path.join(self.tmp.name, "trades.json")
        self.dispatch = os.path.join(self.tmp.name, "dispatch.json")
        self.execution = os.path.join(self.tmp.name, "execution.json")
        self.advice = os.path.join(self.tmp.name, "advice.json")
        submit(self.jobs, _job(), "jk-1")
        publish_resource(self.supply, _resource("r-1"), "rk-1")
        publish_resource(self.supply,
                         _resource("r-2", region="us-west"), "rk-2")
        publish_signal(self.signals,
                       _signal("eu-north", unit_cost=5, carbon_intensity=7),
                       "sk-1")
        publish_signal(self.signals,
                       _signal("us-west", unit_cost=9, carbon_intensity=2),
                       "sk-2")
        Path(self.execution).write_text(_EMPTY_EXECUTION, encoding="utf-8")

    def _live_trade(self, job_id: str = "j-1", key: str = "tk-1",
                    at: int = 40) -> dict[str, object]:
        trade, _ = clear_live(self.jobs, self.supply, self.signals,
                              self.trades, job_id, key, at)
        return trade

    def _commit(self, job_id: str = "j-1", key: str = "ck-1",
                at: int = 45) -> None:
        commit(self.jobs, self.supply, self.trades, self.dispatch,
               job_id, key, at)

    def _greener_eu_signal(self, key: str = "sk-3", **overrides: object,
                           ) -> None:
        params: dict[str, object] = {"observed": 50, "unit_cost": 1,
                                    "carbon_intensity": 1}
        params.update(overrides)
        publish_signal(self.signals, _signal("eu-north", **params), key)

    def _evaluate(self, job_id: str = "j-1", key: str = "ak-1",
                  at: int = 60, ledger: str | None = None
                  ) -> tuple[dict[str, object], bool]:
        return evaluate(self.jobs, self.supply, self.signals, self.trades,
                        self.dispatch, self.execution,
                        ledger or self.advice, job_id, key, at)

    # -- advice --------------------------------------------------------

    def test_migrate_keeps_resource_version_but_prices_on_latest_signal(
            self) -> None:
        trade = self._live_trade()
        self._commit()
        # A newer supply version for the current resource must not replace
        # the trade-frozen version in the retained candidate...
        publish_resource(self.supply,
                         _resource("r-2", region="us-west", start=30),
                         "rk-3")
        # ...but a newer signal for its region is adopted: every item
        # prices on the region's latest unexpired signal.
        publish_signal(self.signals,
                       _signal("us-west", observed=55, unit_cost=20,
                               carbon_intensity=8),
                       "sk-4")
        self._greener_eu_signal()

        record, created = self._evaluate()
        self.assertTrue(created)
        self.assertEqual(list(record.keys()),
                         ["job_id", "at", "current", "dispatch",
                          "execution", "candidates", "advice", "target"])
        self.assertEqual(record["job_id"], "j-1")
        self.assertEqual(record["at"], 60)
        self.assertEqual(record["current"],
                         {"resource_id": "r-2", "version": 1})
        self.assertEqual(record["dispatch"], "ready")
        self.assertEqual(record["execution"], "none")
        self.assertEqual(record["advice"], "migrate")
        self.assertEqual(record["target"],
                         {"resource_id": "r-1", "version": 1})

        by_id = {c["resource"]["resource_id"]: c
                 for c in record["candidates"]}
        self.assertEqual(list(by_id), ["r-1", "r-2"])
        for candidate in record["candidates"]:
            self.assertEqual(list(candidate.keys()),
                             ["resource", "signal", "total_cost",
                              "total_carbon"])
        # The migration item is r-1's highest valid version priced on the
        # region's latest signal.
        self.assertEqual(by_id["r-1"]["resource"]["version"], 1)
        self.assertEqual(by_id["r-1"]["signal"]["region"], "eu-north")
        self.assertEqual(by_id["r-1"]["signal"]["version"], 2)
        self.assertEqual(by_id["r-1"]["total_cost"], 10)
        self.assertEqual(by_id["r-1"]["total_carbon"], 10)
        # The retained item keeps the trade's exact resource version even
        # though a newer supply version exists, but prices on the latest
        # unexpired us-west signal (version 2).
        self.assertEqual(by_id["r-2"]["resource"]["version"], 1)
        self.assertEqual(by_id["r-2"]["signal"]["version"], 2)
        self.assertEqual(by_id["r-2"]["signal"]["carbon_intensity"], 8)
        self.assertEqual(by_id["r-2"]["total_cost"], 200)
        # The trade itself is untouched.
        self.assertEqual(trade["resource_id"], "r-2")

    def test_keep_when_current_version_still_ranks_first(self) -> None:
        self._live_trade()
        self._commit()
        record, created = self._evaluate(at=45)
        self.assertTrue(created)
        self.assertEqual(record["advice"], "keep")
        self.assertEqual(record["target"], record["current"])
        self.assertEqual([c["resource"]["resource_id"]
                          for c in record["candidates"]][0], "r-2")

    def test_ordering_by_signal_carbon_cost_and_resource_id(self) -> None:
        self._live_trade()
        self._commit()
        # The retained r-2 keeps its frozen figures (carbon 2, cost 9);
        # a new eu-north signal tying on both leaves the order to the
        # resource id.
        self._greener_eu_signal("sk-3", unit_cost=9, carbon_intensity=2)
        record, _ = self._evaluate(at=60)
        self.assertEqual([c["resource"]["resource_id"]
                          for c in record["candidates"]], ["r-1", "r-2"])

    def test_migrations_use_highest_valid_resource_version(self) -> None:
        self._live_trade()
        self._commit()
        # A second r-1 version is the only one valid past its earlier
        # start window; migration items must pick the highest valid one.
        publish_resource(self.supply,
                         _resource("r-1", start=30, end=600),
                         "rk-3")
        self._greener_eu_signal()
        record, _ = self._evaluate()
        candidate = next(c for c in record["candidates"]
                         if c["resource"]["resource_id"] == "r-1")
        self.assertEqual(candidate["resource"]["version"], 2)

    def test_latest_unexpired_signal_drives_migration_item(self) -> None:
        world = _SignalWindowWorld(self.tmp.name)
        at45, _ = world.evaluate("ak-a", 45, "advice-a.json")
        # At 45 eu-north still runs the less green v1: r-2 stays first.
        self.assertEqual([c["resource"]["resource_id"]
                          for c in at45["candidates"]], ["r-2", "r-1"])
        eu45 = next(c for c in at45["candidates"]
                    if c["resource"]["resource_id"] == "r-1")
        self.assertEqual(eu45["signal"]["version"], 1)
        # A greener v2 observation starts at 50.
        world.add_v2()
        at60, _ = world.evaluate("ak-b", 60, "advice-b.json")
        eu60 = next(c for c in at60["candidates"]
                    if c["resource"]["resource_id"] == "r-1")
        self.assertEqual(eu60["signal"]["version"], 2)
        self.assertEqual(at60["advice"], "migrate")

    def test_static_trade_prices_retained_item_on_current_signal(self) -> None:
        # A static clear picks r-1 on static figures (tie broken by id);
        # the retained advice item still carries the region's current
        # signal rather than the supply version's static figures.
        trade, created = clear(self.jobs, self.supply, self.trades,
                               "j-1", "tk-1", 40)
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-1")
        self._commit()
        record, _ = self._evaluate(at=60)
        by_id = {c["resource"]["resource_id"]: c
                 for c in record["candidates"]}
        self.assertEqual(by_id["r-1"]["resource"]["version"], 1)
        self.assertEqual(by_id["r-1"]["signal"]["version"], 1)
        self.assertEqual(by_id["r-1"]["signal"]["carbon_intensity"], 7)
        self.assertEqual(by_id["r-1"]["total_cost"], 50)
        # us-west live carbon 2 beats eu-north 7: migrate advice.
        self.assertEqual(record["advice"], "migrate")
        self.assertEqual(record["target"],
                         {"resource_id": "r-2", "version": 1})

    def test_region_and_residency_constraints_exclude_candidates(self) -> None:
        self._live_trade()
        self._commit()
        # A resource outside the job regions never contributes.
        publish_resource(self.supply,
                         _resource("r-3", region="ap-south",
                                   residency=["ap-south", "eu-north"]),
                         "rk-3")
        publish_signal(self.signals,
                       _signal("ap-south", carbon_intensity=0, unit_cost=0),
                       "sk-3")
        # A resource whose residency does not cover the job is excluded.
        publish_resource(self.supply,
                         _resource("r-4", region="us-west",
                                   residency=["us-west"], start=1),
                         "rk-4")
        record, _ = self._evaluate(at=60)
        ids = {c["resource"]["resource_id"] for c in record["candidates"]}
        self.assertNotIn("r-3", ids)
        self.assertNotIn("r-4", ids)

    def test_budget_exclusion_uses_live_signal(self) -> None:
        # A new eu-north observation that is greener on carbon but pricier
        # on cost (carbon 1, unit cost 100): with the default budgets r-1
        # moves ahead on the carbon order.
        self._live_trade()
        self._commit()
        publish_signal(self.signals,
                       _signal("eu-north", observed=50, unit_cost=100,
                               carbon_intensity=1),
                       "sk-3")
        record, _ = evaluate(
            self.jobs, self.supply, self.signals, self.trades,
            self.dispatch, self.execution,
            os.path.join(self.tmp.name, "advice-cap.json"),
            "j-1", "ak-cap", 60)
        self.assertEqual([c["resource"]["resource_id"]
                          for c in record["candidates"]], ["r-1", "r-2"])
        # The same world with a cost budget of 90 clears the trade on
        # r-2 (9 * 10 == 90) but prices the new eu signal at 1000, so
        # r-1 is budget-excluded and the advice keeps r-2.
        tight_record, _ = _submit_tight_world(self.tmp.name)
        self.assertEqual(tight_record["advice"], "keep")
        self.assertEqual(
            [c["resource"]["resource_id"]
             for c in tight_record["candidates"]], ["r-2"])

    def test_capacity_deducts_other_jobs_but_not_own_booking(self) -> None:
        world = _CapacityWorld(self.tmp.name)
        record, _ = world.run()
        ids = [c["resource"]["resource_id"] for c in record["candidates"]]
        # r-1 sold 10 of its 15 units to j-2: it cannot host j-1's work
        # of 10, so it is excluded.
        self.assertNotIn("r-1", ids)
        # r-2 hosts exactly j-1's 10 units; its own booking is not
        # deducted twice, so the retained candidate survives.
        self.assertIn("r-2", ids)
        self.assertEqual(record["advice"], "keep")

    def test_no_feasible_candidate_lookup_error_writes_nothing(self) -> None:
        # Both region signals expire at 49: at 51 every candidate -- the
        # retained frozen version included -- lacks a current signal.
        root = self.tmp.name
        supply = os.path.join(root, "supply-e.json")
        signals = os.path.join(root, "signals-e.json")
        trades = os.path.join(root, "trades-e.json")
        dispatchp = os.path.join(root, "dispatch-e.json")
        submit(self.jobs, _job("j-e"), "jk-e")
        publish_resource(supply, _resource("r-1"), "rk-1")
        publish_resource(supply,
                         _resource("r-2", region="us-west"), "rk-2")
        publish_signal(signals, _signal("eu-north", expires=49), "sk-1")
        publish_signal(signals, _signal("us-west", expires=49), "sk-2")
        clear_live(self.jobs, supply, signals, trades, "j-e", "tk-1", 40)
        commit(self.jobs, supply, trades, dispatchp, "j-e", "ck-1", 45)
        advice_path = os.path.join(root, "advice-e.json")
        with self.assertRaises(LookupError):
            evaluate(self.jobs, supply, signals, trades, dispatchp,
                     self.execution, advice_path, "j-e", "ak-1", 51)
        self.assertFalse(Path(advice_path).exists())

    # -- gates ---------------------------------------------------------

    def test_succeeded_dispatch_raises_value_error(self) -> None:
        self._live_trade()
        self._commit()
        claim(self.dispatch, "j-1", "lk-1", "worker-1", 20, 50)
        finish(self.dispatch, "j-1", "fk-1", "worker-1", "succeeded", 55)
        with self.assertRaises(ValueError):
            self._evaluate()
        self.assertFalse(Path(self.advice).exists())

    def test_completed_plan_raises_value_error(self) -> None:
        self._live_trade()
        self._commit()
        claim(self.dispatch, "j-1", "lk-1", "worker-1", 20, 60)
        plan(self.jobs, self.supply, self.trades, self.dispatch,
             self.execution, "j-1", "pk-1", "worker-1", None, 61)
        record(self.execution, "j-1", 1, "rr-1", "worker-1", "stage",
               "succeeded", "staged", 62)
        record(self.execution, "j-1", 1, "rr-2", "worker-1", "start",
               "succeeded", "started", 63)
        with self.assertRaises(ValueError):
            self._evaluate(at=64)
        self.assertFalse(Path(self.advice).exists())

    def test_active_plan_raises_permission_error(self) -> None:
        self._live_trade()
        self._commit()
        claim(self.dispatch, "j-1", "lk-1", "worker-1", 20, 60)
        plan(self.jobs, self.supply, self.trades, self.dispatch,
             self.execution, "j-1", "pk-1", "worker-1", None, 61)
        with self.assertRaises(PermissionError):
            self._evaluate(at=62)
        self.assertFalse(Path(self.advice).exists())

    def test_failed_and_interrupted_plans_still_advise(self) -> None:
        self._live_trade()
        self._commit()
        claim(self.dispatch, "j-1", "lk-1", "worker-1", 5, 60)
        plan(self.jobs, self.supply, self.trades, self.dispatch,
             self.execution, "j-1", "pk-1", "worker-1", None, 61)
        record(self.execution, "j-1", 1, "rr-1", "worker-1", "stage",
               "failed", "boom", 62)
        failed_record, _ = self._evaluate(at=63)
        self.assertEqual(failed_record["execution"], "failed")
        self.assertEqual(failed_record["dispatch"], "claimed")

        # Attempt 2 requires the expired claim back first, then a new
        # plan that is recovered once its lease strictly expires.
        dispatch_recover(self.dispatch, "j-1", "dk-1", 66)
        claim(self.dispatch, "j-1", "lk-2", "worker-1", 5, 66)
        plan(self.jobs, self.supply, self.trades, self.dispatch,
             self.execution, "j-1", "pk-2", "worker-1", None, 67)
        recover(self.execution, "j-1", 2, "er-1", 72)
        interrupted_record, _ = self._evaluate(
            key="ak-2", at=72,
            ledger=os.path.join(self.tmp.name, "advice-i.json"))
        self.assertEqual(interrupted_record["execution"], "interrupted")

    def test_past_deadline_raises_timeout_error(self) -> None:
        self._live_trade()
        self._commit()
        with self.assertRaises(TimeoutError):
            self._evaluate(at=101)
        self.assertFalse(Path(self.advice).exists())

    # -- lookup errors -------------------------------------------------

    def test_unknown_job_raises_key_error(self) -> None:
        self._live_trade()
        self._commit()
        with self.assertRaises(KeyError):
            self._evaluate(job_id="j-9")
        self.assertFalse(Path(self.advice).exists())

    def test_trade_without_dispatch_decision_raises_key_error(self) -> None:
        self._live_trade()
        # The dispatch ledger exists (a second job is committed) but
        # holds no decision for j-1.
        submit(self.jobs, _job("j-2", regions=["eu-north"],
                               residency=["eu-north"]), "jk-2")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-2", "tk-2", 40)
        commit(self.jobs, self.supply, self.trades, self.dispatch,
               "j-2", "ck-2", 45)
        with self.assertRaises(KeyError):
            self._evaluate()
        self.assertFalse(Path(self.advice).exists())

    def test_job_without_trade_raises_lookup_error(self) -> None:
        # Both ledgers exist through j-1, but j-0 was only accepted.
        self._live_trade()
        self._commit()
        submit(self.jobs, _job("j-0"), "jk-0")
        with self.assertRaises(LookupError):
            self._evaluate(job_id="j-0")
        self.assertFalse(Path(self.advice).exists())

    # -- idempotency and ledger bytes ----------------------------------

    def test_replay_returns_stored_record_without_writing(self) -> None:
        self._live_trade()
        self._commit()
        record, created = self._evaluate(at=45)
        self.assertTrue(created)
        raw = Path(self.advice).read_bytes()
        # A later signal publication must not change an equivalent replay.
        self._greener_eu_signal()
        replayed, created_again = self._evaluate(at=45)
        self.assertFalse(created_again)
        self.assertEqual(replayed, record)
        self.assertEqual(Path(self.advice).read_bytes(), raw)

    def test_same_key_different_request_raises_value_error(self) -> None:
        self._live_trade()
        self._commit()
        self._evaluate(at=45)
        raw = Path(self.advice).read_bytes()
        with self.assertRaises(ValueError):
            self._evaluate(at=46)
        self.assertEqual(Path(self.advice).read_bytes(), raw)

    def test_ledger_canonical_form_and_key_order(self) -> None:
        self._live_trade()
        self._commit()
        self._evaluate(key="b-key", at=45)
        self._evaluate(key="a-key", at=45)
        raw = Path(self.advice).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        self.assertNotIn(b": ", raw)
        self.assertNotIn(b", ", raw)
        data = json.loads(raw)
        self.assertEqual(list(data.keys()),
                         ["version", "records", "idempotency", "audit"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["records"]), ["a-key", "b-key"])
        self.assertEqual(list(data["idempotency"]), ["a-key", "b-key"])
        self.assertEqual(list(data["audit"]), ["a-key", "b-key"])
        self.assertEqual(data["idempotency"]["a-key"],
                         {"job_id": "j-1", "at": 45})
        event = data["audit"]["a-key"]
        self.assertEqual(list(event.keys()), ["key", "request", "result"])
        self.assertEqual(event["key"], "a-key")
        self.assertEqual(event["request"], {"job_id": "j-1", "at": 45})
        self.assertEqual(event["result"], data["records"]["a-key"])

    def test_non_ascii_written_through(self) -> None:
        self._live_trade()
        self._commit()
        self._evaluate(key="建议-1")
        raw = Path(self.advice).read_bytes()
        self.assertIn("建议-1".encode("utf-8"), raw)
        self.assertNotIn(b"\\u", raw)

    def test_invalid_ledger_raises_value_error(self) -> None:
        self._live_trade()
        self._commit()
        self._evaluate()
        raw = Path(self.advice).read_bytes()
        Path(self.advice).write_bytes(raw[:-1])  # drop trailing newline
        with self.assertRaises(ValueError):
            self._evaluate(at=45)
        Path(self.advice).write_bytes(b"{not json\n")
        with self.assertRaises(ValueError):
            self._evaluate(at=45)

    # -- arguments and missing files -----------------------------------

    def test_invalid_arguments(self) -> None:
        args = [self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, "j-1", "ak", 60]
        for index in range(7):
            broken = list(args)
            broken[index] = ""
            with self.assertRaises(ValueError):
                evaluate(*broken)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            evaluate(*args[:7], "", "ak", 60)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            evaluate(*args[:8], "", 60)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            evaluate(*args[:9], -1)
        with self.assertRaises(ValueError):
            evaluate(*args[:9], True)  # type: ignore[arg-type]

    def test_paths_must_be_distinct_real_locations(self) -> None:
        with self.assertRaises(ValueError):
            evaluate(self.jobs, self.jobs, self.signals, self.trades,
                     self.dispatch, self.execution, self.advice,
                     "j-1", "ak", 60)

    def test_missing_inputs_raise_file_not_found(self) -> None:
        self._live_trade()
        self._commit()
        existing = (self.jobs, self.supply, self.signals, self.trades,
                    self.dispatch, self.execution)
        missing_names = ("jobs-m.json", "supply-m.json", "signals-m.json",
                         "trades-m.json", "dispatch-m.json",
                         "execution-m.json")
        for missing_index in range(6):
            paths = list(existing)
            paths[missing_index] = os.path.join(self.tmp.name,
                                                missing_names[missing_index])
            advice_path = os.path.join(
                self.tmp.name, f"advice-m{missing_index}.json")
            with self.assertRaises(FileNotFoundError):
                evaluate(*paths, advice_path, "j-1", "ak", 60)
            self.assertFalse(Path(advice_path).exists())

    def test_missing_ledger_parent_raises_file_not_found(self) -> None:
        self._live_trade()
        self._commit()
        with self.assertRaises(FileNotFoundError):
            self._evaluate(ledger=os.path.join(self.tmp.name, "no-dir",
                                               "advice.json"))
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])


def _submit_tight_world(root: str) -> tuple[dict[str, object], bool]:
    # A fresh world identical to the main setup but with the job's cost
    # budget set to 90: the trade still clears on r-2 (9 * 10 == 90),
    # but the greener eu-north signal arriving at 50 prices at 1000,
    # breaks the cost budget and is excluded from the migration items.
    jobs = os.path.join(root, "t-jobs.json")
    supply = os.path.join(root, "t-supply.json")
    signals = os.path.join(root, "t-signals.json")
    trades = os.path.join(root, "t-trades.json")
    dispatchp = os.path.join(root, "t-dispatch.json")
    execution = os.path.join(root, "t-execution.json")
    advice = os.path.join(root, "t-advice.json")
    Path(execution).write_text(_EMPTY_EXECUTION, encoding="utf-8")
    submit(jobs, _job(max_cost=90), "jk-1")
    publish_resource(supply, _resource("r-1"), "rk-1")
    publish_resource(supply, _resource("r-2", region="us-west"), "rk-2")
    publish_signal(signals, _signal("eu-north", unit_cost=5,
                                    carbon_intensity=7), "sk-1")
    publish_signal(signals, _signal("us-west", unit_cost=9,
                                    carbon_intensity=2), "sk-2")
    clear_live(jobs, supply, signals, trades, "j-1", "tk-1", 40)
    commit(jobs, supply, trades, dispatchp, "j-1", "ck-1", 45)
    publish_signal(signals,
                   _signal("eu-north", observed=50, unit_cost=100,
                           carbon_intensity=1), "sk-3")
    return evaluate(jobs, supply, signals, trades, dispatchp, execution,
                    advice, "j-1", "ak-1", 60)


class _SignalWindowWorld:
    """Trade with a eu-north signal expiring at 49, then publish v2."""

    def __init__(self, root: str) -> None:
        self.jobs = os.path.join(root, "w-jobs.json")
        self.supply = os.path.join(root, "w-supply.json")
        self.signals = os.path.join(root, "w-signals.json")
        self.trades = os.path.join(root, "w-trades.json")
        self.dispatch = os.path.join(root, "w-dispatch.json")
        self.execution = os.path.join(root, "w-execution.json")
        Path(self.execution).write_text(_EMPTY_EXECUTION, encoding="utf-8")
        submit(self.jobs, _job(), "jk-1")
        publish_resource(self.supply, _resource("r-1"), "rk-1")
        publish_resource(self.supply,
                         _resource("r-2", region="us-west"), "rk-2")
        publish_signal(self.signals,
                       _signal("eu-north", expires=49, unit_cost=5,
                               carbon_intensity=7),
                       "sk-1")
        publish_signal(self.signals,
                       _signal("us-west", unit_cost=9, carbon_intensity=2),
                       "sk-2")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-1", "tk-1", 40)
        commit(self.jobs, self.supply, self.trades, self.dispatch,
               "j-1", "ck-1", 45)

    def add_v2(self) -> None:
        publish_signal(self.signals,
                       _signal("eu-north", observed=50, unit_cost=1,
                               carbon_intensity=1, expires=500),
                       "sk-3")

    def evaluate(self, key: str, at: int, advice_name: str
                 ) -> tuple[dict[str, object], bool]:
        return evaluate(self.jobs, self.supply, self.signals, self.trades,
                        self.dispatch, self.execution,
                        os.path.join(os.path.dirname(self.jobs), advice_name),
                        "j-1", key, at)


class _CapacityWorld:
    """Two jobs sharing one supply: j-2 occupies 10 of r-1's 15 units."""

    def __init__(self, root: str) -> None:
        self.jobs = os.path.join(root, "c-jobs.json")
        self.supply = os.path.join(root, "c-supply.json")
        self.signals = os.path.join(root, "c-signals.json")
        self.trades = os.path.join(root, "c-trades.json")
        self.dispatch = os.path.join(root, "c-dispatch.json")
        self.execution = os.path.join(root, "c-execution.json")
        self.advice = os.path.join(root, "c-advice.json")
        Path(self.execution).write_text(_EMPTY_EXECUTION, encoding="utf-8")

    def run(self) -> tuple[dict[str, object], bool]:
        submit(self.jobs, _job("j-1"), "jk-1")
        submit(self.jobs,
               _job("j-2", regions=["eu-north"], residency=["eu-north"]),
               "jk-2")
        publish_resource(self.supply,
                         _resource("r-1", capacity=15), "rk-1")
        publish_resource(self.supply,
                         _resource("r-2", region="us-west", capacity=10),
                         "rk-2")
        publish_signal(self.signals,
                       _signal("eu-north", carbon_intensity=1, unit_cost=1),
                       "sk-1")
        publish_signal(self.signals,
                       _signal("us-west", carbon_intensity=2, unit_cost=9),
                       "sk-2")
        # j-2 can only use eu-north: it books r-1's 10 of 15 units.
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-2", "tk-2", 40)
        # j-1 then finds r-1 with 5 units left and books r-2 instead.
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-1", "tk-1", 40)
        commit(self.jobs, self.supply, self.trades, self.dispatch,
               "j-2", "ck-2", 45)
        commit(self.jobs, self.supply, self.trades, self.dispatch,
               "j-1", "ck-1", 45)
        return evaluate(self.jobs, self.supply, self.signals, self.trades,
                        self.dispatch, self.execution, self.advice,
                        "j-1", "ak-1", 60)


if __name__ == "__main__":
    unittest.main()
