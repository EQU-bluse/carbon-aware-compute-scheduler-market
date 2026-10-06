"""Systematic lifecycle capacity-conservation tests over the public API.

One deterministic timeline is built from an empty temporary directory
using only the public entry points -- ``jobs.submit``,
``resources.publish``, ``signals.publish``, ``market.clear``,
``cancellation.cancel``/``get``, ``dispatch.commit``/``claim``,
``execution.plan``/``record``, ``execution_sync.run``,
``rebalance.evaluate``/``apply``/``start``/``record``/``settle``/
``current`` and ``completion.complete``/``get``/``search`` -- and their
persisted results. Several jobs trade against two versions of one
resource and a second resource, then walk to the four terminal states:
cancellation before dispatch, completion without migration, completion
after a successful migration and completion after a compensated
(failed) migration.

Around every cancellation, migration settlement and completion moment
the tests re-clear at the moment before, the moment itself and the
moment after: only terminal jobs whose event moment is not later than
the clearing moment may release the version they actually occupied. A
successful migration releases the source version and occupies the
target version, a compensated migration keeps occupying the source
version, and a completion releases the current binding together with
every historical migration reservation.

Every expectation is computed independently from the published
capacity and the public records (the persisted ledgers and the public
query functions): for each exact resource version the cumulative
occupancy must never be negative and never exceed the published
capacity, old trade and settlement records must stay byte-identical,
and omitting the optional completion or cancellation ledger must keep
the original occupancy semantics. Fresh clearing requests prove that
released capacity is genuinely reusable and that jobs not yet at their
release moment -- or still running -- cannot be evicted early.

The concurrency tests show that a cancellation racing a first dispatch
commit lets exactly one first write succeed (the loser gets
``PermissionError`` when the commit landed first, ``ValueError`` when
the cancellation did) and never both, and that concurrent clearings
for the last capacity unit never oversell. Idempotent replays of the
same key with an equivalent request return the original record without
rewriting a byte; the same key with a different request raises
``ValueError`` and leaves every input file unchanged.

No product interface is added or changed: parameters, return objects,
exception types and ledger formats are exactly what the sequential
calls already produce.
"""

from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import cancellation as cancellation_module
from carbon_market import completion as completion_module
from carbon_market import dispatch as dispatch_module
from carbon_market import execution as execution_module
from carbon_market import execution_sync as execution_sync_module
from carbon_market import jobs as jobs_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.cancellation import cancel
from carbon_market.completion import complete
from carbon_market.market import clear
from carbon_market.rebalance import (apply, current, evaluate,
                                     record as migrate_record, settle, start)

_INF = float("inf")


def _resource(resource_id: str, region: str = "eu-north",
              **overrides: object) -> dict[str, object]:
    resource: dict[str, object] = {
        "resource_id": resource_id,
        "region": region,
        "capacity": 30,
        "start": 0,
        "end": 500,
        "unit_cost": 1,
        "carbon_intensity": 1,
        "residency": (["eu-north"] if region == "eu-north"
                      else ["eu-north", "us-west"]),
    }
    resource.update(overrides)
    return resource


def _signal(region: str, **overrides: object) -> dict[str, object]:
    signal: dict[str, object] = {
        "region": region,
        "observed": 0,
        "expires": 500,
        "mix": {"solar": 10000},
        "unit_cost": 8,
        "carbon_intensity": 8,
    }
    signal.update(overrides)
    return signal


def _job(job_id: str, work: int,
         regions: tuple[str, ...] = ("eu-north",),
         **overrides: object) -> dict[str, object]:
    job: dict[str, object] = {
        "job_id": job_id,
        "work": work,
        "deadline": 400,
        "regions": sorted(regions),
        "residency": ["eu-north"],
        "max_cost": 1000000,
        "carbon_cap": 1000000,
    }
    job.update(overrides)
    return job


class LifecycleCapacityTest(unittest.TestCase):
    """One deterministic lifecycle timeline with capacity checkpoints."""

    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        base = self.tmp.name
        self.jobs = os.path.join(base, "jobs.json")
        self.supply = os.path.join(base, "supply.json")
        self.signals = os.path.join(base, "signals.json")
        self.trades = os.path.join(base, "trades.json")
        self.dispatch = os.path.join(base, "dispatch.json")
        self.execution = os.path.join(base, "execution.json")
        self.sync = os.path.join(base, "sync.json")
        self.advice = os.path.join(base, "advice.json")
        self.intents = os.path.join(base, "intents.json")
        self.settlements = os.path.join(base, "settlements.json")
        self.completions = os.path.join(base, "completions.json")
        self.cancellations = os.path.join(base, "cancellations.json")
        self._probe_seq = 0
        # Records as first returned by the public calls; every later
        # checkpoint re-reads the persisted state and proves the old
        # records unchanged.
        self._expected_trades: dict[str, dict[str, object]] = {}
        self._expected_settlements: dict[str, dict[str, object]] = {}
        self._expected_completions: dict[str, dict[str, object]] = {}
        self._expected_cancellations: dict[str, dict[str, object]] = {}

    # -- path tuples ---------------------------------------------------

    def _seven(self) -> tuple[str, ...]:
        return (self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice)

    def _eight(self) -> tuple[str, ...]:
        return self._seven() + (self.intents,)

    def _nine(self) -> tuple[str, ...]:
        return self._eight() + (self.settlements,)

    # -- generic helpers ------------------------------------------------

    @staticmethod
    def _read_json(path: str) -> dict[str, object] | None:
        if not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)

    def _snapshot_bytes(self) -> dict[str, bytes]:
        return {name: Path(self.tmp.name, name).read_bytes()
                for name in os.listdir(self.tmp.name)
                if Path(self.tmp.name, name).is_file()}

    def _assert_idempotent(self, replay, changed,
                           expected: dict[str, object] | None = None
                           ) -> dict[str, object]:
        """An equivalent replay returns the stored record with ``False``
        and rewrites nothing; the same key with a changed request raises
        ``ValueError`` and leaves every file byte-for-byte unchanged."""
        before = self._snapshot_bytes()
        record, created = replay()
        self.assertFalse(created)
        if expected is not None:
            self.assertEqual(record, expected)
        self.assertEqual(self._snapshot_bytes(), before)
        with self.assertRaises(ValueError):
            changed()
        self.assertEqual(self._snapshot_bytes(), before)
        return record

    def _capacity(self, slot: tuple[str, int]) -> int:
        resource_id, version = slot
        record = resources_module.get(self.supply, resource_id, version)
        return record["capacity"]

    # -- the independent capacity model ---------------------------------
    #
    # Everything below is recomputed from the published capacity and the
    # public records only: the persisted ledgers on disk and the public
    # query functions, never from the test's own call history.

    def _public_records(self) -> dict[str, object]:
        trades: dict[str, dict[str, object]] = {}
        trades_raw = self._read_json(self.trades)
        if trades_raw is not None:
            for job_id, record in trades_raw["trades"].items():
                trades[job_id] = {
                    "slot": (record["resource_id"], record["version"]),
                    "work": record["work"],
                    "at": record["at"],
                }
        cancellations: dict[str, int] = {}
        cancellations_raw = self._read_json(self.cancellations)
        if cancellations_raw is not None:
            for record in cancellations_raw["cancellations"].values():
                cancellations[record["job_id"]] = record["at"]
        completions: dict[str, int] = {}
        try:
            page = completion_module.search(self.completions)
            for entry in page["entries"]:
                completions[entry["job_id"]] = entry["record"]["at"]
        except FileNotFoundError:
            pass
        settlements: dict[str, list[dict[str, object]]] = {}
        settlements_raw = self._read_json(self.settlements)
        if settlements_raw is not None:
            for record in settlements_raw["records"].values():
                settlements.setdefault(record["job_id"], []).append(record)
        intents: dict[str, list[dict[str, object]]] = {}
        plans: dict[str, list[dict[str, object]]] = {}
        intents_raw = self._read_json(self.intents)
        if intents_raw is not None:
            for record in intents_raw.get("intents", {}).values():
                intents.setdefault(record["job_id"], []).append(record)
            for record in intents_raw.get("plans", {}).values():
                plans.setdefault(record["job_id"], []).append(record)
        return {"trades": trades, "cancellations": cancellations,
                "completions": completions, "settlements": settlements,
                "intents": intents, "plans": plans}

    def _clear_view(self, at: int, records: dict[str, object],
                    completions: bool = True,
                    cancellations: bool = True
                    ) -> dict[tuple[str, int], int]:
        """The occupancy ``market.clear`` deducts at ``at``: every trade
        whose job has no completion or cancellation effective at ``at``
        still occupies its exact traded version."""
        sold: dict[tuple[str, int], int] = {}
        for job_id, trade in records["trades"].items():
            if completions and records["completions"].get(job_id, _INF) <= at:
                continue
            if cancellations \
                    and records["cancellations"].get(job_id, _INF) <= at:
                continue
            slot = trade["slot"]
            sold[slot] = sold.get(slot, 0) + trade["work"]
        return sold

    def _effective_view(self, at: int, records: dict[str, object]
                        ) -> dict[tuple[str, int], int]:
        """The occupancy the lifecycle actually holds at ``at``: a
        cancelled or completed job holds nothing; a settled migration
        holds its after-binding (target when migrated, source when
        compensated); an unsettled migrated plan holds only the target;
        a failed or interrupted plan holds only the source; an open
        reservation holds both source and target."""
        occupancy: dict[tuple[str, int], int] = {}

        def hold(slot: tuple[str, int], work: int) -> None:
            occupancy[slot] = occupancy.get(slot, 0) + work

        for job_id, trade in records["trades"].items():
            work = trade["work"]
            if records["completions"].get(job_id, _INF) <= at:
                continue
            if records["cancellations"].get(job_id, _INF) <= at:
                continue
            settled = [record for record
                       in records["settlements"].get(job_id, ())
                       if record["state"] in ("active", "compensated")
                       and record["at"] <= at]
            if settled:
                latest = max(settled, key=lambda record: record["generation"])
                hold((latest["after"]["resource_id"],
                      latest["after"]["version"]), work)
                continue
            job_plans = records["plans"].get(job_id, ())
            job_intents = records["intents"].get(job_id, ())
            if any(plan["state"] == "migrated" for plan in job_plans):
                target = job_intents[-1]["target"]
                hold((target["resource_id"], target["version"]), work)
            elif any(plan["state"] in ("failed", "interrupted")
                     for plan in job_plans):
                hold(trade["slot"], work)
            elif job_intents:
                # A reserved or active migration round holds the source
                # trade occupancy and the target reservation at once.
                hold(trade["slot"], work)
                target = job_intents[-1]["target"]
                hold((target["resource_id"], target["version"]), work)
            else:
                hold(trade["slot"], work)
        return occupancy

    def _assert_within_capacity(
            self, occupancy: dict[tuple[str, int], int], label: str
    ) -> None:
        for slot, amount in occupancy.items():
            with self.subTest(view=label, slot=slot):
                self.assertGreaterEqual(amount, 0,
                                        f"{label} occupancy of {slot}")
                self.assertLessEqual(amount, self._capacity(slot),
                                     f"{label} occupancy of {slot}")

    def _predict_clear(self, at: int, work: int,
                       sold: dict[tuple[str, int], int]
                       ) -> tuple[str, int] | None:
        """Independently replay ``market.clear``'s documented selection
        for a probe job confined to ``eu-north``: each resource's
        highest version valid at ``at`` with enough remaining capacity,
        ordered by carbon intensity, unit cost and resource id."""
        supply = self._read_json(self.supply)
        best: tuple[tuple[int, int, str], tuple[str, int]] | None = None
        for resource_id, versions in supply["history"].items():
            active = None
            for record in versions:
                if record["start"] <= at <= record["end"]:
                    active = record
            if active is None or active["region"] != "eu-north":
                continue
            remaining = active["capacity"] - sold.get(
                (resource_id, active["version"]), 0)
            if remaining < work:
                continue
            if "eu-north" not in active["residency"]:
                continue
            if active["end"] < 400:
                continue
            if work * active["unit_cost"] > 1000000 \
                    or work * active["carbon_intensity"] > 1000000:
                continue
            rank = (active["carbon_intensity"], active["unit_cost"],
                    resource_id)
            if best is None or rank < best[0]:
                best = (rank, (resource_id, active["version"]))
        return best[1] if best is not None else None

    # -- public-operation wrappers ---------------------------------------

    def _clear_kwargs(self) -> dict[str, str]:
        kwargs: dict[str, str] = {}
        if os.path.exists(self.completions):
            kwargs["completions"] = self.completions
        if os.path.exists(self.cancellations):
            kwargs["cancellations"] = self.cancellations
        return kwargs

    def _trade(self, job_id: str, key: str, at: int) -> dict[str, object]:
        trade, created = clear(self.jobs, self.supply, self.trades,
                               job_id, key, at, **self._clear_kwargs())
        self.assertTrue(created)
        self._expected_trades[job_id] = trade
        return trade

    def _probe(self, at: int, work: int, completions: bool = True,
               cancellations: bool = True) -> dict[str, object] | None:
        """Clear one fresh probe job at ``at`` and assert the outcome
        matches the independently computed prediction: ``LookupError``
        when no version has enough remaining capacity, otherwise a
        trade on exactly the predicted version. A successful probe
        stays booked -- it is one of the still-running jobs that must
        never be evicted early."""
        self._probe_seq += 1
        seq = self._probe_seq
        job_id = f"probe-{seq}"
        jobs_module.submit(self.jobs, _job(job_id, work), f"pjk-{seq}")
        records = self._public_records()
        sold = self._clear_view(at, records, completions, cancellations)
        expected = self._predict_clear(at, work, sold)
        kwargs: dict[str, str] = {}
        if completions and os.path.exists(self.completions):
            kwargs["completions"] = self.completions
        if cancellations and os.path.exists(self.cancellations):
            kwargs["cancellations"] = self.cancellations
        if expected is None:
            with self.assertRaises(LookupError):
                clear(self.jobs, self.supply, self.trades, job_id,
                      f"pt-{seq}", at, **kwargs)
            return None
        trade, created = clear(self.jobs, self.supply, self.trades,
                               job_id, f"pt-{seq}", at, **kwargs)
        self.assertTrue(created)
        self.assertEqual((trade["resource_id"], trade["version"]), expected)
        self._expected_trades[job_id] = trade
        return trade

    def _finish_job(self, job_id: str, tag: str, owner: str,
                    claim_at: int) -> None:
        """Claim, execute and synchronize one job to a stable terminal
        state: the dispatch decision is ``succeeded`` and the launch
        plan ``completed``."""
        dispatch_module.claim(self.dispatch, job_id, f"c-{tag}", owner,
                              20, claim_at)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, job_id,
                              f"p-{tag}", owner, None, claim_at + 1)
        execution_module.record(self.execution, job_id, 1, f"pr-{tag}-1",
                                owner, "stage", "succeeded", "staged",
                                claim_at + 2)
        execution_module.record(self.execution, job_id, 1, f"pr-{tag}-2",
                                owner, "start", "succeeded", "started",
                                claim_at + 3)
        execution_sync_module.run(self.execution, self.dispatch, self.sync,
                                  "sync-1", f"b-{tag}", claim_at + 4, 50)

    def _complete(self, job_id: str, key: str, at: int) -> dict[str, object]:
        record, created = complete(self.jobs, self.supply, self.signals,
                                   self.trades, self.dispatch,
                                   self.execution, self.completions,
                                   job_id, key, at, "succeeded", 5, 5)
        self.assertTrue(created)
        self._expected_completions[job_id] = record
        return record

    # -- checkpoint verification -----------------------------------------

    def _assert_history_unchanged(self) -> None:
        """Every trade, settlement, cancellation and completion recorded
        so far is still persisted exactly as first returned, and the
        public queries agree."""
        trades_raw = self._read_json(self.trades)
        for job_id, trade in self._expected_trades.items():
            self.assertEqual(trades_raw["trades"].get(job_id), trade,
                             f"old trade of {job_id} changed")
        settlements_raw = self._read_json(self.settlements)
        for key, record in self._expected_settlements.items():
            self.assertEqual(settlements_raw["records"].get(key), record,
                             f"old settlement {key} changed")
        for job_id, record in self._expected_cancellations.items():
            self.assertEqual(
                cancellation_module.get(self.cancellations, job_id), record)
        for job_id, record in self._expected_completions.items():
            self.assertEqual(
                completion_module.get(self.completions, job_id), record)

    def _checkpoint(
        self,
        slot: tuple[str, int],
        moment: int,
        expected_sold: dict[int, int],
        expected_effective: dict[int, dict[tuple[str, int], int]],
        release: int = 0,
    ) -> None:
        """Verify one capacity-changing event at ``moment``: at the
        moment before, the moment itself and the moment after, the
        occupancy recomputed from the public records matches the
        independent expectation and stays within the published capacity,
        and fresh clearing requests prove the remaining capacity exact.
        ``release`` is the amount the event frees in the clearing view;
        a probe of exactly that size must succeed at the event moment."""
        capacity = self._capacity(slot)
        for at in (moment - 1, moment, moment + 1):
            records = self._public_records()
            sold = self._clear_view(at, records)
            self.assertEqual(sold.get(slot, 0), expected_sold[at],
                             f"clear-view occupancy of {slot} at {at}")
            self._assert_within_capacity(sold, "clear")
            effective = self._effective_view(at, records)
            if expected_effective:
                self.assertEqual(effective, expected_effective[at],
                                 f"effective occupancy at {at}")
            self._assert_within_capacity(effective, "effective")
            self._assert_history_unchanged()
        # One moment too early the capacity is still fully held.
        self._probe(moment - 1, capacity - expected_sold[moment - 1] + 1)
        # At the event moment only the released amount is free.
        self._probe(moment, capacity - expected_sold[moment] + 1)
        if release:
            trade = self._probe(moment, release)
            self.assertIsNotNone(trade)
            self.assertEqual((trade["resource_id"], trade["version"]), slot)
        # One moment later the accounting is unchanged and still exact.
        records = self._public_records()
        remaining = capacity - self._clear_view(
            moment + 1, records).get(slot, 0)
        self._probe(moment + 1, remaining + 1)

    # -- the lifecycle scenario -------------------------------------------

    def test_lifecycle_capacity_conservation(self) -> None:
        # Publish capacity: two versions of r-a (the second opens at
        # moment 100) and one version of r-b in another region. Static
        # figures prefer r-a; the live signals make r-b the greener
        # migration target.
        record, created = resources_module.publish(
            self.supply, _resource("r-a", capacity=30), "rk-a-1")
        self.assertTrue(created)
        self._assert_idempotent(
            lambda: resources_module.publish(
                self.supply, _resource("r-a", capacity=30), "rk-a-1"),
            lambda: resources_module.publish(
                self.supply, _resource("r-a", capacity=31), "rk-a-1"),
            expected=record)
        resources_module.publish(
            self.supply,
            _resource("r-a", capacity=6, start=100), "rk-a-2")
        resources_module.publish(
            self.supply,
            _resource("r-b", region="us-west", capacity=5, unit_cost=2,
                      carbon_intensity=2), "rk-b-1")
        record, created = signals_module.publish(
            self.signals, _signal("eu-north"), "sk-eu")
        self.assertTrue(created)
        self._assert_idempotent(
            lambda: signals_module.publish(
                self.signals, _signal("eu-north"), "sk-eu"),
            lambda: signals_module.publish(
                self.signals, _signal("eu-north", unit_cost=9), "sk-eu"),
            expected=record)
        signals_module.publish(
            self.signals,
            _signal("us-west", unit_cost=2, carbon_intensity=2), "sk-us")

        # Accept the lifecycle jobs: one per terminal state plus one
        # that trades on the second resource version.
        record, created = jobs_module.submit(
            self.jobs, _job("j-cancel", 4), "jk-cancel")
        self.assertTrue(created)
        self._assert_idempotent(
            lambda: jobs_module.submit(
                self.jobs, _job("j-cancel", 4), "jk-cancel"),
            lambda: jobs_module.submit(
                self.jobs, _job("j-cancel", 5), "jk-cancel"),
            expected=record)
        jobs_module.submit(self.jobs, _job("j-plain", 3), "jk-plain")
        jobs_module.submit(
            self.jobs,
            _job("j-migrate", 2, regions=("eu-north", "us-west")),
            "jk-migrate")
        jobs_module.submit(
            self.jobs,
            _job("j-comp", 2, regions=("eu-north", "us-west")), "jk-comp")
        jobs_module.submit(self.jobs, _job("j-v2", 4), "jk-v2")

        # -- t=10: several jobs trade and compete for one version ------
        for job_id, key in (("j-cancel", "t-cancel"), ("j-plain", "t-plain"),
                            ("j-migrate", "t-migrate"), ("j-comp", "t-comp")):
            trade = self._trade(job_id, key, 10)
            self.assertEqual((trade["resource_id"], trade["version"]),
                             ("r-a", 1))
        self._assert_idempotent(
            lambda: clear(self.jobs, self.supply, self.trades,
                          "j-cancel", "t-cancel", 10),
            lambda: clear(self.jobs, self.supply, self.trades,
                          "j-cancel", "t-cancel", 11),
            expected=self._expected_trades["j-cancel"])
        # Eleven of thirty units are sold; one more unit than the
        # remaining nineteen cannot be traded.
        self._probe(10, 20)
        for job_id, key in (("j-plain", "d-plain"), ("j-migrate", "d-migrate"),
                            ("j-comp", "d-comp")):
            dispatch_module.commit(self.jobs, self.supply, self.trades,
                                   self.dispatch, job_id, key, 12)
        self._assert_idempotent(
            lambda: dispatch_module.commit(
                self.jobs, self.supply, self.trades, self.dispatch,
                "j-plain", "d-plain", 12),
            lambda: dispatch_module.commit(
                self.jobs, self.supply, self.trades, self.dispatch,
                "j-plain", "d-plain", 13))

        # -- t=30: cancellation before dispatch releases the trade -----
        record, created = cancel(self.jobs, self.supply, self.trades,
                                 self.dispatch, self.cancellations,
                                 "j-cancel", "x-cancel", 30, "superseded")
        self.assertTrue(created)
        self.assertEqual(record, {
            "job_id": "j-cancel", "at": 30, "reason": "superseded",
            "resource_id": "r-a", "version": 1})
        self._expected_cancellations["j-cancel"] = record
        self._assert_idempotent(
            lambda: cancel(self.jobs, self.supply, self.trades,
                           self.dispatch, self.cancellations,
                           "j-cancel", "x-cancel", 30, "superseded"),
            lambda: cancel(self.jobs, self.supply, self.trades,
                           self.dispatch, self.cancellations,
                           "j-cancel", "x-cancel", 30, "other"),
            expected=record)
        self._checkpoint(
            ("r-a", 1), 30,
            expected_sold={29: 11, 30: 7, 31: 7},
            expected_effective={
                29: {("r-a", 1): 11},
                30: {("r-a", 1): 7},
                31: {("r-a", 1): 7}},
            release=4)

        # -- t=35..39: j-plain runs to a stable terminal state ---------
        # The completion itself is registered later, at t=60.
        self._finish_job("j-plain", "plain", "owner-plain", 35)
        self._assert_idempotent(
            lambda: execution_module.plan(
                self.jobs, self.supply, self.trades, self.dispatch,
                self.execution, "j-plain", "p-plain", "owner-plain", None,
                36),
            lambda: execution_module.plan(
                self.jobs, self.supply, self.trades, self.dispatch,
                self.execution, "j-plain", "p-plain", "owner-plain", None,
                37))
        self._assert_idempotent(
            lambda: execution_module.record(
                self.execution, "j-plain", 1, "pr-plain-1", "owner-plain",
                "stage", "succeeded", "staged", 37),
            lambda: execution_module.record(
                self.execution, "j-plain", 1, "pr-plain-1", "owner-plain",
                "stage", "succeeded", "other", 37))

        # -- t=40..54: two migration rounds, one succeeds, one fails ---
        advice, created = evaluate(*self._seven(), "j-migrate",
                                   "adv-migrate", 40)
        self.assertTrue(created)
        self.assertEqual(advice["recommendation"], "migrate")
        self.assertEqual(advice["target"],
                         {"resource_id": "r-b", "version": 1})
        self._assert_idempotent(
            lambda: evaluate(*self._seven(), "j-migrate", "adv-migrate", 40),
            lambda: evaluate(*self._seven(), "j-migrate", "adv-migrate", 41),
            expected=advice)
        advice, created = evaluate(*self._seven(), "j-comp", "adv-comp", 41)
        self.assertTrue(created)
        self.assertEqual(advice["target"],
                         {"resource_id": "r-b", "version": 1})
        intent, created = apply(*self._eight(), "j-migrate", "adv-migrate",
                                "res-migrate", 45)
        self.assertTrue(created)
        self.assertEqual(intent["target"],
                         {"resource_id": "r-b", "version": 1})
        self._assert_idempotent(
            lambda: apply(*self._eight(), "j-migrate", "adv-migrate",
                          "res-migrate", 45),
            lambda: apply(*self._eight(), "j-migrate", "adv-migrate",
                          "res-migrate", 46),
            expected=intent)
        apply(*self._eight(), "j-comp", "adv-comp", "res-comp", 46)
        plan, created = start(*self._eight(), "j-migrate", "mig-start",
                              "mig-owner", 90, 50)
        self.assertTrue(created)
        self.assertEqual(plan["state"], "active")
        self._assert_idempotent(
            lambda: start(*self._eight(), "j-migrate", "mig-start",
                          "mig-owner", 90, 50),
            lambda: start(*self._eight(), "j-migrate", "mig-start",
                          "other-owner", 90, 50))
        start(*self._eight(), "j-comp", "comp-start", "comp-owner", 90, 51)
        plan, created = migrate_record(
            *self._eight(), "j-migrate", "mr-copy", "mig-owner", "copy",
            "succeeded", "copy-ok", 52)
        self.assertTrue(created)
        self._assert_idempotent(
            lambda: migrate_record(*self._eight(), "j-migrate", "mr-copy",
                                   "mig-owner", "copy", "succeeded",
                                   "copy-ok", 52),
            lambda: migrate_record(*self._eight(), "j-migrate", "mr-copy",
                                   "mig-owner", "copy", "succeeded",
                                   "other", 52))
        migrate_record(*self._eight(), "j-comp", "cr-copy", "comp-owner",
                       "copy", "failed", "copy-broken", 53)
        plan, created = migrate_record(
            *self._eight(), "j-migrate", "mr-switch", "mig-owner", "switch",
            "succeeded", "switch-ok", 54)
        self.assertTrue(created)
        self.assertEqual(plan["state"], "migrated")

        # -- t=60: completion without migration -------------------------
        record = self._complete("j-plain", "x-plain", 60)
        self.assertEqual(record["generation"], 0)
        self.assertEqual(record["current"],
                         {"resource_id": "r-a", "version": 1})
        self._assert_idempotent(
            lambda: complete(self.jobs, self.supply, self.signals,
                             self.trades, self.dispatch, self.execution,
                             self.completions, "j-plain", "x-plain", 60,
                             "succeeded", 5, 5),
            lambda: complete(self.jobs, self.supply, self.signals,
                             self.trades, self.dispatch, self.execution,
                             self.completions, "j-plain", "x-plain", 61,
                             "succeeded", 5, 5),
            expected=record)
        self._checkpoint(
            ("r-a", 1), 60,
            expected_sold={59: 11, 60: 8, 61: 8},
            expected_effective={
                59: {("r-a", 1): 9, ("r-b", 1): 2},
                60: {("r-a", 1): 6, ("r-b", 1): 2},
                61: {("r-a", 1): 6, ("r-b", 1): 2}},
            release=3)
        # Omitting the optional ledgers keeps the original occupancy:
        # the releases are invisible and even one unit more than the
        # unreleased remainder cannot be cleared.
        self._probe(61, 16, completions=False, cancellations=False)

        # -- t=66..67: a reservation provably holds target capacity -----
        jobs_module.submit(
            self.jobs,
            _job("j-late", 4, regions=("eu-north", "us-west")), "jk-late")
        self._trade("j-late", "t-late", 66)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-late", "d-late", 66)
        advice, created = evaluate(*self._seven(), "j-late", "adv-late-1",
                                   67)
        self.assertTrue(created)
        self.assertEqual(advice["target"],
                         {"resource_id": "r-b", "version": 1})
        # r-b holds two reservations of two units each; only one unit
        # remains, so a four-unit reservation is refused.
        with self.assertRaises(LookupError):
            apply(*self._eight(), "j-late", "adv-late-1", "res-late-1", 67)

        # -- t=70: the successful migration settles ----------------------
        binding = current(*self._nine(), "j-migrate")
        self.assertEqual(binding["state"], "traded")
        self.assertEqual((binding["resource_id"], binding["version"]),
                         ("r-a", 1))
        record, created = settle(*self._nine(), "j-migrate", "mig-start",
                                 "set-migrate", 70)
        self.assertTrue(created)
        self.assertEqual(record["migration"], "migrated")
        self.assertEqual(record["state"], "active")
        self.assertEqual(record["before"],
                         {"resource_id": "r-a", "version": 1})
        self.assertEqual(record["after"],
                         {"resource_id": "r-b", "version": 1})
        self._expected_settlements["set-migrate"] = record
        self._assert_idempotent(
            lambda: settle(*self._nine(), "j-migrate", "mig-start",
                           "set-migrate", 70),
            lambda: settle(*self._nine(), "j-migrate", "mig-start",
                           "set-migrate", 71),
            expected=record)
        binding = current(*self._nine(), "j-migrate")
        self.assertEqual((binding["resource_id"], binding["version"],
                          binding["generation"], binding["state"]),
                         ("r-b", 1, 1, "active"))
        # The settlement moves no clearing-view occupancy: the trade
        # stays booked until the completion releases it.
        self._checkpoint(
            ("r-a", 1), 70,
            expected_sold={69: 15, 70: 15, 71: 15},
            expected_effective={
                69: {("r-a", 1): 13, ("r-b", 1): 2},
                70: {("r-a", 1): 13, ("r-b", 1): 2},
                71: {("r-a", 1): 13, ("r-b", 1): 2}})

        # -- t=75: the failed migration settles as a compensation --------
        record, created = settle(*self._nine(), "j-comp", "comp-start",
                                 "set-comp", 75)
        self.assertTrue(created)
        self.assertEqual(record["migration"], "failed")
        self.assertEqual(record["state"], "compensated")
        self.assertEqual(record["after"],
                         {"resource_id": "r-a", "version": 1})
        self._expected_settlements["set-comp"] = record
        binding = current(*self._nine(), "j-comp")
        self.assertEqual((binding["resource_id"], binding["version"],
                          binding["generation"], binding["state"]),
                         ("r-a", 1, 1, "compensated"))
        self._checkpoint(
            ("r-a", 1), 75,
            expected_sold={74: 15, 75: 15, 76: 15},
            expected_effective={
                74: {("r-a", 1): 13, ("r-b", 1): 2},
                75: {("r-a", 1): 13, ("r-b", 1): 2},
                76: {("r-a", 1): 13, ("r-b", 1): 2}})
        # The compensation released j-comp's target reservation, but the
        # settled migration still holds two units of r-b: a four-unit
        # reservation remains infeasible.
        advice, created = evaluate(*self._seven(), "j-late", "adv-late-2",
                                   76)
        self.assertTrue(created)
        with self.assertRaises(LookupError):
            apply(*self._eight(), "j-late", "adv-late-2", "res-late-2", 76)

        # -- t=85: the compensated job completes on its source ----------
        self._finish_job("j-comp", "comp", "owner-comp", 76)
        record = self._complete("j-comp", "x-comp", 85)
        self.assertEqual(record["generation"], 1)
        self.assertEqual(record["current"],
                         {"resource_id": "r-a", "version": 1})
        self._checkpoint(
            ("r-a", 1), 85,
            expected_sold={84: 15, 85: 13, 86: 13},
            expected_effective={
                84: {("r-a", 1): 13, ("r-b", 1): 2},
                85: {("r-a", 1): 11, ("r-b", 1): 2},
                86: {("r-a", 1): 11, ("r-b", 1): 2}},
            release=2)

        # -- t=90: the migrated job completes on its target -------------
        self._finish_job("j-migrate", "migrate", "owner-migrate", 80)
        record = self._complete("j-migrate", "x-migrate", 90)
        self.assertEqual(record["generation"], 1)
        self.assertEqual(record["current"],
                         {"resource_id": "r-b", "version": 1})
        self._checkpoint(
            ("r-a", 1), 90,
            expected_sold={89: 15, 90: 13, 91: 13},
            expected_effective={
                89: {("r-a", 1): 13, ("r-b", 1): 2},
                90: {("r-a", 1): 13},
                91: {("r-a", 1): 13}},
            release=2)

        # -- t=93..95: the completion released binding and reservation --
        jobs_module.submit(
            self.jobs,
            _job("j-late2", 4, regions=("eu-north", "us-west")), "jk-late2")
        self._trade("j-late2", "t-late2", 93)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-late2", "d-late2", 93)
        advice, created = evaluate(*self._seven(), "j-late2", "adv-late2",
                                   94, completions=self.completions)
        self.assertTrue(created)
        self.assertEqual(advice["target"],
                         {"resource_id": "r-b", "version": 1})
        # r-b is fully free again: the completion of j-migrate released
        # its current binding and its historical migration reservation.
        intent, created = apply(*self._eight(), "j-late2", "adv-late2",
                                "res-late2", 95, completions=self.completions)
        self.assertTrue(created)
        self.assertEqual(intent["target"],
                         {"resource_id": "r-b", "version": 1})

        # -- t=110..130: a trade on the second resource version ----------
        trade = self._trade("j-v2", "t-v2", 110)
        self.assertEqual((trade["resource_id"], trade["version"]),
                         ("r-a", 2))
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-v2", "d-v2", 111)
        self._finish_job("j-v2", "v2", "owner-v2", 112)
        record = self._complete("j-v2", "x-v2", 130)
        self.assertEqual(record["current"],
                         {"resource_id": "r-a", "version": 2})
        self._checkpoint(
            ("r-a", 2), 130,
            expected_sold={129: 4, 130: 0, 131: 0},
            expected_effective={
                129: {("r-a", 1): 19, ("r-a", 2): 4, ("r-b", 1): 4},
                130: {("r-a", 1): 19, ("r-b", 1): 4},
                131: {("r-a", 1): 19, ("r-b", 1): 4}},
            release=4)
        # The first version's occupancy is untouched by the second
        # version's lifecycle.
        records = self._public_records()
        self.assertEqual(
            self._clear_view(131, records).get(("r-a", 1), 0), 19)

        # -- final state: re-read everything through the public queries --
        records = self._public_records()
        sold = self._clear_view(200, records)
        self.assertEqual(sold, {("r-a", 1): 19, ("r-a", 2): 4})
        self._assert_within_capacity(sold, "clear")
        effective = self._effective_view(200, records)
        self.assertEqual(effective,
                         {("r-a", 1): 19, ("r-a", 2): 4, ("r-b", 1): 4})
        self._assert_within_capacity(effective, "effective")
        self._assert_history_unchanged()
        # The still-running jobs (the probes, j-late and the reserved
        # j-late2) cannot be evicted: one unit more than the remaining
        # capacity of the active version is infeasible.
        self._probe(200, 3)
        # Without the optional ledgers every booking still occupies its
        # version: not a single unit of the active version is free.
        self._probe(200, 1, completions=False, cancellations=False)

        page = completion_module.search(self.completions)
        self.assertEqual([entry["job_id"] for entry in page["entries"]],
                         ["j-comp", "j-migrate", "j-plain", "j-v2"])
        self.assertIsNone(page["next"])
        for job_id in ("j-cancel", "j-plain", "j-migrate", "j-comp",
                       "j-v2", "j-late", "j-late2"):
            self.assertEqual(jobs_module.get(self.jobs, job_id)["state"],
                             "queued")
        self.assertEqual(
            resources_module.get(self.supply, "r-a", 1)["capacity"], 30)
        self.assertEqual(
            resources_module.get(self.supply, "r-a", 2)["capacity"], 6)
        self.assertEqual(
            resources_module.get(self.supply, "r-b")["capacity"], 5)
        self.assertEqual(
            signals_module.get(self.signals, "us-west", 200)["version"], 1)
        binding = current(*self._nine(), "j-late2")
        self.assertEqual((binding["resource_id"], binding["version"],
                          binding["generation"], binding["state"]),
                         ("r-a", 1, 0, "traded"))


class ConcurrentCapacityTest(unittest.TestCase):
    """Races over cancellation, first dispatch and the last capacity
    unit must preserve the same capacity invariants."""

    def _market(self, tmp: str, capacity: int = 100):
        paths = {name: os.path.join(tmp, f"{name}.json") for name in
                 ("jobs", "supply", "trades", "dispatch", "cancellations")}
        resources_module.publish(
            paths["supply"],
            _resource("r-1", capacity=capacity), "rk-1")
        return paths

    def test_concurrent_cancel_and_first_commit(self) -> None:
        for iteration in range(10):
            with self.subTest(iteration=iteration):
                tmp = TemporaryDirectory()
                self.addCleanup(tmp.cleanup)
                paths = self._market(tmp.name)
                jobs_module.submit(paths["jobs"], _job("j-1", 10), "jk-1")
                jobs_module.submit(paths["jobs"], _job("j-0", 10), "jk-0")
                clear(paths["jobs"], paths["supply"], paths["trades"],
                      "j-1", "t-1", 10)
                clear(paths["jobs"], paths["supply"], paths["trades"],
                      "j-0", "t-0", 10)
                # An unrelated first cancellation brings the ledger into
                # existence, so the racing commit always reads it.
                cancel(paths["jobs"], paths["supply"], paths["trades"],
                       paths["dispatch"], paths["cancellations"],
                       "j-0", "c-0", 15, "seed")

                barrier = threading.Barrier(2)
                outcome: dict[str, object] = {}

                def run_cancel() -> None:
                    barrier.wait()
                    try:
                        record, created = cancel(
                            paths["jobs"], paths["supply"], paths["trades"],
                            paths["dispatch"], paths["cancellations"],
                            "j-1", "c-1", 20, "race")
                        outcome["cancel"] = (record, created)
                    except (ValueError, PermissionError) as exc:
                        outcome["cancel"] = exc

                def run_commit() -> None:
                    barrier.wait()
                    try:
                        decision, created = dispatch_module.commit(
                            paths["jobs"], paths["supply"], paths["trades"],
                            paths["dispatch"], "j-1", "d-1", 20,
                            cancellations=paths["cancellations"])
                        outcome["commit"] = (decision, created)
                    except (ValueError, PermissionError) as exc:
                        outcome["commit"] = exc

                threads = (threading.Thread(target=run_cancel),
                           threading.Thread(target=run_commit))
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(30)
                for thread in threads:
                    self.assertFalse(thread.is_alive())

                cancelled = isinstance(outcome["cancel"], tuple) \
                    and outcome["cancel"][1]
                committed = isinstance(outcome["commit"], tuple) \
                    and outcome["commit"][1]
                # Exactly one first write succeeds; the loser is refused.
                self.assertNotEqual(cancelled, committed)
                dispatch_raw = self._read_json_static(paths["dispatch"])
                decisions = (dispatch_raw or {}).get("decisions", {})
                if cancelled:
                    self.assertIsInstance(outcome["commit"], ValueError)
                    self.assertEqual(
                        cancellation_module.get(paths["cancellations"],
                                                "j-1")["job_id"], "j-1")
                    self.assertNotIn("j-1", decisions)
                else:
                    self.assertIsInstance(outcome["cancel"], PermissionError)
                    with self.assertRaises(KeyError):
                        cancellation_module.get(paths["cancellations"], "j-1")
                    self.assertIn("j-1", decisions)
                # The capacity accounting is intact either way: the two
                # trades never exceed the published capacity.
                trades_raw = self._read_json_static(paths["trades"])
                sold = sum(record["work"]
                           for record in trades_raw["trades"].values())
                self.assertLessEqual(sold, 100)

    def test_concurrent_clear_never_oversells_last_unit(self) -> None:
        for iteration in range(5):
            with self.subTest(iteration=iteration):
                tmp = TemporaryDirectory()
                self.addCleanup(tmp.cleanup)
                paths = self._market(tmp.name, capacity=2)
                for index in range(8):
                    jobs_module.submit(paths["jobs"],
                                       _job(f"j-{index}", 1), f"jk-{index}")

                barrier = threading.Barrier(8)
                outcomes: dict[str, object] = {}
                lock = threading.Lock()

                def run_clear(index: int) -> None:
                    barrier.wait()
                    try:
                        trade, created = clear(
                            paths["jobs"], paths["supply"], paths["trades"],
                            f"j-{index}", f"t-{index}", 10)
                        with lock:
                            outcomes[f"j-{index}"] = (trade, created)
                    except LookupError as exc:
                        with lock:
                            outcomes[f"j-{index}"] = exc

                threads = [threading.Thread(target=run_clear, args=(index,))
                           for index in range(8)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(30)
                for thread in threads:
                    self.assertFalse(thread.is_alive())

                winners = [job_id for job_id, outcome in outcomes.items()
                           if isinstance(outcome, tuple) and outcome[1]]
                losers = [outcome for outcome in outcomes.values()
                          if isinstance(outcome, LookupError)]
                # Two units of capacity, eight contenders: exactly two
                # trades succeed, the rest are refused, and the ledger
                # never sells more than was published.
                self.assertEqual(len(winners), 2)
                self.assertEqual(len(losers), 6)
                trades_raw = self._read_json_static(paths["trades"])
                self.assertEqual(sorted(trades_raw["trades"]),
                                 sorted(winners))
                sold = sum(record["work"]
                           for record in trades_raw["trades"].values())
                self.assertEqual(sold, 2)
                # A replay of a winning key returns the stored trade
                # without creating anything new.
                trade, created = clear(paths["jobs"], paths["supply"],
                                       paths["trades"], winners[0],
                                       f"t-{winners[0][2:]}", 10)
                self.assertFalse(created)
                self.assertEqual(trade["resource_id"], "r-1")
                # The capacity is still exactly exhausted afterwards.
                jobs_module.submit(paths["jobs"], _job("j-late", 1),
                                   "jk-late")
                with self.assertRaises(LookupError):
                    clear(paths["jobs"], paths["supply"], paths["trades"],
                          "j-late", "t-late", 11)

    @staticmethod
    def _read_json_static(path: str) -> dict[str, object] | None:
        if not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)


if __name__ == "__main__":
    unittest.main()
