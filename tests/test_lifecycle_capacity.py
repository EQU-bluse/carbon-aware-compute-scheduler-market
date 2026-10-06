"""Lifecycle capacity-conservation tests over the public entry points.

These tests drive whole job lifecycles from an empty temporary
directory along one deterministic timeline -- publish capacity, trade
several jobs, then walk them into the four terminal states (cancelled
before dispatch, completed without migration, completed after a
successful migration and completed after a compensated migration) --
and verify that capacity is conserved at every step, using only the
public functions and their persisted results:

* ``jobs.submit``/``jobs.get``, ``resources.publish``/``resources.get``
  and ``signals.publish``/``signals.get`` for the inputs,
* ``market.clear``/``market.clear_live`` for the trades,
* ``dispatch.commit``/``claim`` and ``execution.plan``/``record`` with
  ``execution_sync.run`` for the execution lifecycle,
* ``rebalance.evaluate``/``apply``/``start``/``record``/``settle``/
  ``current`` for migration and the resource binding,
* ``cancellation.cancel``/``get`` and ``completion.complete``/``get``
  for the terminal releases.

The expected occupancy of every resource version is recomputed at each
checkpoint from the input capacities and the persisted public ledgers
alone: a trade books its work on its exact resource version, a
cancellation or completion whose moment is not later than the
checkpoint releases that booking, and nothing else moves capacity in
the clearing view. Around every cancellation, settlement and
completion moment the tests re-clear one moment before, at and after
the event: a probe job asking for one unit more than the expected
remaining capacity must always fail with ``LookupError``, and --
whenever the stricter downstream envelope (which never observes
cancellations) allows it -- a probe asking for exactly the expected
remainder must win the trade, proving released capacity is genuinely
reusable while running or not-yet-released jobs are never evicted
early. Migration bindings are verified through ``rebalance.current``
and the settlement records: a migrated settlement moves the occupancy
to the target version, a compensated one keeps it on the source, and a
completion releases the current binding together with the historical
migration reservation (a new ``rebalance.apply`` can reserve the freed
target capacity again). No product interface is added and no
parameter, return shape, exception type or ledger format is changed.
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
from carbon_market import rebalance as rebalance_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.market import clear, clear_live


def _resource(resource_id: str, region: str,
              **overrides: object) -> dict[str, object]:
    resource: dict[str, object] = {
        "resource_id": resource_id,
        "region": region,
        "capacity": 10,
        "start": 0,
        "end": 1000,
        "unit_cost": 5,
        "carbon_intensity": 8,
        "residency": [region],
    }
    resource.update(overrides)
    return resource


def _signal(region: str, **overrides: object) -> dict[str, object]:
    signal: dict[str, object] = {
        "region": region,
        "observed": 0,
        "expires": 1000,
        "mix": {"solar": 10000},
        "unit_cost": 5,
        "carbon_intensity": 8,
    }
    signal.update(overrides)
    return signal


def _job(job_id: str, **overrides: object) -> dict[str, object]:
    job: dict[str, object] = {
        "job_id": job_id,
        "work": 1,
        "deadline": 1000,
        "regions": ["eu-north"],
        "residency": ["eu-north"],
        "max_cost": 1000,
        "carbon_cap": 1000,
    }
    job.update(overrides)
    return job


def _read_json(path: str) -> dict[str, object]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


class LifecycleCapacityTest(unittest.TestCase):
    """Capacity conservation across one deterministic lifecycle timeline."""

    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        base = self.tmp.name
        self.base = base
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
        # Records observed so far per ledger section; every later check
        # must find them byte-identical in the persisted document.
        self._frozen: dict[str, dict[str, object]] = {}

    # -- path bundles ---------------------------------------------------------

    def _eight(self) -> tuple[str, ...]:
        return (self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, self.intents)

    def _nine(self) -> tuple[str, ...]:
        return self._eight() + (self.settlements,)

    # -- independent capacity model from public records ------------------------

    def _occupancy(self, at: int, use_completions: bool = True,
                   use_cancellations: bool = True
                   ) -> dict[tuple[str, int], int]:
        """Expected occupancy per exact resource version at ``at``.

        Recomputed only from the persisted public ledgers: every trade
        books its work on its frozen resource version, and a job whose
        cancellation or completion moment is not later than ``at`` has
        released that booking. Migration settlements never rewrite the
        clearing view -- the trade's booking stays on its version until
        the job reaches a terminal release.
        """
        trades = _read_json(self.trades)["trades"] \
            if os.path.exists(self.trades) else {}
        released: set[str] = set()
        if use_cancellations and os.path.exists(self.cancellations):
            for record in _read_json(self.cancellations)["cancellations"].values():
                if record["at"] <= at:
                    released.add(record["job_id"])
        if use_completions and os.path.exists(self.completions):
            for record in _read_json(self.completions)["completions"].values():
                if record["at"] <= at:
                    released.add(record["job_id"])
        occupancy: dict[tuple[str, int], int] = {}
        for job_id, trade in trades.items():
            if job_id in released:
                continue
            slot = (trade["resource_id"], trade["version"])
            occupancy[slot] = occupancy.get(slot, 0) + trade["work"]
        return occupancy

    def _active_record(self, resource_id: str, at: int) -> dict[str, object]:
        history = _read_json(self.supply)["history"]
        active = None
        for record in history[resource_id]:
            if record["start"] <= at <= record["end"]:
                active = record
        self.assertIsNotNone(active,
                             f"{resource_id} has no active version at {at}")
        return active

    def _expected_remaining(self, resource_id: str, at: int,
                            use_completions: bool = True,
                            use_cancellations: bool = True) -> int:
        active = self._active_record(resource_id, at)
        used = self._occupancy(at, use_completions, use_cancellations).get(
            (resource_id, active["version"]), 0)
        return active["capacity"] - used

    def _assert_capacity_invariant(self, at: int) -> None:
        """No version's cumulative occupancy is negative or oversold."""
        occupancy = self._occupancy(at)
        history = _read_json(self.supply)["history"]
        known = {(resource_id, record["version"])
                 for resource_id, records in history.items()
                 for record in records}
        for slot in occupancy:
            self.assertIn(slot, known)
        for resource_id, records in history.items():
            for record in records:
                used = occupancy.get((resource_id, record["version"]), 0)
                self.assertGreaterEqual(used, 0)
                self.assertLessEqual(
                    used, record["capacity"],
                    f"{resource_id} v{record['version']} oversold at {at}: "
                    f"{used} > {record['capacity']}")

    def _assert_records_frozen(self) -> None:
        """Old trade, cancellation, completion and settlement records
        must survive every later operation unchanged."""
        sections = ((self.trades, "trades"),
                    (self.cancellations, "cancellations"),
                    (self.completions, "completions"),
                    (self.settlements, "records"))
        for path, section in sections:
            if not os.path.exists(path):
                continue
            records = _read_json(path)[section]
            known = self._frozen.setdefault(path, {})
            for key, record in records.items():
                if key in known:
                    self.assertEqual(known[key], record,
                                     f"{path} record {key!r} was rewritten")
            known.update(records)

    # -- probe jobs ------------------------------------------------------------

    def _submit_probe(self, region: str, work: int) -> str:
        self._probe_seq += 1
        job_id = f"j-probe-{self._probe_seq}"
        record, created = jobs_module.submit(
            self.jobs,
            _job(job_id, work=work, regions=[region], residency=[region]),
            f"pjk-{self._probe_seq}")
        self.assertTrue(created)
        self.assertEqual(record["state"], "queued")
        return job_id

    def _assert_capacity_gate(self, resource_id: str, region: str, at: int,
                              book: int | None = None) -> None:
        """Re-clear at ``at``: one unit above the expected remainder must
        fail, and exactly the remainder (or ``book`` units) must clear."""
        remaining = self._expected_remaining(resource_id, at)
        with self.subTest(resource=resource_id, at=at, remaining=remaining):
            overflow_id = self._submit_probe(region, remaining + 1)
            with self.assertRaises(LookupError):
                clear(self.jobs, self.supply, self.trades, overflow_id,
                      f"ovf-{overflow_id}", at, completions=self.completions,
                      cancellations=self.cancellations)
            if remaining > 0:
                job_id = self._submit_probe(region, book or remaining)
                trade, created = clear(
                    self.jobs, self.supply, self.trades, job_id,
                    f"ok-{job_id}", at, completions=self.completions,
                    cancellations=self.cancellations)
                self.assertTrue(created)
                self.assertEqual(trade["resource_id"], resource_id)
                self.assertEqual(trade["version"],
                                 self._active_record(resource_id, at)["version"])
            self._assert_capacity_invariant(at)

    # -- lifecycle drivers ------------------------------------------------------

    def _launch_completed(self, job_id: str, owner: str, claim_at: int,
                          plan_at: int, stage_at: int, start_at: int) -> None:
        dispatch_module.claim(self.dispatch, job_id, f"cl-{job_id}",
                              owner, 100, claim_at)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, job_id,
                              f"pl-{job_id}", owner, None, plan_at)
        execution_module.record(self.execution, job_id, 1, f"er-{job_id}-1",
                                owner, "stage", "succeeded", "staged",
                                stage_at)
        execution_module.record(self.execution, job_id, 1, f"er-{job_id}-2",
                                owner, "start", "succeeded", "started",
                                start_at)

    def _migrate_started(self, job_id: str, owner: str, lease_end: int,
                         evaluate_at: int, apply_at: int,
                         start_at: int) -> None:
        record, created = rebalance_module.evaluate(
            self.jobs, self.supply, self.signals, self.trades, self.dispatch,
            self.execution, self.advice, job_id, f"ev-{job_id}", evaluate_at)
        self.assertTrue(created)
        self.assertEqual(record["recommendation"], "migrate")
        self.assertEqual(record["target"],
                         {"resource_id": "r-beta", "version": 1})
        intent, created = rebalance_module.apply(
            *self._eight(), job_id, f"ev-{job_id}", f"ap-{job_id}", apply_at)
        self.assertTrue(created)
        self.assertEqual(intent["reserved"], "reserved")
        self.assertEqual(intent["target"],
                         {"resource_id": "r-beta", "version": 1})
        plan, created = rebalance_module.start(
            *self._eight(), job_id, f"st-{job_id}", owner, lease_end,
            start_at)
        self.assertTrue(created)
        self.assertEqual(plan["state"], "active")

    # -- the timeline ------------------------------------------------------------

    def _setup_world(self) -> None:
        # r-alpha: the contended resource the four terminal jobs share.
        # r-beta: the migration target. r-gamma: two versions of one
        # resource, hosting the later migrating jobs on its second
        # version. r-boot: an isolated bootstrap resource whose only
        # purpose is to bring the optional ledgers into existence.
        resources_module.publish(
            self.supply,
            _resource("r-alpha", "eu-north", capacity=5, unit_cost=5,
                      carbon_intensity=8), "rk-alpha")
        resources_module.publish(
            self.supply,
            _resource("r-beta", "us-west", capacity=3, unit_cost=3,
                      carbon_intensity=2,
                      residency=["eu-north", "eu-south", "us-west"]),
            "rk-beta")
        resources_module.publish(
            self.supply,
            _resource("r-gamma", "eu-south", capacity=2, unit_cost=5,
                      carbon_intensity=9), "rk-gamma-1")
        resources_module.publish(
            self.supply,
            _resource("r-gamma", "eu-south", capacity=4, start=200,
                      unit_cost=5, carbon_intensity=9), "rk-gamma-2")
        resources_module.publish(
            self.supply,
            _resource("r-boot", "zz-boot", capacity=100, unit_cost=1,
                      carbon_intensity=1), "rk-boot")
        signals_module.publish(
            self.signals,
            _signal("eu-north", unit_cost=5, carbon_intensity=1), "sk-north")
        signals_module.publish(
            self.signals,
            _signal("us-west", unit_cost=3, carbon_intensity=8), "sk-west-1")
        signals_module.publish(
            self.signals,
            _signal("us-west", observed=300, unit_cost=3, carbon_intensity=0),
            "sk-west-2")
        signals_module.publish(
            self.signals,
            _signal("eu-south", unit_cost=5, carbon_intensity=1), "sk-south")
        signals_module.publish(
            self.signals,
            _signal("zz-boot", unit_cost=1, carbon_intensity=1), "sk-boot")
        jobs_module.submit(self.jobs, _job("j-cancel"), "jk-cancel")
        jobs_module.submit(self.jobs, _job("j-plain"), "jk-plain")
        for job_id in ("j-mig-ok", "j-mig-fail"):
            jobs_module.submit(
                self.jobs,
                _job(job_id, regions=["eu-north", "us-west"]),
                f"jk-{job_id}")
        for job_id in ("j-mig-b", "j-mig-c", "j-mig-d"):
            jobs_module.submit(
                self.jobs,
                _job(job_id, regions=["eu-south", "us-west"],
                     residency=["eu-south"]),
                f"jk-{job_id}")
        jobs_module.submit(
            self.jobs,
            _job("j-gamma-early", work=2, regions=["eu-south"],
                 residency=["eu-south"]), "jk-gamma-early")
        jobs_module.submit(
            self.jobs,
            _job("j-boot-ok", regions=["zz-boot"], residency=["zz-boot"]),
            "jk-boot-ok")
        jobs_module.submit(
            self.jobs,
            _job("j-boot-stop", regions=["zz-boot"], residency=["zz-boot"]),
            "jk-boot-stop")

    def _bootstrap_ledgers(self) -> None:
        # One isolated job completes and another is cancelled on r-boot,
        # so the completion and cancellation ledgers exist before the
        # first measured event and can always be passed explicitly.
        clear(self.jobs, self.supply, self.trades, "j-boot-ok",
              "t-boot-ok", 5)
        clear(self.jobs, self.supply, self.trades, "j-boot-stop",
              "t-boot-stop", 5)
        cancellation_module.cancel(self.jobs, self.supply, self.trades,
                                   self.dispatch, self.cancellations,
                                   "j-boot-stop", "x-boot-stop", 6, "boot")
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-boot-ok", "d-boot-ok", 6)
        self._launch_completed("j-boot-ok", "owner-boot", 7, 8, 9, 10)
        execution_sync_module.run(self.execution, self.dispatch, self.sync,
                                  "owner-sync", "sync-boot", 11, 50)
        record, created = completion_module.complete(
            self.jobs, self.supply, self.signals, self.trades, self.dispatch,
            self.execution, self.completions, "j-boot-ok", "co-boot-ok", 12,
            "succeeded", 5, 5)
        self.assertTrue(created)
        self.assertEqual(record["generation"], 0)

    def test_lifecycle_capacity_conservation(self) -> None:
        self._setup_world()
        self._bootstrap_ledgers()

        # -- t=10: several jobs trade; r-alpha is booked 4/5, r-gamma
        # v1 is booked 2/2 by one job with work 2.
        for job_id in ("j-cancel", "j-plain"):
            trade, created = clear(self.jobs, self.supply, self.trades,
                                   job_id, f"t-{job_id}", 10)
            self.assertTrue(created)
            self.assertEqual(trade["selection"],
                             {"resource_id": "r-alpha", "version": 1})
        for job_id in ("j-mig-ok", "j-mig-fail"):
            trade, created = clear_live(self.jobs, self.supply, self.signals,
                                        self.trades, job_id, f"t-{job_id}",
                                        10)
            self.assertTrue(created)
            self.assertEqual(trade["selection"],
                             {"resource_id": "r-alpha", "version": 1})
        trade, created = clear(self.jobs, self.supply, self.trades,
                               "j-gamma-early", "t-gamma-early", 10)
        self.assertTrue(created)
        self.assertEqual(trade["selection"],
                         {"resource_id": "r-gamma", "version": 1})
        self.assertEqual(self._expected_remaining("r-alpha", 10), 1)
        self.assertEqual(self._expected_remaining("r-gamma", 10), 0)
        self._assert_capacity_invariant(10)
        self._assert_records_frozen()

        # -- t=15..60: j-plain enters dispatch and runs its launch plan
        # (still occupying r-alpha the whole time); the two migration
        # candidates only commit a decision.
        for job_id in ("j-plain", "j-mig-ok", "j-mig-fail"):
            decision, created = dispatch_module.commit(
                self.jobs, self.supply, self.trades, self.dispatch, job_id,
                f"d-{job_id}", 15)
            self.assertTrue(created)
            self.assertEqual(decision["state"], "ready")
        self._launch_completed("j-plain", "owner-1", 20, 30, 40, 50)
        execution_sync_module.run(self.execution, self.dispatch, self.sync,
                                  "owner-sync", "sync-plain", 60, 50)

        # -- t=100: j-plain is mid-execution. Its capacity is not
        # evicted early: exactly the one never-booked unit is available.
        self.assertEqual(self._expected_remaining("r-alpha", 100), 1)
        self._assert_capacity_gate("r-alpha", "eu-north", 100)
        self.assertEqual(self._expected_remaining("r-alpha", 119), 0)
        self._assert_capacity_gate("r-alpha", "eu-north", 119)

        # -- t=120: j-plain completes without any migration and
        # releases its booked version at the completion moment.
        record, created = completion_module.complete(
            self.jobs, self.supply, self.signals, self.trades, self.dispatch,
            self.execution, self.completions, "j-plain", "co-plain", 120,
            "succeeded", 90, 120)
        self.assertTrue(created)
        self.assertEqual(record["generation"], 0)
        self.assertEqual(record["current"],
                         {"resource_id": "r-alpha", "version": 1})
        self.assertFalse(record["cost_exceeded"])
        self.assertFalse(record["carbon_exceeded"])
        self.assertEqual(self._expected_remaining("r-alpha", 120), 1)
        self._assert_capacity_gate("r-alpha", "eu-north", 120)
        self.assertEqual(self._expected_remaining("r-alpha", 121), 0)
        self._assert_capacity_gate("r-alpha", "eu-north", 121)
        self._assert_records_frozen()

        # -- r-gamma: version 1 stays fully booked by j-gamma-early
        # while version 2 (active from t=200) offers fresh capacity;
        # bookings never cross versions.
        self.assertEqual(self._expected_remaining("r-gamma", 150), 0)
        self._assert_capacity_gate("r-gamma", "eu-south", 150)
        self._assert_capacity_gate("r-gamma", "eu-south", 199)
        self.assertEqual(self._expected_remaining("r-gamma", 200), 4)
        # Book one unit of v2 so the later trades leave it exactly full.
        self._assert_capacity_gate("r-gamma", "eu-south", 200, book=1)
        self._assert_records_frozen()

        # -- t=250: three more jobs trade onto r-gamma's second
        # version, filling it (3 trades + the probe = 4/4).
        for job_id in ("j-mig-b", "j-mig-c", "j-mig-d"):
            trade, created = clear_live(self.jobs, self.supply, self.signals,
                                        self.trades, job_id, f"t-{job_id}",
                                        250)
            self.assertTrue(created)
            self.assertEqual(trade["selection"],
                             {"resource_id": "r-gamma", "version": 2})
        self.assertEqual(self._expected_remaining("r-gamma", 251), 0)
        self._assert_capacity_gate("r-gamma", "eu-south", 251)
        for job_id in ("j-mig-b", "j-mig-c", "j-mig-d"):
            dispatch_module.commit(self.jobs, self.supply, self.trades,
                                   self.dispatch, job_id, f"d-{job_id}", 255)
        self._assert_capacity_invariant(255)
        self._assert_records_frozen()

        # -- t=310..350: the us-west signal turns green; every
        # migration candidate is advised onto r-beta and reserves it.
        # r-beta (capacity 3) fills exactly; a fourth reservation is
        # refused until the failed migration releases its target hold.
        self._migrate_started("j-mig-ok", "owner-1", 900, 310, 320, 330)
        self._migrate_started("j-mig-fail", "owner-1", 900, 310, 320, 330)
        self._migrate_started("j-mig-b", "owner-1", 900, 310, 320, 330)
        for job_id in ("j-mig-c", "j-mig-d"):
            record, created = rebalance_module.evaluate(
                self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, job_id,
                f"ev-{job_id}", 310)
            self.assertTrue(created)
            self.assertEqual(record["recommendation"], "migrate")
        # r-beta now holds three reservations: j-mig-c cannot reserve.
        with self.assertRaises(LookupError):
            rebalance_module.apply(*self._eight(), "j-mig-c", "ev-j-mig-c",
                                   "ap-j-mig-c", 325)
        rebalance_module.record(*self._eight(), "j-mig-fail", "rc-j-mig-fail-1",
                                "owner-1", "copy", "failed", "boom", 340)
        rebalance_module.record(*self._eight(), "j-mig-ok", "rc-j-mig-ok-1",
                                "owner-1", "copy", "succeeded", "copied", 340)
        rebalance_module.record(*self._eight(), "j-mig-b", "rc-j-mig-b-1",
                                "owner-1", "copy", "succeeded", "copied", 340)
        # The failed step released j-mig-fail's target reservation, so
        # j-mig-c's retry reserves the freed unit.
        intent, created = rebalance_module.apply(
            *self._eight(), "j-mig-c", "ev-j-mig-c", "ap-j-mig-c-2", 345)
        self.assertTrue(created)
        self.assertEqual(intent["reserved"], "reserved")
        self.assertEqual(intent["source"],
                         {"resource_id": "r-gamma", "version": 2})
        rebalance_module.record(*self._eight(), "j-mig-ok", "rc-j-mig-ok-2",
                                "owner-1", "switch", "succeeded", "switched",
                                350)
        rebalance_module.record(*self._eight(), "j-mig-b", "rc-j-mig-b-2",
                                "owner-1", "switch", "succeeded", "switched",
                                350)
        plan, created = rebalance_module.start(
            *self._eight(), "j-mig-c", "st-j-mig-c", "owner-1", 900, 350)
        self.assertTrue(created)
        self._assert_capacity_invariant(350)
        self._assert_records_frozen()

        # -- t=360: the settlements land. A migrated plan moves the
        # binding to the target version; the failed plan compensates
        # and stays on the source. The clearing view is unchanged:
        # settlements alone release nothing there.
        self.assertEqual(self._expected_remaining("r-alpha", 359), 0)
        self._assert_capacity_gate("r-alpha", "eu-north", 359)
        settled_ok, created = rebalance_module.settle(
            *self._nine(), "j-mig-ok", "st-j-mig-ok", "se-j-mig-ok", 360)
        self.assertTrue(created)
        self.assertEqual(settled_ok["migration"], "migrated")
        self.assertEqual(settled_ok["state"], "active")
        self.assertEqual(settled_ok["generation"], 1)
        self.assertEqual(settled_ok["before"],
                         {"resource_id": "r-alpha", "version": 1})
        self.assertEqual(settled_ok["after"],
                         {"resource_id": "r-beta", "version": 1})
        settled_fail, created = rebalance_module.settle(
            *self._nine(), "j-mig-fail", "st-j-mig-fail", "se-j-mig-fail",
            360)
        self.assertTrue(created)
        self.assertEqual(settled_fail["migration"], "failed")
        self.assertEqual(settled_fail["state"], "compensated")
        self.assertEqual(settled_fail["before"], settled_fail["after"])
        self.assertEqual(settled_fail["after"],
                         {"resource_id": "r-alpha", "version": 1})
        rebalance_module.record(*self._eight(), "j-mig-c", "rc-j-mig-c-1",
                                "owner-1", "copy", "succeeded", "copied", 360)
        self.assertEqual(self._expected_remaining("r-alpha", 360), 0)
        self._assert_capacity_gate("r-alpha", "eu-north", 360)
        self._assert_capacity_gate("r-alpha", "eu-north", 361)
        # The public binding query shows the occupancy move.
        binding = rebalance_module.current(*self._nine(), "j-mig-ok")
        self.assertEqual(binding, {"job_id": "j-mig-ok",
                                   "resource_id": "r-beta", "version": 1,
                                   "generation": 1, "state": "active",
                                   "at": 360})
        binding = rebalance_module.current(*self._nine(), "j-mig-fail")
        self.assertEqual(binding, {"job_id": "j-mig-fail",
                                   "resource_id": "r-alpha", "version": 1,
                                   "generation": 1, "state": "compensated",
                                   "at": 360})
        binding = rebalance_module.current(*self._nine(), "j-plain")
        self.assertEqual(binding["state"], "traded")
        self.assertEqual(binding["resource_id"], "r-alpha")
        self._assert_records_frozen()

        # -- t=370..380: the remaining migrations terminate and
        # settle; migrated occupants keep holding the target version,
        # so j-mig-d still cannot reserve r-beta.
        rebalance_module.record(*self._eight(), "j-mig-c", "rc-j-mig-c-2",
                                "owner-1", "switch", "succeeded", "switched",
                                370)
        rebalance_module.settle(*self._nine(), "j-mig-b", "st-j-mig-b",
                                "se-j-mig-b", 370)
        with self.assertRaises(LookupError):
            rebalance_module.apply(*self._eight(), "j-mig-d", "ev-j-mig-d",
                                   "ap-j-mig-d", 370)
        rebalance_module.settle(*self._nine(), "j-mig-c", "st-j-mig-c",
                                "se-j-mig-c", 380)
        for job_id in ("j-mig-b", "j-mig-c"):
            binding = rebalance_module.current(*self._nine(), job_id)
            self.assertEqual(binding["resource_id"], "r-beta")
            self.assertEqual(binding["state"], "active")
        self._assert_records_frozen()

        # -- t=390..425: both terminal-state migration jobs run and
        # finish their launch plans.
        for job_id in ("j-mig-ok", "j-mig-fail"):
            self._launch_completed(job_id, "owner-1", 390, 400, 410, 420)
        execution_sync_module.run(self.execution, self.dispatch, self.sync,
                                  "owner-sync", "sync-main", 425, 50)

        # -- t=430: the compensated job completes; its release frees
        # the source version it kept occupying.
        self.assertEqual(self._expected_remaining("r-alpha", 429), 0)
        self._assert_capacity_gate("r-alpha", "eu-north", 429)
        record, created = completion_module.complete(
            self.jobs, self.supply, self.signals, self.trades, self.dispatch,
            self.execution, self.completions, "j-mig-fail", "co-j-mig-fail",
            430, "succeeded", 40, 50)
        self.assertTrue(created)
        self.assertEqual(record["generation"], 1)
        self.assertEqual(record["current"],
                         {"resource_id": "r-alpha", "version": 1})
        self.assertEqual(self._expected_remaining("r-alpha", 430), 1)
        self._assert_capacity_gate("r-alpha", "eu-north", 430)
        self.assertEqual(self._expected_remaining("r-alpha", 431), 0)
        self._assert_capacity_gate("r-alpha", "eu-north", 431)
        self._assert_records_frozen()

        # -- t=440: the migrated job completes; the release frees the
        # version its trade booked and, in the migration view, its
        # current binding on r-beta plus the historical reservation.
        self.assertEqual(self._expected_remaining("r-alpha", 439), 0)
        self._assert_capacity_gate("r-alpha", "eu-north", 439)
        record, created = completion_module.complete(
            self.jobs, self.supply, self.signals, self.trades, self.dispatch,
            self.execution, self.completions, "j-mig-ok", "co-j-mig-ok", 440,
            "succeeded", 30, 40)
        self.assertTrue(created)
        self.assertEqual(record["generation"], 1)
        self.assertEqual(record["current"],
                         {"resource_id": "r-beta", "version": 1})
        self.assertEqual(self._expected_remaining("r-alpha", 440), 1)
        self._assert_capacity_gate("r-alpha", "eu-north", 440)
        self.assertEqual(self._expected_remaining("r-alpha", 441), 0)
        self._assert_capacity_gate("r-alpha", "eu-north", 441)
        self._assert_records_frozen()

        # -- t=450: with the completion ledger in hand, j-mig-d can
        # finally reserve r-beta: j-mig-ok's completion released its
        # current binding and historical migration reservation there.
        intent, created = rebalance_module.apply(
            *self._eight(), "j-mig-d", "ev-j-mig-d", "ap-j-mig-d-2", 450,
            completions=self.completions)
        self.assertTrue(created)
        self.assertEqual(intent["reserved"], "reserved")
        self.assertEqual(intent["target"],
                         {"resource_id": "r-beta", "version": 1})
        self._assert_records_frozen()

        # -- t=459: omitting the optional ledgers preserves the
        # historical occupancy semantics exactly.
        self.assertEqual(self._expected_remaining("r-alpha", 459), 0)
        self._assert_capacity_gate("r-alpha", "eu-north", 459)
        for kwargs in ({"completions": self.completions},
                       {"cancellations": self.cancellations},
                       {}):
            with self.subTest(optional=set(kwargs)):
                probe_id = self._submit_probe("eu-north", 1)
                with self.assertRaises(LookupError):
                    clear(self.jobs, self.supply, self.trades, probe_id,
                          f"omit-{probe_id}", 459, **kwargs)

        # -- t=460: j-cancel is cancelled before any dispatch decision
        # and releases its booked version from the cancellation moment
        # on; the freed unit is immediately reusable by a new clearing.
        record, created = cancellation_module.cancel(
            self.jobs, self.supply, self.trades, self.dispatch,
            self.cancellations, "j-cancel", "x-cancel", 460, "user-request")
        self.assertTrue(created)
        self.assertEqual(record, {"job_id": "j-cancel", "at": 460,
                                  "reason": "user-request",
                                  "resource_id": "r-alpha", "version": 1})
        self.assertEqual(self._expected_remaining("r-alpha", 460), 1)
        self._assert_capacity_gate("r-alpha", "eu-north", 460)
        self.assertEqual(self._expected_remaining("r-alpha", 461), 0)
        self._assert_capacity_gate("r-alpha", "eu-north", 461)
        self._assert_capacity_invariant(500)
        self._assert_records_frozen()

        # -- public queries re-read after the operations agree with
        # the persisted records.
        self.assertEqual(
            resources_module.get(self.supply, "r-alpha")["capacity"], 5)
        self.assertEqual(
            resources_module.get(self.supply, "r-gamma", 1)["capacity"], 2)
        self.assertEqual(
            resources_module.get(self.supply, "r-gamma", 2)["capacity"], 4)
        self.assertEqual(
            resources_module.get(self.supply, "r-gamma")["version"], 2)
        self.assertEqual(
            signals_module.get(self.signals, "us-west", 250)["carbon_intensity"],
            8)
        west = signals_module.get(self.signals, "us-west", 310)
        self.assertEqual(west["version"], 2)
        self.assertEqual(west["carbon_intensity"], 0)
        self.assertEqual(jobs_module.get(self.jobs, "j-mig-ok")["state"],
                         "queued")
        self.assertEqual(
            cancellation_module.get(self.cancellations, "j-cancel")["at"],
            460)
        completed = completion_module.get(self.completions, "j-mig-ok")
        self.assertEqual(completed["current"],
                         {"resource_id": "r-beta", "version": 1})
        completed = completion_module.get(self.completions, "j-mig-fail")
        self.assertEqual(completed["current"],
                         {"resource_id": "r-alpha", "version": 1})

    # -- optional ledgers ----------------------------------------------------

    def test_optional_ledgers_preserve_historical_occupancy(self) -> None:
        # One resource per terminal release, capacity exactly one:
        # without the optional ledger the released unit still counts as
        # occupied; with it, the unit is reusable by a new clearing.
        resources_module.publish(
            self.supply, _resource("r-x", "eu-north", capacity=1), "rk-x")
        resources_module.publish(
            self.supply,
            _resource("r-y", "us-west", capacity=1, residency=["us-west"]),
            "rk-y")
        signals_module.publish(self.signals, _signal("eu-north"), "sk-north")
        signals_module.publish(
            self.signals,
            _signal("us-west", unit_cost=3, carbon_intensity=2), "sk-west")
        jobs_module.submit(self.jobs, _job("j-1"), "jk-1")
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        jobs_module.submit(
            self.jobs, _job("j-3", regions=["us-west"], residency=["us-west"]),
            "jk-3")
        jobs_module.submit(
            self.jobs, _job("j-4", regions=["us-west"], residency=["us-west"]),
            "jk-4")

        clear(self.jobs, self.supply, self.trades, "j-1", "t-1", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-1", "d-1", 15)
        self._launch_completed("j-1", "owner-1", 20, 25, 30, 35)
        execution_sync_module.run(self.execution, self.dispatch, self.sync,
                                  "owner-sync", "sync-1", 40, 50)
        completion_module.complete(
            self.jobs, self.supply, self.signals, self.trades, self.dispatch,
            self.execution, self.completions, "j-1", "co-1", 50, "succeeded",
            5, 5)

        # Without the completion ledger the completed job still
        # occupies its version; with it the released unit clears again.
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.trades, "j-2", "t-2", 60)
        trade, created = clear(self.jobs, self.supply, self.trades, "j-2",
                               "t-2", 60, completions=self.completions)
        self.assertTrue(created)
        self.assertEqual(trade["selection"],
                         {"resource_id": "r-x", "version": 1})
        self._assert_capacity_invariant(60)

        clear(self.jobs, self.supply, self.trades, "j-3", "t-3", 70)
        cancellation_module.cancel(self.jobs, self.supply, self.trades,
                                   self.dispatch, self.cancellations, "j-3",
                                   "x-3", 80, "user-request")
        # Without the cancellation ledger the cancelled job still
        # occupies its version; with it the released unit clears again.
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.trades, "j-4", "t-4", 90)
        trade, created = clear(self.jobs, self.supply, self.trades, "j-4",
                               "t-4", 90, cancellations=self.cancellations)
        self.assertTrue(created)
        self.assertEqual(trade["selection"],
                         {"resource_id": "r-y", "version": 1})
        self._assert_capacity_invariant(90)
        self.assertEqual(
            cancellation_module.get(self.cancellations, "j-3")["at"], 80)
        self.assertEqual(
            completion_module.get(self.completions, "j-1")["at"], 50)

    # -- idempotent replay and key conflicts -----------------------------------

    def _dir_bytes(self) -> dict[str, bytes]:
        return {name: Path(os.path.join(self.base, name)).read_bytes()
                for name in os.listdir(self.base) if name.endswith(".json")}

    def _assert_unchanged(self, before: dict[str, bytes]) -> None:
        self.assertEqual(self._dir_bytes(), before,
                         "a replay or refused request rewrote a file")

    def test_idempotent_replay_and_key_conflicts(self) -> None:
        published_r1, _ = resources_module.publish(
            self.supply, _resource("r-1", "eu-north"), "k-r1")
        resources_module.publish(
            self.supply,
            _resource("r-2", "us-west",
                      residency=["eu-north", "us-west"]), "k-r2")
        published_s1, _ = signals_module.publish(
            self.signals, _signal("eu-north", carbon_intensity=1), "k-s1")
        signals_module.publish(
            self.signals, _signal("us-west", unit_cost=3), "k-s2")
        signals_module.publish(
            self.signals,
            _signal("us-west", observed=300, unit_cost=3, carbon_intensity=0),
            "k-s3")
        submitted_j1, _ = jobs_module.submit(self.jobs, _job("j-1"), "k-j1")
        jobs_module.submit(self.jobs, _job("j-2"), "k-j2")
        jobs_module.submit(
            self.jobs, _job("j-3", regions=["eu-north", "us-west"]), "k-j3")

        def replay_and_conflict(expected, replay, conflict) -> None:
            before = self._dir_bytes()
            record, created = replay()
            # An equivalent replay returns the original record, reports
            # that nothing was created and rewrites no file.
            self.assertFalse(created)
            self.assertEqual(record, expected)
            self._assert_unchanged(before)
            # The same key carrying a different request is refused and
            # every input file keeps its exact bytes.
            with self.assertRaises(ValueError):
                conflict()
            self._assert_unchanged(before)

        # Supply, signal and acceptance publications.
        replay_and_conflict(
            published_r1,
            lambda: resources_module.publish(
                self.supply, _resource("r-1", "eu-north"), "k-r1"),
            lambda: resources_module.publish(
                self.supply, _resource("r-1", "eu-north", capacity=11),
                "k-r1"))
        replay_and_conflict(
            published_s1,
            lambda: signals_module.publish(
                self.signals, _signal("eu-north", carbon_intensity=1),
                "k-s1"),
            lambda: signals_module.publish(
                self.signals, _signal("eu-north", carbon_intensity=2),
                "k-s1"))
        replay_and_conflict(
            submitted_j1,
            lambda: jobs_module.submit(self.jobs, _job("j-1"), "k-j1"),
            lambda: jobs_module.submit(self.jobs, _job("j-1", work=2),
                                       "k-j1"))

        # Clearing and the dispatch/execution lifecycle of j-1.
        trade, created = clear(self.jobs, self.supply, self.trades, "j-1",
                               "k-t1", 10)
        self.assertTrue(created)
        replay_and_conflict(
            trade,
            lambda: clear(self.jobs, self.supply, self.trades, "j-1", "k-t1",
                          10),
            lambda: clear(self.jobs, self.supply, self.trades, "j-1", "k-t1",
                          11))
        decision, created = dispatch_module.commit(
            self.jobs, self.supply, self.trades, self.dispatch, "j-1",
            "k-d1", 15)
        self.assertTrue(created)
        replay_and_conflict(
            decision,
            lambda: dispatch_module.commit(self.jobs, self.supply,
                                           self.trades, self.dispatch,
                                           "j-1", "k-d1", 15),
            lambda: dispatch_module.commit(self.jobs, self.supply,
                                           self.trades, self.dispatch,
                                           "j-1", "k-d1", 16))
        decision, created = dispatch_module.claim(self.dispatch, "j-1",
                                                  "k-c1", "owner-1", 100, 20)
        self.assertTrue(created)
        self.assertEqual(decision["state"], "claimed")
        replay_and_conflict(
            decision,
            lambda: dispatch_module.claim(self.dispatch, "j-1", "k-c1",
                                          "owner-1", 100, 20),
            lambda: dispatch_module.claim(self.dispatch, "j-1", "k-c1",
                                          "owner-1", 200, 20))
        plan, created = execution_module.plan(self.jobs, self.supply,
                                              self.trades, self.dispatch,
                                              self.execution, "j-1", "k-p1",
                                              "owner-1", None, 30)
        self.assertTrue(created)
        replay_and_conflict(
            plan,
            lambda: execution_module.plan(self.jobs, self.supply,
                                          self.trades, self.dispatch,
                                          self.execution, "j-1", "k-p1",
                                          "owner-1", None, 30),
            lambda: execution_module.plan(self.jobs, self.supply,
                                          self.trades, self.dispatch,
                                          self.execution, "j-1", "k-p1",
                                          "owner-2", None, 30))
        plan, created = execution_module.record(self.execution, "j-1", 1,
                                                "k-e1", "owner-1", "stage",
                                                "succeeded", "staged", 40)
        self.assertTrue(created)
        replay_and_conflict(
            plan,
            lambda: execution_module.record(self.execution, "j-1", 1,
                                            "k-e1", "owner-1", "stage",
                                            "succeeded", "staged", 40),
            lambda: execution_module.record(self.execution, "j-1", 1,
                                            "k-e1", "owner-1", "stage",
                                            "succeeded", "other", 40))
        execution_module.record(self.execution, "j-1", 1, "k-e2", "owner-1",
                                "start", "succeeded", "started", 50)
        batch, created = execution_sync_module.run(
            self.execution, self.dispatch, self.sync, "owner-sync", "k-sync",
            60, 50)
        self.assertTrue(created)
        before = self._dir_bytes()
        again, created = execution_sync_module.run(
            self.execution, self.dispatch, self.sync, "owner-sync", "k-sync",
            60, 50)
        self.assertFalse(created)
        self.assertEqual(again, batch)
        self._assert_unchanged(before)
        completed, created = completion_module.complete(
            self.jobs, self.supply, self.signals, self.trades, self.dispatch,
            self.execution, self.completions, "j-1", "k-x1", 70, "succeeded",
            90, 120)
        self.assertTrue(created)
        replay_and_conflict(
            completed,
            lambda: completion_module.complete(
                self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.completions, "j-1",
                "k-x1", 70, "succeeded", 90, 120),
            lambda: completion_module.complete(
                self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.completions, "j-1",
                "k-x1", 70, "succeeded", 91, 120))

        # Cancellation of j-2 (traded, never dispatched).
        clear(self.jobs, self.supply, self.trades, "j-2", "k-t2", 10)
        cancelled, created = cancellation_module.cancel(
            self.jobs, self.supply, self.trades, self.dispatch,
            self.cancellations, "j-2", "k-x2", 25, "user-request")
        self.assertTrue(created)
        replay_and_conflict(
            cancelled,
            lambda: cancellation_module.cancel(
                self.jobs, self.supply, self.trades, self.dispatch,
                self.cancellations, "j-2", "k-x2", 25, "user-request"),
            lambda: cancellation_module.cancel(
                self.jobs, self.supply, self.trades, self.dispatch,
                self.cancellations, "j-2", "k-x2", 25, "other-reason"))

        # The migration lifecycle of j-3 up to its settlement.
        clear_live(self.jobs, self.supply, self.signals, self.trades, "j-3",
                   "k-t3", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-3", "k-d3", 15)
        advice, created = rebalance_module.evaluate(
            self.jobs, self.supply, self.signals, self.trades, self.dispatch,
            self.execution, self.advice, "j-3", "k-a3", 310)
        self.assertTrue(created)
        self.assertEqual(advice["recommendation"], "migrate")
        replay_and_conflict(
            advice,
            lambda: rebalance_module.evaluate(
                self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, "j-3", "k-a3",
                310),
            lambda: rebalance_module.evaluate(
                self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, "j-3", "k-a3",
                311))
        intent, created = rebalance_module.apply(
            self.jobs, self.supply, self.signals, self.trades,
            self.dispatch, self.execution, self.advice, self.intents,
            "j-3", "k-a3", "k-r3", 320)
        self.assertTrue(created)
        self.assertEqual(intent["reserved"], "reserved")
        replay_and_conflict(
            intent,
            lambda: rebalance_module.apply(
                self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, self.intents,
                "j-3", "k-a3", "k-r3", 320),
            lambda: rebalance_module.apply(
                self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, self.intents,
                "j-3", "k-a3", "k-r3", 321))
        plan, created = rebalance_module.start(
            self.jobs, self.supply, self.signals, self.trades,
            self.dispatch, self.execution, self.advice, self.intents,
            "j-3", "k-m3", "owner-1", 900, 330)
        self.assertTrue(created)
        self.assertEqual(plan["state"], "active")
        replay_and_conflict(
            plan,
            lambda: rebalance_module.start(
                self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, self.intents,
                "j-3", "k-m3", "owner-1", 900, 330),
            lambda: rebalance_module.start(
                self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, self.intents,
                "j-3", "k-m3", "owner-1", 800, 330))
        plan, created = rebalance_module.record(
            self.jobs, self.supply, self.signals, self.trades,
            self.dispatch, self.execution, self.advice, self.intents,
            "j-3", "k-m3-1", "owner-1", "copy", "succeeded", "rc1", 340)
        self.assertTrue(created)
        replay_and_conflict(
            plan,
            lambda: rebalance_module.record(
                self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, self.intents,
                "j-3", "k-m3-1", "owner-1", "copy", "succeeded", "rc1", 340),
            lambda: rebalance_module.record(
                self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, self.intents,
                "j-3", "k-m3-1", "owner-1", "copy", "succeeded", "rcX", 340))
        rebalance_module.record(
            self.jobs, self.supply, self.signals, self.trades, self.dispatch,
            self.execution, self.advice, self.intents, "j-3", "k-m3-2",
            "owner-1", "switch", "succeeded", "rc2", 350)
        settled, created = rebalance_module.settle(
            self.jobs, self.supply, self.signals, self.trades, self.dispatch,
            self.execution, self.advice, self.intents, self.settlements,
            "j-3", "k-m3", "k-s3", 360)
        self.assertTrue(created)
        self.assertEqual(settled["state"], "active")
        replay_and_conflict(
            settled,
            lambda: rebalance_module.settle(
                self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, self.intents,
                self.settlements, "j-3", "k-m3", "k-s3", 360),
            lambda: rebalance_module.settle(
                self.jobs, self.supply, self.signals, self.trades,
                self.dispatch, self.execution, self.advice, self.intents,
                self.settlements, "j-3", "k-m3", "k-s3", 361))
        binding = rebalance_module.current(
            self.jobs, self.supply, self.signals, self.trades, self.dispatch,
            self.execution, self.advice, self.intents, self.settlements,
            "j-3")
        self.assertEqual(binding["resource_id"], "r-2")
        self.assertEqual(binding["state"], "active")


class LifecycleConcurrencyTest(unittest.TestCase):
    """Races over the public entry points must conserve capacity."""

    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = self.tmp.name

    def _paths(self, base: str) -> dict[str, str]:
        return {name: os.path.join(base, f"{name}.json")
                for name in ("jobs", "supply", "signals", "trades",
                             "dispatch", "cancellations")}

    def test_concurrent_cancel_and_first_commit(self) -> None:
        for _ in range(12):
            tmp = TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            paths = self._paths(tmp.name)
            jobs_module.submit(paths["jobs"], _job("j-a"), "jk-a")
            jobs_module.submit(paths["jobs"], _job("j-b"), "jk-b")
            jobs_module.submit(
                paths["jobs"],
                _job("j-seed", regions=["zz-seed"], residency=["zz-seed"]),
                "jk-seed")
            resources_module.publish(
                paths["supply"], _resource("r-x", "eu-north", capacity=1),
                "rk-x")
            resources_module.publish(
                paths["supply"],
                _resource("r-seed", "zz-seed", capacity=10,
                          residency=["zz-seed"]), "rk-seed")
            clear(paths["jobs"], paths["supply"], paths["trades"], "j-a",
                  "t-a", 10)
            clear(paths["jobs"], paths["supply"], paths["trades"], "j-seed",
                  "t-seed", 10)
            # Seed the cancellation ledger so the racing commit always
            # reads it as part of one consistent snapshot.
            cancellation_module.cancel(
                paths["jobs"], paths["supply"], paths["trades"],
                paths["dispatch"], paths["cancellations"], "j-seed", "x-seed",
                15, "seed")

            barrier = threading.Barrier(2)
            outcome: dict[str, object] = {}

            def run_cancel() -> None:
                barrier.wait()
                try:
                    outcome["cancel"] = cancellation_module.cancel(
                        paths["jobs"], paths["supply"], paths["trades"],
                        paths["dispatch"], paths["cancellations"], "j-a",
                        "x-a", 20, "race")
                except (ValueError, PermissionError) as exc:
                    outcome["cancel"] = exc

            def run_commit() -> None:
                barrier.wait()
                try:
                    outcome["commit"] = dispatch_module.commit(
                        paths["jobs"], paths["supply"], paths["trades"],
                        paths["dispatch"], "j-a", "d-a", 20,
                        cancellations=paths["cancellations"])
                except (ValueError, PermissionError) as exc:
                    outcome["commit"] = exc

            threads = (threading.Thread(target=run_cancel),
                       threading.Thread(target=run_commit))
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            cancelled = isinstance(outcome["cancel"], tuple) \
                and outcome["cancel"][1]
            committed = isinstance(outcome["commit"], tuple) \
                and outcome["commit"][1]
            # Exactly one first write wins; the job never holds both a
            # cancellation and a dispatch decision.
            self.assertNotEqual(cancelled, committed)
            decisions = _read_json(paths["dispatch"])["decisions"] \
                if os.path.exists(paths["dispatch"]) else {}
            if cancelled:
                self.assertIsInstance(outcome["commit"], ValueError)
                self.assertNotIn("j-a", decisions)
                self.assertEqual(
                    cancellation_module.get(paths["cancellations"], "j-a")
                    ["job_id"], "j-a")
                # The released unit is reusable by a new clearing.
                _trade, created = clear(
                    paths["jobs"], paths["supply"], paths["trades"], "j-b",
                    "t-b", 25, cancellations=paths["cancellations"])
                self.assertTrue(created)
            else:
                self.assertIsInstance(outcome["cancel"], PermissionError)
                self.assertIn("j-a", decisions)
                with self.assertRaises(KeyError):
                    cancellation_module.get(paths["cancellations"], "j-a")
                # The committed job still occupies the only unit.
                with self.assertRaises(LookupError):
                    clear(paths["jobs"], paths["supply"], paths["trades"],
                          "j-b", "t-b", 25,
                          cancellations=paths["cancellations"])

    def test_concurrent_clear_never_oversells_last_unit(self) -> None:
        for _ in range(5):
            tmp = TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            paths = self._paths(tmp.name)
            resources_module.publish(
                paths["supply"], _resource("r-x", "eu-north", capacity=3),
                "rk-x")
            for index in range(8):
                jobs_module.submit(paths["jobs"], _job(f"j-{index}"),
                                   f"jk-{index}")

            barrier = threading.Barrier(8)
            results: list[object] = []
            lock = threading.Lock()

            def worker(index: int) -> None:
                barrier.wait()
                try:
                    outcome = clear(paths["jobs"], paths["supply"],
                                    paths["trades"], f"j-{index}",
                                    f"t-{index}", 50)
                    with lock:
                        results.append(outcome)
                except LookupError as exc:
                    with lock:
                        results.append(exc)

            threads = [threading.Thread(target=worker, args=(i,))
                       for i in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            wins = [outcome for outcome in results
                    if isinstance(outcome, tuple)]
            losses = [outcome for outcome in results
                      if isinstance(outcome, LookupError)]
            # The successful trades never exceed the available units.
            self.assertEqual(len(wins), 3)
            self.assertEqual(len(losses), 5)
            self.assertTrue(all(created for _, created in wins))
            trades = _read_json(paths["trades"])["trades"]
            self.assertEqual(len(trades), 3)
            sold = sum(trade["work"] for trade in trades.values())
            self.assertEqual(sold, 3)
            capacity = resources_module.get(paths["supply"], "r-x")[
                "capacity"]
            self.assertLessEqual(sold, capacity)
            # One more job still cannot clear: the last unit is gone.
            jobs_module.submit(paths["jobs"], _job("j-late"), "jk-late")
            with self.assertRaises(LookupError):
                clear(paths["jobs"], paths["supply"], paths["trades"],
                      "j-late", "t-late", 50)


if __name__ == "__main__":
    unittest.main()
