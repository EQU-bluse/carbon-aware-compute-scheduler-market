from __future__ import annotations

import json
import os
import threading
import unittest
from tempfile import TemporaryDirectory

from carbon_market import cancellation as cancellation_module
from carbon_market import dispatch as dispatch_module
from carbon_market import jobs as jobs_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.cancellation import cancel, get
from carbon_market.market import clear, clear_live


def _resource(resource_id: str = "r-1", region: str = "eu-north",
              **overrides: object) -> dict[str, object]:
    resource: dict[str, object] = {
        "resource_id": resource_id,
        "region": region,
        "capacity": 100,
        "start": 0,
        "end": 500,
        "unit_cost": 5,
        "carbon_intensity": 8,
        "residency": [region],
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
        "carbon_intensity": 8,
    }
    signal.update(overrides)
    return signal


def _job(job_id: str = "j-1", **overrides: object) -> dict[str, object]:
    job: dict[str, object] = {
        "job_id": job_id,
        "work": 10,
        "deadline": 100,
        "regions": ["eu-north"],
        "residency": ["eu-north"],
        "max_cost": 1000,
        "carbon_cap": 1000,
    }
    job.update(overrides)
    return job


class CancellationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        base = self.tmp.name
        self.jobs = os.path.join(base, "jobs.json")
        self.supply = os.path.join(base, "supply.json")
        self.signals = os.path.join(base, "signals.json")
        self.trades = os.path.join(base, "trades.json")
        self.dispatch = os.path.join(base, "dispatch.json")
        self.cancellations = os.path.join(base, "cancellations.json")
        jobs_module.submit(self.jobs, _job(), "jk-1")
        resources_module.publish(self.supply, _resource(), "rk-1")

    # -- fixture helpers -----------------------------------------------------

    def _trade(self, job_id: str = "j-1", key: str = "t-1",
               at: int = 10) -> dict[str, object]:
        trade, created = clear(self.jobs, self.supply, self.trades,
                               job_id, key, at)
        self.assertTrue(created)
        return trade

    def _cancel(self, job_id: str = "j-1", key: str = "c-1", at: int = 20,
                reason: str = "no longer needed"
                ) -> tuple[dict[str, object], bool]:
        return cancel(self.jobs, self.supply, self.trades, self.dispatch,
                      self.cancellations, job_id, key, at, reason)

    def _read_ledger(self) -> dict[str, object]:
        with open(self.cancellations, encoding="utf-8") as handle:
            return json.load(handle)

    # -- first cancellation --------------------------------------------------

    def test_first_cancel_commits_record_atomically(self) -> None:
        self._trade()
        record, created = self._cancel()
        self.assertTrue(created)
        self.assertEqual(record, {
            "job_id": "j-1",
            "at": 20,
            "reason": "no longer needed",
            "resource_id": "r-1",
            "version": 1,
        })
        # The record, the request binding and the audit event landed in
        # one document, all keyed by the idempotency key.
        ledger = self._read_ledger()
        self.assertEqual(set(ledger),
                         {"version", "cancellations", "idempotency",
                          "audit"})
        self.assertEqual(set(ledger["cancellations"]), {"c-1"})
        self.assertEqual(ledger["cancellations"]["c-1"], record)
        self.assertEqual(ledger["idempotency"]["c-1"],
                         {"job_id": "j-1", "at": 20,
                          "reason": "no longer needed"})
        event = ledger["audit"]["c-1"]
        self.assertEqual(event["key"], "c-1")
        self.assertEqual(event["request"], ledger["idempotency"]["c-1"])
        self.assertEqual(event["result"], record)
        # The read-only query returns the same record.
        self.assertEqual(get(self.cancellations, "j-1"), record)

    def test_cancel_freezes_trade_selection(self) -> None:
        resources_module.publish(
            self.supply, _resource("r-2", carbon_intensity=1), "rk-2")
        trade = self._trade()
        self.assertEqual(trade["resource_id"], "r-2")
        record, created = self._cancel(at=30, reason="superseded")
        self.assertTrue(created)
        self.assertEqual(record["resource_id"], "r-2")
        self.assertEqual(record["version"], 1)

    def test_replay_returns_record_without_rewrite(self) -> None:
        self._trade()
        record, created = self._cancel()
        self.assertTrue(created)
        with open(self.cancellations, "rb") as handle:
            before = handle.read()
        replayed, created = self._cancel()
        self.assertFalse(created)
        self.assertEqual(replayed, record)
        with open(self.cancellations, "rb") as handle:
            self.assertEqual(handle.read(), before)

    def test_same_key_different_request_rejected(self) -> None:
        self._trade()
        self._cancel()
        for kwargs in ({"at": 21}, {"reason": "other"},
                       {"at": 21, "reason": "other"}):
            with self.assertRaises(ValueError):
                self._cancel(key="c-1", **kwargs)
        # A key bound to one job cannot be reused for another.
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        self._trade("j-2", "t-2")
        with self.assertRaises(ValueError):
            self._cancel("j-2", "c-1")

    def test_same_job_different_key_rejected(self) -> None:
        self._trade()
        self._cancel()
        with self.assertRaises(ValueError):
            self._cancel(key="c-2")
        with self.assertRaises(ValueError):
            self._cancel(key="c-2", at=30, reason="other")

    # -- refusal taxonomy ----------------------------------------------------

    def test_unknown_job_raises_key_error(self) -> None:
        self._trade()
        with self.assertRaises(KeyError):
            self._cancel("j-unknown")
        self.assertFalse(os.path.exists(self.cancellations))

    def test_untraded_job_raises_lookup_error(self) -> None:
        # The clearing ledger exists but records no trade for j-1.
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        self._trade("j-2", "t-2")
        with self.assertRaises(LookupError):
            self._cancel()
        self.assertFalse(os.path.exists(self.cancellations))

    def test_dispatched_job_raises_permission_error(self) -> None:
        self._trade()
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-1", "d-1", 15)
        with self.assertRaises(PermissionError):
            self._cancel()
        self.assertFalse(os.path.exists(self.cancellations))

    def test_moment_before_trade_rejected(self) -> None:
        self._trade(at=10)
        with self.assertRaises(ValueError):
            self._cancel(at=9)
        # The trade moment itself is acceptable.
        record, created = self._cancel(at=10)
        self.assertTrue(created)
        self.assertEqual(record["at"], 10)

    def test_invalid_arguments_rejected(self) -> None:
        self._trade()
        bad_calls = (
            # Empty or mistyped reason.
            ("", 20, "c-1"), (None, 20, "c-1"), (5, 20, "c-1"),
        )
        for reason, at, key in bad_calls:
            with self.assertRaises(ValueError):
                cancel(self.jobs, self.supply, self.trades, self.dispatch,
                       self.cancellations, "j-1", key, at, reason)
        for at in (-1, True, 1.5, "20"):
            with self.assertRaises(ValueError):
                self._cancel(at=at)
        for key in ("", None):
            with self.assertRaises(ValueError):
                self._cancel(key=key)
        # Paths must be non-empty strings and distinct real locations.
        with self.assertRaises(ValueError):
            cancel("", self.supply, self.trades, self.dispatch,
                   self.cancellations, "j-1", "c-1", 20, "x")
        with self.assertRaises(ValueError):
            cancel(self.jobs, self.supply, self.trades, self.dispatch,
                   self.trades, "j-1", "c-1", 20, "x")
        self.assertFalse(os.path.exists(self.cancellations))

    def test_missing_inputs_raise_file_not_found(self) -> None:
        self._trade()
        missing = os.path.join(self.tmp.name, "missing.json")
        with self.assertRaises(FileNotFoundError):
            cancel(missing, self.supply, self.trades, self.dispatch,
                   self.cancellations, "j-1", "c-1", 20, "x")
        with self.assertRaises(FileNotFoundError):
            cancel(self.jobs, missing, self.trades, self.dispatch,
                   self.cancellations, "j-1", "c-1", 20, "x")
        with self.assertRaises(FileNotFoundError):
            cancel(self.jobs, self.supply, missing, self.dispatch,
                   self.cancellations, "j-1", "c-1", 20, "x")
        with self.assertRaises(FileNotFoundError):
            cancel(self.jobs, self.supply, self.trades, self.dispatch,
                   os.path.join(self.tmp.name, "no-dir", "cancel.json"),
                   "j-1", "c-1", 20, "x")

    def test_corrupt_or_noncanonical_ledger_rejected(self) -> None:
        self._trade()
        self._cancel()
        with open(self.cancellations, "rb") as handle:
            canonical = handle.read()
        with open(self.cancellations, "w", encoding="utf-8") as handle:
            handle.write("not json")
        with self.assertRaises(ValueError):
            self._cancel(key="c-2")
        with self.assertRaises(ValueError):
            get(self.cancellations, "j-1")
        # Valid JSON but not the canonical compact form.
        with open(self.cancellations, "w", encoding="utf-8") as handle:
            json.dump(json.loads(canonical), handle, indent=2)
        with self.assertRaises(ValueError):
            self._cancel(key="c-2")
        with open(self.cancellations, "wb") as handle:
            handle.write(canonical)
        replayed, created = self._cancel()
        self.assertFalse(created)

    def test_get_unknown_job_and_missing_ledger(self) -> None:
        with self.assertRaises(KeyError):
            get(self.cancellations, "j-1")
        self._trade()
        self._cancel()
        with self.assertRaises(KeyError):
            get(self.cancellations, "j-2")
        with self.assertRaises(ValueError):
            get("", "j-1")
        with self.assertRaises(ValueError):
            get(self.cancellations, "")

    # -- commit failure ------------------------------------------------------

    def _fail_directory_sync(self) -> object:
        original = cancellation_module._fsync_directory

        def failing(directory: str) -> None:
            raise OSError("simulated sync failure")

        cancellation_module._fsync_directory = failing
        return original

    def test_failed_sync_restores_previous_bytes(self) -> None:
        self._trade()
        self._cancel()
        with open(self.cancellations, "rb") as handle:
            before = handle.read()
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        self._trade("j-2", "t-2")
        original = self._fail_directory_sync()
        try:
            with self.assertRaises(OSError):
                self._cancel("j-2", "c-2")
        finally:
            cancellation_module._fsync_directory = original
        with open(self.cancellations, "rb") as handle:
            self.assertEqual(handle.read(), before)
        # The ledger still serves the original record and the failed
        # call can be retried.
        self.assertEqual(get(self.cancellations, "j-1")["job_id"], "j-1")
        record, created = self._cancel("j-2", "c-2")
        self.assertTrue(created)
        self.assertEqual(record["job_id"], "j-2")

    def test_failed_first_commit_leaves_no_file(self) -> None:
        self._trade()
        original = self._fail_directory_sync()
        try:
            with self.assertRaises(OSError):
                self._cancel()
        finally:
            cancellation_module._fsync_directory = original
        self.assertFalse(os.path.exists(self.cancellations))
        self.assertEqual([name for name in os.listdir(self.tmp.name)
                          if name.endswith(".tmp")], [])

    # -- market.clear integration --------------------------------------------

    def _submit_full(self, job_id: str, key: str) -> None:
        # A job whose work exactly fills the resource's whole capacity.
        jobs_module.submit(self.jobs, _job(job_id, work=100), key)

    def test_clear_releases_cancelled_capacity(self) -> None:
        self._submit_full("j-full", "jk-f")
        self._submit_full("j-2", "jk-2")
        self._trade("j-full", "t-f")
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.trades, "j-2", "t-2", 25)
        self._cancel("j-full", "c-f", 20)
        # Without the cancellation ledger the occupancy still counts.
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.trades, "j-2", "t-2", 25)
        # Before the cancellation moment the release is not effective.
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.trades, "j-2", "t-2", 15,
                  cancellations=self.cancellations)
        # From the cancellation moment on the exact version's capacity
        # is free again.
        trade, created = clear(self.jobs, self.supply, self.trades,
                               "j-2", "t-2", 25,
                               cancellations=self.cancellations)
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-1")
        self.assertEqual(trade["version"], 1)

    def test_clear_live_releases_cancelled_capacity(self) -> None:
        signals_module.publish(self.signals, _signal(), "sk-1")
        self._submit_full("j-full", "jk-f")
        self._submit_full("j-2", "jk-2")
        trade, created = clear_live(self.jobs, self.supply, self.signals,
                                    self.trades, "j-full", "t-f", 10)
        self.assertTrue(created)
        with self.assertRaises(LookupError):
            clear_live(self.jobs, self.supply, self.signals, self.trades,
                       "j-2", "t-2", 25)
        self._cancel("j-full", "c-f", 20)
        trade, created = clear_live(self.jobs, self.supply, self.signals,
                                    self.trades, "j-2", "t-2", 25,
                                    cancellations=self.cancellations)
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-1")

    def test_clear_rejects_new_trade_for_cancelled_job(self) -> None:
        self._trade()
        self._cancel()
        # The original key's equivalent replay still returns the
        # original trade; the cancellation does not rewrite history.
        trade, created = clear(self.jobs, self.supply, self.trades,
                               "j-1", "t-1", 10,
                               cancellations=self.cancellations)
        self.assertFalse(created)
        self.assertEqual(trade["resource_id"], "r-1")
        # A new trade for the cancelled job is refused.
        with self.assertRaises(ValueError):
            clear(self.jobs, self.supply, self.trades, "j-1", "t-9", 30,
                  cancellations=self.cancellations)

    def test_clear_with_missing_cancellation_ledger(self) -> None:
        self._trade()
        with self.assertRaises(FileNotFoundError):
            clear(self.jobs, self.supply, self.trades, "j-1", "t-9", 30,
                  cancellations=self.cancellations)

    # -- dispatch.commit integration -----------------------------------------

    def test_commit_rejects_cancelled_job(self) -> None:
        self._trade()
        self._cancel()
        with self.assertRaises(ValueError):
            dispatch_module.commit(self.jobs, self.supply, self.trades,
                                   self.dispatch, "j-1", "d-1", 25,
                                   cancellations=self.cancellations)
        # No decision was created.
        self.assertFalse(os.path.exists(self.dispatch))
        # Without the cancellation ledger the historical behavior is
        # unchanged: the commit succeeds.
        decision, created = dispatch_module.commit(
            self.jobs, self.supply, self.trades, self.dispatch,
            "j-1", "d-1", 25)
        self.assertTrue(created)
        self.assertEqual(decision["state"], "ready")

    def test_commit_with_missing_cancellation_ledger(self) -> None:
        self._trade()
        with self.assertRaises(FileNotFoundError):
            dispatch_module.commit(self.jobs, self.supply, self.trades,
                                   self.dispatch, "j-1", "d-1", 25,
                                   cancellations=self.cancellations)

    # -- concurrency ---------------------------------------------------------

    def test_concurrent_cancel_and_commit_are_consistent(self) -> None:
        for _ in range(10):
            tmp = TemporaryDirectory()
            self.addCleanup(tmp.cleanup)
            base = tmp.name
            jobs = os.path.join(base, "jobs.json")
            supply = os.path.join(base, "supply.json")
            trades = os.path.join(base, "trades.json")
            dispatch = os.path.join(base, "dispatch.json")
            cancellations = os.path.join(base, "cancellations.json")
            jobs_module.submit(jobs, _job(), "jk-1")
            jobs_module.submit(jobs, _job("j-0"), "jk-0")
            resources_module.publish(supply, _resource(), "rk-1")
            clear(jobs, supply, trades, "j-1", "t-1", 10)
            clear(jobs, supply, trades, "j-0", "t-0", 10)
            # An unrelated first cancellation brings the cancellation
            # ledger into existence, so the racing commit always reads
            # it as part of the snapshot.
            cancel(jobs, supply, trades, dispatch, cancellations,
                   "j-0", "c-0", 15, "seed")

            barrier = threading.Barrier(2)
            outcome: dict[str, object] = {}

            def run_cancel() -> None:
                barrier.wait()
                try:
                    record, created = cancel(
                        jobs, supply, trades, dispatch, cancellations,
                        "j-1", "c-1", 20, "race")
                    outcome["cancel"] = (record, created)
                except (ValueError, PermissionError) as exc:
                    outcome["cancel"] = exc

            def run_commit() -> None:
                barrier.wait()
                try:
                    decision, created = dispatch_module.commit(
                        jobs, supply, trades, dispatch, "j-1", "d-1", 20,
                        cancellations=cancellations)
                    outcome["commit"] = (decision, created)
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
            # Exactly one side wins the race; the loser is refused.
            self.assertNotEqual(cancelled, committed)
            if cancelled:
                self.assertIsInstance(outcome["commit"], ValueError)
                self.assertEqual(get(cancellations, "j-1")["job_id"],
                                 "j-1")
            else:
                self.assertIsInstance(outcome["cancel"], PermissionError)
                with self.assertRaises(KeyError):
                    get(cancellations, "j-1")


if __name__ == "__main__":
    unittest.main()
