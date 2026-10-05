from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

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
        signals_module.publish(self.signals, _signal(), "sk-1")

    # -- fixture helpers -----------------------------------------------------

    def _trade(self, job_id: str = "j-1", key: str = "t-1",
               at: int = 10) -> dict[str, object]:
        trade, created = clear(self.jobs, self.supply, self.trades,
                               job_id, key, at)
        self.assertTrue(created)
        return trade

    def _cancel(self, job_id: str = "j-1", key: str = "c-1",
                at: int = 20, reason: str = "no longer needed",
                ) -> tuple[dict[str, object], bool]:
        return cancel(self.jobs, self.supply, self.trades, self.dispatch,
                      self.cancellations, job_id, key, at, reason)

    def _fill_capacity(self) -> None:
        # A second version of r-1 leaves room for exactly one job.
        resources_module.publish(
            self.supply, _resource(capacity=10, start=1), "rk-2")

    # -- cancel --------------------------------------------------------------

    def test_cancel_freezes_job_moment_reason_and_selection(self) -> None:
        self._trade()
        record, created = self._cancel()
        self.assertTrue(created)
        self.assertEqual(record, {
            "job_id": "j-1",
            "at": 20,
            "reason": "no longer needed",
            "selection": {"resource_id": "r-1", "version": 1},
        })

    def test_cancel_live_trade_selection(self) -> None:
        trade, created = clear_live(self.jobs, self.supply, self.signals,
                                    self.trades, "j-1", "t-1", 10)
        self.assertTrue(created)
        record, created = self._cancel()
        self.assertTrue(created)
        self.assertEqual(record["selection"], {
            "resource_id": trade["resource_id"],
            "version": trade["version"],
        })

    def test_cancel_writes_canonical_ledger(self) -> None:
        self._trade()
        self._cancel()
        raw = Path(self.cancellations).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertNotIn(b'", "', raw)
        self.assertNotIn(b'": "', raw)
        data = json.loads(raw.decode("utf-8"))
        self.assertEqual(list(data),
                         ["version", "cancellations", "idempotency",
                          "audit"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["cancellations"]), ["c-1"])
        record = data["cancellations"]["c-1"]
        self.assertEqual(list(record),
                         ["job_id", "at", "reason", "selection"])
        self.assertEqual(data["idempotency"]["c-1"],
                         {"job_id": "j-1", "at": 20,
                          "reason": "no longer needed"})
        event = data["audit"]["c-1"]
        self.assertEqual(event["key"], "c-1")
        self.assertEqual(event["request"], data["idempotency"]["c-1"])
        self.assertEqual(event["result"], record)

    def test_replay_returns_record_without_rewrite(self) -> None:
        self._trade()
        record, _ = self._cancel()
        before = Path(self.cancellations).read_bytes()
        replayed, created = self._cancel()
        self.assertFalse(created)
        self.assertEqual(replayed, record)
        self.assertEqual(Path(self.cancellations).read_bytes(), before)

    def test_same_key_with_another_request_is_value_error(self) -> None:
        self._trade()
        self._cancel()
        for kwargs in ({"at": 21}, {"reason": "other"},
                       {"job_id": "j-2"}):
            with self.assertRaises(ValueError, msg=repr(kwargs)):
                self._cancel(**kwargs)  # type: ignore[arg-type]

    def test_same_job_under_another_key_is_value_error(self) -> None:
        self._trade()
        self._cancel()
        with self.assertRaises(ValueError):
            self._cancel(key="c-2")

    def test_unknown_job_is_key_error(self) -> None:
        self._trade()
        with self.assertRaises(KeyError):
            self._cancel(job_id="j-unknown")

    def test_untraded_job_is_lookup_error(self) -> None:
        self._trade()
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        with self.assertRaises(LookupError):
            self._cancel(job_id="j-2")

    def test_dispatch_decision_is_permission_error(self) -> None:
        self._trade()
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, "j-1", "d-1", 15)
        with self.assertRaises(PermissionError):
            self._cancel()
        # The refused request never creates the ledger.
        self.assertFalse(Path(self.cancellations).exists())

    def test_cancel_before_trade_moment_is_value_error(self) -> None:
        self._trade(at=10)
        with self.assertRaises(ValueError):
            self._cancel(at=9)
        record, created = self._cancel(at=10)
        self.assertTrue(created)
        self.assertEqual(record["at"], 10)

    def test_invalid_arguments(self) -> None:
        self._trade()
        for kwargs in (
            {"reason": ""},
            {"key": ""},
            {"job_id": ""},
            {"at": -1},
            {"at": True},
            {"at": 1.5},
        ):
            with self.assertRaises(ValueError, msg=repr(kwargs)):
                self._cancel(**kwargs)  # type: ignore[arg-type]
        for bad_path in ("", None, 3):
            with self.assertRaises(ValueError):
                cancel(bad_path, self.supply, self.trades, self.dispatch,  # type: ignore[arg-type]
                       self.cancellations, "j-1", "c-9", 20, "r")
        self.assertFalse(Path(self.cancellations).exists())

    def test_paths_must_be_distinct(self) -> None:
        self._trade()
        with self.assertRaises(ValueError):
            cancel(self.jobs, self.supply, self.trades, self.trades,
                   self.cancellations, "j-1", "c-1", 20, "r")
        with self.assertRaises(ValueError):
            cancel(self.jobs, self.supply, self.trades, self.dispatch,
                   self.jobs, "j-1", "c-1", 20, "r")

    def test_missing_input_is_file_not_found(self) -> None:
        self._trade()
        with self.assertRaises(FileNotFoundError):
            cancel(os.path.join(self.tmp.name, "no-jobs.json"),
                   self.supply, self.trades, self.dispatch,
                   self.cancellations, "j-1", "c-1", 20, "r")
        with self.assertRaises(FileNotFoundError):
            cancel(self.jobs, os.path.join(self.tmp.name, "no-supply.json"),
                   self.trades, self.dispatch, self.cancellations,
                   "j-1", "c-1", 20, "r")
        with self.assertRaises(FileNotFoundError):
            cancel(self.jobs, self.supply,
                   os.path.join(self.tmp.name, "no-trades.json"),
                   self.dispatch, self.cancellations, "j-1", "c-1", 20, "r")
        self.assertFalse(Path(self.cancellations).exists())

    def test_missing_dispatch_ledger_means_no_decisions(self) -> None:
        self._trade()
        # The dispatch ledger simply does not exist yet.
        _record, created = self._cancel()
        self.assertTrue(created)

    def test_missing_parent_is_file_not_found_and_leaves_no_trace(
            self) -> None:
        self._trade()
        missing = os.path.join(self.tmp.name, "no-dir", "cancel.json")
        with self.assertRaises(FileNotFoundError):
            cancel(self.jobs, self.supply, self.trades, self.dispatch,
                   missing, "j-1", "c-1", 20, "r")
        self.assertFalse(Path(os.path.join(self.tmp.name, "no-dir"))
                         .exists())
        # No temporary fragment is left behind.
        self.assertEqual([name for name in os.listdir(self.tmp.name)
                          if name.startswith(".")], [])

    def test_corrupt_ledger_is_value_error(self) -> None:
        self._trade()
        self._cancel()
        Path(self.cancellations).write_bytes(b"not json")
        with self.assertRaises(ValueError):
            self._cancel(key="c-2")
        with self.assertRaises(ValueError):
            get(self.cancellations, "j-1")

    def test_noncanonical_ledger_is_value_error(self) -> None:
        self._trade()
        self._cancel()
        data = json.loads(Path(self.cancellations).read_bytes())
        Path(self.cancellations).write_bytes(
            json.dumps(data, indent=2).encode("utf-8"))
        with self.assertRaises(ValueError):
            self._cancel(key="c-2")

    def test_structurally_invalid_record_is_value_error(self) -> None:
        self._trade()
        self._cancel()
        data = json.loads(Path(self.cancellations).read_bytes())
        data["cancellations"]["c-1"]["reason"] = ""
        Path(self.cancellations).write_bytes(
            (json.dumps(data, ensure_ascii=False, separators=(",", ":"))
             + "\n").encode("utf-8"))
        with self.assertRaises(ValueError):
            get(self.cancellations, "j-1")

    def test_tampered_selection_is_value_error(self) -> None:
        self._trade()
        self._cancel()
        data = json.loads(Path(self.cancellations).read_bytes())
        data["cancellations"]["c-1"]["selection"]["version"] = 99
        # Rewrite in canonical compact form so only the frozen business
        # reference is wrong; the next cancel revalidates it against
        # the trades snapshot.
        Path(self.cancellations).write_bytes(
            (json.dumps(data, ensure_ascii=False, separators=(",", ":"))
             + "\n").encode("utf-8"))
        with self.assertRaises(ValueError):
            self._cancel(key="c-2")

    # -- get -----------------------------------------------------------------

    def test_get_returns_record_and_raises_key_error(self) -> None:
        self._trade()
        record, _ = self._cancel()
        self.assertEqual(get(self.cancellations, "j-1"), record)
        with self.assertRaises(KeyError):
            get(self.cancellations, "j-2")
        # A missing ledger means no job was ever cancelled.
        with self.assertRaises(KeyError):
            get(os.path.join(self.tmp.name, "none.json"), "j-1")
        with self.assertRaises(ValueError):
            get("", "j-1")
        with self.assertRaises(ValueError):
            get(self.cancellations, "")

    # -- market clearing with the cancellation ledger ------------------------

    def test_clear_releases_capacity_after_cancellation(self) -> None:
        self._fill_capacity()
        self._trade()
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.trades, "j-2", "t-2", 30)
        self._cancel(at=20)
        # Without the cancellation ledger the result is unchanged.
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.trades, "j-2", "t-2", 30)
        # With it, j-1's booking no longer occupies the version.
        trade, created = clear(self.jobs, self.supply, self.trades,
                               "j-2", "t-2", 30,
                               cancellations=self.cancellations)
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-1")
        self.assertEqual(trade["version"], 2)

    def test_clear_live_releases_capacity_after_cancellation(
            self) -> None:
        self._fill_capacity()
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   "j-1", "t-1", 10)
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        self._cancel(at=20)
        trade, created = clear_live(self.jobs, self.supply, self.signals,
                                    self.trades, "j-2", "t-2", 30,
                                    cancellations=self.cancellations)
        self.assertTrue(created)
        self.assertEqual(trade["resource_id"], "r-1")

    def test_future_cancellation_does_not_release_yet(self) -> None:
        self._fill_capacity()
        self._trade()
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        self._cancel(at=40)
        # The cancellation lies in the future of the evaluation moment.
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.trades, "j-2", "t-2", 30,
                  cancellations=self.cancellations)
        trade, _ = clear(self.jobs, self.supply, self.trades, "j-2",
                         "t-2b", 40, cancellations=self.cancellations)
        self.assertEqual(trade["resource_id"], "r-1")

    def test_cancelled_job_cannot_be_traded_again(self) -> None:
        self._trade()
        self._cancel()
        with self.assertRaises(ValueError):
            clear(self.jobs, self.supply, self.trades, "j-1", "t-2", 30,
                  cancellations=self.cancellations)
        with self.assertRaises(ValueError):
            clear_live(self.jobs, self.supply, self.signals, self.trades,
                       "j-1", "t-2", 30,
                       cancellations=self.cancellations)

    def test_original_trade_replay_is_unaffected_by_cancellation(
            self) -> None:
        original = self._trade()
        self._cancel()
        replayed, created = clear(self.jobs, self.supply, self.trades,
                                  "j-1", "t-1", 10,
                                  cancellations=self.cancellations)
        self.assertFalse(created)
        self.assertEqual(replayed, original)

    def test_reused_capacity_keeps_ledger_readable_with_ledger(
            self) -> None:
        self._fill_capacity()
        self._trade()
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        self._cancel(at=20)
        clear(self.jobs, self.supply, self.trades, "j-2", "t-2", 30,
              cancellations=self.cancellations)
        # A later clear with the ledger still validates the reused
        # envelope, and dispatch.commit given the same ledger accepts
        # the new trade.
        jobs_module.submit(self.jobs, _job("j-3", work=1), "jk-3")
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.trades, "j-3", "t-3", 30,
                  cancellations=self.cancellations)
        decision, created = dispatch_module.commit(
            self.jobs, self.supply, self.trades, self.dispatch, "j-2",
            "d-2", 35, cancellations=self.cancellations)
        self.assertTrue(created)
        self.assertEqual(decision["job_id"], "j-2")

    # -- dispatch.commit with the cancellation ledger ------------------------

    def test_commit_rejects_cancelled_job_and_creates_nothing(
            self) -> None:
        self._trade()
        self._cancel()
        with self.assertRaises(ValueError):
            dispatch_module.commit(
                self.jobs, self.supply, self.trades, self.dispatch,
                "j-1", "d-1", 25, cancellations=self.cancellations)
        self.assertFalse(Path(self.dispatch).exists())

    def test_commit_without_ledger_is_unchanged(self) -> None:
        self._trade()
        self._cancel()
        # Without the cancellation ledger the historical behavior is
        # kept: the commit succeeds.
        decision, created = dispatch_module.commit(
            self.jobs, self.supply, self.trades, self.dispatch, "j-1",
            "d-1", 25)
        self.assertTrue(created)
        self.assertEqual(decision["state"], "ready")

    def test_commit_accepts_uncancelled_job_with_ledger(self) -> None:
        self._trade()
        jobs_module.submit(self.jobs, _job("j-2"), "jk-2")
        self._trade("j-2", "t-2")
        self._cancel(job_id="j-2", key="c-2")
        decision, created = dispatch_module.commit(
            self.jobs, self.supply, self.trades, self.dispatch, "j-1",
            "d-1", 25, cancellations=self.cancellations)
        self.assertTrue(created)
        self.assertEqual(decision["job_id"], "j-1")

    # -- concurrency ---------------------------------------------------------

    def test_concurrent_cancel_and_commit_are_mutually_exclusive(
            self) -> None:
        # Across many rounds the same job never gains both a first
        # cancellation and a first dispatch decision.
        for round_index in range(10):
            base = self.tmp.name
            jobs = os.path.join(base, f"jobs-{round_index}.json")
            supply = os.path.join(base, f"supply-{round_index}.json")
            trades = os.path.join(base, f"trades-{round_index}.json")
            dispatch = os.path.join(base, f"dispatch-{round_index}.json")
            cancellations = os.path.join(
                base, f"cancellations-{round_index}.json")
            jobs_module.submit(jobs, _job(), "jk-1")
            resources_module.publish(supply, _resource(), "rk-1")
            clear(jobs, supply, trades, "j-1", "t-1", 10)

            barrier = threading.Barrier(2)
            outcomes: dict[str, object] = {}

            def run_cancel() -> None:
                barrier.wait()
                try:
                    _record, created = cancel(
                        jobs, supply, trades, dispatch, cancellations,
                        "j-1", "c-1", 20, "r")
                    outcomes["cancel"] = created
                except (ValueError, PermissionError) as exc:
                    outcomes["cancel"] = type(exc).__name__

            def run_commit() -> None:
                barrier.wait()
                try:
                    _decision, created = dispatch_module.commit(
                        jobs, supply, trades, dispatch, "j-1", "d-1", 20,
                        cancellations=cancellations)
                    outcomes["commit"] = created
                except (ValueError, PermissionError) as exc:
                    outcomes["commit"] = type(exc).__name__

            threads = [threading.Thread(target=run_cancel),
                       threading.Thread(target=run_commit)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            winners = [name for name, outcome in outcomes.items()
                       if outcome is True]
            losers = [outcome for outcome in outcomes.values()
                      if outcome is not True]
            self.assertEqual(len(winners), 1, f"round {round_index}")
            self.assertEqual(len(losers), 1, f"round {round_index}")
            self.assertIn(losers[0], ("ValueError", "PermissionError"),
                          f"round {round_index}: {outcomes}")
            # Exactly one of the two ledgers exists.
            self.assertEqual(
                Path(cancellations).exists()
                + Path(dispatch).exists(), 1)


if __name__ == "__main__":
    unittest.main()
