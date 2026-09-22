from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from carbon_market import recover_all
from carbon_market.history import get
from carbon_market.jobs import register as register_job
from carbon_market.market import match
from carbon_market.migrate import run as migrate
from carbon_market.offers import register as register_offer
from carbon_market.recover_all import run
from carbon_market.reserve import run as reserve


def _job(job_id: str, **overrides: object) -> dict[str, object]:
    job: dict[str, object] = {
        "job_id": job_id,
        "deadline": 100,
        "energy_wh": 100,
        "residency_regions": ["eu-north"],
    }
    job.update(overrides)
    return job


def _offer(resource_id: str, **overrides: object) -> dict[str, object]:
    offer: dict[str, object] = {
        "resource_id": resource_id,
        "region": "eu-north",
        "capacity_wh": 500,
        "unit_cost": 1000,
        "carbon_intensity": 42,
    }
    offer.update(overrides)
    return offer


class HistoryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs = os.path.join(self.tmp.name, "jobs.json")
        self.offers = os.path.join(self.tmp.name, "offers.json")
        self.ledger = os.path.join(self.tmp.name, "ledger.json")
        self.reserves = os.path.join(self.tmp.name, "reserves.json")
        self.state = os.path.join(self.tmp.name, "state.json")
        self.coord = os.path.join(self.tmp.name, "coord.json")
        self.history = self.coord + ".history"

    def _seed(self, *job_ids: str) -> None:
        register_offer(self.offers,
                       _offer("r-1", carbon_intensity=10), "offer-key-1")
        register_offer(self.offers,
                       _offer("r-2", carbon_intensity=100), "offer-key-2")
        for job_id in job_ids:
            register_job(self.jobs, _job(job_id), f"job-key-{job_id}")
            match(self.jobs, self.offers, self.ledger, job_id,
                  f"match-key-{job_id}")
            reserve(self.jobs, self.offers, self.ledger, self.reserves,
                    job_id, "reserve", f"reserve-key-{job_id}", 10)
            migrate(self.jobs, self.offers, self.ledger, self.reserves,
                    self.state, job_id, "r-2", "prepare",
                    f"prepare-key-{job_id}", 20)

    def _run(self, owner: str = "owner-1", key: str = "batch-key",
             now: int = 50, ttl: int = 30) -> tuple[dict[str, object], bool]:
        return run(self.jobs, self.offers, self.ledger, self.reserves,
                   self.state, self.coord, owner, key, now, ttl)

    def _read_history(self) -> dict[str, object]:
        return json.loads(Path(self.history).read_text(encoding="utf-8"))

    # -- snapshot recording -------------------------------------------------

    def test_creation_and_settlements_leave_snapshots(self) -> None:
        self._seed("j-1", "j-2")
        coord, created = self._run()
        self.assertTrue(created)
        history = self._read_history()
        self.assertEqual(list(history.keys()),
                         ["version", "key", "snapshots"])
        self.assertEqual(history["version"], 1)
        self.assertEqual(history["key"], "batch-key")
        snapshots = history["snapshots"]
        # Creation, then one snapshot per settled item.
        self.assertEqual(len(snapshots), 3)
        self.assertEqual([entry[0] for entry in snapshots],
                         ["pending", "pending", "completed"])
        # Each snapshot is the coordination object at that moment.
        self.assertEqual(snapshots[0][1]["items"],
                         {"j-1": [None, None], "j-2": [None, None]})
        self.assertEqual(snapshots[1][1]["items"]["j-2"], [None, None])
        self.assertIsNotNone(snapshots[1][1]["items"]["j-1"][0])
        # The final snapshot equals the returned coordination object.
        self.assertEqual(snapshots[2][1], coord)
        self.assertEqual(snapshots[2][1]["owner"], "owner-1")
        self.assertEqual(snapshots[2][1]["until"], 80)

    def test_snapshot_coords_are_independent_deep_copies(self) -> None:
        self._seed("j-1", "j-2")
        self._run()
        snapshots = self._read_history()["snapshots"]
        # Later settlements must not leak into earlier snapshots.
        self.assertEqual(snapshots[0][1]["items"]["j-1"], [None, None])
        self.assertEqual(snapshots[1][1]["items"]["j-2"], [None, None])
        self.assertEqual(list(snapshots[0][1].keys()),
                         ["version", "key", "owner", "until", "items"])
        self.assertEqual(list(snapshots[0][1]["items"].keys()), ["j-1", "j-2"])

    def test_failed_item_records_failed_status(self) -> None:
        self._seed("j-1")
        # j-2's prepare is recorded under j-1's future recovery subkey, so
        # the batch's recovery of j-1 clashes with the idempotency key.
        register_job(self.jobs, _job("j-2"), "job-key-j-2")
        match(self.jobs, self.offers, self.ledger, "j-2", "match-key-j-2")
        reserve(self.jobs, self.offers, self.ledger, self.reserves,
                "j-2", "reserve", "reserve-key-j-2", 10)
        subkey = "9:batch-keyj-1"
        migrate(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-2", "r-2", "prepare", subkey, 20)
        coord, _ = self._run()
        self.assertEqual(coord["items"]["j-1"][1], "ValueError")
        self.assertIsNotNone(coord["items"]["j-2"][0])
        snapshots = self._read_history()["snapshots"]
        self.assertEqual([entry[0] for entry in snapshots],
                         ["pending", "pending", "failed"])

    def test_takeover_records_snapshot(self) -> None:
        self._seed("j-1")
        # Simulate a crash: the batch is created but the item loop dies
        # before settling anything.
        with mock.patch.object(recover_all._recover, "run",
                               side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self._run(now=50, ttl=5)
        self.assertEqual(len(self._read_history()["snapshots"]), 1)

        coord, created = self._run(owner="owner-2", now=56, ttl=10)
        self.assertFalse(created)
        self.assertEqual(coord["owner"], "owner-2")
        snapshots = self._read_history()["snapshots"]
        self.assertEqual([entry[0] for entry in snapshots],
                         ["pending", "pending", "completed"])
        # The takeover snapshot carries the new owner and lease.
        self.assertEqual(snapshots[1][1]["owner"], "owner-2")
        self.assertEqual(snapshots[1][1]["until"], 66)
        self.assertEqual(snapshots[1][1]["items"]["j-1"], [None, None])

    def test_history_write_failure_stops_coord(self) -> None:
        self._seed("j-1")
        with mock.patch.object(recover_all, "_append_history",
                               side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self._run()
        self.assertFalse(os.path.exists(self.coord))
        self.assertFalse(os.path.exists(self.history))

    # -- re-entry: dedupe and backfill ---------------------------------------

    def test_replay_does_not_duplicate_snapshots(self) -> None:
        self._seed("j-1")
        self._run()
        before = Path(self.history).read_bytes()
        coord, created = self._run()
        self.assertFalse(created)
        self.assertEqual(Path(self.history).read_bytes(), before)

    def test_reentry_after_crash_dedupes_identical_states(self) -> None:
        self._seed("j-1")
        with mock.patch.object(recover_all._recover, "run",
                               side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self._run(now=50, ttl=5)
        # Same owner, same lease: the recomputed states are identical to
        # the ones already recorded and must not be duplicated.
        self._run(now=50, ttl=5)
        snapshots = self._read_history()["snapshots"]
        self.assertEqual(len(snapshots), 2)
        self.assertEqual([entry[0] for entry in snapshots],
                         ["pending", "completed"])

    def test_reentry_backfills_missing_history(self) -> None:
        self._seed("j-1")
        coord, _ = self._run()
        os.unlink(self.history)
        replay, created = self._run()
        self.assertFalse(created)
        self.assertEqual(replay, coord)
        history = self._read_history()
        self.assertEqual(history["key"], "batch-key")
        self.assertEqual(history["snapshots"], [["completed", coord]])

    def test_history_key_conflict_raises_value_error(self) -> None:
        self._seed("j-1")
        self._run(key="batch-key")
        # A stale history under another key conflicts with a new batch.
        os.unlink(self.coord)
        with self.assertRaises(ValueError):
            self._run(key="other-key")

    def test_history_file_format(self) -> None:
        self._seed("j-1")
        self._run(key="键-批次")
        raw = Path(self.history).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        text = raw.decode("utf-8")
        self.assertNotIn(" ", text.split("\n", 1)[0])
        self.assertIn("键-批次", text)
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    # -- history.get ---------------------------------------------------------

    def test_get_returns_full_page_by_default(self) -> None:
        self._seed("j-1", "j-2")
        self._run()
        result = get(self.history, "batch-key")
        self.assertEqual(list(result.keys()), ["key", "snapshots", "next"])
        self.assertEqual(result["key"], "batch-key")
        self.assertIsNone(result["next"])
        snapshots = result["snapshots"]
        self.assertEqual([entry[0] for entry in snapshots], [0, 1, 2])
        self.assertEqual([entry[1] for entry in snapshots],
                         ["pending", "pending", "completed"])
        self.assertEqual(snapshots[2][2]["items"]["j-1"][0]["op"], "commit")

    def test_get_paginates_with_cursor(self) -> None:
        self._seed("j-1", "j-2")
        self._run()
        first = get(self.history, "batch-key", count=2)
        self.assertEqual([entry[0] for entry in first["snapshots"]], [0, 1])
        self.assertEqual(first["next"], 1)
        second = get(self.history, "batch-key", cursor=first["next"],
                     count=2)
        self.assertEqual([entry[0] for entry in second["snapshots"]], [2])
        self.assertIsNone(second["next"])
        # A cursor at or past the last index sees nothing.
        empty = get(self.history, "batch-key", cursor=2)
        self.assertEqual(empty["snapshots"], [])
        self.assertIsNone(empty["next"])

    def test_get_next_points_at_page_end_only_when_more_matches(self) -> None:
        self._seed("j-1", "j-2")
        self._run()
        # Exactly two pending snapshots exist; the page ends exactly at
        # the last match, so there is no continuation.
        result = get(self.history, "batch-key", count=2, status="pending")
        self.assertEqual([entry[0] for entry in result["snapshots"]], [0, 1])
        self.assertIsNone(result["next"])
        result = get(self.history, "batch-key", count=1, status="pending")
        self.assertEqual(result["next"], 0)

    def test_get_filters_by_status(self) -> None:
        self._seed("j-1")
        register_job(self.jobs, _job("j-2"), "job-key-j-2")
        match(self.jobs, self.offers, self.ledger, "j-2", "match-key-j-2")
        reserve(self.jobs, self.offers, self.ledger, self.reserves,
                "j-2", "reserve", "reserve-key-j-2", 10)
        migrate(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-2", "r-2", "prepare", "9:batch-keyj-1", 20)
        self._run()
        failed = get(self.history, "batch-key", status="failed")
        self.assertEqual([entry[0] for entry in failed["snapshots"]], [2])
        self.assertEqual(failed["snapshots"][0][1], "failed")
        completed = get(self.history, "batch-key", status="completed")
        self.assertEqual(completed["snapshots"], [])
        # Filtering combines with the cursor.
        pending = get(self.history, "batch-key", status="pending", cursor=0)
        self.assertEqual([entry[0] for entry in pending["snapshots"]], [1])

    def test_get_never_writes(self) -> None:
        self._seed("j-1")
        self._run()
        before = Path(self.history).read_bytes()
        get(self.history, "batch-key")
        self.assertEqual(Path(self.history).read_bytes(), before)
        missing = os.path.join(self.tmp.name, "missing.history")
        with self.assertRaises(FileNotFoundError):
            get(missing, "batch-key")
        self.assertFalse(os.path.exists(missing))

    def test_get_invalid_arguments(self) -> None:
        self._seed("j-1")
        self._run()
        for bad in ("", 123, None):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    get(bad, "batch-key")  # type: ignore[arg-type]
                with self.assertRaises(ValueError):
                    get(self.history, bad)  # type: ignore[arg-type]
        for bad_cursor in (-2, -100, True, False, 1.5, "0", None):
            with self.subTest(bad_cursor=bad_cursor):
                with self.assertRaises(ValueError):
                    get(self.history, "batch-key",
                        cursor=bad_cursor)  # type: ignore[arg-type]
        for bad_count in (0, -1, 1001, True, False, 1.5, "1", None):
            with self.subTest(bad_count=bad_count):
                with self.assertRaises(ValueError):
                    get(self.history, "batch-key",
                        count=bad_count)  # type: ignore[arg-type]
        for bad_status in ("", "done", "PENDING", 1, True):
            with self.subTest(bad_status=bad_status):
                with self.assertRaises(ValueError):
                    get(self.history, "batch-key",
                        status=bad_status)  # type: ignore[arg-type]
        # Boundary values are accepted.
        get(self.history, "batch-key", cursor=-1, count=1)
        get(self.history, "batch-key", count=1000)
        get(self.history, "batch-key", status=None)

    def test_get_key_mismatch_raises_key_error(self) -> None:
        self._seed("j-1")
        self._run()
        with self.assertRaises(KeyError):
            get(self.history, "other-key")

    def test_get_malformed_history_raises_value_error(self) -> None:
        self._seed("j-1")
        self._run()
        for bad in ("{not json",
                     '{"version":2,"key":"batch-key","snapshots":[]}',
                     '{"version":1,"key":"batch-key"}',
                     '{"version":1,"key":"batch-key","snapshots":{}}',
                     '{"version":1,"key":"batch-key","snapshots":[["done",'
                     '{}]]}',
                     '{"version":1,"key":"batch-key","snapshots":[]}extra'):
            with self.subTest(bad=bad):
                Path(self.history).write_text(bad, encoding="utf-8")
                with self.assertRaises(ValueError):
                    get(self.history, "batch-key")

    def test_get_negative_zero_literal_raises_value_error(self) -> None:
        self._seed("j-1")
        self._run()
        text = Path(self.history).read_text(encoding="utf-8")
        Path(self.history).write_text(text.replace('"until":80',
                                                   '"until":-0'),
                                      encoding="utf-8")
        with self.assertRaises(ValueError):
            get(self.history, "batch-key")


if __name__ == "__main__":
    unittest.main()
