from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market.jobs import register as register_job
from carbon_market.market import match
from carbon_market.migrate import run as migrate
from carbon_market.offers import register as register_offer
from carbon_market.recover_all import run
from carbon_market.reserve import run as reserve


def _job(job_id: str = "j-1") -> dict[str, object]:
    return {"job_id": job_id, "deadline": 100, "energy_wh": 100,
            "residency_regions": ["eu-north"]}


def _offer(resource_id: str = "r-1", carbon_intensity: int = 42) -> dict[str, object]:
    return {"resource_id": resource_id, "region": "eu-north",
            "capacity_wh": 250, "unit_cost": 1000,
            "carbon_intensity": carbon_intensity}


class RecoverAllTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        t = self.tmp.name
        self.jobs = os.path.join(t, "jobs.json")
        self.offers = os.path.join(t, "offers.json")
        self.ledger = os.path.join(t, "ledger.json")
        self.reserves = os.path.join(t, "reserves.json")
        self.state = os.path.join(t, "state.json")
        self.coord = os.path.join(t, "coord.json")
        self.history = self.coord + ".history"
        register_job(self.jobs, _job(), "jk")
        register_offer(self.offers, _offer("r-1", 10), "ok1")
        register_offer(self.offers, _offer("r-2", 100), "ok2")
        match(self.jobs, self.offers, self.ledger, "j-1", "mk")
        reserve(self.jobs, self.offers, self.ledger, self.reserves,
                "j-1", "reserve", "rk", 10)
        migrate(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "pk", 20)

    def _run(self, owner: str = "owner-a", key: str = "batch-key",
             now: int = 50, ttl: int = 30):
        return run(self.jobs, self.offers, self.ledger, self.reserves,
                   self.state, self.coord, owner, key, now, ttl)

    def _history(self) -> dict[str, object]:
        return json.loads(Path(self.history).read_text(encoding="utf-8"))

    # -- creation and shape ----------------------------------------------

    def test_creation_writes_coord_and_two_snapshots(self) -> None:
        coord, created = self._run()
        self.assertTrue(created)
        self.assertEqual(list(coord.keys()),
                         ["version", "key", "owner", "until", "items"])
        self.assertEqual(coord["version"], 1)
        self.assertEqual(coord["key"], "batch-key")
        self.assertEqual(coord["owner"], "owner-a")
        self.assertEqual(coord["until"], 80)
        self.assertEqual(list(coord["items"].keys()), ["j-1"])
        event, error = coord["items"]["j-1"]
        self.assertIsNone(error)
        self.assertEqual(event, {"job_id": "j-1", "source_id": "r-1",
                                 "target_id": "r-2", "op": "commit", "now": 50})

        raw = Path(self.coord).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        history = self._history()
        self.assertEqual(list(history.keys()),
                         ["version", "key", "snapshots"])
        self.assertEqual([s[0] for s in history["snapshots"]],
                         ["pending", "completed"])
        for status, snap in history["snapshots"]:
            self.assertEqual(list(snap.keys()),
                             ["version", "key", "owner", "until", "items"])
        # The final snapshot is a deep copy of the persisted coord.
        self.assertEqual(history["snapshots"][-1][1],
                         json.loads(raw.decode("utf-8")))

    # -- replay and re-entry idempotency ---------------------------------

    def test_terminal_replay_writes_nothing_and_keeps_lease(self) -> None:
        first, c1 = self._run()
        self.assertTrue(c1)
        before_coord = Path(self.coord).read_bytes()
        before_mtime = os.stat(self.history).st_mtime_ns
        replay, c2 = self._run(owner="owner-b", now=999, ttl=7)
        self.assertFalse(c2)
        self.assertEqual(replay, first)
        self.assertEqual(replay["until"], 80)  # no lease renewal
        self.assertEqual(Path(self.coord).read_bytes(), before_coord)
        self.assertEqual(os.stat(self.history).st_mtime_ns, before_mtime)
        self.assertEqual(len(self._history()["snapshots"]), 2)

    def test_missing_history_next_to_terminal_coord_is_backfilled_once(self) -> None:
        coord, created = self._run()
        self.assertTrue(created)
        on_disk = json.loads(Path(self.coord).read_text(encoding="utf-8"))
        os.unlink(self.history)

        backfilled, created2 = self._run()
        self.assertFalse(created2)
        self.assertEqual(backfilled, coord)
        history = self._history()
        self.assertEqual(history["version"], 1)
        self.assertEqual(history["key"], "batch-key")
        self.assertEqual(len(history["snapshots"]), 1)
        status, snap = history["snapshots"][0]
        self.assertEqual(status, "completed")
        self.assertEqual(snap, on_disk)
        # coord itself is never rewritten by a backfill
        self.assertEqual(json.loads(Path(self.coord).read_text(encoding="utf-8")),
                         on_disk)

        mtime = os.stat(self.history).st_mtime_ns
        self._run()
        self.assertEqual(os.stat(self.history).st_mtime_ns, mtime)
        self.assertEqual(len(self._history()["snapshots"]), 1)

    def test_history_without_terminal_snapshot_gets_only_terminal(self) -> None:
        self._run()
        on_disk = json.loads(Path(self.coord).read_text(encoding="utf-8"))
        Path(self.history).write_text(
            json.dumps({"version": 1, "key": "batch-key", "snapshots": []})
            + "\n", encoding="utf-8")
        self._run()
        history = self._history()
        self.assertEqual([s[0] for s in history["snapshots"]], ["completed"])
        self.assertEqual(history["snapshots"][0][1], on_disk)
        self.assertEqual(json.loads(Path(self.coord).read_text(encoding="utf-8")),
                         on_disk)

    # -- ownership and takeover ------------------------------------------

    def test_other_owner_before_expiry_raises_permission_error(self) -> None:
        self._run(now=50, ttl=10)
        # Reopen the batch as pending under owner-a's live lease.
        data = json.loads(Path(self.coord).read_text(encoding="utf-8"))
        data["items"]["j-1"] = [None, None]
        Path(self.coord).write_text(json.dumps(data) + "\n", encoding="utf-8")
        with self.assertRaises(PermissionError):
            self._run(owner="owner-b", now=55, ttl=10)

    def test_takeover_after_expiry_records_takeover_snapshot(self) -> None:
        self._run(now=50, ttl=10)
        data = json.loads(Path(self.coord).read_text(encoding="utf-8"))
        data["items"]["j-1"] = [None, None]
        data["until"] = 40
        Path(self.coord).write_text(json.dumps(data) + "\n", encoding="utf-8")
        Path(self.history).write_text(
            json.dumps({"version": 1, "key": "batch-key", "snapshots": []})
            + "\n", encoding="utf-8")

        coord, created = self._run(owner="owner-b", now=50, ttl=30)
        self.assertFalse(created)
        self.assertEqual(coord["owner"], "owner-b")
        self.assertEqual(coord["until"], 80)
        statuses = [s[0] for s in self._history()["snapshots"]]
        self.assertEqual(statuses, ["pending", "completed"])
        self.assertEqual(self._history()["snapshots"][0][1]["owner"], "owner-b")

    # -- argument and file errors ----------------------------------------

    def test_invalid_arguments(self) -> None:
        good = [self.jobs, self.offers, self.ledger, self.reserves,
                self.state, self.coord, "o", "k", 50, 30]
        for index in range(8):
            for bad in ("", 123, None):
                args = list(good)
                args[index] = bad
                with self.subTest(index=index, bad=bad):
                    with self.assertRaises(ValueError):
                        run(*args)  # type: ignore[arg-type]
        for bad_now in (-1, True, False, 1.5, "10", None):
            with self.subTest(bad_now=bad_now):
                with self.assertRaises(ValueError):
                    run(self.jobs, self.offers, self.ledger, self.reserves,
                        self.state, self.coord, "o", "k", bad_now, 30)  # type: ignore[arg-type]
        for bad_ttl in (0, -1, True, False, 1.5, "30", None):
            with self.subTest(bad_ttl=bad_ttl):
                with self.assertRaises(ValueError):
                    run(self.jobs, self.offers, self.ledger, self.reserves,
                        self.state, self.coord, "o", "k", 50, bad_ttl)  # type: ignore[arg-type]

    def test_different_key_on_existing_coord_raises_value_error(self) -> None:
        self._run()
        with self.assertRaises(ValueError):
            self._run(key="other-key")

    def test_missing_input_raises_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            run(os.path.join(self.tmp.name, "nope.json"), self.offers,
                self.ledger, self.reserves, self.state, self.coord,
                "o", "k", 50, 30)

    def test_corrupt_history_raises_value_error(self) -> None:
        self._run()
        Path(self.history).write_text("{not json", encoding="utf-8")
        with self.assertRaises(ValueError):
            self._run()

    def test_corrupt_coord_raises_value_error(self) -> None:
        self._run()
        Path(self.coord).write_text('{"version":2}', encoding="utf-8")
        with self.assertRaises(ValueError):
            self._run()

    def test_history_key_mismatch_raises_value_error(self) -> None:
        self._run()
        history = self._history()
        history["key"] = "different-key"
        Path(self.history).write_text(json.dumps(history) + "\n",
                                      encoding="utf-8")
        with self.assertRaises(ValueError):
            self._run()


if __name__ == "__main__":
    unittest.main()
