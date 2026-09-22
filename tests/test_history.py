from __future__ import annotations

import json
import os
import unittest
from tempfile import TemporaryDirectory

from carbon_market import history
from carbon_market.jobs import register as register_job
from carbon_market.market import match
from carbon_market.migrate import run as migrate
from carbon_market.offers import register as register_offer
from carbon_market.recover_all import run as recover_all
from carbon_market.reserve import run as reserve


def _job(job_id: str = "j-1") -> dict[str, object]:
    return {"job_id": job_id, "deadline": 100, "energy_wh": 100,
            "residency_regions": ["eu-north"]}


def _offer(resource_id: str, carbon_intensity: int) -> dict[str, object]:
    return {"resource_id": resource_id, "region": "eu-north",
            "capacity_wh": 250, "unit_cost": 1000,
            "carbon_intensity": carbon_intensity}


def _event(job_id: str = "j-1", now: int = 50) -> dict[str, object]:
    return {"job_id": job_id, "source_id": "r-1", "target_id": "r-2",
            "op": "commit", "now": now}


def _coord(key: str = "batch-key", owner: str = "owner-a", until: int = 80,
           items: dict[str, list] | None = None) -> dict[str, object]:
    return {"version": 1, "key": key, "owner": owner, "until": until,
            "items": {"j-1": [None, None]} if items is None else items}


def _snapshot(status: str, coord: dict[str, object]) -> list[object]:
    return [status, coord]


def _history_doc(key: str = "batch-key",
                 snapshots: list | None = None) -> dict[str, object]:
    if snapshots is None:
        snapshots = [
            _snapshot("pending",
                      _coord(key=key, items={"j-1": [None, None]})),
            _snapshot("completed",
                      _coord(key=key, until=80,
                             items={"j-1": [_event(), None]})),
        ]
    return {"version": 1, "key": key, "snapshots": snapshots}


class HistoryVerifyTest(unittest.TestCase):
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
        self.path = self.coord + ".history"
        register_job(self.jobs, _job(), "jk")
        register_offer(self.offers, _offer("r-1", 10), "ok1")
        register_offer(self.offers, _offer("r-2", 100), "ok2")
        match(self.jobs, self.offers, self.ledger, "j-1", "mk")
        reserve(self.jobs, self.offers, self.ledger, self.reserves,
                "j-1", "reserve", "rk", 10)
        migrate(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "pk", 20)

    def _write(self, doc: dict[str, object]) -> str:
        path = os.path.join(self.tmp.name, "history.json")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(doc) + "\n")
        return path

    def _run_batch(self, key: str = "batch-key") -> None:
        recover_all(self.jobs, self.offers, self.ledger, self.reserves,
                    self.state, self.coord, "owner-a", key, 50, 30)

    # -- happy path ------------------------------------------------------

    def test_verify_real_history(self) -> None:
        self._run_batch()
        result = history.verify(self.path, "batch-key")
        self.assertEqual(list(result.keys()),
                         ["key", "count", "statuses", "terminal"])
        self.assertEqual(result["key"], "batch-key")
        self.assertEqual(result["count"], 2)
        self.assertEqual(result["statuses"], ["pending", "completed"])
        self.assertIs(result["terminal"], True)
        self.assertIsInstance(result["count"], int)
        self.assertNotIsInstance(result["count"], bool)
        self.assertIsInstance(result["statuses"], list)
        self.assertIsInstance(result["terminal"], bool)

    def test_verify_empty_history_is_not_terminal(self) -> None:
        path = self._write(_history_doc(snapshots=[]))
        result = history.verify(path, "batch-key")
        self.assertEqual(result, {"key": "batch-key", "count": 0,
                                  "statuses": [], "terminal": False})

    def test_verify_pending_only_is_not_terminal(self) -> None:
        pending = _snapshot("pending", _coord(items={"j-1": [None, None]}))
        path = self._write(_history_doc(snapshots=[pending]))
        result = history.verify(path, "batch-key")
        self.assertEqual(result["statuses"], ["pending"])
        self.assertIs(result["terminal"], False)

    def test_verify_failed_is_terminal(self) -> None:
        doc = _history_doc(snapshots=[
            _snapshot("pending", _coord(items={"j-1": [None, None]})),
            _snapshot("failed", _coord(items={"j-1": [None, "Boom"]})),
        ])
        result = history.verify(self._write(doc), "batch-key")
        self.assertEqual(result["statuses"], ["pending", "failed"])
        self.assertIs(result["terminal"], True)

    def test_verify_allows_repeated_status_with_different_coord(self) -> None:
        first = _coord(until=80, items={"j-1": [None, None]})
        second = _coord(until=90, items={"j-1": [None, None]})
        doc = _history_doc(snapshots=[_snapshot("pending", first),
                                      _snapshot("pending", second)])
        result = history.verify(self._write(doc), "batch-key")
        self.assertEqual(result["statuses"], ["pending", "pending"])
        self.assertIs(result["terminal"], False)

    # -- consistency errors ----------------------------------------------

    def test_snapshot_coord_key_must_equal_top_level_key(self) -> None:
        doc = _history_doc()
        doc["snapshots"][0][1]["key"] = "other-key"
        with self.assertRaises(ValueError):
            history.verify(self._write(doc), "batch-key")

    def test_items_must_be_in_code_point_order(self) -> None:
        doc = _history_doc(snapshots=[
            _snapshot("pending",
                      _coord(items={"z": [None, None], "a": [None, None]})),
        ])
        with self.assertRaises(ValueError):
            history.verify(self._write(doc), "batch-key")

    def test_status_must_be_derived_from_items_pending_label(self) -> None:
        doc = _history_doc()
        doc["snapshots"][0][0] = "completed"
        with self.assertRaises(ValueError):
            history.verify(self._write(doc), "batch-key")

    def test_status_must_be_derived_from_items_completed_label(self) -> None:
        doc = _history_doc()
        doc["snapshots"][-1][0] = "pending"
        with self.assertRaises(ValueError):
            history.verify(self._write(doc), "batch-key")

    def test_full_snapshot_must_not_repeat(self) -> None:
        snap = _snapshot("pending", _coord(items={"j-1": [None, None]}))
        doc = _history_doc(snapshots=[snap, json.loads(json.dumps(snap))])
        with self.assertRaises(ValueError):
            history.verify(self._write(doc), "batch-key")

    def test_no_snapshot_after_terminal(self) -> None:
        completed = _snapshot("completed",
                              _coord(items={"j-1": [_event(), None]}))
        after = _snapshot("pending",
                          _coord(until=900, items={"j-1": [None, None]}))
        doc = _history_doc(snapshots=[completed, after])
        with self.assertRaises(ValueError):
            history.verify(self._write(doc), "batch-key")

    # -- structural / encoding errors ------------------------------------

    def test_bad_arguments(self) -> None:
        for bad in ("", 1, None, True, b"k"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    history.verify(self.path, bad)  # type: ignore[arg-type]
                with self.assertRaises(ValueError):
                    history.verify(bad, "batch-key")  # type: ignore[arg-type]

    def test_missing_file_raises_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            history.verify(os.path.join(self.tmp.name, "missing.history"),
                           "batch-key")

    def test_malformed_json_raises_value_error(self) -> None:
        path = self._write(_history_doc())
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("{not json")
        with self.assertRaises(ValueError):
            history.verify(path, "batch-key")

    def test_negative_zero_raises_value_error(self) -> None:
        path = os.path.join(self.tmp.name, "nz.json")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write('{"version":1,"key":"batch-key",'
                         '"snapshots":[["pending",{"version":-0}]]}')
        with self.assertRaises(ValueError):
            history.verify(path, "batch-key")

    def test_bad_utf8_raises_value_error(self) -> None:
        path = os.path.join(self.tmp.name, "badutf8.json")
        with open(path, "wb") as handle:
            handle.write(b'{"version":1,"key":"k","snapshots":[]}\xff')
        with self.assertRaises(ValueError):
            history.verify(path, "k")

    def test_bad_version_raises_value_error(self) -> None:
        doc = _history_doc()
        doc["version"] = 2
        with self.assertRaises(ValueError):
            history.verify(self._write(doc), "batch-key")

    def test_bad_structure_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            history.verify(self._write({"version": 1, "key": "batch-key"}),
                           "batch-key")

    def test_wrong_key_raises_key_error(self) -> None:
        path = self._write(_history_doc(key="real-key"))
        with self.assertRaises(KeyError) as ctx:
            history.verify(path, "claimed-key")
        self.assertEqual(ctx.exception.args[0], "claimed-key")

    # -- shared validation and read-only behaviour -----------------------

    def test_get_shares_consistency_validation(self) -> None:
        doc = _history_doc()
        doc["snapshots"][0][0] = "completed"
        with self.assertRaises(ValueError):
            history.get(self._write(doc), "batch-key")

    def test_get_contract_unchanged_on_real_history(self) -> None:
        self._run_batch()
        page = history.get(self.path, "batch-key")
        self.assertEqual(list(page.keys()), ["key", "snapshots", "next"])
        self.assertEqual(page["key"], "batch-key")
        self.assertIsNone(page["next"])
        self.assertEqual([row[1] for row in page["snapshots"]],
                         ["pending", "completed"])
        self.assertEqual([row[0] for row in page["snapshots"]], [0, 1])
        for row in page["snapshots"]:
            self.assertEqual(
                list(row[2].keys()),
                ["version", "key", "owner", "until", "items"])
        rest = history.get(self.path, "batch-key", cursor=0, count=1)
        self.assertEqual([row[0] for row in rest["snapshots"]], [1])
        self.assertIsNone(rest["next"])
        filtered = history.get(self.path, "batch-key", status="pending")
        self.assertEqual([row[0] for row in filtered["snapshots"]], [0])

    def test_verify_is_read_only(self) -> None:
        self._run_batch()
        before = open(self.path, "rb").read()
        history.verify(self.path, "batch-key")
        after = open(self.path, "rb").read()
        self.assertEqual(after, before)


if __name__ == "__main__":
    unittest.main()
