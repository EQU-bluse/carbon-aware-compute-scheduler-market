from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import dispatch as dispatch_module
from carbon_market.dispatch import claim, commit, finish, recover
from carbon_market.jobs import submit
from carbon_market.market import clear
from carbon_market.resources import publish


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


class DispatchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs = os.path.join(self.tmp.name, "jobs.json")
        self.supply = os.path.join(self.tmp.name, "supply.json")
        self.trades = os.path.join(self.tmp.name, "clear.json")
        self.ledger = os.path.join(self.tmp.name, "dispatch.json")

    def _seed(self, job: dict[str, object] | None = None,
              resource: dict[str, object] | None = None) -> None:
        submit(self.jobs, job if job is not None else _job(), "jk-1")
        publish(self.supply,
                resource if resource is not None else _resource(), "rk-1")

    def _seed_trade(self, at: int = 50) -> None:
        self._seed()
        clear(self.jobs, self.supply, self.trades, "j-1", "ck-1", at)

    def _commit(self, key: str = "k-1", at: int = 60):
        return commit(self.jobs, self.supply, self.trades, self.ledger,
                      "j-1", key, at)

    # -- commit ---------------------------------------------------------

    def test_commit_creates_ledger_and_returns_decision(self) -> None:
        self._seed_trade()
        decision, created = self._commit()
        self.assertTrue(created)
        self.assertEqual(decision, {
            "job_id": "j-1",
            "resource_id": "r-1",
            "version": 1,
            "deadline": 100,
            "state": "ready",
            "attempts": 0,
            "owner": None,
            "lease_end": None,
        })
        self.assertEqual(list(decision.keys()),
                         ["job_id", "resource_id", "version", "deadline",
                          "state", "attempts", "owner", "lease_end"])

        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()), ["version", "decisions", "audit"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(data["decisions"], {"j-1": decision})
        event = data["audit"]["k-1"]
        self.assertEqual(list(event.keys()),
                         ["key", "action", "job_id", "owner", "result",
                          "at", "lease", "decision"])
        self.assertEqual(event, {
            "key": "k-1", "action": "commit", "job_id": "j-1",
            "owner": None, "result": None, "at": 60, "lease": None,
            "decision": decision,
        })

    def test_commit_compact_utf8_and_sorted_keys(self) -> None:
        submit(self.jobs, _job("j-b"), "jk-b")
        submit(self.jobs, _job("j-a"), "jk-a")
        publish(self.supply, _resource("r-中"), "rk")
        clear(self.jobs, self.supply, self.trades, "j-b", "ck-b", 50)
        clear(self.jobs, self.supply, self.trades, "j-a", "ck-a", 50)
        commit(self.jobs, self.supply, self.trades, self.ledger,
               "j-b", "键-b", 60)
        commit(self.jobs, self.supply, self.trades, self.ledger,
               "j-a", "键-a", 60)
        raw = Path(self.ledger).read_text(encoding="utf-8")
        self.assertIn("r-中", raw)
        self.assertIn("键-b", raw)
        self.assertNotIn(" ", raw.split("\n", 1)[0])
        data = json.loads(raw)
        self.assertEqual(list(data["decisions"].keys()), ["j-a", "j-b"])
        self.assertEqual(list(data["audit"].keys()), ["键-a", "键-b"])

    def test_commit_replay_returns_current_decision_without_writing(
            self) -> None:
        self._seed_trade()
        first, created_first = self._commit()
        raw = Path(self.ledger).read_bytes()
        replay, created_replay = self._commit()
        self.assertTrue(created_first)
        self.assertFalse(created_replay)
        self.assertEqual(replay, first)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        # A replay after later transitions returns the current record.
        claim(self.ledger, "j-1", "w-1", 10, "k-claim", 70)
        again, created_again = self._commit()
        self.assertFalse(created_again)
        self.assertEqual(again["state"], "claimed")

    def test_commit_same_key_changed_request_raises(self) -> None:
        self._seed_trade()
        submit(self.jobs, _job("j-2"), "jk-2")
        clear(self.jobs, self.supply, self.trades, "j-2", "ck-2", 50)
        self._commit()
        raw = Path(self.ledger).read_bytes()
        with self.assertRaises(ValueError):
            commit(self.jobs, self.supply, self.trades, self.ledger,
                   "j-2", "k-1", 60)
        with self.assertRaises(ValueError):
            commit(self.jobs, self.supply, self.trades, self.ledger,
                   "j-1", "k-1", 61)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)

    def test_commit_job_committed_under_other_key_raises(self) -> None:
        self._seed_trade()
        self._commit()
        with self.assertRaises(ValueError):
            self._commit(key="k-2")

    def test_commit_unknown_job_raises_key_error_without_ledger(self) -> None:
        self._seed_trade()
        with self.assertRaises(KeyError):
            commit(self.jobs, self.supply, self.trades, self.ledger,
                   "ghost", "k", 60)
        self.assertFalse(os.path.exists(self.ledger))

    def test_commit_without_trade_raises_lookup_error_without_ledger(
            self) -> None:
        self._seed()
        clear(self.jobs, self.supply, self.trades, "j-1", "ck-1", 50)
        submit(self.jobs, _job("j-2"), "jk-2")
        with self.assertRaises(LookupError):
            commit(self.jobs, self.supply, self.trades, self.ledger,
                   "j-2", "k", 60)
        self.assertFalse(os.path.exists(self.ledger))

    def test_commit_past_deadline_raises_timeout_without_ledger(self) -> None:
        self._seed_trade()
        with self.assertRaises(TimeoutError):
            self._commit(at=101)
        self.assertFalse(os.path.exists(self.ledger))
        # The deadline itself is still inside the window.
        decision, created = self._commit(at=100)
        self.assertTrue(created)
        self.assertEqual(decision["state"], "ready")

    def test_commit_missing_inputs_raise_file_not_found(self) -> None:
        self._seed_trade()
        missing = os.path.join(self.tmp.name, "missing.json")
        for paths in ((missing, self.supply, self.trades),
                      (self.jobs, missing, self.trades),
                      (self.jobs, self.supply, missing)):
            with self.subTest(paths=paths):
                with self.assertRaises(FileNotFoundError):
                    commit(paths[0], paths[1], paths[2], self.ledger,
                           "j-1", "k", 60)
        with self.assertRaises(FileNotFoundError):
            commit(self.jobs, self.supply, self.trades,
                   os.path.join(self.tmp.name, "no-such-dir", "d.json"),
                   "j-1", "k", 60)
        self.assertFalse(os.path.exists(self.ledger))

    def test_commit_paths_must_be_distinct_real_locations(self) -> None:
        self._seed_trade()
        for paths in ((self.jobs, self.supply, self.trades, self.jobs),
                      (self.jobs, self.supply, self.trades, self.supply),
                      (self.jobs, self.supply, self.trades, self.trades),
                      (self.jobs, self.jobs, self.trades, self.ledger),
                      (os.path.join(self.tmp.name, ".", "jobs.json"),
                       self.supply, self.trades, self.jobs)):
            with self.subTest(paths=paths):
                with self.assertRaises(ValueError):
                    commit(paths[0], paths[1], paths[2], paths[3],
                           "j-1", "k", 60)

    def test_commit_invalid_arguments_before_files_are_read(self) -> None:
        self._seed_trade()
        good = (self.jobs, self.supply, self.trades, self.ledger, "j-1",
                "k", 60)
        for index in range(6):
            for bad in ("", 123, None):
                args = list(good)
                args[index] = bad
                with self.subTest(index=index, bad=bad):
                    with self.assertRaises(ValueError):
                        commit(*args)  # type: ignore[arg-type]
        for bad in (-1, True, False, 1.5, None, ""):
            with self.subTest(at=bad):
                with self.assertRaises(ValueError):
                    commit(self.jobs, self.supply, self.trades, self.ledger,
                           "j-1", "k", bad)  # type: ignore[arg-type]

    # -- claim ----------------------------------------------------------

    def test_claim_moves_ready_decision_to_claimed(self) -> None:
        self._seed_trade()
        self._commit()
        decision, created = claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        self.assertTrue(created)
        self.assertEqual(decision["state"], "claimed")
        self.assertEqual(decision["attempts"], 1)
        self.assertEqual(decision["owner"], "w-1")
        self.assertEqual(decision["lease_end"], 80)
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        event = data["audit"]["k-c"]
        self.assertEqual(event["action"], "claim")
        self.assertEqual(event["owner"], "w-1")
        self.assertEqual(event["lease"], 10)
        self.assertEqual(event["at"], 70)
        self.assertEqual(event["decision"], decision)

    def test_claim_replay_and_changed_request(self) -> None:
        self._seed_trade()
        self._commit()
        first, _ = claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        raw = Path(self.ledger).read_bytes()
        replay, created = claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        self.assertFalse(created)
        self.assertEqual(replay, first)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        for args in (("j-1", "w-2", 10, "k-c", 70),
                     ("j-1", "w-1", 11, "k-c", 70),
                     ("j-1", "w-1", 10, "k-c", 71)):
            with self.subTest(args=args):
                with self.assertRaises(ValueError):
                    claim(self.ledger, *args)

    def test_claim_rejects_claimed_and_succeeded_states(self) -> None:
        self._seed_trade()
        self._commit()
        claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        with self.assertRaises(ValueError):
            claim(self.ledger, "j-1", "w-2", 10, "k-c2", 71)
        finish(self.ledger, "j-1", "w-1", "succeeded", "k-f", 75)
        with self.assertRaises(ValueError):
            claim(self.ledger, "j-1", "w-2", 10, "k-c3", 76)

    def test_claim_lease_end_must_not_cross_deadline(self) -> None:
        self._seed_trade()
        self._commit()
        with self.assertRaises(TimeoutError):
            claim(self.ledger, "j-1", "w-1", 41, "k-c", 60)
        with self.assertRaises(TimeoutError):
            claim(self.ledger, "j-1", "w-1", 1, "k-c2", 100)
        decision, created = claim(self.ledger, "j-1", "w-1", 40, "k-c3", 60)
        self.assertTrue(created)
        self.assertEqual(decision["lease_end"], 100)

    def test_claim_accepts_failed_decision_and_counts_attempts(self) -> None:
        self._seed_trade()
        self._commit()
        claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        finish(self.ledger, "j-1", "w-1", "failed", "k-f", 75)
        decision, created = claim(self.ledger, "j-1", "w-2", 10, "k-c2", 80)
        self.assertTrue(created)
        self.assertEqual(decision["state"], "claimed")
        self.assertEqual(decision["attempts"], 2)
        self.assertEqual(decision["owner"], "w-2")

    def test_claim_invalid_arguments(self) -> None:
        self._seed_trade()
        self._commit()
        for bad in ("", 0, -1, True, 1.5, None):
            with self.subTest(lease=bad):
                with self.assertRaises(ValueError):
                    claim(self.ledger, "j-1", "w-1", bad, "k", 70)  # type: ignore[arg-type]
        for bad in ("", 0, None):
            with self.subTest(owner=bad):
                with self.assertRaises(ValueError):
                    claim(self.ledger, "j-1", bad, 10, "k", 70)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            claim(self.ledger, "j-1", "w-1", 10, "k", -1)

    # -- finish ---------------------------------------------------------

    def test_finish_succeeded_clears_owner_and_lease(self) -> None:
        self._seed_trade()
        self._commit()
        claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        decision, created = finish(self.ledger, "j-1", "w-1", "succeeded",
                                   "k-f", 75)
        self.assertTrue(created)
        self.assertEqual(decision["state"], "succeeded")
        self.assertEqual(decision["attempts"], 1)
        self.assertIsNone(decision["owner"])
        self.assertIsNone(decision["lease_end"])
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        event = data["audit"]["k-f"]
        self.assertEqual(event["action"], "finish")
        self.assertEqual(event["result"], "succeeded")
        self.assertEqual(event["decision"], decision)

    def test_finish_replay_and_changed_request(self) -> None:
        self._seed_trade()
        self._commit()
        claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        first, _ = finish(self.ledger, "j-1", "w-1", "failed", "k-f", 75)
        raw = Path(self.ledger).read_bytes()
        replay, created = finish(self.ledger, "j-1", "w-1", "failed",
                                 "k-f", 75)
        self.assertFalse(created)
        self.assertEqual(replay, first)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        for args in (("j-1", "w-2", "failed", "k-f", 75),
                     ("j-1", "w-1", "succeeded", "k-f", 75),
                     ("j-1", "w-1", "failed", "k-f", 76)):
            with self.subTest(args=args):
                with self.assertRaises(ValueError):
                    finish(self.ledger, *args)

    def test_finish_requires_claimed_state(self) -> None:
        self._seed_trade()
        self._commit()
        with self.assertRaises(ValueError):
            finish(self.ledger, "j-1", "w-1", "succeeded", "k-f", 70)

    def test_finish_wrong_owner_raises_permission_error(self) -> None:
        self._seed_trade()
        self._commit()
        claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        with self.assertRaises(PermissionError):
            finish(self.ledger, "j-1", "w-2", "succeeded", "k-f", 75)

    def test_finish_past_lease_end_raises_timeout(self) -> None:
        self._seed_trade()
        self._commit()
        claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        with self.assertRaises(TimeoutError):
            finish(self.ledger, "j-1", "w-1", "succeeded", "k-f", 81)
        decision, created = finish(self.ledger, "j-1", "w-1", "succeeded",
                                   "k-f2", 80)
        self.assertTrue(created)
        self.assertEqual(decision["state"], "succeeded")

    def test_finish_invalid_result_raises_value_error(self) -> None:
        self._seed_trade()
        self._commit()
        claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        for bad in ("ready", "claimed", "", "ok", None, 1, True):
            with self.subTest(result=bad):
                with self.assertRaises(ValueError):
                    finish(self.ledger, "j-1", "w-1", bad, "k-f", 75)  # type: ignore[arg-type]

    # -- recover --------------------------------------------------------

    def test_recover_returns_expired_claim_to_ready(self) -> None:
        self._seed_trade()
        self._commit()
        claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        decision, created = recover(self.ledger, "j-1", "k-r", 81)
        self.assertTrue(created)
        self.assertEqual(decision["state"], "ready")
        self.assertEqual(decision["attempts"], 1)  # no attempt consumed
        self.assertIsNone(decision["owner"])
        self.assertIsNone(decision["lease_end"])
        # The recovered decision can be claimed again.
        again, _ = claim(self.ledger, "j-1", "w-2", 5, "k-c2", 90)
        self.assertEqual(again["attempts"], 2)

    def test_recover_replay_and_changed_request(self) -> None:
        self._seed_trade()
        self._commit()
        claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        first, _ = recover(self.ledger, "j-1", "k-r", 81)
        raw = Path(self.ledger).read_bytes()
        replay, created = recover(self.ledger, "j-1", "k-r", 81)
        self.assertFalse(created)
        self.assertEqual(replay, first)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        with self.assertRaises(ValueError):
            recover(self.ledger, "j-1", "k-r", 82)

    def test_recover_requires_claimed_state(self) -> None:
        self._seed_trade()
        self._commit()
        with self.assertRaises(ValueError):
            recover(self.ledger, "j-1", "k-r", 70)

    def test_recover_before_strict_expiry_raises_permission_error(
            self) -> None:
        self._seed_trade()
        self._commit()
        claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        with self.assertRaises(PermissionError):
            recover(self.ledger, "j-1", "k-r", 80)  # lease end exactly
        with self.assertRaises(PermissionError):
            recover(self.ledger, "j-1", "k-r2", 75)

    # -- shared behavior --------------------------------------------------

    def test_actions_on_missing_ledger_raise_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            claim(self.ledger, "j-1", "w-1", 10, "k", 70)
        with self.assertRaises(FileNotFoundError):
            finish(self.ledger, "j-1", "w-1", "succeeded", "k", 70)
        with self.assertRaises(FileNotFoundError):
            recover(self.ledger, "j-1", "k", 70)
        self.assertFalse(os.path.exists(self.ledger))

    def test_unknown_job_raises_key_error(self) -> None:
        self._seed_trade()
        self._commit()
        with self.assertRaises(KeyError):
            claim(self.ledger, "ghost", "w-1", 10, "k", 70)
        with self.assertRaises(KeyError):
            finish(self.ledger, "ghost", "w-1", "succeeded", "k", 70)
        with self.assertRaises(KeyError):
            recover(self.ledger, "ghost", "k", 70)

    def test_same_key_across_actions_raises_value_error(self) -> None:
        self._seed_trade()
        self._commit(key="k")
        with self.assertRaises(ValueError):
            claim(self.ledger, "j-1", "w-1", 10, "k", 70)
        with self.assertRaises(ValueError):
            finish(self.ledger, "j-1", "w-1", "succeeded", "k", 70)
        with self.assertRaises(ValueError):
            recover(self.ledger, "j-1", "k", 70)

    def test_full_lifecycle(self) -> None:
        self._seed_trade()
        self._commit()
        claim(self.ledger, "j-1", "w-1", 10, "k1", 70)
        finish(self.ledger, "j-1", "w-1", "failed", "k2", 75)
        claim(self.ledger, "j-1", "w-2", 10, "k3", 80)
        decision, _ = finish(self.ledger, "j-1", "w-2", "succeeded",
                             "k4", 85)
        self.assertEqual(decision["state"], "succeeded")
        self.assertEqual(decision["attempts"], 2)
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual([e["action"] for e in data["audit"].values()],
                         ["commit", "claim", "finish", "claim", "finish"])

    def test_invalid_ledger_structures_raise_value_error(self) -> None:
        self._seed_trade()
        decision, _ = self._commit()
        event = {"key": "k-1", "action": "commit", "job_id": "j-1",
                 "owner": None, "result": None, "at": 60, "lease": None,
                 "decision": decision}
        bad_payloads = [
            [],
            {},
            {"version": 1, "decisions": {}, "audit": {}, "extra": 1},
            {"version": 2, "decisions": {}, "audit": {}},
            {"version": True, "decisions": {}, "audit": {}},
            {"version": 1, "decisions": [], "audit": {}},
            {"version": 1, "decisions": {"j-1": decision}, "audit": {}},
            {"version": 1,
             "decisions": {"j-1": dict(decision, state="bogus")},
             "audit": {"k-1": event}},
            {"version": 1,
             "decisions": {"j-1": dict(decision, deadline=101)},
             "audit": {"k-1": event}},
            {"version": 1,
             "decisions": {"j-1": dict(decision, owner="w")},
             "audit": {"k-1": event}},
            {"version": 1,
             "decisions": {"j-1": dict(decision, resource_id="ghost")},
             "audit": {"k-1": dict(event,
                                   decision=dict(decision,
                                                 resource_id="ghost"))}},
            {"version": 1, "decisions": {"j-1": decision},
             "audit": {"k-1": dict(event, key="other")}},
            {"version": 1, "decisions": {"j-1": decision},
             "audit": {"k-1": dict(event, at=61)}},
            {"version": 1, "decisions": {"j-1": decision},
             "audit": {"k-1": dict(event, decision=dict(decision,
                                                        attempts=1))}},
        ]
        for payload in bad_payloads:
            with self.subTest(payload=payload):
                Path(self.ledger).write_bytes(
                    json.dumps(payload).encode("utf-8"))
                with self.assertRaises(ValueError):
                    claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)

    def test_non_canonical_ledger_raises_value_error(self) -> None:
        self._seed_trade()
        self._commit()
        original = Path(self.ledger).read_bytes()
        data = json.loads(original)
        variants = [
            json.dumps(data, indent=2),
            json.dumps(data, ensure_ascii=True),
            json.dumps(data, separators=(", ", ": ")),
            json.dumps(data, ensure_ascii=False) + " \n",
            json.dumps(data, ensure_ascii=False)[:-1],
            json.dumps(data, ensure_ascii=False) + "\n\n",
        ]
        for text in variants:
            Path(self.ledger).write_text(text, encoding="utf-8")
            with self.subTest(text=text[:20]):
                with self.assertRaises(ValueError):
                    claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        Path(self.ledger).write_bytes(original)
        decision, created = claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        self.assertTrue(created)
        self.assertEqual(decision["state"], "claimed")

    def test_negative_zero_and_non_finite_rejected(self) -> None:
        self._seed_trade()
        self._commit()
        raw = Path(self.ledger).read_text(encoding="utf-8")
        for text in (raw.replace('"attempts":0', '"attempts":-0'),
                     raw.replace('"attempts":0', '"attempts":NaN'),
                     raw.replace('"at":60', '"at":Infinity')):
            self.assertNotEqual(text, raw)
            Path(self.ledger).write_text(text, encoding="utf-8")
            with self.subTest(text=text[:40]):
                with self.assertRaises(ValueError):
                    claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)

    def test_failed_directory_sync_restores_previous_bytes(self) -> None:
        self._seed_trade()
        self._commit()
        before = Path(self.ledger).read_bytes()
        original = dispatch_module._fsync_directory

        def failing(directory: str) -> None:
            raise OSError("simulated sync failure")

        dispatch_module._fsync_directory = failing
        try:
            with self.assertRaises(OSError):
                claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        finally:
            dispatch_module._fsync_directory = original
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_failed_first_commit_leaves_no_file(self) -> None:
        self._seed_trade()
        original = dispatch_module._fsync_directory

        def failing(directory: str) -> None:
            raise OSError("simulated sync failure")

        dispatch_module._fsync_directory = failing
        try:
            with self.assertRaises(OSError):
                self._commit()
        finally:
            dispatch_module._fsync_directory = original
        self.assertFalse(os.path.exists(self.ledger))
        self.assertEqual([n for n in os.listdir(self.tmp.name)
                          if n.endswith(".tmp")], [])

    def test_no_tmp_files_left_behind(self) -> None:
        self._seed_trade()
        self._commit()
        claim(self.ledger, "j-1", "w-1", 10, "k-c", 70)
        finish(self.ledger, "j-1", "w-1", "succeeded", "k-f", 75)
        self.assertEqual([n for n in os.listdir(self.tmp.name)
                          if n.endswith(".tmp")], [])

    def test_concurrent_claims_serialize(self) -> None:
        self._seed_trade()
        self._commit()
        results: list[tuple[dict[str, object], bool]] = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def worker(index: int) -> None:
            try:
                outcome = claim(self.ledger, "j-1", f"w-{index}", 10,
                                f"k-c{index}", 70)
                with lock:
                    results.append(outcome)
            except ValueError:
                pass  # lost the race to a ready decision
            except BaseException as exc:  # noqa: BLE001 - report all
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 1)
        decision, created = results[0]
        self.assertTrue(created)
        self.assertEqual(decision["attempts"], 1)
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(data["decisions"]["j-1"]["state"], "claimed")
        self.assertEqual(data["decisions"]["j-1"]["attempts"], 1)


if __name__ == "__main__":
    unittest.main()
