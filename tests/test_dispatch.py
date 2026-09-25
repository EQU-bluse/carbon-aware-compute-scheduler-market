from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

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

    def _commit(self, job_id: str = "j-1", key: str = "ck-1",
                at: int = 50) -> tuple[dict[str, object], bool]:
        return commit(self.jobs, self.supply, self.trades, self.ledger,
                      job_id, key, at)

    def _seed_commit(self) -> None:
        self._seed()
        clear(self.jobs, self.supply, self.trades, "j-1", "tk-1", 40)

    def test_commit_creates_ledger_and_ready_decision(self) -> None:
        self._seed_commit()
        decision, created = self._commit()
        self.assertTrue(created)
        self.assertEqual(decision, {
            "job_id": "j-1", "at": 50, "resource_id": "r-1", "version": 1,
            "deadline": 100, "state": "ready", "attempts": 0,
            "owner": None, "lease_end": None,
        })
        self.assertEqual(list(decision.keys()),
                         ["job_id", "at", "resource_id", "version",
                          "deadline", "state", "attempts", "owner",
                          "lease_end"])

        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()),
                         ["version", "decisions", "idempotency", "audit"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["decisions"].keys()), ["j-1"])
        self.assertEqual(data["idempotency"],
                         {"ck-1": {"action": "commit", "job_id": "j-1",
                                   "at": 50}})
        self.assertEqual(data["audit"], {"ck-1": {
            "key": "ck-1",
            "request": {"action": "commit", "job_id": "j-1", "at": 50},
            "result": decision,
        }})

    def test_commit_replay_returns_current_record_without_write(self) -> None:
        self._seed_commit()
        self._commit()
        raw = Path(self.ledger).read_bytes()
        decision, created = self._commit()
        self.assertFalse(created)
        self.assertEqual(decision["state"], "ready")
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        # The replay returns the current record, not the commit snapshot.
        claim(self.ledger, "j-1", "lk-1", "worker-1", 10, 60)
        decision, created = self._commit()
        self.assertFalse(created)
        self.assertEqual(decision["state"], "claimed")
        self.assertEqual(decision["owner"], "worker-1")

    def test_commit_key_conflict_and_double_commit(self) -> None:
        self._seed_commit()
        self._commit()
        with self.assertRaises(ValueError):
            self._commit(at=51)
        with self.assertRaises(ValueError):
            self._commit(key="ck-2")

    def test_commit_requires_trade_and_job(self) -> None:
        self._seed()
        # A missing clearing ledger is a missing input.
        with self.assertRaises(FileNotFoundError):
            self._commit()
        self.assertFalse(os.path.exists(self.ledger))
        # An existing clearing ledger without a trade for the job.
        submit(self.jobs, _job("j-2"), "jk-2")
        clear(self.jobs, self.supply, self.trades, "j-2", "tk-2", 40)
        with self.assertRaises(LookupError):
            self._commit()
        self.assertFalse(os.path.exists(self.ledger))
        clear(self.jobs, self.supply, self.trades, "j-1", "tk-1", 40)
        with self.assertRaises(KeyError):
            self._commit(job_id="j-unknown")
        self.assertFalse(os.path.exists(self.ledger))

    def test_commit_past_deadline_raises_timeout(self) -> None:
        self._seed_commit()
        with self.assertRaises(TimeoutError):
            self._commit(at=101)
        self.assertFalse(os.path.exists(self.ledger))
        decision, created = self._commit(at=100)
        self.assertTrue(created)

    def test_commit_missing_inputs(self) -> None:
        self._seed_commit()
        missing = os.path.join(self.tmp.name, "missing.json")
        with self.assertRaises(FileNotFoundError):
            commit(missing, self.supply, self.trades, self.ledger,
                   "j-1", "k", 1)
        with self.assertRaises(FileNotFoundError):
            commit(self.jobs, missing, self.trades, self.ledger,
                   "j-1", "k", 1)
        with self.assertRaises(FileNotFoundError):
            commit(self.jobs, self.supply, missing, self.ledger,
                   "j-1", "k", 1)
        with self.assertRaises(FileNotFoundError):
            commit(self.jobs, self.supply, self.trades,
                   os.path.join(self.tmp.name, "no-dir", "d.json"),
                   "j-1", "k", 1)
        self.assertFalse(os.path.exists(self.ledger))

    def test_commit_argument_validation(self) -> None:
        self._seed_commit()
        for bad in ("", None, 1):
            with self.assertRaises(ValueError):
                commit(bad, self.supply, self.trades, self.ledger,
                       "j-1", "k", 1)
        with self.assertRaises(ValueError):
            self._commit(at=-1)
        with self.assertRaises(ValueError):
            self._commit(at=True)
        with self.assertRaises(ValueError):
            commit(self.jobs, self.supply, self.trades, self.trades,
                   "j-1", "k", 1)

    def test_claim_happy_path_and_attempts(self) -> None:
        self._seed_commit()
        self._commit()
        decision, created = claim(self.ledger, "j-1", "lk-1", "w-1", 10, 60)
        self.assertTrue(created)
        self.assertEqual(decision["state"], "claimed")
        self.assertEqual(decision["attempts"], 1)
        self.assertEqual(decision["owner"], "w-1")
        self.assertEqual(decision["lease_end"], 70)

    def test_claim_lease_must_not_pass_deadline(self) -> None:
        self._seed_commit()
        self._commit()
        with self.assertRaises(TimeoutError):
            claim(self.ledger, "j-1", "lk-1", "w-1", 51, 50)
        # A lease ending exactly at the deadline is fine.
        decision, created = claim(self.ledger, "j-1", "lk-1", "w-1", 50, 50)
        self.assertTrue(created)
        self.assertEqual(decision["lease_end"], 100)

    def test_claim_valid_lease_raises_timeout(self) -> None:
        self._seed_commit()
        self._commit()
        claim(self.ledger, "j-1", "lk-1", "w-1", 10, 60)
        with self.assertRaises(TimeoutError):
            claim(self.ledger, "j-1", "lk-2", "w-2", 10, 70)
        with self.assertRaises(ValueError):
            claim(self.ledger, "j-1", "lk-2", "w-2", 10, 71)

    def test_claim_on_succeeded_raises_value_error(self) -> None:
        self._seed_commit()
        self._commit()
        claim(self.ledger, "j-1", "lk-1", "w-1", 10, 60)
        finish(self.ledger, "j-1", "fk-1", "w-1", "succeeded", 65)
        with self.assertRaises(ValueError):
            claim(self.ledger, "j-1", "lk-2", "w-2", 10, 66)

    def test_claim_failed_state_counts_new_attempt(self) -> None:
        self._seed_commit()
        self._commit()
        claim(self.ledger, "j-1", "lk-1", "w-1", 10, 60)
        finish(self.ledger, "j-1", "fk-1", "w-1", "failed", 65)
        decision, created = claim(self.ledger, "j-1", "lk-2", "w-2", 5, 66)
        self.assertTrue(created)
        self.assertEqual(decision["attempts"], 2)
        self.assertEqual(decision["owner"], "w-2")

    def test_claim_replay_and_conflicts(self) -> None:
        self._seed_commit()
        self._commit()
        claim(self.ledger, "j-1", "lk-1", "w-1", 10, 60)
        raw = Path(self.ledger).read_bytes()
        decision, created = claim(self.ledger, "j-1", "lk-1", "w-1", 10, 60)
        self.assertFalse(created)
        self.assertEqual(decision["owner"], "w-1")
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        with self.assertRaises(ValueError):
            claim(self.ledger, "j-1", "lk-1", "w-2", 10, 60)
        with self.assertRaises(ValueError):
            claim(self.ledger, "j-1", "lk-1", "w-1", 11, 60)
        with self.assertRaises(ValueError):
            claim(self.ledger, "j-1", "lk-1", "w-1", 10, 61)

    def test_claim_argument_validation(self) -> None:
        self._seed_commit()
        self._commit()
        with self.assertRaises(ValueError):
            claim(self.ledger, "j-1", "lk", "", 10, 60)
        with self.assertRaises(ValueError):
            claim(self.ledger, "j-1", "lk", "w", 0, 60)
        with self.assertRaises(ValueError):
            claim(self.ledger, "j-1", "lk", "w", True, 60)
        with self.assertRaises(ValueError):
            claim(self.ledger, "j-1", "lk", "w", 10, -1)

    def test_finish_happy_path(self) -> None:
        self._seed_commit()
        self._commit()
        claim(self.ledger, "j-1", "lk-1", "w-1", 10, 60)
        decision, created = finish(self.ledger, "j-1", "fk-1", "w-1",
                                   "succeeded", 70)
        self.assertTrue(created)
        self.assertEqual(decision["state"], "succeeded")
        self.assertEqual(decision["attempts"], 1)
        self.assertIsNone(decision["owner"])
        self.assertIsNone(decision["lease_end"])

    def test_finish_requires_current_owner_within_lease(self) -> None:
        self._seed_commit()
        self._commit()
        claim(self.ledger, "j-1", "lk-1", "w-1", 10, 60)
        with self.assertRaises(PermissionError):
            finish(self.ledger, "j-1", "fk-1", "w-2", "succeeded", 65)
        with self.assertRaises(TimeoutError):
            finish(self.ledger, "j-1", "fk-1", "w-1", "succeeded", 71)
        decision, created = finish(self.ledger, "j-1", "fk-1", "w-1",
                                   "succeeded", 70)
        self.assertTrue(created)

    def test_finish_requires_claimed_state(self) -> None:
        self._seed_commit()
        self._commit()
        with self.assertRaises(ValueError):
            finish(self.ledger, "j-1", "fk-1", "w-1", "succeeded", 60)

    def test_finish_result_validation(self) -> None:
        self._seed_commit()
        self._commit()
        claim(self.ledger, "j-1", "lk-1", "w-1", 10, 60)
        for bad in ("done", "", None, 1):
            with self.assertRaises(ValueError):
                finish(self.ledger, "j-1", "fk-1", "w-1", bad, 65)

    def test_finish_replay_and_conflicts(self) -> None:
        self._seed_commit()
        self._commit()
        claim(self.ledger, "j-1", "lk-1", "w-1", 10, 60)
        finish(self.ledger, "j-1", "fk-1", "w-1", "failed", 65)
        raw = Path(self.ledger).read_bytes()
        decision, created = finish(self.ledger, "j-1", "fk-1", "w-1",
                                   "failed", 65)
        self.assertFalse(created)
        self.assertEqual(decision["state"], "failed")
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        with self.assertRaises(ValueError):
            finish(self.ledger, "j-1", "fk-1", "w-1", "succeeded", 65)

    def test_recover_happy_path(self) -> None:
        self._seed_commit()
        self._commit()
        claim(self.ledger, "j-1", "lk-1", "w-1", 10, 60)
        decision, created = recover(self.ledger, "j-1", "rk-1", 71)
        self.assertTrue(created)
        self.assertEqual(decision["state"], "ready")
        self.assertEqual(decision["attempts"], 1)
        self.assertIsNone(decision["owner"])
        self.assertIsNone(decision["lease_end"])

    def test_recover_requires_strictly_expired_lease(self) -> None:
        self._seed_commit()
        self._commit()
        claim(self.ledger, "j-1", "lk-1", "w-1", 10, 60)
        with self.assertRaises(PermissionError):
            recover(self.ledger, "j-1", "rk-1", 70)
        with self.assertRaises(PermissionError):
            recover(self.ledger, "j-1", "rk-1", 60)
        decision, created = recover(self.ledger, "j-1", "rk-1", 71)
        self.assertTrue(created)

    def test_recover_requires_claimed_state(self) -> None:
        self._seed_commit()
        self._commit()
        with self.assertRaises(ValueError):
            recover(self.ledger, "j-1", "rk-1", 60)

    def test_recover_replay_and_conflicts(self) -> None:
        self._seed_commit()
        self._commit()
        claim(self.ledger, "j-1", "lk-1", "w-1", 10, 60)
        recover(self.ledger, "j-1", "rk-1", 71)
        raw = Path(self.ledger).read_bytes()
        decision, created = recover(self.ledger, "j-1", "rk-1", 71)
        self.assertFalse(created)
        self.assertEqual(decision["state"], "ready")
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        with self.assertRaises(ValueError):
            recover(self.ledger, "j-1", "rk-1", 72)

    def test_actions_require_existing_ledger(self) -> None:
        for call in (
            lambda: claim(self.ledger, "j-1", "k", "w", 1, 1),
            lambda: finish(self.ledger, "j-1", "k", "w", "succeeded", 1),
            lambda: recover(self.ledger, "j-1", "k", 1),
        ):
            with self.assertRaises(FileNotFoundError):
                call()

    def test_unknown_job_raises_key_error(self) -> None:
        self._seed_commit()
        self._commit()
        with self.assertRaises(KeyError):
            claim(self.ledger, "j-x", "k", "w", 1, 1)
        with self.assertRaises(KeyError):
            finish(self.ledger, "j-x", "k", "w", "succeeded", 1)
        with self.assertRaises(KeyError):
            recover(self.ledger, "j-x", "k", 1)

    def test_key_namespaces_are_shared_across_actions(self) -> None:
        self._seed_commit()
        self._commit(key="shared")
        with self.assertRaises(ValueError):
            claim(self.ledger, "j-1", "shared", "w-1", 10, 60)
        with self.assertRaises(ValueError):
            finish(self.ledger, "j-1", "shared", "w-1", "succeeded", 60)
        with self.assertRaises(ValueError):
            recover(self.ledger, "j-1", "shared", 60)

    def test_full_lifecycle_and_audit_trail(self) -> None:
        self._seed_commit()
        self._commit()
        claim(self.ledger, "j-1", "lk-1", "w-1", 10, 60)
        recover(self.ledger, "j-1", "rk-1", 71)
        claim(self.ledger, "j-1", "lk-2", "w-2", 20, 72)
        decision, created = finish(self.ledger, "j-1", "fk-1", "w-2",
                                   "succeeded", 80)
        self.assertTrue(created)
        self.assertEqual(decision["state"], "succeeded")
        self.assertEqual(decision["attempts"], 2)

        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(list(data["idempotency"].keys()),
                         ["ck-1", "fk-1", "lk-1", "lk-2", "rk-1"])
        self.assertEqual(list(data["audit"].keys()),
                         ["ck-1", "fk-1", "lk-1", "lk-2", "rk-1"])
        self.assertEqual(
            data["audit"]["lk-1"]["request"],
            {"action": "claim", "job_id": "j-1", "owner": "w-1",
             "lease": 10, "at": 60})
        self.assertEqual(data["audit"]["lk-1"]["result"]["lease_end"], 70)
        self.assertEqual(data["audit"]["rk-1"]["result"]["state"], "ready")
        self.assertEqual(data["audit"]["fk-1"]["result"]["state"],
                         "succeeded")
        self.assertEqual(data["audit"]["lk-2"]["result"]["attempts"], 2)

    def test_sorted_sections_and_non_ascii(self) -> None:
        submit(self.jobs, _job("j-b"), "jk-b")
        submit(self.jobs, _job("j-a"), "jk-a")
        publish(self.supply, _resource("r-中"), "rk")
        clear(self.jobs, self.supply, self.trades, "j-b", "tk-b", 40)
        clear(self.jobs, self.supply, self.trades, "j-a", "tk-a", 40)
        commit(self.jobs, self.supply, self.trades, self.ledger,
               "j-b", "键-b", 50)
        commit(self.jobs, self.supply, self.trades, self.ledger,
               "j-a", "键-a", 50)
        raw = Path(self.ledger).read_text(encoding="utf-8")
        self.assertIn("r-中", raw)
        self.assertIn("键-b", raw)
        self.assertNotIn(" ", raw.split("\n", 1)[0])
        data = json.loads(raw)
        self.assertEqual(list(data["decisions"].keys()), ["j-a", "j-b"])
        self.assertEqual(list(data["idempotency"].keys()), ["键-a", "键-b"])
        self.assertEqual(list(data["audit"].keys()), ["键-a", "键-b"])

    def test_invalid_ledger_bytes_raise_value_error(self) -> None:
        self._seed_commit()
        self._commit()
        raw = Path(self.ledger).read_bytes()
        for bad in (raw + b"\n", raw[:-1], raw.replace(b'"ready"',
                                                       b'"Ready"', 1),
                    b"not json", json.dumps({"version": 2}).encode()):
            Path(self.ledger).write_bytes(bad)
            with self.assertRaises(ValueError):
                claim(self.ledger, "j-1", "lk", "w", 1, 1)
        Path(self.ledger).write_bytes(raw)
        decision, created = claim(self.ledger, "j-1", "lk", "w", 1, 1)
        self.assertTrue(created)

    def test_failed_call_leaves_ledger_untouched(self) -> None:
        self._seed_commit()
        self._commit()
        raw = Path(self.ledger).read_bytes()
        for call in (
            lambda: self._commit(key="ck-2"),
            lambda: claim(self.ledger, "j-1", "lk", "w", 0, 1),
            lambda: claim(self.ledger, "j-x", "lk", "w", 1, 1),
            lambda: claim(self.ledger, "j-1", "lk", "w", 1000, 1),
            lambda: finish(self.ledger, "j-1", "fk", "w", "bad", 1),
            lambda: recover(self.ledger, "j-1", "rk", 1),
        ):
            with self.assertRaises((ValueError, KeyError, TimeoutError,
                                    PermissionError)):
                call()
            self.assertEqual(Path(self.ledger).read_bytes(), raw)

    def test_concurrent_claims_serialize(self) -> None:
        self._seed_commit()
        self._commit()
        results: list[object] = []

        def worker(index: int) -> None:
            try:
                results.append(claim(self.ledger, "j-1", f"lk-{index}",
                                     f"w-{index}", 10, 60))
            except (ValueError, TimeoutError) as exc:
                results.append(exc)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        winners = [r for r in results if isinstance(r, tuple)]
        self.assertEqual(len(winners), 1)
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(data["decisions"]["j-1"]["attempts"], 1)


if __name__ == "__main__":
    unittest.main()
