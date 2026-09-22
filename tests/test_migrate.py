from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market.jobs import register as register_job
from carbon_market.market import match
from carbon_market.migrate import run
from carbon_market.offers import register as register_offer
from carbon_market.reserve import run as reserve


def _job(job_id: str = "j-1", **overrides: object) -> dict[str, object]:
    job: dict[str, object] = {
        "job_id": job_id,
        "deadline": 100,
        "energy_wh": 100,
        "residency_regions": ["eu-north"],
    }
    job.update(overrides)
    return job


def _offer(resource_id: str = "r-1", **overrides: object) -> dict[str, object]:
    offer: dict[str, object] = {
        "resource_id": resource_id,
        "region": "eu-north",
        "capacity_wh": 250,
        "unit_cost": 1000,
        "carbon_intensity": 42,
    }
    offer.update(overrides)
    return offer


class MigrateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs = os.path.join(self.tmp.name, "jobs.json")
        self.offers = os.path.join(self.tmp.name, "offers.json")
        self.ledger = os.path.join(self.tmp.name, "ledger.json")
        self.reserves = os.path.join(self.tmp.name, "reserves.json")
        self.state = os.path.join(self.tmp.name, "state.json")

    def _seed(self, job: dict[str, object] | None = None,
              offers: list[dict[str, object]] | None = None,
              job_id: str = "j-1") -> None:
        register_job(self.jobs, job if job is not None else _job(job_id=job_id),
                     f"job-key-{job_id}")
        for index, offer in enumerate(
                offers if offers is not None else [_offer(), _offer("r-2")]):
            register_offer(self.offers, offer, f"offer-key-{index}")
        match(self.jobs, self.offers, self.ledger, job_id, f"match-{job_id}")
        reserve(self.jobs, self.offers, self.ledger, self.reserves,
                job_id, "reserve", f"reserve-{job_id}", 10)

    # -- happy path -------------------------------------------------------

    def test_prepare_creates_state_and_returns_event(self) -> None:
        self._seed()
        event, created = run(self.jobs, self.offers, self.ledger,
                             self.reserves, self.state,
                             "j-1", "r-2", "prepare", "k-1", 50)
        self.assertTrue(created)
        self.assertEqual(event, {"job_id": "j-1", "source_id": "r-1",
                                 "target_id": "r-2", "op": "prepare",
                                 "now": 50})
        self.assertEqual(list(event.keys()),
                         ["job_id", "source_id", "target_id", "op", "now"])
        raw = Path(self.state).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()), ["version", "events"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["events"].keys()), ["k-1"])
        self.assertEqual(list(data["events"]["k-1"].keys()),
                         ["job_id", "source_id", "target_id", "op", "now"])

    def test_commit_and_abort_return_events(self) -> None:
        self._seed()
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "prepare", "k-p", 50)
        event, created = run(self.jobs, self.offers, self.ledger,
                             self.reserves, self.state,
                             "j-1", "r-2", "commit", "k-c", 60)
        self.assertTrue(created)
        self.assertEqual(event, {"job_id": "j-1", "source_id": "r-1",
                                 "target_id": "r-2", "op": "commit",
                                 "now": 60})

        self._seed(job_id="j-2")
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-2", "r-2", "prepare", "k-p2", 50)
        event, created = run(self.jobs, self.offers, self.ledger,
                             self.reserves, self.state,
                             "j-2", "r-2", "abort", "k-a2", 70)
        self.assertTrue(created)
        self.assertEqual(event["op"], "abort")
        self.assertEqual(event["source_id"], "r-1")

    def test_prepare_at_deadline_allowed(self) -> None:
        self._seed(job=_job(deadline=100))
        event, created = run(self.jobs, self.offers, self.ledger,
                             self.reserves, self.state,
                             "j-1", "r-2", "prepare", "k", 100)
        self.assertTrue(created)
        self.assertEqual(event["now"], 100)

    # -- idempotency ------------------------------------------------------

    def test_replay_returns_event_and_false(self) -> None:
        self._seed()
        first, c1 = run(self.jobs, self.offers, self.ledger, self.reserves,
                        self.state, "j-1", "r-2", "prepare", "k", 50)
        replay, c2 = run(self.jobs, self.offers, self.ledger, self.reserves,
                         self.state, "j-1", "r-2", "prepare", "k", 50)
        self.assertTrue(c1)
        self.assertFalse(c2)
        self.assertEqual(first, replay)
        data = json.loads(Path(self.state).read_text(encoding="utf-8"))
        self.assertEqual(len(data["events"]), 1)

    def test_same_key_different_event_params_raises(self) -> None:
        self._seed()
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "prepare", "k", 50)
        for args in (("j-1", "r-2", "prepare", "k", 51),
                     ("j-1", "r-2", "commit", "k", 50)):
            with self.subTest(args=args):
                with self.assertRaises(ValueError):
                    run(self.jobs, self.offers, self.ledger, self.reserves,
                        self.state, *args)
        register_offer(self.offers, _offer("r-3"), "offer-key-3")
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-3", "prepare", "k", 50)

    # -- lifecycle ----------------------------------------------------------

    def test_commit_without_prepare_raises_value_error(self) -> None:
        self._seed()
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "commit", "k", 10)

    def test_abort_without_prepare_raises_value_error(self) -> None:
        self._seed()
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "abort", "k", 10)

    def test_double_prepare_raises_value_error(self) -> None:
        self._seed()
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "prepare", "k1", 10)
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "k2", 20)

    def test_one_round_per_job(self) -> None:
        self._seed()
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "prepare", "k-p", 10)
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "abort", "k-a", 20)
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "k-p2", 30)

    def test_double_commit_raises_value_error(self) -> None:
        self._seed()
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "prepare", "k-p", 10)
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "commit", "k-c", 20)
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "commit", "k-c2", 30)

    def test_commit_with_other_target_raises_value_error(self) -> None:
        self._seed()
        register_offer(self.offers, _offer("r-3"), "offer-key-3")
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "prepare", "k-p", 10)
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-3", "commit", "k-c", 20)

    # -- gates ------------------------------------------------------------

    def test_unknown_job_raises_key_error(self) -> None:
        self._seed()
        with self.assertRaises(KeyError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-ghost", "r-2", "prepare", "k", 10)

    def test_unknown_target_raises_key_error(self) -> None:
        self._seed()
        with self.assertRaises(KeyError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-ghost", "prepare", "k", 10)

    def test_unmatched_job_raises_lookup_error(self) -> None:
        register_job(self.jobs, _job(job_id="j-1"), "jk1")
        register_job(self.jobs, _job(job_id="j-2"), "jk2")
        register_offer(self.offers, _offer(), "o1")
        register_offer(self.offers, _offer("r-2"), "o2")
        match(self.jobs, self.offers, self.ledger, "j-1", "m1")
        Path(self.reserves).write_text(
            json.dumps({"version": 1, "events": {}}), encoding="utf-8")
        with self.assertRaises(LookupError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-2", "r-2", "prepare", "k", 10)
        self.assertFalse(os.path.exists(self.state))

    def test_unreserved_job_raises_lookup_error(self) -> None:
        register_job(self.jobs, _job(), "jk")
        register_offer(self.offers, _offer(), "o1")
        register_offer(self.offers, _offer("r-2"), "o2")
        match(self.jobs, self.offers, self.ledger, "j-1", "m")
        Path(self.reserves).write_text(
            json.dumps({"version": 1, "events": {}}), encoding="utf-8")
        with self.assertRaises(LookupError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "k", 10)

    def test_cancelled_reservation_raises_lookup_error(self) -> None:
        self._seed()
        reserve(self.jobs, self.offers, self.ledger, self.reserves,
                "j-1", "cancel", "cancel-j1", 20)
        with self.assertRaises(LookupError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "k", 10)

    def test_target_equal_source_raises_value_error(self) -> None:
        self._seed()
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-1", "prepare", "k", 10)

    def test_region_not_permitted_raises_lookup_error(self) -> None:
        self._seed(offers=[_offer(), _offer("r-2", region="us-west")])
        with self.assertRaises(LookupError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "k", 10)

    def test_prepare_past_deadline_raises_timeout_without_write(self) -> None:
        self._seed()
        with self.assertRaises(TimeoutError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "k", 101)
        self.assertFalse(os.path.exists(self.state))

    def test_commit_after_deadline_allowed(self) -> None:
        self._seed()
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "prepare", "k-p", 10)
        event, _ = run(self.jobs, self.offers, self.ledger, self.reserves,
                       self.state, "j-1", "r-2", "commit", "k-c", 10_000)
        self.assertEqual(event["op"], "commit")

    # -- capacity ---------------------------------------------------------

    def _seed_two_jobs(self, target_capacity: int = 150) -> None:
        register_offer(self.offers, _offer(capacity_wh=10_000), "o1")
        register_offer(self.offers,
                       _offer("r-2", capacity_wh=target_capacity), "o2")
        for index, job_id in enumerate(("j-1", "j-2")):
            register_job(self.jobs, _job(job_id=job_id), f"jk{index}")
            match(self.jobs, self.offers, self.ledger, job_id, f"m{index}")
            reserve(self.jobs, self.offers, self.ledger, self.reserves,
                    job_id, "reserve", f"r{index}", 10)

    def test_pending_prepare_locks_target_capacity(self) -> None:
        self._seed_two_jobs(target_capacity=150)
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "prepare", "k-p1", 10)
        with self.assertRaises(LookupError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-2", "r-2", "prepare", "k-p2", 10)

    def test_abort_releases_target_placeholder(self) -> None:
        self._seed_two_jobs(target_capacity=150)
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "prepare", "k-p1", 10)
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "abort", "k-a1", 20)
        event, created = run(self.jobs, self.offers, self.ledger,
                             self.reserves, self.state,
                             "j-2", "r-2", "prepare", "k-p2", 30)
        self.assertTrue(created)
        self.assertEqual(event["job_id"], "j-2")

    def test_commit_keeps_target_occupied(self) -> None:
        self._seed_two_jobs(target_capacity=150)
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "prepare", "k-p1", 10)
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "commit", "k-c1", 20)
        with self.assertRaises(LookupError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-2", "r-2", "prepare", "k-p2", 30)

    def test_active_reservation_on_target_counts(self) -> None:
        # j-2 is matched to r-2 directly and actively reserves it; a
        # migration prepare onto r-2 must account for that reservation.
        register_offer(self.offers, _offer(capacity_wh=10_000), "o1")
        register_offer(self.offers,
                       _offer("r-2", capacity_wh=150), "o2")
        register_job(self.jobs, _job(job_id="j-1"), "jk1")
        register_job(self.jobs, _job(job_id="j-2"), "jk2")
        Path(self.ledger).write_text(json.dumps({
            "version": 1,
            "matches": {"j-1": {"job_id": "j-1", "resource_id": "r-1"},
                        "j-2": {"job_id": "j-2", "resource_id": "r-2"}},
            "idempotency": {"m1": "j-1", "m2": "j-2"},
        }), encoding="utf-8")
        reserve(self.jobs, self.offers, self.ledger, self.reserves,
                "j-1", "reserve", "r1", 10)
        reserve(self.jobs, self.offers, self.ledger, self.reserves,
                "j-2", "reserve", "r2", 10)
        with self.assertRaises(LookupError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "k", 10)

    def test_capacity_lookup_error_writes_nothing(self) -> None:
        self._seed_two_jobs(target_capacity=150)
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "prepare", "k-p1", 10)
        before = Path(self.state).read_bytes()
        with self.assertRaises(LookupError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-2", "r-2", "prepare", "k-p2", 10)
        self.assertEqual(Path(self.state).read_bytes(), before)

    # -- persistence format -------------------------------------------------

    def test_compact_utf8_json_and_sorted_keys(self) -> None:
        register_offer(self.offers, _offer("r-中", capacity_wh=10_000,
                                           carbon_intensity=1), "o1")
        register_offer(self.offers, _offer("r-2", capacity_wh=10_000), "o2")
        for index, job_id in enumerate(("j-b", "j-a")):
            register_job(self.jobs, _job(job_id=job_id), f"jk-{job_id}")
            match(self.jobs, self.offers, self.ledger, job_id, f"m-{job_id}")
            reserve(self.jobs, self.offers, self.ledger, self.reserves,
                    job_id, "reserve", f"r-{job_id}", 1)
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-b", "r-2", "prepare", "键-b", 1)
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-a", "r-2", "prepare", "键-a", 1)
        raw = Path(self.state).read_text(encoding="utf-8")
        self.assertIn("r-中", raw)
        self.assertIn("键-a", raw)
        self.assertNotIn(" ", raw.split("\n", 1)[0])
        data = json.loads(raw)
        self.assertEqual(list(data["events"].keys()), ["键-a", "键-b"])
        self.assertEqual(
            data["events"]["键-a"],
            {"job_id": "j-a", "source_id": "r-中", "target_id": "r-2",
             "op": "prepare", "now": 1})

    def test_seeded_state_replays(self) -> None:
        self._seed()
        Path(self.state).write_text(json.dumps({
            "version": 1,
            "events": {
                "k": {"job_id": "j-1", "source_id": "r-1",
                      "target_id": "r-2", "op": "prepare", "now": 5},
            },
        }), encoding="utf-8")
        event, created = run(self.jobs, self.offers, self.ledger,
                             self.reserves, self.state,
                             "j-1", "r-2", "prepare", "k", 5)
        self.assertFalse(created)
        self.assertEqual(event["now"], 5)

    # -- invalid arguments and files ---------------------------------------

    def test_invalid_arguments(self) -> None:
        good = (self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "k", 10)
        for index in range(9):
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
                        self.state, "j-1", "r-2", "prepare", "k",
                        bad_now)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "delete", "k", 10)

    def test_missing_inputs_raise_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "k", 10)
        register_job(self.jobs, _job(), "jk")
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "k", 10)
        register_offer(self.offers, _offer(), "o1")
        register_offer(self.offers, _offer("r-2"), "o2")
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "k", 10)
        match(self.jobs, self.offers, self.ledger, "j-1", "m")
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "k", 10)

    def test_missing_state_parent_raises_file_not_found(self) -> None:
        self._seed()
        state = os.path.join(self.tmp.name, "no-such-dir", "state.json")
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, self.reserves, state,
                "j-1", "r-2", "prepare", "k", 10)

    def test_corrupt_files_raise_value_error(self) -> None:
        self._seed()
        for path in (self.jobs, self.offers, self.ledger, self.reserves,
                     self.state):
            if path == self.state:
                run(self.jobs, self.offers, self.ledger, self.reserves,
                    self.state, "j-1", "r-2", "prepare", "k-seed", 1)
            original = Path(path).read_bytes() if os.path.exists(path) else None
            Path(path).write_text("{not json", encoding="utf-8")
            with self.subTest(path=path):
                with self.assertRaises(ValueError):
                    run(self.jobs, self.offers, self.ledger, self.reserves,
                        self.state, "j-1", "r-2", "prepare", "k", 2)
            if original is None:
                os.unlink(path)
            else:
                Path(path).write_bytes(original)

    def test_invalid_state_structures_raise_value_error(self) -> None:
        self._seed()
        good_event = {"job_id": "j-1", "source_id": "r-1",
                      "target_id": "r-2", "op": "prepare", "now": 1}
        bad_payloads = [
            [],
            {},
            {"version": 1},
            {"version": 1, "events": {}, "extra": 1},
            {"version": 2, "events": {}},
            {"version": "1", "events": {}},
            {"version": 1, "events": []},
            {"version": 1, "events": {"k": []}},
            {"version": 1, "events": {"k": {**good_event, "x": 1}}},
            {"version": 1, "events": {"k": {"source_id": "r-1",
                                            "target_id": "r-2",
                                            "op": "prepare", "now": 1}}},
            {"version": 1, "events": {"k": {**good_event, "job_id": ""}}},
            {"version": 1, "events": {"k": {**good_event, "source_id": ""}}},
            {"version": 1, "events": {"k": {**good_event, "target_id": ""}}},
            {"version": 1, "events": {"k": {**good_event, "op": "delete"}}},
            {"version": 1, "events": {"k": {**good_event, "now": -1}}},
            {"version": 1, "events": {"k": {**good_event, "now": True}}},
            {"version": 1, "events": {"k": {**good_event,
                                            "job_id": "ghost"}}},
            {"version": 1, "events": {"k": {**good_event,
                                            "source_id": "ghost"}}},
            {"version": 1, "events": {"k": {**good_event,
                                            "target_id": "ghost"}}},
            {"version": 1, "events": {"k": {**good_event, "now": 101}}},
            {"version": 1, "events": {
                "p1": good_event,
                "p2": {**good_event, "now": 2}}},
            {"version": 1, "events": {"c": {**good_event, "op": "commit"}}},
            {"version": 1, "events": {
                "p": good_event,
                "c": {**good_event, "op": "commit", "target_id": "r-1",
                      "now": 2}}},
        ]
        for payload in bad_payloads:
            with self.subTest(payload=payload):
                Path(self.state).write_text(json.dumps(payload),
                                            encoding="utf-8")
                with self.assertRaises(ValueError):
                    run(self.jobs, self.offers, self.ledger, self.reserves,
                        self.state, "j-1", "r-2", "prepare", "k-new", 5)

    def test_state_event_for_wrong_source_raises_value_error(self) -> None:
        self._seed()
        Path(self.state).write_text(json.dumps({
            "version": 1,
            "events": {"k": {"job_id": "j-1", "source_id": "r-2",
                             "target_id": "r-2", "op": "prepare", "now": 1}},
        }), encoding="utf-8")
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "k2", 2)

    def test_negative_zero_literal_in_state_raises_value_error(self) -> None:
        self._seed()
        Path(self.state).write_text(
            '{"version":1,"events":{"k":{"job_id":"j-1","source_id":"r-1",'
            '"target_id":"r-2","op":"prepare","now":-0}}}',
            encoding="utf-8")
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "k2", 2)

    # -- concurrency / files -----------------------------------------------

    def test_concurrent_same_key_serializes_to_one_event(self) -> None:
        self._seed()
        outcomes: list[tuple[dict[str, object], bool]] = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def worker() -> None:
            try:
                outcome = run(self.jobs, self.offers, self.ledger,
                              self.reserves, self.state,
                              "j-1", "r-2", "prepare", "k", 10)
                with lock:
                    outcomes.append(outcome)
            except BaseException as exc:  # noqa: BLE001 - report all
                with lock:
                    errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        self.assertEqual(errors, [])
        self.assertEqual(len(outcomes), 12)
        self.assertEqual(sum(1 for _, created in outcomes if created), 1)
        data = json.loads(Path(self.state).read_text(encoding="utf-8"))
        self.assertEqual(len(data["events"]), 1)

    def test_no_tmp_files_left_behind(self) -> None:
        self._seed()
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "prepare", "k-p", 1)
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "r-2", "commit", "k-c", 2)
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])


if __name__ == "__main__":
    unittest.main()
