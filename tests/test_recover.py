from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market.jobs import register as register_job
from carbon_market.market import match
from carbon_market.migrate import run as migrate
from carbon_market.offers import register as register_offer
from carbon_market.recover import run
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


class RecoverTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs = os.path.join(self.tmp.name, "jobs.json")
        self.offers = os.path.join(self.tmp.name, "offers.json")
        self.ledger = os.path.join(self.tmp.name, "ledger.json")
        self.reserves = os.path.join(self.tmp.name, "reserves.json")
        self.state = os.path.join(self.tmp.name, "state.json")

    def _seed(self) -> None:
        register_job(self.jobs, _job(), "job-key")
        # r-1 is the greenest offer, so match lands j-1 there; r-2 is a
        # feasible migration target in the same permitted region.
        register_offer(self.offers,
                       _offer(resource_id="r-1", carbon_intensity=10),
                       "offer-key-1")
        register_offer(self.offers,
                       _offer(resource_id="r-2", carbon_intensity=100),
                       "offer-key-2")
        match(self.jobs, self.offers, self.ledger, "j-1", "match-key")
        reserve(self.jobs, self.offers, self.ledger, self.reserves,
                "j-1", "reserve", "reserve-key", 10)
        migrate(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "prepare-key", 20)

    # -- happy path -------------------------------------------------------

    def test_recover_commits_active_pending_migration(self) -> None:
        self._seed()
        event, created = run(self.jobs, self.offers, self.ledger,
                             self.reserves, self.state, "j-1", "rec-key", 50)
        self.assertTrue(created)
        self.assertEqual(event, {"job_id": "j-1", "source_id": "r-1",
                                 "target_id": "r-2", "op": "commit",
                                 "now": 50})
        self.assertEqual(list(event.keys()),
                         ["job_id", "source_id", "target_id", "op", "now"])

    def test_now_equal_deadline_commits(self) -> None:
        self._seed()
        event, _ = run(self.jobs, self.offers, self.ledger, self.reserves,
                       self.state, "j-1", "rec-key", 100)
        self.assertEqual(event["op"], "commit")

    def test_now_past_deadline_aborts(self) -> None:
        self._seed()
        event, created = run(self.jobs, self.offers, self.ledger,
                             self.reserves, self.state, "j-1", "rec-key", 101)
        self.assertTrue(created)
        self.assertEqual(event["op"], "abort")
        self.assertEqual(event["now"], 101)
        self.assertEqual(event["source_id"], "r-1")
        self.assertEqual(event["target_id"], "r-2")

    def test_cancelled_reservation_aborts_even_before_deadline(self) -> None:
        self._seed()
        reserve(self.jobs, self.offers, self.ledger, self.reserves,
                "j-1", "cancel", "cancel-key", 30)
        event, created = run(self.jobs, self.offers, self.ledger,
                             self.reserves, self.state, "j-1", "rec-key", 40)
        self.assertTrue(created)
        self.assertEqual(event["op"], "abort")

    # -- idempotency ------------------------------------------------------

    def test_replay_returns_event_and_false(self) -> None:
        self._seed()
        first, c1 = run(self.jobs, self.offers, self.ledger, self.reserves,
                        self.state, "j-1", "rec-key", 50)
        replay, c2 = run(self.jobs, self.offers, self.ledger, self.reserves,
                         self.state, "j-1", "rec-key", 50)
        self.assertTrue(c1)
        self.assertFalse(c2)
        self.assertEqual(first, replay)
        data = json.loads(Path(self.state).read_text(encoding="utf-8"))
        self.assertEqual(len(data["events"]), 2)

    def test_abort_replay_stable(self) -> None:
        self._seed()
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "rec-key", 101)
        event, created = run(self.jobs, self.offers, self.ledger,
                             self.reserves, self.state, "j-1", "rec-key", 101)
        self.assertFalse(created)
        self.assertEqual(event["op"], "abort")

    def test_same_key_different_event_raises_value_error(self) -> None:
        self._seed()
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "rec-key", 101)
        # The recorded event is an abort; a fresh call at an earlier now
        # would decide commit, which clashes with the key.
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "rec-key", 50)
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "rec-key", 102)

        # Same key replayed against a different job's prepare clashes too.
        register_offer(self.offers,
                       _offer(resource_id="r-3", carbon_intensity=100), "ok3")
        register_job(self.jobs, _job(job_id="j-2"), "jk2")
        # j-1 still actively reserves r-1, so r-3 is the feasible greenest
        # offer left for j-2 (r-2 already holds j-1's landed... target);
        # either way j-2's match differs from its migration target.
        match(self.jobs, self.offers, self.ledger, "j-2", "m2")
        reserve(self.jobs, self.offers, self.ledger, self.reserves,
                "j-2", "reserve", "r2", 10)
        matched = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        j2_resource = matched["matches"]["j-2"]["resource_id"]
        other = "r-1" if j2_resource != "r-1" else "r-3"
        migrate(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-2", other, "prepare", "p2", 20)
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-2", "rec-key", 50)

    # -- lifecycle errors -------------------------------------------------

    def test_unknown_job_raises_key_error(self) -> None:
        self._seed()
        with self.assertRaises(KeyError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-ghost", "rec-key", 50)

    def test_unmatched_job_raises_value_error(self) -> None:
        self._seed()
        register_job(self.jobs, _job(job_id="j-2"), "jk2")
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-2", "rec-key", 50)

    def test_missing_state_raises_file_not_found(self) -> None:
        register_job(self.jobs, _job(), "job-key")
        register_offer(self.offers, _offer(), "offer-key-1")
        match(self.jobs, self.offers, self.ledger, "j-1", "match-key")
        reserve(self.jobs, self.offers, self.ledger, self.reserves,
                "j-1", "reserve", "reserve-key", 10)
        self.assertFalse(os.path.exists(self.state))
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "rec-key", 50)

    def test_existing_journal_without_prepare_raises_value_error(self) -> None:
        register_job(self.jobs, _job(), "job-key")
        register_offer(self.offers, _offer(), "offer-key-1")
        match(self.jobs, self.offers, self.ledger, "j-1", "match-key")
        reserve(self.jobs, self.offers, self.ledger, self.reserves,
                "j-1", "reserve", "reserve-key", 10)
        Path(self.state).write_text(
            '{"version":1,"events":{}}\n', encoding="utf-8")
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "rec-key", 50)

    def test_already_committed_raises_value_error(self) -> None:
        self._seed()
        migrate(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "commit", "commit-key", 40)
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "rec-key", 50)

    def test_already_aborted_raises_value_error(self) -> None:
        self._seed()
        migrate(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "abort", "abort-key", 40)
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "rec-key", 50)

    def test_failure_writes_nothing(self) -> None:
        self._seed()
        register_job(self.jobs, _job(job_id="j-2"), "jk2")
        before = Path(self.state).read_bytes()
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-2", "rec-key", 50)
        self.assertEqual(Path(self.state).read_bytes(), before)

    # -- invalid arguments and files -------------------------------------

    def test_invalid_arguments(self) -> None:
        good = [self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "k", 10]
        for index in range(7):
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
                        self.state, "j-1", "k", bad_now)  # type: ignore[arg-type]

    def test_missing_inputs_raise_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "k", 10)
        register_job(self.jobs, _job(), "jk")
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "k", 10)
        register_offer(self.offers, _offer(), "ok")
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "k", 10)
        match(self.jobs, self.offers, self.ledger, "j-1", "m")
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "k", 10)
        reserve(self.jobs, self.offers, self.ledger, self.reserves,
                "j-1", "reserve", "rk", 10)
        # Every other input exists; only the migration state is missing.
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "k", 10)
        missing_parent = os.path.join(self.tmp.name, "no-such-dir", "x.json")
        with self.assertRaises(FileNotFoundError):
            run(missing_parent, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "k", 10)
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, missing_parent, self.ledger, self.reserves,
                self.state, "j-1", "k", 10)
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, missing_parent, self.reserves,
                self.state, "j-1", "k", 10)
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, missing_parent,
                self.state, "j-1", "k", 10)
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                missing_parent, "j-1", "k", 10)

    def test_corrupt_state_raises_value_error(self) -> None:
        self._seed()
        Path(self.state).write_text("{not json", encoding="utf-8")
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "rec-key", 50)

    def test_negative_zero_literal_in_state_raises_value_error(self) -> None:
        self._seed()
        Path(self.state).write_text(
            '{"version":1,"events":{"k":{"job_id":"j-1","source_id":"r-1",'
            '"target_id":"r-2","op":"abort","now":-0}}}',
            encoding="utf-8")
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "rec-key", 50)

    # -- persistence format ----------------------------------------------

    def test_compact_utf8_json_and_sorted_keys(self) -> None:
        self._seed()
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "键-rec", 50)
        raw = Path(self.state).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        text = raw.decode("utf-8")
        self.assertNotIn(" ", text.split("\n", 1)[0])
        data = json.loads(text)
        self.assertEqual(list(data.keys()), ["version", "events"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["events"].keys()),
                         ["prepare-key", "键-rec"])
        self.assertEqual(list(data["events"]["键-rec"].keys()),
                         ["job_id", "source_id", "target_id", "op", "now"])

    def test_no_tmp_files_left_behind(self) -> None:
        self._seed()
        run(self.jobs, self.offers, self.ledger, self.reserves, self.state,
            "j-1", "rec-key", 50)
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    # -- concurrency ------------------------------------------------------

    def test_equivalent_paths_share_lock(self) -> None:
        self._seed()
        relative = "state.json"
        cwd = os.getcwd()
        os.chdir(self.tmp.name)
        try:
            paths = [
                relative,
                os.path.abspath(relative),
                os.path.join(self.tmp.name, ".", "state.json"),
            ]
            errors: list[BaseException] = []
            created_count = 0
            counts_lock = threading.Lock()

            def worker(path: str, key: str) -> None:
                nonlocal created_count
                try:
                    _, created = run(self.jobs, self.offers, self.ledger,
                                     self.reserves, path, "j-1", key, 50)
                    with counts_lock:
                        if created:
                            created_count += 1
                except ValueError as exc:
                    with counts_lock:
                        errors.append(exc)

            threads = [
                threading.Thread(target=worker,
                                 args=(paths[i % len(paths)], f"k{i}"))
                for i in range(12)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
            # One distinct key wins; the other eleven find the migration
            # already finished -- linearizable, never corrupt.
            self.assertEqual(created_count, 1)
            self.assertEqual(len(errors), 11)
        finally:
            os.chdir(cwd)

    def test_recovery_and_manual_commit_linearize(self) -> None:
        self._seed()
        errors: list[BaseException] = []
        lock = threading.Lock()

        def recover_worker(key: str) -> None:
            try:
                run(self.jobs, self.offers, self.ledger, self.reserves,
                    self.state, "j-1", key, 50)
            except ValueError as exc:
                with lock:
                    errors.append(exc)

        def commit_worker(key: str) -> None:
            try:
                migrate(self.jobs, self.offers, self.ledger, self.reserves,
                        self.state, "j-1", "r-2", "commit", key, 50)
            except ValueError as exc:
                with lock:
                    errors.append(exc)

        # Every caller uses its own key, so the eleven losers cannot replay:
        # they must observe the migration as already finished.
        threads = [threading.Thread(target=recover_worker,
                                    args=(f"rec-{i}",)) for i in range(6)]
        threads += [threading.Thread(target=commit_worker,
                                     args=(f"com-{i}",)) for i in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        # Exactly one finisher wins; the other eleven see the finished
        # migration -- no corruption, no double terminal event.
        self.assertEqual(len(errors), 11)
        data = json.loads(Path(self.state).read_text(encoding="utf-8"))
        terminal = [event for event in data["events"].values()
                    if event["op"] in ("commit", "abort")]
        self.assertEqual(len(terminal), 1)

    def test_concurrent_same_key_serializes_to_one_event(self) -> None:
        self._seed()
        outcomes: list[tuple[dict[str, object], bool]] = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def worker() -> None:
            try:
                outcome = run(self.jobs, self.offers, self.ledger,
                              self.reserves, self.state,
                              "j-1", "rec-key", 50)
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
        terminal = [event for event in data["events"].values()
                    if event["op"] in ("commit", "abort")]
        self.assertEqual(len(terminal), 1)


if __name__ == "__main__":
    unittest.main()
