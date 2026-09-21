from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market.jobs import register as register_job
from carbon_market.market import match
from carbon_market.offers import register as register_offer
from carbon_market.reserve import run


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


class ReserveTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs = os.path.join(self.tmp.name, "jobs.json")
        self.offers = os.path.join(self.tmp.name, "offers.json")
        self.ledger = os.path.join(self.tmp.name, "ledger.json")
        self.state = os.path.join(self.tmp.name, "state.json")

    def _seed(self, job: dict[str, object] | None = None,
              offer: dict[str, object] | None = None,
              job_id: str = "j-1",
              job_key: str = "job-key",
              offer_key: str = "offer-key",
              match_key: str = "match-key") -> None:
        register_job(self.jobs,
                     job if job is not None else _job(job_id=job_id),
                     job_key)
        register_offer(self.offers,
                       offer if offer is not None else _offer(), offer_key)
        match(self.jobs, self.offers, self.ledger, job_id, match_key)

    # -- happy path -------------------------------------------------------

    def test_reserve_creates_state_and_returns_event(self) -> None:
        self._seed()
        event, created = run(self.jobs, self.offers, self.ledger, self.state,
                             "j-1", "reserve", "k-1", 50)
        self.assertTrue(created)
        self.assertEqual(event, {"job_id": "j-1", "resource_id": "r-1",
                                 "op": "reserve", "now": 50})
        self.assertEqual(list(event.keys()),
                         ["job_id", "resource_id", "op", "now"])
        raw = Path(self.state).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()), ["version", "events"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["events"].keys()), ["k-1"])
        self.assertEqual(list(data["events"]["k-1"].keys()),
                         ["job_id", "resource_id", "op", "now"])

    def _seed_matches(self, job_ids: list[str], capacity_wh: int = 150) -> None:
        """Register jobs/offers and write a match ledger directly.

        Unlike market.match, a hand-written ledger is not capacity-gated, so
        several jobs can be recorded against one offer whose capacity could
        not hold all of them at once -- the situation reserve() must then
        police.
        """
        register_offer(self.offers, _offer(capacity_wh=capacity_wh), "o")
        for index, job_id in enumerate(job_ids):
            register_job(self.jobs, _job(job_id=job_id), f"jk{index}")
        Path(self.ledger).write_text(json.dumps({
            "version": 1,
            "matches": {job_id: {"job_id": job_id, "resource_id": "r-1"}
                        for job_id in job_ids},
            "idempotency": {f"m{index}": job_id
                            for index, job_id in enumerate(job_ids)},
        }), encoding="utf-8")

    def test_cancel_returns_event_and_releases_capacity(self) -> None:
        self._seed_matches(["j-1", "j-2"], capacity_wh=150)
        run(self.jobs, self.offers, self.ledger, self.state,
            "j-1", "reserve", "k-r1", 10)
        # Only 50 Wh remain: j-2's reserve would overrun.
        with self.assertRaises(LookupError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-2", "reserve", "k-r2", 10)
        event, created = run(self.jobs, self.offers, self.ledger, self.state,
                             "j-1", "cancel", "k-c1", 20)
        self.assertTrue(created)
        self.assertEqual(event, {"job_id": "j-1", "resource_id": "r-1",
                                 "op": "cancel", "now": 20})
        # Released: j-2 now fits.
        event2, created2 = run(self.jobs, self.offers, self.ledger, self.state,
                               "j-2", "reserve", "k-r2", 30)
        self.assertTrue(created2)
        self.assertEqual(event2["resource_id"], "r-1")

    def test_deadline_equal_now_allowed(self) -> None:
        self._seed(job=_job(deadline=100))
        event, created = run(self.jobs, self.offers, self.ledger, self.state,
                             "j-1", "reserve", "k", 100)
        self.assertTrue(created)
        self.assertEqual(event["now"], 100)

    # -- idempotency ------------------------------------------------------

    def test_reserve_replay_returns_event_and_false(self) -> None:
        self._seed()
        first, c1 = run(self.jobs, self.offers, self.ledger, self.state,
                        "j-1", "reserve", "k", 50)
        replay, c2 = run(self.jobs, self.offers, self.ledger, self.state,
                         "j-1", "reserve", "k", 50)
        self.assertTrue(c1)
        self.assertFalse(c2)
        self.assertEqual(first, replay)
        data = json.loads(Path(self.state).read_text(encoding="utf-8"))
        self.assertEqual(len(data["events"]), 1)

    def test_replay_does_not_consume_capacity(self) -> None:
        self._seed_matches(["j-1", "j-2"], capacity_wh=150)
        run(self.jobs, self.offers, self.ledger, self.state,
            "j-1", "reserve", "k", 10)
        run(self.jobs, self.offers, self.ledger, self.state,
            "j-1", "reserve", "k", 10)
        with self.assertRaises(LookupError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-2", "reserve", "k2", 10)

    def test_same_key_different_event_params_raises(self) -> None:
        self._seed()
        run(self.jobs, self.offers, self.ledger, self.state,
            "j-1", "reserve", "k", 50)
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-1", "reserve", "k", 51)
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-1", "cancel", "k", 50)
        register_job(self.jobs, _job(job_id="j-2"), "jk2")
        match(self.jobs, self.offers, self.ledger, "j-2", "m2")
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-2", "reserve", "k", 50)

    def test_replay_skips_capacity_gate(self) -> None:
        self._seed()
        run(self.jobs, self.offers, self.ledger, self.state,
            "j-1", "reserve", "k", 50)
        # Shrink the offer below the already-recorded reservation; the
        # journal stays replayable even though a fresh reserve would not fit.
        Path(self.offers).write_text(json.dumps({
            "version": 1,
            "offers": {"r-1": {"resource_id": "r-1", "region": "eu-north",
                               "capacity_wh": 10, "unit_cost": 1000,
                               "carbon_intensity": 42}},
            "idempotency": {"offer-key": {"resource_id": "r-1"}},
        }), encoding="utf-8")
        replay, created = run(self.jobs, self.offers, self.ledger, self.state,
                              "j-1", "reserve", "k", 50)
        self.assertFalse(created)
        self.assertEqual(replay["resource_id"], "r-1")

    # -- lifecycle --------------------------------------------------------

    def test_double_reserve_raises_value_error(self) -> None:
        self._seed()
        run(self.jobs, self.offers, self.ledger, self.state,
            "j-1", "reserve", "k1", 10)
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-1", "reserve", "k2", 10)

    def test_cancel_without_reserve_raises_value_error(self) -> None:
        self._seed()
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-1", "cancel", "k", 10)

    def test_double_cancel_raises_value_error(self) -> None:
        self._seed()
        run(self.jobs, self.offers, self.ledger, self.state,
            "j-1", "reserve", "kr", 10)
        run(self.jobs, self.offers, self.ledger, self.state,
            "j-1", "cancel", "kc", 20)
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-1", "cancel", "kc2", 30)

    # -- gates ------------------------------------------------------------

    def test_unknown_job_raises_key_error(self) -> None:
        self._seed()
        with self.assertRaises(KeyError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-ghost", "reserve", "k", 10)

    def test_unmatched_job_raises_lookup_error(self) -> None:
        register_job(self.jobs, _job(job_id="j-1"), "jk1")
        register_job(self.jobs, _job(job_id="j-2"), "jk2")
        register_offer(self.offers, _offer(), "o")
        match(self.jobs, self.offers, self.ledger, "j-1", "m1")
        with self.assertRaises(LookupError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-2", "reserve", "k", 10)
        self.assertFalse(os.path.exists(self.state))

    def test_reserve_past_deadline_raises_timeout_without_write(self) -> None:
        self._seed()
        with self.assertRaises(TimeoutError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-1", "reserve", "k", 101)
        self.assertFalse(os.path.exists(self.state))

    def test_cancel_after_deadline_allowed(self) -> None:
        self._seed()
        run(self.jobs, self.offers, self.ledger, self.state,
            "j-1", "reserve", "kr", 10)
        event, _ = run(self.jobs, self.offers, self.ledger, self.state,
                       "j-1", "cancel", "kc", 10_000)
        self.assertEqual(event["op"], "cancel")

    def test_capacity_counts_active_reservations_only(self) -> None:
        self._seed_matches(["j-1", "j-2"], capacity_wh=150)
        run(self.jobs, self.offers, self.ledger, self.state,
            "j-1", "reserve", "r1", 1)
        with self.assertRaises(LookupError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-2", "reserve", "r2", 1)
        run(self.jobs, self.offers, self.ledger, self.state,
            "j-1", "cancel", "c1", 2)
        # Capacity is exactly back to 100 free.
        event, _ = run(self.jobs, self.offers, self.ledger, self.state,
                       "j-2", "reserve", "r2", 3)
        self.assertEqual(event["job_id"], "j-2")

    def test_capacity_lookup_error_writes_nothing(self) -> None:
        self._seed_matches(["j-1", "j-2"], capacity_wh=150)
        run(self.jobs, self.offers, self.ledger, self.state,
            "j-1", "reserve", "r1", 1)
        before = Path(self.state).read_bytes()
        with self.assertRaises(LookupError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-2", "reserve", "r2", 1)
        self.assertEqual(Path(self.state).read_bytes(), before)

    # -- persistence format ----------------------------------------------

    def test_compact_utf8_json_and_sorted_keys(self) -> None:
        register_job(self.jobs, _job(job_id="j-b"), "jk-b")
        register_job(self.jobs, _job(job_id="j-a"), "jk-a")
        register_offer(self.offers, _offer(resource_id="r-中"), "ok")
        match(self.jobs, self.offers, self.ledger, "j-b", "m-b")
        match(self.jobs, self.offers, self.ledger, "j-a", "m-a")
        run(self.jobs, self.offers, self.ledger, self.state,
            "j-b", "reserve", "键-b", 1)
        run(self.jobs, self.offers, self.ledger, self.state,
            "j-a", "reserve", "键-a", 1)
        raw = Path(self.state).read_text(encoding="utf-8")
        self.assertIn("r-中", raw)
        self.assertIn("键-a", raw)
        self.assertNotIn(" ", raw.split("\n", 1)[0])
        data = json.loads(raw)
        self.assertEqual(list(data["events"].keys()), ["键-a", "键-b"])
        self.assertEqual(
            data["events"]["键-a"],
            {"job_id": "j-a", "resource_id": "r-中", "op": "reserve",
             "now": 1})

    def test_keys_sort_by_code_point_regardless_of_arrival_order(self) -> None:
        register_job(self.jobs, _job(job_id="j1"), "jk1")
        register_job(self.jobs, _job(job_id="j2"), "jk2")
        register_offer(self.offers, _offer(capacity_wh=10_000), "o")
        match(self.jobs, self.offers, self.ledger, "j1", "m1")
        match(self.jobs, self.offers, self.ledger, "j2", "m2")
        run(self.jobs, self.offers, self.ledger, self.state,
            "j1", "reserve", "b", 1)
        run(self.jobs, self.offers, self.ledger, self.state,
            "j2", "reserve", "a", 2)
        data = json.loads(Path(self.state).read_text(encoding="utf-8"))
        self.assertEqual(list(data["events"].keys()), ["a", "b"])
        # Lifecycle survives the reordering on reload.
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j2", "reserve", "c", 3)
        event, created = run(self.jobs, self.offers, self.ledger, self.state,
                             "j2", "cancel", "c", 4)
        self.assertTrue(created)
        self.assertEqual(event["op"], "cancel")

    def test_seeded_state_replays(self) -> None:
        self._seed_matches(["j-1", "j-2"], capacity_wh=150)
        Path(self.state).write_text(json.dumps({
            "version": 1,
            "events": {
                "k": {"job_id": "j-1", "resource_id": "r-1",
                      "op": "reserve", "now": 5},
            },
        }), encoding="utf-8")
        event, created = run(self.jobs, self.offers, self.ledger, self.state,
                             "j-1", "reserve", "k", 5)
        self.assertFalse(created)
        self.assertEqual(event["now"], 5)
        # The seeded reserve occupies the energy.
        with self.assertRaises(LookupError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-2", "reserve", "k2", 6)

    # -- invalid arguments and files -------------------------------------

    def test_invalid_arguments(self) -> None:
        good = (self.jobs, self.offers, self.ledger, self.state,
                "j-1", "reserve", "k", 10)
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
                    run(self.jobs, self.offers, self.ledger, self.state,
                        "j-1", "reserve", "k", bad_now)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-1", "delete", "k", 10)

    def test_missing_inputs_raise_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-1", "reserve", "k", 10)
        register_job(self.jobs, _job(), "jk")
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-1", "reserve", "k", 10)
        register_offer(self.offers, _offer(), "ok")
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-1", "reserve", "k", 10)
        missing_parent = os.path.join(self.tmp.name, "no-such-dir", "x.json")
        with self.assertRaises(FileNotFoundError):
            run(missing_parent, self.offers, self.ledger, self.state,
                "j-1", "reserve", "k", 10)
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, missing_parent, self.ledger, self.state,
                "j-1", "reserve", "k", 10)
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, missing_parent, self.state,
                "j-1", "reserve", "k", 10)

    def test_missing_state_parent_raises_file_not_found(self) -> None:
        self._seed()
        state = os.path.join(self.tmp.name, "no-such-dir", "state.json")
        with self.assertRaises(FileNotFoundError):
            run(self.jobs, self.offers, self.ledger, state,
                "j-1", "reserve", "k", 10)

    def test_corrupt_files_raise_value_error(self) -> None:
        self._seed()
        for path in (self.jobs, self.offers, self.ledger, self.state):
            if path == self.state:
                run(self.jobs, self.offers, self.ledger, self.state,
                    "j-1", "reserve", "k-seed", 1)
            original = Path(path).read_bytes() if os.path.exists(path) else None
            Path(path).write_text("{not json", encoding="utf-8")
            with self.subTest(path=path):
                with self.assertRaises(ValueError):
                    run(self.jobs, self.offers, self.ledger, self.state,
                        "j-1", "reserve", "k", 2)
            if original is None:
                os.unlink(path)
            else:
                Path(path).write_bytes(original)

    def test_invalid_state_structures_raise_value_error(self) -> None:
        self._seed()
        good_event = {"job_id": "j-1", "resource_id": "r-1",
                      "op": "reserve", "now": 1}
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
            {"version": 1, "events": {"k": {"resource_id": "r-1",
                                            "op": "reserve", "now": 1}}},
            {"version": 1, "events": {"k": {"job_id": "", "resource_id": "r-1",
                                            "op": "reserve", "now": 1}}},
            {"version": 1, "events": {"k": {"job_id": "j-1",
                                            "resource_id": "",
                                            "op": "reserve", "now": 1}}},
            {"version": 1, "events": {"k": {"job_id": "j-1",
                                            "resource_id": "r-1",
                                            "op": "delete", "now": 1}}},
            {"version": 1, "events": {"k": {"job_id": "j-1",
                                            "resource_id": "r-1",
                                            "op": "reserve", "now": -1}}},
            {"version": 1, "events": {"k": {"job_id": "j-1",
                                            "resource_id": "r-1",
                                            "op": "reserve", "now": True}}},
            {"version": 1, "events": {"k": {"job_id": "ghost",
                                            "resource_id": "r-1",
                                            "op": "reserve", "now": 1}}},
            {"version": 1, "events": {"k": {"job_id": "j-1",
                                            "resource_id": "ghost",
                                            "op": "reserve", "now": 1}}},
            {"version": 1, "events": {"k": {"job_id": "j-1",
                                            "resource_id": "r-1",
                                            "op": "reserve", "now": 101}}},
            {"version": 1, "events": {
                "r1": {"job_id": "j-1", "resource_id": "r-1",
                       "op": "reserve", "now": 1},
                "r2": {"job_id": "j-1", "resource_id": "r-1",
                       "op": "reserve", "now": 2}}},
            {"version": 1, "events": {"c": {"job_id": "j-1",
                                            "resource_id": "r-1",
                                            "op": "cancel", "now": 2}}},
        ]
        for payload in bad_payloads:
            with self.subTest(payload=payload):
                Path(self.state).write_text(json.dumps(payload),
                                            encoding="utf-8")
                with self.assertRaises(ValueError):
                    run(self.jobs, self.offers, self.ledger, self.state,
                        "j-1", "reserve", "k-new", 5)

    def test_state_event_for_wrong_resource_raises_value_error(self) -> None:
        register_job(self.jobs, _job(job_id="j-1"), "jk")
        register_offer(self.offers, _offer(resource_id="r-1"), "o1")
        register_offer(self.offers, _offer(resource_id="r-2",
                                           carbon_intensity=100), "o2")
        match(self.jobs, self.offers, self.ledger, "j-1", "m")
        Path(self.state).write_text(json.dumps({
            "version": 1,
            "events": {"k": {"job_id": "j-1", "resource_id": "r-2",
                             "op": "reserve", "now": 1}},
        }), encoding="utf-8")
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-1", "reserve", "k2", 2)

    def test_negative_zero_literal_in_state_raises_value_error(self) -> None:
        self._seed()
        Path(self.state).write_text(
            '{"version":1,"events":{"k":{"job_id":"j-1","resource_id":"r-1",'
            '"op":"reserve","now":-0}}}',
            encoding="utf-8")
        with self.assertRaises(ValueError):
            run(self.jobs, self.offers, self.ledger, self.state,
                "j-1", "reserve", "k2", 2)

    # -- concurrency / files ---------------------------------------------

    def test_equivalent_paths_share_lock_and_concurrency(self) -> None:
        register_offer(self.offers, _offer(capacity_wh=10_000), "o")
        for index in range(30):
            jid = f"j{index:03d}"
            register_job(self.jobs, _job(job_id=jid), f"jk{index}")
            match(self.jobs, self.offers, self.ledger, jid, f"m{index}")
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
            results: list[tuple[dict[str, object], bool]] = []
            lock = threading.Lock()

            def worker(path: str, index: int) -> None:
                try:
                    outcome = run(self.jobs, self.offers, self.ledger, path,
                                  f"j{index:03d}", "reserve",
                                  f"k{index:03d}", index)
                    with lock:
                        results.append(outcome)
                except BaseException as exc:  # noqa: BLE001 - report all
                    with lock:
                        errors.append(exc)

            threads = [threading.Thread(target=worker,
                                        args=(paths[i % len(paths)], i))
                       for i in range(30)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
            self.assertEqual(errors, [])
            self.assertEqual(len(results), 30)
            self.assertTrue(all(created for _, created in results))
            data = json.loads(Path(relative).read_text(encoding="utf-8"))
            self.assertEqual(len(data["events"]), 30)
        finally:
            os.chdir(cwd)

    def test_concurrent_same_key_serializes_to_one_event(self) -> None:
        self._seed()
        outcomes: list[tuple[dict[str, object], bool]] = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def worker() -> None:
            try:
                outcome = run(self.jobs, self.offers, self.ledger, self.state,
                              "j-1", "reserve", "k", 10)
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
        run(self.jobs, self.offers, self.ledger, self.state,
            "j-1", "reserve", "kr", 1)
        run(self.jobs, self.offers, self.ledger, self.state,
            "j-1", "cancel", "kc", 2)
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])


if __name__ == "__main__":
    unittest.main()
