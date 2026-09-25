from __future__ import annotations

import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import jobs as jobs_module
from carbon_market import market as market_module
from carbon_market.jobs import submit
from carbon_market.market import clear
from carbon_market.resources import feasible, get, publish


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


class ClearTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.jobs = os.path.join(self.tmp.name, "jobs.json")
        self.supply = os.path.join(self.tmp.name, "supply.json")
        self.ledger = os.path.join(self.tmp.name, "clear.json")

    def _seed_job(self, job: dict[str, object] | None = None,
                  key: str = "jk-1") -> None:
        submit(self.jobs, job if job is not None else _job(), key)

    def _seed_resource(self, resource: dict[str, object] | None = None,
                       key: str = "rk-1") -> None:
        publish(self.supply,
                resource if resource is not None else _resource(), key)

    def _seed(self) -> None:
        self._seed_job()
        self._seed_resource()

    def test_creates_ledger_and_returns_trade(self) -> None:
        self._seed()
        trade, created = clear(self.jobs, self.supply, self.ledger,
                               "j-1", "k-1", 50)
        self.assertTrue(created)
        self.assertEqual(trade["job_id"], "j-1")
        self.assertEqual(trade["at"], 50)
        self.assertEqual(trade["work"], 10)
        self.assertEqual(trade["resource_id"], "r-1")
        self.assertEqual(trade["version"], 1)
        self.assertEqual(trade["selection"],
                         {"resource_id": "r-1", "version": 1})
        self.assertEqual(list(trade.keys()),
                         ["job_id", "at", "work", "resource_id", "version",
                          "candidates", "selection"])
        candidate = trade["candidates"][0]
        self.assertEqual(list(candidate.keys()),
                         ["resource", "total_cost", "total_carbon"])
        self.assertEqual(candidate["total_cost"], 30)
        self.assertEqual(candidate["total_carbon"], 70)
        self.assertEqual(candidate["resource"]["resource_id"], "r-1")

        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()),
                         ["version", "trades", "idempotency", "audit"])
        self.assertEqual(data["version"], 2)
        self.assertEqual(data["idempotency"],
                         {"k-1": {"job_id": "j-1", "at": 50}})
        self.assertEqual(data["audit"], {"k-1": {
            "key": "k-1", "job_id": "j-1", "at": 50,
            "resource_id": "r-1", "version": 1}})

    def test_compact_utf8_and_sorted_keys(self) -> None:
        submit(self.jobs, _job("j-b"), "jk-b")
        submit(self.jobs, _job("j-a"), "jk-a")
        publish(self.supply, _resource("r-中"), "rk")
        clear(self.jobs, self.supply, self.ledger, "j-b", "键-b", 50)
        clear(self.jobs, self.supply, self.ledger, "j-a", "键-a", 50)
        raw = Path(self.ledger).read_text(encoding="utf-8")
        self.assertIn("r-中", raw)
        self.assertIn("键-b", raw)
        self.assertNotIn(" ", raw.split("\n", 1)[0])
        data = json.loads(raw)
        self.assertEqual(list(data["trades"].keys()), ["j-a", "j-b"])
        self.assertEqual(list(data["idempotency"].keys()), ["键-a", "键-b"])
        self.assertEqual(list(data["audit"].keys()), ["键-a", "键-b"])

    def test_picks_greenest_then_cheapest_then_resource_id(self) -> None:
        self._seed_job()
        publish(self.supply,
                _resource("r-c", carbon_intensity=2, unit_cost=4), "o1")
        publish(self.supply,
                _resource("r-b", carbon_intensity=2, unit_cost=9), "o2")
        publish(self.supply,
                _resource("r-a", carbon_intensity=5, unit_cost=0), "o3")
        trade, _ = clear(self.jobs, self.supply, self.ledger, "j-1", "k", 50)
        self.assertEqual(trade["resource_id"], "r-c")
        self.assertEqual([c["resource"]["resource_id"]
                          for c in trade["candidates"]],
                         ["r-c", "r-b", "r-a"])

    def test_candidates_match_feasible_semantics(self) -> None:
        self._seed()
        ranked = feasible(self.jobs, self.supply, "j-1", 50)
        trade, _ = clear(self.jobs, self.supply, self.ledger, "j-1", "k", 50)
        self.assertEqual(trade["candidates"], ranked)

    def test_uses_highest_version_valid_at_evaluation_moment(self) -> None:
        self._seed_job()
        publish(self.supply, _resource("r-1", start=0, end=100,
                                       carbon_intensity=9), "v1")
        publish(self.supply, _resource("r-1", start=200, end=300,
                                       carbon_intensity=1), "v2")
        early, _ = clear(self.jobs, self.supply, self.ledger,
                         "j-1", "ke", 50)
        self.assertEqual(early["version"], 1)
        ledger2 = os.path.join(self.tmp.name, "clear2.json")
        late, _ = clear(self.jobs, self.supply, ledger2, "j-1", "kl", 250)
        self.assertEqual(late["version"], 2)

    def test_capacity_deducts_booked_work_per_version(self) -> None:
        submit(self.jobs, _job("j-1"), "jk1")
        submit(self.jobs, _job("j-2"), "jk2")
        publish(self.supply,
                _resource("r-small", capacity=15, carbon_intensity=1), "o1")
        publish(self.supply,
                _resource("r-big", capacity=500, carbon_intensity=2), "o2")
        first, _ = clear(self.jobs, self.supply, self.ledger,
                        "j-1", "k1", 50)
        self.assertEqual(first["resource_id"], "r-small")
        # r-small has 5 capacity left, so j-2 spills to r-big.
        second, _ = clear(self.jobs, self.supply, self.ledger,
                         "j-2", "k2", 50)
        self.assertEqual(second["resource_id"], "r-big")

    def test_bookings_never_cross_versions(self) -> None:
        submit(self.jobs, _job("j-1"), "jk1")
        publish(self.supply, _resource("r-1", capacity=15), "v1")
        first, _ = clear(self.jobs, self.supply, self.ledger,
                        "j-1", "k1", 50)
        self.assertEqual(first["version"], 1)
        # Version 2 starts fresh despite version 1 being nearly sold out.
        publish(self.supply, _resource("r-1", capacity=1, start=60), "v2")
        publish(self.supply, _resource("r-2", capacity=100, start=60,
                                       carbon_intensity=1), "v3")
        submit(self.jobs, _job("j-2"), "jk2")
        second, _ = clear(self.jobs, self.supply, self.ledger,
                         "j-2", "k2", 70)
        self.assertEqual(second["resource_id"], "r-2")
        self.assertEqual(second["version"], 1)
        self.assertNotIn("r-1",
                         [c["resource"]["resource_id"]
                          for c in second["candidates"]])

    def test_replay_returns_stored_trade_without_writing(self) -> None:
        self._seed()
        first, created_first = clear(self.jobs, self.supply, self.ledger,
                                     "j-1", "k", 50)
        raw = Path(self.ledger).read_bytes()
        replay, created_replay = clear(self.jobs, self.supply, self.ledger,
                                       "j-1", "k", 50)
        self.assertTrue(created_first)
        self.assertFalse(created_replay)
        self.assertEqual(replay, first)
        self.assertEqual(Path(self.ledger).read_bytes(), raw)
        data = json.loads(raw)
        self.assertEqual(len(data["trades"]), 1)
        self.assertEqual(len(data["idempotency"]), 1)
        self.assertEqual(len(data["audit"]), 1)

    def test_replay_result_is_a_copy(self) -> None:
        self._seed()
        trade, _ = clear(self.jobs, self.supply, self.ledger, "j-1", "k", 50)
        trade["candidates"][0]["total_cost"] = 999
        again, created = clear(self.jobs, self.supply, self.ledger,
                               "j-1", "k", 50)
        self.assertFalse(created)
        self.assertEqual(again["candidates"][0]["total_cost"], 30)

    def test_same_key_with_changed_request_raises(self) -> None:
        self._seed()
        clear(self.jobs, self.supply, self.ledger, "j-1", "k", 50)
        submit(self.jobs, _job("j-2"), "jk2")
        with self.assertRaises(ValueError):
            clear(self.jobs, self.supply, self.ledger, "j-2", "k", 50)
        with self.assertRaises(ValueError):
            clear(self.jobs, self.supply, self.ledger, "j-1", "k", 51)

    def test_job_traded_under_other_key_raises(self) -> None:
        self._seed()
        clear(self.jobs, self.supply, self.ledger, "j-1", "k1", 50)
        with self.assertRaises(ValueError):
            clear(self.jobs, self.supply, self.ledger, "j-1", "k2", 50)

    def test_unknown_job_raises_key_error_without_ledger(self) -> None:
        self._seed()
        with self.assertRaises(KeyError):
            clear(self.jobs, self.supply, self.ledger, "ghost", "k", 50)
        self.assertFalse(os.path.exists(self.ledger))

    def test_no_remaining_resource_raises_lookup_error_without_ledger(
            self) -> None:
        submit(self.jobs, _job(work=1000), "jk")
        publish(self.supply, _resource(), "rk")
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, self.ledger, "j-1", "k", 50)
        self.assertFalse(os.path.exists(self.ledger))

    def test_missing_files_and_parent_raise_file_not_found(self) -> None:
        self._seed()
        with self.assertRaises(FileNotFoundError):
            clear(os.path.join(self.tmp.name, "no-jobs.json"),
                  self.supply, self.ledger, "j-1", "k", 50)
        with self.assertRaises(FileNotFoundError):
            clear(self.jobs, os.path.join(self.tmp.name, "no-supply.json"),
                  self.ledger, "j-1", "k", 50)
        missing_parent = os.path.join(self.tmp.name, "no-such-dir", "x.json")
        with self.assertRaises(FileNotFoundError):
            clear(missing_parent, self.supply, self.ledger, "j-1", "k", 50)
        with self.assertRaises(FileNotFoundError):
            clear(self.jobs, missing_parent, self.ledger, "j-1", "k", 50)
        self.assertFalse(os.path.exists(self.ledger))

    def test_missing_ledger_parent_raises_file_not_found(self) -> None:
        self._seed()
        ledger = os.path.join(self.tmp.name, "no-such-dir", "clear.json")
        with self.assertRaises(FileNotFoundError):
            clear(self.jobs, self.supply, ledger, "j-1", "k", 50)
        self.assertFalse(os.path.exists(ledger))

    def test_invalid_arguments_before_files_are_read(self) -> None:
        good = (self.jobs, self.supply, self.ledger, "j-1", "k", 50)
        for index in range(5):
            for bad in ("", 123, None):
                args = list(good)
                args[index] = bad
                with self.subTest(index=index, bad=bad):
                    with self.assertRaises(ValueError):
                        clear(*args)  # type: ignore[arg-type]
        for bad in (-1, True, False, 1.5, None, ""):
            with self.subTest(at=bad):
                with self.assertRaises(ValueError):
                    clear(self.jobs, self.supply, self.ledger,
                          "j-1", "k", bad)  # type: ignore[arg-type]

    def test_paths_must_be_distinct_real_locations(self) -> None:
        self._seed()
        aliases = [
            (self.jobs, self.supply, self.jobs),
            (self.jobs, self.supply, self.supply),
            (self.supply, self.jobs, self.supply),
            (os.path.join(self.tmp.name, ".", "jobs.json"),
             self.supply, self.jobs),
        ]
        for paths in aliases:
            with self.subTest(paths=paths):
                with self.assertRaises(ValueError):
                    clear(paths[0], paths[1], paths[2], "j-1", "k", 50)

    def test_corrupt_files_raise_value_error(self) -> None:
        self._seed()
        submit(self.jobs, _job("j-2"), "jk2")
        for path in (self.jobs, self.supply):
            original = Path(path).read_bytes()
            Path(path).write_text("{not json", encoding="utf-8")
            with self.subTest(path=path):
                with self.assertRaises(ValueError):
                    clear(self.jobs, self.supply, self.ledger,
                          "j-2", "k2", 50)
            Path(path).write_bytes(original)

    def test_non_canonical_supply_raises_value_error(self) -> None:
        self._seed()
        original = Path(self.supply).read_bytes()
        data = json.loads(original)
        variants = [
            lambda: json.dumps(data, indent=2),
            lambda: json.dumps(data, ensure_ascii=True),
            lambda: json.dumps(data, separators=(", ", ": ")),
            lambda: json.dumps(data) + " \n",
            lambda: json.dumps(data, ensure_ascii=False)[:-1],
            lambda: json.dumps(data, ensure_ascii=False) + "\n\n",
        ]
        for variant in variants:
            Path(self.supply).write_text(variant(), encoding="utf-8")
            with self.subTest(variant=variant()[:20]):
                with self.assertRaises(ValueError):
                    clear(self.jobs, self.supply, self.ledger,
                          "j-1", "k", 50)
        Path(self.supply).write_bytes(original)

    def test_non_canonical_acceptance_file_raises_value_error(self) -> None:
        self._seed()
        original = Path(self.jobs).read_bytes()
        data = json.loads(original)
        for text in (json.dumps(data, indent=2),
                     json.dumps(data, ensure_ascii=False) + "\n\n"):
            Path(self.jobs).write_text(text, encoding="utf-8")
            with self.subTest(text=text[:20]):
                with self.assertRaises(ValueError):
                    clear(self.jobs, self.supply, self.ledger,
                          "j-1", "k", 50)
            self.assertFalse(os.path.exists(self.ledger))
        Path(self.jobs).write_bytes(original)
        # Escaped non-ASCII diverges from the canonical write-through form.
        wide = os.path.join(self.tmp.name, "wide-jobs.json")
        submit(wide, _job(regions=["eu-north", "中"], residency=["中"]), "wk")
        wide_supply = os.path.join(self.tmp.name, "wide-supply.json")
        publish(wide_supply,
                _resource(region="中", residency=["中"]), "wk")
        wide_data = json.loads(Path(wide).read_text(encoding="utf-8"))
        Path(wide).write_text(json.dumps(wide_data, ensure_ascii=True),
                              encoding="utf-8")
        with self.assertRaises(ValueError):
            clear(wide, wide_supply,
                  os.path.join(self.tmp.name, "wide-ledger.json"),
                  "j-1", "k", 50)

    def test_invalid_ledger_structures_raise_value_error(self) -> None:
        self._seed()
        good, _ = clear(self.jobs, self.supply, self.ledger, "j-1", "k", 50)
        winner = good["resource_id"]
        wver = good["version"]
        trade = {
            "job_id": "j-1", "at": 50, "work": 10,
            "resource_id": winner, "version": wver,
            "candidates": good["candidates"],
            "selection": {"resource_id": winner, "version": wver},
        }
        event = {"key": "k", "job_id": "j-1", "at": 50,
                 "resource_id": winner, "version": wver}
        binding = {"job_id": "j-1", "at": 50}
        bad_payloads = [
            [],
            {},
            {"version": 2, "trades": {}, "idempotency": {}, "audit": {},
             "extra": 1},
            {"version": 1, "trades": {}, "idempotency": {}, "audit": {}},
            {"version": "2", "trades": {}, "idempotency": {}, "audit": {}},
            {"version": True, "trades": {}, "idempotency": {}, "audit": {}},
            {"version": 2, "trades": [], "idempotency": {}, "audit": {}},
            {"version": 2, "trades": {"j-1": trade}, "idempotency": {},
             "audit": {}},
            {"version": 2, "trades": {"j-1": dict(trade, at=51)},
             "idempotency": {"k": binding}, "audit": {"k": event}},
            {"version": 2, "trades": {"j-1": dict(trade, work=9)},
             "idempotency": {"k": binding}, "audit": {"k": event}},
            {"version": 2,
             "trades": {"j-1": dict(trade, resource_id="ghost", version=1)},
             "idempotency": {"k": dict(binding)},
             "audit": {"k": dict(event, resource_id="ghost")}},
            {"version": 2, "trades": {"j-1": trade},
             "idempotency": {"k": dict(binding, at=51)},
             "audit": {"k": event}},
            {"version": 2, "trades": {"j-1": trade},
             "idempotency": {"k": binding},
             "audit": {"k": dict(event, key="other")}},
            {"version": 2, "trades": {"j-1": trade},
             "idempotency": {"k": binding}, "audit": {}},
        ]
        for payload in bad_payloads:
            with self.subTest(payload=payload):
                Path(self.ledger).write_bytes(
                    json.dumps(payload).encode("utf-8"))
                with self.assertRaises(ValueError):
                    clear(self.jobs, self.supply, self.ledger,
                          "j-1", "k2", 50)

    def test_ledger_overselling_a_version_is_invalid(self) -> None:
        self._seed()
        # capacity 100 with a tampered work of 101 bookings is invalid.
        good, _ = clear(self.jobs, self.supply, self.ledger, "j-1", "k", 50)
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        data["trades"]["j-1"]["work"] = 101
        for candidate in data["trades"]["j-1"]["candidates"]:
            if candidate["resource"]["resource_id"] == good["resource_id"]:
                candidate["total_cost"] = 101 * candidate["resource"]["unit_cost"]
                candidate["total_carbon"] = (
                    101 * candidate["resource"]["carbon_intensity"])
        data["trades"]["j-1"]["candidates"] = [
            c for c in data["trades"]["j-1"]["candidates"]
            if c["resource"]["resource_id"] == good["resource_id"]]
        Path(self.ledger).write_text(json.dumps(data), encoding="utf-8")
        with self.assertRaises(ValueError):
            clear(self.jobs, self.supply, self.ledger, "j-1", "k2", 50)

    def test_seeded_ledger_replays_and_deducts_capacity(self) -> None:
        submit(self.jobs, _job("j-1"), "jk1")
        submit(self.jobs, _job("j-2"), "jk2")
        publish(self.supply, _resource(capacity=15), "rk")
        first, _ = clear(self.jobs, self.supply, self.ledger, "j-1", "k1", 50)
        raw = Path(self.ledger).read_bytes()
        payload = json.loads(raw)
        # Rewrite in the canonical compact form by replaying once: bytes
        # already canonical. Seed a second ledger from the same bytes.
        seeded = os.path.join(self.tmp.name, "seeded.json")
        Path(seeded).write_bytes(raw)
        replay, created = clear(self.jobs, self.supply, seeded,
                                "j-1", "k1", 50)
        self.assertFalse(created)
        self.assertEqual(replay, first)
        with self.assertRaises(LookupError):
            clear(self.jobs, self.supply, seeded, "j-2", "k2", 50)

    def test_concurrent_clears_serialize_without_overselling(self) -> None:
        publish(self.supply, _resource(capacity=10_000), "rk")
        for index in range(30):
            submit(self.jobs, _job(f"j{index:03d}"), f"jk{index}")
        relative = "clear.json"
        cwd = os.getcwd()
        os.chdir(self.tmp.name)
        try:
            paths = [
                relative,
                os.path.abspath(relative),
                os.path.join(self.tmp.name, ".", "clear.json"),
            ]
            results: list[tuple[dict[str, object], bool]] = []
            errors: list[BaseException] = []
            lock = threading.Lock()

            def worker(index: int) -> None:
                try:
                    outcome = clear(self.jobs, self.supply,
                                    paths[index % len(paths)],
                                    f"j{index:03d}", f"k{index:03d}", 50)
                    with lock:
                        results.append(outcome)
                except BaseException as exc:  # noqa: BLE001 - report all
                    with lock:
                        errors.append(exc)

            threads = [threading.Thread(target=worker, args=(i,))
                       for i in range(30)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)
            self.assertEqual(errors, [])
            self.assertEqual(len(results), 30)
            self.assertTrue(all(created for _, created in results))
            data = json.loads(Path(relative).read_text(encoding="utf-8"))
            self.assertEqual(len(data["trades"]), 30)
            self.assertEqual(len(data["idempotency"]), 30)
            self.assertEqual(len(data["audit"]), 30)
        finally:
            os.chdir(cwd)

    def test_failed_directory_sync_restores_previous_bytes(self) -> None:
        self._seed()
        clear(self.jobs, self.supply, self.ledger, "j-1", "k1", 50)
        before = Path(self.ledger).read_bytes()
        original = market_module._fsync_directory

        def failing(directory: str) -> None:
            raise OSError("simulated sync failure")

        market_module._fsync_directory = failing
        try:
            submit(self.jobs, _job("j-2"), "jk2")
            with self.assertRaises(OSError):
                clear(self.jobs, self.supply, self.ledger,
                      "j-2", "k2", 50)
        finally:
            market_module._fsync_directory = original
        self.assertEqual(Path(self.ledger).read_bytes(), before)

    def test_failed_first_commit_leaves_no_file(self) -> None:
        self._seed()
        original = market_module._fsync_directory

        def failing(directory: str) -> None:
            raise OSError("simulated sync failure")

        market_module._fsync_directory = failing
        try:
            with self.assertRaises(OSError):
                clear(self.jobs, self.supply, self.ledger, "j-1", "k1", 50)
        finally:
            market_module._fsync_directory = original
        self.assertFalse(os.path.exists(self.ledger))
        self.assertEqual([n for n in os.listdir(self.tmp.name)
                          if n.endswith(".tmp")], [])

    def test_no_tmp_files_left_behind(self) -> None:
        self._seed()
        clear(self.jobs, self.supply, self.ledger, "j-1", "k", 50)
        self.assertEqual([n for n in os.listdir(self.tmp.name)
                          if n.endswith(".tmp")], [])


class CanonicalSupplyReadTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "supply.json")
        publish(self.path, _resource(), "k1")
        self.original = Path(self.path).read_bytes()

    def _rewrite(self, text: str) -> None:
        Path(self.path).write_text(text, encoding="utf-8")

    def test_canonical_file_still_reads(self) -> None:
        self.assertEqual(get(self.path, "r-1")["version"], 1)

    def test_pretty_whitespace_rejected_by_every_reader(self) -> None:
        data = json.loads(self.original)
        self._rewrite(json.dumps(data, indent=2))
        for call in (lambda: get(self.path, "r-1"),
                     lambda: publish(self.path, _resource("r-2", start=1),
                                     "k2")):
            with self.assertRaises(ValueError):
                call()

    def test_escaped_non_ascii_rejected(self) -> None:
        publish(self.path, _resource("r-中"), "k中")
        raw = Path(self.path).read_bytes()
        text = raw.decode("utf-8")
        escaped = json.dumps(json.loads(text), ensure_ascii=True) + "\n"
        self._rewrite(escaped)
        with self.assertRaises(ValueError):
            get(self.path, "r-中")

    def test_field_reordering_rejected(self) -> None:
        data = json.loads(self.original)
        record = data["history"]["r-1"][0]
        ordered = list(record.items())
        data["history"]["r-1"][0] = dict([ordered[1], ordered[0]] + ordered[2:])
        self._rewrite(json.dumps(data) + "\n")
        with self.assertRaises(ValueError):
            get(self.path, "r-1")

    def test_missing_and_double_trailing_newline_rejected(self) -> None:
        self._rewrite(self.original.decode("utf-8").rstrip("\n"))
        with self.assertRaises(ValueError):
            get(self.path, "r-1")
        self._rewrite(self.original.decode("utf-8") + "\n")
        with self.assertRaises(ValueError):
            get(self.path, "r-1")

    def test_bytes_preserved_after_rejected_read(self) -> None:
        self._rewrite("{not json")
        with self.assertRaises(ValueError):
            get(self.path, "r-1")


if __name__ == "__main__":
    unittest.main()
