from __future__ import annotations

import json
import multiprocessing
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import jobs as jobs_module
from carbon_market.jobs import get, register, submit


def _job(job_id: str = "j-1", **overrides: object) -> dict[str, object]:
    job: dict[str, object] = {
        "job_id": job_id,
        "work": 10,
        "deadline": 100,
        "regions": ["eu-north", "us-west"],
        "residency": ["eu-north"],
        "max_cost": 500,
        "carbon_cap": 42,
    }
    job.update(overrides)
    return job


def _seed(path: str, *specs: tuple[str, str]) -> None:
    for job_id, key in specs:
        submit(path, _job(job_id), key)


def _valid_document() -> dict[str, object]:
    return {
        "version": 2,
        "jobs": {
            "j-1": {
                "job_id": "j-1",
                "work": 10,
                "deadline": 100,
                "regions": ["eu-north", "us-west"],
                "residency": ["eu-north"],
                "max_cost": 500,
                "carbon_cap": 42,
                "state": "queued",
            },
        },
        "idempotency": {"key-1": "j-1"},
        "events": {
            "key-1": {
                "type": "submit",
                "idempotency_key": "key-1",
                "job_id": "j-1",
                "result": "queued",
            },
        },
    }


def _mp_submit_distinct(args: tuple[str, int]) -> bool:
    path, index = args
    _, created = submit(path, _job(f"j{index:03d}"), f"k{index:03d}")
    return created


def _mp_submit_same(path: str) -> bool:
    _, created = submit(path, _job("dup"), "dup-key")
    return created


class SubmitTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "acceptance.json")

    def _write(self, document: object) -> None:
        Path(self.path).write_text(json.dumps(document, ensure_ascii=False),
                                   encoding="utf-8")

    def test_creates_file_and_returns_record(self) -> None:
        record, created = submit(self.path, _job(), "key-1")
        self.assertTrue(created)
        self.assertEqual(record, {
            "job_id": "j-1",
            "work": 10,
            "deadline": 100,
            "regions": ["eu-north", "us-west"],
            "residency": ["eu-north"],
            "max_cost": 500,
            "carbon_cap": 42,
            "state": "queued",
        })
        self.assertTrue(os.path.exists(self.path))
        raw = Path(self.path).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        data = json.loads(raw)
        self.assertEqual(list(data.keys()),
                         ["version", "jobs", "idempotency", "events"])
        self.assertEqual(data["version"], 2)
        self.assertEqual(list(data["jobs"].keys()), ["j-1"])
        self.assertEqual(list(data["jobs"]["j-1"].keys()),
                         ["job_id", "work", "deadline", "regions",
                          "residency", "max_cost", "carbon_cap", "state"])
        self.assertEqual(data["idempotency"], {"key-1": "j-1"})
        self.assertEqual(data["events"], {
            "key-1": {"type": "submit", "idempotency_key": "key-1",
                      "job_id": "j-1", "result": "queued"},
        })

    def test_regions_sorted_by_code_point(self) -> None:
        record, _ = submit(
            self.path,
            _job(regions=["z", "a", "中", "A"],
                 residency=["中", "A"]),
            "k")
        self.assertEqual(record["regions"], ["A", "a", "z", "中"])
        self.assertEqual(record["residency"], ["A", "中"])

    def test_compact_utf8_json(self) -> None:
        submit(self.path, _job(job_id="j-中"), "键")
        raw = Path(self.path).read_text(encoding="utf-8")
        self.assertIn("j-中", raw)
        self.assertIn("键", raw)
        self.assertNotIn(" ", raw.split("\n", 1)[0])

    def test_idempotent_replay_is_read_only(self) -> None:
        first, created_first = submit(
            self.path,
            _job(regions=["us-west", "eu-north"],
                 residency=["eu-north"]),
            "key")
        raw_after_create = Path(self.path).read_bytes()
        second, created_second = submit(
            self.path,
            _job(regions=["eu-north", "us-west"],
                 residency=["eu-north"]),
            "key")
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first, second)
        self.assertEqual(Path(self.path).read_bytes(), raw_after_create)
        data = json.loads(raw_after_create)
        self.assertEqual(len(data["jobs"]), 1)
        self.assertEqual(len(data["idempotency"]), 1)
        self.assertEqual(len(data["events"]), 1)

    def test_same_key_different_job_raises(self) -> None:
        submit(self.path, _job(), "key")
        raw = Path(self.path).read_bytes()
        variants = [
            _job(work=11),
            _job(deadline=101),
            _job(regions=["eu-north", "us-west", "ap-south"]),
            _job(residency=["us-west"]),
            _job(max_cost=499),
            _job(carbon_cap=41),
            _job(job_id="j-2"),
        ]
        for variant in variants:
            with self.subTest(variant=variant):
                with self.assertRaises(ValueError):
                    submit(self.path, variant, "key")
                self.assertEqual(Path(self.path).read_bytes(), raw)

    def test_same_job_id_other_key_raises_and_preserves_bytes(self) -> None:
        submit(self.path, _job(), "key-1")
        raw = Path(self.path).read_bytes()
        with self.assertRaises(ValueError):
            submit(self.path, _job(), "key-2")
        self.assertEqual(Path(self.path).read_bytes(), raw)

    def test_distinct_jobs_persist_sorted(self) -> None:
        submit(self.path, _job(job_id="j3"), "k3")
        submit(self.path, _job(job_id="j1"), "k1")
        submit(self.path, _job(job_id="j2"), "k2")
        data = json.loads(Path(self.path).read_text(encoding="utf-8"))
        self.assertEqual(list(data["jobs"].keys()), ["j1", "j2", "j3"])
        self.assertEqual(list(data["idempotency"].keys()),
                         ["k1", "k2", "k3"])
        self.assertEqual(list(data["events"].keys()), ["k1", "k2", "k3"])
        for key, record in data["events"].items():
            self.assertEqual(data["idempotency"][key], record["job_id"])

    def test_invalid_arguments(self) -> None:
        cases = [
            ("empty id", _job(job_id="")),
            ("non-string id", _job(job_id=1)),
            ("zero work", _job(work=0)),
            ("negative work", _job(work=-1)),
            ("bool work", _job(work=True)),
            ("float work", _job(work=1.5)),
            ("negative deadline", _job(deadline=-1)),
            ("bool deadline", _job(deadline=False)),
            ("float deadline", _job(deadline=1.5)),
            ("negative max_cost", _job(max_cost=-1)),
            ("bool max_cost", _job(max_cost=True)),
            ("negative carbon_cap", _job(carbon_cap=-1)),
            ("bool carbon_cap", _job(carbon_cap=False)),
            ("empty regions", _job(regions=["eu-north"], residency=[])),
            ("empty residency",
             _job(regions=["a"], residency=[])),
            ("non-string region", _job(regions=["a", 1])),
            ("empty region", _job(regions=["a", ""])),
            ("duplicate region", _job(regions=["a", "a"], residency=["a"])),
            ("duplicate residency",
             _job(regions=["a", "b"], residency=["a", "a"])),
            ("residency not subset",
             _job(regions=["a", "b"], residency=["c"])),
            ("regions not a list", _job(regions=("a", "b"), residency=["a"])),
            ("extra field", _job(state="queued")),
            ("missing field",
             {"job_id": "x", "work": 1, "deadline": 2, "regions": ["a"],
              "residency": ["a"], "max_cost": 3}),
            ("not a dict", ["x"]),
        ]
        for label, bad_job in cases:
            with self.subTest(label):
                with self.assertRaises(ValueError):
                    submit(self.path, bad_job, "key")  # type: ignore[arg-type]

        with self.assertRaises(ValueError):
            submit(self.path, _job(), "")
        with self.assertRaises(ValueError):
            submit(self.path, _job(), None)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            submit(self.path, _job(), 123)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            submit(123, _job(), "key")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            submit("", _job(), "key")

    def test_missing_parent_directory_raises_file_not_found(self) -> None:
        missing = os.path.join(self.tmp.name, "no-such-dir", "acceptance.json")
        with self.assertRaises(FileNotFoundError):
            submit(missing, _job(), "key")
        self.assertFalse(os.path.exists(missing))

    def test_equivalent_paths_share_lock_and_concurrency(self) -> None:
        relative = "acceptance.json"
        cwd = os.getcwd()
        os.chdir(self.tmp.name)
        try:
            paths = [
                relative,
                os.path.abspath(relative),
                os.path.join(self.tmp.name, ".", "acceptance.json"),
            ]
            errors: list[BaseException] = []

            def worker(path: str, index: int) -> None:
                try:
                    submit(path, _job(f"j{index:03d}"), f"k{index:03d}")
                except BaseException as exc:  # noqa: BLE001 - report all
                    errors.append(exc)

            threads = [threading.Thread(target=worker, args=(path, i))
                       for i, path in enumerate(paths * 20)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)
            self.assertEqual(errors, [])
            data = json.loads(Path("acceptance.json").read_text("utf-8"))
            self.assertEqual(len(data["jobs"]), 60)
            self.assertEqual(len(data["idempotency"]), 60)
            self.assertEqual(len(data["events"]), 60)
        finally:
            os.chdir(cwd)

    def test_concurrent_replays(self) -> None:
        submit(self.path, _job(), "key")
        results: list[tuple[bool, dict[str, object]]] = []
        lock = threading.Lock()

        def worker() -> None:
            record, created = submit(
                self.path,
                _job(regions=["us-west", "eu-north"],
                     residency=["eu-north"]),
                "key")
            with lock:
                results.append((created, record))

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        self.assertEqual(len(results), 10)
        self.assertTrue(all(not created for created, _ in results))
        data = json.loads(Path(self.path).read_text("utf-8"))
        self.assertEqual(len(data["jobs"]), 1)
        self.assertEqual(len(data["events"]), 1)

    def test_multiprocess_distinct_submits(self) -> None:
        context = multiprocessing.get_context("fork")
        with context.Pool(8) as pool:
            created_flags = pool.map(
                _mp_submit_distinct,
                [(self.path, i) for i in range(40)])
        self.assertTrue(all(created_flags))
        data = json.loads(Path(self.path).read_text("utf-8"))
        self.assertEqual(len(data["jobs"]), 40)
        self.assertEqual(len(data["idempotency"]), 40)
        self.assertEqual(len(data["events"]), 40)
        self.assertEqual(set(data["jobs"]),
                             {f"j{i:03d}" for i in range(40)})

    def test_multiprocess_same_key_single_create(self) -> None:
        context = multiprocessing.get_context("fork")
        with context.Pool(8) as pool:
            created_flags = pool.map(
                _mp_submit_same, [self.path] * 16)
        self.assertEqual(sorted(created_flags), [False] * 15 + [True])
        data = json.loads(Path(self.path).read_text("utf-8"))
        self.assertEqual(len(data["jobs"]), 1)
        self.assertEqual(len(data["idempotency"]), 1)
        self.assertEqual(len(data["events"]), 1)

    def test_corrupt_file_raises_value_error(self) -> None:
        Path(self.path).write_text("{not json", encoding="utf-8")
        with self.assertRaises(ValueError):
            submit(self.path, _job(), "key")

    def test_bad_encoding_raises_value_error(self) -> None:
        Path(self.path).write_bytes(b"\xff\xfe" + json.dumps(
            _valid_document()).encode("utf-8"))
        with self.assertRaises(ValueError):
            submit(self.path, _job(), "key")

    def test_negative_zero_and_non_finite_raise(self) -> None:
        for literal in ("-0", "-0.0", "NaN", "Infinity", "-Infinity"):
            with self.subTest(literal=literal):
                text = (
                    '{"version":2,"jobs":{"j-1":{"job_id":"j-1","work":'
                    + literal
                    + ',"deadline":1,"regions":["a"],"residency":["a"],'
                      '"max_cost":1,"carbon_cap":1,"state":"queued"}},'
                      '"idempotency":{"k":"j-1"},'
                      '"events":{"k":{"type":"submit","idempotency_key":'
                      '"k","job_id":"j-1","result":"queued"}}}\n')
                Path(self.path).write_text(text, encoding="utf-8")
                with self.assertRaises(ValueError):
                    submit(self.path, _job(job_id="j-2"), "k2")

    def test_invalid_structures_raise_value_error(self) -> None:
        good_job = {
            "job_id": "j-1",
            "work": 10,
            "deadline": 100,
            "regions": ["eu-north", "us-west"],
            "residency": ["eu-north"],
            "max_cost": 500,
            "carbon_cap": 42,
            "state": "queued",
        }
        good_event = {"type": "submit", "idempotency_key": "key-1",
                      "job_id": "j-1", "result": "queued"}
        bad_payloads: list[object] = [
            [],
            {},
            {"version": 2, "jobs": {}, "idempotency": {}},
            {"version": 2, "jobs": {}, "idempotency": {},
             "events": {}, "extra": 1},
            {"events": {}, "version": 2, "jobs": {}, "idempotency": {}},
            {"version": 1, "jobs": {}, "idempotency": {}, "events": {}},
            {"version": 3, "jobs": {}, "idempotency": {}, "events": {}},
            {"version": "2", "jobs": {}, "idempotency": {}, "events": {}},
            {"version": 2, "jobs": [], "idempotency": {}, "events": {}},
            {"version": 2, "jobs": {}, "idempotency": [], "events": {}},
            {"version": 2, "jobs": {}, "idempotency": {}, "events": []},
            # Jobs.
            {"version": 2,
             "jobs": {"j-2": dict(good_job, job_id="j-2"),
                      "j-1": dict(good_job)},
             "idempotency": {}, "events": {}},
            {"version": 2,
             "jobs": {"j-1": dict(good_job, work=0)},
             "idempotency": {}, "events": {}},
            {"version": 2,
             "jobs": {"j-1": dict(good_job, deadline=-1)},
             "idempotency": {}, "events": {}},
            {"version": 2,
             "jobs": {"j-1": dict(good_job, max_cost=-1)},
             "idempotency": {}, "events": {}},
            {"version": 2,
             "jobs": {"j-1": dict(good_job, carbon_cap=-1)},
             "idempotency": {}, "events": {}},
            {"version": 2,
             "jobs": {"j-1": dict(good_job, regions=["b", "a"],
                                  residency=["a"])},
             "idempotency": {}, "events": {}},
            {"version": 2,
             "jobs": {"j-1": dict(good_job, regions=["a", "a"],
                                  residency=["a"])},
             "idempotency": {}, "events": {}},
            {"version": 2,
             "jobs": {"j-1": dict(good_job, regions=[], residency=[])},
             "idempotency": {}, "events": {}},
            {"version": 2,
             "jobs": {"j-1": dict(good_job, residency=["us-west", "zzz"])},
             "idempotency": {}, "events": {}},
            {"version": 2,
             "jobs": {"j-1": dict(good_job, state="running")},
             "idempotency": {}, "events": {}},
            {"version": 2,
             "jobs": {"j-1": dict(good_job, job_id="other")},
             "idempotency": {}, "events": {}},
            {"version": 2,
             "jobs": {"j-1": dict(good_job, extra=1)},
             "idempotency": {}, "events": {}},
            {"version": 2,
             "jobs": {"j-1": {k: good_job[k] for k in list(good_job)[:-1]}},
             "idempotency": {}, "events": {}},
            {"version": 2,
             "jobs": {"j-1": {
                 "state": "queued", "carbon_cap": 42, "max_cost": 500,
                 "residency": ["eu-north"],
                 "regions": ["eu-north", "us-west"], "deadline": 100,
                 "work": 10, "job_id": "j-1"}},
             "idempotency": {}, "events": {}},
            # Idempotency.
            {"version": 2,
             "jobs": {"j-1": good_job},
             "idempotency": {"b": "j-1", "a": "j-1"},
             "events": {}},
            {"version": 2,
             "jobs": {"j-1": good_job},
             "idempotency": {"key-1": "missing"}, "events": {}},
            {"version": 2,
             "jobs": {"j-1": good_job},
             "idempotency": {"": "j-1"}, "events": {}},
            # Events.
            {"version": 2,
             "jobs": {"j-1": good_job},
             "idempotency": {"key-1": "j-1"},
             "events": {"key-1": dict(good_event, type="other")}},
            {"version": 2,
             "jobs": {"j-1": good_job},
             "idempotency": {"key-1": "j-1"},
             "events": {"key-1": dict(good_event, result="running")}},
            {"version": 2,
             "jobs": {"j-1": good_job},
             "idempotency": {"key-1": "j-1"},
             "events": {"key-1": dict(good_event, job_id="other")}},
            {"version": 2,
             "jobs": {"j-1": good_job},
             "idempotency": {"key-1": "j-1"},
             "events": {"key-1": dict(good_event,
                                      idempotency_key="other")}},
            {"version": 2,
             "jobs": {"j-1": good_job},
             "idempotency": {"key-1": "j-1"},
             "events": {"other": good_event}},
            {"version": 2,
             "jobs": {"j-1": good_job},
             "idempotency": {"key-1": "j-1"},
             "events": {"key-1": dict(good_event, extra=1)}},
            {"version": 2,
             "jobs": {"j-1": good_job},
             "idempotency": {"key-1": "j-1"},
             "events": {"key-1": {
                 "result": "queued", "job_id": "j-1",
                 "idempotency_key": "key-1", "type": "submit"}}},
            {"version": 2,
             "jobs": {"j-1": good_job},
             "idempotency": {"key-1": "j-1"},
             "events": {"b": dict(good_event, idempotency_key="b"),
                        "a": dict(good_event, idempotency_key="a")}},
            # Missing and dangling events break the one-to-one binding.
            {"version": 2,
             "jobs": {"j-1": good_job},
             "idempotency": {"key-1": "j-1"},
             "events": {}},
            {"version": 2,
             "jobs": {"j-1": good_job},
             "idempotency": {},
             "events": {"key-1": good_event}},
        ]
        for payload in bad_payloads:
            with self.subTest(payload=payload):
                self._write(payload)
                with self.assertRaises(ValueError):
                    submit(self.path, _job(job_id="j-2"), "key-2")

    def test_valid_existing_v2_loads_and_replays(self) -> None:
        self._write(_valid_document())
        record, created = submit(self.path, _job(job_id="j-2"), "key-2")
        self.assertTrue(created)
        self.assertEqual(record["job_id"], "j-2")
        replay, replayed = submit(self.path, _job(), "key-1")
        self.assertFalse(replayed)
        self.assertEqual(replay["state"], "queued")
        self.assertEqual(replay["work"], 10)

    def test_v1_file_is_rejected_not_upgraded(self) -> None:
        v1_job = {"job_id": "a", "deadline": 1, "energy_wh": 2,
                  "residency_regions": ["r"]}
        register(self.path, v1_job, "k")
        raw = Path(self.path).read_bytes()
        with self.assertRaises(ValueError):
            submit(self.path, _job(), "kk")
        with self.assertRaises(ValueError):
            get(self.path, "a")
        self.assertEqual(Path(self.path).read_bytes(), raw)
        data = json.loads(raw)
        self.assertEqual(data["version"], 1)

    def test_returned_record_is_a_copy(self) -> None:
        record, _ = submit(self.path, _job(), "k")
        record["deadline"] = 999
        record["regions"].append("zzz")
        replay = get(self.path, "j-1")
        self.assertEqual(replay["deadline"], 100)
        self.assertEqual(replay["regions"], ["eu-north", "us-west"])

    def test_no_tmp_files_left_behind(self) -> None:
        submit(self.path, _job(), "k")
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    def test_commit_failure_on_first_create_removes_file(self) -> None:
        original = jobs_module._fsync_directory

        def fail(_directory: str) -> None:
            raise OSError("injected directory fsync failure")

        jobs_module._fsync_directory = fail
        try:
            with self.assertRaises(OSError):
                submit(self.path, _job(), "k")
        finally:
            jobs_module._fsync_directory = original
        self.assertFalse(os.path.exists(self.path))
        # The lock was released: a later submit succeeds normally.
        record, created = submit(self.path, _job(), "k")
        self.assertTrue(created)
        self.assertEqual(record["job_id"], "j-1")

    def test_commit_failure_restores_pre_call_bytes(self) -> None:
        submit(self.path, _job(), "k1")
        raw = Path(self.path).read_bytes()
        original = jobs_module._fsync_directory
        calls = {"n": 0}

        def fail_once(directory: str) -> None:
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("injected directory fsync failure")
            return original(directory)

        jobs_module._fsync_directory = fail_once
        try:
            with self.assertRaises(OSError):
                submit(self.path, _job(job_id="j-2"), "k2")
        finally:
            jobs_module._fsync_directory = original
        self.assertEqual(Path(self.path).read_bytes(), raw)
        data = json.loads(raw)
        self.assertEqual(set(data["jobs"]), {"j-1"})
        self.assertEqual(set(data["events"]), {"k1"})
        # Rollback synced the directory; the next submit commits cleanly.
        _, created = submit(self.path, _job(job_id="j-2"), "k2")
        self.assertTrue(created)

    def test_equivalent_realpath_same_store(self) -> None:
        submit(self.path, _job(), "k")
        alt = os.path.join(self.tmp.name, "sub", "..", "acceptance.json")
        self.assertEqual(jobs_module._get_store(os.path.realpath(alt)),
                         jobs_module._get_store(os.path.realpath(self.path)))


class GetTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "acceptance.json")

    def test_get_returns_stored_record(self) -> None:
        submit(self.path, _job(regions=["z", "a"], residency=["a"]), "k")
        record = get(self.path, "j-1")
        self.assertEqual(record, {
            "job_id": "j-1",
            "work": 10,
            "deadline": 100,
            "regions": ["a", "z"],
            "residency": ["a"],
            "max_cost": 500,
            "carbon_cap": 42,
            "state": "queued",
        })

    def test_get_returns_a_copy(self) -> None:
        submit(self.path, _job(), "k")
        record = get(self.path, "j-1")
        record["work"] = 1000
        record["regions"].append("zzz")
        self.assertEqual(get(self.path, "j-1")["work"], 10)
        self.assertEqual(get(self.path, "j-1")["regions"],
                         ["eu-north", "us-west"])

    def test_get_unknown_job_raises_key_error(self) -> None:
        submit(self.path, _job(), "k")
        with self.assertRaises(KeyError):
            get(self.path, "missing")

    def test_get_missing_file_raises_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            get(self.path, "j-1")

    def test_get_invalid_arguments(self) -> None:
        submit(self.path, _job(), "k")
        with self.assertRaises(ValueError):
            get(self.path, "")
        with self.assertRaises(ValueError):
            get(self.path, 1)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            get(self.path, None)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            get(123, "j-1")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            get("", "j-1")

    def test_get_invalid_file_raises_value_error(self) -> None:
        Path(self.path).write_text("{not json", encoding="utf-8")
        with self.assertRaises(ValueError):
            get(self.path, "j-1")

    def test_get_v1_file_raises_value_error(self) -> None:
        register(self.path,
                 {"job_id": "a", "deadline": 1, "energy_wh": 2,
                  "residency_regions": ["r"]}, "k")
        with self.assertRaises(ValueError):
            get(self.path, "a")


if __name__ == "__main__":
    unittest.main()
