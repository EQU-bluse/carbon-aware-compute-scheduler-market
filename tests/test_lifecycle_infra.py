"""Regression and fault-injection tests for the shared lifecycle
persistence infrastructure.

The dispatch, execution and completion ledgers persist one kind of
document -- canonical compact JSON sections, an idempotency map and an
audit trail -- and these tests pin the semantics the three modules now
share through ``carbon_market._lifecycle``:

* one in-process mutex per resolved real path (symlinks and the three
  modules themselves collapse onto one store), a companion flock for
  shared and exclusive cross-process locks, and resolved-real-path lock
  ordering for every joint operation;
* strict reads -- non-UTF-8 bytes, invalid JSON, non-finite and
  negative-zero numbers, wrong field or primary-key order and a missing
  or duplicated trailing newline are all rejected with the ledger's own
  wording, while a missing file keeps each public entry's original
  mapping (``FileNotFoundError``, ``KeyError`` or empty initial state);
* one durable commit per call -- synced same-directory temporary,
  atomic replace, directory fsync, exact-byte rollback (or removal) on
  failure, with recovery errors chained after the original cause, and
  no leftover temporary file;
* whole-version visibility, first-call atomic publication and no-write
  replay under thread contention and between independent processes;
* byte-identical output versus the pre-refactor ledgers.

Every check goes through the public entry points and the on-disk
bytes; only the read-validation and lock-order probes touch module
privates, and those privates are deliberately kept as module seams.
"""

from __future__ import annotations

import contextlib
import json
import multiprocessing
import os
import threading
import traceback
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from carbon_market import _lifecycle
from carbon_market import completion as completion_module
from carbon_market import dispatch as dispatch_module
from carbon_market import execution as execution_module
from carbon_market import execution_sync as execution_sync_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.completion import (
    complete, get as completion_get, get_response, search,
    search_response,
)
from carbon_market.dispatch import claim, commit, finish, recover
from carbon_market.execution import plan, record, recover as plan_recover
from carbon_market.jobs import submit
from carbon_market.market import clear_live
from carbon_market.resources import publish as publish_resource
from carbon_market.signals import publish as publish_signal

_WAIT = 30.0


# ---------------------------------------------------------------------------
# Exact bytes produced by the pre-refactor code: the refactor must not move
# a single byte.
# ---------------------------------------------------------------------------

_DISPATCH_FIRST_BYTES = (
    b'{"version":1,"decisions":{"j-1":{"job_id":"j-1","at":10,'
    b'"resource_id":"r-1","version":1,"deadline":100,"state":"ready",'
    b'"attempts":0,"owner":null,"lease_end":null}},'
    b'"idempotency":{"dk-1":{"action":"commit","job_id":"j-1","at":10}},'
    b'"audit":{"dk-1":{"key":"dk-1","request":{"action":"commit",'
    b'"job_id":"j-1","at":10},"result":{"job_id":"j-1","at":10,'
    b'"resource_id":"r-1","version":1,"deadline":100,"state":"ready",'
    b'"attempts":0,"owner":null,"lease_end":null}}}}\n'
)

_DISPATCH_UNICODE_FIRST_BYTES = (
    b'{"version":1,"decisions":{"j-\xc3\xa9":{"job_id":"j-\xc3\xa9",'
    b'"at":50,"resource_id":"r-1","version":1,"deadline":100,'
    b'"state":"ready","attempts":0,"owner":null,"lease_end":null}},'
    b'"idempotency":{"dk-\xc3\xa9":{"action":"commit","job_id":"j-\xc3\xa9",'
    b'"at":50}},"audit":{"dk-\xc3\xa9":{"key":"dk-\xc3\xa9",'
    b'"request":{"action":"commit","job_id":"j-\xc3\xa9","at":50},'
    b'"result":{"job_id":"j-\xc3\xa9","at":50,"resource_id":"r-1",'
    b'"version":1,"deadline":100,"state":"ready","attempts":0,'
    b'"owner":null,"lease_end":null}}}}\n'
)

_EXECUTION_FIRST_BYTES = (
    b'{"version":1,"plans":{"j-1":{"1":{"job_id":"j-1","attempt":1,'
    b'"kind":"launch","source":null,"resource_id":"r-1","version":1,'
    b'"deadline":100,"owner":"owner-1","lease_end":60,"state":"active",'
    b'"steps":[]}}},"idempotency":{"ek-1":{"action":"plan","job_id":"j-1",'
    b'"owner":"owner-1","source":null,"at":21}},"audit":{"ek-1":{'
    b'"key":"ek-1","request":{"action":"plan","job_id":"j-1",'
    b'"owner":"owner-1","source":null,"at":21},"result":{"job_id":"j-1",'
    b'"attempt":1,"kind":"launch","source":null,"resource_id":"r-1",'
    b'"version":1,"deadline":100,"owner":"owner-1","lease_end":60,'
    b'"state":"active","steps":[]}}}}\n'
)

_COMPLETION_FIRST_BYTES = (
    b'{"version":1,"completions":{"xk-1":{"job_id":"j-1","at":90,'
    b'"outcome":"succeeded","actual_cost":10,"actual_carbon":20,'
    b'"generation":0,"current":{"resource_id":"r-1","version":1},'
    b'"cost_exceeded":false,"carbon_exceeded":false}},'
    b'"idempotency":{"xk-1":{"job_id":"j-1","at":90,"outcome":"succeeded",'
    b'"actual_cost":10,"actual_carbon":20}},"audit":{"xk-1":{"key":"xk-1",'
    b'"request":{"job_id":"j-1","at":90,"outcome":"succeeded",'
    b'"actual_cost":10,"actual_carbon":20},"result":{"job_id":"j-1",'
    b'"at":90,"outcome":"succeeded","actual_cost":10,"actual_carbon":20,'
    b'"generation":0,"current":{"resource_id":"r-1","version":1},'
    b'"cost_exceeded":false,"carbon_exceeded":false}}}}\n'
)


# ---------------------------------------------------------------------------
# Business stack fixtures shared by the three lifecycle kinds
# ---------------------------------------------------------------------------

def _job(job_id: str) -> dict[str, object]:
    return {
        "job_id": job_id, "work": 10, "deadline": 100,
        "regions": ["eu-north"], "residency": ["eu-north"],
        "max_cost": 1000, "carbon_cap": 1000,
    }


_RESOURCE = {
    "resource_id": "r-1", "region": "eu-north", "capacity": 100,
    "start": 0, "end": 500, "unit_cost": 3, "carbon_intensity": 7,
    "residency": ["eu-north"],
}

_SIGNAL = {
    "region": "eu-north", "observed": 0, "expires": 500,
    "mix": {"solar": 10000}, "unit_cost": 3, "carbon_intensity": 7,
}


class _Stack:
    """One directory holding the full business stack and the three
    lifecycle ledgers, with the exact seeding calls the production
    layers make public."""

    def __init__(self, directory: str) -> None:
        base = directory
        self.dir = base
        self.jobs = os.path.join(base, "jobs.json")
        self.supply = os.path.join(base, "supply.json")
        self.signals = os.path.join(base, "signals.json")
        self.trades = os.path.join(base, "trades.json")
        self.dispatch = os.path.join(base, "dispatch.json")
        self.execution = os.path.join(base, "execution.json")
        self.sync = os.path.join(base, "sync.json")
        self.completions = os.path.join(base, "completions.json")

    def publish_base(self) -> None:
        publish_resource(self.supply, _RESOURCE, "rk-1")
        publish_signal(self.signals, _SIGNAL, "sk-1")

    def submit(self, index: int) -> str:
        job_id = f"j-{index}"
        submit(self.jobs, _job(job_id), f"jk-{index}")
        return job_id

    def trade(self, index: int, job_id: str | None = None, at: int = 10) -> str:
        job_id = job_id or f"j-{index}"
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   job_id, f"tk-{index}", at)
        return job_id

    def commit_dispatch(self, index: int, job_id: str | None = None,
                        at: int = 10) -> str:
        job_id = job_id or f"j-{index}"
        commit(self.jobs, self.supply, self.trades, self.dispatch,
               job_id, f"dk-{index}", at)
        return job_id

    def claim_ready(self, index: int, job_id: str | None = None,
                    owner: str | None = None, at: int = 20) -> str:
        job_id = job_id or f"j-{index}"
        claim(self.dispatch, job_id, f"ck-{index}",
              owner or f"owner-{index}", 40, at)
        return job_id

    def plan_launch(self, index: int, job_id: str | None = None,
                    owner: str | None = None, at: int = 21) -> str:
        job_id = job_id or f"j-{index}"
        plan(self.jobs, self.supply, self.trades, self.dispatch,
             self.execution, job_id, f"ek-{index}",
             owner or f"owner-{index}", None, at)
        return job_id

    def record_stage(self, index: int, job_id: str | None = None,
                     owner: str | None = None, step: str = "stage",
                     result: str = "succeeded", receipt: str | None = None,
                     at: int = 22, attempt: int = 1,
                     key: str | None = None) -> str:
        job_id = job_id or f"j-{index}"
        record(self.execution, job_id, attempt,
               key or f"rk-{index}-{step}", owner or f"owner-{index}",
               step, result, receipt or f"receipt-{index}-{step}", at)
        return job_id

    def finish_to_terminal(self, index: int) -> str:
        """Trade, dispatch, launch-execute and synchronize one job to a
        succeeded dispatch decision, the completion predicate's state."""
        job_id = f"j-{index}"
        self.submit(index)
        self.trade(index, job_id)
        self.commit_dispatch(index, job_id)
        self.claim_ready(index, job_id)
        self.plan_launch(index, job_id)
        self.record_stage(index, job_id, step="stage", at=22,
                          receipt=f"tok-{index}-stage")
        self.record_stage(index, job_id, step="start", at=23,
                          receipt=f"tok-{index}-start")
        execution_sync_module.run(self.execution, self.dispatch, self.sync,
                                  f"sync-owner-{index}", f"bk-{index}",
                                  80, 10)
        return job_id

    def complete_job(self, index: int, job_id: str | None = None,
                     key: str | None = None, at: int = 90) -> tuple[object, bool]:
        job_id = job_id or f"j-{index}"
        return complete(self.jobs, self.supply, self.signals, self.trades,
                        self.dispatch, self.execution, self.completions,
                        job_id, key or f"xk-{index}", at,
                        "succeeded", 10, 20)


class LifecycleTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.stack = _Stack(self.tmp.name)
        self.stack.publish_base()

    def _seed_ready_jobs(self, count: int) -> list[str]:
        ids = []
        for index in range(1, count + 1):
            job_id = self.stack.submit(index)
            self.stack.trade(index, job_id)
            self.stack.commit_dispatch(index, job_id)
            ids.append(job_id)
        return ids

    def _seed_active_plans(self, count: int) -> list[str]:
        ids = []
        for index in range(1, count + 1):
            job_id = self.stack.submit(index)
            self.stack.trade(index, job_id)
            self.stack.commit_dispatch(index, job_id)
            self.stack.claim_ready(index, job_id)
            self.stack.plan_launch(index, job_id)
            ids.append(job_id)
        return ids

    def _seed_completable_jobs(self, count: int) -> list[str]:
        return [self.stack.finish_to_terminal(index)
                for index in range(1, count + 1)]

    def _entries(self, prefix: str | None = None) -> list[str]:
        names = os.listdir(self.tmp.name)
        if prefix is None:
            return names
        return [name for name in names if name.startswith(prefix)]

    def _new_stack(self, name: str) -> _Stack:
        directory = os.path.join(self.tmp.name, name)
        os.mkdir(directory)
        stack = _Stack(directory)
        stack.publish_base()
        return stack


# ---------------------------------------------------------------------------
# The shared infrastructure itself
# ---------------------------------------------------------------------------

class LifecycleInfrastructureTest(LifecycleTestBase):
    def test_equivalent_real_paths_share_one_store(self) -> None:
        target = os.path.join(self.tmp.name, "alias-target.json")
        Path(target).write_bytes(b"{}\n")
        alias = os.path.join(self.tmp.name, "alias-link.json")
        os.symlink(target, alias)
        # Across callers and across the three lifecycle modules: one
        # resolved real path is one in-process mutex.
        self.assertIs(dispatch_module._get_store(target),
                      dispatch_module._get_store(alias))
        self.assertIs(dispatch_module._get_store(target),
                      execution_module._get_store(alias))
        self.assertIs(completion_module._get_store(target),
                      dispatch_module._get_store(alias))
        self.assertEqual(dispatch_module._get_store(alias).realpath,
                         os.path.realpath(target))

    def test_plain_int_rejects_booleans(self) -> None:
        self.assertTrue(_lifecycle.is_plain_int(0))
        self.assertTrue(_lifecycle.is_plain_int(-1))
        self.assertFalse(_lifecycle.is_plain_int(True))
        self.assertFalse(_lifecycle.is_plain_int(False))
        self.assertFalse(_lifecycle.is_plain_int(1.0))
        self.assertFalse(_lifecycle.is_plain_int("1"))

    def test_sorted_key_check_uses_code_points(self) -> None:
        _lifecycle.check_sorted_keys({"a": 1, "b": 2, "é": 3}, "section")
        with self.assertRaises(ValueError):
            _lifecycle.check_sorted_keys({"b": 1, "a": 2}, "section")

    def test_exclusive_lock_serializes_threads(self) -> None:
        path = os.path.join(self.tmp.name, "guarded.json")
        Path(path).write_bytes(b"{}\n")
        real = os.path.realpath(path)
        held = threading.Event()
        release = threading.Event()
        proceeded = threading.Event()

        def hold() -> None:
            with _lifecycle.file_lock(real):
                held.set()
                release.wait(_WAIT)

        waiter = threading.Thread(target=hold)
        waiter.start()
        self.assertTrue(held.wait(_WAIT))

        def try_enter() -> None:
            with _lifecycle.file_lock(real):
                proceeded.set()

        contender = threading.Thread(target=try_enter)
        contender.start()
        self.assertFalse(proceeded.wait(0.3))
        release.set()
        self.assertTrue(proceeded.wait(_WAIT))
        waiter.join(_WAIT)
        contender.join(_WAIT)
        self.assertFalse(waiter.is_alive() or contender.is_alive())

    def test_shared_lock_then_exclusive_blocks_across_modules(self) -> None:
        path = os.path.join(self.tmp.name, "guarded.json")
        Path(path).write_bytes(b"{}\n")
        real = os.path.realpath(path)
        shared_held = threading.Event()
        release = threading.Event()
        exclusive_entered = threading.Event()

        def share() -> None:
            with dispatch_module._lock(real, shared=True):
                shared_held.set()
                release.wait(_WAIT)

        holder = threading.Thread(target=share)
        holder.start()
        self.assertTrue(shared_held.wait(_WAIT))

        def exclusive() -> None:
            with completion_module._lock(real):
                exclusive_entered.set()

        contender = threading.Thread(target=exclusive)
        contender.start()
        self.assertFalse(exclusive_entered.wait(0.3))
        release.set()
        self.assertTrue(exclusive_entered.wait(_WAIT))
        holder.join(_WAIT)
        contender.join(_WAIT)

    def test_load_canonical_missing_file_is_empty_state(self) -> None:
        missing = os.path.join(self.tmp.name, "absent.json")

        def validate(data: object) -> object:
            return data

        def canonical(sections: object) -> bytes:
            return b"{}\n"

        sections, raw = _lifecycle.load_canonical(
            missing, "test ledger", validate, canonical)
        self.assertIsNone(sections)
        self.assertIsNone(raw)

    def test_load_canonical_rejects_bad_bytes_and_off_canonical(self) -> None:
        target = os.path.join(self.tmp.name, "ledger.json")

        def validate(data: object) -> dict[str, object]:
            if not isinstance(data, dict) or set(data) != {"version"}:
                raise ValueError("bad shape")
            return data

        def canonical(sections: dict[str, object]) -> bytes:
            return json.dumps(sections, separators=(",", ":")).encode() \
                + b"\n"

        Path(target).write_bytes(b"\xff\n")
        with self.assertRaises(ValueError) as caught:
            _lifecycle.load_canonical(target, "test ledger", validate,
                                      canonical)
        self.assertIn("test ledger", str(caught.exception))
        self.assertIn("not valid UTF-8", str(caught.exception))

        Path(target).write_bytes(b"{broken\n")
        with self.assertRaisesRegex(ValueError, "not valid JSON"):
            _lifecycle.load_canonical(target, "test ledger", validate,
                                      canonical)

        Path(target).write_bytes(b'{"version": 1}\n')
        with self.assertRaisesRegex(ValueError, "canonical compact form"):
            _lifecycle.load_canonical(target, "test ledger", validate,
                                      canonical)

        Path(target).write_bytes(canonical({"version": 1}))
        sections, raw = _lifecycle.load_canonical(
            target, "test ledger", validate, canonical)
        self.assertEqual(sections, {"version": 1})
        self.assertEqual(raw, canonical({"version": 1}))

    def test_commit_file_creates_overwrites_and_removes_tmp(self) -> None:
        target = os.path.join(self.tmp.name, "direct.json")
        _lifecycle.commit_file(target, b"one\n", None, prefix=".direct-")
        self.assertEqual(Path(target).read_bytes(), b"one\n")
        _lifecycle.commit_file(target, b"two\n", b"one\n",
                               prefix=".direct-")
        self.assertEqual(Path(target).read_bytes(), b"two\n")
        self.assertEqual(self._entries(".direct-"), [])

    def test_commit_file_write_failure_leaves_previous_bytes(self) -> None:
        target = os.path.join(self.tmp.name, "direct.json")
        Path(target).write_bytes(b"old\n")
        real_fdopen = os.fdopen
        injected = {"done": False}

        class FailingWriter:
            def __init__(self, handle) -> None:
                self._handle = handle

            def __enter__(self):
                return self

            def __exit__(self, *exc) -> bool:
                self._handle.close()
                return False

            def write(self, data) -> None:
                raise OSError("injected write failure")

        def flaky_fdopen(fd, mode="r", *args, **kwargs):
            handle = real_fdopen(fd, mode, *args, **kwargs)
            if not injected["done"] and "w" in mode:
                injected["done"] = True
                return FailingWriter(handle)
            return handle

        with mock.patch("os.fdopen", flaky_fdopen):
            with self.assertRaises(OSError):
                _lifecycle.commit_file(target, b"new\n", b"old\n",
                                       prefix=".direct-")
        self.assertTrue(injected["done"])
        self.assertEqual(Path(target).read_bytes(), b"old\n")
        self.assertEqual(self._entries(".direct-"), [])

    def test_commit_file_rolls_back_when_directory_sync_fails(self) -> None:
        target = os.path.join(self.tmp.name, "direct.json")
        Path(target).write_bytes(b"old\n")
        calls = {"fsync": 0}

        def failing_dirsync(directory: str) -> None:
            calls["fsync"] += 1
            raise OSError("injected directory fsync failure")

        with self.assertRaises(OSError):
            _lifecycle.commit_file(target, b"new\n", b"old\n",
                                   prefix=".direct-",
                                   fsync_dir=failing_dirsync)
        # The replace landed, the rollback restored the old bytes and
        # synced the directory through the same failing seam twice.
        self.assertGreaterEqual(calls["fsync"], 2)
        self.assertEqual(Path(target).read_bytes(), b"old\n")
        self.assertEqual(self._entries(".direct-"), [])


# ---------------------------------------------------------------------------
# Strict reading shared by the three ledgers
# ---------------------------------------------------------------------------

_EMPTY_DOCS = {
    "dispatch": (
        dispatch_module,
        b'{"version":1,"decisions":{},"idempotency":{},"audit":{}}\n',
        ".dispatch-",
    ),
    "execution": (
        execution_module,
        b'{"version":1,"plans":{},"idempotency":{},"audit":{}}\n',
        ".execution-",
    ),
    "completion": (
        completion_module,
        b'{"version":1,"completions":{},"idempotency":{},"audit":{}}\n',
        ".completion-",
    ),
}


class StrictReadTest(LifecycleTestBase):
    def _raw_loader(self, kind: str):
        if kind == "dispatch":
            return dispatch_module._load_ledger
        if kind == "execution":
            return execution_module._load_ledger
        return completion_module._load_completion_ledger

    def test_missing_file_maps_to_empty_raw_state(self) -> None:
        for kind in ("dispatch", "execution", "completion"):
            with self.subTest(kind=kind):
                missing = os.path.join(self.tmp.name, f"{kind}-absent.json")
                sections, *_, raw = self._raw_loader(kind)(missing)
                self.assertIsNone(raw)
                self.assertEqual(sections, {})
                # A raw read never creates the business ledger.
                self.assertFalse(Path(missing).exists())

    def test_strict_read_rejects_every_noncanonical_shape(self) -> None:
        reordered_objects = {
            "dispatch": {"version": 1, "idempotency": {}, "decisions": {},
                         "audit": {}},
            "execution": {"version": 1, "idempotency": {}, "plans": {},
                          "audit": {}},
            "completion": {"version": 1, "idempotency": {},
                           "completions": {}, "audit": {}},
        }
        for kind, (module, empty, _prefix) in _EMPTY_DOCS.items():
            path = os.path.join(self.tmp.name, f"{kind}-strict.json")
            loader = self._raw_loader(kind)
            reordered = json.dumps(reordered_objects[kind],
                                   separators=(",", ":")).encode("utf-8") \
                + b"\n"
            cases = {
                "non-utf8": b"\xff\n",
                "invalid-json": b"{not json\n",
                "non-finite": empty.replace(b"1", b"NaN", 1),
                "negative-zero": empty.replace(b":1", b":-0", 1),
                "reordered-root": reordered,
                "missing-newline": empty[:-1],
                "double-newline": empty + b"\n",
            }
            for label, raw in cases.items():
                with self.subTest(kind=kind, case=label):
                    Path(path).write_bytes(raw)
                    with self.assertRaises(ValueError) as caught:
                        loader(path)
                    self.assertIn(f"{kind} ledger", str(caught.exception))

    def test_unsorted_primary_keys_are_rejected(self) -> None:
        # Real entries per ledger, serialized with one section in
        # reverse key order: validation refuses before the byte compare.
        self._seed_ready_jobs(2)
        dispatch_path = self.stack.dispatch
        data = json.loads(Path(dispatch_path).read_text(encoding="utf-8"))
        data["idempotency"] = dict(reversed(list(data["idempotency"].items())))
        Path(dispatch_path).write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"))
            + "\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "ordered by key code point"):
            dispatch_module._load_ledger(dispatch_path)

        # Execution primary keys live in a separate stack: two active
        # plans j-1/j-2, plans section reversed.
        exec_stack = self._new_stack("exec-order")
        for index in (1, 2):
            job_id = exec_stack.submit(index)
            exec_stack.trade(index, job_id)
            exec_stack.commit_dispatch(index, job_id)
            exec_stack.claim_ready(index, job_id)
            exec_stack.plan_launch(index, job_id)
        execution_path = exec_stack.execution
        data = json.loads(Path(execution_path).read_text(encoding="utf-8"))
        data["plans"] = dict(reversed(list(data["plans"].items())))
        Path(execution_path).write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"))
            + "\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "ordered by key code point"):
            execution_module._load_ledger(execution_path)

        # Completion: two completed jobs in their own stack, completions
        # section reversed.
        comp_stack = self._new_stack("comp-order")
        comp_stack.finish_to_terminal(1)
        comp_stack.finish_to_terminal(2)
        comp_stack.complete_job(1)
        comp_stack.complete_job(2)
        data = json.loads(Path(comp_stack.completions).read_text(
            encoding="utf-8"))
        data["completions"] = dict(
            reversed(list(data["completions"].items())))
        Path(comp_stack.completions).write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":"))
            + "\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "ordered by key code point"):
            completion_module._load_completion_ledger(comp_stack.completions)


# ---------------------------------------------------------------------------
# Missing-file mappings stay different at every public entry
# ---------------------------------------------------------------------------

class MissingFileMappingTest(LifecycleTestBase):
    def test_dispatch_missing_ledger_mappings(self) -> None:
        # commit creates the ledger; the other three entries must fail
        # with FileNotFoundError before any business decision changes.
        self._seed_ready_jobs(1)
        fresh = os.path.join(self.tmp.name, "dispatch-fresh.json")
        self.assertFalse(Path(fresh).exists())
        _decision, created = commit(
            self.stack.jobs, self.stack.supply, self.stack.trades,
            fresh, "j-1", "dk-create", 10)
        self.assertTrue(created)
        self.assertTrue(Path(fresh).exists())

        absent = os.path.join(self.tmp.name, "dispatch-absent.json")
        with self.assertRaises(FileNotFoundError):
            claim(absent, "j-1", "ck-x", "owner-x", 10, 20)
        with self.assertRaises(FileNotFoundError):
            finish(absent, "j-1", "fk-x", "owner-x", "succeeded", 20)
        with self.assertRaises(FileNotFoundError):
            recover(absent, "j-1", "rk-x", 90)
        self.assertFalse(Path(absent).exists())

    def test_execution_missing_ledger_mappings(self) -> None:
        ids = self._seed_ready_jobs(2)
        self.stack.claim_ready(1, ids[0])
        self.assertFalse(Path(self.stack.execution).exists())
        _plan, created = plan(
            self.stack.jobs, self.stack.supply, self.stack.trades,
            self.stack.dispatch, self.stack.execution, ids[0],
            "ek-create", "owner-1", None, 21)
        self.assertTrue(created)
        self.assertTrue(Path(self.stack.execution).exists())

        absent = os.path.join(self.tmp.name, "execution-absent.json")
        with self.assertRaises(FileNotFoundError):
            record(absent, ids[1], 1, "rk-x", "owner-2", "stage",
                   "succeeded", "tok", 22)
        with self.assertRaises(FileNotFoundError):
            plan_recover(absent, ids[1], 1, "rk-y", 90)
        self.assertFalse(Path(absent).exists())

    def test_completion_missing_ledger_mappings(self) -> None:
        self._seed_completable_jobs(1)
        # The exact lookup keeps its KeyError for a missing ledger...
        with self.assertRaises(KeyError):
            completion_get(self.stack.completions, "j-1")
        # ...while the paginated and conditional queries, like the
        # HTTP endpoints, keep FileNotFoundError.
        with self.assertRaises(FileNotFoundError):
            search(self.stack.completions)
        with self.assertRaises(FileNotFoundError):
            get_response(self.stack.completions, "j-1")
        with self.assertRaises(FileNotFoundError):
            search_response(self.stack.completions)
        self.assertFalse(Path(self.stack.completions).exists())

        _record, created = self.stack.complete_job(1)
        self.assertTrue(created)
        self.assertTrue(Path(self.stack.completions).exists())
        # An unknown job in an existing ledger stays KeyError.
        with self.assertRaises(KeyError):
            completion_get(self.stack.completions, "j-nope")

    def test_read_only_queries_create_no_business_ledger(self) -> None:
        for invocation in (
            lambda: completion_get(self.stack.completions, "j-1"),
            lambda: search(self.stack.completions),
            lambda: get_response(self.stack.completions, "j-1"),
            lambda: search_response(self.stack.completions),
        ):
            with self.assertRaises((KeyError, FileNotFoundError)):
                invocation()
            self.assertFalse(Path(self.stack.completions).exists())


# ---------------------------------------------------------------------------
# Commit fault injection through the public entry points
# ---------------------------------------------------------------------------

class _FaultBase(LifecycleTestBase):
    module = None
    prefix = None

    def _assert_no_fragments(self) -> None:
        self.assertEqual(self._entries(self.prefix), [])

    @contextlib.contextmanager
    def _failing_fdopen(self):
        real_fdopen = os.fdopen
        injected = {"done": False}

        class FailingWriter:
            def __init__(self, handle) -> None:
                self._handle = handle

            def __enter__(self):
                return self

            def __exit__(self, *exc) -> bool:
                self._handle.close()
                return False

            def write(self, data) -> None:
                raise OSError("injected temporary-file write failure")

        def flaky_fdopen(fd, mode="r", *args, **kwargs):
            handle = real_fdopen(fd, mode, *args, **kwargs)
            if not injected["done"] and "w" in mode:
                injected["done"] = True
                return FailingWriter(handle)
            return handle

        with mock.patch("os.fdopen", flaky_fdopen):
            yield injected

    @contextlib.contextmanager
    def _failing_file_fsync(self):
        real_fsync = os.fsync
        injected = {"done": False}

        def flaky_fsync(fd):
            if not injected["done"]:
                injected["done"] = True
                raise OSError("injected file fsync failure")
            return real_fsync(fd)

        with mock.patch("os.fsync", flaky_fsync):
            yield injected

    @contextlib.contextmanager
    def _failing_replace(self):
        real_replace = os.replace
        injected = {"done": False}

        def flaky_replace(src, dst):
            if not injected["done"]:
                injected["done"] = True
                raise OSError("injected atomic replace failure")
            return real_replace(src, dst)

        with mock.patch("os.replace", flaky_replace):
            yield injected

    @contextlib.contextmanager
    def _failing_directory_fsync_once(self):
        original = self.module._fsync_directory
        injected = {"done": False}

        def flaky(directory) -> None:
            if not injected["done"]:
                injected["done"] = True
                raise OSError("injected directory fsync failure")
            return original(directory)

        with mock.patch.object(self.module, "_fsync_directory", flaky):
            yield injected

    @contextlib.contextmanager
    def _directory_fsync_always_fails(self):
        def always(directory) -> None:
            raise OSError("injected directory fsync failure")

        with mock.patch.object(self.module, "_fsync_directory", always):
            yield


class DispatchCommitFaultTest(_FaultBase):
    module = dispatch_module
    prefix = ".dispatch-"

    def _overwrite_call(self) -> None:
        claim(self.stack.dispatch, "j-1", "ck-1", "owner-1", 40, 20)

    def _retry_overwrite(self) -> None:
        decision, created = claim(
            self.stack.dispatch, "j-1", "ck-1", "owner-1", 40, 20)
        self.assertTrue(created)
        self.assertEqual(decision["state"], "claimed")
        # The replay of the same request rewrites nothing.
        raw = Path(self.stack.dispatch).read_bytes()
        replay, created = claim(
            self.stack.dispatch, "j-1", "ck-1", "owner-1", 40, 20)
        self.assertFalse(created)
        self.assertEqual(Path(self.stack.dispatch).read_bytes(), raw)
        self.assertEqual(replay["state"], "claimed")

    def test_temporary_write_failure_on_create_leaves_absence(self) -> None:
        self._seed_ready_jobs(1)
        # Remove the commit-created ledger to force the create path.
        os.unlink(self.stack.dispatch)
        with self._failing_fdopen() as injected:
            with self.assertRaises(OSError):
                commit(self.stack.jobs, self.stack.supply, self.stack.trades,
                       self.stack.dispatch, "j-1", "dk-again", 10)
        self.assertTrue(injected["done"])
        self.assertFalse(Path(self.stack.dispatch).exists())
        self._assert_no_fragments()

    def test_temporary_write_failure_on_overwrite_restores_bytes(self) -> None:
        self._seed_ready_jobs(1)
        before = Path(self.stack.dispatch).read_bytes()
        with self._failing_fdopen() as injected:
            with self.assertRaises(OSError):
                self._overwrite_call()
        self.assertTrue(injected["done"])
        self.assertEqual(Path(self.stack.dispatch).read_bytes(), before)
        self._assert_no_fragments()
        self._retry_overwrite()

    def test_file_fsync_failure_restores_bytes(self) -> None:
        self._seed_ready_jobs(1)
        before = Path(self.stack.dispatch).read_bytes()
        with self._failing_file_fsync() as injected:
            with self.assertRaises(OSError):
                self._overwrite_call()
        self.assertTrue(injected["done"])
        self.assertEqual(Path(self.stack.dispatch).read_bytes(), before)
        self._assert_no_fragments()
        self._retry_overwrite()

    def test_replace_failure_restores_bytes(self) -> None:
        self._seed_ready_jobs(1)
        before = Path(self.stack.dispatch).read_bytes()
        with self._failing_replace() as injected:
            with self.assertRaises(OSError):
                self._overwrite_call()
        self.assertTrue(injected["done"])
        self.assertEqual(Path(self.stack.dispatch).read_bytes(), before)
        self._assert_no_fragments()
        self._retry_overwrite()

    def test_directory_fsync_failure_rolls_back_existing_bytes(self) -> None:
        self._seed_ready_jobs(1)
        before = Path(self.stack.dispatch).read_bytes()
        with self._failing_directory_fsync_once() as injected:
            with self.assertRaises(OSError):
                self._overwrite_call()
        self.assertTrue(injected["done"])
        self.assertEqual(Path(self.stack.dispatch).read_bytes(), before)
        self._assert_no_fragments()
        self._retry_overwrite()

    def test_directory_fsync_failure_on_create_rolls_back_to_absence(
        self) -> None:
        self._seed_ready_jobs(1)
        os.unlink(self.stack.dispatch)
        ledger = os.path.join(self.tmp.name, "dispatch-create.json")
        with self._failing_directory_fsync_once() as injected:
            with self.assertRaises(OSError):
                commit(self.stack.jobs, self.stack.supply, self.stack.trades,
                       ledger, "j-1", "dk-new", 10)
        self.assertTrue(injected["done"])
        self.assertFalse(Path(ledger).exists())
        self._assert_no_fragments()

    def test_rollback_directory_sync_failure_keeps_original_cause(self) -> None:
        self._seed_ready_jobs(1)
        before = Path(self.stack.dispatch).read_bytes()
        with self._directory_fsync_always_fails():
            with self.assertRaises(OSError) as caught:
                self._overwrite_call()
        # Both the commit directory sync and the rollback directory sync
        # fail; the reported error chains from the original failure and
        # the restore itself (replace + file fsync) landed, so the bytes
        # are the pre-call ones.
        self.assertIsNotNone(caught.exception.__cause__)
        self.assertIsInstance(caught.exception.__cause__, OSError)
        self.assertEqual(Path(self.stack.dispatch).read_bytes(), before)
        self._assert_no_fragments()

    def test_rollback_restore_replace_failure_keeps_original_cause(self) -> None:
        self._seed_ready_jobs(1)
        real_replace = os.replace
        state = {"calls": 0}

        def flaky_replace(src, dst):
            state["calls"] += 1
            # First call is the commit replace: succeed. Second call is
            # the rollback restore after the directory fsync fails: fail.
            if state["calls"] == 2:
                raise OSError("injected rollback replace failure")
            return real_replace(src, dst)

        def always_failing_dirsync(directory) -> None:
            raise OSError("injected directory fsync failure")

        with mock.patch("os.replace", flaky_replace), \
                mock.patch.object(self.module, "_fsync_directory",
                                  always_failing_dirsync):
            with self.assertRaises(OSError) as caught:
                self._overwrite_call()
        self.assertIn("rollback replace", str(caught.exception))
        self.assertIsNotNone(caught.exception.__cause__)
        self.assertIn("directory fsync", str(caught.exception.__cause__))
        self._assert_no_fragments()
        # The ledger is still a complete, readable version (the commit
        # replace landed); the next call replays or writes cleanly once
        # the injection is gone.
        sections = dispatch_module._load_ledger(self.stack.dispatch)
        self.assertEqual(sections[3] is not None, True)


class ExecutionCommitFaultTest(_FaultBase):
    module = execution_module
    prefix = ".execution-"

    def setUp(self) -> None:
        super().setUp()
        self._ids = self._seed_active_plans(1)

    def _overwrite_call(self) -> None:
        record(self.stack.execution, "j-1", 1, "rk-1-stage", "owner-1",
               "stage", "succeeded", "tok-stage", 22)

    def _retry_overwrite(self) -> None:
        plan_doc, created = record(
            self.stack.execution, "j-1", 1, "rk-1-stage", "owner-1",
            "stage", "succeeded", "tok-stage", 22)
        self.assertTrue(created)
        self.assertEqual([step["step"] for step in plan_doc["steps"]],
                         ["stage"])
        raw = Path(self.stack.execution).read_bytes()
        replay, created = record(
            self.stack.execution, "j-1", 1, "rk-1-stage", "owner-1",
            "stage", "succeeded", "tok-stage", 22)
        self.assertFalse(created)
        self.assertEqual(Path(self.stack.execution).read_bytes(), raw)
        self.assertEqual(replay, plan_doc)

    def test_temporary_write_failure_on_create_leaves_absence(self) -> None:
        ids = self._seed_ready_jobs(2)
        self.stack.claim_ready(2, ids[1])
        ledger = os.path.join(self.tmp.name, "execution-create.json")
        with self._failing_fdopen() as injected:
            with self.assertRaises(OSError):
                plan(self.stack.jobs, self.stack.supply, self.stack.trades,
                     self.stack.dispatch, ledger, ids[1], "ek-new",
                     "owner-2", None, 21)
        self.assertTrue(injected["done"])
        self.assertFalse(Path(ledger).exists())
        self.assertEqual(self._entries(".execution-"), [])

    def test_temporary_write_failure_on_overwrite_restores_bytes(self) -> None:
        before = Path(self.stack.execution).read_bytes()
        with self._failing_fdopen() as injected:
            with self.assertRaises(OSError):
                self._overwrite_call()
        self.assertTrue(injected["done"])
        self.assertEqual(Path(self.stack.execution).read_bytes(), before)
        self._retry_overwrite()

    def test_file_fsync_failure_restores_bytes(self) -> None:
        before = Path(self.stack.execution).read_bytes()
        with self._failing_file_fsync() as injected:
            with self.assertRaises(OSError):
                self._overwrite_call()
        self.assertTrue(injected["done"])
        self.assertEqual(Path(self.stack.execution).read_bytes(), before)
        self._retry_overwrite()

    def test_replace_failure_restores_bytes(self) -> None:
        before = Path(self.stack.execution).read_bytes()
        with self._failing_replace() as injected:
            with self.assertRaises(OSError):
                self._overwrite_call()
        self.assertTrue(injected["done"])
        self.assertEqual(Path(self.stack.execution).read_bytes(), before)
        self._retry_overwrite()

    def test_directory_fsync_failure_rolls_back_existing_bytes(self) -> None:
        before = Path(self.stack.execution).read_bytes()
        with self._failing_directory_fsync_once() as injected:
            with self.assertRaises(OSError):
                self._overwrite_call()
        self.assertTrue(injected["done"])
        self.assertEqual(Path(self.stack.execution).read_bytes(), before)
        self._retry_overwrite()

    def test_directory_fsync_failure_on_create_rolls_back_to_absence(
        self) -> None:
        ids = self._seed_ready_jobs(3)
        self.stack.claim_ready(3, ids[2])
        ledger = os.path.join(self.tmp.name, "execution-create.json")
        with self._failing_directory_fsync_once() as injected:
            with self.assertRaises(OSError):
                plan(self.stack.jobs, self.stack.supply, self.stack.trades,
                     self.stack.dispatch, ledger, ids[2], "ek-new",
                     "owner-3", None, 21)
        self.assertTrue(injected["done"])
        self.assertFalse(Path(ledger).exists())

    def test_rollback_directory_sync_failure_keeps_original_cause(self) -> None:
        before = Path(self.stack.execution).read_bytes()
        with self._directory_fsync_always_fails():
            with self.assertRaises(OSError) as caught:
                self._overwrite_call()
        self.assertIsInstance(caught.exception.__cause__, OSError)
        self.assertEqual(Path(self.stack.execution).read_bytes(), before)

    def test_rollback_restore_replace_failure_keeps_original_cause(self) -> None:
        real_replace = os.replace
        state = {"calls": 0}

        def flaky_replace(src, dst):
            state["calls"] += 1
            if state["calls"] == 2:
                raise OSError("injected rollback replace failure")
            return real_replace(src, dst)

        with mock.patch("os.replace", flaky_replace), \
                self._directory_fsync_always_fails():
            with self.assertRaises(OSError) as caught:
                self._overwrite_call()
        self.assertIn("rollback replace", str(caught.exception))
        self.assertIsNotNone(caught.exception.__cause__)
        self.assertIn("directory fsync", str(caught.exception.__cause__))
        sections = execution_module._load_ledger(self.stack.execution)
        self.assertIsNotNone(sections[3])


class CompletionCommitFaultTest(_FaultBase):
    module = completion_module
    prefix = ".completion-"

    def setUp(self) -> None:
        super().setUp()
        self._seed_completable_jobs(2)
        self.stack.complete_job(1)

    def _overwrite_call(self) -> None:
        self.stack.complete_job(2)

    def _retry_overwrite(self) -> None:
        record, created = self.stack.complete_job(2)
        self.assertTrue(created)
        self.assertEqual(record["job_id"], "j-2")
        raw = Path(self.stack.completions).read_bytes()
        replay, created = self.stack.complete_job(2)
        self.assertFalse(created)
        self.assertEqual(replay, record)
        self.assertEqual(Path(self.stack.completions).read_bytes(), raw)

    def test_write_failure_on_create_leaves_absence(self) -> None:
        ledger = os.path.join(self.tmp.name, "completions-create.json")
        with self._failing_fdopen() as injected:
            with self.assertRaises(OSError):
                complete(self.stack.jobs, self.stack.supply,
                         self.stack.signals, self.stack.trades,
                         self.stack.dispatch, self.stack.execution, ledger,
                         "j-2", "xk-new", 90, "succeeded", 10, 20)
        self.assertTrue(injected["done"])
        self.assertFalse(Path(ledger).exists())
        self._assert_no_fragments()

    def test_write_failure_on_overwrite_restores_bytes(self) -> None:
        before = Path(self.stack.completions).read_bytes()
        with self._failing_fdopen() as injected:
            with self.assertRaises(OSError):
                self._overwrite_call()
        self.assertTrue(injected["done"])
        self.assertEqual(Path(self.stack.completions).read_bytes(), before)
        self._retry_overwrite()

    def test_replace_failure_restores_bytes(self) -> None:
        before = Path(self.stack.completions).read_bytes()
        with self._failing_replace() as injected:
            with self.assertRaises(OSError):
                self._overwrite_call()
        self.assertTrue(injected["done"])
        self.assertEqual(Path(self.stack.completions).read_bytes(), before)
        self._retry_overwrite()

    def test_directory_fsync_failure_rolls_back_existing_bytes(self) -> None:
        before = Path(self.stack.completions).read_bytes()
        with self._failing_directory_fsync_once() as injected:
            with self.assertRaises(OSError):
                self._overwrite_call()
        self.assertTrue(injected["done"])
        self.assertEqual(Path(self.stack.completions).read_bytes(), before)
        self._retry_overwrite()

    def test_rollback_failure_keeps_original_cause(self) -> None:
        before = Path(self.stack.completions).read_bytes()
        with self._directory_fsync_always_fails():
            with self.assertRaises(OSError) as caught:
                self._overwrite_call()
        self.assertIsInstance(caught.exception.__cause__, OSError)
        self.assertEqual(Path(self.stack.completions).read_bytes(), before)
        self._assert_no_fragments()


# ---------------------------------------------------------------------------
# First-call atomicity, replay and conflicting-key semantics
# ---------------------------------------------------------------------------

class PublicationAndReplayTest(LifecycleTestBase):
    def test_first_bytes_are_exactly_the_legacy_bytes(self) -> None:
        ids = self._seed_ready_jobs(1)
        self.assertEqual(Path(self.stack.dispatch).read_bytes(),
                         _DISPATCH_FIRST_BYTES)

    def test_unicode_first_bytes_are_exactly_the_legacy_bytes(self) -> None:
        submit(self.stack.jobs, _job("j-é"), "jk-u")
        clear_live(self.stack.jobs, self.stack.supply, self.stack.signals,
                   self.stack.trades, "j-é", "tk-u", 40)
        ledger = os.path.join(self.tmp.name, "dispatch-u.json")
        commit(self.stack.jobs, self.stack.supply, self.stack.trades,
               ledger, "j-é", "dk-é", 50)
        self.assertEqual(Path(ledger).read_bytes(),
                         _DISPATCH_UNICODE_FIRST_BYTES)

    def test_execution_first_bytes_are_exactly_the_legacy_bytes(self) -> None:
        ids = self._seed_ready_jobs(1)
        self.stack.claim_ready(1, ids[0])
        self.stack.plan_launch(1, ids[0])
        self.assertEqual(Path(self.stack.execution).read_bytes(),
                         _EXECUTION_FIRST_BYTES)

    def test_completion_first_bytes_are_exactly_the_legacy_bytes(self) -> None:
        self.stack.finish_to_terminal(1)
        self.stack.complete_job(1)
        self.assertEqual(Path(self.stack.completions).read_bytes(),
                         _COMPLETION_FIRST_BYTES)

    def test_every_ledger_has_exactly_one_trailing_newline(self) -> None:
        self._seed_completable_jobs(1)
        self.stack.complete_job(1)
        for path in (self.stack.dispatch, self.stack.execution,
                     self.stack.completions):
            raw = Path(path).read_bytes()
            self.assertTrue(raw.endswith(b"\n"))
            self.assertFalse(raw.endswith(b"\n\n"))

    def test_same_key_same_request_replays_without_writing(self) -> None:
        ids = self._seed_ready_jobs(1)
        raw = Path(self.stack.dispatch).read_bytes()
        first, created = claim(self.stack.dispatch, ids[0], "ck-same",
                               "owner-1", 40, 20)
        self.assertTrue(created)
        raw_after = Path(self.stack.dispatch).read_bytes()
        self.assertNotEqual(raw_after, raw)
        replay, created = claim(self.stack.dispatch, ids[0], "ck-same",
                                "owner-1", 40, 20)
        self.assertFalse(created)
        self.assertEqual(replay, first)
        self.assertEqual(Path(self.stack.dispatch).read_bytes(), raw_after)

        self.stack.plan_launch(1, ids[0])
        receipt, created = record(
            self.stack.execution, ids[0], 1, "rk-same", "owner-1",
            "stage", "succeeded", "tok", 22)
        self.assertTrue(created)
        plan_raw_after = Path(self.stack.execution).read_bytes()
        replay, created = record(
            self.stack.execution, ids[0], 1, "rk-same", "owner-1",
            "stage", "succeeded", "tok", 22)
        self.assertFalse(created)
        self.assertEqual(replay, receipt)
        # Replay rewrites nothing at all.
        self.assertEqual(Path(self.stack.execution).read_bytes(),
                         plan_raw_after)

    def test_same_key_different_request_leaves_ledger_untouched(self) -> None:
        ids = self._seed_ready_jobs(1)
        claim(self.stack.dispatch, ids[0], "ck-conflict", "owner-1", 40, 20)
        before = Path(self.stack.dispatch).read_bytes()
        with self.assertRaises(ValueError):
            claim(self.stack.dispatch, ids[0], "ck-conflict", "owner-1",
                  39, 20)
        self.assertEqual(Path(self.stack.dispatch).read_bytes(), before)

        self.stack.plan_launch(1, ids[0])
        record(self.stack.execution, ids[0], 1, "rk-conflict", "owner-1",
               "stage", "succeeded", "tok-a", 22)
        before = Path(self.stack.execution).read_bytes()
        with self.assertRaises(ValueError):
            record(self.stack.execution, ids[0], 1, "rk-conflict", "owner-1",
                   "stage", "succeeded", "tok-b", 22)
        self.assertEqual(Path(self.stack.execution).read_bytes(), before)

    def test_completion_same_key_replay_and_conflict(self) -> None:
        self.stack.finish_to_terminal(1)
        first, created = self.stack.complete_job(1, key="xk-same")
        self.assertTrue(created)
        raw = Path(self.stack.completions).read_bytes()
        replay, created = self.stack.complete_job(1, key="xk-same")
        self.assertFalse(created)
        self.assertEqual(replay, first)
        self.assertEqual(Path(self.stack.completions).read_bytes(), raw)
        with self.assertRaises(ValueError):
            self.stack.complete_job(1, key="xk-other", at=91)
        self.assertEqual(Path(self.stack.completions).read_bytes(), raw)


# ---------------------------------------------------------------------------
# Lock ordering for joint operations
# ---------------------------------------------------------------------------

class LockOrderingTest(LifecycleTestBase):
    def _recording_lock(self, module, order: list[tuple[str, bool]]):
        real_lock = module._lock

        @contextlib.contextmanager
        def recorder(realpath: str, *, shared: bool = False):
            order.append((realpath, shared))
            with real_lock(realpath, shared=shared):
                yield

        return recorder

    def test_dispatch_commit_takes_locks_in_real_path_order(self) -> None:
        self._seed_ready_jobs(1)
        os.unlink(self.stack.dispatch)
        order: list[tuple[str, bool]] = []
        with mock.patch.object(dispatch_module, "_lock",
                               self._recording_lock(dispatch_module, order)):
            commit(self.stack.jobs, self.stack.supply, self.stack.trades,
                   self.stack.dispatch, "j-1", "dk-order", 10)
        paths = [path for path, _shared in order]
        self.assertEqual(paths, sorted(paths))
        locked = dict(order)
        self.assertFalse(locked[os.path.realpath(self.stack.dispatch)])
        for snapshot in (self.stack.jobs, self.stack.supply,
                         self.stack.trades):
            self.assertTrue(locked[os.path.realpath(snapshot)])

    def test_execution_plan_takes_locks_in_real_path_order(self) -> None:
        ids = self._seed_ready_jobs(1)
        self.stack.claim_ready(1, ids[0])
        order: list[tuple[str, bool]] = []
        with mock.patch.object(execution_module, "_lock",
                               self._recording_lock(execution_module, order)):
            plan(self.stack.jobs, self.stack.supply, self.stack.trades,
                 self.stack.dispatch, self.stack.execution, ids[0],
                 "ek-order", "owner-1", None, 21)
        paths = [path for path, _shared in order]
        self.assertEqual(paths, sorted(paths))
        self.assertFalse(dict(order)[
            os.path.realpath(self.stack.execution)])

    def test_completion_takes_locks_in_real_path_order(self) -> None:
        self.stack.finish_to_terminal(1)
        order: list[tuple[str, bool]] = []
        with mock.patch.object(completion_module, "_lock",
                               self._recording_lock(completion_module,
                                                    order)):
            self.stack.complete_job(1)
        paths = [path for path, _shared in order]
        self.assertEqual(paths, sorted(paths))
        self.assertFalse(dict(order)[
            os.path.realpath(self.stack.completions)])


# ---------------------------------------------------------------------------
# Thread races: whole versions, once-only publication and no-write replay
# ---------------------------------------------------------------------------

class _ThreadHarness:
    def __init__(self, test: unittest.TestCase) -> None:
        self.test = test
        self.outcomes: dict[str, object] = {}

    def spawn(self, name: str, fn, barrier: threading.Barrier | None = None
              ) -> threading.Thread:
        def target() -> None:
            try:
                if barrier is not None:
                    barrier.wait(_WAIT)
                self.outcomes[name] = fn()
            except BaseException as exc:  # asserted from the main thread
                self.outcomes[name] = exc

        thread = threading.Thread(target=target, name=name, daemon=True)
        thread.start()
        return thread

    def join(self, *threads: threading.Thread) -> None:
        for thread in threads:
            thread.join(_WAIT)
            self.test.assertFalse(thread.is_alive(),
                                 f"{thread.name} stuck (possible deadlock)")

    def result(self, name: str):
        outcome = self.outcomes[name]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class ThreadRaceTest(LifecycleTestBase):
    def test_dispatch_distinct_keys_all_publish_once(self) -> None:
        count = 4
        ids = self._seed_ready_jobs(count)
        harness = _ThreadHarness(self)
        barrier = threading.Barrier(count)
        threads = []
        for index, job_id in enumerate(ids, start=1):
            threads.append(harness.spawn(
                f"claim-{index}",
                lambda j=job_id, i=index: claim(
                    self.stack.dispatch, j, f"ck-{i}", f"owner-{i}",
                    40, 20),
                barrier))
        harness.join(*threads)
        for index, job_id in enumerate(ids, start=1):
            decision, created = harness.result(f"claim-{index}")
            self.assertTrue(created)
            self.assertEqual(decision["state"], "claimed")
            self.assertEqual(decision["owner"], f"owner-{index}")
        data = json.loads(Path(self.stack.dispatch).read_text("utf-8"))
        self.assertEqual(
            sorted(data["idempotency"]),
            sorted([f"dk-{i}" for i in range(1, count + 1)]
                   + [f"ck-{i}" for i in range(1, count + 1)]))
        self.assertEqual(set(data["audit"]), set(data["idempotency"]))
        # Every committed decision remains internally consistent.
        dispatch_module._load_ledger(self.stack.dispatch)

    def test_dispatch_same_key_race_publishes_exactly_once(self) -> None:
        ids = self._seed_ready_jobs(1)
        harness = _ThreadHarness(self)
        barrier = threading.Barrier(2)
        call = lambda: claim(self.stack.dispatch, ids[0], "ck-dup",
                             "owner-1", 40, 20)
        first = harness.spawn("claim-1", call, barrier)
        second = harness.spawn("claim-2", call, barrier)
        harness.join(first, second)
        results = [harness.result("claim-1"), harness.result("claim-2")]
        created_flags = sorted(created for _d, created in results)
        self.assertEqual(created_flags, [False, True])
        self.assertEqual(results[0][0], results[1][0])
        data = json.loads(Path(self.stack.dispatch).read_text("utf-8"))
        self.assertEqual(sorted(data["idempotency"]), ["ck-dup", "dk-1"])

    def test_execution_distinct_keys_all_publish_once(self) -> None:
        count = 3
        ids = self._seed_active_plans(count)
        harness = _ThreadHarness(self)
        barrier = threading.Barrier(count)
        threads = []
        for index, job_id in enumerate(ids, start=1):
            threads.append(harness.spawn(
                f"record-{index}",
                lambda j=job_id, i=index: record(
                    self.stack.execution, j, 1, f"rk-{i}", f"owner-{i}",
                    "stage", "succeeded", f"tok-{i}", 22),
                barrier))
        harness.join(*threads)
        for index in range(1, count + 1):
            plan_doc, created = harness.result(f"record-{index}")
            self.assertTrue(created)
            self.assertEqual(plan_doc["steps"][0]["receipt"],
                             f"tok-{index}")
        plans, _keys, events, raw = execution_module._load_ledger(
            self.stack.execution)
        self.assertIsNotNone(raw)
        for index, job_id in enumerate(ids, start=1):
            self.assertEqual(plans[job_id]["1"]["state"], "active")
            self.assertEqual(len(plans[job_id]["1"]["steps"]), 1)
        # One plan event plus one record event per job, each once.
        self.assertEqual(len(events), 2 * count)

    def test_execution_same_key_race_publishes_exactly_once(self) -> None:
        ids = self._seed_active_plans(1)
        harness = _ThreadHarness(self)
        barrier = threading.Barrier(2)
        call = lambda: record(self.stack.execution, ids[0], 1, "rk-dup",
                              "owner-1", "stage", "succeeded", "tok", 22)
        first = harness.spawn("record-1", call, barrier)
        second = harness.spawn("record-2", call, barrier)
        harness.join(first, second)
        results = [harness.result("record-1"), harness.result("record-2")]
        self.assertEqual(sorted(created for _p, created in results),
                         [False, True])
        plans, _keys, _events, _raw = execution_module._load_ledger(
            self.stack.execution)
        self.assertEqual(len(plans[ids[0]]["1"]["steps"]), 1)

    def test_completion_distinct_jobs_all_publish_once(self) -> None:
        count = 3
        ids = self._seed_completable_jobs(count)
        harness = _ThreadHarness(self)
        barrier = threading.Barrier(count)
        threads = []
        for index, job_id in enumerate(ids, start=1):
            threads.append(harness.spawn(
                f"complete-{index}",
                lambda j=job_id, i=index: self.stack.complete_job(i, j),
                barrier))
        harness.join(*threads)
        for index, job_id in enumerate(ids, start=1):
            record_doc, created = harness.result(f"complete-{index}")
            self.assertTrue(created)
            self.assertEqual(record_doc["job_id"], job_id)
        page = search(self.stack.completions)
        self.assertEqual(sorted(entry["job_id"] for entry in page["entries"]),
                         sorted(ids))
        data = json.loads(Path(self.stack.completions).read_text("utf-8"))
        self.assertEqual(len(data["completions"]), count)
        self.assertEqual(set(data["idempotency"]), set(data["audit"]))

    def test_completion_same_key_race_publishes_exactly_once(self) -> None:
        self.stack.finish_to_terminal(1)
        harness = _ThreadHarness(self)
        barrier = threading.Barrier(2)
        call = lambda: self.stack.complete_job(1, key="xk-dup")
        first = harness.spawn("complete-1", call, barrier)
        second = harness.spawn("complete-2", call, barrier)
        harness.join(first, second)
        results = [harness.result("complete-1"),
                   harness.result("complete-2")]
        self.assertEqual(sorted(created for _r, created in results),
                         [False, True])
        self.assertEqual(results[0][0], results[1][0])
        data = json.loads(Path(self.stack.completions).read_text("utf-8"))
        self.assertEqual(list(data["completions"]), ["xk-dup"])

    def test_concurrent_readers_always_see_whole_versions(self) -> None:
        # Writers replace the ledgers while readers loop: every observed
        # version must pass the strict canonical loader (a torn read
        # cannot) and the completion public queries must never raise.
        self._seed_ready_jobs(3)
        errors: list[BaseException] = []
        stop = threading.Event()

        def read_dispatch() -> None:
            try:
                while not stop.is_set():
                    decisions, _keys, _events, raw = \
                        dispatch_module._load_ledger(self.stack.dispatch)
                    if raw is not None:
                        self.assertGreaterEqual(len(decisions), 3)
            except BaseException as exc:  # pragma: no cover - failure path
                errors.append(exc)

        def read_execution() -> None:
            try:
                while not stop.is_set():
                    plans, _keys, _events, raw = \
                        execution_module._load_ledger(self.stack.execution)
                    if raw is not None:
                        for attempts in plans.values():
                            for plan_doc in attempts.values():
                                self.assertLessEqual(
                                    len(plan_doc["steps"]), 2)
            except BaseException as exc:  # pragma: no cover - failure path
                errors.append(exc)

        readers = [threading.Thread(target=read_dispatch, daemon=True),
                   threading.Thread(target=read_execution, daemon=True)]
        for reader in readers:
            reader.start()

        # Write three more dispatch claims and three execution records,
        # each replacing the whole ledger atomically.
        more_dispatch = []
        for index in (4, 5, 6):
            job_id = self.stack.submit(index)
            self.stack.trade(index, job_id)
            self.stack.commit_dispatch(index, job_id)
            more_dispatch.append((index, job_id))
        for index, job_id in more_dispatch:
            claim(self.stack.dispatch, job_id, f"ck-{index}",
                  f"owner-{index}", 40, 20)

        for index in (7, 8, 9):
            job_id = self.stack.submit(index)
            self.stack.trade(index, job_id)
            self.stack.commit_dispatch(index, job_id)
            self.stack.claim_ready(index, job_id)
            self.stack.plan_launch(index, job_id)
            self.stack.record_stage(index, job_id, step="stage", at=22,
                                    receipt=f"tok-{index}-stage")
            self.stack.record_stage(index, job_id, step="start", at=23,
                                    receipt=f"tok-{index}-start")

        stop.set()
        for reader in readers:
            reader.join(_WAIT)
            self.assertFalse(reader.is_alive())
        self.assertEqual(errors, [])


# ---------------------------------------------------------------------------
# Independent-process races over the companion flocks
# ---------------------------------------------------------------------------

def _proc_dispatch_writer(stack_paths: dict[str, str], index: int,
                          queue) -> None:
    try:
        job_id = f"j-{index}"
        _decision, created = claim(
            stack_paths["dispatch"], job_id, f"ck-{index}",
            f"owner-{index}", 40, 20)
        queue.put((index, "ok", created))
    except BaseException:
        queue.put((index, "err", traceback.format_exc()))


def _proc_execution_writer(stack_paths: dict[str, str], index: int,
                           queue) -> None:
    try:
        job_id = f"j-{index}"
        _plan, created = record(
            stack_paths["execution"], job_id, 1, f"rk-{index}",
            f"owner-{index}", "stage", "succeeded", f"tok-{index}", 22)
        queue.put((index, "ok", created))
    except BaseException:
        queue.put((index, "err", traceback.format_exc()))


def _proc_completion_writer(stack_paths: dict[str, str], index: int,
                            queue) -> None:
    try:
        job_id = f"j-{index}"
        _record, created = complete(
            stack_paths["jobs"], stack_paths["supply"],
            stack_paths["signals"], stack_paths["trades"],
            stack_paths["dispatch"], stack_paths["execution"],
            stack_paths["completions"], job_id, f"xk-{index}", 90,
            "succeeded", 10, 20)
        queue.put((index, "ok", created))
    except BaseException:
        queue.put((index, "err", traceback.format_exc()))


class ProcessRaceTest(LifecycleTestBase):
    COUNT = 3

    def _run_writers(self, target, seed) -> None:
        ids = seed(self.COUNT)
        ctx = multiprocessing.get_context("fork")
        queue = ctx.Queue()
        paths = dict(vars(self.stack))
        del paths["dir"]
        processes = []
        for index in range(1, self.COUNT + 1):
            process = ctx.Process(
                target=target, args=(paths, index, queue))
            process.start()
            processes.append(process)
        results = {}
        for _ in range(self.COUNT):
            index, status, payload = queue.get(timeout=_WAIT)
            results[index] = (status, payload)
        for process in processes:
            process.join(_WAIT)
            self.assertFalse(process.is_alive(),
                             "a writer process is stuck (possible "
                             "cross-process deadlock)")
        for index in range(1, self.COUNT + 1):
            status, payload = results[index]
            self.assertEqual(status, "ok", payload)
            self.assertIs(payload, True)
        return ids

    def test_dispatch_writers_race_across_processes(self) -> None:
        ids = self._run_writers(_proc_dispatch_writer,
                                self._seed_ready_jobs)
        decisions, idempotency, events, raw = dispatch_module._load_ledger(
            self.stack.dispatch)
        self.assertIsNotNone(raw)
        self.assertEqual(
            sorted(idempotency),
            sorted([f"ck-{i}" for i in range(1, self.COUNT + 1)]
                   + [f"dk-{i}" for i in range(1, self.COUNT + 1)]))
        self.assertEqual(set(events), set(idempotency))
        for index, job_id in enumerate(ids, start=1):
            self.assertEqual(decisions[job_id]["state"], "claimed")
            self.assertEqual(decisions[job_id]["owner"], f"owner-{index}")
        self.assertEqual(self._entries(".dispatch-"), [])

    def test_execution_writers_race_across_processes(self) -> None:
        ids = self._run_writers(_proc_execution_writer,
                                self._seed_active_plans)
        plans, idempotency, events, raw = execution_module._load_ledger(
            self.stack.execution)
        self.assertIsNotNone(raw)
        self.assertEqual(
            sorted(idempotency),
            sorted([f"rk-{i}" for i in range(1, self.COUNT + 1)]
                   + [f"ek-{i}" for i in range(1, self.COUNT + 1)]))
        for index, job_id in enumerate(ids, start=1):
            steps = plans[job_id]["1"]["steps"]
            self.assertEqual(len(steps), 1)
            self.assertEqual(steps[0]["receipt"], f"tok-{index}")
        self.assertEqual(self._entries(".execution-"), [])

    def test_completion_writers_race_across_processes(self) -> None:
        ids = self._run_writers(_proc_completion_writer,
                                self._seed_completable_jobs)
        records, idempotency, events, raw = \
            completion_module._load_completion_ledger(self.stack.completions)
        self.assertIsNotNone(raw)
        self.assertEqual(sorted(idempotency),
                         [f"xk-{i}" for i in range(1, self.COUNT + 1)])
        self.assertEqual(set(events), set(records))
        page = search(self.stack.completions)
        self.assertEqual(sorted(entry["job_id"] for entry in page["entries"]),
                         sorted(ids))
        self.assertEqual(self._entries(".completion-"), [])


# ---------------------------------------------------------------------------
# Public surface: no new entries, exports unchanged
# ---------------------------------------------------------------------------

class PublicSurfaceTest(unittest.TestCase):
    def test_module_exports_are_unchanged(self) -> None:
        self.assertEqual(dispatch_module.__all__,
                         ["commit", "claim", "finish", "recover"])
        self.assertEqual(execution_module.__all__,
                         ["plan", "record", "recover"])
        self.assertEqual(completion_module.__all__,
                         ["complete", "get", "search", "get_response",
                          "search_response"])

    def test_infrastructure_module_is_private(self) -> None:
        self.assertTrue(_lifecycle.__name__.startswith("carbon_market._"))
        self.assertFalse(hasattr(_lifecycle, "commit"))
        self.assertFalse(hasattr(_lifecycle, "complete"))


if __name__ == "__main__":
    unittest.main()
