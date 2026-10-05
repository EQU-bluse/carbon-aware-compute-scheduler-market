"""Shared persistence semantics for the three lifecycle ledgers.

These tests pin the behavior the dispatch, execution and completion
ledgers inherited from one another and now obtain from the shared
private ``carbon_market._lifecycle`` module:

* every read still rejects non-UTF-8 bytes, malformed or non-finite
  JSON, negative-zero literals, wrong field/key order and a missing or
  duplicated trailing newline;
* missing ledgers keep their per-entry difference -- the first-serve
  call creates, later write calls raise ``FileNotFoundError``, while
  completion ``get`` raises ``KeyError`` and its HTTP-ready queries
  raise ``FileNotFoundError``;
* each commit writes a synced same-directory temporary, atomically
  replaces and fsyncs the directory, restoring the exact prior bytes
  (or absence) on failure and chaining after the original error when the
  restore itself fails;
* concurrent first-serve calls, in threads and in separate processes,
  collapse to exactly one create plus one replay, and a same-key /
  different-request clash never corrupts or rolls the ledger back;
* the produced bytes are byte-for-byte the golden ledgers captured
  before the refactor.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import subprocess
import sys
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from carbon_market import _lifecycle
from carbon_market import completion as completion_module
from carbon_market import dispatch as dispatch_module
from carbon_market import execution as execution_module
from carbon_market.completion import get as completion_get
from carbon_market.completion import get_response, search, search_response

from tests import _lifecycle_fixture as fx

_REPO = Path(__file__).resolve().parents[1]
_GOLDEN = _REPO / "tests" / "golden_lifecycle"
_WAIT = 20.0


# ---------------------------------------------------------------------
# Per-ledger adapters
# ---------------------------------------------------------------------

class _Adapter:
    module = None
    kind = ""
    prefix = ""
    job_main = "j-é"
    job_new = "j-a"
    read_job = "j-é"

    def seed(self, base: str) -> dict[str, str]:
        raise NotImplementedError

    def first_call(self, p: dict[str, str], ledger: str) -> object:
        raise NotImplementedError

    def read_call(self, ledger: str) -> object:
        raise NotImplementedError

    def missing_write_call(self, ledger: str) -> object:
        raise NotImplementedError


class _DispatchAdapter(_Adapter):
    module = dispatch_module
    kind = "dispatch"
    prefix = ".dispatch-"
    job_new = "j-a"

    def seed(self, base: str) -> dict[str, str]:
        # Both inputs are traded; only j-é is committed in the
        # existing dispatch ledger, so the absent-ledger create call
        # for j-a finds its trade while the overwrite fault call for
        # j-é exercises an existing ledger.
        p = fx.seed_inputs(base, self.job_main, self.job_new)
        fx.trade(p, self.job_main, "t-0")
        fx.trade(p, self.job_new, "t-1")
        fx.commit(p, self.job_main, "d-0")
        return p

    def first_call(self, p: dict[str, str], ledger: str) -> object:
        return dispatch_module.claim(
            ledger, self.job_main, "rk-fail", "owner-0", 30, 60)

    def create_call(self, p: dict[str, str], ledger: str) -> object:
        return dispatch_module.commit(
            p["jobs.json"], p["supply.json"], p["clear.json"], ledger,
            self.job_new, "rk-new", 10)

    def read_call(self, ledger: str) -> object:
        return dispatch_module.claim(
            ledger, self.read_job, "rk-read", "owner-x", 30, 60)

    def missing_write_call(self, ledger: str) -> object:
        return dispatch_module.claim(
            ledger, self.job_main, "rk-missing", "owner-0", 30, 60)


class _ExecutionAdapter(_Adapter):
    module = execution_module
    kind = "execution"
    prefix = ".execution-"
    job_new = "j-b"

    def seed(self, base: str) -> dict[str, str]:
        p = fx.seed_inputs(base, self.job_main, self.job_new)
        for index, job_id in enumerate((self.job_main, self.job_new)):
            fx.trade(p, job_id, f"t-{index}")
            fx.commit(p, job_id, f"d-{index}")
            fx.claim(p, job_id, f"c-{index}", f"owner-{index}")
        fx.plan(p, self.job_main, "p-0", "owner-0")
        return p

    def first_call(self, p: dict[str, str], ledger: str) -> object:
        return execution_module.record(
            ledger, self.job_main, 1, "rk-fail", "owner-0", "stage",
            "succeeded", "staged", 62)

    def create_call(self, p: dict[str, str], ledger: str) -> object:
        return execution_module.plan(
            p["jobs.json"], p["supply.json"], p["clear.json"],
            p["dispatch.json"], ledger, self.job_new, "rk-new",
            "owner-1", None, 61)

    def read_call(self, ledger: str) -> object:
        return execution_module.recover(
            ledger, self.read_job, 1, "rk-read", 80)

    def missing_write_call(self, ledger: str) -> object:
        return execution_module.record(
            ledger, self.job_main, 1, "rk-missing", "owner-0", "stage",
            "succeeded", "staged", 62)


class _CompletionAdapter(_Adapter):
    module = completion_module
    kind = "completion"
    prefix = ".completion-"
    job_new = "j-b"

    def seed(self, base: str) -> dict[str, str]:
        # j-é is fully completed in the existing completion ledger;
        # j-b is traded and sitting at a stable terminal state but
        # not yet completed, so the absent-ledger create call for
        # j-b succeeds.
        p = fx.seed_inputs(base, self.job_main, self.job_new)
        fx.trade(p, self.job_main, "t-0")
        fx.build_completed(p, self.job_main, "x-0")
        fx.complete_job(p, self.job_main, "x-0")
        fx.trade(p, self.job_new, "t-1")
        fx.build_completed(p, self.job_new, "x-1")
        return p

    def first_call(self, p: dict[str, str], ledger: str) -> object:
        return completion_module.complete(
            p["jobs.json"], p["supply.json"], p["signals.json"],
            p["clear.json"], p["dispatch.json"], p["execution.json"],
            ledger, self.job_new, "rk-fail", 70, "succeeded", 1, 1)

    def create_call(self, p: dict[str, str], ledger: str) -> object:
        return self.first_call(p, ledger)

    def read_call(self, ledger: str) -> object:
        return completion_get(ledger, self.read_job)

    def missing_write_call(self, ledger: str) -> object:
        # complete() is the only writer, and a missing target is the
        # create case (exercised by the create tests); run it against a
        # job already completed in this ledger to hit the replay/no-write
        # path for completeness.
        return completion_module.complete(
            *self._snapshot_paths(ledger),
            ledger, self.job_main, "x-0", 70, "succeeded", 1, 1)

    def _snapshot_paths(self, ledger: str) -> list[str]:
        base = os.path.dirname(ledger)
        return [os.path.join(base, name) for name in (
            "jobs.json", "supply.json", "signals.json", "clear.json",
            "dispatch.json", "execution.json")]


_ADAPTERS = (_DispatchAdapter(), _ExecutionAdapter(), _CompletionAdapter())


# ---------------------------------------------------------------------
# Golden byte identity
# ---------------------------------------------------------------------

class GoldenBytesTest(unittest.TestCase):
    def test_lifecycle_ledgers_match_pre_refactor_bytes(self) -> None:
        with TemporaryDirectory() as base:
            p = fx.seed_inputs(base, "j-é")
            # Exactly the call sequence the golden generator used:
            # the synchronization key is "b-0", not derived from the
            # completion key.
            fx.trade(p, "j-é", "t-0")
            fx.commit(p, "j-é", "d-0")
            fx.claim(p, "j-é", "c-0", "owner-0")
            fx.plan(p, "j-é", "p-0", "owner-0")
            execution_module.record(p["execution.json"], "j-é", 1,
                                    "s1-0", "owner-0", "stage",
                                    "succeeded", "staged", 62)
            execution_module.record(p["execution.json"], "j-é", 1,
                                    "s2-0", "owner-0", "start",
                                    "succeeded", "started", 63)
            fx.synchronize(p, "b-0")
            fx.complete_job(p, "j-é", "x-0")
            for name in ("dispatch.json", "execution.json",
                          "completion.json"):
                self.assertEqual(
                    Path(p[name]).read_bytes(),
                    (_GOLDEN / name).read_bytes(),
                    f"{name} bytes diverged from the golden ledger")


# ---------------------------------------------------------------------
# Strict reading and missing-file mapping
# ---------------------------------------------------------------------

class StrictReadTest(unittest.TestCase):
    def _two_entry_bytes(self, adapter: _Adapter) -> bytes:
        with TemporaryDirectory() as base:
            p = adapter.seed(base)
            ledger = self._ledger_path(adapter, p)
            if not os.path.exists(ledger):
                adapter.create_call(p, ledger)
            adapter.first_call(p, ledger)
            return Path(ledger).read_bytes()

    def _ledger_path(self, adapter: _Adapter, p: dict[str, str]) -> str:
        return p[f"{adapter.kind}.json"]

    def _rebuild(self, raw: bytes, *, root_order: list[str] | None = None,
                 reverse_keys: bool = False) -> bytes:
        data = json.loads(raw.decode("utf-8"))
        sections = [section for section in data
                    if section != "version"]
        if reverse_keys:
            for section in sections:
                keys = list(data[section])
                data[section] = {key: data[section][key]
                                  for key in reversed(keys)}
        if root_order is not None:
            ordered = {name: data[name] for name in root_order}
            ordered["version"] = data["version"]
            data = ordered
        return (json.dumps(data, ensure_ascii=False,
                            separators=(",", ":"), allow_nan=False)
                + "\n").encode("utf-8")

    def test_non_utf8_rejected(self) -> None:
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                raw = self._two_entry_bytes(adapter)
                with TemporaryDirectory() as base:
                    ledger = os.path.join(base, f"{adapter.kind}.json")
                    Path(ledger).write_bytes(raw[:40] + b"\xff" + raw[40:])
                    with self.assertRaises(ValueError):
                        adapter.read_call(ledger)
                    self.assertEqual(
                        [name for name in sorted(os.listdir(base))
                         if not name.endswith(".lock")],
                        [f"{adapter.kind}.json"])

    def test_malformed_json_rejected(self) -> None:
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                raw = self._two_entry_bytes(adapter)
                with TemporaryDirectory() as base:
                    ledger = os.path.join(base, f"{adapter.kind}.json")
                    Path(ledger).write_bytes(raw[:-3] + b"\n")
                    with self.assertRaises(ValueError):
                        adapter.read_call(ledger)

    def test_non_finite_literal_rejected(self) -> None:
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                raw = self._two_entry_bytes(adapter)
                with TemporaryDirectory() as base:
                    ledger = os.path.join(base, f"{adapter.kind}.json")
                    Path(ledger).write_bytes(
                        raw.replace(b'"version":1', b'"version":NaN', 1))
                    with self.assertRaises(ValueError):
                        adapter.read_call(ledger)

    def test_negative_zero_literal_rejected(self) -> None:
        needles = {
            "dispatch": (b'"at":10', b'"at":-0'),
            "execution": (b'"at":61', b'"at":-0'),
            "completion": (b'"at":70', b'"at":-0'),
        }
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                raw = self._two_entry_bytes(adapter)
                old, new = needles[adapter.kind]
                self.assertIn(old, raw)
                with TemporaryDirectory() as base:
                    ledger = os.path.join(base, f"{adapter.kind}.json")
                    Path(ledger).write_bytes(raw.replace(old, new, 1))
                    with self.assertRaises(ValueError):
                        adapter.read_call(ledger)

    def test_root_field_order_rejected(self) -> None:
        orders = {
            "dispatch": ["audit", "idempotency", "decisions", "version"],
            "execution": ["audit", "idempotency", "plans", "version"],
            "completion": ["audit", "idempotency", "completions",
                            "version"],
        }
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                raw = self._two_entry_bytes(adapter)
                rebuilt = self._rebuild(raw, root_order=orders[adapter.kind])
                with TemporaryDirectory() as base:
                    ledger = os.path.join(base, f"{adapter.kind}.json")
                    Path(ledger).write_bytes(rebuilt)
                    with self.assertRaises(ValueError):
                        adapter.read_call(ledger)

    def test_primary_key_order_rejected(self) -> None:
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                raw = self._two_entry_bytes(adapter)
                rebuilt = self._rebuild(raw, reverse_keys=True)
                self.assertNotEqual(rebuilt, raw)
                with TemporaryDirectory() as base:
                    ledger = os.path.join(base, f"{adapter.kind}.json")
                    Path(ledger).write_bytes(rebuilt)
                    with self.assertRaises(ValueError):
                        adapter.read_call(ledger)

    def test_trailing_newline_rules_enforced(self) -> None:
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                raw = self._two_entry_bytes(adapter)
                with TemporaryDirectory() as base:
                    missing = os.path.join(base, "missing-newline.json")
                    extra = os.path.join(base, "extra-newline.json")
                    Path(missing).write_bytes(raw[:-1])
                    Path(extra).write_bytes(raw + b"\n")
                    with self.assertRaises(ValueError):
                        adapter.read_call(missing)
                    with self.assertRaises(ValueError):
                        adapter.read_call(extra)

    def test_read_only_query_creates_no_ledger(self) -> None:
        with TemporaryDirectory() as base:
            ledger = os.path.join(base, "absent-completion.json")
            with self.assertRaises(KeyError):
                completion_get(ledger, "j-x")
            with self.assertRaises(FileNotFoundError):
                search(ledger)
            with self.assertRaises(FileNotFoundError):
                get_response(ledger, "j-x")
            with self.assertRaises(FileNotFoundError):
                search_response(ledger)
            # The business ledger itself was never created; only the
            # companion lock file may appear.
            self.assertFalse(os.path.exists(ledger))
            self.assertEqual(
                [name for name in os.listdir(base)
                 if not name.endswith(".lock")], [])

    def test_missing_ledger_mapping_keeps_module_differences(self) -> None:
        with TemporaryDirectory() as base:
            dispatch_absent = os.path.join(base, "dispatch-absent.json")
            execution_absent = os.path.join(base, "execution-absent.json")
            completion_absent = os.path.join(base, "completion-absent.json")
            with self.assertRaises(FileNotFoundError):
                dispatch_module.claim(
                    dispatch_absent, "j-é", "rk", "owner", 30, 60)
            with self.assertRaises(FileNotFoundError):
                dispatch_module.finish(
                    dispatch_absent, "j-é", "rk", "owner", "succeeded", 60)
            with self.assertRaises(FileNotFoundError):
                dispatch_module.recover(
                    dispatch_absent, "j-é", "rk", 70)
            with self.assertRaises(FileNotFoundError):
                execution_module.record(
                    execution_absent, "j-é", 1, "rk", "owner", "stage",
                    "succeeded", "receipt", 62)
            with self.assertRaises(FileNotFoundError):
                execution_module.recover(
                    execution_absent, "j-é", 1, "rk", 80)
            with self.assertRaises(KeyError):
                completion_get(completion_absent, "j-é")
            with self.assertRaises(FileNotFoundError):
                search(completion_absent)
            with self.assertRaises(FileNotFoundError):
                get_response(completion_absent, "j-é")


# ---------------------------------------------------------------------
# Commit fault injection
# ---------------------------------------------------------------------

class _CommitFaultBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _pristine(self, adapter: _Adapter) -> tuple[dict[str, str], str, str]:
        """Return (snapshot paths, existing ledger path, absent path).

        Each adapter gets its own subdirectory, so the three layers can
        share one TemporaryDirectory without colliding on input ids.
        """
        base = os.path.join(self.tmp.name, f"fault-{adapter.kind}")
        os.mkdir(base)
        p = adapter.seed(base)
        existing = p[f"{adapter.kind}.json"]
        absent = os.path.join(base, f"{adapter.kind}-absent.json")
        return p, existing, absent

    def _clone_inputs(self, p: dict[str, str], name: str) -> dict[str, str]:
        clone_dir = os.path.join(self.tmp.name, name)
        os.mkdir(clone_dir)
        cloned: dict[str, str] = {}
        for key, path in p.items():
            target = os.path.join(clone_dir, os.path.basename(path))
            shutil.copy2(path, target)
            cloned[key] = target
        return cloned

    def _assert_no_fragments(self, directory: str, adapter: _Adapter) -> None:
        leftovers = [name for name in os.listdir(directory)
                     if name.startswith(adapter.prefix)]
        self.assertEqual(leftovers, [])


class CommitFaultTest(_CommitFaultBase):
    def test_temporary_write_failure_preserves_overwrite(self) -> None:
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                p, ledger, _absent = self._pristine(adapter)
                before = Path(ledger).read_bytes()
                real_fdopen = os.fdopen
                injected = {"done": False}

                class FailingWriter:
                    def __init__(self, handle):
                        self._handle = handle

                    def __enter__(self):
                        return self

                    def __exit__(self, *exc):
                        self._handle.close()
                        return False

                    def write(self, data):
                        raise OSError("injected temporary write failure")

                def flaky_fdopen(fd, mode="r", *args, **kwargs):
                    handle = real_fdopen(fd, mode, *args, **kwargs)
                    if not injected["done"] and "w" in mode:
                        injected["done"] = True
                        return FailingWriter(handle)
                    return handle

                with mock.patch("os.fdopen", flaky_fdopen):
                    with self.assertRaises(OSError):
                        adapter.first_call(p, ledger)
                self.assertTrue(injected["done"])
                self.assertEqual(Path(ledger).read_bytes(), before)
                self._assert_no_fragments(os.path.dirname(ledger), adapter)

    def test_file_fsync_failure_preserves_overwrite(self) -> None:
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                p, ledger, _absent = self._pristine(adapter)
                before = Path(ledger).read_bytes()
                real_fsync = os.fsync
                injected = {"done": False}

                def flaky_fsync(fd):
                    if not injected["done"]:
                        injected["done"] = True
                        raise OSError("injected file fsync failure")
                    return real_fsync(fd)

                with mock.patch("os.fsync", flaky_fsync):
                    with self.assertRaises(OSError):
                        adapter.first_call(p, ledger)
                self.assertTrue(injected["done"])
                self.assertEqual(Path(ledger).read_bytes(), before)
                self._assert_no_fragments(os.path.dirname(ledger), adapter)

    def test_replace_failure_preserves_overwrite(self) -> None:
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                p, ledger, _absent = self._pristine(adapter)
                before = Path(ledger).read_bytes()
                real_replace = os.replace
                injected = {"done": False}

                def flaky_replace(src, dst):
                    if not injected["done"]:
                        injected["done"] = True
                        raise OSError("injected replace failure")
                    return real_replace(src, dst)

                with mock.patch("os.replace", flaky_replace):
                    with self.assertRaises(OSError):
                        adapter.first_call(p, ledger)
                self.assertTrue(injected["done"])
                self.assertEqual(Path(ledger).read_bytes(), before)
                self._assert_no_fragments(os.path.dirname(ledger), adapter)

    def test_directory_sync_failure_rolls_back_overwrite(self) -> None:
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                p, ledger, _absent = self._pristine(adapter)
                before = Path(ledger).read_bytes()
                real_dirsync = adapter.module._fsync_directory
                injected = {"done": False}

                def flaky_dirsync(directory):
                    if not injected["done"]:
                        injected["done"] = True
                        raise OSError("injected directory fsync failure")
                    return real_dirsync(directory)

                with mock.patch.object(adapter.module, "_fsync_directory",
                                       flaky_dirsync):
                    with self.assertRaises(OSError):
                        adapter.first_call(p, ledger)
                self.assertTrue(injected["done"])
                self.assertEqual(Path(ledger).read_bytes(), before)
                self._assert_no_fragments(os.path.dirname(ledger), adapter)

    def test_temporary_write_failure_keeps_ledger_absent(self) -> None:
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                p, _existing, ledger = self._pristine(adapter)
                self.assertFalse(os.path.exists(ledger))
                real_fdopen = os.fdopen
                injected = {"done": False}

                class FailingWriter:
                    def __init__(self, handle):
                        self._handle = handle

                    def __enter__(self):
                        return self

                    def __exit__(self, *exc):
                        self._handle.close()
                        return False

                    def write(self, data):
                        raise OSError("injected temporary write failure")

                def flaky_fdopen(fd, mode="r", *args, **kwargs):
                    handle = real_fdopen(fd, mode, *args, **kwargs)
                    if not injected["done"] and "w" in mode:
                        injected["done"] = True
                        return FailingWriter(handle)
                    return handle

                with mock.patch("os.fdopen", flaky_fdopen):
                    with self.assertRaises(OSError):
                        adapter.create_call(p, ledger)
                self.assertTrue(injected["done"])
                self.assertFalse(os.path.exists(ledger))
                self._assert_no_fragments(os.path.dirname(ledger), adapter)

    def test_directory_sync_failure_rolls_new_ledger_to_absent(self) -> None:
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                p, _existing, ledger = self._pristine(adapter)
                real_dirsync = adapter.module._fsync_directory
                injected = {"done": False}

                def flaky_dirsync(directory):
                    if not injected["done"]:
                        injected["done"] = True
                        raise OSError("injected directory fsync failure")
                    return real_dirsync(directory)

                with mock.patch.object(adapter.module, "_fsync_directory",
                                       flaky_dirsync):
                    with self.assertRaises(OSError):
                        adapter.create_call(p, ledger)
                self.assertTrue(injected["done"])
                self.assertFalse(os.path.exists(ledger))
                self._assert_no_fragments(os.path.dirname(ledger), adapter)

    def test_rollback_failure_keeps_original_cause(self) -> None:
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                p, ledger, _absent = self._pristine(adapter)

                def always_dirsync(directory):
                    raise OSError("injected unrecoverable directory "
                                  "sync failure")

                # The post-replace directory sync raises first; the
                # restore's own directory sync raises again. The surfaced
                # error must still chain after the original.
                with mock.patch.object(adapter.module, "_fsync_directory",
                                       always_dirsync):
                    try:
                        adapter.first_call(p, ledger)
                    except OSError as exc:
                        self.assertIsInstance(exc.__cause__, OSError)
                        self.assertIsNotNone(exc.__cause__)
                    else:
                        self.fail("expected OSError")

    def test_retry_after_temporary_failure_creates_once(self) -> None:
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                p, ledger, _absent = self._pristine(adapter)
                before = Path(ledger).read_bytes()
                real_fdopen = os.fdopen
                state = {"fail": True}

                class FailingWriter:
                    def __init__(self, handle):
                        self._handle = handle

                    def __enter__(self):
                        return self

                    def __exit__(self, *exc):
                        self._handle.close()
                        return False

                    def write(self, data):
                        raise OSError("injected one-shot write failure")

                def flaky_fdopen(fd, mode="r", *args, **kwargs):
                    handle = real_fdopen(fd, mode, *args, **kwargs)
                    if state["fail"] and "w" in mode:
                        state["fail"] = False
                        return FailingWriter(handle)
                    return handle

                with mock.patch("os.fdopen", flaky_fdopen):
                    with self.assertRaises(OSError):
                        adapter.first_call(p, ledger)
                self.assertEqual(Path(ledger).read_bytes(), before)

                created, was_created = adapter.first_call(p, ledger)
                self.assertTrue(was_created)
                raw = Path(ledger).read_bytes()
                replayed, replayed_created = adapter.first_call(p, ledger)
                self.assertFalse(replayed_created)
                self.assertEqual(replayed, created)
                self.assertEqual(Path(ledger).read_bytes(), raw)


# ---------------------------------------------------------------------
# Thread and process races
# ---------------------------------------------------------------------

class _RaceBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _pristine(self, adapter: _Adapter) -> tuple[dict[str, str], str, str]:
        base = os.path.join(self.tmp.name, f"race-{adapter.kind}")
        os.mkdir(base)
        p = adapter.seed(base)
        existing = p[f"{adapter.kind}.json"]
        absent = os.path.join(base, f"{adapter.kind}-race.json")
        return p, existing, absent

    def _clone(self, p: dict[str, str], name: str) -> dict[str, str]:
        clone_dir = os.path.join(self.tmp.name, name)
        os.mkdir(clone_dir)
        cloned: dict[str, str] = {}
        for key, path in p.items():
            # The overwrite fault seeds need not create every later
            # snapshot; the compared call only touches its own ledger.
            if not os.path.exists(path):
                continue
            target = os.path.join(clone_dir, os.path.basename(path))
            shutil.copy2(path, target)
            cloned[key] = target
        return cloned

    def _run_threads(self, calls) -> list[object]:
        barrier = threading.Barrier(len(calls))
        outcomes: list[object] = []
        outcomes_lock = threading.Lock()

        def runner(call):
            barrier.wait(_WAIT)
            try:
                result = call()
            except BaseException as exc:
                result = exc
            with outcomes_lock:
                outcomes.append(result)

        threads = [threading.Thread(target=runner, args=(call,), daemon=True)
                   for call in calls]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(_WAIT)
            self.assertFalse(thread.is_alive(), "race thread deadlocked")
        return outcomes


class ThreadRaceTest(_RaceBase):
    def test_duplicate_first_serve_collapses_to_one_create(self) -> None:
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                p, ledger, _absent = self._pristine(adapter)
                clone = self._clone(p, f"seq-{adapter.kind}")
                clone_ledger = clone[f"{adapter.kind}.json"]

                outcomes = self._run_threads([
                    lambda: adapter.first_call(p, ledger),
                    lambda: adapter.first_call(p, ledger),
                ])
                errors = [item for item in outcomes
                          if isinstance(item, BaseException)]
                self.assertEqual(errors, [])
                created_flags = sorted(flag for _record, flag in outcomes)
                self.assertEqual(created_flags, [False, True])
                self.assertEqual(outcomes[0][0], outcomes[1][0])

                # The sequential version produces exactly these bytes.
                sequential, was_created = adapter.first_call(
                    clone, clone_ledger)
                self.assertTrue(was_created)
                self.assertEqual(Path(ledger).read_bytes(),
                                 Path(clone_ledger).read_bytes())
                self.assertEqual(outcomes[0][0], sequential)

    def test_same_key_different_request_leaves_winner_intact(self) -> None:
        # Bind the key first, then race an exact replay (no write)
        # against the same key carrying a different request. The clash
        # raises ValueError and the committed ledger stays byte-for-byte
        # the already-bound version; scheduling cannot pick a winner.
        for adapter in _ADAPTERS:
            with self.subTest(kind=adapter.kind):
                if adapter.kind == "dispatch":
                    def winning(p, ledger):
                        return dispatch_module.claim(
                            ledger, "j-é", "rk-clash", "owner-0", 30, 60)

                    def clashing(p, ledger):
                        return dispatch_module.claim(
                            ledger, "j-é", "rk-clash", "owner-9", 30, 60)
                elif adapter.kind == "execution":
                    def winning(p, ledger):
                        return execution_module.record(
                            ledger, "j-é", 1, "rk-clash", "owner-0",
                            "stage", "succeeded", "staged", 62)

                    def clashing(p, ledger):
                        return execution_module.record(
                            ledger, "j-é", 1, "rk-clash", "owner-0",
                            "stage", "succeeded", "different", 62)
                else:
                    def winning(p, ledger):
                        return completion_module.complete(
                            p["jobs.json"], p["supply.json"],
                            p["signals.json"], p["clear.json"],
                            p["dispatch.json"], p["execution.json"], ledger,
                            "j-b", "rk-clash", 70, "succeeded", 1, 1)

                    def clashing(p, ledger):
                        return completion_module.complete(
                            p["jobs.json"], p["supply.json"],
                            p["signals.json"], p["clear.json"],
                            p["dispatch.json"], p["execution.json"], ledger,
                            "j-b", "rk-clash", 70, "succeeded", 2, 1)

                p, ledger, _absent = self._pristine(adapter)
                bound_record, was_created = winning(p, ledger)
                self.assertTrue(was_created)
                bound_bytes = Path(ledger).read_bytes()

                outcomes = self._run_threads([
                    lambda: winning(p, ledger),
                    lambda: clashing(p, ledger),
                ])
                value_errors = [item for item in outcomes
                                if isinstance(item, ValueError)]
                self.assertEqual(len(value_errors), 1)
                replays = [item for item in outcomes
                           if not isinstance(item, BaseException)]
                self.assertEqual(len(replays), 1)
                replayed_record, replayed_created = replays[0]
                self.assertFalse(replayed_created)
                self.assertEqual(replayed_record, bound_record)
                self.assertEqual(Path(ledger).read_bytes(), bound_bytes)


class ProcessRaceTest(_RaceBase):
    def _seed_race_state(self) -> str:
        base = self.tmp.name
        p = fx.seed_inputs(base, "j-é", "j-a", "j-b", "j-d")
        for index, job_id in enumerate(("j-é", "j-a", "j-b", "j-d")):
            fx.trade(p, job_id, f"t-{index}")
            fx.commit(p, job_id, f"d-{index}")
        # j-a waits at ready (race target = its commit).
        # j-b is claimed (race target = its plan).
        fx.claim(p, "j-b", "c-b", "owner-b")
        # j-d reaches a stable terminal state (race target = completion).
        fx.claim(p, "j-d", "c-d", "owner-d")
        fx.plan(p, "j-d", "p-d", "owner-d")
        fx.steps(p, "j-d", "x-d", "owner-d")
        fx.synchronize(p, "b-d")
        return base

    def _run_pair(self, module: str) -> list[bool]:
        env = dict(os.environ)
        repo = str(_REPO)
        env["PYTHONPATH"] = repo + os.pathsep + env.get("PYTHONPATH", "")
        procs = [
            subprocess.Popen(
                [sys.executable, "-m", "tests._lifecycle_worker",
                 module, self.tmp.name],
                cwd=repo, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            for _ in range(2)
        ]
        flags: list[bool] = []
        for proc in procs:
            stdout, stderr = proc.communicate(timeout=_WAIT)
            self.assertEqual(proc.returncode, 0,
                             stderr.decode("utf-8", "replace"))
            flags.append(json.loads(stdout.decode("utf-8")))
        return flags

    def test_duplicate_first_serve_across_processes(self) -> None:
        self._seed_race_state()
        for module, ledger_name in (
                ("dispatch", "race-dispatch.json"),
                ("execution", "race-execution.json"),
                ("completion", "race-completion.json")):
            with self.subTest(module=module):
                flags = self._run_pair(module)
                self.assertEqual(sorted(flags), [False, True])
                ledger = os.path.join(self.tmp.name, ledger_name)
                data = json.loads(Path(ledger).read_text("utf-8"))
                section = {"dispatch": "idempotency",
                           "execution": "idempotency",
                           "completion": "idempotency"}[module]
                self.assertEqual(len(data[section]), 1)
                self.assertTrue(Path(ledger).read_bytes().endswith(b"\n"))


# ---------------------------------------------------------------------
# Shared lock registry and ordering
# ---------------------------------------------------------------------

class LockSemanticsTest(unittest.TestCase):
    def test_equivalent_real_paths_share_one_store_across_modules(self) -> None:
        with TemporaryDirectory() as base:
            ledger = os.path.join(base, "dispatch.json")
            alt = os.path.join(base, ".", "dispatch.json")
            stores = [
                dispatch_module._get_store(ledger),
                execution_module._get_store(alt),
                completion_module._get_store(ledger),
                _lifecycle.get_store(ledger),
            ]
            for store in stores[1:]:
                self.assertIs(store, stores[0])
            self.assertEqual(stores[0].realpath, os.path.realpath(ledger))

    def test_lifecycle_locks_take_sorted_order_with_one_exclusive(self) -> None:
        order: list[tuple[str, bool]] = []

        @contextlib.contextmanager
        def recording_lock(realpath: str, *, shared: bool = False):
            order.append((realpath, shared))
            yield

        reals = ("/tmp/c-ledger", "/tmp/a-input", "/tmp/b-input")
        with _lifecycle.lifecycle_locks(
                reals, exclusive="/tmp/c-ledger", lock=recording_lock):
            self.assertEqual(
                [real for real, _shared in order],
                ["/tmp/a-input", "/tmp/b-input", "/tmp/c-ledger"])
        self.assertEqual(
            [shared for _real, shared in order],
            [True, True, False])

    def test_lifecycle_locks_deduplicates_equivalent_paths(self) -> None:
        order: list[str] = []

        @contextlib.contextmanager
        def recording_lock(realpath: str, *, shared: bool = False):
            order.append(realpath)
            yield

        with _lifecycle.lifecycle_locks(
                ("/tmp/a", "/tmp/a", "/tmp/b"), exclusive="/tmp/b",
                lock=recording_lock):
            self.assertEqual(order, ["/tmp/a", "/tmp/b"])


if __name__ == "__main__":
    unittest.main()
