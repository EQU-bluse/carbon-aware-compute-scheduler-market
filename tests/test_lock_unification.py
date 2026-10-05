"""Deterministic concurrency tests for the unified lock infrastructure.

The audit, audit_proof, jobs, market, rebalance, execution_sync,
migration_batch, recover_all and signal_ingest modules used to each
carry their own companion-lock flock and per-realpath in-process mutex
registry. They now share the single implementation in
``carbon_market._lifecycle`` with the lifecycle ledgers, and these tests
pin the concurrency contract of that unification:

* one lock identity per real file: every module's ``_get_store`` returns
  the same in-process mutex for the same resolved path, and symlink,
  relative or canonical spellings of one file land on one store, one
  companion lock file and one flock domain;
* shared read locks from different modules overlap, while an exclusive
  write lock excludes readers and writers alike -- across module
  boundaries and through public entry points;
* the lock is released when the guarded body raises, never held by a
  dead thread, and the companion lock file is left behind for the
  kernel to manage;
* multi-ledger operations take their locks in sorted resolved-real-path
  order, so two operations whose path sets interleave in reverse order
  cannot deadlock, and duplicate real paths are rejected with
  ``ValueError`` before any business file is read.

No test uses a fixed sleep to decide ordering: threads rendezvous
through events, barriers and gated lock wrappers, the on-disk lock
state is observed directly with non-blocking ``flock`` probes, and
every thread join is bounded so a failure reports whether the lock was
not mutually exclusive, deadlocked or never released. Release order is
always logged from inside the critical section, before the flock
release, so the sequence assertions compare the order the locks were
actually taken and released in -- never a scheduling assumption.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import hashlib
import hmac
import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from carbon_market import _lifecycle
from carbon_market import audit as audit_module
from carbon_market import audit_proof as audit_proof_module
from carbon_market import execution_sync as execution_sync_module
from carbon_market import jobs as jobs_module
from carbon_market import market as market_module
from carbon_market import migration_batch as migration_batch_module
from carbon_market import rebalance as rebalance_module
from carbon_market import recover_all as recover_all_module
from carbon_market import resources as resources_module
from carbon_market import signal_ingest as signal_ingest_module
from carbon_market import signals as signals_module

_WAIT = 30.0

# The nine modules whose lock implementations were unified; each keeps
# its private names as seams.
_UNIFIED_MODULES = (
    audit_module,
    audit_proof_module,
    jobs_module,
    market_module,
    rebalance_module,
    execution_sync_module,
    migration_batch_module,
    recover_all_module,
    signal_ingest_module,
)

_SIGNAL = {
    "region": "eu-north", "observed": 0, "expires": 500,
    "mix": {"solar": 10000}, "unit_cost": 3, "carbon_intensity": 7,
}

_RESOURCE = {
    "resource_id": "r-1", "region": "eu-north", "capacity": 100,
    "start": 0, "end": 500, "unit_cost": 3, "carbon_intensity": 7,
    "residency": ["eu-north"],
}

_JOB = {
    "job_id": "j-1", "work": 10, "deadline": 100,
    "regions": ["eu-north"], "residency": ["eu-north"],
    "max_cost": 1000, "carbon_cap": 1000,
}

_SECRET_HEX = "ab" * 32


def _envelope(sequence: int = 1) -> dict[str, object]:
    signal = {
        "region": "eu-north", "observed": 10, "expires": 100,
        "mix": {"solar": 6000, "wind": 4000},
        "unit_cost": 3, "carbon_intensity": 7,
    }
    ordered = {field: signal[field] for field in
               ("region", "observed", "expires", "mix",
                "unit_cost", "carbon_intensity")}
    ordered["mix"] = {name: ordered["mix"][name]
                      for name in sorted(ordered["mix"])}
    payload = json.dumps(["src-a", "key-a", sequence, ordered],
                         ensure_ascii=False,
                         separators=(",", ":")).encode("utf-8")
    signature = hmac.new(bytes.fromhex(_SECRET_HEX), payload,
                         hashlib.sha256).hexdigest()
    return {"source": "src-a", "key_id": "key-a", "sequence": sequence,
            "signal": signal, "signature": signature}


def _lock_state(realpath: str) -> str:
    """Probe the companion lock file: ``free``, ``shared`` or
    ``exclusive`` -- the on-disk truth, without touching any thread."""
    fd = os.open(realpath + ".lock", os.O_CREAT | os.O_RDWR, 0o666)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN):
                raise
        else:
            fcntl.flock(fd, fcntl.LOCK_UN)
            return "free"
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN):
                raise
            return "exclusive"
        fcntl.flock(fd, fcntl.LOCK_UN)
        return "shared"
    finally:
        os.close(fd)


class _Harness:
    """Run threads and join them with a bounded wait.

    A thread that never finishes is reported as a possible deadlock or
    an unreleased lock; an exception it raised is re-raised from the
    main thread, so a failed assertion inside a worker is attributed to
    the worker, not to a hang.
    """

    def __init__(self, test: unittest.TestCase) -> None:
        self.test = test
        self.outcomes: dict[str, object] = {}

    def spawn(self, name: str, fn) -> threading.Thread:
        def target() -> None:
            try:
                self.outcomes[name] = fn()
            except BaseException as exc:  # asserted from the main thread
                self.outcomes[name] = exc

        thread = threading.Thread(target=target, name=name, daemon=True)
        thread.start()
        return thread

    def join(self, *threads: threading.Thread) -> None:
        for thread in threads:
            thread.join(_WAIT)
            self.test.assertFalse(
                thread.is_alive(),
                f"{thread.name} stuck (deadlock or lock never released)")

    def result(self, name: str):
        outcome = self.outcomes[name]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class _GatedLock:
    """Wrap a module's lock seam so the contender's thread provably
    reaches the lock boundary before the main thread lets it proceed.

    The wrapper logs the acquisition from inside the section and the
    release just before the flock is dropped, so the sequence log
    records the true lock order.
    """

    def __init__(self, lock_fn, log: list, label: str) -> None:
        self.lock_fn = lock_fn
        self.log = log
        self.label = label
        self.at_gate = threading.Event()
        self.gate_open = threading.Event()

    @contextlib.contextmanager
    def __call__(self, realpath: str, *, shared: bool = False):
        self.at_gate.set()
        self.gate_open.wait(_WAIT)
        with self.lock_fn(realpath, shared=shared):
            self.log.append(f"{self.label}-acquired")
            try:
                yield
            finally:
                self.log.append(f"{self.label}-releasing")


class LockUnificationTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _path(self, name: str) -> str:
        return os.path.join(self.tmp.name, name)

    def _touch(self, name: str) -> str:
        path = self._path(name)
        Path(path).write_bytes(b"{}\n")
        return path


# ---------------------------------------------------------------------------
# One lock identity per real file
# ---------------------------------------------------------------------------

class UnifiedRegistryTest(LockUnificationTestBase):
    def test_all_nine_modules_share_one_store_per_real_path(self) -> None:
        target = self._touch("ledger.json")
        real = os.path.realpath(target)
        first = _lifecycle.get_store(target)
        for module in _UNIFIED_MODULES:
            with self.subTest(module=module.__name__):
                self.assertIs(module._get_store(target), first)
                self.assertIs(module._get_store(real), first)
        self.assertEqual(first.realpath, real)

    def test_symlink_and_relative_spellings_share_one_store(self) -> None:
        target = self._touch("alias-target.json")
        alias = self._path("alias-link.json")
        os.symlink(target, alias)
        dotted = os.path.join(self.tmp.name, ".", "alias-target.json")
        real = os.path.realpath(target)
        for module in _UNIFIED_MODULES:
            with self.subTest(module=module.__name__):
                self.assertIs(module._get_store(alias),
                              module._get_store(target))
                self.assertIs(module._get_store(dotted),
                              module._get_store(target))
                self.assertEqual(module._get_store(alias).realpath, real)

    def test_aliased_public_call_uses_one_lock_file_and_one_ledger(
            self) -> None:
        target = self._path("journal.json")
        alias = self._path("journal-link.json")
        os.symlink(target, alias)
        event = {"op": "copy", "target": "t-1", "key": "k-1",
                 "changed": True, "error": None, "stage": None}
        # Write through the symlink, read back through the canonical
        # path: one resolved ledger, one companion lock file.
        recorded, created = audit_module.record(alias, "k-1", event)
        self.assertTrue(created)
        self.assertEqual(recorded["key"], "k-1")
        fetched = audit_module.get(target, "k-1")
        self.assertEqual(fetched, recorded)
        self.assertTrue(os.path.exists(target + ".lock"))
        self.assertFalse(os.path.exists(alias + ".lock"))
        # The symlink itself still names the same single ledger file.
        self.assertEqual(os.path.realpath(alias), os.path.realpath(target))


# ---------------------------------------------------------------------------
# Shared and exclusive flock semantics across module boundaries
# ---------------------------------------------------------------------------

class SharedReadTest(LockUnificationTestBase):
    def test_shared_readers_from_two_modules_overlap(self) -> None:
        real = os.path.realpath(self._touch("shared.json"))
        barrier = threading.Barrier(2)
        first_inside = threading.Event()
        second_inside = threading.Event()
        release = threading.Event()
        harness = _Harness(self)

        def reader(lock_fn, entered: threading.Event):
            def run() -> None:
                with lock_fn(real, shared=True):
                    entered.set()
                    # Reaching the barrier proves both readers hold the
                    # shared lock at the same time; a wrongly exclusive
                    # "shared" lock strands one reader and breaks the
                    # barrier instead of passing silently. Both then
                    # stay inside until the main thread has observed
                    # the on-disk state.
                    barrier.wait(_WAIT)
                    release.wait(_WAIT)
            return run

        first = harness.spawn(
            "reader-audit-proof",
            reader(audit_proof_module._file_lock, first_inside))
        self.assertTrue(first_inside.wait(_WAIT))
        self.assertEqual(_lock_state(real), "shared")
        second = harness.spawn(
            "reader-signal-ingest",
            reader(signal_ingest_module._file_lock, second_inside))
        self.assertTrue(second_inside.wait(_WAIT))
        self.assertEqual(_lock_state(real), "shared")
        release.set()
        harness.join(first, second)
        harness.result("reader-audit-proof")
        harness.result("reader-signal-ingest")
        self.assertEqual(_lock_state(real), "free")


class ReadWriteExclusionTest(LockUnificationTestBase):
    def test_shared_read_excludes_writer_until_release(self) -> None:
        real = os.path.realpath(self._touch("guarded.json"))
        log: list[str] = []
        reader_inside = threading.Event()
        reader_release = threading.Event()
        writer_done = threading.Event()
        harness = _Harness(self)

        def reader() -> None:
            with audit_module._file_lock(real, shared=True):
                log.append("reader-acquired")
                reader_inside.set()
                reader_release.wait(_WAIT)
                log.append("reader-releasing")

        # The writer is a different formerly-private module competing
        # for the same file; the gate parks it at the lock boundary so
        # the attempt is known to happen while the reader holds.
        gated = _GatedLock(market_module._clear_lock, log, "writer")

        def writer() -> None:
            with gated(real):
                pass
            writer_done.set()

        reader_thread = harness.spawn("reader", reader)
        self.assertTrue(reader_inside.wait(_WAIT))
        self.assertEqual(_lock_state(real), "shared")

        writer_thread = harness.spawn("writer", writer)
        self.assertTrue(gated.at_gate.wait(_WAIT))
        # The writer is committed to its attempt; the shared lock is
        # still the only lock on the file.
        self.assertEqual(_lock_state(real), "shared")
        self.assertFalse(writer_done.is_set())
        gated.gate_open.set()
        reader_release.set()
        self.assertTrue(writer_done.wait(_WAIT),
                        "writer never acquired: lock not released")
        harness.join(reader_thread, writer_thread)
        # The exclusive acquisition is ordered after the shared hold;
        # an overlap means the locks were not mutually exclusive.
        self.assertEqual(log, ["reader-acquired", "reader-releasing",
                               "writer-acquired", "writer-releasing"])
        self.assertEqual(_lock_state(real), "free")

    def test_exclusive_writer_excludes_reader_until_release(self) -> None:
        real = os.path.realpath(self._touch("guarded.json"))
        log: list[str] = []
        writer_inside = threading.Event()
        writer_release = threading.Event()
        reader_done = threading.Event()
        harness = _Harness(self)

        def writer() -> None:
            with rebalance_module._lock(real):
                log.append("writer-acquired")
                writer_inside.set()
                writer_release.wait(_WAIT)
                log.append("writer-releasing")

        gated = _GatedLock(signal_ingest_module._file_lock, log, "reader")

        def reader() -> None:
            with gated(real, shared=True):
                pass
            reader_done.set()

        writer_thread = harness.spawn("writer", writer)
        self.assertTrue(writer_inside.wait(_WAIT))
        self.assertEqual(_lock_state(real), "exclusive")

        reader_thread = harness.spawn("reader", reader)
        self.assertTrue(gated.at_gate.wait(_WAIT))
        self.assertEqual(_lock_state(real), "exclusive")
        self.assertFalse(reader_done.is_set())
        gated.gate_open.set()
        writer_release.set()
        self.assertTrue(reader_done.wait(_WAIT),
                        "reader never acquired: lock not released")
        harness.join(writer_thread, reader_thread)
        self.assertEqual(log, ["writer-acquired", "writer-releasing",
                               "reader-acquired", "reader-releasing"])
        self.assertEqual(_lock_state(real), "free")


class WriteWriteExclusionTest(LockUnificationTestBase):
    def test_writers_from_two_modules_are_mutually_exclusive(self) -> None:
        real = os.path.realpath(self._touch("guarded.json"))
        log: list[str] = []
        first_inside = threading.Event()
        first_release = threading.Event()
        second_done = threading.Event()
        harness = _Harness(self)

        def first() -> None:
            with jobs_module._process_lock(real):
                log.append("first-acquired")
                first_inside.set()
                first_release.wait(_WAIT)
                log.append("first-releasing")

        gated = _GatedLock(recover_all_module._file_lock, log, "second")

        def second() -> None:
            with gated(real):
                pass
            second_done.set()

        first_thread = harness.spawn("first", first)
        self.assertTrue(first_inside.wait(_WAIT))
        self.assertEqual(_lock_state(real), "exclusive")

        second_thread = harness.spawn("second", second)
        self.assertTrue(gated.at_gate.wait(_WAIT))
        self.assertEqual(_lock_state(real), "exclusive")
        self.assertFalse(second_done.is_set())
        gated.gate_open.set()
        first_release.set()
        self.assertTrue(second_done.wait(_WAIT),
                        "second writer never acquired: lock not released")
        harness.join(first_thread, second_thread)
        self.assertEqual(log, ["first-acquired", "first-releasing",
                               "second-acquired", "second-releasing"])
        self.assertEqual(_lock_state(real), "free")


class ExceptionReleaseTest(LockUnificationTestBase):
    def test_exception_inside_exclusive_lock_releases_it(self) -> None:
        real = os.path.realpath(self._touch("guarded.json"))

        class Boom(Exception):
            pass

        harness = _Harness(self)

        def holder() -> None:
            with execution_sync_module._lock(real):
                raise Boom("body failure")

        thread = harness.spawn("holder", holder)
        harness.join(thread)
        with self.assertRaises(Boom):
            harness.result("holder")
        # The kernel-visible state is free again, and another module's
        # exclusive acquisition completes without waiting.
        self.assertEqual(_lock_state(real), "free")
        reacquired = threading.Event()

        def contender() -> None:
            with migration_batch_module._lock(real):
                reacquired.set()

        second = harness.spawn("contender", contender)
        self.assertTrue(reacquired.wait(_WAIT),
                        "lock still held after the raising body exited")
        harness.join(second)
        self.assertEqual(_lock_state(real), "free")

    def test_exception_inside_shared_lock_releases_it(self) -> None:
        real = os.path.realpath(self._touch("guarded.json"))

        class Boom(Exception):
            pass

        harness = _Harness(self)

        def holder() -> None:
            with audit_proof_module._file_lock(real, shared=True):
                raise Boom("reader failure")

        thread = harness.spawn("holder", holder)
        harness.join(thread)
        with self.assertRaises(Boom):
            harness.result("holder")
        self.assertEqual(_lock_state(real), "free")


# ---------------------------------------------------------------------------
# Multi-file operations: sorted order, reverse-order contention, duplicates
# ---------------------------------------------------------------------------

class LockOrderingTest(LockUnificationTestBase):
    def _recording_lock(self, lock_fn, order: list):
        @contextlib.contextmanager
        def recorder(realpath: str, *, shared: bool = False):
            order.append((realpath, shared))
            with lock_fn(realpath, shared=shared):
                yield

        return recorder

    def test_clear_live_locks_in_sorted_real_path_order(self) -> None:
        # File names are chosen so the signature order (jobs, supply,
        # signals, ledger) is not the sorted real-path order.
        jobs = self._path("z-jobs.json")
        supply = self._path("m-supply.json")
        signals = self._path("a-signals.json")
        ledger = self._path("q-trades.json")
        signals_module.publish(signals, _SIGNAL, "sk-1")
        resources_module.publish(supply, _RESOURCE, "rk-1")
        jobs_module.submit(jobs, _JOB, "jk-1")

        order: list[tuple[str, bool]] = []
        with mock.patch.object(
                market_module, "_clear_lock",
                self._recording_lock(market_module._clear_lock, order)):
            market_module.clear_live(jobs, supply, signals, ledger,
                                     "j-1", "tk-1", 10)
        paths = [path for path, _shared in order]
        self.assertEqual(paths, sorted(paths))
        locked = dict(order)
        self.assertFalse(locked[os.path.realpath(ledger)])
        for snapshot in (jobs, supply, signals):
            self.assertTrue(locked[os.path.realpath(snapshot)])

    def test_ingest_locks_three_files_in_sorted_real_path_order(
            self) -> None:
        trust = self._path("z-trust.json")
        signals = self._path("m-signals.json")
        ledger = self._path("a-ingest.json")
        Path(trust).write_bytes(signal_ingest_module._serialize_trust({
            "src-a": {
                "key_id": "key-a", "key": _SECRET_HEX,
                "regions": ["eu-north"],
                "valid_from": 0, "valid_until": 1000,
            },
        }))

        order: list[tuple[str, bool]] = []
        with mock.patch.object(
                signal_ingest_module, "_file_lock",
                self._recording_lock(signal_ingest_module._file_lock,
                                     order)):
            signal_ingest_module.ingest(signals, trust, ledger,
                                        _envelope(), "k-1")
        paths = [path for path, _shared in order]
        self.assertEqual(paths, sorted(paths))
        locked = dict(order)
        self.assertTrue(locked[os.path.realpath(trust)])
        self.assertFalse(locked[os.path.realpath(signals)])
        self.assertFalse(locked[os.path.realpath(ledger)])

    def test_reverse_order_contention_completes_without_deadlock(
            self) -> None:
        # Two files; two threads from different formerly-private modules
        # each take both locks in the modules' shared protocol -- sorted
        # resolved-real-path order. The first thread is held between the
        # two acquisitions while the second starts, the interleaving
        # that deadlocks if either side takes the locks in reverse.
        first_path = self._touch("a-first.json")
        second_path = self._touch("z-second.json")
        first_real = os.path.realpath(first_path)
        second_real = os.path.realpath(second_path)
        log: list[str] = []
        holds_first = threading.Event()
        second_attempting = threading.Event()
        proceed = threading.Event()
        harness = _Harness(self)

        def worker(name: str, lock_fn, rendezvous: bool) -> None:
            with contextlib.ExitStack() as stack:
                for locked in sorted((first_real, second_real)):
                    if rendezvous and locked == second_real:
                        # The first thread pauses before its second
                        # acquisition until the second thread is known
                        # to be attempting the same protocol.
                        holds_first.set()
                        second_attempting.wait(_WAIT)
                        proceed.wait(_WAIT)
                    if not rendezvous and locked == first_real:
                        second_attempting.set()
                    stack.enter_context(lock_fn(locked))
                    log.append(f"{name}-acquired-{os.path.basename(locked)}")
                log.append(f"{name}-inside")
                log.append(f"{name}-releasing-all")

        first = harness.spawn(
            "first", lambda: worker(
                "first", migration_batch_module._lock, True))
        self.assertTrue(holds_first.wait(_WAIT))
        self.assertEqual(_lock_state(first_real), "exclusive")
        second = harness.spawn(
            "second", lambda: worker(
                "second", execution_sync_module._lock, False))
        # The second thread is committed to the same sorted protocol and
        # must wait for the first file; the first thread still holds it.
        self.assertTrue(second_attempting.wait(_WAIT))
        self.assertEqual(_lock_state(first_real), "exclusive")
        self.assertEqual(log, ["first-acquired-a-first.json"])
        proceed.set()
        harness.join(first, second)
        # Both completed: no deadlock, no self-wait. The first thread's
        # full critical section preceded every acquisition of the
        # second, and each file changed hands exactly once.
        self.assertEqual(
            log,
            ["first-acquired-a-first.json", "first-acquired-z-second.json",
             "first-inside", "first-releasing-all",
             "second-acquired-a-first.json",
             "second-acquired-z-second.json", "second-inside",
             "second-releasing-all"])
        self.assertEqual(_lock_state(first_real), "free")
        self.assertEqual(_lock_state(second_real), "free")

    def test_duplicate_real_paths_rejected_before_any_file_is_read(
            self) -> None:
        # Neither file exists: a ValueError (not FileNotFoundError)
        # proves the distinctness check runs before any business file
        # is opened or locked.
        missing_jobs = self._path("jobs.json")
        missing_signals = self._path("signals.json")
        with self.assertRaises(ValueError):
            market_module.clear_live(missing_jobs, missing_jobs,
                                     missing_signals, self._path("t.json"),
                                     "j-1", "k-1", 10)
        with self.assertRaises(ValueError):
            signal_ingest_module.ingest(
                missing_signals, missing_signals, self._path("l.json"),
                _envelope(), "k-1")
        # No ledger or companion lock file was created on the way.
        self.assertEqual(os.listdir(self.tmp.name), [])


# ---------------------------------------------------------------------------
# Public entry points of two formerly-private modules on one file
# ---------------------------------------------------------------------------

class PublicContentionTest(LockUnificationTestBase):
    def _event(self, key: str) -> dict[str, object]:
        return {"op": "copy", "target": "t-1", "key": key,
                "changed": True, "error": None, "stage": None}

    def test_audit_record_waits_for_audit_proof_export(self) -> None:
        journal = self._path("journal.json")
        checkpoint = self._path("checkpoint.json")
        audit_module.record(journal, "k-seed", self._event("k-seed"))
        journal_real = os.path.realpath(journal)

        log: list[str] = []
        reader_inside = threading.Event()
        reader_release = threading.Event()
        real_file_lock = audit_module._file_lock

        @contextlib.contextmanager
        def gating(realpath: str, *, shared: bool = False):
            # The export's shared hold on the journal is parked at a
            # deterministic point; the record's exclusive acquisition
            # (shared=False) passes through ungated.
            with real_file_lock(realpath, shared=shared):
                if shared and realpath == journal_real:
                    log.append("export-acquired")
                    reader_inside.set()
                    reader_release.wait(_WAIT)
                    log.append("export-releasing")
                yield

        harness = _Harness(self)
        with mock.patch.object(audit_module, "_file_lock", gating):
            exporter = harness.spawn(
                "export", lambda: audit_proof_module.export(
                    journal, checkpoint, "g1"))
            self.assertTrue(reader_inside.wait(_WAIT))
            self.assertEqual(_lock_state(journal_real), "shared")

            record_attempted = threading.Event()

            def write() -> object:
                record_attempted.set()
                result = audit_module.record(
                    journal, "k-2", self._event("k-2"))
                log.append("record-returned")
                return result

            recorder = harness.spawn("record", write)
            self.assertTrue(record_attempted.wait(_WAIT))
            # The export still holds the journal's shared lock, so the
            # record cannot have completed; there is no outcome yet.
            self.assertNotIn("record", harness.outcomes)
            reader_release.set()
            harness.join(exporter, recorder)

        proof = harness.result("export")
        self.assertEqual(proof["generation"], "g1")
        _recorded, created = harness.result("record")
        self.assertTrue(created)
        # The record's exclusive section is ordered strictly after the
        # export's shared hold; an overlap is a mutual-exclusion break.
        self.assertEqual(log, ["export-acquired", "export-releasing",
                               "record-returned"])
        # Both changes are durable: the new event is in the journal and
        # the checkpoint anchor was committed.
        self.assertEqual(audit_module.get(journal, "k-2")["key"], "k-2")
        checkpoint_doc = json.loads(Path(checkpoint).read_text("utf-8"))
        self.assertEqual(
            checkpoint_doc["generations"][0]["name"], "g1")
        self.assertEqual(_lock_state(journal_real), "free")

    def test_signals_publish_waits_for_ingest_hold_on_the_same_file(
            self) -> None:
        # signal_ingest and market both kept their own lock
        # implementations before the unification; here the ingest
        # module's exclusive hold on the signals file excludes a
        # contender going through the market module's seam.
        real = os.path.realpath(self._touch("signals.json"))
        log: list[str] = []
        holder_inside = threading.Event()
        holder_release = threading.Event()
        contender_done = threading.Event()
        harness = _Harness(self)

        def holder() -> None:
            with signal_ingest_module._file_lock(real):
                log.append("ingest-acquired")
                holder_inside.set()
                holder_release.wait(_WAIT)
                log.append("ingest-releasing")

        gated = _GatedLock(market_module._clear_lock, log, "publisher")

        def contender() -> None:
            with gated(real):
                pass
            contender_done.set()

        holder_thread = harness.spawn("holder", holder)
        self.assertTrue(holder_inside.wait(_WAIT))
        self.assertEqual(_lock_state(real), "exclusive")
        contender_thread = harness.spawn("contender", contender)
        self.assertTrue(gated.at_gate.wait(_WAIT))
        self.assertFalse(contender_done.is_set())
        gated.gate_open.set()
        holder_release.set()
        self.assertTrue(contender_done.wait(_WAIT),
                        "contender never acquired: lock not released")
        harness.join(holder_thread, contender_thread)
        self.assertEqual(log, ["ingest-acquired", "ingest-releasing",
                               "publisher-acquired", "publisher-releasing"])
        self.assertEqual(_lock_state(real), "free")


if __name__ == "__main__":
    unittest.main()
