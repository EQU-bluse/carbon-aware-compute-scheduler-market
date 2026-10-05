"""Cross-process concurrency tests for the persistent market ledgers.

The sibling thread-based tests (``test_completion_concurrency.py`` and
``test_migration_consumer_concurrency.py``) prove the in-process mutex
registry; these tests prove the companion ``flock`` file locks carry
the same guarantees across operating-system processes. Two independent
interpreter processes -- started with the ``spawn`` method so no lock,
store registry or module state is inherited -- race the existing public
entry points ``cancellation.cancel``, ``dispatch.commit``,
``market.clear`` and ``market.clear_live`` over one shared set of
ledger files, staged at deterministic synchronization points (events,
barriers and lock-attempt probes installed inside each child) instead
of fixed sleeps.

Two race families are covered:

* the first cancellation of a traded job versus its first dispatch
  commit -- exactly one side commits with ``created`` true; a committed
  cancellation makes the commit fail with ``ValueError``, a committed
  dispatch decision makes the cancellation fail with
  ``PermissionError``, and the loser leaves no record, no temporary
  fragment and no damage to the winner's commit;
* the cancellation of a job that exactly fills a resource version
  versus the clearing of a new job against the same version -- a
  clearing that snapshots first is refused with ``LookupError`` and
  succeeds on retry once the cancellation lands, a clearing that
  snapshots after the cancellation books the released capacity
  directly, and the effective occupancy never exceeds the capacity of
  the resource version.

Every child reports its return value or exception type through a queue
and is joined with a bounded wait; a stuck child is terminated so a
deadlock surfaces as a loud test failure instead of a hang. All
verification goes through the public read/replay entries and the
on-disk bytes. No interface, ledger format, exception type or
sequential-call result is changed, and the existing thread-based tests
keep their assertions untouched.
"""

from __future__ import annotations

import contextlib
import multiprocessing as mp
import os
import queue as queue_module
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import cancellation as cancellation_module
from carbon_market import dispatch as dispatch_module
from carbon_market import jobs as jobs_module
from carbon_market import market as market_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.cancellation import cancel, get

_WAIT = 30.0  # Bounded wait (seconds): a deadlock fails, never hangs.


def _resource(resource_id: str = "r-1", region: str = "eu-north",
              **overrides: object) -> dict[str, object]:
    resource: dict[str, object] = {
        "resource_id": resource_id,
        "region": region,
        "capacity": 100,
        "start": 0,
        "end": 500,
        "unit_cost": 5,
        "carbon_intensity": 8,
        "residency": [region],
    }
    resource.update(overrides)
    return resource


def _signal(region: str = "eu-north", **overrides: object) -> dict[str, object]:
    signal: dict[str, object] = {
        "region": region,
        "observed": 0,
        "expires": 500,
        "mix": {"solar": 10000},
        "unit_cost": 5,
        "carbon_intensity": 8,
    }
    signal.update(overrides)
    return signal


def _job(job_id: str = "j-1", **overrides: object) -> dict[str, object]:
    job: dict[str, object] = {
        "job_id": job_id,
        "work": 10,
        "deadline": 100,
        "regions": ["eu-north"],
        "residency": ["eu-north"],
        "max_cost": 1000,
        "carbon_cap": 1000,
    }
    job.update(overrides)
    return job


# -- child-process machinery -------------------------------------------------
#
# The workers below run inside the spawned child interpreters; they must
# stay module-level so the spawn pickler can resolve them by name. Each
# worker optionally installs a gate (holding the child's business call at
# a deterministic point while it keeps every lock it already holds) or a
# lock-attempt probe (only observing), then runs exactly one public entry
# point and reports the outcome as one small picklable dict.


def _child_report(queue, fn) -> None:
    """Run ``fn`` and report the outcome; the child exits 0 unless it
    crashes outside the business call."""
    try:
        record, created = fn()
    except BaseException as exc:
        queue.put({"status": "raised", "exc_type": type(exc).__name__,
                   "message": str(exc)})
    else:
        queue.put({"status": "returned", "record": record,
                   "created": created})


def _gate_commit_file(module, arrived, release) -> None:
    """Hold the module's atomic commit until the parent opens the gate:
    the child keeps every lock it already holds while the rival process
    is staged behind it."""
    real_commit = module._commit_file

    def gated_commit(realpath, payload, old_bytes):
        arrived.set()
        if not release.wait(_WAIT):
            raise RuntimeError("test gate was never released")
        return real_commit(realpath, payload, old_bytes)

    module._commit_file = gated_commit


def _gate_clear_snapshot(arrived, release) -> None:
    """Hold ``market.clear``/``clear_live`` just before the clearing
    ledger is loaded -- after the cancellation snapshot was read and
    while every lock is held -- until the parent opens the gate."""
    real_load = market_module._load_clear_ledger
    fired = {"done": False}

    def gated_load(*args, **kwargs):
        if not fired["done"]:
            fired["done"] = True
            arrived.set()
            if not release.wait(_WAIT):
                raise RuntimeError("test gate was never released")
        return real_load(*args, **kwargs)

    market_module._load_clear_ledger = gated_load


def _install_lock_probe(module, attribute, probe_realpath, probing) -> None:
    """Signal the parent when the child is about to block on
    ``probe_realpath``'s flock; the probe only observes and never
    changes the underlying lock behavior."""
    real_lock = getattr(module, attribute)

    @contextlib.contextmanager
    def probed(realpath, *, shared=False):
        if realpath == probe_realpath:
            probing.set()
        with real_lock(realpath, shared=shared):
            yield

    setattr(module, attribute, probed)


def _cancel_worker(paths, job_id, key, at, reason, barrier, gate, probe,
                   queue) -> None:
    if gate is not None:
        _gate_commit_file(cancellation_module, *gate)
    if probe is not None:
        _install_lock_probe(cancellation_module, "_lock", *probe)
    if barrier is not None:
        barrier.wait(_WAIT)
    _child_report(queue, lambda: cancel(*paths, job_id, key, at, reason))


def _commit_worker(paths, job_id, key, at, cancellations, barrier, gate,
                   probe, queue) -> None:
    if gate is not None:
        _gate_commit_file(dispatch_module, *gate)
    if probe is not None:
        _install_lock_probe(dispatch_module, "_lock", *probe)
    if barrier is not None:
        barrier.wait(_WAIT)
    _child_report(queue, lambda: dispatch_module.commit(
        *paths, job_id, key, at, cancellations))


def _clear_worker(live, paths, job_id, key, at, cancellations, barrier,
                  gate, probe, queue) -> None:
    if gate is not None:
        _gate_clear_snapshot(*gate)
    if probe is not None:
        _install_lock_probe(market_module, "_clear_lock", *probe)
    if barrier is not None:
        barrier.wait(_WAIT)
    if live:
        _child_report(queue, lambda: market_module.clear_live(
            *paths, job_id, key, at, cancellations=cancellations))
    else:
        _child_report(queue, lambda: market_module.clear(
            *paths, job_id, key, at, cancellations=cancellations))


class _ProcessRaceTestBase(unittest.TestCase):
    """Shared paths, spawn harness and report assertions."""

    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        base = self.tmp.name
        self.jobs = os.path.join(base, "jobs.json")
        self.supply = os.path.join(base, "supply.json")
        self.signals = os.path.join(base, "signals.json")
        self.trades = os.path.join(base, "trades.json")
        self.dispatch = os.path.join(base, "dispatch.json")
        self.cancellations = os.path.join(base, "cancellations.json")
        # Spawned children inherit neither the in-process store registry
        # nor any lock state: only the on-disk flock files serialize them.
        self._ctx = mp.get_context("spawn")
        self._children: list[tuple[mp.Process, object]] = []
        self.addCleanup(self._terminate_children)

    # -- process harness ---------------------------------------------------

    def _terminate_children(self) -> None:
        for proc, _queue in self._children:
            if proc.is_alive():
                proc.terminate()
                proc.join(_WAIT)

    def _event(self):
        return self._ctx.Event()

    def _spawn(self, target, args):
        queue = self._ctx.Queue()
        proc = self._ctx.Process(target=target, args=(*args, queue),
                                 daemon=True)
        proc.start()
        self._children.append((proc, queue))
        return proc, queue

    def _await_event(self, event, label: str) -> None:
        self.assertTrue(event.wait(_WAIT),
                        f"timed out waiting for {label}")

    def _finish(self, proc, queue, label: str):
        """Join one child with a bounded wait and collect its exit
        status and report; a stuck child is terminated so a deadlock
        fails loudly instead of hanging."""
        proc.join(_WAIT)
        if proc.is_alive():
            proc.terminate()
            proc.join(_WAIT)
            self.fail(f"{label} did not finish within {_WAIT} seconds "
                      "(possible deadlock)")
        self.assertEqual(proc.exitcode, 0,
                         f"{label} exited with status {proc.exitcode}")
        try:
            report = queue.get(timeout=_WAIT)
        except queue_module.Empty:
            self.fail(f"{label} left no report")
        queue.close()
        return report

    # -- report assertions ---------------------------------------------------

    def _assert_returned(self, report, label: str):
        self.assertEqual("returned", report["status"],
                         f"{label} raised {report.get('exc_type')}: "
                         f"{report.get('message')}")
        return report["record"], report["created"]

    def _assert_raised(self, report, exc_type: str, label: str) -> None:
        self.assertEqual("raised", report["status"],
                         f"{label} unexpectedly returned "
                         f"{report.get('record')!r}")
        self.assertEqual(exc_type, report["exc_type"],
                         f"{label}: {report['message']}")

    # -- shared on-disk assertions -------------------------------------------

    def _assert_no_fragments(self) -> None:
        # No half-written commit or rollback temporary survives any race.
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.startswith(".")]
        self.assertEqual([], leftovers)


class CancelCommitProcessRaceTest(_ProcessRaceTestBase):
    """First ``cancellation.cancel`` versus first ``dispatch.commit``
    for the same traded job, raced by two spawned interpreters."""

    def setUp(self) -> None:
        super().setUp()
        # A bootstrap job on its own resource is traded and cancelled
        # first, so a valid cancellation ledger already exists when the
        # race starts.
        jobs_module.submit(
            self.jobs,
            _job("j-0", regions=["zz-boot"], residency=["zz-boot"]),
            "jk-0")
        jobs_module.submit(self.jobs, _job("j-1"), "jk-1")
        resources_module.publish(
            self.supply,
            _resource("r-0", region="zz-boot", capacity=10,
                      residency=["zz-boot"]), "rk-0")
        resources_module.publish(self.supply, _resource("r-1"), "rk-1")
        trade, created = market_module.clear(
            self.jobs, self.supply, self.trades, "j-0", "t-0", 10)
        self.assertTrue(created)
        record, created = cancel(*self._cancel_paths(), "j-0", "c-0", 15,
                                 "bootstrap")
        self.assertTrue(created)
        # j-1 is traded on r-1 and is the job the two processes race over.
        trade, created = market_module.clear(
            self.jobs, self.supply, self.trades, "j-1", "t-1", 10)
        self.assertTrue(created)
        self.assertEqual(trade["selection"],
                         {"resource_id": "r-1", "version": 1})

    # -- call helpers --------------------------------------------------------

    def _cancel_paths(self) -> tuple[str, ...]:
        return (self.jobs, self.supply, self.trades, self.dispatch,
                self.cancellations)

    def _commit_paths(self) -> tuple[str, ...]:
        return (self.jobs, self.supply, self.trades, self.dispatch)

    def _cancel_j1(self):
        return cancel(*self._cancel_paths(), "j-1", "c-1", 20,
                      "no longer needed")

    def _commit_j1(self):
        return dispatch_module.commit(*self._commit_paths(), "j-1", "d-1",
                                      20, self.cancellations)

    def _spawn_cancel(self, barrier=None, gate=None, probe=None):
        return self._spawn(_cancel_worker,
                           (self._cancel_paths(), "j-1", "c-1", 20,
                            "no longer needed", barrier, gate, probe))

    def _spawn_commit(self, barrier=None, gate=None, probe=None):
        return self._spawn(_commit_worker,
                           (self._commit_paths(), "j-1", "d-1", 20,
                            self.cancellations, barrier, gate, probe))

    # -- races -----------------------------------------------------------------

    def test_cancel_commits_first(self) -> None:
        # The cancellation commits while the dispatch commit is queued
        # on the cancellation ledger's shared lock: the commit must
        # observe the complete post-commit snapshot and be refused.
        arrived, release, probing = (self._event() for _ in range(3))
        cancellation_real = os.path.realpath(self.cancellations)
        dispatch_before = Path(self.dispatch).exists()

        p_cancel, q_cancel = self._spawn_cancel(gate=(arrived, release))
        self._await_event(arrived, "cancel to reach its commit gate")
        p_commit, q_commit = self._spawn_commit(
            probe=(cancellation_real, probing))
        self._await_event(probing,
                          "commit to queue on the cancellation lock")
        release.set()
        cancel_report = self._finish(p_cancel, q_cancel, "cancel")
        commit_report = self._finish(p_commit, q_commit, "commit")

        record, created = self._assert_returned(cancel_report, "cancel")
        self.assertTrue(created)
        self.assertEqual(record, {"job_id": "j-1", "at": 20,
                                  "reason": "no longer needed",
                                  "resource_id": "r-1", "version": 1})
        # The loser was refused and left no record, no ledger and no
        # temporary fragment behind.
        self._assert_raised(commit_report, "ValueError", "commit")
        self.assertEqual(Path(self.dispatch).exists(), dispatch_before)
        self._assert_no_fragments()

        # The winner's record is readable through the public entry and
        # an equivalent replay returns it without rewriting a byte.
        self.assertEqual(get(self.cancellations, "j-1"), record)
        raw = Path(self.cancellations).read_bytes()
        replayed, created = self._cancel_j1()
        self.assertFalse(created)
        self.assertEqual(replayed, record)
        self.assertEqual(Path(self.cancellations).read_bytes(), raw)
        # The refused commit stays refused and still creates nothing.
        with self.assertRaises(ValueError):
            self._commit_j1()
        self.assertEqual(Path(self.dispatch).exists(), dispatch_before)
        # The raced-over trade is still replayable through its public
        # entry: the winner's commit was not corrupted.
        trade, created = market_module.clear(
            self.jobs, self.supply, self.trades, "j-1", "t-1", 10)
        self.assertFalse(created)
        self.assertEqual(trade["job_id"], "j-1")

    def test_commit_commits_first(self) -> None:
        # The dispatch commit lands while the cancellation is queued on
        # the cancellation ledger's exclusive lock: the cancellation
        # must observe the committed decision and be refused.
        arrived, release, probing = (self._event() for _ in range(3))
        cancellation_real = os.path.realpath(self.cancellations)
        cancellations_before = Path(self.cancellations).read_bytes()

        p_commit, q_commit = self._spawn_commit(gate=(arrived, release))
        self._await_event(arrived, "commit to reach its commit gate")
        p_cancel, q_cancel = self._spawn_cancel(
            probe=(cancellation_real, probing))
        self._await_event(probing,
                          "cancel to queue on the cancellation lock")
        release.set()
        commit_report = self._finish(p_commit, q_commit, "commit")
        cancel_report = self._finish(p_cancel, q_cancel, "cancel")

        decision, created = self._assert_returned(commit_report, "commit")
        self.assertTrue(created)
        self.assertEqual(decision["job_id"], "j-1")
        self.assertEqual(decision["state"], "ready")
        self.assertEqual(decision["attempts"], 0)
        # The loser was refused and left the pre-race cancellation
        # ledger byte-for-byte untouched.
        self._assert_raised(cancel_report, "PermissionError", "cancel")
        self.assertEqual(Path(self.cancellations).read_bytes(),
                         cancellations_before)
        self._assert_no_fragments()

        # An equivalent replay of the winner returns the stored decision
        # without rewriting a byte.
        raw = Path(self.dispatch).read_bytes()
        replayed, created = self._commit_j1()
        self.assertFalse(created)
        self.assertEqual(replayed, decision)
        self.assertEqual(Path(self.dispatch).read_bytes(), raw)
        # The refused cancellation stays refused and still writes
        # nothing; the bootstrap record remains publicly readable.
        with self.assertRaises(PermissionError):
            self._cancel_j1()
        self.assertEqual(Path(self.cancellations).read_bytes(),
                         cancellations_before)
        self.assertEqual(get(self.cancellations, "j-0")["job_id"], "j-0")
        with self.assertRaises(KeyError):
            get(self.cancellations, "j-1")

    def test_simultaneous_first_cancel_and_first_commit(self) -> None:
        # No staging: both processes are released at one barrier and the
        # file locks alone decide the order. Exactly one side commits.
        barrier = self._ctx.Barrier(2)
        p_cancel, q_cancel = self._spawn_cancel(barrier=barrier)
        p_commit, q_commit = self._spawn_commit(barrier=barrier)
        cancel_report = self._finish(p_cancel, q_cancel, "cancel")
        commit_report = self._finish(p_commit, q_commit, "commit")

        if cancel_report["status"] == "returned":
            # Cancel won: the commit must have been refused and only the
            # cancellation may exist for j-1.
            record, created = self._assert_returned(cancel_report,
                                                    "cancel")
            self.assertTrue(created)
            self._assert_raised(commit_report, "ValueError", "commit")
            self.assertFalse(Path(self.dispatch).exists())
            self.assertEqual(get(self.cancellations, "j-1"), record)
            replayed, created = self._cancel_j1()
            self.assertFalse(created)
            self.assertEqual(replayed, record)
        else:
            # Commit won: the cancellation must have been refused and
            # only the decision may exist for j-1.
            self._assert_raised(cancel_report, "PermissionError",
                                "cancel")
            decision, created = self._assert_returned(commit_report,
                                                      "commit")
            self.assertTrue(created)
            replayed, created = self._commit_j1()
            self.assertFalse(created)
            self.assertEqual(replayed, decision)
            with self.assertRaises(KeyError):
                get(self.cancellations, "j-1")
        self._assert_no_fragments()


class CancelClearProcessRaceTest(_ProcessRaceTestBase):
    """``cancellation.cancel`` of a job filling a resource version
    versus ``market.clear``/``market.clear_live`` of a new job wanting
    the same version, raced by two spawned interpreters."""

    def setUp(self) -> None:
        super().setUp()
        # A bootstrap job on its own resource is traded and cancelled
        # first, so a valid cancellation ledger already exists when the
        # race starts.
        jobs_module.submit(
            self.jobs,
            _job("j-0", regions=["zz-boot"], residency=["zz-boot"]),
            "jk-0")
        for index in (1, 2, 3):
            jobs_module.submit(self.jobs, _job(f"j-{index}"),
                               f"jk-{index}")
        resources_module.publish(
            self.supply,
            _resource("r-0", region="zz-boot", capacity=10,
                      residency=["zz-boot"]), "rk-0")
        resources_module.publish(
            self.supply, _resource("r-1", capacity=10), "rk-1")
        signals_module.publish(self.signals, _signal(), "sk-1")
        trade, created = market_module.clear(
            self.jobs, self.supply, self.trades, "j-0", "t-0", 10)
        self.assertTrue(created)
        record, created = cancel(*self._cancel_paths(), "j-0", "c-0", 15,
                                 "bootstrap")
        self.assertTrue(created)
        # j-1 exactly fills r-1's version 1 (capacity 10, work 10).
        trade, created = market_module.clear(
            self.jobs, self.supply, self.trades, "j-1", "t-1", 10)
        self.assertTrue(created)
        self.assertEqual(trade["selection"],
                         {"resource_id": "r-1", "version": 1})

    # -- call helpers --------------------------------------------------------

    def _cancel_paths(self) -> tuple[str, ...]:
        return (self.jobs, self.supply, self.trades, self.dispatch,
                self.cancellations)

    def _clear_paths(self, live: bool) -> tuple[str, ...]:
        if live:
            return (self.jobs, self.supply, self.signals, self.trades)
        return (self.jobs, self.supply, self.trades)

    def _cancel_j1(self):
        return cancel(*self._cancel_paths(), "j-1", "c-1", 20,
                      "no longer needed")

    def _clear_job(self, job_id: str, key: str, live: bool):
        if live:
            return market_module.clear_live(
                self.jobs, self.supply, self.signals, self.trades, job_id,
                key, 30, cancellations=self.cancellations)
        return market_module.clear(
            self.jobs, self.supply, self.trades, job_id, key, 30,
            cancellations=self.cancellations)

    def _spawn_cancel(self, barrier=None, gate=None, probe=None):
        return self._spawn(_cancel_worker,
                           (self._cancel_paths(), "j-1", "c-1", 20,
                            "no longer needed", barrier, gate, probe))

    def _spawn_clear(self, live: bool, barrier=None, gate=None,
                     probe=None):
        return self._spawn(_clear_worker,
                           (live, self._clear_paths(live), "j-2", "t-2",
                            30, self.cancellations, barrier, gate, probe))

    # -- shared post-race assertions -----------------------------------------

    def _assert_capacity_consistent(self, live: bool,
                                    record: dict[str, object],
                                    trade: dict[str, object]) -> None:
        """Whatever the interleaving, the release was spent exactly once
        and the version's effective occupancy never exceeds capacity."""
        # The cancellation is recorded once and readable through the
        # public entry; an equivalent replay rewrites nothing.
        self.assertEqual(get(self.cancellations, "j-1"), record)
        replayed, created = self._cancel_j1()
        self.assertFalse(created)
        self.assertEqual(replayed, record)
        # The winning clearing replays through its public entry without
        # rewriting a byte of the trades ledger.
        raw = Path(self.trades).read_bytes()
        replayed_trade, created = self._clear_job("j-2", "t-2", live)
        self.assertFalse(created)
        self.assertEqual(replayed_trade, trade)
        self.assertEqual(Path(self.trades).read_bytes(), raw)
        # r-1 version 1 has capacity 10: j-1's cancellation released its
        # 10 and j-2's trade spends exactly those 10, so one more job of
        # work 10 finds the version full -- the release was never spent
        # twice and the capacity was never oversold.
        with self.assertRaises(LookupError):
            self._clear_job("j-3", "t-3", live)

    # -- races -----------------------------------------------------------------

    def _run_clear_snapshots_first(self, live: bool) -> None:
        # The clearing reads its snapshot while the cancellation is
        # queued on the cancellation ledger's exclusive lock: it must
        # observe the complete pre-commit snapshot (the version is still
        # sold out) and be refused; the cancellation then commits and
        # exactly one retry books the freed capacity.
        arrived, release, probing = (self._event() for _ in range(3))
        cancellation_real = os.path.realpath(self.cancellations)
        trades_before = Path(self.trades).read_bytes()

        p_clear, q_clear = self._spawn_clear(live, gate=(arrived,
                                                         release))
        self._await_event(arrived, "clear to reach its snapshot gate")
        p_cancel, q_cancel = self._spawn_cancel(
            probe=(cancellation_real, probing))
        self._await_event(probing,
                          "cancel to queue on the cancellation lock")
        release.set()
        clear_report = self._finish(p_clear, q_clear, "clear")
        cancel_report = self._finish(p_cancel, q_cancel, "cancel")

        self._assert_raised(clear_report, "LookupError", "clear")
        record, created = self._assert_returned(cancel_report, "cancel")
        self.assertTrue(created)
        self.assertEqual(record["job_id"], "j-1")
        # The refused clearing wrote nothing: the trades ledger is
        # byte-for-byte the pre-race one, so the same idempotency key is
        # still free and books exactly once on retry.
        self.assertEqual(Path(self.trades).read_bytes(), trades_before)
        self._assert_no_fragments()
        trade, created = self._clear_job("j-2", "t-2", live)
        self.assertTrue(created)
        self.assertEqual(trade["selection"],
                         {"resource_id": "r-1", "version": 1})
        self._assert_capacity_consistent(live, record, trade)

    def _run_cancel_commits_first(self, live: bool) -> None:
        # The cancellation commits while the clearing is queued on the
        # cancellation ledger's shared lock: the clearing must observe
        # the complete post-commit snapshot and book the freed capacity
        # directly.
        arrived, release, probing = (self._event() for _ in range(3))
        cancellation_real = os.path.realpath(self.cancellations)

        p_cancel, q_cancel = self._spawn_cancel(gate=(arrived, release))
        self._await_event(arrived, "cancel to reach its commit gate")
        p_clear, q_clear = self._spawn_clear(
            live, probe=(cancellation_real, probing))
        self._await_event(probing,
                          "clear to queue on the cancellation lock")
        release.set()
        cancel_report = self._finish(p_cancel, q_cancel, "cancel")
        clear_report = self._finish(p_clear, q_clear, "clear")

        record, created = self._assert_returned(cancel_report, "cancel")
        self.assertTrue(created)
        trade, created = self._assert_returned(clear_report, "clear")
        self.assertTrue(created)
        self.assertEqual(trade["selection"],
                         {"resource_id": "r-1", "version": 1})
        self._assert_no_fragments()
        self._assert_capacity_consistent(live, record, trade)

    def test_clear_snapshots_first_then_retry_static(self) -> None:
        self._run_clear_snapshots_first(live=False)

    def test_clear_snapshots_first_then_retry_live(self) -> None:
        self._run_clear_snapshots_first(live=True)

    def test_cancel_commits_first_static(self) -> None:
        self._run_cancel_commits_first(live=False)

    def test_cancel_commits_first_live(self) -> None:
        self._run_cancel_commits_first(live=True)

    def test_simultaneous_cancel_and_clear(self) -> None:
        # No staging: both processes are released at one barrier and the
        # file locks alone decide the order. The cancellation always
        # commits; the clearing either books the released capacity
        # directly or is refused and books it on exactly one retry.
        barrier = self._ctx.Barrier(2)
        p_cancel, q_cancel = self._spawn_cancel(barrier=barrier)
        p_clear, q_clear = self._spawn_clear(False, barrier=barrier)
        cancel_report = self._finish(p_cancel, q_cancel, "cancel")
        clear_report = self._finish(p_clear, q_clear, "clear")

        record, created = self._assert_returned(cancel_report, "cancel")
        self.assertTrue(created)
        if clear_report["status"] == "raised":
            # The clearing snapshotted first and was refused; the retry
            # after the cancellation must succeed exactly once.
            self._assert_raised(clear_report, "LookupError", "clear")
            trade, created = self._clear_job("j-2", "t-2", False)
            self.assertTrue(created)
        else:
            trade, created = self._assert_returned(clear_report, "clear")
            self.assertTrue(created)
        self.assertEqual(trade["selection"],
                         {"resource_id": "r-1", "version": 1})
        self._assert_no_fragments()
        self._assert_capacity_consistent(False, record, trade)


if __name__ == "__main__":
    unittest.main()
