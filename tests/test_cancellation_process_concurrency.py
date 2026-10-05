"""Cross-process concurrency tests for the cancellation layer.

The in-process mutex registry in ``carbon_market._lifecycle`` can only
serialize the threads of one interpreter; the cross-process guarantee
is carried by the companion ``flock`` files. These tests run two fully
independent interpreter processes -- started with the multiprocessing
``spawn`` start method, so neither inherits the other's locks or
memory -- against one shared ledger set, synchronize them at
deterministic points and collect each child's return value, exception
type and exit status. Ordering is never inferred from sleeps.

Scenario 1: the first ``cancellation.cancel`` of one traded job races
the first ``dispatch.commit`` for the same job. Exactly one side
commits with ``created=True``; when the cancellation commits first the
commit is refused with ``ValueError``, when the dispatch commits first
the cancellation is refused with ``PermissionError``. The loser leaves
neither a record nor a temporary file and cannot corrupt the winner's
commit; replaying the winner's request returns the stored record with
``False`` and leaves the ledger bytes unchanged.

Scenario 2: cancelling a resource-filling old job races a
``market.clear`` / ``market.clear_live`` of a new job, with a valid
cancellation ledger prepared beforehand. A clear whose snapshot is
taken first is refused with ``LookupError``; the retry after the
cancellation succeeds. When the cancellation completes first the clear
succeeds directly. Whatever the interleaving, the final effective
occupancy never exceeds the capacity of the corresponding resource
version.

Every racing branch is bounded: the parent waits on each child with a
finite deadline and reliably terminates a child that overruns it, so a
deadlock surfaces as an explicit test failure instead of a hang. All
persistent files are afterwards readable through the public read
entries, carry canonical bytes and show no half-written state.
"""

from __future__ import annotations

import json
import os
import queue
import unittest
from multiprocessing import get_context
from tempfile import TemporaryDirectory
from typing import Any

from carbon_market import dispatch as dispatch_module
from carbon_market import jobs as jobs_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.cancellation import cancel, get as get_cancellation
from carbon_market.market import clear, clear_live

# Finite bound for every wait: generous enough that a healthy machine
# never trips it, small enough that a deadlock fails the test promptly.
WAIT_TIMEOUT = 30.0

# Spawned interpreters share neither locks nor memory with the parent.
_CTX = get_context("spawn")


# ---------------------------------------------------------------------------
# Fixture factories (imported and executed in the parent only)
# ---------------------------------------------------------------------------


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


def _make_paths(base: str) -> dict[str, str]:
    return {
        "jobs": os.path.join(base, "jobs.json"),
        "supply": os.path.join(base, "supply.json"),
        "signals": os.path.join(base, "signals.json"),
        "trades": os.path.join(base, "trades.json"),
        "dispatch": os.path.join(base, "dispatch.json"),
        "cancellations": os.path.join(base, "cancellations.json"),
    }


# ---------------------------------------------------------------------------
# Child workers -- module level so spawn can import and pickle them.
# They only use public package entries.
# ---------------------------------------------------------------------------


def _park(result_q: Any, ready: Any, go: Any, tag: str) -> bool:
    """Announce readiness, then block until the parent releases us."""
    ready.set()
    if not go.wait(timeout=WAIT_TIMEOUT):
        # Never hang: report the stall and let the parent fail loudly.
        result_q.put({"tag": tag, "kind": "stall", "error": None})
        return False
    return True


def _report(result_q: Any, tag: str, call: Any, *args: Any,
            **kwargs: Any) -> None:
    try:
        record, created = call(*args, **kwargs)
    except Exception as exc:  # report every business outcome
        result_q.put({"tag": tag, "kind": "error",
                      "error": type(exc).__name__,
                      "message": str(exc)})
    else:
        result_q.put({"tag": tag, "kind": "ok", "record": record,
                      "created": bool(created)})


def _worker_cancel(result_q: Any, ready: Any, go: Any, paths: dict[str, str],
                   payload: dict[str, Any]) -> None:
    if not _park(result_q, ready, go, "cancel"):
        return
    _report(result_q, "cancel", cancel,
            paths["jobs"], paths["supply"], paths["trades"],
            paths["dispatch"], paths["cancellations"],
            payload["job_id"], payload["key"], payload["at"],
            payload["reason"])


def _worker_commit(result_q: Any, ready: Any, go: Any, paths: dict[str, str],
                   payload: dict[str, Any]) -> None:
    if not _park(result_q, ready, go, "commit"):
        return
    _report(result_q, "commit", dispatch_module.commit,
            paths["jobs"], paths["supply"], paths["trades"],
            paths["dispatch"], payload["job_id"], payload["key"],
            payload["at"], cancellations=paths["cancellations"])


def _worker_clear(result_q: Any, ready: Any, go: Any, paths: dict[str, str],
                  payload: dict[str, Any]) -> None:
    if not _park(result_q, ready, go, "clear"):
        return
    if payload["live"]:
        args = (paths["jobs"], paths["supply"], paths["signals"],
                paths["trades"])
        call = clear_live
    else:
        args = (paths["jobs"], paths["supply"], paths["trades"])
        call = clear
    _report(result_q, "clear", call, *args,
            payload["job_id"], payload["key"], payload["at"],
            cancellations=paths["cancellations"])


def _worker_replay_cancel_commit(result_q: Any, ready: Any, go: Any,
                                 paths: dict[str, str],
                                 payload: dict[str, Any]) -> None:
    """Replay both racing requests from a third fresh process.

    The winner's replay must return its stored record with created
    False; the loser's repeated request must stay refused with the same
    exception type as its first attempt.
    """
    if not _park(result_q, ready, go, "replay"):
        return
    report: dict[str, Any] = {}
    try:
        record, created = cancel(
            paths["jobs"], paths["supply"], paths["trades"],
            paths["dispatch"], paths["cancellations"],
            "j-1", "c-1", 20, "race")
        report["cancel"] = {"kind": "ok", "created": bool(created),
                            "record": record}
    except (ValueError, PermissionError) as exc:
        report["cancel"] = {"kind": "error", "error": type(exc).__name__}
    try:
        record, created = dispatch_module.commit(
            paths["jobs"], paths["supply"], paths["trades"],
            paths["dispatch"], "j-1", "d-1", 20,
            cancellations=paths["cancellations"])
        report["commit"] = {"kind": "ok", "created": bool(created),
                            "record": record}
    except ValueError as exc:
        report["commit"] = {"kind": "error", "error": type(exc).__name__}
    result_q.put({"tag": "replay", "kind": "ok", "report": report})


# ---------------------------------------------------------------------------
# Parent-side process orchestration
# ---------------------------------------------------------------------------


def _collect(result_q: Any, results: dict[str, dict[str, Any]]) -> None:
    # A queue timeout means a child never reported -- a deadlock or lost
    # child -- and must surface as an explicit, descriptive failure; the
    # finally blocks in the callers still reap the live children.
    try:
        item = result_q.get(timeout=WAIT_TIMEOUT)
    except queue.Empty as exc:
        raise AssertionError(
            f"timed out after {WAIT_TIMEOUT:.0f}s waiting for a child "
            "result (deadlock or child lost)") from exc
    results[item["tag"]] = item


def _reap(proc: Any) -> int:
    # Join with a finite bound, then terminate; a deadlock must surface
    # as an assertion, never as a hung child.
    proc.join(timeout=5)
    if proc.exitcode is None:
        proc.kill()
        proc.join(timeout=5)
    if proc.exitcode is None:
        raise AssertionError(f"child {proc.name} would not terminate")
    return proc.exitcode


def _spawn_pair(target_a: Any, payload_a: dict[str, Any],
                target_b: Any, payload_b: dict[str, Any],
                paths: dict[str, str], *, first: str | None = None
                ) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    """Run two spawned children parked on identical sync points.

    ``first`` selects deterministic ordering: ``"a"`` releases child A
    and awaits its terminal result before releasing B (and vice versa
    for ``"b"``); ``None`` releases both simultaneously so the file
    locks decide the order.
    """
    result_q: Any = _CTX.Queue()
    ready_a, ready_b = _CTX.Event(), _CTX.Event()
    go_a, go_b = _CTX.Event(), _CTX.Event()
    proc_a = _CTX.Process(
        target=target_a, name="racer-a",
        args=(result_q, ready_a, go_a, dict(paths), payload_a))
    proc_b = _CTX.Process(
        target=target_b, name="racer-b",
        args=(result_q, ready_b, go_b, dict(paths), payload_b))
    results: dict[str, dict[str, Any]] = {}
    proc_a.start()
    proc_b.start()
    try:
        if not ready_a.wait(WAIT_TIMEOUT):
            raise AssertionError("child A never reached its sync point")
        if not ready_b.wait(WAIT_TIMEOUT):
            raise AssertionError("child B never reached its sync point")
        if first is None:
            go_a.set()
            go_b.set()
            _collect(result_q, results)
            _collect(result_q, results)
        elif first == "a":
            go_a.set()
            _collect(result_q, results)
            go_b.set()
            _collect(result_q, results)
        else:
            assert first == "b"
            go_b.set()
            _collect(result_q, results)
            go_a.set()
            _collect(result_q, results)
        for item in results.values():
            if item["kind"] not in ("ok", "error"):
                raise AssertionError(f"child did not finish cleanly: {item!r}")
    finally:
        exitcodes = {"a": _reap(proc_a), "b": _reap(proc_b)}
        result_q.close()
        result_q.join_thread()
    return results, exitcodes


def _spawn_one(target: Any, payload: dict[str, Any], paths: dict[str, str]
               ) -> dict[str, Any]:
    result_q: Any = _CTX.Queue()
    ready, go = _CTX.Event(), _CTX.Event()
    proc = _CTX.Process(
        target=target, name="observer",
        args=(result_q, ready, go, dict(paths), payload))
    proc.start()
    try:
        if not ready.wait(WAIT_TIMEOUT):
            raise AssertionError("observer never reached its sync point")
        go.set()
        try:
            item = result_q.get(timeout=WAIT_TIMEOUT)
        except queue.Empty as exc:
            raise AssertionError(
                f"timed out after {WAIT_TIMEOUT:.0f}s waiting for the "
                "observer result (deadlock or child lost)") from exc
    finally:
        exitcode = _reap(proc)
        result_q.close()
        result_q.join_thread()
    if exitcode != 0:
        raise AssertionError(f"observer exited with {exitcode}: {item!r}")
    if item["kind"] != "ok":
        raise AssertionError(f"observer failed: {item!r}")
    return item


# ---------------------------------------------------------------------------
# Integrity assertions (parent process, public entries only)
# ---------------------------------------------------------------------------


def _read_json(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _read_bytes(path: str) -> bytes:
    with open(path, "rb") as handle:
        return handle.read()


def _assert_no_stray_files(testcase: unittest.TestCase,
                           paths: dict[str, str]) -> None:
    # Companion flock files are permanent by design; any other hidden
    # entry or *.tmp fragment is a half-written commit.
    directory = os.path.dirname(paths["jobs"])
    allowed = {os.path.basename(path) + ".lock" for path in paths.values()}
    leftovers = [
        name for name in os.listdir(directory)
        if name not in allowed
        and (name.endswith(".tmp") or name.startswith("."))]
    testcase.assertEqual(leftovers, [])


def _snapshot(paths: dict[str, str]) -> dict[str, bytes]:
    return {name: _read_bytes(path)
            for name, path in paths.items() if os.path.exists(path)}


def _assert_readable_and_canonical(testcase: unittest.TestCase,
                                   paths: dict[str, str],
                                   expected_jobs: list[str]) -> None:
    """Every persisted file is readable via a public entry and canonical.

    The public readers run the strict canonical-byte validation on
    load, so reading every record through them proves there is no
    half-written state. Idempotent replays through the writing entries
    must return created=False without rewriting a byte; the snapshot
    taken before them must match the bytes afterwards.
    """
    before = _snapshot(paths)

    # Input registries: public readers over the published records.
    resources_module.get(paths["supply"], "r-1")
    signals_module.get(paths["signals"], "eu-north", 10)
    for job_id in expected_jobs:
        jobs_module.get(paths["jobs"], job_id)

    # Cancellation ledger: every recorded cancellation via get().
    cancellations: list[dict[str, Any]] = []
    if os.path.exists(paths["cancellations"]):
        doc = _read_json(paths["cancellations"])
        cancellations = list(doc["cancellations"].values())
        for record in cancellations:
            testcase.assertEqual(
                get_cancellation(paths["cancellations"],
                                 record["job_id"]),
                record)

    # Trades ledger: replay each idempotency key through clear; the
    # replay branch never depends on live signals and never rewrites.
    if os.path.exists(paths["trades"]):
        doc = _read_json(paths["trades"])
        for key, binding in doc["idempotency"].items():
            _trade, created = clear(
                paths["jobs"], paths["supply"], paths["trades"],
                binding["job_id"], key, binding["at"],
                cancellations=paths["cancellations"])
            testcase.assertFalse(created)

    # Dispatch ledger: replay each idempotency key through commit.
    if os.path.exists(paths["dispatch"]):
        doc = _read_json(paths["dispatch"])
        for key, binding in doc["idempotency"].items():
            _decision, created = dispatch_module.commit(
                paths["jobs"], paths["supply"], paths["trades"],
                paths["dispatch"], binding["job_id"], key, binding["at"],
                cancellations=paths["cancellations"])
            testcase.assertFalse(created)

    testcase.assertEqual(_snapshot(paths), before)
    _assert_no_stray_files(testcase, paths)


def _assert_effective_occupancy_within_capacity(
        testcase: unittest.TestCase, paths: dict[str, str], at: int = 60
) -> None:
    """Effective occupancy per exact resource version never oversells.

    Mirrors the public clearing envelope at ``at``: a trade whose job
    was cancelled at a moment not later than ``at`` no longer deducts
    its work from the booked resource version.
    """
    supply = _read_json(paths["supply"])
    capacities = {
        (resource_id, index + 1): record["capacity"]
        for resource_id, versions in supply["history"].items()
        for index, record in enumerate(versions)}
    trades = _read_json(paths["trades"])
    released: set[str] = set()
    if os.path.exists(paths["cancellations"]):
        cancellations = _read_json(paths["cancellations"])
        for record in cancellations["cancellations"].values():
            if record["at"] <= at:
                released.add(record["job_id"])
    occupied: dict[tuple[str, int], int] = {}
    for job_id, trade in trades["trades"].items():
        if job_id in released:
            continue
        slot = (trade["resource_id"], trade["version"])
        occupied[slot] = occupied.get(slot, 0) + trade["work"]
    for slot, total in occupied.items():
        testcase.assertLessEqual(
            total, capacities[slot],
            f"effective occupancy {total} oversells {slot} capacity "
            f"{capacities[slot]}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class CancelCommitProcessRaceTest(unittest.TestCase):
    """Scenario 1: first cancel vs first dispatch commit, two processes."""

    def _setup(self, tmp: TemporaryDirectory) -> dict[str, str]:
        paths = _make_paths(tmp.name)
        signals_module.publish(paths["signals"], _signal(), "sk-1")
        resources_module.publish(paths["supply"], _resource(), "rk-1")
        jobs_module.submit(paths["jobs"], _job("j-1"), "jk-1")
        jobs_module.submit(paths["jobs"], _job("j-seed"), "jk-s")
        trade1, created = clear(paths["jobs"], paths["supply"],
                                paths["trades"], "j-1", "t-1", 10)
        self.assertTrue(created)
        self.assertEqual(trade1["resource_id"], "r-1")
        _seed, created = clear(paths["jobs"], paths["supply"],
                               paths["trades"], "j-seed", "t-s", 10)
        self.assertTrue(created)
        # Bring the cancellation ledger into existence legally so the
        # racing commit always reads it as part of its snapshot.
        _record, created = cancel(paths["jobs"], paths["supply"],
                                  paths["trades"], paths["dispatch"],
                                  paths["cancellations"],
                                  "j-seed", "c-s", 15, "seed")
        self.assertTrue(created)
        return paths

    def test_cancel_commits_first_then_commit_gets_value_error(self) -> None:
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        paths = self._setup(tmp)

        results, exitcodes = _spawn_pair(
            _worker_cancel,
            {"job_id": "j-1", "key": "c-1", "at": 20, "reason": "race"},
            _worker_commit,
            {"job_id": "j-1", "key": "d-1", "at": 20},
            paths, first="a")

        self.assertEqual(exitcodes, {"a": 0, "b": 0}, results)
        cancel_item, commit_item = results["cancel"], results["commit"]
        self.assertEqual(cancel_item["kind"], "ok")
        self.assertTrue(cancel_item["created"])
        self.assertEqual(cancel_item["record"], {
            "job_id": "j-1", "at": 20, "reason": "race",
            "resource_id": "r-1", "version": 1})
        # The refused commit leaves exactly the promised exception...
        self.assertEqual(commit_item["kind"], "error")
        self.assertEqual(commit_item["error"], "ValueError")
        # ...no dispatch ledger, no temp file, no corrupted bytes.
        self.assertFalse(os.path.exists(paths["dispatch"]))
        self.assertEqual(get_cancellation(paths["cancellations"], "j-1"),
                         cancel_item["record"])
        _assert_readable_and_canonical(self, paths, ["j-1", "j-seed"])

    def test_commit_commits_first_then_cancel_gets_permission_error(
            self) -> None:
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        paths = self._setup(tmp)

        results, exitcodes = _spawn_pair(
            _worker_commit,
            {"job_id": "j-1", "key": "d-1", "at": 20},
            _worker_cancel,
            {"job_id": "j-1", "key": "c-1", "at": 20, "reason": "race"},
            paths, first="a")

        self.assertEqual(exitcodes, {"a": 0, "b": 0}, results)
        commit_item, cancel_item = results["commit"], results["cancel"]
        self.assertEqual(commit_item["kind"], "ok")
        self.assertTrue(commit_item["created"])
        self.assertEqual(commit_item["record"]["state"], "ready")
        # The refused cancel leaves exactly the promised exception...
        self.assertEqual(cancel_item["kind"], "error")
        self.assertEqual(cancel_item["error"], "PermissionError")
        # ...and no cancellation record for j-1; the seed survives.
        doc = _read_json(paths["cancellations"])
        self.assertEqual(set(doc["cancellations"]), {"c-s"})
        with self.assertRaises(KeyError):
            get_cancellation(paths["cancellations"], "j-1")
        _assert_readable_and_canonical(self, paths, ["j-1", "j-seed"])

    def test_simultaneous_first_cancel_and_commit_have_one_winner(self) -> None:
        for iteration in range(5):
            with self.subTest(iteration=iteration):
                self._race_once(iteration)

    def _race_once(self, iteration: int) -> None:
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        paths = self._setup(tmp)

        results, exitcodes = _spawn_pair(
            _worker_cancel,
            {"job_id": "j-1", "key": "c-1", "at": 20, "reason": "race"},
            _worker_commit,
            {"job_id": "j-1", "key": "d-1", "at": 20},
            paths)

        self.assertEqual(exitcodes, {"a": 0, "b": 0}, results)
        cancel_item, commit_item = results["cancel"], results["commit"]
        cancel_won = (cancel_item["kind"] == "ok"
                      and cancel_item["created"]
                      and commit_item["kind"] == "error"
                      and commit_item["error"] == "ValueError")
        commit_won = (commit_item["kind"] == "ok"
                      and commit_item["created"]
                      and cancel_item["kind"] == "error"
                      and cancel_item["error"] == "PermissionError")
        self.assertTrue(cancel_won or commit_won, results)
        self.assertIsNot(cancel_won, commit_won, results)  # exactly one

        if cancel_won:
            self.assertFalse(os.path.exists(paths["dispatch"]))
            winning_record = get_cancellation(
                paths["cancellations"], "j-1")
            self.assertEqual(winning_record, cancel_item["record"])
        else:
            with self.assertRaises(KeyError):
                get_cancellation(paths["cancellations"], "j-1")

        # Replay both requests from a third spawned process; bytes stay
        # unchanged through both replays.
        before = _snapshot(paths)
        item = _spawn_one(_worker_replay_cancel_commit, {}, paths)
        report = item["report"]
        if cancel_won:
            self.assertEqual(report["cancel"],
                             {"kind": "ok", "created": False,
                              "record": cancel_item["record"]})
            self.assertEqual(report["commit"],
                             {"kind": "error", "error": "ValueError"})
        else:
            self.assertEqual(report["cancel"],
                             {"kind": "error",
                              "error": "PermissionError"})
            self.assertEqual(report["commit"]["kind"], "ok")
            self.assertFalse(report["commit"]["created"])
            self.assertEqual(report["commit"]["record"],
                             commit_item["record"])
        self.assertEqual(_snapshot(paths), before)

        _assert_readable_and_canonical(self, paths, ["j-1", "j-seed"])
        _assert_effective_occupancy_within_capacity(self, paths)


class CancelClearProcessRaceTest(unittest.TestCase):
    """Scenario 2: cancelling a filling old job vs clearing a new one."""

    def _setup(self, tmp: TemporaryDirectory, live: bool) -> dict[str, str]:
        paths = _make_paths(tmp.name)
        signals_module.publish(paths["signals"], _signal(), "sk-1")
        # Capacity 110 lets the unrelated 10-work seed trade (needed to
        # create a legal cancellation ledger) sit beside the 100-work
        # old job; the 100-work new job fits only after the old job's
        # release, never while it still occupies its version.
        resources_module.publish(paths["supply"],
                                 _resource(capacity=110), "rk-1")
        jobs_module.submit(paths["jobs"], _job("j-old", work=100), "jk-o")
        jobs_module.submit(paths["jobs"], _job("j-seed", work=10), "jk-s")
        jobs_module.submit(paths["jobs"], _job("j-new", work=100), "jk-n")
        if live:
            old_trade, created = clear_live(
                paths["jobs"], paths["supply"], paths["signals"],
                paths["trades"], "j-old", "t-old", 10)
            seed_trade, created_seed = clear_live(
                paths["jobs"], paths["supply"], paths["signals"],
                paths["trades"], "j-seed", "t-s", 10)
        else:
            old_trade, created = clear(
                paths["jobs"], paths["supply"], paths["trades"],
                "j-old", "t-old", 10)
            seed_trade, created_seed = clear(
                paths["jobs"], paths["supply"], paths["trades"],
                "j-seed", "t-s", 10)
        self.assertTrue(created and created_seed)
        self.assertEqual((old_trade["resource_id"], old_trade["version"]),
                         ("r-1", 1))
        self.assertEqual((seed_trade["resource_id"], seed_trade["version"]),
                         ("r-1", 1))
        # A valid, pre-existing cancellation ledger for an unrelated
        # released trade.
        _record, seed_cancelled = cancel(
            paths["jobs"], paths["supply"], paths["trades"],
            paths["dispatch"], paths["cancellations"],
            "j-seed", "c-s", 15, "seed")
        self.assertTrue(seed_cancelled)
        return paths

    def _retry_clear(self, paths: dict[str, str], live: bool
                     ) -> dict[str, object]:
        if live:
            trade, created = clear_live(
                paths["jobs"], paths["supply"], paths["signals"],
                paths["trades"], "j-new", "t-new", 25,
                cancellations=paths["cancellations"])
        else:
            trade, created = clear(
                paths["jobs"], paths["supply"], paths["trades"],
                "j-new", "t-new", 25,
                cancellations=paths["cancellations"])
        self.assertTrue(created)
        self.assertEqual((trade["resource_id"], trade["version"]),
                         ("r-1", 1))
        return trade

    def test_cancel_completes_first_clear_succeeds(self) -> None:
        for live in (False, True):
            with self.subTest(live=live):
                tmp = TemporaryDirectory()
                self.addCleanup(tmp.cleanup)
                paths = self._setup(tmp, live)

                results, exitcodes = _spawn_pair(
                    _worker_cancel,
                    {"job_id": "j-old", "key": "c-old", "at": 20,
                     "reason": "freed"},
                    _worker_clear,
                    {"live": live, "job_id": "j-new", "key": "t-new",
                     "at": 25},
                    paths, first="a")

                self.assertEqual(exitcodes, {"a": 0, "b": 0}, results)
                cancel_item, clear_item = results["cancel"], results["clear"]
                self.assertEqual(cancel_item["kind"], "ok")
                self.assertTrue(cancel_item["created"])
                self.assertEqual(clear_item["kind"], "ok", results)
                self.assertTrue(clear_item["created"])
                self.assertEqual(clear_item["record"]["resource_id"], "r-1")
                self.assertEqual(clear_item["record"]["version"], 1)

                # Replaying the winning clear returns the same trade
                # with False and rewrites nothing.
                before = _snapshot(paths)
                if live:
                    _trade, replayed = clear_live(
                        paths["jobs"], paths["supply"], paths["signals"],
                        paths["trades"], "j-new", "t-new", 25,
                        cancellations=paths["cancellations"])
                else:
                    _trade, replayed = clear(
                        paths["jobs"], paths["supply"], paths["trades"],
                        "j-new", "t-new", 25,
                        cancellations=paths["cancellations"])
                self.assertFalse(replayed)
                self.assertEqual(_snapshot(paths), before)

                _assert_readable_and_canonical(
                    self, paths, ["j-old", "j-seed", "j-new"])
                _assert_effective_occupancy_within_capacity(self, paths)

    def test_clear_takes_snapshot_first_then_succeeds_on_retry(self) -> None:
        for live in (False, True):
            with self.subTest(live=live):
                tmp = TemporaryDirectory()
                self.addCleanup(tmp.cleanup)
                paths = self._setup(tmp, live)

                results, exitcodes = _spawn_pair(
                    _worker_clear,
                    {"live": live, "job_id": "j-new", "key": "t-new",
                     "at": 25},
                    _worker_cancel,
                    {"job_id": "j-old", "key": "c-old", "at": 20,
                     "reason": "freed"},
                    paths, first="a")

                self.assertEqual(exitcodes, {"a": 0, "b": 0}, results)
                clear_item, cancel_item = results["clear"], results["cancel"]
                # The clear's snapshot saw the old job still filling the
                # version: LookupError, with nothing written...
                self.assertEqual(clear_item["kind"], "error")
                self.assertEqual(clear_item["error"], "LookupError")
                trades_doc = _read_json(paths["trades"])
                self.assertEqual(set(trades_doc["trades"]),
                                 {"j-old", "j-seed"})
                self.assertEqual(set(trades_doc["idempotency"]),
                                 {"t-old", "t-s"})
                # ...and the cancellation still completed cleanly.
                self.assertEqual(cancel_item["kind"], "ok")
                self.assertTrue(cancel_item["created"])
                self.assertEqual(
                    get_cancellation(paths["cancellations"], "j-old"),
                    cancel_item["record"])

                # After the release the identical request succeeds.
                trade = self._retry_clear(paths, live)
                self.assertEqual(trade["job_id"], "j-new")

                _assert_readable_and_canonical(
                    self, paths, ["j-old", "j-seed", "j-new"])
                _assert_effective_occupancy_within_capacity(self, paths)

    def test_simultaneous_cancel_and_clear_never_oversell(self) -> None:
        for live in (False, True):
            for iteration in range(5):
                with self.subTest(live=live, iteration=iteration):
                    self._race_once(live)

    def _race_once(self, live: bool) -> None:
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        paths = self._setup(tmp, live)

        results, exitcodes = _spawn_pair(
            _worker_cancel,
            {"job_id": "j-old", "key": "c-old", "at": 20,
             "reason": "freed"},
            _worker_clear,
            {"live": live, "job_id": "j-new", "key": "t-new", "at": 25},
            paths)

        self.assertEqual(exitcodes, {"a": 0, "b": 0}, results)
        cancel_item, clear_item = results["cancel"], results["clear"]
        self.assertEqual(cancel_item["kind"], "ok", results)
        self.assertTrue(cancel_item["created"])
        self.assertIn(clear_item["kind"], ("ok", "error"), results)

        if clear_item["kind"] == "error":
            # Snapshot lost the race: refused for lack of capacity...
            self.assertEqual(clear_item["error"], "LookupError")
            # ...and the refused clear left the trades ledger untouched.
            trades_doc = _read_json(paths["trades"])
            self.assertEqual(set(trades_doc["trades"]),
                             {"j-old", "j-seed"})
            trade = self._retry_clear(paths, live)
        else:
            self.assertTrue(clear_item["created"])
            trade = clear_item["record"]
            self.assertEqual((trade["resource_id"], trade["version"]),
                             ("r-1", 1))

        self.assertEqual(trade["job_id"], "j-new")
        _assert_readable_and_canonical(
            self, paths, ["j-old", "j-seed", "j-new"])
        _assert_effective_occupancy_within_capacity(self, paths)


if __name__ == "__main__":
    unittest.main()
