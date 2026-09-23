from __future__ import annotations

import contextlib
import json
import os
import time
import unittest
from multiprocessing import Process, Queue
from tempfile import TemporaryDirectory

from carbon_market import history, history_copy
from carbon_market.jobs import register as register_job
from carbon_market.market import match
from carbon_market.migrate import run as migrate
from carbon_market.offers import register as register_offer
from carbon_market.recover_all import run as recover_all
from carbon_market.reserve import run as reserve


def _job(job_id: str = "j-1") -> dict[str, object]:
    return {"job_id": job_id, "deadline": 100, "energy_wh": 100,
            "residency_regions": ["eu-north"]}


def _offer(resource_id: str, carbon_intensity: int) -> dict[str, object]:
    return {"resource_id": resource_id, "region": "eu-north",
            "capacity_wh": 250, "unit_cost": 1000,
            "carbon_intensity": carbon_intensity}


def _event(job_id: str = "j-1", now: int = 50) -> dict[str, object]:
    return {"job_id": job_id, "source_id": "r-1", "target_id": "r-2",
            "op": "commit", "now": now}


def _coord(key: str = "batch-key", owner: str = "owner-a", until: int = 80,
           items: dict[str, list] | None = None) -> dict[str, object]:
    return {"version": 1, "key": key, "owner": owner, "until": until,
            "items": {"j-1": [None, None]} if items is None else items}


def _history_doc(key: str = "batch-key",
                 snapshots: list | None = None) -> dict[str, object]:
    if snapshots is None:
        snapshots = [
            ["pending", _coord(key=key, items={"j-1": [None, None]})],
            ["completed", _coord(key=key, until=80,
                                 items={"j-1": [_event(), None]})],
        ]
    return {"version": 1, "key": key, "snapshots": snapshots}


def _hold_exclusive(lock_path: str, queue: "Queue[str]", seconds: float) -> None:
    import fcntl
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o666)
    fcntl.flock(fd, fcntl.LOCK_EX)
    queue.put("locked")
    time.sleep(seconds)
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)


class HistoryCopyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        t = self.tmp.name
        self.jobs = os.path.join(t, "jobs.json")
        self.offers = os.path.join(t, "offers.json")
        self.ledger = os.path.join(t, "ledger.json")
        self.reserves = os.path.join(t, "reserves.json")
        self.state = os.path.join(t, "state.json")
        self.coord = os.path.join(t, "coord.json")
        self.path = self.coord + ".history"
        register_job(self.jobs, _job(), "jk")
        register_offer(self.offers, _offer("r-1", 10), "ok1")
        register_offer(self.offers, _offer("r-2", 100), "ok2")
        match(self.jobs, self.offers, self.ledger, "j-1", "mk")
        reserve(self.jobs, self.offers, self.ledger, self.reserves,
                "j-1", "reserve", "rk", 10)
        migrate(self.jobs, self.offers, self.ledger, self.reserves,
                self.state, "j-1", "r-2", "prepare", "pk", 20)

    def _write(self, doc: dict[str, object], name: str = "history.json") -> str:
        path = os.path.join(self.tmp.name, name)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(doc) + "\n")
        return path

    def _run_batch(self, key: str = "batch-key") -> None:
        recover_all(self.jobs, self.offers, self.ledger, self.reserves,
                    self.state, self.coord, "owner-a", key, 50, 30)

    # -- happy path ------------------------------------------------------

    def test_copy_real_history_is_verifiable_and_byte_identical(self) -> None:
        self._run_batch()
        target = os.path.join(self.tmp.name, "copy.history")
        source_bytes = open(self.path, "rb").read()

        result = history_copy.run(self.path, target, "batch-key")

        self.assertEqual(list(result.keys()),
                         ["key", "count", "statuses", "terminal"])
        self.assertEqual(result, history.verify(self.path, "batch-key"))
        self.assertEqual(result, history.verify(target, "batch-key"))
        self.assertEqual(open(target, "rb").read(), source_bytes)

    def test_result_matches_verify_contract(self) -> None:
        self._run_batch()
        target = os.path.join(self.tmp.name, "copy.history")
        result = history_copy.run(self.path, target, "batch-key")
        self.assertEqual(result["key"], "batch-key")
        self.assertEqual(result["count"], 2)
        self.assertEqual(result["statuses"], ["pending", "completed"])
        self.assertIs(result["terminal"], True)
        self.assertNotIsInstance(result["count"], bool)

    def test_copy_preserves_original_bytes_without_reserializing(self) -> None:
        source = self._write(_history_doc())
        with open(source, "a", encoding="utf-8") as handle:
            handle.write("  \n\t ")
        raw = open(source, "rb").read()
        target = os.path.join(self.tmp.name, "pretty.history")
        history_copy.run(source, target, "batch-key")
        self.assertEqual(open(target, "rb").read(), raw)

    def test_empty_history_copies_with_terminal_false(self) -> None:
        source = self._write(_history_doc(snapshots=[]))
        target = os.path.join(self.tmp.name, "empty-copy.history")
        result = history_copy.run(source, target, "batch-key")
        self.assertEqual(result, {"key": "batch-key", "count": 0,
                                  "statuses": [], "terminal": False})
        self.assertEqual(open(target, "rb").read(), open(source, "rb").read())

    def test_source_is_left_untouched(self) -> None:
        self._run_batch()
        before = open(self.path, "rb").read()
        history_copy.run(self.path,
                         os.path.join(self.tmp.name, "out.history"),
                         "batch-key")
        self.assertEqual(open(self.path, "rb").read(), before)

    # -- arguments -------------------------------------------------------

    def test_bad_string_arguments(self) -> None:
        target = os.path.join(self.tmp.name, "t.history")
        for bad in ("", 1, None, True, b"k"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    history_copy.run(bad, target, "batch-key")  # type: ignore[arg-type]
                with self.assertRaises(ValueError):
                    history_copy.run(self.path, bad, "batch-key")  # type: ignore[arg-type]
                with self.assertRaises(ValueError):
                    history_copy.run(self.path, target, bad)  # type: ignore[arg-type]

    def test_overwrite_must_be_bool(self) -> None:
        for bad in (0, 1, "true", None):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    history_copy.run(self.path,
                                     os.path.join(self.tmp.name, "t.history"),
                                     "batch-key", bad)  # type: ignore[arg-type]

    def test_same_realpath_raises_value_error(self) -> None:
        self._run_batch()
        with self.assertRaises(ValueError):
            history_copy.run(self.path, self.path, "batch-key")
        link = os.path.join(self.tmp.name, "alias.history")
        os.symlink(self.path, link)
        with self.assertRaises(ValueError):
            history_copy.run(self.path, link, "batch-key")

    # -- target handling -------------------------------------------------

    def test_existing_target_raises_file_exists_error_untouched(self) -> None:
        source = self._write(_history_doc())
        target = os.path.join(self.tmp.name, "target.history")
        with open(target, "w", encoding="utf-8") as handle:
            handle.write("previous contents")
        with self.assertRaises(FileExistsError):
            history_copy.run(source, target, "batch-key")
        self.assertEqual(open(target, "r", encoding="utf-8").read(),
                         "previous contents")

    def test_overwrite_replaces_target_byte_for_byte(self) -> None:
        source = self._write(_history_doc())
        target = os.path.join(self.tmp.name, "target.history")
        with open(target, "w", encoding="utf-8") as handle:
            handle.write("old version")
        result = history_copy.run(source, target, "batch-key", overwrite=True)
        self.assertEqual(result, history.verify(source, "batch-key"))
        self.assertEqual(open(target, "rb").read(), open(source, "rb").read())

    def test_failed_commit_keeps_old_target_and_cleans_temp(self) -> None:
        if os.geteuid() == 0:
            self.skipTest("root bypasses directory write permissions")
        source = self._write(_history_doc())
        directory = os.path.join(self.tmp.name, "locked-dir")
        os.mkdir(directory)
        target = os.path.join(directory, "target.history")
        with open(target, "w", encoding="utf-8") as handle:
            handle.write("old version")
        os.chmod(directory, 0o555)
        try:
            with self.assertRaises(OSError):
                history_copy.run(source, target, "batch-key", overwrite=True)
        finally:
            os.chmod(directory, 0o755)
        self.assertEqual(open(target, "r", encoding="utf-8").read(),
                         "old version")
        leftovers = [name for name in os.listdir(directory)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    # -- validation follows history.verify -------------------------------

    def test_wrong_key_raises_key_error_and_writes_nothing(self) -> None:
        source = self._write(_history_doc(key="real-key"))
        target = os.path.join(self.tmp.name, "by-key.history")
        with self.assertRaises(KeyError) as ctx:
            history_copy.run(source, target, "claimed-key")
        self.assertEqual(ctx.exception.args[0], "claimed-key")
        self.assertFalse(os.path.exists(target))

    def test_malformed_json_raises_value_error(self) -> None:
        source = os.path.join(self.tmp.name, "broken.history")
        with open(source, "w", encoding="utf-8") as handle:
            handle.write("{not json")
        with self.assertRaises(ValueError):
            history_copy.run(source,
                             os.path.join(self.tmp.name, "o.history"), "k")

    def test_negative_zero_raises_value_error(self) -> None:
        source = os.path.join(self.tmp.name, "nz.history")
        with open(source, "w", encoding="utf-8") as handle:
            handle.write('{"version":-0,"key":"k","snapshots":[]}')
        with self.assertRaises(ValueError):
            history_copy.run(source,
                             os.path.join(self.tmp.name, "o.history"), "k")

    def test_bad_utf8_raises_value_error(self) -> None:
        source = os.path.join(self.tmp.name, "badutf8.history")
        with open(source, "wb") as handle:
            handle.write(b'{"version":1,"key":"k","snapshots":[]}\xff')
        with self.assertRaises(ValueError):
            history_copy.run(source,
                             os.path.join(self.tmp.name, "o.history"), "k")

    def test_inconsistent_history_raises_value_error(self) -> None:
        doc = _history_doc()
        doc["snapshots"][0][0] = "completed"
        source = self._write(doc)
        with self.assertRaises(ValueError):
            history_copy.run(source,
                             os.path.join(self.tmp.name, "o.history"),
                             "batch-key")

    # -- missing files ---------------------------------------------------

    def test_missing_source_raises_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            history_copy.run(os.path.join(self.tmp.name, "missing.history"),
                             os.path.join(self.tmp.name, "o.history"), "k")

    def test_missing_target_parent_raises_file_not_found(self) -> None:
        source = self._write(_history_doc())
        target = os.path.join(self.tmp.name, "no-such-dir", "o.history")
        with self.assertRaises(FileNotFoundError):
            history_copy.run(source, target, "batch-key")
        self.assertFalse(os.path.exists(os.path.dirname(target)))

    # -- locking ---------------------------------------------------------

    def test_copy_blocks_behind_exclusive_source_lock(self) -> None:
        source = self._write(_history_doc())
        target = os.path.join(self.tmp.name, "o.history")
        queue: Queue[str] = Queue()
        holder = Process(target=_hold_exclusive,
                         args=(source + ".lock", queue, 1.5))
        holder.start()
        self.addCleanup(holder.join, 5)
        self.assertEqual(queue.get(timeout=5), "locked")
        start = time.monotonic()
        history_copy.run(source, target, "batch-key")
        self.assertGreaterEqual(time.monotonic() - start, 1.2)

    def test_copy_blocks_behind_exclusive_target_lock(self) -> None:
        source = self._write(_history_doc())
        target = os.path.join(self.tmp.name, "target.history")
        with open(target, "w", encoding="utf-8") as handle:
            handle.write("old\n")
        queue: Queue[str] = Queue()
        holder = Process(target=_hold_exclusive,
                         args=(target + ".lock", queue, 1.5))
        holder.start()
        self.addCleanup(holder.join, 5)
        self.assertEqual(queue.get(timeout=5), "locked")
        start = time.monotonic()
        history_copy.run(source, target, "batch-key", overwrite=True)
        self.assertGreaterEqual(time.monotonic() - start, 1.2)
        self.assertEqual(open(target, "rb").read(), open(source, "rb").read())


# -- fault injection -------------------------------------------------------

def _io_error(stage: str) -> OSError:
    return OSError(5, f"injected I/O failure at {stage}")


@contextlib.contextmanager
def _fault_at(*stages: str):
    # Fail the first occurrence of each named protocol stage; every later
    # occurrence (and every other stage) proceeds normally.
    failing = set(stages)
    original = history_copy._fault

    def inject(stage: str) -> None:
        if stage in failing:
            failing.discard(stage)
            raise _io_error(stage)

    history_copy._fault = inject
    try:
        yield
    finally:
        history_copy._fault = original


class HistoryCopyFaultInjectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        # Two distinct, individually valid histories: the source has two
        # snapshots, the pre-call target has one. After any failed
        # overwrite history.verify must report the old one-snapshot file.
        self.source = os.path.join(self.tmp.name, "source.history")
        with open(self.source, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(_history_doc()) + "\n")
        self.old_doc = _history_doc(snapshots=[
            ["pending", _coord(items={"j-1": [None, None]})],
        ])
        self.target = os.path.join(self.tmp.name, "target.history")
        self.old_bytes = json.dumps(self.old_doc).encode("utf-8") + b"\n"
        with open(self.target, "wb") as handle:
            handle.write(self.old_bytes)
        self.new_bytes = open(self.source, "rb").read()
        self.assertNotEqual(self.old_bytes, self.new_bytes)

    def _leftovers(self) -> list[str]:
        return [name for name in os.listdir(self.tmp.name)
                if name.startswith(".history-copy")]

    def _recovery_path(self) -> str:
        return self.target + ".recovery"

    def _assert_no_recovery_copy(self) -> None:
        self.assertFalse(os.path.exists(self._recovery_path()))

    def _assert_recovery_copy_holds_old_bytes(self) -> None:
        # A retained fixed-path recovery copy must carry exactly the
        # pre-call target bytes and itself verify as that old history.
        path = self._recovery_path()
        self.assertTrue(os.path.isfile(path))
        with open(path, "rb") as handle:
            self.assertEqual(handle.read(), self.old_bytes)
        summary = history.verify(path, "batch-key")
        self.assertEqual(summary["count"], 1)

    def _remove_recovery_copy(self) -> None:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(self._recovery_path())

    def _assert_old_version_readable(self) -> None:
        # get/verify take the shared target lock and read: they must see
        # only the complete pre-call version.
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        summary = history.verify(self.target, "batch-key")
        self.assertEqual(summary["count"], 1)
        self.assertEqual(summary["statuses"], ["pending"])
        page = history.get(self.target, "batch-key")
        self.assertEqual([row[1] for row in page["snapshots"]], ["pending"])

    def _assert_target_lock_is_free(self) -> None:
        import fcntl
        fd = os.open(self.target + ".lock", os.O_CREAT | os.O_RDWR, 0o666)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def test_recovery_fsync_failure_leaves_old_target(self) -> None:
        with _fault_at("recovery_fsync"):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(self.source, self.target, "batch-key",
                                 overwrite=True)
        self.assertIsNone(ctx.exception.__cause__)
        self._assert_old_version_readable()
        self.assertEqual(self._leftovers(), [])
        self._assert_no_recovery_copy()
        self._assert_target_lock_is_free()

    def test_recovery_dir_sync_failures_keep_recovery_copy(self) -> None:
        for stage in ("recovery_dir_open", "recovery_dir_fsync"):
            with self.subTest(stage=stage):
                with open(self.target, "wb") as handle:
                    handle.write(self.old_bytes)
                with _fault_at(stage):
                    with self.assertRaises(OSError):
                        history_copy.run(self.source, self.target,
                                         "batch-key", overwrite=True)
                self._assert_old_version_readable()
                self.assertEqual(self._leftovers(), [])
                self._assert_recovery_copy_holds_old_bytes()
                self._remove_recovery_copy()

    def test_temp_stage_failures_leave_old_target_and_copy(self) -> None:
        for stage in ("temp_fsync", "replace"):
            with self.subTest(stage=stage):
                with open(self.target, "wb") as handle:
                    handle.write(self.old_bytes)
                with _fault_at(stage):
                    with self.assertRaises(OSError):
                        history_copy.run(self.source, self.target,
                                         "batch-key", overwrite=True)
                self._assert_old_version_readable()
                self.assertEqual(self._leftovers(), [])
                self._assert_recovery_copy_holds_old_bytes()
                self._remove_recovery_copy()

    def test_post_replace_dir_failures_roll_back_old_bytes(self) -> None:
        for stage in ("commit_dir_open", "commit_dir_fsync"):
            with self.subTest(stage=stage):
                with open(self.target, "wb") as handle:
                    handle.write(self.old_bytes)
                with _fault_at(stage):
                    with self.assertRaises(OSError) as ctx:
                        history_copy.run(self.source, self.target,
                                         "batch-key", overwrite=True)
                self.assertIsNone(ctx.exception.__cause__)
                self._assert_old_version_readable()
                self.assertEqual(self._leftovers(), [])
                self._assert_target_lock_is_free()
                # The rollback moved the recovery copy back over the
                # target (the original contract), so no copy is left.
                self._assert_no_recovery_copy()

    def test_backup_cleanup_failures_roll_back_old_bytes(self) -> None:
        # The replace is durable and the recovery-copy unlink is what
        # fails: the copy is moved back over the new target, restoring
        # the pre-call bytes (and consuming the copy).
        with _fault_at("cleanup_unlink"):
            with self.assertRaises(OSError):
                history_copy.run(self.source, self.target, "batch-key",
                                 overwrite=True)
        self._assert_old_version_readable()
        self.assertEqual(self._leftovers(), [])
        self._assert_no_recovery_copy()

    def test_final_dir_sync_failures_restore_from_memory(self) -> None:
        # The recovery copy has already been removed; the old bytes must
        # still be reconstructed from the in-memory copy.
        for stage in ("final_dir_open", "final_dir_fsync"):
            with self.subTest(stage=stage):
                with open(self.target, "wb") as handle:
                    handle.write(self.old_bytes)
                with _fault_at(stage):
                    with self.assertRaises(OSError):
                        history_copy.run(self.source, self.target,
                                         "batch-key", overwrite=True)
                self._assert_old_version_readable()
                self.assertEqual(self._leftovers(), [])
                self._assert_no_recovery_copy()

    def test_existing_recovery_copy_raises_file_exists_error(self) -> None:
        with open(self._recovery_path(), "wb") as handle:
            handle.write(b"a stale recovery copy")
        with self.assertRaises(FileExistsError):
            history_copy.run(self.source, self.target, "batch-key",
                             overwrite=True)
        self._assert_old_version_readable()
        # Neither target nor the pre-existing recovery copy was touched.
        with open(self._recovery_path(), "rb") as handle:
            self.assertEqual(handle.read(), b"a stale recovery copy")
        self.assertEqual(self._leftovers(), [])

    def test_failed_overwrite_then_retry_is_refused_until_copy_cleared(self) -> None:
        # A failure before the replace leaves the target untouched and
        # retains the recovery copy; the copy then blocks a blind retry.
        with _fault_at("temp_fsync"):
            with self.assertRaises(OSError):
                history_copy.run(self.source, self.target, "batch-key",
                                 overwrite=True)
        self._assert_old_version_readable()
        self._assert_recovery_copy_holds_old_bytes()
        with self.assertRaises(FileExistsError):
            history_copy.run(self.source, self.target, "batch-key",
                             overwrite=True)
        self._remove_recovery_copy()
        result = history_copy.run(self.source, self.target, "batch-key",
                                  overwrite=True)
        self.assertEqual(result["count"], 2)
        self.assertEqual(open(self.target, "rb").read(), self.new_bytes)
        self._assert_no_recovery_copy()

    def test_rollback_replace_failure_chains_and_keeps_copy(self) -> None:
        # Post-replace directory sync fails, then moving the recovery
        # copy back fails too: the rollback OSError is raised chained
        # after the first one and the fixed-path recovery copy stays on
        # disk, still carrying the old bytes for manual recovery.
        with _fault_at("commit_dir_fsync", "rollback_replace"):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(self.source, self.target, "batch-key",
                                 overwrite=True)
        self.assertIsNotNone(ctx.exception.__cause__)
        self.assertIn("commit_dir_fsync", str(ctx.exception.__cause__))
        self._assert_recovery_copy_holds_old_bytes()
        self._assert_target_lock_is_free()

    def test_rollback_dir_sync_failure_chains_after_restore(self) -> None:
        # The old bytes make it back onto the target, but syncing the
        # rollback itself fails: still a chained OSError, with the target
        # holding the old bytes.
        with _fault_at("commit_dir_fsync", "rollback_dir_fsync"):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(self.source, self.target, "batch-key",
                                 overwrite=True)
        self.assertIsNotNone(ctx.exception.__cause__)
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        # Moving the copy back already consumed it; only the final
        # directory sync failed.
        self._assert_no_recovery_copy()

    def test_new_target_post_replace_failure_deletes_new_file(self) -> None:
        target = os.path.join(self.tmp.name, "fresh.history")
        with _fault_at("commit_dir_fsync"):
            with self.assertRaises(OSError):
                history_copy.run(self.source, target, "batch-key")
        self.assertFalse(os.path.exists(target))
        self.assertFalse(os.path.exists(target + ".recovery"))
        self.assertEqual([name for name in os.listdir(self.tmp.name)
                          if name.startswith(".history-copy")], [])
        with self.assertRaises(FileNotFoundError):
            history.verify(target, "batch-key")

    def test_new_target_rollback_unlink_failure_chains(self) -> None:
        target = os.path.join(self.tmp.name, "fresh.history")
        with _fault_at("commit_dir_fsync", "rollback_unlink"):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(self.source, target, "batch-key")
        self.assertIsNotNone(ctx.exception.__cause__)
        # The new file the rollback could not remove is the only leftover.
        self.assertEqual(open(target, "rb").read(), self.new_bytes)
        self.assertEqual([name for name in os.listdir(self.tmp.name)
                          if name.startswith(".history-copy-")
                          and name.endswith(".tmp")], [])

    def test_target_lock_failure_raises_oserror_and_writes_nothing(self) -> None:
        import fcntl
        real_lock = history_copy._recover_all._history_file_lock

        def locked_out(realpath, *, shared=False):
            if not shared:
                raise OSError(77, "injected target lock failure")
            return real_lock(realpath, shared=True)

        history_copy._recover_all._history_file_lock = locked_out
        try:
            with self.assertRaises(OSError):
                history_copy.run(self.source, self.target, "batch-key",
                                 overwrite=True)
        finally:
            history_copy._recover_all._history_file_lock = real_lock
        # Nothing beyond the pre-call target was created in the directory
        # (the source-side shared lock path was untouched).
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        self.assertEqual(self._leftovers(), [])
        self._assert_no_recovery_copy()
        # Locking was never actually taken, so the companion lock is free.
        fd = os.open(self.target + ".lock", os.O_CREAT | os.O_RDWR, 0o666)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def test_successful_overwrite_clears_every_temp(self) -> None:
        seen: list[str] = []
        history_copy._fault = seen.append
        try:
            result = history_copy.run(self.source, self.target,
                                      "batch-key", overwrite=True)
        finally:
            history_copy._fault = None
        self.assertEqual(result, history.verify(self.target, "batch-key"))
        self.assertEqual(open(self.target, "rb").read(), self.new_bytes)
        self.assertEqual(self._leftovers(), [])
        self._assert_no_recovery_copy()
        self.assertIn("recovery_fsync", seen)
        self.assertIn("commit_dir_fsync", seen)
        self.assertIn("final_dir_fsync", seen)


if __name__ == "__main__":
    unittest.main()
