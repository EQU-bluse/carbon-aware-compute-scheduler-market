from __future__ import annotations

import errno
import json
import os
import stat
import time
import unittest
from multiprocessing import Process, Queue
from tempfile import TemporaryDirectory
from unittest import mock

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


def _io_error(message: str) -> OSError:
    return OSError(errno.EIO, message)


def _fsync_wrapper(fail_on):
    """Patch ``os.fsync`` classifying each call as file or directory fsync.

    ``fail_on(kind, ordinal)`` returns an ``OSError`` to raise, or ``None``.
    File fsyncs and directory fsyncs are counted independently, in call
    order, so callers can target e.g. the 2nd directory fsync (the
    post-replace commit sync) without tracking fragile fd numbers.
    """
    real_fsync = os.fsync
    counts = {"file": 0, "dir": 0}

    def fsync(fd):
        mode = os.fstat(fd).st_mode
        kind = "dir" if stat.S_ISDIR(mode) else "file"
        counts[kind] += 1
        failure = fail_on(kind, counts[kind])
        if failure is not None:
            raise failure
        return real_fsync(fd)

    return fsync


def _open_wrapper(directory, fail_on_open_ordinal, failure):
    """Patch ``os.open``; raise ``failure`` on the nth open of the dir itself."""
    real_open = os.open
    state = {"dir_opens": 0}

    def open_(path, flags, *args, **kwargs):
        if path == directory:
            state["dir_opens"] += 1
            if state["dir_opens"] == fail_on_open_ordinal:
                raise failure
        return real_open(path, flags, *args, **kwargs)

    return open_


class HistoryCopyCrashSafetyTest(unittest.TestCase):
    """Fault-injection coverage for the overwrite rollback protocol."""

    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = self.tmp.name

    def _write(self, doc: dict[str, object], name: str) -> str:
        path = os.path.join(self.directory, name)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(doc) + "\n")
        return path

    def _old_target(self, name: str = "target.history") -> tuple[str, bytes]:
        """A pre-existing, self-consistent one-snapshot target history."""
        doc = _history_doc(snapshots=[
            ["pending", _coord(items={"j-1": [None, None]})],
        ])
        path = os.path.join(self.directory, name)
        raw = (json.dumps(doc) + "\n").encode("utf-8")
        with open(path, "wb") as handle:
            handle.write(raw)
        return path, raw

    def _attempt_files(self) -> list[str]:
        return sorted(
            name for name in os.listdir(self.directory)
            if name.startswith(".history-copy")
        )

    def _assert_reads_complete_old_version(
        self, target: str, old_raw: bytes
    ) -> None:
        # Raw bytes and the locked read path must both show the complete
        # previous version only.
        self.assertEqual(open(target, "rb").read(), old_raw)
        summary = history.verify(target, "batch-key")
        self.assertEqual(summary, {"key": "batch-key", "count": 1,
                                   "statuses": ["pending"], "terminal": False})
        paged = history.get(target, "batch-key")
        self.assertEqual(len(paged["snapshots"]), 1)
        self.assertEqual(paged["snapshots"][0][1], "pending")

    # -- failures before the replace leave the old target in place ------

    def test_backup_file_fsync_failure_leaves_old_target_clean(self) -> None:
        source = self._write(_history_doc(), "source.history")
        target, old_raw = self._old_target()
        failure = _io_error("backup fsync failed")
        wrapper = _fsync_wrapper(
            lambda kind, n: failure if (kind, n) == ("file", 1) else None)
        with mock.patch("carbon_market.history_copy.os.fsync",
                        side_effect=wrapper):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(source, target, "batch-key", overwrite=True)
        self.assertIs(ctx.exception, failure)
        self.assertIsNone(ctx.exception.__cause__)
        self._assert_reads_complete_old_version(target, old_raw)
        self.assertEqual(self._attempt_files(), [])

    def test_backup_directory_fsync_failure_leaves_old_target_clean(self) -> None:
        source = self._write(_history_doc(), "source.history")
        target, old_raw = self._old_target()
        failure = _io_error("backup directory fsync failed")
        wrapper = _fsync_wrapper(
            lambda kind, n: failure if (kind, n) == ("dir", 1) else None)
        with mock.patch("carbon_market.history_copy.os.fsync",
                        side_effect=wrapper):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(source, target, "batch-key", overwrite=True)
        self.assertIs(ctx.exception, failure)
        self._assert_reads_complete_old_version(target, old_raw)
        self.assertEqual(self._attempt_files(), [])

    def test_new_temp_fsync_failure_leaves_old_target_clean(self) -> None:
        source = self._write(_history_doc(), "source.history")
        target, old_raw = self._old_target()
        failure = _io_error("new temp fsync failed")
        wrapper = _fsync_wrapper(
            lambda kind, n: failure if (kind, n) == ("file", 2) else None)
        with mock.patch("carbon_market.history_copy.os.fsync",
                        side_effect=wrapper):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(source, target, "batch-key", overwrite=True)
        self.assertIs(ctx.exception, failure)
        self._assert_reads_complete_old_version(target, old_raw)
        self.assertEqual(self._attempt_files(), [])

    def test_replace_failure_leaves_old_target_clean(self) -> None:
        source = self._write(_history_doc(), "source.history")
        target, old_raw = self._old_target()
        failure = _io_error("replace failed")
        real_replace = os.replace

        def replace(src, dst):
            # Fail the commit replace only; a rollback restore replace
            # (".history-copy-restore-") must still be allowed.
            base = os.path.basename(src)
            if (base.startswith(".history-copy-")
                    and not base.startswith(".history-copy-restore-")):
                raise failure
            return real_replace(src, dst)

        with mock.patch("carbon_market.history_copy.os.replace",
                        side_effect=replace):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(source, target, "batch-key", overwrite=True)
        self.assertIs(ctx.exception, failure)
        self._assert_reads_complete_old_version(target, old_raw)
        self.assertEqual(self._attempt_files(), [])

    # -- failures after the replace restore the old bytes ---------------

    def test_commit_directory_open_failure_restores_old_bytes(self) -> None:
        source = self._write(_history_doc(), "source.history")
        target, old_raw = self._old_target()
        new_raw = open(source, "rb").read()
        failure = _io_error("commit directory open failed")
        wrapper = _open_wrapper(self.directory, 2, failure)
        with mock.patch("carbon_market.history_copy.os.open",
                        side_effect=wrapper):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(source, target, "batch-key", overwrite=True)
        self.assertIs(ctx.exception, failure)
        self.assertIsNone(ctx.exception.__cause__)
        self.assertNotEqual(new_raw, old_raw)
        self._assert_reads_complete_old_version(target, old_raw)
        self.assertEqual(self._attempt_files(), [])

    def test_commit_directory_fsync_failure_restores_old_bytes(self) -> None:
        source = self._write(_history_doc(), "source.history")
        target, old_raw = self._old_target()
        failure = _io_error("commit directory fsync failed")
        wrapper = _fsync_wrapper(
            lambda kind, n: failure if (kind, n) == ("dir", 2) else None)
        with mock.patch("carbon_market.history_copy.os.fsync",
                        side_effect=wrapper):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(source, target, "batch-key", overwrite=True)
        self.assertIs(ctx.exception, failure)
        self.assertIsNone(ctx.exception.__cause__)
        self._assert_reads_complete_old_version(target, old_raw)
        self.assertEqual(self._attempt_files(), [])
        # The target lock was released after rollback: a later overwrite
        # on the same target succeeds.
        history_copy.run(source, target, "batch-key", overwrite=True)
        self.assertEqual(open(target, "rb").read(), open(source, "rb").read())

    def test_backup_unlink_failure_restores_target_and_keeps_backup(self) -> None:
        source = self._write(_history_doc(), "source.history")
        target, old_raw = self._old_target()
        first = _io_error("backup unlink failed #1")
        second = _io_error("backup unlink failed #2")
        failures = [first, second]
        real_unlink = os.unlink

        def unlink(path):
            if os.path.basename(path).startswith(".history-copy-backup-"):
                raise failures.pop(0)
            return real_unlink(path)

        with mock.patch("carbon_market.history_copy.os.unlink",
                        side_effect=unlink):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(source, target, "batch-key", overwrite=True)
        # The original error is the first unlink failure; rollback
        # restored the old bytes and then failed re-deleting the backup,
        # so the second OSError is raised chained from the first.
        self.assertIs(ctx.exception, second)
        self.assertIs(ctx.exception.__cause__, first)
        self.assertEqual(ctx.exception.errno, errno.EIO)
        self._assert_reads_complete_old_version(target, old_raw)
        leftovers = self._attempt_files()
        backups = [n for n in leftovers
                   if n.startswith(".history-copy-backup-")]
        self.assertEqual(len(backups), 1)
        self.assertEqual(len(leftovers), 1)
        backup_path = os.path.join(self.directory, backups[0])
        self.assertEqual(open(backup_path, "rb").read(), old_raw)
        # The retained backup is a directly recoverable old-bytes copy.
        recovered = target + ".recovered"
        os.replace(backup_path, recovered)
        self.assertEqual(open(recovered, "rb").read(), old_raw)

    # -- rollback failures chain from the first error -------------------

    def test_restore_replace_failure_chains_and_keeps_backup(self) -> None:
        source = self._write(_history_doc(), "source.history")
        target, old_raw = self._old_target()
        first = _io_error("commit directory fsync failed")
        rollback_failure = _io_error("restore replace failed")
        fsync_wrapper = _fsync_wrapper(
            lambda kind, n: first if (kind, n) == ("dir", 2) else None)
        real_replace = os.replace

        def replace(src, dst):
            if os.path.basename(src).startswith(".history-copy-restore-"):
                raise rollback_failure
            return real_replace(src, dst)

        with mock.patch("carbon_market.history_copy.os.fsync",
                        side_effect=fsync_wrapper), \
             mock.patch("carbon_market.history_copy.os.replace",
                        side_effect=replace):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(source, target, "batch-key", overwrite=True)
        self.assertIs(ctx.exception, rollback_failure)
        self.assertIs(ctx.exception.__cause__, first)
        # Restore could not happen: the target holds the new bytes, but
        # the synced old-bytes backup survives for manual recovery.
        self.assertEqual(open(target, "rb").read(), open(source, "rb").read())
        backups = [n for n in self._attempt_files()
                   if n.startswith(".history-copy-backup-")]
        self.assertEqual(len(backups), 1)
        self.assertEqual(
            open(os.path.join(self.directory, backups[0]), "rb").read(),
            old_raw)

    def test_rollback_directory_fsync_failure_chains_from_first(self) -> None:
        source = self._write(_history_doc(), "source.history")
        target, old_raw = self._old_target()
        first = _io_error("commit directory fsync failed")
        rollback_failure = _io_error("rollback directory fsync failed")

        def fail_on(kind, n):
            if kind != "dir":
                return None
            # 1 backup durability; 2 commit (first failure); 3 restore
            # durability during rollback; 4 cleanup sync during rollback.
            if n == 2:
                return first
            if n == 4:
                return rollback_failure
            return None

        with mock.patch("carbon_market.history_copy.os.fsync",
                        side_effect=_fsync_wrapper(fail_on)):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(source, target, "batch-key", overwrite=True)
        self.assertIs(ctx.exception, rollback_failure)
        self.assertIs(ctx.exception.__cause__, first)
        # The restore itself (3rd directory fsync) had completed, so the
        # target holds the complete old bytes.
        self._assert_reads_complete_old_version(target, old_raw)

    # -- original target absent: failure removes the new target ---------

    def test_commit_failure_with_no_prior_target_removes_new_target(self) -> None:
        source = self._write(_history_doc(), "source.history")
        target = os.path.join(self.directory, "fresh.history")
        failure = _io_error("commit directory fsync failed")
        wrapper = _fsync_wrapper(
            lambda kind, n: failure if (kind, n) == ("dir", 1) else None)
        with mock.patch("carbon_market.history_copy.os.fsync",
                        side_effect=wrapper):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(source, target, "batch-key", overwrite=True)
        self.assertIs(ctx.exception, failure)
        self.assertFalse(os.path.exists(target))
        self.assertEqual(self._attempt_files(), [])

    def test_temp_failure_with_no_prior_target_leaves_nothing(self) -> None:
        source = self._write(_history_doc(), "source.history")
        target = os.path.join(self.directory, "fresh.history")
        failure = _io_error("new temp fsync failed")
        wrapper = _fsync_wrapper(
            lambda kind, n: failure if (kind, n) == ("file", 1) else None)
        with mock.patch("carbon_market.history_copy.os.fsync",
                        side_effect=wrapper):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(source, target, "batch-key", overwrite=True)
        self.assertIs(ctx.exception, failure)
        self.assertFalse(os.path.exists(target))
        self.assertEqual(self._attempt_files(), [])

    # -- overwrite=False and success contracts ---------------------------

    def test_overwrite_false_still_raises_and_writes_nothing(self) -> None:
        source = self._write(_history_doc(), "source.history")
        target = os.path.join(self.directory, "target.history")
        with open(target, "wb") as handle:
            handle.write(b"old version")
        with self.assertRaises(FileExistsError):
            history_copy.run(source, target, "batch-key", overwrite=False)
        self.assertEqual(open(target, "rb").read(), b"old version")
        self.assertEqual(self._attempt_files(), [])

    def test_successful_overwrite_leaves_no_backup_or_temp(self) -> None:
        source = self._write(_history_doc(), "source.history")
        target, _old_raw = self._old_target()
        result = history_copy.run(source, target, "batch-key", overwrite=True)
        self.assertEqual(open(target, "rb").read(), open(source, "rb").read())
        self.assertEqual(result, history.verify(source, "batch-key"))
        self.assertEqual(list(result.keys()),
                         ["key", "count", "statuses", "terminal"])
        self.assertEqual(self._attempt_files(), [])


if __name__ == "__main__":
    unittest.main()
