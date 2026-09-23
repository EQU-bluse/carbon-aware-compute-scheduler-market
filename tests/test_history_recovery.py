from __future__ import annotations

import json
import os
import time
import unittest
from multiprocessing import Process, Queue
from tempfile import TemporaryDirectory

from carbon_market import history, history_recovery


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


class RestoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.target = os.path.join(self.tmp.name, "target.history")
        self.recovery = self.target + ".recovery"
        self.old_doc = _history_doc(snapshots=[
            ["pending", _coord(items={"j-1": [None, None]})],
        ])
        self.old_bytes = json.dumps(self.old_doc).encode("utf-8") + b"\n"
        self.new_doc = _history_doc()
        self.new_bytes = json.dumps(self.new_doc).encode("utf-8") + b"\n"
        self.assertNotEqual(self.old_bytes, self.new_bytes)
        with open(self.target, "wb") as handle:
            handle.write(self.old_bytes)

    def _write_recovery(self, payload: bytes | None = None) -> None:
        with open(self.recovery, "wb") as handle:
            handle.write(self.new_bytes if payload is None else payload)

    def _leftovers(self) -> list[str]:
        return [name for name in os.listdir(self.tmp.name)
                if name.endswith(".tmp")]

    def _assert_lock_free(self) -> None:
        import fcntl
        fd = os.open(self.target + ".lock", os.O_CREAT | os.O_RDWR, 0o666)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    # -- no recovery file -------------------------------------------------

    def test_without_recovery_verifies_target_and_reports_unchanged(self) -> None:
        before = open(self.target, "rb").read()
        summary, changed = history_recovery.restore(self.target, "batch-key")
        self.assertEqual(list(summary.keys()),
                         ["key", "count", "statuses", "terminal"])
        self.assertEqual(summary, history.verify(self.target, "batch-key"))
        self.assertEqual(summary["count"], 1)
        self.assertEqual(summary["statuses"], ["pending"])
        self.assertIs(summary["terminal"], False)
        self.assertIs(changed, False)
        self.assertEqual(open(self.target, "rb").read(), before)
        self.assertFalse(os.path.exists(self.recovery))
        self.assertEqual(self._leftovers(), [])

    def test_without_recovery_missing_target_raises_file_not_found(self) -> None:
        os.unlink(self.target)
        with self.assertRaises(FileNotFoundError):
            history_recovery.restore(self.target, "batch-key")
        self.assertFalse(os.path.exists(self.target))
        self.assertFalse(os.path.exists(self.recovery))

    def test_without_recovery_wrong_key_raises_key_error(self) -> None:
        with self.assertRaises(KeyError) as ctx:
            history_recovery.restore(self.target, "other-key")
        self.assertEqual(ctx.exception.args[0], "other-key")
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)

    def test_without_recovery_bad_bytes_raise_value_error(self) -> None:
        with open(self.target, "wb") as handle:
            handle.write(b"{not json")
        with self.assertRaises(ValueError):
            history_recovery.restore(self.target, "batch-key")
        self.assertEqual(self._leftovers(), [])

    # -- recovery present: happy path ------------------------------------

    def test_recovery_replaces_target_byte_for_byte_and_is_removed(self) -> None:
        self._write_recovery()
        summary, changed = history_recovery.restore(self.target, "batch-key")
        self.assertEqual(summary, history.verify(self.target, "batch-key"))
        self.assertEqual(summary["count"], 2)
        self.assertEqual(summary["statuses"], ["pending", "completed"])
        self.assertIs(summary["terminal"], True)
        self.assertIs(changed, True)
        self.assertEqual(open(self.target, "rb").read(), self.new_bytes)
        self.assertFalse(os.path.exists(self.recovery))
        self.assertEqual(self._leftovers(), [])
        self._assert_lock_free()

    def test_identical_recovery_reports_unchanged_and_is_still_removed(self) -> None:
        self._write_recovery(self.old_bytes)
        summary, changed = history_recovery.restore(self.target, "batch-key")
        self.assertIs(changed, False)
        self.assertEqual(summary["count"], 1)
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        self.assertFalse(os.path.exists(self.recovery))

    def test_recovery_restores_onto_missing_target(self) -> None:
        os.unlink(self.target)
        self._write_recovery()
        summary, changed = history_recovery.restore(self.target, "batch-key")
        self.assertEqual(summary, history.verify(self.target, "batch-key"))
        self.assertIs(changed, True)
        self.assertEqual(open(self.target, "rb").read(), self.new_bytes)
        self.assertFalse(os.path.exists(self.recovery))

    def test_recovery_file_is_previewable_with_verify(self) -> None:
        self._write_recovery()
        preview = history.verify(self.recovery, "batch-key")
        self.assertEqual(preview["count"], 2)
        summary, _ = history_recovery.restore(self.target, "batch-key")
        self.assertEqual(summary, preview)

    # -- validation follows verify ---------------------------------------

    def test_malformed_recovery_raises_value_error_and_writes_nothing(self) -> None:
        target_before = open(self.target, "rb").read()
        self._write_recovery(b"{not json")
        with self.assertRaises(ValueError):
            history_recovery.restore(self.target, "batch-key")
        self.assertEqual(open(self.target, "rb").read(), target_before)
        self.assertEqual(open(self.recovery, "rb").read(), b"{not json")
        self.assertEqual(self._leftovers(), [])

    def test_bad_utf8_recovery_raises_value_error_and_writes_nothing(self) -> None:
        target_before = open(self.target, "rb").read()
        self._write_recovery(
            b'{"version":1,"key":"batch-key","snapshots":[]}\xff')
        with self.assertRaises(ValueError):
            history_recovery.restore(self.target, "batch-key")
        self.assertEqual(open(self.target, "rb").read(), target_before)
        self.assertEqual(self._leftovers(), [])

    def test_inconsistent_recovery_raises_value_error_and_writes_nothing(self) -> None:
        doc = _history_doc()
        doc["snapshots"][0][0] = "completed"
        self._write_recovery(json.dumps(doc).encode("utf-8") + b"\n")
        with self.assertRaises(ValueError):
            history_recovery.restore(self.target, "batch-key")
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)

    def test_wrong_key_recovery_raises_key_error_and_writes_nothing(self) -> None:
        self._write_recovery()
        with self.assertRaises(KeyError) as ctx:
            history_recovery.restore(self.target, "claimed-key")
        self.assertEqual(ctx.exception.args[0], "claimed-key")
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        self.assertEqual(open(self.recovery, "rb").read(), self.new_bytes)

    # -- arguments --------------------------------------------------------

    def test_bad_arguments_raise_value_error_without_touching_files(self) -> None:
        for bad in ("", 1, None, True, b"k"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    history_recovery.restore(bad, "batch-key")  # type: ignore[arg-type]
                with self.assertRaises(ValueError):
                    history_recovery.restore(self.target, bad)  # type: ignore[arg-type]
        for bad in ("", "after", "before_replace", 0, True):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    history_recovery.restore(self.target, "batch-key", bad)  # type: ignore[arg-type]
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        self.assertFalse(os.path.exists(self.recovery))

    # -- fault injection --------------------------------------------------

    def test_after_replace_failure_rolls_back_target_keeps_recovery(self) -> None:
        self._write_recovery()
        with self.assertRaises(OSError) as ctx:
            history_recovery.restore(
                self.target, "batch-key", fault="after_replace")
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIn("after replace", str(ctx.exception))
        # Call-before state of both paths, synced, lock released.
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        self.assertEqual(open(self.recovery, "rb").read(), self.new_bytes)
        self.assertEqual(self._leftovers(), [])
        self.assertEqual(history.verify(self.target, "batch-key")["count"], 1)
        self._assert_lock_free()
        # A plain retry after the simulated fault succeeds.
        summary, changed = history_recovery.restore(self.target, "batch-key")
        self.assertIs(changed, True)
        self.assertEqual(summary["count"], 2)
        self.assertFalse(os.path.exists(self.recovery))

    def test_after_replace_failure_with_missing_target_removes_new_file(self) -> None:
        os.unlink(self.target)
        self._write_recovery()
        with self.assertRaises(OSError):
            history_recovery.restore(
                self.target, "batch-key", fault="after_replace")
        self.assertFalse(os.path.exists(self.target))
        self.assertEqual(open(self.recovery, "rb").read(), self.new_bytes)

    def test_after_unlink_failure_restores_both_files(self) -> None:
        self._write_recovery()
        with self.assertRaises(OSError) as ctx:
            history_recovery.restore(
                self.target, "batch-key", fault="after_unlink")
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIn("after unlink", str(ctx.exception))
        # The recovery file was already unlinked; the rollback rebuilds it
        # and puts the old target bytes back.
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        self.assertEqual(open(self.recovery, "rb").read(), self.new_bytes)
        self.assertEqual(self._leftovers(), [])
        self._assert_lock_free()

    def test_after_unlink_failure_with_missing_target_restores_absence(self) -> None:
        os.unlink(self.target)
        self._write_recovery()
        with self.assertRaises(OSError):
            history_recovery.restore(
                self.target, "batch-key", fault="after_unlink")
        self.assertFalse(os.path.exists(self.target))
        self.assertEqual(open(self.recovery, "rb").read(), self.new_bytes)

    def test_rollback_target_failure_chains_after_first(self) -> None:
        # The forward replace lands, the after_replace fault fires, and the
        # rollback's own replace (putting the old target back) fails too:
        # the rollback OSError is raised chained after the first one; the
        # target holds the new bytes while the recovery file survives.
        from unittest import mock
        self._write_recovery()
        real_replace = os.replace
        calls = {"n": 0}

        def flaky_replace(src, dst):
            calls["n"] += 1
            if calls["n"] == 2:
                raise OSError(77, "injected rollback replace failure")
            return real_replace(src, dst)

        with mock.patch.object(history_recovery.os, "replace", flaky_replace):
            with self.assertRaises(OSError) as ctx:
                history_recovery.restore(
                    self.target, "batch-key", fault="after_replace")
        self.assertIsNotNone(ctx.exception.__cause__)
        self.assertIn("after replace", str(ctx.exception.__cause__))
        self.assertIn("rollback", str(ctx.exception))
        self.assertEqual(open(self.target, "rb").read(), self.new_bytes)
        self.assertEqual(open(self.recovery, "rb").read(), self.new_bytes)
        self._assert_lock_free()

    def test_rollback_recovery_rebuild_failure_chains_after_first(self) -> None:
        # after_unlink fault: the target is restored, but rebuilding the
        # unlinked recovery file fails; the chained OSError still surfaces
        # with the old bytes safely back on the target.
        from unittest import mock
        self._write_recovery()
        real_replace = os.replace
        calls = {"n": 0}

        def flaky_replace(src, dst):
            calls["n"] += 1
            # #1 forward target, #2 rollback target, #3 rollback recovery.
            if calls["n"] == 3:
                raise OSError(78, "injected recovery rebuild failure")
            return real_replace(src, dst)

        with mock.patch.object(history_recovery.os, "replace", flaky_replace):
            with self.assertRaises(OSError) as ctx:
                history_recovery.restore(
                    self.target, "batch-key", fault="after_unlink")
        self.assertIsNotNone(ctx.exception.__cause__)
        self.assertIn("after unlink", str(ctx.exception.__cause__))
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        self.assertFalse(os.path.exists(self.recovery))

    # -- locking ----------------------------------------------------------

    def test_restore_blocks_behind_exclusive_target_lock(self) -> None:
        self._write_recovery()
        queue: Queue[str] = Queue()
        holder = Process(target=_hold_exclusive,
                         args=(self.target + ".lock", queue, 1.5))
        holder.start()
        self.addCleanup(holder.join, 5)
        self.assertEqual(queue.get(timeout=5), "locked")
        start = time.monotonic()
        history_recovery.restore(self.target, "batch-key")
        self.assertGreaterEqual(time.monotonic() - start, 1.2)
        self.assertEqual(open(self.target, "rb").read(), self.new_bytes)
        self.assertFalse(os.path.exists(self.recovery))

    def test_lock_failure_raises_oserror_and_writes_nothing(self) -> None:
        real_lock = history_recovery._recover_all._history_file_lock

        def locked_out(realpath, *, shared=False):
            raise OSError(77, "injected target lock failure")

        history_recovery._recover_all._history_file_lock = locked_out
        try:
            with self.assertRaises(OSError):
                history_recovery.restore(self.target, "batch-key")
        finally:
            history_recovery._recover_all._history_file_lock = real_lock
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        self.assertFalse(os.path.exists(self.recovery))
        self.assertEqual(self._leftovers(), [])


if __name__ == "__main__":
    unittest.main()
