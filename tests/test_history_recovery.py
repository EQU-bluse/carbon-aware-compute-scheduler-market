from __future__ import annotations

import json
import os
import time
import unittest
from multiprocessing import Process, Queue
from tempfile import TemporaryDirectory

from carbon_market import history, history_copy, history_recovery


def _event(job_id: str = "j-1", now: int = 50) -> dict[str, object]:
    return {"job_id": job_id, "source_id": "r-1", "target_id": "r-2",
            "op": "commit", "now": now}


def _coord(key: str = "batch-key", until: int = 80,
           items: dict[str, list] | None = None) -> dict[str, object]:
    return {"version": 1, "key": key, "owner": "owner-a", "until": until,
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


class HistoryRecoveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.target = os.path.join(self.tmp.name, "target.history")
        self.recovery = self.target + ".recovery"
        self.old_doc = _history_doc(snapshots=[
            ["pending", _coord(items={"j-1": [None, None]})],
        ])
        self.new_doc = _history_doc()
        self.old_bytes = json.dumps(self.old_doc).encode("utf-8") + b"\n"
        self.new_bytes = json.dumps(self.new_doc).encode("utf-8") + b"\n"
        self.assertNotEqual(self.old_bytes, self.new_bytes)

    def _write(self, path: str, payload: bytes) -> None:
        with open(path, "wb") as handle:
            handle.write(payload)

    def _leftover_temps(self) -> list[str]:
        return [name for name in os.listdir(self.tmp.name)
                if name.startswith(".history-recovery")
                and name.endswith(".tmp")]

    # -- no recovery copy: verify only ----------------------------------

    def test_without_copy_verifies_target_reports_unchanged(self) -> None:
        self._write(self.target, self.old_bytes)
        result, changed = history_recovery.restore(self.target, "batch-key")
        self.assertIs(changed, False)
        self.assertEqual(list(result.keys()),
                         ["key", "count", "statuses", "terminal"])
        self.assertEqual(result, history.verify(self.target, "batch-key"))
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["statuses"], ["pending"])
        # Nothing was written at all.
        self.assertFalse(os.path.exists(self.recovery))
        self.assertEqual(self._leftover_temps(), [])
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)

    def test_without_copy_missing_target_is_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            history_recovery.restore(self.target, "batch-key")
        self.assertFalse(os.path.exists(self.recovery))

    def test_without_copy_wrong_key_raises_key_error(self) -> None:
        self._write(self.target, self.old_bytes)
        with self.assertRaises(KeyError) as ctx:
            history_recovery.restore(self.target, "other-key")
        self.assertEqual(ctx.exception.args[0], "other-key")
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)

    def test_without_copy_malformed_target_raises_value_error(self) -> None:
        self._write(self.target, b"{not json")
        with self.assertRaises(ValueError):
            history_recovery.restore(self.target, "batch-key")

    # -- recovery copy present -------------------------------------------

    def test_identical_copy_reports_unchanged_and_removes_copy(self) -> None:
        self._write(self.target, self.old_bytes)
        self._write(self.recovery, self.old_bytes)
        result, changed = history_recovery.restore(self.target, "batch-key")
        self.assertIs(changed, False)
        self.assertEqual(result, history.verify(self.target, "batch-key"))
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        self.assertFalse(os.path.exists(self.recovery))
        self.assertEqual(self._leftover_temps(), [])

    def test_different_copy_replaces_target_byte_for_byte(self) -> None:
        self._write(self.target, self.old_bytes)
        self._write(self.recovery, self.new_bytes)
        result, changed = history_recovery.restore(self.target, "batch-key")
        self.assertIs(changed, True)
        self.assertEqual(result, history.verify(self.target, "batch-key"))
        self.assertEqual(result["count"], 2)
        self.assertEqual(result["statuses"], ["pending", "completed"])
        self.assertEqual(open(self.target, "rb").read(), self.new_bytes)
        self.assertFalse(os.path.exists(self.recovery))
        self.assertEqual(self._leftover_temps(), [])

    def test_copy_restores_missing_target(self) -> None:
        self._write(self.recovery, self.new_bytes)
        result, changed = history_recovery.restore(self.target, "batch-key")
        self.assertIs(changed, True)
        self.assertEqual(result["count"], 2)
        self.assertEqual(open(self.target, "rb").read(), self.new_bytes)
        self.assertFalse(os.path.exists(self.recovery))

    def test_copy_can_be_previewed_with_history_verify(self) -> None:
        self._write(self.target, self.old_bytes)
        self._write(self.recovery, self.new_bytes)
        # The documented preview path before deciding to restore.
        self.assertEqual(history.verify(self.recovery, "batch-key")["count"], 2)
        summary, changed = history_recovery.restore(self.target, "batch-key")
        self.assertIs(changed, True)
        self.assertEqual(summary, history.verify(self.target, "batch-key"))

    def test_bad_copy_raises_value_error_and_writes_nothing(self) -> None:
        self._write(self.target, self.old_bytes)
        self._write(self.recovery, b"{not json")
        with self.assertRaises(ValueError):
            history_recovery.restore(self.target, "batch-key")
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        self.assertEqual(open(self.recovery, "rb").read(), b"{not json")
        self.assertEqual(self._leftover_temps(), [])

    def test_copy_wrong_key_raises_key_error(self) -> None:
        self._write(self.target, self.old_bytes)
        self._write(self.recovery,
                    json.dumps(_history_doc(key="real-key")).encode())
        with self.assertRaises(KeyError) as ctx:
            history_recovery.restore(self.target, "claimed-key")
        self.assertEqual(ctx.exception.args[0], "claimed-key")
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)

    def test_missing_copy_and_missing_target_is_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            history_recovery.restore(self.target, "batch-key")

    # -- arguments --------------------------------------------------------

    def test_bad_arguments_raise_value_error(self) -> None:
        for bad in ("", 1, None, True, b"k"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    history_recovery.restore(bad, "batch-key")  # type: ignore[arg-type]
                with self.assertRaises(ValueError):
                    history_recovery.restore(self.target, bad)  # type: ignore[arg-type]
        for bad in ("after", "afterreplace", "", 0, True):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    history_recovery.restore(self.target, "batch-key", bad)  # type: ignore[arg-type]

    # -- fault injection: rollback to the pre-call state -----------------

    def _setup_old_target_and_copy(self) -> None:
        self._write(self.target, self.old_bytes)
        self._write(self.recovery, self.new_bytes)

    def test_fault_after_replace_rolls_back_target_and_copy(self) -> None:
        self._setup_old_target_and_copy()
        with self.assertRaises(OSError) as ctx:
            history_recovery.restore(
                self.target, "batch-key", fault="after_replace")
        self.assertIsNone(ctx.exception.__cause__)
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        self.assertEqual(open(self.recovery, "rb").read(), self.new_bytes)
        self.assertEqual(self._leftover_temps(), [])
        # The rolled-back state is fully usable again.
        result, changed = history_recovery.restore(self.target, "batch-key")
        self.assertIs(changed, True)
        self.assertEqual(result["count"], 2)

    def test_fault_after_unlink_recreates_copy_and_target(self) -> None:
        self._setup_old_target_and_copy()
        with self.assertRaises(OSError) as ctx:
            history_recovery.restore(
                self.target, "batch-key", fault="after_unlink")
        self.assertIsNone(ctx.exception.__cause__)
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        self.assertEqual(open(self.recovery, "rb").read(), self.new_bytes)
        self.assertEqual(self._leftover_temps(), [])

    def test_fault_after_replace_with_missing_target_removes_it_again(self) -> None:
        self._write(self.recovery, self.new_bytes)
        with self.assertRaises(OSError):
            history_recovery.restore(
                self.target, "batch-key", fault="after_replace")
        self.assertFalse(os.path.exists(self.target))
        self.assertEqual(open(self.recovery, "rb").read(), self.new_bytes)

    def test_fault_after_unlink_with_missing_target_recreates_copy(self) -> None:
        self._write(self.recovery, self.new_bytes)
        with self.assertRaises(OSError):
            history_recovery.restore(
                self.target, "batch-key", fault="after_unlink")
        self.assertFalse(os.path.exists(self.target))
        self.assertEqual(open(self.recovery, "rb").read(), self.new_bytes)

    def test_rollback_failure_chains_first_error(self) -> None:
        # The after_replace fault triggers the rollback; the rollback's
        # own directory sync then fails, so its OSError is raised chained
        # after the injected one. The restored bytes stay on disk.
        self._setup_old_target_and_copy()
        original = history_recovery._fsync_dir_plain

        def failing_sync(directory: str) -> None:
            raise OSError(99, "injected rollback sync failure")

        history_recovery._fsync_dir_plain = failing_sync
        try:
            with self.assertRaises(OSError) as ctx:
                history_recovery.restore(
                    self.target, "batch-key", fault="after_replace")
        finally:
            history_recovery._fsync_dir_plain = original
        self.assertIsNotNone(ctx.exception.__cause__)
        self.assertIn("after replace", str(ctx.exception.__cause__))
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        self.assertEqual(open(self.recovery, "rb").read(), self.new_bytes)

    # -- locking ----------------------------------------------------------

    def test_restore_blocks_behind_exclusive_target_lock(self) -> None:
        self._setup_old_target_and_copy()
        queue: Queue[str] = Queue()
        holder = Process(target=_hold_exclusive,
                         args=(self.target + ".lock", queue, 1.5))
        holder.start()
        self.addCleanup(holder.join, 5)
        self.assertEqual(queue.get(timeout=5), "locked")
        start = time.monotonic()
        _, changed = history_recovery.restore(self.target, "batch-key")
        self.assertGreaterEqual(time.monotonic() - start, 1.2)
        self.assertIs(changed, True)
        self.assertEqual(open(self.target, "rb").read(), self.new_bytes)

    # -- integration with history_copy -----------------------------------

    def test_recovers_after_failed_copy_leaving_recovery_copy(self) -> None:
        source = os.path.join(self.tmp.name, "source.history")
        self._write(source, self.new_bytes)
        self._write(self.target, self.old_bytes)
        # A pre-replace failure leaves the old target and the retained
        # fixed-path recovery copy, both holding the old bytes.
        original_fault = history_copy._fault

        def fail_temp_fsync(stage: str) -> None:
            if stage == "temp_fsync":
                raise OSError("injected temp fsync failure")

        history_copy._fault = fail_temp_fsync
        try:
            with self.assertRaises(OSError):
                history_copy.run(source, self.target, "batch-key",
                                 overwrite=True)
        finally:
            history_copy._fault = original_fault
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        self.assertEqual(open(self.recovery, "rb").read(), self.old_bytes)
        # restore consumes the copy and verifies the (unchanged) target.
        summary, changed = history_recovery.restore(self.target, "batch-key")
        self.assertIs(changed, False)
        self.assertEqual(summary["count"], 1)
        self.assertFalse(os.path.exists(self.recovery))

    def test_recovers_target_left_at_new_bytes_after_a_crash(self) -> None:
        source = os.path.join(self.tmp.name, "source.history")
        self._write(source, self.new_bytes)
        # Simulate a real crash: the replace and both directory syncs
        # landed, but the process died holding the recovery copy -- the
        # target shows the new bytes while the copy keeps the old ones.
        self._write(self.target, self.new_bytes)
        self._write(self.recovery, self.old_bytes)
        summary, changed = history_recovery.restore(self.target, "batch-key")
        self.assertIs(changed, True)
        self.assertEqual(summary["count"], 1)
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)
        self.assertFalse(os.path.exists(self.recovery))
        self.assertEqual(history.verify(self.target, "batch-key"), summary)


if __name__ == "__main__":
    unittest.main()
