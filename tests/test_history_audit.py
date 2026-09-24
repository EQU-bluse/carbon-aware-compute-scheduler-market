"""End-to-end audit integration for history_copy.run / history_recovery.restore.

These tests cover the bridge added on top of the independently tested
audit journal: the optional (audit_path, audit_key) pair, the success and
failure events appended around an otherwise unchanged copy/restore, the
校验/执行/同步/回滚 stage classification, the post-exception ``changed``
computation, idempotent replay and conflict, exception preservation and
chaining, and the strict on-disk ordering contract.
"""

from __future__ import annotations

import contextlib
import json
import os
import unittest
from tempfile import TemporaryDirectory
from unittest import mock

from carbon_market import audit, history_copy, history_recovery


def _coord(key: str = "batch-key", until: int = 80,
           items: dict | None = None) -> dict:
    return {"version": 1, "key": key, "owner": "owner-a", "until": until,
            "items": {"j-1": [None, None]} if items is None else items}


def _doc(key: str = "batch-key", snapshots: list | None = None) -> dict:
    if snapshots is None:
        snapshots = [
            ["pending", _coord(key=key)],
            ["completed", _coord(key=key, items={
                "j-1": [{"job_id": "j-1", "source_id": "r-1",
                         "target_id": "r-2", "op": "commit", "now": 50},
                        None]})],
        ]
    return {"version": 1, "key": key, "snapshots": snapshots}


def _one_snapshot_doc(key: str = "batch-key") -> dict:
    return _doc(key=key, snapshots=[["pending", _coord(key=key)]])


def _write(path: str, doc: dict) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(doc) + "\n")


@contextlib.contextmanager
def _copy_fault_at(*stages: str):
    failing = set(stages)
    original = history_copy._fault

    def inject(stage: str) -> None:
        if stage in failing:
            failing.discard(stage)
            raise OSError(5, f"injected I/O failure at {stage}")

    history_copy._fault = inject
    try:
        yield
    finally:
        history_copy._fault = original


class AuditPairTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        t = self.tmp.name
        self.source = os.path.join(t, "source.history")
        self.target = os.path.join(t, "target.history")
        self.audit_path = os.path.join(t, "audit.json")
        _write(self.source, _doc())

    def test_pair_must_be_given_together_or_as_non_empty_strings(self) -> None:
        for path, key in ((self.audit_path, None), (None, "ak"),
                          ("", "ak"), (self.audit_path, ""),
                          (1, "ak"), (self.audit_path, 2),
                          (True, "ak")):
            with self.subTest(path=path, key=key):
                with self.assertRaises(ValueError):
                    history_copy.run(self.source, self.target, "batch-key",
                                     audit_path=path, audit_key=key)  # type: ignore[arg-type]
                with self.assertRaises(ValueError):
                    history_recovery.restore(
                        self.target, "batch-key",
                        audit_path=path, audit_key=key)  # type: ignore[arg-type]
                self.assertFalse(os.path.exists(self.audit_path))

    def test_pair_checked_before_operation_starts(self) -> None:
        # Even when the operation itself is impossible, a bad pair is the
        # error surfaced and nothing runs.
        with self.assertRaises(ValueError):
            history_copy.run(self.source, self.target, "batch-key",
                             audit_path=self.audit_path)
        with self.assertRaises(ValueError):
            history_copy.run("does-not-exist", self.target, "batch-key",
                             audit_path=self.audit_path)
        self.assertFalse(os.path.exists(self.target))
        self.assertFalse(os.path.exists(self.audit_path))

    def test_omitted_pair_changes_nothing_and_creates_no_journal(self) -> None:
        result = history_copy.run(self.source, self.target, "batch-key")
        self.assertEqual(result["key"], "batch-key")
        self.assertTrue(os.path.exists(self.target))

        other = os.path.join(self.tmp.name, "restored.history")
        os.rename(self.target, other)
        summary, changed = history_recovery.restore(other, "batch-key")
        self.assertFalse(changed)
        self.assertEqual(summary["key"], "batch-key")
        self.assertFalse(os.path.exists(self.audit_path))


class CopyAuditTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        t = self.tmp.name
        self.source = os.path.join(t, "source.history")
        self.target = os.path.join(t, "target.history")
        self.audit_path = os.path.join(t, "audit.json")
        _write(self.source, _doc())

    def _event(self, key: str = "ak") -> dict:
        return audit.get(self.audit_path, key)

    def test_success_event_on_new_target_records_changed_true(self) -> None:
        result = history_copy.run(
            self.source, self.target, "batch-key",
            audit_path=self.audit_path, audit_key="ak")
        self.assertEqual(result["key"], "batch-key")
        event = self._event()
        self.assertEqual(list(event.keys()),
                         ["op", "target", "key", "changed", "error", "stage"])
        self.assertEqual(event, {"op": "copy", "target": self.target,
                                 "key": "batch-key", "changed": True,
                                 "error": None, "stage": None})

    def test_target_is_the_passed_in_string_not_the_realpath(self) -> None:
        quirky = os.path.join(self.tmp.name, "sub", "..", "quirky.history")
        history_copy.run(self.source, quirky, "batch-key",
                         audit_path=self.audit_path, audit_key="ak")
        self.assertEqual(self._event()["target"], quirky)

    def test_overwrite_of_identical_bytes_records_changed_false(self) -> None:
        _write(self.target, _doc())
        history_copy.run(self.source, self.target, "batch-key",
                         overwrite=True, audit_path=self.audit_path,
                         audit_key="ak")
        self.assertIs(self._event()["changed"], False)

    def test_overwrite_of_different_bytes_records_changed_true(self) -> None:
        _write(self.target, _one_snapshot_doc())
        history_copy.run(self.source, self.target, "batch-key",
                         overwrite=True, audit_path=self.audit_path,
                         audit_key="ak")
        self.assertIs(self._event()["changed"], True)

    def test_idempotent_replay_writes_nothing_and_keeps_result(self) -> None:
        first = history_copy.run(
            self.source, self.target, "batch-key",
            audit_path=self.audit_path, audit_key="ak")
        raw_after_first = open(self.audit_path, "rb").read()
        os.unlink(self.target)
        second = history_copy.run(
            self.source, self.target, "batch-key",
            audit_path=self.audit_path, audit_key="ak")
        self.assertEqual(second, first)
        # The replay appended nothing even though the second copy created
        # the target again: one event, one trailing newline.
        self.assertEqual(open(self.audit_path, "rb").read(), raw_after_first)

    def test_missing_source_is_a_validation_failure(self) -> None:
        missing = os.path.join(self.tmp.name, "missing.history")
        with self.assertRaises(FileNotFoundError):
            history_copy.run(missing, self.target, "batch-key",
                             audit_path=self.audit_path, audit_key="ak")
        event = self._event()
        self.assertEqual(event["error"], "FileNotFoundError")
        self.assertEqual(event["stage"], "校验")
        self.assertIs(event["changed"], False)
        self.assertFalse(os.path.exists(self.target))

    def test_wrong_history_key_is_a_validation_failure(self) -> None:
        with self.assertRaises(KeyError):
            history_copy.run(self.source, self.target, "claimed",
                             audit_path=self.audit_path, audit_key="ak")
        event = self._event()
        self.assertEqual(event["error"], "KeyError")
        self.assertEqual(event["stage"], "校验")

    def test_malformed_source_is_a_validation_failure(self) -> None:
        bad = os.path.join(self.tmp.name, "bad.history")
        with open(bad, "w", encoding="utf-8") as handle:
            handle.write("{not json")
        with self.assertRaises(ValueError):
            history_copy.run(bad, self.target, "batch-key",
                             audit_path=self.audit_path, audit_key="ak")
        self.assertEqual(self._event()["stage"], "校验")
        self.assertEqual(self._event()["error"], "ValueError")

    def test_existing_target_is_an_execution_failure(self) -> None:
        _write(self.target, _doc())
        with self.assertRaises(FileExistsError):
            history_copy.run(self.source, self.target, "batch-key",
                             audit_path=self.audit_path, audit_key="ak")
        event = self._event()
        self.assertEqual(event["error"], "FileExistsError")
        self.assertEqual(event["stage"], "执行")
        self.assertIs(event["changed"], False)

    def test_leftover_recovery_is_an_execution_failure(self) -> None:
        _write(self.target, _doc())
        with open(self.target + ".recovery", "wb") as handle:
            handle.write(b"stale")
        with self.assertRaises(FileExistsError):
            history_copy.run(self.source, self.target, "batch-key",
                             overwrite=True, audit_path=self.audit_path,
                             audit_key="ak")
        event = self._event()
        self.assertEqual(event["stage"], "执行")
        self.assertEqual(event["error"], "FileExistsError")

    def test_first_io_failure_is_a_sync_failure_with_target_restored(self) -> None:
        _write(self.target, _doc(key="other"))
        with _copy_fault_at("temp_fsync"):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(self.source, self.target, "batch-key",
                                 overwrite=True, audit_path=self.audit_path,
                                 audit_key="ak")
        self.assertIsNone(ctx.exception.__cause__)
        event = self._event()
        self.assertEqual(event["error"], "OSError")
        self.assertEqual(event["stage"], "同步")
        # The failed copy rolled the target back: no departure at exit.
        self.assertIs(event["changed"], False)

    def test_failed_rollback_is_a_rollback_failure_and_target_departed(self) -> None:
        _write(self.target, _doc(key="other"))
        with _copy_fault_at("commit_dir_fsync", "rollback_replace"):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(self.source, self.target, "batch-key",
                                 overwrite=True, audit_path=self.audit_path,
                                 audit_key="ak")
        self.assertIsNotNone(ctx.exception.__cause__)
        event = self._event()
        self.assertEqual(event["stage"], "回滚")
        # Moving the recovery file back failed, so the target is left
        # holding the new bytes: a real departure from the call-before.
        self.assertIs(event["changed"], True)
        self.assertTrue(os.path.exists(self.target + ".recovery"))

    def test_new_target_rollback_failure_reports_departure(self) -> None:
        with _copy_fault_at("commit_dir_fsync", "rollback_unlink"):
            with self.assertRaises(OSError):
                history_copy.run(self.source, self.target, "batch-key",
                                 audit_path=self.audit_path, audit_key="ak")
        event = self._event()
        self.assertEqual(event["stage"], "回滚")
        self.assertIs(event["changed"], True)

    def test_failure_event_is_idempotent_and_original_error_kept(self) -> None:
        missing = os.path.join(self.tmp.name, "missing.history")
        for _ in range(2):
            with self.assertRaises(FileNotFoundError) as ctx:
                history_copy.run(missing, self.target, "batch-key",
                                 audit_path=self.audit_path, audit_key="ak")
            self.assertIsNone(ctx.exception.__cause__)
        raw = open(self.audit_path, "rb").read()
        self.assertEqual(raw.count(b'"ak"'), 1)

    def test_audit_conflict_after_success_raises_keeps_history_result(self) -> None:
        # Pre-seed the audit key with a different event.
        audit.record(self.audit_path, "ak",
                     {"op": "restore", "target": "elsewhere",
                      "key": "batch-key", "changed": False,
                      "error": None, "stage": None})
        with self.assertRaises(ValueError):
            history_copy.run(self.source, self.target, "batch-key",
                             audit_path=self.audit_path, audit_key="ak")
        # The copy itself succeeded; only the journal rejected the event.
        self.assertEqual(open(self.target, "rb").read(),
                         open(self.source, "rb").read())

    def test_audit_conflict_after_failure_chains_audit_after_operation(self) -> None:
        audit.record(self.audit_path, "ak",
                     {"op": "restore", "target": "elsewhere",
                      "key": "batch-key", "changed": False,
                      "error": None, "stage": None})
        missing = os.path.join(self.tmp.name, "missing.history")
        with self.assertRaises(ValueError) as ctx:
            history_copy.run(missing, self.target, "batch-key",
                             audit_path=self.audit_path, audit_key="ak")
        # The audit failure is what surfaces; the operation error is its
        # explicit cause, and that operation error keeps its own shape.
        self.assertIsInstance(ctx.exception.__cause__, FileNotFoundError)


class RestoreAuditTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        t = self.tmp.name
        self.target = os.path.join(t, "target.history")
        self.recovery = self.target + ".recovery"
        self.audit_path = os.path.join(t, "audit.json")
        self.old = _one_snapshot_doc()
        self.new = _doc()
        _write(self.target, self.old)

    def _write_recovery(self, doc: dict | None = None) -> None:
        _write(self.recovery, self.new if doc is None else doc)

    def _event(self, key: str = "ak") -> dict:
        return audit.get(self.audit_path, key)

    def test_success_without_recovery_records_changed_false(self) -> None:
        summary, changed = history_recovery.restore(
            self.target, "batch-key",
            audit_path=self.audit_path, audit_key="ak")
        self.assertFalse(changed)
        self.assertEqual(summary["key"], "batch-key")
        self.assertEqual(self._event(),
                         {"op": "restore", "target": self.target,
                          "key": "batch-key", "changed": False,
                          "error": None, "stage": None})

    def test_success_with_recovery_records_changed_true(self) -> None:
        self._write_recovery()
        summary, changed = history_recovery.restore(
            self.target, "batch-key",
            audit_path=self.audit_path, audit_key="ak")
        self.assertTrue(changed)
        self.assertEqual(summary["key"], "batch-key")
        event = self._event()
        self.assertEqual(event["op"], "restore")
        self.assertIs(event["changed"], True)
        self.assertIsNone(event["error"])
        self.assertIsNone(event["stage"])

    def test_identical_recovery_records_changed_false(self) -> None:
        self._write_recovery(self.old)
        _, changed = history_recovery.restore(
            self.target, "batch-key",
            audit_path=self.audit_path, audit_key="ak")
        self.assertFalse(changed)
        self.assertFalse(self._event()["changed"])

    def test_missing_target_is_a_validation_failure(self) -> None:
        os.unlink(self.target)
        with self.assertRaises(FileNotFoundError):
            history_recovery.restore(
                self.target, "batch-key",
                audit_path=self.audit_path, audit_key="ak")
        event = self._event()
        self.assertEqual(event["error"], "FileNotFoundError")
        self.assertEqual(event["stage"], "校验")

    def test_wrong_key_is_a_validation_failure(self) -> None:
        with self.assertRaises(KeyError):
            history_recovery.restore(
                self.target, "claimed",
                audit_path=self.audit_path, audit_key="ak")
        event = self._event()
        self.assertEqual(event["error"], "KeyError")
        self.assertEqual(event["stage"], "校验")

    def test_malformed_recovery_is_a_validation_failure(self) -> None:
        with open(self.recovery, "wb") as handle:
            handle.write(b"{not json")
        with self.assertRaises(ValueError):
            history_recovery.restore(
                self.target, "batch-key",
                audit_path=self.audit_path, audit_key="ak")
        event = self._event()
        self.assertEqual(event["stage"], "校验")
        self.assertEqual(event["error"], "ValueError")

    def test_injected_fault_is_a_sync_failure_fully_rolled_back(self) -> None:
        self._write_recovery()
        with self.assertRaises(OSError) as ctx:
            history_recovery.restore(
                self.target, "batch-key", fault="after_replace",
                audit_path=self.audit_path, audit_key="ak")
        self.assertIsNone(ctx.exception.__cause__)
        event = self._event()
        self.assertEqual(event["error"], "OSError")
        self.assertEqual(event["stage"], "同步")
        self.assertIs(event["changed"], False)

    def test_failed_rollback_is_a_rollback_failure_with_target_departed(self) -> None:
        self._write_recovery()
        real_replace = os.replace
        calls = {"n": 0}

        def flaky_replace(src, dst):
            calls["n"] += 1
            if calls["n"] == 2:  # forward target, then rollback target
                raise OSError(77, "injected rollback replace failure")
            return real_replace(src, dst)

        with mock.patch.object(history_recovery.os, "replace", flaky_replace):
            with self.assertRaises(OSError) as ctx:
                history_recovery.restore(
                    self.target, "batch-key", fault="after_replace",
                    audit_path=self.audit_path, audit_key="ak")
        self.assertIsNotNone(ctx.exception.__cause__)
        event = self._event()
        self.assertEqual(event["stage"], "回滚")
        self.assertIs(event["changed"], True)

    def test_audit_conflict_chains_after_operation_failure(self) -> None:
        audit.record(self.audit_path, "ak",
                     {"op": "copy", "target": "elsewhere",
                      "key": "batch-key", "changed": True,
                      "error": None, "stage": None})
        with self.assertRaises(ValueError) as ctx:
            history_recovery.restore(
                self.target, "claimed",
                audit_path=self.audit_path, audit_key="ak")
        self.assertIsInstance(ctx.exception.__cause__, KeyError)


class AuditJournalContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        t = self.tmp.name
        self.source = os.path.join(t, "source.history")
        self.target = os.path.join(t, "copy.history")
        self.audit_path = os.path.join(t, "audit.json")
        _write(self.source, _doc())

    def test_events_sorted_compact_utf8_single_trailing_newline(self) -> None:
        for audit_key in ("k2", "k1", "中"):
            target = os.path.join(self.tmp.name, f"out-{audit_key}.history")
            history_copy.run(self.source, target, "batch-key",
                             audit_path=self.audit_path, audit_key=audit_key)
        raw = open(self.audit_path, "rb").read()
        # Code-point order: k1, k2, then the CJK key; compact separators;
        # non-ASCII written through; exactly one terminating newline.
        self.assertIn("中".encode("utf-8"), raw)
        self.assertNotIn(b"\\u4e2d", raw)
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        self.assertNotIn(b", ", raw)
        self.assertNotIn(b": ", raw)
        text = raw.decode("utf-8")
        keys = [match for match in ("k1", "k2", "中")]
        positions = [text.index(f'"{k}":{{') for k in keys]
        self.assertEqual(positions, sorted(positions))

    def _disorder(self, payload: str) -> None:
        with open(self.audit_path, "w", encoding="utf-8") as handle:
            handle.write(payload)

    def test_out_of_order_events_member_rejected_and_not_rewritten(self) -> None:
        self._disorder(
            '{"version":1,"events":{"b":{"op":"copy","target":"t","key":"k",'
            '"changed":true,"error":null,"stage":null},'
            '"a":{"op":"copy","target":"t","key":"k","changed":true,'
            '"error":null,"stage":null}}}\n')
        raw_before = open(self.audit_path, "rb").read()
        with self.assertRaises(ValueError):
            audit.get(self.audit_path, "a")
        # The read-only query must not repair the file.
        self.assertEqual(open(self.audit_path, "rb").read(), raw_before)
        # record() (the copy/restore bridge) rejects it too.
        with self.assertRaises(ValueError):
            history_copy.run(self.source, self.target, "batch-key",
                             audit_path=self.audit_path, audit_key="c")

    def test_out_of_order_root_fields_rejected(self) -> None:
        self._disorder(
            '{"events":{},"version":1}\n')
        with self.assertRaises(ValueError):
            audit.get(self.audit_path, "a")

    def test_out_of_order_event_fields_rejected(self) -> None:
        self._disorder(
            '{"version":1,"events":{"a":{"key":"k","op":"copy","target":"t",'
            '"changed":true,"error":null,"stage":null}}}\n')
        with self.assertRaises(ValueError):
            audit.get(self.audit_path, "a")

    def test_well_ordered_file_is_accepted_and_preserved(self) -> None:
        payload = ('{"version":1,"events":{"a":{"op":"copy","target":"t",'
                   '"key":"k","changed":true,"error":null,"stage":null}}}\n')
        self._disorder(payload)
        event = audit.get(self.audit_path, "a")
        self.assertEqual(event["op"], "copy")
        # A read leaves the bytes untouched.
        self.assertEqual(open(self.audit_path, "rb").read().decode("utf-8"),
                         payload)


class AuditConcurrencyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        t = self.tmp.name
        self.source = os.path.join(t, "source.history")
        self.audit_path = os.path.join(t, "audit.json")
        _write(self.source, _doc())

    def test_concurrent_copies_only_leave_a_complete_ordered_journal(self) -> None:
        import threading

        errors: list[BaseException] = []

        def worker(index: int) -> None:
            try:
                history_copy.run(
                    self.source,
                    os.path.join(self.tmp.name, f"t{index}.history"),
                    "batch-key", audit_path=self.audit_path,
                    audit_key=f"key-{index:03d}")
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        self.assertEqual(errors, [])
        # The journal is complete (all keys) and code-point ordered.
        raw = open(self.audit_path, "rb").read()
        document = json.loads(raw)
        self.assertEqual(len(document["events"]), 12)
        self.assertEqual(list(document["events"].keys()),
                         sorted(document["events"]))
        for i in range(12):
            event = audit.get(self.audit_path, f"key-{i:03d}")
            self.assertEqual(event["op"], "copy")


if __name__ == "__main__":
    unittest.main()
