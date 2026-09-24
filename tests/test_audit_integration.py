"""End-to-end audit integration for history_copy.run and restore.

Covers the optional audit_path/audit_key pair on both operations: the
pair validation, the success/failure event contract (op, target as
passed, history key, real changed result, exception class and stage),
the strict read order of the audit journal, idempotent replay and
conflict, the compact UTF-8 encoding, exception preservation and
chaining, and the unchanged behavior when auditing is omitted.
"""

from __future__ import annotations

import contextlib
import json
import os
import threading
import unittest
from tempfile import TemporaryDirectory

from carbon_market import audit, history, history_copy, history_recovery


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


def _io_error(stage: str) -> OSError:
    return OSError(5, f"injected I/O failure at {stage}")


@contextlib.contextmanager
def _fault_at(*stages: str):
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


class AuditJournalOrderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.jsonl")
        self.event = {
            "op": "copy", "target": "t.history", "key": "batch-key",
            "changed": True, "error": None, "stage": None,
        }

    def test_record_uses_compact_utf8_sorted_keys_and_trailing_newline(self) -> None:
        audit.record(self.journal, "z-key", self.event)
        raw = open(self.journal, "rb").read()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        # Compact separators, non-ASCII written through, root field order.
        self.assertIn(b'"version":1,"events"', raw)
        self.assertNotIn(b"\\u6821", raw)
        doc = json.loads(raw)
        self.assertEqual(list(doc), ["version", "events"])
        self.assertEqual(list(doc["events"]["z-key"]),
                         ["op", "target", "key", "changed", "error", "stage"])

    def test_events_are_sorted_by_audit_key_code_point(self) -> None:
        audit.record(self.journal, "z", self.event)
        audit.record(self.journal, "a", self.event)
        audit.record(self.journal, "m", self.event)
        raw = open(self.journal, "r", encoding="utf-8").read()
        self.assertEqual(list(json.loads(raw)["events"]), ["a", "m", "z"])
        # The serialized order itself is sorted, not just the parse view.
        self.assertLess(raw.index('"a"'), raw.index('"m"'))
        self.assertLess(raw.index('"m"'), raw.index('"z"'))

    def test_failure_event_persists_chinese_stage_as_utf8(self) -> None:
        failed = dict(self.event, changed=False, error="ValueError",
                      stage="校验")
        audit.record(self.journal, "k", failed)
        raw = open(self.journal, "rb").read()
        self.assertIn("校验".encode("utf-8"), raw)
        got = audit.get(self.journal, "k")
        self.assertEqual(got["stage"], "校验")
        self.assertEqual(got["error"], "ValueError")

    def test_get_rejects_root_fields_out_of_order_without_rewriting(self) -> None:
        text = ('{"events":{},"version":1}')
        with open(self.journal, "w", encoding="utf-8") as handle:
            handle.write(text)
        with self.assertRaises(ValueError):
            audit.get(self.journal, "anything")
        # A read-only query never rewrites the malformed-order file.
        self.assertEqual(open(self.journal, "r", encoding="utf-8").read(),
                         text)

    def test_get_rejects_event_fields_out_of_order_without_rewriting(self) -> None:
        swapped = ('{"op":"copy","target":"t.history","changed":true,'
                   '"key":"batch-key","error":null,"stage":null}')
        text = '{"version":1,"events":{"k":' + swapped + "}}"
        with open(self.journal, "w", encoding="utf-8") as handle:
            handle.write(text)
        with self.assertRaises(ValueError):
            audit.get(self.journal, "k")
        self.assertEqual(open(self.journal, "r", encoding="utf-8").read(),
                         text)

    def test_get_rejects_events_out_of_code_point_order(self) -> None:
        doc = {"version": 1, "events": {"b": self.event, "a": self.event}}
        text = json.dumps(doc, separators=(",", ":"))
        with open(self.journal, "w", encoding="utf-8") as handle:
            handle.write(text)
        with self.assertRaises(ValueError):
            audit.get(self.journal, "a")
        # Not silently sorted and accepted, and not rewritten either.
        self.assertEqual(open(self.journal, "r", encoding="utf-8").read(),
                         text)
        with self.assertRaises(ValueError):
            audit.record(self.journal, "c", self.event)

    def test_replay_identical_event_writes_nothing_conflict_raises(self) -> None:
        _, created = audit.record(self.journal, "k", self.event)
        self.assertIs(created, True)
        _, replayed = audit.record(self.journal, "k", self.event)
        self.assertIs(replayed, False)
        with self.assertRaises(ValueError):
            audit.record(self.journal, "k", dict(self.event, changed=False))


class _CopyFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        d = self.tmp.name
        self.source = os.path.join(d, "source.history")
        self.target = os.path.join(d, "target.history")
        self.journal = os.path.join(d, "audit.json")
        with open(self.source, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(_history_doc()) + "\n")
        self.source_bytes = open(self.source, "rb").read()
        self.single = json.dumps(_history_doc(snapshots=[
            ["pending", _coord(items={"j-1": [None, None]})],
        ])).encode("utf-8") + b"\n"

    def _event(self, audit_key: str = "ak") -> dict[str, object]:
        return audit.get(self.journal, audit_key)


class HistoryCopyAuditTest(_CopyFixture):
    # -- argument pair ----------------------------------------------------

    def test_pair_must_be_given_together_before_operation(self) -> None:
        for kwargs in (
            {"audit_path": self.journal},
            {"audit_key": "ak"},
            {"audit_path": self.journal, "audit_key": ""},
            {"audit_path": "", "audit_key": "ak"},
            {"audit_path": 1, "audit_key": "ak"},
            {"audit_path": self.journal, "audit_key": True},
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    history_copy.run(self.source, self.target, "batch-key",
                                     **kwargs)
        # The pair check runs before the operation starts: no target and
        # no journal are created even when the operation would otherwise
        # fail for its own reasons.
        self.assertFalse(os.path.exists(self.target))
        self.assertFalse(os.path.exists(self.journal))

    def test_without_pair_no_journal_and_behavior_unchanged(self) -> None:
        result = history_copy.run(self.source, self.target, "batch-key")
        self.assertEqual(result["key"], "batch-key")
        self.assertFalse(os.path.exists(self.journal))
        self.assertEqual(open(self.target, "rb").read(), self.source_bytes)

    # -- success events ---------------------------------------------------

    def test_success_event_on_new_target(self) -> None:
        result = history_copy.run(
            self.source, self.target, "batch-key",
            audit_path=self.journal, audit_key="ak-1")
        event = self._event("ak-1")
        self.assertEqual(event, {
            "op": "copy",
            "target": self.target,
            "key": "batch-key",
            "changed": True,
            "error": None,
            "stage": None,
        })
        self.assertEqual(result["key"], "batch-key")

    def test_success_event_changed_false_when_bytes_identical(self) -> None:
        with open(self.target, "wb") as handle:
            handle.write(self.source_bytes)
        history_copy.run(
            self.source, self.target, "batch-key", overwrite=True,
            audit_path=self.journal, audit_key="ak")
        self.assertIs(self._event()["changed"], False)

    def test_target_is_recorded_as_passed_not_realpath(self) -> None:
        relative = os.path.join(os.path.relpath(
            self.tmp.name, os.getcwd()), "via-rel.history")
        history_copy.run(
            self.source, relative, "batch-key",
            audit_path=self.journal, audit_key="ak")
        self.assertEqual(self._event()["target"], relative)

    def test_success_replay_is_idempotent_and_returns_same_summary(self) -> None:
        first = history_copy.run(
            self.source, self.target, "batch-key",
            audit_path=self.journal, audit_key="ak")
        os.unlink(self.target)
        second = history_copy.run(
            self.source, self.target, "batch-key",
            audit_path=self.journal, audit_key="ak")
        # The replay returns the same summary even though the bytes were
        # copied again; the journal keeps exactly one event for the key.
        self.assertEqual(first, second)
        doc = json.loads(open(self.journal, encoding="utf-8").read())
        self.assertEqual(list(doc["events"]), ["ak"])

    # -- validation stage -------------------------------------------------

    def test_missing_source_is_validation_stage(self) -> None:
        missing = os.path.join(self.tmp.name, "missing.history")
        with self.assertRaises(FileNotFoundError):
            history_copy.run(
                missing, self.target, "batch-key",
                audit_path=self.journal, audit_key="ak")
        event = self._event()
        self.assertEqual(event["error"], "FileNotFoundError")
        self.assertEqual(event["stage"], "校验")
        self.assertIs(event["changed"], False)

    def test_wrong_history_key_is_validation_stage(self) -> None:
        with self.assertRaises(KeyError):
            history_copy.run(
                self.source, self.target, "claimed-key",
                audit_path=self.journal, audit_key="ak")
        event = audit.get(self.journal, "ak")
        self.assertEqual(event["error"], "KeyError")
        self.assertEqual(event["stage"], "校验")
        self.assertEqual(event["key"], "claimed-key")

    def test_malformed_history_is_validation_stage(self) -> None:
        with open(self.source, "wb") as handle:
            handle.write(b"{not json")
        with self.assertRaises(ValueError):
            history_copy.run(
                self.source, self.target, "batch-key",
                audit_path=self.journal, audit_key="ak")
        event = self._event()
        self.assertEqual(event["error"], "ValueError")
        self.assertEqual(event["stage"], "校验")

    def test_bad_source_argument_is_validation_stage(self) -> None:
        # The source is not part of the event, so this parameter failure
        # can still be recorded faithfully.
        with self.assertRaises(ValueError):
            history_copy.run(
                123, self.target, "batch-key",  # type: ignore[arg-type]
                audit_path=self.journal, audit_key="ak")
        self.assertEqual(self._event()["stage"], "校验")
        self.assertEqual(self._event()["error"], "ValueError")

    def test_unrepresentable_bad_target_or_key_chains_audit_failure(self) -> None:
        # A non-string/empty target or history key cannot populate the
        # event's non-empty string fields, so audit.record raises its own
        # public ValueError chained after the operation's ValueError --
        # the audit-failure rule -- rather than fabricating an event.
        for args in ((self.source, 123, "batch-key"),
                     (self.source, self.target, "")):
            with self.subTest(args=args):
                with self.assertRaises(ValueError) as ctx:
                    history_copy.run(
                        *args,  # type: ignore[arg-type]
                        audit_path=self.journal, audit_key="ak-x")
                self.assertIsInstance(ctx.exception.__cause__, ValueError)

    # -- execution stage --------------------------------------------------

    def test_existing_target_conflict_is_execution_stage(self) -> None:
        with open(self.target, "wb") as handle:
            handle.write(b"previous contents")
        with self.assertRaises(FileExistsError):
            history_copy.run(
                self.source, self.target, "batch-key",
                audit_path=self.journal, audit_key="ak")
        event = self._event()
        self.assertEqual(event["error"], "FileExistsError")
        self.assertEqual(event["stage"], "执行")
        self.assertIs(event["changed"], False)
        self.assertEqual(open(self.target, "rb").read(), b"previous contents")

    def test_leftover_recovery_conflict_is_execution_stage(self) -> None:
        with open(self.target, "wb") as handle:
            handle.write(self.single)
        with open(self.target + ".recovery", "wb") as handle:
            handle.write(b"stale")
        with self.assertRaises(FileExistsError):
            history_copy.run(
                self.source, self.target, "batch-key", overwrite=True,
                audit_path=self.journal, audit_key="ak")
        self.assertEqual(self._event()["stage"], "执行")

    # -- sync stage -------------------------------------------------------

    def test_sync_failure_with_successful_rollback_is_sync_stage(self) -> None:
        with open(self.target, "wb") as handle:
            handle.write(self.single)
        with _fault_at("commit_dir_fsync"):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(
                    self.source, self.target, "batch-key", overwrite=True,
                    audit_path=self.journal, audit_key="ak")
        # The original OSError leaves with no chain; rollback succeeded.
        self.assertIsNone(ctx.exception.__cause__)
        event = self._event()
        self.assertEqual(event["error"], "OSError")
        self.assertEqual(event["stage"], "同步")
        # Rollback restored the old bytes: no deviation at departure.
        self.assertIs(event["changed"], False)
        self.assertEqual(open(self.target, "rb").read(), self.single)

    # -- rollback stage ---------------------------------------------------

    def test_rollback_failure_is_rollback_stage_with_real_deviation(self) -> None:
        with open(self.target, "wb") as handle:
            handle.write(self.single)
        with _fault_at("commit_dir_fsync", "rollback_replace"):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(
                    self.source, self.target, "batch-key", overwrite=True,
                    audit_path=self.journal, audit_key="ak")
        self.assertIsNotNone(ctx.exception.__cause__)
        event = self._event()
        self.assertEqual(event["stage"], "回滚")
        # The failed move-back leaves the new bytes on the target: the
        # changed flag is computed from the actual departure state.
        self.assertIs(event["changed"], True)
        self.assertEqual(open(self.target, "rb").read(), self.source_bytes)

    def test_new_target_rollback_unlink_failure_is_rollback_stage(self) -> None:
        with _fault_at("commit_dir_fsync", "rollback_unlink"):
            with self.assertRaises(OSError):
                history_copy.run(
                    self.source, self.target, "batch-key",
                    audit_path=self.journal, audit_key="ak")
        event = self._event()
        self.assertEqual(event["stage"], "回滚")
        self.assertIs(event["changed"], True)

    # -- exception preservation and chaining ------------------------------

    def test_operation_exception_chain_leaves_untouched(self) -> None:
        with open(self.target, "wb") as handle:
            handle.write(self.single)
        with _fault_at("commit_dir_fsync", "rollback_dir_fsync"):
            with self.assertRaises(OSError) as ctx:
                history_copy.run(
                    self.source, self.target, "batch-key", overwrite=True,
                    audit_path=self.journal, audit_key="ak")
        # Audit succeeded, so the operation error and its own chain leave
        # exactly as they do without auditing.
        self.assertIn("commit_dir_fsync", str(ctx.exception.__cause__))
        self.assertEqual(self._event()["stage"], "回滚")

    def test_audit_failure_after_failed_operation_chains_first_error(self) -> None:
        # Occupy the audit key with an event this call cannot produce.
        audit.record(self.journal, "ak", {
            "op": "restore", "target": "other", "key": "k",
            "changed": False, "error": None, "stage": None})
        with open(self.target, "wb") as handle:
            handle.write(b"old")
        with self.assertRaises(ValueError) as ctx:
            history_copy.run(
                self.source, self.target, "batch-key",
                audit_path=self.journal, audit_key="ak")
        # The journal's conflict is what surfaces, chaining the operation's
        # FileExistsError; the history-file result is retained.
        self.assertIsInstance(ctx.exception.__cause__, FileExistsError)
        self.assertEqual(open(self.target, "rb").read(), b"old")

    def test_audit_failure_after_success_raises_journal_error_alone(self) -> None:
        audit.record(self.journal, "ak", {
            "op": "restore", "target": "other", "key": "k",
            "changed": False, "error": None, "stage": None})
        with self.assertRaises(ValueError):
            history_copy.run(
                self.source, self.target, "batch-key",
                audit_path=self.journal, audit_key="ak")
        # The successful copy is retained even though the audit failed.
        self.assertEqual(open(self.target, "rb").read(), self.source_bytes)

    # -- concurrency ------------------------------------------------------

    def test_concurrent_copies_share_one_complete_journal(self) -> None:
        results: list[Exception] = []

        def copy_to(index: int) -> None:
            try:
                history_copy.run(
                    self.source,
                    os.path.join(self.tmp.name, f"t-{index}.history"),
                    "batch-key",
                    audit_path=self.journal, audit_key=f"k-{index:03d}")
            except Exception as exc:  # noqa: BLE001
                results.append(exc)

        threads = [threading.Thread(target=copy_to, args=(i,))
                   for i in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(results, [])
        # A read observes the whole sorted document, never a partial one.
        doc = json.loads(open(self.journal, encoding="utf-8").read())
        keys = list(doc["events"])
        self.assertEqual(keys, sorted(keys))
        self.assertEqual(len(keys), 12)
        for key, event in doc["events"].items():
            self.assertEqual(event["error"], None)
            self.assertIs(event["changed"], True)


class _RestoreFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        d = self.tmp.name
        self.target = os.path.join(d, "target.history")
        self.recovery = self.target + ".recovery"
        self.journal = os.path.join(d, "audit.json")
        self.old_bytes = json.dumps(_history_doc(snapshots=[
            ["pending", _coord(items={"j-1": [None, None]})],
        ])).encode("utf-8") + b"\n"
        self.new_bytes = json.dumps(_history_doc()).encode("utf-8") + b"\n"
        with open(self.target, "wb") as handle:
            handle.write(self.old_bytes)

    def _event(self) -> dict[str, object]:
        return audit.get(self.journal, "ak")


class HistoryRecoveryAuditTest(_RestoreFixture):
    def test_pair_must_be_given_together(self) -> None:
        # A whitespace string is still a non-empty string and is accepted
        # by the same rule record() applies; only empty/non-string values
        # are caller errors.
        for kwargs in (
            {"audit_path": self.journal},
            {"audit_key": "ak"},
            {"audit_path": self.journal, "audit_key": ""},
            {"audit_path": "", "audit_key": "ak"},
            {"audit_path": 1, "audit_key": "ak"},
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    history_recovery.restore(self.target, "batch-key",
                                             **kwargs)
        self.assertFalse(os.path.exists(self.journal))

    def test_success_changed_event_records_restored_bytes(self) -> None:
        with open(self.recovery, "wb") as handle:
            handle.write(self.new_bytes)
        summary, changed = history_recovery.restore(
            self.target, "batch-key",
            audit_path=self.journal, audit_key="ak")
        self.assertIs(changed, True)
        self.assertEqual(summary["key"], "batch-key")
        self.assertEqual(self._event(), {
            "op": "restore",
            "target": self.target,
            "key": "batch-key",
            "changed": True,
            "error": None,
            "stage": None,
        })
        self.assertFalse(os.path.exists(self.recovery))

    def test_success_unchanged_event_when_bytes_identical(self) -> None:
        with open(self.recovery, "wb") as handle:
            handle.write(self.old_bytes)
        _, changed = history_recovery.restore(
            self.target, "batch-key",
            audit_path=self.journal, audit_key="ak")
        self.assertIs(changed, False)
        self.assertIs(self._event()["changed"], False)

    def test_missing_target_and_recovery_is_validation_stage(self) -> None:
        os.unlink(self.target)
        with self.assertRaises(FileNotFoundError):
            history_recovery.restore(
                self.target, "batch-key",
                audit_path=self.journal, audit_key="ak")
        event = self._event()
        self.assertEqual(event["error"], "FileNotFoundError")
        self.assertEqual(event["stage"], "校验")
        self.assertIs(event["changed"], False)

    def test_wrong_key_is_validation_stage(self) -> None:
        with self.assertRaises(KeyError):
            history_recovery.restore(
                self.target, "claimed-key",
                audit_path=self.journal, audit_key="ak")
        event = self._event()
        self.assertEqual(event["error"], "KeyError")
        self.assertEqual(event["stage"], "校验")
        self.assertEqual(event["key"], "claimed-key")

    def test_bad_recovery_bytes_are_validation_stage(self) -> None:
        with open(self.recovery, "wb") as handle:
            handle.write(b"{not json")
        with self.assertRaises(ValueError):
            history_recovery.restore(
                self.target, "batch-key",
                audit_path=self.journal, audit_key="ak")
        event = self._event()
        self.assertEqual(event["stage"], "校验")
        self.assertIs(event["changed"], False)
        # Nothing was written before the validation failure.
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)

    def test_after_replace_fault_is_sync_stage_and_restores(self) -> None:
        with open(self.recovery, "wb") as handle:
            handle.write(self.new_bytes)
        with self.assertRaises(OSError) as ctx:
            history_recovery.restore(
                self.target, "batch-key", fault="after_replace",
                audit_path=self.journal, audit_key="ak")
        self.assertIsNone(ctx.exception.__cause__)
        event = self._event()
        self.assertEqual(event["error"], "OSError")
        self.assertEqual(event["stage"], "同步")
        self.assertIs(event["changed"], False)
        self.assertEqual(open(self.target, "rb").read(), self.old_bytes)

    def test_rollback_failure_is_rollback_stage_with_deviation(self) -> None:
        from unittest import mock
        with open(self.recovery, "wb") as handle:
            handle.write(self.new_bytes)
        real_replace = os.replace
        calls = {"n": 0}

        def flaky_replace(src, dst):
            calls["n"] += 1
            if calls["n"] == 2:  # forward target is #1; rollback target #2
                raise OSError(77, "injected rollback replace failure")
            return real_replace(src, dst)

        with mock.patch.object(history_recovery.os, "replace", flaky_replace):
            with self.assertRaises(OSError) as ctx:
                history_recovery.restore(
                    self.target, "batch-key", fault="after_replace",
                    audit_path=self.journal, audit_key="ak")
        self.assertIsNotNone(ctx.exception.__cause__)
        event = self._event()
        self.assertEqual(event["stage"], "回滚")
        self.assertIs(event["changed"], True)
        self.assertEqual(open(self.target, "rb").read(), self.new_bytes)

    def test_audit_failure_chains_operation_error(self) -> None:
        audit.record(self.journal, "ak", {
            "op": "copy", "target": "other", "key": "k",
            "changed": True, "error": None, "stage": None})
        with open(self.recovery, "wb") as handle:
            handle.write(b"{not json")
        with self.assertRaises(ValueError) as ctx:
            history_recovery.restore(
                self.target, "batch-key",
                audit_path=self.journal, audit_key="ak")
        self.assertIsInstance(ctx.exception.__cause__, ValueError)
        # The recovery file is retained: the operation failed validation
        # and auditing did not change any history result.
        self.assertEqual(open(self.recovery, "rb").read(), b"{not json")

    def test_without_pair_no_journal_written(self) -> None:
        with open(self.recovery, "wb") as handle:
            handle.write(self.new_bytes)
        summary, changed = history_recovery.restore(self.target, "batch-key")
        self.assertIs(changed, True)
        self.assertEqual(summary["count"], 2)
        self.assertFalse(os.path.exists(self.journal))


if __name__ == "__main__":
    unittest.main()
