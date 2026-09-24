"""Unit tests for audit.record field-order tolerance and audit.search.

record accepts the six event fields in any caller-chosen order while the
on-disk order contract stays strict; search pages the journal by audit
key code point with an exclusive cursor and AND-combined op/stage/
history-key filters, never writing the journal.
"""

from __future__ import annotations

import json
import os
import threading
import unittest
from tempfile import TemporaryDirectory

from carbon_market import audit

_FIELDS = ("op", "target", "key", "changed", "error", "stage")


def _read(path: str) -> str:
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def _event(op: str = "copy", target: str = "t.history", key: str = "hk",
           changed: bool = True, error: object = None,
           stage: object = None) -> dict[str, object]:
    return {"op": op, "target": target, "key": key, "changed": changed,
            "error": error, "stage": stage}


def _failed(op: str = "copy", stage: str = "校验", key: str = "hk",
            error: str = "ValueError") -> dict[str, object]:
    return {"op": op, "target": "t.history", "key": key,
            "changed": False, "error": error, "stage": stage}


class RecordFieldOrderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")

    def test_record_accepts_shuffled_order_and_normalizes(self) -> None:
        shuffled = {"stage": None, "error": None, "changed": True,
                    "key": "hk", "target": "t.history", "op": "copy"}
        recorded, created = audit.record(self.journal, "ak", shuffled)
        self.assertIs(created, True)
        self.assertEqual(list(recorded), list(_FIELDS))
        self.assertEqual(recorded, _event())
        # get and the persisted form both expose the public order.
        self.assertEqual(list(audit.get(self.journal, "ak")), list(_FIELDS))
        with open(self.journal, encoding="utf-8") as handle:
            self.assertEqual(list(json.loads(handle.read())["events"]["ak"]),
                             list(_FIELDS))

    def test_replay_equivalent_events_in_different_orders(self) -> None:
        first, created = audit.record(self.journal, "ak", {
            "stage": None, "error": None, "changed": True, "key": "hk",
            "target": "t.history", "op": "copy"})
        self.assertIs(created, True)
        second, replayed = audit.record(self.journal, "ak", _event())
        self.assertIs(replayed, False)
        self.assertEqual(second, first)

    def test_conflicting_values_still_rejected_regardless_of_order(self) -> None:
        audit.record(self.journal, "ak", {"stage": None, "error": None,
                                          "changed": True, "key": "hk",
                                          "target": "t.history", "op": "copy"})
        with self.assertRaises(ValueError):
            audit.record(self.journal, "ak", _event(changed=False))

    def test_missing_or_extra_fields_still_rejected(self) -> None:
        with self.assertRaises(ValueError):
            audit.record(self.journal, "ak", {
                "op": "copy", "target": "t", "key": "hk",
                "changed": True, "error": None})  # stage missing
        with self.assertRaises(ValueError):
            audit.record(self.journal, "ak", dict(_event(), extra=1))

    def test_disk_event_field_order_stays_strict(self) -> None:
        # A shuffled event on disk is rejected by every entry point and
        # is never normalized in place by the read-only queries.
        swapped = ('{"op":"copy","target":"t.history","changed":true,'
                   '"key":"hk","error":null,"stage":null}')
        text = '{"version":1,"events":{"ak":' + swapped + "}}"
        with open(self.journal, "w", encoding="utf-8") as handle:
            handle.write(text)
        with self.assertRaises(ValueError):
            audit.get(self.journal, "ak")
        with self.assertRaises(ValueError):
            audit.search(self.journal)
        with self.assertRaises(ValueError):
            audit.record(self.journal, "ak2", _event())
        self.assertEqual(_read(self.journal), text)


class SearchValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        audit.record(self.journal, "ak", _event())

    def test_path_must_be_non_empty_string(self) -> None:
        missing = os.path.join(self.tmp.name, "missing.json")
        with self.assertRaises(FileNotFoundError):
            audit.search(missing)
        for path in ("", 1, None):
            with self.subTest(path=path):
                with self.assertRaises(ValueError):
                    audit.search(path)  # type: ignore[arg-type]

    def test_cursor_must_be_none_or_non_empty_string(self) -> None:
        for cursor in ("", 1, True, b"ak"):
            with self.subTest(cursor=cursor):
                with self.assertRaises(ValueError):
                    audit.search(self.journal, cursor)  # type: ignore[arg-type]

    def test_count_must_be_plain_int_between_1_and_1000(self) -> None:
        for count in (0, -1, 1001, True, False, 1.0, "100", None):
            with self.subTest(count=count):
                with self.assertRaises(ValueError):
                    audit.search(self.journal, count=count)  # type: ignore[arg-type]
        page = audit.search(self.journal, count=1)
        self.assertEqual(len(page["events"]), 1)
        page = audit.search(self.journal, count=1000)
        self.assertEqual(len(page["events"]), 1)

    def test_filters_must_be_in_range(self) -> None:
        for value in ("", "COPY", "delete", 1, True):
            with self.subTest(op=value):
                with self.assertRaises(ValueError):
                    audit.search(self.journal, op=value)  # type: ignore[arg-type]
        for value in ("", "success", "校验 ", "失败", 1):
            with self.subTest(stage=value):
                with self.assertRaises(ValueError):
                    audit.search(self.journal, stage=value)  # type: ignore[arg-type]
        for value in ("", 1, False):
            with self.subTest(history_key=value):
                with self.assertRaises(ValueError):
                    audit.search(
                        self.journal, history_key=value)  # type: ignore[arg-type]


class SearchPagingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")

    def _seed(self) -> None:
        # Insert out of order; the journal stores them by code point.
        for audit_key in ("m", "a", "z", "b", "k"):
            audit.record(self.journal, audit_key,
                         _event(key=f"hk-{audit_key}"))

    def test_default_count_pages_in_code_point_order(self) -> None:
        self._seed()
        page = audit.search(self.journal)
        self.assertEqual(list(page), ["events", "next"])
        self.assertEqual(page["next"], None)
        self.assertEqual([pair[0] for pair in page["events"]],
                         ["a", "b", "k", "m", "z"])
        for pair in page["events"]:
            self.assertEqual(len(pair), 2)
            self.assertEqual(list(pair[1]), list(_FIELDS))

    def test_next_points_at_last_pair_when_more_matches_follow(self) -> None:
        self._seed()
        page = audit.search(self.journal, count=3)
        self.assertEqual([pair[0] for pair in page["events"]], ["a", "b", "k"])
        self.assertEqual(page["next"], "k")
        page = audit.search(self.journal, cursor=page["next"], count=3)
        self.assertEqual([pair[0] for pair in page["events"]], ["m", "z"])
        self.assertEqual(page["next"], None)

    def test_exactly_count_matches_has_null_next(self) -> None:
        self._seed()
        page = audit.search(self.journal, count=5)
        self.assertEqual(len(page["events"]), 5)
        self.assertIsNone(page["next"])

    def test_full_walk_via_cursors_recovers_every_event(self) -> None:
        self._seed()
        seen: list[str] = []
        cursor = None
        while True:
            page = audit.search(self.journal, cursor=cursor, count=2)
            seen.extend(pair[0] for pair in page["events"])
            if page["next"] is None:
                break
            cursor = page["next"]
        self.assertEqual(seen, ["a", "b", "k", "m", "z"])

    def test_cursor_is_exclusive_and_need_not_exist(self) -> None:
        self._seed()
        self.assertEqual(
            [pair[0] for pair in audit.search(
                self.journal, cursor="k")["events"]],
            ["m", "z"])
        # A cursor between existing keys works without naming one.
        self.assertEqual(
            [pair[0] for pair in audit.search(
                self.journal, cursor="h")["events"]],
            ["k", "m", "z"])
        # A cursor past the last key yields an empty, null-next page.
        page = audit.search(self.journal, cursor="zzz")
        self.assertEqual(page, {"events": [], "next": None})

    def test_empty_events_document_returns_empty_page(self) -> None:
        with open(self.journal, "w", encoding="utf-8") as handle:
            handle.write('{"version":1,"events":{}}')
        self.assertEqual(audit.search(self.journal),
                         {"events": [], "next": None})

    def test_non_matching_events_do_not_consume_page_capacity(self) -> None:
        for audit_key in ("a", "b", "c", "d", "e"):
            audit.record(self.journal, audit_key,
                         _failed(stage="校验", key="hk"))
        audit.record(self.journal, "f", _event(key="hk"))
        page = audit.search(self.journal, stage="成功", count=5)
        self.assertEqual([pair[0] for pair in page["events"]], ["f"])
        self.assertIsNone(page["next"])

    def test_filter_op(self) -> None:
        audit.record(self.journal, "a", _event(op="copy"))
        audit.record(self.journal, "b", _event(op="restore"))
        audit.record(self.journal, "c", _event(op="restore"))
        self.assertEqual(
            [pair[0] for pair in audit.search(self.journal, op="copy")["events"]],
            ["a"])
        self.assertEqual(
            [pair[0] for pair in audit.search(
                self.journal, op="restore")["events"]],
            ["b", "c"])

    def test_filter_success_stage_matches_only_null_error_and_stage(self) -> None:
        audit.record(self.journal, "a", _event())
        audit.record(self.journal, "b", _failed(stage="校验"))
        audit.record(self.journal, "c", _failed(stage="回滚", error="OSError"))
        keys = {pair[0] for pair in
                audit.search(self.journal, stage="成功")["events"]}
        self.assertEqual(keys, {"a"})
        self.assertEqual(
            [pair[0] for pair in audit.search(
                self.journal, stage="回滚")["events"]],
            ["c"])

    def test_filter_history_key_is_exact_equality_only(self) -> None:
        audit.record(self.journal, "a", _event(key="k"))
        audit.record(self.journal, "b", _event(key="k-1"))
        audit.record(self.journal, "c", _event(key="K"))
        # No prefix match and no case folding.
        self.assertEqual(
            [pair[0] for pair in audit.search(
                self.journal, history_key="k")["events"]],
            ["a"])

    def test_filters_combine_by_logical_and(self) -> None:
        audit.record(self.journal, "a", _event(op="copy", key="hk"))
        audit.record(self.journal, "b", _event(op="restore", key="hk"))
        audit.record(self.journal, "c", _failed(op="copy", stage="校验",
                                                key="hk"))
        page = audit.search(self.journal, op="copy", stage="成功",
                            history_key="hk")
        self.assertEqual([pair[0] for pair in page["events"]], ["a"])

    def test_code_point_order_for_mixed_keys(self) -> None:
        for audit_key in ("中", "a", "A", "aa", "aa1"):
            audit.record(self.journal, audit_key, _event())
        keys = [pair[0] for pair in audit.search(self.journal)["events"]]
        self.assertEqual(keys, sorted(keys))
        self.assertEqual(keys, ["A", "a", "aa", "aa1", "中"])

    def test_returned_events_are_fresh_copies(self) -> None:
        audit.record(self.journal, "a", _event())
        page = audit.search(self.journal)
        page["events"][0][1]["changed"] = "tampered"
        self.assertIs(audit.get(self.journal, "a")["changed"], True)

    def test_search_never_writes_and_rejects_malformed_order(self) -> None:
        doc = {"version": 1, "events": {"b": _event(), "a": _event()}}
        text = json.dumps(doc, separators=(",", ":"))
        with open(self.journal, "w", encoding="utf-8") as handle:
            handle.write(text)
        with self.assertRaises(ValueError):
            audit.search(self.journal)
        self.assertEqual(_read(self.journal), text)
        # And leaves no stray lock artifacts beyond the companion lock file
        # used by every entry point.
        self.assertEqual(sorted(os.listdir(self.tmp.name)),
                         ["audit.json", "audit.json.lock"])

    def test_malformed_json_and_negative_zero_raise_value_error(self) -> None:
        with open(self.journal, "w", encoding="utf-8") as handle:
            handle.write("{not json")
        with self.assertRaises(ValueError):
            audit.search(self.journal)
        with open(self.journal, "w", encoding="utf-8") as handle:
            handle.write('{"version":-0,"events":{}}')
        with self.assertRaises(ValueError):
            audit.search(self.journal)


class SearchConcurrencyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")

    def test_search_racing_writers_always_sees_complete_snapshot(self) -> None:
        errors: list[BaseException] = []

        def writer(index: int) -> None:
            try:
                for n in range(40):
                    audit.record(self.journal, f"w{index}-{n:03d}",
                                 _event(key="hk"))
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        def reader() -> None:
            try:
                cursor = None
                while True:
                    page = audit.search(self.journal, cursor=cursor,
                                        count=7, op="copy", stage="成功")
                    keys = [pair[0] for pair in page["events"]]
                    self.assertEqual(keys, sorted(keys))
                    # Every page is internally consistent: each next cursor
                    # names the page's own last key and walks forward only.
                    if cursor is not None:
                        self.assertTrue(all(key > cursor for key in keys))
                    if page["next"] is None:
                        break
                    cursor = page["next"]
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = ([threading.Thread(target=writer, args=(i,))
                    for i in range(4)] + [threading.Thread(target=reader)])
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        page = audit.search(self.journal, count=1000)
        self.assertEqual(len(page["events"]), 160)


if __name__ == "__main__":
    unittest.main()
