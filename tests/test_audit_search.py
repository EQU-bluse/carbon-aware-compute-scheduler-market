"""Audit search pagination/filtering and record field-order tolerance.

Covers audit.record accepting a caller event's fields in any order
(while the on-disk order checks stay strict) and the read-only
audit.search entry: argument validation, the exclusive cursor, stable
code-point scan order, the op/stage/key filters combined with AND,
page-size limits, the next-cursor contract and the read-only guarantee.
"""

from __future__ import annotations

import json
import os
import unittest
from tempfile import TemporaryDirectory

from carbon_market import audit


def _event(op: str = "copy", target: str = "t.history", key: str = "batch-key",
           changed: bool = True, error: str | None = None,
           stage: str | None = None) -> dict[str, object]:
    return {"op": op, "target": target, "key": key, "changed": changed,
            "error": error, "stage": stage}


class RecordFieldOrderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")

    def test_record_accepts_any_field_order_and_stores_public_order(self) -> None:
        shuffled = {"stage": None, "error": None, "changed": True,
                    "key": "batch-key", "target": "t.history", "op": "copy"}
        stored, created = audit.record(self.journal, "k", shuffled)
        self.assertIs(created, True)
        self.assertEqual(list(stored),
                         ["op", "target", "key", "changed", "error", "stage"])
        doc = json.loads(open(self.journal, encoding="utf-8").read())
        self.assertEqual(list(doc["events"]["k"]),
                         ["op", "target", "key", "changed", "error", "stage"])

    def test_record_replay_matches_regardless_of_field_order(self) -> None:
        audit.record(self.journal, "k", _event())
        shuffled = {"error": None, "stage": None, "op": "copy",
                    "target": "t.history", "key": "batch-key",
                    "changed": True}
        _, created = audit.record(self.journal, "k", shuffled)
        self.assertIs(created, False)

    def test_record_still_requires_the_exact_field_set(self) -> None:
        for bad in (
            {"op": "copy", "target": "t", "key": "k", "changed": True,
             "error": None},  # missing stage
            dict(_event(), extra=1),  # extra field
            "not-a-dict",
        ):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    audit.record(self.journal, "k", bad)  # type: ignore[arg-type]

    def test_record_still_rejects_out_of_order_document_on_disk(self) -> None:
        doc = {"version": 1, "events": {"b": _event(), "a": _event()}}
        text = json.dumps(doc, separators=(",", ":"))
        with open(self.journal, "w", encoding="utf-8") as handle:
            handle.write(text)
        with self.assertRaises(ValueError):
            audit.record(self.journal, "c", _event())
        self.assertEqual(open(self.journal, encoding="utf-8").read(), text)


class SearchValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        audit.record(self.journal, "k", _event())

    def test_bad_arguments_raise_value_error(self) -> None:
        bad_calls = [
            {"path": ""}, {"path": 1},
            {"cursor": ""}, {"cursor": 1},
            {"limit": 0}, {"limit": 1001}, {"limit": True},
            {"limit": False}, {"limit": "10"}, {"limit": 1.5},
            {"op": "delete"}, {"op": ""}, {"op": 1},
            {"stage": "失败"}, {"stage": ""}, {"stage": 1},
            {"key": ""}, {"key": 1},
        ]
        for kwargs in bad_calls:
            with self.subTest(kwargs=kwargs):
                kwargs.setdefault("path", self.journal)
                with self.assertRaises(ValueError):
                    audit.search(**kwargs)  # type: ignore[arg-type]

    def test_boundary_page_sizes_are_accepted(self) -> None:
        for limit in (1, 1000):
            with self.subTest(limit=limit):
                result = audit.search(self.journal, limit=limit)
                self.assertEqual(len(result["events"]), 1)

    def test_missing_journal_raises_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            audit.search(os.path.join(self.tmp.name, "missing.json"))

    def test_malformed_journal_raises_value_error_without_rewriting(self) -> None:
        out_of_order = json.dumps(
            {"version": 1, "events": {"b": _event(), "a": _event()}},
            separators=(",", ":"))
        for text in (
            "{not json",
            '{"events":{},"version":1}',
            out_of_order,
            '{"version":-0,"events":{}}',
        ):
            with self.subTest(text=text):
                with open(self.journal, "w", encoding="utf-8") as handle:
                    handle.write(text)
                with self.assertRaises(ValueError):
                    audit.search(self.journal)
                self.assertEqual(
                    open(self.journal, encoding="utf-8").read(), text)


class SearchPaginationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        # Keys interleave matching and non-matching events so the scan
        # must skip non-matches without consuming page capacity.
        self.events = {
            "k-01": _event(op="copy", key="alpha"),
            "k-02": _event(op="restore", key="beta", changed=False,
                           error="ValueError", stage="校验"),
            "k-03": _event(op="copy", key="alpha", changed=False),
            "k-04": _event(op="copy", key="beta"),
            "k-05": _event(op="restore", key="alpha", changed=True,
                           error="OSError", stage="同步"),
            "k-06": _event(op="copy", key="alpha"),
        }

    def _record_all(self) -> None:
        for audit_key, event in self.events.items():
            audit.record(self.journal, audit_key, event)

    def test_result_shape_and_default_page(self) -> None:
        self._record_all()
        result = audit.search(self.journal)
        self.assertEqual(list(result), ["events", "next"])
        self.assertEqual([key for key, _ in result["events"]],
                         sorted(self.events))
        self.assertIsNone(result["next"])
        for audit_key, event in result["events"]:
            self.assertEqual(event, self.events[audit_key])
            self.assertEqual(
                list(event),
                ["op", "target", "key", "changed", "error", "stage"])

    def test_empty_journal_and_no_match_return_empty_page(self) -> None:
        audit.record(self.journal, "k", _event())
        result = audit.search(self.journal, op="restore")
        self.assertEqual(result, {"events": [], "next": None})

    def test_pagination_walks_all_pages_with_next_cursor(self) -> None:
        self._record_all()
        seen: list[str] = []
        cursor = None
        pages = 0
        while True:
            result = audit.search(self.journal, cursor=cursor, limit=2)
            pages += 1
            seen.extend(key for key, _ in result["events"])
            if result["next"] is None:
                break
            cursor = result["next"]
        self.assertEqual(seen, sorted(self.events))
        self.assertEqual(pages, 3)

    def test_cursor_is_exclusive_and_need_not_exist(self) -> None:
        self._record_all()
        result = audit.search(self.journal, cursor="k-03")
        self.assertEqual([key for key, _ in result["events"]],
                         ["k-04", "k-05", "k-06"])
        # A cursor between two existing keys excludes nothing more.
        result = audit.search(self.journal, cursor="k-04x")
        self.assertEqual([key for key, _ in result["events"]],
                         ["k-05", "k-06"])
        # A cursor past the last key yields an empty page.
        result = audit.search(self.journal, cursor="zz")
        self.assertEqual(result, {"events": [], "next": None})

    def test_next_cursor_is_last_item_of_page_when_more_remain(self) -> None:
        self._record_all()
        result = audit.search(self.journal, limit=3)
        self.assertEqual([key for key, _ in result["events"]],
                         ["k-01", "k-02", "k-03"])
        self.assertEqual(result["next"], "k-03")
        # Exactly one full page remaining: next is null.
        result = audit.search(self.journal, cursor=result["next"], limit=3)
        self.assertEqual(len(result["events"]), 3)
        self.assertIsNone(result["next"])

    def test_filters_combine_with_and_and_skip_non_matches(self) -> None:
        self._record_all()
        result = audit.search(self.journal, op="copy")
        self.assertEqual([key for key, _ in result["events"]],
                         ["k-01", "k-03", "k-04", "k-06"])
        result = audit.search(self.journal, op="copy", key="alpha")
        self.assertEqual([key for key, _ in result["events"]],
                         ["k-01", "k-03", "k-06"])
        # Non-matching events do not consume page capacity: the two
        # matching copies after k-03 fill the page by themselves.
        result = audit.search(self.journal, cursor="k-03", op="copy",
                              limit=2)
        self.assertEqual([key for key, _ in result["events"]],
                         ["k-04", "k-06"])
        self.assertIsNone(result["next"])

    def test_stage_filter_success_and_failure_stages(self) -> None:
        self._record_all()
        result = audit.search(self.journal, stage="成功")
        self.assertEqual([key for key, _ in result["events"]],
                         ["k-01", "k-03", "k-04", "k-06"])
        result = audit.search(self.journal, stage="校验")
        self.assertEqual([key for key, _ in result["events"]], ["k-02"])
        result = audit.search(self.journal, stage="同步")
        self.assertEqual([key for key, _ in result["events"]], ["k-05"])
        result = audit.search(self.journal, stage="回滚")
        self.assertEqual(result, {"events": [], "next": None})

    def test_key_filter_is_exact_string_equality(self) -> None:
        self._record_all()
        # No prefix matching, no case folding, no normalization.
        for needle in ("alp", "alpha/", "Alpha", "ALPHA", "./alpha"):
            with self.subTest(needle=needle):
                result = audit.search(self.journal, key=needle)
                self.assertEqual(result, {"events": [], "next": None})
        result = audit.search(self.journal, key="alpha")
        self.assertEqual([key for key, _ in result["events"]],
                         ["k-01", "k-03", "k-05", "k-06"])

    def test_returned_events_are_copies(self) -> None:
        self._record_all()
        result = audit.search(self.journal, limit=1)
        result["events"][0][1]["op"] = "mutated"
        again = audit.search(self.journal, limit=1)
        self.assertEqual(again["events"][0][1]["op"], "copy")

    def test_search_never_rewrites_the_journal(self) -> None:
        self._record_all()
        before = open(self.journal, "rb").read()
        audit.search(self.journal, op="copy", stage="成功", key="alpha",
                     limit=1)
        self.assertEqual(open(self.journal, "rb").read(), before)


if __name__ == "__main__":
    unittest.main()
