"""Verifiable digest chain for the audit journal.

Covers the version-2 sealed wire format (``[event, previous, digest]``
values, root ``head``), the independently computed SHA-256 item digests
over compact ``[audit_key, event, previous]`` JSON, the read-only
``audit.verify`` report for version 1 / intact version 2 / tampered
version 2 documents, the version-1 -> version-2 atomic upgrade (and the
no-rewrite guarantees for pure reads and idempotent replays), rejection
of a broken chain by record/get/search without touching the file,
commit-failure byte preservation, and concurrent inserts plus
restart-continuation from the persisted last item.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import unittest
from tempfile import TemporaryDirectory

from carbon_market import audit


def _event(op: str = "copy", target: str = "t.history", key: str = "batch-key",
           changed: bool = True, error: str | None = None,
           stage: str | None = None) -> dict[str, object]:
    return {"op": op, "target": target, "key": key, "changed": changed,
            "error": error, "stage": stage}


def _digest_of(audit_key: str, event: dict[str, object],
               previous: str | None) -> str:
    # Independent re-implementation of the item-digest rule: SHA-256 of
    # the compact UTF-8 JSON of [audit_key, event, previous_digest].
    text = json.dumps([audit_key, event, previous], ensure_ascii=False,
                      separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _read_json(path: str) -> dict[str, object]:
    with open(path, encoding="utf-8") as handle:
        return json.loads(handle.read())


def _write_json(path: str, doc: object) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(doc, ensure_ascii=False, separators=(",", ":")))


def _write_v1(path: str, events: dict[str, dict[str, object]]) -> None:
    _write_json(path, {"version": 1, "events": events})


class _JournalCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")

    def _record(self, audit_key: str, event: dict[str, object] | None = None,
                **kwargs: object) -> None:
        audit.record(self.journal, audit_key,
                     _event(**kwargs) if event is None else event)


class SealedFormatTest(_JournalCase):
    def test_new_journal_is_version_2_with_head(self) -> None:
        self._record("k")
        doc = _read_json(self.journal)
        self.assertEqual(list(doc), ["version", "events", "head"])
        self.assertEqual(doc["version"], 2)

    def test_entry_shape_and_first_previous_null(self) -> None:
        event = _event()
        self._record("k", event)
        entry = _read_json(self.journal)["events"]["k"]
        self.assertEqual(entry, [event, None, entry[2]])
        self.assertIsNone(entry[1])
        self.assertEqual(len(entry[2]), 64)
        self.assertEqual(entry[2], entry[2].lower())

    def test_item_digest_matches_independent_vector(self) -> None:
        event = _event()
        self._record("k", event)
        entry = _read_json(self.journal)["events"]["k"]
        self.assertEqual(entry[2], _digest_of("k", event, None))

    def test_chain_links_follow_code_point_order_not_insertion_order(self) -> None:
        self._record("z", _event(key="z-event"))
        self._record("a", _event(key="a-event"))
        self._record("m", _event(key="m-event"))
        doc = _read_json(self.journal)
        self.assertEqual(list(doc["events"]), ["a", "m", "z"])
        entries = doc["events"]
        digest_a = _digest_of("a", entries["a"][0], None)
        digest_m = _digest_of("m", entries["m"][0], digest_a)
        digest_z = _digest_of("z", entries["z"][0], digest_m)
        self.assertEqual(entries["a"], [entries["a"][0], None, digest_a])
        self.assertEqual(entries["m"], [entries["m"][0], digest_a, digest_m])
        self.assertEqual(entries["z"], [entries["z"][0], digest_m, digest_z])
        self.assertEqual(doc["head"], digest_z)

    def test_chinese_stage_is_hashed_over_utf8_compact_json(self) -> None:
        failed = _event(changed=False, error="ValueError", stage="校验")
        self._record("k", failed)
        doc = _read_json(self.journal)
        entry = doc["events"]["k"]
        self.assertEqual(entry[2], _digest_of("k", failed, None))
        self.assertEqual(doc["head"], entry[2])

    def test_queries_return_plain_events_without_chain_fields(self) -> None:
        self._record("k", _event())
        got = audit.get(self.journal, "k")
        self.assertEqual(got, _event())
        self.assertEqual(list(got),
                         ["op", "target", "key", "changed", "error", "stage"])
        page = audit.search(self.journal)
        self.assertEqual(page["events"][0], ["k", _event()])
        self.assertIsInstance(page["events"][0][1], dict)


class VerifyTest(_JournalCase):
    def test_result_key_order(self) -> None:
        self._record("k")
        result = audit.verify(self.journal)
        self.assertEqual(list(result),
                         ["version", "count", "sealed", "valid",
                          "first_invalid", "head"])

    def test_legal_version_1_non_empty(self) -> None:
        _write_v1(self.journal, {"a": _event(key="a"), "b": _event(key="b")})
        self.assertEqual(audit.verify(self.journal), {
            "version": 1, "count": 2, "sealed": False, "valid": False,
            "first_invalid": None, "head": None})

    def test_legal_version_1_empty(self) -> None:
        _write_v1(self.journal, {})
        self.assertEqual(audit.verify(self.journal), {
            "version": 1, "count": 0, "sealed": False, "valid": False,
            "first_invalid": None, "head": None})

    def test_intact_version_2_reports_declared_head(self) -> None:
        self._record("a", _event(key="a"))
        self._record("b", _event(key="b"))
        doc = _read_json(self.journal)
        result = audit.verify(self.journal)
        self.assertEqual(result, {
            "version": 2, "count": 2, "sealed": True, "valid": True,
            "first_invalid": None, "head": doc["head"]})

    def test_empty_sealed_version_2_is_valid_with_null_head(self) -> None:
        _write_json(self.journal, {"version": 2, "events": {}, "head": None})
        self.assertEqual(audit.verify(self.journal), {
            "version": 2, "count": 0, "sealed": True, "valid": True,
            "first_invalid": None, "head": None})

    def test_missing_journal_is_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            audit.verify(os.path.join(self.tmp.name, "missing.json"))

    def test_bad_path_is_value_error(self) -> None:
        for bad in ("", 1, None, b"x"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    audit.verify(bad)  # type: ignore[arg-type]


class TamperReportTest(_JournalCase):
    def setUp(self) -> None:
        super().setUp()
        for letter, index_key in (("a", "ka"), ("b", "kb"), ("c", "kc")):
            self._record(letter, _event(key=index_key))
        self.declared_head = _read_json(self.journal)["head"]

    def _rewrite(self, mutate) -> dict[str, object]:
        doc = _read_json(self.journal)
        mutate(doc)
        _write_json(self.journal, doc)
        return doc

    def test_changed_event_content_reports_that_index(self) -> None:
        self._rewrite(lambda doc: doc["events"]["b"][0].__setitem__(
            "changed", False))
        result = audit.verify(self.journal)
        self.assertFalse(result["valid"])
        self.assertEqual(result["first_invalid"], 1)
        self.assertEqual(result["head"], self.declared_head)

    def test_wrong_stored_digest_reports_that_index(self) -> None:
        self._rewrite(lambda doc: doc["events"]["c"].__setitem__(2, "0" * 64))
        result = audit.verify(self.journal)
        self.assertEqual(result["first_invalid"], 2)
        self.assertFalse(result["valid"])

    def test_broken_previous_reference_reports_current_index(self) -> None:
        # Keep the stored item digest self-consistent with the bogus
        # previous reference, so only the link is wrong: the report must
        # be the current index, not a digest mismatch.
        def break_link(doc: dict[str, object]) -> None:
            event = doc["events"]["b"][0]
            doc["events"]["b"][1] = "0" * 64
            doc["events"]["b"][2] = _digest_of("b", event, "0" * 64)

        self._rewrite(break_link)
        result = audit.verify(self.journal)
        self.assertEqual(result["first_invalid"], 1)

    def test_first_item_must_reference_null(self) -> None:
        def break_first(doc: dict[str, object]) -> None:
            event = doc["events"]["a"][0]
            doc["events"]["a"][1] = "0" * 64
            doc["events"]["a"][2] = _digest_of("a", event, "0" * 64)

        self._rewrite(break_first)
        self.assertEqual(audit.verify(self.journal)["first_invalid"], 0)

    def test_only_root_head_wrong_reports_count(self) -> None:
        self._rewrite(lambda doc: doc.__setitem__("head", "0" * 64))
        result = audit.verify(self.journal)
        self.assertEqual(result["first_invalid"], 3)
        self.assertEqual(result["count"], 3)
        self.assertEqual(result["head"], "0" * 64)

    def test_null_head_on_non_empty_log_reports_count(self) -> None:
        self._rewrite(lambda doc: doc.__setitem__("head", None))
        result = audit.verify(self.journal)
        self.assertEqual(result["first_invalid"], 3)
        self.assertIsNone(result["head"])

    def test_tail_deletion_exposes_incomplete_chain_at_count(self) -> None:
        # Drop the last entry but leave the old head in place: the
        # surviving prefix is intact, only the root anchor mismatches.
        self._rewrite(lambda doc: doc["events"].pop("c"))
        result = audit.verify(self.journal)
        self.assertEqual(result["count"], 2)
        self.assertEqual(result["first_invalid"], 2)
        self.assertFalse(result["valid"])
        self.assertEqual(result["head"], self.declared_head)

    def test_verify_never_raises_for_chain_tampering_or_rewrites(self) -> None:
        raw_before = open(self.journal, "rb").read()
        self._rewrite(lambda doc: doc["events"].pop("c"))
        raw_tampered = open(self.journal, "rb").read()
        audit.verify(self.journal)
        self.assertEqual(open(self.journal, "rb").read(), raw_tampered)
        self.assertNotEqual(raw_tampered, raw_before)


class BrokenChainRejectionTest(_JournalCase):
    def setUp(self) -> None:
        super().setUp()
        self.events = {"a": _event(key="ka"), "b": _event(key="kb")}
        for audit_key, event in self.events.items():
            self._record(audit_key, event)
        doc = _read_json(self.journal)
        doc["events"]["b"][0]["changed"] = False
        _write_json(self.journal, doc)

    def test_record_new_key_raises_and_keeps_bytes(self) -> None:
        raw = open(self.journal, "rb").read()
        with self.assertRaises(ValueError):
            audit.record(self.journal, "c", _event(key="kc"))
        self.assertEqual(open(self.journal, "rb").read(), raw)

    def test_record_replay_also_raises_and_keeps_bytes(self) -> None:
        # Chain validation precedes the idempotent replay lookup.
        raw = open(self.journal, "rb").read()
        with self.assertRaises(ValueError):
            audit.record(self.journal, "a", self.events["a"])
        self.assertEqual(open(self.journal, "rb").read(), raw)

    def test_get_raises_even_for_an_unaffected_key(self) -> None:
        raw = open(self.journal, "rb").read()
        with self.assertRaises(ValueError):
            audit.get(self.journal, "a")
        self.assertEqual(open(self.journal, "rb").read(), raw)

    def test_search_raises_and_keeps_bytes(self) -> None:
        raw = open(self.journal, "rb").read()
        with self.assertRaises(ValueError):
            audit.search(self.journal)
        self.assertEqual(open(self.journal, "rb").read(), raw)

    def test_verify_reports_instead_of_raising(self) -> None:
        result = audit.verify(self.journal)
        self.assertEqual(result["first_invalid"], 1)


class StructuralRejectionTest(_JournalCase):
    def _assert_verify_raises(self, doc: object) -> None:
        _write_json(self.journal, doc)
        with self.assertRaises(ValueError):
            audit.verify(self.journal)

    def test_bad_versions_and_root_shapes(self) -> None:
        good_event = _event()
        for doc in (
            {"events": {}, "version": 2, "head": None},          # v2 order
            {"version": 2, "events": {}},                        # missing head
            {"version": 2, "events": {}, "head": None, "x": 1},  # extra field
            {"version": 1, "events": {}, "head": None},          # v1 with head
            {"version": 3, "events": {}, "head": None},
            {"version": True, "events": {}, "head": None},
            {"version": 2, "events": [], "head": None},
            {"version": 2, "events": {}, "head": "ab"},
            {"version": 2, "events": {}, "head": "A" * 64},
            {"version": 2, "events": {"k": [good_event, None]},
             "head": None},                                     # short entry
            {"version": 2,
             "events": {"k": [good_event, "ab", "0" * 64]},
             "head": None},                                     # bad previous
            {"version": 2,
             "events": {"k": [good_event, None, "A" * 64]},
             "head": None},                                     # uppercase digest
            {"version": 2,
             "events": {"a": good_event},
             "head": None},                                     # v2 plain event
            {"version": 2,
             "events": {"b": [good_event, None, "0" * 64],
                        "a": [good_event, None, "0" * 64]},
             "head": None},                                     # key order
        ):
            with self.subTest(doc=doc):
                self._assert_verify_raises(doc)

    def test_malformed_encoding_json_and_negative_zero(self) -> None:
        for raw in (
            b"{not json",
            b'{"version":2,"events":{},"head":null}\xff',
            b'{"version":-0,"events":{},"head":null}',
        ):
            with self.subTest(raw=raw):
                with open(self.journal, "wb") as handle:
                    handle.write(raw)
                with self.assertRaises(ValueError):
                    audit.verify(self.journal)


class UpgradeTest(_JournalCase):
    def setUp(self) -> None:
        super().setUp()
        self.v1_events = {"a": _event(key="ka"), "m": _event(key="km")}
        _write_v1(self.journal, self.v1_events)
        self.v1_bytes = open(self.journal, "rb").read()

    def test_new_event_atomically_seals_old_events_into_v2(self) -> None:
        new_event = _event(key="kz")
        stored, created = audit.record(self.journal, "z", new_event)
        self.assertIs(created, True)
        self.assertEqual(stored, new_event)

        report = audit.verify(self.journal)
        self.assertEqual(report["version"], 2)
        self.assertEqual(report["count"], 3)
        self.assertTrue(report["sealed"])
        self.assertTrue(report["valid"])

        doc = _read_json(self.journal)
        self.assertEqual(list(doc), ["version", "events", "head"])
        entries = doc["events"]
        digest_a = _digest_of("a", entries["a"][0], None)
        digest_m = _digest_of("m", entries["m"][0], digest_a)
        digest_z = _digest_of("z", entries["z"][0], digest_m)
        self.assertEqual(entries["a"][1:], [None, digest_a])
        self.assertEqual(entries["m"][1:], [digest_a, digest_m])
        self.assertEqual(entries["z"][1:], [digest_m, digest_z])
        self.assertEqual(doc["head"], digest_z)
        # The old events ride through unchanged.
        self.assertEqual(entries["a"][0], self.v1_events["a"])
        self.assertEqual(entries["m"][0], self.v1_events["m"])

    def test_queries_against_upgraded_journal_still_return_events(self) -> None:
        audit.record(self.journal, "z", _event(key="kz"))
        self.assertEqual(audit.get(self.journal, "a"), self.v1_events["a"])
        self.assertEqual([k for k, _ in audit.search(self.journal)["events"]],
                         ["a", "m", "z"])

    def test_pure_reads_never_upgrade_version_1(self) -> None:
        audit.get(self.journal, "a")
        audit.search(self.journal)
        audit.verify(self.journal)
        audit.search(self.journal, op="restore")
        self.assertEqual(open(self.journal, "rb").read(), self.v1_bytes)
        self.assertEqual(audit.verify(self.journal)["version"], 1)

    def test_idempotent_replay_never_upgrades_or_rewrites(self) -> None:
        stored, created = audit.record(self.journal, "a", self.v1_events["a"])
        self.assertIs(created, False)
        self.assertEqual(stored, self.v1_events["a"])
        self.assertEqual(open(self.journal, "rb").read(), self.v1_bytes)
        self.assertEqual(audit.verify(self.journal)["version"], 1)

    def test_same_key_different_event_on_v1_conflicts_without_upgrade(self) -> None:
        with self.assertRaises(ValueError):
            audit.record(self.journal, "a", dict(self.v1_events["a"],
                                                 changed=False))
        self.assertEqual(open(self.journal, "rb").read(), self.v1_bytes)

    def test_replace_failure_during_upgrade_restores_v1_bytes(self) -> None:
        with self.assertRaises(OSError):
            audit.record(self.journal, "z", _event(key="kz"),
                         fault="replace")
        self.assertEqual(open(self.journal, "rb").read(), self.v1_bytes)
        self.assertEqual(audit.verify(self.journal)["version"], 1)

    def test_replace_failure_creating_new_journal_removes_it(self) -> None:
        path = os.path.join(self.tmp.name, "fresh.json")
        self.assertFalse(os.path.exists(path))
        with self.assertRaises(OSError):
            audit.record(path, "k", _event(), fault="replace")
        self.assertFalse(os.path.exists(path))

    def test_appends_keep_prior_item_digests(self) -> None:
        audit.record(self.journal, "z", _event(key="kz"))
        first = _read_json(self.journal)
        digest_a = first["events"]["a"][2]
        digest_m = first["events"]["m"][2]
        audit.record(self.journal, "z2", _event(key="kz2"))
        second = _read_json(self.journal)
        self.assertEqual(second["events"]["a"][2], digest_a)
        self.assertEqual(second["events"]["m"][2], digest_m)
        self.assertEqual(second["events"]["z2"][1], first["head"])
        self.assertTrue(audit.verify(self.journal)["valid"])

    def test_replay_on_sealed_v2_writes_nothing(self) -> None:
        audit.record(self.journal, "z", _event(key="kz"))
        sealed_bytes = open(self.journal, "rb").read()
        _, created = audit.record(self.journal, "z", _event(key="kz"))
        self.assertIs(created, False)
        self.assertEqual(open(self.journal, "rb").read(), sealed_bytes)


class ConcurrencyAndRestartTest(_JournalCase):
    def test_concurrent_inserts_serialize_into_one_valid_chain(self) -> None:
        count = 16
        errors: list[BaseException] = []

        def insert(index: int) -> None:
            try:
                audit.record(self.journal, f"k-{index:03d}",
                             _event(key=f"job-{index:03d}"))
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=insert, args=(i,))
                   for i in range(count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])

        report = audit.verify(self.journal)
        self.assertTrue(report["valid"])
        self.assertEqual(report["count"], count)

        doc = _read_json(self.journal)
        previous = None
        keys = list(doc["events"])
        self.assertEqual(keys, sorted(keys))
        for audit_key in keys:
            event, prev, digest = doc["events"][audit_key]
            self.assertEqual(prev, previous)
            self.assertEqual(digest, _digest_of(audit_key, event, prev))
            previous = digest
        self.assertEqual(doc["head"], previous)

    def test_chain_continues_from_persisted_last_item_after_restart(self) -> None:
        # Simulate a process restart: drop the in-memory store registry
        # so the next call has no cached suffix state whatsoever.
        audit.record(self.journal, "a", _event(key="ka"))
        persisted_head = _read_json(self.journal)["head"]
        audit._stores.clear()
        audit.record(self.journal, "b", _event(key="kb"))
        doc = _read_json(self.journal)
        self.assertEqual(doc["events"]["b"][1], persisted_head)
        report = audit.verify(self.journal)
        self.assertTrue(report["valid"])
        self.assertEqual(doc["head"], report["head"])


if __name__ == "__main__":
    unittest.main()
