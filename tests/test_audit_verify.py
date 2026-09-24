"""Audit digest chain and the read-only audit.verify query.

Covers the sealed version 2 journal format (event triples, predecessor
references and the root head), the verify result contract for version 1
and version 2 journals, the atomic version 1 upgrade on record, the
first-invalid-position reporting for content, link and head deviations,
and the read-only guarantee of every query against a broken chain.
"""

from __future__ import annotations

import hashlib
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


def _digest(audit_key: str, event: dict, previous: str | None) -> str:
    payload = json.dumps([audit_key, event, previous], ensure_ascii=False,
                         separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class _Fixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")

    def _write(self, doc: object) -> None:
        with open(self.journal, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(doc, ensure_ascii=False,
                                    separators=(",", ":")) + "\n")

    def _read_doc(self) -> dict:
        return json.loads(open(self.journal, encoding="utf-8").read())

    def _record_all(self, *keys: str) -> None:
        for audit_key in keys:
            audit.record(self.journal, audit_key, _event())


class SealedFormatTest(_Fixture):
    def test_record_writes_sealed_version_2_document(self) -> None:
        self._record_all("b", "a")
        doc = self._read_doc()
        self.assertEqual(list(doc), ["version", "events", "head"])
        self.assertEqual(doc["version"], 2)
        self.assertEqual(list(doc["events"]), ["a", "b"])
        previous = None
        for audit_key in ("a", "b"):
            event, prev, digest = doc["events"][audit_key]
            self.assertEqual(event, _event())
            self.assertEqual(prev, previous)
            self.assertEqual(digest, _digest(audit_key, event, prev))
            previous = digest
        self.assertEqual(doc["head"], previous)

    def test_middle_insert_recomputes_the_suffix(self) -> None:
        self._record_all("a", "m", "z")
        before = self._read_doc()
        audit.record(self.journal, "c", _event(key="other"))
        doc = self._read_doc()
        self.assertEqual(list(doc["events"]), ["a", "c", "m", "z"])
        # The prefix before the insertion keeps its digests.
        self.assertEqual(doc["events"]["a"], before["events"]["a"])
        # Everything from the insertion point on is resealed.
        self.assertNotEqual(doc["events"]["m"], before["events"]["m"])
        self.assertTrue(audit.verify(self.journal)["valid"])

    def test_verify_reports_complete_chain(self) -> None:
        self._record_all("a", "b", "c")
        result = audit.verify(self.journal)
        self.assertEqual(list(result),
                         ["version", "count", "sealed", "valid",
                          "first_invalid", "head"])
        self.assertEqual(result, {
            "version": 2, "count": 3, "sealed": True, "valid": True,
            "first_invalid": None, "head": self._read_doc()["head"]})

    def test_verify_empty_sealed_journal_is_valid(self) -> None:
        self._write({"version": 2, "events": {}, "head": None})
        self.assertEqual(audit.verify(self.journal), {
            "version": 2, "count": 0, "sealed": True, "valid": True,
            "first_invalid": None, "head": None})


class VersionOneTest(_Fixture):
    def _write_v1(self, *keys: str) -> bytes:
        doc = {"version": 1,
               "events": {key: _event() for key in sorted(keys)}}
        text = json.dumps(doc, ensure_ascii=False,
                          separators=(",", ":")) + "\n"
        with open(self.journal, "w", encoding="utf-8") as handle:
            handle.write(text)
        return text.encode("utf-8")

    def test_verify_reports_unsealed_version_1(self) -> None:
        self._write_v1("a", "b", "c")
        self.assertEqual(audit.verify(self.journal), {
            "version": 1, "count": 3, "sealed": False, "valid": False,
            "first_invalid": None, "head": None})

    def test_reads_and_replays_never_upgrade_version_1(self) -> None:
        before = self._write_v1("a", "b")
        self.assertEqual(audit.get(self.journal, "a"), _event())
        self.assertEqual(audit.search(self.journal)["next"], None)
        _, created = audit.record(self.journal, "a", _event())
        self.assertIs(created, False)
        self.assertEqual(open(self.journal, "rb").read(), before)

    def test_record_upgrades_version_1_atomically_sealing_old_events(self) -> None:
        before = self._write_v1("a", "b")
        _, created = audit.record(self.journal, "c", _event())
        self.assertIs(created, True)
        result = audit.verify(self.journal)
        self.assertEqual(result["version"], 2)
        self.assertEqual(result["count"], 3)
        self.assertTrue(result["valid"])
        # A failed upgrade preserves the complete pre-call bytes.
        with self.assertRaises(OSError):
            audit.record(self.journal, "d", _event(), fault="replace")
        self.assertTrue(audit.verify(self.journal)["valid"])
        with open(self.journal, "w", encoding="utf-8") as handle:
            handle.write(before.decode("utf-8"))
        with self.assertRaises(OSError):
            audit.record(self.journal, "d", _event(), fault="replace")
        self.assertEqual(open(self.journal, "rb").read(), before)


class ChainDeviationTest(_Fixture):
    def test_tampered_event_content_reports_its_position(self) -> None:
        self._record_all("a", "b", "c")
        doc = self._read_doc()
        doc["events"]["b"][0]["changed"] = False
        self._write(doc)
        result = audit.verify(self.journal)
        self.assertFalse(result["valid"])
        self.assertEqual(result["first_invalid"], 1)
        self.assertTrue(result["sealed"])

    def test_broken_predecessor_reference_reports_current_position(self) -> None:
        self._record_all("a", "b", "c")
        doc = self._read_doc()
        doc["events"]["c"][1] = "0" * 64
        doc["events"]["c"][2] = _digest("c", doc["events"]["c"][0], "0" * 64)
        self._write(doc)
        result = audit.verify(self.journal)
        self.assertFalse(result["valid"])
        self.assertEqual(result["first_invalid"], 2)

    def test_first_event_with_non_null_predecessor_reports_zero(self) -> None:
        self._record_all("a", "b")
        doc = self._read_doc()
        doc["events"]["a"][1] = "0" * 64
        doc["events"]["a"][2] = _digest("a", doc["events"]["a"][0], "0" * 64)
        self._write(doc)
        self.assertEqual(audit.verify(self.journal)["first_invalid"], 0)

    def test_head_mismatch_alone_reports_the_count(self) -> None:
        self._record_all("a", "b", "c")
        doc = self._read_doc()
        doc["head"] = "0" * 64
        self._write(doc)
        result = audit.verify(self.journal)
        self.assertFalse(result["valid"])
        self.assertEqual(result["first_invalid"], 3)
        self.assertEqual(result["head"], "0" * 64)

    def test_tail_deletion_reports_the_count(self) -> None:
        self._record_all("a", "b", "c")
        doc = self._read_doc()
        del doc["events"]["c"]
        self._write(doc)
        self.assertEqual(audit.verify(self.journal)["first_invalid"], 2)

    def test_queries_raise_on_broken_chain_without_rewriting(self) -> None:
        self._record_all("a", "b")
        doc = self._read_doc()
        doc["events"]["b"][0]["changed"] = False
        self._write(doc)
        before = open(self.journal, "rb").read()
        with self.assertRaises(ValueError):
            audit.get(self.journal, "a")
        with self.assertRaises(ValueError):
            audit.search(self.journal)
        with self.assertRaises(ValueError):
            audit.record(self.journal, "c", _event())
        with self.assertRaises(ValueError):
            audit.record(self.journal, "a", _event())  # replay still fails
        self.assertEqual(open(self.journal, "rb").read(), before)


class VerifyValidationTest(_Fixture):
    def test_bad_path_raises_value_error(self) -> None:
        for bad in ("", 1, None):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    audit.verify(bad)  # type: ignore[arg-type]

    def test_missing_journal_raises_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            audit.verify(os.path.join(self.tmp.name, "missing.json"))

    def test_malformed_journal_raises_value_error_without_rewriting(self) -> None:
        sealed = json.dumps({"version": 2,
                             "events": {"k": [_event(), None, "zz"]},
                             "head": None}, separators=(",", ":"))
        for text in (
            "{not json",
            "\xff\xfe".encode("latin1"),
            '{"events":{},"version":1}',
            '{"version":-0,"events":{}}',
            '{"version":3,"events":{}}',
            '{"version":2,"events":{}}',
            '{"version":1,"events":{},"head":null}',
            '{"version":2,"events":{"k":[]},"head":null}',
            sealed,
        ):
            with self.subTest(text=text):
                raw = text if isinstance(text, bytes) else text.encode("utf-8")
                with open(self.journal, "wb") as handle:
                    handle.write(raw)
                with self.assertRaises(ValueError):
                    audit.verify(self.journal)
                self.assertEqual(open(self.journal, "rb").read(), raw)

    def test_verify_never_rewrites_the_journal(self) -> None:
        self._record_all("a", "b")
        before = open(self.journal, "rb").read()
        audit.verify(self.journal)
        self.assertEqual(open(self.journal, "rb").read(), before)


if __name__ == "__main__":
    unittest.main()
