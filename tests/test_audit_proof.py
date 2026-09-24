"""Independent checkpoints and offline range proofs (audit_proof).

Covers audit_proof.export and audit_proof.verify: the on-disk checkpoint
format and its anchor digest chain, growth-only generation semantics,
idempotent exports, final/closing behaviour, generation sequencing,
offline verification from nothing but checkpoint and proof, detection
of every tampered proof field, rejection of version 1 and broken
journals, structural checkpoint anomalies, validation-before-read,
missing-file errors, commit rollback and concurrent/cross-process
anchor continuation.
"""

from __future__ import annotations

import base64
import hashlib
import json
import multiprocessing
import os
import threading
import unittest
from tempfile import TemporaryDirectory

from carbon_market import audit, audit_proof


def _event(op: str = "copy", target: str = "t.history", key: str = "batch-key",
           changed: bool = True, error: str | None = None,
           stage: str | None = None) -> dict[str, object]:
    return {"op": op, "target": target, "key": key, "changed": changed,
            "error": error, "stage": stage}


def _anchor_digest(anchor: dict) -> str:
    payload = {field: anchor[field] for field in
               ("generation", "query", "events", "log_digest", "root",
                "previous", "closed")}
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class _Fixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        d = self.tmp.name
        self.journal = os.path.join(d, "audit.json")
        self.checkpoint = os.path.join(d, "checkpoint.json")

    def _record_all(self, *keys: str) -> None:
        for audit_key in keys:
            audit.record(self.journal, audit_key, _event())

    def _anchors(self) -> list[dict]:
        return json.loads(open(self.checkpoint, encoding="utf-8").read())[
            "anchors"]

    def _write_v2_journal(self, path: str, *keys: str,
                          event: dict | None = None) -> None:
        # Build a complete version 2 journal straight through record()
        # against a fresh path, so its digest chain is always valid.
        if os.path.exists(path):
            os.unlink(path)
        for audit_key in keys:
            audit.record(path, audit_key, _event() if event is None else event)


class ExportFormatTest(_Fixture):
    def test_creates_compact_checkpoint_with_public_field_order(self) -> None:
        self._record_all("a", "b")
        audit_proof.export(self.journal, self.checkpoint, "g1", False)
        raw = open(self.checkpoint, "rb").read()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        self.assertIn(b'"version":1,"anchors"', raw)
        doc = json.loads(raw)
        self.assertEqual(list(doc), ["version", "anchors"])
        self.assertEqual(doc["version"], 1)
        self.assertEqual(len(doc["anchors"]), 1)
        anchor = doc["anchors"][0]
        self.assertEqual(list(anchor),
                         ["generation", "query", "events", "log_digest",
                          "root", "previous", "closed", "digest"])

    def test_anchor_binds_generation_manifest_log_root_prev_and_state(self) -> None:
        self._record_all("a", "b")
        proof = audit_proof.export(self.journal, self.checkpoint, "g1", False)
        anchor = self._anchors()[0]
        self.assertEqual(anchor["generation"], "g1")
        self.assertEqual(anchor["query"],
                         {"cursor": None, "limit": 100, "op": None,
                          "stage": None, "key": None})
        self.assertEqual([event_key for event_key, _ in anchor["events"]],
                         ["a", "b"])
        log_bytes = open(self.journal, "rb").read()
        self.assertEqual(anchor["log_digest"],
                         hashlib.sha256(log_bytes).hexdigest())
        self.assertEqual(anchor["root"], audit.verify(self.journal)["head"])
        self.assertIsNone(anchor["previous"])
        self.assertIs(anchor["closed"], False)
        self.assertEqual(anchor["digest"], _anchor_digest(anchor))
        # The proof names that anchor and carries the log's raw bytes.
        self.assertEqual(proof["anchor"], anchor["digest"])
        self.assertEqual(base64.b64decode(proof["log"]), log_bytes)

    def test_non_ascii_stage_is_written_through(self) -> None:
        audit.record(self.journal, "k",
                     _event(op="restore", changed=False, error="ValueError",
                            stage="校验"))
        audit_proof.export(self.journal, self.checkpoint, "g1", False)
        raw = open(self.checkpoint, "rb").read()
        self.assertIn("校验".encode("utf-8"), raw)
        self.assertNotIn(b"\\u6821", raw)

    def test_query_parameters_are_bound_into_the_anchor(self) -> None:
        self._record_all("a", "b")
        audit_proof.export(self.journal, self.checkpoint, "g1", False,
                           cursor="a", limit=1, op="copy", stage="成功",
                           key="batch-key")
        anchor = self._anchors()[0]
        self.assertEqual(anchor["query"],
                         {"cursor": "a", "limit": 1, "op": "copy",
                          "stage": "成功", "key": "batch-key"})
        self.assertEqual([event_key for event_key, _ in anchor["events"]],
                         ["b"])


class IdempotenceTest(_Fixture):
    def test_identical_repeat_appends_nothing_and_returns_same_proof(self) -> None:
        self._record_all("a")
        first = audit_proof.export(self.journal, self.checkpoint, "g1", False)
        raw_after_first = open(self.checkpoint, "rb").read()
        again = audit_proof.export(self.journal, self.checkpoint, "g1", False)
        self.assertEqual(again, first)
        self.assertEqual(open(self.checkpoint, "rb").read(), raw_after_first)
        self.assertEqual(len(self._anchors()), 1)

    def test_final_flag_alone_is_idempotent_after_close(self) -> None:
        self._record_all("a")
        closing = audit_proof.export(self.journal, self.checkpoint, "g1", True)
        self.assertIs(closing["closed"], True)
        self.assertEqual(len(self._anchors()), 1)
        raw = open(self.checkpoint, "rb").read()
        # Either final flag reproduces the identical closed proof.
        for final in (True, False):
            with self.subTest(final=final):
                replay = audit_proof.export(
                    self.journal, self.checkpoint, "g1", final)
                self.assertEqual(replay, closing)
                self.assertEqual(open(self.checkpoint, "rb").read(), raw)

    def test_changing_only_closed_state_appends_one_closing_anchor(self) -> None:
        self._record_all("a")
        audit_proof.export(self.journal, self.checkpoint, "g1", False)
        closing = audit_proof.export(self.journal, self.checkpoint, "g1", True)
        self.assertIs(closing["closed"], True)
        anchors = self._anchors()
        self.assertEqual(len(anchors), 2)
        self.assertFalse(anchors[0]["closed"])
        self.assertTrue(anchors[1]["closed"])
        self.assertEqual(anchors[1]["previous"], anchors[0]["digest"])
        self.assertEqual(anchors[1]["digest"], _anchor_digest(anchors[1]))


class GenerationGrowthTest(_Fixture):
    def _export(self, generation: str, final: bool = False, **kwargs: object):
        return audit_proof.export(self.journal, self.checkpoint, generation,
                                  final, **kwargs)

    def test_new_events_in_open_generation_extend_the_manifest(self) -> None:
        self._record_all("a", "b")
        first = self._export("g1")
        self.assertEqual(
            [k for k, _ in first["events"]], ["a", "b"])
        self._record_all("c")
        second = self._export("g1")
        self.assertEqual(
            [k for k, _ in second["events"]], ["a", "b", "c"])
        anchors = self._anchors()
        self.assertEqual(len(anchors), 2)
        self.assertEqual(anchors[1]["previous"], anchors[0]["digest"])
        # Existing events survive byte-for-byte.
        self.assertEqual(anchors[1]["events"][:2], anchors[0]["events"])

    def test_dropped_existing_event_is_rejected(self) -> None:
        # A rotated journal missing a previously anchored event still has
        # a complete v2 chain of its own; the generation check is what
        # must reject the rollback.
        self._record_all("a", "b")
        self._export("g1")
        rotated = os.path.join(self.tmp.name, "rotated.json")
        self._write_v2_journal(rotated, "a")
        with self.assertRaises(ValueError):
            audit_proof.export(rotated, self.checkpoint, "g1", False)
        self.assertEqual(len(self._anchors()), 1)

    def test_rewritten_existing_event_is_rejected(self) -> None:
        self._record_all("a")
        self._export("g1")
        rotated = os.path.join(self.tmp.name, "rotated.json")
        self._write_v2_journal(rotated, "a",
                               event=_event(op="restore"))
        with self.assertRaises(ValueError):
            audit_proof.export(rotated, self.checkpoint, "g1", False)
        self.assertEqual(len(self._anchors()), 1)

    def test_narrowed_filter_that_drops_an_event_is_rejected(self) -> None:
        audit.record(self.journal, "a", _event(op="copy"))
        audit.record(self.journal, "b",
                     _event(op="restore", changed=False, error="ValueError",
                            stage="校验"))
        self._export("g1")  # page covers both events
        with self.assertRaises(ValueError):
            self._export("g1", op="copy")  # b would disappear
        self.assertEqual(len(self._anchors()), 1)

    def test_filter_that_only_adds_new_keys_extends(self) -> None:
        audit.record(self.journal, "a", _event(key="alpha"))
        self._export("g1", key="alpha")
        audit.record(self.journal, "b", _event(key="alpha"))
        audit.record(self.journal, "c", _event(key="beta"))
        grown = self._export("g1", key="alpha")
        self.assertEqual([k for k, _ in grown["events"]], ["a", "b"])

    def test_change_after_close_is_rejected_even_adding_keys(self) -> None:
        self._record_all("a")
        self._export("g1", True)
        self._record_all("b")
        with self.assertRaises(ValueError):
            self._export("g1", True)
        self.assertEqual(len(self._anchors()), 1)


class GenerationSequenceTest(_Fixture):
    def test_new_generation_requires_previous_closed(self) -> None:
        self._record_all("a")
        audit_proof.export(self.journal, self.checkpoint, "g1", False)
        with self.assertRaises(ValueError):
            audit_proof.export(self.journal, self.checkpoint, "g2", False)
        self.assertEqual(len(self._anchors()), 1)

    def test_generations_advance_in_code_point_order(self) -> None:
        self._record_all("a")
        audit_proof.export(self.journal, self.checkpoint, "g1", True)
        audit_proof.export(self.journal, self.checkpoint, "g2", False)
        self.assertEqual(
            [anchor["generation"] for anchor in self._anchors()],
            ["g1", "g2"])
        # Repeating an earlier name regresses; the identical re-export of
        # the just-closed g2 is the one allowed idempotent replay.
        audit_proof.export(self.journal, self.checkpoint, "g2", True)
        idempotent = audit_proof.export(
            self.journal, self.checkpoint, "g2", False)
        self.assertEqual(
            [anchor["generation"] for anchor in self._anchors()],
            ["g1", "g2", "g2"])
        with self.assertRaises(ValueError):
            audit_proof.export(self.journal, self.checkpoint, "g1", False)
        audit_proof.export(self.journal, self.checkpoint, "g3", True)
        self.assertEqual(
            [anchor["generation"] for anchor in self._anchors()],
            ["g1", "g2", "g2", "g3"])
        self.assertEqual(
            idempotent["anchor"], self._anchors()[2]["digest"])

    def test_anchor_chain_links_across_log_rotation(self) -> None:
        # The anchor chain depends only on previous anchor digests, so it
        # stays continuous when the bound journal bytes change.
        self._record_all("a")
        audit_proof.export(self.journal, self.checkpoint, "g1", True)
        rotated = os.path.join(self.tmp.name, "rotated.json")
        self._write_v2_journal(rotated, "a", "b")
        audit_proof.export(rotated, self.checkpoint, "g2", True)
        anchors = self._anchors()
        self.assertEqual(len(anchors), 2)
        self.assertNotEqual(anchors[0]["log_digest"], anchors[1]["log_digest"])
        self.assertEqual(anchors[1]["previous"], anchors[0]["digest"])
        # The on-disk checkpoint itself validates as a complete chain.
        audit_proof.verify(
            self.checkpoint,
            audit_proof.export(rotated, self.checkpoint, "g2", True))


class JournalRequirementsTest(_Fixture):
    def test_version_1_journal_is_rejected_without_creating_checkpoint(self) -> None:
        doc = {"version": 1, "events": {"k": _event()}}
        with open(self.journal, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(doc, ensure_ascii=False,
                                    separators=(",", ":")) + "\n")
        with self.assertRaises(ValueError):
            audit_proof.export(self.journal, self.checkpoint, "g1", False)
        self.assertFalse(os.path.exists(self.checkpoint))

    def test_broken_chain_is_rejected_without_appending(self) -> None:
        self._record_all("a", "b")
        audit_proof.export(self.journal, self.checkpoint, "g1", False)
        doc = json.loads(open(self.journal, encoding="utf-8").read())
        doc["events"]["b"][0]["changed"] = False
        with open(self.journal, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(doc, separators=(",", ":")))
        with self.assertRaises(ValueError):
            audit_proof.export(self.journal, self.checkpoint, "g1", False)
        self.assertEqual(len(self._anchors()), 1)

    def test_missing_journal_is_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            audit_proof.export(
                os.path.join(self.tmp.name, "missing.json"),
                self.checkpoint, "g1", False)
        self.assertFalse(os.path.exists(self.checkpoint))


class ExportValidationTest(_Fixture):
    def setUp(self) -> None:
        super().setUp()
        self._record_all("k")

    def test_bad_arguments_raise_value_error_before_files_open(self) -> None:
        calls = [
            {"audit_path": ""}, {"audit_path": 1}, {"audit_path": None},
            {"checkpoint_path": ""}, {"checkpoint_path": 2},
            {"generation": ""}, {"generation": 3},
            {"final": 0}, {"final": 1}, {"final": "yes"},
            {"cursor": ""}, {"cursor": 5},
            {"limit": 0}, {"limit": 1001}, {"limit": True},
            {"limit": 1.5}, {"limit": "5"},
            {"op": "delete"}, {"op": 1},
            {"stage": "失败"}, {"stage": 0},
            {"key": ""}, {"key": 7},
        ]
        for kwargs in calls:
            with self.subTest(kwargs=kwargs):
                kwargs.setdefault("audit_path", self.journal)
                kwargs.setdefault("checkpoint_path", self.checkpoint)
                kwargs.setdefault("generation", "g1")
                with self.assertRaises(ValueError):
                    audit_proof.export(**kwargs)  # type: ignore[arg-type]

    def test_checkpoint_path_naming_the_journal_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            audit_proof.export(self.journal, self.journal, "g1", False)

    def test_missing_checkpoint_parent_directory_is_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            audit_proof.export(
                self.journal,
                os.path.join(self.tmp.name, "nope", "cp.json"),
                "g1", False)


class CheckpointAnomalyTest(_Fixture):
    def setUp(self) -> None:
        super().setUp()
        self._record_all("k")
        audit_proof.export(self.journal, self.checkpoint, "g1", True)

    def _corrupt(self, mutate) -> None:
        path = self.checkpoint
        doc = json.loads(open(path, encoding="utf-8").read())
        mutate(doc)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(doc, ensure_ascii=False,
                                    separators=(",", ":")) + "\n")

    def test_structural_anomalies_raise_value_error(self) -> None:
        raw_cases = [
            b"",
            b"{not json",
            b"\xff\xfe",
            b'{"anchors":[],"version":1}',
            b'{"version":2,"anchors":[]}\n',
            b'{"version":1,"anchors":{}}\n',
            b'{"version":true,"anchors":[]}\n',
            b'{"version":1,"anchors":[{}]}\n',
        ]
        for raw in raw_cases:
            with self.subTest(raw=raw):
                with open(self.checkpoint, "wb") as handle:
                    handle.write(raw)
                with self.assertRaises(ValueError):
                    audit_proof.export(
                        self.journal, self.checkpoint, "g2", False)
                with self.assertRaises(ValueError):
                    audit_proof.verify(
                        self.checkpoint, {"version": 1, "generation": "g1",
                                          "closed": True, "query": {},
                                          "events": [], "next": None,
                                          "root": None, "log": "",
                                          "anchor": "0" * 64})

    def test_anchor_digest_or_link_tampering_is_rejected(self) -> None:
        def break_digest(doc: dict) -> None:
            doc["anchors"][0]["digest"] = "0" * 64

        def break_link(doc: dict) -> None:
            doc["anchors"][0]["previous"] = "1" * 64

        def break_closed(doc: dict) -> None:
            doc["anchors"][0]["closed"] = False

        for mutate in (break_digest, break_link, break_closed):
            with self.subTest(mutate=mutate.__name__):
                self._corrupt(mutate)
                with self.assertRaises(ValueError):
                    audit_proof.export(
                        self.journal, self.checkpoint, "g2", False)

    def test_generation_rewritten_to_regress_is_rejected(self) -> None:
        # Two well-formed anchors whose generation segment regresses make
        # the checkpoint structurally invalid rather than salvageable.
        self._write_v2_journal(self.journal, "k", "l")
        audit_proof.export(self.journal, self.checkpoint, "g2", True)

        def regress(doc: dict) -> None:
            # Rewrite the second segment's name backwards and reseal the
            # anchor, so its own digest still checks and the failure is
            # the generation-segment regression rather than a bad digest.
            anchor = doc["anchors"][1]
            anchor["generation"] = "g0"
            payload = {field: anchor[field] for field in
                       ("generation", "query", "events", "log_digest",
                        "root", "previous", "closed")}
            anchor["digest"] = hashlib.sha256(
                json.dumps(payload, ensure_ascii=False,
                           separators=(",", ":")).encode("utf-8")).hexdigest()

        self._corrupt(regress)
        with self.assertRaises(ValueError):
            audit_proof.export(self.journal, self.checkpoint, "g3", False)


class VerifySuccessTest(_Fixture):
    def test_verify_returns_result_generation_and_closed_state(self) -> None:
        audit.record(self.journal, "k-01", _event(op="copy", key="alpha"))
        audit.record(self.journal, "k-02",
                     _event(op="restore", changed=False, error="ValueError",
                            stage="校验"))
        audit.record(self.journal, "k-03", _event(op="copy", key="alpha"))
        proof = audit_proof.export(self.journal, self.checkpoint, "g1", False,
                                   limit=2)
        self.assertEqual([k for k, _ in proof["events"]], ["k-01", "k-02"])
        self.assertEqual(proof["next"], "k-02")

        # Verification needs neither the audit journal nor its directory.
        journal_copy = proof
        os.unlink(self.journal)
        result = audit_proof.verify(self.checkpoint, journal_copy)
        self.assertEqual(list(result), ["events", "next", "generation",
                                        "closed"])
        self.assertEqual([k for k, _ in result["events"]],
                         ["k-01", "k-02"])
        self.assertEqual(result["next"], "k-02")
        self.assertEqual(result["generation"], "g1")
        self.assertIs(result["closed"], False)

    def test_verify_closed_generation_proof(self) -> None:
        self._record_all("a")
        proof = audit_proof.export(self.journal, self.checkpoint, "g1", True)
        result = audit_proof.verify(self.checkpoint, proof)
        self.assertIs(result["closed"], True)

    def test_verify_locates_any_historical_anchor(self) -> None:
        self._record_all("a")
        first = audit_proof.export(self.journal, self.checkpoint, "g1", True)
        self._record_all("b")
        audit_proof.export(self.journal, self.checkpoint, "g2", True)
        # The older proof still verifies against the grown checkpoint.
        result = audit_proof.verify(self.checkpoint, first)
        self.assertEqual(result["generation"], "g1")
        self.assertEqual([k for k, _ in result["events"]], ["a"])

    def test_returned_events_are_copies(self) -> None:
        self._record_all("a")
        proof = audit_proof.export(self.journal, self.checkpoint, "g1", False)
        result = audit_proof.verify(self.checkpoint, proof)
        result["events"][0][1]["op"] = "mutated"
        again = audit_proof.verify(self.checkpoint, proof)
        self.assertEqual(again["events"][0][1]["op"], "copy")

    def test_missing_checkpoint_is_file_not_found(self) -> None:
        self._record_all("a")
        proof = audit_proof.export(self.journal, self.checkpoint, "g1", False)
        with self.assertRaises(FileNotFoundError):
            audit_proof.verify(
                os.path.join(self.tmp.name, "missing-cp.json"), proof)

    def test_bad_checkpoint_argument_raises_value_error(self) -> None:
        for bad in ("", 1, None):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    audit_proof.verify(bad, {})  # type: ignore[arg-type]


class VerifyTamperTest(_Fixture):
    def setUp(self) -> None:
        super().setUp()
        self._record_all("a", "b")
        self.proof = audit_proof.export(
            self.journal, self.checkpoint, "g1", False, limit=1)

    def _verify(self, proof: dict) -> None:
        audit_proof.verify(self.checkpoint, proof)

    def test_malformed_proof_envelope_raises_value_error(self) -> None:
        for bad in (None, 4, [], {},
                    {"version": 1, "generation": "g1", "closed": False,
                     "query": {}, "events": [], "next": None, "root": None,
                     "log": "AA", "anchor": "0" * 64, "extra": 1},
                    {"version": 2, "generation": "g1", "closed": False,
                     "query": {}, "events": [], "next": None, "root": None,
                     "log": "AA", "anchor": "0" * 64},
                    {"version": "1", "generation": "g1", "closed": False,
                     "query": {}, "events": [], "next": None, "root": None,
                     "log": "AA", "anchor": "0" * 64}):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    self._verify(bad)  # type: ignore[arg-type]

    def test_any_proof_field_change_is_rejected(self) -> None:
        good_log = base64.b64decode(self.proof["log"])

        def tampered(**changes: object) -> dict:
            proof = json.loads(json.dumps(self.proof))
            proof.update(changes)
            return proof

        candidates = [
            tampered(generation="g2"),
            tampered(closed=True),
            tampered(next="b"),
            tampered(root="0" * 64),
            tampered(anchor="0" * 64),
            tampered(events=[["a", _event(op="restore")]]),
            tampered(log=base64.b64encode(b"{}").decode("ascii")),
            tampered(log=base64.b64encode(good_log + b" ").decode("ascii")),
            tampered(log="@@not-base64@@"),
        ]
        query = json.loads(json.dumps(self.proof["query"]))
        query["limit"] = 5
        candidates.append(tampered(query=query))
        query = json.loads(json.dumps(self.proof["query"]))
        query["op"] = "restore"
        candidates.append(tampered(query=query))

        for proof in candidates:
            with self.subTest(proof=proof):
                with self.assertRaises(ValueError):
                    self._verify(proof)

    def test_log_byte_tampering_even_with_valid_chain_is_rejected(self) -> None:
        # Rebuild the embedded journal as a complete v2 document with
        # changed content: its internal chain is valid, but the anchor's
        # log digest and root no longer match.
        other = os.path.join(self.tmp.name, "other.json")
        self._write_v2_journal(other, "a", event=_event(changed=False))
        proof = json.loads(json.dumps(self.proof))
        proof["log"] = base64.b64encode(
            open(other, "rb").read()).decode("ascii")
        with self.assertRaises(ValueError):
            self._verify(proof)

    def test_unknown_anchor_digest_is_rejected(self) -> None:
        proof = json.loads(json.dumps(self.proof))
        proof["anchor"] = "f" * 64
        with self.assertRaises(ValueError):
            self._verify(proof)

    def test_version_1_or_broken_embedded_log_is_rejected(self) -> None:
        v1 = (json.dumps({"version": 1, "events": {"a": _event()}},
                         ensure_ascii=False, separators=(",", ":"))
              + "\n").encode("utf-8")
        broken_doc = json.loads(open(self.journal, encoding="utf-8").read())
        broken_doc["events"]["b"][0]["changed"] = False
        broken = (json.dumps(broken_doc, ensure_ascii=False,
                             separators=(",", ":")) + "\n").encode("utf-8")
        for raw in (v1, broken):
            with self.subTest(raw=raw[:12]):
                proof = json.loads(json.dumps(self.proof))
                proof["log"] = base64.b64encode(raw).decode("ascii")
                with self.assertRaises(ValueError):
                    self._verify(proof)


class CommitFailureTest(_Fixture):
    def test_failed_commit_restores_existing_checkpoint(self) -> None:
        self._record_all("a")
        audit_proof.export(self.journal, self.checkpoint, "g1", False)
        before = open(self.checkpoint, "rb").read()

        calls = {"n": 0}
        real_fsync = audit._fsync_dir

        def flaky(directory: str) -> None:
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("injected checkpoint directory fsync failure")
            return real_fsync(directory)

        original = audit._fsync_dir
        audit._fsync_dir = flaky  # type: ignore[assignment]
        try:
            with self.assertRaises(OSError):
                audit_proof.export(self.journal, self.checkpoint, "g1", True)
        finally:
            audit._fsync_dir = original  # type: ignore[assignment]
        self.assertEqual(open(self.checkpoint, "rb").read(), before)
        self.assertEqual(len(self._anchors()), 1)

    def test_failed_commit_removes_newly_created_checkpoint(self) -> None:
        self._record_all("a")
        calls = {"n": 0}
        real_fsync = audit._fsync_dir

        def flaky(directory: str) -> None:
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("injected checkpoint directory fsync failure")
            return real_fsync(directory)

        original = audit._fsync_dir
        audit._fsync_dir = flaky  # type: ignore[assignment]
        try:
            with self.assertRaises(OSError):
                audit_proof.export(self.journal, self.checkpoint, "g1", False)
        finally:
            audit._fsync_dir = original  # type: ignore[assignment]
        self.assertFalse(os.path.exists(self.checkpoint))


class ConcurrencyTest(_Fixture):
    def test_concurrent_exporters_only_see_complete_checkpoints(self) -> None:
        audit.record(self.journal, "k-base", _event())
        audit_proof.export(self.journal, self.checkpoint, "g1", False)

        proofs: list[dict] = []
        errors: list[BaseException] = []

        def grow_and_export(index: int) -> None:
            try:
                # Each thread rotates the journal with its own event, then
                # races the others through the exclusive checkpoint lock;
                # every observed non-identical state appends one anchor.
                audit.record(self.journal, f"k-{index:02d}", _event())
                proof = audit_proof.export(
                    self.journal, self.checkpoint, "g1", False)
                audit_proof.verify(self.checkpoint, proof)
                proofs.append(proof)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=grow_and_export, args=(i,))
                   for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])

        anchors = self._anchors()
        self.assertGreaterEqual(len(anchors), 2)
        # Every anchor links a complete chain and the manifest grows.
        previous = None
        seen: set[str] = set()
        for anchor in anchors:
            self.assertEqual(anchor["previous"], previous)
            self.assertEqual(anchor["digest"], _anchor_digest(anchor))
            keys = {key for key, _ in anchor["events"]}
            self.assertTrue(seen.issubset(keys))
            seen = keys
            previous = anchor["digest"]
        # The final anchor's manifest contains every thread's event.
        self.assertEqual(seen, {"k-base"} | {f"k-{i:02d}" for i in range(8)})
        # Every proof handed out still verifies against the final chain.
        for proof in proofs:
            audit_proof.verify(self.checkpoint, proof)


def _child_export(journal: str, checkpoint: str, queue: "multiprocessing.Queue") -> None:
    audit.record(journal, "k-child",
                 _event(op="restore", changed=False, error="ValueError",
                        stage="同步"))
    proof = audit_proof.export(journal, checkpoint, "g1", False)
    queue.put(proof)


class CrossProcessTest(_Fixture):
    def test_anchor_chain_continues_across_a_process_restart(self) -> None:
        self._record_all("a")
        proof = audit_proof.export(self.journal, self.checkpoint, "g1", False)
        before_anchor = proof["anchor"]

        ctx = multiprocessing.get_context("fork")
        queue: multiprocessing.Queue = ctx.Queue()
        child = ctx.Process(target=_child_export,
                            args=(self.journal, self.checkpoint, queue))
        child.start()
        child.join()
        self.assertEqual(child.exitcode, 0)
        child_proof = queue.get()

        # A fresh process extended the same on-disk anchor chain.
        anchors = self._anchors()
        self.assertEqual(len(anchors), 2)
        self.assertEqual(anchors[1]["previous"], before_anchor)
        audit_proof.verify(self.checkpoint, proof)
        result = audit_proof.verify(self.checkpoint, child_proof)
        self.assertEqual(
            [key for key, _ in result["events"]], ["a", "k-child"])


if __name__ == "__main__":
    unittest.main()
