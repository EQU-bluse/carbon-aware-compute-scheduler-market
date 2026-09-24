"""Independent checkpoints and offline scope proofs (audit_proof).

Covers audit_proof.export/verify: the proof and checkpoint on-disk
contracts, the anchor digest chain, same-generation append-only
semantics, duplicate exports, the final close, generation sequencing,
log-rotation continuity, offline verification against the checkpoint
alone, rejection of tampered proofs and malformed checkpoints, the
before-any-read argument validation, FileNotFoundError/OSError
boundaries, commit rollback, and concurrent/restarted appenders.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import threading
import unittest
from tempfile import TemporaryDirectory
from unittest import mock

from carbon_market import audit, audit_proof


def _event(op: str = "copy", target: str = "t.history", key: str = "batch-k",
           changed: bool = True, error: str | None = None,
           stage: str | None = None) -> dict[str, object]:
    return {"op": op, "target": target, "key": key, "changed": changed,
            "error": error, "stage": stage}


def _write_bytes(path: str, payload: bytes) -> None:
    with open(path, "wb") as handle:
        handle.write(payload)


def _sealed(events: dict[str, dict]) -> bytes:
    # Build a sealed version 2 document with the same compact convention
    # audit.record uses, allowing hand-crafted tampered journals.
    chain: dict[str, list] = {}
    previous = None
    for audit_key in sorted(events):
        event = events[audit_key]
        payload = json.dumps([audit_key, event, previous], ensure_ascii=False,
                             separators=(",", ":"), allow_nan=False)
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        chain[audit_key] = [event, previous, digest]
        previous = digest
    doc = {"version": 2, "events": chain, "head": previous}
    return (json.dumps(doc, ensure_ascii=False, separators=(",", ":"),
                       allow_nan=False) + "\n").encode("utf-8")


class _Fixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.checkpoint = os.path.join(self.tmp.name, "checkpoint.json")

    def _record_all(self, *keys: str, **event_kw: object) -> None:
        for audit_key in keys:
            audit.record(self.journal, audit_key, _event(**event_kw))

    def _checkpoint(self) -> dict:
        with open(self.checkpoint, encoding="utf-8") as handle:
            return json.load(handle)

    def _anchors(self, generation: str = "g1") -> list[dict]:
        generations = self._checkpoint()["generations"]
        return next(g["anchors"] for g in generations
                    if g["name"] == generation)

    def _export(self, generation: str = "g1", **kw: object) -> dict:
        return audit_proof.export(self.journal, self.checkpoint, generation,
                                  **kw)

    def _verify(self, proof: dict) -> dict:
        return audit_proof.verify(self.checkpoint, proof)


class ExportBasicsTest(_Fixture):
    def test_first_export_creates_checkpoint_with_one_anchor(self) -> None:
        self._record_all("a", "b")
        proof = self._export(limit=1)
        doc = self._checkpoint()
        self.assertEqual(list(doc), ["version", "generations"])
        self.assertEqual(doc["version"], 1)
        self.assertEqual([g["name"] for g in doc["generations"]], ["g1"])
        anchors = doc["generations"][0]["anchors"]
        self.assertEqual(len(anchors), 1)
        anchor = anchors[0]
        self.assertEqual(list(anchor),
                         ["manifest", "log_digest", "head", "previous",
                          "closed", "digest"])
        self.assertIsNone(anchor["previous"])
        self.assertFalse(anchor["closed"])
        self.assertEqual([item[0] for item in anchor["manifest"]], ["a", "b"])
        raw = open(self.journal, "rb").read()
        self.assertEqual(anchor["log_digest"],
                         hashlib.sha256(raw).hexdigest())
        self.assertEqual(anchor["head"], json.loads(raw)["head"])
        expected = hashlib.sha256(json.dumps(
            [anchor["manifest"], anchor["log_digest"], anchor["head"],
             None, False], ensure_ascii=False,
            separators=(",", ":")).encode("utf-8")).hexdigest()
        self.assertEqual(anchor["digest"], expected)

    def test_proof_carries_query_page_and_raw_log_bytes(self) -> None:
        self._record_all("a", "b", "c")
        proof = self._export(limit=2, op="copy")
        self.assertEqual(list(proof),
                         ["version", "generation", "params", "result",
                          "log_bytes", "log_digest", "head", "closed",
                          "anchor_digest", "checkpoint_etag"])
        self.assertEqual(proof["version"], 2)
        # The proof binds the strong tag of the checkpoint bytes left
        # behind by its own anchor commit -- the exact tag the
        # checkpoint download serves.
        raw_checkpoint = open(self.checkpoint, "rb").read()
        self.assertEqual(
            proof["checkpoint_etag"],
            '"' + hashlib.sha256(raw_checkpoint).hexdigest() + '"')
        self.assertEqual(proof["params"],
                         {"cursor": None, "limit": 2, "op": "copy",
                          "stage": None, "key": None})
        self.assertEqual([item[0] for item in proof["result"]["events"]],
                         ["a", "b"])
        self.assertEqual(proof["result"]["next"], "b")
        self.assertEqual(proof["log_bytes"],
                         open(self.journal, encoding="utf-8").read())
        self.assertEqual(proof["generation"], "g1")
        self.assertFalse(proof["closed"])
        # The page is exactly what audit.search returns for the args.
        self.assertEqual(
            proof["result"],
            audit.search(self.journal, limit=2, op="copy"))

    def test_export_accepts_empty_sealed_journal(self) -> None:
        _write_bytes(self.journal,
                     b'{"version":2,"events":{},"head":null}\n')
        proof = self._export()
        self.assertEqual(proof["result"], {"events": [], "next": None})
        self.assertEqual(self._anchors()[0]["manifest"], [])
        self.assertIsNone(self._anchors()[0]["head"])
        self._verify(proof)

    def test_non_ascii_content_is_digested_compact_utf8(self) -> None:
        audit.record(self.journal, "a",
                     _event(target="目标.history", key="历史批次"))
        proof = self._export()
        self.assertIn("目标", proof["log_bytes"])
        self._verify(proof)


class DuplicateExportTest(_Fixture):
    def test_repeated_identical_export_appends_nothing_and_matches(self) -> None:
        self._record_all("a")
        first = self._export(limit=2)
        before = open(self.checkpoint, "rb").read()
        second = self._export(limit=2)
        self.assertEqual(second, first)
        self.assertEqual(open(self.checkpoint, "rb").read(), before)
        self.assertEqual(len(self._anchors()), 1)
        self.assertEqual(second["anchor_digest"], first["anchor_digest"])

    def test_different_query_on_unchanged_snapshot_appends_nothing(self) -> None:
        self._record_all("a", "b", "c")
        self._export(limit=2)
        paged = self._export(limit=1, cursor="a")
        self.assertEqual(len(self._anchors()), 1)
        self.assertEqual(
            [item[0] for item in paged["result"]["events"]], ["b"])
        # The different query still verifies against the one anchor.
        result = self._verify(paged)
        self.assertEqual(result["events"], paged["result"]["events"])
        self.assertEqual(result["next"], "b")


class AppendAndGenerationTest(_Fixture):
    def test_new_events_append_an_anchor_chaining_to_the_previous(self) -> None:
        self._record_all("a", "b")
        p1 = self._export()
        self._record_all("c")
        p2 = self._export()
        anchors = self._anchors()
        self.assertEqual(len(anchors), 2)
        self.assertEqual(anchors[1]["previous"], anchors[0]["digest"])
        self.assertEqual(
            [item[0] for item in anchors[0]["manifest"]], ["a", "b"])
        self.assertEqual(
            [item[0] for item in anchors[1]["manifest"]], ["a", "b", "c"])
        self.assertNotEqual(anchors[0]["digest"], anchors[1]["digest"])
        self.assertFalse(anchors[0]["closed"])
        self.assertFalse(anchors[1]["closed"])
        # Only the proof bound to the current checkpoint snapshot
        # verifies; p1 names the pre-append bytes and is rejected.
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._verify(p1)
        self._verify(p2)

    def test_deleted_event_is_rejected_without_appending(self) -> None:
        self._record_all("a", "b", "c")
        self._export()
        payload = _sealed({"a": _event(), "c": _event()})
        _write_bytes(self.journal, payload)
        with self.assertRaises(ValueError):
            self._export()
        self.assertEqual(len(self._anchors()), 1)

    def test_rewritten_known_event_is_rejected_without_appending(self) -> None:
        self._record_all("a", "b")
        self._export()
        payload = _sealed({"a": _event(changed=False), "b": _event(),
                           "c": _event()})
        _write_bytes(self.journal, payload)
        with self.assertRaises(ValueError):
            self._export()
        self.assertEqual(len(self._anchors()), 1)

    def test_retreated_snapshot_is_rejected_without_appending(self) -> None:
        self._record_all("a", "b", "c")
        self._export()
        payload = _sealed({"a": _event(), "b": _event()})
        _write_bytes(self.journal, payload)
        with self.assertRaises(ValueError):
            self._export()
        self.assertEqual(len(self._anchors()), 1)

    def test_log_rotation_with_same_events_appends_continuous_anchor(self) -> None:
        self._record_all("a")
        p1 = self._export()
        # Rotate the bytes (extra newline) without touching events; the
        # chain stays continuous and the new proof verifies offline.
        with open(self.journal, "ab") as handle:
            handle.write(b"\n")
        p2 = self._export()
        anchors = self._anchors()
        self.assertEqual(len(anchors), 2)
        self.assertEqual(anchors[1]["previous"], anchors[0]["digest"])
        self.assertEqual(anchors[1]["manifest"], anchors[0]["manifest"])
        self.assertNotEqual(anchors[1]["log_digest"],
                            anchors[0]["log_digest"])
        self.assertEqual(p2["head"], p1["head"])
        # The rotation changed the checkpoint bytes, so the pre-rotation
        # proof is bound to a snapshot that no longer exists.
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._verify(p1)
        self._verify(p2)

    def _extend_checkpoint(self, manifest: list) -> None:
        # Append a hand-built second anchor carrying ``manifest`` to the
        # single-anchor checkpoint, keeping the anchor chain intact.
        doc = self._checkpoint()
        anchor = doc["generations"][0]["anchors"][0]
        following = dict(anchor, manifest=manifest,
                         previous=anchor["digest"])
        following["digest"] = audit_proof._anchor_digest(
            manifest, anchor["log_digest"], anchor["head"],
            anchor["digest"], False)
        doc["generations"][0]["anchors"].append(following)
        _write_bytes(self.checkpoint,
                     (json.dumps(doc, ensure_ascii=False,
                                 separators=(",", ":")) + "\n").encode())

    def test_new_keys_may_land_anywhere_in_code_point_order(self) -> None:
        # Growth is judged key by key: a checkpoint whose second anchor
        # inserts "b" between the anchored "a" and "c" -- every old
        # [key, digest] pair intact -- is a valid extension.
        self._record_all("a", "c")
        proof = self._export()
        manifest = self._anchors()[0]["manifest"]
        self._extend_checkpoint(
            [manifest[0], ["b", "0" * 64], manifest[1]])
        # The extended checkpoint still validates as a snapshot, but
        # the old proof is bound to the pre-extension bytes and no
        # longer verifies against it.
        audit_proof.read_snapshot(self.checkpoint)
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._verify(proof)

    def test_inserted_key_cannot_rewrite_a_known_digest(self) -> None:
        self._record_all("a", "c")
        proof = self._export()
        manifest = self._anchors()[0]["manifest"]
        # "c" keeps its key but its digest changed: a rewrite, not growth.
        self._extend_checkpoint(
            [manifest[0], ["b", "0" * 64], ["c", "1" * 64]])
        with self.assertRaises(ValueError):
            self._export()
        # The malformed checkpoint fails verification as well.
        with self.assertRaises(ValueError):
            self._verify(proof)


class FinalCloseTest(_Fixture):
    def test_final_appends_closing_anchor_pinning_same_snapshot(self) -> None:
        self._record_all("a")
        open_proof = self._export()
        closed_proof = self._export(final=True)
        anchors = self._anchors()
        self.assertEqual(len(anchors), 2)
        self.assertFalse(anchors[0]["closed"])
        self.assertTrue(anchors[1]["closed"])
        self.assertEqual(anchors[1]["manifest"], anchors[0]["manifest"])
        self.assertEqual(anchors[1]["previous"], anchors[0]["digest"])
        self.assertFalse(open_proof["closed"])
        self.assertTrue(closed_proof["closed"])
        # The closing anchor changed the checkpoint bytes, so the open
        # proof's snapshot binding no longer matches.
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._verify(open_proof)
        self.assertEqual(
            self._verify(closed_proof)["closed"], True)

    def test_closed_generation_accepts_only_identical_idempotent_export(
            self) -> None:
        self._record_all("a")
        self._export()
        self._export(final=True)
        before = open(self.checkpoint, "rb").read()
        self.assertEqual(len(self._anchors()), 2)
        # Identical snapshot with final=True is the idempotent replay; it
        # appends nothing and names the closing anchor.
        again = self._export(final=True, limit=5)
        self.assertEqual(open(self.checkpoint, "rb").read(), before)
        self.assertTrue(again["closed"])
        self.assertEqual(len(self._anchors()), 2)
        # Any deviation is rejected: final omitted/false, new events.
        with self.assertRaises(ValueError):
            self._export()
        with self.assertRaises(ValueError):
            self._export(final=False)
        self._record_all("b")
        with self.assertRaises(ValueError):
            self._export(final=True)
        self.assertEqual(open(self.checkpoint, "rb").read(), before)


class GenerationSequenceTest(_Fixture):
    def test_new_generation_follows_a_closed_one_and_keeps_one_chain(
            self) -> None:
        self._record_all("a")
        p1 = self._export("g1", final=True)
        self._record_all("b")
        p2 = self._export("g2", final=True)
        doc = self._checkpoint()
        self.assertEqual([g["name"] for g in doc["generations"]],
                         ["g1", "g2"])
        g1_last = doc["generations"][0]["anchors"][-1]
        g2_first = doc["generations"][1]["anchors"][0]
        self.assertTrue(g1_last["closed"])
        # The anchor chain continues across the generation boundary.
        self.assertEqual(g2_first["previous"], g1_last["digest"])
        # p1 is bound to the checkpoint snapshot before g2 opened.
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._verify(p1)
        self.assertEqual(self._verify(p2)["generation"], "g2")

    def test_new_generation_cannot_start_while_previous_is_open(self) -> None:
        self._record_all("a")
        self._export("g1")
        with self.assertRaises(ValueError):
            self._export("g2")
        self.assertEqual([g["name"] for g in self._checkpoint()
                          ["generations"]], ["g1"])

    def test_generation_name_cannot_repeat_or_reopen_a_past_one(self) -> None:
        self._record_all("a")
        self._export("g1", final=True)
        self._record_all("b")
        self._export("g2", final=True)
        self._record_all("c")
        # The name is unique across the whole checkpoint.
        with self.assertRaises(ValueError):
            self._export("g1", final=True)
        with self.assertRaises(ValueError):
            self._export("g2", final=True)

    def test_empty_generation_name_is_rejected(self) -> None:
        self._record_all("a")
        with self.assertRaises(ValueError):
            self._export("")

    def test_generation_names_cannot_regress(self) -> None:
        self._record_all("a")
        self._export("g2", final=True)
        self._record_all("b")
        for bad_name in ("g1", "g2", "g0"):
            with self.subTest(bad_name=bad_name):
                with self.assertRaises(ValueError):
                    self._export(bad_name)
        proof = self._export("g3")
        self.assertEqual(
            [g["name"] for g in self._checkpoint()["generations"]],
            ["g2", "g3"])
        self._verify(proof)


class OfflineVerifyTest(_Fixture):
    def test_verify_does_not_open_the_journal(self) -> None:
        self._record_all("a", "b")
        proof = self._export(limit=1)
        os.unlink(self.journal)
        result = self._verify(proof)
        self.assertEqual([item[0] for item in result["events"]], ["a"])
        self.assertEqual(result["next"], "a")
        self.assertEqual(result["generation"], "g1")
        self.assertFalse(result["closed"])

    def test_verify_recomputes_filters_and_cursor(self) -> None:
        self._record_all("a", "b", "c")
        audit.record(self.journal, "f",
                     _event(op="restore", key="other", changed=False,
                            error="ValueError", stage="校验"))
        proof = self._export(stage="成功", limit=10)
        self.assertEqual([item[0] for item in proof["result"]["events"]],
                         ["a", "b", "c"])
        result = self._verify(proof)
        self.assertEqual([item[0] for item in result["events"]],
                         ["a", "b", "c"])

    def test_unknown_generation_or_anchor_is_rejected(self) -> None:
        self._record_all("a")
        proof = self._export()
        other = copy.deepcopy(proof)
        other["generation"] = "nope"
        with self.assertRaises(ValueError):
            self._verify(other)
        other = copy.deepcopy(proof)
        other["anchor_digest"] = "f" * 64
        with self.assertRaises(ValueError):
            self._verify(other)


class SnapshotBindingTest(_Fixture):
    def test_proof_binds_the_committed_checkpoint_tag(self) -> None:
        self._record_all("a")
        proof = self._export()
        raw = open(self.checkpoint, "rb").read()
        self.assertEqual(proof["checkpoint_etag"],
                         '"' + hashlib.sha256(raw).hexdigest() + '"')
        self._verify(proof)

    def test_duplicate_export_binds_the_unchanged_tag(self) -> None:
        self._record_all("a")
        first = self._export(limit=1)
        second = self._export(limit=5)
        self.assertEqual(first["checkpoint_etag"],
                         second["checkpoint_etag"])
        self._verify(second)

    def test_new_anchor_moves_the_bound_tag(self) -> None:
        self._record_all("a")
        p1 = self._export()
        self._record_all("b")
        p2 = self._export()
        self.assertNotEqual(p1["checkpoint_etag"], p2["checkpoint_etag"])
        self.assertEqual(
            p2["checkpoint_etag"],
            '"' + hashlib.sha256(open(self.checkpoint, "rb").read())
            .hexdigest() + '"')
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._verify(p1)
        self._verify(p2)

    def test_replaced_checkpoint_snapshot_is_rejected(self) -> None:
        # A proof verifies only against the exact checkpoint bytes it
        # was exported from, never against a re-created lookalike.
        self._record_all("a")
        proof = self._export()
        original = open(self.checkpoint, "rb").read()
        self._record_all("b")
        self._export()
        _write_bytes(self.checkpoint, original)
        self._verify(proof)
        self._record_all("c")
        self._export()
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._verify(proof)

    def test_tampered_checkpoint_etag_is_a_mismatch(self) -> None:
        self._record_all("a")
        proof = self._export()
        proof["checkpoint_etag"] = '"' + "0" * 64 + '"'
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._verify(proof)

    def test_malformed_checkpoint_etag_is_a_format_error(self) -> None:
        self._record_all("a")
        proof = self._export()
        for bad in (0, None, "unquoted", '"' + "0" * 63 + '"',
                    'W/"' + "0" * 64 + '"'):
            with self.subTest(bad=bad):
                tampered = copy.deepcopy(proof)
                tampered["checkpoint_etag"] = bad
                with self.assertRaises(audit_proof.BundleFormatError):
                    self._verify(tampered)

    def test_version_one_proof_is_an_incomplete_structure(self) -> None:
        self._record_all("a")
        proof = self._export()
        # A version 1 proof carries no snapshot binding at all.
        del proof["checkpoint_etag"]
        proof["version"] = 1
        with self.assertRaises(audit_proof.BundleFormatError):
            self._verify(proof)



    def _tampered(self, **changes: object) -> dict:
        self._record_all("a", "b", "c")
        proof = self._export(limit=2)
        proof.update(changes)
        return proof

    def test_changed_log_bytes_raise_value_error(self) -> None:
        proof = self._tampered()
        proof["log_bytes"] = proof["log_bytes"].replace(
            '"target":"t.history"', '"target":"x.history"')
        with self.assertRaises(ValueError):
            self._verify(proof)

    def test_changed_log_digest_raises_value_error(self) -> None:
        proof = self._tampered()
        proof["log_digest"] = "f" * 64
        with self.assertRaises(ValueError):
            self._verify(proof)

    def test_changed_head_raises_value_error(self) -> None:
        proof = self._tampered()
        proof["head"] = "f" * 64
        with self.assertRaises(ValueError):
            self._verify(proof)

    def test_changed_params_raise_value_error(self) -> None:
        proof = self._tampered()
        proof["params"]["limit"] = 3
        with self.assertRaises(ValueError):
            self._verify(proof)
        proof = self._tampered()
        proof["params"]["cursor"] = "a"
        with self.assertRaises(ValueError):
            self._verify(proof)
        proof = self._tampered()
        proof["params"]["op"] = "restore"
        with self.assertRaises(ValueError):
            self._verify(proof)

    def test_changed_page_content_raises_value_error(self) -> None:
        proof = self._tampered()
        proof["result"]["events"][0][1]["changed"] = False
        with self.assertRaises(ValueError):
            self._verify(proof)
        proof = self._tampered()
        proof["result"]["next"] = "zzz"
        with self.assertRaises(ValueError):
            self._verify(proof)

    def test_changed_closed_flag_raises_value_error(self) -> None:
        proof = self._tampered()
        proof["closed"] = True
        with self.assertRaises(ValueError):
            self._verify(proof)

    def test_unknown_proof_shapes_raise_value_error(self) -> None:
        proof = self._tampered()
        for bad in ("not-a-dict", [], None, 7):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    audit_proof.verify(self.checkpoint, bad)  # type: ignore[arg-type]
        extra = copy.deepcopy(proof)
        extra["new_field"] = 1
        with self.assertRaises(ValueError):
            self._verify(extra)


class MalformedInputsTest(_Fixture):
    def test_version_one_journal_is_rejected(self) -> None:
        self._record_all("a")
        raw = open(self.journal, "rb").read()
        doc = json.loads(raw)
        v1 = {"version": 1,
              "events": {k: triple[0] for k, triple in doc["events"].items()}}
        _write_bytes(self.journal,
                     (json.dumps(v1, ensure_ascii=False,
                                 separators=(",", ":")) + "\n").encode())
        with self.assertRaises(ValueError):
            self._export()
        self.assertFalse(os.path.exists(self.checkpoint))

    def test_broken_digest_chain_is_rejected(self) -> None:
        self._record_all("a", "b")
        doc = json.loads(open(self.journal, encoding="utf-8").read())
        doc["events"]["b"][0]["changed"] = False
        _write_bytes(
            self.journal,
            (json.dumps(doc, ensure_ascii=False, separators=(",", ":"))
             + "\n").encode())
        with self.assertRaises(ValueError):
            self._export()

    def test_malformed_checkpoint_structure_is_rejected(self) -> None:
        self._record_all("a")
        # A well-formed checkpoint and proof are produced separately, so
        # each malformed variant is tested on a fresh checkpoint file.
        good_cp = os.path.join(self.tmp.name, "good.json")
        proof = audit_proof.export(self.journal, good_cp, "g1", final=True)
        good_raw = open(good_cp, "rb").read()
        good = json.loads(good_raw)
        variants: list[object] = [
            b"{not json",
            b"\xff\xfe",
            {"generations": []},
            {"version": 2, "generations": []},
            {"version": 1, "extra": 1, "generations": []},
            {"version": 1, "generations": {}},
            {"version": 1, "generations": [{"name": "g1"}]},
            {"version": 1,
             "generations": [{"name": "", "anchors": [{}]}]},
            {"version": 1,
             "generations": [{"name": "g1", "anchors": []}]},
        ]
        broken_chain = copy.deepcopy(good)
        broken_chain["generations"][0]["anchors"][0]["digest"] = "f" * 64
        variants.append(broken_chain)
        broken_prev = copy.deepcopy(good)
        broken_prev["generations"][0]["anchors"][0]["previous"] = "0" * 64
        variants.append(broken_prev)
        bad_manifest = copy.deepcopy(good)
        bad_manifest["generations"][0]["anchors"][0]["manifest"][0][1] = "z"
        variants.append(bad_manifest)
        closed_mid = copy.deepcopy(good)
        anchor = closed_mid["generations"][0]["anchors"][0]
        following = dict(anchor, closed=False)
        following["previous"] = anchor["digest"]
        following["digest"] = audit_proof._anchor_digest(
            following["manifest"], following["log_digest"],
            following["head"], anchor["digest"], False)
        closed_mid["generations"][0]["anchors"] = [
            dict(anchor, closed=True), following]
        variants.append(closed_mid)
        for variant in variants:
            with self.subTest(variant=variant):
                if isinstance(variant, bytes):
                    _write_bytes(self.checkpoint, variant)
                else:
                    _write_bytes(
                        self.checkpoint,
                        (json.dumps(variant, ensure_ascii=False,
                                    separators=(",", ":")) + "\n").encode())
                with self.assertRaises(ValueError):
                    self._export()
                with self.assertRaises(ValueError):
                    self._verify(proof)

    def test_open_generation_followed_by_another_is_malformed(self) -> None:
        self._record_all("a")
        self._export("g1")
        doc = self._checkpoint()
        first = copy.deepcopy(doc["generations"][0]["anchors"][0])
        second = copy.deepcopy(first)
        second["previous"] = first["digest"]
        second["digest"] = audit_proof._anchor_digest(
            second["manifest"], second["log_digest"], second["head"],
            first["digest"], False)
        doc["generations"].append({"name": "g2", "anchors": [second]})
        _write_bytes(
            self.checkpoint,
            (json.dumps(doc, ensure_ascii=False, separators=(",", ":"))
             + "\n").encode())
        with self.assertRaises(ValueError):
            self._export("g2")


class ArgumentValidationTest(_Fixture):
    def test_bad_arguments_raise_before_any_file_is_read(self) -> None:
        missing = os.path.join(self.tmp.name, "missing-journal.json")
        cp = os.path.join(self.tmp.name, "never-created.json")

        def expect_value_error(*args: object, **kw: object) -> None:
            with self.assertRaises(ValueError):
                audit_proof.export(*args, **kw)  # type: ignore[arg-type]
            self.assertFalse(os.path.exists(cp))

        expect_value_error("", cp, "g")
        expect_value_error(missing, "", "g")
        expect_value_error(missing, cp, "")
        expect_value_error(missing, cp, "g", "yes")  # type: ignore[arg-type]
        expect_value_error(missing, cp, "g", 1)  # type: ignore[arg-type]
        for bad_limit in (0, 1001, -1, True, "2", 1.5):
            expect_value_error(missing, cp, "g", False, None,
                               bad_limit)  # type: ignore[arg-type]
        expect_value_error(missing, cp, "g", False, "")
        expect_value_error(missing, cp, "g", False, None, 100, "delete")
        expect_value_error(missing, cp, "g", False, None, 100, None,
                           "nope")
        expect_value_error(missing, cp, "g", False, None, 100, None,
                           None, "")
        # The audit file was never opened: its absence would otherwise be
        # FileNotFoundError, not ValueError.
        self.assertFalse(os.path.exists(missing))

    def test_verify_bad_arguments_raise_value_error(self) -> None:
        for bad in ("", 1, None):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    audit_proof.verify(bad, {})  # type: ignore[arg-type]


class MissingFilesTest(_Fixture):
    def test_missing_audit_journal_raises_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            audit_proof.export(
                os.path.join(self.tmp.name, "missing.json"),
                self.checkpoint, "g")

    def test_missing_checkpoint_for_verify_raises_file_not_found(self) -> None:
        self._record_all("a")
        proof = self._export()
        with self.assertRaises(FileNotFoundError):
            audit_proof.verify(
                os.path.join(self.tmp.name, "missing-checkpoint.json"),
                proof)

    def test_missing_checkpoint_parent_raises_file_not_found(self) -> None:
        self._record_all("a")
        with self.assertRaises(FileNotFoundError):
            audit_proof.export(
                self.journal,
                os.path.join(self.tmp.name, "nodir", "checkpoint.json"),
                "g")

    def test_other_io_failure_stays_oserror(self) -> None:
        self._record_all("a")
        # The checkpoint lock path living in a non-writable place surfaces
        # as a plain OSError, not a ValueError.
        os.mkdir(os.path.join(self.tmp.name, "locked"))
        target = os.path.join(self.tmp.name, "locked")
        os.chmod(target, 0o500)
        try:
            with self.assertRaises(OSError):
                audit_proof.export(
                    self.journal, os.path.join(target, "cp.json"), "g")
        finally:
            os.chmod(target, 0o700)


class CommitFailureTest(_Fixture):
    def test_failed_directory_sync_rolls_the_new_anchor_back(self) -> None:
        self._record_all("a")
        proof = self._export()
        before = open(self.checkpoint, "rb").read()
        self._record_all("b")

        def fail(_directory: str) -> None:
            raise OSError("injected directory fsync failure")

        with mock.patch.object(audit_proof, "_fsync_dir", fail):
            with self.assertRaises(OSError):
                self._export()
        # No new anchor remains and the old proof still verifies.
        self.assertEqual(open(self.checkpoint, "rb").read(), before)
        self.assertEqual(len(self._anchors()), 1)
        self._verify(proof)
        # The failed attempt is fully retryable.
        again = self._export()
        self.assertEqual(len(self._anchors()), 2)
        self._verify(again)


class ConcurrencyTest(_Fixture):
    def test_concurrent_exporters_each_see_a_complete_checkpoint(self) -> None:
        self._record_all(*(f"k{i}" for i in range(20)))
        errors: list[BaseException] = []

        def worker(index: int) -> None:
            try:
                audit_proof.export(
                    self.journal, self.checkpoint, "g1", limit=index + 1)
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        doc = self._checkpoint()
        anchors = doc["generations"][0]["anchors"]
        # Every export pinned the same 20-event snapshot, so only one
        # content anchor exists; the checkpoint parses and verifies.
        self.assertTrue(all(
            len(anchor["manifest"]) == 20 for anchor in anchors))
        self.assertEqual(len(anchors), 1)

    def test_chain_continues_after_a_simulated_process_restart(self) -> None:
        self._record_all("a")
        self._export(final=True)
        self._record_all("b")
        # Drop every in-process lock/registry state, as a fresh process
        # would; the persisted checkpoint must still anchor the chain.
        audit_proof._stores.clear()
        audit._stores.clear()
        proof = self._export("g2")
        doc = self._checkpoint()
        g1_last = doc["generations"][0]["anchors"][-1]
        g2_first = doc["generations"][1]["anchors"][0]
        self.assertEqual(g2_first["previous"], g1_last["digest"])
        self._verify(proof)


if __name__ == "__main__":
    unittest.main()
