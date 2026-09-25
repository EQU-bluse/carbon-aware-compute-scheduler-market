"""Acceptance ledger over offline bundle verification.

Covers :mod:`carbon_market.acceptance`: the pending-first submission
order, active/quarantined replays, same-key-different-package conflicts
with both packages retained, content-addressed checkpoint/proof bytes,
recovery from a pending ledger after a restart, the compact version-1
ledger format (version, records, conflicts; exactly one trailing
newline), idempotent key pagination with state filters and exclusive
cursors, and the error contract (ValueError for bad types, labels,
states, page sizes and illegal ledgers; KeyError(key) for unknown keys;
FileNotFoundError for a missing ledger or parent; OSError for the rest).
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import unittest
from tempfile import TemporaryDirectory

from carbon_market import acceptance, audit, audit_proof


def _read_bytes(path: str) -> bytes:
    with open(path, "rb") as handle:
        return handle.read()


def _event(key: str = "batch-key") -> dict:
    return {"op": "copy", "target": "t.history", "key": key,
            "changed": True, "error": None, "stage": None}


class _Fixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = self.tmp.name
        self.journal = os.path.join(root, "audit.json")
        self.checkpoint = os.path.join(root, "checkpoint.json")
        self.proof_path = os.path.join(root, "proof.json")
        self.trust = os.path.join(root, "trust")
        self.ledger = os.path.join(root, "acc", "ledger.json")

    def _bundle(self, audit_key: str = "a", generation: str = "g1"
                ) -> tuple[bytes, bytes, str]:
        audit.record(self.journal, audit_key, _event(audit_key))
        proof = audit_proof.export(self.journal, self.checkpoint,
                                   generation)
        with open(self.proof_path, "wb") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False,
                                    separators=(",", ":")).encode())
        with open(self.checkpoint, "rb") as handle:
            checkpoint = handle.read()
        with open(self.proof_path, "rb") as handle:
            proof_raw = handle.read()
        tag = '"' + hashlib.sha256(checkpoint).hexdigest() + '"'
        return checkpoint, proof_raw, tag

    def _submit(self, checkpoint: bytes, proof: bytes, tag: str,
                key: str, trust=None):
        return acceptance.submit(checkpoint, proof, tag,
                                 self.trust if trust is None else trust,
                                 self.ledger, key)


class SubmitTest(_Fixture):
    def test_first_submission_is_active_and_created(self) -> None:
        checkpoint, proof, tag = self._bundle()
        record, created = self._submit(checkpoint, proof, tag, "k1")
        self.assertIs(created, True)
        self.assertEqual(list(record),
                         ["key", "etag", "checkpoint_digest",
                          "proof_digest", "state", "error"])
        self.assertEqual(record["key"], "k1")
        self.assertEqual(record["etag"], tag)
        self.assertEqual(record["checkpoint_digest"],
                         hashlib.sha256(checkpoint).hexdigest())
        self.assertEqual(record["proof_digest"],
                         hashlib.sha256(proof).hexdigest())
        self.assertEqual(record["state"], "active")
        self.assertIsNone(record["error"])
        # Content-addressed evidence of both files.
        acc = os.path.dirname(self.ledger)
        cp_stored = os.path.join(
            acc, "checkpoints",
            hashlib.sha256(checkpoint).hexdigest() + ".bin")
        pf_stored = os.path.join(
            acc, "proofs",
            hashlib.sha256(proof).hexdigest() + ".bin")
        with open(cp_stored, "rb") as handle:
            self.assertEqual(handle.read(), checkpoint)
        with open(pf_stored, "rb") as handle:
            self.assertEqual(handle.read(), proof)

    def test_active_replay_returns_original_record_and_false(self) -> None:
        checkpoint, proof, tag = self._bundle()
        first, created = self._submit(checkpoint, proof, tag, "k1")
        self.assertIs(created, True)
        replay, created = self._submit(checkpoint, proof, tag, "k1")
        self.assertIs(created, False)
        self.assertEqual(replay, first)
        # Still one record, no conflict.
        document = json.loads(_read_bytes(self.ledger))
        self.assertEqual(list(document["records"]), ["k1"])
        self.assertEqual(document["conflicts"], [])

    def test_format_failure_quarantines_with_evidence(self) -> None:
        checkpoint, _proof, tag = self._bundle()
        bad_proof = b"{not json"
        with self.assertRaises(audit_proof.BundleFormatError):
            self._submit(checkpoint, bad_proof, tag, "k1")
        record = acceptance.get(self.ledger, "k1")
        self.assertEqual(record["state"], "quarantined")
        self.assertEqual(record["error"], "BundleFormatError")
        # The evidence survives the failure.
        stored = os.path.join(
            os.path.dirname(self.ledger), "proofs",
            hashlib.sha256(bad_proof).hexdigest() + ".bin")
        with open(stored, "rb") as handle:
            self.assertEqual(handle.read(), bad_proof)
        # An identical replay re-raises the same public type and changes
        # nothing.
        before = _read_bytes(self.ledger)
        with self.assertRaises(audit_proof.BundleFormatError):
            self._submit(checkpoint, bad_proof, tag, "k1")
        self.assertEqual(_read_bytes(self.ledger), before)

    def test_mismatch_quarantines_and_replays_same_type(self) -> None:
        checkpoint, proof, _tag = self._bundle()
        other = '"' + "0" * 64 + '"'
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._submit(checkpoint, proof, other, "k2")
        record = acceptance.get(self.ledger, "k2")
        self.assertEqual(record["state"], "quarantined")
        self.assertEqual(record["error"], "BundleMismatchError")
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._submit(checkpoint, proof, other, "k2")

    def test_same_key_different_package_conflicts(self) -> None:
        checkpoint, proof, tag = self._bundle()
        self._submit(checkpoint, proof, tag, "k1")
        bad_proof = b"{not json"
        with self.assertRaises(ValueError) as caught:
            self._submit(checkpoint, bad_proof, tag, "k1")
        self.assertNotIsInstance(caught.exception,
                                 audit_proof.BundleFormatError)
        # Main record untouched, one conflict appended, both packages'
        # bytes retained.
        self.assertEqual(acceptance.get(self.ledger, "k1")["state"],
                         "active")
        document = json.loads(_read_bytes(self.ledger))
        self.assertEqual(len(document["conflicts"]), 1)
        conflict = document["conflicts"][0]
        self.assertEqual(list(conflict),
                         ["key", "existing", "incoming", "state"])
        self.assertEqual(conflict["key"], "k1")
        self.assertEqual(conflict["state"], "active")
        self.assertEqual(
            conflict["existing"],
            [hashlib.sha256(checkpoint).hexdigest(),
             hashlib.sha256(proof).hexdigest()])
        self.assertEqual(
            conflict["incoming"],
            [hashlib.sha256(checkpoint).hexdigest(),
             hashlib.sha256(bad_proof).hexdigest()])
        stored = os.path.join(
            os.path.dirname(self.ledger), "proofs",
            hashlib.sha256(bad_proof).hexdigest() + ".bin")
        self.assertEqual(open(stored, "rb").read(), bad_proof)
        # Resubmitting the same foreign package logs no second conflict.
        with self.assertRaises(ValueError):
            self._submit(checkpoint, bad_proof, tag, "k1")
        document = json.loads(_read_bytes(self.ledger))
        self.assertEqual(len(document["conflicts"]), 1)

    def test_pending_record_is_resumed_after_restart(self) -> None:
        checkpoint, proof, tag = self._bundle()
        # Fabricate a pending ledger for a key never finished.
        os.makedirs(os.path.dirname(self.ledger))
        document = {"version": 1, "records": {
            "k9": {"key": "k9", "etag": tag,
                   "checkpoint_digest":
                   hashlib.sha256(checkpoint).hexdigest(),
                   "proof_digest": hashlib.sha256(proof).hexdigest(),
                   "state": "pending", "error": None}},
            "conflicts": []}
        with open(self.ledger, "wb") as handle:
            handle.write(json.dumps(document, ensure_ascii=False,
                                   separators=(",", ":")).encode()
                         + b"\n")
        record, created = self._submit(checkpoint, proof, tag, "k9")
        self.assertIs(created, True)
        self.assertEqual(record["state"], "active")
        self.assertEqual(acceptance.get(self.ledger, "k9")["state"],
                         "active")

    def test_concurrent_identical_submissions_advance_once(self) -> None:
        checkpoint, proof, tag = self._bundle()
        created_flags: list[bool] = []

        def worker() -> None:
            _, created = self._submit(checkpoint, proof, tag, "same")
            created_flags.append(created)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(created_flags), [False] * 7 + [True])


class LedgerFormatTest(_Fixture):
    def test_ledger_is_compact_version_one_with_single_newline(self) -> None:
        checkpoint, proof, tag = self._bundle()
        self._submit(checkpoint, proof, tag, "键")
        raw = _read_bytes(self.ledger)
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        self.assertNotIn(b": ", raw)
        self.assertNotIn(b", ", raw)
        self.assertIn("键".encode(), raw)
        document = json.loads(raw)
        self.assertEqual(list(document), ["version", "records",
                                          "conflicts"])
        self.assertEqual(document["version"], 1)

    def test_records_are_sorted_by_key_code_point(self) -> None:
        checkpoint, proof, tag = self._bundle()
        for key in ("c", "a", "b"):
            try:
                self._submit(checkpoint, b"{not json", tag, key)
            except audit_proof.BundleFormatError:
                pass
        raw = _read_bytes(self.ledger)
        self.assertLess(raw.index(b'"a"'), raw.index(b'"b"'))
        self.assertLess(raw.index(b'"b"'), raw.index(b'"c"'))


class SearchTest(_Fixture):
    def _seed(self) -> tuple[bytes, bytes, str]:
        checkpoint, proof, tag = self._bundle()
        self._submit(checkpoint, proof, tag, "k1")
        try:
            self._submit(checkpoint, proof, '"' + "0" * 64 + '"', "k2")
        except audit_proof.BundleMismatchError:
            pass
        try:
            self._submit(bad := b"xxx", proof,
                         '"' + hashlib.sha256(bad).hexdigest() + '"',
                         "k3")
        except audit_proof.BundleFormatError:
            pass
        return checkpoint, proof, tag

    def test_pagination_and_cursor(self) -> None:
        self._seed()
        page = acceptance.search(self.ledger, limit=2)
        self.assertEqual([key for key, _ in page["entries"]],
                         ["k1", "k2"])
        self.assertEqual(page["next"], "k2")
        page = acceptance.search(self.ledger, cursor="k2", limit=2)
        self.assertEqual([key for key, _ in page["entries"]], ["k3"])
        self.assertIsNone(page["next"])

    def test_state_filter_composes_with_cursor(self) -> None:
        self._seed()
        self.assertEqual(
            [key for key, _ in acceptance.search(
                self.ledger, state="active")["entries"]], ["k1"])
        self.assertEqual(
            sorted(key for key, _ in acceptance.search(
                self.ledger, state="quarantined")["entries"]),
            ["k2", "k3"])
        page = acceptance.search(self.ledger, cursor="k2",
                                 state="quarantined")
        self.assertEqual([key for key, _ in page["entries"]], ["k3"])

    def test_default_limit_is_one_hundred(self) -> None:
        import inspect
        signature = inspect.signature(acceptance.search)
        self.assertEqual(signature.parameters["limit"].default, 100)

    def test_entries_are_key_record_pairs_and_copies(self) -> None:
        self._seed()
        page = acceptance.search(self.ledger, limit=1)
        key, record = page["entries"][0]
        self.assertEqual(key, "k1")
        self.assertEqual(record["key"], "k1")
        record["state"] = "tampered"
        self.assertEqual(acceptance.get(self.ledger, "k1")["state"],
                         "active")


class ErrorContractTest(_Fixture):
    def test_unknown_key_raises_keyerror_with_key(self) -> None:
        checkpoint, proof, tag = self._bundle()
        self._submit(checkpoint, proof, tag, "k1")
        with self.assertRaises(KeyError) as caught:
            acceptance.get(self.ledger, "missing")
        self.assertEqual(caught.exception.args[0], "missing")

    def test_bad_arguments_are_value_error(self) -> None:
        checkpoint, proof, tag = self._bundle()
        bad_submits = [
            ("x", proof, tag, None, self.ledger, "k"),
            (checkpoint, "x", tag, None, self.ledger, "k"),
            (checkpoint, proof, "tag", None, self.ledger, "k"),
            (checkpoint, proof, tag, "", self.ledger, "k"),
            (checkpoint, proof, tag, None, "", "k"),
            (checkpoint, proof, tag, None, self.ledger, ""),
            (checkpoint, proof, tag, 3, self.ledger, "k"),
        ]
        for args in bad_submits:
            with self.subTest(args=args):
                with self.assertRaises(ValueError):
                    acceptance.submit(*args)
        with self.assertRaises(ValueError):
            acceptance.get(123, "k")
        with self.assertRaises(ValueError):
            acceptance.get(self.ledger, "")
        for kwargs in ({"limit": 0}, {"limit": 1001}, {"limit": True},
                       {"limit": "1"}, {"cursor": ""}, {"state": "x"},
                       {"cursor": 1}):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    acceptance.search(self.ledger, **kwargs)

    def test_missing_ledger_and_parent(self) -> None:
        with self.assertRaises(FileNotFoundError):
            acceptance.get(self.ledger, "k")
        with self.assertRaises(FileNotFoundError):
            acceptance.search(self.ledger)
        checkpoint, proof, tag = self._bundle()
        with self.assertRaises(FileNotFoundError):
            acceptance.submit(
                checkpoint, proof, tag, None,
                os.path.join(self.tmp.name, "no", "such", "l.json"),
                "k")

    def test_illegal_ledger_is_value_error(self) -> None:
        os.makedirs(os.path.dirname(self.ledger))
        for raw in (b"{bad", b"[]",
                    b'{"version":2,"records":{},"conflicts":[]}',
                    b'{"version":1,"records":{},"conflicts":{}}',
                    b'{"version":1,"records":{},"extra":[],"conflicts":[]}',
                    b'{"records":{},"conflicts":[],"version":1}'):
            with self.subTest(raw=raw):
                with open(self.ledger, "wb") as handle:
                    handle.write(raw)
                with self.assertRaises(ValueError):
                    acceptance.get(self.ledger, "k")
                with self.assertRaises(ValueError):
                    acceptance.search(self.ledger)


if __name__ == "__main__":
    unittest.main()
