"""Acceptance ledger of verified checkpoint/proof bundles.

Covers ``carbon_market.acceptance``: the first submission's
pending-then-verify sequence with content-addressed evidence, the
active/quarantined replay rules, the same-key different-bundle conflict
retention, the pending resume after an interrupted verification, the
ledger's compact on-disk format and validation, the read-only ``get``
and ``search`` queries, and the error mapping (ValueError,
FileNotFoundError, OSError). Also covers the ``verify_bundle`` success
result now carrying the actual etag.
"""

from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import unittest
from tempfile import TemporaryDirectory
from unittest import mock

from carbon_market import acceptance, audit, audit_proof


def _event(op: str = "copy", key: str = "batch-key",
           error: str | None = None, stage: str | None = None) -> dict:
    return {"op": op, "target": "t.history", "key": key,
            "changed": True, "error": error, "stage": stage}


def _digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _submit_worker(bundle: tuple[str, str, str, str, str, str],
                   queue) -> None:
    checkpoint, proof, etag, trust_dir, ledger_dir, key = bundle
    try:
        record, created = acceptance.submit(
            checkpoint, proof, etag, trust_dir, ledger_dir, key)
        queue.put(("ok", record["state"], created))
    except Exception as exc:  # pragma: no cover - failure path
        queue.put(("error", type(exc).__name__, str(exc)))


class _Fixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.checkpoint = os.path.join(self.tmp.name, "checkpoint.json")
        self.proof_path = os.path.join(self.tmp.name, "proof.json")
        self.trust_dir = os.path.join(self.tmp.name, "trust")
        self.ledger_dir = os.path.join(self.tmp.name, "ledger")

    def _record(self, audit_key: str, **event_kwargs) -> None:
        audit.record(self.journal, audit_key, _event(**event_kwargs))

    def _export(self, generation: str = "g1", **kwargs) -> dict:
        proof = audit_proof.export(self.journal, self.checkpoint,
                                   generation, **kwargs)
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False))
        return proof

    def _etag(self, path: str | None = None) -> str:
        with open(path or self.checkpoint, "rb") as handle:
            return '"' + _digest(handle.read()) + '"'

    def _ledger_bytes(self) -> bytes:
        with open(os.path.join(self.ledger_dir, "ledger.json"),
                  "rb") as handle:
            return handle.read()

    def _ledger(self) -> dict:
        return json.loads(self._ledger_bytes().decode("utf-8"))

    def _submit(self, key: str = "k1", **overrides):
        arguments = {
            "checkpoint_path": self.checkpoint,
            "proof_path": self.proof_path,
            "trust_dir": self.trust_dir,
            "ledger_dir": self.ledger_dir,
            "key": key,
        }
        arguments.update(overrides)
        if "etag" not in overrides:
            arguments["etag"] = self._etag()
        return acceptance.submit(**arguments)


class SubmitTest(_Fixture):
    def test_first_submit_activates_and_returns_true(self) -> None:
        self._record("a")
        proof = self._export("世代-1")
        record, created = self._submit()
        self.assertIs(created, True)
        self.assertEqual(list(record),
                         ["key", "etag", "checkpoint_digest",
                          "proof_digest", "state", "error"])
        self.assertEqual(record["key"], "k1")
        self.assertEqual(record["etag"], self._etag())
        with open(self.checkpoint, "rb") as handle:
            self.assertEqual(record["checkpoint_digest"],
                             _digest(handle.read()))
        with open(self.proof_path, "rb") as handle:
            self.assertEqual(record["proof_digest"],
                             _digest(handle.read()))
        self.assertEqual(record["state"], "active")
        self.assertIsNone(record["error"])
        # The trust directory advanced through its lifecycle as well.
        self.assertEqual(audit_proof.verify_bundle(
            self.checkpoint, self.proof_path, self._etag(),
            trust_dir=self.trust_dir)["etag"], self._etag())
        self.assertEqual(proof["checkpoint_etag"], record["etag"])

    def test_evidence_is_content_addressed(self) -> None:
        self._record("a")
        self._export()
        record, _ = self._submit()
        for kind, digest in (("checkpoints", record["checkpoint_digest"]),
                             ("proofs", record["proof_digest"])):
            evidence = os.path.join(self.ledger_dir, kind,
                                    digest + ".json")
            source = self.checkpoint if kind == "checkpoints" \
                else self.proof_path
            with open(evidence, "rb") as handle:
                saved = handle.read()
            with open(source, "rb") as handle:
                self.assertEqual(saved, handle.read())

    def test_ledger_format(self) -> None:
        self._record("a")
        self._export()
        self._submit("b-key")
        self._record("b")
        self._export()
        self._submit("a-key")
        raw = self._ledger_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        self.assertEqual(raw, json.dumps(
            json.loads(raw), ensure_ascii=False,
            separators=(",", ":")).encode("utf-8") + b"\n")
        document = json.loads(raw)
        self.assertEqual(list(document), ["version", "records",
                                          "conflicts"])
        self.assertEqual(document["version"], 1)
        self.assertEqual(list(document["records"]), ["a-key", "b-key"])
        self.assertEqual(document["conflicts"], [])

    def test_active_replay_returns_false_and_writes_nothing(self) -> None:
        self._record("a")
        self._export()
        first, created = self._submit()
        self.assertIs(created, True)
        before = self._ledger_bytes()
        record, created = self._submit()
        self.assertIs(created, False)
        self.assertEqual(record, first)
        self.assertEqual(record["state"], "active")
        self.assertEqual(self._ledger_bytes(), before)

    def test_pending_submission_resumes_to_active(self) -> None:
        self._record("a")
        self._export()
        # Interrupt the first submission during verification: the
        # pending record and the evidence stay behind.
        with mock.patch.object(audit_proof, "verify_bundle",
                               side_effect=OSError("injected")):
            with self.assertRaises(OSError):
                self._submit()
        record = acceptance.get(self.ledger_dir, "k1")
        self.assertEqual(record["state"], "pending")
        self.assertIsNone(record["error"])
        # The identical bundle resumes and completes the acceptance.
        record, created = self._submit()
        self.assertIs(created, True)
        self.assertEqual(record["state"], "active")
        record, created = self._submit()
        self.assertIs(created, False)

    def test_format_failure_quarantines_then_raises(self) -> None:
        self._record("a")
        self._export()
        with open(self.proof_path, "wb") as handle:
            handle.write(b"{not json")
        with self.assertRaises(audit_proof.BundleFormatError):
            self._submit()
        record = acceptance.get(self.ledger_dir, "k1")
        self.assertEqual(record["state"], "quarantined")
        self.assertEqual(record["error"], "BundleFormatError")
        # The evidence of the failed bundle is retained.
        self.assertTrue(os.path.exists(os.path.join(
            self.ledger_dir, "checkpoints",
            record["checkpoint_digest"] + ".json")))
        self.assertTrue(os.path.exists(os.path.join(
            self.ledger_dir, "proofs", record["proof_digest"] + ".json")))

    def test_mismatch_quarantines_then_raises(self) -> None:
        self._record("a")
        self._export()
        wrong_tag = '"' + "0" * 64 + '"'
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._submit(etag=wrong_tag)
        record = acceptance.get(self.ledger_dir, "k1")
        self.assertEqual(record["state"], "quarantined")
        self.assertEqual(record["error"], "BundleMismatchError")
        self.assertEqual(record["etag"], wrong_tag)

    def test_quarantined_replay_reraises_without_writing(self) -> None:
        self._record("a")
        self._export()
        with open(self.proof_path, "wb") as handle:
            handle.write(b"\xff\xfe")
        with self.assertRaises(audit_proof.BundleFormatError):
            self._submit()
        before = self._ledger_bytes()
        with self.assertRaises(audit_proof.BundleFormatError):
            self._submit()
        self.assertEqual(self._ledger_bytes(), before)

    def test_quarantined_mismatch_replay_reraises_mismatch(self) -> None:
        self._record("a")
        self._export()
        wrong_tag = '"' + "0" * 64 + '"'
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._submit(etag=wrong_tag)
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._submit(etag=wrong_tag)

    def test_conflict_retains_both_bundles_and_raises(self) -> None:
        self._record("a")
        self._export()
        record, _ = self._submit()
        # A different bundle under the same key.
        self._record("b")
        self._export()
        other_etag = self._etag()
        with self.assertRaises(ValueError):
            self._submit()
        document = self._ledger()
        # The main record is untouched; the conflict is recorded with
        # the second bundle's summary.
        self.assertEqual(len(document["records"]), 1)
        self.assertEqual(document["records"]["k1"], record)
        self.assertEqual(len(document["conflicts"]), 1)
        conflict = document["conflicts"][0]
        self.assertEqual(list(conflict), ["key", "etag",
                                          "checkpoint_digest",
                                          "proof_digest"])
        self.assertEqual(conflict["key"], "k1")
        self.assertEqual(conflict["etag"], other_etag)
        # Both bundles' raw bytes are retained content-addressed.
        with open(self.checkpoint, "rb") as handle:
            other_digest = _digest(handle.read())
        self.assertEqual(conflict["checkpoint_digest"], other_digest)
        for digest in (record["checkpoint_digest"], other_digest):
            self.assertTrue(os.path.exists(os.path.join(
                self.ledger_dir, "checkpoints", digest + ".json")))
        # The identical conflict advances nothing further.
        with self.assertRaises(ValueError):
            self._submit()
        self.assertEqual(len(self._ledger()["conflicts"]), 1)

    def test_conflict_against_quarantined_record(self) -> None:
        self._record("a")
        self._export()
        wrong_tag = '"' + "0" * 64 + '"'
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._submit(etag=wrong_tag)
        with self.assertRaises(ValueError):
            self._submit()  # same key, the valid bundle: a conflict
        record = acceptance.get(self.ledger_dir, "k1")
        self.assertEqual(record["state"], "quarantined")

    def test_distinct_keys_share_the_ledger(self) -> None:
        self._record("a")
        self._export()
        first, _ = self._submit("k1")
        self._record("b")
        self._export()
        second, created = self._submit("k2")
        self.assertIs(created, True)
        self.assertEqual(acceptance.get(self.ledger_dir, "k1"), first)
        self.assertEqual(acceptance.get(self.ledger_dir, "k2"), second)

    def test_submit_without_trust_dir(self) -> None:
        self._record("a")
        self._export()
        record, created = self._submit(trust_dir=None)
        self.assertIs(created, True)
        self.assertEqual(record["state"], "active")
        self.assertFalse(os.path.exists(self.trust_dir))


class ArgumentTest(_Fixture):
    def test_argument_errors_precede_any_file_read(self) -> None:
        missing = os.path.join(self.tmp.name, "missing.json")
        base = {"checkpoint_path": missing, "proof_path": missing,
                "etag": '"' + "0" * 64 + '"'}
        for kwargs in (
                {"checkpoint_path": ""},
                {"proof_path": ""},
                {"checkpoint_path": None},
                {"etag": "not-a-tag"},
                {"etag": '"' + "0" * 63 + '"'},
                {"etag": None},
                {"trust_dir": ""},
                {"ledger_dir": ""},
                {"ledger_dir": None},
                {"key": ""},
                {"key": None}):
            with self.assertRaises(ValueError, msg=kwargs):
                self._submit(**dict(base, **kwargs))
        self.assertFalse(os.path.exists(self.ledger_dir))

    def test_missing_inputs_raise_file_not_found(self) -> None:
        missing = os.path.join(self.tmp.name, "missing.json")
        with self.assertRaises(FileNotFoundError):
            self._submit(checkpoint_path=missing)
        with self.assertRaises(FileNotFoundError):
            self._submit(proof_path=missing)
        self.assertFalse(os.path.exists(self.ledger_dir))

    def test_missing_ledger_parent_raises_file_not_found(self) -> None:
        self._record("a")
        self._export()
        with self.assertRaises(FileNotFoundError):
            self._submit(ledger_dir=os.path.join(
                self.tmp.name, "no-such-parent", "ledger"))

    def test_missing_trust_parent_raises_file_not_found(self) -> None:
        self._record("a")
        self._export()
        with self.assertRaises(FileNotFoundError):
            self._submit(trust_dir=os.path.join(
                self.tmp.name, "no-such-parent", "trust"))
        # The failed verification leaves the record pending for a
        # later resume.
        self.assertEqual(acceptance.get(self.ledger_dir, "k1")["state"],
                         "pending")

    def test_invalid_ledger_raises_value_error(self) -> None:
        self._record("a")
        self._export()
        os.mkdir(self.ledger_dir)
        ledger_path = os.path.join(self.ledger_dir, "ledger.json")
        for raw in (b"\xff", b"{not json", b"{}",
                    b'{"version":2,"records":{},"conflicts":[]}',
                    b'{"version":1,"records":[],"conflicts":[]}',
                    b'{"version":1,"records":{},"conflicts":{}}',
                    b'{"records":{},"version":1,"conflicts":[]}'):
            with open(ledger_path, "wb") as handle:
                handle.write(raw)
            with self.assertRaises(ValueError, msg=raw):
                self._submit()
            with self.assertRaises(ValueError, msg=raw):
                acceptance.get(self.ledger_dir, "k1")
            with self.assertRaises(ValueError, msg=raw):
                acceptance.search(self.ledger_dir)


class QueryTest(_Fixture):
    def _populate(self) -> None:
        self._record("a")
        self._export()
        self._submit("k1")
        self._record("b")
        self._export()
        self._submit("k2")
        # k3 quarantines on a tag mismatch.
        self._record("c")
        self._export()
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._submit("k3", etag='"' + "0" * 64 + '"')

    def test_get_returns_a_copy(self) -> None:
        self._populate()
        record = acceptance.get(self.ledger_dir, "k1")
        record["state"] = "mutated"
        self.assertEqual(
            acceptance.get(self.ledger_dir, "k1")["state"], "active")
        with self.assertRaises(KeyError) as caught:
            acceptance.get(self.ledger_dir, "unknown")
        self.assertEqual(caught.exception.args, ("unknown",))

    def test_get_missing_ledger_raises_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            acceptance.get(self.ledger_dir, "k1")
        with self.assertRaises(FileNotFoundError):
            acceptance.search(self.ledger_dir)

    def test_search_paginates_in_key_order(self) -> None:
        self._populate()
        page = acceptance.search(self.ledger_dir, limit=2)
        self.assertEqual([key for key, _ in page["entries"]],
                         ["k1", "k2"])
        self.assertEqual(page["next"], "k2")
        page = acceptance.search(self.ledger_dir, cursor=page["next"],
                                 limit=2)
        self.assertEqual([key for key, _ in page["entries"]], ["k3"])
        self.assertIsNone(page["next"])
        # The entries are [key, record] pairs with the public order.
        key, record = acceptance.search(self.ledger_dir)["entries"][0]
        self.assertEqual(key, "k1")
        self.assertEqual(list(record),
                         ["key", "etag", "checkpoint_digest",
                          "proof_digest", "state", "error"])

    def test_search_state_filter_composes_with_cursor(self) -> None:
        self._populate()
        page = acceptance.search(self.ledger_dir, state="quarantined")
        self.assertEqual([key for key, _ in page["entries"]], ["k3"])
        self.assertIsNone(page["next"])
        page = acceptance.search(self.ledger_dir, state="active",
                                 cursor="k1", limit=1)
        self.assertEqual([key for key, _ in page["entries"]], ["k2"])
        self.assertIsNone(page["next"])
        page = acceptance.search(self.ledger_dir, state="active",
                                 limit=1)
        self.assertEqual([key for key, _ in page["entries"]], ["k1"])
        self.assertEqual(page["next"], "k1")
        page = acceptance.search(self.ledger_dir, state="pending")
        self.assertEqual(page["entries"], [])
        self.assertIsNone(page["next"])

    def test_search_argument_validation(self) -> None:
        self._populate()
        for kwargs in ({"cursor": ""}, {"cursor": 1}, {"limit": 0},
                       {"limit": 1001}, {"limit": True},
                       {"limit": "100"}, {"state": "unknown"},
                       {"state": ""}):
            with self.assertRaises(ValueError, msg=kwargs):
                acceptance.search(self.ledger_dir, **kwargs)
        with self.assertRaises(ValueError):
            acceptance.search("")
        with self.assertRaises(ValueError):
            acceptance.get(self.ledger_dir, "")


class ConcurrencyTest(_Fixture):
    def test_concurrent_identical_submits_advance_once(self) -> None:
        self._record("a")
        self._export()
        bundle = (self.checkpoint, self.proof_path, self._etag(),
                  self.trust_dir, self.ledger_dir, "k1")
        context = multiprocessing.get_context("fork")
        queue = context.Queue()
        workers = [context.Process(target=_submit_worker,
                                   args=(bundle, queue))
                   for _ in range(4)]
        for worker in workers:
            worker.start()
        outcomes = [queue.get(timeout=60) for _ in workers]
        for worker in workers:
            worker.join(timeout=60)
        self.assertEqual([outcome[0] for outcome in outcomes],
                         ["ok"] * 4)
        # Exactly one process advanced the record to active; every
        # other process replayed it.
        created = [outcome[2] for outcome in outcomes]
        self.assertEqual(sorted(created), [False, False, False, True])
        self.assertEqual(
            acceptance.get(self.ledger_dir, "k1")["state"], "active")


class VerifyBundleEtagTest(_Fixture):
    def test_verify_bundle_reports_the_actual_etag(self) -> None:
        self._record("a")
        self._export()
        for trust_dir in (None, self.trust_dir):
            result = audit_proof.verify_bundle(
                self.checkpoint, self.proof_path, self._etag(),
                trust_dir=trust_dir)
            self.assertEqual(
                list(result),
                ["events", "next", "generation", "closed", "etag"])
            self.assertEqual(result["etag"], self._etag())


if __name__ == "__main__":
    unittest.main()
