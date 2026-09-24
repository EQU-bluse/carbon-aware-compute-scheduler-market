"""Persistent snapshot sequence of the offline bundle verifier.

Covers the optional trust directory of ``audit_proof.verify_bundle``
and the ``--trust-dir`` option of the ``verify-bundle`` command: the
first use establishing the trust anchor from the verified checkpoint,
the idempotent replay of the retained head tag, the strict append-only
advance on a new tag, the rollback rejection of earlier versions,
forks and non-continuations, the tamper evidence that is never
auto-repaired, the argument contract (exit 2), the format/mismatch/
I-O exit mapping (3/4/5) and the unchanged single-bundle behaviour
when the option is absent.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import sys
import unittest
from tempfile import TemporaryDirectory
from unittest import mock

from carbon_market import audit, audit_proof
from carbon_market.__main__ import main


def _event(op: str = "copy", key: str = "batch-key") -> dict:
    return {"op": op, "target": "t.history", "key": key,
            "changed": True, "error": None, "stage": None}


def _run_cli(*args: str) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    argv = ["carbon_market", "verify-bundle", *args]
    with mock.patch.object(sys, "argv", argv), \
            contextlib.redirect_stdout(stdout), \
            contextlib.redirect_stderr(stderr):
        try:
            main()
            status = 0
        except SystemExit as exc:
            status = exc.code
    return status, stdout.getvalue(), stderr.getvalue()


class _Fixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.checkpoint = os.path.join(self.tmp.name, "checkpoint.json")
        self.proof_path = os.path.join(self.tmp.name, "proof.json")
        self.trust = os.path.join(self.tmp.name, "trust")

    def _record(self, audit_key: str) -> None:
        audit.record(self.journal, audit_key, _event())

    def _export(self, generation: str = "g1", **kwargs) -> dict:
        proof = audit_proof.export(self.journal, self.checkpoint,
                                   generation, **kwargs)
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False))
        return proof

    def _etag(self, path: str | None = None) -> str:
        with open(path or self.checkpoint, "rb") as handle:
            return '"' + hashlib.sha256(handle.read()).hexdigest() + '"'

    def _run(self, *args: str) -> tuple[int, str, str]:
        return _run_cli(*args)

    def _run_bundle(self, trust: str | None = None,
                    etag: str | None = None,
                    checkpoint: str | None = None,
                    proof: str | None = None) -> tuple[int, str, str]:
        args = ["--checkpoint", checkpoint or self.checkpoint,
                "--proof", proof or self.proof_path,
                "--etag", etag if etag is not None else self._etag()]
        if trust is not None:
            args += ["--trust-dir", trust]
        return self._run(*args)

    def _tree(self, path: str) -> dict[str, bytes]:
        result = {}
        for root, _, files in os.walk(path):
            for name in files:
                full = os.path.join(root, name)
                with open(full, "rb") as handle:
                    result[os.path.relpath(full, path)] = handle.read()
        return result


class FirstUseTest(_Fixture):
    def test_first_use_establishes_the_trust_anchor(self) -> None:
        self._record("a")
        proof = self._export("g1")
        self.assertFalse(os.path.exists(self.trust))
        status, stdout, stderr = self._run_bundle(trust=self.trust)
        self.assertEqual((status, stderr), (0, ""))
        result = json.loads(stdout)
        self.assertEqual(list(result),
                         ["valid", "etag", "generation", "closed",
                          "events", "next"])
        self.assertEqual(result["etag"], self._etag())
        # The raw checkpoint bytes are saved and the head names the
        # single complete node.
        tree = self._tree(self.trust)
        digest = hashlib.sha256(
            open(self.checkpoint, "rb").read()).hexdigest()
        self.assertEqual(tree[f"snapshots/{digest}.json"],
                         open(self.checkpoint, "rb").read())
        head = json.loads(tree["head.json"])
        node = json.loads(tree[f"nodes/{head['digest']}.json"])
        self.assertEqual(node["tag"], self._etag())
        self.assertIsNone(node["previous_tag"])
        self.assertIsNone(node["previous_digest"])
        self.assertEqual(node["checkpoint_digest"], digest)
        self.assertEqual(proof["checkpoint_etag"], node["tag"])

    def test_library_first_use_returns_the_same_result(self) -> None:
        self._record("a")
        self._export("g1")
        result = audit_proof.verify_bundle(
            self.checkpoint, self.proof_path, self._etag(),
            trust_dir=self.trust)
        self.assertEqual(result["generation"], "g1")
        self.assertFalse(result["closed"])

    def test_failed_bundle_verification_creates_nothing(self) -> None:
        self._record("a")
        self._export("g1")
        status, _, stderr = self._run_bundle(
            trust=self.trust, etag='"' + "0" * 64 + '"')
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})
        self.assertFalse(os.path.exists(self.trust))


class ReplayAndAdvanceTest(_Fixture):
    def test_same_tag_is_an_idempotent_replay(self) -> None:
        self._record("a")
        self._export("g1")
        self.assertEqual(self._run_bundle(trust=self.trust)[0], 0)
        before = self._tree(self.trust)
        status, stdout, _ = self._run_bundle(trust=self.trust)
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(stdout)["etag"], self._etag())
        # The directory is byte-for-byte untouched.
        self.assertEqual(self._tree(self.trust), before)

    def test_new_tag_advances_the_head(self) -> None:
        self._record("a")
        self._export("g1")
        first_tag = self._etag()
        self.assertEqual(self._run_bundle(trust=self.trust)[0], 0)
        self._record("b")
        self._export("g1")
        second_tag = self._etag()
        status, stdout, _ = self._run_bundle(trust=self.trust)
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(stdout)["etag"], second_tag)
        head = json.loads(self._tree(self.trust)["head.json"])
        tree = self._tree(self.trust)
        node = json.loads(tree[f"nodes/{head['digest']}.json"])
        self.assertEqual(node["tag"], second_tag)
        self.assertEqual(node["previous_tag"], first_tag)
        # The predecessor node is still retained and referenced.
        previous = json.loads(
            tree[f"nodes/{node['previous_digest']}.json"])
        self.assertEqual(previous["tag"], first_tag)

    def test_earlier_version_replay_is_rejected(self) -> None:
        self._record("a")
        proof1 = self._export("g1")
        old_raw = open(self.checkpoint, "rb").read()
        old_etag = self._etag()
        self.assertEqual(self._run_bundle(trust=self.trust)[0], 0)
        self._record("b")
        self._export("g1")
        self.assertEqual(self._run_bundle(trust=self.trust)[0], 0)
        # Replay the retained older pair: self-consistent, but a
        # rollback against the advanced head.
        with open(self.checkpoint, "wb") as handle:
            handle.write(old_raw)
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof1, ensure_ascii=False))
        status, stdout, stderr = self._run_bundle(
            trust=self.trust, etag=old_etag)
        self.assertEqual(status, 4)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})

    def test_fork_rewriting_old_anchors_is_rejected(self) -> None:
        self._record("a")
        self._export("g1")
        self._record("b")
        self._export("g1")
        # The trusted head holds anchors [A1, A2] for events a and b.
        self.assertEqual(self._run_bundle(trust=self.trust)[0], 0)
        # A fork shares A1 but diverges afterwards: a second journal
        # with a different event extends a copy of the first snapshot.
        fork_journal = os.path.join(self.tmp.name, "fork-audit.json")
        audit.record(fork_journal, "a", _event())
        audit.record(fork_journal, "c", _event())
        document = json.loads(open(self.checkpoint, encoding="utf-8")
                              .read())
        document["generations"][0]["anchors"] = \
            document["generations"][0]["anchors"][:1]
        with open(self.checkpoint, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(document, ensure_ascii=False,
                                    separators=(",", ":")))
        proof = audit_proof.export(fork_journal, self.checkpoint, "g1")
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False))
        # The bundle is self-consistent, but it rewrites the trusted
        # second anchor and must be rejected as a rollback.
        status, _, stderr = self._run_bundle(trust=self.trust)
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})

    def test_unrelated_checkpoint_cannot_continue(self) -> None:
        self._record("a")
        self._export("g1")
        self.assertEqual(self._run_bundle(trust=self.trust)[0], 0)
        # A different lineage: another journal, another first anchor.
        other_journal = os.path.join(self.tmp.name, "other-audit.json")
        audit.record(other_journal, "x", _event())
        other = os.path.join(self.tmp.name, "other-checkpoint.json")
        proof = audit_proof.export(other_journal, other, "g1")
        with open(other, "rb") as handle:
            raw = handle.read()
        with open(self.checkpoint, "wb") as handle:
            handle.write(raw)
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False))
        status, _, stderr = self._run_bundle(trust=self.trust)
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})

    def test_new_generation_appends_across_the_sequence(self) -> None:
        self._record("a")
        self._export("g1", final=True)
        self.assertEqual(self._run_bundle(trust=self.trust)[0], 0)
        self._record("b")
        self._export("g2")
        status, stdout, _ = self._run_bundle(trust=self.trust)
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(stdout)["generation"], "g2")


class TamperTest(_Fixture):
    def _establish(self) -> None:
        self._record("a")
        self._export("g1")
        self.assertEqual(self._run_bundle(trust=self.trust)[0], 0)

    def test_tampered_node_content_is_not_repaired(self) -> None:
        self._establish()
        tree = self._tree(self.trust)
        head = json.loads(tree["head.json"])
        node_path = os.path.join(self.trust, "nodes",
                                 head["digest"] + ".json")
        node = json.loads(tree[f"nodes/{head['digest']}.json"])
        node["checkpoint_digest"] = "0" * 64
        with open(node_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(node, ensure_ascii=False))
        status, _, stderr = self._run_bundle(trust=self.trust)
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})
        # The evidence is left in place, not overwritten.
        self.assertEqual(json.loads(open(node_path, encoding="utf-8")
                                  .read())["checkpoint_digest"],
                         "0" * 64)

    def test_tampered_head_is_verification_failed(self) -> None:
        self._establish()
        with open(os.path.join(self.trust, "head.json"), "w",
                  encoding="utf-8") as handle:
            handle.write(json.dumps({"version": 1, "digest": "0" * 64}))
        status, _, stderr = self._run_bundle(trust=self.trust)
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})

    def test_malformed_trust_metadata_is_invalid_bundle(self) -> None:
        self._establish()
        tree = self._tree(self.trust)
        head = json.loads(tree["head.json"])
        node_path = os.path.join(self.trust, "nodes",
                                 head["digest"] + ".json")
        for raw in (b"\xff\xfe", b"{not json",
                    b'{"version":-0,"tag":"x","previous_tag":null,'
                    b'"checkpoint_digest":"' + b"0" * 64
                    + b'","previous_digest":null}',
                    b'{"version":2,"tag":"x","previous_tag":null,'
                    b'"checkpoint_digest":"' + b"0" * 64
                    + b'","previous_digest":null}'):
            with self.subTest(raw=raw[:16]):
                with open(node_path, "wb") as handle:
                    handle.write(raw)
                status, _, stderr = self._run_bundle(trust=self.trust)
                self.assertEqual(status, 3)
                self.assertEqual(json.loads(stderr),
                                 {"error": "invalid_bundle"})

    def test_missing_referenced_node_is_verification_failed(self) -> None:
        self._establish()
        tree = self._tree(self.trust)
        head = json.loads(tree["head.json"])
        os.unlink(os.path.join(self.trust, "nodes",
                               head["digest"] + ".json"))
        status, _, stderr = self._run_bundle(trust=self.trust)
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})


class ArgumentAndIoTest(_Fixture):
    def test_empty_and_repeated_trust_dir_are_invalid_request(self) -> None:
        self._record("a")
        self._export("g1")
        etag = self._etag()
        variants = [
            ["--checkpoint", self.checkpoint, "--proof", self.proof_path,
             "--etag", etag, "--trust-dir", ""],
            ["--checkpoint", self.checkpoint, "--proof", self.proof_path,
             "--etag", etag, "--trust-dir", self.trust,
             "--trust-dir", self.trust],
        ]
        for args in variants:
            with self.subTest(args=args):
                status, stdout, stderr = self._run(*args)
                self.assertEqual(status, 2)
                self.assertEqual(stdout, "")
                self.assertEqual(json.loads(stderr),
                                 {"error": "invalid_request"})
        self.assertFalse(os.path.exists(self.trust))

    def test_library_rejects_a_bad_trust_dir_before_reading(self) -> None:
        missing = os.path.join(self.tmp.name, "missing.json")
        for trust_dir in ("", 0, 1.5, [], object()):
            with self.subTest(trust_dir=trust_dir):
                with self.assertRaises(ValueError):
                    audit_proof.verify_bundle(missing, missing, "x",
                                              trust_dir=trust_dir)

    def test_missing_trust_parent_is_bundle_unavailable(self) -> None:
        self._record("a")
        self._export("g1")
        trust = os.path.join(self.tmp.name, "no-such-parent", "trust")
        status, stdout, stderr = self._run_bundle(trust=trust)
        self.assertEqual(status, 5)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr),
                         {"error": "bundle_unavailable"})

    def test_without_trust_dir_no_directory_is_created(self) -> None:
        self._record("a")
        self._export("g1")
        status, _, _ = self._run_bundle()
        self.assertEqual(status, 0)
        self.assertFalse(os.path.exists(self.trust))
        self.assertEqual(sorted(os.listdir(self.tmp.name)),
                         ["audit.json", "audit.json.lock",
                          "checkpoint.json", "checkpoint.json.lock",
                          "proof.json"])


class ConcurrencyTest(_Fixture):
    def test_concurrent_advance_lets_only_the_successor_through(self) -> None:
        import threading
        self._record("a")
        self._export("g1")
        old_raw = open(self.checkpoint, "rb").read()
        old_proof = open(self.proof_path, "rb").read()
        old_etag = self._etag()
        self.assertEqual(self._run_bundle(trust=self.trust)[0], 0)
        self._record("b")
        self._export("g1")
        new_raw = open(self.checkpoint, "rb").read()
        new_proof = open(self.proof_path, "rb").read()
        new_etag = self._etag()

        other_dir = os.path.join(self.tmp.name, "other")
        os.mkdir(other_dir)
        outcomes: list[str] = []
        failures: list[BaseException] = []

        def verify(raw: bytes, proof: bytes, etag: str, label: str) -> None:
            try:
                checkpoint = os.path.join(other_dir, label + "-cp.json")
                proof_path = os.path.join(other_dir, label + "-pp.json")
                with open(checkpoint, "wb") as handle:
                    handle.write(raw)
                with open(proof_path, "wb") as handle:
                    handle.write(proof)
                audit_proof.verify_bundle(checkpoint, proof_path, etag,
                                          trust_dir=self.trust)
                outcomes.append(label)
            except audit_proof.BundleMismatchError:
                outcomes.append("mismatch")
            except BaseException as exc:  # pragma: no cover
                failures.append(exc)

        threads = [threading.Thread(target=verify, args=(
            old_raw, old_proof, old_etag, "old")),
            threading.Thread(target=verify, args=(
                new_raw, new_proof, new_etag, "new"))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(failures, [])
        # The old version may only succeed if it ran before the
        # advance; once the successor moved the head it must fail.
        if "new" in outcomes:
            head = json.loads(self._tree(self.trust)["head.json"])
            node = json.loads(
                self._tree(self.trust)[f"nodes/{head['digest']}.json"])
            self.assertEqual(node["tag"], new_etag)
        self.assertIn("new", outcomes)


if __name__ == "__main__":
    unittest.main()
