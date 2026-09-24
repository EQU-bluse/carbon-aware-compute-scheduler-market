"""Persistent trusted snapshot sequence for offline bundle verification.

Covers the optional ``--trust-dir PATH`` of the ``verify-bundle``
command and the ``trust_dir`` argument of ``audit_proof.verify_bundle``:
the unchanged single-bundle behavior without the option, the trust
anchor established on first use with the checkpoint's original bytes,
the idempotent replay of the head's strong tag, the strictly
append-only advance for a new tag, the rollback rejection of retained
older versions, forks and non-continuations, the format/mismatch split
of trust-metadata failures, the preserved chain head on commit
failures, and the exit-status mapping (2 invalid_request,
3 invalid_bundle, 4 verification_failed, 5 bundle_unavailable).
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import sys
import threading
import unittest
from tempfile import TemporaryDirectory
from unittest import mock

from carbon_market import audit, audit_proof
from carbon_market.__main__ import main


def _event(op: str = "copy", key: str = "batch-key",
           error: str | None = None, stage: str | None = None) -> dict:
    return {"op": op, "target": "t.history", "key": key,
            "changed": True, "error": error, "stage": stage}


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

    def _run_trusted(self) -> tuple[int, str, str]:
        return self._run_bundle(trust=self.trust)

    def _trust_files(self) -> dict[str, bytes]:
        files: dict[str, bytes] = {}
        for name in sorted(os.listdir(self.trust)):
            path = os.path.join(self.trust, name)
            if os.path.isfile(path):
                with open(path, "rb") as handle:
                    files[name] = handle.read()
        return files

    def _head(self) -> str:
        with open(os.path.join(self.trust, "head.json"), "rb") as handle:
            return json.loads(handle.read())["head"]


class EstablishTest(_Fixture):
    def test_first_use_creates_the_trust_anchor(self) -> None:
        self._record("a")
        self._export()
        raw = open(self.checkpoint, "rb").read()
        status, stdout, stderr = self._run_trusted()
        self.assertEqual((status, stderr), (0, ""))
        result = json.loads(stdout)
        self.assertEqual(list(result),
                         ["valid", "etag", "generation", "closed",
                          "events", "next"])
        self.assertEqual(result["etag"], self._etag())
        # The directory now holds the head, one node and the saved
        # original checkpoint bytes.
        files = self._trust_files()
        self.assertEqual(len(files), 3)
        self.assertIn(raw, files.values())
        node = json.loads(files["node-" + self._head() + ".json"])
        self.assertEqual(node["tag"], self._etag())
        self.assertIsNone(node["previous_tag"])
        self.assertIsNone(node["previous"])
        self.assertEqual(node["checkpoint_digest"],
                         hashlib.sha256(raw).hexdigest())

    def test_without_trust_dir_no_directory_is_created(self) -> None:
        self._record("a")
        self._export()
        status, _, _ = self._run_bundle()
        self.assertEqual(status, 0)
        self.assertFalse(os.path.exists(self.trust))
        self.assertFalse(os.path.exists(self.trust + ".lock"))

    def test_missing_trust_parent_is_unavailable(self) -> None:
        self._record("a")
        self._export()
        missing = os.path.join(self.tmp.name, "missing", "trust")
        status, stdout, stderr = self._run_bundle(trust=missing)
        self.assertEqual(status, 5)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr), {"error": "bundle_unavailable"})

    def test_library_rejects_a_bad_trust_dir_before_reading(self) -> None:
        missing = os.path.join(self.tmp.name, "missing.json")
        for trust_dir in ("", 0, 1.5, b"x", object()):
            with self.subTest(trust_dir=trust_dir):
                with self.assertRaises(ValueError):
                    audit_proof.verify_bundle(missing, missing,
                                              '"' + "0" * 64 + '"',
                                              trust_dir=trust_dir)


class ReplayTest(_Fixture):
    def test_same_tag_is_an_idempotent_replay(self) -> None:
        self._record("a")
        self._export()
        self.assertEqual(self._run_trusted()[0], 0)
        before = self._trust_files()
        before_head = self._head()
        # The proof is fully rechecked and the directory is not
        # rewritten.
        status, stdout, stderr = self._run_trusted()
        self.assertEqual((status, stderr), (0, ""))
        self.assertEqual(json.loads(stdout)["etag"], self._etag())
        self.assertEqual(self._trust_files(), before)
        self.assertEqual(self._head(), before_head)

    def test_replay_still_rejects_a_tampered_proof(self) -> None:
        self._record("a")
        proof = self._export()
        self.assertEqual(self._run_trusted()[0], 0)
        proof["closed"] = True
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False))
        status, _, stderr = self._run_trusted()
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})


class AdvanceTest(_Fixture):
    def test_new_tag_advances_the_chain_head(self) -> None:
        self._record("a")
        self._export("g1")
        self.assertEqual(self._run_trusted()[0], 0)
        genesis = self._head()
        self._record("b")
        self._export("g1")
        status, stdout, stderr = self._run_trusted()
        self.assertEqual((status, stderr), (0, ""))
        self.assertEqual(json.loads(stdout)["etag"], self._etag())
        head = self._head()
        self.assertNotEqual(head, genesis)
        files = self._trust_files()
        # Both nodes, both snapshots and the head are retained.
        self.assertEqual(len(files), 5)
        node = json.loads(files["node-" + head + ".json"])
        old_raw_tag = node["previous_tag"]
        self.assertEqual(node["previous"], genesis)
        previous = json.loads(files["node-" + genesis + ".json"])
        self.assertEqual(previous["tag"], old_raw_tag)

    def test_chain_survives_a_process_restart(self) -> None:
        # All chain state is on disk: a fresh verification walk after
        # the last writer is gone still checks the predecessor links.
        self._record("a")
        self._export("g1")
        self.assertEqual(self._run_trusted()[0], 0)
        self._record("b")
        self._export("g1")
        self.assertEqual(self._run_trusted()[0], 0)
        self._record("c")
        self._export("g1", final=True)
        status, _, _ = self._run_trusted()
        self.assertEqual(status, 0)
        nodes = [name for name in self._trust_files()
                 if name.startswith("node-")]
        self.assertEqual(len(nodes), 3)

    def test_old_version_replay_is_rejected_as_rollback(self) -> None:
        self._record("a")
        self._export("g1")
        self.assertEqual(self._run_trusted()[0], 0)
        old_proof = open(self.proof_path, "rb").read()
        old_raw = open(self.checkpoint, "rb").read()
        old_etag = self._etag()
        self._record("b")
        self._export("g1")
        self.assertEqual(self._run_trusted()[0], 0)
        # The retained older bundle is self-consistent but must fail.
        with open(self.checkpoint, "wb") as handle:
            handle.write(old_raw)
        with open(self.proof_path, "wb") as handle:
            handle.write(old_proof)
        status, stdout, stderr = self._run_bundle(etag=old_etag,
                                                  trust=self.trust)
        self.assertEqual(status, 4)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})

    def test_later_arriving_older_version_fails(self) -> None:
        # Concurrent verification of different versions: only the
        # successor advances; the older version, even self-consistent,
        # fails once the head moved.
        self._record("a")
        self._export("g1")
        self.assertEqual(self._run_trusted()[0], 0)
        old_proof = open(self.proof_path, "rb").read()
        old_raw = open(self.checkpoint, "rb").read()
        old_etag = self._etag()
        self._record("b")
        self._export("g1")
        new_etag = self._etag()

        other_dir = os.path.join(self.tmp.name, "other")
        os.mkdir(other_dir)
        old_checkpoint = os.path.join(other_dir, "old-checkpoint.json")
        old_proof_path = os.path.join(other_dir, "old-proof.json")
        with open(old_checkpoint, "wb") as handle:
            handle.write(old_raw)
        with open(old_proof_path, "wb") as handle:
            handle.write(old_proof)

        outcomes: dict[str, int] = {}
        barrier = threading.Barrier(2)

        def run(name: str, checkpoint: str, proof: str, etag: str) -> None:
            barrier.wait()
            try:
                audit_proof.verify_bundle(
                    checkpoint, proof, etag, trust_dir=self.trust)
            except audit_proof.BundleMismatchError:
                outcomes[name] = 4
            else:
                outcomes[name] = 0

        threads = [threading.Thread(
                       target=run,
                       args=("old", old_checkpoint, old_proof_path,
                             old_etag)),
                   threading.Thread(
                       target=run,
                       args=("new", self.checkpoint, self.proof_path,
                             new_etag))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        # The successor advanced exactly once; the older version either
        # lost the race (mismatch) or, if it ran first, was the replay
        # of the then-current head -- never both advancing.
        self.assertEqual(outcomes["new"], 0)
        self.assertIn(outcomes["old"], (0, 4))
        if outcomes["old"] == 0:
            # The old one replayed the head before the advance; a
            # second attempt now must fail.
            with self.assertRaises(audit_proof.BundleMismatchError):
                audit_proof.verify_bundle(
                    old_checkpoint, old_proof_path, old_etag,
                    trust_dir=self.trust)
        # The head names the successor's node, exactly once advanced.
        head = json.loads(open(os.path.join(
            self.trust, "node-" + self._head() + ".json"), "rb").read())
        self.assertEqual(head["tag"], new_etag)

    def test_forked_checkpoint_is_rejected(self) -> None:
        self._record("a")
        self._export("g1")
        self.assertEqual(self._run_trusted()[0], 0)
        # A self-consistent bundle whose checkpoint rewrote the
        # retained anchor history: a different journal and checkpoint.
        other_dir = os.path.join(self.tmp.name, "fork")
        os.mkdir(other_dir)
        fork_journal = os.path.join(other_dir, "audit.json")
        fork_checkpoint = os.path.join(other_dir, "checkpoint.json")
        fork_proof = os.path.join(other_dir, "proof.json")
        audit.record(fork_journal, "z", _event())
        proof = audit_proof.export(fork_journal, fork_checkpoint, "g1")
        with open(fork_proof, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False))
        status, stdout, stderr = self._run_bundle(
            checkpoint=fork_checkpoint, proof=fork_proof,
            etag=self._etag(fork_checkpoint), trust=self.trust)
        self.assertEqual(status, 4)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})
        # The failed fork did not move the head or add files.
        self.assertEqual(len(self._trust_files()), 3)

    def test_checkpoint_missing_a_retained_anchor_is_rejected(self) -> None:
        self._record("a")
        self._export("g1")
        self._record("b")
        self._export("g1")
        self.assertEqual(self._run_trusted()[0], 0)
        # A checkpoint that continues from the first anchor only -- a
        # prefix of the trusted history -- cannot replace the head.
        other_dir = os.path.join(self.tmp.name, "prefix")
        os.mkdir(other_dir)
        prefix_journal = os.path.join(other_dir, "audit.json")
        prefix_checkpoint = os.path.join(other_dir, "checkpoint.json")
        prefix_proof = os.path.join(other_dir, "proof.json")
        audit.record(prefix_journal, "a", _event())
        audit.record(prefix_journal, "b", _event())
        proof = audit_proof.export(prefix_journal, prefix_checkpoint,
                                   "g1")
        with open(prefix_proof, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False))
        status, _, stderr = self._run_bundle(
            checkpoint=prefix_checkpoint, proof=prefix_proof,
            etag=self._etag(prefix_checkpoint), trust=self.trust)
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})


class TrustMetadataTest(_Fixture):
    def _establish(self) -> None:
        self._record("a")
        self._export("g1")
        self.assertEqual(self._run_trusted()[0], 0)

    def _tamper(self, name: str, raw: bytes) -> None:
        with open(os.path.join(self.trust, name), "wb") as handle:
            handle.write(raw)

    def test_malformed_trust_metadata_is_invalid_bundle(self) -> None:
        self._establish()
        head = os.path.join(self.trust, "head.json")
        good = open(head, "rb").read()
        variants = [
            b"\xff\xfe",
            b"{not json",
            good.replace(b'"version":1', b'"version":-0', 1),
            good.replace(b'"version":1', b'"version":NaN', 1),
            good.replace(b'"version":1', b'"version":2', 1),
            b'{"head":"' + self._head().encode() + b'","version":1}\n',
            b'{"version":1,"head":"' + b"0" * 63 + b'"}\n',
        ]
        for raw in variants:
            with self.subTest(raw=raw[:24]):
                self._tamper("head.json", raw)
                status, stdout, stderr = self._run_trusted()
                self.assertEqual(status, 3)
                self.assertEqual(stdout, "")
                self.assertEqual(json.loads(stderr),
                                 {"error": "invalid_bundle"})

    def test_malformed_node_is_invalid_bundle(self) -> None:
        self._establish()
        name = "node-" + self._head() + ".json"
        path = os.path.join(self.trust, name)
        good = open(path, "rb").read()
        node = json.loads(good)
        reordered = {"version": 1, "previous_tag": None,
                     "tag": node["tag"],
                     "checkpoint_digest": node["checkpoint_digest"],
                     "previous": None, "digest": node["digest"]}
        variants = [
            b"{not json",
            good.replace(b'"version":1', b'"version":-0', 1),
            (json.dumps(reordered, separators=(",", ":")) + "\n")
            .encode("utf-8"),
        ]
        for raw in variants:
            with self.subTest(raw=raw[:24]):
                self._tamper(name, raw)
                status, _, stderr = self._run_trusted()
                self.assertEqual(status, 3)
                self.assertEqual(json.loads(stderr),
                                 {"error": "invalid_bundle"})

    def test_tampered_chain_is_verification_failed_and_preserved(
            self) -> None:
        self._record("a")
        self._export("g1")
        self._record("b")
        self._export("g1")
        self.assertEqual(self._run_trusted()[0], 0)
        head_name = "node-" + self._head() + ".json"
        head_path = os.path.join(self.trust, head_name)
        node = json.loads(open(head_path, "rb").read())

        def tampered(**changes) -> bytes:
            document = dict(node)
            document.update(changes)
            return (json.dumps(document, separators=(",", ":")) + "\n"
                    ).encode("utf-8")

        variants = [
            tampered(digest="0" * 64),               # chain digest
            tampered(previous="0" * 64),             # predecessor link
            tampered(previous_tag='"' + "0" * 64 + '"'),
            tampered(checkpoint_digest="0" * 64),    # snapshot binding
        ]
        for raw in variants:
            with self.subTest(raw=raw[:40]):
                self._tamper(head_name, raw)
                tampered_files = self._trust_files()
                status, stdout, stderr = self._run_trusted()
                self.assertEqual(status, 4)
                self.assertEqual(stdout, "")
                self.assertEqual(json.loads(stderr),
                                 {"error": "verification_failed"})
                # The tampered evidence is not repaired or overwritten
                # with the current bundle.
                self.assertEqual(self._trust_files(), tampered_files)

    def test_missing_chain_members_are_verification_failed(self) -> None:
        self._record("a")
        self._export("g1")
        self._record("b")
        self._export("g1")
        self.assertEqual(self._run_trusted()[0], 0)
        head = self._head()
        members = ["head.json", "node-" + head + ".json"]
        node = json.loads(open(os.path.join(
            self.trust, members[1]), "rb").read())
        members.append("checkpoint-" + node["checkpoint_digest"]
                       + ".json")
        for name in members:
            with self.subTest(name=name):
                path = os.path.join(self.trust, name)
                saved = open(path, "rb").read()
                os.unlink(path)
                try:
                    status, _, stderr = self._run_trusted()
                    self.assertEqual(status, 4)
                    self.assertEqual(json.loads(stderr),
                                     {"error": "verification_failed"})
                finally:
                    with open(path, "wb") as handle:
                        handle.write(saved)

    def test_existing_directory_without_a_head_is_not_initialized(
            self) -> None:
        self._record("a")
        self._export()
        os.mkdir(self.trust)
        status, _, stderr = self._run_trusted()
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})
        # Nothing was established inside the suspicious directory.
        self.assertEqual(os.listdir(self.trust), [])


class ArgumentTest(_Fixture):
    def test_trust_dir_argument_errors_are_invalid_request(self) -> None:
        self._record("a")
        self._export()
        etag = self._etag()
        variants = [
            ["--checkpoint", self.checkpoint, "--proof", self.proof_path,
             "--etag", etag, "--trust-dir", ""],
            ["--checkpoint", self.checkpoint, "--proof", self.proof_path,
             "--etag", etag, "--trust-dir", self.trust,
             "--trust-dir", self.trust],
            ["--checkpoint", self.checkpoint, "--proof", self.proof_path,
             "--etag", etag, "--trust-dir"],
        ]
        for args in variants:
            with self.subTest(args=args):
                status, stdout, stderr = self._run(*args)
                self.assertEqual(status, 2)
                self.assertEqual(stdout, "")
                self.assertEqual(json.loads(stderr),
                                 {"error": "invalid_request"})
        self.assertFalse(os.path.exists(self.trust))

    def test_success_output_is_unchanged_with_trust_dir(self) -> None:
        self._record("b", error="OSError", stage="同步")
        self._record("a")
        self._export("世代-1")
        status, stdout, stderr = self._run_trusted()
        self.assertEqual((status, stderr), (0, ""))
        result = json.loads(stdout)
        self.assertEqual(list(result),
                         ["valid", "etag", "generation", "closed",
                          "events", "next"])
        self.assertIs(result["valid"], True)
        self.assertEqual(result["generation"], "世代-1")
        self.assertIn("世代-1", stdout)
        self.assertNotIn("\\u", stdout)
        self.assertTrue(stdout.endswith("\n"))
        self.assertFalse(stdout.endswith("\n\n"))


class CommitFailureTest(_Fixture):
    def test_failed_advance_preserves_the_previous_head(self) -> None:
        self._record("a")
        self._export("g1")
        self.assertEqual(self._run_trusted()[0], 0)
        genesis = self._head()
        self._record("b")
        self._export("g1")
        real_replace = os.replace
        calls: list[str] = []

        def failing_replace(src: str, dst: str) -> None:
            if os.path.basename(dst) == "head.json" and not calls:
                calls.append(dst)
                raise OSError("injected head commit failure")
            real_replace(src, dst)

        with mock.patch.object(os, "replace", failing_replace):
            with self.assertRaises(OSError):
                audit_proof.verify_bundle(
                    self.checkpoint, self.proof_path, self._etag(),
                    trust_dir=self.trust)
        # The chain head still names the genesis node; the unreferenced
        # complete node and snapshot may remain and are reused on
        # re-entry.
        self.assertEqual(self._head(), genesis)
        status, _, _ = self._run_trusted()
        self.assertEqual(status, 0)
        self.assertNotEqual(self._head(), genesis)


if __name__ == "__main__":
    unittest.main()
