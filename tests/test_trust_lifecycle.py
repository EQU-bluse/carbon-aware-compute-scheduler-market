"""Recoverable lifecycle and full-chain inspection of the trust directory.

Complements ``test_trust_chain``: the preparing/ready lifecycle marker
and its crash windows, idempotent completion from preparing with the
identical bundle, rejection and evidence preservation for a different
bundle, adoption of a legacy head-only directory after a full-chain
verification, refusal to take over empty or fragment-only directories,
the ready/head/head-node/head-snapshot binding, the first-node-to-head
inspection on every verification (including same-tag replays), the
strict append-only relation between adjacent retained snapshots,
unreferenced fragments staying out of every decision, and the
no-overwrite rule for content-addressed evidence.
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
from types import SimpleNamespace
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


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _tag_of(raw: bytes) -> str:
    return '"' + _sha(raw) + '"'


class _Fixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.checkpoint = os.path.join(self.tmp.name, "checkpoint.json")
        self.bundles = os.path.join(self.tmp.name, "bundles")
        os.mkdir(self.bundles)
        self.trust = os.path.join(self.tmp.name, "trust")

    def _record(self, audit_key: str) -> None:
        audit.record(self.journal, audit_key, _event(key=audit_key))

    def _bundle(self, name: str, generation: str = "g1",
                **kwargs: object) -> SimpleNamespace:
        # Exports append to the one growing checkpoint; each named
        # bundle then snapshots the exact checkpoint/proof bytes, so an
        # older bundle stays verifiable after newer exports.
        proof_path = os.path.join(self.bundles, name + ".pp.json")
        proof = audit_proof.export(self.journal, self.checkpoint,
                                   generation, **kwargs)
        with open(proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False))
        with open(self.checkpoint, "rb") as handle:
            raw = handle.read()
        checkpoint = os.path.join(self.bundles, name + ".cp.json")
        with open(checkpoint, "wb") as handle:
            handle.write(raw)
        return SimpleNamespace(cp=checkpoint, pp=proof_path, raw=raw,
                               tag=_tag_of(raw), proof=proof)

    def _lib(self, bundle: SimpleNamespace,
             trust: str | None = None) -> dict:
        return audit_proof.verify_bundle(
            bundle.cp, bundle.pp, bundle.tag,
            trust_dir=self.trust if trust is None else trust)

    def _cli(self, bundle: SimpleNamespace,
             trust: str | None = None) -> tuple[int, str, str]:
        trust_dir = self.trust if trust is None else trust
        args = ["--checkpoint", bundle.cp, "--proof", bundle.pp,
                "--etag", bundle.tag, "--trust-dir", trust_dir]
        return _run_cli(*args)

    def _tree(self, path: str) -> dict[str, bytes]:
        result = {}
        for root, _, files in os.walk(path):
            for name in files:
                full = os.path.join(root, name)
                rel = os.path.relpath(full, path)
                if rel.endswith(".lock"):
                    continue
                with open(full, "rb") as handle:
                    result[rel] = handle.read()
        return result

    def _read_state(self) -> dict:
        return json.loads(self._tree(self.trust)["state.json"])

    def _read_head(self) -> dict:
        return json.loads(self._tree(self.trust)["head.json"])

    def _node_path(self, digest: str) -> str:
        return os.path.join(self.trust, "nodes", digest + ".json")

    def _snapshot_path(self, digest: str) -> str:
        return os.path.join(self.trust, "snapshots", digest + ".json")

    # --- legacy (pre-lifecycle) directory construction ---

    def _write_legacy(self, trust: str,
                      members: list[bytes]) -> tuple[str, str]:
        # Write nodes/, snapshots/ and head.json exactly like a
        # directory created before state.json existed: no lifecycle
        # marker. Returns (head node digest, head checkpoint digest).
        os.makedirs(os.path.join(trust, "nodes"))
        os.makedirs(os.path.join(trust, "snapshots"))
        previous_digest: str | None = None
        previous_tag: str | None = None
        node_digest = ""
        checkpoint_digest = ""
        for raw in members:
            checkpoint_digest = _sha(raw)
            tag = '"' + checkpoint_digest + '"'
            node_digest = audit_proof._node_digest(
                tag, previous_tag, checkpoint_digest, previous_digest)
            node = {"tag": tag, "previous_tag": previous_tag,
                    "checkpoint_digest": checkpoint_digest,
                    "previous_digest": previous_digest}
            with open(self._node_path_for(trust, node_digest), "wb") \
                    as handle:
                handle.write(audit_proof._serialize_node(node))
            with open(self._snapshot_path_for(
                    trust, checkpoint_digest), "wb") as handle:
                handle.write(raw)
            previous_digest, previous_tag = node_digest, tag
        with open(os.path.join(trust, "head.json"), "wb") as handle:
            handle.write(audit_proof._serialize_head(node_digest))
        return node_digest, checkpoint_digest

    @staticmethod
    def _node_path_for(trust: str, digest: str) -> str:
        return os.path.join(trust, "nodes", digest + ".json")

    @staticmethod
    def _snapshot_path_for(trust: str, digest: str) -> str:
        return os.path.join(trust, "snapshots", digest + ".json")

    # --- crash injection (per-class helpers build the expected chain) ---

class FirstUseLifecycleTest(_Fixture):
    def test_first_use_passes_through_preparing_to_ready(self) -> None:
        self._record("a")
        bundle = self._bundle("b1")
        status, stdout, stderr = self._cli(bundle)
        self.assertEqual((status, stderr), (0, ""))
        state = self._read_state()
        self.assertEqual(state["state"], "ready")
        self.assertEqual(state["tag"], bundle.tag)
        self.assertEqual(state["checkpoint_digest"], _sha(bundle.raw))
        self.assertEqual(state["node_digest"], state["head_digest"])
        head = self._read_head()
        self.assertEqual(head["digest"], state["head_digest"])
        node = json.loads(self._tree(self.trust)[
            f"nodes/{head['digest']}.json"])
        self.assertIsNone(node["previous_tag"])
        self.assertIsNone(node["previous_digest"])
        self.assertEqual(node["checkpoint_digest"],
                         state["checkpoint_digest"])

    def test_ready_state_has_exact_field_order(self) -> None:
        self._record("a")
        bundle = self._bundle("b1")
        self.assertEqual(self._cli(bundle)[0], 0)
        raw = self._tree(self.trust)["state.json"]
        self.assertEqual(list(json.loads(raw)),
                         ["version", "state", "tag", "checkpoint_digest",
                          "node_digest", "head_digest"])
        self.assertTrue(raw.endswith(b"\n"))


class FirstUseRecoveryTest(_Fixture):
    def _b1(self) -> SimpleNamespace:
        self._record("a")
        return self._bundle("b1")

    def test_crash_in_every_first_use_window_resumes(self) -> None:
        bundle = self._b1()
        # Four interrupted-preparation windows: preparing marker,
        # snapshot, node, head committed -- the fifth commit is the
        # ready marker that completes the whole sequence.
        for window in range(1, 5):
            with self.subTest(window=window):
                trust = os.path.join(self.tmp.name, f"trust{window}")
                committed = self._crash_in(trust, window, bundle)
                # The first commit is always the preparing marker.
                self.assertTrue(committed[0].endswith("state.json"))
                state = json.loads(open(os.path.join(
                    trust, "state.json"), encoding="utf-8").read())
                self.assertEqual(state["state"], "preparing")
                # The identical bundle finishes the preparation.
                result = audit_proof.verify_bundle(
                    bundle.cp, bundle.pp, bundle.tag, trust_dir=trust)
                self.assertEqual(result["generation"], "g1")
                state = json.loads(open(os.path.join(
                    trust, "state.json"), encoding="utf-8").read())
                self.assertEqual(state["state"], "ready")
                self.assertEqual(state["tag"], bundle.tag)
                self.assertEqual(state["head_digest"],
                                 state["node_digest"])

    def _crash_in(self, trust: str, count: int,
                  bundle: SimpleNamespace) -> list[str]:
        real_commit = audit_proof._commit_checkpoint
        committed: list[str] = []

        def flaky(realpath, directory, payload, old_bytes):
            real_commit(realpath, directory, payload, old_bytes)
            committed.append(realpath)
            if len(committed) == count:
                raise OSError("simulated interruption")

        with mock.patch.object(audit_proof, "_commit_checkpoint", flaky):
            with self.assertRaises(OSError):
                audit_proof.verify_bundle(
                    bundle.cp, bundle.pp, bundle.tag, trust_dir=trust)
        return committed

    def test_different_bundle_cannot_finish_preparing(self) -> None:
        b1 = self._b1()
        self._crash_in(self.trust, 1, b1)
        # A second, self-consistent bundle arrives while the first
        # preparation is unfinished: it must be rejected, not overwrite
        # the staged evidence.
        self._record("b")
        b2 = self._bundle("b2")
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._lib(b2)
        status, _, stderr = self._cli(b2)
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})
        state = self._read_state()
        self.assertEqual(state["state"], "preparing")
        self.assertEqual(state["tag"], b1.tag)
        # Nothing from the rejected bundle was stored.
        tree = self._tree(self.trust)
        self.assertNotIn(f"snapshots/{_sha(b2.raw)}.json", tree)
        # The original bundle still completes idempotently.
        self.assertEqual(self._cli(b1)[0], 0)
        self.assertEqual(self._read_state()["state"], "ready")

    def test_preparing_with_wrong_candidate_digest_is_rejected(
            self) -> None:
        b1 = self._b1()
        self._crash_in(self.trust, 1, b1)
        state_path = os.path.join(self.trust, "state.json")
        state = json.loads(open(state_path, encoding="utf-8").read())
        state["node_digest"] = "0" * 64
        with open(state_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(state, separators=(",", ":")) + "\n")
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._lib(b1)


class AdvanceRecoveryTest(_Fixture):
    def _chains(self) -> tuple[SimpleNamespace, SimpleNamespace,
                               SimpleNamespace]:
        self._record("a")
        b1 = self._bundle("b1")
        self.assertEqual(self._cli(b1)[0], 0)
        self._record("b")
        b2 = self._bundle("b2")
        self._record("c")
        b3 = self._bundle("b3")
        return b1, b2, b3

    def test_crash_in_every_advance_window_resumes(self) -> None:
        b1, b2, _ = self._chains()
        for window in range(1, 6):
            with self.subTest(window=window):
                trust = os.path.join(self.tmp.name, f"trust{window}")
                self.assertEqual(audit_proof.verify_bundle(
                    b1.cp, b1.pp, b1.tag, trust_dir=trust)["generation"],
                    "g1")
                self._advance_crash(trust, window, b2)
                result = audit_proof.verify_bundle(
                    b2.cp, b2.pp, b2.tag, trust_dir=trust)
                self.assertEqual(result["generation"], "g1")
                head = json.loads(open(os.path.join(
                    trust, "head.json"), encoding="utf-8").read())
                tree = self._tree(trust)
                node = json.loads(tree[f"nodes/{head['digest']}.json"])
                self.assertEqual(node["tag"], b2.tag)
                self.assertEqual(node["previous_tag"], b1.tag)
                state = json.loads(open(os.path.join(
                    trust, "state.json"), encoding="utf-8").read())
                self.assertEqual(state["state"], "ready")
                self.assertEqual(state["head_digest"], head["digest"])

    def _advance_crash(self, trust: str, count: int,
                       bundle: SimpleNamespace) -> None:
        real_commit = audit_proof._commit_checkpoint
        committed: list[str] = []

        def flaky(realpath, directory, payload, old_bytes):
            real_commit(realpath, directory, payload, old_bytes)
            committed.append(realpath)
            if len(committed) == count:
                raise OSError("simulated interruption")

        with mock.patch.object(audit_proof, "_commit_checkpoint", flaky):
            with self.assertRaises(OSError):
                audit_proof.verify_bundle(
                    bundle.cp, bundle.pp, bundle.tag, trust_dir=trust)

    def test_other_bundle_rejected_then_candidate_resumes(self) -> None:
        b1, b2, b3 = self._chains()
        # Crash right after the preparing flip and again after the head
        # commit; b3 must never finish either preparation.
        for window in (1, 4):
            with self.subTest(window=window):
                trust = os.path.join(self.tmp.name, f"trust-w{window}")
                self.assertEqual(self._cli(b1, trust=trust)[0], 0)
                self._advance_crash(trust, window, b2)
                with self.assertRaises(audit_proof.BundleMismatchError):
                    audit_proof.verify_bundle(
                        b3.cp, b3.pp, b3.tag, trust_dir=trust)
                # The preparing marker still binds b2, evidence and all.
                state = json.loads(open(os.path.join(
                    trust, "state.json"), encoding="utf-8").read())
                self.assertEqual(state["tag"], b2.tag)
                self.assertEqual(audit_proof.verify_bundle(
                    b2.cp, b2.pp, b2.tag, trust_dir=trust)["generation"],
                    "g1")
                # The recovered sequence accepts a normal next advance.
                self.assertEqual(audit_proof.verify_bundle(
                    b3.cp, b3.pp, b3.tag, trust_dir=trust)["generation"],
                    "g1")

    def test_candidate_evidence_tampered_after_crash_fails(self) -> None:
        b1, b2, _ = self._chains()
        # Crash after the head committed: state still preparing.
        self._advance_crash(self.trust, 4, b2)
        head = self._read_head()
        node_path = self._node_path(head["digest"])
        node = json.loads(open(node_path, encoding="utf-8").read())
        node["checkpoint_digest"] = "0" * 64
        with open(node_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(node, separators=(",", ":")) + "\n")
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._lib(b2)


class LegacyAdoptionTest(_Fixture):
    def _two(self) -> tuple[SimpleNamespace, SimpleNamespace]:
        self._record("a")
        b1 = self._bundle("b1")
        self._record("b")
        b2 = self._bundle("b2")
        return b1, b2

    def test_legacy_single_head_directory_is_verified_and_stamped(
            self) -> None:
        b1, _ = self._two()
        os.mkdir(self.trust)
        head_digest, _ = self._write_legacy(self.trust, [b1.raw])
        self.assertFalse(os.path.exists(
            os.path.join(self.trust, "state.json")))
        status, stdout, stderr = self._cli(b1)
        self.assertEqual((status, stderr), (0, ""))
        self.assertEqual(json.loads(stdout)["etag"], b1.tag)
        state = self._read_state()
        self.assertEqual(state["state"], "ready")
        self.assertEqual(state["head_digest"], head_digest)
        self.assertEqual(state["node_digest"], head_digest)
        self.assertEqual(state["tag"], b1.tag)

    def test_legacy_multi_node_chain_is_adopted_then_advanced(self) -> None:
        b1, b2 = self._two()
        os.mkdir(self.trust)
        head_digest, _ = self._write_legacy(self.trust, [b1.raw, b2.raw])
        self.assertEqual(self._cli(b2)[0], 0)
        self.assertEqual(self._read_state()["head_digest"], head_digest)
        # A same-tag replay and a later advance both keep working.
        self.assertEqual(self._cli(b2)[0], 0)
        self._record("c")
        b3 = self._bundle("b3")
        self.assertEqual(self._cli(b3)[0], 0)
        self.assertEqual(self._read_state()["tag"], b3.tag)

    def test_legacy_directory_with_damaged_chain_is_not_adopted(
            self) -> None:
        b1, _ = self._two()
        os.mkdir(self.trust)
        head_digest, _ = self._write_legacy(self.trust, [b1.raw])
        node_path = self._node_path(head_digest)
        node = json.loads(open(node_path, encoding="utf-8").read())
        node["checkpoint_digest"] = "0" * 64
        with open(node_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(node, separators=(",", ":")) + "\n")
        status, stdout, stderr = self._cli(b1)
        self.assertEqual(status, 4)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})
        # No state marker is stamped over a failed adoption.
        self.assertFalse(os.path.exists(
            os.path.join(self.trust, "state.json")))

    def test_legacy_malformed_head_is_invalid_bundle(self) -> None:
        b1, _ = self._two()
        os.mkdir(self.trust)
        self._write_legacy(self.trust, [b1.raw])
        with open(os.path.join(self.trust, "head.json"), "wb") as handle:
            handle.write(b"{not json")
        status, _, stderr = self._cli(b1)
        self.assertEqual(status, 3)
        self.assertEqual(json.loads(stderr), {"error": "invalid_bundle"})

    def test_legacy_snapshot_must_pass_full_structure_check(self) -> None:
        # A retained snapshot whose bytes hit their content address but
        # that is not a well-formed checkpoint is a format error, not a
        # mismatch: every historical snapshot is revalidated.
        b1, _ = self._two()
        bad = b"{not a checkpoint"
        os.mkdir(self.trust)
        os.makedirs(os.path.join(self.trust, "nodes"))
        os.makedirs(os.path.join(self.trust, "snapshots"))
        digest = _sha(bad)
        tag = '"' + digest + '"'
        node_digest = audit_proof._node_digest(tag, None, digest, None)
        node = {"tag": tag, "previous_tag": None,
                "checkpoint_digest": digest, "previous_digest": None}
        with open(self._node_path(node_digest), "wb") as handle:
            handle.write(audit_proof._serialize_node(node))
        with open(self._snapshot_path(digest), "wb") as handle:
            handle.write(bad)
        with open(os.path.join(self.trust, "head.json"), "wb") as handle:
            handle.write(audit_proof._serialize_head(node_digest))
        status, _, stderr = self._cli(b1)
        self.assertEqual(status, 3)
        self.assertEqual(json.loads(stderr), {"error": "invalid_bundle"})

    def test_legacy_chain_must_be_strict_append_between_snapshots(
            self) -> None:
        # Node metadata claims a predecessor relation, but the retained
        # snapshots themselves are in the wrong order: history continuity
        # cannot be inferred from the nodes alone.
        b1, b2 = self._two()
        os.mkdir(self.trust)
        self._write_legacy(self.trust, [b2.raw, b1.raw])
        status, stdout, stderr = self._cli(b1)
        self.assertEqual(status, 4)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})


class OrphanDirectoryTest(_Fixture):
    def _b1(self) -> SimpleNamespace:
        self._record("a")
        return self._bundle("b1")

    def test_pre_existing_empty_directory_is_not_taken_over(self) -> None:
        bundle = self._b1()
        os.mkdir(self.trust)
        status, stdout, stderr = self._cli(bundle)
        self.assertEqual(status, 4)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})
        # Nothing was written into the refused directory.
        self.assertEqual(os.listdir(self.trust), [])

    def test_fragment_only_directory_is_not_taken_over(self) -> None:
        bundle = self._b1()
        os.makedirs(os.path.join(self.trust, "nodes"))
        os.makedirs(os.path.join(self.trust, "snapshots"))
        junk = b'{"version":1}'
        fragments = ["nodes/" + "a" * 64 + ".json",
                     "snapshots/" + "b" * 64 + ".json",
                     ".audit-proof-leftover.tmp"]
        for rel in fragments:
            path = os.path.join(self.trust, rel)
            with open(path, "wb") as handle:
                handle.write(junk)
        status, _, stderr = self._cli(bundle)
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})
        # The fragments are preserved, never adopted or deleted.
        for rel in fragments:
            with open(os.path.join(self.trust, rel), "rb") as handle:
                self.assertEqual(handle.read(), junk)

    def test_trust_path_that_is_a_file_is_bundle_unavailable(self) -> None:
        bundle = self._b1()
        with open(self.trust, "wb") as handle:
            handle.write(b"not a directory")
        status, _, stderr = self._cli(bundle)
        self.assertEqual(status, 5)
        self.assertEqual(json.loads(stderr),
                         {"error": "bundle_unavailable"})


class ReadyBindingTest(_Fixture):
    def _ready(self) -> SimpleNamespace:
        self._record("a")
        bundle = self._bundle("b1")
        self.assertEqual(self._cli(bundle)[0], 0)
        return bundle

    def _write_state(self, state: dict, raw: bytes | None = None) -> None:
        path = os.path.join(self.trust, "state.json")
        if raw is None:
            raw = json.dumps(state, separators=(",", ":")).encode() + b"\n"
        with open(path, "wb") as handle:
            handle.write(raw)

    def _read_state_dict(self) -> dict:
        return json.loads(open(os.path.join(
            self.trust, "state.json"), encoding="utf-8").read())

    def test_ready_with_wrong_head_digest_fails(self) -> None:
        bundle = self._ready()
        state = self._read_state_dict()
        state["head_digest"] = "0" * 64
        self._write_state(state)
        self.assertEqual(self._cli(bundle)[0], 4)

    def test_ready_with_wrong_node_digest_fails(self) -> None:
        bundle = self._ready()
        state = self._read_state_dict()
        state["node_digest"] = "0" * 64
        self._write_state(state)
        self.assertEqual(self._cli(bundle)[0], 4)

    def test_ready_that_does_not_bind_head_node_tag_fails(self) -> None:
        bundle = self._ready()
        state = self._read_state_dict()
        other = '"' + "1" * 64 + '"'
        state["tag"] = other
        state["checkpoint_digest"] = "1" * 64
        self._write_state(state)
        self.assertEqual(self._cli(bundle)[0], 4)

    def test_ready_without_head_file_fails(self) -> None:
        bundle = self._ready()
        os.unlink(os.path.join(self.trust, "head.json"))
        self.assertEqual(self._cli(bundle)[0], 4)

    def test_ready_state_contradicting_its_field_set_fails(self) -> None:
        bundle = self._ready()
        state = self._read_state_dict()
        # Ready fields present, preparing literal: a lifecycle
        # contradiction, not a format error.
        state["state"] = "preparing"
        self._write_state(state)
        self.assertEqual(self._cli(bundle)[0], 4)
        # Preparing fields present, ready literal: same.
        del state["head_digest"]
        state["state"] = "ready"
        self._write_state(state)
        self.assertEqual(self._cli(bundle)[0], 4)

    def test_malformed_state_metadata_is_invalid_bundle(self) -> None:
        bundle = self._ready()
        good = json.loads(open(os.path.join(
            self.trust, "state.json"), encoding="utf-8").read())
        variants = [
            b"\xff\xfe",
            b"{not json",
            b"[]",
            json.dumps(dict(good, version=2),
                       separators=(",", ":")).encode(),
            json.dumps(dict(good, state="other"),
                       separators=(",", ":")).encode(),
            json.dumps(dict(good, tag="not-a-tag"),
                       separators=(",", ":")).encode(),
            json.dumps(dict(good, extra=1),
                       separators=(",", ":")).encode(),
        ]
        # A negative-zero version literal.
        variants.append(json.dumps(dict(good, version=0),
                                   separators=(",", ":")).replace(
            '"version":0', '"version":-0', 1).encode())
        for raw in variants:
            with self.subTest(raw=raw[:20]):
                self._write_state(good, raw=raw)
                status, stdout, stderr = self._cli(bundle)
                self.assertEqual(status, 3)
                self.assertEqual(stdout, "")
                self.assertEqual(json.loads(stderr),
                                 {"error": "invalid_bundle"})


class FullChainInspectionTest(_Fixture):
    def _chain_of_three(self) -> list[SimpleNamespace]:
        bundles = []
        for key in ("a", "b", "c"):
            self._record(key)
            bundle = self._bundle(f"b{key}")
            bundles.append(bundle)
            self.assertEqual(self._cli(bundle)[0], 0)
        return bundles

    def test_same_tag_replay_walks_the_whole_chain(self) -> None:
        bundles = self._chain_of_three()
        # The replay succeeds while the history is intact...
        self.assertEqual(self._cli(bundles[-1])[0], 0)

    def test_corrupted_historical_node_fails_replay(self) -> None:
        bundles = self._chain_of_three()
        head = self._read_head()
        head_node = json.loads(self._tree(self.trust)[
            f"nodes/{head['digest']}.json"])
        old_path = self._node_path(head_node["previous_digest"])
        old_node = json.loads(open(old_path, encoding="utf-8").read())
        old_node["checkpoint_digest"] = "f" * 64
        with open(old_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(old_node, separators=(",", ":")) + "\n")
        status, stdout, stderr = self._cli(bundles[-1])
        self.assertEqual(status, 4)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})
        # The evidence is preserved, not repaired from the current bundle.
        self.assertEqual(json.loads(open(old_path, encoding="utf-8")
                                   .read())["checkpoint_digest"],
                         "f" * 64)

    def test_missing_historical_node_fails_replay(self) -> None:
        bundles = self._chain_of_three()
        head = self._read_head()
        head_node = json.loads(self._tree(self.trust)[
            f"nodes/{head['digest']}.json"])
        os.unlink(self._node_path(head_node["previous_digest"]))
        self.assertEqual(self._cli(bundles[-1])[0], 4)

    def test_missing_historical_snapshot_fails_replay(self) -> None:
        bundles = self._chain_of_three()
        head = self._read_head()
        node = json.loads(self._tree(self.trust)[
            f"nodes/{head['digest']}.json"])
        os.unlink(self._snapshot_path(node["checkpoint_digest"]))
        # The head snapshot missing fails even the same-tag replay.
        self.assertEqual(self._cli(bundles[-1])[0], 4)

    def test_corrupted_historical_snapshot_fails_and_is_kept(self) -> None:
        bundles = self._chain_of_three()
        head = self._read_head()
        node = json.loads(self._tree(self.trust)[
            f"nodes/{head['digest']}.json"])
        snapshot_path = self._snapshot_path(node["checkpoint_digest"])
        with open(snapshot_path, "wb") as handle:
            handle.write(b"tampered bytes")
        status, _, stderr = self._cli(bundles[-1])
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})
        # No overwrite or "repair" with the bundle's correct bytes.
        self.assertEqual(open(snapshot_path, "rb").read(),
                         b"tampered bytes")

    def test_broken_predecessor_relation_fails(self) -> None:
        bundles = self._chain_of_three()
        head = self._read_head()
        head_path = self._node_path(head["digest"])
        head_node = json.loads(open(head_path, encoding="utf-8").read())
        # Point the predecessor digest at a nonexistent node; the tag
        # relation cannot save it.
        head_node["previous_digest"] = "9" * 64
        with open(head_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(head_node, separators=(",", ":"))
                         + "\n")
        self.assertEqual(self._cli(bundles[-1])[0], 4)

    def test_anchor_node_naming_a_predecessor_fails(self) -> None:
        bundles = self._chain_of_three()
        tree = self._tree(self.trust)
        # Find the first (anchor) node through the chain.
        digest = self._read_head()["digest"]
        while True:
            node = json.loads(tree[f"nodes/{digest}.json"])
            if node["previous_digest"] is None:
                anchor_digest = digest
                break
            digest = node["previous_digest"]
        anchor_path = self._node_path(anchor_digest)
        node["previous_tag"] = '"' + "2" * 64 + '"'
        node["previous_digest"] = "2" * 64
        with open(anchor_path, "wb") as handle:
            handle.write(audit_proof._serialize_node(node))
        # The file no longer lives at its own content digest.
        self.assertEqual(self._cli(bundles[-1])[0], 4)


class FragmentAndEvidenceTest(_Fixture):
    def test_unreferenced_fragments_never_take_part_in_decisions(
            self) -> None:
        self._record("a")
        b1 = self._bundle("b1")
        self.assertEqual(self._cli(b1)[0], 0)
        tree_before = self._tree(self.trust)
        fragments = {
            "nodes/" + "d" * 64 + ".json": b'{"version":1}',
            "snapshots/" + "e" * 64 + ".json": b"junk",
            ".audit-proof-x.tmp": b"partial",
        }
        for rel, raw in fragments.items():
            path = os.path.join(self.trust, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as handle:
                handle.write(raw)
        # The replay ignores the fragments and succeeds.
        self.assertEqual(self._cli(b1)[0], 0)
        self.assertEqual(self._cli(b1)[0], 0)
        tree_after = self._tree(self.trust)
        for rel, raw in fragments.items():
            self.assertEqual(tree_after[rel], raw)
        for rel, raw in tree_before.items():
            self.assertEqual(tree_after[rel], raw)

    def test_content_address_with_other_bytes_blocks_advance(self) -> None:
        self._record("a")
        b1 = self._bundle("b1")
        self.assertEqual(self._cli(b1)[0], 0)
        self._record("b")
        b2 = self._bundle("b2")
        # Pre-stage wrong bytes at the candidate's content address.
        target = self._snapshot_path(_sha(b2.raw))
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "wb") as handle:
            handle.write(b"attacker controlled bytes")
        status, stdout, stderr = self._cli(b2)
        self.assertEqual(status, 4)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})
        # The foreign bytes are not overwritten and the head stays put.
        self.assertEqual(open(target, "rb").read(),
                         b"attacker controlled bytes")
        self.assertEqual(self._read_state()["tag"], b1.tag)
        self.assertEqual(self._cli(b1)[0], 0)


if __name__ == "__main__":
    unittest.main()
