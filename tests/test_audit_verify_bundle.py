"""CLI tests for the verify-bundle offline verification command.

Covers python -m carbon_market verify-bundle: the exactly-once
non-empty option contract and strong-tag format (exit 2,
invalid_request, before any file is read), the etag-first checkpoint
recheck, UTF-8/JSON/structure failures (exit 3, invalid_bundle),
tag/chain/generation/anchor/state/page mismatches (exit 4,
verification_failed), missing files (exit 5, bundle_unavailable), the
compact single-line success object, and the read-only behaviour.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import audit, audit_proof


def _event(op: str = "copy", target: str = "t.history", key: str = "batch-k",
           changed: bool = True, error: str | None = None,
           stage: str | None = None) -> dict[str, object]:
    return {"op": op, "target": target, "key": key, "changed": changed,
            "error": error, "stage": stage}


def _compact(payload: object) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                       allow_nan=False) + "\n").encode("utf-8")


class _Fixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.checkpoint = os.path.join(self.tmp.name, "checkpoint.json")
        self.proof = os.path.join(self.tmp.name, "proof.json")

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "carbon_market", "verify-bundle", *args],
            capture_output=True, timeout=10)

    def _run_bundle(self, checkpoint: str | None = None,
                    proof: str | None = None,
                    etag: str | None = None) -> subprocess.CompletedProcess:
        return self._run(
            "--checkpoint", self.checkpoint if checkpoint is None
            else checkpoint,
            "--proof", self.proof if proof is None else proof,
            "--etag", self._etag() if etag is None else etag)

    def _etag(self) -> str:
        with open(self.checkpoint, "rb") as handle:
            raw = handle.read()
        return '"' + hashlib.sha256(raw).hexdigest() + '"'

    def _digest(self) -> str:
        return self._etag()[1:-1]

    def _write(self, path: str, payload: bytes) -> None:
        with open(path, "wb") as handle:
            handle.write(payload)

    def _write_proof(self, proof: dict) -> None:
        self._write(self.proof, _compact(proof))

    def _export(self, generation: str = "g1", **kw: object) -> dict:
        proof = audit_proof.export(self.journal, self.checkpoint, generation,
                                   **kw)
        self._write_proof(proof)
        return proof

    def _record_all(self, *keys: str, **event_kw: object) -> None:
        for audit_key in keys:
            audit.record(self.journal, audit_key, _event(**event_kw))

    def _tamper_proof(self, mutate) -> None:
        with open(self.proof, encoding="utf-8") as handle:
            proof = json.load(handle)
        mutate(proof)
        self._write_proof(proof)

    def _tamper_checkpoint(self, mutate) -> None:
        with open(self.checkpoint, encoding="utf-8") as handle:
            document = json.load(handle)
        mutate(document)
        self._write(self.checkpoint, _compact(document))

    def _assert_failure(self, result: subprocess.CompletedProcess,
                        status: int, code: str) -> None:
        self.assertEqual(result.returncode, status)
        self.assertEqual(result.stdout, b"")
        self.assertEqual(result.stderr, _compact({"error": code}))


class SuccessTest(_Fixture):
    def test_valid_bundle_reports_compact_result(self) -> None:
        self._record_all("a", "b")
        proof = self._export(limit=1)
        result = self._run_bundle()
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stderr, b"")
        expected = {"valid": True, "etag": self._digest(),
                    "generation": "g1", "closed": False,
                    "events": proof["result"]["events"],
                    "next": proof["result"]["next"]}
        self.assertEqual(result.stdout, _compact(expected))
        self.assertEqual(result.stdout.count(b"\n"), 1)
        payload = json.loads(result.stdout)
        self.assertEqual(list(payload),
                         ["valid", "etag", "generation", "closed",
                          "events", "next"])
        self.assertIs(payload["valid"], True)
        self.assertEqual(payload["next"], "a")

    def test_non_ascii_content_is_written_through_unescaped(self) -> None:
        audit.record(self.journal, "a",
                     _event(error="ValueError", stage="执行"))
        self._export()
        result = self._run_bundle()
        self.assertEqual(result.returncode, 0)
        self.assertIn("执行".encode("utf-8"), result.stdout)
        self.assertNotIn(b"\\u", result.stdout)

    def test_closed_generation_reports_closed_true(self) -> None:
        self._record_all("a")
        self._export(final=True)
        result = self._run_bundle()
        self.assertEqual(result.returncode, 0)
        self.assertIs(json.loads(result.stdout)["closed"], True)

    def test_inputs_are_not_modified(self) -> None:
        self._record_all("a", "b")
        self._export()
        before = {path: Path(path).read_bytes()
                  for path in (self.journal, self.checkpoint, self.proof)}
        result = self._run_bundle()
        self.assertEqual(result.returncode, 0)
        for path, raw in before.items():
            self.assertEqual(Path(path).read_bytes(), raw)


class ArgumentErrorsTest(_Fixture):
    def test_missing_repeated_empty_or_unknown_options_exit_2(self) -> None:
        # The files deliberately do not exist: an argument error must be
        # reported before any input file is read.
        missing = os.path.join(self.tmp.name, "missing.json")
        etag = '"' + "0" * 64 + '"'
        for args in (
            (),
            ("--checkpoint", missing),
            ("--proof", missing),
            ("--etag", etag),
            ("--checkpoint", missing, "--proof", missing),
            ("--checkpoint", missing, "--proof", missing, "--etag", etag,
             "--etag", etag),
            ("--checkpoint", missing, "--checkpoint", missing,
             "--proof", missing, "--etag", etag),
            ("--checkpoint", "", "--proof", missing, "--etag", etag),
            ("--checkpoint", missing, "--proof", "", "--etag", etag),
            ("--checkpoint", missing, "--proof", missing, "--etag", ""),
            ("--checkpoint", missing, "--proof", missing, "--etag", etag,
             "--unknown", "x"),
            ("--checkpoint", missing, "--proof", missing, "--etag", etag,
             "extra"),
        ):
            with self.subTest(args=args):
                self._assert_failure(self._run(*args), 2, "invalid_request")

    def test_etag_must_be_a_single_strong_tag(self) -> None:
        missing = os.path.join(self.tmp.name, "missing.json")
        digest = "0a" * 32
        for etag in (digest, digest.upper(), digest[:-1], digest + "0",
                     "g" * 64, f'W/"{digest}"', f'"{digest}", "{digest}"',
                     f'"{digest}" ', f' "{digest}"', '""'):
            with self.subTest(etag=etag):
                result = self._run("--checkpoint", missing,
                                   "--proof", missing, "--etag", etag)
                self._assert_failure(result, 2, "invalid_request")


class UnavailableTest(_Fixture):
    def test_missing_checkpoint_exits_5(self) -> None:
        result = self._run("--checkpoint",
                           os.path.join(self.tmp.name, "missing.json"),
                           "--proof", self.proof,
                           "--etag", '"' + "0" * 64 + '"')
        self._assert_failure(result, 5, "bundle_unavailable")

    def test_missing_proof_exits_5(self) -> None:
        self._record_all("a")
        self._export()
        os.unlink(self.proof)
        self._assert_failure(self._run_bundle(), 5, "bundle_unavailable")


class CheckpointFormatTest(_Fixture):
    def test_etag_mismatch_fails_before_any_parsing(self) -> None:
        # Not even valid JSON, but the tag check comes first.
        self._write(self.checkpoint, b"not json")
        self._write_proof({})
        result = self._run_bundle(etag='"' + "0" * 64 + '"')
        self._assert_failure(result, 4, "verification_failed")

    def test_checkpoint_encoding_json_and_number_errors_exit_3(self) -> None:
        self._record_all("a")
        self._export()
        for payload in (b"\xff\xfe",
                        b"not json",
                        b'{"version":-0,"generations":[]}',
                        b'{"version":NaN,"generations":[]}',
                        b'{"version":Infinity,"generations":[]}'):
            with self.subTest(payload=payload):
                self._write(self.checkpoint, payload)
                self._assert_failure(self._run_bundle(), 3, "invalid_bundle")

    def test_checkpoint_public_structure_errors_exit_3(self) -> None:
        self._record_all("a")
        self._export()
        for document in (
            {"generations": [], "version": 1},
            {"version": 2, "generations": []},
            {"version": 1, "generations": []},
            {"version": 1, "generations": [{"anchors": [], "name": "g1"}]},
            {"version": 1, "generations": [{"name": "g1", "anchors": []}]},
        ):
            with self.subTest(document=document):
                self._write(self.checkpoint, _compact(document))
                self._assert_failure(self._run_bundle(), 3, "invalid_bundle")

    def test_checkpoint_digest_format_error_exits_3(self) -> None:
        self._record_all("a")
        self._export()

        def mutate(document: dict) -> None:
            document["generations"][0]["anchors"][0]["digest"] = "xyz"

        self._tamper_checkpoint(mutate)
        self._assert_failure(self._run_bundle(), 3, "invalid_bundle")

    def test_checkpoint_anchor_digest_mismatch_exits_4(self) -> None:
        self._record_all("a")
        self._export()

        def mutate(document: dict) -> None:
            anchor = document["generations"][0]["anchors"][0]
            anchor["manifest"][0][1] = "0" * 64

        self._tamper_checkpoint(mutate)
        self._assert_failure(self._run_bundle(), 4, "verification_failed")

    def test_checkpoint_broken_previous_chain_exits_4(self) -> None:
        self._record_all("a")
        self._export()
        self._record_all("b")
        audit_proof.export(self.journal, self.checkpoint, "g1", final=True)

        def mutate(document: dict) -> None:
            anchor = document["generations"][0]["anchors"][1]
            anchor["previous"] = "0" * 64

        self._tamper_checkpoint(mutate)
        self._assert_failure(self._run_bundle(), 4, "verification_failed")


class ProofFormatTest(_Fixture):
    def test_proof_encoding_json_and_number_errors_exit_3(self) -> None:
        self._record_all("a")
        self._export()
        for payload in (b"\xff\xfe",
                        b"not json",
                        b"-0",
                        b'{"version":NaN}',
                        b'{"version":-Infinity}'):
            with self.subTest(payload=payload):
                self._write(self.proof, payload)
                self._assert_failure(self._run_bundle(), 3, "invalid_bundle")

    def test_proof_public_structure_errors_exit_3(self) -> None:
        self._record_all("a")
        proof = self._export()
        for mutated in (
            {key: value for key, value in proof.items() if key != "head"},
            dict(reversed(list(proof.items()))),
            {**proof, "version": 2},
            {**proof, "generation": ""},
            {**proof, "params": {**proof["params"], "limit": 0}},
            {**proof, "params": {**proof["params"], "limit": True}},
            {**proof, "closed": "false"},
            {**proof, "anchor_digest": "XYZ"},
        ):
            with self.subTest(mutated=mutated):
                self._write_proof(mutated)
                self._assert_failure(self._run_bundle(), 3, "invalid_bundle")


class ProofVerificationTest(_Fixture):
    def test_tampered_page_params_log_and_state_exit_4(self) -> None:
        self._record_all("a", "b")
        self._export(limit=1)

        def change_page(proof: dict) -> None:
            proof["result"]["events"][0][1]["changed"] = False

        def change_params(proof: dict) -> None:
            proof["params"]["limit"] = 2

        def change_cursor(proof: dict) -> None:
            proof["result"]["next"] = "b"

        def change_log_bytes(proof: dict) -> None:
            proof["log_bytes"] = proof["log_bytes"].replace(
                '"changed":true', '"changed":false', 1)

        def change_head(proof: dict) -> None:
            proof["head"] = "0" * 64

        def change_closed(proof: dict) -> None:
            proof["closed"] = True

        def change_anchor(proof: dict) -> None:
            proof["anchor_digest"] = "0" * 64

        def change_generation(proof: dict) -> None:
            proof["generation"] = "g2"

        for mutate in (change_page, change_params, change_cursor,
                       change_log_bytes, change_head, change_closed,
                       change_anchor, change_generation):
            with self.subTest(mutate=mutate.__name__):
                self._tamper_proof(mutate)
                self._assert_failure(self._run_bundle(), 4,
                                     "verification_failed")

    def test_proof_from_another_checkpoint_is_rejected(self) -> None:
        self._record_all("a")
        self._export()
        # A foreign checkpoint whose anchor pins a different snapshot:
        # the proof's anchor is not recorded there.
        self._record_all("b")
        other = os.path.join(self.tmp.name, "other-checkpoint.json")
        audit_proof.export(self.journal, other, "g1")
        result = self._run_bundle(
            checkpoint=other,
            etag='"' + hashlib.sha256(open(other, "rb").read())
            .hexdigest() + '"')
        self._assert_failure(result, 4, "verification_failed")


class LibraryTest(_Fixture):
    def test_verify_bundle_returns_the_verified_result(self) -> None:
        self._record_all("a", "b")
        proof = self._export(limit=1)
        result = audit_proof.verify_bundle(self.checkpoint, self.proof,
                                           self._etag())
        self.assertEqual(list(result), ["valid", "etag", "generation",
                                        "closed", "events", "next"])
        self.assertEqual(result,
                         {"valid": True, "etag": self._digest(),
                          "generation": "g1", "closed": False,
                          "events": proof["result"]["events"],
                          "next": "a"})

    def test_argument_errors_raise_before_any_read(self) -> None:
        missing = os.path.join(self.tmp.name, "missing.json")
        for args in ((missing, missing, "0" * 64),
                     ("", missing, '"' + "0" * 64 + '"'),
                     (missing, "", '"' + "0" * 64 + '"'),
                     (missing, missing, '"0" * 64')):
            with self.subTest(args=args):
                with self.assertRaises(ValueError):
                    audit_proof.verify_bundle(*args)

    def test_failures_stay_value_errors_for_existing_callers(self) -> None:
        self._record_all("a")
        self._export()
        with self.assertRaises(ValueError):
            audit_proof.verify_bundle(self.checkpoint, self.proof,
                                      '"' + "0" * 64 + '"')
        with self.assertRaises(FileNotFoundError):
            audit_proof.verify_bundle(
                os.path.join(self.tmp.name, "missing.json"), self.proof,
                '"' + "0" * 64 + '"')


if __name__ == "__main__":
    unittest.main()
