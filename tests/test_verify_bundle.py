"""Offline verification of a downloaded checkpoint/proof pair.

Covers the ``verify-bundle`` command and the underlying
``audit_proof.verify_bundle``: the exactly-once non-empty argument
contract and the strong-tag format (usage errors that never read the
input files), the tag recomputation from the raw checkpoint bytes
before any parsing, the UTF-8/strict-JSON format checks of both files,
the full anchor-chain and proof cross-checks against one locked
snapshot, the compact single-line success and failure objects, and the
exit-status/error-code mapping (2 invalid_request, 3 invalid_bundle,
4 verification_failed, 5 bundle_unavailable).
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

    def _write(self, path: str, raw: bytes) -> str:
        with open(path, "wb") as handle:
            handle.write(raw)
        return '"' + hashlib.sha256(raw).hexdigest() + '"'

    def _run(self, *args: str) -> tuple[int, str, str]:
        return _run_cli(*args)

    def _run_bundle(self, checkpoint: str | None = None,
                    proof: str | None = None,
                    etag: str | None = None) -> tuple[int, str, str]:
        return self._run(
            "--checkpoint", checkpoint or self.checkpoint,
            "--proof", proof or self.proof_path,
            "--etag", etag if etag is not None else self._etag())


class SuccessTest(_Fixture):
    def test_valid_bundle_reports_verified_fields(self) -> None:
        self._record("b", error="OSError", stage="同步")
        self._record("a")
        self._record("c", error="OSError", stage="校验")
        self._export("世代-1")
        status, stdout, stderr = self._run_bundle()
        self.assertEqual(status, 0)
        self.assertEqual(stderr, "")
        result = json.loads(stdout)
        self.assertEqual(list(result),
                         ["valid", "etag", "generation", "closed",
                          "events", "next"])
        self.assertIs(result["valid"], True)
        self.assertEqual(result["etag"], self._etag())
        self.assertEqual(result["generation"], "世代-1")
        self.assertIs(result["closed"], False)
        self.assertEqual([key for key, _ in result["events"]],
                         ["a", "b", "c"])
        self.assertIsNone(result["next"])
        # Compact UTF-8 JSON: Chinese written through, exactly one
        # trailing newline.
        self.assertIn("世代-1", stdout)
        self.assertIn("同步", stdout)
        self.assertNotIn("\\u", stdout)
        self.assertNotIn(": ", stdout)
        self.assertNotIn(", ", stdout)
        self.assertTrue(stdout.endswith("\n"))
        self.assertFalse(stdout.endswith("\n\n"))

    def test_page_and_cursor_come_from_the_verified_proof(self) -> None:
        for index in range(5):
            self._record(f"k{index}")
        proof = self._export(limit=2)
        status, stdout, stderr = self._run_bundle()
        self.assertEqual((status, stderr), (0, ""))
        result = json.loads(stdout)
        self.assertEqual(result["events"], proof["result"]["events"])
        self.assertEqual(result["next"], "k1")
        # The following page verifies against the same checkpoint.
        proof2 = audit_proof.export(self.journal, self.checkpoint, "g1",
                                    cursor="k1", limit=2)
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof2, ensure_ascii=False))
        status, stdout, _ = self._run_bundle()
        self.assertEqual(status, 0)
        result = json.loads(stdout)
        self.assertEqual([key for key, _ in result["events"]],
                         ["k2", "k3"])
        self.assertEqual(result["next"], "k3")

    def test_closed_generation_reports_closed_true(self) -> None:
        self._record("a")
        self._export("g1", final=True)
        status, stdout, _ = self._run_bundle()
        self.assertEqual(status, 0)
        result = json.loads(stdout)
        self.assertIs(result["closed"], True)

    def test_inputs_are_never_rewritten(self) -> None:
        self._record("a")
        self._export()
        before_checkpoint = open(self.checkpoint, "rb").read()
        before_proof = open(self.proof_path, "rb").read()
        self.assertEqual(self._run_bundle()[0], 0)
        self.assertEqual(open(self.checkpoint, "rb").read(),
                         before_checkpoint)
        self.assertEqual(open(self.proof_path, "rb").read(), before_proof)


class ArgumentTest(_Fixture):
    def test_missing_options_are_invalid_request(self) -> None:
        self._record("a")
        self._export()
        good = ["--checkpoint", self.checkpoint,
                "--proof", self.proof_path,
                "--etag", self._etag()]
        for omit in (0, 2, 4):
            args = good[:omit] + good[omit + 2:]
            with self.subTest(omit=good[omit]):
                status, stdout, stderr = self._run(*args)
                self.assertEqual(status, 2)
                self.assertEqual(stdout, "")
                self.assertEqual(json.loads(stderr),
                                 {"error": "invalid_request"})

    def test_repeated_empty_and_unknown_arguments_are_invalid_request(
            self) -> None:
        self._record("a")
        self._export()
        etag = self._etag()
        variants = [
            ["--checkpoint", self.checkpoint, "--checkpoint",
             self.checkpoint, "--proof", self.proof_path, "--etag", etag],
            ["--checkpoint", "", "--proof", self.proof_path,
             "--etag", etag],
            ["--checkpoint", self.checkpoint, "--proof", "",
             "--etag", etag],
            ["--checkpoint", self.checkpoint, "--proof", self.proof_path,
             "--etag", ""],
            ["--checkpoint", self.checkpoint, "--proof", self.proof_path,
             "--etag", etag, "--extra", "x"],
            ["--checkpoint", self.checkpoint, "--proof", self.proof_path,
             "--etag", etag, "positional"],
            ["--checkpoint"],  # option without a value
        ]
        for args in variants:
            with self.subTest(args=args):
                status, stdout, stderr = self._run(*args)
                self.assertEqual(status, 2)
                self.assertEqual(stdout, "")
                self.assertEqual(json.loads(stderr),
                                 {"error": "invalid_request"})

    def test_malformed_etags_are_invalid_request(self) -> None:
        self._record("a")
        self._export()
        good = hashlib.sha256(b"x").hexdigest()
        for etag in (
            f'"{good[:63]}"',                       # too short
            f'"{good}a"',                           # too long
            f'"{good.upper()}"',                    # uppercase
            f'"{"g" * 64}"',                        # not hexadecimal
            good,                                   # unquoted
            f'"{good}", "{good}"',                  # list
            "*",                                    # wildcard
            f'W/"{good}"',                          # weak tag
            f' "{good}"',                           # surrounding space
        ):
            with self.subTest(etag=etag):
                status, stdout, stderr = self._run_bundle(etag=etag)
                self.assertEqual(status, 2)
                self.assertEqual(stdout, "")
                self.assertEqual(json.loads(stderr),
                                 {"error": "invalid_request"})

    def test_argument_errors_never_read_the_input_files(self) -> None:
        # Neither path exists; an argument error must win over any file
        # access, so the outcome is 2, never 5.
        missing = os.path.join(self.tmp.name, "missing.json")
        for args in (
            ["--checkpoint", missing, "--proof", missing],
            ["--checkpoint", missing, "--proof", missing, "--etag", ""],
            ["--checkpoint", missing, "--proof", missing,
             "--etag", "not-a-tag"],
            ["--checkpoint", missing, "--proof", missing,
             "--etag", '"0" * 64"'],
        ):
            with self.subTest(args=args):
                status, _, stderr = self._run(*args)
                self.assertEqual(status, 2)
                self.assertEqual(json.loads(stderr),
                                 {"error": "invalid_request"})

    def test_library_rejects_bad_arguments_before_reading(self) -> None:
        missing = os.path.join(self.tmp.name, "missing.json")
        for args in (("", "p", '"0"'), ("c", "", '"0"'), ("c", "p", ""),
                     ("c", "p", "unquoted"), (missing, missing, "x")):
            with self.subTest(args=args):
                with self.assertRaises(ValueError):
                    audit_proof.verify_bundle(*args)


class UnavailableTest(_Fixture):
    def test_missing_files_are_bundle_unavailable(self) -> None:
        self._record("a")
        self._export()
        missing = os.path.join(self.tmp.name, "missing.json")
        for checkpoint, proof in ((missing, self.proof_path),
                                  (self.checkpoint, missing),
                                  (missing, missing)):
            with self.subTest(checkpoint=checkpoint, proof=proof):
                status, stdout, stderr = self._run_bundle(
                    checkpoint=checkpoint, proof=proof)
                self.assertEqual(status, 5)
                self.assertEqual(stdout, "")
                self.assertEqual(json.loads(stderr),
                                 {"error": "bundle_unavailable"})

    def test_other_io_errors_are_bundle_unavailable(self) -> None:
        self._record("a")
        self._export()
        # A directory opens with an OSError that is not
        # FileNotFoundError.
        status, stdout, stderr = self._run_bundle(checkpoint=self.tmp.name)
        self.assertEqual(status, 5)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr),
                         {"error": "bundle_unavailable"})

    def test_failure_object_never_leaks_paths_or_content(self) -> None:
        status, stdout, stderr = self._run_bundle(etag='"' + "0" * 64 + '"')
        self.assertEqual(status, 5)
        self.assertNotIn(self.tmp.name, stderr)
        self.assertNotIn("checkpoint", stderr)
        self.assertEqual(stderr.strip(), '{"error":"bundle_unavailable"}')


class InvalidBundleTest(_Fixture):
    def test_checkpoint_format_errors_are_invalid_bundle(self) -> None:
        self._record("a")
        proof = self._export()
        good = open(self.checkpoint, "rb").read()
        document = json.loads(good)
        variants = [
            b"\xff\xfe is not utf-8",
            b"{not json",
            b'"version":1,"generations":[]}',  # not an object
            good.replace(b'"version":1', b'"version":-0', 1),
            good.replace(b'"version":1', b'"version":NaN', 1),
            good.replace(b'"version":1', b'"version":Infinity', 1),
            good.replace(b'"version":1', b'"version":2', 1),
            json.dumps({"generations": document["generations"],
                        "version": 1}, ensure_ascii=False,
                       separators=(",", ":")).encode("utf-8"),
        ]
        for raw in variants:
            with self.subTest(raw=raw[:24]):
                etag = self._write(self.checkpoint, raw)
                status, stdout, stderr = self._run_bundle(etag=etag)
                self.assertEqual(status, 3)
                self.assertEqual(stdout, "")
                self.assertEqual(json.loads(stderr),
                                 {"error": "invalid_bundle"})

    def test_proof_format_errors_are_invalid_bundle(self) -> None:
        self._record("a")
        proof = self._export()
        good = json.dumps(proof, ensure_ascii=False,
                          separators=(",", ":"))
        incomplete = dict(proof)
        del incomplete["closed"]
        variants = [
            b"\xff\xfe",
            b"{not json",
            good.replace('"limit":100', '"limit":-0', 1).encode("utf-8"),
            good.replace('"limit":100', '"limit":NaN', 1).encode("utf-8"),
            json.dumps(incomplete, ensure_ascii=False).encode("utf-8"),
            good.replace('"limit":100', '"limit":0', 1).encode("utf-8"),
        ]
        for raw in variants:
            with self.subTest(raw=raw[:24]):
                with open(self.proof_path, "wb") as handle:
                    handle.write(raw)
                status, stdout, stderr = self._run_bundle()
                self.assertEqual(status, 3)
                self.assertEqual(stdout, "")
                self.assertEqual(json.loads(stderr),
                                 {"error": "invalid_bundle"})


class VerificationFailedTest(_Fixture):
    def test_etag_mismatch_fails_before_anything_else(self) -> None:
        self._record("a")
        self._export()
        other = '"' + "0" * 64 + '"'
        status, stdout, stderr = self._run_bundle(etag=other)
        self.assertEqual(status, 4)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})
        # Even a checkpoint that is not valid JSON at all fails with the
        # tag mismatch first: the tag is recomputed from the raw bytes
        # before any parsing.
        raw = b"{not json"
        self._write(self.checkpoint, raw)
        status, _, stderr = self._run_bundle(etag=other)
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})

    def test_checkpoint_chain_deviations_are_verification_failed(
            self) -> None:
        self._record("a")
        self._record("b")
        self._export("g1")
        audit_proof.export(self.journal, self.checkpoint, "g1", final=True)
        document = json.loads(open(self.checkpoint, encoding="utf-8")
                              .read())
        variants = []
        tampered = json.loads(json.dumps(document))
        tampered["generations"][-1]["anchors"][-1]["digest"] = "0" * 64
        variants.append(tampered)
        tampered = json.loads(json.dumps(document))
        tampered["generations"][-1]["anchors"][-1]["previous"] = "0" * 64
        variants.append(tampered)
        tampered = json.loads(json.dumps(document))
        tampered["generations"][-1]["anchors"][0]["log_digest"] = "0" * 64
        variants.append(tampered)
        for variant in variants:
            with self.subTest(variant=len(variants)):
                raw = json.dumps(variant, ensure_ascii=False,
                                 separators=(",", ":")).encode("utf-8")
                etag = self._write(self.checkpoint, raw)
                status, stdout, stderr = self._run_bundle(etag=etag)
                self.assertEqual(status, 4)
                self.assertEqual(stdout, "")
                self.assertEqual(json.loads(stderr),
                                 {"error": "verification_failed"})

    def test_proof_mismatches_are_verification_failed(self) -> None:
        self._record("a", error="OSError", stage="同步")
        self._record("b")
        proof = self._export("g1")

        def mutated(**changes) -> None:
            document = json.loads(json.dumps(proof))
            document.update(changes)
            with open(self.proof_path, "w", encoding="utf-8") as handle:
                handle.write(json.dumps(document, ensure_ascii=False))

        variants = []
        result = json.loads(json.dumps(proof["result"]))
        result["events"] = result["events"][:1]
        variants.append({"result": result})                    # page
        result = json.loads(json.dumps(proof["result"]))
        result["next"] = "a"
        variants.append({"result": result})                    # cursor
        variants.append({"params": {"cursor": None, "limit": 1,
                                    "op": None, "stage": None,
                                    "key": None}})             # query
        variants.append({"generation": "g2"})                  # generation
        variants.append({"anchor_digest": "0" * 64})           # anchor
        variants.append({"closed": True})                      # state
        variants.append({"log_bytes": proof["log_bytes"] + "\n"})
        variants.append({"head": "0" * 64})
        for changes in variants:
            with self.subTest(changes=sorted(changes)):
                mutated(**changes)
                status, stdout, stderr = self._run_bundle()
                self.assertEqual(status, 4)
                self.assertEqual(stdout, "")
                self.assertEqual(json.loads(stderr),
                                 {"error": "verification_failed"})

    def test_stale_etag_after_reexport_is_verification_failed(self) -> None:
        self._record("a")
        self._export("g1")
        stale = self._etag()
        self._record("b")
        proof2 = audit_proof.export(self.journal, self.checkpoint, "g1")
        status, _, stderr = self._run_bundle(etag=stale)
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})
        # The old proof is bound to the old snapshot: even with the new
        # tag its checkpoint_etag disagrees with the checkpoint bytes.
        status, _, stderr = self._run_bundle()
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})
        # The proof exported from the current snapshot verifies.
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof2, ensure_ascii=False))
        status, stdout, _ = self._run_bundle()
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(stdout)["etag"], self._etag())

    def test_old_checkpoint_with_new_proof_is_verification_failed(
            self) -> None:
        self._record("a")
        self._export("g1")
        old_bytes = open(self.checkpoint, "rb").read()
        self._record("b")
        proof2 = audit_proof.export(self.journal, self.checkpoint, "g1")
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof2, ensure_ascii=False))
        # Roll the checkpoint file back to the previous snapshot: the
        # proof's bound tag names the newer bytes.
        etag = self._write(self.checkpoint, old_bytes)
        status, _, stderr = self._run_bundle(etag=etag)
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})

    def test_tag_from_another_download_is_verification_failed(self) -> None:
        self._record("a")
        self._export("g1")
        # A tag computed over a different checkpoint response -- even a
        # well-formed strong tag -- is not this snapshot's tag.
        other = os.path.join(self.tmp.name, "other.json")
        audit_proof.export(self.journal, other, "g1", final=True)
        foreign = self._etag(other)
        status, _, stderr = self._run_bundle(etag=foreign)
        self.assertEqual(status, 4)
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})

    def test_version_one_proof_is_invalid_bundle(self) -> None:
        self._record("a")
        proof = self._export("g1")
        # A version 1 proof carries no snapshot binding: it is an
        # incomplete proof structure, not a mismatch.
        del proof["checkpoint_etag"]
        proof["version"] = 1
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False))
        status, stdout, stderr = self._run_bundle()
        self.assertEqual(status, 3)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr), {"error": "invalid_bundle"})

    def test_tampered_checkpoint_etag_is_verification_failed(self) -> None:
        self._record("a")
        proof = self._export("g1")
        proof["checkpoint_etag"] = '"' + "0" * 64 + '"'
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False))
        status, stdout, stderr = self._run_bundle()
        self.assertEqual(status, 4)
        self.assertEqual(stdout, "")
        self.assertEqual(json.loads(stderr),
                         {"error": "verification_failed"})


class ConcurrencyTest(_Fixture):
    def test_racing_exports_yield_only_self_consistent_outcomes(
            self) -> None:
        for index in range(5):
            self._record(f"k{index}")
        self._export("g1")
        outcomes: list[str] = []
        failures: list[BaseException] = []

        def exporter(index: int) -> None:
            try:
                self._record(f"new{index}")
                audit_proof.export(self.journal, self.checkpoint, "g1")
            except audit_proof.BundleMismatchError:
                # A record that lands before an already anchored key
                # rewrites that key's sealed digest; the racing export
                # is then rejected and appends nothing -- itself a
                # self-consistent outcome.
                pass
            except BaseException as exc:  # pragma: no cover
                failures.append(exc)

        def verifier() -> None:
            try:
                for _ in range(10):
                    try:
                        result = audit_proof.verify_bundle(
                            self.checkpoint, self.proof_path, self._etag())
                    except audit_proof.BundleMismatchError:
                        # The tag was computed from a version the locked
                        # read no longer saw: a complete old or new
                        # combination, never a torn one.
                        outcomes.append("mismatch")
                    else:
                        self.assertEqual(result["generation"], "g1")
                        outcomes.append("valid")
            except BaseException as exc:
                failures.append(exc)

        threads = [threading.Thread(target=verifier) for _ in range(4)]
        threads += [threading.Thread(target=exporter, args=(i,))
                    for i in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(failures, [])
        self.assertTrue(outcomes)
        self.assertIn("valid", outcomes)


if __name__ == "__main__":
    unittest.main()
