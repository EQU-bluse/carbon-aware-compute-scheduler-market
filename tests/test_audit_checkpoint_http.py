"""HTTP tests for the read-only GET /audit/checkpoint snapshot."""

from __future__ import annotations

import hashlib
import json
import os
import threading
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from tempfile import TemporaryDirectory

from carbon_market import audit, audit_proof
from carbon_market.server import Handler

TOKEN = "s3cret"
FULL_TOKEN = "full-token"
SCOPED_TOKEN = "scoped-token"


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _event(op: str = "copy", key: str = "batch-key",
           error: str | None = None, stage: str = None) -> dict:
    return {"op": op, "target": "t.history", "key": key,
            "changed": True, "error": error, "stage": stage}


class _HttpFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.checkpoint = os.path.join(self.tmp.name, "checkpoint.json")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=self.server.serve_forever,
                                  daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)

    def _request(self, path: str = "/audit/checkpoint",
                 headers: dict[str, str] | None = None,
                 raw_headers: list[tuple[str, str]] | None = None):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=5)
        if raw_headers is not None:
            connection.putrequest("GET", path)
            for name, value in raw_headers:
                connection.putheader(name, value)
            connection.endheaders()
        else:
            connection.request("GET", path, headers=headers or {})
        response = connection.getresponse()
        body = response.read()
        self.addCleanup(connection.close)
        return response.status, response.headers, body

    def _get(self, **kwargs):
        status, _, body = self._request(**kwargs)
        return status, json.loads(body) if body else None

    def _auth_headers(self, token: str = TOKEN) -> list[tuple[str, str]]:
        return [("X-Audit-Token", token)]

    def _record(self, audit_key: str, **event_kwargs) -> None:
        audit.record(self.journal, audit_key, _event(**event_kwargs))

    def _export(self, generation: str = "g1") -> dict:
        return audit_proof.export(self.journal, self.checkpoint, generation)


class CheckpointHttpTest(_HttpFixture):
    def setUp(self) -> None:
        super().setUp()
        self.server.audit_path = self.journal
        self.server.audit_token = TOKEN
        self.server.audit_checkpoint = self.checkpoint

    # -- authorization ----------------------------------------------------

    def test_missing_blank_and_duplicate_token_are_401(self) -> None:
        self.assertEqual(self._get()[0], 401)
        self.assertEqual(
            self._get(headers={"X-Audit-Token": " "})[0], 401)
        status, body = self._get(raw_headers=[
            ("X-Audit-Token", TOKEN), ("X-Audit-Token", TOKEN)])
        self.assertEqual(status, 401)
        self.assertEqual(body, {"error": "unauthorized"})
        # No checkpoint is ever created by the download.
        self.assertFalse(os.path.exists(self.checkpoint))

    def test_wrong_token_is_403_and_never_opens_checkpoint(self) -> None:
        # No checkpoint exists: 403 (not 404) proves it was not opened.
        status, body = self._get(headers={"X-Audit-Token": "nope"})
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "forbidden"})
        self.assertFalse(os.path.exists(self.checkpoint))

    # -- conditional header validation ------------------------------------

    def test_query_string_is_400_and_never_opens_checkpoint(self) -> None:
        for path in ("/audit/checkpoint?x=1",
                     "/audit/checkpoint?cursor=a", "/audit/checkpoint?a="):
            with self.subTest(path=path):
                status, body = self._get(
                    path=path, headers={"X-Audit-Token": TOKEN})
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})
        self.assertFalse(os.path.exists(self.checkpoint))

    def test_malformed_conditional_headers_are_400(self) -> None:
        good = hashlib.sha256(b"x").hexdigest()
        for value in (
            "", " ", "   ",                                   # blank
            f'"{good[:63]}"',                                 # too short
            f'"{good}a"',                                     # too long
            f'"{good.upper()}"',                              # uppercase
            f'"{"g" * 64}"',                                  # not hex
            good,                                             # unquoted
            f"\"{good}\", \"{good}\"",                        # list
            "*",                                              # wildcard
            f'W/"{good}"',                                    # weak tag
            f' {{"{good}"}} ',                                # surrounding space
        ):
            with self.subTest(value=value):
                status, body = self._get(
                    headers={"X-Audit-Token": TOKEN,
                             "If-None-Match": value})
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})
        # A malformed header never opens the checkpoint file.
        self.assertFalse(os.path.exists(self.checkpoint))

    def test_duplicate_conditional_header_is_400(self) -> None:
        tag = f'"{hashlib.sha256(b"x").hexdigest()}"'
        status, body = self._get(raw_headers=[
            ("X-Audit-Token", TOKEN),
            ("If-None-Match", tag), ("If-None-Match", tag)])
        self.assertEqual(status, 400)
        self.assertEqual(body, {"error": "invalid_request"})

    def test_header_validation_follows_auth_and_precedes_scope(self) -> None:
        # Without a token the malformed header is still masked by 401.
        status, _, _ = self._request(
            headers={"If-None-Match": "not-a-tag"})
        self.assertEqual(status, 401)

    # -- successful downloads ---------------------------------------------

    def test_download_returns_original_bytes_content_type_and_etag(self) -> None:
        self._record("a", error="OSError", stage="同步")
        self._export("世代-1")
        raw = open(self.checkpoint, "rb").read()
        status, headers, body = self._request(
            headers={"X-Audit-Token": TOKEN})
        self.assertEqual(status, 200)
        # The exact file bytes: no reserialization, no added newline.
        self.assertEqual(body, raw)
        self.assertEqual(headers.get("Content-Type"), "application/json")
        self.assertEqual(headers.get("Content-Length"), str(len(raw)))
        expected = '"' + hashlib.sha256(raw).hexdigest() + '"'
        self.assertEqual(headers.get("ETag"), expected)
        # The Chinese generation name ships as direct UTF-8, compact,
        # with no trailing newline added by the response.
        self.assertIn("世代-1".encode("utf-8"), body)
        self.assertNotIn(b"\\u", body)
        self.assertFalse(body.endswith(b"\n\n"))
        self.assertNotIn(b": ", body)
        self.assertNotIn(b", ", body)

    def test_pretty_printed_checkpoint_is_served_byte_for_byte(self) -> None:
        self._record("a")
        self._export()
        # Reformat the valid checkpoint with indentation; validation only
        # parses structure, so the download must preserve the exact bytes
        # rather than re-serializing them compactly.
        with open(self.checkpoint, encoding="utf-8") as handle:
            document = json.load(handle)
        pretty = (json.dumps(document, ensure_ascii=False, indent=2)
                  + "\n\n").encode("utf-8")
        with open(self.checkpoint, "wb") as handle:
            handle.write(pretty)
        status, headers, body = self._request(
            headers={"X-Audit-Token": TOKEN})
        self.assertEqual(status, 200)
        self.assertEqual(body, pretty)
        self.assertEqual(headers.get("ETag"),
                         '"' + hashlib.sha256(pretty).hexdigest() + '"')

    def test_matching_etag_is_304_empty_with_same_etag(self) -> None:
        self._record("a")
        self._export()
        raw = open(self.checkpoint, "rb").read()
        tag = '"' + hashlib.sha256(raw).hexdigest() + '"'
        status, headers, body = self._request(
            headers={"X-Audit-Token": TOKEN, "If-None-Match": tag})
        self.assertEqual(status, 304)
        self.assertEqual(body, b"")
        self.assertEqual(headers.get("ETag"), tag)
        self.assertEqual(headers.get("Content-Length"), "0")

    def test_non_matching_etag_is_200_with_full_bytes(self) -> None:
        self._record("a")
        self._export()
        raw = open(self.checkpoint, "rb").read()
        other = '"' + "0" * 64 + '"'
        status, headers, body = self._request(
            headers={"X-Audit-Token": TOKEN, "If-None-Match": other})
        self.assertEqual(status, 200)
        self.assertEqual(body, raw)
        self.assertNotEqual(headers.get("ETag"), other)

    # -- checkpoint state errors ------------------------------------------

    def test_missing_checkpoint_is_404_without_leaking_path(self) -> None:
        status, headers, body = self._request(
            headers={"X-Audit-Token": TOKEN})
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body), {"error": "checkpoint_not_found"})
        self.assertNotIn(self.checkpoint.encode("utf-8"), body)
        self.assertNotIn(self.tmp.name.encode("utf-8"), body)
        self.assertEqual(headers.get("ETag"), None)

    def test_invalid_checkpoints_are_409_with_only_error_body(self) -> None:
        self._record("a")
        self._export()

        def write(raw: bytes) -> None:
            with open(self.checkpoint, "wb") as handle:
                handle.write(raw)

        good = open(self.checkpoint, encoding="utf-8").read()
        document = json.loads(good)
        variants = [
            b"\xff\xfe is not utf-8",
            b"{not json",
            json.dumps({"version": 2,
                        "generations": document["generations"]},
                       ensure_ascii=False).encode("utf-8"),     # bad version
            json.dumps({"generations": document["generations"],
                        "version": 1}, ensure_ascii=False,
                       separators=(",", ":")).encode("utf-8"),  # field order
        ]
        tampered = document
        tampered["generations"][-1]["anchors"][-1]["digest"] = "0" * 64
        variants.append(json.dumps(tampered, ensure_ascii=False,
                                   separators=(",", ":")).encode("utf-8"))
        for raw in variants:
            with self.subTest(raw=raw[:20]):
                write(raw)
                status, _, body = self._request(
                    headers={"X-Audit-Token": TOKEN})
                self.assertEqual(status, 409)
                self.assertEqual(json.loads(body),
                                 {"error": "checkpoint_invalid"})
                self.assertEqual(list(json.loads(body)), ["error"])
                self.assertNotIn(self.tmp.name.encode("utf-8"), body)

    def test_io_failure_is_503_checkpoint_unavailable(self) -> None:
        # A directory opens with an OSError that is not FileNotFoundError.
        self.server.audit_checkpoint = self.tmp.name
        status, _, body = self._request(
            headers={"X-Audit-Token": TOKEN})
        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body),
                         {"error": "checkpoint_unavailable"})

    # -- JSON encoding (shared with the proof endpoint) -------------------

    def test_proof_success_emits_direct_utf8_compact_without_newline(self) -> None:
        self._record("a", error="OSError", stage="同步")
        status, headers, body = self._request(
            path="/audit/proof?generation=g1",
            headers={"X-Audit-Token": TOKEN})
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("Content-Type"), "application/json")
        # The embedded journal and page carry the Chinese stage as direct
        # UTF-8; the response itself is compact and has no trailing newline.
        self.assertIn("同步".encode("utf-8"), body)
        self.assertNotIn(b"\\u", body)
        self.assertFalse(body.endswith(b"\n"))
        self.assertNotIn(b": ", body)
        self.assertNotIn(b", ", body)

    # -- concurrency ------------------------------------------------------

    def test_downloads_racing_exports_see_only_complete_versions(self) -> None:
        for index in range(5):
            self._record(f"k{index}", error="OSError", stage="校验")
        self._export("g1")
        results: list[tuple[int, bytes, str | None]] = []

        def downloader() -> None:
            status, headers, body = self._request(
                headers={"X-Audit-Token": TOKEN})
            results.append((status, body, headers.get("ETag")))

        def exporter(index: int) -> None:
            self._record(f"new{index}")
            audit_proof.export(self.journal, self.checkpoint, "g1")

        threads = [threading.Thread(target=downloader) for _ in range(8)]
        threads += [threading.Thread(target=exporter, args=(i,))
                    for i in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertTrue(results)
        for status, body, etag in results:
            self.assertEqual(status, 200)
            self.assertIsNotNone(etag)
            # The ETag summarizes exactly the bytes in hand.
            self.assertEqual(etag,
                             '"' + hashlib.sha256(body).hexdigest() + '"')
            # The bytes are a complete validated checkpoint, never torn.
            audit_proof._validate_checkpoint(json.loads(body))

    # -- unchanged surface ------------------------------------------------

    def test_other_paths_unchanged(self) -> None:
        status, body = self._get(path="/health")
        self.assertEqual((status, body), (200, {"status": "ok"}))
        status, body = self._get(path="/missing",
                                 headers={"X-Audit-Token": TOKEN})
        self.assertEqual((status, body), (404, {"error": "not_found"}))
        status, body = self._get(path="/audit/checkpoint/",
                                 headers={"X-Audit-Token": TOKEN})
        self.assertEqual((status, body), (404, {"error": "not_found"}))


class CheckpointDisabledTest(_HttpFixture):
    def test_path_is_plain_404_without_checkpoint(self) -> None:
        self.server.audit_path = self.journal
        self.server.audit_token = TOKEN
        status, _, body = self._request(
            headers={"X-Audit-Token": TOKEN})
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body), {"error": "not_found"})


class CheckpointAuthHttpTest(_HttpFixture):
    def setUp(self) -> None:
        super().setUp()
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        self._write_config(
            self._line("full", FULL_TOKEN, None) + "\n"
            + self._line("scoped", SCOPED_TOKEN, None,
                         ops=["copy"], stages=["成功"],
                         keys=["hist-1"]) + "\n")
        self.server.audit_path = self.journal
        self.server.audit_token = None
        self.server.audit_auth = self.config
        self.server.audit_checkpoint = self.checkpoint

    @staticmethod
    def _line(name: str, token: str, grace_until,
              ops="*", stages="*", keys="*") -> str:
        return json.dumps([name, _digest(token), grace_until, ops, stages,
                           keys], ensure_ascii=False)

    def _write_config(self, text: str) -> None:
        tmp_path = self.config + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp_path, self.config)

    def test_all_star_record_may_download(self) -> None:
        self._record("a")
        self._export()
        raw = open(self.checkpoint, "rb").read()
        status, headers, body = self._request(
            headers={"X-Audit-Token": FULL_TOKEN})
        self.assertEqual(status, 200)
        self.assertEqual(body, raw)
        self.assertEqual(headers.get("ETag"),
                         '"' + hashlib.sha256(raw).hexdigest() + '"')

    def test_any_restricted_scope_is_403_without_opening_file(self) -> None:
        # No checkpoint yet: 403 (not 404) proves it stayed closed.
        status, body = self._get(
            headers={"X-Audit-Token": SCOPED_TOKEN})
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "forbidden"})
        self.assertFalse(os.path.exists(self.checkpoint))

    def test_restricted_scope_with_each_single_axis_is_403(self) -> None:
        for line in (
            self._line("ops", SCOPED_TOKEN, None, ops=["copy"]),
            self._line("stages", SCOPED_TOKEN, None, stages=["成功"]),
            self._line("keys", SCOPED_TOKEN, None, keys=["k"]),
        ):
            with self.subTest(line=line):
                self._write_config(line + "\n")
                status, body = self._get(
                    headers={"X-Audit-Token": SCOPED_TOKEN})
                self.assertEqual(status, 403)
                self.assertEqual(body, {"error": "forbidden"})

    def test_malformed_header_is_400_before_scope_check(self) -> None:
        status, body = self._get(headers={
            "X-Audit-Token": SCOPED_TOKEN, "If-None-Match": "bad"})
        self.assertEqual(status, 400)
        self.assertEqual(body, {"error": "invalid_request"})

    def test_expired_token_is_403(self) -> None:
        self._write_config(
            self._line("full", FULL_TOKEN, 1) + "\n")
        status, body = self._get(
            headers={"X-Audit-Token": FULL_TOKEN})
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "forbidden"})

    def test_unreadable_or_invalid_config_is_503(self) -> None:
        os.unlink(self.config)
        status, body = self._get(headers={"X-Audit-Token": FULL_TOKEN})
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "auth_unavailable"})
        self._write_config("{not json\n")
        status, _, response_body = self._request(
            headers={"X-Audit-Token": FULL_TOKEN})
        self.assertEqual(status, 503)
        self.assertEqual(json.loads(response_body),
                         {"error": "auth_unavailable"})
        self.assertNotIn(b"json", response_body)


class ReadSnapshotTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.checkpoint = os.path.join(self.tmp.name, "checkpoint.json")

    def _record(self, audit_key: str) -> None:
        audit.record(self.journal, audit_key, _event())

    def test_returns_validated_bytes_and_their_digest(self) -> None:
        self._record("a")
        audit_proof.export(self.journal, self.checkpoint, "g1")
        raw = open(self.checkpoint, "rb").read()
        body, etag = audit_proof.read_snapshot(self.checkpoint)
        self.assertEqual(body, raw)
        self.assertEqual(etag, hashlib.sha256(raw).hexdigest())

    def test_missing_file_is_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            audit_proof.read_snapshot(self.checkpoint)

    def test_bad_bytes_are_value_error(self) -> None:
        with open(self.checkpoint, "wb") as handle:
            handle.write(b"{not json")
        with self.assertRaises(ValueError):
            audit_proof.read_snapshot(self.checkpoint)

    def test_bad_argument_is_value_error_before_open(self) -> None:
        with self.assertRaises(ValueError):
            audit_proof.read_snapshot("")


if __name__ == "__main__":
    unittest.main()
