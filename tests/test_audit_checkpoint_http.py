"""HTTP tests for the read-only GET /audit/checkpoint download."""

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
OPS_TOKEN = "ops-token"


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _event(op: str = "copy", key: str = "batch-key",
           error: str | None = None, stage: str | None = None) -> dict:
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

    def _get(self, path: str, headers: dict[str, str] | None = None,
             raw_headers: list[tuple[str, str]] | None = None):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        if raw_headers is not None:
            connection.putrequest("GET", path)
            for name, value in raw_headers:
                connection.putheader(name, value)
            connection.endheaders()
        else:
            connection.request("GET", path, headers=headers or {})
        response = connection.getresponse()
        body = response.read()
        etag = response.getheader("ETag")
        self.addCleanup(connection.close)
        return response.status, body, etag

    def _auth_get(self, path: str, token: str = TOKEN, **kwargs):
        headers = dict(kwargs.pop("headers", {}))
        headers["X-Audit-Token"] = token
        return self._get(path, headers=headers, **kwargs)

    def _record(self, audit_key: str, **event_kwargs) -> None:
        audit.record(self.journal, audit_key, _event(**event_kwargs))

    def _export(self, generation: str = "g1", final: bool = False) -> None:
        audit_proof.export(self.journal, self.checkpoint, generation,
                           final=final)

    def _checkpoint_bytes(self) -> bytes:
        with open(self.checkpoint, "rb") as handle:
            return handle.read()


class CheckpointHttpTest(_HttpFixture):
    def setUp(self) -> None:
        super().setUp()
        self.server.audit_path = self.journal
        self.server.audit_token = TOKEN
        self.server.audit_checkpoint = self.checkpoint

    # -- authorization ----------------------------------------------------

    def test_missing_blank_and_duplicate_token_are_401(self) -> None:
        self.assertEqual(self._get("/audit/checkpoint")[0], 401)
        self.assertEqual(
            self._get("/audit/checkpoint",
                      headers={"X-Audit-Token": "  "})[0], 401)
        status, body, _ = self._get(
            "/audit/checkpoint",
            raw_headers=[("X-Audit-Token", TOKEN),
                         ("X-Audit-Token", TOKEN)])
        self.assertEqual(status, 401)
        self.assertEqual(json.loads(body), {"error": "unauthorized"})

    def test_wrong_token_is_403_and_never_touches_files(self) -> None:
        status, body, _ = self._get("/audit/checkpoint",
                                    headers={"X-Audit-Token": "nope"})
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(body), {"error": "forbidden"})

    def test_auth_precedes_header_validation(self) -> None:
        self.assertEqual(
            self._get("/audit/checkpoint",
                      headers={"If-None-Match": "bogus"})[0], 401)
        self.assertEqual(
            self._get("/audit/checkpoint",
                      headers={"X-Audit-Token": "nope",
                               "If-None-Match": "bogus"})[0], 403)

    # -- query and conditional header validation ---------------------------

    def test_any_query_parameter_is_400(self) -> None:
        for path in ("/audit/checkpoint?generation=g1",
                     "/audit/checkpoint?checkpoint=other.json",
                     "/audit/checkpoint?path=other.json",
                     "/audit/checkpoint?x=1"):
            with self.subTest(path=path):
                status, body, _ = self._auth_get(path)
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(body),
                                 {"error": "invalid_request"})

    def test_malformed_if_none_match_is_400_and_never_opens_file(self) -> None:
        digest = "a" * 64
        # No checkpoint exists: a 400 (not 404) proves it was not opened.
        for value in ("",                 # missing value
                      "   ",              # blank value
                      digest,             # unquoted
                      f'"{digest[:-1]}"',        # too short
                      f'"{digest}x"',            # too long
                      f'"{digest.upper()}"',     # uppercase
                      f'"g{digest[1:]}"',        # not hexadecimal
                      f'W/"{digest}"',           # weak tag
                      f'"{digest}", "{digest}"',  # a list, not one value
                      ):
            with self.subTest(value=value):
                status, body, _ = self._get(
                    "/audit/checkpoint",
                    raw_headers=[("X-Audit-Token", TOKEN),
                                 ("If-None-Match", value)])
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(body),
                                 {"error": "invalid_request"})
        # A repeated header is invalid even when both values are legal.
        status, body, _ = self._get(
            "/audit/checkpoint",
            raw_headers=[("X-Audit-Token", TOKEN),
                         ("If-None-Match", f'"{digest}"'),
                         ("If-None-Match", f'"{digest}"')])
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body), {"error": "invalid_request"})

    # -- successful downloads ------------------------------------------------

    def test_download_returns_raw_bytes_and_strong_etag(self) -> None:
        self._record("a", key="hist-1")
        self._export()
        expected = self._checkpoint_bytes()
        status, body, etag = self._auth_get("/audit/checkpoint")
        self.assertEqual(status, 200)
        self.assertEqual(body, expected)
        self.assertEqual(
            etag, f'"{hashlib.sha256(expected).hexdigest()}"')
        # The bytes are the file's own: field order and serialization
        # are exactly what the export committed.
        self.assertTrue(body.endswith(b"\n"))
        self.assertEqual(json.loads(body)["version"], 1)

    def test_conditional_match_is_304_mismatch_is_200(self) -> None:
        self._record("a")
        self._export()
        _, _, etag = self._auth_get("/audit/checkpoint")
        status, body, again = self._auth_get(
            "/audit/checkpoint", headers={"If-None-Match": etag})
        self.assertEqual(status, 304)
        self.assertEqual(body, b"")
        self.assertEqual(again, etag)
        # A valid but different digest gets the full snapshot.
        other = '"%s"' % hashlib.sha256(b"other").hexdigest()
        status, body, again = self._auth_get(
            "/audit/checkpoint", headers={"If-None-Match": other})
        self.assertEqual(status, 200)
        self.assertEqual(body, self._checkpoint_bytes())
        self.assertEqual(again, etag)

    def test_etag_tracks_new_checkpoint_versions(self) -> None:
        self._record("a")
        self._export()
        _, _, first = self._auth_get("/audit/checkpoint")
        self._record("b")
        self._export()
        status, body, second = self._auth_get("/audit/checkpoint")
        self.assertEqual(status, 200)
        self.assertNotEqual(first, second)
        # The old digest no longer matches: a conditional request with
        # it downloads the complete new snapshot.
        status, body, etag = self._auth_get(
            "/audit/checkpoint", headers={"If-None-Match": first})
        self.assertEqual(status, 200)
        self.assertEqual(body, self._checkpoint_bytes())
        self.assertEqual(etag, second)

    def test_concurrent_downloads_observe_complete_checkpoints(self) -> None:
        for index in range(10):
            self._record(f"k{index}")
        self._export()
        # Every committed version is a complete observation; a download
        # racing the exports must return one of them, never a mixture.
        versions = [self._checkpoint_bytes()]
        results: list[tuple[int, bytes]] = []
        stop = threading.Event()

        def downloader() -> None:
            while not stop.is_set():
                status, body, _ = self._auth_get("/audit/checkpoint")
                results.append((status, body))

        threads = [threading.Thread(target=downloader) for _ in range(4)]
        for thread in threads:
            thread.start()
        for index in range(5):
            self._record(f"new-{index}")
            self._export()
            versions.append(self._checkpoint_bytes())
        stop.set()
        for thread in threads:
            thread.join()
        self.assertTrue(results)
        for status, body in results:
            self.assertEqual(status, 200)
            self.assertIn(body, versions)

    # -- checkpoint state errors ---------------------------------------------

    def test_missing_checkpoint_is_404_without_leaking_path(self) -> None:
        status, body, _ = self._auth_get("/audit/checkpoint")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body),
                         {"error": "checkpoint_not_found"})
        self.assertNotIn(self.tmp.name, body.decode("utf-8"))

    def test_invalid_checkpoint_is_409(self) -> None:
        self._record("a")
        self._export()
        for payload in (b"\xff\xfe not utf-8",
                        b"{not json",
                        b'{"version":2,"generations":[]}',
                        # Field order is part of the format.
                        b'{"generations":[],"version":1}'):
            with self.subTest(payload=payload[:20]):
                with open(self.checkpoint, "wb") as handle:
                    handle.write(payload)
                status, body, _ = self._auth_get("/audit/checkpoint")
                self.assertEqual(status, 409)
                self.assertEqual(json.loads(body),
                                 {"error": "checkpoint_invalid"})
                self.assertEqual(list(json.loads(body)), ["error"])

    def test_broken_anchor_chain_is_409(self) -> None:
        self._record("a")
        self._export()
        document = json.loads(self._checkpoint_bytes())
        document["generations"][0]["anchors"][0]["log_digest"] = "0" * 64
        with open(self.checkpoint, "w", encoding="utf-8") as handle:
            json.dump(document, handle)
        status, body, _ = self._auth_get("/audit/checkpoint")
        self.assertEqual(status, 409)
        self.assertEqual(json.loads(body), {"error": "checkpoint_invalid"})

    def test_checkpoint_io_failure_is_503(self) -> None:
        self.server.audit_checkpoint = self.tmp.name  # a directory
        status, body, _ = self._auth_get("/audit/checkpoint")
        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body),
                         {"error": "checkpoint_unavailable"})

    # -- unchanged surface ----------------------------------------------------

    def test_health_audit_proof_and_unknown_paths_unchanged(self) -> None:
        self.assertEqual(self._get("/health")[0], 200)
        self.assertEqual(self._get("/missing")[0], 404)
        self.assertEqual(self._get("/audit/checkpoint/")[0], 404)
        self._record("a")
        status, body, _ = self._auth_get("/audit")
        self.assertEqual(status, 200)
        self.assertEqual([key for key, _ in json.loads(body)["events"]],
                         ["a"])
        status, body, _ = self._auth_get("/audit/proof?generation=g1")
        self.assertEqual(status, 200)


class CheckpointDisabledTest(_HttpFixture):
    def test_checkpoint_path_is_plain_404_without_checkpoint(self) -> None:
        # Audit is fully configured; only --checkpoint is missing.
        self.server.audit_path = self.journal
        self.server.audit_token = TOKEN
        status, body, _ = self._auth_get("/audit/checkpoint")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body), {"error": "not_found"})


class CheckpointAuthHttpTest(_HttpFixture):
    def setUp(self) -> None:
        super().setUp()
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        self._write_config(
            self._line("full", FULL_TOKEN, None) + "\n"
            + self._line("ops", OPS_TOKEN, None, ops=["copy"],
                         stages=["成功"], keys=["hist-1"]) + "\n")
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

    def test_full_scope_token_downloads(self) -> None:
        self._record("a")
        self._export()
        status, body, _ = self._auth_get("/audit/checkpoint",
                                         token=FULL_TOKEN)
        self.assertEqual(status, 200)
        self.assertEqual(body, self._checkpoint_bytes())

    def test_any_restricted_scope_is_403(self) -> None:
        self._record("a")
        self._export()
        for line in (
            self._line("s", OPS_TOKEN, None, ops=["copy"]),
            self._line("s", OPS_TOKEN, None, stages=["成功"]),
            self._line("s", OPS_TOKEN, None, keys=["hist-1"]),
        ):
            with self.subTest(line=line):
                self._write_config(line + "\n")
                status, body, _ = self._auth_get("/audit/checkpoint",
                                                 token=OPS_TOKEN)
                self.assertEqual(status, 403)
                self.assertEqual(json.loads(body), {"error": "forbidden"})

    def test_unknown_and_expired_tokens_are_403(self) -> None:
        self.assertEqual(
            self._auth_get("/audit/checkpoint", token="wrong")[0], 403)
        self._write_config(self._line("old", OPS_TOKEN, 1) + "\n")
        self.assertEqual(
            self._auth_get("/audit/checkpoint", token=OPS_TOKEN)[0], 403)

    def test_unreadable_or_invalid_config_is_503(self) -> None:
        os.unlink(self.config)
        status, body, _ = self._auth_get("/audit/checkpoint",
                                         token=FULL_TOKEN)
        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body), {"error": "auth_unavailable"})
        self._write_config("{not json\n")
        status, body, _ = self._auth_get("/audit/checkpoint",
                                         token=FULL_TOKEN)
        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body), {"error": "auth_unavailable"})


class ProofEncodingTest(_HttpFixture):
    """The proof success response writes non-ASCII through as UTF-8."""

    def setUp(self) -> None:
        super().setUp()
        self.server.audit_path = self.journal
        self.server.audit_token = TOKEN
        self.server.audit_checkpoint = self.checkpoint

    def test_proof_response_is_compact_utf8_without_trailing_newline(
            self) -> None:
        self._record("a", error="OSError", stage="同步")
        status, body, _ = self._auth_get("/audit/proof?generation=g1")
        self.assertEqual(status, 200)
        # The Chinese stage value appears directly as UTF-8 bytes, never
        # as \uXXXX escapes; the body is compact and ends without a
        # newline.
        self.assertIn("同步".encode("utf-8"), body)
        self.assertNotIn(b"\\u", body)
        self.assertNotIn(b": ", body)
        self.assertFalse(body.endswith(b"\n"))
        self.assertEqual(json.loads(body)["result"]["events"][0][1]
                         ["stage"], "同步")


if __name__ == "__main__":
    unittest.main()
