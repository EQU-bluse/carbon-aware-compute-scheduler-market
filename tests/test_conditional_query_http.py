"""Regression tests for the shared conditional-query pipeline.

GET /audit/checkpoint, GET /acceptance and GET /completions run one
shared request pipeline: the caller's identity first, then the
endpoint's own query-string validation, then the If-None-Match
validation, then the token's scope, and only then the locked snapshot
read that produces the body and its ETag. These tests pin the common
protocol (the 401/400/403 order, the strong-ETag shapes, the 304/200
decision) and the per-endpoint differences (query parameters, scope
rules, error names, body shapes), and prove a rejection at one stage
never opens a later stage's files.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from tempfile import TemporaryDirectory

from carbon_market import acceptance, audit, audit_proof
from carbon_market.server import Handler

FULL_TOKEN = "full-token"
KEY_TOKEN = "key-token"        # key scope names one acceptance key, one job
OPS_TOKEN = "ops-token"        # operation-scoped
STAGE_TOKEN = "stage-token"    # stage-scoped

CHECKPOINT = "/audit/checkpoint"
ACCEPTANCE = "/acceptance"
COMPLETIONS = "/completions"
ENDPOINTS = (CHECKPOINT, ACCEPTANCE, COMPLETIONS)


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _line(name: str, token: str, grace_until,
          ops="*", stages="*", keys="*") -> str:
    return json.dumps([name, _digest(token), grace_until, ops, stages, keys],
                      ensure_ascii=False)


def _event(key: str) -> dict:
    return {"op": "copy", "target": "t.history", "key": key,
            "changed": True, "error": None, "stage": None}


def _completion(job_id: str, outcome: str = "succeeded") -> dict:
    return {"job_id": job_id, "at": 70, "outcome": outcome,
            "actual_cost": 90, "actual_carbon": 120, "generation": 0,
            "current": {"resource_id": "r-1", "version": 1},
            "cost_exceeded": False, "carbon_exceeded": False}


class ConditionalQueryHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.checkpoint = os.path.join(self.tmp.name, "checkpoint.json")
        self.proof_path = os.path.join(self.tmp.name, "proof.json")
        self.ledger_dir = os.path.join(self.tmp.name, "ledger")
        self.completions = os.path.join(self.tmp.name, "completions.json")
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        self._write_config(
            _line("full", FULL_TOKEN, None) + "\n"
            + _line("key", KEY_TOKEN, None, keys=["k1", "j-a"]) + "\n"
            + _line("ops", OPS_TOKEN, None, ops=["copy"]) + "\n"
            + _line("stage", STAGE_TOKEN, None, stages=["执行"]) + "\n")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.audit_path = self.journal
        self.server.audit_token = None
        self.server.audit_auth = self.config
        self.server.audit_checkpoint = self.checkpoint
        self.server.acceptance_dir = self.ledger_dir
        self.server.completions_path = self.completions
        thread = threading.Thread(target=self.server.serve_forever,
                                  daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)

    # -- fixture helpers -------------------------------------------------

    def _write_config(self, text: str) -> None:
        # Same-directory atomic replacement, as a rotation would do.
        tmp_path = self.config + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp_path, self.config)

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
        result = (response.status, response.headers, body)
        self.addCleanup(connection.close)
        return result

    def _json_get(self, path: str, **kwargs):
        status, _headers, body = self._get(path, **kwargs)
        return status, json.loads(body)

    def _token_get(self, token: str, path: str):
        return self._json_get(path, headers={"X-Audit-Token": token})

    def _export(self, generation: str = "g1") -> dict:
        audit.record(self.journal, "k1", _event("k1"))
        return audit_proof.export(self.journal, self.checkpoint, generation)

    def _submit(self, key: str) -> None:
        proof = self._export()
        with open(self.checkpoint, "rb") as handle:
            etag = '"' + hashlib.sha256(handle.read()).hexdigest() + '"'
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False))
        acceptance.submit(self.checkpoint, self.proof_path, etag, None,
                          self.ledger_dir, key)

    def _write_completions(self, records: dict[str, dict]) -> None:
        completions: dict[str, dict] = {}
        idempotency: dict[str, dict] = {}
        events: dict[str, dict] = {}
        for key, record in records.items():
            request = {field: record[field] for field in
                       ("job_id", "at", "outcome", "actual_cost",
                        "actual_carbon")}
            completions[key] = record
            idempotency[key] = request
            events[key] = {"key": key, "request": dict(request),
                           "result": dict(record)}
        payload = {
            "version": 1,
            "completions": {key: completions[key]
                            for key in sorted(completions)},
            "idempotency": {key: idempotency[key]
                            for key in sorted(idempotency)},
            "audit": {key: events[key] for key in sorted(events)},
        }
        raw = json.dumps(payload, ensure_ascii=False,
                         separators=(",", ":")) + "\n"
        with open(self.completions, "w", encoding="utf-8") as handle:
            handle.write(raw)

    def _populate(self) -> None:
        self._submit("k1")
        self._submit("k2")
        self._write_completions({"x-1": _completion("j-a"),
                                 "x-2": _completion("j-b",
                                                    outcome="failed")})

    def _assert_no_files(self) -> None:
        # Nothing was created or needed: every rejection happened before
        # the snapshot stage.
        self.assertFalse(os.path.exists(self.checkpoint))
        self.assertFalse(os.path.exists(self.ledger_dir))
        self.assertFalse(os.path.exists(self.completions))

    # -- stage order: identity first --------------------------------------

    def test_identity_precedes_all_validation_on_every_endpoint(self) -> None:
        tag = 'W/"' + "0" * 64 + '"'
        for path in (CHECKPOINT + "?bogus=1", ACCEPTANCE + "?bogus=1",
                     COMPLETIONS + "?bogus=1"):
            with self.subTest(path=path):
                # No token at all: 401 masks the bad query and the weak
                # conditional tag.
                status, body = self._json_get(
                    path, headers={"If-None-Match": tag})
                self.assertEqual(status, 401)
                self.assertEqual(body, {"error": "unauthorized"})
                # An unknown token: 403 masks them too.
                status, body = self._json_get(
                    path, headers={"X-Audit-Token": "wrong",
                                   "If-None-Match": tag})
                self.assertEqual(status, 403)
                self.assertEqual(body, {"error": "forbidden"})
        self._assert_no_files()

    def test_unreadable_auth_config_is_503_on_every_endpoint(self) -> None:
        os.unlink(self.config)
        for path in ENDPOINTS:
            with self.subTest(path=path):
                status, body = self._token_get(FULL_TOKEN, path)
                self.assertEqual(status, 503)
                self.assertEqual(body, {"error": "auth_unavailable"})
        self._assert_no_files()

    # -- stage order: parameters, then the conditional header -------------

    def test_parameters_precede_scope_on_every_endpoint(self) -> None:
        # A fully scope-restricted token with an invalid query string
        # gets the parameter error, not the scope denial.
        for token, path in (
                (OPS_TOKEN, CHECKPOINT + "?cursor=a"),
                (OPS_TOKEN, ACCEPTANCE + "?key=k1&limit=2"),
                (OPS_TOKEN, COMPLETIONS + "?job=j-a&outcome=failed"),
                (KEY_TOKEN, ACCEPTANCE + "?state=unknown"),
                (KEY_TOKEN, COMPLETIONS + "?exceeded=unknown")):
            with self.subTest(path=path):
                status, body = self._token_get(token, path)
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})
        self._assert_no_files()

    def test_conditional_header_precedes_scope_on_every_endpoint(self) -> None:
        # A fully scope-restricted token with a malformed conditional
        # header gets the header error, not the scope denial.
        for path in ENDPOINTS:
            with self.subTest(path=path):
                status, body = self._json_get(
                    path, headers={"X-Audit-Token": OPS_TOKEN,
                                   "If-None-Match": "*"})
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})
        self._assert_no_files()

    def test_scope_precedes_file_access_on_every_endpoint(self) -> None:
        # No checkpoint or ledger exists: a 403 (not 404) proves the
        # scope denial happened before any file was opened.
        for token, path in (
                (KEY_TOKEN, CHECKPOINT),
                (OPS_TOKEN, CHECKPOINT),
                (STAGE_TOKEN, CHECKPOINT),
                (KEY_TOKEN, ACCEPTANCE),
                (KEY_TOKEN, ACCEPTANCE + "?key=k9"),
                (OPS_TOKEN, ACCEPTANCE + "?key=k1"),
                (STAGE_TOKEN, ACCEPTANCE + "?key=k1"),
                (KEY_TOKEN, COMPLETIONS),
                (KEY_TOKEN, COMPLETIONS + "?job=j-9"),
                (OPS_TOKEN, COMPLETIONS + "?job=j-a"),
                (STAGE_TOKEN, COMPLETIONS + "?job=j-a")):
            with self.subTest(path=path):
                status, body = self._token_get(token, path)
                self.assertEqual(status, 403)
                self.assertEqual(body, {"error": "forbidden"})
        self._assert_no_files()

    def test_validation_precedes_file_access_on_every_endpoint(self) -> None:
        # No checkpoint or ledger exists: a 400 (not 404) proves the
        # query and header were rejected before any file was opened.
        good = hashlib.sha256(b"x").hexdigest()
        for path, headers in (
                (CHECKPOINT + "?x=1", {}),
                (ACCEPTANCE + "?limit=0", {}),
                (COMPLETIONS + "?limit=abc", {}),
                (CHECKPOINT, {"If-None-Match": f'W/"{good}"'}),
                (ACCEPTANCE, {"If-None-Match": good}),
                (COMPLETIONS, {"If-None-Match": f'"{good}", "{good}"'})):
            with self.subTest(path=path):
                status, body = self._json_get(
                    path, headers={"X-Audit-Token": FULL_TOKEN, **headers})
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})
        self._assert_no_files()

    # -- the shared conditional-header protocol ----------------------------

    def test_malformed_conditional_headers_are_400_on_every_endpoint(self):
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
        ):
            for path in ENDPOINTS:
                with self.subTest(path=path, value=value):
                    status, body = self._json_get(
                        path, headers={"X-Audit-Token": FULL_TOKEN,
                                       "If-None-Match": value})
                    self.assertEqual(status, 400)
                    self.assertEqual(body, {"error": "invalid_request"})
        self._assert_no_files()

    def test_duplicate_conditional_header_is_400_on_every_endpoint(self):
        tag = f'"{hashlib.sha256(b"x").hexdigest()}"'
        for path in ENDPOINTS:
            with self.subTest(path=path):
                status, body = self._json_get(path, raw_headers=[
                    ("X-Audit-Token", FULL_TOKEN),
                    ("If-None-Match", tag), ("If-None-Match", tag)])
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})
        self._assert_no_files()

    # -- scope differences --------------------------------------------------

    def test_checkpoint_requires_fully_unrestricted_scope(self) -> None:
        self._export()
        # Even a key scope that would name an existing acceptance key
        # may not download the whole snapshot.
        for token in (KEY_TOKEN, OPS_TOKEN, STAGE_TOKEN):
            with self.subTest(token=token):
                status, body = self._token_get(token, CHECKPOINT)
                self.assertEqual(status, 403)
                self.assertEqual(body, {"error": "forbidden"})
        self.assertEqual(self._token_get(FULL_TOKEN, CHECKPOINT)[0], 200)

    def test_exact_lookup_allows_a_matching_key_scope(self) -> None:
        self._populate()
        # The key-scoped token reads exactly the acceptance key and the
        # completion job its scope names.
        status, body = self._token_get(KEY_TOKEN, ACCEPTANCE + "?key=k1")
        self.assertEqual(status, 200)
        self.assertEqual(body["state"], "active")
        status, body = self._token_get(KEY_TOKEN, COMPLETIONS + "?job=j-a")
        self.assertEqual(status, 200)
        self.assertEqual(body["job_id"], "j-a")
        # A key outside the scope is forbidden, and so is every
        # paginated query, even one whose page would only hold the
        # scoped key.
        for path in (ACCEPTANCE + "?key=k2", ACCEPTANCE,
                     ACCEPTANCE + "?state=active",
                     COMPLETIONS + "?job=j-b", COMPLETIONS,
                     COMPLETIONS + "?outcome=succeeded"):
            with self.subTest(path=path):
                status, body = self._token_get(KEY_TOKEN, path)
                self.assertEqual(status, 403)
                self.assertEqual(body, {"error": "forbidden"})

    # -- the shared 200/304 success shape -----------------------------------

    def _check_etag_roundtrip(self, path: str) -> None:
        # A 200 carries the strong tag of its exact bytes; a matching
        # condition answers 304 with an empty body and the same tag; a
        # mismatching one answers the full 200 again.
        status, headers, body = self._get(
            path, headers={"X-Audit-Token": FULL_TOKEN})
        self.assertEqual(status, 200)
        etag = headers.get("ETag")
        self.assertEqual(etag, '"' + hashlib.sha256(body).hexdigest() + '"')
        status, headers, again = self._get(
            path, headers={"X-Audit-Token": FULL_TOKEN,
                           "If-None-Match": etag})
        self.assertEqual(status, 304)
        self.assertEqual(again, b"")
        self.assertEqual(headers.get("ETag"), etag)
        stale = '"' + hashlib.sha256(b"stale").hexdigest() + '"'
        status, headers, third = self._get(
            path, headers={"X-Audit-Token": FULL_TOKEN,
                           "If-None-Match": stale})
        self.assertEqual(status, 200)
        self.assertEqual(third, body)
        self.assertEqual(headers.get("ETag"), etag)

    def test_checkpoint_serves_original_bytes_with_strong_etag(self) -> None:
        self._export("世代-1")
        with open(self.checkpoint, "rb") as handle:
            raw = handle.read()
        status, headers, body = self._get(
            CHECKPOINT, headers={"X-Audit-Token": FULL_TOKEN})
        self.assertEqual(status, 200)
        # The exact file bytes: no reserialization, no added newline.
        self.assertEqual(body, raw)
        self.assertIn("世代-1".encode("utf-8"), body)
        self._check_etag_roundtrip(CHECKPOINT)

    def test_acceptance_bodies_are_compact_with_strong_etag(self) -> None:
        self._populate()
        for path in (ACCEPTANCE + "?key=k1", ACCEPTANCE,
                     ACCEPTANCE + "?state=active&limit=1",
                     ACCEPTANCE + "?cursor=k1"):
            with self.subTest(path=path):
                status, headers, body = self._get(
                    path, headers={"X-Audit-Token": FULL_TOKEN})
                self.assertEqual(status, 200)
                # Compact UTF-8 JSON, no trailing newline.
                self.assertFalse(body.endswith(b"\n"))
                self.assertNotIn(b": ", body)
                self.assertEqual(headers.get("Content-Type"),
                                 "application/json")
                self._check_etag_roundtrip(path)

    def test_completions_bodies_are_compact_with_strong_etag(self) -> None:
        self._populate()
        for path in (COMPLETIONS + "?job=j-a", COMPLETIONS,
                     COMPLETIONS + "?outcome=failed",
                     COMPLETIONS + "?exceeded=none&limit=1"):
            with self.subTest(path=path):
                status, headers, body = self._get(
                    path, headers={"X-Audit-Token": FULL_TOKEN})
                self.assertEqual(status, 200)
                self.assertFalse(body.endswith(b"\n"))
                self.assertNotIn(b": ", body)
                self.assertEqual(headers.get("Content-Type"),
                                 "application/json")
                self._check_etag_roundtrip(path)

    # -- per-endpoint error names -------------------------------------------

    def test_missing_files_keep_their_own_404_names(self) -> None:
        for path, error in (
                (CHECKPOINT, "checkpoint_not_found"),
                (ACCEPTANCE, "acceptance_not_found"),
                (ACCEPTANCE + "?key=k1", "acceptance_not_found"),
                (COMPLETIONS, "completion_not_found"),
                (COMPLETIONS + "?job=j-a", "completion_not_found")):
            with self.subTest(path=path):
                status, _headers, body = self._get(
                    path, headers={"X-Audit-Token": FULL_TOKEN})
                self.assertEqual(status, 404)
                self.assertEqual(json.loads(body), {"error": error})
                # The configured paths never leak into the response.
                self.assertNotIn(self.tmp.name.encode("utf-8"), body)

    def test_unknown_keys_keep_their_own_404_names(self) -> None:
        self._populate()
        status, body = self._token_get(FULL_TOKEN, ACCEPTANCE + "?key=k9")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "acceptance_key_not_found"})
        status, body = self._token_get(FULL_TOKEN, COMPLETIONS + "?job=j-9")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "completion_job_not_found"})

    def test_invalid_content_keeps_its_own_409_names(self) -> None:
        self._populate()
        with open(self.checkpoint, "wb") as handle:
            handle.write(b"{not json")
        with open(os.path.join(self.ledger_dir, "ledger.json"),
                  "wb") as handle:
            handle.write(b"{not json")
        with open(self.completions, "wb") as handle:
            handle.write(b"{not json")
        for path, error in (
                (CHECKPOINT, "checkpoint_invalid"),
                (ACCEPTANCE, "acceptance_invalid"),
                (ACCEPTANCE + "?key=k1", "acceptance_invalid"),
                (COMPLETIONS, "completion_invalid"),
                (COMPLETIONS + "?job=j-a", "completion_invalid")):
            with self.subTest(path=path):
                status, _headers, body = self._get(
                    path, headers={"X-Audit-Token": FULL_TOKEN})
                self.assertEqual(status, 409)
                self.assertEqual(json.loads(body), {"error": error})
                self.assertNotIn(self.tmp.name.encode("utf-8"), body)


if __name__ == "__main__":
    unittest.main()
