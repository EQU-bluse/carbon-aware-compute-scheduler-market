"""Cross-endpoint regression tests for the shared conditional-query
pipeline of GET /audit/checkpoint, GET /acceptance and GET
/completions.

The three entries share one request pipeline in the server -- authorize,
validate the entry's own query string, validate the conditional
If-None-Match header, decide the token scope, and only then open the
snapshot -- while keeping their own query parameters, data reads and
error names. These tests pin the shared protocol (401/400/403 ordering,
the strong-tag conditional rule, the 200/304 response decision) and the
per-entry differences (query shapes, scope targets, error names) so the
shared implementation cannot drift the entries apart.
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

from carbon_market import acceptance, audit, audit_proof, completion
from carbon_market.server import Handler

FULL_TOKEN = "full-token"
KEY_TOKEN = "key-token"
OPS_TOKEN = "ops-token"

CHECKPOINT = "/audit/checkpoint"
ACCEPTANCE = "/acceptance"
COMPLETIONS = "/completions"
ENTRIES = (CHECKPOINT, ACCEPTANCE, COMPLETIONS)


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _line(name: str, token: str, grace_until,
          ops="*", stages="*", keys="*") -> str:
    return json.dumps([name, _digest(token), grace_until, ops, stages, keys],
                      ensure_ascii=False)


def _event() -> dict:
    return {"op": "copy", "target": "t.history", "key": "batch-key",
            "changed": True, "error": None, "stage": None}


def _completion_record(job_id: str, at: int = 70) -> dict:
    return {"job_id": job_id, "at": at, "outcome": "succeeded",
            "actual_cost": 90, "actual_carbon": 120, "generation": 0,
            "current": {"resource_id": "r-1", "version": 1},
            "cost_exceeded": False, "carbon_exceeded": False}


def _completions_bytes(records: dict[str, dict]) -> bytes:
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
    return (json.dumps(payload, ensure_ascii=False,
                       separators=(",", ":")) + "\n").encode("utf-8")


class ConditionalQueryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.checkpoint = os.path.join(self.tmp.name, "checkpoint.json")
        self.proof_path = os.path.join(self.tmp.name, "proof.json")
        self.ledger_dir = os.path.join(self.tmp.name, "ledger")
        self.ledger = os.path.join(self.tmp.name, "completions.json")
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        self._write_config(
            _line("full", FULL_TOKEN, None) + "\n"
            + _line("key", KEY_TOKEN, None, keys=["k1", "j-a"]) + "\n"
            + _line("ops", OPS_TOKEN, None, ops=["copy"]) + "\n")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.audit_path = self.journal
        self.server.audit_token = None
        self.server.audit_auth = self.config
        self.server.audit_checkpoint = self.checkpoint
        self.server.acceptance_dir = self.ledger_dir
        self.server.completions_path = self.ledger
        thread = threading.Thread(target=self.server.serve_forever,
                                  daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)

    def _write_config(self, text: str) -> None:
        # Same-directory atomic replacement, as a rotation would do.
        tmp_path = self.config + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp_path, self.config)

    def _get(self, path: str, headers: dict[str, str] | None = None,
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
        result = (response.status, response.headers, body)
        self.addCleanup(connection.close)
        return result

    def _token_get(self, path: str, token: str = FULL_TOKEN,
                   extra_headers: dict[str, str] | None = None):
        headers = {"X-Audit-Token": token}
        if extra_headers:
            headers.update(extra_headers)
        return self._get(path, headers=headers)

    # -- fixtures --------------------------------------------------------

    def _checkpoint_etag(self) -> str:
        with open(self.checkpoint, "rb") as handle:
            return '"' + hashlib.sha256(handle.read()).hexdigest() + '"'

    def _populate(self) -> None:
        # One accepted bundle under key k1, one completion for job j-a.
        audit.record(self.journal, "k1", _event())
        proof = audit_proof.export(self.journal, self.checkpoint, "g1")
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False))
        acceptance.submit(self.checkpoint, self.proof_path,
                          self._checkpoint_etag(), None,
                          self.ledger_dir, "k1")
        with open(self.ledger, "wb") as handle:
            records = {"x-1": _completion_record("j-a")}
            handle.write(_completions_bytes(records))

    def _assert_no_files(self) -> None:
        self.assertFalse(os.path.exists(self.checkpoint))
        self.assertFalse(os.path.exists(self.ledger_dir))
        self.assertFalse(os.path.exists(self.ledger))

    # -- stage order: authorization first ---------------------------------

    def test_authorization_precedes_parameters_header_and_files(self):
        # No token at all: an invalid query string and a malformed
        # conditional header are still masked by 401 on every entry, and
        # no snapshot is ever opened (the files stay absent).
        for path in (CHECKPOINT + "?x=1",
                     ACCEPTANCE + "?state=bogus",
                     COMPLETIONS + "?outcome=bogus"):
            with self.subTest(path=path):
                status, _headers, body = self._get(
                    path, headers={"If-None-Match": "not-a-tag"})
                self.assertEqual(status, 401)
                self.assertEqual(json.loads(body), {"error": "unauthorized"})
        self._assert_no_files()

    def test_unknown_token_is_403_before_any_file(self):
        # The data files do not exist: a 403 (not 404) on every entry
        # proves authorization still precedes any file access.
        for path in ENTRIES:
            with self.subTest(path=path):
                status, _headers, body = self._token_get(path, token="nope")
                self.assertEqual(status, 403)
                self.assertEqual(json.loads(body), {"error": "forbidden"})
        self._assert_no_files()

    # -- stage order: parameters and conditional header before scope ------

    def test_parameter_validation_precedes_scope(self):
        # A scope-restricted token with an invalid query string gets the
        # parameter 400, not the scope 403, on every entry.
        for path in (CHECKPOINT + "?limit=1",
                     ACCEPTANCE + "?key=k1&limit=1",
                     ACCEPTANCE + "?state=bogus",
                     COMPLETIONS + "?job=j-a&limit=1",
                     COMPLETIONS + "?exceeded=bogus"):
            with self.subTest(path=path):
                status, _headers, body = self._token_get(
                    path, token=OPS_TOKEN)
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(body),
                                 {"error": "invalid_request"})
        self._assert_no_files()

    def test_conditional_header_shapes_are_400_before_scope(self):
        # The same strong-tag rule on every entry, checked with a
        # scope-restricted token: a malformed header is a 400, never the
        # scope 403, and never opens a file.
        good = hashlib.sha256(b"x").hexdigest()
        bad_values = (
            "", " ", "   ",                                   # blank
            f'"{good[:63]}"',                                 # too short
            f'"{good}a"',                                     # too long
            f'"{good.upper()}"',                              # uppercase
            f'"{"g" * 64}"',                                  # not hex
            good,                                             # unquoted
            f"\"{good}\", \"{good}\"",                        # list
            "*",                                              # wildcard
            f'W/"{good}"',                                    # weak tag
            f' "{good}" ',                                    # spaces
        )
        for path in ENTRIES:
            for value in bad_values:
                with self.subTest(path=path, value=value):
                    status, _headers, body = self._token_get(
                        path, token=OPS_TOKEN,
                        extra_headers={"If-None-Match": value})
                    self.assertEqual(status, 400)
                    self.assertEqual(json.loads(body),
                                     {"error": "invalid_request"})
        self._assert_no_files()

    def test_duplicate_conditional_header_is_400_on_every_entry(self):
        tag = f'"{hashlib.sha256(b"x").hexdigest()}"'
        for path in ENTRIES:
            with self.subTest(path=path):
                status, _headers, body = self._get(
                    path, raw_headers=[
                        ("X-Audit-Token", FULL_TOKEN),
                        ("If-None-Match", tag), ("If-None-Match", tag)])
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(body),
                                 {"error": "invalid_request"})
        self._assert_no_files()

    # -- stage order: scope before the snapshot ---------------------------

    def test_scope_precedes_file_access(self):
        # No data files exist: every scope refusal is a 403, never the
        # 404 an opened file would produce.
        restricted = (
            (CHECKPOINT, OPS_TOKEN),
            (CHECKPOINT, KEY_TOKEN),
            (ACCEPTANCE, OPS_TOKEN),
            (ACCEPTANCE + "?key=k1", OPS_TOKEN),
            (ACCEPTANCE, KEY_TOKEN),          # pagination needs all axes
            (COMPLETIONS, OPS_TOKEN),
            (COMPLETIONS + "?job=j-a", OPS_TOKEN),
            (COMPLETIONS, KEY_TOKEN),         # pagination needs all axes
        )
        for path, token in restricted:
            with self.subTest(path=path, token=token):
                status, _headers, body = self._token_get(path, token=token)
                self.assertEqual(status, 403)
                self.assertEqual(json.loads(body), {"error": "forbidden"})
        self._assert_no_files()

    def test_exact_lookup_scope_rules(self):
        self._populate()
        # A key-scoped token reaches an exact lookup that names an
        # allowed key or job id, and only that one.
        allowed = ((ACCEPTANCE + "?key=k1", "key"),
                   (COMPLETIONS + "?job=j-a", "job_id"))
        for path, field in allowed:
            with self.subTest(path=path):
                status, _headers, body = self._token_get(
                    path, token=KEY_TOKEN)
                self.assertEqual(status, 200)
                self.assertEqual(json.loads(body)[field],
                                 "k1" if field == "key" else "j-a")
        for path in (ACCEPTANCE + "?key=k2", COMPLETIONS + "?job=j-b"):
            with self.subTest(path=path):
                status, _headers, body = self._token_get(
                    path, token=KEY_TOKEN)
                self.assertEqual(status, 403)
                self.assertEqual(json.loads(body), {"error": "forbidden"})

    # -- shared success and conditional protocol --------------------------

    def test_200_304_and_stale_tag_behave_the_same_on_every_entry(self):
        self._populate()
        with open(self.checkpoint, "rb") as handle:
            checkpoint_raw = handle.read()
        stale = '"' + "0" * 64 + '"'
        for path in (CHECKPOINT, ACCEPTANCE, ACCEPTANCE + "?key=k1",
                     COMPLETIONS, COMPLETIONS + "?job=j-a"):
            with self.subTest(path=path):
                status, headers, body = self._token_get(path)
                self.assertEqual(status, 200)
                tag = headers.get("ETag")
                # The strong tag summarizes exactly the served bytes.
                self.assertEqual(
                    tag, '"' + hashlib.sha256(body).hexdigest() + '"')
                if path == CHECKPOINT:
                    # The checkpoint is served as its original bytes.
                    self.assertEqual(body, checkpoint_raw)
                else:
                    # Compact UTF-8 JSON with no trailing newline.
                    self.assertFalse(body.endswith(b"\n"))
                self.assertNotIn(b": ", body)
                self.assertNotIn(b", ", body)
                # A matching condition is a 304 with an empty body and
                # the same tag; a stale one is the full 200 again.
                status, headers, not_modified = self._token_get(
                    path, extra_headers={"If-None-Match": tag})
                self.assertEqual(status, 304)
                self.assertEqual(not_modified, b"")
                self.assertEqual(headers.get("ETag"), tag)
                self.assertEqual(headers.get("Content-Length"), "0")
                status, headers, again = self._token_get(
                    path, extra_headers={"If-None-Match": stale})
                self.assertEqual(status, 200)
                self.assertEqual(again, body)
                self.assertEqual(headers.get("ETag"), tag)

    def test_checkpoint_takes_no_parameters_unlike_the_queries(self):
        self._populate()
        status, _headers, body = self._token_get(CHECKPOINT + "?limit=1")
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body), {"error": "invalid_request"})
        for path in (ACCEPTANCE + "?limit=1", COMPLETIONS + "?limit=1"):
            with self.subTest(path=path):
                status, _headers, _body = self._token_get(path)
                self.assertEqual(status, 200)

    # -- per-entry error names ---------------------------------------------

    def test_missing_snapshot_keeps_each_entrys_own_404(self):
        expected = {CHECKPOINT: "checkpoint_not_found",
                    ACCEPTANCE: "acceptance_not_found",
                    COMPLETIONS: "completion_not_found"}
        for path, error in expected.items():
            with self.subTest(path=path):
                status, _headers, body = self._token_get(path)
                self.assertEqual(status, 404)
                self.assertEqual(json.loads(body), {"error": error})
                self.assertNotIn(self.tmp.name.encode(), body)

    def test_unknown_exact_key_keeps_each_entrys_own_404(self):
        self._populate()
        status, _headers, body = self._token_get(ACCEPTANCE + "?key=k2")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body),
                         {"error": "acceptance_key_not_found"})
        status, _headers, body = self._token_get(COMPLETIONS + "?job=j-b")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body),
                         {"error": "completion_job_not_found"})

    def test_invalid_snapshot_keeps_each_entrys_own_409(self):
        self._populate()
        with open(self.checkpoint, "wb") as handle:
            handle.write(b"{not json")
        with open(os.path.join(self.ledger_dir, "ledger.json"),
                  "wb") as handle:
            handle.write(b"{not json")
        with open(self.ledger, "wb") as handle:
            handle.write(b"{not json")
        expected = {CHECKPOINT: "checkpoint_invalid",
                    ACCEPTANCE: "acceptance_invalid",
                    COMPLETIONS: "completion_invalid"}
        for path, error in expected.items():
            with self.subTest(path=path):
                status, _headers, body = self._token_get(path)
                self.assertEqual(status, 409)
                self.assertEqual(json.loads(body), {"error": error})
                self.assertNotIn(self.tmp.name.encode(), body)

    def test_io_failure_keeps_each_entrys_own_503(self):
        # A directory where each file should be: the open fails with an
        # OSError that is not FileNotFoundError.
        os.mkdir(self.checkpoint)
        os.mkdir(self.ledger_dir)
        os.mkdir(os.path.join(self.ledger_dir, "ledger.json"))
        os.mkdir(self.ledger)
        expected = {CHECKPOINT: "checkpoint_unavailable",
                    ACCEPTANCE: "acceptance_unavailable",
                    COMPLETIONS: "completion_unavailable"}
        for path, error in expected.items():
            with self.subTest(path=path):
                status, _headers, body = self._token_get(path)
                self.assertEqual(status, 503)
                self.assertEqual(json.loads(body), {"error": error})
                self.assertNotIn(self.tmp.name.encode(), body)

    # -- concurrency: one complete version per response --------------------

    def test_racing_writes_never_mix_an_old_tag_with_a_new_body(self):
        self._populate()
        results: list[tuple[int, str | None, bytes]] = []
        errors: list[BaseException] = []

        def reader(path: str) -> None:
            try:
                for _ in range(15):
                    status, headers, body = self._token_get(path)
                    results.append((status, headers.get("ETag"), body))
            except BaseException as exc:  # pragma: no cover - failure path
                errors.append(exc)

        def acceptance_writer() -> None:
            try:
                for index in range(5):
                    acceptance.submit(self.checkpoint, self.proof_path,
                                      self._checkpoint_etag(), None,
                                      self.ledger_dir, f"w{index}")
            except BaseException as exc:  # pragma: no cover - failure path
                errors.append(exc)

        def completions_writer() -> None:
            try:
                records = {"x-1": _completion_record("j-a")}
                for index in range(5):
                    records[f"x-w{index}"] = _completion_record(
                        f"j-w{index}", at=71 + index)
                    payload = _completions_bytes(records)
                    realpath = os.path.realpath(self.ledger)
                    with completion._lock(realpath):
                        with open(self.ledger, "wb") as handle:
                            handle.write(payload)
            except BaseException as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=reader, args=(ACCEPTANCE,))
                   for _ in range(3)]
        threads += [threading.Thread(target=reader, args=(COMPLETIONS,))
                    for _ in range(3)]
        threads += [threading.Thread(target=acceptance_writer),
                    threading.Thread(target=completions_writer)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertTrue(results)
        for status, etag, body in results:
            self.assertEqual(status, 200)
            self.assertIsNotNone(etag)
            # Whatever version was served, the tag summarizes exactly
            # the bytes in hand -- never an old tag over a new body.
            self.assertEqual(
                etag, '"' + hashlib.sha256(body).hexdigest() + '"')
            json.loads(body)


if __name__ == "__main__":
    unittest.main()
