"""HTTP tests for the read-only GET /completions endpoint.

Covers the multi-token authorization (401/403/503 auth_unavailable),
the exact-lookup and paginated query shapes, the scope rules (exact
lookup needs unrestricted operation and stage scopes and a key scope
that names the requested job id, pagination needs all three scopes
unrestricted), the conditional If-None-Match handling, the error
mapping (400 invalid_request, 404 completion_not_found, 404
completion_job_not_found, 409 completion_invalid, 503
completion_unavailable) and the ``serve`` command's ``--completions``
option, which only ever accompanies ``--audit`` together with
``--auth``.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import sys
import threading
import time
import unittest
import urllib.parse
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from tempfile import TemporaryDirectory

from carbon_market import completion
from carbon_market.server import Handler

FULL_TOKEN = "full-token"
KEY_TOKEN = "key-token"
OPS_TOKEN = "ops-token"
OLD_TOKEN = "old-token"


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _line(name: str, token: str, grace_until,
          ops="*", stages="*", keys="*") -> str:
    return json.dumps([name, _digest(token), grace_until, ops, stages, keys],
                      ensure_ascii=False)


def _record(job_id: str, at: int = 70, outcome: str = "succeeded",
            cost: int = 90, carbon: int = 120) -> dict:
    return {"job_id": job_id, "at": at, "outcome": outcome,
            "actual_cost": cost, "actual_carbon": carbon,
            "generation": 0,
            "current": {"resource_id": "r-1", "version": 1},
            "cost_exceeded": cost > 1000, "carbon_exceeded": carbon > 1000}


class CompletionHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.ledger = os.path.join(self.tmp.name, "completions.json")
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        self._write_config(
            _line("full", FULL_TOKEN, None) + "\n"
            + _line("key", KEY_TOKEN, None, keys=["j-a"]) + "\n"
            + _line("ops", OPS_TOKEN, None, ops=["copy"]) + "\n")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.audit_path = self.journal
        self.server.audit_token = None
        self.server.audit_auth = self.config
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
        result = (response.status, body, response.headers)
        self.addCleanup(connection.close)
        return result

    def _json_get(self, path: str, **kwargs):
        status, body, _headers = self._get(path, **kwargs)
        return status, json.loads(body)

    def _token_get(self, token: str, path: str = "/completions"):
        return self._json_get(path, headers={"X-Audit-Token": token})

    # -- ledger fixture ------------------------------------------------------

    def _write_ledger(self, records: dict[str, dict]) -> None:
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
        with open(self.ledger, "w", encoding="utf-8") as handle:
            handle.write(raw)

    def _populate(self) -> None:
        self._write_ledger({
            "x-1": _record("j-a"),
            "x-2": _record("j-b", outcome="failed", cost=1001),
            "x-3": _record("j-c", carbon=1001),
            "x-4": _record("j-d", outcome="failed", cost=1001,
                           carbon=1001),
        })

    # -- authorization --------------------------------------------------------

    def test_missing_blank_and_duplicate_token_are_401(self) -> None:
        self.assertEqual(self._json_get("/completions")[0], 401)
        self.assertEqual(
            self._json_get("/completions",
                           headers={"X-Audit-Token": "  "})[0], 401)
        status, body = self._json_get(
            "/completions", raw_headers=[("X-Audit-Token", FULL_TOKEN),
                                         ("X-Audit-Token", FULL_TOKEN)])
        self.assertEqual(status, 401)
        self.assertEqual(body, {"error": "unauthorized"})

    def test_unknown_and_expired_token_are_403_without_opening_ledger(self):
        # No ledger exists; a 403 (not 404) proves it was never opened.
        status, body = self._token_get("wrong-token")
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "forbidden"})
        cutoff = int(time.time()) - 1
        self._write_config(_line("old", OLD_TOKEN, cutoff) + "\n")
        status, body = self._token_get(OLD_TOKEN)
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "forbidden"})

    def test_unreadable_or_invalid_config_is_503(self) -> None:
        os.unlink(self.config)
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "auth_unavailable"})
        self._write_config("{not json\n")
        status, body = self._token_get(FULL_TOKEN)
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "auth_unavailable"})

    def test_authorization_precedes_parameter_validation(self) -> None:
        self.assertEqual(self._json_get("/completions?bogus=1")[0], 401)
        self.assertEqual(
            self._token_get("wrong-token", "/completions?bogus=1")[0], 403)

    # -- query parameter validation -------------------------------------------

    def test_invalid_parameters_are_400(self) -> None:
        for path in (
            "/completions?bogus=1",              # unknown name
            "/completions?job=j-a&job=j-b",      # repeated name
            "/completions?job=",                 # empty value
            "/completions?cursor=",              # empty cursor
            "/completions?outcome=",             # empty outcome
            "/completions?exceeded=",            # empty exceeded
            "/completions?job=j-a&cursor=j-a",   # job with cursor
            "/completions?job=j-a&limit=10",     # job with page size
            "/completions?job=j-a&outcome=failed",  # job with filter
            "/completions?job=j-a&exceeded=any",    # job with filter
            "/completions?outcome=unknown",      # illegal outcome
            "/completions?exceeded=both",        # illegal exceeded
            "/completions?limit=0",
            "/completions?limit=1001",
            "/completions?limit=-1",
            "/completions?limit=abc",
            "/completions?cursor=%FF%FE",        # not UTF-8
        ):
            with self.subTest(path=path):
                status, body = self._token_get(FULL_TOKEN, path)
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})

    # -- scopes -----------------------------------------------------------------

    def test_exact_lookup_scopes(self) -> None:
        self._populate()
        # A restricted operation or stage scope forbids the lookup
        # entirely, whatever the key scope says.
        self.assertEqual(
            self._token_get(OPS_TOKEN, "/completions?job=j-a")[0], 403)
        # A key scope must name the requested job id explicitly.
        status, body = self._token_get(KEY_TOKEN, "/completions?job=j-a")
        self.assertEqual(status, 200)
        self.assertEqual(body["job_id"], "j-a")
        self.assertEqual(
            self._token_get(KEY_TOKEN, "/completions?job=j-b")[0], 403)

    def test_pagination_requires_unrestricted_scopes(self) -> None:
        self._populate()
        for token in (KEY_TOKEN, OPS_TOKEN):
            with self.subTest(token=token):
                status, body = self._token_get(token, "/completions")
                self.assertEqual(status, 403)
                self.assertEqual(body, {"error": "forbidden"})
        self.assertEqual(self._token_get(FULL_TOKEN, "/completions")[0], 200)

    def test_scope_403_never_opens_the_ledger(self) -> None:
        # No ledger exists: a scoped-out request still gets 403, not 404.
        self.assertEqual(
            self._token_get(OPS_TOKEN, "/completions?job=j-a")[0], 403)
        self.assertEqual(self._token_get(KEY_TOKEN, "/completions")[0], 403)

    # -- exact lookup ----------------------------------------------------------

    def test_exact_lookup_returns_the_record(self) -> None:
        self._populate()
        status, body = self._token_get(FULL_TOKEN, "/completions?job=j-b")
        self.assertEqual(status, 200)
        self.assertEqual(list(body),
                         ["job_id", "at", "outcome", "actual_cost",
                          "actual_carbon", "generation", "current",
                          "cost_exceeded", "carbon_exceeded"])
        self.assertEqual(body, completion.get(self.ledger, "j-b"))
        self.assertTrue(body["cost_exceeded"])
        self.assertFalse(body["carbon_exceeded"])

    def test_exact_lookup_response_is_compact_utf8(self) -> None:
        self._write_ledger({"x-1": _record("作业")})
        quoted = urllib.parse.quote("作业")
        status, raw, _ = self._get("/completions?job=" + quoted,
                                   headers={"X-Audit-Token": FULL_TOKEN})
        self.assertEqual(status, 200)
        record = completion.get(self.ledger, "作业")
        self.assertEqual(raw, json.dumps(
            record, ensure_ascii=False,
            separators=(",", ":")).encode("utf-8"))
        self.assertFalse(raw.endswith(b"\n"))

    # -- pagination --------------------------------------------------------------

    def test_pagination_follows_the_library_page_object(self) -> None:
        self._populate()
        status, body = self._token_get(FULL_TOKEN, "/completions?limit=3")
        self.assertEqual(status, 200)
        self.assertEqual(list(body), ["entries", "next"])
        self.assertEqual([e["job_id"] for e in body["entries"]],
                         ["j-a", "j-b", "j-c"])
        self.assertEqual(body["next"], "j-c")
        self.assertEqual(body, completion.search(self.ledger, limit=3))

        status, body = self._token_get(
            FULL_TOKEN, "/completions?limit=3&cursor=j-c")
        self.assertEqual([e["job_id"] for e in body["entries"]], ["j-d"])
        self.assertIsNone(body["next"])

        status, body = self._token_get(
            FULL_TOKEN, "/completions?outcome=failed&exceeded=any")
        self.assertEqual([e["job_id"] for e in body["entries"]],
                         ["j-b", "j-d"])
        status, body = self._token_get(
            FULL_TOKEN, "/completions?outcome=failed&exceeded=carbon")
        self.assertEqual([e["job_id"] for e in body["entries"]], ["j-d"])
        status, body = self._token_get(FULL_TOKEN,
                                       "/completions?exceeded=none")
        self.assertEqual([e["job_id"] for e in body["entries"]], ["j-a"])

    def test_pagination_response_is_compact_utf8(self) -> None:
        self._write_ledger({"x-1": _record("作业")})
        status, raw, _ = self._get("/completions",
                                   headers={"X-Audit-Token": FULL_TOKEN})
        self.assertEqual(status, 200)
        page = completion.search(self.ledger)
        self.assertEqual(raw, json.dumps(
            page, ensure_ascii=False,
            separators=(",", ":")).encode("utf-8"))

    # -- conditional reads ---------------------------------------------------

    def test_200_carries_strong_etag_of_the_response_bytes(self) -> None:
        self._populate()
        for path in ("/completions?job=j-a", "/completions?limit=2"):
            with self.subTest(path=path):
                status, body, headers = self._get(
                    path, headers={"X-Audit-Token": FULL_TOKEN})
                self.assertEqual(status, 200)
                # The strong tag is the SHA-256 of the exact response
                # bytes, quoted; the body keeps its compact form.
                self.assertEqual(
                    headers.get("ETag"),
                    '"' + hashlib.sha256(body).hexdigest() + '"')
                self.assertFalse(body.endswith(b"\n"))

    def test_matching_if_none_match_is_304_empty_with_same_etag(self):
        self._populate()
        for path in ("/completions?job=j-a", "/completions"):
            with self.subTest(path=path):
                _, _, headers = self._get(
                    path, headers={"X-Audit-Token": FULL_TOKEN})
                tag = headers.get("ETag")
                status, body, headers = self._get(
                    path, headers={"X-Audit-Token": FULL_TOKEN,
                                   "If-None-Match": tag})
                self.assertEqual(status, 304)
                self.assertEqual(body, b"")
                self.assertEqual(headers.get("ETag"), tag)

    def test_non_matching_if_none_match_is_200_with_full_body(self) -> None:
        self._populate()
        other = '"' + "0" * 64 + '"'
        status, body, headers = self._get(
            "/completions?job=j-a",
            headers={"X-Audit-Token": FULL_TOKEN, "If-None-Match": other})
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body),
                         completion.get(self.ledger, "j-a"))
        self.assertNotEqual(headers.get("ETag"), other)

    def test_invalid_conditional_headers_are_400_without_reading(self):
        # No ledger exists: a 400 (not 404) proves the ledger was never
        # read.
        good = hashlib.sha256(b"x").hexdigest()
        for value in ("", " ",                                   # blank
                      f'"{good[:63]}"',                          # too short
                      f'"{good}a"',                              # too long
                      f'"{good.upper()}"',                       # uppercase
                      f'"{"g" * 64}"',                           # not hex
                      good,                                      # unquoted
                      f'"{good}", "{good}"',                     # list
                      "*",                                       # wildcard
                      f'W/"{good}"',                             # weak tag
                      ):
            with self.subTest(value=value):
                status, body = self._json_get(
                    "/completions",
                    headers={"X-Audit-Token": FULL_TOKEN,
                             "If-None-Match": value})
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})
        tag = f'"{good}"'
        status, body = self._json_get(
            "/completions",
            raw_headers=[("X-Audit-Token", FULL_TOKEN),
                         ("If-None-Match", tag), ("If-None-Match", tag)])
        self.assertEqual(status, 400)
        self.assertEqual(body, {"error": "invalid_request"})

    def test_conditional_validation_order(self) -> None:
        self._populate()
        # Authorization precedes conditional validation.
        self.assertEqual(self._json_get(
            "/completions", headers={"If-None-Match": "bad"})[0], 401)
        # Conditional validation precedes the scope decision.
        self.assertEqual(self._json_get(
            "/completions", headers={"X-Audit-Token": OPS_TOKEN,
                                     "If-None-Match": "bad"})[0], 400)
        # A valid conditional does not rescue a scoped-out request.
        self.assertEqual(self._json_get(
            "/completions", headers={
                "X-Audit-Token": OPS_TOKEN,
                "If-None-Match": '"' + "0" * 64 + '"'})[0], 403)

    # -- ledger state errors ------------------------------------------------------

    def test_missing_ledger_is_404(self) -> None:
        status, body = self._token_get(FULL_TOKEN, "/completions?job=j-a")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "completion_not_found"})
        status, body = self._token_get(FULL_TOKEN, "/completions")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "completion_not_found"})

    def test_unknown_job_is_404(self) -> None:
        self._populate()
        status, body = self._token_get(FULL_TOKEN, "/completions?job=nope")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "completion_job_not_found"})

    def test_invalid_ledger_is_409(self) -> None:
        with open(self.ledger, "wb") as handle:
            handle.write(b"{not json")
        status, body = self._token_get(FULL_TOKEN, "/completions?job=j-a")
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "completion_invalid"})
        status, body = self._token_get(FULL_TOKEN, "/completions")
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "completion_invalid"})

    def test_io_failure_is_503(self) -> None:
        # A directory where the ledger file should be: the open fails.
        os.mkdir(self.ledger)
        status, body = self._token_get(FULL_TOKEN, "/completions?job=j-a")
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "completion_unavailable"})

    def test_error_bodies_never_leak_paths(self) -> None:
        with open(self.ledger, "wb") as handle:
            handle.write(b"{not json")
        for path in ("/completions", "/completions?job=j-a"):
            with self.subTest(path=path):
                _status, raw, _ = self._get(
                    path, headers={"X-Audit-Token": FULL_TOKEN})
                self.assertNotIn(self.tmp.name.encode(), raw)
                self.assertNotIn(b"completions.json", raw)

    # -- unchanged surface ---------------------------------------------------------

    def test_health_and_unknown_paths_unchanged(self) -> None:
        self.assertEqual(self._json_get("/health"), (200, {"status": "ok"}))
        self.assertEqual(self._json_get("/missing"),
                         (404, {"error": "not_found"}))
        self.assertEqual(self._json_get("/completions/"),
                         (404, {"error": "not_found"}))


class CompletionDisabledTest(unittest.TestCase):
    def test_completions_is_plain_404_without_configuration(self) -> None:
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connection = HTTPConnection("127.0.0.1", server.server_port,
                                        timeout=2)
            connection.request("GET", "/completions")
            response = connection.getresponse()
            self.assertEqual(response.status, 404)
            self.assertEqual(json.loads(response.read()),
                             {"error": "not_found"})
            connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


class ServeCompletionsArgumentsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        with open(self.config, "w", encoding="utf-8") as handle:
            handle.write(_line("full", FULL_TOKEN, None) + "\n")

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "carbon_market", "serve", *args],
            capture_output=True, timeout=10)

    def test_misplaced_empty_or_repeated_completions_exits_2(self) -> None:
        ledger = os.path.join(self.tmp.name, "completions.json")
        for args in (
            ("--completions", ledger),                 # no --audit
            ("--audit", "j", "--completions", ledger),  # no method
            ("--audit", "j", "--token", "t",
             "--completions", ledger),                   # single-token mode
            ("--audit", "j", "--auth", self.config,
             "--completions", ""),                       # empty value
            ("--audit", "j", "--auth", self.config,
             "--completions", ledger, "--completions", ledger),  # repeated
        ):
            with self.subTest(args=args):
                self.assertEqual(self._run(*args).returncode, 2)

    def test_valid_completions_starts_and_serves(self) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        process = subprocess.Popen(
            [sys.executable, "-m", "carbon_market", "serve",
             "--host", "127.0.0.1", "--port", str(port),
             "--audit", os.path.join(self.tmp.name, "audit.json"),
             "--auth", self.config,
             "--completions",
             os.path.join(self.tmp.name, "completions.json")])
        try:
            deadline = time.time() + 10
            statuses = []
            while time.time() < deadline:
                try:
                    connection = HTTPConnection("127.0.0.1", port, timeout=1)
                    connection.request("GET", "/completions",
                                       headers={"X-Audit-Token": FULL_TOKEN})
                    response = connection.getresponse()
                    body = json.loads(response.read())
                    statuses.append((response.status, body))
                    connection.close()
                    break
                except OSError:
                    time.sleep(0.05)
            # The ledger does not exist yet, but authorization worked.
            self.assertEqual(
                statuses, [(404, {"error": "completion_not_found"})])
        finally:
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
