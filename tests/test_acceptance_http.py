"""HTTP tests for the read-only GET /acceptance endpoint.

Covers the multi-token authorization (401/403/503 auth_unavailable),
the exact-lookup and paginated query shapes, the scope rules (exact
lookup needs unrestricted operation and stage scopes and a key scope
that names the requested key, pagination needs all three scopes
unrestricted), the error mapping (400 invalid_request, 404
acceptance_not_found, 404 acceptance_key_not_found, 409
acceptance_invalid, 503 acceptance_unavailable) and the ``serve``
command's ``--acceptance`` option, which only ever accompanies
``--audit`` together with ``--auth``.
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

from carbon_market import acceptance, audit, audit_proof
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


def _event(op: str = "copy", key: str = "batch-key",
           error: str | None = None, stage: str | None = None) -> dict:
    return {"op": op, "target": "t.history", "key": key,
            "changed": True, "error": error, "stage": stage}


class AcceptanceHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.checkpoint = os.path.join(self.tmp.name, "checkpoint.json")
        self.proof_path = os.path.join(self.tmp.name, "proof.json")
        self.ledger_dir = os.path.join(self.tmp.name, "ledger")
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        self._write_config(
            _line("full", FULL_TOKEN, None) + "\n"
            + _line("key", KEY_TOKEN, None, keys=["k1"]) + "\n"
            + _line("ops", OPS_TOKEN, None, ops=["copy"]) + "\n")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.audit_path = self.journal
        self.server.audit_token = None
        self.server.audit_auth = self.config
        self.server.acceptance_dir = self.ledger_dir
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
        self.addCleanup(connection.close)
        return response.status, body

    def _json_get(self, path: str, **kwargs):
        status, body = self._get(path, **kwargs)
        return status, json.loads(body)

    def _token_get(self, token: str, path: str = "/acceptance"):
        return self._json_get(path, headers={"X-Audit-Token": token})

    # -- ledger fixture ------------------------------------------------------

    def _etag(self) -> str:
        with open(self.checkpoint, "rb") as handle:
            return '"' + hashlib.sha256(handle.read()).hexdigest() + '"'

    def _submit(self, key: str, etag: str | None = None):
        audit.record(self.journal, key, _event())
        proof = audit_proof.export(self.journal, self.checkpoint, "g1")
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False))
        return acceptance.submit(self.checkpoint, self.proof_path,
                                 etag or self._etag(), None,
                                 self.ledger_dir, key)

    def _populate(self) -> None:
        self._submit("k1")
        self._submit("k2")
        # k3 quarantines on a claimed-tag mismatch.
        with self.assertRaises(audit_proof.BundleMismatchError):
            self._submit("k3", etag='"' + "0" * 64 + '"')

    # -- authorization --------------------------------------------------------

    def test_missing_blank_and_duplicate_token_are_401(self) -> None:
        self.assertEqual(self._json_get("/acceptance")[0], 401)
        self.assertEqual(
            self._json_get("/acceptance",
                           headers={"X-Audit-Token": "  "})[0], 401)
        status, body = self._json_get(
            "/acceptance", raw_headers=[("X-Audit-Token", FULL_TOKEN),
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
        self.assertEqual(self._json_get("/acceptance?bogus=1")[0], 401)
        self.assertEqual(
            self._token_get("wrong-token", "/acceptance?bogus=1")[0], 403)

    # -- query parameter validation -------------------------------------------

    def test_invalid_parameters_are_400(self) -> None:
        for path in (
            "/acceptance?bogus=1",           # unknown name
            "/acceptance?key=k1&key=k2",     # repeated name
            "/acceptance?key=",              # empty value
            "/acceptance?cursor=",           # empty cursor
            "/acceptance?state=",            # empty state
            "/acceptance?key=k1&cursor=k0",  # key with cursor
            "/acceptance?key=k1&limit=10",   # key with page size
            "/acceptance?key=k1&state=active",  # key with state
            "/acceptance?state=unknown",     # illegal state
            "/acceptance?limit=0",
            "/acceptance?limit=1001",
            "/acceptance?limit=-1",
            "/acceptance?limit=abc",
            "/acceptance?cursor=%FF%FE",     # not UTF-8
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
            self._token_get(OPS_TOKEN, "/acceptance?key=k1")[0], 403)
        # A key scope must name the requested key explicitly.
        status, body = self._token_get(KEY_TOKEN, "/acceptance?key=k1")
        self.assertEqual(status, 200)
        self.assertEqual(body["key"], "k1")
        self.assertEqual(
            self._token_get(KEY_TOKEN, "/acceptance?key=k2")[0], 403)

    def test_pagination_requires_unrestricted_scopes(self) -> None:
        self._populate()
        for token in (KEY_TOKEN, OPS_TOKEN):
            with self.subTest(token=token):
                status, body = self._token_get(token, "/acceptance")
                self.assertEqual(status, 403)
                self.assertEqual(body, {"error": "forbidden"})
        self.assertEqual(self._token_get(FULL_TOKEN, "/acceptance")[0], 200)

    def test_scope_403_never_opens_the_ledger(self) -> None:
        # No ledger exists: a scoped-out request still gets 403, not 404.
        self.assertEqual(
            self._token_get(OPS_TOKEN, "/acceptance?key=k1")[0], 403)
        self.assertEqual(self._token_get(KEY_TOKEN, "/acceptance")[0], 403)

    # -- exact lookup ----------------------------------------------------------

    def test_exact_lookup_returns_the_record(self) -> None:
        self._populate()
        status, body = self._token_get(FULL_TOKEN, "/acceptance?key=k1")
        self.assertEqual(status, 200)
        self.assertEqual(list(body),
                         ["key", "etag", "checkpoint_digest",
                          "proof_digest", "state", "error"])
        self.assertEqual(body, acceptance.get(self.ledger_dir, "k1"))
        status, body = self._token_get(FULL_TOKEN, "/acceptance?key=k3")
        self.assertEqual(status, 200)
        self.assertEqual(body["state"], "quarantined")
        self.assertEqual(body["error"], "BundleMismatchError")

    def test_exact_lookup_response_is_compact_utf8(self) -> None:
        self._submit("键")
        quoted = urllib.parse.quote("键")
        status, raw = self._get("/acceptance?key=" + quoted,
                                headers={"X-Audit-Token": FULL_TOKEN})
        self.assertEqual(status, 200)
        record = acceptance.get(self.ledger_dir, "键")
        self.assertEqual(raw, json.dumps(
            record, ensure_ascii=False,
            separators=(",", ":")).encode("utf-8"))
        self.assertFalse(raw.endswith(b"\n"))

    # -- pagination --------------------------------------------------------------

    def test_pagination_follows_the_library_page_object(self) -> None:
        self._populate()
        status, body = self._token_get(FULL_TOKEN, "/acceptance?limit=2")
        self.assertEqual(status, 200)
        self.assertEqual(list(body), ["entries", "next"])
        self.assertEqual([key for key, _ in body["entries"]], ["k1", "k2"])
        self.assertEqual(body["next"], "k2")
        self.assertEqual(body, acceptance.search(self.ledger_dir, limit=2))

        status, body = self._token_get(
            FULL_TOKEN, "/acceptance?limit=2&cursor=k2")
        self.assertEqual([key for key, _ in body["entries"]], ["k3"])
        self.assertIsNone(body["next"])

        status, body = self._token_get(FULL_TOKEN,
                                       "/acceptance?state=quarantined")
        self.assertEqual([key for key, _ in body["entries"]], ["k3"])
        status, body = self._token_get(FULL_TOKEN, "/acceptance?state=pending")
        self.assertEqual(body, {"entries": [], "next": None})

    def test_pagination_response_is_compact_utf8(self) -> None:
        self._submit("键")
        status, raw = self._get("/acceptance",
                                headers={"X-Audit-Token": FULL_TOKEN})
        self.assertEqual(status, 200)
        page = acceptance.search(self.ledger_dir)
        self.assertEqual(raw, json.dumps(
            page, ensure_ascii=False,
            separators=(",", ":")).encode("utf-8"))

    # -- conditional reads ---------------------------------------------------

    def _get_h(self, path: str, headers: dict[str, str] | None = None,
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

    def test_200_carries_strong_etag_of_the_response_bytes(self) -> None:
        self._populate()
        for path in ("/acceptance?key=k1", "/acceptance?limit=2"):
            with self.subTest(path=path):
                status, body, headers = self._get_h(
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
        for path in ("/acceptance?key=k1", "/acceptance"):
            with self.subTest(path=path):
                _, _, headers = self._get_h(
                    path, headers={"X-Audit-Token": FULL_TOKEN})
                tag = headers.get("ETag")
                status, body, headers = self._get_h(
                    path, headers={"X-Audit-Token": FULL_TOKEN,
                                   "If-None-Match": tag})
                self.assertEqual(status, 304)
                self.assertEqual(body, b"")
                self.assertEqual(headers.get("ETag"), tag)

    def test_non_matching_if_none_match_is_200_with_full_body(self) -> None:
        self._populate()
        other = '"' + "0" * 64 + '"'
        status, body, headers = self._get_h(
            "/acceptance?key=k1",
            headers={"X-Audit-Token": FULL_TOKEN, "If-None-Match": other})
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body),
                         acceptance.get(self.ledger_dir, "k1"))
        self.assertNotEqual(headers.get("ETag"), other)

    def test_etag_changes_with_visible_record_order_and_cursor(self) -> None:
        self._submit("k1")
        _, _, headers = self._get_h(
            "/acceptance", headers={"X-Audit-Token": FULL_TOKEN})
        first_tag = headers.get("ETag")
        # A new visible record changes the page's tag.
        self._submit("k2")
        _, body, headers = self._get_h(
            "/acceptance", headers={"X-Audit-Token": FULL_TOKEN})
        self.assertNotEqual(headers.get("ETag"), first_tag)
        # A different cursor is a different response with its own tag.
        _, cursor_body, cursor_headers = self._get_h(
            "/acceptance?cursor=k1",
            headers={"X-Audit-Token": FULL_TOKEN})
        self.assertNotEqual(cursor_body, body)
        self.assertEqual(
            cursor_headers.get("ETag"),
            '"' + hashlib.sha256(cursor_body).hexdigest() + '"')

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
                    "/acceptance",
                    headers={"X-Audit-Token": FULL_TOKEN,
                             "If-None-Match": value})
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})
        tag = f'"{good}"'
        status, body = self._json_get(
            "/acceptance",
            raw_headers=[("X-Audit-Token", FULL_TOKEN),
                         ("If-None-Match", tag), ("If-None-Match", tag)])
        self.assertEqual(status, 400)
        self.assertEqual(body, {"error": "invalid_request"})

    def test_conditional_validation_order(self) -> None:
        self._populate()
        # Authorization precedes conditional validation.
        self.assertEqual(self._json_get(
            "/acceptance", headers={"If-None-Match": "bad"})[0], 401)
        # Conditional validation precedes the scope decision.
        self.assertEqual(self._json_get(
            "/acceptance", headers={"X-Audit-Token": OPS_TOKEN,
                                    "If-None-Match": "bad"})[0], 400)
        # A valid conditional does not rescue a scoped-out request.
        self.assertEqual(self._json_get(
            "/acceptance", headers={
                "X-Audit-Token": OPS_TOKEN,
                "If-None-Match": '"' + "0" * 64 + '"'})[0], 403)

    # -- ledger state errors ------------------------------------------------------

    def test_missing_ledger_is_404(self) -> None:
        status, body = self._token_get(FULL_TOKEN, "/acceptance?key=k1")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "acceptance_not_found"})
        status, body = self._token_get(FULL_TOKEN, "/acceptance")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "acceptance_not_found"})

    def test_unknown_key_is_404(self) -> None:
        self._populate()
        status, body = self._token_get(FULL_TOKEN, "/acceptance?key=nope")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "acceptance_key_not_found"})

    def test_invalid_ledger_is_409(self) -> None:
        os.mkdir(self.ledger_dir)
        with open(os.path.join(self.ledger_dir, "ledger.json"),
                  "wb") as handle:
            handle.write(b"{not json")
        status, body = self._token_get(FULL_TOKEN, "/acceptance?key=k1")
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "acceptance_invalid"})
        status, body = self._token_get(FULL_TOKEN, "/acceptance")
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "acceptance_invalid"})

    def test_io_failure_is_503(self) -> None:
        os.mkdir(self.ledger_dir)
        # A directory where the ledger file should be: the open fails.
        os.mkdir(os.path.join(self.ledger_dir, "ledger.json"))
        status, body = self._token_get(FULL_TOKEN, "/acceptance?key=k1")
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "acceptance_unavailable"})

    # -- unchanged surface ---------------------------------------------------------

    def test_health_and_unknown_paths_unchanged(self) -> None:
        self.assertEqual(self._json_get("/health"), (200, {"status": "ok"}))
        self.assertEqual(self._json_get("/missing"),
                         (404, {"error": "not_found"}))
        self.assertEqual(self._json_get("/acceptance/"),
                         (404, {"error": "not_found"}))


class AcceptanceDisabledTest(unittest.TestCase):
    def test_acceptance_is_plain_404_without_configuration(self) -> None:
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connection = HTTPConnection("127.0.0.1", server.server_port,
                                        timeout=2)
            connection.request("GET", "/acceptance")
            response = connection.getresponse()
            self.assertEqual(response.status, 404)
            self.assertEqual(json.loads(response.read()),
                             {"error": "not_found"})
            connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


class ServeAcceptanceArgumentsTest(unittest.TestCase):
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

    def test_misplaced_empty_or_repeated_acceptance_exits_2(self) -> None:
        ledger = os.path.join(self.tmp.name, "ledger")
        for args in (
            ("--acceptance", ledger),                 # no --audit
            ("--audit", "j", "--acceptance", ledger),  # no method
            ("--audit", "j", "--token", "t",
             "--acceptance", ledger),                  # single-token mode
            ("--audit", "j", "--auth", self.config,
             "--acceptance", ""),                      # empty value
            ("--audit", "j", "--auth", self.config,
             "--acceptance", ledger, "--acceptance", ledger),  # repeated
        ):
            with self.subTest(args=args):
                self.assertEqual(self._run(*args).returncode, 2)

    def test_valid_acceptance_starts_and_serves(self) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        process = subprocess.Popen(
            [sys.executable, "-m", "carbon_market", "serve",
             "--host", "127.0.0.1", "--port", str(port),
             "--audit", os.path.join(self.tmp.name, "audit.json"),
             "--auth", self.config,
             "--acceptance", os.path.join(self.tmp.name, "ledger")])
        try:
            deadline = time.time() + 10
            statuses = []
            while time.time() < deadline:
                try:
                    connection = HTTPConnection("127.0.0.1", port, timeout=1)
                    connection.request("GET", "/acceptance",
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
                statuses, [(404, {"error": "acceptance_not_found"})])
        finally:
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
