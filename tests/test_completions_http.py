"""HTTP tests for the read-only GET /completions endpoint.

Covers the multi-token authorization (401/403/503 auth_unavailable),
the exact-job and paginated query shapes, the scope rules (exact job
lookup needs unrestricted operation and stage scopes and a key range
that names the requested job id, pagination needs all three scopes
unrestricted), the outcome/exceeded filters, the error mapping (400
invalid_request, 404 completion_not_found, 404
completion_job_not_found, 409 completion_invalid, 503
completion_unavailable), the strong ETag conditional reads and the
``serve`` command's ``--completions`` option, which only ever
accompanies ``--audit`` together with ``--auth``, never ``--token``.
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
from carbon_market import dispatch as dispatch_module
from carbon_market import execution as execution_module
from carbon_market import execution_sync as execution_sync_module
from carbon_market import jobs as jobs_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.market import clear_live
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


def _resource(resource_id: str = "r-1", **overrides: object) -> dict:
    resource = {
        "resource_id": resource_id,
        "region": "eu-north",
        "capacity": 100,
        "start": 0,
        "end": 5000,
        "unit_cost": 5,
        "carbon_intensity": 8,
        "residency": ["eu-north"],
    }
    resource.update(overrides)
    return resource


def _signal(**overrides: object) -> dict:
    signal = {
        "region": "eu-north",
        "observed": 0,
        "expires": 5000,
        "mix": {"solar": 10000},
        "unit_cost": 5,
        "carbon_intensity": 8,
    }
    signal.update(overrides)
    return signal


def _job(job_id: str, **overrides: object) -> dict:
    job = {
        "job_id": job_id,
        "work": 10,
        "deadline": 100,
        "regions": ["eu-north"],
        "residency": ["eu-north"],
        "max_cost": 1000,
        "carbon_cap": 1000,
    }
    job.update(overrides)
    return job


class CompletionHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        base = self.tmp.name
        self.journal = os.path.join(base, "audit.json")
        self.jobs = os.path.join(base, "jobs.json")
        self.supply = os.path.join(base, "supply.json")
        self.signals = os.path.join(base, "signals.json")
        self.trades = os.path.join(base, "trades.json")
        self.dispatch = os.path.join(base, "dispatch.json")
        self.execution = os.path.join(base, "execution.json")
        self.sync = os.path.join(base, "sync.json")
        self.completions = os.path.join(base, "completions.json")
        self.config = os.path.join(base, "auth.jsonl")
        self._write_config(
            _line("full", FULL_TOKEN, None) + "\n"
            + _line("key", KEY_TOKEN, None, keys=["j-1"]) + "\n"
            + _line("ops", OPS_TOKEN, None, ops=["copy"]) + "\n")
        jobs_module.submit(self.jobs, _job("placeholder"), "jk-0")
        resources_module.publish(self.supply, _resource(), "rk-1")
        signals_module.publish(self.signals, _signal(), "sk-1")

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.audit_path = self.journal
        self.server.audit_token = None
        self.server.audit_auth = self.config
        self.server.completions_path = self.completions
        thread = threading.Thread(target=self.server.serve_forever,
                                  daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)

    def _write_config(self, text: str) -> None:
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

    def _token_get(self, token: str, path: str = "/completions"):
        return self._json_get(path, headers={"X-Audit-Token": token})

    def _get_h(self, path: str, headers: dict[str, str] | None = None):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        connection.request("GET", path, headers=headers or {})
        response = connection.getresponse()
        body = response.read()
        result = (response.status, body, response.headers)
        self.addCleanup(connection.close)
        return result

    # -- ledger fixture -------------------------------------------------------

    def _complete_job(self, index: int, job_id: str, outcome: str,
                      cost: int, carbon: int, *, at: int | None = None,
                      work: int = 1) -> None:
        jobs_module.submit(self.jobs, _job(job_id, work=work),
                           f"jk-{index}")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   job_id, f"t-{index}", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, job_id, f"d-{index}", 10)
        dispatch_module.claim(self.dispatch, job_id, f"c-{index}",
                              f"owner-{index}", 30, 60)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, job_id,
                              f"p-{index}", f"owner-{index}", None, 61)
        execution_module.record(self.execution, job_id, 1, f"s1-{index}",
                                f"owner-{index}", "stage", "succeeded",
                                "staged", 62 + index)
        execution_module.record(self.execution, job_id, 1, f"s2-{index}",
                                f"owner-{index}", "start", "succeeded",
                                "started", 63 + index)
        moment = 70 + index if at is None else at
        execution_sync_module.run(self.execution, self.dispatch, self.sync,
                                  "sync-1", f"b-{index}", moment, 50)
        completion.complete(
            self.jobs, self.supply, self.signals, self.trades,
            self.dispatch, self.execution, self.completions, job_id,
            f"x-{index}", moment, outcome, cost, carbon)

    def _populate(self) -> None:
        # j-1 succeeded within budget; j-2 failed, cost exceeded; j-3
        # succeeded with both flags; j-东 succeeded with carbon only.
        self._complete_job(1, "j-1", "succeeded", 90, 90)
        self._complete_job(2, "j-2", "failed", 1001, 90)
        self._complete_job(3, "j-3", "succeeded", 1001, 1001)
        self._complete_job(4, "j-东", "succeeded", 90, 1001)

    # -- authorization ---------------------------------------------------------

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

    def test_unknown_and_expired_token_are_403_without_reading_ledger(self):
        # No ledger exists; a 403 (not 404) proves it was never read.
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

    # -- query parameter validation --------------------------------------------

    def test_invalid_parameters_are_400(self) -> None:
        for path in (
            "/completions?bogus=1",
            "/completions?job=j-1&job=j-2",     # repeated job
            "/completions?job=",                # empty job
            "/completions?cursor=",             # empty cursor
            "/completions?outcome=",            # empty outcome
            "/completions?exceeded=",           # empty exceeded
            "/completions?job=j-1&cursor=j-0",  # job with cursor
            "/completions?job=j-1&limit=10",    # job with page size
            "/completions?job=j-1&outcome=failed",  # job with filter
            "/completions?job=j-1&exceeded=any",    # job with filter
            "/completions?outcome=unknown",
            "/completions?exceeded=both",
            "/completions?exceeded=ANY",
            "/completions?limit=0",
            "/completions?limit=1001",
            "/completions?limit=-1",
            "/completions?limit=abc",
            "/completions?cursor=%FF%FE",       # not UTF-8
        ):
            with self.subTest(path=path):
                status, body = self._token_get(FULL_TOKEN, path)
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "invalid_request"})

    # -- scopes ------------------------------------------------------------------

    def test_exact_lookup_scopes(self) -> None:
        self._populate()
        # A restricted operation or stage scope forbids the lookup
        # entirely, whatever the key scope says.
        self.assertEqual(
            self._token_get(OPS_TOKEN, "/completions?job=j-1")[0], 403)
        # The key range must name the requested job id explicitly.
        status, body = self._token_get(KEY_TOKEN, "/completions?job=j-1")
        self.assertEqual(status, 200)
        self.assertEqual(body["job_id"], "j-1")
        self.assertEqual(
            self._token_get(KEY_TOKEN, "/completions?job=j-2")[0], 403)

    def test_pagination_requires_unrestricted_scopes(self) -> None:
        self._populate()
        for token in (KEY_TOKEN, OPS_TOKEN):
            with self.subTest(token=token):
                status, body = self._token_get(token, "/completions")
                self.assertEqual(status, 403)
                self.assertEqual(body, {"error": "forbidden"})
        self.assertEqual(
            self._token_get(FULL_TOKEN, "/completions")[0], 200)

    def test_scope_403_never_opens_the_ledger(self) -> None:
        # No ledger exists: a scoped-out request still gets 403, not 404.
        self.assertEqual(
            self._token_get(OPS_TOKEN, "/completions?job=j-1")[0], 403)
        self.assertEqual(self._token_get(KEY_TOKEN, "/completions")[0], 403)

    # -- exact lookup ------------------------------------------------------------

    def test_exact_lookup_returns_the_record(self) -> None:
        self._populate()
        status, body = self._token_get(FULL_TOKEN,
                                       "/completions?job=j-2")
        self.assertEqual(status, 200)
        self.assertEqual(list(body),
                         ["job_id", "at", "outcome", "actual_cost",
                          "actual_carbon", "generation", "current",
                          "cost_exceeded", "carbon_exceeded"])
        self.assertEqual(body, completion.get(self.completions, "j-2"))
        self.assertTrue(body["cost_exceeded"])
        self.assertFalse(body["carbon_exceeded"])

    def test_exact_lookup_body_is_compact_utf8(self) -> None:
        self._populate()
        quoted = urllib.parse.quote("j-东")
        status, raw = self._get("/completions?job=" + quoted,
                                headers={"X-Audit-Token": FULL_TOKEN})
        self.assertEqual(status, 200)
        self.assertEqual(
            raw, json.dumps(completion.get(self.completions, "j-东"),
                            ensure_ascii=False,
                            separators=(",", ":")).encode("utf-8"))
        self.assertFalse(raw.endswith(b"\n"))
        self.assertNotIn(b"\\u", raw)

    # -- pagination and filters --------------------------------------------------

    def test_pagination_follows_the_library_page_object(self) -> None:
        self._populate()
        status, body = self._token_get(FULL_TOKEN,
                                       "/completions?limit=2")
        self.assertEqual(status, 200)
        self.assertEqual(list(body), ["entries", "next"])
        self.assertEqual([j for j, _ in body["entries"]],
                         ["j-1", "j-2"])
        self.assertEqual(body["next"], "j-2")
        self.assertEqual(body, completion.search(self.completions,
                                                 limit=2))

        status, body = self._token_get(
            FULL_TOKEN, "/completions?limit=2&cursor=j-2")
        self.assertEqual([j for j, _ in body["entries"]], ["j-3", "j-东"])
        self.assertIsNone(body["next"])

    def test_filters_apply(self) -> None:
        self._populate()
        status, body = self._token_get(
            FULL_TOKEN, "/completions?outcome=failed")
        self.assertEqual([j for j, _ in body["entries"]], ["j-2"])
        status, body = self._token_get(
            FULL_TOKEN, "/completions?exceeded=cost")
        self.assertEqual([j for j, _ in body["entries"]], ["j-2", "j-3"])
        status, body = self._token_get(
            FULL_TOKEN, "/completions?exceeded=carbon")
        self.assertEqual([j for j, _ in body["entries"]], ["j-3", "j-东"])
        status, body = self._token_get(
            FULL_TOKEN, "/completions?exceeded=any&outcome=succeeded")
        self.assertEqual([j for j, _ in body["entries"]], ["j-3", "j-东"])
        status, body = self._token_get(
            FULL_TOKEN, "/completions?exceeded=none")
        self.assertEqual([j for j, _ in body["entries"]], ["j-1"])

    def test_list_body_is_compact_utf8(self) -> None:
        self._populate()
        status, raw = self._get("/completions?exceeded=carbon",
                                headers={"X-Audit-Token": FULL_TOKEN})
        self.assertEqual(status, 200)
        page = completion.search(self.completions, exceeded="carbon")
        self.assertEqual(
            raw, json.dumps(page, ensure_ascii=False,
                            separators=(",", ":")).encode("utf-8"))

    # -- conditional reads -------------------------------------------------------

    def test_200_carries_strong_etag_of_the_response_bytes(self) -> None:
        self._populate()
        for path in ("/completions?job=j-1", "/completions?limit=2",
                     "/completions?exceeded=any"):
            with self.subTest(path=path):
                status, body, headers = self._get_h(
                    path, headers={"X-Audit-Token": FULL_TOKEN})
                self.assertEqual(status, 200)
                self.assertEqual(
                    headers.get("ETag"),
                    '"' + hashlib.sha256(body).hexdigest() + '"')
                self.assertFalse(body.endswith(b"\n"))

    def test_matching_if_none_match_is_304_empty_with_same_etag(self):
        self._populate()
        for path in ("/completions?job=j-1", "/completions"):
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
            "/completions?job=j-1",
            headers={"X-Audit-Token": FULL_TOKEN, "If-None-Match": other})
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body),
                         completion.get(self.completions, "j-1"))
        self.assertNotEqual(headers.get("ETag"), other)

    def test_invalid_conditional_headers_are_400_without_reading(self):
        # No ledger exists: a 400 (not 404) proves the ledger was never
        # read.
        good = hashlib.sha256(b"x").hexdigest()
        for value in ("", " ", f'"{good[:63]}"', f'"{good}a"',
                      f'"{good.upper()}"', f'"{"g" * 64}"', good,
                      f'"{good}", "{good}"', "*", f'W/"{good}"'):
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
        self.assertEqual(self._json_get(
            "/completions", headers={"If-None-Match": "bad"})[0], 401)
        self.assertEqual(self._json_get(
            "/completions", headers={"X-Audit-Token": OPS_TOKEN,
                                     "If-None-Match": "bad"})[0], 400)
        self.assertEqual(self._json_get(
            "/completions", headers={
                "X-Audit-Token": OPS_TOKEN,
                "If-None-Match": '"' + "0" * 64 + '"'})[0], 403)

    # -- ledger state errors -----------------------------------------------------

    def test_missing_ledger_is_404_completion_not_found(self) -> None:
        status, body = self._token_get(FULL_TOKEN, "/completions?job=j-1")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "completion_not_found"})
        status, body = self._token_get(FULL_TOKEN, "/completions")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "completion_not_found"})

    def test_unknown_job_is_404_completion_job_not_found(self) -> None:
        self._populate()
        status, body = self._token_get(FULL_TOKEN,
                                       "/completions?job=nope")
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "completion_job_not_found"})

    def test_invalid_ledger_is_409_completion_invalid(self) -> None:
        with open(self.completions, "wb") as handle:
            handle.write(b"{not json\n")
        status, body = self._token_get(FULL_TOKEN, "/completions?job=j-1")
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "completion_invalid"})
        status, body = self._token_get(FULL_TOKEN, "/completions")
        self.assertEqual(status, 409)
        self.assertEqual(body, {"error": "completion_invalid"})

    def test_io_failure_is_503_completion_unavailable(self) -> None:
        # A directory where the ledger file should be: the shared lock
        # opens its own companion file, but reading the ledger path
        # itself fails with a non-FileNotFound OSError.
        os.mkdir(self.completions)
        status, body = self._token_get(FULL_TOKEN, "/completions?job=j-1")
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "completion_unavailable"})
        status, body = self._token_get(FULL_TOKEN, "/completions")
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "completion_unavailable"})

    def test_errors_do_not_leak_paths(self) -> None:
        os.mkdir(self.completions)
        _status, body = self._token_get(FULL_TOKEN, "/completions")
        self.assertNotIn(self.tmp.name, json.dumps(body))
        os.rmdir(self.completions)
        # An empty file is invalid JSON, not a path-bearing message.
        with open(self.completions, "wb"):
            pass
        _status, body = self._token_get(FULL_TOKEN, "/completions?job=j-1")
        self.assertEqual(body, {"error": "completion_invalid"})
        self.assertNotIn(self.tmp.name, json.dumps(body))

    # -- unchanged surface --------------------------------------------------------

    def test_health_and_unknown_paths_unchanged(self) -> None:
        self.assertEqual(self._json_get("/health"), (200, {"status": "ok"}))
        self.assertEqual(self._json_get("/missing"),
                         (404, {"error": "not_found"}))
        self.assertEqual(self._json_get("/completions/"),
                         (404, {"error": "not_found"}))


class CompletionsDisabledTest(unittest.TestCase):
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
            ("--completions", ledger),                    # no --audit
            ("--audit", "j", "--completions", ledger),    # no method
            ("--audit", "j", "--token", "t",
             "--completions", ledger),                    # single-token
            ("--audit", "j", "--auth", self.config,
             "--completions", ""),                        # empty
            ("--audit", "j", "--auth", self.config,
             "--completions", ledger,
             "--completions", ledger),                    # repeated
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
                    connection.request(
                        "GET", "/completions",
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
                statuses,
                [(404, {"error": "completion_not_found"})])
        finally:
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
