"""Cross-endpoint regression tests for the unified request pipeline.

Every fixed-path entry of the server -- the audit and proof queries,
the checkpoint download, the acceptance and completion ledgers, the
migration batch and event queries, the migration consumer status,
dead-letter and operation entries and the process metrics -- runs one
shared pipeline (``Handler._serve`` driven by per-entry ``_Entry``
hooks): authorize, validate the entry's own parameters (and the
conditional header of a conditional entry), decide the token scope,
and only then open the snapshot. These tests pin the shared stage
order, the per-entry parameter shapes and error names, the strong-tag
304 rule, the success wire bytes and the concurrent-read semantics
across all entries at once, so the unified implementation cannot drift
any entry away from the behavior the per-endpoint tests pin
individually.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from tempfile import TemporaryDirectory

from carbon_market import acceptance, audit, audit_proof, completion, \
    migration_batch
from carbon_market.metrics import Metrics
from carbon_market.server import Handler
from tests.test_migration_batch import MigrationBatchTest

FULL_TOKEN = "full-token"
KEY_TOKEN = "key-token"
OPS_TOKEN = "ops-token"

# Every read-only GET entry with one valid and one invalid query
# string; the metrics entry takes no query at all.
GET_ENTRIES = (
    ("/audit", "?limit=5", "?bogus=1"),
    ("/audit/proof", "?generation=g1", "?bogus=1"),
    ("/audit/checkpoint", "", "?limit=1"),
    ("/acceptance", "?limit=5", "?state=bogus"),
    ("/migration-batches", "?limit=5", "?key=aaa&limit=1"),
    ("/migration-batches/events", "?limit=5", "?cursor=-1"),
    ("/migration-consumers/status", "?consumer=c1&now=50", "?consumer=c1"),
    ("/migration-consumers/dead-letters", "?consumer=c1",
     "?consumer=c1&cursor=-1"),
    ("/completions", "?limit=5", "?outcome=bogus"),
    ("/metrics", "", "?x=1"),
)
CONDITIONAL_ENTRIES = ("/audit/checkpoint", "/acceptance", "/completions")


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _line(name: str, token: str, grace_until=None,
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


class _HttpMixin:
    server: ThreadingHTTPServer
    config: str

    def _write_config(self, text: str) -> None:
        # Same-directory atomic replacement, as a rotation would do.
        tmp_path = self.config + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp_path, self.config)

    def _request(self, method: str, path: str,
                 headers: dict[str, str] | None = None,
                 body: bytes | None = None,
                 raw_headers: list[tuple[str, str]] | None = None):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=5)
        if raw_headers is not None:
            connection.putrequest(method, path)
            for name, value in raw_headers:
                connection.putheader(name, value)
            connection.endheaders(body)
        else:
            connection.request(method, path, body=body,
                               headers=headers or {})
        response = connection.getresponse()
        raw = response.read()
        result = (response.status, response.headers, raw)
        connection.close()
        return result

    def _get(self, path: str, token: str | None = FULL_TOKEN,
             extra_headers: dict[str, str] | None = None):
        headers = {} if token is None else {"X-Audit-Token": token}
        if extra_headers:
            headers.update(extra_headers)
        return self._request("GET", path, headers=headers)

    def _post(self, operation: str, body: dict | None = None,
              token: str | None = FULL_TOKEN, query: str = "",
              raw: bytes | None = None):
        headers = {"Content-Type": "application/json"}
        if token is not None:
            headers["X-Audit-Token"] = token
        data = raw if raw is not None \
            else json.dumps(body or {}).encode("utf-8")
        return self._request(
            "POST", f"/migration-consumers/{operation}{query}",
            headers=headers, body=data)


class PipelineStageOrderTest(_HttpMixin, unittest.TestCase):
    """The shared stage order on a fully configured, populated server."""

    def setUp(self) -> None:
        self.fx = MigrationBatchTest(
            "test_get_returns_copy_and_unknown_key_raises")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.fx._prepare_migrate()
        tmp = self.fx.tmp.name
        # The server's own files live in their own subdirectory: the
        # migration snapshot discovery validates any completion-shaped
        # sibling of the business ledgers, so a completions ledger next
        # to them would poison every coordination read.
        self.web = os.path.join(tmp, "web")
        os.mkdir(self.web)
        self.coord = self.fx.paths["coord"]
        self.consumers = os.path.join(tmp, "consumers.json")
        self.journal = os.path.join(self.web, "audit.json")
        self.checkpoint = os.path.join(self.web, "checkpoint.json")
        self.proof_path = os.path.join(self.web, "proof.json")
        self.ledger_dir = os.path.join(self.web, "ledger")
        self.ledger = os.path.join(self.web, "completions.json")
        self.config = os.path.join(tmp, "auth.jsonl")
        self._write_config(
            _line("full", FULL_TOKEN) + "\n"
            + _line("key", KEY_TOKEN, keys=["k1", "j-a", "aaa"]) + "\n"
            + _line("ops", OPS_TOKEN, ops=["copy"]) + "\n")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.audit_path = self.journal
        self.server.audit_token = None
        self.server.audit_auth = self.config
        self.server.audit_checkpoint = self.checkpoint
        self.server.acceptance_dir = self.ledger_dir
        self.server.migration_batches = self.coord
        self.server.migration_consumers = self.consumers
        self.server.completions_path = self.ledger
        self.server.metrics = Metrics(int(time.time()))
        thread = threading.Thread(target=self.server.serve_forever,
                                  daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)
        self._populate()

    def _checkpoint_etag(self) -> str:
        with open(self.checkpoint, "rb") as handle:
            return '"' + hashlib.sha256(handle.read()).hexdigest() + '"'

    def _populate(self) -> None:
        self.fx._run(key="aaa", owner="负责人-1", now=30)
        self.fx._run(key="zzz", now=31)
        audit.record(self.journal, "k1", _event())
        proof = audit_proof.export(self.journal, self.checkpoint, "g1")
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False))
        acceptance.submit(self.checkpoint, self.proof_path,
                          self._checkpoint_etag(), None,
                          self.ledger_dir, "k1")
        with open(self.ledger, "wb") as handle:
            handle.write(_completions_bytes(
                {"x-1": _completion_record("j-a")}))

    def _claim(self) -> None:
        status, _headers, body = self._post(
            "claim", {"consumer": "c1", "owner": "o1", "now": 40,
                      "lease": 100, "idem": "k1"})
        self.assertEqual(status, 200, body)

    # -- stage 1: identity precedes parameters, scope and files --------

    def test_missing_blank_and_duplicate_token_are_401_on_every_entry(
            self) -> None:
        for path, valid, invalid in GET_ENTRIES:
            for target in (path + valid, path + invalid):
                with self.subTest(target=target):
                    _status, _headers, body = self._get(target, token=None)
                    self.assertEqual(json.loads(body),
                                     {"error": "unauthorized"})
                    status, _headers, body = self._get(target, token="  ")
                    # A blank token is unauthorized, not a bad request.
                    self.assertEqual(status, 401)
        with self.subTest(target="POST pull"):
            _status, _headers, body = self._post("pull", token=None)
            self.assertEqual(json.loads(body), {"error": "unauthorized"})
        status, _headers, body = self._request(
            "GET", "/audit?bogus=1",
            raw_headers=[("X-Audit-Token", FULL_TOKEN),
                         ("X-Audit-Token", FULL_TOKEN)])
        self.assertEqual(status, 401)
        self.assertEqual(json.loads(body), {"error": "unauthorized"})

    def test_unknown_token_is_403_on_every_entry(self) -> None:
        for path, valid, invalid in GET_ENTRIES:
            for target in (path + valid, path + invalid):
                with self.subTest(target=target):
                    status, _headers, body = self._get(target, token="nope")
                    self.assertEqual(status, 403)
                    self.assertEqual(json.loads(body),
                                     {"error": "forbidden"})
        status, _headers, body = self._post("pull", token="nope")
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(body), {"error": "forbidden"})

    def test_unreadable_auth_config_is_503_on_every_entry(self) -> None:
        os.unlink(self.config)
        for path, valid, _invalid in GET_ENTRIES:
            with self.subTest(path=path):
                status, _headers, body = self._get(path + valid)
                self.assertEqual(status, 503)
                self.assertEqual(json.loads(body),
                                 {"error": "auth_unavailable"})
        status, _headers, body = self._post("pull")
        self.assertEqual(status, 503)
        self.assertEqual(json.loads(body), {"error": "auth_unavailable"})

    # -- stage 2: parameters precede the scope decision -----------------

    def test_invalid_parameters_are_400_on_every_entry(self) -> None:
        for path, _valid, invalid in GET_ENTRIES:
            with self.subTest(path=path):
                status, _headers, body = self._get(path + invalid)
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(body),
                                 {"error": "invalid_request"})
        # The metrics entry rejects even a bare '?' with no parameters.
        status, _headers, body = self._get("/metrics?")
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body), {"error": "invalid_request"})
        # The consumer operations reject any query string and any body
        # outside the fixed field set.
        status, _headers, body = self._post(
            "pull", {"consumer": "c1", "owner": "o1", "now": 1},
            query="?x=1")
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body), {"error": "invalid_request"})
        status, _headers, body = self._post("pull", {"consumer": "c1"})
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body), {"error": "invalid_request"})
        status, _headers, body = self._post(
            "pull", raw=b'{"consumer":"c1","consumer":"c2"}')
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body), {"error": "invalid_request"})

    def test_parameter_validation_precedes_scope_on_every_entry(
            self) -> None:
        # A scope-restricted token with an invalid query string gets the
        # parameter 400, not the scope 403.
        for path, _valid, invalid in GET_ENTRIES:
            with self.subTest(path=path):
                status, _headers, body = self._get(path + invalid,
                                                   token=OPS_TOKEN)
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(body),
                                 {"error": "invalid_request"})

    # -- stage 3: scope precedes any snapshot read ----------------------

    def test_scoped_token_is_403_on_every_entry(self) -> None:
        # An operation-scoped token is rejected on every entry; the
        # consumer it names does not exist, so the 403 (not a 404)
        # proves the scope decision precedes the subscription read.
        for path, valid, _invalid in GET_ENTRIES:
            with self.subTest(path=path):
                status, _headers, body = self._get(path + valid,
                                                   token=OPS_TOKEN)
                self.assertEqual(status, 403)
                self.assertEqual(json.loads(body), {"error": "forbidden"})
        status, _headers, body = self._post(
            "pull", {"consumer": "c1", "owner": "o1", "now": 1},
            token=OPS_TOKEN)
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(body), {"error": "forbidden"})

    def test_key_scope_matches_only_the_named_target(self) -> None:
        # The shared exact-or-paginated scope rule: a key-scoped token
        # reads an allowed exact lookup but never a paginated query.
        status, _headers, _body = self._get("/acceptance?key=k1",
                                            token=KEY_TOKEN)
        self.assertEqual(status, 200)
        status, _headers, body = self._get("/acceptance?key=k2",
                                           token=KEY_TOKEN)
        self.assertEqual(status, 403)
        self.assertEqual(json.loads(body), {"error": "forbidden"})
        for path in ("/acceptance?limit=1", "/completions?limit=1",
                     "/migration-batches?limit=1",
                     "/migration-batches/events?limit=1",
                     "/audit/checkpoint"):
            with self.subTest(path=path):
                status, _headers, body = self._get(path, token=KEY_TOKEN)
                self.assertEqual(status, 403)
                self.assertEqual(json.loads(body), {"error": "forbidden"})

    # -- stage 4: the snapshot read and its success response ------------

    def test_success_wire_bytes_match_the_library_on_every_entry(
            self) -> None:
        # Each entry's body is exactly what the underlying read-only
        # library builder produced under its locks.
        status, _headers, body = self._get("/audit?limit=5")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body),
                         audit.search(self.journal, limit=5))

        status, _headers, body = self._get("/audit/proof?generation=g1")
        self.assertEqual(status, 200)
        self.assertEqual(
            json.loads(body),
            audit_proof.export(self.journal, self.checkpoint, "g1",
                               final=False))

        with open(self.checkpoint, "rb") as handle:
            raw_checkpoint = handle.read()
        status, headers, body = self._get("/audit/checkpoint")
        self.assertEqual(status, 200)
        self.assertEqual(body, raw_checkpoint)
        self.assertEqual(headers.get("ETag"),
                         '"' + hashlib.sha256(body).hexdigest() + '"')

        status, headers, body = self._get("/acceptance?key=k1")
        self.assertEqual(status, 200)
        self.assertEqual(body, acceptance.get_response(
            self.ledger_dir, "k1", None)[0])
        self.assertEqual(headers.get("ETag"),
                         '"' + hashlib.sha256(body).hexdigest() + '"')

        status, headers, body = self._get("/completions?job=j-a")
        self.assertEqual(status, 200)
        self.assertEqual(body, completion.get_response(
            self.ledger, "j-a", None)[0])
        self.assertEqual(headers.get("ETag"),
                         '"' + hashlib.sha256(body).hexdigest() + '"')

        status, _headers, body = self._get("/migration-batches?key=aaa")
        self.assertEqual(status, 200)
        self.assertEqual(body, migration_batch.get_response(
            self.coord, "aaa"))

        status, _headers, body = self._get(
            "/migration-batches/events?limit=5")
        self.assertEqual(status, 200)
        self.assertEqual(body, migration_batch.events_response(
            self.coord, limit=5))

        # The consumer entries: claim, then the status, dead-letter and
        # pull reads of that consumer.
        self._claim()
        status, _headers, body = self._get(
            "/migration-consumers/status?consumer=c1&now=50")
        self.assertEqual(status, 200)
        self.assertEqual(body, migration_batch.consumer_status_response(
            self.coord, self.consumers, "c1", 50))

        status, _headers, body = self._get(
            "/migration-consumers/dead-letters?consumer=c1")
        self.assertEqual(status, 200)
        self.assertEqual(
            body, migration_batch.consumer_dead_letters_response(
                self.coord, self.consumers, "c1", cursor=None,
                limit=100))

        status, _headers, body = self._post(
            "pull", {"consumer": "c1", "owner": "o1", "now": 41})
        self.assertEqual(status, 200)
        self.assertIn(b'"events"', body)

        status, _headers, body = self._get("/metrics")
        self.assertEqual(status, 200)
        snapshot = json.loads(body)
        self.assertEqual(snapshot["version"], 1)
        self.assertEqual(list(snapshot),
                         ["version", "started", "total", "routes"])

        status, _headers, body = self._request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(body, b'{"status":"ok"}')

    def test_304_rule_on_every_conditional_entry(self) -> None:
        stale = '"' + "0" * 64 + '"'
        for path in CONDITIONAL_ENTRIES:
            with self.subTest(path=path):
                status, headers, body = self._get(path)
                self.assertEqual(status, 200)
                tag = headers.get("ETag")
                self.assertEqual(
                    tag, '"' + hashlib.sha256(body).hexdigest() + '"')
                # A matching condition is a 304 with an empty body and
                # the same tag; a stale one is the full 200 again.
                status, headers, not_modified = self._get(
                    path, extra_headers={"If-None-Match": tag})
                self.assertEqual(status, 304)
                self.assertEqual(not_modified, b"")
                self.assertEqual(headers.get("ETag"), tag)
                self.assertEqual(headers.get("Content-Length"), "0")
                status, _headers, again = self._get(
                    path, extra_headers={"If-None-Match": stale})
                self.assertEqual(status, 200)
                self.assertEqual(again, body)

    def test_unknown_exact_key_keeps_each_entrys_own_404(self) -> None:
        expected = (
            ("/acceptance?key=k2", "acceptance_key_not_found"),
            ("/completions?job=j-b", "completion_job_not_found"),
            ("/migration-batches?key=k2", "migration_batch_not_found"),
            ("/migration-consumers/status?consumer=c2&now=1",
             "migration_consumer_not_found"),
            ("/migration-consumers/dead-letters?consumer=c2",
             "migration_consumer_not_found"),
        )
        for path, error in expected:
            with self.subTest(path=path):
                status, _headers, body = self._get(path)
                self.assertEqual(status, 404)
                self.assertEqual(json.loads(body), {"error": error})
                self.assertNotIn(self.fx.tmp.name.encode(), body)

    # -- concurrency: one complete version per response -----------------

    def test_racing_reads_never_mix_an_old_tag_with_a_new_body(
            self) -> None:
        results: list[tuple[int, str | None, bytes]] = []
        errors: list[BaseException] = []

        def reader(path: str) -> None:
            try:
                for _ in range(15):
                    results.append(self._get(path))
            except BaseException as exc:  # pragma: no cover - failure
                errors.append(exc)

        def acceptance_writer() -> None:
            try:
                for index in range(5):
                    acceptance.submit(self.checkpoint, self.proof_path,
                                      self._checkpoint_etag(), None,
                                      self.ledger_dir, f"w{index}")
            except BaseException as exc:  # pragma: no cover - failure
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
            except BaseException as exc:  # pragma: no cover - failure
                errors.append(exc)

        threads = [threading.Thread(target=reader, args=(path,))
                   for path in CONDITIONAL_ENTRIES for _ in range(2)]
        threads += [threading.Thread(target=acceptance_writer),
                    threading.Thread(target=completions_writer)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertTrue(results)
        for status, headers, body in results:
            self.assertEqual(status, 200)
            # Whatever version was served, the tag summarizes exactly
            # the bytes in hand -- never an old tag over a new body.
            self.assertEqual(
                headers.get("ETag"),
                '"' + hashlib.sha256(body).hexdigest() + '"')
            json.loads(body)

    def test_metrics_count_every_request_exactly_once_under_concurrency(
            self) -> None:
        self._claim()
        # A deterministic request mix across the unified pipeline: each
        # thread observes its own statuses, and the final snapshot must
        # match the client-observed counts exactly -- no request lost,
        # none counted twice, and the metrics read itself excluded.
        script = (
            ("GET", "/health", None, 200),
            ("GET", "/audit?limit=1", FULL_TOKEN, 200),
            ("GET", "/audit/checkpoint", FULL_TOKEN, 200),
            ("GET", "/completions?limit=1", FULL_TOKEN, 200),
            ("GET", "/migration-batches?limit=1", FULL_TOKEN, 200),
            ("GET", "/migration-consumers/status?consumer=c1&now=50",
             FULL_TOKEN, 200),
            ("POST", "/migration-consumers/pull", FULL_TOKEN, 200),
            ("GET", "/missing", None, 404),
            ("GET", "/audit", None, 401),
            ("GET", "/audit?bogus=1", FULL_TOKEN, 400),
            ("GET", "/audit?limit=1", OPS_TOKEN, 403),
        )
        observed: dict[tuple[str, int], int] = {}
        lock = threading.Lock()
        errors: list[BaseException] = []
        known_routes = {
            "/health", "/audit", "/audit/checkpoint", "/completions",
            "/migration-batches", "/migration-consumers/status",
            "/migration-consumers/pull", "/migration-consumers/claim",
        }

        def classify(path: str) -> str:
            route = path.partition("?")[0]
            return route if route in known_routes else "other"

        def worker(worker_id: int) -> None:
            local: dict[tuple[str, int], int] = {}
            try:
                for i in range(6):
                    method, path, token, expected = \
                        script[(worker_id + i) % len(script)]
                    headers = ({} if token is None
                               else {"X-Audit-Token": token})
                    body = None
                    if method == "POST":
                        headers["Content-Type"] = "application/json"
                        body = json.dumps(
                            {"consumer": "c1", "owner": "o1",
                             "now": 41}).encode("utf-8")
                    connection = HTTPConnection(
                        "127.0.0.1", self.server.server_port, timeout=5)
                    connection.request(method, path, body=body,
                                       headers=headers)
                    response = connection.getresponse()
                    response.read()
                    connection.close()
                    self.assertEqual(response.status, expected)
                    key = (classify(path), response.status)
                    local[key] = local.get(key, 0) + 1
            except BaseException as exc:  # pragma: no cover - failure
                errors.append(exc)
            with lock:
                for key, count in local.items():
                    observed[key] = observed.get(key, 0) + count

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])

        status, _headers, body = self._get("/metrics")
        self.assertEqual(status, 200)
        snapshot = json.loads(body)
        # The claim in this test and every worker request are counted;
        # this final read is not in its own snapshot.
        expected = dict(observed)
        claim_key = ("/migration-consumers/claim", 200)
        expected[claim_key] = expected.get(claim_key, 0) + 1
        self.assertEqual(snapshot["total"], sum(expected.values()))
        for (route, code), count in expected.items():
            with self.subTest(route=route, code=code):
                self.assertEqual(
                    snapshot["routes"][route]["statuses"][str(code)],
                    count)


class PipelineErrorMappingTest(_HttpMixin, unittest.TestCase):
    """Each entry keeps its own error names through the shared mapping."""

    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.checkpoint = os.path.join(self.tmp.name, "checkpoint.json")
        self.ledger_dir = os.path.join(self.tmp.name, "ledger")
        self.ledger = os.path.join(self.tmp.name, "completions.json")
        self.coord = os.path.join(self.tmp.name, "coord.json")
        self.consumers = os.path.join(self.tmp.name, "consumers.json")
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        self._write_config(_line("full", FULL_TOKEN) + "\n")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.audit_path = self.journal
        self.server.audit_token = None
        self.server.audit_auth = self.config
        self.server.audit_checkpoint = self.checkpoint
        self.server.acceptance_dir = self.ledger_dir
        self.server.migration_batches = self.coord
        self.server.migration_consumers = self.consumers
        self.server.completions_path = self.ledger
        self.server.metrics = Metrics(int(time.time()))
        thread = threading.Thread(target=self.server.serve_forever,
                                  daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)

    def _assert_error(self, path: str, status: int, error: str) -> None:
        # The error object is the whole response: no configured path,
        # no token and no system message ever leaks.
        actual_status, _headers, body = self._get(path)
        self.assertEqual(actual_status, status)
        self.assertEqual(json.loads(body), {"error": error})
        self.assertNotIn(self.tmp.name.encode(), body)

    def test_missing_files_keep_each_entrys_own_404(self) -> None:
        expected = (
            ("/audit", "audit_not_found"),
            ("/audit/proof?generation=g1", "proof_not_found"),
            ("/audit/checkpoint", "checkpoint_not_found"),
            ("/acceptance", "acceptance_not_found"),
            ("/completions", "completion_not_found"),
            ("/migration-batches", "migration_batches_not_found"),
            ("/migration-batches/events", "migration_batches_not_found"),
            ("/migration-consumers/status?consumer=c1&now=1",
             "migration_consumer_not_found"),
            ("/migration-consumers/dead-letters?consumer=c1",
             "migration_consumer_not_found"),
        )
        for path, error in expected:
            with self.subTest(path=path):
                self._assert_error(path, 404, error)

    def test_corrupt_files_keep_each_entrys_own_409(self) -> None:
        os.mkdir(self.ledger_dir)
        for path in (self.journal, self.checkpoint,
                     os.path.join(self.ledger_dir, "ledger.json"),
                     self.ledger, self.coord, self.consumers):
            with open(path, "wb") as handle:
                handle.write(b"{not json")
        expected = (
            ("/audit", "audit_invalid"),
            ("/audit/proof?generation=g1", "proof_invalid"),
            ("/audit/checkpoint", "checkpoint_invalid"),
            ("/acceptance", "acceptance_invalid"),
            ("/completions", "completion_invalid"),
            ("/migration-batches", "migration_batches_invalid"),
            ("/migration-batches/events", "migration_batches_invalid"),
            ("/migration-consumers/status?consumer=c1&now=1",
             "migration_consumers_invalid"),
            ("/migration-consumers/dead-letters?consumer=c1",
             "migration_consumers_invalid"),
        )
        for path, error in expected:
            with self.subTest(path=path):
                self._assert_error(path, 409, error)

    def test_io_failure_keeps_each_entrys_own_503(self) -> None:
        # A directory where each file should be: the open fails with an
        # OSError that is not FileNotFoundError.
        os.mkdir(self.journal)
        os.mkdir(self.checkpoint)
        os.mkdir(self.ledger_dir)
        os.mkdir(os.path.join(self.ledger_dir, "ledger.json"))
        os.mkdir(self.ledger)
        os.mkdir(self.coord)
        os.mkdir(self.consumers)
        expected = (
            ("/audit", "audit_unavailable"),
            ("/audit/proof?generation=g1", "proof_unavailable"),
            ("/audit/checkpoint", "checkpoint_unavailable"),
            ("/acceptance", "acceptance_unavailable"),
            ("/completions", "completion_unavailable"),
            ("/migration-batches", "migration_batches_unavailable"),
            ("/migration-batches/events", "migration_batches_unavailable"),
            ("/migration-consumers/status?consumer=c1&now=1",
             "migration_consumers_unavailable"),
            ("/migration-consumers/dead-letters?consumer=c1",
             "migration_consumers_unavailable"),
        )
        for path, error in expected:
            with self.subTest(path=path):
                self._assert_error(path, 503, error)


class PipelineDisabledTest(_HttpMixin, unittest.TestCase):
    """Unconfigured entries and unknown paths stay plain 404s."""

    def setUp(self) -> None:
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=self.server.serve_forever,
                                  daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)

    def test_disabled_entries_and_unknown_paths_are_404(self) -> None:
        for path, _valid, _invalid in GET_ENTRIES:
            with self.subTest(path=path):
                status, _headers, body = self._get(path, token=None)
                self.assertEqual(status, 404)
                self.assertEqual(json.loads(body), {"error": "not_found"})
        for path in ("/nope", "/audit/checkpoint/extra"):
            with self.subTest(path=path):
                status, _headers, body = self._get(path, token=None)
                self.assertEqual(status, 404)
                self.assertEqual(json.loads(body), {"error": "not_found"})
        status, _headers, body = self._post("pull", token=None)
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body), {"error": "not_found"})


if __name__ == "__main__":
    unittest.main()
