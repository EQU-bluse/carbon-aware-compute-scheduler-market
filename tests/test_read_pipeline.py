"""Cross-endpoint regression tests for the unified read-only pipeline.

Every read-only GET entry -- the audit and proof queries, the
checkpoint download, the acceptance and completion queries, the
migration batch and event queries, the consumer status and dead-letter
queries and the process metrics -- runs one shared request pipeline in
the server (see ``Handler._read_request``): token identity, the
entry's own query string and, for the conditional entries, the
If-None-Match header, the scope decision, and only then the snapshot
read and the response. These tests pin the shared stage order
(identity before parameters, parameters before scope, scope before any
file), the per-entry error names, the success and 304 wire shapes, the
exactly-once metrics counting and the complete-version guarantee of
concurrent reads, so the unified implementation cannot drift the
entries apart.
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

from carbon_market import acceptance, audit, audit_proof, completion
from carbon_market.metrics import Metrics
from carbon_market.server import Handler
from tests.test_migration_batch import MigrationBatchTest

FULL_TOKEN = "full-token"
KEY_TOKEN = "key-token"
OPS_TOKEN = "ops-token"

# Every read-only entry with one valid query each.
ENTRIES = (
    "/audit?limit=1",
    "/audit/proof?generation=g1",
    "/audit/checkpoint",
    "/acceptance?limit=1",
    "/migration-batches?limit=1",
    "/migration-batches/events?limit=1",
    "/migration-consumers/status?consumer=c1&now=1",
    "/migration-consumers/dead-letters?consumer=c1",
    "/completions?limit=1",
    "/metrics",
)
# The three conditional entries; every other read-only entry ignores
# the If-None-Match header entirely.
CONDITIONAL = ("/audit/checkpoint", "/acceptance", "/completions")


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


def _bogus(path: str) -> str:
    # One unknown parameter appended to the entry's valid query.
    return path + ("&bogus=1" if "?" in path else "?bogus=1")


class _HttpMixin:
    def _start(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.journal = os.path.join(self.tmp.name, "audit.json")
        self.checkpoint = os.path.join(self.tmp.name, "checkpoint.json")
        self.proof_path = os.path.join(self.tmp.name, "proof.json")
        self.ledger_dir = os.path.join(self.tmp.name, "ledger")
        self.completions = os.path.join(self.tmp.name, "completions.json")
        self.coord = os.path.join(self.tmp.name, "coord.json")
        self.consumers = os.path.join(self.tmp.name, "consumers.json")
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        self._write_config(
            _line("full", FULL_TOKEN) + "\n"
            + _line("key", KEY_TOKEN, keys=["k1", "aaa"]) + "\n"
            + _line("ops", OPS_TOKEN, ops=["copy"]) + "\n")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.audit_path = self.journal
        self.server.audit_token = None
        self.server.audit_auth = self.config
        self.server.audit_checkpoint = self.checkpoint
        self.server.acceptance_dir = self.ledger_dir
        self.server.migration_batches = self.coord
        self.server.migration_consumers = self.consumers
        self.server.completions_path = self.completions
        self.server.metrics = Metrics(int(time.time()))
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

    def _get(self, path: str, token: str | None = FULL_TOKEN,
             headers: dict[str, str] | None = None):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=5)
        all_headers = {}
        if token is not None:
            all_headers["X-Audit-Token"] = token
        if headers:
            all_headers.update(headers)
        connection.request("GET", path, headers=all_headers)
        response = connection.getresponse()
        body = response.read()
        result = (response.status, response.headers, body)
        connection.close()
        return result

    def _get_duplicate_token(self, path: str):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=5)
        connection.putrequest("GET", path)
        connection.putheader("X-Audit-Token", FULL_TOKEN)
        connection.putheader("X-Audit-Token", FULL_TOKEN)
        connection.endheaders()
        response = connection.getresponse()
        body = response.read()
        connection.close()
        return response.status, body

    def _json_get(self, path: str, **kwargs):
        status, _headers, body = self._get(path, **kwargs)
        return status, json.loads(body)


class ReadPipelineStageOrderTest(_HttpMixin, unittest.TestCase):
    """The shared stage order, pinned on a server without any files."""

    def setUp(self) -> None:
        self._start()

    def _assert_no_files(self) -> None:
        for path in (self.journal, self.checkpoint, self.ledger_dir,
                     self.completions, self.coord, self.consumers):
            self.assertFalse(os.path.exists(path), path)

    def test_identity_precedes_parameters_on_every_entry(self) -> None:
        for path in ENTRIES:
            with self.subTest(entry=path):
                bogus = _bogus(path)
                status, body = self._json_get(bogus, token=None)
                self.assertEqual((status, body),
                                 (401, {"error": "unauthorized"}))
                status, body = self._json_get(bogus, token="wrong-token")
                self.assertEqual((status, body),
                                 (403, {"error": "forbidden"}))

    def test_duplicate_token_is_401_on_every_entry(self) -> None:
        for path in ENTRIES:
            with self.subTest(entry=path):
                status, body = self._get_duplicate_token(path)
                self.assertEqual((status, json.loads(body)),
                                 (401, {"error": "unauthorized"}))

    def test_parameter_validation_precedes_scope_on_every_entry(self) -> None:
        # An ops-scoped token with a malformed query gets the 400
        # before the scope decision.
        for path in ENTRIES:
            with self.subTest(entry=path):
                status, body = self._json_get(_bogus(path), token=OPS_TOKEN)
                self.assertEqual((status, body),
                                 (400, {"error": "invalid_request"}))

    def test_scope_precedes_file_access_on_every_entry(self) -> None:
        for path in ENTRIES:
            with self.subTest(entry=path):
                status, body = self._json_get(path, token=OPS_TOKEN)
                self.assertEqual((status, body),
                                 (403, {"error": "forbidden"}))
        self._assert_no_files()

    def test_auth_config_unavailable_is_503_on_every_entry(self) -> None:
        os.unlink(self.config)
        for path in ENTRIES:
            with self.subTest(entry=path):
                status, body = self._json_get(path)
                self.assertEqual((status, body),
                                 (503, {"error": "auth_unavailable"}))

    def test_missing_snapshot_keeps_each_entrys_own_404(self) -> None:
        expected = {
            "/audit?limit=1": "audit_not_found",
            "/audit/proof?generation=g1": "proof_not_found",
            "/audit/checkpoint": "checkpoint_not_found",
            "/acceptance?limit=1": "acceptance_not_found",
            "/migration-batches?limit=1": "migration_batches_not_found",
            "/migration-batches/events?limit=1":
                "migration_batches_not_found",
            "/migration-consumers/status?consumer=c1&now=1":
                "migration_consumer_not_found",
            "/migration-consumers/dead-letters?consumer=c1":
                "migration_consumer_not_found",
            "/completions?limit=1": "completion_not_found",
        }
        for path, error in expected.items():
            with self.subTest(entry=path):
                status, body = self._json_get(path)
                self.assertEqual((status, body), (404, {"error": error}))
                # The configured paths never leak into the response.
                self.assertNotIn(self.tmp.name.encode(), body)

    def test_invalid_content_keeps_each_entrys_own_409(self) -> None:
        acceptance_ledger = os.path.join(self.ledger_dir, "ledger.json")
        cases = (
            ((self.journal,), "/audit?limit=1", "audit_invalid"),
            ((self.journal, self.checkpoint),
             "/audit/proof?generation=g1", "proof_invalid"),
            ((self.checkpoint,), "/audit/checkpoint", "checkpoint_invalid"),
            ((acceptance_ledger,), "/acceptance?limit=1",
             "acceptance_invalid"),
            ((self.coord,), "/migration-batches?limit=1",
             "migration_batches_invalid"),
            ((self.coord,), "/migration-batches/events?limit=1",
             "migration_batches_invalid"),
            ((self.consumers,),
             "/migration-consumers/status?consumer=c1&now=1",
             "migration_consumers_invalid"),
            ((self.consumers,),
             "/migration-consumers/dead-letters?consumer=c1",
             "migration_consumers_invalid"),
            ((self.completions,), "/completions?limit=1",
             "completion_invalid"),
        )
        for paths, query, error in cases:
            with self.subTest(entry=query):
                for path in paths:
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    with open(path, "wb") as handle:
                        handle.write(b"{not json\n")
                try:
                    status, body = self._json_get(query)
                    self.assertEqual((status, body), (409, {"error": error}))
                    self.assertNotIn(self.tmp.name.encode(), body)
                finally:
                    for path in paths:
                        os.unlink(path)

    def test_io_failure_keeps_each_entrys_own_503(self) -> None:
        # A directory where each file should be: the open fails with an
        # OSError that is not FileNotFoundError.
        cases = (
            (self.journal, "/audit?limit=1", "audit_unavailable"),
            (self.checkpoint, "/audit/checkpoint",
             "checkpoint_unavailable"),
            (os.path.join(self.ledger_dir, "ledger.json"),
             "/acceptance?limit=1", "acceptance_unavailable"),
            (self.coord, "/migration-batches?limit=1",
             "migration_batches_unavailable"),
            (self.coord, "/migration-batches/events?limit=1",
             "migration_batches_unavailable"),
            (self.consumers, "/migration-consumers/status?consumer=c1&now=1",
             "migration_consumers_unavailable"),
            (self.consumers, "/migration-consumers/dead-letters?consumer=c1",
             "migration_consumers_unavailable"),
            (self.completions, "/completions?limit=1",
             "completion_unavailable"),
        )
        for path, query, error in cases:
            with self.subTest(entry=query):
                os.makedirs(path)
                try:
                    status, body = self._json_get(query)
                    self.assertEqual((status, body), (503, {"error": error}))
                    self.assertNotIn(self.tmp.name.encode(), body)
                finally:
                    os.rmdir(path)


class ReadPipelineSuccessTest(_HttpMixin, unittest.TestCase):
    """Success, 304 and metrics counting on the populated server."""

    def setUp(self) -> None:
        self._start()
        audit.record(self.journal, "k1", _event())
        proof = audit_proof.export(self.journal, self.checkpoint, "g1")
        with open(self.proof_path, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(proof, ensure_ascii=False))
        acceptance.submit(self.checkpoint, self.proof_path,
                          self._checkpoint_etag(), None,
                          self.ledger_dir, "k1")
        with open(self.completions, "wb") as handle:
            handle.write(_completions_bytes(
                {"x-1": _completion_record("j-a")}))

    def _checkpoint_etag(self) -> str:
        with open(self.checkpoint, "rb") as handle:
            return '"' + hashlib.sha256(handle.read()).hexdigest() + '"'

    # -- success shapes --------------------------------------------------

    def test_success_on_every_local_entry(self) -> None:
        # 200 with compact UTF-8 JSON and no trailing newline; the
        # conditional entries additionally carry the strong ETag of the
        # exact body bytes.
        for path in ("/audit?limit=1", "/audit/proof?generation=g1",
                     "/audit/checkpoint", "/acceptance?limit=1",
                     "/acceptance?key=k1", "/completions?limit=1",
                     "/completions?job=j-a", "/metrics"):
            with self.subTest(entry=path):
                status, headers, body = self._get(path)
                self.assertEqual(status, 200)
                # The checkpoint download serves the checkpoint file
                # verbatim, trailing newline included; every other
                # body is the endpoint's newline-free compact JSON.
                if path != "/audit/checkpoint":
                    self.assertFalse(body.endswith(b"\n"))
                json.loads(body)
                fixed = path.partition("?")[0]
                if fixed in CONDITIONAL:
                    self.assertEqual(
                        headers.get("ETag"),
                        '"' + hashlib.sha256(body).hexdigest() + '"')

    def test_checkpoint_download_serves_the_file_bytes(self) -> None:
        status, headers, body = self._get("/audit/checkpoint")
        self.assertEqual(status, 200)
        with open(self.checkpoint, "rb") as handle:
            self.assertEqual(body, handle.read())
        self.assertEqual(headers.get("ETag"), self._checkpoint_etag())

    def test_304_roundtrip_on_every_conditional_entry(self) -> None:
        stale = '"' + "0" * 64 + '"'
        for path in CONDITIONAL:
            with self.subTest(entry=path):
                status, headers, body = self._get(path)
                self.assertEqual(status, 200)
                etag = headers.get("ETag")
                # A matching tag answers an empty 304 with the same tag.
                status, headers, body = self._get(
                    path, headers={"If-None-Match": etag})
                self.assertEqual(status, 304)
                self.assertEqual(body, b"")
                self.assertEqual(headers.get("ETag"), etag)
                # A stale but well-formed tag answers the full 200.
                status, headers, body = self._get(
                    path, headers={"If-None-Match": stale})
                self.assertEqual(status, 200)
                self.assertEqual(
                    headers.get("ETag"),
                    '"' + hashlib.sha256(body).hexdigest() + '"')

    def test_conditional_header_shapes_are_400(self) -> None:
        for path in CONDITIONAL:
            for value in ("not-a-tag", "W/\"abc\"", "*", ""):
                with self.subTest(entry=path, value=value):
                    status, body = self._json_get(
                        path, headers={"If-None-Match": value})
                    self.assertEqual((status, body),
                                     (400, {"error": "invalid_request"}))

    def test_other_entries_ignore_the_conditional_header(self) -> None:
        # Only the checkpoint download and the acceptance/completion
        # queries are conditional; every other read-only entry ignores
        # the header entirely, whatever its shape.
        for path in ("/audit?limit=1", "/audit/proof?generation=g1",
                     "/metrics"):
            with self.subTest(entry=path):
                status, _headers, _body = self._get(
                    path, headers={"If-None-Match": "not-a-tag"})
                self.assertEqual(status, 200)

    # -- metrics counting --------------------------------------------------

    def test_metrics_counts_each_request_exactly_once(self) -> None:
        # One decided response is one count under its fixed route;
        # authorization failures and unknown paths are classified the
        # same way.
        self._get("/health")
        self._get("/audit?limit=1")
        self._get("/audit?limit=1", token=None)          # 401
        self._get("/audit/proof?generation=g1")
        self._get("/audit/checkpoint")
        self._get("/acceptance?limit=1")
        self._get("/completions?limit=1")
        self._get("/missing")                            # 404 "other"
        status, _headers, body = self._get("/metrics")
        self.assertEqual(status, 200)
        snapshot = json.loads(body)
        # The metrics read itself is not in its own snapshot.
        self.assertEqual(snapshot["total"], 8)
        self.assertEqual(
            snapshot["routes"],
            {"/acceptance": {"total": 1, "statuses": {"200": 1}},
             "/audit": {"total": 2, "statuses": {"200": 1, "401": 1}},
             "/audit/checkpoint": {"total": 1, "statuses": {"200": 1}},
             "/audit/proof": {"total": 1, "statuses": {"200": 1}},
             "/completions": {"total": 1, "statuses": {"200": 1}},
             "/health": {"total": 1, "statuses": {"200": 1}},
             "other": {"total": 1, "statuses": {"404": 1}}})
        # The first metrics read shows up exactly once in the next one.
        status, _headers, body = self._get("/metrics")
        self.assertEqual(status, 200)
        snapshot = json.loads(body)
        self.assertEqual(snapshot["total"], 9)
        self.assertEqual(snapshot["routes"]["/metrics"],
                         {"total": 1, "statuses": {"200": 1}})

    # -- concurrency: one complete version per response --------------------

    def test_racing_reads_see_only_complete_versions(self) -> None:
        results: list[tuple[int, str | None, bytes]] = []
        errors: list[BaseException] = []

        def reader(path: str) -> None:
            try:
                etag: str | None = None
                for _ in range(15):
                    headers = ({"If-None-Match": etag}
                               if etag is not None else None)
                    status, response_headers, body = self._get(
                        path, headers=headers)
                    etag = response_headers.get("ETag")
                    results.append((status, etag, body))
            except BaseException as exc:  # pragma: no cover - failure
                errors.append(exc)

        def completions_writer() -> None:
            try:
                records = {"x-1": _completion_record("j-a")}
                for index in range(5):
                    records[f"x-w{index}"] = _completion_record(
                        f"j-w{index}", at=71 + index)
                    payload = _completions_bytes(records)
                    realpath = os.path.realpath(self.completions)
                    with completion._lock(realpath):
                        with open(self.completions, "wb") as handle:
                            handle.write(payload)
            except BaseException as exc:  # pragma: no cover - failure
                errors.append(exc)

        def checkpoint_writer() -> None:
            try:
                for index in range(5):
                    audit.record(self.journal, f"w{index}", _event())
                    # Re-export the open generation: the checkpoint
                    # bytes change under the readers.
                    audit_proof.export(self.journal, self.checkpoint,
                                       "g1")
            except BaseException as exc:  # pragma: no cover - failure
                errors.append(exc)

        threads = [threading.Thread(target=reader, args=("/completions",))
                   for _ in range(3)]
        threads += [threading.Thread(target=reader,
                                     args=("/audit/checkpoint",))
                    for _ in range(3)]
        threads += [threading.Thread(target=completions_writer),
                    threading.Thread(target=checkpoint_writer)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
            self.assertFalse(thread.is_alive())

        self.assertEqual(errors, [])
        self.assertTrue(results)
        for status, etag, body in results:
            self.assertIsNotNone(etag)
            if status == 304:
                # A conditional hit is the empty body with the tag the
                # reader sent -- never a partial body.
                self.assertEqual(body, b"")
                continue
            self.assertEqual(status, 200)
            # Whatever version was served, the tag summarizes exactly
            # the bytes in hand -- never an old tag over a new body.
            self.assertEqual(
                etag, '"' + hashlib.sha256(body).hexdigest() + '"')
            json.loads(body)


class ReadPipelineMigrationTest(_HttpMixin, unittest.TestCase):
    """The migration entries on a real coordination ledger."""

    def setUp(self) -> None:
        self._start()
        # Reuse the migration-batch module fixture to build a real
        # coordination ledger and its business ledgers.
        self.fx = MigrationBatchTest(
            "test_get_returns_copy_and_unknown_key_raises")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.fx._prepare_migrate()
        self.coord = self.fx.paths["coord"]
        self.consumers = os.path.join(self.fx.tmp.name, "consumers.json")
        self.server.migration_batches = self.coord
        self.server.migration_consumers = self.consumers
        fx = self.fx
        fx._run(key="aaa", owner="负责人-1", now=30)
        fx._run(key="zzz", now=31)
        fx._run(key="aaa", owner="负责人-1", now=32,
                receipts={"j-1": fx._receipt(
                    "copy", "succeeded", "复制完成", 33)})
        fx._run(key="aaa", owner="负责人-1", now=34,
                receipts={"j-1": fx._receipt(
                    "switch", "succeeded", "切换完成", 35)})

    def _claim(self, consumer: str, token: str = FULL_TOKEN,
               **filters) -> None:
        body = {"consumer": consumer, "owner": "o1", "now": 40,
                "lease": 100, "idem": f"idem-{consumer}", **filters}
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=5)
        connection.request("POST", "/migration-consumers/claim",
                           json.dumps(body).encode(),
                           {"Content-Type": "application/json",
                            "X-Audit-Token": token})
        response = connection.getresponse()
        payload = response.read()
        connection.close()
        self.assertEqual(response.status, 200, payload)

    def test_success_on_every_migration_entry(self) -> None:
        self._claim("c1", key="aaa")
        for path in ("/migration-batches?limit=1",
                     "/migration-batches/events?limit=1",
                     "/migration-consumers/status?consumer=c1&now=50",
                     "/migration-consumers/dead-letters?consumer=c1"):
            with self.subTest(entry=path):
                status, _headers, body = self._get(path)
                self.assertEqual(status, 200)
                self.assertFalse(body.endswith(b"\n"))
                json.loads(body)
        _status, _headers, body = self._get(
            "/migration-consumers/status?consumer=c1&now=50")
        self.assertEqual(json.loads(body)["consumer"], "c1")
        _status, _headers, body = self._get(
            "/migration-consumers/dead-letters?consumer=c1")
        self.assertEqual(json.loads(body)["entries"], [])

    def test_key_scope_rules_match_the_snapshot_entries(self) -> None:
        self._claim("c1", key="aaa")
        self._claim("c2")
        # An exact lookup or a key filter naming an allowed batch
        # passes; any other combination is forbidden.
        allowed = ("/migration-batches?key=aaa",
                   "/migration-batches/events?key=aaa",
                   "/migration-consumers/status?consumer=c1&now=50",
                   "/migration-consumers/dead-letters?consumer=c1")
        for path in allowed:
            with self.subTest(entry=path):
                status, _headers, _body = self._get(path, token=KEY_TOKEN)
                self.assertEqual(status, 200)
        forbidden = ("/migration-batches?key=zzz",
                     "/migration-batches?limit=1",
                     "/migration-batches/events?key=zzz",
                     "/migration-batches/events?limit=1",
                     # A cross-batch subscription needs an unrestricted
                     # key scope.
                     "/migration-consumers/status?consumer=c2&now=50",
                     "/migration-consumers/dead-letters?consumer=c2")
        for path in forbidden:
            with self.subTest(entry=path):
                status, body = self._json_get(path, token=KEY_TOKEN)
                self.assertEqual((status, body),
                                 (403, {"error": "forbidden"}))

    def test_unknown_exact_key_keeps_its_own_404(self) -> None:
        status, body = self._json_get("/migration-batches?key=nope")
        self.assertEqual((status, body),
                         (404, {"error": "migration_batch_not_found"}))


if __name__ == "__main__":
    unittest.main()
