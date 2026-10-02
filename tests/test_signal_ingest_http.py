"""HTTP tests for the POST /signals/ingest push endpoint.

Covers the first-acceptance 201 / replay 200 contract with the receipt
and created fields, the pending-resume continuation, the strict request
shape (application/json only, one UTF-8 JSON object of at most 1 MiB
with exactly ``envelope`` and ``key``, no duplicate members or
non-finite numbers), the error mapping (400 signal_ingest_invalid, 403
signal_ingest_forbidden, 404 signal_ingest_not_found, 503
signal_ingest_unavailable) with leak-free error bodies, the query-string
and non-POST rules, the unconfigured plain 404 and the ``serve``
command's ``--signals``/``--signal-trust``/``--signal-receipts`` group,
which only ever appears complete, once and non-empty.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import socket
import subprocess
import sys
import threading
import time
import unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from carbon_market import signal_ingest as si
from carbon_market import signals
from carbon_market.server import Handler

_SECRET_HEX = "ab" * 32
_SECRET = bytes.fromhex(_SECRET_HEX)
_SIGNAL_FIELDS = ("region", "observed", "expires", "mix", "unit_cost",
                  "carbon_intensity")
_RECEIPT_FIELDS = ("source", "key_id", "sequence", "region",
                   "signal_version", "signature", "state")


def _signal(region: str = "eu-north", observed: int = 10,
            **overrides: object) -> dict[str, object]:
    signal: dict[str, object] = {
        "region": region,
        "observed": observed,
        "expires": 100,
        "mix": {"solar": 6000, "wind": 4000},
        "unit_cost": 3,
        "carbon_intensity": 7,
    }
    signal.update(overrides)
    return signal


def _canonical_signal(signal: dict[str, object]) -> dict[str, object]:
    ordered = {field: signal[field] for field in _SIGNAL_FIELDS}
    ordered["mix"] = {name: ordered["mix"][name]  # type: ignore[index]
                      for name in sorted(ordered["mix"])}  # type: ignore[arg-type]
    return ordered


def _sign(source: str, key_id: str, sequence: int,
          signal: dict[str, object], secret: bytes = _SECRET) -> str:
    payload = json.dumps(
        [source, key_id, sequence, _canonical_signal(signal)],
        ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hmac.new(secret, payload, hashlib.sha256).hexdigest()


def _envelope(source: str = "src-a", key_id: str = "key-a",
              sequence: int = 1, signal: dict[str, object] | None = None,
              secret: bytes = _SECRET) -> dict[str, object]:
    if signal is None:
        signal = _signal()
    return {
        "source": source,
        "key_id": key_id,
        "sequence": sequence,
        "signal": signal,
        "signature": _sign(source, key_id, sequence, signal, secret),
    }


def _trust_bytes() -> bytes:
    return si._serialize_trust({
        "src-a": {
            "key_id": "key-a",
            "key": _SECRET_HEX,
            "regions": ["eu-north", "us-west"],
            "valid_from": 0,
            "valid_until": 1000,
        },
    })


def _body(env: dict[str, object], key: str = "k-1") -> bytes:
    return json.dumps({"envelope": env, "key": key},
                      ensure_ascii=False, separators=(",", ":")) \
        .encode("utf-8")


class SignalIngestHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.signals_path = os.path.join(self.tmp.name, "signals.json")
        self.trust = os.path.join(self.tmp.name, "trust.json")
        self.receipts = os.path.join(self.tmp.name, "receipts.json")
        with open(self.trust, "wb") as handle:
            handle.write(_trust_bytes())
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.signals_path = self.signals_path
        self.server.signal_trust = self.trust
        self.server.signal_receipts = self.receipts
        thread = threading.Thread(target=self.server.serve_forever,
                                  daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)

    # -- request helpers -----------------------------------------------------

    def _post(self, body: bytes | None = None, path: str = "/signals/ingest",
              headers: dict[str, str] | None = None):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        sent_headers = {"Content-Type": "application/json"}
        if headers is not None:
            sent_headers = headers
        connection.request("POST", path, body=body, headers=sent_headers)
        response = connection.getresponse()
        data = response.read()
        result = (response.status, data, response.headers)
        connection.close()
        return result

    def _json_post(self, env: dict[str, object] | None = None,
                   key: str = "k-1", **kwargs):
        status, data, headers = self._post(
            _body(_envelope() if env is None else env, key), **kwargs)
        return status, json.loads(data), headers

    def _request(self, method: str, path: str):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        connection.request(method, path)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, data

    # -- happy path ----------------------------------------------------------

    def test_first_ingest_returns_201_created(self) -> None:
        status, payload, headers = self._json_post()
        self.assertEqual(status, 201)
        self.assertEqual(list(payload.keys()), ["receipt", "created"])
        self.assertIs(payload["created"], True)
        self.assertEqual(list(payload["receipt"].keys()),
                         list(_RECEIPT_FIELDS))
        self.assertEqual(payload["receipt"], {
            "source": "src-a",
            "key_id": "key-a",
            "sequence": 1,
            "region": "eu-north",
            "signal_version": 1,
            "signature": _envelope()["signature"],
            "state": "active",
        })
        self.assertEqual(headers["Content-Type"], "application/json")
        record = signals.get(self.signals_path, "eu-north", 10)
        self.assertEqual(record["version"], 1)

    def test_success_body_is_compact_without_trailing_newline(self) -> None:
        status, data, _headers = self._post(_body(_envelope()))
        self.assertEqual(status, 201)
        self.assertFalse(data.endswith(b"\n"))
        self.assertEqual(data, json.dumps(
            json.loads(data), ensure_ascii=False,
            separators=(",", ":")).encode("utf-8"))

    def test_exact_replay_returns_200_not_created(self) -> None:
        first = self._json_post()
        self.assertEqual(first[0], 201)
        ledger_bytes = Path(self.receipts).read_bytes()
        signal_bytes = Path(self.signals_path).read_bytes()
        status, payload, _headers = self._json_post()
        self.assertEqual(status, 200)
        self.assertIs(payload["created"], False)
        self.assertEqual(payload["receipt"], first[1]["receipt"])
        # An active replay never rewrites a byte.
        self.assertEqual(Path(self.receipts).read_bytes(), ledger_bytes)
        self.assertEqual(Path(self.signals_path).read_bytes(),
                         signal_bytes)

    def test_interrupted_request_resumes_pending(self) -> None:
        # A fault during the signal publication leaves the pending
        # receipt; the retry resumes it and creates exactly one version.
        with mock.patch.object(si._signals, "_publish_locked",
                               side_effect=OSError("injected fault")):
            status, payload, _headers = self._json_post()
        self.assertEqual(status, 503)
        self.assertEqual(payload, {"error": "signal_ingest_unavailable"})
        ledger = si._load_file(os.path.realpath(self.receipts))[0]
        self.assertEqual(ledger["k-1"]["state"], "pending")

        status, payload, _headers = self._json_post()
        self.assertEqual(status, 201)
        self.assertIs(payload["created"], True)
        self.assertEqual(payload["receipt"]["signal_version"], 1)
        self.assertEqual(payload["receipt"]["state"], "active")
        self.assertFalse(os.path.exists(self.signals_path + ".tmp"))
        record = signals.get(self.signals_path, "eu-north", 10)
        self.assertEqual(record["version"], 1)
        # A later replay of the same request is the stored active receipt.
        status, payload, _headers = self._json_post()
        self.assertEqual((status, payload["created"]), (200, False))

    def test_concurrent_identical_requests_publish_one_version(self) -> None:
        results: list[tuple[int, dict]] = []
        lock = threading.Lock()

        def post() -> None:
            outcome = self._json_post()
            with lock:
                results.append((outcome[0], outcome[1]))

        threads = [threading.Thread(target=post) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(status for status, _ in results),
                         [200, 200, 200, 200, 200, 201])
        created = [payload for status, payload in results if status == 201]
        self.assertEqual(created[0]["receipt"]["signal_version"], 1)
        for _status, payload in results:
            self.assertEqual(payload["receipt"], created[0]["receipt"])
        record = signals.get(self.signals_path, "eu-north", 10)
        self.assertEqual(record["version"], 1)

    # -- business rejections leave the ledgers untouched ---------------------

    def _assert_unchanged(self) -> None:
        self.assertFalse(os.path.exists(self.receipts))
        self.assertFalse(os.path.exists(self.signals_path))

    def test_same_key_changed_envelope_is_400(self) -> None:
        self.assertEqual(self._json_post()[0], 201)
        ledger_bytes = Path(self.receipts).read_bytes()
        changed = _envelope(sequence=2, signal=_signal(observed=20))
        status, payload, _headers = self._json_post(changed)
        self.assertEqual(status, 400)
        self.assertEqual(payload, {"error": "signal_ingest_invalid"})
        self.assertEqual(Path(self.receipts).read_bytes(), ledger_bytes)

    def test_non_increasing_sequence_is_400(self) -> None:
        self.assertEqual(self._json_post()[0], 201)
        ledger_bytes = Path(self.receipts).read_bytes()
        again = _envelope(sequence=1, signal=_signal(observed=20))
        status, payload, _headers = self._json_post(again, key="k-2")
        self.assertEqual(status, 400)
        self.assertEqual(payload, {"error": "signal_ingest_invalid"})
        self.assertEqual(Path(self.receipts).read_bytes(), ledger_bytes)

    def test_expired_key_is_400(self) -> None:
        env = _envelope(signal=_signal(observed=1001, expires=2000))
        status, payload, _headers = self._json_post(env)
        self.assertEqual(status, 400)
        self.assertEqual(payload, {"error": "signal_ingest_invalid"})
        self._assert_unchanged()

    def test_unauthorized_region_is_400(self) -> None:
        env = _envelope(signal=_signal(region="ap-south"))
        status, payload, _headers = self._json_post(env)
        self.assertEqual(status, 400)
        self.assertEqual(payload, {"error": "signal_ingest_invalid"})
        self._assert_unchanged()

    def test_pending_source_blocks_new_request(self) -> None:
        with mock.patch.object(si._signals, "_publish_locked",
                               side_effect=OSError("injected fault")):
            self.assertEqual(self._json_post()[0], 503)
        later = _envelope(sequence=2, signal=_signal(observed=20))
        status, payload, _headers = self._json_post(later, key="k-2")
        self.assertEqual(status, 400)
        self.assertEqual(payload, {"error": "signal_ingest_invalid"})

    def test_unknown_source_is_403(self) -> None:
        env = _envelope(source="src-b")
        status, payload, _headers = self._json_post(env)
        self.assertEqual(status, 403)
        self.assertEqual(payload, {"error": "signal_ingest_forbidden"})
        self._assert_unchanged()

    def test_unknown_key_id_is_403(self) -> None:
        env = _envelope(key_id="key-b")
        status, payload, _headers = self._json_post(env)
        self.assertEqual(status, 403)
        self.assertEqual(payload, {"error": "signal_ingest_forbidden"})
        self._assert_unchanged()

    def test_signature_mismatch_is_403(self) -> None:
        env = _envelope(secret=b"\x00" * 32)
        status, payload, _headers = self._json_post(env)
        self.assertEqual(status, 403)
        self.assertEqual(payload, {"error": "signal_ingest_forbidden"})
        self._assert_unchanged()

    def test_missing_trust_file_is_404(self) -> None:
        os.unlink(self.trust)
        status, payload, _headers = self._json_post()
        self.assertEqual(status, 404)
        self.assertEqual(payload, {"error": "signal_ingest_not_found"})
        self._assert_unchanged()

    def test_missing_parent_directory_is_404(self) -> None:
        self.server.signals_path = os.path.join(self.tmp.name, "missing",
                                                "signals.json")
        status, payload, _headers = self._json_post()
        self.assertEqual(status, 404)
        self.assertEqual(payload, {"error": "signal_ingest_not_found"})

    def test_error_bodies_never_leak_details(self) -> None:
        # No path, key material, signature or system message appears in
        # any error body.
        os.unlink(self.trust)
        _status, data, _headers = self._post(_body(_envelope()))
        self.assertEqual(json.loads(data),
                         {"error": "signal_ingest_not_found"})
        self.assertNotIn(self.tmp.name.encode(), data)
        self.assertNotIn(_SECRET_HEX.encode(), data)

    # -- request shape -------------------------------------------------------

    def test_wrong_content_type_is_400(self) -> None:
        for content_type in ("text/plain", "application/jsonx",
                             "text/json", ""):
            headers = {"Content-Type": content_type} if content_type else {}
            status, data, _headers = self._post(
                _body(_envelope()), headers=headers)
            with self.subTest(content_type=content_type):
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(data),
                                 {"error": "signal_ingest_invalid"})
        self._assert_unchanged()

    def test_content_type_with_charset_is_accepted(self) -> None:
        status, data, _headers = self._post(
            _body(_envelope()),
            headers={"Content-Type": "application/json; charset=utf-8"})
        self.assertEqual(status, 201)
        self.assertIs(json.loads(data)["created"], True)

    def test_empty_body_is_400(self) -> None:
        status, data, _headers = self._post(b"")
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(data),
                         {"error": "signal_ingest_invalid"})

    def test_oversized_body_is_400(self) -> None:
        # The declared length alone exceeds the 1 MiB cap, so the
        # request is rejected on its headers without the body bytes.
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        connection.putrequest("POST", "/signals/ingest")
        connection.putheader("Content-Type", "application/json")
        connection.putheader("Content-Length", str((1 << 20) + 1))
        connection.endheaders()
        response = connection.getresponse()
        self.assertEqual(response.status, 400)
        self.assertEqual(json.loads(response.read()),
                         {"error": "signal_ingest_invalid"})
        connection.close()
        self._assert_unchanged()

    def test_undecodable_body_is_400(self) -> None:
        status, data, _headers = self._post(b'{"envelope": \xff\xfe}')
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(data),
                         {"error": "signal_ingest_invalid"})

    def test_malformed_json_is_400(self) -> None:
        status, data, _headers = self._post(b'{"envelope":')
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(data),
                         {"error": "signal_ingest_invalid"})

    def test_duplicate_member_is_400(self) -> None:
        body = b'{"envelope":{},"key":"k-1","key":"k-1"}'
        status, data, _headers = self._post(body)
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(data),
                         {"error": "signal_ingest_invalid"})
        self._assert_unchanged()

    def test_non_finite_number_is_400(self) -> None:
        for literal in (b"NaN", b"Infinity", b"-Infinity", b"1e999"):
            env = json.dumps(_envelope())
            body = ('{"envelope":' + env + ',"key":'
                    + literal.decode("ascii") + "}").encode("utf-8")
            status, data, _headers = self._post(body)
            with self.subTest(literal=literal):
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(data),
                                 {"error": "signal_ingest_invalid"})
        self._assert_unchanged()

    def test_unknown_or_missing_field_is_400(self) -> None:
        env = json.dumps(_envelope(), separators=(",", ":"))
        bodies = (
            b'{"envelope":' + env.encode() + b'}',                 # no key
            b'{"key":"k-1"}',                                      # no envelope
            b'{"envelope":' + env.encode() + b',"key":"k-1","x":1}',
            b'{"envelope":' + env.encode() + b',"key":"k-1",'
            b'"envelope2":{}}',
        )
        for body in bodies:
            status, data, _headers = self._post(body)
            with self.subTest(body=body[:40]):
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(data),
                                 {"error": "signal_ingest_invalid"})
        self._assert_unchanged()

    def test_non_object_body_is_400(self) -> None:
        for body in (b"[1,2]", b'"text"', b"42", b"null", b"true"):
            status, data, _headers = self._post(body)
            with self.subTest(body=body):
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(data),
                                 {"error": "signal_ingest_invalid"})

    def test_invalid_envelope_or_key_is_400(self) -> None:
        env = _envelope()
        cases = [
            {"envelope": {}, "key": "k-1"},
            {"envelope": {**env, "extra": 1}, "key": "k-1"},
            {"envelope": {k: v for k, v in env.items() if k != "source"},
             "key": "k-1"},
            {"envelope": env, "key": ""},
            {"envelope": env, "key": 7},
            {"envelope": "text", "key": "k-1"},
        ]
        for case in cases:
            body = json.dumps(case).encode("utf-8")
            status, data, _headers = self._post(body)
            with self.subTest(case=str(case)[:60]):
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(data),
                                 {"error": "signal_ingest_invalid"})
        self._assert_unchanged()

    def test_query_string_is_400_without_reading_files(self) -> None:
        # The trust file is gone: a query-string request must still be
        # answered 400 before any business file is opened.
        os.unlink(self.trust)
        status, data = self._request("POST", "/signals/ingest?key=k-1")
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(data),
                         {"error": "signal_ingest_invalid"})

    def test_non_post_method_is_plain_404(self) -> None:
        os.unlink(self.trust)
        for method in ("GET", "HEAD"):
            status, data = self._request(method, "/signals/ingest")
            with self.subTest(method=method):
                self.assertEqual(status, 404)
                if method != "HEAD":
                    self.assertEqual(json.loads(data),
                                     {"error": "not_found"})


class SignalIngestUnconfiguredTest(unittest.TestCase):
    def setUp(self) -> None:
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=self.server.serve_forever,
                                  daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)

    def test_unconfigured_path_is_plain_404(self) -> None:
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        connection.request("POST", "/signals/ingest",
                           body=b'{"envelope":{},"key":"k-1"}',
                           headers={"Content-Type": "application/json"})
        response = connection.getresponse()
        self.assertEqual(response.status, 404)
        self.assertEqual(json.loads(response.read()),
                         {"error": "not_found"})
        connection.close()

    def test_other_endpoints_behave_as_before(self) -> None:
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        connection.request("GET", "/health")
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        self.assertEqual(json.loads(response.read()), {"status": "ok"})
        connection.close()


class ServeSignalArgumentsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.signals_path = os.path.join(self.tmp.name, "signals.json")
        self.trust = os.path.join(self.tmp.name, "trust.json")
        self.receipts = os.path.join(self.tmp.name, "receipts.json")
        with open(self.trust, "wb") as handle:
            handle.write(_trust_bytes())

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "carbon_market", "serve", *args],
            capture_output=True, timeout=10)

    def test_incomplete_empty_or_repeated_group_exits_2(self) -> None:
        group = ("--signals", self.signals_path,
                 "--signal-trust", self.trust,
                 "--signal-receipts", self.receipts)
        for args in (
            ("--signals", self.signals_path),                 # one only
            ("--signal-trust", self.trust),                   # one only
            ("--signal-receipts", self.receipts),             # one only
            ("--signals", self.signals_path,
             "--signal-trust", self.trust),                   # two of three
            ("--signals", self.signals_path,
             "--signal-receipts", self.receipts),             # two of three
            ("--signal-trust", self.trust,
             "--signal-receipts", self.receipts),             # two of three
            group[:2] + ("--signal-trust", "",) + group[4:],  # empty value
            ("--signals", "",) + group[2:],                   # empty value
            group + ("--signals", self.signals_path),         # repeated
            group + ("--signal-receipts", self.receipts),     # repeated
        ):
            with self.subTest(args=args):
                self.assertEqual(self._run(*args).returncode, 2)

    def test_valid_group_starts_and_serves(self) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        process = subprocess.Popen(
            [sys.executable, "-m", "carbon_market", "serve",
             "--host", "127.0.0.1", "--port", str(port),
             "--signals", self.signals_path,
             "--signal-trust", self.trust,
             "--signal-receipts", self.receipts])
        try:
            deadline = time.time() + 10
            outcomes = []
            while time.time() < deadline:
                try:
                    connection = HTTPConnection("127.0.0.1", port,
                                                timeout=1)
                    connection.request(
                        "POST", "/signals/ingest", body=_body(_envelope()),
                        headers={"Content-Type": "application/json"})
                    response = connection.getresponse()
                    outcomes.append(
                        (response.status, json.loads(response.read())))
                    connection.close()
                    break
                except OSError:
                    time.sleep(0.05)
            self.assertEqual(len(outcomes), 1)
            status, payload = outcomes[0]
            self.assertEqual(status, 201)
            self.assertIs(payload["created"], True)
            self.assertEqual(payload["receipt"]["state"], "active")
        finally:
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
