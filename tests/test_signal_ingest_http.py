"""HTTP tests for the POST /signals/ingest endpoint.

Covers the request shape (exactly the envelope and a non-empty
idempotency key, Content-Type application/json, one UTF-8 JSON object
of at most 1 MiB, no duplicate members, no non-finite numbers), the
success contract (201 with created true on first acceptance and on a
resumed pending receipt, 200 with created false on an active replay,
the body carrying receipt then created), the error mapping (400
signal_ingest_invalid, 403 signal_ingest_forbidden, 404
signal_ingest_not_found, 503 signal_ingest_unavailable), the plain 404
for other methods and for the unconfigured server, and the ``serve``
command's --signals/--signal-trust/--signal-receipts group, which must
appear together, at most once each and non-empty.
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

from carbon_market import signal_ingest as si
from carbon_market import signals
from carbon_market.server import Handler

_SECRET_HEX = "ab" * 32
_SECRET = bytes.fromhex(_SECRET_HEX)
_SIGNAL_FIELDS = ("region", "observed", "expires", "mix", "unit_cost",
                  "carbon_intensity")


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


def envelope(source: str = "src-a", key_id: str = "key-a", sequence: int = 1,
             signal: dict[str, object] | None = None,
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


def _trust_doc() -> dict[str, object]:
    return {
        "src-a": {
            "key_id": "key-a",
            "key": _SECRET_HEX,
            "regions": ["eu-north", "us-west"],
            "valid_from": 0,
            "valid_until": 1000,
        },
    }


class SignalIngestHttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.signals = os.path.join(self.tmp.name, "signals.json")
        self.trust = os.path.join(self.tmp.name, "trust.json")
        self.receipts = os.path.join(self.tmp.name, "ingest.json")
        Path(self.trust).write_bytes(si._serialize_trust(_trust_doc()))
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.signals_path = self.signals
        self.server.signal_trust = self.trust
        self.server.signal_receipts = self.receipts
        thread = threading.Thread(target=self.server.serve_forever,
                                  daemon=True)
        thread.start()
        self.addCleanup(self.server.shutdown)
        self.addCleanup(self.server.server_close)
        self.addCleanup(thread.join, 2)

    def _request(self, method: str, path: str, body: bytes | None = None,
                 headers: dict[str, str] | None = None):
        connection = HTTPConnection("127.0.0.1", self.server.server_port,
                                    timeout=2)
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        data = response.read()
        result = (response.status, data, response.headers)
        connection.close()
        return result

    def _post(self, payload: object, key: str = "k-1",
              path: str = "/signals/ingest",
              content_type: str = "application/json"):
        if isinstance(payload, (bytes, str)):
            raw = payload if isinstance(payload, bytes) \
                else payload.encode("utf-8")
        else:
            raw = json.dumps({"envelope": payload, "key": key},
                             ensure_ascii=False,
                             separators=(",", ":")).encode("utf-8")
        return self._request("POST", path, body=raw,
                             headers={"Content-Type": content_type})

    def _json_post(self, env: dict[str, object], key: str = "k-1",
                   **kwargs):
        status, body, _headers = self._post(env, key, **kwargs)
        return status, json.loads(body)

    # -- success contract ---------------------------------------------------

    def test_first_ingest_returns_201_created(self) -> None:
        status, body = self._json_post(envelope())
        self.assertEqual(status, 201)
        self.assertEqual(list(body), ["receipt", "created"])
        self.assertIs(body["created"], True)
        receipt = body["receipt"]
        self.assertEqual(
            list(receipt),
            ["source", "key_id", "sequence", "region", "signal_version",
             "signature", "state"])
        self.assertEqual(receipt["source"], "src-a")
        self.assertEqual(receipt["key_id"], "key-a")
        self.assertEqual(receipt["sequence"], 1)
        self.assertEqual(receipt["region"], "eu-north")
        self.assertEqual(receipt["signal_version"], 1)
        self.assertEqual(receipt["state"], "active")
        self.assertEqual(receipt["signature"], envelope()["signature"])

    def test_success_body_is_compact_without_trailing_newline(self) -> None:
        status, body, headers = self._post(envelope())
        self.assertEqual(status, 201)
        self.assertFalse(body.endswith(b"\n"))
        self.assertEqual(body.decode("utf-8"),
                         json.dumps(json.loads(body), ensure_ascii=False,
                                    separators=(",", ":")))
        self.assertEqual(headers["Content-Type"], "application/json")

    def test_active_replay_returns_200_not_created(self) -> None:
        env = envelope()
        first = self._json_post(env)
        self.assertEqual(first[0], 201)
        status, body = self._json_post(env)
        self.assertEqual(status, 200)
        self.assertIs(body["created"], False)
        self.assertEqual(body["receipt"], first[1]["receipt"])

    def test_replay_survives_later_sequences(self) -> None:
        env1 = envelope(sequence=1)
        env2 = envelope(sequence=2, signal=_signal(observed=20))
        self.assertEqual(self._json_post(env1, key="k-1")[0], 201)
        self.assertEqual(self._json_post(env2, key="k-2")[0], 201)
        status, body = self._json_post(env1, key="k-1")
        self.assertEqual(status, 200)
        self.assertIs(body["created"], False)
        self.assertEqual(body["receipt"]["sequence"], 1)

    def test_pending_receipt_is_resumed_not_duplicated(self) -> None:
        env = envelope()
        pending = {
            "source": "src-a",
            "key_id": "key-a",
            "sequence": 1,
            "region": "eu-north",
            "signal_version": 0,
            "signature": env["signature"],
            "state": "pending",
        }
        Path(self.receipts).write_bytes(si._serialize({"k-1": pending}))
        status, body = self._json_post(env)
        self.assertEqual(status, 201)
        self.assertIs(body["created"], True)
        self.assertEqual(body["receipt"]["state"], "active")
        self.assertEqual(body["receipt"]["signal_version"], 1)
        history = signals._load_file(os.path.realpath(self.signals))[0]
        self.assertEqual(len(history["eu-north"]), 1)

    def test_concurrent_identical_requests_publish_one_version(self) -> None:
        env = envelope()
        results: list[tuple[int, dict]] = []
        lock = threading.Lock()

        def post() -> None:
            status, body = self._json_post(env)
            with lock:
                results.append((status, body))

        threads = [threading.Thread(target=post) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(status for status, _ in results),
                         [200] * 7 + [201])
        created = [body["created"] for _status, body in results]
        self.assertEqual(created.count(True), 1)
        history = signals._load_file(os.path.realpath(self.signals))[0]
        self.assertEqual(len(history["eu-north"]), 1)

    # -- request shape --------------------------------------------------------

    def test_query_string_is_invalid_and_reads_nothing(self) -> None:
        status, body = self._json_post(envelope(),
                                       path="/signals/ingest?x=1")
        self.assertEqual(status, 400)
        self.assertEqual(body, {"error": "signal_ingest_invalid"})
        self.assertFalse(os.path.exists(self.receipts))
        self.assertFalse(os.path.exists(self.signals))

    def test_get_returns_plain_404(self) -> None:
        status, body, _headers = self._request("GET", "/signals/ingest")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body), {"error": "not_found"})

    def test_wrong_content_type_is_invalid(self) -> None:
        for content_type in ("text/plain", "application/jsonx",
                             "application/xml", ""):
            with self.subTest(content_type=content_type):
                status, body = self._json_post(
                    envelope(), content_type=content_type)
                self.assertEqual(status, 400)
                self.assertEqual(body, {"error": "signal_ingest_invalid"})
        self.assertFalse(os.path.exists(self.receipts))

    def test_empty_body_is_invalid(self) -> None:
        status, body, _headers = self._post(b"")
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body),
                         {"error": "signal_ingest_invalid"})

    def test_oversized_body_is_invalid(self) -> None:
        # The declared length alone exceeds 1 MiB; the server rejects
        # before the body is read, so only the headers are sent.
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

    def test_undecodable_body_is_invalid(self) -> None:
        status, body, _headers = self._post(b"\xff\xfe{}")
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body),
                         {"error": "signal_ingest_invalid"})

    def test_malformed_json_is_invalid(self) -> None:
        status, body, _headers = self._post(b'{"envelope":')
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body),
                         {"error": "signal_ingest_invalid"})

    def test_duplicate_member_is_invalid(self) -> None:
        raw = json.dumps({"envelope": envelope(), "key": "k-1"},
                         separators=(",", ":"))
        raw = raw[:-1] + ',"key":"k-2"}'
        status, body, _headers = self._post(raw)
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body),
                         {"error": "signal_ingest_invalid"})

    def test_non_finite_number_is_invalid(self) -> None:
        for token in ("NaN", "Infinity", "1e999"):
            with self.subTest(token=token):
                env = json.loads(json.dumps(envelope()))
                env["signal"]["unit_cost"] = None
                raw = json.dumps({"envelope": env, "key": "k-1"},
                                 separators=(",", ":"))
                raw = raw.replace(
                    '"unit_cost":null', f'"unit_cost":{token}')
                status, body, _headers = self._post(raw)
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(body),
                                 {"error": "signal_ingest_invalid"})

    def test_unknown_missing_and_misshaped_fields_are_invalid(self) -> None:
        env = envelope()
        cases = [
            {},                                             # both missing
            {"envelope": env},                              # key missing
            {"key": "k-1"},                                 # envelope missing
            {"envelope": env, "key": "k-1", "extra": 1},    # unknown field
            {"envelope": env, "key": ""},                   # empty key
            {"envelope": env, "key": 7},                    # non-string key
            {"envelope": "x", "key": "k-1"},                # non-object env
            ["envelope", "key"],                            # non-object body
        ]
        for payload in cases:
            with self.subTest(payload=repr(payload)[:60]):
                raw = json.dumps(payload, separators=(",", ":"))
                status, body, _headers = self._post(raw)
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(body),
                                 {"error": "signal_ingest_invalid"})
        self.assertFalse(os.path.exists(self.receipts))

    # -- library error mapping ------------------------------------------------

    def test_unknown_source_is_forbidden(self) -> None:
        env = envelope(source="src-b")
        status, body = self._json_post(env)
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "signal_ingest_forbidden"})

    def test_unknown_key_id_is_forbidden(self) -> None:
        env = envelope(key_id="key-b")
        status, body = self._json_post(env)
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "signal_ingest_forbidden"})

    def test_bad_signature_is_forbidden(self) -> None:
        env = envelope(secret=b"\x00" * 32)
        status, body = self._json_post(env)
        self.assertEqual(status, 403)
        self.assertEqual(body, {"error": "signal_ingest_forbidden"})
        self.assertFalse(os.path.exists(self.receipts))

    def test_expired_key_is_invalid(self) -> None:
        env = envelope(signal=_signal(observed=2000))
        status, body = self._json_post(env)
        self.assertEqual(status, 400)
        self.assertEqual(body, {"error": "signal_ingest_invalid"})

    def test_unauthorized_region_is_invalid(self) -> None:
        env = envelope(signal=_signal(region="ap-south"))
        status, body = self._json_post(env)
        self.assertEqual(status, 400)
        self.assertEqual(body, {"error": "signal_ingest_invalid"})

    def test_non_increasing_sequence_is_invalid(self) -> None:
        self.assertEqual(self._json_post(envelope(sequence=1))[0], 201)
        status, body = self._json_post(
            envelope(sequence=1, signal=_signal(observed=20)), key="k-2")
        self.assertEqual(status, 400)
        self.assertEqual(body, {"error": "signal_ingest_invalid"})

    def test_same_key_changed_content_is_invalid(self) -> None:
        self.assertEqual(self._json_post(envelope(sequence=1))[0], 201)
        changed = envelope(sequence=2, signal=_signal(observed=20))
        status, body = self._json_post(changed, key="k-1")
        self.assertEqual(status, 400)
        self.assertEqual(body, {"error": "signal_ingest_invalid"})

    def test_pending_source_blocks_new_keys(self) -> None:
        env = envelope()
        pending = {
            "source": "src-a",
            "key_id": "key-a",
            "sequence": 1,
            "region": "eu-north",
            "signal_version": 0,
            "signature": env["signature"],
            "state": "pending",
        }
        Path(self.receipts).write_bytes(si._serialize({"k-1": pending}))
        later = envelope(sequence=2, signal=_signal(observed=20))
        status, body = self._json_post(later, key="k-2")
        self.assertEqual(status, 400)
        self.assertEqual(body, {"error": "signal_ingest_invalid"})

    def test_missing_trust_file_is_not_found(self) -> None:
        os.unlink(self.trust)
        status, body = self._json_post(envelope())
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "signal_ingest_not_found"})

    def test_missing_parent_directory_is_not_found(self) -> None:
        self.server.signal_receipts = os.path.join(
            self.tmp.name, "missing", "ingest.json")
        status, body = self._json_post(envelope())
        self.assertEqual(status, 404)
        self.assertEqual(body, {"error": "signal_ingest_not_found"})

    def test_unwritable_ledger_is_unavailable(self) -> None:
        os.chmod(self.tmp.name, 0o500)
        self.addCleanup(os.chmod, self.tmp.name, 0o700)
        try:
            status, body = self._json_post(envelope())
        except OSError:
            self.skipTest("running as a user who can still write")
            return
        self.assertEqual(status, 503)
        self.assertEqual(body, {"error": "signal_ingest_unavailable"})

    def test_error_body_never_leaks_details(self) -> None:
        os.unlink(self.trust)
        _status, body, _headers = self._post(envelope())
        text = body.decode("utf-8")
        self.assertNotIn(self.tmp.name, text)
        self.assertNotIn(_SECRET_HEX, text)
        self.assertNotIn(envelope()["signature"], text)  # type: ignore[arg-type]
        self.assertEqual(set(json.loads(body)), {"error"})


class SignalIngestUnconfiguredTest(unittest.TestCase):
    def test_unconfigured_path_is_plain_404(self) -> None:
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever,
                                  daemon=True)
        thread.start()
        try:
            connection = HTTPConnection("127.0.0.1", server.server_port,
                                        timeout=2)
            raw = json.dumps({"envelope": envelope(), "key": "k-1"},
                             separators=(",", ":"))
            connection.request("POST", "/signals/ingest", body=raw,
                               headers={"Content-Type": "application/json"})
            response = connection.getresponse()
            self.assertEqual(response.status, 404)
            self.assertEqual(json.loads(response.read()),
                             {"error": "not_found"})
            connection.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)


class ServeSignalsArgumentsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.signals = os.path.join(self.tmp.name, "signals.json")
        self.trust = os.path.join(self.tmp.name, "trust.json")
        self.receipts = os.path.join(self.tmp.name, "ingest.json")

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "carbon_market", "serve", *args],
            capture_output=True, timeout=10)

    def test_incomplete_empty_or_repeated_group_exits_2(self) -> None:
        for args in (
            ("--signals", self.signals),
            ("--signals", self.signals, "--signal-trust", self.trust),
            ("--signal-trust", self.trust,
             "--signal-receipts", self.receipts),
            ("--signals", "", "--signal-trust", self.trust,
             "--signal-receipts", self.receipts),
            ("--signals", self.signals, "--signal-trust", "",
             "--signal-receipts", self.receipts),
            ("--signals", self.signals, "--signal-trust", self.trust,
             "--signal-receipts", ""),
            ("--signals", self.signals, "--signal-trust", self.trust,
             "--signal-receipts", self.receipts,
             "--signals", self.signals),
        ):
            with self.subTest(args=args):
                self.assertEqual(self._run(*args).returncode, 2)

    def test_valid_group_starts_and_serves(self) -> None:
        Path(self.trust).write_bytes(si._serialize_trust(_trust_doc()))
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        process = subprocess.Popen(
            [sys.executable, "-m", "carbon_market", "serve",
             "--host", "127.0.0.1", "--port", str(port),
             "--signals", self.signals,
             "--signal-trust", self.trust,
             "--signal-receipts", self.receipts])
        try:
            deadline = time.time() + 10
            outcome = None
            while time.time() < deadline:
                try:
                    connection = HTTPConnection("127.0.0.1", port,
                                                timeout=1)
                    raw = json.dumps({"envelope": envelope(), "key": "k-1"},
                                     separators=(",", ":"))
                    connection.request(
                        "POST", "/signals/ingest", body=raw,
                        headers={"Content-Type": "application/json"})
                    response = connection.getresponse()
                    body = json.loads(response.read())
                    outcome = (response.status, body["created"])
                    connection.close()
                    break
                except OSError:
                    time.sleep(0.05)
            self.assertEqual(outcome, (201, True))
        finally:
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
