from __future__ import annotations

import hashlib
import hmac
import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from carbon_market import signal_ingest as si
from carbon_market import signals

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


def _trust_doc(sources: dict[str, dict[str, object]] | None = None
               ) -> dict[str, object]:
    if sources is None:
        sources = {
            "src-a": {
                "key_id": "key-a",
                "key": _SECRET_HEX,
                "regions": ["eu-north", "us-west"],
                "valid_from": 0,
                "valid_until": 1000,
            },
        }
    return dict(sources)


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


class IngestTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.signals = os.path.join(self.tmp.name, "signals.json")
        self.trust = os.path.join(self.tmp.name, "trust.json")
        self.ledger = os.path.join(self.tmp.name, "ingest.json")
        Path(self.trust).write_bytes(
            si._serialize_trust(_trust_doc()))

    def ingest(self, env: dict[str, object], key: str = "k-1", **paths: str):
        kwargs = {"signals": paths.get("signals", self.signals),
                  "trust": paths.get("trust", self.trust),
                  "ledger": paths.get("ledger", self.ledger)}
        return si.ingest(envelope=env, key=key, **kwargs)


class HappyPathTest(IngestTestBase):
    def test_first_ingest_publishes_and_activates(self) -> None:
        receipt, created = self.ingest(envelope(), "k-1")
        self.assertTrue(created)
        self.assertEqual(list(receipt.keys()), list(_RECEIPT_FIELDS))
        self.assertEqual(receipt, {
            "source": "src-a",
            "key_id": "key-a",
            "sequence": 1,
            "region": "eu-north",
            "signal_version": 1,
            "signature": envelope()["signature"],
            "state": "active",
        })
        record = signals.get(self.signals, "eu-north", 10)
        self.assertEqual(record["version"], 1)
        self.assertEqual(record["unit_cost"], 3)

    def test_signature_covers_mix_code_point_order(self) -> None:
        # The mix arrives out of order; the signing text reorders it by
        # code point, so the signature still verifies and the published
        # mix is stored canonically.
        signal = _signal(mix={"wind": 4000, "solar": 6000})
        env = envelope(signal=signal)
        receipt, created = self.ingest(env)
        self.assertTrue(created)
        record = signals.get(self.signals, "eu-north", 10)
        self.assertEqual(list(record["mix"].keys()), ["solar", "wind"])

    def test_versions_accumulate_per_region(self) -> None:
        r1, c1 = self.ingest(
            envelope(sequence=1, signal=_signal(observed=10)), "k1")
        r2, c2 = self.ingest(
            envelope(sequence=2, signal=_signal(observed=20)), "k2")
        self.assertTrue(c1 and c2)
        self.assertEqual((r1["signal_version"], r2["signal_version"]), (1, 2))
        self.assertEqual(signals.get(self.signals, "eu-north", 20)["version"],
                         2)

    def test_second_region_starts_at_version_one(self) -> None:
        self.ingest(envelope(sequence=1), "k1")
        env = envelope(sequence=2,
                       signal=_signal("us-west", observed=0, expires=5))
        receipt, created = self.ingest(env, "k2")
        self.assertTrue(created)
        self.assertEqual(receipt["region"], "us-west")
        self.assertEqual(receipt["signal_version"], 1)


class ReplayTest(IngestTestBase):
    def test_exact_replay_returns_false_without_writing(self) -> None:
        env = envelope()
        first, c1 = self.ingest(env, "k-1")
        self.assertTrue(c1)
        ledger_before = Path(self.ledger).read_bytes()
        signal_before = Path(self.signals).read_bytes()
        second, c2 = self.ingest(env, "k-1")
        self.assertFalse(c2)
        self.assertEqual(second, first)
        self.assertEqual(Path(self.ledger).read_bytes(), ledger_before)
        self.assertEqual(Path(self.signals).read_bytes(), signal_before)

    def test_replay_after_later_sequence_still_returns_original(self) -> None:
        env1 = envelope(sequence=1)
        self.ingest(env1, "k1")
        self.ingest(envelope(sequence=2, signal=_signal(observed=20)), "k2")
        replay, created = self.ingest(env1, "k1")
        self.assertFalse(created)
        self.assertEqual(replay["sequence"], 1)
        self.assertEqual(replay["signal_version"], 1)
        self.assertEqual(replay["state"], "active")

    def test_same_key_changed_field_raises_value_error(self) -> None:
        self.ingest(envelope(sequence=1), "dup")
        cases = [
            envelope(sequence=5, signal=_signal(observed=50, expires=90)),
            envelope(sequence=1, signal=_signal(observed=11)),
            envelope(sequence=1, signal=_signal(unit_cost=4)),
            envelope(sequence=1, signal=_signal("us-west", observed=0)),
        ]
        for changed in cases:
            with self.subTest(changed=changed):
                with self.assertRaises(ValueError):
                    self.ingest(changed, "dup")

    def test_same_key_re_signed_changed_envelope_is_value_error(self) -> None:
        # A fully valid signature over a changed envelope under the same
        # ingest key is the idempotency conflict -> ValueError, never
        # PermissionError.
        self.ingest(envelope(sequence=1), "dup")
        changed = envelope(sequence=5, signal=_signal(observed=50))
        with self.assertRaises(ValueError):
            self.ingest(changed, "dup")

    def test_sequence_replay_under_another_key_raises_value_error(self) -> None:
        self.ingest(envelope(sequence=1), "k1")
        with self.assertRaises(ValueError):
            self.ingest(envelope(sequence=1), "other-key")

    def test_non_increasing_sequence_rejected(self) -> None:
        self.ingest(envelope(sequence=3), "k3")
        with self.assertRaises(ValueError):
            self.ingest(envelope(sequence=3), "k3again")
        with self.assertRaises(ValueError):
            self.ingest(envelope(sequence=2), "k2")
        # The higher sequence still goes through.
        receipt, created = self.ingest(
            envelope(sequence=4, signal=_signal(observed=20)), "k4")
        self.assertTrue(created)
        self.assertEqual(receipt["sequence"], 4)

    def test_sequence_is_per_source(self) -> None:
        other = bytes.fromhex("cd" * 32)
        doc = _trust_doc()
        doc["src-b"] = {
            "key_id": "key-b", "key": "cd" * 32,
            "regions": ["eu-north"], "valid_from": 0, "valid_until": 1000}
        Path(self.trust).write_bytes(si._serialize_trust(doc))
        self.ingest(envelope(sequence=7), "a")
        receipt, created = self.ingest(
            envelope("src-b", "key-b", 1, _signal(observed=20), other), "b")
        self.assertTrue(created)
        self.assertEqual(receipt["source"], "src-b")
        self.assertEqual(receipt["sequence"], 1)


class AuthenticationTest(IngestTestBase):
    def test_unknown_source_raises_permission_error(self) -> None:
        with self.assertRaises(PermissionError):
            self.ingest(envelope(source="src-x"), "k")

    def test_unknown_key_id_raises_permission_error(self) -> None:
        with self.assertRaises(PermissionError):
            self.ingest(envelope(key_id="nope"), "k")

    def test_bad_signature_raises_permission_error(self) -> None:
        for signature in ("00" * 32, "ff" * 32):
            env = envelope()
            env["signature"] = signature
            with self.assertRaises(PermissionError):
                self.ingest(env, "k")

    def test_tampered_envelope_raises_permission_error(self) -> None:
        env = envelope()
        env["sequence"] = 2
        with self.assertRaises(PermissionError):
            self.ingest(env, "k")
        env = envelope()
        env["signal"] = _signal(observed=11)
        with self.assertRaises(PermissionError):
            self.ingest(env, "k")

    def test_wrong_secret_raises_permission_error(self) -> None:
        env = envelope(secret=b"\x01" * 32)
        with self.assertRaises(PermissionError):
            self.ingest(env, "k")

    def test_signature_uses_canonical_signing_text(self) -> None:
        # Signing the envelope with pretty-printed JSON (or a different
        # field order) must not verify.
        signal = _signal()
        pretty = json.dumps(
            ["src-a", "key-a", 1, signal], ensure_ascii=False).encode()
        bad_sig = hmac.new(_SECRET, pretty, hashlib.sha256).hexdigest()
        env = envelope()
        env["signature"] = bad_sig
        with self.assertRaises(PermissionError):
            self.ingest(env, "k")


class AuthorizationTest(IngestTestBase):
    def test_unauthorized_region_raises_value_error(self) -> None:
        env = envelope(signal=_signal("ap-south", observed=10))
        with self.assertRaises(ValueError):
            self.ingest(env, "k")

    def test_key_validity_window(self) -> None:
        # Boundaries are inclusive.
        for observed, ok in ((0, True), (1000, True), (1001, False)):
            env = envelope(sequence=observed + 1,
                           signal=_signal(observed=observed,
                                          expires=2000))
            if ok:
                receipt, created = self.ingest(env, f"k-{observed}")
                self.assertTrue(created)
            else:
                with self.assertRaises(ValueError):
                    self.ingest(env, f"k-{observed}")


class EnvelopeValidationTest(IngestTestBase):
    def test_envelope_shape(self) -> None:
        good = envelope()
        for bad in (None, [], "x", 5,
                    {k: good[k] for k in good if k != "signature"},
                    dict(good, extra=1)):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    self.ingest(bad, "k")  # type: ignore[arg-type]

    def test_envelope_field_types(self) -> None:
        for field, value in (("source", ""), ("source", 1),
                            ("key_id", ""), ("key_id", 7),
                            ("sequence", 0), ("sequence", -1),
                            ("sequence", True), ("sequence", 1.5),
                            ("signature", "abc"),
                            ("signature", "A" * 64),
                            ("signature", "g" * 64)):
            env = envelope()
            env[field] = value
            with self.subTest(field=field, value=value):
                with self.assertRaises(ValueError):
                    self.ingest(env, "k")

    def test_signal_keeps_existing_validation(self) -> None:
        bad_signals = [
            _signal(region=""),
            _signal(observed=-1),
            _signal(observed=True),
            _signal(expires=9),
            _signal(unit_cost=-2),
            _signal(mix={}),
            _signal(mix={"solar": 9999}),
            _signal(mix={"solar": True, "wind": 9999}),
        ]
        for bad in bad_signals:
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    self.ingest(envelope(signal=bad), "k")

    def test_path_and_key_validation(self) -> None:
        env = envelope()
        with self.assertRaises(ValueError):
            si.ingest("", self.trust, self.ledger, env, "k")
        with self.assertRaises(ValueError):
            si.ingest(self.signals, self.trust, self.ledger, env, "")
        with self.assertRaises(ValueError):
            si.ingest(self.signals, self.trust, self.ledger, env, 5)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            si.ingest(self.signals, self.trust, self.signals, env, "k")

    def test_nothing_created_on_argument_error(self) -> None:
        with self.assertRaises(ValueError):
            self.ingest(envelope(sequence=True), "k")  # type: ignore[arg-type]
        self.assertFalse(os.path.exists(self.ledger))
        self.assertFalse(os.path.exists(self.signals))


class TrustFileTest(IngestTestBase):
    def _write(self, doc: object) -> None:
        Path(self.trust).write_text(json.dumps(doc, ensure_ascii=False),
                                   encoding="utf-8")

    def test_missing_trust_file_raises_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            self.ingest(envelope(), "k",
                        trust=os.path.join(self.tmp.name, "missing.json"))

    def test_non_canonical_trust_rejected(self) -> None:
        doc = _trust_doc()
        Path(self.trust).write_text(json.dumps(doc, indent=2),
                                   encoding="utf-8")
        with self.assertRaises(ValueError):
            self.ingest(envelope(), "k")

    def test_malformed_trust_rejected(self) -> None:
        for raw in ("{nope", '{"x": -0}', '{"x": NaN}',
                    "{}", "[]", '{"src-a": 1}'):
            Path(self.trust).write_text(raw, encoding="utf-8")
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    self.ingest(envelope(), "k")

    def test_bad_trust_entries_rejected(self) -> None:
        base = {
            "key_id": "key-a", "key": _SECRET_HEX,
            "regions": ["eu-north"], "valid_from": 0, "valid_until": 1000}
        bad_entries = [
            dict(base, key_id=""),
            dict(base, key="zz"),
            dict(base, key="abc"),
            dict(base, key="ABCDEF"),
            dict(base, regions=[]),
            dict(base, regions=[""]),
            dict(base, regions=["eu-north", "eu-north"]),
            dict(base, valid_from=-1),
            dict(base, valid_from=True),
            dict(base, valid_until=-1),
            dict(base, valid_from=10, valid_until=9),
            {"key_id": "key-a", "key": _SECRET_HEX,
             "regions": ["eu-north"], "valid_from": 0},
        ]
        for entry in bad_entries:
            Path(self.trust).write_bytes(
                si._serialize_trust({"src-a": entry}))
            with self.subTest(entry=entry):
                with self.assertRaises(ValueError):
                    self.ingest(envelope(), "k")

    def test_duplicate_key_id_rejected(self) -> None:
        doc = _trust_doc({
            "src-a": {"key_id": "shared", "key": _SECRET_HEX,
                      "regions": ["eu-north"], "valid_from": 0,
                      "valid_until": 1000},
            "src-b": {"key_id": "shared", "key": "cd" * 32,
                      "regions": ["eu-north"], "valid_from": 0,
                      "valid_until": 1000},
        })
        Path(self.trust).write_bytes(si._serialize_trust(doc))
        with self.assertRaises(ValueError):
            self.ingest(envelope(), "k")

    def test_regions_must_be_code_point_ordered(self) -> None:
        doc = _trust_doc({"src-a": {"key_id": "key-a", "key": _SECRET_HEX,
                                    "regions": ["us-west", "eu-north"],
                                    "valid_from": 0, "valid_until": 1000}})
        Path(self.trust).write_bytes(si._serialize_trust(doc))
        with self.assertRaises(ValueError):
            self.ingest(envelope(), "k")


class LedgerFormatTest(IngestTestBase):
    def test_receipt_ledger_is_canonical(self) -> None:
        self.ingest(envelope(sequence=1), "k1")
        raw = Path(self.ledger).read_text(encoding="utf-8")
        self.assertTrue(raw.endswith("\n"))
        self.assertFalse(raw.endswith("\n\n"))
        self.assertNotIn(" ", raw.split("\n", 1)[0])
        data = json.loads(raw)
        self.assertEqual(list(data.keys()), ["version", "receipts"])
        receipt = data["receipts"]["k1"]
        self.assertEqual(list(receipt.keys()), list(_RECEIPT_FIELDS))
        self.assertNotIn(_SECRET_HEX, raw)

    def test_pending_receipt_shape(self) -> None:
        # Force the process to stop between the pending commit and the
        # publication; the persisted pending receipt is canonical.
        real_commit = si._commit_file
        sentinel = {"done": False}

        def fail_first_pending(realpath, receipts, old_bytes):
            payload = real_commit(realpath, receipts, old_bytes)
            if not sentinel["done"] and receipts \
                    and any(r["state"] == "pending"
                            for r in receipts.values()):
                sentinel["done"] = True
                raise OSError("stop after pending")
            return payload

        with mock.patch.object(si, "_commit_file",
                               side_effect=fail_first_pending):
            with self.assertRaises(OSError):
                self.ingest(envelope(), "k1")
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        receipt = data["receipts"]["k1"]
        self.assertEqual(receipt["state"], "pending")
        self.assertEqual(receipt["signal_version"], 0)

    def test_corrupt_ledger_raises_value_error(self) -> None:
        for raw in ("{bad", '{"version": -0}', '{"version": 2}'):
            Path(self.ledger).write_text(raw, encoding="utf-8")
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    self.ingest(envelope(), "k")

    def test_non_canonical_ledger_raises_value_error(self) -> None:
        self.ingest(envelope(), "k1")
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        Path(self.ledger).write_text(json.dumps(data, indent=2),
                                    encoding="utf-8")
        with self.assertRaises(ValueError):
            self.ingest(envelope(sequence=2, signal=_signal(observed=20)),
                        "k2")

    def test_missing_ledger_parent_raises_file_not_found(self) -> None:
        missing = os.path.join(self.tmp.name, "no-such-dir", "ingest.json")
        with self.assertRaises(FileNotFoundError):
            self.ingest(envelope(), "k", ledger=missing)

    def test_missing_signal_parent_raises_file_not_found(self) -> None:
        missing = os.path.join(self.tmp.name, "no-such-dir", "signals.json")
        with self.assertRaises(FileNotFoundError):
            self.ingest(envelope(), "k", signals=missing)


class RecoveryTest(IngestTestBase):
    def _stop_after_pending(self) -> None:
        real_commit = si._commit_file
        state = {"done": False}

        def fail_once(realpath, receipts, old_bytes):
            payload = real_commit(realpath, receipts, old_bytes)
            if not state["done"] and receipts \
                    and any(r["state"] == "pending"
                            for r in receipts.values()):
                state["done"] = True
                raise OSError("stop after pending")
            return payload

        self._patch = mock.patch.object(si, "_commit_file",
                                        side_effect=fail_once)
        self._patch.start()
        self.addCleanup(self._patch.stop)

    def test_resume_from_pending_before_publish(self) -> None:
        self._stop_after_pending()
        env = envelope()
        with self.assertRaises(OSError):
            self.ingest(env, "k1")
        self.assertFalse(os.path.exists(self.signals))
        receipt, created = self.ingest(env, "k1")
        self.assertTrue(created)
        self.assertEqual(receipt["state"], "active")
        self.assertEqual(receipt["signal_version"], 1)
        data = json.loads(Path(self.signals).read_text(encoding="utf-8"))
        self.assertEqual(len(data["history"]["eu-north"]), 1)

    def test_resume_after_publish_does_not_republish(self) -> None:
        real_publish = si._signals._publish_locked

        def fail_on_create(realpath, signal, key, history, idempotency,
                           events, old_bytes):
            record, created = real_publish(
                realpath, signal, key, history, idempotency, events,
                old_bytes)
            if created:
                raise OSError("stop after publish")
            return record, created

        with mock.patch.object(si._signals, "_publish_locked",
                               side_effect=fail_on_create):
            with self.assertRaises(OSError):
                self.ingest(envelope(), "k1")

        data = json.loads(Path(self.signals).read_text(encoding="utf-8"))
        self.assertEqual(
            [r["version"] for r in data["history"]["eu-north"]], [1])

        receipt, created = self.ingest(envelope(), "k1")
        self.assertTrue(created)
        self.assertEqual(receipt["state"], "active")
        self.assertEqual(receipt["signal_version"], 1)
        data = json.loads(Path(self.signals).read_text(encoding="utf-8"))
        self.assertEqual(
            [r["version"] for r in data["history"]["eu-north"]], [1])

    def test_resume_is_idempotent_under_further_failures(self) -> None:
        # Fail the active finalization twice: the one signal version
        # stays published while the receipt stays pending, and every
        # retry replays the publication until the finalization lands.
        env = envelope()
        real_commit = si._commit_file

        def fail_active_twice(realpath, receipts, old_bytes):
            if receipts and any(r["state"] == "active"
                                for r in receipts.values()):
                fail_active_twice.failures += 1
                if fail_active_twice.failures <= 2:
                    # Refuse to write the active bytes at all, like a
                    # failure before the replace: the ledger keeps its
                    # pending content and the published signal survives.
                    raise OSError("stop before active")
            return real_commit(realpath, receipts, old_bytes)
        fail_active_twice.failures = 0

        with mock.patch.object(si, "_commit_file",
                               side_effect=fail_active_twice):
            with self.assertRaises(OSError):
                self.ingest(env, "k1")
            with self.assertRaises(OSError):
                self.ingest(env, "k1")
            receipt, created = self.ingest(env, "k1")
        self.assertTrue(created)
        self.assertEqual(receipt["signal_version"], 1)
        data = json.loads(Path(self.signals).read_text(encoding="utf-8"))
        self.assertEqual(len(data["history"]["eu-north"]), 1)


class ConcurrencyTest(IngestTestBase):
    def test_concurrent_distinct_requests_serialize(self) -> None:
        # Each thread is its own source writing its own region, so the
        # only contention is the shared three-file locking; no
        # sequence or observation-ordering rule can reject a request.
        n = 20
        doc = {}
        secrets = {}
        for i in range(n):
            name = f"src-{i:03d}"
            secret = (bytes([i + 1]) * 32)
            secrets[name] = secret
            doc[name] = {
                "key_id": f"key-{i:03d}",
                "key": secret.hex(),
                "regions": [f"r-{i:03d}"],
                "valid_from": 0,
                "valid_until": 1000,
            }
        Path(self.trust).write_bytes(si._serialize_trust(doc))
        errors: list[BaseException] = []

        def worker(index: int) -> None:
            try:
                name = f"src-{index:03d}"
                env = envelope(
                    name, f"key-{index:03d}", 1,
                    _signal(f"r-{index:03d}", observed=0),
                    secrets[name])
                self.ingest(env, f"k{index:03d}")
            except BaseException as exc:  # noqa: BLE001 - report all
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(n)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        self.assertEqual(errors, [])
        data = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(len(data["receipts"]), n)
        signals_data = json.loads(
            Path(self.signals).read_text(encoding="utf-8"))
        self.assertEqual(len(signals_data["history"]), n)

    def test_concurrent_same_request_accepted_once(self) -> None:
        results: list[tuple[bool, int]] = []
        lock = threading.Lock()
        env = envelope()

        def worker() -> None:
            try:
                receipt, created = self.ingest(env, "same-key")
                with lock:
                    results.append((created, receipt["signal_version"]))
            except BaseException as exc:  # noqa: BLE001 - report all
                with lock:
                    results.append(exc)  # type: ignore[arg-type]

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        self.assertEqual(len(results), 10)
        self.assertTrue(all(not isinstance(r, BaseException) for r in results))
        self.assertEqual(sum(1 for created, _ in results if created), 1)
        self.assertEqual({version for _, version in results}, {1})


class SignalsUnchangedTest(IngestTestBase):
    def test_plain_publish_still_works_alongside_ingest(self) -> None:
        self.ingest(envelope(sequence=1, signal=_signal(observed=10)), "k1")
        # The plain, unauthenticated entry point keeps its own key space
        # and semantics.
        record, created = signals.publish(
            self.signals, _signal(observed=30), "plain-key")
        self.assertTrue(created)
        self.assertEqual(record["version"], 2)

    def test_get_semantics_unchanged(self) -> None:
        self.ingest(envelope(sequence=1), "k1")
        self.assertEqual(
            signals.get(self.signals, "eu-north", 10)["version"], 1)
        with self.assertRaises(LookupError):
            signals.get(self.signals, "eu-north", 101)
        with self.assertRaises(KeyError):
            signals.get(self.signals, "nowhere", 10)


class EdgeCaseTest(IngestTestBase):
    def test_non_ascii_source_region_and_mix(self) -> None:
        secret = bytes.fromhex("77" * 32)
        doc = _trust_doc({
            "来源": {"key_id": "密钥一", "key": "77" * 32,
                    "regions": ["欧北"], "valid_from": 0,
                    "valid_until": 1000}})
        Path(self.trust).write_bytes(si._serialize_trust(doc))
        signal = _signal("欧北", observed=10,
                         mix={"太阳能": 6000, "风电": 4000})
        env = envelope("来源", "密钥一", 1, signal, secret)
        receipt, created = self.ingest(env, "键")
        self.assertTrue(created)
        self.assertEqual(receipt["region"], "欧北")
        raw = Path(self.ledger).read_text(encoding="utf-8")
        self.assertIn("来源", raw)
        self.assertNotIn(" ", raw.split("\n", 1)[0])

    def test_observation_ordering_failure_raises_value_error(self) -> None:
        self.ingest(envelope(sequence=1, signal=_signal(observed=10)), "k1")
        # A strictly later sequence whose signal does not advance the
        # region's observation is rejected before anything is written.
        ledger_before = Path(self.ledger).read_bytes()
        with self.assertRaises(ValueError):
            self.ingest(
                envelope(sequence=2, signal=_signal(observed=10)), "k2")
        self.assertEqual(Path(self.ledger).read_bytes(), ledger_before)

    def test_active_replay_checks_published_signal(self) -> None:
        env = envelope()
        self.ingest(env, "k1")
        # Corrupting the signal ledger so the receipt no longer matches
        # turns the active replay into ValueError rather than returning a
        # stale receipt.
        data = json.loads(Path(self.signals).read_text(encoding="utf-8"))
        data["history"]["eu-north"][0]["unit_cost"] = 999
        Path(self.signals).write_text(json.dumps(data, separators=(",", ":"))
                                      + "\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            self.ingest(env, "k1")

    def test_derived_key_collision_is_value_error(self) -> None:
        # Pre-seed the signal ledger with a plain publication under the
        # exact derived key but a different signal: the ingest must
        # refuse rather than publish a second binding.
        env = envelope()
        publish_key = si._idem_key(
            env["source"], env["key_id"], env["sequence"],  # type: ignore[arg-type]
            env["signature"])  # type: ignore[arg-type]
        signals.publish(
            self.signals,
            _signal("us-west", observed=0, expires=5), publish_key)
        with self.assertRaises(ValueError):
            self.ingest(env, "k1")

    def test_unresolved_pending_blocks_new_requests_for_source(self) -> None:
        real_commit = si._commit_file
        state = {"done": False}

        def fail_once(realpath, receipts, old_bytes):
            payload = real_commit(realpath, receipts, old_bytes)
            if not state["done"] and receipts and any(
                    r["state"] == "pending" for r in receipts.values()):
                state["done"] = True
                raise OSError("stop")
            return payload

        with mock.patch.object(si, "_commit_file", side_effect=fail_once):
            with self.assertRaises(OSError):
                self.ingest(envelope(sequence=1), "k1")
        # Neither a higher sequence nor the same sequence under another
        # ingest key may advance the source while k1 is pending.
        with self.assertRaises(ValueError):
            self.ingest(
                envelope(sequence=2, signal=_signal(observed=20)), "k2")
        with self.assertRaises(ValueError):
            self.ingest(envelope(sequence=1), "other")
        # Resuming the pending key clears the block.
        receipt, created = self.ingest(envelope(sequence=1), "k1")
        self.assertTrue(created)
        receipt2, created2 = self.ingest(
            envelope(sequence=2, signal=_signal(observed=20)), "k2")
        self.assertTrue(created2)
        self.assertEqual(receipt2["signal_version"], 2)

    def test_pending_does_not_block_its_own_replay_key(self) -> None:
        real_commit = si._commit_file
        state = {"done": False}

        def fail_once(realpath, receipts, old_bytes):
            payload = real_commit(realpath, receipts, old_bytes)
            if not state["done"] and receipts and any(
                    r["state"] == "pending" for r in receipts.values()):
                state["done"] = True
                raise OSError("stop")
            return payload

        with mock.patch.object(si, "_commit_file", side_effect=fail_once):
            with self.assertRaises(OSError):
                self.ingest(envelope(), "k1")
        # The same ingest key resumes; a *different* key for the same
        # source is rejected while the pending receipt is unresolved.
        with self.assertRaises(ValueError):
            self.ingest(envelope(), "other-key")
        receipt, created = self.ingest(envelope(), "k1")
        self.assertTrue(created)
        self.assertEqual(receipt["state"], "active")


if __name__ == "__main__":
    unittest.main()
