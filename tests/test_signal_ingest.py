from __future__ import annotations

import hashlib
import hmac
import json
import os
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import signal_ingest, signals
from carbon_market.signal_ingest import ingest

_SECRET = "0123456789abcdef" * 2
_OTHER_SECRET = "fedcba9876543210" * 2


def _signal(region: str = "eu-north", observed: int = 10,
            expires: int = 100, **overrides: object) -> dict[str, object]:
    signal: dict[str, object] = {
        "region": region,
        "observed": observed,
        "expires": expires,
        "mix": {"solar": 6000, "wind": 4000},
        "unit_cost": 3,
        "carbon_intensity": 7,
    }
    signal.update(overrides)
    return signal


def _sign(source: str, key_id: str, sequence: int,
          signal: dict[str, object], secret: str = _SECRET) -> str:
    canonical_signal = {
        "region": signal["region"],
        "observed": signal["observed"],
        "expires": signal["expires"],
        "mix": {name: signal["mix"][name]  # type: ignore[index]
                for name in sorted(signal["mix"])},  # type: ignore[arg-type]
        "unit_cost": signal["unit_cost"],
        "carbon_intensity": signal["carbon_intensity"],
    }
    base = json.dumps([source, key_id, sequence, canonical_signal],
                      ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hmac.new(bytes.fromhex(secret), base,
                    hashlib.sha256).hexdigest()


def _envelope(sequence: int = 1, signal: dict[str, object] | None = None,
              *, source: str = "src-a", key_id: str = "k1",
              secret: str = _SECRET, **overrides: object) -> dict[str, object]:
    if signal is None:
        signal = _signal(observed=10 + sequence - 1)
    envelope: dict[str, object] = {
        "source": source,
        "key_id": key_id,
        "sequence": sequence,
        "signal": signal,
        "signature": _sign(source, key_id, sequence, signal, secret),
    }
    envelope.update(overrides)
    return envelope


def _trust_document() -> dict[str, object]:
    return {
        "src-a": {
            "key_id": "k1",
            "secret": _SECRET,
            "regions": ["eu-north", "us-west"],
            "valid_from": 0,
            "valid_until": 1000,
        },
    }


def _write_trust(path: str, document: object | None = None) -> None:
    if document is None:
        document = _trust_document()
    Path(path).write_text(
        json.dumps(document, ensure_ascii=False, separators=(",", ":"))
        + "\n",
        encoding="utf-8")


class IngestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.signals = os.path.join(self.tmp.name, "signals.json")
        self.trust = os.path.join(self.tmp.name, "trust.json")
        self.ledger = os.path.join(self.tmp.name, "receipts.json")
        _write_trust(self.trust)

    # -- happy path -----------------------------------------------------

    def test_first_ingest_publishes_and_finalizes(self) -> None:
        receipt, created = ingest(self.signals, self.trust, self.ledger,
                                  _envelope(1), "K1")
        self.assertTrue(created)
        self.assertEqual(list(receipt.keys()),
                         ["source", "key_id", "sequence", "region",
                          "signal_version", "signature", "state"])
        self.assertEqual(receipt, {
            "source": "src-a",
            "key_id": "k1",
            "sequence": 1,
            "region": "eu-north",
            "signal_version": 1,
            "signature": receipt["signature"],
            "state": "active",
        })
        record = signals.get(self.signals, "eu-north", 50)
        self.assertEqual(record["version"], 1)
        self.assertEqual(record["observed"], 10)

    def test_sequences_map_to_consecutive_signal_versions(self) -> None:
        r1, c1 = ingest(self.signals, self.trust, self.ledger,
                        _envelope(1), "K1")
        r2, c2 = ingest(self.signals, self.trust, self.ledger,
                        _envelope(2), "K2")
        r3, c3 = ingest(self.signals, self.trust, self.ledger,
                        _envelope(3, _signal("us-west", observed=30)), "K3")
        self.assertEqual([c1, c2, c3], [True, True, True])
        self.assertEqual([r1["signal_version"], r2["signal_version"]], [1, 2])
        self.assertEqual(r3["region"], "us-west")
        self.assertEqual(r3["signal_version"], 1)

    def test_ledger_is_canonical_and_never_holds_the_secret(self) -> None:
        envelope = _envelope(1)
        ingest(self.signals, self.trust, self.ledger, envelope, "K1")
        raw = Path(self.ledger).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        self.assertNotIn(b" ", raw.rstrip(b"\n"))
        text = raw.decode("utf-8")
        self.assertNotIn(_SECRET, text)
        data = json.loads(text)
        self.assertEqual(list(data.keys()), ["version", "receipts"])
        self.assertEqual(data["version"], 1)
        record = data["receipts"]["K1"]
        self.assertEqual(list(record.keys()),
                         ["source", "key_id", "sequence", "signal",
                          "signal_version", "signature", "state"])
        self.assertEqual(record["state"], "active")
        self.assertEqual(record["signal_version"], 1)

    def test_signals_file_keeps_the_existing_format(self) -> None:
        ingest(self.signals, self.trust, self.ledger, _envelope(1), "K1")
        data = json.loads(Path(self.signals).read_text(encoding="utf-8"))
        self.assertEqual(list(data.keys()),
                         ["version", "history", "idempotency", "audit"])
        # The derived idempotency key is stable and carries the identity.
        derived = signal_ingest._derived_key("src-a", "k1", 1)
        self.assertEqual(data["idempotency"], {derived: "eu-north"})
        self.assertEqual(data["audit"], {derived: {
            "key": derived, "region": "eu-north", "version": 1}})

    # -- replay ----------------------------------------------------------

    def test_active_replay_returns_original_without_writes(self) -> None:
        first, created_first = ingest(self.signals, self.trust, self.ledger,
                                      _envelope(1), "K1")
        ledger_before = Path(self.ledger).read_bytes()
        signals_before = Path(self.signals).read_bytes()
        second, created_second = ingest(self.signals, self.trust, self.ledger,
                                        _envelope(1, _signal(observed=10)),
                                        "K1")
        self.assertFalse(created_second)
        self.assertEqual(second, first)
        self.assertEqual(Path(self.ledger).read_bytes(), ledger_before)
        self.assertEqual(Path(self.signals).read_bytes(), signals_before)

    def test_replay_still_returns_false_after_later_sequences(self) -> None:
        first, _ = ingest(self.signals, self.trust, self.ledger,
                          _envelope(1), "K1")
        ingest(self.signals, self.trust, self.ledger, _envelope(2), "K2")
        ingest(self.signals, self.trust, self.ledger, _envelope(3), "K3")
        ledger_before = Path(self.ledger).read_bytes()
        signals_before = Path(self.signals).read_bytes()
        again, created = ingest(self.signals, self.trust, self.ledger,
                                _envelope(1, _signal(observed=10)), "K1")
        self.assertFalse(created)
        self.assertEqual(again, first)
        self.assertEqual(Path(self.ledger).read_bytes(), ledger_before)
        self.assertEqual(Path(self.signals).read_bytes(), signals_before)

    def test_same_key_with_any_changed_field_rejected(self) -> None:
        ingest(self.signals, self.trust, self.ledger, _envelope(1), "K1")
        variants = [
            (_envelope(1, source="src-b"), PermissionError),
            (_envelope(1, key_id="other"), PermissionError),
            (_envelope(2), ValueError),
            (_envelope(1, _signal(region="us-west")), ValueError),
            (_envelope(1, _signal(observed=11)), ValueError),
            (_envelope(1, _signal(expires=99)), ValueError),
            (_envelope(1, _signal(unit_cost=4)), ValueError),
            (_envelope(1, _signal(carbon_intensity=8)), ValueError),
            (_envelope(1, _signal(mix={"solar": 5999, "wind": 4001})),
             ValueError),
        ]
        for variant, expected in variants:
            with self.subTest(variant=variant):
                with self.assertRaises(expected):
                    ingest(self.signals, self.trust, self.ledger,
                           variant, "K1")
        tampered = _envelope(1)
        tampered["signature"] = "0" * 64
        with self.assertRaises(PermissionError):
            ingest(self.signals, self.trust, self.ledger, tampered, "K1")

    # -- authentication --------------------------------------------------

    def test_unknown_source_or_key_and_bad_signature(self) -> None:
        unknown_source = _envelope(1, source="ghost")
        unknown_source["signature"] = _sign(
            "ghost", "k1", 1, _signal())
        with self.assertRaises(PermissionError):
            ingest(self.signals, self.trust, self.ledger,
                   unknown_source, "K1")
        with self.assertRaises(PermissionError):
            ingest(self.signals, self.trust, self.ledger,
                    _envelope(1, key_id="ghost"), "K1")
        bad = _envelope(1)
        bad["signature"] = "f" * 64
        with self.assertRaises(PermissionError):
            ingest(self.signals, self.trust, self.ledger, bad, "K1")

    def test_empty_trust_file_allows_nothing(self) -> None:
        Path(self.trust).write_text("{}\n", encoding="utf-8")
        with self.assertRaises(PermissionError):
            ingest(self.signals, self.trust, self.ledger,
                   _envelope(1), "K1")

    def test_signature_must_cover_exact_canonical_array(self) -> None:
        # A signature computed with whitespace or a reordered array
        # must not authenticate.
        signal = _signal()
        canonical_signal = {
            "region": "eu-north", "observed": 10, "expires": 100,
            "mix": {"solar": 6000, "wind": 4000},
            "unit_cost": 3, "carbon_intensity": 7}
        wrong_order = json.dumps(
            ["src-a", "k1", canonical_signal, 1],
            separators=(",", ":")).encode("utf-8")
        with_spaces = json.dumps(
            ["src-a", "k1", 1, canonical_signal]).encode("utf-8")
        for raw in (wrong_order, with_spaces):
            envelope = _envelope(1)
            envelope["signature"] = hmac.new(
                bytes.fromhex(_SECRET), raw, hashlib.sha256).hexdigest()
            with self.assertRaises(PermissionError):
                ingest(self.signals, self.trust, self.ledger,
                       envelope, "K" + raw[:4].hex())

    def test_mix_key_order_in_envelope_does_not_change_signature(self) -> None:
        signal = _signal()
        signal["mix"] = {"wind": 4000, "solar": 6000}  # unsorted input
        receipt, _ = ingest(self.signals, self.trust, self.ledger,
                            _envelope(1, signal), "K1")
        self.assertEqual(receipt["region"], "eu-north")
        stored = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(list(stored["receipts"]["K1"]["signal"]["mix"]),
                         ["solar", "wind"])

    def test_key_validity_window_is_inclusive_at_observed(self) -> None:
        receipt, _ = ingest(self.signals, self.trust, self.ledger,
                            _envelope(1, _signal(observed=0)), "K0")
        self.assertEqual(receipt["state"], "active")
        ingest(self.signals, self.trust, self.ledger,
               _envelope(2, _signal(observed=1000, expires=2000)), "K1000")
        with self.assertRaises(ValueError):
            ingest(self.signals, self.trust, self.ledger,
                   _envelope(3, _signal(observed=1001, expires=2000)),
                   "K1001")

    def test_unauthorized_region_rejected(self) -> None:
        with self.assertRaises(ValueError):
            ingest(self.signals, self.trust, self.ledger,
                   _envelope(1, _signal("ap-south")), "K1")

    # -- sequence ordering -----------------------------------------------

    def test_sequence_must_strictly_increase_per_source(self) -> None:
        ingest(self.signals, self.trust, self.ledger, _envelope(5), "K5")
        for sequence in (5, 4, 1):
            with self.subTest(sequence=sequence):
                with self.assertRaises(ValueError):
                    ingest(self.signals, self.trust, self.ledger,
                           _envelope(sequence,
                                     _signal(observed=10 + sequence)),
                           f"L{sequence}")
        next_receipt, created = ingest(
            self.signals, self.trust, self.ledger, _envelope(6), "K6")
        self.assertTrue(created)
        self.assertEqual(next_receipt["sequence"], 6)

    def test_sequence_spaces_are_independent_per_source(self) -> None:
        doc = _trust_document()
        doc["src-b"] = {
            "key_id": "k2",
            "secret": _OTHER_SECRET,
            "regions": ["eu-north"],
            "valid_from": 0,
            "valid_until": 1000,
        }
        _write_trust(self.trust, doc)
        first, _ = ingest(self.signals, self.trust, self.ledger,
                          _envelope(1, source="src-a", key_id="k1"), "A")
        second, _ = ingest(
            self.signals, self.trust, self.ledger,
            _envelope(1, _signal(observed=11), source="src-b", key_id="k2",
                      secret=_OTHER_SECRET), "B")
        self.assertEqual(first["source"], "src-a")
        self.assertEqual(second["source"], "src-b")
        self.assertEqual(second["signal_version"], 2)

    # -- argument validation ---------------------------------------------

    def test_invalid_arguments(self) -> None:
        good = _envelope(1)
        with self.assertRaises(ValueError):
            ingest("", self.trust, self.ledger, good, "K1")
        with self.assertRaises(ValueError):
            ingest(self.signals, "", self.ledger, good, "K1")
        with self.assertRaises(ValueError):
            ingest(self.signals, self.trust, "", good, "K1")
        with self.assertRaises(ValueError):
            ingest(self.signals, self.trust, self.ledger, good, "")
        with self.assertRaises(ValueError):
            ingest(123, self.trust, self.ledger, good, "K1")  # type: ignore[arg-type]
        # The three files must be distinct real paths.
        with self.assertRaises(ValueError):
            ingest(self.signals, self.signals, self.ledger, good, "K1")

        bad_envelopes = [
            [],
            {},
            {**good, "extra": 1},
            {"key_id": "k1", "sequence": 1, "signal": _signal(),
             "signature": good["signature"]},  # missing source
            {**good, "source": ""},
            {**good, "source": 1},
            {**good, "key_id": ""},
            {**good, "sequence": 0},
            {**good, "sequence": -1},
            {**good, "sequence": True},
            {**good, "sequence": 1.5},
            {**good, "signature": "g" * 64},
            {**good, "signature": "a" * 63},
            {**good, "signature": "A" * 64},
            {**good, "signal": _signal(region="")},
            {**good, "signal": _signal(mix={"solar": 9999})},
        ]
        for bad in bad_envelopes:
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    ingest(self.signals, self.trust, self.ledger,
                           bad, "K1")

    def test_signal_validation_is_the_existing_one(self) -> None:
        # A valid signature over a structurally invalid signal still
        # fails argument validation before authentication.
        signal = _signal(mix={"solar": 9999})
        envelope = _envelope(1, signal)
        with self.assertRaises(ValueError):
            ingest(self.signals, self.trust, self.ledger, envelope, "K1")

    # -- files and error mapping -----------------------------------------

    def test_missing_trust_and_parents_raise_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            ingest(self.signals, os.path.join(self.tmp.name, "absent.json"),
                   self.ledger, _envelope(1), "K1")
        with self.assertRaises(FileNotFoundError):
            ingest(os.path.join(self.tmp.name, "d1", "signals.json"),
                   self.trust,
                   os.path.join(self.tmp.name, "d2", "receipts.json"),
                   _envelope(1), "K1")

    def test_non_canonical_trust_file_raises_value_error(self) -> None:
        doc = _trust_document()
        variants = [
            json.dumps(doc, indent=2),
            json.dumps(doc, separators=(",", ":")) + "\n\n",
            "{not json",
        ]
        # Escaping non-ASCII breaks canonical bytes as well.
        unicode_doc = {"源": {
            "key_id": "k1", "secret": _SECRET,
            "regions": ["eu-north", "us-west"],
            "valid_from": 0, "valid_until": 1000}}
        variants.append(json.dumps(
            unicode_doc, ensure_ascii=True, separators=(",", ":")) + "\n")
        # A reordered entry field is non-canonical too.
        variants.append(json.dumps(
            {"src-a": {
                "secret": _SECRET,
                "key_id": "k1",
                "regions": ["eu-north", "us-west"],
                "valid_from": 0,
                "valid_until": 1000}},
            separators=(",", ":")) + "\n")
        for raw in variants:
            with self.subTest(raw=raw[:30]):
                path = os.path.join(self.tmp.name, "trust-variant.json")
                Path(path).write_text(raw, encoding="utf-8")
                with self.assertRaises(ValueError):
                    ingest(self.signals, path, self.ledger,
                           _envelope(1), "K1")

    def test_invalid_trust_structures(self) -> None:
        entries: list[tuple[str, object]] = [
            ("root not object", []),
            ("bad key id", {"src-a": {"key_id": "", "secret": _SECRET,
                                      "regions": ["eu-north"],
                                      "valid_from": 0, "valid_until": 1}}),
            ("odd secret", {"src-a": {"key_id": "k1", "secret": "abc",
                                      "regions": ["eu-north"],
                                      "valid_from": 0, "valid_until": 1}}),
            ("upper secret", {"src-a": {"key_id": "k1", "secret": "AB" * 4,
                                        "regions": ["eu-north"],
                                        "valid_from": 0,
                                        "valid_until": 1}}),
            ("empty regions", {"src-a": {"key_id": "k1", "secret": _SECRET,
                                         "regions": [], "valid_from": 0,
                                         "valid_until": 1}}),
            ("unsorted regions", {"src-a": {
                "key_id": "k1", "secret": _SECRET,
                "regions": ["us-west", "eu-north"],
                "valid_from": 0, "valid_until": 1}}),
            ("duplicate regions", {"src-a": {
                "key_id": "k1", "secret": _SECRET,
                "regions": ["eu-north", "eu-north"],
                "valid_from": 0, "valid_until": 1}}),
            ("inverted interval", {"src-a": {
                "key_id": "k1", "secret": _SECRET,
                "regions": ["eu-north"], "valid_from": 9,
                "valid_until": 8}}),
            ("bool bound", {"src-a": {"key_id": "k1", "secret": _SECRET,
                                      "regions": ["eu-north"],
                                      "valid_from": False,
                                      "valid_until": 1}}),
        ]
        doc = _trust_document()
        doc["src-b"] = {
            "key_id": "k1",  # same key id as src-a
            "secret": _OTHER_SECRET,
            "regions": ["eu-north"],
            "valid_from": 0,
            "valid_until": 1,
        }
        entries.append(("duplicate key id", doc))
        for label, bad in entries:
            with self.subTest(label=label):
                path = os.path.join(self.tmp.name, "trust-bad.json")
                _write_trust(path, bad)
                with self.assertRaises(ValueError):
                    ingest(self.signals, path, self.ledger,
                           _envelope(1), "K1")

    def test_trust_is_reread_on_every_call(self) -> None:
        ingest(self.signals, self.trust, self.ledger, _envelope(1), "K1")
        rotated = _trust_document()
        rotated["src-c"] = {
            "key_id": "k9",
            "secret": _OTHER_SECRET,
            "regions": ["eu-north"],
            "valid_from": 0,
            "valid_until": 1000,
        }
        _write_trust(self.trust, rotated)
        receipt, created = ingest(
            self.signals, self.trust, self.ledger,
            _envelope(2, _signal(observed=11), source="src-c", key_id="k9",
                      secret=_OTHER_SECRET),
            "K2")
        self.assertTrue(created)
        self.assertEqual(receipt["source"], "src-c")

    def test_corrupt_ledger_raises_value_error_and_keeps_bytes(self) -> None:
        ingest(self.signals, self.trust, self.ledger, _envelope(1), "K1")
        for bad in ("{not json", '{"version": -0}'):
            with self.subTest(bad=bad):
                Path(self.ledger).write_text(bad, encoding="utf-8")
                with self.assertRaises(ValueError):
                    ingest(self.signals, self.trust, self.ledger,
                           _envelope(2, _signal(observed=11)), "K2")
                self.assertEqual(
                    Path(self.ledger).read_text(encoding="utf-8"), bad)

    def test_ledger_integrity_failures(self) -> None:
        # Two ingest keys may never share one (source, sequence).
        envelope = _envelope(1)
        record = {
            "source": "src-a", "key_id": "k1", "sequence": 1,
            "signal": _signal(observed=10),
            "signal_version": 1, "signature": envelope["signature"],
            "state": "active",
        }
        document = {"version": 1, "receipts": {"K1": record,
                                               "K2": dict(record)}}
        Path(self.ledger).write_text(
            json.dumps(document, ensure_ascii=False,
                       separators=(",", ":")) + "\n", encoding="utf-8")
        signals.publish(self.signals, _signal(observed=10), "seed")
        with self.assertRaises(ValueError):
            ingest(self.signals, self.trust, self.ledger,
                   _envelope(2, _signal(observed=11)), "K3")

    # -- crash recovery --------------------------------------------------

    def _write_pending(self, key: str, envelope: dict[str, object]) -> None:
        record = {
            "source": envelope["source"],
            "key_id": envelope["key_id"],
            "sequence": envelope["sequence"],
            "signal": envelope["signal"],
            "signal_version": None,
            "signature": envelope["signature"],
            "state": "pending",
        }
        document = {"version": 1, "receipts": {key: record}}
        Path(self.ledger).write_text(
            json.dumps(document, ensure_ascii=False,
                       separators=(",", ":")) + "\n", encoding="utf-8")

    def test_resume_from_pending_before_publication(self) -> None:
        envelope = _envelope(1)
        self._write_pending("K1", envelope)
        self.assertFalse(os.path.exists(self.signals))
        receipt, created = ingest(self.signals, self.trust, self.ledger,
                                  envelope, "K1")
        self.assertTrue(created)
        self.assertEqual(receipt["state"], "active")
        self.assertEqual(receipt["signal_version"], 1)
        # Resuming created exactly one signal version and one event.
        data = json.loads(Path(self.signals).read_text(encoding="utf-8"))
        self.assertEqual(len(data["history"]["eu-north"]), 1)
        self.assertEqual(len(data["audit"]), 1)
        again, created_again = ingest(self.signals, self.trust, self.ledger,
                                      envelope, "K1")
        self.assertFalse(created_again)
        self.assertEqual(again, receipt)

    def test_resume_from_pending_after_publication(self) -> None:
        envelope = _envelope(2)
        derived = signal_ingest._derived_key("src-a", "k1", 2)
        published, _ = signals.publish(self.signals, _signal(observed=11),
                                       derived)
        self.assertEqual(published["version"], 1)
        self._write_pending("K2", envelope)
        receipt, created = ingest(self.signals, self.trust, self.ledger,
                                  envelope, "K2")
        self.assertTrue(created)
        self.assertEqual(receipt["signal_version"], 1)
        data = json.loads(Path(self.signals).read_text(encoding="utf-8"))
        self.assertEqual(len(data["history"]["eu-north"]), 1)
        self.assertEqual(len(data["audit"]), 1)

    def test_conflicting_derived_key_rejects_before_pending(self) -> None:
        # The derived publish key exists but binds a different signal:
        # the request is rejected without ever saving a pending receipt.
        derived = signal_ingest._derived_key("src-a", "k1", 1)
        signals.publish(self.signals,
                        _signal(observed=10, unit_cost=99), derived)
        with self.assertRaises(ValueError):
            ingest(self.signals, self.trust, self.ledger,
                   _envelope(1), "K1")
        self.assertFalse(os.path.exists(self.ledger))

    # -- concurrency and hygiene -----------------------------------------

    def test_concurrent_first_ingestions_serialize(self) -> None:
        # Each worker races on its own source, key id and region: the
        # sequence spaces and signal histories are independent, so the
        # test exercises the cross-process/in-process lock serialization
        # without depending on arrival order.
        entries = _trust_document()
        count = 20
        for index in range(count):
            entries[f"src-{index:03d}"] = {
                "key_id": f"kid-{index:03d}",
                "secret": hashlib.sha256(f"secret-{index}".encode())
                .hexdigest(),
                "regions": [f"r{index:03d}"],
                "valid_from": 0,
                "valid_until": 1000,
            }
        # The trust file is canonical: sources in code-point order.
        doc = {name: entries[name] for name in sorted(entries)}
        _write_trust(self.trust, doc)
        errors: list[BaseException] = []
        results: list[object] = []
        results_lock = threading.Lock()
        start = threading.Event()

        def worker(index: int) -> None:
            try:
                start.wait(5)
                name = f"src-{index:03d}"
                key_id = f"kid-{index:03d}"
                secret = hashlib.sha256(f"secret-{index}".encode()) \
                    .hexdigest()
                envelope = _envelope(
                    1, _signal(f"r{index:03d}"),
                    source=name, key_id=key_id, secret=secret)
                result = ingest(self.signals, self.trust, self.ledger,
                                envelope, f"K{index:03d}")
                with results_lock:
                    results.append(result)
            except BaseException as exc:  # noqa: BLE001 - report all
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(count)]
        for thread in threads:
            thread.start()
        start.set()
        for thread in threads:
            thread.join(timeout=10)
        self.assertEqual(errors, [])
        self.assertEqual(len(results), count)
        data = json.loads(Path(self.signals).read_text(encoding="utf-8"))
        self.assertEqual(len(data["history"]), count)
        ledger = json.loads(Path(self.ledger).read_text(encoding="utf-8"))
        self.assertEqual(len(ledger["receipts"]), count)
        self.assertTrue(all(r["state"] == "active"
                            for r in ledger["receipts"].values()))

    def test_concurrent_replays_create_one_version(self) -> None:
        envelope = _envelope(1)
        outcomes: list[tuple[dict[str, object], bool]] = []
        lock = threading.Lock()

        def worker() -> None:
            outcome = ingest(self.signals, self.trust, self.ledger,
                             envelope, "K1")
            with lock:
                outcomes.append(outcome)

        threads = [threading.Thread(target=worker) for _ in range(12)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        self.assertEqual(len(outcomes), 12)
        self.assertEqual(sum(1 for _, created in outcomes if created), 1)
        receipts = {receipt["signature"] for receipt, _ in outcomes}
        self.assertEqual(len(receipts), 1)

    def test_no_tmp_files_left_behind(self) -> None:
        ingest(self.signals, self.trust, self.ledger, _envelope(1), "K1")
        leftovers = [name for name in os.listdir(self.tmp.name)
                     if name.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    def test_plain_publish_and_get_still_work(self) -> None:
        ingest(self.signals, self.trust, self.ledger, _envelope(1), "K1")
        record, created = signals.publish(self.signals,
                                          _signal(observed=21), "plain")
        self.assertTrue(created)
        self.assertEqual(record["version"], 2)
        self.assertEqual(signals.get(self.signals, "eu-north", 21)["version"],
                         2)


if __name__ == "__main__":
    unittest.main()
