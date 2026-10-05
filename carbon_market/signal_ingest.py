"""Authenticated ingestion of signed live signals.

The :mod:`carbon_market.signals` ledger persists signal versions without
asking where they came from; this module adds the source-authenticated
write path. A source proves its identity and the request's integrity
with an HMAC-SHA256 envelope, and every accepted request is recorded in
its own receipt ledger.

The envelope carries exactly five fields -- ``source``, ``key_id``,
``sequence``, ``signal`` and ``signature``. ``source`` and ``key_id``
are non-empty strings naming the sending source and the key it signed
with; ``sequence`` is a non-boolean positive integer; ``signal`` is a
signal object with the existing six fields
(``signals._SIGNAL_FIELDS``) and their existing validation; and
``signature`` is the lowercase hexadecimal HMAC-SHA256 -- exactly 64
digits -- of the signing text.

The signing text is the compact UTF-8 JSON array made, in order, of
``source``, ``key_id``, ``sequence`` and ``signal``; inside the signal
the six fields keep their fixed order and the ``mix`` keys are ordered
by Unicode code point. It therefore serializes independently of the
key order the envelope or signal dictionaries happened to arrive in.

The trust file is one canonical compact JSON document. It maps each
source to exactly ``key_id`` (the unique key identifier that source may
sign with), ``key`` (the key as a non-empty even-length lowercase
hexadecimal string), ``regions`` (an array of distinct non-empty region
strings the source is authorized for) and ``valid_from`` and
``valid_until`` (non-boolean non-negative moments); the source may use
its key exactly when the signal's ``observed`` moment lies in the
closed interval between them. Like every other persisted document its
on-disk bytes must be the canonical compact form, and a non-canonical
file is rejected.

Two identities decide admission. Each *source* accepts only strictly
increasing sequences -- a later request must carry a sequence greater
than every sequence already recorded for that source. The caller also
supplies an idempotency ``key`` for the ingest call itself: an exact
replay of the identical envelope under the same key returns the stored
receipt with ``False`` even once later sequences have been accepted,
while the same key carrying any changed envelope field is rejected. A
different key can never replay another key's sequence, because the
sequence is then not strictly increasing.

A first request first persists a ``pending`` receipt, then publishes
the signal under an idempotency key derived deterministically from the
authenticated request, and finally settles the receipt as ``active``
and returns ``True``. An interrupted request resumes from its pending
receipt on retry and can never produce a second signal version; an
active replay never rewrites a file.

Receipts carry, in order, ``source``, ``key_id``, ``sequence``,
``region``, ``signal_version``, ``signature`` and ``state``
(``pending`` or ``active``); no key material is ever stored.

The trust file, the receipt ledger and the signal file are handled
together under cross-process locks taken in resolved-real-path order
(the two ledgers exclusive, the trust file shared); distinct paths are
required. On a commit failure the pre-call bytes -- or the resumable
pending bytes -- are restored.

An unknown source or key identifier, or a signature that does not
match, raises :class:`PermissionError`. An expired/not-yet-valid key,
an unauthorized region, a non-increasing sequence, a conflicting
idempotency key, an illegal argument or a non-canonical trust or
receipt file raises :class:`ValueError`. A missing trust file or a
missing parent directory of either written file raises
:class:`FileNotFoundError`; every other locking or read/write failure
raises :class:`OSError`. The live query, clearing, migration and
completion paths, the HTTP service and the existing signal file format
are unchanged. Only the standard library is used.
"""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import json
import os
import tempfile
from typing import Any

from . import _lifecycle
from . import signals as _signals
from ._jsonio import finite_loads

__all__ = ["ingest"]

_VERSION = 1
_TRUST_FIELDS = ("key_id", "key", "regions", "valid_from", "valid_until")
_ENVELOPE_FIELDS = ("source", "key_id", "sequence", "signal", "signature")
_ROOT_FIELDS = ("version", "receipts")
_RECEIPT_FIELDS = ("source", "key_id", "sequence", "region",
                   "signal_version", "signature", "state")
_STATES = ("pending", "active")
_HEX_DIGITS = frozenset("0123456789abcdef")
# The idempotency keys published into the signal ledger all share this
# prefix, so an authenticated publication is recognizable and can never
# collide with a caller-chosen key of the plain publish entry point.
_IDEM_PREFIX = "signal-ingest:"

# The in-process mutex registry and the companion flock live in the
# shared lifecycle infrastructure -- the same lock identity the signal
# ledger itself uses; the names are kept as the module's own seams.
_Store = _lifecycle.Store
_get_store = _lifecycle.get_store
_file_lock = _lifecycle.file_lock


def _is_plain_int(value: object) -> bool:
    # bool is a subclass of int and must be rejected.
    return isinstance(value, int) and not isinstance(value, bool)


# ---------------------------------------------------------------------------
# Trust file
# ---------------------------------------------------------------------------

class _TrustKey:
    """One source's validated trust entry."""

    __slots__ = ("key_id", "secret", "regions", "valid_from", "valid_until")

    def __init__(self, key_id: str, secret: bytes, regions: frozenset[str],
                 valid_from: int, valid_until: int) -> None:
        self.key_id = key_id
        self.secret = secret
        self.regions = regions
        self.valid_from = valid_from
        self.valid_until = valid_until


def _validate_secret_key(raw: object) -> str:
    if not isinstance(raw, str) or not raw or len(raw) % 2 != 0 \
            or any(char not in _HEX_DIGITS for char in raw):
        raise ValueError("trust key must be a non-empty even-length "
                         "lowercase hexadecimal string")
    return raw


def _validate_trust(raw: object) -> tuple[dict[str, _TrustKey],
                                          dict[str, Any]]:
    # Parse and validate the trust document, returning the operational
    # key view and the canonical plain-data view used for the on-disk
    # byte check. Source keys must be ordered by code point and each
    # entry's fields in their fixed order, as in every canonical
    # document.
    if not isinstance(raw, dict) or not raw:
        raise ValueError("trust file must be a non-empty object mapping "
                         "each source to its key configuration")
    sources = list(raw)
    if sources != sorted(sources):
        raise ValueError("trust file sources must be ordered by code point")

    keys: dict[str, _TrustKey] = {}
    plain: dict[str, Any] = {}
    key_ids: set[str] = set()
    for source in sources:
        if not isinstance(source, str) or not source:
            raise ValueError("trust sources must be non-empty strings")
        entry = raw[source]
        if not isinstance(entry, dict) \
                or list(entry.keys()) != list(_TRUST_FIELDS):
            raise ValueError("trust entry must be an object with keys "
                             "key_id, key, regions, valid_from and "
                             "valid_until, in that order")

        key_id = entry["key_id"]
        if not isinstance(key_id, str) or not key_id:
            raise ValueError("trust key_id must be a non-empty string")
        if key_id in key_ids:
            raise ValueError("trust key_id values must be unique across "
                             "sources")

        key_hex = _validate_secret_key(entry["key"])

        regions_raw = entry["regions"]
        if not isinstance(regions_raw, list) or not regions_raw:
            raise ValueError("trust regions must be a non-empty array")
        regions: set[str] = set()
        plain_regions: list[str] = []
        for region in regions_raw:
            if not isinstance(region, str) or not region:
                raise ValueError("trust regions must be non-empty strings")
            if region in regions:
                raise ValueError("trust regions must be distinct")
            regions.add(region)
            plain_regions.append(region)
        # Regions are a set with no sequence semantics, so the canonical
        # bytes fix one order (code point) like every other mapping's
        # keys: two byte-different files must not compare equal.
        if plain_regions != sorted(plain_regions):
            raise ValueError("trust regions must be ordered by code point")

        valid_from = entry["valid_from"]
        valid_until = entry["valid_until"]
        for name, moment in (("valid_from", valid_from),
                             ("valid_until", valid_until)):
            if not _is_plain_int(moment) or moment < 0:
                raise ValueError(f"trust {name} must be a non-boolean "
                                 "non-negative integer")
        if valid_until < valid_from:
            raise ValueError("trust valid_until must not be earlier than "
                             "valid_from")

        key_ids.add(key_id)
        keys[source] = _TrustKey(
            key_id, bytes.fromhex(key_hex), frozenset(regions),
            valid_from, valid_until)
        plain[source] = {
            "key_id": key_id,
            "key": key_hex,
            "regions": plain_regions,
            "valid_from": valid_from,
            "valid_until": valid_until,
        }
    return keys, plain


def _serialize_trust(plain: dict[str, Any]) -> bytes:
    payload = {source: plain[source] for source in sorted(plain)}
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_trust(realpath: str) -> dict[str, _TrustKey]:
    with open(realpath, "rb") as handle:
        raw = handle.read()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"trust file {realpath!r} is not valid UTF-8") \
            from exc
    try:
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(f"trust file {realpath!r} is not valid JSON") \
            from exc
    keys, plain = _validate_trust(data)
    if raw != _serialize_trust(plain):
        raise ValueError(
            f"trust file {realpath!r} is not in canonical compact form")
    return keys


# ---------------------------------------------------------------------------
# Envelope and signing text
# ---------------------------------------------------------------------------

def _canonical_signal(signal: dict[str, Any]) -> dict[str, Any]:
    # The signal's six fields in their fixed order, mix ordered by code
    # point; values are already normalized by signals._normalize_signal.
    return {
        "region": signal["region"],
        "observed": signal["observed"],
        "expires": signal["expires"],
        "mix": {source: signal["mix"][source]
                for source in sorted(signal["mix"])},
        "unit_cost": signal["unit_cost"],
        "carbon_intensity": signal["carbon_intensity"],
    }


def _signing_bytes(source: str, key_id: str, sequence: int,
                   signal: dict[str, Any]) -> bytes:
    payload = [source, key_id, sequence, _canonical_signal(signal)]
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False)
    return text.encode("utf-8")


def _is_signature(raw: object) -> bool:
    return isinstance(raw, str) and len(raw) == 64 \
        and all(char in _HEX_DIGITS for char in raw)


def _validate_envelope(envelope: object) -> tuple[str, str, int,
                                                  dict[str, Any], str]:
    if not isinstance(envelope, dict) \
            or set(envelope.keys()) != set(_ENVELOPE_FIELDS):
        raise ValueError("envelope must be an object with exactly source, "
                         "key_id, sequence, signal and signature")
    source = envelope["source"]
    key_id = envelope["key_id"]
    sequence = envelope["sequence"]
    signature = envelope["signature"]
    if not isinstance(source, str) or not source:
        raise ValueError("envelope source must be a non-empty string")
    if not isinstance(key_id, str) or not key_id:
        raise ValueError("envelope key_id must be a non-empty string")
    if not _is_plain_int(sequence) or sequence < 1:
        raise ValueError("envelope sequence must be a non-boolean positive "
                         "integer")
    if not _is_signature(signature):
        raise ValueError("envelope signature must be 64 lowercase "
                         "hexadecimal digits")
    # The signal keeps the existing six fields and their full validation.
    normalized = _signals._normalize_signal(envelope["signal"])
    return source, key_id, sequence, normalized, signature


# ---------------------------------------------------------------------------
# Receipt ledger
# ---------------------------------------------------------------------------

def _validate_receipt(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict) or list(raw.keys()) != list(_RECEIPT_FIELDS):
        raise ValueError("receipts must be objects with keys source, "
                         "key_id, sequence, region, signal_version, "
                         "signature and state, in that order")
    source = raw["source"]
    key_id = raw["key_id"]
    sequence = raw["sequence"]
    region = raw["region"]
    signal_version = raw["signal_version"]
    signature = raw["signature"]
    state = raw["state"]
    if not isinstance(source, str) or not source:
        raise ValueError("receipt source must be a non-empty string")
    if not isinstance(key_id, str) or not key_id:
        raise ValueError("receipt key_id must be a non-empty string")
    if not _is_plain_int(sequence) or sequence < 1:
        raise ValueError("receipt sequence must be a positive integer")
    if not isinstance(region, str) or not region:
        raise ValueError("receipt region must be a non-empty string")
    # An active receipt froze a published version; a pending receipt was
    # written before the signal was published and carries 0.
    if not _is_plain_int(signal_version) or signal_version < 0 \
            or (state == "active") != (signal_version >= 1):
        raise ValueError("receipt signal_version must be 0 while pending "
                         "and a positive integer once active")
    if not _is_signature(signature):
        raise ValueError("receipt signature must be 64 lowercase "
                         "hexadecimal digits")
    if state not in _STATES:
        raise ValueError("receipt state must be pending or active")
    return {field: raw[field] for field in _RECEIPT_FIELDS}


def _validate_ledger(data: object) -> dict[str, dict[str, Any]]:
    if not isinstance(data, dict) or list(data.keys()) != list(_ROOT_FIELDS):
        raise ValueError("receipt ledger root must be an object with keys "
                         "version and receipts, in that order")
    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported receipt ledger version")
    receipts_raw = data["receipts"]
    if not isinstance(receipts_raw, dict):
        raise ValueError("receipt ledger receipts must be an object")
    keys = list(receipts_raw)
    if keys != sorted(keys):
        raise ValueError("receipt ledger keys must be ordered by code "
                         "point")
    receipts: dict[str, dict[str, Any]] = {}
    for ledger_key in keys:
        if not isinstance(ledger_key, str) or not ledger_key:
            raise ValueError("receipt ledger keys must be non-empty "
                             "strings")
        receipts[ledger_key] = _validate_receipt(receipts_raw[ledger_key])
    return receipts


def _serialize(receipts: dict[str, dict[str, Any]]) -> bytes:
    payload = {
        "version": _VERSION,
        "receipts": {key: receipts[key] for key in sorted(receipts)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_file(realpath: str) -> tuple[dict[str, dict[str, Any]],
                                       bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return {}, None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"receipt ledger {realpath!r} is not valid UTF-8") from exc
    try:
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"receipt ledger {realpath!r} is not valid JSON") from exc
    receipts = _validate_ledger(data)
    if raw != _serialize(receipts):
        raise ValueError(
            f"receipt ledger {realpath!r} is not in canonical compact form")
    return receipts, raw


def _fsync_directory(directory: str) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _rollback_file(realpath: str, directory: str, old_bytes: bytes | None,
                   first: BaseException) -> None:
    # Restore the exact pre-stage bytes while the exclusive lock is held
    # so an unsuccessful stage leaves a complete earlier (or resumable
    # pending) state, never a torn write.
    try:
        if old_bytes is None:
            try:
                os.unlink(realpath)
            except FileNotFoundError:
                pass
        else:
            fd, tmp_path = tempfile.mkstemp(
                dir=directory, prefix=".ingest-restore-", suffix=".tmp")
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(old_bytes)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp_path, realpath)
            except BaseException:
                with contextlib.suppress(OSError):
                    os.unlink(tmp_path)
                raise
        _fsync_directory(directory)
    except OSError as recovery:
        raise recovery from first


def _commit_file(realpath: str, receipts: dict[str, dict[str, Any]],
                 old_bytes: bytes | None) -> bytes:
    # One durable commit per stage; returns the bytes just committed,
    # which become the rollback target for the following stage.
    directory = os.path.dirname(realpath) or "."
    payload = _serialize(receipts)
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".ingest-", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, realpath)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise
    try:
        _fsync_directory(directory)
    except BaseException as first:
        _rollback_file(realpath, directory, old_bytes, first)
        raise
    return payload


def _request_key(source: str, key_id: str, sequence: int) -> str:
    # Stable textual form of the authenticated triple, used to derive
    # the signal ledger's idempotency key. The strings are JSON-quoted
    # so no separator can be spoofed; the sequence is a plain integer.
    return (json.dumps(source, ensure_ascii=False, separators=(",", ":"))
            + " " + json.dumps(key_id, ensure_ascii=False,
                                    separators=(",", ":"))
            + " " + str(sequence))


def _idem_key(source: str, key_id: str, sequence: int,
              signature: str) -> str:
    # Deterministically derived from the whole authenticated request:
    # the source, the key it signed with, its sequence and the exact
    # signature. An identical request derives the same key and replays
    # through signals' own idempotency; a changed field derives another.
    digest = hashlib.sha256(
        _request_key(source, key_id, sequence).encode("utf-8")
        + b" " + signature.encode("ascii")).hexdigest()
    return _IDEM_PREFIX + digest


def _public_receipt(receipt: dict[str, Any]) -> dict[str, Any]:
    # Fixed field order, no key material.
    return {field: receipt[field] for field in _RECEIPT_FIELDS}


def _same_envelope(existing: dict[str, Any], source: str, key_id: str,
                   sequence: int, region: str, signature: str) -> bool:
    return (existing["source"] == source
            and existing["key_id"] == key_id
            and existing["sequence"] == sequence
            and existing["region"] == region
            and existing["signature"] == signature)


def _highest_sequence(receipts: dict[str, dict[str, Any]],
                      source: str) -> int | None:
    highest: int | None = None
    for receipt in receipts.values():
        if receipt["source"] == source and (
                highest is None or receipt["sequence"] > highest):
            highest = receipt["sequence"]
    return highest


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def ingest(signals: str, trust: str, ledger: str, envelope: dict[str, Any],
           key: str) -> tuple[dict[str, Any], bool]:
    """Authenticate and persist one signed signal envelope.

    ``signals`` is the existing signal ledger path (its format and the
    :func:`signals.publish`/:func:`signals.get` semantics are
    unchanged), ``trust`` the canonical trust file and ``ledger`` the
    receipt ledger written by this module. The three paths and ``key``
    must be non-empty strings, the paths resolving to distinct real
    locations. ``key`` is the idempotency key of the ingest call: the
    receipt ledger is keyed by it, although the receipt itself never
    stores it.

    The envelope is validated and its HMAC checked against the trusted
    key for its source; an unknown source or ``key_id`` or a mismatched
    signature raises :class:`PermissionError`. The key must be valid at
    the signal's ``observed`` moment and the region must be authorized,
    else :class:`ValueError`. Sequences per source must be strictly
    increasing. Under an existing ``key`` an exact replay of the
    identical envelope returns the stored receipt with ``False`` even
    after later sequences were accepted and writes nothing; the same
    ``key`` carrying any changed field raises :class:`ValueError`
    (conflicting idempotency key), as does a derived signal publication
    key already bound to a different signal.

    A first request persists a ``pending`` receipt, publishes the signal
    under the derived idempotency key and then settles the receipt as
    ``active``; it returns ``(receipt, True)``. Retrying an interrupted
    request resumes from its pending receipt and never creates a second
    signal version. A missing trust file or a missing parent directory
    of either written file raises :class:`FileNotFoundError`;
    non-canonical trust or receipt files or illegal arguments raise
    :class:`ValueError`; other locking or read/write failures raise
    :class:`OSError`.
    """
    for value in (signals, trust, ledger, key):
        if not isinstance(value, str) or not value:
            raise ValueError("signals, trust, ledger paths and key must be "
                             "non-empty strings")

    source, key_id, sequence, signal, signature = _validate_envelope(
        envelope)
    region = signal["region"]

    trust_real = os.path.realpath(trust)
    ledger_real = os.path.realpath(ledger)
    signal_real = os.path.realpath(signals)
    if len({trust_real, ledger_real, signal_real}) != 3:
        raise ValueError("signals, trust and ledger paths must be distinct "
                         "real paths")

    publish_key = _idem_key(source, key_id, sequence, signature)

    # The receipt ledger drives the transaction; serialize per ledger
    # path in-process as well as across processes.
    store = _get_store(ledger)
    with store.lock:
        with contextlib.ExitStack() as stack:
            # Three files, one resolved-real-path lock order shared by
            # every caller: the two ledgers are written (exclusive), the
            # trust file is only read (shared).
            for locked, shared in sorted(
                    ((trust_real, True), (ledger_real, False),
                     (signal_real, False)), key=lambda item: item[0]):
                stack.enter_context(_file_lock(locked, shared=shared))

            trusted = _load_trust(trust_real)

            trusted_key = trusted.get(source)
            # Authentication failures do not reveal whether the source
            # or the key was unknown and carry no key material.
            if trusted_key is None or trusted_key.key_id != key_id:
                raise PermissionError("unknown signal source or key")
            expected = hmac.new(
                trusted_key.secret,
                _signing_bytes(source, key_id, sequence, signal),
                hashlib.sha256).hexdigest()
            if not hmac.compare_digest(expected, signature):
                raise PermissionError("envelope signature does not match")

            # The signature proved the source; enforce the key's validity
            # window and regional authorization.
            observed = signal["observed"]
            if not trusted_key.valid_from <= observed \
                    <= trusted_key.valid_until:
                raise ValueError("signing key is not valid at the signal "
                                 "observation moment")
            if region not in trusted_key.regions:
                raise ValueError("signal region is not authorized for the "
                                 "source")

            receipts, ledger_old = _load_file(ledger_real)
            history, idempotency, events, signal_old = \
                _signals._load_file(signal_real)

            existing = receipts.get(key)
            if existing is not None:
                # Replay takes precedence over the sequence check: the
                # identical request returns its stored receipt even once
                # later sequences were accepted; any changed field under
                # the same idempotency key is a conflict.
                if not _same_envelope(existing, source, key_id, sequence,
                                      region, signature):
                    raise ValueError("ingest idempotency key was already "
                                     "used with a different request")
                if existing["state"] == "active":
                    _check_published(existing, publish_key, signal,
                                     idempotency, events, history)
                    return _public_receipt(existing), False
                # A pending receipt is resumed below.
                resume_from = existing
            else:
                # A source with an unresolved pending receipt is a gap in
                # its stream: block every new request for that source
                # until the pending key resumes. Otherwise a higher
                # sequence could publish first and strand the pending
                # signal behind a later observation, making its promised
                # recovery impossible.
                pending = [ledger_key for ledger_key, receipt in
                           receipts.items()
                           if receipt["source"] == source
                           and receipt["state"] == "pending"]
                if pending:
                    raise ValueError("source has an unresolved pending "
                                     "ingest that must be resumed first")
                # A different ingest key can never reuse a sequence the
                # source already recorded.
                highest = _highest_sequence(receipts, source)
                if highest is not None and sequence <= highest:
                    raise ValueError("sequence must be strictly increasing "
                                     "per source")
                # The derived publish key must not bind another signal in
                # the existing ledger. The underlying region observation
                # ordering is checked up front as well, so after the
                # pending receipt is written the publication can only
                # fail on a read/write fault and a retry resumes cleanly.
                bound_region = idempotency.get(publish_key)
                if bound_region is not None:
                    event = events[publish_key]
                    bound = history[bound_region][event["version"] - 1]
                    if {field: bound[field]
                            for field in _signals._SIGNAL_FIELDS} != signal:
                        raise ValueError("derived signal idempotency key "
                                         "conflicts with an existing "
                                         "binding")
                versions = history.get(region)
                if versions is not None \
                        and signal["observed"] <= versions[-1]["observed"]:
                    raise ValueError("signal observation must increase "
                                     "across versions")
                resume_from = None

            return _adopt(
                resume_from, key, source, key_id, sequence, region,
                signature, ledger_real, receipts, ledger_old, signal_real,
                history, idempotency, events, signal_old, publish_key,
                signal)


def _check_published(
    existing: dict[str, Any], publish_key: str, signal: dict[str, Any],
    idempotency: dict[str, str], events: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
) -> None:
    # An active receipt is only returned when the signal ledger still
    # holds the exact publication the receipt froze; an active replay
    # itself never writes a byte.
    bound_region = idempotency.get(publish_key)
    if bound_region is None:
        raise ValueError("receipt references a missing signal publication")
    event = events[publish_key]
    record = history[bound_region][event["version"] - 1]
    if record["version"] != existing["signal_version"] \
            or {field: record[field]
                for field in _signals._SIGNAL_FIELDS} != signal:
        raise ValueError("receipt no longer matches its published signal")


def _adopt(
    resume_from: dict[str, Any] | None, key: str,
    source: str, key_id: str, sequence: int, region: str, signature: str,
    ledger_real: str, receipts: dict[str, dict[str, Any]],
    ledger_old: bytes | None,
    signal_real: str,
    history: dict[str, list[dict[str, Any]]], idempotency: dict[str, str],
    events: dict[str, dict[str, Any]], signal_old: bytes | None,
    publish_key: str, signal: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    if resume_from is None:
        # Stage 1: persist the pending receipt before the signal is
        # published, so an interruption leaves exactly this one
        # explainable, resumable stage.
        receipt: dict[str, Any] = {
            "source": source,
            "key_id": key_id,
            "sequence": sequence,
            "region": region,
            "signal_version": 0,
            "signature": signature,
            "state": "pending",
        }
        receipts[key] = receipt
        pending_bytes = _commit_file(ledger_real, receipts, ledger_old)
        stage3_target: bytes | None = pending_bytes
    else:
        # Resuming: the pending receipt survived the interruption; keep
        # it as the rollback target for the final commit.
        receipt = resume_from
        stage3_target = ledger_old

    # Stage 2: publish the existing signal under the derived key. Either
    # this call creates the one version, or the pre-crash call already
    # did and the ledger replays it with False -- a resume never creates
    # a second version. If the publication itself faults, the pending
    # receipt stays on disk as the recoverable state and signals' own
    # commit restores the pre-call signal bytes, so retrying the same
    # request resumes cleanly.
    record, _created = _signals._publish_locked(
        signal_real, signal, publish_key, history, idempotency, events,
        signal_old)
    receipt["signal_version"] = record["version"]
    receipt["state"] = "active"
    _commit_file(ledger_real, receipts, stage3_target)
    return _public_receipt(receipt), True
