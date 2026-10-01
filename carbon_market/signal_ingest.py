"""Authenticated, replay-safe ingestion for live signals.

The :mod:`carbon_market.signals` ledger accepts every publication
equally; this module adds a source-authenticated front door with
per-source anti-replay sequence numbers while leaving
``signals.publish`` and ``signals.get`` untouched.

:func:`ingest` takes the signals path, a trust file, an independent
receipt ledger and one signed envelope under a caller-chosen
idempotency key. The envelope carries exactly ``source``, ``key_id``,
``sequence``, ``signal`` and ``signature``: the signal is the existing
six-field live signal with its existing validation, the sequence is a
non-boolean positive integer, and the signature is 64 lowercase
hexadecimal digits -- an HMAC-SHA256 over the compact UTF-8 JSON array
``[source, key_id, sequence, signal]``, the signal keeping its six
fields in their fixed order and the ``mix`` keys ordered by Unicode
code point.

The trust file is canonical JSON keyed by source; each source
configures one ``key_id``, one lowercase hexadecimal ``secret``, the
list of authorized regions and the inclusive validity interval
``valid_from``/``valid_until``. The key must be valid at the signal's
observation moment and the region must be authorized. Each source only
accepts a strictly increasing sequence; a replay of the exact same
request under the same idempotency key returns the original receipt
with ``False`` even after later sequences, while the same key carrying
any changed field is rejected.

A first ingestion durably saves a ``pending`` receipt first, then
publishes the signal through the existing ``signals.publish``
machinery under a stably derived idempotency key, and only then
finalizes the receipt as ``active`` and returns ``True``. An
interruption resumes from the persisted pending receipt, never
creating a second signal version; an active replay rewrites nothing.

The receipt always carries, in order, ``source``, ``key_id``,
``sequence``, ``region``, ``signal_version``, ``signature`` and
``state``; the secret is never persisted in the receipt ledger.

An unknown source or key id, or a signature mismatch, raises
``PermissionError``; an expired key, an unauthorized region, a
non-increasing sequence, an invalid argument, a non-canonical file or
a conflicting publish idempotency key raises ``ValueError``; a missing
trust file or output parent raises ``FileNotFoundError``; every other
locking or I/O failure raises ``OSError``.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import hmac
import json
import os
import re
import tempfile
import threading
from typing import Any, Iterator

from . import signals as _signals
from ._jsonio import finite_loads

__all__ = ["ingest"]

_VERSION = 1
_LOCK_SUFFIX = ".lock"
_SIGNAL_FIELDS = _signals._SIGNAL_FIELDS
_ENVELOPE_FIELDS = ("source", "key_id", "sequence", "signal", "signature")
_TRUST_FIELDS = ("key_id", "secret", "regions", "valid_from", "valid_until")
_RECORD_FIELDS = ("source", "key_id", "sequence", "signal",
                  "signal_version", "signature", "state")
_ROOT_FIELDS = ("version", "receipts")
_STATES = ("pending", "active")
_SIGNATURE_RE = re.compile(r"[0-9a-f]{64}")
_SECRET_RE = re.compile(r"[0-9a-f]{2,}")


class _Store:
    def __init__(self, realpath: str) -> None:
        self.realpath = realpath
        self.lock = threading.Lock()


_stores_lock = threading.Lock()
_stores: dict[str, _Store] = {}


def _get_store(path: str) -> _Store:
    realpath = os.path.realpath(path)
    with _stores_lock:
        store = _stores.get(realpath)
        if store is None:
            store = _Store(realpath)
            _stores[realpath] = store
        return store


def _is_plain_int(value: object) -> bool:
    # bool is a subclass of int and must be rejected.
    return isinstance(value, int) and not isinstance(value, bool)


@contextlib.contextmanager
def _flock(realpath: str, *, shared: bool = False) -> Iterator[None]:
    # Same companion-lock convention as the other ledgers: the lock
    # file is never unlinked and the kernel releases the flock on exit.
    lock_path = realpath + _LOCK_SUFFIX
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _fsync_directory(directory: str) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _commit(realpath: str, payload: bytes, base_bytes: bytes | None) -> None:
    # Synced same-directory temporary and atomic replace; a failure at
    # or after the replace restores base_bytes -- the pre-call bytes for
    # the pending commit, the still-recoverable pending bytes for the
    # final commit -- so no unsuccessful write leaves a half state.
    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".signal-ingest-", suffix=".tmp")
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
        _signals._rollback_file(realpath, directory, base_bytes, first)
        raise


# --------------------------------------------------------------------- #
# Trust file
# --------------------------------------------------------------------- #

def _validate_trust(data: object) -> dict[str, dict[str, Any]]:
    if not isinstance(data, dict):
        raise ValueError("trust file must be an object keyed by source")
    sources: dict[str, dict[str, Any]] = {}
    for source, entry_raw in data.items():
        if not isinstance(source, str) or not source:
            raise ValueError("trust sources must be non-empty strings")
        if not isinstance(entry_raw, dict) \
                or list(entry_raw.keys()) != list(_TRUST_FIELDS):
            raise ValueError("trust entries must be objects with keys "
                             "key_id, secret, regions, valid_from and "
                             "valid_until, in that order")
        key_id = entry_raw["key_id"]
        if not isinstance(key_id, str) or not key_id:
            raise ValueError("trust key_id must be a non-empty string")
        secret = entry_raw["secret"]
        if not isinstance(secret, str) or len(secret) % 2 != 0 \
                or not _SECRET_RE.fullmatch(secret):
            raise ValueError("trust secret must be an even-length string "
                             "of lowercase hexadecimal digits")
        regions_raw = entry_raw["regions"]
        if not isinstance(regions_raw, list) or not regions_raw:
            raise ValueError("trust regions must be a non-empty list")
        regions: list[str] = []
        for region in regions_raw:
            if not isinstance(region, str) or not region:
                raise ValueError("trust regions must be non-empty strings")
            if region in regions:
                raise ValueError("trust regions must be distinct")
            regions.append(region)
        if regions != sorted(regions):
            raise ValueError("trust regions must be ordered by code point")
        valid_from = entry_raw["valid_from"]
        valid_until = entry_raw["valid_until"]
        if not _is_plain_int(valid_from) or valid_from < 0:
            raise ValueError("valid_from must be a non-boolean "
                             "non-negative integer")
        if not _is_plain_int(valid_until) or valid_until < 0:
            raise ValueError("valid_until must be a non-boolean "
                             "non-negative integer")
        if valid_until < valid_from:
            raise ValueError("trust validity interval must not end "
                             "before it starts")
        sources[source] = {
            "key_id": key_id,
            "secret": secret,
            "regions": regions,
            "valid_from": valid_from,
            "valid_until": valid_until,
        }
    if list(data) != sorted(data):
        raise ValueError("trust sources must be ordered by code point")
    # A key id is a unique identifier for one secret: the same key id
    # under two sources would make an authenticated envelope ambiguous.
    key_owners: dict[str, str] = {}
    for source, entry in sources.items():
        owner = key_owners.get(entry["key_id"])
        if owner is not None:
            raise ValueError("trust key_id must be unique across sources")
        key_owners[entry["key_id"]] = source
    return sources


def _serialize_trust(sources: dict[str, dict[str, Any]]) -> bytes:
    payload = {source: {
        "key_id": sources[source]["key_id"],
        "secret": sources[source]["secret"],
        "regions": list(sources[source]["regions"]),
        "valid_from": sources[source]["valid_from"],
        "valid_until": sources[source]["valid_until"],
    } for source in sorted(sources)}
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_trust(realpath: str) -> tuple[dict[str, dict[str, Any]], bytes]:
    # Unlike the ledgers, the trust file must already exist: ingestion
    # without configured sources cannot authenticate anything.
    with open(realpath, "rb") as handle:
        raw = handle.read()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"trust file {realpath!r} is not valid UTF-8") from exc
    try:
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"trust file {realpath!r} is not valid JSON") from exc
    sources = _validate_trust(data)
    if raw != _serialize_trust(sources):
        raise ValueError(
            f"trust file {realpath!r} is not in canonical compact form")
    return sources, raw


# --------------------------------------------------------------------- #
# Envelope
# --------------------------------------------------------------------- #

def _normalize_envelope(envelope: object) -> dict[str, Any]:
    if not isinstance(envelope, dict) \
            or set(envelope.keys()) != set(_ENVELOPE_FIELDS):
        raise ValueError("envelope must be an object with exactly source, "
                         "key_id, sequence, signal and signature")
    source = envelope["source"]
    if not isinstance(source, str) or not source:
        raise ValueError("envelope source must be a non-empty string")
    key_id = envelope["key_id"]
    if not isinstance(key_id, str) or not key_id:
        raise ValueError("envelope key_id must be a non-empty string")
    sequence = envelope["sequence"]
    if not _is_plain_int(sequence) or sequence < 1:
        raise ValueError("envelope sequence must be a non-boolean positive "
                         "integer")
    # The signal keeps the existing six fields and all their rules.
    signal = _signals._normalize_signal(envelope["signal"])
    signature = envelope["signature"]
    if not isinstance(signature, str) or not _SIGNATURE_RE.fullmatch(
            signature):
        raise ValueError("envelope signature must be 64 lowercase "
                         "hexadecimal digits")
    return {
        "source": source,
        "key_id": key_id,
        "sequence": sequence,
        "signal": signal,
        "signature": signature,
    }


def _signing_bytes(source: str, key_id: str, sequence: int,
                   signal: dict[str, Any]) -> bytes:
    # The signed text is the compact UTF-8 JSON array in exactly the
    # order source, key_id, sequence, signal; the signal keeps the six
    # field order and the normalized mix is code-point sorted.
    canonical_signal = {field: signal[field] for field in _SIGNAL_FIELDS}
    payload = [source, key_id, sequence, canonical_signal]
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False)
    return text.encode("utf-8")


# --------------------------------------------------------------------- #
# Receipt ledger
# --------------------------------------------------------------------- #

def _validate_record(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict) or list(raw.keys()) != list(_RECORD_FIELDS):
        raise ValueError("receipt records must be objects with keys "
                         "source, key_id, sequence, signal, "
                         "signal_version, signature and state, in that "
                         "order")
    source = raw["source"]
    if not isinstance(source, str) or not source:
        raise ValueError("receipt source must be a non-empty string")
    key_id = raw["key_id"]
    if not isinstance(key_id, str) or not key_id:
        raise ValueError("receipt key_id must be a non-empty string")
    sequence = raw["sequence"]
    if not _is_plain_int(sequence) or sequence < 1:
        raise ValueError("receipt sequence must be a positive integer")
    # The stored signal is the existing six-field signal in its fixed
    # field order; _normalize_signal re-runs every value rule and
    # code-point-sorts the mix.
    signal = _signals._normalize_signal(raw["signal"])
    signal_version = raw["signal_version"]
    state = raw["state"]
    if state not in _STATES:
        raise ValueError("receipt state must be pending or active")
    if state == "pending":
        if signal_version is not None:
            raise ValueError("a pending receipt must not yet carry a "
                             "signal version")
    elif not _is_plain_int(signal_version) or signal_version < 1:
        raise ValueError("an active receipt must carry a positive signal "
                         "version")
    signature = raw["signature"]
    if not isinstance(signature, str) or not _SIGNATURE_RE.fullmatch(
            signature):
        raise ValueError("receipt signature must be 64 lowercase "
                         "hexadecimal digits")
    return {
        "source": source,
        "key_id": key_id,
        "sequence": sequence,
        "signal": signal,
        "signal_version": signal_version,
        "signature": signature,
        "state": state,
    }


def _serialize_ledger(receipts: dict[str, dict[str, Any]]) -> bytes:
    ordered = {key: _record_payload(receipts[key])
               for key in sorted(receipts)}
    document = {"version": _VERSION, "receipts": ordered}
    text = json.dumps(document, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _record_payload(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": record["source"],
        "key_id": record["key_id"],
        "sequence": record["sequence"],
        "signal": {field: record["signal"][field]
                   for field in _SIGNAL_FIELDS},
        "signal_version": record["signal_version"],
        "signature": record["signature"],
        "state": record["state"],
    }


def _load_ledger(realpath: str) -> tuple[dict[str, dict[str, Any]],
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
            f"ingest ledger {realpath!r} is not valid UTF-8") from exc
    try:
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"ingest ledger {realpath!r} is not valid JSON") from exc
    if not isinstance(data, dict) or list(data.keys()) != list(_ROOT_FIELDS):
        raise ValueError("ingest ledger root must be an object with keys "
                         "version and receipts, in that order")
    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported ingest ledger version")
    receipts_raw = data["receipts"]
    if not isinstance(receipts_raw, dict):
        raise ValueError("ingest ledger receipts must be an object")
    if list(receipts_raw) != sorted(receipts_raw):
        raise ValueError("ingest ledger receipts must be ordered by key "
                         "code point")
    receipts: dict[str, dict[str, Any]] = {}
    for key, record_raw in receipts_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("ingest keys must be non-empty strings")
        receipts[key] = _validate_record(record_raw)
    if raw != _serialize_ledger(receipts):
        raise ValueError(
            f"ingest ledger {realpath!r} is not in canonical compact form")
    return receipts, raw


def _check_ledger_integrity(
    receipts: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
    idempotency: dict[str, str],
    events: dict[str, dict[str, Any]],
) -> dict[str, int]:
    # The ledger describes one append-only sequence story per source:
    # distinct ingest keys never share one (source, sequence) pair, and
    # every active receipt's frozen signal must exist, version for
    # version, in the signals ledger, published under exactly the
    # receipt's derived idempotency key -- so two receipts can never
    # claim one published version. Pending receipts predate their
    # publication and have no signal reference yet.
    high_water: dict[str, int] = {}
    seen: set[tuple[str, int]] = set()
    for record in receipts.values():
        source = record["source"]
        sequence = record["sequence"]
        pair = (source, sequence)
        if pair in seen:
            raise ValueError("two receipts carry the same source sequence")
        seen.add(pair)
        if sequence > high_water.get(source, 0):
            high_water[source] = sequence
        if record["state"] == "active":
            region = record["signal"]["region"]
            version = record["signal_version"]
            records = history.get(region)
            if records is None or len(records) < version:
                raise ValueError("active receipt references a signal that "
                                 "does not exist")
            stored = records[version - 1]
            if {field: stored[field] for field in _SIGNAL_FIELDS} \
                    != record["signal"]:
                raise ValueError("active receipt does not match its "
                                 "published signal")
            derived = _derived_key(source, record["key_id"], sequence)
            if idempotency.get(derived) != region \
                    or events.get(derived) != {
                        "key": derived, "region": region, "version": version}:
                raise ValueError("active receipt is not bound to its "
                                 "derived publish event")
    return high_water


def _check_publication(
    history: dict[str, list[dict[str, Any]]],
    idempotency: dict[str, str],
    events: dict[str, dict[str, Any]],
    signal: dict[str, Any],
    derived: str,
) -> None:
    # Deterministic publication pre-check performed while the signals
    # lock is held and before the pending receipt is committed, so a
    # conflicting derived idempotency key or a non-increasing
    # observation rejects the request without leaving a stuck pending
    # receipt. The subsequent _publish_locked is then only exposed to
    # I/O failures, which are exactly the crashes a pending receipt can
    # recover from.
    bound_region = idempotency.get(derived)
    if bound_region is not None:
        event = events[derived]
        stored = history[bound_region][event["version"] - 1]
        if {field: stored[field] for field in _SIGNAL_FIELDS} != signal:
            raise ValueError("derived publish idempotency key is already "
                             "bound to a different signal")
        return
    versions = history.get(signal["region"])
    if versions is not None \
            and signal["observed"] <= versions[-1]["observed"]:
        raise ValueError("signal observation must increase across versions")


def _receipt(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": record["source"],
        "key_id": record["key_id"],
        "sequence": record["sequence"],
        "region": record["signal"]["region"],
        "signal_version": record["signal_version"],
        "signature": record["signature"],
        "state": record["state"],
    }


def _derived_key(source: str, key_id: str, sequence: int) -> str:
    # Stable from the authenticated identity alone, with length
    # prefixes making the join unambiguous: a resumed request always
    # re-derives the exact signals idempotency key, so a crash between
    # the pending commit and the publication replays the same publish
    # instead of minting a second signal version.
    return (f"signal-ingest:{len(source)}:{source}"
            f"{len(key_id)}:{key_id}:{sequence}")


# --------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------- #

def ingest(
    signals: str,
    trust: str,
    ledger: str,
    envelope: dict[str, Any],
    key: str,
) -> tuple[dict[str, Any], bool]:
    """Authenticate and ingest one signed live signal envelope.

    ``signals``, ``trust`` and ``ledger`` must be non-empty strings
    resolving to three distinct real locations, and ``key`` a
    non-empty idempotency string; the envelope must carry exactly
    ``source``, ``key_id``, ``sequence`` (a non-boolean positive
    integer), a valid six-field ``signal`` and a 64 lowercase
    hexadecimal HMAC-SHA256 ``signature``. Any argument violation
    raises ``ValueError`` before a file is touched.

    The signature is verified over the compact UTF-8 JSON array
    ``[source, key_id, sequence, signal]`` with the signal fields in
    their fixed order and ``mix`` keys code-point sorted, using the
    trust file's per-source secret. The key must be valid at the
    signal's observation moment and the region authorized. An unknown
    source or key id, or a mismatching signature, raises
    ``PermissionError``; an expired key, an unauthorized region or a
    sequence not strictly greater than the source's latest raises
    ``ValueError``.

    A first request commits a ``pending`` receipt, publishes the
    signal through the existing publication machinery under a stably
    derived idempotency key and then finalizes the receipt as
    ``active``, returning ``(receipt, True)``; a crash resumes from the
    pending receipt without a second signal version. Replaying the
    same key with the identical request returns the original receipt
    with ``False`` and writes nothing; the same key with any changed
    field raises ``ValueError``. The receipt carries source, key_id,
    sequence, region, signal_version, signature and state; no secret is
    persisted.

    A missing trust file or output parent raises ``FileNotFoundError``;
    a non-canonical trust or ledger file or a conflicting publish key
    raises ``ValueError``; other locking or I/O failures raise
    ``OSError``.
    """
    for value in (signals, trust, ledger, key):
        if not isinstance(value, str) or not value:
            raise ValueError("signals, trust, ledger paths and key must be "
                             "non-empty strings")
    normalized = _normalize_envelope(envelope)
    source = normalized["source"]
    key_id = normalized["key_id"]
    sequence = normalized["sequence"]
    signal = normalized["signal"]
    signature = normalized["signature"]

    signals_real = os.path.realpath(signals)
    trust_real = os.path.realpath(trust)
    ledger_real = os.path.realpath(ledger)
    if len({signals_real, trust_real, ledger_real}) != 3:
        raise ValueError("signals, trust and ledger paths must be distinct "
                         "real paths")

    ledger_store = _get_store(ledger_real)
    signal_store = _signals._get_store(signals_real)
    # Both the in-process locks and the cross-process flocks are taken
    # in one resolved-real-path sorted order, so neither same-process
    # threads (even callers with cross-wired ledger/signals locations)
    # nor concurrent processes can deadlock against each other or
    # against a plain signals.publish/get.
    ordered_locks = sorted(((ledger_real, ledger_store.lock),
                            (signals_real, signal_store.lock)))
    with contextlib.ExitStack() as thread_stack, \
            contextlib.ExitStack() as flock_stack:
        for _realpath, thread_lock in ordered_locks:
            thread_stack.enter_context(thread_lock)
        roles = ((ledger_real, False, "ledger"),
                 (signals_real, False, "signals"),
                 (trust_real, True, "trust"))
        for realpath, shared, role in sorted(roles):
            if role == "signals":
                flock_stack.enter_context(
                    _signals._process_lock(realpath, shared=shared))
            else:
                flock_stack.enter_context(_flock(realpath, shared=shared))

        # Re-read the trust configuration on every call, so an
        # atomic same-directory rotation takes effect without a
        # restart; a missing trust file is FileNotFoundError.
        sources, _trust_raw = _load_trust(trust_real)
        entry = sources.get(source)
        if entry is None or entry["key_id"] != key_id:
            raise PermissionError("unknown signal source or key")
        expected = hmac.new(
            bytes.fromhex(entry["secret"]),
            _signing_bytes(source, key_id, sequence, signal),
            hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise PermissionError("signal signature does not match")
        observed = signal["observed"]
        if observed < entry["valid_from"] \
                or observed > entry["valid_until"]:
            raise ValueError("signing key is not valid at the "
                             "signal observation moment")
        region = signal["region"]
        if region not in entry["regions"]:
            raise ValueError("signal region is not authorized for "
                             "this source")

        receipts, old_bytes = _load_ledger(ledger_real)
        history, idempotency, events, _signal_raw = \
            _signals._load_file(signals_real)
        high_water = _check_ledger_integrity(
            receipts, history, idempotency, events)

        existing = receipts.get(key)
        derived = _derived_key(source, key_id, sequence)
        if existing is not None:
            # A replay is the exact same envelope under the same
            # ingest key -- source, key id, sequence, signal and
            # signature, compared together. Any changed field is
            # rejected regardless of the sequences seen since.
            same_request = (
                existing["source"] == source
                and existing["key_id"] == key_id
                and existing["sequence"] == sequence
                and existing["signature"] == signature
                and existing["signal"] == signal)
            if not same_request:
                raise ValueError("ingest key was already used with "
                                 "a different request")
            if existing["state"] == "active":
                # Active replays are pure reads: no publication,
                # no event, not one rewritten byte.
                return _receipt(existing), False
            # A pending receipt resumes the interrupted publish
            # and finalization below; its sequence stays
            # consumed and must not be re-checked.
            record = existing
            _check_publication(history, idempotency, events, signal,
                               derived)
        else:
            if sequence <= high_water.get(source, 0):
                raise ValueError("source sequence must be strictly "
                                 "increasing")
            _check_publication(history, idempotency, events, signal,
                               derived)
            record = {
                "source": source,
                "key_id": key_id,
                "sequence": sequence,
                "signal": signal,
                "signal_version": None,
                "signature": signature,
                "state": "pending",
            }
            receipts[key] = record
            pending_bytes = _serialize_ledger(receipts)
            _commit(ledger_real, pending_bytes, old_bytes)
            old_bytes = pending_bytes

        # Publish through the existing signals machinery while
        # its locks are already held. The derived idempotency
        # key makes the publication idempotent across a crash:
        # the resumed call replays the identical publish
        # (created=False, same version), never a second version;
        # a key already bound to another signal raises
        # ValueError with the pending receipt preserved.
        published, _created = _signals._publish_locked(
            signals_real, signal, _derived_key(
                source, key_id, sequence))

        if record["signal_version"] is None:
            record["signal_version"] = published["version"]
        elif record["signal_version"] != published["version"]:
            raise ValueError("pending receipt does not match its "
                             "published signal version")
        record["state"] = "active"
        final_bytes = _serialize_ledger(receipts)
        # The rollback base is the pending receipt, so a failed
        # finalization leaves a resumable pending state instead
        # of restoring the pre-call ledger.
        _commit(ledger_real, final_bytes, old_bytes)
        return _receipt(record), True
