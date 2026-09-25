"""Acceptance ledger for verified checkpoint/proof bundles.

The trust directory of :func:`carbon_market.audit_proof.verify_bundle`
already keeps the full verified chain of accepted checkpoint snapshots,
but nothing records *which* bundles this machine was offered and what
became of them. This module adds that acceptance ledger: every
submission is first verified by :func:`audit_proof.verify_bundle` --
the complete existing verification, trust-directory sequence included
-- and the outcome is then recorded under a caller-chosen idempotency
key.

:func:`submit` takes the two downloaded bundle files, the strong entity
tag the checkpoint download carried, the trust directory, the ledger
directory and the idempotency key. Each ledger record carries, in that
order, ``key``, ``etag``, ``checkpoint_digest``, ``proof_digest``,
``state`` and ``error``: the key it was submitted under, the actual
strong tag recomputed from the checkpoint's raw bytes -- never the
caller-claimed value, which is only a verification input -- the
SHA-256 of the checkpoint's raw bytes, the SHA-256 of the proof's raw
bytes, the acceptance state -- ``pending``, ``active`` or
``quarantined`` -- and, only while quarantined, the class name of the
public exception the verification raised (``error`` is null in every
other state).

A first submission commits the record as ``pending`` and only then
stores the checkpoint's and the proof's raw bytes content-addressed
under the ledger directory, both *before* the verification runs, so a
crash in any later window -- the evidence write included -- leaves
exactly one explainable stage: a restarted process resumes the same
bundle from the stored ``pending`` record, re-storing any missing
evidence and finishing the sequence, never mistaking the submission
for one that was never received. Leftover unreferenced fragments never
take part in any decision. A successful verification flips the record
to ``active`` in the same call. A format failure keeps the evidence,
quarantines the record and re-raises ``BundleFormatError``; a tag,
digest-chain or lifecycle contradiction -- a claimed tag that does not
match the recomputed one included -- keeps the evidence, quarantines
the record and re-raises ``BundleMismatchError``. Missing inputs,
missing parents and other locking or I/O failures propagate without
moving the record: a failed call always keeps the pre-call state.

Submissions are idempotent. The same bundle under the same key -- the
recomputed tag and both digests all equal -- replays: an ``active`` record returns
its stored copy together with ``False`` and writes nothing (only the
first successful completion returns ``True``), a ``pending`` record
resumes and completes, and a ``quarantined`` record re-raises the
recorded public exception type without touching the retained evidence.
The same key with a different bundle is a conflict: both bundles' raw
bytes and summaries are retained -- the new bundle's evidence is stored
content-addressed and its summary, its tag again recomputed from its
checkpoint's raw bytes, appended to the ledger's ``conflicts`` -- the
main record is never overwritten, and ``ValueError`` is raised.
An already recorded identical conflict advances nothing further.

The ledger itself is one ``ledger.json`` inside the ledger directory: a
compact UTF-8 JSON document ``{"version": 1, "records": {...},
"conflicts": [...]}`` with the root fields in that order, the records
ordered by key code point, terminated by exactly one newline. Every
commit writes a same-directory temporary file, fsyncs it, atomically
replaces the ledger and fsyncs the directory, restoring the pre-call
bytes if the commit fails; concurrent submissions across processes
serialize on the ledger directory's exclusive kernel flock, so the same
request advances the state exactly once.

:func:`get` returns a copy of the record stored under a key; an unknown
key raises ``KeyError(key)``. :func:`search` is the read-only paginated
query: it scans the records in ascending key code-point order, keeps
only the records at keys strictly greater than an optional exclusive
cursor and matching an optional state filter, and returns one page of
``[key, record]`` pairs as ``entries`` together with the ``next`` cursor
(or ``None`` on the last page). The page size defaults to 100 and is
limited to 1..1000.

Invalid argument types, empty values, a malformed tag, an unknown state
filter, an out-of-range page size or an invalid ledger raise
``ValueError``; a missing input file, ledger or parent directory raises
``FileNotFoundError``; every other locking or I/O failure raises
``OSError``. Only the standard library is used.
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any

from . import audit_proof
from ._jsonio import strict_loads

__all__ = ["submit", "get", "search"]

_VERSION = 1
_ROOT_FIELDS = ("version", "records", "conflicts")
_RECORD_FIELDS = ("key", "etag", "checkpoint_digest", "proof_digest",
                  "state", "error")
_CONFLICT_FIELDS = ("key", "etag", "checkpoint_digest", "proof_digest")
_STATES = ("pending", "active", "quarantined")
# The public exception types a verification failure quarantines with;
# the record's error field carries exactly one of these class names.
_QUARANTINE_ERRORS = ("BundleFormatError", "BundleMismatchError")
_DEFAULT_LIMIT = 100
_MAX_LIMIT = 1000
_LEDGER_NAME = "ledger.json"


def _ledger_path(ledger_real: str) -> str:
    return os.path.join(ledger_real, _LEDGER_NAME)


def _validate_record(raw: object, map_key: str) -> dict[str, Any]:
    # The field set and order are part of the contract; a record whose
    # key, tag, digests, state or error relation deviates is an invalid
    # ledger, never repaired or reordered.
    if not isinstance(raw, dict) or list(raw.keys()) != list(_RECORD_FIELDS):
        raise ValueError("ledger records must be objects with keys key, "
                         "etag, checkpoint_digest, proof_digest, state "
                         "and error, in that order")
    key = raw["key"]
    if not isinstance(key, str) or not key:
        raise ValueError("ledger record key must be a non-empty string")
    if key != map_key:
        raise ValueError("ledger record key does not match its records "
                         "key")
    etag = raw["etag"]
    if not isinstance(etag, str) or not audit_proof._ETAG_RE.fullmatch(etag):
        raise ValueError("ledger record etag must be a single strong tag: "
                         "a quoted 64-digit lowercase hexadecimal digest")
    if not audit_proof._is_digest(raw["checkpoint_digest"]):
        raise ValueError("ledger record checkpoint_digest must be a "
                         "64-digit lowercase hexadecimal digest")
    if not audit_proof._is_digest(raw["proof_digest"]):
        raise ValueError("ledger record proof_digest must be a 64-digit "
                         "lowercase hexadecimal digest")
    state = raw["state"]
    if state not in _STATES:
        raise ValueError("ledger record state must be pending, active or "
                         "quarantined")
    error = raw["error"]
    if state == "quarantined":
        if error not in _QUARANTINE_ERRORS:
            raise ValueError("a quarantined ledger record must name the "
                             "public exception type in error")
    elif error is not None:
        raise ValueError("ledger record error must be null unless "
                         "quarantined")
    return {field: raw[field] for field in _RECORD_FIELDS}


def _validate_conflict(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict) \
            or list(raw.keys()) != list(_CONFLICT_FIELDS):
        raise ValueError("ledger conflicts must be objects with keys key, "
                         "etag, checkpoint_digest and proof_digest, in "
                         "that order")
    if not isinstance(raw["key"], str) or not raw["key"]:
        raise ValueError("ledger conflict key must be a non-empty string")
    etag = raw["etag"]
    if not isinstance(etag, str) or not audit_proof._ETAG_RE.fullmatch(etag):
        raise ValueError("ledger conflict etag must be a single strong "
                         "tag: a quoted 64-digit lowercase hexadecimal "
                         "digest")
    if not audit_proof._is_digest(raw["checkpoint_digest"]):
        raise ValueError("ledger conflict checkpoint_digest must be a "
                         "64-digit lowercase hexadecimal digest")
    if not audit_proof._is_digest(raw["proof_digest"]):
        raise ValueError("ledger conflict proof_digest must be a 64-digit "
                         "lowercase hexadecimal digest")
    return {field: raw[field] for field in _CONFLICT_FIELDS}


def _validate_ledger(data: object) -> dict[str, Any]:
    if not isinstance(data, dict) or list(data.keys()) != list(_ROOT_FIELDS):
        raise ValueError("ledger root must be an object with keys version, "
                         "records and conflicts, in that order")
    version = data["version"]
    if not isinstance(version, int) or isinstance(version, bool) \
            or version != _VERSION:
        raise ValueError("unsupported ledger version")
    records_raw = data["records"]
    if not isinstance(records_raw, dict):
        raise ValueError("ledger records must be an object")
    record_keys = list(records_raw)
    if record_keys != sorted(record_keys):
        raise ValueError("ledger records must be ordered by key code point")
    records: dict[str, Any] = {}
    for record_key in record_keys:
        if not isinstance(record_key, str) or not record_key:
            raise ValueError("ledger record keys must be non-empty strings")
        records[record_key] = _validate_record(records_raw[record_key],
                                               record_key)
    conflicts_raw = data["conflicts"]
    if not isinstance(conflicts_raw, list):
        raise ValueError("ledger conflicts must be a list")
    conflicts = [_validate_conflict(item) for item in conflicts_raw]
    return {"records": records, "conflicts": conflicts}


def _parse_ledger(realpath: str, raw: bytes) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"ledger file {realpath!r} is not valid UTF-8") from exc
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"ledger file {realpath!r} is not valid JSON") from exc
    return _validate_ledger(data)


def _read_ledger(ledger_path: str) -> tuple[dict[str, Any] | None,
                                            bytes | None]:
    # A missing ledger simply means nothing was ever submitted; the raw
    # bytes are returned alongside so a later rollback can restore the
    # exact pre-call content.
    try:
        with open(ledger_path, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return None, None
    return _parse_ledger(ledger_path, raw), raw


def _serialize(records: dict[str, Any], conflicts: list[Any]) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, records ordered by
    # key code point, terminated by exactly one newline.
    ordered = {record_key: records[record_key]
               for record_key in sorted(records)}
    document = {"version": _VERSION, "records": ordered,
                "conflicts": conflicts}
    return (json.dumps(document, ensure_ascii=False, separators=(",", ":"),
                       allow_nan=False) + "\n").encode("utf-8")


def _save_evidence(ledger_real: str, checkpoint_digest: str,
                   checkpoint_raw: bytes, proof_digest: str,
                   proof_raw: bytes) -> None:
    # Content-addressed evidence: the file name commits to the bytes, an
    # identical existing file is reused as-is, and an address holding
    # different bytes is never overwritten or repaired (the trust
    # directory's _store_evidence raises BundleMismatchError there).
    audit_proof._ensure_dir(os.path.join(ledger_real, "checkpoints"))
    audit_proof._ensure_dir(os.path.join(ledger_real, "proofs"))
    audit_proof._store_evidence(
        os.path.join(ledger_real, "checkpoints",
                     checkpoint_digest + ".json"), checkpoint_raw)
    audit_proof._store_evidence(
        os.path.join(ledger_real, "proofs", proof_digest + ".json"),
        proof_raw)


def _raise_quarantined(record: dict[str, Any]) -> None:
    # A quarantined record replays its first public failure without
    # re-verifying and without touching the retained evidence.
    exc_type = getattr(audit_proof, record["error"])
    raise exc_type(f"bundle {record['etag']} is quarantined under key "
                   f"{record['key']!r}")


def submit(checkpoint_path: str, proof_path: str, etag: str,
           trust_dir: str | None = None, ledger_dir: str | None = None,
           key: str | None = None) -> tuple[dict[str, Any], bool]:
    """Verify a checkpoint/proof bundle and record its acceptance.

    ``checkpoint_path``, ``proof_path``, ``ledger_dir`` and ``key`` must
    be non-empty strings; ``etag`` must be a single strong tag -- one
    double-quoted string of 64 lowercase hexadecimal digits, exactly as
    the checkpoint download's ``ETag`` header carried it; ``trust_dir``
    must be ``None`` or a non-empty string. Argument errors raise
    ``ValueError`` before any file is read.

    The bundle's raw bytes are read (a missing input raises
    ``FileNotFoundError``) and, under the ledger directory's exclusive
    kernel flock, the submission is matched against the ledger. A new
    key commits a ``pending`` record and then stores both files' bytes
    content-addressed under the ledger directory; the bundle is then
    verified by :func:`carbon_market.audit_proof.verify_bundle` -- the
    complete existing verification, trust directory included. Success
    flips the record to ``active`` and returns ``(record, True)``. A
    format failure quarantines the record and re-raises
    ``BundleFormatError``; a tag, digest-chain or lifecycle mismatch
    quarantines it and re-raises ``BundleMismatchError`` -- in both
    cases the evidence stays retained. Any other failure (a missing
    parent, an evidence write, locking or I/O) propagates with the
    record left in its pre-call state, so a restarted process resumes
    from ``pending``.

    The same bundle under an existing key replays: an ``active`` record
    returns its copy with ``False`` and writes nothing, a ``pending``
    record resumes and completes (returning ``True`` only when this call
    is the first successful completion), and a ``quarantined`` record
    re-raises the recorded public exception type without changing the
    retained evidence. The same key with a different bundle retains both
    bundles' raw bytes and summaries, appends the new summary to the
    ledger's ``conflicts`` without overwriting the main record and
    raises ``ValueError``; an already recorded identical conflict
    advances nothing further.

    A missing ledger-directory parent raises ``FileNotFoundError``; an
    invalid existing ledger raises ``ValueError``; every other locking
    or I/O failure raises ``OSError``. The returned record is a fresh
    copy with fields key, etag, checkpoint_digest, proof_digest, state
    and error in that order; the etag is always the actual strong tag
    recomputed from the checkpoint's raw bytes, never the claimed
    value the call carried.
    """
    for value in (checkpoint_path, proof_path):
        if not isinstance(value, str) or not value:
            raise ValueError("checkpoint_path and proof_path must be "
                             "non-empty strings")
    if not isinstance(etag, str) or not audit_proof._ETAG_RE.fullmatch(etag):
        raise ValueError("etag must be a single strong tag: a quoted "
                         "64-digit lowercase hexadecimal digest")
    if trust_dir is not None \
            and (not isinstance(trust_dir, str) or not trust_dir):
        raise ValueError("trust_dir must be None or a non-empty string")
    if not isinstance(ledger_dir, str) or not ledger_dir:
        raise ValueError("ledger_dir must be a non-empty string")
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a non-empty string")

    # The submitted bytes are the evidence; read them before the ledger
    # is touched so a missing input never creates anything.
    with open(checkpoint_path, "rb") as handle:
        checkpoint_raw = handle.read()
    with open(proof_path, "rb") as handle:
        proof_raw = handle.read()
    checkpoint_digest = hashlib.sha256(checkpoint_raw).hexdigest()
    proof_digest = hashlib.sha256(proof_raw).hexdigest()
    # The ledger only ever records the actual strong tag recomputed
    # from the checkpoint's raw bytes; the caller-claimed tag is a
    # verification input and never a stored value.
    actual_tag = audit_proof._strong_tag(checkpoint_raw)

    ledger_real = os.path.realpath(ledger_dir)
    store = audit_proof._get_store(ledger_real)
    with store.lock:
        with audit_proof._file_lock(ledger_real):
            # A not-yet-existing ledger directory with a usable parent
            # is the first use; a missing parent surfaces as
            # FileNotFoundError, an existing non-directory as OSError.
            if not os.path.isdir(ledger_real):
                os.mkdir(ledger_real)
                audit_proof._fsync_dir(os.path.dirname(ledger_real) or ".")
            ledger_path = _ledger_path(ledger_real)
            ledger, old_bytes = _read_ledger(ledger_path)
            records: dict[str, Any] = (
                {} if ledger is None else ledger["records"])
            conflicts: list[Any] = (
                [] if ledger is None else ledger["conflicts"])

            existing = records.get(key)
            if existing is not None:
                same_bundle = (
                    existing["etag"] == actual_tag
                    and existing["checkpoint_digest"] == checkpoint_digest
                    and existing["proof_digest"] == proof_digest)
                if not same_bundle:
                    # Same key, different bundle: retain the new
                    # bundle's bytes and summary alongside the record's,
                    # never overwrite the main record. An already
                    # recorded identical conflict advances nothing.
                    conflict = {"key": key, "etag": actual_tag,
                                "checkpoint_digest": checkpoint_digest,
                                "proof_digest": proof_digest}
                    if conflict not in conflicts:
                        _save_evidence(ledger_real, checkpoint_digest,
                                       checkpoint_raw, proof_digest,
                                       proof_raw)
                        conflicts.append(conflict)
                        audit_proof._commit_checkpoint(
                            ledger_path, ledger_real,
                            _serialize(records, conflicts), old_bytes)
                    raise ValueError("acceptance key was already used "
                                     "with a different bundle")
                if existing["state"] == "active":
                    return dict(existing), False
                if existing["state"] == "quarantined":
                    _raise_quarantined(existing)
                # A pending record is resumed below: the identical
                # bundle finishes its interrupted acceptance. The
                # evidence is re-stored first -- a crash between the
                # pending commit and the evidence write may have left
                # it incomplete.
                record = existing
                _save_evidence(ledger_real, checkpoint_digest,
                               checkpoint_raw, proof_digest, proof_raw)
            else:
                # First submission: commit the pending record before the
                # evidence is stored and the verification runs, so a
                # crash or an evidence-write failure in any later window
                # leaves exactly this one explainable stage and a retry
                # of the same bundle resumes from the stored record
                # instead of pretending the submission never arrived.
                record = {"key": key, "etag": actual_tag,
                          "checkpoint_digest": checkpoint_digest,
                          "proof_digest": proof_digest,
                          "state": "pending", "error": None}
                records[key] = record
                old_bytes = _commit_pending(
                    ledger_path, ledger_real, records, conflicts,
                    old_bytes)
                _save_evidence(ledger_real, checkpoint_digest,
                               checkpoint_raw, proof_digest, proof_raw)

            # The complete existing verification runs first; only its
            # outcome decides the acceptance state.
            try:
                audit_proof.verify_bundle(
                    checkpoint_path, proof_path, etag,
                    trust_dir=trust_dir)
            except (audit_proof.BundleFormatError,
                    audit_proof.BundleMismatchError) as exc:
                # Keep the evidence, quarantine the record with the
                # public exception's class name, then re-raise.
                record["state"] = "quarantined"
                record["error"] = type(exc).__name__
                audit_proof._commit_checkpoint(
                    ledger_path, ledger_real,
                    _serialize(records, conflicts), old_bytes)
                raise
            record["state"] = "active"
            audit_proof._commit_checkpoint(
                ledger_path, ledger_real, _serialize(records, conflicts),
                old_bytes)
            return dict(record), True


def _commit_pending(ledger_path: str, ledger_real: str,
                    records: dict[str, Any], conflicts: list[Any],
                    old_bytes: bytes | None) -> bytes:
    # Commit the freshly created pending record and return the committed
    # bytes, which the follow-up commit (active or quarantined) rolls
    # back to if it fails.
    payload = _serialize(records, conflicts)
    audit_proof._commit_checkpoint(ledger_path, ledger_real, payload,
                                   old_bytes)
    return payload


def _read_existing_ledger(ledger_real: str) -> dict[str, Any]:
    # Read-only path shared by get and search: the shared flock is held
    # only while the ledger is opened and read, so a query racing a
    # submit observes either the complete previous ledger or the
    # complete new one. A missing ledger surfaces as FileNotFoundError;
    # malformed UTF-8/JSON (negative-zero literals included) or any
    # structural deviation raises ValueError.
    ledger_path = _ledger_path(ledger_real)
    with audit_proof._file_lock(ledger_real, shared=True):
        with open(ledger_path, "rb") as handle:
            raw = handle.read()
    return _parse_ledger(ledger_path, raw)


def get(ledger_dir: str, key: str) -> dict[str, Any]:
    """Return a read-only copy of the acceptance record under ``key``.

    ``ledger_dir`` and ``key`` must be non-empty strings, else
    ``ValueError``. A missing ledger directory or ledger file raises
    ``FileNotFoundError``; an invalid ledger raises ``ValueError``; an
    unknown key raises ``KeyError(key)``; any other locking or I/O
    failure raises ``OSError``. The query never writes. The returned
    dict is a fresh copy with fields key, etag, checkpoint_digest,
    proof_digest, state and error in that order; mutating it never
    affects the ledger.
    """
    for value in (ledger_dir, key):
        if not isinstance(value, str) or not value:
            raise ValueError("ledger_dir and key must be non-empty "
                             "strings")

    ledger_real = os.path.realpath(ledger_dir)
    ledger = _read_existing_ledger(ledger_real)

    records = ledger["records"]
    if key not in records:
        raise KeyError(key)
    return dict(records[key])


def search(ledger_dir: str, cursor: str | None = None,
           limit: int = _DEFAULT_LIMIT,
           state: str | None = None) -> dict[str, Any]:
    """Return one page of acceptance records matching the given filters.

    ``ledger_dir`` must be a non-empty string. ``cursor`` is ``None`` or
    a non-empty string -- it need not name an existing record -- and
    only records whose key is strictly greater than it in code-point
    order are considered. ``limit`` is the page size: an integer from 1
    to 1000 (booleans are rejected), defaulting to 100. ``state`` is
    ``None`` or one of ``"pending"``, ``"active"`` and ``"quarantined"``;
    cursor and state filter compose. Any other value raises
    ``ValueError``.

    The records are scanned in ascending key code-point order and
    filtered afterwards, so non-matching records never consume page
    capacity. Returns ``{"entries": [[key, record], ...], "next":
    cursor_or_none}``: each record is a fresh copy with the public field
    order, and ``next`` is the key of the page's last item when further
    matching records remain, else ``None``. With no matching records the
    page is empty and ``next`` is ``None``.

    The query is strictly read-only: a missing ledger directory or
    ledger file raises ``FileNotFoundError``; an invalid ledger raises
    ``ValueError``; any other locking or I/O failure raises ``OSError``.
    """
    if not isinstance(ledger_dir, str) or not ledger_dir:
        raise ValueError("ledger_dir must be a non-empty string")
    if cursor is not None and (not isinstance(cursor, str) or not cursor):
        raise ValueError("cursor must be None or a non-empty string")
    # bool is a subclass of int and must be rejected as a page size.
    if not isinstance(limit, int) or isinstance(limit, bool) \
            or not 1 <= limit <= _MAX_LIMIT:
        raise ValueError("limit must be an integer between 1 and 1000")
    if state is not None and state not in _STATES:
        raise ValueError("state filter must be pending, active or "
                         "quarantined")

    ledger_real = os.path.realpath(ledger_dir)
    ledger = _read_existing_ledger(ledger_real)

    # The persisted records are validated to be in ascending key
    # code-point order, so the document order is the scan order. One
    # extra match is collected to learn whether the page is the last.
    matches: list[list[Any]] = []
    for record_key, record in ledger["records"].items():
        if cursor is not None and record_key <= cursor:
            continue
        if state is not None and record["state"] != state:
            continue
        matches.append([record_key, dict(record)])
        if len(matches) > limit:
            break

    if len(matches) > limit:
        page = matches[:limit]
        next_cursor: str | None = page[-1][0]
    else:
        page = matches
        next_cursor = None
    return {"entries": page, "next": next_cursor}
