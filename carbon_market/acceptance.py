"""Acceptance ledger for offline checkpoint/proof bundle submissions.

The trust directory keeps the sequence of snapshots a machine has been
willing to verify, but it keeps no per-submission ledger: this module
adds the missing acceptance bookkeeping on top of
:func:`carbon_market.audit_proof.verify_bundle`. Every submission is
addressed by a caller-chosen idempotency key and runs the *existing*
bundle verification first; the ledger only records which state the
submission ended in.

A ledger root is compact UTF-8 JSON, version 1, with ``version``,
``records`` and ``conflicts`` in that order and exactly one trailing
newline::

    {"version":1,"records":{...},"conflicts":[...]}

``records`` maps each idempotency key to its record, ordered by key code
point. A record carries ``key``, ``etag``, ``checkpoint_digest``,
``proof_digest``, ``state`` and ``error`` in that order. ``state`` is one
of:

* ``"pending"`` -- the first submission landed its record before the
  bundle had finished verifying, so an interrupted process recovers from
  it;
* ``"active"`` -- :func:`verify_bundle` accepted the bundle;
* ``"quarantined"`` -- verification failed with a public bundle error.

``error`` is null except on a quarantined record, where it names the
exception class -- ``BundleFormatError`` or ``BundleMismatchError`` --
the first public failure raised. A format failure (encoding, JSON or
structure, including a version 1 proof) keeps the submitted evidence and
quarantines before :class:`~carbon_market.audit_proof.BundleFormatError`
is raised; a tag, chain or lifecycle contradiction does the same with
:class:`~carbon_market.audit_proof.BundleMismatchError`.

The checkpoint and proof raw bytes are kept content-addressed beside
the ledger (``checkpoints/<digest>.bin`` and ``proofs/<digest>.bin``);
verification runs against those retained copies, so recovery after a
crash never needs the original files. A first submission commits the
pending record first and only then stores evidence and verifies; a
restart resumes the pending record with the identical package and
reports success once. An active record is re-verified against the
retained evidence, replays the original record together with ``False``
and returns ``(record, False)`` -- only the first successful acceptance
returns ``(record, True)``, the same ``(record, created)`` shape
:func:`carbon_market.audit.record` uses. A quarantined record replays
without touching the retained evidence and re-raises the first public
exception type. The same key with a *different* package never overwrites
the main record: both packages' raw bytes and digests are kept (the
newcomer content-addressed on its own digests), one conflict is appended
to ``conflicts`` -- key, the existing digest pair, the incoming digest
pair and the main record's state -- and a plain ``ValueError`` is
raised.

Submissions serialize on the ledger directory's exclusive kernel flock
(the read-only :func:`get` and :func:`search` take the shared one), so
identical concurrent requests across processes allow exactly one state
transition. Every ledger commit writes a same-directory temporary file,
fsyncs it, atomically replaces the ledger and fsyncs the directory; a
failed commit restores the pre-call bytes. Crashes leave either the old
ledger or the complete new one; an interrupted preparation is resumed
from its pending record and leftover temporary fragments are ignored.

:func:`get` returns a fresh copy of one key's record (an unknown key
raises ``KeyError(key)``); :func:`search` pages through records in key
code-point order with an optional exclusive cursor and a state filter,
returning ``{"entries": [[key, record], ...], "next": cursor}`` with a
page size from 1 to 1000, default 100.

Illegal argument types, empty values, a malformed label, state or page
size, or an illegal ledger all raise ``ValueError`` before/instead of
serving data. A missing ledger input or a missing parent directory raises
``FileNotFoundError``; every other locking or I/O failure raises
``OSError``. The standard library only.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import tempfile
import threading
from typing import Any, Iterator

from . import audit, audit_proof
from ._jsonio import finite_loads

__all__ = ["submit", "get", "search"]

_LEDGER_VERSION = 1
_ROOT_FIELDS = ("version", "records", "conflicts")
_RECORD_FIELDS = ("key", "etag", "checkpoint_digest", "proof_digest",
                  "state", "error")
_CONFLICT_FIELDS = ("key", "existing", "incoming", "state")
_STATES = ("pending", "active", "quarantined")
_STATE_PENDING = "pending"
_STATE_ACTIVE = "active"
_STATE_QUARANTINED = "quarantined"
_DEFAULT_LIMIT = 100
_MAX_LIMIT = 1000
_HEXADECIMAL = frozenset("0123456789abcdef")


class _Store:
    def __init__(self, realpath: str) -> None:
        self.realpath = realpath
        self.lock = threading.Lock()


_stores_lock = threading.Lock()
_stores: dict[str, _Store] = {}


def _get_store(realpath: str) -> _Store:
    with _stores_lock:
        store = _stores.get(realpath)
        if store is None:
            store = _Store(realpath)
            _stores[realpath] = store
        return store


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 \
        and all(char in _HEXADECIMAL for char in value)


def _fsync_dir(directory: str) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _ensure_dir(path: str) -> None:
    try:
        os.mkdir(path)
    except FileExistsError:
        return
    _fsync_dir(os.path.dirname(path) or ".")


@contextlib.contextmanager
def _acceptance_lock(ledger_dir: str, *, shared: bool = False
                     ) -> Iterator[None]:
    # The acceptance directory's kernel flock lives on a sibling lock
    # file (like the trust directory's), so it can be taken before the
    # directory itself exists: a missing parent surfaces as
    # FileNotFoundError from the lock-file creation.
    lock_path = ledger_dir + ".lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _compact(payload: Any) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n").encode("utf-8")


def _serialize(document: dict[str, Any]) -> bytes:
    # Records are persisted in key code-point order like the journal
    # events; conflicts keep their append order for the audit trail.
    records = {key: document["records"][key]
               for key in sorted(document["records"])}
    root = {"version": _LEDGER_VERSION, "records": records,
            "conflicts": document["conflicts"]}
    return _compact(root)


def _commit(realpath: str, directory: str, payload: bytes,
            old_bytes: bytes | None) -> None:
    # Same-directory temporary, fsync, atomic replace, directory fsync --
    # rolling back to the pre-call bytes if the directory sync fails, so
    # a failed commit never leaves a partially advanced ledger.
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".acceptance-", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, realpath)
    except BaseException as first:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise
    try:
        _fsync_dir(directory)
    except BaseException as first:
        audit._rollback(realpath, directory, old_bytes, first)
        raise


def _validate_record(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict) or list(raw.keys()) != list(_RECORD_FIELDS):
        raise ValueError("acceptance record must be an object with keys "
                         "key, etag, checkpoint_digest, proof_digest, "
                         "state and error, in that order")
    key = raw["key"]
    if not isinstance(key, str) or not key:
        raise ValueError("acceptance record key must be a non-empty "
                         "string")
    if not isinstance(raw["etag"], str) \
            or not audit_proof._ETAG_RE.fullmatch(raw["etag"]):
        raise ValueError("acceptance record etag must be a single strong "
                         "tag: a quoted 64-digit lowercase hexadecimal "
                         "digest")
    if not _is_digest(raw["checkpoint_digest"]) \
            or not _is_digest(raw["proof_digest"]):
        raise ValueError("acceptance record digests must be 64-digit "
                         "lowercase hexadecimal digests")
    if raw["etag"] != '"' + raw["checkpoint_digest"] + '"':
        # The record tag is the checkpoint content address in strong-tag
        # form, the same self-binding convention trust nodes use.
        raise ValueError("acceptance record etag does not bind its "
                         "checkpoint digest")
    state = raw["state"]
    if state not in _STATES:
        raise ValueError("acceptance state must be pending, active or "
                         "quarantined")
    error = raw["error"]
    if state == _STATE_QUARANTINED:
        if error not in ("BundleFormatError", "BundleMismatchError"):
            # Only the two public bundle error types can have caused a
            # quarantine; anything else is a malformed ledger.
            raise ValueError("a quarantined acceptance record must name "
                             "BundleFormatError or BundleMismatchError")
    elif error is not None:
        raise ValueError("only a quarantined acceptance record may carry "
                         "an error")
    return {"key": key, "etag": raw["etag"],
            "checkpoint_digest": raw["checkpoint_digest"],
            "proof_digest": raw["proof_digest"], "state": state,
            "error": error}


def _validate_conflict(raw: object, records: dict[str, Any]) -> None:
    if not isinstance(raw, dict) or list(raw.keys()) != list(_CONFLICT_FIELDS):
        raise ValueError("acceptance conflict must be an object with "
                         "keys key, existing, incoming and state, in "
                         "that order")
    key = raw["key"]
    if not isinstance(key, str) or not key or key not in records:
        raise ValueError("acceptance conflict must reference a recorded "
                         "key")
    for field in ("existing", "incoming"):
        pair = raw[field]
        if not isinstance(pair, list) or len(pair) != 2 \
                or not _is_digest(pair[0]) or not _is_digest(pair[1]):
            raise ValueError("acceptance conflict digests must be "
                             "[checkpoint digest, proof digest] pairs")
    if raw["existing"] != [records[key]["checkpoint_digest"],
                           records[key]["proof_digest"]]:
        raise ValueError("acceptance conflict must bind the recorded "
                         "package digests")
    if raw["existing"] == raw["incoming"]:
        raise ValueError("an acceptance conflict needs two different "
                         "packages")
    if raw["state"] not in _STATES:
        raise ValueError("acceptance conflict state must be pending, "
                         "active or quarantined")


def _parse_ledger(raw: bytes) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("acceptance ledger is not valid UTF-8") from exc
    try:
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError("acceptance ledger is not valid JSON") from exc
    if not isinstance(data, dict) or list(data.keys()) != list(_ROOT_FIELDS):
        raise ValueError("acceptance ledger root must be an object with "
                         "keys version, records and conflicts, in that "
                         "order")
    version = data["version"]
    if not isinstance(version, int) or isinstance(version, bool) \
            or version != _LEDGER_VERSION:
        raise ValueError("unsupported acceptance ledger version")
    records_raw = data["records"]
    if not isinstance(records_raw, dict):
        raise ValueError("acceptance ledger records must be an object")
    keys = list(records_raw)
    if keys != sorted(keys):
        raise ValueError("acceptance ledger records must be ordered by "
                         "key code point")
    records: dict[str, Any] = {}
    for key in keys:
        record = _validate_record(records_raw[key])
        if record["key"] != key:
            raise ValueError("acceptance record must carry its own key")
        records[key] = record
    conflicts_raw = data["conflicts"]
    if not isinstance(conflicts_raw, list):
        raise ValueError("acceptance ledger conflicts must be a list")
    conflicts: list[dict[str, Any]] = []
    for raw_conflict in conflicts_raw:
        _validate_conflict(raw_conflict, records)
        conflicts.append({
            "key": raw_conflict["key"],
            "existing": list(raw_conflict["existing"]),
            "incoming": list(raw_conflict["incoming"]),
            "state": raw_conflict["state"]})
    return {"records": records, "conflicts": conflicts}


def _read_ledger(realpath: str) -> tuple[dict[str, Any] | None, bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return None, None
    return _parse_ledger(raw), raw


def _copy_record(record: dict[str, Any]) -> dict[str, Any]:
    return {field: record[field] for field in _RECORD_FIELDS}


def _store_evidence(realpath: str, payload: bytes) -> None:
    # Content-addressed raw bytes: the file name commits to the content.
    # Exact bytes already on disk are a crashed or replayed attempt and
    # are reused untouched; different bytes under the same digest cannot
    # be repaired from the package in hand.
    try:
        with open(realpath, "rb") as handle:
            existing = handle.read()
    except FileNotFoundError:
        existing = None
    if existing is not None:
        if existing == payload:
            return
        raise OSError("an acceptance content address already holds "
                      "different bytes")
    _ensure_dir(os.path.dirname(realpath))
    _commit(realpath, os.path.dirname(realpath), payload, None)


def _evidence_paths(directory: str, checkpoint_digest: str,
                    proof_digest: str) -> tuple[str, str]:
    return (os.path.join(directory, "checkpoints",
                         checkpoint_digest + ".bin"),
            os.path.join(directory, "proofs", proof_digest + ".bin"))


def _stored_failure(error_name: str) -> ValueError:
    # Re-raise the first public exception type a quarantined record
    # retained, without re-running the bundle.
    if error_name == "BundleFormatError":
        return audit_proof.BundleFormatError(
            "bundle was quarantined on its first submission")
    return audit_proof.BundleMismatchError(
        "bundle was quarantined on its first submission")


def _quarantine(record: dict[str, Any],
                exc: ValueError) -> None:
    record["state"] = _STATE_QUARANTINED
    record["error"] = type(exc).__name__


def _verify_retained(checkpoint_path: str, proof_path: str, etag: str,
                     trust_dir: str | None) -> dict[str, Any]:
    return audit_proof.verify_bundle(
        checkpoint_path, proof_path, etag, trust_dir=trust_dir)


def _conflict(document: dict[str, Any], key: str,
              existing_pair: list[str], incoming_pair: list[str],
              state: str) -> None:
    entry = {"key": key, "existing": list(existing_pair),
             "incoming": list(incoming_pair), "state": state}
    # The same foreign package resubmitted in the same state logs once;
    # a different package, or the same one seen after the record moved,
    # is a distinct entry.
    if entry not in document["conflicts"]:
        document["conflicts"].append(entry)


def _replay(document: dict[str, Any], key: str,
            checkpoint_digest: str, proof_digest: str,
            checkpoint: bytes, proof: bytes,
            trust_dir: str | None, ledger_real: str,
            ledger_dir: str, ledger_old: bytes | None
            ) -> tuple[dict[str, Any], bool]:
    record = document["records"][key]
    existing_pair = [record["checkpoint_digest"], record["proof_digest"]]
    incoming_pair = [checkpoint_digest, proof_digest]

    if incoming_pair != existing_pair:
        # Same key, different package: the main record is never
        # overwritten in any state. Both packages are retained on their
        # own content addresses, one conflict is appended and a plain
        # ValueError surfaces -- the existing evidence is never altered.
        incoming_cp, incoming_pf = _evidence_paths(
            ledger_dir, checkpoint_digest, proof_digest)
        _store_evidence(incoming_cp, checkpoint)
        _store_evidence(incoming_pf, proof)
        _conflict(document, key, existing_pair, incoming_pair,
                  record["state"])
        _commit(ledger_real, ledger_dir, _serialize(document),
                ledger_old)
        raise ValueError("acceptance key was already used with a "
                         "different bundle")

    # Identical package: dispatch on the recorded state.
    cp_path, pf_path = _evidence_paths(
        ledger_dir, record["checkpoint_digest"], record["proof_digest"])
    if record["state"] == _STATE_QUARANTINED:
        # A quarantined replay changes no evidence and re-raises the
        # first public exception type that was recorded.
        raise _stored_failure(record["error"])
    if record["state"] == _STATE_ACTIVE:
        # Idempotent active replay: run the existing verification in full
        # against the retained evidence, replay the original record and
        # report False -- only the first acceptance reports True.
        _verify_retained(cp_path, pf_path, record["etag"], trust_dir)
        return _copy_record(record), False

    # An interrupted first submission: finish the pending record with
    # the identical package. The pending record and its digests were
    # committed first, so the evidence only needs (re)staging.
    _store_evidence(cp_path, checkpoint)
    _store_evidence(pf_path, proof)
    old_bytes = _serialize(document)
    try:
        result = _verify_retained(cp_path, pf_path, record["etag"],
                                  trust_dir)
    except (audit_proof.BundleFormatError,
            audit_proof.BundleMismatchError) as exc:
        _quarantine(record, exc)
        _commit(ledger_real, ledger_dir, _serialize(document), old_bytes)
        raise
    record["state"] = _STATE_ACTIVE
    record["error"] = None
    record["etag"] = result["etag"]
    _commit(ledger_real, ledger_dir, _serialize(document), old_bytes)
    return _copy_record(record), True


def submit(checkpoint: bytes, proof: bytes, tag: str,
           trust_dir: str | None, ledger: str, key: str
           ) -> tuple[dict[str, Any], bool]:
    """Submit one checkpoint/proof package under an idempotency key.

    ``checkpoint`` and ``proof`` are the two package files' raw bytes;
    ``tag`` is the strong entity tag the checkpoint download carried --
    one double-quoted string of 64 lowercase hexadecimal digits, exactly
    the value :func:`~carbon_market.audit_proof.verify_bundle` takes;
    ``trust_dir`` is ``None`` or the persistent trust directory the
    verification extends; ``ledger`` is the acceptance ledger path and
    ``key`` a non-empty idempotency key. Every type or empty-value error
    raises ``ValueError`` before any state is advanced.

    The existing offline verification runs first and decides the state.
    Returns ``(record, created)``, the same idempotent shape as
    :func:`carbon_market.audit.record`: ``record`` is the ledger record
    (fields key, etag, checkpoint_digest, proof_digest, state and error
    in that order) and ``created`` is ``True`` only for the first
    successful acceptance of a new or resumed ``pending`` record.

    * a new key first lands a ``pending`` record and stores both files'
      raw bytes at their content addresses; a successful
      :func:`~carbon_market.audit_proof.verify_bundle` advances the
      record to ``active`` and returns ``(record, True)``;
    * a format failure keeps the evidence, quarantines the record and
      raises :class:`~carbon_market.audit_proof.BundleFormatError`; a
      tag, chain or lifecycle contradiction does the same with
      :class:`~carbon_market.audit_proof.BundleMismatchError`;
    * the identical package resumes a ``pending`` record; an ``active``
      record is fully re-verified against the retained evidence and
      replays the original record with ``False``; a ``quarantined``
      record re-raises the first public exception type without changing
      the retained evidence;
    * the same key with a different package retains both packages' bytes
      and digests, appends one conflict without overwriting the main
      record and raises a plain ``ValueError``.

    A missing ledger parent raises ``FileNotFoundError``; every other
    locking or I/O failure raises ``OSError`` with the previous ledger
    state preserved -- a restart resumes from ``pending`` and ignores
    leftover fragments.
    """
    if isinstance(checkpoint, bool) or not isinstance(checkpoint, bytes):
        raise ValueError("checkpoint must be bytes")
    if isinstance(proof, bool) or not isinstance(proof, bytes):
        raise ValueError("proof must be bytes")
    if not isinstance(tag, str) or not audit_proof._ETAG_RE.fullmatch(tag):
        raise ValueError("tag must be a single strong tag: a quoted "
                         "64-digit lowercase hexadecimal digest")
    if trust_dir is not None \
            and (not isinstance(trust_dir, str) or not trust_dir):
        raise ValueError("trust_dir must be None or a non-empty string")
    if not isinstance(ledger, str) or not ledger:
        raise ValueError("ledger must be a non-empty string")
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a non-empty string")

    checkpoint_digest = hashlib.sha256(checkpoint).hexdigest()
    proof_digest = hashlib.sha256(proof).hexdigest()
    # The record binds the checkpoint's actual strong tag -- the quoted
    # checkpoint digest, the same self-binding convention trust nodes
    # use -- so a record can never cross-bind a carried tag against
    # different evidence. The caller's tag only participates in the
    # three-way verification.
    actual_tag = '"' + checkpoint_digest + '"'
    ledger_real = os.path.realpath(ledger)
    ledger_dir = os.path.dirname(ledger_real) or "."
    store = _get_store(ledger_real)
    with store.lock:
        with _acceptance_lock(ledger_dir):
            # Create the acceptance directory under the exclusive flock,
            # exactly like the trust directory: a missing parent
            # surfaces as FileNotFoundError above or here, an existing
            # non-directory as OSError, and racing first submissions
            # serialize on the lock.
            if not os.path.exists(ledger_dir):
                os.mkdir(ledger_dir)
                _fsync_dir(os.path.dirname(ledger_dir) or ".")
            elif not os.path.isdir(ledger_dir):
                raise OSError(f"{ledger_dir!r} is not a directory")
            document, ledger_old = _read_ledger(ledger_real)
            if document is not None and key in document["records"]:
                return _replay(document, key, checkpoint_digest,
                               proof_digest, checkpoint, proof,
                               trust_dir, ledger_real, ledger_dir,
                               ledger_old)
            if document is None:
                document = {"records": {}, "conflicts": []}

            # The pending record lands before any evidence is stored or
            # verified, so a crash leaves exactly one explainable stage
            # that the identical package can resume.
            record = {"key": key, "etag": actual_tag,
                      "checkpoint_digest": checkpoint_digest,
                      "proof_digest": proof_digest,
                      "state": _STATE_PENDING, "error": None}
            document["records"][key] = record
            pending_bytes = _serialize(document)
            _commit(ledger_real, ledger_dir, pending_bytes, ledger_old)
            _ensure_dir(os.path.join(ledger_dir, "checkpoints"))
            _ensure_dir(os.path.join(ledger_dir, "proofs"))
            cp_path, pf_path = _evidence_paths(
                ledger_dir, checkpoint_digest, proof_digest)
            try:
                _store_evidence(cp_path, checkpoint)
                _store_evidence(pf_path, proof)
                result = _verify_retained(cp_path, pf_path, tag, trust_dir)
            except (audit_proof.BundleFormatError,
                    audit_proof.BundleMismatchError) as exc:
                # Keep the evidence and quarantine before surfacing the
                # public bundle error.
                _quarantine(record, exc)
                _commit(ledger_real, ledger_dir, _serialize(document),
                        pending_bytes)
                raise
            record["state"] = _STATE_ACTIVE
            record["etag"] = result["etag"]
            _commit(ledger_real, ledger_dir, _serialize(document),
                    pending_bytes)
            return _copy_record(record), True


def _read_for_query(ledger: str) -> tuple[str, dict[str, Any]]:
    if not isinstance(ledger, str) or not ledger:
        raise ValueError("ledger must be a non-empty string")
    ledger_real = os.path.realpath(ledger)
    ledger_dir = os.path.dirname(ledger_real) or "."
    with _acceptance_lock(ledger_dir, shared=True):
        document, _ = _read_ledger(ledger_real)
    if document is None:
        raise FileNotFoundError(
            f"acceptance ledger {ledger_real!r} does not exist")
    return ledger_real, document


def get(ledger: str, key: str) -> dict[str, Any]:
    """Return a copy of the acceptance record stored under ``key``.

    ``ledger`` and ``key`` must be non-empty strings, else
    ``ValueError``. A missing ledger raises ``FileNotFoundError``; a
    malformed ledger or unsupported version raises ``ValueError``; an
    unknown key raises ``KeyError(key)``; any other locking or I/O
    failure raises ``OSError``. The returned dict is a fresh copy with
    fields key, etag, checkpoint_digest, proof_digest, state and error in
    that order; mutating it never affects the ledger.
    """
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a non-empty string")
    _, document = _read_for_query(ledger)
    record = document["records"].get(key)
    if record is None:
        raise KeyError(key)
    return _copy_record(record)


def search(ledger: str, cursor: str | None = None,
           limit: int = _DEFAULT_LIMIT, state: str | None = None
           ) -> dict[str, Any]:
    """Page through acceptance records in key code-point order.

    ``ledger`` must be a non-empty string. ``cursor`` is ``None`` or a
    non-empty string; only keys strictly greater than it in code-point
    order are considered, and it composes freely with the state filter.
    ``limit`` is an integer from 1 to 1000 (booleans rejected),
    defaulting to 100. ``state`` is ``None`` or one of ``pending``,
    ``active`` and ``quarantined``. Any other value raises
    ``ValueError``.

    Returns ``{"entries": [[key, record], ...], "next": cursor}``: each
    record is a fresh copy and ``next`` is the last entry's key when
    further matching records remain, else ``None``. A missing ledger
    raises ``FileNotFoundError``; a malformed ledger raises
    ``ValueError``; any other locking or I/O failure raises
    ``OSError``.
    """
    if cursor is not None and (not isinstance(cursor, str) or not cursor):
        raise ValueError("cursor must be None or a non-empty string")
    if not isinstance(limit, int) or isinstance(limit, bool) \
            or not 1 <= limit <= _MAX_LIMIT:
        raise ValueError("limit must be an integer between 1 and 1000")
    if state is not None and state not in _STATES:
        raise ValueError("state filter must be pending, active or "
                         "quarantined")
    _, document = _read_for_query(ledger)

    matches: list[list[Any]] = []
    for audit_key, record in document["records"].items():
        if cursor is not None and audit_key <= cursor:
            continue
        if state is not None and record["state"] != state:
            continue
        matches.append([audit_key, _copy_record(record)])
        if len(matches) > limit:
            break
    if len(matches) > limit:
        page = matches[:limit]
        return {"entries": page, "next": page[-1][0]}
    return {"entries": matches, "next": None}
