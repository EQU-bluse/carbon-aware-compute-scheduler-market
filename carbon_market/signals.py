"""Persistent, thread-safe and multi-process live signal ledger.

The baseline registry only knows the static unit cost and carbon
intensity baked into a published resource version. This module adds an
independent ledger of live signals -- per-region observations of the
energy mix, unit cost and carbon intensity at a moment in time:

* ``history`` maps each region to its published signals, versions
  consecutive from 1 with observation moments strictly increasing
  across versions and each signal not expiring before it was observed;
* ``idempotency`` binds each publish key to the region it carried;
* ``audit`` holds one publish event per first publication, binding the
  idempotency key, the region and the version it created.

A signal carries exactly the region, the observation and expiration
moments, the energy mix (a non-empty map of source shares, integer
basis points summing to ten thousand), the unit cost and the carbon
intensity; every numeric value is a non-boolean non-negative integer.

As for the supply file, every read requires the on-disk document to be
exactly the canonical compact form :func:`_serialize` produces --
fields and keys in their fixed/code-point order, no whitespace beyond
structural tokens, non-ASCII written through and one trailing newline.

:func:`publish` appends a new signal version under an idempotency key,
and :func:`get` returns the region's latest version observed no later
than the evaluation moment and not expired at it. The signal file is
never rewritten by the read path.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import tempfile
import threading
from typing import Any, Iterator

from ._jsonio import finite_loads

__all__ = ["publish", "get"]

_VERSION = 1
_MIX_TOTAL = 10_000
_SIGNAL_FIELDS = ("region", "observed", "expires", "mix", "unit_cost",
                  "carbon_intensity")
_RECORD_FIELDS = ("region", "version", "observed", "expires", "mix",
                  "unit_cost", "carbon_intensity")
_ROOT_FIELDS = ("version", "history", "idempotency", "audit")
_EVENT_FIELDS = ("key", "region", "version")
_LOCK_SUFFIX = ".lock"


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
def _process_lock(realpath: str, *, shared: bool = False) -> Iterator[None]:
    # The companion lock file is never unlinked; the kernel releases the
    # flock on process exit, so a leftover lock never blocks a later
    # call. Equivalent real paths in different processes therefore share
    # the same exclusive lock as the per-realpath in-process lock.
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


def _normalize_mix(mix: object) -> dict[str, int]:
    if not isinstance(mix, dict) or not mix:
        raise ValueError("mix must be a non-empty object")
    normalized: dict[str, int] = {}
    for source, share in mix.items():
        if not isinstance(source, str) or not source:
            raise ValueError("mix sources must be non-empty strings")
        if not _is_plain_int(share) or share < 0:
            raise ValueError("mix shares must be non-boolean non-negative "
                             "integers")
        if source in normalized:
            raise ValueError("mix sources must be distinct")
        normalized[source] = share
    if sum(normalized.values()) != _MIX_TOTAL:
        raise ValueError("mix shares must sum to ten thousand")
    # Stored in code-point order like every other canonical mapping.
    return {source: normalized[source] for source in sorted(normalized)}


def _check_values(signal: dict[str, Any]) -> None:
    region = signal["region"]
    if not isinstance(region, str) or not region:
        raise ValueError("region must be a non-empty string")

    for name in ("observed", "expires", "unit_cost", "carbon_intensity"):
        value = signal[name]
        if not _is_plain_int(value) or value < 0:
            raise ValueError(f"{name} must be a non-boolean non-negative "
                             "integer")
    if signal["expires"] < signal["observed"]:
        raise ValueError("expires must not be earlier than observed")

    signal["mix"] = _normalize_mix(signal["mix"])


def _normalize_signal(signal: object) -> dict[str, Any]:
    if not isinstance(signal, dict) \
            or set(signal.keys()) != set(_SIGNAL_FIELDS):
        raise ValueError("signal must be an object with exactly region, "
                         "observed, expires, mix, unit_cost and "
                         "carbon_intensity")

    normalized: dict[str, Any] = {
        "region": signal["region"],
        "observed": signal["observed"],
        "expires": signal["expires"],
        "mix": signal["mix"],
        "unit_cost": signal["unit_cost"],
        "carbon_intensity": signal["carbon_intensity"],
    }
    _check_values(normalized)
    return normalized


def _check_sorted_keys(mapping: dict[Any, Any], label: str) -> None:
    keys = list(mapping)
    if keys != sorted(keys):
        raise ValueError(f"{label} must be ordered by key code point")


def _validate_structure(data: object) -> tuple[
    dict[str, list[dict[str, Any]]], dict[str, str],
    dict[str, dict[str, Any]]
]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("signal file root must be an object with keys "
                         "version, history, idempotency and audit")

    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported signal file version")

    history_raw = data["history"]
    idempotency_raw = data["idempotency"]
    audit_raw = data["audit"]
    if not isinstance(history_raw, dict) \
            or not isinstance(idempotency_raw, dict) \
            or not isinstance(audit_raw, dict):
        raise ValueError("history, idempotency and audit must be objects")
    _check_sorted_keys(history_raw, "history")
    _check_sorted_keys(idempotency_raw, "idempotency")
    _check_sorted_keys(audit_raw, "audit")

    history: dict[str, list[dict[str, Any]]] = {}
    for region, records_raw in history_raw.items():
        if not isinstance(region, str) or not region:
            raise ValueError("regions must be non-empty strings")
        if not isinstance(records_raw, list) or not records_raw:
            raise ValueError("history must hold a non-empty record list "
                             "per region")
        records: list[dict[str, Any]] = []
        for index, record in enumerate(records_raw, start=1):
            if not isinstance(record, dict) \
                    or set(record.keys()) != set(_RECORD_FIELDS):
                raise ValueError("signal record has invalid fields")
            if record["region"] != region:
                raise ValueError("signal record region does not match its "
                                 "key")
            if not _is_plain_int(record["version"]) \
                    or record["version"] != index:
                raise ValueError("versions must be consecutive from 1")
            _check_values(record)
            if records and record["observed"] <= records[-1]["observed"]:
                raise ValueError("signal observations must increase across "
                                 "versions")
            records.append({
                "region": region,
                "version": index,
                "observed": record["observed"],
                "expires": record["expires"],
                "mix": dict(record["mix"]),
                "unit_cost": record["unit_cost"],
                "carbon_intensity": record["carbon_intensity"],
            })
        history[region] = records

    idempotency: dict[str, str] = {}
    for key, region in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        if not isinstance(region, str) or not region or region not in history:
            raise ValueError("idempotency entry must reference a published "
                             "region")
        idempotency[key] = region

    events: dict[str, dict[str, Any]] = {}
    for key, event in audit_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event, dict) \
                or set(event.keys()) != set(_EVENT_FIELDS):
            raise ValueError("signal audit event has invalid fields")
        event_key = event["key"]
        region = event["region"]
        event_version = event["version"]
        if event_key != key or not isinstance(event_key, str) or not event_key:
            raise ValueError("audit event key does not match its map key")
        if not isinstance(region, str) or not region or region not in history:
            raise ValueError("audit event must reference a published region")
        if not _is_plain_int(event_version) or event_version < 1 \
                or event_version > len(history[region]):
            raise ValueError("audit event must reference a published version")
        events[key] = {"key": event_key, "region": region,
                       "version": event_version}

    # The three sections describe one publication history: each
    # idempotency key binds one region and one publish event, the map and
    # the event agree on the region, and every published version is bound
    # to exactly one event.
    if set(idempotency) != set(events):
        raise ValueError("idempotency keys and audit events do not match")
    for key, region in idempotency.items():
        if events[key]["region"] != region:
            raise ValueError("audit event does not match its idempotency "
                             "entry")
    published = {(region, record["version"])
                 for region, records in history.items()
                 for record in records}
    bound = {(event["region"], event["version"])
             for event in events.values()}
    if bound != published:
        raise ValueError("every published version must be bound to an "
                         "audit event")

    return history, idempotency, events


def _serialize(
    history: dict[str, list[dict[str, Any]]],
    idempotency: dict[str, str],
    events: dict[str, dict[str, Any]],
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, each section's
    # primary keys in code-point order, terminated by exactly one newline.
    payload = {
        "version": _VERSION,
        "history": {region: history[region] for region in sorted(history)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
        "audit": {key: events[key] for key in sorted(events)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_file(realpath: str) -> tuple[
    dict[str, list[dict[str, Any]]], dict[str, str],
    dict[str, dict[str, Any]], bytes | None
]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return {}, {}, {}, None

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"signal file {realpath!r} is not valid UTF-8") from exc
    try:
        # Negative-zero and non-finite literals are format errors.
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"signal file {realpath!r} is not valid JSON") from exc
    history, idempotency, events = _validate_structure(data)
    # Every read entry requires the on-disk bytes to be the canonical
    # form: fields and keys in their original order, compact JSON with
    # non-ASCII written through and exactly one trailing newline.
    if raw != _serialize(history, idempotency, events):
        raise ValueError(
            f"signal file {realpath!r} is not in canonical compact form")
    return history, idempotency, events, raw


def _fsync_directory(directory: str) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _rollback_file(realpath: str, directory: str,
                   old_bytes: bytes | None, first: BaseException) -> None:
    # Restore the exact pre-call bytes while the exclusive lock is held:
    # stage them back over the replaced file, or remove a file that did
    # not exist beforehand, then sync the directory. A failed recovery
    # surfaces chained after the original error.
    try:
        if old_bytes is None:
            try:
                os.unlink(realpath)
            except FileNotFoundError:
                pass
        else:
            fd, tmp_path = tempfile.mkstemp(
                dir=directory, prefix=".signals-restore-", suffix=".tmp")
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


def _commit_file(realpath: str, payload: bytes,
                 old_bytes: bytes | None) -> None:
    # One durable commit for the history, the idempotency map and the
    # audit event: same-directory temporary, fsync, atomic replace and a
    # directory fsync. Any failure after the replace restores the
    # pre-call bytes, so an unsuccessful publish leaves the original file
    # byte-for-byte.
    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".signals-", suffix=".tmp")
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


def publish(
    path: str,
    signal: dict[str, object],
    idempotency_key: str,
) -> tuple[dict[str, object], bool]:
    """Persistently publish one live signal version.

    ``path`` and ``idempotency_key`` must be non-empty strings. The
    signal must be an object with exactly the six top-level fields
    region, observed, expires, mix, unit_cost and carbon_intensity: a
    non-empty region string, non-boolean non-negative observation and
    expiration moments (booleans rejected) with the expiration not
    earlier than the observation, non-negative unit cost and carbon
    intensity, and a non-empty energy ``mix`` object whose values are
    non-negative integers summing to exactly ten thousand. Any deviation
    raises ``ValueError`` before the file is touched.

    A first publication saves the canonical record -- the signal fields
    plus the next consecutive ``version`` for that region, starting at
    1 -- together with the idempotency binding and a publish audit event
    carrying the key, the region and the version, all three in one
    durable commit, and returns ``(record, True)``. A new version for an
    already observed region must carry a strictly later observation
    moment. Replaying the same key with an identical signal returns the
    stored record with ``False`` without adding an event or rewriting
    the file; the same key with a different signal raises
    ``ValueError`` and preserves the original bytes.

    A missing parent directory raises ``FileNotFoundError``; an invalid
    existing file (encoding, JSON, negative-zero or non-finite numbers,
    version, structure, ordering, canonical bytes or references) raises
    ``ValueError``; any other locking or I/O failure raises ``OSError``.
    Calls serialize across threads and processes per resolved real path.
    """
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")
    if not isinstance(idempotency_key, str) or not idempotency_key:
        raise ValueError("idempotency_key must be a non-empty string")
    normalized = _normalize_signal(signal)
    region = normalized["region"]

    store = _get_store(path)
    with store.lock:
        # Opening the companion lock in a missing directory surfaces as
        # FileNotFoundError before the data file is created.
        with _process_lock(store.realpath):
            history, idempotency, events, old_bytes = _load_file(
                store.realpath)

            existing_region = idempotency.get(idempotency_key)
            if existing_region is not None:
                event = events[idempotency_key]
                existing = history[existing_region][event["version"] - 1]
                if {field: existing[field] for field in _SIGNAL_FIELDS} \
                        != normalized:
                    raise ValueError("idempotency key was already used with "
                                     "a different signal")
                return dict(existing), False

            versions = history.get(region)
            if versions is None:
                versions = []
                history[region] = versions
                version = 1
            else:
                if normalized["observed"] <= versions[-1]["observed"]:
                    raise ValueError("signal observation must increase "
                                     "across versions")
                version = versions[-1]["version"] + 1

            record: dict[str, Any] = {
                "region": region,
                "version": version,
                "observed": normalized["observed"],
                "expires": normalized["expires"],
                "mix": dict(normalized["mix"]),
                "unit_cost": normalized["unit_cost"],
                "carbon_intensity": normalized["carbon_intensity"],
            }
            versions.append(record)
            idempotency[idempotency_key] = region
            events[idempotency_key] = {
                "key": idempotency_key,
                "region": region,
                "version": version,
            }
            _commit_file(store.realpath,
                         _serialize(history, idempotency, events), old_bytes)
            return dict(record), True


def get(path: str, region: str, at: int) -> dict[str, object]:
    """Return the region's latest signal valid at evaluation moment ``at``.

    ``path`` and ``region`` must be non-empty strings and ``at`` a
    non-boolean non-negative integer evaluation moment, else
    ``ValueError``. The latest record whose observation is not later
    than ``at`` and whose expiration is not earlier than ``at`` is
    returned -- when several windows cover the moment, the highest
    version wins. A missing signal file raises ``FileNotFoundError``; an
    invalid file -- including non-canonical bytes -- raises
    ``ValueError``; an unknown region raises ``KeyError(region)`` and
    the absence of any valid version ``LookupError``; any other locking
    or I/O failure raises ``OSError``. The call never writes, and the
    returned record is a fresh copy with region, version, observed,
    expires, mix, unit_cost and carbon_intensity.
    """
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")
    if not isinstance(region, str) or not region:
        raise ValueError("region must be a non-empty string")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    realpath = os.path.realpath(path)
    with _process_lock(realpath, shared=True):
        history, _idempotency, _events, raw = _load_file(realpath)
    if raw is None:
        raise FileNotFoundError(f"signal file {realpath!r} does not exist")
    versions = history.get(region)
    if versions is None:
        raise KeyError(region)

    valid: dict[str, Any] | None = None
    for record in versions:
        if record["observed"] <= at <= record["expires"]:
            valid = record
    if valid is None:
        raise LookupError(region)
    result = dict(valid)
    result["mix"] = dict(valid["mix"])
    return result
