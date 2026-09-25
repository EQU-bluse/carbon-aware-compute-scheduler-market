"""Persistent, thread-safe and multi-process resource-supply registry.

One on-disk document (version 1) holds the whole supply side of the
market:

* ``history`` maps each resource id to its published records, versions
  consecutive from 1 and starts strictly increasing across versions;
* ``idempotency`` binds each publish key to the resource it carried;
* ``audit`` holds one publish event per first publication, binding the
  idempotency key, the resource id and the version it created.

Every read entry requires the on-disk document to be exactly the
canonical compact form :func:`_serialize` produces -- fields and keys
in their fixed/code-point order, no whitespace beyond structural
tokens, non-ASCII written through and one trailing newline. Any
out-of-order section, extra whitespace, ``\\uXXXX``-escaped non-ASCII
character, or missing or duplicated final newline is a ``ValueError``.

:func:`publish` appends a new version under an idempotency key,
:func:`get` reads one resource record back by id and optional version,
and :func:`feasible` joins a ``jobs.submit`` acceptance file with the
supply file under both shared locks to rank the resources that can
serve one accepted job at an evaluation moment. The supply file is
never rewritten by the read paths, and a version-1 ``jobs.register``
file is not a supply file.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import tempfile
import threading
from typing import Any, Iterator

from . import jobs as _jobs
from . import signals as _signals
from ._jsonio import finite_loads

__all__ = ["publish", "get", "feasible", "feasible_live"]

_VERSION = 1
_RESOURCE_FIELDS = ("resource_id", "region", "capacity", "start", "end",
                    "unit_cost", "carbon_intensity", "residency")
_RECORD_FIELDS = ("resource_id", "version", "region", "capacity", "start",
                  "end", "unit_cost", "carbon_intensity", "residency")
_ROOT_FIELDS = ("version", "history", "idempotency", "audit")
_EVENT_FIELDS = ("key", "resource_id", "version")
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


def _check_values(resource: dict[str, Any]) -> None:
    region = resource["region"]
    if not isinstance(region, str) or not region:
        raise ValueError("region must be a non-empty string")

    capacity = resource["capacity"]
    if not _is_plain_int(capacity) or capacity <= 0:
        raise ValueError("capacity must be a non-boolean positive integer")

    for name in ("start", "end", "unit_cost", "carbon_intensity"):
        value = resource[name]
        if not _is_plain_int(value) or value < 0:
            raise ValueError(f"{name} must be a non-boolean non-negative "
                             "integer")
    if resource["start"] > resource["end"]:
        raise ValueError("start must not be later than end")

    residency = resource["residency"]
    if not isinstance(residency, list) or not residency:
        raise ValueError("residency must be a non-empty list")
    seen: set[str] = set()
    for entry in residency:
        if not isinstance(entry, str) or not entry:
            raise ValueError("residency regions must be non-empty strings")
        if entry in seen:
            raise ValueError("residency regions must be distinct")
        seen.add(entry)
    # Python str ordering is Unicode code-point ordering.
    if residency != sorted(residency):
        raise ValueError("residency regions must be sorted by code point")
    if region not in seen:
        raise ValueError("residency regions must contain the resource region")


def _normalize_resource(resource: object) -> dict[str, Any]:
    if not isinstance(resource, dict) \
            or set(resource.keys()) != set(_RESOURCE_FIELDS):
        raise ValueError("resource must be an object with exactly "
                         "resource_id, region, capacity, start, end, "
                         "unit_cost, carbon_intensity and residency")

    resource_id = resource["resource_id"]
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("resource_id must be a non-empty string")

    normalized: dict[str, Any] = {
        "resource_id": resource_id,
        "region": resource["region"],
        "capacity": resource["capacity"],
        "start": resource["start"],
        "end": resource["end"],
        "unit_cost": resource["unit_cost"],
        "carbon_intensity": resource["carbon_intensity"],
        "residency": resource["residency"],
    }
    _check_values(normalized)
    normalized["residency"] = list(normalized["residency"])
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
        raise ValueError("supply file root must be an object with keys "
                         "version, history, idempotency and audit")

    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported supply file version")

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
    for resource_id, records_raw in history_raw.items():
        if not isinstance(resource_id, str) or not resource_id:
            raise ValueError("resource ids must be non-empty strings")
        if not isinstance(records_raw, list) or not records_raw:
            raise ValueError("history must hold a non-empty record list "
                             "per resource")
        records: list[dict[str, Any]] = []
        for index, record in enumerate(records_raw, start=1):
            if not isinstance(record, dict) \
                    or set(record.keys()) != set(_RECORD_FIELDS):
                raise ValueError("resource record has invalid fields")
            if record["resource_id"] != resource_id:
                raise ValueError("resource record id does not match its key")
            if not _is_plain_int(record["version"]) \
                    or record["version"] != index:
                raise ValueError("versions must be consecutive from 1")
            _check_values(record)
            if records and record["start"] <= records[-1]["start"]:
                raise ValueError("resource starts must increase across "
                                 "versions")
            records.append({
                "resource_id": resource_id,
                "version": index,
                "region": record["region"],
                "capacity": record["capacity"],
                "start": record["start"],
                "end": record["end"],
                "unit_cost": record["unit_cost"],
                "carbon_intensity": record["carbon_intensity"],
                "residency": list(record["residency"]),
            })
        history[resource_id] = records

    idempotency: dict[str, str] = {}
    for key, resource_id in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        if not isinstance(resource_id, str) or not resource_id \
                or resource_id not in history:
            raise ValueError("idempotency entry must reference a published "
                             "resource")
        idempotency[key] = resource_id

    events: dict[str, dict[str, Any]] = {}
    for key, event in audit_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event, dict) \
                or set(event.keys()) != set(_EVENT_FIELDS):
            raise ValueError("publish audit event has invalid fields")
        event_key = event["key"]
        resource_id = event["resource_id"]
        event_version = event["version"]
        if event_key != key or not isinstance(event_key, str) or not event_key:
            raise ValueError("audit event key does not match its map key")
        if not isinstance(resource_id, str) or not resource_id \
                or resource_id not in history:
            raise ValueError("audit event must reference a published "
                             "resource")
        if not _is_plain_int(event_version) or event_version < 1 \
                or event_version > len(history[resource_id]):
            raise ValueError("audit event must reference a published "
                             "version")
        events[key] = {"key": event_key, "resource_id": resource_id,
                       "version": event_version}

    # The three sections describe one publication history: each
    # idempotency key binds one resource and one publish event, the map
    # and the event agree on the resource, and every published version
    # is bound to exactly one event.
    if set(idempotency) != set(events):
        raise ValueError("idempotency keys and audit events do not match")
    for key, resource_id in idempotency.items():
        if events[key]["resource_id"] != resource_id:
            raise ValueError("audit event does not match its idempotency "
                             "entry")
    published = {(resource_id, record["version"])
                 for resource_id, records in history.items()
                 for record in records}
    bound = {(event["resource_id"], event["version"])
             for event in events.values()}
    if bound != published:
        raise ValueError("every published version must be bound to an "
                         "audit event")

    return history, idempotency, events


def _canonical_bytes(
    history: dict[str, list[dict[str, Any]]],
    idempotency: dict[str, str],
    events: dict[str, dict[str, Any]],
) -> bytes:
    # The canonical wire form every reader accepts: compact UTF-8 JSON
    # with non-ASCII written through, sections in their fixed field order
    # and each section's primary keys in code-point order, terminated by
    # exactly one newline. The round trip also rejects duplicate keys
    # (json.loads keeps the last occurrence) and any escaping of
    # non-ASCII characters that json.dumps would write through.
    return _serialize(history, idempotency, events)


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
            f"supply file {realpath!r} is not valid UTF-8") from exc
    try:
        # Negative-zero and non-finite literals are format errors.
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"supply file {realpath!r} is not valid JSON") from exc
    history, idempotency, events = _validate_structure(data)
    # Every read entry requires the on-disk bytes to be the canonical
    # form: fields and keys in their original order, compact JSON with
    # non-ASCII written through and exactly one trailing newline. Any
    # extra whitespace, escaped non-ASCII character, reordered field or
    # missing/duplicated final newline is a ValueError.
    if raw != _canonical_bytes(history, idempotency, events):
        raise ValueError(
            f"supply file {realpath!r} is not in canonical compact form")
    return history, idempotency, events, raw


def _serialize(
    history: dict[str, list[dict[str, Any]]],
    idempotency: dict[str, str],
    events: dict[str, dict[str, Any]],
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, each section's
    # primary keys in code-point order, terminated by exactly one newline.
    payload = {
        "version": _VERSION,
        "history": {resource_id: history[resource_id]
                    for resource_id in sorted(history)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
        "audit": {key: events[key] for key in sorted(events)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


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
                dir=directory, prefix=".resources-restore-", suffix=".tmp")
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
        dir=directory, prefix=".resources-", suffix=".tmp")
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
    resource: dict[str, object],
    idempotency_key: str,
) -> tuple[dict[str, object], bool]:
    """Persistently publish one resource-supply version.

    ``path`` and ``idempotency_key`` must be non-empty strings. The
    resource must be an object with exactly the eight top-level fields
    resource_id, region, capacity, start, end, unit_cost,
    carbon_intensity and residency: non-empty id and region strings, a
    positive integer capacity, non-negative start, end, unit_cost and
    carbon_intensity (booleans rejected) with start not later than end,
    and a non-empty residency list of distinct non-empty strings sorted
    by code point that contains the resource's own region. Any
    deviation raises ``ValueError`` before the file is touched.

    A first publication saves the canonical record -- the resource
    fields plus the next consecutive ``version`` for that resource id,
    starting at 1 -- together with the idempotency binding and a
    publish audit event carrying the key, the resource id and the
    version, all three in one durable commit, and returns
    ``(record, True)``. A new version of an already published resource
    must carry a strictly later start than the current latest version.
    Replaying the same key with an identical resource returns the
    stored record with ``False`` without adding an event or rewriting
    the file; the same key with a different resource raises
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
    normalized = _normalize_resource(resource)
    resource_id = normalized["resource_id"]

    store = _get_store(path)
    with store.lock:
        # Opening the companion lock in a missing directory surfaces as
        # FileNotFoundError before the data file is created.
        with _process_lock(store.realpath):
            history, idempotency, events, old_bytes = _load_file(
                store.realpath)

            existing_id = idempotency.get(idempotency_key)
            if existing_id is not None:
                event = events[idempotency_key]
                existing = history[existing_id][event["version"] - 1]
                if {field: existing[field]
                        for field in _RESOURCE_FIELDS} != normalized:
                    raise ValueError("idempotency key was already used with "
                                     "a different resource")
                return dict(existing), False

            versions = history.get(resource_id)
            if versions is None:
                versions = []
                history[resource_id] = versions
                version = 1
            else:
                if normalized["start"] <= versions[-1]["start"]:
                    raise ValueError("resource start must increase across "
                                     "versions")
                version = versions[-1]["version"] + 1

            record: dict[str, Any] = {
                "resource_id": resource_id,
                "version": version,
                "region": normalized["region"],
                "capacity": normalized["capacity"],
                "start": normalized["start"],
                "end": normalized["end"],
                "unit_cost": normalized["unit_cost"],
                "carbon_intensity": normalized["carbon_intensity"],
                "residency": list(normalized["residency"]),
            }
            versions.append(record)
            idempotency[idempotency_key] = resource_id
            events[idempotency_key] = {
                "key": idempotency_key,
                "resource_id": resource_id,
                "version": version,
            }
            _commit_file(store.realpath,
                         _serialize(history, idempotency, events), old_bytes)
            return dict(record), True


def get(
    path: str,
    resource_id: str,
    version: int | None = None,
) -> dict[str, object]:
    """Return a read-only copy of one published resource record.

    ``path`` and ``resource_id`` must be non-empty strings and
    ``version``, when given, a non-boolean positive integer, else
    ``ValueError``. Without ``version`` the latest record of the
    resource is returned. A missing supply file raises
    ``FileNotFoundError``; an invalid file -- including non-canonical
    bytes -- raises ``ValueError``; an unknown resource id raises
    ``KeyError(resource_id)`` and an unknown version
    ``KeyError(version)``; any other locking or I/O failure raises
    ``OSError``. The call never writes, and the returned record is a
    fresh copy with resource_id, version, region, capacity, start, end,
    unit_cost, carbon_intensity and residency.
    """
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("resource_id must be a non-empty string")
    if version is not None \
            and (not _is_plain_int(version) or version <= 0):
        raise ValueError("version must be a non-boolean positive integer")

    realpath = os.path.realpath(path)
    with _process_lock(realpath, shared=True):
        history, _idempotency, _events, raw = _load_file(realpath)
    if raw is None:
        raise FileNotFoundError(f"supply file {realpath!r} does not exist")
    versions = history.get(resource_id)
    if versions is None:
        raise KeyError(resource_id)
    if version is None:
        return dict(versions[-1])
    if version > len(versions):
        raise KeyError(version)
    return dict(versions[version - 1])

def feasible(
    job_path: str,
    supply_path: str,
    job_id: str,
    at: int,
) -> list[dict[str, object]]:
    """Rank the resources that can serve one accepted job at ``at``.

    ``job_path`` and ``supply_path`` must be non-empty strings,
    ``job_id`` a non-empty string and ``at`` a non-boolean non-negative
    integer evaluation moment, else ``ValueError``. The job file must be
    a ``jobs.submit`` acceptance file -- a version-1 ``jobs.register``
    file is invalid structure -- and both files are read as complete
    snapshots while their shared locks are held together.

    A missing acceptance or supply file raises ``FileNotFoundError``;
    an invalid file -- including a supply file outside canonical
    compact form -- raises ``ValueError``; an unknown job id raises
    ``KeyError(job_id)``; any other locking or I/O failure raises
    ``OSError``.

    Each resource contributes only its highest version valid at ``at``
    (start not later than ``at`` not later than end). A candidate must
    sit in one of the job's regions, offer at least the job's work as
    capacity, cover the job's residency regions with its own residency
    list, and stay available at least until the job's deadline. The
    total cost is the job's work times the unit cost and the total
    carbon the work times the carbon intensity; a candidate exceeding
    the job's max_cost or carbon_cap budget is excluded. The result is
    ordered by carbon intensity, then unit cost, then resource id, each
    entry carrying the resource record and both totals; with no
    candidate the list is empty.
    """
    if not isinstance(job_path, str) or not job_path:
        raise ValueError("job_path must be a non-empty string")
    if not isinstance(supply_path, str) or not supply_path:
        raise ValueError("supply_path must be a non-empty string")
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    job_real = os.path.realpath(job_path)
    supply_real = os.path.realpath(supply_path)
    # Lock both snapshots together, in a path order shared by every
    # caller, so concurrent calls can never deadlock.
    with contextlib.ExitStack() as stack:
        for locked in sorted({job_real, supply_real}):
            stack.enter_context(_process_lock(locked, shared=True))

        accepted, _job_map, _job_events, job_raw = _jobs._load_submit_file(
            job_real)
        if job_raw is None:
            raise FileNotFoundError(
                f"acceptance file {job_real!r} does not exist")
        history, _idempotency, _events, supply_raw = _load_file(supply_real)
        if supply_raw is None:
            raise FileNotFoundError(
                f"supply file {supply_real!r} does not exist")

        job = accepted.get(job_id)
        if job is None:
            raise KeyError(job_id)

        work = job["work"]
        regions = set(job["regions"])
        residency = set(job["residency"])
        candidates: list[dict[str, object]] = []
        for records in history.values():
            active: dict[str, Any] | None = None
            for record in records:
                if record["start"] <= at <= record["end"]:
                    active = record
            if active is None:
                continue
            if active["region"] not in regions:
                continue
            if active["capacity"] < work:
                continue
            if not residency <= set(active["residency"]):
                continue
            if active["end"] < job["deadline"]:
                continue
            total_cost = work * active["unit_cost"]
            total_carbon = work * active["carbon_intensity"]
            if total_cost > job["max_cost"] \
                    or total_carbon > job["carbon_cap"]:
                continue
            candidates.append({
                "resource": dict(active),
                "total_cost": total_cost,
                "total_carbon": total_carbon,
            })

    candidates.sort(key=lambda entry: (entry["resource"]["carbon_intensity"],
                                       entry["resource"]["unit_cost"],
                                       entry["resource"]["resource_id"]))
    return candidates


def feasible_live(
    job_path: str,
    supply_path: str,
    signal_path: str,
    job_id: str,
    at: int,
) -> list[dict[str, object]]:
    """Rank resources for one accepted job at ``at`` against live signals.

    This is the dynamic counterpart of :func:`feasible`: the
    ``jobs.submit`` acceptance file, the supply file and the live signal
    file are read as one snapshot while their shared locks are held
    together. ``job_path``, ``supply_path`` and ``signal_path`` must be
    non-empty strings (and resolve to distinct real locations),
    ``job_id`` a non-empty string and ``at`` a non-boolean non-negative
    integer evaluation moment, else ``ValueError``.

    As in :func:`feasible`, each resource contributes only its highest
    supply version valid at ``at`` and must sit in one of the job's
    regions, offer at least the job's work as capacity, cover the job's
    residency regions and stay available at least until the job's
    deadline. Instead of the version's static figures, the unit cost and
    carbon intensity are taken from the resource region's latest signal
    observed no later than ``at`` and not expired at it; a resource
    whose region has no valid signal at ``at`` is simply excluded. The
    total cost is the job's work times the signal unit cost and the
    total carbon the work times the signal carbon intensity, with the
    same max_cost and carbon_cap budget exclusions as :func:`feasible`.

    The result is ordered by signal carbon intensity, then signal unit
    cost, then resource id; each entry carries the resource record, the
    signal record (including its version) and both totals --
    ``resource``, ``signal``, ``total_cost`` and ``total_carbon`` in that
    order -- and is an empty list when nothing qualifies.

    A missing acceptance, supply or signal file raises
    ``FileNotFoundError``; an invalid file -- including non-canonical
    bytes -- raises ``ValueError``; an unknown job id raises
    ``KeyError(job_id)``; the absence of a valid signal is never an
    error, it only removes the resource; any other locking or I/O
    failure raises ``OSError``.
    """
    if not isinstance(job_path, str) or not job_path:
        raise ValueError("job_path must be a non-empty string")
    if not isinstance(supply_path, str) or not supply_path:
        raise ValueError("supply_path must be a non-empty string")
    if not isinstance(signal_path, str) or not signal_path:
        raise ValueError("signal_path must be a non-empty string")
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    job_real = os.path.realpath(job_path)
    supply_real = os.path.realpath(supply_path)
    signal_real = os.path.realpath(signal_path)
    if len({job_real, supply_real, signal_real}) != 3:
        raise ValueError("job, supply and signal paths must be distinct "
                         "real paths")

    # Lock all three snapshots together, in a path order shared by every
    # caller, so concurrent calls can never deadlock.
    with contextlib.ExitStack() as stack:
        for locked in sorted({job_real, supply_real, signal_real}):
            stack.enter_context(_process_lock(locked, shared=True))

        accepted, _job_map, _job_events, job_raw = _jobs._load_submit_file(
            job_real)
        if job_raw is None:
            raise FileNotFoundError(
                f"acceptance file {job_real!r} does not exist")
        history, _idempotency, _events, supply_raw = _load_file(supply_real)
        if supply_raw is None:
            raise FileNotFoundError(
                f"supply file {supply_real!r} does not exist")
        signal_history, _signal_map, _signal_events, signal_raw = \
            _signals._load_file(signal_real)
        if signal_raw is None:
            raise FileNotFoundError(
                f"signal file {signal_real!r} does not exist")

        job = accepted.get(job_id)
        if job is None:
            raise KeyError(job_id)

        work = job["work"]
        regions = set(job["regions"])
        residency = set(job["residency"])
        candidates: list[dict[str, object]] = []
        for records in history.values():
            active: dict[str, Any] | None = None
            for record in records:
                if record["start"] <= at <= record["end"]:
                    active = record
            if active is None:
                continue
            if active["region"] not in regions:
                continue
            if active["capacity"] < work:
                continue
            if not residency <= set(active["residency"]):
                continue
            if active["end"] < job["deadline"]:
                continue
            # The live signal drives both figures; a region without a
            # current signal contributes no candidate.
            signal = None
            for signal_record in signal_history.get(active["region"], ()):
                if signal_record["observed"] <= at <= signal_record["expires"]:
                    signal = signal_record
            if signal is None:
                continue
            total_cost = work * signal["unit_cost"]
            total_carbon = work * signal["carbon_intensity"]
            if total_cost > job["max_cost"] \
                    or total_carbon > job["carbon_cap"]:
                continue
            signal_copy = dict(signal)
            signal_copy["mix"] = dict(signal["mix"])
            candidates.append({
                "resource": dict(active),
                "signal": signal_copy,
                "total_cost": total_cost,
                "total_carbon": total_carbon,
            })

    candidates.sort(key=lambda entry: (
        entry["signal"]["carbon_intensity"],
        entry["signal"]["unit_cost"],
        entry["resource"]["resource_id"]))
    return candidates
