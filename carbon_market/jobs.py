"""Persistent, thread-safe job registries.

The version 1 :func:`register` registry (job_id, deadline, energy_wh,
residency_regions) stays as it was. :func:`submit` and :func:`get` are
the version 2 acceptance API for complete scheduling-constraint jobs:
:func:`submit` durably accepts a job -- job_id, work, deadline, regions,
residency, max_cost and carbon_cap -- under an idempotency key together
with its submit audit event, and :func:`get` answers the read-only
record by job id. A version 1 file is never read as or upgraded to
version 2.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import tempfile
import threading
from typing import Any, Iterator

from ._jsonio import finite_loads, strict_loads

__all__ = ["register", "submit", "get"]

_VERSION = 1
_STATE = "queued"
_JOB_FIELDS = ("job_id", "deadline", "energy_wh", "residency_regions", "state")
_REQUIRED_FIELDS = frozenset(_JOB_FIELDS[:-1])
_ROOT_FIELDS = ("version", "jobs", "idempotency")


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


def _normalize_job(job: object) -> tuple[str, int, int, list[str]]:
    if not isinstance(job, dict) or set(job.keys()) != _REQUIRED_FIELDS:
        raise ValueError("job must be an object with exactly job_id, deadline, "
                         "energy_wh and residency_regions")

    job_id = job["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")

    deadline = job["deadline"]
    if not _is_plain_int(deadline) or deadline < 0:
        raise ValueError("deadline must be a non-boolean non-negative integer")

    energy_wh = job["energy_wh"]
    if not _is_plain_int(energy_wh) or energy_wh <= 0:
        raise ValueError("energy_wh must be a non-boolean positive integer")

    regions = job["residency_regions"]
    if not isinstance(regions, list) or not regions:
        raise ValueError("residency_regions must be a non-empty list")
    normalized: list[str] = []
    seen: set[str] = set()
    for region in regions:
        if not isinstance(region, str) or not region:
            raise ValueError("residency regions must be non-empty strings")
        if region in seen:
            raise ValueError("residency regions must be distinct")
        seen.add(region)
        normalized.append(region)
    normalized.sort()  # Python str ordering is Unicode code-point ordering.

    return job_id, deadline, energy_wh, normalized


def _validate_regions(regions: object) -> list[str]:
    if not isinstance(regions, list) or not regions:
        raise ValueError("residency_regions must be a non-empty list")
    seen: set[str] = set()
    for region in regions:
        if not isinstance(region, str) or not region or region in seen:
            raise ValueError("invalid residency region")
        seen.add(region)
    if regions != sorted(regions):
        raise ValueError("residency_regions are not normalized")
    return list(regions)


def _validate_structure(
    data: object,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("registry root must be an object with keys "
                         "version, jobs and idempotency")

    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported registry version")

    jobs_raw = data["jobs"]
    idempotency_raw = data["idempotency"]
    if not isinstance(jobs_raw, dict) or not isinstance(idempotency_raw, dict):
        raise ValueError("jobs and idempotency must be objects")

    jobs: dict[str, dict[str, Any]] = {}
    for name, record in jobs_raw.items():
        if not isinstance(name, str) or not name:
            raise ValueError("job ids must be non-empty strings")
        if not isinstance(record, dict) or set(record.keys()) != set(_JOB_FIELDS):
            raise ValueError("job record has invalid fields")
        if record["job_id"] != name:
            raise ValueError("job record id does not match its key")
        if not isinstance(record["job_id"], str):
            raise ValueError("job_id must be a string")
        if not _is_plain_int(record["deadline"]) or record["deadline"] < 0:
            raise ValueError("invalid deadline")
        if not _is_plain_int(record["energy_wh"]) or record["energy_wh"] <= 0:
            raise ValueError("invalid energy_wh")
        _validate_regions(record["residency_regions"])
        if record["state"] != _STATE:
            raise ValueError("invalid job state")
        jobs[name] = dict(record)

    idempotency: dict[str, str] = {}
    referenced: set[str] = set()
    for key, job_id in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        if not isinstance(job_id, str) or not job_id or job_id not in jobs:
            raise ValueError("idempotency entry must reference a registered job")
        if job_id in referenced:
            raise ValueError("job registered under more than one idempotency key")
        referenced.add(job_id)
        idempotency[key] = job_id

    return jobs, idempotency


def _load(realpath: str) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    try:
        with open(realpath, encoding="utf-8") as handle:
            text = handle.read()
    except FileNotFoundError:
        return {}, {}

    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(f"registry file {realpath!r} is not valid JSON") from exc
    return _validate_structure(data)


def _atomic_write(
    realpath: str,
    jobs: dict[str, dict[str, Any]],
    idempotency: dict[str, str],
) -> None:
    payload = {
        "version": _VERSION,
        "jobs": {name: jobs[name] for name in sorted(jobs)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"

    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".jobs-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, realpath)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise

    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def register(
    path: str,
    job: dict[str, object],
    idempotency_key: str,
) -> tuple[dict[str, object], bool]:
    """Register a job idempotently, persisting it as compact UTF-8 JSON.

    Returns ``(record, created)`` where ``record`` carries job_id, deadline,
    energy_wh, residency_regions (sorted by code point) and ``state`` set to
    ``"queued"``; ``created`` is ``True`` for a new registration and ``False``
    when the idempotency key replays an identical, normalized job.
    """
    if not isinstance(path, str):
        raise ValueError("path must be a string")
    if not isinstance(idempotency_key, str) or not idempotency_key:
        raise ValueError("idempotency_key must be a non-empty string")
    job_id, deadline, energy_wh, regions = _normalize_job(job)

    store = _get_store(path)
    with store.lock:
        jobs, idempotency = _load(store.realpath)
        record: dict[str, object] = {
            "job_id": job_id,
            "deadline": deadline,
            "energy_wh": energy_wh,
            "residency_regions": regions,
            "state": _STATE,
        }

        existing_id = idempotency.get(idempotency_key)
        if existing_id is not None:
            existing = jobs[existing_id]
            if existing != record:
                raise ValueError("idempotency key was already used with a "
                                 "different job")
            return dict(existing), False

        if job_id in jobs:
            raise ValueError("job_id is already registered under another "
                             "idempotency key")

        jobs[job_id] = record
        idempotency[idempotency_key] = job_id
        _atomic_write(store.realpath, jobs, idempotency)
        return dict(record), True


# --- Version 2: complete scheduling-constraint acceptance ---------------

_V2 = 2
_V2_STATE = "queued"
_V2_ROOT_FIELDS = ("version", "jobs", "idempotency", "events")
_V2_JOB_FIELDS = ("job_id", "work", "deadline", "regions", "residency",
                  "max_cost", "carbon_cap", "state")
_V2_JOB_INPUT_FIELDS = frozenset(_V2_JOB_FIELDS[:-1])
_V2_EVENT_FIELDS = ("type", "idempotency_key", "job_id", "result")
_V2_EVENT_TYPE = "submit"


def _is_non_empty_string(value: object) -> bool:
    return isinstance(value, str) and not isinstance(value, bool) and bool(value)


def _normalize_v2_job(
    job: object,
) -> tuple[str, int, int, list[str], list[str], int, int]:
    if not isinstance(job, dict) or set(job.keys()) != _V2_JOB_INPUT_FIELDS:
        raise ValueError("job must be an object with exactly job_id, work, "
                         "deadline, regions, residency, max_cost and "
                         "carbon_cap")

    job_id = job["job_id"]
    if not _is_non_empty_string(job_id):
        raise ValueError("job_id must be a non-empty string")

    work = job["work"]
    if not _is_plain_int(work) or work <= 0:
        raise ValueError("work must be a non-boolean positive integer")

    deadline = job["deadline"]
    if not _is_plain_int(deadline) or deadline < 0:
        raise ValueError("deadline must be a non-boolean non-negative "
                         "integer")

    max_cost = job["max_cost"]
    if not _is_plain_int(max_cost) or max_cost < 0:
        raise ValueError("max_cost must be a non-boolean non-negative "
                         "integer")

    carbon_cap = job["carbon_cap"]
    if not _is_plain_int(carbon_cap) or carbon_cap < 0:
        raise ValueError("carbon_cap must be a non-boolean non-negative "
                         "integer")

    regions = _normalize_region_list(job["regions"], "regions")
    residency = _normalize_region_list(job["residency"], "residency")

    allowed = set(regions)
    if not set(residency).issubset(allowed):
        raise ValueError("residency regions must be a subset of regions")

    return job_id, work, deadline, regions, residency, max_cost, carbon_cap


def _normalize_region_list(value: object, name: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} must be a non-empty list of non-empty "
                         "strings")
    normalized: list[str] = []
    seen: set[str] = set()
    for region in value:
        if not _is_non_empty_string(region):
            raise ValueError(f"{name} must be non-empty strings")
        if region in seen:
            raise ValueError(f"{name} regions must be distinct")
        seen.add(region)
        normalized.append(region)
    normalized.sort()  # Python str ordering is Unicode code-point ordering.
    return normalized


def _v2_record(
    job_id: str,
    work: int,
    deadline: int,
    regions: list[str],
    residency: list[str],
    max_cost: int,
    carbon_cap: int,
) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "work": work,
        "deadline": deadline,
        "regions": regions,
        "residency": residency,
        "max_cost": max_cost,
        "carbon_cap": carbon_cap,
        "state": _V2_STATE,
    }


def _validate_v2_regions(value: object, name: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} must be a non-empty list of non-empty "
                         "strings")
    validated: list[str] = []
    seen: set[str] = set()
    for region in value:
        if not _is_non_empty_string(region) or region in seen:
            raise ValueError(f"invalid {name} region")
        seen.add(region)
        validated.append(region)
    if value != sorted(value):
        raise ValueError(f"{name} regions must be ordered by code point")
    return validated


def _validate_v2_structure(data: object) -> dict[str, Any]:
    if not isinstance(data, dict) or list(data.keys()) != list(_V2_ROOT_FIELDS):
        raise ValueError("acceptance file root must be an object with keys "
                         "version, jobs, idempotency and events, in that "
                         "order")

    version = data["version"]
    if not _is_plain_int(version) or version != _V2:
        raise ValueError("unsupported acceptance file version")

    jobs_raw = data["jobs"]
    idempotency_raw = data["idempotency"]
    events_raw = data["events"]
    if not isinstance(jobs_raw, dict) \
            or not isinstance(idempotency_raw, dict) \
            or not isinstance(events_raw, dict):
        raise ValueError("jobs, idempotency and events must be objects")

    if list(jobs_raw) != sorted(jobs_raw):
        raise ValueError("jobs must be ordered by job id code point")
    if list(idempotency_raw) != sorted(idempotency_raw):
        raise ValueError("idempotency must be ordered by key code point")
    if list(events_raw) != sorted(events_raw):
        raise ValueError("events must be ordered by key code point")

    jobs: dict[str, dict[str, Any]] = {}
    for name, record in jobs_raw.items():
        if not _is_non_empty_string(name):
            raise ValueError("job ids must be non-empty strings")
        if not isinstance(record, dict) \
                or list(record.keys()) != list(_V2_JOB_FIELDS):
            raise ValueError("job record must have exactly job_id, work, "
                             "deadline, regions, residency, max_cost, "
                             "carbon_cap and state, in that order")
        if record["job_id"] != name:
            raise ValueError("job record id does not match its key")
        if not _is_plain_int(record["work"]) or record["work"] <= 0:
            raise ValueError("work must be a positive integer")
        if not _is_plain_int(record["deadline"]) or record["deadline"] < 0:
            raise ValueError("deadline must be a non-negative integer")
        if not _is_plain_int(record["max_cost"]) or record["max_cost"] < 0:
            raise ValueError("max_cost must be a non-negative integer")
        if not _is_plain_int(record["carbon_cap"]) \
                or record["carbon_cap"] < 0:
            raise ValueError("carbon_cap must be a non-negative integer")
        regions = _validate_v2_regions(record["regions"], "regions")
        residency = _validate_v2_regions(record["residency"], "residency")
        if not set(residency).issubset(set(regions)):
            raise ValueError("residency regions must be a subset of regions")
        if record["state"] != _V2_STATE:
            raise ValueError("invalid job state")
        jobs[name] = dict(record)

    idempotency: dict[str, str] = {}
    job_to_key: dict[str, str] = {}
    for key, job_id in idempotency_raw.items():
        if not _is_non_empty_string(key):
            raise ValueError("idempotency keys must be non-empty strings")
        if not _is_non_empty_string(job_id) or job_id not in jobs:
            raise ValueError("idempotency entry must reference an accepted "
                             "job")
        if job_id in job_to_key:
            raise ValueError("a job may be bound to at most one "
                             "idempotency key")
        job_to_key[job_id] = key
        idempotency[key] = job_id

    events: dict[str, dict[str, Any]] = {}
    for event_key, event in events_raw.items():
        if not _is_non_empty_string(event_key):
            raise ValueError("event keys must be non-empty strings")
        if event_key not in idempotency:
            raise ValueError("submit event must reference an idempotency "
                             "key")
        if not isinstance(event, dict) \
                or list(event.keys()) != list(_V2_EVENT_FIELDS):
            raise ValueError("submit events must have exactly type, "
                             "idempotency_key, job_id and result, in that "
                             "order")
        if event["type"] != _V2_EVENT_TYPE:
            raise ValueError("only submit audit events are supported")
        idem_key = event["idempotency_key"]
        job_id = event["job_id"]
        if idem_key != event_key:
            raise ValueError("submit event key must bind its idempotency "
                             "key")
        if not _is_non_empty_string(job_id) \
                or idempotency.get(idem_key) != job_id:
            raise ValueError("submit event must reference its idempotency "
                             "key's accepted job")
        if event["result"] != _V2_STATE:
            raise ValueError("submit event result must be queued")
        events[event_key] = dict(event)

    if set(events) != set(idempotency):
        raise ValueError("every first acceptance must bind exactly one "
                         "submit event to its idempotency key")

    return {"jobs": jobs, "idempotency": idempotency, "events": events}


def _load_v2(realpath: str, raw: bytes) -> dict[str, Any] | None:
    # A missing file is a brand-new registry. An existing file is decoded
    # strictly: bad UTF-8, non-finite or negative-zero literals and any
    # version, field, ordering or reference deviation are ValueError; a
    # version 1 file is rejected outright rather than upgraded.
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"acceptance file {realpath!r} is not valid UTF-8") from exc
    try:
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"acceptance file {realpath!r} is not valid JSON") from exc
    if not isinstance(data, dict) or "version" not in data:
        raise ValueError("acceptance file must be an object with a version")
    if data["version"] != _V2:
        raise ValueError("unsupported acceptance file version")
    return _validate_v2_structure(data)


def _read_v2(realpath: str) -> tuple[dict[str, Any] | None, bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return None, None
    return _load_v2(realpath, raw), raw


@contextlib.contextmanager
def _acceptance_lock(realpath: str, *, shared: bool = False) -> Iterator[None]:
    # Same flock discipline as the audit journals: a companion lock file
    # (never unlinked), exclusive for submit and shared for get, so
    # equivalent real paths serialize across processes; the kernel
    # releases the flock on process exit.
    lock_path = realpath + ".lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _serialize_v2(state: dict[str, Any]) -> bytes:
    jobs = state["jobs"]
    idempotency = state["idempotency"]
    events = state["events"]
    payload = {
        "version": _V2,
        "jobs": {name: jobs[name] for name in sorted(jobs)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
        "events": {key: events[key] for key in sorted(events)},
    }
    return (json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                       allow_nan=False) + "\n").encode("utf-8")


def _fsync_directory(directory: str) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _commit_v2(realpath: str, directory: str, payload: bytes,
               old_bytes: bytes | None) -> None:
    # One durable commit for the jobs, the idempotency map and the audit
    # events: same-directory temporary, fsync, atomic replace and a
    # directory fsync. Any failure after the replace restores the exact
    # pre-call bytes (or removes a file that did not exist) while the
    # exclusive lock is still held.
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".jobs-v2-", suffix=".tmp")
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
        try:
            if old_bytes is None:
                try:
                    os.unlink(realpath)
                except FileNotFoundError:
                    pass
            else:
                restore_fd, restore_path = tempfile.mkstemp(
                    dir=directory, prefix=".jobs-v2-restore-", suffix=".tmp")
                try:
                    with os.fdopen(restore_fd, "wb") as handle:
                        handle.write(old_bytes)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.replace(restore_path, realpath)
                except BaseException:
                    with contextlib.suppress(OSError):
                        os.unlink(restore_path)
                    raise
            _fsync_directory(directory)
        except OSError as recovery:
            raise recovery from first
        raise


def submit(
    path: str,
    job: dict[str, object],
    idempotency_key: str,
) -> tuple[dict[str, object], bool]:
    """Durably accept a complete scheduling-constraint job.

    ``path`` and ``idempotency_key`` must be non-empty strings and
    ``job`` an object with exactly seven top-level fields -- job_id,
    work, deadline, regions, residency, max_cost and carbon_cap.
    ``job_id`` is a non-empty string; ``work`` a positive integer;
    ``deadline``, ``max_cost`` and ``carbon_cap`` non-negative integers
    (booleans are never accepted); ``regions`` and ``residency`` are
    lists of distinct, non-empty strings, residency non-empty and a
    subset of regions. The regions are stored sorted by code point.

    Returns ``(record, created)``: the canonical record adds
    ``state: "queued"`` as its last field, and ``created`` is ``True``
    for a first acceptance. A repeat call with the same idempotency key
    and an equivalent job is a read-only replay that returns the stored
    record with ``False``: no event is added and the file bytes are not
    touched. The same key with a different job, the same job id under a
    different key, or inconsistent constraints raise ``ValueError`` and
    preserve the pre-call bytes.

    A first acceptance commits the job, the idempotency mapping and one
    ``submit`` audit event -- binding the idempotency key, the job id and
    the ``queued`` result -- in a single durable transaction. The file
    is compact UTF-8 JSON (non-ASCII written through, decimal integers,
    exactly one trailing newline). Equivalent real paths and multiple
    processes share one exclusive kernel flock. A missing parent
    directory raises ``FileNotFoundError``; bad arguments or an invalid
    existing file raise ``ValueError``; a version 1 ``register`` file is
    rejected rather than upgraded; any other locking or I/O failure
    raises ``OSError``.
    """
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")
    if not _is_non_empty_string(idempotency_key):
        raise ValueError("idempotency_key must be a non-empty string")
    job_id, work, deadline, regions, residency, max_cost, carbon_cap = \
        _normalize_v2_job(job)
    record = _v2_record(job_id, work, deadline, regions, residency,
                        max_cost, carbon_cap)

    realpath = os.path.realpath(path)
    store = _get_store(realpath)
    with store.lock:
        with _acceptance_lock(realpath):
            directory = os.path.dirname(realpath) or "."
            state, old_bytes = _read_v2(realpath)
            if state is None:
                state = {"jobs": {}, "idempotency": {}, "events": {}}
            jobs = state["jobs"]
            idempotency = state["idempotency"]
            events = state["events"]

            existing_id = idempotency.get(idempotency_key)
            if existing_id is not None:
                existing = jobs[existing_id]
                if existing != record:
                    raise ValueError("idempotency key was already used with "
                                     "a different job")
                return dict(existing), False

            if job_id in jobs:
                raise ValueError("job_id is already accepted under another "
                                 "idempotency key")

            jobs[job_id] = record
            idempotency[idempotency_key] = job_id
            events[idempotency_key] = {
                "type": _V2_EVENT_TYPE,
                "idempotency_key": idempotency_key,
                "job_id": job_id,
                "result": _V2_STATE,
            }
            _commit_v2(realpath, directory, _serialize_v2(state), old_bytes)
            return dict(record), True


def get(path: str, job_id: str) -> dict[str, object]:
    """Return a read-only copy of the accepted job under ``job_id``.

    ``path`` and ``job_id`` must be non-empty strings, else
    ``ValueError``. A missing file raises ``FileNotFoundError``; an
    invalid file (including a version 1 ``register`` file) raises
    ``ValueError``; an unknown job id raises ``KeyError(job_id)``; any
    other locking or I/O failure raises ``OSError``. The query never
    writes and the returned dict is a fresh copy.
    """
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")
    if not _is_non_empty_string(job_id):
        raise ValueError("job_id must be a non-empty string")

    realpath = os.path.realpath(path)
    store = _get_store(realpath)
    with store.lock:
        with _acceptance_lock(realpath, shared=True):
            state, _ = _read_v2(realpath)
            if state is None:
                raise FileNotFoundError(realpath)
            jobs = state["jobs"]
            if job_id not in jobs:
                raise KeyError(job_id)
            return dict(jobs[job_id])
