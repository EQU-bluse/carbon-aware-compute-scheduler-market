"""Persistent, thread-safe and multi-process job registries.

Two on-disk formats live side by side and are never converted into one
another:

* :func:`register` serves the original field-sparse registry (version
  1) with job_id, deadline, energy_wh, residency_regions and state.
* :func:`submit` / :func:`get` serve complete scheduling-constraint
  jobs (version 2) with job_id, work, deadline, regions, residency,
  max_cost, carbon_cap and state, an idempotency map and, for every
  first acceptance, a submit audit event.

A version-1 file handed to :func:`submit`/:func:`get` is an unsupported
version and raises ``ValueError`` with its bytes untouched, just as a
version-2 file handed to :func:`register` does; old ``jobs.register``
files are never upgraded.
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

# Version 2: complete scheduling-constraint jobs, the idempotency map
# and one submit audit event per first acceptance in a single document.
_SUBMIT_VERSION = 2
_SUBMIT_FIELDS = ("job_id", "work", "deadline", "regions", "residency",
                  "max_cost", "carbon_cap")
_SUBMIT_REQUIRED_FIELDS = frozenset(_SUBMIT_FIELDS)
_SUBMIT_JOB_FIELDS = _SUBMIT_FIELDS + ("state",)
_SUBMIT_ROOT_FIELDS = ("version", "jobs", "idempotency", "audit")
_SUBMIT_EVENT_FIELDS = ("key", "job_id", "result")
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


# ---------------------------------------------------------------------------
# Version 2: complete scheduling-constraint jobs
# ---------------------------------------------------------------------------


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


def _normalize_region_list(value: object, label: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{label} must be a non-empty list")
    normalized: list[str] = []
    seen: set[str] = set()
    for region in value:
        if not isinstance(region, str) or not region:
            raise ValueError(f"{label} must be non-empty strings")
        if region in seen:
            raise ValueError(f"{label} must be distinct")
        seen.add(region)
        normalized.append(region)
    normalized.sort()  # Python str ordering is Unicode code-point ordering.
    return normalized


def _normalize_submit_job(job: object) -> dict[str, Any]:
    if not isinstance(job, dict) or set(job.keys()) != _SUBMIT_REQUIRED_FIELDS:
        raise ValueError("job must be an object with exactly job_id, work, "
                         "deadline, regions, residency, max_cost and "
                         "carbon_cap")

    job_id = job["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")

    work = job["work"]
    if not _is_plain_int(work) or work <= 0:
        raise ValueError("work must be a non-boolean positive integer")

    for name in ("deadline", "max_cost", "carbon_cap"):
        value = job[name]
        if not _is_plain_int(value) or value < 0:
            raise ValueError(f"{name} must be a non-boolean non-negative "
                             "integer")

    regions = _normalize_region_list(job["regions"], "regions")
    residency = _normalize_region_list(job["residency"], "residency")
    if not set(residency).issubset(regions):
        raise ValueError("residency regions must be a subset of regions")

    return {
        "job_id": job_id,
        "work": work,
        "deadline": job["deadline"],
        "regions": regions,
        "residency": residency,
        "max_cost": job["max_cost"],
        "carbon_cap": job["carbon_cap"],
        "state": _STATE,
    }


def _validate_region_field(value: object, label: str) -> list[str]:
    normalized = _normalize_region_list(value, label)
    if value != normalized:
        raise ValueError(f"{label} must be sorted by code point and free "
                         "of duplicates")
    return normalized


def _check_sorted_keys(mapping: dict[Any, Any], label: str) -> None:
    keys = list(mapping)
    if keys != sorted(keys):
        raise ValueError(f"{label} must be ordered by key code point")


def _validate_submit_structure(data: object) -> tuple[
    dict[str, dict[str, Any]], dict[str, str], dict[str, dict[str, str]]
]:
    if not isinstance(data, dict) or set(data.keys()) != set(_SUBMIT_ROOT_FIELDS):
        raise ValueError("acceptance file root must be an object with keys "
                         "version, jobs, idempotency and audit")

    version = data["version"]
    if not _is_plain_int(version) or version != _SUBMIT_VERSION:
        raise ValueError("unsupported acceptance file version")

    jobs_raw = data["jobs"]
    idempotency_raw = data["idempotency"]
    audit_raw = data["audit"]
    if not isinstance(jobs_raw, dict) \
            or not isinstance(idempotency_raw, dict) \
            or not isinstance(audit_raw, dict):
        raise ValueError("jobs, idempotency and audit must be objects")
    _check_sorted_keys(jobs_raw, "jobs")
    _check_sorted_keys(idempotency_raw, "idempotency")
    _check_sorted_keys(audit_raw, "audit")

    jobs: dict[str, dict[str, Any]] = {}
    for name, record in jobs_raw.items():
        if not isinstance(name, str) or not name:
            raise ValueError("job ids must be non-empty strings")
        if not isinstance(record, dict) \
                or set(record.keys()) != set(_SUBMIT_JOB_FIELDS):
            raise ValueError("job record has invalid fields")
        if record["job_id"] != name or not isinstance(record["job_id"], str):
            raise ValueError("job record id does not match its key")
        if not _is_plain_int(record["work"]) or record["work"] <= 0:
            raise ValueError("invalid work")
        for field in ("deadline", "max_cost", "carbon_cap"):
            if not _is_plain_int(record[field]) or record[field] < 0:
                raise ValueError(f"invalid {field}")
        regions = _validate_region_field(record["regions"], "regions")
        residency = _validate_region_field(record["residency"], "residency")
        if not set(residency).issubset(regions):
            raise ValueError("residency regions must be a subset of regions")
        if record["state"] != _STATE:
            raise ValueError("invalid job state")
        jobs[name] = {field: record[field] for field in _SUBMIT_JOB_FIELDS}

    idempotency: dict[str, str] = {}
    referenced: set[str] = set()
    for key, job_id in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        if not isinstance(job_id, str) or not job_id or job_id not in jobs:
            raise ValueError("idempotency entry must reference an accepted "
                             "job")
        if job_id in referenced:
            raise ValueError("job accepted under more than one idempotency "
                             "key")
        referenced.add(job_id)
        idempotency[key] = job_id

    events: dict[str, dict[str, str]] = {}
    for key, event in audit_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event, dict) \
                or set(event.keys()) != set(_SUBMIT_EVENT_FIELDS):
            raise ValueError("submit audit event has invalid fields")
        event_key = event["key"]
        job_id = event["job_id"]
        result = event["result"]
        if event_key != key or not isinstance(event_key, str) or not event_key:
            raise ValueError("audit event key does not match its map key")
        if not isinstance(job_id, str) or not job_id or job_id not in jobs:
            raise ValueError("audit event must reference an accepted job")
        if result != _STATE:
            raise ValueError("audit event result must be queued")
        events[key] = {"key": event_key, "job_id": job_id,
                       "result": result}

    # The three sections describe one acceptance history: each
    # idempotency key binds one job and one submit event, every accepted
    # job has both, and the event and the map agree on the job.
    if set(idempotency) != set(events):
        raise ValueError("idempotency keys and audit events do not match")
    for key, job_id in idempotency.items():
        if events[key]["job_id"] != job_id:
            raise ValueError("audit event does not match its idempotency "
                             "entry")
    if set(jobs) != referenced:
        raise ValueError("every accepted job must be bound to an "
                         "idempotency key")

    return jobs, idempotency, events


def _load_submit_file(
    realpath: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, str],
           dict[str, dict[str, str]], bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return {}, {}, {}, None

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"acceptance file {realpath!r} is not valid UTF-8") from exc
    try:
        # Negative-zero and non-finite literals are format errors.
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"acceptance file {realpath!r} is not valid JSON") from exc
    jobs, idempotency, events = _validate_submit_structure(data)
    return jobs, idempotency, events, raw


def _serialize_submit_file(
    jobs: dict[str, dict[str, Any]],
    idempotency: dict[str, str],
    events: dict[str, dict[str, str]],
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, each section's
    # primary keys in code-point order, terminated by exactly one newline.
    payload = {
        "version": _SUBMIT_VERSION,
        "jobs": {name: jobs[name] for name in sorted(jobs)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
        "audit": {key: events[key] for key in sorted(events)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _rollback_submit_file(realpath: str, directory: str,
                          old_bytes: bytes | None,
                          first: BaseException) -> None:
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
                dir=directory, prefix=".jobs-restore-", suffix=".tmp")
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


def _fsync_directory(directory: str) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _commit_submit_file(realpath: str, payload: bytes,
                        old_bytes: bytes | None) -> None:
    # One durable commit for jobs, the idempotency map and the audit
    # event: same-directory temporary, fsync, atomic replace and a
    # directory fsync. Any failure after the replace restores the
    # pre-call bytes, so an unsuccessful submit leaves the original file
    # byte-for-byte.
    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".jobs-submit-", suffix=".tmp")
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
        _fsync_directory(directory)
    except BaseException as first:
        _rollback_submit_file(realpath, directory, old_bytes, first)
        raise


def submit(path: str, job: dict[str, object], idempotency_key: str
           ) -> tuple[dict[str, object], bool]:
    """Persistently accept a complete scheduling-constraint job.

    ``path`` and ``idempotency_key`` must be non-empty strings. The job
    must be an object with exactly the seven top-level fields job_id,
    work, deadline, regions, residency, max_cost and carbon_cap: a
    non-empty id, a positive work amount, non-negative deadline,
    max_cost and carbon_cap (booleans rejected), and two non-empty lists
    of distinct non-empty region strings, residency a subset of regions.
    Any deviation raises ``ValueError`` before the file is touched.

    A first acceptance saves the canonical job -- both region lists
    sorted by code point, state ``"queued"`` -- together with the
    idempotency binding and a submit audit event carrying the key, the
    job id and the queued result, all three in one durable commit, and
    returns ``(record, True)``. Replaying the same key with an
    equivalent job returns the stored record with ``False`` without
    adding an event or rewriting the file; the same key with a different
    job, an already accepted job id under another key, or any constraint
    mismatch raises ``ValueError`` and preserves the original bytes.

    A missing parent directory raises ``FileNotFoundError``; an invalid
    existing file (encoding, JSON, negative-zero or non-finite numbers,
    version, structure, ordering or references) raises ``ValueError``;
    any other locking or I/O failure raises ``OSError``. Calls serialize
    across threads and processes per resolved real path.
    """
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")
    if not isinstance(idempotency_key, str) or not idempotency_key:
        raise ValueError("idempotency_key must be a non-empty string")
    record = _normalize_submit_job(job)
    job_id = record["job_id"]

    store = _get_store(path)
    with store.lock:
        # Opening the companion lock in a missing directory surfaces as
        # FileNotFoundError before the data file is created.
        with _process_lock(store.realpath):
            jobs, idempotency, events, old_bytes = _load_submit_file(
                store.realpath)

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
                "key": idempotency_key,
                "job_id": job_id,
                "result": _STATE,
            }
            _commit_submit_file(
                store.realpath,
                _serialize_submit_file(jobs, idempotency, events), old_bytes)
            return dict(record), True


def get(path: str, job_id: str) -> dict[str, object]:
    """Return a read-only copy of the accepted job stored under ``job_id``.

    ``path`` and ``job_id`` must be non-empty strings, else
    ``ValueError``. A missing acceptance file raises
    ``FileNotFoundError``; an invalid file -- including a version-1
    ``jobs.register`` file, which is never upgraded -- raises
    ``ValueError``; an unknown job id raises ``KeyError(job_id)``; any
    other locking or I/O failure raises ``OSError``. The call never
    writes, and the returned record is a fresh copy with job_id, work,
    deadline, regions, residency, max_cost, carbon_cap and state.
    """
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")

    realpath = os.path.realpath(path)
    with _process_lock(realpath, shared=True):
        jobs, _idempotency, _events, raw = _load_submit_file(realpath)
    if raw is None:
        raise FileNotFoundError(f"acceptance file {realpath!r} does not exist")
    if job_id not in jobs:
        raise KeyError(job_id)
    return dict(jobs[job_id])
