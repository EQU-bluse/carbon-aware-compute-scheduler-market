"""Persistent, thread-safe job registration."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
import threading
from typing import Any

_JOB_FIELDS = ("job_id", "deadline", "energy_wh", "residency_regions")
_ENTRY_FIELDS = _JOB_FIELDS + ("state",)
_STORE_FIELDS = ("version", "jobs", "idempotency")
_STATE = "queued"
_VERSION = 1

_locks_guard = threading.Lock()
_path_locks: dict[str, threading.Lock] = {}


def register(
    path: str, job: dict[str, object], idempotency_key: str
) -> tuple[dict[str, object], bool]:
    """Register a job persistently.

    Returns the stored entry and ``True`` for a new registration, or the
    previously stored entry and ``False`` when the idempotency key is replayed
    with the identical, normalized job.
    """
    canonical = _validate_path(path)
    _validate_nonempty_str(idempotency_key, "idempotency_key")
    normalized = _normalize_job(job)

    lock = _lock_for(canonical)
    with lock:
        try:
            with open(canonical, encoding="utf-8") as handle:
                raw = json.load(handle)
        except FileNotFoundError:
            parent = os.path.dirname(canonical)
            if not os.path.isdir(parent):
                raise
            jobs: dict[str, dict[str, Any]] = {}
            idempotency: dict[str, str] = {}
        else:
            jobs, idempotency = _parse_store(raw)

        existing_id = idempotency.get(idempotency_key)
        if existing_id is not None:
            existing = jobs[existing_id]
            if all(existing[field] == normalized[field] for field in _JOB_FIELDS):
                return _copy_entry(existing), False
            raise ValueError(
                f"idempotency key {idempotency_key!r} is already registered "
                "with a different job"
            )

        job_id = normalized["job_id"]
        if job_id in jobs:
            raise ValueError(
                f"job_id {job_id!r} is already registered under another "
                "idempotency key"
            )

        entry: dict[str, Any] = {field: normalized[field] for field in _JOB_FIELDS}
        entry["state"] = _STATE
        jobs[job_id] = entry
        idempotency[idempotency_key] = job_id
        _write_store(canonical, jobs, idempotency)
        return _copy_entry(entry), True


def _validate_path(path: str) -> str:
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")
    return os.path.realpath(path)


def _validate_nonempty_str(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _normalize_job(job: object) -> dict[str, Any]:
    if not isinstance(job, dict):
        raise ValueError("job must be a JSON object")
    if set(job) != set(_JOB_FIELDS):
        raise ValueError(
            f"job must contain exactly the fields {list(_JOB_FIELDS)}"
        )
    return _normalize_fields(job)


def _normalize_fields(values: dict[str, object]) -> dict[str, Any]:
    job_id = _validate_nonempty_str(values["job_id"], "job_id")

    deadline = values["deadline"]
    if isinstance(deadline, bool) or not isinstance(deadline, int) or deadline < 0:
        raise ValueError("deadline must be a non-boolean non-negative integer")

    energy = values["energy_wh"]
    if isinstance(energy, bool) or not isinstance(energy, int) or energy <= 0:
        raise ValueError("energy_wh must be a non-boolean positive integer")

    regions = values["residency_regions"]
    if not isinstance(regions, list) or not regions:
        raise ValueError("residency_regions must be a non-empty list")
    seen: set[str] = set()
    for region in regions:
        _validate_nonempty_str(region, "residency_regions element")
        if region in seen:
            raise ValueError(f"residency_regions contains duplicate {region!r}")
        seen.add(region)

    return {
        "job_id": job_id,
        "deadline": deadline,
        "energy_wh": energy,
        "residency_regions": sorted(regions),
    }


def _parse_store(raw: object) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    if not isinstance(raw, dict):
        raise ValueError("job store root must be a JSON object")
    if set(raw) != set(_STORE_FIELDS):
        raise ValueError(
            f"job store must contain exactly the fields {list(_STORE_FIELDS)}"
        )

    version = raw["version"]
    if isinstance(version, bool) or not isinstance(version, int):
        raise ValueError("version must be an integer")
    if version != _VERSION:
        raise ValueError(f"unsupported job store version: {version}")

    raw_jobs = raw["jobs"]
    raw_idempotency = raw["idempotency"]
    if not isinstance(raw_jobs, dict) or not isinstance(raw_idempotency, dict):
        raise ValueError("jobs and idempotency must be JSON objects")

    jobs: dict[str, dict[str, Any]] = {}
    for job_id, stored in raw_jobs.items():
        _validate_nonempty_str(job_id, "job id key")
        if not isinstance(stored, dict):
            raise ValueError(f"job entry for {job_id!r} must be an object")
        if set(stored) != set(_ENTRY_FIELDS):
            raise ValueError(
                f"job entry for {job_id!r} must contain exactly "
                f"the fields {list(_ENTRY_FIELDS)}"
            )
        if stored["state"] != _STATE:
            raise ValueError(f"job entry for {job_id!r} has an unknown state")
        fields = _normalize_fields(stored)
        if fields["job_id"] != job_id:
            raise ValueError(
                f"job entry key {job_id!r} does not match its job_id "
                f"{fields['job_id']!r}"
            )
        jobs[job_id] = {field: fields[field] for field in _JOB_FIELDS}
        jobs[job_id]["state"] = stored["state"]

    idempotency: dict[str, str] = {}
    for key, value in raw_idempotency.items():
        _validate_nonempty_str(key, "idempotency key")
        _validate_nonempty_str(value, "idempotency value")
        if value not in jobs:
            raise ValueError(
                f"idempotency key {key!r} references unknown job {value!r}"
            )
        idempotency[key] = value

    return jobs, idempotency


def _write_store(
    path: str, jobs: dict[str, dict[str, Any]], idempotency: dict[str, str]
) -> None:
    payload = {
        "version": _VERSION,
        "jobs": {key: jobs[key] for key in sorted(jobs)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
    }
    text = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )
    _atomic_write(path, (text + "\n").encode("utf-8"))


def _atomic_write(path: str, data: bytes) -> None:
    directory = os.path.dirname(path) or "."
    fd, temporary = tempfile.mkstemp(dir=directory, prefix=".jobs-", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary)
        raise
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _copy_entry(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        field: (list(entry[field]) if field == "residency_regions" else entry[field])
        for field in _ENTRY_FIELDS
    }


def _lock_for(canonical: str) -> threading.Lock:
    with _locks_guard:
        lock = _path_locks.get(canonical)
        if lock is None:
            lock = threading.Lock()
            _path_locks[canonical] = lock
        return lock
