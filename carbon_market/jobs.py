"""Persistent, thread-safe job registry."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
import threading
from typing import Any

from ._json import StrictJSONError, loads as _json_loads

__all__ = ["register"]

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
        data = _json_loads(text)
    except (StrictJSONError, UnicodeDecodeError) as exc:
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

    with contextlib.suppress(OSError):
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
