"""Persistent, thread-safe job-to-offer matching ledger."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
import threading
from typing import Any

from ._jsonio import strict_loads

__all__ = ["match"]

_VERSION = 1
_MATCH_FIELDS = ("job_id", "resource_id")
_REQUIRED_FIELDS = frozenset(_MATCH_FIELDS)
_ROOT_FIELDS = ("version", "matches", "idempotency")


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


def _validate_offer_record(record: object) -> dict[str, Any]:
    if not isinstance(record, dict) or set(record.keys()) != {
            "resource_id", "region", "capacity_wh", "unit_cost",
            "carbon_intensity"}:
        raise ValueError("offer record has invalid fields")
    resource_id = record["resource_id"]
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("resource_id must be a non-empty string")
    region = record["region"]
    if not isinstance(region, str) or not region:
        raise ValueError("region must be a non-empty string")
    if not _is_plain_int(record["capacity_wh"]) or record["capacity_wh"] <= 0:
        raise ValueError("invalid capacity_wh")
    if not _is_plain_int(record["unit_cost"]) or record["unit_cost"] < 0:
        raise ValueError("invalid unit_cost")
    if (not _is_plain_int(record["carbon_intensity"])
            or record["carbon_intensity"] < 0):
        raise ValueError("invalid carbon_intensity")
    return dict(record)


def _load_offers(realpath: str) -> dict[str, dict[str, Any]]:
    with open(realpath, encoding="utf-8") as handle:
        text = handle.read()
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(f"offers file {realpath!r} is not valid JSON") from exc

    if not isinstance(data, dict) or set(data.keys()) != {
            "version", "offers", "idempotency"}:
        raise ValueError("offers registry root has an invalid structure")
    if not _is_plain_int(data["version"]) or data["version"] != _VERSION:
        raise ValueError("unsupported offers registry version")
    offers_raw, idempotency_raw = data["offers"], data["idempotency"]
    if not isinstance(offers_raw, dict) or not isinstance(idempotency_raw, dict):
        raise ValueError("offers and idempotency must be objects")

    offers: dict[str, dict[str, Any]] = {}
    for name, record in offers_raw.items():
        if not isinstance(name, str) or not name:
            raise ValueError("resource ids must be non-empty strings")
        normalized = _validate_offer_record(record)
        if normalized["resource_id"] != name:
            raise ValueError("offer record id does not match its key")
        offers[name] = normalized

    referenced: set[str] = set()
    for key, entry in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        if not isinstance(entry, dict) or set(entry.keys()) != {"resource_id"}:
            raise ValueError("offers idempotency entry must be a "
                             "resource_id object")
        resource_id = entry["resource_id"]
        if (not isinstance(resource_id, str) or not resource_id
                or resource_id not in offers):
            raise ValueError("offers idempotency entry must reference a "
                             "registered offer")
        if resource_id in referenced:
            raise ValueError("offer registered under more than one "
                             "idempotency key")
        referenced.add(resource_id)

    return offers


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


def _load_jobs(realpath: str) -> dict[str, dict[str, Any]]:
    with open(realpath, encoding="utf-8") as handle:
        text = handle.read()
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(f"jobs file {realpath!r} is not valid JSON") from exc

    if not isinstance(data, dict) or set(data.keys()) != {
            "version", "jobs", "idempotency"}:
        raise ValueError("jobs registry root has an invalid structure")
    if not _is_plain_int(data["version"]) or data["version"] != _VERSION:
        raise ValueError("unsupported jobs registry version")
    jobs_raw, idempotency_raw = data["jobs"], data["idempotency"]
    if not isinstance(jobs_raw, dict) or not isinstance(idempotency_raw, dict):
        raise ValueError("jobs and idempotency must be objects")

    jobs: dict[str, dict[str, Any]] = {}
    for name, record in jobs_raw.items():
        if not isinstance(name, str) or not name:
            raise ValueError("job ids must be non-empty strings")
        if not isinstance(record, dict) or set(record.keys()) != {
                "job_id", "deadline", "energy_wh", "residency_regions",
                "state"}:
            raise ValueError("job record has invalid fields")
        if record["job_id"] != name or not isinstance(record["job_id"], str):
            raise ValueError("job record id does not match its key")
        if not _is_plain_int(record["deadline"]) or record["deadline"] < 0:
            raise ValueError("invalid deadline")
        if not _is_plain_int(record["energy_wh"]) or record["energy_wh"] <= 0:
            raise ValueError("invalid energy_wh")
        _validate_regions(record["residency_regions"])
        if record["state"] != "queued":
            raise ValueError("invalid job state")
        jobs[name] = dict(record)

    referenced: set[str] = set()
    for key, job_id in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        if not isinstance(job_id, str) or not job_id or job_id not in jobs:
            raise ValueError("jobs idempotency entry must reference a "
                             "registered job")
        if job_id in referenced:
            raise ValueError("job registered under more than one "
                             "idempotency key")
        referenced.add(job_id)

    return jobs


def _validate_structure(
    data: object,
) -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("ledger root must be an object with keys "
                         "version, matches and idempotency")
    if not _is_plain_int(data["version"]) or data["version"] != _VERSION:
        raise ValueError("unsupported ledger version")

    matches_raw = data["matches"]
    idempotency_raw = data["idempotency"]
    if not isinstance(matches_raw, dict) or not isinstance(idempotency_raw, dict):
        raise ValueError("matches and idempotency must be objects")

    matches: dict[str, dict[str, str]] = {}
    for job_id, record in matches_raw.items():
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("match keys must be non-empty job ids")
        if not isinstance(record, dict) or set(record.keys()) != _REQUIRED_FIELDS:
            raise ValueError("match record must have exactly job_id and "
                             "resource_id")
        record_job_id = record["job_id"]
        resource_id = record["resource_id"]
        if record_job_id != job_id or not isinstance(record_job_id, str):
            raise ValueError("match record job_id does not match its key")
        if not isinstance(resource_id, str) or not resource_id:
            raise ValueError("resource_id must be a non-empty string")
        matches[job_id] = {"job_id": record_job_id,
                           "resource_id": resource_id}

    idempotency: dict[str, str] = {}
    referenced: set[str] = set()
    for key, job_id in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        if not isinstance(job_id, str) or not job_id or job_id not in matches:
            raise ValueError("idempotency entry must reference a recorded "
                             "match")
        if job_id in referenced:
            raise ValueError("job matched under more than one idempotency key")
        referenced.add(job_id)
        idempotency[key] = job_id

    return matches, idempotency


def _load_ledger(
    realpath: str,
) -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    try:
        with open(realpath, encoding="utf-8") as handle:
            text = handle.read()
    except FileNotFoundError:
        return {}, {}

    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(f"ledger file {realpath!r} is not valid JSON") from exc
    return _validate_structure(data)


def _atomic_write(
    realpath: str,
    matches: dict[str, dict[str, str]],
    idempotency: dict[str, str],
) -> None:
    payload = {
        "version": _VERSION,
        "matches": {job_id: matches[job_id] for job_id in sorted(matches)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"

    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".ledger-",
                                    suffix=".tmp")
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


def match(jobs: str, offers: str, ledger: str, job_id: str,
          key: str) -> tuple[dict[str, object], bool]:
    """Match a queued job against capacity offers, idempotently.

    A candidate offer must serve one of the job's residency regions and
    still have enough capacity after subtracting the energy already
    allocated to earlier matches on it; the candidate with the lowest
    ``(carbon_intensity, unit_cost, resource_id)`` triple wins. Returns
    ``(record, created)`` with ``record`` keyed by job_id and resource_id.
    The ledger is compact UTF-8 JSON, atomically replaced.
    """
    for name, value in (("jobs", jobs), ("offers", offers),
                        ("ledger", ledger)):
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} path must be a non-empty string")
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a non-empty string")

    jobs_realpath = os.path.realpath(jobs)
    offers_realpath = os.path.realpath(offers)
    ledger_realpath = os.path.realpath(ledger)

    store = _get_store(ledger)
    with store.lock:
        job_records = _load_jobs(jobs_realpath)
        job = job_records.get(job_id)
        if job is None:
            raise KeyError(job_id)

        offer_records = _load_offers(offers_realpath)
        matches, idempotency = _load_ledger(ledger_realpath)

        replayed_job_id = idempotency.get(key)
        if replayed_job_id is not None:
            if replayed_job_id != job_id:
                raise ValueError("idempotency key was already used for a "
                                 "different job")
            # A replay does not consume any capacity.
            return dict(matches[replayed_job_id]), False

        if job_id in matches:
            raise ValueError("job is already matched under another "
                             "idempotency key")

        # Tally energy already allocated per resource from earlier matches.
        # A ledger referencing a job or offer absent from the registries is a
        # dangling reference.
        used: dict[str, int] = {}
        for entry in matches.values():
            matched_job = job_records.get(entry["job_id"])
            if matched_job is None:
                raise ValueError("ledger references an unknown job")
            rid = entry["resource_id"]
            if rid not in offer_records:
                raise ValueError("ledger references an unknown offer")
            used[rid] = used.get(rid, 0) + matched_job["energy_wh"]

        allowed = set(job["residency_regions"])
        energy_wh = job["energy_wh"]
        candidates = [
            record for record in offer_records.values()
            if record["region"] in allowed
            and record["capacity_wh"] - used.get(record["resource_id"], 0)
            >= energy_wh
        ]
        if not candidates:
            raise LookupError("no feasible offer for the job")

        chosen = min(candidates, key=lambda record: (
            record["carbon_intensity"], record["unit_cost"],
            record["resource_id"]))

        record: dict[str, object] = {
            "job_id": job_id,
            "resource_id": chosen["resource_id"],
        }
        matches[job_id] = record
        idempotency[key] = job_id
        _atomic_write(ledger_realpath, matches, idempotency)
        return dict(record), True
