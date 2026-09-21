"""Persistent, thread-safe reservation state log."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
import threading
from typing import Any

from . import jobs as _jobs
from . import market as _market
from . import offers as _offers
from ._jsonio import strict_loads

__all__ = ["run"]

_VERSION = 1
_OPS = ("reserve", "cancel")
_EVENT_FIELDS = ("job_id", "resource_id", "op", "now")
_ROOT_FIELDS = ("version", "events")


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


def _validate_state(
    data: object,
    job_ids: dict[str, Any],
    resource_ids: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("state root must be an object with keys "
                         "version and events")

    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported state version")

    events_raw = data["events"]
    if not isinstance(events_raw, dict):
        raise ValueError("events must be an object")

    events: dict[str, dict[str, Any]] = {}
    reserved: set[str] = set()
    cancelled: set[str] = set()
    for key, event in events_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        if not isinstance(event, dict) or set(event.keys()) != set(_EVENT_FIELDS):
            raise ValueError("event record has invalid fields")
        job_id = event["job_id"]
        if not isinstance(job_id, str) or not job_id or job_id not in job_ids:
            raise ValueError("event must reference a registered job")
        resource_id = event["resource_id"]
        if not isinstance(resource_id, str) or not resource_id \
                or resource_id not in resource_ids:
            raise ValueError("event must reference a registered offer")
        op = event["op"]
        if op not in _OPS:
            raise ValueError("event op must be reserve or cancel")
        now = event["now"]
        if not _is_plain_int(now) or now < 0:
            raise ValueError("event now must be a non-boolean "
                             "non-negative integer")
        # A job may reserve at most once and then cancel at most once; the
        # cancel is only meaningful after a reserve. The file stores events
        # keyed by idempotency key, so only the set relation is checked.
        if op == "reserve":
            if job_id in reserved:
                raise ValueError("job reserved more than once")
            reserved.add(job_id)
        else:
            if job_id in cancelled:
                raise ValueError("job cancelled more than once")
            cancelled.add(job_id)
        # Rebuild in canonical field order so that rewrites of events stored
        # out of order still follow _EVENT_FIELDS.
        events[key] = {field: event[field] for field in _EVENT_FIELDS}
    if not cancelled <= reserved:
        raise ValueError("job cancelled without a prior reserve")
    return events


def _load_state(
    realpath: str,
    job_ids: dict[str, Any],
    resource_ids: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    try:
        with open(realpath, encoding="utf-8") as handle:
            text = handle.read()
    except FileNotFoundError:
        return {}

    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(f"state file {realpath!r} is not valid JSON") from exc
    return _validate_state(data, job_ids, resource_ids)


def _atomic_write(
    realpath: str,
    events: dict[str, dict[str, Any]],
) -> None:
    payload = {
        "version": _VERSION,
        "events": {key: events[key] for key in sorted(events)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"

    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".state-",
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


def run(
    jobs: str,
    offers: str,
    matches: str,
    state: str,
    job_id: str,
    op: str,
    key: str,
    now: int,
) -> tuple[dict[str, object], bool]:
    """Reserve or release matched capacity for a job idempotently.

    ``op`` is ``"reserve"`` or ``"cancel"``; a job may reserve once and then
    cancel once. Reserving requires the job to be matched in the ``matches``
    ledger, the matched offer to have enough remaining capacity (active
    reservations occupy their job's energy_wh against the matched resource)
    and ``now`` to not exceed the job's deadline. Returns ``(event, created)``
    where ``event`` carries job_id, resource_id, op and now; ``created`` is
    ``True`` for a new event and ``False`` when the idempotency key replays
    an identical event.
    """
    for value in (jobs, offers, matches, state, job_id, op, key):
        if not isinstance(value, str) or not value:
            raise ValueError("jobs, offers, matches, state, job_id, op and "
                             "key must be non-empty strings")
    if op not in _OPS:
        raise ValueError("op must be reserve or cancel")
    if not _is_plain_int(now) or now < 0:
        raise ValueError("now must be a non-boolean non-negative integer")

    store = _get_store(state)
    with store.lock:
        job_records, _ = _market._load_registry(
            os.path.realpath(jobs), _jobs._validate_structure, "jobs registry")
        if job_id not in job_records:
            raise KeyError(job_id)
        job = job_records[job_id]

        offer_records, _ = _market._load_registry(
            os.path.realpath(offers), _offers._validate_structure,
            "offers registry")
        match_records, _ = _market._load_registry(
            os.path.realpath(matches),
            lambda data: _market._validate_ledger(data, job_records,
                                                  offer_records),
            "matches ledger")
        if job_id not in match_records:
            raise LookupError("job is not matched")
        resource_id = match_records[job_id]["resource_id"]

        events = _load_state(store.realpath, job_records, offer_records)

        event: dict[str, object] = {
            "job_id": job_id,
            "resource_id": resource_id,
            "op": op,
            "now": now,
        }

        existing = events.get(key)
        if existing is not None:
            if existing != event:
                raise ValueError("idempotency key was already used with a "
                                 "different event")
            return dict(existing), False

        reserved = {entry["job_id"] for entry in events.values()
                    if entry["op"] == "reserve"}
        cancelled = {entry["job_id"] for entry in events.values()
                     if entry["op"] == "cancel"}
        if op == "reserve":
            if job_id in reserved:
                raise ValueError("job is already reserved")
            allocated: dict[str, int] = {}
            for entry in events.values():
                if entry["op"] == "reserve" \
                        and entry["job_id"] not in cancelled:
                    active_id = entry["job_id"]
                    allocated[entry["resource_id"]] = (
                        allocated.get(entry["resource_id"], 0)
                        + job_records[active_id]["energy_wh"])
            capacity = offer_records[resource_id]["capacity_wh"]
            if allocated.get(resource_id, 0) + job["energy_wh"] > capacity:
                raise LookupError("matched offer lacks the capacity for "
                                  "this reservation")
            if now > job["deadline"]:
                raise TimeoutError("job deadline has passed")
        else:
            if job_id not in reserved or job_id in cancelled:
                raise ValueError("job has no active reservation to cancel")

        events[key] = event
        _atomic_write(store.realpath, events)
        return dict(event), True
