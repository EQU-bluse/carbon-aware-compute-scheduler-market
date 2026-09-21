"""Persistent, thread-safe reserve/cancel event journal.

A reserve event occupies a matched job's energy on the matched offer; a
cancel event releases it again. Each job may be reserved once and, once
reserved, cancelled once. The journal is append-only in spirit: events are
never removed or rewritten, and an idempotency key replays the event it
first recorded.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
import threading
from typing import Any

from . import jobs as _jobs
from . import offers as _offers
from .market import _load_registry, _validate_ledger
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


def _load_matches(
    realpath: str,
    job_records: dict[str, dict[str, Any]],
    offer_records: dict[str, dict[str, Any]],
) -> dict[str, dict[str, str]]:
    # The match ledger is a required input, so a missing file surfaces as
    # FileNotFoundError (unlike the journal, which is created on demand).
    with open(realpath, encoding="utf-8") as handle:
        text = handle.read()
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"match ledger {realpath!r} is not valid JSON") from exc
    matches, _ = _validate_ledger(data, job_records, offer_records)
    return matches


def _validate_journal(
    data: object,
    job_records: dict[str, dict[str, Any]],
    offer_records: dict[str, dict[str, Any]],
    match_records: dict[str, dict[str, str]],
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("journal root must be an object with keys "
                         "version and events")

    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported journal version")

    events_raw = data["events"]
    if not isinstance(events_raw, dict):
        raise ValueError("events must be an object")

    # Keys are sorted for the persisted form, but they carry no temporal
    # meaning: a key added later may sort first. Rebuild each job's
    # reserve/cancel status from op counts instead of traversal order.
    events: dict[str, dict[str, Any]] = {}
    reserved_jobs: set[str] = set()
    cancelled_jobs: set[str] = set()
    for event_key in sorted(events_raw):
        if not isinstance(event_key, str) or not event_key:
            raise ValueError("idempotency keys must be non-empty strings")
        event = events_raw[event_key]
        if not isinstance(event, dict) or set(event.keys()) != set(_EVENT_FIELDS):
            raise ValueError("event must be an object with exactly job_id, "
                             "resource_id, op and now")
        job_id = event["job_id"]
        resource_id = event["resource_id"]
        op = event["op"]
        now = event["now"]
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("job_id must be a non-empty string")
        if not isinstance(resource_id, str) or not resource_id:
            raise ValueError("resource_id must be a non-empty string")
        if op not in _OPS:
            raise ValueError("op must be reserve or cancel")
        if not _is_plain_int(now) or now < 0:
            raise ValueError("now must be a non-boolean non-negative integer")
        if job_id not in job_records:
            raise ValueError("event must reference a registered job")
        if resource_id not in offer_records:
            raise ValueError("event must reference a registered offer")
        match = match_records.get(job_id)
        if match is None or match["resource_id"] != resource_id:
            raise ValueError("event must reference the matched resource")

        if op == "reserve":
            if job_id in reserved_jobs:
                raise ValueError("job reserved more than once")
            if now > job_records[job_id]["deadline"]:
                raise ValueError("reserve event later than the job deadline")
            reserved_jobs.add(job_id)
        else:
            if job_id in cancelled_jobs:
                raise ValueError("job cancelled more than once")
            cancelled_jobs.add(job_id)

        events[event_key] = {field: event[field] for field in _EVENT_FIELDS}

    if not cancelled_jobs <= reserved_jobs:
        raise ValueError("cancel must follow a reserve")

    lifecycle = {
        job_id: ("cancelled" if job_id in cancelled_jobs else "reserved")
        for job_id in reserved_jobs
    }

    return events, lifecycle


def _load_journal(
    realpath: str,
    job_records: dict[str, dict[str, Any]],
    offer_records: dict[str, dict[str, Any]],
    match_records: dict[str, dict[str, str]],
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    try:
        with open(realpath, encoding="utf-8") as handle:
            text = handle.read()
    except FileNotFoundError:
        return {}, {}

    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(f"journal file {realpath!r} is not valid JSON") from exc
    return _validate_journal(
        data, job_records, offer_records, match_records)


def _atomic_write(realpath: str, events: dict[str, dict[str, Any]]) -> None:
    payload = {
        "version": _VERSION,
        "events": {key: events[key] for key in sorted(events)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"

    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".reserve-",
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
    """Reserve or cancel a matched job's energy idempotently.

    Returns ``(event, created)`` where ``event`` carries job_id,
    resource_id, op and now; ``created`` is ``True`` for a new event and
    ``False`` when the idempotency key replays the identical event. A
    reserve requires the job to be matched, still open (``now`` not past
    its deadline) and room for its energy on the matched resource among
    the active reservations; a cancel releases that energy. A job may be
    reserved once and then cancelled once.
    """
    for value in (jobs, offers, matches, state, job_id, op, key):
        if not isinstance(value, str) or not value:
            raise ValueError("string arguments must be non-empty strings")
    if op not in _OPS:
        raise ValueError("op must be reserve or cancel")
    if not _is_plain_int(now) or now < 0:
        raise ValueError("now must be a non-boolean non-negative integer")

    store = _get_store(state)
    with store.lock:
        jobs_real = os.path.realpath(jobs)
        offers_real = os.path.realpath(offers)
        matches_real = os.path.realpath(matches)

        job_records, _ = _load_registry(
            jobs_real, _jobs._validate_structure, "jobs registry")
        if job_id not in job_records:
            raise KeyError(job_id)
        job = job_records[job_id]

        offer_records, _ = _load_registry(
            offers_real, _offers._validate_structure, "offers registry")
        match_records = _load_matches(
            matches_real, job_records, offer_records)
        events, lifecycle = _load_journal(
            store.realpath, job_records, offer_records, match_records)

        existing = events.get(key)
        if existing is not None:
            # Validation already ties the stored event's resource to the
            # job's match, so job/op/now fully identify the replay.
            if (existing["job_id"] != job_id
                    or existing["op"] != op
                    or existing["now"] != now):
                raise ValueError("idempotency key was already used with a "
                                 "different event")
            return dict(existing), False

        matched = match_records.get(job_id)
        if matched is None:
            raise LookupError("job is not matched")
        resource_id = matched["resource_id"]

        status = lifecycle.get(job_id)
        if op == "reserve":
            if status is not None:
                raise ValueError("job can only be reserved once")
            if now > job["deadline"]:
                raise TimeoutError("reserve request is past the job deadline")
        elif status != "reserved":
            raise ValueError("job must be reserved before it is cancelled")

        if op == "reserve":
            usage: dict[str, int] = {}
            for event in events.values():
                if event["op"] != "reserve":
                    continue
                if lifecycle[event["job_id"]] != "reserved":
                    continue
                rid = event["resource_id"]
                usage[rid] = (
                    usage.get(rid, 0)
                    + job_records[event["job_id"]]["energy_wh"])
            if usage.get(resource_id, 0) + job["energy_wh"] \
                    > offer_records[resource_id]["capacity_wh"]:
                raise LookupError("matched offer has no remaining capacity "
                                  "for the job")

        event: dict[str, object] = {
            "job_id": job_id,
            "resource_id": resource_id,
            "op": op,
            "now": now,
        }
        events[key] = event
        _atomic_write(store.realpath, events)
        return dict(event), True
