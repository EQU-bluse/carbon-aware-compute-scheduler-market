"""Persistent, thread-safe job migration journal.

A migration moves an actively reserved, matched job from its matched
resource to another capacity offer in two phases. A ``prepare`` event
keeps the job's energy on its source (its matched resource) and locks
the target by placing a pending placeholder there; a ``commit`` event
then releases the source and lands the energy on the target, while an
``abort`` event releases the target lock and leaves the energy on the
source. Each job migrates at most once: one prepare followed by at most
one commit or abort. The journal is append-only in spirit: events are
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
from . import reserve as _reserve
from .market import _load_registry, _validate_ledger
from ._jsonio import strict_loads

__all__ = ["run"]

_VERSION = 1
_OPS = ("prepare", "commit", "abort")
_EVENT_FIELDS = ("job_id", "source_id", "target_id", "op", "now")
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


def _load_reserves(
    realpath: str,
    job_records: dict[str, dict[str, Any]],
    offer_records: dict[str, dict[str, Any]],
    match_records: dict[str, dict[str, str]],
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    # The reserve journal is a required input as well; FileNotFoundError
    # (and any other OSError) propagates instead of being treated as empty.
    with open(realpath, encoding="utf-8") as handle:
        text = handle.read()
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"reserve journal {realpath!r} is not valid JSON") from exc
    return _reserve._validate_journal(
        data, job_records, offer_records, match_records)


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
    # meaning: a key added later may sort first (e.g. an abort key before
    # its prepare key). Gather every job's op set first and only then
    # verify the prepare/commit-or-abort sequence.
    events: dict[str, dict[str, Any]] = {}
    prepared: dict[str, dict[str, Any]] = {}
    terminal: dict[str, dict[str, Any]] = {}
    for event_key in sorted(events_raw):
        if not isinstance(event_key, str) or not event_key:
            raise ValueError("idempotency keys must be non-empty strings")
        event = events_raw[event_key]
        if not isinstance(event, dict) or set(event.keys()) != set(_EVENT_FIELDS):
            raise ValueError("event must be an object with exactly job_id, "
                             "source_id, target_id, op and now")
        job_id = event["job_id"]
        source_id = event["source_id"]
        target_id = event["target_id"]
        op = event["op"]
        now = event["now"]
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("job_id must be a non-empty string")
        if not isinstance(source_id, str) or not source_id:
            raise ValueError("source_id must be a non-empty string")
        if not isinstance(target_id, str) or not target_id:
            raise ValueError("target_id must be a non-empty string")
        if op not in _OPS:
            raise ValueError("op must be prepare, commit or abort")
        if not _is_plain_int(now) or now < 0:
            raise ValueError("now must be a non-boolean non-negative integer")
        if job_id not in job_records:
            raise ValueError("event must reference a registered job")
        if target_id not in offer_records:
            raise ValueError("event must reference a registered offer")
        match = match_records.get(job_id)
        if match is None or match["resource_id"] != source_id:
            raise ValueError("event source must be the matched resource")
        if source_id == target_id:
            raise ValueError("migration target must differ from the source")
        if (offer_records[target_id]["region"]
                not in job_records[job_id]["residency_regions"]):
            raise ValueError("migration target region is not permitted by "
                             "the job residency_regions")

        if op == "prepare":
            if job_id in prepared:
                raise ValueError("job prepared for migration more than once")
            if now > job_records[job_id]["deadline"]:
                raise ValueError("prepare event later than the job deadline")
            prepared[job_id] = event
        else:
            if job_id in terminal:
                raise ValueError("migration finished more than once")
            terminal[job_id] = event

        events[event_key] = {field: event[field] for field in _EVENT_FIELDS}

    status: dict[str, str] = {}
    for job_id, prepare_event in prepared.items():
        status[job_id] = "prepared"
    for job_id, end_event in terminal.items():
        prepare_event = prepared.get(job_id)
        if prepare_event is None:
            raise ValueError("commit or abort must follow a prepare")
        if prepare_event["target_id"] != end_event["target_id"]:
            raise ValueError("commit or abort must target the prepared "
                             "resource")
        status[job_id] = ("committed" if end_event["op"] == "commit"
                          else "aborted")

    return events, status


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
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".migrate-",
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

    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def run(
    jobs: str,
    offers: str,
    matches: str,
    reserves: str,
    state: str,
    job_id: str,
    target_id: str,
    op: str,
    key: str,
    now: int,
) -> tuple[dict[str, object], bool]:
    """Migrate a matched, actively reserved job idempotently.

    Returns ``(event, created)`` where ``event`` carries job_id,
    source_id, target_id, op and now; ``created`` is ``True`` for a new
    event and ``False`` when the idempotency key replays the identical
    event.

    A ``prepare`` requires the job to be matched, hold an active
    reservation and not yet have migrated; the target must be a
    registered offer other than the matched resource, in a region the
    job permits, with ``now`` no later than the job deadline and enough
    capacity for the job's energy once the energy already landed there
    by active reservations (accounting for committed migrations) and the
    pending placeholders of other prepared migrations are deducted. It
    keeps the energy on the source and locks the target. A ``commit``
    releases the source and lands the energy on the target; an ``abort``
    releases the target lock. Both require a preceding, still-open
    prepare for the same job and target. Each job goes through the
    prepare/commit-or-abort sequence at most once.
    """
    for value in (jobs, offers, matches, reserves, state, job_id, target_id,
                  op, key):
        if not isinstance(value, str) or not value:
            raise ValueError("string arguments must be non-empty strings")
    if op not in _OPS:
        raise ValueError("op must be prepare, commit or abort")
    if not _is_plain_int(now) or now < 0:
        raise ValueError("now must be a non-boolean non-negative integer")

    store = _get_store(state)
    with store.lock:
        jobs_real = os.path.realpath(jobs)
        offers_real = os.path.realpath(offers)
        matches_real = os.path.realpath(matches)
        reserves_real = os.path.realpath(reserves)

        job_records, _ = _load_registry(
            jobs_real, _jobs._validate_structure, "jobs registry")
        if job_id not in job_records:
            raise KeyError(job_id)
        job = job_records[job_id]

        offer_records, _ = _load_registry(
            offers_real, _offers._validate_structure, "offers registry")
        if target_id not in offer_records:
            raise KeyError(target_id)

        match_records = _load_matches(
            matches_real, job_records, offer_records)
        _reserve_events, reserve_lifecycle = _load_reserves(
            reserves_real, job_records, offer_records, match_records)
        events, lifecycle = _load_journal(
            store.realpath, job_records, offer_records, match_records)

        existing = events.get(key)
        if existing is not None:
            if (existing["job_id"] != job_id
                    or existing["target_id"] != target_id
                    or existing["op"] != op
                    or existing["now"] != now):
                raise ValueError("idempotency key was already used with a "
                                 "different event")
            return dict(existing), False

        matched = match_records.get(job_id)
        if matched is None:
            raise LookupError("job is not matched")
        source_id = matched["resource_id"]

        status = lifecycle.get(job_id)
        if op == "prepare":
            if reserve_lifecycle.get(job_id) != "reserved":
                raise LookupError("job has no active reservation")
            if status is not None:
                raise ValueError("job can only be prepared for migration "
                                 "once")
            if target_id == source_id:
                raise LookupError("migration target must differ from the "
                                  "matched resource")
            if (offer_records[target_id]["region"]
                    not in job["residency_regions"]):
                raise LookupError("migration target region is not permitted "
                                  "by the job")
            if now > job["deadline"]:
                raise TimeoutError("migration prepare is past the job "
                                   "deadline")

            # Energy currently landed per resource: active reservations at
            # their matched resource, except jobs whose migration committed
            # land at their commit target instead. Prepared and aborted
            # migrations still occupy their source.
            prepare_target = {
                event["job_id"]: event["target_id"]
                for event in events.values()
                if event["op"] == "prepare"
            }
            landed: dict[str, int] = {}
            for reserved_job_id, reserve_status in reserve_lifecycle.items():
                if reserve_status != "reserved":
                    continue
                if lifecycle.get(reserved_job_id) == "committed":
                    rid = prepare_target[reserved_job_id]
                else:
                    rid = match_records[reserved_job_id]["resource_id"]
                landed[rid] = (
                    landed.get(rid, 0)
                    + job_records[reserved_job_id]["energy_wh"])

            # Pending placeholders: each open prepare locks its target.
            pending: dict[str, int] = {}
            for event in events.values():
                if event["op"] != "prepare":
                    continue
                if lifecycle[event["job_id"]] != "prepared":
                    continue
                rid = event["target_id"]
                pending[rid] = (
                    pending.get(rid, 0)
                    + job_records[event["job_id"]]["energy_wh"])

            target = offer_records[target_id]
            if (target["capacity_wh"]
                    - landed.get(target_id, 0)
                    - pending.get(target_id, 0) < job["energy_wh"]):
                raise LookupError("migration target has no remaining capacity "
                                  "for the job")
        else:
            if status != "prepared":
                raise ValueError("migration must be prepared before it is "
                                 "committed or aborted")
            prepared = next(
                event for event in events.values()
                if event["job_id"] == job_id and event["op"] == "prepare")
            if prepared["target_id"] != target_id:
                raise ValueError("commit or abort must target the prepared "
                                 "resource")

        event: dict[str, object] = {
            "job_id": job_id,
            "source_id": source_id,
            "target_id": target_id,
            "op": op,
            "now": now,
        }
        events[key] = event
        _atomic_write(store.realpath, events)
        return dict(event), True
