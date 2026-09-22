"""Persistent, thread-safe two-phase migration journal.

A prepare event locks a matched, actively reserved job's migration from its
matched source offer to a chosen target offer, occupying the source and
locking the target; a commit event releases the source and lands the
reservation on the target, while an abort releases the target placeholder.
Each job runs through at most one migration round. The journal is append-only
in spirit: events are never removed or rewritten, and an idempotency key
replays the event it first recorded.
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
from ._jsonio import strict_loads
from .market import _load_registry

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


def _load_reserves(
    realpath: str,
    job_records: dict[str, dict[str, Any]],
    offer_records: dict[str, dict[str, Any]],
    match_records: dict[str, dict[str, str]],
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    # The reserve journal is a required input, so a missing file surfaces as
    # FileNotFoundError (unlike the migration journal, which is created on
    # demand).
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
    # meaning: a key added later may sort first. Rebuild each job's
    # prepare/commit/abort status from op counts instead of traversal order.
    events: dict[str, dict[str, Any]] = {}
    prepares: dict[str, dict[str, Any]] = {}
    settles: dict[str, dict[str, Any]] = {}
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
        if source_id not in offer_records:
            raise ValueError("event must reference a registered source offer")
        if target_id not in offer_records:
            raise ValueError("event must reference a registered target offer")
        match = match_records.get(job_id)
        if match is None or match["resource_id"] != source_id:
            raise ValueError("event source must be the matched resource")

        normalized = {field: event[field] for field in _EVENT_FIELDS}
        if op == "prepare":
            if job_id in prepares:
                raise ValueError("job prepared more than once")
            if now > job_records[job_id]["deadline"]:
                raise ValueError("prepare event later than the job deadline")
            prepares[job_id] = normalized
        else:
            if job_id in settles:
                raise ValueError("job migration settled more than once")
            settles[job_id] = normalized

        events[event_key] = normalized

    for job_id, settle in settles.items():
        prepared = prepares.get(job_id)
        if prepared is None:
            raise ValueError("commit or abort must follow a prepare")
        if prepared["target_id"] != settle["target_id"]:
            raise ValueError("commit or abort must use the prepared target")

    lifecycle: dict[str, str] = {
        job_id: "prepared" for job_id in prepares
    }
    for job_id, event in settles.items():
        lifecycle[job_id] = (
            "committed" if event["op"] == "commit" else "aborted")

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
    """Prepare, commit or abort a matched job's migration idempotently.

    Returns ``(event, created)`` where ``event`` carries job_id, source_id,
    target_id, op and now; ``created`` is ``True`` for a new event and
    ``False`` when the idempotency key replays the identical event. A prepare
    requires the job to be matched, actively reserved and not yet migrated;
    the source is the matched resource and the target must exist, differ
    from the source, sit in a permitted residency region, meet the job
    deadline (``now`` not past it) and hold the job's energy after the
    active reservations currently landing on it and the placeholders of
    other pending prepares. A successful prepare occupies the source and
    locks the target; a commit releases the source and occupies the target,
    an abort releases the target. Commit and abort require a prepare for the
    same job and target; each job runs through at most one migration round.
    """
    for value in (jobs, offers, matches, reserves, state,
                  job_id, target_id, op, key):
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

        match_records = _reserve._load_matches(
            matches_real, job_records, offer_records)
        reserve_events, reserve_lifecycle = _load_reserves(
            reserves_real, job_records, offer_records, match_records)
        events, lifecycle = _load_journal(
            store.realpath, job_records, offer_records, match_records)

        existing = events.get(key)
        if existing is not None:
            # Validation already ties the stored event's source to the job's
            # match, so job/target/op/now fully identify the replay.
            if (existing["job_id"] != job_id
                    or existing["target_id"] != target_id
                    or existing["op"] != op
                    or existing["now"] != now):
                raise ValueError("idempotency key was already used with a "
                                 "different event")
            return dict(existing), False

        if op == "prepare":
            matched = match_records.get(job_id)
            if matched is None:
                raise LookupError("job is not matched")
            source_id = matched["resource_id"]
            if reserve_lifecycle.get(job_id) != "reserved":
                raise LookupError("job has no active reservation")
            if lifecycle.get(job_id) is not None:
                raise ValueError("job can only run one migration round")
            if target_id == source_id:
                raise ValueError("migration target must differ from the "
                                 "source")
            if offer_records[target_id]["region"] \
                    not in job["residency_regions"]:
                raise LookupError("target region is not permitted for the "
                                  "job")
            if now > job["deadline"]:
                raise TimeoutError("prepare request is past the job deadline")

            commit_targets = {
                event["job_id"]: event["target_id"]
                for event in events.values() if event["op"] == "commit"
            }
            usage: dict[str, int] = {}
            for event in reserve_events.values():
                if event["op"] != "reserve":
                    continue
                other = event["job_id"]
                if reserve_lifecycle[other] != "reserved":
                    continue
                if lifecycle.get(other) == "committed":
                    resource_id = commit_targets[other]
                else:
                    resource_id = event["resource_id"]
                usage[resource_id] = (
                    usage.get(resource_id, 0)
                    + job_records[other]["energy_wh"])
            for event in events.values():
                if event["op"] != "prepare":
                    continue
                if lifecycle[event["job_id"]] != "prepared":
                    continue
                resource_id = event["target_id"]
                usage[resource_id] = (
                    usage.get(resource_id, 0)
                    + job_records[event["job_id"]]["energy_wh"])
            if usage.get(target_id, 0) + job["energy_wh"] \
                    > offer_records[target_id]["capacity_wh"]:
                raise LookupError("target offer has no remaining capacity "
                                  "for the job")
        else:
            if lifecycle.get(job_id) != "prepared":
                raise ValueError("job must be prepared before it is "
                                 "committed or aborted")
            prepared = next(
                event for event in events.values()
                if event["op"] == "prepare" and event["job_id"] == job_id)
            if prepared["target_id"] != target_id:
                raise ValueError("commit or abort must use the prepared "
                                 "target")
            source_id = prepared["source_id"]

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
