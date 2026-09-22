"""Automatic recovery of pending migrations.

Recovery finishes a migration left in its open ``prepare`` phase: it finds
the job's pending prepare in the migration journal and appends a terminal
event whose ``op`` is chosen for it -- ``commit`` when the job still holds
an active reservation and ``now`` is no later than its deadline, ``abort``
otherwise. The source and target come straight from the prepare event.

The state file is the very same migration journal written by
:mod:`carbon_market.migrate`, and recovery shares that module's per-realpath
lock, so an automatic recovery and a manual commit/abort linearize against
each other. As everywhere else, an idempotency key replays the event it
first recorded.
"""

from __future__ import annotations

import os
from typing import Any

from . import migrate as _migrate
from . import jobs as _jobs
from . import offers as _offers
from .market import _load_registry
from ._jsonio import strict_loads

__all__ = ["run"]


def _is_plain_int(value: object) -> bool:
    # bool is a subclass of int and must be rejected.
    return isinstance(value, int) and not isinstance(value, bool)


def _load_state(
    realpath: str,
    job_records: dict[str, dict[str, Any]],
    offer_records: dict[str, dict[str, Any]],
    match_records: dict[str, dict[str, str]],
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    # Recovery never creates a journal: without an existing state file
    # there is no pending prepare to finish, so FileNotFoundError (and any
    # other OSError) propagates just as for the other required inputs.
    with open(realpath, encoding="utf-8") as handle:
        text = handle.read()
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"migration journal {realpath!r} is not valid JSON") from exc
    return _migrate._validate_journal(
        data, job_records, offer_records, match_records)


def run(
    jobs: str,
    offers: str,
    matches: str,
    reserves: str,
    state: str,
    job_id: str,
    key: str,
    now: int,
) -> tuple[dict[str, object], bool]:
    """Recover a pending migration idempotently.

    Returns ``(event, created)`` where ``event`` carries job_id,
    source_id, target_id, op and now; ``created`` is ``True`` for a new
    event and ``False`` when the idempotency key replays the identical
    event. The op is ``commit`` when the job has an active reservation
    and ``now`` is at most the job deadline, and ``abort`` otherwise.
    """
    for value in (jobs, offers, matches, reserves, state, job_id, key):
        if not isinstance(value, str) or not value:
            raise ValueError("string arguments must be non-empty strings")
    if not _is_plain_int(now) or now < 0:
        raise ValueError("now must be a non-boolean non-negative integer")

    # Share migrate's store registry so recovery and manual commit/abort
    # take the same lock for a given state file.
    store = _migrate._get_store(state)
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
        match_records = _migrate._load_matches(
            matches_real, job_records, offer_records)
        _reserve_events, reserve_lifecycle = _migrate._load_reserves(
            reserves_real, job_records, offer_records, match_records)
        events, lifecycle = _load_state(
            store.realpath, job_records, offer_records, match_records)

        if match_records.get(job_id) is None:
            raise ValueError("job is not matched")

        prepare_event = next(
            (event for event in events.values()
             if event["job_id"] == job_id and event["op"] == "prepare"),
            None)
        if prepare_event is None:
            raise ValueError("job has no prepare event to recover")

        op = ("commit"
              if reserve_lifecycle.get(job_id) == "reserved"
              and now <= job["deadline"]
              else "abort")
        event: dict[str, object] = {
            "job_id": job_id,
            "source_id": prepare_event["source_id"],
            "target_id": prepare_event["target_id"],
            "op": op,
            "now": now,
        }

        existing = events.get(key)
        if existing is not None:
            if existing != event:
                raise ValueError("idempotency key was already used with a "
                                 "different event")
            return dict(existing), False

        if lifecycle.get(job_id) != "prepared":
            raise ValueError("migration is already committed or aborted")

        events[key] = event
        _migrate._atomic_write(store.realpath, events)
        return dict(event), True
