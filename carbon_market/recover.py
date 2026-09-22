"""Persistent, thread-safe migration recovery.

Recovery settles a migration that crashed while prepared: it looks up the
job's still-open ``prepare`` event in the migration journal and appends the
finishing event itself — a ``commit`` when the job still holds an active
reservation and ``now`` is no later than the job deadline, an ``abort``
otherwise. The source and target are taken from the prepare event, so the
recovered event is exactly the one a manual commit or abort would have
recorded. Recovery shares the journal's per-realpath lock with manual
commit/abort, so the two interleave linearizably, and an idempotency key
replays the event it first recorded.
"""

from __future__ import annotations

import os

from . import jobs as _jobs
from . import migrate as _migrate
from . import offers as _offers
from .market import _load_registry

__all__ = ["run"]


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
    """Settle a job's pending migration prepare idempotently.

    Returns ``(event, created)`` where ``event`` carries job_id, source_id,
    target_id, op and now; ``created`` is ``True`` for a new event and
    ``False`` when the idempotency key replays the identical event. The op
    is ``commit`` when the job still holds an active reservation and ``now``
    is no later than its deadline, and ``abort`` otherwise; source_id and
    target_id come from the pending prepare. A job that is not matched, has
    no pending prepare or whose migration already finished is rejected.
    """
    for value in (jobs, offers, matches, reserves, state, job_id, key):
        if not isinstance(value, str) or not value:
            raise ValueError("string arguments must be non-empty strings")
    if not _migrate._is_plain_int(now) or now < 0:
        raise ValueError("now must be a non-boolean non-negative integer")

    # The store (and its lock) is shared with migrate.run keyed by real
    # path, so recovery and manual commit/abort serialize on the journal.
    store = _migrate._get_store(state)
    with store.lock:
        job_records, _ = _load_registry(
            os.path.realpath(jobs), _jobs._validate_structure, "jobs registry")
        if job_id not in job_records:
            raise KeyError(job_id)
        job = job_records[job_id]

        offer_records, _ = _load_registry(
            os.path.realpath(offers), _offers._validate_structure,
            "offers registry")
        match_records = _migrate._load_matches(
            os.path.realpath(matches), job_records, offer_records)
        _reserve_events, reserve_lifecycle = _migrate._load_reserves(
            os.path.realpath(reserves), job_records, offer_records,
            match_records)
        events, lifecycle = _migrate._load_journal(
            store.realpath, job_records, offer_records, match_records)

        existing = events.get(key)
        if existing is not None:
            # The op and endpoints are decided by the recorded prepare, so
            # the caller-supplied job_id and now fully identify the replay.
            if existing["job_id"] != job_id or existing["now"] != now:
                raise ValueError("idempotency key was already used with a "
                                 "different event")
            return dict(existing), False

        if job_id not in match_records:
            raise ValueError("job is not matched")
        status = lifecycle.get(job_id)
        if status is None:
            raise ValueError("job has no pending prepare")
        if status != "prepared":
            raise ValueError("migration is already finished")

        prepared = next(
            event for event in events.values()
            if event["job_id"] == job_id and event["op"] == "prepare")

        if (reserve_lifecycle.get(job_id) == "reserved"
                and now <= job["deadline"]):
            op = "commit"
        else:
            op = "abort"

        event: dict[str, object] = {
            "job_id": job_id,
            "source_id": prepared["source_id"],
            "target_id": prepared["target_id"],
            "op": op,
            "now": now,
        }
        events[key] = event
        _migrate._atomic_write(store.realpath, events)
        return dict(event), True
