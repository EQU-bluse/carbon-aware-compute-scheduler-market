"""Persistent, thread-safe dispatch commitments over cleared trades.

A dispatch decision is the schedulable unit derived from one
:func:`market.clear` trade: :func:`commit` reads the ``jobs.submit``
acceptance file, the resource supply file and the clearing ledger as
one snapshot and binds the trade's resource version and the job's
deadline into a decision that starts ``ready`` with zero attempts.
:func:`claim` leases a ``ready`` or ``failed`` decision to an owner,
:func:`finish` lets the current owner inside its lease close the
attempt as ``succeeded`` or ``failed``, and :func:`recover` returns a
decision whose lease has strictly expired to ``ready`` without
consuming an attempt.

The dispatch ledger is one compact UTF-8 JSON document -- version,
decisions sorted by job id, audit events sorted by idempotency key --
with non-ASCII written through and exactly one trailing newline;
negative-zero and non-finite number literals are illegal. Every audit
event binds the complete request (action, job, owner, result, moment
and lease) together with the decision snapshot the action produced, so
an equivalent replay of any of the four calls returns the current
record with ``False`` and writes nothing, while the same key carrying a
changed request raises ``ValueError``.
"""

from __future__ import annotations

import contextlib
import copy
import fcntl
import json
import os
import tempfile
import threading
from typing import Any, Iterator

from . import jobs as _jobs
from . import market as _market
from . import resources as _resources
from ._jsonio import finite_loads

__all__ = ["commit", "claim", "finish", "recover"]

_VERSION = 1
_ROOT_FIELDS = ("version", "decisions", "audit")
_DECISION_FIELDS = ("job_id", "resource_id", "version", "deadline",
                    "state", "attempts", "owner", "lease_end")
_EVENT_FIELDS = ("key", "action", "job_id", "owner", "result", "at",
                 "lease", "decision")
_STATES = ("ready", "claimed", "succeeded", "failed")
_ACTIONS = ("commit", "claim", "finish", "recover")
_RESULTS = ("succeeded", "failed")
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


@contextlib.contextmanager
def _dispatch_lock(realpath: str, *, shared: bool = False) -> Iterator[None]:
    # As in market.clear, the companion lock file is never unlinked and
    # an flock is released by the kernel on process exit, so equivalent
    # real paths share one lock across threads and processes.
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


def _check_sorted_keys(mapping: dict[Any, Any], label: str) -> None:
    keys = list(mapping)
    if keys != sorted(keys):
        raise ValueError(f"{label} must be ordered by key code point")


def _check_decision(record: object, label: str) -> dict[str, Any]:
    if not isinstance(record, dict) \
            or set(record.keys()) != set(_DECISION_FIELDS):
        raise ValueError(f"{label} has invalid fields")
    job_id = record["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError(f"{label} job_id must be a non-empty string")
    resource_id = record["resource_id"]
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError(f"{label} resource_id must be a non-empty string")
    version = record["version"]
    if not _is_plain_int(version) or version < 1:
        raise ValueError(f"{label} version must be a positive integer")
    deadline = record["deadline"]
    if not _is_plain_int(deadline) or deadline < 0:
        raise ValueError(f"{label} deadline must be a non-boolean "
                         "non-negative integer")
    state = record["state"]
    if state not in _STATES:
        raise ValueError(f"{label} state is invalid")
    attempts = record["attempts"]
    if not _is_plain_int(attempts) or attempts < 0:
        raise ValueError(f"{label} attempts must be a non-boolean "
                         "non-negative integer")
    owner = record["owner"]
    lease_end = record["lease_end"]
    if state == "claimed":
        # A live claim always binds a non-empty owner and a lease end
        # that does not cross the job's deadline.
        if not isinstance(owner, str) or not owner:
            raise ValueError(f"{label} owner must be a non-empty string "
                             "while claimed")
        if not _is_plain_int(lease_end) or lease_end < 0:
            raise ValueError(f"{label} lease_end must be a non-boolean "
                             "non-negative integer while claimed")
        if lease_end > deadline:
            raise ValueError(f"{label} lease end crosses the job deadline")
    else:
        if owner is not None or lease_end is not None:
            raise ValueError(f"{label} owner and lease must be cleared "
                             "unless claimed")
    return {field: record[field] for field in _DECISION_FIELDS}


def _validate_ledger(
    data: object,
    accepted: dict[str, dict[str, Any]] | None,
    trades: dict[str, dict[str, Any]] | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("dispatch ledger root must be an object with keys "
                         "version, decisions and audit")
    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported dispatch ledger version")

    decisions_raw = data["decisions"]
    audit_raw = data["audit"]
    if not isinstance(decisions_raw, dict) \
            or not isinstance(audit_raw, dict):
        raise ValueError("decisions and audit must be objects")
    _check_sorted_keys(decisions_raw, "decisions")
    _check_sorted_keys(audit_raw, "audit")

    decisions: dict[str, dict[str, Any]] = {}
    for name, record in decisions_raw.items():
        if not isinstance(name, str) or not name:
            raise ValueError("decision job ids must be non-empty strings")
        decision = _check_decision(record, "decision record")
        if decision["job_id"] != name:
            raise ValueError("decision record id does not match its key")
        if accepted is not None:
            # The commit path holds every input snapshot, so each
            # decision must still reference the accepted job, its
            # deadline and the exact traded resource version.
            job = accepted.get(name)
            if job is None:
                raise ValueError("decision must reference an accepted job")
            if decision["deadline"] != job["deadline"]:
                raise ValueError("decision deadline does not match its "
                                 "accepted job")
            trade = trades.get(name) if trades is not None else None
            if trade is None:
                raise ValueError("decision must reference a recorded trade")
            if decision["resource_id"] != trade["resource_id"] \
                    or decision["version"] != trade["version"]:
                raise ValueError("decision must bind the traded resource "
                                 "version")
        decisions[name] = decision

    events: dict[str, dict[str, Any]] = {}
    commit_jobs: set[str] = set()
    claim_attempts: dict[str, list[int]] = {}
    for key, event in audit_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event, dict) \
                or set(event.keys()) != set(_EVENT_FIELDS):
            raise ValueError("dispatch audit event has invalid fields")
        event_key = event["key"]
        action = event["action"]
        job_id = event["job_id"]
        owner = event["owner"]
        result = event["result"]
        at = event["at"]
        lease = event["lease"]
        if event_key != key or not isinstance(event_key, str) or not event_key:
            raise ValueError("audit event key does not match its map key")
        if action not in _ACTIONS:
            raise ValueError("audit event action is invalid")
        if not isinstance(job_id, str) or not job_id or job_id not in decisions:
            raise ValueError("audit event must reference a recorded decision")
        if not _is_plain_int(at) or at < 0:
            raise ValueError("audit event at must be a non-boolean "
                             "non-negative integer")
        if action in ("commit", "recover"):
            if owner is not None or result is not None or lease is not None:
                raise ValueError("commit and recover events carry no owner, "
                                 "result or lease")
        elif action == "claim":
            if not isinstance(owner, str) or not owner:
                raise ValueError("claim event owner must be a non-empty "
                                 "string")
            if result is not None:
                raise ValueError("claim event carries no result")
            if not _is_plain_int(lease) or lease <= 0:
                raise ValueError("claim event lease must be a positive "
                                 "integer")
        else:
            if not isinstance(owner, str) or not owner:
                raise ValueError("finish event owner must be a non-empty "
                                 "string")
            if result not in _RESULTS:
                raise ValueError("finish event result is invalid")
            if lease is not None:
                raise ValueError("finish event carries no lease")

        snapshot = _check_decision(event["decision"],
                                   "audit event decision snapshot")
        if snapshot["job_id"] != job_id:
            raise ValueError("audit event snapshot does not match its job")
        decision = decisions[job_id]
        # Resource version and deadline are immutable over a decision's
        # life, so every snapshot must bind the same ones.
        if snapshot["resource_id"] != decision["resource_id"] \
                or snapshot["version"] != decision["version"] \
                or snapshot["deadline"] != decision["deadline"]:
            raise ValueError("audit event snapshot does not bind the "
                             "decision's resource version and deadline")
        if action == "commit":
            if snapshot["state"] != "ready" or snapshot["attempts"] != 0:
                raise ValueError("commit snapshot must be a fresh ready "
                                 "decision")
            if job_id in commit_jobs:
                raise ValueError("job committed under more than one "
                                 "audit event")
            commit_jobs.add(job_id)
        elif action == "claim":
            if snapshot["state"] != "claimed" \
                    or snapshot["owner"] != owner \
                    or snapshot["lease_end"] != at + lease:
                raise ValueError("claim snapshot does not match its "
                                 "request")
            claim_attempts.setdefault(job_id, []).append(
                snapshot["attempts"])
        elif action == "finish":
            if snapshot["state"] != result:
                raise ValueError("finish snapshot must carry the "
                                 "submitted result")
        else:
            if snapshot["state"] != "ready":
                raise ValueError("recover snapshot must be ready")
        events[key] = {
            "key": event_key,
            "action": action,
            "job_id": job_id,
            "owner": owner,
            "result": result,
            "at": at,
            "lease": lease,
            "decision": snapshot,
        }

    # Decisions and events describe one dispatch history: every decision
    # is born from exactly one commit event, and the attempt counter is
    # exactly the number of claims, each claim consuming the next one.
    if commit_jobs != set(decisions):
        raise ValueError("every decision must be born from exactly one "
                         "commit event")
    for job_id, seen in claim_attempts.items():
        if sorted(seen) != list(range(1, len(seen) + 1)) \
                or decisions[job_id]["attempts"] != len(seen):
            raise ValueError("decision attempts do not match their "
                             "claim events")

    return decisions, events


def _canonical_bytes(
    decisions: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
) -> bytes:
    payload = {
        "version": _VERSION,
        "decisions": {name: decisions[name] for name in sorted(decisions)},
        "audit": {key: events[key] for key in sorted(events)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_ledger(
    realpath: str,
    accepted: dict[str, dict[str, Any]] | None = None,
    trades: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return {}, {}, None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"dispatch ledger {realpath!r} is not valid UTF-8") from exc
    try:
        # Negative-zero and non-finite literals are format errors.
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"dispatch ledger {realpath!r} is not valid JSON") from exc
    decisions, events = _validate_ledger(data, accepted, trades)
    # As for the clearing ledger, the file is accepted only in canonical
    # compact form with a single trailing newline.
    if raw != _canonical_bytes(decisions, events):
        raise ValueError(
            f"dispatch ledger {realpath!r} is not in canonical compact "
            "form")
    return decisions, events, raw


def _fsync_directory(directory: str) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _rollback_ledger(realpath: str, directory: str, old_bytes: bytes | None,
                     first: BaseException) -> None:
    # Restore the exact pre-call bytes while the exclusive lock is held,
    # or remove a ledger that did not exist beforehand, then sync the
    # directory. A failed recovery chains after the original error.
    try:
        if old_bytes is None:
            try:
                os.unlink(realpath)
            except FileNotFoundError:
                pass
        else:
            fd, tmp_path = tempfile.mkstemp(
                dir=directory, prefix=".dispatch-restore-", suffix=".tmp")
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


def _commit_ledger(realpath: str, payload: bytes,
                   old_bytes: bytes | None) -> None:
    # One durable commit for the decision and its audit event: synced
    # same-directory temporary, atomic replace and a directory fsync,
    # restoring the pre-call bytes on any failure.
    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".dispatch-", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, realpath)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise
    try:
        _fsync_directory(directory)
    except BaseException as first:
        _rollback_ledger(realpath, directory, old_bytes, first)
        raise


def _replay(
    events: dict[str, dict[str, Any]],
    key: str,
    action: str,
    job_id: str,
    owner: str | None,
    result: str | None,
    at: int,
    lease: int | None,
) -> dict[str, Any] | None:
    # An idempotency key replays only the exact request it first
    # recorded; the same key with a changed action, job, owner, result,
    # moment or lease is a ValueError.
    event = events.get(key)
    if event is None:
        return None
    if event["action"] != action or event["job_id"] != job_id \
            or event["owner"] != owner or event["result"] != result \
            or event["at"] != at or event["lease"] != lease:
        raise ValueError("idempotency key was already used with a "
                         "different request")
    return event


def _audit_event(key: str, action: str, job_id: str, owner: str | None,
                 result: str | None, at: int, lease: int | None,
                 decision: dict[str, Any]) -> dict[str, Any]:
    return {
        "key": key,
        "action": action,
        "job_id": job_id,
        "owner": owner,
        "result": result,
        "at": at,
        "lease": lease,
        "decision": copy.deepcopy(decision),
    }


def commit(
    jobs: str,
    supply: str,
    trades: str,
    ledger: str,
    job_id: str,
    key: str,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Commit a dispatch decision for one cleared job idempotently.

    ``jobs``, ``supply``, ``trades`` and ``ledger`` paths, ``job_id``
    and ``key`` must be non-empty strings and ``at`` a non-boolean
    non-negative integer moment; the four paths must also resolve to
    distinct real locations. Any violation raises ``ValueError`` before
    a business file is read.

    The acceptance file, the supply file and the clearing ledger are
    read as one snapshot under their shared locks together with the
    dispatch ledger's exclusive lock, all four taken in resolved
    real-path order. The decision binds the trade's resource id and
    version and the accepted job's deadline, starts ``ready`` with zero
    attempts and no owner or lease, and is committed together with the
    audit event binding the complete request and the decision snapshot.
    A missing dispatch ledger is created only by this first commit.

    Returns ``(decision, created)``. Replaying the same key with the
    same job and moment returns the current decision with ``False``
    without writing; the same key with a changed request, or a job
    already committed under another key, raises ``ValueError`` with the
    ledger untouched. An unknown job raises ``KeyError``, a job without
    a recorded trade raises ``LookupError``, and a moment past the
    job's deadline raises ``TimeoutError``; none of them creates the
    ledger. Missing input files or a missing ledger parent raise
    ``FileNotFoundError``; invalid structure, references, ordering or
    canonical bytes raise ``ValueError``; other locking, read/write or
    sync failures raise ``OSError``.
    """
    for value in (jobs, supply, trades, ledger, job_id, key):
        if not isinstance(value, str) or not value:
            raise ValueError("jobs, supply, trades, ledger, job_id and key "
                             "must be non-empty strings")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    job_real = os.path.realpath(jobs)
    supply_real = os.path.realpath(supply)
    trades_real = os.path.realpath(trades)
    ledger_real = os.path.realpath(ledger)
    if len({job_real, supply_real, trades_real, ledger_real}) != 4:
        raise ValueError("jobs, supply, trades and ledger paths must be "
                         "distinct real paths")

    store = _get_store(ledger)
    with store.lock:
        # Locks are taken in one resolved-real-path order shared by
        # every caller, so concurrent commits can never deadlock; the
        # dispatch ledger lock is exclusive, the input snapshots shared.
        with contextlib.ExitStack() as stack:
            for locked in sorted({job_real, supply_real, trades_real,
                                  ledger_real}):
                stack.enter_context(
                    _dispatch_lock(locked, shared=(locked != ledger_real)))

            accepted, _job_map, _job_events, job_raw = \
                _jobs._load_submit_file(job_real)
            if job_raw is None:
                raise FileNotFoundError(
                    f"acceptance file {job_real!r} does not exist")
            if job_raw != _jobs._serialize_submit_file(
                    accepted, _job_map, _job_events):
                raise ValueError(
                    f"acceptance file {job_real!r} is not in canonical "
                    "compact form")
            history, _supply_map, _supply_events, supply_raw = \
                _resources._load_file(supply_real)
            if supply_raw is None:
                raise FileNotFoundError(
                    f"supply file {supply_real!r} does not exist")
            cleared, _clear_keys, trades_raw = _market._load_clear_ledger(
                trades_real, accepted, history)
            if trades_raw is None:
                raise FileNotFoundError(
                    f"clearing ledger {trades_real!r} does not exist")
            decisions, events, old_bytes = _load_ledger(
                ledger_real, accepted, cleared)

            job = accepted.get(job_id)
            if job is None:
                raise KeyError(job_id)

            if _replay(events, key, "commit", job_id, None, None, at,
                       None) is not None:
                return copy.deepcopy(decisions[job_id]), False
            if job_id in decisions:
                raise ValueError("job is already committed under another "
                                 "idempotency key")
            trade = cleared.get(job_id)
            if trade is None:
                raise LookupError("job has no recorded trade")
            if at > job["deadline"]:
                raise TimeoutError("commit moment is past the job deadline")

            decision: dict[str, Any] = {
                "job_id": job_id,
                "resource_id": trade["resource_id"],
                "version": trade["version"],
                "deadline": job["deadline"],
                "state": "ready",
                "attempts": 0,
                "owner": None,
                "lease_end": None,
            }
            decisions[job_id] = decision
            events[key] = _audit_event(key, "commit", job_id, None, None,
                                       at, None, decision)
            _commit_ledger(ledger_real,
                           _canonical_bytes(decisions, events), old_bytes)
            return copy.deepcopy(decision), True


def _load_existing(
    ledger: str,
) -> tuple[_Store, dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           bytes]:
    # Shared prologue of claim, finish and recover: the dispatch ledger
    # is only ever created by commit, so every other action fails on a
    # missing ledger.
    store = _get_store(ledger)
    decisions, events, old_bytes = _load_ledger(store.realpath)
    if old_bytes is None:
        raise FileNotFoundError(
            f"dispatch ledger {store.realpath!r} does not exist")
    return store, decisions, events, old_bytes


def claim(
    ledger: str,
    job_id: str,
    owner: str,
    lease: int,
    key: str,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Lease a ready or failed decision to an owner idempotently.

    ``ledger``, ``job_id``, ``owner`` and ``key`` must be non-empty
    strings, ``lease`` a non-boolean positive integer and ``at`` a
    non-boolean non-negative integer moment, else ``ValueError`` before
    the ledger is read. Only the dispatch ledger is touched, under its
    exclusive lock.

    The decision must be ``ready`` or ``failed`` (anything else is
    ``ValueError``) and the lease end ``at + lease`` must not cross the
    job's deadline (``TimeoutError``). A successful claim moves the
    decision to ``claimed`` under ``owner`` with the lease end set and
    the attempt counter incremented by one, committed together with the
    audit event. Returns ``(decision, created)``; an equivalent replay
    returns the current decision with ``False`` and writes nothing,
    while the same key with a changed request raises ``ValueError``.
    An unknown job raises ``KeyError`` and a missing ledger
    ``FileNotFoundError``.
    """
    for value in (ledger, job_id, owner, key):
        if not isinstance(value, str) or not value:
            raise ValueError("ledger, job_id, owner and key must be "
                             "non-empty strings")
    if not _is_plain_int(lease) or lease <= 0:
        raise ValueError("lease must be a non-boolean positive integer")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    store = _get_store(ledger)
    with store.lock:
        with _dispatch_lock(store.realpath):
            _store, decisions, events, old_bytes = _load_existing(ledger)
            decision = decisions.get(job_id)
            if decision is None:
                raise KeyError(job_id)

            if _replay(events, key, "claim", job_id, owner, None, at,
                       lease) is not None:
                return copy.deepcopy(decision), False
            if decision["state"] not in ("ready", "failed"):
                raise ValueError("only a ready or failed decision can be "
                                 "claimed")
            lease_end = at + lease
            if lease_end > decision["deadline"]:
                raise TimeoutError("lease end crosses the job deadline")

            decision["state"] = "claimed"
            decision["attempts"] += 1
            decision["owner"] = owner
            decision["lease_end"] = lease_end
            events[key] = _audit_event(key, "claim", job_id, owner, None,
                                       at, lease, decision)
            _commit_ledger(store.realpath,
                           _canonical_bytes(decisions, events), old_bytes)
            return copy.deepcopy(decision), True


def finish(
    ledger: str,
    job_id: str,
    owner: str,
    result: str,
    key: str,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Close the current claim of a decision idempotently.

    ``ledger``, ``job_id``, ``owner`` and ``key`` must be non-empty
    strings, ``result`` exactly ``succeeded`` or ``failed`` and ``at``
    a non-boolean non-negative integer moment, else ``ValueError``
    before the ledger is read. Only the dispatch ledger is touched,
    under its exclusive lock.

    The decision must be ``claimed`` (anything else is ``ValueError``)
    by ``owner`` (a different owner raises ``PermissionError``) and the
    moment must lie inside the lease (a moment past the lease end
    raises ``TimeoutError``). A successful finish moves the decision to
    the submitted result state and clears the owner and the lease,
    committed together with the audit event. Returns
    ``(decision, created)``; an equivalent replay returns the current
    decision with ``False`` and writes nothing, while the same key with
    a changed request raises ``ValueError``. An unknown job raises
    ``KeyError`` and a missing ledger ``FileNotFoundError``.
    """
    for value in (ledger, job_id, owner, key):
        if not isinstance(value, str) or not value:
            raise ValueError("ledger, job_id, owner and key must be "
                             "non-empty strings")
    if result not in _RESULTS:
        raise ValueError("result must be succeeded or failed")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    store = _get_store(ledger)
    with store.lock:
        with _dispatch_lock(store.realpath):
            _store, decisions, events, old_bytes = _load_existing(ledger)
            decision = decisions.get(job_id)
            if decision is None:
                raise KeyError(job_id)

            if _replay(events, key, "finish", job_id, owner, result, at,
                       None) is not None:
                return copy.deepcopy(decision), False
            if decision["state"] != "claimed":
                raise ValueError("only a claimed decision can be finished")
            if decision["owner"] != owner:
                raise PermissionError("only the current owner can finish "
                                      "the claim")
            if at > decision["lease_end"]:
                raise TimeoutError("finish moment is past the lease end")

            decision["state"] = result
            decision["owner"] = None
            decision["lease_end"] = None
            events[key] = _audit_event(key, "finish", job_id, owner,
                                       result, at, None, decision)
            _commit_ledger(store.realpath,
                           _canonical_bytes(decisions, events), old_bytes)
            return copy.deepcopy(decision), True


def recover(
    ledger: str,
    job_id: str,
    key: str,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Return a decision whose lease strictly expired to ready.

    ``ledger``, ``job_id`` and ``key`` must be non-empty strings and
    ``at`` a non-boolean non-negative integer moment, else
    ``ValueError`` before the ledger is read. Only the dispatch ledger
    is touched, under its exclusive lock.

    The decision must be ``claimed`` (anything else is ``ValueError``)
    and its lease must be strictly expired at the moment (a moment not
    past the lease end raises ``PermissionError``). A successful
    recovery moves the decision back to ``ready`` and clears the owner
    and the lease without consuming an attempt, committed together with
    the audit event. Returns ``(decision, created)``; an equivalent
    replay returns the current decision with ``False`` and writes
    nothing, while the same key with a changed request raises
    ``ValueError``. An unknown job raises ``KeyError`` and a missing
    ledger ``FileNotFoundError``.
    """
    for value in (ledger, job_id, key):
        if not isinstance(value, str) or not value:
            raise ValueError("ledger, job_id and key must be non-empty "
                             "strings")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    store = _get_store(ledger)
    with store.lock:
        with _dispatch_lock(store.realpath):
            _store, decisions, events, old_bytes = _load_existing(ledger)
            decision = decisions.get(job_id)
            if decision is None:
                raise KeyError(job_id)

            if _replay(events, key, "recover", job_id, None, None, at,
                       None) is not None:
                return copy.deepcopy(decision), False
            if decision["state"] != "claimed":
                raise ValueError("only a claimed decision can be "
                                 "recovered")
            if at <= decision["lease_end"]:
                raise PermissionError("lease has not strictly expired yet")

            decision["state"] = "ready"
            decision["owner"] = None
            decision["lease_end"] = None
            events[key] = _audit_event(key, "recover", job_id, None, None,
                                       at, None, decision)
            _commit_ledger(store.realpath,
                           _canonical_bytes(decisions, events), old_bytes)
            return copy.deepcopy(decision), True
