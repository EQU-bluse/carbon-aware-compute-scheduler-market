"""Persistent job-cancellation ledger.

The ``carbon_market.cancellation`` module adds the formal revocation
path for jobs that were accepted and traded but never dispatched: a
traded job whose reservation is no longer needed is *cancelled* once,
freezing the cancellation moment, the reason and the exact resource
version its trade had selected, so the booked capacity can be released
and the job can never reach dispatch.

Two public calls share one independent cancellation ledger:

* :func:`cancel` registers the cancellation record, its idempotency
  request binding and an audit event in one synced atomic commit.
* :func:`get` answers the recorded cancellation for one job id.

Only a job that is accepted, has a recorded ``market.clear`` or
``market.clear_live`` trade and still has no dispatch decision of any
kind can be cancelled; the cancellation moment must not precede the
trade moment. The acceptance file, the supply file, the clearing
ledger and the dispatch ledger are read as one snapshot under their
shared locks together with the cancellation ledger's exclusive lock,
all taken in resolved real-path order, so a concurrent
``dispatch.commit`` (given the same ledger), a concurrent clearing or
a concurrent cancellation is always decided against one consistent
version and the same job can never gain both a first cancellation and
a first dispatch decision.

The ledger (``version``, ``cancellations``, ``idempotency`` and
``audit``, each section keyed by the idempotency key in code-point
order) is one compact UTF-8 JSON document with non-ASCII written
through, no negative-zero or non-finite number literals and exactly
one trailing newline; every read accepts that canonical form only.
"""

from __future__ import annotations

import contextlib
import copy
import json
import os
from typing import Any

from . import _lifecycle
from . import dispatch as _dispatch
from . import jobs as _jobs
from . import market as _market
from . import resources as _resources

__all__ = ["cancel", "get"]

_VERSION = 1
_ROOT_FIELDS = ("version", "cancellations", "idempotency", "audit")
_RECORD_FIELDS = ("job_id", "at", "reason", "selection")
_SELECTION_FIELDS = ("resource_id", "version")
_EVENT_FIELDS = ("key", "request", "result")
_REQUEST_FIELDS = ("job_id", "at", "reason")

# The in-process mutex registry, the companion flock, the small input
# checks and the synced atomic commit live in the shared lifecycle
# infrastructure; the names are kept as the module's own seams.
_Store = _lifecycle.Store
_get_store = _lifecycle.get_store
_is_plain_int = _lifecycle.is_plain_int
_check_sorted_keys = _lifecycle.check_sorted_keys
_lock = _lifecycle.file_lock


def _validate_selection(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) \
            or set(value.keys()) != set(_SELECTION_FIELDS):
        raise ValueError("cancellation selection has invalid fields")
    resource_id = value["resource_id"]
    version = value["version"]
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("cancellation resource_id must be a non-empty "
                         "string")
    if not _is_plain_int(version) or version < 1:
        raise ValueError("cancellation version must be a positive integer")
    return {"resource_id": resource_id, "version": version}


def _validate_request(request: object) -> dict[str, Any]:
    if not isinstance(request, dict) \
            or set(request.keys()) != set(_REQUEST_FIELDS):
        raise ValueError("cancellation request has invalid fields")
    job_id = request["job_id"]
    at = request["at"]
    reason = request["reason"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("cancellation request job_id must be a non-empty "
                         "string")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("cancellation request at must be a non-boolean "
                         "non-negative integer")
    if not isinstance(reason, str) or not reason:
        raise ValueError("cancellation request reason must be a non-empty "
                         "string")
    return {"job_id": job_id, "at": at, "reason": reason}


def _validate_record(
    record: object,
    accepted: dict[str, dict[str, Any]] | None = None,
    trades: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if not isinstance(record, dict) \
            or set(record.keys()) != set(_RECORD_FIELDS):
        raise ValueError("cancellation record has invalid fields")
    job_id = record["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("cancellation job_id must be a non-empty string")
    at = record["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("cancellation at must be a non-boolean "
                         "non-negative integer")
    reason = record["reason"]
    if not isinstance(reason, str) or not reason:
        raise ValueError("cancellation reason must be a non-empty string")
    selection = _validate_selection(record["selection"])

    # Accepted jobs and trades are immutable once written, so the frozen
    # references are revalidated against the current snapshots exactly
    # as the other ledgers revalidate theirs. A dispatch decision is
    # new state that may legitimately appear after the cancellation
    # (a commit that was never given this ledger), so it is a
    # write-time predicate of cancel(), never a ledger invariant.
    if accepted is not None and job_id not in accepted:
        raise ValueError("cancellation record must reference an accepted "
                         "job")
    if trades is not None:
        trade = trades.get(job_id)
        if trade is None:
            raise ValueError("cancellation record must reference a "
                             "recorded trade")
        if at < trade["at"]:
            raise ValueError("cancellation moment must not precede the "
                             "trade moment")
        if selection["resource_id"] != trade["resource_id"] \
                or selection["version"] != trade["version"]:
            raise ValueError("cancellation selection must be the trade's "
                             "selection")
    return {
        "job_id": job_id,
        "at": at,
        "reason": reason,
        "selection": selection,
    }


def _validate_ledger(
    data: object,
    accepted: dict[str, dict[str, Any]] | None = None,
    trades: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("cancellation ledger root must be an object with "
                         "keys version, cancellations, idempotency and "
                         "audit")
    if not _is_plain_int(data["version"]) or data["version"] != _VERSION:
        raise ValueError("unsupported cancellation ledger version")
    records_raw = data["cancellations"]
    idempotency_raw = data["idempotency"]
    audit_raw = data["audit"]
    if not isinstance(records_raw, dict) \
            or not isinstance(idempotency_raw, dict) \
            or not isinstance(audit_raw, dict):
        raise ValueError("cancellations, idempotency and audit must be "
                         "objects")
    _check_sorted_keys(records_raw, "cancellations")
    _check_sorted_keys(idempotency_raw, "idempotency")
    _check_sorted_keys(audit_raw, "audit")

    records: dict[str, dict[str, Any]] = {}
    cancelled_jobs: set[str] = set()
    for key, record_raw in records_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        record = _validate_record(record_raw, accepted, trades)
        if record["job_id"] in cancelled_jobs:
            raise ValueError("a job can only be cancelled once")
        cancelled_jobs.add(record["job_id"])
        records[key] = record

    idempotency: dict[str, dict[str, Any]] = {}
    for key, request_raw in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        request = _validate_request(request_raw)
        if key not in records:
            raise ValueError("idempotency entry must reference a recorded "
                             "cancellation")
        record = records[key]
        if any(request[field] != record[field]
               for field in _REQUEST_FIELDS):
            raise ValueError("idempotency entry does not match its "
                             "cancellation record")
        idempotency[key] = request

    events: dict[str, dict[str, Any]] = {}
    for key, event_raw in audit_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event_raw, dict) \
                or set(event_raw.keys()) != set(_EVENT_FIELDS):
            raise ValueError("cancellation audit event has invalid fields")
        if event_raw["key"] != key or not isinstance(event_raw["key"], str):
            raise ValueError("audit event key does not match its map key")
        request = _validate_request(event_raw["request"])
        if request != idempotency.get(key):
            raise ValueError("audit event does not match its idempotency "
                             "entry")
        result = _validate_record(event_raw["result"], accepted, trades)
        if result != records[key]:
            raise ValueError("audit event result does not match its record")
        events[key] = {"key": key, "request": request, "result": result}

    if set(events) != set(records) or set(idempotency) != set(records):
        raise ValueError("cancellation sections do not match")
    return records, idempotency, events


def _canonical_bytes(
    records: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, sections in their
    # fixed field order and all sections keyed by idempotency key in
    # code-point order, terminated by exactly one newline.
    payload = {
        "version": _VERSION,
        "cancellations": {key: records[key] for key in sorted(records)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
        "audit": {key: events[key] for key in sorted(events)},
    }
    text = json.dumps(payload, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_cancellation_ledger(
    realpath: str,
    accepted: dict[str, dict[str, Any]] | None = None,
    trades: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]], bytes | None]:
    def validate(
        data: object,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
               dict[str, dict[str, Any]]]:
        return _validate_ledger(data, accepted, trades)

    def canonical(
        sections: tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
                        dict[str, dict[str, Any]]],
    ) -> bytes:
        records, idempotency, events = sections
        return _canonical_bytes(records, idempotency, events)

    sections, raw = _lifecycle.load_canonical(
        realpath, "cancellation ledger", validate, canonical)
    if sections is None:
        return {}, {}, {}, None
    records, idempotency, events = sections
    return records, idempotency, events, raw


def _validate_records(
    records: dict[str, dict[str, Any]],
    accepted: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
) -> None:
    # Cross-check every record against the immutable business snapshots.
    # Readers that learn the cancellation map before the clearing ledger
    # is loaded (the capacity envelope needs the moments first) re-run
    # the reference validation once the trades are known.
    for record in records.values():
        _validate_record(record, accepted, trades)


def _fsync_directory(directory: str) -> None:
    _lifecycle.fsync_directory(directory)


def _rollback_file(realpath: str, directory: str, old_bytes: bytes | None,
                   first: BaseException) -> None:
    _lifecycle.rollback_file(realpath, directory, old_bytes, first,
                             prefix=".cancellation-restore-",
                             fsync_dir=_fsync_directory)


def _commit_file(realpath: str, payload: bytes,
                 old_bytes: bytes | None) -> None:
    # One durable commit for the record, the idempotency binding and the
    # audit event: synced same-directory temporary, atomic replace and a
    # directory fsync, restoring the pre-call bytes on any failure, so a
    # failed call leaves neither a fragment nor half an event and an
    # interruption observes only the pre- or post-commit bytes.
    _lifecycle.commit_file(realpath, payload, old_bytes,
                           prefix=".cancellation-",
                           fsync_dir=_fsync_directory)


def _record_for_job(
    records: dict[str, dict[str, Any]],
    job_id: str,
) -> dict[str, Any] | None:
    for record in records.values():
        if record["job_id"] == job_id:
            return record
    return None


def cancel(
    jobs: str,
    supply: str,
    trades: str,
    dispatch: str,
    cancellations: str,
    job_id: str,
    key: str,
    at: int,
    reason: str,
) -> tuple[dict[str, object], bool]:
    """Cancel one traded, never-dispatched job idempotently.

    ``jobs``, ``supply``, ``trades``, ``dispatch`` and ``cancellations``
    paths, ``job_id``, ``key`` and ``reason`` must be non-empty strings
    and ``at`` a non-boolean non-negative integer cancellation moment;
    the five paths must also resolve to distinct real locations. Any
    violation raises ``ValueError`` before a business file is read.

    The ``jobs.submit`` acceptance file, the resource supply file, the
    clearing ledger and the dispatch ledger are read as one snapshot
    under their shared locks together with the cancellation ledger's
    exclusive lock, all five taken in resolved real-path order, so a
    concurrent :func:`carbon_market.dispatch.commit` given the same
    cancellation ledger, a concurrent clearing or a concurrent
    cancellation observes one consistent version and the same job can
    never gain both a first cancellation and a first dispatch decision.

    Only a job that is accepted, has a recorded trade and still has no
    dispatch decision of any kind cancels: an unknown job raises
    ``KeyError``, a job without a recorded trade raises
    ``LookupError`` and a job with any dispatch decision raises
    ``PermissionError``. A cancellation moment earlier than the trade
    moment raises ``ValueError``. None of these creates the ledger.

    A first success freezes, in order, the job id, the cancellation
    moment, the non-empty reason and the trade's selection (resource id
    and version), and commits the record, the idempotency binding (the
    complete request) and one audit event in a single synced atomic
    write; the call returns ``(record, True)``. Replaying the same key
    with the same request returns the stored record with ``False``
    without rewriting a byte; the same key bound to another request, or
    the same job cancelled under another key, raises ``ValueError`` and
    leaves every ledger untouched. A refused or failed request never
    creates a cancellation record or changes an existing ledger.

    Missing input files or the cancellation ledger parent raise
    ``FileNotFoundError``; invalid arguments, structure, ordering,
    references or non-canonical bytes raise ``ValueError``; other
    locking, read/write or sync failures raise ``OSError``.
    """
    for value in (jobs, supply, trades, dispatch, cancellations,
                  job_id, key, reason):
        if not isinstance(value, str) or not value:
            raise ValueError("the five paths, job_id, key and reason must "
                             "be non-empty strings")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    job_real = os.path.realpath(jobs)
    supply_real = os.path.realpath(supply)
    trades_real = os.path.realpath(trades)
    dispatch_real = os.path.realpath(dispatch)
    cancellation_real = os.path.realpath(cancellations)
    business_reals = {job_real, supply_real, trades_real, dispatch_real,
                      cancellation_real}
    if len(business_reals) != 5:
        raise ValueError("the five paths must be distinct real paths")
    # Completion ledgers beside the snapshots complete the trades
    # ledger's capacity envelope; they are discovered before locking and
    # shared in the same global order, exactly as dispatch.commit does.
    completion_reals = set(
        _market._discover_completion_paths(tuple(sorted(business_reals))))

    store = _get_store(cancellation_real)
    with store.lock:
        # Locks are taken in one resolved-real-path order shared by
        # every caller, so concurrent cancellations, commits and
        # clearings can never deadlock; the cancellation ledger lock is
        # exclusive, every other lock shared.
        with contextlib.ExitStack() as stack:
            for locked in sorted(business_reals | completion_reals):
                stack.enter_context(
                    _lock(locked, shared=(locked != cancellation_real)))

            accepted, job_map, job_events, job_raw = \
                _jobs._load_submit_file(job_real)
            if job_raw is None:
                raise FileNotFoundError(
                    f"acceptance file {job_real!r} does not exist")
            # Like the supply file and the ledgers, the acceptance
            # snapshot must be in the canonical compact form
            # jobs.submit writes.
            if job_raw != _jobs._serialize_submit_file(
                    accepted, job_map, job_events):
                raise ValueError(
                    f"acceptance file {job_real!r} is not in canonical "
                    "compact form")
            history, _supply_map, _supply_events, supply_raw = \
                _resources._load_file(supply_real)
            if supply_raw is None:
                raise FileNotFoundError(
                    f"supply file {supply_real!r} does not exist")

            records, idempotency, events, old_bytes = \
                _load_cancellation_ledger(cancellation_real)
            # The cancellation moments feed the trades ledger's capacity
            # envelope, so a clearing that legitimately reused released
            # capacity still validates here.
            cancelled = {record["job_id"]: record["at"]
                         for record in records.values()}
            cleared, _clear_keys, trades_raw = _market._load_clear_ledger(
                trades_real, accepted, history,
                completion_paths=sorted(completion_reals),
                cancelled=cancelled)
            if trades_raw is None:
                raise FileNotFoundError(
                    f"clearing ledger {trades_real!r} does not exist")
            # A missing dispatch ledger simply holds no decisions yet.
            decisions, _dispatch_keys, _dispatch_events, _dispatch_raw = \
                _dispatch._load_ledger(dispatch_real)
            # The existing records must still describe this exact
            # acceptance and clearing snapshot.
            _validate_records(records, accepted, cleared)

            request = {"job_id": job_id, "at": at, "reason": reason}
            binding = idempotency.get(key)
            if binding is not None:
                if binding != request:
                    raise ValueError("idempotency key was already used "
                                     "with a different request")
                # An equivalent replay returns the stored record without
                # rechecking the predicate or rewriting a byte.
                return copy.deepcopy(records[key]), False

            if job_id not in accepted:
                raise KeyError(job_id)
            if _record_for_job(records, job_id) is not None:
                raise ValueError("the job is already cancelled under "
                                 "another idempotency key")
            trade = cleared.get(job_id)
            if trade is None:
                raise LookupError("job has no recorded trade")
            if job_id in decisions:
                raise PermissionError("the job already has a dispatch "
                                      "decision")
            if at < trade["at"]:
                raise ValueError("cancellation moment must not precede "
                                 "the trade moment")

            record: dict[str, Any] = {
                "job_id": job_id,
                "at": at,
                "reason": reason,
                "selection": {"resource_id": trade["resource_id"],
                              "version": trade["version"]},
            }
            # Full independent validation against the snapshot before
            # the commit is accepted.
            _validate_record(record, accepted, cleared)
            records[key] = record
            idempotency[key] = dict(request)
            events[key] = {"key": key, "request": dict(request),
                           "result": copy.deepcopy(record)}
            _commit_file(cancellation_real,
                         _canonical_bytes(records, idempotency, events),
                         old_bytes)
            return copy.deepcopy(record), True


def get(cancellations: str, job_id: str) -> dict[str, object]:
    """Return the recorded cancellation for ``job_id``.

    ``cancellations`` and ``job_id`` must be non-empty strings, else
    ``ValueError``. The cancellation ledger is read under its shared
    lock and never modified. A job that was never cancelled -- an
    unknown job id, a job without a record or a missing cancellation
    ledger -- raises ``KeyError(job_id)``; invalid structure, ordering
    or non-canonical bytes raise ``ValueError``; other locking or I/O
    failures raise ``OSError``. The returned dict is a fresh copy in
    the record's fixed field order.
    """
    for value in (cancellations, job_id):
        if not isinstance(value, str) or not value:
            raise ValueError("cancellations and job_id must be non-empty "
                             "strings")

    realpath = os.path.realpath(cancellations)
    with _lock(realpath, shared=True):
        records, _idempotency, _events, _raw = \
            _load_cancellation_ledger(realpath)
    record = _record_for_job(records, job_id)
    if record is None:
        raise KeyError(job_id)
    return copy.deepcopy(record)
