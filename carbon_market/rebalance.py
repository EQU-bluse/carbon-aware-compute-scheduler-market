"""Persistent re-evaluation and migration advice for booked trades.

The market already books a job against one exact resource version
(``market.clear``/``market.clear_live``), dispatch commits and claims
that decision and the execution layer runs launch or migration plans,
while the live signal ledger keeps publishing fresh region observations.
What is missing until this module is a re-evaluation of an unfinished
booking: given the signals valid *now*, should the job stay on the
resource version its trade froze, or migrate to another resource whose
current version is cleaner or cheaper right now?

The module exposes two public calls. :func:`evaluate` reads the seven
layers -- the ``jobs.submit`` acceptance file, the resource supply file,
the live signal file, the ``market.clear`` clearing ledger, the dispatch
ledger, the execution ledger and this module's advice ledger -- as one
snapshot under their shared locks together with the advice ledger's
exclusive lock, all seven taken in resolved real-path order, so
concurrent calls across processes can never deadlock.

The re-evaluation rules are deliberately asymmetric between the slot
the job already occupies and every alternative:

* the retained item always uses the exact resource version the trade
  froze, even once that version's own supply window has closed -- the
  job is already running on it;
* a migration item uses only another resource's highest version valid
  at the evaluation moment, never another version of the same resource;
* every item prices on the region's latest signal observed no later
  than the moment and not expired at it, and keeps the job's region,
  residency, deadline, cost and carbon budgets;
* capacity is deducted per exact resource version from every trade
  other jobs booked, while this job's own frozen occupancy is never
  deducted twice.

Feasible items are ordered by signal carbon intensity, then signal unit
cost, then resource id. When the first item is still the traded version
the advice is ``keep``; otherwise it is ``migrate`` to that first item.

Advice is refused while the booking is past re-evaluation: a succeeded
dispatch decision or a completed execution plan raises ``ValueError``,
an active execution plan raises ``PermissionError`` and an evaluation
moment past the job deadline raises ``TimeoutError`` -- in that order,
and none of them writes advice. An unknown job or dispatch decision
raises ``KeyError`` and a job without a trade, or one with no feasible
item, raises ``LookupError``; neither creates the ledger.

:func:`apply` turns one ``migrate`` advice into a persisted
re-reservation. It reads the same seven layers as one consistent
snapshot -- the seven inputs under shared locks, this time including
the advice ledger, and the intent ledger under its exclusive lock, all
eight taken in resolved real-path order -- and recomputes the candidate
set at the reservation moment under the same region, residency, budget,
deadline and ordering rules, deducting capacity per exact resource
version for every trade and every other job's intent while never
deducting this job's own source occupancy against itself. The advice
target must still be the first feasible candidate: a changed target, an
empty feasible set or insufficient remaining capacity raises
``LookupError`` and reserves nothing. Only an unfinished, unclaimed
booking may be re-reserved: a succeeded dispatch decision or a
completed execution plan raises ``ValueError``, a claimed decision or
an active plan raises ``PermissionError`` and a reservation moment past
the job deadline raises ``TimeoutError`` -- none of them writes. A
successful call freezes the job, the advice key, the moment, the source
and target selections, the target's supply and signal records, the
dispatch and execution states and the ``reserved`` state into the
intent ledger.

The advice ledger (version 1) holds ``records`` keyed by idempotency
key and one ``audit`` event per first-served key; both sections are
ordered by key code point. A first evaluation returns
``(record, True)``; replaying the same key with the same job and
evaluation moment returns the stored record with ``False`` without
writing, while the same key with a changed request raises
``ValueError``.

The intent ledger (version 1) holds ``intents`` keyed by job id and
ordered by job id code point, ``idempotency`` bindings ordered by key
code point and one ``audit`` event per first-served key binding the
complete request and the committed intent. A first reservation returns
``(record, True)``; replaying the same key with the same job, advice
key and moment returns the stored record with ``False`` without
writing, while the same key with a changed request, or a job already
reserved under another key, raises ``ValueError``.

:func:`start`, :func:`record` and :func:`recover` close the migration
loop over the reserved intents. :func:`start` claims one intent that is
still ``reserved`` -- neither finished nor occupied -- and freezes the
intent's source and target resource versions into a migration plan
bound to an owner and a lease end that must not pass the job deadline;
the target capacity stays exclusively held by the intent for the whole
execution. :func:`record` lets the current owner, inside the lease,
commit the non-empty receipt of the next pending step -- ``copy`` then
``switch``, never skipped or repeated: a successful ``switch`` turns
the plan ``migrated`` and releases the source capacity the trade froze,
while any ``failed`` step turns it ``failed`` and releases the target
reservation immediately, leaving the source trade occupancy untouched.
:func:`recover` turns an active plan whose lease has strictly expired
into ``interrupted``, preserving every receipt and releasing the target
reservation. Capacity accounting recognizes the plan states: a
``reserved`` or ``active`` intent occupies both source and target, a
``migrated`` one only the target, a ``failed`` or ``interrupted`` one
only the source.

The three calls share one idempotency key space with :func:`apply`:
replaying a key with the same request returns the current plan snapshot
with ``False`` and writes nothing, while the same key with a changed
request raises ``ValueError``. The intent and plan histories are kept
per job and per plan key: a reservation-only file stays at version 1,
and the first migration plan or settlement marker upgrades it in place
to the nested version-3 form, preserving every old intent, plan, step
receipt, binding and audit event while later rounds are appended,
never overwritten. Each first change commits the plan, its state, the
idempotency binding and the audit event atomically. Every read requires the on-disk bytes to be exactly the
canonical compact form :func:`_canonical_bytes` produces -- compact
UTF-8 JSON with non-ASCII written through, no negative-zero or
non-finite number literals and exactly one trailing newline -- and
every commit goes through a synced same-directory temporary file, an
atomic replace and a directory fsync, restoring the pre-call bytes on
failure, so an unsuccessful call leaves neither a temporary fragment
nor half an audit event.

:func:`settle` finally gives later dispatch and execution a current
occupancy that survives each terminal plan, without rewriting any of
the existing layers: it writes one independent settlement ledger
(loaded read-only by :func:`current`) and appends only a small
``settled`` marker to the intent ledger so reservations and later
migrations can see an open settlement. A job may run many migration
rounds; each plan key settles at most once, the generations run
continuously from the immutable trade (the first settlement is
generation 1, every later one the previous completed generation plus
one) and each settlement's ``before`` equals the previous completion's
``after``. A ``migrated`` plan confirms the target version as the
current binding, while a ``failed`` or ``interrupted`` plan compensates
and keeps the source version. Each settlement lands through a
crash-safe sequence -- a pending intent marker, then a durable
``pending`` record, binding and audit event, then the final record and
the settled marker -- so a crash at any stage is resumed by the same
key carrying the same request, which completes the missing stage and
still returns ``True``; only an already fully completed same-key
request returns the record with ``False`` without writing a byte.
:func:`current` ignores pending settlements and returns the job's most
recent completed active or compensated binding (or the trade with
generation 0 when none completed).
"""

from __future__ import annotations

import contextlib
import copy
import fcntl
import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any, Iterator

from . import dispatch as _dispatch
from . import execution as _execution
from . import jobs as _jobs
from . import market as _market
from . import resources as _resources
from . import signals as _signals
from ._jsonio import finite_loads

__all__ = ["evaluate", "apply", "start", "record", "recover", "settle",
           "current"]

_VERSION = 1
_ROOT_FIELDS = ("version", "records", "audit")
_RECORD_FIELDS = ("job_id", "at", "current", "dispatch", "execution",
                  "candidates", "recommendation", "target")
_SELECTION_FIELDS = ("resource_id", "version")
_CANDIDATE_FIELDS = ("resource", "signal", "total_cost", "total_carbon")
_EVENT_FIELDS = ("key", "request", "result")
_REQUEST_FIELDS = ("job_id", "at")
_DISPATCH_STATES = ("ready", "claimed", "succeeded", "failed")
_EXECUTION_STATES = ("none", "active", "completed", "failed",
                     "interrupted")
_RECOMMENDATIONS = ("keep", "migrate")
_LOCK_SUFFIX = ".lock"

_INTENT_VERSION = 1
_INTENT_VERSION_V2 = 2
_INTENT_VERSION_V3 = 3
_INTENT_ROOT_FIELDS = ("version", "intents", "idempotency", "audit")
_INTENT_ROOT_FIELDS_V2 = ("version", "intents", "plans", "idempotency",
                          "audit")
_INTENT_ROOT_FIELDS_V3 = ("version", "intents", "plans", "settled",
                          "idempotency", "audit")
_INTENT_FIELDS = ("job_id", "advice_key", "at", "source", "target",
                  "supply", "signal", "dispatch", "execution", "reserved")
_INTENT_REQUEST_FIELDS = ("job_id", "advice_key", "at")
_RESERVED_STATES = ("reserved",)
_PLAN_FIELDS = ("job_id", "source", "target", "owner", "lease_end", "at",
                "state", "steps")
_RECEIPT_FIELDS = ("step", "result", "receipt", "at")
_PLAN_STEPS = ("copy", "switch")
_PLAN_STATES = ("active", "migrated", "failed", "interrupted")
_STEP_RESULTS = ("succeeded", "failed")
_ACTION_REQUEST_FIELDS = {
    "start": ("action", "job_id", "owner", "lease_end", "at"),
    "record": ("action", "job_id", "owner", "step", "result", "receipt",
               "at"),
    "recover": ("action", "job_id", "owner", "at"),
}


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
def _lock(realpath: str, *, shared: bool = False) -> Iterator[None]:
    # As in the other ledgers, the companion lock file is never unlinked
    # and an flock is released by the kernel on process exit, so
    # equivalent real paths share one lock across threads, processes and
    # modules.
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


def _validate_selection(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) \
            or set(value.keys()) != set(_SELECTION_FIELDS):
        raise ValueError(f"{label} has invalid fields")
    resource_id = value["resource_id"]
    version = value["version"]
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError(f"{label} resource_id must be a non-empty string")
    if not _is_plain_int(version) or version < 1:
        raise ValueError(f"{label} version must be a positive integer")
    return {"resource_id": resource_id, "version": version}


def _validate_candidate(
    entry: object,
    work: int,
    at: int,
    job: dict[str, Any],
    current: dict[str, Any],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]],
    seen: set[str],
) -> tuple[int, int, str]:
    if not isinstance(entry, dict) \
            or set(entry.keys()) != set(_CANDIDATE_FIELDS):
        raise ValueError("advice candidate has invalid fields")
    resource = entry["resource"]
    signal = entry["signal"]
    if not isinstance(resource, dict) \
            or set(resource.keys()) != set(_resources._RECORD_FIELDS):
        raise ValueError("advice candidate resource has invalid fields")
    resource_id = resource["resource_id"]
    version = resource["version"]
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("advice candidate resource_id must be a non-empty "
                         "string")
    if not _is_plain_int(version) or version < 1:
        raise ValueError("advice candidate version must be a positive "
                         "integer")
    # The versioned record must be the exact, immutable supply record.
    records = history.get(resource_id)
    if records is None or version > len(records) \
            or dict(resource) != records[version - 1]:
        raise ValueError("advice candidate must reference a published "
                         "resource version")
    if resource_id in seen:
        raise ValueError("advice candidates must be distinct resources")
    seen.add(resource_id)
    # The retained item is the trade's frozen version; a migration item
    # may only be another resource's highest version valid at at -- never
    # a different version of the current resource.
    if resource_id == current["resource_id"]:
        if version != current["version"]:
            raise ValueError("advice retained candidate must use the "
                             "trade's frozen resource version")
    else:
        # The construction path picks the resource's highest version
        # valid at at; like a frozen trade candidate, a later supply
        # publication must not invalidate the recorded advice, so on
        # readback only the frozen version's own window is checked.
        if not resource["start"] <= at <= resource["end"]:
            raise ValueError("advice migration candidate must be valid at "
                             "the evaluation moment")
    if resource["region"] not in set(job["regions"]):
        raise ValueError("advice candidate is outside the job's regions")
    if not set(job["residency"]) <= set(resource["residency"]):
        raise ValueError("advice candidate does not cover job residency")
    if resource["end"] < job["deadline"]:
        raise ValueError("advice candidate does not cover the job deadline")
    # Validate a copy: _check_values normalizes the mix order in place,
    # and the parsed record's raw key order must survive for the
    # canonical-byte comparison.
    if not isinstance(signal, dict) \
            or set(signal.keys()) != set(_signals._RECORD_FIELDS):
        raise ValueError("advice candidate signal has invalid fields")
    signal_copy = dict(signal)
    signal_copy["mix"] = dict(signal["mix"])
    _signals._check_values(signal_copy)
    if signal["region"] != resource["region"]:
        raise ValueError("advice candidate signal must cover the resource "
                         "region")
    signal_records = signal_history.get(resource["region"])
    if signal_records is None or signal["version"] > len(signal_records) \
            or dict(signal) != signal_records[signal["version"] - 1]:
        raise ValueError("advice candidate signal must reference a "
                         "published signal version")
    if not (signal["observed"] <= at <= signal["expires"]):
        raise ValueError("advice candidate signal must be valid at the "
                         "evaluation moment")
    total_cost = entry["total_cost"]
    total_carbon = entry["total_carbon"]
    if not _is_plain_int(total_cost) or total_cost < 0 \
            or total_cost != work * signal["unit_cost"]:
        raise ValueError("advice candidate total_cost is invalid")
    if not _is_plain_int(total_carbon) or total_carbon < 0 \
            or total_carbon != work * signal["carbon_intensity"]:
        raise ValueError("advice candidate total_carbon is invalid")
    if total_cost > job["max_cost"] or total_carbon > job["carbon_cap"]:
        raise ValueError("advice candidate exceeds a job budget")
    # Capacity feasibility is a decision-time fact: later trades may
    # legitimately fill the version, so like the clearing ledger's
    # per-candidate snapshot it is enforced at construction, not on
    # readback; advice itself books no capacity.
    return signal["carbon_intensity"], signal["unit_cost"], resource_id


def _validate_record(
    record: object,
    accepted: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    if not isinstance(record, dict) \
            or set(record.keys()) != set(_RECORD_FIELDS):
        raise ValueError("advice record has invalid fields")
    job_id = record["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("advice job_id must be a non-empty string")
    at = record["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("advice at must be a non-boolean non-negative "
                         "integer")
    current = _validate_selection(record["current"], "advice current "
                                  "selection")
    dispatch = record["dispatch"]
    if dispatch not in _DISPATCH_STATES:
        raise ValueError("advice dispatch state is invalid")
    execution = record["execution"]
    if execution not in _EXECUTION_STATES:
        raise ValueError("advice execution state is invalid")
    recommendation = record["recommendation"]
    if recommendation not in _RECOMMENDATIONS:
        raise ValueError("advice recommendation is invalid")
    target = _validate_selection(record["target"], "advice target")

    job = accepted.get(job_id)
    if job is None:
        raise ValueError("advice record must reference an accepted job")
    trade = trades.get(job_id)
    if trade is None:
        raise ValueError("advice record must reference a recorded trade")
    if at > job["deadline"]:
        raise ValueError("advice moment must not pass the job deadline")
    if current != {"resource_id": trade["resource_id"],
                   "version": trade["version"]}:
        raise ValueError("advice current selection must match the trade")
    # The dispatch and execution states are frozen point-in-time facts:
    # a decision keeps evolving after the advice, so the record is
    # checked for the state vocabulary only, never against the current
    # ledgers (the same way an execution audit snapshot keeps the state
    # its action committed).

    candidates_raw = record["candidates"]
    if not isinstance(candidates_raw, list) or not candidates_raw:
        raise ValueError("advice candidates must be a non-empty list")
    candidates: list[dict[str, Any]] = []
    last_rank: tuple[int, int, str] | None = None
    seen: set[str] = set()
    for entry_raw in candidates_raw:
        carbon, unit_cost, resource_id = _validate_candidate(
            entry_raw, job["work"], at, job, current, history,
            signal_history, seen)
        rank = (carbon, unit_cost, resource_id)
        if last_rank is not None and rank < last_rank:
            raise ValueError("advice candidates must be ordered by signal "
                             "carbon intensity, unit cost and resource id")
        last_rank = rank
        candidates.append(copy.deepcopy(entry_raw))

    first = candidates[0]["resource"]
    first_selection = {"resource_id": first["resource_id"],
                       "version": first["version"]}
    if target != first_selection:
        raise ValueError("advice target must be the first ordered "
                         "candidate")
    if recommendation == "keep":
        if target != current:
            raise ValueError("a keep advice must target the current "
                             "resource version")
    elif target == current:
        raise ValueError("a migrate advice must target another resource "
                         "version")

    return {
        "job_id": job_id,
        "at": at,
        "current": current,
        "dispatch": dispatch,
        "execution": execution,
        "candidates": candidates,
        "recommendation": recommendation,
        "target": target,
    }


def _validate_request(request: object) -> dict[str, Any]:
    if not isinstance(request, dict) \
            or set(request.keys()) != set(_REQUEST_FIELDS):
        raise ValueError("advice request has invalid fields")
    job_id = request["job_id"]
    at = request["at"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("advice request job_id must be a non-empty string")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("advice request at must be a non-boolean "
                         "non-negative integer")
    return {"job_id": job_id, "at": at}


def _validate_ledger(
    data: object,
    accepted: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("advice ledger root must be an object with keys "
                         "version, records and audit")
    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported advice ledger version")

    records_raw = data["records"]
    audit_raw = data["audit"]
    if not isinstance(records_raw, dict) or not isinstance(audit_raw, dict):
        raise ValueError("records and audit must be objects")
    _check_sorted_keys(records_raw, "records")
    _check_sorted_keys(audit_raw, "audit")

    # Capacity already sold per exact resource version by every trade is
    # a clearing-ledger invariant already enforced when that ledger is
    # loaded; advice books nothing itself, so the advice ledger only
    # rechecks the frozen supply/signal references, arithmetic and order.
    records: dict[str, dict[str, Any]] = {}
    for key, record_raw in records_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        record = _validate_record(
            record_raw, accepted, trades, history, signal_history)
        records[key] = record

    events: dict[str, dict[str, Any]] = {}
    for key, event_raw in audit_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event_raw, dict) \
                or set(event_raw.keys()) != set(_EVENT_FIELDS):
            raise ValueError("advice audit event has invalid fields")
        if event_raw["key"] != key or not isinstance(event_raw["key"], str):
            raise ValueError("audit event key does not match its map key")
        request = _validate_request(event_raw["request"])
        result = _validate_record(
            event_raw["result"], accepted, trades, history,
            signal_history)
        if key not in records:
            raise ValueError("audit event must reference a recorded "
                             "advice record")
        if result != records[key]:
            raise ValueError("audit event result does not match its "
                             "record")
        if request["job_id"] != result["job_id"] \
                or request["at"] != result["at"]:
            raise ValueError("audit event request does not match its "
                             "record")
        events[key] = {"key": key, "request": request, "result": result}

    # The two sections describe one advice history: one audit event per
    # record and vice versa.
    if set(events) != set(records):
        raise ValueError("advice records and audit events do not match")
    return records, events


def _canonical_bytes(
    records: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, sections in their
    # fixed field order and records/events keyed by idempotency key in
    # code-point order, terminated by exactly one newline.
    payload = {
        "version": _VERSION,
        "records": {key: records[key] for key in sorted(records)},
        "audit": {key: events[key] for key in sorted(events)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_ledger(
    realpath: str,
    accepted: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return {}, {}, None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"advice ledger {realpath!r} is not valid UTF-8") from exc
    try:
        # Negative-zero and non-finite literals are format errors.
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"advice ledger {realpath!r} is not valid JSON") from exc
    records, events = _validate_ledger(
        data, accepted, trades, history, signal_history)
    # As for the other ledgers, the ledger is accepted only in canonical
    # compact form with a single trailing newline.
    if raw != _canonical_bytes(records, events):
        raise ValueError(
            f"advice ledger {realpath!r} is not in canonical compact form")
    return records, events, raw


def _fsync_directory(directory: str) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _rollback_file(realpath: str, directory: str, old_bytes: bytes | None,
                   first: BaseException,
                   prefix: str = ".rebalance-") -> None:
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
                dir=directory, prefix=prefix + "restore-", suffix=".tmp")
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


def _commit_file(realpath: str, payload: bytes,
                 old_bytes: bytes | None,
                 prefix: str = ".rebalance-") -> None:
    # One durable commit for the record and its audit event: synced
    # same-directory temporary, atomic replace and a directory fsync,
    # restoring the pre-call bytes on any failure, so an unsuccessful
    # evaluation leaves neither a temporary fragment nor half an audit
    # event.
    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=prefix, suffix=".tmp")
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
        _rollback_file(realpath, directory, old_bytes, first, prefix)
        raise


def _latest_signal(
    signal_history: dict[str, list[dict[str, Any]]],
    region: str,
    at: int,
) -> dict[str, Any] | None:
    # The region's latest observation covering the moment; when several
    # windows cover it the highest version wins.
    current: dict[str, Any] | None = None
    for signal in signal_history.get(region, ()):
        if signal["observed"] <= at <= signal["expires"]:
            current = signal
    if current is None:
        return None
    signal_copy = dict(current)
    signal_copy["mix"] = dict(current["mix"])
    return signal_copy


def evaluate(
    jobs: str,
    supply: str,
    signals: str,
    trades: str,
    dispatch: str,
    execution: str,
    ledger: str,
    job_id: str,
    key: str,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Re-evaluate one unfinished trade and advise keep or migrate.

    The seven paths, ``job_id`` and ``key`` must be non-empty strings
    and ``at`` a non-boolean non-negative integer evaluation moment; the
    seven paths must also resolve to distinct real locations. Any
    violation raises ``ValueError`` before a business file is read.

    The acceptance, supply, signal, clearing, dispatch and execution
    files are read as one snapshot under their shared locks together
    with the advice ledger's exclusive lock, all seven taken in
    resolved real-path order. The retained candidate is the exact
    resource version the job's trade froze; every migration candidate is
    another resource's highest supply version valid at ``at``. Each
    candidate must sit in one of the job's regions, cover its residency,
    stay available until the deadline and price within both budgets on
    the region's latest signal valid at ``at`` -- a region without such
    a signal contributes nothing. Capacity is deducted per exact
    resource version for every trade other jobs booked; this job's own
    trade is never deducted against itself. Candidates are ordered by
    signal carbon intensity, signal unit cost and resource id.

    Advice is refused, in this order, when the dispatch decision has
    already succeeded or any execution plan has completed
    (``ValueError``), when an execution plan is still active
    (``PermissionError``) and when ``at`` is past the job deadline
    (``TimeoutError``); none of these writes. With no feasible candidate
    the call raises ``LookupError`` without writing.

    Returns ``(record, created)``. The record freezes, in order, the job
    id, the evaluation moment, the current selection, the dispatch and
    execution states, the ordered candidates, the recommendation
    (``keep`` when the first candidate is the current version, otherwise
    ``migrate``) and the target selection. A missing advice ledger is
    created only by the first record, the record and its audit event
    committed together in one synced atomic write. Replaying the same
    key with the same job and moment returns the stored record with
    ``False`` without writing; the same key with a changed request
    raises ``ValueError`` and leaves the ledger untouched.

    An unknown job or dispatch decision raises ``KeyError``; a job
    without a trade and an empty feasible set raise ``LookupError``;
    neither creates the ledger. Missing input files or the ledger parent
    raise ``FileNotFoundError``; invalid arguments, structure, ordering,
    references or non-canonical bytes raise ``ValueError``; other
    locking, read/write or sync failures raise ``OSError``.
    """
    for value in (jobs, supply, signals, trades, dispatch, execution,
                  ledger, job_id, key):
        if not isinstance(value, str) or not value:
            raise ValueError("the seven paths, job_id and key must be "
                             "non-empty strings")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    job_real = os.path.realpath(jobs)
    supply_real = os.path.realpath(supply)
    signal_real = os.path.realpath(signals)
    trades_real = os.path.realpath(trades)
    dispatch_real = os.path.realpath(dispatch)
    execution_real = os.path.realpath(execution)
    ledger_real = os.path.realpath(ledger)
    all_paths = (job_real, supply_real, signal_real, trades_real,
                 dispatch_real, execution_real, ledger_real)
    if len(set(all_paths)) != 7:
        raise ValueError("the seven paths must be distinct real paths")

    store = _get_store(ledger)
    with store.lock:
        # Locks are taken in one resolved-real-path order shared by every
        # caller, so concurrent evaluations can never deadlock; the
        # advice ledger lock is exclusive, the six snapshot locks shared.
        with contextlib.ExitStack() as stack:
            for locked in sorted(set(all_paths)):
                stack.enter_context(
                    _lock(locked, shared=(locked != ledger_real)))

            accepted, job_map, job_events, job_raw = \
                _jobs._load_submit_file(job_real)
            if job_raw is None:
                raise FileNotFoundError(
                    f"acceptance file {job_real!r} does not exist")
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
            signal_history, _signal_map, _signal_events, signal_raw = \
                _signals._load_file(signal_real)
            if signal_raw is None:
                raise FileNotFoundError(
                    f"signal file {signal_real!r} does not exist")
            # Live trades freeze signal versions, so the signal history
            # resolves their references; static trades validate as before.
            cleared, _clear_keys, trades_raw = _market._load_clear_ledger(
                trades_real, accepted, history, signal_history)
            if trades_raw is None:
                raise FileNotFoundError(
                    f"clearing ledger {trades_real!r} does not exist")
            decisions, _dispatch_keys, _dispatch_events, dispatch_raw = \
                _dispatch._load_ledger(dispatch_real)
            if dispatch_raw is None:
                raise FileNotFoundError(
                    f"dispatch ledger {dispatch_real!r} does not exist")
            # The execution ledger is a required input, as in the other
            # layers that read it: advice presupposes a claim, and the
            # natural re-evaluation point is a job whose prior execution
            # attempt failed or was interrupted (its plan history frozen
            # in this ledger). A missing file is FileNotFoundError.
            plans, _plan_keys, _plan_events = \
                _execution._load_existing_ledger(execution_real)[:3]
            records, events, old_bytes = _load_ledger(
                ledger_real, accepted, cleared, history, signal_history)

            request = {"job_id": job_id, "at": at}
            binding = records.get(key)
            if binding is not None:
                if binding["job_id"] != job_id or binding["at"] != at:
                    raise ValueError("idempotency key was already used with "
                                     "a different request")
                # An equivalent replay returns the stored record without
                # re-evaluating or rewriting a byte.
                return copy.deepcopy(binding), False

            job = accepted.get(job_id)
            if job is None:
                raise KeyError(job_id)
            trade = cleared.get(job_id)
            if trade is None:
                raise LookupError("job has no recorded trade")
            decision = decisions.get(job_id)
            if decision is None:
                raise KeyError(job_id)

            job_plans = plans.get(job_id, {})
            # Refusal order is fixed: a finished booking is a ValueError,
            # work still in flight is a PermissionError, and a moment
            # past the deadline is a TimeoutError.
            if decision["state"] == "succeeded" \
                    or any(plan["state"] == "completed"
                           for plan in job_plans.values()):
                raise ValueError("a finished booking cannot be "
                                 "re-evaluated")
            if any(plan["state"] == "active"
                   for plan in job_plans.values()):
                raise PermissionError("an execution plan is active")
            if at > job["deadline"]:
                raise TimeoutError("evaluation moment exceeds the job "
                                   "deadline")

            current_id = trade["resource_id"]
            current_version = trade["version"]
            current = {"resource_id": current_id,
                       "version": current_version}
            work = job["work"]
            regions = set(job["regions"])
            residency = set(job["residency"])

            # Capacity already booked per exact resource version by
            # every OTHER job; this job's own frozen occupancy is never
            # deducted against itself.
            booked: dict[tuple[str, int], int] = {}
            for other_id, other in cleared.items():
                if other_id == job_id:
                    continue
                slot = (other["resource_id"], other["version"])
                booked[slot] = booked.get(slot, 0) + other["work"]

            candidates: list[dict[str, Any]] = []

            def consider(resource_record: dict[str, Any]) -> None:
                if resource_record["region"] not in regions:
                    return
                if not residency <= set(resource_record["residency"]):
                    return
                if resource_record["end"] < job["deadline"]:
                    return
                signal = _latest_signal(
                    signal_history, resource_record["region"], at)
                if signal is None:
                    return
                remaining = resource_record["capacity"] - booked.get(
                    (resource_record["resource_id"],
                     resource_record["version"]), 0)
                if remaining < work:
                    return
                total_cost = work * signal["unit_cost"]
                total_carbon = work * signal["carbon_intensity"]
                if total_cost > job["max_cost"] \
                        or total_carbon > job["carbon_cap"]:
                    return
                candidates.append({
                    "resource": dict(resource_record),
                    "signal": signal,
                    "total_cost": total_cost,
                    "total_carbon": total_carbon,
                })

            # The retained item is the immutable version the trade froze,
            # independent of whether its supply window is still open.
            frozen_records = history.get(current_id)
            if frozen_records is None or current_version > len(frozen_records):
                raise ValueError("trade references an unpublished "
                                 "resource version")
            consider(frozen_records[current_version - 1])

            # Migration items: only other resources' highest versions
            # valid at the moment -- never another version of the current
            # resource.
            for resource_id, versions in history.items():
                if resource_id == current_id:
                    continue
                active: dict[str, Any] | None = None
                for version_record in versions:
                    if version_record["start"] <= at <= version_record["end"]:
                        active = version_record
                if active is None:
                    continue
                consider(active)

            candidates.sort(key=lambda entry: (
                entry["signal"]["carbon_intensity"],
                entry["signal"]["unit_cost"],
                entry["resource"]["resource_id"]))
            if not candidates:
                raise LookupError("no feasible resource for the job at "
                                  "the evaluation moment")

            winner = candidates[0]["resource"]
            target = {"resource_id": winner["resource_id"],
                      "version": winner["version"]}
            recommendation = ("keep" if target == current else "migrate")
            if job_plans:
                execution_state = max(
                    job_plans.values(),
                    key=lambda plan: plan["attempt"])["state"]
            else:
                execution_state = "none"

            record: dict[str, Any] = {
                "job_id": job_id,
                "at": at,
                "current": current,
                "dispatch": decision["state"],
                "execution": execution_state,
                "candidates": candidates,
                "recommendation": recommendation,
                "target": target,
            }
            records[key] = record
            events[key] = {"key": key, "request": dict(request),
                           "result": copy.deepcopy(record)}
            _commit_file(ledger_real,
                         _canonical_bytes(records, events), old_bytes)
            return copy.deepcopy(record), True


# ---------------------------------------------------------------------------
# Advice execution: persistent re-reservation intents over migrate advice
# ---------------------------------------------------------------------------


def _validate_intent(
    record: object,
    accepted: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]],
    advice_records: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(record, dict) \
            or set(record.keys()) != set(_INTENT_FIELDS):
        raise ValueError("intent record has invalid fields")
    job_id = record["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("intent job_id must be a non-empty string")
    advice_key = record["advice_key"]
    if not isinstance(advice_key, str) or not advice_key:
        raise ValueError("intent advice_key must be a non-empty string")
    at = record["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("intent at must be a non-boolean non-negative "
                         "integer")
    source = _validate_selection(record["source"], "intent source "
                                 "selection")
    target = _validate_selection(record["target"], "intent target "
                                 "selection")
    if source == target:
        raise ValueError("an intent must reserve another resource version")
    dispatch = record["dispatch"]
    if dispatch not in _DISPATCH_STATES:
        raise ValueError("intent dispatch state is invalid")
    execution = record["execution"]
    if execution not in _EXECUTION_STATES:
        raise ValueError("intent execution state is invalid")
    reserved = record["reserved"]
    if reserved not in _RESERVED_STATES:
        raise ValueError("intent reserved state is invalid")

    job = accepted.get(job_id)
    if job is None:
        raise ValueError("intent record must reference an accepted job")
    if at > job["deadline"]:
        raise ValueError("intent moment must not pass the job deadline")
    # The source binding is verified against the continuous migration
    # chain by the ledger-level validator: a first-round reservation
    # leaves the immutable trade, while a later round leaves the latest
    # completed settlement's binding, so it is not checked against the
    # trade here.
    advice = advice_records.get(advice_key)
    if advice is None:
        raise ValueError("intent record must reference a recorded advice")
    if advice["job_id"] != job_id or advice["recommendation"] != "migrate":
        raise ValueError("intent record must reference a matching migrate "
                         "advice")
    if target != advice["target"]:
        raise ValueError("intent target must match its advice target")
    if at < advice["at"]:
        raise ValueError("intent moment must not precede its advice moment")
    # The dispatch and execution states are frozen point-in-time facts,
    # checked for the state vocabulary only, exactly as the advice
    # ledger treats its own snapshots.

    # The frozen supply record must be the exact, immutable published
    # record of the target version, feasible for the job and valid at
    # the reservation moment -- like a migration advice candidate, a
    # later supply publication must not invalidate the recorded intent.
    supply = record["supply"]
    if not isinstance(supply, dict) \
            or set(supply.keys()) != set(_resources._RECORD_FIELDS):
        raise ValueError("intent supply has invalid fields")
    supply_id = supply["resource_id"]
    supply_version = supply["version"]
    if not isinstance(supply_id, str) or not supply_id:
        raise ValueError("intent supply resource_id must be a non-empty "
                         "string")
    if not _is_plain_int(supply_version) or supply_version < 1:
        raise ValueError("intent supply version must be a positive "
                         "integer")
    if {"resource_id": supply_id, "version": supply_version} != target:
        raise ValueError("intent supply must be the target resource "
                         "version")
    records = history.get(supply_id)
    if records is None or supply_version > len(records) \
            or dict(supply) != records[supply_version - 1]:
        raise ValueError("intent supply must reference a published "
                         "resource version")
    if supply["region"] not in set(job["regions"]):
        raise ValueError("intent supply is outside the job's regions")
    if not set(job["residency"]) <= set(supply["residency"]):
        raise ValueError("intent supply does not cover job residency")
    if supply["end"] < job["deadline"]:
        raise ValueError("intent supply does not cover the job deadline")
    if not supply["start"] <= at <= supply["end"]:
        raise ValueError("intent supply must be valid at the reservation "
                         "moment")

    # The frozen signal record must be the exact published version that
    # priced the target at the reservation moment.
    signal = record["signal"]
    if not isinstance(signal, dict) \
            or set(signal.keys()) != set(_signals._RECORD_FIELDS):
        raise ValueError("intent signal has invalid fields")
    signal_copy = dict(signal)
    signal_copy["mix"] = dict(signal["mix"])
    _signals._check_values(signal_copy)
    if signal["region"] != supply["region"]:
        raise ValueError("intent signal must cover the supply region")
    signal_records = signal_history.get(signal["region"])
    if signal_records is None or signal["version"] > len(signal_records) \
            or dict(signal) != signal_records[signal["version"] - 1]:
        raise ValueError("intent signal must reference a published "
                         "signal version")
    if not (signal["observed"] <= at <= signal["expires"]):
        raise ValueError("intent signal must be valid at the reservation "
                         "moment")
    work = job["work"]
    if work * signal["unit_cost"] > job["max_cost"] \
            or work * signal["carbon_intensity"] > job["carbon_cap"]:
        raise ValueError("intent signal exceeds a job budget")
    # Capacity is a decision-time fact: later trades and intents may
    # legitimately fill the target version, so -- exactly as for the
    # advice candidates -- it is enforced at construction, not on
    # readback.

    return {
        "job_id": job_id,
        "advice_key": advice_key,
        "at": at,
        "source": source,
        "target": target,
        "supply": copy.deepcopy(supply),
        "signal": copy.deepcopy(signal),
        "dispatch": dispatch,
        "execution": execution,
        "reserved": reserved,
    }


def _validate_intent_request(request: object) -> dict[str, Any]:
    if not isinstance(request, dict) \
            or set(request.keys()) != set(_INTENT_REQUEST_FIELDS):
        raise ValueError("intent request has invalid fields")
    job_id = request["job_id"]
    advice_key = request["advice_key"]
    at = request["at"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("intent request job_id must be a non-empty "
                         "string")
    if not isinstance(advice_key, str) or not advice_key:
        raise ValueError("intent request advice_key must be a non-empty "
                         "string")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("intent request at must be a non-boolean "
                         "non-negative integer")
    return {"job_id": job_id, "advice_key": advice_key, "at": at}


def _validate_plan(
    record: object,
    intent: dict[str, Any] | None,
    accepted: dict[str, dict[str, Any]],
    job_id: str,
) -> dict[str, Any]:
    if not isinstance(record, dict) \
            or set(record.keys()) != set(_PLAN_FIELDS):
        raise ValueError("plan record has invalid fields")
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("plan job id must be a non-empty string")
    if record.get("job_id") != job_id:
        raise ValueError("plan record id does not match its history key")
    source = _validate_selection(record["source"], "plan source selection")
    target = _validate_selection(record["target"], "plan target selection")
    if source == target:
        raise ValueError("a plan must migrate to another resource version")
    owner = record["owner"]
    if not isinstance(owner, str) or not owner:
        raise ValueError("plan owner must be a non-empty string")
    lease_end = record["lease_end"]
    if not _is_plain_int(lease_end) or lease_end < 1:
        raise ValueError("plan lease_end must be a positive integer")
    at = record["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("plan at must be a non-boolean non-negative "
                         "integer")
    if at > lease_end:
        raise ValueError("plan start moment must lie inside the lease")
    state = record["state"]
    if state not in _PLAN_STATES:
        raise ValueError("plan state is invalid")

    steps_raw = record["steps"]
    if not isinstance(steps_raw, list) or len(steps_raw) > len(_PLAN_STEPS):
        raise ValueError("plan steps must be a list within the step "
                         "sequence")
    steps: list[dict[str, Any]] = []
    for index, receipt_raw in enumerate(steps_raw):
        if not isinstance(receipt_raw, dict) \
                or set(receipt_raw.keys()) != set(_RECEIPT_FIELDS):
            raise ValueError("step receipt has invalid fields")
        if receipt_raw["step"] != _PLAN_STEPS[index]:
            raise ValueError("step receipts must follow the copy/switch "
                             "sequence")
        result = receipt_raw["result"]
        if result not in _STEP_RESULTS:
            raise ValueError("step receipt result is invalid")
        token = receipt_raw["receipt"]
        if not isinstance(token, str) or not token:
            raise ValueError("step receipt must carry a non-empty receipt "
                             "string")
        receipt_at = receipt_raw["at"]
        if not _is_plain_int(receipt_at) or receipt_at < 0:
            raise ValueError("step receipt at must be a non-boolean "
                             "non-negative integer")
        if receipt_at < record["at"]:
            raise ValueError("step receipt must not precede the plan "
                             "start moment")
        if index > 0 and receipt_at < steps[-1]["at"]:
            raise ValueError("step receipt must not precede the previous "
                             "receipt")
        if receipt_at > lease_end:
            raise ValueError("step receipt must be recorded inside the "
                             "lease")
        steps.append({"step": receipt_raw["step"], "result": result,
                      "receipt": token, "at": receipt_at})

    # The state must be exactly what the recorded receipts imply: a
    # failed receipt terminates the plan and must be the last one, a
    # complete successful copy/switch sequence terminates it as migrated,
    # and anything else is active or recovered-interrupted.
    failed = [receipt for receipt in steps if receipt["result"] == "failed"]
    if failed:
        if steps[-1]["result"] != "failed" or len(failed) != 1:
            raise ValueError("a failed step must terminate the plan")
        if state != "failed":
            raise ValueError("a plan with a failed step must be failed")
    elif len(steps) == len(_PLAN_STEPS):
        if state != "migrated":
            raise ValueError("a fully recorded plan must be migrated")
    elif state not in ("active", "interrupted"):
        raise ValueError("an unfinished plan must be active or "
                         "interrupted")

    if intent is not None:
        if source != intent["source"] or target != intent["target"]:
            raise ValueError("plan selections must match its reservation "
                             "intent")
    # The intent validation guarantees the job is accepted.
    if job_id not in accepted or lease_end > accepted[job_id]["deadline"]:
        raise ValueError("plan lease end must not pass the job deadline")

    return {
        "job_id": job_id,
        "source": source,
        "target": target,
        "owner": owner,
        "lease_end": lease_end,
        "at": at,
        "state": state,
        "steps": steps,
    }


def _ordered_plans(
    plans_for_job: dict[str, dict[str, Any]],
) -> list[tuple[str, dict[str, Any]]]:
    # Rounds are ordered by their start moment; two plans of one job must
    # not start at the same moment, since that moment is what binds the
    # continuous migration chain.
    ordered = sorted(plans_for_job.items(),
                     key=lambda item: (item[1]["at"], item[0]))
    lasts = [plan["at"] for _, plan in ordered]
    if len(set(lasts)) != len(lasts):
        raise ValueError("migration rounds of one job must start at "
                         "distinct moments")
    return ordered


def _plan_outcome(plan: dict[str, Any]) -> dict[str, Any]:
    # The binding a settled plan leaves behind: the target after a
    # confirmed migration, the unchanged source after a compensation.
    return copy.deepcopy(plan["target"] if plan["state"] == "migrated"
                         else plan["source"])


def _latest_settled_binding(
    job_id: str,
    trade_selection: dict[str, Any],
    job_plans: dict[str, dict[str, Any]],
    markers: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    # The source a new reservation or plan must leave: the outcome of
    # the latest already-settled plan, or the immutable trade before the
    # first settlement. Pending markers never advance the binding.
    binding = copy.deepcopy(trade_selection)
    for plan_key, plan in _ordered_plans(job_plans):
        marker = markers.get(plan_key)
        if marker is not None and marker["state"] == "settled" \
                and marker["job_id"] == job_id:
            binding = _plan_outcome(plan)
    return binding


def _pair_rounds(
    job_intents: dict[str, dict[str, Any]],
    ordered: list[tuple[str, dict[str, Any]]],
) -> tuple[dict[str, str], list[str]]:
    # Pair every plan with the one reservation intent it consumed: the
    # latest still-unconsumed reservation with matching selections made
    # no later than the plan start. Intents are consumed in order, so a
    # later round repeating an earlier resource pair never reclaims the
    # earlier reservation key. Returns the plan-key to reservation-key
    # map and the reservation keys left open (no plan yet).
    remaining = dict(job_intents)
    pairing: dict[str, str] = {}
    for plan_key, plan in ordered:
        candidates = [reserve_key for reserve_key, intent in remaining.items()
                      if intent["source"] == plan["source"]
                      and intent["target"] == plan["target"]
                      and intent["at"] <= plan["at"]]
        if not candidates:
            raise ValueError("plan must consume a recorded reservation "
                             "intent")
        reserve_key = max(candidates,
                          key=lambda key: (remaining[key]["at"], key))
        pairing[plan_key] = reserve_key
        del remaining[reserve_key]
    return pairing, sorted(remaining)


def _round_action_keys(
    job_id: str,
    plan: dict[str, Any],
    later_at: int | None,
    reserve_key: str,
    idempotency: dict[str, dict[str, Any]],
) -> list[str]:
    # Every intent-ledger action one migration round committed: its
    # reservation, the claim and the step receipts/recovery. The round
    # opens at the plan start and ends at the next round's start, so an
    # action can never be counted into another round's settlement audit.
    keys = [reserve_key]
    for key, request in idempotency.items():
        if request.get("job_id") != job_id or "action" not in request:
            continue
        if request["at"] < plan["at"]:
            continue
        if later_at is not None and request["at"] >= later_at:
            continue
        keys.append(key)
    return sorted(set(keys))


def _terminal_plan_at(
    plan: dict[str, Any],
    round_action_keys: list[str],
    idempotency: dict[str, dict[str, Any]],
) -> int:
    # The terminal evidence moment: the recorded recovery moment for an
    # interrupted plan, otherwise the last committed step receipt.
    if plan["state"] == "interrupted":
        moments = sorted(idempotency[key]["at"] for key in round_action_keys
                         if idempotency[key].get("action") == "recover")
        if len(moments) != 1:
            raise ValueError("an interrupted plan must reference exactly "
                             "one recovery receipt")
        return moments[0]
    if not plan["steps"]:
        raise ValueError("a terminal plan must carry a terminal receipt")
    return plan["steps"][-1]["at"]


_MARKER_STATES = ("pending", "settled")
_MARKER_FIELDS = ("job_id", "key", "state")


def _validate_marker(value: object, plan_key: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != set(_MARKER_FIELDS):
        raise ValueError("settlement marker has invalid fields")
    job_id = value["job_id"]
    settle_key = value["key"]
    state = value["state"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("settlement marker must name its job")
    if not isinstance(settle_key, str) or not settle_key:
        raise ValueError("settlement marker must name its settle key")
    if state not in _MARKER_STATES:
        raise ValueError("settlement marker state is invalid")
    if not isinstance(plan_key, str) or not plan_key:
        raise ValueError("settlement marker plan key must be a non-empty "
                         "string")
    return {"job_id": job_id, "key": settle_key, "state": state}


def _validate_action_request(request: object) -> dict[str, Any]:
    if not isinstance(request, dict):
        raise ValueError("request must be an object")
    action = request.get("action")
    if not isinstance(action, str) or action not in _ACTION_REQUEST_FIELDS:
        raise ValueError("request action is invalid")
    if set(request.keys()) != set(_ACTION_REQUEST_FIELDS[action]):
        raise ValueError("request has invalid fields")
    job_id = request["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("request job_id must be a non-empty string")
    owner = request["owner"]
    if not isinstance(owner, str) or not owner:
        raise ValueError("request owner must be a non-empty string")
    at = request["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("request at must be a non-boolean non-negative "
                         "integer")
    normalized: dict[str, Any] = {"action": action, "job_id": job_id,
                                  "owner": owner}
    if action == "start":
        lease_end = request["lease_end"]
        if not _is_plain_int(lease_end) or lease_end < 1:
            raise ValueError("request lease_end must be a positive "
                             "integer")
        normalized["lease_end"] = lease_end
    if action == "record":
        step = request["step"]
        if step not in _PLAN_STEPS:
            raise ValueError("request step is invalid")
        normalized["step"] = step
        result = request["result"]
        if result not in _STEP_RESULTS:
            raise ValueError("request result is invalid")
        normalized["result"] = result
        receipt = request["receipt"]
        if not isinstance(receipt, str) or not receipt:
            raise ValueError("request receipt must be a non-empty string")
        normalized["receipt"] = receipt
    normalized["at"] = at
    return {field: normalized[field]
            for field in _ACTION_REQUEST_FIELDS[action]}


def _validate_any_request(request: object) -> dict[str, Any]:
    # The idempotency key space is shared: apply bindings carry no
    # action, the migration lifecycle bindings carry one.
    if isinstance(request, dict) and "action" in request:
        return _validate_action_request(request)
    return _validate_intent_request(request)


def _normalize_legacy_sections(
    data: dict[str, Any],
) -> tuple[int, dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    # Version 1 keeps one flat intent per job; version 2 adds one flat
    # plan per job. Both single-migration layouts stay readable and are
    # upgraded in place to the nested version-3 history only when a new
    # round or a settlement is committed.
    keys = set(data.keys())
    if keys == set(_INTENT_ROOT_FIELDS):
        if not _is_plain_int(data["version"]) \
                or data["version"] != _INTENT_VERSION:
            raise ValueError("unsupported intent ledger version")
        return _INTENT_VERSION, data["intents"], {}, None
    if keys == set(_INTENT_ROOT_FIELDS_V2):
        if not _is_plain_int(data["version"]) \
                or data["version"] != _INTENT_VERSION_V2:
            raise ValueError("unsupported intent ledger version")
        return _INTENT_VERSION_V2, data["intents"], data["plans"], None
    if keys != set(_INTENT_ROOT_FIELDS_V3):
        raise ValueError("intent ledger root must be an object with keys "
                         "version, intents, idempotency and audit, plus "
                         "plans once upgraded and settled markers")
    if not _is_plain_int(data["version"]) \
            or data["version"] != _INTENT_VERSION_V3:
        raise ValueError("unsupported intent ledger version")
    return _INTENT_VERSION_V3, data["intents"], data["plans"], data["settled"]


def _nest_legacy_intents(
    flat_intents: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, Any]],
) -> dict[str, dict[str, dict[str, Any]]]:
    # The single reservation key of a legacy job is its one non-action
    # idempotency binding; the request is guaranteed to match the intent.
    nested: dict[str, dict[str, dict[str, Any]]] = {}
    for reserve_key, request in idempotency.items():
        if "action" in request:
            continue
        nested.setdefault(request["job_id"], {})[reserve_key] = \
            flat_intents[request["job_id"]]
    for job_id in flat_intents:
        if job_id not in nested:
            raise ValueError("every legacy intent must be bound to a "
                             "reservation key")
    return nested


def _nest_legacy_plans(
    flat_plans: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, Any]],
) -> dict[str, dict[str, dict[str, Any]]]:
    # The single plan key of a legacy job is its one start binding.
    nested: dict[str, dict[str, dict[str, Any]]] = {}
    for plan_key, request in idempotency.items():
        if request.get("action") == "start":
            nested.setdefault(request["job_id"], {})[plan_key] = \
                flat_plans[request["job_id"]]
    for job_id in flat_plans:
        if job_id not in nested:
            raise ValueError("every legacy plan must be bound to a start "
                             "key")
    return nested


def _validate_intent_ledger(
    data: object,
    accepted: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]],
    advice_records: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, dict[str, Any]]],
           dict[str, dict[str, dict[str, Any]]],
           dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]], int]:
    if not isinstance(data, dict):
        raise ValueError("intent ledger root must be an object")
    version, intents_raw, plans_raw, settled_raw = \
        _normalize_legacy_sections(data)
    idempotency_raw = data["idempotency"]
    audit_raw = data["audit"]
    if not isinstance(intents_raw, dict) \
            or not isinstance(plans_raw, dict) \
            or not isinstance(idempotency_raw, dict) \
            or not isinstance(audit_raw, dict) \
            or (settled_raw is not None and not isinstance(settled_raw, dict)):
        raise ValueError("intents, plans, settled, idempotency and audit "
                         "must be objects")
    _check_sorted_keys(idempotency_raw, "idempotency")
    _check_sorted_keys(audit_raw, "audit")

    # Parse and validate every idempotency binding first; the nested
    # intent and plan histories are validated structurally below and
    # then cross-checked against these bindings and the audit events.
    idempotency: dict[str, dict[str, Any]] = {}
    for key, request_raw in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        idempotency[key] = _validate_any_request(request_raw)

    nested_intents: dict[str, dict[str, dict[str, Any]]] = {}
    nested_plans: dict[str, dict[str, dict[str, Any]]] = {}
    markers: dict[str, dict[str, Any]] = {}

    flat_intent_records: dict[str, dict[str, Any]] = {}
    if version in (_INTENT_VERSION, _INTENT_VERSION_V2):
        _check_sorted_keys(intents_raw, "intents")
        for job_id, record_raw in intents_raw.items():
            if not isinstance(job_id, str) or not job_id:
                raise ValueError("intent job ids must be non-empty strings")
            record = _validate_intent(
                record_raw, accepted, trades, history, signal_history,
                advice_records)
            if record["job_id"] != job_id:
                raise ValueError("intent record id does not match its key")
            flat_intent_records[job_id] = record
        nested_intents = _nest_legacy_intents(flat_intent_records,
                                              idempotency)
        if version == _INTENT_VERSION_V2:
            _check_sorted_keys(plans_raw, "plans")
            flat_plan_records: dict[str, dict[str, Any]] = {}
            for job_id, plan_raw in plans_raw.items():
                if not isinstance(job_id, str) or not job_id:
                    raise ValueError("plan job ids must be non-empty "
                                     "strings")
                intent = flat_intent_records.get(job_id)
                plan = _validate_plan(plan_raw, intent, accepted, job_id)
                flat_plan_records[job_id] = plan
            nested_plans = _nest_legacy_plans(flat_plan_records, idempotency)
    else:
        _check_sorted_keys(intents_raw, "intents")
        for job_id, history_raw in intents_raw.items():
            if not isinstance(job_id, str) or not job_id \
                    or not isinstance(history_raw, dict) or not history_raw:
                raise ValueError("intent history must be a non-empty "
                                 "object per job")
            _check_sorted_keys(history_raw, "intent history")
            job_intents: dict[str, dict[str, Any]] = {}
            for reserve_key, record_raw in history_raw.items():
                if not isinstance(reserve_key, str) or not reserve_key:
                    raise ValueError("reservation keys must be non-empty "
                                     "strings")
                record = _validate_intent(
                    record_raw, accepted, trades, history, signal_history,
                    advice_records)
                if record["job_id"] != job_id:
                    raise ValueError("intent record id does not match its "
                                     "history key")
                job_intents[reserve_key] = record
            nested_intents[job_id] = job_intents

        _check_sorted_keys(plans_raw, "plans")
        for job_id, job_plans_raw in plans_raw.items():
            if not isinstance(job_id, str) or not job_id \
                    or not isinstance(job_plans_raw, dict) \
                    or not job_plans_raw:
                raise ValueError("plan history must be a non-empty object "
                                 "per job")
            _check_sorted_keys(job_plans_raw, "plan history")
            job_plans: dict[str, dict[str, Any]] = {}
            for plan_key, plan_raw in job_plans_raw.items():
                if not isinstance(plan_key, str) or not plan_key:
                    raise ValueError("plan keys must be non-empty strings")
                # Intent pairing is rechecked against the ordered chain
                # after every plan is parsed; structural validation runs
                # without the intent first.
                plan = _validate_plan(plan_raw, None, accepted, job_id)
                job_plans[plan_key] = plan
            nested_plans[job_id] = job_plans

        assert settled_raw is not None
        _check_sorted_keys(settled_raw, "settled")
        for plan_key, marker_raw in settled_raw.items():
            marker = _validate_marker(marker_raw, plan_key)
            if marker["job_id"] not in nested_plans \
                    or plan_key not in nested_plans[marker["job_id"]]:
                raise ValueError("settlement marker must reference a "
                                 "recorded migration plan")
            plan = nested_plans[marker["job_id"]][plan_key]
            if plan["state"] not in _SETTLE_TERMINAL:
                raise ValueError("only a terminal plan may carry a "
                                 "settlement marker")
            markers[plan_key] = marker

    # Every reservation binding must reference a recorded intent in the
    # job's history, and each round reserves exactly once.
    reserve_bindings: dict[str, str] = {}
    for key, request in idempotency.items():
        if "action" in request:
            continue
        job_id = request["job_id"]
        job_intents = nested_intents.get(job_id, {})
        record = job_intents.get(key)
        if record is None:
            raise ValueError("reservation binding must reference a "
                             "recorded intent")
        if request["advice_key"] != record["advice_key"] \
                or request["at"] != record["at"]:
            raise ValueError("reservation binding does not match its "
                             "intent")
        reserve_bindings[key] = job_id

    # Every action binding must reference one recorded plan of its job.
    for key, request in idempotency.items():
        if "action" not in request:
            continue
        if request["job_id"] not in nested_plans:
            raise ValueError("action binding must reference a recorded "
                             "plan history")

    # The continuous migration chain per job: the first plan leaves the
    # immutable trade, every later plan leaves the latest settled plan's
    # outcome, each plan pairs with the reservation intent it consumed,
    # and a pending settlement is always the open last round. The write
    # paths serialize rounds (a new round is reserved or started only
    # once the previous plan carries a settled marker), so a settled
    # plan is always the predecessor of whatever follows it.
    for job_id, job_plans in nested_plans.items():
        trade = trades.get(job_id)
        if trade is None:
            raise ValueError("plan history must reference a recorded "
                             "trade")
        trade_selection = {"resource_id": trade["resource_id"],
                           "version": trade["version"]}
        job_intents = nested_intents.get(job_id, {})
        ordered = _ordered_plans(job_plans)
        binding = copy.deepcopy(trade_selection)
        active_seen = False
        for index, (plan_key, plan) in enumerate(ordered):
            later_at = ordered[index + 1][1]["at"] \
                if index + 1 < len(ordered) else None
            if plan["source"] != binding:
                raise ValueError("migration plan breaks the continuous "
                                 "binding chain")
            paired = [intent for intent in job_intents.values()
                      if intent["source"] == plan["source"]
                      and intent["target"] == plan["target"]
                      and intent["at"] <= plan["at"]]
            if not paired:
                raise ValueError("plan must consume a recorded "
                                 "reservation intent")
            if plan["state"] == "active":
                if active_seen:
                    raise ValueError("a job can hold at most one active "
                                     "migration plan")
                active_seen = True
            marker = markers.get(plan_key)
            if marker is not None:
                if marker["job_id"] != job_id:
                    raise ValueError("settlement marker references the "
                                     "wrong job")
                if later_at is not None and marker["state"] == "pending":
                    raise ValueError("a pending settlement must be the "
                                     "open migration round")
            if marker is not None and marker["state"] == "settled":
                binding = _plan_outcome(plan)

        # A reservation intent not yet consumed by a plan is the single
        # open round and must leave the latest settled binding (or the
        # immutable trade before the first settlement).
        consumed: set[str] = set()
        for plan in job_plans.values():
            for reserve_key, intent in job_intents.items():
                if intent["source"] == plan["source"] \
                        and intent["target"] == plan["target"] \
                        and intent["at"] <= plan["at"]:
                    consumed.add(reserve_key)
        open_intents = [key for key in job_intents if key not in consumed]
        if len(open_intents) > 1:
            raise ValueError("a job can hold at most one open "
                             "reservation")
        if open_intents:
            open_intent = job_intents[open_intents[0]]
            if open_intent["source"] != binding:
                raise ValueError("an open reservation must leave the "
                                 "latest settled binding")

    # Every recorded intent, including reserved-only rounds, must be
    # bound to its reservation key.
    for job_id, job_intents in nested_intents.items():
        if set(job_intents) != {key for key, bound_job in
                                reserve_bindings.items()
                                if bound_job == job_id}:
            raise ValueError("every reservation intent must be bound to "
                             "exactly one reservation key")

    events: dict[str, dict[str, Any]] = {}
    for key, event_raw in audit_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event_raw, dict) \
                or set(event_raw.keys()) != set(_EVENT_FIELDS):
            raise ValueError("intent audit event has invalid fields")
        if event_raw["key"] != key or not isinstance(event_raw["key"], str):
            raise ValueError("audit event key does not match its map key")
        request = _validate_any_request(event_raw["request"])
        if key not in idempotency:
            raise ValueError("audit event must reference an idempotency "
                             "entry")
        if request != idempotency[key]:
            raise ValueError("audit event does not match its idempotency "
                             "entry")
        if "action" in request:
            job_id = request["job_id"]
            plan_key = _plan_key_for_request(request, nested_plans[job_id])
            result_plan = _validate_plan(
                event_raw["result"], None, accepted, job_id)
            # The audit event freezes the plan snapshot the action
            # committed (a later receipt legitimately grows the plan),
            # so only the round's fixed fields must agree here; the
            # per-round lifecycle loop reconciles every snapshot and
            # receipt below.
            current_plan = nested_plans[job_id][plan_key]
            for field in ("source", "target", "owner", "lease_end", "at"):
                if result_plan[field] != current_plan[field]:
                    raise ValueError("audit event result does not match "
                                     "the recorded plan")
            result: dict[str, Any] = result_plan
        else:
            job_id = request["job_id"]
            result_intent = _validate_intent(
                event_raw["result"], accepted, trades, history,
                signal_history, advice_records)
            if result_intent != nested_intents[job_id][key]:
                raise ValueError("audit event result does not match its "
                                 "intent")
            result = result_intent
        events[key] = {"key": key, "request": request, "result": result}

    if set(events) != set(idempotency):
        raise ValueError("idempotency keys and audit events do not match")

    # The action history describes each plan's lifecycle: exactly one
    # start per plan, the recorded receipts are exactly the requests
    # committed, and a plan is interrupted exactly when its one recovery
    # was recorded. Actions are assigned to rounds by the start moments.
    starts: dict[tuple[str, str], str] = {}
    for key, request in idempotency.items():
        if request.get("action") != "start":
            continue
        job_id = request["job_id"]
        plan_key = _plan_key_for_request(request, nested_plans[job_id])
        if (job_id, plan_key) in starts:
            raise ValueError("plan started under more than one "
                             "idempotency key")
        starts[(job_id, plan_key)] = key
    if {identity for identity in starts} != \
            {(job, key) for job, job_plans in nested_plans.items()
             for key in job_plans}:
        raise ValueError("every plan must be bound to a start request")

    for job_id, job_plans in nested_plans.items():
        ordered = _ordered_plans(job_plans)
        for index, (plan_key, plan) in enumerate(ordered):
            start_key = starts[(job_id, plan_key)]
            start_event = events[start_key]
            if start_event["request"]["owner"] != plan["owner"] \
                    or start_event["request"]["lease_end"] != \
                    plan["lease_end"] \
                    or start_event["request"]["at"] != plan["at"]:
                raise ValueError("start audit does not match its plan")
            later_at = ordered[index + 1][1]["at"] \
                if index + 1 < len(ordered) else None
            recorded_steps: list[dict[str, Any]] = []
            recoveries = 0
            for key, request in idempotency.items():
                if request.get("job_id") != job_id \
                        or "action" not in request:
                    continue
                moment = request["at"]
                if moment < plan["at"] or (later_at is not None
                                           and moment >= later_at):
                    continue
                action = request["action"]
                result_plan = events[key]["result"]
                for field in ("source", "target", "owner", "lease_end",
                              "at"):
                    if result_plan[field] != plan[field]:
                        raise ValueError("audit result does not match its "
                                         "plan")
                if action == "start":
                    if key != start_key:
                        raise ValueError("two starts inside one migration "
                                         "round")
                    if result_plan["state"] != "active" \
                            or result_plan["steps"] != []:
                        raise ValueError("start audit result must be a "
                                         "fresh active plan")
                elif action == "record":
                    if not result_plan["steps"]:
                        raise ValueError("record audit must carry a step")
                    receipt = result_plan["steps"][-1]
                    if receipt != {"step": request["step"],
                                   "result": request["result"],
                                   "receipt": request["receipt"],
                                   "at": request["at"]}:
                        raise ValueError("record audit does not match its "
                                         "request")
                    recorded_steps.append(receipt)
                else:  # recover
                    if result_plan["state"] != "interrupted":
                        raise ValueError("recover audit must interrupt the "
                                         "plan")
                    if request["at"] <= result_plan["lease_end"]:
                        raise ValueError("recover request must lie past "
                                         "the lease end")
                    recoveries += 1
            committed = sorted(json.dumps(receipt, sort_keys=True)
                               for receipt in recorded_steps)
            held = sorted(json.dumps(receipt, sort_keys=True)
                          for receipt in plan["steps"])
            if committed != held:
                raise ValueError("plan steps do not match the record "
                                 "history")
            if (plan["state"] == "interrupted") != (recoveries == 1):
                raise ValueError("plan state does not match the recover "
                                 "history")
            if recoveries > 1:
                raise ValueError("plan recovered more than once")

    return (nested_intents, nested_plans, idempotency, events, markers,
            version)


def _plan_key_for_request(
    request: dict[str, Any],
    job_plans: dict[str, dict[str, Any]],
) -> str:
    # The start request is the plan's identity; record and recover
    # requests are assigned to the round whose start window they fall in.
    if request.get("action") == "start":
        for plan_key, plan in job_plans.items():
            if plan["at"] == request["at"] and plan["owner"] == \
                    request["owner"] and plan["lease_end"] == \
                    request["lease_end"]:
                return plan_key
        raise ValueError("start request does not match a recorded plan")
    candidates = [plan_key for plan_key, plan in job_plans.items()
                  if plan["at"] <= request["at"]]
    if not candidates:
        raise ValueError("action does not belong to a recorded round")
    return max(candidates, key=lambda key: (job_plans[key]["at"], key))


def _intent_canonical_bytes(
    intents: dict[str, dict[str, dict[str, Any]]],
    plans: dict[str, dict[str, dict[str, Any]]],
    markers: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
    version: int,
    legacy_intents: dict[str, dict[str, Any]] | None = None,
    legacy_plans: dict[str, dict[str, Any]] | None = None,
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, sections in their
    # fixed field order and bindings/events keyed by idempotency key,
    # each section in code-point order, terminated by exactly one
    # newline. Version 1 predates the plan history, version 2 keeps one
    # flat plan per job, and version 3 nests both histories by job and
    # plan key and adds the settlement markers the pending-conflict
    # rules need visible to apply/start/record/recover.
    payload: dict[str, Any] = {"version": version}
    if version == _INTENT_VERSION and legacy_intents is not None:
        payload["intents"] = {job_id: legacy_intents[job_id]
                              for job_id in sorted(legacy_intents)}
    elif version == _INTENT_VERSION_V2 and legacy_intents is not None:
        payload["intents"] = {job_id: legacy_intents[job_id]
                              for job_id in sorted(legacy_intents)}
        payload["plans"] = {job_id: legacy_plans[job_id]
                            for job_id in sorted(legacy_plans or {})}
    else:
        payload["intents"] = {
            job_id: {key: intents[job_id][key]
                     for key in sorted(intents[job_id])}
            for job_id in sorted(intents)}
        payload["plans"] = {
            job_id: {key: plans[job_id][key]
                     for key in sorted(plans[job_id])}
            for job_id in sorted(plans)}
        payload["settled"] = {key: markers[key] for key in sorted(markers)}
    payload["idempotency"] = {key: idempotency[key]
                              for key in sorted(idempotency)}
    payload["audit"] = {key: events[key] for key in sorted(events)}
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_intent_ledger(
    realpath: str,
    accepted: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]],
    advice_records: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, dict[str, Any]]],
           dict[str, dict[str, dict[str, Any]]],
           dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]], int, bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return {}, {}, {}, {}, {}, _INTENT_VERSION, None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"intent ledger {realpath!r} is not valid UTF-8") from exc
    try:
        # Negative-zero and non-finite literals are format errors.
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"intent ledger {realpath!r} is not valid JSON") from exc
    intents, plans, idempotency, events, markers, version = \
        _validate_intent_ledger(
            data, accepted, trades, history, signal_history, advice_records)
    # As for the other ledgers, the ledger is accepted only in canonical
    # compact form with a single trailing newline.
    legacy_intents: dict[str, dict[str, Any]] | None = None
    legacy_plans: dict[str, dict[str, Any]] | None = None
    if version in (_INTENT_VERSION, _INTENT_VERSION_V2):
        # A legacy file holds exactly one reservation and one plan per
        # job, so the flat sections are the sole entries of each nested
        # job history.
        legacy_intents = {job_id: next(iter(job_intents.values()))
                          for job_id, job_intents in intents.items()}
        if version == _INTENT_VERSION_V2:
            legacy_plans = {job_id: next(iter(job_plans.values()))
                            for job_id, job_plans in plans.items()}
    if raw != _intent_canonical_bytes(intents, plans, markers, idempotency,
                                      events, version, legacy_intents,
                                      legacy_plans):
        raise ValueError(
            f"intent ledger {realpath!r} is not in canonical compact "
            "form")
    return intents, plans, idempotency, events, markers, version, raw


def apply(
    jobs: str,
    supply: str,
    signals: str,
    trades: str,
    dispatch: str,
    execution: str,
    advice: str,
    ledger: str,
    job_id: str,
    advice_key: str,
    key: str,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Apply one migrate advice as a persisted re-reservation.

    The call form, refusal order and canonical storage match the public
    contract; unlike the single-migration baseline, a job may run
    several migration rounds and every reservation and plan is kept in a
    per-job history keyed by its idempotency key. The new reservation
    leaves the latest completed binding :func:`current` would return --
    the latest settled migration's outcome, or the immutable trade for
    the first round -- and never falls back to the original trade once
    the job moved away. A pending settlement blocks a new reservation,
    and a new round can only be reserved once the previous plan settled.
    """
    for value in (jobs, supply, signals, trades, dispatch, execution,
                  advice, ledger, job_id, advice_key, key):
        if not isinstance(value, str) or not value:
            raise ValueError("the eight paths, job_id, advice_key and "
                             "key must be non-empty strings")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    job_real = os.path.realpath(jobs)
    supply_real = os.path.realpath(supply)
    signal_real = os.path.realpath(signals)
    trades_real = os.path.realpath(trades)
    dispatch_real = os.path.realpath(dispatch)
    execution_real = os.path.realpath(execution)
    advice_real = os.path.realpath(advice)
    ledger_real = os.path.realpath(ledger)
    all_paths = (job_real, supply_real, signal_real, trades_real,
                 dispatch_real, execution_real, advice_real, ledger_real)
    if len(set(all_paths)) != 8:
        raise ValueError("the eight paths must be distinct real paths")

    store = _get_store(ledger)
    with store.lock:
        with contextlib.ExitStack() as stack:
            for locked in sorted(set(all_paths)):
                stack.enter_context(
                    _lock(locked, shared=(locked != ledger_real)))

            accepted, job_map, job_events, job_raw = \
                _jobs._load_submit_file(job_real)
            if job_raw is None:
                raise FileNotFoundError(
                    f"acceptance file {job_real!r} does not exist")
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
            signal_history, _signal_map, _signal_events, signal_raw = \
                _signals._load_file(signal_real)
            if signal_raw is None:
                raise FileNotFoundError(
                    f"signal file {signal_real!r} does not exist")
            cleared, _clear_keys, trades_raw = _market._load_clear_ledger(
                trades_real, accepted, history, signal_history)
            if trades_raw is None:
                raise FileNotFoundError(
                    f"clearing ledger {trades_real!r} does not exist")
            decisions, _dispatch_keys, _dispatch_events, dispatch_raw = \
                _dispatch._load_ledger(dispatch_real)
            if dispatch_raw is None:
                raise FileNotFoundError(
                    f"dispatch ledger {dispatch_real!r} does not exist")
            exec_plans, _plan_keys, _plan_events = \
                _execution._load_existing_ledger(execution_real)[:3]
            advice_records, _advice_events, advice_raw = _load_ledger(
                advice_real, accepted, cleared, history, signal_history)
            if advice_raw is None:
                raise FileNotFoundError(
                    f"advice ledger {advice_real!r} does not exist")
            intents, migration_plans, idempotency, events, markers, \
                _ledger_version, old_bytes = _load_intent_ledger(
                    ledger_real, accepted, cleared, history,
                    signal_history, advice_records)

            request = {"job_id": job_id, "advice_key": advice_key,
                       "at": at}
            binding = idempotency.get(key)
            if binding is not None:
                if binding != request:
                    raise ValueError("idempotency key was already used "
                                     "with a different request")
                return copy.deepcopy(intents[job_id][key]), False

            job = accepted.get(job_id)
            if job is None:
                raise KeyError(job_id)
            trade = cleared.get(job_id)
            if trade is None:
                raise LookupError("job has no recorded trade")
            advice_record = advice_records.get(advice_key)
            if advice_record is None:
                raise KeyError(advice_key)
            if advice_record["job_id"] != job_id:
                raise ValueError("advice key belongs to a different job")
            if advice_record["recommendation"] != "migrate":
                raise ValueError("only a migrate advice can be applied")
            if at < advice_record["at"]:
                raise ValueError("reservation moment precedes the advice "
                                 "moment")
            decision = decisions.get(job_id)
            if decision is None:
                raise KeyError(job_id)

            job_intents = intents.get(job_id, {})
            job_plans = migration_plans.get(job_id, {})
            ordered_rounds = _ordered_plans(job_plans) if job_plans else []
            if _pending_marker_for_job(markers, job_id) is not None:
                raise ValueError("a pending settlement blocks further "
                                 "reservations and migrations")
            if ordered_rounds and markers.get(
                    ordered_rounds[-1][0]) is None:
                raise ValueError("the previous migration round must "
                                 "settle before a new reservation")
            # A reservation not yet consumed by a plan is the single open
            # round; a second reservation before any plan starts is a
            # conflict, not a new round.
            pairing, open_reserve_keys = _pair_rounds(
                job_intents, ordered_rounds)
            if open_reserve_keys:
                raise ValueError("a reservation is already open for the "
                                 "job")

            # Refusal order is fixed: a finished booking is a
            # ValueError, work claimed or in flight is a
            # PermissionError, and a moment past the deadline is a
            # TimeoutError.
            if decision["state"] == "succeeded" \
                    or any(plan["state"] == "completed"
                           for plan in exec_plans.get(job_id, {}).values()):
                raise ValueError("a finished booking cannot be "
                                 "re-reserved")
            if decision["state"] == "claimed" \
                    or any(plan["state"] == "active"
                           for plan in exec_plans.get(job_id, {}).values()):
                raise PermissionError("the booking is claimed or has an "
                                      "active execution plan")
            if at > job["deadline"]:
                raise TimeoutError("reservation moment exceeds the job "
                                   "deadline")

            current = _latest_settled_binding(
                job_id,
                {"resource_id": trade["resource_id"],
                 "version": trade["version"]},
                job_plans, markers)
            current_id = current["resource_id"]
            current_version = current["version"]
            work = job["work"]
            regions = set(job["regions"])
            residency = set(job["residency"])

            # Capacity still held per exact resource version. Walking a
            # job's rounds in order yields the union of selections it
            # currently occupies (the boundary resource -- a migrated
            # plan's target that is also the next reservation's source
            # -- is one hold, not two): an open reservation or an active
            # round holds source and target, a migrated round the target
            # and a failed or interrupted round the source. A job with
            # no migration history occupies its trade slot. This job's
            # own holds are never deducted against itself.
            booked: dict[tuple[str, int], int] = {}

            def occupy(selection: dict[str, Any], amount: int) -> None:
                slot = (selection["resource_id"], selection["version"])
                booked[slot] = booked.get(slot, 0) + amount

            for other_id, other in cleared.items():
                if other_id == job_id:
                    continue
                other_work = accepted[other_id]["work"]
                other_job_plans = migration_plans.get(other_id, {})
                other_job_intents = intents.get(other_id, {})
                held: set[tuple[str, int]] = set()

                def hold(selection: dict[str, Any]) -> None:
                    held.add((selection["resource_id"],
                              selection["version"]))

                if not other_job_plans:
                    if other_job_intents:
                        for intent in other_job_intents.values():
                            hold(intent["source"])
                            hold(intent["target"])
                    else:
                        hold({"resource_id": other["resource_id"],
                              "version": other["version"]})
                else:
                    ordered_other = _ordered_plans(other_job_plans)
                    _pairing, open_keys = _pair_rounds(
                        other_job_intents, ordered_other)
                    for _pk, other_plan in ordered_other:
                        if other_plan["state"] in ("failed",
                                                  "interrupted"):
                            hold(other_plan["source"])
                        elif other_plan["state"] == "migrated":
                            hold(other_plan["target"])
                        else:
                            hold(other_plan["source"])
                            hold(other_plan["target"])
                    for reserve_key in open_keys:
                        open_intent = other_job_intents[reserve_key]
                        hold(open_intent["source"])
                        hold(open_intent["target"])
                for resource_id, version in held:
                    occupy({"resource_id": resource_id, "version": version},
                           other_work)

            candidates: list[dict[str, Any]] = []

            def consider(resource_record: dict[str, Any]) -> None:
                if resource_record["region"] not in regions:
                    return
                if not residency <= set(resource_record["residency"]):
                    return
                if resource_record["end"] < job["deadline"]:
                    return
                signal = _latest_signal(
                    signal_history, resource_record["region"], at)
                if signal is None:
                    return
                remaining = resource_record["capacity"] - booked.get(
                    (resource_record["resource_id"],
                     resource_record["version"]), 0)
                if remaining < work:
                    return
                total_cost = work * signal["unit_cost"]
                total_carbon = work * signal["carbon_intensity"]
                if total_cost > job["max_cost"] \
                        or total_carbon > job["carbon_cap"]:
                    return
                candidates.append({
                    "resource": dict(resource_record),
                    "signal": signal,
                    "total_cost": total_cost,
                    "total_carbon": total_carbon,
                })

            frozen_records = history.get(current_id)
            if frozen_records is None \
                    or current_version > len(frozen_records):
                raise ValueError("current binding references an "
                                 "unpublished resource version")
            consider(frozen_records[current_version - 1])
            for resource_id, versions in history.items():
                if resource_id == current_id:
                    continue
                active: dict[str, Any] | None = None
                for version_record in versions:
                    if version_record["start"] <= at <= version_record["end"]:
                        active = version_record
                if active is None:
                    continue
                consider(active)

            candidates.sort(key=lambda entry: (
                entry["signal"]["carbon_intensity"],
                entry["signal"]["unit_cost"],
                entry["resource"]["resource_id"]))
            if not candidates:
                raise LookupError("no feasible resource for the job at "
                                  "the reservation moment")

            winner = candidates[0]
            target = {"resource_id": winner["resource"]["resource_id"],
                      "version": winner["resource"]["version"]}
            if target != advice_record["target"]:
                raise LookupError("the advice target is no longer the "
                                  "first feasible candidate")
            if exec_plans.get(job_id):
                execution_state = max(
                    exec_plans[job_id].values(),
                    key=lambda plan: plan["attempt"])["state"]
            else:
                execution_state = "none"

            record: dict[str, Any] = {
                "job_id": job_id,
                "advice_key": advice_key,
                "at": at,
                "source": current,
                "target": target,
                "supply": dict(winner["resource"]),
                "signal": winner["signal"],
                "dispatch": decision["state"],
                "execution": execution_state,
                "reserved": "reserved",
            }
            intents.setdefault(job_id, {})[key] = record
            idempotency[key] = request
            events[key] = {"key": key, "request": dict(request),
                           "result": copy.deepcopy(record)}
            # A reservation-only ledger stays the legacy version-1 flat
            # form (exactly one reservation per job is possible there);
            # once a plan history or a settlement marker exists the
            # nested version-3 form is used, preserving every old entry.
            if not migration_plans and not markers:
                flat_intents = {job_id: next(iter(job_intents.values()))
                                for job_id, job_intents in intents.items()}
                _commit_file(
                    ledger_real,
                    _intent_canonical_bytes(intents, migration_plans,
                                            markers, idempotency, events,
                                            _INTENT_VERSION, flat_intents),
                    old_bytes, prefix=".rebalance-apply-")
            else:
                _commit_file(
                    ledger_real,
                    _intent_canonical_bytes(intents, migration_plans,
                                            markers, idempotency, events,
                                            _INTENT_VERSION_V3),
                    old_bytes, prefix=".rebalance-apply-")
            return copy.deepcopy(record), True


# ---------------------------------------------------------------------------
# Migration lifecycle: claim, execute and release over reserved intents
# ---------------------------------------------------------------------------


def _load_snapshot_layers(
    job_real: str,
    supply_real: str,
    signal_real: str,
    trades_real: str,
    dispatch_real: str,
    execution_real: str,
    advice_real: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]],
           dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]],
           dict[str, dict[str, dict[str, Any]]],
           dict[str, dict[str, Any]]]:
    # The seven input layers read as one consistent snapshot, exactly as
    # in apply: the acceptance, supply, signal, clearing, dispatch,
    # execution and advice files, each required to exist.
    accepted, job_map, job_events, job_raw = \
        _jobs._load_submit_file(job_real)
    if job_raw is None:
        raise FileNotFoundError(
            f"acceptance file {job_real!r} does not exist")
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
    signal_history, _signal_map, _signal_events, signal_raw = \
        _signals._load_file(signal_real)
    if signal_raw is None:
        raise FileNotFoundError(
            f"signal file {signal_real!r} does not exist")
    cleared, _clear_keys, trades_raw = _market._load_clear_ledger(
        trades_real, accepted, history, signal_history)
    if trades_raw is None:
        raise FileNotFoundError(
            f"clearing ledger {trades_real!r} does not exist")
    decisions, _dispatch_keys, _dispatch_events, dispatch_raw = \
        _dispatch._load_ledger(dispatch_real)
    if dispatch_raw is None:
        raise FileNotFoundError(
            f"dispatch ledger {dispatch_real!r} does not exist")
    exec_plans, _plan_keys, _plan_events = \
        _execution._load_existing_ledger(execution_real)[:3]
    advice_records, _advice_events, advice_raw = _load_ledger(
        advice_real, accepted, cleared, history, signal_history)
    if advice_raw is None:
        raise FileNotFoundError(
            f"advice ledger {advice_real!r} does not exist")
    return (accepted, history, signal_history, cleared, decisions,
            exec_plans, advice_records)


def _resolve_lifecycle_paths(
    jobs: str,
    supply: str,
    signals: str,
    trades: str,
    dispatch: str,
    execution: str,
    advice: str,
    ledger: str,
) -> tuple[str, str, str, str, str, str, str, str]:
    reals = tuple(os.path.realpath(path)
                  for path in (jobs, supply, signals, trades, dispatch,
                               execution, advice, ledger))
    if len(set(reals)) != 8:
        raise ValueError("the eight paths must be distinct real paths")
    return reals  # type: ignore[return-value]


def _load_lifecycle_intents(
    stack: contextlib.ExitStack,
    all_paths: set[str],
    ledger_real: str,
    job_real: str,
    supply_real: str,
    signal_real: str,
    trades_real: str,
    dispatch_real: str,
    execution_real: str,
    advice_real: str,
) -> tuple[dict[str, Any], ...]:
    for locked in sorted(all_paths):
        stack.enter_context(
            _lock(locked, shared=(locked != ledger_real)))
    accepted, history, signal_history, cleared, decisions, exec_plans, \
        advice_records = _load_snapshot_layers(
            job_real, supply_real, signal_real, trades_real,
            dispatch_real, execution_real, advice_real)
    intents, plans, idempotency, events, markers, version, old_bytes = \
        _load_intent_ledger(
            ledger_real, accepted, cleared, history, signal_history,
            advice_records)
    return (accepted, history, signal_history, cleared, decisions,
            exec_plans, advice_records, intents, plans, idempotency,
            events, markers, version, old_bytes)


def start(
    jobs: str,
    supply: str,
    signals: str,
    trades: str,
    dispatch: str,
    execution: str,
    advice: str,
    ledger: str,
    job_id: str,
    key: str,
    owner: str,
    lease_end: int,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Claim one reserved intent as an active migration plan.

    Call form and refusal order are unchanged from the single-migration
    contract; internally the claim is stored under its plan key (the
    start idempotency key) in the job's complete plan history, and an
    existing single-migration ledger is upgraded to the nested
    version-3 form without rewriting an old plan, receipt or audit
    event. Only the one open reservation of the latest round can be
    claimed; a pending settlement or an unsettled earlier round raises
    ``ValueError`` and starts nothing.
    """
    for value in (jobs, supply, signals, trades, dispatch, execution,
                  advice, ledger, job_id, key, owner):
        if not isinstance(value, str) or not value:
            raise ValueError("the eight paths, job_id, key and owner "
                             "must be non-empty strings")
    if not _is_plain_int(lease_end) or lease_end < 1:
        raise ValueError("lease_end must be a non-boolean positive "
                         "integer")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    (job_real, supply_real, signal_real, trades_real, dispatch_real,
     execution_real, advice_real, ledger_real) = \
        _resolve_lifecycle_paths(jobs, supply, signals, trades, dispatch,
                                 execution, advice, ledger)

    store = _get_store(ledger)
    with store.lock:
        with contextlib.ExitStack() as stack:
            (accepted, _history, _signal_history, cleared, decisions,
             exec_plans, _advice_records, intents, plans, idempotency,
             events, markers, _version, old_bytes) = \
                _load_lifecycle_intents(
                    stack,
                    {job_real, supply_real, signal_real, trades_real,
                     dispatch_real, execution_real, advice_real,
                     ledger_real}, ledger_real, job_real, supply_real,
                    signal_real, trades_real, dispatch_real,
                    execution_real, advice_real)

            request = {"action": "start", "job_id": job_id,
                       "owner": owner, "lease_end": lease_end, "at": at}
            binding = idempotency.get(key)
            if binding is not None:
                if binding != request:
                    raise ValueError("idempotency key was already used "
                                     "with a different request")
                return copy.deepcopy(plans[job_id][key]), False

            job = accepted.get(job_id)
            if job is None:
                raise KeyError(job_id)
            if job_id not in intents:
                raise KeyError(job_id)
            decision = decisions.get(job_id)
            if decision is None:
                raise KeyError(job_id)

            job_plans = plans.get(job_id, {})
            ordered = _ordered_plans(job_plans) if job_plans else []
            if _pending_marker_for_job(markers, job_id) is not None:
                raise ValueError("a pending settlement blocks further "
                                 "migrations")
            if ordered:
                latest_plan_key, latest_plan = ordered[-1]
                if latest_plan["state"] == "active" \
                        and markers.get(latest_plan_key) is None:
                    # An in-flight round is occupied: starting another is
                    # a permission error, not a generation conflict.
                    raise PermissionError("the intent already holds an "
                                          "active migration plan")
                if markers.get(latest_plan_key) is None:
                    raise ValueError("the previous migration plan must "
                                     "settle before another migration "
                                     "starts")
            pairing, open_keys = _pair_rounds(intents[job_id], ordered)
            if not open_keys:
                if ordered:
                    raise ValueError("the intent's migration plan is "
                                     "finished")
                raise KeyError(job_id)
            reserve_key = max(open_keys,
                             key=lambda k: (intents[job_id][k]["at"], k))
            intent = intents[job_id][reserve_key]

            job_exec_plans = exec_plans.get(job_id, {})
            if decision["state"] == "succeeded" \
                    or any(item["state"] == "completed"
                           for item in job_exec_plans.values()):
                raise ValueError("a finished booking cannot be migrated")
            if decision["state"] == "claimed" \
                    or any(item["state"] == "active"
                           for item in job_exec_plans.values()):
                raise PermissionError("the booking is claimed or has an "
                                      "active execution plan")
            if at > job["deadline"]:
                raise TimeoutError("start moment exceeds the job "
                                   "deadline")
            if lease_end > job["deadline"]:
                raise ValueError("lease end must not pass the job "
                                 "deadline")
            if at > lease_end:
                raise TimeoutError("the lease has already expired")

            new_plan: dict[str, Any] = {
                "job_id": job_id,
                "source": copy.deepcopy(intent["source"]),
                "target": copy.deepcopy(intent["target"]),
                "owner": owner,
                "lease_end": lease_end,
                "at": at,
                "state": "active",
                "steps": [],
            }
            plans.setdefault(job_id, {})[key] = new_plan
            idempotency[key] = request
            events[key] = {"key": key, "request": dict(request),
                           "result": copy.deepcopy(new_plan)}
            _commit_file(ledger_real,
                         _intent_canonical_bytes(intents, plans, markers,
                                                 idempotency, events,
                                                 _INTENT_VERSION_V3),
                         old_bytes, prefix=".rebalance-start-")
            return copy.deepcopy(new_plan), True


def record(
    jobs: str,
    supply: str,
    signals: str,
    trades: str,
    dispatch: str,
    execution: str,
    advice: str,
    ledger: str,
    job_id: str,
    key: str,
    owner: str,
    step: str,
    result: str,
    receipt: str,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Record one step receipt for the job's active migration plan.

    The active round is the plan whose start window contains the
    receipt moment; the call form, step sequencing and refusal order
    are otherwise unchanged. Every receipt is appended to that plan's
    own step history, so older rounds and their receipts are never
    overwritten.
    """
    for value in (jobs, supply, signals, trades, dispatch, execution,
                  advice, ledger, job_id, key, owner, receipt):
        if not isinstance(value, str) or not value:
            raise ValueError("the eight paths, job_id, key, owner and "
                             "receipt must be non-empty strings")
    if step not in _PLAN_STEPS:
        raise ValueError("step must be copy or switch")
    if result not in _STEP_RESULTS:
        raise ValueError("result must be succeeded or failed")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    (job_real, supply_real, signal_real, trades_real, dispatch_real,
     execution_real, advice_real, ledger_real) = \
        _resolve_lifecycle_paths(jobs, supply, signals, trades, dispatch,
                                 execution, advice, ledger)

    store = _get_store(ledger)
    with store.lock:
        with contextlib.ExitStack() as stack:
            (accepted, _history, _signal_history, _cleared, _decisions,
             _exec_plans, _advice_records, intents, plans, idempotency,
             events, markers, _version, old_bytes) = \
                _load_lifecycle_intents(
                    stack,
                    {job_real, supply_real, signal_real, trades_real,
                     dispatch_real, execution_real, advice_real,
                     ledger_real}, ledger_real, job_real, supply_real,
                    signal_real, trades_real, dispatch_real,
                    execution_real, advice_real)

            request = {"action": "record", "job_id": job_id,
                       "owner": owner, "step": step, "result": result,
                       "receipt": receipt, "at": at}
            binding = idempotency.get(key)
            if binding is not None:
                if binding != request:
                    raise ValueError("idempotency key was already used "
                                     "with a different request")
                plan_key = _plan_key_for_request(request, plans[job_id])
                return copy.deepcopy(plans[job_id][plan_key]), False

            if job_id not in accepted:
                raise KeyError(job_id)
            if job_id not in intents:
                raise KeyError(job_id)
            if job_id not in plans:
                raise ValueError("the intent has no migration plan yet")
            plan_key = _plan_key_for_request(request, plans[job_id])
            plan = plans[job_id][plan_key]
            if plan["state"] != "active":
                raise ValueError("plan is not active")
            if plan["owner"] != owner:
                raise PermissionError("record requires the plan owner")
            if at > plan["lease_end"]:
                raise TimeoutError("the lease has already expired")
            if step != _PLAN_STEPS[len(plan["steps"])]:
                raise ValueError("step is not the plan's next pending "
                                 "step")
            if at < plan["at"]:
                raise ValueError("receipt moment must not precede the "
                                 "plan start moment")
            if plan["steps"] and at < plan["steps"][-1]["at"]:
                raise ValueError("receipt moment must not precede the "
                                 "previous receipt")

            plan["steps"].append({"step": step, "result": result,
                                  "receipt": receipt, "at": at})
            if result == "failed":
                plan["state"] = "failed"
            elif len(plan["steps"]) == len(_PLAN_STEPS):
                plan["state"] = "migrated"
            idempotency[key] = request
            events[key] = {"key": key, "request": dict(request),
                           "result": copy.deepcopy(plan)}
            _commit_file(ledger_real,
                         _intent_canonical_bytes(intents, plans, markers,
                                                 idempotency, events,
                                                 _INTENT_VERSION_V3),
                         old_bytes, prefix=".rebalance-record-")
            return copy.deepcopy(plan), True


def recover(
    jobs: str,
    supply: str,
    signals: str,
    trades: str,
    dispatch: str,
    execution: str,
    advice: str,
    ledger: str,
    job_id: str,
    key: str,
    owner: str,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Interrupt the active migration plan whose lease has expired.

    The active round is selected by the recovery moment's start window;
    the recovered plan keeps every recorded receipt and older rounds
    are never touched.
    """
    for value in (jobs, supply, signals, trades, dispatch, execution,
                  advice, ledger, job_id, key, owner):
        if not isinstance(value, str) or not value:
            raise ValueError("the eight paths, job_id, key and owner "
                             "must be non-empty strings")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    (job_real, supply_real, signal_real, trades_real, dispatch_real,
     execution_real, advice_real, ledger_real) = \
        _resolve_lifecycle_paths(jobs, supply, signals, trades, dispatch,
                                 execution, advice, ledger)

    store = _get_store(ledger)
    with store.lock:
        with contextlib.ExitStack() as stack:
            (accepted, _history, _signal_history, _cleared, _decisions,
             _exec_plans, _advice_records, intents, plans, idempotency,
             events, markers, _version, old_bytes) = \
                _load_lifecycle_intents(
                    stack,
                    {job_real, supply_real, signal_real, trades_real,
                     dispatch_real, execution_real, advice_real,
                     ledger_real}, ledger_real, job_real, supply_real,
                    signal_real, trades_real, dispatch_real,
                    execution_real, advice_real)

            request = {"action": "recover", "job_id": job_id,
                       "owner": owner, "at": at}
            binding = idempotency.get(key)
            if binding is not None:
                if binding != request:
                    raise ValueError("idempotency key was already used "
                                     "with a different request")
                plan_key = _plan_key_for_request(request, plans[job_id])
                return copy.deepcopy(plans[job_id][plan_key]), False

            if job_id not in accepted:
                raise KeyError(job_id)
            if job_id not in intents:
                raise KeyError(job_id)
            if job_id not in plans:
                raise ValueError("the intent has no migration plan yet")
            plan_key = _plan_key_for_request(request, plans[job_id])
            plan = plans[job_id][plan_key]
            if plan["state"] != "active":
                raise ValueError("plan is not active")
            if plan["owner"] != owner:
                raise PermissionError("recover requires the plan owner")
            if at <= plan["lease_end"]:
                raise PermissionError("the lease has not expired yet")

            plan["state"] = "interrupted"
            idempotency[key] = request
            events[key] = {"key": key, "request": dict(request),
                           "result": copy.deepcopy(plan)}
            _commit_file(ledger_real,
                         _intent_canonical_bytes(intents, plans, markers,
                                                 idempotency, events,
                                                 _INTENT_VERSION_V3),
                         old_bytes, prefix=".rebalance-recover-")
            return copy.deepcopy(plan), True


# ---------------------------------------------------------------------------
# Terminal settlement: a durable current-occupancy binding in its own ledger
# ---------------------------------------------------------------------------
#
# evaluate/apply/start/record/recover cover the advice, the reservation and
# the migration attempt, yet none of them leaves later dispatch and
# execution a *current* binding that survives the terminal plan: the
# immutable trade keeps pointing at the source version forever while the
# intent ledger only describes that one attempt. settle closes that gap
# without rewriting any of the existing layers -- it writes one
# independent settlement ledger, read back through current.
#
# Every settlement lands through a crash-safe two-phase commit. The
# request is first persisted durably as ``pending`` together with its
# idempotency binding and audit event, and only then advanced to its
# final ``active`` or ``compensated`` state in a second synced atomic
# write. A crash between the two writes is resumed by the same key
# carrying the same request: the re-entry completes the missing stage and
# still returns the record with ``True``, never repeating an occupancy,
# generation or audit action. Only an already completed same-key
# same-request call returns the current record with ``False`` and writes
# no bytes.

_SETTLE_VERSION = 1
_SETTLE_ROOT_FIELDS = ("version", "records", "idempotency", "audit")
_SETTLE_FIELDS = ("job_id", "plan_key", "generation", "migration",
                  "before", "after", "at", "state", "audit")
_SETTLE_REQUEST_FIELDS = ("job_id", "plan_key", "at")
_SETTLE_STATES = ("pending", "active", "compensated")
_SETTLE_TERMINAL = ("migrated", "failed", "interrupted")
_SETTLE_FINAL_STATES = ("active", "compensated")




def _job_generation_context(
    accepted: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
    plans_for_job: dict[str, dict[str, Any]],
    markers: dict[str, dict[str, Any]],
    records: dict[str, dict[str, Any]],
    job_id: str,
) -> dict[str, Any]:
    # Walk the job's plan rounds in order and align them with the
    # settlement records. Settlements form a strict prefix of the rounds:
    # a later round can only exist once the earlier plan is settled, so
    # the first gap must also be the last round with a record. Every
    # generation follows the previous completion by exactly one and
    # ``before`` chains to the previous ``after`` (the trade binds the
    # first generation), so a gap, a fork or a broken chain fails here.
    trade = trades[job_id]
    trade_selection = {"resource_id": trade["resource_id"],
                       "version": trade["version"]}
    ordered = _ordered_plans(plans_for_job)
    records_by_plan: dict[str, dict[str, Any]] = {}
    pending_count = 0
    for record in records.values():
        if record["job_id"] != job_id:
            continue
        if record["plan_key"] in records_by_plan:
            raise ValueError("each migration plan can settle at most "
                             "once")
        records_by_plan[record["plan_key"]] = record
        if record["state"] == "pending":
            pending_count += 1
    if pending_count > 1:
        raise ValueError("a job can carry at most one pending settlement")

    last_completed: dict[str, Any] | None = None
    saw_gap = False
    pending_seen = False
    for index, (plan_key, plan) in enumerate(ordered):
        record = records_by_plan.get(plan_key)
        marker = markers.get(plan_key)
        # A final record with no marker is the single-migration legacy
        # form (settle predates the marker mirror): it is a completed
        # generation and stays readable without any write.
        legacy_completed = record is not None \
            and record["state"] in _SETTLE_FINAL_STATES and marker is None
        completed_record = legacy_completed or (
            record is not None
            and record["state"] in _SETTLE_FINAL_STATES
            and marker is not None and marker["state"] == "settled")
        open_record = marker is not None and marker["state"] == "pending"
        if not completed_record and not open_record:
            saw_gap = True
            continue
        if saw_gap:
            raise ValueError("settlement generations must not skip a "
                             "migration round")
        if open_record:
            # The durable pending window -- marker pending, with the
            # independent record absent, pending or already final but
            # not mirrored as settled yet -- is always the open last
            # round and never advances the running generation.
            pending_seen = True
            if index != len(ordered) - 1:
                raise ValueError("a pending settlement must be the open "
                                 "last round")
            if record is not None and marker["key"] in records \
                    and record["state"] in _SETTLE_FINAL_STATES:
                # Final-but-unmirrored record: its generation and before
                # must already chain correctly so the flip only commits
                # a consistent state.
                expected_generation = (last_completed["generation"] + 1
                                       if last_completed is not None else 1)
                if record["generation"] != expected_generation:
                    raise ValueError("settlement generations must be "
                                     "continuous")
                expected_before = (last_completed["after"]
                                   if last_completed is not None
                                   else trade_selection)
                if record["before"] != expected_before:
                    raise ValueError("settlement before must equal the "
                                     "previous completed after (or the "
                                     "traded binding)")
                if record["after"] != _plan_outcome(plan):
                    raise ValueError("settlement after must follow the "
                                     "terminal plan outcome")
            continue
        assert completed_record and record is not None
        expected_generation = (last_completed["generation"] + 1
                               if last_completed is not None else 1)
        if record["generation"] != expected_generation:
            raise ValueError("settlement generations must be continuous")
        expected_before = (last_completed["after"]
                           if last_completed is not None
                           else trade_selection)
        if record["before"] != expected_before:
            raise ValueError("settlement before must equal the previous "
                             "completed after (or the traded binding)")
        if record["after"] != _plan_outcome(plan):
            raise ValueError("settlement after must follow the "
                             "terminal plan outcome")
        last_completed = record
    if pending_seen and sum(
            1 for m in markers.values()
            if m["job_id"] == job_id and m["state"] == "pending") > 1:
        raise ValueError("a job can carry at most one pending settlement")
    return {"trade": trade, "trade_selection": trade_selection,
            "ordered": ordered, "records_by_plan": records_by_plan,
            "last_completed": last_completed}


def _settlement_round_audit(
    job_id: str,
    plan_key: str,
    plans_for_job: dict[str, dict[str, Any]],
    intents_for_job: dict[str, dict[str, Any]],
    intent_idempotency: dict[str, dict[str, Any]],
) -> list[str]:
    # The audit association binds one settlement to exactly the intent
    # ledger actions its round committed: the reservation it consumed,
    # the claim, the step receipts and the recovery. Rounds are separated
    # by their start moments, so an action can never bleed into another
    # round's settlement.
    ordered = _ordered_plans(plans_for_job)
    pairing, _open = _pair_rounds(intents_for_job, ordered)
    index = next(i for i, (key, _) in enumerate(ordered)
                 if key == plan_key)
    _plan = ordered[index][1]
    later_at = ordered[index + 1][1]["at"] \
        if index + 1 < len(ordered) else None
    return _round_action_keys(job_id, _plan, later_at, pairing[plan_key],
                              intent_idempotency)


def _validate_settlement_record(
    record: object,
    accepted: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
    plans: dict[str, dict[str, dict[str, Any]]],
    intents: dict[str, dict[str, dict[str, Any]]],
    intent_idempotency: dict[str, dict[str, Any]],
    intent_events: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(record, dict) \
            or set(record.keys()) != set(_SETTLE_FIELDS):
        raise ValueError("settlement record has invalid fields")
    job_id = record["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("settlement job_id must be a non-empty string")
    plan_key = record["plan_key"]
    if not isinstance(plan_key, str) or not plan_key:
        raise ValueError("settlement plan_key must be a non-empty string")
    generation = record["generation"]
    if not _is_plain_int(generation) or generation < 1:
        raise ValueError("settlement generation must be a positive "
                         "integer")
    migration = record["migration"]
    if migration not in _SETTLE_TERMINAL:
        raise ValueError("settlement migration state is invalid")
    before = _validate_selection(record["before"],
                                 "settlement previous binding")
    after = _validate_selection(record["after"],
                                "settlement current binding")
    at = record["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("settlement at must be a non-boolean "
                         "non-negative integer")
    state = record["state"]
    if state not in _SETTLE_STATES:
        raise ValueError("settlement state is invalid")
    audit_raw = record["audit"]
    if not isinstance(audit_raw, list) or not audit_raw \
            or any(not isinstance(key, str) or not key
                   for key in audit_raw):
        raise ValueError("settlement audit association must be a "
                         "non-empty list of non-empty strings")
    if len(set(audit_raw)) != len(audit_raw) \
            or list(audit_raw) != sorted(audit_raw):
        raise ValueError("settlement audit association must hold "
                         "distinct keys ordered by code point")

    if job_id not in accepted:
        raise ValueError("settlement record must reference an accepted "
                         "job")
    trade = trades.get(job_id)
    if trade is None:
        raise ValueError("settlement record must reference a recorded "
                         "trade")
    plans_for_job = plans.get(job_id)
    if plans_for_job is None or plan_key not in plans_for_job:
        raise ValueError("settlement record must reference a recorded "
                         "migration plan")
    plan = plans_for_job[plan_key]
    intents_for_job = intents.get(job_id, {})

    # plan_key is the start request that created the plan; the recorded
    # start snapshot must agree with the plan's fixed fields.
    start_request = intent_idempotency.get(plan_key)
    if start_request is None or start_request.get("action") != "start" \
            or start_request.get("job_id") != job_id:
        raise ValueError("settlement plan_key must reference the plan's "
                         "start request")
    started = intent_events.get(plan_key)
    if started is None or any(
            started["result"][field] != plan[field] for field in
            ("source", "target", "owner", "lease_end", "at")):
        raise ValueError("settlement plan_key does not match its "
                         "recorded plan")

    # The before/after continuity across generations is enforced by the
    # ledger-level walker; here the record only has to agree with the one
    # terminal plan it names.
    ordered = _ordered_plans(plans_for_job)
    index = next(i for i, (key, _) in enumerate(ordered)
                 if key == plan_key)

    if plan["state"] != migration:
        raise ValueError("settlement migration state must match the "
                         "terminal plan state")
    expected_after = _plan_outcome(plan)
    if after != expected_after:
        raise ValueError("settlement current binding must match the "
                         "terminal plan outcome")

    pairing, _open = _pair_rounds(intents_for_job, ordered)
    if plan_key not in pairing:
        raise ValueError("settled plan must consume a recorded "
                         "reservation intent")
    round_keys = _round_action_keys(
        job_id, plan, ordered[index + 1][1]["at"]
        if index + 1 < len(ordered) else None,
        pairing[plan_key], intent_idempotency)
    if at < _terminal_plan_at(plan, round_keys, intent_idempotency):
        raise ValueError("settlement moment must not precede the "
                         "terminal plan evidence")
    if audit_raw != round_keys:
        raise ValueError("settlement audit association does not match "
                         "the intent ledger round")

    return {
        "job_id": job_id,
        "plan_key": plan_key,
        "generation": generation,
        "migration": migration,
        "before": before,
        "after": after,
        "at": at,
        "state": state,
        "audit": list(audit_raw),
    }


def _validate_settlement_request(request: object) -> dict[str, Any]:
    if not isinstance(request, dict) \
            or set(request.keys()) != set(_SETTLE_REQUEST_FIELDS):
        raise ValueError("settlement request has invalid fields")
    job_id = request["job_id"]
    plan_key = request["plan_key"]
    at = request["at"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("settlement request job_id must be a non-empty "
                         "string")
    if not isinstance(plan_key, str) or not plan_key:
        raise ValueError("settlement request plan_key must be a "
                         "non-empty string")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("settlement request at must be a non-boolean "
                         "non-negative integer")
    return {"job_id": job_id, "plan_key": plan_key, "at": at}


def _validate_settlement_ledger(
    data: object,
    accepted: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
    plans: dict[str, dict[str, dict[str, Any]]],
    intents: dict[str, dict[str, dict[str, Any]]],
    intent_idempotency: dict[str, dict[str, Any]],
    intent_events: dict[str, dict[str, Any]],
    markers: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]]]:
    if not isinstance(data, dict) \
            or set(data.keys()) != set(_SETTLE_ROOT_FIELDS):
        raise ValueError("settlement ledger root must be an object with "
                         "keys version, records, idempotency and audit")
    if not _is_plain_int(data["version"]) \
            or data["version"] != _SETTLE_VERSION:
        raise ValueError("unsupported settlement ledger version")
    records_raw = data["records"]
    idempotency_raw = data["idempotency"]
    audit_raw = data["audit"]
    if not isinstance(records_raw, dict) \
            or not isinstance(idempotency_raw, dict) \
            or not isinstance(audit_raw, dict):
        raise ValueError("settlement sections must be objects")
    _check_sorted_keys(records_raw, "records")
    _check_sorted_keys(idempotency_raw, "idempotency")
    _check_sorted_keys(audit_raw, "audit")

    records: dict[str, dict[str, Any]] = {}
    for key, record_raw in records_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("settlement keys must be non-empty strings")
        records[key] = _validate_settlement_record(
            record_raw, accepted, trades, plans, intents,
            intent_idempotency, intent_events)

    # One job can settle many plans, but each plan key settles exactly
    # once, the generations run continuously from the trade through
    # every completed record, and a pending record is always the open
    # round. The walker enforces all of that per job.
    settled_jobs = {record["job_id"] for record in records.values()}
    for job_id in settled_jobs:
        _job_generation_context(
            accepted, trades, plans.get(job_id, {}), markers, records,
            job_id)

    # The intent-ledger marker mirrors the settlement ledger so apply
    # and the migration lifecycle, which never read the settlement
    # ledger, can see an open settlement. A settled marker pairs with a
    # final record; a pending marker is the crash-safe open window and
    # may pair with an absent, pending or already-final record. Any
    # other combination is cross-ledger drift.
    for plan_key, marker in markers.items():
        settle_key = marker["key"]
        record = records.get(settle_key)
        if record is None or record["plan_key"] != plan_key \
                or record["job_id"] != marker["job_id"]:
            if marker["state"] == "settled":
                raise ValueError("a settled marker must reference a "
                                 "final settlement record")
            # A pending marker is allowed between its marker commit and
            # the pending settlement record commit.
            continue
        if marker["state"] == "settled" \
                and record["state"] not in _SETTLE_FINAL_STATES:
            raise ValueError("a settled marker must reference a final "
                             "settlement record")
        if marker["state"] == "pending" \
                and record["state"] not in _SETTLE_STATES:
            raise ValueError("settlement marker state does not match the "
                             "settlement record")
    for settle_key, record in records.items():
        marker = markers.get(record["plan_key"])
        if marker is None:
            # A final record without a marker is the pre-marker legacy
            # settlement; a pending record must always have a pending
            # marker, so that combination is drift.
            if record["state"] in _SETTLE_FINAL_STATES:
                continue
            raise ValueError("a pending settlement must carry a pending "
                             "marker")
        if marker["key"] != settle_key \
                or marker["job_id"] != record["job_id"]:
            raise ValueError("settlement record and its intent-ledger "
                             "marker disagree")
        if record["state"] in _SETTLE_FINAL_STATES \
                and marker["state"] == "settled":
            continue
        if record["state"] == "pending" and marker["state"] == "pending":
            continue
        if record["state"] in _SETTLE_FINAL_STATES \
                and marker["state"] == "pending":
            # Final record committed an instant before the marker flip.
            continue
        raise ValueError("settlement marker state does not match the "
                         "settlement record")

    idempotency: dict[str, dict[str, Any]] = {}
    for key, request_raw in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        request = _validate_settlement_request(request_raw)
        if key not in records:
            raise ValueError("idempotency entry must reference a "
                             "recorded settlement")
        record = records[key]
        if request["job_id"] != record["job_id"] \
                or request["plan_key"] != record["plan_key"] \
                or request["at"] != record["at"]:
            raise ValueError("idempotency entry does not match its "
                             "settlement")
        idempotency[key] = request

    events: dict[str, dict[str, Any]] = {}
    for key, event_raw in audit_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event_raw, dict) \
                or set(event_raw.keys()) != set(_EVENT_FIELDS):
            raise ValueError("settlement audit event has invalid fields")
        if event_raw["key"] != key:
            raise ValueError("audit event key does not match its map key")
        request = _validate_settlement_request(event_raw["request"])
        if request != idempotency.get(key):
            raise ValueError("audit event does not match its "
                             "idempotency entry")
        result = _validate_settlement_record(
            event_raw["result"], accepted, trades, plans, intents,
            intent_idempotency, intent_events)
        if result != records[key]:
            raise ValueError("audit event result does not match its "
                             "record")
        events[key] = {"key": key, "request": request, "result": result}

    if set(events) != set(records) \
            or set(idempotency) != set(records):
        raise ValueError("settlement sections do not match")
    return records, idempotency, events


def _settlement_canonical_bytes(
    records: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, sections in their
    # fixed field order, records/idempotency/audit each keyed by
    # idempotency key in code-point order, terminated by exactly one
    # newline.
    payload = {
        "version": _SETTLE_VERSION,
        "records": {key: records[key] for key in sorted(records)},
        "idempotency": {key: idempotency[key]
                        for key in sorted(idempotency)},
        "audit": {key: events[key] for key in sorted(events)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_settlement_ledger(
    realpath: str,
    accepted: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
    plans: dict[str, dict[str, dict[str, Any]]],
    intents: dict[str, dict[str, dict[str, Any]]],
    intent_idempotency: dict[str, dict[str, Any]],
    intent_events: dict[str, dict[str, Any]],
    markers: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]], bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return {}, {}, {}, None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"settlement ledger {realpath!r} is not valid UTF-8") from exc
    try:
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"settlement ledger {realpath!r} is not valid JSON") from exc
    records, idempotency, events = _validate_settlement_ledger(
        data, accepted, trades, plans, intents, intent_idempotency,
        intent_events, markers)
    if raw != _settlement_canonical_bytes(records, idempotency, events):
        raise ValueError(
            f"settlement ledger {realpath!r} is not in canonical "
            "compact form")
    return records, idempotency, events, raw


def _resolve_nine_paths(
    jobs: str,
    supply: str,
    signals: str,
    trades: str,
    dispatch: str,
    execution: str,
    advice: str,
    ledger: str,
    settlements: str,
) -> tuple[str, str, str, str, str, str, str, str, str]:
    reals = tuple(os.path.realpath(path)
                  for path in (jobs, supply, signals, trades, dispatch,
                               execution, advice, ledger, settlements))
    if len(set(reals)) != 9:
        raise ValueError("the nine paths must be distinct real paths")
    return reals  # type: ignore[return-value]


def _load_settle_snapshot(
    job_real: str,
    supply_real: str,
    signal_real: str,
    trades_real: str,
    dispatch_real: str,
    execution_real: str,
    advice_real: str,
    ledger_real: str,
) -> tuple[dict[str, Any], ...]:
    # The same eight layers every lifecycle call reads as one snapshot;
    # the intent ledger is a required input here because settlement
    # presupposes a recorded plan.
    (accepted, history, signal_history, cleared, decisions, exec_plans,
     advice_records) = _load_snapshot_layers(
        job_real, supply_real, signal_real, trades_real, dispatch_real,
        execution_real, advice_real)
    intents, plans, intent_idempotency, intent_events, markers, _version, \
        intent_raw = _load_intent_ledger(
            ledger_real, accepted, cleared, history, signal_history,
            advice_records)
    if intent_raw is None:
        raise FileNotFoundError(
            f"intent ledger {ledger_real!r} does not exist")
    return (accepted, history, signal_history, cleared, decisions,
            exec_plans, advice_records, intents, plans, intent_idempotency,
            intent_events, markers)


def _pending_marker_for_job(
    markers: dict[str, dict[str, Any]],
    job_id: str,
) -> dict[str, Any] | None:
    pending = [marker for marker in markers.values()
               if marker["job_id"] == job_id and marker["state"] == "pending"]
    if len(pending) > 1:
        raise ValueError("a job can carry at most one pending settlement")
    return pending[0] if pending else None


def settle(
    jobs: str,
    supply: str,
    signals: str,
    trades: str,
    dispatch: str,
    execution: str,
    advice: str,
    ledger: str,
    settlements: str,
    job_id: str,
    plan_key: str,
    key: str,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Settle a terminal migration into a durable current binding.

    Each plan key settles at most once. The generation runs continuously
    from the immutable trade (generation 1 for the first settlement, the
    previous completed settlement's generation plus one afterwards) and
    ``before`` equals the previous completion's ``after``; the first
    generation's ``before`` is the traded binding. A ``migrated`` plan
    activates the target as the current binding; a ``failed`` or
    ``interrupted`` plan compensates and keeps the source binding.

    The settlement spans two ledgers. A pending marker is first appended
    to the intent ledger, so while the settlement is open no further
    reservation or migration can start for the job; the pending
    settlement record is then committed to the independent ledger; both
    finally advance together. A same-key same-request re-entry resumes
    whichever stage was interrupted and still returns ``True``; an
    equivalent replay after completion returns the record with ``False``
    without writing a byte. Another idempotency key for the same plan,
    a generation gap, a broken before-chain or a pending settlement for
    the job raises ``ValueError``.
    """
    for value in (jobs, supply, signals, trades, dispatch, execution,
                  advice, ledger, settlements, job_id, plan_key, key):
        if not isinstance(value, str) or not value:
            raise ValueError("the nine paths, job_id, plan_key and key "
                             "must be non-empty strings")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    (job_real, supply_real, signal_real, trades_real, dispatch_real,
     execution_real, advice_real, ledger_real, settlement_real) = \
        _resolve_nine_paths(jobs, supply, signals, trades, dispatch,
                            execution, advice, ledger, settlements)

    settle_store = _get_store(settlement_real)
    intent_store = _get_store(ledger_real)
    with settle_store.lock, intent_store.lock:
        with contextlib.ExitStack() as stack:
            # Seven snapshot locks shared; the two written ledgers take
            # exclusive locks, all nine in resolved real-path order.
            exclusive = {settlement_real, ledger_real}
            for locked in sorted({job_real, supply_real, signal_real,
                                  trades_real, dispatch_real,
                                  execution_real, advice_real, ledger_real,
                                  settlement_real}):
                stack.enter_context(
                    _lock(locked, shared=locked not in exclusive))

            (accepted, _history, _signal_history, cleared, _decisions,
             _exec_plans, _advice_records, intents, plans,
             intent_idempotency, intent_events, markers) = \
                _load_settle_snapshot(
                    job_real, supply_real, signal_real, trades_real,
                    dispatch_real, execution_real, advice_real,
                    ledger_real)
            records, settle_idempotency, events, old_settle_bytes = \
                _load_settlement_ledger(
                    settlement_real, accepted, cleared, plans, intents,
                    intent_idempotency, intent_events, markers)
            old_intent_bytes = Path(ledger_real).read_bytes()

            request = {"job_id": job_id, "plan_key": plan_key, "at": at}
            existing_request = settle_idempotency.get(key)
            if existing_request is not None and existing_request != request:
                raise ValueError("idempotency key was already used with "
                                 "a different request")
            stored = records.get(key)
            marker = markers.get(plan_key)
            if marker is not None and marker["job_id"] != job_id:
                raise ValueError("settlement plan_key belongs to another "
                                 "job")
            if marker is not None and marker["key"] != key:
                raise ValueError("the migration plan is already settled "
                                 "or settling under another idempotency "
                                 "key")

            plans_for_job = plans.get(job_id, {})
            plan = plans_for_job.get(plan_key)
            # Classify the crash stage before touching either file.
            if stored is not None and stored["state"] in \
                    _SETTLE_FINAL_STATES and marker is not None \
                    and marker["state"] == "settled":
                # Completed equivalent replay: nothing is rewritten.
                return copy.deepcopy(stored), False
            if stored is not None and stored["state"] in \
                    _SETTLE_FINAL_STATES and marker is None:
                # A pre-marker legacy final settlement: the independent
                # record is already complete, so heal only the intent
                # ledger marker without touching a settlement byte.
                stage = "heal-marker"
            elif stored is None and marker is None and existing_request \
                    is None:
                stage = "fresh"
            elif marker is not None and marker["state"] == "pending":
                # The marker is the durable claim; the independent
                # record may be missing, pending or already final.
                stage = ("final-record" if stored is not None
                         and stored["state"] in _SETTLE_FINAL_STATES
                         else "pending-record")
            else:
                raise ValueError("settlement and marker state are "
                                 "inconsistent")

            if stage == "fresh":
                if job_id not in accepted:
                    raise KeyError(job_id)
                if job_id not in intents:
                    raise KeyError(job_id)
                start_request = intent_idempotency.get(plan_key)
                if start_request is None \
                        or start_request.get("action") != "start" \
                        or start_request.get("job_id") != job_id:
                    raise KeyError(plan_key)
                if plan is None:
                    raise KeyError(plan_key)
                same_plan_record = next(
                    (record for record in records.values()
                     if record["job_id"] == job_id
                     and record["plan_key"] == plan_key), None)
                if same_plan_record is not None:
                    raise ValueError("the terminal migration is already "
                                     "settled under another idempotency "
                                     "key")
                if _pending_marker_for_job(markers, job_id) is not None:
                    raise ValueError("a pending settlement blocks further "
                                     "settlements")
                if plan["state"] == "active":
                    raise PermissionError("the migration plan is still "
                                          "active")
                if plan["state"] not in _SETTLE_TERMINAL:
                    raise ValueError("the migration plan is not in a "
                                     "terminal state")
                context = _job_generation_context(
                    accepted, cleared, plans_for_job, markers, records,
                    job_id)
                ordered = context["ordered"]
                position = next(i for i, (p_key, _) in enumerate(ordered)
                                if p_key == plan_key)
                earlier_keys = {p_key for p_key, _ in ordered[:position]}
                fully_settled = {p_key for p_key, marker in markers.items()
                                 if marker["job_id"] == job_id
                                 and marker["state"] == "settled"}
                if not earlier_keys <= fully_settled:
                    raise ValueError("earlier migration rounds must settle "
                                     "before a later round")
                prev_completed = context["last_completed"]
                generation = (prev_completed["generation"] + 1
                              if prev_completed is not None else 1)
                before = copy.deepcopy(
                    prev_completed["after"] if prev_completed is not None
                    else context["trade_selection"])
            else:
                if plan is None or job_id not in intents:
                    raise ValueError("a pending settlement lost its "
                                     "migration plan")
                if stored is not None:
                    if stored["job_id"] != job_id:
                        raise ValueError("pending settlement references "
                                         "the wrong job")
                    generation = stored["generation"]
                    before = copy.deepcopy(stored["before"])
                else:
                    context = _job_generation_context(
                        accepted, cleared, plans_for_job, markers, records,
                        job_id)
                    prev_completed = context["last_completed"]
                    generation = (prev_completed["generation"] + 1
                                  if prev_completed is not None else 1)
                    before = copy.deepcopy(
                        prev_completed["after"]
                        if prev_completed is not None
                        else context["trade_selection"])
                if plan["state"] not in _SETTLE_TERMINAL:
                    raise ValueError("a pending settlement contradicts "
                                     "the migration plan state")

            round_keys = _settlement_round_audit(
                job_id, plan_key, plans_for_job, intents[job_id],
                intent_idempotency)
            if stage == "heal-marker":
                # The independent record is already a legacy completion;
                # only mirror the settled marker into the intent ledger,
                # without writing a settlement byte, and return the
                # stored record as resumed (True).
                assert stored is not None
                if stored["audit"] != round_keys \
                        or request != {"job_id": stored["job_id"],
                                       "plan_key": stored["plan_key"],
                                       "at": stored["at"]}:
                    raise ValueError("legacy settlement replay carries a "
                                     "changed request")
                markers[plan_key] = {"job_id": job_id, "key": key,
                                     "state": "settled"}
                _commit_file(
                    ledger_real,
                    _intent_canonical_bytes(intents, plans, markers,
                                            intent_idempotency,
                                            intent_events,
                                            _INTENT_VERSION_V3),
                    old_intent_bytes,
                    prefix=".rebalance-settle-marker-final-")
                return copy.deepcopy(stored), True
            if at < _terminal_plan_at(plan, round_keys,
                                      intent_idempotency):
                raise ValueError("settlement moment must not precede the "
                                 "terminal plan evidence")

            final_record: dict[str, Any] = {
                "job_id": job_id,
                "plan_key": plan_key,
                "generation": generation,
                "migration": plan["state"],
                "before": before,
                "after": _plan_outcome(plan),
                "at": at,
                "state": ("active" if plan["state"] == "migrated"
                          else "compensated"),
                "audit": round_keys,
            }
            pending_record = dict(final_record, state="pending")

            # Stage 1: the pending marker lands in the intent ledger
            # first. A legacy single-migration file is upgraded to the
            # nested history here, with every old plan and audit event
            # preserved byte-for-byte semantically.
            if marker is None:
                markers[plan_key] = {"job_id": job_id, "key": key,
                                     "state": "pending"}
                pending_intent_bytes = _intent_canonical_bytes(
                    intents, plans, markers, intent_idempotency,
                    intent_events, _INTENT_VERSION_V3)
                _commit_file(ledger_real, pending_intent_bytes,
                             old_intent_bytes,
                             prefix=".rebalance-settle-marker-")
            else:
                pending_intent_bytes = _intent_canonical_bytes(
                    intents, plans, markers, intent_idempotency,
                    intent_events, _INTENT_VERSION_V3)

            # Stage 2: the pending settlement record, its binding and
            # its audit event in the independent ledger.
            if stored is None:
                records[key] = pending_record
                settle_idempotency[key] = dict(request)
                events[key] = {"key": key, "request": dict(request),
                               "result": copy.deepcopy(pending_record)}
                pending_settle_bytes = _settlement_canonical_bytes(
                    records, settle_idempotency, events)
                _commit_file(settlement_real, pending_settle_bytes,
                             old_settle_bytes,
                             prefix=".rebalance-settle-")
            else:
                pending_settle_bytes = old_settle_bytes

            # Stage 3: advance the record to its final state. The
            # pending bytes remain the rollback base, so a crash leaves
            # a resumable pending settlement rather than restoring the
            # pre-call bytes.
            if stage != "final-record":
                records[key] = final_record
                events[key] = {"key": key, "request": dict(request),
                               "result": copy.deepcopy(final_record)}
                _commit_file(
                    settlement_real,
                    _settlement_canonical_bytes(records, settle_idempotency,
                                                events),
                    pending_settle_bytes,
                    prefix=".rebalance-settle-final-")

            # Stage 4: flip the intent-ledger marker to settled. A
            # failure rolls back to the pending-marker bytes, which the
            # same-key re-entry reconciles without settling twice.
            markers[plan_key] = {"job_id": job_id, "key": key,
                                 "state": "settled"}
            _commit_file(
                ledger_real,
                _intent_canonical_bytes(intents, plans, markers,
                                        intent_idempotency, intent_events,
                                        _INTENT_VERSION_V3),
                pending_intent_bytes,
                prefix=".rebalance-settle-marker-final-")
            return copy.deepcopy(final_record), True


def current(
    jobs: str,
    supply: str,
    signals: str,
    trades: str,
    dispatch: str,
    execution: str,
    advice: str,
    ledger: str,
    settlements: str,
    job_id: str,
) -> dict[str, object]:
    """Read the job's latest completed resource binding read-only.

    Pending settlements are ignored: the view stably returns the most
    recent ``active`` or ``compensated`` record, or the immutable trade
    binding with generation 0 and state ``traded`` when no settlement
    completed. An unknown job raises ``KeyError``; an accepted job
    without a trade raises ``LookupError``. Nothing is created or
    written.
    """
    for value in (jobs, supply, signals, trades, dispatch, execution,
                  advice, ledger, settlements, job_id):
        if not isinstance(value, str) or not value:
            raise ValueError("the nine paths and job_id must be "
                             "non-empty strings")

    (job_real, supply_real, signal_real, trades_real, dispatch_real,
     execution_real, advice_real, ledger_real, settlement_real) = \
        _resolve_nine_paths(jobs, supply, signals, trades, dispatch,
                            execution, advice, ledger, settlements)

    store = _get_store(settlement_real)
    with store.lock:
        with contextlib.ExitStack() as stack:
            for locked in sorted({job_real, supply_real, signal_real,
                                  trades_real, dispatch_real,
                                  execution_real, advice_real, ledger_real,
                                  settlement_real}):
                stack.enter_context(_lock(locked, shared=True))

            (accepted, _history, _signal_history, cleared, _decisions,
             _exec_plans, _advice_records, _intents, plans,
             intent_idempotency, intent_events, markers) = \
                _load_settle_snapshot(
                    job_real, supply_real, signal_real, trades_real,
                    dispatch_real, execution_real, advice_real,
                    ledger_real)
            if job_id not in accepted:
                raise KeyError(job_id)
            trade = cleared.get(job_id)
            if trade is None:
                raise LookupError("job has no recorded trade")
            records, _idempotency, _events, _raw = \
                _load_settlement_ledger(
                    settlement_real, accepted, cleared, plans, _intents,
                    intent_idempotency, intent_events, markers)
            # Completion normally requires both ledgers: a final record
            # counts once its marker flipped to settled. The one
            # exception is the pre-marker legacy form -- a final record
            # with no marker at all. A pending or still-unmirrored
            # settlement is ignored, so the view stably returns the
            # previous binding across every crash window.
            def _completed(key: str, record: dict[str, Any]) -> bool:
                if record["job_id"] != job_id \
                        or record["state"] not in _SETTLE_FINAL_STATES:
                    return False
                marker = markers.get(record["plan_key"])
                if marker is None:
                    return True
                return marker["state"] == "settled" and marker["key"] == key

            finalized = [record for key, record in records.items()
                         if _completed(key, record)]
            if finalized:
                latest = max(finalized,
                             key=lambda record: record["generation"])
                binding = latest["after"]
                return {
                    "job_id": job_id,
                    "resource_id": binding["resource_id"],
                    "version": binding["version"],
                    "generation": latest["generation"],
                    "state": latest["state"],
                    "at": latest["at"],
                }
            return {
                "job_id": job_id,
                "resource_id": trade["resource_id"],
                "version": trade["version"],
                "generation": 0,
                "state": "traded",
                "at": trade["at"],
            }
