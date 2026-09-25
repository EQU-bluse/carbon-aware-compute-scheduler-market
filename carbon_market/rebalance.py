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
request raises ``ValueError``. The ledger upgrade to version 2 keeps
every recorded intent and appends the ``plans`` section keyed by job
id; only the first change of a call commits the plan, its state, the
idempotency binding and the audit event atomically. Every read requires the on-disk bytes to be exactly the
canonical compact form :func:`_canonical_bytes` produces -- compact
UTF-8 JSON with non-ASCII written through, no negative-zero or
non-finite number literals and exactly one trailing newline -- and
every commit goes through a synced same-directory temporary file, an
atomic replace and a directory fsync, restoring the pre-call bytes on
failure, so an unsuccessful call leaves neither a temporary fragment
nor half an audit event.
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

from . import dispatch as _dispatch
from . import execution as _execution
from . import jobs as _jobs
from . import market as _market
from . import resources as _resources
from . import signals as _signals
from ._jsonio import finite_loads

__all__ = ["evaluate", "apply", "start", "record", "recover"]

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
_INTENT_ROOT_FIELDS = ("version", "intents", "idempotency", "audit")
_INTENT_ROOT_FIELDS_V2 = ("version", "intents", "plans", "idempotency",
                          "audit")
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
    trade = trades.get(job_id)
    if trade is None:
        raise ValueError("intent record must reference a recorded trade")
    if at > job["deadline"]:
        raise ValueError("intent moment must not pass the job deadline")
    if source != {"resource_id": trade["resource_id"],
                  "version": trade["version"]}:
        raise ValueError("intent source selection must match the trade")
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
    intents: dict[str, dict[str, Any]],
    accepted: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(record, dict) \
            or set(record.keys()) != set(_PLAN_FIELDS):
        raise ValueError("plan record has invalid fields")
    job_id = record["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("plan job_id must be a non-empty string")
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

    intent = intents.get(job_id)
    if intent is None:
        raise ValueError("plan must reference a recorded intent")
    if source != intent["source"] or target != intent["target"]:
        raise ValueError("plan selections must match its intent")
    # The intent validation guarantees the job is accepted.
    if lease_end > accepted[job_id]["deadline"]:
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


def _validate_intent_ledger(
    data: object,
    accepted: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]],
    advice_records: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]], dict[str, dict[str, Any]], int]:
    if not isinstance(data, dict):
        raise ValueError("intent ledger root must be an object")
    keys = set(data.keys())
    if keys == set(_INTENT_ROOT_FIELDS):
        plans_raw: object = None
        expected = _INTENT_VERSION
    elif keys == set(_INTENT_ROOT_FIELDS_V2):
        plans_raw = data["plans"]
        expected = _INTENT_VERSION_V2
    else:
        raise ValueError("intent ledger root must be an object with keys "
                         "version, intents, idempotency and audit, plus "
                         "plans once upgraded")
    version = data["version"]
    if not _is_plain_int(version) or version != expected:
        raise ValueError("unsupported intent ledger version")

    intents_raw = data["intents"]
    idempotency_raw = data["idempotency"]
    audit_raw = data["audit"]
    if not isinstance(intents_raw, dict) \
            or not isinstance(idempotency_raw, dict) \
            or not isinstance(audit_raw, dict):
        raise ValueError("intents, idempotency and audit must be objects")
    _check_sorted_keys(intents_raw, "intents")
    _check_sorted_keys(idempotency_raw, "idempotency")
    _check_sorted_keys(audit_raw, "audit")

    intents: dict[str, dict[str, Any]] = {}
    for job_id, record_raw in intents_raw.items():
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("intent job ids must be non-empty strings")
        record = _validate_intent(
            record_raw, accepted, trades, history, signal_history,
            advice_records)
        if record["job_id"] != job_id:
            raise ValueError("intent record id does not match its key")
        intents[job_id] = record

    plans: dict[str, dict[str, Any]] = {}
    if plans_raw is not None:
        if not isinstance(plans_raw, dict):
            raise ValueError("plans must be an object")
        _check_sorted_keys(plans_raw, "plans")
        for job_id, plan_raw in plans_raw.items():
            if not isinstance(job_id, str) or not job_id:
                raise ValueError("plan job ids must be non-empty strings")
            plan = _validate_plan(plan_raw, intents, accepted)
            if plan["job_id"] != job_id:
                raise ValueError("plan record id does not match its key")
            plans[job_id] = plan

    idempotency: dict[str, dict[str, Any]] = {}
    bound: dict[str, str] = {}
    for key, request_raw in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        request = _validate_any_request(request_raw)
        if "action" in request:
            if request["job_id"] not in plans:
                raise ValueError("action idempotency entry must reference "
                                 "a recorded plan")
        else:
            record = intents.get(request["job_id"])
            if record is None:
                raise ValueError("idempotency entry must reference a "
                                 "recorded intent")
            if request["advice_key"] != record["advice_key"] \
                    or request["at"] != record["at"]:
                raise ValueError("idempotency entry does not match its "
                                 "intent")
            if request["job_id"] in bound:
                raise ValueError("job reserved under more than one "
                                 "idempotency key")
            bound[request["job_id"]] = key
        idempotency[key] = request
    if set(bound) != set(intents):
        raise ValueError("every intent must be bound to an idempotency "
                         "key")

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
            result: dict[str, Any] = _validate_plan(
                event_raw["result"], intents, accepted)
            if result["job_id"] != request["job_id"]:
                raise ValueError("audit event result does not match its "
                                 "request")
        else:
            result = _validate_intent(
                event_raw["result"], accepted, trades, history,
                signal_history, advice_records)
            if result != intents[request["job_id"]]:
                raise ValueError("audit event result does not match its "
                                 "intent")
        events[key] = {"key": key, "request": request, "result": result}

    # The sections describe one reservation history: one binding and one
    # audit event per intent, and vice versa.
    if set(events) != set(idempotency):
        raise ValueError("idempotency keys and audit events do not match")

    # The plans and the action history describe one migration lifecycle:
    # a plan's selections, owner, lease end and start moment never change
    # after the plan is created, so every committed snapshot must agree
    # with them; every plan is started exactly once, its steps are
    # exactly the receipts the record requests committed and it is
    # interrupted exactly when one recovery was recorded.
    fixed = ("source", "target", "owner", "lease_end", "at")
    starts: dict[str, str] = {}
    recordings: dict[str, list[dict[str, Any]]] = {}
    recoveries: dict[str, int] = {}
    for key, request in idempotency.items():
        if "action" not in request:
            continue
        result = events[key]["result"]
        job_id = request["job_id"]
        plan = plans[job_id]
        for field in fixed:
            if result[field] != plan[field]:
                raise ValueError("audit event result does not match its "
                                 "plan")
        action = request["action"]
        if action == "start":
            if job_id in starts:
                raise ValueError("plan started under more than one "
                                 "idempotency key")
            starts[job_id] = key
            if result["state"] != "active" or result["steps"] != []:
                raise ValueError("start audit result must be a fresh "
                                 "active plan")
            if request["owner"] != result["owner"] \
                    or request["lease_end"] != result["lease_end"] \
                    or request["at"] != result["at"]:
                raise ValueError("start audit result does not match its "
                                 "request")
        elif action == "record":
            if request["owner"] != result["owner"]:
                raise ValueError("record audit result does not match its "
                                 "request")
            if not result["steps"]:
                raise ValueError("record audit result must carry the "
                                 "recorded step")
            receipt = result["steps"][-1]
            if receipt != {"step": request["step"],
                           "result": request["result"],
                           "receipt": request["receipt"],
                           "at": request["at"]}:
                raise ValueError("record audit result does not match its "
                                 "request")
            recordings.setdefault(job_id, []).append(receipt)
        else:  # recover
            if request["owner"] != result["owner"]:
                raise ValueError("recover audit result does not match "
                                 "its request")
            if result["state"] != "interrupted":
                raise ValueError("recover audit result must be an "
                                 "interrupted plan")
            if request["at"] <= result["lease_end"]:
                raise ValueError("recover request must lie past the "
                                 "lease end")
            recoveries[job_id] = recoveries.get(job_id, 0) + 1

    if set(starts) != set(plans):
        raise ValueError("every plan must be bound to a start request")
    for job_id, plan in plans.items():
        committed = sorted(json.dumps(receipt, sort_keys=True)
                           for receipt in recordings.get(job_id, []))
        held = sorted(json.dumps(receipt, sort_keys=True)
                      for receipt in plan["steps"])
        if committed != held:
            raise ValueError("plan steps do not match the record "
                             "history")
        if (plan["state"] == "interrupted") \
                != (recoveries.get(job_id, 0) == 1):
            raise ValueError("plan state does not match the recover "
                             "history")
        if recoveries.get(job_id, 0) > 1:
            raise ValueError("plan recovered more than once")

    return intents, plans, idempotency, events, version


def _intent_canonical_bytes(
    intents: dict[str, dict[str, Any]],
    plans: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
    version: int,
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, sections in their
    # fixed field order, intents and plans keyed by job id and
    # bindings/events by idempotency key, each in code-point order,
    # terminated by exactly one newline. Version 1 predates the plans
    # section; version 2 keeps every intent and appends it.
    payload: dict[str, Any] = {"version": version}
    payload["intents"] = {job_id: intents[job_id]
                          for job_id in sorted(intents)}
    if version == _INTENT_VERSION_V2:
        payload["plans"] = {job_id: plans[job_id]
                            for job_id in sorted(plans)}
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
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]], dict[str, dict[str, Any]], int,
           bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return {}, {}, {}, {}, _INTENT_VERSION, None
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
    intents, plans, idempotency, events, version = _validate_intent_ledger(
        data, accepted, trades, history, signal_history, advice_records)
    # As for the other ledgers, the ledger is accepted only in canonical
    # compact form with a single trailing newline.
    if raw != _intent_canonical_bytes(intents, plans, idempotency, events,
                                      version):
        raise ValueError(
            f"intent ledger {realpath!r} is not in canonical compact "
            "form")
    return intents, plans, idempotency, events, version, raw


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

    The eight paths, ``job_id``, ``advice_key`` and ``key`` must be
    non-empty strings and ``at`` a non-boolean non-negative integer
    reservation moment; the eight paths must also resolve to distinct
    real locations. Any violation raises ``ValueError`` before a
    business file is read.

    The acceptance, supply, signal, clearing, dispatch, execution and
    advice files are read as one snapshot under their shared locks
    together with the intent ledger's exclusive lock, all eight taken
    in resolved real-path order. Only a ``migrate`` advice recorded
    under ``advice_key`` for this very job can be applied: a ``keep``
    advice, an advice belonging to another job or a reservation moment
    earlier than the advice moment raises ``ValueError``. The booking
    must be unfinished and not in flight: a succeeded dispatch decision
    or a completed execution plan raises ``ValueError``, a claimed
    decision or an active plan raises ``PermissionError`` and a moment
    past the job deadline raises ``TimeoutError`` -- in that order, and
    none of them writes.

    The candidate set is recomputed at ``at`` under the same rules as
    :func:`evaluate` -- the retained item is the trade's frozen version,
    every migration item is another resource's highest version valid at
    ``at``, each item prices on its region's latest signal valid at
    ``at`` and keeps the job's regions, residency, deadline and budgets
    -- except that capacity is now also deducted per exact resource
    version for every other job's recorded intent, while this job's own
    source occupancy is never deducted against itself. The deduction
    recognizes the migration plan states: a ``reserved`` or ``active``
    intent occupies both its source (through its trade) and its target,
    a ``migrated`` intent has released the source trade occupancy and
    occupies only the target, and a ``failed`` or ``interrupted``
    intent has released the target reservation and occupies only the
    source. The advice
    target must still be the first ordered candidate: a changed target,
    an empty feasible set or insufficient remaining capacity raises
    ``LookupError`` and reserves nothing.

    Returns ``(record, created)``. The record freezes, in order, the
    job id, the advice key, the reservation moment, the source and
    target selections, the target's frozen supply and signal records,
    the dispatch and execution states and the ``reserved`` state. A
    missing intent ledger is created only by the first reservation, the
    intent, its idempotency binding and the audit event -- the complete
    request plus the committed intent -- committed together in one
    synced atomic write; a ledger already upgraded to version 2 keeps
    its version and its recorded plans, a version 1 ledger stays a
    version 1 ledger. Replaying the same key with the same job,
    advice key and moment returns the stored record with ``False``
    without writing; the same key with a changed request, or a job
    already reserved under another key, raises ``ValueError`` and
    leaves the ledger untouched.

    An unknown job, advice key or dispatch decision raises
    ``KeyError``; a job without a trade raises ``LookupError``; neither
    creates the ledger. Missing input files or the ledger parent raise
    ``FileNotFoundError``; invalid arguments, structure, ordering,
    references or non-canonical bytes raise ``ValueError``; other
    locking, read/write or sync failures raise ``OSError``.
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
        # Locks are taken in one resolved-real-path order shared by
        # every caller, so concurrent calls can never deadlock; the
        # intent ledger lock is exclusive, the seven snapshot locks
        # shared.
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
            plans, _plan_keys, _plan_events = \
                _execution._load_existing_ledger(execution_real)[:3]
            # The advice to consume lives in the advice ledger, an
            # input snapshot here: a missing file is FileNotFoundError,
            # a missing key inside it is KeyError.
            advice_records, _advice_events, advice_raw = _load_ledger(
                advice_real, accepted, cleared, history, signal_history)
            if advice_raw is None:
                raise FileNotFoundError(
                    f"advice ledger {advice_real!r} does not exist")
            intents, migration_plans, idempotency, events, \
                ledger_version, old_bytes = _load_intent_ledger(
                    ledger_real, accepted, cleared, history,
                    signal_history, advice_records)

            request = {"job_id": job_id, "advice_key": advice_key,
                       "at": at}
            binding = idempotency.get(key)
            if binding is not None:
                if binding != request:
                    raise ValueError("idempotency key was already used "
                                     "with a different request")
                # An equivalent replay returns the stored record
                # without re-reserving or rewriting a byte.
                return copy.deepcopy(intents[job_id]), False

            job = accepted.get(job_id)
            if job is None:
                raise KeyError(job_id)
            if job_id in intents:
                raise ValueError("job is already reserved under another "
                                 "idempotency key")
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

            job_plans = plans.get(job_id, {})
            # Refusal order is fixed: a finished booking is a
            # ValueError, work claimed or in flight is a
            # PermissionError, and a moment past the deadline is a
            # TimeoutError.
            if decision["state"] == "succeeded" \
                    or any(plan["state"] == "completed"
                           for plan in job_plans.values()):
                raise ValueError("a finished booking cannot be "
                                 "re-reserved")
            if decision["state"] == "claimed" \
                    or any(plan["state"] == "active"
                           for plan in job_plans.values()):
                raise PermissionError("the booking is claimed or has an "
                                      "active execution plan")
            if at > job["deadline"]:
                raise TimeoutError("reservation moment exceeds the job "
                                   "deadline")

            current_id = trade["resource_id"]
            current_version = trade["version"]
            current = {"resource_id": current_id,
                       "version": current_version}
            work = job["work"]
            regions = set(job["regions"])
            residency = set(job["residency"])

            # Capacity already booked per exact resource version by
            # every OTHER job's trade and by every OTHER job's recorded
            # intent; this job's own frozen source occupancy is never
            # deducted against itself. The plan states decide what an
            # intent still holds: reserved or active intents occupy both
            # source (through the trade) and target, a migrated intent
            # has released the source trade occupancy and holds only the
            # target, and a failed or interrupted intent has released
            # the target reservation and holds only the source.
            booked: dict[tuple[str, int], int] = {}
            for other_id, other in cleared.items():
                if other_id == job_id:
                    continue
                other_plan = migration_plans.get(other_id)
                if other_plan is not None \
                        and other_plan["state"] == "migrated":
                    continue
                slot = (other["resource_id"], other["version"])
                booked[slot] = booked.get(slot, 0) + other["work"]
            for other_id, intent in intents.items():
                if other_id == job_id:
                    continue
                other_plan = migration_plans.get(other_id)
                if other_plan is not None \
                        and other_plan["state"] in ("failed",
                                                    "interrupted"):
                    continue
                other_target = intent["target"]
                slot = (other_target["resource_id"],
                        other_target["version"])
                booked[slot] = booked.get(slot, 0) \
                    + accepted[other_id]["work"]

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

            # The retained item is the immutable version the trade
            # froze; migration items are the other resources' highest
            # versions valid at the moment -- exactly as in evaluate.
            frozen_records = history.get(current_id)
            if frozen_records is None \
                    or current_version > len(frozen_records):
                raise ValueError("trade references an unpublished "
                                 "resource version")
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
            if job_plans:
                execution_state = max(
                    job_plans.values(),
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
            intents[job_id] = record
            idempotency[key] = request
            events[key] = {"key": key, "request": dict(request),
                           "result": copy.deepcopy(record)}
            # A ledger that already carries migration plans keeps its
            # version 2 form; anything else stays a version 1 ledger.
            _commit_file(ledger_real,
                         _intent_canonical_bytes(intents, migration_plans,
                                                 idempotency, events,
                                                 ledger_version),
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

    The eight paths, ``job_id``, ``key`` and ``owner`` must be non-empty
    strings, ``lease_end`` a non-boolean positive integer and ``at`` a
    non-boolean non-negative integer start moment; the eight paths must
    also resolve to distinct real locations. Any violation raises
    ``ValueError`` before a business file is read.

    The seven input files are read as one snapshot under their shared
    locks together with the intent ledger's exclusive lock, all eight
    taken in resolved real-path order. Only an intent that is still
    ``reserved`` -- neither finished nor occupied -- can be claimed: an
    intent whose plan is already active raises ``PermissionError`` and
    one whose plan reached a terminal state raises ``ValueError``. The
    booking must also be unfinished and not in flight, refused in a
    fixed order: a succeeded dispatch decision or a completed execution
    plan raises ``ValueError``, a claimed decision or an active
    execution plan raises ``PermissionError`` and a start moment past
    the job deadline raises ``TimeoutError`` -- none of them writes.
    The lease end must not pass the job deadline (``ValueError``) and
    the start moment must lie inside the lease (``TimeoutError``).

    A successful claim freezes the intent's source and target resource
    versions, the owner, the lease end and the start moment into an
    ``active`` migration plan with the step sequence ``copy`` then
    ``switch``; the target capacity stays exclusively held by the
    intent for the whole execution. Returns ``(plan, created)``; the
    plan, its idempotency binding and the audit event are committed in
    one synced atomic write that upgrades the ledger to version 2 while
    keeping every recorded intent. Replaying the same key with the same
    job, owner, lease end and moment returns the current plan with
    ``False`` without writing; the same key with a changed request
    raises ``ValueError`` and leaves the ledger untouched.

    An unknown job, intent or dispatch decision raises ``KeyError``.
    Missing input files or the ledger parent raise
    ``FileNotFoundError``; invalid arguments, structure, ordering,
    references or non-canonical bytes raise ``ValueError``; other
    locking, read/write or sync failures raise ``OSError``.
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
        # Locks are taken in one resolved-real-path order shared by
        # every caller, so concurrent calls can never deadlock; the
        # intent ledger lock is exclusive, the seven snapshot locks
        # shared.
        with contextlib.ExitStack() as stack:
            for locked in sorted({job_real, supply_real, signal_real,
                                  trades_real, dispatch_real,
                                  execution_real, advice_real,
                                  ledger_real}):
                stack.enter_context(
                    _lock(locked, shared=(locked != ledger_real)))

            (accepted, history, signal_history, cleared, decisions,
             exec_plans, advice_records) = _load_snapshot_layers(
                job_real, supply_real, signal_real, trades_real,
                dispatch_real, execution_real, advice_real)
            intents, plans, idempotency, events, _version, old_bytes = \
                _load_intent_ledger(
                    ledger_real, accepted, cleared, history,
                    signal_history, advice_records)

            request = {"action": "start", "job_id": job_id,
                       "owner": owner, "lease_end": lease_end, "at": at}
            binding = idempotency.get(key)
            if binding is not None:
                if binding != request:
                    raise ValueError("idempotency key was already used "
                                     "with a different request")
                # An equivalent replay returns the current plan without
                # rewriting a byte.
                return copy.deepcopy(plans[job_id]), False

            job = accepted.get(job_id)
            if job is None:
                raise KeyError(job_id)
            intent = intents.get(job_id)
            if intent is None:
                raise KeyError(job_id)
            decision = decisions.get(job_id)
            if decision is None:
                raise KeyError(job_id)

            plan = plans.get(job_id)
            if plan is not None:
                # An active plan is occupied; a terminal one is finished.
                if plan["state"] == "active":
                    raise PermissionError("the intent already holds an "
                                          "active migration plan")
                raise ValueError("the intent's migration plan is "
                                 "finished")
            job_plans = exec_plans.get(job_id, {})
            # Refusal order is fixed: a finished booking is a
            # ValueError, work claimed or in flight is a
            # PermissionError, and a moment past the deadline is a
            # TimeoutError.
            if decision["state"] == "succeeded" \
                    or any(item["state"] == "completed"
                           for item in job_plans.values()):
                raise ValueError("a finished booking cannot be "
                                 "migrated")
            if decision["state"] == "claimed" \
                    or any(item["state"] == "active"
                           for item in job_plans.values()):
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
            plans[job_id] = new_plan
            idempotency[key] = request
            events[key] = {"key": key, "request": dict(request),
                           "result": copy.deepcopy(new_plan)}
            _commit_file(ledger_real,
                         _intent_canonical_bytes(intents, plans,
                                                 idempotency, events,
                                                 _INTENT_VERSION_V2),
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
    """Record one step receipt for an active migration plan.

    The eight paths, ``job_id``, ``key``, ``owner`` and ``receipt``
    must be non-empty strings, ``step`` the plan's next pending step
    (``copy`` then ``switch``), ``result`` exactly ``succeeded`` or
    ``failed`` and ``at`` a non-boolean non-negative integer moment;
    the eight paths must also resolve to distinct real locations. Any
    violation raises ``ValueError`` before a business file is read.

    The seven input files are read as one snapshot under their shared
    locks together with the intent ledger's exclusive lock, all eight
    taken in resolved real-path order. Only the plan's owner acting
    inside the lease may record: a plan that is not active raises
    ``ValueError``, a different owner raises ``PermissionError`` and a
    moment past the lease end raises ``TimeoutError``. Steps advance
    only on success and are never skipped or repeated: a receipt naming
    anything but the next pending step raises ``ValueError``. A
    successful ``switch`` turns the plan ``migrated`` and releases the
    source capacity the trade froze; any ``failed`` step turns the plan
    ``failed`` and releases the target reservation immediately, leaving
    the source trade occupancy untouched. Terminal plans accept no
    further receipts.

    Returns ``(plan, created)``; the new plan state, the idempotency
    binding and the audit event are committed in one synced atomic
    write. Replaying the same key with the same job, owner, step,
    result, receipt and moment returns the current plan with ``False``
    without writing; the same key with a changed request raises
    ``ValueError`` and leaves the ledger untouched.

    An unknown job or intent raises ``KeyError``; an intent without a
    migration plan raises ``ValueError``. Missing input files or the
    ledger parent raise ``FileNotFoundError``; invalid arguments,
    structure, ordering, references or non-canonical bytes raise
    ``ValueError``; other locking, read/write or sync failures raise
    ``OSError``.
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
            for locked in sorted({job_real, supply_real, signal_real,
                                  trades_real, dispatch_real,
                                  execution_real, advice_real,
                                  ledger_real}):
                stack.enter_context(
                    _lock(locked, shared=(locked != ledger_real)))

            (accepted, history, signal_history, cleared, _decisions,
             _exec_plans, advice_records) = _load_snapshot_layers(
                job_real, supply_real, signal_real, trades_real,
                dispatch_real, execution_real, advice_real)
            intents, plans, idempotency, events, _version, old_bytes = \
                _load_intent_ledger(
                    ledger_real, accepted, cleared, history,
                    signal_history, advice_records)

            request = {"action": "record", "job_id": job_id,
                       "owner": owner, "step": step, "result": result,
                       "receipt": receipt, "at": at}
            binding = idempotency.get(key)
            if binding is not None:
                if binding != request:
                    raise ValueError("idempotency key was already used "
                                     "with a different request")
                # An equivalent replay returns the current plan without
                # rewriting a byte.
                return copy.deepcopy(plans[job_id]), False

            if job_id not in accepted:
                raise KeyError(job_id)
            if job_id not in intents:
                raise KeyError(job_id)
            plan = plans.get(job_id)
            if plan is None:
                raise ValueError("the intent has no migration plan yet")
            if plan["state"] != "active":
                raise ValueError("plan is not active")
            if plan["owner"] != owner:
                raise PermissionError("record requires the plan owner")
            if at > plan["lease_end"]:
                raise TimeoutError("the lease has already expired")
            if step != _PLAN_STEPS[len(plan["steps"])]:
                raise ValueError("step is not the plan's next pending "
                                 "step")

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
                         _intent_canonical_bytes(intents, plans,
                                                 idempotency, events,
                                                 _INTENT_VERSION_V2),
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
    """Interrupt an active migration plan whose lease has expired.

    The eight paths, ``job_id``, ``key`` and ``owner`` must be
    non-empty strings and ``at`` a non-boolean non-negative integer
    moment; the eight paths must also resolve to distinct real
    locations. Any violation raises ``ValueError`` before a business
    file is read.

    The seven input files are read as one snapshot under their shared
    locks together with the intent ledger's exclusive lock, all eight
    taken in resolved real-path order. Only the plan's owner may
    recover and only once the lease has strictly expired: a plan that
    is not active raises ``ValueError``, a different owner or a lease
    that has not expired yet raises ``PermissionError``. A successful
    recovery turns the plan ``interrupted``, preserves every recorded
    receipt and releases the target reservation, leaving the source
    trade occupancy untouched.

    Returns ``(plan, created)``; the new plan state, the idempotency
    binding and the audit event are committed in one synced atomic
    write. Replaying the same key with the same job, owner and moment
    returns the current plan with ``False`` without writing; the same
    key with a changed request raises ``ValueError`` and leaves the
    ledger untouched.

    An unknown job or intent raises ``KeyError``; an intent without a
    migration plan raises ``ValueError``. Missing input files or the
    ledger parent raise ``FileNotFoundError``; invalid arguments,
    structure, ordering, references or non-canonical bytes raise
    ``ValueError``; other locking, read/write or sync failures raise
    ``OSError``.
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
            for locked in sorted({job_real, supply_real, signal_real,
                                  trades_real, dispatch_real,
                                  execution_real, advice_real,
                                  ledger_real}):
                stack.enter_context(
                    _lock(locked, shared=(locked != ledger_real)))

            (accepted, history, signal_history, cleared, _decisions,
             _exec_plans, advice_records) = _load_snapshot_layers(
                job_real, supply_real, signal_real, trades_real,
                dispatch_real, execution_real, advice_real)
            intents, plans, idempotency, events, _version, old_bytes = \
                _load_intent_ledger(
                    ledger_real, accepted, cleared, history,
                    signal_history, advice_records)

            request = {"action": "recover", "job_id": job_id,
                       "owner": owner, "at": at}
            binding = idempotency.get(key)
            if binding is not None:
                if binding != request:
                    raise ValueError("idempotency key was already used "
                                     "with a different request")
                # An equivalent replay returns the current plan without
                # rewriting a byte.
                return copy.deepcopy(plans[job_id]), False

            if job_id not in accepted:
                raise KeyError(job_id)
            if job_id not in intents:
                raise KeyError(job_id)
            plan = plans.get(job_id)
            if plan is None:
                raise ValueError("the intent has no migration plan yet")
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
                         _intent_canonical_bytes(intents, plans,
                                                 idempotency, events,
                                                 _INTENT_VERSION_V2),
                         old_bytes, prefix=".rebalance-recover-")
            return copy.deepcopy(plan), True
