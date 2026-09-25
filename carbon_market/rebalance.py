"""Persistent re-evaluation and migration advice for booked trades.

The market already books a job against one exact resource version
(``market.clear``/``market.clear_live``), dispatch commits and claims
that decision and the execution layer runs launch or migration plans,
while the live signal ledger keeps publishing fresh region observations.
What is missing until this module is a re-evaluation of an unfinished
booking: given the signals valid *now*, should the job stay on the
resource version its trade froze, or migrate to another resource whose
current version is cleaner or cheaper right now?

:func:`evaluate` is the one and only public call. It reads the seven
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

The advice ledger (version 1) holds ``records`` keyed by idempotency
key and one ``audit`` event per first-served key; both sections are
ordered by key code point. A first evaluation returns
``(record, True)``; replaying the same key with the same job and
evaluation moment returns the stored record with ``False`` without
writing, while the same key with a changed request raises
``ValueError``. Every read requires the on-disk bytes to be exactly the
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

__all__ = ["evaluate"]

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
                dir=directory, prefix=".rebalance-restore-", suffix=".tmp")
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
                 old_bytes: bytes | None) -> None:
    # One durable commit for the record and its audit event: synced
    # same-directory temporary, atomic replace and a directory fsync,
    # restoring the pre-call bytes on any failure, so an unsuccessful
    # evaluation leaves neither a temporary fragment nor half an audit
    # event.
    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".rebalance-", suffix=".tmp")
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
        _rollback_file(realpath, directory, old_bytes, first)
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
