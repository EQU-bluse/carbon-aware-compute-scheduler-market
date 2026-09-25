"""Persistent re-evaluation and migration advice for open trades.

The clearing, dispatch and execution layers already move a job through a
trade, a scheduling commitment and a recoverable execution plan, but an
unfinished trade is never re-examined against the live market. This
module adds that read-mostly layer: a single public call,
:func:`evaluate`, snapshots the accepted jobs, the versioned resource
supply, the live signal ledger, the clearing ledger, the dispatch
ledger and the execution ledger, and -- when nothing is in flight --
freezes one re-evaluation record into an advice ledger.

One on-disk document (version 1), the advice ledger, holds:

* ``records`` maps each idempotency key to the single advice record the
  key first served, in code-point order;
* ``idempotency`` binds each key to the complete request it first
  served (job id and evaluation moment);
* ``audit`` holds one event per first-served key, binding the request
  and the committed record snapshot.

The retained candidate keeps the exact resource version the trade froze --
never a later publication of that resource -- but every candidate, the
retained one included, prices on the region's latest signal still valid
at the evaluation moment. Every other resource contributes only its
highest supply version valid at the evaluation moment. Capacity of an
exact resource version is reduced by every other job's trade; this job's
own booking is never deducted twice. Feasible candidates are ordered by
signal carbon intensity, signal unit cost and resource id; the record
advises ``keep`` when the first candidate is still the current resource
version and ``migrate`` otherwise.

Advice is only produced for a job that can still change course: once the
dispatch decision succeeded or any execution plan completed, once an
execution plan is active, or once the evaluation moment passes the job's
deadline, the call raises without writing a record. As for the other
ledgers, every read requires the on-disk bytes to be exactly the
canonical compact form :func:`_canonical_bytes` produces -- sections in
their fixed order, primary keys sorted by code point, compact UTF-8 JSON
with non-ASCII written through, no negative-zero or non-finite number
literals and exactly one trailing newline -- and commits go through a
synced same-directory temporary file, an atomic replace and a directory
fsync, restoring the pre-call bytes on any failure.
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
_ROOT_FIELDS = ("version", "records", "idempotency", "audit")
_RECORD_FIELDS = ("job_id", "at", "current", "dispatch", "execution",
                  "candidates", "advice", "target")
_CURRENT_FIELDS = ("resource_id", "version")
_CANDIDATE_FIELDS = ("resource", "signal", "total_cost", "total_carbon")
_REQUEST_FIELDS = ("job_id", "at")
_EVENT_FIELDS = ("key", "request", "result")
_ADVICE = ("keep", "migrate")
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
    # equivalent real paths share one lock across threads and processes.
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


def _validate_request(request: object) -> dict[str, Any]:
    if not isinstance(request, dict) \
            or set(request.keys()) != set(_REQUEST_FIELDS):
        raise ValueError("idempotency binding must carry exactly job_id "
                         "and at")
    job_id = request["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("request job_id must be a non-empty string")
    at = request["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("request at must be a non-boolean non-negative "
                         "integer")
    return {"job_id": job_id, "at": at}


def _validate_current(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value.keys()) != set(_CURRENT_FIELDS):
        raise ValueError("current selection must carry resource_id and "
                         "version")
    resource_id = value["resource_id"]
    version = value["version"]
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("current resource_id must be a non-empty string")
    if not _is_plain_int(version) or version < 1:
        raise ValueError("current version must be a positive integer")
    return {"resource_id": resource_id, "version": version}


def _validate_candidate(
    entry: object,
    work: int,
    at: int,
    job: dict[str, Any],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]],
    seen: set[str],
) -> tuple[int, int, str]:
    # An advice candidate has the live trade candidate shape. Every
    # candidate prices on the region's latest signal valid at ``at``, so
    # the frozen signal must be exactly that version; the resource record
    # must be the exact immutable published version. The retained item
    # may carry a supply version whose validity window no longer covers
    # ``at``, so that window is not checked here.
    if not isinstance(entry, dict) \
            or set(entry.keys()) != set(_CANDIDATE_FIELDS):
        raise ValueError("advice candidate has invalid fields")
    resource = entry["resource"]
    signal = entry["signal"]
    if not isinstance(resource, dict) \
            or set(resource.keys()) != set(_resources._RECORD_FIELDS):
        raise ValueError("advice candidate resource has invalid fields")
    _resources._check_values(resource)
    resource_id = resource["resource_id"]
    version = resource["version"]
    if not _is_plain_int(version) or version < 1:
        raise ValueError("candidate version must be a positive integer")
    records = history.get(resource_id)
    if records is None or version > len(records) \
            or dict(resource) != records[version - 1]:
        raise ValueError("advice candidate must reference a published "
                         "resource version")
    if resource_id in seen:
        raise ValueError("advice candidates must be distinct resources")
    seen.add(resource_id)
    if not isinstance(signal, dict) \
            or set(signal.keys()) != set(_signals._RECORD_FIELDS):
        raise ValueError("advice candidate signal has invalid fields")
    if signal["region"] != resource["region"]:
        raise ValueError("advice candidate signal must cover the resource "
                         "region")
    signal_copy = dict(signal)
    signal_copy["mix"] = dict(signal["mix"])
    _signals._check_values(signal_copy)
    signal_records = signal_history.get(signal["region"])
    if signal_records is None \
            or signal["version"] > len(signal_records) \
            or dict(signal) != signal_records[signal["version"] - 1]:
        raise ValueError("advice candidate signal must reference a "
                         "published signal version")
    # The signal must be observed no later than ``at`` and still
    # unexpired at it, mirroring the clearing ledger's frozen-signal
    # check: a record frozen against the latest signal at evaluation
    # time stays valid even if a later publication adds another
    # observation covering the same historical moment.
    if not (signal["observed"] <= at <= signal["expires"]):
        raise ValueError("advice candidate signal must be valid at the "
                         "evaluation moment")
    unit_cost = signal["unit_cost"]
    carbon_intensity = signal["carbon_intensity"]
    total_cost = entry["total_cost"]
    total_carbon = entry["total_carbon"]
    if not _is_plain_int(total_cost) or total_cost < 0 \
            or total_cost != work * unit_cost:
        raise ValueError("advice candidate total_cost is invalid")
    if not _is_plain_int(total_carbon) or total_carbon < 0 \
            or total_carbon != work * carbon_intensity:
        raise ValueError("advice candidate total_carbon is invalid")
    if resource["region"] not in set(job["regions"]):
        raise ValueError("advice candidate is outside the job's regions")
    if not set(job["residency"]) <= set(resource["residency"]):
        raise ValueError("advice candidate does not cover job residency")
    if resource["end"] < job["deadline"]:
        raise ValueError("advice candidate does not cover the job deadline")
    if total_cost > job["max_cost"] or total_carbon > job["carbon_cap"]:
        raise ValueError("advice candidate exceeds a job budget")
    return carbon_intensity, unit_cost, resource_id


def _execution_snapshot(plans: dict[str, dict[str, dict[str, Any]]],
                        job_id: str) -> str:
    # The job's execution status is the state of its latest attempt, or
    # ``none`` when no plan was ever made for it.
    attempts = plans.get(job_id)
    if not attempts:
        return "none"
    latest = max(attempts.values(), key=lambda plan: plan["attempt"])
    return latest["state"]


def _validate_record(
    record: object,
    accepted: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]],
    trades: dict[str, dict[str, Any]],
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
    current = _validate_current(record["current"])
    dispatch_state = record["dispatch"]
    execution_state = record["execution"]
    if dispatch_state not in _dispatch._STATES:
        raise ValueError("advice dispatch status is invalid")
    if execution_state not in ("none",) + _execution._STATES:
        raise ValueError("advice execution status is invalid")
    advice = record["advice"]
    if advice not in _ADVICE:
        raise ValueError("advice must be keep or migrate")

    # The record is a historical snapshot: the trade, the dispatch status
    # and the execution status it froze may have moved on by the time the
    # ledger is re-read, so only the immutable trade and its selection
    # are cross-checked against the current ledgers.
    job = accepted.get(job_id)
    if job is None:
        raise ValueError("advice record must reference an accepted job")
    trade = trades.get(job_id)
    if trade is None:
        raise ValueError("advice record must reference a recorded trade")
    if current != {"resource_id": trade["resource_id"],
                   "version": trade["version"]}:
        raise ValueError("advice current selection does not match the "
                         "recorded trade")

    candidates_raw = record["candidates"]
    if not isinstance(candidates_raw, list) or not candidates_raw:
        raise ValueError("advice candidates must be a non-empty list")
    candidates: list[dict[str, Any]] = []
    last_rank: tuple[int, int, str] | None = None
    seen: set[str] = set()
    for entry_raw in candidates_raw:
        carbon_intensity, unit_cost, resource_id = _validate_candidate(
            entry_raw, job["work"], at, job, history, signal_history, seen)
        rank = (carbon_intensity, unit_cost, resource_id)
        if last_rank is not None and rank < last_rank:
            raise ValueError("advice candidates must be ordered by signal "
                             "carbon intensity, unit cost and resource id")
        last_rank = rank
        candidates.append(copy.deepcopy(entry_raw))

    winner = candidates[0]
    target = record["target"]
    if not isinstance(target, dict) or set(target.keys()) != set(_CURRENT_FIELDS):
        raise ValueError("advice target must carry resource_id and version")
    if target != {"resource_id": winner["resource"]["resource_id"],
                  "version": winner["resource"]["version"]}:
        raise ValueError("advice target must be the first ordered "
                         "candidate")
    expected_advice = ("keep" if current == target else "migrate")
    if advice != expected_advice:
        raise ValueError("advice does not match its ordered candidates")
    return {
        "job_id": job_id,
        "at": at,
        "current": current,
        "dispatch": dispatch_state,
        "execution": execution_state,
        "candidates": candidates,
        "advice": advice,
        "target": target,
    }


def _validate_ledger(
    data: object,
    accepted: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]],
    trades: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("advice ledger root must be an object with keys "
                         "version, records, idempotency and audit")
    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported advice ledger version")

    records_raw = data["records"]
    idempotency_raw = data["idempotency"]
    audit_raw = data["audit"]
    if not isinstance(records_raw, dict) \
            or not isinstance(idempotency_raw, dict) \
            or not isinstance(audit_raw, dict):
        raise ValueError("records, idempotency and audit must be objects")
    _check_sorted_keys(records_raw, "records")
    _check_sorted_keys(idempotency_raw, "idempotency")
    _check_sorted_keys(audit_raw, "audit")

    records: dict[str, dict[str, Any]] = {}
    for key, record_raw in records_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("advice keys must be non-empty strings")
        record = _validate_record(record_raw, accepted, history,
                                  signal_history, trades)
        records[key] = record

    idempotency: dict[str, dict[str, Any]] = {}
    for key, binding_raw in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        binding = _validate_request(binding_raw)
        if key not in records:
            raise ValueError("idempotency entry must reference a recorded "
                             "advice")
        record = records[key]
        if binding["job_id"] != record["job_id"] \
                or binding["at"] != record["at"]:
            raise ValueError("idempotency entry does not match its record")
        idempotency[key] = binding

    events: dict[str, dict[str, Any]] = {}
    for key, event_raw in audit_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event_raw, dict) \
                or set(event_raw.keys()) != set(_EVENT_FIELDS):
            raise ValueError("advice audit event has invalid fields")
        event_key = event_raw["key"]
        if event_key != key or not isinstance(event_key, str) or not event_key:
            raise ValueError("audit event key does not match its map key")
        request = _validate_request(event_raw["request"])
        if request != idempotency.get(key):
            raise ValueError("audit event does not match its idempotency "
                             "entry")
        result = _validate_record(event_raw["result"], accepted, history,
                                  signal_history, trades)
        if result != records[key]:
            raise ValueError("audit event result does not match its record")
        events[key] = {"key": key, "request": request, "result": result}

    # The three sections describe one advice history: each key binds one
    # request, one record and one audit event.
    if set(records) != set(idempotency) or set(records) != set(events):
        raise ValueError("records, idempotency and audit do not match")
    return records, idempotency


def _canonical_bytes(
    records: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, Any]],
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, sections in their
    # fixed field order and each section's primary keys in code-point
    # order, terminated by exactly one newline.
    audit = {key: {"key": key,
                   "request": idempotency[key],
                   "result": copy.deepcopy(records[key])}
             for key in records}
    payload = {
        "version": _VERSION,
        "records": {key: copy.deepcopy(records[key])
                    for key in sorted(records)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
        "audit": {key: audit[key] for key in sorted(audit)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_ledger(
    realpath: str,
    accepted: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]],
    trades: dict[str, dict[str, Any]],
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
    records, idempotency = _validate_ledger(
        data, accepted, history, signal_history, trades)
    if raw != _canonical_bytes(records, idempotency):
        raise ValueError(
            f"advice ledger {realpath!r} is not in canonical compact form")
    return records, idempotency, raw


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
    # One durable commit for the record, the idempotency binding and the
    # audit event: synced same-directory temporary, atomic replace and a
    # directory fsync, restoring the pre-call bytes on any failure.
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
    signal: dict[str, Any] | None = None
    for record in signal_history.get(region, ()):
        if record["observed"] <= at <= record["expires"]:
            signal = record
    if signal is None:
        return None
    signal_copy = dict(signal)
    signal_copy["mix"] = dict(signal["mix"])
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
    """Re-evaluate one unfinished trade and record keep/migrate advice.

    ``jobs``, ``supply``, ``signals``, ``trades``, ``dispatch``,
    ``execution`` and ``ledger`` paths, ``job_id`` and ``key`` must be
    non-empty strings and ``at`` a non-boolean non-negative integer
    evaluation moment; the seven paths must also resolve to distinct
    real locations. Any violation raises ``ValueError`` before a
    business file is read.

    The six input files are read as one snapshot under their shared
    locks together with the advice ledger's exclusive lock, all seven
    taken in resolved real-path order. A dispatch decision that already
    succeeded, or a job whose execution plans include a completed one,
    raises ``ValueError`` and writes no advice; a job with an active
    execution plan raises ``PermissionError``; an evaluation moment past
    the job's deadline raises ``TimeoutError``. Advice is produced only
    for an unfinished trade, so an unknown job id raises ``KeyError``,
    an unknown dispatch decision ``KeyError`` and a job without a
    recorded trade ``LookupError``; none of them creates the ledger.

    The retained candidate keeps the exact resource version the trade
    froze -- never a later supply publication, even when its window no
    longer covers ``at`` -- but every candidate, the retained one
    included, prices on the resource region's latest signal still valid
    at ``at``. Every other resource contributes only its highest supply
    version valid at ``at``; a region without a current signal removes
    that resource. Region, residency, deadline, cost and carbon budgets
    follow the live feasibility rules, and an exact resource version's
    capacity is reduced by the work of every other job's trade -- this
    job's own booking is never deducted twice. Candidates are ordered by
    signal carbon intensity, signal unit cost and resource id; with no
    feasible candidate the call raises ``LookupError`` and writes
    nothing. The first candidate still names the current resource
    version advises ``keep``, otherwise the advice is ``migrate`` to it.

    Returns ``(record, created)``; the record freezes the job id, the
    evaluation moment, the current selection, the dispatch and
    execution statuses, the ordered candidates, the advice and its
    target selection. The first call for a key commits the record, its
    idempotency binding (job id and evaluation moment) and an audit
    event in one synced atomic write. Replaying the same key with the
    same request returns the stored record with ``False`` without
    writing; the same key with a changed request raises ``ValueError``
    with the ledger byte-for-byte untouched.

    Missing input files or the ledger parent raise
    ``FileNotFoundError``; invalid arguments, structure, ordering,
    references or non-canonical bytes raise ``ValueError``; other
    locking or I/O failures raise ``OSError``.
    """
    for value in (jobs, supply, signals, trades, dispatch, execution,
                  ledger, job_id, key):
        if not isinstance(value, str) or not value:
            raise ValueError("all paths, job_id and key must be non-empty "
                             "strings")
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
        # Locks are taken in one resolved-real-path order shared by
        # every caller, so concurrent evaluations can never deadlock;
        # the advice ledger lock is exclusive and the six input
        # snapshots shared.
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
            cleared, _clear_keys, clear_raw = _market._load_clear_ledger(
                trades_real, accepted, history, signal_history)
            if clear_raw is None:
                raise FileNotFoundError(
                    f"clearing ledger {trades_real!r} does not exist")
            decisions, _dispatch_bindings, _dispatch_events, dispatch_raw = \
                _dispatch._load_ledger(dispatch_real)
            if dispatch_raw is None:
                raise FileNotFoundError(
                    f"dispatch ledger {dispatch_real!r} does not exist")
            plans, _plan_bindings, _plan_events, execution_raw = \
                _execution._load_ledger(execution_real)
            if execution_raw is None:
                raise FileNotFoundError(
                    f"execution ledger {execution_real!r} does not exist")
            records, idempotency, old_bytes = _load_ledger(
                ledger_real, accepted, history, signal_history, cleared)

            request = {"job_id": job_id, "at": at}
            binding = idempotency.get(key)
            if binding is not None:
                if binding != request:
                    raise ValueError("idempotency key was already used with "
                                     "a different request")
                return copy.deepcopy(records[key]), False

            job = accepted.get(job_id)
            if job is None:
                raise KeyError(job_id)
            trade = cleared.get(job_id)
            if trade is None:
                raise LookupError("job has no recorded trade")
            decision = decisions.get(job_id)
            if decision is None:
                raise KeyError(job_id)

            # A finished dispatch decision leaves nothing to
            # re-evaluate; this gate and the ones below never write the
            # advice ledger.
            if decision["state"] == "succeeded":
                raise ValueError("dispatch decision already succeeded")
            attempts = plans.get(job_id, {})
            plan_states = {plan["state"] for plan in attempts.values()}
            if "completed" in plan_states:
                raise ValueError("an execution plan already completed")
            # An active plan is work in flight: advice must not change
            # the verdict while execution is under way.
            if "active" in plan_states:
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

            # Capacity sold on each exact resource version by every other
            # job's trade; this job's own booking is deliberately left
            # out so it is not deducted against itself.
            sold: dict[tuple[str, int], int] = {}
            for other_id, other in cleared.items():
                if other_id == job_id:
                    continue
                slot = (other["resource_id"], other["version"])
                sold[slot] = sold.get(slot, 0) + other["work"]

            candidates: list[dict[str, Any]] = []
            for resource_id, records_for_id in history.items():
                if resource_id == current_id:
                    # The retained item is the exact version the trade
                    # froze, even when that window no longer covers ``at``
                    # or a newer supply version now exists; every other
                    # resource only contributes its highest valid version.
                    if current_version > len(records_for_id):
                        continue
                    resource = records_for_id[current_version - 1]
                else:
                    resource = None
                    for record in records_for_id:
                        if record["start"] <= at <= record["end"]:
                            resource = record
                    if resource is None:
                        continue
                if resource["region"] not in regions:
                    continue
                remaining = resource["capacity"] - sold.get(
                    (resource["resource_id"], resource["version"]), 0)
                if remaining < work:
                    continue
                if not residency <= set(resource["residency"]):
                    continue
                if resource["end"] < job["deadline"]:
                    continue
                # Every candidate prices on the region's latest signal
                # still valid at the evaluation moment, the retained
                # frozen version included.
                signal = _latest_signal(signal_history,
                                        resource["region"], at)
                if signal is None:
                    continue
                total_cost = work * signal["unit_cost"]
                total_carbon = work * signal["carbon_intensity"]
                if total_cost > job["max_cost"] \
                        or total_carbon > job["carbon_cap"]:
                    continue
                candidates.append({
                    "resource": dict(resource),
                    "signal": signal,
                    "total_cost": total_cost,
                    "total_carbon": total_carbon,
                })

            candidates.sort(key=lambda entry: (
                entry["signal"]["carbon_intensity"],
                entry["signal"]["unit_cost"],
                entry["resource"]["resource_id"]))
            if not candidates:
                raise LookupError("no feasible resource for job")

            winner = candidates[0]
            target = {"resource_id": winner["resource"]["resource_id"],
                      "version": winner["resource"]["version"]}
            advice = "keep" if target == current else "migrate"
            record: dict[str, Any] = {
                "job_id": job_id,
                "at": at,
                "current": current,
                "dispatch": decision["state"],
                "execution": _execution_snapshot(plans, job_id),
                "candidates": candidates,
                "advice": advice,
                "target": target,
            }
            records[key] = record
            idempotency[key] = request
            _commit_file(ledger_real,
                         _canonical_bytes(records, idempotency),
                         old_bytes)
            return copy.deepcopy(record), True
