"""Public completion registry for finished trading jobs.

A traded, dispatched and executed job keeps occupying resource capacity
-- its trade keeps deducting a resource version in market clearing, and
its migration lineage keeps holding historical reservations -- until
someone declares the business run over. :func:`register` publishes that
declaration in one independent completion ledger, and :func:`get` reads
a registered completion back.

A completion request names the relevant business ledgers, this
independent completion ledger, the job id, an idempotency key, the
completion moment ``at``, an ``outcome`` (``succeeded`` or ``failed``)
and non-negative integer ``actual_cost`` and ``actual_carbon``. A
success record freezes the generation and resource selection
:func:`carbon_market.rebalance.current` observes at that moment and
reports ``cost_exceeded``/``carbon_exceeded`` against the accepted
job's original limits.

Only a stably finished job completes: it must be traded, its dispatch
decision must be ``succeeded`` and a launch execution plan must be
``completed``, while no active migration, no unsettled terminal
migration and no pending migration settlement may remain; the
completion moment must not precede the latest relevant execution or
migration evidence. Anything short of the stable terminal state raises
``PermissionError``; a backwards moment, an illegal field, a second
completion of the same job under another key or an idempotency key
bound to a different request raises ``ValueError``.

The completion ledger (version 1) holds ``completions`` keyed by
idempotency key, an ``idempotency`` request binding and one ``audit``
event per first-served key, all sections in code-point order. A first
write commits the record, the binding and the audit event in one
synced same-directory atomic replace with a directory fsync; an
interruption leaves either the complete pre-call state or the complete
post-call state. Replaying the same key with the same request returns
the stored record with ``False`` without rewriting a byte.

The root shape (``completions`` in place of ``records``) is deliberately
distinct from every other ledger's, in particular the settlement
ledger's, so the lineage-ledger discovery in :mod:`carbon_market.market`
and :mod:`carbon_market.rebalance` never mistakes a completion ledger
for a business ledger.
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
from . import resources as _resources
from . import signals as _signals
from ._jsonio import finite_loads

__all__ = ["register", "get"]

_VERSION = 1
_ROOT_FIELDS = ("version", "completions", "idempotency", "audit")
_RECORD_FIELDS = ("job_id", "key", "at", "outcome", "actual_cost",
                  "actual_carbon", "generation", "current",
                  "cost_exceeded", "carbon_exceeded")
_REQUEST_FIELDS = ("job_id", "at", "outcome", "actual_cost",
                   "actual_carbon")
_SELECTION_FIELDS = ("resource_id", "version")
_EVENT_FIELDS = ("key", "request", "result")
_OUTCOMES = ("succeeded", "failed")
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


def _validate_selection(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) \
            or set(value.keys()) != set(_SELECTION_FIELDS):
        raise ValueError("completion current binding has invalid fields")
    resource_id = value["resource_id"]
    version = value["version"]
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("completion resource_id must be a non-empty "
                         "string")
    if not _is_plain_int(version) or version < 1:
        raise ValueError("completion resource version must be a positive "
                         "integer")
    return {"resource_id": resource_id, "version": version}


def _validate_request(request: object) -> dict[str, Any]:
    if not isinstance(request, dict) \
            or set(request.keys()) != set(_REQUEST_FIELDS):
        raise ValueError("completion request has invalid fields")
    job_id = request["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("completion request job_id must be a non-empty "
                         "string")
    at = request["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("completion request at must be a non-boolean "
                         "non-negative integer")
    outcome = request["outcome"]
    if outcome not in _OUTCOMES:
        raise ValueError("completion outcome must be succeeded or failed")
    for name in ("actual_cost", "actual_carbon"):
        value = request[name]
        if not _is_plain_int(value) or value < 0:
            raise ValueError(f"{name} must be a non-boolean non-negative "
                             "integer")
    return {"job_id": job_id, "at": at, "outcome": outcome,
            "actual_cost": request["actual_cost"],
            "actual_carbon": request["actual_carbon"]}


def _validate_record(record: object) -> dict[str, Any]:
    if not isinstance(record, dict) \
            or set(record.keys()) != set(_RECORD_FIELDS):
        raise ValueError("completion record has invalid fields")
    job_id = record["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("completion job_id must be a non-empty string")
    key = record["key"]
    if not isinstance(key, str) or not key:
        raise ValueError("completion key must be a non-empty string")
    at = record["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("completion at must be a non-boolean non-negative "
                         "integer")
    outcome = record["outcome"]
    if outcome not in _OUTCOMES:
        raise ValueError("completion outcome must be succeeded or failed")
    for name in ("actual_cost", "actual_carbon"):
        value = record[name]
        if not _is_plain_int(value) or value < 0:
            raise ValueError(f"{name} must be a non-boolean non-negative "
                             "integer")
    generation = record["generation"]
    if not _is_plain_int(generation) or generation < 0:
        raise ValueError("completion generation must be a non-boolean "
                         "non-negative integer")
    current = _validate_selection(record["current"])
    for name in ("cost_exceeded", "carbon_exceeded"):
        if not isinstance(record[name], bool):
            raise ValueError(f"{name} must be a boolean")
    return {
        "job_id": job_id,
        "key": key,
        "at": at,
        "outcome": outcome,
        "actual_cost": record["actual_cost"],
        "actual_carbon": record["actual_carbon"],
        "generation": generation,
        "current": current,
        "cost_exceeded": record["cost_exceeded"],
        "carbon_exceeded": record["carbon_exceeded"],
    }


def _validate_structure(data: object) -> tuple[
        dict[str, dict[str, Any]], dict[str, dict[str, Any]],
        dict[str, dict[str, Any]]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("completion ledger root must be an object with "
                         "keys version, completions, idempotency and audit")
    if not _is_plain_int(data["version"]) or data["version"] != _VERSION:
        raise ValueError("unsupported completion ledger version")
    records_raw = data["completions"]
    idempotency_raw = data["idempotency"]
    audit_raw = data["audit"]
    if not isinstance(records_raw, dict) \
            or not isinstance(idempotency_raw, dict) \
            or not isinstance(audit_raw, dict):
        raise ValueError("completion sections must be objects")
    _check_sorted_keys(records_raw, "completions")
    _check_sorted_keys(idempotency_raw, "idempotency")
    _check_sorted_keys(audit_raw, "audit")

    records: dict[str, dict[str, Any]] = {}
    completed_jobs: set[str] = set()
    for key, record_raw in records_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("completion keys must be non-empty strings")
        record = _validate_record(record_raw)
        if record["key"] != key:
            raise ValueError("completion record key does not match its "
                             "map key")
        if record["job_id"] in completed_jobs:
            raise ValueError("a job can only be completed once")
        completed_jobs.add(record["job_id"])
        records[key] = record

    idempotency: dict[str, dict[str, Any]] = {}
    for key, request_raw in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        request = _validate_request(request_raw)
        if key not in records:
            raise ValueError("idempotency entry must reference a recorded "
                             "completion")
        record = records[key]
        for field in _REQUEST_FIELDS:
            if request[field] != record[field]:
                raise ValueError("idempotency entry does not match its "
                                 "completion")
        idempotency[key] = request

    events: dict[str, dict[str, Any]] = {}
    for key, event_raw in audit_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event_raw, dict) \
                or set(event_raw.keys()) != set(_EVENT_FIELDS):
            raise ValueError("completion audit event has invalid fields")
        if event_raw["key"] != key:
            raise ValueError("audit event key does not match its map key")
        request = _validate_request(event_raw["request"])
        if request != idempotency.get(key):
            raise ValueError("audit event does not match its idempotency "
                             "entry")
        result = _validate_record(event_raw["result"])
        if result != records[key]:
            raise ValueError("audit event result does not match its "
                             "completion record")
        events[key] = {"key": key, "request": request, "result": result}

    if set(records) != set(idempotency) or set(records) != set(events):
        raise ValueError("completion sections do not match")
    return records, idempotency, events


def _canonical_bytes(
    records: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, sections in their
    # fixed field order, each section keyed in code-point order and
    # terminated by exactly one newline.
    payload = {
        "version": _VERSION,
        "completions": {key: records[key] for key in sorted(records)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
        "audit": {key: events[key] for key in sorted(events)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_standalone(
    realpath: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]], bytes | None]:
    # Structural, self-contained read used by get() and by the capacity
    # consumers; a missing ledger simply means nothing has completed.
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return {}, {}, {}, None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"completion ledger {realpath!r} is not valid UTF-8") from exc
    try:
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"completion ledger {realpath!r} is not valid JSON") from exc
    records, idempotency, events = _validate_structure(data)
    if raw != _canonical_bytes(records, idempotency, events):
        raise ValueError(
            f"completion ledger {realpath!r} is not in canonical compact "
            "form")
    return records, idempotency, events, raw


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
                dir=directory, prefix=".completion-restore-", suffix=".tmp")
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
    # One durable commit for the completion record, its request binding
    # and its audit event: synced same-directory temporary, atomic
    # replace and a directory fsync, restoring the pre-call bytes on any
    # failure, so an interruption leaves only the complete old or new
    # state.
    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".completion-", suffix=".tmp")
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


# ---------------------------------------------------------------------------
# Snapshot assembly and completion eligibility
# ---------------------------------------------------------------------------


def _discover_signal_paths(
    anchor_reals: tuple[str, ...],
    known_reals: set[str],
) -> list[str]:
    # Candidate live-signal ledgers beside the anchors, recognized by
    # the signals ledger's own structural validation. The supply ledger
    # shares the root section names but carries supply records, so it
    # fails signal validation and is never a candidate.
    found: list[str] = []
    seen_dirs: set[str] = set()
    for anchor in anchor_reals:
        directory = os.path.dirname(anchor) or "."
        if directory in seen_dirs:
            continue
        seen_dirs.add(directory)
        try:
            names = os.listdir(directory)
        except OSError:
            continue
        for name in names:
            if not name.endswith(".json"):
                continue
            real = os.path.realpath(os.path.join(directory, name))
            if real in known_reals or real in found:
                continue
            try:
                _signals._load_file(real)
            except (ValueError, OSError):
                continue
            found.append(real)
    found.sort()
    return found


def _latest_settled_binding(
    finalized_by_job: dict[str, list[dict[str, Any]]],
    job_id: str,
) -> dict[str, Any] | None:
    # The highest-generation completed settlement binding current()
    # would observe for the job, aggregated over every discovered
    # settlement ledger.
    items = finalized_by_job.get(job_id)
    if not items:
        return None
    return max(items, key=lambda record: record["generation"])


def _migration_evidence_at(
    plan: dict[str, Any],
    intent_events: dict[str, dict[str, Any]],
    plan_key: str,
    version: int,
) -> int | None:
    # The last migration evidence moment one plan contributes: the
    # recorded recovery moment for an interrupted plan, otherwise the
    # last committed step receipt. An empty active plan contributes
    # nothing.
    moments = [receipt["at"] for receipt in plan["steps"]]
    if plan["state"] == "interrupted":
        # Reuse the settlement module's own recovery-moment rule rather
        # than re-deriving it.
        from . import rebalance as _rebalance
        moments.append(_rebalance._terminal_plan_at(
            plan, intent_events, plan_key, version))
    return max(moments, default=None)


def register(
    jobs: str,
    supply: str,
    trades: str,
    dispatch: str,
    execution: str,
    ledger: str,
    job_id: str,
    key: str,
    at: int,
    outcome: str,
    actual_cost: int,
    actual_carbon: int,
    *,
    signals: str | None = None,
) -> tuple[dict[str, object], bool]:
    """Register one job's completion idempotently.

    ``jobs``, ``supply``, ``trades``, ``dispatch`` and ``execution`` are
    the relevant business ledgers and ``ledger`` the independent
    completion ledger; all six paths, ``job_id`` and ``key`` must be
    non-empty strings resolving (together with the optional ``signals``
    path) to distinct real locations. ``at`` must be a non-boolean
    non-negative integer, ``outcome`` exactly ``succeeded`` or
    ``failed`` and ``actual_cost``/``actual_carbon`` non-boolean
    non-negative integers; any violation raises ``ValueError`` before a
    business file is read.

    The business ledgers are read as one snapshot under their shared
    locks together with the completion ledger's exclusive lock, all
    taken in resolved real-path order. The migration lineage -- advice,
    intent and settlement ledgers -- is discovered beside the explicit
    ledgers exactly as in ``rebalance.evaluate`` and read under shared
    locks, so a job that never migrated needs none of them.

    Only a stably finished job may complete: it must be traded, its
    dispatch decision must be ``succeeded``, a launch execution plan
    must be ``completed``, and no active migration, unsettled terminal
    migration or pending migration settlement may remain. Failing the
    stable terminal state raises ``PermissionError`` and writes nothing.
    The completion moment must not precede the latest relevant
    execution, migration or settlement evidence, else ``ValueError``.

    Returns ``(record, created)``. The success record freezes the
    generation and resource selection ``rebalance.current`` observes --
    the settled binding after a migration chain, else the immutable
    trade selection at generation 0 -- and carries
    ``cost_exceeded``/``carbon_exceeded`` against the job's original
    ``max_cost``/``carbon_cap``. The record, the idempotency binding and
    the audit event land in one synced atomic write. Replaying the same
    key with the same request returns the stored record with ``False``
    without rewriting a byte; the same key with a different request, or
    the same job completed under another key, raises ``ValueError``.

    An unknown job raises ``KeyError``. Missing input ledgers or the
    completion ledger parent raise ``FileNotFoundError``; invalid
    arguments, structure, ordering, references or non-canonical bytes
    raise ``ValueError``; other locking or I/O failures raise
    ``OSError``.
    """
    # Imported lazily: market imports this module for its consumers.
    from . import market as _market
    from . import rebalance as _rebalance

    for value in (jobs, supply, trades, dispatch, execution, ledger,
                  job_id, key):
        if not isinstance(value, str) or not value:
            raise ValueError("the six paths, job_id and key must be "
                             "non-empty strings")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")
    if outcome not in _OUTCOMES:
        raise ValueError("outcome must be succeeded or failed")
    if not _is_plain_int(actual_cost) or actual_cost < 0:
        raise ValueError("actual_cost must be a non-boolean non-negative "
                         "integer")
    if not _is_plain_int(actual_carbon) or actual_carbon < 0:
        raise ValueError("actual_carbon must be a non-boolean non-negative "
                         "integer")
    if signals is not None and (not isinstance(signals, str)
                                or not signals):
        raise ValueError("signals must be a non-empty string when given")

    job_real = os.path.realpath(jobs)
    supply_real = os.path.realpath(supply)
    trades_real = os.path.realpath(trades)
    dispatch_real = os.path.realpath(dispatch)
    execution_real = os.path.realpath(execution)
    ledger_real = os.path.realpath(ledger)
    signal_real = os.path.realpath(signals) if signals is not None else None
    anchor_reals = tuple(real for real in (
        job_real, supply_real, trades_real, dispatch_real, execution_real,
        ledger_real, signal_real) if real is not None)
    if len(set(anchor_reals)) != len(anchor_reals):
        raise ValueError("the completion paths must be distinct real paths")

    store = _get_store(ledger)
    with store.lock:
        # Discover the migration lineage beside every anchor directory
        # before locking, then take every lock in one resolved-real-path
        # order: the completion ledger exclusive, every business ledger
        # (including the discovered lineage) shared. A concurrent
        # settlement or clearing call locks a subset in the same global
        # order, so the two can never deadlock, and the completion view
        # is one consistent snapshot.
        discovered = _rebalance._discover_lineage_paths(
            anchor_reals, ("advice", "intent", "settlement"))
        snapshot_real_set = set(anchor_reals)
        known_reals = snapshot_real_set | {
            real for kind in ("advice", "intent", "settlement")
            for real in discovered.get(kind, ())}
        # Advice and intent ledgers freeze signal records, so validating
        # a discovered lineage needs the live signal ledger. When the
        # caller does not name it, the relevant ledger is discovered
        # beside the anchors: sibling JSON files that parse as the
        # signals ledger (the supply ledger shares the root section
        # names but fails the signal-record validation). Every
        # candidate is locked and the lineage is read under the one
        # whose signal history validates the frozen references.
        signal_candidates: list[str] = []
        if signal_real is None and (discovered.get("advice")
                                    or discovered.get("intent")):
            signal_candidates = _discover_signal_paths(
                anchor_reals, known_reals)
        extra_reals = {real for kind in ("advice", "intent", "settlement")
                       for real in discovered.get(kind, ())
                       if real not in snapshot_real_set}
        lock_reals = snapshot_real_set | extra_reals | set(signal_candidates)
        with contextlib.ExitStack() as stack:
            for locked in sorted(lock_reals):
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
            if signal_real is not None:
                explicit_signal_history, _sm, _se, signal_raw = \
                    _signals._load_file(signal_real)
                if signal_raw is None:
                    raise FileNotFoundError(
                        f"signal file {signal_real!r} does not exist")
            else:
                explicit_signal_history = None
            # Cleared live trades validate arithmetically without the
            # signal history; the discovered migration lineage is what
            # needs it (frozen advice/intent signals).
            cleared, _clear_keys, trades_raw = _market._load_clear_ledger(
                trades_real, accepted, history, None)
            if trades_raw is None:
                raise FileNotFoundError(
                    f"clearing ledger {trades_real!r} does not exist")
            decisions, dispatch_idem, dispatch_events, dispatch_raw = \
                _dispatch._load_ledger(dispatch_real)
            if dispatch_raw is None:
                raise FileNotFoundError(
                    f"dispatch ledger {dispatch_real!r} does not exist")
            exec_plans, _plan_keys, exec_events = \
                _execution._load_existing_ledger(execution_real)[:3]

            # The migration lineage is optional for a job that never
            # migrated; discovered ledgers are validated exactly as in
            # apply: the advice records ground the intent ledger, and
            # every settlement ledger is validated against that one
            # intent ledger. Frozen advice/intent signals pin the live
            # signal ledger: the explicitly supplied one when given,
            # otherwise the discovered candidate whose history validates
            # the frozen references.
            settlement_reals = [
                real for real in discovered.get("settlement", ())
                if real not in snapshot_real_set]
            advice_reals = [
                real for real in discovered.get("advice", ())
                if real not in snapshot_real_set]
            intent_real = next(
                (real for real in discovered.get("intent", ())
                 if real not in snapshot_real_set), None)

            def load_lineage(signal_history: object):
                adv: dict[str, dict[str, Any]] = {}
                if advice_reals:
                    adv, _adv_events, adv_raw = _rebalance._load_ledger(
                        advice_reals[0], accepted, cleared, history,
                        signal_history, defer_current=True)
                    if adv_raw is None:
                        raise FileNotFoundError(
                            f"advice ledger {advice_reals[0]!r} does not "
                            "exist")
                if intent_real is None:
                    return (adv, {}, {}, {}, {}, _rebalance._INTENT_VERSION)
                (lineage_intents, lineage_plans, lineage_idempotency,
                 lineage_events, lineage_version, intent_raw) = \
                    _rebalance._load_intent_ledger(
                        intent_real, accepted, cleared, history,
                        signal_history, adv)
                if intent_raw is None:
                    raise FileNotFoundError(
                        f"intent ledger {intent_real!r} does not exist")
                return (adv, lineage_intents, lineage_plans,
                        lineage_idempotency, lineage_events,
                        lineage_version)

            signal_history = explicit_signal_history
            needs_signals = bool(advice_reals) or intent_real is not None
            if signal_history is None and needs_signals:
                # The frozen advice/intent signals pin a live signal
                # ledger; the explicit one was not given, so the
                # lineage must validate against a discovered candidate.
                resolved: tuple | None = None
                last_error: Exception | None = None
                for candidate in signal_candidates:
                    candidate_history, _sm, _se = \
                        _signals._load_file(candidate)[:3]
                    try:
                        resolved = load_lineage(candidate_history)
                        signal_history = candidate_history
                        break
                    except ValueError as exc:
                        last_error = exc
                if resolved is None:
                    if last_error is not None:
                        raise last_error
                    raise FileNotFoundError(
                        "the migration lineage requires the live signal "
                        "ledger")
            else:
                resolved = load_lineage(signal_history)
            (advice_records, lineage_intents, lineage_plans,
             lineage_idempotency, lineage_events,
             lineage_version) = resolved
            plans_by_key = _rebalance._plans_by_start_key(
                lineage_plans, lineage_idempotency, lineage_version)

            finalized_by_job: dict[str, list[dict[str, Any]]] = {}
            pending_jobs: set[str] = set()
            for settlement_real in settlement_reals:
                s_records, _sid, _sev, s_raw = \
                    _rebalance._load_settlement_ledger(
                        settlement_real, accepted, cleared, plans_by_key,
                        lineage_idempotency, lineage_events,
                        lineage_version)
                if s_raw is None:
                    raise FileNotFoundError(
                        f"settlement ledger {settlement_real!r} does not "
                        "exist")
                for s_record in s_records.values():
                    if s_record["state"] == "pending":
                        pending_jobs.add(s_record["job_id"])
                    else:
                        finalized_by_job.setdefault(
                            s_record["job_id"], []).append(s_record)

            # Existing completion records are revalidated against this
            # very snapshot: frozen generation/binding references and
            # the original-limit flags must still follow it.
            records, idempotency, events, old_bytes = \
                _load_standalone(ledger_real)
            for existing in records.values():
                _cross_validate_record(
                    existing, accepted, cleared, finalized_by_job)

            request = {"job_id": job_id, "at": at, "outcome": outcome,
                       "actual_cost": actual_cost,
                       "actual_carbon": actual_carbon}
            binding = idempotency.get(key)
            if binding is not None:
                if binding != request:
                    raise ValueError("idempotency key was already used with "
                                     "a different request")
                # An equivalent replay returns the stored record without
                # rewriting a byte.
                return copy.deepcopy(records[key]), False

            job = accepted.get(job_id)
            if job is None:
                raise KeyError(job_id)

            if any(record["job_id"] == job_id
                   for record in records.values()):
                raise ValueError("job is already completed under another "
                                 "idempotency key")

            trade = cleared.get(job_id)
            decision = decisions.get(job_id)
            job_exec_plans = exec_plans.get(job_id, {})
            job_lineage_plans = [plan for plan in plans_by_key.values()
                                 if plan["job_id"] == job_id]
            settled_plan_keys = {
                s_record["plan_key"]
                for s_record in finalized_by_job.get(job_id, ())}

            # Stable terminal state: traded, dispatch succeeded, a launch
            # plan completed, and nothing left open in the execution or
            # migration lineage. Every unmet predicate is a
            # PermissionError, and the whole check precedes the
            # moment-ordering ValueError.
            stable = trade is not None
            stable = stable and decision is not None \
                and decision["state"] == "succeeded"
            stable = stable and any(
                plan["kind"] == "launch" and plan["state"] == "completed"
                for plan in job_exec_plans.values())
            stable = stable and not any(
                plan["state"] == "active"
                for plan in job_exec_plans.values())
            # A reservation without a started migration plan still
            # occupies its target and keeps the run in flight.
            claimed_refs = _rebalance._claimed_intent_refs(
                plans_by_key, lineage_idempotency, lineage_version)
            stable = stable and not any(
                ref not in claimed_refs
                for ref, _record in _rebalance._job_intent_items(
                    lineage_intents, lineage_version, job_id))
            stable = stable and not any(
                plan["state"] == "active" for plan in job_lineage_plans)
            stable = stable and not any(
                plan["state"] in ("migrated", "failed", "interrupted")
                and plan_key not in settled_plan_keys
                for plan_key, plan in plans_by_key.items()
                if plan["job_id"] == job_id)
            stable = stable and job_id not in pending_jobs
            if not stable:
                raise PermissionError("the job is not in a stable terminal "
                                      "state")

            # The completion moment must not precede the latest relevant
            # execution, dispatch, migration or settlement evidence:
            # every launch/migrate receipt, each execution-recovery
            # moment, the succeeded dispatch finish, every migration
            # plan receipt/recovery in the intent ledger, every
            # settlement moment and the trade itself.
            evidence = [trade["at"]]
            for attempt_plan in job_exec_plans.values():
                evidence.extend(
                    receipt["at"] for receipt in attempt_plan["steps"])
                if attempt_plan["state"] == "interrupted":
                    attempt = attempt_plan["attempt"]
                    for event in exec_events.values():
                        event_request = event["request"]
                        if event_request.get("action") == "recover" \
                                and event_request.get("job_id") == job_id \
                                and event_request.get("attempt") == attempt:
                            evidence.append(event_request["at"])
            for event in dispatch_events.values():
                event_request = event["request"]
                if event_request.get("job_id") == job_id:
                    evidence.append(event_request["at"])
            for plan_key, plan in plans_by_key.items():
                if plan["job_id"] != job_id:
                    continue
                moment = _migration_evidence_at(
                    plan, lineage_events, plan_key, lineage_version)
                if moment is not None:
                    evidence.append(moment)
            evidence.extend(
                s_record["at"]
                for s_record in finalized_by_job.get(job_id, ()))
            if at < max(evidence):
                raise ValueError("completion moment must not precede the "
                                 "latest execution or migration evidence")

            settled = _latest_settled_binding(finalized_by_job, job_id)
            if settled is not None:
                generation = settled["generation"]
                current = dict(settled["after"])
            else:
                generation = 0
                current = {"resource_id": trade["resource_id"],
                           "version": trade["version"]}

            record: dict[str, Any] = {
                "job_id": job_id,
                "key": key,
                "at": at,
                "outcome": outcome,
                "actual_cost": actual_cost,
                "actual_carbon": actual_carbon,
                "generation": generation,
                "current": current,
                "cost_exceeded": actual_cost > job["max_cost"],
                "carbon_exceeded": actual_carbon > job["carbon_cap"],
            }
            _cross_validate_record(
                record, accepted, cleared, finalized_by_job)
            records[key] = record
            idempotency[key] = dict(request)
            events[key] = {"key": key, "request": dict(request),
                           "result": copy.deepcopy(record)}
            _commit_file(ledger_real,
                         _canonical_bytes(records, idempotency, events),
                         old_bytes)
            return copy.deepcopy(record), True


def _cross_validate_record(
    record: dict[str, Any],
    accepted: dict[str, dict[str, Any]],
    cleared: dict[str, dict[str, Any]],
    finalized_by_job: dict[str, list[dict[str, Any]]],
) -> None:
    # Revalidate one stored completion against the business snapshot:
    # the job must still be accepted and traded, the frozen binding must
    # follow rebalance.current's lineage, and the exceeded flags must be
    # the job's original limits.
    job = accepted.get(record["job_id"])
    if job is None:
        raise ValueError("completion record must reference an accepted "
                         "job")
    trade = cleared.get(record["job_id"])
    if trade is None:
        raise ValueError("completion record must reference a recorded "
                         "trade")
    if record["cost_exceeded"] != \
            (record["actual_cost"] > job["max_cost"]):
        raise ValueError("completion cost_exceeded does not match the "
                         "job's original max_cost")
    if record["carbon_exceeded"] != \
            (record["actual_carbon"] > job["carbon_cap"]):
        raise ValueError("completion carbon_exceeded does not match the "
                         "job's original carbon_cap")
    current = record["current"]
    generation = record["generation"]
    if generation == 0:
        if current != {"resource_id": trade["resource_id"],
                       "version": trade["version"]}:
            raise ValueError("generation-0 completion must freeze the "
                             "trade's resource selection")
        return
    matches = [s_record for s_record
               in finalized_by_job.get(record["job_id"], ())
               if s_record["generation"] == generation]
    if not any(dict(s_record["after"]) == current for s_record in matches):
        raise ValueError("completion current binding must match a "
                         "completed settlement generation")


def get(ledger: str, job_id: str) -> dict[str, object]:
    """Return a read-only copy of one job's completion record.

    ``ledger`` and ``job_id`` must be non-empty strings. A missing
    completion ledger raises ``FileNotFoundError``; a job that is
    unknown or has not completed raises ``KeyError``; invalid ledger
    bytes raise ``ValueError``; other locking or I/O failures raise
    ``OSError``. The call takes the shared lock and never writes.
    """
    if not isinstance(ledger, str) or not ledger:
        raise ValueError("ledger must be a non-empty string")
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("job_id must be a non-empty string")

    realpath = os.path.realpath(ledger)
    with _lock(realpath, shared=True):
        records, _idempotency, _events, raw = _load_standalone(realpath)
    if raw is None:
        raise FileNotFoundError(
            f"completion ledger {realpath!r} does not exist")
    for record in records.values():
        if record["job_id"] == job_id:
            return copy.deepcopy(record)
    raise KeyError(job_id)
