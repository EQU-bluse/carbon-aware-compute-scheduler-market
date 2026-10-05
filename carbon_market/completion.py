"""Persistent job-completion registry.

The ``carbon_market.completion`` module adds the public completion layer
on top of the booking, dispatch, execution and migration-settlement
layers: a finished, settled job is *completed* once, freezing the
generation and resource version :func:`carbon_market.rebalance.current`
shows at the completion moment, together with the business outcome and
the actual cost and carbon figures.

Two public calls share one independent completion ledger:

* :func:`complete` registers the terminal completion record, its
  idempotency request binding and an audit event in one synced atomic
  commit.
* :func:`get` answers the recorded terminal state for one job id.

:func:`search` is the read-only paginated query over the same ledger:
it scans the recorded completions in ascending job-id code-point order,
keeps only the records strictly past an optional exclusive cursor and
matching the optional outcome and budget-exceeded filters (combined by
logical AND), and returns one page of entries together with the ``next``
cursor. :func:`get_response` and :func:`search_response` are the
conditional HTTP-ready counterparts: each forms the same business
object, serializes it to compact UTF-8 JSON bytes and computes the
strong entity tag while the ledger's shared lock is held, so the tag
always summarizes one complete ledger version.

The migration advice, intent and settlement ledgers are not part of the
call shape: exactly as :func:`carbon_market.rebalance.evaluate` does,
they are discovered beside the six business snapshots when present, so
a job that never migrated completes from its immutable trade while a
settled job completes from its latest current binding.

The ledger (``version``, ``completions``, ``idempotency`` and ``audit``,
each keyed by the idempotency key in code-point order) is one compact
UTF-8 JSON document with non-ASCII written through, no negative-zero or
non-finite number literals and exactly one trailing newline; every read
accepts that canonical form only.
"""

from __future__ import annotations

import contextlib
import copy
import hashlib
import json
import os
from typing import Any

from . import _lifecycle
from . import dispatch as _dispatch
from . import execution as _execution
from . import jobs as _jobs
from . import market as _market
from . import resources as _resources
from . import signals as _signals

__all__ = ["complete", "get", "search", "get_response", "search_response"]

_VERSION = 1
_ROOT_FIELDS = ("version", "completions", "idempotency", "audit")
_RECORD_FIELDS = ("job_id", "at", "outcome", "actual_cost",
                  "actual_carbon", "generation", "current",
                  "cost_exceeded", "carbon_exceeded")
_SELECTION_FIELDS = ("resource_id", "version")
_OUTCOMES = ("succeeded", "failed")
_EVENT_FIELDS = ("key", "request", "result")
_REQUEST_FIELDS = ("job_id", "at", "outcome", "actual_cost",
                   "actual_carbon")
_DEFAULT_LIMIT = 100
_MAX_LIMIT = 1000
_EXCEEDED_FILTERS = ("cost", "carbon", "any", "none")
_HEX_DIGITS = frozenset("0123456789abcdef")

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
        raise ValueError("completion current selection has invalid fields")
    resource_id = value["resource_id"]
    version = value["version"]
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("completion resource_id must be a non-empty "
                         "string")
    if not _is_plain_int(version) or version < 1:
        raise ValueError("completion version must be a positive integer")
    return {"resource_id": resource_id, "version": version}


def _validate_request(request: object) -> dict[str, Any]:
    if not isinstance(request, dict) \
            or set(request.keys()) != set(_REQUEST_FIELDS):
        raise ValueError("completion request has invalid fields")
    job_id = request["job_id"]
    at = request["at"]
    outcome = request["outcome"]
    actual_cost = request["actual_cost"]
    actual_carbon = request["actual_carbon"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("completion request job_id must be a non-empty "
                         "string")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("completion request at must be a non-boolean "
                         "non-negative integer")
    if outcome not in _OUTCOMES:
        raise ValueError("completion outcome must be succeeded or failed")
    if not _is_plain_int(actual_cost) or actual_cost < 0:
        raise ValueError("completion request actual_cost must be a "
                         "non-boolean non-negative integer")
    if not _is_plain_int(actual_carbon) or actual_carbon < 0:
        raise ValueError("completion request actual_carbon must be a "
                         "non-boolean non-negative integer")
    return {"job_id": job_id, "at": at, "outcome": outcome,
            "actual_cost": actual_cost, "actual_carbon": actual_carbon}


def _validate_record(
    record: object,
    accepted: dict[str, dict[str, Any]] | None = None,
    history: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    if not isinstance(record, dict) \
            or set(record.keys()) != set(_RECORD_FIELDS):
        raise ValueError("completion record has invalid fields")
    job_id = record["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("completion job_id must be a non-empty string")
    at = record["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("completion at must be a non-boolean non-negative "
                         "integer")
    outcome = record["outcome"]
    if outcome not in _OUTCOMES:
        raise ValueError("completion outcome is invalid")
    actual_cost = record["actual_cost"]
    actual_carbon = record["actual_carbon"]
    if not _is_plain_int(actual_cost) or actual_cost < 0:
        raise ValueError("completion actual_cost must be a non-boolean "
                         "non-negative integer")
    if not _is_plain_int(actual_carbon) or actual_carbon < 0:
        raise ValueError("completion actual_carbon must be a non-boolean "
                         "non-negative integer")
    generation = record["generation"]
    if not _is_plain_int(generation) or generation < 0:
        raise ValueError("completion generation must be a non-boolean "
                         "non-negative integer")
    current = _validate_selection(record["current"])
    cost_exceeded = record["cost_exceeded"]
    carbon_exceeded = record["carbon_exceeded"]
    if not isinstance(cost_exceeded, bool) \
            or not isinstance(carbon_exceeded, bool):
        raise ValueError("completion limit flags must be booleans")

    # With the business snapshots in hand the frozen references and
    # flags are revalidated exactly as the other ledgers revalidate
    # theirs.
    if accepted is not None:
        job = accepted.get(job_id)
        if job is None:
            raise ValueError("completion record must reference an accepted "
                             "job")
        if cost_exceeded != (actual_cost > job["max_cost"]):
            raise ValueError("completion cost_exceeded does not match the "
                             "job's original cost upper bound")
        if carbon_exceeded != (actual_carbon > job["carbon_cap"]):
            raise ValueError("completion carbon_exceeded does not match "
                             "the job's original carbon upper bound")
    if history is not None:
        published = history.get(current["resource_id"])
        if published is None or current["version"] > len(published):
            raise ValueError("completion current must reference a "
                             "published resource version")
    return {
        "job_id": job_id,
        "at": at,
        "outcome": outcome,
        "actual_cost": actual_cost,
        "actual_carbon": actual_carbon,
        "generation": generation,
        "current": current,
        "cost_exceeded": cost_exceeded,
        "carbon_exceeded": carbon_exceeded,
    }


def _validate_ledger(
    data: object,
    accepted: dict[str, dict[str, Any]] | None = None,
    history: dict[str, list[dict[str, Any]]] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
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
        raise ValueError("completions, idempotency and audit must be "
                         "objects")
    _check_sorted_keys(records_raw, "completions")
    _check_sorted_keys(idempotency_raw, "idempotency")
    _check_sorted_keys(audit_raw, "audit")

    records: dict[str, dict[str, Any]] = {}
    completed_jobs: set[str] = set()
    for key, record_raw in records_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        record = _validate_record(record_raw, accepted, history)
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
        if any(request[field] != record[field] for field in _REQUEST_FIELDS):
            raise ValueError("idempotency entry does not match its "
                             "completion record")
        idempotency[key] = request

    events: dict[str, dict[str, Any]] = {}
    for key, event_raw in audit_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event_raw, dict) \
                or set(event_raw.keys()) != set(_EVENT_FIELDS):
            raise ValueError("completion audit event has invalid fields")
        if event_raw["key"] != key or not isinstance(event_raw["key"], str):
            raise ValueError("audit event key does not match its map key")
        request = _validate_request(event_raw["request"])
        if request != idempotency.get(key):
            raise ValueError("audit event does not match its idempotency "
                             "entry")
        result = _validate_record(event_raw["result"], accepted, history)
        if result != records[key]:
            raise ValueError("audit event result does not match its record")
        events[key] = {"key": key, "request": request, "result": result}

    if set(events) != set(records) or set(idempotency) != set(records):
        raise ValueError("completion sections do not match")
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
        "completions": {key: records[key] for key in sorted(records)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
        "audit": {key: events[key] for key in sorted(events)},
    }
    text = json.dumps(payload, ensure_ascii=False,
                      separators=(",", ":"), allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_completion_ledger(
    realpath: str,
    accepted: dict[str, dict[str, Any]] | None = None,
    history: dict[str, list[dict[str, Any]]] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]], bytes | None]:
    def validate(
        data: object,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
               dict[str, dict[str, Any]]]:
        return _validate_ledger(data, accepted, history)

    def canonical(
        sections: tuple[dict[str, dict[str, Any]],
                        dict[str, dict[str, Any]],
                        dict[str, dict[str, Any]]],
    ) -> bytes:
        records, idempotency, events = sections
        return _canonical_bytes(records, idempotency, events)

    sections, raw = _lifecycle.load_canonical(
        realpath, "completion ledger", validate, canonical)
    if sections is None:
        return {}, {}, {}, None
    records, idempotency, events = sections
    return records, idempotency, events, raw


def _fsync_directory(directory: str) -> None:
    _lifecycle.fsync_directory(directory)


def _rollback_file(realpath: str, directory: str, old_bytes: bytes | None,
                   first: BaseException) -> None:
    _lifecycle.rollback_file(realpath, directory, old_bytes, first,
                             prefix=".completion-restore-",
                             fsync_dir=_fsync_directory)


def _commit_file(realpath: str, payload: bytes,
                 old_bytes: bytes | None) -> None:
    # One durable commit for the record, the idempotency binding and the
    # audit event: synced same-directory temporary, atomic replace and a
    # directory fsync, restoring the pre-call bytes on any failure, so a
    # failed call leaves neither a fragment nor half an event and an
    # interruption observes only the pre- or post-commit bytes.
    _lifecycle.commit_file(realpath, payload, old_bytes,
                           prefix=".completion-",
                           fsync_dir=_fsync_directory)


def _record_for_job(
    records: dict[str, dict[str, Any]],
    job_id: str,
) -> dict[str, Any] | None:
    for record in records.values():
        if record["job_id"] == job_id:
            return record
    return None


def complete(
    jobs: str,
    supply: str,
    signals: str,
    trades: str,
    dispatch: str,
    execution: str,
    completions: str,
    job_id: str,
    key: str,
    at: int,
    outcome: str,
    actual_cost: int,
    actual_carbon: int,
) -> tuple[dict[str, object], bool]:
    """Register the terminal completion of one finished job.

    The six business paths and the independent ``completions`` ledger
    path, ``job_id`` and ``key`` must be non-empty strings resolving to
    seven distinct real locations; ``at`` must be a non-boolean
    non-negative integer, ``outcome`` exactly ``succeeded`` or
    ``failed`` and ``actual_cost`` and ``actual_carbon`` non-boolean
    non-negative integers. Any violation raises ``ValueError`` before a
    business file is read.

    The six business snapshots are read under shared locks together with
    the completion ledger's exclusive lock, all taken in resolved
    real-path order; the independent migration advice, intent and
    settlement ledgers beside the snapshots are discovered and read
    shared as part of the same snapshot, so the registration, a
    concurrent clearing or migration decision and a concurrent migration
    settlement observe one consistent version.

    Only a job in a stable terminal state completes: it must be traded,
    its dispatch decision must be ``succeeded``, the corresponding
    launch execution plan (the highest-attempt launch plan) must be
    ``completed``, and there must be no active migration, no unsettled
    terminal migration and no pending migration settlement; the
    completion moment must not precede the last recorded execution or
    migration evidence. An accepted job outside that predicate raises
    ``PermissionError``; an unknown job raises ``KeyError``.

    A first success freezes, in order, the job id, the completion
    moment, the outcome, the actual figures, the generation and resource
    selection :func:`carbon_market.rebalance.current` shows and the
    ``cost_exceeded``/``carbon_exceeded`` flags against the job's
    original upper bounds, and commits the record, the idempotency
    binding and one audit event in a single synced atomic write; the
    call returns ``(record, True)``. Replaying the same key with the
    same request returns the stored record with ``False`` without
    rewriting a byte; the same key bound to another request, or the same
    job completed under another key, raises ``ValueError`` and leaves
    every ledger untouched. A refused or failed request never creates a
    completion record or changes an existing ledger.

    Missing input files or the completion ledger parent raise
    ``FileNotFoundError``; invalid arguments, a completion moment
    earlier than the last evidence, structure, ordering, references or
    non-canonical bytes raise ``ValueError``; other locking or I/O
    failures raise ``OSError``.
    """
    # Imported lazily: the completion layer sits on top of rebalance and
    # must not pull it in at module import time.
    from . import rebalance as _rebalance

    for value in (jobs, supply, signals, trades, dispatch, execution,
                  completions, job_id, key):
        if not isinstance(value, str) or not value:
            raise ValueError("the seven paths, job_id and key must be "
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

    job_real = os.path.realpath(jobs)
    supply_real = os.path.realpath(supply)
    signal_real = os.path.realpath(signals)
    trades_real = os.path.realpath(trades)
    dispatch_real = os.path.realpath(dispatch)
    execution_real = os.path.realpath(execution)
    completion_real = os.path.realpath(completions)
    all_paths = (job_real, supply_real, signal_real, trades_real,
                 dispatch_real, execution_real, completion_real)
    if len(set(all_paths)) != 7:
        raise ValueError("the seven paths must be distinct real paths")

    store = _get_store(completion_real)
    with store.lock:
        # The migration lineage and sibling completion ledgers are
        # discovered beside the snapshots before any lock is taken, the
        # same mechanism evaluate uses for its lineage. The completion
        # ledger's own root shape (completions + idempotency) is
        # distinct from every lineage shape and is never mistaken for
        # one.
        discovered = _rebalance._discover_lineage_paths(
            all_paths, ("advice", "intent", "settlement"))
        lineage_reals = {real for kind in ("advice", "intent", "settlement")
                         for real in discovered.get(kind, ())
                         if real not in set(all_paths)}
        sibling_completions = [
            real for real in _market._discover_completion_paths(all_paths)
            if real != completion_real]
        extra_reals = lineage_reals | set(sibling_completions)
        with contextlib.ExitStack() as stack:
            for locked in sorted(set(all_paths) | extra_reals):
                stack.enter_context(
                    _lock(locked, shared=(locked != completion_real)))

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
            # Every completion ledger beside the snapshots feeds the
            # envelope check, this call's target included when it
            # already exists; the write itself lands only in the target.
            envelope_paths = list(sibling_completions)
            if os.path.exists(completion_real):
                envelope_paths.append(completion_real)
            cleared, _clear_keys, trades_raw = _market._load_clear_ledger(
                trades_real, accepted, history, signal_history,
                completion_paths=envelope_paths)
            if trades_raw is None:
                raise FileNotFoundError(
                    f"clearing ledger {trades_real!r} does not exist")
            decisions, _dispatch_keys, dispatch_events, dispatch_raw = \
                _dispatch._load_ledger(dispatch_real)
            if dispatch_raw is None:
                raise FileNotFoundError(
                    f"dispatch ledger {dispatch_real!r} does not exist")
            exec_plans, _plan_keys, exec_events = \
                _execution._load_existing_ledger(execution_real)[:3]

            # Load the discovered lineage exactly in evaluate's
            # reference order: advice first (current grounding
            # deferred), then the intent ledger, then the settlement
            # ledger, and only then ground the advice currents.
            snapshot_real_set = set(all_paths)
            advice_real = next(
                (real for real in discovered.get("advice", ())
                 if real not in snapshot_real_set), None)
            intent_real = next(
                (real for real in discovered.get("intent", ())
                 if real not in snapshot_real_set), None)
            settlement_real = next(
                (real for real in discovered.get("settlement", ())
                 if real not in snapshot_real_set), None)
            advice_records: dict[str, dict[str, Any]] = {}
            lineage_intents: dict[str, dict[str, Any]] = {}
            lineage_plans: dict[str, dict[str, Any]] = {}
            lineage_idempotency: dict[str, dict[str, Any]] = {}
            lineage_events: dict[str, dict[str, Any]] = {}
            lineage_version = _rebalance._INTENT_VERSION
            if advice_real is not None:
                advice_records, _advice_events, _advice_raw = \
                    _rebalance._load_ledger(
                        advice_real, accepted, cleared, history,
                        signal_history, defer_current=True)
            if intent_real is not None:
                (lineage_intents, lineage_plans, lineage_idempotency,
                 lineage_events, lineage_version, _ir) = \
                    _rebalance._load_intent_ledger(
                        intent_real, accepted, cleared, history,
                        signal_history, advice_records)
                lineage_plans = _rebalance._plans_by_start_key(
                    lineage_plans, lineage_idempotency, lineage_version)
            completed_settlements: dict[str, list[dict[str, Any]]] = {}
            pending_settlements: set[str] = set()
            if settlement_real is not None:
                s_records, _sid, _sev, _sraw = \
                    _rebalance._load_settlement_ledger(
                        settlement_real, accepted, cleared, lineage_plans,
                        lineage_idempotency, lineage_events,
                        lineage_version)
                completed_settlements, pending_settlements = \
                    _rebalance._lineage_from_settlement_records(s_records)
                settlement_currents = _rebalance._settlement_currents(
                    completed_settlements)
                for advice_record in advice_records.values():
                    _rebalance._ground_advice_current(
                        advice_record, cleared, settlement_currents)

            records, idempotency, events, old_bytes = \
                _load_completion_ledger(completion_real, accepted, history)

            # Sibling completion ledgers beside the snapshots are part of
            # the same completion state: a job already completed in one
            # of them cannot be completed again in this one.
            sibling_union: dict[str, dict[str, Any]] = {}
            for sibling_real in sibling_completions:
                sibling_records, _sids, _sevents, _sraw = \
                    _load_completion_ledger(sibling_real, accepted, history)
                for sibling_record in sibling_records.values():
                    sibling_union[sibling_record["job_id"]] = sibling_record

            request = {"job_id": job_id, "at": at, "outcome": outcome,
                       "actual_cost": actual_cost,
                       "actual_carbon": actual_carbon}
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
            if _record_for_job(records, job_id) is not None \
                    or job_id in sibling_union:
                raise ValueError("the job is already completed under "
                                 "another idempotency key")

            # Stable-terminal predicate, in one fixed order. Every
            # failure to reach a stable terminal state is a
            # PermissionError.
            trade = cleared.get(job_id)
            decision = decisions.get(job_id)
            if trade is None or decision is None \
                    or decision["state"] != "succeeded":
                raise PermissionError("the job is not in a stable terminal "
                                      "state")
            launch_plans = [plan for plan in
                            exec_plans.get(job_id, {}).values()
                            if plan["kind"] == "launch"]
            if not launch_plans \
                    or max(launch_plans,
                           key=lambda plan: plan["attempt"])["state"] \
                    != "completed":
                raise PermissionError("the job's launch execution plan is "
                                      "not completed")
            job_migration_plans = {
                plan_key: plan for plan_key, plan in lineage_plans.items()
                if plan["job_id"] == job_id}
            if any(plan["state"] == "active"
                   for plan in job_migration_plans.values()):
                raise PermissionError("the job still has an active "
                                      "migration")
            if job_id in pending_settlements:
                raise PermissionError("the job still holds a pending "
                                      "migration settlement")
            # A reservation never claimed by a plan still holds target
            # capacity and keeps the job outside a stable terminal
            # state.
            claimed_refs = _rebalance._claimed_intent_refs(
                lineage_plans, lineage_idempotency, lineage_version)
            if any(ref not in claimed_refs
                   for ref, intent_record
                   in _rebalance._job_intent_items(lineage_intents,
                                                   lineage_version, job_id)):
                raise PermissionError("the job still holds an open "
                                      "migration reservation")
            settled_plan_keys = {
                record["plan_key"]
                for record in completed_settlements.get(job_id, ())
                if record["state"] in _rebalance._SETTLE_FINAL_STATES}
            for plan_key in job_migration_plans:
                if plan_key not in settled_plan_keys:
                    raise PermissionError("a terminal migration is not "
                                          "settled yet")

            # The completion moment must not precede the last relevant
            # execution or migration evidence: step receipts and
            # execution recoveries in the execution ledger, every
            # reservation, claim, migration step and recovery in the
            # intent ledger, and the completed settlement moments.
            evidence = 0
            for attempt_plan in exec_plans.get(job_id, {}).values():
                for receipt in attempt_plan["steps"]:
                    evidence = max(evidence, receipt["at"])
            for event in exec_events.values():
                request_event = event["request"]
                if request_event.get("job_id") == job_id \
                        and request_event.get("action") in ("record",
                                                            "recover"):
                    evidence = max(evidence, request_event["at"])
            # A dispatch decision can also reach succeeded/failed
            # through a direct finish outside execution_sync; that finish
            # moment is terminal evidence too.
            for event in dispatch_events.values():
                request_event = event["request"]
                if request_event.get("job_id") == job_id \
                        and request_event.get("action") == "finish":
                    evidence = max(evidence, request_event["at"])
            for event in lineage_events.values():
                if event["request"].get("job_id") == job_id:
                    evidence = max(evidence, event["request"]["at"])
            for settlement in completed_settlements.get(job_id, ()):
                evidence = max(evidence, settlement["at"])
            if at < evidence:
                raise ValueError("completion moment must not precede the "
                                 "last execution or migration evidence")

            # Freeze exactly what rebalance.current shows from this
            # snapshot: the highest-generation completed settlement's
            # after-binding, otherwise the immutable trade.
            latest_settled = _rebalance._latest_completed_binding(
                completed_settlements, job_id)
            if latest_settled is not None:
                generation = latest_settled["generation"]
                current = copy.deepcopy(latest_settled["after"])
            else:
                generation = 0
                current = {"resource_id": trade["resource_id"],
                           "version": trade["version"]}

            job = accepted[job_id]
            record: dict[str, Any] = {
                "job_id": job_id,
                "at": at,
                "outcome": outcome,
                "actual_cost": actual_cost,
                "actual_carbon": actual_carbon,
                "generation": generation,
                "current": current,
                "cost_exceeded": actual_cost > job["max_cost"],
                "carbon_exceeded": actual_carbon > job["carbon_cap"],
            }
            # Full independent validation against the snapshot before
            # the commit is accepted.
            _validate_record(record, accepted, history)
            records[key] = record
            idempotency[key] = dict(request)
            events[key] = {"key": key, "request": dict(request),
                           "result": copy.deepcopy(record)}
            _commit_file(completion_real,
                         _canonical_bytes(records, idempotency, events),
                         old_bytes)
            return copy.deepcopy(record), True


def get(completions: str, job_id: str) -> dict[str, object]:
    """Return the recorded completion for ``job_id``.

    ``completions`` and ``job_id`` must be non-empty strings, else
    ``ValueError``. The completion ledger is read under its shared lock
    and never modified. A job that was never completed -- an unknown job
    id, a job without a record or a missing completion ledger -- raises
    ``KeyError(job_id)``; invalid structure, ordering or non-canonical
    bytes raise ``ValueError``; other locking or I/O failures raise
    ``OSError``. The returned dict is a fresh copy in the record's
    fixed field order.
    """
    for value in (completions, job_id):
        if not isinstance(value, str) or not value:
            raise ValueError("completions and job_id must be non-empty "
                             "strings")

    realpath = os.path.realpath(completions)
    with _lock(realpath, shared=True):
        records, _idempotency, _events, _raw = _load_completion_ledger(
            realpath)
    record = _record_for_job(records, job_id)
    if record is None:
        raise KeyError(job_id)
    return copy.deepcopy(record)


def _validate_search_arguments(
    completions: str,
    cursor: str | None,
    limit: int,
    outcome: str | None,
    exceeded: str | None,
) -> None:
    if not isinstance(completions, str) or not completions:
        raise ValueError("completions must be a non-empty string")
    if cursor is not None and (not isinstance(cursor, str) or not cursor):
        raise ValueError("cursor must be None or a non-empty string")
    # bool is a subclass of int and must be rejected as a page size.
    if not isinstance(limit, int) or isinstance(limit, bool) \
            or not 1 <= limit <= _MAX_LIMIT:
        raise ValueError("limit must be an integer between 1 and 1000")
    if outcome is not None and outcome not in _OUTCOMES:
        raise ValueError("outcome filter must be succeeded or failed")
    if exceeded is not None and exceeded not in _EXCEEDED_FILTERS:
        raise ValueError("exceeded filter must be cost, carbon, any or "
                         "none")


def _matches(record: dict[str, Any], outcome: str | None,
             exceeded: str | None) -> bool:
    if outcome is not None and record["outcome"] != outcome:
        return False
    if exceeded == "cost":
        return record["cost_exceeded"]
    if exceeded == "carbon":
        return record["carbon_exceeded"]
    if exceeded == "any":
        return record["cost_exceeded"] or record["carbon_exceeded"]
    if exceeded == "none":
        return not record["cost_exceeded"] \
            and not record["carbon_exceeded"]
    return True


def _page(
    records: dict[str, dict[str, Any]],
    cursor: str | None,
    limit: int,
    outcome: str | None,
    exceeded: str | None,
) -> dict[str, Any]:
    # The scan order is the job id's code-point order, not the ledger's
    # idempotency-key order. The filters are applied first and the
    # exclusive cursor then selects from the filtered sequence; one
    # extra match is collected to learn whether the page is the last.
    ordered = sorted((record["job_id"], record)
                     for record in records.values())
    matches: list[dict[str, Any]] = []
    for job_id, record in ordered:
        if not _matches(record, outcome, exceeded):
            continue
        if cursor is not None and job_id <= cursor:
            continue
        matches.append({"job_id": job_id,
                        "record": copy.deepcopy(record)})
        if len(matches) > limit:
            break

    if len(matches) > limit:
        page = matches[:limit]
        next_cursor: str | None = page[-1]["job_id"]
    else:
        page = matches
        next_cursor = None
    return {"entries": page, "next": next_cursor}


def search(completions: str, cursor: str | None = None,
           limit: int = _DEFAULT_LIMIT, outcome: str | None = None,
           exceeded: str | None = None) -> dict[str, Any]:
    """Return one page of completion records matching the given filters.

    ``completions`` must be a non-empty string. ``cursor`` is ``None``
    or a non-empty string -- it need not name a recorded job -- and only
    records whose job id is strictly greater than it in code-point order
    are considered. ``limit`` is the page size: an integer from 1 to
    1000 (booleans are rejected), defaulting to 100. ``outcome`` is
    ``None``, ``"succeeded"`` or ``"failed"``; ``exceeded`` is ``None``
    or one of ``"cost"`` (the cost budget was exceeded), ``"carbon"``
    (the carbon budget was exceeded), ``"any"`` (at least one budget was
    exceeded) and ``"none"`` (neither was). Both filters combine by
    logical AND and the cursor selects from the filtered sequence. Any
    other value raises ``ValueError``.

    The records are scanned in ascending job-id code-point order.
    Returns ``{"entries": [{"job_id": ..., "record": ...}, ...],
    "next": cursor_or_none}``: each record is a fresh copy in the
    record's fixed field order, and ``next`` is the job id of the page's
    last entry when further matching records remain, else ``None``. With
    no matching records the page is empty and ``next`` is ``None``.

    The ledger is validated, filtered and paginated as one snapshot
    while its shared lock is held, and the query never writes. A missing
    completion ledger raises ``FileNotFoundError``; invalid structure,
    ordering or non-canonical bytes raise ``ValueError``; any other
    locking or I/O failure raises ``OSError``.
    """
    _validate_search_arguments(completions, cursor, limit, outcome,
                               exceeded)

    realpath = os.path.realpath(completions)
    with _lock(realpath, shared=True):
        records, _idempotency, _events, raw = _load_completion_ledger(
            realpath)
        if raw is None:
            raise FileNotFoundError(
                f"completion ledger {realpath!r} does not exist")
        return _page(records, cursor, limit, outcome, exceeded)


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 \
        and all(char in _HEX_DIGITS for char in value)


def _validate_condition(condition: str | None) -> None:
    if condition is not None and not _is_digest(condition):
        raise ValueError("condition must be None or a 64-digit lowercase "
                         "hexadecimal digest")


def _respond(payload: Any, condition: str | None
             ) -> tuple[bytes | None, str]:
    # Serialize, digest and compare while the caller holds the ledger's
    # shared lock: the tag summarizes exactly the bytes a 200 would
    # serve and both come from the same complete ledger version. The
    # body is the endpoint's compact UTF-8 JSON -- compact separators,
    # non-ASCII written through, no trailing newline.
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")
    etag = hashlib.sha256(body).hexdigest()
    if condition is not None and condition == etag:
        return None, etag
    return body, etag


def get_response(completions: str, job_id: str,
                 condition: str | None = None
                 ) -> tuple[bytes | None, str]:
    """Read one job's completion as a conditional, HTTP-ready response.

    ``completions`` and ``job_id`` follow :func:`get`; ``condition`` is
    ``None`` or a single 64-digit lowercase hexadecimal digest -- the
    tag a previous response carried, without its quotes. The ledger is
    read under its shared lock and the record object is formed,
    serialized to the response's compact UTF-8 JSON bytes and digested
    while the lock is held, so a completion racing the query is observed
    as either the complete old ledger or the complete new one and the
    tag always summarizes the bytes of one version.

    Returns ``(None, etag)`` when ``condition`` equals the current
    tag -- the endpoint answers 304 with an empty body and the same
    tag -- and ``(body, etag)`` otherwise. Unlike :func:`get`, a missing
    completion ledger raises ``FileNotFoundError`` so the endpoint can
    tell it apart from an unknown job, which raises ``KeyError``;
    invalid arguments or an invalid ledger raise ``ValueError`` and any
    other locking or I/O failure raises ``OSError``.
    """
    for value in (completions, job_id):
        if not isinstance(value, str) or not value:
            raise ValueError("completions and job_id must be non-empty "
                             "strings")
    _validate_condition(condition)

    realpath = os.path.realpath(completions)
    with _lock(realpath, shared=True):
        records, _idempotency, _events, raw = _load_completion_ledger(
            realpath)
        if raw is None:
            raise FileNotFoundError(
                f"completion ledger {realpath!r} does not exist")
        record = _record_for_job(records, job_id)
        if record is None:
            raise KeyError(job_id)
        return _respond(copy.deepcopy(record), condition)


def search_response(completions: str, cursor: str | None = None,
                    limit: int = _DEFAULT_LIMIT, outcome: str | None = None,
                    exceeded: str | None = None,
                    condition: str | None = None
                    ) -> tuple[bytes | None, str]:
    """Read one page as a conditional, HTTP-ready response.

    The filters follow :func:`search` and ``condition`` follows
    :func:`get_response`. The page object is formed, serialized and
    digested under the ledger's shared lock, so any visible record,
    order or cursor change changes the tag and a racing completion is
    observed as one complete ledger version. Returns ``(None, etag)``
    on a conditional hit and ``(body, etag)`` otherwise; the error
    mapping is :func:`search`'s.
    """
    _validate_search_arguments(completions, cursor, limit, outcome,
                               exceeded)
    _validate_condition(condition)

    realpath = os.path.realpath(completions)
    with _lock(realpath, shared=True):
        records, _idempotency, _events, raw = _load_completion_ledger(
            realpath)
        if raw is None:
            raise FileNotFoundError(
                f"completion ledger {realpath!r} does not exist")
        return _respond(_page(records, cursor, limit, outcome, exceeded),
                        condition)
