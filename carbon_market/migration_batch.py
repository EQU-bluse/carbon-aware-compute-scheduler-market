"""Persistent cross-job migration batches.

The single-job layers already cover one booking end to end -- the trade,
dispatch, a launch plan, a migration advice with its reservation, claim,
receipts, recovery and a terminal settlement fixing the job's current
binding -- but nothing coordinates those steps across jobs. This module
adds the coordination: one persistent migration batch takes one complete
snapshot of the nine business ledgers, fixes its members, drives each
member's ``keep``/``migrate`` round with optional per-job receipts,
survives a crash mid-batch and settles every terminal migration before
the batch closes.

:func:`run` is the only write entry point. The first call for a batch
key fixes the members forever: every accepted job that already holds a
trade, whose booking has not already run to completion and which no
other still-open batch drives, ordered by job id.
The judgment is rooted at the latest completed binding
``rebalance.current`` returns (the immutable trade when there is none):

* a job whose booking already completed through the baseline layers --
  a succeeded dispatch or a completed execution -- is done and never
  becomes a member;
* a job with no current-round migration evidence whose booking is in
  flight through the baseline layers, or whose deadline has already
  passed, simply stays on its current binding (``keep``);
* otherwise a fresh advice is evaluated at the batch moment; a ``keep``
  advice completes directly and a ``migrate`` advice is reserved and
  claimed under stable request keys;
* an unclaimed reservation rooted at the current binding is claimed;
* an active plan receives the receipt that names its next pending step,
  waits while the plan lease is valid without such a receipt, and is
  recovered once the lease has strictly expired; a receipt exactly
  matching an already-recorded step simply returns the current
  progress, while the same step with a changed result, credential or
  moment raises ``ValueError`` before a byte is written;
* a ``migrated``, ``failed`` or ``interrupted`` plan rooted at the
  current binding enters settlement.

Every downstream action (``evaluate``, ``apply``, ``start``, ``record``,
``recover``, ``settle``) and its full parameters are persisted under a
stable request key *before* the existing interface is called, and the
item only advances after success, so a crashed call replays the very
same request and a completed phase is never redone; a persisted request
always belongs to the generation it rooted and receipts, recovery and
settlement name that plan explicitly, never a later generation. One
member's business exception saves the public exception class and fails
just that member, while the other members continue.

An active batch may only be continued by its current owner; another
owner may take over only after the coordination lease has strictly
expired. The coordination ledger (version 1, sections ``version``,
``batches`` and an ``audit`` appended in real completion order, never
re-sorted) is one compact UTF-8 JSON
document with non-ASCII written through, decimal integers and exactly
one trailing newline.
"""

from __future__ import annotations

import contextlib
import copy
import fcntl
import json
import os
import tempfile
import threading
from typing import Any, Callable, Iterator

from . import rebalance as _rebalance
from ._jsonio import finite_loads

__all__ = ["run", "get"]

_VERSION = 1
_ROOT_FIELDS = ("version", "batches", "audit")
_BATCH_FIELDS = ("key", "owner", "until", "status", "inputs", "items")
_ITEM_FIELDS = ("job_id", "plan_key", "phase", "request_key", "lease",
                "error", "snapshot")
_EVENT_FIELDS = ("key", "at", "batch")
# Field order of the recorded input map; the values are resolved real
# paths, so one batch permanently names one exact business ledger set.
_INPUT_NAMES = ("advice", "dispatch", "execution", "jobs", "ledger",
                "settlements", "signals", "supply", "trades")
_BATCH_STATUSES = ("pending", "completed")
# Coarse item stage. evaluating/reserved/claimed persist the next pending
# downstream call as a marker snapshot; active drives a claimed plan with
# receipts, waiting or recovery; settling persists the settlement call;
# kept/settled/failed are terminal.
_ITEM_PHASES = ("kept", "evaluating", "reserved", "claimed", "active",
                "settling", "settled", "failed")
_ITEM_TERMINAL = ("kept", "settled", "failed")
_MARKER_ACTIONS = ("evaluate", "apply", "start", "record", "recover",
                   "settle")
_PLAN_TERMINAL = ("migrated", "failed", "interrupted")
_STEPS = ("copy", "switch")
_LOCK_SUFFIX = ".lock"
_PREFIX = ".migration-batch-"


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
    # and an flock is released by the kernel on process exit, so equal
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
                dir=directory, prefix=_PREFIX + "restore-", suffix=".tmp")
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
    # One durable commit through a synced same-directory temporary file,
    # an atomic replace and a directory fsync, restoring the pre-call
    # bytes (and clearing the leftover fragment) on any failure.
    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=_PREFIX, suffix=".tmp")
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
# Stable request keys
# ---------------------------------------------------------------------------


def _stable_key(batch_key: str, job_id: str, tag: str) -> str:
    # Stable from the batch key, the member and the phase; the length
    # prefix makes the join unambiguous. Every downstream request is
    # replayed under exactly this key, so a crashed call never starts a
    # second evaluate, reservation, claim, receipt, recovery or
    # settlement.
    return f"{len(batch_key)}:{batch_key}{job_id}\t{tag}"


def _plan_key_for(batch_key: str, job_id: str) -> str:
    # The migration plan's own start key for a plan this batch begins;
    # it binds every later receipt, recovery and settlement to this one
    # generation so a pending request can never cross into a later plan.
    return _stable_key(batch_key, job_id, "start")


def _settle_key_for(plan_key: str) -> str:
    # The settlement key derives from the plan's start key alone, so a
    # crash between settle's pending and final writes is replayed under
    # the same key even by a later batch that picks the plan up.
    return f"{len(plan_key)}:settle\t{plan_key}"


# ---------------------------------------------------------------------------
# Snapshot over the nine business ledgers
# ---------------------------------------------------------------------------


class _Snapshot:
    """One validated read of the nine business ledgers."""

    def __init__(self, reals: dict[str, str]) -> None:
        self.reals = reals
        # The seven baseline layers are read exactly as
        # rebalance._load_snapshot_layers does, except that a missing
        # advice ledger is an empty one: the first member that judges a
        # round creates it through rebalance.evaluate.
        accepted, _job_map, _job_events, job_raw = \
            _rebalance._jobs._load_submit_file(reals["jobs"])
        if job_raw is None:
            raise FileNotFoundError(
                f"acceptance file {reals['jobs']!r} does not exist")
        if job_raw != _rebalance._jobs._serialize_submit_file(
                accepted, _job_map, _job_events):
            raise ValueError(
                f"acceptance file {reals['jobs']!r} is not in canonical "
                "compact form")
        history, _supply_map, _supply_events, supply_raw = \
            _rebalance._resources._load_file(reals["supply"])
        if supply_raw is None:
            raise FileNotFoundError(
                f"supply file {reals['supply']!r} does not exist")
        signal_history, _signal_map, _signal_events, signal_raw = \
            _rebalance._signals._load_file(reals["signals"])
        if signal_raw is None:
            raise FileNotFoundError(
                f"signal file {reals['signals']!r} does not exist")
        cleared, _clear_keys, trades_raw = _rebalance._market.\
            _load_clear_ledger(
                reals["trades"], accepted, history, signal_history)
        if trades_raw is None:
            raise FileNotFoundError(
                f"clearing ledger {reals['trades']!r} does not exist")
        decisions, _dispatch_keys, _dispatch_events, dispatch_raw = \
            _rebalance._dispatch._load_ledger(reals["dispatch"])
        if dispatch_raw is None:
            raise FileNotFoundError(
                f"dispatch ledger {reals['dispatch']!r} does not exist")
        self.exec_plans, _plan_keys, _plan_events = \
            _rebalance._execution._load_existing_ledger(
                reals["execution"])[:3]
        advice_records, _advice_events, _advice_raw = \
            _rebalance._load_ledger(
                reals["advice"], accepted, cleared, history,
                signal_history, defer_current=True)
        self.accepted = accepted
        self.history = history
        self.signal_history = signal_history
        self.cleared = cleared
        self.decisions = decisions
        self.advice_records = advice_records
        intents, plans, idempotency, events, version, intent_raw = \
            _rebalance._load_intent_ledger(
                reals["ledger"], self.accepted, self.cleared, self.history,
                self.signal_history, self.advice_records)
        if intent_raw is not None \
                and version != _rebalance._INTENT_VERSION_V3:
            # Only the history layout keeps every generation apart; a
            # legacy ledger is upgraded in memory for reading and is
            # never rewritten by this module. A missing ledger is simply
            # empty: the first downstream apply creates it.
            intents, plans, idempotency, events = \
                _rebalance._upgrade_intent_ledger_v3(
                    intents, plans, idempotency, events)
        self.intents = intents
        self.plans = plans
        self.idempotency = idempotency
        self.events = events
        records, settle_bindings, _events, _settle_raw = _rebalance.\
            _load_settlement_ledger(
                reals["settlements"], self.accepted, self.cleared, plans,
                idempotency, events, _rebalance._INTENT_VERSION_V3)
        # A missing settlement ledger is simply empty: the first member
        # that reaches settlement creates it through rebalance.settle.
        self.settlements = records
        self.settle_bindings = settle_bindings
        self.completed, self.pending_jobs = \
            _rebalance._lineage_from_settlement_records(records)
        for advice_record in self.advice_records.values():
            _rebalance._ground_advice_current(
                advice_record, self.cleared,
                _rebalance._settlement_currents(self.completed))

    @property
    def input_map(self) -> dict[str, str]:
        return {name: self.reals[name] for name in _INPUT_NAMES}

    @property
    def input_paths(self) -> list[str]:
        return [self.reals[name] for name in _INPUT_NAMES]

    def current_binding(self, job_id: str) -> dict[str, Any]:
        # The judgment source is current: the latest completed binding
        # or the immutable trade's frozen version as generation 0.
        latest = _rebalance._latest_completed_binding(
            self.completed, job_id)
        if latest is not None:
            return copy.deepcopy(latest["after"])
        trade = self.cleared[job_id]
        return {"resource_id": trade["resource_id"],
                "version": trade["version"]}

    def rooted_plans(self, job_id: str,
                     binding: dict[str, Any]) -> list[tuple[str,
                                                            dict[str, Any]]]:
        return sorted(((start_key, plan) for start_key, plan
                       in self.plans.items()
                       if plan["job_id"] == job_id
                       and plan["source"] == binding),
                      key=lambda entry: (entry[1]["at"], entry[0]))

    def unclaimed_rooted_intent(
        self, job_id: str, binding: dict[str, Any],
    ) -> tuple[str, dict[str, Any]] | None:
        # The newest reservation rooted at the current binding that no
        # start request has claimed yet.
        claimed = _rebalance._claimed_intent_refs(
            self.plans, self.idempotency, _rebalance._INTENT_VERSION_V3)
        candidates = [(ref, intent) for ref, intent in self.intents.items()
                      if ref not in claimed and intent["job_id"] == job_id
                      and intent["source"] == binding]
        if not candidates:
            return None
        return max(candidates, key=lambda entry: (entry[1]["at"], entry[0]))

    def terminal_moment(self, plan: dict[str, Any],
                        plan_key: str) -> int:
        return _rebalance._terminal_plan_at(
            plan, self.events, plan_key, _rebalance._INTENT_VERSION_V3)

    def settle_request(
        self, job_id: str, plan_key: str, at: int, fallback_key: str,
    ) -> tuple[str, int]:
        # When a previous crashed run already bound a settlement request
        # to this terminal plan, re-entry (even from a later batch)
        # replays that same key and moment; otherwise the caller's
        # stable key is used for the first settlement.
        for existing_key, request in self.settle_bindings.items():
            if request["job_id"] == job_id \
                    and request["plan_key"] == plan_key:
                return existing_key, request["at"]
        return fallback_key, at


# ---------------------------------------------------------------------------
# Pending-action markers
# ---------------------------------------------------------------------------


_MARKER_REQUEST_FIELDS = {
    "evaluate": ("at",),
    "apply": ("advice_key", "at"),
    "start": ("owner", "lease_end", "at"),
    "record": ("owner", "step", "result", "receipt", "at"),
    "recover": ("owner", "at"),
    "settle": ("plan_key", "at"),
}


def _marker(action: str, **params: Any) -> dict[str, Any]:
    # Persisted before the downstream interface is called: the action
    # name and the exact parameters the crash re-entry must replay.
    return {"action": action,
            "request": {field: params[field]
                        for field in _MARKER_REQUEST_FIELDS[action]}}


def _is_marker(value: object) -> bool:
    return isinstance(value, dict) \
        and set(value.keys()) == {"action", "request"} \
        and isinstance(value.get("action"), str) \
        and isinstance(value.get("request"), dict)


def _validate_marker(raw: object) -> dict[str, Any]:
    if not _is_marker(raw):
        raise ValueError("migration batch item snapshot must be a result "
                         "record or a pending-action marker")
    action = raw["action"]  # type: ignore[index]
    if action not in _MARKER_ACTIONS:
        raise ValueError("migration batch marker action is invalid")
    request = raw["request"]  # type: ignore[index]
    fields = _MARKER_REQUEST_FIELDS[action]
    if set(request.keys()) != set(fields):
        raise ValueError("migration batch marker request has invalid "
                         "fields")
    for field in ("owner", "advice_key", "receipt", "step", "plan_key"):
        if field in request and (not isinstance(request[field], str)
                                 or not request[field]):
            raise ValueError(f"marker request {field} must be a non-empty "
                             "string")
    if "step" in request and request["step"] not in _STEPS:
        raise ValueError("marker step must be copy or switch")
    if "result" in request \
            and request["result"] not in ("succeeded", "failed"):
        raise ValueError("marker result must be succeeded or failed")
    for field, minimum in (("at", 0), ("lease_end", 1)):
        if field in request:
            value = request[field]
            if not _is_plain_int(value) or value < minimum:
                raise ValueError(f"marker {field} is invalid")
    return {"action": action, "request": dict(request)}


# ---------------------------------------------------------------------------
# Coordination ledger validation and canonical form
# ---------------------------------------------------------------------------


def _validate_inputs(raw: object) -> dict[str, str]:
    if not isinstance(raw, dict) or set(raw.keys()) != set(_INPUT_NAMES):
        raise ValueError("batch inputs must name the nine business "
                         "ledgers")
    paths: dict[str, str] = {}
    for name in _INPUT_NAMES:
        value = raw[name]
        if not isinstance(value, str) or not value:
            raise ValueError("batch input paths must be non-empty "
                             "strings")
        paths[name] = value
    if len(set(paths.values())) != 9:
        raise ValueError("batch input paths must be distinct")
    return paths


def _validate_item(
    raw: object,
    job_id: object,
    snapshot: _Snapshot,
) -> dict[str, Any]:
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("batch item keys must be non-empty strings")
    if not isinstance(raw, dict) or set(raw.keys()) != set(_ITEM_FIELDS):
        raise ValueError("migration batch item has invalid fields")
    if raw["job_id"] != job_id:
        raise ValueError("migration batch item key must match its job id")
    plan_key = raw["plan_key"]
    if plan_key is not None and (not isinstance(plan_key, str)
                                 or not plan_key):
        raise ValueError("migration batch item plan_key must be null or a "
                         "non-empty string")
    phase = raw["phase"]
    if phase not in _ITEM_PHASES:
        raise ValueError("migration batch item phase is invalid")
    request_key = raw["request_key"]
    if not isinstance(request_key, str) or not request_key:
        raise ValueError("migration batch item request_key must be a "
                         "non-empty string")
    lease = raw["lease"]
    if lease is not None and (not _is_plain_int(lease) or lease < 1):
        raise ValueError("migration batch item lease must be null or a "
                         "positive integer")
    error = raw["error"]
    if error is not None and (not isinstance(error, str) or not error):
        raise ValueError("migration batch item error must be null or a "
                         "non-empty string")
    item_snapshot = raw["snapshot"]

    if job_id not in snapshot.accepted:
        raise ValueError("migration batch item must reference an accepted "
                         "job")
    if snapshot.cleared.get(job_id) is None:
        raise ValueError("migration batch item must reference a recorded "
                         "trade")
    binding_choices = [{"resource_id": snapshot.cleared[job_id]["resource_id"],
                        "version": snapshot.cleared[job_id]["version"]}]
    binding_choices.extend(record["after"] for record
                          in snapshot.completed.get(job_id, ()))

    if phase == "kept":
        if plan_key is not None or lease is not None or error is not None:
            raise ValueError("a kept item must not carry plan, lease or "
                             "error data")
        if item_snapshot not in binding_choices:
            raise ValueError("a kept item snapshot must be a current "
                             "binding")
    elif phase == "failed":
        if error is None:
            raise ValueError("a failed item must carry the public error "
                             "class name")
        if item_snapshot is not None:
            raise ValueError("a failed item must not carry a snapshot")
    else:
        if plan_key is None or error is not None:
            raise ValueError("an active migration item must name its "
                             "plan key and carry no error")
        if _is_marker(item_snapshot):
            marker = _validate_marker(item_snapshot)
            action = marker["action"]
            # The persisted action must match the stage that waits on
            # it; a mismatched marker is a broken reference just like a
            # missing plan or advice record.
            expected_actions = {
                "evaluating": ("evaluate",),
                "reserved": ("apply",),
                "claimed": ("start",),
                "active": ("record", "recover"),
                "settling": ("settle",),
            }[phase]
            if action not in expected_actions:
                raise ValueError("migration batch item marker action does "
                                 "not match its phase")
            if action == "apply":
                advice = snapshot.advice_records.get(
                    marker["request"]["advice_key"])
                if advice is None or advice["job_id"] != job_id \
                        or advice["recommendation"] != "migrate":
                    raise ValueError("apply marker must reference a "
                                     "recorded migrate advice for the job")
            elif action == "start":
                if lease != marker["request"]["lease_end"]:
                    raise ValueError("start marker lease must match the "
                                     "item lease")
            elif action in ("record", "recover"):
                plan = snapshot.plans.get(plan_key)
                if plan is None or plan["job_id"] != job_id:
                    raise ValueError("record/recover marker must reference "
                                     "a recorded migration plan")
            elif action == "settle":
                if marker["request"]["plan_key"] != plan_key:
                    raise ValueError("settle marker must name the item's "
                                     "plan")
                settled_already = any(
                    record["job_id"] == job_id
                    and record["plan_key"] == plan_key
                    for record in snapshot.settlements.values())
                plan = snapshot.plans.get(plan_key)
                if not settled_already and (
                        plan is None or plan["job_id"] != job_id
                        or plan["state"] not in _PLAN_TERMINAL):
                    raise ValueError("settle marker must reference a "
                                     "terminal migration plan")
        elif phase == "settled":
            settled = next((record for record in snapshot.settlements.values()
                            if record["job_id"] == job_id
                            and record["plan_key"] == plan_key), None)
            if settled is None or item_snapshot != settled:
                raise ValueError("a settled item must snapshot its "
                                 "settlement record")
        else:
            if phase != "active":
                raise ValueError(f"a {phase} item must carry a pending-"
                                 "action marker")
            plan = snapshot.plans.get(plan_key)
            if plan is None or plan["job_id"] != job_id:
                raise ValueError("migration batch item must reference a "
                                 "recorded migration plan")
            if item_snapshot != plan:
                raise ValueError("migration batch item snapshot must be "
                                 "the current plan or a pending marker")
            if lease != plan["lease_end"]:
                raise ValueError("migration batch item lease must be the "
                                 "plan lease end")

    return {
        "job_id": job_id,
        "plan_key": plan_key,
        "phase": phase,
        "request_key": request_key,
        "lease": lease,
        "error": error,
        "snapshot": copy.deepcopy(item_snapshot),
    }


def _validate_ledger(
    data: object,
    snapshots: dict[frozenset[tuple[str, str]], _Snapshot],
    default_inputs: dict[str, str] | None,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]],
           dict[str, str]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("migration coordination ledger root must be an "
                         "object with keys version, batches and audit")
    if not _is_plain_int(data["version"]) or data["version"] != _VERSION:
        raise ValueError("unsupported migration coordination ledger "
                         "version")
    batches_raw = data["batches"]
    audit_raw = data["audit"]
    if not isinstance(batches_raw, dict) or not isinstance(audit_raw, list):
        raise ValueError("batches must be an object and audit a list")
    _check_sorted_keys(batches_raw, "batches")

    batches: dict[str, dict[str, Any]] = {}
    completed: set[str] = set()
    ledger_inputs: dict[str, str] | None = None
    for batch_key, batch_raw in batches_raw.items():
        if not isinstance(batch_key, str) or not batch_key:
            raise ValueError("batch keys must be non-empty strings")
        if not isinstance(batch_raw, dict) \
                or set(batch_raw.keys()) != set(_BATCH_FIELDS):
            raise ValueError("migration batch has invalid fields")
        if batch_raw["key"] != batch_key:
            raise ValueError("batch key does not match its map key")
        owner = batch_raw["owner"]
        if not isinstance(owner, str) or not owner:
            raise ValueError("batch owner must be a non-empty string")
        until = batch_raw["until"]
        if not _is_plain_int(until) or until < 1:
            raise ValueError("batch until must be a positive integer")
        status = batch_raw["status"]
        if status not in _BATCH_STATUSES:
            raise ValueError("batch status is invalid")
        inputs = _validate_inputs(batch_raw["inputs"])
        if ledger_inputs is None:
            ledger_inputs = inputs
        elif inputs != ledger_inputs:
            raise ValueError("batches in one coordination ledger must "
                             "share one business ledger set")
        items_raw = batch_raw["items"]
        if not isinstance(items_raw, dict):
            raise ValueError("batch items must be an object")
        _check_sorted_keys(items_raw, "batch items")

        snapshot = snapshots.get(frozenset(inputs.items()))
        if snapshot is None:
            raise ValueError("batch inputs do not name the loaded "
                             "business snapshot")
        items: dict[str, dict[str, Any]] = {}
        for member_key, item_raw in items_raw.items():
            item = _validate_item(item_raw, member_key, snapshot)
            items[member_key] = item
        if status == "completed":
            if any(item["phase"] not in _ITEM_TERMINAL
                   for item in items.values()):
                raise ValueError("a completed batch may not hold active "
                                 "items")
            completed.add(batch_key)
        batches[batch_key] = {
            "key": batch_key,
            "owner": owner,
            "until": until,
            "status": status,
            "inputs": inputs,
            "items": items,
        }

    audit: list[dict[str, Any]] = []
    seen_events: set[str] = set()
    for event_raw in audit_raw:
        if not isinstance(event_raw, dict) \
                or set(event_raw.keys()) != set(_EVENT_FIELDS):
            raise ValueError("migration audit event has invalid fields")
        event_key = event_raw["key"]
        if not isinstance(event_key, str) or not event_key:
            raise ValueError("migration audit event key must be a "
                             "non-empty string")
        if event_key in seen_events:
            # The audit is append-only in real completion order:
            # duplicated events are illegal, but the entry order is the
            # completion order and is never re-sorted by batch key.
            raise ValueError("migration audit keys must be unique")
        seen_events.add(event_key)
        at = event_raw["at"]
        if not _is_plain_int(at) or at < 0:
            raise ValueError("migration audit moment must be a non-"
                             "boolean non-negative integer")
        batch = batches.get(event_key)
        if batch is None or batch["status"] != "completed":
            raise ValueError("migration audit event must reference a "
                             "completed batch")
        if event_raw["batch"] != batch:
            raise ValueError("migration audit event batch does not match "
                             "its batch")
        audit.append({"key": event_key, "at": at,
                      "batch": copy.deepcopy(batch)})

    # Exactly one closing event per completed batch, none for a batch
    # still running.
    if seen_events != completed:
        raise ValueError("migration audit events must match the completed "
                         "batches")
    if ledger_inputs is None:
        if default_inputs is None:
            raise ValueError("a migration coordination ledger must hold "
                             "at least one batch")
        ledger_inputs = default_inputs
    return batches, audit, ledger_inputs


def _canonical_item(item: dict[str, Any]) -> dict[str, Any]:
    return {field: copy.deepcopy(item[field]) for field in _ITEM_FIELDS}


def _canonical_batch(batch: dict[str, Any]) -> dict[str, Any]:
    return {
        "key": batch["key"],
        "owner": batch["owner"],
        "until": batch["until"],
        "status": batch["status"],
        "inputs": {name: batch["inputs"][name] for name in _INPUT_NAMES},
        "items": {job_id: _canonical_item(batch["items"][job_id])
                  for job_id in sorted(batch["items"])},
    }


def _canonical_bytes(
    batches: dict[str, dict[str, Any]],
    audit: list[dict[str, Any]],
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, sections in their
    # fixed field order, batches and their items code-point sorted and
    # the audit kept in its append (real completion) order, terminated
    # by exactly one newline.
    payload = {
        "version": _VERSION,
        "batches": {key: _canonical_batch(batches[key])
                    for key in sorted(batches)},
        "audit": [{"key": event["key"], "at": event["at"],
                   "batch": _canonical_batch(event["batch"])}
                  for event in audit],
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_coordination(
    realpath: str,
    snapshots: dict[frozenset[tuple[str, str]], _Snapshot],
    default_inputs: dict[str, str] | None = None,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]],
           dict[str, str], bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        if default_inputs is None:
            raise
        return {}, [], default_inputs, None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"migration coordination ledger {realpath!r} is not valid "
            "UTF-8") from exc
    try:
        # Negative-zero and non-finite literals are format errors.
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"migration coordination ledger {realpath!r} is not valid "
            "JSON") from exc
    batches, audit, inputs = _validate_ledger(data, snapshots,
                                              default_inputs)
    if raw != _canonical_bytes(batches, audit):
        raise ValueError(
            f"migration coordination ledger {realpath!r} is not in "
            "canonical compact form")
    return batches, audit, inputs, raw


# ---------------------------------------------------------------------------
# Receipts
# ---------------------------------------------------------------------------


def _validate_receipts(receipts: object) -> dict[str, dict[str, Any]]:
    if receipts is None:
        return {}
    if not isinstance(receipts, dict):
        raise ValueError("receipts must be an object keyed by job id")
    checked: dict[str, dict[str, Any]] = {}
    for job_id, raw in receipts.items():
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("receipt job ids must be non-empty strings")
        if not isinstance(raw, dict) \
                or set(raw.keys()) != {"step", "result", "receipt", "at"}:
            raise ValueError("receipt must carry step, result, receipt and "
                             "at")
        step = raw["step"]
        if step not in _STEPS:
            raise ValueError("receipt step must be copy or switch")
        result = raw["result"]
        if result not in ("succeeded", "failed"):
            raise ValueError("receipt result must be succeeded or failed")
        receipt = raw["receipt"]
        if not isinstance(receipt, str) or not receipt:
            raise ValueError("receipt text must be a non-empty string")
        at = raw["at"]
        if not _is_plain_int(at) or at < 0:
            raise ValueError("receipt at must be a non-boolean non-"
                             "negative integer")
        checked[job_id] = {"step": step, "result": result,
                           "receipt": receipt, "at": at}
    return checked


def _recorded_step(plan: dict[str, Any],
                   receipt: dict[str, Any]) -> dict[str, Any] | None:
    # The plan step this receipt names, if the plan already recorded one.
    return next((step for step in plan["steps"]
                 if step["step"] == receipt["step"]), None)


def _check_receipt_against_plan(plan: dict[str, Any],
                                receipt: dict[str, Any]) -> None:
    # A receipt naming a step the plan already recorded is only legal as
    # an exact replay; any change of the result, the credential or the
    # moment is a changed request, refused before a byte is written.
    recorded = _recorded_step(plan, receipt)
    if recorded is not None and recorded != {
            "step": receipt["step"],
            "result": receipt["result"],
            "receipt": receipt["receipt"],
            "at": receipt["at"]}:
        raise ValueError("a receipt already recorded for this step "
                         "cannot change")


# ---------------------------------------------------------------------------
# Member construction
# ---------------------------------------------------------------------------


def _lease_end_for(job: dict[str, Any], at: int, lease: int) -> int:
    # A plan this batch starts takes one coordination-lease window,
    # capped at the job deadline the claim may not pass.
    return min(at + lease, job["deadline"])


def _build_members(
    snapshot: _Snapshot,
    batch_key: str,
    owner: str,
    now: int,
    lease: int,
    excluded: set[str],
) -> list[dict[str, Any]]:
    # Fix the membership from one complete snapshot: accepted jobs with
    # a trade whose booking has not already completed, minus jobs
    # another still-open batch already drives, ordered by job id. The
    # stage each member starts in is fixed here; only its downstream
    # requests advance later.
    members: list[dict[str, Any]] = []
    for job_id in sorted(snapshot.accepted):
        if snapshot.cleared.get(job_id) is None or job_id in excluded:
            continue
        decision = snapshot.decisions.get(job_id)
        exec_states = {plan["state"]
                       for plan in snapshot.exec_plans.get(job_id,
                                                           {}).values()}
        if decision is not None and decision["state"] == "succeeded" \
                or "completed" in exec_states:
            # The booking already ran to completion through the
            # baseline layers: the job is done and never becomes a
            # member.
            continue
        binding = snapshot.current_binding(job_id)
        own_plan_key = _plan_key_for(batch_key, job_id)
        rooted = snapshot.rooted_plans(job_id, binding)
        active = next((plan for _ref, plan in rooted
                       if plan["state"] == "active"), None)
        if active is not None:
            # A migration round for the current binding is already in
            # flight: drive it with receipts, waiting or recovery.
            active_ref = next(ref for ref, plan in rooted if plan is active)
            members.append({
                "job_id": job_id,
                "plan_key": active_ref,
                "phase": "active",
                "request_key": active_ref,
                "lease": active["lease_end"],
                "error": None,
                "snapshot": copy.deepcopy(active),
            })
            continue
        terminal = next((plan for _ref, plan in reversed(rooted)
                         if plan["state"] in _PLAN_TERMINAL
                         and not any(record["plan_key"] == _ref
                                     for record in
                                     snapshot.settlements.values()
                                     if record["job_id"] == job_id)), None)
        if terminal is not None:
            # A terminal plan rooted at the current binding has not been
            # settled: enter settlement straight away; reservation and
            # claim are history and are never repeated.
            terminal_ref = next(
                ref for ref, plan in rooted
                if plan is terminal and not any(
                    record["job_id"] == job_id
                    and record["plan_key"] == ref
                    for record in snapshot.settlements.values()))
            settle_at = max(now, snapshot.terminal_moment(terminal,
                                                          terminal_ref))
            settle_key, settle_at = snapshot.settle_request(
                job_id, terminal_ref, settle_at,
                _settle_key_for(terminal_ref))
            members.append({
                "job_id": job_id,
                "plan_key": terminal_ref,
                "phase": "settling",
                "request_key": settle_key,
                "lease": terminal["lease_end"],
                "error": None,
                "snapshot": _marker("settle", plan_key=terminal_ref,
                                    at=settle_at),
            })
            continue
        unclaimed = snapshot.unclaimed_rooted_intent(job_id, binding)
        if unclaimed is not None:
            # A current-round reservation exists but has not been
            # claimed: claim it under the batch owner for one lease
            # window.
            _ref, intent = unclaimed
            lease_end = _lease_end_for(snapshot.accepted[job_id], now, lease)
            members.append({
                "job_id": job_id,
                "plan_key": own_plan_key,
                "phase": "claimed",
                "request_key": own_plan_key,
                "lease": lease_end,
                "error": None,
                "snapshot": _marker("start", owner=owner, lease_end=lease_end,
                                    at=now),
            })
            continue
        if decision is not None and (
                decision["state"] == "claimed"
                or exec_states & {"active"}) \
                or now > snapshot.accepted[job_id]["deadline"]:
            # No current migration round and the booking itself is in
            # flight through the baseline layers, or the deadline has
            # already passed so no migration is possible: the job
            # simply stays on its current binding.
            members.append({
                "job_id": job_id,
                "plan_key": None,
                "phase": "kept",
                "request_key": _stable_key(batch_key, job_id, "keep"),
                "lease": None,
                "error": None,
                "snapshot": binding,
            })
            continue
        # Otherwise the batch judges the round itself: the evaluate
        # call persisted below decides keep or migrate at the batch
        # moment, rooted at the latest completed binding.
        members.append({
            "job_id": job_id,
            "plan_key": own_plan_key,
            "phase": "evaluating",
            "request_key": _stable_key(batch_key, job_id, "evaluate"),
            "lease": None,
            "error": None,
            "snapshot": _marker("evaluate", at=now),
        })
    return members


def _batch_snapshot(batch: dict[str, Any]) -> dict[str, Any]:
    # A fixed-order deep copy the caller can never mutate.
    return {
        "key": batch["key"],
        "owner": batch["owner"],
        "until": batch["until"],
        "status": batch["status"],
        "inputs": {name: batch["inputs"][name] for name in _INPUT_NAMES},
        "items": {job_id: _canonical_item(batch["items"][job_id])
                  for job_id in sorted(batch["items"])},
    }


def _is_business_error(exc: BaseException) -> bool:
    # The downstream interfaces' refusal vocabulary is business state
    # and fails just the member; locking and I/O failures surface as
    # OSError and abort the whole run.
    return not isinstance(exc, OSError) and isinstance(
        exc, (ValueError, KeyError, LookupError, PermissionError,
              TimeoutError))


# ---------------------------------------------------------------------------
# Write entry point
# ---------------------------------------------------------------------------


def run(
    jobs: str,
    supply: str,
    signals: str,
    trades: str,
    dispatch: str,
    execution: str,
    advice: str,
    ledger: str,
    settlements: str,
    coordination: str,
    owner: str,
    key: str,
    now: int,
    lease: int,
    receipts: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, object], bool]:
    """Coordinate one persistent batch of migrations idempotently.

    The nine business ledger paths (acceptance, supply, signals,
    clearing, dispatch, execution, advice, migration intent and
    settlement), the coordination ledger path, ``owner`` and ``key``
    must be non-empty strings and the ten paths must resolve to distinct
    real locations; ``now`` must be a non-boolean non-negative integer
    moment and ``lease`` a non-boolean positive integer coordination
    lease (also the claim-lease window of plans this batch starts,
    capped at the job deadline). ``receipts`` is an optional job-keyed
    map; each receipt carries ``step`` (``copy`` or ``switch``),
    ``result`` (``succeeded`` or ``failed``), a non-empty ``receipt``
    string and a non-negative ``at`` moment. Any violation raises
    ``ValueError`` before a business file is read.

    The first call for ``key`` fixes the members from one complete
    snapshot -- accepted jobs holding a trade whose booking has not
    already completed and which no other still-open batch drives -- and
    persists them in job-id order; an empty scan still
    commits a completed batch with its audit event. Each downstream
    action and its parameters are durably saved before the call and the
    member advances only on success, so crash re-entry replays the
    original request and a completed phase is never redone. A member's
    business exception saves the public exception class and fails that
    member; the other members continue. The same key with a changed
    ledger set raises ``ValueError`` and an active batch owned by another
    caller raises ``PermissionError`` until the lease has strictly
    expired.

    Returns ``(batch, created)`` -- a fixed-order copy and the
    first-creation flag. Missing input files or the coordination ledger
    parent raise ``FileNotFoundError``; invalid arguments, receipts,
    structure, ordering, references or non-canonical bytes raise
    ``ValueError``; other locking or I/O failures raise ``OSError``.
    """
    for value in (jobs, supply, signals, trades, dispatch, execution,
                  advice, ledger, settlements, coordination, owner, key):
        if not isinstance(value, str) or not value:
            raise ValueError("the ten paths, owner and key must be "
                             "non-empty strings")
    if not _is_plain_int(now) or now < 0:
        raise ValueError("now must be a non-boolean non-negative integer")
    if not _is_plain_int(lease) or lease < 1:
        raise ValueError("lease must be a non-boolean positive integer")
    checked_receipts = _validate_receipts(receipts)

    reals = {
        "jobs": os.path.realpath(jobs),
        "supply": os.path.realpath(supply),
        "signals": os.path.realpath(signals),
        "trades": os.path.realpath(trades),
        "dispatch": os.path.realpath(dispatch),
        "execution": os.path.realpath(execution),
        "advice": os.path.realpath(advice),
        "ledger": os.path.realpath(ledger),
        "settlements": os.path.realpath(settlements),
        "coordination": os.path.realpath(coordination),
    }
    if len(set(reals.values())) != 10:
        raise ValueError("the ten paths must be distinct real paths")
    coordination_real = reals["coordination"]
    paths7 = tuple(reals[name] for name in
                   ("jobs", "supply", "signals", "trades", "dispatch",
                    "execution", "advice"))
    paths8 = paths7 + (reals["ledger"],)

    store = _get_store(coordination)
    with store.lock:
        # Lock order is fixed for every process: the coordination
        # ledger's exclusive lock first -- the one lock that serializes
        # whole batches and that only this module takes -- and then the
        # nine business ledgers' shared locks in resolved real-path
        # order. The shared locks are released around each downstream
        # call (apply/start/record/recover/settle take the intent and
        # settlement ledgers' own exclusive locks themselves, and
        # discover the settlement ledger beside the snapshots), exactly
        # as execution_sync releases its inputs before dispatching.
        held: list[Any] = []

        def acquire_inputs() -> None:
            for real in sorted(set(reals[name] for name in _INPUT_NAMES)):
                manager = _lock(real, shared=True)
                manager.__enter__()
                held.append(manager)

        def release_inputs() -> None:
            while held:
                manager = held.pop()
                with contextlib.suppress(BaseException):
                    manager.__exit__(None, None, None)

        with _lock(coordination_real):
            acquire_inputs()
            snapshot = _Snapshot(reals)
            try:
                snapshot_map = {frozenset(snapshot.input_map.items()):
                                snapshot}
                batches, audit_events, _ledger_inputs, old_bytes = \
                    _load_coordination(coordination_real, snapshot_map,
                                       snapshot.input_map)

                batch = batches.get(key)
                created = batch is None
                if created:
                    excluded = {
                        member
                        for existing in batches.values()
                        if existing["status"] == "pending"
                        for member in existing["items"]
                    }
                    members = _build_members(
                        snapshot, key, owner, now, lease, excluded)
                    # A receipt only matches a member that is or becomes
                    # an active migration plan; a keep or settling member
                    # cannot consume one. A receipt contradicting a step
                    # the member's plan already recorded is a changed
                    # request, refused before the first commit.
                    for receipt_job, receipt in checked_receipts.items():
                        member = next(
                            (candidate for candidate in members
                             if candidate["job_id"] == receipt_job), None)
                        if member is None \
                                or member["phase"] not in (
                                    "evaluating", "reserved", "claimed",
                                    "active"):
                            raise ValueError("receipt does not match an "
                                             "active migration member")
                        if member["plan_key"] is not None:
                            plan = snapshot.plans.get(member["plan_key"])
                            if plan is not None:
                                _check_receipt_against_plan(plan, receipt)
                    batch = {
                        "key": key,
                        "owner": owner,
                        "until": now + lease,
                        "status": "pending",
                        "inputs": snapshot.input_map,
                        "items": {member["job_id"]: member
                                  for member in members},
                    }
                    batches[key] = batch
                else:
                    if batch["inputs"] != snapshot.input_map:
                        raise ValueError("batch key was already used with "
                                         "a different business ledger set")
                    if batch["status"] == "completed":
                        # A terminal batch replays byte-for-byte for
                        # every caller and never renews its lease.
                        return _batch_snapshot(batch), False
                    # A receipt only makes sense for a member that is or
                    # is about to become active; a receipt that
                    # contradicts a step already recorded for its plan is
                    # a changed request, refused before the ownership
                    # decision.
                    for job_id, receipt in checked_receipts.items():
                        item = batch["items"].get(job_id)
                        if item is None \
                                or item["phase"] in ("kept", "settled",
                                                     "failed", "settling"):
                            raise ValueError("receipt does not match an "
                                             "active migration member")
                        if _is_marker(item["snapshot"]) \
                                and item["snapshot"]["action"] == "record":
                            # The persisted record request is the durable
                            # original; a later receipt for the same step
                            # must equal it exactly.
                            request = item["snapshot"]["request"]
                            if request["step"] == receipt["step"] and (
                                    request["result"] != receipt["result"]
                                    or request["receipt"]
                                    != receipt["receipt"]
                                    or request["at"] != receipt["at"]):
                                raise ValueError(
                                    "a receipt already recorded for this "
                                    "step cannot change")
                        plan = snapshot.plans.get(item["plan_key"])
                        if plan is None:
                            # The claim has not happened yet; the receipt
                            # is consumed later in this same run once the
                            # plan exists.
                            continue
                        _check_receipt_against_plan(plan, receipt)
                    if batch["owner"] != owner:
                        if now <= batch["until"]:
                            raise PermissionError(
                                "migration batch is owned by another "
                                f"owner until {batch['until']}")
                        batch["owner"] = owner
                    batch["until"] = now + lease

                def persist() -> None:
                    nonlocal old_bytes
                    payload = _canonical_bytes(batches, audit_events)
                    _commit_file(coordination_real, payload, old_bytes)
                    old_bytes = payload

                def downstream(call: Callable[[], Any]) -> Any:
                    # Release the shared business locks while the
                    # existing interface takes its own exclusive locks,
                    # then re-acquire them in the same global order; the
                    # caller re-reads the whole snapshot afterwards.
                    release_inputs()
                    try:
                        return call()
                    finally:
                        acquire_inputs()

                def fail_member(item: dict[str, Any],
                                exc: BaseException) -> None:
                    item["phase"] = "failed"
                    item["error"] = type(exc).__name__
                    item["snapshot"] = None
                    persist()

                def replay_pending(item: dict[str, Any],
                                   marker: dict[str, Any]) -> bool:
                    """Replay one persisted downstream request.

                    Returns False when the member was failed by a
                    business exception; the caller then moves to the
                    next member.
                    """
                    action = marker["action"]
                    request = marker["request"]
                    job_id = item["job_id"]
                    plan_key = item["plan_key"]
                    try:
                        if action == "evaluate":
                            evaluate_key = item["request_key"]
                            advice, _ = downstream(lambda: _rebalance.
                                                   evaluate(
                                                       *paths7, job_id,
                                                       evaluate_key,
                                                       request["at"]))
                            if advice["recommendation"] == "keep":
                                item["phase"] = "kept"
                                item["plan_key"] = None
                                item["lease"] = None
                                item["request_key"] = _stable_key(
                                    key, job_id, "keep")
                                item["snapshot"] = copy.deepcopy(
                                    advice["current"])
                            else:
                                # The advice record is bound to the
                                # evaluate request key; the reservation
                                # is persisted next under its own stable
                                # key before apply is ever called.
                                item["phase"] = "reserved"
                                item["request_key"] = _stable_key(
                                    key, job_id, "apply")
                                item["snapshot"] = _marker(
                                    "apply", advice_key=evaluate_key,
                                    at=request["at"])
                        elif action == "apply":
                            _intent, _ = downstream(lambda: _rebalance.apply(
                                *paths8, job_id, request["advice_key"],
                                item["request_key"], request["at"]))
                            # The claim happens in this run: its owner is
                            # the current batch owner and the lease window
                            # starts at the current moment, capped at the
                            # job deadline.
                            lease_end = _lease_end_for(
                                snapshot.accepted[job_id], now, lease)
                            item["phase"] = "claimed"
                            item["plan_key"] = _plan_key_for(key, job_id)
                            item["request_key"] = item["plan_key"]
                            item["lease"] = lease_end
                            item["snapshot"] = _marker(
                                "start", owner=batch["owner"],
                                lease_end=lease_end, at=now)
                        elif action == "start":
                            plan, _ = downstream(lambda: _rebalance.start(
                                *paths8, job_id, item["request_key"],
                                request["owner"], request["lease_end"],
                                request["at"]))
                            item["phase"] = "active"
                            item["plan_key"] = item["request_key"]
                            item["lease"] = request["lease_end"]
                            item["snapshot"] = copy.deepcopy(plan)
                        elif action == "record":
                            plan, _ = downstream(lambda: _rebalance.record(
                                *paths8, job_id, item["request_key"],
                                request["owner"], request["step"],
                                request["result"], request["receipt"],
                                request["at"]))
                            item["phase"] = "active"
                            item["lease"] = plan["lease_end"]
                            item["snapshot"] = copy.deepcopy(plan)
                        elif action == "recover":
                            plan, _ = downstream(lambda: _rebalance.recover(
                                *paths8, job_id, item["request_key"],
                                request["owner"], request["at"]))
                            settle_key, settle_at = snapshot.\
                                settle_request(
                                    job_id, plan_key, request["at"],
                                    _settle_key_for(plan_key))
                            item["phase"] = "settling"
                            item["request_key"] = settle_key
                            item["snapshot"] = _marker(
                                "settle", plan_key=plan_key,
                                at=settle_at)
                        else:  # settle
                            record, _ = downstream(lambda: _rebalance.settle(
                                *paths8, reals["settlements"], job_id,
                                request["plan_key"], item["request_key"],
                                request["at"]))
                            item["phase"] = "settled"
                            item["snapshot"] = copy.deepcopy(record)
                    except Exception as exc:
                        if _is_business_error(exc):
                            fail_member(item, exc)
                            return False
                        raise
                    persist()
                    return True

                # First commit: every member persisted in its starting
                # stage before any downstream interface is called; the
                # lease renewal of a re-entry lands here as well.
                persist()

                # Receipts are consumed once per run as their step is
                # persisted; a crash replays from the durable marker, not
                # from this in-memory map, so the same receipt can never
                # drive a second record call within one run.
                pending_receipts = dict(checked_receipts)

                for item in sorted(batch["items"].values(),
                                   key=lambda member: member["job_id"]):
                    while item["phase"] not in _ITEM_TERMINAL:
                        if _is_marker(item["snapshot"]):
                            # A persisted request is continued only with
                            # its original action and parameters.
                            marker = _validate_marker(item["snapshot"])
                            fresh = _Snapshot(reals)
                            if item["phase"] == "active" \
                                    and item["plan_key"] is not None:
                                plan = fresh.plans.get(item["plan_key"])
                                if plan is None or plan["source"] != \
                                        fresh.current_binding(item["job_id"]):
                                    fail_member(item, ValueError(
                                        "the migration generation is no "
                                        "longer current"))
                                    break
                            elif item["phase"] == "settling":
                                # A settle may already have completed in a
                                # crashed run, advancing the current
                                # binding past the plan's source; that is
                                # an idempotent replay, not a generation
                                # break. Only when no settlement exists
                                # must the plan still be rooted at the
                                # current binding.
                                already_settled = any(
                                    record["job_id"] == item["job_id"]
                                    and record["plan_key"] == item["plan_key"]
                                    for record in fresh.settlements.values())
                                plan = fresh.plans.get(item["plan_key"])
                                if not already_settled and (
                                        plan is None or plan["source"] !=
                                        fresh.current_binding(item["job_id"])):
                                    fail_member(item, ValueError(
                                        "the migration generation is no "
                                        "longer current"))
                                    break
                            snapshot = fresh
                            if not replay_pending(item, marker):
                                break
                            snapshot = _Snapshot(reals)
                            continue

                        if item["phase"] != "active":
                            # A result snapshot outside the active phase
                            # is a stage that cannot be advanced.
                            fail_member(item, ValueError(
                                "migration batch item is stuck in the "
                                f"{item['phase']} stage"))
                            break

                        plan = snapshot.plans.get(item["plan_key"])
                        if plan is None or plan["source"] != \
                                snapshot.current_binding(item["job_id"]):
                            fail_member(item, ValueError(
                                "the migration generation is no longer "
                                "current"))
                            break
                        if plan["state"] in _PLAN_TERMINAL:
                            settle_at = max(
                                now, snapshot.terminal_moment(
                                    plan, item["plan_key"]))
                            settle_key, settle_at = snapshot.\
                                settle_request(
                                    item["job_id"], item["plan_key"],
                                    settle_at,
                                    _settle_key_for(item["plan_key"]))
                            item["phase"] = "settling"
                            item["request_key"] = settle_key
                            item["snapshot"] = _marker(
                                "settle", plan_key=item["plan_key"],
                                at=settle_at)
                            persist()
                            continue
                        if plan["state"] != "active":
                            fail_member(item, ValueError(
                                "migration plan is not active"))
                            break

                        next_step = _STEPS[len(plan["steps"])]
                        receipt = pending_receipts.get(item["job_id"])
                        if receipt is not None and \
                                _recorded_step(plan, receipt) is not None:
                            # The receipt already landed downstream: an
                            # exactly equivalent replay simply returns
                            # the current progress -- no second record
                            # action, no member failure -- and the
                            # latest snapshot keeps the plan with its
                            # real step receipts. (A changed receipt was
                            # refused before the first commit.)
                            _check_receipt_against_plan(plan, receipt)
                            pending_receipts.pop(item["job_id"], None)
                            item["lease"] = plan["lease_end"]
                            item["snapshot"] = copy.deepcopy(plan)
                            persist()
                            continue
                        if receipt is not None and \
                                receipt["step"] == next_step:
                            item["request_key"] = _stable_key(
                                key, item["job_id"], "record:" + next_step)
                            item["snapshot"] = _marker(
                                "record", owner=plan["owner"],
                                step=next_step, result=receipt["result"],
                                receipt=receipt["receipt"], at=receipt["at"])
                            pending_receipts.pop(item["job_id"], None)
                            persist()
                            continue
                        if receipt is not None:
                            # A receipt naming anything but the next
                            # pending step goes through the existing
                            # interface, which refuses it; the public
                            # class fails this member.
                            item["request_key"] = _stable_key(
                                key, item["job_id"], "receipt:" +
                                receipt["step"])
                            item["snapshot"] = _marker(
                                "record", owner=plan["owner"],
                                step=receipt["step"],
                                result=receipt["result"],
                                receipt=receipt["receipt"],
                                at=receipt["at"])
                            pending_receipts.pop(item["job_id"], None)
                            persist()
                            continue
                        if now <= plan["lease_end"]:
                            # No matching receipt and a still-valid
                            # lease: wait for a later run; the other
                            # members are independent and continue.
                            item["lease"] = plan["lease_end"]
                            item["snapshot"] = copy.deepcopy(plan)
                            persist()
                            break
                        # Strict expiry: persist the recovery action and
                        # parameters before the existing recover
                        # interface is called. Recovery on re-entry
                        # replays this exact moment.
                        item["request_key"] = _stable_key(
                            key, item["job_id"], "recover")
                        item["snapshot"] = _marker(
                            "recover", owner=plan["owner"], at=now)
                        persist()

                if all(member["phase"] in _ITEM_TERMINAL
                       for member in batch["items"].values()):
                    batch["status"] = "completed"
                    audit_events.append({
                        "key": key,
                        "at": now,
                        "batch": _canonical_batch(batch),
                    })
                    payload = _canonical_bytes(batches, audit_events)
                    # Validate the closing bytes against a fresh
                    # snapshot of the nine business ledgers.
                    release_inputs()
                    acquire_inputs()
                    fresh = _Snapshot(reals)
                    _validate_ledger(
                        json.loads(payload.decode("utf-8")),
                        {frozenset(fresh.input_map.items()): fresh},
                        fresh.input_map)
                    _commit_file(coordination_real, payload, old_bytes)
                    old_bytes = payload
                return _batch_snapshot(batch), created
            finally:
                release_inputs()


# ---------------------------------------------------------------------------
# Read-only lookup
# ---------------------------------------------------------------------------


def get(ledger: str, key: str) -> dict[str, object]:
    """Return a copy of one batch without writing anything.

    ``ledger`` is the coordination ledger path and ``key`` the batch
    key; both must be non-empty strings. The batch's nine recorded
    business ledgers are read to revalidate every reference, so
    malformed, out-of-order or non-canonical bytes raise ``ValueError``
    and a missing business input ``FileNotFoundError``. An unknown
    batch key -- including a missing coordination ledger -- raises
    ``KeyError``. The lookup is purely read-only: it never opens a lock
    file and never creates a lock file, a directory or any other trace.
    """
    for value in (ledger, key):
        if not isinstance(value, str) or not value:
            raise ValueError("ledger and key must be non-empty strings")
    realpath = os.path.realpath(ledger)
    store = _get_store(realpath)
    with store.lock:
        # Every ledger write lands by atomic replace, so a plain read
        # always sees one complete generation; no lock file is opened
        # and nothing is ever created.
        try:
            with open(realpath, "rb") as handle:
                raw = handle.read()
        except FileNotFoundError:
            raise KeyError(key)
        try:
            data = finite_loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            raise ValueError(
                f"migration coordination ledger {realpath!r} is not "
                "valid JSON") from exc
        if not isinstance(data, dict) \
                or not isinstance(data.get("batches"), dict):
            raise ValueError("migration coordination ledger has "
                             "invalid fields")
        if key not in data["batches"]:
            raise KeyError(key)
        inputs = _validate_inputs(data["batches"][key]["inputs"])
        snapshot = _Snapshot(inputs)
        batches, _audit, _inputs, _raw = _load_coordination(
            realpath,
            {frozenset(snapshot.input_map.items()): snapshot},
            None)
        return _batch_snapshot(batches[key])
