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
trade and is not driven by another still-open batch, ordered by job id.
The judgment is rooted at the latest completed binding
``rebalance.current`` returns (the immutable trade when there is none):

* a job with no current-round migration evidence whose booking is
  finished or in flight through the baseline layers simply stays on its
  current binding (``keep``);
* otherwise a fresh advice is evaluated at the batch moment; a ``keep``
  advice completes directly and a ``migrate`` advice is reserved and
  claimed under stable request keys;
* an unclaimed reservation rooted at the current binding is claimed;
* an active plan receives the receipt that names its next pending step,
  waits while the plan lease is valid without such a receipt, and is
  recovered once the lease has strictly expired;
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
expired. A member that drives a migration plan keeps the plan's actual
step receipts -- the credential text verbatim, including non-ASCII -- in
its most recent snapshot, never just a stage marker. A receipt that is
fully equivalent to a step already saved returns the current progress
instead of failing the member, while any change to that step's result,
credential or moment raises ``ValueError`` with the coordination bytes
untouched; if the step previously landed downstream but the coordination
snapshot has not advanced yet, re-entry catches the original action up
exactly once.

The coordination ledger (version 1, sections ``version``, key-sorted
``batches``, an append-only ``audit`` and the incremental ``events``
stream) is one compact UTF-8 JSON document with non-ASCII written
through, decimal integers and exactly one trailing newline. The audit
is appended in the real order batches complete and is never re-sorted
by batch key; each completed batch has exactly one event whose snapshot
equals that batch's complete state at completion. The progress events
record every observable durable commit at a continuous zero-based
position that is never reused: creation, takeover, per-member progress,
receipt landings, member failure and terminalization, each embedding
the complete post-commit
batch snapshot; equivalent replays, coordination-lease renewals and
stateless waits append nothing. Ledgers written without the per-item
receipts field still load and replay byte-for-byte; a batch a run
rewrites gains that field. Ledgers written before the event stream
existed carry no ``events`` section, stay loading read-only and seed
one baseline event per recorded batch on their first later write.
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

__all__ = ["run", "get", "search", "events", "get_response",
           "search_response", "events_response"]

_VERSION = 1
_ROOT_FIELDS = ("version", "batches", "audit", "events")
# Ledgers written before the incremental event stream exist carry only
# version, batches and audit; they keep loading read-only and byte-for-byte,
# and the first later write seeds the baseline event at position 0.
_LEGACY_ROOT_FIELDS = ("version", "batches", "audit")
_BATCH_FIELDS = ("key", "owner", "until", "status", "inputs", "items")
_ITEM_FIELDS = ("job_id", "plan_key", "phase", "request_key", "lease",
                "error", "snapshot")
# Every item that drove (or drives) a migration plan also carries the
# actual step receipts its plan recorded, so the credential evidence --
# including non-ASCII credentials -- survives in the item's most recent
# snapshot instead of being reduced to a stage marker. The field is
# appended last and is optional on read: ledgers written by the baseline
# without it keep validating and replaying byte-for-byte.
_ITEM_RECEIPTS_FIELD = "receipts"
_ITEM_FIELDS_WITH_RECEIPTS = _ITEM_FIELDS + (_ITEM_RECEIPTS_FIELD,)
_EVENT_FIELDS = ("key", "at", "batch")
# Incremental progress events: one is appended for every durable commit
# an observer can distinguish. Each record carries, in fixed order, its
# zero-based stream position, the commit moment, the batch key, the member
# job the change belongs to (null for batch-wide changes), the change
# category and the complete post-commit batch snapshot.
_PROGRESS_EVENT_FIELDS = ("position", "at", "key", "job", "kind", "batch")
_EVENT_KINDS = ("created", "taken_over", "progress", "receipt",
                "failed", "completed")
_KIND_CREATED = "created"
_KIND_TAKEN_OVER = "taken_over"
_KIND_PROGRESS = "progress"
_KIND_RECEIPT = "receipt"
_KIND_FAILED = "failed"
_KIND_COMPLETED = "completed"
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


class _ChangedRequest(ValueError):
    # Internal marker: a persisted request conflicts with evidence the
    # downstream ledger already holds. It is a public ValueError to
    # callers but must abort the run with the coordination bytes
    # untouched instead of failing just the member.
    pass


class _CoordinationMissing(FileNotFoundError):
    # Internal marker for the read-only response builders: the fixed
    # coordination ledger itself is absent, as opposed to an unknown
    # batch key (KeyError) or a business ledger a canonical coordination
    # file references but cannot find (ValueError -- a broken
    # reference). It stays a FileNotFoundError to every other handler.
    pass


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


def _validate_step_receipts(raw: object) -> list[dict[str, Any]]:
    # The actual step receipts a migration item's plan recorded, in the
    # plan's fixed copy/switch order. The credentials are kept verbatim
    # (non-ASCII written through on serialization), never collapsed to a
    # stage marker.
    if not isinstance(raw, list) or len(raw) > len(_STEPS):
        raise ValueError("migration batch item receipts must be a list "
                         "within the copy/switch sequence")
    receipts: list[dict[str, Any]] = []
    for index, entry in enumerate(raw):
        if not isinstance(entry, dict) \
                or set(entry.keys()) != {"step", "result", "receipt", "at"}:
            raise ValueError("item step receipt has invalid fields")
        if entry["step"] != _STEPS[index]:
            raise ValueError("item step receipts must follow copy then "
                             "switch")
        if entry["result"] not in ("succeeded", "failed"):
            raise ValueError("item step result must be succeeded or failed")
        if not isinstance(entry["receipt"], str) or not entry["receipt"]:
            raise ValueError("item step receipt text must be a non-empty "
                             "string")
        if not _is_plain_int(entry["at"]) or entry["at"] < 0:
            raise ValueError("item step receipt at must be a non-boolean "
                             "non-negative integer")
        receipts.append({"step": entry["step"], "result": entry["result"],
                         "receipt": entry["receipt"], "at": entry["at"]})
    return receipts


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
    snapshot: _Snapshot | None,
    references: bool = True,
) -> tuple[dict[str, Any], bool]:
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("batch item keys must be non-empty strings")
    if not isinstance(raw, dict) \
            or set(raw.keys()) not in (set(_ITEM_FIELDS),
                                       set(_ITEM_FIELDS_WITH_RECEIPTS)):
        raise ValueError("migration batch item has invalid fields")
    legacy = _ITEM_RECEIPTS_FIELD not in raw
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
    item_receipts = _validate_step_receipts(
        raw["receipts"]) if not legacy else []

    if not references:
        # Intrinsic form only: a reader that has not opened the business
        # ledgers can still reject every shape, type, field-order and
        # enumeration error and every contradiction internal to the item
        # without following a single cross-reference. Cross-references
        # (accepted job, trade, plan, advice, settlement and the exact
        # receipt match) are the reference pass's job.
        if phase == "kept":
            if plan_key is not None or lease is not None \
                    or error is not None:
                raise ValueError("a kept item must not carry plan, lease or "
                                 "error data")
        elif phase == "failed":
            if error is None:
                raise ValueError("a failed item must carry the public "
                                 "error class name")
            if item_snapshot is not None:
                raise ValueError("a failed item must not carry a snapshot")
        else:
            if plan_key is None or error is not None:
                raise ValueError("an active migration item must name its "
                                 "plan key and carry no error")
            marker_actions = {
                "evaluating": ("evaluate",),
                "reserved": ("apply",),
                "claimed": ("start",),
                "active": ("record", "recover"),
                "settling": ("settle",),
            }
            if phase in ("evaluating", "reserved", "claimed", "settling") \
                    and not _is_marker(item_snapshot):
                # These four stages only ever persist a pending-action
                # marker; an active item may hold the plan snapshot and a
                # settled item its settlement record.
                raise ValueError(f"a {phase} item must carry a "
                                 "pending-action marker")
            if phase == "settled" and _is_marker(item_snapshot):
                # A settled item persists the settlement record, never a
                # pending action.
                raise ValueError("a settled item must snapshot its "
                                 "settlement record")
            if _is_marker(item_snapshot):
                marker = _validate_marker(item_snapshot)
                action = marker["action"]
                expected_actions = marker_actions[phase]
                if action not in expected_actions:
                    raise ValueError("migration batch item marker action "
                                     "does not match its phase")
                if action == "start" \
                        and lease != marker["request"]["lease_end"]:
                    raise ValueError("start marker lease must match the "
                                     "item lease")
                if action == "settle" \
                        and marker["request"]["plan_key"] != plan_key:
                    raise ValueError("settle marker must name the item's "
                                     "plan")
        return {
            "job_id": job_id,
            "plan_key": plan_key,
            "phase": phase,
            "request_key": request_key,
            "lease": lease,
            "error": error,
            "snapshot": copy.deepcopy(item_snapshot),
            "receipts": copy.deepcopy(item_receipts),
        }, legacy

    assert snapshot is not None
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

    def require_receipts_match(plan: dict[str, Any] | None) -> None:
        # The item's saved step receipts are exactly the plan's recorded
        # steps; an item that has not recorded a step carries none. A
        # baseline ledger item has no receipts field: its receipts are
        # derived from the referenced plan instead of enforced, so a
        # pending baseline batch still loads and normalizes on its next
        # commit while a completed one keeps replaying byte-for-byte.
        nonlocal item_receipts
        expected = plan["steps"] if plan is not None else []
        if legacy:
            item_receipts = copy.deepcopy(expected)
        elif item_receipts != expected:
            raise ValueError("migration batch item receipts must match the "
                             "plan's recorded step receipts")

    if phase == "kept":
        if plan_key is not None or lease is not None or error is not None:
            raise ValueError("a kept item must not carry plan, lease or "
                             "error data")
        if item_snapshot not in binding_choices:
            raise ValueError("a kept item snapshot must be a current "
                             "binding")
        require_receipts_match(None)
    elif phase == "failed":
        if error is None:
            raise ValueError("a failed item must carry the public error "
                             "class name")
        if item_snapshot is not None:
            raise ValueError("a failed item must not carry a snapshot")
        failed_plan = (snapshot.plans.get(plan_key)
                       if plan_key is not None else None)
        require_receipts_match(failed_plan)
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
            marker_plan = snapshot.plans.get(plan_key)
            if action == "apply":
                advice = snapshot.advice_records.get(
                    marker["request"]["advice_key"])
                if advice is None or advice["job_id"] != job_id \
                        or advice["recommendation"] != "migrate":
                    raise ValueError("apply marker must reference a "
                                     "recorded migrate advice for the job")
                require_receipts_match(None)
            elif action == "start":
                if lease != marker["request"]["lease_end"]:
                    raise ValueError("start marker lease must match the "
                                     "item lease")
                require_receipts_match(None)
            elif action in ("record", "recover"):
                if marker_plan is None or marker_plan["job_id"] != job_id:
                    raise ValueError("record/recover marker must reference "
                                     "a recorded migration plan")
                if phase == "active" and action == "record":
                    # The record action is in flight. Normally the item's
                    # receipts are exactly the plan's steps. A crash
                    # between the downstream commit and the coordination
                    # snapshot update leaves the plan carrying the very
                    # step the marker persists while the item receipts
                    # lag by that one step -- the catch-up state re-entry
                    # completes -- in which case the plan steps must be
                    # the item receipts followed by exactly the marker's
                    # own step. Any other divergence is corruption.
                    req = marker["request"]
                    lagging = {"step": req["step"], "result": req["result"],
                               "receipt": req["receipt"], "at": req["at"]}
                    plan_steps = marker_plan["steps"]
                    if not (item_receipts == plan_steps
                            or (len(plan_steps) == len(item_receipts) + 1
                                and plan_steps[:len(item_receipts)]
                                == item_receipts
                                and plan_steps[-1] == lagging)):
                        raise ValueError(
                            "migration batch item receipts must match the "
                            "plan's recorded step receipts")
                else:
                    require_receipts_match(marker_plan)
            elif action == "settle":
                if marker["request"]["plan_key"] != plan_key:
                    raise ValueError("settle marker must name the item's "
                                     "plan")
                settled_already = any(
                    record["job_id"] == job_id
                    and record["plan_key"] == plan_key
                    for record in snapshot.settlements.values())
                if not settled_already and (
                        marker_plan is None
                        or marker_plan["job_id"] != job_id
                        or marker_plan["state"] not in _PLAN_TERMINAL):
                    raise ValueError("settle marker must reference a "
                                     "terminal migration plan")
                require_receipts_match(marker_plan)
        elif phase == "settled":
            settled = next((record for record in snapshot.settlements.values()
                            if record["job_id"] == job_id
                            and record["plan_key"] == plan_key), None)
            settled_plan = snapshot.plans.get(plan_key)
            if settled is None or item_snapshot != settled:
                raise ValueError("a settled item must snapshot its "
                                 "settlement record")
            require_receipts_match(settled_plan)
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
            require_receipts_match(plan)

    return {
        "job_id": job_id,
        "plan_key": plan_key,
        "phase": phase,
        "request_key": request_key,
        "lease": lease,
        "error": error,
        "snapshot": copy.deepcopy(item_snapshot),
        "receipts": copy.deepcopy(item_receipts),
    }, legacy


def _validate_ledger(
    data: object,
    snapshots: dict[frozenset[tuple[str, str]], _Snapshot],
    default_inputs: dict[str, str] | None,
    require_references: bool = True,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]],
           list[dict[str, Any]], dict[str, str], set[str]]:
    if not isinstance(data, dict) \
            or set(data.keys()) not in (set(_ROOT_FIELDS),
                                        set(_LEGACY_ROOT_FIELDS)):
        raise ValueError("migration coordination ledger root must be an "
                         "object with keys version, batches, audit and "
                         "events")
    legacy_doc = "events" not in data
    if not _is_plain_int(data["version"]) or data["version"] != _VERSION:
        raise ValueError("unsupported migration coordination ledger "
                         "version")
    batches_raw = data["batches"]
    audit_raw = data["audit"]
    if not isinstance(batches_raw, dict) or not isinstance(audit_raw, list):
        raise ValueError("batches must be an object and audit a list")
    events_raw = [] if legacy_doc else data["events"]
    if not isinstance(events_raw, list):
        raise ValueError("events must be a list")
    _check_sorted_keys(batches_raw, "batches")

    batches: dict[str, dict[str, Any]] = {}
    completed: set[str] = set()
    # Batches written by the baseline carry no per-item receipts field.
    # Their arity is uniform within a batch (a batch is always written as
    # one document); a mixed batch is non-canonical. The set lets reads
    # reproduce the original bytes while every new commit writes the
    # receipts field on every item.
    legacy_batches: set[str] = set()
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
        if require_references and snapshot is None:
            raise ValueError("batch inputs do not name the loaded "
                             "business snapshot")
        item_shapes: set[bool] = set()
        items: dict[str, dict[str, Any]] = {}
        for member_key, item_raw in items_raw.items():
            item, item_legacy = _validate_item(
                item_raw, member_key,
                snapshot if require_references else None,
                references=require_references)
            item_shapes.add(item_legacy)
            items[member_key] = item
        if len(item_shapes) > 1:
            raise ValueError("a batch must uniformly carry the per-item "
                             "receipts field or omit it")
        if item_shapes == {True}:
            legacy_batches.add(batch_key)
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
        # The audit is an append-only log in the real order batches
        # completed: the physical position carries the order, so events
        # need not be -- and must not be re-sorted by -- batch key. Only
        # duplication is illegal here.
        if event_key in seen_events:
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
        # Compare against the batch in its on-disk arity: a baseline
        # batch's audit snapshot predates the per-item receipts field,
        # whose values are derived on load.
        expected_event_batch = _canonical_batch(
            batch, legacy=event_key in legacy_batches)
        if event_raw["batch"] != expected_event_batch:
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

    events = _validate_progress_events(
        events_raw, batches, legacy_batches, legacy_doc)
    return batches, audit, events, ledger_inputs, legacy_batches


def _event_snapshot_legacy(snapshot: dict[str, Any],
                           legacy_batches: set[str]) -> bool:
    # A progress event embeds its batch in the arity the batch had when
    # the event was committed: a non-empty snapshot reveals it directly
    # from its items, while an empty batch is arity-identical either way
    # and falls back to the batches section's current classification.
    items = snapshot["items"]
    if items:
        first = next(iter(items.values()))
        return "receipts" not in first if isinstance(first, dict) else False
    return snapshot["key"] in legacy_batches


def _validate_event_snapshot(
    event_key: str,
    raw: object,
    kind: str,
    job: str | None,
) -> tuple[dict[str, Any], bool]:
    # Intrinsic validation of a historical post-commit batch snapshot:
    # shape, types, field order and enumerations only, never the
    # cross-references -- those move with the business ledgers and a
    # later event legitimately embeds a state the ledgers no longer
    # contain. Returns the fixed-order snapshot and its own per-item
    # arity (legacy events predate the per-item receipts field).
    if not isinstance(raw, dict) or set(raw.keys()) != set(_BATCH_FIELDS):
        raise ValueError("migration progress event snapshot has invalid "
                         "fields")
    if raw["key"] != event_key:
        raise ValueError("migration progress event snapshot key mismatch")
    owner = raw["owner"]
    if not isinstance(owner, str) or not owner:
        raise ValueError("migration progress event snapshot owner must be a "
                         "non-empty string")
    until = raw["until"]
    if not _is_plain_int(until) or until < 1:
        raise ValueError("migration progress event snapshot until must be a "
                         "positive integer")
    if raw["status"] not in _BATCH_STATUSES:
        raise ValueError("migration progress event snapshot status is "
                         "invalid")
    inputs = _validate_inputs(raw["inputs"])
    items_raw = raw["items"]
    if not isinstance(items_raw, dict):
        raise ValueError("migration progress event snapshot items must be "
                         "an object")
    _check_sorted_keys(items_raw, "progress event batch items")
    canonical_items: dict[str, Any] = {}
    shapes: set[bool] = set()
    for member, item_raw in items_raw.items():
        item, this_legacy = _validate_item(
            item_raw, member, None, references=False)
        shapes.add(this_legacy)
        # Preserve the snapshot's own arity so the event re-serializes
        # to its exact original bytes.
        canonical_items[member] = _canonical_item(item, legacy=this_legacy)
    if len(shapes) > 1:
        raise ValueError("a progress event snapshot must uniformly carry "
                         "or omit the per-item receipts field")
    event_legacy = next(iter(shapes), False)
    if kind == _KIND_COMPLETED and raw["status"] != "completed":
        raise ValueError("a completed progress event must snapshot a "
                         "completed batch")
    if kind in (_KIND_PROGRESS, _KIND_RECEIPT, _KIND_FAILED,
                _KIND_TAKEN_OVER) \
            and raw["status"] != "pending":
        raise ValueError("an in-flight progress event must snapshot a "
                         "pending batch")
    if kind == _KIND_FAILED:
        assert job is not None
        failed_raw = items_raw.get(job)
        if not isinstance(failed_raw, dict) \
                or failed_raw.get("phase") != "failed" \
                or failed_raw.get("error") is None:
            raise ValueError("a failed progress event must name the item "
                             "that carries the error")
    snapshot = {"key": event_key, "owner": owner, "until": until,
                "status": raw["status"], "inputs": inputs,
                "items": canonical_items}
    return snapshot, event_legacy


def _validate_progress_events(
    events_raw: list[Any],
    batches: dict[str, dict[str, Any]],
    legacy_batches: set[str],
    legacy_doc: bool,
) -> list[dict[str, Any]]:
    # The incremental stream is a zero-based gapless sequence: positions
    # are strictly increasing and never reused, one record per observable
    # durable commit. A legacy document (no events section) reads as an
    # empty stream; its first write seeds one created baseline event per
    # already recorded batch instead.
    events: list[dict[str, Any]] = []
    latest_position: dict[str, int] = {}
    seen_keys: set[str] = set()
    for position, event_raw in enumerate(events_raw):
        if not isinstance(event_raw, dict) \
                or set(event_raw.keys()) != set(_PROGRESS_EVENT_FIELDS):
            raise ValueError("migration progress event has invalid fields")
        if not _is_plain_int(event_raw["position"]) \
                or event_raw["position"] != position:
            raise ValueError("migration progress event positions must be a "
                             "continuous zero-based sequence")
        at = event_raw["at"]
        if not _is_plain_int(at) or at < 0:
            raise ValueError("migration progress event moment must be a "
                             "non-boolean non-negative integer")
        event_key = event_raw["key"]
        if not isinstance(event_key, str) or not event_key \
                or event_key not in batches:
            raise ValueError("migration progress event must reference a "
                             "recorded batch")
        job = event_raw["job"]
        if job is not None and (not isinstance(job, str) or not job
                                or job not in batches[event_key]["items"]):
            raise ValueError("migration progress event job must be null or "
                             "name one of the batch's items")
        kind = event_raw["kind"]
        if kind not in _EVENT_KINDS:
            raise ValueError("migration progress event kind is invalid")
        if position == 0 and kind != _KIND_CREATED:
            # The stream opens with the created baseline.
            raise ValueError("the first migration progress event must be "
                             "the created baseline")
        if kind == _KIND_CREATED:
            if event_key in seen_keys:
                raise ValueError("a batch is created at most once in the "
                                 "progress event stream")
        if (job is None) != (kind in (_KIND_CREATED, _KIND_COMPLETED,
                                      _KIND_TAKEN_OVER)):
            raise ValueError("migration progress event kind does not match "
                             "its job field")
        snapshot, _event_legacy = _validate_event_snapshot(
            event_key, event_raw["batch"], kind, job)
        seen_keys.add(event_key)
        events.append({"position": position, "at": at, "key": event_key,
                       "job": job, "kind": kind, "batch": snapshot})
        latest_position[event_key] = position

    if legacy_doc:
        # A pre-events ledger reads as an empty stream and makes no
        # coverage promise; its first later write seeds the baseline.
        return []

    # Every recorded batch must have a created event and its last event
    # must observe its current state; for a completed batch that last
    # event is the completion itself, so a finish can never be observed
    # half-done. A still-pending batch's last snapshot may lag the
    # batches section by a coordination-lease renewal, which appends no
    # event by design.
    for event_key, batch in batches.items():
        if event_key not in seen_keys:
            raise ValueError("every batch must appear in the progress event "
                             "stream")
        if batch["status"] != "completed":
            continue
        latest = events[latest_position[event_key]]
        if latest["kind"] != _KIND_COMPLETED:
            raise ValueError("a completed batch's last progress event must "
                             "be its completed event")
        event_legacy = _event_snapshot_legacy(latest["batch"], legacy_batches)
        if _canonical_batch(latest["batch"], legacy=event_legacy) \
                != _canonical_batch(batch, legacy=event_legacy):
            raise ValueError("the completed progress event must snapshot "
                             "the batch's current state")
    if not events and not legacy_doc:
        # A current-format ledger always holds at least the created
        # baseline; an empty stream only exists in a legacy document.
        raise ValueError("migration progress events must start with the "
                         "created baseline")
    return events


def _canonical_item(item: dict[str, Any], legacy: bool = False) -> dict[str, Any]:
    fields = _ITEM_FIELDS if legacy else _ITEM_FIELDS_WITH_RECEIPTS
    return {field: copy.deepcopy(item[field]) for field in fields}


def _canonical_batch(batch: dict[str, Any], legacy: bool = False) -> dict[str, Any]:
    return {
        "key": batch["key"],
        "owner": batch["owner"],
        "until": batch["until"],
        "status": batch["status"],
        "inputs": {name: batch["inputs"][name] for name in _INPUT_NAMES},
        "items": {job_id: _canonical_item(batch["items"][job_id], legacy)
                  for job_id in sorted(batch["items"])},
    }


def _canonical_event(event: dict[str, Any]) -> dict[str, Any]:
    # The embedded batch keeps the exact arity it was committed with: a
    # baseline batch's seed/audit-era events omit the per-item receipts
    # field while later events carry it. Stored snapshots are already in
    # canonical fixed-order form, so they serialize verbatim.
    return {
        "position": event["position"],
        "at": event["at"],
        "key": event["key"],
        "job": event["job"],
        "kind": event["kind"],
        "batch": copy.deepcopy(event["batch"]),
    }


def _canonical_bytes(
    batches: dict[str, dict[str, Any]],
    audit: list[dict[str, Any]],
    legacy_batches: set[str] | None = None,
    events: list[dict[str, Any]] | None = None,
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, sections in their
    # fixed field order, batches and their items code-point sorted and
    # the audit appended in the physical order in which the batches
    # actually completed (never re-sorted by batch key), terminated by
    # exactly one newline. Baseline batches without the per-item
    # receipts field are reproduced field-for-field; every batch a run
    # (re)writes carries that field. The incremental progress events
    # follow the audit; ``events=None`` reproduces a pre-events legacy
    # document byte-for-byte, while a current write always carries the
    # continuous zero-based stream.
    legacy_batches = legacy_batches or set()
    payload: dict[str, Any] = {
        "version": _VERSION,
        "batches": {key: _canonical_batch(
                        batches[key], legacy=key in legacy_batches)
                    for key in sorted(batches)},
        "audit": [{"key": event["key"], "at": event["at"],
                   "batch": _canonical_batch(
                       event["batch"],
                       legacy=event["key"] in legacy_batches)}
                  for event in audit],
    }
    if events is not None:
        payload["events"] = [_canonical_event(event) for event in events]
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _parse_coordination_bytes(
    realpath: str,
    raw: bytes,
    snapshots: dict[frozenset[tuple[str, str]], _Snapshot],
    default_inputs: dict[str, str] | None = None,
    require_references: bool = True,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]],
           list[dict[str, Any]], dict[str, str], set[str], bool]:
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
    batches, audit, events, inputs, legacy_batches = _validate_ledger(
        data, snapshots, default_inputs,
        require_references=require_references)
    legacy_doc = "events" not in (data if isinstance(data, dict) else {})
    if raw != _canonical_bytes(
            batches, audit, legacy_batches,
            events=None if legacy_doc else events):
        raise ValueError(
            f"migration coordination ledger {realpath!r} is not in "
            "canonical compact form")
    return batches, audit, events, inputs, legacy_batches, legacy_doc


def _load_coordination(
    realpath: str,
    snapshots: dict[frozenset[tuple[str, str]], _Snapshot],
    default_inputs: dict[str, str] | None = None,
    require_references: bool = True,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]],
           list[dict[str, Any]], dict[str, str], bytes | None, set[str],
           bool]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        if default_inputs is None:
            raise
        # A not-yet-created coordination ledger starts a current-format
        # document: its first batch carries the created event, with no
        # legacy section to reproduce.
        return {}, [], [], default_inputs, None, set(), False
    result = _parse_coordination_bytes(
        realpath, raw, snapshots, default_inputs,
        require_references=require_references)
    batches, audit, events, inputs, legacy_batches, legacy_doc = result
    return batches, audit, events, inputs, raw, legacy_batches, legacy_doc


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
    # Fix the membership from one complete snapshot: accepted jobs that
    # already hold a trade and whose booking is not yet finished -- a
    # succeeded dispatch or a completed execution plan never becomes a
    # member again -- minus jobs another still-open batch already
    # drives, ordered by job id. The stage each member starts in is
    # fixed here; only its downstream requests advance later.
    members: list[dict[str, Any]] = []
    for job_id in sorted(snapshot.accepted):
        if snapshot.cleared.get(job_id) is None or job_id in excluded:
            continue
        decision = snapshot.decisions.get(job_id)
        exec_states = {plan["state"]
                       for plan in snapshot.exec_plans.get(job_id, {}).values()}
        if (decision is not None and decision["state"] == "succeeded") \
                or "completed" in exec_states:
            # A completed dispatch or execution is a finished booking:
            # it is fixed by no batch, now or on any later scan.
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
                # The plan may already hold recorded step receipts; the
                # first snapshot keeps them verbatim, including non-ASCII
                # credentials, instead of reducing the member to a stage.
                "receipts": copy.deepcopy(active["steps"]),
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
                "receipts": copy.deepcopy(terminal["steps"]),
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
                "receipts": [],
            })
            continue
        decision = snapshot.decisions.get(job_id)
        exec_states = {plan["state"]
                       for plan in snapshot.exec_plans.get(job_id, {}).values()}
        if decision is not None and (
                decision["state"] in ("succeeded", "claimed")
                or exec_states & {"completed", "active"}) \
                or now > snapshot.accepted[job_id]["deadline"]:
            # No current migration round and the booking itself is
            # finished or in flight through the baseline layers, or the
            # deadline has already passed so no migration is possible:
            # the job simply stays on its current binding.
            members.append({
                "job_id": job_id,
                "plan_key": None,
                "phase": "kept",
                "request_key": _stable_key(batch_key, job_id, "keep"),
                "lease": None,
                "error": None,
                "snapshot": binding,
                "receipts": [],
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
            "receipts": [],
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
    snapshot and persists them in job-id order; an empty scan still
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
                batches, audit_events, progress_events, _ledger_inputs, \
                    old_bytes, legacy_batches, legacy_doc = \
                    _load_coordination(
                        coordination_real, snapshot_map, snapshot.input_map)

                # A pre-events ledger stays read-only until this write:
                # the first commit seeds one created baseline per
                # recorded batch, frozen at the just-loaded state so the
                # very same commit's real change still follows as its own
                # event. The snapshots are copied before any in-memory
                # mutation below.
                baseline_seed: list[dict[str, Any]] | None = None
                if legacy_doc:
                    completion_at = {event["key"]: event["at"]
                                     for event in audit_events}
                    baseline_seed = []
                    for batch_key in sorted(batches):
                        seeded = batches[batch_key]
                        seed_snapshot = _canonical_batch(
                            seeded, legacy=batch_key in legacy_batches)
                        baseline_seed.append({
                            "at": completion_at.get(batch_key, now),
                            "key": batch_key, "job": None,
                            "kind": _KIND_CREATED,
                            "batch": seed_snapshot,
                        })
                        if seeded["status"] == "completed":
                            # A batch the legacy ledger already closed
                            # seeds its terminalization too, so the
                            # stream's completed-batch invariant holds
                            # from the first write on.
                            baseline_seed.append({
                                "at": completion_at[batch_key],
                                "key": batch_key, "job": None,
                                "kind": _KIND_COMPLETED,
                                "batch": seed_snapshot,
                            })
                # The batch state the stream last observed, used to
                # suppress events for commits nothing observable changed
                # in (a pure lease renewal, an equivalent replay or a
                # wait that re-synced the identical plan). It is seeded
                # from the batch's newest event already on disk.
                last_observed: dict[str, Any] | None = next(
                    (event["batch"] for event in reversed(progress_events)
                     if event["key"] == key), None)

                batch = batches.get(key)
                created = batch is None
                takeover = False
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
                    # cannot consume one.
                    for receipt_job in checked_receipts:
                        member = next(
                            (candidate for candidate in members
                             if candidate["job_id"] == receipt_job), None)
                        if member is None \
                                or member["phase"] not in (
                                    "evaluating", "reserved", "claimed",
                                    "active"):
                            raise ValueError("receipt does not match an "
                                             "active migration member")
                    batch = {
                        "key": key,
                        "owner": owner,
                        "until": now + lease,
                        "status": "pending",
                        "inputs": snapshot.input_map,
                        "items": {member["job_id"]:
                                      dict(member, receipts=list(
                                          member.get("receipts", ())))
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
                    # contradicts a step already recorded for its plan or
                    # already saved on the member is a changed request,
                    # refused before the ownership decision and before any
                    # byte is written.
                    for job_id, receipt in checked_receipts.items():
                        item = batch["items"].get(job_id)
                        if item is None \
                                or item["phase"] in ("kept", "settled",
                                                     "failed", "settling"):
                            raise ValueError("receipt does not match an "
                                             "active migration member")

                        def conflicts_with(saved: dict[str, Any]) -> bool:
                            return saved["step"] == receipt["step"] and (
                                saved["result"] != receipt["result"]
                                or saved["receipt"] != receipt["receipt"]
                                or saved["at"] != receipt["at"])

                        if any(conflicts_with(saved)
                               for saved in item.get("receipts", ())):
                            raise ValueError("a receipt already saved for "
                                             "this step cannot change")
                        plan = snapshot.plans.get(item["plan_key"])
                        if plan is None:
                            # The claim has not happened yet; the receipt
                            # is consumed later in this same run once the
                            # plan exists.
                            continue
                        recorded = next(
                            (step for step in plan["steps"]
                             if step["step"] == receipt["step"]), None)
                        if recorded is not None and recorded != {
                                "step": receipt["step"],
                                "result": receipt["result"],
                                "receipt": receipt["receipt"],
                                "at": receipt["at"]}:
                            raise ValueError("a receipt already recorded "
                                             "for this step cannot change")
                    if batch["owner"] != owner:
                        if now <= batch["until"]:
                            raise PermissionError(
                                "migration batch is owned by another "
                                f"owner until {batch['until']}")
                        batch["owner"] = owner
                        takeover = True
                    else:
                        takeover = False

                    # A replay by the current owner carrying only
                    # receipts already saved equivalently on their items
                    # is a pure idempotent replay -- but only when the run
                    # genuinely has nothing else to do: no pending marker,
                    # no active item whose downstream plan is missing,
                    # unrooted or terminal (it still has to settle), and
                    # no active plan whose lease has strictly expired (it
                    # still has to recover). Then it returns the current
                    # progress with created False, neither renewing the
                    # coordination lease nor rewriting a byte. A
                    # crash-window item (the plan landed a step but the
                    # item still holds its record marker, or the terminal
                    # plan still has to be settled) is left for the main
                    # loop to advance exactly once.
                    def receipt_already_settled(receipt_job: str,
                                                receipt: dict[str, Any]
                                                ) -> bool:
                        saved = next(
                            (s for s in batch["items"][receipt_job]
                             .get("receipts", ())
                             if s["step"] == receipt["step"]), None)
                        return saved == {
                            "step": receipt["step"],
                            "result": receipt["result"],
                            "receipt": receipt["receipt"],
                            "at": receipt["at"]}

                    def batch_is_stable_wait() -> bool:
                        for candidate in batch["items"].values():
                            if candidate["phase"] in _ITEM_TERMINAL:
                                continue
                            if _is_marker(candidate["snapshot"]):
                                return False
                            if candidate["phase"] != "active":
                                return False
                            plan_now = snapshot.plans.get(
                                candidate["plan_key"])
                            if plan_now is None \
                                    or plan_now["source"] != \
                                    snapshot.current_binding(
                                        candidate["job_id"]):
                                return False
                            if plan_now["state"] in _PLAN_TERMINAL:
                                return False
                            if plan_now["state"] != "active" \
                                    or now > plan_now["lease_end"]:
                                return False
                        return True

                    pure_receipt_replay = (
                        not takeover and bool(checked_receipts)
                        and batch_is_stable_wait()
                        and all(receipt_already_settled(
                                    receipt_job, receipt)
                                for receipt_job, receipt
                                in checked_receipts.items()))
                    if pure_receipt_replay:
                        return _batch_snapshot(batch), False

                    batch["until"] = now + lease

                def persist(kind: str | None = None,
                            job_id: str | None = None) -> None:
                    nonlocal old_bytes, last_observed, baseline_seed, \
                        progress_events
                    # A pending batch this run rewrites moves to the
                    # current shape (per-item receipts); untouched
                    # baseline batches keep their original bytes.
                    legacy_batches.discard(key)
                    if baseline_seed is not None:
                        # The first write of a pre-events ledger seeds
                        # the stream with one created baseline per
                        # recorded batch, frozen at the loaded state,
                        # ahead of this commit's own event.
                        for seed in baseline_seed:
                            progress_events.append({
                                "position": len(progress_events),
                                "at": seed["at"], "key": seed["key"],
                                "job": seed["job"], "kind": seed["kind"],
                                "batch": seed["batch"],
                            })
                        baseline_seed = None
                    snapshot = _canonical_batch(batch)
                    if kind is not None:
                        observable = True
                        if kind == _KIND_PROGRESS \
                                and last_observed is not None:
                            # A pure coordination-lease renewal changes
                            # only the batch until and appends no event;
                            # compare the member state against the last
                            # observed snapshot with the renewed until
                            # normalized away, so an equivalent replay or
                            # a wait that re-syncs the identical plan is
                            # stateless too.
                            normalized = dict(snapshot)
                            normalized["until"] = last_observed["until"]
                            observable = normalized != last_observed
                        if observable:
                            progress_events.append({
                                "position": len(progress_events),
                                "at": now, "key": key, "job": job_id,
                                "kind": kind, "batch": snapshot,
                            })
                    payload = _canonical_bytes(batches, audit_events,
                                               legacy_batches,
                                               progress_events)
                    _commit_file(coordination_real, payload, old_bytes)
                    old_bytes = payload
                    last_observed = snapshot

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
                    persist(_KIND_FAILED, item["job_id"])

                def sync_from_plan(item: dict[str, Any],
                                   plan: dict[str, Any]) -> None:
                    # The most recent item snapshot is the plan itself,
                    # carrying every actual step receipt -- including a
                    # non-ASCII credential -- never just a stage marker.
                    item["lease"] = plan["lease_end"]
                    item["snapshot"] = copy.deepcopy(plan)
                    item["receipts"] = copy.deepcopy(plan["steps"])

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
                            sync_from_plan(item, plan)
                        elif action == "record":
                            landed = snapshot.plans.get(plan_key)
                            saved = next(
                                (step for step
                                 in (landed["steps"] if landed is not None
                                     else ())
                                 if step["step"] == request["step"]), None)
                            if saved is not None:
                                if saved != {
                                        "step": request["step"],
                                        "result": request["result"],
                                        "receipt": request["receipt"],
                                        "at": request["at"]}:
                                    # A different result, credential or
                                    # moment for the same saved step is a
                                    # changed request: refuse it outright
                                    # and leave the coordination ledger's
                                    # bytes exactly as they were.
                                    raise _ChangedRequest(
                                        "a receipt already recorded for "
                                        "this step cannot change")
                                # The receipt already landed downstream in
                                # a crashed call while the coordination
                                # snapshot still held the marker: re-entry
                                # catches the snapshot up once, never
                                # issuing the original action a second
                                # time.
                                plan = landed
                            else:
                                plan, _ = downstream(
                                    lambda: _rebalance.record(
                                        *paths8, job_id, item["request_key"],
                                        request["owner"], request["step"],
                                        request["result"],
                                        request["receipt"], request["at"]))
                            item["phase"] = "active"
                            sync_from_plan(item, plan)
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
                            item["receipts"] = copy.deepcopy(plan["steps"])
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
                            settled_plan = snapshot.plans.get(
                                request["plan_key"])
                            item["receipts"] = copy.deepcopy(
                                settled_plan["steps"]
                                if settled_plan is not None else [])
                    except Exception as exc:
                        if isinstance(exc, _ChangedRequest):
                            # A changed request aborts the run with the
                            # coordination ledger untouched; it is a
                            # plain ValueError to the caller.
                            raise ValueError(str(exc)) from None
                        if _is_business_error(exc):
                            fail_member(item, exc)
                            return False
                        raise
                    # A record action lands the step's receipt; every
                    # other replayed action is plain member progress.
                    persist(_KIND_RECEIPT if action == "record"
                            else _KIND_PROGRESS, job_id)
                    return True

                # First commit: every member persisted in its starting
                # stage before any downstream interface is called. A new
                # batch emits the created event; a re-entry after a lease
                # expiry emits taken_over when the owner changed. Another
                # owner's re-entry only renews the coordination lease,
                # which appends no event at all (the member catch-ups
                # later in the run emit per-item progress); a pre-events
                # ledger still receives its baseline seed here.
                if created:
                    persist(_KIND_CREATED)
                elif takeover:
                    persist(_KIND_TAKEN_OVER)
                else:
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

                        next_step = (_STEPS[len(plan["steps"])]
                                     if len(plan["steps"]) < len(_STEPS)
                                     else None)
                        receipt = pending_receipts.get(item["job_id"])
                        if receipt is not None:
                            already = next(
                                (step for step in plan["steps"]
                                 if step["step"] == receipt["step"]), None)
                            if already is not None:
                                if already != {
                                        "step": receipt["step"],
                                        "result": receipt["result"],
                                        "receipt": receipt["receipt"],
                                        "at": receipt["at"]}:
                                    # The same step's result, credential or
                                    # moment changed: a changed request that
                                    # aborts the run with the coordination
                                    # bytes untouched, never a failed member.
                                    raise ValueError(
                                        "a receipt already recorded for this "
                                        "step cannot change")
                                # Fully equivalent to a step the plan
                                # already holds (including a duplicate after
                                # a crash between the downstream commit and
                                # the snapshot update): return the current
                                # progress without another record call and
                                # without failing the member.
                                pending_receipts.pop(item["job_id"], None)
                                sync_from_plan(item, plan)
                                persist(_KIND_RECEIPT, item["job_id"])
                                continue
                        if receipt is not None and next_step is not None \
                                and receipt["step"] == next_step:
                            item["request_key"] = _stable_key(
                                key, item["job_id"], "record:" + next_step)
                            item["snapshot"] = _marker(
                                "record", owner=plan["owner"],
                                step=next_step, result=receipt["result"],
                                receipt=receipt["receipt"], at=receipt["at"])
                            pending_receipts.pop(item["job_id"], None)
                            persist(_KIND_PROGRESS, item["job_id"])
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
                            persist(_KIND_PROGRESS, item["job_id"])
                            continue
                        if now <= plan["lease_end"]:
                            # No matching receipt and a still-valid
                            # lease: wait for a later run; the other
                            # members are independent and continue. A
                            # wait that re-syncs the identical plan is a
                            # stateless commit and appends no event.
                            sync_from_plan(item, plan)
                            persist(_KIND_PROGRESS, item["job_id"])
                            break
                        # Strict expiry: persist the recovery action and
                        # parameters before the existing recover
                        # interface is called. Recovery on re-entry
                        # replays this exact moment.
                        item["request_key"] = _stable_key(
                            key, item["job_id"], "recover")
                        item["snapshot"] = _marker(
                            "recover", owner=plan["owner"], at=now)
                        persist(_KIND_PROGRESS, item["job_id"])

                if all(member["phase"] in _ITEM_TERMINAL
                       for member in batch["items"].values()):
                    batch["status"] = "completed"
                    # The closing audit event is appended at its
                    # physical end: the audit order is the real
                    # completion order, not the batch key order. The
                    # progress stream records the same terminalization.
                    audit_events.append({
                        "key": key,
                        "at": now,
                        "batch": _canonical_batch(batch),
                    })
                    if baseline_seed is not None:
                        # A pre-events ledger whose first write already
                        # closes a batch still seeds every earlier batch
                        # before this completion event.
                        for seed in baseline_seed:
                            progress_events.append({
                                "position": len(progress_events),
                                "at": seed["at"], "key": seed["key"],
                                "job": seed["job"], "kind": seed["kind"],
                                "batch": seed["batch"],
                            })
                        baseline_seed = None
                    progress_events.append({
                        "position": len(progress_events),
                        "at": now, "key": key, "job": None,
                        "kind": _KIND_COMPLETED,
                        "batch": _canonical_batch(batch),
                    })
                    payload = _canonical_bytes(batches, audit_events,
                                               legacy_batches,
                                               progress_events)
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


# ---------------------------------------------------------------------------
# Read-only lookup and pagination
# ---------------------------------------------------------------------------

_DEFAULT_LIMIT = 100
_MAX_LIMIT = 1000


def _validate_cursor_limit(cursor: object, limit: object) -> None:
    if cursor is not None and (not isinstance(cursor, str) or not cursor):
        raise ValueError("cursor must be None or a non-empty string")
    # bool is a subclass of int and must be rejected as a page size.
    if not isinstance(limit, int) or isinstance(limit, bool) \
            or not 1 <= limit <= _MAX_LIMIT:
        raise ValueError("limit must be an integer between 1 and 1000")


@contextlib.contextmanager
def _business_locks(inputs: dict[str, str]) -> Iterator[None]:
    # Shared flocks on the business ledgers in resolved real-path order,
    # the same order the writer takes them in. A ledger that does not
    # exist is not flocked (which would create its companion lock file);
    # _Snapshot rejects the broken reference itself.
    held: list[Any] = []
    try:
        for input_real in sorted(set(inputs.values())):
            if not os.path.exists(input_real):
                continue
            manager = _lock(input_real, shared=True)
            manager.__enter__()
            held.append(manager)
        yield
    finally:
        while held:
            manager = held.pop()
            with contextlib.suppress(BaseException):
                manager.__exit__(None, None, None)


def _read_raw(realpath: str) -> bytes:
    with open(realpath, "rb") as handle:
        return handle.read()


def _completion(audit: list[dict[str, Any]],
                batch: dict[str, Any]) -> dict[str, int] | None:
    # An active batch carries no completion information; a terminal one
    # publishes its zero-based position in the append-only audit (the
    # real completion order) and its completion moment.
    if batch["status"] != "completed":
        return None
    for index, event in enumerate(audit):
        if event["key"] == batch["key"]:
            return {"index": index, "at": event["at"]}
    raise ValueError("completed batch is missing its audit event")


def _entry(batch: dict[str, Any], audit: list[dict[str, Any]]) -> dict[str, Any]:
    return {"key": batch["key"], "snapshot": _batch_snapshot(batch),
            "completion": _completion(audit, batch)}


def _page(batches: dict[str, dict[str, Any]],
          audit: list[dict[str, Any]], cursor: str | None,
          limit: int) -> dict[str, Any]:
    # The persisted batches are validated in ascending key code-point
    # order; sorted() reproduces that document order. One extra match is
    # collected to learn whether the page is the last; the cursor is
    # exclusive.
    entries: list[dict[str, Any]] = []
    for batch_key in sorted(batches):
        if cursor is not None and batch_key <= cursor:
            continue
        entries.append(_entry(batches[batch_key], audit))
        if len(entries) > limit:
            break
    if len(entries) > limit:
        page = entries[:limit]
        next_cursor: str | None = page[-1]["key"]
    else:
        page = entries
        next_cursor = None
    return {"entries": page, "next": next_cursor}


def _render(payload: Any) -> bytes:
    # The endpoint's compact UTF-8 JSON: compact separators, non-ASCII
    # written through, no trailing newline.
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def get(ledger: str, key: str) -> dict[str, object]:
    """Return a copy of one batch without writing anything.

    ``ledger`` is the coordination ledger path and ``key`` the batch
    key; both must be non-empty strings.

    An existing coordination file first has to prove its *own* format --
    UTF-8, finite JSON, version 1, the fixed root/batch/item/audit field
    sets and order, key sorting and the exact canonical compact bytes --
    before the target key is ever looked up. Any encoding, JSON,
    version, field-order or canonical-byte violation therefore raises
    ``ValueError`` even when ``key`` is absent; an unknown key can never
    mask a malformed file. Only a missing ledger or a canonical ledger
    that genuinely lacks the key raises ``KeyError``.

    Once the format and the key are established without a lock, the
    coordination ledger and the batch's nine business ledgers are read
    under shared locks and every cross-reference is revalidated, so a
    broken reference raises ``ValueError`` and a missing required
    business ledger :class:`FileNotFoundError`. A miss never creates the
    ledger, a lock file, a directory or any other trace: both miss
    decisions are made by unlocked reads before a companion lock file is
    opened (``os.open`` would create it), and a missing business ledger
    is refused without creating its lock file either.
    """
    for value in (ledger, key):
        if not isinstance(value, str) or not value:
            raise ValueError("ledger and key must be non-empty strings")
    realpath = os.path.realpath(ledger)
    # A read-only miss must leave no trace behind, so the coordination
    # ledger is read unlocked first and its whole self-format is proven
    # before the key membership decision. Writes land through an atomic
    # replace, so an unlocked read only ever sees a complete document.
    try:
        preliminary = _read_raw(realpath)
    except FileNotFoundError:
        raise KeyError(key)
    # Format first, membership second: a malformed ledger is a
    # ValueError that an unknown key must never hide. This pass opens no
    # business ledger and takes no lock.
    intrinsic, _audit, _events_unused, _inputs_unused, _legacy, \
        _legacy_doc = _parse_coordination_bytes(
        realpath, preliminary, {}, None, require_references=False)
    if key not in intrinsic:
        raise KeyError(key)

    store = _get_store(realpath)
    with store.lock:
        with _lock(realpath, shared=True):
            try:
                raw = _read_raw(realpath)
            except FileNotFoundError:
                # Lost to a concurrent removal before the lock: still a
                # trace-free miss.
                raise KeyError(key)
            # Re-prove the format under the lock before touching a
            # business ledger; a concurrent replacement may have changed
            # the document.
            locked_batches, _audit, _events, locked_inputs, _legacy, \
                _legacy_doc = _parse_coordination_bytes(
                    realpath, raw, {}, None, require_references=False)
            if key not in locked_batches:
                raise KeyError(key)
            with _business_locks(locked_inputs):
                snapshot = _Snapshot(locked_inputs)
                batches, _audit_events, _progress, _ledger_inputs, _raw, \
                    _legacy, _legacy_doc = _load_coordination(
                        realpath,
                        {frozenset(snapshot.input_map.items()): snapshot},
                        None)
                return _batch_snapshot(batches[key])


def search(ledger: str, cursor: str | None = None,
           limit: int = _DEFAULT_LIMIT) -> dict[str, Any]:
    """Return one page of batch snapshots in key code-point order.

    ``ledger`` must be a non-empty string. ``cursor`` is ``None`` or a
    non-empty string -- it need not name an existing batch -- and only
    batches whose key is strictly greater than it in code-point order
    are considered. ``limit`` is the page size: a non-boolean integer
    from 1 to 1000, defaulting to 100. Anything else raises
    ``ValueError`` before a file is read.

    The page object is ``{"entries": [entry, ...], "next": cursor}`` in
    that order; each entry carries, in order, ``key``, the complete
    batch ``snapshot`` and ``completion``, which is ``null`` for an
    active batch and ``{"index": i, "at": t}`` for a terminal one -- the
    batch's zero-based position in the append-only audit (the real
    completion order) and its completion moment. ``next`` is the key of
    the page's last entry when further batches remain, else ``None``.

    The query is strictly read-only and, like :func:`get`, proves the
    coordination ledger's own format before its business references are
    followed. A missing coordination ledger raises
    :class:`FileNotFoundError`; malformed, out-of-order or non-canonical
    bytes or a broken cross-reference raise ``ValueError`` and a missing
    required business ledger :class:`FileNotFoundError`; any other
    locking or I/O failure raises ``OSError``.
    """
    if not isinstance(ledger, str) or not ledger:
        raise ValueError("ledger must be a non-empty string")
    _validate_cursor_limit(cursor, limit)
    realpath = os.path.realpath(ledger)
    # Format before locks: an unlocked atomic read only ever sees a
    # complete document, and a malformed ledger never reaches a business
    # ledger or creates a companion lock file.
    preliminary = _read_raw(realpath)
    _parse_coordination_bytes(realpath, preliminary, {}, None,
                              require_references=False)

    store = _get_store(realpath)
    with store.lock:
        with _lock(realpath, shared=True):
            raw = _read_raw(realpath)
            _locked_batches, _audit, _events, inputs, _legacy, \
                _legacy_doc = _parse_coordination_bytes(
                    realpath, raw, {}, None, require_references=False)
            with _business_locks(inputs):
                snapshot = _Snapshot(inputs)
                batches, audit_events, _progress, _ledger_inputs, _raw, \
                    _legacy, _legacy_doc = _load_coordination(
                        realpath,
                        {frozenset(snapshot.input_map.items()): snapshot},
                        None)
            return _page(batches, audit_events, cursor, limit)


def _snapshot_checked(inputs: dict[str, str]) -> _Snapshot:
    # Build the business snapshot, classifying a missing required
    # business ledger as a broken reference the canonical coordination
    # document makes (ValueError -> 409 migration_batches_invalid) rather
    # than an absent file of the endpoint's own (FileNotFoundError ->
    # 404). The six mandatory ledgers are the acceptance, supply, signal,
    # clearing, dispatch and execution files; the advice, intent and
    # settlement ledgers may legitimately be absent and load as empty.
    try:
        return _Snapshot(inputs)
    except FileNotFoundError as exc:
        raise ValueError(
            "migration coordination ledger references a missing business "
            "ledger") from exc


def get_response(ledger: str, key: str) -> bytes:
    """Serialize :func:`get`'s snapshot while every read lock is held.

    The shared locks cover the coordination ledger, the nine related
    business snapshots and the response serialization, so a concurrent
    writer is observed as either the complete old or the complete new
    version, never a mix. A missing coordination ledger raises
    :class:`FileNotFoundError`; an unknown key raises ``KeyError``;
    malformed bytes, a broken reference or a missing required business
    ledger raise ``ValueError``; any other locking or I/O failure raises
    ``OSError``.
    """
    for value in (ledger, key):
        if not isinstance(value, str) or not value:
            raise ValueError("ledger and key must be non-empty strings")
    realpath = os.path.realpath(ledger)
    try:
        preliminary = _read_raw(realpath)
    except FileNotFoundError:
        raise _CoordinationMissing(realpath)
    intrinsic, _audit, _events_unused, _inputs, _legacy, _legacy_doc = \
        _parse_coordination_bytes(
            realpath, preliminary, {}, None, require_references=False)
    if key not in intrinsic:
        raise KeyError(key)
    store = _get_store(realpath)
    with store.lock:
        with _lock(realpath, shared=True):
            try:
                raw = _read_raw(realpath)
            except FileNotFoundError:
                # Lost to a concurrent removal before the lock.
                raise _CoordinationMissing(realpath)
            batches_locked, _audit, _events, inputs_locked, _legacy, \
                _legacy_doc = _parse_coordination_bytes(
                    realpath, raw, {}, None, require_references=False)
            if key not in batches_locked:
                raise KeyError(key)
            with _business_locks(inputs_locked):
                snapshot = _snapshot_checked(inputs_locked)
                batches, _audit_events, _progress, _ledger_inputs, _raw, \
                    _legacy, _legacy_doc = _load_coordination(
                        realpath,
                        {frozenset(snapshot.input_map.items()): snapshot},
                        None)
                # Serialized before a single shared lock is released.
                return _render(_batch_snapshot(batches[key]))


def search_response(ledger: str, cursor: str | None = None,
                    limit: int = _DEFAULT_LIMIT) -> bytes:
    """Serialize :func:`search`'s page while every read lock is held.

    The page is formed and rendered under the coordination ledger's and
    the business ledgers' shared locks, so a racing writer is observed
    as one complete version. A missing coordination ledger raises
    :class:`FileNotFoundError`; bad arguments, malformed bytes, a broken
    cross-reference or a missing required business ledger raise
    ``ValueError``; other locking or I/O failures raise ``OSError``.
    """
    if not isinstance(ledger, str) or not ledger:
        raise ValueError("ledger must be a non-empty string")
    _validate_cursor_limit(cursor, limit)
    realpath = os.path.realpath(ledger)
    preliminary = _read_raw(realpath)
    _parse_coordination_bytes(realpath, preliminary, {}, None,
                              require_references=False)
    store = _get_store(realpath)
    with store.lock:
        with _lock(realpath, shared=True):
            raw = _read_raw(realpath)
            _batches_locked, _audit, _events_locked, inputs, _legacy, \
                _legacy_doc = _parse_coordination_bytes(
                    realpath, raw, {}, None, require_references=False)
            with _business_locks(inputs):
                snapshot = _snapshot_checked(inputs)
                batches, audit_events, _progress, _ledger_inputs, _raw, \
                    _legacy, _legacy_doc = _load_coordination(
                        realpath,
                        {frozenset(snapshot.input_map.items()): snapshot},
                        None)
                page = _page(batches, audit_events, cursor, limit)
                return _render(page)


# ---------------------------------------------------------------------------
# Incremental progress event stream
# ---------------------------------------------------------------------------


def _validate_events_arguments(
    cursor: object, limit: object, key: object, job: object,
) -> None:
    # The position cursor is exclusive: None reads from the start, a
    # non-boolean non-negative integer considers only greater positions.
    if cursor is not None and (not _is_plain_int(cursor) or cursor < 0):
        raise ValueError("cursor must be None or a non-boolean "
                         "non-negative integer")
    if not isinstance(limit, int) or isinstance(limit, bool) \
            or not 1 <= limit <= _MAX_LIMIT:
        raise ValueError("limit must be an integer between 1 and 1000")
    for name, value in (("key", key), ("job", job)):
        if value is not None and (not isinstance(value, str) or not value):
            raise ValueError(f"{name} must be None or a non-empty string")


def _event_copy(event: dict[str, Any]) -> dict[str, Any]:
    return {field: copy.deepcopy(event[field]) for field
            in _PROGRESS_EVENT_FIELDS}


def _events_page(
    progress_events: list[dict[str, Any]],
    cursor: int | None,
    limit: int,
    key: str | None,
    job: str | None,
) -> dict[str, Any]:
    # Filters combine by logical AND and unmatched events do not occupy
    # page capacity; one extra match reveals whether another page
    # remains. The cursor is an exclusive position.
    matches: list[dict[str, Any]] = []
    for event in progress_events:
        if cursor is not None and event["position"] <= cursor:
            continue
        if key is not None and event["key"] != key:
            continue
        if job is not None and event["job"] != job:
            continue
        matches.append(event)
        if len(matches) > limit:
            break
    if len(matches) > limit:
        page = matches[:limit]
        next_cursor: int | None = page[-1]["position"]
    else:
        page = matches
        next_cursor = None
    return {"events": [_event_copy(event) for event in page],
            "next": next_cursor}


def events(ledger: str, cursor: int | None = None,
           limit: int = _DEFAULT_LIMIT, key: str | None = None,
           job: str | None = None) -> dict[str, Any]:
    """Return one page of incremental progress events by position.

    ``ledger`` must be a non-empty string. ``cursor`` is ``None`` (read
    from position 0) or a non-boolean non-negative integer; only events
    at strictly greater positions are considered. ``limit`` is a
    non-boolean integer from 1 to 1000, defaulting to 100. ``key`` and
    ``job`` are optional non-empty strings filtering by batch key and
    member job; the filters combine by logical AND and unmatched events
    do not occupy page capacity. Bad arguments raise ``ValueError``
    before a file is read.

    The page object is ``{"events": [event, ...], "next": cursor}`` in
    that order; each event carries, in order, ``position`` (a
    continuous, zero-based, strictly increasing position that is never
    reused), ``at`` (the commit moment), ``key`` (the batch), ``job``
    (the member job, or null for batch-wide changes), ``kind``
    (``created``, ``taken_over``, ``progress``, ``receipt``, ``failed``
    or ``completed``) and the complete post-commit ``batch`` snapshot.
    ``next`` is the page's last event position when more matches
    remain, else ``None``.

    A pre-events ledger reads as an empty stream and is never
    rewritten. A missing coordination ledger raises
    :class:`FileNotFoundError`; malformed or non-canonical bytes, a bad
    cursor or filter, a broken cross-reference or a missing required
    business ledger raise ``ValueError``; other locking or I/O failures
    raise ``OSError``.
    """
    if not isinstance(ledger, str) or not ledger:
        raise ValueError("ledger must be a non-empty string")
    _validate_events_arguments(cursor, limit, key, job)
    realpath = os.path.realpath(ledger)
    preliminary = _read_raw(realpath)
    _parse_coordination_bytes(realpath, preliminary, {}, None,
                              require_references=False)

    store = _get_store(realpath)
    with store.lock:
        with _lock(realpath, shared=True):
            raw = _read_raw(realpath)
            _batches, _audit, _events, inputs, _legacy, _legacy_doc = \
                _parse_coordination_bytes(
                    realpath, raw, {}, None, require_references=False)
            with _business_locks(inputs):
                snapshot = _Snapshot(inputs)
                _batches, _audit, progress_events, _ledger_inputs, _raw, \
                    _legacy_b, _legacy_doc = _load_coordination(
                        realpath,
                        {frozenset(snapshot.input_map.items()): snapshot},
                        None)
            return _events_page(progress_events, cursor, limit, key, job)


def events_response(ledger: str, cursor: int | None = None,
                    limit: int = _DEFAULT_LIMIT, key: str | None = None,
                    job: str | None = None) -> bytes:
    """Serialize :func:`events`' page while every read lock is held.

    The coordination ledger's shared lock and the nine business
    ledgers' shared locks cover validation, filtering, page formation
    and serialization, so a racing batch writer is observed as one
    complete version. A missing coordination ledger raises
    :class:`FileNotFoundError`; bad arguments, malformed or
    non-canonical bytes, a broken cross-reference or a missing
    required business ledger raise ``ValueError``; other locking or I/O
    failures raise ``OSError``.
    """
    if not isinstance(ledger, str) or not ledger:
        raise ValueError("ledger must be a non-empty string")
    _validate_events_arguments(cursor, limit, key, job)
    realpath = os.path.realpath(ledger)
    preliminary = _read_raw(realpath)
    _parse_coordination_bytes(realpath, preliminary, {}, None,
                              require_references=False)
    store = _get_store(realpath)
    with store.lock:
        with _lock(realpath, shared=True):
            raw = _read_raw(realpath)
            _batches, _audit, _events, inputs, _legacy, _legacy_doc = \
                _parse_coordination_bytes(
                    realpath, raw, {}, None, require_references=False)
            with _business_locks(inputs):
                snapshot = _snapshot_checked(inputs)
                _batches, _audit, progress_events, _ledger_inputs, _raw, \
                    _legacy_b, _legacy_doc = _load_coordination(
                        realpath,
                        {frozenset(snapshot.input_map.items()): snapshot},
                        None)
                page = _events_page(progress_events, cursor, limit, key, job)
                return _render(page)
