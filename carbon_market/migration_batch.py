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
``batches``, an append-only ``audit`` and, once upgraded, an
append-only ``events``) is one compact UTF-8 JSON document with
non-ASCII written through, decimal integers and exactly one trailing
newline. The audit is appended in the real order batches complete and
is never re-sorted by batch key; each completed batch has exactly one
event whose snapshot equals that batch's complete state at completion.
Ledgers written without the per-item receipts field still load and
replay byte-for-byte; a batch a run rewrites gains that field.

The ``events`` section is the incremental progress stream: the
baseline published only batch snapshots, keyed pagination and the
per-batch completion audit, which a fresh observer could only poll.
Every durable commit that changes an observable batch state appends
one progress event here -- creation, lease takeover, an item's phase
or saved error, a receipt landing in the item snapshot and batch
closure. Each event stores, in fixed order, its strictly increasing
zero-based position (never reused), the commit moment, the batch key,
the related member's job id (``null`` for batch-scoped changes), the
change category and the complete batch snapshot as it stood after
that commit. Equivalent replays, pure lease renewals, waiting runs
and other commits that change no observable batch state append no
event. Ledgers written by the baseline without an ``events`` section
stay read-only compatible and keep replaying byte-for-byte; the first
later write upgrades the document and seeds the stream with one
synthesized baseline ``completed`` event per existing audit entry, in
audit order, before this run's own events.

Persistent consumers of the progress stream -- claims, pulls, acks,
rejects and dead letters over the events published here, plus the
read-only subscription and status queries -- are the
:mod:`carbon_market.migration_consumer` module's domain, built on this
module's narrow read interface for coordination event snapshots (the
locked parse, validation and business-snapshot primitives below). This
module stays the compatibility entry point and re-exports the consumer
public surface (:func:`consume`, :func:`consume_response`,
:func:`consumer_subscription`, :func:`consumer_status`,
:func:`consumer_status_response`, :func:`consumer_dead_letters`,
:func:`consumer_dead_letters_response` and the consumer exceptions)
unchanged; see :mod:`carbon_market.migration_consumer` for their
semantics.
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

__all__ = ["run", "get", "search", "get_response", "search_response",
           "events", "events_response", "consume", "consume_response",
           "consumer_subscription", "consumer_status",
           "consumer_status_response", "consumer_dead_letters",
           "consumer_dead_letters_response", "ConsumerLedgerInvalid",
           "ConsumerOwnershipError", "ConsumerLeaseExpired",
           "CoordinationLedgerInvalid", "CoordinationLedgerMissing",
           "ConsumersLedgerMissing"]

_VERSION = 1
_ROOT_FIELDS = ("version", "batches", "audit")
# The incremental events section is appended last and optional on
# read: baseline ledgers carry no such section and keep validating and
# replaying byte-for-byte until the first later write upgrades them.
_ROOT_EVENTS_FIELD = "events"
_ROOT_FIELDS_WITH_EVENTS = _ROOT_FIELDS + (_ROOT_EVENTS_FIELD,)
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
# Incremental progress events. Each durable commit whose observable
# batch state changed appends exactly one event in one of these
# categories: batch creation, an ownership takeover after lease
# expiry, an item phase transition or saved error, a step receipt
# landing in the item snapshot, and batch closure. The event's fixed
# fields are the contiguous zero-based position, the commit moment,
# the batch key, the related member job id (null for batch-scoped
# changes), the category and the post-commit batch snapshot.
_PROGRESS_FIELDS = ("position", "at", "key", "job_id", "category",
                    "batch")
_PROGRESS_CATEGORIES = ("created", "taken_over", "item", "receipt",
                        "completed")
_PROGRESS_BATCH_SCOPED = ("created", "taken_over", "completed")
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


class CoordinationLedgerMissing(FileNotFoundError):
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
                 old_bytes: bytes | None,
                 prefix: str = _PREFIX) -> None:
    # One durable commit through a synced same-directory temporary file,
    # an atomic replace and a directory fsync, restoring the pre-call
    # bytes (and clearing the leftover fragment) on any failure. The
    # consumer ledger passes its own prefix so its fragments never look
    # like a coordination commit's.
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
                reals["trades"], accepted, history, signal_history,
                completion_paths=_rebalance._market.
                _discover_completion_paths(tuple(sorted(reals.values()))))
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


def _validate_progress_event(
    raw: object,
    position: int,
    batches: dict[str, dict[str, Any]],
    ledger_inputs: dict[str, str],
    *,
    require_recorded: bool = True,
) -> dict[str, Any]:
    # One incremental progress event, validated intrinsically: its
    # batch is a historical snapshot taken right after that commit, so
    # it must carry every field in canonical order and arity but it is
    # never re-followed into the business ledgers, whose plans and
    # bindings have legitimately advanced since. A dead letter embeds
    # one such event in the independent consumer ledger and passes
    # require_recorded=False: it is form-checked the same way but does
    # not have to reference a batch recorded beside it or share a
    # business-ledger set, since the consumer ledger only binds the
    # coordination ledger's path.
    if not isinstance(raw, dict) \
            or set(raw.keys()) != set(_PROGRESS_FIELDS):
        raise ValueError("migration progress event has invalid fields")
    if not _is_plain_int(raw["position"]) or raw["position"] != position:
        raise ValueError("migration progress event positions must be "
                         "contiguous and zero-based")
    at = raw["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("migration progress event moment must be a "
                         "non-boolean non-negative integer")
    event_key = raw["key"]
    if not isinstance(event_key, str) or not event_key:
        raise ValueError("migration progress event key must be a "
                         "non-empty string")
    if require_recorded and event_key not in batches:
        raise ValueError("migration progress event must reference a "
                         "recorded batch")
    category = raw["category"]
    if category not in _PROGRESS_CATEGORIES:
        raise ValueError("migration progress event category is invalid")
    job_id = raw["job_id"]
    if job_id is not None and (not isinstance(job_id, str) or not job_id):
        raise ValueError("migration progress event job id must be null "
                         "or a non-empty string")
    if category in _PROGRESS_BATCH_SCOPED:
        if job_id is not None:
            raise ValueError("a batch-scoped progress event must not "
                             "name a job id")
    elif job_id is None:
        raise ValueError("an item progress event must name its job id")
    event_batch_raw = raw["batch"]
    if not isinstance(event_batch_raw, dict) \
            or set(event_batch_raw.keys()) != set(_BATCH_FIELDS):
        raise ValueError("progress event batch has invalid fields")
    if event_batch_raw["key"] != event_key:
        raise ValueError("progress event batch key does not match its "
                         "event key")
    inputs = _validate_inputs(event_batch_raw["inputs"])
    if require_recorded and inputs != ledger_inputs:
        raise ValueError("progress event batch must share the ledger's "
                         "business ledger set")
    owner = event_batch_raw["owner"]
    if not isinstance(owner, str) or not owner:
        raise ValueError("progress event batch owner must be a "
                         "non-empty string")
    until = event_batch_raw["until"]
    if not _is_plain_int(until) or until < 1:
        raise ValueError("progress event batch until must be a positive "
                         "integer")
    status = event_batch_raw["status"]
    if status not in _BATCH_STATUSES:
        raise ValueError("progress event batch status is invalid")
    if category == "completed":
        if status != "completed":
            raise ValueError("a completed progress event must snapshot "
                             "a completed batch")
    elif status != "pending":
        raise ValueError("a non-closing progress event must snapshot a "
                         "pending batch")
    items_raw = event_batch_raw["items"]
    if not isinstance(items_raw, dict):
        raise ValueError("progress event batch items must be an object")
    _check_sorted_keys(items_raw, "progress event items")
    event_items: dict[str, dict[str, Any]] = {}
    for member_key, item_raw in items_raw.items():
        # Historical snapshots always carry the current per-item
        # receipts arity; the stream is never written in baseline shape.
        item, item_legacy = _validate_item(
            item_raw, member_key, None, references=False)
        if item_legacy:
            raise ValueError("progress event items must carry the "
                             "receipts field")
        event_items[member_key] = item
    if job_id is not None and job_id not in event_items:
        raise ValueError("progress event job id must name one of its "
                         "batch's items")
    event_batch = {
        "key": event_key,
        "owner": owner,
        "until": until,
        "status": status,
        "inputs": inputs,
        "items": event_items,
    }
    if event_batch_raw != _canonical_batch(event_batch):
        raise ValueError("progress event batch is not in canonical form")
    return {"position": position, "at": at, "key": event_key,
            "job_id": job_id, "category": category,
            "batch": event_batch}


def _validate_progress(
    raw: object,
    batches: dict[str, dict[str, Any]],
    ledger_inputs: dict[str, str],
) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        raise ValueError("migration progress events must be a list")
    progress: list[dict[str, Any]] = []
    # Per-batch sequence invariants. A batch that completed before the
    # stream was upgraded is legitimately known only by its seeded
    # closing event, so a creation event is not mandatory; but a
    # creation, when present, must be the batch's first event, and a
    # completed batch's last event must be the immutable closing
    # snapshot equal to the batch as it now stands.
    seen_keys: set[str] = set()
    last_by_key: dict[str, dict[str, Any]] = {}
    for position, event_raw in enumerate(raw):
        event = _validate_progress_event(
            event_raw, position, batches, ledger_inputs)
        event_key = event["key"]
        if event_key in seen_keys:
            if event["category"] == "created":
                raise ValueError("a batch creation may be recorded only "
                                 "once")
        elif event["category"] not in ("created", "completed",
                                        "taken_over"):
            raise ValueError("a batch's first progress event must be its "
                             "creation, a takeover or its seeded "
                             "completion")
        seen_keys.add(event_key)
        if last_by_key.get(event_key, {}).get("category") == "completed":
            raise ValueError("no progress event may follow a batch's "
                             "completion")
        last_by_key[event_key] = event
        progress.append(event)
    for event_key, event in last_by_key.items():
        batch = batches[event_key]
        if batch["status"] == "completed":
            if event["category"] != "completed":
                raise ValueError("a completed batch must end its progress "
                                 "stream with a completed event")
            if event["batch"] != _canonical_batch(batch):
                raise ValueError("the completed progress snapshot must "
                                 "equal the completed batch")
    return progress


def _validate_ledger(
    data: object,
    snapshots: dict[frozenset[tuple[str, str]], _Snapshot],
    default_inputs: dict[str, str] | None = None,
    require_references: bool = True,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]],
           list[dict[str, Any]], dict[str, str], set[str], bool]:
    if not isinstance(data, dict) \
            or set(data.keys()) not in (set(_ROOT_FIELDS),
                                        set(_ROOT_FIELDS_WITH_EVENTS)):
        raise ValueError("migration coordination ledger root must be an "
                         "object with version, batches, audit and "
                         "optionally events")
    document_legacy = _ROOT_EVENTS_FIELD not in data
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
    # The incremental stream is optional: a baseline document ends
    # after the audit and stays valid byte-for-byte. Once present it is
    # fully validated -- contiguous positions, per-batch ordering and
    # the immutable closing snapshot.
    progress: list[dict[str, Any]] = []
    if not document_legacy:
        progress = _validate_progress(
            data[_ROOT_EVENTS_FIELD], batches, ledger_inputs)
        progress_keys = {event["key"] for event in progress}
        if not completed.issubset(progress_keys):
            raise ValueError("every completed batch must appear in the "
                             "progress event stream")
    return (batches, audit, progress, ledger_inputs, legacy_batches,
            document_legacy)


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
    # One progress event in fixed field order; its batch snapshot is
    # always written in the current per-item arity, even when the
    # closing audit entry it was seeded from predates the receipts
    # field.
    return {"position": event["position"], "at": event["at"],
            "key": event["key"], "job_id": event["job_id"],
            "category": event["category"],
            "batch": _canonical_batch(event["batch"])}


def _canonical_bytes(
    batches: dict[str, dict[str, Any]],
    audit: list[dict[str, Any]],
    progress: list[dict[str, Any]] | None = None,
    legacy_batches: set[str] | None = None,
    document_legacy: bool = False,
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, sections in their
    # fixed field order, batches and their items code-point sorted and
    # the audit appended in the physical order in which the batches
    # actually completed (never re-sorted by batch key), terminated by
    # exactly one newline. Baseline batches without the per-item
    # receipts field are reproduced field-for-field; every batch a run
    # (re)writes carries that field. A baseline document carries no
    # events section; the first new write appends it after the audit
    # with one section per root field in fixed order.
    legacy_batches = legacy_batches or set()
    payload = {
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
    if not document_legacy:
        payload[_ROOT_EVENTS_FIELD] = [_canonical_event(event)
                                       for event in (progress or [])]
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
    batches, audit, progress, inputs, legacy_batches, document_legacy = \
        _validate_ledger(
            data, snapshots, default_inputs,
            require_references=require_references)
    if raw != _canonical_bytes(batches, audit, progress, legacy_batches,
                               document_legacy):
        raise ValueError(
            f"migration coordination ledger {realpath!r} is not in "
            "canonical compact form")
    return (batches, audit, progress, inputs, legacy_batches,
            document_legacy)


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
        return {}, [], [], default_inputs, None, set(), True
    batches, audit, progress, inputs, legacy_batches, document_legacy = \
        _parse_coordination_bytes(
            realpath, raw, snapshots, default_inputs,
            require_references=require_references)
    return (batches, audit, progress, inputs, raw, legacy_batches,
            document_legacy)


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
            input_reals = set(reals[name] for name in _INPUT_NAMES)
            input_reals.update(_rebalance._market._discover_completion_paths(
                tuple(sorted(input_reals))))
            for real in sorted(input_reals):
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
                    old_bytes, legacy_batches, document_legacy = \
                    _load_coordination(
                        coordination_real, snapshot_map, snapshot.input_map)

                batch = batches.get(key)
                created = batch is None
                # A takeover of a batch a pre-stream ledger recorded as
                # pending has no creation event yet; its first upgraded
                # event must still be named for what actually happened.
                first_event_is_takeover = False
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
                        if document_legacy:
                            # The upgrade seeds only completions, so this
                            # pending batch has no stream entry yet; its
                            # first event records the takeover itself.
                            first_event_is_takeover = True
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

                # A baseline ledger carries no progress section. The
                # first write of this run upgrades the document and
                # seeds the stream with the already-recorded history:
                # one completed event per audit entry, in the audit's
                # real completion order. Pending baseline batches get
                # their first event from the first commit that touches
                # them below. Read-only calls never reach this point
                # and keep reproducing the baseline bytes.
                if document_legacy:
                    for audit_event in audit_events:
                        progress_events.append({
                            "position": len(progress_events),
                            "at": audit_event["at"],
                            "key": audit_event["key"],
                            "job_id": None,
                            "category": "completed",
                            "batch": _canonical_batch(audit_event["batch"]),
                        })
                    document_legacy = False

                # The post-commit snapshot recorded in each batch's
                # latest progress event; the diff against it decides
                # whether a commit earns an event and which category.
                last_progress_by_key: dict[str, dict[str, Any]] = {
                    event["key"]: event for event in progress_events}

                def append_progress() -> None:
                    # Append exactly one event for this commit when it
                    # changed an observable batch state. A commit whose
                    # batch snapshot equals its latest event's snapshot
                    # appends nothing -- that is the pure coordination
                    # lease renewal, an equivalent replay and a wait on
                    # an unchanged plan.
                    current = _canonical_batch(batch)
                    previous = last_progress_by_key.get(key)
                    if previous is not None \
                            and current == previous["batch"]:
                        return
                    if current["status"] == "completed":
                        category = "completed"
                        event_job: str | None = None
                    elif previous is None and first_event_is_takeover:
                        # A pending batch a baseline ledger recorded and
                        # whose first upgraded write is an expired-lease
                        # takeover opens its stream with the takeover.
                        category = "taken_over"
                        event_job = None
                    elif previous is None:
                        category = "created"
                        event_job = None
                    elif current["owner"] != previous["batch"]["owner"]:
                        # The run-start commit of an expired-lease
                        # takeover changes the owner and nothing else.
                        category = "taken_over"
                        event_job = None
                    elif current["items"] == previous["batch"]["items"]:
                        # Only the coordination lease window moved:
                        # a renewal, not an observable batch change.
                        return
                    else:
                        old_items = previous["batch"]["items"]
                        changed_jobs = [
                            member for member in sorted(
                                set(old_items) | set(current["items"]))
                            if old_items.get(member)
                            != current["items"][member]]
                        event_job = changed_jobs[0]
                        old_receipts = old_items.get(event_job, {}) \
                            .get("receipts", [])
                        new_receipts = current["items"][event_job]["receipts"]
                        # A commit that extends the item's saved
                        # receipts by exactly the next equivalent step
                        # is a receipt landing; every other item change
                        # (a phase move, a saved error, a marker) is an
                        # item event.
                        if len(new_receipts) == len(old_receipts) + 1 \
                                and new_receipts[:len(old_receipts)] \
                                == old_receipts:
                            category = "receipt"
                        else:
                            category = "item"
                    event = {
                        "position": len(progress_events),
                        "at": now,
                        "key": key,
                        "job_id": event_job,
                        "category": category,
                        "batch": current,
                    }
                    progress_events.append(event)
                    last_progress_by_key[key] = event

                def persist() -> None:
                    nonlocal old_bytes
                    # A pending batch this run rewrites moves to the
                    # current shape (per-item receipts); untouched
                    # baseline batches keep their original bytes.
                    legacy_batches.discard(key)
                    append_progress()
                    payload = _canonical_bytes(
                        batches, audit_events, progress_events,
                        legacy_batches, document_legacy)
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
                                persist()
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
                            sync_from_plan(item, plan)
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
                    # The closing event is appended at its physical end:
                    # the audit order is the real completion order, not
                    # the batch key order.
                    audit_events.append({
                        "key": key,
                        "at": now,
                        "batch": _canonical_batch(batch),
                    })
                    # The closing commit earns its own terminal event
                    # before the bytes are formed and validated.
                    append_progress()
                    payload = _canonical_bytes(
                        batches, audit_events, progress_events,
                        legacy_batches, document_legacy)
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
    # _Snapshot rejects the broken reference itself. Completion ledgers
    # beside the business ledgers complete the trades ledger's capacity
    # envelope and are shared in the same global order.
    held: list[Any] = []
    try:
        locked_reals = set(inputs.values())
        locked_reals.update(_rebalance._market._discover_completion_paths(
            tuple(sorted(locked_reals))))
        for input_real in sorted(locked_reals):
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
        _doc_legacy = _parse_coordination_bytes(
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
                _doc_legacy = _parse_coordination_bytes(
                    realpath, raw, {}, None, require_references=False)
            if key not in locked_batches:
                raise KeyError(key)
            with _business_locks(locked_inputs):
                snapshot = _Snapshot(locked_inputs)
                batches, _audit_events, _events, _ledger_inputs, _raw, \
                    _legacy, _doc_legacy = _load_coordination(
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
                _doc_legacy = _parse_coordination_bytes(
                    realpath, raw, {}, None, require_references=False)
            with _business_locks(inputs):
                snapshot = _Snapshot(inputs)
                batches, audit_events, _events, _ledger_inputs, _raw, \
                    _legacy, _doc_legacy = _load_coordination(
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
        raise CoordinationLedgerMissing(realpath)
    intrinsic, _audit, _events, _inputs, _legacy, _doc_legacy = \
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
                raise CoordinationLedgerMissing(realpath)
            batches_locked, _audit, _events, inputs_locked, _legacy, \
                _doc_legacy = _parse_coordination_bytes(
                    realpath, raw, {}, None, require_references=False)
            if key not in batches_locked:
                raise KeyError(key)
            with _business_locks(inputs_locked):
                snapshot = _snapshot_checked(inputs_locked)
                batches, _audit_events, _events, _ledger_inputs, _raw, \
                    _legacy, _doc_legacy = _load_coordination(
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
            _batches_locked, _audit, _events, inputs, _legacy, \
                _doc_legacy = _parse_coordination_bytes(
                    realpath, raw, {}, None, require_references=False)
            with _business_locks(inputs):
                snapshot = _snapshot_checked(inputs)
                batches, audit_events, _events, _ledger_inputs, _raw, \
                    _legacy, _doc_legacy = _load_coordination(
                        realpath,
                        {frozenset(snapshot.input_map.items()): snapshot},
                        None)
                page = _page(batches, audit_events, cursor, limit)
                return _render(page)


# ---------------------------------------------------------------------------
# Incremental progress event stream
# ---------------------------------------------------------------------------


def _progress_entry(event: dict[str, Any]) -> dict[str, Any]:
    # A fixed-order deep copy of one progress event and its complete
    # post-commit batch snapshot.
    return {
        "position": event["position"],
        "at": event["at"],
        "key": event["key"],
        "job_id": event["job_id"],
        "category": event["category"],
        "batch": _batch_snapshot(event["batch"]),
    }


def _validate_events_arguments(
    ledger: object, cursor: object, limit: object, key: object,
    job_id: object,
) -> None:
    if not isinstance(ledger, str) or not ledger:
        raise ValueError("ledger must be a non-empty string")
    # bool is a subclass of int and must be rejected as a cursor.
    if cursor is not None and (not _is_plain_int(cursor) or cursor < 0):
        raise ValueError("cursor must be None or a non-boolean "
                         "non-negative integer")
    if not _is_plain_int(limit) or not 1 <= limit <= _MAX_LIMIT:
        raise ValueError("limit must be a non-boolean integer between 1 "
                         "and 1000")
    for name, value in (("key", key), ("job_id", job_id)):
        if value is not None and (not isinstance(value, str) or not value):
            raise ValueError(f"{name} must be None or a non-empty string")


def _events_page(
    progress: list[dict[str, Any]], cursor: int | None, limit: int,
    key: str | None, job_id: str | None,
) -> dict[str, Any]:
    # The stream is stored in strictly increasing zero-based position
    # order. The cursor is exclusive, the batch-key and job filters
    # combine by logical AND and items they skip never consume page
    # capacity. One extra match is collected to learn whether more
    # matches remain; the continuation cursor is the page's last
    # position, null on the final page.
    matched: list[dict[str, Any]] = []
    for event in progress:
        position = event["position"]
        if cursor is not None and position <= cursor:
            continue
        if key is not None and event["key"] != key:
            continue
        if job_id is not None and event["job_id"] != job_id:
            continue
        matched.append(event)
        if len(matched) > limit:
            break
    if len(matched) > limit:
        page = matched[:limit]
        next_cursor: int | None = page[-1]["position"]
    else:
        page = matched
        next_cursor = None
    return {"events": [_progress_entry(event) for event in page],
            "next": next_cursor}


def events(ledger: str, cursor: int | None = None,
           limit: int = _DEFAULT_LIMIT, *, key: str | None = None,
           job_id: str | None = None) -> dict[str, Any]:
    """Return one page of incremental progress events by position.

    ``ledger`` must be a non-empty string. ``cursor`` is ``None`` or a
    non-boolean non-negative integer: only events at strictly greater
    positions are considered, so omitting it reads from the start.
    ``limit`` is a non-boolean integer from 1 to 1000, defaulting to
    100. ``key`` and ``job_id`` are optional non-empty string filters
    combined by logical AND (the batch key and the related member job
    id, where batch-scoped events carry a null job id); positions they
    skip never consume page capacity. Bad arguments raise
    ``ValueError`` before a file is read.

    The page object is ``{"events": [event, ...], "next": cursor}`` in
    that order. Each event carries, in order, its contiguous zero-based
    ``position`` (strictly increasing and never reused), the commit
    moment ``at``, the batch ``key``, the related ``job_id`` (null for
    batch-scoped changes), the ``category`` (``created``,
    ``taken_over``, ``item``, ``receipt`` or ``completed``) and the
    complete batch ``snapshot`` after that commit. ``next`` is the
    position of the page's last event when further matches remain,
    else ``None``.

    A baseline ledger written before the stream existed stays
    read-only compatible: it simply yields an empty page. A missing
    coordination ledger raises :class:`FileNotFoundError`; malformed,
    out-of-order or non-canonical bytes or a broken cross-reference
    raise ``ValueError`` and a missing required business ledger
    :class:`FileNotFoundError`; any other locking or I/O failure
    raises ``OSError``.
    """
    _validate_events_arguments(ledger, cursor, limit, key, job_id)
    realpath = os.path.realpath(ledger)
    # Format before locks, as in search(): a malformed ledger never
    # reaches a business ledger or creates a companion lock file.
    preliminary = _read_raw(realpath)
    _parse_coordination_bytes(realpath, preliminary, {}, None,
                              require_references=False)
    store = _get_store(realpath)
    with store.lock:
        with _lock(realpath, shared=True):
            raw = _read_raw(realpath)
            _batches, _audit, _events, inputs, _legacy, _doc_legacy = \
                _parse_coordination_bytes(
                    realpath, raw, {}, None, require_references=False)
            with _business_locks(inputs):
                snapshot = _Snapshot(inputs)
                _batches, _audit, progress, _ledger_inputs, _raw, \
                    _legacy, _doc_legacy = _load_coordination(
                        realpath,
                        {frozenset(snapshot.input_map.items()): snapshot},
                        None)
                # The page is formed while the whole shared-lock group is
                # still held -- the coordination ledger together with the
                # nine business ledgers -- so validation, filtering,
                # pagination and (for the serializer) the response bytes
                # all observe one complete version.
                return _events_page(progress, cursor, limit, key, job_id)


def events_response(ledger: str, cursor: int | None = None,
                    limit: int = _DEFAULT_LIMIT, *, key: str | None = None,
                    job_id: str | None = None) -> bytes:
    """Serialize :func:`events`' page while every read lock is held.

    The page is formed and rendered under the coordination ledger's and
    the business ledgers' shared locks, so a racing batch writer is
    observed as one complete version. A missing coordination ledger
    raises :class:`FileNotFoundError`; bad arguments, malformed bytes, a
    broken cross-reference or a missing required business ledger raise
    ``ValueError``; other locking or I/O failures raise ``OSError``.
    """
    _validate_events_arguments(ledger, cursor, limit, key, job_id)
    realpath = os.path.realpath(ledger)
    preliminary = _read_raw(realpath)
    _parse_coordination_bytes(realpath, preliminary, {}, None,
                              require_references=False)
    store = _get_store(realpath)
    with store.lock:
        with _lock(realpath, shared=True):
            raw = _read_raw(realpath)
            _batches, _audit, _events, inputs, _legacy, _doc_legacy = \
                _parse_coordination_bytes(
                    realpath, raw, {}, None, require_references=False)
            with _business_locks(inputs):
                snapshot = _snapshot_checked(inputs)
                _batches, _audit, progress, _ledger_inputs, _raw, \
                    _legacy, _doc_legacy = _load_coordination(
                        realpath,
                        {frozenset(snapshot.input_map.items()): snapshot},
                        None)
                page = _events_page(progress, cursor, limit, key, job_id)
                return _render(page)


# ---------------------------------------------------------------------------
# Persistent consumers: compatibility re-exports
# ---------------------------------------------------------------------------
#
# The persistent consumer domain (claim, pull, ack, reject, dead
# letters, status and subscription reads) lives in
# ``carbon_market.migration_consumer`` and is no longer implemented
# here; this module only lends it the coordination-snapshot read
# primitives above and re-exports its public surface unchanged, so
# every pre-split import path keeps working. The names resolve lazily
# on first access: either module may be imported first without a
# circular-import failure.

_CONSUMER_EXPORTS = frozenset({
    "consume", "consume_response", "consumer_subscription",
    "consumer_status", "consumer_status_response",
    "consumer_dead_letters", "consumer_dead_letters_response",
    "ConsumerLedgerInvalid", "ConsumerOwnershipError",
    "ConsumerLeaseExpired", "CoordinationLedgerInvalid",
    "ConsumersLedgerMissing"})


def __getattr__(name: str) -> Any:
    if name in _CONSUMER_EXPORTS:
        from . import migration_consumer
        return getattr(migration_consumer, name)
    raise AttributeError(
        f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | _CONSUMER_EXPORTS)
