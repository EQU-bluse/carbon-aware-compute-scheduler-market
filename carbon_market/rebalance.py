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
request raises ``ValueError``. The first successful :func:`start`
atomically upgrades the ledger to version 3 -- the continuous-migration
layout that keeps every recorded intent (keyed by its reservation key)
and appends the ``plans`` section keyed by the plan's own start key,
with every action request carrying its derived association -- so no
generation's evidence is ever overwritten; only the first change of a
call commits the plan, its state, the idempotency binding and the audit
event atomically. Every read requires the on-disk bytes to be exactly the
canonical compact form :func:`_canonical_bytes` produces -- compact
UTF-8 JSON with non-ASCII written through, no negative-zero or
non-finite number literals and exactly one trailing newline -- and
every commit goes through a synced same-directory temporary file, an
atomic replace and a directory fsync, restoring the pre-call bytes on
failure, so an unsuccessful call leaves neither a temporary fragment
nor half an audit event.

:func:`settle` finally gives later dispatch and execution a current
occupancy that survives the terminal plan, without rewriting any of
the existing layers: it writes one independent settlement ledger
(loaded read-only by :func:`current`). A ``migrated`` plan confirms the
target version as the current binding and retires the source version
as the previous-generation binding; a ``failed`` or ``interrupted``
plan compensates with the current occupancy kept on the source
version. Migrations are no longer limited to one per job: the intent
ledger keeps every reservation (keyed by its apply key) and every plan
(keyed by its own start key), and each terminal plan settles exactly
once into the job's continuous generation chain -- the immutable trade
is generation 0, the first settlement generation 1, and every later
settlement chains from the binding the previous completed settlement
left behind. A new round of advice, reservation or migration is rooted
at the latest completed binding, so it never falls back to the original
trade, while a settlement still in ``pending`` is not a generation and
blocks both another settlement and a follow-up migration. Each
settlement lands through a crash-safe two-phase commit
-- a durable ``pending`` record, binding and audit event first, then
the advance to ``active`` or ``compensated`` -- so a crash between the
two writes is resumed by the same key carrying the same request, which
completes the missing stage and still returns ``True``; only an
already completed same-key request returns the record with ``False``
without writing a byte.
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
# Version 3 keeps every generation of a job's migration lineage in one
# ledger: intents are keyed by their reservation (apply) key and plans by
# their start key, so each plan is uniquely identified by its own start
# idempotency key. The action requests carry the derived association --
# the start request the reserved intent's key, the step and recovery
# requests the plan's start key -- so replaying an old action key always
# resolves to its own plan, never to a later generation's.
_ACTION_REQUEST_FIELDS_V3 = {
    "start": ("action", "job_id", "owner", "lease_end", "at",
              "intent_key"),
    "record": ("action", "job_id", "owner", "step", "result", "receipt",
               "at", "plan_key"),
    "recover": ("action", "job_id", "owner", "at", "plan_key"),
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


# ---------------------------------------------------------------------------
# Lineage ledger discovery
# ---------------------------------------------------------------------------
#
# evaluate and the reservation/lifecycle calls keep their README call
# shapes (seven snapshot layers plus one explicit intent ledger), yet a
# new round of advice, reservation or migration must be rooted at the
# latest *completed* settlement rather than at the immutable trade and
# must not start while a settlement of the job is still pending. The
# independent lineage ledgers are therefore discovered beside the
# snapshots: every canonical ledger root in this module has a unique
# section shape, so the intent and settlement ledgers can be recognized
# without another path parameter. Discovered ledgers are only read under
# their shared locks; a write still lands exclusively in the explicit
# ledger.

_ADVICE_ROOT_SHAPE = frozenset(("version", "records", "audit"))
_INTENT_ROOT_SHAPES = (
    frozenset(("version", "intents", "idempotency", "audit")),
    frozenset(("version", "intents", "plans", "idempotency", "audit")),
)
_SETTLEMENT_ROOT_SHAPE = frozenset(
    ("version", "records", "idempotency", "audit"))


def _classify_ledger(data: object) -> str | None:
    # Classify a parsed sibling purely by its canonical root shape. The
    # settlement root (records + idempotency + audit) is distinct from
    # the advice root (no idempotency) and from the other ledgers
    # (trades/decisions/plans/history/... in place of records/intents), so
    # the root shape alone identifies it -- including a malformed one,
    # which must then be rejected by the full settlement validation
    # rather than silently ignored.
    if not isinstance(data, dict):
        return None
    keys = frozenset(data.keys())
    if keys == _ADVICE_ROOT_SHAPE:
        return "advice"
    if keys in _INTENT_ROOT_SHAPES:
        return "intent"
    if keys == _SETTLEMENT_ROOT_SHAPE:
        return "settlement"
    return None


def _discover_lineage_paths(
    anchor_reals: tuple[str, ...],
    kinds: tuple[str, ...],
) -> dict[str, list[str]]:
    # Scan each anchor directory once for canonical lineage ledgers of
    # the requested kinds. Malformed or unrelated siblings are ignored;
    # an explicit malformed ledger is still rejected by its own loader.
    found: dict[str, list[str]] = {kind: [] for kind in kinds}
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
            try:
                with open(real, "rb") as handle:
                    raw = handle.read()
            except OSError:
                continue
            try:
                data = finite_loads(raw.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                continue
            kind = _classify_ledger(data)
            if kind is not None and kind in found \
                    and real not in found[kind]:
                found[kind].append(real)
    for kind in found:
        found[kind].sort()
    return found




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
    lineage_currents: dict[str, set[tuple[str, int, int]]] | None = None,
    defer_current: bool = False,
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
    if not defer_current:
        trade_selection = {"resource_id": trade["resource_id"],
                           "version": trade["version"]}
        allowed_currents = {(trade_selection["resource_id"],
                             trade_selection["version"])}
        if lineage_currents is not None:
            # A later round of advice is rooted at the latest binding a
            # completed settlement had already confirmed at or before the
            # evaluation moment, never forced back to the immutable trade.
            allowed_currents |= {
                (rid, ver) for rid, ver, settled_at
                in lineage_currents.get(job_id, set()) if settled_at <= at}
        if (current["resource_id"], current["version"]) \
                not in allowed_currents:
            raise ValueError("advice current selection must match the "
                             "job's current binding")
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
    lineage_currents: dict[str, set[tuple[str, int, int]]] | None = None,
    defer_current: bool = False,
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
            record_raw, accepted, trades, history, signal_history,
            lineage_currents, defer_current)
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
            signal_history, lineage_currents, defer_current)
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
    lineage_currents: dict[str, set[tuple[str, int, int]]] | None = None,
    defer_current: bool = False,
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
        data, accepted, trades, history, signal_history, lineage_currents,
        defer_current)
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
        # The independent intent and settlement ledgers beside the
        # snapshots are discovered before locking and added to the same
        # ordered set, all shared (evaluate never writes them).
        anchor_reals = (job_real, supply_real, signal_real, trades_real,
                        dispatch_real, execution_real, ledger_real)
        discovered = _discover_lineage_paths(
            anchor_reals, ("intent", "settlement"))
        extra_reals = {real for kind in ("intent", "settlement")
                       for real in discovered.get(kind, ())
                       if real not in set(all_paths)}
        with contextlib.ExitStack() as stack:
            for locked in sorted(set(all_paths) | extra_reals):
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

            # The migration lineage lives in the intent and settlement
            # ledgers beside the snapshots. They are read-only inputs
            # here and are discovered rather than passed, so the public
            # evaluate call shape stays at seven paths. Loading order is
            # fixed by the references: the advice ledger is parsed first
            # with its current-binding grounding deferred, the intent
            # ledger needs those advice records, the settlement ledger
            # needs the intent ledger, and only then are the advice
            # currents grounded in the completed bindings.
            snapshot_real_set = set(all_paths)
            intent_lineage_real = next(
                (real for real in discovered.get("intent", ())
                 if real not in snapshot_real_set), None)
            settlement_real = next(
                (real for real in discovered.get("settlement", ())
                 if real not in snapshot_real_set), None)

            records, events, old_bytes = _load_ledger(
                ledger_real, accepted, cleared, history, signal_history,
                defer_current=True)

            lineage_plans: dict[str, dict[str, Any]] = {}
            lineage_idempotency: dict[str, dict[str, Any]] = {}
            lineage_events: dict[str, dict[str, Any]] = {}
            lineage_version = _INTENT_VERSION
            if intent_lineage_real is not None:
                (_li, lineage_plans, lineage_idempotency, lineage_events,
                 lineage_version, _ir) = _load_intent_ledger(
                    intent_lineage_real, accepted, cleared, history,
                    signal_history, records)
                lineage_plans = _plans_by_start_key(
                    lineage_plans, lineage_idempotency, lineage_version)
            completed: dict[str, list[dict[str, Any]]] = {}
            if settlement_real is not None:
                settlement_records, _sid, _sev, _sraw = \
                    _load_settlement_ledger(
                        settlement_real, accepted, cleared, lineage_plans,
                        lineage_idempotency, lineage_events,
                        lineage_version)
                completed, _pending_jobs = \
                    _lineage_from_settlement_records(settlement_records)
            currents = _settlement_currents(completed)
            for advice_record in records.values():
                _ground_advice_current(advice_record, cleared, currents)

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
            # A still-open migration in the lineage ledger is work in
            # flight exactly like an active execution plan.
            active_lineage_plan = any(
                plan["job_id"] == job_id and plan["state"] == "active"
                for plan in lineage_plans.values())
            # Refusal order is fixed: a finished booking is a ValueError,
            # work still in flight is a PermissionError, and a moment
            # past the deadline is a TimeoutError.
            if decision["state"] == "succeeded" \
                    or any(plan["state"] == "completed"
                           for plan in job_plans.values()):
                raise ValueError("a finished booking cannot be "
                                 "re-evaluated")
            if any(plan["state"] == "active"
                   for plan in job_plans.values()) \
                    or active_lineage_plan:
                raise PermissionError("an execution plan is active")
            if at > job["deadline"]:
                raise TimeoutError("evaluation moment exceeds the job "
                                   "deadline")

            # A new round is rooted at the latest *completed* binding
            # current returns, never forced back to the original trade.
            # A pending settlement is ignored as a generation here.
            latest_settled = _latest_completed_binding(
                completed, job_id, at)
            if latest_settled is not None:
                current = dict(latest_settled["after"])
            else:
                current = {"resource_id": trade["resource_id"],
                           "version": trade["version"]}
            current_id = current["resource_id"]
            current_version = current["version"]
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
    allowed_sources: set[tuple[str, int]] | None = frozenset(),
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
    grounded = {(trade["resource_id"], trade["version"])}
    if allowed_sources:
        # Version 3 lineage: a later round leaves the outcome of one of
        # the job's own earlier terminal plans -- the target after a
        # migration, the unchanged source after a compensation. Strict
        # no-gap/no-fork continuity is enforced by the settlement
        # generation chain; this grounds every recorded source in the
        # job's own lineage instead of forcing it back to the trade.
        grounded |= set(allowed_sources)
    if (source["resource_id"], source["version"]) not in grounded:
        raise ValueError("intent source selection must match the job's "
                         "current binding")
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


def _validate_plan_record(
    record: object,
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


def _validate_plan(
    record: object,
    intents: dict[str, dict[str, Any]],
    accepted: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    plan = _validate_plan_record(record)
    intent = intents.get(plan["job_id"])
    if intent is None:
        raise ValueError("plan must reference a recorded intent")
    if plan["source"] != intent["source"] \
            or plan["target"] != intent["target"]:
        raise ValueError("plan selections must match its intent")
    # The intent validation guarantees the job is accepted.
    if plan["lease_end"] > accepted[plan["job_id"]]["deadline"]:
        raise ValueError("plan lease end must not pass the job deadline")
    return plan


def _validate_action_request(request: object,
                             version: int = _INTENT_VERSION_V2
                             ) -> dict[str, Any]:
    fields = (_ACTION_REQUEST_FIELDS_V3
              if version == _INTENT_VERSION_V3 else _ACTION_REQUEST_FIELDS)
    if not isinstance(request, dict):
        raise ValueError("request must be an object")
    action = request.get("action")
    if not isinstance(action, str) or action not in fields:
        raise ValueError("request action is invalid")
    if set(request.keys()) != set(fields[action]):
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
    if version == _INTENT_VERSION_V3:
        # The derived association: the start request names the reserved
        # intent it claims, the step and recovery requests the plan they
        # acted on -- each by the owning idempotency key.
        derived = "intent_key" if action == "start" else "plan_key"
        token = request[derived]
        if not isinstance(token, str) or not token:
            raise ValueError(f"request {derived} must be a non-empty "
                             "string")
        normalized[derived] = token
    return {field: normalized[field] for field in fields[action]}


def _validate_any_request(request: object,
                          version: int = _INTENT_VERSION_V2
                          ) -> dict[str, Any]:
    # The idempotency key space is shared: apply bindings carry no
    # action, the migration lifecycle bindings carry one.
    if isinstance(request, dict) and "action" in request:
        return _validate_action_request(request, version)
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
    if keys == set(_INTENT_ROOT_FIELDS_V2) \
            and data.get("version") == _INTENT_VERSION_V3:
        return _validate_intent_ledger_v3(
            data, accepted, trades, history, signal_history,
            advice_records)
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


def _validate_intent_ledger_v3(
    data: dict[str, Any],
    accepted: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]],
    advice_records: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]], dict[str, dict[str, Any]], int]:
    # Version 3 keeps every generation of each job's migration lineage:
    # intents are keyed by their reservation (apply) idempotency key and
    # plans by their start idempotency key, so a new generation never
    # overwrites an earlier one's plan, receipts or start key.
    intents_raw = data["intents"]
    plans_raw = data["plans"]
    idempotency_raw = data["idempotency"]
    audit_raw = data["audit"]
    if not isinstance(intents_raw, dict) \
            or not isinstance(plans_raw, dict) \
            or not isinstance(idempotency_raw, dict) \
            or not isinstance(audit_raw, dict):
        raise ValueError("intents, plans, idempotency and audit must be "
                         "objects")
    _check_sorted_keys(intents_raw, "intents")
    _check_sorted_keys(plans_raw, "plans")
    _check_sorted_keys(idempotency_raw, "idempotency")
    _check_sorted_keys(audit_raw, "audit")

    idempotency: dict[str, dict[str, Any]] = {}
    for key, request_raw in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        idempotency[key] = _validate_any_request(
            request_raw, _INTENT_VERSION_V3)

    # Plans are parsed first, structurally: each start request names the
    # reserved intent it claims, and the terminal outcomes of earlier
    # plans ground the sources of later rounds. Intent grounding and the
    # plan/intent cross-checks follow once both are in hand.
    plans: dict[str, dict[str, Any]] = {}
    plan_intent_key: dict[str, str] = {}
    intent_plans: dict[str, str] = {}
    for key, plan_raw in plans_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("plan keys must be non-empty strings")
        request = idempotency.get(key)
        if request is None or request.get("action") != "start":
            raise ValueError("every plan must be bound to its start "
                             "request")
        plan = _validate_plan_record(plan_raw)
        intent_key = request["intent_key"]
        if not isinstance(intent_key, str) or not intent_key:
            raise ValueError("plan start request must name its intent key")
        plan_intent_key[key] = intent_key
        if intent_key in intent_plans:
            raise ValueError("intent claimed under more than one start "
                             "key")
        intent_plans[intent_key] = key
        plans[key] = plan
    for key, request in idempotency.items():
        if request.get("action") == "start" and key not in plans:
            raise ValueError("start request must reference a recorded "
                             "plan")

    # The terminal outcome each claimed plan left behind: the target
    # after a migration, the unchanged source after a compensation. A
    # later round's reservation is grounded in one of the job's own
    # earlier terminal outcomes (or the immutable trade for the first
    # round).
    def _outcome(plan: dict[str, Any]) -> tuple[str, int]:
        binding = plan["target"] if plan["state"] == "migrated" \
            else plan["source"]
        return binding["resource_id"], binding["version"]

    outcomes_by_job: dict[str, set[tuple[str, int]]] = {}
    for plan in plans.values():
        if plan["state"] in ("migrated", "failed", "interrupted"):
            outcomes_by_job.setdefault(plan["job_id"], set()).add(
                _outcome(plan))

    intents: dict[str, dict[str, Any]] = {}
    for key, record_raw in intents_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("intent keys must be non-empty strings")
        request = idempotency.get(key)
        if request is None or "action" in request:
            raise ValueError("every intent must be bound to a reservation "
                             "request")
        job_id = request["job_id"]
        record = _validate_intent(
            record_raw, accepted, trades, history, signal_history,
            advice_records,
            allowed_sources=outcomes_by_job.get(job_id, frozenset()))
        if request["job_id"] != record["job_id"] \
                or request["advice_key"] != record["advice_key"] \
                or request["at"] != record["at"]:
            raise ValueError("idempotency entry does not match its "
                             "intent")
        intents[key] = record
    for key, request in idempotency.items():
        if "action" not in request and key not in intents:
            raise ValueError("reservation request must reference a "
                             "recorded intent")

    # Cross-check each plan against the intent it claimed and against its
    # own start request.
    for key, plan in plans.items():
        request = idempotency[key]
        intent = intents.get(plan_intent_key[key])
        if intent is None:
            raise ValueError("plan must reference a recorded intent")
        if intent["job_id"] != request["job_id"] \
                or plan["job_id"] != intent["job_id"]:
            raise ValueError("plan start request does not match its "
                             "intent")
        if plan["source"] != intent["source"] \
                or plan["target"] != intent["target"]:
            raise ValueError("plan selections must match its intent")
        # The intent validation guarantees the job is accepted.
        if plan["lease_end"] > accepted[plan["job_id"]]["deadline"]:
            raise ValueError("plan lease end must not pass the job "
                             "deadline")
        if plan["owner"] != request["owner"] \
                or plan["lease_end"] != request["lease_end"] \
                or plan["at"] != request["at"]:
            raise ValueError("plan does not match its start request")

    events: dict[str, dict[str, Any]] = {}
    for key, event_raw in audit_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event_raw, dict) \
                or set(event_raw.keys()) != set(_EVENT_FIELDS):
            raise ValueError("intent audit event has invalid fields")
        if event_raw["key"] != key or not isinstance(event_raw["key"], str):
            raise ValueError("audit event key does not match its map key")
        request = _validate_any_request(
            event_raw["request"], _INTENT_VERSION_V3)
        if key not in idempotency:
            raise ValueError("audit event must reference an idempotency "
                             "entry")
        if request != idempotency[key]:
            raise ValueError("audit event does not match its idempotency "
                             "entry")
        if "action" not in request:
            result: dict[str, Any] = _validate_intent(
                event_raw["result"], accepted, trades, history,
                signal_history, advice_records,
                allowed_sources=outcomes_by_job.get(
                    request["job_id"], frozenset()))
            if result != intents[key]:
                raise ValueError("audit event result does not match its "
                                 "intent")
        else:
            plan_key = (key if request["action"] == "start"
                        else request["plan_key"])
            plan = plans.get(plan_key)
            if plan is None:
                raise ValueError("action audit event must reference a "
                                 "recorded plan")
            result = _validate_plan_record(event_raw["result"])
            if result["job_id"] != request["job_id"]:
                raise ValueError("audit event result does not match its "
                                 "request")
            for field in ("source", "target", "owner", "lease_end", "at"):
                if result[field] != plan[field]:
                    raise ValueError("audit event result does not match "
                                     "its plan")
            action = request["action"]
            if action == "start":
                if result["state"] != "active" or result["steps"] != []:
                    raise ValueError("start audit result must be a fresh "
                                     "active plan")
            elif action == "record":
                if not result["steps"]:
                    raise ValueError("record audit result must carry the "
                                     "recorded step")
                receipt = result["steps"][-1]
                if receipt != {"step": request["step"],
                               "result": request["result"],
                               "receipt": request["receipt"],
                               "at": request["at"]}:
                    raise ValueError("record audit result does not match "
                                     "its request")
            else:  # recover
                if result["state"] != "interrupted":
                    raise ValueError("recover audit result must be an "
                                     "interrupted plan")
                if request["at"] <= result["lease_end"]:
                    raise ValueError("recover request must lie past the "
                                     "lease end")
        events[key] = {"key": key, "request": request, "result": result}

    # The sections describe one reservation history: one binding and one
    # audit event per intent and per action, and vice versa.
    if set(events) != set(idempotency):
        raise ValueError("idempotency keys and audit events do not match")

    # Every plan's steps are exactly the receipts its record requests
    # committed, and it is interrupted exactly when one recovery was
    # recorded for it.
    recordings: dict[str, list[dict[str, Any]]] = {}
    recoveries: dict[str, int] = {}
    for key, request in idempotency.items():
        action = request.get("action")
        if action == "record":
            plan_key = request["plan_key"]
            if plan_key not in plans:
                raise ValueError("record request must reference a "
                                 "recorded plan")
            if plans[plan_key]["job_id"] != request["job_id"]:
                raise ValueError("record request does not match its "
                                 "plan")
            recordings.setdefault(plan_key, []).append(
                events[key]["result"]["steps"][-1])
        elif action == "recover":
            plan_key = request["plan_key"]
            if plan_key not in plans:
                raise ValueError("recover request must reference a "
                                 "recorded plan")
            if plans[plan_key]["job_id"] != request["job_id"]:
                raise ValueError("recover request does not match its "
                                 "plan")
            recoveries[plan_key] = recoveries.get(plan_key, 0) + 1
    for start_key, plan in plans.items():
        committed = sorted(json.dumps(receipt, sort_keys=True)
                           for receipt in recordings.get(start_key, []))
        held = sorted(json.dumps(receipt, sort_keys=True)
                      for receipt in plan["steps"])
        if committed != held:
            raise ValueError("plan steps do not match the record "
                             "history")
        if (plan["state"] == "interrupted") \
                != (recoveries.get(start_key, 0) == 1):
            raise ValueError("plan state does not match the recover "
                             "history")
        if recoveries.get(start_key, 0) > 1:
            raise ValueError("plan recovered more than once")

    # The lineage invariants the lifecycle enforces at construction.
    # Rounds are ordered by their reservation moment (ties broken by the
    # reservation key), and the chain is walked in that order: the first
    # round leaves the immutable trade, and every later round must leave
    # exactly what the immediately preceding *terminal* plan left behind
    # -- its target after a migration, its unchanged source after a
    # compensation. This rejects a generation gap or a fork. A job holds
    # at most one open round: one unclaimed intent, at most one active
    # plan, and a new reservation is allowed only once every earlier plan
    # has reached a terminal state.
    intents_by_job: dict[str, list[str]] = {}
    for apply_key, intent in intents.items():
        intents_by_job.setdefault(intent["job_id"], []).append(apply_key)
    for job_id, apply_keys in intents_by_job.items():
        ordered = sorted(apply_keys,
                         key=lambda ref: (intents[ref]["at"], ref))
        trade_selection = {"resource_id": trades[job_id]["resource_id"],
                           "version": trades[job_id]["version"]}
        expected_source = trade_selection
        unclaimed = 0
        active_plans = 0
        terminal_before_open = True
        for index, ref in enumerate(ordered):
            intent = intents[ref]
            if index == 0:
                if intent["source"] != trade_selection:
                    raise ValueError("the first migration round must leave "
                                     "the job's traded version")
            elif intent["source"] != expected_source:
                raise ValueError("migration rounds must form one "
                                 "continuous binding chain")
            plan_key = intent_plans.get(ref)
            if plan_key is None:
                # A fresh, still-unclaimed reservation: it may only be
                # the open tail of the chain and every earlier plan must
                # already be terminal.
                unclaimed += 1
                if unclaimed > 1:
                    raise ValueError("job holds more than one unclaimed "
                                     "intent")
                if any((intent_plans.get(earlier) is not None
                        and plans[intent_plans[earlier]]["state"] == "active")
                       for earlier in ordered[:index]):
                    raise ValueError("a new reservation requires every "
                                     "earlier plan to be terminal")
                terminal_before_open = all(
                    intent_plans.get(earlier) is None
                    or plans[intent_plans[earlier]]["state"]
                    in ("migrated", "failed", "interrupted")
                    for earlier in ordered[:index])
                if not terminal_before_open:
                    raise ValueError("a new reservation requires every "
                                     "earlier plan of the job to be "
                                     "terminal")
                continue
            plan = plans[plan_key]
            if plan["state"] == "active":
                active_plans += 1
                if active_plans > 1:
                    raise ValueError("job holds more than one active plan")
                continue
            expected_source = (plan["target"] if plan["state"] == "migrated"
                               else plan["source"])

    return intents, plans, idempotency, events, _INTENT_VERSION_V3


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
    # section; version 2 keeps every intent and appends it. Version 3
    # keeps every generation of each job's lineage: intents keyed by
    # their reservation key and plans by their start key.
    payload: dict[str, Any] = {"version": version}
    payload["intents"] = {job_id: intents[job_id]
                          for job_id in sorted(intents)}
    if version in (_INTENT_VERSION_V2, _INTENT_VERSION_V3):
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


def _job_intent_items(
    intents: dict[str, dict[str, Any]],
    version: int,
    job_id: str,
) -> list[tuple[str, dict[str, Any]]]:
    # The job's intents as (reference, record) pairs: the reference is
    # the reservation idempotency key in version 3, the job id before.
    if version == _INTENT_VERSION_V3:
        return [(key, record) for key, record in intents.items()
                if record["job_id"] == job_id]
    record = intents.get(job_id)
    return [(job_id, record)] if record is not None else []


def _plans_by_start_key(
    plans: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, Any]],
    version: int,
) -> dict[str, dict[str, Any]]:
    # Every plan keyed by its own start idempotency key: directly in
    # version 3, derived from the start bindings before.
    if version == _INTENT_VERSION_V3:
        return dict(plans)
    keyed: dict[str, dict[str, Any]] = {}
    for key, request in idempotency.items():
        if request.get("action") == "start":
            plan = plans.get(request["job_id"])
            if plan is not None:
                keyed[key] = plan
    return keyed


def _claimed_intent_refs(
    plans_by_key: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, Any]],
    version: int,
) -> set[str]:
    # The intent reference each started plan claimed: the start request's
    # intent_key in version 3, the job id before.
    claimed: set[str] = set()
    for start_key, plan in plans_by_key.items():
        if version == _INTENT_VERSION_V3:
            claimed.add(idempotency[start_key]["intent_key"])
        else:
            claimed.add(plan["job_id"])
    return claimed


def _request_conflicts(
    stored: dict[str, Any],
    fresh: dict[str, Any],
) -> bool:
    # A replay conflicts when any caller-provided field differs; the
    # derived associations a version 3 binding carries (intent_key,
    # plan_key) are recomputed from the ledger, not supplied by the
    # caller, so they take no part in the comparison.
    return any(stored.get(field) != value
               for field, value in fresh.items())


def _upgrade_intent_ledger_v3(
    intents: dict[str, dict[str, Any]],
    plans: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    # Upgrade a version 1/2 ledger to version 3 in place: intents are
    # re-keyed by their reservation key and plans by their start key,
    # and every action request gains its derived association. Version 2
    # holds exactly one intent and one plan per job, so every derived
    # value is unique and no recorded evidence is rewritten or lost.
    apply_keys: dict[str, str] = {}
    start_keys: dict[str, str] = {}
    for key, request in idempotency.items():
        if "action" not in request:
            apply_keys[request["job_id"]] = key
        elif request["action"] == "start":
            start_keys[request["job_id"]] = key

    new_intents = {apply_keys[job_id]: record
                   for job_id, record in intents.items()}
    new_plans = {start_keys[job_id]: plan
                 for job_id, plan in plans.items()}

    def upgrade_request(request: dict[str, Any]) -> dict[str, Any]:
        upgraded = dict(request)
        action = upgraded.get("action")
        if action == "start":
            upgraded["intent_key"] = apply_keys[upgraded["job_id"]]
        elif action in ("record", "recover"):
            upgraded["plan_key"] = start_keys[upgraded["job_id"]]
        return upgraded

    new_idempotency = {key: upgrade_request(request)
                       for key, request in idempotency.items()}
    new_events = {
        key: {"key": event["key"],
              "request": upgrade_request(event["request"]),
              "result": event["result"]}
        for key, event in events.items()
    }
    return new_intents, new_plans, new_idempotency, new_events


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
        # intent ledger lock is exclusive, the seven snapshot locks and
        # the independent settlement ledger discovered beside them are
        # shared.
        anchor_reals = all_paths
        discovered_settlements = _discover_lineage_paths(
            anchor_reals, ("settlement",))
        extra_reals = {real for real in discovered_settlements.get(
                           "settlement", ())
                       if real not in set(all_paths)}
        with contextlib.ExitStack() as stack:
            for locked in sorted(set(all_paths) | extra_reals):
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
            # The advice ledger is parsed first with only its structural
            # checks (current-binding grounding deferred): the intent
            # ledger references those advice records, while the
            # settlement ledger references the intent ledger, and only
            # at the end can the advice currents be grounded at the
            # completed settlement bindings.
            advice_records, _advice_events, advice_raw = _load_ledger(
                advice_real, accepted, cleared, history, signal_history,
                defer_current=True)
            if advice_raw is None:
                raise FileNotFoundError(
                    f"advice ledger {advice_real!r} does not exist")
            intents, migration_plans, idempotency, events, \
                ledger_version, old_bytes = _load_intent_ledger(
                    ledger_real, accepted, cleared, history,
                    signal_history, advice_records)

            # The independent settlement ledger is discovered beside the
            # snapshots so a new reservation is rooted at the latest
            # completed binding and refused while a settlement is still
            # pending. It is validated against this very intent ledger.
            plans_by_key_early = _plans_by_start_key(
                migration_plans, idempotency, ledger_version)
            settlement_paths = sorted(set(
                real for real in discovered_settlements.get("settlement", ())
                if real not in set(all_paths)))
            settlement_completed: dict[str, list[dict[str, Any]]] = {}
            settlement_pending: set[str] = set()
            for settlement_path in settlement_paths:
                s_records, _sid, _sev, _sraw = _load_settlement_ledger(
                    settlement_path, accepted, cleared, plans_by_key_early,
                    idempotency, events, ledger_version)
                completed_items, pending_items = \
                    _lineage_from_settlement_records(s_records)
                for jid, items in completed_items.items():
                    settlement_completed.setdefault(jid, []).extend(items)
                settlement_pending |= pending_items
            settlement_currents = _settlement_currents(settlement_completed)
            for advice_record in advice_records.values():
                _ground_advice_current(advice_record, cleared,
                                       settlement_currents)

            request = {"job_id": job_id, "advice_key": advice_key,
                       "at": at}
            binding = idempotency.get(key)
            if binding is not None:
                if binding != request:
                    raise ValueError("idempotency key was already used "
                                     "with a different request")
                # An equivalent replay returns the stored record
                # without re-reserving or rewriting a byte.
                if ledger_version == _INTENT_VERSION_V3:
                    return copy.deepcopy(intents[key]), False
                return copy.deepcopy(intents[job_id]), False

            job = accepted.get(job_id)
            if job is None:
                raise KeyError(job_id)
            plans_by_key = _plans_by_start_key(
                migration_plans, idempotency, ledger_version)
            job_intents = _job_intent_items(intents, ledger_version,
                                            job_id)
            job_prior_plans = [plan for plan in plans_by_key.values()
                               if plan["job_id"] == job_id]
            if job_intents:
                # A later reservation is a new settlement generation, not
                # a second claim on the open lineage. An intent that is
                # still merely reserved keeps the job reserved
                # (ValueError); an active plan is work in flight
                # (PermissionError); while a settlement is pending, or a
                # terminal plan has not published its completed binding,
                # the lineage cannot open another round (ValueError).
                claimed = _claimed_intent_refs(
                    plans_by_key, idempotency, ledger_version)
                if any(ref not in claimed for ref, _record in job_intents):
                    raise ValueError("job is already reserved under "
                                     "another idempotency key")
                if any(plan["state"] == "active"
                       for plan in job_prior_plans):
                    raise PermissionError("the booking has an active "
                                          "migration plan")
                if job_id in settlement_pending:
                    raise ValueError("the job still holds a pending "
                                     "settlement")
                settled_plan_keys = {
                    record["plan_key"]
                    for record in settlement_completed.get(job_id, ())}
                if any(plan_key_hint not in settled_plan_keys
                       for plan_key_hint, plan in plans_by_key.items()
                       if plan["job_id"] == job_id):
                    raise ValueError("a previous migration is not settled "
                                     "yet")
            elif job_id in settlement_pending:
                raise ValueError("the job still holds a pending "
                                 "settlement")
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

            # The reservation leaves the latest *completed* binding
            # current returns -- the settled target after a migration,
            # the unchanged source after a compensation -- and only the
            # first round leaves the immutable trade.
            latest_settled = _latest_completed_binding(
                settlement_completed, job_id)
            if latest_settled is not None:
                current = dict(latest_settled["after"])
            else:
                current = {"resource_id": trade["resource_id"],
                           "version": trade["version"]}
            current_id = current["resource_id"]
            current_version = current["version"]
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
            plans_of_job: dict[str, list[dict[str, Any]]] = {}
            for plan in plans_by_key.values():
                plans_of_job.setdefault(plan["job_id"], []).append(plan)
            intent_plan: dict[str, dict[str, Any]] = {}
            if ledger_version == _INTENT_VERSION_V3:
                intent_items = list(intents.items())
                for start_key, plan in plans_by_key.items():
                    intent_plan[idempotency[start_key]["intent_key"]] = \
                        plan
            else:
                intent_items = list(intents.items())
                for plan in plans_by_key.values():
                    intent_plan[plan["job_id"]] = plan
            booked: dict[tuple[str, int], int] = {}
            # Each OTHER job occupies exactly one version: the after of
            # its latest completed settlement when one exists, otherwise
            # its traded version adjusted for the open/terminal migration
            # plan the intent ledger still holds.
            settled_binding: dict[str, dict[str, Any]] = {}
            for other_id in cleared:
                if other_id == job_id:
                    continue
                latest_other = _latest_completed_binding(
                    settlement_completed, other_id)
                if latest_other is not None:
                    settled_binding[other_id] = latest_other["after"]
            for other_id, other in cleared.items():
                if other_id == job_id:
                    continue
                if other_id in settled_binding:
                    binding = settled_binding[other_id]
                    booked[(binding["resource_id"], binding["version"])] = \
                        booked.get((binding["resource_id"],
                                    binding["version"]), 0) + other["work"]
                else:
                    if any(plan["state"] == "migrated"
                           for plan in plans_of_job.get(other_id, ())):
                        # A migrated plan not yet settled already releases
                        # the traded source; its target is added through
                        # the intent loop below.
                        continue
                    slot = (other["resource_id"], other["version"])
                    booked[slot] = booked.get(slot, 0) + other["work"]
            # Map each claimed intent reference to its plan start key so
            # the completed settlements can suppress its stale occupancy.
            if ledger_version == _INTENT_VERSION_V3:
                intent_start_key = {
                    idempotency[start_key]["intent_key"]: start_key
                    for start_key in plans_by_key
                    if idempotency[start_key].get("action") == "start"}
            else:
                intent_start_key = {
                    plan["job_id"]: start_key
                    for start_key, plan in plans_by_key.items()}
            settled_rounds: dict[str, set[str]] = {}
            for other_id, items in settlement_completed.items():
                settled_rounds[other_id] = {
                    record["plan_key"] for record in items}
            # Open reservations and plans still hold their target; a
            # round already covered by a completed settlement adds
            # nothing more, and a compensation holds no target.
            for ref, intent in intent_items:
                other_id = intent["job_id"]
                if other_id == job_id:
                    continue
                other_plan = intent_plan.get(ref)
                if other_plan is not None:
                    start_key = intent_start_key.get(ref)
                    if start_key in settled_rounds.get(other_id, set()):
                        continue
                    if other_plan["state"] in ("failed", "interrupted"):
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
            if ledger_version != _INTENT_VERSION_V3 and job_intents:
                # A subsequent reservation of the same job upgrades the
                # ledger to version 3: every recorded intent, plan,
                # binding and audit event is preserved, re-keyed so no
                # generation's evidence is overwritten.
                intents, migration_plans, idempotency, events = \
                    _upgrade_intent_ledger_v3(
                        intents, migration_plans, idempotency, events)
                ledger_version = _INTENT_VERSION_V3
            if ledger_version == _INTENT_VERSION_V3:
                intents[key] = record
            else:
                intents[job_id] = record
            idempotency[key] = request
            events[key] = {"key": key, "request": dict(request),
                           "result": copy.deepcopy(record)}
            # A ledger that already carries migration plans keeps its
            # version 2 or 3 form; anything else stays a version 1
            # ledger.
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
    # The advice current-binding grounding is deferred: later-round
    # advice is rooted at a completed settlement, which is loaded after
    # the intent ledger this snapshot feeds. Callers ground the records
    # once the settlement lineage is in hand.
    advice_records, _advice_events, advice_raw = _load_ledger(
        advice_real, accepted, cleared, history, signal_history,
        defer_current=True)
    if advice_raw is None:
        raise FileNotFoundError(
            f"advice ledger {advice_real!r} does not exist")
    return (accepted, history, signal_history, cleared, decisions,
            exec_plans, advice_records)


def _lifecycle_extra_locks(
    all_reals: set[str],
) -> tuple[set[str], list[str]]:
    # Discover the independent settlement ledger beside the lifecycle
    # snapshots and return the extra real paths to lock and the ordered
    # settlement paths to read.
    discovered = _discover_lineage_paths(tuple(all_reals), ("settlement",))
    paths = sorted(set(
        real for real in discovered.get("settlement", ())
        if real not in all_reals))
    return set(paths), paths


def _lifecycle_settlement_view(
    settlement_paths: list[str],
    accepted: dict[str, dict[str, Any]],
    cleared: dict[str, dict[str, Any]],
    plans_by_key: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
    version: int,
) -> tuple[dict[str, list[dict[str, Any]]], set[str]]:
    # Read-only validate the discovered settlement ledger(s) against the
    # explicit intent snapshot, returning the completed bindings and the
    # jobs that still hold a pending settlement.
    completed: dict[str, list[dict[str, Any]]] = {}
    pending: set[str] = set()
    for real in settlement_paths:
        records, _ids, _events, _raw = _load_settlement_ledger(
            real, accepted, cleared, plans_by_key, idempotency, events,
            version)
        ledger_completed, ledger_pending = \
            _lineage_from_settlement_records(records)
        for jid, items in ledger_completed.items():
            completed.setdefault(jid, []).extend(items)
        pending |= ledger_pending
    return completed, pending


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
    one synced atomic write that upgrades the ledger to version 3 --
    intents keyed by their reservation key, plans keyed by their start
    key -- while keeping every recorded intent, plan, binding and audit
    event. Replaying the same key with the same
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
        # intent ledger lock is exclusive, the seven snapshot locks and
        # the discovered settlement ledger shared.
        lifecycle_reals = {job_real, supply_real, signal_real,
                           trades_real, dispatch_real, execution_real,
                           advice_real, ledger_real}
        settlement_extras, settlement_paths = _lifecycle_extra_locks(
            lifecycle_reals)
        with contextlib.ExitStack() as stack:
            for locked in sorted(lifecycle_reals | settlement_extras):
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
            plans_by_key_settle = _plans_by_start_key(
                plans, idempotency, _version)
            settlement_completed, settlement_pending = \
                _lifecycle_settlement_view(
                    settlement_paths, accepted, cleared,
                    plans_by_key_settle, idempotency, events, _version)
            for advice_record in advice_records.values():
                _ground_advice_current(
                    advice_record, cleared,
                    _settlement_currents(settlement_completed))

            request = {"action": "start", "job_id": job_id,
                       "owner": owner, "lease_end": lease_end, "at": at}
            binding = idempotency.get(key)
            if binding is not None:
                if _request_conflicts(binding, request):
                    raise ValueError("idempotency key was already used "
                                     "with a different request")
                # An equivalent replay returns the current state of the
                # plan this very key started -- never a later
                # generation's plan -- without rewriting a byte.
                if _version == _INTENT_VERSION_V3:
                    return copy.deepcopy(plans[key]), False
                return copy.deepcopy(plans[job_id]), False

            if _version != _INTENT_VERSION_V3:
                # The first successful start atomically upgrades the
                # ledger to the continuous-migration layout (version 3):
                # every recorded intent, plan, idempotency binding and
                # audit event is preserved, re-keyed so each reservation
                # key names exactly one intent and each start key exactly
                # one plan, and every action request gains its derived
                # association. The upgrade is in-memory until the final
                # commit, so the refusal checks below still leave the
                # file untouched, and a replay (handled above) never
                # rewrites a byte.
                intents, plans, idempotency, events = \
                    _upgrade_intent_ledger_v3(
                        intents, plans, idempotency, events)
                _version = _INTENT_VERSION_V3

            job = accepted.get(job_id)
            if job is None:
                raise KeyError(job_id)
            job_intents = _job_intent_items(intents, _version, job_id)
            if not job_intents:
                raise KeyError(job_id)
            decision = decisions.get(job_id)
            if decision is None:
                raise KeyError(job_id)

            plans_by_key = _plans_by_start_key(plans, idempotency,
                                               _version)
            migration_plans = [plan for plan in plans_by_key.values()
                               if plan["job_id"] == job_id]
            claimed = _claimed_intent_refs(plans_by_key, idempotency,
                                           _version)
            unclaimed = [(ref, intent) for ref, intent in job_intents
                         if ref not in claimed]
            if not unclaimed:
                # An active plan is occupied; a terminal one is finished.
                if any(plan["state"] == "active"
                       for plan in migration_plans):
                    raise PermissionError("the intent already holds an "
                                          "active migration plan")
                raise ValueError("the intent's migration plan is "
                                 "finished")
            if job_id in settlement_pending:
                # A pending settlement must be resumed before another
                # migration round may start.
                raise ValueError("the job still holds a pending "
                                 "settlement")
            intent_ref, intent = sorted(
                unclaimed, key=lambda item: (item[1]["at"], item[0]))[-1]
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
            # The plan is keyed by its own start idempotency key and the
            # request names the reserved intent it claims, so every
            # earlier generation's plan, receipts and keys stay
            # untouched.
            request["intent_key"] = intent_ref
            plans[key] = new_plan
            idempotency[key] = request
            events[key] = {"key": key, "request": dict(request),
                           "result": copy.deepcopy(new_plan)}
            _commit_file(ledger_real,
                         _intent_canonical_bytes(intents, plans,
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
    anything but the next pending step raises ``ValueError``, and so
    does a receipt moment earlier than the plan start or earlier than
    the previous receipt. A
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
        lifecycle_reals = {job_real, supply_real, signal_real,
                           trades_real, dispatch_real, execution_real,
                           advice_real, ledger_real}
        settlement_extras, settlement_paths = _lifecycle_extra_locks(
            lifecycle_reals)
        with contextlib.ExitStack() as stack:
            for locked in sorted(lifecycle_reals | settlement_extras):
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
            plans_by_key_settle = _plans_by_start_key(
                plans, idempotency, _version)
            settlement_completed, settlement_pending = \
                _lifecycle_settlement_view(
                    settlement_paths, accepted, cleared,
                    plans_by_key_settle, idempotency, events, _version)
            for advice_record in advice_records.values():
                _ground_advice_current(
                    advice_record, cleared,
                    _settlement_currents(settlement_completed))

            request = {"action": "record", "job_id": job_id,
                       "owner": owner, "step": step, "result": result,
                       "receipt": receipt, "at": at}
            binding = idempotency.get(key)
            if binding is not None:
                if _request_conflicts(binding, request):
                    raise ValueError("idempotency key was already used "
                                     "with a different request")
                # An equivalent replay returns the current state of the
                # plan this very key recorded a step for -- never a
                # later generation's plan -- without rewriting a byte.
                if _version == _INTENT_VERSION_V3:
                    return copy.deepcopy(plans[binding["plan_key"]]), False
                return copy.deepcopy(plans[job_id]), False

            if job_id not in accepted:
                raise KeyError(job_id)
            if not _job_intent_items(intents, _version, job_id):
                raise KeyError(job_id)
            plans_by_key = _plans_by_start_key(plans, idempotency,
                                               _version)
            job_plans = [(start_key, plan)
                         for start_key, plan in plans_by_key.items()
                         if plan["job_id"] == job_id]
            if not job_plans:
                raise ValueError("the intent has no migration plan yet")
            active = [(start_key, plan) for start_key, plan in job_plans
                      if plan["state"] == "active"]
            if not active:
                raise ValueError("plan is not active")
            plan_key, plan = active[0]
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
            if _version == _INTENT_VERSION_V3:
                # The request names the plan it acted on by the plan's
                # own start key.
                request["plan_key"] = plan_key
            idempotency[key] = request
            events[key] = {"key": key, "request": dict(request),
                           "result": copy.deepcopy(plan)}
            _commit_file(ledger_real,
                         _intent_canonical_bytes(intents, plans,
                                                 idempotency, events,
                                                 _version),
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
        lifecycle_reals = {job_real, supply_real, signal_real,
                           trades_real, dispatch_real, execution_real,
                           advice_real, ledger_real}
        settlement_extras, settlement_paths = _lifecycle_extra_locks(
            lifecycle_reals)
        with contextlib.ExitStack() as stack:
            for locked in sorted(lifecycle_reals | settlement_extras):
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
            plans_by_key_settle = _plans_by_start_key(
                plans, idempotency, _version)
            settlement_completed, settlement_pending = \
                _lifecycle_settlement_view(
                    settlement_paths, accepted, cleared,
                    plans_by_key_settle, idempotency, events, _version)
            for advice_record in advice_records.values():
                _ground_advice_current(
                    advice_record, cleared,
                    _settlement_currents(settlement_completed))

            request = {"action": "recover", "job_id": job_id,
                       "owner": owner, "at": at}
            binding = idempotency.get(key)
            if binding is not None:
                if _request_conflicts(binding, request):
                    raise ValueError("idempotency key was already used "
                                     "with a different request")
                # An equivalent replay returns the current state of the
                # plan this very key recovered -- never a later
                # generation's plan -- without rewriting a byte.
                if _version == _INTENT_VERSION_V3:
                    return copy.deepcopy(plans[binding["plan_key"]]), False
                return copy.deepcopy(plans[job_id]), False

            if job_id not in accepted:
                raise KeyError(job_id)
            if not _job_intent_items(intents, _version, job_id):
                raise KeyError(job_id)
            plans_by_key = _plans_by_start_key(plans, idempotency,
                                               _version)
            job_plans = [(start_key, plan)
                         for start_key, plan in plans_by_key.items()
                         if plan["job_id"] == job_id]
            if not job_plans:
                raise ValueError("the intent has no migration plan yet")
            active = [(start_key, plan) for start_key, plan in job_plans
                      if plan["state"] == "active"]
            if not active:
                raise ValueError("plan is not active")
            plan_key, plan = active[0]
            if plan["owner"] != owner:
                raise PermissionError("recover requires the plan owner")
            if at <= plan["lease_end"]:
                raise PermissionError("the lease has not expired yet")

            plan["state"] = "interrupted"
            if _version == _INTENT_VERSION_V3:
                # The request names the plan it acted on by the plan's
                # own start key.
                request["plan_key"] = plan_key
            idempotency[key] = request
            events[key] = {"key": key, "request": dict(request),
                           "result": copy.deepcopy(plan)}
            _commit_file(ledger_real,
                         _intent_canonical_bytes(intents, plans,
                                                 idempotency, events,
                                                 _version),
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


def _terminal_plan_at(
    plan: dict[str, Any],
    intent_events: dict[str, dict[str, Any]],
    plan_key: str,
    intent_version: int,
) -> int:
    # The terminal evidence moment: the recorded recovery moment for an
    # interrupted plan, otherwise the last committed step receipt.
    if plan["state"] == "interrupted":
        if intent_version == _INTENT_VERSION_V3:
            moments = sorted(
                event["request"]["at"] for event in intent_events.values()
                if event["request"].get("action") == "recover"
                and event["request"].get("plan_key") == plan_key)
        else:
            moments = sorted(
                event["request"]["at"] for event in intent_events.values()
                if event["request"].get("action") == "recover"
                and event["request"].get("job_id") == plan["job_id"])
        if len(moments) != 1:
            raise ValueError("an interrupted plan must reference exactly "
                             "one recovery receipt")
        return moments[0]
    if not plan["steps"]:
        raise ValueError("a terminal plan must carry a terminal receipt")
    return plan["steps"][-1]["at"]


def _settlement_audit_keys(
    intent_idempotency: dict[str, dict[str, Any]],
    job_id: str,
    plan_key: str,
    intent_version: int,
) -> list[str]:
    # The audit association binds a settlement to every intent-ledger
    # action committed for its migration generation: the reservation,
    # the claim, the step receipts and the recovery of its own plan.
    # It is fully determined by that ledger, so any divergence is
    # detected on readback.
    if intent_version == _INTENT_VERSION_V3:
        keys = {plan_key}
        start_request = intent_idempotency.get(plan_key)
        if start_request is not None \
                and start_request.get("action") == "start":
            intent_key = start_request.get("intent_key")
            if isinstance(intent_key, str) and intent_key:
                keys.add(intent_key)
        keys.update(key for key, request in intent_idempotency.items()
                    if request.get("plan_key") == plan_key)
        return sorted(keys)
    return sorted(key for key, request in intent_idempotency.items()
                  if request.get("job_id") == job_id)


def _validate_settlement_record(
    record: object,
    accepted: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
    plans_by_key: dict[str, dict[str, Any]],
    intent_idempotency: dict[str, dict[str, Any]],
    intent_events: dict[str, dict[str, Any]],
    intent_version: int,
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

    job = accepted.get(job_id)
    if job is None:
        raise ValueError("settlement record must reference an accepted "
                         "job")
    trade = trades.get(job_id)
    if trade is None:
        raise ValueError("settlement record must reference a recorded "
                         "trade")
    plan = plans_by_key.get(plan_key)
    if plan is None or plan["job_id"] != job_id:
        raise ValueError("settlement record must reference a recorded "
                         "migration plan")

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

    # The terminal evidence must agree with itself: the plan state, the
    # before/after resources and the moment all follow one snapshot.
    if plan["state"] != migration:
        raise ValueError("settlement migration state must match the "
                         "terminal plan state")
    if before != plan["source"]:
        raise ValueError("settlement previous binding must match the "
                         "plan source")
    expected_after = (plan["target"] if migration == "migrated"
                      else plan["source"])
    if after != expected_after:
        raise ValueError("settlement current binding must match the "
                         "terminal plan outcome")
    if at < _terminal_plan_at(plan, intent_events, plan_key,
                              intent_version):
        raise ValueError("settlement moment must not precede the "
                         "terminal plan evidence")

    if audit_raw != _settlement_audit_keys(
            intent_idempotency, job_id, plan_key, intent_version):
        raise ValueError("settlement audit association does not match "
                         "the intent ledger history")

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
    plans_by_key: dict[str, dict[str, Any]],
    intent_idempotency: dict[str, dict[str, Any]],
    intent_events: dict[str, dict[str, Any]],
    intent_version: int,
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

    # Each migration plan settles at most once: two settlement records
    # referencing the same plan would duplicate a close and break the
    # generation sequence, while one job may hold a continuous sequence
    # of generations distinguished by their plans.
    records: dict[str, dict[str, Any]] = {}
    settled_plans: set[tuple[str, str]] = set()
    for key, record_raw in records_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("settlement keys must be non-empty strings")
        record = _validate_settlement_record(
            record_raw, accepted, trades, plans_by_key, intent_idempotency,
            intent_events, intent_version)
        identity = (record["job_id"], record["plan_key"])
        if identity in settled_plans:
            raise ValueError("a terminal migration can only settle once")
        settled_plans.add(identity)
        records[key] = record

    # The generations of one job form one continuous chain: the first
    # completed record runs generation 1 leaving the traded version,
    # every later completed record continues from the previous one's
    # real after-binding, and a pending record is the next generation in
    # waiting -- never a completed one. The idempotency key order carries
    # no meaning for the sequence.
    records_by_job: dict[str, list[dict[str, Any]]] = {}
    for record in records.values():
        records_by_job.setdefault(record["job_id"], []).append(record)
    for job_id, job_records in records_by_job.items():
        completed = sorted(
            (record for record in job_records
             if record["state"] in _SETTLE_FINAL_STATES),
            key=lambda record: record["generation"])
        pending = [record for record in job_records
                   if record["state"] == "pending"]
        for index, record in enumerate(completed):
            if record["generation"] != index + 1:
                raise ValueError("settlement generations must be "
                                 "continuous")
        if len(pending) > 1:
            raise ValueError("a job can hold only one pending "
                             "settlement")
        if pending and pending[0]["generation"] != len(completed) + 1:
            raise ValueError("a pending settlement must continue the "
                             "completed generations")
        trade = trades[job_id]
        previous_after = {"resource_id": trade["resource_id"],
                          "version": trade["version"]}
        for record in completed:
            if record["before"] != previous_after:
                raise ValueError("settlement generations must be "
                                 "continuous")
            previous_after = record["after"]
        if pending and pending[0]["before"] != previous_after:
            raise ValueError("a pending settlement must continue the "
                             "completed generations")

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
            event_raw["result"], accepted, trades, plans_by_key,
            intent_idempotency, intent_events, intent_version)
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
    plans_by_key: dict[str, dict[str, Any]],
    intent_idempotency: dict[str, dict[str, Any]],
    intent_events: dict[str, dict[str, Any]],
    intent_version: int,
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
        data, accepted, trades, plans_by_key, intent_idempotency,
        intent_events, intent_version)
    if raw != _settlement_canonical_bytes(records, idempotency, events):
        raise ValueError(
            f"settlement ledger {realpath!r} is not in canonical "
            "compact form")
    return records, idempotency, events, raw


def _lineage_from_settlement_records(
    records: dict[str, dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], set[str]]:
    # Fold one validated settlement ledger into the completed-binding
    # history (generation -> record) and the set of jobs that still hold
    # a pending settlement. Pending records are not completed
    # generations: they neither source a new round nor appear in current,
    # but they do block another settlement or a follow-up migration.
    completed: dict[str, list[dict[str, Any]]] = {}
    pending: set[str] = set()
    for record in records.values():
        job_id = record["job_id"]
        if record["state"] == "pending":
            pending.add(job_id)
        else:
            completed.setdefault(job_id, []).append(record)
    return completed, pending


def _settlement_currents(
    completed: dict[str, list[dict[str, Any]]],
) -> dict[str, set[tuple[str, int, int]]]:
    # The completed bindings each advice round may root at, as
    # (resource_id, version, settlement moment) triples.
    currents: dict[str, set[tuple[str, int, int]]] = {}
    for job_id, records in completed.items():
        currents[job_id] = {
            (record["after"]["resource_id"], record["after"]["version"],
             record["at"])
            for record in records
        }
    return currents


def _latest_completed_binding(
    completed: dict[str, list[dict[str, Any]]],
    job_id: str,
    at: int | None = None,
) -> dict[str, Any] | None:
    # The highest-generation completed record, optionally only those
    # confirmed at or before ``at``. Generations are validated as
    # continuous, so the highest one is the job's current binding.
    items = [record for record in completed.get(job_id, ())
             if at is None or record["at"] <= at]
    if not items:
        return None
    return max(items, key=lambda record: record["generation"])

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
) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]],
           dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]],
           dict[str, dict[str, dict[str, Any]]],
           dict[str, dict[str, Any]],
           dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]], dict[str, dict[str, Any]], int]:
    # The same eight layers every lifecycle call reads as one snapshot;
    # the intent ledger is a required input here because settlement
    # presupposes a recorded plan -- a missing file is distinct from a
    # missing intent record (the latter is KeyError at the call site).
    (accepted, history, signal_history, cleared, decisions, exec_plans,
     advice_records) = _load_snapshot_layers(
        job_real, supply_real, signal_real, trades_real, dispatch_real,
        execution_real, advice_real)
    intents, plans, intent_idempotency, intent_events, intent_version, \
        intent_raw = _load_intent_ledger(
            ledger_real, accepted, cleared, history, signal_history,
            advice_records)
    if intent_raw is None:
        raise FileNotFoundError(
            f"intent ledger {ledger_real!r} does not exist")
    return (accepted, history, signal_history, cleared, decisions,
            exec_plans, advice_records, intents, plans, intent_idempotency,
            intent_events, intent_version)


def _ground_advice_current(
    record: dict[str, Any],
    trades: dict[str, dict[str, Any]],
    currents: dict[str, set[tuple[str, int, int]]],
) -> None:
    # Complete the deferred current-binding grounding of one advice
    # record once the completed settlement lineage is known.
    trade = trades[record["job_id"]]
    allowed = {(trade["resource_id"], trade["version"])}
    allowed |= {(rid, ver) for rid, ver, settled_at
                in currents.get(record["job_id"], set())
                if settled_at <= record["at"]}
    current = record["current"]
    if (current["resource_id"], current["version"]) not in allowed:
        raise ValueError("advice current selection must match the job's "
                         "current binding")


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

    The nine paths -- the eight lifecycle paths plus the independent
    settlement ledger -- ``job_id``, ``plan_key`` and ``key`` must be
    non-empty strings resolving to distinct real locations and ``at`` a
    non-boolean non-negative integer settlement moment; any violation
    raises ``ValueError`` before a business file is read.

    The existing eight layers are read as one snapshot under their
    shared locks together with the settlement ledger's exclusive lock,
    all nine taken in resolved real-path order. None of the eight input
    files is ever rewritten; the settlement lands only in the
    independent ledger. ``plan_key`` must be the idempotency key of the
    ``start`` request that created the job's migration plan.

    Only a terminal plan settles: an ``active`` plan raises
    ``PermissionError`` and creates no record; a missing job, intent or
    plan raises ``KeyError``. A settlement moment earlier than the
    terminal evidence -- the last step receipt, or the recovery receipt
    for an interrupted plan -- raises ``ValueError``; a legitimate late
    recovery past the job deadline may still settle, so no deadline
    timeout is enforced here. Contradictory terminal evidence, the same
    key carrying a changed request, another key settling the same
    migration, a generation break or contradictory snapshot references
    raise ``ValueError``.

    A ``migrated`` plan confirms the target version as the job's current
    occupancy and records the source version as the previous-generation
    binding (state ``active``); a ``failed`` or ``interrupted`` plan
    forms a compensation that keeps the current occupancy on the source
    version and releases the target reservation (state
    ``compensated``). The record freezes the continuous running
    generation, the source terminal state, the previous and current
    resource selections, the settlement moment, the state and the audit
    association with every intent-ledger action of the migration.

    The first request is persisted durably as ``pending`` -- record,
    idempotency binding and audit event in one synced atomic write -- and
    only then advanced to ``active`` or ``compensated`` in a second
    synced atomic write. If the process dies after the pending write,
    re-entering with the same key and request resumes the unfinished
    stage, completes it and still returns the record with ``True``,
    never repeating a committed occupancy, generation or audit action.
    Only an already completed same-key same-request call returns the
    current record with ``False`` and writes no bytes.

    Missing input files or the settlement ledger parent raise
    ``FileNotFoundError``; invalid arguments or ledger bytes raise
    ``ValueError``; other locking or I/O failures raise ``OSError``.
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

    store = _get_store(settlement_real)
    with store.lock:
        with contextlib.ExitStack() as stack:
            # The settlement ledger is the only written file and takes
            # the one exclusive lock; every snapshot lock is shared, all
            # nine in the single resolved-real-path order.
            for locked in sorted({job_real, supply_real, signal_real,
                                  trades_real, dispatch_real,
                                  execution_real, advice_real, ledger_real,
                                  settlement_real}):
                stack.enter_context(
                    _lock(locked, shared=(locked != settlement_real)))

            (accepted, _history, _signal_history, cleared, _decisions,
             _exec_plans, _advice_records, intents, plans,
             intent_idempotency, intent_events, intent_version) = \
                _load_settle_snapshot(
                    job_real, supply_real, signal_real, trades_real,
                    dispatch_real, execution_real, advice_real,
                    ledger_real)
            plans_by_key = _plans_by_start_key(
                plans, intent_idempotency, intent_version)
            records, idempotency, events, old_bytes = \
                _load_settlement_ledger(
                    settlement_real, accepted, cleared, plans_by_key,
                    intent_idempotency, intent_events, intent_version)
            # Complete the deferred advice grounding now that the
            # completed settlement lineage is loaded.
            for advice_record in _advice_records.values():
                _ground_advice_current(
                    advice_record, cleared,
                    _settlement_currents(
                        _lineage_from_settlement_records(records)[0]))

            request = {"job_id": job_id, "plan_key": plan_key, "at": at}
            existing = idempotency.get(key)
            resume = existing is not None
            if resume:
                if existing != request:
                    raise ValueError("idempotency key was already used "
                                     "with a different request")
                stored = records[key]
                if stored["state"] != "pending":
                    # An already completed same-key same-request call
                    # replays the stored record without writing a byte.
                    return copy.deepcopy(stored), False

            if not resume:
                if job_id not in accepted:
                    raise KeyError(job_id)
                if not _job_intent_items(intents, intent_version, job_id):
                    raise KeyError(job_id)
                plan = plans_by_key.get(plan_key)
                if plan is None or plan["job_id"] != job_id:
                    raise KeyError(plan_key)
                if any(record["job_id"] == job_id
                       and record["plan_key"] == plan_key
                       for record in records.values()):
                    raise ValueError("the terminal migration is already "
                                     "settled under another idempotency "
                                     "key")
                if any(record["job_id"] == job_id
                       and record["state"] == "pending"
                       for record in records.values()):
                    raise ValueError("the job still holds a pending "
                                     "settlement")
                if plan["state"] == "active":
                    raise PermissionError("the migration plan is still "
                                          "active")
                if plan["state"] not in _SETTLE_TERMINAL:
                    raise ValueError("the migration plan is not in a "
                                     "terminal state")
                if at < _terminal_plan_at(plan, intent_events, plan_key,
                                          intent_version):
                    raise ValueError("settlement moment must not precede "
                                     "the terminal plan evidence")
                completed = [record for record in records.values()
                             if record["job_id"] == job_id
                             and record["state"] in _SETTLE_FINAL_STATES]
                # The first completed settlement runs generation 1 and
                # every later one continues the sequence, independent of
                # the idempotency key order.
                generation = 1 + max(
                    (record["generation"] for record in completed),
                    default=0)
                if completed:
                    previous = max(
                        completed,
                        key=lambda record: record["generation"])
                    expected_before = previous["after"]
                else:
                    trade = cleared[job_id]
                    expected_before = {
                        "resource_id": trade["resource_id"],
                        "version": trade["version"]}
                if plan["source"] != expected_before:
                    raise ValueError("settlement source does not continue "
                                     "the previous generation")
            else:
                # Resume the durable pending settlement: the plan it
                # bound must still be the same terminal migration, and
                # every frozen field must still follow that snapshot.
                pending = records[key]
                plan = plans_by_key.get(plan_key)
                if plan is None or plan["job_id"] != job_id \
                        or not _job_intent_items(intents, intent_version,
                                                 job_id):
                    raise ValueError("a pending settlement lost its "
                                     "migration plan")
                if plan["state"] not in _SETTLE_TERMINAL:
                    raise ValueError("a pending settlement contradicts "
                                     "the migration plan state")
                if at < _terminal_plan_at(plan, intent_events, plan_key,
                                          intent_version):
                    raise ValueError("settlement moment must not precede "
                                     "the terminal plan evidence")
                generation = pending["generation"]

            migration = plan["state"]
            final_state = ("active" if migration == "migrated"
                           else "compensated")
            before = copy.deepcopy(plan["source"])
            after = copy.deepcopy(plan["target"]
                                  if migration == "migrated"
                                  else plan["source"])
            audit_keys = _settlement_audit_keys(
                intent_idempotency, job_id, plan_key, intent_version)
            final_record: dict[str, Any] = {
                "job_id": job_id,
                "plan_key": plan_key,
                "generation": generation,
                "migration": migration,
                "before": before,
                "after": after,
                "at": at,
                "state": final_state,
                "audit": audit_keys,
            }
            # Full independent validation against the snapshot before
            # the finalization is accepted on either path.
            _validate_settlement_record(
                final_record, accepted, cleared, plans_by_key,
                intent_idempotency, intent_events, intent_version)

            if resume:
                for field in ("job_id", "plan_key", "generation",
                              "migration", "before", "after", "at",
                              "audit"):
                    if pending[field] != final_record[field]:
                        raise ValueError("pending settlement contradicts "
                                         "the terminal plan evidence")
                stage_a_bytes = old_bytes
            else:
                pending_record = dict(final_record, state="pending")
                records[key] = pending_record
                idempotency[key] = dict(request)
                events[key] = {"key": key, "request": dict(request),
                               "result": copy.deepcopy(pending_record)}
                stage_a_bytes = _settlement_canonical_bytes(
                    records, idempotency, events)
                # Stage A: the pending settlement, its binding and its
                # audit event durably first.
                _commit_file(settlement_real, stage_a_bytes, old_bytes,
                             prefix=".rebalance-settle-")

            # Stage B: advance the persisted pending record to its final
            # state. The rollback base is the pending bytes (on a fresh
            # call stage A's bytes, on resume the bytes read from disk),
            # so a crash here leaves a resumable pending settlement
            # rather than a half-finalized one.
            records[key] = final_record
            events[key] = {"key": key, "request": dict(request),
                           "result": copy.deepcopy(final_record)}
            _commit_file(
                settlement_real,
                _settlement_canonical_bytes(records, idempotency, events),
                stage_a_bytes, prefix=".rebalance-settle-final-")
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
    """Read the job's latest resource binding without writing anything.

    The nine paths and ``job_id`` must be non-empty strings and the nine
    paths must resolve to distinct real locations; any violation raises
    ``ValueError``. All nine files are read under shared locks in
    resolved real-path order, and no file is created or modified. A job
    whose terminal migration was settled returns the settlement's
    current binding -- the target version for an ``active`` settlement,
    the source version for a ``compensated`` one. A job without a
    completed settlement returns the immutable version its trade froze.

    An unknown job raises ``KeyError``; an accepted job without a
    recorded trade raises ``LookupError``. Invalid arguments or ledger
    bytes raise ``ValueError``; missing input files raise
    ``FileNotFoundError``; other locking or I/O failures raise
    ``OSError``.
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
            # A pure read: every lock, including the settlement
            # ledger's, is shared.
            for locked in sorted({job_real, supply_real, signal_real,
                                  trades_real, dispatch_real,
                                  execution_real, advice_real, ledger_real,
                                  settlement_real}):
                stack.enter_context(_lock(locked, shared=True))

            (accepted, _history, _signal_history, cleared, _decisions,
             _exec_plans, _advice_records, _intents, plans,
             intent_idempotency, intent_events, intent_version) = \
                _load_settle_snapshot(
                    job_real, supply_real, signal_real, trades_real,
                    dispatch_real, execution_real, advice_real,
                    ledger_real)
            if job_id not in accepted:
                raise KeyError(job_id)
            trade = cleared.get(job_id)
            if trade is None:
                raise LookupError("job has no recorded trade")
            plans_by_key = _plans_by_start_key(
                plans, intent_idempotency, intent_version)
            records, _idempotency, _events, _raw = \
                _load_settlement_ledger(
                    settlement_real, accepted, cleared, plans_by_key,
                    intent_idempotency, intent_events, intent_version)
            # Finish the deferred advice grounding so a tampered advice
            # ledger is still rejected on this read-only path.
            for advice_record in _advice_records.values():
                _ground_advice_current(
                    advice_record, cleared,
                    _settlement_currents(
                        _lineage_from_settlement_records(records)[0]))
            finalized = [record for record in records.values()
                         if record["job_id"] == job_id
                         and record["state"] in _SETTLE_FINAL_STATES]
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
