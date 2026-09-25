"""Coordinated reconciliation of terminal execution plans with dispatch.

The execution layer can leave a decision claimed past its work: a plan
may have run to ``completed`` or ``failed`` without the dispatch
decision being finished, or an active plan may have been
``interrupted`` once its lease strictly expired. This module adds the
persistent reconciliation on top of those two existing ledgers.

One on-disk document (version 1), the coordination ledger, holds:

* ``batches`` maps each batch key to one fixed batch -- the owner that
  may continue it while its coordination lease is live, that lease's
  end, a lifecycle state (``pending`` or ``completed``) and one item per
  terminal plan the batch snapped up at creation, each item going
  ``pending`` -> ``applied`` while recording the action, the historical
  action moment and the returned dispatch decision (or the public error
  class while the dispatch call keeps failing);
* ``audit`` holds one event per completed batch, appended only in the
  same commit that settles the batch, binding the batch key to the full
  fixed batch snapshot.

:func:`run` first scans the execution ledger -- plans ordered by job id
and attempt -- and collects only the terminal plans (``completed``,
``failed`` or ``interrupted``) no earlier batch has synced. Completed
and failed plans finish the dispatch decision as succeeded or failed;
interrupted plans recover the expired claim. The action moment is the
last step receipt's moment for a finish and the recorded recovery
moment for a recover, so retries can never rewrite that history. Each
item is persisted ``pending`` before the existing dispatch interface is
called; the dispatch idempotency key is derived stably from the batch
key and the plan identity, so a crashed call replays the very same
request. Only a successful call or an equivalent replay turns the item
``applied``; a failure records the public exception class, keeps the
item pending and re-raises the original exception with its chain.

A batch fixes its first scan: re-entry continues the unfinished items
and a completed batch replays byte-for-byte; later terminal plans are
picked up by a new batch key, and an empty batch still commits an
explicit completed state and audit snapshot. Only the current owner may
continue an active batch; another owner must wait for the strict lease
expiry before taking over, and a takeover never repeats an applied
item. The three ledgers are locked by resolved real path in one order;
the coordination ledger is one compact UTF-8 JSON document -- version,
sorted batches and an append-only audit -- with non-ASCII written
through, decimal integers and exactly one trailing newline.
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
from ._jsonio import finite_loads

__all__ = ["run"]

_VERSION = 1
_ROOT_FIELDS = ("version", "batches", "audit")
_BATCH_FIELDS = ("key", "owner", "until", "status", "items")
_ITEM_FIELDS = ("job_id", "attempt", "state", "action", "at", "status",
                "error", "decision")
_EVENT_FIELDS = ("key", "batch")
_PLAN_TERMINAL = ("completed", "failed", "interrupted")
_BATCH_STATUSES = ("pending", "completed")
_ITEM_STATUSES = ("pending", "applied")
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


def _item_order_key(item: dict[str, Any]) -> tuple[str, int]:
    return item["job_id"], item["attempt"]


def _ordered_items(batch: dict[str, Any]) -> list[dict[str, Any]]:
    # The fixed processing order is job id then numeric attempt; the
    # on-disk item map itself stays code-point sorted.
    return sorted(batch["items"].values(), key=_item_order_key)


def _action_for(state: str) -> str:
    return "recover" if state == "interrupted" else "finish"


def _provenance_at(
    plan: dict[str, Any],
    events: dict[str, dict[str, Any]],
) -> int:
    # The action moment is history, never the current clock: the last
    # recorded step receipt for a finished plan, or the moment the
    # execution recovery audit first recorded for an interrupted plan.
    if plan["state"] != "interrupted":
        return plan["steps"][-1]["at"]
    slot = (plan["job_id"], plan["attempt"])
    for event in events.values():
        request = event["request"]
        if request["action"] == "recover" \
                and (event["result"]["job_id"],
                     event["result"]["attempt"]) == slot:
            return request["at"]
    raise ValueError("interrupted plan has no recovery audit event")


def _validate_item(
    record: object,
    plans: dict[str, dict[str, dict[str, Any]]],
    events: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(record, dict) \
            or set(record.keys()) != set(_ITEM_FIELDS):
        raise ValueError("batch item has invalid fields")
    job_id = record["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("batch item job_id must be a non-empty string")
    attempt = record["attempt"]
    if not _is_plain_int(attempt) or attempt < 1:
        raise ValueError("batch item attempt must be a positive integer")
    state = record["state"]
    if state not in _PLAN_TERMINAL:
        raise ValueError("batch item state is invalid")
    action = record["action"]
    expected_action = _action_for(state)
    if action != expected_action:
        raise ValueError("batch item action does not match its plan state")
    at = record["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("batch item at must be a non-boolean non-negative "
                         "integer")
    status = record["status"]
    if status not in _ITEM_STATUSES:
        raise ValueError("batch item status is invalid")
    error = record["error"]
    decision_raw = record["decision"]

    attempts = plans.get(job_id)
    plan = attempts.get(str(attempt)) if attempts is not None else None
    if plan is None:
        raise ValueError("batch item must reference a recorded plan")
    if plan["state"] != state:
        raise ValueError("batch item state does not match its plan")
    # The persisted action moment must stay the original historical
    # value; a re-entry can never rewrite it.
    if at != _provenance_at(plan, events):
        raise ValueError("batch item action moment does not match the "
                         "plan history")

    if status == "pending":
        # A pending item either has not been attempted yet (no error)
        # or keeps the public class of the last failed dispatch call.
        if error is not None and (not isinstance(error, str) or not error):
            raise ValueError("a pending item error must be null or a "
                             "non-empty string")
        if decision_raw is not None:
            raise ValueError("a pending item must not carry a decision")
        decision = None
    else:
        if error is not None:
            raise ValueError("an applied item must not carry an error")
        decision = _dispatch._validate_decision(decision_raw)
        if decision["job_id"] != job_id:
            raise ValueError("item decision does not match its job")
        if decision["attempts"] != attempt:
            raise ValueError("item decision attempt does not match the plan")
        if action == "finish":
            expected_result = ("succeeded" if state == "completed"
                               else "failed")
            if decision["state"] != expected_result:
                raise ValueError("finish decision does not match the "
                                 "plan state")
        elif decision["state"] != "ready":
            raise ValueError("a recovered decision must be ready")

    return {
        "job_id": job_id,
        "attempt": attempt,
        "state": state,
        "action": action,
        "at": at,
        "status": status,
        "error": error,
        "decision": decision,
    }


def _validate_ledger(
    data: object,
    plans: dict[str, dict[str, dict[str, Any]]],
    events: dict[str, dict[str, Any]],
    decisions: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("coordination ledger root must be an object with "
                         "keys version, batches and audit")
    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported coordination ledger version")

    batches_raw = data["batches"]
    audit_raw = data["audit"]
    if not isinstance(batches_raw, dict) or not isinstance(audit_raw, dict):
        raise ValueError("batches and audit must be objects")
    _check_sorted_keys(batches_raw, "batches")
    _check_sorted_keys(audit_raw, "audit")

    batches: dict[str, dict[str, Any]] = {}
    synced: set[tuple[str, int]] = set()
    completed: set[str] = set()
    for batch_key, batch_raw in batches_raw.items():
        if not isinstance(batch_key, str) or not batch_key:
            raise ValueError("batch keys must be non-empty strings")
        if not isinstance(batch_raw, dict) \
                or set(batch_raw.keys()) != set(_BATCH_FIELDS):
            raise ValueError("batch has invalid fields")
        if batch_raw["key"] != batch_key \
                or not isinstance(batch_raw["key"], str):
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
        items_raw = batch_raw["items"]
        if not isinstance(items_raw, dict):
            raise ValueError("batch items must be an object")
        _check_sorted_keys(items_raw, "batch items")

        items: dict[str, dict[str, Any]] = {}
        for item_key, item_raw in items_raw.items():
            item = _validate_item(item_raw, plans, events)
            if item["job_id"] not in decisions:
                raise ValueError("batch item must reference a dispatch "
                                 "decision")
            if item_key != f"{item['job_id']}\t{item['attempt']}":
                raise ValueError("batch item key does not match its "
                                 "job and attempt")
            slot = (item["job_id"], item["attempt"])
            if slot in synced:
                raise ValueError("a plan may be synced by at most one "
                                 "batch item")
            synced.add(slot)
            items[item_key] = item

        if status == "completed":
            if any(item["status"] != "applied" for item in items.values()):
                raise ValueError("a completed batch may not hold pending "
                                 "items")
            completed.add(batch_key)

        batches[batch_key] = {
            "key": batch_key,
            "owner": owner,
            "until": until,
            "status": status,
            "items": items,
        }

    audit: dict[str, dict[str, Any]] = {}
    for event_key, event_raw in audit_raw.items():
        if not isinstance(event_key, str) or not event_key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event_raw, dict) \
                or set(event_raw.keys()) != set(_EVENT_FIELDS):
            raise ValueError("coordination audit event has invalid fields")
        if event_raw["key"] != event_key:
            raise ValueError("audit event key does not match its map key")
        batch = batches.get(event_key)
        if batch is None:
            raise ValueError("audit event must reference a recorded batch")
        if event_raw["batch"] != batch:
            raise ValueError("audit event batch does not match its batch")
        audit[event_key] = {"key": event_key, "batch": copy.deepcopy(batch)}

    # Exactly one terminal audit event per completed batch and none for
    # a batch still in progress.
    if set(audit) != completed:
        raise ValueError("audit events must match the completed batches")

    return batches, audit


def _items_map(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    # The map order is irrelevant in memory; _canonical_bytes emits the
    # keys code-point sorted when persisting.
    return {f"{item['job_id']}\t{item['attempt']}": item for item in items}


def _canonical_batch(batch: dict[str, Any]) -> dict[str, Any]:
    return {
        "key": batch["key"],
        "owner": batch["owner"],
        "until": batch["until"],
        "status": batch["status"],
        "items": {item_key: copy.deepcopy(batch["items"][item_key])
                  for item_key in sorted(batch["items"])},
    }


def _canonical_bytes(
    batches: dict[str, dict[str, Any]],
    audit: dict[str, dict[str, Any]],
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, sections in their
    # fixed field order, batches/audit and each batch's items in
    # code-point order, terminated by exactly one newline.
    payload = {
        "version": _VERSION,
        "batches": {key: _canonical_batch(batches[key])
                    for key in sorted(batches)},
        "audit": {key: {"key": key,
                        "batch": _canonical_batch(audit[key]["batch"])}
                  for key in sorted(audit)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_ledger(
    realpath: str,
    plans: dict[str, dict[str, dict[str, Any]]],
    events: dict[str, dict[str, Any]],
    decisions: dict[str, dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return {}, {}, None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"coordination ledger {realpath!r} is not valid UTF-8") from exc
    try:
        # Negative-zero and non-finite literals are format errors.
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"coordination ledger {realpath!r} is not valid JSON") from exc
    batches, audit = _validate_ledger(data, plans, events, decisions)
    # As for the other ledgers, the ledger is accepted only in canonical
    # compact form with a single trailing newline.
    if raw != _canonical_bytes(batches, audit):
        raise ValueError(
            f"coordination ledger {realpath!r} is not in canonical compact "
            "form")
    return batches, audit, raw


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
                dir=directory, prefix=".execution-sync-restore-",
                suffix=".tmp")
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
        dir=directory, prefix=".execution-sync-", suffix=".tmp")
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


def _snapshot(batch: dict[str, Any]) -> dict[str, Any]:
    # A fixed-order deep copy in the batch's processing order -- job id
    # then numeric attempt, independent of the on-disk code-point order
    # -- and the caller can never mutate the stored object.
    snap = {field: batch[field] for field in _BATCH_FIELDS if field != "items"}
    snap["items"] = {f"{item['job_id']}\t{item['attempt']}":
                     copy.deepcopy(item)
                     for item in _ordered_items(batch)}
    return {field: snap[field] for field in _BATCH_FIELDS}


def _expected_dispatch_request(
    item: dict[str, Any],
    plan: dict[str, Any],
) -> dict[str, Any]:
    # The exact request the existing dispatch interface binds to the
    # stable dispatch key; a crash re-entry must find precisely this.
    if item["action"] == "finish":
        return {
            "action": "finish",
            "job_id": item["job_id"],
            "owner": plan["owner"],
            "result": ("succeeded" if item["state"] == "completed"
                       else "failed"),
            "at": item["at"],
        }
    return {"action": "recover", "job_id": item["job_id"], "at": item["at"]}


def _expected_decision_state(item: dict[str, Any]) -> str:
    if item["action"] == "recover":
        return "ready"
    return "succeeded" if item["state"] == "completed" else "failed"


def _check_pending_item(
    item: dict[str, Any],
    plan: dict[str, Any],
    decisions: dict[str, dict[str, Any]],
    dispatch_bindings: dict[str, dict[str, Any]],
    dispatch_events: dict[str, dict[str, Any]],
    dispatch_key: str,
) -> None:
    # Before a dispatch interface is touched, the decision the item is
    # about to settle must be referenced. If the stable dispatch key has
    # never been served, the decision must still be the claim the plan
    # was made against -- same attempt, owner and lease, with the action
    # moment inside (finish) or strictly past (recover) the lease. If the
    # key was served before (a crash between the dispatch commit and the
    # applied marker), the dispatch ledger must already bind exactly the
    # request and the result this item expects. Any mismatch raises
    # ValueError without advancing the batch.
    decision = decisions.get(item["job_id"])
    if decision is None:
        raise ValueError("batch item does not reference a dispatch "
                         "decision")
    binding = dispatch_bindings.get(dispatch_key)
    expected_request = _expected_dispatch_request(item, plan)
    if binding is None:
        if decision["state"] != "claimed":
            raise ValueError("dispatch decision is not claimed")
        if decision["attempts"] != item["attempt"]:
            raise ValueError("dispatch attempt does not match the plan")
        if decision["owner"] != plan["owner"] \
                or decision["lease_end"] != plan["lease_end"]:
            raise ValueError("dispatch owner or lease does not match the "
                             "plan")
        if item["action"] == "finish":
            if item["at"] > decision["lease_end"]:
                raise ValueError("finish moment is past the dispatch lease")
        elif item["at"] <= decision["lease_end"]:
            raise ValueError("the dispatch lease has not expired yet")
    else:
        if binding != expected_request:
            raise ValueError("dispatch idempotency key was already used "
                             "with a different request")
        if dispatch_events[dispatch_key]["result"]["state"] \
                != _expected_decision_state(item):
            raise ValueError("dispatch replay result does not match the "
                             "plan state")


def _dispatch_key(batch_key: str, job_id: str, attempt: int) -> str:
    # Stable from the batch key and the plan identity: the length prefix
    # makes the join unambiguous, so a crashed call replays the very
    # same dispatch request.
    return f"{len(batch_key)}:{batch_key}{job_id}/{attempt}"


def _scan_plans(
    plans: dict[str, dict[str, dict[str, Any]]],
    events: dict[str, dict[str, Any]],
    synced: set[tuple[str, int]],
) -> list[dict[str, Any]]:
    # First-scan snapshot: job id then attempt, only terminal plans no
    # earlier batch has synced, with the action moment taken from the
    # plan history.
    found: list[dict[str, Any]] = []
    for job_id in sorted(plans):
        attempts = plans[job_id]
        for attempt in sorted(attempts.values(), key=lambda plan: plan["attempt"]):
            if attempt["state"] not in _PLAN_TERMINAL:
                continue
            slot = (job_id, attempt["attempt"])
            if slot in synced:
                continue
            found.append({
                "job_id": job_id,
                "attempt": attempt["attempt"],
                "state": attempt["state"],
                "action": _action_for(attempt["state"]),
                "at": _provenance_at(attempt, events),
                "status": "pending",
                "error": None,
                "decision": None,
            })
    return found


def run(
    execution: str,
    dispatch: str,
    ledger: str,
    owner: str,
    key: str,
    now: int,
    lease: int,
) -> tuple[dict[str, object], bool]:
    """Reconcile one batch of terminal execution plans idempotently.

    ``execution``, ``dispatch`` and ``ledger`` paths must be non-empty
    strings resolving to three distinct real locations; ``owner`` and
    ``key`` must be non-empty strings, ``now`` a non-boolean
    non-negative integer moment and ``lease`` a non-boolean positive
    integer coordination lease, else ``ValueError`` before any business
    file is read.

    The first call for ``key`` scans the execution ledger by job id and
    attempt and fixes the batch as the terminal plans (``completed``,
    ``failed`` or ``interrupted``) no earlier batch has synced.
    Completed and failed plans finish the dispatch decision as
    succeeded and failed, interrupted plans recover the expired claim;
    the action moment is the last step receipt's or the recovery
    audit's original value and never changes on retry. Every item is
    persisted ``pending`` before the existing dispatch interface is
    called with a dispatch idempotency key derived stably from the
    batch key and the plan identity, so crash re-entry replays the same
    request. The item becomes ``applied`` only after a success or an
    equivalent replay, saving the returned decision; a failure records
    the public exception class, keeps the item pending and re-raises
    the original exception and its chain.

    Returns ``(batch, created)`` -- a fixed-order copy of the current
    batch snapshot and whether this call created it. Re-entry with the
    same batch key continues the unfinished items; once completed the
    batch replays without rewriting a byte. Later terminal plans
    require a new batch key; an empty batch still commits an explicit
    completed state and full audit snapshot. An active batch may only
    be continued by its current owner; a different owner before strict
    expiry raises ``PermissionError``, and after expiry may take over
    without repeating applied items.

    Missing input files or the ledger parent raise
    ``FileNotFoundError``; invalid structure, ordering, references or
    non-canonical bytes raise ``ValueError``; other locking or I/O
    failures raise ``OSError``.
    """
    for value in (execution, dispatch, ledger, owner, key):
        if not isinstance(value, str) or not value:
            raise ValueError("execution, dispatch, ledger, owner and key "
                             "must be non-empty strings")
    if not _is_plain_int(now) or now < 0:
        raise ValueError("now must be a non-boolean non-negative integer")
    if not _is_plain_int(lease) or lease < 1:
        raise ValueError("lease must be a non-boolean positive integer")

    execution_real = os.path.realpath(execution)
    dispatch_real = os.path.realpath(dispatch)
    ledger_real = os.path.realpath(ledger)
    if len({execution_real, dispatch_real, ledger_real}) != 3:
        raise ValueError("execution, dispatch and ledger paths must be "
                         "distinct real paths")


    store = _get_store(ledger)
    with store.lock:
        # The lock order is fixed for every process: the coordination
        # ledger's exclusive lock first -- the one lock that serializes
        # whole batches and that only this module ever takes -- and then
        # the two input ledgers' shared locks in resolved-real-path
        # order. Coordination-first is what keeps the order deadlock
        # free: a process waiting for the coordination lock never holds
        # an input lock yet, and the lock holder, once it releases the
        # shared inputs, is the only process that can ask for the
        # dispatch ledger's own exclusive lock through the existing
        # dispatch interface. The shared input locks are released right
        # after the snapshot; the exclusive coordination lock stays
        # held for the whole reconciliation.
        input_paths = sorted(p for p in (execution_real, dispatch_real)
                             if p != ledger_real)
        lock_entries: list[tuple[str, Any, bool]] = [
            (ledger_real, _lock(ledger_real), False)
        ]
        lock_entries.extend(
            (path, _lock(path, shared=True), True) for path in input_paths
        )

        held: list[Any] = []
        try:
            for path, manager, releasable in lock_entries:
                manager.__enter__()
                held.append((path, manager, releasable))

            plans, _plan_keys, plan_events = \
                _execution._load_existing_ledger(execution_real)[:3]
            decisions, dispatch_bindings, dispatch_events, dispatch_raw = \
                _dispatch._load_ledger(dispatch_real)
            if dispatch_raw is None:
                raise FileNotFoundError(
                    f"dispatch ledger {dispatch_real!r} does not exist")
            batches, audit, old_bytes = _load_ledger(
                ledger_real, plans, plan_events, decisions)

            # Release only the two shared input locks, in reverse
            # acquisition order; the coordination lock stays held.
            remaining: list[Any] = []
            for path, manager, releasable in reversed(held):
                if releasable:
                    manager.__exit__(None, None, None)
                else:
                    remaining.append((path, manager, releasable))
            held = list(reversed(remaining))

            synced = {(item["job_id"], item["attempt"])
                      for existing in batches.values()
                      for item in existing["items"].values()}

            def persist() -> None:
                nonlocal old_bytes
                payload = _canonical_bytes(batches, audit)
                _commit_file(ledger_real, payload, old_bytes)
                old_bytes = payload

            batch = batches.get(key)
            created = batch is None
            if created:
                # The first scan fixes the batch: job id then attempt,
                # only terminal plans no earlier batch has synced.
                items = _scan_plans(plans, plan_events, synced)
                batch = {
                    "key": key,
                    "owner": owner,
                    "until": now + lease,
                    "status": ("completed" if not items else "pending"),
                    "items": _items_map(items),
                }
                batches[key] = batch
            else:
                if batch["status"] == "completed":
                    # A terminal batch replays byte-for-byte for every
                    # caller and never renews its lease.
                    return _snapshot(batch), False
                # An active batch is continued only by its current
                # owner; a different owner must wait for the strict
                # lease expiry before taking over. Whoever runs renews
                # the lease to now + lease.
                if batch["owner"] != owner:
                    if now <= batch["until"]:
                        raise PermissionError(
                            "reconciliation batch is owned by another "
                            f"owner until {batch['until']}")
                    batch["owner"] = owner
                batch["until"] = now + lease

            # Every item must still match the ledgers read in this
            # snapshot, and a pending item's dispatch side must be
            # claimable exactly as the plan requires or already bound
            # to the equivalent replay. These checks run before any
            # write, so a mismatch never advances the batch.
            for item in batch["items"].values():
                plan = plans.get(item["job_id"], {}).get(str(item["attempt"]))
                if plan is None or plan["state"] != item["state"] \
                        or _provenance_at(plan, plan_events) != item["at"]:
                    raise ValueError("batch item does not match the "
                                     "execution ledger")
                if item["status"] == "pending":
                    _check_pending_item(
                        item, plan, decisions, dispatch_bindings,
                        dispatch_events,
                        _dispatch_key(key, item["job_id"], item["attempt"]))

            if created and batch["status"] == "completed":
                # An empty batch still commits an explicit completed
                # state and the full audit snapshot.
                audit[key] = {"key": key, "batch": copy.deepcopy(batch)}
            # Every item is persisted pending before a dispatch
            # interface is ever called; a re-entry persists the lease
            # renewal first, and an empty batch directly commits its
            # terminal state.
            persist()
            if created and batch["status"] == "completed":
                return _snapshot(batch), True

            pending = [item for item in _ordered_items(batch)
                       if item["status"] == "pending"]
            for item in pending:
                plan = plans[item["job_id"]][str(item["attempt"])]
                dispatch_key = _dispatch_key(key, item["job_id"],
                                             item["attempt"])
                try:
                    if item["action"] == "finish":
                        # A finish is issued by the owner the decision
                        # was claimed to -- the plan's fixed owner -- at
                        # the historical last-receipt moment.
                        result = ("succeeded" if item["state"] == "completed"
                                  else "failed")
                        decision, was_created = _dispatch.finish(
                            dispatch, item["job_id"], dispatch_key,
                            plan["owner"], result, item["at"])
                    else:
                        decision, was_created = _dispatch.recover(
                            dispatch, item["job_id"], dispatch_key,
                            item["at"])
                except Exception as exc:
                    # Record the public exception class, keep the item
                    # pending and commit that before leaving, then
                    # re-raise the original exception with its chain.
                    item["error"] = type(exc).__name__
                    persist()
                    raise
                if not was_created:
                    # An equivalent replay (e.g. a crash between the
                    # dispatch commit and the applied marker) saves the
                    # immutable decision the original call returned, not
                    # a decision later re-claims may have produced.
                    with _lock(dispatch_real, shared=True):
                        _replayed, _bindings, replay_events = \
                            _dispatch._load_ledger(dispatch_real)[:3]
                    decision = copy.deepcopy(
                        replay_events[dispatch_key]["result"])
                item["status"] = "applied"
                item["error"] = None
                item["decision"] = decision
                persist()

            # The terminal commit is validated against the post-dispatch
            # dispatch snapshot before it replaces the previous bytes.
            batch["status"] = "completed"
            audit[key] = {"key": key, "batch": copy.deepcopy(batch)}
            payload = _canonical_bytes(batches, audit)
            with _lock(dispatch_real, shared=True):
                final_decisions = _dispatch._load_ledger(dispatch_real)[0]
            _validate_ledger(json.loads(payload.decode("utf-8")),
                             plans, plan_events, final_decisions)
            _commit_file(ledger_real, payload, old_bytes)
            return _snapshot(batch), created
        finally:
            for _path, manager, _releasable in reversed(held):
                with contextlib.suppress(BaseException):
                    manager.__exit__(None, None, None)
