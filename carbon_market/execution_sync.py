"""Persistent execution synchronization over the dispatch ledger.

One on-disk document (version 1), the synchronization ledger, holds the
execution-coordination layer on top of the dispatch and execution
ledgers:

* ``batches`` maps each batch key to one reconciliation batch, binding
  the current owner and its lease end, a lifecycle state (``active`` or
  ``completed``) and the sorted plan items the first scan collected;
* ``audit`` is an append-only list of events, one per committed state,
  binding the call request (owner, moment and lease) and the complete
  batch snapshot that commit durable.

:func:`run` reads the dispatch ledger and the execution ledger as one
snapshot under their shared locks together with the synchronization
ledger's exclusive lock. The first run of a batch key scans the
execution snapshot by job id and attempt number and collects only the
terminal plans (``completed``, ``failed`` or ``interrupted``) no earlier
batch covers; the scan result is fixed for the batch's whole lifetime.
Each item is first persisted as ``pending`` and only then reconciled
through the existing dispatch interface: a ``completed`` or ``failed``
plan finishes its dispatch decision as ``succeeded`` or ``failed``, an
``interrupted`` plan recovers the expired dispatch lease. The action
moment is the original value of the plan's last step receipt or of the
interruption audit request, so a retry never rewrites that history, and
the dispatch idempotency key is stably derived from the batch key and
the plan identity, so a crashed run replays exactly the same request.
Only a successful or equivalently replayed dispatch call flips the item
to ``applied`` and saves the committed decision; a failed call records
the public exception class, keeps the item ``pending`` and re-raises the
exception with its chain untouched. A dispatch decision whose state,
attempt, owner or lease no longer matches the plan raises ``ValueError``
without advancing the item.

An active batch may only be continued by its current owner; a different
owner calling before the lease has strictly expired raises
``PermissionError``, while an expired lease lets another owner take the
batch over without re-applying finished items. A completed batch --
including an empty one, which still commits its definite completion
state and complete audit snapshot -- replays without changing a byte,
and later terminal plans are picked up by a new batch key.

Every read requires the on-disk bytes to be exactly the canonical
compact form :func:`_canonical_bytes` produces -- sections in their
fixed order, batch keys sorted by code point, items sorted by job id and
attempt, compact UTF-8 JSON with non-ASCII written through, no
negative-zero or non-finite number literals and exactly one trailing
newline.
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
_BATCH_FIELDS = ("key", "owner", "lease_end", "state", "items")
_ITEM_FIELDS = ("job_id", "attempt", "action", "result", "owner",
                "lease_end", "at", "key", "state", "decision", "error")
_REQUEST_FIELDS = ("owner", "at", "lease")
_EVENT_FIELDS = ("key", "request", "result")
_BATCH_STATES = ("active", "completed")
_ITEM_STATES = ("pending", "applied")
_ACTIONS = ("finish", "recover")
_RESULTS = ("succeeded", "failed")
_TERMINAL_STATES = ("completed", "failed", "interrupted")
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
    # As in the other registries, the companion lock file is never
    # unlinked and an flock is released by the kernel on process exit,
    # so equivalent real paths share one lock across threads and
    # processes -- and across modules, since every layer uses the same
    # suffix for the same file.
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


def _dispatch_key(batch_key: str, job_id: str, attempt: int) -> str:
    # The dispatch idempotency key is stably derived from the batch key
    # and the plan identity, so a crashed run replays the same request.
    return json.dumps([batch_key, job_id, attempt], ensure_ascii=False,
                      separators=(",", ":"))


def _validate_request(request: object) -> dict[str, Any]:
    if not isinstance(request, dict) \
            or set(request.keys()) != set(_REQUEST_FIELDS):
        raise ValueError("synchronization audit request has invalid "
                         "fields")
    owner = request["owner"]
    if not isinstance(owner, str) or not owner:
        raise ValueError("request owner must be a non-empty string")
    at = request["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("request at must be a non-boolean non-negative "
                         "integer")
    lease = request["lease"]
    if not _is_plain_int(lease) or lease <= 0:
        raise ValueError("request lease must be a non-boolean positive "
                         "integer")
    return {"owner": owner, "at": at, "lease": lease}


def _validate_item(record: object, batch_key: str) -> dict[str, Any]:
    if not isinstance(record, dict) \
            or set(record.keys()) != set(_ITEM_FIELDS):
        raise ValueError("batch item has invalid fields")
    job_id = record["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("item job_id must be a non-empty string")
    attempt = record["attempt"]
    if not _is_plain_int(attempt) or attempt < 1:
        raise ValueError("item attempt must be a positive integer")
    action = record["action"]
    if action not in _ACTIONS:
        raise ValueError("item action is invalid")
    result = record["result"]
    if action == "finish":
        if result not in _RESULTS:
            raise ValueError("a finish item must carry a valid result")
    elif result is not None:
        raise ValueError("a recover item must not carry a result")
    owner = record["owner"]
    if not isinstance(owner, str) or not owner:
        raise ValueError("item owner must be a non-empty string")
    lease_end = record["lease_end"]
    if not _is_plain_int(lease_end) or lease_end < 1:
        raise ValueError("item lease_end must be a positive integer")
    at = record["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("item at must be a non-boolean non-negative "
                         "integer")
    key = record["key"]
    if not isinstance(key, str) or not key:
        raise ValueError("item key must be a non-empty string")
    if key != _dispatch_key(batch_key, job_id, attempt):
        raise ValueError("item key does not match the batch and plan "
                         "identity")
    state = record["state"]
    if state not in _ITEM_STATES:
        raise ValueError("item state is invalid")
    decision_raw = record["decision"]
    decision = None
    if decision_raw is not None:
        decision = _dispatch._validate_decision(decision_raw)
    error = record["error"]
    if error is not None and (not isinstance(error, str) or not error):
        raise ValueError("item error must be null or a non-empty string")
    if state == "applied":
        if decision is None:
            raise ValueError("an applied item must carry its decision")
        if error is not None:
            raise ValueError("an applied item must not carry an error")
    elif decision is not None:
        raise ValueError("a pending item must not carry a decision")
    if decision is not None:
        if decision["job_id"] != job_id:
            raise ValueError("item decision does not match its job")
        if decision["attempts"] != attempt:
            raise ValueError("item decision does not match its attempt")
        if action == "finish":
            if decision["state"] != result:
                raise ValueError("item decision does not match its "
                                 "result")
        elif decision["state"] != "ready":
            raise ValueError("a recover item decision must be ready")
    return {
        "job_id": job_id,
        "attempt": attempt,
        "action": action,
        "result": result,
        "owner": owner,
        "lease_end": lease_end,
        "at": at,
        "key": key,
        "state": state,
        "decision": decision,
        "error": error,
    }


def _validate_batch(record: object) -> dict[str, Any]:
    if not isinstance(record, dict) \
            or set(record.keys()) != set(_BATCH_FIELDS):
        raise ValueError("batch record has invalid fields")
    key = record["key"]
    if not isinstance(key, str) or not key:
        raise ValueError("batch key must be a non-empty string")
    owner = record["owner"]
    if not isinstance(owner, str) or not owner:
        raise ValueError("batch owner must be a non-empty string")
    lease_end = record["lease_end"]
    if not _is_plain_int(lease_end) or lease_end < 1:
        raise ValueError("batch lease_end must be a positive integer")
    state = record["state"]
    if state not in _BATCH_STATES:
        raise ValueError("batch state is invalid")
    items_raw = record["items"]
    if not isinstance(items_raw, list):
        raise ValueError("batch items must be a list")
    items = [_validate_item(item_raw, key) for item_raw in items_raw]
    identities = [(item["job_id"], item["attempt"]) for item in items]
    if identities != sorted(identities) \
            or len(set(identities)) != len(identities):
        raise ValueError("batch items must be ordered by job id and "
                         "attempt")
    # A batch is completed exactly when every item is applied; an empty
    # batch is therefore always completed.
    if (state == "completed") \
            != all(item["state"] == "applied" for item in items):
        raise ValueError("batch state does not match its items")
    return {"key": key, "owner": owner, "lease_end": lease_end,
            "state": state, "items": items}


def _validate_ledger(
    data: object,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("synchronization ledger root must be an object "
                         "with keys version, batches and audit")
    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported synchronization ledger version")

    batches_raw = data["batches"]
    audit_raw = data["audit"]
    if not isinstance(batches_raw, dict) or not isinstance(audit_raw, list):
        raise ValueError("batches must be an object and audit a list")
    _check_sorted_keys(batches_raw, "batches")

    batches: dict[str, dict[str, Any]] = {}
    for key, batch_raw in batches_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("batch keys must be non-empty strings")
        batch = _validate_batch(batch_raw)
        if batch["key"] != key:
            raise ValueError("batch record key does not match its map key")
        batches[key] = batch

    events: list[dict[str, Any]] = []
    for event_raw in audit_raw:
        if not isinstance(event_raw, dict) \
                or set(event_raw.keys()) != set(_EVENT_FIELDS):
            raise ValueError("synchronization audit event has invalid "
                             "fields")
        key = event_raw["key"]
        if not isinstance(key, str) or not key:
            raise ValueError("audit event key must be a non-empty string")
        request = _validate_request(event_raw["request"])
        result = _validate_batch(event_raw["result"])
        if result["key"] != key:
            raise ValueError("audit event result does not match its key")
        events.append({"key": key, "request": request, "result": result})

    # Every event references a recorded batch, and each batch's last
    # event carries exactly its current record while its first carries
    # the all-pending creation snapshot.
    first: dict[str, dict[str, Any]] = {}
    last: dict[str, dict[str, Any]] = {}
    for event in events:
        if event["key"] not in batches:
            raise ValueError("audit event references an unknown batch")
        first.setdefault(event["key"], event["result"])
        last[event["key"]] = event["result"]
    for key, batch in batches.items():
        if key not in last:
            raise ValueError("every batch must be bound to an audit "
                             "event")
        if last[key] != batch:
            raise ValueError("batch does not match its last audit "
                             "snapshot")
        if any(item["state"] != "pending" for item in first[key]["items"]):
            raise ValueError("the first audit snapshot of a batch must "
                             "be all pending")

    # One plan identity is collected by at most one batch.
    seen: set[tuple[str, int]] = set()
    for batch in batches.values():
        for item in batch["items"]:
            identity = (item["job_id"], item["attempt"])
            if identity in seen:
                raise ValueError("plan identity collected by more than "
                                 "one batch")
            seen.add(identity)

    return batches, events


def _canonical_bytes(batches: dict[str, dict[str, Any]],
                     events: list[dict[str, Any]]) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, sections in their
    # fixed field order, batch keys in code-point order and the audit in
    # append order, terminated by exactly one newline.
    payload = {
        "version": _VERSION,
        "batches": {key: batches[key] for key in sorted(batches)},
        "audit": events,
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_ledger(
    realpath: str,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return {}, [], None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"synchronization ledger {realpath!r} is not valid UTF-8") \
            from exc
    try:
        # Negative-zero and non-finite literals are format errors.
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"synchronization ledger {realpath!r} is not valid JSON") \
            from exc
    batches, events = _validate_ledger(data)
    # As for the other registries, the ledger is accepted only in
    # canonical compact form with a single trailing newline.
    if raw != _canonical_bytes(batches, events):
        raise ValueError(
            f"synchronization ledger {realpath!r} is not in canonical "
            "compact form")
    return batches, events, raw


def _fsync_directory(directory: str) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _rollback_file(realpath: str, directory: str, old_bytes: bytes | None,
                   first: BaseException) -> None:
    # Restore the exact pre-commit bytes while the exclusive lock is
    # held, or remove a ledger that did not exist beforehand, then sync
    # the directory. A failed recovery chains after the original error.
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
    # One durable commit for the batch state and the appended audit
    # event: synced same-directory temporary, atomic replace and a
    # directory fsync, restoring the previous bytes on any failure.
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


def _scan_item(batch_key: str, plan: dict[str, Any],
               events: dict[str, dict[str, Any]]) -> dict[str, Any]:
    # One batch item for one terminal plan: the action, the original
    # action moment and the stable dispatch idempotency key are fixed
    # here and never rewritten by later retries.
    job_id = plan["job_id"]
    attempt = plan["attempt"]
    state = plan["state"]
    if state in ("completed", "failed"):
        action = "finish"
        result = "succeeded" if state == "completed" else "failed"
        moment = plan["steps"][-1]["at"]
    else:  # interrupted
        action = "recover"
        result = None
        moment = None
        for event in events.values():
            request = event["request"]
            if request["action"] == "recover" \
                    and request["job_id"] == job_id \
                    and request["attempt"] == attempt:
                moment = request["at"]
                break
        if moment is None:
            raise ValueError("interrupted plan has no recover audit "
                             "event")
    return {
        "job_id": job_id,
        "attempt": attempt,
        "action": action,
        "result": result,
        "owner": plan["owner"],
        "lease_end": plan["lease_end"],
        "at": moment,
        "key": _dispatch_key(batch_key, job_id, attempt),
        "state": "pending",
        "decision": None,
        "error": None,
    }


def run(
    dispatch: str,
    execution: str,
    ledger: str,
    owner: str,
    key: str,
    at: int,
    lease: int,
) -> tuple[dict[str, object], bool]:
    """Reconcile the terminal execution plans of one batch idempotently.

    ``dispatch``, ``execution`` and ``ledger`` paths, ``owner`` and
    ``key`` must be non-empty strings, ``at`` a non-boolean non-negative
    integer current moment and ``lease`` a non-boolean positive integer;
    the three paths must also resolve to distinct real locations. Any
    violation raises ``ValueError`` before a business file is read.

    The dispatch ledger and the execution ledger are read as one
    snapshot under their shared locks together with the synchronization
    ledger's exclusive lock. The first run of a batch key scans the
    execution snapshot by job id and attempt number and collects only
    the terminal plans no earlier batch covers, fixing the batch's items
    for its whole lifetime; an empty scan still commits the batch's
    definite completion state and complete audit snapshot. Each item is
    persisted as ``pending`` first and only then reconciled through the
    existing dispatch interface: ``completed`` and ``failed`` plans
    finish their decision as ``succeeded`` and ``failed``, an
    ``interrupted`` plan recovers the expired lease. The action moment
    is the original value of the plan's last step receipt or of the
    interruption audit request, and the dispatch idempotency key is
    stably derived from the batch key and the plan identity, so a
    crashed run replays exactly the same request. Only a successful or
    equivalently replayed dispatch call flips the item to ``applied``
    and saves the committed decision; a failed call records the public
    exception class, keeps the item ``pending`` and re-raises the
    exception with its chain untouched. A dispatch decision whose state,
    attempt, owner or lease no longer matches the plan raises
    ``ValueError`` without advancing the item.

    Returns ``(batch, created)`` with a fixed-order copy of the current
    batch. An active batch may only be continued by its current owner: a
    different owner calling before the lease has strictly expired raises
    ``PermissionError``, while an expired lease lets the caller take the
    batch over without re-applying finished items. A completed batch
    replays with ``False`` without changing a byte; later terminal plans
    are picked up by a new batch key.

    Missing input files or a missing ledger parent raise
    ``FileNotFoundError``; invalid structure, values, ordering,
    references or canonical bytes raise ``ValueError``; other locking,
    read/write or sync failures raise ``OSError``.
    """
    for value in (dispatch, execution, ledger, owner, key):
        if not isinstance(value, str) or not value:
            raise ValueError("dispatch, execution, ledger, owner and key "
                             "must be non-empty strings")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")
    if not _is_plain_int(lease) or lease <= 0:
        raise ValueError("lease must be a non-boolean positive integer")

    dispatch_real = os.path.realpath(dispatch)
    execution_real = os.path.realpath(execution)
    ledger_real = os.path.realpath(ledger)
    if len({dispatch_real, execution_real, ledger_real}) != 3:
        raise ValueError("dispatch, execution and ledger paths must be "
                         "distinct real paths")

    store = _get_store(ledger)
    with store.lock:
        # The synchronization ledger's exclusive lock covers the whole
        # run; the input snapshots are read under shared locks that are
        # released before any dispatch call, so no lock ordering cycle
        # can form with the other layers.
        with _lock(ledger_real):
            with _lock(dispatch_real, shared=True):
                _, _, _, dispatch_raw = _dispatch._load_ledger(
                    dispatch_real)
                if dispatch_raw is None:
                    raise FileNotFoundError(
                        f"dispatch ledger {dispatch_real!r} does not "
                        "exist")
            with _lock(execution_real, shared=True):
                plans, _plan_keys, plan_events, execution_raw = \
                    _execution._load_ledger(execution_real)
                if execution_raw is None:
                    raise FileNotFoundError(
                        f"execution ledger {execution_real!r} does not "
                        "exist")
            batches, events, old_bytes = _load_ledger(ledger_real)

            old_bytes_holder = [old_bytes]

            def commit() -> None:
                # Each commit's rollback restores the last durable
                # bytes, then becomes the new durable state.
                payload = _canonical_bytes(batches, events)
                _commit_file(ledger_real, payload, old_bytes_holder[0])
                old_bytes_holder[0] = payload

            request = {"owner": owner, "at": at, "lease": lease}

            created = False
            batch = batches.get(key)
            if batch is None:
                # First run of this batch key: scan the execution
                # snapshot by job id and attempt number and collect only
                # the terminal plans no earlier batch covers.
                synced = {(item["job_id"], item["attempt"])
                          for existing in batches.values()
                          for item in existing["items"]}
                items = []
                for job_id in sorted(plans):
                    attempts = plans[job_id]
                    for attempt_key in sorted(attempts, key=int):
                        plan = attempts[attempt_key]
                        if plan["state"] not in _TERMINAL_STATES:
                            continue
                        if (job_id, plan["attempt"]) in synced:
                            continue
                        items.append(_scan_item(key, plan, plan_events))
                batch = {"key": key, "owner": owner,
                         "lease_end": at + lease,
                         "state": "active" if items else "completed",
                         "items": items}
                batches[key] = batch
                events.append({"key": key, "request": request,
                               "result": copy.deepcopy(batch)})
                commit()
                created = True
            else:
                if batch["state"] == "completed":
                    # A completed batch replays without changing a byte.
                    return copy.deepcopy(batch), False
                if batch["owner"] != owner:
                    if at <= batch["lease_end"]:
                        raise PermissionError(
                            "batch is held by another owner under a "
                            "valid lease")
                    # The lease has strictly expired: take the batch
                    # over and continue with the remaining items.
                    batch["owner"] = owner
                    batch["lease_end"] = at + lease
                    events.append({"key": key, "request": request,
                                   "result": copy.deepcopy(batch)})
                    commit()

            for item in batch["items"]:
                if item["state"] == "applied":
                    continue
                with _lock(dispatch_real, shared=True):
                    decisions, dispatch_keys, _, dispatch_raw = \
                        _dispatch._load_ledger(dispatch_real)
                    if dispatch_raw is None:
                        raise FileNotFoundError(
                            f"dispatch ledger {dispatch_real!r} does "
                            "not exist")
                if item["key"] not in dispatch_keys:
                    # The dispatch request was never committed: the
                    # decision must still match the plan exactly.
                    decision = decisions.get(item["job_id"])
                    if decision is None \
                            or decision["state"] != "claimed" \
                            or decision["attempts"] != item["attempt"] \
                            or decision["owner"] != item["owner"] \
                            or decision["lease_end"] != item["lease_end"]:
                        raise ValueError(
                            "dispatch decision does not match the plan")
                try:
                    if item["action"] == "finish":
                        decision, served = _dispatch.finish(
                            dispatch, item["job_id"], item["key"],
                            item["owner"], item["result"], item["at"])
                    else:
                        decision, served = _dispatch.recover(
                            dispatch, item["job_id"], item["key"],
                            item["at"])
                except Exception as exc:
                    # Record the public exception class, keep the item
                    # pending and re-raise the exception untouched.
                    item["error"] = type(exc).__name__
                    events.append({"key": key, "request": request,
                                   "result": copy.deepcopy(batch)})
                    commit()
                    raise
                if served:
                    committed_decision: dict[str, Any] = decision
                else:
                    # An equivalent replay returns the current decision;
                    # the item instead saves the snapshot this request
                    # committed, so a retry never rewrites history.
                    with _lock(dispatch_real, shared=True):
                        _, _, dispatch_events, _ = \
                            _dispatch._load_ledger(dispatch_real)
                    committed_decision = copy.deepcopy(
                        dispatch_events[item["key"]]["result"])
                item["state"] = "applied"
                item["decision"] = committed_decision
                item["error"] = None
                if all(entry["state"] == "applied"
                       for entry in batch["items"]):
                    batch["state"] = "completed"
                events.append({"key": key, "request": request,
                               "result": copy.deepcopy(batch)})
                commit()
            return copy.deepcopy(batch), created
