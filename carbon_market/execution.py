"""Persistent execution plans over ``dispatch`` claims.

One on-disk document (version 1), the execution ledger, holds the whole
execution layer on top of the dispatch ledger:

* ``plans`` maps each job id to its execution plans keyed by the
  dispatch attempt number, each plan binding the traded resource
  version, the job deadline, the claiming owner and the lease end, with
  a kind (``launch`` or ``migrate``), an optional source resource, a
  lifecycle state (``active``, ``completed``, ``failed`` or
  ``interrupted``) and the step receipts recorded so far;
* ``idempotency`` binds each call key to the complete request it first
  served (action, job id and moment, plus owner, source, attempt, step,
  result or receipt where the action carries them);
* ``audit`` holds one event per first-served key, binding the complete
  request and the plan snapshot the call committed.

:func:`plan` reads the ``jobs.submit`` acceptance file, the resource
supply file, the ``market.clear`` clearing ledger and the dispatch
ledger as one snapshot under their shared locks together with the
execution ledger's exclusive lock, all five taken in resolved real-path
order, and creates the plan for one claimed decision: numbered by the
dispatch attempt count, it fixes the job, the target resource version,
the deadline, the owner and the lease end. An empty source yields a
``launch`` plan (steps ``stage`` then ``start``); a non-empty source
yields a ``migrate`` plan (steps ``copy`` then ``switch``) and must
name a published resource other than the target whose residency covers
the job's. :func:`record` and :func:`recover` only touch the execution
ledger under its exclusive lock: recording appends one step receipt --
steps advance only on success, any failure terminates the plan as
``failed`` and a successful last step as ``completed`` -- and recovering
turns a plan whose lease has strictly expired into ``interrupted`` with
every receipt preserved. The execution ledger is created only by the
first plan; every other call facing a missing ledger fails with
``FileNotFoundError``.

Every read requires the on-disk bytes to be exactly the canonical
compact form :func:`_canonical_bytes` produces -- sections in their
fixed order, primary keys sorted by code point, compact UTF-8 JSON with
non-ASCII written through, no negative-zero or non-finite number
literals and exactly one trailing newline.
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
from . import jobs as _jobs
from . import market as _market
from . import resources as _resources
from ._jsonio import finite_loads

__all__ = ["plan", "record", "recover"]

_VERSION = 1
_ROOT_FIELDS = ("version", "plans", "idempotency", "audit")
_PLAN_FIELDS = ("job_id", "attempt", "kind", "source", "resource_id",
                "version", "deadline", "owner", "lease_end", "state",
                "steps")
_RECEIPT_FIELDS = ("step", "result", "receipt", "at")
_KINDS = ("launch", "migrate")
_STATES = ("active", "completed", "failed", "interrupted")
_RESULTS = ("succeeded", "failed")
_STEPS = {"launch": ("stage", "start"), "migrate": ("copy", "switch")}
_ALL_STEPS = ("stage", "start", "copy", "switch")
_EVENT_FIELDS = ("key", "request", "result")
_REQUEST_FIELDS = {
    "plan": ("action", "job_id", "owner", "source", "at"),
    "record": ("action", "job_id", "attempt", "owner", "step", "result",
               "receipt", "at"),
    "recover": ("action", "job_id", "attempt", "at"),
}
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


def _validate_receipt(receipt: object, sequence: tuple[str, ...],
                      index: int, lease_end: int) -> dict[str, Any]:
    if not isinstance(receipt, dict) \
            or set(receipt.keys()) != set(_RECEIPT_FIELDS):
        raise ValueError("step receipt has invalid fields")
    step = receipt["step"]
    if step != sequence[index]:
        raise ValueError("step receipts must follow the plan's step "
                         "sequence")
    result = receipt["result"]
    if result not in _RESULTS:
        raise ValueError("step receipt result is invalid")
    token = receipt["receipt"]
    if not isinstance(token, str) or not token:
        raise ValueError("step receipt must carry a non-empty receipt "
                         "string")
    at = receipt["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("step receipt at must be a non-boolean "
                         "non-negative integer")
    if at > lease_end:
        raise ValueError("step receipt must be recorded inside the lease")
    return {"step": step, "result": result, "receipt": token, "at": at}


def _validate_plan(record: object) -> dict[str, Any]:
    if not isinstance(record, dict) \
            or set(record.keys()) != set(_PLAN_FIELDS):
        raise ValueError("plan record has invalid fields")
    job_id = record["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("plan job_id must be a non-empty string")
    attempt = record["attempt"]
    if not _is_plain_int(attempt) or attempt < 1:
        raise ValueError("plan attempt must be a positive integer")
    kind = record["kind"]
    if kind not in _KINDS:
        raise ValueError("plan kind is invalid")
    source = record["source"]
    if kind == "launch":
        if source is not None:
            raise ValueError("a launch plan must not carry a source")
    elif not isinstance(source, str) or not source:
        raise ValueError("a migrate plan must carry a non-empty source")
    resource_id = record["resource_id"]
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("plan resource_id must be a non-empty string")
    if kind == "migrate" and source == resource_id:
        raise ValueError("plan source must differ from its target")
    version = record["version"]
    if not _is_plain_int(version) or version < 1:
        raise ValueError("plan version must be a positive integer")
    deadline = record["deadline"]
    if not _is_plain_int(deadline) or deadline < 0:
        raise ValueError("plan deadline must be a non-boolean "
                         "non-negative integer")
    owner = record["owner"]
    if not isinstance(owner, str) or not owner:
        raise ValueError("plan owner must be a non-empty string")
    lease_end = record["lease_end"]
    if not _is_plain_int(lease_end) or lease_end < 1:
        raise ValueError("plan lease_end must be a positive integer")
    if lease_end > deadline:
        raise ValueError("plan lease end must not pass the job deadline")
    state = record["state"]
    if state not in _STATES:
        raise ValueError("plan state is invalid")

    sequence = _STEPS[kind]
    steps_raw = record["steps"]
    if not isinstance(steps_raw, list) or len(steps_raw) > len(sequence):
        raise ValueError("plan steps must be a list within the step "
                         "sequence")
    steps: list[dict[str, Any]] = []
    for index, receipt_raw in enumerate(steps_raw):
        steps.append(_validate_receipt(receipt_raw, sequence, index,
                                       lease_end))

    # The state must be exactly what the recorded receipts imply: a
    # failed receipt terminates the plan and must be the last one, a
    # complete successful sequence terminates it as completed, and
    # anything else is active or recovered-interrupted.
    failed = [receipt for receipt in steps if receipt["result"] == "failed"]
    if failed:
        if steps[-1]["result"] != "failed" or len(failed) != 1:
            raise ValueError("a failed step must terminate the plan")
        if state != "failed":
            raise ValueError("a plan with a failed step must be failed")
    elif len(steps) == len(sequence):
        if state != "completed":
            raise ValueError("a fully recorded plan must be completed")
    elif state not in ("active", "interrupted"):
        raise ValueError("an unfinished plan must be active or "
                         "interrupted")

    return {
        "job_id": job_id,
        "attempt": attempt,
        "kind": kind,
        "source": source,
        "resource_id": resource_id,
        "version": version,
        "deadline": deadline,
        "owner": owner,
        "lease_end": lease_end,
        "state": state,
        "steps": steps,
    }


def _validate_request(request: object) -> dict[str, Any]:
    if not isinstance(request, dict):
        raise ValueError("request must be an object")
    action = request.get("action")
    if not isinstance(action, str) or action not in _REQUEST_FIELDS:
        raise ValueError("request action is invalid")
    if set(request.keys()) != set(_REQUEST_FIELDS[action]):
        raise ValueError("request has invalid fields")
    job_id = request["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("request job_id must be a non-empty string")
    at = request["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("request at must be a non-boolean non-negative "
                         "integer")
    normalized: dict[str, Any] = {"action": action, "job_id": job_id}
    if action in ("record", "recover"):
        attempt = request["attempt"]
        if not _is_plain_int(attempt) or attempt < 1:
            raise ValueError("request attempt must be a positive integer")
        normalized["attempt"] = attempt
    if action in ("plan", "record"):
        owner = request["owner"]
        if not isinstance(owner, str) or not owner:
            raise ValueError("request owner must be a non-empty string")
        normalized["owner"] = owner
    if action == "plan":
        source = request["source"]
        if source is not None \
                and (not isinstance(source, str) or not source):
            raise ValueError("request source must be null or a non-empty "
                             "string")
        normalized["source"] = source
    if action == "record":
        step = request["step"]
        if step not in _ALL_STEPS:
            raise ValueError("request step is invalid")
        normalized["step"] = step
        result = request["result"]
        if result not in _RESULTS:
            raise ValueError("request result is invalid")
        normalized["result"] = result
        receipt = request["receipt"]
        if not isinstance(receipt, str) or not receipt:
            raise ValueError("request receipt must be a non-empty string")
        normalized["receipt"] = receipt
    normalized["at"] = at
    return {field: normalized[field] for field in _REQUEST_FIELDS[action]}


def _validate_ledger(
    data: object,
) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("execution ledger root must be an object with "
                         "keys version, plans, idempotency and audit")
    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported execution ledger version")

    plans_raw = data["plans"]
    idempotency_raw = data["idempotency"]
    audit_raw = data["audit"]
    if not isinstance(plans_raw, dict) \
            or not isinstance(idempotency_raw, dict) \
            or not isinstance(audit_raw, dict):
        raise ValueError("plans, idempotency and audit must be objects")
    _check_sorted_keys(plans_raw, "plans")
    _check_sorted_keys(idempotency_raw, "idempotency")
    _check_sorted_keys(audit_raw, "audit")

    plans: dict[str, dict[str, dict[str, Any]]] = {}
    for job_id, attempts_raw in plans_raw.items():
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("plan job ids must be non-empty strings")
        if not isinstance(attempts_raw, dict) or not attempts_raw:
            raise ValueError("plans must hold a non-empty attempt map "
                             "per job")
        _check_sorted_keys(attempts_raw, "plans")
        attempts: dict[str, dict[str, Any]] = {}
        for attempt_key, record in attempts_raw.items():
            plan = _validate_plan(record)
            if plan["job_id"] != job_id:
                raise ValueError("plan record id does not match its key")
            if not isinstance(attempt_key, str) \
                    or str(plan["attempt"]) != attempt_key:
                raise ValueError("plan record attempt does not match its "
                                 "key")
            attempts[attempt_key] = plan
        plans[job_id] = attempts

    idempotency: dict[str, dict[str, Any]] = {}
    for key, request_raw in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        idempotency[key] = _validate_request(request_raw)

    events: dict[str, dict[str, Any]] = {}
    for key, event_raw in audit_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event_raw, dict) \
                or set(event_raw.keys()) != set(_EVENT_FIELDS):
            raise ValueError("execution audit event has invalid fields")
        if event_raw["key"] != key or not isinstance(event_raw["key"], str):
            raise ValueError("audit event key does not match its map key")
        request = _validate_request(event_raw["request"])
        result = _validate_plan(event_raw["result"])
        if request["job_id"] != result["job_id"]:
            raise ValueError("audit event result does not match its "
                             "request")
        events[key] = {"key": key, "request": request, "result": result}

    # The three sections describe one execution history: each
    # idempotency key binds one request and one event, and the event
    # carries exactly the bound request.
    if set(idempotency) != set(events):
        raise ValueError("idempotency keys and audit events do not match")
    for key, request in idempotency.items():
        if events[key]["request"] != request:
            raise ValueError("audit event does not match its idempotency "
                             "entry")

    # A plan's job id, attempt, kind, source, target resource version,
    # deadline, owner and lease end never change after the plan is
    # created; every recorded result snapshot must agree with them.
    fixed = ("job_id", "attempt", "kind", "source", "resource_id",
             "version", "deadline", "owner", "lease_end")
    creations: dict[tuple[str, int], str] = {}
    recordings: dict[tuple[str, int], list[dict[str, Any]]] = {}
    recoveries: dict[tuple[str, int], int] = {}
    for key, request in idempotency.items():
        result = events[key]["result"]
        slot = (result["job_id"], result["attempt"])
        attempts = plans.get(result["job_id"])
        current = attempts.get(str(result["attempt"])) \
            if attempts is not None else None
        if current is None:
            raise ValueError("audit event must reference a recorded plan")
        for field in fixed:
            if result[field] != current[field]:
                raise ValueError("audit event result does not match its "
                                 "plan")
        action = request["action"]
        if action == "plan":
            if slot in creations:
                raise ValueError("attempt planned under more than one "
                                 "idempotency key")
            creations[slot] = key
            if result["state"] != "active" or result["steps"] != []:
                raise ValueError("plan audit result must be a fresh "
                                 "active plan")
            if request["owner"] != result["owner"] \
                    or request["source"] != result["source"]:
                raise ValueError("plan audit result does not match its "
                                 "request")
            if request["at"] > result["lease_end"]:
                raise ValueError("plan request must lie inside the lease")
        elif action == "record":
            if request["attempt"] != result["attempt"]:
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
            recordings.setdefault(slot, []).append(receipt)
        else:  # recover
            if request["attempt"] != result["attempt"]:
                raise ValueError("recover audit result does not match "
                                 "its request")
            if result["state"] != "interrupted":
                raise ValueError("recover audit result must be an "
                                 "interrupted plan")
            if request["at"] <= result["lease_end"]:
                raise ValueError("recover request must lie past the "
                                 "lease end")
            recoveries[slot] = recoveries.get(slot, 0) + 1

    if set(creations) != {(job_id, plan["attempt"])
                          for job_id, attempts in plans.items()
                          for plan in attempts.values()}:
        raise ValueError("every plan must be bound to a plan request")

    # The recorded steps of a plan are exactly the receipts its record
    # requests committed, and an interrupted plan is exactly one whose
    # recovery was recorded.
    for job_id, attempts in plans.items():
        for plan in attempts.values():
            slot = (job_id, plan["attempt"])
            committed = sorted(json.dumps(receipt, sort_keys=True)
                               for receipt in recordings.get(slot, []))
            held = sorted(json.dumps(receipt, sort_keys=True)
                          for receipt in plan["steps"])
            if committed != held:
                raise ValueError("plan steps do not match the record "
                                 "history")
            if (plan["state"] == "interrupted") \
                    != (recoveries.get(slot, 0) == 1):
                raise ValueError("plan state does not match the recover "
                                 "history")
            if recoveries.get(slot, 0) > 1:
                raise ValueError("plan recovered more than once")

    return plans, idempotency, events


def _canonical_bytes(
    plans: dict[str, dict[str, dict[str, Any]]],
    idempotency: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, sections in their
    # fixed field order and each section's primary keys in code-point
    # order, terminated by exactly one newline.
    payload = {
        "version": _VERSION,
        "plans": {job_id: {attempt: plans[job_id][attempt]
                           for attempt in sorted(plans[job_id])}
                  for job_id in sorted(plans)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
        "audit": {key: events[key] for key in sorted(events)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_ledger(
    realpath: str,
) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, dict[str, Any]],
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
            f"execution ledger {realpath!r} is not valid UTF-8") from exc
    try:
        # Negative-zero and non-finite literals are format errors.
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"execution ledger {realpath!r} is not valid JSON") from exc
    plans, idempotency, events = _validate_ledger(data)
    # As for the other registries, the ledger is accepted only in
    # canonical compact form with a single trailing newline.
    if raw != _canonical_bytes(plans, idempotency, events):
        raise ValueError(
            f"execution ledger {realpath!r} is not in canonical compact "
            "form")
    return plans, idempotency, events, raw


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
                dir=directory, prefix=".execution-restore-", suffix=".tmp")
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
    # One durable commit for the plan, the idempotency binding and the
    # audit event: synced same-directory temporary, atomic replace and a
    # directory fsync, restoring the pre-call bytes on any failure.
    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".execution-", suffix=".tmp")
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


def _request(action: str, job_id: str, at: int, owner: str | None = None,
             source: str | None = None, attempt: int | None = None,
             step: str | None = None, result: str | None = None,
             receipt: str | None = None) -> dict[str, Any]:
    request: dict[str, Any] = {"action": action, "job_id": job_id}
    if action in ("record", "recover"):
        request["attempt"] = attempt
    if action in ("plan", "record"):
        request["owner"] = owner
    if action == "plan":
        request["source"] = source
    if action == "record":
        request["step"] = step
        request["result"] = result
        request["receipt"] = receipt
    request["at"] = at
    return request


def _load_existing_ledger(
    realpath: str,
) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]], bytes]:
    plans, idempotency, events, raw = _load_ledger(realpath)
    if raw is None:
        # Only the first plan creates the execution ledger; every other
        # action fails directly when it is missing.
        raise FileNotFoundError(
            f"execution ledger {realpath!r} does not exist")
    return plans, idempotency, events, raw


def _replay(
    plans: dict[str, dict[str, dict[str, Any]]],
    idempotency: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
    key: str,
    request: dict[str, Any],
) -> tuple[dict[str, Any], bool] | None:
    binding = idempotency.get(key)
    if binding is None:
        return None
    if binding != request:
        raise ValueError("idempotency key was already used with a "
                         "different request")
    # An equivalent replay returns the current record without writing;
    # the committed audit snapshot names the plan the key first served.
    result = events[key]["result"]
    current = plans[result["job_id"]][str(result["attempt"])]
    return copy.deepcopy(current), False


def _lookup_plan(
    plans: dict[str, dict[str, dict[str, Any]]],
    job_id: str,
    attempt: int,
) -> dict[str, Any]:
    attempts = plans.get(job_id)
    if attempts is None:
        raise KeyError(job_id)
    plan = attempts.get(str(attempt))
    if plan is None:
        raise KeyError(attempt)
    return plan


def plan(
    jobs: str,
    supply: str,
    trades: str,
    dispatch: str,
    ledger: str,
    job_id: str,
    key: str,
    owner: str,
    source: str | None,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Create the execution plan for one claimed decision idempotently.

    ``jobs``, ``supply``, ``trades``, ``dispatch`` and ``ledger`` paths,
    ``job_id``, ``key`` and ``owner`` must be non-empty strings,
    ``source`` must be ``None`` or a string (an empty string counts as
    no source) and ``at`` a non-boolean non-negative integer moment; the
    five paths must also resolve to distinct real locations. Any
    violation raises ``ValueError`` before a business file is read.

    The ``jobs.submit`` acceptance file, the resource supply file, the
    ``market.clear`` clearing ledger and the dispatch ledger are read as
    one snapshot under their shared locks together with the execution
    ledger's exclusive lock, all five taken in resolved real-path order
    so concurrent calls can never deadlock. Only a ``claimed`` decision
    held by ``owner`` and still inside its lease is accepted: a decision
    in any other state raises ``ValueError``, a different owner raises
    ``PermissionError`` and a moment past the lease end raises
    ``TimeoutError``. The plan is numbered by the decision's dispatch
    attempt count and fixes the job, the traded resource id and version,
    the deadline, the owner and the lease end; one dispatch attempt
    yields at most one plan.

    An empty ``source`` creates a ``launch`` plan (steps ``stage`` then
    ``start``). A non-empty ``source`` creates a ``migrate`` plan (steps
    ``copy`` then ``switch``): the source must name a published resource
    other than the target whose residency list covers the job's
    residency regions, else ``LookupError``. A job without a recorded
    trade raises ``LookupError`` as well, and an unknown job or decision
    raises ``KeyError``.

    Returns ``(plan, created)``. A missing execution ledger is created
    only by this call, the plan, its idempotency binding and the audit
    event -- the complete request plus the committed plan snapshot -- in
    one synced atomic write. Replaying the same key with the same job,
    owner, source and moment returns the current plan with ``False``
    without rewriting; the same key with a different action, job,
    attempt, owner, source, step, result, receipt or moment raises
    ``ValueError`` with the ledger untouched.

    Missing input files or a missing ledger parent raise
    ``FileNotFoundError``; invalid structure, values, ordering,
    references or canonical bytes raise ``ValueError``; other locking,
    read/write or sync failures raise ``OSError``.
    """
    for value in (jobs, supply, trades, dispatch, ledger, job_id, key,
                  owner):
        if not isinstance(value, str) or not value:
            raise ValueError("jobs, supply, trades, dispatch, ledger, "
                             "job_id, key and owner must be non-empty "
                             "strings")
    if source is not None and not isinstance(source, str):
        raise ValueError("source must be None or a string")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")
    source = source or None

    job_real = os.path.realpath(jobs)
    supply_real = os.path.realpath(supply)
    trades_real = os.path.realpath(trades)
    dispatch_real = os.path.realpath(dispatch)
    ledger_real = os.path.realpath(ledger)
    if len({job_real, supply_real, trades_real, dispatch_real,
            ledger_real}) != 5:
        raise ValueError("jobs, supply, trades, dispatch and ledger "
                         "paths must be distinct real paths")

    store = _get_store(ledger)
    with store.lock:
        # Locks are taken in one resolved-real-path order shared by
        # every caller, so concurrent plans can never deadlock; the
        # execution ledger lock is exclusive, the input snapshots shared.
        with contextlib.ExitStack() as stack:
            for locked in sorted({job_real, supply_real, trades_real,
                                  dispatch_real, ledger_real}):
                stack.enter_context(
                    _lock(locked, shared=(locked != ledger_real)))

            accepted, job_map, job_events, job_raw = \
                _jobs._load_submit_file(job_real)
            if job_raw is None:
                raise FileNotFoundError(
                    f"acceptance file {job_real!r} does not exist")
            # Like the supply file and the ledgers, the acceptance
            # snapshot must be in the canonical compact form
            # jobs.submit writes.
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
            cleared, _clear_keys, trades_raw = _market._load_clear_ledger(
                trades_real, accepted, history)
            if trades_raw is None:
                raise FileNotFoundError(
                    f"clearing ledger {trades_real!r} does not exist")
            decisions, _dispatch_keys, _dispatch_events, dispatch_raw = \
                _dispatch._load_ledger(dispatch_real)
            if dispatch_raw is None:
                raise FileNotFoundError(
                    f"dispatch ledger {dispatch_real!r} does not exist")
            plans, idempotency, events, old_bytes = _load_ledger(
                ledger_real)

            request = _request("plan", job_id, at, owner=owner,
                               source=source)
            replayed = _replay(plans, idempotency, events, key, request)
            if replayed is not None:
                return replayed

            job = accepted.get(job_id)
            if job is None:
                raise KeyError(job_id)
            decision = decisions.get(job_id)
            if decision is None:
                raise KeyError(job_id)
            if decision["state"] != "claimed":
                raise ValueError("decision is not claimed")
            if decision["owner"] != owner:
                raise PermissionError("plan requires the current owner")
            if at > decision["lease_end"]:
                raise TimeoutError("the lease has already expired")
            if job_id not in cleared:
                raise LookupError("job has no recorded trade")

            attempt = decision["attempts"]
            attempts = plans.setdefault(job_id, {})
            if str(attempt) in attempts:
                raise ValueError("attempt is already planned under "
                                 "another idempotency key")

            if source is None:
                kind = "launch"
            else:
                kind = "migrate"
                source_records = history.get(source)
                if source_records is None:
                    raise LookupError("source resource is not published")
                if source == decision["resource_id"]:
                    raise LookupError("migration source must differ from "
                                      "the target resource")
                if not set(job["residency"]) <= set(
                        source_records[-1]["residency"]):
                    raise LookupError("source resource does not cover the "
                                      "job residency")

            record: dict[str, Any] = {
                "job_id": job_id,
                "attempt": attempt,
                "kind": kind,
                "source": source,
                "resource_id": decision["resource_id"],
                "version": decision["version"],
                "deadline": decision["deadline"],
                "owner": owner,
                "lease_end": decision["lease_end"],
                "state": "active",
                "steps": [],
            }
            attempts[str(attempt)] = record
            idempotency[key] = request
            events[key] = {"key": key, "request": request,
                           "result": copy.deepcopy(record)}
            _commit_file(ledger_real,
                         _canonical_bytes(plans, idempotency, events),
                         old_bytes)
            return copy.deepcopy(record), True


def record(
    ledger: str,
    job_id: str,
    attempt: int,
    key: str,
    owner: str,
    step: str,
    result: str,
    receipt: str,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Record one step receipt for an active plan idempotently.

    ``ledger``, ``job_id``, ``key``, ``owner`` and ``receipt`` must be
    non-empty strings, ``attempt`` a non-boolean positive integer,
    ``step`` the plan's next pending step (``stage``/``start`` for a
    launch, ``copy``/``switch`` for a migrate), ``result`` exactly
    ``succeeded`` or ``failed`` and ``at`` a non-boolean non-negative
    integer moment, else ``ValueError``. Only the execution ledger is
    touched, under its exclusive lock.

    Only the plan's owner acting inside the lease may record: a plan
    that is not active raises ``ValueError``, a different owner raises
    ``PermissionError`` and a moment past the lease end raises
    ``TimeoutError``. Steps advance only on success: a receipt naming
    anything but the next pending step raises ``ValueError``. A failed
    step terminates the plan as ``failed``; a successful last step
    terminates it as ``completed``; terminal plans accept no further
    receipts. Returns ``(plan, created)``; replaying the same key with
    the same job, attempt, owner, step, result, receipt and moment
    returns the current plan with ``False`` without writing, while the
    same key with a different request raises ``ValueError``.

    An unknown job or attempt raises ``KeyError``; a missing execution
    ledger raises ``FileNotFoundError``; invalid ledger structure,
    values, ordering, references or canonical bytes raise
    ``ValueError``; other locking, read/write or sync failures raise
    ``OSError``.
    """
    for value in (ledger, job_id, key, owner, receipt):
        if not isinstance(value, str) or not value:
            raise ValueError("ledger, job_id, key, owner and receipt "
                             "must be non-empty strings")
    if not _is_plain_int(attempt) or attempt < 1:
        raise ValueError("attempt must be a non-boolean positive integer")
    if step not in _ALL_STEPS:
        raise ValueError("step must be a valid plan step")
    if result not in _RESULTS:
        raise ValueError("result must be succeeded or failed")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    store = _get_store(ledger)
    with store.lock:
        with _lock(store.realpath):
            plans, idempotency, events, old_bytes = \
                _load_existing_ledger(store.realpath)

            request = _request("record", job_id, at, owner=owner,
                               attempt=attempt, step=step, result=result,
                               receipt=receipt)
            replayed = _replay(plans, idempotency, events, key, request)
            if replayed is not None:
                return replayed

            current = _lookup_plan(plans, job_id, attempt)
            if current["state"] != "active":
                raise ValueError("plan is not active")
            if current["owner"] != owner:
                raise PermissionError("record requires the plan owner")
            if at > current["lease_end"]:
                raise TimeoutError("the lease has already expired")
            sequence = _STEPS[current["kind"]]
            if step != sequence[len(current["steps"])]:
                raise ValueError("step is not the plan's next pending "
                                 "step")

            current["steps"].append({"step": step, "result": result,
                                     "receipt": receipt, "at": at})
            if result == "failed":
                current["state"] = "failed"
            elif len(current["steps"]) == len(sequence):
                current["state"] = "completed"
            idempotency[key] = request
            events[key] = {"key": key, "request": request,
                           "result": copy.deepcopy(current)}
            _commit_file(store.realpath,
                         _canonical_bytes(plans, idempotency, events),
                         old_bytes)
            return copy.deepcopy(current), True


def recover(
    ledger: str,
    job_id: str,
    attempt: int,
    key: str,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Interrupt an unfinished plan whose lease has strictly expired.

    ``ledger``, ``job_id`` and ``key`` must be non-empty strings,
    ``attempt`` a non-boolean positive integer and ``at`` a non-boolean
    non-negative integer moment, else ``ValueError``. Only the execution
    ledger is touched, under its exclusive lock.

    A plan that is not active raises ``ValueError``; one whose lease has
    not strictly expired yet raises ``PermissionError``. A successful
    recovery turns the plan ``interrupted`` and preserves every recorded
    receipt. Returns ``(plan, created)``; replaying the same key with
    the same job, attempt and moment returns the current plan with
    ``False`` without writing, while the same key with a different
    request raises ``ValueError``.

    An unknown job or attempt raises ``KeyError``; a missing execution
    ledger raises ``FileNotFoundError``; invalid ledger structure,
    values, ordering, references or canonical bytes raise
    ``ValueError``; other locking, read/write or sync failures raise
    ``OSError``.
    """
    for value in (ledger, job_id, key):
        if not isinstance(value, str) or not value:
            raise ValueError("ledger, job_id and key must be non-empty "
                             "strings")
    if not _is_plain_int(attempt) or attempt < 1:
        raise ValueError("attempt must be a non-boolean positive integer")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    store = _get_store(ledger)
    with store.lock:
        with _lock(store.realpath):
            plans, idempotency, events, old_bytes = \
                _load_existing_ledger(store.realpath)

            request = _request("recover", job_id, at, attempt=attempt)
            replayed = _replay(plans, idempotency, events, key, request)
            if replayed is not None:
                return replayed

            current = _lookup_plan(plans, job_id, attempt)
            if current["state"] != "active":
                raise ValueError("plan is not active")
            if at <= current["lease_end"]:
                raise PermissionError("the lease has not expired yet")

            current["state"] = "interrupted"
            idempotency[key] = request
            events[key] = {"key": key, "request": request,
                           "result": copy.deepcopy(current)}
            _commit_file(store.realpath,
                         _canonical_bytes(plans, idempotency, events),
                         old_bytes)
            return copy.deepcopy(current), True
