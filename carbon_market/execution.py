"""Recoverable execution plans over the dispatch commitment layer.

The baseline dispatch layer records *decisions* -- claims, leases and
finish results -- but a claim never says which concrete steps must run
to put the claimed resource to work. This module adds the execution
layer on top of the accepted jobs, the versioned supply, the clearing
ledger and the dispatch ledger with three persistent, idempotent calls
over one more ledger:

* :func:`plan` freezes one execution plan for a decision currently held
  by an owner inside its lease, numbered by the decision's dispatch
  attempt count; an empty source produces a ``launch`` (steps
  ``stage`` then ``start``) and a non-empty source a ``migrate`` (steps
  ``copy`` then ``switch``);
* :func:`record` appends one step receipt (``succeeded`` or ``failed``
  with a non-empty receipt string) and advances a running plan only in
  its fixed step order -- any failed step ends the attempt ``failed``,
  the last successful step ends it ``completed``;
* :func:`recover` marks a still unfinished plan ``interrupted`` once
  its lease has strictly expired, keeping every recorded receipt.

One on-disk document (version 1), the execution ledger, holds the whole
layer:

* ``plans`` maps each job id to its attempts, an attempt-ordered list
  of frozen plans -- kind, source (``null`` for a launch) and target
  resource version, deadline, owner, lease end, lifecycle state and the
  ordered step receipts;
* ``idempotency`` binds each call key, shared by all three entries, to
  the complete request it first served;
* ``audit`` holds one event per first-served key, binding the complete
  request and the plan snapshot the call committed.

:func:`plan` reads the acceptance file, the supply file, the clearing
ledger and the dispatch ledger as one snapshot under their shared locks
together with this ledger's exclusive lock, all five taken in resolved
real-path order; :func:`record` and :func:`recover` take only this
ledger's exclusive lock and fail with ``FileNotFoundError`` when the
ledger does not exist.

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
_PLAN_FIELDS = ("job_id", "attempt", "kind", "source", "target",
                "deadline", "owner", "lease_end", "state", "receipts")
_RECEIPT_FIELDS = ("step", "result", "receipt", "at")
_SELECTION_FIELDS = ("resource_id", "version")
_EVENT_FIELDS = ("key", "request", "result")
_KIND_STEPS = {"launch": ("stage", "start"),
               "migrate": ("copy", "switch")}
_STATES = ("running", "failed", "completed", "interrupted")
_RESULTS = ("succeeded", "failed")
_REQUEST_FIELDS = {
    "plan": ("action", "job_id", "attempt", "source", "target", "at"),
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
    # processes.
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


def _validate_selection(value: object) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict) \
            or set(value.keys()) != set(_SELECTION_FIELDS):
        raise ValueError("resource selection must be null or an object "
                         "with resource_id and version")
    resource_id = value["resource_id"]
    version = value["version"]
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("selection resource_id must be a non-empty string")
    if not _is_plain_int(version) or version < 1:
        raise ValueError("selection version must be a positive integer")
    return {"resource_id": resource_id, "version": version}


def _validate_receipt(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) \
            or set(value.keys()) != set(_RECEIPT_FIELDS):
        raise ValueError("step receipt has invalid fields")
    step = value["step"]
    result = value["result"]
    receipt = value["receipt"]
    at = value["at"]
    if not isinstance(step, str) or not step:
        raise ValueError("receipt step must be a non-empty string")
    if result not in _RESULTS:
        raise ValueError("receipt result must be succeeded or failed")
    if not isinstance(receipt, str) or not receipt:
        raise ValueError("receipt string must be a non-empty string")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("receipt at must be a non-boolean non-negative "
                         "integer")
    return {"step": step, "result": result, "receipt": receipt, "at": at}


def _validate_plan(record: object) -> dict[str, Any]:
    if not isinstance(record, dict) \
            or set(record.keys()) != set(_PLAN_FIELDS):
        raise ValueError("execution plan has invalid fields")
    job_id = record["job_id"]
    attempt = record["attempt"]
    kind = record["kind"]
    source = record["source"]
    target = record["target"]
    deadline = record["deadline"]
    owner = record["owner"]
    lease_end = record["lease_end"]
    state = record["state"]
    receipts_raw = record["receipts"]

    if not isinstance(job_id, str) or not job_id:
        raise ValueError("plan job_id must be a non-empty string")
    if not _is_plain_int(attempt) or attempt < 1:
        raise ValueError("plan attempt must be a positive integer")
    if kind not in _KIND_STEPS:
        raise ValueError("plan kind must be launch or migrate")
    source_sel = _validate_selection(source)
    target_sel = _validate_selection(target)
    if target_sel is None:
        raise ValueError("plan must freeze a target resource version")
    if kind == "launch" and source_sel is not None:
        raise ValueError("a launch plan has no source")
    if kind == "migrate":
        if source_sel is None:
            raise ValueError("a migrate plan must name a source")
        if source_sel == target_sel:
            raise ValueError("migrate target must differ from the source")
    if not _is_plain_int(deadline) or deadline < 0:
        raise ValueError("plan deadline must be a non-boolean non-negative "
                         "integer")
    if not isinstance(owner, str) or not owner:
        raise ValueError("plan owner must be a non-empty string")
    if not _is_plain_int(lease_end) or lease_end < 1:
        raise ValueError("plan lease_end must be a positive integer")
    if lease_end > deadline:
        raise ValueError("plan lease end must not pass the job deadline")
    if state not in _STATES:
        raise ValueError("plan state is invalid")
    if not isinstance(receipts_raw, list):
        raise ValueError("plan receipts must be a list")

    steps = _KIND_STEPS[kind]
    receipts: list[dict[str, Any]] = []
    failed = False
    for index, receipt_raw in enumerate(receipts_raw):
        receipt = _validate_receipt(receipt_raw)
        if failed:
            raise ValueError("no receipt may follow a failed step")
        if index >= len(steps) or receipt["step"] != steps[index]:
            raise ValueError("plan receipts must follow the fixed step "
                             "order")
        if receipt["at"] > lease_end:
            raise ValueError("plan receipt is past the lease end")
        receipts.append(receipt)
        if receipt["result"] == "failed":
            failed = True

    # The state is fully determined by the receipt history: a failed
    # step ends the attempt failed, the final successful step completes
    # it, anything else is still running unless recovery interrupted it.
    if receipts and receipts[-1]["result"] == "failed":
        if state != "failed":
            raise ValueError("a plan ending on a failed step is failed")
    elif len(receipts) == len(steps) \
            and receipts and receipts[-1]["result"] == "succeeded":
        if state != "completed":
            raise ValueError("a plan past its final successful step is "
                             "completed")
    elif state not in ("running", "interrupted"):
        raise ValueError("an unfinished plan is running or interrupted")

    return {
        "job_id": job_id,
        "attempt": attempt,
        "kind": kind,
        "source": copy.deepcopy(source_sel),
        "target": copy.deepcopy(target_sel),
        "deadline": deadline,
        "owner": owner,
        "lease_end": lease_end,
        "state": state,
        "receipts": receipts,
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
    attempt = request["attempt"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("request job_id must be a non-empty string")
    if not _is_plain_int(attempt) or attempt < 1:
        raise ValueError("request attempt must be a positive integer")
    at = request["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("request at must be a non-boolean non-negative "
                         "integer")
    normalized: dict[str, Any] = {"action": action, "job_id": job_id,
                                  "attempt": attempt}
    if action == "plan":
        normalized["source"] = _validate_selection(request["source"])
        normalized["target"] = _validate_selection(request["target"])
        if normalized["target"] is None:
            raise ValueError("a plan request must bind its target "
                             "selection")
        if normalized["source"] is not None \
                and normalized["source"] == normalized["target"]:
            raise ValueError("a migrate plan must move to a different "
                             "target")
    if action == "record":
        owner = request["owner"]
        if not isinstance(owner, str) or not owner:
            raise ValueError("request owner must be a non-empty string")
        normalized["owner"] = owner
        step = request["step"]
        if not isinstance(step, str) or not step:
            raise ValueError("request step must be a non-empty string")
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


def _snapshot_after(plan: dict[str, Any], receipts: list[dict[str, Any]]
                    ) -> str:
    steps = _KIND_STEPS[plan["kind"]]
    if receipts and receipts[-1]["result"] == "failed":
        return "failed"
    if len(receipts) == len(steps):
        return "completed"
    return "running"


def _validate_ledger(
    data: object,
) -> tuple[dict[str, list[dict[str, Any]]],
           dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
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

    plans: dict[str, list[dict[str, Any]]] = {}
    for job_id, attempts_raw in plans_raw.items():
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("plan job ids must be non-empty strings")
        if not isinstance(attempts_raw, list) or not attempts_raw:
            raise ValueError("each planned job must carry a non-empty list "
                             "of plans")
        attempts: list[dict[str, Any]] = []
        last_attempt = 0
        for plan_raw in attempts_raw:
            plan = _validate_plan(plan_raw)
            if plan["job_id"] != job_id:
                raise ValueError("plan identity does not match its job key")
            if plan["attempt"] <= last_attempt:
                raise ValueError("plans must be ordered by attempt")
            last_attempt = plan["attempt"]
            attempts.append(plan)
        plans[job_id] = attempts

    idempotency: dict[str, dict[str, Any]] = {}
    for key, request_raw in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        request = _validate_request(request_raw)
        if _find(plans.get(request["job_id"]), request["attempt"]) is None:
            raise ValueError("idempotency entry must reference a recorded "
                             "plan")
        idempotency[key] = request

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
        if request["job_id"] != result["job_id"] \
                or request["attempt"] != result["attempt"]:
            raise ValueError("audit event result does not match its "
                             "request")
        events[key] = {"key": key, "request": request, "result": result}

    # The three sections describe one execution history: each
    # idempotency key binds one request and one event carrying exactly
    # that request.
    if set(idempotency) != set(events):
        raise ValueError("idempotency keys and audit events do not match")
    for key, request in idempotency.items():
        if events[key]["request"] != request:
            raise ValueError("audit event does not match its idempotency "
                             "entry")

    # Replay every attempt's key history: exactly one plan, an unbroken
    # step-ordered prefix of receipts and at most one recovery that is
    # the final action on a still unfinished plan. Each event's result
    # snapshot must show exactly the state its action committed.
    for job_id, attempts in plans.items():
        for plan in attempts:
            attempt = plan["attempt"]
            history = [
                (key, idempotency[key])
                for key in sorted(idempotency)
                if idempotency[key]["job_id"] == job_id
                and idempotency[key]["attempt"] == attempt
            ]
            plan_keys = [key for key, request in history
                         if request["action"] == "plan"]
            if len(plan_keys) != 1:
                raise ValueError("each attempt must be planned exactly "
                                 "once")
            plan_key = plan_keys[0]
            plan_request = idempotency[plan_key]
            initial = events[plan_key]["result"]
            for field in ("kind", "source", "target", "deadline", "owner",
                          "lease_end"):
                if initial[field] != plan[field]:
                    raise ValueError("plan snapshot does not match its "
                                     "plan")
            if plan_request["source"] != plan["source"] \
                    or plan_request["target"] != plan["target"]:
                raise ValueError("plan request does not match its frozen "
                                 "selections")
            if initial["state"] != "running" or initial["receipts"]:
                raise ValueError("plan audit result must be a fresh "
                                 "running plan")
            if plan_request["at"] > plan["lease_end"]:
                raise ValueError("plan was created outside its lease")

            record_keys: dict[str, str] = {}
            by_step: dict[str, dict[str, Any]] = {}
            recover_keys: list[str] = []
            for key, request in history:
                if request["action"] == "record":
                    if request["owner"] != plan["owner"]:
                        raise ValueError("record must be made by the plan "
                                         "owner")
                    if request["step"] not in _KIND_STEPS[plan["kind"]]:
                        raise ValueError("record step does not belong to "
                                         "the plan kind")
                    if request["step"] in by_step:
                        raise ValueError("a step can only be recorded "
                                         "once")
                    by_step[request["step"]] = request
                    record_keys[request["step"]] = key
                elif request["action"] == "recover":
                    recover_keys.append(key)
            if len(recover_keys) > 1:
                raise ValueError("a plan can be recovered at most once")

            steps = _KIND_STEPS[plan["kind"]]
            receipts: list[dict[str, Any]] = []
            for index, step in enumerate(steps):
                request = by_step.get(step)
                if request is None:
                    # Records form an unbroken prefix; later steps
                    # cannot arrive without earlier ones.
                    if any(later in by_step for later in steps[index + 1:]):
                        raise ValueError("step receipts must be contiguous")
                    break
                receipts.append({"step": step, "result": request["result"],
                                 "receipt": request["receipt"],
                                 "at": request["at"]})
                snapshot = events[record_keys[step]]["result"]
                if snapshot["receipts"] != receipts:
                    raise ValueError("record audit result does not carry "
                                     "the receipt prefix")
                expected = _snapshot_after(plan, receipts)
                if snapshot["state"] != expected:
                    raise ValueError("record audit result state is wrong")
                for field in ("kind", "source", "target", "deadline",
                              "owner", "lease_end"):
                    if snapshot[field] != plan[field]:
                        raise ValueError("record audit result does not "
                                         "match its plan")

            if receipts != plan["receipts"]:
                raise ValueError("plan receipts do not match their "
                                 "records")

            if recover_keys:
                recover_key = recover_keys[0]
                request = idempotency[recover_key]
                if request["at"] <= plan["lease_end"]:
                    raise ValueError("recovery must follow strict lease "
                                     "expiry")
                if _snapshot_after(plan, receipts) != "running":
                    raise ValueError("only an unfinished plan can be "
                                     "interrupted")
                snapshot = events[recover_key]["result"]
                if snapshot["state"] != "interrupted" \
                        or snapshot["receipts"] != receipts:
                    raise ValueError("recovery audit result must preserve "
                                     "the receipts")
                if plan["state"] != "interrupted":
                    raise ValueError("a recovered plan is interrupted")
            else:
                expected = _snapshot_after(plan, receipts)
                if plan["state"] != expected:
                    raise ValueError("plan state does not match its "
                                     "receipts")

    return plans, idempotency, events


def _canonical_bytes(
    plans: dict[str, list[dict[str, Any]]],
    idempotency: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, sections in their
    # fixed field order and each section's primary keys (and attempt
    # keys) in code-point order, terminated by exactly one newline.
    payload = {
        "version": _VERSION,
        "plans": {
            job_id: plans[job_id]
            for job_id in sorted(plans)
        },
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
        "audit": {key: events[key] for key in sorted(events)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_ledger(
    realpath: str,
) -> tuple[dict[str, list[dict[str, Any]]],
           dict[str, dict[str, Any]], dict[str, dict[str, Any]], bytes | None]:
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
    # One durable commit for the plan or receipt, the idempotency
    # binding and the audit event: synced same-directory temporary,
    # atomic replace and a directory fsync, restoring the pre-call bytes
    # on any failure.
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


def _load_existing_ledger(
    realpath: str,
) -> tuple[dict[str, list[dict[str, Any]]],
           dict[str, dict[str, Any]], dict[str, dict[str, Any]], bytes]:
    plans, idempotency, events, raw = _load_ledger(realpath)
    if raw is None:
        # Only the first plan creates the execution ledger; records and
        # recovery fail directly when it is missing.
        raise FileNotFoundError(
            f"execution ledger {realpath!r} does not exist")
    return plans, idempotency, events, raw


def _replay(
    plans: dict[str, list[dict[str, Any]]],
    idempotency: dict[str, dict[str, Any]],
    key: str,
    request: dict[str, Any],
) -> tuple[dict[str, Any], bool] | None:
    binding = idempotency.get(key)
    if binding is None:
        return None
    if binding != request:
        raise ValueError("idempotency key was already used with a "
                         "different request")
    # An equivalent replay returns the current plan without writing.
    for candidate in plans.get(request["job_id"], ()):
        if candidate["attempt"] == request["attempt"]:
            return copy.deepcopy(candidate), False
    raise ValueError("idempotency entry references a missing plan")


def _find(job_plans: list[dict[str, Any]] | None, attempt: int
          ) -> dict[str, Any] | None:
    if job_plans is None:
        return None
    for candidate in job_plans:
        if candidate["attempt"] == attempt:
            return candidate
    return None


def _published(history: dict[str, list[dict[str, Any]]],
               selection: dict[str, Any]) -> dict[str, Any] | None:
    records = history.get(selection["resource_id"])
    if records is None or selection["version"] > len(records):
        return None
    return records[selection["version"] - 1]


def plan(
    jobs: str,
    supply: str,
    trades: str,
    dispatch: str,
    ledger: str,
    job_id: str,
    key: str,
    source: dict[str, object] | None,
    target: dict[str, object] | None,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Freeze one execution plan for a claimed decision idempotently.

    ``jobs``, ``supply``, ``trades``, ``dispatch`` and ``ledger`` paths,
    ``job_id`` and ``key`` must be non-empty strings and ``at`` a
    non-boolean non-negative integer moment; the five paths must also
    resolve to distinct real locations. ``source`` is ``None`` for a
    launch or an object with exactly ``resource_id`` (a non-empty
    string) and ``version`` (a positive integer) for a migration;
    ``target`` must be ``None`` for a launch and a selection for a
    migration. Any violation raises ``ValueError`` before a business
    file is read.

    The acceptance file, the supply file, the clearing ledger and the
    dispatch ledger are read as one snapshot under their shared locks
    together with this ledger's exclusive lock, all five taken in
    resolved real-path order. Only a decision currently ``claimed`` by
    its owner with ``at`` inside its lease is accepted; the plan is
    numbered with the decision's dispatch attempt count and freezes the
    job id, that attempt, the kind, the source and target resource
    versions, the deadline, the owner and the lease end. A launch runs
    ``stage`` then ``start`` off the claimed resource; a migration runs
    ``copy`` then ``switch`` from the claimed resource to a different
    existing target version whose region the job permits and whose
    residency covers the job's residency regions.

    Returns ``(plan, created)``; the plan starts ``running`` with no
    receipts, and the plan, its idempotency binding and the audit event
    -- the complete request plus the committed plan snapshot -- commit
    in one synced atomic write. Replaying the same key with the same
    request returns the current plan with ``False`` without rewriting;
    the same key with a different request, or a second plan for the
    same job and dispatch attempt, raises ``ValueError``.

    An unknown job raises ``KeyError``; no dispatch decision, no trade
    or an unavailable source or target resource raises ``LookupError``;
    a decision that is not currently claimed, a source other than the
    claimed resource, a source equal to the target or a residency
    violation raises ``ValueError``; a moment past the lease end raises
    ``TimeoutError``. Missing input files or a missing ledger parent
    raise ``FileNotFoundError``; invalid structure, values, ordering,
    references or canonical bytes raise ``ValueError``; other locking,
    read/write or sync failures raise ``OSError``.
    """
    for value in (jobs, supply, trades, dispatch, ledger, job_id, key):
        if not isinstance(value, str) or not value:
            raise ValueError("jobs, supply, trades, dispatch, ledger, "
                             "job_id and key must be non-empty strings")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")
    if source is None:
        if target is not None:
            raise ValueError("a launch plan takes its target from the "
                             "claimed decision")
        source_sel = None
        target_sel = None
    else:
        source_sel = _validate_selection(source)
        if target is None:
            raise ValueError("a migrate plan must name a target")
        target_sel = _validate_selection(target)

    job_real = os.path.realpath(jobs)
    supply_real = os.path.realpath(supply)
    trades_real = os.path.realpath(trades)
    dispatch_real = os.path.realpath(dispatch)
    ledger_real = os.path.realpath(ledger)
    if len({job_real, supply_real, trades_real, dispatch_real,
            ledger_real}) != 5:
        raise ValueError("jobs, supply, trades, dispatch and ledger paths "
                         "must be distinct real paths")

    store = _get_store(ledger)
    with store.lock:
        # Locks are taken in one resolved-real-path order shared by
        # every caller, so concurrent plans can never deadlock; the
        # execution ledger lock is exclusive, the input snapshots
        # shared.
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

            job = accepted.get(job_id)
            if job is None:
                raise KeyError(job_id)
            decision = decisions.get(job_id)
            if decision is None:
                raise LookupError("job has no dispatch decision")

            attempt = decision["attempts"]
            claimed_sel = {"resource_id": decision["resource_id"],
                           "version": decision["version"]}
            if source_sel is None:
                kind = "launch"
                # A launch freezes the claimed resource version; that
                # resolved selection is what the idempotency request
                # binds, not the caller's null argument.
                bound_source = None
                bound_target = dict(claimed_sel)
            else:
                kind = "migrate"
                bound_source = copy.deepcopy(source_sel)
                bound_target = copy.deepcopy(target_sel)
            request = {"action": "plan", "job_id": job_id,
                       "attempt": attempt, "source": bound_source,
                       "target": bound_target, "at": at}
            replayed = _replay(plans, idempotency, key, request)
            if replayed is not None:
                return replayed

            if decision["state"] != "claimed":
                raise ValueError("only a claimed decision accepts a plan")
            if at > decision["lease_end"]:
                raise TimeoutError("the claim lease has already expired")

            job_plans = plans.setdefault(job_id, [])
            if _find(job_plans, attempt) is not None:
                raise ValueError("a plan already exists for this dispatch "
                                 "attempt")

            trade = cleared.get(job_id)
            if trade is None:
                raise LookupError("job has no recorded trade")

            if kind == "migrate":
                if source_sel == bound_target:
                    raise ValueError("migration target must differ from "
                                     "the source")
                if source_sel != claimed_sel:
                    raise ValueError("migration source must be the claimed "
                                     "resource version")
                source_record = _published(history, source_sel)
                if source_record is None:
                    raise LookupError("migration source resource version "
                                      "is unavailable")
                if source_record["region"] not in set(job["regions"]) \
                        or not set(job["residency"]) \
                        <= set(source_record["residency"]):
                    raise ValueError("migration source does not satisfy the "
                                     "job residency constraints")

            target_record = _published(history, bound_target)
            if target_record is None:
                raise LookupError("target resource version is unavailable")
            if target_record["region"] not in set(job["regions"]):
                raise ValueError("target region is not permitted by the "
                                 "job")
            if not set(job["residency"]) <= set(target_record["residency"]):
                raise ValueError("target does not satisfy the job's data "
                                 "residency constraints")

            plan_record: dict[str, Any] = {
                "job_id": job_id,
                "attempt": attempt,
                "kind": kind,
                "source": bound_source,
                "target": bound_target,
                "deadline": decision["deadline"],
                "owner": decision["owner"],
                "lease_end": decision["lease_end"],
                "state": "running",
                "receipts": [],
            }
            # Attempts stay ordered by the dispatch attempt number.
            job_plans.append(plan_record)
            job_plans.sort(key=lambda entry: entry["attempt"])
            idempotency[key] = request
            events[key] = {"key": key, "request": request,
                           "result": copy.deepcopy(plan_record)}
            _commit_file(ledger_real,
                         _canonical_bytes(plans, idempotency, events),
                         old_bytes)
            return copy.deepcopy(plan_record), True


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
    """Append one step receipt to a running execution plan idempotently.

    ``ledger``, ``job_id``, ``key``, ``owner``, ``step`` and ``receipt``
    must be non-empty strings, ``attempt`` a non-boolean positive
    integer (the dispatch attempt number the plan was frozen for),
    ``result`` exactly ``succeeded`` or ``failed`` and ``at`` a
    non-boolean non-negative integer moment, else ``ValueError``. Only
    the execution ledger is touched, under its exclusive lock.

    Only the plan's owner acting inside its lease may record: a plan in
    a terminal state raises ``ValueError`` (no receipt may be appended),
    a different owner raises ``PermissionError`` and a moment later than
    the lease end raises ``TimeoutError``. The step must be the next
    step of the plan's fixed order (``stage``/``start`` for a launch,
    ``copy``/``switch`` for a migration); skipping, repeating or
    recording an out-of-kind step raises ``ValueError``. A failed step
    ends the attempt ``failed``; the final successful step ends it
    ``completed`` and every other successful step leaves the plan
    ``running``. Returns ``(plan, created)``; replaying the same key
    with the same request returns the current plan with ``False``
    without writing, while the same key with a different request raises
    ``ValueError``.

    An unknown job or attempt raises ``KeyError``; a missing execution
    ledger raises ``FileNotFoundError``; invalid ledger structure,
    values, ordering, references or canonical bytes raise ``ValueError``;
    other locking, read/write or sync failures raise ``OSError``.
    """
    for value in (ledger, job_id, key, owner, step, receipt):
        if not isinstance(value, str) or not value:
            raise ValueError("ledger, job_id, key, owner, step and "
                             "receipt must be non-empty strings")
    if not _is_plain_int(attempt) or attempt < 1:
        raise ValueError("attempt must be a non-boolean positive integer")
    if result not in _RESULTS:
        raise ValueError("result must be succeeded or failed")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    store = _get_store(ledger)
    with store.lock:
        with _lock(store.realpath):
            plans, idempotency, events, old_bytes = \
                _load_existing_ledger(store.realpath)

            request = {"action": "record", "job_id": job_id,
                       "attempt": attempt, "owner": owner, "step": step,
                       "result": result, "receipt": receipt, "at": at}
            replayed = _replay(plans, idempotency, key, request)
            if replayed is not None:
                return replayed

            plan_record = _find(plans.get(job_id), attempt)
            if plan_record is None:
                if job_id not in plans:
                    raise KeyError(job_id)
                raise KeyError(attempt)

            if plan_record["state"] != "running":
                raise ValueError("a terminal plan accepts no more receipts")
            if plan_record["owner"] != owner:
                raise PermissionError("recording requires the plan owner")
            steps = _KIND_STEPS[plan_record["kind"]]
            index = len(plan_record["receipts"])
            if index >= len(steps) or step != steps[index]:
                raise ValueError("step does not follow the plan's fixed "
                                 "step order")
            if at > plan_record["lease_end"]:
                raise TimeoutError("the plan lease has already expired")

            plan_record["receipts"].append(
                {"step": step, "result": result, "receipt": receipt,
                 "at": at})
            if result == "failed":
                plan_record["state"] = "failed"
            elif step == steps[-1]:
                plan_record["state"] = "completed"
            idempotency[key] = request
            events[key] = {"key": key, "request": request,
                           "result": copy.deepcopy(plan_record)}
            _commit_file(store.realpath,
                         _canonical_bytes(plans, idempotency, events),
                         old_bytes)
            return copy.deepcopy(plan_record), True


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

    A plan that is not still running (already failed, completed or
    interrupted) raises ``ValueError``; recovery before the lease end
    is strictly past raises ``PermissionError``. A successful recovery
    changes the state to ``interrupted`` and preserves every recorded
    receipt byte for byte. Returns ``(plan, created)``; replaying the
    same key with the same request returns the current plan with
    ``False`` without writing, while the same key with a different
    request raises ``ValueError``.

    An unknown job or attempt raises ``KeyError``; a missing execution
    ledger raises ``FileNotFoundError``; invalid ledger structure,
    values, ordering, references or canonical bytes raise ``ValueError``;
    other locking, read/write or sync failures raise ``OSError``.
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

            request = {"action": "recover", "job_id": job_id,
                       "attempt": attempt, "at": at}
            replayed = _replay(plans, idempotency, key, request)
            if replayed is not None:
                return replayed

            plan_record = _find(plans.get(job_id), attempt)
            if plan_record is None:
                if job_id not in plans:
                    raise KeyError(job_id)
                raise KeyError(attempt)

            if plan_record["state"] != "running":
                raise ValueError("only a running plan can be interrupted")
            if at <= plan_record["lease_end"]:
                raise PermissionError("the lease has not expired yet")

            plan_record["state"] = "interrupted"
            idempotency[key] = request
            events[key] = {"key": key, "request": request,
                           "result": copy.deepcopy(plan_record)}
            _commit_file(store.realpath,
                         _canonical_bytes(plans, idempotency, events),
                         old_bytes)
            return copy.deepcopy(plan_record), True
