"""Persistent dispatch commitments over ``market.clear`` trades.

One on-disk document (version 1), the dispatch ledger, holds the whole
dispatch layer on top of the clearing ledger:

* ``decisions`` maps each job id to its scheduling decision, binding the
  traded resource version and the job deadline, with a lifecycle state
  (``ready``, ``claimed``, ``succeeded`` or ``failed``), an attempt
  counter and, while claimed, the owner and the lease end;
* ``idempotency`` binds each call key to the complete request it first
  served (action, job id and moment, plus owner, lease or result where
  the action carries them);
* ``audit`` holds one event per first-served key, binding the complete
  request and the decision snapshot the call committed.

:func:`commit` reads the ``jobs.submit`` acceptance file, the resource
supply file and the ``market.clear`` clearing ledger as one snapshot
under their shared locks together with the dispatch ledger's exclusive
lock, all four taken in resolved real-path order, and creates the
decision for one traded job in state ``ready`` with zero attempts.
:func:`claim`, :func:`finish` and :func:`recover` only touch the
dispatch ledger under its exclusive lock: claiming moves a ``ready`` or
``failed`` decision to ``claimed`` with one more attempt and a lease
that never passes the job deadline, finishing lets the current owner
inside its lease record ``succeeded`` or ``failed`` and clears owner
and lease, and recovering returns a decision whose lease has strictly
expired to ``ready`` without counting an attempt. The dispatch ledger
is created only by the first commit; every other call facing a missing
ledger fails with ``FileNotFoundError``.

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

from . import jobs as _jobs
from . import market as _market
from . import resources as _resources
from ._jsonio import finite_loads

__all__ = ["commit", "claim", "finish", "recover"]

_VERSION = 1
_ROOT_FIELDS = ("version", "decisions", "idempotency", "audit")
_DECISION_FIELDS = ("job_id", "at", "resource_id", "version", "deadline",
                    "state", "attempts", "owner", "lease_end")
_STATES = ("ready", "claimed", "succeeded", "failed")
_RESULTS = ("succeeded", "failed")
_EVENT_FIELDS = ("key", "request", "result")
_REQUEST_FIELDS = {
    "commit": ("action", "job_id", "at"),
    "claim": ("action", "job_id", "owner", "lease", "at"),
    "finish": ("action", "job_id", "owner", "result", "at"),
    "recover": ("action", "job_id", "at"),
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


def _validate_decision(record: object) -> dict[str, Any]:
    if not isinstance(record, dict) \
            or set(record.keys()) != set(_DECISION_FIELDS):
        raise ValueError("decision record has invalid fields")
    job_id = record["job_id"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("decision job_id must be a non-empty string")
    at = record["at"]
    if not _is_plain_int(at) or at < 0:
        raise ValueError("decision at must be a non-boolean non-negative "
                         "integer")
    resource_id = record["resource_id"]
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("decision resource_id must be a non-empty string")
    version = record["version"]
    if not _is_plain_int(version) or version < 1:
        raise ValueError("decision version must be a positive integer")
    deadline = record["deadline"]
    if not _is_plain_int(deadline) or deadline < 0:
        raise ValueError("decision deadline must be a non-boolean "
                         "non-negative integer")
    state = record["state"]
    if state not in _STATES:
        raise ValueError("decision state is invalid")
    attempts = record["attempts"]
    if not _is_plain_int(attempts) or attempts < 0:
        raise ValueError("decision attempts must be a non-boolean "
                         "non-negative integer")
    owner = record["owner"]
    lease_end = record["lease_end"]
    if state == "claimed":
        if not isinstance(owner, str) or not owner:
            raise ValueError("claimed decision must carry a non-empty "
                             "owner")
        if not _is_plain_int(lease_end) or lease_end < 1:
            raise ValueError("claimed decision must carry a positive "
                             "lease end")
        if lease_end > deadline:
            raise ValueError("decision lease end must not pass the job "
                             "deadline")
    elif owner is not None or lease_end is not None:
        raise ValueError("an unclaimed decision must not carry an owner "
                         "or a lease")
    return {
        "job_id": job_id,
        "at": at,
        "resource_id": resource_id,
        "version": version,
        "deadline": deadline,
        "state": state,
        "attempts": attempts,
        "owner": owner,
        "lease_end": lease_end,
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
    if action in ("claim", "finish"):
        owner = request["owner"]
        if not isinstance(owner, str) or not owner:
            raise ValueError("request owner must be a non-empty string")
        normalized["owner"] = owner
    if action == "claim":
        lease = request["lease"]
        if not _is_plain_int(lease) or lease <= 0:
            raise ValueError("request lease must be a non-boolean "
                             "positive integer")
        normalized["lease"] = lease
    if action == "finish":
        result = request["result"]
        if result not in _RESULTS:
            raise ValueError("request result is invalid")
        normalized["result"] = result
    normalized["at"] = at
    return {field: normalized[field] for field in _REQUEST_FIELDS[action]}


def _validate_ledger(
    data: object,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("dispatch ledger root must be an object with keys "
                         "version, decisions, idempotency and audit")
    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported dispatch ledger version")

    decisions_raw = data["decisions"]
    idempotency_raw = data["idempotency"]
    audit_raw = data["audit"]
    if not isinstance(decisions_raw, dict) \
            or not isinstance(idempotency_raw, dict) \
            or not isinstance(audit_raw, dict):
        raise ValueError("decisions, idempotency and audit must be objects")
    _check_sorted_keys(decisions_raw, "decisions")
    _check_sorted_keys(idempotency_raw, "idempotency")
    _check_sorted_keys(audit_raw, "audit")

    decisions: dict[str, dict[str, Any]] = {}
    for job_id, record in decisions_raw.items():
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("decision job ids must be non-empty strings")
        decision = _validate_decision(record)
        if decision["job_id"] != job_id:
            raise ValueError("decision record id does not match its key")
        decisions[job_id] = decision

    idempotency: dict[str, dict[str, Any]] = {}
    for key, request_raw in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        request = _validate_request(request_raw)
        if request["job_id"] not in decisions:
            raise ValueError("idempotency entry must reference a recorded "
                             "decision")
        idempotency[key] = request

    events: dict[str, dict[str, Any]] = {}
    for key, event_raw in audit_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event_raw, dict) \
                or set(event_raw.keys()) != set(_EVENT_FIELDS):
            raise ValueError("dispatch audit event has invalid fields")
        if event_raw["key"] != key or not isinstance(event_raw["key"], str):
            raise ValueError("audit event key does not match its map key")
        request = _validate_request(event_raw["request"])
        result = _validate_decision(event_raw["result"])
        if request["job_id"] != result["job_id"]:
            raise ValueError("audit event result does not match its "
                             "request")
        events[key] = {"key": key, "request": request, "result": result}

    # The three sections describe one dispatch history: each idempotency
    # key binds one request and one event, and the event carries exactly
    # the bound request.
    if set(idempotency) != set(events):
        raise ValueError("idempotency keys and audit events do not match")
    for key, request in idempotency.items():
        if events[key]["request"] != request:
            raise ValueError("audit event does not match its idempotency "
                             "entry")

    # A decision's job id, commit moment, traded resource version and
    # deadline never change after the commit; every recorded result
    # snapshot must agree with them, and each action's snapshot must
    # show exactly the state the action commits.
    commits: dict[str, str] = {}
    claims: dict[str, int] = {}
    for key, request in idempotency.items():
        job_id = request["job_id"]
        decision = decisions[job_id]
        result = events[key]["result"]
        for field in ("at", "resource_id", "version", "deadline"):
            if result[field] != decision[field]:
                raise ValueError("audit event result does not match its "
                                 "decision")
        action = request["action"]
        if action == "commit":
            if job_id in commits:
                raise ValueError("job committed under more than one "
                                 "idempotency key")
            commits[job_id] = key
            if result["at"] != request["at"] or result["state"] != "ready" \
                    or result["attempts"] != 0:
                raise ValueError("commit audit result must be a fresh "
                                 "ready decision")
        elif action == "claim":
            claims[job_id] = claims.get(job_id, 0) + 1
            if result["state"] != "claimed" \
                    or result["owner"] != request["owner"] \
                    or result["lease_end"] != request["at"] + request["lease"] \
                    or result["attempts"] < 1:
                raise ValueError("claim audit result does not match its "
                                 "request")
        elif action == "finish":
            if result["state"] != request["result"]:
                raise ValueError("finish audit result does not match its "
                                 "request")
        else:  # recover
            if result["state"] != "ready":
                raise ValueError("recover audit result must be a ready "
                                 "decision")
    if set(commits) != set(decisions):
        raise ValueError("every decision must be bound to a commit "
                         "request")
    # Claims are the only calls that count attempts.
    for job_id, decision in decisions.items():
        if decision["attempts"] != claims.get(job_id, 0):
            raise ValueError("decision attempts do not match the claim "
                             "history")

    return decisions, idempotency, events


def _canonical_bytes(
    decisions: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, Any]],
    events: dict[str, dict[str, Any]],
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, sections in their
    # fixed field order and each section's primary keys in code-point
    # order, terminated by exactly one newline.
    payload = {
        "version": _VERSION,
        "decisions": {job_id: decisions[job_id]
                      for job_id in sorted(decisions)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
        "audit": {key: events[key] for key in sorted(events)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_ledger(
    realpath: str,
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
            f"dispatch ledger {realpath!r} is not valid UTF-8") from exc
    try:
        # Negative-zero and non-finite literals are format errors.
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"dispatch ledger {realpath!r} is not valid JSON") from exc
    decisions, idempotency, events = _validate_ledger(data)
    # As for the other registries, the ledger is accepted only in
    # canonical compact form with a single trailing newline.
    if raw != _canonical_bytes(decisions, idempotency, events):
        raise ValueError(
            f"dispatch ledger {realpath!r} is not in canonical compact "
            "form")
    return decisions, idempotency, events, raw


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
                dir=directory, prefix=".dispatch-restore-", suffix=".tmp")
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
    # One durable commit for the decision, the idempotency binding and
    # the audit event: synced same-directory temporary, atomic replace
    # and a directory fsync, restoring the pre-call bytes on any
    # failure.
    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".dispatch-", suffix=".tmp")
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
             lease: int | None = None, result: str | None = None
             ) -> dict[str, Any]:
    request: dict[str, Any] = {"action": action, "job_id": job_id}
    if action in ("claim", "finish"):
        request["owner"] = owner
    if action == "claim":
        request["lease"] = lease
    if action == "finish":
        request["result"] = result
    request["at"] = at
    return request


def _load_existing_ledger(
    realpath: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]], bytes]:
    decisions, idempotency, events, raw = _load_ledger(realpath)
    if raw is None:
        # Only the first commit creates the dispatch ledger; every other
        # action fails directly when it is missing.
        raise FileNotFoundError(
            f"dispatch ledger {realpath!r} does not exist")
    return decisions, idempotency, events, raw


def _replay(
    decisions: dict[str, dict[str, Any]],
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
    # An equivalent replay returns the current record without writing.
    return copy.deepcopy(decisions[request["job_id"]]), False


def commit(
    jobs: str,
    supply: str,
    trades: str,
    ledger: str,
    job_id: str,
    key: str,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Create the dispatch decision for one traded job idempotently.

    ``jobs``, ``supply``, ``trades`` and ``ledger`` paths, ``job_id``
    and ``key`` must be non-empty strings and ``at`` a non-boolean
    non-negative integer evaluation moment; the four paths must also
    resolve to distinct real locations. Any violation raises
    ``ValueError`` before a business file is read.

    The ``jobs.submit`` acceptance file, the resource supply file and
    the ``market.clear`` clearing ledger are read as one snapshot under
    their shared locks together with the dispatch ledger's exclusive
    lock, all four taken in resolved real-path order so concurrent
    calls can never deadlock. The decision binds the job id, the
    commit moment, the traded resource id and version and the job's
    deadline; it starts in state ``ready`` with zero attempts and no
    owner or lease. A commit moment later than the job's deadline
    raises ``TimeoutError``.

    Returns ``(decision, created)``. A missing dispatch ledger is
    created only by this call, the decision, its idempotency binding
    and the audit event -- the complete request plus the committed
    decision snapshot -- in one synced atomic write. Replaying the same
    key with the same job and moment returns the current decision with
    ``False`` without rewriting; the same key with a different action,
    job or moment, or a job already committed under another key, raises
    ``ValueError`` with the ledger untouched.

    An unknown job raises ``KeyError`` and a job without a recorded
    trade raises ``LookupError``; neither creates the ledger. Missing
    input files or a missing ledger parent raise
    ``FileNotFoundError``; invalid structure, values, ordering,
    references or canonical bytes raise ``ValueError``; other locking,
    read/write or sync failures raise ``OSError``.
    """
    for value in (jobs, supply, trades, ledger, job_id, key):
        if not isinstance(value, str) or not value:
            raise ValueError("jobs, supply, trades, ledger, job_id and key "
                             "must be non-empty strings")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    job_real = os.path.realpath(jobs)
    supply_real = os.path.realpath(supply)
    trades_real = os.path.realpath(trades)
    ledger_real = os.path.realpath(ledger)
    if len({job_real, supply_real, trades_real, ledger_real}) != 4:
        raise ValueError("jobs, supply, trades and ledger paths must be "
                         "distinct real paths")

    store = _get_store(ledger)
    with store.lock:
        # Locks are taken in one resolved-real-path order shared by
        # every caller, so concurrent commits can never deadlock; the
        # dispatch ledger lock is exclusive, the input snapshots shared.
        with contextlib.ExitStack() as stack:
            for locked in sorted({job_real, supply_real, trades_real,
                                  ledger_real}):
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
            decisions, idempotency, events, old_bytes = _load_ledger(
                ledger_real)

            request = _request("commit", job_id, at)
            replayed = _replay(decisions, idempotency, key, request)
            if replayed is not None:
                return replayed

            job = accepted.get(job_id)
            if job is None:
                raise KeyError(job_id)
            if job_id in decisions:
                raise ValueError("job is already committed under another "
                                 "idempotency key")
            trade = cleared.get(job_id)
            if trade is None:
                raise LookupError("job has no recorded trade")
            deadline = job["deadline"]
            if at > deadline:
                raise TimeoutError("commit moment exceeds the job "
                                   "deadline")

            decision: dict[str, Any] = {
                "job_id": job_id,
                "at": at,
                "resource_id": trade["resource_id"],
                "version": trade["version"],
                "deadline": deadline,
                "state": "ready",
                "attempts": 0,
                "owner": None,
                "lease_end": None,
            }
            decisions[job_id] = decision
            idempotency[key] = request
            events[key] = {"key": key, "request": request,
                           "result": copy.deepcopy(decision)}
            _commit_file(ledger_real,
                         _canonical_bytes(decisions, idempotency, events),
                         old_bytes)
            return copy.deepcopy(decision), True


def claim(
    ledger: str,
    job_id: str,
    key: str,
    owner: str,
    lease: int,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Claim a ready or failed decision for one owner idempotently.

    ``ledger``, ``job_id``, ``key`` and ``owner`` must be non-empty
    strings, ``lease`` a non-boolean positive integer and ``at`` a
    non-boolean non-negative integer current moment, else
    ``ValueError``. Only the dispatch ledger is touched, under its
    exclusive lock.

    A successful claim moves the decision to ``claimed``, counts one
    more attempt and records the owner with a lease ending at
    ``at + lease``; a lease end later than the job's deadline raises
    ``TimeoutError``. A decision still claimed under a valid lease
    raises ``TimeoutError`` as well, one whose lease has expired must
    be recovered first and a finished one cannot be claimed -- both
    raise ``ValueError``. Returns ``(decision, created)``; replaying
    the same key with the same job, owner, lease and moment returns the
    current decision with ``False`` without writing, while the same key
    with a different action, job, owner, moment or lease raises
    ``ValueError``.

    An unknown job raises ``KeyError``; a missing dispatch ledger
    raises ``FileNotFoundError``; invalid ledger structure, values,
    ordering, references or canonical bytes raise ``ValueError``; other
    locking, read/write or sync failures raise ``OSError``.
    """
    for value in (ledger, job_id, key, owner):
        if not isinstance(value, str) or not value:
            raise ValueError("ledger, job_id, key and owner must be "
                             "non-empty strings")
    if not _is_plain_int(lease) or lease <= 0:
        raise ValueError("lease must be a non-boolean positive integer")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    store = _get_store(ledger)
    with store.lock:
        with _lock(store.realpath):
            decisions, idempotency, events, old_bytes = \
                _load_existing_ledger(store.realpath)

            request = _request("claim", job_id, at, owner=owner,
                               lease=lease)
            replayed = _replay(decisions, idempotency, key, request)
            if replayed is not None:
                return replayed

            decision = decisions.get(job_id)
            if decision is None:
                raise KeyError(job_id)
            state = decision["state"]
            if state == "claimed":
                if at <= decision["lease_end"]:
                    raise TimeoutError("decision is claimed under a "
                                       "valid lease")
                raise ValueError("decision is claimed under an expired "
                                 "lease and must be recovered first")
            if state not in ("ready", "failed"):
                raise ValueError("decision state does not accept a claim")
            lease_end = at + lease
            if lease_end > decision["deadline"]:
                raise TimeoutError("lease end exceeds the job deadline")

            decision["state"] = "claimed"
            decision["attempts"] += 1
            decision["owner"] = owner
            decision["lease_end"] = lease_end
            idempotency[key] = request
            events[key] = {"key": key, "request": request,
                           "result": copy.deepcopy(decision)}
            _commit_file(store.realpath,
                         _canonical_bytes(decisions, idempotency, events),
                         old_bytes)
            return copy.deepcopy(decision), True


def finish(
    ledger: str,
    job_id: str,
    key: str,
    owner: str,
    result: str,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Finish a claimed decision with ``succeeded`` or ``failed``.

    ``ledger``, ``job_id``, ``key`` and ``owner`` must be non-empty
    strings, ``result`` exactly ``succeeded`` or ``failed`` and ``at``
    a non-boolean non-negative integer current moment, else
    ``ValueError``. Only the dispatch ledger is touched, under its
    exclusive lock.

    Only the current owner acting inside its lease may finish: a
    decision that is not claimed raises ``ValueError``, a different
    owner raises ``PermissionError`` and a moment later than the lease
    end raises ``TimeoutError``. A successful finish records the result
    as the new state and clears the owner and the lease; the attempt
    count is unchanged. Returns ``(decision, created)``; replaying the
    same key with the same job, owner, result and moment returns the
    current decision with ``False`` without writing, while the same key
    with a different action, job, owner, result or moment raises
    ``ValueError``.

    An unknown job raises ``KeyError``; a missing dispatch ledger
    raises ``FileNotFoundError``; invalid ledger structure, values,
    ordering, references or canonical bytes raise ``ValueError``; other
    locking, read/write or sync failures raise ``OSError``.
    """
    for value in (ledger, job_id, key, owner):
        if not isinstance(value, str) or not value:
            raise ValueError("ledger, job_id, key and owner must be "
                             "non-empty strings")
    if result not in _RESULTS:
        raise ValueError("result must be succeeded or failed")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    store = _get_store(ledger)
    with store.lock:
        with _lock(store.realpath):
            decisions, idempotency, events, old_bytes = \
                _load_existing_ledger(store.realpath)

            request = _request("finish", job_id, at, owner=owner,
                               result=result)
            replayed = _replay(decisions, idempotency, key, request)
            if replayed is not None:
                return replayed

            decision = decisions.get(job_id)
            if decision is None:
                raise KeyError(job_id)
            if decision["state"] != "claimed":
                raise ValueError("decision is not claimed")
            if decision["owner"] != owner:
                raise PermissionError("finish requires the current owner")
            if at > decision["lease_end"]:
                raise TimeoutError("the lease has already expired")

            decision["state"] = result
            decision["owner"] = None
            decision["lease_end"] = None
            idempotency[key] = request
            events[key] = {"key": key, "request": request,
                           "result": copy.deepcopy(decision)}
            _commit_file(store.realpath,
                         _canonical_bytes(decisions, idempotency, events),
                         old_bytes)
            return copy.deepcopy(decision), True


def recover(
    ledger: str,
    job_id: str,
    key: str,
    at: int,
) -> tuple[dict[str, object], bool]:
    """Recover a claimed decision whose lease has strictly expired.

    ``ledger``, ``job_id`` and ``key`` must be non-empty strings and
    ``at`` a non-boolean non-negative integer current moment, else
    ``ValueError``. Only the dispatch ledger is touched, under its
    exclusive lock.

    A decision that is not claimed raises ``ValueError``; one whose
    lease has not strictly expired yet raises ``PermissionError``. A
    successful recovery returns the decision to ``ready`` and clears
    the owner and the lease without counting an attempt. Returns
    ``(decision, created)``; replaying the same key with the same job
    and moment returns the current decision with ``False`` without
    writing, while the same key with a different action, job or moment
    raises ``ValueError``.

    An unknown job raises ``KeyError``; a missing dispatch ledger
    raises ``FileNotFoundError``; invalid ledger structure, values,
    ordering, references or canonical bytes raise ``ValueError``; other
    locking, read/write or sync failures raise ``OSError``.
    """
    for value in (ledger, job_id, key):
        if not isinstance(value, str) or not value:
            raise ValueError("ledger, job_id and key must be non-empty "
                             "strings")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    store = _get_store(ledger)
    with store.lock:
        with _lock(store.realpath):
            decisions, idempotency, events, old_bytes = \
                _load_existing_ledger(store.realpath)

            request = _request("recover", job_id, at)
            replayed = _replay(decisions, idempotency, key, request)
            if replayed is not None:
                return replayed

            decision = decisions.get(job_id)
            if decision is None:
                raise KeyError(job_id)
            if decision["state"] != "claimed":
                raise ValueError("decision is not claimed")
            if at <= decision["lease_end"]:
                raise PermissionError("the lease has not expired yet")

            decision["state"] = "ready"
            decision["owner"] = None
            decision["lease_end"] = None
            idempotency[key] = request
            events[key] = {"key": key, "request": request,
                           "result": copy.deepcopy(decision)}
            _commit_file(store.realpath,
                         _canonical_bytes(decisions, idempotency, events),
                         old_bytes)
            return copy.deepcopy(decision), True
