"""Coordinated batch recovery of every pending migration.

Where :mod:`carbon_market.recover` finishes one job's open ``prepare``,
this module sweeps all of them in a single coordinated batch. The batch
is described by a coordination file (``coord``): it records the
idempotency key of the batch, the owner currently holding the recovery
lease and its expiry, and one ``[event, error]`` slot per job snapped
up at creation -- ``[null, null]`` while pending, ``[event, null]`` once
recovered and ``[null, error]`` when the recovery attempt raised.

The coordination file is created on demand and guarded by a per-realpath
lock that excludes both threads of this process and other processes. A
batch with pending items may only be continued by its owner; another
owner must wait out the lease (``now`` later than ``until``) before
taking over, and whoever runs refreshes the lease to ``now + ttl``. Each
pending item is recovered through :func:`carbon_market.recover.run`
under a subkey derived from the batch key, so an item interrupted by a
crash is simply re-entered: the subkey replays whatever the interrupted
attempt already recorded in the migration journal. A batch whose items
are all resolved replays its original coordination object for the same
key; a different key is a conflict. When an already terminal
coordination file is re-entered and its history file is missing or was
left without that terminal snapshot -- a crash, or the history being
lost -- the one missing terminal snapshot is backfilled into the
history, and a replay whose terminal state is already recorded writes
nothing. Encoding, atomic synchronization and
file-error behaviour follow :func:`carbon_market.recover.run`, and a
failed write never damages the previous coordination file.

Every time the coordination object is created, taken over by a new
owner, or has one of its items settle -- the last settlement being the
batch's termination -- one snapshot ``[status, coord]`` is first
appended to a history file next to it (``coord + ".history"``), inside
the same lock. The status follows the rules of
:mod:`carbon_market.recovery_audit` and the coord is an
order-preserving deep copy of the object at that moment. Each history
write -- validation, snapshot append, atomic replacement and directory
sync -- additionally runs under an exclusive lock on the history file's
own lock file (``coord + ".history.lock"``), so readers holding the
shared lock never observe a half-updated history. The history
write always precedes the coordination write, so a failed snapshot
leaves the previous coordination state untouched and unadvanced; a
re-entry recomputes whatever a crashed attempt had not yet recorded and
skips the duplicate when it recomputes an already-recorded state. The
history file shares the coordination file's version, encoding and
atomic-synchronization behaviour.
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
from . import migrate as _migrate
from . import offers as _offers
from . import recover as _recover
from .market import _load_registry
from ._jsonio import strict_loads

__all__ = ["run"]

_VERSION = 1
_ROOT_FIELDS = ("version", "key", "owner", "until", "items")
_EVENT_FIELDS = ("job_id", "source_id", "target_id", "op", "now")
_OPS = ("commit", "abort")
_HISTORY_FIELDS = ("version", "key", "snapshots")
_STATUSES = ("pending", "failed", "completed")


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


@contextlib.contextmanager
def _file_lock(realpath: str, exclusive: bool = True) -> Iterator[None]:
    # Cross-process mutual exclusion via a kernel lock: flock serializes
    # holders of the same lock file across processes -- shared holders
    # coexist with one another but never with an exclusive holder -- and
    # the kernel releases the lock automatically when the holding process
    # exits, even on a crash, so a leftover lock file never blocks anyone.
    # The file itself is deliberately never unlinked: removing it while
    # another process waits on the old inode would split the lock domain.
    # Any failure to open or lock surfaces as OSError.
    lock_path = realpath + ".lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _is_plain_int(value: object) -> bool:
    # bool is a subclass of int and must be rejected.
    return isinstance(value, int) and not isinstance(value, bool)


def _validate_event(event: object, job_id: str) -> dict[str, Any]:
    if not isinstance(event, dict) or set(event.keys()) != set(_EVENT_FIELDS):
        raise ValueError("event must be an object with exactly job_id, "
                         "source_id, target_id, op and now")
    if event["job_id"] != job_id:
        raise ValueError("event job_id must match the item it is stored for")
    for field in ("source_id", "target_id"):
        if not isinstance(event[field], str) or not event[field]:
            raise ValueError(f"{field} must be a non-empty string")
    if event["op"] not in _OPS:
        raise ValueError("op must be commit or abort")
    if not _is_plain_int(event["now"]) or event["now"] < 0:
        raise ValueError("now must be a non-boolean non-negative integer")
    return {field: event[field] for field in _EVENT_FIELDS}


def _validate_coord(data: object) -> dict[str, Any]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("coordination root must be an object with keys "
                         "version, key, owner, until and items")

    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported coordination version")

    key = data["key"]
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a non-empty string")
    owner = data["owner"]
    if not isinstance(owner, str) or not owner:
        raise ValueError("owner must be a non-empty string")
    until = data["until"]
    if not _is_plain_int(until) or until < 0:
        raise ValueError("until must be a non-boolean non-negative integer")

    items_raw = data["items"]
    if not isinstance(items_raw, dict):
        raise ValueError("items must be an object")
    items: dict[str, list[Any]] = {}
    for job_id in sorted(items_raw):
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("item keys must be non-empty strings")
        pair = items_raw[job_id]
        if not isinstance(pair, list) or len(pair) != 2:
            raise ValueError("item values must be [event, error] pairs")
        event_raw, error = pair
        if error is not None and (not isinstance(error, str) or not error):
            raise ValueError("error must be null or a non-empty string")
        event = None
        if event_raw is not None:
            if error is not None:
                raise ValueError("event and error cannot both be set")
            event = _validate_event(event_raw, job_id)
        items[job_id] = [event, error]

    return {"version": _VERSION, "key": key, "owner": owner, "until": until,
            "items": items}


def _load_coord(realpath: str) -> dict[str, Any] | None:
    # Unlike the five required inputs, the coordination file is created on
    # demand, so a missing file simply means the batch has never run.
    try:
        with open(realpath, encoding="utf-8") as handle:
            text = handle.read()
    except FileNotFoundError:
        return None

    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"coordination file {realpath!r} is not valid JSON") from exc
    return _validate_coord(data)


def _status_of(items: dict[str, list[Any]]) -> str:
    # The status rules are the ones recovery_audit.get reports: pending
    # while any item is still [None, None], failed when nothing is
    # pending but at least one error is recorded, completed otherwise.
    if any(pair[0] is None and pair[1] is None for pair in items.values()):
        return "pending"
    if any(pair[1] is not None for pair in items.values()):
        return "failed"
    return "completed"


def _validate_history(data: object) -> dict[str, Any]:
    if not isinstance(data, dict) or set(data.keys()) != set(_HISTORY_FIELDS):
        raise ValueError("history root must be an object with keys "
                         "version, key and snapshots")

    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported history version")

    key = data["key"]
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a non-empty string")

    snapshots_raw = data["snapshots"]
    if not isinstance(snapshots_raw, list):
        raise ValueError("snapshots must be an array")
    snapshots: list[list[Any]] = []
    for entry in snapshots_raw:
        if not isinstance(entry, list) or len(entry) != 2:
            raise ValueError("snapshot entries must be [status, coord] pairs")
        status, coord_raw = entry
        if status not in _STATUSES:
            raise ValueError("status must be pending, failed or completed")
        snapshots.append([status, _validate_coord(coord_raw)])

    return {"version": _VERSION, "key": key, "snapshots": snapshots}


def _load_history(realpath: str) -> dict[str, Any] | None:
    # Like the coordination file, the history file is created on demand,
    # so a missing file simply means no snapshot has been recorded yet.
    try:
        with open(realpath, encoding="utf-8") as handle:
            text = handle.read()
    except FileNotFoundError:
        return None

    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"history file {realpath!r} is not valid JSON") from exc
    return _validate_history(data)


def _atomic_write(realpath: str, coord: dict[str, Any]) -> None:
    text = json.dumps(coord, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"

    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".recover-all-",
                                    suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, realpath)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise

    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _ensure_snapshot(realpath: str, coord_obj: dict[str, Any]) -> None:
    # Record one snapshot of the coordination object as it is about to be
    # persisted. The history is always written before the coordination
    # file itself, so a failure here leaves the previous coordination
    # state untouched and the caller must not advance it. A re-entry that
    # recomputes a state a crashed attempt already recorded -- not just
    # the immediately previous one, e.g. a missing history next to a
    # terminal coordination file -- finds it among the snapshots and
    # records no duplicate.
    #
    # The whole cycle -- loading and validating the current history,
    # appending the snapshot, atomically replacing the file and syncing
    # the directory -- runs under one exclusive lock on the history
    # file's own lock file, so a concurrent reader holding the shared
    # lock (carbon_market.history.get / verify) only ever observes the
    # complete history from before the write or the one from after it.
    history_real = os.path.realpath(realpath + ".history")
    with _file_lock(history_real):
        history = _load_history(history_real)
        snapshot = [_status_of(coord_obj["items"]), copy.deepcopy(coord_obj)]
        if history is None:
            history = {"version": _VERSION, "key": coord_obj["key"],
                       "snapshots": []}
        elif history["key"] != coord_obj["key"]:
            raise ValueError("coordination key was already used with a "
                             "different recovery batch")
        if snapshot in history["snapshots"]:
            return
        history["snapshots"].append(snapshot)
        _atomic_write(history_real, history)


def run(
    jobs: str,
    offers: str,
    matches: str,
    reserves: str,
    state: str,
    coord: str,
    owner: str,
    key: str,
    now: int,
    ttl: int,
) -> tuple[dict[str, Any], bool]:
    """Recover every pending migration as one coordinated batch.

    Returns ``(coord, created)`` where ``coord`` is the coordination
    object itself -- keys version, key, owner, until and items, with
    items mapping each job_id (in code-point order) to its
    ``[event, error]`` outcome -- and ``created`` is ``True`` only when
    this call created the coordination file. A later call with the same
    key and no pending items replays the original object without
    renewing the lease; when the history file is missing or has not
    recorded that terminal state, the one missing terminal snapshot is
    backfilled into it, and a re-entry that finds it already recorded
    writes nothing. A different key raises ``ValueError``. While items
    are pending only the recorded owner may continue; another owner
    must wait until ``now`` is past ``until`` to take over, otherwise
    ``PermissionError`` is raised, and the lease is refreshed to
    ``now + ttl``. Creation, each takeover and each item settlement
    first append one ``[status, coord]`` snapshot to the history file
    ``coord + ".history"``; a failed snapshot leaves the previous
    coordination state unadvanced, and no path ever appends the same
    snapshot twice.
    """
    for value in (jobs, offers, matches, reserves, state, coord, owner, key):
        if not isinstance(value, str) or not value:
            raise ValueError("string arguments must be non-empty strings")
    if not _is_plain_int(now) or now < 0:
        raise ValueError("now must be a non-boolean non-negative integer")
    if not _is_plain_int(ttl) or ttl < 1:
        raise ValueError("ttl must be a non-boolean positive integer")

    store = _get_store(coord)
    with store.lock:
        with _file_lock(store.realpath):
            jobs_real = os.path.realpath(jobs)
            offers_real = os.path.realpath(offers)
            matches_real = os.path.realpath(matches)
            reserves_real = os.path.realpath(reserves)
            state_real = os.path.realpath(state)

            job_records, _ = _load_registry(
                jobs_real, _jobs._validate_structure, "jobs registry")
            offer_records, _ = _load_registry(
                offers_real, _offers._validate_structure, "offers registry")
            match_records = _migrate._load_matches(
                matches_real, job_records, offer_records)
            _migrate._load_reserves(
                reserves_real, job_records, offer_records, match_records)
            _events, lifecycle = _recover._load_state(
                state_real, job_records, offer_records, match_records)

            # Snapshot the jobs whose prepare is still open, in job_id
            # code-point order; this snapshot defines the batch at creation.
            snapshot = sorted(job_id for job_id, status in lifecycle.items()
                              if status == "prepared")

            coord_obj = _load_coord(store.realpath)
            created = False
            if coord_obj is None:
                coord_obj = {
                    "version": _VERSION,
                    "key": key,
                    "owner": owner,
                    "until": now + ttl,
                    "items": {job_id: [None, None] for job_id in snapshot},
                }
                created = True
                _ensure_snapshot(store.realpath, coord_obj)
                _atomic_write(store.realpath, coord_obj)
            else:
                if coord_obj["key"] != key:
                    raise ValueError("coordination key was already used with "
                                     "a different recovery batch")
                if not any(pair[0] is None and pair[1] is None
                           for pair in coord_obj["items"].values()):
                    # The batch is terminal. Re-entry must not renew the
                    # lease, settle anything or replay a duplicate; but a
                    # history missing the terminal snapshot (lost file, or
                    # a crash after the final coordination write) is healed
                    # with the one missing terminal snapshot. When it is
                    # already recorded the history write is skipped too.
                    _ensure_snapshot(store.realpath, coord_obj)
                    return coord_obj, False
                if coord_obj["owner"] != owner and now <= coord_obj["until"]:
                    raise PermissionError(
                        "recovery batch is owned by another owner until "
                        f"{coord_obj['until']}")
                takeover = coord_obj["owner"] != owner
                coord_obj["owner"] = owner
                coord_obj["until"] = now + ttl
                if takeover:
                    _ensure_snapshot(store.realpath, coord_obj)
                _atomic_write(store.realpath, coord_obj)

            for job_id, pair in coord_obj["items"].items():
                if pair[0] is not None or pair[1] is not None:
                    continue
                subkey = f"{len(key)}:{key}{job_id}"
                try:
                    event, _ = _recover.run(
                        jobs, offers, matches, reserves, state,
                        job_id, subkey, now)
                    pair[0] = event
                except Exception as exc:  # noqa: BLE001 - record and continue
                    pair[1] = type(exc).__name__
                _ensure_snapshot(store.realpath, coord_obj)
                _atomic_write(store.realpath, coord_obj)
            return coord_obj, created
