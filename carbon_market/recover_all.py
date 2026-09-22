"""Coordinated batch recovery of every pending migration.

Where :mod:`carbon_market.recover` finishes one job's open ``prepare``,
this module sweeps all of them in a single run. The set of jobs to
recover is snapshot from the migration journal -- every prepare without
a terminal commit/abort, ordered by job_id code points -- and recorded
in a coordination file so an interrupted run can be re-entered: each
item starts out pending (``[null, null]``) and ends up carrying either
the recovered event (``[event, null]``) or the exception class name
(``[null, error]``). Items still pending when a run dies are simply
retried by the next one; the per-job idempotency subkey
``f"{len(key)}:{key}{job_id}"`` makes such a retry replay the event the
crashed run may already have recorded.

The coordination file is created on first use and leased to an owner
until ``now + ttl``. While items remain pending only the same owner may
continue the run; a different owner must wait until the lease has
expired (``now > until``) to take over, and gets ``PermissionError``
otherwise. Once no pending items remain the run is complete: the same
idempotency key replays the recorded object, a different key is a
``ValueError``.

The five market inputs are read under exactly the same contract as
:func:`carbon_market.recover.run` -- required files surface their
OSError, malformed JSON is a ValueError -- and the coordination file
itself is written atomically (temp file, fsync, rename), so a failed
write never damages the previous file. Mutual exclusion is by the
coordination file's realpath, both within this process and across
processes via an advisory file lock.
"""

from __future__ import annotations

import contextlib
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


class _CoordStore:
    def __init__(self, realpath: str) -> None:
        self.realpath = realpath
        self.lock = threading.Lock()


_stores_lock = threading.Lock()
_stores: dict[str, _CoordStore] = {}


def _get_store(path: str) -> _CoordStore:
    realpath = os.path.realpath(path)
    with _stores_lock:
        store = _stores.get(realpath)
        if store is None:
            store = _CoordStore(realpath)
            _stores[realpath] = store
        return store


@contextlib.contextmanager
def _process_lock(realpath: str) -> Iterator[None]:
    # The coordination file itself is replaced atomically on every write,
    # so its inode cannot anchor a lock; a stable sidecar file does.
    fd = os.open(realpath + ".lock", os.O_RDWR | os.O_CREAT, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _is_plain_int(value: object) -> bool:
    # bool is a subclass of int and must be rejected.
    return isinstance(value, int) and not isinstance(value, bool)


def _validate_event(event: object, job_id: str) -> None:
    if not isinstance(event, dict) or set(event.keys()) != set(_EVENT_FIELDS):
        raise ValueError("item event must be an object with exactly job_id, "
                         "source_id, target_id, op and now")
    if event["job_id"] != job_id:
        raise ValueError("item event must reference its item job_id")
    if not isinstance(event["source_id"], str) or not event["source_id"]:
        raise ValueError("source_id must be a non-empty string")
    if not isinstance(event["target_id"], str) or not event["target_id"]:
        raise ValueError("target_id must be a non-empty string")
    if event["op"] not in _migrate._OPS:
        raise ValueError("op must be prepare, commit or abort")
    if not _is_plain_int(event["now"]) or event["now"] < 0:
        raise ValueError("now must be a non-boolean non-negative integer")


def _validate_coord(data: object) -> dict[str, Any]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("coordination root must be an object with keys "
                         "version, key, owner, until and items")

    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported coordination version")

    key = data["key"]
    if not isinstance(key, str) or not key:
        raise ValueError("coordination key must be a non-empty string")

    owner = data["owner"]
    if not isinstance(owner, str) or not owner:
        raise ValueError("coordination owner must be a non-empty string")

    until = data["until"]
    if not _is_plain_int(until) or until < 0:
        raise ValueError("until must be a non-boolean non-negative integer")

    items_raw = data["items"]
    if not isinstance(items_raw, dict):
        raise ValueError("items must be an object")

    items: dict[str, list[object]] = {}
    for job_id in sorted(items_raw):
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("item keys must be non-empty strings")
        entry = items_raw[job_id]
        if not isinstance(entry, list) or len(entry) != 2:
            raise ValueError("item must be a two-element [event, error] list")
        event, error = entry
        if event is None:
            if error is not None and (
                    not isinstance(error, str) or not error):
                raise ValueError("item error must be a non-empty string")
        else:
            if error is not None:
                raise ValueError("item cannot carry both an event and an "
                                 "error")
            _validate_event(event, job_id)
        items[job_id] = [event, error]

    return {"version": _VERSION, "key": key, "owner": owner, "until": until,
            "items": items}


def _load_coord(realpath: str) -> dict[str, Any] | None:
    # Unlike the five market inputs the coordination file is created on
    # demand, so a missing file simply means this is the first run.
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


def _run_locked(
    coord_real: str,
    jobs: str,
    offers: str,
    matches: str,
    reserves: str,
    state: str,
    owner: str,
    key: str,
    now: int,
    ttl: int,
) -> tuple[dict[str, Any], bool]:
    # The five market inputs follow recover.run's contract: every file is
    # required, malformed JSON is a ValueError, and the journal is
    # validated against the registries.
    job_records, _ = _load_registry(
        os.path.realpath(jobs), _jobs._validate_structure, "jobs registry")
    offer_records, _ = _load_registry(
        os.path.realpath(offers), _offers._validate_structure,
        "offers registry")
    match_records = _migrate._load_matches(
        os.path.realpath(matches), job_records, offer_records)
    _reserve_events, _reserve_lifecycle = _migrate._load_reserves(
        os.path.realpath(reserves), job_records, offer_records,
        match_records)
    _events, lifecycle = _recover._load_state(
        os.path.realpath(state), job_records, offer_records, match_records)

    # Snapshot the unfinished prepares in job_id code point order.
    snapshot = sorted(job_id for job_id, status in lifecycle.items()
                      if status == "prepared")

    coord = _load_coord(coord_real)
    created = coord is None
    if created:
        coord = {
            "version": _VERSION,
            "key": key,
            "owner": owner,
            "until": now + ttl,
            "items": {job_id: [None, None] for job_id in snapshot},
        }
        # Persist the lease and the pending items before touching any
        # migration, so a crash leaves a re-enterable coordination file.
        _atomic_write(coord_real, coord)
    else:
        items = coord["items"]
        if not any(entry == [None, None] for entry in items.values()):
            # The run is complete: the same key replays the recorded
            # object, a different key is a misuse.
            if coord["key"] != key:
                raise ValueError("coordination file was created with a "
                                 "different key")
            return coord, False
        if coord["owner"] != owner:
            if now <= coord["until"]:
                raise PermissionError(
                    f"recovery is owned by {coord['owner']!r} until "
                    f"{coord['until']}")
        coord["owner"] = owner
        coord["until"] = now + ttl

    items = coord["items"]
    pending = [job_id for job_id in sorted(items)
               if items[job_id] == [None, None]]
    for job_id in pending:
        subkey = f"{len(key)}:{key}{job_id}"
        try:
            event, _ = _recover.run(jobs, offers, matches, reserves, state,
                                    job_id, subkey, now)
        except Exception as exc:
            # A failed item is terminal for this run but never blocks the
            # remaining ones; only a crash leaves an item re-enterable.
            items[job_id] = [None, type(exc).__name__]
        else:
            items[job_id] = [event, None]
    if pending:
        _atomic_write(coord_real, coord)
    return coord, created


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
) -> tuple[dict[str, object], bool]:
    """Recover every pending migration under a leased coordination file.

    Returns ``(coord, created)`` where ``coord`` is the coordination
    object with keys version, key, owner, until and items -- items maps
    each snapshotted job_id, in order, to ``[event, null]`` on success,
    ``[null, error]`` with the exception class name on failure, and
    ``[null, null]`` while still pending. ``created`` is ``True`` only
    when this call created the coordination file; a completed run
    replays its recorded object with ``False``.
    """
    for value in (jobs, offers, matches, reserves, state, coord, owner, key):
        if not isinstance(value, str) or not value:
            raise ValueError("string arguments must be non-empty strings")
    if not _is_plain_int(now) or now < 0:
        raise ValueError("now must be a non-boolean non-negative integer")
    if not _is_plain_int(ttl) or ttl < 1:
        raise ValueError("ttl must be a non-boolean positive integer")

    store = _get_store(coord)
    with store.lock, _process_lock(store.realpath):
        return _run_locked(store.realpath, jobs, offers, matches, reserves,
                           state, owner, key, now, ttl)
