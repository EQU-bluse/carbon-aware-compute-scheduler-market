"""Idempotent audit journal of copy/restore operations.

Every event is recorded under a caller-chosen idempotency key in one
JSON audit file (``path``): a replay of a key already present returns
the stored event without writing, a different event for the same key is
a conflict, and a new key appends. The file is a JSON object with keys
``version`` and ``events``, in that order: ``version`` is ``1`` and
``events`` maps each key -- in key code-point order -- to the one event
currently recorded for it. Each event carries exactly the keys ``op``,
``target``, ``key``, ``changed``, ``error`` and ``stage``, in that
order: ``op`` is ``"copy"`` or ``"restore"``, ``target`` and ``key``
are non-empty strings and ``changed`` is a boolean; a successful event
carries null ``error`` and ``stage``, while a failed one carries the
exception class name in ``error`` and one of ``"validate"``,
``"execute"``, ``"sync"`` or ``"rollback"`` in ``stage``.

The whole validate/replay-or-append/commit sequence of :func:`record`
runs under one exclusive hold on a per-realpath lock -- an in-process
lock plus a kernel flock on ``path + ".lock"`` -- while :func:`get`
takes the same flock shared only while opening and reading the file, so
a read racing a record observes either the complete pre-update file or
the complete post-update one. The kernel releases the flock
automatically when a process exits, so a leftover lock file never
blocks a later call.

A record commits as the other registries do: the whole new document is
written to a temporary file in the audit file's own directory, flushed
and fsynced, moved over the audit file with :func:`os.replace` and
followed by a directory fsync; output is UTF-8 compact JSON with
non-ASCII characters written directly and a single trailing newline.
The audit file is created on demand. A failure from the commit -- a
write, replace or directory-sync error, including the test-only
``fault="replace"`` injection, which raises ``OSError`` immediately
after the replace -- restores, still under the exclusive lock, the
exact bytes the file held when the call began, or removes the file when
it did not exist, and syncs that restoration to disk; should the
rollback itself fail, its ``OSError`` is raised with the first error
chained as its ``__cause__``. Invalid input and malformed audit content
raise ``ValueError``; a missing file is ``FileNotFoundError`` for
:func:`get` (and a missing parent directory likewise for
:func:`record`), and every other locking or I/O failure is an
``OSError``.
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

from ._jsonio import strict_loads

__all__ = ["record", "get"]

_VERSION = 1
_ROOT_FIELDS = ("version", "events")
_EVENT_FIELDS = ("op", "target", "key", "changed", "error", "stage")
_OPS = ("copy", "restore")
_STAGES = ("validate", "execute", "sync", "rollback")


class _Store:
    def __init__(self, realpath: str) -> None:
        self.realpath = realpath
        self.lock = threading.Lock()


_stores_lock = threading.Lock()
_stores: dict[str, _Store] = {}


def _get_store(realpath: str) -> _Store:
    with _stores_lock:
        store = _stores.get(realpath)
        if store is None:
            store = _Store(realpath)
            _stores[realpath] = store
        return store


@contextlib.contextmanager
def _file_lock(realpath: str, *, shared: bool = False) -> Iterator[None]:
    # Cross-process mutual exclusion via a kernel flock on a companion
    # lock file (``path + ".lock"``): flock serializes holders of the same
    # lock file across processes, and the kernel releases it automatically
    # when the holding process exits -- even on a crash -- so a leftover
    # lock file never blocks a later call. The file itself is deliberately
    # never unlinked: removing it while another process waited on the old
    # inode would split the lock domain. Any failure to open or lock
    # surfaces as OSError.
    lock_path = realpath + ".lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _is_plain_int(value: object) -> bool:
    # bool is a subclass of int and must be rejected.
    return isinstance(value, int) and not isinstance(value, bool)


def _validate_event(event: object) -> dict[str, Any]:
    if not isinstance(event, dict) or set(event.keys()) != set(_EVENT_FIELDS):
        raise ValueError("event must be an object with exactly op, target, "
                         "key, changed, error and stage")
    if event["op"] not in _OPS:
        raise ValueError("event op must be copy or restore")
    for field in ("target", "key"):
        if not isinstance(event[field], str) or not event[field]:
            raise ValueError(f"event {field} must be a non-empty string")
    if not isinstance(event["changed"], bool):
        raise ValueError("event changed must be a boolean")
    stage = event["stage"]
    error = event["error"]
    if stage is None:
        if error is not None:
            raise ValueError("a successful event must carry null error "
                             "and stage")
    else:
        if stage not in _STAGES:
            raise ValueError("event stage must be validate, execute, sync "
                             "or rollback when the event failed")
        if not isinstance(error, str) or not error:
            raise ValueError("a failed event must carry the exception "
                             "class name as a non-empty error")
    return {field: event[field] for field in _EVENT_FIELDS}


def _validate_document(data: object) -> dict[str, Any]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("audit root must be an object with keys version "
                         "and events")
    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported audit version")
    raw_events = data["events"]
    if not isinstance(raw_events, dict):
        raise ValueError("audit events must be an object")
    if list(raw_events) != sorted(raw_events):
        raise ValueError("audit events must be ordered by key code point")
    events: dict[str, dict[str, Any]] = {}
    for name, event in raw_events.items():
        if not isinstance(name, str) or not name:
            raise ValueError("audit keys must be non-empty strings")
        events[name] = _validate_event(event)
    return {"version": _VERSION, "events": events}


def _read_bytes_locked(realpath: str) -> bytes | None:
    # None records that the file did not exist; the bytes are read in full
    # while the caller holds the companion lock, so they are one complete
    # pre- or post-update document.
    try:
        with open(realpath, "rb") as handle:
            return handle.read()
    except FileNotFoundError:
        return None


def _parse(raw: bytes, realpath: str) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"audit file {realpath!r} is not valid UTF-8") from exc
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"audit file {realpath!r} is not valid JSON") from exc
    return _validate_document(data)


def _serialize(events: dict[str, dict[str, Any]]) -> bytes:
    ordered = {"version": _VERSION,
               "events": {name: events[name] for name in sorted(events)}}
    text = json.dumps(ordered, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _rollback(realpath: str, directory: str, existed: bool,
              old_bytes: bytes | None, first: BaseException) -> None:
    # Under the still-held exclusive lock, put the audit file back exactly
    # as it was when the record began (its previous bytes, or its
    # nonexistence) and fsync the directory so the restoration is durable.
    # Any failure of this recovery is raised chained after the first error.
    try:
        if existed:
            fd, tmp_path = tempfile.mkstemp(
                dir=directory, prefix=".audit-rollback-", suffix=".tmp")
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
        else:
            try:
                os.unlink(realpath)
            except FileNotFoundError:
                pass
        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError as recovery:
        raise recovery from first


def _commit(realpath: str, directory: str, payload: bytes, *,
            existed: bool, old_bytes: bytes | None, inject_fault: bool
            ) -> None:
    # Same-directory temporary -> fsync -> atomic replace -> directory
    # fsync. Any failure after writing began -- including the injected
    # post-replace fault -- restores the call-before bytes or
    # nonexistence before the exclusive lock is released; a rollback that
    # itself fails is raised chained after the first error.
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".audit-", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, realpath)
        if inject_fault:
            raise OSError(5, "injected failure after replace")
        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except BaseException as first:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        _rollback(realpath, directory, existed, old_bytes, first)
        raise


def record(
    path: str,
    key: str,
    event: dict[str, Any],
    fault: str | None = None,
) -> tuple[dict[str, Any], bool]:
    """Record ``event`` under idempotency ``key`` in the audit at ``path``.

    ``path`` and ``key`` must be non-empty strings and ``event`` an
    object carrying exactly op, target, key, changed, error and stage:
    op is ``"copy"`` or ``"restore"``, target and key are non-empty
    strings, changed is a boolean, and error/stage are both null on
    success while on failure error is the exception class name and
    stage is ``"validate"``, ``"execute"``, ``"sync"`` or
    ``"rollback"``; anything else raises ``ValueError``. ``fault`` is a
    test hook: ``None`` runs normally, ``"replace"`` raises
    ``OSError`` right after the audit file is atomically replaced
    (rolling the commit back to the call-before bytes), and every other
    non-null value is ``ValueError``.

    Returns ``(event_copy, created)``: a replay of a key whose event is
    already recorded returns a copy of the stored event and ``False``
    without writing; a different event for the same key raises
    ``ValueError`` as a conflict; a new key returns a copy of the
    recorded event and ``True``. The audit file is created on demand
    with top-level keys version and events, events in key code-point
    order, as UTF-8 compact JSON with non-ASCII characters written
    directly and one trailing newline. A missing parent directory
    raises ``FileNotFoundError``; malformed existing audit content
    raises ``ValueError``; every other locking or I/O failure raises
    ``OSError``, and a failed commit restores the file's previous bytes
    (or nonexistence), with a rollback failure chained after the first
    error.
    """
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a non-empty string")
    event_copy = _validate_event(event)
    if fault is not None and fault != "replace":
        raise ValueError("fault must be None or 'replace'")

    realpath = os.path.realpath(path)
    directory = os.path.dirname(realpath) or "."
    store = _get_store(realpath)

    # The read/decide/commit sequence is one critical section: the
    # in-process lock serializes threads of this process and the
    # exclusive flock serializes every other process, so a replay
    # decision and the append can never race another writer, and a
    # shared-lock get only ever sees the whole pre- or post-update file.
    with store.lock:
        with _file_lock(realpath):
            raw = _read_bytes_locked(realpath)
            existed = raw is not None
            document = _parse(raw, realpath) if existed else None
            events = document["events"] if document is not None else {}

            if key in events:
                if events[key] == event_copy:
                    return copy.deepcopy(event_copy), False
                raise ValueError(
                    f"a different audit event is already recorded for {key!r}")

            events[key] = event_copy
            _commit(realpath, directory, _serialize(events),
                    existed=existed, old_bytes=raw,
                    inject_fault=fault == "replace")
            return copy.deepcopy(event_copy), True


def get(path: str, key: str) -> dict[str, Any]:
    """Return a copy of the event recorded for ``key`` in the audit at ``path``.

    ``path`` and ``key`` must be non-empty strings, else ``ValueError``.
    The audit file is read under a shared flock on ``path + ".lock"``
    while it is opened and read, so a racing :func:`record` can only
    expose the complete pre- or post-update document; parsing and
    validation run after the hold is released. A missing audit file
    raises ``FileNotFoundError`` and an unknown key raises
    ``KeyError(key)``; malformed UTF-8 or JSON, a negative-zero literal,
    or an unsupported version or structure raises ``ValueError``, and
    any other locking or I/O failure raises ``OSError``. The query
    never writes, and the returned dict carries keys op, target, key,
    changed, error and stage in that order.
    """
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a non-empty string")

    realpath = os.path.realpath(path)
    with _file_lock(realpath, shared=True):
        raw = _read_bytes_locked(realpath)

    if raw is None:
        raise FileNotFoundError(2, os.strerror(2), realpath)
    document = _parse(raw, realpath)
    events = document["events"]
    if key not in events:
        raise KeyError(key)
    return copy.deepcopy(events[key])
