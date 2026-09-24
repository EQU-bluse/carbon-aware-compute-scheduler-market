"""Idempotent audit journal of copy/restore operations.

Each journal file records the outcomes of copy and restore operations
under caller-chosen idempotency keys. A document has the form
``{"version": 1, "events": {key: event}}`` with the events ordered by key
code point; every event carries exactly op, target, key, changed, error
and stage, in that order. ``op`` is ``"copy"`` or ``"restore"`` and
``changed`` is a boolean; a successful event has ``error`` and ``stage``
both null, while a failed one names the exception class in ``error`` and
the stage it failed at in ``stage`` -- one of ``"校验"``, ``"执行"``,
``"同步"`` and ``"回滚"``.

Ordering is part of the on-disk contract, not just the serialization:
when an existing journal is read (by :func:`record` before appending or
by the strictly read-only :func:`get`), the two root fields must appear
as version then events, the events member must list its keys in code
point order, and each event must list its six fields in their canonical
order. Any disorder is a malformed document and raises ``ValueError``;
the reader never sorts an out-of-order document into acceptance, and the
read-only query never rewrites the file to repair it.

:func:`record` replays the event already stored under a key: the same
key with the same event returns a copy of it together with ``False`` and
writes nothing, while the same key with a different event is a conflict
and raises ``ValueError``. A new key appends its event, creates the file
when missing, and returns the event copy together with ``True``.
:func:`get` is strictly read-only and returns a copy of the event stored
under a key; an unknown key raises ``KeyError(key)``.

Both functions resolve the journal to its real path and guard the file
with a per-realpath lock that excludes both threads of this process and
other processes: :func:`record` holds an exclusive kernel flock on the
companion lock file (``path + ".lock"``) around the whole
validate/replace sequence and :func:`get` holds a shared one only while
opening and reading, so a reader racing a writer observes either the
complete previous document or the complete new one -- never a truncated
or half-replaced file. The kernel releases the flock on process exit, so
a leftover lock file never blocks a later call.

A record commits via a same-directory temporary file that is written and
fsynced, moved over the journal with :func:`os.replace` and followed by
a directory fsync. When ``fault`` is ``"replace"`` an ``OSError`` is
injected immediately after the replace (before the directory sync),
exercising recovery: the journal's pre-call bytes are staged back over
it -- or, when the file did not exist beforehand, the new file is
removed -- and the directory synced again, all while the exclusive lock
is held. If the rollback itself fails, that ``OSError`` is raised with
the first error chained as its ``__cause__``. A failure before the
replace only discards the temporary. The journal is therefore either the
complete pre-call document or the complete post-call one; a failed
record never leaves a partially updated file.
"""

from __future__ import annotations

import contextlib
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
_STAGES = ("校验", "执行", "同步", "回滚")


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
    # Cross-process exclusion via a kernel flock on the companion lock
    # file: flock serializes separate opens of the same lock file across
    # processes (and across separate opens in this process), and the
    # kernel releases it automatically when the holding process exits --
    # even on a crash -- so a leftover lock file never blocks anyone. The
    # file is deliberately never unlinked: removing it while another
    # process waits on the old inode would split the lock domain. Any
    # failure to open or lock surfaces unchanged as OSError.
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


def _validate_event(raw: object, *, ordered: bool = False) -> dict[str, Any]:
    # An event read from disk must carry its six fields in canonical
    # order: json decodes objects into insertion-ordered dicts, so a
    # reordered event on disk surfaces here as a disordered key list and
    # is rejected rather than quietly rebuilt in order. An event handed to
    # record() in memory is only required to carry the six fields exactly
    # once; it is normalized into canonical order before being stored.
    if not isinstance(raw, dict) or set(raw.keys()) != set(_EVENT_FIELDS):
        raise ValueError("event must be an object with exactly op, target, "
                         "key, changed, error and stage")
    if ordered and list(raw.keys()) != list(_EVENT_FIELDS):
        raise ValueError("event fields must appear in order op, target, "
                         "key, changed, error, stage")
    if raw["op"] not in _OPS:
        raise ValueError("event op must be copy or restore")
    for field in ("target", "key"):
        if not isinstance(raw[field], str) or not raw[field]:
            raise ValueError(f"event {field} must be a non-empty string")
    # bool is checked exactly: ints must not be accepted as booleans.
    if not isinstance(raw["changed"], bool):
        raise ValueError("event changed must be a boolean")

    error = raw["error"]
    stage = raw["stage"]
    if error is None:
        if stage is not None:
            raise ValueError("event stage must be null when error is null")
    else:
        if not isinstance(error, str) or not error:
            raise ValueError("event error must be null or a non-empty "
                             "exception class name")
        if stage not in _STAGES:
            raise ValueError(
                "event stage must be one of 校验, 执行, 同步, 回滚")
    return {field: raw[field] for field in _EVENT_FIELDS}


def _validate_document(data: object, *, ordered: bool = False) -> dict[str, Any]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("audit root must be an object with keys version "
                         "and events")
    # A document read off disk must present its root fields version then
    # events; the read never accepts a reordered file by sorting it first.
    if ordered and list(data.keys()) != list(_ROOT_FIELDS):
        raise ValueError("audit root fields must appear in order version, "
                         "events")

    version = data["version"]
    # bool is a subclass of int and must be rejected as a version.
    if not isinstance(version, int) or isinstance(version, bool) \
            or version != _VERSION:
        raise ValueError("unsupported audit version")

    events_raw = data["events"]
    if not isinstance(events_raw, dict):
        raise ValueError("events must be an object")
    # On disk the events member lists its keys in code-point order; any
    # disorder is rejected rather than silently sorted into acceptance.
    if ordered and list(events_raw.keys()) != sorted(events_raw):
        raise ValueError("audit events must be ordered by key code point")

    # New in-memory documents have no on-disk order to honor and are built
    # sorted; a read document already proved its order above, so iterating
    # in stored order either way yields the code-point order.
    events: dict[str, Any] = {}
    for event_key in events_raw:
        if not isinstance(event_key, str) or not event_key:
            raise ValueError("event keys must be non-empty strings")
        events[event_key] = _validate_event(
            events_raw[event_key], ordered=ordered)

    return {"version": _VERSION, "events": events}


def _read_document(
    realpath: str,
) -> tuple[dict[str, Any] | None, bytes | None]:
    # The journal is created on demand, so a missing file simply means no
    # event has been recorded yet; the raw bytes are returned as well so a
    # later rollback can restore that exact pre-call content.
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return None, None

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
    return _validate_document(data, ordered=True), raw


def _serialize(events: dict[str, Any]) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, events ordered by key
    # code point, terminated by exactly one newline.
    document = {
        "version": _VERSION,
        "events": {key: events[key] for key in sorted(events)},
    }
    text = json.dumps(document, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _fsync_dir(directory: str) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _rollback(
    realpath: str,
    directory: str,
    old_bytes: bytes | None,
    first: BaseException,
) -> None:
    # Restore the exact state the journal held when record started, while
    # the exclusive lock is still held: stage the pre-call bytes back over
    # the replaced file (same-directory temporary, fsync, replace), or
    # remove the file when it did not exist beforehand; sync the directory
    # afterwards. Any failure of this recovery is raised chained after the
    # first error.
    try:
        if old_bytes is None:
            try:
                os.unlink(realpath)
            except FileNotFoundError:
                pass
        else:
            fd, tmp_path = tempfile.mkstemp(
                dir=directory, prefix=".audit-restore-", suffix=".tmp")
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
        _fsync_dir(directory)
    except OSError as recovery:
        raise recovery from first


def _commit(
    realpath: str,
    directory: str,
    payload: bytes,
    old_bytes: bytes | None,
    fault: str | None,
) -> None:
    # Phase 1: stage the new bytes in a same-directory temporary, fsync
    # them, then atomically move them over the journal. Until the replace
    # succeeds the journal still holds the pre-call bytes, so a failure
    # only removes this attempt's temporary.
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".audit-", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, realpath)
    except BaseException as first:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        except OSError as cleanup:
            raise cleanup from first
        raise

    # Phase 2: persist the replace with a directory fsync. The journal has
    # already been replaced at this point, so any failure -- the injected
    # "replace" fault included -- must restore the pre-call bytes (or the
    # file's prior nonexistence) while the lock is held.
    try:
        if fault == "replace":
            raise OSError("injected failure after audit file replace")
        _fsync_dir(directory)
    except BaseException as first:
        _rollback(realpath, directory, old_bytes, first)
        raise


def record(
    path: str,
    key: str,
    event: dict[str, Any],
    fault: str | None = None,
) -> tuple[dict[str, Any], bool]:
    """Record one copy/restore audit event under an idempotency ``key``.

    ``path`` and ``key`` must be non-empty strings and ``event`` an object
    with exactly op, target, key, changed, error and stage: ``op`` must be
    ``"copy"`` or ``"restore"``, ``target`` and the event's own ``key``
    non-empty strings and ``changed`` a boolean. On success ``error`` and
    ``stage`` must both be null; on failure ``error`` must be a non-empty
    exception class name and ``stage`` one of ``"校验"``, ``"执行"``,
    ``"同步"`` and ``"回滚"``. Any invalid input raises ``ValueError``, and
    so does ``fault`` being anything other than ``None`` or ``"replace"``.

    Returns ``(event, created)``: ``True`` when this call appended the
    event (creating the journal file if missing), ``False`` when the key
    already carried an identical event -- that replay writes nothing and
    never triggers the ``fault``. A key already carrying a different event
    raises ``ValueError``. A missing parent directory raises
    ``FileNotFoundError``; malformed JSON or UTF-8, a negative-zero
    literal, or an unsupported version or structure in an existing
    journal raises ``ValueError``; any other locking or I/O failure raises
    ``OSError``.

    The commit writes a same-directory temporary, fsyncs it, replaces the
    journal atomically and fsyncs its directory. With
    ``fault="replace"`` an ``OSError`` is raised right after the replace;
    the call then restores the journal's pre-call bytes, or removes the
    newly created file, before propagating it -- chained after the first
    error should the rollback itself fail.
    """
    for value in (path, key):
        if not isinstance(value, str) or not value:
            raise ValueError("path and key must be non-empty strings")
    clean_event = _validate_event(event)
    if fault is not None and fault != "replace":
        raise ValueError('fault must be None or "replace"')

    realpath = os.path.realpath(path)
    store = _get_store(realpath)
    with store.lock:
        with _file_lock(realpath):
            document, old_bytes = _read_document(realpath)
            events: dict[str, Any] = (
                {} if document is None else dict(document["events"]))

            existing = events.get(key)
            if existing is not None:
                if existing != clean_event:
                    raise ValueError(
                        "audit key was already used with a different event")
                return dict(existing), False

            events[key] = clean_event
            directory = os.path.dirname(realpath) or "."
            _commit(realpath, directory, _serialize(events), old_bytes, fault)
            return dict(clean_event), True


def get(path: str, key: str) -> dict[str, Any]:
    """Return a read-only copy of the audit event stored under ``key``.

    ``path`` and ``key`` must be non-empty strings, else ``ValueError``.
    A missing journal file raises ``FileNotFoundError``; malformed JSON or
    UTF-8, a negative-zero literal, or an unsupported version or structure
    raises ``ValueError``; an unknown key raises ``KeyError(key)``; any
    other locking or I/O failure raises ``OSError``. The query never
    writes and holds the journal's companion lock shared only while the
    file is opened and read, so it can only observe a complete document.
    The returned dict is a fresh copy with fields op, target, key,
    changed, error and stage in that order; mutating it never affects the
    journal.
    """
    for value in (path, key):
        if not isinstance(value, str) or not value:
            raise ValueError("path and key must be non-empty strings")

    realpath = os.path.realpath(path)
    with _file_lock(realpath, shared=True):
        with open(realpath, "rb") as handle:
            raw = handle.read()

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
    document = _validate_document(data, ordered=True)

    events = document["events"]
    if key not in events:
        raise KeyError(key)
    return dict(events[key])


# -- integration with the history_copy / history_recovery entry points -----
#
# These helpers are the single bridge between an audited copy/restore and
# the journal: they validate the optional (path, key) pair, snapshot the
# target bytes so a failure event can report whether the target actually
# moved, classify the stage an exception escaped from, and append the
# outcome event. The entry points keep every one of their existing locks,
# bytes and exceptions; the journal is a strictly separate file.

def check_pair(
    audit_path: object, audit_key: object
) -> tuple[str, str] | None:
    """Validate the optional audit ``(path, key)`` pair.

    Both omitted (``None``) means auditing is disabled and returns ``None``
    -- the operation then runs exactly as it does without audit support.
    The pair must otherwise be given in full as two non-empty strings: a
    single missing side, a non-string or an empty string is a caller error
    raised before the operation begins.
    """
    if audit_path is None and audit_key is None:
        return None
    if (not isinstance(audit_path, str) or not audit_path
            or not isinstance(audit_key, str) or not audit_key):
        raise ValueError(
            "audit_path and audit_key must be omitted together or both "
            "given as non-empty strings")
    return audit_path, audit_key


def snapshot(path: str) -> bytes | None:
    """Return a file's current bytes, or ``None`` when it does not exist.

    A missing file is a state rather than an error here; ``None`` must
    stay distinct from an empty file, so a failure event can tell a target
    that was created (or deleted) by the operation from one that merely
    changed content.
    """
    try:
        with open(path, "rb") as handle:
            return handle.read()
    except FileNotFoundError:
        return None


def stage_for(exc: BaseException) -> str:
    """Classify the stage an operation exception escaped from.

    Argument, history-content, idempotency-key and recovery-copy failures,
    including missing source/target/recovery files, are ``"校验"``; a
    pre-existing target or a leftover recovery copy is ``"执行"``; the
    first failure of a lock, temporary file, replace, unlink or fsync is
    ``"同步"``; a compensation cleanup or rollback that then fails itself
    -- always escaping chained after the first error -- is ``"回滚"``.

    Validation ``ValueError``/``KeyError`` may legitimately carry a
    ``__cause__`` (the underlying decode/parse error) and are classified
    first; among ``OSError`` family, a chained exception is exactly the
    "later error chained after the first" produced by a failed cleanup or
    rollback, so it wins over the FileNotFound/FileExists buckets, which in
    the copy/restore protocols are always raised unchained.
    """
    if isinstance(exc, (ValueError, KeyError)):
        return "校验"
    if isinstance(exc, OSError):
        if exc.__cause__ is not None:
            return "回滚"
        if isinstance(exc, FileNotFoundError):
            return "校验"
        if isinstance(exc, FileExistsError):
            return "执行"
        return "同步"
    # Every error the copy/restore entry points raise fits the buckets
    # above; an unexpected one happened while the operation was executing.
    return "执行"


def emit(
    audit_path: str,
    audit_key: str,
    *,
    op: str,
    target: str,
    key: str,
    changed: bool,
    error: str | None,
    stage: str | None,
) -> None:
    """Append one copy/restore outcome event under the audit key."""
    event = {
        "op": op,
        "target": target,
        "key": key,
        "changed": changed,
        "error": error,
        "stage": stage,
    }
    record(audit_path, audit_key, event)
