"""Idempotent audit journal of copy/restore operations.

Each journal file records the outcomes of copy and restore operations
under caller-chosen idempotency keys. A document has the form
``{"version": 1, "events": {key: event}}`` with ``version`` before
``events`` and the events ordered by key code point; every event carries
exactly op, target, key, changed, error and stage, persisted and returned
in that order. The public order is part of the on-disk contract and is
verified on read: a document read from disk whose root fields, event
fields or event keys appear out of order is malformed and raises
``ValueError`` -- it is never reordered and accepted, and a read-only
query never rewrites it. Events handed to :func:`record` are checked only
for the exact field *set*, never for their incoming order: an equivalent
event whose six fields arrive in another order is accepted, normalized to
the public order and returned and persisted in it. ``op`` is ``"copy"``
or ``"restore"`` and ``changed`` is a boolean; a successful event has
``error`` and ``stage`` both null, while a failed one names the
exception class in ``error`` and the stage it failed at in ``stage`` --
one of ``"校验"``, ``"执行"``, ``"同步"`` and ``"回滚"``.

:func:`record` replays the event already stored under a key: the same
key with the same event returns a copy of it together with ``False`` and
writes nothing, while the same key with a different event is a conflict
and raises ``ValueError``. A new key appends its event, creates the file
when missing, and returns the event copy together with ``True``.
:func:`get` is strictly read-only and returns a copy of the event stored
under a key; an unknown key raises ``KeyError(key)``. :func:`search` is
likewise strictly read-only and pages the journal in audit-key code
point order with an exclusive cursor, optionally filtering by op,
failure stage and history key.

:func:`get` and :func:`search` hold the shared companion lock only
while opening and reading, so a reader racing a writer observes either
the complete previous document or the complete new one -- never a
truncated or half-replaced file. All three resolve the journal to its
real path and guard it with a per-realpath threading lock plus a kernel
flock on the companion lock file (``path + ".lock"``): :func:`record`
holds its flock exclusively around the whole validate/replace sequence
while the read-only entry points hold theirs shared only while opening
and reading. The kernel releases the flock on process exit, so a
leftover lock file never blocks a later call.

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

__all__ = ["record", "get", "search"]

_VERSION = 1
_ROOT_FIELDS = ("version", "events")
_EVENT_FIELDS = ("op", "target", "key", "changed", "error", "stage")
_OPS = ("copy", "restore")
_STAGES = ("校验", "执行", "同步", "回滚")
_SEARCH_STAGES = ("成功",) + _STAGES
_DEFAULT_COUNT = 100
_MAX_COUNT = 1000


def _is_plain_int(value: object) -> bool:
    # bool is a subclass of int and must be rejected as a count.
    return isinstance(value, int) and not isinstance(value, bool)


def check_pair(audit_path: object, audit_key: object) -> bool:
    """Validate the optional ``(audit_path, audit_key)`` argument pair.

    Both omitted means auditing is off and returns ``False``; both given
    as non-empty strings means auditing and returns ``True``. Exactly one
    given, or either value not a non-empty string, is a caller error and
    raises ``ValueError`` before the operation starts.
    """
    if audit_path is None and audit_key is None:
        return False
    if not isinstance(audit_path, str) or not audit_path \
            or not isinstance(audit_key, str) or not audit_key:
        raise ValueError(
            "audit_path and audit_key must be provided together as "
            "non-empty strings")
    return True


def failure_stage(exc: BaseException) -> str:
    """Classify the stage an operation exception leaves the call at.

    A target/recovery conflict is ``"执行"``; an :class:`OSError` chained
    after the first error by a compensation cleanup or rollback is
    ``"回滚"``; argument, history-content, history-key, recovery-copy or
    missing-file validation failures (``ValueError``, ``KeyError`` and
    ``FileNotFoundError``) are ``"校验"``; every other locking, temporary
    file, replace, unlink or fsync failure is ``"同步"``.
    """
    # The explicit chain (raise ... from first) is the marker of a
    # compensation cleanup or rollback that itself failed: the later
    # error leaves with the first one as its __cause__ and the stage is
    # 回滚, even when that later error happens to be a FileExistsError.
    if isinstance(exc, OSError) and exc.__cause__ is not None:
        return "回滚"
    if isinstance(exc, FileExistsError):
        return "执行"
    if isinstance(exc, (ValueError, KeyError, FileNotFoundError)):
        return "校验"
    return "同步"


def emit(audit_path: str, audit_key: str, op: str, target: str,
         history_key: str, changed: bool,
         exc: BaseException | None, stage: str | None) -> Exception | None:
    """Append one operation result event, or return the audit's own error.

    On success ``exc`` and ``stage`` are ``None``; on failure ``exc`` is
    the operation exception (its class name is recorded) and ``stage``
    the stage :func:`failure_stage` assigned. Returns ``None`` when the
    event was recorded or replayed idempotently, or the journal's own
    exception when recording failed so the caller can surface it --
    chained after the operation exception when there was one.
    """
    event = {
        "op": op,
        "target": target,
        "key": history_key,
        "changed": bool(changed),
        "error": None if exc is None else type(exc).__name__,
        "stage": stage,
    }
    try:
        record(audit_path, audit_key, event)
    except Exception as audit_exc:  # journal failure: ValueError/OSError/...
        return audit_exc
    return None


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


def _normalize_event(raw: object) -> dict[str, Any]:
    # An event handed to record is bound by its field *set*, not by the
    # caller's in-memory dictionary order: the same six fields arriving in
    # any order are the same event and are rebuilt in the public order.
    if not isinstance(raw, dict) or set(raw) != set(_EVENT_FIELDS):
        raise ValueError("event must be an object with exactly op, target, "
                         "key, changed, error and stage")
    return _event_values(raw)


def _validate_event(raw: object) -> dict[str, Any]:
    # A document read from disk is additionally bound by the public
    # order: an event with a missing/extra field -- or the six fields in
    # another order -- is malformed and is never reordered and accepted.
    if not isinstance(raw, dict) or list(raw.keys()) != list(_EVENT_FIELDS):
        raise ValueError("event must be an object with exactly op, target, "
                         "key, changed, error and stage, in that order")
    return _event_values(raw)


def _event_values(raw: dict[str, Any]) -> dict[str, Any]:
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


def _validate_document(data: object) -> dict[str, Any]:
    # The root's public order is part of the contract: an object carrying
    # any other set or order of fields is malformed rather than rebuilt.
    if not isinstance(data, dict) or list(data.keys()) != list(_ROOT_FIELDS):
        raise ValueError("audit root must be an object with keys version "
                         "and events, in that order")

    version = data["version"]
    # bool is a subclass of int and must be rejected as a version.
    if not isinstance(version, int) or isinstance(version, bool) \
            or version != _VERSION:
        raise ValueError("unsupported audit version")

    events_raw = data["events"]
    if not isinstance(events_raw, dict):
        raise ValueError("events must be an object")

    # The persisted form is keyed in code-point order; a document offered
    # out of order is malformed and is rejected as it stands -- never
    # sorted and accepted, and never rewritten by a read-only query.
    event_keys = list(events_raw)
    if event_keys != sorted(event_keys):
        raise ValueError("audit events must be ordered by key code point")

    events: dict[str, Any] = {}
    for event_key in event_keys:
        if not isinstance(event_key, str) or not event_key:
            raise ValueError("event keys must be non-empty strings")
        events[event_key] = _validate_event(events_raw[event_key])

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
    return _validate_document(data), raw


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


def _read_locked(realpath: str) -> bytes:
    # Hold the companion lock shared only while opening and reading, so a
    # concurrent record (exclusive lock around validate/replace/sync) can
    # never expose a truncated or half-replaced journal. Parsing and
    # validation happen only after the lock is released: the bytes in
    # memory are already one complete pre- or post-update snapshot. Unlike
    # record's on-demand-create view, a missing file is an error for a
    # query, so FileNotFoundError from open surfaces unchanged.
    with _file_lock(realpath, shared=True):
        with open(realpath, "rb") as handle:
            return handle.read()


def _decode_document(realpath: str, raw: bytes) -> dict[str, Any]:
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
    with exactly op, target, key, changed, error and stage -- the six
    fields may arrive in any order; the stored and returned copy always
    uses the public order op, target, key, changed, error and stage.
    ``op`` must be ``"copy"`` or ``"restore"``, ``target`` and the
    event's own ``key`` non-empty strings and ``changed`` a boolean. On
    success ``error`` and ``stage`` must both be null; on failure
    ``error`` must be a non-empty exception class name and ``stage`` one
    of ``"校验"``, ``"执行"``, ``"同步"`` and ``"回滚"``. Any invalid
    input raises ``ValueError``, and so does ``fault`` being anything
    other than ``None`` or ``"replace"``.

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
    clean_event = _normalize_event(event)
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
    raw = _read_locked(realpath)
    document = _decode_document(realpath, raw)

    events = document["events"]
    if key not in events:
        raise KeyError(key)
    return dict(events[key])


def search(
    path: str,
    cursor: str | None = None,
    count: int = _DEFAULT_COUNT,
    op: str | None = None,
    stage: str | None = None,
    history_key: str | None = None,
) -> dict[str, object]:
    """Return one read-only, stable page of events from the journal.

    ``path`` must be a non-empty string and ``cursor`` ``None`` or a
    non-empty string: when given, only audit keys strictly greater than
    it in code-point order are considered -- it need not name an existing
    key. ``count`` must be a non-boolean integer between 1 and 1000 and
    defaults to 100. Each supplied filter must be a string in its range;
    the supplied filters apply together by logical AND: ``op`` must be
    ``"copy"`` or ``"restore"``, ``stage`` one of ``"成功"``,
    ``"校验"``, ``"执行"``, ``"同步"`` and ``"回滚"``, and
    ``history_key`` is compared to the event's key for full string
    equality only -- no prefix match, case folding or path
    normalization. Any invalid argument raises ``ValueError``.

    ``"成功"`` matches exactly the events whose error and stage are both
    null; each other stage matches the failed events carrying that same
    stage. The journal is scanned in ascending audit-key order and
    non-matching events do not consume page capacity. The result carries
    keys events and next, in that order: events lists ``[audit_key,
    event]`` pairs (each event a fresh copy in public field order) and
    next is the last pair's audit key when further matching events
    follow, or ``None`` otherwise -- including an empty page.

    A missing journal file raises ``FileNotFoundError``; malformed JSON
    or UTF-8, a negative-zero literal, or an unsupported version,
    structure or field order raises ``ValueError``; any other locking or
    I/O failure raises ``OSError``. The query never writes and holds the
    companion lock shared only while opening and reading, so it observes
    either the complete previous document or the complete new one.
    """
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")
    if cursor is not None and (not isinstance(cursor, str) or not cursor):
        raise ValueError("cursor must be None or a non-empty string")
    if not _is_plain_int(count) or not 1 <= count <= _MAX_COUNT:
        raise ValueError(
            "count must be a non-boolean integer between 1 and 1000")
    if op is not None and op not in _OPS:
        raise ValueError("op must be None, copy or restore")
    if stage is not None and stage not in _SEARCH_STAGES:
        raise ValueError(
            "stage must be None or one of 成功, 校验, 执行, 同步, 回滚")
    if history_key is not None and (
            not isinstance(history_key, str) or not history_key):
        raise ValueError("history_key must be None or a non-empty string")

    realpath = os.path.realpath(path)
    raw = _read_locked(realpath)
    document = _decode_document(realpath, raw)

    def matches(event: dict[str, Any]) -> bool:
        if op is not None and event["op"] != op:
            return False
        if stage is not None:
            if stage == "成功":
                if not (event["error"] is None and event["stage"] is None):
                    return False
            elif event["stage"] != stage:
                return False
        if history_key is not None and event["key"] != history_key:
            return False
        return True

    page: list[list[Any]] = []
    next_cursor: str | None = None
    # The on-disk order is verified to be ascending by audit key code
    # point, so the validated mapping's own iteration order is the stable
    # scan order; the exclusive cursor is a strict code-point comparison.
    for audit_key, event in document["events"].items():
        if cursor is not None and not audit_key > cursor:
            continue
        if not matches(event):
            continue
        if len(page) < count:
            page.append([audit_key, dict(event)])
        else:
            # One more match beyond the page: the page's last audit key
            # is the exclusive cursor that continues the scan.
            next_cursor = page[-1][0]
            break

    return {"events": page, "next": next_cursor}
