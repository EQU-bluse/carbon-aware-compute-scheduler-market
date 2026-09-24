"""Idempotent audit journal of copy/restore operations with a digest chain.

Two on-disk versions are supported:

Version 1 is the legacy form ``{"version": 1, "events": {key: event}}``
with ``version`` before ``events`` and the events ordered by key code
point; every event carries exactly op, target, key, changed, error and
stage, in that order. It carries no integrity metadata. It is accepted
for reads and for idempotent replays, but a :func:`record` that appends a
new event seals the whole document -- old events included -- into version
2 in the same atomic commit. Pure reads and identical replays never
upgrade it.

Version 2 is the sealed form: the root keeps ``version`` and ``events``
in that order and appends ``head`` after them; each events value is the
triple ``[event, previous_digest, digest]``, where the first item's
previous digest is ``null`` and every later item references the previous
item in code-point order. Item digests are 64 lowercase hex chars: an
item's digest is the SHA-256 of the compact UTF-8 JSON (the same compact
encoding the journal uses) of ``[audit_key, event, previous_digest]``.
The root ``head`` holds the last item's digest, or ``null`` for an empty
log; removing or altering the tail therefore breaks the chain rather
than rewrites its anchor.

Read queries (:func:`get`, :func:`search`) still return exactly the
original event objects -- chain fields never leak into query results --
and :func:`verify` is the read-only integrity report:

* a legal version-1 journal returns its real count, ``sealed`` and
  ``valid`` false and ``first_invalid``/``head`` null;
* a version-2 journal validates item digests, previous references and
  the root head, returning count, ``sealed`` and ``valid`` true,
  ``first_invalid`` null and the declared head;

* tampering is reported rather than raised: a bad item digest reports
  that zero-based index, a broken previous reference reports the current
  index, and only the root-head mismatch (with the chain itself intact)
  reports ``count``;
* structural/encoding/version violations still raise ``ValueError``.

:func:`record` validates the old chain under the exclusive lock before
appending; a broken chain raises ``ValueError`` without touching the
file. Concurrent inserts serialize on the lock, each rebuilding the
sorted document with fresh digests for every item (no cached state
survives across calls), so a restart simply continues from the persisted
last item. Same-key same-event replay writes nothing (even against
version 1) and returns the stored event with ``False``; same-key
different-event conflict raises ``ValueError``.

All entry points resolve the journal to its real path and guard the
file with a per-realpath threading lock plus a per-realpath kernel flock
on the companion lock file (``path + ".lock"``): :func:`record` holds an
exclusive flock around the whole validate/replace sequence and the
read-only queries hold a shared one only while opening and reading, so a
reader racing a writer observes either the complete previous document or
the complete new one -- never a truncated or half-replaced file.

A record commits via a same-directory temporary file that is written and
fsynced, moved over the journal with :func:`os.replace` and followed by
a directory fsync. With ``fault="replace"`` an ``OSError`` is injected
right after the replace (before the directory sync), and the call
restores the journal's pre-call bytes (or removes the newly created
file), dir-fsynced, while the exclusive lock is held. A failure before
the replace only discards the temporary. The journal is therefore
either the complete pre-call document -- a legacy version-1 document
included -- or the complete sealed version-2 document; a failed record
never leaves a partially updated file.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import tempfile
import threading
from typing import Any, Iterator

from ._jsonio import strict_loads

__all__ = ["record", "get", "search", "verify"]

_V1 = 1
_V2 = 2
_ROOT_FIELDS_V1 = ("version", "events")
_ROOT_FIELDS_V2 = ("version", "events", "head")
_EVENT_FIELDS = ("op", "target", "key", "changed", "error", "stage")
_OPS = ("copy", "restore")
_STAGES = ("校验", "执行", "同步", "回滚")
# The search stage filter additionally accepts 成功, selecting the
# events whose error and stage are both null.
_SEARCH_STAGES = ("成功",) + _STAGES
_DEFAULT_LIMIT = 100
_MAX_LIMIT = 1000
_HEX = frozenset("0123456789abcdef")


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
        # Recomputing every item digest at append time seals version-1
        # journals on first insert and makes every v2 commit stand on
        # its own chain.
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


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_event(raw: object, *, strict_order: bool) -> dict[str, Any]:
    # The field set is part of the contract everywhere; the public field
    # order is enforced only for events read from disk. A caller-passed
    # event may arrive in any order and is normalized to the public
    # order -- never reordered on read, where an out-of-order event is
    # malformed and rejected as it stands.
    if not isinstance(raw, dict):
        raise ValueError("event must be an object with exactly op, target, "
                         "key, changed, error and stage")
    if strict_order:
        if list(raw.keys()) != list(_EVENT_FIELDS):
            raise ValueError(
                "event fields must be op, target, key, changed, error and "
                "stage, in that order")
    elif set(raw.keys()) != set(_EVENT_FIELDS):
        raise ValueError("event must be an object with exactly op, target, "
                         "key, changed, error and stage")
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


def _is_digest(value: object) -> bool:
    # Declared digests and previous references are exactly 64 lowercase
    # hex characters; uppercase or odd lengths cannot name a real digest
    # and count as malformed structure rather than a broken chain.
    return isinstance(value, str) and len(value) == 64 \
        and all(char in _HEX for char in value)


def _validate_v1_events(events_raw: object) -> dict[str, Any]:
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
        events[event_key] = _validate_event(events_raw[event_key],
                                            strict_order=True)
    return events


def _validate_v2_entry(raw: object) -> tuple[dict[str, Any], str | None, str]:
    # A sealed value is exactly [event, previous, digest]: a JSON array of
    # length 3, previous null or a 64-lowercase-hex digest and digest a
    # 64-lowercase-hex digest. The chaining relationships themselves are
    # checked separately by _chain_first_invalid so a tampered document
    # can report the offending zero-based position instead of raising.
    if not isinstance(raw, list) or len(raw) != 3:
        raise ValueError("sealed audit entries must be [event, previous, "
                         "digest] triples")
    event_raw, previous_raw, digest_raw = raw
    previous: str | None
    if previous_raw is None:
        previous = None
    elif _is_digest(previous_raw):
        previous = previous_raw
    else:
        raise ValueError("previous digest must be null or a 64-character "
                         "lowercase hex digest")
    if not _is_digest(digest_raw):
        raise ValueError("digest must be a 64-character lowercase hex "
                         "digest")
    event = _validate_event(event_raw, strict_order=True)
    return event, previous, digest_raw


def _validate_v2_events(
    events_raw: object,
) -> dict[str, tuple[dict[str, Any], str | None, str]]:
    if not isinstance(events_raw, dict):
        raise ValueError("events must be an object")
    event_keys = list(events_raw)
    if event_keys != sorted(event_keys):
        raise ValueError("audit events must be ordered by key code point")

    entries: dict[str, tuple[dict[str, Any], str | None, str]] = {}
    for event_key in event_keys:
        if not isinstance(event_key, str) or not event_key:
            raise ValueError("event keys must be non-empty strings")
        entries[event_key] = _validate_v2_entry(events_raw[event_key])
    return entries


def _validate_head(head_raw: object) -> str | None:
    # Shape only: null or a 64-lowercase-hex digest. Whether null is
    # permitted for a non-empty log, or the digest equals the last
    # item's digest, is a chain condition -- verify reports it (count)
    # instead of raising, and record/get/search reject it as a broken
    # chain.
    if head_raw is None:
        return None
    if not _is_digest(head_raw):
        raise ValueError("head must be null or a 64-character lowercase "
                         "hex digest")
    return head_raw


def _validate_document(data: object) -> dict[str, Any]:
    # Returns {"version": int, "events": {key: event} (plain events),
    # "entries": {key: (event, previous, digest)} for v2, "head": str|None
    # for v2}. Structure, field order, key order and value shapes are
    # enforced here with ValueError; digest/previous/head *content* is
    # not -- that is a chain condition, reported via verify and rejected
    # by record/get/search through _assert_intact_chain.
    if not isinstance(data, dict):
        raise ValueError("audit root must be an object with version and "
                         "events (and head for version 2), in order")
    root_fields = list(data.keys())
    version_raw = data.get("version", object())
    # bool is a subclass of int and must be rejected as a version.
    if not isinstance(version_raw, int) or isinstance(version_raw, bool):
        raise ValueError("unsupported audit version")
    if version_raw == _V1:
        if root_fields != list(_ROOT_FIELDS_V1):
            raise ValueError("audit root must be an object with keys version "
                             "and events, in that order")
        events = _validate_v1_events(data["events"])
        return {"version": _V1, "events": events,
                "entries": None, "head": None}
    if version_raw == _V2:
        if root_fields != list(_ROOT_FIELDS_V2):
            raise ValueError("audit root must be an object with keys version, "
                             "events and head, in that order")
        entries = _validate_v2_events(data["events"])
        head = _validate_head(data["head"])
        events = {key: entry[0] for key, entry in entries.items()}
        return {"version": _V2, "events": events,
                "entries": entries, "head": head}
    raise ValueError("unsupported audit version")


def _read_document_raw(realpath: str) -> tuple[object, bytes]:
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
    return data, raw


def _read_document(
    realpath: str,
) -> tuple[dict[str, Any] | None, bytes | None]:
    # The journal is created on demand, so a missing file simply means no
    # event has been recorded yet; the raw bytes are returned as well so a
    # later rollback can restore that exact pre-call content.
    try:
        data, raw = _read_document_raw(realpath)
    except FileNotFoundError:
        return None, None
    return _validate_document(data), raw


# ---------------------------------------------------------------------------
# Digest chain
# ---------------------------------------------------------------------------

def _compact_json(value: Any) -> bytes:
    # The established compact UTF-8 convention: compact separators,
    # non-ASCII written through, NaN/Infinity impossible for the value
    # shapes involved. Both the per-item preimage and the whole document
    # serialize through this one function.
    text = json.dumps(value, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False)
    return text.encode("utf-8")


def _item_digest(audit_key: str, event: dict[str, Any],
                 previous: str | None) -> str:
    preimage = _compact_json([audit_key, event, previous])
    return hashlib.sha256(preimage).hexdigest()


def _chain_first_invalid(
    keys: list[str],
    entries: dict[str, tuple[dict[str, Any], str | None, str]],
    head: str | None,
) -> int | None:
    # Walk the sealed entries in persisted (code-point) order:
    #   * a wrong item digest reports its own index;
    #   * a previous reference not equal to the preceding item's digest
    #     reports the current index (the first item must reference null);
    #   * only the root head mismatch, chain otherwise intact, reports
    #     count.
    # The declared-digest check precedes the previous check within an
    # item; the previous check at index i needs the declared digest at
    # i-1. Both checks at i run before advancing.
    previous_expected: str | None = None
    last_digest: str | None = None
    for index, audit_key in enumerate(keys):
        event, previous, digest = entries[audit_key]
        if digest != _item_digest(audit_key, event, previous):
            # Even if the previous reference is also wrong, the item's
            # own digest mismatch is reported at its index first.
            return index
        if previous != previous_expected:
            # A tamperer changing the stored previous breaks both this
            # link and the item digest, so reaching here means the event
            # content changed while the triple stayed self-consistent --
            # report the current item.
            return index
        previous_expected = digest
        last_digest = digest
    if head != last_digest:
        return len(keys)
    return None


def _assert_intact_chain(document: dict[str, Any]) -> None:
    if document["version"] == _V1:
        return
    keys = list(document["events"])
    bad = _chain_first_invalid(keys, document["entries"], document["head"])
    if bad is not None:
        raise ValueError("audit digest chain is broken")


# ---------------------------------------------------------------------------
# Serialization / commit
# ---------------------------------------------------------------------------

def _seal_events(
    events: dict[str, Any],
) -> tuple[dict[str, tuple[dict[str, Any], str | None, str]], str | None]:
    # Rebuild the whole chain from scratch: insert paths (new v2 log,
    # v1->v2 upgrade, and append to an existing v2 log) all funnel here,
    # and the exclusive lock plus full recomputation make concurrent
    # inserts serialize correctly with no cached suffix to invalidate.
    keys = sorted(events)
    entries: dict[str, tuple[dict[str, Any], str | None, str]] = {}
    previous: str | None = None
    head: str | None = None
    for audit_key in keys:
        event = events[audit_key]
        digest = _item_digest(audit_key, event, previous)
        entries[audit_key] = (event, previous, digest)
        previous = digest
        head = digest
    return entries, head


def _serialize(events: dict[str, Any]) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, events ordered by key
    # code point, terminated by exactly one newline. Every journal written
    # from now on is version 2 with the full [event, previous, digest]
    # chain and a root head (null for an empty log).
    entries, head = _seal_events(events)
    document: dict[str, Any] = {
        "version": _V2,
        "events": {
            key: [entries[key][0], entries[key][1], entries[key][2]]
            for key in sorted(entries)
        },
        "head": head,
    }
    return _compact_json(document) + b"\n"


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
    # the replaced file (same-directory temp, fsync, replace), or remove
    # the file when it did not exist beforehand; sync the directory
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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def record(
    path: str,
    key: str,
    event: dict[str, Any],
    fault: str | None = None,
) -> tuple[dict[str, Any], bool]:
    """Record one copy/restore audit event under an idempotency ``key``.

    ``path`` and ``key`` must be non-empty strings and ``event`` an object
    with exactly op, target, key, changed, error and stage -- in any
    order; the event is stored and returned in the public order. ``op``
    must be ``"copy"`` or ``"restore"``, ``target`` and the event's own
    ``key`` non-empty strings and ``changed`` a boolean. On success
    ``error`` and ``stage`` must both be null; on failure ``error`` must
    be a non-empty exception class name and ``stage`` one of ``"校验"``,
    ``"执行"``, ``"同步"`` and ``"回滚"``. Any invalid input raises
    ``ValueError``, and so does ``fault`` being anything other than
    ``None`` or ``"replace"``.

    An existing journal is chain-validated under the exclusive lock
    before anything is written; a broken v2 chain raises ``ValueError``
    and leaves the file untouched. Returns ``(event, created)``:
    ``True`` when this call appended the event -- sealing a legacy
    version-1 journal into version 2 (old events included) or creating
    the file (version 2) as needed -- and ``False`` when the key already
    carried an identical event. That replay writes nothing, never
    upgrades a version-1 file and never triggers the ``fault``. A key
    already carrying a different event raises ``ValueError``.

    A missing parent directory raises ``FileNotFoundError``; malformed
    JSON or UTF-8, a negative-zero literal, an unsupported version or
    structure, or a broken digest chain in an existing journal raises
    ``ValueError``; any other locking or I/O failure raises ``OSError``.

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
    clean_event = _validate_event(event, strict_order=False)
    if fault is not None and fault != "replace":
        raise ValueError('fault must be None or "replace"')

    realpath = os.path.realpath(path)
    store = _get_store(realpath)
    with store.lock:
        with _file_lock(realpath):
            document, old_bytes = _read_document(realpath)
            if document is not None:
                _assert_intact_chain(document)
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


def _read_existing_document(realpath: str) -> dict[str, Any]:
    # Read-only path shared by get/search/verify: the shared flock is held
    # only while the file is opened and read, so a query racing a writer
    # observes either the complete previous document or the complete new
    # one. A missing journal surfaces as FileNotFoundError; malformed
    # UTF-8/JSON (negative-zero literals included), an unsupported
    # version or any structural or order deviation raises ValueError --
    # the document is never reordered and accepted, and never rewritten.
    with _file_lock(realpath, shared=True):
        data, _raw = _read_document_raw(realpath)
    return _validate_document(data)


def get(path: str, key: str) -> dict[str, Any]:
    """Return a read-only copy of the audit event stored under ``key``.

    Sealed (version-2) events are returned as plain event dicts -- the
    previous/digest chain fields never appear in query results.

    ``path`` and ``key`` must be non-empty strings, else ``ValueError``.
    A missing journal file raises ``FileNotFoundError``; malformed JSON or
    UTF-8, a negative-zero literal, an unsupported version or structure,
    or a broken digest chain raises ``ValueError`` without rewriting the
    file; an unknown key raises ``KeyError(key)``; any other locking or
    I/O failure raises ``OSError``. The query never writes and holds the
    journal's companion lock shared only while the file is opened and
    read, so it can only observe a complete document. The returned dict
    is a fresh copy with fields op, target, key, changed, error and
    stage in that order; mutating it never affects the journal.
    """
    for value in (path, key):
        if not isinstance(value, str) or not value:
            raise ValueError("path and key must be non-empty strings")

    realpath = os.path.realpath(path)
    document = _read_existing_document(realpath)
    _assert_intact_chain(document)

    events = document["events"]
    if key not in events:
        raise KeyError(key)
    return dict(events[key])


def _matches(event: dict[str, Any], op: str | None, stage: str | None,
             key: str | None) -> bool:
    # Every provided filter must hold (logical AND). "成功" selects the
    # successful events -- error and stage both null -- while any other
    # stage filter names the failure stage to match exactly. The history
    # key filter is a plain string equality: no prefix matching, case
    # folding or path normalization.
    if op is not None and event["op"] != op:
        return False
    if stage is not None:
        if stage == "成功":
            if event["error"] is not None or event["stage"] is not None:
                return False
        elif event["stage"] != stage:
            return False
    if key is not None and event["key"] != key:
        return False
    return True


def search(
    path: str,
    cursor: str | None = None,
    limit: int = _DEFAULT_LIMIT,
    op: str | None = None,
    stage: str | None = None,
    key: str | None = None,
) -> dict[str, Any]:
    """Return one page of audit events matching the given filters.

    ``path`` must be a non-empty string. ``cursor`` is ``None`` or a
    non-empty string -- it need not name an existing event -- and only
    events whose audit key is strictly greater than it in code-point
    order are considered. ``limit`` is the page size: an integer from 1
    to 1000 (booleans are rejected), defaulting to 100. Each of ``op``,
    ``stage`` and ``key`` is an optional filter; all given filters must
    hold simultaneously. ``op`` is ``"copy"`` or ``"restore"``; ``stage``
    is one of ``"成功"`` (events whose error and stage are both null),
    ``"校验"``, ``"执行"``, ``"同步"`` and ``"回滚"`` (events that failed
    at that stage); ``key`` matches the event's history key by exact
    string equality. Any other value raises ``ValueError``.

    The journal is scanned in ascending audit-key code-point order and
    filtered afterwards, so non-matching events never consume page
    capacity. Returns ``{"events": [[audit_key, event], ...], "next":
    cursor_or_none}`` where each value is the plain event (chain fields
    are not exposed), and ``next`` is the audit key of the page's last
    item when further matching events remain, else ``None``. With no
    matching events the page is empty and ``next`` is ``None``.

    The query is strictly read-only: a missing journal raises
    ``FileNotFoundError``; malformed JSON or UTF-8, a negative-zero
    literal, an unsupported version or structure, or a broken digest
    chain raises ``ValueError`` and never rewrites the file; any other
    locking or I/O failure raises ``OSError``. It holds the journal's
    companion lock shared only while the file is opened and read, so a
    query racing a writer observes either the complete previous document
    or the complete new one.
    """
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")
    if cursor is not None and (not isinstance(cursor, str) or not cursor):
        raise ValueError("cursor must be None or a non-empty string")
    # bool is a subclass of int and must be rejected as a page size.
    if not isinstance(limit, int) or isinstance(limit, bool) \
            or not 1 <= limit <= _MAX_LIMIT:
        raise ValueError("limit must be an integer between 1 and 1000")
    if op is not None and op not in _OPS:
        raise ValueError('op filter must be "copy" or "restore"')
    if stage is not None and stage not in _SEARCH_STAGES:
        raise ValueError(
            "stage filter must be one of 成功, 校验, 执行, 同步, 回滚")
    if key is not None and (not isinstance(key, str) or not key):
        raise ValueError("key filter must be a non-empty string")

    realpath = os.path.realpath(path)
    document = _read_existing_document(realpath)
    _assert_intact_chain(document)

    # The persisted events are validated to be in ascending key
    # code-point order, so the document order is the scan order. One
    # extra match is collected to learn whether the page is the last.
    matches: list[list[Any]] = []
    for audit_key, event in document["events"].items():
        if cursor is not None and audit_key <= cursor:
            continue
        if not _matches(event, op, stage, key):
            continue
        matches.append([audit_key, dict(event)])
        if len(matches) > limit:
            break

    if len(matches) > limit:
        page = matches[:limit]
        next_cursor: str | None = page[-1][0]
    else:
        page = matches
        next_cursor = None
    return {"events": page, "next": next_cursor}


def verify(path: str) -> dict[str, Any]:
    """Return the read-only integrity report of the audit journal.

    ``path`` must be a non-empty string, else ``ValueError``. A missing
    journal raises ``FileNotFoundError``; malformed UTF-8/JSON, a
    negative-zero literal, an unsupported version, or any root/event/
    entry structural or field-order violation raises ``ValueError``;
    other locking or I/O failures raise ``OSError``. The call never
    writes.

    Returns a dict with keys ``version``, ``count``, ``sealed``,
    ``valid``, ``first_invalid`` and ``head``, in that order:

    * Version 1: the real event count, ``sealed`` and ``valid`` false,
      ``first_invalid`` and ``head`` null.
    * Intact version 2: ``count`` events, ``sealed`` and ``valid`` true,
      ``first_invalid`` null, ``head`` the digest declared by the file.
    * Tampered version 2 (bad item digest, broken previous reference or
      wrong root head) is not raised: ``valid`` is false and
      ``first_invalid`` names the first zero-based position -- the
      item's own index for a digest mismatch, the current index for a
      broken previous link, or ``count`` solely when just the root head
      is wrong -- while ``head`` stays the declared value.
    """
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")

    realpath = os.path.realpath(path)
    document = _read_existing_document(realpath)

    keys = list(document["events"])
    count = len(keys)
    if document["version"] == _V1:
        sealed = False
        valid = False
        first_invalid: int | None = None
        head: str | None = None
    else:
        sealed = True
        first_invalid = _chain_first_invalid(
            keys, document["entries"], document["head"])
        valid = first_invalid is None
        head = document["head"]
    return {
        "version": document["version"],
        "count": count,
        "sealed": sealed,
        "valid": valid,
        "first_invalid": first_invalid,
        "head": head,
    }
