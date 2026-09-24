"""Idempotent audit journal of copy/restore operations, with a digest chain.

Each journal file records the outcomes of copy and restore operations
under caller-chosen idempotency keys. The current sealed format (version
2) is ``{"version": 2, "events": {key: [event, previous, digest]},
"head": head}`` with ``version``, ``events`` and ``head`` in that order
and the events ordered by key code point; every event carries exactly
op, target, key, changed, error and stage, in that order. The public
order is part of the contract and is verified on read: a document whose
root fields, event fields or event keys appear out of order is malformed
and raises ``ValueError`` -- it is never reordered and accepted, and a
read-only query never rewrites it. ``op`` is ``"copy"`` or ``"restore"``
and ``changed`` is a boolean; a successful event has ``error`` and
``stage`` both null, while a failed one names the exception class in
``error`` and the stage it failed at in ``stage`` -- one of ``"校验"``,
``"执行"``, ``"同步"`` and ``"回滚"``.

The chain seals every event to its predecessor: each event's digest is
the SHA-256 of ``[audit_key, event, previous_digest]`` encoded with the
journal's compact UTF-8 JSON convention (compact separators, non-ASCII
written through), the first event's previous digest is null, every later
one names the digest of the event before it in code-point order, and the
root ``head`` carries the last event's digest -- null for an empty
journal, so even a tail deletion shows up as an incomplete chain.
Digests are sixty-four lowercase hexadecimal digits; anything else is
malformed. The older version 1 format (``{"version": 1, "events":
{key: event}}``) carries no chain and stays readable: a version 1
journal is upgraded to version 2 atomically, old events sealed into the
new chain, exactly when :func:`record` appends a new event -- a pure
read or an idempotent replay never rewrites it.

:func:`record` replays the event already stored under a key: the same
key with the same event returns a copy of it together with ``False`` and
writes nothing, while the same key with a different event is a conflict
and raises ``ValueError``. A new key appends its event -- creating the
file when missing, upgrading a version 1 journal, and recomputing the
digests of every event from the key's sorted position on -- and returns
the event copy together with ``True``. The caller may pass the event's
fields in any order -- only the field set must match exactly -- and the
event is stored and returned in the public order. :func:`get` is
strictly read-only and returns a copy of the event stored under a key;
an unknown key raises ``KeyError(key)``.

:func:`search` is the read-only paginated query: it scans the journal in
ascending audit-key code-point order, keeps only the events at keys
strictly greater than an optional exclusive cursor and matching every
given filter -- op, failure stage (``"成功"`` selects the successful
events) and history key, combined with logical AND -- and returns one
page of ``[audit_key, event]`` pairs together with the cursor to resume
from, or ``None`` when the page is the last. Non-matching events never
consume page capacity.

:func:`verify` is the read-only integrity query: it returns ``version``,
``count``, ``sealed``, ``valid``, ``first_invalid`` and ``head``, in
that order. A well-formed version 1 journal reports its real event count
with ``sealed`` and ``valid`` both false and ``first_invalid`` and
``head`` both null. A sealed journal whose chain is complete reports
``sealed`` and ``valid`` true, ``first_invalid`` null and ``head`` the
root digest the file declares. A chain deviation never raises: ``valid``
is false and ``first_invalid`` is the zero-based position of the first
event whose digest does not match its content or whose predecessor
reference is wrong, or the event count when only the root head
disagrees. Malformed UTF-8/JSON, an unsupported version or any
structural deviation still raises ``ValueError`` here as everywhere.

All four functions resolve the journal to its real path and guard the
file with a per-realpath lock that excludes both threads of this process
and other processes: :func:`record` holds an exclusive kernel flock on
the companion lock file (``path + ".lock"``) around the whole
validate/replace sequence -- validating the existing chain before
appending, so a broken chain raises ``ValueError`` and concurrent
inserters serialize, each recomputing its suffix over the complete
predecessor document -- and the read-only queries hold a shared one only
while opening and reading, so a reader racing a writer observes either
the complete previous document or the complete new one -- never a
truncated or half-replaced file. The kernel releases the flock on
process exit, so a leftover lock file never blocks a later call, and the
chain is rebuilt purely from the persisted document, so a restarted
process continues from the persisted last event.

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
import hashlib
import json
import os
import tempfile
import threading
from typing import Any, Iterator

from ._jsonio import strict_loads

__all__ = ["record", "get", "search", "verify"]

_VERSION = 2
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
_HEXADECIMAL = frozenset("0123456789abcdef")


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
    # A digest is exactly sixty-four lowercase hexadecimal digits.
    return isinstance(value, str) and len(value) == 64 \
        and all(char in _HEXADECIMAL for char in value)


def _event_digest(audit_key: str, event: dict[str, Any],
                  previous: str | None) -> str:
    # The digest commits to the audit key, the event and the declared
    # predecessor digest, encoded with the journal's compact UTF-8 JSON
    # convention: compact separators, non-ASCII written through.
    payload = json.dumps([audit_key, event, previous], ensure_ascii=False,
                         separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _validate_document(data: object) -> dict[str, Any]:
    # The root's public order is part of the contract: an object carrying
    # any other set or order of fields is malformed rather than rebuilt.
    # Version 1 carries plain events and no chain; version 2 seals every
    # event into a [event, previous digest, digest] triple and adds the
    # root head.
    if not isinstance(data, dict):
        raise ValueError("audit root must be an object with keys version "
                         "and events, in that order")
    root_fields = list(data.keys())
    if root_fields == list(_ROOT_FIELDS_V1):
        version = 1
    elif root_fields == list(_ROOT_FIELDS_V2):
        version = 2
    else:
        raise ValueError("audit root must be an object with keys version "
                         "and events, in that order, followed by head in "
                         "a sealed journal")

    raw_version = data["version"]
    # bool is a subclass of int and must be rejected as a version.
    if not isinstance(raw_version, int) or isinstance(raw_version, bool) \
            or raw_version != version:
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
    chain: dict[str, Any] = {}
    for event_key in event_keys:
        if not isinstance(event_key, str) or not event_key:
            raise ValueError("event keys must be non-empty strings")
        value = events_raw[event_key]
        if version == 1:
            events[event_key] = _validate_event(value, strict_order=True)
            continue
        if not isinstance(value, list) or len(value) != 3:
            raise ValueError("sealed events must be [event, previous "
                             "digest, digest] triples")
        event, previous, digest = value
        events[event_key] = _validate_event(event, strict_order=True)
        if previous is not None and not _is_digest(previous):
            raise ValueError("previous digest must be null or a 64-digit "
                             "lowercase hexadecimal digest")
        if not _is_digest(digest):
            raise ValueError("event digest must be a 64-digit lowercase "
                             "hexadecimal digest")
        chain[event_key] = [previous, digest]

    head = None
    if version == 2:
        head = data["head"]
        if head is not None and not _is_digest(head):
            raise ValueError("head must be null or a 64-digit lowercase "
                             "hexadecimal digest")

    return {"version": version, "events": events,
            "chain": chain if version == 2 else None, "head": head}


def _chain_first_invalid(document: dict[str, Any]) -> int | None:
    # Zero-based position of the first event whose digest does not match
    # its content or whose predecessor reference does not name the digest
    # of the event before it; the event count when only the root head
    # disagrees with the last event's digest; None when the chain is
    # complete. The digest is recomputed over the declared predecessor,
    # so a wrong reference surfaces as a broken link at its own position.
    events = document["events"]
    chain = document["chain"]
    previous: str | None = None
    for index, event_key in enumerate(events):
        declared_previous, digest = chain[event_key]
        if digest != _event_digest(event_key, events[event_key],
                                   declared_previous):
            return index
        if declared_previous != previous:
            return index
        previous = digest
    if document["head"] != previous:
        return len(events)
    return None


def _ensure_chain(document: dict[str, Any]) -> None:
    # Record and the read-only queries reject a sealed journal whose
    # chain is incomplete; only verify reports the deviation instead.
    if document["version"] == 2 \
            and _chain_first_invalid(document) is not None:
        raise ValueError("audit digest chain is incomplete")


def _parse_document(realpath: str, raw: bytes) -> dict[str, Any]:
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

    document = _parse_document(realpath, raw)
    _ensure_chain(document)
    return document, raw


def _serialize(chain: dict[str, Any]) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, events ordered by key
    # code point, terminated by exactly one newline. ``chain`` maps each
    # audit key to its [event, previous digest, digest] triple; the root
    # head carries the last event's digest, null for an empty journal.
    ordered = {key: chain[key] for key in sorted(chain)}
    head = ordered[list(ordered)[-1]][2] if ordered else None
    document = {"version": _VERSION, "events": ordered, "head": head}
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
    with exactly op, target, key, changed, error and stage -- in any
    order; the event is stored and returned in the public order. ``op``
    must be ``"copy"`` or ``"restore"``, ``target`` and the event's own
    ``key`` non-empty strings and ``changed`` a boolean. On success
    ``error`` and ``stage`` must both be null; on failure ``error`` must
    be a non-empty exception class name and ``stage`` one of ``"校验"``,
    ``"执行"``, ``"同步"`` and ``"回滚"``. Any invalid input raises
    ``ValueError``, and so does ``fault`` being anything other than
    ``None`` or ``"replace"``.

    Returns ``(event, created)``: ``True`` when this call appended the
    event (creating the journal file if missing), ``False`` when the key
    already carried an identical event -- that replay writes nothing and
    never triggers the ``fault``. A key already carrying a different event
    raises ``ValueError``. A missing parent directory raises
    ``FileNotFoundError``; malformed JSON or UTF-8, a negative-zero
    literal, an unsupported version or structure, or an incomplete digest
    chain in an existing journal raises ``ValueError``; any other locking
    or I/O failure raises ``OSError``.

    The append seals the event into the digest chain: a version 1
    journal is upgraded to version 2 atomically with the same commit,
    its old events sealed alongside the new one, and a key landing
    between existing ones has every digest from its sorted position on
    recomputed. The existing chain is validated under the exclusive lock
    before anything is written, so concurrent inserters serialize and
    each extends a complete predecessor document.

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
            events: dict[str, Any] = (
                {} if document is None else dict(document["events"]))

            existing = events.get(key)
            if existing is not None:
                if existing != clean_event:
                    raise ValueError(
                        "audit key was already used with a different event")
                return dict(existing), False

            events[key] = clean_event
            # Seal the whole sorted key sequence: a key landing between
            # existing ones changes its successor's predecessor, so every
            # digest from the insertion point on is recomputed -- earlier
            # ones recompute to themselves. A version 1 journal is
            # upgraded by the same commit, its old events sealed into the
            # new chain.
            chain: dict[str, Any] = {}
            previous: str | None = None
            for event_key in sorted(events):
                digest = _event_digest(event_key, events[event_key],
                                       previous)
                chain[event_key] = [events[event_key], previous, digest]
                previous = digest
            directory = os.path.dirname(realpath) or "."
            _commit(realpath, directory, _serialize(chain), old_bytes,
                    fault)
            return dict(clean_event), True


def _read_existing_document(realpath: str) -> dict[str, Any]:
    # Read-only path shared by get and search: the shared flock is held
    # only while the file is opened and read, so a query racing a writer
    # observes either the complete previous document or the complete new
    # one. A missing journal surfaces as FileNotFoundError; malformed
    # UTF-8/JSON (negative-zero literals included), an unsupported
    # version, any structural or order deviation, or an incomplete digest
    # chain raises ValueError -- the document is never reordered and
    # accepted, and never rewritten.
    with _file_lock(realpath, shared=True):
        with open(realpath, "rb") as handle:
            raw = handle.read()

    document = _parse_document(realpath, raw)
    _ensure_chain(document)
    return document


def get(path: str, key: str) -> dict[str, Any]:
    """Return a read-only copy of the audit event stored under ``key``.

    ``path`` and ``key`` must be non-empty strings, else ``ValueError``.
    A missing journal file raises ``FileNotFoundError``; malformed JSON or
    UTF-8, a negative-zero literal, an unsupported version or structure,
    or an incomplete digest chain raises ``ValueError``; an unknown key
    raises ``KeyError(key)``; any other locking or I/O failure raises
    ``OSError``. The query never writes and holds the journal's companion
    lock shared only while the file is opened and read, so it can only
    observe a complete document. The returned dict is a fresh copy with
    fields op, target, key, changed, error and stage in that order;
    mutating it never affects the journal.
    """
    for value in (path, key):
        if not isinstance(value, str) or not value:
            raise ValueError("path and key must be non-empty strings")

    realpath = os.path.realpath(path)
    document = _read_existing_document(realpath)

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
    cursor_or_none}``: each event is a fresh copy with the public field
    order, and ``next`` is the audit key of the page's last item when
    further matching events remain, else ``None``. With no matching
    events the page is empty and ``next`` is ``None``.

    The query is strictly read-only: a missing journal raises
    ``FileNotFoundError``; malformed JSON or UTF-8, a negative-zero
    literal, or an unsupported version, structure, order or digest chain
    raises ``ValueError``; any other locking or I/O failure raises
    ``OSError``. It holds the journal's companion lock shared only while
    the file is opened and read, so a query racing a writer observes
    either the complete previous document or the complete new one.
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
    """Verify the integrity of the audit journal stored at ``path``.

    ``path`` must be a non-empty string, else ``ValueError``. A missing
    journal raises ``FileNotFoundError``; malformed JSON or UTF-8, a
    negative-zero literal, or an unsupported version, field order or
    structure raises ``ValueError``; any other locking or I/O failure
    raises ``OSError``. The query never writes and holds the journal's
    companion lock shared only while the file is opened and read.

    Returns ``version``, ``count``, ``sealed``, ``valid``,
    ``first_invalid`` and ``head``, in that order. A well-formed version
    1 journal carries no chain: ``count`` is its real event count,
    ``sealed`` and ``valid`` are false and ``first_invalid`` and
    ``head`` are null. A sealed (version 2) journal whose chain is
    complete reports ``sealed`` and ``valid`` true, ``first_invalid``
    null and ``head`` the root digest the file declares. A chain
    deviation never raises: ``valid`` is false and ``first_invalid`` is
    the zero-based position of the first event whose digest does not
    match its content or whose predecessor reference is wrong, or the
    event count when only the root head disagrees with the last event's
    digest.
    """
    if not isinstance(path, str) or not path:
        raise ValueError("path must be a non-empty string")

    realpath = os.path.realpath(path)
    with _file_lock(realpath, shared=True):
        with open(realpath, "rb") as handle:
            raw = handle.read()
    document = _parse_document(realpath, raw)

    events = document["events"]
    if document["version"] == 1:
        return {"version": 1, "count": len(events), "sealed": False,
                "valid": False, "first_invalid": None, "head": None}
    first_invalid = _chain_first_invalid(document)
    return {"version": 2, "count": len(events), "sealed": True,
            "valid": first_invalid is None, "first_invalid": first_invalid,
            "head": document["head"]}
