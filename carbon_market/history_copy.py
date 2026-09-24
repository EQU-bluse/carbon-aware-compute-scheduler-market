"""Locked byte-for-byte copies of a coordinated recovery history.

Where :mod:`carbon_market.history` only reads a history and
:mod:`carbon_market.recover_all` only appends snapshots to the history
belonging to its own coordination file, this module copies one history
file onto another path -- for example to seed a new batch's history from
an existing one, or to publish a snapshot copy elsewhere -- without ever
re-serializing it. The source is opened under the same shared companion
lock (``source + ".lock"``) that :func:`carbon_market.history.get` and
:func:`carbon_market.history.verify` take, and its bytes are read in full
and the file closed before the lock is dropped; validation -- the
structural, consistency, negative-zero and key checks of
:func:`carbon_market.history.verify` -- then runs against those exact
bytes. The copy commits the validated bytes under an exclusive hold on
the target's own companion lock (``target + ".lock"``), via a temporary
file in the target's directory, fsync, atomic replace and directory
fsync. Because the bytes are copied verbatim instead of being parsed and
re-dumped, the target is byte-identical to the source, quirks of
whitespace and key order included.

When an existing target is overwritten, the commit is crash-safe as well
as atomic for readers: before the target is touched, its previous bytes
are written byte for byte to a fixed recovery file next to the target
(``target + ".recovery"``), created exclusively, and that file and the
directory are fsynced; only then are the new bytes written, fsynced,
moved over the target with :func:`os.replace` and followed by a
directory fsync. The recovery file is deleted (with one more directory
fsync) only after every one of those steps succeeded. A failure between
the synced recovery file and the atomic replace leaves the recovery file
in place beside the still-untouched target; a failure after the replace
-- the directory open or fsync that follows it, or the recovery unlink /
final directory sync -- puts the previous bytes back over the target:
the recovery file is moved back over it with :func:`os.replace` when it
is still there (the move consumes the copy, the bytes being back in
their original place), or re-staged from the in-memory copy when the
recovery file was already removed; the directory is synced again, all
before the target lock is released. A shared-lock reader can therefore
only ever observe the complete pre-call target or the complete new one.
When the pre-call target did not exist, a failed commit deletes the new
target instead. A leftover recovery file from a failure that never
reached the replace is itself a valid history and is consumed by
:func:`carbon_market.history_recovery.restore`. Should the rollback
itself fail, that ``OSError`` is raised with the first error chained as
its ``__cause__``, and whatever still holds the old bytes -- the recovery
file when the move back failed, or a recovery temporary -- is left in
place. A recovery file already present when a copy begins -- in any
overwrite mode -- makes the copy raise ``FileExistsError`` before
writing anything, so recovery bytes can never be silently overwritten.

Both locks are kernel flocks: they block other processes (and other
opens of the same lock file in this process), and the kernel releases
them automatically when the process exits, so a leftover lock file never
blocks a later copy or query. The source is never written.
"""

from __future__ import annotations

import contextlib
import os
import tempfile

from . import audit as _audit
from . import history as _history
from . import recover_all as _recover_all
from ._jsonio import strict_loads

__all__ = ["run"]

# Test-only fault injection point: when set to a callable, it is invoked
# with a stage name at every fsync/replace/unlink decision point of the
# commit protocol and may raise to simulate that step failing. Production
# code leaves this as None; it is not part of the public API.
_fault = None


def _maybe_fault(stage: str) -> None:
    fault = _fault
    if fault is not None:
        fault(stage)


def _fsync_dir(directory: str, open_stage: str, fsync_stage: str) -> None:
    _maybe_fault(open_stage)
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        _maybe_fault(fsync_stage)
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _unlink_quiet(path: str) -> None:
    with contextlib.suppress(OSError):
        os.unlink(path)


def _unlink_or_chain(path: str, first: BaseException) -> None:
    # Remove a throwaway temporary after an earlier failure; a second
    # OSError here is raised chained after the first one instead of being
    # swallowed or replacing it.
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    except OSError as cleanup:
        raise cleanup from first


def _stage_over(target_real: str, directory: str, payload: bytes,
                *, prefix: str, retain_on_failure: bool) -> None:
    # Write payload to a same-directory temporary, fsync it and move it
    # over the target atomically. When this is itself a recovery move, a
    # failure leaves the temporary (a best-effort copy of the old bytes)
    # on disk instead of deleting it.
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=prefix, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        _maybe_fault("restore_replace")
        os.replace(tmp_path, target_real)
    except BaseException:
        if not retain_on_failure:
            _unlink_quiet(tmp_path)
        raise


def _rollback(target_real: str, directory: str, existed: bool,
              old_bytes: bytes, recovery_path: str,
              first: BaseException) -> None:
    # Restore the bytes (or nonexistence) the target held when the copy
    # started, while the target lock is still held exclusively: when the
    # recovery file still exists, move it back over the target -- the move
    # consumes the copy, the old bytes being back in their original place;
    # if it was already removed (the only failure left was the directory
    # fsync after its unlink), re-stage the pre-call bytes from the copy
    # held in memory; when the target did not exist beforehand, remove
    # what this call created. Any failure of this recovery is raised
    # chained after the first error, and whatever still carries the old
    # bytes at that point -- the recovery file when the move back failed,
    # or a recovery temporary -- is left in place for
    # history_recovery.restore.
    try:
        if existed:
            if os.path.exists(recovery_path):
                _maybe_fault("rollback_replace")
                os.replace(recovery_path, target_real)
            else:
                _stage_over(target_real, directory, old_bytes,
                            prefix=".history-copy-restore-",
                            retain_on_failure=True)
            _fsync_dir(directory, "rollback_dir_open", "rollback_dir_fsync")
        else:
            _maybe_fault("rollback_unlink")
            try:
                os.unlink(target_real)
            except FileNotFoundError:
                pass
            _fsync_dir(directory, "rollback_dir_open", "rollback_dir_fsync")
    except OSError as recovery:
        raise recovery from first


def _commit_copy(target_real: str, directory: str, raw: bytes,
                 overwrite: bool) -> bool:
    """Commit the copy and return whether the target bytes really changed.

    The decision is made under the exclusive target lock: a brand-new
    target always changed, while an overwrite changed only when the new
    bytes differ from the pre-call bytes being replaced.
    """
    recovery_path = target_real + ".recovery"

    # Both existence decisions are made under the exclusive target lock
    # before anything is written. A leftover recovery file always blocks
    # the call, no matter the overwrite mode: it carries bytes of unknown
    # provenance and must never be silently replaced.
    if os.path.exists(recovery_path):
        raise FileExistsError(
            f"history recovery file already exists: {recovery_path!r}")
    existed = os.path.exists(target_real)
    if existed and not overwrite:
        raise FileExistsError(
            f"history file already exists: {target_real!r}")

    # The target's pre-call bytes, kept in memory so the recovery can
    # rebuild them even when the on-disk recovery file has already been
    # removed.
    old_bytes = b""
    if existed:
        with open(target_real, "rb") as old:
            old_bytes = old.read()

    # Phase 1: when overwriting, first persist an fsynced byte-for-byte
    # recovery file at the fixed target+".recovery" path, created
    # exclusively (the existence check above runs under the same lock),
    # plus a directory fsync so the recovery file itself is crash-durable.
    # The target is not modified yet. A failure while writing our own
    # recovery file only discards the partial file this call created; once
    # the recovery file is complete and synced it is removed only when the
    # whole copy succeeds, so a failure from the directory sync onward
    # leaves it in place.
    if existed:
        # O_EXCL makes the create exclusive: the existence check above and
        # this open run in one exclusive-lock critical section, so a
        # FileExistsError here means another writer raced the lock and is
        # surfaced unchanged without writing anything.
        recovery_fd = os.open(
            recovery_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
        try:
            with os.fdopen(recovery_fd, "wb") as recovery:
                recovery.write(old_bytes)
                recovery.flush()
                _maybe_fault("backup_fsync")
                os.fsync(recovery.fileno())
        except BaseException as first:
            _unlink_or_chain(recovery_path, first)
            raise
        _fsync_dir(directory, "backup_dir_open", "backup_dir_fsync")

    # Phase 2: stage the new bytes in a same-directory temporary, fsync
    # them, then atomically move them over the target. Until the replace
    # succeeds the target still holds the pre-call bytes, so a failure
    # only removes this attempt's temporary; the recovery file (if any)
    # is deliberately retained -- it is deleted only on full success.
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".history-copy-", suffix=".tmp")
    try:
        with os.fdopen(tmp_fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            _maybe_fault("temp_fsync")
            os.fsync(handle.fileno())
        _maybe_fault("replace")
        os.replace(tmp_path, target_real)
    except BaseException as first:
        _unlink_or_chain(tmp_path, first)
        raise

    # Phase 3: persist the replace. Failure here -- opening or fsyncing
    # the directory included -- must restore the pre-call bytes before
    # the target lock is released.
    try:
        _fsync_dir(directory, "commit_dir_open", "commit_dir_fsync")
    except BaseException as first:
        _rollback(target_real, directory, existed, old_bytes,
                 recovery_path, first)
        raise

    # Phase 4: the new target is complete and durable; only now drop the
    # recovery file and sync its removal. A failure here restores the
    # pre-call target bytes before the lock is released -- the recovery
    # file is moved back over it when still present, or the bytes are
    # re-staged from memory when it was already removed -- so a failed
    # call never leaves the new version readable. Only when that rollback
    # itself fails is a recovery copy left on disk (carried by the
    # recovery file or a recovery temporary), for
    # history_recovery.restore to consume.
    if existed:
        try:
            _maybe_fault("cleanup_unlink")
            os.unlink(recovery_path)
            _fsync_dir(directory, "final_dir_open", "final_dir_fsync")
        except BaseException as first:
            _rollback(target_real, directory, existed, old_bytes,
                     recovery_path, first)
            raise

    return not existed or old_bytes != raw


def run(
    source: str,
    target: str,
    key: str,
    overwrite: bool = False,
    audit_path: str | None = None,
    audit_key: str | None = None,
) -> dict[str, object]:
    """Copy the history at ``source`` onto ``target`` and return its summary.

    ``source``, ``target`` and ``key`` must be non-empty strings and
    ``overwrite`` a boolean, else ``ValueError``; if both paths resolve to
    the same real path, ``ValueError`` is raised before any lock is taken.
    A missing source file or missing target parent directory raises
    ``FileNotFoundError``; malformed JSON or UTF-8, a negative-zero
    literal, an unsupported version or structure, or an inconsistent
    history raises ``ValueError``; a source recorded under another
    idempotency key raises ``KeyError(key)``; any other locking or I/O
    failure raises ``OSError``.

    The source is fully read while holding ``source + ".lock"`` shared and
    validated only after it has been closed; the exact bytes read are then
    committed to the target while holding ``target + ".lock"`` exclusive.
    If the target already exists, ``overwrite=False`` raises
    ``FileExistsError`` and writes nothing, while ``overwrite=True``
    replaces it atomically: the old target bytes are first written to the
    fixed recovery file ``target + ".recovery"`` (created exclusively and
    fsynced together with its directory); a recovery file already present
    makes the call raise ``FileExistsError`` without writing anything.
    The new bytes are then written to a temporary file and fsynced, moved
    over the target with :func:`os.replace` and followed by a directory
    fsync, and the recovery file is deleted with one more directory fsync
    only once all of that succeeded. A failure at any of those steps --
    the post-replace directory open/fsync included -- restores the
    target's pre-call bytes (or deletes the new target when none existed)
    before the target lock is released and leaves the recovery file in
    place with the pre-call bytes; if the rollback or a cleanup itself
    fails, that ``OSError`` is raised with the first error chained as its
    ``__cause__`` and a recoverable copy of the old bytes is retained.
    After a failed overwrite a history query (``get``/``verify``)
    therefore only ever reads the complete old version, and
    :func:`carbon_market.history_recovery.restore` consumes a leftover
    recovery file.

    On success the result carries keys key, count, statuses and terminal,
    in that order, with the values :func:`carbon_market.history.verify`
    reports for the source of this copy.

    ``audit_path`` and ``audit_key`` are an optional pair that must be
    omitted together or both given as non-empty strings, else ``ValueError``
    before the copy begins. When given, the call appends one result event
    to the audit journal keyed by ``audit_key`` before returning or
    re-raising: a success event records op ``"copy"``, the target passed
    in, the history ``key`` and whether the target's bytes actually
    changed, with error and stage null; a failure event names the final
    operation exception's class and the stage it escaped from (``"校验"``
    for parameter, history-content, history-key or missing-file failures,
    ``"执行"`` for an existing target or a leftover recovery copy,
    ``"同步"`` for the first lock/temp/replace/unlink/fsync failure and
    ``"回滚"`` when a compensation cleanup or rollback then fails), with
    ``changed`` computed from whether the target bytes depart from their
    call-before state when the exception leaves. The original exception
    and its chain leave unchanged when the audit write succeeds; an audit
    failure of its own raises its public exception with the history result
    retained, chained after the operation error when the copy also failed.
    With the pair omitted the copy behaves byte-for-byte as before.
    """
    # The audit pair is checked before the operation begins: an unpaired
    # side, a non-string or an empty value raises before any validation,
    # lock or read, and is not itself recorded.
    audit = _audit.check_pair(audit_path, audit_key)
    # A failure event carries the target and history key verbatim and so
    # can only be appended once those two are valid non-empty strings;
    # every other parameter failure (bad source, bad overwrite, same
    # realpath) is still recorded below as a 校验 failure.
    can_emit = (audit is not None
                and isinstance(target, str) and bool(target)
                and isinstance(key, str) and bool(key))

    target_real = os.path.realpath(target) if can_emit else None
    # Snapshot the target's call-before bytes (None when absent) so a
    # failure event can report whether the target really departed from
    # this state by the time the exception left the call.
    before = _audit.snapshot(target_real) if can_emit else None

    try:
        result, changed = _run_copy(source, target, key, overwrite)
    except BaseException as exc:
        if can_emit:
            _record_failure(audit, target, key, target_real, before, exc)
        raise
    if can_emit:
        # Use the in-lock change decision for success: it is the exact
        # result of the commit, free of any writer that might race the
        # target after its lock was released.
        _audit.emit(audit[0], audit[1], op="copy", target=target, key=key,
                    changed=changed, error=None, stage=None)
    return result


def _run_copy(
    source: str,
    target: str,
    key: str,
    overwrite: bool,
) -> tuple[dict[str, object], bool]:
    for value in (source, target, key):
        if not isinstance(value, str) or not value:
            raise ValueError(
                "source, target and key must be non-empty strings")
    if not isinstance(overwrite, bool):
        raise ValueError("overwrite must be a boolean")

    source_real = os.path.realpath(source)
    target_real = os.path.realpath(target)
    if source_real == target_real:
        raise ValueError(
            f"source and target resolve to the same path: {source_real!r}")

    # Read the source bytes under the same shared hold a read-only history
    # query takes, from before the file is opened until it has been fully
    # read and closed. A concurrent recover_all.run (exclusive holder) can
    # therefore never expose a truncated or half-replaced file; the bytes
    # buffered here are one complete pre- or post-update history. Parsing
    # and validation follow only after the hold is released, exactly as in
    # carbon_market.history._load -- the in-memory bytes are already fixed.
    with _recover_all._history_file_lock(source_real, shared=True):
        with open(source_real, "rb") as handle:
            raw = handle.read()

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"history file {source_real!r} is not valid UTF-8") from exc
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"history file {source_real!r} is not valid JSON") from exc
    history = _recover_all._validate_history(data)
    _history._validate_consistency(data, history)
    if history["key"] != key:
        raise KeyError(key)

    directory = os.path.dirname(target_real) or "."

    # Commit under the target's own exclusive lock: the existence decision,
    # the byte-for-byte write, the atomic replace, the directory sync and
    # (on overwrite) the recovery file and the rollback are all one
    # critical section, so a shared-lock history query on the target sees
    # either the complete old file or the complete copy. The original
    # bytes are written verbatim -- never re-serialized -- so the target
    # is byte-identical to the source that was validated.
    with _recover_all._history_file_lock(target_real):
        changed = _commit_copy(target_real, directory, raw, overwrite)

    entries = history["snapshots"]
    statuses = [entry_status for entry_status, _coord in entries]
    terminal = bool(entries) and entries[-1][0] in _history._TERMINAL_STATUSES
    result = {
        "key": history["key"],
        "count": len(entries),
        "statuses": statuses,
        "terminal": terminal,
    }
    return result, changed


def _record_failure(
    audit: tuple[str, str],
    target: str,
    key: str,
    target_real: str,
    before: bytes | None,
    exc: BaseException,
) -> None:
    # Append the failure outcome without touching the original exception:
    # changed is whether the target bytes depart from the call-before
    # snapshot at the moment the exception leaves; a failure of the audit
    # write itself is raised chained after the operation error, the
    # on-disk history result already formed being left in place.
    changed = _audit.snapshot(target_real) != before
    try:
        _audit.emit(audit[0], audit[1], op="copy", target=target, key=key,
                    changed=changed, error=type(exc).__name__,
                    stage=_audit.stage_for(exc))
    except BaseException as audit_exc:
        raise audit_exc from exc
