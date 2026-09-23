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
are written to a backup at the fixed recovery path
``target + ".recovery"`` in the same directory -- a same-directory
temporary is written, fsynced and atomically replaced onto that path
(which must not already exist: a present recovery file raises
``FileExistsError`` and writes nothing), followed by a directory fsync;
only then are the new bytes written to a commit temporary, fsynced,
moved over the target with :func:`os.replace`, and followed by a
directory fsync; the recovery copy is unlinked and its removal
directory-synced only after every one of those steps succeeded. If any
step fails -- including the recovery backup itself, the commit
temporary, the replace, or the directory open/fsync that follows it --
the old target is left untouched when the replace has not happened yet;
after the replace has happened, the old bytes are moved back over the
target with :func:`os.replace` from the recovery copy and the directory
synced again, all before the target lock is released, so a shared-lock
reader can only ever observe the complete pre-call target or the
complete new one. When the pre-call target did not exist, a failed
commit deletes the new target instead.

The recovery copy at ``target + ".recovery"`` survives even a failed
call -- it is removed only when the whole overwrite fully succeeds --
so the old bytes always remain available to
:mod:`carbon_market.history_recovery` (which can restore them) or to
manual recovery. Should the automatic rollback itself fail, that
``OSError`` is raised with the first error chained as its
``__cause__``, and whatever still holds the old bytes (the recovery
copy, or the restored target) is left in place.

Both locks are kernel flocks: they block other processes (and other
opens of the same lock file in this process), and the kernel releases
them automatically when the process exits, so a leftover lock file never
blocks a later copy or query. The source is never written.
"""

from __future__ import annotations

import contextlib
import os
import tempfile

from . import history as _history
from . import recover_all as _recover_all
from ._jsonio import strict_loads

__all__ = ["run"]

#: Suffix of the fixed recovery-copy path created on overwrite.
RECOVERY_SUFFIX = ".recovery"

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


def _stage_at(dest: str, directory: str, payload: bytes, *,
              prefix: str, retain_on_failure: bool,
              fsync_stage: str, replace_stage: str) -> None:
    # Write payload to a same-directory temporary, fsync it and move it
    # onto dest atomically. When this is itself a recovery move, a
    # failure leaves the temporary (a best-effort copy of the old bytes)
    # on disk instead of deleting it.
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=prefix, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            _maybe_fault(fsync_stage)
            os.fsync(handle.fileno())
        _maybe_fault(replace_stage)
        os.replace(tmp_path, dest)
    except BaseException:
        # A normal staging attempt removes its partial temporary; a
        # recovery staging attempt leaves it on disk, since it may
        # itself be the last on-disk copy of the old bytes.
        if not retain_on_failure:
            _unlink_quiet(tmp_path)
        raise


def _rollback(target_real: str, directory: str, existed: bool,
              old_bytes: bytes, recovery_path: str | None, replaced: bool,
              first: BaseException) -> None:
    # Restore the state the target held when the copy started, while the
    # target lock is still held exclusively: after a committed replace,
    # move the recovery copy back over the target when it is still on
    # disk, and re-stage the pre-call bytes from the in-memory copy when
    # the failure happened only after the recovery copy was unlinked;
    # before the replace the old target is by construction still in
    # place (only stale commit temps need clearing, handled by the
    # caller); when the target did not exist beforehand, remove what
    # this call created. Any failure of this recovery is raised with the
    # first error chained as its __cause__, and whatever still carries
    # the old bytes (the recovery copy, or a recovery temporary) is left
    # in place for manual recovery.
    try:
        if replaced:
            if recovery_path is not None and os.path.exists(recovery_path):
                _maybe_fault("rollback_replace")
                os.replace(recovery_path, target_real)
            elif existed:
                _stage_at(target_real, directory, old_bytes,
                          prefix=".history-copy-restore-",
                          retain_on_failure=True,
                          fsync_stage="restore_fsync",
                          replace_stage="restore_replace")
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
                 overwrite: bool) -> None:
    existed = os.path.exists(target_real)
    if existed and not overwrite:
        raise FileExistsError(
            f"history file already exists: {target_real!r}")

    # The target's pre-call bytes, kept in memory as a last-resort copy
    # even though the durable recovery file normally carries them.
    old_bytes = b""
    if existed:
        with open(target_real, "rb") as old:
            old_bytes = old.read()

    # Phase 1: when overwriting, first persist an fsynced byte-for-byte
    # recovery copy of the target's pre-call bytes at the fixed path
    # target + ".recovery", via a same-directory temporary, atomic
    # replace and directory fsync, so the copy itself is crash-durable.
    # A recovery copy left by a previous (failed) call is never
    # overwritten silently: its presence raises FileExistsError before
    # anything is written. The target is not modified yet, so a failure
    # here only discards this attempt's temporary.
    recovery_path: str | None = None
    if existed:
        recovery_path = target_real + RECOVERY_SUFFIX
        if os.path.lexists(recovery_path):
            raise FileExistsError(
                f"recovery copy already exists: {recovery_path!r}")
        # The target itself is not touched yet. A failure while staging
        # only discards that attempt's temporary (handled in _stage_at);
        # once the recovery copy has landed at the fixed path it is left
        # there on a directory-sync failure -- it is deleted only after
        # the whole overwrite succeeds -- so the old bytes stay
        # recoverable and a later call trips the already-exists
        # FileExistsError instead of overwriting them.
        _stage_at(recovery_path, directory, old_bytes,
                  prefix=".history-copy-recovery-",
                  retain_on_failure=False,
                  fsync_stage="recovery_fsync",
                  replace_stage="recovery_replace")
        _fsync_dir(directory, "recovery_dir_open",
                   "recovery_dir_fsync")

    # Phase 2: stage the new bytes in a same-directory temporary, fsync
    # them, then atomically move them over the target. Until the replace
    # succeeds the target still holds the pre-call bytes; a failure only
    # discards this attempt's temporary. The recovery copy is never
    # touched on a failure -- it is removed only once the whole
    # overwrite has fully succeeded -- so it always remains available
    # for carbon_market.history_recovery.restore.
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".history-copy-", suffix=".tmp")
    replaced = False
    try:
        with os.fdopen(tmp_fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            _maybe_fault("temp_fsync")
            os.fsync(handle.fileno())
        _maybe_fault("replace")
        os.replace(tmp_path, target_real)
        replaced = True
    except BaseException:
        _unlink_quiet(tmp_path)
        raise

    # Phase 3: persist the replace. Failure here -- opening or fsyncing
    # the directory included -- must restore the pre-call bytes before
    # the target lock is released.
    try:
        _fsync_dir(directory, "commit_dir_open", "commit_dir_fsync")
    except BaseException as first:
        _rollback(target_real, directory, existed, old_bytes,
                  recovery_path, replaced, first)
        raise

    # Phase 4: the new target is complete and durable; only now drop the
    # recovery copy and sync its removal. Any failure restores the
    # pre-call bytes (the recovery copy is moved back over the target; if
    # it is already gone, the bytes are re-staged from memory) before the
    # lock is released, so a failed call never leaves the new version
    # readable.
    if recovery_path is not None:
        try:
            _maybe_fault("cleanup_unlink")
            os.unlink(recovery_path)
            _fsync_dir(directory, "final_dir_open", "final_dir_fsync")
        except BaseException as first:
            _rollback(target_real, directory, existed, old_bytes,
                      recovery_path, replaced, first)
            raise


def run(
    source: str,
    target: str,
    key: str,
    overwrite: bool = False,
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
    replaces it atomically: the old target bytes are first persisted to a
    recovery copy at the fixed path ``target + ".recovery"`` (a
    same-directory temporary is fsynced, atomically replaced onto that
    path and followed by a directory fsync); if that path already exists
    ``FileExistsError`` is raised and nothing is written. The new bytes
    are then written to a temporary file and fsynced, moved over the
    target with :func:`os.replace` and followed by a directory fsync, and
    the recovery copy is deleted with one more directory fsync only once
    all of that succeeded. A failure at any of those steps -- the
    post-replace directory open/fsync included -- restores the target's
    pre-call bytes (or deletes the new target when none existed) and
    removes this attempt's temporary files before the target lock is
    released; the recovery copy is retained unless the old target was
    never at risk (a failure before the replace), and if the rollback or
    cleanup itself fails, that ``OSError`` is raised with the first error
    chained as its ``__cause__`` and a recoverable copy of the old bytes
    is retained. After a failed overwrite a history query
    (``get``/``verify``) therefore only ever reads the complete old
    version, and a retained ``target + ".recovery"`` can be restored with
    :func:`carbon_market.history_recovery.restore`.

    On success the result carries keys key, count, statuses and terminal,
    in that order, with the values :func:`carbon_market.history.verify`
    reports for the source of this copy.
    """
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
    # (on overwrite) the recovery copy and the rollback are all one
    # critical section, so a shared-lock history query on the target sees
    # either the complete old file or the complete copy. The original
    # bytes are written verbatim -- never re-serialized -- so the target
    # is byte-identical to the source that was validated.
    with _recover_all._history_file_lock(target_real):
        _commit_copy(target_real, directory, raw, overwrite)

    entries = history["snapshots"]
    statuses = [entry_status for entry_status, _coord in entries]
    terminal = bool(entries) and entries[-1][0] in _history._TERMINAL_STATUSES
    return {
        "key": history["key"],
        "count": len(entries),
        "statuses": statuses,
        "terminal": terminal,
    }
