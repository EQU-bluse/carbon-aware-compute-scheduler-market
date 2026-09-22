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

When the target already exists and ``overwrite=True``, the old target
bytes are first staged as a backup file in the target's own directory
and made durable (backup fsync followed by a directory fsync) before the
new bytes are written anywhere. Only once the new content sits in place
atomically (``os.replace`` plus directory fsync) is the backup deleted,
followed by one more directory fsync. Any failure after the target was
replaced -- including a failure to open or fsync the directory, or to
delete the backup -- rolls the target back to its exact pre-call bytes
(still under the exclusive lock, from a freshly synced restore file) and
removes this attempt's temporary files; when the original target did not
exist, rollback removes the new target instead. If a rollback or cleanup
step itself fails, that ``OSError`` is raised with the original error on
its exception chain and a recoverable copy of the old bytes is left on
disk. A shared-lock reader therefore only ever observes the complete old
target or the complete new one, and after a failed call reads the
complete previous version. The source is never written.

Both locks are kernel flocks: they block other processes (and other
opens of the same lock file in this process), and the kernel releases
them automatically when the process exits, so a leftover lock file never
blocks a later copy or query.
"""

from __future__ import annotations

import contextlib
import os
import tempfile

from . import history as _history
from . import recover_all as _recover_all
from ._jsonio import strict_loads

__all__ = ["run"]


def _fsync_directory(directory: str) -> None:
    # Sync the directory's entry set so a crash cannot resurrect a
    # replaced/unlinked name or lose a freshly created one.
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _stage_bytes(
    directory: str, prefix: str, suffix: str, raw: bytes
) -> str:
    # Create ``raw`` in a new file in ``directory`` and return its name
    # only once its bytes are closed and fsynced. A failure at any point
    # removes the partial file before the error propagates.
    fd, path = tempfile.mkstemp(dir=directory, prefix=prefix, suffix=suffix)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(path)
        raise
    return path


def _unlink_if_exists(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def _restore_old_target(
    target_real: str, directory: str, raw_old: bytes
) -> None:
    # Put the complete pre-call bytes back at the target while the backup
    # of them stays untouched: stage raw_old in its own synced file,
    # replace it into place and sync the directory. If this fails, the
    # backup remains a recoverable old-bytes copy.
    restore_path = _stage_bytes(
        directory, ".history-copy-restore-", ".tmp", raw_old)
    try:
        os.replace(restore_path, target_real)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(restore_path)
        raise
    _fsync_directory(directory)


def _rollback(
    *,
    target_real: str,
    directory: str,
    existed: bool,
    replaced: bool,
    raw_old: bytes,
    backup_path: str | None,
    tmp_path: str | None,
) -> None:
    # Runs while the target's exclusive companion lock is still held.
    # Either it restores the exact pre-call state (old bytes at the
    # target, no attempt artifacts in the directory), or it raises the
    # OSError that prevented that -- in which case a synced backup of the
    # old bytes is deliberately left on disk where one exists.
    if not existed:
        # The pre-call state is absence: remove the new target once the
        # replace happened, otherwise just the staged temp.
        if replaced:
            os.unlink(target_real)
        elif tmp_path is not None:
            _unlink_if_exists(tmp_path)
        _fsync_directory(directory)
        return

    if replaced:
        # The target currently holds the new bytes; write the old bytes
        # back from a freshly synced restore file (never touching the
        # backup) before any cleanup. A failure here propagates with the
        # backup intact.
        _restore_old_target(target_real, directory, raw_old)

    # The target is (or was never changed away from being) the complete
    # old version. Remove this attempt's files and sync their removal.
    if tmp_path is not None:
        _unlink_if_exists(tmp_path)
    if backup_path is not None:
        _unlink_if_exists(backup_path)
    _fsync_directory(directory)


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
    replaces it atomically. An overwrite first stages the previous target
    bytes as a synced backup in the target's directory (backup fsync and
    directory fsync); the new bytes are written to a synced temporary
    file, installed with ``os.replace`` and a directory fsync, and the
    backup is deleted only then, followed by another directory fsync. Any
    failure -- including after the replace, when opening or fsyncing the
    directory or deleting the backup -- restores the exact pre-call bytes
    before the target lock is released (or deletes the target if it did
    not exist beforehand) and removes the attempt's temporary files;
    ``history.get``/``history.verify`` afterwards read the complete old
    version. If rollback or cleanup itself fails, that ``OSError`` is
    raised with the first error chained and a recoverable copy of the old
    bytes is left in place.

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
    # the backup, the byte-for-byte write, the atomic replace, both
    # directory syncs and the backup removal are all one critical section,
    # so a shared-lock history query on the target sees either the
    # complete old file or the complete copy. The original bytes are
    # written verbatim -- never re-serialized -- so the target is
    # byte-identical to the source that was validated.
    with _recover_all._history_file_lock(target_real):
        existed = os.path.exists(target_real)
        if existed and not overwrite:
            raise FileExistsError(
                f"history file already exists: {target_real!r}")
        if not os.path.isdir(directory):
            raise FileNotFoundError(
                f"target parent directory not found: {directory!r}")

        raw_old = b""
        if existed:
            with open(target_real, "rb") as handle:
                raw_old = handle.read()

        backup_path: str | None = None
        tmp_path: str | None = None
        replaced = False
        try:
            if existed:
                # Durable old-bytes backup before anything new is written:
                # it survives even a process crash between here and commit.
                backup_path = _stage_bytes(
                    directory, ".history-copy-backup-", ".bak", raw_old)
                _fsync_directory(directory)

            tmp_path = _stage_bytes(
                directory, ".history-copy-", ".tmp", raw)
            os.replace(tmp_path, target_real)
            tmp_path = None
            replaced = True
            _fsync_directory(directory)

            if existed:
                # Commit point reached: new bytes are durable at the
                # target. Only now may the old-bytes backup go away.
                os.unlink(backup_path)
                backup_path = None
                _fsync_directory(directory)
        except BaseException as first_exc:
            try:
                _rollback(
                    target_real=target_real,
                    directory=directory,
                    existed=existed,
                    replaced=replaced,
                    raw_old=raw_old,
                    backup_path=backup_path,
                    tmp_path=tmp_path,
                )
            except OSError as rollback_exc:
                raise rollback_exc from first_exc
            raise

    entries = history["snapshots"]
    statuses = [entry_status for entry_status, _coord in entries]
    terminal = bool(entries) and entries[-1][0] in _history._TERMINAL_STATUSES
    return {
        "key": history["key"],
        "count": len(entries),
        "statuses": statuses,
        "terminal": terminal,
    }
