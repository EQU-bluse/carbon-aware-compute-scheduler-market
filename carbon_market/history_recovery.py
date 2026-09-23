"""Restore a coordinated recovery history from its recovery copy.

When :func:`carbon_market.history_copy.run` overwrites a history it
first persists the target's pre-call bytes at the fixed path
``target + ".recovery"``; that copy is deleted only once the overwrite
has fully succeeded, so after a failed (or interrupted) overwrite it
may be left on disk next to a target that never advanced. This module
puts it back.

:func:`restore` holds the target's companion lock
(``target + ".lock"``) exclusively for the whole operation. With no
recovery copy present it only verifies the target exactly as
:func:`carbon_market.history.verify` would and returns the summary
together with ``False``: the target bytes are unchanged. With a
recovery copy present, the copy is verified the same way and its exact
bytes are staged through a same-directory temporary -- written, fsynced,
moved over the target with :func:`os.replace` and directory-synced --
after which the recovery copy is unlinked and its removal
directory-synced. The return then reports whether the target's bytes
actually differ from what it held before the call.

Any failure after the target has been replaced restores, all while the
lock is still held, the exact pre-call state: the pre-call target bytes
are staged back over the target (or the target is removed when it did
not exist) and an already-unlinked recovery copy is recreated from its
captured bytes, every restore followed by a directory fsync. Should
that rollback itself fail, its ``OSError`` is raised with the first
error chained as its ``__cause__``. Only the Python standard library is
used; nothing here serves HTTP.
"""

from __future__ import annotations

import os
import tempfile

from . import history as _history
from . import recover_all as _recover_all
from ._jsonio import strict_loads

__all__ = ["restore"]

#: Optional fault points accepted by :func:`restore`.
_FAULTS = ("after_replace", "after_unlink")


def _fsync_dir_plain(directory: str) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _stage(dest: str, directory: str, payload: bytes, *,
           prefix: str) -> None:
    # Write payload to a same-directory temporary, fsync it and move it
    # onto dest atomically; a failure removes the partial temporary.
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=prefix, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, dest)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _verify_raw(path: str, raw: bytes, key: str) -> dict[str, object]:
    # The exact validation chain of carbon_market.history.verify run
    # against bytes already read under the exclusive target lock (taking
    # the shared companion lock here would self-conflict with it).
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"history file {path!r} is not valid UTF-8") from exc
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"history file {path!r} is not valid JSON") from exc
    history = _recover_all._validate_history(data)
    _history._validate_consistency(data, history)
    if history["key"] != key:
        raise KeyError(key)

    entries = history["snapshots"]
    statuses = [entry_status for entry_status, _coord in entries]
    terminal = bool(entries) and entries[-1][0] in _history._TERMINAL_STATUSES
    return {
        "key": history["key"],
        "count": len(entries),
        "statuses": statuses,
        "terminal": terminal,
    }


def _read_and_verify(path: str, key: str) -> tuple[bytes, dict[str, object]]:
    # Lock, open and read failures surface unchanged -- OSError,
    # including FileNotFoundError for a missing file -- exactly as
    # history.verify does.
    with open(path, "rb") as handle:
        raw = handle.read()
    return raw, _verify_raw(path, raw, key)


def _rollback(directory: str, target_real: str, recovery_path: str,
              tmp_path: str, target_existed: bool, old_target: bytes,
              payload: bytes, replaced: bool, recovery_unlinked: bool,
              first: BaseException) -> None:
    # Reconstruct the exact pre-call file system state while still
    # holding the target lock exclusively: discard the staging temp,
    # recreate an unlinked recovery copy durably, and return a replaced
    # target to its pre-call bytes (or non-existence). A single
    # directory fsync after all of that syncs every name change (all
    # files live in this one directory). Any OSError here is raised
    # chained after the error that triggered the rollback, and whatever
    # bytes were already restored are left on disk.
    try:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        if recovery_unlinked:
            _stage(recovery_path, directory, payload,
                   prefix=".history-recovery-copy-")
        if replaced:
            if target_existed:
                _stage(target_real, directory, old_target,
                       prefix=".history-recovery-target-")
            else:
                try:
                    os.unlink(target_real)
                except FileNotFoundError:
                    pass
        _fsync_dir_plain(directory)
    except OSError as recovery:
        raise recovery from first


def restore(
    target: str,
    key: str,
    fault: str | None = None,
) -> tuple[dict[str, object], bool]:
    """Restore the history at ``target`` from ``target + ".recovery"``.

    ``target`` and ``key`` must be non-empty strings and ``fault`` must
    be ``None`` or one of ``"after_replace"`` and ``"after_unlink"``,
    else ``ValueError``; validation errors follow
    :func:`carbon_market.history.verify` -- a missing file raises
    ``FileNotFoundError``, malformed JSON or UTF-8, a negative-zero
    literal, an unsupported version or structure, or an inconsistent
    history raises ``ValueError``, and a history recorded under another
    idempotency key raises ``KeyError(key)``; any locking or other I/O
    failure raises ``OSError``.

    The whole operation holds ``target + ".lock"`` exclusively. Without
    a recovery copy the target is only verified and returned as
    ``(summary, False)``; nothing is written. With a recovery copy it is
    verified, its bytes are written to a same-directory temporary,
    fsynced, moved over the target with :func:`os.replace` and
    directory-synced, the recovery copy is unlinked and that removal is
    directory-synced, and the call returns ``(summary, changed)`` where
    ``changed`` says whether the target's bytes differ from its
    pre-call bytes (a target that did not exist always counts as
    changed).

    ``fault`` simulates a crash at one decision point: ``"after_replace"``
    raises ``OSError`` right after the target is replaced (before its
    directory fsync) and ``"after_unlink"`` right after the recovery
    copy is unlinked (before its directory fsync). On any failure the
    target and the recovery copy are rolled back, under the lock, to
    their exact pre-call bytes or non-existence and synced; if that
    rollback fails too, its ``OSError`` is raised with the first error
    chained as its ``__cause__``.
    """
    if not isinstance(target, str) or not target:
        raise ValueError("target must be a non-empty string")
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a non-empty string")
    if fault is not None and fault not in _FAULTS:
        raise ValueError(
            "fault must be None, 'after_replace' or 'after_unlink'")

    target_real = os.path.realpath(target)
    recovery_path = target_real + ".recovery"
    directory = os.path.dirname(target_real) or "."

    with _recover_all._history_file_lock(target_real):
        if not os.path.lexists(recovery_path):
            # No copy to restore from: the read and the verify checks
            # happen under the exclusive hold, which is at least as
            # strong as the shared hold history.verify takes.
            with open(target_real, "rb") as handle:
                raw = handle.read()
            summary = _verify_raw(target_real, raw, key)
            return summary, False

        payload, summary = _read_and_verify(recovery_path, key)

        target_existed = os.path.lexists(target_real)
        old_target = b""
        if target_existed:
            with open(target_real, "rb") as handle:
                old_target = handle.read()
        changed = not target_existed or old_target != payload

        replaced = False
        recovery_unlinked = False
        fd, tmp_path = tempfile.mkstemp(
            dir=directory, prefix=".history-recovery-", suffix=".tmp")
        try:
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp_path, target_real)
                replaced = True
                if fault == "after_replace":
                    raise OSError("injected I/O failure after replace")
                _fsync_dir_plain(directory)

                os.unlink(recovery_path)
                recovery_unlinked = True
                if fault == "after_unlink":
                    raise OSError("injected I/O failure after unlink")
                _fsync_dir_plain(directory)
            except BaseException as first:
                _rollback(directory, target_real, recovery_path,
                          tmp_path, target_existed, old_target, payload,
                          replaced, recovery_unlinked, first)
                raise
        finally:
            # On the success path the staging temp became the target via
            # replace; on failure the rollback already removed it. This
            # only catches a temp left by a failure outside the rollback.
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    return summary, changed
