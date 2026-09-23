"""Restore a coordinated recovery history from its ``.recovery`` copy.

When :func:`carbon_market.history_copy.run` overwrites a history, it
first writes the pre-call bytes byte for byte to a fixed recovery file
next to the target (``target + ".recovery"``), fsynced together with its
directory. A failure before the new bytes replace the target leaves that
recovery file in place beside the untouched target; a failure after the
replace normally moves the recovery file back over the target, but a
failure that cannot be fully cleaned up can also leave one behind. This
module is the supported way to consume such a file: :func:`restore`
replaces a history with the bytes its recovery file carries, validating
them with the same checks :func:`carbon_market.history.verify` performs
first, and removes the recovery file only once the replacement is
durable.

The whole restore runs under one exclusive hold on
``target + ".lock"``. With no recovery file present, ``restore`` only
verifies the target itself (from the bytes read under that lock) and
reports it unchanged. With one present, the recovery bytes are verified
against the given key with the exact checks verify performs, written to
a same-directory temporary, fsynced and moved over the target with
:func:`os.replace`; the recovery file is then unlinked and each change
is followed by a directory fsync, so a shared-lock history reader can
only ever observe the complete pre-call target or the complete restored
one. If any step of that switch fails -- including the injected
``after_replace``/``after_unlink`` test faults -- the target and the
recovery file are restored, still under the lock, to exactly their bytes
(or nonexistence) when the call began, synced to disk, and the first
error is raised; should that rollback itself fail, its ``OSError`` is
raised with the first error chained as its ``__cause__``. Validation
failures are not rolled back -- nothing has been written yet -- and
follow the :func:`carbon_market.history.verify` contract: ``ValueError``
for malformed or inconsistent bytes and ``KeyError(key)`` for the wrong
idempotency key.

The lock is the same kernel flock the other history operations use; the
kernel releases it automatically when the process exits, so a leftover
lock file never blocks a later restore or query. The recovery file is
itself a complete history, so it can also be previewed directly with
:func:`carbon_market.history.verify` before a restore -- that preview
takes the recovery file's own companion lock, not the target's.
"""

from __future__ import annotations

import os
import tempfile

from . import history as _history
from . import recover_all as _recover_all
from ._jsonio import strict_loads

__all__ = ["restore"]

_FAULTS = (None, "after_replace", "after_unlink")


def _fsync_dir(directory: str) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _summary(history: dict[str, object]) -> dict[str, object]:
    entries = history["snapshots"]
    statuses = [entry_status for entry_status, _coord in entries]
    terminal = bool(entries) and entries[-1][0] in _history._TERMINAL_STATUSES
    return {
        "key": history["key"],
        "count": len(entries),
        "statuses": statuses,
        "terminal": terminal,
    }


def _verify_bytes(label_path: str, raw: bytes,
                  key: str) -> dict[str, object]:
    # Run the exact checks of carbon_market.history.verify against bytes
    # already read under the target's exclusive lock: a second flock on
    # the same lock file via another fd would self-conflict, so verify
    # cannot be called on the target from inside this critical section.
    # The decoding, strict-JSON, structural, consistency and key checks
    # are the same ones history._load performs, and the raised errors
    # follow verify exactly: a bad UTF-8 decode propagates as the raw
    # UnicodeDecodeError (itself a ValueError), malformed JSON is wrapped
    # in verify's ValueError, other structural/consistency problems raise
    # ValueError and another idempotency key raises KeyError(key).
    text = raw.decode("utf-8")
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"history file {label_path!r} is not valid JSON") from exc
    history = _recover_all._validate_history(data)
    _history._validate_consistency(data, history)
    if history["key"] != key:
        raise KeyError(key)
    return _summary(history)


def _read_existing(path: str) -> bytes | None:
    # None records that the file did not exist; a file's bytes may
    # themselves be empty in principle, so an empty b"" is not the same
    # answer.
    try:
        with open(path, "rb") as handle:
            return handle.read()
    except FileNotFoundError:
        return None


def _stage_over(path: str, directory: str, payload: bytes) -> None:
    # Write payload to an fsynced same-directory temporary and move it over
    # path atomically; a failure removes this attempt's temporary.
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".history-restore-", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise


def _rollback(target_real: str, recovery_path: str, directory: str,
              target_before: bytes | None, recovery_before: bytes | None,
              first: BaseException) -> None:
    # Under the still-held target lock, put the target and the recovery
    # file back exactly as they were when the call began (bytes, or
    # nonexistence) and fsync the directory so the rollback is durable.
    # The target always holds post-switch bytes at this point and must be
    # put back (or removed); the recovery file is only rewritten when the
    # switch already unlinked it -- after the after_replace fault it is
    # still intact and must not be touched. A failure of the rollback
    # itself is raised chained after the first error, with whatever still
    # carries the bytes left in place.
    try:
        if target_before is None:
            try:
                os.unlink(target_real)
            except FileNotFoundError:
                pass
        else:
            _stage_over(target_real, directory, target_before)
        if recovery_before is not None and not os.path.exists(recovery_path):
            _stage_over(recovery_path, directory, recovery_before)
        elif recovery_before is None:
            try:
                os.unlink(recovery_path)
            except FileNotFoundError:
                pass
        _fsync_dir(directory)
    except OSError as recovery:
        raise recovery from first


def restore(
    target: str,
    key: str,
    fault: str | None = None,
) -> tuple[dict[str, object], bool]:
    """Restore the history at ``target`` from ``target + ".recovery"``.

    ``target`` and ``key`` must be non-empty strings and ``fault`` must be
    ``None``, ``"after_replace"`` or ``"after_unlink"``, else
    ``ValueError``; ``fault`` is a test hook -- when given, an ``OSError``
    is raised immediately after the named operation (the recovery bytes
    replacing the target, or the recovery file being unlinked) and before
    the following directory fsync; the rollback below syncs the restored
    state, and the whole switch is rolled back under the target lock.

    The call holds ``target + ".lock"`` exclusively throughout. When no
    recovery file exists, the target itself is verified (with the exact
    checks of :func:`carbon_market.history.verify`, run on the bytes read
    under this lock) and ``(summary, False)`` is returned without writing.
    When one exists, its bytes are verified the same way -- the recovery
    file is itself an ordinary history and may also be previewed with
    verify beforehand -- then written byte for byte to an fsynced
    same-directory temporary and moved over the target with
    :func:`os.replace`; the recovery file is then unlinked and the
    directory is fsynced after each of the replace and the unlink. The
    return is ``(summary, changed)`` where ``summary`` is the
    verify-summary of the restored bytes and ``changed`` is ``True`` only
    when the target's bytes differed from the recovery bytes.

    Verification errors follow :func:`carbon_market.history.verify`: with
    no recovery file present, a missing target raises
    ``FileNotFoundError``; malformed or inconsistent bytes (in either the
    target or the recovery file) raise ``ValueError`` and the wrong
    idempotency key raises ``KeyError(key)``; no write happens before such
    a failure. Any failure after a write -- an injected fault or a real
    I/O error -- restores the target and the recovery file to their
    call-before bytes or nonexistence, synced, still under the lock; if
    that rollback itself fails, its ``OSError`` is raised with the first
    error as its ``__cause__``. Every other locking or I/O failure raises
    ``OSError``.
    """
    if not isinstance(target, str) or not target:
        raise ValueError("target must be a non-empty string")
    if not isinstance(key, str) or not key:
        raise ValueError("key must be a non-empty string")
    if fault not in _FAULTS:
        raise ValueError(
            "fault must be None, 'after_replace' or 'after_unlink'")

    target_real = os.path.realpath(target)
    recovery_path = target_real + ".recovery"
    directory = os.path.dirname(target_real) or "."

    with _recover_all._history_file_lock(target_real):
        # The existence decision, validation, the byte-for-byte switch and
        # the rollback are all one critical section under the target's own
        # exclusive lock, so a shared-lock history query on the target
        # sees either the complete pre-call file or the complete restored
        # one. The two paths can only be created or removed by a holder of
        # this same exclusive lock (history_copy.run), so the captured
        # bytes stay current for the whole section.
        target_before = _read_existing(target_real)
        recovery_before = _read_existing(recovery_path)

        if recovery_before is None:
            # No recovery copy: verify the target in place from the bytes
            # read under this lock and report it unchanged.
            if target_before is None:
                raise FileNotFoundError(2, os.strerror(2), target_real)
            return _verify_bytes(target_real, target_before, key), False

        # A recovery file is itself a complete history: validate it with
        # history.verify proper before the target is touched. verify takes
        # the recovery file's own companion lock (target+".recovery.lock"),
        # not target+".lock", so nesting it under this exclusive hold does
        # not self-conflict, and a verification failure performs no writes
        # at all.
        summary = _history.verify(recovery_path, key)
        changed = target_before != recovery_before

        try:
            _stage_over(target_real, directory, recovery_before)
            if fault == "after_replace":
                raise OSError(5, "injected failure after replace")
            _fsync_dir(directory)

            os.unlink(recovery_path)
            if fault == "after_unlink":
                raise OSError(5, "injected failure after unlink")
            _fsync_dir(directory)
        except BaseException as first:
            _rollback(target_real, recovery_path, directory,
                      target_before, recovery_before, first)
            raise

        return summary, changed
