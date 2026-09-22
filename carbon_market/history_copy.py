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

Both locks are kernel flocks: they block other processes (and other
opens of the same lock file in this process), and the kernel releases
them automatically when the process exits, so a leftover lock file never
blocks a later copy or query. A shared-lock reader of the target only
ever observes the complete old target or the complete new one, never a
truncated or half-replaced file; a failed commit leaves the previous
target untouched and removes its temporary file. The source is never
written.
"""

from __future__ import annotations

import contextlib
import os
import tempfile

from . import history as _history
from . import recover_all as _recover_all
from ._jsonio import strict_loads

__all__ = ["run"]


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
    replaces it atomically (temporary file in the target's directory,
    fsync, replace, directory fsync); a failed commit never damages the
    previous target and always cleans up its temporary file.

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
    # the byte-for-byte write, the atomic replace and the directory sync
    # are all one critical section, so a shared-lock history query on the
    # target sees either the complete old file or the complete copy. The
    # original bytes are written verbatim -- never re-serialized -- so the
    # target is byte-identical to the source that was validated.
    with _recover_all._history_file_lock(target_real):
        if os.path.exists(target_real) and not overwrite:
            raise FileExistsError(
                f"history file already exists: {target_real!r}")

        fd, tmp_path = tempfile.mkstemp(
            dir=directory, prefix=".history-copy-", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, target_real)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            raise

        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    entries = history["snapshots"]
    statuses = [entry_status for entry_status, _coord in entries]
    terminal = bool(entries) and entries[-1][0] in _history._TERMINAL_STATUSES
    return {
        "key": history["key"],
        "count": len(entries),
        "statuses": statuses,
        "terminal": terminal,
    }
