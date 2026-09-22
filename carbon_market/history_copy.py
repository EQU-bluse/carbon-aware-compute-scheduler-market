"""Byte-for-byte copies of a coordinated recovery batch's history.

Where :mod:`carbon_market.history` only reads a history and
:mod:`carbon_market.recover_all` only appends snapshots to one, this
module copies a verified history from one path to another -- as the
exact UTF-8 bytes the source file held at the moment it was read, never
a re-serialization. That preserves the file byte for byte (spacing and
key order included); two byte-different files must not silently
collapse into one canonical rendering.

The source is validated exactly as :func:`carbon_market.history.verify`
validates it: strict JSON with no negative-zero number literals, the
supported structure, cross-snapshot consistency, and the idempotency
key. The bytes are read once under a shared hold on the source's
companion lock (``source + ".lock"``); the file is closed and the lock
released before parsing, since the bytes in memory are already one
complete pre- or post-update snapshot. The source is never written to.

The commit runs under an exclusive hold on the target's companion lock
(``target + ".lock"``), the same lock a ``history.get``/``verify`` on
the target takes shared, so a racing query only ever observes the
complete old target or the complete new one. An existing target is
replaced only with ``overwrite=True``; otherwise ``FileExistsError`` is
raised before anything is written. The bytes land in a temporary file
in the target's own directory, are fsynced, atomically replace the
target via :func:`os.replace`, and the directory is fsynced; a failed
commit leaves the previous target untouched and removes the temporary
file. Both locks are kernel flocks, so they block across processes and
the kernel releases them automatically when a process exits.
"""

from __future__ import annotations

import contextlib
import os
import tempfile
from typing import Any

from . import history as _history
from . import recover_all as _recover_all
from ._jsonio import strict_loads

__all__ = ["run"]


def _verified_payload(
    payload: bytes, source_real: str, key: str
) -> dict[str, object]:
    # The same post-read pipeline history._load runs on its text: strict
    # UTF-8 decoding (invalid bytes are a ValueError), strict JSON with
    # negative-zero literals rejected, structural validation, the
    # cross-snapshot consistency checks, and finally the key match. It
    # runs on the bytes just read -- never on a fresh reopen -- so the
    # summary describes exactly the bytes that are copied even if the
    # source is replaced afterwards.
    try:
        text = payload.decode("utf-8")
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

    entries = history["snapshots"]
    statuses = [entry_status for entry_status, _coord in entries]
    terminal = bool(entries) and entries[-1][0] in _history._TERMINAL_STATUSES
    return {
        "key": history["key"],
        "count": len(entries),
        "statuses": statuses,
        "terminal": terminal,
    }


def _atomic_copy(target_real: str, payload: bytes) -> None:
    # Temporary file in the target's own directory so os.replace is
    # atomic, fsync the bytes before the rename and the directory
    # afterwards so the replacement survives a crash. Any failure before
    # the rename leaves an existing target in place; the temporary file
    # is always removed.
    directory = os.path.dirname(target_real) or "."
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".history-copy-", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
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


def run(
    source: str,
    target: str,
    key: str,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Copy the history at ``source`` to ``target`` as its raw bytes.

    ``source``, ``target`` and ``key`` must be non-empty strings and
    ``overwrite`` a boolean, else ``ValueError``; paths resolving to
    the same real path raise ``ValueError``. The source is validated
    exactly as :func:`carbon_market.history.verify` -- a missing source
    raises ``FileNotFoundError``; malformed JSON or UTF-8, a
    negative-zero literal, an unsupported version or structure,
    or an inconsistent history raises ``ValueError``; a history
    recorded under a different idempotency key raises ``KeyError(key)``;
    a missing target parent directory raises ``FileNotFoundError``
    while locking; any other lock or I/O failure raises ``OSError``.

    The source's original UTF-8 bytes are copied without
    re-serialization. When the target already exists, ``overwrite=False``
    raises ``FileExistsError`` and ``overwrite=True`` atomically replaces
    it; a failed write never damages the previous target. On success the
    result carries keys key, count, statuses and terminal, in that
    order, with the values :func:`carbon_market.history.verify` would
    return for the copied source.
    """
    for name, value in (("source", source), ("target", target),
                        ("key", key)):
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} must be a non-empty string")
    if not isinstance(overwrite, bool):
        raise TypeError("overwrite must be a bool")

    source_real = os.path.realpath(source)
    target_real = os.path.realpath(target)
    if source_real == target_real:
        raise ValueError(
            "source and target must resolve to different paths")

    # Shared hold only around open/read/close, exactly like
    # history._load: a concurrent recover_all.run waits with its
    # exclusive lock, so the bytes are one complete history version.
    with _recover_all._history_file_lock(source_real, shared=True):
        with open(source_real, "rb") as handle:
            payload = handle.read()

    summary = _verified_payload(payload, source_real, key)

    # Exclusive hold for the existence check and the replace so a
    # shared-lock history query on the target sees only the complete
    # old file or the complete new one. Opening the lock first means a
    # missing target parent surfaces as FileNotFoundError here.
    with _recover_all._history_file_lock(target_real):
        if os.path.exists(target_real) and not overwrite:
            raise FileExistsError(target_real)
        _atomic_copy(target_real, payload)

    return summary
