"""Read-only views of a coordinated recovery batch's history.

Where :mod:`carbon_market.recover_all` appends one ``[status, coord]``
snapshot to the history file (``coord + ".history"``) every time it
creates the coordination object, takes it over, or settles one of its
items, this module only ever reads it. ``get`` returns one page of the
recorded snapshots -- each page item is ``[index, status, coord]``, the
snapshot's position in the history (which is also its sequence number),
the batch status at that moment derived as in
:mod:`carbon_market.recovery_audit`, and the coordination object as it
was then. ``verify`` returns no snapshots, only a consistency summary:
the history key, the number of snapshots, their statuses in the
recorded order and whether the history ends in a terminal state.

Both queries share the same validation. Besides the structural checks,
a history is consistent only when its top-level key equals the key of
every snapshot's coord, each coord's items are ordered by job id code
point, each recorded status is the one derived from its coord's items
(pending while any item is ``[null, null]``, failed when an error is
recorded, completed otherwise), no ``[status, coord]`` snapshot is
repeated in full, and no snapshot follows a terminal one.

The queries never write. Each one takes a shared hold on the history
file's companion lock (``path + ".lock"``) before opening the file and
releases it only after the file has been fully read and closed, while
:func:`carbon_market.recover_all.run` validates, appends, atomically
replaces and directory-syncs the history under the same lock held
exclusively. A read racing a write therefore observes either the
complete snapshot list from before the write or the one from after it
-- never a truncated, mixed or half-replaced file. The kernel releases
the flock automatically when a process exits, so a leftover lock file
never blocks a later query.
"""

from __future__ import annotations

import os

from . import recover_all as _recover_all
from ._jsonio import strict_loads

__all__ = ["get", "verify"]

_MAX_COUNT = 1000
_TERMINAL_STATUSES = ("failed", "completed")


def _is_plain_int(value: object) -> bool:
    # bool is a subclass of int and must be rejected.
    return isinstance(value, int) and not isinstance(value, bool)


def _validate_path_key(path: str, key: str) -> None:
    for value in (path, key):
        if not isinstance(value, str) or not value:
            raise ValueError("path and key must be non-empty strings")


def _validate_consistency(data: dict[str, object], history: dict[str, object]) -> None:
    # Structural validation (_recover_all._validate_history) only checks
    # each snapshot on its own; these are the cross-snapshot guarantees a
    # history written by recover_all.run upholds. ``data`` is the raw
    # parse: the validated coord rebuilds its items dict in code-point
    # order, so the on-disk item order can only be inspected here.
    top_key = history["key"]
    seen: list[list[object]] = []
    terminal_seen = False
    for entry, raw_entry in zip(history["snapshots"], data["snapshots"]):
        status, coord = entry
        if terminal_seen:
            raise ValueError(
                "no snapshot may follow a failed or completed snapshot")
        if coord["key"] != top_key:
            raise ValueError(
                "every snapshot coord must carry the history key")
        raw_items = raw_entry[1]["items"]
        if list(raw_items) != sorted(raw_items):
            raise ValueError(
                "coord items must be ordered by job id code point")
        if _recover_all._status_of(coord["items"]) != status:
            raise ValueError(
                "snapshot status must match the state derived from its "
                "coord items")
        if entry in seen:
            raise ValueError(
                "snapshots must not repeat the same [status, coord] entry")
        seen.append(entry)
        if status in _TERMINAL_STATUSES:
            terminal_seen = True


def _load(path: str, key: str) -> dict[str, object]:
    _validate_path_key(path, key)

    realpath = os.path.realpath(path)
    # Hold the history's companion lock shared from before the file is
    # opened until it has been fully read and closed, so a concurrent
    # recover_all.run (which holds the same lock exclusively around the
    # validate/append/replace/sync sequence) can never expose a truncated
    # or half-replaced history. Parsing and validation happen only after
    # the lock is released: the bytes in memory are already one complete
    # pre- or post-update snapshot. Lock, open and read failures surface
    # unchanged -- OSError, including FileNotFoundError for a missing
    # history.
    with _recover_all._history_file_lock(realpath, shared=True):
        with open(realpath, encoding="utf-8") as handle:
            text = handle.read()
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"history file {realpath!r} is not valid JSON") from exc
    history = _recover_all._validate_history(data)
    _validate_consistency(data, history)

    if history["key"] != key:
        raise KeyError(key)
    return history


def get(
    path: str,
    key: str,
    cursor: int = -1,
    count: int = 100,
    status: str | None = None,
) -> dict[str, object]:
    """Return a read-only page of the snapshot history stored at ``path``.

    ``path`` and ``key`` must be non-empty strings, else ``ValueError``.
    ``cursor`` must be a non-boolean integer no smaller than -1 and
    ``count`` a non-boolean integer between 1 and 1000; ``status`` must
    be ``None`` or one of ``"pending"``, ``"failed"`` and
    ``"completed"``. A missing history file raises
    ``FileNotFoundError``; malformed JSON, a negative-zero literal, an
    unsupported version or structure, or an inconsistent history
    raises ``ValueError``; a history recorded under a different
    idempotency key raises ``KeyError(key)``; any other I/O failure
    raises ``OSError``.

    The page holds the first ``count`` snapshots whose index is greater
    than ``cursor`` and whose status matches ``status`` (every status
    matches when it is ``None``), in their original order. The result
    carries keys key, snapshots and next, in that order; ``next`` is
    the page's last index when further matching snapshots follow, and
    ``None`` otherwise.
    """
    _validate_path_key(path, key)
    if not _is_plain_int(cursor) or cursor < -1:
        raise ValueError("cursor must be a non-boolean integer of at least -1")
    if not _is_plain_int(count) or not 1 <= count <= _MAX_COUNT:
        raise ValueError(
            "count must be a non-boolean integer between 1 and 1000")
    if status is not None and status not in _recover_all._STATUSES:
        raise ValueError("status must be None, pending, failed or completed")

    history = _load(path, key)

    snapshots: list[list[object]] = []
    next_cursor = None
    entries = history["snapshots"]
    for index in range(cursor + 1, len(entries)):
        entry_status, entry_coord = entries[index]
        if status is not None and entry_status != status:
            continue
        if len(snapshots) < count:
            snapshots.append([index, entry_status, entry_coord])
        else:
            # One more matching snapshot beyond the page: the page's
            # last index becomes the cursor that continues the scan.
            next_cursor = snapshots[-1][0]
            break

    return {"key": history["key"], "snapshots": snapshots, "next": next_cursor}


def verify(path: str, key: str) -> dict[str, object]:
    """Verify the snapshot history stored at ``path`` without paging it.

    ``path`` and ``key`` must be non-empty strings, else ``ValueError``.
    A missing history file raises ``FileNotFoundError``; malformed JSON,
    a negative-zero literal, an unsupported version or structure, or an
    inconsistent history (a coord carrying another key, items out of
    code-point order, a status not derived from its coord items, a
    repeated ``[status, coord]`` snapshot, or a snapshot after a
    terminal one) raises ``ValueError``; a history recorded under a
    different idempotency key raises ``KeyError(key)``; any other I/O
    failure raises ``OSError``.

    On success the result carries keys key, count, statuses and
    terminal, in that order: the history key, the number of snapshots,
    their statuses in the recorded order, and whether the history is
    non-empty and ends in ``"failed"`` or ``"completed"``. The query
    never writes.
    """
    history = _load(path, key)

    entries = history["snapshots"]
    statuses = [entry_status for entry_status, _coord in entries]
    terminal = bool(entries) and entries[-1][0] in _TERMINAL_STATUSES
    return {
        "key": history["key"],
        "count": len(entries),
        "statuses": statuses,
        "terminal": terminal,
    }
