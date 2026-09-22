"""Read-only paginated view of a coordinated recovery batch's history.

Where :mod:`carbon_market.recover_all` appends one ``[status, coord]``
snapshot to the history file (``coord + ".history"``) every time it
creates the coordination object, takes it over, or settles one of its
items, this module only ever reads it: ``get`` returns one page of the
recorded snapshots. Each page item
is ``[index, status, coord]`` -- the snapshot's position in the history
(which is also its sequence number), the batch status at that moment
derived as in :mod:`carbon_market.recovery_audit`, and the coordination
object as it was then.

The query never writes. Because the history file is only ever replaced
atomically, a read racing a write observes either the complete snapshot
list from before the write or the one from after it -- never a partial
file.
"""

from __future__ import annotations

import os

from . import recover_all as _recover_all
from ._jsonio import strict_loads

__all__ = ["get"]

_MAX_COUNT = 1000


def _is_plain_int(value: object) -> bool:
    # bool is a subclass of int and must be rejected.
    return isinstance(value, int) and not isinstance(value, bool)


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
    ``FileNotFoundError``; malformed JSON, a negative-zero literal, or
    an unsupported version or structure raises ``ValueError``; a
    history recorded under a different idempotency key raises
    ``KeyError(key)``; any other I/O failure raises ``OSError``.

    The page holds the first ``count`` snapshots whose index is greater
    than ``cursor`` and whose status matches ``status`` (every status
    matches when it is ``None``), in their original order. The result
    carries keys key, snapshots and next, in that order; ``next`` is
    the page's last index when further matching snapshots follow, and
    ``None`` otherwise.
    """
    for value in (path, key):
        if not isinstance(value, str) or not value:
            raise ValueError("path and key must be non-empty strings")
    if not _is_plain_int(cursor) or cursor < -1:
        raise ValueError("cursor must be a non-boolean integer of at least -1")
    if not _is_plain_int(count) or not 1 <= count <= _MAX_COUNT:
        raise ValueError(
            "count must be a non-boolean integer between 1 and 1000")
    if status is not None and status not in _recover_all._STATUSES:
        raise ValueError("status must be None, pending, failed or completed")

    realpath = os.path.realpath(path)
    with open(realpath, encoding="utf-8") as handle:
        text = handle.read()
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"history file {realpath!r} is not valid JSON") from exc
    history = _recover_all._validate_history(data)

    if history["key"] != key:
        raise KeyError(key)

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
