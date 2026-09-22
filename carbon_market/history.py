"""Read-only paginated view of a recovery batch's snapshot history.

Where :mod:`carbon_market.recover_all` appends a snapshot to the history
sidecar (``coord + ".history"``) at every transition of the coordination
object, this module only ever reads it: ``get`` returns one page of the
recorded ``[status, coord]`` snapshots, each prefixed with its sequence
number, optionally restricted to a single status. Paging is cursor-based:
passing the returned ``next`` value back as ``cursor`` resumes exactly
where the previous page stopped, and ``next`` is ``None`` once no
further matching snapshot remains.

The query never writes. Because the history file is only ever replaced
atomically, a read racing a write observes either the complete version
from before the write or the one from after it -- never a partial file.
"""

from __future__ import annotations

import os
from typing import Any

from . import recover_all as _recover_all
from ._jsonio import strict_loads

__all__ = ["get"]

_STATUSES = ("pending", "failed", "completed")


def get(
    path: str,
    key: str,
    cursor: int = -1,
    count: int = 100,
    status: str | None = None,
) -> dict[str, object]:
    """Return one page of the snapshot history stored at ``path``.

    ``path`` and ``key`` must be non-empty strings, else ``ValueError``.
    ``cursor`` must be a non-boolean integer of at least ``-1`` and
    ``count`` a non-boolean integer between 1 and 1000; ``status`` must
    be ``None`` or one of ``"pending"``, ``"failed"`` and
    ``"completed"`` -- anything else raises ``ValueError``. A missing
    history file raises ``FileNotFoundError``; malformed JSON, a
    negative-zero literal, or an unsupported version or structure raises
    ``ValueError``; a history recorded under a different idempotency key
    raises ``KeyError(key)``; any other I/O failure raises ``OSError``.

    The result carries keys key, snapshots and next, in that order.
    Snapshots are the first ``count`` entries whose sequence number is
    greater than ``cursor`` and whose status matches ``status`` (every
    status matches when it is ``None``), in their original order, each
    as ``[index, status, coord]``. When further matching snapshots
    remain beyond the page, ``next`` is the sequence number of the
    page's last entry and may be passed back as ``cursor``; otherwise
    ``next`` is ``None``.
    """
    for value in (path, key):
        if not isinstance(value, str) or not value:
            raise ValueError("path and key must be non-empty strings")
    if not _recover_all._is_plain_int(cursor) or cursor < -1:
        raise ValueError("cursor must be a non-boolean integer of at least -1")
    if not _recover_all._is_plain_int(count) or not 1 <= count <= 1000:
        raise ValueError(
            "count must be a non-boolean integer between 1 and 1000")
    if status is not None and status not in _STATUSES:
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

    page: list[list[Any]] = []
    has_more = False
    for index, snapshot in enumerate(history["snapshots"]):
        if index <= cursor:
            continue
        if status is not None and snapshot[0] != status:
            continue
        if len(page) < count:
            page.append([index, snapshot[0], snapshot[1]])
        else:
            has_more = True
            break

    return {
        "key": history["key"],
        "snapshots": page,
        "next": page[-1][0] if has_more else None,
    }
