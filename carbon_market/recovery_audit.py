"""Read-only audit view of a coordinated recovery batch.

This module only *reads* the coordination file written by
:mod:`carbon_market.recover_all`; it never creates or modifies it. The
write side publishes each new coordination object by atomically
replacing the file (``os.replace`` of a fully fsynced temp file), so an
audit racing a write observes either the complete pre-write snapshot or
the complete post-write snapshot -- never a torn or half-written file,
with no locking required on the read side.

The returned snapshot gains one derived field, ``status``: ``"pending"``
while at least one job slot is still ``[null, null]``, otherwise
``"failed"`` if any slot recorded an error, and ``"completed"`` once
every slot holds an event.
"""

from __future__ import annotations

import os
from typing import Any

from .recover_all import _load_coord

__all__ = ["get"]


def get(coord: str, key: str) -> dict[str, object]:
    """Return the audited state of coordination file ``coord`` for ``key``.

    The returned object has keys in the order
    ``version, key, owner, until, status, items``; ``version``, ``key``,
    ``owner`` and ``until`` are copied from the coordination file, items
    map each job_id (in code-point order) to its ``[event, error]`` slot,
    and each event keeps the order
    ``job_id, source_id, target_id, op, now``.

    Raises ``ValueError`` when either argument is not a non-empty string
    or the file is malformed (bad JSON, a negative-zero literal, an
    unsupported version or any structural violation), ``FileNotFoundError``
    when no coordination file exists at ``coord``, ``KeyError(key)`` when
    the batch recorded under the file belongs to a different key, and
    ``OSError`` for any other I/O failure.
    """
    for name, value in (("coord", coord), ("key", key)):
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} must be a non-empty string")

    coord_obj = _load_coord(os.path.realpath(coord))
    if coord_obj is None:
        raise FileNotFoundError(
            f"coordination file {os.path.realpath(coord)!r} does not exist")

    if coord_obj["key"] != key:
        raise KeyError(key)

    items_raw = coord_obj["items"]
    pending = any(pair[0] is None and pair[1] is None
                  for pair in items_raw.values())
    failed = (not pending) and any(pair[1] is not None
                                   for pair in items_raw.values())
    status = "pending" if pending else ("failed" if failed else "completed")

    # Rebuild items so the audit view is a fresh snapshot whose pairs and
    # events cannot be aliased back into the write-side object.
    items: dict[str, list[Any]] = {
        job_id: [None if pair[0] is None else dict(pair[0]), pair[1]]
        for job_id, pair in items_raw.items()
    }

    return {
        "version": coord_obj["version"],
        "key": coord_obj["key"],
        "owner": coord_obj["owner"],
        "until": coord_obj["until"],
        "status": status,
        "items": items,
    }
