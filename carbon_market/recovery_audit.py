"""Read-only audit view of a coordinated recovery batch.

Where :mod:`carbon_market.recover_all` creates and advances the
coordination file, this module only ever reads it: ``get`` returns a
snapshot of one batch -- its version, idempotency key, owner and lease
expiry copied from the coordination object, a derived ``status`` and the
per-job ``[event, error]`` outcomes. The status is ``"pending"`` while
any item is still ``[null, null]``, ``"failed"`` when no item is pending
but at least one carries a non-null error, and ``"completed"`` otherwise.

The query never writes. Because the coordination file is only ever
replaced atomically, a read racing a write observes either the complete
snapshot from before the write or the one from after it -- never a
partial file.
"""

from __future__ import annotations

import os
from typing import Any

from . import recover_all as _recover_all
from ._jsonio import strict_loads

__all__ = ["get"]


def get(coord: str, key: str) -> dict[str, object]:
    """Return a read-only audit snapshot of the batch stored at ``coord``.

    Both arguments must be non-empty strings, else ``ValueError``. A
    missing coordination file raises ``FileNotFoundError``; malformed
    JSON, a negative-zero literal, or an unsupported version or structure
    raises ``ValueError``; a batch recorded under a different idempotency
    key raises ``KeyError(key)``; any other I/O failure raises
    ``OSError``. The result carries keys version, key, owner, until,
    status and items, in that order, with items mapping each job_id in
    code-point order to its ``[event, error]`` pair.
    """
    for value in (coord, key):
        if not isinstance(value, str) or not value:
            raise ValueError("coord and key must be non-empty strings")

    realpath = os.path.realpath(coord)
    with open(realpath, encoding="utf-8") as handle:
        text = handle.read()
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"coordination file {realpath!r} is not valid JSON") from exc
    coord_obj = _recover_all._validate_coord(data)

    if coord_obj["key"] != key:
        raise KeyError(key)

    items: dict[str, list[Any]] = {
        job_id: [pair[0], pair[1]] for job_id, pair in coord_obj["items"].items()
    }
    if any(pair[0] is None and pair[1] is None for pair in items.values()):
        status = "pending"
    elif any(pair[1] is not None for pair in items.values()):
        status = "failed"
    else:
        status = "completed"

    return {
        "version": coord_obj["version"],
        "key": coord_obj["key"],
        "owner": coord_obj["owner"],
        "until": coord_obj["until"],
        "status": status,
        "items": items,
    }
