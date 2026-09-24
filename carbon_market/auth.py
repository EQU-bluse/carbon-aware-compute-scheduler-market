"""Hot-swappable multi-token scoped authorization for GET /audit.

The ``--auth`` configuration file is UTF-8 text whose every non-empty
line is a six-element JSON array::

    [name, digest, grace_until, ops, stages, keys]

``name`` is a unique non-empty string identifying the token's owner;
``digest`` is the lowercase hexadecimal SHA-256 of the token's raw
UTF-8 bytes -- the file never holds a plaintext token -- and is unique
as well. ``grace_until`` is ``null`` or a non-negative integer Unix
second (never a boolean): the token stays valid while the current Unix
second is not greater than it, so setting a cutoff on the old digest
and appending the new one rotates a token without a restart. Each of
the three scopes is either ``"*"`` -- the filter may be omitted or take
any legal value -- or an array of distinct non-empty strings, in which
case the request must carry that filter explicitly with a value in the
array. Operation and stage scopes draw from the search filter domains
(``"copy"``/``"restore"`` and ``"成功"``, ``"校验"``, ``"执行"``,
``"同步"``, ``"回滚"``); history-key scopes match the event's key by
exact string equality.

:func:`load` reads and validates the whole file: a missing file, an
empty one (no records), invalid UTF-8 or JSON (negative-zero literals
included), a wrong line shape, a bad name, digest, cutoff or scope
value, or a duplicated name or digest all raise. The service validates
the file once at startup and re-reads it on every request, so a
same-directory atomic replacement swaps the complete old configuration
for the complete new one with no restart and no mixed view.

:func:`identify` hashes the presented token and compares the digest
against every record with a fixed-time comparison, deciding the
identity only after all records were checked; an unknown digest and a
token past its grace cutoff are indistinguishable -- both yield
``None``. :func:`scope_allows` then checks the validated query filters
against the record's scopes before the audit file is ever opened.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from typing import Any, NamedTuple

from ._jsonio import strict_loads

__all__ = ["Record", "load", "identify", "scope_allows"]

_OPS = ("copy", "restore")
_STAGES = ("成功", "校验", "执行", "同步", "回滚")
_HEX_DIGITS = frozenset("0123456789abcdef")
_DIGEST_HEX_LENGTH = 64


class Record(NamedTuple):
    """One validated authorization record.

    ``digest`` is the 32 raw bytes of the token's SHA-256. Each scope
    is ``None`` for ``"*"`` (unrestricted) or a frozenset of the
    allowed filter values.
    """

    name: str
    digest: bytes
    grace_until: int | None
    ops: frozenset[str] | None
    stages: frozenset[str] | None
    keys: frozenset[str] | None


def _validate_name(raw: object) -> str:
    if not isinstance(raw, str) or not raw:
        raise ValueError("auth name must be a non-empty string")
    return raw


def _validate_digest(raw: object) -> bytes:
    if not isinstance(raw, str) or len(raw) != _DIGEST_HEX_LENGTH \
            or any(char not in _HEX_DIGITS for char in raw):
        raise ValueError(
            "auth digest must be the lowercase hexadecimal SHA-256 of "
            "the token")
    return bytes.fromhex(raw)


def _validate_grace_until(raw: object) -> int | None:
    # bool is a subclass of int and must be rejected as a cutoff.
    if raw is None:
        return None
    if not isinstance(raw, int) or isinstance(raw, bool) or raw < 0:
        raise ValueError(
            "auth grace cutoff must be null or a non-negative integer")
    return raw


def _validate_scope(raw: object, allowed: tuple[str, ...] | None,
                    label: str) -> frozenset[str] | None:
    # "*" lifts the restriction entirely; otherwise the scope is an
    # array of distinct non-empty strings, drawn from the search filter
    # domain when one applies (operations and stages).
    if raw == "*":
        return None
    if not isinstance(raw, list):
        raise ValueError(f'auth {label} scope must be "*" or an array')
    values: set[str] = set()
    for item in raw:
        if not isinstance(item, str) or not item:
            raise ValueError(
                f"auth {label} scope entries must be non-empty strings")
        if allowed is not None and item not in allowed:
            raise ValueError(
                f"auth {label} scope entries must be drawn from "
                + ", ".join(allowed))
        if item in values:
            raise ValueError(f"auth {label} scope entries must be distinct")
        values.add(item)
    return frozenset(values)


def _parse_line(line: str) -> Record:
    try:
        data: Any = strict_loads(line)
    except ValueError as exc:
        raise ValueError("auth line is not valid JSON") from exc
    if not isinstance(data, list) or len(data) != 6:
        raise ValueError(
            "auth line must be an array of name, digest, grace cutoff "
            "and the operation, stage and history-key scopes")
    name, digest, grace_until, ops, stages, keys = data
    return Record(
        name=_validate_name(name),
        digest=_validate_digest(digest),
        grace_until=_validate_grace_until(grace_until),
        ops=_validate_scope(ops, _OPS, "operation"),
        stages=_validate_scope(stages, _STAGES, "stage"),
        keys=_validate_scope(keys, None, "history-key"),
    )


def load(path: str) -> list[Record]:
    """Read and validate the whole authorization file.

    Every non-empty line must be a six-element JSON array as described
    in the module docstring; names and digests must each be unique and
    the file must yield at least one record. A missing file or other
    I/O failure raises :class:`OSError`; invalid UTF-8, malformed JSON
    (negative-zero literals included), a wrong shape, a bad value, a
    duplicated name or digest, or no records at all raise
    :class:`ValueError`. The returned list is a complete snapshot: a
    caller either gets the whole old configuration or the whole new
    one, never a mixture.
    """
    with open(path, "rb") as handle:
        raw = handle.read()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"auth file {path!r} is not valid UTF-8") from exc

    records: list[Record] = []
    names: set[str] = set()
    digests: set[bytes] = set()
    for line in text.splitlines():
        if not line.strip():
            continue
        record = _parse_line(line)
        if record.name in names:
            raise ValueError(f"duplicate auth name {record.name!r}")
        if record.digest in digests:
            raise ValueError("duplicate auth token digest")
        names.add(record.name)
        digests.add(record.digest)
        records.append(record)

    if not records:
        raise ValueError(f"auth file {path!r} holds no token records")
    return records


def identify(records: list[Record], token: str,
             now: int | None = None) -> Record | None:
    """Resolve a presented token to its record, or ``None``.

    The token's SHA-256 is compared against every record's digest with
    a fixed-time comparison and the identity is decided only after all
    records were checked, so the comparison count never reveals a
    prefix match. A token whose grace cutoff lies in the past -- the
    current Unix second is greater than it -- yields ``None`` exactly
    like an unknown digest. ``now`` defaults to the current Unix
    second.
    """
    if now is None:
        now = int(time.time())
    candidate = hashlib.sha256(token.encode("utf-8")).digest()
    matched: Record | None = None
    for record in records:
        if hmac.compare_digest(candidate, record.digest):
            matched = record
    if matched is None:
        return None
    if matched.grace_until is not None and now > matched.grace_until:
        return None
    return matched


def scope_allows(record: Record, params: dict[str, str]) -> bool:
    """Check validated query filters against the record's scopes.

    A ``"*"`` scope (``None``) lets the request omit the filter or use
    any legal value; an array scope requires the request to carry the
    filter explicitly with a value in the array. Only the op, stage and
    key filters constrain; cursor and limit are not scoped.
    """
    for scope, name in ((record.ops, "op"), (record.stages, "stage"),
                        (record.keys, "key")):
        if scope is None:
            continue
        value = params.get(name)
        if value is None or value not in scope:
            return False
    return True
