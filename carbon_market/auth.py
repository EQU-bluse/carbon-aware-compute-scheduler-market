"""Hot-rotatable multi-token scoped authorization for GET /audit.

The authorization file is UTF-8 text; every non-empty line is a single
JSON array of exactly six elements::

    [name, digest, deadline, op_scope, stage_scope, key_scope]

* ``name`` -- a unique non-empty string naming the record.
* ``digest`` -- the lowercase hex SHA-256 of the token's original UTF-8
  bytes. Plaintext tokens must never be stored; a 64-character lowercase
  hex string is the only accepted form.
* ``deadline`` -- ``null`` (never expires) or a non-negative integer of
  current Unix seconds; the token is valid while ``now <= deadline``.
* ``op_scope``, ``stage_scope``, ``key_scope`` -- either the string
  ``"*"`` (any value, and the filter may be omitted) or an array of
  distinct non-empty strings. ``op`` values are the search operations
  (``copy``/``restore``), ``stage`` values the search stages
  (``成功``/``校验``/``执行``/``同步``/``回滚``) and ``key`` values match
  the history key verbatim.

:func:`load_config` validates the whole file and returns an immutable
snapshot the whole request is authorized against; a rotation replaces
the file atomically in the same directory, so a request is always
evaluated against either the complete old or the complete new
configuration. A missing or empty file, an encoding or JSON error, any
shape/value error, or a duplicate name or digest raises
:class:`AuthConfigError`.

:func:`authenticate` takes the single non-blank token header value and
returns the matching, currently valid record or ``None``. Every digest
is compared in constant time and the expiry decision is made only once
after all comparisons, so an unmatched token and an expired one are
indistinguishable to the caller, and neither the name nor any other
configuration detail is implied by the timing.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from typing import Any, NamedTuple

from ._jsonio import strict_loads

__all__ = ["AuthConfigError", "Record", "Config", "load_config",
           "authenticate", "check_scope"]

# Mirror the values accepted by audit.search: the scopes reuse the
# existing retrieval vocabulary rather than defining their own.
_OPS = ("copy", "restore")
_STAGES = ("成功", "校验", "执行", "同步", "回滚")
_SCOPE_VALUES = {"op": frozenset(_OPS), "stage": frozenset(_STAGES)}
_HEX = frozenset("0123456789abcdef")
_SHA256_HEX_LEN = 64


class AuthConfigError(ValueError):
    """The authorization file is missing, unreadable or invalid."""


class Record(NamedTuple):
    name: str
    digest: str
    deadline: int | None
    ops: frozenset[str]
    stages: frozenset[str]
    keys: frozenset[str]


class Config(NamedTuple):
    records: tuple[Record, ...]


def _scope_from(raw: object, field: str) -> frozenset[str]:
    # "*" means unrestricted; otherwise the scope is a list of distinct
    # non-empty strings drawn from the existing retrieval vocabulary (op
    # and stage) or matched verbatim (history key).
    if raw == "*":
        return frozenset()
    if not isinstance(raw, list) or not raw:
        raise AuthConfigError(f"{field} scope must be \"*\" or a non-empty "
                             "array of distinct non-empty strings")
    values: set[str] = set()
    for item in raw:
        if not isinstance(item, str) or not item:
            raise AuthConfigError(
                f"{field} scope entries must be non-empty strings")
        if item in values:
            raise AuthConfigError(
                f"{field} scope entries must be distinct")
        allowed = _SCOPE_VALUES.get(field)
        if allowed is not None and item not in allowed:
            raise AuthConfigError(f"unknown {field} scope value: {item!r}")
        values.add(item)
    return frozenset(values)


def _record_from(raw: object, line_number: int) -> Record:
    where = f"auth config line {line_number}"
    if not isinstance(raw, list):
        raise AuthConfigError(f"{where}: entry must be a JSON array")
    if len(raw) != 6:
        raise AuthConfigError(f"{where}: entry must have exactly 6 elements")

    name, digest, deadline, raw_ops, raw_stages, raw_keys = raw

    if not isinstance(name, str) or not name:
        raise AuthConfigError(f"{where}: name must be a non-empty string")

    if not isinstance(digest, str) or len(digest) != _SHA256_HEX_LEN \
            or any(char not in _HEX for char in digest):
        raise AuthConfigError(
            f"{where}: digest must be the 64-character lowercase hex "
            "SHA-256 of the token")

    # bool is a subclass of int and is not a valid deadline.
    if deadline is not None and (
            not isinstance(deadline, int) or isinstance(deadline, bool)
            or deadline < 0):
        raise AuthConfigError(
            f"{where}: deadline must be null or a non-negative integer")

    return Record(
        name=name,
        digest=digest,
        deadline=deadline,
        ops=_scope_from(raw_ops, "op"),
        stages=_scope_from(raw_stages, "stage"),
        keys=_scope_from(raw_keys, "key"),
    )


def load_config(path: str) -> Config:
    """Read and validate the whole authorization file into a snapshot.

    Raises :class:`AuthConfigError` for a missing, empty or unreadable
    file, invalid UTF-8 or JSON, any shape/value violation, or a
    duplicate record name or digest.
    """
    try:
        with open(path, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError as exc:
        raise AuthConfigError(f"auth config {path!r} does not exist") from exc
    except OSError as exc:
        raise AuthConfigError(f"auth config {path!r} cannot be read") from exc

    if not raw:
        raise AuthConfigError(f"auth config {path!r} is empty")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise AuthConfigError(
            f"auth config {path!r} is not valid UTF-8") from exc

    records: list[Record] = []
    seen_names: set[str] = set()
    seen_digests: set[str] = set()
    for index, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            raw_entry = strict_loads(line)
        except ValueError as exc:
            raise AuthConfigError(
                f"auth config line {index}: not valid JSON") from exc
        record = _record_from(raw_entry, index)
        if record.name in seen_names:
            raise AuthConfigError(
                f"auth config line {index}: duplicate token name "
                f"{record.name!r}")
        if record.digest in seen_digests:
            raise AuthConfigError(
                f"auth config line {index}: duplicate token digest")
        seen_names.add(record.name)
        seen_digests.add(record.digest)
        records.append(record)

    if not records:
        raise AuthConfigError(f"auth config {path!r} defines no tokens")
    return Config(tuple(records))


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def authenticate(config: Config, token: str, now: int | None = None) \
        -> Record | None:
    """Return the record matching ``token`` and still within its deadline.

    ``None`` covers both an unknown token and one whose deadline has
    passed; the caller treats both as 403. Every record digest is
    compared in constant time and the expiry check happens once, after
    the comparisons, so the two cases do not differ in timing.
    """
    matched: Record | None = None
    token_digest = _hash_token(token)
    for record in config.records:
        if hmac.compare_digest(token_digest, record.digest):
            matched = record
    if matched is None:
        return None
    if now is None:
        now = int(time.time())
    if matched.deadline is not None and now > matched.deadline:
        return None
    return matched


def check_scope(record: Record, op: str | None, stage: str | None,
                key: str | None) -> bool:
    """Whether the record permits a request carrying these filters.

    A listed scope ("*" is stored as an empty frozenset) requires the
    corresponding filter to be present explicitly, with a value in the
    scope; "*" permits omission or any legal value. The filters have
    already passed the usual query-parameter validation, so scope values
    are not re-validated here.
    """
    if record.ops and (op is None or op not in record.ops):
        return False
    if record.stages and (stage is None or stage not in record.stages):
        return False
    if record.keys and (key is None or key not in record.keys):
        return False
    return True
