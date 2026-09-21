"""Strict JSON decoding shared by the on-disk registries."""

from __future__ import annotations

import json

__all__ = ["StrictJSONError", "loads"]


class StrictJSONError(ValueError):
    """Raised when JSON text is malformed or contains a negative zero."""


def _parse_int(token: str) -> int:
    if token[0] == "-" and int(token) == 0:
        raise StrictJSONError("negative zero numbers are not allowed")
    return int(token)


def _parse_float(token: str) -> float:
    mantissa = token.split("e", 1)[0].split("E", 1)[0]
    if mantissa[0] == "-" and float(mantissa) == 0.0:
        raise StrictJSONError("negative zero numbers are not allowed")
    return float(token)


def loads(text: str) -> object:
    """Decode JSON like ``json.loads`` but reject negative-zero numbers.

    Tokens such as ``-0``, ``-0.0`` and ``-0e2`` raise ``ValueError``.
    Number tokens only ever reach the integer/float hooks, so the same
    characters inside strings are never flagged.
    """
    try:
        return json.loads(text, parse_int=_parse_int, parse_float=_parse_float)
    except StrictJSONError:
        raise
    except json.JSONDecodeError as exc:
        raise StrictJSONError(str(exc)) from exc
