"""Strict JSON decoding shared by the persistent registries.

The only difference from :func:`json.loads` is that number tokens whose
lexical form is a negative zero (``-0``, ``-0.0``, ``-0e2`` ...) are rejected.
``json.loads`` silently normalizes ``-0`` to the integer ``0`` and ``-0.0``
to a non-negative float, which would let two byte-different files compare
equal; the registries instead treat such literals as malformed input. The
hooks only see number tokens, so a ``-0`` appearing inside a string is never
inspected.
"""

from __future__ import annotations

import json

__all__ = ["strict_loads", "finite_loads"]


def _is_negative_zero(token: str) -> bool:
    body = token.lstrip()
    if not body.startswith("-"):
        return False
    # A signed numeric token is negative zero exactly when its value is zero;
    # -0.5 stays non-zero, while every spelling of -0 (fraction/exponent) maps
    # to zero.
    return float(body) == 0.0


def _parse_int(token: str) -> int:
    if _is_negative_zero(token):
        raise ValueError("negative-zero number literal is not allowed")
    return int(token)


def _parse_float(token: str) -> float:
    if _is_negative_zero(token):
        raise ValueError("negative-zero number literal is not allowed")
    return float(token)


def strict_loads(text: str) -> object:
    """Parse JSON, raising ValueError on negative-zero number literals."""
    return json.loads(text, parse_int=_parse_int, parse_float=_parse_float)


def _reject_constant(token: str) -> float:
    # Only NaN/Infinity/-Infinity reach the constant hook; they are not
    # finite numbers and never valid in a verified document.
    raise ValueError("non-finite number literal is not allowed")


def finite_loads(text: str) -> object:
    """Parse JSON like strict_loads, also rejecting non-finite literals."""
    return json.loads(text, parse_int=_parse_int, parse_float=_parse_float,
                      parse_constant=_reject_constant)
