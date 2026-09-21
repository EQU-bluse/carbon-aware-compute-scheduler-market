"""Persistent, thread-safe capacity-offer registry."""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
import threading
from typing import Any

from ._jsonio import strict_loads

__all__ = ["register"]

_VERSION = 1
_OFFER_FIELDS = ("resource_id", "region", "capacity_wh",
                 "unit_cost", "carbon_intensity")
_REQUIRED_FIELDS = frozenset(_OFFER_FIELDS)
_ROOT_FIELDS = ("version", "offers", "idempotency")


class _Store:
    def __init__(self, realpath: str) -> None:
        self.realpath = realpath
        self.lock = threading.Lock()


_stores_lock = threading.Lock()
_stores: dict[str, _Store] = {}


def _get_store(path: str) -> _Store:
    realpath = os.path.realpath(path)
    with _stores_lock:
        store = _stores.get(realpath)
        if store is None:
            store = _Store(realpath)
            _stores[realpath] = store
        return store


def _is_plain_int(value: object) -> bool:
    # bool is a subclass of int and must be rejected.
    return isinstance(value, int) and not isinstance(value, bool)


def _normalize_offer(
    offer: object,
) -> tuple[str, str, int, int, int]:
    if not isinstance(offer, dict) or set(offer.keys()) != _REQUIRED_FIELDS:
        raise ValueError("offer must be an object with exactly resource_id, "
                         "region, capacity_wh, unit_cost and carbon_intensity")

    resource_id = offer["resource_id"]
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("resource_id must be a non-empty string")

    region = offer["region"]
    if not isinstance(region, str) or not region:
        raise ValueError("region must be a non-empty string")

    capacity_wh = offer["capacity_wh"]
    if not _is_plain_int(capacity_wh) or capacity_wh <= 0:
        raise ValueError("capacity_wh must be a non-boolean positive integer")

    unit_cost = offer["unit_cost"]
    if not _is_plain_int(unit_cost) or unit_cost < 0:
        raise ValueError("unit_cost must be a non-boolean non-negative integer")

    carbon_intensity = offer["carbon_intensity"]
    if not _is_plain_int(carbon_intensity) or carbon_intensity < 0:
        raise ValueError("carbon_intensity must be a non-boolean "
                         "non-negative integer")

    return resource_id, region, capacity_wh, unit_cost, carbon_intensity


def _validate_structure(
    data: object,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, str]]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("registry root must be an object with keys "
                         "version, offers and idempotency")

    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported registry version")

    offers_raw = data["offers"]
    idempotency_raw = data["idempotency"]
    if not isinstance(offers_raw, dict) or not isinstance(idempotency_raw, dict):
        raise ValueError("offers and idempotency must be objects")

    offers: dict[str, dict[str, Any]] = {}
    for name, record in offers_raw.items():
        if not isinstance(name, str) or not name:
            raise ValueError("resource ids must be non-empty strings")
        if not isinstance(record, dict) or set(record.keys()) != _REQUIRED_FIELDS:
            raise ValueError("offer record has invalid fields")
        if record["resource_id"] != name:
            raise ValueError("offer record id does not match its key")
        if not isinstance(record["region"], str) or not record["region"]:
            raise ValueError("region must be a non-empty string")
        if (not _is_plain_int(record["capacity_wh"])
                or record["capacity_wh"] <= 0):
            raise ValueError("invalid capacity_wh")
        if not _is_plain_int(record["unit_cost"]) or record["unit_cost"] < 0:
            raise ValueError("invalid unit_cost")
        if (not _is_plain_int(record["carbon_intensity"])
                or record["carbon_intensity"] < 0):
            raise ValueError("invalid carbon_intensity")
        # Rebuild the record in canonical field order so that replaying an
        # idempotency key against, or rewriting, a file that stored the fields
        # in a different order still observes the established order.
        offers[name] = {
            "resource_id": record["resource_id"],
            "region": record["region"],
            "capacity_wh": record["capacity_wh"],
            "unit_cost": record["unit_cost"],
            "carbon_intensity": record["carbon_intensity"],
        }

    idempotency: dict[str, dict[str, str]] = {}
    referenced: set[str] = set()
    for key, entry in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        if not isinstance(entry, dict) or set(entry.keys()) != {"resource_id"}:
            raise ValueError("idempotency entry must be a resource_id object")
        resource_id = entry["resource_id"]
        if not isinstance(resource_id, str) or not resource_id \
                or resource_id not in offers:
            raise ValueError("idempotency entry must reference a "
                             "registered offer")
        if resource_id in referenced:
            raise ValueError("offer registered under more than one "
                             "idempotency key")
        referenced.add(resource_id)
        idempotency[key] = {"resource_id": resource_id}

    return offers, idempotency


def _load(
    realpath: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, str]]]:
    try:
        with open(realpath, encoding="utf-8") as handle:
            text = handle.read()
    except FileNotFoundError:
        return {}, {}

    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(f"registry file {realpath!r} is not valid JSON") from exc
    return _validate_structure(data)


def _atomic_write(
    realpath: str,
    offers: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, str]],
) -> None:
    payload = {
        "version": _VERSION,
        "offers": {name: offers[name] for name in sorted(offers)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"

    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".offers-",
                                    suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, realpath)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise

    with contextlib.suppress(OSError):
        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)


def register(
    path: str,
    offer: dict[str, object],
    idempotency_key: str,
) -> tuple[dict[str, object], bool]:
    """Register a capacity offer idempotently as compact UTF-8 JSON.

    Returns ``(record, created)`` where ``record`` carries resource_id,
    region, capacity_wh, unit_cost (micro-units per Wh) and carbon_intensity
    (g/kWh); ``created`` is ``True`` for a new registration and ``False``
    when the idempotency key replays an identical offer.
    """
    if not isinstance(path, str):
        raise ValueError("path must be a string")
    if not isinstance(idempotency_key, str) or not idempotency_key:
        raise ValueError("idempotency_key must be a non-empty string")
    resource_id, region, capacity_wh, unit_cost, carbon_intensity = \
        _normalize_offer(offer)

    store = _get_store(path)
    with store.lock:
        offers, idempotency = _load(store.realpath)
        record: dict[str, object] = {
            "resource_id": resource_id,
            "region": region,
            "capacity_wh": capacity_wh,
            "unit_cost": unit_cost,
            "carbon_intensity": carbon_intensity,
        }

        existing_entry = idempotency.get(idempotency_key)
        if existing_entry is not None:
            existing = offers[existing_entry["resource_id"]]
            if existing != record:
                raise ValueError("idempotency key was already used with a "
                                 "different offer")
            return dict(existing), False

        if resource_id in offers:
            raise ValueError("resource_id is already registered under "
                             "another idempotency key")

        offers[resource_id] = record
        idempotency[idempotency_key] = {"resource_id": resource_id}
        _atomic_write(store.realpath, offers, idempotency)
        return dict(record), True
