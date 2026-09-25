"""Persistent, thread-safe market ledgers.

Two independent ledgers live side by side:

* :func:`match` settles the original job-to-offer registry (version 1)
  against ``jobs.register`` and ``offers.register`` files.
* :func:`clear` settles a separate version-1 clearing ledger against the
  public ``jobs.submit`` acceptance file and the versioned
  ``resources.publish`` supply file, with feasibility semantics shared
  with ``resources.feasible`` and per-resource-version capacity already
  consumed by earlier clearings deducted before ranking.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
import threading
from typing import Any, Callable

from . import jobs as _jobs
from . import offers as _offers
from . import resources as _resources
from ._jsonio import finite_loads, strict_loads

__all__ = ["match", "clear"]

_VERSION = 1
_MATCH_FIELDS = ("job_id", "resource_id")
_ROOT_FIELDS = ("version", "matches", "idempotency")

# Version 1 of the submit/supply clearing ledger: a new document kind,
# separate from the match ledger, carrying the cleared records, the
# idempotency map and one audit event per first clearing.
_CLEAR_VERSION = 1
_CLEAR_RECORD_FIELDS = ("job_id", "at", "work", "candidates", "selected")
_CLEAR_SELECTED_FIELDS = ("resource_id", "version")
_CLEAR_EVENT_FIELDS = ("key", "job_id", "resource_id", "version")
_CLEAR_ROOT_FIELDS = ("version", "cleared", "idempotency", "audit")


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


def _load_registry(
    realpath: str,
    validate: Callable[[object], tuple[dict[str, dict[str, Any]], Any]],
    kind: str,
) -> tuple[dict[str, dict[str, Any]], Any]:
    # Unlike the registries themselves, match() requires the jobs and offers
    # files to exist, so FileNotFoundError (and any other OSError) propagates.
    with open(realpath, encoding="utf-8") as handle:
        text = handle.read()
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"{kind} file {realpath!r} is not valid JSON") from exc
    return validate(data)


def _validate_ledger(
    data: object,
    job_ids: dict[str, Any],
    resource_ids: dict[str, Any],
) -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_ROOT_FIELDS):
        raise ValueError("ledger root must be an object with keys "
                         "version, matches and idempotency")

    version = data["version"]
    if not _is_plain_int(version) or version != _VERSION:
        raise ValueError("unsupported ledger version")

    matches_raw = data["matches"]
    idempotency_raw = data["idempotency"]
    if not isinstance(matches_raw, dict) or not isinstance(idempotency_raw, dict):
        raise ValueError("matches and idempotency must be objects")

    matches: dict[str, dict[str, str]] = {}
    for name, record in matches_raw.items():
        if not isinstance(name, str) or not name:
            raise ValueError("job ids must be non-empty strings")
        if not isinstance(record, dict) or set(record.keys()) != set(_MATCH_FIELDS):
            raise ValueError("match record has invalid fields")
        if record["job_id"] != name:
            raise ValueError("match record id does not match its key")
        resource_id = record["resource_id"]
        if not isinstance(resource_id, str) or not resource_id:
            raise ValueError("resource_id must be a non-empty string")
        if name not in job_ids:
            raise ValueError("match record must reference a registered job")
        if resource_id not in resource_ids:
            raise ValueError("match record must reference a registered offer")
        matches[name] = {"job_id": name, "resource_id": resource_id}

    idempotency: dict[str, str] = {}
    referenced: set[str] = set()
    for key, job_id in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        if not isinstance(job_id, str) or not job_id or job_id not in matches:
            raise ValueError("idempotency entry must reference a recorded match")
        if job_id in referenced:
            raise ValueError("job matched under more than one idempotency key")
        referenced.add(job_id)
        idempotency[key] = job_id

    return matches, idempotency


def _load_ledger(
    realpath: str,
    job_ids: dict[str, Any],
    resource_ids: dict[str, Any],
) -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    try:
        with open(realpath, encoding="utf-8") as handle:
            text = handle.read()
    except FileNotFoundError:
        return {}, {}

    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(f"ledger file {realpath!r} is not valid JSON") from exc
    return _validate_ledger(data, job_ids, resource_ids)


def _atomic_write(
    realpath: str,
    matches: dict[str, dict[str, str]],
    idempotency: dict[str, str],
) -> None:
    payload = {
        "version": _VERSION,
        "matches": {name: matches[name] for name in sorted(matches)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"

    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".ledger-",
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

    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def match(
    jobs: str,
    offers: str,
    ledger: str,
    job_id: str,
    key: str,
) -> tuple[dict[str, object], bool]:
    """Match a registered job to the greenest feasible offer idempotently.

    Candidates are offers whose region is permitted by the job's
    residency_regions and whose remaining capacity (capacity_wh minus the
    energy already allocated in the ledger) covers the job's energy_wh;
    the winner is the first by ascending carbon_intensity, unit_cost and
    resource_id. Returns ``(record, created)`` where ``record`` carries
    job_id and resource_id; ``created`` is ``True`` for a new match and
    ``False`` when the idempotency key replays a match for the same job
    (which allocates no further capacity).
    """
    for value in (jobs, offers, ledger, job_id, key):
        if not isinstance(value, str) or not value:
            raise ValueError("jobs, offers, ledger, job_id and key must be "
                             "non-empty strings")

    store = _get_store(ledger)
    with store.lock:
        job_records, _ = _load_registry(
            os.path.realpath(jobs), _jobs._validate_structure, "jobs registry")
        if job_id not in job_records:
            raise KeyError(job_id)
        job = job_records[job_id]

        offer_records, _ = _load_registry(
            os.path.realpath(offers), _offers._validate_structure,
            "offers registry")
        matches, idempotency = _load_ledger(
            store.realpath, job_records, offer_records)

        existing = idempotency.get(key)
        if existing is not None:
            if existing != job_id:
                raise ValueError("idempotency key was already used with a "
                                 "different job")
            return dict(matches[job_id]), False
        if job_id in matches:
            raise ValueError("job is already matched under another "
                             "idempotency key")

        allocated: dict[str, int] = {}
        for record in matches.values():
            resource_id = record["resource_id"]
            allocated[resource_id] = (
                allocated.get(resource_id, 0)
                + job_records[record["job_id"]]["energy_wh"])

        allowed = set(job["residency_regions"])
        needed = job["energy_wh"]
        best: tuple[int, int, str] | None = None
        for resource_id, offer in offer_records.items():
            if offer["region"] not in allowed:
                continue
            if offer["capacity_wh"] - allocated.get(resource_id, 0) < needed:
                continue
            candidate = (offer["carbon_intensity"], offer["unit_cost"],
                         resource_id)
            if best is None or candidate < best:
                best = candidate
        if best is None:
            raise LookupError("no feasible offer for job")

        record = {"job_id": job_id, "resource_id": best[2]}
        matches[job_id] = record
        idempotency[key] = job_id
        _atomic_write(store.realpath, matches, idempotency)
        return dict(record), True


# ---------------------------------------------------------------------------
# Version 2: submit-job / versioned-supply clearing ledger
# ---------------------------------------------------------------------------


def _candidate_copy(entry: dict[str, Any]) -> dict[str, Any]:
    # A snapshot entry is the feasibility result -- the resource record
    # with its nine fields in record order plus both totals -- rebuilt in
    # canonical field order and deep-copied so neither callers nor later
    # publications can mutate the ledger.
    resource = entry["resource"]
    canonical = {field: resource[field] for field in _resources._RECORD_FIELDS}
    canonical["residency"] = list(canonical["residency"])
    return {
        "resource": canonical,
        "total_cost": entry["total_cost"],
        "total_carbon": entry["total_carbon"],
    }


def _validate_candidate(entry: object, resources_seen: set[str]) -> None:
    if not isinstance(entry, dict) \
            or set(entry.keys()) != {"resource", "total_cost",
                                     "total_carbon"}:
        raise ValueError("clearing candidate has invalid fields")
    resource = entry["resource"]
    if not isinstance(resource, dict) \
            or set(resource.keys()) != set(_resources._RECORD_FIELDS):
        raise ValueError("clearing candidate resource has invalid fields")
    resource_id = resource["resource_id"]
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("candidate resource_id must be a non-empty string")
    if resource_id in resources_seen:
        raise ValueError("candidate snapshot must be distinct by resource")
    resources_seen.add(resource_id)
    if not _is_plain_int(resource["version"]) or resource["version"] < 1:
        raise ValueError("candidate resource version must be a positive "
                         "integer")
    if resource["resource_id"] != resource_id:
        raise ValueError("candidate resource id does not match its record")
    # The candidate must be a self-consistent resource record: plain
    # integer values (booleans rejected), a valid window and a sorted
    # residency list containing the resource's own region.
    _resources._check_values(resource)
    for name in ("total_cost", "total_carbon"):
        if not _is_plain_int(entry[name]) or entry[name] < 0:
            raise ValueError(f"candidate {name} must be a non-negative "
                             "integer")


def _validate_clear_ledger(
    data: object,
    job_ids: dict[str, Any],
    supply: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, dict[str, Any]], dict[str, str],
           dict[str, dict[str, Any]]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_CLEAR_ROOT_FIELDS):
        raise ValueError("clearing ledger root must be an object with keys "
                         "version, cleared, idempotency and audit")

    version = data["version"]
    if not _is_plain_int(version) or version != _CLEAR_VERSION:
        raise ValueError("unsupported clearing ledger version")

    cleared_raw = data["cleared"]
    idempotency_raw = data["idempotency"]
    audit_raw = data["audit"]
    if not isinstance(cleared_raw, dict) \
            or not isinstance(idempotency_raw, dict) \
            or not isinstance(audit_raw, dict):
        raise ValueError("cleared, idempotency and audit must be objects")
    if list(cleared_raw) != sorted(cleared_raw) \
            or list(idempotency_raw) != sorted(idempotency_raw) \
            or list(audit_raw) != sorted(audit_raw):
        raise ValueError("clearing ledger sections must be ordered by key "
                         "code point")

    records: dict[str, dict[str, Any]] = {}
    for name, record in cleared_raw.items():
        if not isinstance(name, str) or not name:
            raise ValueError("job ids must be non-empty strings")
        if not isinstance(record, dict) \
                or set(record.keys()) != set(_CLEAR_RECORD_FIELDS):
            raise ValueError("clearing record has invalid fields")
        if record["job_id"] != name or name not in job_ids:
            raise ValueError("clearing record must reference an accepted job")
        at = record["at"]
        work = record["work"]
        if not _is_plain_int(at) or at < 0:
            raise ValueError("clearing record at must be a non-negative "
                             "integer")
        if not _is_plain_int(work) or work <= 0:
            raise ValueError("clearing record work must be a positive "
                             "integer")

        candidates_raw = record["candidates"]
        if not isinstance(candidates_raw, list) or not candidates_raw:
            raise ValueError("clearing record must hold a non-empty "
                             "candidate snapshot")
        candidates: list[dict[str, Any]] = []
        seen: set[str] = set()
        rank_key = None
        for entry in candidates_raw:
            _validate_candidate(entry, seen)
            resource = entry["resource"]
            key = (resource["carbon_intensity"], resource["unit_cost"],
                   resource["resource_id"])
            if rank_key is not None and key < rank_key:
                raise ValueError("candidate snapshot must be ordered by "
                                 "carbon intensity, unit cost and resource id")
            rank_key = key
            candidates.append(_candidate_copy(entry))

        selected_raw = record["selected"]
        if not isinstance(selected_raw, dict) \
                or set(selected_raw.keys()) != set(_CLEAR_SELECTED_FIELDS):
            raise ValueError("clearing record selected has invalid fields")
        selected_id = selected_raw["resource_id"]
        selected_version = selected_raw["version"]
        if not isinstance(selected_id, str) or not selected_id \
                or not _is_plain_int(selected_version) \
                or selected_version < 1:
            raise ValueError("clearing record selected must name a resource "
                             "version")
        first_resource = candidates[0]["resource"]
        if selected_id != first_resource["resource_id"] \
                or selected_version != first_resource["version"]:
            raise ValueError("selected resource version must be the first "
                             "ordered candidate")
        records[name] = {"job_id": name, "at": at, "work": work,
                         "candidates": candidates,
                         "selected": {"resource_id": selected_id,
                                      "version": selected_version}}

    idempotency: dict[str, str] = {}
    referenced: set[str] = set()
    for key, job_id in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        if not isinstance(job_id, str) or not job_id or job_id not in records:
            raise ValueError("idempotency entry must reference a recorded "
                             "clearing")
        if job_id in referenced:
            raise ValueError("job cleared under more than one idempotency key")
        referenced.add(job_id)
        idempotency[key] = job_id

    events: dict[str, dict[str, Any]] = {}
    for key, event in audit_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event, dict) \
                or set(event.keys()) != set(_CLEAR_EVENT_FIELDS):
            raise ValueError("clearing audit event has invalid fields")
        event_key = event["key"]
        job_id = event["job_id"]
        resource_id = event["resource_id"]
        event_version = event["version"]
        if event_key != key or not isinstance(event_key, str) or not event_key:
            raise ValueError("audit event key does not match its map key")
        if not isinstance(job_id, str) or not job_id or job_id not in records:
            raise ValueError("audit event must reference a recorded clearing")
        if not isinstance(resource_id, str) or not resource_id:
            raise ValueError("audit event must name a resource")
        if not _is_plain_int(event_version) or event_version < 1:
            raise ValueError("audit event must name a resource version")
        events[key] = {"key": event_key, "job_id": job_id,
                       "resource_id": resource_id,
                       "version": event_version}

    # The three sections describe one clearing history: every idempotency
    # key binds one record and one event agreeing on the job, and every
    # recorded clearing is bound to exactly one event whose resource
    # version is the record's selected one.
    if set(idempotency) != set(events):
        raise ValueError("idempotency keys and audit events do not match")
    if set(records) != referenced:
        raise ValueError("every clearing must be bound to an idempotency key")
    for key, job_id in idempotency.items():
        event = events[key]
        record = records[job_id]
        if event["job_id"] != job_id \
                or event["resource_id"] != record["selected"]["resource_id"] \
                or event["version"] != record["selected"]["version"]:
            raise ValueError("audit event does not match its clearing record")

    # Every selected resource version must be one present in the current
    # supply history: no cross-version bookings, no dangling references.
    # Candidate snapshots are immutable history, too -- the stored
    # resource record must be the published version byte-for-byte, the
    # totals must equal work times the version's rates, and every entry
    # must still satisfy the feasibility predicates against its own
    # immutable job and evaluation moment.
    for record in records.values():
        job = job_ids[record["job_id"]]
        if record["work"] != job["work"]:
            raise ValueError("clearing record work must match its job")
        regions = set(job["regions"])
        residency = set(job["residency"])
        for entry in record["candidates"]:
            resource = entry["resource"]
            versions = supply.get(resource["resource_id"])
            if versions is None or resource["version"] > len(versions):
                raise ValueError("candidate references an unpublished "
                                 "resource version")
            published = versions[resource["version"] - 1]
            if resource != published:
                raise ValueError("candidate resource record must match its "
                                 "published version")
            if not (resource["start"] <= record["at"] <= resource["end"]):
                raise ValueError("candidate must be valid at the record "
                                 "evaluation moment")
            if resource["region"] not in regions:
                raise ValueError("candidate region must be permitted by its "
                                 "job")
            if not residency <= set(resource["residency"]):
                raise ValueError("candidate residency must cover its job")
            if resource["end"] < job["deadline"]:
                raise ValueError("candidate must cover its job deadline")
            if entry["total_cost"] != record["work"] * resource["unit_cost"] \
                    or entry["total_cost"] > job["max_cost"] \
                    or entry["total_carbon"] != record["work"] \
                    * resource["carbon_intensity"] \
                    or entry["total_carbon"] > job["carbon_cap"]:
                raise ValueError("candidate totals must match the job and "
                                 "respect its budgets")

        selected = record["selected"]
        versions = supply.get(selected["resource_id"])
        if versions is None or selected["version"] > len(versions):
            raise ValueError("clearing record selects an unpublished "
                             "resource version")

    return records, idempotency, events


def _load_clear_ledger(
    realpath: str,
    jobs: dict[str, Any],
    supply: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, dict[str, Any]], dict[str, str],
           dict[str, dict[str, Any]], bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return {}, {}, {}, None

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"clearing ledger {realpath!r} is not valid UTF-8") from exc
    try:
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"clearing ledger {realpath!r} is not valid JSON") from exc
    records, idempotency, events = _validate_clear_ledger(data, jobs, supply)
    # The ledger is canonical compact UTF-8 JSON with a single trailing
    # newline; a re-serialization that does not reproduce the exact bytes
    # means extra whitespace, escaped non-ASCII, wrong field/key order or
    # a missing/duplicated newline.
    canonical = _serialize_clear_ledger(records, idempotency, events)
    if canonical != raw:
        raise ValueError(
            f"clearing ledger {realpath!r} is not canonical compact UTF-8 "
            "JSON")
    return records, idempotency, events, raw


def _serialize_clear_ledger(
    records: dict[str, dict[str, Any]],
    idempotency: dict[str, str],
    events: dict[str, dict[str, Any]],
) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, every section's
    # primary keys in code-point order, terminated by exactly one newline.
    payload = {
        "version": _CLEAR_VERSION,
        "cleared": {job_id: records[job_id] for job_id in sorted(records)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
        "audit": {key: events[key] for key in sorted(events)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _fsync_directory_clear(directory: str) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _rollback_clear_ledger(realpath: str, directory: str,
                           old_bytes: bytes | None,
                           first: BaseException) -> None:
    # Restore the exact pre-call bytes while the ledger's exclusive lock
    # is held, or remove a ledger that did not exist beforehand.
    try:
        if old_bytes is None:
            try:
                os.unlink(realpath)
            except FileNotFoundError:
                pass
        else:
            fd, tmp_path = tempfile.mkstemp(
                dir=directory, prefix=".clear-restore-", suffix=".tmp")
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(old_bytes)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp_path, realpath)
            except BaseException:
                with contextlib.suppress(OSError):
                    os.unlink(tmp_path)
                raise
        _fsync_directory_clear(directory)
    except OSError as recovery:
        raise recovery from first


def _commit_clear_ledger(realpath: str, payload: bytes,
                         old_bytes: bytes | None) -> None:
    # Same-directory synced temporary file, atomic replace and a
    # directory fsync; any failure after the replace restores the
    # pre-call bytes, so an unsuccessful clearing leaves the original
    # ledger byte-for-byte and never leaves temporary fragments.
    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".clear-", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, realpath)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise
    try:
        _fsync_directory_clear(directory)
    except BaseException as first:
        _rollback_clear_ledger(realpath, directory, old_bytes, first)
        raise


def _rank_candidates(
    job: dict[str, Any],
    history: dict[str, list[dict[str, Any]]],
    at: int,
    reserved: dict[tuple[str, int], int],
) -> list[dict[str, Any]]:
    # Feasibility semantics identical to resources.feasible: each
    # resource contributes its highest version valid at at, and the
    # region, residency, deadline and budget filters are applied to that
    # version. Capacity is additionally reduced by the work already
    # cleared against that exact resource version, so concurrent
    # clearings can neither oversell a version nor borrow capacity from
    # another version.
    work = job["work"]
    regions = set(job["regions"])
    residency = set(job["residency"])
    candidates: list[dict[str, Any]] = []
    for records in history.values():
        active: dict[str, Any] | None = None
        for record in records:
            if record["start"] <= at <= record["end"]:
                active = record
        if active is None:
            continue
        if active["region"] not in regions:
            continue
        remaining = active["capacity"] - reserved.get(
            (active["resource_id"], active["version"]), 0)
        if remaining < work:
            continue
        if not residency <= set(active["residency"]):
            continue
        if active["end"] < job["deadline"]:
            continue
        total_cost = work * active["unit_cost"]
        total_carbon = work * active["carbon_intensity"]
        if total_cost > job["max_cost"] \
                or total_carbon > job["carbon_cap"]:
            continue
        candidates.append({
            "resource": dict(active),
            "total_cost": total_cost,
            "total_carbon": total_carbon,
        })
    candidates.sort(key=lambda entry: (entry["resource"]["carbon_intensity"],
                                       entry["resource"]["unit_cost"],
                                       entry["resource"]["resource_id"]))
    return candidates


def clear(
    jobs: str,
    supply: str,
    ledger: str,
    job_id: str,
    key: str,
    at: int,
) -> tuple[dict[str, Any], bool]:
    """Clear one accepted job against the versioned supply idempotently.

    ``jobs``, ``supply`` and ``ledger`` are paths and ``job_id`` and
    ``key`` non-empty strings; ``at`` is a non-boolean non-negative
    evaluation moment; the three resolved real paths must be pairwise
    distinct. Any deviation raises ``ValueError`` before any business
    file is read.

    The acceptance file and the supply file are read together as one
    snapshot under their shared locks, and only the highest version of
    each resource valid at ``at`` is considered. Region, residency,
    deadline, cost and carbon-budget feasibility follow
    ``resources.feasible`` exactly; each candidate version's capacity is
    additionally reduced by the work of jobs already cleared against
    that version in the ledger, so no version is oversold and bookings
    never cross versions. Remaining candidates are ordered by carbon
    intensity, unit cost and resource id; the first is the unique
    selected resource.

    Returns ``(record, created)``; the record binds the job, the
    evaluation moment, the job's work, the ordered candidate snapshot
    and the selected resource version. The ledger is created only on
    the first successful clearing, committing the record, the
    idempotency binding and the request-key/resource-version audit
    event in one canonical, synced atomic write.

    Replaying the same key for the same job and moment returns the
    stored record with ``False`` without reselecting, appending events
    or rewriting bytes. The same key with a different request, or a job
    already cleared under another key, raises ``ValueError`` and leaves
    the ledger untouched. An unknown job raises ``KeyError`` and no
    remaining feasible resource ``LookupError``; neither creates the
    ledger. A missing input file or ledger parent directory raises
    ``FileNotFoundError``; invalid structure, references, ordering or
    canonical bytes raise ``ValueError``; other locking or I/O failures
    raise ``OSError``.
    """
    for value in (jobs, supply, ledger, job_id, key):
        if not isinstance(value, str) or not value:
            raise ValueError("jobs, supply, ledger, job_id and key must be "
                             "non-empty strings")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")

    job_real = os.path.realpath(jobs)
    supply_real = os.path.realpath(supply)
    ledger_real = os.path.realpath(ledger)
    if len({job_real, supply_real, ledger_real}) != 3:
        raise ValueError("jobs, supply and ledger paths must be distinct "
                         "real paths")

    store = _get_store(ledger)
    # All three files are locked in one global real-path order together
    # with every other market/resources caller, so concurrent reads and
    # clearings can never deadlock or observe a half-committed ledger.
    # Input snapshots take shared locks, the ledger an exclusive one.
    order = sorted(((job_real, "shared"), (supply_real, "shared"),
                    (ledger_real, "exclusive")))
    with store.lock:
        with contextlib.ExitStack() as stack:
            for real, mode in order:
                stack.enter_context(
                    _resources._process_lock(real, shared=(mode == "shared")))

            accepted, _job_map, _job_events, job_raw = _jobs._load_submit_file(
                job_real)
            if job_raw is None:
                raise FileNotFoundError(
                    f"acceptance file {job_real!r} does not exist")
            history, _supply_map, _supply_events, supply_raw = \
                _resources._load_file(supply_real)
            if supply_raw is None:
                raise FileNotFoundError(
                    f"supply file {supply_real!r} does not exist")

            job = accepted.get(job_id)
            if job is None:
                raise KeyError(job_id)

            records, idempotency, events, old_bytes = _load_clear_ledger(
                ledger_real, accepted, history)

            existing_job = idempotency.get(key)
            if existing_job is not None:
                if existing_job != job_id:
                    raise ValueError("idempotency key was already used with "
                                     "a different job")
                record = records[job_id]
                if record["at"] != at:
                    raise ValueError("idempotency key was already used at a "
                                     "different evaluation moment")
                return {
                    "job_id": job_id,
                    "at": record["at"],
                    "work": record["work"],
                    "candidates": [_candidate_copy(entry)
                                   for entry in record["candidates"]],
                    "selected": dict(record["selected"]),
                }, False
            if job_id in records:
                raise ValueError("job is already cleared under another "
                                 "idempotency key")

            reserved: dict[tuple[str, int], int] = {}
            for prior in records.values():
                selected = prior["selected"]
                slot = (selected["resource_id"], selected["version"])
                reserved[slot] = reserved.get(slot, 0) + prior["work"]

            candidates = _rank_candidates(job, history, at, reserved)
            if not candidates:
                raise LookupError("no remaining feasible resource for job")

            winner = candidates[0]["resource"]
            record = {
                "job_id": job_id,
                "at": at,
                "work": job["work"],
                "candidates": [_candidate_copy(entry) for entry in candidates],
                "selected": {"resource_id": winner["resource_id"],
                             "version": winner["version"]},
            }
            records[job_id] = record
            idempotency[key] = job_id
            events[key] = {
                "key": key,
                "job_id": job_id,
                "resource_id": winner["resource_id"],
                "version": winner["version"],
            }
            # Opening the lock file in a missing directory already
            # surfaced FileNotFoundError before the ledger is created.
            _commit_clear_ledger(
                ledger_real,
                _serialize_clear_ledger(records, idempotency, events),
                old_bytes)
            return {
                "job_id": job_id,
                "at": at,
                "work": job["work"],
                "candidates": [_candidate_copy(entry) for entry in candidates],
                "selected": {"resource_id": winner["resource_id"],
                             "version": winner["version"]},
            }, True
