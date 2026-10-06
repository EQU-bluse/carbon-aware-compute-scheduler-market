"""Persistent, thread-safe job-to-offer match and clearing ledgers."""

from __future__ import annotations

import contextlib
import copy
import json
import os
import tempfile
from typing import Any, Callable

from . import _lifecycle
from . import jobs as _jobs
from . import offers as _offers
from . import resources as _resources
from . import signals as _signals
from ._jsonio import finite_loads, strict_loads

__all__ = ["match", "clear", "clear_live", "clear_live_batch"]

_VERSION = 1
_MATCH_FIELDS = ("job_id", "resource_id")
_ROOT_FIELDS = ("version", "matches", "idempotency")

# The in-process mutex registry and the companion flock live in the
# shared lifecycle infrastructure; the names are kept as the module's
# own seams.
_Store = _lifecycle.Store
_get_store = _lifecycle.get_store
_clear_lock = _lifecycle.file_lock


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
# Version 2: market clearing over jobs.submit acceptances and supply versions
# ---------------------------------------------------------------------------

_CLEAR_VERSION = 2
_CLEAR_ROOT_FIELDS = ("version", "trades", "idempotency", "audit")
_TRADE_FIELDS = ("job_id", "at", "work", "resource_id", "version",
                 "candidates", "selection")
_SELECTION_FIELDS = ("resource_id", "version")
_EVENT_FIELDS = ("key", "job_id", "at", "resource_id", "version")
_STATIC_CANDIDATE_FIELDS = ("resource", "total_cost", "total_carbon")
_LIVE_CANDIDATE_FIELDS = ("resource", "signal", "total_cost",
                          "total_carbon")


def _check_sorted_keys(mapping: dict[Any, Any], label: str) -> None:
    keys = list(mapping)
    if keys != sorted(keys):
        raise ValueError(f"{label} must be ordered by key code point")


def _validate_signal(
    signal: object,
    resource_region: str,
    at: int,
    signal_history: dict[str, list[dict[str, Any]]] | None,
) -> tuple[int, int]:
    # Returns the binary numeric pair (unit_cost, carbon_intensity) the
    # caller prices on, never the signal record itself. A live candidate
    # freezes the exact published signal record -- region, version and
    # all observations -- so a later signal publish cannot change what an
    # earlier trade was decided under. When the
    # signal snapshot is available (clear_live), the frozen record must
    # be the exact published version. Readers that never take the signal
    # file (static clear, dispatch, execution) still validate the frozen
    # record structurally and arithmetically: its totals, window and
    # budget relations are all decidable from the frozen bytes, so their
    # invariants hold without resolving the reference.
    if not isinstance(signal, dict) \
            or set(signal.keys()) != set(_signals._RECORD_FIELDS):
        raise ValueError("trade candidate signal has invalid fields")
    region = signal["region"]
    version = signal["version"]
    if region != resource_region:
        raise ValueError("trade candidate signal must cover the resource "
                         "region")
    if not _is_plain_int(version) or version < 1:
        raise ValueError("candidate signal version must be a positive "
                         "integer")
    # Validate a copy: _check_values normalizes the mix order in place,
    # and the parsed record's raw key order must be left untouched so the
    # ledger's canonical-byte comparison still rejects a reordered mix.
    signal_copy = dict(signal)
    signal_copy["mix"] = dict(signal["mix"])
    _signals._check_values(signal_copy)
    if signal_history is not None:
        records = signal_history.get(region)
        if records is None or version > len(records) \
                or dict(signal) != records[version - 1]:
            raise ValueError("trade candidate signal must reference a "
                             "published signal version")
    if not (signal["observed"] <= at <= signal["expires"]):
        raise ValueError("trade candidate signal must be valid at the "
                         "trade's evaluation moment")
    return signal["unit_cost"], signal["carbon_intensity"]


def _validate_candidate(
    entry: object,
    work: int,
    at: int,
    job: dict[str, Any],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]] | None,
    seen: set[str],
) -> tuple[bool, int, int, str]:
    if not isinstance(entry, dict) \
            or set(entry.keys()) not in (set(_STATIC_CANDIDATE_FIELDS),
                                         set(_LIVE_CANDIDATE_FIELDS)):
        raise ValueError("trade candidate has invalid fields")
    live = "signal" in entry
    resource = entry["resource"]
    if not isinstance(resource, dict) \
            or set(resource.keys()) != set(_resources._RECORD_FIELDS):
        raise ValueError("trade candidate resource has invalid fields")
    # Strict value typing (booleans rejected), the same rules a
    # published version had to satisfy.
    _resources._check_values(resource)
    resource_id = resource["resource_id"]
    version = resource["version"]
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("candidate resource_id must be a non-empty string")
    if not _is_plain_int(version) or version < 1:
        raise ValueError("candidate version must be a positive integer")
    # The versioned record must be the exact, immutable supply record.
    records = history.get(resource_id)
    if records is None or version > len(records) \
            or dict(resource) != records[version - 1]:
        raise ValueError("trade candidate must reference a published "
                         "resource version")
    if resource_id in seen:
        raise ValueError("trade candidates must be distinct resources")
    seen.add(resource_id)
    if not (resource["start"] <= at <= resource["end"]):
        raise ValueError("trade candidate must be valid at the trade's "
                         "evaluation moment")
    # Static trades price on the version's baked-in figures; live trades
    # price on the frozen signal for the same region.
    if live:
        unit_cost, carbon_intensity = _validate_signal(
            entry["signal"], resource["region"], at, signal_history)
    else:
        unit_cost = resource["unit_cost"]
        carbon_intensity = resource["carbon_intensity"]
    total_cost = entry["total_cost"]
    total_carbon = entry["total_carbon"]
    if not _is_plain_int(total_cost) or total_cost < 0 \
            or total_cost != work * unit_cost:
        raise ValueError("trade candidate total_cost is invalid")
    if not _is_plain_int(total_carbon) or total_carbon < 0 \
            or total_carbon != work * carbon_intensity:
        raise ValueError("trade candidate total_carbon is invalid")
    # The snapshot must still describe the same feasibility the trade
    # was decided under; published versions and accepted jobs never
    # change, so a valid trade stays valid forever.
    if resource["region"] not in set(job["regions"]):
        raise ValueError("trade candidate is outside the job's regions")
    if not set(job["residency"]) <= set(resource["residency"]):
        raise ValueError("trade candidate does not cover job residency")
    if resource["end"] < job["deadline"]:
        raise ValueError("trade candidate does not cover the job deadline")
    if total_cost > job["max_cost"] \
            or total_carbon > job["carbon_cap"]:
        raise ValueError("trade candidate exceeds a job budget")
    return live, carbon_intensity, unit_cost, resource_id


def _validate_trade(
    record: object,
    accepted: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]] | None,
) -> dict[str, Any]:
    if not isinstance(record, dict) \
            or set(record.keys()) != set(_TRADE_FIELDS):
        raise ValueError("trade record has invalid fields")
    job_id = record["job_id"]
    at = record["at"]
    work = record["work"]
    resource_id = record["resource_id"]
    version = record["version"]
    if not isinstance(job_id, str) or not job_id:
        raise ValueError("trade job_id must be a non-empty string")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("trade at must be a non-boolean non-negative "
                         "integer")
    if not _is_plain_int(work) or work <= 0:
        raise ValueError("trade work must be a positive integer")
    if not isinstance(resource_id, str) or not resource_id:
        raise ValueError("trade resource_id must be a non-empty string")
    if not _is_plain_int(version) or version < 1:
        raise ValueError("trade version must be a positive integer")
    selection = record["selection"]
    if not isinstance(selection, dict) \
            or set(selection.keys()) != set(_SELECTION_FIELDS):
        raise ValueError("trade selection has invalid fields")
    if selection["resource_id"] != resource_id \
            or not isinstance(selection["resource_id"], str):
        raise ValueError("trade selection does not name its resource")
    if selection["version"] != version:
        raise ValueError("trade selection does not name its version")

    job = accepted.get(job_id)
    if job is None:
        raise ValueError("trade must reference an accepted job")
    if work != job["work"]:
        raise ValueError("trade work does not match its accepted job")
    records = history.get(resource_id)
    if records is None or version > len(records):
        raise ValueError("trade must reference a published resource version")

    candidates_raw = record["candidates"]
    if not isinstance(candidates_raw, list) or not candidates_raw:
        raise ValueError("trade candidates must be a non-empty list")
    candidates: list[dict[str, Any]] = []
    last_rank: tuple[int, int, str] | None = None
    seen: set[str] = set()
    live_kind: bool | None = None
    for entry_raw in candidates_raw:
        live, carbon, unit_cost, candidate_id = _validate_candidate(
            entry_raw, work, at, job, history, signal_history, seen)
        # A trade is decided under exactly one pricing mode: static
        # trades never carry a signal, live trades always carry one.
        if live_kind is None:
            live_kind = live
        elif live != live_kind:
            raise ValueError("trade candidates must all be static or all "
                             "live")
        rank = (carbon, unit_cost, candidate_id)
        if last_rank is not None and rank < last_rank:
            raise ValueError("trade candidates must be ordered by carbon "
                             "intensity, unit cost and resource id")
        last_rank = rank
        # Keep the parsed key order: the candidate's resource must equal
        # the supply record field-for-field and its frozen signal -- when
        # resolvable -- the published signal, while the ledger's
        # canonical-byte comparison rejects any reordered nested object.
        candidates.append(copy.deepcopy(entry_raw))

    # The first ordered candidate is the unique winner.
    winner = candidates[0]["resource"]
    if winner["resource_id"] != resource_id or winner["version"] != version:
        raise ValueError("trade selection must be the first ordered "
                         "candidate")
    return {
        "job_id": job_id,
        "at": at,
        "work": work,
        "resource_id": resource_id,
        "version": version,
        "candidates": candidates,
        "selection": {"resource_id": resource_id, "version": version},
    }


def _completion_root_shape(data: object) -> bool:
    # Recognize a completion ledger sibling purely by its canonical
    # root shape, which is distinct from every other ledger's (the
    # settlement root carries "records" where this carries
    # "completions").
    return isinstance(data, dict) and frozenset(data.keys()) == frozenset(
        ("version", "completions", "idempotency", "audit"))


def _discover_completion_paths(anchor_reals: tuple[str, ...]) -> list[str]:
    # Scan each anchor directory once for completion ledgers. Malformed
    # or unrelated siblings are ignored; a sibling with the completion
    # root shape that fails full validation is still returned and the
    # full loader rejects it.
    found: list[str] = []
    seen_dirs: set[str] = set()
    for anchor in anchor_reals:
        directory = os.path.dirname(anchor) or "."
        if directory in seen_dirs:
            continue
        seen_dirs.add(directory)
        try:
            names = os.listdir(directory)
        except OSError:
            continue
        for name in names:
            if not name.endswith(".json"):
                continue
            real = os.path.realpath(os.path.join(directory, name))
            if real in found or real in set(anchor_reals):
                continue
            try:
                with open(real, "rb") as handle:
                    raw = handle.read()
                data = finite_loads(raw.decode("utf-8"))
            except (OSError, ValueError, UnicodeDecodeError):
                continue
            if _completion_root_shape(data):
                found.append(real)
    found.sort()
    return found


def _load_completion_union(
    paths: list[str],
    accepted: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    # The union of completion records across the explicit ledger and the
    # ledgers discovered beside the snapshots, keyed by job id. Ledgers
    # are fully validated against the same acceptance and supply
    # snapshots; a malformed completion ledger raises just like any
    # other malformed business ledger.
    from . import completion as _completion
    records: dict[str, dict[str, Any]] = {}
    for real in paths:
        ledger_records, _ids, _events, raw = \
            _completion._load_completion_ledger(real, accepted, history)
        if raw is None:
            raise FileNotFoundError(
                f"completion ledger {real!r} does not exist")
        for record in ledger_records.values():
            prior = records.get(record["job_id"])
            if prior is not None and prior != record:
                raise ValueError(
                    "a job is completed differently in two completion "
                    "ledgers")
            records.setdefault(record["job_id"], record)
    return records


def _completion_capacity_envelope(
    trades: dict[str, dict[str, Any]],
    completed: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
    cancelled: dict[str, dict[str, Any]] | None = None,
) -> None:
    # Replay every capacity-changing event per exact resource version:
    # a trade books its work at its evaluation moment, and the job's
    # completion releases that traded occupancy again at the completion
    # moment regardless of its migration history -- completion is the
    # terminal event, after which a job occupies nothing. A recorded
    # cancellation likewise releases the traded occupancy at the
    # cancellation moment: the job never entered dispatch, so nothing
    # but the booking itself is given back. Releasing at the completion
    # moment is conservative for jobs that migrated away
    # earlier (their source frees at the switch, which this trades-only
    # view cannot see): reuse between switch and completion stays
    # counted, reuse from the completion moment on is accepted. Releases
    # are applied before bookings at the same moment, since releases
    # effective "not later than" that moment free the capacity. Capacity
    # must never be oversold at any booking moment.
    def capacity_of(slot: tuple[str, int]) -> int:
        resource_id, version = slot
        return history[resource_id][version - 1]["capacity"]

    events: dict[tuple[str, int], list[tuple[int, int, int]]] = {}
    for job_id, trade in trades.items():
        slot = (trade["resource_id"], trade["version"])
        events.setdefault(slot, []).append(
            (trade["at"], 1, trade["work"]))
        record = completed.get(job_id)
        if record is not None:
            events.setdefault(slot, []).append(
                (record["at"], 0, trade["work"]))
        if cancelled:
            cancellation = cancelled.get(job_id)
            if cancellation is not None:
                events.setdefault(slot, []).append(
                    (cancellation["at"], 0, trade["work"]))
    for slot, slot_events in events.items():
        running = 0
        # Releases (kind 0) before bookings (kind 1) at the same moment.
        for at, kind, work in sorted(slot_events):
            if kind == 0:
                running -= work
                if running < 0:
                    raise ValueError(
                        "completion releases capacity a trade never booked")
            else:
                running += work
                if running > capacity_of(slot):
                    raise ValueError(
                        "ledger oversells a published resource version")


def _validate_clear_ledger(
    data: object,
    accepted: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]] | None = None,
    completed: dict[str, dict[str, Any]] | None = None,
    cancelled: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    if not isinstance(data, dict) or set(data.keys()) != set(_CLEAR_ROOT_FIELDS):
        raise ValueError("clearing ledger root must be an object with keys "
                         "version, trades, idempotency and audit")
    version = data["version"]
    if not _is_plain_int(version) or version != _CLEAR_VERSION:
        raise ValueError("unsupported clearing ledger version")

    trades_raw = data["trades"]
    idempotency_raw = data["idempotency"]
    audit_raw = data["audit"]
    if not isinstance(trades_raw, dict) \
            or not isinstance(idempotency_raw, dict) \
            or not isinstance(audit_raw, dict):
        raise ValueError("trades, idempotency and audit must be objects")
    _check_sorted_keys(trades_raw, "trades")
    _check_sorted_keys(idempotency_raw, "idempotency")
    _check_sorted_keys(audit_raw, "audit")

    trades: dict[str, dict[str, Any]] = {}
    for job_id, record in trades_raw.items():
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("trade job ids must be non-empty strings")
        trade = _validate_trade(record, accepted, history, signal_history)
        if trade["job_id"] != job_id:
            raise ValueError("trade record id does not match its key")
        trades[job_id] = trade

    idempotency: dict[str, dict[str, Any]] = {}
    events: dict[str, dict[str, Any]] = {}
    referenced: set[str] = set()
    for key, binding in idempotency_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("idempotency keys must be non-empty strings")
        if not isinstance(binding, dict) \
                or set(binding.keys()) != {"job_id", "at"}:
            raise ValueError("idempotency binding has invalid fields")
        bound_job = binding["job_id"]
        bound_at = binding["at"]
        if not isinstance(bound_job, str) or not bound_job \
                or bound_job not in trades:
            raise ValueError("idempotency entry must reference a recorded "
                             "trade")
        if not _is_plain_int(bound_at) or bound_at < 0:
            raise ValueError("idempotency entry must bind an evaluation "
                             "moment")
        if bound_job in referenced:
            raise ValueError("job traded under more than one idempotency "
                             "key")
        referenced.add(bound_job)
        trade = trades[bound_job]
        if trade["at"] != bound_at:
            raise ValueError("idempotency entry does not match its trade")
        idempotency[key] = {"job_id": bound_job, "at": bound_at}

    for key, event in audit_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("audit keys must be non-empty strings")
        if not isinstance(event, dict) \
                or set(event.keys()) != set(_EVENT_FIELDS):
            raise ValueError("clear audit event has invalid fields")
        event_key = event["key"]
        job_id = event["job_id"]
        at = event["at"]
        resource_id = event["resource_id"]
        event_version = event["version"]
        if event_key != key or not isinstance(event_key, str) or not event_key:
            raise ValueError("audit event key does not match its map key")
        if not isinstance(job_id, str) or not job_id or job_id not in trades:
            raise ValueError("audit event must reference a recorded trade")
        if not _is_plain_int(at) or at < 0:
            raise ValueError("audit event at must be a non-negative integer")
        if not isinstance(resource_id, str) or not resource_id:
            raise ValueError("audit event resource_id must be a non-empty "
                             "string")
        if not _is_plain_int(event_version) or event_version < 1:
            raise ValueError("audit event version must be a positive integer")
        trade = trades[job_id]
        if at != trade["at"] or resource_id != trade["resource_id"] \
                or event_version != trade["version"]:
            raise ValueError("audit event does not match its trade")
        events[key] = {"key": event_key, "job_id": job_id, "at": at,
                       "resource_id": resource_id, "version": event_version}

    # The three sections describe one clearing history: each idempotency
    # key binds one trade and one event, and each trade is bound exactly
    # once.
    if set(idempotency) != set(events):
        raise ValueError("idempotency keys and audit events do not match")
    for key, binding in idempotency.items():
        event = events[key]
        if event["job_id"] != binding["job_id"] or event["at"] != binding["at"]:
            raise ValueError("audit event does not match its idempotency "
                             "entry")
    if set(trades) != referenced:
        raise ValueError("every trade must be bound to an idempotency key")

    # Recorded bookings may not oversell any published version at any
    # moment. Without a completion or cancellation snapshot the
    # historical aggregate is the same as before; with one, releases
    # complete bookings opened and only the running envelope must stay
    # within capacity.
    if completed or cancelled:
        _completion_capacity_envelope(trades, completed or {}, history,
                                      cancelled)
    else:
        booked: dict[tuple[str, int], int] = {}
        for trade in trades.values():
            slot = (trade["resource_id"], trade["version"])
            booked[slot] = booked.get(slot, 0) + trade["work"]
        for (resource_id, version), amount in booked.items():
            record = history[resource_id][version - 1]
            if record["capacity"] < amount:
                raise ValueError(
                    "ledger oversells a published resource version")

    return trades, idempotency


def _canonical_clear_bytes(
    trades: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, Any]],
) -> bytes:
    audit = {key: {
        "key": key,
        "job_id": trades[binding["job_id"]]["job_id"],
        "at": binding["at"],
        "resource_id": trades[binding["job_id"]]["resource_id"],
        "version": trades[binding["job_id"]]["version"],
    } for key, binding in idempotency.items()}
    payload = {
        "version": _CLEAR_VERSION,
        "trades": {job_id: trades[job_id] for job_id in sorted(trades)},
        "idempotency": {key: idempotency[key] for key in sorted(idempotency)},
        "audit": {key: audit[key] for key in sorted(audit)},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_clear_ledger(
    realpath: str,
    accepted: dict[str, dict[str, Any]],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]] | None = None,
    completion_paths: list[str] | None = None,
    cancelled: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return {}, {}, None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"clearing ledger {realpath!r} is not valid UTF-8") from exc
    try:
        # Negative-zero and non-finite literals are format errors.
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"clearing ledger {realpath!r} is not valid JSON") from exc
    # Completions open booked capacity again. The completion paths to
    # observe are supplied by the caller, which discovers siblings
    # *before* taking its locks and shares every completion lock in the
    # one global resolved-path order, so a concurrent completion that
    # holds the completion lock exclusively can never deadlock against
    # this read. The running-envelope check then accepts a trades ledger
    # whose capacity was legitimately reused after a completion.
    completed = _load_completion_union(
        completion_paths or (), accepted, history)
    trades, idempotency = _validate_clear_ledger(
        data, accepted, history, signal_history, completed, cancelled)
    # As for the supply file, the ledger is accepted only in canonical
    # compact form with a single trailing newline.
    if raw != _canonical_clear_bytes(trades, idempotency):
        raise ValueError(
            f"clearing ledger {realpath!r} is not in canonical compact form")
    return trades, idempotency, raw


def _fsync_directory(directory: str) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _rollback_clear(realpath: str, directory: str, old_bytes: bytes | None,
                    first: BaseException) -> None:
    # Restore the exact pre-call bytes while the exclusive lock is held,
    # or remove a ledger that did not exist beforehand, then sync the
    # directory. A failed recovery chains after the original error.
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
        _fsync_directory(directory)
    except OSError as recovery:
        raise recovery from first


def _commit_clear(realpath: str, payload: bytes,
                  old_bytes: bytes | None) -> None:
    # One durable commit for the trade, the idempotency binding and the
    # audit event: synced same-directory temporary, atomic replace and a
    # directory fsync, restoring the pre-call bytes on any failure.
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
        _fsync_directory(directory)
    except BaseException as first:
        _rollback_clear(realpath, directory, old_bytes, first)
        raise


def clear(
    jobs: str,
    supply: str,
    ledger: str,
    job_id: str,
    key: str,
    at: int,
    completions: str | None = None,
    cancellations: str | None = None,
) -> tuple[dict[str, object], bool]:
    """Clear one accepted job against the versioned supply idempotently.

    ``jobs``, ``supply`` and ``ledger`` paths, ``job_id`` and ``key``
    must be non-empty strings and ``at`` a non-boolean non-negative
    integer evaluation moment; the three paths must also resolve to
    distinct real locations. Any violation raises ``ValueError`` before
    a business file is read.

    The ``jobs.submit`` acceptance file and the resource supply file are
    read as one snapshot under their shared locks together with the
    ledger's exclusive lock, all three taken in resolved real-path
    order. Only each resource's highest version valid at ``at`` is
    considered, and feasibility -- region, residency, cost and carbon
    budgets, deadline coverage and the candidate entries -- follows
    :func:`resources.feasible`. A version's capacity is additionally
    reduced by the work of every trade already booked against that exact
    resource version in the ledger, so capacity is never oversold and
    bookings never cross versions. Remaining candidates are ordered by
    carbon intensity, unit cost and resource id; the first is the unique
    winner.

    Returns ``(trade, created)``; the trade binds the job, the
    evaluation moment, the work, the ordered candidate snapshot and a
    versioned selection. A missing ledger is created only on the first
    trade, the trade, its idempotency binding and the audit event
    binding the key, resource id and version committed in one synced
    atomic write. Replaying the same key with the same job and
    evaluation moment returns the stored trade with ``False`` without
    reselecting, appending or rewriting. The same key with a different
    request, or a job already traded under another key, raises
    ``ValueError`` with the ledger untouched.

    An unknown job raises ``KeyError``; no remaining feasible resource
    raises ``LookupError``; neither creates the ledger. Missing input
    files or a missing ledger parent raise ``FileNotFoundError`` without
    leaving a ledger or temporary fragment. Invalid structure,
    references, ordering or canonical bytes raise ``ValueError``; other
    locking, read/write or sync failures raise ``OSError``.

    When ``cancellations`` names a cancellation ledger, it is read as
    part of the same snapshot under its shared lock: a trade whose job
    was cancelled at a moment not later than ``at`` no longer deducts
    capacity from its exact resource version (the running capacity
    envelope accepts the reuse symmetrically), and a cancelled job is
    refused a new trade with ``ValueError`` -- replaying the original
    trade's idempotency key with an equivalent request still returns
    the stored trade with ``False``, since a cancellation never
    rewrites history. Omitting the argument keeps every result, file
    format and exception exactly as before.
    """
    for value in (jobs, supply, ledger, job_id, key):
        if not isinstance(value, str) or not value:
            raise ValueError("jobs, supply, ledger, job_id and key must be "
                             "non-empty strings")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")
    if completions is not None and (
            not isinstance(completions, str) or not completions):
        raise ValueError("completions must be a non-empty string or None")
    if cancellations is not None and (
            not isinstance(cancellations, str) or not cancellations):
        raise ValueError("cancellations must be a non-empty string or None")

    job_real = os.path.realpath(jobs)
    supply_real = os.path.realpath(supply)
    ledger_real = os.path.realpath(ledger)
    business_paths = {job_real, supply_real, ledger_real}
    completion_real = (os.path.realpath(completions)
                       if completions is not None else None)
    if completion_real is not None and completion_real in business_paths:
        raise ValueError("completions must be a distinct real path")
    cancellation_real = (os.path.realpath(cancellations)
                         if cancellations is not None else None)
    if cancellation_real is not None and (
            cancellation_real in business_paths
            or cancellation_real == completion_real):
        raise ValueError("cancellations must be a distinct real path")
    # Completion ledgers beside the snapshots are part of the physical
    # ledger state and must be locked in the same global order; they are
    # discovered before any lock is taken, so a concurrent completion
    # can never deadlock against this clearing.
    completion_paths = _discover_completion_paths(
        (job_real, supply_real, ledger_real))
    if completion_real is not None and completion_real \
            not in completion_paths:
        completion_paths.append(completion_real)
    locked_paths = business_paths | set(completion_paths)
    if cancellation_real is not None:
        # The explicitly named cancellation ledger is part of the
        # snapshot and is shared in the same global lock order.
        locked_paths = locked_paths | {cancellation_real}
    if len({job_real, supply_real, ledger_real}) != 3:
        raise ValueError("jobs, supply and ledger paths must be distinct "
                         "real paths")

    store = _get_store(ledger)
    with store.lock:
        # Locks are taken in one resolved-real-path order shared by
        # every caller, so concurrent clears can never deadlock; the
        # ledger lock is exclusive, every other lock shared.
        with contextlib.ExitStack() as stack:
            for locked in sorted(locked_paths):
                stack.enter_context(
                    _clear_lock(locked, shared=(locked != ledger_real)))

            accepted, job_map, job_events, job_raw = \
                _jobs._load_submit_file(job_real)
            if job_raw is None:
                raise FileNotFoundError(
                    f"acceptance file {job_real!r} does not exist")
            # Like the supply file and the ledger, the acceptance
            # snapshot must be in the canonical compact form jobs.submit
            # writes; this does not change resources.feasible, which keeps
            # its own documented read behavior.
            if job_raw != _jobs._serialize_submit_file(
                    accepted, job_map, job_events):
                raise ValueError(
                    f"acceptance file {job_real!r} is not in canonical "
                    "compact form")
            history, _supply_map, _supply_events, supply_raw = \
                _resources._load_file(supply_real)
            if supply_raw is None:
                raise FileNotFoundError(
                    f"supply file {supply_real!r} does not exist")
            # The union is read under the shared completion locks as one
            # snapshot. Releases affect the decision only when a
            # completion ledger was explicitly provided: omitting the
            # argument keeps the historical result (every trade still
            # occupies its booked version), while the envelope check
            # below still accepts capacity legitimately reused through a
            # completion ledger that lives beside the trades ledger.
            completion_union = _load_completion_union(
                completion_paths, accepted, history) \
                if completion_paths else {}
            released_jobs: set[str] = set()
            if completion_real is not None:
                released_jobs = {
                    job for job, record in completion_union.items()
                    if record["at"] <= at}
            # The explicitly named cancellation ledger is part of the
            # same snapshot: its records release their trade's occupancy
            # from the cancellation moment on and bar the job from any
            # new trade. Omitting the argument keeps the historical
            # result (every trade still occupies its booked version).
            cancelled_by_job: dict[str, dict[str, Any]] = {}
            if cancellation_real is not None:
                from . import cancellation as _cancellation
                cancellation_records, _cancel_keys, _cancel_events, \
                    cancellation_raw = \
                    _cancellation._load_cancellation_ledger(
                        cancellation_real, accepted, history)
                if cancellation_raw is None:
                    raise FileNotFoundError(
                        f"cancellation ledger {cancellation_real!r} does "
                        "not exist")
                cancelled_by_job = {
                    record["job_id"]: record
                    for record in cancellation_records.values()}
                released_jobs |= {
                    job for job, record in cancelled_by_job.items()
                    if record["at"] <= at}
            trades, idempotency, old_bytes = _load_clear_ledger(
                ledger_real, accepted, history,
                completion_paths=completion_paths,
                cancelled=cancelled_by_job or None)
            if cancellation_real is not None:
                _cancellation._check_records_against_trades(
                    cancellation_records, trades)

            job = accepted.get(job_id)
            if job is None:
                raise KeyError(job_id)

            binding = idempotency.get(key)
            if binding is not None:
                if binding["job_id"] != job_id or binding["at"] != at:
                    raise ValueError("idempotency key was already used with "
                                     "a different request")
                return copy.deepcopy(trades[job_id]), False
            if job_id in trades:
                raise ValueError("job is already traded under another "
                                 "idempotency key")
            if job_id in cancelled_by_job:
                raise ValueError("job is cancelled and cannot be traded "
                                 "again")

            # Capacity already sold per exact resource version; replays
            # never re-enter this path, so every recorded trade counts
            # exactly once and bookings never cross versions. A job
            # whose completion or cancellation is already effective at
            # the evaluation moment has released its booked occupancy,
            # so its trade no longer deducts capacity.
            sold: dict[tuple[str, int], int] = {}
            for traded_job, trade in trades.items():
                if traded_job in released_jobs:
                    continue
                slot = (trade["resource_id"], trade["version"])
                sold[slot] = sold.get(slot, 0) + trade["work"]

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
                remaining = active["capacity"] - sold.get(
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

            candidates.sort(key=lambda entry: (
                entry["resource"]["carbon_intensity"],
                entry["resource"]["unit_cost"],
                entry["resource"]["resource_id"]))
            if not candidates:
                raise LookupError("no remaining feasible resource for job")

            winner = candidates[0]["resource"]
            resource_id = winner["resource_id"]
            version = winner["version"]
            trade: dict[str, Any] = {
                "job_id": job_id,
                "at": at,
                "work": work,
                "resource_id": resource_id,
                "version": version,
                "candidates": candidates,
                "selection": {"resource_id": resource_id,
                              "version": version},
            }
            trades[job_id] = trade
            idempotency[key] = {"job_id": job_id, "at": at}
            _commit_clear(ledger_real,
                          _canonical_clear_bytes(trades, idempotency),
                          old_bytes)
            return copy.deepcopy(trade), True


def clear_live(
    jobs: str,
    supply: str,
    signals: str,
    ledger: str,
    job_id: str,
    key: str,
    at: int,
    completions: str | None = None,
    cancellations: str | None = None,
) -> tuple[dict[str, object], bool]:
    """Clear one accepted job against live signals idempotently.

    This is the dynamic counterpart of :func:`clear` and shares the same
    clearing ledger: trades booked by either call deduct capacity from
    the same resource versions. ``jobs``, ``supply``, ``signals`` and
    ``ledger`` paths, ``job_id`` and ``key`` must be non-empty strings
    and ``at`` a non-boolean non-negative integer evaluation moment;
    the four paths must also resolve to distinct real locations. Any
    violation raises ``ValueError`` before a business file is read.

    The acceptance file, the supply file and the live signal file are
    read as one snapshot under their shared locks together with the
    ledger's exclusive lock, all four taken in resolved real-path
    order. Only each resource's highest supply version valid at ``at``
    is considered, and region, residency, budget, deadline and capacity
    already booked against the exact resource version -- by a static
    :func:`clear` trade or a live one -- follow :func:`clear`. The unit
    cost and carbon intensity, however, come from the resource region's
    latest signal observed no later than ``at`` and not expired at it;
    a region without such a signal contributes no candidate. Remaining
    candidates are ordered by signal carbon intensity, signal unit cost
    and resource id; the first one is the unique winner.

    Returns ``(trade, created)``. The trade freezes the ordered
    candidates with both their immutable resource version and their
    immutable signal version (the selection keeps resource id and
    version), so a signal published afterwards can never change an
    earlier trade. A missing ledger is created only on the first trade;
    the trade, the idempotency binding (key to job id and evaluation
    moment) and the audit event are committed together in one synced
    atomic write, byte-compatible with :func:`clear`. Replaying the same
    key with the same job and evaluation moment returns the stored
    trade -- static or live -- with ``False`` without reselecting,
    appending an event or rewriting a byte. The same key with a changed
    request, or a job already traded under another idempotency key,
    raises ``ValueError`` and leaves the ledger byte-for-byte untouched.

    An unknown job raises ``KeyError`` and no remaining feasible
    resource raises ``LookupError``; neither creates the ledger. Missing
    input files or a missing ledger parent raise ``FileNotFoundError``
    without leaving a ledger or temporary fragment. Invalid structure,
    references, ordering or non-canonical bytes raise ``ValueError``;
    other locking, read/write or sync failures raise ``OSError``. The
    existing :func:`match`, :func:`clear`, the ``resources`` interfaces,
    serving and HTTP behavior are unchanged.

    When ``cancellations`` names a cancellation ledger it is read as
    part of the same snapshot under its shared lock, exactly as in
    :func:`clear`: a trade whose job was cancelled at a moment not
    later than ``at`` no longer deducts capacity from its exact
    resource version, and a cancelled job is refused a new trade with
    ``ValueError`` -- replaying the original trade's idempotency key
    with an equivalent request still returns the stored trade with
    ``False``. Omitting the argument keeps every result, file format
    and exception exactly as before.
    """
    for value in (jobs, supply, signals, ledger, job_id, key):
        if not isinstance(value, str) or not value:
            raise ValueError("jobs, supply, signals, ledger, job_id and key "
                             "must be non-empty strings")
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")
    if completions is not None and (
            not isinstance(completions, str) or not completions):
        raise ValueError("completions must be a non-empty string or None")
    if cancellations is not None and (
            not isinstance(cancellations, str) or not cancellations):
        raise ValueError("cancellations must be a non-empty string or None")

    job_real = os.path.realpath(jobs)
    supply_real = os.path.realpath(supply)
    signal_real = os.path.realpath(signals)
    ledger_real = os.path.realpath(ledger)
    business_paths = {job_real, supply_real, signal_real, ledger_real}
    completion_real = (os.path.realpath(completions)
                       if completions is not None else None)
    if completion_real is not None and completion_real in business_paths:
        raise ValueError("completions must be a distinct real path")
    cancellation_real = (os.path.realpath(cancellations)
                         if cancellations is not None else None)
    if cancellation_real is not None and (
            cancellation_real in business_paths
            or cancellation_real == completion_real):
        raise ValueError("cancellations must be a distinct real path")
    completion_paths = _discover_completion_paths(
        (job_real, supply_real, signal_real, ledger_real))
    if completion_real is not None and completion_real \
            not in completion_paths:
        completion_paths.append(completion_real)
    locked_paths = business_paths | set(completion_paths)
    if cancellation_real is not None:
        # The explicitly named cancellation ledger is part of the
        # snapshot and is shared in the same global lock order.
        locked_paths = locked_paths | {cancellation_real}
    if len(business_paths) != 4:
        raise ValueError("jobs, supply, signals and ledger paths must be "
                         "distinct real paths")

    store = _get_store(ledger)
    with store.lock:
        # Locks are taken in one resolved-real-path order shared by
        # every caller, so concurrent clears can never deadlock; the
        # ledger lock is exclusive, every other lock shared.
        with contextlib.ExitStack() as stack:
            for locked in sorted(locked_paths):
                stack.enter_context(
                    _clear_lock(locked, shared=(locked != ledger_real)))

            accepted, job_map, job_events, job_raw = \
                _jobs._load_submit_file(job_real)
            if job_raw is None:
                raise FileNotFoundError(
                    f"acceptance file {job_real!r} does not exist")
            if job_raw != _jobs._serialize_submit_file(
                    accepted, job_map, job_events):
                raise ValueError(
                    f"acceptance file {job_real!r} is not in canonical "
                    "compact form")
            history, _supply_map, _supply_events, supply_raw = \
                _resources._load_file(supply_real)
            if supply_raw is None:
                raise FileNotFoundError(
                    f"supply file {supply_real!r} does not exist")
            signal_history, _signal_map, _signal_events, signal_raw = \
                _signals._load_file(signal_real)
            if signal_raw is None:
                raise FileNotFoundError(
                    f"signal file {signal_real!r} does not exist")
            completion_union = _load_completion_union(
                completion_paths, accepted, history) \
                if completion_paths else {}
            released_jobs: set[str] = set()
            if completion_real is not None:
                released_jobs = {
                    job for job, record in completion_union.items()
                    if record["at"] <= at}
            # The explicitly named cancellation ledger is part of the
            # same snapshot: its records release their trade's occupancy
            # from the cancellation moment on and bar the job from any
            # new trade. Omitting the argument keeps the historical
            # result (every trade still occupies its booked version).
            cancelled_by_job: dict[str, dict[str, Any]] = {}
            if cancellation_real is not None:
                from . import cancellation as _cancellation
                cancellation_records, _cancel_keys, _cancel_events, \
                    cancellation_raw = \
                    _cancellation._load_cancellation_ledger(
                        cancellation_real, accepted, history)
                if cancellation_raw is None:
                    raise FileNotFoundError(
                        f"cancellation ledger {cancellation_real!r} does "
                        "not exist")
                cancelled_by_job = {
                    record["job_id"]: record
                    for record in cancellation_records.values()}
                released_jobs |= {
                    job for job, record in cancelled_by_job.items()
                    if record["at"] <= at}
            # The ledger now accepts live trades, each freezing the
            # signal version it priced on; static trades validate as
            # before.
            trades, idempotency, old_bytes = _load_clear_ledger(
                ledger_real, accepted, history, signal_history,
                completion_paths=completion_paths,
                cancelled=cancelled_by_job or None)
            if cancellation_real is not None:
                _cancellation._check_records_against_trades(
                    cancellation_records, trades)

            job = accepted.get(job_id)
            if job is None:
                raise KeyError(job_id)

            binding = idempotency.get(key)
            if binding is not None:
                if binding["job_id"] != job_id or binding["at"] != at:
                    raise ValueError("idempotency key was already used with "
                                     "a different request")
                return copy.deepcopy(trades[job_id]), False
            if job_id in trades:
                raise ValueError("job is already traded under another "
                                 "idempotency key")
            if job_id in cancelled_by_job:
                raise ValueError("job is cancelled and cannot be traded "
                                 "again")

            # Capacity already sold per exact resource version counts
            # every trade -- static and live alike -- so either clearing
            # path can oversell, and bookings never cross versions. A
            # job whose completion or cancellation is already effective
            # at the moment has released its booked occupancy, so its
            # trade no longer deducts capacity.
            sold: dict[tuple[str, int], int] = {}
            for traded_job, trade in trades.items():
                if traded_job in released_jobs:
                    continue
                slot = (trade["resource_id"], trade["version"])
                sold[slot] = sold.get(slot, 0) + trade["work"]

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
                remaining = active["capacity"] - sold.get(
                    (active["resource_id"], active["version"]), 0)
                if remaining < work:
                    continue
                if not residency <= set(active["residency"]):
                    continue
                if active["end"] < job["deadline"]:
                    continue
                # Latest unexpired observation for the region; without
                # one the resource is simply not a live candidate.
                signal = None
                for signal_record in signal_history.get(active["region"], ()):
                    if signal_record["observed"] <= at \
                            <= signal_record["expires"]:
                        signal = signal_record
                if signal is None:
                    continue
                total_cost = work * signal["unit_cost"]
                total_carbon = work * signal["carbon_intensity"]
                if total_cost > job["max_cost"] \
                        or total_carbon > job["carbon_cap"]:
                    continue
                signal_copy = dict(signal)
                signal_copy["mix"] = dict(signal["mix"])
                candidates.append({
                    "resource": dict(active),
                    "signal": signal_copy,
                    "total_cost": total_cost,
                    "total_carbon": total_carbon,
                })

            candidates.sort(key=lambda entry: (
                entry["signal"]["carbon_intensity"],
                entry["signal"]["unit_cost"],
                entry["resource"]["resource_id"]))
            if not candidates:
                raise LookupError("no remaining feasible resource for job")

            winner = candidates[0]["resource"]
            resource_id = winner["resource_id"]
            version = winner["version"]
            trade: dict[str, Any] = {
                "job_id": job_id,
                "at": at,
                "work": work,
                "resource_id": resource_id,
                "version": version,
                "candidates": candidates,
                "selection": {"resource_id": resource_id,
                              "version": version},
            }
            trades[job_id] = trade
            idempotency[key] = {"job_id": job_id, "at": at}
            _commit_clear(ledger_real,
                          _canonical_clear_bytes(trades, idempotency),
                          old_bytes)
            return copy.deepcopy(trade), True


# ---------------------------------------------------------------------------
# Batch live clearing: one consistent snapshot, one global selection
# ---------------------------------------------------------------------------

_BATCH_VERSION = 1
_BATCH_SIDECAR_SUFFIX = ".batches"
_BATCH_ROOT_FIELDS = ("version", "batches")
_BATCH_RECORD_FIELDS = ("key", "at", "jobs", "allocated", "unallocated",
                        "snapshot")
_BATCH_SELECTION_FIELDS = ("resource_id", "version")


def _batch_trade_key(key: str, job_id: str) -> str:
    # Every clearing-ledger trade is bound to exactly one idempotency
    # key, so a batch derives one sub-key per allocated job from its
    # batch key. The JSON pair encoding is injective: distinct
    # (batch key, job id) pairs never produce the same sub-key.
    return json.dumps([key, job_id], ensure_ascii=False,
                      separators=(",", ":"))


def _check_batch_ids(
    value: object,
    label: str,
    *,
    allow_empty: bool,
    normalized: bool,
) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list")
    if not value and not allow_empty:
        raise ValueError(f"{label} must be a non-empty list")
    seen: set[str] = set()
    result: list[str] = []
    for job_id in value:
        if not isinstance(job_id, str) or not job_id:
            raise ValueError(f"{label} must be non-empty strings")
        if job_id in seen:
            raise ValueError(f"{label} must be distinct")
        seen.add(job_id)
        result.append(job_id)
    if normalized and result != sorted(result):
        raise ValueError(f"{label} must be ordered by code point")
    return result


def _validate_batch_ledger(data: object) -> dict[str, dict[str, Any]]:
    if not isinstance(data, dict) \
            or set(data.keys()) != set(_BATCH_ROOT_FIELDS):
        raise ValueError("batch ledger root must be an object with keys "
                         "version and batches")
    version = data["version"]
    if not _is_plain_int(version) or version != _BATCH_VERSION:
        raise ValueError("unsupported batch ledger version")
    batches_raw = data["batches"]
    if not isinstance(batches_raw, dict):
        raise ValueError("batches must be an object")
    _check_sorted_keys(batches_raw, "batches")

    batches: dict[str, dict[str, Any]] = {}
    for key, record in batches_raw.items():
        if not isinstance(key, str) or not key:
            raise ValueError("batch keys must be non-empty strings")
        if not isinstance(record, dict) \
                or set(record.keys()) != set(_BATCH_RECORD_FIELDS):
            raise ValueError("batch record has invalid fields")
        if record["key"] != key:
            raise ValueError("batch record key does not match its map key")
        at = record["at"]
        if not _is_plain_int(at) or at < 0:
            raise ValueError("batch record at must be a non-boolean "
                             "non-negative integer")
        jobs = _check_batch_ids(record["jobs"], "batch record jobs",
                                allow_empty=False, normalized=True)
        job_set = set(jobs)
        allocated_raw = record["allocated"]
        if not isinstance(allocated_raw, dict):
            raise ValueError("batch record allocated must be an object")
        _check_sorted_keys(allocated_raw, "batch record allocated")
        allocated: dict[str, dict[str, Any]] = {}
        for job_id, selection in allocated_raw.items():
            if job_id not in job_set:
                raise ValueError("batch record allocation must reference a "
                                 "requested job")
            if not isinstance(selection, dict) \
                    or set(selection.keys()) != set(_BATCH_SELECTION_FIELDS):
                raise ValueError("batch record allocation has invalid "
                                 "fields")
            resource_id = selection["resource_id"]
            selection_version = selection["version"]
            if not isinstance(resource_id, str) or not resource_id:
                raise ValueError("batch record allocation resource_id must "
                                 "be a non-empty string")
            if not _is_plain_int(selection_version) \
                    or selection_version < 1:
                raise ValueError("batch record allocation version must be "
                                 "a positive integer")
            allocated[job_id] = {"resource_id": resource_id,
                                 "version": selection_version}
        unallocated = _check_batch_ids(record["unallocated"],
                                       "batch record unallocated",
                                       allow_empty=True, normalized=True)
        if set(allocated) & set(unallocated):
            raise ValueError("batch record must not allocate and skip one "
                             "job")
        if set(allocated) | set(unallocated) != job_set:
            raise ValueError("batch record must dispose of every requested "
                             "job")
        snapshot = _check_batch_ids(record["snapshot"],
                                    "batch record snapshot",
                                    allow_empty=True, normalized=True)
        if not set(allocated) <= set(snapshot):
            raise ValueError("batch record allocations must appear in the "
                             "trades snapshot")
        batches[key] = {
            "key": key,
            "at": at,
            "jobs": jobs,
            "allocated": allocated,
            "unallocated": unallocated,
            "snapshot": snapshot,
        }
    return batches


def _canonical_batch_bytes(batches: dict[str, dict[str, Any]]) -> bytes:
    payload = {
        "version": _BATCH_VERSION,
        "batches": {key: {
            "key": record["key"],
            "at": record["at"],
            "jobs": list(record["jobs"]),
            "allocated": {job_id: {
                "resource_id": selection["resource_id"],
                "version": selection["version"],
            } for job_id, selection in sorted(record["allocated"].items())},
            "unallocated": list(record["unallocated"]),
            "snapshot": list(record["snapshot"]),
        } for key, record in sorted(batches.items())},
    }
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _load_batch_ledger(
    realpath: str,
) -> tuple[dict[str, dict[str, Any]], bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return {}, None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"batch ledger {realpath!r} is not valid UTF-8") from exc
    try:
        # Negative-zero and non-finite literals are format errors.
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"batch ledger {realpath!r} is not valid JSON") from exc
    batches = _validate_batch_ledger(data)
    # As for the clearing ledger, the batch ledger is accepted only in
    # canonical compact form with a single trailing newline.
    if raw != _canonical_batch_bytes(batches):
        raise ValueError(
            f"batch ledger {realpath!r} is not in canonical compact form")
    return batches, raw


def _check_batch_ledger_against_trades(
    batches: dict[str, dict[str, Any]],
    trades: dict[str, dict[str, Any]],
) -> None:
    # The batch ledger is only ever a view over the append-only clearing
    # ledger: every snapshot trade must still be recorded, and every
    # recorded allocation must still name its trade's exact selection.
    for record in batches.values():
        for job_id in record["snapshot"]:
            if job_id not in trades:
                raise ValueError("batch ledger snapshot must reference "
                                 "recorded trades")
        for job_id, selection in record["allocated"].items():
            trade = trades[job_id]
            if trade["resource_id"] != selection["resource_id"] \
                    or trade["version"] != selection["version"]:
                raise ValueError("batch record does not match its trade "
                                 "selection")


def _batch_result(
    key: str,
    record: dict[str, Any],
    trades: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    # The result freezes the complete trades snapshot the batch was
    # committed with; trades later requests add never alter what a
    # replay returns.
    snapshot = {job_id: copy.deepcopy(trades[job_id])
                for job_id in record["snapshot"]}
    return {
        "key": key,
        "at": record["at"],
        "trades": snapshot,
        "unallocated": list(record["unallocated"]),
    }


def _live_batch_candidates(
    job: dict[str, Any],
    history: dict[str, list[dict[str, Any]]],
    signal_history: dict[str, list[dict[str, Any]]],
    sold: dict[tuple[str, int], int],
    at: int,
) -> list[dict[str, Any]]:
    # The live feasibility rules of clear_live for one job: the highest
    # supply version valid at the moment, region, residency, deadline
    # coverage, the remaining capacity of the exact resource version, a
    # live signal for the region and the job's budgets. Candidates are
    # ordered by signal carbon intensity, signal unit cost and resource
    # id.
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
        remaining = active["capacity"] - sold.get(
            (active["resource_id"], active["version"]), 0)
        if remaining < work:
            continue
        if not residency <= set(active["residency"]):
            continue
        if active["end"] < job["deadline"]:
            continue
        # Latest unexpired observation for the region; without one the
        # resource is simply not a live candidate.
        signal = None
        for signal_record in signal_history.get(active["region"], ()):
            if signal_record["observed"] <= at \
                    <= signal_record["expires"]:
                signal = signal_record
        if signal is None:
            continue
        total_cost = work * signal["unit_cost"]
        total_carbon = work * signal["carbon_intensity"]
        if total_cost > job["max_cost"] \
                or total_carbon > job["carbon_cap"]:
            continue
        signal_copy = dict(signal)
        signal_copy["mix"] = dict(signal["mix"])
        candidates.append({
            "resource": dict(active),
            "signal": signal_copy,
            "total_cost": total_cost,
            "total_carbon": total_carbon,
        })
    candidates.sort(key=lambda entry: (
        entry["signal"]["carbon_intensity"],
        entry["signal"]["unit_cost"],
        entry["resource"]["resource_id"]))
    return candidates


def _select_batch(
    works: list[int],
    options: list[list[dict[str, Any]]],
    remaining: dict[tuple[str, int], int],
) -> list[int]:
    """The unique optimum of the batch objective, as candidate indexes.

    ``works`` and ``options`` are indexed by the jobs sorted by job id;
    ``remaining`` maps each candidate's exact resource version to the
    capacity the committed trades leave on it. The returned list gives
    the chosen candidate index per job, -1 for an unallocated job. The
    objective, compared lexicographically: maximize the total allocated
    work, then the number of allocated jobs, then minimize the total
    added carbon, then the total added cost, and finally take the
    lexicographically smallest sequence of (resource id, version) over
    the jobs in job-id order, an unallocated job sorting after every
    allocation. The final criterion makes the optimum unique, so the
    input order of the job ids can never influence the result.
    """
    n = len(works)
    # Suffix sums over the jobs that still have a candidate: the
    # optimistic bound on the work any completion can still add.
    suffix_work = [0] * (n + 1)
    for i in range(n - 1, -1, -1):
        suffix_work[i] = suffix_work[i + 1] + (works[i] if options[i] else 0)
    free_total = sum(remaining.values())
    used: dict[tuple[str, int], int] = {}
    chosen = [-1] * n
    best_key: tuple[Any, ...] | None = None
    best_chosen: list[int] | None = None

    def dfs(i: int, work_sum: int, count: int, carbon: int, cost: int,
            free: int) -> None:
        nonlocal best_key, best_chosen
        if best_key is not None:
            bound = (-(work_sum + min(suffix_work[i], free)),
                     -(count + min(n - i, free)))
            if bound > (best_key[0], best_key[1]):
                return
            if bound == (best_key[0], best_key[1]) \
                    and (carbon, cost) > (best_key[2], best_key[3]):
                return
        if i == n:
            sequence = tuple(
                (0, options[j][chosen[j]]["resource"]["resource_id"],
                 options[j][chosen[j]]["resource"]["version"])
                if chosen[j] >= 0 else (1,)
                for j in range(n))
            key = (-work_sum, -count, carbon, cost, sequence)
            if best_key is None or key < best_key:
                best_key = key
                best_chosen = list(chosen)
            return
        for index, entry in enumerate(options[i]):
            resource = entry["resource"]
            slot = (resource["resource_id"], resource["version"])
            if used.get(slot, 0) + works[i] > remaining[slot]:
                continue
            used[slot] = used.get(slot, 0) + works[i]
            chosen[i] = index
            dfs(i + 1, work_sum + works[i], count + 1,
                carbon + entry["total_carbon"],
                cost + entry["total_cost"], free - works[i])
            chosen[i] = -1
            used[slot] -= works[i]
        dfs(i + 1, work_sum, count, carbon, cost, free)

    dfs(0, 0, 0, 0, 0, free_total)
    # The all-unallocated assignment is always feasible, so the search
    # always settles on an optimum.
    assert best_chosen is not None
    return best_chosen


def clear_live_batch(
    jobs: str,
    supply: str,
    signals: str,
    ledger: str,
    job_ids: list[str],
    key: str,
    at: int,
    completions: str | None = None,
    cancellations: str | None = None,
) -> tuple[dict[str, object], bool]:
    """Clear a batch of accepted jobs against live signals atomically.

    This is the batch counterpart of :func:`clear_live` and shares the
    same clearing ledger: trades booked by either single-job call or by
    a batch deduct capacity from the same resource versions. ``jobs``,
    ``supply``, ``signals`` and ``ledger`` paths and the batch ``key``
    must be non-empty strings, ``job_ids`` a non-empty list of distinct
    non-empty job id strings and ``at`` a non-boolean non-negative
    integer evaluation moment; the four paths must also resolve to
    distinct real locations, and the optional completion and
    cancellation ledgers to distinct ones again. An empty list, a
    duplicate or invalid job id, an invalid moment, an empty key or
    colliding resolved paths raise ``ValueError`` before a business
    file is read.

    The acceptance file, the supply file, the live signal file, the
    clearing ledger and the batch request ledger are read as one
    snapshot under the same locks and in the same resolved real-path
    order as :func:`clear_live` -- the ledger lock exclusive, every
    other lock shared -- so a concurrent single-job or batch clearing
    only ever observes the complete state before or after a commit and
    the two can never oversell, double-book or deadlock. Every
    requested job must be accepted, not yet traded under any other
    request and not terminated under the existing lifecycle rules (a
    recorded completion or, when ``cancellations`` is given, a recorded
    cancellation): an unknown job raises ``KeyError``, a traded or
    terminated one ``ValueError``, and in every case the whole batch
    fails without leaving a partial result.

    Feasibility follows :func:`clear_live` exactly: each resource's
    highest supply version valid at ``at``, region and data residency,
    deadline coverage, the job's cost and carbon budgets priced on the
    region's latest unexpired signal, and the remaining capacity of the
    exact resource version after the committed trades (and the releases
    of jobs completed or cancelled no later than ``at``, when the
    corresponding ledger is given). The batch then chooses one global
    assignment: each job takes at most one resource version, the new
    work booked on a version never exceeds its remaining capacity, and
    the result maximizes first the total allocated work, then the
    number of allocated jobs, then minimizes the total added carbon and
    the total added cost; remaining ties are broken by the
    lexicographically smallest sequence of resource id and version over
    the jobs sorted by job id, so the input order of ``job_ids`` never
    influences the outcome. A job that loses the capacity race, lacks
    a live signal or has no feasible resource is simply unallocated --
    never an error.

    Returns ``(result, created)``. The result carries the batch ``key``,
    the evaluation moment ``at``, the complete trades snapshot ordered
    by job id -- every trade recorded in the clearing ledger as of the
    commit, including pre-existing ones -- and the sorted list of
    unallocated job ids. Each new trade is an ordinary live trade
    freezing its selected resource version and signal version, bound to
    a derived idempotency sub-key of the batch key, so ``clear``,
    ``clear_live`` and every downstream reader observe them exactly
    like single-job trades. The first success commits all new trades
    with their idempotency bindings and audit events to the clearing
    ledger and the batch request binding -- the full request, the
    allocations and the snapshot -- to the batch ledger beside it, both
    durable together or not at all; a batch that allocates nothing
    still persists its replayable binding, and a commit failure
    restores the pre-call content of both files. Replaying the same
    batch key with an equivalent complete request -- the same job id
    set in any order and the same moment -- returns the original result
    with ``False`` without reselecting or rewriting a byte; the same
    key with a changed request raises ``ValueError``.

    Missing input files or a missing ledger parent raise
    ``FileNotFoundError`` without leaving a ledger or temporary
    fragment; invalid structure, references, ordering or non-canonical
    bytes in any ledger raise ``ValueError``; other locking, read/write
    or sync failures raise ``OSError``. The existing :func:`match`,
    :func:`clear`, :func:`clear_live`, dispatch, execution and
    migration readers keep their exact behavior over the shared
    clearing ledger, and omitting ``completions`` or ``cancellations``
    keeps the historical capacity accounting of :func:`clear_live`.
    """
    for value in (jobs, supply, signals, ledger, key):
        if not isinstance(value, str) or not value:
            raise ValueError("jobs, supply, signals, ledger and key must "
                             "be non-empty strings")
    requested_ids = _check_batch_ids(job_ids, "job_ids", allow_empty=False,
                                     normalized=False)
    sorted_ids = sorted(requested_ids)
    if not _is_plain_int(at) or at < 0:
        raise ValueError("at must be a non-boolean non-negative integer")
    if completions is not None and (
            not isinstance(completions, str) or not completions):
        raise ValueError("completions must be a non-empty string or None")
    if cancellations is not None and (
            not isinstance(cancellations, str) or not cancellations):
        raise ValueError("cancellations must be a non-empty string or None")

    job_real = os.path.realpath(jobs)
    supply_real = os.path.realpath(supply)
    signal_real = os.path.realpath(signals)
    ledger_real = os.path.realpath(ledger)
    business_paths = {job_real, supply_real, signal_real, ledger_real}
    if len(business_paths) != 4:
        raise ValueError("jobs, supply, signals and ledger paths must be "
                         "distinct real paths")
    completion_real = (os.path.realpath(completions)
                       if completions is not None else None)
    if completion_real is not None and completion_real in business_paths:
        raise ValueError("completions must be a distinct real path")
    cancellation_real = (os.path.realpath(cancellations)
                         if cancellations is not None else None)
    if cancellation_real is not None and (
            cancellation_real in business_paths
            or cancellation_real == completion_real):
        raise ValueError("cancellations must be a distinct real path")
    # The batch request ledger lives beside the clearing ledger under a
    # derived name; it must never alias a business path either.
    batch_real = ledger_real + _BATCH_SIDECAR_SUFFIX
    if batch_real in business_paths or batch_real == completion_real \
            or batch_real == cancellation_real:
        raise ValueError("batch ledger path must be a distinct real path")
    # Completion ledgers beside the snapshots are discovered before any
    # lock is taken, exactly as in clear_live.
    completion_paths = _discover_completion_paths(
        (job_real, supply_real, signal_real, ledger_real))
    if completion_real is not None and completion_real \
            not in completion_paths:
        completion_paths.append(completion_real)
    locked_paths = business_paths | set(completion_paths)
    if cancellation_real is not None:
        # The explicitly named cancellation ledger is part of the
        # snapshot and is shared in the same global lock order.
        locked_paths = locked_paths | {cancellation_real}

    store = _get_store(ledger)
    with store.lock:
        # Locks are taken in one resolved-real-path order shared by
        # every caller, so concurrent clears can never deadlock; the
        # ledger lock is exclusive, every other lock shared. The batch
        # ledger is only ever touched under these locks, so it needs no
        # lock of its own.
        with contextlib.ExitStack() as stack:
            for locked in sorted(locked_paths):
                stack.enter_context(
                    _clear_lock(locked, shared=(locked != ledger_real)))

            accepted, job_map, job_events, job_raw = \
                _jobs._load_submit_file(job_real)
            if job_raw is None:
                raise FileNotFoundError(
                    f"acceptance file {job_real!r} does not exist")
            if job_raw != _jobs._serialize_submit_file(
                    accepted, job_map, job_events):
                raise ValueError(
                    f"acceptance file {job_real!r} is not in canonical "
                    "compact form")
            history, _supply_map, _supply_events, supply_raw = \
                _resources._load_file(supply_real)
            if supply_raw is None:
                raise FileNotFoundError(
                    f"supply file {supply_real!r} does not exist")
            signal_history, _signal_map, _signal_events, signal_raw = \
                _signals._load_file(signal_real)
            if signal_raw is None:
                raise FileNotFoundError(
                    f"signal file {signal_real!r} does not exist")
            completion_union = _load_completion_union(
                completion_paths, accepted, history) \
                if completion_paths else {}
            released_jobs: set[str] = set()
            if completion_real is not None:
                released_jobs = {
                    job for job, record in completion_union.items()
                    if record["at"] <= at}
            # The explicitly named cancellation ledger is part of the
            # same snapshot: its records release their trade's occupancy
            # from the cancellation moment on and bar the job from any
            # new trade. Omitting the argument keeps the historical
            # result (every trade still occupies its booked version).
            cancelled_by_job: dict[str, dict[str, Any]] = {}
            if cancellation_real is not None:
                from . import cancellation as _cancellation
                cancellation_records, _cancel_keys, _cancel_events, \
                    cancellation_raw = \
                    _cancellation._load_cancellation_ledger(
                        cancellation_real, accepted, history)
                if cancellation_raw is None:
                    raise FileNotFoundError(
                        f"cancellation ledger {cancellation_real!r} does "
                        "not exist")
                cancelled_by_job = {
                    record["job_id"]: record
                    for record in cancellation_records.values()}
                released_jobs |= {
                    job for job, record in cancelled_by_job.items()
                    if record["at"] <= at}
            trades, idempotency, old_bytes = _load_clear_ledger(
                ledger_real, accepted, history, signal_history,
                completion_paths=completion_paths,
                cancelled=cancelled_by_job or None)
            if cancellation_real is not None:
                _cancellation._check_records_against_trades(
                    cancellation_records, trades)
            batches, batch_old_bytes = _load_batch_ledger(batch_real)
            _check_batch_ledger_against_trades(batches, trades)

            for job_id in sorted_ids:
                if job_id not in accepted:
                    raise KeyError(job_id)

            stored = batches.get(key)
            if stored is not None:
                if stored["jobs"] != sorted_ids or stored["at"] != at:
                    raise ValueError("batch idempotency key was already "
                                     "used with a different request")
                return _batch_result(key, stored, trades), False

            for job_id in sorted_ids:
                if job_id in trades:
                    raise ValueError("job is already traded under another "
                                     "request")
                if job_id in cancelled_by_job \
                        or job_id in completion_union:
                    raise ValueError("job is already terminated and cannot "
                                     "be traded")

            # Capacity already sold per exact resource version counts
            # every trade -- static, live and batch alike -- and a job
            # whose completion or cancellation is already effective at
            # the moment has released its booked occupancy, exactly as
            # in clear_live.
            sold: dict[tuple[str, int], int] = {}
            for traded_job, trade in trades.items():
                if traded_job in released_jobs:
                    continue
                slot = (trade["resource_id"], trade["version"])
                sold[slot] = sold.get(slot, 0) + trade["work"]

            works = [accepted[job_id]["work"] for job_id in sorted_ids]
            options = [
                _live_batch_candidates(
                    accepted[job_id], history, signal_history, sold, at)
                for job_id in sorted_ids]
            remaining: dict[tuple[str, int], int] = {}
            for entries in options:
                for entry in entries:
                    resource = entry["resource"]
                    slot = (resource["resource_id"], resource["version"])
                    if slot not in remaining:
                        remaining[slot] = resource["capacity"] \
                            - sold.get(slot, 0)
            chosen = _select_batch(works, options, remaining)

            allocated: dict[str, dict[str, Any]] = {}
            unallocated: list[str] = []
            new_trades: dict[str, dict[str, Any]] = {}
            for i, job_id in enumerate(sorted_ids):
                index = chosen[i]
                if index < 0:
                    unallocated.append(job_id)
                    continue
                entry = options[i][index]
                resource = entry["resource"]
                resource_id = resource["resource_id"]
                version = resource["version"]
                # A batch trade freezes the single selected candidate:
                # the decision is the global assignment, so the snapshot
                # records exactly the resource version and signal version
                # the job was priced on.
                new_trades[job_id] = {
                    "job_id": job_id,
                    "at": at,
                    "work": works[i],
                    "resource_id": resource_id,
                    "version": version,
                    "candidates": [entry],
                    "selection": {"resource_id": resource_id,
                                  "version": version},
                }
                allocated[job_id] = {"resource_id": resource_id,
                                     "version": version}

            derived: dict[str, str] = {}
            for job_id in allocated:
                sub_key = _batch_trade_key(key, job_id)
                if sub_key in idempotency:
                    raise ValueError("batch idempotency key collides with "
                                     "an existing clearing key")
                derived[sub_key] = job_id

            trades.update(new_trades)
            for sub_key, job_id in derived.items():
                idempotency[sub_key] = {"job_id": job_id, "at": at}
            batches[key] = {
                "key": key,
                "at": at,
                "jobs": sorted_ids,
                "allocated": allocated,
                "unallocated": unallocated,
                "snapshot": sorted(trades),
            }

            if new_trades:
                # The trades commit comes first; a failed batch-binding
                # commit restores the pre-call clearing ledger, so the
                # pair is only ever observed entirely before or entirely
                # after the call.
                _commit_clear(ledger_real,
                              _canonical_clear_bytes(trades, idempotency),
                              old_bytes)
                try:
                    _commit_clear(batch_real,
                                  _canonical_batch_bytes(batches),
                                  batch_old_bytes)
                except BaseException as first:
                    _rollback_clear(ledger_real,
                                    os.path.dirname(ledger_real) or ".",
                                    old_bytes, first)
                    raise
            else:
                # Even a batch that allocates nothing persists its
                # replayable binding; the clearing ledger is untouched.
                _commit_clear(batch_real, _canonical_batch_bytes(batches),
                              batch_old_bytes)
            return _batch_result(key, batches[key], trades), True
