"""Persistent consumers of the migration-batch progress stream.

The ``migration_batch`` coordination ledger publishes one append-only
incremental event stream; this module owns the durable consumer side
of that stream. A persistent consumer keeps its own independent
ledger, bound forever to one resolved coordination ledger path: a
claim fixes the subscription -- one optional fixed batch key and one
optional job filter -- the owner, the lease end and the acknowledged
stream position; a pull returns the matching events after that
position without advancing it, and an ack moves the position forward
to a real matching event. A repeat claim by the live owner extends
the lease only while it is still valid (the moment equal to the lease
end counts); once it has strictly expired even the former owner may
not renew in place and gets TimeoutError with the ledger untouched,
while a different owner may take over. Only the current owner while
the lease is valid may pull or ack; after strict expiry another owner
may take over without changing the position, so unacknowledged events
redeliver.

A reject (the fourth consume operation) is an ack that also dead-
letters the event: only the current owner during a valid lease may
reject, and the position must name the earliest unacknowledged event
the fixed subscription still matches -- predecessors may not be
skipped and the stream tail may not be passed. The cursor advances to
that position in the same atomic commit that records the dead letter,
so a rejected event is never pulled again but stays fully observable
through the dead-letter query (:func:`consumer_dead_letters`). A
consumer ledger written before dead letters existed keeps its exact
bytes through claim and ack; its first successful reject upgrades it
with the append-only ``dead_letters`` section.

A read-only status query (:func:`consumer_status`) reports one
consumer's fixed subscription, current owner and lease end, the lease
state at an observation moment (``active`` or ``expired``), the
acknowledged position and the matching backlog: the count and the
oldest still-unacknowledged matching event. It takes no owner and
never writes.

The coordination event snapshots this module reads come from the
``migration_batch`` module's narrow read interface (its ledger parse,
validation and locked-snapshot primitives); the batch orchestration
never duplicates the consumer domain's event filtering, checkpoint
confirmation or exception mapping. ``migration_batch`` re-exports
this module's public surface for compatibility, so existing callers
keep importing it from there.
"""

from __future__ import annotations

import copy
import json
import os
from typing import Any

from ._jsonio import finite_loads
from .migration_batch import (
    _DEFAULT_LIMIT,
    _MAX_LIMIT,
    _PROGRESS_FIELDS,
    CoordinationLedgerMissing,
    _Snapshot,
    _business_locks,
    _check_sorted_keys,
    _commit_file,
    _events_page,
    _get_store,
    _is_plain_int,
    _load_coordination,
    _lock,
    _parse_coordination_bytes,
    _progress_entry,
    _read_raw,
    _render,
    _validate_progress_event,
)

__all__ = ["consume", "consume_response", "consumer_subscription",
           "consumer_status", "consumer_status_response",
           "consumer_dead_letters", "consumer_dead_letters_response",
           "ConsumerLedgerInvalid", "ConsumerOwnershipError",
           "ConsumerLeaseExpired", "CoordinationLedgerInvalid",
           "ConsumersLedgerMissing"]


# ---------------------------------------------------------------------------
# Persistent consumer ledger: claims, pulls, acks, rejects and lease
# renewal
# ---------------------------------------------------------------------------
#
# The consumer ledger is an independent document bound to one resolved
# coordination ledger path; a consume call never rewrites a coordination
# byte. The four operations are claim, pull, ack and reject; a repeat
# claim by the live owner renews the lease only while it is still valid
# (the claim moment equal to the lease end counts).
#
#   {"version": 1,
#    "coordination": <resolved coordination ledger real path>,
#    "subscriptions": {<consumer id>: {"key": ..., "job_id": ...}},
#    "consumers":     {<consumer id>: {"owner", "until", "position"}},
#    "idempotency":   {<idem key>:  <the complete write request>},
#    "audit":         [{"seq", "at", "request", "result"}, ...],
#    "dead_letters":  [{"consumer", "position", "event", "reason",
#                       "rejected_at", "owner"}, ...]}
#
# subscriptions/consumers/idempotency are key-sorted and share the
# consumer-id set; the audit is appended in physical order with a
# contiguous zero-based seq and is never re-sorted. Every first-served
# write commits the state, its idempotency binding and one audit event
# in the same synced atomic replace as every other ledger.
#
# dead_letters is optional on read, like the coordination ledger's
# events section: a ledger written before rejects existed carries no
# such field and keeps its exact bytes through claim, pull and ack; the
# first successful reject upgrades the document and appends the
# section, once, after the audit. It stays append-only: a dead letter
# is an immutable record ordered by append (reject) time, and the
# reject ordering within one consumer is the original position order.

_CONSUMER_VERSION = 1
_CONSUMER_ROOT_FIELDS = ("version", "coordination", "subscriptions",
                        "consumers", "idempotency", "audit")
# Dead letters were added after the claim/pull/ack ledger shipped: the
# section is optional on read and a pre-dead-letter document keeps
# validating and reproducing byte-for-byte until its first successful
# reject upgrades it, exactly like a baseline coordination ledger
# without the events section.
_CONSUMER_DEAD_LETTERS_FIELD = "dead_letters"
_CONSUMER_ROOT_FIELDS_WITH_DEAD_LETTERS = _CONSUMER_ROOT_FIELDS + \
    (_CONSUMER_DEAD_LETTERS_FIELD,)
_CONSUMER_SUBSCRIPTION_FIELDS = ("key", "job_id")
_CONSUMER_STATE_FIELDS = ("owner", "until", "position")
_CONSUMER_AUDIT_FIELDS = ("seq", "at", "request", "result")
_CONSUMER_OPERATIONS = ("claim", "pull", "ack", "reject")
_CONSUMER_REQUEST_FIELDS = {
    "claim": ("operation", "consumer", "key", "job_id", "owner", "lease",
              "now"),
    "ack": ("operation", "consumer", "owner", "position", "now"),
    "reject": ("operation", "consumer", "owner", "position", "reason",
               "now"),
}
_CONSUMER_CLAIM_RESULT_FIELDS = ("consumer", "key", "job_id", "owner",
                                "until", "position", "taken_over")
_CONSUMER_STATE_RESULT_FIELDS = ("consumer", "owner", "until", "position")
# A reject result carries the state fields exactly like an ack and then
# the dead letter it just recorded.
_CONSUMER_REJECT_RESULT_FIELDS = _CONSUMER_STATE_RESULT_FIELDS + \
    ("dead_letter",)
# A dead letter as it rides a reject response and a dead-letter page:
# the event's original position, its complete event snapshot, the
# trimmed reason, the reject moment and the rejecting owner. The owning
# consumer rides the enclosing response/page and the ledger record.
_CONSUMER_DEAD_LETTER_FIELDS = ("position", "event", "reason",
                               "rejected_at", "owner")
# The ledger's append-only section additionally attributes each record
# to its consumer, since one ledger serves every consumer.
_CONSUMER_DEAD_LETTER_RECORD_FIELDS = ("consumer",) + \
    _CONSUMER_DEAD_LETTER_FIELDS
# A reject reason is stripped of surrounding whitespace and then must
# hold between one and this many Unicode code points.
_CONSUMER_REASON_MAX = 512
_CONSUMER_PREFIX = ".migration-consumers-"


class ConsumerLedgerInvalid(ValueError):
    # Malformed or non-canonical consumer-ledger bytes observed through
    # the consume surface. It stays a public ValueError to library
    # callers but lets the endpoint answer 409 (an invalid ledger)
    # instead of 400 (an invalid request).
    pass


class ConsumerOwnershipError(PermissionError):
    # A non-owner tried to act while the lease still holds. It stays a
    # public PermissionError to library callers but lets the endpoint
    # distinguish the 409 ownership conflict from a filesystem
    # PermissionError, which must answer 503.
    pass


class ConsumerLeaseExpired(TimeoutError):
    # The lease has strictly expired for the acting owner (or a
    # same-owner renewal was attempted after strict expiry). It stays a
    # public TimeoutError to library callers while the endpoint maps it
    # to the same 409 ownership code and never mistakes it for an I/O
    # timeout.
    pass


class CoordinationLedgerInvalid(ConsumerLedgerInvalid):
    # The coordination ledger (or a business ledger it references) is
    # malformed or missing, as opposed to the consumer ledger itself.
    # The endpoint keeps the read-only migration-batches error code.
    pass


class ConsumersLedgerMissing(FileNotFoundError):
    # The fixed consumer ledger's parent directory is absent, as
    # opposed to an unknown consumer id (KeyError).
    pass


def _consumer_canonical_request(request: dict[str, Any]) -> dict[str, Any]:
    fields = _CONSUMER_REQUEST_FIELDS[request["operation"]]
    return {field: copy.deepcopy(request[field]) for field in fields}


def _consumer_canonical_dead_letter(
    record: dict[str, Any],
) -> dict[str, Any]:
    # One ledger dead-letter record in fixed field order: the owning
    # consumer then the five public letter fields; the complete event
    # snapshot rides along verbatim in the event stream's shape.
    return {
        "consumer": record["consumer"],
        **_public_dead_letter(record),
    }


def _public_dead_letter(record: dict[str, Any]) -> dict[str, Any]:
    # The five-field dead letter a reject answer and the dead-letter
    # query expose, in fixed order: original position, complete event,
    # trimmed reason, reject moment and rejecting owner.
    return {
        "position": record["position"],
        "event": _progress_entry(record["event"]),
        "reason": record["reason"],
        "rejected_at": record["rejected_at"],
        "owner": record["owner"],
    }


def _consumer_canonical_bytes(
    coordination_real: str,
    subscriptions: dict[str, dict[str, Any]],
    consumers: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, Any]],
    audit: list[dict[str, Any]],
    dead_letters: list[dict[str, Any]] | None = None,
) -> bytes:
    payload = {
        "version": _CONSUMER_VERSION,
        "coordination": coordination_real,
        "subscriptions": {
            consumer: {field: subscriptions[consumer][field]
                       for field in _CONSUMER_SUBSCRIPTION_FIELDS}
            for consumer in sorted(subscriptions)},
        "consumers": {
            consumer: {field: consumers[consumer][field]
                       for field in _CONSUMER_STATE_FIELDS}
            for consumer in sorted(consumers)},
        "idempotency": {
            key: _consumer_canonical_request(idempotency[key])
            for key in sorted(idempotency)},
        "audit": [{"seq": event["seq"], "at": event["at"],
                   "request": _consumer_canonical_request(event["request"]),
                   "result": copy.deepcopy(event["result"])}
                  for event in audit],
    }
    # The dead-letter section is emitted only once a ledger has been
    # upgraded by its first reject; a pre-dead-letter document keeps
    # reproducing its original six-field shape byte-for-byte.
    if dead_letters is not None:
        payload["dead_letters"] = [
            _consumer_canonical_dead_letter(record)
            for record in dead_letters]
    text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False) + "\n"
    return text.encode("utf-8")


def _validate_reason(value: object) -> str:
    # A reject reason is a string; leading and trailing whitespace is
    # stripped and what remains must span 1..512 Unicode code points
    # (counting code points, not UTF-16 units or bytes). The stripped
    # form is what gets persisted and replayed.
    if not isinstance(value, str):
        raise ValueError("reason must be a string")
    reason = value.strip()
    if not 1 <= len(reason) <= _CONSUMER_REASON_MAX:
        raise ValueError("reason must contain between 1 and 512 Unicode "
                         "code points after trimming")
    return reason


def _validate_consumer_request(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("a consumer request must be an object")
    operation = raw.get("operation") if isinstance(raw, dict) else None
    fields = _CONSUMER_REQUEST_FIELDS.get(operation)
    if fields is None or list(raw.keys()) != list(fields):
        raise ValueError("consumer request has invalid fields")
    request = {field: raw[field] for field in fields}
    consumer = request["consumer"]
    if not isinstance(consumer, str) or not consumer:
        raise ValueError("consumer must be a non-empty string")
    owner = request["owner"]
    if not isinstance(owner, str) or not owner:
        raise ValueError("owner must be a non-empty string")
    now = request["now"]
    if not _is_plain_int(now) or now < 0:
        raise ValueError("now must be a non-boolean non-negative integer")
    if operation == "claim":
        for name in ("key", "job_id"):
            value = request[name]
            if value is not None and (not isinstance(value, str)
                                      or not value):
                raise ValueError(
                    f"{name} must be null or a non-empty string")
    if operation == "claim":
        lease = request["lease"]
        if not _is_plain_int(lease) or lease < 1:
            raise ValueError("lease must be a non-boolean positive integer")
    if operation in ("ack", "reject"):
        position = request["position"]
        if not _is_plain_int(position) or position < 0:
            raise ValueError("position must be a non-boolean non-negative "
                             "integer")
    if operation == "reject":
        request["reason"] = _validate_reason(request["reason"])
    return request


def _validate_embedded_event(raw: object) -> dict[str, Any]:
    # A dead letter embeds the complete rejected progress event. The
    # event is form-checked exactly like the coordination stream's own
    # events (fixed fields, order, arity and enumerations, canonical
    # snapshot shape) without following it into any business ledger:
    # the consumer ledger is bound only to the coordination path, and
    # the embedded event is a historical snapshot that never has to
    # reference a batch recorded beside it.
    if not isinstance(raw, dict) \
            or set(raw.keys()) != set(_PROGRESS_FIELDS):
        raise ValueError("dead letter event has invalid fields")
    position = raw["position"]
    if not _is_plain_int(position) or position < 0:
        raise ValueError("dead letter event position must be a "
                         "non-boolean non-negative integer")
    return _validate_progress_event(
        raw, position, {}, {}, require_recorded=False)


def _validate_public_dead_letter(raw: object) -> dict[str, Any]:
    # The five-field public dead letter (the shape a reject answer and
    # the dead-letter query expose), validated intrinsically.
    if not isinstance(raw, dict) \
            or list(raw.keys()) != list(_CONSUMER_DEAD_LETTER_FIELDS):
        raise ValueError("dead letter has invalid fields")
    position = raw["position"]
    if not _is_plain_int(position) or position < 0:
        raise ValueError("dead letter position must be a non-boolean "
                         "non-negative integer")
    event = _validate_embedded_event(raw["event"])
    if event["position"] != position:
        raise ValueError("a dead letter event must sit at its recorded "
                         "position")
    reason = _validate_reason(raw["reason"])
    rejected_at = raw["rejected_at"]
    if not _is_plain_int(rejected_at) or rejected_at < 0:
        raise ValueError("dead letter rejected_at must be a non-boolean "
                         "non-negative integer")
    owner = raw["owner"]
    if not isinstance(owner, str) or not owner:
        raise ValueError("dead letter owner must be a non-empty string")
    return {"position": position, "event": event, "reason": reason,
            "rejected_at": rejected_at, "owner": owner}


def _validate_dead_letter(raw: object) -> dict[str, Any]:
    # One immutable ledger record: the six-field public letter plus the
    # owning consumer, since the section serves every consumer.
    if not isinstance(raw, dict) \
            or list(raw.keys()) != list(
                _CONSUMER_DEAD_LETTER_RECORD_FIELDS):
        raise ValueError("consumer dead letter has invalid fields")
    consumer = raw["consumer"]
    if not isinstance(consumer, str) or not consumer:
        raise ValueError("dead letter consumer must be a non-empty string")
    letter = _validate_public_dead_letter(
        {field: raw[field] for field in _CONSUMER_DEAD_LETTER_FIELDS})
    return {"consumer": consumer, **letter}


def _validate_consumer_result(
    raw: object, request: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("consumer audit result must be an object")
    if request["operation"] == "claim":
        fields = _CONSUMER_CLAIM_RESULT_FIELDS
    elif request["operation"] == "reject":
        fields = _CONSUMER_REJECT_RESULT_FIELDS
    else:
        fields = _CONSUMER_STATE_RESULT_FIELDS
    if list(raw.keys()) != list(fields):
        raise ValueError("consumer audit result has invalid fields")
    if raw["consumer"] != request["consumer"]:
        raise ValueError("consumer audit result must name its request's "
                         "consumer")
    owner = raw["owner"]
    if not isinstance(owner, str) or not owner \
            or owner != request["owner"]:
        raise ValueError("consumer audit result owner must match the "
                         "request owner")
    until = raw["until"]
    if not _is_plain_int(until) or until < 1:
        raise ValueError("consumer audit result until must be a positive "
                         "integer")
    position = raw["position"]
    if position is not None and (not _is_plain_int(position)
                                 or position < 0):
        raise ValueError("consumer audit result position must be null or "
                         "a non-boolean non-negative integer")
    if request["operation"] == "reject":
        # The dead letter riding the result is the public five-field
        # shape, pinned to this request: same rejecting owner and
        # original position, with the request's trimmed reason and
        # moment. Its full event snapshot is a historical fact only
        # form-checked here; the ledger adds the consumer attribution
        # when persisting it in the dead_letters section.
        letter = _validate_public_dead_letter(raw["dead_letter"])
        if letter["owner"] != request["owner"] \
                or letter["position"] != request["position"] \
                or letter["reason"] != request["reason"] \
                or letter["rejected_at"] != request["now"]:
            raise ValueError("a reject audit result must record its "
                             "request's dead letter")
        if position != request["position"]:
            raise ValueError("a reject audit result position must name "
                             "the rejected event")
        return {"consumer": raw["consumer"], "owner": owner, "until": until,
                "position": position, "dead_letter": letter}
    if request["operation"] == "claim":
        key = raw["key"]
        job_id = raw["job_id"]
        for name, value in (("key", key), ("job_id", job_id)):
            if value is not None and (not isinstance(value, str)
                                      or not value):
                raise ValueError(
                    f"consumer audit result {name} must be null or a "
                    "non-empty string")
        if key != request["key"] or job_id != request["job_id"]:
            raise ValueError("consumer audit result subscription must "
                             "match its claim")
        if until != request["now"] + request["lease"]:
            raise ValueError("consumer audit result until must equal the "
                             "claim moment plus its lease")
        # A first claim records the null origin; a repeat claim or a
        # takeover carries the position the consumer had already
        # acknowledged. The audit replay pins which is which.
        taken_over = raw["taken_over"]
        if not isinstance(taken_over, bool):
            raise ValueError("consumer audit result taken_over must be a "
                             "boolean")
        # Claim results keep the subscription fields beside the consumer
        # id, before owner/until/position; the position itself is the
        # null origin on a first claim and the carried checkpoint on a
        # repeat claim or takeover, pinned by the audit replay.
        return {"consumer": raw["consumer"], "key": key, "job_id": job_id,
                "owner": owner, "until": until, "position": position,
                "taken_over": taken_over}
    # An ack always lands on a real event position and therefore can
    # never carry null.
    if request["operation"] == "ack" and position is None:
        raise ValueError("an ack audit result must name an event position")
    return {"consumer": raw["consumer"], "owner": owner, "until": until,
            "position": position}


def _validate_consumer_ledger(
    data: object, coordination_real: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]], list[dict[str, Any]],
           list[dict[str, Any]] | None]:
    if not isinstance(data, dict) \
            or list(data.keys()) not in (
                list(_CONSUMER_ROOT_FIELDS),
                list(_CONSUMER_ROOT_FIELDS_WITH_DEAD_LETTERS)):
        raise ValueError("consumer ledger root must be an object with "
                         "version, coordination, subscriptions, consumers, "
                         "idempotency and audit, and once upgraded "
                         "dead_letters")
    document_legacy = _CONSUMER_DEAD_LETTERS_FIELD not in data
    if not _is_plain_int(data["version"]) \
            or data["version"] != _CONSUMER_VERSION:
        raise ValueError("unsupported consumer ledger version")
    bound = data["coordination"]
    if not isinstance(bound, str) or not bound:
        raise ValueError("consumer ledger coordination binding must be a "
                         "non-empty string")
    if bound != coordination_real:
        raise ValueError("consumer ledger is bound to another coordination "
                         "ledger")
    subscriptions_raw = data["subscriptions"]
    consumers_raw = data["consumers"]
    idempotency_raw = data["idempotency"]
    audit_raw = data["audit"]
    dead_letters_raw = ([] if document_legacy
                        else data[_CONSUMER_DEAD_LETTERS_FIELD])
    if not isinstance(subscriptions_raw, dict) \
            or not isinstance(consumers_raw, dict) \
            or not isinstance(idempotency_raw, dict) \
            or not isinstance(audit_raw, list) \
            or not isinstance(dead_letters_raw, list):
        raise ValueError("subscriptions, consumers and idempotency must be "
                         "objects and audit and dead_letters lists")
    _check_sorted_keys(subscriptions_raw, "subscriptions")
    _check_sorted_keys(consumers_raw, "consumers")
    _check_sorted_keys(idempotency_raw, "idempotency")
    if set(subscriptions_raw) != set(consumers_raw):
        raise ValueError("every consumer must carry exactly one "
                         "subscription and one state record")

    subscriptions: dict[str, dict[str, Any]] = {}
    for consumer, raw in subscriptions_raw.items():
        if not isinstance(consumer, str) or not consumer:
            raise ValueError("consumer ids must be non-empty strings")
        if not isinstance(raw, dict) \
                or list(raw.keys()) != list(_CONSUMER_SUBSCRIPTION_FIELDS):
            raise ValueError("consumer subscription has invalid fields")
        key = raw["key"]
        job_id = raw["job_id"]
        for name, value in (("key", key), ("job_id", job_id)):
            if value is not None and (not isinstance(value, str)
                                      or not value):
                raise ValueError(
                    f"subscription {name} must be null or a non-empty "
                    "string")
        subscriptions[consumer] = {"key": key, "job_id": job_id}

    consumers: dict[str, dict[str, Any]] = {}
    for consumer, raw in consumers_raw.items():
        if not isinstance(consumer, str) or not consumer:
            raise ValueError("consumer ids must be non-empty strings")
        if not isinstance(raw, dict) \
                or list(raw.keys()) != list(_CONSUMER_STATE_FIELDS):
            raise ValueError("consumer state has invalid fields")
        owner = raw["owner"]
        if not isinstance(owner, str) or not owner:
            raise ValueError("consumer owner must be a non-empty string")
        until = raw["until"]
        if not _is_plain_int(until) or until < 1:
            raise ValueError("consumer until must be a positive integer")
        position = raw["position"]
        # Null is the pre-stream origin of a consumer that has never
        # acknowledged; after the first ack it is a real event position,
        # which may legitimately be zero.
        if position is not None and (not _is_plain_int(position)
                                     or position < 0):
            raise ValueError("consumer position must be null or a "
                             "non-boolean non-negative integer")
        consumers[consumer] = {"owner": owner, "until": until,
                               "position": position}

    idempotency: dict[str, dict[str, Any]] = {}
    for idem_key, raw in idempotency_raw.items():
        if not isinstance(idem_key, str) or not idem_key:
            raise ValueError("idempotency keys must be non-empty strings")
        idempotency[idem_key] = _validate_consumer_request(raw)

    audit: list[dict[str, Any]] = []
    bound_requests: list[dict[str, Any]] = []
    for seq, raw in enumerate(audit_raw):
        if not isinstance(raw, dict) \
                or list(raw.keys()) != list(_CONSUMER_AUDIT_FIELDS):
            raise ValueError("consumer audit event has invalid fields")
        if not _is_plain_int(raw["seq"]) or raw["seq"] != seq:
            raise ValueError("consumer audit seq must be contiguous and "
                             "zero-based")
        at = raw["at"]
        if not _is_plain_int(at) or at < 0:
            raise ValueError("consumer audit at must be a non-boolean "
                             "non-negative integer")
        request = _validate_consumer_request(raw["request"])
        if at != request["now"]:
            raise ValueError("consumer audit at must equal its request "
                             "moment")
        if request["consumer"] not in consumers:
            raise ValueError("consumer audit event must reference a "
                             "recorded consumer")
        result = _validate_consumer_result(raw["result"], request)
        audit.append({"seq": seq, "at": at, "request": request,
                      "result": result})
        bound_requests.append(request)

    dead_letters: list[dict[str, Any]] = []
    for raw in dead_letters_raw:
        letter = _validate_dead_letter(raw)
        if letter["consumer"] not in consumers:
            raise ValueError("a dead letter must reference a recorded "
                             "consumer")
        dead_letters.append(letter)

    # Replay the append-only audit into scratch state: it has to
    # reconstruct every subscription and state record exactly, which
    # pins the subscription immutability, a takeover's strict-expiry
    # precondition, the lease windows, monotonic ack and reject
    # positions and each event's recorded result. The stream an ack or
    # reject named is a historical fact not re-followed here; the live
    # check happens on consume.
    replay_subs: dict[str, dict[str, Any]] = {}
    replay_state: dict[str, dict[str, Any]] = {}
    replay_rejects: list[dict[str, Any]] = []
    for event in audit:
        request = event["request"]
        consumer_id = request["consumer"]
        if request["operation"] == "claim":
            subscription = {"key": request["key"],
                            "job_id": request["job_id"]}
            current = replay_state.get(consumer_id)
            if current is None:
                expected = {"owner": request["owner"],
                            "until": request["now"] + request["lease"],
                            "position": None}
                replay_subs[consumer_id] = subscription
                replay_state[consumer_id] = dict(expected)
                expected_result = _claim_result(
                    consumer_id, subscription, expected, False)
                taken_over = False
            else:
                if replay_subs[consumer_id] != subscription:
                    raise ValueError("a consumer subscription cannot be "
                                     "changed by a later claim")
                if request["owner"] == current["owner"]:
                    # A same-owner renewal is valid only while the lease
                    # is still active (now == until counts); a strictly
                    # expired lease may not be renewed in place.
                    if request["now"] > current["until"]:
                        raise ValueError("a same-owner renewal requires a "
                                         "still-valid lease")
                    taken_over = False
                else:
                    if request["now"] <= current["until"]:
                        raise ValueError("a takeover claim requires the "
                                         "previous lease to have expired")
                    taken_over = True
                    current["owner"] = request["owner"]
                current["until"] = request["now"] + request["lease"]
                expected_result = _claim_result(
                    consumer_id, subscription, current, taken_over)
            if event["result"] != expected_result:
                raise ValueError("consumer claim audit result does not "
                                 "match its replay")
            continue
        current = replay_state.get(consumer_id)
        if current is None:
            raise ValueError("a consumer audit event precedes its first "
                             "claim")
        if current["owner"] != request["owner"] \
                or request["now"] > current["until"]:
            raise ValueError("only the current owner during a valid lease "
                             "may acknowledge or reject")
        target = request["position"]
        previous = current["position"]
        if previous is not None and target <= previous:
            raise ValueError("an appended ack or reject must advance the "
                             "position past the previous one")
        current["position"] = target
        if request["operation"] == "reject":
            # The reject result is the post-reject state plus exactly
            # the dead letter the dead_letters section keeps; collect
            # the attributed ledger record so the section can be
            # matched against the reject audit events one to one.
            letter = event["result"]["dead_letter"]
            replay_rejects.append(
                {"consumer": consumer_id, **copy.deepcopy(letter)})
            expected_result = {
                **_state_result(consumer_id, current), "dead_letter": letter}
        else:
            expected_result = _state_result(consumer_id, current)
        if event["result"] != expected_result:
            raise ValueError(
                f"consumer {request['operation']} audit result does not "
                "match its replay")
    if replay_subs != subscriptions or replay_state != consumers:
        raise ValueError("the consumer audit must reconstruct the "
                         "recorded subscriptions and states")

    # One audit event per idempotency binding, each event naming a
    # bound request; distinct idempotency keys may carry equivalent
    # requests (e.g. a second renewal under a new key), so the audit is
    # matched as a multiset rather than by unique request content.
    def _request_key(shape: dict[str, Any]) -> str:
        return json.dumps(shape, sort_keys=True, ensure_ascii=False,
                          allow_nan=False)

    binding_keys = sorted(_request_key(shape)
                          for shape in idempotency.values())
    event_keys = sorted(_request_key(shape) for shape in bound_requests)
    if binding_keys != event_keys:
        raise ValueError("consumer idempotency bindings and audit events "
                         "must match one to one")

    # The dead-letter section matches the reject audit events one to
    # one as a multiset: reject requests are idempotency-bound and the
    # audit pins each result's exact letter, so this additionally pins
    # that the section holds precisely the reject letters, no more and
    # no fewer.
    letter_keys = sorted(
        _request_key(_consumer_canonical_dead_letter(letter))
        for letter in dead_letters)
    reject_keys = sorted(
        _request_key(_consumer_canonical_dead_letter(letter))
        for letter in replay_rejects)
    if letter_keys != reject_keys:
        raise ValueError("consumer dead letters must match the reject "
                         "audit events one to one")
    if document_legacy and dead_letters:
        raise ValueError("a pre-dead-letter consumer ledger must carry "
                         "no dead letter section")
    # A ledger upgraded with the section keeps it even when empty; the
    # None return marks the original six-field shape for byte-exact
    # reproduction.
    return subscriptions, consumers, idempotency, audit, \
        (None if document_legacy else dead_letters)


def _load_consumer_bytes(
    consumer_real: str, coordination_real: str, raw: bytes,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]],
           dict[str, dict[str, Any]], list[dict[str, Any]],
           list[dict[str, Any]] | None]:
    try:
        data = finite_loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ConsumerLedgerInvalid(
            f"consumer ledger {consumer_real!r} is not valid canonical "
            "JSON") from exc
    try:
        subscriptions, consumers, idempotency, audit, dead_letters = \
            _validate_consumer_ledger(data, coordination_real)
        canonical = _consumer_canonical_bytes(
            coordination_real, subscriptions, consumers, idempotency, audit,
            dead_letters)
    except ValueError as exc:
        raise ConsumerLedgerInvalid(str(exc)) from exc
    if raw != canonical:
        raise ConsumerLedgerInvalid(
            f"consumer ledger {consumer_real!r} is not in canonical "
            "compact form")
    return subscriptions, consumers, idempotency, audit, dead_letters


def _preliminary_coordination(coordination_real: str) -> None:
    # Existence and own-format proof before any consumer lock is taken;
    # a miss is the endpoint's ordinary 404 and malformed bytes a 409.
    try:
        preliminary = _read_raw(coordination_real)
    except FileNotFoundError:
        raise CoordinationLedgerMissing(coordination_real)
    try:
        _parse_coordination_bytes(coordination_real, preliminary, {}, None,
                                  require_references=False)
    except ValueError as exc:
        raise CoordinationLedgerInvalid(str(exc)) from exc


def _read_locked_progress(
    coordination_real: str,
) -> list[dict[str, Any]]:
    # The coordination ledger's shared lock plus the nine business
    # ledgers' shared locks cover the read, exactly the read-only event
    # query's group; a racing batch writer is one complete version.
    with _get_store(coordination_real).lock:
        with _lock(coordination_real, shared=True):
            try:
                raw = _read_raw(coordination_real)
            except FileNotFoundError:
                raise CoordinationLedgerMissing(coordination_real)
            try:
                _batches, _audit, _events, inputs, _legacy, _doc_legacy = \
                    _parse_coordination_bytes(
                        coordination_real, raw, {}, None,
                        require_references=False)
                with _business_locks(inputs):
                    # A referenced business ledger that has vanished is a
                    # broken reference (409 invalid), like the read-only
                    # event serializer's _snapshot_checked mapping.
                    try:
                        snapshot = _Snapshot(inputs)
                    except FileNotFoundError as exc:
                        raise ValueError(
                            "coordination ledger references a missing "
                            "business ledger") from exc
                    _batches, _audit, progress, _ledger_inputs, _raw, \
                        _legacy, _doc_legacy = _load_coordination(
                            coordination_real,
                            {frozenset(snapshot.input_map.items()):
                             snapshot},
                            None)
            except ValueError as exc:
                raise CoordinationLedgerInvalid(str(exc)) from exc
    return copy.deepcopy(progress)


def _consumer_matches(
    event: dict[str, Any], subscription: dict[str, Any],
) -> bool:
    return (subscription["key"] is None
            or event["key"] == subscription["key"]) \
        and (subscription["job_id"] is None
             or event["job_id"] == subscription["job_id"])


def _event_at(progress: list[dict[str, Any]], position: int,
              subscription: dict[str, Any]) -> dict[str, Any] | None:
    for event in progress:
        if event["position"] == position:
            return event if _consumer_matches(event, subscription) else None
    return None


def _require_checkpoint_current(
    progress: list[dict[str, Any]], subscription: dict[str, Any],
    position: int | None,
) -> None:
    # Null is the pre-stream origin of a consumer that has never
    # acknowledged and has no event to check. Any other confirmed
    # position the current stream no longer holds as the same matching
    # event means the history behind the cursor was truncated, rewritten
    # or reused: the cursor is never auto-reset.
    if position is None:
        return
    event = _event_at(progress, position, subscription)
    if event is None:
        raise LookupError(
            "the acknowledged event is no longer present at its position "
            "in the current stream")


def _consumer_parent(consumer_real: str) -> None:
    if not os.path.isdir(os.path.dirname(consumer_real) or "."):
        raise ConsumersLedgerMissing(
            "consumer ledger parent directory does not exist")


def _claim_result(consumer: str, subscription: dict[str, Any],
                  state: dict[str, Any], taken_over: bool) -> dict[str, Any]:
    return {"consumer": consumer, "key": subscription["key"],
            "job_id": subscription["job_id"], "owner": state["owner"],
            "until": state["until"], "position": state["position"],
            "taken_over": taken_over}


def _state_result(consumer: str, state: dict[str, Any]) -> dict[str, Any]:
    return {"consumer": consumer, "owner": state["owner"],
            "until": state["until"], "position": state["position"]}


def _check_owner(state: dict[str, Any], owner: str, now: int) -> None:
    # Only the current owner while the lease is strictly valid may act.
    # A different owner is a ConsumerOwnershipError while the lease
    # holds and a ConsumerLeaseExpired once the previous owner's lease
    # has strictly expired; the current owner gets the same
    # ConsumerLeaseExpired after strict expiry. Both stay ordinary
    # PermissionError/TimeoutError subclasses to library callers.
    if state["owner"] != owner:
        if now <= state["until"]:
            raise ConsumerOwnershipError(
                "consumer is owned by another owner until "
                f"{state['until']}")
        raise ConsumerLeaseExpired(
            "the previous owner's consumer lease has expired")
    if now > state["until"]:
        raise ConsumerLeaseExpired("the consumer lease has expired")


def consume(
    coordination: str,
    ledger: str,
    operation: str,
    consumer: str,
    owner: str,
    now: int,
    *,
    key: str | None = None,
    job_id: str | None = None,
    lease: int | None = None,
    position: int | None = None,
    reason: str | None = None,
    limit: int = _DEFAULT_LIMIT,
    idem: str | None = None,
) -> tuple[dict[str, Any], bool]:
    """Claim, pull from, acknowledge or reject one persistent consumer.

    ``coordination`` is the fixed migration-batch coordination ledger
    and ``ledger`` the independent consumer ledger; both are non-empty
    strings resolving to distinct real paths. The consumer ledger is
    permanently bound to the coordination path resolved on its first
    write; a later call presenting a different coordination ledger
    raises ``ValueError``. No request field selects a file path.

    The four operations are:

    ``claim``
        First claim of ``consumer`` by ``owner`` at non-negative moment
        ``now`` for a positive ``lease`` window, with an optional fixed
        batch ``key`` and an optional ``job_id`` filter. It persists the
        subscription, the owner, the lease end (``now + lease``) and the
        initial null checkpoint. The fixed subscription can never be
        rewritten: a later claim carrying another key/job filter raises
        ``ValueError``; a claim without a batch key stays a permanent
        cross-batch range. A repeat claim by the same owner (under a new
        idempotency key) is the lease renewal and moves ``until`` only
        while the lease is still valid -- a claim moment equal to the
        lease end still counts as valid; once the lease has strictly
        expired the same owner may no longer renew in place and gets
        ``TimeoutError`` with the ledger untouched. Another owner is
        refused with ``PermissionError`` while the lease is still valid
        and may take over only once it has strictly expired, keeping the
        acknowledged position so unacknowledged events redeliver.

    ``pull``
        Returns one page, ``{"consumer", "owner", "until", "position",
        "events", "next"}`` where ``events``/``next`` follow the
        stream's fixed order and paging semantics over events strictly
        after the acknowledged position; ``limit`` is 1..1000 (default
        100). A pull never advances the checkpoint and never writes; a
        rejected event is never returned.

    ``ack``
        Moves the checkpoint forward to ``position``, which must name a
        real matching event in the current stream. An equal-position
        ack writes nothing and reports ``False``; a backward or
        past-tail ack raises ``ValueError``; a position the current
        stream truncated, rewrote or reused behind the cursor raises
        ``LookupError`` and never resets the cursor.

    ``reject``
        Dead-letters the single earliest unacknowledged event the fixed
        subscription still matches and atomically advances the
        checkpoint to it. ``position`` must name exactly that event:
        skipping a still-pending predecessor, naming a non-matching
        position or passing the stream tail all raise ``ValueError``,
        and, like an ack, a confirmed position the stream truncated,
        rewrote or reused raises ``LookupError``. ``reason`` is a
        string; surrounding whitespace is stripped and the remainder
        must span 1..512 Unicode code points. The result carries
        ``consumer``, ``owner``, ``until``, ``position`` and
        ``dead_letter``; the dead letter keeps the event's original
        position, its complete event snapshot, the trimmed reason,
        ``rejected_at`` (the reject moment) and the rejecting owner.
        Rejecting is idempotent through ``idem`` exactly like an ack.
        The first successful reject upgrades a pre-dead-letter ledger
        with its append-only ``dead_letters`` section; claim, pull and
        ack never change such a ledger's shape.

    Only the current owner while the lease is strictly valid may pull,
    ack or reject; a different owner gets ``PermissionError`` while the
    lease holds and an expired lease (for either owner)
    ``TimeoutError``. An unknown consumer raises ``KeyError``. Claim,
    ack and reject require the non-empty idempotency key ``idem``:
    replaying it with the equivalent complete request returns the
    original result with ``False`` without writing, while the same key
    with a changed request raises ``ValueError``; a pull takes no such
    key.

    Returns ``(result, created)``; ``created`` is ``True`` only for the
    first claim that creates the consumer and for an ack or reject that
    actually moves the checkpoint. A lease-renewing repeat claim and an
    expired-lease takeover write a new binding and audit event but, like
    :func:`run`'s takeover, report ``False`` because they create no new
    consumer; an in-place or idempotent replay also reports ``False``.
    A missing coordination ledger or the consumer ledger parent raises
    ``FileNotFoundError``; malformed or non-canonical bytes raise
    ``ValueError`` like every other ledger; other locking or I/O
    failures raise ``OSError``.
    """
    for value in (coordination, ledger):
        if not isinstance(value, str) or not value:
            raise ValueError("coordination and ledger must be non-empty "
                             "strings")
    if operation not in _CONSUMER_OPERATIONS:
        raise ValueError("operation must be claim, pull, ack or reject")
    if not isinstance(consumer, str) or not consumer:
        raise ValueError("consumer must be a non-empty string")
    if not isinstance(owner, str) or not owner:
        raise ValueError("owner must be a non-empty string")
    if not _is_plain_int(now) or now < 0:
        raise ValueError("now must be a non-boolean non-negative integer")
    if not _is_plain_int(limit) or not 1 <= limit <= _MAX_LIMIT:
        raise ValueError("limit must be a non-boolean integer between 1 "
                         "and 1000")
    if operation == "claim":
        for name, value in (("key", key), ("job_id", job_id)):
            if value is not None and (not isinstance(value, str)
                                      or not value):
                raise ValueError(
                    f"{name} must be null or a non-empty string")
        if not _is_plain_int(lease) or lease < 1:
            raise ValueError("lease must be a non-boolean positive integer")
    if operation in ("ack", "reject"):
        if not _is_plain_int(position) or position < 0:
            raise ValueError("position must be a non-boolean non-negative "
                             "integer")
    if operation == "reject":
        reason = _validate_reason(reason)
    if operation in ("claim", "ack", "reject"):
        if not isinstance(idem, str) or not idem:
            raise ValueError("a write operation requires a non-empty "
                             "idempotency key")
    elif idem is not None:
        raise ValueError("a pull takes no idempotency key")
    coordination_real = os.path.realpath(coordination)
    consumer_real = os.path.realpath(ledger)
    if coordination_real == consumer_real:
        raise ValueError("the consumer ledger must be distinct from the "
                         "coordination ledger")

    if operation == "pull":
        _preliminary_coordination(coordination_real)
        _consumer_parent(consumer_real)
        store = _get_store(consumer_real)
        with store.lock:
            with _lock(consumer_real, shared=True):
                try:
                    with open(consumer_real, "rb") as handle:
                        raw = handle.read()
                except FileNotFoundError:
                    raise KeyError(consumer)
                subscriptions, consumers, _idem, _audit, _dead = \
                    _load_consumer_bytes(
                        consumer_real, coordination_real, raw)
                state = consumers.get(consumer)
                if state is None:
                    raise KeyError(consumer)
                _check_owner(state, owner, now)
                progress = _read_locked_progress(coordination_real)
                _require_checkpoint_current(
                    progress, subscriptions[consumer], state["position"])
                page = _events_page(progress, state["position"], limit,
                                    subscriptions[consumer]["key"],
                                    subscriptions[consumer]["job_id"])
                result = {"consumer": consumer, "owner": state["owner"],
                          "until": state["until"],
                          "position": state["position"],
                          "events": page["events"], "next": page["next"]}
                return result, False

    request_shape = {
        "claim": {"operation": "claim", "consumer": consumer, "key": key,
                  "job_id": job_id, "owner": owner, "lease": lease,
                  "now": now},
        "ack": {"operation": "ack", "consumer": consumer, "owner": owner,
                "position": position, "now": now},
        "reject": {"operation": "reject", "consumer": consumer,
                   "owner": owner, "position": position, "reason": reason,
                   "now": now},
    }[operation]
    request = _validate_consumer_request(request_shape)

    # ack and reject need the stream itself; a claim only proves the
    # coordination ledger exists and is well formed.
    _preliminary_coordination(coordination_real)
    _consumer_parent(consumer_real)
    store = _get_store(consumer_real)
    with store.lock:
        with _lock(consumer_real):
            try:
                with open(consumer_real, "rb") as handle:
                    raw = handle.read()
            except FileNotFoundError:
                raw = None
            if raw is None:
                subscriptions: dict[str, dict[str, Any]] = {}
                consumers: dict[str, dict[str, Any]] = {}
                idempotency: dict[str, dict[str, Any]] = {}
                audit: list[dict[str, Any]] = []
                # A new ledger is born in the original six-field shape;
                # the section is added below only by the first reject,
                # exactly like an old ledger's first reject.
                dead_letters: list[dict[str, Any]] | None = None
                old_bytes: bytes | None = None
            else:
                subscriptions, consumers, idempotency, audit, \
                    dead_letters = _load_consumer_bytes(
                        consumer_real, coordination_real, raw)
                old_bytes = raw

            if idem in idempotency:
                saved = idempotency[idem]
                if saved != request:
                    raise ValueError(
                        "the idempotency key was already used with a "
                        "different request")
                original = next(
                    event["result"] for event in audit
                    if event["request"] == saved)
                return copy.deepcopy(original), False

            progress = (
                _read_locked_progress(coordination_real)
                if operation in ("ack", "reject") else None)
            # The first reject upgrades a pre-dead-letter (or newly
            # created) ledger with the empty section; claim and ack
            # leave the six-field shape untouched.
            if operation == "reject" and dead_letters is None:
                dead_letters = []
            result, changed = _consume_apply(
                request, idem, subscriptions, consumers, idempotency,
                audit, dead_letters, progress)
            payload = _consumer_canonical_bytes(
                coordination_real, subscriptions, consumers, idempotency,
                audit, dead_letters)
            _commit_file(consumer_real, payload, old_bytes,
                         prefix=_CONSUMER_PREFIX)
            return result, changed


def _consume_apply(
    request: dict[str, Any], idem_key: str,
    subscriptions: dict[str, dict[str, Any]],
    consumers: dict[str, dict[str, Any]],
    idempotency: dict[str, dict[str, Any]],
    audit: list[dict[str, Any]],
    dead_letters: list[dict[str, Any]] | None,
    progress: list[dict[str, Any]] | None,
) -> tuple[dict[str, Any], bool]:
    operation = request["operation"]
    consumer_id = request["consumer"]
    now = request["now"]

    def bind(result: dict[str, Any]) -> None:
        idempotency[idem_key] = _consumer_canonical_request(request)
        audit.append({"seq": len(audit), "at": now,
                      "request": _consumer_canonical_request(request),
                      "result": copy.deepcopy(result)})

    if operation == "claim":
        subscription = {
            "key": request["key"], "job_id": request["job_id"]}
        state = consumers.get(consumer_id)
        if state is None:
            state = {"owner": request["owner"],
                     "until": now + request["lease"], "position": None}
            subscriptions[consumer_id] = subscription
            consumers[consumer_id] = state
            result = _claim_result(consumer_id, subscription, state, False)
            bind(result)
            return result, True
        if subscriptions[consumer_id] != subscription:
            raise ValueError(
                "a consumer subscription cannot be changed after the "
                "first claim")
        if state["owner"] == request["owner"]:
            # The same owner may renew only while the current lease is
            # still valid; now == until is still active. Once the lease
            # has strictly expired even its former owner may not renew
            # in place -- a claim then raises TimeoutError and writes
            # nothing, just like a pull or ack would.
            if now > state["until"]:
                raise ConsumerLeaseExpired("the consumer lease has expired")
            state["until"] = now + request["lease"]
            result = _claim_result(
                consumer_id, subscriptions[consumer_id], state, False)
            bind(result)
            return result, False
        if now <= state["until"]:
            raise ConsumerOwnershipError(
                "consumer is owned by another owner until "
                f"{state['until']}")
        # Strict expiry: another owner may take over, keeping the
        # acknowledged position.
        state["owner"] = request["owner"]
        state["until"] = now + request["lease"]
        result = _claim_result(
            consumer_id, subscriptions[consumer_id], state, True)
        bind(result)
        return result, False

    state = consumers.get(consumer_id)
    if state is None:
        raise KeyError(consumer_id)
    _check_owner(state, request["owner"], now)

    assert progress is not None
    target = request["position"]
    subscription = subscriptions[consumer_id]
    _require_checkpoint_current(progress, subscription, state["position"])
    if state["position"] is not None and target < state["position"]:
        raise ValueError("the acknowledged position cannot move backwards")

    if operation == "reject":
        # A reject names exactly the earliest still-unacknowledged
        # event the fixed subscription matches: an already-confirmed
        # position, a skipped predecessor, a non-matching stream
        # position and a position past the tail are all invalid.
        if target == state["position"]:
            raise ValueError("the rejected position is already "
                             "acknowledged")
        earliest: dict[str, Any] | None = None
        for event in progress:
            if state["position"] is not None \
                    and event["position"] <= state["position"]:
                continue
            if _consumer_matches(event, subscription):
                earliest = event
                break
        tail = progress[-1]["position"] if progress else None
        if tail is None or target > tail:
            raise ValueError("the rejected position is past the stream tail")
        if earliest is None or target != earliest["position"]:
            raise ValueError("the rejected position must be the earliest "
                             "unacknowledged event the subscription matches")
        letter = {
            "position": target,
            "event": _progress_entry(earliest),
            "reason": request["reason"],
            "rejected_at": now,
            "owner": request["owner"],
        }
        assert dead_letters is not None
        # The ledger record attributes the public letter to the
        # consumer; the state result and the later query expose the
        # five-field letter without repeating the consumer.
        dead_letters.append(
            {"consumer": consumer_id, **copy.deepcopy(letter)})
        state["position"] = target
        result = {**_state_result(consumer_id, state),
                  "dead_letter": copy.deepcopy(letter)}
        bind(result)
        return result, True

    # ack
    if target == state["position"]:
        # An in-place replay (also acking position 0 when the cursor is
        # already there) changes and writes nothing.
        return _state_result(consumer_id, state), False
    tail = progress[-1]["position"] if progress else None
    if tail is None or target > tail:
        raise ValueError("the acknowledged position is past the stream tail")
    if _event_at(progress, target, subscription) is None:
        # A real stream position the fixed subscription does not match
        # is an invalid ack target, distinct from a confirmed cursor the
        # stream truncated or rewrote (the LookupError above).
        raise ValueError("the acknowledged position is not an event the "
                         "consumer subscription matches")
    state["position"] = target
    result = _state_result(consumer_id, state)
    bind(result)
    return result, True


def consume_response(
    coordination: str,
    ledger: str,
    operation: str,
    consumer: str,
    owner: str,
    now: int,
    **kwargs: Any,
) -> bytes:
    """Serialize :func:`consume`'s result while every lock is held.

    The page of a pull and the checkpoint verification of an ack are
    decided under the coordination ledger's shared group lock; the
    serialized bytes therefore reflect one complete stream version.
    """
    result, _changed = consume(coordination, ledger, operation, consumer,
                               owner, now, **kwargs)
    return _render(result)


def consumer_subscription(
    coordination: str, ledger: str, consumer: str,
) -> dict[str, Any]:
    """Return one consumer's fixed subscription without writing.

    The returned object carries ``key`` and ``job_id`` (each null or
    the fixed non-empty string) and is the authorization scope a pull
    or ack is served under, since a pull/ack request never names the
    batch key itself. A missing consumer ledger parent raises
    ``FileNotFoundError``, an unknown consumer ``KeyError`` and
    malformed or non-canonical bytes ``ValueError``.
    """
    for value in (coordination, ledger):
        if not isinstance(value, str) or not value:
            raise ValueError("coordination and ledger must be non-empty "
                             "strings")
    if not isinstance(consumer, str) or not consumer:
        raise ValueError("consumer must be a non-empty string")
    coordination_real = os.path.realpath(coordination)
    consumer_real = os.path.realpath(ledger)
    _consumer_parent(consumer_real)
    store = _get_store(consumer_real)
    with store.lock:
        with _lock(consumer_real, shared=True):
            try:
                with open(consumer_real, "rb") as handle:
                    raw = handle.read()
            except FileNotFoundError:
                raise KeyError(consumer)
            subscriptions, consumers, _idempotency, _audit, _dead = \
                _load_consumer_bytes(consumer_real, coordination_real, raw)
            if consumer not in consumers:
                raise KeyError(consumer)
            return copy.deepcopy(subscriptions[consumer])


def _status_snapshot(
    coordination_real: str, consumer_real: str, consumer: str,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    # One read-only snapshot: the consumer's fixed subscription and
    # state under the consumer ledger's shared lock, followed by the
    # coordination stream under the coordination ledger's and the nine
    # business ledgers' shared locks (the same group a pull uses). The
    # progress list is deep-copied while those locks are held, so the
    # status is computed from one complete version of every ledger.
    store = _get_store(consumer_real)
    with store.lock:
        with _lock(consumer_real, shared=True):
            try:
                with open(consumer_real, "rb") as handle:
                    raw = handle.read()
            except FileNotFoundError:
                # No consumer ledger yet means the consumer was never
                # claimed: an unknown consumer, exactly like a pull.
                raise KeyError(consumer)
            subscriptions, consumers, _idempotency, _audit, _dead = \
                _load_consumer_bytes(consumer_real, coordination_real, raw)
            state = consumers.get(consumer)
            if state is None:
                raise KeyError(consumer)
            subscription = subscriptions[consumer]
            progress = _read_locked_progress(coordination_real)
            return (copy.deepcopy(subscription), copy.deepcopy(state),
                    progress)


def consumer_status(
    coordination: str, ledger: str, consumer: str, now: int,
) -> dict[str, Any]:
    """Return one consumer's lease state and backlog without writing.

    ``coordination`` is the fixed migration-batch coordination ledger
    and ``ledger`` the independent consumer ledger; both are non-empty
    strings resolving to distinct real paths, ``consumer`` a non-empty
    string and ``now`` a non-boolean non-negative integer observation
    moment. Bad arguments or coinciding real paths raise ``ValueError``
    before any file is opened.

    The result carries, in fixed order, ``consumer``, ``key``,
    ``job_id``, ``owner``, ``until``, ``lease``, ``position``,
    ``pending`` and ``oldest``:

    * ``key``/``job_id`` are the consumer's fixed subscription (each
      null or the fixed non-empty string);
    * ``owner`` and ``until`` are the current lease holder and end;
    * ``lease`` is ``"active"`` when ``now`` is no later than ``until``
      -- the moment equal to ``until`` still counts as active -- and
      ``"expired"`` after strict expiry;
    * ``position`` is the acknowledged stream position (null before the
      first ack);
    * ``pending`` counts the current stream's events that match the
      fixed subscription and sit strictly after that position;
    * ``oldest`` is the earliest such unacknowledged event in the
      stream's full event shape (complete post-commit batch snapshot
      included), or null when there is no backlog -- in which case
      ``pending`` is 0.

    A confirmed position the current stream truncated, rewrote or
    reused raises ``LookupError``; the cursor is never reset. The
    snapshot is read-only under the shared locks of the consumer
    ledger, the coordination ledger and the referenced business
    ledgers and never writes a file. An unknown consumer (including a
    consumer ledger that was never created) raises ``KeyError``; a
    missing coordination ledger or consumer ledger parent raises
    ``FileNotFoundError``; malformed consumer bytes raise
    :class:`ConsumerLedgerInvalid` and malformed coordination or
    referenced-business bytes :class:`CoordinationLedgerInvalid`; any
    other locking or I/O failure raises ``OSError``.
    """
    for value in (coordination, ledger):
        if not isinstance(value, str) or not value:
            raise ValueError("coordination and ledger must be non-empty "
                             "strings")
    if not isinstance(consumer, str) or not consumer:
        raise ValueError("consumer must be a non-empty string")
    if not _is_plain_int(now) or now < 0:
        raise ValueError("now must be a non-boolean non-negative integer")
    coordination_real = os.path.realpath(coordination)
    consumer_real = os.path.realpath(ledger)
    if coordination_real == consumer_real:
        raise ValueError("the consumer ledger must be distinct from the "
                         "coordination ledger")
    # The consumer ledger's directory is the only file-system fact the
    # subscription-read stage needs; the coordination stream opens only
    # after the consumer itself has been found.
    _consumer_parent(consumer_real)
    subscription, state, progress = _status_snapshot(
        coordination_real, consumer_real, consumer)
    position = state["position"]
    _require_checkpoint_current(progress, subscription, position)
    unacknowledged: list[dict[str, Any]] = []
    for event in progress:
        if position is not None and event["position"] <= position:
            continue
        if _consumer_matches(event, subscription):
            unacknowledged.append(event)
    return {
        "consumer": consumer,
        "key": subscription["key"],
        "job_id": subscription["job_id"],
        "owner": state["owner"],
        "until": state["until"],
        "lease": "active" if now <= state["until"] else "expired",
        "position": position,
        "pending": len(unacknowledged),
        "oldest": _progress_entry(unacknowledged[0]) if unacknowledged
        else None,
    }


def consumer_status_response(
    coordination: str, ledger: str, consumer: str, now: int,
) -> bytes:
    """Serialize :func:`consumer_status`'s result from one snapshot.

    The subscription, state and progress are read under the consumer
    ledger's, the coordination ledger's and the referenced business
    ledgers' shared locks, and the compact response bytes reflect that
    one complete version.
    """
    return _render(consumer_status(coordination, ledger, consumer, now))


def consumer_dead_letters(
    coordination: str, ledger: str, consumer: str, *,
    cursor: int | None = None, limit: int = _DEFAULT_LIMIT,
) -> dict[str, Any]:
    """Return one page of one consumer's dead letters without writing.

    ``coordination`` is the fixed migration-batch coordination ledger
    and ``ledger`` the independent consumer ledger; both are non-empty
    strings resolving to distinct real paths and ``consumer`` a
    non-empty string. ``cursor`` is ``None`` or a non-boolean
    non-negative integer: only letters whose original position is
    strictly greater are considered, so omitting it reads from the
    start. ``limit`` is a non-boolean integer from 1 to 1000,
    defaulting to 100. Bad arguments or coinciding real paths raise
    ``ValueError`` before any file is opened.

    The result carries, in fixed order, ``consumer``, ``entries`` and
    ``next``. The entries are that consumer's dead letters ordered by
    their original stream position (the reject order for one consumer):
    each in the five-field public shape, in order ``position``,
    ``event`` (the complete event in the event stream's full shape),
    ``reason``, ``rejected_at`` and ``owner``. Positions the
    subscription's rejects skipped never occur here -- every reject
    advances the cursor -- and filtered positions never consume page
    capacity. ``next`` is the page's last original position when more
    letters remain, else ``None``. The query reads only the consumer
    ledger: a dead letter is a self-contained historical record and the
    coordination stream is never opened.

    A consumer ledger written before dead letters existed answers an
    empty page without being upgraded. An unknown consumer (including a
    consumer ledger that was never created) raises ``KeyError``; a
    missing consumer ledger parent raises ``FileNotFoundError``;
    malformed or non-canonical consumer bytes raise
    :class:`ConsumerLedgerInvalid`; any other locking or I/O failure
    raises ``OSError``.
    """
    for value in (coordination, ledger):
        if not isinstance(value, str) or not value:
            raise ValueError("coordination and ledger must be non-empty "
                             "strings")
    if not isinstance(consumer, str) or not consumer:
        raise ValueError("consumer must be a non-empty string")
    if cursor is not None and (not _is_plain_int(cursor) or cursor < 0):
        raise ValueError("cursor must be None or a non-boolean "
                         "non-negative integer")
    if not _is_plain_int(limit) or not 1 <= limit <= _MAX_LIMIT:
        raise ValueError("limit must be a non-boolean integer between 1 "
                         "and 1000")
    coordination_real = os.path.realpath(coordination)
    consumer_real = os.path.realpath(ledger)
    if coordination_real == consumer_real:
        raise ValueError("the consumer ledger must be distinct from the "
                         "coordination ledger")
    _consumer_parent(consumer_real)
    store = _get_store(consumer_real)
    with store.lock:
        with _lock(consumer_real, shared=True):
            try:
                with open(consumer_real, "rb") as handle:
                    raw = handle.read()
            except FileNotFoundError:
                raise KeyError(consumer)
            subscriptions, consumers, _idempotency, _audit, \
                dead_letters = _load_consumer_bytes(
                    consumer_real, coordination_real, raw)
            if consumer not in consumers:
                raise KeyError(consumer)
            # The section is append-only across consumers; rejects for
            # one consumer strictly advance that consumer's cursor, so
            # its letters already arrive in original-position order --
            # but the page is sorted explicitly to pin the promise
            # independently of inter-consumer interleaving.
            letters = sorted((record for record in (dead_letters or [])
                              if record["consumer"] == consumer),
                             key=lambda record: record["position"])
    matched = [record for record in letters
               if cursor is None or record["position"] > cursor]
    if len(matched) > limit:
        page = matched[:limit]
        next_cursor: int | None = page[-1]["position"]
    else:
        page = matched
        next_cursor = None
    return {"consumer": consumer,
            "entries": [_public_dead_letter(record) for record in page],
            "next": next_cursor}


def consumer_dead_letters_response(
    coordination: str, ledger: str, consumer: str, *,
    cursor: int | None = None, limit: int = _DEFAULT_LIMIT,
) -> bytes:
    """Serialize :func:`consumer_dead_letters`' page from one snapshot.

    The dead-letter section is read under the consumer ledger's shared
    lock, and the compact response bytes reflect that one version.
    """
    return _render(consumer_dead_letters(
        coordination, ledger, consumer, cursor=cursor, limit=limit))
