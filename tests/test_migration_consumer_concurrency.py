"""Model-driven concurrency consistency tests for persistent consumers.

These tests exercise the persistent consumer state machine behind
``migration_batch.consume`` (claim/pull/ack/reject), ``consumer_status``
and ``consumer_dead_letters`` under deliberately raced calls, without
changing any public interface, ledger format, exception type or
sequential-call result.

The fixture is one fixed coordination event stream holding several
batches, several member jobs and contiguous zero-based positions. Every
race is released by one ``threading.Barrier`` and bounded by
``_WAIT``-second waits, so no assertion depends on sleep durations,
network, thread scheduling order or third-party services: a race's
outcome is accepted exactly when *some* legal serial order of the raced
calls explains every observed result and the persisted bytes -- the
linearizability criterion, checked against an independent in-memory
model of the documented state machine (lease boundary with the moment
equal to the lease end still valid, takeover only after strict expiry,
the immutable subscription, the monotonic cursor, the
earliest-unacknowledged-matching-event reject rule and the idempotency
key binding).

After every race the persisted consumer ledger is compared section by
section with the model, then re-read through the public queries
(status, pulls, dead-letter pages -- including filters, empty pages,
page boundaries and ``next`` continuations) and compared again. Every
reference is then dropped and the same queries are re-issued against
the same paths, proving the answers come from the persisted bytes, and
the read-only queries are shown to leave the ledger byte-for-byte
untouched. Fixed-seed rounds of mixed sequential and raced operations
extend the same checks; a failure reports the seed and the full
operation trace so the run reproduces exactly.
"""

from __future__ import annotations

import copy
import gc
import json
import os
import random
import threading
import unittest
from itertools import permutations
from pathlib import Path
from typing import Any

from carbon_market import dispatch as dispatch_module
from carbon_market import jobs as jobs_module
from carbon_market import migration_batch as mb
from carbon_market.market import clear_live
from tests.test_migration_batch import MigrationBatchTest, _job

_WAIT = 15.0  # Bounded wait (seconds): a deadlock fails, never hangs.


def _category(exc: BaseException) -> str:
    # The documented public exception surface, most specific first:
    # ConsumerOwnershipError stays a PermissionError and
    # ConsumerLeaseExpired a TimeoutError to library callers, and a
    # KeyError must not be confused with the cursor-regression
    # LookupError it subclasses.
    if isinstance(exc, PermissionError):
        return "permission"
    if isinstance(exc, TimeoutError):
        return "timeout"
    if isinstance(exc, KeyError):
        return "key"
    if isinstance(exc, LookupError):
        return "lookup"
    if isinstance(exc, ValueError):
        return "value"
    return "other"


def _short(outcome: tuple) -> str:
    if outcome[0] == "err":
        return f"err:{outcome[1]}"
    return f"ok:created={outcome[2]}"


class _Op:
    """One consume call, fully specified so it can be executed against
    the real ledger and replayed against the model."""

    def __init__(self, operation: str, consumer: str, owner: str, now: int,
                 *, key: str | None = None, job_id: str | None = None,
                 lease: int | None = None, position: int | None = None,
                 reason: str | None = None, limit: int = 100,
                 idem: str | None = None) -> None:
        self.operation = operation
        self.consumer = consumer
        self.owner = owner
        self.now = now
        self.key = key
        self.job_id = job_id
        self.lease = lease
        self.position = position
        self.reason = reason
        self.limit = limit
        self.idem = idem

    @classmethod
    def from_request(cls, request: dict[str, Any], idem: str) -> "_Op":
        # Rebuild the call a persisted idempotency binding names, so a
        # replay is byte-equivalent to the original request.
        return cls(request["operation"], request["consumer"],
                   request["owner"], request["now"],
                   key=request.get("key"), job_id=request.get("job_id"),
                   lease=request.get("lease"),
                   position=request.get("position"),
                   reason=request.get("reason"), idem=idem)

    def describe(self) -> str:
        parts = [self.operation, self.consumer, self.owner,
                 f"now={self.now}"]
        if self.operation == "claim":
            parts += [f"key={self.key}", f"job={self.job_id}",
                      f"lease={self.lease}"]
        if self.operation in ("ack", "reject"):
            parts.append(f"pos={self.position}")
        if self.operation == "reject":
            parts.append(f"reason={self.reason!r}")
        if self.operation == "pull":
            parts.append(f"limit={self.limit}")
        if self.idem is not None:
            parts.append(f"idem={self.idem}")
        return " ".join(parts)


class _Model:
    """Independent in-memory model of the documented consumer state
    machine, driven only by the fixed public event stream."""

    def __init__(self, stream: list[dict[str, Any]]) -> None:
        self.stream = stream
        self.tail = stream[-1]["position"] if stream else None
        self.subs: dict[str, dict[str, Any]] = {}
        self.states: dict[str, dict[str, Any]] = {}
        self.idem: dict[str, dict[str, Any]] = {}
        self.audit: list[dict[str, Any]] = []
        self.dead: list[dict[str, Any]] = []
        self.upgraded = False

    def clone(self) -> "_Model":
        other = _Model(self.stream)
        other.subs = copy.deepcopy(self.subs)
        other.states = copy.deepcopy(self.states)
        other.idem = copy.deepcopy(self.idem)
        other.audit = copy.deepcopy(self.audit)
        other.dead = copy.deepcopy(self.dead)
        other.upgraded = self.upgraded
        return other

    # -- stream helpers -------------------------------------------------

    @staticmethod
    def event_matches(event: dict[str, Any], sub: dict[str, Any]) -> bool:
        return (sub["key"] is None or event["key"] == sub["key"]) \
            and (sub["job_id"] is None or event["job_id"] == sub["job_id"])

    def matching(self, sub: dict[str, Any],
                 after: int | None) -> list[dict[str, Any]]:
        return [event for event in self.stream
                if (after is None or event["position"] > after)
                and self.event_matches(event, sub)]

    # -- result shapes ---------------------------------------------------

    @staticmethod
    def _claim_result(consumer: str, sub: dict[str, Any],
                      state: dict[str, Any], taken_over: bool) -> dict:
        return {"consumer": consumer, "key": sub["key"],
                "job_id": sub["job_id"], "owner": state["owner"],
                "until": state["until"], "position": state["position"],
                "taken_over": taken_over}

    @staticmethod
    def _state_result(consumer: str, state: dict[str, Any]) -> dict:
        return {"consumer": consumer, "owner": state["owner"],
                "until": state["until"], "position": state["position"]}

    @staticmethod
    def _check_owner(state: dict[str, Any], owner: str, now: int) -> None:
        if state["owner"] != owner:
            if now <= state["until"]:
                raise PermissionError("owned by another owner")
            raise TimeoutError("previous owner's lease expired")
        if now > state["until"]:
            raise TimeoutError("lease expired")

    def _request(self, op: _Op) -> dict[str, Any]:
        if op.operation == "claim":
            return {"operation": "claim", "consumer": op.consumer,
                    "key": op.key, "job_id": op.job_id, "owner": op.owner,
                    "lease": op.lease, "now": op.now}
        if op.operation == "ack":
            return {"operation": "ack", "consumer": op.consumer,
                    "owner": op.owner, "position": op.position,
                    "now": op.now}
        return {"operation": "reject", "consumer": op.consumer,
                "owner": op.owner, "position": op.position,
                "reason": op.reason.strip(), "now": op.now}

    def _bind(self, op: _Op, request: dict[str, Any],
              result: dict[str, Any]) -> None:
        self.idem[op.idem] = request
        self.audit.append({"seq": len(self.audit), "at": request["now"],
                           "request": request,
                           "result": copy.deepcopy(result)})

    # -- the state machine ------------------------------------------------

    def apply(self, op: _Op) -> tuple:
        try:
            result, created = self._apply(op)
            return ("ok", result, created)
        except (PermissionError, TimeoutError, KeyError, LookupError,
                ValueError) as exc:
            return ("err", _category(exc))

    def _apply(self, op: _Op) -> tuple[dict[str, Any], bool]:
        if op.operation == "pull":
            state = self.states.get(op.consumer)
            if state is None:
                raise KeyError(op.consumer)
            self._check_owner(state, op.owner, op.now)
            return self.pull(op.consumer, op.owner, op.now, op.limit), False
        request = self._request(op)
        saved = self.idem.get(op.idem)
        if saved is not None:
            if saved != request:
                raise ValueError("idempotency key replayed with a "
                                 "different request")
            original = next(event["result"] for event in self.audit
                            if event["request"] == saved)
            return copy.deepcopy(original), False
        if op.operation == "claim":
            return self._claim(op, request)
        return self._ack_or_reject(op, request)

    def _claim(self, op: _Op, request: dict[str, Any]) -> tuple[dict, bool]:
        sub = {"key": op.key, "job_id": op.job_id}
        state = self.states.get(op.consumer)
        if state is None:
            state = {"owner": op.owner, "until": op.now + op.lease,
                     "position": None}
            self.subs[op.consumer] = sub
            self.states[op.consumer] = state
            result = self._claim_result(op.consumer, sub, state, False)
            self._bind(op, request, result)
            return result, True
        if self.subs[op.consumer] != sub:
            raise ValueError("subscription cannot change")
        if state["owner"] == op.owner:
            # Renewal only while the lease is still valid; the moment
            # equal to the lease end counts, strict expiry does not.
            if op.now > state["until"]:
                raise TimeoutError("lease expired")
            state["until"] = op.now + op.lease
            result = self._claim_result(op.consumer, sub, state, False)
            self._bind(op, request, result)
            return result, False
        if op.now <= state["until"]:
            raise PermissionError("owned by another owner")
        # Strict expiry: another owner takes over, keeping the cursor.
        state["owner"] = op.owner
        state["until"] = op.now + op.lease
        result = self._claim_result(op.consumer, sub, state, True)
        self._bind(op, request, result)
        return result, False

    def _ack_or_reject(self, op: _Op,
                       request: dict[str, Any]) -> tuple[dict, bool]:
        state = self.states.get(op.consumer)
        if state is None:
            raise KeyError(op.consumer)
        self._check_owner(state, op.owner, op.now)
        sub = self.subs[op.consumer]
        target = op.position
        if state["position"] is not None and target < state["position"]:
            raise ValueError("position cannot move backwards")
        if op.operation == "ack":
            if target == state["position"]:
                # In-place ack: no write, no idempotency binding.
                return self._state_result(op.consumer, state), False
            if self.tail is None or target > self.tail:
                raise ValueError("past the stream tail")
            event = next((e for e in self.stream
                          if e["position"] == target), None)
            if event is None or not self.event_matches(event, sub):
                raise ValueError("not a matching event")
            state["position"] = target
            result = self._state_result(op.consumer, state)
            self._bind(op, request, result)
            return result, True
        # reject: exactly the earliest unacknowledged matching event.
        if target == state["position"]:
            raise ValueError("already acknowledged")
        earliest = next(iter(self.matching(sub, state["position"])), None)
        if self.tail is None or target > self.tail:
            raise ValueError("past the stream tail")
        if earliest is None or target != earliest["position"]:
            raise ValueError("not the earliest unacknowledged match")
        letter = {"position": target, "event": copy.deepcopy(earliest),
                  "reason": request["reason"], "rejected_at": op.now,
                  "owner": op.owner}
        self.upgraded = True
        self.dead.append({"consumer": op.consumer,
                          **copy.deepcopy(letter)})
        state["position"] = target
        result = {**self._state_result(op.consumer, state),
                  "dead_letter": copy.deepcopy(letter)}
        self._bind(op, request, result)
        return result, True

    # -- read-only queries -------------------------------------------------

    def pull(self, consumer: str, owner: str, now: int,
             limit: int) -> dict[str, Any]:
        state = self.states[consumer]
        self._check_owner(state, owner, now)
        matched = self.matching(self.subs[consumer], state["position"])
        page = matched[:limit]
        return {"consumer": consumer, "owner": state["owner"],
                "until": state["until"], "position": state["position"],
                "events": copy.deepcopy(page),
                "next": page[-1]["position"] if len(matched) > limit
                else None}

    def status(self, consumer: str, now: int) -> dict[str, Any]:
        state = self.states[consumer]
        sub = self.subs[consumer]
        unacknowledged = self.matching(sub, state["position"])
        return {"consumer": consumer, "key": sub["key"],
                "job_id": sub["job_id"], "owner": state["owner"],
                "until": state["until"],
                "lease": "active" if now <= state["until"] else "expired",
                "position": state["position"],
                "pending": len(unacknowledged),
                "oldest": copy.deepcopy(unacknowledged[0])
                if unacknowledged else None}

    def dead_page(self, consumer: str, cursor: int | None,
                  limit: int) -> dict[str, Any]:
        letters = sorted((record for record in self.dead
                          if record["consumer"] == consumer),
                         key=lambda record: record["position"])
        matched = [record for record in letters
                   if cursor is None or record["position"] > cursor]
        page = matched[:limit]
        entries = [{"position": record["position"],
                    "event": copy.deepcopy(record["event"]),
                    "reason": record["reason"],
                    "rejected_at": record["rejected_at"],
                    "owner": record["owner"]} for record in page]
        return {"consumer": consumer, "entries": entries,
                "next": page[-1]["position"] if len(matched) > limit
                else None}


class ConsumerConcurrencyTest(unittest.TestCase):
    """Raced consume calls checked for linearizability against the model."""

    def setUp(self) -> None:
        self.fx = MigrationBatchTest(
            "test_get_returns_copy_and_unknown_key_raises")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.fx._prepare_migrate()
        # A second migratable job so the stream spans several jobs.
        paths = self.fx.paths
        jobs_module.submit(paths["jobs"], _job("j-2"), "jk-2")
        clear_live(paths["jobs"], paths["supply"], paths["signals"],
                   paths["trades"], "j-2", "t-2", 10)
        dispatch_module.commit(paths["jobs"], paths["supply"],
                               paths["trades"], paths["dispatch"], "j-2",
                               "d-2", 10)
        fx = self.fx
        fx._run(key="aaa", owner="o1", now=30)
        fx._run(key="aaa", owner="o1", now=31, receipts={
            "j-1": fx._receipt("copy", "succeeded", "复制完成", 31),
            "j-2": fx._receipt("copy", "succeeded", "c2-copy", 31)})
        fx._run(key="zzz", now=32)
        fx._run(key="aaa", owner="o1", now=33, receipts={
            "j-1": fx._receipt("switch", "succeeded", "切换完成", 33),
            "j-2": fx._receipt("switch", "succeeded", "c2-switch", 33)})
        fx._run(key="mmm", now=34)
        self.coord = paths["coord"]
        self.cons = os.path.join(self.fx.tmp.name, "consumers.json")
        # The event stream is fixed for the whole test: consume calls
        # never rewrite a coordination byte.
        self.stream = mb.events(self.coord, limit=1000)["events"]
        positions = [event["position"] for event in self.stream]
        self.assertEqual(positions, list(range(len(self.stream))))
        self.assertGreaterEqual(
            len({event["key"] for event in self.stream}), 3)
        self.assertGreaterEqual(
            len({event["job_id"] for event in self.stream
                 if event["job_id"] is not None}), 2)

    # -- harness ------------------------------------------------------------

    def _new_model(self) -> _Model:
        return _Model(self.stream)

    def _execute(self, cons: str, op: _Op) -> tuple:
        try:
            result, created = mb.consume(
                self.coord, cons, op.operation, op.consumer, op.owner,
                op.now, key=op.key, job_id=op.job_id, lease=op.lease,
                position=op.position, reason=op.reason, limit=op.limit,
                idem=op.idem)
            return ("ok", result, created)
        except (PermissionError, TimeoutError, KeyError, LookupError,
                ValueError) as exc:
            return ("err", _category(exc))
        except BaseException as exc:  # surfaced by the assertions
            return ("err", "other", repr(exc))

    def _race(self, cons: str, ops: list[_Op]) -> list[tuple]:
        # One barrier releases every thread together; any interleaving
        # the scheduler picks is acceptable as long as some legal serial
        # order explains it.
        barrier = threading.Barrier(len(ops))
        outcomes: list[tuple | None] = [None] * len(ops)

        def target(index: int, op: _Op) -> None:
            barrier.wait(_WAIT)
            outcomes[index] = self._execute(cons, op)

        threads = [threading.Thread(target=target, args=(index, op),
                                    name=f"race-{index}", daemon=True)
                   for index, op in enumerate(ops)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(_WAIT)
        for thread in threads:
            self.assertFalse(thread.is_alive(),
                             f"{thread.name} is stuck (possible deadlock)")
        for outcome in outcomes:
            self.assertIsNotNone(outcome)
        return list(outcomes)

    def _apply_seq(self, cons: str, model: _Model, op: _Op,
                   trace: list[str] | None = None) -> tuple:
        expected = model.apply(op)
        got = self._execute(cons, op)
        if trace is not None:
            trace.append(f"seq {op.describe()} -> {_short(got)}")
        self.assertEqual(got, expected,
                         f"sequential outcome diverges from model: "
                         f"{op.describe()}")
        return got

    def _explain(self, model: _Model, ops: list[_Op],
                 outcomes: list[tuple]) -> list[tuple[tuple, _Model]]:
        # Every permutation of the raced calls is replayed against a
        # fresh clone of the pre-race model; a permutation explains the
        # race when it reproduces every observed outcome exactly.
        explaining = []
        for perm in permutations(range(len(ops))):
            candidate = model.clone()
            if all(candidate.apply(ops[index]) == outcomes[index]
                   for index in perm):
                explaining.append((perm, candidate))
        return explaining

    def _doc_matches(self, cons: str, model: _Model) -> bool:
        doc = json.loads(Path(cons).read_text("utf-8"))
        if doc["subscriptions"] != model.subs \
                or doc["consumers"] != model.states \
                or doc["idempotency"] != model.idem \
                or doc["audit"] != model.audit:
            return False
        if model.upgraded:
            return doc.get("dead_letters") == model.dead
        return "dead_letters" not in doc

    def _race_and_check(self, cons: str, model: _Model, ops: list[_Op],
                        note: str) -> tuple[_Model, list[tuple]]:
        outcomes = self._race(cons, ops)
        detail = f"{note}: " + " | ".join(
            f"{op.describe()} -> {_short(outcome)}"
            for op, outcome in zip(ops, outcomes))
        explaining = self._explain(model, ops, outcomes)
        self.assertTrue(explaining,
                        f"no legal serial order explains {detail}")
        adopted = [candidate for _perm, candidate in explaining
                   if self._doc_matches(cons, candidate)]
        self.assertTrue(adopted,
                        f"persisted state matches no legal serial order "
                        f"of {detail}")
        adopted_model = adopted[0]
        self._verify_round(cons, adopted_model)
        return adopted_model, outcomes

    # -- public-query verification -----------------------------------------

    def _run_query(self, cons: str, model: _Model, query: tuple):
        kind, consumer, arg = query
        if kind == "status":
            got = mb.consumer_status(self.coord, cons, consumer, arg)
            self.assertEqual(got, model.status(consumer, arg))
            return got
        if kind == "pull":
            state = model.states[consumer]
            got, changed = mb.consume(self.coord, cons, "pull", consumer,
                                      state["owner"], state["until"],
                                      limit=arg)
            self.assertFalse(changed)
            self.assertEqual(got, model.pull(
                consumer, state["owner"], state["until"], arg))
            return got
        # A dead-letter walk checks every page and its next cursor, one
        # page at a time, until the drained empty page.
        self.assertEqual(kind, "dead")
        cursor = None
        pages = []
        while True:
            got = mb.consumer_dead_letters(self.coord, cons, consumer,
                                           cursor=cursor, limit=arg)
            self.assertEqual(got, model.dead_page(consumer, cursor, arg))
            pages.append(got)
            if got["next"] is None:
                break
            cursor = got["next"]
        got = mb.consumer_dead_letters(self.coord, cons, consumer,
                                       cursor=2 ** 30, limit=arg)
        self.assertEqual(got, model.dead_page(consumer, 2 ** 30, arg))
        self.assertEqual(got["entries"], [])
        self.assertIsNone(got["next"])
        return pages

    def _verify_round(self, cons: str, model: _Model) -> None:
        # The persisted bytes must equal the model section by section...
        self.assertTrue(self._doc_matches(cons, model),
                        "persisted consumer ledger diverges from model")
        raw = Path(cons).read_bytes()
        queries = []
        for consumer in sorted(model.states):
            until = model.states[consumer]["until"]
            # The lease boundary: the end moment itself is still active.
            for now in (0, until, until + 1):
                queries.append(("status", consumer, now))
            # Page boundaries, empty pages and next continuations.
            for limit in (1, 2, 1000):
                queries.append(("pull", consumer, limit))
                queries.append(("dead", consumer, limit))
        first = [self._run_query(cons, model, query) for query in queries]
        # The read-only queries never write a byte.
        self.assertEqual(Path(cons).read_bytes(), raw)
        # Reopen: drop every collected reference and re-issue the same
        # queries against the same paths; identical answers can only
        # come from the persisted bytes.
        del first
        gc.collect()
        second = [self._run_query(cons, model, query) for query in queries]
        del second
        gc.collect()
        self.assertEqual(Path(cons).read_bytes(), raw)

    def _light_verify(self, cons: str, model: _Model) -> None:
        self.assertTrue(self._doc_matches(cons, model),
                        "persisted consumer ledger diverges from model")
        raw = Path(cons).read_bytes()
        consumer = sorted(model.states)[0]
        until = model.states[consumer]["until"]
        self.assertEqual(
            mb.consumer_status(self.coord, cons, consumer, until),
            model.status(consumer, until))
        self.assertEqual(
            mb.consumer_dead_letters(self.coord, cons, consumer, limit=2),
            model.dead_page(consumer, None, 2))
        self.assertEqual(Path(cons).read_bytes(), raw)

    def _seed_claim(self, model: _Model, consumer: str = "c1",
                    owner: str = "o1", now: int = 40, lease: int = 100,
                    idem: str = "k1", **filters) -> None:
        outcome = self._apply_seq(
            self.cons, model,
            _Op("claim", consumer, owner, now, lease=lease, idem=idem,
                **filters))
        self.assertEqual(outcome[0], "ok")
        self.assertTrue(outcome[2])

    # -- idempotency key binding ---------------------------------------------

    def test_concurrent_identical_claim_binds_once(self) -> None:
        model = self._new_model()
        ops = [_Op("claim", "c1", "o1", 40, lease=100, idem="k1")
               for _ in range(4)]
        model, outcomes = self._race_and_check(
            self.cons, model, ops, "identical-claim")
        # Exactly one call is the new submission; every replay returns
        # the same business result, and the ledger holds one audit
        # event and one binding.
        self.assertTrue(all(outcome[0] == "ok" for outcome in outcomes))
        self.assertEqual(
            sum(1 for outcome in outcomes if outcome[2]), 1)
        self.assertEqual(
            len({json.dumps(outcome[1], sort_keys=True)
                 for outcome in outcomes}), 1)
        self.assertEqual(len(model.audit), 1)
        self.assertEqual(len(model.idem), 1)

    def test_concurrent_identical_ack_binds_once(self) -> None:
        model = self._new_model()
        self._seed_claim(model)
        first = self.stream[0]["position"]
        ops = [_Op("ack", "c1", "o1", 41, position=first, idem="a1")
               for _ in range(4)]
        model, outcomes = self._race_and_check(
            self.cons, model, ops, "identical-ack")
        self.assertTrue(all(outcome[0] == "ok" for outcome in outcomes))
        self.assertEqual(
            sum(1 for outcome in outcomes if outcome[2]), 1)
        self.assertEqual(
            len({json.dumps(outcome[1], sort_keys=True)
                 for outcome in outcomes}), 1)
        # One claim audit event plus exactly one ack audit event.
        self.assertEqual(len(model.audit), 2)
        self.assertEqual(model.states["c1"]["position"], first)

    def test_concurrent_identical_reject_dead_letters_once(self) -> None:
        model = self._new_model()
        self._seed_claim(model)
        first = self.stream[0]["position"]
        ops = [_Op("reject", "c1", "o1", 42, position=first,
                   reason="  坏数据 😀 ", idem="r1")
               for _ in range(4)]
        model, outcomes = self._race_and_check(
            self.cons, model, ops, "identical-reject")
        self.assertTrue(all(outcome[0] == "ok" for outcome in outcomes))
        self.assertEqual(
            sum(1 for outcome in outcomes if outcome[2]), 1)
        # The raced reject dead-letters the event exactly once.
        self.assertEqual(len(model.dead), 1)
        letter = model.dead[0]
        self.assertEqual(letter["position"], first)
        self.assertEqual(letter["reason"], "坏数据 😀")
        self.assertEqual(letter["event"], self.stream[0])

    def test_same_idem_conflicting_claims_bind_one(self) -> None:
        model = self._new_model()
        ops = [
            _Op("claim", "c1", "o1", 40, lease=100, idem="k1"),
            _Op("claim", "c1", "o1", 40, lease=200, idem="k1"),
            _Op("claim", "c1", "o1", 41, lease=100, idem="k1"),
        ]
        _model, outcomes = self._race_and_check(
            self.cons, model, ops, "conflicting-claims")
        # Only the first binding is legal; every divergent replay of
        # the same key is a ValueError.
        self.assertEqual(
            sum(1 for outcome in outcomes
                if outcome[0] == "ok" and outcome[2]), 1)
        self.assertEqual(
            sum(1 for outcome in outcomes
                if outcome == ("err", "value")), 2)

    def test_same_idem_across_operations_binds_one(self) -> None:
        model = self._new_model()
        self._seed_claim(model)
        first = self.stream[0]["position"]
        ops = [
            _Op("ack", "c1", "o1", 41, position=first, idem="s"),
            _Op("reject", "c1", "o1", 41, position=first, reason="x",
                idem="s"),
        ]
        _model, outcomes = self._race_and_check(
            self.cons, model, ops, "cross-operation-idem")
        # Whichever request binds the key first wins; the other
        # operation's divergent request under the same key is refused.
        self.assertEqual(
            sum(1 for outcome in outcomes
                if outcome[0] == "ok" and outcome[2]), 1)
        self.assertEqual(
            sum(1 for outcome in outcomes
                if outcome == ("err", "value")), 1)

    # -- ownership and lease -------------------------------------------------

    def test_owner_race_single_current_owner(self) -> None:
        model = self._new_model()
        ops = [
            _Op("claim", "c1", "o1", 40, lease=100, idem="k1"),
            _Op("claim", "c1", "o2", 40, lease=100, idem="k2"),
        ]
        model, outcomes = self._race_and_check(
            self.cons, model, ops, "owner-race")
        # Exactly one current valid owner emerges; the loser is refused
        # with PermissionError while the lease holds.
        winners = [outcome for outcome in outcomes if outcome[0] == "ok"]
        self.assertEqual(len(winners), 1)
        self.assertTrue(winners[0][2])
        self.assertEqual(
            sum(1 for outcome in outcomes
                if outcome == ("err", "permission")), 1)
        winner = winners[0][1]["owner"]
        self.assertEqual(model.states["c1"]["owner"], winner)
        status = mb.consumer_status(self.coord, self.cons, "c1", 100)
        self.assertEqual(status["owner"], winner)
        self.assertEqual(status["lease"], "active")

    def test_conflicting_subscription_race_binds_one(self) -> None:
        model = self._new_model()
        ops = [
            _Op("claim", "c1", "o1", 40, key="aaa", lease=100, idem="k1"),
            _Op("claim", "c1", "o1", 40, key="zzz", lease=100, idem="k2"),
        ]
        model, outcomes = self._race_and_check(
            self.cons, model, ops, "subscription-race")
        # The fixed subscription is bound by the first claim; the
        # conflicting one is a ValueError, never a rewrite.
        winners = [outcome for outcome in outcomes if outcome[0] == "ok"]
        self.assertEqual(len(winners), 1)
        self.assertEqual(
            sum(1 for outcome in outcomes
                if outcome == ("err", "value")), 1)
        self.assertEqual(model.subs["c1"],
                         {"key": winners[0][1]["key"], "job_id": None})

    def test_renewal_within_lease_blocks_foreign_claim(self) -> None:
        model = self._new_model()
        self._seed_claim(model)  # until 140
        ops = [
            _Op("claim", "c1", "o1", 140, lease=10, idem="k2"),
            _Op("claim", "c1", "o2", 140, lease=10, idem="k3"),
        ]
        model, outcomes = self._race_and_check(
            self.cons, model, ops, "renewal-vs-foreign")
        # The claim moment equal to the lease end still counts as valid:
        # the renewal extends the lease and the foreign claim is refused
        # in either interleaving.
        renewal, foreign = outcomes
        self.assertEqual(renewal, ("ok", {"consumer": "c1", "key": None,
                                          "job_id": None, "owner": "o1",
                                          "until": 150, "position": None,
                                          "taken_over": False}, False))
        self.assertEqual(foreign, ("err", "permission"))
        self.assertEqual(model.states["c1"]["until"], 150)
        self.assertEqual(model.states["c1"]["owner"], "o1")

    def test_no_takeover_before_strict_expiry(self) -> None:
        model = self._new_model()
        self._seed_claim(model)  # until 140
        before = Path(self.cons).read_bytes()
        ops = [
            _Op("claim", "c1", "o2", 140, lease=10, idem="k2"),
            _Op("claim", "c1", "o3", 140, lease=10, idem="k3"),
        ]
        _model, outcomes = self._race_and_check(
            self.cons, model, ops, "premature-takeover")
        # now == until is still the old owner's lease: no takeover.
        self.assertEqual(outcomes, [("err", "permission"),
                                    ("err", "permission")])
        self.assertEqual(Path(self.cons).read_bytes(), before)
        # Once strictly expired, even the former owner may not renew.
        outcome = self._apply_seq(
            self.cons, model,
            _Op("claim", "c1", "o1", 141, lease=10, idem="k4"))
        self.assertEqual(outcome, ("err", "timeout"))
        self.assertEqual(Path(self.cons).read_bytes(), before)

    def test_takeover_after_strict_expiry_preserves_cursor_and_letters(
            self) -> None:
        model = self._new_model()
        self._seed_claim(model)  # until 140
        first = self.stream[0]["position"]
        second = self.stream[1]["position"]
        self._apply_seq(self.cons, model,
                        _Op("ack", "c1", "o1", 41, position=first,
                            idem="a0"))
        self._apply_seq(self.cons, model,
                        _Op("reject", "c1", "o1", 42, position=second,
                            reason="x", idem="r1"))
        ops = [
            _Op("claim", "c1", "o2", 141, lease=50, idem="k2"),
            _Op("claim", "c1", "o3", 141, lease=50, idem="k3"),
        ]
        model, outcomes = self._race_and_check(
            self.cons, model, ops, "takeover-race")
        winners = [outcome for outcome in outcomes if outcome[0] == "ok"]
        self.assertEqual(len(winners), 1)
        self.assertEqual(
            sum(1 for outcome in outcomes
                if outcome == ("err", "permission")), 1)
        result = winners[0][1]
        # The takeover keeps the acknowledged position and reports the
        # takeover; it creates no new consumer.
        self.assertFalse(winners[0][2])
        self.assertTrue(result["taken_over"])
        self.assertEqual(result["position"], second)
        self.assertEqual(result["until"], 191)
        winner = result["owner"]
        self.assertIn(winner, ("o2", "o3"))
        # The dead letter survives the takeover.
        self.assertEqual(len(model.dead), 1)
        page = mb.consumer_dead_letters(self.coord, self.cons, "c1")
        self.assertEqual([entry["position"] for entry in page["entries"]],
                         [second])
        # The new owner continues from the preserved cursor; the old
        # owner is now the foreign one.
        page, changed = mb.consume(self.coord, self.cons, "pull", "c1",
                                   winner, 141, limit=1)
        self.assertFalse(changed)
        self.assertEqual(page["events"][0]["position"], second + 1)
        with self.assertRaises(PermissionError):
            mb.consume(self.coord, self.cons, "pull", "c1", "o1", 150)

    def test_expired_renewal_loses_to_takeover(self) -> None:
        model = self._new_model()
        self._seed_claim(model)  # until 140
        ops = [
            _Op("claim", "c1", "o1", 141, lease=10, idem="k2"),
            _Op("claim", "c1", "o2", 141, lease=50, idem="k3"),
        ]
        model, outcomes = self._race_and_check(
            self.cons, model, ops, "expired-renewal-vs-takeover")
        renewal, takeover = outcomes
        # The expired renewal never lands. If it is served first it is a
        # TimeoutError (its own lease strictly expired); if the takeover
        # commits first, the former owner is a foreign owner inside a
        # valid lease and gets PermissionError instead.
        self.assertIn(renewal, (("err", "timeout"), ("err", "permission")))
        self.assertEqual(takeover[0], "ok")
        self.assertTrue(takeover[1]["taken_over"])
        self.assertEqual(model.states["c1"]["owner"], "o2")

    # -- ack/reject races ------------------------------------------------------

    def test_ack_reject_race_on_earliest_pending_position(self) -> None:
        model = self._new_model()
        self._seed_claim(model)
        first = self.stream[0]["position"]
        ops = [
            _Op("ack", "c1", "o1", 41, position=first, idem="a0"),
            _Op("reject", "c1", "o1", 41, position=first, reason="x",
                idem="r0"),
        ]
        model, outcomes = self._race_and_check(
            self.cons, model, ops, "ack-reject-race")
        # No thread order is presumed, but the result must equal one of
        # the two legal serial orders: ack-then-conflicting-reject
        # (ValueError, no letter) or reject-then-in-place-ack (one
        # letter, no second write). The cursor lands on the position
        # either way and is never moved twice.
        self.assertEqual(
            sum(1 for outcome in outcomes
                if outcome[0] == "ok" and outcome[2]), 1)
        self.assertEqual(model.states["c1"]["position"], first)
        reject_won = any(outcome[0] == "ok" and outcome[2]
                         and "dead_letter" in outcome[1]
                         for outcome in outcomes)
        self.assertEqual(len(model.dead), 1 if reject_won else 0)
        if reject_won:
            self.assertEqual(model.dead[0]["position"], first)
            self.assertEqual(model.dead[0]["event"], self.stream[0])
        # The position is confirmed exactly once: no duplicate letter,
        # no contradictory record, and the next pull resumes after it.
        page, _ = mb.consume(self.coord, self.cons, "pull", "c1", "o1",
                             41, limit=1)
        self.assertEqual(page["events"][0]["position"], first + 1)

    def test_ack_race_never_regresses_cursor(self) -> None:
        model = self._new_model()
        self._seed_claim(model)
        low = self.stream[0]["position"]
        high = self.stream[1]["position"]
        ops = [
            _Op("ack", "c1", "o1", 41, position=low, idem="a0"),
            _Op("ack", "c1", "o1", 41, position=high, idem="a1"),
        ]
        model, outcomes = self._race_and_check(
            self.cons, model, ops, "ack-race")
        # In every legal serial order the cursor ends at the higher
        # position: either both acks land in order, or the higher one
        # lands first and the lower one is a backward ValueError.
        self.assertEqual(model.states["c1"]["position"], high)
        self.assertEqual(outcomes[1][0], "ok")
        self.assertTrue(outcomes[1][2])
        if outcomes[0][0] == "ok":
            self.assertTrue(outcomes[0][2])
        else:
            self.assertEqual(outcomes[0], ("err", "value"))

    def test_mixed_burst_is_linearizable(self) -> None:
        model = self._new_model()
        self._seed_claim(model)  # until 140
        first = self.stream[0]["position"]
        ops = [
            _Op("pull", "c1", "o1", 50, limit=1),
            _Op("ack", "c1", "o1", 51, position=first, idem="a0"),
            _Op("reject", "c1", "o1", 52, position=first, reason="x",
                idem="r0"),
            _Op("claim", "c1", "o1", 53, lease=200, idem="k2"),
        ]
        model, outcomes = self._race_and_check(
            self.cons, model, ops, "mixed-burst")
        # The pull observes either the pre- or the post-commit cursor;
        # exactly one of ack/reject moves the cursor onto the position
        # (the other is a ValueError or an in-place no-op); the renewal
        # always lands.
        self.assertEqual(outcomes[3][0], "ok")
        self.assertEqual(model.states["c1"]["until"], 253)
        self.assertEqual(model.states["c1"]["position"], first)
        self.assertLessEqual(len(model.dead), 1)

    # -- seeded mixed rounds ---------------------------------------------------

    def _other_owner(self, rng: random.Random, owner: str) -> str:
        return rng.choice([candidate for candidate in ("o1", "o2", "o3")
                           if candidate != owner])

    def _pick_consumer(self, rng: random.Random, model: _Model) -> str:
        existing = sorted(model.states)
        if existing and rng.random() < 0.8:
            return rng.choice(existing)
        return rng.choice(["c0", "c1", "c2", "c3"])

    def _rand_op(self, rng: random.Random, model: _Model, fresh_idem,
                 consumer: str | None = None) -> _Op:
        # Idempotent replay or divergent reuse of a bound key.
        if model.idem and rng.random() < 0.15:
            idem = rng.choice(sorted(model.idem))
            request = model.idem[idem]
            op = _Op.from_request(request, idem)
            if rng.random() < 0.5:
                if request["operation"] == "claim":
                    op.lease = request["lease"] + 1
                else:
                    op.now = request["now"] + 1
            return op
        if consumer is None:
            consumer = self._pick_consumer(rng, model)
        if consumer not in model.states:
            key, job_id = rng.choice([
                (None, None), ("aaa", None), ("zzz", None), ("mmm", None),
                (None, "j-1"), (None, "j-2"), ("aaa", "j-1")])
            return _Op("claim", consumer,
                       rng.choice(["o1", "o2", "o3"]), rng.randint(0, 200),
                       key=key, job_id=job_id, lease=rng.randint(5, 60),
                       idem=fresh_idem())
        state = model.states[consumer]
        sub = model.subs[consumer]
        kind = rng.choices(["claim", "ack", "reject", "pull"],
                           weights=[3, 4, 2, 2])[0]
        if kind == "claim":
            roll = rng.random()
            owner = state["owner"] if roll < 0.45 \
                else self._other_owner(rng, state["owner"])
            if roll < 0.45:
                # Around the lease boundary: renewal or expired renewal.
                now = rng.randint(max(0, state["until"] - 10),
                                  state["until"] + 3)
            elif roll < 0.8:
                # Strictly after the lease end: a legal takeover.
                now = state["until"] + rng.randint(1, 8)
            else:
                now = rng.randint(max(0, state["until"] - 5),
                                  state["until"] + 8)
            key, job_id = sub["key"], sub["job_id"]
            if rng.random() < 0.1:
                # A subscription mutation attempt: always ValueError.
                key = "zzz" if key != "zzz" else "aaa"
            return _Op("claim", consumer, owner, now, key=key,
                       job_id=job_id, lease=rng.randint(5, 60),
                       idem=fresh_idem())
        owner = state["owner"] if rng.random() < 0.8 \
            else self._other_owner(rng, state["owner"])
        if rng.random() < 0.7:
            now = rng.randint(max(0, state["until"] - 3), state["until"])
        else:
            now = rng.randint(0, state["until"] + 10)
        if kind == "pull":
            return _Op("pull", consumer, owner, now,
                       limit=rng.choice([1, 2, 3, 1000]))
        candidates = model.matching(sub, state["position"])
        roll = rng.random()
        if kind == "ack":
            if roll < 0.55 and candidates:
                position = rng.choice(candidates)["position"]
            elif roll < 0.7 and state["position"] is not None:
                position = state["position"]  # in-place ack: no write
            elif roll < 0.85 and state["position"]:
                position = state["position"] - 1  # backward
            else:
                position = model.tail + rng.randint(1, 3)  # past tail
            return _Op("ack", consumer, owner, now, position=position,
                       idem=fresh_idem())
        if roll < 0.55 and candidates:
            position = candidates[0]["position"]  # the one legal target
        elif roll < 0.7 and len(candidates) > 1:
            position = candidates[1]["position"]  # skips a predecessor
        elif roll < 0.85:
            non_matching = [event["position"] for event in self.stream
                            if not model.event_matches(event, sub)]
            position = rng.choice(non_matching) if non_matching \
                else model.tail + 1
        else:
            position = model.tail + rng.randint(1, 3)  # past tail
        reason = rng.choice(["bad", "poison pill", "  坏数据 😀 ", "x"])
        return _Op("reject", consumer, owner, now, position=position,
                   reason=reason, idem=fresh_idem())

    def _ensure_takeover(self, cons: str, model: _Model, fresh_idem,
                         trace: list[str]) -> None:
        if any(event["request"]["operation"] == "claim"
               and event["result"]["taken_over"] for event in model.audit):
            return
        consumer = min(model.states,
                       key=lambda name: model.states[name]["until"])
        state = model.states[consumer]
        sub = model.subs[consumer]
        owner = "o-takeover" if state["owner"] != "o-takeover" else "o-alt"
        now = state["until"] + 1
        outcome = self._apply_seq(
            cons, model,
            _Op("claim", consumer, owner, now, key=sub["key"],
                job_id=sub["job_id"], lease=30, idem=fresh_idem()),
            trace)
        self.assertEqual(outcome[0], "ok")
        self.assertTrue(outcome[1]["taken_over"])
        # The new owner continues consuming from the preserved cursor.
        self._apply_seq(cons, model,
                        _Op("pull", consumer, owner, now, limit=2), trace)

    def _run_seeded(self, seed: int) -> None:
        rng = random.Random(seed)
        cons = os.path.join(self.fx.tmp.name, f"consumers-{seed}.json")
        model = self._new_model()
        trace: list[str] = []
        counter = 0

        def fresh_idem() -> str:
            nonlocal counter
            counter += 1
            return f"s{seed}-i{counter}"

        try:
            # Round 0 opens with a subscription that matches no event,
            # so every full verification covers empty pages.
            self._apply_seq(
                cons, model,
                _Op("claim", "c-empty", "o-empty", 5,
                    key="no-such-batch", lease=400, idem=fresh_idem()),
                trace)
            for round_no in range(10):
                for _ in range(5):
                    self._apply_seq(
                        cons, model,
                        self._rand_op(rng, model, fresh_idem), trace)
                focus = self._pick_consumer(rng, model)
                burst = [self._rand_op(rng, model, fresh_idem,
                                       consumer=focus)
                         for _ in range(3)]
                outcomes = self._race(cons, burst)
                trace.append(
                    f"burst r{round_no}: " + " | ".join(
                        f"{op.describe()} -> {_short(outcome)}"
                        for op, outcome in zip(burst, outcomes)))
                explaining = self._explain(model, burst, outcomes)
                self.assertTrue(explaining,
                                f"burst round {round_no} is not "
                                f"linearizable")
                adopted = [candidate for _perm, candidate in explaining
                           if self._doc_matches(cons, candidate)]
                self.assertTrue(
                    adopted, f"burst round {round_no}: persisted state "
                             "matches no legal serial order")
                model = adopted[0]
                if round_no == 6:
                    self._ensure_takeover(cons, model, fresh_idem, trace)
                self._light_verify(cons, model)
            self._verify_round(cons, model)
        except AssertionError as exc:
            raise AssertionError(
                f"seed={seed} failed; operation trace:\n"
                + "\n".join(trace)) from exc

    def test_seeded_mixed_rounds(self) -> None:
        for seed in (3, 1415, 271828):
            with self.subTest(seed=seed):
                self._run_seeded(seed)


if __name__ == "__main__":
    unittest.main()
