"""Model-driven concurrency consistency tests for persistent consumers.

These tests exercise the persistent-consumer state machine through the
public library surface only -- ``migration_batch.consume`` (claim, pull,
ack, reject), ``migration_batch.consumer_status`` and
``migration_batch.consumer_dead_letters`` -- against one fixed event
stream holding several batches, several jobs and contiguous positions.

Every race is staged with ``threading.Barrier`` so all contenders are
released together; no test depends on wait durations, the network,
thread scheduling order or third-party services, and every wait is
bounded so a deadlock fails loudly instead of hanging. The judgment
baseline is an independent in-memory model of the documented state
machine (lease boundary with the equal-moment-still-valid rule, strict
expiry takeover, the immutable fixed subscription, the monotonic
cursor, the earliest-pending-event reject rule and idempotency-key
binding). A race's outcome is not presumed: the model enumerates every
legal serial order of the racing operations and the observed outcomes
must match exactly one of them, with an unambiguous final state.

After each race the state is re-read through the public queries and
dead-letter pages and checked against the model -- every observable
field, the page order and the ``next`` semantics -- and then again
after every materialized object has been dropped, so the answers are
proven to come from the persisted bytes at the same file path, never
from in-process state. Read-only queries are shown to leave the ledger
byte-for-byte untouched. Seeded rounds of mixed operations cover batch
and job filters, empty pages, page boundaries and continued consumption
after takeovers; a failure reports the seed and the exact operation
trace needed to reproduce it. No public interface, exception type,
normalization rule or production behavior is changed.
"""

from __future__ import annotations

import copy
import gc
import itertools
import json
import os
import random
import threading
import unittest
from pathlib import Path

from carbon_market import dispatch as dispatch_module
from carbon_market import jobs as jobs_module
from carbon_market import migration_batch as mb
from carbon_market.market import clear_live
from tests.test_migration_batch import MigrationBatchTest as _FixtureTest
from tests.test_migration_batch import _job

_WAIT = 15.0  # Bounded wait (seconds): a deadlock fails, never hangs.

_BUSINESS_ERRORS = (ValueError, KeyError, PermissionError, TimeoutError,
                    LookupError)


def _capture(fn):
    """Run ``fn`` and normalize its outcome to ("ok", value) or
    ("err", business-exception)."""
    try:
        return ("ok", fn())
    except _BUSINESS_ERRORS as exc:
        return ("err", exc)


class _ConsumerModel:
    """Independent in-memory model of one consumer ledger's state
    machine, driven only by the documented public contract and the
    fixed event stream read once through ``migration_batch.events``."""

    def __init__(self, events: list[dict]) -> None:
        self.events = copy.deepcopy(events)
        self.tail = events[-1]["position"] if events else None
        self.subs: dict[str, dict] = {}
        self.state: dict[str, dict] = {}
        self.idem: dict[str, tuple] = {}
        self.audit = 0
        self.dead: list[dict] = []
        self.upgraded = False

    # -- helpers ------------------------------------------------------------

    @staticmethod
    def _matches(event: dict, sub: dict) -> bool:
        return (sub["key"] is None or event["key"] == sub["key"]) \
            and (sub["job_id"] is None
                 or event["job_id"] == sub["job_id"])

    def _event_at(self, position: int) -> dict | None:
        for event in self.events:
            if event["position"] == position:
                return event
        return None

    def remaining(self, consumer: str) -> list[dict]:
        state = self.state[consumer]
        sub = self.subs[consumer]
        return [event for event in self.events
                if (state["position"] is None
                    or event["position"] > state["position"])
                and self._matches(event, sub)]

    def letters_for(self, consumer: str) -> list[dict]:
        return [record for record in self.dead
                if record["consumer"] == consumer]

    @staticmethod
    def _check_owner(state: dict, owner: str, now: int) -> None:
        if state["owner"] != owner:
            if now <= state["until"]:
                raise PermissionError("owned by another owner")
            raise TimeoutError("the previous lease has expired")
        if now > state["until"]:
            raise TimeoutError("the lease has expired")

    @staticmethod
    def _claim_result(consumer: str, sub: dict, state: dict,
                      taken_over: bool) -> dict:
        return {"consumer": consumer, "key": sub["key"],
                "job_id": sub["job_id"], "owner": state["owner"],
                "until": state["until"], "position": state["position"],
                "taken_over": taken_over}

    @staticmethod
    def _state_result(consumer: str, state: dict) -> dict:
        return {"consumer": consumer, "owner": state["owner"],
                "until": state["until"], "position": state["position"]}

    def _bind(self, idem: str, request: dict, result: dict) -> None:
        self.idem[idem] = (request, copy.deepcopy(result))
        self.audit += 1

    def _replay(self, idem: str, request: dict):
        bound = self.idem.get(idem)
        if bound is None:
            return None
        if bound[0] != request:
            raise ValueError("idempotency key bound to another request")
        return copy.deepcopy(bound[1]), False

    # -- write operations -----------------------------------------------------

    def claim(self, consumer, owner, now, lease, key, job_id, idem):
        request = {"operation": "claim", "consumer": consumer, "key": key,
                   "job_id": job_id, "owner": owner, "lease": lease,
                   "now": now}
        replayed = self._replay(idem, request)
        if replayed is not None:
            return replayed
        state = self.state.get(consumer)
        if state is None:
            sub = {"key": key, "job_id": job_id}
            state = {"owner": owner, "until": now + lease, "position": None}
            self.subs[consumer] = sub
            self.state[consumer] = state
            result = self._claim_result(consumer, sub, state, False)
            self._bind(idem, request, result)
            return result, True
        sub = self.subs[consumer]
        if sub != {"key": key, "job_id": job_id}:
            raise ValueError("the subscription is immutable")
        if state["owner"] == owner:
            # Renewal only while the lease is still valid; now == until
            # still counts, strict expiry refuses with TimeoutError.
            if now > state["until"]:
                raise TimeoutError("the lease has expired")
            state["until"] = now + lease
            result = self._claim_result(consumer, sub, state, False)
            self._bind(idem, request, result)
            return result, False
        if now <= state["until"]:
            raise PermissionError("owned by another owner")
        # Strict expiry: another owner takes over, keeping the cursor.
        state["owner"] = owner
        state["until"] = now + lease
        result = self._claim_result(consumer, sub, state, True)
        self._bind(idem, request, result)
        return result, False

    def ack(self, consumer, owner, now, position, idem):
        request = {"operation": "ack", "consumer": consumer,
                   "owner": owner, "position": position, "now": now}
        replayed = self._replay(idem, request)
        if replayed is not None:
            return replayed
        state = self.state.get(consumer)
        if state is None:
            raise KeyError(consumer)
        self._check_owner(state, owner, now)
        if state["position"] is not None and position < state["position"]:
            raise ValueError("the position cannot move backwards")
        if position == state["position"]:
            # An in-place ack binds nothing and audits nothing.
            return self._state_result(consumer, state), False
        if self.tail is None or position > self.tail:
            raise ValueError("the position is past the stream tail")
        event = self._event_at(position)
        if event is None or not self._matches(event, self.subs[consumer]):
            raise ValueError("the position is not a matching event")
        state["position"] = position
        result = self._state_result(consumer, state)
        self._bind(idem, request, result)
        return result, True

    def reject(self, consumer, owner, now, position, reason, idem):
        reason = reason.strip()
        request = {"operation": "reject", "consumer": consumer,
                   "owner": owner, "position": position, "reason": reason,
                   "now": now}
        replayed = self._replay(idem, request)
        if replayed is not None:
            return replayed
        state = self.state.get(consumer)
        if state is None:
            raise KeyError(consumer)
        self._check_owner(state, owner, now)
        if state["position"] is not None and position < state["position"]:
            raise ValueError("the position cannot move backwards")
        if position == state["position"]:
            raise ValueError("the position is already acknowledged")
        remaining = self.remaining(consumer)
        earliest = remaining[0] if remaining else None
        if self.tail is None or position > self.tail:
            raise ValueError("the position is past the stream tail")
        if earliest is None or position != earliest["position"]:
            raise ValueError("the position is not the earliest pending "
                             "matching event")
        letter = {"position": position, "event": copy.deepcopy(earliest),
                  "reason": reason, "rejected_at": now, "owner": owner}
        self.dead.append({"consumer": consumer, **copy.deepcopy(letter)})
        self.upgraded = True
        state["position"] = position
        result = {**self._state_result(consumer, state),
                  "dead_letter": letter}
        self._bind(idem, request, result)
        return result, True

    # -- read operations ------------------------------------------------------

    def pull(self, consumer, owner, now, limit):
        state = self.state.get(consumer)
        if state is None:
            raise KeyError(consumer)
        self._check_owner(state, owner, now)
        matched = self.remaining(consumer)
        page = matched[:limit]
        next_cursor = page[-1]["position"] if len(matched) > limit else None
        return {"consumer": consumer, "owner": state["owner"],
                "until": state["until"], "position": state["position"],
                "events": copy.deepcopy(page), "next": next_cursor}, False

    def status(self, consumer, now):
        state = self.state.get(consumer)
        if state is None:
            raise KeyError(consumer)
        sub = self.subs[consumer]
        backlog = self.remaining(consumer)
        return {"consumer": consumer, "key": sub["key"],
                "job_id": sub["job_id"], "owner": state["owner"],
                "until": state["until"],
                "lease": "active" if now <= state["until"] else "expired",
                "position": state["position"], "pending": len(backlog),
                "oldest": copy.deepcopy(backlog[0]) if backlog else None}

    def dead_page(self, consumer, cursor, limit):
        if consumer not in self.state:
            raise KeyError(consumer)
        letters = self.letters_for(consumer)
        matched = [record for record in letters
                   if cursor is None or record["position"] > cursor]
        page = matched[:limit]
        next_cursor = page[-1]["position"] if len(matched) > limit else None
        entries = [{"position": record["position"],
                    "event": copy.deepcopy(record["event"]),
                    "reason": record["reason"],
                    "rejected_at": record["rejected_at"],
                    "owner": record["owner"]} for record in page]
        return {"consumer": consumer, "entries": entries,
                "next": next_cursor}


class ConsumerConcurrencyTest(unittest.TestCase):
    """Shared fixture, race harness and model verification."""

    def setUp(self) -> None:
        self.fx = _FixtureTest(
            "test_get_returns_copy_and_unknown_key_raises")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self._build_stream()
        self.coord = self.fx.paths["coord"]
        self.cons = os.path.join(self.fx.tmp.name, "consumers.json")
        self.events = mb.events(self.coord, limit=1000)["events"]
        # Fixture sanity: one contiguous stream over several batches and
        # several jobs, with batch-scoped and item-scoped events.
        positions = [event["position"] for event in self.events]
        self.assertEqual(positions, list(range(len(positions))))
        self.assertGreaterEqual(len(positions), 12)
        self.assertEqual({event["key"] for event in self.events},
                         {"aaa", "zzz"})
        self.assertEqual({event["job_id"] for event in self.events},
                         {None, "j-1", "j-2"})

    # -- fixture --------------------------------------------------------------

    def _build_stream(self) -> None:
        fx = self.fx
        paths = fx.paths
        fx._seed_base()
        # A second job trades on the same resource so one batch drives
        # two migrating members and the stream carries two job ids.
        jobs_module.submit(paths["jobs"], _job("j-2"), "jk-2")
        fx._commit()
        clear_live(paths["jobs"], paths["supply"], paths["signals"],
                   paths["trades"], "j-2", "t-2", 10)
        dispatch_module.commit(paths["jobs"], paths["supply"],
                               paths["trades"], paths["dispatch"], "j-2",
                               "d-2", 10)
        fx._bootstrap_execution()
        fx._green()
        fx._run(key="aaa", owner="o1", now=30)
        fx._run(key="zzz", now=31)
        fx._run(key="aaa", owner="o1", now=32, receipts={
            "j-1": fx._receipt("copy", "succeeded", "复制完成", 33),
            "j-2": fx._receipt("copy", "succeeded", "j2 复制完成", 33)})
        fx._run(key="aaa", owner="o1", now=34, receipts={
            "j-1": fx._receipt("switch", "succeeded", "切换完成", 35),
            "j-2": fx._receipt("switch", "succeeded", "j2 切换完成", 35)})

    # -- operation constructors -------------------------------------------------

    @staticmethod
    def _claim_op(consumer, owner, now, lease, idem, key=None, job_id=None):
        return {"kind": "claim", "consumer": consumer, "owner": owner,
                "now": now, "lease": lease, "key": key, "job_id": job_id,
                "idem": idem}

    @staticmethod
    def _ack_op(consumer, owner, now, position, idem):
        return {"kind": "ack", "consumer": consumer, "owner": owner,
                "now": now, "position": position, "idem": idem}

    @staticmethod
    def _reject_op(consumer, owner, now, position, reason, idem):
        return {"kind": "reject", "consumer": consumer, "owner": owner,
                "now": now, "position": position, "reason": reason,
                "idem": idem}

    @staticmethod
    def _pull_op(consumer, owner, now, limit=1000):
        return {"kind": "pull", "consumer": consumer, "owner": owner,
                "now": now, "limit": limit}

    # -- dispatch -----------------------------------------------------------------

    def _actual(self, cons, op):
        kind = op["kind"]
        if kind == "claim":
            return mb.consume(self.coord, cons, "claim", op["consumer"],
                              op["owner"], op["now"], key=op["key"],
                              job_id=op["job_id"], lease=op["lease"],
                              idem=op["idem"])
        if kind == "pull":
            return mb.consume(self.coord, cons, "pull", op["consumer"],
                              op["owner"], op["now"], limit=op["limit"])
        if kind == "ack":
            return mb.consume(self.coord, cons, "ack", op["consumer"],
                              op["owner"], op["now"],
                              position=op["position"], idem=op["idem"])
        if kind == "reject":
            return mb.consume(self.coord, cons, "reject", op["consumer"],
                              op["owner"], op["now"],
                              position=op["position"], reason=op["reason"],
                              idem=op["idem"])
        if kind == "status":
            return mb.consumer_status(self.coord, cons, op["consumer"],
                                      op["now"])
        if kind == "dead":
            return mb.consumer_dead_letters(
                self.coord, cons, op["consumer"], cursor=op.get("cursor"),
                limit=op["limit"])
        raise AssertionError(f"unknown op kind {kind!r}")

    def _model_apply(self, model, op):
        kind = op["kind"]
        if kind == "claim":
            return model.claim(op["consumer"], op["owner"], op["now"],
                               op["lease"], op["key"], op["job_id"],
                               op["idem"])
        if kind == "pull":
            return model.pull(op["consumer"], op["owner"], op["now"],
                              op["limit"])
        if kind == "ack":
            return model.ack(op["consumer"], op["owner"], op["now"],
                             op["position"], op["idem"])
        if kind == "reject":
            return model.reject(op["consumer"], op["owner"], op["now"],
                                op["position"], op["reason"], op["idem"])
        if kind == "status":
            return model.status(op["consumer"], op["now"])
        if kind == "dead":
            return model.dead_page(op["consumer"], op.get("cursor"),
                                   op["limit"])
        raise AssertionError(f"unknown op kind {kind!r}")

    # -- outcome comparison -------------------------------------------------------

    def _outcome_matches(self, predicted, actual) -> bool:
        if predicted[0] != actual[0]:
            return False
        if predicted[0] == "ok":
            return predicted[1] == actual[1]
        # The documented public categories are the plain built-ins;
        # endpoint-facing subclasses stay instances of them.
        return isinstance(actual[1], type(predicted[1]))

    def _apply_single(self, cons, model, op):
        """Apply one operation to the model and the real ledger and
        require identical outcomes. A failed write operation must leave
        the ledger byte-for-byte untouched; a read-only operation must
        never write at all."""
        before = Path(cons).read_bytes() if Path(cons).exists() else None
        predicted = _capture(lambda: self._model_apply(model, op))
        actual = _capture(lambda: self._actual(cons, op))
        self.assertTrue(
            self._outcome_matches(predicted, actual),
            f"op {op}: model predicted {predicted!r} but the ledger "
            f"answered {actual!r}")
        after = Path(cons).read_bytes() if Path(cons).exists() else None
        if op["kind"] in ("pull", "status", "dead") or actual[0] == "err":
            self.assertEqual(after, before,
                             f"{op['kind']} changed the ledger bytes")
        return actual

    # -- race harness ---------------------------------------------------------------

    def _race(self, cons, ops):
        """Run the (label, op) pairs concurrently, all released by one
        barrier; the interleaving is decided by the ledger locks, never
        by sleeps. Returns {label: ("ok", value) | ("err", exc)}."""
        barrier = threading.Barrier(len(ops))
        outcomes: dict[str, tuple] = {}

        def target(label, op):
            try:
                barrier.wait(_WAIT)
                outcomes[label] = ("ok", self._actual(cons, op))
            except BaseException as exc:  # asserted in the main thread
                outcomes[label] = ("err", exc)

        threads = [threading.Thread(target=target, args=(label, op),
                                    name=label, daemon=True)
                   for label, op in ops]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(_WAIT)
        for thread in threads:
            self.assertFalse(thread.is_alive(),
                             f"{thread.name} is stuck (possible deadlock)")
        return outcomes

    @staticmethod
    def _fingerprint(model) -> str:
        return json.dumps(
            {"subs": model.subs, "state": model.state, "idem": model.idem,
             "audit": model.audit, "dead": model.dead,
             "upgraded": model.upgraded}, sort_keys=True)

    def _check_race(self, cons, model, ops, label):
        """Race the ops, then require the observed outcomes to equal the
        model's prediction for at least one serial order of the ops, and
        every matching serial order to reach the same final state. The
        model is advanced along the matching order."""
        outcomes = self._race(cons, ops)
        matches = []
        for perm in itertools.permutations(range(len(ops))):
            sim = copy.deepcopy(model)
            predicted = {}
            for index in perm:
                op = ops[index][1]
                predicted[index] = _capture(
                    lambda op=op: self._model_apply(sim, op))
            if all(self._outcome_matches(predicted[index],
                                         outcomes[ops[index][0]])
                   for index in range(len(ops))):
                matches.append(sim)
        self.assertTrue(
            matches,
            f"{label}: no legal serial order explains {outcomes!r}")
        fingerprints = {self._fingerprint(sim) for sim in matches}
        self.assertEqual(
            len(fingerprints), 1,
            f"{label}: ambiguous final state across legal serial orders")
        model.__dict__.update(matches[0].__dict__)
        return outcomes

    # -- model vs ledger verification --------------------------------------------

    def _verify_model(self, cons, model, now) -> None:
        # Second pass runs after every object the first pass
        # materialized was dropped: the answers can only come from the
        # persisted bytes at the same path, not from in-process state.
        for attempt in (1, 2):
            self._verify_model_pass(cons, model, now)
            if attempt == 1:
                gc.collect()

    def _verify_model_pass(self, cons, model, now) -> None:
        raw_before = Path(cons).read_bytes()
        doc = json.loads(raw_before)
        # Canonical normalization: compact UTF-8 JSON, keys in their
        # fixed order, exactly one trailing newline.
        self.assertEqual(
            raw_before.decode("utf-8"),
            json.dumps(doc, ensure_ascii=False, separators=(",", ":"))
            + "\n")
        # The persisted state sections equal the model's.
        self.assertEqual(set(doc["subscriptions"]), set(model.subs))
        self.assertEqual(set(doc["consumers"]), set(model.state))
        for consumer in model.state:
            self.assertEqual(doc["subscriptions"][consumer],
                             model.subs[consumer])
            self.assertEqual(doc["consumers"][consumer],
                             model.state[consumer])
        # Exactly one idempotency binding and one audit event per
        # first-served write.
        self.assertEqual(set(doc["idempotency"]), set(model.idem))
        self.assertEqual(len(doc["audit"]), model.audit)
        self.assertEqual([event["seq"] for event in doc["audit"]],
                         list(range(model.audit)))
        for key, (request, _result) in model.idem.items():
            self.assertEqual(doc["idempotency"][key], request)
        # The dead-letter section appears exactly once the first reject
        # upgraded the ledger, and holds the model's records in append
        # order.
        if model.upgraded:
            self.assertEqual(doc["dead_letters"], model.dead)
        else:
            self.assertNotIn("dead_letters", doc)
        # Every public query agrees with the model on every observable
        # field, the page order and the next semantics.
        for consumer, state in model.state.items():
            self.assertEqual(
                mb.consumer_subscription(self.coord, cons, consumer),
                model.subs[consumer])
            for moment, lease_state in ((now, None),
                                        (state["until"], "active"),
                                        (state["until"] + 1, "expired")):
                status = mb.consumer_status(self.coord, cons, consumer,
                                            moment)
                expected = model.status(consumer, moment)
                self.assertEqual(status, expected)
                if lease_state is not None:
                    self.assertEqual(status["lease"], lease_state)
            # Pulls page from the acknowledged position at the lease-end
            # boundary moment (still valid), in several page sizes.
            for limit in (1, 2, 1000):
                page, changed = mb.consume(
                    self.coord, cons, "pull", consumer, state["owner"],
                    state["until"], limit=limit)
                self.assertFalse(changed)
                self.assertEqual(
                    (page, changed),
                    model.pull(consumer, state["owner"], state["until"],
                               limit))
            # Dead-letter pages walk by the exclusive cursor; the final
            # page is empty with a null next once drained.
            cursor = None
            collected: list[dict] = []
            while True:
                page = mb.consumer_dead_letters(
                    self.coord, cons, consumer, cursor=cursor, limit=2)
                self.assertEqual(page, model.dead_page(consumer, cursor,
                                                       2))
                collected.extend(page["entries"])
                if page["next"] is None:
                    break
                cursor = page["next"]
            self.assertEqual(
                collected,
                model.dead_page(consumer, None, 1000)["entries"])
        # None of the read-only queries wrote a byte.
        self.assertEqual(Path(cons).read_bytes(), raw_before)


class ConsumerRaceTest(ConsumerConcurrencyTest):
    """Deterministic races over one shared consumer ledger."""

    def test_concurrent_identical_claim_single_audit(self) -> None:
        model = _ConsumerModel(self.events)
        ops = [(f"t{index}",
                self._claim_op("c1", "o1", 40, 100, "k1"))
               for index in range(4)]
        outcomes = self._check_race(self.cons, model, ops,
                                    "identical-claim")
        # Every contender observes the same business result; exactly one
        # is the first submission.
        results = [outcome[1] for outcome in outcomes.values()]
        self.assertTrue(all(outcome[0] == "ok"
                            for outcome in outcomes.values()))
        created = [created for _result, created in results]
        self.assertEqual(sorted(created), [False, False, False, True])
        self.assertTrue(all(result == results[0][0]
                            for result, _created in results))
        # One binding and one audit event in the ledger.
        self.assertEqual(model.audit, 1)
        self._verify_model(self.cons, model, 40)

    def test_concurrent_identical_reject_replays_once(self) -> None:
        model = _ConsumerModel(self.events)
        self._apply_single(self.cons, model,
                           self._claim_op("c1", "o1", 40, 100, "k1"))
        ops = [(f"t{index}",
                self._reject_op("c1", "o1", 41, 0, "  坏数据 😀  ", "r0"))
               for index in range(3)]
        outcomes = self._check_race(self.cons, model, ops,
                                    "identical-reject")
        results = [outcome[1] for outcome in outcomes.values()]
        self.assertTrue(all(outcome[0] == "ok"
                            for outcome in outcomes.values()))
        self.assertEqual(sorted(created for _r, created in results),
                         [False, False, True])
        self.assertTrue(all(result == results[0][0]
                            for result, _created in results))
        # One dead letter, one reject audit event, one binding.
        self.assertEqual(len(model.dead), 1)
        self.assertEqual(model.audit, 2)
        self.assertEqual(model.dead[0]["reason"], "坏数据 😀")
        self._verify_model(self.cons, model, 41)

    def test_conflicting_idem_single_binding(self) -> None:
        model = _ConsumerModel(self.events)
        # The same idempotency key carried by three different claims:
        # exactly one legal first binding, the rest ValueError.
        ops = [(f"t{index}",
                self._claim_op("c1", "o1", 40, lease, "k-conf"))
               for index, lease in enumerate((10, 20, 30))]
        outcomes = self._check_race(self.cons, model, ops,
                                    "conflicting-claim")
        winners = [label for label, outcome in outcomes.items()
                   if outcome[0] == "ok"]
        self.assertEqual(len(winners), 1)
        for label, outcome in outcomes.items():
            if label not in winners:
                self.assertIsInstance(outcome[1], ValueError)
        winner_op = dict(ops)[winners[0]]
        # The bound request replays the original result without writing;
        # every losing request under the same key is a ValueError.
        replay = self._apply_single(self.cons, model, winner_op)
        self.assertEqual(replay[1][1], False)
        for label, op in ops:
            if label not in winners:
                self._apply_single(self.cons, model, op)
        self.assertEqual(model.audit, 1)
        self._verify_model(self.cons, model, 40)

    def test_first_claim_owner_race(self) -> None:
        model = _ConsumerModel(self.events)
        ops = [("o1", self._claim_op("c1", "o1", 40, 100, "k-o1")),
               ("o2", self._claim_op("c1", "o2", 40, 100, "k-o2"))]
        outcomes = self._check_race(self.cons, model, ops, "owner-race")
        created = [label for label, outcome in outcomes.items()
                   if outcome[0] == "ok" and outcome[1][1]]
        refused = [label for label, outcome in outcomes.items()
                   if outcome[0] == "err"]
        # Exactly one current valid owner; the loser was refused.
        self.assertEqual(len(created), 1)
        self.assertEqual(len(refused), 1)
        self.assertIsInstance(outcomes[refused[0]][1], PermissionError)
        self.assertEqual(model.state["c1"]["owner"], created[0])
        self._verify_model(self.cons, model, 40)

    def test_foreign_owner_lease_boundary_and_takeover_race(self) -> None:
        model = _ConsumerModel(self.events)
        self._apply_single(self.cons, model,
                           self._claim_op("c1", "o1", 40, 100, "k1"))
        self._apply_single(self.cons, model,
                           self._ack_op("c1", "o1", 41, 0, "a0"))
        # The lease is valid through until == 140: foreign claims at 140
        # are still refused, never a takeover.
        ops = [("o2", self._claim_op("c1", "o2", 140, 10, "k-o2")),
               ("o3", self._claim_op("c1", "o3", 140, 10, "k-o3"))]
        outcomes = self._check_race(self.cons, model, ops,
                                    "foreign-at-boundary")
        self.assertTrue(all(outcome[0] == "err"
                            for outcome in outcomes.values()))
        self.assertTrue(all(isinstance(outcome[1], PermissionError)
                            for outcome in outcomes.values()))
        # Strictly after expiry exactly one foreign owner takes over and
        # keeps the acknowledged position.
        ops = [("o2", self._claim_op("c1", "o2", 141, 10, "k-o2b")),
               ("o3", self._claim_op("c1", "o3", 141, 10, "k-o3b"))]
        outcomes = self._check_race(self.cons, model, ops, "takeover")
        winners = [label for label, outcome in outcomes.items()
                   if outcome[0] == "ok"]
        self.assertEqual(len(winners), 1)
        result, created = outcomes[winners[0]][1]
        self.assertFalse(created)
        self.assertTrue(result["taken_over"])
        self.assertEqual(result["position"], 0)
        loser = next(label for label in outcomes if label not in winners)
        self.assertIsInstance(outcomes[loser][1], PermissionError)
        self.assertEqual(model.state["c1"]["position"], 0)
        self._verify_model(self.cons, model, 141)

    def test_renewal_within_lease_and_unauthorized_race(self) -> None:
        model = _ConsumerModel(self.events)
        self._apply_single(self.cons, model,
                           self._claim_op("c1", "o1", 40, 100, "k1"))
        # A renewal at the exact lease end races a foreign claim, pull
        # and ack: the renewal stands, every foreign operation is
        # refused, in any order.
        ops = [("renew", self._claim_op("c1", "o1", 140, 10, "k-renew")),
               ("f-claim", self._claim_op("c1", "o2", 140, 10, "k-f")),
               ("f-pull", self._pull_op("c1", "o2", 140)),
               ("f-ack", self._ack_op("c1", "o2", 140, 0, "k-fa"))]
        outcomes = self._check_race(self.cons, model, ops,
                                    "renew-vs-foreign")
        result, created = outcomes["renew"][1]
        self.assertFalse(created)
        self.assertFalse(result["taken_over"])
        self.assertEqual(result["until"], 150)
        for label in ("f-claim", "f-pull", "f-ack"):
            self.assertIsInstance(outcomes[label][1], PermissionError)
        # After strict expiry the former owner may not renew in place
        # and may not read; the failed renewal writes nothing.
        before = Path(self.cons).read_bytes()
        outcome = self._apply_single(
            self.cons, model, self._claim_op("c1", "o1", 151, 10, "k-late"))
        self.assertIsInstance(outcome[1], TimeoutError)
        self.assertEqual(Path(self.cons).read_bytes(), before)
        outcome = self._apply_single(self.cons, model,
                                     self._pull_op("c1", "o1", 151))
        self.assertIsInstance(outcome[1], TimeoutError)
        self._verify_model(self.cons, model, 151)

    def test_takeover_preserves_cursor_and_dead_letters(self) -> None:
        model = _ConsumerModel(self.events)
        self._apply_single(self.cons, model,
                           self._claim_op("c1", "o1", 40, 100, "k1"))
        self._apply_single(self.cons, model,
                           self._reject_op("c1", "o1", 41, 0, "坏数据", "r0"))
        self._apply_single(self.cons, model,
                           self._ack_op("c1", "o1", 42, 1, "a1"))
        # Not strictly expired yet: no takeover at now == until.
        outcome = self._apply_single(
            self.cons, model, self._claim_op("c1", "o2", 140, 10, "k-o2"))
        self.assertIsInstance(outcome[1], PermissionError)
        # Strict expiry: two new owners race, exactly one takes over.
        ops = [("o2", self._claim_op("c1", "o2", 141, 100, "k-o2b")),
               ("o3", self._claim_op("c1", "o3", 141, 100, "k-o3b"))]
        outcomes = self._check_race(self.cons, model, ops, "takeover")
        winners = [label for label, outcome in outcomes.items()
                   if outcome[0] == "ok"]
        self.assertEqual(len(winners), 1)
        owner = winners[0]
        # The cursor and the dead letter survive the takeover.
        self.assertEqual(model.state["c1"]["position"], 1)
        self.assertEqual(len(model.dead), 1)
        # The old owner is now the foreign one during the new lease:
        # refused, and the failed claim writes nothing.
        before = Path(self.cons).read_bytes()
        outcome = self._apply_single(
            self.cons, model, self._claim_op("c1", "o1", 141, 10, "k-o1b"))
        self.assertIsInstance(outcome[1], PermissionError)
        self.assertEqual(Path(self.cons).read_bytes(), before)
        # The new owner continues from the preserved cursor: the first
        # unacknowledged event redelivers, then a reject advances.
        page, _ = self._apply_single(
            self.cons, model, self._pull_op("c1", owner, 141, limit=1))[1]
        self.assertEqual([event["position"] for event in page["events"]],
                         [2])
        self._apply_single(self.cons, model,
                           self._reject_op("c1", owner, 142, 2, "再次拒绝",
                                           "r2"))
        self.assertEqual(model.state["c1"]["position"], 2)
        self.assertEqual(len(model.dead), 2)
        self._verify_model(self.cons, model, 142)

    def test_ack_reject_race_is_serializable(self) -> None:
        model = _ConsumerModel(self.events)
        self._apply_single(self.cons, model,
                           self._claim_op("c1", "o1", 40, 100, "k1"))
        # Ack and reject race on the same earliest pending position. The
        # two legal serial orders are: ack commits and the reject is
        # refused (already acknowledged), or the reject commits and the
        # ack lands in place without writing.
        ops = [("ack", self._ack_op("c1", "o1", 41, 0, "a-race")),
               ("reject", self._reject_op("c1", "o1", 41, 0, "x", "r-race"))]
        outcomes = self._check_race(self.cons, model, ops, "ack-reject")
        if outcomes["ack"][0] == "ok" and outcomes["ack"][1][1]:
            # ack won: the reject was refused, no dead letter exists.
            self.assertIsInstance(outcomes["reject"][1], ValueError)
            self.assertEqual(model.dead, [])
        else:
            # reject won: the ack landed in place, one letter recorded.
            self.assertEqual(outcomes["reject"][0], "ok")
            self.assertEqual(len(model.dead), 1)
            self.assertEqual(model.dead[0]["position"], 0)
        # Either way the cursor sits at the raced position and never
        # regressed, and no contradictory record exists at position 0.
        self.assertEqual(model.state["c1"]["position"], 0)
        self.assertLessEqual(len(model.dead), 1)
        # Consumption continues legally from whatever state won.
        self._apply_single(self.cons, model,
                           self._ack_op("c1", "o1", 42, 1, "a-next"))
        self._verify_model(self.cons, model, 42)

    def test_ack_ack_race_keeps_cursor_monotonic(self) -> None:
        model = _ConsumerModel(self.events)
        self._apply_single(self.cons, model,
                           self._claim_op("c1", "o1", 40, 100, "k1"))
        # Two acks at different positions race: the final cursor is the
        # higher one; the lower ack either committed first or is refused
        # as a backward move.
        ops = [("low", self._ack_op("c1", "o1", 41, 0, "a-low")),
               ("high", self._ack_op("c1", "o1", 41, 1, "a-high"))]
        outcomes = self._check_race(self.cons, model, ops, "ack-ack")
        self.assertEqual(outcomes["high"][0], "ok")
        if outcomes["low"][0] == "err":
            self.assertIsInstance(outcomes["low"][1], ValueError)
        self.assertEqual(model.state["c1"]["position"], 1)
        self._verify_model(self.cons, model, 41)

    def test_reject_reject_race_records_one_letter(self) -> None:
        model = _ConsumerModel(self.events)
        self._apply_single(self.cons, model,
                           self._claim_op("c1", "o1", 40, 100, "k1"))
        self._apply_single(self.cons, model,
                           self._ack_op("c1", "o1", 41, 0, "a0"))
        # The ledger predates dead letters until the first reject lands.
        doc = json.loads(Path(self.cons).read_text("utf-8"))
        self.assertNotIn("dead_letters", doc)
        # Two rejects for the same earliest pending position race:
        # exactly one dead letter is ever recorded.
        ops = [("r1", self._reject_op("c1", "o1", 42, 1, "第一次", "r-1")),
               ("r2", self._reject_op("c1", "o1", 43, 1, "第二次", "r-2"))]
        outcomes = self._check_race(self.cons, model, ops, "reject-reject")
        winners = [label for label, outcome in outcomes.items()
                   if outcome[0] == "ok"]
        self.assertEqual(len(winners), 1)
        loser = next(label for label in outcomes if label not in winners)
        self.assertIsInstance(outcomes[loser][1], ValueError)
        self.assertEqual(len(model.dead), 1)
        self.assertEqual(model.dead[0]["position"], 1)
        # The upgrade happened exactly once, in the winner's commit.
        doc = json.loads(Path(self.cons).read_text("utf-8"))
        self.assertEqual(len(doc["dead_letters"]), 1)
        self._verify_model(self.cons, model, 43)

    def test_pull_racing_ack_reads_one_consistent_snapshot(self) -> None:
        model = _ConsumerModel(self.events)
        self._apply_single(self.cons, model,
                           self._claim_op("c1", "o1", 40, 100, "k1"))
        # The pull must observe either the complete pre-ack snapshot or
        # the complete post-ack one -- never a torn state.
        ops = [("pull", self._pull_op("c1", "o1", 41, limit=1000)),
               ("ack", self._ack_op("c1", "o1", 41, 0, "a0"))]
        outcomes = self._check_race(self.cons, model, ops, "pull-ack")
        page, changed = outcomes["pull"][1]
        self.assertFalse(changed)
        if page["position"] is None:
            self.assertEqual(
                [event["position"] for event in page["events"]],
                list(range(len(self.events))))
        else:
            self.assertEqual(page["position"], 0)
            self.assertEqual(
                [event["position"] for event in page["events"]],
                list(range(1, len(self.events))))
        self._verify_model(self.cons, model, 41)

    def test_observable_field_orders(self) -> None:
        model = _ConsumerModel(self.events)
        _result, _created = self._apply_single(
            self.cons, model, self._claim_op("c1", "o1", 40, 100, "k1"))[1]
        result, _ = self._apply_single(
            self.cons, model,
            self._reject_op("c1", "o1", 41, 0, "坏", "r0"))[1]
        self.assertEqual(list(result),
                         ["consumer", "owner", "until", "position",
                          "dead_letter"])
        self.assertEqual(list(result["dead_letter"]),
                         ["position", "event", "reason", "rejected_at",
                          "owner"])
        result, _ = self._apply_single(
            self.cons, model, self._ack_op("c1", "o1", 42, 1, "a1"))[1]
        self.assertEqual(list(result),
                         ["consumer", "owner", "until", "position"])
        page, _ = self._apply_single(
            self.cons, model, self._pull_op("c1", "o1", 42, limit=1))[1]
        self.assertEqual(list(page),
                         ["consumer", "owner", "until", "position",
                          "events", "next"])
        status = self._apply_single(
            self.cons, model,
            {"kind": "status", "consumer": "c1", "now": 42})[1]
        self.assertEqual(list(status),
                         ["consumer", "key", "job_id", "owner", "until",
                          "lease", "position", "pending", "oldest"])
        letters = self._apply_single(
            self.cons, model,
            {"kind": "dead", "consumer": "c1", "limit": 100})[1]
        self.assertEqual(list(letters), ["consumer", "entries", "next"])
        self.assertEqual(list(letters["entries"][0]),
                         ["position", "event", "reason", "rejected_at",
                          "owner"])
        claim = self._apply_single(
            self.cons, model, self._claim_op("c2", "o1", 42, 100, "k2"))[1]
        self.assertEqual(list(claim[0]),
                         ["consumer", "key", "job_id", "owner", "until",
                          "position", "taken_over"])
        self._verify_model(self.cons, model, 42)


class ConsumerMixedRoundTest(ConsumerConcurrencyTest):
    """Seeded rounds of mixed operations checked against the model."""

    SUBSCRIPTIONS = {
        "c-all": (None, None),
        "c-aaa": ("aaa", None),
        "c-zzz": ("zzz", None),
        "c-j1": (None, "j-1"),
        "c-j2": (None, "j-2"),
        "c-aj1": ("aaa", "j-1"),
    }
    SEEDS = (3, 17, 29, 51, 88, 140)
    STEPS = 24

    def test_seeded_mixed_rounds(self) -> None:
        for seed in self.SEEDS:
            with self.subTest(seed=seed):
                self._run_seeded_round(seed)

    # -- one reproducible round -------------------------------------------------

    def _run_seeded_round(self, seed: int) -> None:
        cons = os.path.join(self.fx.tmp.name, f"consumers-{seed}.json")
        model = _ConsumerModel(self.events)
        rng = random.Random(seed)
        trace: list[str] = []
        now = 40
        counter = itertools.count()

        def fresh_idem(tag: str) -> str:
            return f"s{seed}-{next(counter)}-{tag}"

        def do(op: dict) -> None:
            trace.append(self._describe(op))
            self._apply_single(cons, model, op)

        def do_race(ops: list[tuple[str, dict]], label: str) -> None:
            trace.append(label + ": " + " | ".join(
                self._describe(op) for _name, op in ops))
            self._check_race(cons, model, ops, label)

        try:
            # Every consumer enters with its own owner and lease.
            for consumer, (key, job_id) in self.SUBSCRIPTIONS.items():
                do(self._claim_op(consumer, f"o-{consumer}", now,
                                  rng.randint(8, 20), fresh_idem("claim"),
                                  key=key, job_id=job_id))
            for step in range(self.STEPS):
                consumer = rng.choice(sorted(self.SUBSCRIPTIONS))
                state = model.state[consumer]
                owner = state["owner"]
                remaining = model.remaining(consumer)
                applicable = ["pull", "status", "ack_past_tail"]
                if remaining:
                    applicable += ["ack", "reject", "drain"]
                    if len(remaining) >= 2:
                        applicable.append("reject_skip")
                else:
                    applicable.append("reject_empty")
                if state["position"] is not None and state["position"] >= 1:
                    applicable.append("ack_backward")
                if model.letters_for(consumer):
                    applicable.append("dead_walk")
                if now <= state["until"]:
                    applicable += ["renew", "race_renew_foreign"]
                    if remaining:
                        applicable += ["race_dup_ack", "race_ack_reject"]
                        if now + 1 <= state["until"]:
                            applicable.append("race_conflict_ack")
                else:
                    applicable += ["takeover", "race_takeover",
                                   "renew_expired", "renew_expired",
                                   "foreign_expired"]
                kind = rng.choice(applicable)

                if kind == "pull":
                    do(self._pull_op(consumer, owner, now,
                                     limit=rng.choice([1, 2, 3, 1000])))
                elif kind == "status":
                    do({"kind": "status", "consumer": consumer, "now": now})
                elif kind == "ack":
                    target = rng.choice(
                        [event["position"] for event in remaining])
                    do(self._ack_op(consumer, owner, now, target,
                                    fresh_idem("ack")))
                elif kind == "reject":
                    do(self._reject_op(
                        consumer, owner, now,
                        remaining[0]["position"],
                        f"  原因 {seed}/{step} 😀  ", fresh_idem("rej")))
                elif kind == "reject_skip":
                    do(self._reject_op(consumer, owner, now,
                                       remaining[1]["position"], "skip",
                                       fresh_idem("skip")))
                elif kind == "reject_empty":
                    do(self._reject_op(consumer, owner, now,
                                       rng.randint(0, model.tail), "empty",
                                       fresh_idem("empty")))
                elif kind == "ack_backward":
                    do(self._ack_op(consumer, owner, now,
                                    rng.randint(0, state["position"] - 1),
                                    fresh_idem("back")))
                elif kind == "ack_past_tail":
                    do(self._ack_op(consumer, owner, now,
                                    model.tail + rng.randint(1, 9),
                                    fresh_idem("tail")))
                elif kind == "dead_walk":
                    cursor = None
                    limit = rng.choice([1, 2])
                    while True:
                        op = {"kind": "dead", "consumer": consumer,
                              "cursor": cursor, "limit": limit}
                        do(op)
                        cursor = model.dead_page(consumer, cursor,
                                                 limit)["next"]
                        if cursor is None:
                            break
                elif kind == "drain":
                    do(self._ack_op(consumer, owner, now,
                                    remaining[-1]["position"],
                                    fresh_idem("drain")))
                    # The drained consumer's next pull is an empty page.
                    do(self._pull_op(consumer, owner, now, limit=1000))
                elif kind == "renew":
                    do(self._claim_op(consumer, owner, now,
                                      rng.randint(5, 15),
                                      fresh_idem("renew"),
                                      key=model.subs[consumer]["key"],
                                      job_id=model.subs[consumer]
                                      ["job_id"]))
                elif kind == "race_renew_foreign":
                    foreign = f"x-{seed}-{step}"
                    do_race([
                        ("renew", self._claim_op(
                            consumer, owner, now, rng.randint(5, 15),
                            fresh_idem("renew"),
                            key=model.subs[consumer]["key"],
                            job_id=model.subs[consumer]["job_id"])),
                        ("f-claim", self._claim_op(
                            consumer, foreign, now, 5,
                            fresh_idem("fclaim"),
                            key=model.subs[consumer]["key"],
                            job_id=model.subs[consumer]["job_id"])),
                        ("f-pull", self._pull_op(consumer, foreign, now)),
                    ], f"renew-foreign-{step}")
                elif kind == "takeover":
                    do(self._claim_op(consumer, f"o-{consumer}-{step}",
                                      now, rng.randint(8, 20),
                                      fresh_idem("take"),
                                      key=model.subs[consumer]["key"],
                                      job_id=model.subs[consumer]
                                      ["job_id"]))
                elif kind == "race_takeover":
                    do_race([
                        ("t1", self._claim_op(
                            consumer, f"o-{consumer}-{step}-a", now,
                            rng.randint(8, 20), fresh_idem("take1"),
                            key=model.subs[consumer]["key"],
                            job_id=model.subs[consumer]["job_id"])),
                        ("t2", self._claim_op(
                            consumer, f"o-{consumer}-{step}-b", now,
                            rng.randint(8, 20), fresh_idem("take2"),
                            key=model.subs[consumer]["key"],
                            job_id=model.subs[consumer]["job_id"])),
                    ], f"takeover-{step}")
                elif kind == "renew_expired":
                    do(self._claim_op(consumer, owner, now, 5,
                                      fresh_idem("late"),
                                      key=model.subs[consumer]["key"],
                                      job_id=model.subs[consumer]
                                      ["job_id"]))
                elif kind == "foreign_expired":
                    foreign = f"x-{seed}-{step}"
                    if rng.random() < 0.5:
                        do(self._pull_op(consumer, foreign, now))
                    else:
                        do(self._ack_op(consumer, foreign, now,
                                        state["position"] or 0,
                                        fresh_idem("fack")))
                elif kind == "race_dup_ack":
                    op = self._ack_op(consumer, owner, now,
                                      remaining[0]["position"],
                                      fresh_idem("dup"))
                    do_race([("d1", dict(op)), ("d2", dict(op))],
                            f"dup-ack-{step}")
                elif kind == "race_conflict_ack":
                    first = self._ack_op(consumer, owner, now,
                                         remaining[0]["position"],
                                         fresh_idem("conf"))
                    second = dict(first, now=now + 1)
                    do_race([("c1", first), ("c2", second)],
                            f"conf-ack-{step}")
                elif kind == "race_ack_reject":
                    do_race([
                        ("ack", self._ack_op(consumer, owner, now,
                                             remaining[0]["position"],
                                             fresh_idem("rack"))),
                        ("reject", self._reject_op(
                            consumer, owner, now,
                            remaining[0]["position"], "竞争",
                            fresh_idem("rrej"))),
                    ], f"ack-reject-{step}")
                else:  # pragma: no cover - generator bug guard
                    raise AssertionError(f"unhandled step kind {kind}")
                # Time passes; leases eventually expire.
                if rng.random() < 0.3:
                    now += rng.randint(1, 6)
            self._verify_model(cons, model, now)
        except BaseException as exc:
            raise AssertionError(
                f"seed {seed} failed after {len(trace)} operations: "
                f"{exc!r}\noperation trace (reproduces with the seed):\n"
                + "\n".join(trace)) from exc

    @staticmethod
    def _describe(op: dict) -> str:
        parts = [op["kind"], op["consumer"]]
        for field in ("owner", "now", "lease", "position", "limit",
                      "cursor", "key", "job_id", "reason", "idem"):
            if field in op:
                parts.append(f"{field}={op[field]!r}")
        return " ".join(parts)


if __name__ == "__main__":
    unittest.main()
