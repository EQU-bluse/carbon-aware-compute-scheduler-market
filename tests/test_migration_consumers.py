"""Library tests for migration_batch.consume: persistent consumers.

Covers the fixed claim/pull/ack subscription ledger: first claim and
idempotent replay, the immutable (key, job_id) subscription, pulls
paged strictly after the acknowledged position without advancing it,
forward/in-place/backward/past-tail/non-matching acks, ownership and
strict lease expiry (PermissionError vs TimeoutError), the same-owner
renewal boundary (valid through until, TimeoutError after strict
expiry with the ledger untouched), expired-lease takeover with
redelivery, the read-only consumer_status status/backlog query, the
LookupError when a confirmed event is truncated, rewritten or
position-reused, canonical ledger bytes, the coordination binding and
the FileNotFoundError/ValueError/OSError surface.
"""

from __future__ import annotations

import json
import os
import unittest
from pathlib import Path

from carbon_market import migration_batch as mb
from tests.test_migration_batch import MigrationBatchTest


class ConsumeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fx = MigrationBatchTest(
            "test_get_returns_copy_and_unknown_key_raises")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.fx._prepare_migrate()
        self.coord = self.fx.paths["coord"]
        self.cons = os.path.join(self.fx.tmp.name, "consumers.json")
        # Every claim requires an existing canonical coordination ledger,
        # so the populated two-batch stream is the common starting state.
        self.total = self._populate()

    # -- fixture --------------------------------------------------------------

    def _populate(self) -> int:
        fx = self.fx
        fx._run(key="aaa", owner="o1", now=30)
        fx._run(key="zzz", now=31)
        fx._run(key="aaa", owner="o1", now=32,
                receipts={"j-1": fx._receipt(
                    "copy", "succeeded", "复制完成", 33)})
        fx._run(key="aaa", owner="o1", now=34,
                receipts={"j-1": fx._receipt(
                    "switch", "succeeded", "切换完成", 35)})
        return len(mb.events(self.coord, limit=1000)["events"])

    def _claim(self, consumer="c1", owner="o1", now=40, lease=100,
               idem="k1", **filters):
        return mb.consume(self.coord, self.cons, "claim", consumer, owner,
                          now, lease=lease, idem=idem, **filters)

    def _pull(self, consumer="c1", owner="o1", now=40, **kwargs):
        return mb.consume(self.coord, self.cons, "pull", consumer, owner,
                          now, **kwargs)

    def _ack(self, position, consumer="c1", owner="o1", now=40, idem=None):
        return mb.consume(self.coord, self.cons, "ack", consumer, owner,
                          now, position=position, idem=idem)

    def _reject(self, position, reason="bad", consumer="c1", owner="o1",
                now=40, idem=None):
        return mb.consume(self.coord, self.cons, "reject", consumer, owner,
                          now, position=position, reason=reason, idem=idem)

    def _dead_letters(self, consumer="c1", cursor=None, limit=100):
        return mb.consumer_dead_letters(
            self.coord, self.cons, consumer, cursor, limit)

    # -- argument validation --------------------------------------------------

    def test_bad_arguments_raise_value_error(self) -> None:
        kwargs = dict(lease=1, idem="i")
        for bad in (
            lambda: mb.consume("", self.cons, "claim", "c", "o", 1, **kwargs),
            lambda: mb.consume(self.coord, "", "claim", "c", "o", 1, **kwargs),
            lambda: mb.consume(self.coord, self.cons, "nope", "c", "o", 1,
                               **kwargs),
            lambda: mb.consume(self.coord, self.cons, "claim", "", "o", 1,
                               **kwargs),
            lambda: mb.consume(self.coord, self.cons, "claim", "c", "", 1,
                               **kwargs),
            lambda: mb.consume(self.coord, self.cons, "claim", "c", "o", -1,
                               **kwargs),
            lambda: mb.consume(self.coord, self.cons, "claim", "c", "o", 1,
                               lease=0, idem="i"),
            lambda: mb.consume(self.coord, self.cons, "claim", "c", "o", 1,
                               lease=True, idem="i"),  # boolean
            lambda: mb.consume(self.coord, self.cons, "claim", "c", "o", 1,
                               lease=1, idem=""),
            lambda: mb.consume(self.coord, self.cons, "claim", "c", "o", 1,
                               lease=1, idem="i", key=""),
            lambda: mb.consume(self.coord, self.cons, "claim", "c", "o", 1,
                               lease=1, idem="i", job_id=""),
            lambda: mb.consume(self.coord, self.cons, "pull", "c", "o", 1,
                               idem="i"),
            lambda: mb.consume(self.coord, self.cons, "pull", "c", "o", 1,
                               limit=0),
            lambda: mb.consume(self.coord, self.cons, "pull", "c", "o", 1,
                               limit=1001),
            lambda: mb.consume(self.coord, self.cons, "ack", "c", "o", 1,
                               position=-1, idem="i"),
        ):
            with self.subTest(bad=bad):
                self.assertRaises(ValueError, bad)

    def test_ledger_paths_must_be_distinct(self) -> None:
        with self.assertRaises(ValueError):
            mb.consume(self.coord, self.coord, "claim", "c", "o", 1,
                       lease=1, idem="i")

    # -- claim ----------------------------------------------------------------

    def test_first_claim_persists_subscription_and_null_position(self) -> None:
        result, created = self._claim()
        self.assertTrue(created)
        self.assertEqual(result, {
            "consumer": "c1", "key": None, "job_id": None, "owner": "o1",
            "until": 140, "position": None, "taken_over": False})
        doc = json.loads(Path(self.cons).read_text("utf-8"))
        self.assertEqual(doc["subscriptions"], {"c1": {"key": None,
                                                       "job_id": None}})
        self.assertEqual(doc["consumers"]["c1"]["position"], None)
        self.assertEqual(doc["coordination"], os.path.realpath(self.coord))

    def test_claim_with_fixed_key_and_job(self) -> None:
        result, created = self._claim(
            consumer="cj", idem="k2", key="aaa", job_id="j-1")
        self.assertTrue(created)
        self.assertEqual(result["key"], "aaa")
        self.assertEqual(result["job_id"], "j-1")

    def test_equivalent_claim_replays_original_without_writing(self) -> None:
        first, _ = self._claim()
        before = Path(self.cons).read_bytes()
        replay, created = self._claim()
        self.assertFalse(created)
        self.assertEqual(replay, first)
        self.assertEqual(Path(self.cons).read_bytes(), before)

    def test_repeat_claim_by_same_owner_renews_lease(self) -> None:
        self._claim(now=40)
        result, created = self._claim(now=60, idem="k1b")
        self.assertFalse(created)
        self.assertEqual(result["until"], 160)
        self.assertFalse(result["taken_over"])

    def test_subscription_cannot_change(self) -> None:
        self._claim()
        with self.assertRaises(ValueError):
            self._claim(idem="k2", key="aaa")
        with self.assertRaises(ValueError):
            self._claim(idem="k3", job_id="j-1")

    def test_claim_idempotency_key_changed_request_raises(self) -> None:
        self._claim(lease=100)
        with self.assertRaises(ValueError):
            self._claim(lease=50)

    def test_foreign_owner_refused_during_valid_lease(self) -> None:
        self._claim()
        with self.assertRaises(PermissionError):
            self._claim(owner="o2", now=100, idem="k2")

    def test_foreign_owner_takes_over_after_strict_expiry(self) -> None:
        self._claim()
        self._ack(0, idem="a0")
        # now == until is still refused; only strictly greater takes over.
        with self.assertRaises(PermissionError):
            self._claim(owner="o2", now=140, idem="kx")
        result, created = self._claim(owner="o2", now=141, lease=10,
                                      idem="k2")
        self.assertFalse(created)
        self.assertTrue(result["taken_over"])
        self.assertEqual(result["owner"], "o2")
        self.assertEqual(result["until"], 151)
        # The acknowledged position survives the takeover.
        self.assertEqual(result["position"], 0)

    # -- pull -----------------------------------------------------------------

    def test_pull_pages_after_position_without_advancing(self) -> None:
        total = self.total
        self._claim()
        page, changed = self._pull(limit=2)
        self.assertFalse(changed)
        self.assertEqual([e["position"] for e in page["events"]], [0, 1])
        self.assertEqual(page["next"], 1)
        self.assertIsNone(page["position"])
        # A second identical pull returns the same page: no checkpoint.
        again, _ = self._pull(limit=2)
        self.assertEqual([e["position"] for e in again["events"]], [0, 1])
        # A pull always resumes at the unchanged checkpoint, so a
        # larger limit re-reads every unacknowledged event from 0;
        # continuation across pages is driven by acking, never by a
        # client cursor.
        rest, _ = self._pull(limit=1000)
        self.assertEqual([e["position"] for e in rest["events"]],
                         list(range(total)))

    def test_pull_respects_fixed_filters(self) -> None:
        self._claim(consumer="ck", idem="kb", key="zzz")
        page, _ = self._pull(consumer="ck", limit=10)
        self.assertTrue(page["events"])
        self.assertTrue(all(e["key"] == "zzz") for e in page["events"])
        self._claim(consumer="cj", idem="kj", job_id="j-1")
        page, _ = self._pull(consumer="cj", limit=10)
        self.assertTrue(all(e["job_id"] == "j-1") for e in page["events"])
        self.assertIsNone(page["next"])

    def test_pull_requires_ownership_and_valid_lease(self) -> None:
        self._claim()
        with self.assertRaises(PermissionError):
            self._pull(owner="o2", now=40)
        with self.assertRaises(TimeoutError):
            self._pull(owner="o1", now=141)

    def test_pull_unknown_consumer_is_key_error(self) -> None:
        self._claim()
        with self.assertRaises(KeyError):
            self._pull(consumer="ghost")

    # -- ack ------------------------------------------------------------------

    def test_ack_advances_only_to_a_real_matching_event(self) -> None:
        self._claim(consumer="ck", idem="kb", key="zzz")
        # Position 0 is an "aaa" event: real, but not this subscription.
        with self.assertRaises(ValueError):
            self._ack(0, consumer="ck", idem="a0")

    def test_ack_in_place_writes_nothing(self) -> None:
        self._claim()
        first, changed = self._ack(0, idem="a0")
        self.assertTrue(changed)
        self.assertEqual(first["position"], 0)
        before = Path(self.cons).read_bytes()
        # A fresh idempotency key at the same position is still a no-op.
        same, changed = self._ack(0, idem="a0-repeat")
        self.assertFalse(changed)
        self.assertEqual(same["position"], 0)
        self.assertEqual(Path(self.cons).read_bytes(), before)

    def test_ack_idempotent_replay_returns_original(self) -> None:
        self._claim()
        first, _ = self._ack(0, idem="a0")
        replay, changed = self._ack(0, idem="a0")
        self.assertFalse(changed)
        self.assertEqual(replay, first)

    def test_ack_backward_and_past_tail_raise_value_error(self) -> None:
        self._claim()
        self._ack(1, idem="a1")
        with self.assertRaises(ValueError):
            self._ack(0, idem="a2")
        with self.assertRaises(ValueError):
            self._ack(999, idem="a3")

    def test_ack_ownership_and_lease(self) -> None:
        self._claim()
        with self.assertRaises(PermissionError):
            self._ack(0, owner="o2", now=40, idem="a0")
        with self.assertRaises(TimeoutError):
            self._ack(0, owner="o1", now=141, idem="a0")

    def test_takeover_keeps_cursor_and_redelivers(self) -> None:
        total = self.total
        self._claim()
        self._ack(1, idem="a1")
        self._claim(owner="o2", now=141, lease=100, idem="k2")
        page, _ = self._pull(owner="o2", now=141, limit=1000)
        self.assertEqual([e["position"] for e in page["events"]],
                         list(range(2, total)))

    def test_confirmed_cursor_truncated_raises_lookup_error(self) -> None:
        # Confirm a late position against one stream, then rebind the
        # consumer ledger to a second, shorter canonical coordination
        # ledger at a different real path: the confirmed event is gone,
        # so the cursor must raise LookupError, never auto-reset.
        total = self.total
        self.assertGreaterEqual(total, 4)
        self._claim()
        self._ack(total - 1, idem="a-last")

        other = MigrationBatchTest(
            "test_get_returns_copy_and_unknown_key_raises")
        other.setUp()
        self.addCleanup(other.doCleanups)
        other._prepare_migrate()
        other._run(key="aaa", owner="o1", now=30)
        other_cons = os.path.join(other.tmp.name, "consumers.json")
        doc = json.loads(Path(self.cons).read_text("utf-8"))
        doc["coordination"] = os.path.realpath(other.paths["coord"])
        Path(other_cons).write_text(
            json.dumps(doc, ensure_ascii=False,
                       separators=(",", ":")) + "\n",
            encoding="utf-8")
        with self.assertRaises(LookupError):
            mb.consume(other.paths["coord"], other_cons, "pull", "c1",
                       "o1", 40)
        with self.assertRaises(LookupError):
            mb.consume(other.paths["coord"], other_cons, "ack", "c1",
                       "o1", 40, position=total - 1, idem="a-again")

    # -- ledger files ---------------------------------------------------------

    def test_consumer_ledger_is_canonical(self) -> None:
        self._claim()
        self._ack(0, idem="a0")
        raw = Path(self.cons).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        self.assertNotIn(b"\\u", raw)
        doc = json.loads(raw)
        self.assertEqual(list(doc),
                         ["version", "coordination", "subscriptions",
                          "consumers", "idempotency", "audit"])
        self.assertEqual(list(doc["subscriptions"]["c1"]),
                         ["key", "job_id"])
        self.assertEqual(list(doc["consumers"]["c1"]),
                         ["owner", "until", "position"])
        # One idempotency binding and one audit event per write.
        self.assertEqual(len(doc["idempotency"]), len(doc["audit"]))

    def test_non_canonical_consumer_ledger_raises_value_error(self) -> None:
        self._claim()
        good = Path(self.cons).read_bytes()
        for payload in (b"{broken\n", good + b"\n",
                        good.replace(b'"version":1', b'"version":2', 1)):
            Path(self.cons).write_bytes(payload)
            with self.subTest(payload=payload[:10]):
                with self.assertRaises(ValueError):
                    self._pull()
        Path(self.cons).write_bytes(good)

    def test_consumer_ledger_bound_to_another_coord_raises(self) -> None:
        self._claim()
        # The other coordination ledger must exist and be canonical for
        # the request to reach the binding check; a byte-for-byte copy at
        # a different real path is bound to the consumer ledger's path.
        other_coord = os.path.join(self.fx.tmp.name, "other.json")
        Path(other_coord).write_bytes(Path(self.coord).read_bytes())
        with self.assertRaises(ValueError):
            mb.consume(other_coord, self.cons, "pull", "c1", "o1", 40)

    def test_missing_parent_raises_file_not_found(self) -> None:
        missing = os.path.join(self.fx.tmp.name, "no-dir", "c.json")
        with self.assertRaises(FileNotFoundError):
            mb.consume(self.coord, missing, "claim", "c", "o", 1,
                       lease=1, idem="i")

    def test_missing_coordination_raises_file_not_found(self) -> None:
        absent = os.path.join(self.fx.tmp.name, "absent.json")
        with self.assertRaises(FileNotFoundError):
            mb.consume(absent, self.cons, "claim", "c", "o", 1,
                       lease=1, idem="i")

    def test_pull_never_creates_ledger(self) -> None:
        with self.assertRaises(KeyError):
            self._pull()
        self.assertFalse(Path(self.cons).exists())

    def test_response_serializer_matches_object(self) -> None:
        total = self.total
        self._claim()
        result, _ = self._pull(limit=total)
        body = mb.consume_response(self.coord, self.cons, "pull", "c1",
                                   "o1", 40, limit=total)
        self.assertEqual(json.loads(body), result)
        self.assertFalse(body.endswith(b"\n"))

    # -- renewal boundary -----------------------------------------------------

    def test_same_owner_renewal_at_lease_end_is_still_active(self) -> None:
        self._claim(now=40, lease=100)  # until 140
        result, created = self._claim(now=140, lease=10, idem="k1b")
        self.assertFalse(created)
        self.assertFalse(result["taken_over"])
        self.assertEqual(result["until"], 150)

    def test_same_owner_renewal_after_strict_expiry_times_out(self) -> None:
        self._claim(now=40, lease=100)  # until 140
        before = Path(self.cons).read_bytes()
        with self.assertRaises(TimeoutError):
            self._claim(now=141, lease=10, idem="k1b")
        # A timed-out renewal leaves the ledger byte-for-byte untouched.
        self.assertEqual(Path(self.cons).read_bytes(), before)
        # A different owner may still take over after strict expiry.
        result, created = self._claim(owner="o2", now=141, lease=10,
                                      idem="k2")
        self.assertFalse(created)
        self.assertTrue(result["taken_over"])
        self.assertEqual(result["owner"], "o2")

    # -- reject ---------------------------------------------------------------

    def test_reject_parks_event_and_advances_cursor(self) -> None:
        total = self.total
        self._claim()
        page, _ = self._pull(limit=1)
        event0 = page["events"][0]
        result, created = self._reject(0, reason="  拒绝理由 \t\n", idem="r0")
        self.assertTrue(created)
        self.assertEqual(list(result),
                         ["consumer", "owner", "until", "position",
                          "dead_letter"])
        self.assertEqual(result["consumer"], "c1")
        self.assertEqual(result["owner"], "o1")
        self.assertEqual(result["until"], 140)
        self.assertEqual(result["position"], 0)
        letter = result["dead_letter"]
        self.assertEqual(list(letter),
                         ["position", "event", "reason", "rejected_at",
                          "owner"])
        self.assertEqual(letter["position"], 0)
        self.assertEqual(letter["event"], event0)
        self.assertEqual(letter["reason"], "拒绝理由")
        self.assertEqual(letter["rejected_at"], 40)
        self.assertEqual(letter["owner"], "o1")
        # The cursor moved, so the next pull starts at 1 and position 0
        # never redelivers.
        page, _ = self._pull(limit=10)
        self.assertEqual([e["position"] for e in page["events"]],
                         list(range(1, min(total, 11))))
        status = self._status()
        self.assertEqual(status["position"], 0)
        self.assertEqual(status["pending"], total - 1)

    def test_reject_reason_validation(self) -> None:
        self._claim()
        for reason in (1, True, None, ["x"], "", "   ", "\t\n ",
                       "x" * 513, "あ" * 513):
            with self.subTest(reason=repr(reason)[:20]):
                with self.assertRaises(ValueError):
                    mb.consume(self.coord, self.cons, "reject", "c1", "o1",
                               40, position=0, reason=reason, idem="rx")
        # 512 code points after trimming are accepted; the stripped text
        # is stored.
        result, _ = self._reject(0, reason=" " + "あ" * 512 + "\n", idem="r0")
        self.assertEqual(result["dead_letter"]["reason"], "あ" * 512)

    def test_reject_must_name_earliest_pending_match(self) -> None:
        self._claim()
        # Skipping the earliest pending event (position 0) to reject a
        # later one is invalid.
        with self.assertRaises(ValueError):
            self._reject(1, idem="r1")
        with self.assertRaises(ValueError):
            self._reject(2, idem="r2")
        self._reject(0, idem="r0")
        # Past the tail is invalid too.
        with self.assertRaises(ValueError):
            self._reject(self.total + 5, idem="rend")
        # Backward and in-place rejects (position at or before the
        # cursor) are invalid; there is no in-place reject.
        with self.assertRaises(ValueError):
            self._reject(0, idem="r0b")

    def test_reject_under_fixed_subscription_skips_non_matches(self) -> None:
        # A zzz-only subscription: position 0 is an aaa event. The
        # earliest *matching* position is what reject must name; a
        # non-matching position is invalid, and rejecting a later match
        # while an earlier match is pending is invalid too.
        self._claim(consumer="ck", idem="kb", key="zzz")
        stream = mb.events(self.coord, limit=1000)["events"]
        zzz = [e["position"] for e in stream if e["key"] == "zzz"]
        self.assertTrue(zzz)
        first, second = zzz[0], zzz[1] if len(zzz) > 1 else None
        with self.assertRaises(ValueError):
            self._reject(0, consumer="ck", idem="r0")
        if second is not None:
            with self.assertRaises(ValueError):
                self._reject(second, consumer="ck", idem="r2")
        result, _ = self._reject(first, consumer="ck", idem="r1")
        self.assertEqual(result["position"], first)
        self.assertEqual(result["dead_letter"]["event"]["key"], "zzz")
        # The rejected zzz event no longer pulls; aaa events never did.
        page, _ = self._pull(consumer="ck", limit=1000)
        self.assertNotIn(first, [e["position"] for e in page["events"]])
        self.assertTrue(all(e["key"] == "zzz" for e in page["events"]))

    def test_reject_and_ack_interleave(self) -> None:
        total = self.total
        self._claim()
        self._reject(0, idem="r0")
        ack1, _ = self._ack(1, idem="a1")
        self.assertEqual(ack1["position"], 1)
        self._reject(2, idem="r2")
        page, _ = self._pull(limit=1000)
        self.assertEqual([e["position"] for e in page["events"]],
                         list(range(3, total)))
        dead = self._dead_letters()
        self.assertEqual([e["position"] for e in dead["entries"]], [0, 2])

    def test_reject_idempotent_replay_returns_original_without_writing(
            self) -> None:
        self._claim()
        first, created = self._reject(0, reason="r", idem="r0")
        self.assertTrue(created)
        before = Path(self.cons).read_bytes()
        replay, replayed = self._reject(0, reason="r", idem="r0")
        self.assertFalse(replayed)
        self.assertEqual(replay, first)
        self.assertEqual(Path(self.cons).read_bytes(), before)
        # Whitespace changes the stored request: the key was bound to
        # the trimmed request, so an equivalent-trim replay still
        # matches, while a different trimmed reason conflicts.
        again, _ = self._reject(0, reason="  r  ", idem="r0")
        self.assertEqual(again, first)
        with self.assertRaises(ValueError):
            self._reject(0, reason="other", idem="r0")
        with self.assertRaises(ValueError):
            self._reject(1, reason="r", idem="r0")

    def test_reject_ownership_and_lease(self) -> None:
        self._claim()
        with self.assertRaises(PermissionError):
            self._reject(0, owner="o2", now=40, idem="r0")
        with self.assertRaises(TimeoutError):
            self._reject(0, owner="o1", now=141, idem="r0")
        # A failed reject (position 2 skips the still-pending position
        # 1) leaves the ledger byte-for-byte untouched.
        self._reject(0, idem="r0")
        before = Path(self.cons).read_bytes()
        with self.assertRaises(ValueError):
            self._reject(2, idem="skip")
        self.assertEqual(Path(self.cons).read_bytes(), before)

    def test_reject_unknown_consumer_is_key_error(self) -> None:
        self._claim()
        with self.assertRaises(KeyError):
            self._reject(0, consumer="ghost", idem="r0")

    def test_reject_checkpoint_regression_raises_lookup_error(self) -> None:
        total = self.total
        self._claim()
        self._ack(total - 1, idem="a-last")
        other = MigrationBatchTest(
            "test_get_returns_copy_and_unknown_key_raises")
        other.setUp()
        self.addCleanup(other.doCleanups)
        other._prepare_migrate()
        other._run(key="aaa", owner="o1", now=30)
        other_cons = os.path.join(other.tmp.name, "consumers.json")
        doc = json.loads(Path(self.cons).read_text("utf-8"))
        doc["coordination"] = os.path.realpath(other.paths["coord"])
        # The baseline ledger has no dead_letters section to rewrite.
        Path(other_cons).write_text(
            json.dumps(doc, ensure_ascii=False,
                       separators=(",", ":")) + "\n",
            encoding="utf-8")
        with self.assertRaises(LookupError):
            mb.consume(other.paths["coord"], other_cons, "reject", "c1",
                       "o1", 40, position=0, reason="r", idem="r-again")

    def test_reject_requires_idem(self) -> None:
        self._claim()
        with self.assertRaises(ValueError):
            mb.consume(self.coord, self.cons, "reject", "c1", "o1", 40,
                       position=0, reason="r", idem="")
        with self.assertRaises(ValueError):
            mb.consume(self.coord, self.cons, "reject", "c1", "o1", 40,
                       position=-1, reason="r", idem="i")
        with self.assertRaises(ValueError):
            mb.consume(self.coord, self.cons, "reject", "c1", "o1", 40,
                       position=True, reason="r", idem="i")

    # -- ledger upgrade -------------------------------------------------------

    def test_claim_and_ack_leave_baseline_shape_untouched(self) -> None:
        self._claim()
        self._ack(0, idem="a0")
        doc = json.loads(Path(self.cons).read_text("utf-8"))
        self.assertEqual(list(doc),
                         ["version", "coordination", "subscriptions",
                          "consumers", "idempotency", "audit"])
        before = Path(self.cons).read_bytes()
        # A pull and an in-place ack neither write nor upgrade the file.
        self._pull()
        self._ack(0, idem="a0-again")
        self.assertEqual(Path(self.cons).read_bytes(), before)
        doc = json.loads(Path(self.cons).read_text("utf-8"))
        self.assertNotIn("dead_letters", doc)

    def test_first_reject_upgrades_and_seeds_one_list_per_consumer(
            self) -> None:
        self._claim(consumer="c1", idem="k1")
        self._claim(consumer="c2", idem="k2", key="zzz")
        raw = Path(self.cons).read_bytes()
        self._reject(0, consumer="c1", idem="r0")
        upgraded = json.loads(Path(self.cons).read_text("utf-8"))
        self.assertEqual(list(upgraded),
                         ["version", "coordination", "subscriptions",
                          "consumers", "idempotency", "audit",
                          "dead_letters"])
        self.assertEqual(sorted(upgraded["dead_letters"]), ["c1", "c2"])
        self.assertEqual(len(upgraded["dead_letters"]["c1"]), 1)
        self.assertEqual(upgraded["dead_letters"]["c2"], [])
        # The pre-upgrade bytes really were the baseline shape.
        self.assertNotIn(b"dead_letters", raw)
        # After the upgrade a claim renewal and ack keep the section.
        self._claim(consumer="c1", now=50, idem="k1b")
        self._ack(1, consumer="c1", idem="a1")
        again = json.loads(Path(self.cons).read_text("utf-8"))
        self.assertIn("dead_letters", again)
        self.assertEqual([e["position"]
                          for e in again["dead_letters"]["c1"]], [0])
        # A consumer first claimed after the upgrade starts with an
        # empty list and can reject on its own.
        self._claim(consumer="c3", idem="k3", key="zzz")
        doc3 = json.loads(Path(self.cons).read_text("utf-8"))
        self.assertEqual(doc3["dead_letters"]["c3"], [])
        stream = mb.events(self.coord, limit=1000)["events"]
        first_zzz = next(e["position"] for e in stream if e["key"] == "zzz")
        result, _ = self._reject(first_zzz, consumer="c3", idem="r3")
        self.assertEqual(result["dead_letter"]["position"], first_zzz)

    # -- dead-letter query ----------------------------------------------------

    def test_dead_letters_pages_by_original_position(self) -> None:
        total = self.total
        self._claim()
        self._reject(0, idem="r0")
        self._ack(1, idem="a1")
        self._reject(2, idem="r2")
        page = self._dead_letters(limit=1)
        self.assertEqual(list(page), ["consumer", "entries", "next"])
        self.assertEqual(page["consumer"], "c1")
        self.assertEqual([e["position"] for e in page["entries"]], [0])
        self.assertEqual(page["next"], 0)
        page = self._dead_letters(cursor=0, limit=1)
        self.assertEqual([e["position"] for e in page["entries"]], [2])
        self.assertIsNone(page["next"])
        full = self._dead_letters()
        self.assertEqual([e["position"] for e in full["entries"]], [0, 2])
        self.assertIsNone(full["next"])
        # Each entry is the complete parked letter.
        self.assertEqual(list(full["entries"][0]),
                         ["position", "event", "reason", "rejected_at",
                          "owner"])
        stream = mb.events(self.coord, limit=1000)["events"]
        self.assertEqual([e["event"] for e in full["entries"]],
                         [stream[0], stream[2]])

    def test_dead_letters_default_limit_and_empty_consumer(self) -> None:
        self._claim(consumer="empty", idem="ke")
        page = mb.consumer_dead_letters(self.coord, self.cons, "empty")
        self.assertEqual(page, {"consumer": "empty", "entries": [],
                                "next": None})

    def test_dead_letters_unknown_consumer_is_key_error(self) -> None:
        self._claim()
        with self.assertRaises(KeyError):
            self._dead_letters(consumer="ghost")
        with self.assertRaises(KeyError):
            mb.consumer_dead_letters(
                self.coord, os.path.join(self.fx.tmp.name, "none.json"),
                "c1")

    def test_dead_letters_bad_arguments_raise_value_error(self) -> None:
        for bad in (
            lambda: mb.consumer_dead_letters("", self.cons, "c1"),
            lambda: mb.consumer_dead_letters(self.coord, "", "c1"),
            lambda: mb.consumer_dead_letters(self.coord, self.cons, ""),
            lambda: mb.consumer_dead_letters(
                self.coord, self.cons, "c1", cursor=-1),
            lambda: mb.consumer_dead_letters(
                self.coord, self.cons, "c1", cursor=True),
            lambda: mb.consumer_dead_letters(
                self.coord, self.cons, "c1", limit=0),
            lambda: mb.consumer_dead_letters(
                self.coord, self.cons, "c1", limit=1001),
        ):
            with self.subTest(bad=bad):
                self.assertRaises(ValueError, bad)

    def test_dead_letters_paths_must_be_distinct(self) -> None:
        with self.assertRaises(ValueError):
            mb.consumer_dead_letters(self.coord, self.coord, "c1")

    def test_dead_letters_missing_parent_raises_file_not_found(self) -> None:
        missing = os.path.join(self.fx.tmp.name, "no-dir", "c.json")
        with self.assertRaises(FileNotFoundError):
            mb.consumer_dead_letters(self.coord, missing, "c1")

    def test_dead_letters_reads_after_stream_truncation(self) -> None:
        # The dead-letter query opens only the consumer ledger, so a
        # rejected event stays observable after the bound stream is
        # shortened (the status path would instead hit LookupError).
        self._claim()
        result, _ = self._reject(0, idem="r0")
        other = MigrationBatchTest(
            "test_get_returns_copy_and_unknown_key_raises")
        other.setUp()
        self.addCleanup(other.doCleanups)
        other._prepare_migrate()
        other._run(key="aaa", owner="o1", now=30)
        other_cons = os.path.join(other.tmp.name, "consumers.json")
        doc = json.loads(Path(self.cons).read_text("utf-8"))
        doc["coordination"] = os.path.realpath(other.paths["coord"])
        Path(other_cons).write_text(
            json.dumps(doc, ensure_ascii=False,
                       separators=(",", ":")) + "\n",
            encoding="utf-8")
        page = mb.consumer_dead_letters(
            other.paths["coord"], other_cons, "c1")
        self.assertEqual([e["position"] for e in page["entries"]], [0])
        self.assertEqual(page["entries"][0], result["dead_letter"])

    def test_dead_letters_response_is_compact(self) -> None:
        self._claim()
        self._reject(0, reason="理由", idem="r0")
        body = mb.consumer_dead_letters_response(
            self.coord, self.cons, "c1")
        self.assertFalse(body.endswith(b"\n"))
        self.assertNotIn(b"\\u", body)
        self.assertIn("理由".encode("utf-8"), body)
        self.assertEqual(json.loads(body), self._dead_letters())

    # -- status ---------------------------------------------------------------

    def _status(self, consumer="c1", now=40):
        return mb.consumer_status(self.coord, self.cons, consumer, now)

    def test_status_fields_and_full_backlog(self) -> None:
        total = self.total
        self._claim()
        status = self._status()
        self.assertEqual(list(status),
                         ["consumer", "key", "job_id", "owner", "until",
                          "lease", "position", "pending", "oldest"])
        self.assertEqual(status["consumer"], "c1")
        self.assertIsNone(status["key"])
        self.assertIsNone(status["job_id"])
        self.assertEqual(status["owner"], "o1")
        self.assertEqual(status["until"], 140)
        self.assertEqual(status["lease"], "active")
        self.assertIsNone(status["position"])
        self.assertEqual(status["pending"], total)
        self.assertEqual(status["oldest"]["position"], 0)

    def test_status_lease_boundary(self) -> None:
        self._claim()
        self.assertEqual(self._status(now=140)["lease"], "active")
        self.assertEqual(self._status(now=141)["lease"], "expired")

    def test_status_pending_follows_ack_and_filters(self) -> None:
        total = self.total
        self._claim()
        self._ack(0, idem="a0")
        status = self._status()
        self.assertEqual(status["position"], 0)
        self.assertEqual(status["pending"], total - 1)
        self.assertEqual(status["oldest"]["position"], 1)
        self._ack(total - 1, idem="a-last")
        status = self._status()
        self.assertEqual(status["pending"], 0)
        self.assertIsNone(status["oldest"])

    def test_status_respects_fixed_subscription(self) -> None:
        self._claim(consumer="ck", idem="kb", key="zzz")
        status = self._status(consumer="ck")
        zzz = [e for e in mb.events(self.coord, limit=1000)["events"]
               if e["key"] == "zzz"]
        self.assertEqual(status["key"], "zzz")
        self.assertEqual(status["pending"], len(zzz))
        self.assertEqual(status["oldest"]["position"], zzz[0]["position"])
        self.assertTrue(all(event["key"] == "zzz"
                            for event in (status["oldest"],)))

    def test_status_oldest_is_the_full_event(self) -> None:
        self._claim()
        status = self._status()
        page = mb.events(self.coord, limit=1)["events"][0]
        self.assertEqual(status["oldest"], page)
        # The full post-commit batch snapshot rides along.
        self.assertEqual(status["oldest"]["batch"]["key"],
                         page["batch"]["key"])

    def test_status_unknown_consumer_is_key_error(self) -> None:
        self._claim()
        with self.assertRaises(KeyError):
            mb.consumer_status(self.coord, self.cons, "ghost", 40)

    def test_status_without_ledger_is_key_error(self) -> None:
        with self.assertRaises(KeyError):
            mb.consumer_status(
                self.coord, os.path.join(self.fx.tmp.name, "none.json"),
                "c1", 40)

    def test_status_bad_arguments_raise_value_error(self) -> None:
        for bad in (
            lambda: mb.consumer_status("", self.cons, "c1", 40),
            lambda: mb.consumer_status(self.coord, "", "c1", 40),
            lambda: mb.consumer_status(self.coord, self.cons, "", 40),
            lambda: mb.consumer_status(self.coord, self.cons, "c1", -1),
            lambda: mb.consumer_status(self.coord, self.cons, "c1", True),
            lambda: mb.consumer_status(self.coord, self.cons, "c1", 1.0),
        ):
            with self.subTest(bad=bad):
                self.assertRaises(ValueError, bad)

    def test_status_paths_must_be_distinct(self) -> None:
        with self.assertRaises(ValueError):
            mb.consumer_status(self.coord, self.coord, "c1", 40)

    def test_status_missing_parent_raises_file_not_found(self) -> None:
        missing = os.path.join(self.fx.tmp.name, "no-dir", "c.json")
        with self.assertRaises(FileNotFoundError):
            mb.consumer_status(self.coord, missing, "c1", 40)

    def test_status_checkpoint_regression_raises_lookup_error(self) -> None:
        total = self.total
        self._claim()
        self._ack(total - 1, idem="a-last")
        other = MigrationBatchTest(
            "test_get_returns_copy_and_unknown_key_raises")
        other.setUp()
        self.addCleanup(other.doCleanups)
        other._prepare_migrate()
        other._run(key="aaa", owner="o1", now=30)
        other_cons = os.path.join(other.tmp.name, "consumers.json")
        doc = json.loads(Path(self.cons).read_text("utf-8"))
        doc["coordination"] = os.path.realpath(other.paths["coord"])
        Path(other_cons).write_text(
            json.dumps(doc, ensure_ascii=False,
                       separators=(",", ":")) + "\n",
            encoding="utf-8")
        with self.assertRaises(LookupError):
            mb.consumer_status(other.paths["coord"], other_cons, "c1", 40)

    def test_status_response_is_compact(self) -> None:
        self._claim()
        self._ack(0, idem="a0")
        body = mb.consumer_status_response(self.coord, self.cons, "c1", 40)
        self.assertFalse(body.endswith(b"\n"))
        self.assertNotIn(b"\\u", body)
        self.assertEqual(json.loads(body), self._status())


if __name__ == "__main__":
    unittest.main()
