"""Library tests for migration_batch.consume rejects and dead letters.

Covers POST-style operation ``reject``: the trimmed 1..512 code-point
reason, the earliest-unacknowledged-matching-event rule (no skipping
predecessors, no non-matching or past-tail targets), atomic cursor
advance and dead-letter recording, pull exclusion, ownership and lease
rules, idempotent replay vs a divergent request, the LookupError when
the confirmed event was truncated, rewritten or reused, the read-only
``consumer_dead_letters`` pagination (exclusive cursor, 1..1000 limit,
next cursor and per-consumer ordering), the pre-dead-letter ledger
upgrade and canonical bytes, and the FileNotFoundError/ValueError/
OSError surface.
"""

from __future__ import annotations

import copy
import json
import os
import unittest
from pathlib import Path

from carbon_market import migration_batch as mb
from tests.test_migration_batch import MigrationBatchTest


class RejectTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fx = MigrationBatchTest(
            "test_get_returns_copy_and_unknown_key_raises")
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.fx._prepare_migrate()
        self.coord = self.fx.paths["coord"]
        self.cons = os.path.join(self.fx.tmp.name, "consumers.json")
        self.total = self._populate()

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
                now=40, idem="r"):
        return mb.consume(self.coord, self.cons, "reject", consumer, owner,
                          now, position=position, reason=reason, idem=idem)

    def _dead_letters(self, consumer="c1", **kwargs):
        return mb.consumer_dead_letters(
            self.coord, self.cons, consumer, **kwargs)

    # -- happy path -----------------------------------------------------------

    def test_reject_records_earliest_event_and_advances(self) -> None:
        self._claim()
        full = mb.events(self.coord, limit=1)["events"][0]
        result, created = self._reject(0, reason="  坏数据 😀\n", idem="r0")
        self.assertTrue(created)
        self.assertEqual(list(result),
                         ["consumer", "owner", "until", "position",
                          "dead_letter"])
        self.assertEqual(result["consumer"], "c1")
        self.assertEqual(result["position"], 0)
        letter = result["dead_letter"]
        self.assertEqual(list(letter),
                         ["position", "event", "reason", "rejected_at",
                          "owner"])
        # Whitespace is trimmed but interior text is preserved.
        self.assertEqual(letter["reason"], "坏数据 😀")
        self.assertEqual(letter["position"], 0)
        self.assertEqual(letter["rejected_at"], 40)
        self.assertEqual(letter["owner"], "o1")
        # The complete event rides along, snapshot included.
        self.assertEqual(letter["event"], full)
        self.assertEqual(letter["event"]["batch"]["key"], "aaa")
        # The cursor moved: the rejected event is no longer pulled.
        page, _ = self._pull(limit=self.total)
        self.assertNotIn(0, [event["position"] for event in page["events"]])
        self.assertEqual(page["position"], 0)

    def test_consecutive_rejects_drain_the_backlog(self) -> None:
        self._claim()
        for position in range(self.total):
            result, created = self._reject(
                position, reason=f"r{position}", idem=f"r{position}")
            self.assertTrue(created)
            self.assertEqual(result["position"], position)
        page, _ = self._pull(limit=self.total)
        self.assertEqual(page["events"], [])
        self.assertIsNone(page["next"])
        # Nothing left to reject: the tail has been passed.
        with self.assertRaises(ValueError):
            self._reject(self.total, reason="x", idem="past")

    def test_reject_tail_position_is_allowed(self) -> None:
        self._claim()
        self._ack(self.total - 2, idem="a")
        result, created = self._reject(self.total - 1, idem="tail")
        self.assertTrue(created)
        self.assertEqual(result["position"], self.total - 1)

    # -- reason validation ----------------------------------------------------

    def test_reason_is_trimmed_and_code_point_bounded(self) -> None:
        self._claim()
        self.assertEqual(self._reject(0, reason="x" * 512,
                                      idem="r0")[0]["position"], 0)
        # Astral-plane characters count as one code point each.
        result, _ = self._reject(1, reason="😀" * 512, idem="r1")
        self.assertEqual(len(result["dead_letter"]["reason"]), 512)
        for bad in ("", "   ", "\n\t", "x" * 513, "😀" * 513, 5, None,
                    ["x"], {"reason": "x"}):
            with self.subTest(bad=repr(bad)[:30]):
                with self.assertRaises(ValueError):
                    mb.consume(self.coord, self.cons, "reject", "c1", "o1",
                               40, position=1, reason=bad, idem=f"b{bad!r}"[:4])

    # -- position rules -------------------------------------------------------

    def test_reject_cannot_skip_an_earlier_matching_event(self) -> None:
        self._claim()
        with self.assertRaises(ValueError):
            self._reject(1, idem="skip")
        # The failed reject writes nothing: position 0 still pulls.
        page, _ = self._pull(limit=1)
        self.assertEqual([e["position"] for e in page["events"]], [0])

    def test_reject_after_ack_targets_next_matching(self) -> None:
        self._claim()
        self._ack(0, idem="a0")
        result, created = self._reject(1, idem="r1")
        self.assertTrue(created)
        self.assertEqual(result["position"], 1)

    def test_reject_equal_to_or_behind_cursor_raises(self) -> None:
        self._claim()
        self._reject(0, idem="r0")
        with self.assertRaises(ValueError):
            self._reject(0, idem="again")
        self._ack(2, idem="a2")
        with self.assertRaises(ValueError):
            self._reject(1, idem="back")

    def test_reject_past_tail_raises(self) -> None:
        self._claim()
        with self.assertRaises(ValueError):
            self._reject(self.total + 10, idem="past")

    def test_reject_non_matching_position_raises(self) -> None:
        # A fixed-key subscription must reject its own earliest match;
        # a non-matching stream position that precedes it is an invalid
        # target (the stream head is an aaa event, position 0).
        events = mb.events(self.coord, limit=1000)["events"]
        aaa = next(event["position"] for event in events
                   if event["key"] == "aaa")
        zzz = next(event["position"] for event in events
                   if event["key"] == "zzz")
        self.assertEqual(aaa, 0)
        self.assertGreater(zzz, aaa)
        self._claim(consumer="ck", idem="kk", key="zzz")
        with self.assertRaises(ValueError):
            self._reject(aaa, consumer="ck", idem="ra")
        # The earliest matching zzz event is accepted even though a
        # non-matching event precedes it.
        result, _ = self._reject(zzz, consumer="ck", idem="rz")
        self.assertEqual(result["position"], zzz)

    def test_reject_with_no_matching_event_raises(self) -> None:
        # A subscription that matches no event in a populated stream
        # has no earliest unacknowledged match: every reject target is
        # invalid and nothing is written.
        self._claim(consumer="cn", idem="kn", key="never-such-batch")
        before = Path(self.cons).read_bytes()
        for position in range(self.total):
            with self.assertRaises(ValueError):
                self._reject(position, consumer="cn",
                             idem=f"r{position}")
        self.assertEqual(Path(self.cons).read_bytes(), before)

    # -- ownership and lease --------------------------------------------------

    def test_reject_requires_ownership_and_valid_lease(self) -> None:
        self._claim()
        with self.assertRaises(PermissionError):
            self._reject(0, owner="o2", idem="rx")
        with self.assertRaises(TimeoutError):
            self._reject(0, now=141, idem="ry")
        # The endpoint-facing subclasses stay ordinary
        # PermissionError/TimeoutError to library callers.
        self.assertIsInstance(mb.ConsumerOwnershipError("x"),
                              PermissionError)
        self.assertIsInstance(mb.ConsumerLeaseExpired("x"), TimeoutError)

    def test_reject_unknown_consumer_is_key_error(self) -> None:
        with self.assertRaises(KeyError):
            self._reject(0, consumer="ghost")

    def test_takeover_keeps_cursor_and_dead_letters(self) -> None:
        self._claim()
        self._reject(0, idem="r0")
        self._claim(owner="o2", now=141, lease=100, idem="k2")
        page, _ = self._pull(owner="o2", now=141, limit=1)
        self.assertNotEqual(page["events"][0]["position"], 0)
        page = self._dead_letters(limit=1)
        self.assertEqual([e["position"] for e in page["entries"]], [0])

    # -- idempotency ----------------------------------------------------------

    def test_reject_replay_returns_original_without_writing(self) -> None:
        self._claim()
        first, created = self._reject(0, reason="x", idem="r0")
        self.assertTrue(created)
        before = Path(self.cons).read_bytes()
        replay, again = self._reject(0, reason="x", idem="r0")
        self.assertFalse(again)
        self.assertEqual(replay, first)
        self.assertEqual(Path(self.cons).read_bytes(), before)

    def test_reject_replay_compares_the_trimmed_reason(self) -> None:
        self._claim()
        first, _ = self._reject(0, reason="x", idem="r0")
        # The stored request carries the trimmed reason, so a replay
        # with equivalent surrounding whitespace still matches.
        replay, created = self._reject(0, reason="  x  ", idem="r0")
        self.assertFalse(created)
        self.assertEqual(replay, first)

    def test_reject_divergent_request_raises(self) -> None:
        self._claim()
        self._reject(0, reason="x", idem="r0", now=40)
        with self.assertRaises(ValueError):
            self._reject(0, reason="different", idem="r0", now=40)
        with self.assertRaises(ValueError):
            mb.consume(self.coord, self.cons, "reject", "c1", "o1", 41,
                       position=0, reason="x", idem="r0")
        # A claim/ack may not reuse a reject key either.
        with self.assertRaises(ValueError):
            self._ack(0, idem="r0")

    def test_failed_reject_writes_nothing(self) -> None:
        self._claim()
        before = Path(self.cons).read_bytes()
        for call in (
                lambda: self._reject(1, idem="bad"),       # skips head
                lambda: self._reject(999, idem="bad2"),    # past tail
                lambda: self._reject(0, owner="o2",
                                     idem="bad3")):         # foreign owner
            with self.assertRaises(Exception):
                call()
            self.assertEqual(Path(self.cons).read_bytes(), before)

    # -- checkpoint regression ------------------------------------------------

    def test_confirmed_cursor_truncated_raises_lookup_error(self) -> None:
        self._claim()
        self._ack(self.total - 1, idem="a-last")
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
            mb.consume(other.paths["coord"], other_cons, "reject", "c1",
                       "o1", 40, position=self.total - 1, reason="x",
                       idem="r-again")

    # -- dead-letter query ----------------------------------------------------

    def test_dead_letters_pages_by_original_position(self) -> None:
        self._claim()
        for position in range(3):
            self._reject(position, reason=f"r{position}",
                         idem=f"r{position}")
        page = self._dead_letters()
        self.assertEqual(list(page), ["consumer", "entries", "next"])
        self.assertEqual([e["position"] for e in page["entries"]], [0, 1, 2])
        self.assertIsNone(page["next"])
        self.assertEqual(
            list(page["entries"][0]),
            ["position", "event", "reason", "rejected_at", "owner"])

    def test_dead_letters_pagination(self) -> None:
        self._claim()
        for position in range(self.total):
            self._reject(position, reason="r", idem=f"r{position}")
        first = self._dead_letters(limit=2)
        self.assertEqual([e["position"] for e in first["entries"]], [0, 1])
        self.assertEqual(first["next"], 1)
        second = self._dead_letters(cursor=first["next"], limit=2)
        self.assertEqual([e["position"] for e in second["entries"]], [2, 3])
        self.assertEqual(second["next"], 3)
        rest = self._dead_letters(cursor=second["next"], limit=1000)
        self.assertEqual([e["position"] for e in rest["entries"]],
                         list(range(4, self.total)))
        self.assertIsNone(rest["next"])
        # A drained tail stays an empty final page.
        drained = self._dead_letters(cursor=self.total - 1)
        self.assertEqual(drained["entries"], [])
        self.assertIsNone(drained["next"])

    def test_dead_letters_are_per_consumer(self) -> None:
        self._claim()
        self._reject(0, idem="r0")
        self._claim(consumer="c2", idem="k2")
        self._reject(0, consumer="c2", idem="r2")
        page_c1 = self._dead_letters("c1")
        page_c2 = self._dead_letters("c2")
        self.assertEqual([e["position"] for e in page_c1["entries"]], [0])
        self.assertEqual([e["position"] for e in page_c2["entries"]], [0])
        self.assertTrue(all(e["owner"] == "o1"
                            for e in page_c1["entries"] + page_c2["entries"]))

    def test_dead_letters_entries_hold_full_events(self) -> None:
        self._claim()
        self._reject(0, reason="x", idem="r0")
        stream = mb.events(self.coord, limit=1)["events"][0]
        self.assertEqual(self._dead_letters()["entries"][0]["event"], stream)

    def test_dead_letters_unknown_consumer_is_key_error(self) -> None:
        self._claim()
        with self.assertRaises(KeyError):
            self._dead_letters("ghost")

    def test_dead_letters_without_ledger_is_key_error(self) -> None:
        with self.assertRaises(KeyError):
            mb.consumer_dead_letters(
                self.coord, os.path.join(self.fx.tmp.name, "none.json"),
                "c1")

    def test_dead_letters_bad_arguments_raise(self) -> None:
        for bad in (
            lambda: mb.consumer_dead_letters("", self.cons, "c1"),
            lambda: mb.consumer_dead_letters(self.coord, "", "c1"),
            lambda: mb.consumer_dead_letters(self.coord, self.cons, ""),
            lambda: mb.consumer_dead_letters(self.coord, self.cons, "c1",
                                            cursor=-1),
            lambda: mb.consumer_dead_letters(self.coord, self.cons, "c1",
                                            cursor=True),
            lambda: mb.consumer_dead_letters(self.coord, self.cons, "c1",
                                            limit=0),
            lambda: mb.consumer_dead_letters(self.coord, self.cons, "c1",
                                            limit=1001),
        ):
            with self.subTest(bad=bad):
                self.assertRaises(ValueError, bad)

    def test_dead_letters_paths_must_be_distinct(self) -> None:
        with self.assertRaises(ValueError):
            mb.consumer_dead_letters(self.coord, self.coord, "c1")

    def test_dead_letters_response_is_compact(self) -> None:
        self._claim()
        self._reject(0, reason="坏", idem="r0")
        body = mb.consumer_dead_letters_response(
            self.coord, self.cons, "c1")
        self.assertFalse(body.endswith(b"\n"))
        self.assertNotIn(b"\\u", body)
        self.assertEqual(json.loads(body), self._dead_letters())

    # -- ledger upgrade and canonical form ------------------------------------

    def test_old_ledger_keeps_shape_until_first_reject(self) -> None:
        self._claim()
        self._ack(0, idem="a0")
        raw = Path(self.cons).read_bytes()
        doc = json.loads(raw)
        self.assertEqual(list(doc),
                         ["version", "coordination", "subscriptions",
                          "consumers", "idempotency", "audit"])
        # Idempotent replays, an in-place ack and the read-only
        # dead-letter query all leave the six-field bytes untouched.
        before = raw
        self._claim()                                   # same idem
        self._ack(0, idem="a0")                        # same idem
        self._ack(0, idem="a0-repeat")                 # in place
        self.assertEqual(self._dead_letters()["entries"], [])
        self.assertEqual(Path(self.cons).read_bytes(), before)
        # A genuine renewal writes but still no dead_letters section.
        self._claim(now=50, idem="k1b")
        self.assertEqual(
            list(json.loads(Path(self.cons).read_text("utf-8"))),
            ["version", "coordination", "subscriptions", "consumers",
             "idempotency", "audit"])
        # The first reject appends the section once.
        self._reject(1, reason="x", idem="r1")
        upgraded = json.loads(Path(self.cons).read_text("utf-8"))
        self.assertEqual(list(upgraded),
                         ["version", "coordination", "subscriptions",
                          "consumers", "idempotency", "audit",
                          "dead_letters"])
        self.assertEqual(len(upgraded["dead_letters"]), 1)
        record = upgraded["dead_letters"][0]
        self.assertEqual(list(record),
                         ["consumer", "position", "event", "reason",
                          "rejected_at", "owner"])
        # A later ack keeps the upgraded seven-field shape.
        self._ack(2, idem="a2")
        self.assertIn("dead_letters",
                      json.loads(Path(self.cons).read_text("utf-8")))

    def test_upgraded_ledger_stays_canonical(self) -> None:
        self._claim()
        self._reject(0, reason="x", idem="r0")
        raw = Path(self.cons).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        self.assertNotIn(b"\\u", raw)
        # Every reader accepts the upgraded bytes.
        self._pull()
        self._dead_letters()
        self._ack(1, idem="a1")
        self._status_read()

    def _status_read(self) -> None:
        mb.consumer_status(self.coord, self.cons, "c1", 40)

    def test_status_still_counts_pending_after_rejects(self) -> None:
        self._claim()
        self._reject(0, idem="r0")
        status = mb.consumer_status(self.coord, self.cons, "c1", 40)
        self.assertEqual(status["position"], 0)
        self.assertEqual(status["pending"], self.total - 1)
        self.assertEqual(status["oldest"]["position"], 1)

    def test_tampered_dead_letter_section_is_invalid(self) -> None:
        self._claim()
        self._reject(0, reason="x", idem="r0")
        good = Path(self.cons).read_bytes()
        doc = json.loads(good)
        # Drop the section: audit still references a reject, so the
        # missing letter must fail validation.
        del doc["dead_letters"]
        self._assert_invalid(doc)
        # Add an unrelated letter that matches no reject audit event.
        doc = json.loads(good)
        extra = copy.deepcopy(doc["dead_letters"][0])
        extra["position"] = 1
        extra["event"]["position"] = 1
        doc["dead_letters"].append(extra)
        self._assert_invalid(doc)
        # Corrupt a letter field.
        doc = json.loads(good)
        doc["dead_letters"][0]["reason"] = ""
        self._assert_invalid(doc)
        Path(self.cons).write_bytes(good)

    def _assert_invalid(self, doc: dict) -> None:
        Path(self.cons).write_text(
            json.dumps(doc, ensure_ascii=False, separators=(",", ":"))
            + "\n", encoding="utf-8")
        with self.assertRaises(mb.ConsumerLedgerInvalid):
            self._pull()
        with self.assertRaises(mb.ConsumerLedgerInvalid):
            self._dead_letters()

    def test_missing_parent_raises_file_not_found(self) -> None:
        missing = os.path.join(self.fx.tmp.name, "no-dir", "c.json")
        with self.assertRaises(FileNotFoundError):
            mb.consume(self.coord, missing, "reject", "c", "o", 1,
                       position=0, reason="x", idem="i")
        with self.assertRaises(FileNotFoundError):
            mb.consumer_dead_letters(self.coord, missing, "c")


if __name__ == "__main__":
    unittest.main()
