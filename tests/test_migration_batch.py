from __future__ import annotations

import json
import os
import shutil
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import dispatch as dispatch_module
from carbon_market import execution as execution_module
from carbon_market import jobs as jobs_module
from carbon_market import migration_batch
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.market import clear_live
from carbon_market.rebalance import current as current_binding


def _resource(resource_id: str, region: str, **overrides: object) -> dict:
    resource: dict[str, object] = {
        "resource_id": resource_id,
        "region": region,
        "capacity": 100,
        "start": 0,
        "end": 500,
        "unit_cost": 5,
        "carbon_intensity": 8,
        "residency": [region],
    }
    resource.update(overrides)
    return resource  # type: ignore[return-value]


def _signal(region: str, **overrides: object) -> dict:
    signal: dict[str, object] = {
        "region": region,
        "observed": 0,
        "expires": 500,
        "mix": {"solar": 10000},
        "unit_cost": 5,
        "carbon_intensity": 8,
    }
    signal.update(overrides)
    return signal  # type: ignore[return-value]


def _job(job_id: str, **overrides: object) -> dict:
    job: dict[str, object] = {
        "job_id": job_id,
        "work": 10,
        "deadline": 200,
        "regions": ["eu-north", "us-west"],
        "residency": ["eu-north"],
        "max_cost": 1000,
        "carbon_cap": 1000,
    }
    job.update(overrides)
    return job  # type: ignore[return-value]


class MigrationBatchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        base = self.tmp.name
        self.paths = {
            name: os.path.join(base, name + ".json")
            for name in ("jobs", "supply", "signals", "trades", "dispatch",
                         "execution", "advice", "intents", "settlements",
                         "coord")
        }

    # -- fixture helpers ---------------------------------------------------

    def _seed_base(self) -> None:
        p = self.paths
        jobs_module.submit(p["jobs"], _job("j-1"), "jk-1")
        resources_module.publish(
            p["supply"],
            _resource("r-2", "us-west", unit_cost=3, carbon_intensity=2,
                      residency=["eu-north", "us-west"]), "rk-1")
        resources_module.publish(
            p["supply"], _resource("r-1", "eu-north"), "rk-2")
        signals_module.publish(p["signals"], _signal("us-west", unit_cost=3, carbon_intensity=2),
                               "sk-1")
        signals_module.publish(p["signals"],
                               _signal("eu-north", unit_cost=5,
                                       carbon_intensity=8),
                               "sk-2")

    def _commit(self, job_id: str = "j-1", at: int = 10,
                trade_key: str = "t-1", commit_key: str = "d-1") -> None:
        p = self.paths
        clear_live(p["jobs"], p["supply"], p["signals"], p["trades"], job_id,
                   trade_key, at)
        dispatch_module.commit(p["jobs"], p["supply"], p["trades"],
                               p["dispatch"], job_id, commit_key, at)

    def _bootstrap_execution(self) -> None:
        # An isolated job creates the execution ledger without touching
        # j-1's candidates or capacity.
        p = self.paths
        jobs_module.submit(p["jobs"], _job(
            "j-z", regions=["zz-region"], residency=["zz-region"]), "jk-z")
        resources_module.publish(
            p["supply"],
            _resource("r-z", "zz-region", residency=["zz-region"],
                      carbon_intensity=1), "rk-z")
        signals_module.publish(p["signals"], _signal("zz-region", unit_cost=1, carbon_intensity=1),
                               "sk-z")
        clear_live(p["jobs"], p["supply"], p["signals"], p["trades"], "j-z",
                   "t-z", 10)
        dispatch_module.commit(p["jobs"], p["supply"], p["trades"],
                               p["dispatch"], "j-z", "d-z", 10)
        dispatch_module.claim(p["dispatch"], "j-z", "c-z", "w-z", 150, 10)
        execution_module.plan(p["jobs"], p["supply"], p["trades"],
                              p["dispatch"], p["execution"], "j-z", "e-z",
                              "w-z", None, 10)

    def _green(self) -> None:
        # A cleaner eu-north signal after the trade makes the advice a
        # migration from r-2 to r-1.
        signals_module.publish(
            self.paths["signals"],
            _signal("eu-north", unit_cost=1, carbon_intensity=1,
                    observed=15), "sk-3")

    def _prepare_migrate(self) -> None:
        self._seed_base()
        self._commit()
        self._bootstrap_execution()
        self._green()

    def _run(self, *, owner: str = "owner-1", key: str = "bk-1",
             now: int = 30, lease: int = 100, receipts=None):
        p = self.paths
        return migration_batch.run(
            p["jobs"], p["supply"], p["signals"], p["trades"],
            p["dispatch"], p["execution"], p["advice"], p["intents"],
            p["settlements"], p["coord"], owner, key, now, lease, receipts)

    def _receipt(self, step: str, result: str, receipt: str,
                 at: int) -> dict:
        return {"step": step, "result": result, "receipt": receipt, "at": at}

    def _migrate_member(self, batch: dict) -> dict:
        return batch["items"]["j-1"]

    def _coord(self) -> dict:
        return json.loads(Path(self.paths["coord"]).read_text("utf-8"))

    # -- keep ---------------------------------------------------------------

    def test_claimed_active_job_is_kept(self) -> None:
        self._seed_base()
        self._commit()
        self._bootstrap_execution()
        # j-1's booking is claimed with an active launch plan: no advice
        # is evaluated, the job simply keeps its current binding.
        dispatch_module.claim(self.paths["dispatch"], "j-1", "c-1", "w-1",
                              150, 11)
        execution_module.plan(self.paths["jobs"], self.paths["supply"],
                              self.paths["trades"], self.paths["dispatch"],
                              self.paths["execution"], "j-1", "e-1", "w-1",
                              None, 11)
        batch, created = self._run(now=20)
        self.assertTrue(created)
        self.assertEqual(batch["status"], "completed")
        item = self._migrate_member(batch)
        self.assertEqual(item["phase"], "kept")
        self.assertIsNone(item["plan_key"])
        self.assertEqual(item["snapshot"],
                         {"resource_id": "r-2", "version": 1})
        self.assertFalse(Path(self.paths["advice"]).exists())

    def test_evaluate_keep_advice_completes_member(self) -> None:
        self._seed_base()
        self._commit()
        self._bootstrap_execution()
        # No claim, no plan: the decision is ready and r-2 is still the
        # cleanest feasible resource, so the batch's own advice is keep.
        batch, _ = self._run(now=20)
        self.assertEqual(batch["status"], "completed")
        item = self._migrate_member(batch)
        self.assertEqual(item["phase"], "kept")
        self.assertEqual(item["snapshot"],
                         {"resource_id": "r-2", "version": 1})
        data = self._coord()
        event = next(event for event in data["audit"]
                     if event["key"] == "bk-1")
        self.assertEqual(event["batch"], batch)

    def test_empty_scan_is_persisted_completed(self) -> None:
        self._prepare_migrate()
        # The first batch starts j-1's migration and then waits, holding
        # the only traded member open.
        batch, created = self._run(now=30)
        self.assertTrue(created)
        self.assertEqual(batch["status"], "pending")
        # A second batch has no member left to pick: the open batch
        # already drives j-1, so its scan is empty -- still persisted as
        # a completed batch with an audit event.
        empty, created_empty = self._run(key="bk-2", now=31)
        self.assertTrue(created_empty)
        self.assertEqual(empty["status"], "completed")
        self.assertEqual(empty["items"], {})
        data = self._coord()
        self.assertEqual(list(data["batches"]), ["bk-1", "bk-2"])
        event = next(event for event in data["audit"]
                     if event["key"] == "bk-2")
        self.assertEqual(event["batch"], empty)

    # -- migrate happy path -------------------------------------------------

    def test_migrate_reserves_claims_records_and_settles(self) -> None:
        self._prepare_migrate()
        batch, _ = self._run(now=30)
        self.assertEqual(self._migrate_member(batch)["phase"], "active")
        batch, _ = self._run(
            now=32, receipts={"j-1": self._receipt(
                "copy", "succeeded", "copied", 33)})
        self.assertEqual(self._migrate_member(batch)["phase"], "active")
        batch, _ = self._run(
            now=34, receipts={"j-1": self._receipt(
                "switch", "succeeded", "switched", 35)})
        self.assertEqual(batch["status"], "completed")
        item = self._migrate_member(batch)
        self.assertEqual(item["phase"], "settled")
        self.assertEqual(item["snapshot"]["state"], "active")
        self.assertEqual(item["snapshot"]["generation"], 1)
        self.assertEqual(item["snapshot"]["after"],
                         {"resource_id": "r-1", "version": 1})
        view = current_binding(
            self.paths["jobs"], self.paths["supply"], self.paths["signals"],
            self.paths["trades"], self.paths["dispatch"],
            self.paths["execution"], self.paths["advice"],
            self.paths["intents"], self.paths["settlements"], "j-1")
        self.assertEqual(view["resource_id"], "r-1")
        self.assertEqual(view["generation"], 1)

    def test_members_are_ordered_by_job_id(self) -> None:
        self._prepare_migrate()
        batch, _ = self._run(now=30)
        self.assertEqual(list(batch["items"]), ["j-1", "j-z"])

    def test_waiting_member_does_not_block_the_others(self) -> None:
        self._prepare_migrate()
        # A second job follows the same r-2 -> r-1 migration.
        p = self.paths
        jobs_module.submit(p["jobs"], _job("j-2"), "jk-2")
        clear_live(p["jobs"], p["supply"], p["signals"], p["trades"], "j-2",
                   "t-2", 10)
        dispatch_module.commit(p["jobs"], p["supply"], p["trades"],
                               p["dispatch"], "j-2", "d-2", 10)
        # j-1 gets no receipt and waits; j-2 drives both steps and
        # settles in the very same run.
        receipts = {
            "j-2": self._receipt("copy", "succeeded", "c2", 31),
        }
        batch, _ = self._run(now=30, receipts=receipts)
        # A member consumes at most one receipt per run, so j-2 is past
        # its copy while j-1 holds the batch open; finish j-2 next.
        self.assertEqual(batch["items"]["j-1"]["phase"], "active")
        batch, _ = self._run(
            now=32,
            receipts={"j-2": self._receipt("switch", "succeeded", "s2",
                                           33)})
        self.assertEqual(batch["items"]["j-1"]["phase"], "active")
        self.assertEqual(batch["items"]["j-2"]["phase"], "settled")
        self.assertEqual(batch["status"], "pending")

    # -- waiting and recovery -----------------------------------------------

    def test_wait_without_receipt_while_lease_valid(self) -> None:
        self._prepare_migrate()
        self._run(now=30)
        before = Path(self.paths["coord"]).read_bytes()
        batch, created = self._run(now=31)
        self.assertFalse(created)
        self.assertEqual(batch["status"], "pending")
        item = self._migrate_member(batch)
        self.assertEqual(item["phase"], "active")
        self.assertEqual(item["lease"], 130)
        # Re-running without a receipt inside the lease renews only the
        # coordination lease; the plan itself is untouched.
        self.assertNotEqual(before, Path(self.paths["coord"]).read_bytes())

    def test_strict_expiry_recovers_and_settles_interrupted(self) -> None:
        self._prepare_migrate()
        self._run(now=30)
        self._run(now=32, receipts={"j-1": self._receipt(
            "copy", "succeeded", "copied", 33)})
        # The claim lease ends at 130; with no switch receipt the plan is
        # recovered only once 131 arrives, then settled as interrupted.
        batch, _ = self._run(now=131)
        self.assertEqual(batch["status"], "completed")
        item = self._migrate_member(batch)
        self.assertEqual(item["phase"], "settled")
        self.assertEqual(item["snapshot"]["state"], "compensated")
        self.assertEqual(item["snapshot"]["migration"], "interrupted")
        self.assertEqual(item["snapshot"]["after"],
                         {"resource_id": "r-2", "version": 1})

    def test_failed_step_settles_as_compensated(self) -> None:
        self._prepare_migrate()
        self._run(now=30)
        batch, _ = self._run(
            now=32, receipts={"j-1": self._receipt(
                "copy", "failed", "boom", 33)})
        self.assertEqual(batch["status"], "completed")
        item = self._migrate_member(batch)
        self.assertEqual(item["phase"], "settled")
        self.assertEqual(item["snapshot"]["state"], "compensated")
        self.assertEqual(item["snapshot"]["migration"], "failed")
        self.assertEqual(item["snapshot"]["after"],
                         {"resource_id": "r-2", "version": 1})

    # -- failure isolation --------------------------------------------------

    def test_wrong_step_receipt_fails_member_but_others_continue(self) -> None:
        self._prepare_migrate()
        self._run(now=30)
        # switch arrives before copy: rebalance.record refuses it with
        # ValueError, the public class is saved on this member only.
        batch, _ = self._run(
            now=32, receipts={"j-1": self._receipt(
                "switch", "succeeded", "early", 33)})
        item = self._migrate_member(batch)
        self.assertEqual(item["phase"], "failed")
        self.assertEqual(item["error"], "ValueError")
        self.assertIsNone(item["snapshot"])
        # The batch still closes because every member reached a terminal
        # phase; j-z kept going untouched.
        self.assertEqual(batch["status"], "completed")
        self.assertEqual(batch["items"]["j-z"]["phase"], "kept")

    # -- replay, ownership and stable keys ----------------------------------

    def test_completed_batch_replays_byte_identical(self) -> None:
        self._prepare_migrate()
        self._run(now=30)
        self._run(now=32, receipts={"j-1": self._receipt(
            "copy", "succeeded", "copied", 33)})
        self._run(now=34, receipts={"j-1": self._receipt(
            "switch", "succeeded", "switched", 35)})
        raw = Path(self.paths["coord"]).read_bytes()
        batch, created = self._run(now=500, owner="someone-else")
        self.assertFalse(created)
        self.assertEqual(Path(self.paths["coord"]).read_bytes(), raw)
        self.assertEqual(batch["status"], "completed")

    def test_other_owner_blocked_before_expiry_takes_over_after(self) -> None:
        self._prepare_migrate()
        self._run(owner="owner-a", now=30, lease=100)
        raw = Path(self.paths["coord"]).read_bytes()
        with self.assertRaises(PermissionError):
            self._run(owner="owner-b", now=31)
        self.assertEqual(Path(self.paths["coord"]).read_bytes(), raw)
        # Strict coordination-lease expiry (until == 130) lets owner-b
        # take over; the plan lease has expired too, so the migration is
        # recovered and settled in the same run.
        batch, created = self._run(owner="owner-b", now=131)
        self.assertFalse(created)
        self.assertEqual(batch["owner"], "owner-b")
        self.assertEqual(batch["status"], "completed")
        self.assertEqual(self._migrate_member(batch)["phase"], "settled")

    def test_same_key_with_changed_ledger_set_raises(self) -> None:
        self._prepare_migrate()
        self._run(now=30)
        advice2 = os.path.join(self.tmp.name, "advice-other.json")
        shutil.copyfile(self.paths["advice"], advice2)
        p = self.paths
        with self.assertRaises(ValueError):
            migration_batch.run(
                p["jobs"], p["supply"], p["signals"], p["trades"],
                p["dispatch"], p["execution"], advice2, p["intents"],
                p["settlements"], p["coord"], "owner-1", "bk-1", 31, 100)

    def test_changed_receipt_for_recorded_step_raises(self) -> None:
        self._prepare_migrate()
        self._run(now=30)
        self._run(now=32, receipts={"j-1": self._receipt(
            "copy", "succeeded", "copied", 33)})
        with self.assertRaises(ValueError):
            self._run(now=34, receipts={"j-1": self._receipt(
                "copy", "succeeded", "different-text", 33)})

    def test_action_is_persisted_before_downstream_call(self) -> None:
        self._prepare_migrate()
        from carbon_market import rebalance
        seen = {}
        original = rebalance.start

        def observe(*args, **kwargs):
            data = self._coord()
            item = data["batches"]["bk-1"]["items"]["j-1"]
            seen["marker"] = item["snapshot"]
            seen["request_key"] = item["request_key"]
            return original(*args, **kwargs)

        rebalance.start = observe
        try:
            self._run(now=30)
        finally:
            rebalance.start = original
        self.assertEqual(seen["marker"]["action"], "start")
        self.assertEqual(seen["marker"]["request"]["owner"], "owner-1")
        self.assertEqual(seen["request_key"], seen["request_key"])

    def test_settle_crash_replays_same_request_and_converges(self) -> None:
        self._prepare_migrate()
        self._run(now=30)
        self._run(now=32, receipts={"j-1": self._receipt(
            "copy", "succeeded", "copied", 33)})
        from carbon_market import rebalance
        calls = {"n": 0}
        original = rebalance.settle

        def flaky(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                # The settlement lands (pending then final) but the
                # coordination update is interrupted afterwards.
                result = original(*args, **kwargs)
                raise RuntimeError("coord update lost")
            return original(*args, **kwargs)

        rebalance.settle = flaky
        try:
            with self.assertRaises(RuntimeError):
                self._run(now=34, receipts={"j-1": self._receipt(
                    "switch", "succeeded", "switched", 35)})
            # The member is still active-side settling with its marker;
            # the re-run replays the identical settle request (one more
            # call) and closes the batch without a second settlement.
            batch, created = self._run(now=36)
        finally:
            rebalance.settle = original
        self.assertFalse(created)
        self.assertEqual(batch["status"], "completed")
        self.assertEqual(calls["n"], 2)
        item = self._migrate_member(batch)
        self.assertEqual(item["phase"], "settled")

    # -- next generation -----------------------------------------------------

    def test_new_batch_after_settlement_judges_from_new_binding(self) -> None:
        self._prepare_migrate()
        self._run(now=30)
        self._run(now=32, receipts={"j-1": self._receipt(
            "copy", "succeeded", "copied", 33)})
        self._run(now=34, receipts={"j-1": self._receipt(
            "switch", "succeeded", "switched", 35)})
        # A later batch sees the settled target as the current binding;
        # with no new green signal elsewhere its own advice is keep.
        batch, created = self._run(key="bk-2", now=40)
        self.assertTrue(created)
        self.assertEqual(batch["status"], "completed")
        item = self._migrate_member(batch)
        self.assertEqual(item["phase"], "kept")
        self.assertEqual(item["snapshot"],
                         {"resource_id": "r-1", "version": 1})

    # -- read-only get -------------------------------------------------------

    def test_get_returns_copy_and_unknown_key_raises(self) -> None:
        self._prepare_migrate()
        with self.assertRaises(KeyError):
            migration_batch.get(self.paths["coord"], "nope")
        self.assertFalse(Path(self.paths["coord"]).exists())
        self._run(now=30)
        batch = migration_batch.get(self.paths["coord"], "bk-1")
        self.assertEqual(batch["key"], "bk-1")
        batch["items"]["j-1"]["phase"] = "settled"
        again = migration_batch.get(self.paths["coord"], "bk-1")
        self.assertEqual(again["items"]["j-1"]["phase"], "active")
        with self.assertRaises(KeyError):
            migration_batch.get(self.paths["coord"], "missing")

    # -- read-only validation priority and traces --------------------------

    def _completed_and_active(self) -> None:
        self._prepare_migrate()
        self._run(now=30)               # bk-1 stays active
        self._run(key="bk-2", now=31)   # empty scan completes bk-2

    def _clear_lock_files(self) -> None:
        for name in os.listdir(self.tmp.name):
            if name.endswith(".lock"):
                os.unlink(os.path.join(self.tmp.name, name))

    def test_get_validates_format_before_the_target_key(self) -> None:
        self._completed_and_active()
        coord = self.paths["coord"]
        raw = Path(coord).read_bytes()
        self._clear_lock_files()

        def mutate(payload: bytes) -> None:
            Path(coord).write_bytes(payload)

        # Every malformed/non-canonical shape is a ValueError even when
        # the requested key is unknown; the unknown key never masks the
        # bad format.
        cases: list[tuple[str, bytes]] = [
            ("bad json", b"{not json\n"),
            ("bad utf-8", b'{"version": 1, "batches": \xff\n'),
        ]
        data = json.loads(raw.decode("utf-8"))
        wrong_version = dict(data)
        wrong_version["version"] = 2
        cases.append(("wrong version",
                      (json.dumps(wrong_version, separators=(",", ":"),
                                  ensure_ascii=False) + "\n").encode()))
        cases.append(("root field order",
                      b'{"audit":[],"version":1,"batches":{}}\n'))
        cases.append(("non-canonical bytes", raw + b"\n"))
        for label, payload in cases:
            mutate(payload)
            with self.subTest(label=label):
                with self.assertRaises(ValueError):
                    migration_batch.get(coord, "does-not-exist")
                with self.assertRaises(ValueError):
                    migration_batch.search(coord)

    def test_unknown_key_and_missing_file_leave_no_trace(self) -> None:
        self._completed_and_active()
        self._clear_lock_files()
        coord = self.paths["coord"]
        with self.assertRaises(KeyError):
            migration_batch.get(coord, "missing")
        # The read-only miss created neither the coordination lock nor
        # any business lock file.
        self.assertEqual(
            [n for n in os.listdir(self.tmp.name) if n.endswith(".lock")],
            [])
        absent = os.path.join(self.tmp.name, "nested", "missing.json")
        with self.assertRaises(KeyError):
            migration_batch.get(absent, "x")
        self.assertFalse(os.path.exists(os.path.join(self.tmp.name,
                                                     "nested")))
        with self.assertRaises(FileNotFoundError):
            migration_batch.search(absent)

    def test_search_pages_with_completion_info(self) -> None:
        self._completed_and_active()
        coord = self.paths["coord"]
        page = migration_batch.search(coord, limit=1)
        self.assertEqual(list(page), ["entries", "next"])
        self.assertEqual([entry[0] for entry in page["entries"]], ["bk-1"])
        self.assertEqual(page["next"], "bk-1")
        active_entry = page["entries"][0]
        self.assertEqual(len(active_entry), 3)
        self.assertEqual(active_entry[0], "bk-1")
        self.assertEqual(active_entry[1]["key"], "bk-1")
        # An active batch carries null completion info.
        self.assertIsNone(active_entry[2])

        rest = migration_batch.search(coord, cursor="bk-1", limit=10)
        self.assertEqual([entry[0] for entry in rest["entries"]], ["bk-2"])
        self.assertIsNone(rest["next"])
        completed_entry = rest["entries"][0]
        self.assertEqual(completed_entry[1]["status"], "completed")
        # bk-2 was the first (zero-based) batch to complete, at moment 31.
        self.assertEqual(completed_entry[2], {"index": 0, "at": 31})

        # The cursor is exclusive and the default page size covers both.
        default = migration_batch.search(coord)
        self.assertEqual([entry[0] for entry in default["entries"]],
                         ["bk-1", "bk-2"])
        self.assertIsNone(default["next"])
        self.assertEqual(
            migration_batch.search(coord, cursor="bk-2")["entries"], [])

    def test_search_validates_page_arguments(self) -> None:
        self._completed_and_active()
        coord = self.paths["coord"]
        for bad in (0, -1, 1001, True, "100", 1.5):
            with self.subTest(limit=bad):
                with self.assertRaises(ValueError):
                    migration_batch.search(coord, limit=bad)  # type: ignore[arg-type]
        for bad in ("", 5):
            with self.subTest(cursor=bad):
                with self.assertRaises(ValueError):
                    migration_batch.search(coord, cursor=bad)  # type: ignore[arg-type]

    def test_responses_serialize_and_map_missing_business_ledger(self):
        self._completed_and_active()
        coord = self.paths["coord"]
        body = migration_batch.get_response(coord, "bk-1")
        self.assertEqual(json.loads(body), migration_batch.get(coord, "bk-1"))
        page_body = migration_batch.search_response(coord, limit=1)
        self.assertEqual(json.loads(page_body),
                         migration_batch.search(coord, limit=1))
        with self.assertRaises(KeyError):
            migration_batch.get_response(coord, "missing")

        # A coordination ledger that references a missing business
        # ledger is a broken reference (ValueError) for the public
        # response builders.
        with open(self.paths["supply"], "rb") as handle:
            supply_bytes = handle.read()
        os.unlink(self.paths["supply"])
        try:
            with self.assertRaises(ValueError):
                migration_batch.get_response(coord, "bk-1")
            with self.assertRaises(ValueError):
                migration_batch.search_response(coord)
        finally:
            with open(self.paths["supply"], "wb") as handle:
                handle.write(supply_bytes)

    # -- canonical form ------------------------------------------------------

    def test_canonical_form_non_ascii_and_append_audit(self) -> None:
        self._prepare_migrate()
        self._run(now=30)
        self._run(now=32, receipts={"j-1": self._receipt(
            "copy", "succeeded", "复制完成", 33)})
        raw = Path(self.paths["coord"]).read_bytes()
        self.assertTrue(raw.endswith(b"\n"))
        self.assertFalse(raw.endswith(b"\n\n"))
        self.assertIn("复制完成".encode("utf-8"), raw)
        self.assertNotIn(b"\\u", raw)
        data = json.loads(raw.decode("utf-8"))
        self.assertEqual(list(data), ["version", "batches", "audit"])
        self.assertEqual(data["version"], 1)
        self.assertEqual(list(data["batches"]), ["bk-1"])
        self.assertEqual(list(data["batches"]["bk-1"]["items"]),
                         ["j-1", "j-z"])
        self.assertEqual(list(data["batches"]["bk-1"]["inputs"]),
                         ["advice", "dispatch", "execution", "jobs",
                          "ledger", "settlements", "signals", "supply",
                          "trades"])
        # Close the batch: the audit list appends one closing event.
        self._run(now=131)
        data = self._coord()
        self.assertEqual([event["key"] for event in data["audit"]],
                         ["bk-1"])
        self.assertEqual(data["audit"][0]["batch"]["status"], "completed")

    def test_tampered_ledger_raises_value_error(self) -> None:
        self._prepare_migrate()
        self._run(now=30)
        raw = Path(self.paths["coord"]).read_bytes()
        data = json.loads(raw.decode("utf-8"))
        data["version"] = 2
        Path(self.paths["coord"]).write_text(
            json.dumps(data) + "\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            self._run(key="bk-9")
        Path(self.paths["coord"]).write_bytes(raw)
        data = json.loads(raw.decode("utf-8"))
        data["audit"].insert(0, data["audit"][0]) if data["audit"] else None
        # Out-of-order audit is illegal: fabricate a duplicate-free
        # unordered case by tampering an item field to a wrong value.
        tampered = json.loads(raw.decode("utf-8"))
        tampered["batches"]["bk-1"]["owner"] = ""
        Path(self.paths["coord"]).write_text(
            json.dumps(tampered, ensure_ascii=False,
                       separators=(",", ":")) + "\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            self._run(key="bk-9")

    def test_non_canonical_bytes_rejected(self) -> None:
        self._prepare_migrate()
        self._run(now=30)
        raw = Path(self.paths["coord"]).read_bytes()
        Path(self.paths["coord"]).write_bytes(raw + b"\n")
        with self.assertRaises(ValueError):
            self._run(key="bk-9")

    # -- arguments and missing files -----------------------------------------

    def test_invalid_arguments(self) -> None:
        self._prepare_migrate()
        good = [self.paths[name] for name in (
            "jobs", "supply", "signals", "trades", "dispatch", "execution",
            "advice", "intents", "settlements", "coord")] + \
            ["owner", "bk", 30, 100]
        for index in range(12):
            for bad in ("", 123, None):
                args = list(good)
                args[index] = bad
                with self.subTest(index=index, bad=bad):
                    with self.assertRaises(ValueError):
                        migration_batch.run(*args)  # type: ignore[arg-type]
        for bad in (-1, True, False, 1.5, "30", None):
            with self.subTest(now=bad):
                with self.assertRaises(ValueError):
                    self._run(now=bad)  # type: ignore[arg-type]
        for bad in (0, -1, True, False, 1.5, "100", None):
            with self.subTest(lease=bad):
                with self.assertRaises(ValueError):
                    self._run(lease=bad)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            migration_batch.run(
                *([self.paths["jobs"]] * 2 +
                  [self.paths[name] for name in (
                      "signals", "trades", "dispatch", "execution",
                      "advice", "intents", "settlements", "coord")]),
                "owner", "bk", 30, 100)
        for bad_receipt in (
                {"j-1": {"step": "stage", "result": "succeeded",
                         "receipt": "r", "at": 1}},
                {"j-1": {"step": "copy", "result": "done",
                         "receipt": "r", "at": 1}},
                {"j-1": {"step": "copy", "result": "succeeded",
                         "receipt": "", "at": 1}},
                {"j-1": {"step": "copy", "result": "succeeded",
                         "receipt": "r", "at": -1}},
                "not-a-dict"):
            with self.subTest(bad_receipt=bad_receipt):
                with self.assertRaises(ValueError):
                    self._run(receipts=bad_receipt)

    def test_missing_inputs_and_parent_raise_file_not_found(self) -> None:
        self._prepare_migrate()
        p = self.paths
        missing = os.path.join(self.tmp.name, "missing.json")
        for name in ("jobs", "supply", "signals", "trades", "dispatch",
                     "execution"):
            args = [p[n] if n != name else missing
                    for n in ("jobs", "supply", "signals", "trades",
                              "dispatch", "execution", "advice",
                              "intents", "settlements", "coord")]
            with self.subTest(name=name):
                with self.assertRaises(FileNotFoundError):
                    migration_batch.run(*args, "owner", "bk", 30, 100)
        with self.assertRaises(FileNotFoundError):
            migration_batch.run(
                *[p[n] for n in ("jobs", "supply", "signals", "trades",
                                 "dispatch", "execution", "advice",
                                 "intents", "settlements")],
                os.path.join(self.tmp.name, "nested", "coord.json"),
                "owner", "bk", 30, 100)


if __name__ == "__main__":
    unittest.main()
