"""Tests for the read-only completion search and conditional responses.

Covers job-id code-point ordering, the exclusive cursor over the
filtered sequence, the outcome and exceeded (cost/carbon/any/none)
filters combined by intersection, the 1..1000 page-size validation,
independent copies, the FileNotFoundError/ValueError/OSError/KeyError
mapping and the strong-ETag conditional response builders.
"""

from __future__ import annotations

import hashlib
import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market import dispatch as dispatch_module
from carbon_market import execution as execution_module
from carbon_market import execution_sync as execution_sync_module
from carbon_market import jobs as jobs_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.completion import (complete, get, get_response, search,
                                      search_response)
from carbon_market.market import clear_live


def _resource(resource_id: str = "r-1", region: str = "eu-north",
              **overrides: object) -> dict[str, object]:
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
    return resource


def _signal(region: str = "eu-north", **overrides: object) -> dict[str, object]:
    signal: dict[str, object] = {
        "region": region,
        "observed": 0,
        "expires": 500,
        "mix": {"solar": 10000},
        "unit_cost": 5,
        "carbon_intensity": 8,
    }
    signal.update(overrides)
    return signal


def _job(job_id: str, **overrides: object) -> dict[str, object]:
    job: dict[str, object] = {
        "job_id": job_id,
        "work": 10,
        "deadline": 100,
        "regions": ["eu-north"],
        "residency": ["eu-north"],
        "max_cost": 1000,
        "carbon_cap": 1000,
    }
    job.update(overrides)
    return job


class CompletionSearchTest(unittest.TestCase):
    # job id -> (outcome, actual_cost, actual_carbon, exceeded set)
    _FIXTURE = {
        "j-1": ("succeeded", 90, 120, frozenset()),
        "j-2": ("failed", 1001, 120, frozenset(("cost",))),
        "j-3": ("succeeded", 1001, 1001,
                frozenset(("cost", "carbon"))),
        "j-4": ("succeeded", 90, 1001, frozenset(("carbon",))),
        "j-5": ("succeeded", 90, 90, frozenset()),
        "j-东": ("failed", 1001, 1001,
                frozenset(("cost", "carbon"))),
    }

    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        base = self.tmp.name
        self.jobs = os.path.join(base, "jobs.json")
        self.supply = os.path.join(base, "supply.json")
        self.signals = os.path.join(base, "signals.json")
        self.trades = os.path.join(base, "trades.json")
        self.dispatch = os.path.join(base, "dispatch.json")
        self.execution = os.path.join(base, "execution.json")
        self.sync = os.path.join(base, "sync.json")
        self.completions = os.path.join(base, "completions.json")
        jobs_module.submit(self.jobs, _job("placeholder"), "jk-0")
        resources_module.publish(self.supply, _resource(), "rk-1")
        signals_module.publish(self.signals, _signal(), "sk-1")

    def _finish_job(self, index: int, job_id: str) -> None:
        jobs_module.submit(self.jobs, _job(job_id), f"jk-{index}")
        clear_live(self.jobs, self.supply, self.signals, self.trades,
                   job_id, f"t-{index}", 10)
        dispatch_module.commit(self.jobs, self.supply, self.trades,
                               self.dispatch, job_id, f"d-{index}", 10)
        dispatch_module.claim(self.dispatch, job_id, f"c-{index}",
                              f"owner-{index}", 30, 60)
        execution_module.plan(self.jobs, self.supply, self.trades,
                              self.dispatch, self.execution, job_id,
                              f"p-{index}", f"owner-{index}", None, 61)
        execution_module.record(self.execution, job_id, 1, f"s1-{index}",
                                f"owner-{index}", "stage", "succeeded",
                                "staged", 62)
        execution_module.record(self.execution, job_id, 1, f"s2-{index}",
                                f"owner-{index}", "start", "succeeded",
                                "started", 63)
        execution_sync_module.run(self.execution, self.dispatch, self.sync,
                                  "sync-1", f"b-{index}", 70 + index, 50)

    def _populate(self) -> None:
        for index, (job_id, (outcome, cost, carbon, _flags)) \
                in enumerate(sorted(self._FIXTURE.items()), start=1):
            self._finish_job(index, job_id)
            complete(self.jobs, self.supply, self.signals, self.trades,
                     self.dispatch, self.execution, self.completions,
                     job_id, f"x-{index}", 80 + index, outcome, cost,
                     carbon)

    # -- ordering and page object --------------------------------------------

    def test_entries_are_ordered_by_job_id_code_point(self) -> None:
        self._populate()
        page = search(self.completions)
        self.assertEqual(list(page), ["entries", "next"])
        self.assertEqual([job_id for job_id, _ in page["entries"]],
                         ["j-1", "j-2", "j-3", "j-4", "j-5", "j-东"])
        # Every entry is a [job_id, record] pair and the record matches
        # get() for that job.
        for job_id, record in page["entries"]:
            self.assertEqual(record["job_id"], job_id)
            self.assertEqual(record, get(self.completions, job_id))
        self.assertIsNone(page["next"])

    def test_entries_are_independent_copies(self) -> None:
        self._populate()
        page = search(self.completions)
        page["entries"][0][1]["actual_cost"] = 999999
        page["entries"][0][1]["current"]["version"] = 99
        again = search(self.completions)
        self.assertEqual(again["entries"][0][1]["actual_cost"], 90)
        self.assertEqual(again["entries"][0][1]["current"]["version"], 1)

    def test_pagination_is_exclusive_with_one_extra_lookahead(self) -> None:
        self._populate()
        page = search(self.completions, limit=2)
        self.assertEqual([j for j, _ in page["entries"]], ["j-1", "j-2"])
        self.assertEqual(page["next"], "j-2")
        page = search(self.completions, cursor="j-2", limit=2)
        self.assertEqual([j for j, _ in page["entries"]], ["j-3", "j-4"])
        self.assertEqual(page["next"], "j-4")
        page = search(self.completions, cursor="j-4", limit=2)
        self.assertEqual([j for j, _ in page["entries"]], ["j-5", "j-东"])
        self.assertIsNone(page["next"])
        # Walking one item at a time reaches the same full sequence.
        seen: list[str] = []
        cursor: str | None = None
        while True:
            page = search(self.completions, cursor=cursor, limit=1)
            seen.extend(job_id for job_id, _ in page["entries"])
            if page["next"] is None:
                break
            cursor = page["next"]
        self.assertEqual(seen,
                         ["j-1", "j-2", "j-3", "j-4", "j-5", "j-东"])

    def test_empty_page_past_the_end(self) -> None:
        self._populate()
        page = search(self.completions, cursor="z")
        self.assertEqual(page, {"entries": [], "next": None})

    def test_default_limit_is_100(self) -> None:
        self._populate()
        self.assertEqual(len(search(self.completions)["entries"]), 6)
        self.assertEqual(len(search(self.completions, limit=1000)["entries"]),
                         6)

    # -- filters ---------------------------------------------------------------

    def test_outcome_filter(self) -> None:
        self._populate()
        page = search(self.completions, outcome="failed")
        self.assertEqual([j for j, _ in page["entries"]], ["j-2", "j-东"])
        for _job_id, record in page["entries"]:
            self.assertEqual(record["outcome"], "failed")
        page = search(self.completions, outcome="succeeded")
        self.assertEqual([j for j, _ in page["entries"]],
                         ["j-1", "j-3", "j-4", "j-5"])

    def test_exceeded_filters(self) -> None:
        self._populate()
        self.assertEqual(
            [j for j, _ in search(self.completions,
                                  exceeded="cost")["entries"]],
            ["j-2", "j-3", "j-东"])
        self.assertEqual(
            [j for j, _ in search(self.completions,
                                  exceeded="carbon")["entries"]],
            ["j-3", "j-4", "j-东"])
        self.assertEqual(
            [j for j, _ in search(self.completions,
                                  exceeded="any")["entries"]],
            ["j-2", "j-3", "j-4", "j-东"])
        self.assertEqual(
            [j for j, _ in search(self.completions,
                                  exceeded="none")["entries"]],
            ["j-1", "j-5"])

    def test_filters_intersect_and_cursor_runs_on_filtered_sequence(self):
        self._populate()
        # succeeded AND any flag -> j-3 (both flags) and j-4 (carbon).
        page = search(self.completions, outcome="succeeded",
                      exceeded="any")
        self.assertEqual([j for j, _ in page["entries"]], ["j-3", "j-4"])
        # failed AND none -> nothing.
        page = search(self.completions, outcome="failed", exceeded="none")
        self.assertEqual(page, {"entries": [], "next": None})
        # The cursor is applied to the filtered sequence: j-3 is filtered
        # out by exceeded=none, so the cursor at it resumes at j-5, the
        # next matching job.
        page = search(self.completions, cursor="j-3", exceeded="none",
                      limit=1)
        self.assertEqual([j for j, _ in page["entries"]], ["j-5"])
        self.assertIsNone(page["next"])
        # Non-matching records never consume page capacity.
        page = search(self.completions, exceeded="none", limit=1)
        self.assertEqual([j for j, _ in page["entries"]], ["j-1"])
        self.assertEqual(page["next"], "j-1")

    def test_cursor_need_not_name_an_existing_job(self) -> None:
        self._populate()
        # A cursor between existing jobs need not match a job id.
        page = search(self.completions, cursor="j-9")
        self.assertEqual([j for j, _ in page["entries"]], ["j-东"])

    # -- argument validation ---------------------------------------------------

    def test_invalid_arguments_raise_value_error(self) -> None:
        self._populate()
        with self.assertRaises(ValueError):
            search("", )
        with self.assertRaises(ValueError):
            search(self.completions, cursor="")
        with self.assertRaises(ValueError):
            search(self.completions, cursor=5)
        for bad_limit in (0, 1001, -1, True, 1.5, "10"):
            with self.subTest(bad_limit=bad_limit):
                with self.assertRaises(ValueError):
                    search(self.completions, limit=bad_limit)
        with self.assertRaises(ValueError):
            search(self.completions, outcome="nope")
        for bad in ("both", "", "COST", "all", 5):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    search(self.completions, exceeded=bad)

    # -- file errors ------------------------------------------------------------

    def test_missing_ledger_is_file_not_found(self) -> None:
        missing = os.path.join(self.tmp.name, "missing.json")
        with self.assertRaises(FileNotFoundError):
            search(missing)
        with self.assertRaises(FileNotFoundError):
            search_response(missing)
        with self.assertRaises(FileNotFoundError):
            get_response(missing, "j-1")

    def test_corrupt_ledger_is_value_error(self) -> None:
        self._populate()
        raw = Path(self.completions).read_bytes()
        try:
            with open(self.completions, "wb") as handle:
                handle.write(b"{not json\n")
            with self.assertRaises(ValueError):
                search(self.completions)
            with self.assertRaises(ValueError):
                search_response(self.completions)
            with self.assertRaises(ValueError):
                get_response(self.completions, "j-1")
        finally:
            with open(self.completions, "wb") as handle:
                handle.write(raw)

    def test_other_read_failure_is_os_error(self) -> None:
        # The ledger path is a directory: the lock companion opens but
        # reading the ledger itself fails with an OSError that is not a
        # FileNotFoundError.
        os.mkdir(os.path.join(self.tmp.name, "dir.json"))
        broken = os.path.join(self.tmp.name, "dir.json")
        with self.assertRaises(OSError):
            search(broken)
        with self.assertRaises(OSError):
            get_response(broken, "j-1")

    def test_get_unknown_job_still_raises_key_error(self) -> None:
        self._populate()
        with self.assertRaises(KeyError):
            get(self.completions, "j-nope")
        with self.assertRaises(KeyError):
            get_response(self.completions, "j-nope")

    # -- conditional response builders ----------------------------------------

    def _compact(self, payload: object) -> bytes:
        return json.dumps(payload, ensure_ascii=False,
                          separators=(",", ":")).encode("utf-8")

    def test_get_response_bytes_and_etag(self) -> None:
        self._populate()
        body, etag = get_response(self.completions, "j-1")
        self.assertEqual(body, self._compact(get(self.completions, "j-1")))
        self.assertEqual(etag, hashlib.sha256(body).hexdigest())
        # A matching condition yields the 304 pair; a non-matching one
        # the full body.
        none_body, same_etag = get_response(self.completions, "j-1", etag)
        self.assertIsNone(none_body)
        self.assertEqual(same_etag, etag)
        other = "0" * 64
        body2, etag2 = get_response(self.completions, "j-1", other)
        self.assertEqual(body2, body)
        self.assertEqual(etag2, etag)

    def test_search_response_matches_search_page(self) -> None:
        self._populate()
        body, etag = search_response(self.completions, limit=2,
                                     exceeded="any")
        page = search(self.completions, limit=2, exceeded="any")
        self.assertEqual(body, self._compact(page))
        self.assertEqual(etag, hashlib.sha256(body).hexdigest())
        none_body, same_etag = search_response(
            self.completions, limit=2, exceeded="any", condition=etag)
        self.assertIsNone(none_body)
        self.assertEqual(same_etag, etag)

    def test_response_builders_reject_bad_conditions(self) -> None:
        self._populate()
        for bad in ("", "abc", "0" * 63, "0" * 65, "g" * 64,
                    "A" * 64, 64):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    get_response(self.completions, "j-1", bad)
                with self.assertRaises(ValueError):
                    search_response(self.completions, condition=bad)

    def test_non_ascii_body_is_written_through(self) -> None:
        self._populate()
        body, _etag = get_response(self.completions, "j-东")
        self.assertIn("东".encode("utf-8"), body)
        self.assertFalse(body.endswith(b"\n"))
        self.assertNotIn(b"\\u", body)


if __name__ == "__main__":
    unittest.main()
