"""Tests for the read-only completion.search paginated query.

Covers the job-id code-point ordering, the exclusive cursor, the page
size rules, the outcome and exceeded filters (combined by logical AND),
the error mapping (ValueError for invalid arguments or a corrupt
ledger, FileNotFoundError for a missing ledger) and the conditional
response builders the HTTP endpoint serves.
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


def _resource(resource_id: str = "r-1", **overrides: object) -> dict:
    resource: dict[str, object] = {
        "resource_id": resource_id,
        "region": "eu-north",
        "capacity": 1000,
        "start": 0,
        "end": 500,
        "unit_cost": 5,
        "carbon_intensity": 8,
        "residency": ["eu-north"],
    }
    resource.update(overrides)
    return resource


def _signal() -> dict:
    return {
        "region": "eu-north",
        "observed": 0,
        "expires": 500,
        "mix": {"solar": 10000},
        "unit_cost": 5,
        "carbon_intensity": 8,
    }


def _job(job_id: str, **overrides: object) -> dict:
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
        resources_module.publish(self.supply, _resource(), "rk-1")
        signals_module.publish(self.signals, _signal(), "sk-1")

    def _finish_job(self, job_id: str, index: int) -> None:
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

    def _complete(self, job_id: str, index: int, *,
                  outcome: str = "succeeded", actual_cost: int = 90,
                  actual_carbon: int = 120) -> dict:
        self._finish_job(job_id, index)
        record, created = complete(
            self.jobs, self.supply, self.signals, self.trades,
            self.dispatch, self.execution, self.completions, job_id,
            f"x-{index}", 200, outcome, actual_cost, actual_carbon)
        self.assertTrue(created)
        return record

    def _populate(self) -> None:
        # j-a: nothing exceeded; j-b: cost exceeded; j-c: carbon
        # exceeded; j-d: both exceeded. Outcomes alternate.
        self._complete("j-a", 1)
        self._complete("j-b", 2, outcome="failed", actual_cost=1001)
        self._complete("j-c", 3, actual_carbon=1001)
        self._complete("j-d", 4, outcome="failed", actual_cost=1001,
                       actual_carbon=1001)

    # -- ordering and pagination --------------------------------------------

    def test_entries_follow_job_id_code_point_order(self) -> None:
        self._complete("j-b", 1)
        self._complete("j-ä", 2)
        self._complete("j-a", 3)
        self._complete("j-1", 4)
        page = search(self.completions)
        self.assertEqual([entry["job_id"] for entry in page["entries"]],
                         ["j-1", "j-a", "j-b", "j-ä"])
        self.assertIsNone(page["next"])
        for entry in page["entries"]:
            self.assertEqual(list(entry), ["job_id", "record"])
            self.assertEqual(entry["record"]["job_id"], entry["job_id"])
            self.assertEqual(list(entry["record"]),
                             ["job_id", "at", "outcome", "actual_cost",
                              "actual_carbon", "generation", "current",
                              "cost_exceeded", "carbon_exceeded"])

    def test_pagination_walks_the_filtered_sequence(self) -> None:
        self._populate()
        first = search(self.completions, limit=3)
        self.assertEqual([e["job_id"] for e in first["entries"]],
                         ["j-a", "j-b", "j-c"])
        self.assertEqual(first["next"], "j-c")
        second = search(self.completions, cursor=first["next"], limit=3)
        self.assertEqual([e["job_id"] for e in second["entries"]], ["j-d"])
        self.assertIsNone(second["next"])
        # A cursor past every job id yields an empty last page.
        third = search(self.completions, cursor="zz")
        self.assertEqual(third, {"entries": [], "next": None})

    def test_default_page_size_is_100(self) -> None:
        self._populate()
        self.assertEqual(len(search(self.completions)["entries"]), 4)

    def test_cursor_need_not_name_a_record(self) -> None:
        self._populate()
        page = search(self.completions, cursor="j-b0")
        self.assertEqual([e["job_id"] for e in page["entries"]],
                         ["j-c", "j-d"])

    def test_returned_records_are_independent_copies(self) -> None:
        self._populate()
        page = search(self.completions)
        page["entries"][0]["record"]["actual_cost"] = -1
        self.assertEqual(search(self.completions)["entries"][0]
                         ["record"]["actual_cost"], 90)
        self.assertEqual(get(self.completions, "j-a")["actual_cost"], 90)

    # -- filters ---------------------------------------------------------------

    def test_outcome_filter(self) -> None:
        self._populate()
        page = search(self.completions, outcome="failed")
        self.assertEqual([e["job_id"] for e in page["entries"]],
                         ["j-b", "j-d"])
        page = search(self.completions, outcome="succeeded")
        self.assertEqual([e["job_id"] for e in page["entries"]],
                         ["j-a", "j-c"])

    def test_exceeded_filter(self) -> None:
        self._populate()
        cases = {
            "cost": ["j-b", "j-d"],
            "carbon": ["j-c", "j-d"],
            "any": ["j-b", "j-c", "j-d"],
            "none": ["j-a"],
        }
        for exceeded, expected in cases.items():
            with self.subTest(exceeded=exceeded):
                page = search(self.completions, exceeded=exceeded)
                self.assertEqual([e["job_id"] for e in page["entries"]],
                                 expected)

    def test_filters_combine_by_intersection(self) -> None:
        self._populate()
        page = search(self.completions, outcome="failed", exceeded="any")
        self.assertEqual([e["job_id"] for e in page["entries"]],
                         ["j-b", "j-d"])
        page = search(self.completions, outcome="failed",
                      exceeded="carbon")
        self.assertEqual([e["job_id"] for e in page["entries"]], ["j-d"])
        page = search(self.completions, outcome="succeeded",
                      exceeded="none")
        self.assertEqual([e["job_id"] for e in page["entries"]], ["j-a"])

    def test_cursor_applies_to_the_filtered_sequence(self) -> None:
        self._populate()
        # j-b is filtered out; a cursor naming it still selects the
        # remaining failed jobs strictly past it.
        page = search(self.completions, outcome="failed", cursor="j-b")
        self.assertEqual([e["job_id"] for e in page["entries"]], ["j-d"])
        # Filtered-out records never consume page capacity.
        page = search(self.completions, exceeded="none", limit=1)
        self.assertEqual([e["job_id"] for e in page["entries"]], ["j-a"])
        self.assertIsNone(page["next"])

    # -- argument and ledger errors ---------------------------------------------

    def test_invalid_arguments_raise_value_error(self) -> None:
        self._populate()
        bad_calls = (
            {"completions": ""},
            {"cursor": ""},
            {"cursor": 1},
            {"limit": 0},
            {"limit": 1001},
            {"limit": True},
            {"limit": "10"},
            {"outcome": "unknown"},
            {"exceeded": "both"},
            {"exceeded": ""},
        )
        for overrides in bad_calls:
            with self.subTest(overrides=overrides):
                kwargs: dict[str, object] = {
                    "completions": self.completions}
                kwargs.update(overrides)
                with self.assertRaises(ValueError):
                    search(**kwargs)

    def test_missing_ledger_is_file_not_found(self) -> None:
        with self.assertRaises(FileNotFoundError):
            search(self.completions)
        # ... while get keeps its KeyError contract for the same file.
        with self.assertRaises(KeyError):
            get(self.completions, "j-a")

    def test_corrupt_ledger_is_value_error(self) -> None:
        self._populate()
        Path(self.completions).write_bytes(b"{not json")
        with self.assertRaises(ValueError):
            search(self.completions)

    # -- conditional response builders -----------------------------------------

    def test_get_response_matches_get_and_digests_the_bytes(self) -> None:
        self._populate()
        body, etag = get_response(self.completions, "j-a")
        self.assertIsNotNone(body)
        self.assertEqual(json.loads(body), get(self.completions, "j-a"))
        self.assertEqual(etag, hashlib.sha256(body).hexdigest())
        self.assertFalse(body.endswith(b"\n"))
        # A matching condition suppresses the body, not the tag.
        self.assertEqual(get_response(self.completions, "j-a", etag),
                         (None, etag))

    def test_get_response_error_mapping(self) -> None:
        with self.assertRaises(FileNotFoundError):
            get_response(self.completions, "j-a")
        self._populate()
        with self.assertRaises(KeyError):
            get_response(self.completions, "j-nope")
        with self.assertRaises(ValueError):
            get_response(self.completions, "j-a", "not-a-digest")

    def test_search_response_matches_search_under_the_lock(self) -> None:
        self._populate()
        body, etag = search_response(self.completions, limit=2)
        self.assertIsNotNone(body)
        self.assertEqual(json.loads(body),
                         search(self.completions, limit=2))
        self.assertEqual(etag, hashlib.sha256(body).hexdigest())
        self.assertEqual(
            search_response(self.completions, limit=2, condition=etag),
            (None, etag))
        with self.assertRaises(FileNotFoundError):
            search_response(os.path.join(self.tmp.name, "missing.json"))


if __name__ == "__main__":
    unittest.main()
