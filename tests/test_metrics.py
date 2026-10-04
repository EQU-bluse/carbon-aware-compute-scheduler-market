"""Unit tests for the process-local metrics registry."""

from __future__ import annotations

import json
import threading
import unittest

from carbon_market.metrics import OTHER_ROUTE, VERSION, Metrics


class MetricsSnapshotTest(unittest.TestCase):
    def test_starts_zeroed(self) -> None:
        body = json.loads(Metrics(100).snapshot())
        self.assertEqual(
            body, {"version": 1, "started": 100, "total": 0,
                   "routes": {}})

    def test_version_is_one_and_started_is_reflected(self) -> None:
        body = json.loads(Metrics(0).snapshot())
        self.assertEqual(body["version"], VERSION)
        self.assertEqual(body["version"], 1)
        self.assertIsInstance(body["started"], int)

    def test_bad_started_is_rejected(self) -> None:
        for bad in (-1, 1.5, True, False, None, "1"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    Metrics(bad)  # type: ignore[arg-type]

    def test_counts_and_layered_totals(self) -> None:
        metrics = Metrics(7)
        metrics.record("/health", 200)
        metrics.record("/health", 200)
        metrics.record("/health", 500)
        metrics.record(OTHER_ROUTE, 404)
        body = json.loads(metrics.snapshot())
        self.assertEqual(body["total"], 4)
        self.assertEqual(body["routes"]["/health"],
                         {"total": 3, "statuses": {"200": 2, "500": 1}})
        self.assertEqual(body["routes"][OTHER_ROUTE],
                         {"total": 1, "statuses": {"404": 1}})

    def test_routes_sort_by_code_point_and_statuses_numerically(self) -> None:
        metrics = Metrics(0)
        # Insert out of order; snapshot must re-sort.
        for code in (503, 200, 404, 401):
            metrics.record("/other-path", code)
        for route in ("/b", "/a", OTHER_ROUTE, "/audit", "/health"):
            metrics.record(route, 200)
        raw = metrics.snapshot().decode("utf-8")
        body = json.loads(raw)
        self.assertEqual(list(body["routes"]),
                         sorted(body["routes"]))
        self.assertEqual(
            list(body["routes"]["/other-path"]["statuses"]),
            ["200", "401", "404", "503"])
        # Status keys are decimal strings, not numbers.
        self.assertIn("200", body["routes"]["/other-path"]["statuses"])

    def test_totals_always_reconcile(self) -> None:
        metrics = Metrics(3)
        for route, status in (("/a", 200), ("/a", 404), ("/b", 200),
                              ("/b", 200), ("/b", 500), (OTHER_ROUTE, 404)):
            metrics.record(route, status)
        body = json.loads(metrics.snapshot())
        route_sum = sum(r["total"] for r in body["routes"].values())
        status_sum = sum(
            count for route in body["routes"].values()
            for count in route["statuses"].values())
        self.assertEqual(route_sum, body["total"])
        self.assertEqual(status_sum, body["total"])

    def test_compact_utf8_without_trailing_newline(self) -> None:
        metrics = Metrics(0)
        metrics.record("/health", 200)
        raw = metrics.snapshot()
        self.assertIsInstance(raw, bytes)
        self.assertFalse(raw.endswith(b"\n"))
        self.assertNotIn(b": ", raw)
        self.assertNotIn(b", ", raw)
        # Re-decoding the exact bytes yields the same object.
        self.assertEqual(json.loads(raw.decode("utf-8"))["total"], 1)


class MetricsConcurrencyTest(unittest.TestCase):
    def test_concurrent_records_are_never_lost_or_doubled(self) -> None:
        metrics = Metrics(0)
        threads_count = 8
        per_thread = 500
        barrier = threading.Barrier(threads_count)

        def worker() -> None:
            barrier.wait()
            for index in range(per_thread):
                metrics.record("/health" if index % 2 else OTHER_ROUTE,
                               200 if index % 3 else 404)

        threads = [threading.Thread(target=worker)
                   for _ in range(threads_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        body = json.loads(metrics.snapshot())
        expected = threads_count * per_thread
        self.assertEqual(body["total"], expected)
        route_sum = sum(r["total"] for r in body["routes"].values())
        status_sum = sum(
            count for route in body["routes"].values()
            for count in route["statuses"].values())
        self.assertEqual(route_sum, expected)
        self.assertEqual(status_sum, expected)

    def test_snapshot_under_contention_is_internally_consistent(self) -> None:
        metrics = Metrics(0)
        stop = threading.Event()
        errors: list[str] = []

        def reader() -> None:
            while not stop.is_set():
                body = json.loads(metrics.snapshot())
                route_sum = sum(
                    r["total"] for r in body["routes"].values())
                status_sum = sum(
                    count for route in body["routes"].values()
                    for count in route["statuses"].values())
                if route_sum != body["total"] \
                        or status_sum != body["total"]:
                    errors.append("contradictory snapshot totals")
                    return

        readers = [threading.Thread(target=reader) for _ in range(4)]
        for thread in readers:
            thread.start()

        def writer() -> None:
            for index in range(2000):
                metrics.record("/health", 200 if index % 2 else 500)

        writers = [threading.Thread(target=writer) for _ in range(4)]
        for thread in writers:
            thread.start()
        for thread in writers:
            thread.join()
        stop.set()
        for thread in readers:
            thread.join()
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
