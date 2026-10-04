"""Process-local, read-only request metrics for the HTTP service.

A :class:`Metrics` instance is created once, right when the service
starts listening, and dies with the process: counters start at zero, a
restart clears them and no file is ever read or written. Every request
whose status code is decided and whose response has been constructed is
recorded exactly once via :meth:`record` -- including the metrics
request's own rejections, business refusals, authorization failures and
unknown paths; the successful ``GET /metrics`` read is never recorded in
the snapshot it returns.

Requests are classified by the public fixed path with the query string
removed: one count per known endpoint, every other path under ``other``,
so arbitrary URLs can never create unbounded dimensions. :meth:`snapshot`
serializes the whole view under one lock: the concurrent writer either
finished before or starts after the copy, so a reader always observes one
complete point in time -- no lost or double count, and the per-route and
per-status subtotals always add up to the total.
"""

from __future__ import annotations

import threading
from collections import Counter

__all__ = ["Metrics", "OTHER"]

#: Bucket for every path that is not one of the fixed public routes.
OTHER = "other"


class Metrics:
    """Locked per-route, per-status request counters for one process."""

    def __init__(self, started: int) -> None:
        # ``started`` is the non-negative Unix second the service began
        # listening; the counters themselves start at zero.
        self._started = started
        self._lock = threading.Lock()
        self._total = 0
        self._routes: dict[str, Counter[str]] = {}

    def record(self, route: str, status: int) -> None:
        # Exactly one call per completed request; the lock makes the
        # total increment and the bucket increment one atomic step.
        with self._lock:
            self._total += 1
            bucket = self._routes.get(route)
            if bucket is None:
                bucket = Counter()
                self._routes[route] = bucket
            bucket[str(status)] += 1

    def snapshot(self) -> dict[str, object]:
        # Copy the complete state under the lock so the totals of the
        # returned view are consistent with one single instant.
        with self._lock:
            routes: dict[str, dict[str, object]] = {}
            for route in sorted(self._routes):
                bucket = self._routes[route]
                # Status keys are decimal code strings, ordered by the
                # numeric value, not lexicographically; the route total
                # is the sum of its status counts and all route totals
                # add up to the grand total.
                statuses = {
                    code: bucket[code]
                    for code in sorted(bucket, key=int)
                }
                routes[route] = {"total": sum(statuses.values()),
                                 "statuses": statuses}
            return {
                "version": 1,
                "started": self._started,
                "total": self._total,
                "routes": routes,
            }
