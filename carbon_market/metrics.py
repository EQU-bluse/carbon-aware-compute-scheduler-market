"""Process-local, read-only request metrics for the HTTP service.

The registry is created once with the Unix second the listening socket
started serving (:data:`version` is fixed at 1) and lives only for the
lifetime of that process: counters initialize at zero, a restart clears
them, and no file is ever created or rewritten. Every request is
recorded exactly once, at the moment its status code has been decided
and its response built, under the registry lock; a snapshot is read
back under the same lock, so it always presents one complete point in
time -- no request is lost, doubled or reported from layers whose
totals do not add up.

Routes are the request's public fixed paths with the query string
stripped; anything that is not one of those paths collapses into
``other``, so an arbitrary URL can never create an unbounded set of
dimensions. The snapshot is compact UTF-8 JSON with no trailing
newline; route keys are ordered by Unicode code point and the decimal
status-code keys numerically ascending, and the totals of every layer
sum to ``total``.
"""

from __future__ import annotations

import json
import threading

__all__ = ["Metrics", "OTHER_ROUTE", "VERSION"]

# Every request whose path matches no public fixed route is counted
# here, whatever method or query string it carried.
OTHER_ROUTE = "other"
VERSION = 1


class Metrics:
    """Locked request counters for one serving process."""

    def __init__(self, started: int) -> None:
        if not isinstance(started, int) or isinstance(started, bool) \
                or started < 0:
            raise ValueError("started must be a non-negative Unix second")
        self._started = started
        self._lock = threading.Lock()
        self._total = 0
        # route -> status code -> count
        self._routes: dict[str, dict[int, int]] = {}

    @property
    def started(self) -> int:
        return self._started

    def record(self, route: str, status: int) -> None:
        # One count per decided response. The lock makes the increment
        # of the grand total, the route total and the status bucket one
        # indivisible step relative to a snapshot read.
        with self._lock:
            self._total += 1
            statuses = self._routes.setdefault(route, {})
            statuses[status] = statuses.get(status, 0) + 1

    def snapshot(self) -> bytes:
        # Build the bytes under the lock from plain copied integers, so
        # they describe one consistent point in time and the per-route
        # and per-status sums always equal total.
        with self._lock:
            routes = {
                route_name: dict(statuses)
                for route_name, statuses in self._routes.items()
            }
            body = self._build(self._started, self._total, routes)
        return body

    @staticmethod
    def _build(started: int, total: int,
               routes: dict[str, dict[int, int]]) -> bytes:
        # Field order is fixed: version, started, total, routes; each
        # route value is total then statuses. Routes are sorted by
        # Unicode code point (plain Python string order) and statuses by
        # numeric code with decimal-string keys.
        route_objects = {}
        for route_name in sorted(routes):
            statuses = routes[route_name]
            statuses_object = {
                str(code): statuses[code]
                for code in sorted(statuses)
            }
            route_objects[route_name] = {
                "total": sum(statuses.values()),
                "statuses": statuses_object,
            }
        payload = {
            "version": VERSION,
            "started": started,
            "total": total,
            "routes": route_objects,
        }
        return json.dumps(payload, ensure_ascii=False,
                          separators=(",", ":")).encode("utf-8")
