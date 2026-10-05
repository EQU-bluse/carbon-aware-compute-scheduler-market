"""Subprocess worker for the lifecycle persistence race tests.

Invoked as ``python -m tests._lifecycle_worker <module> <dir>``; the
parent stages two workers issuing the same idempotent first-serve call
concurrently, so the flock must serialize them into exactly one create
and one replay. Prints only ``True`` or ``False`` (the created flag).
"""

from __future__ import annotations

import json
import os
import sys

from carbon_market import completion as completion_module
from carbon_market import dispatch as dispatch_module
from carbon_market import execution as execution_module


def _paths(base: str) -> dict[str, str]:
    return {name: os.path.join(base, name) for name in (
        "jobs.json", "supply.json", "signals.json", "clear.json",
        "dispatch.json", "execution.json", "sync.json")}


def _dispatch(base: str) -> bool:
    paths = _paths(base)
    _decision, created = dispatch_module.commit(
        paths["jobs.json"], paths["supply.json"], paths["clear.json"],
        os.path.join(base, "race-dispatch.json"), "j-a", "rk-race", 10)
    return created


def _execution(base: str) -> bool:
    paths = _paths(base)
    _plan, created = execution_module.plan(
        paths["jobs.json"], paths["supply.json"], paths["clear.json"],
        paths["dispatch.json"], os.path.join(base, "race-execution.json"),
        "j-b", "pk-race", "owner-b", None, 61)
    return created


def _completion(base: str) -> bool:
    paths = _paths(base)
    _record, created = completion_module.complete(
        paths["jobs.json"], paths["supply.json"], paths["signals.json"],
        paths["clear.json"], paths["dispatch.json"],
        paths["execution.json"],
        os.path.join(base, "race-completion.json"),
        "j-d", "xk-race", 70, "succeeded", 1, 1)
    return created


_CALLS = {
    "dispatch": _dispatch,
    "execution": _execution,
    "completion": _completion,
}


def main(argv: list[str]) -> int:
    module, base = argv[1], argv[2]
    created = _CALLS[module](base)
    sys.stdout.write(json.dumps(created))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
