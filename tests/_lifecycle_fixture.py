"""Deterministic lifecycle fixtures shared by the persistence tests.

The builder reproduces one full handover (clear -> dispatch ->
execution -> synchronization -> completion) for named jobs, so a test can
rebuild the exact ledgers and compare them byte-for-byte with the golden
files captured before the persistence refactor.
"""

from __future__ import annotations

import os

from carbon_market import dispatch as dispatch_module
from carbon_market import execution as execution_module
from carbon_market import execution_sync as execution_sync_module
from carbon_market import jobs as jobs_module
from carbon_market import market as market_module
from carbon_market import resources as resources_module
from carbon_market import signals as signals_module
from carbon_market.completion import complete


def job(job_id: str = "j-é") -> dict[str, object]:
    return {
        "job_id": job_id,
        "work": 10,
        "deadline": 100,
        "regions": ["eu-north", "us-west"],
        "residency": ["eu-north"],
        "max_cost": 1000,
        "carbon_cap": 1000,
    }


def resource(resource_id: str = "r-1") -> dict[str, object]:
    return {
        "resource_id": resource_id,
        "region": "eu-north",
        "capacity": 100,
        "start": 0,
        "end": 500,
        "unit_cost": 5,
        "carbon_intensity": 8,
        "residency": ["eu-north"],
    }


def signal() -> dict[str, object]:
    return {
        "region": "eu-north",
        "observed": 0,
        "expires": 500,
        "mix": {"solar": 10000},
        "unit_cost": 5,
        "carbon_intensity": 8,
    }


def paths(base: str) -> dict[str, str]:
    return {name: os.path.join(base, name) for name in (
        "jobs.json", "supply.json", "signals.json", "clear.json",
        "dispatch.json", "execution.json", "sync.json",
        "completion.json")}


def seed_inputs(base: str, *job_ids: str) -> dict[str, str]:
    p = paths(base)
    for index, job_id in enumerate(job_ids):
        jobs_module.submit(p["jobs.json"], job(job_id), f"jk-{index}")
    # Every job in the fixture trades the same resource version, whose
    # capacity covers all of them.
    resources_module.publish(p["supply.json"], resource("r-1"), "rk-1")
    signals_module.publish(p["signals.json"], signal(), "sk-0")
    return p


def trade(p: dict[str, str], job_id: str, key: str, at: int = 10) -> None:
    market_module.clear_live(p["jobs.json"], p["supply.json"],
                             p["signals.json"], p["clear.json"],
                             job_id, key, at)


def commit(p: dict[str, str], job_id: str, key: str, at: int = 10) -> None:
    dispatch_module.commit(p["jobs.json"], p["supply.json"], p["clear.json"],
                           p["dispatch.json"], job_id, key, at)


def claim(p: dict[str, str], job_id: str, key: str, owner: str,
          at: int = 60) -> None:
    dispatch_module.claim(p["dispatch.json"], job_id, key, owner, 30, at)


def finish(p: dict[str, str], job_id: str, key: str, owner: str,
           at: int = 70) -> None:
    dispatch_module.finish(p["dispatch.json"], job_id, key, owner,
                           "succeeded", at)


def plan(p: dict[str, str], job_id: str, key: str, owner: str,
         at: int = 61) -> None:
    execution_module.plan(p["jobs.json"], p["supply.json"], p["clear.json"],
                          p["dispatch.json"], p["execution.json"], job_id,
                          key, owner, None, at)


def steps(p: dict[str, str], job_id: str, key_prefix: str, owner: str) -> None:
    execution_module.record(p["execution.json"], job_id, 1,
                            f"{key_prefix}-s1", owner, "stage",
                            "succeeded", "staged", 62)
    execution_module.record(p["execution.json"], job_id, 1,
                            f"{key_prefix}-s2", owner, "start",
                            "succeeded", "started", 63)


def synchronize(p: dict[str, str], key: str, at: int = 70) -> None:
    execution_sync_module.run(p["execution.json"], p["dispatch.json"],
                               p["sync.json"], "sync-1", key, at, 50)


def build_completed(p: dict[str, str], job_id: str, key_prefix: str,
                    at: int = 70) -> None:
    """Drive one job to a stable terminal state without a separate
    dispatch finish event: synchronization marks the decision
    succeeded, exactly the path the golden generator exercised."""
    commit(p, job_id, f"{key_prefix}-d")
    claim(p, job_id, f"{key_prefix}-c", f"{key_prefix}-owner")
    plan(p, job_id, f"{key_prefix}-p", f"{key_prefix}-owner")
    steps(p, job_id, key_prefix, f"{key_prefix}-owner")
    synchronize(p, f"{key_prefix}-b")


def complete_job(p: dict[str, str], job_id: str, key: str,
                 at: int = 70) -> tuple[dict[str, object], bool]:
    return complete(p["jobs.json"], p["supply.json"], p["signals.json"],
                    p["clear.json"], p["dispatch.json"],
                    p["execution.json"], p["completion.json"],
                    job_id, key, at, "succeeded", 1, 1)
