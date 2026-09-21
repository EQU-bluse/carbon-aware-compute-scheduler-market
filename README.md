# Carbon-aware Compute Scheduling Market

A backend service for a carbon-aware batch-compute scheduling market. The service will evolve around scheduling and migrating batch jobs using live energy mix, regional carbon intensity, execution cost, deadlines, and data-residency constraints.

## Run

Requires Python 3.11 or newer and has no third-party runtime dependencies.

```bash
python -m carbon_market serve --host 127.0.0.1 --port 8000
```

The initial public endpoint is `GET /health`, which returns JSON with `status: "ok"`.

## Job registry

`carbon_market.jobs.register(path, job, idempotency_key)` persists a job to a
JSON file and returns `(entry, created)`:

```python
from carbon_market.jobs import register

entry, created = register(
    "jobs.json",
    {
        "job_id": "job-1",
        "deadline": 1700000000,
        "energy_wh": 500,
        "residency_regions": ["eu-north", "jp-east"],
    },
    "request-abc",
)
# entry adds "state": "queued"; created is True the first time, False on replay
```

Replaying the same idempotency key with the same job (regions normalized by
Unicode code point) returns the stored entry and `False`. A key reused with a
different job, or a `job_id` already taken by another key, raises `ValueError`.
Writes are atomic, and equivalent paths (relative/absolute) share an in-process
lock.

## Test

```bash
python -m unittest discover -s tests -v
```
