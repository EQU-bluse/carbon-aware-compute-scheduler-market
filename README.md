# Carbon-aware Compute Scheduling Market

A backend service for a carbon-aware batch-compute scheduling market. The service will evolve around scheduling and migrating batch jobs using live energy mix, regional carbon intensity, execution cost, deadlines, and data-residency constraints.

## Run

Requires Python 3.11 or newer and has no third-party runtime dependencies.

```bash
python -m carbon_market serve --host 127.0.0.1 --port 8000
```

The initial public endpoint is `GET /health`, which returns JSON with `status: "ok"`.

## Audit endpoint

```bash
python -m carbon_market serve --host 127.0.0.1 --port 8000 --audit PATH --token TOKEN
```

`--audit` and `--token` form a pair: both omitted leaves `GET /audit` a plain 404 like any other unknown path, while giving only one, an empty value or a repeated option is a usage error (exit status 2). When both are given, `GET /audit` serves read-only queries against the audit journal at `PATH`; clients cannot select another file.

Requests must carry exactly one `X-Audit-Token` header whose value equals `TOKEN` (missing, blank or duplicated headers get 401, a mismatched value 403). The query string maps to `audit.search`: optional `cursor`, `limit` (a decimal integer from 1 to 1000, default 100), `op`, `stage` and `key` filters. Unknown, repeated or empty parameters get 400 `invalid_request`. A missing journal gets 404 `audit_not_found`, malformed journal content 409 `audit_invalid`, and other I/O failures 503 `audit_unavailable`.

## Test

```bash
python -m unittest discover -s tests -v
```
