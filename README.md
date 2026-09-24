# Carbon-aware Compute Scheduling Market

A backend service for a carbon-aware batch-compute scheduling market. The service will evolve around scheduling and migrating batch jobs using live energy mix, regional carbon intensity, execution cost, deadlines, and data-residency constraints.

## Run

Requires Python 3.11 or newer and has no third-party runtime dependencies.

```bash
python -m carbon_market serve --host 127.0.0.1 --port 8000
```

The initial public endpoint is `GET /health`, which returns JSON with `status: "ok"`.

### Audit endpoint

`GET /audit` is an authenticated, read-only view over one audit journal.
It is enabled only by passing both flags together:

```bash
python -m carbon_market serve --audit PATH --token TOKEN
```

`--audit PATH` and `--token TOKEN` are an optional pair: omit both to
leave `/audit` unrouted (it then answers like any other unknown path), or
give both as non-empty values, each at most once. Giving exactly one, an
empty value or a repeated flag is a usage error and exits with code 2.

The served path is fixed at startup; clients cannot select or probe
another path. Requests must carry the header `X-Audit-Token` exactly once
with the configured token; the value is compared in constant time. A
missing, blank or repeated header is `401`, a well-formed but wrong token
is `403`.

With valid credentials, query parameters map onto the audit search:
`cursor`, `limit` (decimal integer 1–1000, default 100), `op`
(`copy`/`restore`), `stage` (`成功`/`校验`/`执行`/`同步`/`回滚`) and
`key`; filters combine with logical AND. Duplicated, blank or unknown
parameters, and percent encoding that is not valid UTF-8, are
`400 invalid_request`. A valid request returns `200` with the search
result. A missing audit file is `404 audit_not_found`; malformed content
is `409 audit_invalid`; other file errors are `503 audit_unavailable`.
Every error body is a compact JSON object with a single string field
`error`; paths and system details are never included.


## Test

```bash
python -m unittest discover -s tests -v
```
