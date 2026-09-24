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

## Scoped multi-token authorization (hot rotation)

As an alternative to the single static `--token`, `serve` accepts `--auth PATH`; the two authorization methods are mutually exclusive, and enabling `--audit` requires exactly one of them (`--auth` without `--audit`, or both `--auth` and `--token`, exits 2). The whole file is validated at startup before the port is bound: a missing, empty or invalid file exits with status 2 and never listens.

```bash
python -m carbon_market serve --audit PATH --auth tokens.jsonl
```

The file is UTF-8 text; every non-empty line is a single JSON array of six elements:

```json
["billing", "9f86d081...", null, ["copy","restore"], "*", ["batch-42"]]
```

1. unique non-empty **name**;
2. **digest**: the lowercase hex SHA-256 of the token's original UTF-8 bytes — plaintext tokens are never stored;
3. **deadline**: `null` (never expires) or a non-negative integer of Unix seconds; the token stays valid while the current time is at most the deadline (a grace period during rotation);
4. **op scope**, 5. **stage scope**, 6. **history-key scope**: each is either `"*"` or an array of distinct non-empty strings. Op and stage values reuse the search vocabulary (`copy`/`restore`; `成功`/`校验`/`执行`/`同步`/`回滚`); key entries match the history key verbatim.

Duplicate names or digests, and any shape or value violation, make the whole file invalid.

The configuration is **re-read on every request**, so a rotation is an atomic same-directory replace (write a temp file, `os.replace`) that adds a new digest and puts a deadline on the old one — no restart, and every request is evaluated against either the complete old or the complete new file. If the file has disappeared or fails validation at request time, the response is 503 `auth_unavailable` (never a stale snapshot, never failing open).

Request handling: missing, blank or duplicated `X-Audit-Token` headers get 401; an unknown digest or a token past its deadline gets 403 (constant-time digest comparison; both cases are indistinguishable). Query parameters are validated as usual (400 `invalid_request`) before scopes are checked. An array scope requires the corresponding filter to be present explicitly with an in-scope value, while `"*"` permits omission or any legal value; an out-of-scope request gets 403 and never opens the audit journal or reveals the matched name or any configuration detail. The single-token `--token` mode behaves exactly as before, as do health checks, ordinary 404s and the audit library entry points.


## Test

```bash
python -m unittest discover -s tests -v
```
