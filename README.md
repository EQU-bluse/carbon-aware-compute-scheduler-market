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

## Multi-token scoped authorization

```bash
python -m carbon_market serve --host 127.0.0.1 --port 8000 --audit PATH --auth CONFIG
```

`--auth` is an alternative to `--token`; the two are mutually exclusive, and enabling the audit endpoint requires exactly one of them (anything else is a usage error, exit status 2). `CONFIG` is a UTF-8 file whose every non-empty line is a six-element JSON array:

```json
["name", "sha256-hex", grace_until, ops, stages, keys]
```

- `name` — a unique non-empty label for the token.
- `sha256-hex` — the lowercase hexadecimal SHA-256 of the token's raw UTF-8 bytes; unique across records. The file never holds plaintext tokens.
- `grace_until` — `null` or a non-negative integer Unix second; the token stays valid while the current Unix second is not greater than it.
- `ops`, `stages`, `keys` — each either `"*"` (the filter may be omitted or take any legal value) or an array of distinct non-empty strings, in which case the request must carry that filter explicitly with a value in the array. Operation and stage scopes use the search filter domains (`copy`/`restore`; `成功`, `校验`, `执行`, `同步`, `回滚`); history keys match exactly.

The whole file is validated at startup (any error exits with status 2 without listening) and re-read on every request, so a same-directory atomic replacement rotates tokens without a restart: add the new digest and set a grace cutoff on the old one. A configuration that cannot be read or validated after startup answers 503 `auth_unavailable`. Unknown or expired tokens get 403; a valid token whose request lacks a required in-scope filter gets 403 as well, after query-parameter validation (400) and before the audit file is ever opened.

## Test

```bash
python -m unittest discover -s tests -v
```
