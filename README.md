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

## Proof export endpoint

```bash
python -m carbon_market serve --host 127.0.0.1 --port 8000 --audit PATH --token TOKEN --checkpoint CPATH
```

`--checkpoint` enables `GET /audit/proof`, the online export of `audit_proof.export` against the fixed checkpoint `CPATH`; it may only be given together with `--audit` (alone, empty or repeated it is a usage error, exit status 2). Without it the proof path stays a plain 404 like any other unknown path. Clients can never select the audit or checkpoint file through the query.

The same authorization as `GET /audit` applies, in the same order (401/403, 400 `invalid_request`, scoped filters 403, 503 `auth_unavailable`). The query takes a mandatory non-empty `generation` name, an optional `final` flag (exactly `true` or `false`, defaulting to `false`) and the usual `cursor`, `limit`, `op`, `stage` and `key` filters. A successful export answers 200 with the offline-savable proof object, its page computed from the same journal snapshot the proof seals. A missing journal or checkpoint parent directory gets 404 `proof_not_found` (the real paths never leak); an invalid chain, checkpoint, generation state or growth relation gets 409 `proof_invalid` and appends no anchor; locking or I/O failures get 503 `proof_unavailable` with the old checkpoint preserved.

## Checkpoint download endpoint

`--checkpoint` also enables `GET /audit/checkpoint`, a read-only download of the checkpoint snapshot fixed at startup. The path takes no parameters and clients can never select or probe the checkpoint location.

The same authorization as `GET /audit` applies, in the same order (401/403, 400 `invalid_request`, 503 `auth_unavailable`); under `--auth` only a token whose operation, stage and history-key scopes are all `"*"` may download — any restricted scope gets 403. A successful download answers 200 with the checkpoint's raw UTF-8 bytes exactly as read and validated under the checkpoint's shared lock (never reordered or reserialized), `Content-Type: application/json` and a strong `ETag` holding the lowercase SHA-256 of the response bytes.

A request may carry one `If-None-Match` header with a single quoted 64-digit lowercase digest; a value equal to the current ETag gets 304 with an empty body and the same `ETag`, any other valid value gets the full 200. A missing, blank, repeated or malformed conditional header gets 400 `invalid_request` without the checkpoint ever being opened. A missing checkpoint gets 404 `checkpoint_not_found`; invalid UTF-8, JSON, version, field order, anchor or digest chain gets 409 `checkpoint_invalid`; other I/O failures get 503 `checkpoint_unavailable`.

## Test

```bash
python -m unittest discover -s tests -v
```
