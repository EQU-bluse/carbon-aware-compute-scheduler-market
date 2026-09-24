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

## Checkpoint snapshot endpoint

With `--checkpoint` configured, `GET /audit/checkpoint` offers the fixed checkpoint file itself as a read-only, conditionally cached download. The endpoint takes no path or query parameters: the response always comes from the one `CPATH` fixed at startup, so a client can neither select nor probe a location.

Authorization follows the same order as the other audit endpoints: a missing, blank or duplicated `X-Audit-Token` is 401 `unauthorized`; an unknown or expired token is 403 `forbidden`; an authorization configuration that cannot be read or validated at request time is 503 `auth_unavailable` with no configuration detail in the body. A multi-token record may download only when its operation, stage and history-key scopes are all `*`; any restricted scope gets 403.

The optional `If-None-Match` header may be omitted or appear exactly once as a single strong tag — one double-quoted string of 64 lowercase hexadecimal digits. A blank, duplicated or malformed conditional header gets 400 `invalid_request` and never opens the checkpoint; the same goes for any query string. The response on success (200) is the checkpoint's original UTF-8 bytes, served verbatim with `Content-Type: application/json` — never re-serialized — and an `ETag` that is the lowercase SHA-256 of the complete response bytes in strong-tag form (`"<64 hex>"`). When the conditional value equals the current ETag the answer is 304 with an empty body and the same ETag; otherwise it is 200 with the full bytes, and both decisions are made from one validated snapshot.

The bytes are read under the same checkpoint lock exports use (shared), held from open through validation and ETag computation, so a request racing an export observes only a complete old or complete new version. A missing checkpoint gets 404 `checkpoint_not_found`; bytes that are not valid UTF-8 or JSON, or a checkpoint with an illegal version, field order, anchor or digest chain, get 409 `checkpoint_invalid` with a body of only `{"error":...}`; other I/O failures get 503 `checkpoint_unavailable`. Real paths and system messages never appear in an error. Without `--checkpoint` the path remains a plain 404 like any other unknown path.

## Offline bundle verification

```bash
python -m carbon_market verify-bundle --checkpoint CPATH --proof PPATH --etag '"<64 hex>"'
```

`verify-bundle` is the offline counterpart of the checkpoint download: it verifies a saved proof (`PPATH`, the object `GET /audit/proof` returned) against the downloaded checkpoint (`CPATH`, the bytes `GET /audit/checkpoint` served) without any server. Each option must appear exactly once with a non-empty value, and `--etag` must be the single strong tag the download response carried — one double-quoted string of 64 lowercase hexadecimal digits. Any argument or tag format error exits 2 with `invalid_request` on stderr, before any input file is read.

Verification first recomputes the tag from the checkpoint's raw bytes — a mismatch fails immediately — then validates the checkpoint's full anchor chain and runs the same offline rechecks as `audit_proof.verify`: the embedded log bytes, query parameters, page, cursor and root digest are all recomputed, and the proof's generation, anchor and closed state must come from that one verified checkpoint snapshot. The checkpoint is read once under the shared lock, so a same-directory atomic replacement racing the read can only yield a self-consistent old or new combination, never a mix.

On success stdout carries one compact UTF-8 JSON line (non-ASCII written through, exactly one trailing newline) with `valid`, `etag`, `generation`, `closed`, `events` and `next` in that order — `valid` always `true`, the rest from the verified tag and proof — and exits 0 with stderr empty. Failures write nothing to stdout and report a compact single-line object on stderr: encoding, JSON (negative-zero or non-finite literals included) or public-structure errors exit 3 with `invalid_bundle`; tag, digest-chain, generation, anchor, closed-state or page mismatches exit 4 with `verification_failed`; a missing file or any other locking, open or read error exits 5 with `bundle_unavailable`. Error objects never leak paths, system messages or input content. The command is read-only: the inputs are never modified, and the HTTP and library entries are unaffected.

## Test

```bash
python -m unittest discover -s tests -v
```
