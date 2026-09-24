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

The same authorization as `GET /audit` applies, in the same order (401/403, 400 `invalid_request`, scoped filters 403, 503 `auth_unavailable`). The query takes a mandatory non-empty `generation` name, an optional `final` flag (exactly `true` or `false`, defaulting to `false`) and the usual `cursor`, `limit`, `op`, `stage` and `key` filters. A successful export answers 200 with the offline-savable proof object, its page computed from the same journal snapshot the proof seals. The proof object is version 2 and ends with `checkpoint_etag`: the strong entity tag of the checkpoint's raw bytes after this export's anchor commit — one double-quoted string of 64 lowercase hexadecimal digits, the SHA-256 of the complete bytes, exactly the computation and quoting the checkpoint download serves — so every proof is bound to precisely one checkpoint snapshot, and any later anchor, closing or new generation rebinds the proofs that follow it. A missing journal or checkpoint parent directory gets 404 `proof_not_found` (the real paths never leak); an invalid chain, checkpoint, generation state or growth relation gets 409 `proof_invalid` and appends no anchor; locking or I/O failures get 503 `proof_unavailable` with the old checkpoint preserved.

## Checkpoint snapshot endpoint

With `--checkpoint` configured, `GET /audit/checkpoint` offers the fixed checkpoint file itself as a read-only, conditionally cached download. The endpoint takes no path or query parameters: the response always comes from the one `CPATH` fixed at startup, so a client can neither select nor probe a location.

Authorization follows the same order as the other audit endpoints: a missing, blank or duplicated `X-Audit-Token` is 401 `unauthorized`; an unknown or expired token is 403 `forbidden`; an authorization configuration that cannot be read or validated at request time is 503 `auth_unavailable` with no configuration detail in the body. A multi-token record may download only when its operation, stage and history-key scopes are all `*`; any restricted scope gets 403.

The optional `If-None-Match` header may be omitted or appear exactly once as a single strong tag — one double-quoted string of 64 lowercase hexadecimal digits. A blank, duplicated or malformed conditional header gets 400 `invalid_request` and never opens the checkpoint; the same goes for any query string. The response on success (200) is the checkpoint's original UTF-8 bytes, served verbatim with `Content-Type: application/json` — never re-serialized — and an `ETag` that is the lowercase SHA-256 of the complete response bytes in strong-tag form (`"<64 hex>"`). When the conditional value equals the current ETag the answer is 304 with an empty body and the same ETag; otherwise it is 200 with the full bytes, and both decisions are made from one validated snapshot.

The bytes are read under the same checkpoint lock exports use (shared), held from open through validation and ETag computation, so a request racing an export observes only a complete old or complete new version. A missing checkpoint gets 404 `checkpoint_not_found`; bytes that are not valid UTF-8 or JSON, or a checkpoint with an illegal version, field order, anchor or digest chain, get 409 `checkpoint_invalid` with a body of only `{"error":...}`; other I/O failures get 503 `checkpoint_unavailable`. Real paths and system messages never appear in an error. Without `--checkpoint` the path remains a plain 404 like any other unknown path.

## Offline bundle verification

```bash
python -m carbon_market verify-bundle --checkpoint CPATH --proof PPATH --etag ETAG [--trust-dir PATH]
```

`verify-bundle` verifies a downloaded checkpoint/proof pair entirely offline; it never touches the journal and never rewrites its inputs. All three options must appear exactly once with non-empty values, and `--etag` must be a single strong tag — one double-quoted string of 64 lowercase hexadecimal digits, exactly as the download's `ETag` header carried it. Any argument or tag-format error exits with status 2 and `{"error":"invalid_request"}` on stderr before either file is read.

Verification first recomputes the tag from the checkpoint's raw bytes — a mismatch fails immediately — then reads both files as UTF-8 JSON (negative-zero and non-finite number literals are format errors), revalidates the checkpoint's complete anchor chain, and applies the same offline proof checks as `audit_proof.verify`: embedded log bytes, query parameters, recomputed page, cursor and root digest, with the generation, anchor and closed state taken from the checkpoint snapshot in hand. The passed `--etag`, the recomputed tag and the proof's bound `checkpoint_etag` must all three agree, so an old proof against a new checkpoint, a new proof against an old checkpoint, or a tag carried by another download response is a verification failure. Proofs without the snapshot binding (version 1) are not verifiable offline and count as incomplete proof structures. Everything happens under the checkpoint's shared lock on the one snapshot that was read, so a same-directory atomic replacement racing the verification can only yield a self-consistent old or new combination, never a mix of two versions.

The optional `--trust-dir PATH` (at most once, non-empty) retains a persistent snapshot sequence on this machine so a complete old bundle can no longer be replayed whole. Without it the command is exactly the single-bundle verification above: no directory is created and the exit statuses are unchanged. With it, the single-bundle verification runs first; then, under the trust directory's exclusive cross-process lock, the verified checkpoint is compared with the retained sequence and the chain head advances. A first use with a not-yet-existing directory establishes the trust anchor from the verified checkpoint and saves its original bytes. A later bundle carrying the head's strong tag is an idempotent replay: the proof is fully rechecked, the directory is not rewritten. A new tag is accepted only when its checkpoint is a strictly append-only successor of the latest trusted one — every retained generation, anchor, field and value unchanged, only legal generations or anchors appended, never a fork. Each sequence node binds the current tag, the predecessor tag, the checkpoint bytes digest and the previous node digest, and the chain head names only the last complete node, so a restarted process still verifies the true predecessor relation. An already retained older version, a fork that deleted or rewrote old anchors, or a bundle that cannot continue the chain is rejected as a rollback (`verification_failed`, status 4); malformed trust metadata (encoding, JSON, negative-zero, field order, version or structure) is `invalid_bundle` (status 3); a tampered node, chain digest or head relation is a verification failure and is never repaired or overwritten with the current bundle. Directory updates commit through same-directory temporary files, fsync, atomic replace and a directory fsync, so any failure preserves the pre-call chain head, unreferenced complete nodes are reused on re-entry, and leftover fragments never participate; concurrent verifications of different versions let only the successor advance, and a later-arriving older version fails even when self-consistent.

On success stdout carries one compact UTF-8 JSON line — `{"valid":true,"etag":...,"generation":...,"closed":...,"events":...,"next":...}` with non-ASCII written through and exactly one trailing newline — the exit status is 0 and stderr stays empty. Failures print no partial stdout, only a single compact failure object on stderr: status 2 `invalid_request` (argument or tag format), 3 `invalid_bundle` (encoding, JSON or structure, including proofs without the snapshot binding), 4 `verification_failed` (tag, digest chain, generation, anchor, state, page, snapshot-binding, chain or head mismatch, old-version replay or forked continuation) and 5 `bundle_unavailable` (missing files — the trust directory's parent included — or other locking, open or read errors). Failure objects never leak paths, system messages or input content.

## Test

```bash
python -m unittest discover -s tests -v
```
