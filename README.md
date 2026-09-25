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

`verify-bundle` verifies a downloaded checkpoint/proof pair entirely offline; it never touches the journal and never rewrites its inputs. All three mandatory options must appear exactly once with non-empty values, and `--etag` must be a single strong tag — one double-quoted string of 64 lowercase hexadecimal digits, exactly as the download's `ETag` header carried it. Any argument or tag-format error exits with status 2 and `{"error":"invalid_request"}` on stderr before either file is read.

Verification first recomputes the tag from the checkpoint's raw bytes — a mismatch fails immediately — then reads both files as UTF-8 JSON (negative-zero and non-finite number literals are format errors), revalidates the checkpoint's complete anchor chain, and applies the same offline proof checks as `audit_proof.verify`: embedded log bytes, query parameters, recomputed page, cursor and root digest, with the generation, anchor and closed state taken from the checkpoint snapshot in hand. The passed `--etag`, the recomputed tag and the proof's bound `checkpoint_etag` must all three agree, so an old proof against a new checkpoint, a new proof against an old checkpoint, or a tag carried by another download response is a verification failure. Proofs without the snapshot binding (version 1) are not verifiable offline and count as incomplete proof structures. Everything happens under the checkpoint's shared lock on the one snapshot that was read, so a same-directory atomic replacement racing the verification can only yield a self-consistent old or new combination, never a mix of two versions.

The optional `--trust-dir PATH` keeps a persistent sequence of the snapshots this machine has accepted, so a whole-bundle replay of an older — otherwise self-consistent — pair is rejected as a rollback. Without the option the single-bundle verification above runs unchanged and no directory is created.

The directory carries a lifecycle marker, `state.json`, that leaves exactly one explainable stage after any interruption. When the directory does not yet exist but its parent does, the first verification commits a `"preparing"` state first — binding the bundle's strong tag, the checkpoint byte digest and the candidate node digest — then saves the content-addressed snapshot and node and commits the chain head; the state advances to `"ready"` only after the head commit, and a ready record binds the head digest as well. An interruption in any of those windows is recovered idempotently: the same complete bundle resumes from `preparing` and finishes the sequence, while a bundle carrying a different tag or snapshot is rejected with every staged piece of evidence retained. An existing directory that contains neither a state nor a head — an empty directory, or one holding only unreferenced fragments — is never taken over. A directory from before the lifecycle existed (a legal chain head but no state) is adopted only after its whole chain verifies, and is then stamped ready.

With it, the existing verification completes first; then, under the trust directory's exclusive lock, the retained sequence is inspected from its first node to its head on every call: each node digest is recomputed from its strong tag, predecessor tag, checkpoint digest and predecessor node digest; each predecessor digest is followed to a real node whose tag matches; every node's checkpoint snapshot must exist, hit its content address and pass the complete structure and anchor-chain validation; and each pair of adjacent snapshots must satisfy the strict append-only relation below — history continuity is never inferred from node metadata alone. A ready state must also bind the head, the head node and the current snapshot to one another; a missing head or a crosswise relation fails verification. A same-tag replay walks the whole chain just the same, so a corrupted historical node or snapshot cannot be masked by the current bundle. A later bundle carrying the head's strong tag is an idempotent replay: the proof is fully re-verified, the full chain is re-inspected and the directory is left untouched. A new tag is accepted only when its checkpoint is a strict append-only successor of the latest trusted one — existing generations, anchors, fields and values must survive untouched, only new anchors or generations may be appended — so an already retained earlier version, a fork that rewrites old anchors, or a bundle that cannot continue the head fails verification.

Each sequence node binds the current tag, the predecessor tag, the checkpoint byte digest and the previous node digest, and the chain head points only at the last complete node, so a restarted process re-checks the real predecessor relation from disk; tampered node content, chain digests, snapshots or head relations are never repaired from the bundle in hand. Directory updates serialize across processes and commit through temporary-file writes, fsync, atomic replace and directory fsyncs, with the head committed before the ready state: any failure keeps the pre-call state, unreferenced complete files are reused on reentry, leftover fragments never take part in any decision, and a content address that already holds different bytes is never overwritten or repaired.

On success stdout carries one compact UTF-8 JSON line — `{"valid":true,"etag":...,"generation":...,"closed":...,"events":...,"next":...}` with non-ASCII written through and exactly one trailing newline — the exit status is 0 and stderr stays empty. Failures print no partial stdout, only a single compact failure object on stderr: status 2 `invalid_request` (argument or tag format), 3 `invalid_bundle` (encoding, JSON or structure, including proofs without the snapshot binding and malformed trust metadata), 4 `verification_failed` (tag, digest chain, generation, anchor, predecessor, snapshot, lifecycle-state, page, snapshot-binding or trust-sequence mismatch) and 5 `bundle_unavailable` (missing files, a missing trust-directory parent, or other locking, open or read errors). Failure objects never leak paths, system messages or input content.

## Test

```bash
python -m unittest discover -s tests -v
```
