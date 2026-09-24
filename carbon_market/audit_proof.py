"""Independent checkpoints and scope proofs over the audit journal.

This module adds two capabilities the read-only :mod:`carbon_market.audit`
journal does not provide on its own: an independent checkpoint file that
is not the journal and an offline-savable proof of a particular search.

:func:`export` reads the complete journal snapshot under the *same*
``cursor`` / ``limit`` / ``op`` / ``stage`` / ``key`` arguments
:func:`carbon_market.audit.search` takes and returns a proof object that
encapsulates the query, the result page and the journal's raw bytes,
together with the named generation and its closed state. Each successful
non-duplicate export appends one anchor to the checkpoint at
``checkpoint_path``: the anchor binds

* the complete event manifest -- every audit key paired with its event
  digest, in journal order;
* the SHA-256 of the journal's raw bytes (the *log digest*), so a log
  rotation that preserves the sealed events still produces a new anchor
  while the chain continues;
* the root head the journal declares;
* the digest of the previous anchor (null for the very first anchor of
  the checkpoint, so the stream stays one continuous chain across
  generations) and the closed flag.

The query parameters and the returned page are not anchored: the proof
carries them and :func:`verify` recomputes the page from the embedded
journal bytes and parameters, so any tampering with log bytes,
parameters, page or cursor shows up as a mismatch. Exports of different
queries against an unchanged snapshot append no anchor; their proofs
name the snapshot's existing anchor.

The anchor digest is the SHA-256 of that five-part binding serialized with the
journal's compact UTF-8 JSON convention, so rotating the journal bytes
while the sealed chain survives keeps the anchor chain continuous --
every anchor also commits to the previous anchor digest.

Generations are non-empty, unique, non-regressing names (strictly
ascending in code-point order): a checkpoint is an ordered list of
generations, and a new generation can only be created once the previous
one is closed. Within one generation every export must
preserve the already anchored events key by key and digest by digest; a
snapshot may keep the same manifest (a re-query, or a byte-preserving
log rotation) or add new keys -- which may land anywhere in code-point
order, since the anchored pairs are compared key by key, not by
position -- but a deletion, a rewrite of a known event or a retreat
raises ``ValueError`` and appends nothing. An export
identical in manifest, log digest, head and closed state is a duplicate:
it appends no anchor and returns a proof naming that same anchor, no
matter which query it carried. Passing ``final=True`` closes the current
generation -- the closing anchor may pin the same snapshot, only its
closed flag flips -- and once closed only the identical idempotent
export (``final=True``) is accepted.

:func:`verify` takes *only* the checkpoint path and a previously
exported proof -- the journal itself is never opened -- rechecks the
proof envelope, recomputes the page from the embedded log bytes and
page, reparses the embedded journal, rechecks every anchor chain, and
returns the proof's original search result together with its generation
and closed state. Any change to the embedded log bytes, parameters, page
content, cursor or any digest raises ``ValueError``.

:func:`snapshot` is the read-only counterpart: it returns the
checkpoint's raw bytes and their SHA-256 ETag, read and validated under
the checkpoint's shared flock, so downloads never observe a
half-committed checkpoint.

Exports only accept a complete version 2 journal: a version 1 journal,
a broken digest chain or a malformed checkpoint structure raises
``ValueError``. Invalid argument types, an empty generation name, a
non-boolean ``final`` or out-of-range search parameters are rejected
with ``ValueError`` *before any file is read*. A missing audit journal,
a checkpoint required for verification, or a missing checkpoint parent
directory raises ``FileNotFoundError``; every other locking or I/O
failure surfaces unchanged as ``OSError``.

An export holds the journal's shared kernel flock and the checkpoint's
exclusive kernel flock together, and the checkpoint commits via a
same-directory temporary file that is written and fsynced, moved over
the checkpoint with :func:`os.replace` and followed by a directory
fsync. A commit failure therefore never leaves a new anchor behind nor
returns a proof; concurrent appenders serialize and each observes
either the complete previous checkpoint or the complete new one, and a
restarted process continues the same anchor chain from the persisted
checkpoint.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import tempfile
import threading
from typing import Any, Iterator

from . import audit
from ._jsonio import strict_loads

__all__ = ["export", "snapshot", "verify"]

_VERSION = 1
_ROOT_FIELDS = ("version", "generations")
_GENERATION_FIELDS = ("name", "anchors")
_ANCHOR_FIELDS = ("manifest", "log_digest", "head", "previous", "closed",
                  "digest")
_PROOF_FIELDS = ("version", "generation", "params", "result", "log_bytes",
                 "log_digest", "head", "closed", "anchor_digest")
_PARAM_FIELDS = ("cursor", "limit", "op", "stage", "key")
_HEXADECIMAL = frozenset("0123456789abcdef")


class _CheckpointStore:
    def __init__(self, realpath: str) -> None:
        self.realpath = realpath
        self.lock = threading.Lock()


_stores_lock = threading.Lock()
_stores: dict[str, _CheckpointStore] = {}


def _get_store(realpath: str) -> _CheckpointStore:
    with _stores_lock:
        store = _stores.get(realpath)
        if store is None:
            store = _CheckpointStore(realpath)
            _stores[realpath] = store
        return store


@contextlib.contextmanager
def _file_lock(realpath: str, *, shared: bool = False) -> Iterator[None]:
    # Same flock discipline as the journal: the companion lock file is
    # never unlinked and the kernel releases the flock on process exit.
    lock_path = realpath + ".lock"
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 \
        and all(char in _HEXADECIMAL for char in value)


def _compact(payload: Any) -> bytes:
    # The journal's compact UTF-8 JSON convention: compact separators,
    # non-ASCII written through. Used for every digest here.
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _anchor_digest(manifest: list[list[Any]], log_digest: str,
                   head: str | None, previous: str | None,
                   closed: bool) -> str:
    return hashlib.sha256(_compact(
        [manifest, log_digest, head, previous, closed])).hexdigest()


def _check_params(cursor: object, limit: object, op: object, stage: object,
                  key: object) -> None:
    # Mirror audit.search exactly; the validation happens before any file
    # is touched, so an out-of-range parameter never reads the journal.
    if cursor is not None and (not isinstance(cursor, str) or not cursor):
        raise ValueError("cursor must be None or a non-empty string")
    if not isinstance(limit, int) or isinstance(limit, bool) \
            or not 1 <= limit <= audit._MAX_LIMIT:
        raise ValueError("limit must be an integer between 1 and 1000")
    if op is not None and op not in audit._OPS:
        raise ValueError('op filter must be "copy" or "restore"')
    if stage is not None and stage not in audit._SEARCH_STAGES:
        raise ValueError(
            "stage filter must be one of 成功, 校验, 执行, 同步, 回滚")
    if key is not None and (not isinstance(key, str) or not key):
        raise ValueError("key filter must be a non-empty string")


def _params_dict(cursor: str | None, limit: int, op: str | None,
                 stage: str | None, key: str | None) -> dict[str, Any]:
    return {"cursor": cursor, "limit": limit, "op": op, "stage": stage,
            "key": key}


def _manifest(document: dict[str, Any]) -> list[list[Any]]:
    # The complete snapshot inventory: every audit key in journal order
    # paired with the exact digest its sealed chain gives it. Comparing
    # manifests item by item enforces "known events unchanged, only new
    # keys added" without trusting the journal bytes alone.
    chain = document["chain"]
    return [[audit_key, chain[audit_key][1]]
            for audit_key in document["events"]]


def _validate_manifest(raw: object) -> list[list[Any]]:
    if not isinstance(raw, list):
        raise ValueError("anchor manifest must be a list of "
                         "[audit key, event digest] pairs")
    manifest: list[list[Any]] = []
    last_key: str | None = None
    for item in raw:
        if not isinstance(item, list) or len(item) != 2 \
                or not isinstance(item[0], str) or not item[0] \
                or not _is_digest(item[1]):
            raise ValueError("anchor manifest entries must be "
                             "[non-empty key, 64-digit hex digest]")
        if last_key is not None and item[0] <= last_key:
            raise ValueError("anchor manifest must be ordered by key "
                             "code point without repeats")
        manifest.append([item[0], item[1]])
        last_key = item[0]
    return manifest


def _check_extension(old_manifest: list[list[Any]],
                     new_manifest: list[list[Any]]) -> None:
    # The old snapshot must survive key by key: every anchored key keeps
    # its exact digest, while new keys may land anywhere in code-point
    # order -- the manifests are compared as key/digest pairs, not by
    # position. Fewer entries (retreat), a missing old key (deletion) or
    # a changed digest under a known key (rewrite) all surface here.
    # Equal manifests (re-query or byte-level log rotation with same
    # events) are permitted by the caller and never reach this check.
    if len(new_manifest) <= len(old_manifest):
        raise ValueError("a generation only accepts snapshots that add new "
                         "audit events")
    new_digests = {audit_key: digest for audit_key, digest in new_manifest}
    for audit_key, digest in old_manifest:
        if new_digests.get(audit_key) != digest:
            raise ValueError("a generation cannot rewrite, delete or "
                             "reorder an already anchored event")


def _validate_checkpoint(data: object) -> list[dict[str, Any]]:
    if not isinstance(data, dict) or list(data.keys()) != list(_ROOT_FIELDS):
        raise ValueError("checkpoint root must be an object with keys "
                         "version and generations, in that order")
    version = data["version"]
    if not isinstance(version, int) or isinstance(version, bool) \
            or version != _VERSION:
        raise ValueError("unsupported checkpoint version")
    generations_raw = data["generations"]
    if not isinstance(generations_raw, list):
        raise ValueError("checkpoint generations must be a list")
    if not generations_raw:
        # export only creates a checkpoint together with its first
        # generation, so a persisted empty list is structural damage.
        raise ValueError("checkpoint must carry at least one generation")

    seen_names: set[str] = set()
    generations: list[dict[str, Any]] = []
    # The previous-anchor chain runs across the whole checkpoint: the
    # first anchor of a later generation names the last anchor of the
    # closed previous generation, so the stream never forks.
    previous: str | None = None
    for gen_position, generation_raw in enumerate(generations_raw):
        is_last_generation = gen_position == len(generations_raw) - 1
        if not isinstance(generation_raw, dict) \
                or list(generation_raw.keys()) != list(_GENERATION_FIELDS):
            raise ValueError("each generation must be an object with keys "
                             "name and anchors, in that order")
        name = generation_raw["name"]
        if not isinstance(name, str) or not name:
            raise ValueError("generation name must be a non-empty string")
        if name in seen_names:
            raise ValueError("generation names must not repeat")
        if generations and name <= generations[-1]["name"]:
            # Names open in strictly ascending code-point order; an
            # equal-or-earlier name is a repeat or a regression.
            raise ValueError("generation names must not regress")
        seen_names.add(name)
        anchors_raw = generation_raw["anchors"]
        if not isinstance(anchors_raw, list) or not anchors_raw:
            raise ValueError("each generation must carry at least one anchor")

        anchors: list[dict[str, Any]] = []
        for index, anchor_raw in enumerate(anchors_raw):
            is_last_anchor = index == len(anchors_raw) - 1
            if not isinstance(anchor_raw, dict) \
                    or list(anchor_raw.keys()) != list(_ANCHOR_FIELDS):
                raise ValueError("each anchor must be an object with keys "
                                 "manifest, log_digest, head, previous, "
                                 "closed and digest, in that order")
            manifest = _validate_manifest(anchor_raw["manifest"])
            if not _is_digest(anchor_raw["log_digest"]):
                raise ValueError("anchor log_digest must be a 64-digit "
                                 "lowercase hexadecimal digest")
            head = anchor_raw["head"]
            if head is not None and not _is_digest(head):
                raise ValueError("anchor head must be null or a 64-digit "
                                 "lowercase hexadecimal digest")
            declared_previous = anchor_raw["previous"]
            if index > 0 and not _is_digest(declared_previous):
                raise ValueError("anchor previous digest must be a "
                                 "64-digit hexadecimal digest")
            if declared_previous != previous:
                raise ValueError("anchor previous digest does not continue "
                                 "the chain")
            if not isinstance(anchor_raw["closed"], bool):
                raise ValueError("anchor closed must be a boolean")
            digest = anchor_raw["digest"]
            if not _is_digest(digest):
                raise ValueError("anchor digest must be a 64-digit "
                                 "lowercase hexadecimal digest")
            expected = _anchor_digest(manifest, anchor_raw["log_digest"],
                                      head, previous, anchor_raw["closed"])
            if digest != expected:
                raise ValueError("anchor digest does not match its content")

            if anchors:
                earlier = anchors[-1]
                if earlier["closed"]:
                    raise ValueError("a closed anchor cannot be followed "
                                     "inside its generation")
                # Consecutive anchors either pin the same manifest (a
                # byte-level log rotation with the events intact, or the
                # closing anchor flipping the closed flag) or extend it
                # with new keys anywhere in code-point order while every
                # anchored pair survives; anything else is a rewrite.
                if manifest == earlier["manifest"]:
                    if head != earlier["head"]:
                        raise ValueError("a same-snapshot anchor cannot "
                                         "move the root digest")
                    if anchor_raw["log_digest"] == earlier["log_digest"] \
                            and anchor_raw["closed"] == earlier["closed"]:
                        raise ValueError("a duplicate anchor changes "
                                         "nothing")
                else:
                    _check_extension(earlier["manifest"], manifest)
            if anchor_raw["closed"] and not is_last_anchor:
                raise ValueError("only the last anchor of a generation "
                                 "may be closed")

            anchor = {"manifest": manifest,
                      "log_digest": anchor_raw["log_digest"], "head": head,
                      "previous": declared_previous,
                      "closed": anchor_raw["closed"], "digest": digest}
            anchors.append(anchor)
            previous = digest

        if not is_last_generation and not anchors[-1]["closed"]:
            # An open generation may only be the last one of the
            # checkpoint: any following generation proves it was closed.
            raise ValueError("a new generation can only start after the "
                             "previous one is closed")
        generations.append({"name": name, "anchors": anchors})
    return generations


def _read_checkpoint(realpath: str) -> tuple[list[dict[str, Any]] | None,
                                             bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return None, None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"checkpoint file {realpath!r} is not valid UTF-8") from exc
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"checkpoint file {realpath!r} is not valid JSON") from exc
    return _validate_checkpoint(data), raw


def _serialize_checkpoint(generations: list[dict[str, Any]]) -> bytes:
    document = {"version": _VERSION, "generations": generations}
    return (json.dumps(document, ensure_ascii=False, separators=(",", ":"),
                       allow_nan=False) + "\n").encode("utf-8")


def _fsync_dir(directory: str) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _commit_checkpoint(realpath: str, directory: str, payload: bytes,
                       old_bytes: bytes | None) -> None:
    # Same-directory temporary, fsync, atomic replace, directory fsync --
    # with a rollback if anything fails after the replace, so a failed
    # commit never leaves the new anchor behind. The pre-call bytes are
    # staged back over the file (or the file removed when it did not
    # exist) and the directory synced again, all while the exclusive
    # lock is held; a failed rollback surfaces chained after the first
    # error.
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=".audit-proof-", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, realpath)
    except BaseException as first:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise
    try:
        _fsync_dir(directory)
    except BaseException as first:
        audit._rollback(realpath, directory, old_bytes, first)
        raise


def _parse_journal_snapshot(
    audit_real: str, raw: bytes
) -> dict[str, Any]:
    document = audit._parse_document(audit_real, raw)
    audit._ensure_chain(document)
    if document["version"] != 2:
        raise ValueError("proof export requires a sealed version 2 audit "
                         "journal with a complete digest chain")
    return document


def _make_anchor(manifest: list[list[Any]], log_digest: str,
                 head: str | None, previous: str | None,
                 closed: bool) -> dict[str, Any]:
    digest = _anchor_digest(manifest, log_digest, head, previous, closed)
    return {"manifest": manifest, "log_digest": log_digest, "head": head,
            "previous": previous, "closed": closed, "digest": digest}


def _build_proof(generation: str, params: dict[str, Any],
                 result: dict[str, Any], raw: bytes, log_digest: str,
                 head: str | None, closed: bool,
                 anchor_digest: str) -> dict[str, Any]:
    return {"version": _VERSION, "generation": generation,
            "params": params, "result": result,
            "log_bytes": raw.decode("utf-8"), "log_digest": log_digest,
            "head": head, "closed": closed,
            "anchor_digest": anchor_digest}


def export(
    audit_path: str,
    checkpoint_path: str,
    generation: str,
    final: bool = False,
    cursor: str | None = None,
    limit: int = audit._DEFAULT_LIMIT,
    op: str | None = None,
    stage: str | None = None,
    key: str | None = None,
) -> dict[str, Any]:
    """Export a verifiable proof and anchor it in the checkpoint.

    ``audit_path`` and ``checkpoint_path`` must be non-empty strings;
    ``generation`` must be a non-empty string and ``final`` a boolean.
    The search arguments follow :func:`carbon_market.audit.search`. All
    argument errors raise ``ValueError`` before any file is read.

    A missing audit journal raises ``FileNotFoundError``; so does a
    missing parent directory of a checkpoint that must be created. A
    version 1 journal, an incomplete digest chain, a malformed
    checkpoint, a repeated generation name or an export that reopens a
    past generation, an attempt to open a new generation while the
    previous one is still open, an export that deletes or rewrites
    events of the current generation, or any non-identical export of a
    closed generation raises ``ValueError``. Remaining locking or I/O
    failures raise ``OSError``.

    Returns the offline proof dict. A repeat of an export whose content
    and closed state are identical appends nothing and returns a proof
    naming the same anchor. With ``final=True`` the appended anchor closes
    generation -- it may pin the same snapshot -- after which only the
    identical idempotent export (``final=True``) is accepted.
    """
    for value in (audit_path, checkpoint_path):
        if not isinstance(value, str) or not value:
            raise ValueError("audit_path and checkpoint_path must be "
                             "non-empty strings")
    if not isinstance(generation, str) or not generation:
        raise ValueError("generation must be a non-empty string")
    if not isinstance(final, bool):
        raise ValueError("final must be a boolean")
    _check_params(cursor, limit, op, stage, key)

    audit_real = os.path.realpath(audit_path)
    checkpoint_real = os.path.realpath(checkpoint_path)
    checkpoint_store = _get_store(checkpoint_real)
    with checkpoint_store.lock:
        # Hold the journal's shared lock and the checkpoint's exclusive
        # lock together until the commit lands, so audit.record cannot
        # rotate the journal while its bytes are sealed into an anchor.
        with audit._file_lock(audit_real, shared=True):
            # Read inside the shared flock exactly like the read-only
            # audit queries: a racing audit.record is observed as one
            # complete document or the other, never a half-replaced
            # file. A missing journal surfaces as FileNotFoundError.
            with open(audit_real, "rb") as handle:
                raw = handle.read()
            document = _parse_journal_snapshot(audit_real, raw)
            with _file_lock(checkpoint_real):
                generations, old_bytes = _read_checkpoint(checkpoint_real)
                if generations is None:
                    generations = []

                params = _params_dict(cursor, limit, op, stage, key)
                # Compute the page from the exact bytes in hand; this is
                # the same filter/pagination audit.search applies.
                result = _search_document(document, params)
                manifest = _manifest(document)
                head = document["head"]
                log_digest = hashlib.sha256(raw).hexdigest()
                names = [item["name"] for item in generations]

                def identical(item: dict[str, Any]) -> bool:
                    return (item["manifest"] == manifest
                            and item["log_digest"] == log_digest
                            and item["head"] == head)

                if generation in names:
                    position = names.index(generation)
                    if position != len(generations) - 1:
                        # A past generation can never be reopened.
                        raise ValueError("a past generation cannot be "
                                         "exported again")
                    current = generations[position]
                    anchors = current["anchors"]
                    last = anchors[-1]

                    if last["closed"]:
                        # Once closed the only accepted export is the
                        # idempotent one: final stays true and the
                        # snapshot is byte-for-byte the same -- events,
                        # log bytes and root head. The query may differ,
                        # since verify recomputes the page from the
                        # embedded log and parameters.
                        if not final or not identical(last):
                            raise ValueError("a closed generation only "
                                             "accepts the identical "
                                             "idempotent export")
                        return _build_proof(
                            generation, params, result, raw, log_digest,
                            head, True, last["digest"])

                    # Still open. Same events, same bytes, same head and
                    # not closing: the duplicate export appends nothing
                    # and returns the proof of the latest anchor. The
                    # manifest must equal the latest anchor's, so an
                    # older snapshot cannot mask a retreat.
                    if not final and manifest == last["manifest"] \
                            and identical(last):
                        return _build_proof(
                            generation, params, result, raw, log_digest,
                            head, False, last["digest"])

                    # Otherwise append: the manifest grows (strict
                    # extension), stays identical with rotated log
                    # bytes, or stays identical while final flips the
                    # closed flag. A smaller or rewritten manifest is
                    # rejected.
                    if manifest != last["manifest"]:
                        _check_extension(last["manifest"], manifest)
                    new_anchor = _make_anchor(manifest, log_digest, head,
                                              last["digest"], final)
                    anchors.append(new_anchor)
                else:
                    if generations and not \
                            generations[-1]["anchors"][-1]["closed"]:
                        raise ValueError("a new generation can only be "
                                         "started after the previous one "
                                         "is closed")
                    if generations and generation <= generations[-1]["name"]:
                        raise ValueError("generation names must not "
                                         "regress")
                    previous_digest = (
                        None if not generations
                        else generations[-1]["anchors"][-1]["digest"])
                    new_anchor = _make_anchor(manifest, log_digest, head,
                                              previous_digest, final)
                    generations.append(
                        {"name": generation, "anchors": [new_anchor]})

                _commit_checkpoint(
                    checkpoint_real,
                    os.path.dirname(checkpoint_real) or ".",
                    _serialize_checkpoint(generations), old_bytes)
                return _build_proof(generation, params, result, raw,
                                    log_digest, head, final,
                                    new_anchor["digest"])


def _validate_proof(data: object) -> dict[str, Any]:
    if not isinstance(data, dict) or list(data.keys()) != list(_PROOF_FIELDS):
        raise ValueError("proof must be an object with the exported fields "
                         "in their original order")
    if not isinstance(data["version"], int) \
            or isinstance(data["version"], bool) \
            or data["version"] != _VERSION:
        raise ValueError("unsupported proof version")
    if not isinstance(data["generation"], str) or not data["generation"]:
        raise ValueError("proof generation must be a non-empty string")
    params = data["params"]
    if not isinstance(params, dict) \
            or list(params.keys()) != list(_PARAM_FIELDS):
        raise ValueError("proof params must be the exported query "
                         "parameters")
    _check_params(params["cursor"], params["limit"], params["op"],
                  params["stage"], params["key"])
    result = data["result"]
    if not isinstance(result, dict) \
            or list(result.keys()) != ["events", "next"]:
        raise ValueError("proof result must be the exported search result")
    if not isinstance(result["events"], list):
        raise ValueError("proof result events must be a list")
    next_cursor = result["next"]
    if next_cursor is not None and (not isinstance(next_cursor, str)
                                    or not next_cursor):
        raise ValueError("proof result next must be null or a non-empty "
                         "string")
    if not isinstance(data["log_bytes"], str):
        raise ValueError("proof log_bytes must be a string")
    if not _is_digest(data["log_digest"]):
        raise ValueError("proof log_digest must be a 64-digit lowercase "
                         "hexadecimal digest")
    head = data["head"]
    if head is not None and not _is_digest(head):
        raise ValueError("proof head must be null or a 64-digit lowercase "
                         "hexadecimal digest")
    if not isinstance(data["closed"], bool):
        raise ValueError("proof closed must be a boolean")
    if not _is_digest(data["anchor_digest"]):
        raise ValueError("proof anchor_digest must be a 64-digit lowercase "
                         "hexadecimal digest")
    return data


def _search_document(document: dict[str, Any], params: dict[str, Any]) \
        -> dict[str, Any]:
    # Recompute audit.search's result from an in-memory document so
    # verify never needs the journal file itself.
    cursor = params["cursor"]
    limit = params["limit"]
    matches: list[list[Any]] = []
    for audit_key, event in document["events"].items():
        if cursor is not None and audit_key <= cursor:
            continue
        if not audit._matches(event, params["op"], params["stage"],
                              params["key"]):
            continue
        matches.append([audit_key, dict(event)])
        if len(matches) > limit:
            break
    if len(matches) > limit:
        page = matches[:limit]
        return {"events": page, "next": page[-1][0]}
    return {"events": matches, "next": None}


def snapshot(checkpoint_path: str) -> tuple[bytes, str]:
    """Read the validated checkpoint bytes and their ETag, read-only.

    The checkpoint is opened, read, validated and hashed while holding
    the same shared kernel flock :func:`verify` uses and :func:`export`
    takes exclusively, so a concurrent export is observed as either the
    complete old checkpoint or the complete new one, never a
    half-committed mixture. The returned bytes are exactly the file's raw
    UTF-8 content -- nothing is reordered or reserialized -- and the
    ETag is the lowercase hexadecimal SHA-256 of those very bytes, so a
    conditional decision made against them stays consistent.

    ``checkpoint_path`` must be a non-empty string. A missing checkpoint
    raises ``FileNotFoundError``; invalid UTF-8, malformed JSON, an
    unsupported version, a wrong field order or a broken anchor or
    digest chain raises ``ValueError``; every other locking or I/O
    failure raises ``OSError``.
    """
    if not isinstance(checkpoint_path, str) or not checkpoint_path:
        raise ValueError("checkpoint_path must be a non-empty string")
    checkpoint_real = os.path.realpath(checkpoint_path)
    with _file_lock(checkpoint_real, shared=True):
        generations, raw = _read_checkpoint(checkpoint_real)
        if generations is None or raw is None:
            raise FileNotFoundError(
                f"checkpoint file {checkpoint_real!r} does not exist")
        etag = hashlib.sha256(raw).hexdigest()
    return raw, etag


def verify(checkpoint_path: str, proof: dict[str, Any]) -> dict[str, Any]:
    """Verify an exported proof solely against the checkpoint.

    The journal is not opened: the proof carries the journal's raw
    bytes, parameters and result. ``checkpoint_path`` must be a
    non-empty string and ``proof`` an object previously returned by
    :func:`export`.

    The embedded log bytes must hash to the proof's log digest, parse as
    a complete version 2 journal whose event manifest and root head
    match the anchor the proof names in the named generation of the
    checkpoint, and the page must recompute from the embedded log and
    parameters. Every anchor chain in the checkpoint is rechecked as
    well. Success returns
    ``{"events": ..., "next": ..., "generation": ..., "closed": ...}``
    -- the original search result together with its generation and
    closed state.

    A missing checkpoint raises ``FileNotFoundError``; any proof or
    checkpoint mismatch -- including changed log bytes, parameters,
    page content, cursor or any digest -- raises ``ValueError``; other
    I/O failures raise ``OSError``.
    """
    if not isinstance(checkpoint_path, str) or not checkpoint_path:
        raise ValueError("checkpoint_path must be a non-empty string")

    clean = _validate_proof(proof)
    checkpoint_real = os.path.realpath(checkpoint_path)
    with _file_lock(checkpoint_real, shared=True):
        generations, _ = _read_checkpoint(checkpoint_real)
    if generations is None:
        raise FileNotFoundError(
            f"checkpoint file {checkpoint_real!r} does not exist")

    names = [item["name"] for item in generations]
    if clean["generation"] not in names:
        raise ValueError("proof generation is not recorded in the "
                         "checkpoint")
    generation = generations[names.index(clean["generation"])]
    anchor = next((item for item in generation["anchors"]
                   if item["digest"] == clean["anchor_digest"]), None)
    if anchor is None:
        raise ValueError("proof anchor is not recorded in the checkpoint")

    params = clean["params"]
    raw = clean["log_bytes"].encode("utf-8")
    if hashlib.sha256(raw).hexdigest() != clean["log_digest"]:
        raise ValueError("proof log bytes do not match their log digest")
    if clean["log_digest"] != anchor["log_digest"]:
        raise ValueError("proof log digest is not bound to the anchor")
    document = audit._parse_document(checkpoint_real, raw)
    audit._ensure_chain(document)
    if document["version"] != 2:
        raise ValueError("proof requires a sealed version 2 audit journal")
    if clean["head"] != document["head"] \
            or anchor["head"] != document["head"]:
        raise ValueError("proof root digest does not match the journal")
    if _manifest(document) != anchor["manifest"]:
        raise ValueError("proof event manifest does not match the anchor")
    if clean["closed"] != anchor["closed"]:
        raise ValueError("proof closed state does not match the anchor")

    recomputed = _search_document(document, params)
    if recomputed != clean["result"]:
        raise ValueError("proof result does not follow from the embedded "
                         "log and parameters")
    return {"events": clean["result"]["events"],
            "next": clean["result"]["next"],
            "generation": clean["generation"],
            "closed": anchor["closed"]}
