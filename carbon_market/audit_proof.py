"""Independent checkpoints and scope proofs over the audit journal.

This module adds two capabilities the read-only :mod:`carbon_market.audit`
journal does not provide on its own: an independent checkpoint file that
is not the journal and an offline-savable proof of a particular search.

:func:`export` reads the complete journal snapshot under the *same*
``cursor`` / ``limit`` / ``op`` / ``stage`` / ``key`` arguments
:func:`carbon_market.audit.search` takes and returns a version 2 proof
object that encapsulates the query, the result page and the journal's
raw bytes, together with the named generation and its closed state. The
proof ends with ``checkpoint_etag``: the strong entity tag of the
checkpoint's raw bytes after this export's anchor commit -- one
double-quoted string of 64 lowercase hexadecimal digits, the SHA-256 of
the complete bytes, exactly the computation and quoting the checkpoint
download serves -- so every proof is bound to precisely one checkpoint
snapshot. Each successful non-duplicate export appends one anchor to
the checkpoint at ``checkpoint_path``: the anchor binds

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
proof envelope, recomputes the strong tag of the checkpoint bytes read
under the lock and confronts it with the proof's bound
``checkpoint_etag``, recomputes the page from the embedded log bytes and
page, reparses the embedded journal, rechecks every anchor chain, and
returns the proof's original search result together with its generation
and closed state. Any change to the embedded log bytes, parameters, page
content, cursor or any digest raises ``ValueError``; a proof whose bound
tag does not match the checkpoint in hand -- an old proof against a new
checkpoint or a new proof against an old one -- mismatches as well.

:func:`verify_bundle` is the offline entry for a downloaded pair: it
takes the checkpoint file, the saved proof file and the strong entity
tag the download carried, recomputes the tag from the checkpoint's raw
bytes before anything else, and then applies the same checkpoint and
proof validation as :func:`verify` against the single locked snapshot
it read, so a racing same-directory replacement can only yield a
self-consistent old or new combination. The passed tag, the recomputed
tag and the proof's bound ``checkpoint_etag`` must all three agree, so
a tag carried by another download response never verifies. Structural
failures (encoding, JSON grammar, field shapes, and version 1 proofs,
which predate the snapshot binding) raise ``BundleFormatError`` while
every tag, digest-chain, generation, anchor, closed-state, page or
snapshot-binding mismatch raises ``BundleMismatchError``; both are
public ``ValueError`` subclasses that let the command-line entry tell a
malformed bundle apart from a failed verification.

:func:`verify_bundle` also accepts an optional *trust_dir*: a directory
holding a persistent sequence of snapshot nodes that lets this machine
reject a whole-bundle replay of an older, otherwise self-consistent
pair. With a trust directory the single-bundle verification runs first,
unchanged; then, under the trust directory's own exclusive lock, the
verified snapshot is compared with the retained chain head.

The directory carries a lifecycle marker, ``state.json``, that makes
every on-disk stage uniquely explainable after an interruption. The
first verification of a not-yet-existing directory (whose parent
exists) commits a ``"preparing"`` state first -- binding the
candidate's strong tag, checkpoint byte digest and candidate node
digest -- then saves the content-addressed snapshot and node and
commits the chain head, and only after the head flips the state to
``"ready"`` (which additionally binds the head node digest). A crash
in any of those windows is resumed idempotently: the identical bundle
completes the preparation, while a different bundle is rejected with
every piece of evidence retained. A ``"ready"`` state is trusted only
after the head it names, the head node and the head snapshot are all
re-checked against one another. An existing directory that has neither
state nor head -- an empty directory or one holding only unreferenced
fragments -- is never taken over, and a directory from before the
lifecycle existed (a legal head but no state) is adopted only after
its whole chain verifies, then stamped ready.

Every verification walks the retained chain from its first node to the
head: each node digest is recomputed over tag, predecessor tag,
checkpoint digest and predecessor node digest, each predecessor
relation is followed to a real node, each checkpoint snapshot must
exist, hit its content address and pass the complete structure and
anchor-chain validation, and each pair of adjacent snapshots must
satisfy the strict append-only relation -- history continuity is never
inferred from node metadata alone. A same-tag replay walks the whole
chain as well, so any corrupted historical node or snapshot fails
instead of being masked by the current bundle. Unreferenced fragments
are left untouched and never take part in a decision; a content
address that already holds different bytes is never overwritten or
repaired.

A later bundle carrying the same strong tag is an idempotent replay:
the proof is fully re-verified, the retained chain is fully
re-inspected and the directory is left untouched. A new tag is accepted
only when its checkpoint is a strict append-only successor of the
latest trusted one -- existing generations, anchors, fields and values
must survive untouched, only new anchors or generations may follow. An
already retained earlier version, a fork that rewrites old anchors or a
bundle that cannot continue the head is rejected as a rollback. Every
sequence node binds the current tag, the predecessor tag, the
checkpoint byte digest and the previous node digest, and the chain head
points only at the last complete node, so a restarted process re-checks
the real predecessor relation from disk. Tampered node content, chain
digests, snapshots or head relations are never repaired from the
bundle in hand: the evidence stays and the verification fails.
Directory updates serialize across processes, commit through
same-directory temporary files, fsync, atomic replace and directory
fsyncs, and the head is always committed before the ready state, so any
failure keeps the pre-call state; unreferenced complete files are
reused on reentry and leftover fragments never take part in any
decision. Trust metadata that is not valid UTF-8/JSON, carries a
negative-zero literal, or breaks field order, version or structure
raises ``BundleFormatError``; a chain digest, snapshot, predecessor,
head or lifecycle-state mismatch, an old-version replay or a forked
continuation raises ``BundleMismatchError``; a missing trust-directory
parent raises ``FileNotFoundError`` and every other locking or I/O
failure surfaces as ``OSError``.

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
import re
import tempfile
import threading
from typing import Any, Iterator

from . import audit
from ._jsonio import finite_loads, strict_loads

__all__ = ["export", "verify", "verify_bundle", "read_snapshot",
           "BundleFormatError", "BundleMismatchError"]

_CHECKPOINT_VERSION = 1
_PROOF_VERSION = 2
_ROOT_FIELDS = ("version", "generations")
_GENERATION_FIELDS = ("name", "anchors")
_ANCHOR_FIELDS = ("manifest", "log_digest", "head", "previous", "closed",
                  "digest")
_PROOF_FIELDS = ("version", "generation", "params", "result", "log_bytes",
                 "log_digest", "head", "closed", "anchor_digest",
                 "checkpoint_etag")
_PARAM_FIELDS = ("cursor", "limit", "op", "stage", "key")
_HEXADECIMAL = frozenset("0123456789abcdef")
# A downloaded checkpoint is identified by exactly one strong entity
# tag: one double-quoted string of 64 lowercase hexadecimal digits, the
# SHA-256 of the complete checkpoint bytes -- the same shape the
# conditional download accepts and serves, and the same shape a proof
# carries in its checkpoint_etag binding.
_ETAG_RE = re.compile(r'"[0-9a-f]{64}"')

# The persistent snapshot sequence kept in a verify-bundle trust
# directory: a content-addressed node per accepted checkpoint, a
# content-addressed copy of every retained checkpoint's raw bytes and a
# head pointer that names the last complete node. All three metadata
# documents are versioned compact JSON with a fixed field order.
#
# The directory additionally keeps state.json, the lifecycle marker that
# makes every on-disk stage explainable after a crash: a first
# verification commits "preparing" -- binding the strong tag, the
# checkpoint digest and the candidate node digest -- before any snapshot
# or node is written, and the head advances only once the complete
# sequence is stored, after which the state flips to "ready" (binding
# the head node digest as well). An interrupted "preparing" is resumed
# idempotently for the identical bundle or rejected for a different one;
# a "ready" directory is only trusted after a full chain inspection.
_TRUST_VERSION = 1
_TRUST_HEAD_FIELDS = ("version", "digest")
_TRUST_NODE_FIELDS = ("version", "tag", "previous_tag",
                      "checkpoint_digest", "previous_digest")
_TRUST_PREPARING_FIELDS = ("version", "state", "tag", "checkpoint_digest",
                           "node_digest")
_TRUST_READY_FIELDS = _TRUST_PREPARING_FIELDS + ("head_digest",)
_TRUST_STATE_PREPARING = "preparing"
_TRUST_STATE_READY = "ready"


class BundleFormatError(ValueError):
    """A structural failure of a checkpoint, proof or bundle file.

    Encoding, JSON grammar, number-range, field-order, version and
    field-shape problems raise this ``ValueError`` subclass -- including
    version 1 proofs, which predate the checkpoint snapshot binding and
    are treated as incomplete proof structures. Every existing caller
    still sees a ``ValueError``.
    """


class BundleMismatchError(ValueError):
    """A digest-chain or cross-reference mismatch, not a format error.

    Both downloaded files failing their *structural* checks raise
    :class:`BundleFormatError`; a tag, digest chain, generation, anchor,
    closed-state, page or snapshot-binding mismatch raises this
    subclass instead, so the offline bundle verifier can tell a
    malformed bundle apart from a well-formed one that does not check
    out. Every existing caller still sees a ``ValueError``.
    """


# The former private name of BundleMismatchError, kept so callers that
# imported it before the exception became public keep working.
_MismatchError = BundleMismatchError


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


def _strong_tag(raw: bytes) -> str:
    # The strong entity tag of a checkpoint snapshot: one double-quoted
    # string of 64 lowercase hexadecimal digits, the SHA-256 of the
    # complete bytes -- exactly the computation and quoting the
    # conditional download serves in its ETag header.
    return '"' + hashlib.sha256(raw).hexdigest() + '"'


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
        raise BundleFormatError("anchor manifest must be a list of "
                                "[audit key, event digest] pairs")
    manifest: list[list[Any]] = []
    last_key: str | None = None
    for item in raw:
        if not isinstance(item, list) or len(item) != 2 \
                or not isinstance(item[0], str) or not item[0] \
                or not _is_digest(item[1]):
            raise BundleFormatError("anchor manifest entries must be "
                                    "[non-empty key, 64-digit hex digest]")
        if last_key is not None and item[0] <= last_key:
            raise BundleFormatError("anchor manifest must be ordered by "
                                    "key code point without repeats")
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
        raise BundleMismatchError("a generation only accepts snapshots "
                                  "that add new audit events")
    new_digests = {audit_key: digest for audit_key, digest in new_manifest}
    for audit_key, digest in old_manifest:
        if new_digests.get(audit_key) != digest:
            raise BundleMismatchError("a generation cannot rewrite, delete "
                                      "or reorder an already anchored event")


def _validate_checkpoint(data: object) -> list[dict[str, Any]]:
    if not isinstance(data, dict) or list(data.keys()) != list(_ROOT_FIELDS):
        raise BundleFormatError("checkpoint root must be an object with "
                                "keys version and generations, in that "
                                "order")
    version = data["version"]
    if not isinstance(version, int) or isinstance(version, bool) \
            or version != _CHECKPOINT_VERSION:
        raise BundleFormatError("unsupported checkpoint version")
    generations_raw = data["generations"]
    if not isinstance(generations_raw, list):
        raise BundleFormatError("checkpoint generations must be a list")
    if not generations_raw:
        # export only creates a checkpoint together with its first
        # generation, so a persisted empty list is structural damage.
        raise BundleFormatError("checkpoint must carry at least one "
                                "generation")

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
            raise BundleFormatError("each generation must be an object "
                                    "with keys name and anchors, in that "
                                    "order")
        name = generation_raw["name"]
        if not isinstance(name, str) or not name:
            raise BundleFormatError("generation name must be a non-empty "
                                    "string")
        if name in seen_names:
            raise BundleFormatError("generation names must not repeat")
        if generations and name <= generations[-1]["name"]:
            # Names open in strictly ascending code-point order; an
            # equal-or-earlier name is a repeat or a regression.
            raise BundleFormatError("generation names must not regress")
        seen_names.add(name)
        anchors_raw = generation_raw["anchors"]
        if not isinstance(anchors_raw, list) or not anchors_raw:
            raise BundleFormatError("each generation must carry at least "
                                    "one anchor")

        anchors: list[dict[str, Any]] = []
        for index, anchor_raw in enumerate(anchors_raw):
            is_last_anchor = index == len(anchors_raw) - 1
            if not isinstance(anchor_raw, dict) \
                    or list(anchor_raw.keys()) != list(_ANCHOR_FIELDS):
                raise BundleFormatError(
                    "each anchor must be an object with keys manifest, "
                    "log_digest, head, previous, closed and digest, in "
                    "that order")
            manifest = _validate_manifest(anchor_raw["manifest"])
            if not _is_digest(anchor_raw["log_digest"]):
                raise BundleFormatError("anchor log_digest must be a "
                                        "64-digit lowercase hexadecimal "
                                        "digest")
            head = anchor_raw["head"]
            if head is not None and not _is_digest(head):
                raise BundleFormatError("anchor head must be null or a "
                                        "64-digit lowercase hexadecimal "
                                        "digest")
            declared_previous = anchor_raw["previous"]
            if index > 0 and not _is_digest(declared_previous):
                raise BundleFormatError("anchor previous digest must be a "
                                        "64-digit hexadecimal digest")
            if declared_previous != previous:
                raise BundleMismatchError("anchor previous digest does not "
                                          "continue the chain")
            if not isinstance(anchor_raw["closed"], bool):
                raise BundleFormatError("anchor closed must be a boolean")
            digest = anchor_raw["digest"]
            if not _is_digest(digest):
                raise BundleFormatError("anchor digest must be a 64-digit "
                                        "lowercase hexadecimal digest")
            expected = _anchor_digest(manifest, anchor_raw["log_digest"],
                                      head, previous, anchor_raw["closed"])
            if digest != expected:
                raise BundleMismatchError("anchor digest does not match "
                                          "its content")

            if anchors:
                earlier = anchors[-1]
                if earlier["closed"]:
                    raise BundleMismatchError("a closed anchor cannot be "
                                              "followed inside its "
                                              "generation")
                # Consecutive anchors either pin the same manifest (a
                # byte-level log rotation with the events intact, or the
                # closing anchor flipping the closed flag) or extend it
                # with new keys anywhere in code-point order while every
                # anchored pair survives; anything else is a rewrite.
                if manifest == earlier["manifest"]:
                    if head != earlier["head"]:
                        raise BundleMismatchError(
                            "a same-snapshot anchor cannot move the root "
                            "digest")
                    if anchor_raw["log_digest"] == earlier["log_digest"] \
                            and anchor_raw["closed"] == earlier["closed"]:
                        raise BundleMismatchError("a duplicate anchor "
                                                  "changes nothing")
                else:
                    _check_extension(earlier["manifest"], manifest)
            if anchor_raw["closed"] and not is_last_anchor:
                raise BundleMismatchError("only the last anchor of a "
                                          "generation may be closed")

            anchor = {"manifest": manifest,
                      "log_digest": anchor_raw["log_digest"], "head": head,
                      "previous": declared_previous,
                      "closed": anchor_raw["closed"], "digest": digest}
            anchors.append(anchor)
            previous = digest

        if not is_last_generation and not anchors[-1]["closed"]:
            # An open generation may only be the last one of the
            # checkpoint: any following generation proves it was closed.
            raise BundleMismatchError("a new generation can only start "
                                      "after the previous one is closed")
        generations.append({"name": name, "anchors": anchors})
    return generations


def _parse_checkpoint(realpath: str, raw: bytes) -> list[dict[str, Any]]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BundleFormatError(
            f"checkpoint file {realpath!r} is not valid UTF-8") from exc
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise BundleFormatError(
            f"checkpoint file {realpath!r} is not valid JSON") from exc
    return _validate_checkpoint(data)


def _read_checkpoint(realpath: str) -> tuple[list[dict[str, Any]] | None,
                                             bytes | None]:
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return None, None
    return _parse_checkpoint(realpath, raw), raw


def _serialize_checkpoint(generations: list[dict[str, Any]]) -> bytes:
    document = {"version": _CHECKPOINT_VERSION, "generations": generations}
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
                 anchor_digest: str, checkpoint_etag: str) -> dict[str, Any]:
    return {"version": _PROOF_VERSION, "generation": generation,
            "params": params, "result": result,
            "log_bytes": raw.decode("utf-8"), "log_digest": log_digest,
            "head": head, "closed": closed,
            "anchor_digest": anchor_digest,
            "checkpoint_etag": checkpoint_etag}


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

    Returns the offline proof dict: a version 2 envelope whose last
    field, ``checkpoint_etag``, is the strong entity tag of the
    checkpoint's raw bytes after this export's anchor commit -- the same
    quoted SHA-256 the checkpoint download serves. A repeat of an export
    whose content and closed state are identical appends nothing and
    returns a proof naming the same anchor and the unchanged snapshot's
    tag. With ``final=True`` the appended anchor closes
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
                            head, True, last["digest"],
                            _strong_tag(old_bytes))

                    # Still open. Same events, same bytes, same head and
                    # not closing: the duplicate export appends nothing
                    # and returns the proof of the latest anchor. The
                    # manifest must equal the latest anchor's, so an
                    # older snapshot cannot mask a retreat.
                    if not final and manifest == last["manifest"] \
                            and identical(last):
                        return _build_proof(
                            generation, params, result, raw, log_digest,
                            head, False, last["digest"],
                            _strong_tag(old_bytes))

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

                payload = _serialize_checkpoint(generations)
                _commit_checkpoint(
                    checkpoint_real,
                    os.path.dirname(checkpoint_real) or ".",
                    payload, old_bytes)
                # The proof binds the snapshot just committed: while the
                # exclusive lock is held the on-disk bytes are exactly
                # the committed payload, so its tag is the one a
                # download of this checkpoint would serve.
                return _build_proof(generation, params, result, raw,
                                    log_digest, head, final,
                                    new_anchor["digest"],
                                    _strong_tag(payload))


def _validate_proof(data: object) -> dict[str, Any]:
    if not isinstance(data, dict) or list(data.keys()) != list(_PROOF_FIELDS):
        raise BundleFormatError("proof must be an object with the "
                                "exported fields in their original order")
    if not isinstance(data["version"], int) \
            or isinstance(data["version"], bool) \
            or data["version"] != _PROOF_VERSION:
        # A version 1 proof predates the checkpoint snapshot binding and
        # is treated as an incomplete proof structure, never as a
        # mismatch.
        raise BundleFormatError("unsupported proof version")
    if not isinstance(data["generation"], str) or not data["generation"]:
        raise BundleFormatError("proof generation must be a non-empty "
                                "string")
    params = data["params"]
    if not isinstance(params, dict) \
            or list(params.keys()) != list(_PARAM_FIELDS):
        raise BundleFormatError("proof params must be the exported query "
                                "parameters")
    try:
        _check_params(params["cursor"], params["limit"], params["op"],
                      params["stage"], params["key"])
    except ValueError as exc:
        raise BundleFormatError(str(exc)) from exc
    result = data["result"]
    if not isinstance(result, dict) \
            or list(result.keys()) != ["events", "next"]:
        raise BundleFormatError("proof result must be the exported search "
                                "result")
    if not isinstance(result["events"], list):
        raise BundleFormatError("proof result events must be a list")
    next_cursor = result["next"]
    if next_cursor is not None and (not isinstance(next_cursor, str)
                                    or not next_cursor):
        raise BundleFormatError("proof result next must be null or a "
                                "non-empty string")
    if not isinstance(data["log_bytes"], str):
        raise BundleFormatError("proof log_bytes must be a string")
    if not _is_digest(data["log_digest"]):
        raise BundleFormatError("proof log_digest must be a 64-digit "
                                "lowercase hexadecimal digest")
    head = data["head"]
    if head is not None and not _is_digest(head):
        raise BundleFormatError("proof head must be null or a 64-digit "
                                "lowercase hexadecimal digest")
    if not isinstance(data["closed"], bool):
        raise BundleFormatError("proof closed must be a boolean")
    if not _is_digest(data["anchor_digest"]):
        raise BundleFormatError("proof anchor_digest must be a 64-digit "
                                "lowercase hexadecimal digest")
    checkpoint_etag = data["checkpoint_etag"]
    if not isinstance(checkpoint_etag, str) \
            or not _ETAG_RE.fullmatch(checkpoint_etag):
        raise BundleFormatError("proof checkpoint_etag must be a single "
                                "strong tag: a quoted 64-digit lowercase "
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


def _verify_proof(generations: list[dict[str, Any]],
                  clean: dict[str, Any],
                  checkpoint_real: str) -> dict[str, Any]:
    # The cross-checks every verified proof must pass against one
    # checkpoint snapshot: the named generation and anchor must be
    # recorded, the embedded log bytes must hash to the anchored log
    # digest and reparse as a sealed version 2 journal whose manifest
    # and root head the anchor pins, and the page must recompute from
    # the embedded log and parameters. Every failure here is a
    # mismatch, never a format error.
    names = [item["name"] for item in generations]
    if clean["generation"] not in names:
        raise BundleMismatchError("proof generation is not recorded in "
                                  "the checkpoint")
    generation = generations[names.index(clean["generation"])]
    anchor = next((item for item in generation["anchors"]
                   if item["digest"] == clean["anchor_digest"]), None)
    if anchor is None:
        raise BundleMismatchError("proof anchor is not recorded in the "
                                  "checkpoint")

    params = clean["params"]
    raw = clean["log_bytes"].encode("utf-8")
    if hashlib.sha256(raw).hexdigest() != clean["log_digest"]:
        raise BundleMismatchError("proof log bytes do not match their "
                                  "log digest")
    if clean["log_digest"] != anchor["log_digest"]:
        raise BundleMismatchError("proof log digest is not bound to the "
                                  "anchor")
    try:
        document = audit._parse_document(checkpoint_real, raw)
        audit._ensure_chain(document)
    except ValueError as exc:
        raise BundleMismatchError(str(exc)) from exc
    if document["version"] != 2:
        raise BundleMismatchError("proof requires a sealed version 2 "
                                  "audit journal")
    if clean["head"] != document["head"] \
            or anchor["head"] != document["head"]:
        raise BundleMismatchError("proof root digest does not match the "
                                  "journal")
    if _manifest(document) != anchor["manifest"]:
        raise BundleMismatchError("proof event manifest does not match "
                                  "the anchor")
    if clean["closed"] != anchor["closed"]:
        raise BundleMismatchError("proof closed state does not match "
                                  "the anchor")

    recomputed = _search_document(document, params)
    if recomputed != clean["result"]:
        raise BundleMismatchError("proof result does not follow from "
                                  "the embedded log and parameters")
    return {"events": clean["result"]["events"],
            "next": clean["result"]["next"],
            "generation": clean["generation"],
            "closed": anchor["closed"]}


def verify(checkpoint_path: str, proof: dict[str, Any]) -> dict[str, Any]:
    """Verify an exported proof solely against the checkpoint.

    The journal is not opened: the proof carries the journal's raw
    bytes, parameters and result. ``checkpoint_path`` must be a
    non-empty string and ``proof`` an object previously returned by
    :func:`export`.

    The checkpoint bytes are read under the shared lock and their
    strong tag is recomputed and confronted with the proof's bound
    ``checkpoint_etag`` before anything else: a proof exported against
    another snapshot -- older or newer -- mismatches. The embedded log
    bytes must hash to the proof's log digest, parse as
    a complete version 2 journal whose event manifest and root head
    match the anchor the proof names in the named generation of the
    checkpoint, and the page must recompute from the embedded log and
    parameters. Every anchor chain in the checkpoint is rechecked as
    well. Success returns
    ``{"events": ..., "next": ..., "generation": ..., "closed": ...}``
    -- the original search result together with its generation and
    closed state.

    A missing checkpoint raises ``FileNotFoundError``; a proof without
    the snapshot binding (version 1) or any other structural problem
    raises ``BundleFormatError``; any proof or checkpoint mismatch --
    including changed log bytes, parameters, page content, cursor, any
    digest or a stale snapshot tag -- raises ``BundleMismatchError``;
    other I/O failures raise ``OSError``. Both error types are
    ``ValueError`` subclasses.
    """
    if not isinstance(checkpoint_path, str) or not checkpoint_path:
        raise ValueError("checkpoint_path must be a non-empty string")

    clean = _validate_proof(proof)
    checkpoint_real = os.path.realpath(checkpoint_path)
    with _file_lock(checkpoint_real, shared=True):
        # One locked snapshot: the tag is recomputed from the exact
        # bytes that are then parsed and validated, so a racing
        # replacement can only yield a self-consistent outcome.
        try:
            with open(checkpoint_real, "rb") as handle:
                raw = handle.read()
        except FileNotFoundError:
            raise FileNotFoundError(
                f"checkpoint file {checkpoint_real!r} does not exist"
            ) from None
        if clean["checkpoint_etag"] != _strong_tag(raw):
            raise BundleMismatchError(
                "proof checkpoint_etag does not bind the checkpoint "
                "snapshot in hand")
        generations = _parse_checkpoint(checkpoint_real, raw)
    return _verify_proof(generations, clean, checkpoint_real)


def _loads_bundle(raw: bytes) -> Any:
    # Both downloaded files are UTF-8 JSON; a decoding failure, a
    # negative-zero or non-finite number literal, or a JSON grammar
    # error is a format error of the bundle, never a mismatch.
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BundleFormatError("bundle file is not valid UTF-8") from exc
    try:
        return finite_loads(text)
    except ValueError as exc:
        raise BundleFormatError("bundle file is not valid JSON") from exc


def _loads_trust(realpath: str, raw: bytes) -> Any:
    # Trust metadata follows the bundle convention: UTF-8 JSON without
    # negative-zero or non-finite literals, anything else is a format
    # error of the retained evidence, never a mismatch.
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BundleFormatError(
            f"trust metadata {realpath!r} is not valid UTF-8") from exc
    try:
        return finite_loads(text)
    except ValueError as exc:
        raise BundleFormatError(
            f"trust metadata {realpath!r} is not valid JSON") from exc


def _node_digest(tag: str, previous_tag: str | None,
                 checkpoint_digest: str,
                 previous_digest: str | None) -> str:
    # Every sequence node binds the current strong tag, the predecessor
    # tag, the digest of the checkpoint's raw bytes and the previous
    # node digest (both null at the trust anchor).
    return hashlib.sha256(_compact(
        [tag, previous_tag, checkpoint_digest,
         previous_digest])).hexdigest()


def _serialize_node(node: dict[str, Any]) -> bytes:
    document = {"version": _TRUST_VERSION, "tag": node["tag"],
                "previous_tag": node["previous_tag"],
                "checkpoint_digest": node["checkpoint_digest"],
                "previous_digest": node["previous_digest"]}
    return (json.dumps(document, ensure_ascii=False,
                       separators=(",", ":"), allow_nan=False)
            + "\n").encode("utf-8")


def _serialize_head(digest: str) -> bytes:
    document = {"version": _TRUST_VERSION, "digest": digest}
    return (json.dumps(document, ensure_ascii=False,
                       separators=(",", ":"), allow_nan=False)
            + "\n").encode("utf-8")


def _validate_head(data: object) -> str:
    if not isinstance(data, dict) \
            or list(data.keys()) != list(_TRUST_HEAD_FIELDS):
        raise BundleFormatError("trust head must be an object with keys "
                                "version and digest, in that order")
    version = data["version"]
    if not isinstance(version, int) or isinstance(version, bool) \
            or version != _TRUST_VERSION:
        raise BundleFormatError("unsupported trust head version")
    if not _is_digest(data["digest"]):
        raise BundleFormatError("trust head digest must be a 64-digit "
                                "lowercase hexadecimal digest")
    return data["digest"]


def _validate_node(data: object) -> dict[str, Any]:
    if not isinstance(data, dict) \
            or list(data.keys()) != list(_TRUST_NODE_FIELDS):
        raise BundleFormatError("trust node must be an object with keys "
                                "version, tag, previous_tag, "
                                "checkpoint_digest and previous_digest, "
                                "in that order")
    version = data["version"]
    if not isinstance(version, int) or isinstance(version, bool) \
            or version != _TRUST_VERSION:
        raise BundleFormatError("unsupported trust node version")
    tag = data["tag"]
    if not isinstance(tag, str) or not _ETAG_RE.fullmatch(tag):
        raise BundleFormatError("trust node tag must be a single strong "
                                "tag: a quoted 64-digit lowercase "
                                "hexadecimal digest")
    previous_tag = data["previous_tag"]
    if previous_tag is not None and (not isinstance(previous_tag, str)
                                     or not _ETAG_RE.fullmatch(previous_tag)):
        raise BundleFormatError("trust node previous_tag must be null or "
                                "a single strong tag")
    checkpoint_digest = data["checkpoint_digest"]
    if not _is_digest(checkpoint_digest):
        raise BundleFormatError("trust node checkpoint_digest must be a "
                                "64-digit lowercase hexadecimal digest")
    previous_digest = data["previous_digest"]
    if previous_digest is not None and not _is_digest(previous_digest):
        raise BundleFormatError("trust node previous_digest must be null "
                                "or a 64-digit lowercase hexadecimal "
                                "digest")
    if (previous_tag is None) != (previous_digest is None):
        raise BundleMismatchError("trust node predecessor relation is "
                                  "incomplete")
    if tag != '"' + checkpoint_digest + '"':
        raise BundleMismatchError("trust node tag does not bind its "
                                  "checkpoint digest")
    return {"tag": tag, "previous_tag": previous_tag,
            "checkpoint_digest": checkpoint_digest,
            "previous_digest": previous_digest}


def _serialize_state(state: dict[str, Any]) -> bytes:
    if state["state"] == _TRUST_STATE_PREPARING:
        fields = _TRUST_PREPARING_FIELDS
    else:
        fields = _TRUST_READY_FIELDS
    document = {"version": _TRUST_VERSION}
    document.update({field: state[field] for field in fields if field
                     != "version"})
    return (json.dumps(document, ensure_ascii=False,
                       separators=(",", ":"), allow_nan=False)
            + "\n").encode("utf-8")


def _validate_state(data: object) -> dict[str, Any]:
    # The lifecycle marker. A preparing record binds the strong tag, the
    # checkpoint digest and the candidate node digest; a ready record
    # additionally binds the head digest. Field shape, version, types
    # and the state literal are structural errors; a state string that
    # contradicts its field set or a tag that does not bind its digest
    # is a lifecycle contradiction, i.e. a mismatch.
    is_ready = isinstance(data, dict) \
        and list(data.keys()) == list(_TRUST_READY_FIELDS)
    if not is_ready and (not isinstance(data, dict)
                         or list(data.keys())
                         != list(_TRUST_PREPARING_FIELDS)):
        raise BundleFormatError("trust state must be an object with the "
                                "preparing or ready fields, in order")
    version = data["version"]
    if not isinstance(version, int) or isinstance(version, bool) \
            or version != _TRUST_VERSION:
        raise BundleFormatError("unsupported trust state version")
    phase = data["state"]
    if phase not in (_TRUST_STATE_PREPARING, _TRUST_STATE_READY):
        raise BundleFormatError("trust state must be preparing or ready")
    if is_ready != (phase == _TRUST_STATE_READY):
        raise BundleMismatchError("trust state contradicts its fields")
    tag = data["tag"]
    if not isinstance(tag, str) or not _ETAG_RE.fullmatch(tag):
        raise BundleFormatError("trust state tag must be a single strong "
                                "tag: a quoted 64-digit lowercase "
                                "hexadecimal digest")
    checkpoint_digest = data["checkpoint_digest"]
    if not _is_digest(checkpoint_digest):
        raise BundleFormatError("trust state checkpoint digest must be a "
                                "64-digit lowercase hexadecimal digest")
    node_digest = data["node_digest"]
    if not _is_digest(node_digest):
        raise BundleFormatError("trust state node digest must be a "
                                "64-digit lowercase hexadecimal digest")
    if tag != '"' + checkpoint_digest + '"':
        raise BundleMismatchError("trust state tag does not bind its "
                                  "checkpoint digest")
    state = {"state": phase, "tag": tag,
             "checkpoint_digest": checkpoint_digest,
             "node_digest": node_digest}
    if is_ready:
        head_digest = data["head_digest"]
        if not _is_digest(head_digest):
            raise BundleFormatError("trust state head digest must be a "
                                    "64-digit lowercase hexadecimal "
                                    "digest")
        state["head_digest"] = head_digest
    return state


def _read_trust_file(path: str) -> bytes | None:
    try:
        with open(path, "rb") as handle:
            return handle.read()
    except FileNotFoundError:
        return None


def _inspect_trust_chain(trust_real: str, head_digest: str
                         ) -> list[dict[str, Any]]:
    # Walk the retained sequence from the head node all the way back to
    # the first node and re-check every link from disk, every time:
    #
    # * each node file must exist and reparse; recomputing its digest
    #   over tag, predecessor tag, checkpoint digest and predecessor
    #   node digest must reproduce the name it is referenced by;
    # * each node's predecessor digest is followed to a real node whose
    #   own tag the successor names as its previous_tag;
    # * every node's checkpoint snapshot must exist, its raw bytes must
    #   hit the node's content address, and they must pass the complete
    #   checkpoint structure and anchor-chain validation;
    # * every pair of adjacent snapshots must satisfy the same strict
    #   append-only relation an advancing bundle is checked against --
    #   history continuity is never inferred from node metadata alone.
    #
    # The walk starts from whatever digest the head names, so a head
    # pointing into damaged history can never be masked by the bundle
    # in hand. Returns the entries in chain order, first node to head.
    reversed_entries: list[dict[str, Any]] = []
    seen: set[str] = set()
    digest: str | None = head_digest
    while digest is not None:
        if digest in seen:
            raise BundleMismatchError("trust chain contains a cycle")
        seen.add(digest)
        node_path = os.path.join(trust_real, "nodes", digest + ".json")
        node_raw = _read_trust_file(node_path)
        if node_raw is None:
            raise BundleMismatchError("trust chain references a missing "
                                      "node")
        node = _validate_node(_loads_trust(node_path, node_raw))
        if _node_digest(node["tag"], node["previous_tag"],
                        node["checkpoint_digest"],
                        node["previous_digest"]) != digest:
            raise BundleMismatchError("trust node digest does not match "
                                      "its content")
        snapshot_path = os.path.join(
            trust_real, "snapshots", node["checkpoint_digest"] + ".json")
        snapshot_raw = _read_trust_file(snapshot_path)
        if snapshot_raw is None:
            raise BundleMismatchError("trust chain references a missing "
                                      "checkpoint snapshot")
        if hashlib.sha256(snapshot_raw).hexdigest() \
                != node["checkpoint_digest"]:
            # Same content address, different bytes: evidence is never
            # overwritten or "repaired" from the bundle in hand.
            raise BundleMismatchError("trust checkpoint bytes do not hit "
                                      "their content address")
        generations = _parse_checkpoint(snapshot_path, snapshot_raw)
        reversed_entries.append(
            {"digest": digest, "node": node, "generations": generations})
        digest = node["previous_digest"]

    entries = list(reversed(reversed_entries))
    anchor_node = entries[0]["node"]
    if anchor_node["previous_tag"] is not None \
            or anchor_node["previous_digest"] is not None:
        raise BundleMismatchError("trust anchor must not name a "
                                  "predecessor")
    for earlier, later in zip(entries, entries[1:]):
        later_node = later["node"]
        if later_node["previous_digest"] != earlier["digest"] \
                or later_node["previous_tag"] != earlier["node"]["tag"]:
            raise BundleMismatchError("trust node predecessor relation "
                                      "does not continue the chain")
        # The on-disk snapshots themselves must form one strict
        # append-only history, not merely look continuous from nodes.
        _check_strict_append(earlier["generations"], later["generations"])
    return entries


def _check_ready_binding(state: dict[str, Any],
                         entries: list[dict[str, Any]]) -> None:
    # A ready state is trusted only when it binds head, head node and
    # the head snapshot all at once: its head digest names the head
    # node, and its tag and checkpoint digest are the ones the head
    # node carries. Any missing or crosswise relation fails.
    head_entry = entries[-1]
    head_node = head_entry["node"]
    head_digest = head_entry["digest"]
    if state["head_digest"] != head_digest \
            or state["node_digest"] != head_digest:
        raise BundleMismatchError("trust ready state does not bind the "
                                  "chain head")
    if state["tag"] != head_node["tag"] \
            or state["checkpoint_digest"] != head_node["checkpoint_digest"]:
        raise BundleMismatchError("trust ready state does not bind the "
                                  "head node")


def _check_strict_append(old_generations: list[dict[str, Any]],
                         new_generations: list[dict[str, Any]]) -> None:
    # The new checkpoint must be the trusted one plus only appended
    # content: every retained generation survives untouched, the head
    # generation keeps its anchors as a prefix, and at least one anchor
    # or generation is added. The appended content's own legality was
    # already established by the checkpoint validation of the bundle.
    if len(new_generations) < len(old_generations):
        raise BundleMismatchError("checkpoint drops trusted generations")
    for position, old_generation in enumerate(old_generations):
        new_generation = new_generations[position]
        if position < len(old_generations) - 1:
            if new_generation != old_generation:
                raise BundleMismatchError("checkpoint rewrites a trusted "
                                          "generation")
        else:
            if new_generation["name"] != old_generation["name"]:
                raise BundleMismatchError("checkpoint rewrites the "
                                          "trusted head generation")
            old_anchors = old_generation["anchors"]
            if new_generation["anchors"][:len(old_anchors)] != old_anchors:
                raise BundleMismatchError("checkpoint rewrites trusted "
                                          "anchors")
    if len(new_generations) == len(old_generations) \
            and len(new_generations[-1]["anchors"]) \
            == len(old_generations[-1]["anchors"]):
        raise BundleMismatchError("checkpoint is not a strict append of "
                                  "the trusted head")


def _ensure_dir(path: str) -> None:
    try:
        os.mkdir(path)
    except FileExistsError:
        return
    _fsync_dir(os.path.dirname(path) or ".")


def _store_evidence(realpath: str, payload: bytes) -> None:
    # Content-addressed evidence: the file name commits to the bytes.
    # An existing file holding exactly the payload is reused as-is (a
    # crashed earlier attempt completed the commit, or the identical
    # bundle is finishing its interrupted preparation); an existing
    # file holding different bytes is never overwritten or "repaired"
    # from the bundle in hand -- the contradiction fails and the
    # evidence stays untouched. A new file commits through the same
    # temporary-file, fsync, atomic replace and directory-fsync
    # discipline as the checkpoint.
    existing = _read_trust_file(realpath)
    if existing is not None:
        if existing == payload:
            return
        raise BundleMismatchError("a retained content address already "
                                  "holds different bytes")
    _commit_checkpoint(realpath, os.path.dirname(realpath), payload, None)


def _evidence_clear(trust_real: str, files: list[tuple[str, bytes]]
                    ) -> None:
    # Preflight the content-addressed destinations of an advance while
    # the directory is still in its pre-call ready state: an address
    # that already holds different bytes is a contradiction that must
    # fail before the lifecycle marker moves, so a blocked advance
    # leaves the trusted head replayable. An address holding the exact
    # payload is a crashed attempt's complete commit and is harmless.
    for name, payload in files:
        existing = _read_trust_file(os.path.join(trust_real, name))
        if existing is not None and existing != payload:
            raise BundleMismatchError("a retained content address "
                                      "already holds different bytes")


def _candidate_node(etag: str, digest_hex: str,
                    predecessor: dict[str, Any] | None) -> dict[str, Any]:
    # The node written for the bundle in hand: the trust anchor names
    # no predecessor, every later node names the current chain head.
    if predecessor is None:
        return {"tag": etag, "previous_tag": None,
                "checkpoint_digest": digest_hex, "previous_digest": None}
    return {"tag": etag, "previous_tag": predecessor["node"]["tag"],
            "checkpoint_digest": digest_hex,
            "previous_digest": predecessor["digest"]}


def _install_candidate(trust_real: str, candidate: dict[str, Any],
                       node_digest: str, raw: bytes,
                       head_old: bytes | None) -> None:
    # Snapshot and node are content-addressed and committed before the
    # head, and the head before the ready state: a crash at any point
    # leaves only reusable complete files and a preparing state that
    # can finish the exact same sequence idempotently.
    _ensure_dir(os.path.join(trust_real, "nodes"))
    _ensure_dir(os.path.join(trust_real, "snapshots"))
    _store_evidence(
        os.path.join(trust_real, "snapshots",
                     candidate["checkpoint_digest"] + ".json"), raw)
    _store_evidence(
        os.path.join(trust_real, "nodes", node_digest + ".json"),
        _serialize_node(candidate))
    _commit_checkpoint(os.path.join(trust_real, "head.json"), trust_real,
                       _serialize_head(node_digest), head_old)


def _update_trust(trust_dir: str, etag: str, raw: bytes,
                  generations: list[dict[str, Any]]) -> None:
    # Advance the persistent snapshot sequence through its
    # preparing/ready lifecycle, under the trust directory's own
    # in-process lock and exclusive kernel flock so concurrent
    # verifications serialize. Every verification re-inspects the
    # whole retained chain from the first node to the head before it
    # trusts anything, so damaged history or an old bundle replayed
    # against it can never be masked by the bundle in hand.
    trust_real = os.path.realpath(trust_dir)
    store = _get_store(trust_real)
    with store.lock:
        with _file_lock(trust_real):
            # Remember whether the directory existed before this call:
            # a not-yet-existing directory with a usable parent is the
            # first use and anchors through a preparing state committed
            # first; a directory that already exists with neither state
            # nor head is an empty or orphan directory and is never
            # taken over.
            fresh = not os.path.exists(trust_real)
            if not os.path.isdir(trust_real):
                # A missing parent surfaces as FileNotFoundError, either
                # from the lock-file creation above or from this mkdir;
                # an existing non-directory raises a plain OSError.
                os.mkdir(trust_real)
                _fsync_dir(os.path.dirname(trust_real) or ".")
            state_path = os.path.join(trust_real, "state.json")
            head_path = os.path.join(trust_real, "head.json")
            state_raw = _read_trust_file(state_path)
            head_raw = _read_trust_file(head_path)
            digest_hex = hashlib.sha256(raw).hexdigest()

            if state_raw is None and head_raw is None:
                if not fresh:
                    # An existing directory with neither lifecycle
                    # marker nor chain head: empty, or holding only
                    # unreferenced fragments. It is never taken over --
                    # a trust anchor is established through the
                    # preparing state committed first, not inferred
                    # from the directory's contents.
                    raise BundleMismatchError(
                        "trust directory has neither a lifecycle state "
                        "nor a chain head")
                # Genuine first use: commit the preparing state before
                # the candidate snapshot, node and head are saved, so a
                # crash at any point leaves this one explainable stage.
                candidate = _candidate_node(etag, digest_hex, None)
                node_digest = _node_digest(
                    candidate["tag"], candidate["previous_tag"],
                    candidate["checkpoint_digest"],
                    candidate["previous_digest"])
                state = {"state": _TRUST_STATE_PREPARING, "tag": etag,
                         "checkpoint_digest": digest_hex,
                         "node_digest": node_digest}
                state_raw = _serialize_state(state)
                _commit_checkpoint(state_path, trust_real, state_raw, None)
                _install_candidate(trust_real, candidate, node_digest,
                                   raw, None)
                ready = {"state": _TRUST_STATE_READY, "tag": etag,
                         "checkpoint_digest": digest_hex,
                         "node_digest": node_digest,
                         "head_digest": node_digest}
                _commit_checkpoint(state_path, trust_real,
                                   _serialize_state(ready), state_raw)
                return

            if state_raw is None:
                # A directory from before the lifecycle existed: it
                # carries a legal chain head but no state. The whole
                # chain is verified first -- every node, snapshot and
                # append relation -- and only then is the ready marker
                # stamped, binding head, head node and head snapshot.
                head_digest = _validate_head(
                    _loads_trust(head_path, head_raw))
                entries = _inspect_trust_chain(trust_real, head_digest)
                head_entry = entries[-1]
                state = {
                    "state": _TRUST_STATE_READY,
                    "tag": head_entry["node"]["tag"],
                    "checkpoint_digest":
                        head_entry["node"]["checkpoint_digest"],
                    "node_digest": head_entry["digest"],
                    "head_digest": head_digest}
                _commit_checkpoint(state_path, trust_real,
                                   _serialize_state(state), None)
                state_raw = _serialize_state(state)
            else:
                state = _validate_state(
                    _loads_trust(state_path, state_raw))

            if state["state"] == _TRUST_STATE_PREPARING:
                _resume_preparing(trust_real, state, state_raw, head_raw,
                                  etag, digest_hex, raw, generations)
                return

            # Ready: the head must exist, name the node the state
            # binds, and the full chain from the first node to that
            # head must re-check before the bundle is compared.
            if head_raw is None:
                raise BundleMismatchError("trust ready state has no chain "
                                          "head")
            head_digest = _validate_head(_loads_trust(head_path, head_raw))
            if state["head_digest"] != head_digest:
                raise BundleMismatchError("trust head does not continue "
                                          "the ready state")
            entries = _inspect_trust_chain(trust_real, head_digest)
            _check_ready_binding(state, entries)
            head_entry = entries[-1]

            if etag == head_entry["node"]["tag"]:
                # Idempotent replay of the trusted head: the bundle was
                # fully re-verified above and the retained chain has
                # just been re-inspected; nothing is written.
                return
            if any(entry["node"]["tag"] == etag for entry in entries):
                raise BundleMismatchError(
                    "checkpoint replays an earlier trusted version")
            _check_strict_append(head_entry["generations"], generations)

            # Advance: flip the ready state to preparing first, then
            # store the candidate evidence and commit head and ready.
            candidate = _candidate_node(etag, digest_hex, head_entry)
            node_digest = _node_digest(
                candidate["tag"], candidate["previous_tag"],
                candidate["checkpoint_digest"],
                candidate["previous_digest"])
            # Refuse before moving the lifecycle marker when a content
            # address already holds different bytes: the foreign
            # evidence is never overwritten, and a blocked advance
            # leaves the pre-call ready state and head in place.
            _evidence_clear(trust_real, [
                (os.path.join("snapshots", digest_hex + ".json"), raw),
                (os.path.join("nodes", node_digest + ".json"),
                 _serialize_node(candidate))])
            preparing = {"state": _TRUST_STATE_PREPARING, "tag": etag,
                         "checkpoint_digest": digest_hex,
                         "node_digest": node_digest}
            _commit_checkpoint(state_path, trust_real,
                               _serialize_state(preparing), state_raw)
            _install_candidate(trust_real, candidate, node_digest, raw,
                               head_raw)
            ready = dict(preparing, state=_TRUST_STATE_READY,
                         head_digest=node_digest)
            _commit_checkpoint(state_path, trust_real,
                               _serialize_state(ready),
                               _serialize_state(preparing))


def _resume_preparing(trust_real: str, state: dict[str, Any],
                      state_raw: bytes, head_raw: bytes | None,
                      etag: str, digest_hex: str, raw: bytes,
                      generations: list[dict[str, Any]]) -> None:
    # Complete an interrupted preparation. The preparing state binds
    # the strong tag, the checkpoint digest and the candidate node
    # digest, so only the identical bundle may finish it: a different
    # tag or snapshot is rejected with all evidence retained.
    if state["tag"] != etag or state["checkpoint_digest"] != digest_hex:
        raise BundleMismatchError("a different bundle cannot resume an "
                                  "interrupted trust preparation")
    state_path = os.path.join(trust_real, "state.json")
    head_path = os.path.join(trust_real, "head.json")
    preparing_bytes = _serialize_state(state)

    if head_raw is None:
        # Interrupted first use: the head was never committed, so the
        # candidate must be the anchor node.
        candidate = _candidate_node(etag, digest_hex, None)
        if _node_digest(candidate["tag"], candidate["previous_tag"],
                        candidate["checkpoint_digest"],
                        candidate["previous_digest"]) \
                != state["node_digest"]:
            raise BundleMismatchError("preparing state does not bind its "
                                      "candidate node")
        _install_candidate(trust_real, candidate, state["node_digest"],
                           raw, None)
    else:
        head_digest = _validate_head(_loads_trust(head_path, head_raw))
        if head_digest == state["node_digest"]:
            # The head already names the candidate node: the snapshot
            # and node commits landed before the crash. Re-inspect the
            # complete chain starting at that new head -- the stored
            # candidate must reproduce the bound digest and its
            # snapshot must hit the candidate content address -- then
            # only the ready marker remains.
            entries = _inspect_trust_chain(trust_real, head_digest)
            if entries[-1]["digest"] != state["node_digest"] \
                    or entries[-1]["node"]["tag"] != etag \
                    or entries[-1]["node"]["checkpoint_digest"] \
                    != digest_hex:
                raise BundleMismatchError("committed candidate does not "
                                          "bind the preparing state")
        else:
            # The head still names the predecessor: reconstruct the
            # candidate node from it and require it to reproduce the
            # digest the preparing state bound, re-inspecting that
            # predecessor chain and re-checking the append relation.
            entries = _inspect_trust_chain(trust_real, head_digest)
            predecessor = entries[-1]
            candidate = _candidate_node(etag, digest_hex, predecessor)
            if _node_digest(candidate["tag"], candidate["previous_tag"],
                            candidate["checkpoint_digest"],
                            candidate["previous_digest"]) \
                    != state["node_digest"]:
                raise BundleMismatchError("preparing state does not bind "
                                          "the chain head it continues")
            _check_strict_append(predecessor["generations"], generations)
            _install_candidate(trust_real, candidate,
                               state["node_digest"], raw, head_raw)

    ready = {"state": _TRUST_STATE_READY, "tag": etag,
             "checkpoint_digest": digest_hex,
             "node_digest": state["node_digest"],
             "head_digest": state["node_digest"]}
    _commit_checkpoint(state_path, trust_real, _serialize_state(ready),
                       preparing_bytes)


def verify_bundle(checkpoint_path: str, proof_path: str,
                  etag: str, trust_dir: str | None = None
                  ) -> dict[str, Any]:
    """Verify a downloaded checkpoint/proof pair entirely offline.

    ``checkpoint_path`` and ``proof_path`` must be non-empty strings and
    ``etag`` a single strong entity tag -- one double-quoted string of
    64 lowercase hexadecimal digits, exactly as the checkpoint
    download's ``ETag`` header carried it. ``trust_dir`` must be
    ``None`` or a non-empty string. Argument errors raise ``ValueError``
    before any file is read.

    The checkpoint is opened, read, hashed, parsed and structurally
    validated under the *same* shared kernel flock an export or a
    download serializes against, and the whole verification runs on
    that one snapshot: a same-directory atomic replacement racing the
    read is observed as either the complete old combination or the
    complete new one, never a mix of two versions. The tag is
    recomputed from the raw checkpoint bytes first and a mismatch fails
    immediately, before any further parsing. The proof file is then
    read under the same lock and both files are checked as UTF-8 JSON
    (negative-zero and non-finite number literals are format errors),
    the checkpoint passes the complete anchor-chain validation, and the
    proof passes the same offline checks :func:`verify` applies --
    embedded log bytes, query parameters, recomputed page, cursor and
    root digest, with the generation, anchor and closed state taken
    from the checkpoint snapshot in hand. The passed tag, the
    recomputed tag and the proof's bound ``checkpoint_etag`` must all
    three agree: an old proof against a new checkpoint, a new proof
    against an old checkpoint or a tag carried by another download
    response is a mismatch. Nothing is rewritten.

    With ``trust_dir`` the verified snapshot then moves the trust
    directory through its preparing/ready lifecycle under its own
    exclusive kernel flock:

    * a not-yet-existing directory with a present parent is the first
      use and anchors through a ``"preparing"`` state committed before
      the snapshot, node and head files; the head commit is what
      advances the sequence and the state flips to ``"ready"`` last;
    * a preparation interrupted in any commit window is completed
      idempotently by the identical bundle and rejected -- evidence
      retained -- for a bundle with a different tag or snapshot;
    * an existing empty or fragment-only directory (neither state nor
      head) is never taken over; a legacy directory with a legal head
      but no state is adopted only after its whole chain verifies, then
      stamped ready;
    * a ``"ready"`` directory requires the state, the head, the head
      node and the head snapshot to bind one another, and every
      verification re-walks the whole chain from the first node to the
      head -- node digests, predecessor tags and digests, every
      snapshot's content address and structure, and the strict
      append-only relation between adjacent snapshots;
    * a bundle carrying the head's strong tag is a fully re-verified
      idempotent replay that changes nothing; a new tag is accepted only
      when its checkpoint strictly appends to the latest trusted one.

    An already retained earlier version, a fork that rewrites trusted
    anchors, a bundle that cannot continue the head, or any
    contradiction in the retained lifecycle state, chain digests,
    snapshots, predecessor or head relations raises
    ``BundleMismatchError``. Unreferenced fragments stay out of every
    decision and a content address holding unexpected bytes is never
    overwritten or repaired from the bundle in hand.

    Returns the same result dict as :func:`verify`, plus ``etag``: the
    actual strong tag recomputed from the verified checkpoint bytes (the
    three agreeing tags are equal by then, so every success branch
    reports the one true tag). A missing file or a
    missing trust-directory parent raises ``FileNotFoundError``; an
    encoding, JSON or structural error in either file or in the trust
    metadata -- including a version 1 proof, which predates the
    snapshot binding -- raises ``BundleFormatError``; a tag,
    digest-chain, generation, anchor, closed-state, page,
    snapshot-binding or trust-sequence mismatch raises
    ``BundleMismatchError``; every other locking or I/O failure raises
    ``OSError``. Both error types are ``ValueError`` subclasses.
    """
    for value in (checkpoint_path, proof_path):
        if not isinstance(value, str) or not value:
            raise ValueError("checkpoint_path and proof_path must be "
                             "non-empty strings")
    if not isinstance(etag, str) or not _ETAG_RE.fullmatch(etag):
        raise ValueError("etag must be a single strong tag: a quoted "
                         "64-digit lowercase hexadecimal digest")
    if trust_dir is not None \
            and (not isinstance(trust_dir, str) or not trust_dir):
        raise ValueError("trust_dir must be None or a non-empty string")

    checkpoint_real = os.path.realpath(checkpoint_path)
    proof_real = os.path.realpath(proof_path)
    with _file_lock(checkpoint_real, shared=True):
        # One locked section for the whole bundle: the bytes whose tag
        # is recomputed are the bytes that are parsed, and the proof is
        # checked against that same snapshot.
        with open(checkpoint_real, "rb") as handle:
            raw = handle.read()
        tag = _strong_tag(raw)
        if etag != tag:
            raise BundleMismatchError("checkpoint bytes do not match "
                                      "the etag")
        generations = _validate_checkpoint(_loads_bundle(raw))
        with open(proof_real, "rb") as handle:
            proof_raw = handle.read()
        clean = _validate_proof(_loads_bundle(proof_raw))
        if clean["checkpoint_etag"] != tag:
            raise BundleMismatchError(
                "proof checkpoint_etag does not bind the checkpoint "
                "snapshot in hand")
        result = _verify_proof(generations, clean, checkpoint_real)
    if trust_dir is not None:
        # The existing single-bundle verification is complete; only now
        # compare and advance the persistent sequence.
        _update_trust(trust_dir, etag, raw, generations)
    # Every success branch reports the actual strong tag recomputed from
    # the verified checkpoint bytes alongside the proof's own fields.
    result["etag"] = tag
    return result


def read_snapshot(checkpoint_path: str) -> tuple[bytes, str]:
    """Read the fixed checkpoint for a conditional, read-only download.

    The checkpoint is opened under the *same* shared kernel flock an
    export uses, so a concurrent exporter serializes against the read
    and the caller observes either the complete old file or the
    complete new one, never a half-replaced document. The raw bytes are
    read, decoded as UTF-8, parsed and structurally validated -- the
    whole anchor/digest chain included -- and the SHA-256 of the exact
    response bytes is computed, all while the shared lock is held. The
    bytes themselves are returned untouched: the caller serves them
    verbatim, never re-serialized.

    ``checkpoint_path`` must be a non-empty string (a bad argument
    raises ``ValueError`` before any file is read). A missing
    checkpoint raises ``FileNotFoundError``; bytes that are not valid
    UTF-8 or JSON, or a checkpoint whose version, field order,
    generation names, anchors or digest chain do not validate, raise
    ``ValueError``; every other locking or I/O failure raises
    ``OSError``.
    """
    if not isinstance(checkpoint_path, str) or not checkpoint_path:
        raise ValueError("checkpoint_path must be a non-empty string")

    checkpoint_real = os.path.realpath(checkpoint_path)
    with _file_lock(checkpoint_real, shared=True):
        # Open, read, validate and hash under one shared lock: the
        # bytes the ETag summarizes are the validated bytes and both
        # come from the same complete file version.
        with open(checkpoint_real, "rb") as handle:
            raw = handle.read()
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise BundleFormatError(
                f"checkpoint file {checkpoint_real!r} is not valid UTF-8"
            ) from exc
        try:
            data = strict_loads(text)
        except ValueError as exc:
            raise BundleFormatError(
                f"checkpoint file {checkpoint_real!r} is not valid JSON"
            ) from exc
        _validate_checkpoint(data)
        etag = hashlib.sha256(raw).hexdigest()
    return raw, etag
