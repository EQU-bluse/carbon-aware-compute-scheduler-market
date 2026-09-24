"""Independent checkpoints and range proofs over the audit journal.

:mod:`carbon_market.audit` already seals its own events into a digest
chain and answers read-only integrity queries, but a digest chain in the
journal can only attest what that file currently holds. This module adds
two pieces that live outside the journal: a *checkpoint* that appends a
chain of snapshot anchors, and an offline *proof* a caller can save and
have verified later with nothing but the proof and that checkpoint.

:func:`export` takes the audit journal path, a checkpoint path, a
non-empty generation name and a ``final`` flag, together with the very
same retrieval arguments :func:`carbon_market.audit.search` takes --
``cursor``, ``limit`` and the ``op``/``stage``/``key`` filters. It reads
the complete journal under a shared lock and returns a proof
encapsulating the query, the result page (events and next cursor), the
generation and its closed state, the journal's root digest and the
journal's exact raw bytes (base64). Argument types, an empty generation,
a non-boolean ``final`` and out-of-range retrieval arguments all raise
``ValueError`` before any file is opened.

The checkpoint is ``{"version": 1, "anchors": [anchor, ...]}`` in
compact UTF-8 JSON with a trailing newline; it is created when missing.
Every anchor binds, in this order, the generation, the query, the page's
event manifest, the SHA-256 of the journal's raw bytes, the journal's
root digest, the previous anchor digest, the generation's closed state
and -- last -- the anchor's own digest. That digest is the SHA-256 of
the other seven fields encoded with the journal's compact UTF-8 JSON
convention, so it authenticates the query as well as the range and links
to the preceding anchor no matter how the journal itself was rotated
underneath. The first anchor's previous digest is null; every later one
names the digest of the anchor before it on disk.

Within one generation the manifest may only stay item-for-item
unchanged or gain new audit keys: an event dropped, rewritten or moved
back raises ``ValueError``. A repeated export whose content *and* closed
state are unchanged is the idempotent case: it appends no anchor and
returns a proof identical to the earlier one. ``final=True`` closes the
generation -- when the content is unchanged the state change still
appends one closing anchor -- and after a generation is closed only an
identical idempotent export is accepted; any changed content raises
``ValueError``. A new generation can only be opened after the previous
one is closed, and generation names must be non-empty, never repeated
and strictly advance in code-point order; a duplicate or regressing
name raises ``ValueError``.

Export only accepts a sealed version 2 journal whose digest chain is
complete. A version 1 journal, a broken chain or a structurally
abnormal checkpoint raises ``ValueError``. A missing audit journal, a
checkpoint missing where :func:`verify` requires one, or a missing
checkpoint parent directory when one must be created, raises
``FileNotFoundError``; every other locking or I/O failure remains an
``OSError``.

An export holds the journal's shared kernel lock and the checkpoint's
exclusive kernel lock (together with a per-realpath thread lock) for
the whole read/validate/append sequence, so a concurrent exporter
observes only the complete previous or the complete new checkpoint, and
a concurrent :func:`carbon_market.audit.record` cannot rotate the
journal under it. The new checkpoint is staged in a same-directory
temporary file, fsynced, moved over the checkpoint with
:func:`os.replace` and followed by a directory fsync; should the commit
fail after the replace the pre-call bytes are staged back (or the new
file removed), so a failed export leaves no new anchor and returns no
proof. The chain is rebuilt purely from the persisted anchors, so a
restarted process continues the same anchor chain.

:func:`verify` takes only the checkpoint path and a proof and never
opens the audit journal: it validates the checkpoint's whole anchor
chain, locates the anchor the proof names, recomputes the journal digest
from the proof's raw bytes, parses that journal (which must be a
complete version 2 document whose head matches the anchor root),
re-runs the proof's query over it and compares the page and next cursor
with the proof and the anchor. Altering the log bytes, any parameter or
cursor, the page content, the next cursor or any digest breaks one of
these comparisons and raises ``ValueError``. On success it returns the
original retrieval result (``events`` and ``next``) together with the
generation and the closed state.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import os
from typing import Any

from . import audit
from ._jsonio import strict_loads

__all__ = ["export", "verify"]

_PROOF_VERSION = 1
_CHECKPOINT_VERSION = 1
_CP_ROOT_FIELDS = ("version", "anchors")
_QUERY_FIELDS = ("cursor", "limit", "op", "stage", "key")
_ANCHOR_FIELDS = ("generation", "query", "events", "log_digest", "root",
                  "previous", "closed", "digest")
_PROOF_FIELDS = ("version", "generation", "closed", "query", "events",
                 "next", "root", "log", "anchor")


# ---------------------------------------------------------------------
# Input validation (all of it runs before any file is opened)
# ---------------------------------------------------------------------

def _validate_retrieval(cursor: object, limit: object, op: object,
                        stage: object, key: object) -> dict[str, Any]:
    # The same contract audit.search enforces; the query object stored in
    # every anchor and proof always carries all five parameters in their
    # public order, so authenticating the anchor authenticates the query.
    if cursor is not None and (not isinstance(cursor, str) or not cursor):
        raise ValueError("cursor must be None or a non-empty string")
    # bool is a subclass of int and must be rejected as a page size.
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
    return {"cursor": cursor, "limit": limit, "op": op,
            "stage": stage, "key": key}


def _validate_export_arguments(
    audit_path: object, checkpoint_path: object, generation: object,
    final: object, cursor: object, limit: object, op: object,
    stage: object, key: object,
) -> None:
    if not isinstance(audit_path, str) or not audit_path:
        raise ValueError("audit_path must be a non-empty string")
    if not isinstance(checkpoint_path, str) or not checkpoint_path:
        raise ValueError("checkpoint_path must be a non-empty string")
    if not isinstance(generation, str) or not generation:
        raise ValueError("generation must be a non-empty string")
    # bool is checked exactly: integers must not pass as final.
    if not isinstance(final, bool):
        raise ValueError("final must be a boolean")
    _validate_retrieval(cursor, limit, op, stage, key)


# ---------------------------------------------------------------------
# Canonical digests and structural validation
# ---------------------------------------------------------------------

def _compact_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _anchor_digest(anchor: dict[str, Any]) -> str:
    # The anchor's own field is excluded; the other seven fields in their
    # public order are the signed payload -- generation, query, manifest,
    # log digest, root digest, previous anchor digest and closed state.
    payload = {field: anchor[field]
               for field in _ANCHOR_FIELDS if field != "digest"}
    return hashlib.sha256(_compact_json(payload)).hexdigest()


def _validate_query_object(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict) or list(raw.keys()) != list(_QUERY_FIELDS):
        raise ValueError(
            "anchor query must be an object with keys cursor, limit, op, "
            "stage and key, in that order")
    query = _validate_retrieval(*(raw[field] for field in _QUERY_FIELDS))
    return query


def _validate_manifest(raw: object) -> list[list[Any]]:
    # The manifest is the page: a list of [audit_key, event] pairs in
    # ascending audit-key code-point order, each event carrying the
    # public field order the journal contract enforces on read.
    if not isinstance(raw, list):
        raise ValueError("anchor events must be a list of [audit key, "
                         "event] pairs")
    manifest: list[list[Any]] = []
    seen: set[str] = set()
    for entry in raw:
        if not isinstance(entry, list) or len(entry) != 2:
            raise ValueError("anchor events must be [audit key, event] "
                             "pairs")
        event_key, event_raw = entry
        if not isinstance(event_key, str) or not event_key \
                or event_key in seen:
            raise ValueError("anchor event keys must be unique non-empty "
                             "strings")
        seen.add(event_key)
        event = audit._validate_event(event_raw, strict_order=True)
        manifest.append([event_key, event])
    keys = [event_key for event_key, _event in manifest]
    if keys != sorted(keys):
        raise ValueError("anchor events must be ordered by audit key "
                         "code point")
    return manifest


def _validate_anchor(raw: object, expected_previous: str | None,
                     previous_generation: str | None,
                     previous_closed: bool) -> dict[str, Any]:
    if not isinstance(raw, dict) or list(raw.keys()) != list(_ANCHOR_FIELDS):
        raise ValueError(
            "anchor must be an object with keys generation, query, "
            "events, log_digest, root, previous, closed and digest, in "
            "that order")
    generation = raw["generation"]
    if not isinstance(generation, str) or not generation:
        raise ValueError("anchor generation must be a non-empty string")
    query = _validate_query_object(raw["query"])
    manifest = _validate_manifest(raw["events"])
    log_digest = raw["log_digest"]
    root = raw["root"]
    previous = raw["previous"]
    closed = raw["closed"]
    digest = raw["digest"]
    if not audit._is_digest(log_digest):
        raise ValueError("anchor log_digest must be a 64-digit lowercase "
                         "hexadecimal digest")
    if root is not None and not audit._is_digest(root):
        raise ValueError("anchor root must be null or a 64-digit "
                         "lowercase hexadecimal digest")
    if previous is not None and not audit._is_digest(previous):
        raise ValueError("anchor previous digest must be null or a "
                         "64-digit lowercase hexadecimal digest")
    if not isinstance(closed, bool):
        raise ValueError("anchor closed must be a boolean")
    if not audit._is_digest(digest):
        raise ValueError("anchor digest must be a 64-digit lowercase "
                         "hexadecimal digest")

    anchor = {"generation": generation, "query": query, "events": manifest,
              "log_digest": log_digest, "root": root, "previous": previous,
              "closed": closed, "digest": digest}
    if previous != expected_previous:
        raise ValueError("anchor previous digest does not link to the "
                         "preceding anchor")
    if _anchor_digest(anchor) != digest:
        raise ValueError("anchor digest does not match its content")

    # The generation segments of a healthy checkpoint advance in strict
    # code-point order, and a closed anchor ends its segment: nothing may
    # be appended under a closed generation, and a new generation name
    # may only follow a closed anchor with a strictly greater name.
    if previous_generation is not None:
        if generation == previous_generation:
            if previous_closed:
                raise ValueError("no anchor may follow a closed anchor "
                                 "under the same generation")
        else:
            if not previous_closed:
                raise ValueError("a new generation requires the previous "
                                 "generation to be closed")
            if generation < previous_generation:
                raise ValueError("generation names must not repeat or "
                                 "regress")
    return anchor


def _validate_checkpoint(data: object) -> dict[str, Any]:
    if not isinstance(data, dict) \
            or list(data.keys()) != list(_CP_ROOT_FIELDS):
        raise ValueError("checkpoint root must be an object with keys "
                         "version and anchors, in that order")
    version = data["version"]
    # bool is a subclass of int and must be rejected as a version.
    if not isinstance(version, int) or isinstance(version, bool) \
            or version != _CHECKPOINT_VERSION:
        raise ValueError("unsupported checkpoint version")
    raw_anchors = data["anchors"]
    if not isinstance(raw_anchors, list):
        raise ValueError("checkpoint anchors must be a list")

    anchors: list[dict[str, Any]] = []
    previous_digest: str | None = None
    previous_generation: str | None = None
    previous_closed = False
    for raw_anchor in raw_anchors:
        anchor = _validate_anchor(raw_anchor, previous_digest,
                                  previous_generation, previous_closed)
        anchors.append(anchor)
        previous_digest = anchor["digest"]
        previous_generation = anchor["generation"]
        previous_closed = anchor["closed"]
    return {"version": _CHECKPOINT_VERSION, "anchors": anchors}


def _parse_checkpoint(realpath: str, raw: bytes) -> dict[str, Any]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"checkpoint {realpath!r} is not valid UTF-8") from exc
    try:
        data = strict_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"checkpoint {realpath!r} is not valid JSON") from exc
    return _validate_checkpoint(data)


def _serialize_checkpoint(anchors: list[dict[str, Any]]) -> bytes:
    # Compact UTF-8 JSON, non-ASCII written through, anchors kept in
    # append order (which is also the previous-digest chain order), each
    # anchor in its public field order, terminated by exactly one newline.
    document = {
        "version": _CHECKPOINT_VERSION,
        "anchors": [{field: anchor[field] for field in _ANCHOR_FIELDS}
                    for anchor in anchors],
    }
    return json.dumps(document, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False).encode("utf-8") + b"\n"


# ---------------------------------------------------------------------
# Journal reading and querying
# ---------------------------------------------------------------------

def _read_sealed_journal(realpath: str) -> tuple[dict[str, Any], bytes]:
    # Called with the shared flock held: the bytes are one complete
    # pre- or post-rotation document. Only a complete version 2 chain is
    # exportable; version 1 and a broken chain are caller errors here.
    with open(realpath, "rb") as handle:
        raw = handle.read()
    document = audit._parse_document(realpath, raw)
    if document["version"] != 2:
        raise ValueError("audit proof export only accepts a sealed "
                         "version 2 audit journal")
    audit._ensure_chain(document)
    return document, raw


def _run_query(document: dict[str, Any], query: dict[str, Any]) -> tuple[
        list[list[Any]], str | None]:
    # Same scan/filter/page contract as audit.search, evaluated over an
    # already-validated document; the persisted events are in ascending
    # audit-key code-point order and one extra match detects the last page.
    cursor = query["cursor"]
    limit = query["limit"]
    matches: list[list[Any]] = []
    for event_key, event in document["events"].items():
        if cursor is not None and event_key <= cursor:
            continue
        if not audit._matches(event, query["op"], query["stage"],
                              query["key"]):
            continue
        matches.append([event_key, dict(event)])
        if len(matches) > limit:
            break
    if len(matches) > limit:
        page = matches[:limit]
        return page, page[-1][0]
    return matches, None


def _ensure_manifest_extends(previous: list[list[Any]],
                             current: list[list[Any]]) -> None:
    # Within one generation the old manifest must survive item for item
    # and in order; the new page may only interleave or append new audit
    # keys. That is exactly "previous is a subsequence of current with
    # each [key, event] pair equal": a dropped key, a rewritten event or a
    # reordered page leaves some previous pair unconsumed. Both lists
    # were already validated to carry unique keys in code-point order.
    cursor = iter(current)
    for expected_key, expected_event in previous:
        for event_key, event in cursor:
            if event_key == expected_key:
                if event != expected_event:
                    raise ValueError(
                        "a generation only accepts its existing events "
                        "unchanged and new audit keys")
                break
        else:
            raise ValueError("a generation only accepts its existing "
                             "events unchanged and new audit keys")


# ---------------------------------------------------------------------
# Proof envelopes
# ---------------------------------------------------------------------

def _build_proof(anchor: dict[str, Any], raw_log: bytes,
                 next_cursor: str | None) -> dict[str, Any]:
    return {
        "version": _PROOF_VERSION,
        "generation": anchor["generation"],
        "closed": anchor["closed"],
        "query": dict(anchor["query"]),
        "events": [[event_key, dict(event)]
                   for event_key, event in anchor["events"]],
        "next": next_cursor,
        "root": anchor["root"],
        "log": base64.b64encode(raw_log).decode("ascii"),
        "anchor": anchor["digest"],
    }


def _validate_proof(data: object) -> dict[str, Any]:
    if not isinstance(data, dict) or list(data.keys()) != list(_PROOF_FIELDS):
        raise ValueError(
            "proof must be an object with keys version, generation, "
            "closed, query, events, next, root, log and anchor, in that "
            "order")
    version = data["version"]
    if not isinstance(version, int) or isinstance(version, bool) \
            or version != _PROOF_VERSION:
        raise ValueError("unsupported proof version")
    generation = data["generation"]
    if not isinstance(generation, str) or not generation:
        raise ValueError("proof generation must be a non-empty string")
    closed = data["closed"]
    if not isinstance(closed, bool):
        raise ValueError("proof closed must be a boolean")
    query = _validate_query_object(data["query"])
    manifest = _validate_manifest(data["events"])
    next_cursor = data["next"]
    if next_cursor is not None and (not isinstance(next_cursor, str)
                                    or not next_cursor):
        raise ValueError("proof next cursor must be null or a non-empty "
                         "string")
    root = data["root"]
    if root is not None and not audit._is_digest(root):
        raise ValueError("proof root must be null or a 64-digit lowercase "
                         "hexadecimal digest")
    log_text = data["log"]
    if not isinstance(log_text, str) or not log_text:
        raise ValueError("proof log must be a non-empty base64 string")
    anchor_digest = data["anchor"]
    if not audit._is_digest(anchor_digest):
        raise ValueError("proof anchor must be a 64-digit lowercase "
                         "hexadecimal digest")
    return {"version": _PROOF_VERSION, "generation": generation,
            "closed": closed, "query": query, "events": manifest,
            "next": next_cursor, "root": root, "log": log_text,
            "anchor": anchor_digest}


# ---------------------------------------------------------------------
# export
# ---------------------------------------------------------------------

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
    """Export one range proof and append its checkpoint anchor.

    ``audit_path`` and ``checkpoint_path`` must be non-empty strings,
    ``generation`` a non-empty string and ``final`` a boolean; the
    retrieval arguments have exactly the contract of
    :func:`carbon_market.audit.search` -- ``cursor`` ``None`` or a
    non-empty string, ``limit`` an integer from 1 to 1000 and ``op``,
    ``stage`` and ``key`` optional legal filters. Any violation raises
    ``ValueError`` before a file is opened.

    The audit journal must exist as a sealed version 2 document with a
    complete digest chain, else ``FileNotFoundError`` or ``ValueError``;
    a version 1 journal or a broken chain is never exported. The
    checkpoint is created when missing (its parent directory must
    exist) and parsed strictly when present; any structural anomaly
    raises ``ValueError``.

    Returns the offline proof. Within ``generation`` the page may only
    preserve its existing events item for item and add new audit keys;
    an unchanged repeated export appends nothing and returns an
    identical proof; ``final=True`` closes the generation, after which
    only an identical idempotent export is accepted. A new generation
    requires the previous one closed and a strictly greater name.

    The call holds the journal's shared lock and the checkpoint's
    exclusive lock for its whole duration; a commit failure restores
    the checkpoint's pre-call bytes (or removes a newly created one),
    so it never leaves a new anchor or return a proof.
    """
    _validate_export_arguments(audit_path, checkpoint_path, generation,
                               final, cursor, limit, op, stage, key)
    query = _validate_retrieval(cursor, limit, op, stage, key)

    log_realpath = os.path.realpath(audit_path)
    checkpoint_realpath = os.path.realpath(checkpoint_path)
    if log_realpath == checkpoint_realpath:
        raise ValueError("checkpoint_path must not name the audit journal")

    directory = os.path.dirname(checkpoint_realpath) or "."

    # The per-realpath thread lock serializes exporters in this process;
    # the kernel flocks (journal shared, checkpoint exclusive) serialize
    # every process. The two flock domains are entered in one global
    # realpath order so two exporters can never wait on each other in a
    # cycle.
    with audit._get_store(checkpoint_realpath).lock:
        ordered = sorted(
            ((log_realpath, True), (checkpoint_realpath, False)),
            key=lambda pair: pair[0])
        with contextlib.ExitStack() as stack:
            for realpath, shared in ordered:
                stack.enter_context(
                    audit._file_lock(realpath, shared=shared))

            document, raw_log = _read_sealed_journal(log_realpath)
            manifest, next_cursor = _run_query(document, query)
            root = document["head"]
            log_digest = hashlib.sha256(raw_log).hexdigest()

            try:
                with open(checkpoint_realpath, "rb") as handle:
                    checkpoint_raw = handle.read()
            except FileNotFoundError:
                checkpoint_raw = None
            if checkpoint_raw is None:
                anchors: list[dict[str, Any]] = []
            else:
                anchors = _parse_checkpoint(
                    checkpoint_realpath, checkpoint_raw)["anchors"]

            last = anchors[-1] if anchors else None
            if last is None:
                closed = bool(final)
                previous: str | None = None
                append = True
            elif last["generation"] == generation:
                previous = last["digest"]
                identical = (
                    last["query"] == query
                    and last["events"] == manifest
                    and last["log_digest"] == log_digest
                    and last["root"] == root)
                if last["closed"]:
                    # A closed generation admits only the exactly
                    # identical idempotent export; its closed state
                    # cannot change back, so it matches either final flag.
                    if not identical:
                        raise ValueError(
                            "generation is closed; only an identical "
                            "idempotent export is allowed")
                    append = False
                    closed = True
                else:
                    desired_closed = bool(final)
                    if identical and last["closed"] == desired_closed:
                        append = False
                        closed = False
                    else:
                        if not identical:
                            _ensure_manifest_extends(last["events"],
                                                     manifest)
                        closed = desired_closed
                        append = True
            else:
                if not last["closed"]:
                    raise ValueError("a new generation can only be opened "
                                     "after the previous generation is "
                                     "closed")
                if generation < last["generation"]:
                    raise ValueError("generation names must not repeat or "
                                     "regress")
                previous = last["digest"]
                closed = bool(final)
                append = True

            if append:
                anchor: dict[str, Any] = {
                    "generation": generation,
                    "query": query,
                    "events": manifest,
                    "log_digest": log_digest,
                    "root": root,
                    "previous": previous,
                    "closed": closed,
                }
                anchor["digest"] = _anchor_digest(anchor)
                payload = _serialize_checkpoint(anchors + [anchor])
                # Temp-file fsync, atomic replace, directory fsync -- with
                # a staged rollback should the post-replace sync fail, so
                # an unsuccessful commit leaves no anchor behind.
                audit._commit(checkpoint_realpath, directory, payload,
                              checkpoint_raw, None)
                current_anchor = anchor
            else:
                current_anchor = last

            return _build_proof(current_anchor, raw_log, next_cursor)


# ---------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------

def verify(checkpoint_path: str, proof: object) -> dict[str, Any]:
    """Verify an offline proof against the checkpoint's anchor chain.

    ``checkpoint_path`` must be a non-empty string, else ``ValueError``;
    a missing checkpoint (or a missing parent directory) raises
    ``FileNotFoundError``. The proof is the object returned by
    :func:`export`; a malformed proof, a malformed checkpoint, an anchor
    chain that does not verify, a proof the checkpoint cannot locate,
    mismatched log bytes, parameters, page, cursor, root or generation,
    a non-version-2 journal, an incomplete journal digest chain or a
    journal head disagreement all raise ``ValueError``; any other I/O
    failure raises ``OSError``. The audit journal itself is never
    opened.

    On success returns the original retrieval result together with the
    provenance: keys ``events``, ``next``, ``generation`` and
    ``closed``, in that order.
    """
    if not isinstance(checkpoint_path, str) or not checkpoint_path:
        raise ValueError("checkpoint_path must be a non-empty string")
    realpath = os.path.realpath(checkpoint_path)

    # Read one complete checkpoint under the shared lock so an exporter's
    # atomic replace can never expose half of an anchor list.
    with audit._file_lock(realpath, shared=True):
        with open(realpath, "rb") as handle:
            checkpoint_raw = handle.read()
    checkpoint = _parse_checkpoint(realpath, checkpoint_raw)

    envelope = _validate_proof(proof)

    try:
        raw_log = base64.b64decode(envelope["log"], validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError("proof log is not valid base64") from exc
    if not raw_log:
        raise ValueError("proof log must not be empty")
    log_digest = hashlib.sha256(raw_log).hexdigest()

    anchor = next(
        (candidate for candidate in checkpoint["anchors"]
         if candidate["digest"] == envelope["anchor"]), None)
    if anchor is None:
        raise ValueError("proof anchor is not recorded in the checkpoint")

    # Every proof field the anchor speaks for must match the anchor the
    # checkpoint authenticated when its own chain was validated.
    if anchor["log_digest"] != log_digest:
        raise ValueError("proof log bytes do not match the anchor digest")
    for field in ("generation", "closed", "query", "events", "root"):
        if anchor[field] != envelope[field]:
            raise ValueError(f"proof {field} does not match its anchor")

    # Independently re-verify the journal sealed inside the proof: it
    # must be a complete version 2 document, its head must be the bound
    # root digest, and re-running the bound query over it must reproduce
    # exactly the page and the next cursor the proof carries.
    document = audit._parse_document("<audit-proof>", raw_log)
    if document["version"] != 2:
        raise ValueError("proof log must be a sealed version 2 audit "
                         "journal")
    audit._ensure_chain(document)
    if document["head"] != anchor["root"]:
        raise ValueError("proof log root digest does not match its anchor")
    page, next_cursor = _run_query(document, anchor["query"])
    if page != envelope["events"] or next_cursor != envelope["next"]:
        raise ValueError("proof result does not match the bound query run "
                         "over the proof log")

    return {
        "events": [[event_key, dict(event)]
                   for event_key, event in envelope["events"]],
        "next": envelope["next"],
        "generation": envelope["generation"],
        "closed": envelope["closed"],
    }
