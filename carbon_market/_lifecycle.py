"""Shared persistence machinery for the lifecycle ledgers.

The dispatch, execution and completion ledgers persist the same
three-section shape (records, idempotency bindings and audit events)
with the same durability discipline, so their locking, strict reading
and atomic-commit code lives here once:

* one process-local mutex per resolved real path, shared by every
  lifecycle layer, serializes same-path callers before the flock;
* the companion ``<path>.lock`` file provides the cross-process shared
  and exclusive flock, never unlinked (the kernel releases the flock
  on process exit);
* multi-ledger snapshots take every lock in one resolved-real-path
  order with exactly one exclusive target, so concurrent lifecycle
  operations can never invert the lock order;
* reads accept only the strict canonical form: UTF-8, finite JSON
  without negative-zero literals, validated structure and exact bytes;
* writes land through a synced same-directory temporary file, an atomic
  replace and a directory fsync; a failure before the replace leaves
  the pre-call bytes in place, a directory-sync failure after the
  replace restores the exact pre-call bytes (or absence), and a failed
  recovery chains after the original error.

This module is private to the three lifecycle ledgers; it adds no public
entry point and defines no on-disk format of its own.
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import tempfile
import threading
from typing import Any, Callable, Iterator

from ._jsonio import finite_loads

__all__ = [
    "get_store",
    "is_plain_int",
    "check_sorted_keys",
    "file_lock",
    "lifecycle_locks",
    "read_raw",
    "parse_ledger",
    "fsync_directory",
    "rollback_file",
    "commit_file",
]

_LOCK_SUFFIX = ".lock"


class _Store:
    """Process-local mutex for one resolved real path."""

    def __init__(self, realpath: str) -> None:
        self.realpath = realpath
        self.lock = threading.Lock()


_stores_lock = threading.Lock()
_stores: dict[str, _Store] = {}


def get_store(path: str) -> _Store:
    # Equivalent real paths share one mutex across threads and across
    # the lifecycle modules; only the resolved path is ever stored.
    realpath = os.path.realpath(path)
    with _stores_lock:
        store = _stores.get(realpath)
        if store is None:
            store = _Store(realpath)
            _stores[realpath] = store
        return store


def is_plain_int(value: object) -> bool:
    # bool is a subclass of int and must be rejected.
    return isinstance(value, int) and not isinstance(value, bool)


def check_sorted_keys(mapping: dict[Any, Any], label: str) -> None:
    keys = list(mapping)
    if keys != sorted(keys):
        raise ValueError(f"{label} must be ordered by key code point")


@contextlib.contextmanager
def file_lock(realpath: str, *, shared: bool = False) -> Iterator[None]:
    # The companion lock file is never unlinked and an flock is released
    # by the kernel on process exit, so equivalent real paths share one
    # lock across threads, processes and modules -- every layer uses the
    # same suffix for the same file.
    lock_path = realpath + _LOCK_SUFFIX
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


@contextlib.contextmanager
def lifecycle_locks(
    reals: Any,
    *,
    exclusive: str,
    lock: Callable[..., Any],
) -> Iterator[None]:
    # Locks are taken in one resolved-real-path order shared by every
    # lifecycle caller, so concurrent operations can never deadlock; the
    # caller's own ledger is taken exclusively and the snapshots shared.
    # ``lock`` is resolved on entry, so a module-level lock probe patched
    # in by a test is honored exactly as a direct call would be.
    with contextlib.ExitStack() as stack:
        for real in sorted(set(reals)):
            stack.enter_context(lock(real, shared=(real != exclusive)))
        yield


def read_raw(realpath: str) -> bytes | None:
    # Read-only: a missing business ledger returns None and no file is
    # ever created or rewritten.
    try:
        with open(realpath, "rb") as handle:
            return handle.read()
    except FileNotFoundError:
        return None


def parse_ledger(
    raw: bytes,
    realpath: str,
    *,
    kind: str,
    validate: Callable[[object], Any],
    serialize: Callable[..., bytes],
) -> Any:
    # Decode, parse and validate strictly, then accept the document only
    # when its bytes are exactly the canonical compact form the ledger
    # writes: non-UTF-8 bytes, malformed or non-finite JSON, wrong
    # structure, ordering or a missing trailing newline all fail.
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"{kind} ledger {realpath!r} is not valid UTF-8") from exc
    try:
        # Negative-zero and non-finite literals are format errors.
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"{kind} ledger {realpath!r} is not valid JSON") from exc
    sections = validate(data)
    if raw != serialize(*sections):
        raise ValueError(
            f"{kind} ledger {realpath!r} is not in canonical compact "
            "form")
    return sections


def fsync_directory(directory: str) -> None:
    dir_fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def rollback_file(
    realpath: str,
    directory: str,
    old_bytes: bytes | None,
    first: BaseException,
    *,
    prefix: str,
    fsync_dir: Callable[[str], None] = fsync_directory,
) -> None:
    # Restore the exact pre-call bytes while the exclusive lock is held,
    # or remove a ledger that did not exist beforehand, then sync the
    # directory. A failed recovery chains after the original error.
    try:
        if old_bytes is None:
            try:
                os.unlink(realpath)
            except FileNotFoundError:
                pass
        else:
            fd, tmp_path = tempfile.mkstemp(
                dir=directory, prefix=prefix, suffix=".tmp")
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(old_bytes)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp_path, realpath)
            except BaseException:
                with contextlib.suppress(OSError):
                    os.unlink(tmp_path)
                raise
        fsync_dir(directory)
    except OSError as recovery:
        raise recovery from first


def commit_file(
    realpath: str,
    payload: bytes,
    old_bytes: bytes | None,
    *,
    prefix: str,
    fsync_dir: Callable[[str], None],
    rollback: Callable[..., None],
) -> None:
    # One durable commit for the record, the idempotency binding and the
    # audit event: synced same-directory temporary, atomic replace and a
    # directory fsync, restoring the pre-call bytes on any failure, so a
    # failed call leaves neither a fragment nor half an event and an
    # interruption observes only the pre- or post-commit bytes.
    directory = os.path.dirname(realpath) or "."
    fd, tmp_path = tempfile.mkstemp(
        dir=directory, prefix=prefix, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, realpath)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)
        raise
    try:
        fsync_dir(directory)
    except BaseException as first:
        rollback(realpath, directory, old_bytes, first,
                  fsync_dir=fsync_dir)
        raise
