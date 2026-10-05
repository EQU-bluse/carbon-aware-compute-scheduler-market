"""Persistence infrastructure shared by the persistent ledgers.

Every persistent ledger -- the dispatch, execution and completion
ledgers, the live market inputs (the resource-supply registry and the
signal ledger), and the audit, audit-proof, jobs, market, rebalance,
execution-sync, migration-batch, recover-all and signal-ingest ledgers
-- protects its file the same way: one in-process mutex per resolved
real path, a companion ``.lock`` file carrying ``flock``
shared/exclusive locks across processes, and a synced same-directory
temporary file atomically renamed over the ledger with its directory
fsynced and the pre-call bytes restored on failure.

This module holds exactly that shared machinery:

* :class:`Store` and :func:`get_store` -- the per-real-path mutex
  registry;
* :func:`file_lock` -- the companion-lock flock;
* :func:`is_plain_int` and :func:`check_sorted_keys` -- the small input
  checks every ledger repeats;
* :func:`load_canonical` -- read, decode, validate and re-canonicalize
  one ledger, preserving each caller's missing-file handling and error
  wording;
* :func:`fsync_directory`, :func:`commit_file` -- the durable atomic
  commit and its rollback.

It is an internal module: it defines no business entry point, owns no
ledger format and opens no file that callers did not name.
"""

from __future__ import annotations

import contextlib
import fcntl
import os
import tempfile
import threading
from typing import Any, Callable, Iterator, TypeVar

from ._jsonio import finite_loads

__all__ = [
    "Store", "get_store", "file_lock", "is_plain_int", "check_sorted_keys",
    "load_canonical", "fsync_directory", "commit_file",
]

_LOCK_SUFFIX = ".lock"

_T = TypeVar("_T")


class Store:
    """The in-process mutex bound to one resolved ledger path.

    Equivalent real paths -- one path given through a symlink and
    another given directly -- resolve to one store, so threads in this
    process serialize on one lock while the companion flock serializes
    against other processes.
    """

    def __init__(self, realpath: str) -> None:
        self.realpath = realpath
        self.lock = threading.Lock()


_stores_lock = threading.Lock()
_stores: dict[str, Store] = {}


def get_store(path: str) -> Store:
    realpath = os.path.realpath(path)
    with _stores_lock:
        store = _stores.get(realpath)
        if store is None:
            store = Store(realpath)
            _stores[realpath] = store
        return store


def is_plain_int(value: object) -> bool:
    # bool is a subclass of int and must be rejected.
    return isinstance(value, int) and not isinstance(value, bool)


@contextlib.contextmanager
def file_lock(realpath: str, *, shared: bool = False) -> Iterator[None]:
    # The companion lock file is never unlinked and an flock is released
    # by the kernel on process exit, so equivalent real paths share one
    # lock across threads and processes -- and across modules, since
    # every layer uses the same suffix for the same file.
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


def check_sorted_keys(mapping: dict[Any, Any], label: str) -> None:
    keys = list(mapping)
    if keys != sorted(keys):
        raise ValueError(f"{label} must be ordered by key code point")


def load_canonical(
    realpath: str,
    label: str,
    validate: Callable[[object], _T],
    canonical: Callable[[_T], bytes],
) -> tuple[_T | None, bytes | None]:
    """Read and verify one canonical lifecycle ledger.

    ``label`` is the ledger name used in error wording (e.g.
    ``"dispatch ledger"``). ``validate`` turns the decoded JSON into the
    caller's normalized section tuple/object, raising ``ValueError`` on
    any structural problem, and ``canonical`` renders it back to the
    exact on-disk form.

    A missing file returns ``(None, None)``: callers keep their own
    missing-file policy (empty initial state, ``FileNotFoundError`` or
    ``KeyError``). Every other problem raises ``ValueError`` with the
    same wording the ledgers carried before the infrastructure was
    shared, and the re-rendered bytes must match the file byte for byte.
    """
    try:
        with open(realpath, "rb") as handle:
            raw = handle.read()
    except FileNotFoundError:
        return None, None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"{label} {realpath!r} is not valid UTF-8") from exc
    try:
        # Negative-zero and non-finite literals are format errors.
        data = finite_loads(text)
    except ValueError as exc:
        raise ValueError(
            f"{label} {realpath!r} is not valid JSON") from exc
    sections = validate(data)
    # The ledger is accepted only in canonical compact form with a
    # single trailing newline.
    if raw != canonical(sections):
        raise ValueError(
            f"{label} {realpath!r} is not in canonical compact form")
    return sections, raw


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
    prefix: str = ".restore-",
    fsync_dir: Callable[[str], None] | None = None,
) -> None:
    # Restore the exact pre-call bytes while the exclusive lock is held,
    # or remove a ledger that did not exist beforehand, then sync the
    # directory. A failed recovery chains after the original error. The
    # directory-sync call is injectable so each ledger keeps its own
    # module-level fault-injection seam.
    if fsync_dir is None:
        fsync_dir = fsync_directory
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
    prefix: str = ".",
    fsync_dir: Callable[[str], None] | None = None,
) -> None:
    """Publish ``payload`` over ``realpath`` as one durable commit.

    The payload is fully written to a same-directory temporary file and
    fsynced, then atomically replaces the ledger and the directory is
    fsynced. A failure before the replace leaves the pre-call bytes (or
    absence) untouched; a failure while syncing the directory after the
    replace restores the original bytes -- or removes a ledger that did
    not exist beforehand -- and chains any recovery failure after the
    original error. Concurrent readers therefore observe only the
    complete old or the complete new version.

    ``fsync_dir`` defaults to :func:`fsync_directory`; a caller passes
    its own module-level function to keep its fault-injection seam.
    """
    # One durable commit for the record, the idempotency binding and the
    # audit event: synced same-directory temporary, atomic replace and a
    # directory fsync, restoring the pre-call bytes on any failure.
    if fsync_dir is None:
        fsync_dir = fsync_directory
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
        rollback_file(realpath, directory, old_bytes, first,
                      prefix=prefix + "restore-", fsync_dir=fsync_dir)
        raise
