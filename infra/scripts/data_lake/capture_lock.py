"""Cross-platform non-blocking file locks for bounded capture runners."""

from __future__ import annotations

import errno
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator


class LockBusyError(RuntimeError):
    """The lock is owned by another capture cycle."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


_BUSY_ERRNOS = {
    errno.EACCES,
    errno.EAGAIN,
    getattr(errno, "EDEADLK", errno.EAGAIN),
}


def _lock_nonblocking(handle) -> None:
    if os.name == "nt":
        import msvcrt

        # msvcrt.locking locks a byte range from the current file position.
        # Keep byte zero present so an empty newly-created lock file is valid.
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined,unused-ignore]
        return

    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock(handle) -> None:
    if os.name == "nt":
        import msvcrt

        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined,unused-ignore]
        return

    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def acquire_lock(
    path: Path,
    *,
    timeout_seconds: float = 0.0,
    busy_error: Callable[[str], Exception] = LockBusyError,
) -> Iterator[None]:
    """Acquire an advisory lock with the same bounded behavior on each OS."""
    if timeout_seconds < 0:
        raise ValueError("lock timeout must be non-negative")
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    deadline = time.monotonic() + timeout_seconds
    acquired = False
    try:
        while True:
            try:
                _lock_nonblocking(handle)
                acquired = True
                break
            except OSError as exc:
                if exc.errno not in _BUSY_ERRNOS:
                    raise
                if time.monotonic() >= deadline:
                    raise busy_error("lock_busy") from None
                time.sleep(min(0.25, max(0.01, deadline - time.monotonic())))
        yield
    finally:
        try:
            if acquired:
                _unlock(handle)
        finally:
            handle.close()
