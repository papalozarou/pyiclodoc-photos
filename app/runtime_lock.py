# ------------------------------------------------------------------------------
# This module enforces single-writer runtime locking for one worker instance.
#
# The lock is intentionally process-scoped and file-backed so independent
# containers or repeated local launches sharing the same config volume cannot
# mutate the same state concurrently.
# ------------------------------------------------------------------------------

from __future__ import annotations

from pathlib import Path
from typing import TextIO
import fcntl
import os


# ------------------------------------------------------------------------------
# This exception signals that another worker already holds the runtime lock.
# ------------------------------------------------------------------------------
class RuntimeLockError(RuntimeError):
    pass


# ------------------------------------------------------------------------------
# This function returns the path used for the worker runtime lock file.
#
# 1. "CONFIG_DIR" is the root config directory for runtime state.
#
# Returns: Lock file path inside the shared config directory.
# ------------------------------------------------------------------------------
def get_runtime_lock_path(CONFIG_DIR: Path) -> Path:
    return CONFIG_DIR / "pyiclodoc-photos.lock"


# ------------------------------------------------------------------------------
# This function acquires the process-wide runtime lock for one worker instance.
#
# 1. "CONFIG_DIR" is the root config directory for runtime state.
# 2. "CONTAINER_USERNAME" identifies the worker in the lock file contents.
#
# Returns: Open lock-file handle that must stay alive while the lock is held.
#
# Failure behaviour:
# 1. Raises "RuntimeLockError" when another process already holds the lock.
# 2. Leaves the existing lock owner in place without modifying its state.
# ------------------------------------------------------------------------------
def acquire_runtime_lock(CONFIG_DIR: Path, CONTAINER_USERNAME: str) -> TextIO:
    LOCK_PATH = get_runtime_lock_path(CONFIG_DIR)
    HANDLE = LOCK_PATH.open("a+", encoding="utf-8")

    try:
        fcntl.flock(HANDLE.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as ERROR:
        HANDLE.close()
        raise RuntimeLockError(
            "Another worker is already using this config directory: "
            f"{LOCK_PATH} ({ERROR})"
        ) from ERROR

    HANDLE.seek(0)
    HANDLE.truncate()
    HANDLE.write(f"pid={os.getpid()}\n")
    HANDLE.write(f"container_username={CONTAINER_USERNAME}\n")
    HANDLE.flush()
    return HANDLE


# ------------------------------------------------------------------------------
# This function releases the runtime lock when the worker is shutting down.
#
# 1. "HANDLE" is the open lock-file handle returned by acquire_runtime_lock.
#
# Returns: None.
# ------------------------------------------------------------------------------
def release_runtime_lock(HANDLE: TextIO | None) -> None:
    if HANDLE is None:
        return

    try:
        fcntl.flock(HANDLE.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass

    HANDLE.close()
