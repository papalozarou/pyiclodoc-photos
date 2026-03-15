# ------------------------------------------------------------------------------
# This module manages delete-phase reconciliation for removed local sync paths.
# ------------------------------------------------------------------------------

from __future__ import annotations

from pathlib import Path

from app.icloud_client import RemoteEntry
from app.logger import log_line


# ------------------------------------------------------------------------------
# This function deletes local files that are no longer present remotely.
#
# 1. "OUTPUT_DIR" is local backup root.
# 2. "ENTRIES" is the current remote photo list.
# 3. "NEW_MANIFEST" is the refreshed manifest under construction.
# 4. "BACKUP_ALBUMS_ENABLED" decides whether the albums tree is managed.
# 5. "BACKUP_ROOT_ALBUMS" is the relative albums root path.
# 6. "LOG_FILE" is optional log file path.
#
# Returns: Tuple "(deleted_files, deleted_directories, errors)".
#
# N.B.
# Delete handling is filesystem-driven. The manifest is updated
# opportunistically so a future run can still self-heal if one unlink fails.
# ------------------------------------------------------------------------------
def delete_removed_local_paths(
    OUTPUT_DIR: Path,
    ENTRIES: list[RemoteEntry],
    NEW_MANIFEST: dict[str, dict[str, object]],
    BACKUP_ALBUMS_ENABLED: bool,
    BACKUP_ROOT_ALBUMS: str,
    LOG_FILE: Path | None,
) -> tuple[int, int, int]:
    DESIRED_PATHS = desired_relative_paths(ENTRIES)
    PROTECTED_PATHS = get_protected_local_paths(OUTPUT_DIR, LOG_FILE)
    DELETED_FILES = 0
    DELETED_DIRECTORIES = 0
    ERRORS = 0

    for PATH in OUTPUT_DIR.rglob("*"):
        if not PATH.is_file():
            continue

        RELATIVE_PATH = str(PATH.relative_to(OUTPUT_DIR))

        if not BACKUP_ALBUMS_ENABLED and is_path_within_root(RELATIVE_PATH, BACKUP_ROOT_ALBUMS):
            continue

        if RELATIVE_PATH in DESIRED_PATHS or RELATIVE_PATH in PROTECTED_PATHS:
            continue

        try:
            PATH.unlink()
        except OSError:
            ERRORS += 1
            if LOG_FILE is not None:
                log_line(LOG_FILE, "debug", f"Local file delete error: {RELATIVE_PATH}")
            continue

        NEW_MANIFEST.pop(RELATIVE_PATH, None)
        DELETED_FILES += 1

        if LOG_FILE is None:
            continue

        log_line(LOG_FILE, "debug", f"Removed local file: {RELATIVE_PATH}")

    DELETED_DIRECTORIES += prune_empty_directories(OUTPUT_DIR, LOG_FILE)
    return DELETED_FILES, DELETED_DIRECTORIES, ERRORS


# ------------------------------------------------------------------------------
# This function checks whether a relative path sits under a managed root path.
#
# 1. "RELATIVE_PATH" is the output-relative file path.
# 2. "ROOT_PATH" is the managed relative root path.
#
# Returns: True when "RELATIVE_PATH" is inside or equal to "ROOT_PATH".
# ------------------------------------------------------------------------------
def is_path_within_root(RELATIVE_PATH: str, ROOT_PATH: str) -> bool:
    CLEAN_ROOT = ROOT_PATH.strip("/").replace("\\", "/")
    CLEAN_RELATIVE_PATH = RELATIVE_PATH.strip("/").replace("\\", "/")

    if not CLEAN_ROOT:
        return False

    return CLEAN_RELATIVE_PATH == CLEAN_ROOT or CLEAN_RELATIVE_PATH.startswith(f"{CLEAN_ROOT}/")


# ------------------------------------------------------------------------------
# This function returns local paths that the delete phase must never remove.
#
# 1. "OUTPUT_DIR" is local backup root.
# 2. "LOG_FILE" is optional worker log path.
#
# Returns: Relative-path set protected from delete handling.
# ------------------------------------------------------------------------------
def get_protected_local_paths(OUTPUT_DIR: Path, LOG_FILE: Path | None) -> set[str]:
    if LOG_FILE is None:
        return set()

    try:
        return {str(LOG_FILE.relative_to(OUTPUT_DIR))}
    except ValueError:
        return set()


# ------------------------------------------------------------------------------
# This function returns all desired relative output paths for the current run.
#
# 1. "ENTRIES" is the current remote photo list.
#
# Returns: Set of canonical and album-view paths.
#
# N.B.
# Derived album-view paths are included here so delete handling remains
# independent from the layout chosen during the previous run.
# ------------------------------------------------------------------------------
def desired_relative_paths(ENTRIES: list[RemoteEntry]) -> set[str]:
    RESULT: set[str] = set()

    for ENTRY in ENTRIES:
        RESULT.add(ENTRY.path)

        for ALBUM_DIR in ENTRY.album_paths:
            RESULT.add(f"{ALBUM_DIR}/{ENTRY.download_name}")

    return RESULT


# ------------------------------------------------------------------------------
# This function removes empty directories left after delete operations.
#
# 1. "OUTPUT_DIR" is local backup root.
#
# Returns: Count of removed directories.
#
# N.B.
# Directories are pruned deepest-first so parents are only considered after any
# removable children have already been handled.
# ------------------------------------------------------------------------------
def prune_empty_directories(OUTPUT_DIR: Path, LOG_FILE: Path | None = None) -> int:
    DIRECTORIES = [PATH for PATH in OUTPUT_DIR.rglob("*") if PATH.is_dir()]
    DIRECTORIES.sort(key=lambda PATH: len(PATH.parts), reverse=True)
    DELETED_DIRECTORIES = 0

    for DIR_PATH in DIRECTORIES:
        try:
            next(DIR_PATH.iterdir())
        except StopIteration:
            try:
                DIR_PATH.rmdir()
                DELETED_DIRECTORIES += 1
                if LOG_FILE is not None:
                    log_line(
                        LOG_FILE,
                        "debug",
                        f"Removed empty directory: {DIR_PATH.relative_to(OUTPUT_DIR)}",
                    )
            except OSError:
                continue
        except OSError:
            continue

    return DELETED_DIRECTORIES
