# ------------------------------------------------------------------------------
# This module manages derived album-output reconciliation for the photo sync
# workflow.
# ------------------------------------------------------------------------------

from __future__ import annotations

from pathlib import Path
import os
import shutil

from app.icloud_client import RemoteEntry
from app.logger import log_line


# ------------------------------------------------------------------------------
# This function creates or refreshes album views for transferred assets.
#
# 1. "OUTPUT_DIR" is local backup root.
# 2. "ENTRIES" is the current remote photo list.
# 3. "NEW_MANIFEST" is the refreshed manifest under construction.
# 4. "VALID_CANONICAL_PATHS" is the set of canonical paths verified for use.
# 5. "BACKUP_ALBUM_LINKS_MODE" selects hard-link or copy-only output.
# 6. "LOG_FILE" is optional log file path.
#
# Returns: Tuple "(created, reused, skipped_missing_source)".
#
# N.B.
# Album view creation is intentionally best-effort. Missing canonical files are
# skipped quietly because a failed canonical transfer has already been counted.
# ------------------------------------------------------------------------------
def reconcile_album_views(
    OUTPUT_DIR: Path,
    ENTRIES: list[RemoteEntry],
    NEW_MANIFEST: dict[str, dict[str, object]],
    VALID_CANONICAL_PATHS: set[str],
    BACKUP_ALBUM_LINKS_MODE: str,
    LOG_FILE: Path | None,
) -> tuple[int, int, int]:
    CREATED = 0
    REUSED = 0
    SKIPPED_MISSING_SOURCE = 0

    for ENTRY in ENTRIES:
        if not ENTRY.album_paths:
            continue

        if ENTRY.path not in VALID_CANONICAL_PATHS:
            SKIPPED_MISSING_SOURCE += len(ENTRY.album_paths)
            if LOG_FILE is not None:
                log_line(
                    LOG_FILE,
                    "debug",
                    "Album view skipped unverified canonical source: "
                    f"{ENTRY.path}",
                )
            continue

        SOURCE_PATH = OUTPUT_DIR / ENTRY.path

        if not SOURCE_PATH.exists():
            SKIPPED_MISSING_SOURCE += len(ENTRY.album_paths)
            if LOG_FILE is not None:
                log_line(
                    LOG_FILE,
                    "debug",
                    "Album view skipped missing canonical source: "
                    f"{ENTRY.path}",
                )
            continue

        for ALBUM_DIR in ENTRY.album_paths:
            TARGET_PATH = OUTPUT_DIR / ALBUM_DIR / ENTRY.download_name
            WAS_CREATED = create_album_link(
                SOURCE_PATH,
                TARGET_PATH,
                BACKUP_ALBUM_LINKS_MODE,
            )
            NEW_MANIFEST[str(TARGET_PATH.relative_to(OUTPUT_DIR))] = {
                "entry_kind": "album_link",
                "is_dir": False,
                "source_path": ENTRY.path,
            }

            if WAS_CREATED:
                CREATED += 1
            else:
                REUSED += 1

            if LOG_FILE is None:
                continue

            log_line(
                LOG_FILE,
                "debug",
                "Album view refreshed: "
                f"{TARGET_PATH.relative_to(OUTPUT_DIR)} -> {ENTRY.path} "
                f"(created={str(WAS_CREATED).lower()})",
            )

    return CREATED, REUSED, SKIPPED_MISSING_SOURCE


# ------------------------------------------------------------------------------
# This function creates an album-view file as a hard link with copy fallback.
#
# 1. "SOURCE_PATH" is the canonical library file.
# 2. "TARGET_PATH" is the album-view file path.
# 3. "BACKUP_ALBUM_LINKS_MODE" selects hard-link or copy-only output.
#
# Returns: True when a new hard link or copy was created, otherwise False.
#
# N.B.
# Hard links are preferred because they avoid duplicate data. "copy" mode is
# strict and never attempts a hard link.
# ------------------------------------------------------------------------------
def create_album_link(
    SOURCE_PATH: Path,
    TARGET_PATH: Path,
    BACKUP_ALBUM_LINKS_MODE: str,
) -> bool:
    TARGET_PATH.parent.mkdir(parents=True, exist_ok=True)

    if TARGET_PATH.exists():
        if same_file_contents(TARGET_PATH, SOURCE_PATH):
            return False

        TARGET_PATH.unlink()

    if BACKUP_ALBUM_LINKS_MODE == "copy":
        shutil.copy2(SOURCE_PATH, TARGET_PATH)
        return True

    try:
        os.link(SOURCE_PATH, TARGET_PATH)
        return True
    except OSError:
        shutil.copy2(SOURCE_PATH, TARGET_PATH)
        return True


# ------------------------------------------------------------------------------
# This function compares two files by inode or byte identity.
#
# 1. "LEFT_PATH" is the first path.
# 2. "RIGHT_PATH" is the second path.
#
# Returns: True when both paths already represent the same file data.
#
# N.B.
# The byte-compare fallback is more expensive than inode checks, but it only
# runs when the platform cannot confirm file identity cheaply.
# ------------------------------------------------------------------------------
def same_file_contents(LEFT_PATH: Path, RIGHT_PATH: Path) -> bool:
    try:
        if os.path.samefile(LEFT_PATH, RIGHT_PATH):
            return True
    except OSError:
        pass

    try:
        return LEFT_PATH.read_bytes() == RIGHT_PATH.read_bytes()
    except OSError:
        return False
