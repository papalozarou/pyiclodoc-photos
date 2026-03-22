# ------------------------------------------------------------------------------
# This module performs incremental iCloud Photos synchronisation with manifest,
# album-link, and safety-net logic.
# 
# The sync model keeps one canonical file path per asset and treats album views
# as derived outputs. That separation keeps deletion and reimport decisions
# easier to reason about.
# ------------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Any
import os

from app.album_reconcile import reconcile_album_views
from app.delete_phase import delete_removed_local_paths
from app.icloud_client import ICloudDriveClient, RemoteEntry
from app.logger import log_line
from app.sync_plan import build_sync_plan, get_valid_canonical_paths
from app.transfer_runner import get_transfer_worker_count, run_transfers


# ------------------------------------------------------------------------------
# This data class records safety-net findings used to block unsafe sync runs.
# ------------------------------------------------------------------------------
@dataclass(frozen=True)
class SafetyNetResult:
    should_block: bool
    expected_uid: int
    expected_gid: int
    mismatched_samples: list[str]


# ------------------------------------------------------------------------------
# This data class captures per-run transfer summary metrics.
# ------------------------------------------------------------------------------
@dataclass(frozen=True)
class SyncResult:
    total_files: int
    transferred_files: int
    transferred_bytes: int
    skipped_files: int
    error_files: int
    transfer_error_files: int = 0
    derived_error_files: int = 0
    deleted_files: int = 0
    deleted_directories: int = 0
    delete_error_files: int = 0


# ------------------------------------------------------------------------------
# This function runs a first-time permission safety check.
#
# 1. "OUTPUT_DIR" is the backup destination root.
# 2. "SAMPLE_SIZE" is the max number of files to inspect.
#
# Returns: "SafetyNetResult" describing whether sync should be blocked and why.
# 
# N.B.
# This check is intentionally bounded. It is designed to catch the common
# "wrong UID/GID against existing files" case before a damaging run.
# ------------------------------------------------------------------------------
def run_first_time_safety_net(OUTPUT_DIR: Path, SAMPLE_SIZE: int) -> SafetyNetResult:
    LOCAL_FILES = collect_local_files(OUTPUT_DIR, SAMPLE_SIZE)
    EXPECTED_UID = os.getuid()
    EXPECTED_GID = os.getgid()

    if not LOCAL_FILES:
        return SafetyNetResult(False, EXPECTED_UID, EXPECTED_GID, [])

    MISMATCHES = collect_mismatches(LOCAL_FILES, EXPECTED_UID, EXPECTED_GID)
    return SafetyNetResult(len(MISMATCHES) > 0, EXPECTED_UID, EXPECTED_GID, MISMATCHES)


# ------------------------------------------------------------------------------
# This function returns one deterministic sample key for a local file path.
#
# 1. "OUTPUT_DIR" is the backup destination root.
# 2. "FILE_PATH" is one local file path under inspection.
#
# Returns: Stable hex sort key used to keep a distributed bounded sample.
# ------------------------------------------------------------------------------
def get_sample_key(OUTPUT_DIR: Path, FILE_PATH: Path) -> str:
    try:
        RELATIVE_PATH = FILE_PATH.relative_to(OUTPUT_DIR).as_posix()
    except ValueError:
        RELATIVE_PATH = str(FILE_PATH)

    return hashlib.sha1(RELATIVE_PATH.encode("utf-8")).hexdigest()


# ------------------------------------------------------------------------------
# This function collects a bounded local-file sample for permission checks.
#
# 1. "OUTPUT_DIR" is the backup destination root.
# 2. "SAMPLE_SIZE" is the sample cap.
#
# Returns: Ordered file list up to "SAMPLE_SIZE" for ownership analysis.
# 
# N.B.
# The sample is bounded, deterministic, and spread across the whole tree using
# stable path hashing. This avoids large-library bias towards whichever files
# the filesystem walk yields first.
# ------------------------------------------------------------------------------
def collect_local_files(OUTPUT_DIR: Path, SAMPLE_SIZE: int) -> list[Path]:
    if SAMPLE_SIZE < 1:
        return []

    SELECTED: list[tuple[str, Path]] = []

    for PATH in OUTPUT_DIR.rglob("*"):
        if not PATH.is_file():
            continue

        SAMPLE_KEY = get_sample_key(OUTPUT_DIR, PATH)

        if len(SELECTED) < SAMPLE_SIZE:
            SELECTED.append((SAMPLE_KEY, PATH))
            SELECTED.sort(key=lambda ITEM: ITEM[0], reverse=True)
            continue

        if SAMPLE_KEY >= SELECTED[0][0]:
            continue

        SELECTED[0] = (SAMPLE_KEY, PATH)
        SELECTED.sort(key=lambda ITEM: ITEM[0], reverse=True)

    SELECTED.sort(key=lambda ITEM: ITEM[0])
    return [PATH for _, PATH in SELECTED]


# ------------------------------------------------------------------------------
# This function returns sampled files with non-matching ownership.
#
# 1. "FILES" is the sampled file list.
# 2. "EXPECTED_UID" is the runtime user ID expected to own files.
# 3. "EXPECTED_GID" is the runtime group ID expected to own files.
# 4. "LIMIT" caps mismatch output.
#
# Returns: Human-readable mismatch list for logs and Telegram alerts.
# 
# Failure behaviour:
# 1. Stops after "LIMIT" mismatches to keep alerts compact.
# 2. Leaves deeper filesystem inspection to the operator after the block.
# ------------------------------------------------------------------------------
def collect_mismatches(
    FILES: list[Path],
    EXPECTED_UID: int,
    EXPECTED_GID: int,
    LIMIT: int = 20,
) -> list[str]:
    MISMATCHES: list[str] = []

    for PATH in FILES:
        try:
            FILE_STAT = PATH.stat()
        except OSError:
            continue

        if FILE_STAT.st_uid == EXPECTED_UID and FILE_STAT.st_gid == EXPECTED_GID:
            continue

        MISMATCHES.append(
            f"{PATH}: uid={FILE_STAT.st_uid}, gid={FILE_STAT.st_gid} "
            f"(expected uid={EXPECTED_UID}, gid={EXPECTED_GID})",
        )

        if len(MISMATCHES) >= LIMIT:
            return MISMATCHES

    return MISMATCHES


# ------------------------------------------------------------------------------
# This function syncs photo contents incrementally and updates manifest data.
#
# 1. "CLIENT" is the active iCloud API wrapper.
# 2. "OUTPUT_DIR" is local backup root.
# 3. "MANIFEST" is previous metadata.
# 4. "SYNC_DOWNLOAD_WORKERS" selects transfer concurrency.
# 5. "LOG_FILE" is optional worker log destination.
# 6. "BACKUP_DELETE_REMOVED" enables delete reconciliation.
# 7. "BACKUP_ALBUMS_ENABLED" enables derived album-output management.
# 8. "BACKUP_ALBUM_LINKS_MODE" selects hard-link or copy-only album output.
# 9. "BACKUP_ROOT_ALBUMS" is the relative albums root path.
#
# Returns: Tuple of sync summary metrics and a refreshed manifest mapping.
# 
# Behaviour notes:
# 1. Only canonical library files participate in transfer decisions.
# 2. Album views are only reconciled when album management is enabled.
# 3. Optional delete handling removes canonical paths and only managed album
#    paths.
# ------------------------------------------------------------------------------
def perform_incremental_sync(
    CLIENT: ICloudDriveClient,
    OUTPUT_DIR: Path,
    MANIFEST: dict[str, dict[str, Any]],
    SYNC_DOWNLOAD_WORKERS: int = 0,
    LOG_FILE: Path | None = None,
    BACKUP_DELETE_REMOVED: bool = False,
    BACKUP_ALBUMS_ENABLED: bool = True,
    BACKUP_ALBUM_LINKS_MODE: str = "hardlink",
    BACKUP_ROOT_ALBUMS: str = "albums",
) -> tuple[SyncResult, dict[str, dict[str, Any]]]:
    if LOG_FILE is not None:
        log_line(LOG_FILE, "info", "Remote photo listing started.")

    ENTRIES = CLIENT.list_entries_for_sync(MANIFEST)
    FILES = [ENTRY for ENTRY in ENTRIES if not ENTRY.is_dir]

    if LOG_FILE is not None:
        log_line(LOG_FILE, "info", f"Remote photo listing finished. files={len(FILES)}.")
        log_line(
            LOG_FILE,
            "debug",
            "Remote listing detail: "
            f"entries={len(ENTRIES)}, files={len(FILES)}",
        )

    NEW_MANIFEST, TRANSFER_CANDIDATES, SKIPPED = build_sync_plan(
        FILES,
        MANIFEST,
        LOG_FILE,
    )
    TRANSFERRED = 0
    TRANSFERRED_BYTES = 0
    TRANSFER_ERRORS = 0
    FAILURE_REASON_COUNTS: dict[str, int] = {}

    if LOG_FILE is not None:
        log_line(
            LOG_FILE,
            "debug",
            "Transfer planning detail: "
            f"candidates={len(TRANSFER_CANDIDATES)}, skipped_unchanged={SKIPPED}",
        )

    if TRANSFER_CANDIDATES:
        if LOG_FILE is not None:
            log_line(
                LOG_FILE,
                "info",
                f"Transfer started. candidates={len(TRANSFER_CANDIDATES)}.",
            )

        WORKER_COUNT = get_transfer_worker_count(SYNC_DOWNLOAD_WORKERS)
        if LOG_FILE is not None:
            log_line(
                LOG_FILE,
                "debug",
                "Transfer execution detail: "
                f"workers={WORKER_COUNT}, sync_workers={SYNC_DOWNLOAD_WORKERS}",
            )

        TRANSFERRED, TRANSFERRED_BYTES, TRANSFER_ERRORS, FAILURE_REASON_COUNTS = run_transfers(
            CLIENT,
            OUTPUT_DIR,
            TRANSFER_CANDIDATES,
            NEW_MANIFEST,
            LOG_FILE,
            WORKER_COUNT,
        )
    elif LOG_FILE is not None:
        log_line(LOG_FILE, "info", "Transfer skipped. candidates=0.")

    if LOG_FILE is not None and BACKUP_ALBUMS_ENABLED:
        log_line(LOG_FILE, "info", "Album reconciliation started.")

    ALBUM_RESULT = None
    DERIVED_ERRORS = 0
    DELETED_FILES = 0
    DELETED_DIRECTORIES = 0
    DELETE_ERRORS = 0

    if BACKUP_ALBUMS_ENABLED:
        VALID_CANONICAL_PATHS = get_valid_canonical_paths(NEW_MANIFEST)
        ALBUM_RESULT = reconcile_album_views(
            OUTPUT_DIR,
            FILES,
            NEW_MANIFEST,
            VALID_CANONICAL_PATHS,
            BACKUP_ALBUM_LINKS_MODE,
            LOG_FILE,
        )
        DERIVED_ERRORS = ALBUM_RESULT.errors

    if LOG_FILE is not None and BACKUP_ALBUMS_ENABLED:
        log_line(
            LOG_FILE,
            "info",
            "Album reconciliation finished. "
            f"created={ALBUM_RESULT.created}, "
            f"reused={ALBUM_RESULT.reused}, "
            f"skipped_missing_source={ALBUM_RESULT.skipped_missing_source}, "
            f"errors={ALBUM_RESULT.errors}.",
        )

    if BACKUP_DELETE_REMOVED:
        if LOG_FILE is not None:
            log_line(LOG_FILE, "info", "Delete phase started.")

        DELETED_FILES, DELETED_DIRECTORIES, DELETE_ERRORS = delete_removed_local_paths(
            OUTPUT_DIR,
            FILES,
            NEW_MANIFEST,
            BACKUP_ALBUMS_ENABLED,
            BACKUP_ROOT_ALBUMS,
            LOG_FILE,
        )

        if LOG_FILE is not None:
            log_line(
                LOG_FILE,
                "info",
                "Delete phase finished. "
                f"deleted_files={DELETED_FILES}, "
                f"deleted_directories={DELETED_DIRECTORIES}, "
                f"errors={DELETE_ERRORS}.",
            )

    if LOG_FILE is not None:
        log_line(
            LOG_FILE,
            "info",
            "Transfer finished. "
            f"transferred={TRANSFERRED}, skipped={SKIPPED}, errors={TRANSFER_ERRORS}.",
        )
        if FAILURE_REASON_COUNTS:
            DETAIL_TEXT = ", ".join(
                f"{REASON}={COUNT}"
                for REASON, COUNT in sorted(FAILURE_REASON_COUNTS.items())
            )
            log_line(
                LOG_FILE,
                "debug",
                f"Transfer failure reason detail: {DETAIL_TEXT}",
            )

    TOTAL_ERRORS = TRANSFER_ERRORS + DERIVED_ERRORS + DELETE_ERRORS

    return SyncResult(
        total_files=len(FILES),
        transferred_files=TRANSFERRED,
        transferred_bytes=TRANSFERRED_BYTES,
        skipped_files=SKIPPED,
        error_files=TOTAL_ERRORS,
        transfer_error_files=TRANSFER_ERRORS,
        derived_error_files=DERIVED_ERRORS,
        deleted_files=DELETED_FILES,
        deleted_directories=DELETED_DIRECTORIES,
        delete_error_files=DELETE_ERRORS,
    ), NEW_MANIFEST
