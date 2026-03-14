# ------------------------------------------------------------------------------
# This module performs incremental iCloud Photos synchronisation with manifest,
# album-link, and safety-net logic.
# ------------------------------------------------------------------------------

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import os
import shutil
import time

from app.icloud_client import ICloudDriveClient, RemoteEntry
from app.logger import log_line

TRANSFER_PROGRESS_LOG_INTERVAL_SECONDS = 30.0
PROGRESS_LOG_SEPARATOR = "------------------------------------------------------------"


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


# ------------------------------------------------------------------------------
# This function derives automatic transfer worker count from host CPU capacity.
#
# Returns: Bounded worker count for concurrent file download tasks.
# ------------------------------------------------------------------------------
def get_auto_worker_count() -> int:
    CPU_COUNT = os.cpu_count() or 1
    return min(max(CPU_COUNT, 1), 8)


# ------------------------------------------------------------------------------
# This function resolves effective transfer worker count.
#
# 1. "SYNC_DOWNLOAD_WORKERS" uses 0 for auto mode and positive values for
#    explicit overrides.
#
# Returns: Bounded worker count for concurrent file download tasks.
# ------------------------------------------------------------------------------
def get_transfer_worker_count(SYNC_DOWNLOAD_WORKERS: int) -> int:
    if SYNC_DOWNLOAD_WORKERS > 0:
        return min(max(SYNC_DOWNLOAD_WORKERS, 1), 16)

    return get_auto_worker_count()


# ------------------------------------------------------------------------------
# This function runs a first-time permission safety check.
#
# 1. "OUTPUT_DIR" is the backup destination root.
# 2. "SAMPLE_SIZE" is the max number of files to inspect.
#
# Returns: "SafetyNetResult" describing whether sync should be blocked and why.
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
# This function collects a bounded local-file sample for permission checks.
#
# 1. "OUTPUT_DIR" is the backup destination root.
# 2. "SAMPLE_SIZE" is the sample cap.
#
# Returns: Ordered file list up to "SAMPLE_SIZE" for ownership analysis.
# ------------------------------------------------------------------------------
def collect_local_files(OUTPUT_DIR: Path, SAMPLE_SIZE: int) -> list[Path]:
    RESULT: list[Path] = []

    for PATH in OUTPUT_DIR.rglob("*"):
        if not PATH.is_file():
            continue

        RESULT.append(PATH)

        if len(RESULT) >= SAMPLE_SIZE:
            return RESULT

    return RESULT


# ------------------------------------------------------------------------------
# This function returns sampled files with non-matching ownership.
#
# 1. "FILES" is the sampled file list.
# 2. "EXPECTED_UID" is the runtime user ID expected to own files.
# 3. "EXPECTED_GID" is the runtime group ID expected to own files.
# 4. "LIMIT" caps mismatch output.
#
# Returns: Human-readable mismatch list for logs and Telegram alerts.
# ------------------------------------------------------------------------------
def collect_mismatches(
    FILES: list[Path],
    EXPECTED_UID: int,
    EXPECTED_GID: int,
    LIMIT: int = 20,
) -> list[str]:
    MISMATCHES: list[str] = []

    for PATH in FILES:
        FILE_STAT = PATH.stat()

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
# This function returns a deterministic metadata dictionary for a remote entry.
#
# 1. "ENTRY" is a remote photo metadata record.
#
# Returns: Dictionary payload persisted in the incremental manifest.
# ------------------------------------------------------------------------------
def entry_metadata(ENTRY: RemoteEntry) -> dict[str, Any]:
    return {
        "asset_id": ENTRY.asset_id,
        "album_paths": list(ENTRY.album_paths),
        "created": ENTRY.created,
        "download_name": ENTRY.download_name,
        "is_dir": False,
        "modified": ENTRY.modified,
        "size": ENTRY.size,
    }


# ------------------------------------------------------------------------------
# This function decides whether a file should be transferred.
#
# 1. "ENTRY" is current remote metadata.
# 2. "MANIFEST" is previous run metadata.
#
# Returns: True when transfer is required, otherwise False.
# ------------------------------------------------------------------------------
def needs_transfer(ENTRY: RemoteEntry, MANIFEST: dict[str, dict[str, Any]]) -> bool:
    EXISTING = MANIFEST.get(ENTRY.path)

    if EXISTING is None:
        return True

    if str(EXISTING.get("asset_id", "")) != ENTRY.asset_id:
        return True

    if int(EXISTING.get("size", -1)) != ENTRY.size:
        return True

    if str(EXISTING.get("modified", "")) != ENTRY.modified:
        return True

    return False


# ------------------------------------------------------------------------------
# This function syncs photo contents incrementally and updates manifest data.
#
# 1. "CLIENT" is the active iCloud API wrapper.
# 2. "OUTPUT_DIR" is local backup root.
# 3. "MANIFEST" is previous metadata.
#
# Returns: Tuple of sync summary metrics and a refreshed manifest mapping.
# ------------------------------------------------------------------------------
def perform_incremental_sync(
    CLIENT: ICloudDriveClient,
    OUTPUT_DIR: Path,
    MANIFEST: dict[str, dict[str, Any]],
    SYNC_DOWNLOAD_WORKERS: int = 0,
    LOG_FILE: Path | None = None,
    BACKUP_DELETE_REMOVED: bool = False,
) -> tuple[SyncResult, dict[str, dict[str, Any]]]:
    if LOG_FILE is not None:
        log_line(LOG_FILE, "info", "Remote photo listing started.")

    ENTRIES = CLIENT.list_entries()

    if LOG_FILE is not None:
        log_line(LOG_FILE, "info", f"Remote photo listing finished. files={len(ENTRIES)}.")

    NEW_MANIFEST: dict[str, dict[str, Any]] = {}
    TRANSFER_CANDIDATES: list[RemoteEntry] = []
    TRANSFERRED = 0
    TRANSFERRED_BYTES = 0
    SKIPPED = 0
    ERRORS = 0

    for ENTRY in ENTRIES:
        if needs_transfer(ENTRY, MANIFEST):
            TRANSFER_CANDIDATES.append(ENTRY)
            continue

        NEW_MANIFEST[ENTRY.path] = entry_metadata(ENTRY)
        SKIPPED += 1

    if LOG_FILE is not None:
        log_line(
            LOG_FILE,
            "debug",
            "Transfer planning detail: "
            f"candidates={len(TRANSFER_CANDIDATES)}, skipped_unchanged={SKIPPED}",
        )

    if TRANSFER_CANDIDATES:
        WORKER_COUNT = get_transfer_worker_count(SYNC_DOWNLOAD_WORKERS)
        TRANSFERRED, TRANSFERRED_BYTES, ERRORS = run_transfers(
            CLIENT,
            OUTPUT_DIR,
            TRANSFER_CANDIDATES,
            NEW_MANIFEST,
            LOG_FILE,
            WORKER_COUNT,
        )

    reconcile_album_views(OUTPUT_DIR, ENTRIES, NEW_MANIFEST, LOG_FILE)

    if BACKUP_DELETE_REMOVED:
        delete_removed_local_paths(OUTPUT_DIR, ENTRIES, NEW_MANIFEST, LOG_FILE)

    return SyncResult(
        total_files=len(ENTRIES),
        transferred_files=TRANSFERRED,
        transferred_bytes=TRANSFERRED_BYTES,
        skipped_files=SKIPPED,
        error_files=ERRORS,
    ), NEW_MANIFEST


# ------------------------------------------------------------------------------
# This function executes parallel canonical-file transfers.
#
# 1. "CLIENT" is the active iCloud API wrapper.
# 2. "OUTPUT_DIR" is local backup root.
# 3. "TRANSFER_CANDIDATES" is the file list to download.
# 4. "NEW_MANIFEST" is the refreshed manifest under construction.
# 5. "LOG_FILE" is optional log file path.
# 6. "WORKER_COUNT" is the bounded transfer pool size.
#
# Returns: Tuple "(transferred, transferred_bytes, errors)".
# ------------------------------------------------------------------------------
def run_transfers(
    CLIENT: ICloudDriveClient,
    OUTPUT_DIR: Path,
    TRANSFER_CANDIDATES: list[RemoteEntry],
    NEW_MANIFEST: dict[str, dict[str, Any]],
    LOG_FILE: Path | None,
    WORKER_COUNT: int,
) -> tuple[int, int, int]:
    TRANSFERRED = 0
    TRANSFERRED_BYTES = 0
    ERRORS = 0
    STARTED_EPOCH = time.monotonic()
    LAST_PROGRESS_EPOCH = STARTED_EPOCH

    with ThreadPoolExecutor(max_workers=WORKER_COUNT) as EXECUTOR:
        FUTURES = {
            EXECUTOR.submit(transfer_if_required, CLIENT, OUTPUT_DIR, ENTRY): ENTRY
            for ENTRY in TRANSFER_CANDIDATES
        }
        PENDING = set(FUTURES.keys())
        COMPLETED = 0

        while PENDING:
            DONE, PENDING = wait(
                PENDING,
                timeout=TRANSFER_PROGRESS_LOG_INTERVAL_SECONDS,
                return_when=FIRST_COMPLETED,
            )

            for FUTURE in DONE:
                ENTRY = FUTURES[FUTURE]
                COMPLETED += 1

                try:
                    IS_SUCCESS = FUTURE.result()
                except Exception as ERROR:
                    IS_SUCCESS = False
                    if LOG_FILE is not None:
                        log_line(
                            LOG_FILE,
                            "error",
                            f"File transfer worker failed: {ENTRY.path} ({type(ERROR).__name__}: {ERROR})",
                        )

                if IS_SUCCESS:
                    LOCAL_PATH = OUTPUT_DIR / ENTRY.path
                    apply_remote_modified_time(LOCAL_PATH, ENTRY.modified)
                    NEW_MANIFEST[ENTRY.path] = entry_metadata(ENTRY)
                    TRANSFERRED += 1
                    TRANSFERRED_BYTES += max(ENTRY.size, 0)
                    continue

                ERRORS += 1

                if LOG_FILE is not None:
                    REASON = CLIENT.get_last_download_failure_reason() or "unknown_error"
                    log_line(LOG_FILE, "error", f"File transfer failed: {ENTRY.path} ({REASON})")

            NOW_EPOCH = time.monotonic()

            if LOG_FILE is None:
                continue

            if NOW_EPOCH - LAST_PROGRESS_EPOCH < TRANSFER_PROGRESS_LOG_INTERVAL_SECONDS:
                continue

            log_line(LOG_FILE, "debug", PROGRESS_LOG_SEPARATOR)
            log_line(
                LOG_FILE,
                "debug",
                "Transfer progress detail: "
                f"completed={COMPLETED}/{len(TRANSFER_CANDIDATES)}, "
                f"active={len(PENDING)}, "
                f"transferred={TRANSFERRED}, "
                f"bytes={TRANSFERRED_BYTES}, "
                f"errors={ERRORS}, "
                f"elapsed_seconds={NOW_EPOCH - STARTED_EPOCH:.1f}",
            )
            log_line(LOG_FILE, "debug", PROGRESS_LOG_SEPARATOR)
            LAST_PROGRESS_EPOCH = NOW_EPOCH

    return TRANSFERRED, TRANSFERRED_BYTES, ERRORS


# ------------------------------------------------------------------------------
# This function downloads one file when it is required.
#
# 1. "CLIENT" is the active iCloud API wrapper.
# 2. "OUTPUT_DIR" is local backup root.
# 3. "ENTRY" is the remote photo metadata record.
#
# Returns: True on successful file transfer, otherwise False.
# ------------------------------------------------------------------------------
def transfer_if_required(
    CLIENT: ICloudDriveClient,
    OUTPUT_DIR: Path,
    ENTRY: RemoteEntry,
) -> bool:
    return CLIENT.download_file(ENTRY.path, OUTPUT_DIR / ENTRY.path)


# ------------------------------------------------------------------------------
# This function applies a remote modified timestamp to a downloaded file.
#
# 1. "LOCAL_PATH" is the local file path.
# 2. "MODIFIED" is an ISO-like timestamp.
#
# Returns: None.
# ------------------------------------------------------------------------------
def apply_remote_modified_time(LOCAL_PATH: Path, MODIFIED: str) -> None:
    try:
        MODIFIED_DT = datetime.fromisoformat(MODIFIED.replace("Z", "+00:00"))
    except ValueError:
        return

    MODIFIED_EPOCH = MODIFIED_DT.astimezone(timezone.utc).timestamp()
    os.utime(LOCAL_PATH, (MODIFIED_EPOCH, MODIFIED_EPOCH))


# ------------------------------------------------------------------------------
# This function creates or refreshes album views for transferred assets.
#
# 1. "OUTPUT_DIR" is local backup root.
# 2. "ENTRIES" is the current remote photo list.
# 3. "NEW_MANIFEST" is the refreshed manifest under construction.
# 4. "LOG_FILE" is optional log file path.
#
# Returns: None.
# ------------------------------------------------------------------------------
def reconcile_album_views(
    OUTPUT_DIR: Path,
    ENTRIES: list[RemoteEntry],
    NEW_MANIFEST: dict[str, dict[str, Any]],
    LOG_FILE: Path | None,
) -> None:
    for ENTRY in ENTRIES:
        if not ENTRY.album_paths:
            continue

        SOURCE_PATH = OUTPUT_DIR / ENTRY.path

        if not SOURCE_PATH.exists():
            continue

        for ALBUM_DIR in ENTRY.album_paths:
            TARGET_PATH = OUTPUT_DIR / ALBUM_DIR / ENTRY.download_name
            create_album_link(SOURCE_PATH, TARGET_PATH)
            NEW_MANIFEST[str(TARGET_PATH.relative_to(OUTPUT_DIR))] = {
                "entry_kind": "album_link",
                "is_dir": False,
                "source_path": ENTRY.path,
            }

            if LOG_FILE is None:
                continue

            log_line(
                LOG_FILE,
                "debug",
                f"Album view refreshed: {TARGET_PATH.relative_to(OUTPUT_DIR)} -> {ENTRY.path}",
            )


# ------------------------------------------------------------------------------
# This function creates an album-view file as a hard link with copy fallback.
#
# 1. "SOURCE_PATH" is the canonical library file.
# 2. "TARGET_PATH" is the album-view file path.
#
# Returns: None.
# ------------------------------------------------------------------------------
def create_album_link(SOURCE_PATH: Path, TARGET_PATH: Path) -> None:
    TARGET_PATH.parent.mkdir(parents=True, exist_ok=True)

    if TARGET_PATH.exists():
        if same_file_contents(TARGET_PATH, SOURCE_PATH):
            return

        TARGET_PATH.unlink()

    try:
        os.link(SOURCE_PATH, TARGET_PATH)
        return
    except OSError:
        shutil.copy2(SOURCE_PATH, TARGET_PATH)


# ------------------------------------------------------------------------------
# This function compares two files by inode or byte identity.
#
# 1. "LEFT_PATH" is the first path.
# 2. "RIGHT_PATH" is the second path.
#
# Returns: True when both paths already represent the same file data.
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


# ------------------------------------------------------------------------------
# This function deletes local files that are no longer present remotely.
#
# 1. "OUTPUT_DIR" is local backup root.
# 2. "ENTRIES" is the current remote photo list.
# 3. "NEW_MANIFEST" is the refreshed manifest under construction.
# 4. "LOG_FILE" is optional log file path.
#
# Returns: None.
# ------------------------------------------------------------------------------
def delete_removed_local_paths(
    OUTPUT_DIR: Path,
    ENTRIES: list[RemoteEntry],
    NEW_MANIFEST: dict[str, dict[str, Any]],
    LOG_FILE: Path | None,
) -> None:
    DESIRED_PATHS = desired_relative_paths(ENTRIES)

    for PATH in OUTPUT_DIR.rglob("*"):
        if not PATH.is_file():
            continue

        RELATIVE_PATH = str(PATH.relative_to(OUTPUT_DIR))

        if RELATIVE_PATH in DESIRED_PATHS:
            continue

        try:
            PATH.unlink()
        except OSError:
            continue

        NEW_MANIFEST.pop(RELATIVE_PATH, None)

        if LOG_FILE is None:
            continue

        log_line(LOG_FILE, "debug", f"Removed local file: {RELATIVE_PATH}")

    prune_empty_directories(OUTPUT_DIR)


# ------------------------------------------------------------------------------
# This function returns all desired relative output paths for the current run.
#
# 1. "ENTRIES" is the current remote photo list.
#
# Returns: Set of canonical and album-view paths.
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
# Returns: None.
# ------------------------------------------------------------------------------
def prune_empty_directories(OUTPUT_DIR: Path) -> None:
    DIRECTORIES = [PATH for PATH in OUTPUT_DIR.rglob("*") if PATH.is_dir()]
    DIRECTORIES.sort(key=lambda PATH: len(PATH.parts), reverse=True)

    for DIR_PATH in DIRECTORIES:
        try:
            next(DIR_PATH.iterdir())
        except StopIteration:
            try:
                DIR_PATH.rmdir()
            except OSError:
                continue
        except OSError:
            continue
