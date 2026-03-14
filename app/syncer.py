# ------------------------------------------------------------------------------
# This module performs incremental iCloud Photos synchronisation with manifest,
# album-link, and safety-net logic.
# 
# The sync model keeps one canonical file path per asset and treats album views
# as derived outputs. That separation keeps deletion and reimport decisions
# easier to reason about.
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
# 
# N.B.
# The upper bound is deliberately conservative because photo downloads are more
# likely to be network-bound than CPU-bound.
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
# This function collects a bounded local-file sample for permission checks.
#
# 1. "OUTPUT_DIR" is the backup destination root.
# 2. "SAMPLE_SIZE" is the sample cap.
#
# Returns: Ordered file list up to "SAMPLE_SIZE" for ownership analysis.
# 
# N.B.
# The sample uses filesystem walk order. It does not try to be statistically
# representative; it only needs to surface obvious ownership mismatches.
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
# 
# N.B.
# Album paths are stored in the canonical entry metadata so derived album views
# can be recreated without re-reading remote album membership during deletes.
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
# 
# Behaviour notes:
# 1. Only canonical library files participate in transfer decisions.
# 2. Album views are reconciled after canonical transfers complete.
# 3. Optional delete handling removes both canonical and derived local paths.
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
    FILES = [ENTRY for ENTRY in ENTRIES if not ENTRY.is_dir]

    if LOG_FILE is not None:
        log_line(LOG_FILE, "info", f"Remote photo listing finished. files={len(FILES)}.")
        log_line(
            LOG_FILE,
            "debug",
            "Remote listing detail: "
            f"entries={len(ENTRIES)}, files={len(FILES)}",
        )

    NEW_MANIFEST: dict[str, dict[str, Any]] = {}
    TRANSFER_CANDIDATES: list[RemoteEntry] = []
    TRANSFERRED = 0
    TRANSFERRED_BYTES = 0
    SKIPPED = 0
    ERRORS = 0
    FAILURE_REASON_COUNTS: dict[str, int] = {}

    for ENTRY in FILES:
        if needs_transfer(ENTRY, MANIFEST):
            TRANSFER_CANDIDATES.append(ENTRY)
            if LOG_FILE is not None:
                log_line(
                    LOG_FILE,
                    "debug",
                    f"Photo queued for transfer: {ENTRY.path} "
                    f"({max(ENTRY.size, 0)} bytes)",
                )
            continue

        NEW_MANIFEST[ENTRY.path] = entry_metadata(ENTRY)
        SKIPPED += 1
        if LOG_FILE is not None:
            log_line(LOG_FILE, "debug", f"Photo skipped unchanged: {ENTRY.path}")

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

        TRANSFERRED, TRANSFERRED_BYTES, ERRORS, FAILURE_REASON_COUNTS = run_transfers(
            CLIENT,
            OUTPUT_DIR,
            TRANSFER_CANDIDATES,
            NEW_MANIFEST,
            LOG_FILE,
            WORKER_COUNT,
        )
    elif LOG_FILE is not None:
        log_line(LOG_FILE, "info", "Transfer skipped. candidates=0.")

    if LOG_FILE is not None:
        log_line(LOG_FILE, "info", "Album reconciliation started.")

    ALBUM_VIEWS_CREATED, ALBUM_VIEWS_REUSED, ALBUM_VIEWS_SKIPPED = reconcile_album_views(
        OUTPUT_DIR,
        FILES,
        NEW_MANIFEST,
        LOG_FILE,
    )

    if LOG_FILE is not None:
        log_line(
            LOG_FILE,
            "info",
            "Album reconciliation finished. "
            f"created={ALBUM_VIEWS_CREATED}, "
            f"reused={ALBUM_VIEWS_REUSED}, "
            f"skipped_missing_source={ALBUM_VIEWS_SKIPPED}.",
        )

    if BACKUP_DELETE_REMOVED:
        if LOG_FILE is not None:
            log_line(LOG_FILE, "info", "Delete phase started.")

        DELETED_FILES, DELETED_DIRECTORIES, DELETE_ERRORS = delete_removed_local_paths(
            OUTPUT_DIR,
            FILES,
            NEW_MANIFEST,
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
            f"transferred={TRANSFERRED}, skipped={SKIPPED}, errors={ERRORS}.",
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

    return SyncResult(
        total_files=len(FILES),
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
# Returns: Tuple "(transferred, transferred_bytes, errors, failure_reason_counts)".
# 
# N.B.
# Transfer workers only touch canonical file paths. Album views are rebuilt in
# a later serial phase so link creation never races a file download.
# ------------------------------------------------------------------------------
def run_transfers(
    CLIENT: ICloudDriveClient,
    OUTPUT_DIR: Path,
    TRANSFER_CANDIDATES: list[RemoteEntry],
    NEW_MANIFEST: dict[str, dict[str, Any]],
    LOG_FILE: Path | None,
    WORKER_COUNT: int,
) -> tuple[int, int, int, dict[str, int]]:
    TRANSFERRED = 0
    TRANSFERRED_BYTES = 0
    ERRORS = 0
    FAILURE_REASON_COUNTS: dict[str, int] = {}
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
                    REASON = f"worker_exception:{type(ERROR).__name__}"
                    FAILURE_REASON_COUNTS[REASON] = FAILURE_REASON_COUNTS.get(REASON, 0) + 1
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
                    if LOG_FILE is not None:
                        log_line(
                            LOG_FILE,
                            "debug",
                            f"Photo transferred: {ENTRY.path} ({max(ENTRY.size, 0)} bytes)",
                        )
                    continue

                ERRORS += 1

                if LOG_FILE is not None:
                    REASON = CLIENT.get_last_download_failure_reason() or "unknown_error"
                    FAILURE_REASON_COUNTS[REASON] = FAILURE_REASON_COUNTS.get(REASON, 0) + 1
                    log_line(LOG_FILE, "error", f"File transfer failed: {ENTRY.path} ({REASON})")
                else:
                    REASON = CLIENT.get_last_download_failure_reason() or "unknown_error"
                    FAILURE_REASON_COUNTS[REASON] = FAILURE_REASON_COUNTS.get(REASON, 0) + 1

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

    return TRANSFERRED, TRANSFERRED_BYTES, ERRORS, FAILURE_REASON_COUNTS


# ------------------------------------------------------------------------------
# This function downloads one file when it is required.
#
# 1. "CLIENT" is the active iCloud API wrapper.
# 2. "OUTPUT_DIR" is local backup root.
# 3. "ENTRY" is the remote photo metadata record.
#
# Returns: True on successful file transfer, otherwise False.
# 
# N.B.
# The sync layer delegates all remote-open behaviour to the client so retry and
# response-shape complexity stays in one place.
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
# 
# N.B.
# Invalid or missing timestamps are ignored rather than treated as sync
# failures because the file content itself has already been written.
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
# Returns: Tuple "(created, reused, skipped_missing_source)".
# 
# N.B.
# Album view creation is intentionally best-effort. Missing canonical files are
# skipped quietly because a failed canonical transfer has already been counted.
# ------------------------------------------------------------------------------
def reconcile_album_views(
    OUTPUT_DIR: Path,
    ENTRIES: list[RemoteEntry],
    NEW_MANIFEST: dict[str, dict[str, Any]],
    LOG_FILE: Path | None,
) -> tuple[int, int, int]:
    CREATED = 0
    REUSED = 0
    SKIPPED_MISSING_SOURCE = 0

    for ENTRY in ENTRIES:
        if not ENTRY.album_paths:
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
            WAS_CREATED = create_album_link(SOURCE_PATH, TARGET_PATH)
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
#
# Returns: True when a new hard link or copy was created, otherwise False.
# 
# N.B.
# Hard links are preferred because they avoid duplicate data. Copy fallback is
# retained for filesystems and bind mounts that do not permit linking.
# ------------------------------------------------------------------------------
def create_album_link(SOURCE_PATH: Path, TARGET_PATH: Path) -> bool:
    TARGET_PATH.parent.mkdir(parents=True, exist_ok=True)

    if TARGET_PATH.exists():
        if same_file_contents(TARGET_PATH, SOURCE_PATH):
            return False

        TARGET_PATH.unlink()

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


# ------------------------------------------------------------------------------
# This function deletes local files that are no longer present remotely.
#
# 1. "OUTPUT_DIR" is local backup root.
# 2. "ENTRIES" is the current remote photo list.
# 3. "NEW_MANIFEST" is the refreshed manifest under construction.
# 4. "LOG_FILE" is optional log file path.
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
    NEW_MANIFEST: dict[str, dict[str, Any]],
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
