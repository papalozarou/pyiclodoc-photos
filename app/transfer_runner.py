# ------------------------------------------------------------------------------
# This module contains canonical file transfer execution for the photo sync
# workflow.
# ------------------------------------------------------------------------------

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import os
import time

from app.icloud_client import DownloadResult, ICloudDriveClient, RemoteEntry
from app.logger import log_line
from app.sync_plan import entry_metadata

TRANSFER_PROGRESS_LOG_INTERVAL_SECONDS = 30.0
PROGRESS_LOG_SEPARATOR = "------------------------------------------------------------"
TRANSFER_MAX_ATTEMPTS = 3
TRANSFER_RETRY_DELAY_SECONDS = 1.0


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
# This function downloads one file when it is required.
#
# 1. "CLIENT" is the active iCloud API wrapper.
# 2. "OUTPUT_DIR" is local backup root.
# 3. "ENTRY" is the remote photo metadata record.
#
# Returns: "DownloadResult" for the file transfer attempt.
#
# N.B.
# The sync layer delegates all remote-open behaviour to the client so retry and
# response-shape complexity stays in one place.
# ------------------------------------------------------------------------------
def transfer_if_required(
    CLIENT: ICloudDriveClient,
    OUTPUT_DIR: Path,
    ENTRY: RemoteEntry,
) -> DownloadResult:
    return CLIENT.download_file_result(ENTRY.path, OUTPUT_DIR / ENTRY.path)


# ------------------------------------------------------------------------------
# This function decides whether a failed transfer should be retried.
#
# 1. "FAILURE_REASON" is the transfer failure token returned by the client.
#
# Returns: True when the failure looks transient and worth retrying.
# ------------------------------------------------------------------------------
def should_retry_transfer(FAILURE_REASON: str) -> bool:
    if FAILURE_REASON.startswith("worker_exception:"):
        return True

    return FAILURE_REASON in {
        "download_read_failed",
        "empty_download",
        "incomplete_download",
        "network_error",
        "timeout",
    }


# ------------------------------------------------------------------------------
# This function runs one transfer with bounded retry handling.
#
# 1. "CLIENT" is the active iCloud API wrapper.
# 2. "OUTPUT_DIR" is local backup root.
# 3. "ENTRY" is the remote photo metadata record.
#
# Returns: Final "DownloadResult" after success or the last failed attempt.
# ------------------------------------------------------------------------------
def transfer_with_retry(
    CLIENT: ICloudDriveClient,
    OUTPUT_DIR: Path,
    ENTRY: RemoteEntry,
) -> DownloadResult:
    LAST_RESULT = DownloadResult(False, failure_reason="unknown_error")

    for ATTEMPT in range(1, TRANSFER_MAX_ATTEMPTS + 1):
        LAST_RESULT = transfer_if_required(CLIENT, OUTPUT_DIR, ENTRY)

        if LAST_RESULT.success:
            return LAST_RESULT

        if ATTEMPT >= TRANSFER_MAX_ATTEMPTS:
            return LAST_RESULT

        if not should_retry_transfer(LAST_RESULT.failure_reason):
            return LAST_RESULT

        time.sleep(TRANSFER_RETRY_DELAY_SECONDS * ATTEMPT)

    return LAST_RESULT


# ------------------------------------------------------------------------------
# This function records one failed canonical transfer outcome.
#
# 1. "ENTRY" is the remote file metadata for the failed transfer.
# 2. "RESULT" is the authoritative failed transfer result.
# 3. "FAILURE_REASON_COUNTS" is the shared per-run reason counter mapping.
# 4. "LOG_FILE" is optional worker log destination.
#
# Returns: Failure reason string recorded for the transfer.
#
# N.B.
# All failure aggregation passes through this function so worker exceptions and
# normal failed results cannot drift into separate counting paths.
# ------------------------------------------------------------------------------
def record_failed_transfer(
    ENTRY: RemoteEntry,
    RESULT: DownloadResult,
    FAILURE_REASON_COUNTS: dict[str, int],
    LOG_FILE: Path | None,
) -> str:
    REASON = RESULT.failure_reason or "unknown_error"
    FAILURE_REASON_COUNTS[REASON] = FAILURE_REASON_COUNTS.get(REASON, 0) + 1

    if LOG_FILE is not None:
        log_line(LOG_FILE, "error", f"File transfer failed: {ENTRY.path} ({REASON})")

    return REASON


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
            EXECUTOR.submit(transfer_with_retry, CLIENT, OUTPUT_DIR, ENTRY): ENTRY
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
                    RESULT = FUTURE.result()
                except Exception as ERROR:
                    RESULT = DownloadResult(
                        False,
                        failure_reason=f"worker_exception:{type(ERROR).__name__}",
                    )

                if RESULT.success:
                    LOCAL_PATH = OUTPUT_DIR / ENTRY.path
                    apply_remote_modified_time(LOCAL_PATH, ENTRY.modified)
                    NEW_MANIFEST[ENTRY.path] = entry_metadata(ENTRY)
                    TRANSFERRED += 1
                    TRANSFERRED_BYTES += max(RESULT.written_bytes, 0)
                    if LOG_FILE is not None:
                        log_line(
                            LOG_FILE,
                            "debug",
                            f"Photo transferred: {ENTRY.path} ({max(RESULT.written_bytes, 0)} bytes)",
                        )
                    continue

                ERRORS += 1
                record_failed_transfer(
                    ENTRY,
                    RESULT,
                    FAILURE_REASON_COUNTS,
                    LOG_FILE,
                )

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
