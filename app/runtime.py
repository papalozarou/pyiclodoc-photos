# ------------------------------------------------------------------------------
# This module coordinates one-shot and persistent worker runtime execution.
# ------------------------------------------------------------------------------

from __future__ import annotations

from pathlib import Path
import time

from app.auth_flow import attempt_auth, process_reauth_reminders
from app.config import AppConfig
from app.icloud_client import ICloudDriveClient
from app.logger import log_line
from app.scheduler import format_schedule_line, get_next_run_epoch
from app.state import AuthState, load_manifest, save_manifest
from app.syncer import perform_incremental_sync, run_first_time_safety_net
from app.telegram_bot import TelegramConfig, send_message
from app.telegram_control import handle_command, process_commands
from app.telegram_messages import (
    build_backup_complete_message,
    build_backup_skipped_auth_message,
    build_backup_skipped_reauth_message,
    build_backup_started_message,
    build_one_shot_auth_wait_message,
    build_safety_net_blocked_message,
    format_apple_id_label,
)
from app.transfer_runner import get_transfer_worker_count

RUN_ONCE_AUTH_WAIT_SECONDS = 900
RUN_ONCE_AUTH_POLL_SECONDS = 5


# ------------------------------------------------------------------------------
# This function sends a Telegram message when integration is configured.
#
# 1. "TELEGRAM" is Telegram integration configuration.
# 2. "MESSAGE" is outgoing message content.
#
# Returns: None.
# ------------------------------------------------------------------------------
def notify(TELEGRAM: TelegramConfig, MESSAGE: str) -> None:
    send_message(TELEGRAM, MESSAGE)


# ------------------------------------------------------------------------------
# This function formats elapsed seconds as "HH:MM:SS".
#
# 1. "TOTAL_SECONDS" is elapsed duration in seconds.
#
# Returns: Zero-padded duration string.
# ------------------------------------------------------------------------------
def format_duration_clock(TOTAL_SECONDS: int) -> str:
    SAFE_SECONDS = max(TOTAL_SECONDS, 0)
    HOURS = SAFE_SECONDS // 3600
    MINUTES = (SAFE_SECONDS % 3600) // 60
    SECONDS = SAFE_SECONDS % 60
    return f"{HOURS:02d}:{MINUTES:02d}:{SECONDS:02d}"


# ------------------------------------------------------------------------------
# This function formats average transfer speed using binary megabytes per
# second.
#
# 1. "TRANSFERRED_BYTES" is successful download byte total.
# 2. "DURATION_SECONDS" is elapsed run duration in seconds.
#
# Returns: Human-readable transfer speed string.
# ------------------------------------------------------------------------------
def format_average_speed(TRANSFERRED_BYTES: int, DURATION_SECONDS: int) -> str:
    SAFE_BYTES = max(TRANSFERRED_BYTES, 0)
    SAFE_DURATION_SECONDS = max(DURATION_SECONDS, 1)
    MEBIBYTES_PER_SECOND = SAFE_BYTES / SAFE_DURATION_SECONDS / (1024 * 1024)
    return f"{MEBIBYTES_PER_SECOND:.2f} MiB/s"


# ------------------------------------------------------------------------------
# This function logs effective non-secret backup settings for debug runs.
#
# 1. "CONFIG" is runtime configuration.
# 2. "LOG_FILE" is worker log destination.
# 3. "BUILD_DETAIL" contains app and dependency version metadata.
#
# Returns: None.
# ------------------------------------------------------------------------------
def log_effective_backup_settings(
    CONFIG: AppConfig,
    LOG_FILE: Path,
    BUILD_DETAIL: dict[str, str],
) -> None:
    SYNC_WORKERS_LABEL = "auto" if CONFIG.sync_workers == 0 else str(CONFIG.sync_workers)
    EFFECTIVE_WORKERS = get_transfer_worker_count(CONFIG.sync_workers)
    log_line(
        LOG_FILE,
        "debug",
        "Build detail: "
        f"app_build_ref={BUILD_DETAIL['app_build_ref']}, "
        f"pyicloud_version={BUILD_DETAIL['pyicloud_version']}",
    )
    log_line(
        LOG_FILE,
        "debug",
        "Effective backup settings detail: "
        f"run_once={CONFIG.run_once}, "
        f"schedule_mode={CONFIG.schedule_mode}, "
        f"schedule_interval_minutes={CONFIG.schedule_interval_minutes}, "
        f"schedule_backup_time={CONFIG.schedule_backup_time}, "
        f"schedule_weekdays={CONFIG.schedule_weekdays}, "
        f"schedule_monthly_week={CONFIG.schedule_monthly_week}, "
        f"sync_download_workers={SYNC_WORKERS_LABEL}, "
        f"effective_download_workers={EFFECTIVE_WORKERS}, "
        f"sync_download_chunk_mib={CONFIG.download_chunk_mib}, "
        f"backup_delete_removed={CONFIG.backup_delete_removed}, "
        f"backup_albums_enabled={CONFIG.backup_albums_enabled}, "
        f"backup_album_links_mode={CONFIG.backup_album_links_mode}",
    )


# ------------------------------------------------------------------------------
# This function removes a safety-net marker when it is no longer required.
#
# 1. "MARKER_PATH" is the marker file to remove.
# 2. "LOG_FILE" is worker log destination.
# 3. "DESCRIPTION" names the marker purpose for logs.
#
# Returns: True when the marker is absent or removed, otherwise False.
#
# N.B.
# Safety-net markers are operational state only. Failure to clear them must not
# crash the worker, but it must stop the run because the state transition was
# not persisted safely.
# ------------------------------------------------------------------------------
def clear_safety_net_marker(
    MARKER_PATH: Path,
    LOG_FILE: Path,
    DESCRIPTION: str,
) -> bool:
    if not MARKER_PATH.exists():
        return True

    try:
        MARKER_PATH.unlink()
        return True
    except OSError as ERROR:
        log_line(
            LOG_FILE,
            "error",
            f"Safety-net marker clear failed for {DESCRIPTION}: {MARKER_PATH} ({ERROR})",
        )
        return False


# ------------------------------------------------------------------------------
# This function writes one safety-net marker outcome to disk.
#
# 1. "MARKER_PATH" is the marker file path.
# 2. "MARKER_VALUE" is the marker file content.
# 3. "LOG_FILE" is worker log destination.
# 4. "DESCRIPTION" names the marker purpose for logs.
#
# Returns: True when the marker was written, otherwise False.
#
# N.B.
# Safety-net marker persistence is treated as explicit operational state. The
# worker must fail safely when it cannot record that state.
# ------------------------------------------------------------------------------
def write_safety_net_marker(
    MARKER_PATH: Path,
    MARKER_VALUE: str,
    LOG_FILE: Path,
    DESCRIPTION: str,
) -> bool:
    try:
        MARKER_PATH.write_text(MARKER_VALUE, encoding="utf-8")
        return True
    except OSError as ERROR:
        log_line(
            LOG_FILE,
            "error",
            f"Safety-net marker write failed for {DESCRIPTION}: {MARKER_PATH} ({ERROR})",
        )
        return False


# ------------------------------------------------------------------------------
# This function enforces first-run safety checks before backups are allowed.
#
# 1. "CONFIG" is runtime configuration.
# 2. "TELEGRAM" is Telegram integration configuration.
# 3. "LOG_FILE" is worker log path.
#
# Returns: True when backup can proceed; otherwise False.
# ------------------------------------------------------------------------------
def enforce_safety_net(CONFIG: AppConfig, TELEGRAM: TelegramConfig, LOG_FILE: Path) -> bool:
    DONE_MARKER = CONFIG.config_dir / "pyiclodoc-photos-safety_net_done.flag"
    BLOCKED_MARKER = CONFIG.config_dir / "pyiclodoc-photos-safety_net_blocked.flag"

    if DONE_MARKER.exists():
        return True

    RESULT = run_first_time_safety_net(CONFIG.output_dir, CONFIG.safety_net_sample_size)

    if not RESULT.should_block and not clear_safety_net_marker(
        BLOCKED_MARKER,
        LOG_FILE,
        "blocked state",
    ):
        return False

    if not RESULT.should_block:
        if not write_safety_net_marker(
            DONE_MARKER,
            "ok\n",
            LOG_FILE,
            "done state",
        ):
            return False

        log_line(LOG_FILE, "info", "First-run safety net passed.")
        return True

    if BLOCKED_MARKER.exists():
        return False

    MISMATCH_TEXT = "\n".join(RESULT.mismatched_samples)
    log_line(LOG_FILE, "error", "Safety net blocked backup due to permissions.")
    log_line(LOG_FILE, "error", MISMATCH_TEXT)
    APPLE_ID_LABEL = format_apple_id_label(CONFIG.icloud_email)
    SAMPLE_TEXT = ", ".join(RESULT.mismatched_samples[:2]) or "<none>"
    notify(
        TELEGRAM,
        build_safety_net_blocked_message(
            APPLE_ID_LABEL,
            RESULT.expected_uid,
            RESULT.expected_gid,
            SAMPLE_TEXT,
        ),
    )
    write_succeeded = write_safety_net_marker(
        BLOCKED_MARKER,
        "blocked\n",
        LOG_FILE,
        "blocked state",
    )
    return False


# ------------------------------------------------------------------------------
# This function executes one backup pass and persists refreshed manifest data.
#
# 1. "CLIENT" is iCloud client wrapper.
# 2. "CONFIG" is runtime configuration.
# 3. "TELEGRAM" is Telegram integration configuration.
# 4. "LOG_FILE" is worker log destination.
# 5. "TRIGGER" is backup trigger context.
# 6. "BUILD_DETAIL" contains app and dependency version metadata.
#
# Returns: None.
# ------------------------------------------------------------------------------
def run_backup(
    CLIENT: ICloudDriveClient,
    CONFIG: AppConfig,
    TELEGRAM: TelegramConfig,
    LOG_FILE: Path,
    TRIGGER: str,
    BUILD_DETAIL: dict[str, str],
) -> None:
    log_effective_backup_settings(CONFIG, LOG_FILE, BUILD_DETAIL)
    MANIFEST = load_manifest(CONFIG.manifest_path)
    log_line(LOG_FILE, "debug", f"Loaded manifest entries: {len(MANIFEST)}")
    APPLE_ID_LABEL = format_apple_id_label(CONFIG.icloud_email)
    RUN_START_EPOCH = int(time.time())
    SCHEDULE_LINE = format_schedule_line(CONFIG, TRIGGER)
    notify(TELEGRAM, build_backup_started_message(APPLE_ID_LABEL, SCHEDULE_LINE))

    SUMMARY, NEW_MANIFEST = perform_incremental_sync(
        CLIENT,
        CONFIG.output_dir,
        MANIFEST,
        CONFIG.sync_workers,
        LOG_FILE,
        BACKUP_DELETE_REMOVED=CONFIG.backup_delete_removed,
        BACKUP_ALBUMS_ENABLED=CONFIG.backup_albums_enabled,
        BACKUP_ALBUM_LINKS_MODE=CONFIG.backup_album_links_mode,
        BACKUP_ROOT_ALBUMS=CONFIG.backup_root_albums,
    )
    log_line(
        LOG_FILE,
        "debug",
        "Sync summary detail: "
        f"total={SUMMARY.total_files}, "
        f"transferred={SUMMARY.transferred_files}, "
        f"bytes={SUMMARY.transferred_bytes}, "
        f"skipped={SUMMARY.skipped_files}, "
        f"errors={SUMMARY.error_files}, "
        f"manifest_entries={len(NEW_MANIFEST)}",
    )
    save_manifest(CONFIG.manifest_path, NEW_MANIFEST)

    DURATION_SECONDS = int(time.time()) - RUN_START_EPOCH
    AVERAGE_SPEED = format_average_speed(SUMMARY.transferred_bytes, DURATION_SECONDS)
    STATUS_LINES = [
        f"Transferred: {SUMMARY.transferred_files}/{SUMMARY.total_files}",
        f"Skipped: {SUMMARY.skipped_files}",
        f"Errors: {SUMMARY.error_files}",
        f"Duration: {format_duration_clock(DURATION_SECONDS)}",
    ]

    if SUMMARY.transferred_files > 0:
        STATUS_LINES.append(f"Average speed: {AVERAGE_SPEED}")

    COMPLETION_MESSAGE = build_backup_complete_message(APPLE_ID_LABEL, STATUS_LINES)
    notify(TELEGRAM, COMPLETION_MESSAGE)
    log_line(
        LOG_FILE,
        "info",
        "Backup complete. "
        f"Transferred {SUMMARY.transferred_files}/{SUMMARY.total_files}, "
        f"skipped {SUMMARY.skipped_files}, errors {SUMMARY.error_files}.",
    )


# ------------------------------------------------------------------------------
# This function waits for one-shot authentication commands before exit.
#
# 1. "CONFIG" is runtime configuration.
# 2. "CLIENT" is iCloud client wrapper.
# 3. "AUTH_STATE" is current auth state.
# 4. "IS_AUTHENTICATED" tracks current auth validity.
# 5. "TELEGRAM" is Telegram integration configuration.
#
# Returns: Tuple "(auth_state, is_authenticated)".
# ------------------------------------------------------------------------------
def wait_for_one_shot_auth(
    CONFIG: AppConfig,
    CLIENT: ICloudDriveClient,
    AUTH_STATE: AuthState,
    IS_AUTHENTICATED: bool,
    TELEGRAM: TelegramConfig,
) -> tuple[AuthState, bool]:
    START_EPOCH = int(time.time())
    UPDATE_OFFSET: int | None = None

    while True:
        if IS_AUTHENTICATED and not AUTH_STATE.reauth_pending:
            return AUTH_STATE, IS_AUTHENTICATED

        NOW_EPOCH = int(time.time())
        ELAPSED_SECONDS = NOW_EPOCH - START_EPOCH

        if ELAPSED_SECONDS >= RUN_ONCE_AUTH_WAIT_SECONDS:
            return AUTH_STATE, IS_AUTHENTICATED

        COMMANDS, UPDATE_OFFSET = process_commands(
            TELEGRAM,
            CONFIG.container_username,
            UPDATE_OFFSET,
        )

        for COMMAND, ARGS in COMMANDS:
            OUTCOME = handle_command(
                COMMAND,
                ARGS,
                CONFIG,
                AUTH_STATE,
                IS_AUTHENTICATED,
                lambda MESSAGE: notify(TELEGRAM, MESSAGE),
                lambda CURRENT_STATE, PROVIDED_CODE: attempt_auth(
                    CLIENT,
                    CURRENT_STATE,
                    CONFIG.auth_state_path,
                    lambda MESSAGE: notify(TELEGRAM, MESSAGE),
                    CONFIG.container_username,
                    CONFIG.icloud_email,
                    PROVIDED_CODE,
                ),
            )
            AUTH_STATE = OUTCOME.auth_state
            IS_AUTHENTICATED = OUTCOME.is_authenticated

        time.sleep(RUN_ONCE_AUTH_POLL_SECONDS)


# ------------------------------------------------------------------------------
# This function runs the one-shot worker path and returns the process exit
# state.
#
# 1. "CONFIG" is runtime configuration.
# 2. "CLIENT" is iCloud client wrapper.
# 3. "AUTH_STATE" is current auth state.
# 4. "IS_AUTHENTICATED" tracks current auth validity.
# 5. "TELEGRAM" is Telegram integration configuration.
# 6. "LOG_FILE" is worker log destination.
# 7. "BUILD_DETAIL" contains app and dependency version metadata.
#
# Returns: Tuple "(exit_code, stop_status)".
# ------------------------------------------------------------------------------
def run_one_shot_runtime(
    CONFIG: AppConfig,
    CLIENT: ICloudDriveClient,
    AUTH_STATE: AuthState,
    IS_AUTHENTICATED: bool,
    TELEGRAM: TelegramConfig,
    LOG_FILE: Path,
    BUILD_DETAIL: dict[str, str],
) -> tuple[int, str]:
    APPLE_ID_LABEL = format_apple_id_label(CONFIG.icloud_email)

    if not IS_AUTHENTICATED or AUTH_STATE.reauth_pending:
        notify(
            TELEGRAM,
            build_one_shot_auth_wait_message(
                APPLE_ID_LABEL,
                max(1, RUN_ONCE_AUTH_WAIT_SECONDS // 60),
            ),
        )
        AUTH_STATE, IS_AUTHENTICATED = wait_for_one_shot_auth(
            CONFIG,
            CLIENT,
            AUTH_STATE,
            IS_AUTHENTICATED,
            TELEGRAM,
        )

    if not IS_AUTHENTICATED:
        notify(TELEGRAM, build_backup_skipped_auth_message(APPLE_ID_LABEL))
        return 2, "One-shot backup skipped due to incomplete authentication."

    if AUTH_STATE.reauth_pending:
        notify(TELEGRAM, build_backup_skipped_reauth_message(APPLE_ID_LABEL))
        return 3, "One-shot backup skipped due to pending reauthentication."

    if not enforce_safety_net(CONFIG, TELEGRAM, LOG_FILE):
        return 4, "One-shot backup blocked by safety net."

    run_backup(CLIENT, CONFIG, TELEGRAM, LOG_FILE, "one-shot", BUILD_DETAIL)
    return 0, "Run completed and container exited."


# ------------------------------------------------------------------------------
# This function runs the persistent worker loop for scheduled and manual
# backups.
#
# 1. "CONFIG" is runtime configuration.
# 2. "CLIENT" is iCloud client wrapper.
# 3. "AUTH_STATE" is current auth state.
# 4. "IS_AUTHENTICATED" tracks current auth validity.
# 5. "TELEGRAM" is Telegram integration configuration.
# 6. "LOG_FILE" is worker log destination.
# 7. "BUILD_DETAIL" contains app and dependency version metadata.
#
# Returns: None.
# ------------------------------------------------------------------------------
def run_persistent_runtime(
    CONFIG: AppConfig,
    CLIENT: ICloudDriveClient,
    AUTH_STATE: AuthState,
    IS_AUTHENTICATED: bool,
    TELEGRAM: TelegramConfig,
    LOG_FILE: Path,
    BUILD_DETAIL: dict[str, str],
) -> None:
    BACKUP_REQUESTED = False
    NEXT_UPDATE_OFFSET: int | None = None
    INITIAL_EPOCH = int(time.time())

    if CONFIG.schedule_mode == "interval":
        NEXT_RUN_EPOCH = INITIAL_EPOCH
    else:
        NEXT_RUN_EPOCH = get_next_run_epoch(CONFIG, INITIAL_EPOCH)

    while True:
        AUTH_STATE = process_reauth_reminders(
            AUTH_STATE,
            CONFIG.auth_state_path,
            lambda MESSAGE: notify(TELEGRAM, MESSAGE),
            CONFIG.container_username,
            CONFIG.reauth_interval_days,
        )
        COMMANDS, NEXT_UPDATE_OFFSET = process_commands(
            TELEGRAM,
            CONFIG.container_username,
            NEXT_UPDATE_OFFSET,
        )

        for COMMAND, ARGS in COMMANDS:
            OUTCOME = handle_command(
                COMMAND,
                ARGS,
                CONFIG,
                AUTH_STATE,
                IS_AUTHENTICATED,
                lambda MESSAGE: notify(TELEGRAM, MESSAGE),
                lambda CURRENT_STATE, PROVIDED_CODE: attempt_auth(
                    CLIENT,
                    CURRENT_STATE,
                    CONFIG.auth_state_path,
                    lambda MESSAGE: notify(TELEGRAM, MESSAGE),
                    CONFIG.container_username,
                    CONFIG.icloud_email,
                    PROVIDED_CODE,
                ),
            )
            AUTH_STATE = OUTCOME.auth_state
            IS_AUTHENTICATED = OUTCOME.is_authenticated
            BACKUP_REQUESTED = BACKUP_REQUESTED or OUTCOME.backup_requested
            if OUTCOME.details:
                log_line(LOG_FILE, "info", f"Auth command result: {OUTCOME.details}")

        NOW_EPOCH = int(time.time())
        SCHEDULE_DUE = NOW_EPOCH >= NEXT_RUN_EPOCH

        if not SCHEDULE_DUE and not BACKUP_REQUESTED:
            time.sleep(5)
            continue

        NEXT_RUN_EPOCH = get_next_run_epoch(CONFIG, NOW_EPOCH)

        if not IS_AUTHENTICATED:
            notify(
                TELEGRAM,
                build_backup_skipped_auth_message(format_apple_id_label(CONFIG.icloud_email)),
            )
            time.sleep(5)
            continue

        if AUTH_STATE.reauth_pending:
            notify(
                TELEGRAM,
                build_backup_skipped_reauth_message(format_apple_id_label(CONFIG.icloud_email)),
            )
            time.sleep(5)
            continue

        if not enforce_safety_net(CONFIG, TELEGRAM, LOG_FILE):
            time.sleep(30)
            continue

        BACKUP_TRIGGER = "manual" if BACKUP_REQUESTED else "scheduled"
        run_backup(CLIENT, CONFIG, TELEGRAM, LOG_FILE, BACKUP_TRIGGER, BUILD_DETAIL)
        BACKUP_REQUESTED = False
        time.sleep(5)
