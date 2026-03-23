# ------------------------------------------------------------------------------
# This module builds all Telegram message text emitted by the worker.
#
# Message construction is kept separate from runtime orchestration so wording
# changes do not require edits inside the worker loop, auth flow, or backup
# control paths.
# ------------------------------------------------------------------------------

from __future__ import annotations

from pathlib import Path


# ------------------------------------------------------------------------------
# This function formats a fallback-safe Apple ID label for Telegram messages.
#
# 1. "APPLE_ID" is the configured iCloud email value.
#
# Returns: Non-empty Apple ID label.
# ------------------------------------------------------------------------------
def format_apple_id_label(APPLE_ID: str) -> str:
    CLEAN_VALUE = APPLE_ID.strip()

    if CLEAN_VALUE:
        return CLEAN_VALUE

    return "<unknown>"


# ------------------------------------------------------------------------------
# This function builds a compact multi-line Telegram event message.
#
# 1. "ICON" is the leading emoji marker.
# 2. "TITLE" is sentence-case message heading.
# 3. "DESCRIPTION" is one-line activity summary.
# 4. "STATUS_LINES" are optional detail lines.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def format_telegram_event(
    ICON: str,
    TITLE: str,
    DESCRIPTION: str,
    STATUS_LINES: list[str] | None = None,
) -> str:
    LINES = [f"{ICON} PCD Photos - {TITLE}", DESCRIPTION]

    if STATUS_LINES:
        LINES.extend([LINE for LINE in STATUS_LINES if LINE.strip()])

    return "\n".join(LINES)


# ------------------------------------------------------------------------------
# This function builds the container-started Telegram message.
#
# 1. "APPLE_ID" is the configured iCloud email value.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def build_container_started_message(APPLE_ID: str) -> str:
    return format_telegram_event(
        "🟢",
        "Container started",
        f"Worker started for Apple ID {format_apple_id_label(APPLE_ID)}.",
        ["Initialising authentication and backup checks."],
    )


# ------------------------------------------------------------------------------
# This function builds the container-stopped Telegram message.
#
# 1. "APPLE_ID" is the configured iCloud email value.
# 2. "STOP_STATUS" is the worker shutdown status line.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def build_container_stopped_message(APPLE_ID: str, STOP_STATUS: str) -> str:
    return format_telegram_event(
        "🛑",
        "Container stopped",
        f"Worker stopped for Apple ID {format_apple_id_label(APPLE_ID)}.",
        [STOP_STATUS],
    )


# ------------------------------------------------------------------------------
# This function builds the authentication-complete Telegram message.
#
# 1. "APPLE_ID" is the configured iCloud email value.
# 2. "DETAILS" is the auth-result detail string.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def build_auth_complete_message(APPLE_ID: str, DETAILS: str) -> str:
    return format_telegram_event(
        "🔒",
        "Authentication complete",
        f"Authenticated for Apple ID {format_apple_id_label(APPLE_ID)}.",
        [DETAILS],
    )


# ------------------------------------------------------------------------------
# This function builds the authentication-required Telegram message.
#
# 1. "USERNAME" is the Telegram command prefix.
# 2. "APPLE_ID" is the configured iCloud email value.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def build_auth_required_message(USERNAME: str, APPLE_ID: str) -> str:
    return format_telegram_event(
        "🔑",
        "Authentication required",
        f"Authentication required for Apple ID {format_apple_id_label(APPLE_ID)}.",
        [
            f"Send: {USERNAME} auth 123456",
            f"Or: {USERNAME} reauth 123456",
        ],
    )


# ------------------------------------------------------------------------------
# This function builds the authentication-failed Telegram message.
#
# 1. "APPLE_ID" is the configured iCloud email value.
# 2. "DETAILS" is the auth-result detail string.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def build_auth_failed_message(APPLE_ID: str, DETAILS: str) -> str:
    return format_telegram_event(
        "❌",
        "Authentication failed",
        f"Authentication failed for Apple ID {format_apple_id_label(APPLE_ID)}.",
        [f"Reason: {DETAILS}"],
    )


# ------------------------------------------------------------------------------
# This function builds the one-shot auth-wait Telegram message.
#
# 1. "APPLE_ID" is the configured iCloud email value.
# 2. "WAIT_MINUTES" is the auth wait window in whole minutes.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def build_one_shot_auth_wait_message(APPLE_ID: str, WAIT_MINUTES: int) -> str:
    return format_telegram_event(
        "🔑",
        "Authentication required",
        f"Authentication required for Apple ID {format_apple_id_label(APPLE_ID)}.",
        [
            "One-shot mode is waiting for an auth command before backup.",
            f"Wait window: {WAIT_MINUTES} mins.",
        ],
    )


# ------------------------------------------------------------------------------
# This function builds the reauthentication-required Telegram reminder message.
#
# 1. "USERNAME" is the Telegram command prefix.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def build_reauth_due_message(USERNAME: str) -> str:
    return format_telegram_event(
        "🔑",
        "Reauthentication required",
        "Reauthentication is due within two days.",
        [f"Send: {USERNAME} reauth"],
    )


# ------------------------------------------------------------------------------
# This function builds the manual reauthentication Telegram prompt.
#
# 1. "USERNAME" is the Telegram command prefix.
# 2. "APPLE_ID" is the configured iCloud email value.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def build_manual_reauth_message(USERNAME: str, APPLE_ID: str) -> str:
    return format_telegram_event(
        "🔑",
        "Reauthentication required",
        f"Reauthentication required for Apple ID {format_apple_id_label(APPLE_ID)}.",
        [f"Send: {USERNAME} reauth 123456"],
    )


# ------------------------------------------------------------------------------
# This function builds the auth-state persistence failure Telegram message.
#
# 1. "ACTION_LABEL" names the rejected auth or reauth command.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def build_auth_state_persistence_failed_message(ACTION_LABEL: str) -> str:
    return format_telegram_event(
        "⚠️",
        "Auth state update failed",
        f"Could not persist the requested {ACTION_LABEL} state change.",
        ["Retry the command after checking /config write access."],
    )


# ------------------------------------------------------------------------------
# This function builds the reauthentication reminder Telegram message.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def build_reauth_reminder_message() -> str:
    return format_telegram_event(
        "📣",
        "Reauth reminder",
        "Reauthentication will be required within five days.",
    )


# ------------------------------------------------------------------------------
# This function builds the safety-net blocked Telegram message.
#
# 1. "APPLE_ID" is the configured iCloud email value.
# 2. "EXPECTED_UID" is the runtime user ID that should own files.
# 3. "EXPECTED_GID" is the runtime group ID that should own files.
# 4. "SAMPLE_TEXT" is a compact mismatch preview.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def build_safety_net_blocked_message(
    APPLE_ID: str,
    EXPECTED_UID: int,
    EXPECTED_GID: int,
    SAMPLE_TEXT: str,
) -> str:
    return format_telegram_event(
        "⚠️",
        "Safety net blocked",
        f"Backup blocked for Apple ID {format_apple_id_label(APPLE_ID)}.",
        [
            "Permission mismatches detected in existing files.",
            f"Expected: uid {EXPECTED_UID}, gid {EXPECTED_GID}",
            f"Sample mismatches: {SAMPLE_TEXT}",
        ],
    )


# ------------------------------------------------------------------------------
# This function builds the backup-requested Telegram message.
#
# 1. "APPLE_ID" is the configured iCloud email value.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def build_backup_requested_message(APPLE_ID: str) -> str:
    return format_telegram_event(
        "📥",
        "Backup requested",
        f"Manual backup requested for Apple ID {format_apple_id_label(APPLE_ID)}.",
        ["Worker queued backup to run now."],
    )


# ------------------------------------------------------------------------------
# This function builds the backup-started Telegram message.
#
# 1. "APPLE_ID" is the configured iCloud email value.
# 2. "SCHEDULE_LINE" is the human-readable schedule status line.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def build_backup_started_message(APPLE_ID: str, SCHEDULE_LINE: str) -> str:
    return format_telegram_event(
        "⬇️",
        "Backup started",
        f"Photos downloading for Apple ID {format_apple_id_label(APPLE_ID)}.",
        [SCHEDULE_LINE],
    )


# ------------------------------------------------------------------------------
# This function builds the backup-complete Telegram message.
#
# 1. "APPLE_ID" is the configured iCloud email value.
# 2. "STATUS_LINES" is the summary line list for completion detail.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def build_backup_complete_message(APPLE_ID: str, STATUS_LINES: list[str]) -> str:
    return format_telegram_event(
        "📦",
        "Backup complete",
        f"Backup finished for Apple ID {format_apple_id_label(APPLE_ID)}.",
        STATUS_LINES,
    )


# ------------------------------------------------------------------------------
# This function builds the authentication-incomplete skipped message.
#
# 1. "APPLE_ID" is the configured iCloud email value.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def build_backup_skipped_auth_message(APPLE_ID: str) -> str:
    return format_telegram_event(
        "⏭️",
        "Backup skipped",
        f"Backup skipped for Apple ID {format_apple_id_label(APPLE_ID)}.",
        ["Reason: Authentication incomplete."],
    )


# ------------------------------------------------------------------------------
# This function builds the reauthentication-pending skipped message.
#
# 1. "APPLE_ID" is the configured iCloud email value.
#
# Returns: Formatted Telegram message text.
# ------------------------------------------------------------------------------
def build_backup_skipped_reauth_message(APPLE_ID: str) -> str:
    return format_telegram_event(
        "⏭️",
        "Backup skipped",
        f"Backup skipped for Apple ID {format_apple_id_label(APPLE_ID)}.",
        ["Reason: Reauthentication pending."],
    )
