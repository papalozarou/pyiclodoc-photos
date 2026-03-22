# ------------------------------------------------------------------------------
# This module manages authentication state transitions and reauthentication
# reminder behaviour for the worker.
# ------------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from dateutil import parser as date_parser

from app.icloud_client import ICloudDriveClient
from app.state import AuthState, now_iso, save_auth_state
from app.telegram_messages import (
    build_auth_complete_message,
    build_auth_failed_message,
    build_auth_required_message,
    build_reauth_due_message,
    build_reauth_reminder_message,
    format_apple_id_label,
)
from app.time_utils import now_local


# ------------------------------------------------------------------------------
# This function parses an ISO timestamp with a strict epoch fallback.
#
# 1. "VALUE" is an ISO-formatted timestamp string.
#
# Returns: Offset-aware datetime; Unix epoch when parsing fails.
#
# Notes: dateutil parsing reference:
# https://dateutil.readthedocs.io/en/stable/parser.html
# ------------------------------------------------------------------------------
def parse_iso(VALUE: str) -> datetime:
    try:
        return date_parser.isoparse(VALUE)
    except (TypeError, ValueError, OverflowError):
        return datetime(1970, 1, 1, tzinfo=timezone.utc)


# ------------------------------------------------------------------------------
# This function calculates remaining whole days before reauthentication.
#
# 1. "LAST_AUTH_UTC" is stored offset-aware auth timestamp.
# 2. "INTERVAL_DAYS" is the reauthentication interval in days.
#
# Returns: Remaining whole days before reauthentication should complete.
# ------------------------------------------------------------------------------
def get_reauth_days_left(LAST_AUTH_UTC: str, INTERVAL_DAYS: int) -> int:
    LAST_AUTH = parse_iso(LAST_AUTH_UTC)
    ELAPSED = now_local() - LAST_AUTH
    ELAPSED_DAYS = max(int(ELAPSED.total_seconds() // 86400), 0)
    return INTERVAL_DAYS - ELAPSED_DAYS


# ------------------------------------------------------------------------------
# This function executes authentication and persists updated auth state.
#
# 1. "CLIENT" is iCloud client wrapper.
# 2. "AUTH_STATE" is current auth state.
# 3. "AUTH_STATE_PATH" is auth state file path.
# 4. "NOTIFY_MESSAGE" emits outgoing Telegram content.
# 5. "USERNAME" is command prefix used by Telegram control.
# 6. "APPLE_ID" is the configured Apple account identifier.
# 7. "PROVIDED_CODE" is optional MFA code.
#
# Returns: Tuple "(new_state, is_authenticated, details_message)".
# ------------------------------------------------------------------------------
def attempt_auth(
    CLIENT: ICloudDriveClient,
    AUTH_STATE: AuthState,
    AUTH_STATE_PATH: Path,
    NOTIFY_MESSAGE: Callable[[str], None],
    USERNAME: str,
    APPLE_ID: str,
    PROVIDED_CODE: str,
    ) -> tuple[AuthState, bool, str]:
    PERSISTENCE_WARNING = " Auth state persistence failed."
    CODE = PROVIDED_CODE.strip()
    APPLE_ID_LABEL = format_apple_id_label(APPLE_ID)

    if CODE:
        IS_SUCCESS, DETAILS = CLIENT.complete_authentication(CODE)
    else:
        IS_SUCCESS, DETAILS = CLIENT.start_authentication()

    if IS_SUCCESS:
        NEW_STATE = AuthState(
            last_auth_utc=now_iso(),
            auth_pending=False,
            reauth_pending=False,
            reminder_stage="none",
        )
        if not save_auth_state(AUTH_STATE_PATH, NEW_STATE):
            DETAILS = f"{DETAILS}{PERSISTENCE_WARNING}"
        NOTIFY_MESSAGE(build_auth_complete_message(APPLE_ID_LABEL, DETAILS))
        return NEW_STATE, True, DETAILS

    if "Two-factor code is required" in DETAILS:
        NEW_STATE = replace(AUTH_STATE, auth_pending=True)
        if not save_auth_state(AUTH_STATE_PATH, NEW_STATE):
            DETAILS = f"{DETAILS}{PERSISTENCE_WARNING}"
        NOTIFY_MESSAGE(build_auth_required_message(USERNAME, APPLE_ID_LABEL))
        return NEW_STATE, False, DETAILS

    NEW_STATE = replace(
        AUTH_STATE,
        auth_pending=AUTH_STATE.auth_pending if CODE else False,
    )
    if not save_auth_state(AUTH_STATE_PATH, NEW_STATE):
        DETAILS = f"{DETAILS}{PERSISTENCE_WARNING}"
    NOTIFY_MESSAGE(build_auth_failed_message(APPLE_ID_LABEL, DETAILS))
    return NEW_STATE, False, DETAILS


# ------------------------------------------------------------------------------
# This function applies 5-day and 2-day reauthentication reminder stages.
#
# 1. "AUTH_STATE" is current auth state.
# 2. "AUTH_STATE_PATH" is persistence file path.
# 3. "NOTIFY_MESSAGE" emits outgoing Telegram content.
# 4. "USERNAME" is Telegram command prefix.
# 5. "INTERVAL_DAYS" is reauthentication interval in days.
#
# Returns: Updated authentication state.
# ------------------------------------------------------------------------------
def process_reauth_reminders(
    AUTH_STATE: AuthState,
    AUTH_STATE_PATH: Path,
    NOTIFY_MESSAGE: Callable[[str], None],
    USERNAME: str,
    INTERVAL_DAYS: int,
) -> AuthState:
    DAYS_LEFT = get_reauth_days_left(AUTH_STATE.last_auth_utc, INTERVAL_DAYS)

    if DAYS_LEFT > 5:
        if AUTH_STATE.reminder_stage == "none" and not AUTH_STATE.reauth_pending:
            return AUTH_STATE

        NEW_STATE = replace(AUTH_STATE, reminder_stage="none", reauth_pending=False)
        save_auth_state(AUTH_STATE_PATH, NEW_STATE)
        return NEW_STATE

    if DAYS_LEFT <= 2 and AUTH_STATE.reminder_stage != "prompt2":
        NOTIFY_MESSAGE(build_reauth_due_message(USERNAME))
        NEW_STATE = replace(AUTH_STATE, reminder_stage="prompt2", reauth_pending=True)
        save_auth_state(AUTH_STATE_PATH, NEW_STATE)
        return NEW_STATE

    if DAYS_LEFT <= 5 and AUTH_STATE.reminder_stage == "none":
        NOTIFY_MESSAGE(build_reauth_reminder_message())
        NEW_STATE = replace(AUTH_STATE, reminder_stage="alert5")
        save_auth_state(AUTH_STATE_PATH, NEW_STATE)
        return NEW_STATE

    return AUTH_STATE
