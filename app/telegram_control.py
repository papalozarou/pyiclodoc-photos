# ------------------------------------------------------------------------------
# This module handles Telegram command intake and command-side state changes.
#
# Command polling and command execution are kept separate from the main runtime
# loop so the worker coordinator does not also need to own Telegram-specific
# control flow.
# ------------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Callable

from app.config import AppConfig
from app.state import AuthState, now_iso, persist_auth_state_transition
from app.telegram_bot import TelegramConfig, fetch_updates, parse_command
from app.telegram_messages import (
    build_auth_state_persistence_failed_message,
    build_auth_required_message,
    build_backup_requested_message,
    build_manual_reauth_message,
)


# ------------------------------------------------------------------------------
# This data class captures the result of handling one Telegram command.
# ------------------------------------------------------------------------------
@dataclass(frozen=True)
class CommandOutcome:
    auth_state: AuthState
    is_authenticated: bool
    backup_requested: bool
    details: str = ""


# ------------------------------------------------------------------------------
# This function polls Telegram and returns parsed command intents.
#
# 1. "TELEGRAM" is Telegram configuration.
# 2. "USERNAME" is command prefix.
# 3. "UPDATE_OFFSET" is update offset cursor.
#
# Returns: Tuple "(commands, next_offset)" for command execution.
# ------------------------------------------------------------------------------
def process_commands(
    TELEGRAM: TelegramConfig,
    USERNAME: str,
    UPDATE_OFFSET: int | None,
) -> tuple[list[tuple[str, str]], int | None]:
    UPDATES = fetch_updates(TELEGRAM, UPDATE_OFFSET)

    if not UPDATES:
        return [], UPDATE_OFFSET

    COMMANDS: list[tuple[str, str]] = []
    MAX_UPDATE = UPDATE_OFFSET or 0

    for UPDATE in UPDATES:
        EVENT = parse_command(UPDATE, USERNAME, TELEGRAM.chat_id)
        UPDATE_ID = int(UPDATE.get("update_id", 0))
        MAX_UPDATE = max(MAX_UPDATE, UPDATE_ID + 1)

        if EVENT is None:
            continue

        COMMANDS.append((EVENT.command, EVENT.args))

    return COMMANDS, MAX_UPDATE


# ------------------------------------------------------------------------------
# This function handles a single Telegram command.
#
# 1. "COMMAND" is parsed command keyword.
# 2. "ARGS" is optional command payload.
# 3. "CONFIG" is runtime configuration.
# 4. "AUTH_STATE" is current auth state.
# 5. "IS_AUTHENTICATED" tracks current auth validity.
# 6. "MESSAGE_SENDER" sends one formatted Telegram message string.
# 7. "AUTH_EXECUTOR" performs auth or reauth using an optional code.
#
# Returns: "CommandOutcome" with updated state and backup intent.
# ------------------------------------------------------------------------------
def handle_command(
    COMMAND: str,
    ARGS: str,
    CONFIG: AppConfig,
    AUTH_STATE: AuthState,
    IS_AUTHENTICATED: bool,
    MESSAGE_SENDER: Callable[[str], None],
    AUTH_EXECUTOR: Callable[[AuthState, str], tuple[AuthState, bool, str]],
) -> CommandOutcome:
    if COMMAND == "backup":
        MESSAGE_SENDER(build_backup_requested_message(CONFIG.icloud_email))
        return CommandOutcome(AUTH_STATE, IS_AUTHENTICATED, True)

    if COMMAND == "auth" and not ARGS:
        PROPOSED_STATE = replace(AUTH_STATE, auth_pending=True)
        NEW_STATE, PERSISTED = persist_auth_state_transition(
            CONFIG.auth_state_path,
            AUTH_STATE,
            PROPOSED_STATE,
        )
        DETAILS = "" if PERSISTED else "Auth state persistence failed."
        if PERSISTED:
            MESSAGE_SENDER(
                build_auth_required_message(
                    CONFIG.container_username,
                    CONFIG.icloud_email,
                ),
            )
        else:
            MESSAGE_SENDER(build_auth_state_persistence_failed_message("auth"))
        return CommandOutcome(NEW_STATE, IS_AUTHENTICATED, False, DETAILS)

    if COMMAND == "reauth" and not ARGS:
        PROPOSED_STATE = replace(
            AUTH_STATE,
            reauth_pending=True,
            reminder_stage=AUTH_STATE.reminder_stage,
            last_reminder_utc=now_iso(),
            manual_reauth_pending=True,
        )
        NEW_STATE, PERSISTED = persist_auth_state_transition(
            CONFIG.auth_state_path,
            AUTH_STATE,
            PROPOSED_STATE,
        )
        DETAILS = "" if PERSISTED else "Auth state persistence failed."
        if PERSISTED:
            MESSAGE_SENDER(
                build_manual_reauth_message(
                    CONFIG.container_username,
                    CONFIG.icloud_email,
                ),
            )
        else:
            MESSAGE_SENDER(build_auth_state_persistence_failed_message("reauth"))
        return CommandOutcome(NEW_STATE, IS_AUTHENTICATED, False, DETAILS)

    NEW_STATE, NEW_AUTH, DETAILS = AUTH_EXECUTOR(AUTH_STATE, ARGS)
    return CommandOutcome(NEW_STATE, NEW_AUTH, False, DETAILS)
