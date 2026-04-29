# ------------------------------------------------------------------------------
# This module handles Telegram command intake and command-side state changes.
#
# Command polling and command execution are kept separate from the main runtime
# loop so the worker coordinator does not also need to own Telegram-specific
# control flow.
# ------------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Callable

from app.config import AppConfig
from app.logger import log_line
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
# 4. "LOG_FILE" is optional worker log destination.
#
# Returns: Tuple "(commands, next_offset)" for command execution.
# ------------------------------------------------------------------------------
def process_commands(
    TELEGRAM: TelegramConfig,
    USERNAME: str,
    UPDATE_OFFSET: int | None,
    LOG_FILE: Path | None = None,
) -> tuple[list[tuple[str, str]], int | None]:
    UPDATES = fetch_updates(TELEGRAM, UPDATE_OFFSET, LOG_FILE=LOG_FILE)

    if not UPDATES:
        if LOG_FILE is not None:
            log_line(
                LOG_FILE,
                "debug",
                f"Telegram command poll finished. accepted=0, next_offset={UPDATE_OFFSET}",
            )
        return [], UPDATE_OFFSET

    COMMANDS: list[tuple[str, str]] = []
    MAX_UPDATE = UPDATE_OFFSET or 0

    for UPDATE in UPDATES:
        EVENT = parse_command(UPDATE, USERNAME, TELEGRAM.chat_id)
        UPDATE_ID = int(UPDATE.get("update_id", 0))
        MAX_UPDATE = max(MAX_UPDATE, UPDATE_ID + 1)

        if EVENT is None:
            continue

        if LOG_FILE is not None:
            log_line(
                LOG_FILE,
                "debug",
                "Telegram command accepted. "
                f"command={EVENT.command}, has_args={bool(EVENT.args)}, "
                f"update_id={EVENT.update_id}",
            )
        COMMANDS.append((EVENT.command, EVENT.args))

    if LOG_FILE is not None:
        log_line(
            LOG_FILE,
            "debug",
            "Telegram command poll finished. "
            f"updates={len(UPDATES)}, accepted={len(COMMANDS)}, "
            f"next_offset={MAX_UPDATE}",
        )
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
# 8. "DEBUG_LOGGER" emits non-secret command trace lines when supplied.
#
# Returns: "CommandOutcome" with updated state and backup intent.
#
# N.B.
# Command arguments are not logged because auth and reauth arguments may contain
# a one-time Apple verification code.
# ------------------------------------------------------------------------------
def handle_command(
    COMMAND: str,
    ARGS: str,
    CONFIG: AppConfig,
    AUTH_STATE: AuthState,
    IS_AUTHENTICATED: bool,
    MESSAGE_SENDER: Callable[[str], None],
    AUTH_EXECUTOR: Callable[[AuthState, str], tuple[AuthState, bool, str]],
    DEBUG_LOGGER: Callable[[str], None] | None = None,
) -> CommandOutcome:
    if DEBUG_LOGGER is not None:
        DEBUG_LOGGER(
            "Telegram command handling started. "
            f"command={COMMAND}, has_args={bool(ARGS)}, "
            f"is_authenticated={IS_AUTHENTICATED}, "
            f"auth_pending={AUTH_STATE.auth_pending}, "
            f"reauth_pending={AUTH_STATE.reauth_pending}",
        )

    if COMMAND == "backup":
        MESSAGE_SENDER(build_backup_requested_message(CONFIG.icloud_email))
        if DEBUG_LOGGER is not None:
            DEBUG_LOGGER("Telegram backup command queued a manual backup.")
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
        if DEBUG_LOGGER is not None:
            DEBUG_LOGGER(f"Telegram auth prompt command persisted={PERSISTED}.")
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
        if DEBUG_LOGGER is not None:
            DEBUG_LOGGER(f"Telegram reauth prompt command persisted={PERSISTED}.")
        return CommandOutcome(NEW_STATE, IS_AUTHENTICATED, False, DETAILS)

    if DEBUG_LOGGER is not None:
        DEBUG_LOGGER(
            "Telegram auth command delegated to auth executor. "
            f"command={COMMAND}, has_code={bool(ARGS)}",
        )
    NEW_STATE, NEW_AUTH, DETAILS = AUTH_EXECUTOR(AUTH_STATE, ARGS)
    if DEBUG_LOGGER is not None:
        DEBUG_LOGGER(
            "Telegram auth command finished. "
            f"is_authenticated={NEW_AUTH}, "
            f"auth_pending={NEW_STATE.auth_pending}, "
            f"reauth_pending={NEW_STATE.reauth_pending}",
        )
    return CommandOutcome(NEW_STATE, NEW_AUTH, False, DETAILS)
