# ------------------------------------------------------------------------------
# This module handles Telegram Bot API messaging and command polling for
# backup control.
# ------------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from app.logger import log_line


# ------------------------------------------------------------------------------
# This data class defines token and chat settings for Telegram integration.
# ------------------------------------------------------------------------------
@dataclass(frozen=True)
class TelegramConfig:
    bot_token: str
    chat_id: str


# ------------------------------------------------------------------------------
# This data class represents a parsed command accepted from Telegram updates.
# ------------------------------------------------------------------------------
@dataclass(frozen=True)
class CommandEvent:
    command: str
    args: str
    update_id: int


# ------------------------------------------------------------------------------
# This function builds a Bot API endpoint from a token and method name.
#
# 1. "TOKEN" is the bot token.
# 2. "METHOD" is the Bot API method name.
#
# Returns: Fully-qualified HTTPS URL for the selected method.
# ------------------------------------------------------------------------------
def get_endpoint(TOKEN: str, METHOD: str) -> str:
    return f"https://api.telegram.org/bot{TOKEN}/{METHOD}"


# ------------------------------------------------------------------------------
# This function sends a Telegram message and returns success state.
#
# 1. "CONFIG" carries token and chat configuration.
# 2. "TEXT" is message body.
# 3. "TIMEOUT" is request timeout in seconds.
# 4. "LOG_FILE" is optional worker log destination.
#
# Returns: True on successful HTTP/API response, otherwise False.
#
# N.B.
# Missing bot token or chat ID is treated as integration-disabled state rather
# than a fatal error, because Telegram is optional for local development.
#
# Notes: Telegram Bot API reference:
# https://core.telegram.org/bots/api#sendmessage
# ------------------------------------------------------------------------------
def send_message(
    CONFIG: TelegramConfig,
    TEXT: str,
    TIMEOUT: int = 20,
    LOG_FILE: Path | None = None,
) -> bool:
    if not CONFIG.bot_token:
        if LOG_FILE is not None:
            log_line(LOG_FILE, "debug", "Telegram send skipped. reason=missing_bot_token")
        return False

    if not CONFIG.chat_id:
        if LOG_FILE is not None:
            log_line(LOG_FILE, "debug", "Telegram send skipped. reason=missing_chat_id")
        return False

    PAYLOAD = {
        "chat_id": CONFIG.chat_id,
        "text": TEXT,
    }

    try:
        if LOG_FILE is not None:
            log_line(LOG_FILE, "debug", "Telegram send started.")
        RESPONSE = requests.post(
            get_endpoint(CONFIG.bot_token, "sendMessage"),
            json=PAYLOAD,
            timeout=TIMEOUT,
        )
        if LOG_FILE is not None:
            log_line(
                LOG_FILE,
                "debug",
                f"Telegram send finished. ok={RESPONSE.ok}, status_code={RESPONSE.status_code}",
            )
        return RESPONSE.ok
    except requests.RequestException as ERROR:
        if LOG_FILE is not None:
            log_line(
                LOG_FILE,
                "debug",
                f"Telegram send failed. error_type={type(ERROR).__name__}",
            )
        return False


# ------------------------------------------------------------------------------
# This function requests recent updates with optional offset tracking.
#
# 1. "CONFIG" carries token and chat configuration.
# 2. "OFFSET" is update offset.
# 3. "TIMEOUT" is long-poll timeout in seconds.
# 4. "LOG_FILE" is optional worker log destination.
#
# Returns: List of update dictionaries from Telegram, or empty list on errors.
#
# N.B.
# All network and API failures collapse to an empty update list so the command
# poll loop can keep running without forcing a worker restart.
#
# Notes: Telegram Bot API reference:
# https://core.telegram.org/bots/api#getupdates
# ------------------------------------------------------------------------------
def fetch_updates(
    CONFIG: TelegramConfig,
    OFFSET: int | None,
    TIMEOUT: int = 30,
    LOG_FILE: Path | None = None,
) -> list[dict[str, Any]]:
    if not CONFIG.bot_token:
        if LOG_FILE is not None:
            log_line(LOG_FILE, "debug", "Telegram poll skipped. reason=missing_bot_token")
        return []

    PARAMS: dict[str, Any] = {"timeout": TIMEOUT}

    if OFFSET is not None:
        PARAMS["offset"] = OFFSET

    try:
        if LOG_FILE is not None:
            log_line(LOG_FILE, "debug", f"Telegram poll started. offset={OFFSET}")
        RESPONSE = requests.get(
            get_endpoint(CONFIG.bot_token, "getUpdates"),
            params=PARAMS,
            timeout=TIMEOUT + 5,
        )
    except requests.RequestException as ERROR:
        if LOG_FILE is not None:
            log_line(
                LOG_FILE,
                "debug",
                f"Telegram poll failed. error_type={type(ERROR).__name__}",
            )
        return []

    if not RESPONSE.ok:
        if LOG_FILE is not None:
            log_line(
                LOG_FILE,
                "debug",
                f"Telegram poll failed. status_code={RESPONSE.status_code}",
            )
        return []

    try:
        PAYLOAD = RESPONSE.json()
    except ValueError:
        if LOG_FILE is not None:
            log_line(LOG_FILE, "debug", "Telegram poll failed. reason=invalid_json")
        return []

    if not PAYLOAD.get("ok"):
        if LOG_FILE is not None:
            log_line(LOG_FILE, "debug", "Telegram poll failed. reason=api_not_ok")
        return []

    RESULT = PAYLOAD.get("result", [])
    if not isinstance(RESULT, list):
        if LOG_FILE is not None:
            log_line(LOG_FILE, "debug", "Telegram poll failed. reason=result_not_list")
        return []

    if LOG_FILE is not None:
        log_line(LOG_FILE, "debug", f"Telegram poll finished. updates={len(RESULT)}")
    return RESULT


# ------------------------------------------------------------------------------
# This function parses a command for a matching username prefix and chat.
#
# 1. "UPDATE" is a Telegram update payload.
# 2. "USERNAME" is command prefix.
# 3. "EXPECTED_CHAT_ID" restricts accepted chats.
#
# Returns: Parsed "CommandEvent" when valid, otherwise None.
#
# N.B.
# Command parsing is intentionally strict. Only the configured chat ID and the
# exact "<username> <command>" prefix are accepted.
#
# Notes: Update payload structure follows Telegram Bot API documentation:
# https://core.telegram.org/bots/api#update
# ------------------------------------------------------------------------------
def parse_command(
    UPDATE: dict[str, Any],
    USERNAME: str,
    EXPECTED_CHAT_ID: str,
) -> CommandEvent | None:
    UPDATE_ID = int(UPDATE.get("update_id", 0))
    MESSAGE = UPDATE.get("message")

    if not isinstance(MESSAGE, dict):
        return None

    CHAT = MESSAGE.get("chat", {})
    CHAT_ID = str(CHAT.get("id", ""))

    if EXPECTED_CHAT_ID and CHAT_ID != EXPECTED_CHAT_ID:
        return None

    TEXT = str(MESSAGE.get("text", "")).strip()

    if not TEXT:
        return None

    PREFIX = f"{USERNAME} "

    if not TEXT.lower().startswith(PREFIX.lower()):
        return None

    BODY = TEXT[len(PREFIX) :].strip()

    if not BODY:
        return None

    PARTS = BODY.split(maxsplit=1)
    COMMAND = PARTS[0].lower()
    ARGS = PARTS[1] if len(PARTS) == 2 else ""

    if COMMAND not in {"backup", "auth", "reauth"}:
        return None

    return CommandEvent(command=COMMAND, args=ARGS, update_id=UPDATE_ID)
