# ------------------------------------------------------------------------------
# This module provides lightweight structured logging helpers for console and
# file output.
# ------------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import gzip
import os
import shutil
from typing import Optional

from app.time_utils import now_local

LOG_LEVELS = {
    "debug": 10,
    "info": 20,
    "error": 30,
}
ANSI_RED = "\033[31m"
ANSI_RESET = "\033[0m"
ROTATED_FILE_PATTERN = "{name}.{stamp}.log"
ROTATION_CHECK_INTERVAL_SECONDS = 1


# ------------------------------------------------------------------------------
# This data class stores parsed logger settings derived from the environment.
# ------------------------------------------------------------------------------
@dataclass(frozen=True)
class LoggerSettings:
    log_level: str
    rotate_max_bytes: int
    rotate_daily: bool
    rotate_keep_days: int


# ------------------------------------------------------------------------------
# This data class stores lightweight per-file rotation check state.
# ------------------------------------------------------------------------------
@dataclass
class RotationState:
    last_checked_epoch: int = 0


_CACHED_SETTINGS_SIGNATURE: Optional[tuple[str, str, str, str]] = None
_CACHED_SETTINGS: Optional[LoggerSettings] = None
_ROTATION_STATES: dict[Path, RotationState] = {}


# ------------------------------------------------------------------------------
# This function produces a configured-timezone timestamp string.
#
# Returns: Display timestamp including timezone abbreviation.
# ------------------------------------------------------------------------------
def get_timestamp() -> str:
    return now_local().strftime("%Y-%m-%d %H:%M:%S %Z")


# ------------------------------------------------------------------------------
# This function returns the current logger environment signature.
#
# Returns: Tuple of raw environment values used by logger settings.
# ------------------------------------------------------------------------------
def get_settings_signature() -> tuple[str, str, str, str]:
    return (
        os.getenv("LOG_LEVEL", "info"),
        os.getenv("LOG_ROTATE_MAX_MIB", "100"),
        os.getenv("LOG_ROTATE_DAILY", "true"),
        os.getenv("LOG_ROTATE_KEEP_DAYS", "14"),
    )


# ------------------------------------------------------------------------------
# This function parses the configured log threshold from raw environment text.
#
# 1. "RAW_VALUE" is the unparsed environment value.
#
# Returns: Normalised log level token.
# ------------------------------------------------------------------------------
def parse_log_level(RAW_VALUE: str) -> str:
    CLEAN_VALUE = RAW_VALUE.strip().lower()

    if CLEAN_VALUE in LOG_LEVELS:
        return CLEAN_VALUE

    return "info"


# ------------------------------------------------------------------------------
# This function parses the configured maximum log size in bytes.
#
# 1. "RAW_VALUE" is the unparsed environment value.
#
# Returns: Positive byte count, defaulting to 100 MiB.
# ------------------------------------------------------------------------------
def parse_log_rotate_max_bytes(RAW_VALUE: str) -> int:
    DEFAULT_BYTES = 100 * 1024 * 1024
    CLEAN_VALUE = RAW_VALUE.strip()

    if not CLEAN_VALUE.isdigit():
        return DEFAULT_BYTES

    VALUE_MIB = int(CLEAN_VALUE)
    if VALUE_MIB < 1:
        return DEFAULT_BYTES

    return VALUE_MIB * 1024 * 1024


# ------------------------------------------------------------------------------
# This function parses the configured daily rollover toggle.
#
# 1. "RAW_VALUE" is the unparsed environment value.
#
# Returns: True when daily rollover is enabled, defaulting to true.
# ------------------------------------------------------------------------------
def parse_log_rotate_daily(RAW_VALUE: str) -> bool:
    CLEAN_VALUE = RAW_VALUE.strip().lower()

    if CLEAN_VALUE in {"1", "true", "yes", "on"}:
        return True

    if CLEAN_VALUE in {"0", "false", "no", "off"}:
        return False

    return True


# ------------------------------------------------------------------------------
# This function parses the rotated-log retention period in days.
#
# 1. "RAW_VALUE" is the unparsed environment value.
#
# Returns: Positive day count, defaulting to 14 days.
# ------------------------------------------------------------------------------
def parse_log_rotate_keep_days(RAW_VALUE: str) -> int:
    CLEAN_VALUE = RAW_VALUE.strip()

    if not CLEAN_VALUE.isdigit():
        return 14

    VALUE = int(CLEAN_VALUE)
    if VALUE < 1:
        return 14

    return VALUE


# ------------------------------------------------------------------------------
# This function returns cached logger settings with env-change invalidation.
#
# Returns: Parsed logger settings for the current process state.
# ------------------------------------------------------------------------------
def get_logger_settings() -> LoggerSettings:
    global _CACHED_SETTINGS_SIGNATURE
    global _CACHED_SETTINGS

    SIGNATURE = get_settings_signature()

    if _CACHED_SETTINGS is not None and _CACHED_SETTINGS_SIGNATURE == SIGNATURE:
        return _CACHED_SETTINGS

    SETTINGS = LoggerSettings(
        log_level=parse_log_level(SIGNATURE[0]),
        rotate_max_bytes=parse_log_rotate_max_bytes(SIGNATURE[1]),
        rotate_daily=parse_log_rotate_daily(SIGNATURE[2]),
        rotate_keep_days=parse_log_rotate_keep_days(SIGNATURE[3]),
    )
    _CACHED_SETTINGS_SIGNATURE = SIGNATURE
    _CACHED_SETTINGS = SETTINGS
    return SETTINGS


# ------------------------------------------------------------------------------
# This function resets cached logger settings and rotation state.
#
# Returns: None.
#
# N.B.
# This exists mainly for tests and controlled runtime reconfiguration.
# ------------------------------------------------------------------------------
def reset_logger_state() -> None:
    global _CACHED_SETTINGS_SIGNATURE
    global _CACHED_SETTINGS

    _CACHED_SETTINGS_SIGNATURE = None
    _CACHED_SETTINGS = None
    _ROTATION_STATES.clear()


# ------------------------------------------------------------------------------
# This function checks whether a log line should be emitted.
#
# 1. "LEVEL" is message severity token.
#
# Returns: True when line should be written and printed.
# ------------------------------------------------------------------------------
def should_log(LEVEL: str) -> bool:
    CURRENT_WEIGHT = LOG_LEVELS.get(get_logger_settings().log_level, LOG_LEVELS["info"])
    MESSAGE_WEIGHT = LOG_LEVELS.get(LEVEL.lower(), LOG_LEVELS["info"])
    return MESSAGE_WEIGHT >= CURRENT_WEIGHT


# ------------------------------------------------------------------------------
# This function prints a log line and appends it to the worker log.
#
# 1. "LOG_FILE" is the destination log file.
# 2. "LEVEL" is severity.
# 3. "MESSAGE" is log content.
#
# Returns: None.
# ------------------------------------------------------------------------------
def log_line(LOG_FILE: Path, LEVEL: str, MESSAGE: str) -> None:
    if not should_log(LEVEL):
        return

    rotate_log_if_needed(LOG_FILE)

    LEVEL_UPPER = LEVEL.upper()
    LINE = f"[{get_timestamp()}] [{LEVEL_UPPER}] {MESSAGE}"
    CONSOLE_LINE = format_console_line(LINE, LEVEL_UPPER)
    print(CONSOLE_LINE, flush=True)

    with LOG_FILE.open("a", encoding="utf-8") as HANDLE:
        HANDLE.write(f"{LINE}\n")


# ------------------------------------------------------------------------------
# This function applies console-only formatting for selected log levels.
#
# 1. "LINE" is the plain log line.
# 2. "LEVEL_UPPER" is uppercase severity token.
#
# Returns: Console display string.
# ------------------------------------------------------------------------------
def format_console_line(LINE: str, LEVEL_UPPER: str) -> str:
    if LEVEL_UPPER != "ERROR":
        return LINE

    return f"{ANSI_RED}{LINE}{ANSI_RESET}"


# ------------------------------------------------------------------------------
# This function rotates and prunes worker logs based on configured policy.
#
# 1. "LOG_FILE" is the destination log file.
#
# Returns: None.
# ------------------------------------------------------------------------------
def rotate_log_if_needed(LOG_FILE: Path) -> None:
    if not LOG_FILE.exists():
        return

    if not should_check_rotation(LOG_FILE):
        return

    SETTINGS = get_logger_settings()
    SHOULD_ROTATE = should_rotate_for_size(LOG_FILE, SETTINGS) or should_rotate_for_daily_rollover(
        LOG_FILE,
        SETTINGS,
    )
    if not SHOULD_ROTATE:
        return

    rotate_log_file(LOG_FILE)
    prune_rotated_logs(LOG_FILE, SETTINGS)


# ------------------------------------------------------------------------------
# This function checks whether rotation state should be re-evaluated now.
#
# 1. "LOG_FILE" is the destination log file.
#
# Returns: True when rotation checks should run for this write.
# ------------------------------------------------------------------------------
def should_check_rotation(LOG_FILE: Path) -> bool:
    NOW_EPOCH = int(now_local().timestamp())
    STATE = _ROTATION_STATES.setdefault(LOG_FILE, RotationState())

    if NOW_EPOCH - STATE.last_checked_epoch < ROTATION_CHECK_INTERVAL_SECONDS:
        return False

    STATE.last_checked_epoch = NOW_EPOCH
    return True


# ------------------------------------------------------------------------------
# This function checks size-based log rotation trigger.
#
# 1. "LOG_FILE" is the destination log file.
# 2. "SETTINGS" is the current parsed logger settings.
#
# Returns: True when file size meets or exceeds configured threshold.
# ------------------------------------------------------------------------------
def should_rotate_for_size(LOG_FILE: Path, SETTINGS: LoggerSettings) -> bool:
    if SETTINGS.rotate_max_bytes < 1:
        return False

    try:
        return LOG_FILE.stat().st_size >= SETTINGS.rotate_max_bytes
    except OSError:
        return False


# ------------------------------------------------------------------------------
# This function checks date-based daily rollover trigger.
#
# 1. "LOG_FILE" is the destination log file.
# 2. "SETTINGS" is the current parsed logger settings.
#
# Returns: True when file has entries from a previous local date.
# ------------------------------------------------------------------------------
def should_rotate_for_daily_rollover(LOG_FILE: Path, SETTINGS: LoggerSettings) -> bool:
    if not SETTINGS.rotate_daily:
        return False

    try:
        MODIFIED_EPOCH = LOG_FILE.stat().st_mtime
    except OSError:
        return False

    FILE_DATE = datetime.fromtimestamp(MODIFIED_EPOCH, tz=now_local().tzinfo).date()
    NOW_DATE = now_local().date()
    return FILE_DATE != NOW_DATE


# ------------------------------------------------------------------------------
# This function rotates the active log file into a compressed archive.
#
# 1. "LOG_FILE" is the destination log file.
#
# Returns: None.
# ------------------------------------------------------------------------------
def rotate_log_file(LOG_FILE: Path) -> None:
    STAMP = now_local().strftime("%Y%m%d-%H%M%S")
    ROTATED_NAME = ROTATED_FILE_PATTERN.format(name=LOG_FILE.stem, stamp=STAMP)
    ROTATED_PATH = LOG_FILE.with_name(ROTATED_NAME)
    COMPRESSED_PATH = ROTATED_PATH.with_suffix(f"{ROTATED_PATH.suffix}.gz")

    try:
        LOG_FILE.replace(ROTATED_PATH)
    except OSError:
        return

    try:
        with ROTATED_PATH.open("rb") as SOURCE:
            with gzip.open(COMPRESSED_PATH, "wb") as TARGET:
                shutil.copyfileobj(SOURCE, TARGET)
    except OSError:
        return

    try:
        ROTATED_PATH.unlink()
    except OSError:
        return


# ------------------------------------------------------------------------------
# This function removes old rotated log archives by retention age.
#
# 1. "LOG_FILE" is the destination log file.
# 2. "SETTINGS" is the current parsed logger settings.
#
# Returns: None.
# ------------------------------------------------------------------------------
def prune_rotated_logs(LOG_FILE: Path, SETTINGS: LoggerSettings) -> None:
    if SETTINGS.rotate_keep_days < 1:
        return

    CUTOFF = now_local() - timedelta(days=SETTINGS.rotate_keep_days)
    PATTERN = f"{LOG_FILE.stem}.*.log.gz"

    for PATH in LOG_FILE.parent.glob(PATTERN):
        try:
            MODIFIED_DT = datetime.fromtimestamp(PATH.stat().st_mtime, tz=now_local().tzinfo)
        except OSError:
            continue

        if MODIFIED_DT >= CUTOFF:
            continue

        try:
            PATH.unlink()
        except OSError:
            continue
