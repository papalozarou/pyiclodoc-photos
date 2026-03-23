# ------------------------------------------------------------------------------
# This module manages persisted runtime state such as manifests and
# authentication metadata.
# ------------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
from pathlib import Path
import json
from typing import Any

from dateutil import parser as date_parser

from app.logger import get_timestamp
from app.time_utils import now_local_iso


DEFAULT_AUTH_TIME = "1970-01-01T00:00:00+00:00"
VALID_REMINDER_STAGES = {"none", "alert5", "prompt2"}


# ------------------------------------------------------------------------------
# This data class stores authentication timestamp and pending auth flags.
# ------------------------------------------------------------------------------
@dataclass(frozen=True)
class AuthState:
    last_auth_utc: str
    auth_pending: bool
    reauth_pending: bool
    reminder_stage: str
    last_reminder_utc: str = ""
    manual_reauth_pending: bool = False


# ------------------------------------------------------------------------------
# This function loads JSON content from disk with empty defaults.
#
# 1. "PATH" is the JSON file path to read.
#
# Returns: Parsed dictionary payload, or an empty dictionary when absent.
# ------------------------------------------------------------------------------
def read_json(PATH: Path) -> Any:
    if not PATH.exists():
        return {}

    try:
        with PATH.open("r", encoding="utf-8") as HANDLE:
            return json.load(HANDLE)
    except json.JSONDecodeError as ERROR:
        warn_state_issue(
            f"Corrupt JSON state ignored at {PATH}: "
            f"{type(ERROR).__name__}: {ERROR}",
        )
        quarantine_corrupt_json(PATH)
        return {}
    except OSError as ERROR:
        warn_state_issue(
            f"State read failed at {PATH}: {type(ERROR).__name__}: {ERROR}",
        )
        return {}


# ------------------------------------------------------------------------------
# This function emits a state-layer warning to worker stdout.
#
# 1. "MESSAGE" is warning content to print.
#
# Returns: None.
# ------------------------------------------------------------------------------
def warn_state_issue(MESSAGE: str) -> None:
    print(f"[{get_timestamp()}] [ERROR] {MESSAGE}", flush=True)


# ------------------------------------------------------------------------------
# This function quarantines a corrupt JSON file to stop repeated parse failures.
#
# 1. "PATH" is the invalid JSON file path.
#
# Returns: None.
#
# N.B.
# The corrupt file is renamed rather than deleted so the operator can still
# inspect what went wrong after the worker has recovered.
# ------------------------------------------------------------------------------
def quarantine_corrupt_json(PATH: Path) -> None:
    QUARANTINE_PATH = PATH.with_suffix(f"{PATH.suffix}.corrupt")

    try:
        if QUARANTINE_PATH.exists():
            QUARANTINE_PATH.unlink()
    except OSError:
        return

    try:
        PATH.replace(QUARANTINE_PATH)
    except OSError as ERROR:
        warn_state_issue(
            f"Failed to quarantine corrupt JSON state at {PATH}: "
            f"{type(ERROR).__name__}: {ERROR}",
        )


# ------------------------------------------------------------------------------
# This function writes JSON content atomically with a temp file.
#
# 1. "PATH" is the destination JSON file.
# 2. "PAYLOAD" is the dictionary to persist.
#
# Returns: True on success, otherwise False.
#
# N.B.
# The worker writes to a sibling temporary file first, then replaces the final
# file path so interrupted writes do not leave half-written state behind.
# ------------------------------------------------------------------------------
def write_json(PATH: Path, PAYLOAD: dict[str, Any]) -> bool:
    TEMPORARY_PATH = PATH.with_suffix(PATH.suffix + ".tmp")

    try:
        with TEMPORARY_PATH.open("w", encoding="utf-8") as HANDLE:
            json.dump(PAYLOAD, HANDLE, indent=2, sort_keys=True)

        TEMPORARY_PATH.replace(PATH)
        return True
    except OSError as ERROR:
        warn_state_issue(
            f"State write failed at {PATH}: {type(ERROR).__name__}: {ERROR}",
        )
        try:
            TEMPORARY_PATH.unlink()
        except OSError:
            pass
        return False


# ------------------------------------------------------------------------------
# This function returns a configured-timezone ISO-8601 timestamp.
#
# Returns: Offset-aware ISO-8601 timestamp string.
# ------------------------------------------------------------------------------
def now_iso() -> str:
    return now_local_iso()


# ------------------------------------------------------------------------------
# This function returns the default authentication state model.
#
# Returns: Safe default "AuthState" for missing or invalid persisted payloads.
# ------------------------------------------------------------------------------
def default_auth_state() -> AuthState:
    return AuthState(
        last_auth_utc=DEFAULT_AUTH_TIME,
        auth_pending=False,
        reauth_pending=False,
        reminder_stage="none",
        last_reminder_utc="",
        manual_reauth_pending=False,
    )


# ------------------------------------------------------------------------------
# This function validates one persisted boolean auth-state field.
#
# 1. "VALUE" is the raw payload field value.
# 2. "PATH" is the auth-state file path.
# 3. "FIELD_NAME" is the persisted field key.
# 4. "DEFAULT" is the fallback boolean.
#
# Returns: Validated boolean value.
# ------------------------------------------------------------------------------
def validate_auth_state_bool(
    VALUE: Any,
    PATH: Path,
    FIELD_NAME: str,
    DEFAULT: bool,
) -> bool:
    if VALUE is None:
        return DEFAULT

    if isinstance(VALUE, bool):
        return VALUE

    warn_state_issue(
        f'Invalid auth state field "{FIELD_NAME}" at {PATH}: '
        f"expected boolean, using default {DEFAULT}.",
    )
    return DEFAULT


# ------------------------------------------------------------------------------
# This function validates one persisted reminder-stage value.
#
# 1. "VALUE" is the raw payload field value.
# 2. "PATH" is the auth-state file path.
#
# Returns: Validated reminder-stage string.
# ------------------------------------------------------------------------------
def validate_reminder_stage(VALUE: Any, PATH: Path) -> str:
    if VALUE is None:
        return "none"

    if isinstance(VALUE, str) and VALUE in VALID_REMINDER_STAGES:
        return VALUE

    warn_state_issue(
        f'Invalid auth state field "reminder_stage" at {PATH}: '
        'expected one of "none", "alert5", or "prompt2". Using default "none".',
    )
    return "none"


# ------------------------------------------------------------------------------
# This function normalizes one persisted auth-state timestamp.
#
# 1. "VALUE" is the raw payload field value.
# 2. "PATH" is the auth-state file path.
# 3. "FIELD_NAME" is the persisted field key.
# 4. "DEFAULT" is the fallback ISO-8601 timestamp or empty string.
# 5. "ALLOW_EMPTY" allows an empty string to remain empty.
#
# Returns: Offset-aware ISO-8601 string or the configured default.
# ------------------------------------------------------------------------------
def normalize_auth_state_timestamp(
    VALUE: Any,
    PATH: Path,
    FIELD_NAME: str,
    DEFAULT: str,
    ALLOW_EMPTY: bool = False,
) -> str:
    if VALUE is None:
        return DEFAULT

    if not isinstance(VALUE, str):
        warn_state_issue(
            f'Invalid auth state field "{FIELD_NAME}" at {PATH}: '
            f"expected string timestamp, using default {DEFAULT or '<empty>'}.",
        )
        return DEFAULT

    TIMESTAMP_TEXT = VALUE.strip()

    if not TIMESTAMP_TEXT:
        if ALLOW_EMPTY:
            return ""

        warn_state_issue(
            f'Invalid auth state field "{FIELD_NAME}" at {PATH}: '
            f"empty timestamp, using default {DEFAULT}.",
        )
        return DEFAULT

    try:
        PARSED = date_parser.isoparse(TIMESTAMP_TEXT)
    except (TypeError, ValueError, OverflowError):
        warn_state_issue(
            f'Invalid auth state field "{FIELD_NAME}" at {PATH}: '
            f"{TIMESTAMP_TEXT!r}. Using default {DEFAULT or '<empty>'}.",
        )
        return DEFAULT

    if PARSED.tzinfo is None or PARSED.utcoffset() is None:
        warn_state_issue(
            f'Normalized auth state field "{FIELD_NAME}" at {PATH}: '
            "timestamp had no timezone offset, assuming UTC.",
        )
        PARSED = PARSED.replace(tzinfo=timezone.utc)

    return PARSED.isoformat()


# ------------------------------------------------------------------------------
# This function persists one auth-state transition before it becomes live.
#
# 1. "PATH" is the auth-state file path.
# 2. "CURRENT_STATE" is the current in-memory state.
# 3. "NEXT_STATE" is the proposed next state.
#
# Returns: Tuple "(state, persisted)" with the safe live state.
# ------------------------------------------------------------------------------
def persist_auth_state_transition(
    PATH: Path,
    CURRENT_STATE: AuthState,
    NEXT_STATE: AuthState,
) -> tuple[AuthState, bool]:
    if not save_auth_state(PATH, NEXT_STATE):
        return CURRENT_STATE, False

    return NEXT_STATE, True


# ------------------------------------------------------------------------------
# This function loads persisted authentication state with robust defaults.
#
# 1. "PATH" is the JSON state file location.
#
# Returns: "AuthState" with default values when fields are missing.
#
# N.B.
# Missing files and partial payloads are normal during first boot and after
# manual cleanup, so this function must remain tolerant.
# ------------------------------------------------------------------------------
def load_auth_state(PATH: Path) -> AuthState:
    PAYLOAD = read_json(PATH)

    if not isinstance(PAYLOAD, dict):
        warn_state_issue(
            f"Invalid auth state ignored at {PATH}: expected JSON object.",
        )
        return default_auth_state()

    return AuthState(
        last_auth_utc=normalize_auth_state_timestamp(
            PAYLOAD.get("last_auth_utc"),
            PATH,
            "last_auth_utc",
            DEFAULT_AUTH_TIME,
        ),
        auth_pending=validate_auth_state_bool(
            PAYLOAD.get("auth_pending"),
            PATH,
            "auth_pending",
            False,
        ),
        reauth_pending=validate_auth_state_bool(
            PAYLOAD.get("reauth_pending"),
            PATH,
            "reauth_pending",
            False,
        ),
        reminder_stage=validate_reminder_stage(PAYLOAD.get("reminder_stage"), PATH),
        last_reminder_utc=normalize_auth_state_timestamp(
            PAYLOAD.get("last_reminder_utc"),
            PATH,
            "last_reminder_utc",
            "",
            ALLOW_EMPTY=True,
        ),
        manual_reauth_pending=validate_auth_state_bool(
            PAYLOAD.get("manual_reauth_pending"),
            PATH,
            "manual_reauth_pending",
            False,
        ),
    )


# ------------------------------------------------------------------------------
# This function persists authentication state to disk.
#
# 1. "PATH" is the JSON state file location.
# 2. "STATE" is the model to persist.
#
# Returns: True on success, otherwise False.
# ------------------------------------------------------------------------------
def save_auth_state(PATH: Path, STATE: AuthState) -> bool:
    PAYLOAD = {
        "last_auth_utc": STATE.last_auth_utc,
        "auth_pending": STATE.auth_pending,
        "reauth_pending": STATE.reauth_pending,
        "reminder_stage": STATE.reminder_stage,
        "last_reminder_utc": STATE.last_reminder_utc,
        "manual_reauth_pending": STATE.manual_reauth_pending,
    }
    return write_json(PATH, PAYLOAD)


# ------------------------------------------------------------------------------
# This function loads a manifest that tracks remote file metadata by path.
#
# 1. "PATH" is the manifest file location.
#
# Returns: Mapping keyed by remote path for incremental diff checks.
#
# N.B.
# Non-dictionary payloads are treated as invalid state and collapsed to an
# empty manifest so the worker can rebuild safely on the next sync run.
# ------------------------------------------------------------------------------
def load_manifest(PATH: Path) -> dict[str, dict[str, Any]]:
    PAYLOAD = read_json(PATH)

    if not isinstance(PAYLOAD, dict):
        warn_state_issue(
            f"Invalid manifest state ignored at {PATH}: expected JSON object.",
        )
        return {}

    return {
        str(KEY): VALUE for KEY, VALUE in PAYLOAD.items() if isinstance(VALUE, dict)
    }


# ------------------------------------------------------------------------------
# This function saves the manifest in stable ordering.
#
# 1. "PATH" is the manifest file location.
# 2. "MANIFEST" is the payload to persist.
#
# Returns: True on success, otherwise False.
# ------------------------------------------------------------------------------
def save_manifest(PATH: Path, MANIFEST: dict[str, dict[str, Any]]) -> bool:
    return write_json(PATH, MANIFEST)
