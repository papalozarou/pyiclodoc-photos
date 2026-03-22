# ------------------------------------------------------------------------------
# This module runs the backup worker loop and coordinates auth and sync.
# ------------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import replace
from importlib import metadata as importlib_metadata
import os
import threading
from typing import TextIO

from app.auth_flow import attempt_auth
from app.config import AppConfig, load_config
from app.credential_store import configure_keyring, load_credentials, save_credentials
from app.heartbeat import start_heartbeat_updater
from app.icloud_client import ICloudDriveClient
from app.logger import log_line
from app.runtime_lock import RuntimeLockError, acquire_runtime_lock, release_runtime_lock
from app.runtime import notify, run_one_shot_runtime, run_persistent_runtime
from app.scheduler import validate_schedule_config
from app.state import load_auth_state
from app.telegram_bot import TelegramConfig
from app.telegram_messages import (
    build_container_started_message,
    build_container_stopped_message,
    format_apple_id_label,
)


# ------------------------------------------------------------------------------
# This function validates required runtime configuration.
#
# 1. "CONFIG" is the loaded runtime configuration model.
#
# Returns: Validation error list; empty list means configuration is usable.
# ------------------------------------------------------------------------------
def validate_config(CONFIG: AppConfig) -> list[str]:
    ERRORS: list[str] = list(CONFIG.config_errors)

    if not CONFIG.icloud_email:
        ERRORS.append("ICLOUD_EMAIL is required.")

    if not CONFIG.icloud_password:
        ERRORS.append("ICLOUD_PASSWORD is required.")

    ERRORS.extend(validate_schedule_config(CONFIG))

    if CONFIG.backup_discovery_mode not in {"full", "until_found"}:
        ERRORS.append("BACKUP_DISCOVERY_MODE must be one of: full, until_found.")

    if (
        CONFIG.backup_discovery_mode == "until_found"
        and CONFIG.backup_until_found_count < 1
    ):
        ERRORS.append(
            "BACKUP_UNTIL_FOUND_COUNT must be at least 1 when "
            "BACKUP_DISCOVERY_MODE is until_found."
        )

    if CONFIG.backup_discovery_mode == "until_found" and CONFIG.backup_delete_removed:
        ERRORS.append(
            "BACKUP_DISCOVERY_MODE=until_found cannot be used when "
            "BACKUP_DELETE_REMOVED=true."
        )

    if CONFIG.backup_discovery_mode == "until_found" and CONFIG.backup_albums_enabled:
        ERRORS.append(
            "BACKUP_DISCOVERY_MODE=until_found cannot be used when "
            "BACKUP_ALBUMS_ENABLED=true."
        )

    if CONFIG.sync_workers < 0 or CONFIG.sync_workers > 16:
        ERRORS.append("SYNC_DOWNLOAD_WORKERS must be auto or an integer between 1 and 16.")

    if CONFIG.download_chunk_mib < 1 or CONFIG.download_chunk_mib > 16:
        ERRORS.append("SYNC_DOWNLOAD_CHUNK_MIB must be an integer between 1 and 16.")

    if CONFIG.backup_album_links_mode not in {"hardlink", "copy"}:
        ERRORS.append("BACKUP_ALBUM_LINKS_MODE must be one of: hardlink, copy.")

    return ERRORS


# ------------------------------------------------------------------------------
# This function returns runtime build metadata for startup diagnostics.
#
# Returns: Mapping with app build ref and pyicloud package version.
# ------------------------------------------------------------------------------
def get_build_detail() -> dict[str, str]:
    APP_BUILD_REF = os.getenv("C_APP_BUILD_REF", "unknown").strip() or "unknown"

    try:
        PYICLOUD_VERSION = importlib_metadata.version("pyicloud")
    except importlib_metadata.PackageNotFoundError:
        PYICLOUD_VERSION = "unknown"

    return {
        "app_build_ref": APP_BUILD_REF,
        "pyicloud_version": PYICLOUD_VERSION,
    }


# ------------------------------------------------------------------------------
# This function is the worker entrypoint used by the container launcher.
#
# Returns: Non-zero on startup validation/runtime failure.
# ------------------------------------------------------------------------------
def main() -> int:
    CONFIG: AppConfig | None = None
    TELEGRAM = TelegramConfig("", "")
    BUILD_DETAIL = get_build_detail()
    HEARTBEAT_STOP_EVENT: threading.Event | None = None
    LOCK_HANDLE: TextIO | None = None
    STOP_STATUS = "Worker process exited."

    try:
        CONFIG = load_config()
        LOG_FILE = CONFIG.logs_dir / "pyiclodoc-photos-worker.log"
        TELEGRAM = TelegramConfig(CONFIG.telegram_bot_token, CONFIG.telegram_chat_id)
        try:
            LOCK_HANDLE = acquire_runtime_lock(
                CONFIG.config_dir,
                CONFIG.container_username,
            )
        except RuntimeLockError as ERROR:
            log_line(LOG_FILE, "error", str(ERROR))
            STOP_STATUS = "Worker startup blocked by active runtime lock."
            return 1

        configure_keyring(CONFIG.config_dir)
        STORED_EMAIL, STORED_PASSWORD = load_credentials(
            CONFIG.keychain_service_name,
            CONFIG.container_username,
        )
        CONFIG = replace(
            CONFIG,
            icloud_email=CONFIG.icloud_email or STORED_EMAIL,
            icloud_password=CONFIG.icloud_password or STORED_PASSWORD,
        )

        ERRORS = validate_config(CONFIG)

        if ERRORS:
            for LINE in ERRORS:
                log_line(LOG_FILE, "error", LINE)

            return 1

        HEARTBEAT_STOP_EVENT = start_heartbeat_updater(CONFIG.heartbeat_path)

        save_credentials(
            CONFIG.keychain_service_name,
            CONFIG.container_username,
            CONFIG.icloud_email,
            CONFIG.icloud_password,
        )
        APPLE_ID_LABEL = format_apple_id_label(CONFIG.icloud_email)
        notify(TELEGRAM, build_container_started_message(APPLE_ID_LABEL))

        CLIENT = ICloudDriveClient(CONFIG)
        AUTH_STATE = load_auth_state(CONFIG.auth_state_path)
        AUTH_STATE, IS_AUTHENTICATED, DETAILS = attempt_auth(
            CLIENT,
            AUTH_STATE,
            CONFIG.auth_state_path,
            lambda MESSAGE: notify(TELEGRAM, MESSAGE),
            CONFIG.container_username,
            CONFIG.icloud_email,
            "",
        )
        log_line(LOG_FILE, "info", DETAILS)
        log_line(
            LOG_FILE,
            "debug",
            "Auth state after startup attempt: "
            f"is_authenticated={IS_AUTHENTICATED}, "
            f"auth_pending={AUTH_STATE.auth_pending}, "
            f"reauth_pending={AUTH_STATE.reauth_pending}",
        )

        if CONFIG.run_once:
            RETURN_CODE, STOP_STATUS = run_one_shot_runtime(
                CONFIG,
                CLIENT,
                AUTH_STATE,
                IS_AUTHENTICATED,
                TELEGRAM,
                LOG_FILE,
                BUILD_DETAIL,
            )
            return RETURN_CODE

        run_persistent_runtime(
            CONFIG,
            CLIENT,
            AUTH_STATE,
            IS_AUTHENTICATED,
            TELEGRAM,
            LOG_FILE,
            BUILD_DETAIL,
        )
    finally:
        if CONFIG is not None:
            notify(
                TELEGRAM,
                build_container_stopped_message(CONFIG.icloud_email, STOP_STATUS),
            )
        if HEARTBEAT_STOP_EVENT is not None:
            HEARTBEAT_STOP_EVENT.set()
        release_runtime_lock(LOCK_HANDLE)


if __name__ == "__main__":
    raise SystemExit(main())
