# ------------------------------------------------------------------------------
# This module provides a small keychain wrapper for persistent iCloud
# credential storage.
# ------------------------------------------------------------------------------

from __future__ import annotations

from pathlib import Path
import os

import keyring
from keyrings.alt.file import PlaintextKeyring


# ------------------------------------------------------------------------------
# This function configures a deterministic file-based keyring path.
#
# 1. "CONFIG_DIR" is the root directory used for worker runtime state.
#
# Returns: None.
#
# N.B.
# This worker intentionally uses a file-backed keyring inside mounted storage
# so Apple credentials survive container restarts and reauth windows.
# ------------------------------------------------------------------------------
def configure_keyring(CONFIG_DIR: Path) -> None:
    KEYRING_DIR = CONFIG_DIR / "keyring"
    KEYRING_FILE_PATH = KEYRING_DIR / "keyring_pass.cfg"
    KEYRING_DIR.mkdir(parents=True, exist_ok=True)
    os.environ["PYTHON_KEYRING_FILENAME"] = str(KEYRING_FILE_PATH)
    os.environ["HOME"] = str(CONFIG_DIR)
    os.environ["XDG_DATA_HOME"] = str(CONFIG_DIR / ".local" / "share")

    KEYRING_BACKEND = PlaintextKeyring()
    KEYRING_BACKEND.file_path = str(KEYRING_FILE_PATH)
    keyring.set_keyring(KEYRING_BACKEND)


# ------------------------------------------------------------------------------
# This function reads credentials from keyring storage.
#
# 1. "SERVICE_NAME" scopes credentials for this application.
# 2. "USERNAME" identifies the account key prefix inside the keyring.
#
# Returns: Tuple "(email, password)" with empty-string fallbacks.
# ------------------------------------------------------------------------------
def load_credentials(SERVICE_NAME: str, USERNAME: str) -> tuple[str, str]:
    EMAIL = keyring.get_password(SERVICE_NAME, f"{USERNAME}:email") or ""
    PASSWORD = keyring.get_password(SERVICE_NAME, f"{USERNAME}:password") or ""
    return EMAIL, PASSWORD


# ------------------------------------------------------------------------------
# This function writes credentials to keyring storage when values are available.
#
# 1. "SERVICE_NAME" scopes credentials for this application.
# 2. "USERNAME" identifies the account key prefix inside the keyring.
# 3. "EMAIL" is the Apple ID email to persist when non-empty.
# 4. "PASSWORD" is the Apple ID password to persist when non-empty.
#
# Returns: None.
#
# N.B.
# Empty values are ignored so transient config gaps do not wipe previously
# stored credentials.
# ------------------------------------------------------------------------------
def save_credentials(
    SERVICE_NAME: str,
    USERNAME: str,
    EMAIL: str,
    PASSWORD: str,
) -> None:
    if EMAIL:
        keyring.set_password(SERVICE_NAME, f"{USERNAME}:email", EMAIL)

    if PASSWORD:
        keyring.set_password(SERVICE_NAME, f"{USERNAME}:password", PASSWORD)
