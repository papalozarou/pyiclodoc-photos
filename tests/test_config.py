# ------------------------------------------------------------------------------
# This test module validates environment-driven config loading behaviour.
# ------------------------------------------------------------------------------

from pathlib import Path
import os
import tempfile
import unittest
from unittest.mock import patch

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.config import load_config


# ------------------------------------------------------------------------------
# These tests verify photo-worker config defaults and path construction.
# ------------------------------------------------------------------------------
class TestConfig(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms default photo-specific file paths and feature flags.
# --------------------------------------------------------------------------
    def test_load_config_uses_photo_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            ROOT_DIR = Path(TMPDIR)
            ENV = {
                "CONFIG_DIR": str(ROOT_DIR / "config"),
                "OUTPUT_DIR": str(ROOT_DIR / "output"),
                "LOGS_DIR": str(ROOT_DIR / "logs"),
                "COOKIE_DIR": str(ROOT_DIR / "config" / "cookies"),
                "SESSION_DIR": str(ROOT_DIR / "config" / "session"),
                "ICLOUDPD_COMPAT_DIR": str(ROOT_DIR / "config" / "icloudpd"),
            }

            with patch.dict(os.environ, ENV, clear=True):
                CONFIG = load_config()

        self.assertEqual(CONFIG.keychain_service_name, "pyiclodoc-photos")
        self.assertEqual(CONFIG.manifest_path.name, "pyiclodoc-photos-manifest.json")
        self.assertEqual(CONFIG.auth_state_path.name, "pyiclodoc-photos-auth_state.json")
        self.assertEqual(CONFIG.heartbeat_path.name, "pyiclodoc-photos-heartbeat.txt")
        self.assertTrue(CONFIG.backup_albums_enabled)
        self.assertEqual(CONFIG.backup_album_links_mode, "hardlink")
        self.assertEqual(CONFIG.backup_discovery_mode, "full")
        self.assertEqual(CONFIG.backup_until_found_count, 50)
        self.assertEqual(CONFIG.config_errors, ())

# --------------------------------------------------------------------------
# This test confirms invalid numeric env values are preserved as explicit
# config parse errors instead of being silently accepted.
# --------------------------------------------------------------------------
    def test_load_config_records_invalid_numeric_env_values(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            ROOT_DIR = Path(TMPDIR)
            ENV = {
                "CONFIG_DIR": str(ROOT_DIR / "config"),
                "OUTPUT_DIR": str(ROOT_DIR / "output"),
                "LOGS_DIR": str(ROOT_DIR / "logs"),
                "COOKIE_DIR": str(ROOT_DIR / "config" / "cookies"),
                "SESSION_DIR": str(ROOT_DIR / "config" / "session"),
                "ICLOUDPD_COMPAT_DIR": str(ROOT_DIR / "config" / "icloudpd"),
                "SYNC_DOWNLOAD_CHUNK_MIB": "abc",
                "SYNC_DOWNLOAD_WORKERS": "bad",
            }

            with patch.dict(os.environ, ENV, clear=True):
                CONFIG = load_config()

        self.assertIn("SYNC_DOWNLOAD_CHUNK_MIB must be an integer.", CONFIG.config_errors)
        self.assertIn(
            "SYNC_DOWNLOAD_WORKERS must be auto or a positive integer.",
            CONFIG.config_errors,
        )
