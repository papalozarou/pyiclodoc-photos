# ------------------------------------------------------------------------------
# This test module verifies Telegram message branding and main entrypoint
# orchestration.
# ------------------------------------------------------------------------------

from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.config import AppConfig
from app.main import get_build_detail, main, validate_config
from app.state import AuthState
from app.telegram_messages import format_telegram_event


# ------------------------------------------------------------------------------
# These tests verify Photos-specific Telegram headings.
# ------------------------------------------------------------------------------
class TestTelegramMessages(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms Telegram event headings use the Photos title prefix.
# --------------------------------------------------------------------------
    def test_format_telegram_event_uses_photos_branding(self) -> None:
        MESSAGE = format_telegram_event("🟢", "Container started", "Hello.")
        self.assertIn("PCD Photos - Container started", MESSAGE)


# ------------------------------------------------------------------------------
# These tests verify main entrypoint validation and runtime orchestration.
# ------------------------------------------------------------------------------
class TestMainEntrypoint(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms validate_config reports the expected required-field and
# range errors.
# --------------------------------------------------------------------------
    def test_validate_config_reports_expected_errors(self) -> None:
        CONFIG = self._create_config(Path("/tmp/test-main"))
        INVALID_MODE_CONFIG = AppConfig(**{
            **CONFIG.__dict__,
            "icloud_email": "",
            "icloud_password": "",
            "backup_discovery_mode": "bad",
            "sync_workers": 99,
            "download_chunk_mib": 99,
            "backup_album_links_mode": "bad",
        })
        INVALID_THRESHOLD_CONFIG = AppConfig(**{
            **CONFIG.__dict__,
            "backup_discovery_mode": "until_found",
            "backup_until_found_count": 0,
        })

        ERRORS = validate_config(INVALID_MODE_CONFIG)
        THRESHOLD_ERRORS = validate_config(INVALID_THRESHOLD_CONFIG)

        self.assertIn("ICLOUD_EMAIL is required.", ERRORS)
        self.assertIn("ICLOUD_PASSWORD is required.", ERRORS)
        self.assertIn("BACKUP_DISCOVERY_MODE must be one of: full, until_found.", ERRORS)
        self.assertIn(
            "BACKUP_UNTIL_FOUND_COUNT must be at least 1 when "
            "BACKUP_DISCOVERY_MODE is until_found.",
            THRESHOLD_ERRORS,
        )
        self.assertIn(
            "SYNC_DOWNLOAD_WORKERS must be auto or an integer between 1 and 16.",
            ERRORS,
        )
        self.assertIn("SYNC_DOWNLOAD_CHUNK_MIB must be an integer between 1 and 16.", ERRORS)
        self.assertIn("BACKUP_ALBUM_LINKS_MODE must be one of: hardlink, copy.", ERRORS)

# --------------------------------------------------------------------------
# This test confirms build detail uses env and returns unknown when pyicloud
# package metadata is unavailable.
# --------------------------------------------------------------------------
    def test_get_build_detail_handles_missing_pyicloud_package(self) -> None:
        with patch.dict("os.environ", {"C_APP_BUILD_REF": " build-123 "}, clear=False):
            with patch(
                "app.main.importlib_metadata.version",
                side_effect=Exception("missing"),
            ):
                with patch("app.main.importlib_metadata.PackageNotFoundError", Exception):
                    BUILD_DETAIL = get_build_detail()

        self.assertEqual(BUILD_DETAIL["app_build_ref"], "build-123")
        self.assertEqual(BUILD_DETAIL["pyicloud_version"], "unknown")

# --------------------------------------------------------------------------
# This test confirms main returns a validation error code and logs the
# validation failures.
# --------------------------------------------------------------------------
    def test_main_returns_one_on_validation_failure(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG = self._create_config(Path(TMPDIR))
            CONFIG = AppConfig(**{**CONFIG.__dict__, "icloud_email": "", "icloud_password": ""})

            with patch("app.main.load_config", return_value=CONFIG):
                with patch("app.main.configure_keyring"):
                    with patch("app.main.load_credentials", return_value=("", "")):
                        with patch("app.main.notify") as NOTIFY:
                            RESULT = main()

            self.assertEqual(RESULT, 1)
            STOP_MESSAGE = NOTIFY.call_args_list[-1].args[1]
            self.assertIn("Container stopped", STOP_MESSAGE)

# --------------------------------------------------------------------------
# This test confirms main runs the one-shot runtime path and stops the
# heartbeat on exit.
# --------------------------------------------------------------------------
    def test_main_runs_one_shot_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG = self._create_config(Path(TMPDIR))
            CONFIG = AppConfig(**{**CONFIG.__dict__, "run_once": True})
            CLIENT = MagicMock()
            HEARTBEAT_STOP_EVENT = MagicMock()
            AUTH_STATE = AuthState("2026-03-15T10:00:00+00:00", False, False, "none")

            with patch("app.main.load_config", return_value=CONFIG):
                with patch("app.main.configure_keyring"):
                    with patch("app.main.load_credentials", return_value=("", "")):
                        with patch("app.main.start_heartbeat_updater", return_value=HEARTBEAT_STOP_EVENT):
                            with patch("app.main.save_credentials"):
                                with patch("app.main.ICloudDriveClient", return_value=CLIENT):
                                    with patch("app.main.load_auth_state", return_value=AUTH_STATE):
                                        with patch(
                                            "app.main.attempt_auth",
                                            return_value=(AUTH_STATE, True, "auth ok"),
                                        ):
                                            with patch(
                                                "app.main.run_one_shot_runtime",
                                                return_value=(7, "one-shot done"),
                                            ) as RUN_ONE_SHOT:
                                                RESULT = main()

            self.assertEqual(RESULT, 7)
            RUN_ONE_SHOT.assert_called_once()
            HEARTBEAT_STOP_EVENT.set.assert_called_once()

# --------------------------------------------------------------------------
# This test confirms main runs the persistent runtime path when not in
# one-shot mode.
# --------------------------------------------------------------------------
    def test_main_runs_persistent_runtime(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG = self._create_config(Path(TMPDIR))
            CLIENT = MagicMock()
            HEARTBEAT_STOP_EVENT = MagicMock()
            AUTH_STATE = AuthState("2026-03-15T10:00:00+00:00", False, False, "none")

            with patch("app.main.load_config", return_value=CONFIG):
                with patch("app.main.configure_keyring"):
                    with patch("app.main.load_credentials", return_value=("", "")):
                        with patch("app.main.start_heartbeat_updater", return_value=HEARTBEAT_STOP_EVENT):
                            with patch("app.main.save_credentials"):
                                with patch("app.main.ICloudDriveClient", return_value=CLIENT):
                                    with patch("app.main.load_auth_state", return_value=AUTH_STATE):
                                        with patch(
                                            "app.main.attempt_auth",
                                            return_value=(AUTH_STATE, True, "auth ok"),
                                        ):
                                            with patch("app.main.run_persistent_runtime") as RUN_PERSISTENT:
                                                RESULT = main()

            self.assertIsNone(RESULT)
            RUN_PERSISTENT.assert_called_once()
            HEARTBEAT_STOP_EVENT.set.assert_called_once()

# --------------------------------------------------------------------------
# This helper builds a minimal valid configuration object for main-entrypoint
# tests.
# --------------------------------------------------------------------------
    def _create_config(self, ROOT_DIR: Path) -> AppConfig:
        CONFIG_DIR = ROOT_DIR / "config"
        OUTPUT_DIR = ROOT_DIR / "output"
        LOGS_DIR = ROOT_DIR / "logs"
        COOKIE_DIR = CONFIG_DIR / "cookies"
        SESSION_DIR = CONFIG_DIR / "session"
        COMPAT_DIR = CONFIG_DIR / "icloudpd"

        for DIR_PATH in (
            CONFIG_DIR,
            OUTPUT_DIR,
            LOGS_DIR,
            COOKIE_DIR,
            SESSION_DIR,
            COMPAT_DIR,
        ):
            DIR_PATH.mkdir(parents=True, exist_ok=True)

        return AppConfig(
            container_username="icloudphotos",
            icloud_email="alice@example.com",
            icloud_password="secret",
            telegram_bot_token="",
            telegram_chat_id="",
            keychain_service_name="icloud-photos-backup",
            run_once=False,
            schedule_mode="interval",
            schedule_backup_time="02:00",
            schedule_weekdays="monday",
            schedule_monthly_week="first",
            schedule_interval_minutes=1440,
            backup_discovery_mode="full",
            backup_until_found_count=50,
            backup_delete_removed=False,
            sync_workers=0,
            download_chunk_mib=4,
            reauth_interval_days=30,
            output_dir=OUTPUT_DIR,
            config_dir=CONFIG_DIR,
            logs_dir=LOGS_DIR,
            manifest_path=CONFIG_DIR / "pyiclodoc-photos-manifest.json",
            auth_state_path=CONFIG_DIR / "pyiclodoc-photos-auth_state.json",
            heartbeat_path=LOGS_DIR / "pyiclodoc-photos-heartbeat.txt",
            cookie_dir=COOKIE_DIR,
            session_dir=SESSION_DIR,
            icloudpd_compat_dir=COMPAT_DIR,
            safety_net_sample_size=200,
            backup_albums_enabled=True,
            backup_album_links_mode="hardlink",
            backup_include_shared_albums=True,
            backup_include_favourites=True,
            backup_root_library="library",
            backup_root_albums="albums",
        )
