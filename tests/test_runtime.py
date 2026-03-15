# ------------------------------------------------------------------------------
# This test module verifies runtime safety-net persistence behaviour.
# ------------------------------------------------------------------------------

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.config import AppConfig
from app.runtime import enforce_safety_net
from app.syncer import SafetyNetResult
from app.telegram_bot import TelegramConfig


# ------------------------------------------------------------------------------
# This class verifies safety-net marker persistence is treated as fallible
# operational state rather than a crash path.
# ------------------------------------------------------------------------------
class TestRuntime(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms a failed done-marker write blocks the run safely and
# logs the marker persistence failure instead of crashing.
# --------------------------------------------------------------------------
    def test_enforce_safety_net_handles_done_marker_write_failure(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"
            CONFIG = self._create_config(TMPDIR_PATH)
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")

            with patch(
                "app.runtime.run_first_time_safety_net",
                return_value=SafetyNetResult(False, 1000, 1000, []),
            ):
                with patch(
                    "pathlib.Path.write_text",
                    side_effect=OSError("read-only file system"),
                ):
                    RESULT = enforce_safety_net(CONFIG, TELEGRAM, LOG_FILE)

            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertFalse(RESULT)
            self.assertFalse((CONFIG.config_dir / "pyiclodoc-photos-safety_net_done.flag").exists())
            self.assertIn(
                "Safety-net marker write failed for done state:",
                LOG_TEXT,
            )

# --------------------------------------------------------------------------
# This test confirms a failed blocked-marker clear blocks the run safely and
# logs the marker clear failure instead of crashing.
# --------------------------------------------------------------------------
    def test_enforce_safety_net_handles_blocked_marker_clear_failure(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"
            CONFIG = self._create_config(TMPDIR_PATH)
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            BLOCKED_MARKER = CONFIG.config_dir / "pyiclodoc-photos-safety_net_blocked.flag"
            BLOCKED_MARKER.write_text("blocked\n", encoding="utf-8")

            with patch(
                "app.runtime.run_first_time_safety_net",
                return_value=SafetyNetResult(False, 1000, 1000, []),
            ):
                with patch(
                    "pathlib.Path.unlink",
                    side_effect=OSError("permission denied"),
                ):
                    RESULT = enforce_safety_net(CONFIG, TELEGRAM, LOG_FILE)

            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertFalse(RESULT)
            self.assertTrue(BLOCKED_MARKER.exists())
            self.assertIn(
                "Safety-net marker clear failed for blocked state:",
                LOG_TEXT,
            )

# --------------------------------------------------------------------------
# This helper builds a minimal configuration object for runtime tests.
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
