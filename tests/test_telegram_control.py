# ------------------------------------------------------------------------------
# This test module verifies Telegram command intake and command-side state
# changes.
# ------------------------------------------------------------------------------

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.config import AppConfig
from app.state import AuthState
from app.telegram_bot import TelegramConfig
from app.telegram_control import handle_command, process_commands


# ------------------------------------------------------------------------------
# This function builds a minimal app config for Telegram control tests.
# ------------------------------------------------------------------------------
def create_config(ROOT_DIR: Path) -> AppConfig:
    return AppConfig(
        container_username="alice",
        icloud_email="alice@example.com",
        icloud_password="secret",
        telegram_bot_token="token",
        telegram_chat_id="1",
        keychain_service_name="pyiclodoc-photos",
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
        output_dir=ROOT_DIR / "output",
        config_dir=ROOT_DIR / "config",
        logs_dir=ROOT_DIR / "logs",
        manifest_path=ROOT_DIR / "config" / "pyiclodoc-photos-manifest.json",
        auth_state_path=ROOT_DIR / "config" / "pyiclodoc-photos-auth_state.json",
        heartbeat_path=ROOT_DIR / "logs" / "pyiclodoc-photos-heartbeat.txt",
        cookie_dir=ROOT_DIR / "config" / "cookies",
        session_dir=ROOT_DIR / "config" / "session",
        icloudpd_compat_dir=ROOT_DIR / "config" / "icloudpd",
        safety_net_sample_size=200,
        backup_albums_enabled=True,
        backup_album_links_mode="hardlink",
        backup_include_shared_albums=True,
        backup_include_favourites=True,
        backup_root_library="library",
        backup_root_albums="albums",
    )


# ------------------------------------------------------------------------------
# These tests verify extracted Telegram command logic.
# ------------------------------------------------------------------------------
class TestTelegramControl(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms command polling preserves the existing offset when no
# Telegram updates are available.
# --------------------------------------------------------------------------
    def test_process_commands_returns_existing_offset_when_no_updates_exist(self) -> None:
        TELEGRAM = TelegramConfig(bot_token="token", chat_id="1")

        with patch("app.telegram_control.fetch_updates", return_value=[]):
            COMMANDS, NEXT_OFFSET = process_commands(TELEGRAM, "alice", 10)

        self.assertEqual(COMMANDS, [])
        self.assertEqual(NEXT_OFFSET, 10)

# --------------------------------------------------------------------------
# This test confirms command polling advances the offset and filters invalid
# updates out of the returned command list.
# --------------------------------------------------------------------------
    def test_process_commands_collects_valid_commands_and_advances_offset(self) -> None:
        TELEGRAM = TelegramConfig(bot_token="token", chat_id="1")
        UPDATES = [
            {"update_id": 4, "message": {"text": "ignore"}},
            {"update_id": 7, "message": {"text": "command"}},
        ]

        with patch("app.telegram_control.fetch_updates", return_value=UPDATES):
            with patch(
                "app.telegram_control.parse_command",
                side_effect=[None, type("Event", (), {"command": "backup", "args": ""})()],
            ):
                COMMANDS, NEXT_OFFSET = process_commands(TELEGRAM, "alice", None)

        self.assertEqual(COMMANDS, [("backup", "")])
        self.assertEqual(NEXT_OFFSET, 8)

# --------------------------------------------------------------------------
# This test confirms command polling debug logs command metadata without
# writing command arguments.
# --------------------------------------------------------------------------
    def test_process_commands_logs_sanitised_command_metadata(self) -> None:
        TELEGRAM = TelegramConfig(bot_token="token", chat_id="1")
        UPDATES = [
            {
                "update_id": 4,
                "message": {
                    "chat": {"id": "1"},
                    "text": "alice auth 123456",
                },
            },
        ]

        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"

            with patch.dict("os.environ", {"LOG_LEVEL": "debug"}, clear=False):
                with patch("app.telegram_control.fetch_updates", return_value=UPDATES):
                    COMMANDS, NEXT_OFFSET = process_commands(TELEGRAM, "alice", None, LOG_FILE)

            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")

        self.assertEqual(COMMANDS, [("auth", "123456")])
        self.assertEqual(NEXT_OFFSET, 5)
        self.assertIn("Telegram command accepted. command=auth, has_args=True", LOG_TEXT)
        self.assertIn("Telegram command poll finished. updates=1, accepted=1", LOG_TEXT)
        self.assertNotIn("123456", LOG_TEXT)

# --------------------------------------------------------------------------
# This test confirms a backup command returns backup intent and sends the
# request message.
# --------------------------------------------------------------------------
    def test_handle_command_sets_backup_requested(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            ROOT_DIR = Path(TMPDIR)
            CONFIG = create_config(ROOT_DIR)
            AUTH_STATE = AuthState("1970-01-01T00:00:00+00:00", False, False, "none")
            SENT_MESSAGES: list[str] = []

            OUTCOME = handle_command(
                "backup",
                "",
                CONFIG,
                AUTH_STATE,
                True,
                SENT_MESSAGES.append,
                lambda CURRENT_STATE, PROVIDED_CODE: (CURRENT_STATE, True, PROVIDED_CODE),
            )

        self.assertTrue(OUTCOME.backup_requested)
        self.assertTrue(SENT_MESSAGES)
        self.assertIn("Backup requested", SENT_MESSAGES[0])

# --------------------------------------------------------------------------
# This test confirms a manual auth prompt updates state and emits the auth
# message without marking a backup request.
# --------------------------------------------------------------------------
    def test_handle_command_marks_auth_pending_without_code(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            ROOT_DIR = Path(TMPDIR)
            CONFIG = create_config(ROOT_DIR)
            CONFIG.config_dir.mkdir(parents=True, exist_ok=True)
            AUTH_STATE = AuthState("1970-01-01T00:00:00+00:00", False, False, "none")
            SENT_MESSAGES: list[str] = []

            OUTCOME = handle_command(
                "auth",
                "",
                CONFIG,
                AUTH_STATE,
                False,
                SENT_MESSAGES.append,
                lambda CURRENT_STATE, PROVIDED_CODE: (CURRENT_STATE, False, PROVIDED_CODE),
            )

        self.assertTrue(OUTCOME.auth_state.auth_pending)
        self.assertFalse(OUTCOME.backup_requested)
        self.assertIn("Authentication required", SENT_MESSAGES[0])

# --------------------------------------------------------------------------
# This test confirms a manual reauth prompt updates state and emits the
# reauth message without marking a backup request.
# --------------------------------------------------------------------------
    def test_handle_command_marks_reauth_pending_without_code(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            ROOT_DIR = Path(TMPDIR)
            CONFIG = create_config(ROOT_DIR)
            CONFIG.config_dir.mkdir(parents=True, exist_ok=True)
            AUTH_STATE = AuthState("1970-01-01T00:00:00+00:00", False, False, "none")
            SENT_MESSAGES: list[str] = []

            with patch("app.telegram_control.now_iso", return_value="2026-03-15T12:00:00+00:00"):
                OUTCOME = handle_command(
                    "reauth",
                    "",
                    CONFIG,
                    AUTH_STATE,
                    False,
                    SENT_MESSAGES.append,
                    lambda CURRENT_STATE, PROVIDED_CODE: (CURRENT_STATE, False, PROVIDED_CODE),
                )

        self.assertTrue(OUTCOME.auth_state.reauth_pending)
        self.assertEqual(OUTCOME.auth_state.reminder_stage, "none")
        self.assertEqual(OUTCOME.auth_state.last_reminder_utc, "2026-03-15T12:00:00+00:00")
        self.assertTrue(OUTCOME.auth_state.manual_reauth_pending)
        self.assertFalse(OUTCOME.backup_requested)
        self.assertIn("Reauthentication required", SENT_MESSAGES[0])

# --------------------------------------------------------------------------
# This test confirms command-side auth persistence failure is surfaced in the
# outcome details and leaves in-memory state unchanged.
# --------------------------------------------------------------------------
    def test_handle_command_surfaces_auth_state_persistence_failure(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            ROOT_DIR = Path(TMPDIR)
            CONFIG = create_config(ROOT_DIR)
            CONFIG.config_dir.mkdir(parents=True, exist_ok=True)
            AUTH_STATE = AuthState("1970-01-01T00:00:00+00:00", False, False, "none")
            SENT_MESSAGES: list[str] = []

            with patch("app.telegram_control.persist_auth_state_transition", return_value=(AUTH_STATE, False)):
                OUTCOME = handle_command(
                    "auth",
                    "",
                    CONFIG,
                    AUTH_STATE,
                    False,
                    SENT_MESSAGES.append,
                    lambda CURRENT_STATE, PROVIDED_CODE: (CURRENT_STATE, False, PROVIDED_CODE),
                )

        self.assertEqual(OUTCOME.details, "Auth state persistence failed.")
        self.assertEqual(OUTCOME.auth_state, AUTH_STATE)
        self.assertIn("Auth state update failed", SENT_MESSAGES[0])

# --------------------------------------------------------------------------
# This test confirms failed reauth-state persistence also leaves the existing
# state unchanged instead of creating an in-memory-only pending reauth state.
# --------------------------------------------------------------------------
    def test_handle_command_keeps_existing_state_when_reauth_persistence_fails(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            ROOT_DIR = Path(TMPDIR)
            CONFIG = create_config(ROOT_DIR)
            CONFIG.config_dir.mkdir(parents=True, exist_ok=True)
            AUTH_STATE = AuthState("1970-01-01T00:00:00+00:00", False, False, "none")
            SENT_MESSAGES: list[str] = []

            with patch("app.telegram_control.persist_auth_state_transition", return_value=(AUTH_STATE, False)):
                OUTCOME = handle_command(
                    "reauth",
                    "",
                    CONFIG,
                    AUTH_STATE,
                    False,
                    SENT_MESSAGES.append,
                    lambda CURRENT_STATE, PROVIDED_CODE: (CURRENT_STATE, False, PROVIDED_CODE),
                )

        self.assertEqual(OUTCOME.details, "Auth state persistence failed.")
        self.assertEqual(OUTCOME.auth_state, AUTH_STATE)
        self.assertIn("Auth state update failed", SENT_MESSAGES[0])

# --------------------------------------------------------------------------
# This test confirms auth commands with a code delegate to the auth executor
# and return its details.
# --------------------------------------------------------------------------
    def test_handle_command_delegates_to_auth_executor_when_args_are_present(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            ROOT_DIR = Path(TMPDIR)
            CONFIG = create_config(ROOT_DIR)
            AUTH_STATE = AuthState("1970-01-01T00:00:00+00:00", True, False, "none")
            SENT_MESSAGES: list[str] = []
            RETURNED_STATE = AuthState("2026-03-15T11:00:00+00:00", False, False, "none")

            OUTCOME = handle_command(
                "auth",
                "123456",
                CONFIG,
                AUTH_STATE,
                False,
                SENT_MESSAGES.append,
                lambda CURRENT_STATE, PROVIDED_CODE: (
                    RETURNED_STATE,
                    True,
                    f"used:{PROVIDED_CODE}",
                ),
            )

        self.assertEqual(OUTCOME.auth_state, RETURNED_STATE)
        self.assertTrue(OUTCOME.is_authenticated)
        self.assertEqual(OUTCOME.details, "used:123456")
        self.assertEqual(SENT_MESSAGES, [])
