# ------------------------------------------------------------------------------
# This test module verifies runtime safety-net persistence behaviour.
# ------------------------------------------------------------------------------

from dataclasses import replace
from pathlib import Path
import tempfile
import unittest
import os
from unittest.mock import MagicMock
from unittest.mock import patch

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.config import AppConfig
from app.runtime import (
    SafetyNetEnforcementResult,
    clear_safety_net_marker,
    format_delete_summary,
    enforce_safety_net,
    format_average_speed,
    format_duration_clock,
    log_effective_backup_settings,
    run_backup,
    run_one_shot_runtime,
    run_persistent_runtime,
    wait_for_one_shot_auth,
    write_safety_net_marker,
)
from app.state import AuthState
from app.syncer import SafetyNetResult
from app.syncer import SyncResult
from app.telegram_bot import TelegramConfig
from app.telegram_control import CommandOutcome


# ------------------------------------------------------------------------------
# This class verifies safety-net marker persistence is treated as fallible
# operational state rather than a crash path.
# ------------------------------------------------------------------------------
class TestRuntime(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms the runtime duration formatter clamps negative input
# and zero-pads the clock representation.
# --------------------------------------------------------------------------
    def test_format_duration_clock_clamps_negative_seconds(self) -> None:
        self.assertEqual(format_duration_clock(-5), "00:00:00")
        self.assertEqual(format_duration_clock(3661), "01:01:01")

# --------------------------------------------------------------------------
# This test confirms average speed formatting uses a minimum duration floor.
# --------------------------------------------------------------------------
    def test_format_average_speed_uses_safe_duration_floor(self) -> None:
        self.assertEqual(format_average_speed(1048576, 0), "1.00 MiB/s")

# --------------------------------------------------------------------------
# This test confirms delete-summary formatting uses natural singular and
# plural labels for files and directories.
# --------------------------------------------------------------------------
    def test_format_delete_summary_uses_natural_pluralisation(self) -> None:
        self.assertEqual(
            format_delete_summary(0, 0),
            "Deleted: 0 files, 0 directories",
        )
        self.assertEqual(
            format_delete_summary(1, 0),
            "Deleted: 1 file, 0 directories",
        )
        self.assertEqual(
            format_delete_summary(2, 1),
            "Deleted: 2 files, 1 directory",
        )

# --------------------------------------------------------------------------
# This test confirms marker helpers return success on happy-path writes and
# marker removal.
# --------------------------------------------------------------------------
    def test_safety_net_marker_helpers_succeed_on_happy_path(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"
            MARKER_PATH = TMPDIR_PATH / "marker.flag"

            self.assertTrue(write_safety_net_marker(MARKER_PATH, "ok\n", LOG_FILE, "done state"))
            self.assertTrue(MARKER_PATH.exists())
            self.assertTrue(clear_safety_net_marker(MARKER_PATH, LOG_FILE, "done state"))
            self.assertFalse(MARKER_PATH.exists())

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
            self.assertFalse(RESULT.can_proceed)
            self.assertFalse(RESULT.should_retry)
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
            self.assertFalse(RESULT.can_proceed)
            self.assertFalse(RESULT.should_retry)
            self.assertTrue(BLOCKED_MARKER.exists())
            self.assertIn(
                "Safety-net marker clear failed for blocked state:",
                LOG_TEXT,
            )

# --------------------------------------------------------------------------
# This test confirms an existing done marker allows backup immediately.
# --------------------------------------------------------------------------
    def test_enforce_safety_net_returns_true_when_done_marker_exists(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"
            CONFIG = self._create_config(TMPDIR_PATH)
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            DONE_MARKER = CONFIG.config_dir / "pyiclodoc-photos-safety_net_done.flag"
            DONE_MARKER.write_text("ok\n", encoding="utf-8")

            RESULT = enforce_safety_net(CONFIG, TELEGRAM, LOG_FILE)

            self.assertTrue(RESULT.can_proceed)
            self.assertFalse(RESULT.should_retry)

# --------------------------------------------------------------------------
# This test confirms a blocked safety-net path records the blocked marker and
# emits the notification hook.
# --------------------------------------------------------------------------
    def test_enforce_safety_net_records_blocked_state(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"
            CONFIG = self._create_config(TMPDIR_PATH)
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")

            with patch(
                "app.runtime.run_first_time_safety_net",
                return_value=SafetyNetResult(
                    True,
                    1000,
                    1000,
                    ["a.jpg: uid=1, gid=1 (expected uid=1000, gid=1000)"],
                ),
            ):
                with patch("app.runtime.notify") as NOTIFY:
                    RESULT = enforce_safety_net(CONFIG, TELEGRAM, LOG_FILE)

            self.assertFalse(RESULT.can_proceed)
            self.assertTrue(RESULT.should_retry)
            self.assertTrue(
                (CONFIG.config_dir / "pyiclodoc-photos-safety_net_blocked.flag").exists(),
            )
            NOTIFY.assert_called_once()

# --------------------------------------------------------------------------
# This test confirms a blocked-marker write failure stops the worker instead
# of retrying a blocked alert loop without persisted state.
# --------------------------------------------------------------------------
    def test_enforce_safety_net_handles_blocked_marker_write_failure(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"
            CONFIG = self._create_config(TMPDIR_PATH)
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")

            with patch(
                "app.runtime.run_first_time_safety_net",
                return_value=SafetyNetResult(
                    True,
                    1000,
                    1000,
                    ["a.jpg: uid=1, gid=1 (expected uid=1000, gid=1000)"],
                ),
            ):
                with patch("app.runtime.notify") as NOTIFY:
                    with patch(
                        "app.runtime.write_safety_net_marker",
                        return_value=False,
                    ):
                        RESULT = enforce_safety_net(CONFIG, TELEGRAM, LOG_FILE)

            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertFalse(RESULT.can_proceed)
            self.assertFalse(RESULT.should_retry)
            self.assertIn(
                "Stopping instead of retrying the blocked alert loop.",
                LOG_TEXT,
            )
            NOTIFY.assert_called_once()

# --------------------------------------------------------------------------
# This test confirms effective backup settings logging includes the resolved
# worker-count label.
# --------------------------------------------------------------------------
    def test_log_effective_backup_settings_writes_expected_detail(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"
            CONFIG = self._create_config(TMPDIR_PATH)
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}

            with patch.dict("os.environ", {"LOG_LEVEL": "debug"}, clear=False):
                log_effective_backup_settings(CONFIG, LOG_FILE, BUILD_DETAIL)

            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertIn("Build detail: app_build_ref=abc123, pyicloud_version=2.4.1", LOG_TEXT)
            self.assertIn("effective_download_workers=", LOG_TEXT)

# --------------------------------------------------------------------------
# This test confirms backup execution persists the new manifest and includes
# average speed only when files were transferred.
# --------------------------------------------------------------------------
    def test_run_backup_saves_manifest_and_sends_completion_message(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"
            CONFIG = self._create_config(TMPDIR_PATH)
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            CLIENT = MagicMock()
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            SUMMARY = SyncResult(3, 2, 2097152, 1, 0)
            NEW_MANIFEST = {"library/2026/03/15/IMG_0001.JPG": {"size": 4}}

            with patch("app.runtime.load_manifest", return_value={"old": {"size": 1}}):
                with patch("app.runtime.perform_incremental_sync", return_value=(SUMMARY, NEW_MANIFEST)):
                    with patch("app.runtime.save_manifest") as SAVE_MANIFEST:
                        with patch("app.runtime.notify") as NOTIFY:
                            with patch.dict("os.environ", {"LOG_LEVEL": "debug"}, clear=False):
                                with patch("app.runtime.time.time", side_effect=[100, 104]):
                                    run_backup(
                                        CLIENT,
                                        CONFIG,
                                        TELEGRAM,
                                        LOG_FILE,
                                        "scheduled",
                                        BUILD_DETAIL,
                                    )

            SAVE_MANIFEST.assert_called_once_with(CONFIG.manifest_path, NEW_MANIFEST)
            self.assertEqual(NOTIFY.call_count, 2)
            COMPLETION_MESSAGE = NOTIFY.call_args_list[-1].args[1]
            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertIn("Transferred: 2/3", COMPLETION_MESSAGE)
            self.assertIn("Average speed:", COMPLETION_MESSAGE)
            self.assertIn(
                "Manifest growth detail: previous_entries=1, refreshed_entries=1, delta=0",
                LOG_TEXT,
            )

# --------------------------------------------------------------------------
# This test confirms backup completion includes delete totals when mirror
# delete handling is enabled for the worker.
# --------------------------------------------------------------------------
    def test_run_backup_includes_delete_totals_when_delete_mode_is_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"
            CONFIG = replace(self._create_config(TMPDIR_PATH), backup_delete_removed=True)
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            CLIENT = MagicMock()
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            SUMMARY = SyncResult(3, 2, 2097152, 1, 0, 0, 0, 4, 2, 0)
            NEW_MANIFEST = {"library/2026/03/15/IMG_0001.JPG": {"size": 4}}

            with patch("app.runtime.load_manifest", return_value={"old": {"size": 1}}):
                with patch("app.runtime.perform_incremental_sync", return_value=(SUMMARY, NEW_MANIFEST)):
                    with patch("app.runtime.save_manifest") as SAVE_MANIFEST:
                        with patch("app.runtime.notify") as NOTIFY:
                            with patch("app.runtime.time.time", side_effect=[100, 104]):
                                run_backup(
                                    CLIENT,
                                    CONFIG,
                                    TELEGRAM,
                                    LOG_FILE,
                                    "scheduled",
                                    BUILD_DETAIL,
                                )

            SAVE_MANIFEST.assert_called_once_with(CONFIG.manifest_path, NEW_MANIFEST)
            COMPLETION_MESSAGE = NOTIFY.call_args_list[-1].args[1]
            self.assertIn("Deleted: 4 files, 2 directories", COMPLETION_MESSAGE)

# --------------------------------------------------------------------------
# This test confirms backup completion surfaces manifest persistence failure
# instead of assuming the save succeeded.
# --------------------------------------------------------------------------
    def test_run_backup_surfaces_manifest_save_failure(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"
            CONFIG = self._create_config(TMPDIR_PATH)
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            CLIENT = MagicMock()
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            SUMMARY = SyncResult(1, 1, 1024, 0, 0)

            with patch("app.runtime.load_manifest", return_value={}):
                with patch("app.runtime.perform_incremental_sync", return_value=(SUMMARY, {})):
                    with patch("app.runtime.save_manifest", return_value=False):
                        with patch("app.runtime.notify") as NOTIFY:
                            with patch("app.runtime.time.time", side_effect=[100, 101]):
                                run_backup(
                                    CLIENT,
                                    CONFIG,
                                    TELEGRAM,
                                    LOG_FILE,
                                    "scheduled",
                                    BUILD_DETAIL,
                                )

            COMPLETION_MESSAGE = NOTIFY.call_args_list[-1].args[1]
            self.assertIn("Manifest save failed. Next run may repeat work.", COMPLETION_MESSAGE)
            self.assertIn("Manifest save failed at", LOG_FILE.read_text(encoding="utf-8"))

# --------------------------------------------------------------------------
# This test confirms backup completion reports derived-output errors through
# the primary run summary model instead of hiding them from the final status.
# --------------------------------------------------------------------------
    def test_run_backup_surfaces_derived_output_errors_in_completion_status(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"
            CONFIG = self._create_config(TMPDIR_PATH)
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            CLIENT = MagicMock()
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            SUMMARY = SyncResult(2, 1, 1024, 1, 1, 0, 1)

            with patch.dict("os.environ", {"LOG_LEVEL": "debug"}, clear=False):
                with patch("app.runtime.load_manifest", return_value={}):
                    with patch("app.runtime.perform_incremental_sync", return_value=(SUMMARY, {})):
                        with patch("app.runtime.save_manifest", return_value=True):
                            with patch("app.runtime.notify") as NOTIFY:
                                with patch("app.runtime.time.time", side_effect=[100, 101]):
                                    run_backup(
                                        CLIENT,
                                        CONFIG,
                                        TELEGRAM,
                                        LOG_FILE,
                                        "scheduled",
                                        BUILD_DETAIL,
                                    )

            COMPLETION_MESSAGE = NOTIFY.call_args_list[-1].args[1]
            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertIn("Errors: 1", COMPLETION_MESSAGE)
            self.assertIn("transfer_errors=0, derived_errors=1", LOG_TEXT)

# --------------------------------------------------------------------------
# This test confirms backup completion reports delete-phase failures through
# the primary error count shown to operators.
# --------------------------------------------------------------------------
    def test_run_backup_surfaces_delete_errors_in_completion_status(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"
            CONFIG = self._create_config(TMPDIR_PATH)
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            CLIENT = MagicMock()
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            SUMMARY = SyncResult(2, 1, 1024, 1, 2, 0, 0, 0, 0, 2)

            with patch.dict("os.environ", {"LOG_LEVEL": "debug"}, clear=False):
                with patch("app.runtime.load_manifest", return_value={}):
                    with patch("app.runtime.perform_incremental_sync", return_value=(SUMMARY, {})):
                        with patch("app.runtime.save_manifest", return_value=True):
                            with patch("app.runtime.notify") as NOTIFY:
                                with patch("app.runtime.time.time", side_effect=[100, 101]):
                                    run_backup(
                                        CLIENT,
                                        CONFIG,
                                        TELEGRAM,
                                        LOG_FILE,
                                        "scheduled",
                                        BUILD_DETAIL,
                                    )

            COMPLETION_MESSAGE = NOTIFY.call_args_list[-1].args[1]
            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertIn("Errors: 2", COMPLETION_MESSAGE)
            self.assertIn("delete_errors=2", LOG_TEXT)

# --------------------------------------------------------------------------
# This test confirms backup completion omits average speed when no files were
# transferred in the run.
# --------------------------------------------------------------------------
    def test_run_backup_omits_average_speed_when_nothing_transferred(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"
            CONFIG = self._create_config(TMPDIR_PATH)
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            CLIENT = MagicMock()
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            SUMMARY = SyncResult(3, 0, 0, 3, 0)

            with patch("app.runtime.load_manifest", return_value={}):
                with patch("app.runtime.perform_incremental_sync", return_value=(SUMMARY, {})):
                    with patch("app.runtime.save_manifest"):
                        with patch("app.runtime.notify") as NOTIFY:
                            with patch("app.runtime.time.time", side_effect=[100, 101]):
                                run_backup(
                                    CLIENT,
                                    CONFIG,
                                    TELEGRAM,
                                    LOG_FILE,
                                    "scheduled",
                                    BUILD_DETAIL,
                                )

            COMPLETION_MESSAGE = NOTIFY.call_args_list[-1].args[1]
            self.assertNotIn("Average speed:", COMPLETION_MESSAGE)

# --------------------------------------------------------------------------
# This test confirms one-shot auth wait exits immediately once auth is valid.
# --------------------------------------------------------------------------
    def test_wait_for_one_shot_auth_returns_immediately_when_already_valid(self) -> None:
        CONFIG = self._create_config(Path(tempfile.mkdtemp()))
        CLIENT = MagicMock()
        TELEGRAM = TelegramConfig(bot_token="", chat_id="")
        AUTH_STATE = AuthState("2026-03-15T10:00:00+00:00", False, False, "none")

        RESULT_STATE, RESULT_AUTH = wait_for_one_shot_auth(
            CONFIG,
            CLIENT,
            AUTH_STATE,
            True,
            TELEGRAM,
        )

        self.assertEqual(RESULT_STATE, AUTH_STATE)
        self.assertTrue(RESULT_AUTH)

# --------------------------------------------------------------------------
# This test confirms one-shot auth wait processes command outcomes before
# succeeding.
# --------------------------------------------------------------------------
    def test_wait_for_one_shot_auth_processes_commands(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG = self._create_config(Path(TMPDIR))
            CLIENT = MagicMock()
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            INITIAL_STATE = AuthState("2026-03-15T10:00:00+00:00", True, False, "none")
            FINAL_STATE = AuthState("2026-03-15T10:05:00+00:00", False, False, "none")

            with patch(
                "app.runtime.process_commands",
                return_value=([("auth", "123456")], 5),
            ):
                with patch(
                    "app.runtime.handle_command",
                    return_value=CommandOutcome(FINAL_STATE, True, False, "ok"),
                ):
                    with patch("app.runtime.time.sleep"):
                        RESULT_STATE, RESULT_AUTH = wait_for_one_shot_auth(
                            CONFIG,
                            CLIENT,
                            INITIAL_STATE,
                            False,
                            TELEGRAM,
                        )

            self.assertEqual(RESULT_STATE, FINAL_STATE)
            self.assertTrue(RESULT_AUTH)

# --------------------------------------------------------------------------
# This test confirms one-shot runtime skips with auth-required status when
# authentication never completes.
# --------------------------------------------------------------------------
    def test_run_one_shot_runtime_returns_auth_skip_status(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG = self._create_config(Path(TMPDIR))
            CLIENT = MagicMock()
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            LOG_FILE = Path(TMPDIR) / "worker.log"
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            AUTH_STATE = AuthState("2026-03-15T10:00:00+00:00", True, False, "none")

            with patch(
                "app.runtime.wait_for_one_shot_auth",
                return_value=(AUTH_STATE, False),
            ):
                with patch("app.runtime.notify") as NOTIFY:
                    EXIT_CODE, STOP_STATUS = run_one_shot_runtime(
                        CONFIG,
                        CLIENT,
                        AUTH_STATE,
                        False,
                        TELEGRAM,
                        LOG_FILE,
                        BUILD_DETAIL,
                    )

            self.assertEqual(EXIT_CODE, 2)
            self.assertIn("incomplete authentication", STOP_STATUS)
            self.assertGreaterEqual(NOTIFY.call_count, 2)

# --------------------------------------------------------------------------
# This test confirms one-shot runtime skips with reauth status when reauth is
# still pending after the wait.
# --------------------------------------------------------------------------
    def test_run_one_shot_runtime_returns_reauth_skip_status(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG = self._create_config(Path(TMPDIR))
            CLIENT = MagicMock()
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            LOG_FILE = Path(TMPDIR) / "worker.log"
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            AUTH_STATE = AuthState("2026-03-15T10:00:00+00:00", False, True, "prompt2")

            with patch(
                "app.runtime.wait_for_one_shot_auth",
                return_value=(AUTH_STATE, True),
            ):
                with patch("app.runtime.notify"):
                    EXIT_CODE, STOP_STATUS = run_one_shot_runtime(
                        CONFIG,
                        CLIENT,
                        AUTH_STATE,
                        True,
                        TELEGRAM,
                        LOG_FILE,
                        BUILD_DETAIL,
                    )

            self.assertEqual(EXIT_CODE, 3)
            self.assertIn("pending reauthentication", STOP_STATUS)

# --------------------------------------------------------------------------
# This test confirms one-shot runtime returns a safety-net block status when
# the safety check refuses the run.
# --------------------------------------------------------------------------
    def test_run_one_shot_runtime_returns_safety_net_status(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG = self._create_config(Path(TMPDIR))
            CLIENT = MagicMock()
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            LOG_FILE = Path(TMPDIR) / "worker.log"
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            AUTH_STATE = AuthState("2026-03-15T10:00:00+00:00", False, False, "none")

            with patch(
                "app.runtime.enforce_safety_net",
                return_value=SafetyNetEnforcementResult(False, True),
            ):
                EXIT_CODE, STOP_STATUS = run_one_shot_runtime(
                    CONFIG,
                    CLIENT,
                    AUTH_STATE,
                    True,
                    TELEGRAM,
                    LOG_FILE,
                    BUILD_DETAIL,
                )

            self.assertEqual(EXIT_CODE, 4)
            self.assertIn("blocked by safety net", STOP_STATUS)

# --------------------------------------------------------------------------
# This test confirms one-shot runtime returns a distinct status when the
# safety-net blocked state could not be persisted safely.
# --------------------------------------------------------------------------
    def test_run_one_shot_runtime_returns_safety_net_persist_failure_status(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG = self._create_config(Path(TMPDIR))
            CLIENT = MagicMock()
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            LOG_FILE = Path(TMPDIR) / "worker.log"
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            AUTH_STATE = AuthState("2026-03-15T10:00:00+00:00", False, False, "none")

            with patch(
                "app.runtime.enforce_safety_net",
                return_value=SafetyNetEnforcementResult(False, False),
            ):
                EXIT_CODE, STOP_STATUS = run_one_shot_runtime(
                    CONFIG,
                    CLIENT,
                    AUTH_STATE,
                    True,
                    TELEGRAM,
                    LOG_FILE,
                    BUILD_DETAIL,
                )

            self.assertEqual(EXIT_CODE, 5)
            self.assertIn("failed to persist safety-net state", STOP_STATUS)

# --------------------------------------------------------------------------
# This test confirms one-shot runtime performs the backup and returns success
# when auth and safety checks are already satisfied.
# --------------------------------------------------------------------------
    def test_run_one_shot_runtime_runs_backup_on_success(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG = self._create_config(Path(TMPDIR))
            CLIENT = MagicMock()
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            LOG_FILE = Path(TMPDIR) / "worker.log"
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            AUTH_STATE = AuthState("2026-03-15T10:00:00+00:00", False, False, "none")

            with patch(
                "app.runtime.enforce_safety_net",
                return_value=SafetyNetEnforcementResult(True, False),
            ):
                with patch("app.runtime.run_backup") as RUN_BACKUP:
                    EXIT_CODE, STOP_STATUS = run_one_shot_runtime(
                        CONFIG,
                        CLIENT,
                        AUTH_STATE,
                        True,
                        TELEGRAM,
                        LOG_FILE,
                        BUILD_DETAIL,
                    )

            self.assertEqual(EXIT_CODE, 0)
            self.assertIn("Run completed", STOP_STATUS)
            RUN_BACKUP.assert_called_once()

# --------------------------------------------------------------------------
# This test confirms the persistent runtime exits the current iteration after
# logging an auth command result.
# --------------------------------------------------------------------------
    def test_run_persistent_runtime_logs_auth_command_details_then_stops(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG = self._create_config(Path(TMPDIR))
            CLIENT = MagicMock()
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            LOG_FILE = Path(TMPDIR) / "worker.log"
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            AUTH_STATE = AuthState("2026-03-15T10:00:00+00:00", False, False, "none")
            OUTCOME = CommandOutcome(AUTH_STATE, True, False, "auth ok")

            with patch("app.runtime.process_reauth_reminders", return_value=AUTH_STATE):
                with patch("app.runtime.process_commands", return_value=([("auth", "123456")], 9)):
                    with patch("app.runtime.handle_command", return_value=OUTCOME):
                        with patch(
                            "app.runtime.enforce_safety_net",
                            return_value=SafetyNetEnforcementResult(True, False),
                        ):
                            with patch("app.runtime.run_backup", side_effect=RuntimeError("stop loop")):
                                with patch("app.runtime.time.time", side_effect=[100, 100, 100]):
                                    with self.assertRaisesRegex(RuntimeError, "stop loop"):
                                        run_persistent_runtime(
                                            CONFIG,
                                            CLIENT,
                                            AUTH_STATE,
                                            False,
                                            TELEGRAM,
                                            LOG_FILE,
                                            BUILD_DETAIL,
                                        )

            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertIn("Auth command result: auth ok", LOG_TEXT)

# --------------------------------------------------------------------------
# This test confirms the persistent runtime logs the initial and recalculated
# next scheduled run times for schedule debugging.
# --------------------------------------------------------------------------
    def test_run_persistent_runtime_logs_next_run_times(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG = self._create_config(Path(TMPDIR))
            CONFIG = replace(CONFIG, schedule_mode="interval", schedule_interval_minutes=15)
            CLIENT = MagicMock()
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            LOG_FILE = Path(TMPDIR) / "worker.log"
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            AUTH_STATE = AuthState("2026-03-15T10:00:00+00:00", False, False, "none")

            with patch("app.runtime.process_reauth_reminders", return_value=AUTH_STATE):
                with patch("app.runtime.process_commands", return_value=([], None)):
                    with patch(
                        "app.runtime.enforce_safety_net",
                        return_value=SafetyNetEnforcementResult(True, False),
                    ):
                        with patch("app.runtime.run_backup", side_effect=RuntimeError("stop loop")):
                            with patch("app.runtime.get_next_run_epoch", return_value=300):
                                with patch("app.runtime.time.time", side_effect=[100, 100]):
                                    with self.assertRaisesRegex(RuntimeError, "stop loop"):
                                        run_persistent_runtime(
                                            CONFIG,
                                            CLIENT,
                                            AUTH_STATE,
                                            True,
                                            TELEGRAM,
                                            LOG_FILE,
                                            BUILD_DETAIL,
                                        )

            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertIn("Next scheduled run:", LOG_TEXT)
            self.assertIn("Next scheduled run recalculated:", LOG_TEXT)

# --------------------------------------------------------------------------
# This test confirms the persistent runtime skips scheduled work when auth is
# incomplete.
# --------------------------------------------------------------------------
    def test_run_persistent_runtime_skips_when_not_authenticated(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG = self._create_config(Path(TMPDIR))
            CLIENT = MagicMock()
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            LOG_FILE = Path(TMPDIR) / "worker.log"
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            AUTH_STATE = AuthState("2026-03-15T10:00:00+00:00", False, False, "none")

            with patch("app.runtime.process_reauth_reminders", return_value=AUTH_STATE):
                with patch("app.runtime.process_commands", return_value=([], None)):
                    with patch("app.runtime.notify") as NOTIFY:
                        with patch("app.runtime.time.sleep", side_effect=RuntimeError("stop loop")):
                            with patch("app.runtime.time.time", side_effect=[100, 100, 101]):
                                with self.assertRaisesRegex(RuntimeError, "stop loop"):
                                    run_persistent_runtime(
                                        CONFIG,
                                        CLIENT,
                                        AUTH_STATE,
                                        False,
                                        TELEGRAM,
                                        LOG_FILE,
                                        BUILD_DETAIL,
                                    )

            NOTIFY.assert_called_once()

# --------------------------------------------------------------------------
# This test confirms the persistent runtime skips scheduled work when reauth
# remains pending.
# --------------------------------------------------------------------------
    def test_run_persistent_runtime_skips_when_reauth_pending(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG = self._create_config(Path(TMPDIR))
            CLIENT = MagicMock()
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            LOG_FILE = Path(TMPDIR) / "worker.log"
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            AUTH_STATE = AuthState("2026-03-15T10:00:00+00:00", False, True, "prompt2")

            with patch("app.runtime.process_reauth_reminders", return_value=AUTH_STATE):
                with patch("app.runtime.process_commands", return_value=([], None)):
                    with patch("app.runtime.notify") as NOTIFY:
                        with patch("app.runtime.time.sleep", side_effect=RuntimeError("stop loop")):
                            with patch("app.runtime.time.time", side_effect=[100, 100, 101]):
                                with self.assertRaisesRegex(RuntimeError, "stop loop"):
                                    run_persistent_runtime(
                                        CONFIG,
                                        CLIENT,
                                        AUTH_STATE,
                                        True,
                                        TELEGRAM,
                                        LOG_FILE,
                                        BUILD_DETAIL,
                                    )

            NOTIFY.assert_called_once()

# --------------------------------------------------------------------------
# This test confirms the persistent runtime pauses and retries when the safety
# net blocks the run.
# --------------------------------------------------------------------------
    def test_run_persistent_runtime_retries_when_safety_net_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG = self._create_config(Path(TMPDIR))
            CLIENT = MagicMock()
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            LOG_FILE = Path(TMPDIR) / "worker.log"
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            AUTH_STATE = AuthState("2026-03-15T10:00:00+00:00", False, False, "none")

            with patch("app.runtime.process_reauth_reminders", return_value=AUTH_STATE):
                with patch("app.runtime.process_commands", return_value=([], None)):
                    with patch(
                        "app.runtime.enforce_safety_net",
                        return_value=SafetyNetEnforcementResult(False, True),
                    ):
                        with patch("app.runtime.time.sleep", side_effect=RuntimeError("stop loop")):
                            with patch("app.runtime.time.time", side_effect=[100, 100, 101]):
                                with self.assertRaisesRegex(RuntimeError, "stop loop"):
                                    run_persistent_runtime(
                                        CONFIG,
                                        CLIENT,
                                        AUTH_STATE,
                                        True,
                                        TELEGRAM,
                                        LOG_FILE,
                                        BUILD_DETAIL,
                                    )

# --------------------------------------------------------------------------
# This test confirms a safety-net persistence failure stops the persistent
# runtime instead of retrying without a stored blocked-state marker.
# --------------------------------------------------------------------------
    def test_run_persistent_runtime_raises_on_safety_net_persist_failure(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG = self._create_config(Path(TMPDIR))
            CLIENT = MagicMock()
            TELEGRAM = TelegramConfig(bot_token="", chat_id="")
            LOG_FILE = Path(TMPDIR) / "worker.log"
            BUILD_DETAIL = {"app_build_ref": "abc123", "pyicloud_version": "2.4.1"}
            AUTH_STATE = AuthState("2026-03-15T10:00:00+00:00", False, False, "none")

            with patch("app.runtime.process_reauth_reminders", return_value=AUTH_STATE):
                with patch("app.runtime.process_commands", return_value=([], None)):
                    with patch(
                        "app.runtime.enforce_safety_net",
                        return_value=SafetyNetEnforcementResult(False, False),
                    ):
                        with patch("app.runtime.time.time", side_effect=[100, 100]):
                            with self.assertRaisesRegex(
                                RuntimeError,
                                "Safety-net state persistence failed.",
                            ):
                                run_persistent_runtime(
                                    CONFIG,
                                    CLIENT,
                                    AUTH_STATE,
                                    True,
                                    TELEGRAM,
                                    LOG_FILE,
                                    BUILD_DETAIL,
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
