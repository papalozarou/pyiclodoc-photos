# ------------------------------------------------------------------------------
# This test module verifies cached logger settings and throttled rotation
# checks.
# ------------------------------------------------------------------------------

from __future__ import annotations

from pathlib import Path
import os
import tempfile
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app import logger


# ------------------------------------------------------------------------------
# These tests verify logger state caching and rotation throttling behaviour.
# ------------------------------------------------------------------------------
class TestLogger(unittest.TestCase):
# --------------------------------------------------------------------------
# This function resets module logger state and environment drift between
# tests.
# --------------------------------------------------------------------------
    def setUp(self) -> None:
        self.original_log_level = logger.os.getenv("LOG_LEVEL")
        self.original_rotate_mib = logger.os.getenv("LOG_ROTATE_MAX_MIB")
        self.original_rotate_daily = logger.os.getenv("LOG_ROTATE_DAILY")
        self.original_keep_days = logger.os.getenv("LOG_ROTATE_KEEP_DAYS")
        logger.reset_logger_state()

# --------------------------------------------------------------------------
# This function restores module logger state after each test.
# --------------------------------------------------------------------------
    def tearDown(self) -> None:
        self._restore_env("LOG_LEVEL", self.original_log_level)
        self._restore_env("LOG_ROTATE_MAX_MIB", self.original_rotate_mib)
        self._restore_env("LOG_ROTATE_DAILY", self.original_rotate_daily)
        self._restore_env("LOG_ROTATE_KEEP_DAYS", self.original_keep_days)
        logger.reset_logger_state()

# --------------------------------------------------------------------------
# This test confirms logger settings are cached until relevant env values
# change.
# --------------------------------------------------------------------------
    def test_get_logger_settings_invalidates_on_env_change(self) -> None:
        logger.os.environ["LOG_LEVEL"] = "info"
        FIRST = logger.get_logger_settings()
        SECOND = logger.get_logger_settings()

        logger.os.environ["LOG_LEVEL"] = "debug"
        THIRD = logger.get_logger_settings()

        self.assertIs(FIRST, SECOND)
        self.assertEqual(FIRST.log_level, "info")
        self.assertEqual(THIRD.log_level, "debug")

# --------------------------------------------------------------------------
# This test confirms the parser helpers apply defaults and valid overrides.
# --------------------------------------------------------------------------
    def test_parser_helpers_apply_defaults_and_valid_values(self) -> None:
        self.assertEqual(logger.parse_log_level(" debug "), "debug")
        self.assertEqual(logger.parse_log_level("weird"), "info")
        self.assertEqual(logger.parse_log_rotate_max_bytes("2"), 2 * 1024 * 1024)
        self.assertEqual(logger.parse_log_rotate_max_bytes("0"), 100 * 1024 * 1024)
        self.assertTrue(logger.parse_log_rotate_daily("yes"))
        self.assertFalse(logger.parse_log_rotate_daily("off"))
        self.assertTrue(logger.parse_log_rotate_daily("weird"))
        self.assertEqual(logger.parse_log_rotate_keep_days("7"), 7)
        self.assertEqual(logger.parse_log_rotate_keep_days("0"), 14)

# --------------------------------------------------------------------------
# This test confirms get_settings_signature reads the raw logger env values.
# --------------------------------------------------------------------------
    def test_get_settings_signature_reflects_environment(self) -> None:
        with patch.dict(
            logger.os.environ,
            {
                "LOG_LEVEL": "debug",
                "LOG_ROTATE_MAX_MIB": "9",
                "LOG_ROTATE_DAILY": "false",
                "LOG_ROTATE_KEEP_DAYS": "3",
            },
            clear=False,
        ):
            self.assertEqual(
                logger.get_settings_signature(),
                ("debug", "9", "false", "3"),
            )

# --------------------------------------------------------------------------
# This test confirms should_log respects the configured severity threshold.
# --------------------------------------------------------------------------
    def test_should_log_honours_threshold(self) -> None:
        with patch("app.logger.get_logger_settings", return_value=logger.LoggerSettings("error", 1, True, 1)):
            self.assertFalse(logger.should_log("info"))
            self.assertTrue(logger.should_log("error"))

# --------------------------------------------------------------------------
# This test confirms console formatting colours only error lines.
# --------------------------------------------------------------------------
    def test_format_console_line_colours_only_errors(self) -> None:
        self.assertEqual(logger.format_console_line("hello", "INFO"), "hello")
        self.assertIn("\033[31m", logger.format_console_line("boom", "ERROR"))

# --------------------------------------------------------------------------
# This test confirms log_line prints and appends when the level is enabled.
# --------------------------------------------------------------------------
    def test_log_line_prints_and_appends_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"

            with patch("app.logger.should_log", return_value=True):
                with patch("app.logger.rotate_log_if_needed") as ROTATE:
                    with patch("app.logger.get_timestamp", return_value="2026-03-15 10:00:00 UTC"):
                        with patch("app.logger.print") as PRINT:
                            logger.log_line(LOG_FILE, "info", "hello")

            self.assertEqual(LOG_FILE.read_text(encoding="utf-8"), "[2026-03-15 10:00:00 UTC] [INFO] hello\n")
            ROTATE.assert_called_once_with(LOG_FILE)
            PRINT.assert_called_once()

# --------------------------------------------------------------------------
# This test confirms log_line returns early when the level is disabled.
# --------------------------------------------------------------------------
    def test_log_line_returns_early_when_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"

            with patch("app.logger.should_log", return_value=False):
                with patch("app.logger.print") as PRINT:
                    logger.log_line(LOG_FILE, "debug", "hello")

            self.assertFalse(LOG_FILE.exists())
            PRINT.assert_not_called()

# --------------------------------------------------------------------------
# This test confirms rotation checks are throttled for repeated writes to the
# same file within the configured interval.
# --------------------------------------------------------------------------
    def test_rotate_log_if_needed_throttles_repeated_checks(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"
            LOG_FILE.write_text("hello\n", encoding="utf-8")

            with patch("app.logger.now_local") as mock_now_local:
                FIRST_TIME = logger.datetime(2026, 3, 15, 10, 0, 0)
                mock_now_local.return_value = FIRST_TIME

                with patch("app.logger.should_rotate_for_size", return_value=False) as mock_size:
                    with patch("app.logger.should_rotate_for_daily_rollover", return_value=False) as mock_daily:
                        logger.rotate_log_if_needed(LOG_FILE)
                        logger.rotate_log_if_needed(LOG_FILE)

                self.assertEqual(mock_size.call_count, 1)
                self.assertEqual(mock_daily.call_count, 1)

# --------------------------------------------------------------------------
# This test confirms should_check_rotation updates per-file check state.
# --------------------------------------------------------------------------
    def test_should_check_rotation_updates_state(self) -> None:
        LOG_FILE = Path("/tmp/worker.log")

        with patch("app.logger.now_local") as NOW_LOCAL:
            NOW_LOCAL.return_value = logger.datetime(2026, 3, 15, 10, 0, 0)
            self.assertTrue(logger.should_check_rotation(LOG_FILE))
            self.assertFalse(logger.should_check_rotation(LOG_FILE))

# --------------------------------------------------------------------------
# This test confirms size-based rotation handles both true and OSError paths.
# --------------------------------------------------------------------------
    def test_should_rotate_for_size_handles_threshold_and_oserror(self) -> None:
        SETTINGS = logger.LoggerSettings("info", 10, True, 14)

        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"
            LOG_FILE.write_text("1234567890", encoding="utf-8")
            self.assertTrue(logger.should_rotate_for_size(LOG_FILE, SETTINGS))

        with patch("pathlib.Path.stat", side_effect=OSError("denied")):
            self.assertFalse(logger.should_rotate_for_size(Path("/tmp/worker.log"), SETTINGS))

# --------------------------------------------------------------------------
# This test confirms daily rollover detection honours the rotate_daily flag
# and date comparison.
# --------------------------------------------------------------------------
    def test_should_rotate_for_daily_rollover_handles_disabled_and_date_change(self) -> None:
        DISABLED = logger.LoggerSettings("info", 10, False, 14)
        ENABLED = logger.LoggerSettings("info", 10, True, 14)

        self.assertFalse(logger.should_rotate_for_daily_rollover(Path("/tmp/worker.log"), DISABLED))

        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"
            LOG_FILE.write_text("x", encoding="utf-8")

            OLD_NOW = logger.datetime(2026, 3, 16, 10, 0, 0)
            OLD_MTIME = logger.datetime(2026, 3, 15, 10, 0, 0).timestamp()

            with patch("app.logger.now_local", return_value=OLD_NOW):
                with patch("pathlib.Path.stat", return_value=type("Stat", (), {"st_mtime": OLD_MTIME})()):
                    self.assertTrue(logger.should_rotate_for_daily_rollover(LOG_FILE, ENABLED))

        with patch("pathlib.Path.stat", side_effect=OSError("denied")):
            with patch("app.logger.now_local", return_value=logger.datetime(2026, 3, 16, 10, 0, 0)):
                self.assertFalse(logger.should_rotate_for_daily_rollover(Path("/tmp/worker.log"), ENABLED))

# --------------------------------------------------------------------------
# This test confirms rotate_log_if_needed returns early when the log file is
# missing, throttled, or not due for rotation.
# --------------------------------------------------------------------------
    def test_rotate_log_if_needed_returns_early_for_non_rotation_cases(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"

            logger.rotate_log_if_needed(LOG_FILE)
            self.assertFalse(LOG_FILE.exists())

            LOG_FILE.write_text("x", encoding="utf-8")

            with patch("app.logger.should_check_rotation", return_value=False):
                with patch("app.logger.rotate_log_file") as ROTATE:
                    logger.rotate_log_if_needed(LOG_FILE)
                    ROTATE.assert_not_called()

            with patch("app.logger.should_check_rotation", return_value=True):
                with patch("app.logger.should_rotate_for_size", return_value=False):
                    with patch("app.logger.should_rotate_for_daily_rollover", return_value=False):
                        with patch("app.logger.rotate_log_file") as ROTATE:
                            logger.rotate_log_if_needed(LOG_FILE)
                            ROTATE.assert_not_called()

# --------------------------------------------------------------------------
# This test confirms rotate_log_if_needed rotates and prunes when either
# rotation policy triggers.
# --------------------------------------------------------------------------
    def test_rotate_log_if_needed_calls_rotate_and_prune(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"
            LOG_FILE.write_text("x", encoding="utf-8")
            SETTINGS = logger.LoggerSettings("info", 10, True, 14)

            with patch("app.logger.should_check_rotation", return_value=True):
                with patch("app.logger.get_logger_settings", return_value=SETTINGS):
                    with patch("app.logger.should_rotate_for_size", return_value=True):
                        with patch("app.logger.rotate_log_file") as ROTATE:
                            with patch("app.logger.prune_rotated_logs") as PRUNE:
                                logger.rotate_log_if_needed(LOG_FILE)

            ROTATE.assert_called_once_with(LOG_FILE)
            PRUNE.assert_called_once_with(LOG_FILE, SETTINGS)

# --------------------------------------------------------------------------
# This test confirms rotate_log_file compresses the rotated log and removes the
# temporary plain-text rotated file.
# --------------------------------------------------------------------------
    def test_rotate_log_file_creates_gzip_archive(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"
            LOG_FILE.write_text("hello\n", encoding="utf-8")

            with patch("app.logger.now_local", return_value=logger.datetime(2026, 3, 15, 10, 0, 0)):
                logger.rotate_log_file(LOG_FILE)

            self.assertFalse(LOG_FILE.exists())
            self.assertTrue((Path(TMPDIR) / "worker.20260315-100000.log.gz").exists())

# --------------------------------------------------------------------------
# This test confirms rotate_log_file returns quietly when replace or compress
# steps fail.
# --------------------------------------------------------------------------
    def test_rotate_log_file_returns_quietly_on_failures(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"
            LOG_FILE.write_text("hello\n", encoding="utf-8")

            with patch("pathlib.Path.replace", side_effect=OSError("denied")):
                logger.rotate_log_file(LOG_FILE)

            self.assertTrue(LOG_FILE.exists())

        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"
            LOG_FILE.write_text("hello\n", encoding="utf-8")

            with patch("app.logger.now_local", return_value=logger.datetime(2026, 3, 15, 10, 0, 0)):
                with patch("gzip.open", side_effect=OSError("denied")):
                    logger.rotate_log_file(LOG_FILE)

            self.assertTrue((Path(TMPDIR) / "worker.20260315-100000.log").exists())

# --------------------------------------------------------------------------
# This test confirms prune_rotated_logs removes only files older than the
# retention cutoff and ignores filesystem errors.
# --------------------------------------------------------------------------
    def test_prune_rotated_logs_removes_only_expired_archives(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"
            LOG_FILE.write_text("hello\n", encoding="utf-8")
            OLD_ARCHIVE = Path(TMPDIR) / "worker.20260301-100000.log.gz"
            NEW_ARCHIVE = Path(TMPDIR) / "worker.20260314-100000.log.gz"
            OLD_ARCHIVE.write_text("old", encoding="utf-8")
            NEW_ARCHIVE.write_text("new", encoding="utf-8")
            SETTINGS = logger.LoggerSettings("info", 10, True, 7)
            NOW = logger.datetime(2026, 3, 15, 10, 0, 0)
            os.utime(
                OLD_ARCHIVE,
                (
                    logger.datetime(2026, 3, 1, 10, 0, 0).timestamp(),
                    logger.datetime(2026, 3, 1, 10, 0, 0).timestamp(),
                ),
            )
            os.utime(
                NEW_ARCHIVE,
                (
                    logger.datetime(2026, 3, 14, 10, 0, 0).timestamp(),
                    logger.datetime(2026, 3, 14, 10, 0, 0).timestamp(),
                ),
            )

            with patch("app.logger.now_local", return_value=NOW):
                logger.prune_rotated_logs(LOG_FILE, SETTINGS)

            self.assertFalse(OLD_ARCHIVE.exists())
            self.assertTrue(NEW_ARCHIVE.exists())

# --------------------------------------------------------------------------
# This test confirms prune_rotated_logs returns early when retention is
# disabled.
# --------------------------------------------------------------------------
    def test_prune_rotated_logs_returns_early_when_keep_days_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"
            SETTINGS = logger.LoggerSettings("info", 10, True, 0)

            with patch("pathlib.Path.glob") as GLOB:
                logger.prune_rotated_logs(LOG_FILE, SETTINGS)

            GLOB.assert_not_called()

# --------------------------------------------------------------------------
# This helper restores one environment variable to its original state.
# --------------------------------------------------------------------------
    def _restore_env(self, NAME: str, VALUE: str | None) -> None:
        if VALUE is None:
            logger.os.environ.pop(NAME, None)
            return

        logger.os.environ[NAME] = VALUE
