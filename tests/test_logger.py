# ------------------------------------------------------------------------------
# This test module verifies cached logger settings and throttled rotation
# checks.
# ------------------------------------------------------------------------------

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
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
# This helper restores one environment variable to its original state.
# --------------------------------------------------------------------------
    def _restore_env(self, NAME: str, VALUE: str | None) -> None:
        if VALUE is None:
            logger.os.environ.pop(NAME, None)
            return

        logger.os.environ[NAME] = VALUE
