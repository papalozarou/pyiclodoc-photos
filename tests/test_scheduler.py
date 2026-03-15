# ------------------------------------------------------------------------------
# This test module verifies extracted scheduler parsing and next-run behaviour.
# ------------------------------------------------------------------------------

from datetime import datetime, timezone
from pathlib import Path
import unittest
from unittest.mock import patch

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.config import AppConfig
from app.scheduler import (
    calculate_next_daily_run_epoch,
    calculate_next_monthly_run_epoch,
    calculate_next_twice_weekly_run_epoch,
    calculate_next_weekly_run_epoch,
    format_schedule_line,
    get_monthly_weekday_day,
    get_next_run_epoch,
    parse_daily,
    parse_weekday,
    parse_weekday_list,
    validate_schedule_config,
)


# ------------------------------------------------------------------------------
# This function builds a minimal app config for scheduler tests.
#
# Returns: Valid application configuration with overridable schedule fields.
# ------------------------------------------------------------------------------
def create_config(**OVERRIDES) -> AppConfig:
    DEFAULTS = {
        "container_username": "alice",
        "icloud_email": "alice@example.com",
        "icloud_password": "secret",
        "telegram_bot_token": "token",
        "telegram_chat_id": "1",
        "keychain_service_name": "icloud-photos-backup",
        "run_once": False,
        "schedule_mode": "interval",
        "schedule_backup_time": "02:00",
        "schedule_weekdays": "monday",
        "schedule_monthly_week": "first",
        "schedule_interval_minutes": 1440,
        "backup_discovery_mode": "full",
        "backup_until_found_count": 50,
        "backup_delete_removed": False,
        "sync_workers": 0,
        "download_chunk_mib": 4,
        "reauth_interval_days": 30,
        "output_dir": Path("/tmp/output"),
        "config_dir": Path("/tmp/config"),
        "logs_dir": Path("/tmp/logs"),
        "manifest_path": Path("/tmp/config/pyiclodoc-photos-manifest.json"),
        "auth_state_path": Path("/tmp/config/pyiclodoc-photos-auth_state.json"),
        "heartbeat_path": Path("/tmp/logs/pyiclodoc-photos-heartbeat.txt"),
        "cookie_dir": Path("/tmp/config/cookies"),
        "session_dir": Path("/tmp/config/session"),
        "icloudpd_compat_dir": Path("/tmp/config/icloudpd"),
        "safety_net_sample_size": 200,
        "backup_albums_enabled": True,
        "backup_album_links_mode": "hardlink",
        "backup_include_shared_albums": True,
        "backup_include_favourites": True,
        "backup_root_library": "library",
        "backup_root_albums": "albums",
    }
    DEFAULTS.update(OVERRIDES)
    return AppConfig(**DEFAULTS)


# ------------------------------------------------------------------------------
# These tests verify scheduler validation, parsing, and notification wording.
# ------------------------------------------------------------------------------
class TestScheduler(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms invalid daily times fail parsing.
# --------------------------------------------------------------------------
    def test_parse_daily_rejects_invalid_value(self) -> None:
        self.assertIsNone(parse_daily("25:61"))
        self.assertIsNone(parse_daily("10"))
        self.assertIsNone(parse_daily("aa:30"))
        self.assertIsNone(parse_daily("10:aa"))

# --------------------------------------------------------------------------
# This test confirms valid daily times parse into hour and minute values.
# --------------------------------------------------------------------------
    def test_parse_daily_accepts_valid_value(self) -> None:
        self.assertEqual(parse_daily("09:30"), (9, 30))

# --------------------------------------------------------------------------
# This test confirms weekday parsing normalises case and whitespace.
# --------------------------------------------------------------------------
    def test_parse_weekday_accepts_valid_value(self) -> None:
        self.assertEqual(parse_weekday(" Monday "), 0)
        self.assertIsNone(parse_weekday("funday"))

# --------------------------------------------------------------------------
# This test confirms duplicate weekday entries fail strict list parsing.
# --------------------------------------------------------------------------
    def test_parse_weekday_list_rejects_duplicates(self) -> None:
        self.assertIsNone(parse_weekday_list("monday,monday", 2))
        self.assertIsNone(parse_weekday_list("monday", 2))
        self.assertIsNone(parse_weekday_list("monday,funday", 2))

# --------------------------------------------------------------------------
# This test confirms strict weekday parsing preserves valid order.
# --------------------------------------------------------------------------
    def test_parse_weekday_list_accepts_valid_values(self) -> None:
        self.assertEqual(parse_weekday_list("thursday,monday", 2), [3, 0])

# --------------------------------------------------------------------------
# This test confirms daily scheduling rolls to the following day when the
# target time has already passed.
# --------------------------------------------------------------------------
    def test_calculate_next_daily_run_epoch_rolls_forward(self) -> None:
        NOW_LOCAL = datetime(2026, 3, 15, 10, 30, tzinfo=timezone.utc)
        NEXT_EPOCH = calculate_next_daily_run_epoch(NOW_LOCAL, "09:00")

        self.assertEqual(
            NEXT_EPOCH,
            int(datetime(2026, 3, 16, 9, 0, tzinfo=timezone.utc).timestamp()),
        )

# --------------------------------------------------------------------------
# This test confirms daily scheduling returns the current epoch baseline when
# the configured daily time is invalid.
# --------------------------------------------------------------------------
    def test_calculate_next_daily_run_epoch_returns_now_for_invalid_time(self) -> None:
        NOW_LOCAL = datetime(2026, 3, 15, 10, 30, tzinfo=timezone.utc)

        self.assertEqual(
            calculate_next_daily_run_epoch(NOW_LOCAL, "bad"),
            int(NOW_LOCAL.timestamp()),
        )

# --------------------------------------------------------------------------
# This test confirms weekly scheduling picks the next matching weekday and
# rolls to the following week when today's slot has passed.
# --------------------------------------------------------------------------
    def test_calculate_next_weekly_run_epoch_rolls_forward(self) -> None:
        NOW_LOCAL = datetime(2026, 3, 16, 10, 30, tzinfo=timezone.utc)
        NEXT_EPOCH = calculate_next_weekly_run_epoch(NOW_LOCAL, "monday", "09:00")

        self.assertEqual(
            NEXT_EPOCH,
            int(datetime(2026, 3, 23, 9, 0, tzinfo=timezone.utc).timestamp()),
        )

# --------------------------------------------------------------------------
# This test confirms weekly scheduling returns the current epoch baseline when
# the weekday or time is invalid.
# --------------------------------------------------------------------------
    def test_calculate_next_weekly_run_epoch_returns_now_for_invalid_input(self) -> None:
        NOW_LOCAL = datetime(2026, 3, 16, 10, 30, tzinfo=timezone.utc)

        self.assertEqual(
            calculate_next_weekly_run_epoch(NOW_LOCAL, "funday", "09:00"),
            int(NOW_LOCAL.timestamp()),
        )
        self.assertEqual(
            calculate_next_weekly_run_epoch(NOW_LOCAL, "monday", "bad"),
            int(NOW_LOCAL.timestamp()),
        )

# --------------------------------------------------------------------------
# This test confirms twice-weekly scheduling picks the earliest upcoming slot.
# --------------------------------------------------------------------------
    def test_calculate_next_twice_weekly_run_epoch_picks_earliest_candidate(self) -> None:
        NOW_LOCAL = datetime(2026, 3, 16, 10, 30, tzinfo=timezone.utc)
        NEXT_EPOCH = calculate_next_twice_weekly_run_epoch(
            NOW_LOCAL,
            "thursday,tuesday",
            "09:00",
        )

        self.assertEqual(
            NEXT_EPOCH,
            int(datetime(2026, 3, 17, 9, 0, tzinfo=timezone.utc).timestamp()),
        )

# --------------------------------------------------------------------------
# This test confirms twice-weekly scheduling returns the current epoch
# baseline when the weekday list is invalid.
# --------------------------------------------------------------------------
    def test_calculate_next_twice_weekly_run_epoch_returns_now_for_invalid_weekdays(self) -> None:
        NOW_LOCAL = datetime(2026, 3, 16, 10, 30, tzinfo=timezone.utc)

        self.assertEqual(
            calculate_next_twice_weekly_run_epoch(NOW_LOCAL, "monday", "09:00"),
            int(NOW_LOCAL.timestamp()),
        )

# --------------------------------------------------------------------------
# This test confirms monthly weekday lookup supports ordinal and last-week
# selection.
# --------------------------------------------------------------------------
    def test_get_monthly_weekday_day_supports_first_and_last(self) -> None:
        self.assertEqual(get_monthly_weekday_day(2026, 3, 0, "first"), 2)
        self.assertEqual(get_monthly_weekday_day(2026, 3, 0, "last"), 30)
        self.assertIsNone(get_monthly_weekday_day(2026, 3, 0, "fifth"))

# --------------------------------------------------------------------------
# This test confirms monthly scheduling rolls into the next month when the
# current month's slot has already passed.
# --------------------------------------------------------------------------
    def test_calculate_next_monthly_run_epoch_rolls_into_next_month(self) -> None:
        NOW_LOCAL = datetime(2026, 3, 31, 10, 30, tzinfo=timezone.utc)
        NEXT_EPOCH = calculate_next_monthly_run_epoch(
            NOW_LOCAL,
            "monday",
            "first",
            "09:00",
        )

        self.assertEqual(
            NEXT_EPOCH,
            int(datetime(2026, 4, 6, 9, 0, tzinfo=timezone.utc).timestamp()),
        )

# --------------------------------------------------------------------------
# This test confirms monthly scheduling returns the current epoch baseline
# when the weekday or time is invalid.
# --------------------------------------------------------------------------
    def test_calculate_next_monthly_run_epoch_returns_now_for_invalid_input(self) -> None:
        NOW_LOCAL = datetime(2026, 3, 31, 10, 30, tzinfo=timezone.utc)

        self.assertEqual(
            calculate_next_monthly_run_epoch(NOW_LOCAL, "funday", "first", "09:00"),
            int(NOW_LOCAL.timestamp()),
        )
        self.assertEqual(
            calculate_next_monthly_run_epoch(NOW_LOCAL, "monday", "first", "bad"),
            int(NOW_LOCAL.timestamp()),
        )

# --------------------------------------------------------------------------
# This test confirms monthly schedule validation rejects unsupported week
# names.
# --------------------------------------------------------------------------
    def test_validate_schedule_config_rejects_invalid_monthly_week(self) -> None:
        CONFIG = create_config(schedule_mode="monthly", schedule_monthly_week="fifth")

        self.assertIn(
            "SCHEDULE_MONTHLY_WEEK must be one of: first, second, third, fourth, last.",
            validate_schedule_config(CONFIG),
        )

# --------------------------------------------------------------------------
# This test confirms schedule validation reports mode-specific weekday and
# time failures.
# --------------------------------------------------------------------------
    def test_validate_schedule_config_reports_mode_specific_errors(self) -> None:
        DAILY_CONFIG = create_config(schedule_mode="daily", schedule_backup_time="bad")
        WEEKLY_CONFIG = create_config(schedule_mode="weekly", schedule_weekdays="monday,tuesday")
        TWICE_CONFIG = create_config(schedule_mode="twice_weekly", schedule_weekdays="monday")
        MONTHLY_CONFIG = create_config(
            schedule_mode="monthly",
            schedule_weekdays="funday",
            schedule_backup_time="bad",
        )
        INTERVAL_CONFIG = create_config(schedule_mode="interval", schedule_interval_minutes=0)

        self.assertIn(
            "SCHEDULE_BACKUP_TIME must use 24-hour HH:MM format.",
            validate_schedule_config(DAILY_CONFIG),
        )
        self.assertIn(
            "SCHEDULE_WEEKDAYS must contain exactly one valid weekday name for weekly mode.",
            validate_schedule_config(WEEKLY_CONFIG),
        )
        self.assertIn(
            "SCHEDULE_WEEKDAYS must contain exactly two distinct weekday names.",
            validate_schedule_config(TWICE_CONFIG),
        )
        MONTHLY_ERRORS = validate_schedule_config(MONTHLY_CONFIG)
        self.assertIn(
            "SCHEDULE_WEEKDAYS must contain exactly one valid weekday name for monthly mode.",
            MONTHLY_ERRORS,
        )
        self.assertIn(
            "SCHEDULE_BACKUP_TIME must use 24-hour HH:MM format.",
            MONTHLY_ERRORS,
        )
        self.assertIn(
            "SCHEDULE_INTERVAL_MINUTES must be at least 1 when RUN_ONCE is false.",
            validate_schedule_config(INTERVAL_CONFIG),
        )

# --------------------------------------------------------------------------
# This test confirms unknown schedule modes are rejected explicitly.
# --------------------------------------------------------------------------
    def test_validate_schedule_config_rejects_unknown_mode(self) -> None:
        CONFIG = create_config(schedule_mode="weird")

        self.assertIn(
            "SCHEDULE_MODE must be one of: interval, daily, weekly, twice_weekly, monthly.",
            validate_schedule_config(CONFIG),
        )

# --------------------------------------------------------------------------
# This test confirms manual trigger text uses the extracted scheduler wording.
# --------------------------------------------------------------------------
    def test_format_schedule_line_formats_manual_trigger(self) -> None:
        CONFIG = create_config(schedule_mode="weekly", schedule_weekdays="monday")

        self.assertEqual(
            format_schedule_line(CONFIG, "manual"),
            "Manual, then weekly on monday at 02:00.",
        )

# --------------------------------------------------------------------------
# This test confirms schedule line formatting covers scheduled and one-shot
# variants.
# --------------------------------------------------------------------------
    def test_format_schedule_line_covers_other_trigger_variants(self) -> None:
        DAILY_CONFIG = create_config(schedule_mode="daily", schedule_backup_time="03:15")
        ONE_SHOT_CONFIG = create_config(schedule_mode="interval", schedule_interval_minutes=15)

        self.assertEqual(
            format_schedule_line(DAILY_CONFIG, "scheduled"),
            "Scheduled daily at 03:15.",
        )
        self.assertEqual(
            format_schedule_line(ONE_SHOT_CONFIG, "one-shot"),
            "One-shot run – configured schedule is ignored.",
        )

# --------------------------------------------------------------------------
# This test confirms interval scheduling uses the provided epoch baseline.
# --------------------------------------------------------------------------
    def test_get_next_run_epoch_uses_interval_offset(self) -> None:
        CONFIG = create_config(schedule_mode="interval", schedule_interval_minutes=30)

        self.assertEqual(get_next_run_epoch(CONFIG, 100), 1900)

# --------------------------------------------------------------------------
# This test confirms next-run selection delegates correctly for the non-
# interval schedule modes.
# --------------------------------------------------------------------------
    def test_get_next_run_epoch_delegates_by_mode(self) -> None:
        DAILY_CONFIG = create_config(schedule_mode="daily", schedule_backup_time="09:00")
        WEEKLY_CONFIG = create_config(schedule_mode="weekly", schedule_weekdays="monday")
        TWICE_CONFIG = create_config(schedule_mode="twice_weekly", schedule_weekdays="monday,thursday")
        MONTHLY_CONFIG = create_config(
            schedule_mode="monthly",
            schedule_weekdays="monday",
            schedule_monthly_week="first",
        )
        NOW_LOCAL = datetime(2026, 3, 16, 10, 30, tzinfo=timezone.utc)

        with patch("app.scheduler.now_local", return_value=NOW_LOCAL):
            self.assertEqual(
                get_next_run_epoch(DAILY_CONFIG, 100),
                int(datetime(2026, 3, 17, 9, 0, tzinfo=timezone.utc).timestamp()),
            )
            self.assertEqual(
                get_next_run_epoch(WEEKLY_CONFIG, 100),
                int(datetime(2026, 3, 23, 2, 0, tzinfo=timezone.utc).timestamp()),
            )
            self.assertEqual(
                get_next_run_epoch(TWICE_CONFIG, 100),
                int(datetime(2026, 3, 19, 2, 0, tzinfo=timezone.utc).timestamp()),
            )
            self.assertEqual(
                get_next_run_epoch(MONTHLY_CONFIG, 100),
                int(datetime(2026, 4, 6, 2, 0, tzinfo=timezone.utc).timestamp()),
            )

# --------------------------------------------------------------------------
# This test confirms invalid weekly and monthly weekday lists fall back to the
# provided epoch baseline.
# --------------------------------------------------------------------------
    def test_get_next_run_epoch_returns_now_epoch_for_invalid_weekday_modes(self) -> None:
        WEEKLY_CONFIG = create_config(schedule_mode="weekly", schedule_weekdays="monday,tuesday")
        MONTHLY_CONFIG = create_config(schedule_mode="monthly", schedule_weekdays="funday")

        self.assertEqual(get_next_run_epoch(WEEKLY_CONFIG, 123), 123)
        self.assertEqual(get_next_run_epoch(MONTHLY_CONFIG, 456), 456)
