# ------------------------------------------------------------------------------
# This test module verifies extracted scheduler parsing and next-run behaviour.
# ------------------------------------------------------------------------------

from datetime import datetime, timezone
from pathlib import Path
import unittest

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.config import AppConfig
from app.scheduler import (
    calculate_next_daily_run_epoch,
    format_schedule_line,
    get_next_run_epoch,
    parse_daily,
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

# --------------------------------------------------------------------------
# This test confirms duplicate weekday entries fail strict list parsing.
# --------------------------------------------------------------------------
    def test_parse_weekday_list_rejects_duplicates(self) -> None:
        self.assertIsNone(parse_weekday_list("monday,monday", 2))

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
# This test confirms manual trigger text uses the extracted scheduler wording.
# --------------------------------------------------------------------------
    def test_format_schedule_line_formats_manual_trigger(self) -> None:
        CONFIG = create_config(schedule_mode="weekly", schedule_weekdays="monday")

        self.assertEqual(
            format_schedule_line(CONFIG, "manual"),
            "Manual, then weekly on monday at 02:00.",
        )

# --------------------------------------------------------------------------
# This test confirms interval scheduling uses the provided epoch baseline.
# --------------------------------------------------------------------------
    def test_get_next_run_epoch_uses_interval_offset(self) -> None:
        CONFIG = create_config(schedule_mode="interval", schedule_interval_minutes=30)

        self.assertEqual(get_next_run_epoch(CONFIG, 100), 1900)
