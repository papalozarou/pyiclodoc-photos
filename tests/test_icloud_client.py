# ------------------------------------------------------------------------------
# This test module verifies photo entry normalisation and album mapping.
# ------------------------------------------------------------------------------

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import unittest

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.config import AppConfig
from app.icloud_client import ICloudDriveClient


# ------------------------------------------------------------------------------
# This function builds a minimal app config for client tests.
# ------------------------------------------------------------------------------
def create_config() -> AppConfig:
    return AppConfig(
        container_username="alice",
        icloud_email="alice@example.com",
        icloud_password="secret",
        telegram_bot_token="token",
        telegram_chat_id="1",
        keychain_service_name="icloud-photos-backup",
        run_once=False,
        schedule_mode="interval",
        schedule_backup_time="02:00",
        schedule_weekdays="monday",
        schedule_monthly_week="first",
        schedule_interval_minutes=1440,
        backup_delete_removed=False,
        traversal_workers=1,
        sync_workers=0,
        download_chunk_mib=4,
        reauth_interval_days=30,
        output_dir=Path("/tmp/output"),
        config_dir=Path("/tmp/config"),
        logs_dir=Path("/tmp/logs"),
        manifest_path=Path("/tmp/config/pyiclodoc-photos-manifest.json"),
        auth_state_path=Path("/tmp/config/pyiclodoc-photos-auth_state.json"),
        heartbeat_path=Path("/tmp/logs/pyiclodoc-photos-heartbeat.txt"),
        cookie_dir=Path("/tmp/config/cookies"),
        session_dir=Path("/tmp/config/session"),
        icloudpd_compat_dir=Path("/tmp/config/icloudpd"),
        safety_net_sample_size=200,
        backup_library_enabled=True,
        backup_albums_enabled=True,
        backup_album_links_mode="hardlink",
        backup_include_shared_albums=True,
        backup_include_favourites=True,
        backup_root_library="library",
        backup_root_albums="albums",
    )


# ------------------------------------------------------------------------------
# These tests verify photo listing and path derivation behaviour.
# ------------------------------------------------------------------------------
class TestIcloudClient(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms the client derives canonical year/month/day paths and
# album output paths from photo metadata.
# --------------------------------------------------------------------------
    def test_list_entries_builds_photo_paths(self) -> None:
        CLIENT = ICloudDriveClient(create_config())
        ASSET = SimpleNamespace(
            id="asset-1",
            filename="IMG_0001.JPG",
            size=1024,
            created=datetime(2026, 3, 14, 9, 30, tzinfo=timezone.utc),
            modified=datetime(2026, 3, 14, 9, 31, tzinfo=timezone.utc),
        )
        CLIENT.api = SimpleNamespace(
            photos=SimpleNamespace(
                all=[ASSET],
                albums={
                    "Favourites": [ASSET],
                    "Trips": [ASSET],
                },
            )
        )

        ENTRIES = CLIENT.list_entries()

        self.assertEqual(len(ENTRIES), 1)
        self.assertEqual(ENTRIES[0].path, "library/2026/03/14/IMG_0001.JPG")
        self.assertEqual(ENTRIES[0].album_paths, ("albums/Favourites", "albums/Trips"))
