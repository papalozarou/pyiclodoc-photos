# ------------------------------------------------------------------------------
# This test module verifies photo sync planning and album-link behaviour.
# ------------------------------------------------------------------------------

from dataclasses import dataclass
from pathlib import Path
import tempfile
import unittest

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.syncer import perform_incremental_sync


# ------------------------------------------------------------------------------
# This data class mirrors production remote-entry shape used by helpers.
# ------------------------------------------------------------------------------
@dataclass(frozen=True)
class RemoteEntry:
    path: str
    is_dir: bool
    size: int
    modified: str
    asset_id: str = ""
    created: str = ""
    download_name: str = ""
    album_paths: tuple[str, ...] = ()


# ------------------------------------------------------------------------------
# This class provides a minimal client stub for sync tests.
# ------------------------------------------------------------------------------
class FakeClient:
    def __init__(self, ENTRIES: list[RemoteEntry]):
        self.entries = ENTRIES
        self.download_calls: list[str] = []

    def list_entries(self) -> list[RemoteEntry]:
        return self.entries

    def download_file(self, REMOTE_PATH: str, LOCAL_PATH: Path) -> bool:
        self.download_calls.append(REMOTE_PATH)
        LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        LOCAL_PATH.write_bytes(b"data")
        return True

    def get_last_download_failure_reason(self) -> str:
        return ""


# ------------------------------------------------------------------------------
# These tests verify canonical sync and derived album output behaviour.
# ------------------------------------------------------------------------------
class TestSyncer(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms the sync creates canonical files and derived album
# views from one remote photo entry.
# --------------------------------------------------------------------------
    def test_perform_incremental_sync_creates_library_and_album_views(self) -> None:
        ENTRY = RemoteEntry(
            path="library/2026/03/14/IMG_0001.JPG",
            is_dir=False,
            size=4,
            modified="2026-03-14T09:31:00+00:00",
            asset_id="asset-1",
            created="2026-03-14T09:30:00+00:00",
            download_name="IMG_0001.JPG",
            album_paths=("albums/Trips",),
        )
        CLIENT = FakeClient([ENTRY])

        with tempfile.TemporaryDirectory() as TMPDIR:
            SUMMARY, MANIFEST = perform_incremental_sync(CLIENT, Path(TMPDIR), {})

            self.assertEqual(SUMMARY.transferred_files, 1)
            self.assertTrue((Path(TMPDIR) / ENTRY.path).exists())
            self.assertTrue((Path(TMPDIR) / "albums/Trips/IMG_0001.JPG").exists())
            self.assertIn(ENTRY.path, MANIFEST)
            self.assertIn("albums/Trips/IMG_0001.JPG", MANIFEST)

