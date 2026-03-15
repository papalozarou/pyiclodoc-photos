# ------------------------------------------------------------------------------
# This test module verifies photo sync planning and album-link behaviour.
# ------------------------------------------------------------------------------

from dataclasses import dataclass
from pathlib import Path
import os
import tempfile
import unittest

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.icloud_client import DownloadResult
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
        self.failure_reason = ""

    def list_entries(self) -> list[RemoteEntry]:
        return self.entries

    def download_file_result(self, REMOTE_PATH: str, LOCAL_PATH: Path) -> DownloadResult:
        self.download_calls.append(REMOTE_PATH)
        LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        LOCAL_PATH.write_bytes(b"data")
        return DownloadResult(True, written_bytes=4)

    def get_last_download_failure_reason(self) -> str:
        return self.failure_reason


# ------------------------------------------------------------------------------
# This class provides a failing client stub for transfer-error logging tests.
# ------------------------------------------------------------------------------
class FailingClient(FakeClient):
    def __init__(self, ENTRIES: list[RemoteEntry], FAILURE_REASON: str):
        super().__init__(ENTRIES)
        self.failure_reason = FAILURE_REASON

    def download_file_result(self, REMOTE_PATH: str, LOCAL_PATH: Path) -> DownloadResult:
        _ = (REMOTE_PATH, LOCAL_PATH)
        return DownloadResult(False, failure_reason=self.failure_reason)


# ------------------------------------------------------------------------------
# This class provides per-path failure results for album-gating tests.
# ------------------------------------------------------------------------------
class SelectiveClient(FakeClient):
    def __init__(self, ENTRIES: list[RemoteEntry], FAILED_PATHS: dict[str, str]):
        super().__init__(ENTRIES)
        self.failed_paths = FAILED_PATHS

    def download_file_result(self, REMOTE_PATH: str, LOCAL_PATH: Path) -> DownloadResult:
        self.download_calls.append(REMOTE_PATH)

        if REMOTE_PATH in self.failed_paths:
            return DownloadResult(False, failure_reason=self.failed_paths[REMOTE_PATH])

        LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        LOCAL_PATH.write_bytes(b"data")
        return DownloadResult(True, written_bytes=4)


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

# --------------------------------------------------------------------------
# This test confirms the sync honours copy-only album mode without creating
# hard links to the canonical source.
# --------------------------------------------------------------------------
    def test_perform_incremental_sync_uses_strict_copy_mode_for_albums(self) -> None:
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
            TMPDIR_PATH = Path(TMPDIR)
            SUMMARY, MANIFEST = perform_incremental_sync(
                CLIENT,
                TMPDIR_PATH,
                {},
                BACKUP_ALBUM_LINKS_MODE="copy",
            )
            LIBRARY_PATH = TMPDIR_PATH / ENTRY.path
            ALBUM_PATH = TMPDIR_PATH / "albums/Trips/IMG_0001.JPG"

            self.assertEqual(SUMMARY.transferred_files, 1)
            self.assertTrue(ALBUM_PATH.exists())
            self.assertFalse(os.path.samefile(LIBRARY_PATH, ALBUM_PATH))
            self.assertIn("albums/Trips/IMG_0001.JPG", MANIFEST)

# --------------------------------------------------------------------------
# This test confirms disabling album output stops both creation and delete
# management for the albums tree.
# --------------------------------------------------------------------------
    def test_perform_incremental_sync_leaves_existing_albums_tree_untouched_when_disabled(self) -> None:
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
            TMPDIR_PATH = Path(TMPDIR)
            STALE_ALBUM_PATH = TMPDIR_PATH / "albums/Trips/STALE.JPG"
            STALE_ALBUM_PATH.parent.mkdir(parents=True, exist_ok=True)
            STALE_ALBUM_PATH.write_bytes(b"stale")

            SUMMARY, MANIFEST = perform_incremental_sync(
                CLIENT,
                TMPDIR_PATH,
                {},
                BACKUP_DELETE_REMOVED=True,
                BACKUP_ALBUMS_ENABLED=False,
            )

            self.assertEqual(SUMMARY.transferred_files, 1)
            self.assertFalse((TMPDIR_PATH / "albums/Trips/IMG_0001.JPG").exists())
            self.assertTrue(STALE_ALBUM_PATH.exists())
            self.assertNotIn("albums/Trips/IMG_0001.JPG", MANIFEST)

# --------------------------------------------------------------------------
# This test confirms the sync emits verbose planning, transfer, album, and
# delete diagnostics when debug logging is enabled.
# --------------------------------------------------------------------------
    def test_perform_incremental_sync_writes_verbose_debug_logs(self) -> None:
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
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"
            STALE_FILE = TMPDIR_PATH / "albums/Old/STALE.JPG"
            STALE_FILE.parent.mkdir(parents=True, exist_ok=True)
            STALE_FILE.write_bytes(b"stale")

            self.addCleanup(self._restore_log_level)
            self._set_debug_logging()

            perform_incremental_sync(
                CLIENT,
                TMPDIR_PATH,
                {},
                LOG_FILE=LOG_FILE,
                BACKUP_DELETE_REMOVED=True,
            )

            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertIn("Remote listing detail: entries=1, files=1", LOG_TEXT)
            self.assertIn("Photo queued for transfer: library/2026/03/14/IMG_0001.JPG", LOG_TEXT)
            self.assertIn("Transfer execution detail: workers=", LOG_TEXT)
            self.assertIn("Photo transferred: library/2026/03/14/IMG_0001.JPG", LOG_TEXT)
            self.assertIn("Album reconciliation finished. created=1, reused=0", LOG_TEXT)
            self.assertIn("Removed local file: albums/Old/STALE.JPG", LOG_TEXT)
            self.assertIn("Removed empty directory: albums/Old", LOG_TEXT)
            self.assertIn("Delete phase finished. deleted_files=1, deleted_directories=1, errors=0.", LOG_TEXT)
            self.assertIn("Transfer finished. transferred=1, skipped=0, errors=0.", LOG_TEXT)

# --------------------------------------------------------------------------
# This test confirms the sync emits failure summaries when transfers fail.
# --------------------------------------------------------------------------
    def test_perform_incremental_sync_logs_transfer_failure_reasons(self) -> None:
        ENTRY = RemoteEntry(
            path="library/2026/03/14/IMG_0002.JPG",
            is_dir=False,
            size=4,
            modified="2026-03-14T09:31:00+00:00",
            asset_id="asset-2",
            created="2026-03-14T09:30:00+00:00",
            download_name="IMG_0002.JPG",
        )
        CLIENT = FailingClient([ENTRY], "timeout")

        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"

            self.addCleanup(self._restore_log_level)
            self._set_debug_logging()

            SUMMARY, _ = perform_incremental_sync(CLIENT, Path(TMPDIR), {}, LOG_FILE=LOG_FILE)

            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertEqual(SUMMARY.error_files, 1)
            self.assertIn("File transfer failed: library/2026/03/14/IMG_0002.JPG (timeout)", LOG_TEXT)
            self.assertIn("Transfer failure reason detail: timeout=1", LOG_TEXT)

# --------------------------------------------------------------------------
# This test confirms album outputs are not derived from stale canonical files
# when the current transfer for that asset fails.
# --------------------------------------------------------------------------
    def test_perform_incremental_sync_skips_album_output_for_failed_transfer(self) -> None:
        ENTRY = RemoteEntry(
            path="library/2026/03/14/IMG_0003.JPG",
            is_dir=False,
            size=4,
            modified="2026-03-14T09:31:00+00:00",
            asset_id="asset-3",
            created="2026-03-14T09:30:00+00:00",
            download_name="IMG_0003.JPG",
            album_paths=("albums/Trips",),
        )
        CLIENT = SelectiveClient([ENTRY], {ENTRY.path: "timeout"})

        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            STALE_SOURCE = TMPDIR_PATH / ENTRY.path
            STALE_SOURCE.parent.mkdir(parents=True, exist_ok=True)
            STALE_SOURCE.write_bytes(b"stale")

            SUMMARY, MANIFEST = perform_incremental_sync(CLIENT, TMPDIR_PATH, {})

            self.assertEqual(SUMMARY.error_files, 1)
            self.assertFalse((TMPDIR_PATH / "albums/Trips/IMG_0003.JPG").exists())
            self.assertNotIn("albums/Trips/IMG_0003.JPG", MANIFEST)

# --------------------------------------------------------------------------
# This test confirms transfer failure detail aggregates distinct per-file
# reasons instead of relying on shared mutable client state.
# --------------------------------------------------------------------------
    def test_perform_incremental_sync_aggregates_per_transfer_failure_reasons(self) -> None:
        FIRST_ENTRY = RemoteEntry(
            path="library/2026/03/14/IMG_0004.JPG",
            is_dir=False,
            size=4,
            modified="2026-03-14T09:31:00+00:00",
            asset_id="asset-4",
            created="2026-03-14T09:30:00+00:00",
            download_name="IMG_0004.JPG",
        )
        SECOND_ENTRY = RemoteEntry(
            path="library/2026/03/14/IMG_0005.JPG",
            is_dir=False,
            size=4,
            modified="2026-03-14T09:31:00+00:00",
            asset_id="asset-5",
            created="2026-03-14T09:30:00+00:00",
            download_name="IMG_0005.JPG",
        )
        CLIENT = SelectiveClient(
            [FIRST_ENTRY, SECOND_ENTRY],
            {
                FIRST_ENTRY.path: "timeout",
                SECOND_ENTRY.path: "write_failed",
            },
        )

        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"

            self.addCleanup(self._restore_log_level)
            self._set_debug_logging()

            SUMMARY, _ = perform_incremental_sync(
                CLIENT,
                Path(TMPDIR),
                {},
                LOG_FILE=LOG_FILE,
            )

            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertEqual(SUMMARY.error_files, 2)
            self.assertIn("Transfer failure reason detail: timeout=1, write_failed=1", LOG_TEXT)

# --------------------------------------------------------------------------
# This helper sets debug logging for syncer log assertions.
# --------------------------------------------------------------------------
    def _set_debug_logging(self) -> None:
        self.previous_log_level = os.environ.get("LOG_LEVEL")
        os.environ["LOG_LEVEL"] = "debug"

# --------------------------------------------------------------------------
# This helper restores the prior log level after log assertions.
# --------------------------------------------------------------------------
    def _restore_log_level(self) -> None:
        if getattr(self, "previous_log_level", None) is None:
            os.environ.pop("LOG_LEVEL", None)
            return

        os.environ["LOG_LEVEL"] = self.previous_log_level
