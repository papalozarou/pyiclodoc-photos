# ------------------------------------------------------------------------------
# This test module verifies photo sync planning and album-link behaviour.
# ------------------------------------------------------------------------------

from dataclasses import dataclass
from pathlib import Path
import os
import stat
import tempfile
import unittest
from unittest.mock import patch

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.icloud_client import DownloadResult
from app.syncer import (
    collect_local_files,
    collect_mismatches,
    get_sample_key,
    perform_incremental_sync,
    run_first_time_safety_net,
)
from app.transfer_runner import transfer_with_retry


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
        self.manifest_inputs: list[dict[str, dict[str, object]]] = []

    def list_entries_for_sync(self, MANIFEST: dict[str, dict[str, object]]) -> list[RemoteEntry]:
        self.manifest_inputs.append(MANIFEST)
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
# This class raises from the download call to exercise worker exception paths.
# ------------------------------------------------------------------------------
class ExplodingClient(FakeClient):
    def download_file_result(self, REMOTE_PATH: str, LOCAL_PATH: Path) -> DownloadResult:
        _ = (REMOTE_PATH, LOCAL_PATH)
        raise RuntimeError("boom")


# ------------------------------------------------------------------------------
# This class fails once with a transient reason and then succeeds on retry.
# ------------------------------------------------------------------------------
class FlakyClient(FakeClient):
    def __init__(self, ENTRIES: list[RemoteEntry], FAILURE_REASON: str):
        super().__init__(ENTRIES)
        self.failure_reason = FAILURE_REASON
        self.failed_once_paths: set[str] = set()

    def download_file_result(self, REMOTE_PATH: str, LOCAL_PATH: Path) -> DownloadResult:
        self.download_calls.append(REMOTE_PATH)

        if REMOTE_PATH not in self.failed_once_paths:
            self.failed_once_paths.add(REMOTE_PATH)
            return DownloadResult(False, failure_reason=self.failure_reason)

        LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        LOCAL_PATH.write_bytes(b"data")
        return DownloadResult(True, written_bytes=4)


# ------------------------------------------------------------------------------
# This helper mimics the subset of Path behaviour used by mismatch tests.
# ------------------------------------------------------------------------------
class FakeStatPath:
    def __init__(self, LABEL: str, UID: int, GID: int):
        self.label = LABEL
        self.uid = UID
        self.gid = GID

    def stat(self):
        return type("Stat", (), {"st_uid": self.uid, "st_gid": self.gid})()

    def __str__(self) -> str:
        return self.label


# ------------------------------------------------------------------------------
# This helper raises from "stat()" to mimic raced or unreadable sample files.
# ------------------------------------------------------------------------------
class BrokenStatPath:
    def __init__(self, LABEL: str):
        self.label = LABEL

    def stat(self):
        raise OSError("permission denied")

    def __str__(self) -> str:
        return self.label


# ------------------------------------------------------------------------------
# These tests verify canonical sync and derived album output behaviour.
# ------------------------------------------------------------------------------
class TestSyncer(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms the bounded local-file collector ignores directories and
# respects the requested sample size.
# --------------------------------------------------------------------------
    def test_collect_local_files_respects_sample_limit(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            (TMPDIR_PATH / "dir").mkdir()
            for INDEX in range(3):
                (TMPDIR_PATH / f"file-{INDEX}.txt").write_text("x", encoding="utf-8")

            RESULT = collect_local_files(TMPDIR_PATH, 2)
            self.assertEqual(len(RESULT), 2)
            self.assertTrue(all(PATH.is_file() for PATH in RESULT))

# --------------------------------------------------------------------------
# This test confirms the bounded local-file collector uses deterministic
# distributed selection instead of trusting filesystem walk order alone.
# --------------------------------------------------------------------------
    def test_collect_local_files_uses_deterministic_distributed_selection(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            FILE_PATHS = [
                TMPDIR_PATH / "library" / "2024" / "01" / "01" / "a.jpg",
                TMPDIR_PATH / "library" / "2025" / "06" / "07" / "b.jpg",
                TMPDIR_PATH / "albums" / "Trips" / "c.jpg",
                TMPDIR_PATH / "albums" / "Favourites" / "d.jpg",
            ]

            for FILE_PATH in FILE_PATHS:
                FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
                FILE_PATH.write_text("x", encoding="utf-8")

            RESULT = collect_local_files(TMPDIR_PATH, 2)
            EXPECTED = [
                FILE_PATH
                for _, FILE_PATH in sorted(
                    (
                        (get_sample_key(TMPDIR_PATH, FILE_PATH), FILE_PATH)
                        for FILE_PATH in FILE_PATHS
                    ),
                    key=lambda ITEM: ITEM[0],
                )[:2]
            ]

            self.assertEqual(RESULT, EXPECTED)


# --------------------------------------------------------------------------
# This test confirms mismatch collection ignores matching files and stops at
# the configured output limit.
# --------------------------------------------------------------------------
    def test_collect_mismatches_filters_matches_and_respects_limit(self) -> None:
        FIRST_PATH = FakeStatPath("/tmp/a", 1000, 1000)
        SECOND_PATH = FakeStatPath("/tmp/b", 1001, 1000)
        THIRD_PATH = FakeStatPath("/tmp/c", 1002, 1003)

        RESULT = collect_mismatches(
            [FIRST_PATH, SECOND_PATH, THIRD_PATH],
            1000,
            1000,
            LIMIT=1,
        )

        self.assertEqual(len(RESULT), 1)
        self.assertIn("/tmp/b", RESULT[0])

# --------------------------------------------------------------------------
# This test confirms mismatch collection skips files whose metadata cannot be
# read instead of treating them as fatal sampling failures.
# --------------------------------------------------------------------------
    def test_collect_mismatches_skips_stat_failures(self) -> None:
        RESULT = collect_mismatches(
            [
                BrokenStatPath("/tmp/unreadable"),
                FakeStatPath("/tmp/b", 1001, 1000),
            ],
            1000,
            1000,
        )

        self.assertEqual(len(RESULT), 1)
        self.assertIn("/tmp/b", RESULT[0])

# --------------------------------------------------------------------------
# This test confirms transient transfer failures are retried before the sync
# reports a final error.
# --------------------------------------------------------------------------
    def test_transfer_with_retry_retries_transient_failure_once(self) -> None:
        ENTRY = RemoteEntry(
            path="library/2026/03/14/IMG_0001.JPG",
            is_dir=False,
            size=4,
            modified="2026-03-14T09:31:00+00:00",
            asset_id="asset-1",
            created="2026-03-14T09:30:00+00:00",
            download_name="IMG_0001.JPG",
        )
        CLIENT = FlakyClient([ENTRY], "timeout")

        with tempfile.TemporaryDirectory() as TMPDIR:
            with patch("app.transfer_runner.time.sleep"):
                RESULT = transfer_with_retry(CLIENT, Path(TMPDIR), ENTRY)

        self.assertTrue(RESULT.success)
        self.assertEqual(CLIENT.download_calls, [ENTRY.path, ENTRY.path])

# --------------------------------------------------------------------------
# This test confirms the first-run safety net passes cleanly when no files
# exist under the output root.
# --------------------------------------------------------------------------
    def test_run_first_time_safety_net_passes_when_output_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            RESULT = run_first_time_safety_net(Path(TMPDIR), 10)

        self.assertFalse(RESULT.should_block)
        self.assertEqual(RESULT.mismatched_samples, [])

# --------------------------------------------------------------------------
# This test confirms the first-run safety net blocks when sampled ownership
# does not match the current runtime user and group.
# --------------------------------------------------------------------------
    def test_run_first_time_safety_net_blocks_on_mismatch(self) -> None:
        SAMPLE_FILE = FakeStatPath("/tmp/photo.jpg", 2000, 3000)

        with patch("app.syncer.collect_local_files", return_value=[SAMPLE_FILE]):
            with patch("app.syncer.os.getuid", return_value=1000):
                with patch("app.syncer.os.getgid", return_value=1000):
                    RESULT = run_first_time_safety_net(Path("/tmp/out"), 10)

        self.assertTrue(RESULT.should_block)
        self.assertEqual(RESULT.expected_uid, 1000)
        self.assertEqual(RESULT.expected_gid, 1000)
        self.assertIn("/tmp/photo.jpg", RESULT.mismatched_samples[0])

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
            self.assertEqual(CLIENT.manifest_inputs, [{}])
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
# This test confirms delete totals are exposed through the primary sync
# summary model when mirror-delete handling is enabled.
# --------------------------------------------------------------------------
    def test_perform_incremental_sync_records_delete_totals_in_summary(self) -> None:
        ENTRY = RemoteEntry(
            path="library/2026/03/14/IMG_0001.JPG",
            is_dir=False,
            size=4,
            modified="2026-03-14T09:31:00+00:00",
            asset_id="asset-1",
            created="2026-03-14T09:30:00+00:00",
            download_name="IMG_0001.JPG",
        )
        CLIENT = FakeClient([ENTRY])

        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            STALE_FILE = TMPDIR_PATH / "albums/Old/STALE.JPG"
            STALE_FILE.parent.mkdir(parents=True, exist_ok=True)
            STALE_FILE.write_bytes(b"stale")

            SUMMARY, _ = perform_incremental_sync(
                CLIENT,
                TMPDIR_PATH,
                {},
                BACKUP_DELETE_REMOVED=True,
            )

            self.assertEqual(SUMMARY.deleted_files, 1)
            self.assertEqual(SUMMARY.deleted_directories, 2)
            self.assertEqual(SUMMARY.delete_error_files, 0)

# --------------------------------------------------------------------------
# This test confirms delete errors contribute to the primary sync error total
# so operator-facing summaries do not under-report failed cleanup.
# --------------------------------------------------------------------------
    def test_perform_incremental_sync_counts_delete_errors_in_primary_total(self) -> None:
        ENTRY = RemoteEntry(
            path="library/2026/03/14/IMG_0001.JPG",
            is_dir=False,
            size=4,
            modified="2026-03-14T09:31:00+00:00",
            asset_id="asset-1",
            created="2026-03-14T09:30:00+00:00",
            download_name="IMG_0001.JPG",
        )
        CLIENT = FakeClient([ENTRY])

        with tempfile.TemporaryDirectory() as TMPDIR:
            with patch(
                "app.syncer.delete_removed_local_paths",
                return_value=(0, 0, 2),
            ):
                SUMMARY, _ = perform_incremental_sync(
                    CLIENT,
                    Path(TMPDIR),
                    {},
                    BACKUP_DELETE_REMOVED=True,
                )

        self.assertEqual(SUMMARY.error_files, 2)
        self.assertEqual(SUMMARY.delete_error_files, 2)

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
# This test confirms the sync logs the zero-candidate transfer skip path when
# the manifest already matches the remote entry set.
# --------------------------------------------------------------------------
    def test_perform_incremental_sync_logs_transfer_skipped_when_no_candidates_exist(self) -> None:
        ENTRY = RemoteEntry(
            path="library/2026/03/14/IMG_0002.JPG",
            is_dir=False,
            size=4,
            modified="2026-03-14T09:31:00+00:00",
            asset_id="asset-2",
            created="2026-03-14T09:30:00+00:00",
            download_name="IMG_0002.JPG",
        )
        CLIENT = FakeClient([ENTRY])
        MANIFEST = {
            ENTRY.path: {
                "asset_id": "asset-2",
                "album_paths": [],
                "created": ENTRY.created,
                "download_name": ENTRY.download_name,
                "is_dir": False,
                "modified": ENTRY.modified,
                "size": ENTRY.size,
            },
        }

        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"

            self.addCleanup(self._restore_log_level)
            self._set_debug_logging()

            SUMMARY, NEW_MANIFEST = perform_incremental_sync(
                CLIENT,
                Path(TMPDIR),
                MANIFEST,
                LOG_FILE=LOG_FILE,
            )

            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertEqual(SUMMARY.transferred_files, 0)
            self.assertEqual(SUMMARY.skipped_files, 1)
            self.assertIn("Transfer skipped. candidates=0.", LOG_TEXT)
            self.assertIn(ENTRY.path, NEW_MANIFEST)

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
# This test confirms album-view filesystem failures are counted and logged
# without failing canonical transfer success.
# --------------------------------------------------------------------------
    def test_perform_incremental_sync_treats_album_refresh_failures_as_best_effort(self) -> None:
        ENTRY = RemoteEntry(
            path="library/2026/03/14/IMG_0006.JPG",
            is_dir=False,
            size=4,
            modified="2026-03-14T09:31:00+00:00",
            asset_id="asset-6",
            created="2026-03-14T09:30:00+00:00",
            download_name="IMG_0006.JPG",
            album_paths=("albums/Trips",),
        )
        CLIENT = FakeClient([ENTRY])

        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"

            self.addCleanup(self._restore_log_level)
            self._set_debug_logging()

            with patch(
                "app.album_reconcile.create_album_link",
                side_effect=OSError("permission denied"),
            ):
                SUMMARY, MANIFEST = perform_incremental_sync(
                    CLIENT,
                    TMPDIR_PATH,
                    {},
                    LOG_FILE=LOG_FILE,
                )

            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertEqual(SUMMARY.transferred_files, 1)
            self.assertEqual(SUMMARY.error_files, 1)
            self.assertEqual(SUMMARY.transfer_error_files, 0)
            self.assertEqual(SUMMARY.derived_error_files, 1)
            self.assertTrue((TMPDIR_PATH / ENTRY.path).exists())
            self.assertFalse((TMPDIR_PATH / "albums/Trips/IMG_0006.JPG").exists())
            self.assertNotIn("albums/Trips/IMG_0006.JPG", MANIFEST)
            self.assertIn(
                "Album view refresh failed: albums/Trips/IMG_0006.JPG -> "
                "library/2026/03/14/IMG_0006.JPG (permission denied)",
                LOG_TEXT,
            )
            self.assertIn(
                "Album reconciliation finished. created=0, reused=0, "
                "skipped_missing_source=0, errors=1.",
                LOG_TEXT,
            )

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
# This test confirms worker exceptions are counted once through the shared
# failure aggregation path.
# --------------------------------------------------------------------------
    def test_perform_incremental_sync_counts_worker_exception_once(self) -> None:
        ENTRY = RemoteEntry(
            path="library/2026/03/14/IMG_0007.JPG",
            is_dir=False,
            size=4,
            modified="2026-03-14T09:31:00+00:00",
            asset_id="asset-7",
            created="2026-03-14T09:30:00+00:00",
            download_name="IMG_0007.JPG",
        )
        CLIENT = ExplodingClient([ENTRY])

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
            self.assertEqual(SUMMARY.error_files, 1)
            self.assertIn(
                "File transfer failed: library/2026/03/14/IMG_0007.JPG "
                "(worker_exception:RuntimeError)",
                LOG_TEXT,
            )
            self.assertNotIn("File transfer worker failed:", LOG_TEXT)
            self.assertIn(
                "Transfer failure reason detail: worker_exception:RuntimeError=1",
                LOG_TEXT,
            )

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
