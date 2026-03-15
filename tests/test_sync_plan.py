# ------------------------------------------------------------------------------
# This test module verifies sync planning and manifest comparison helpers.
# ------------------------------------------------------------------------------

from dataclasses import dataclass
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.sync_plan import (
    build_sync_plan,
    entry_matches_manifest,
    entry_metadata,
    get_valid_canonical_paths,
    needs_transfer,
)


# ------------------------------------------------------------------------------
# This data class mirrors the remote-entry shape used by sync planning helpers.
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
# These tests verify manifest comparison and planning helpers directly.
# ------------------------------------------------------------------------------
class TestSyncPlan(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms entry metadata preserves the manifest fields expected by
# the incremental sync workflow.
# --------------------------------------------------------------------------
    def test_entry_metadata_returns_expected_manifest_shape(self) -> None:
        ENTRY = RemoteEntry(
            path="library/2026/03/15/photo.jpg",
            is_dir=False,
            size=4,
            modified="2026-03-15T10:00:00+00:00",
            asset_id="asset-1",
            created="2026-03-15T09:59:00+00:00",
            download_name="photo.jpg",
            album_paths=("albums/Trips",),
        )

        self.assertEqual(
            entry_metadata(ENTRY),
            {
                "asset_id": "asset-1",
                "album_paths": ["albums/Trips"],
                "created": "2026-03-15T09:59:00+00:00",
                "download_name": "photo.jpg",
                "is_dir": False,
                "modified": "2026-03-15T10:00:00+00:00",
                "size": 4,
            },
        )

# --------------------------------------------------------------------------
# This test confirms needs_transfer returns true for missing and changed
# manifest states and false for unchanged entries.
# --------------------------------------------------------------------------
    def test_needs_transfer_handles_missing_changed_and_unchanged_entries(self) -> None:
        ENTRY = RemoteEntry(
            path="library/2026/03/15/photo.jpg",
            is_dir=False,
            size=4,
            modified="2026-03-15T10:00:00+00:00",
            asset_id="asset-1",
        )
        MATCHING_MANIFEST = {
            ENTRY.path: {
                "asset_id": "asset-1",
                "size": 4,
                "modified": "2026-03-15T10:00:00+00:00",
            },
        }

        self.assertTrue(needs_transfer(ENTRY, {}))
        self.assertTrue(
            needs_transfer(
                ENTRY,
                {ENTRY.path: {"asset_id": "asset-2", "size": 4, "modified": ENTRY.modified}},
            ),
        )
        self.assertTrue(
            needs_transfer(
                ENTRY,
                {ENTRY.path: {"asset_id": "asset-1", "size": 5, "modified": ENTRY.modified}},
            ),
        )
        self.assertTrue(
            needs_transfer(
                ENTRY,
                {ENTRY.path: {"asset_id": "asset-1", "size": 4, "modified": "old"}},
            ),
        )
        self.assertFalse(needs_transfer(ENTRY, MATCHING_MANIFEST))
        self.assertTrue(entry_matches_manifest(ENTRY, MATCHING_MANIFEST))
        self.assertFalse(entry_matches_manifest(ENTRY, {}))

# --------------------------------------------------------------------------
# This test confirms build_sync_plan queues changed entries and preserves
# unchanged entries in the new manifest.
# --------------------------------------------------------------------------
    def test_build_sync_plan_splits_candidates_and_skipped_entries(self) -> None:
        FIRST_ENTRY = RemoteEntry(
            path="library/2026/03/15/photo-a.jpg",
            is_dir=False,
            size=4,
            modified="2026-03-15T10:00:00+00:00",
            asset_id="asset-a",
            created="2026-03-15T09:59:00+00:00",
            download_name="photo-a.jpg",
        )
        SECOND_ENTRY = RemoteEntry(
            path="library/2026/03/15/photo-b.jpg",
            is_dir=False,
            size=5,
            modified="2026-03-15T11:00:00+00:00",
            asset_id="asset-b",
            created="2026-03-15T10:59:00+00:00",
            download_name="photo-b.jpg",
        )
        MANIFEST = {
            SECOND_ENTRY.path: {
                "asset_id": "asset-b",
                "size": 5,
                "modified": "2026-03-15T11:00:00+00:00",
            },
        }

        NEW_MANIFEST, TRANSFER_CANDIDATES, SKIPPED = build_sync_plan(
            [FIRST_ENTRY, SECOND_ENTRY],
            MANIFEST,
            None,
        )

        self.assertEqual(TRANSFER_CANDIDATES, [FIRST_ENTRY])
        self.assertEqual(SKIPPED, 1)
        self.assertIn(SECOND_ENTRY.path, NEW_MANIFEST)

# --------------------------------------------------------------------------
# This test confirms build_sync_plan emits debug planning logs for queued and
# unchanged entries when debug logging is enabled.
# --------------------------------------------------------------------------
    def test_build_sync_plan_writes_debug_logs_for_queue_and_skip_paths(self) -> None:
        FIRST_ENTRY = RemoteEntry(
            path="library/2026/03/15/photo-a.jpg",
            is_dir=False,
            size=4,
            modified="2026-03-15T10:00:00+00:00",
            asset_id="asset-a",
            download_name="photo-a.jpg",
        )
        SECOND_ENTRY = RemoteEntry(
            path="library/2026/03/15/photo-b.jpg",
            is_dir=False,
            size=5,
            modified="2026-03-15T11:00:00+00:00",
            asset_id="asset-b",
            download_name="photo-b.jpg",
        )
        MANIFEST = {
            SECOND_ENTRY.path: {
                "asset_id": "asset-b",
                "size": 5,
                "modified": "2026-03-15T11:00:00+00:00",
            },
        }

        with tempfile.TemporaryDirectory() as TMPDIR:
            LOG_FILE = Path(TMPDIR) / "worker.log"

            with patch.dict("os.environ", {"LOG_LEVEL": "debug"}, clear=False):
                build_sync_plan([FIRST_ENTRY, SECOND_ENTRY], MANIFEST, LOG_FILE)

            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertIn("Photo queued for transfer: library/2026/03/15/photo-a.jpg", LOG_TEXT)
            self.assertIn("Photo skipped unchanged: library/2026/03/15/photo-b.jpg", LOG_TEXT)

# --------------------------------------------------------------------------
# This test confirms get_valid_canonical_paths filters album links out of the
# manifest-derived path set.
# --------------------------------------------------------------------------
    def test_get_valid_canonical_paths_excludes_album_links(self) -> None:
        NEW_MANIFEST = {
            "library/2026/03/15/photo-a.jpg": {"entry_kind": "file"},
            "albums/Trips/photo-a.jpg": {"entry_kind": "album_link"},
            "library/2026/03/15/photo-b.jpg": {},
        }

        self.assertEqual(
            get_valid_canonical_paths(NEW_MANIFEST),
            {
                "library/2026/03/15/photo-a.jpg",
                "library/2026/03/15/photo-b.jpg",
            },
        )
