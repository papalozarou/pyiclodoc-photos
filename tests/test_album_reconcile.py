# ------------------------------------------------------------------------------
# This test module verifies album reconciliation helper behaviour directly.
# ------------------------------------------------------------------------------

from dataclasses import dataclass
from pathlib import Path
import os
import tempfile
import unittest
from unittest.mock import patch

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.album_reconcile import create_album_link, reconcile_album_views, same_file_contents


# ------------------------------------------------------------------------------
# This data class mirrors the remote-entry shape used by album reconciliation.
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
# These tests verify direct album reconciliation helper behaviour.
# ------------------------------------------------------------------------------
class TestAlbumReconcile(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms same_file_contents returns true for the same file path.
# --------------------------------------------------------------------------
    def test_same_file_contents_returns_true_for_same_file(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            FILE_PATH = Path(TMPDIR) / "photo.jpg"
            FILE_PATH.write_bytes(b"data")

            self.assertTrue(same_file_contents(FILE_PATH, FILE_PATH))

# --------------------------------------------------------------------------
# This test confirms same_file_contents falls back to byte comparison when
# samefile cannot be used.
# --------------------------------------------------------------------------
    def test_same_file_contents_falls_back_to_byte_comparison(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            LEFT_PATH = Path(TMPDIR) / "left.jpg"
            RIGHT_PATH = Path(TMPDIR) / "right.jpg"
            LEFT_PATH.write_bytes(b"data")
            RIGHT_PATH.write_bytes(b"data")

            with patch("app.album_reconcile.os.path.samefile", side_effect=OSError("unsupported")):
                self.assertTrue(same_file_contents(LEFT_PATH, RIGHT_PATH))

# --------------------------------------------------------------------------
# This test confirms same_file_contents returns false when byte reads fail.
# --------------------------------------------------------------------------
    def test_same_file_contents_returns_false_when_byte_reads_fail(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            LEFT_PATH = Path(TMPDIR) / "left.jpg"
            RIGHT_PATH = Path(TMPDIR) / "right.jpg"
            LEFT_PATH.write_bytes(b"data")
            RIGHT_PATH.write_bytes(b"data")

            with patch("app.album_reconcile.os.path.samefile", side_effect=OSError("unsupported")):
                with patch("pathlib.Path.read_bytes", side_effect=OSError("denied")):
                    self.assertFalse(same_file_contents(LEFT_PATH, RIGHT_PATH))

# --------------------------------------------------------------------------
# This test confirms create_album_link reuses an existing identical target.
# --------------------------------------------------------------------------
    def test_create_album_link_reuses_existing_identical_target(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            SOURCE_PATH = TMPDIR_PATH / "library" / "photo.jpg"
            TARGET_PATH = TMPDIR_PATH / "albums" / "Trips" / "photo.jpg"
            SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)
            TARGET_PATH.parent.mkdir(parents=True, exist_ok=True)
            SOURCE_PATH.write_bytes(b"data")
            os.link(SOURCE_PATH, TARGET_PATH)

            self.assertFalse(create_album_link(SOURCE_PATH, TARGET_PATH, "hardlink"))

# --------------------------------------------------------------------------
# This test confirms create_album_link overwrites a stale target in copy mode.
# --------------------------------------------------------------------------
    def test_create_album_link_replaces_stale_target_in_copy_mode(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            SOURCE_PATH = TMPDIR_PATH / "library" / "photo.jpg"
            TARGET_PATH = TMPDIR_PATH / "albums" / "Trips" / "photo.jpg"
            SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)
            TARGET_PATH.parent.mkdir(parents=True, exist_ok=True)
            SOURCE_PATH.write_bytes(b"fresh")
            TARGET_PATH.write_bytes(b"stale")

            self.assertTrue(create_album_link(SOURCE_PATH, TARGET_PATH, "copy"))
            self.assertEqual(TARGET_PATH.read_bytes(), b"fresh")
            self.assertFalse(os.path.samefile(SOURCE_PATH, TARGET_PATH))

# --------------------------------------------------------------------------
# This test confirms create_album_link falls back to copy when hard-linking
# fails.
# --------------------------------------------------------------------------
    def test_create_album_link_falls_back_to_copy_when_hardlink_fails(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            SOURCE_PATH = TMPDIR_PATH / "library" / "photo.jpg"
            TARGET_PATH = TMPDIR_PATH / "albums" / "Trips" / "photo.jpg"
            SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)
            SOURCE_PATH.write_bytes(b"data")

            with patch("app.album_reconcile.os.link", side_effect=OSError("cross-device")):
                self.assertTrue(create_album_link(SOURCE_PATH, TARGET_PATH, "hardlink"))

            self.assertTrue(TARGET_PATH.exists())
            self.assertEqual(TARGET_PATH.read_bytes(), b"data")

# --------------------------------------------------------------------------
# This test confirms reconciliation skips unverified canonical sources and
# logs the debug detail.
# --------------------------------------------------------------------------
    def test_reconcile_album_views_skips_unverified_sources(self) -> None:
        ENTRY = RemoteEntry(
            path="library/2026/03/15/photo.jpg",
            is_dir=False,
            size=4,
            modified="2026-03-15T10:00:00+00:00",
            download_name="photo.jpg",
            album_paths=("albums/Trips", "albums/Favourites"),
        )

        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"

            with patch.dict("os.environ", {"LOG_LEVEL": "debug"}, clear=False):
                RESULT = reconcile_album_views(
                    TMPDIR_PATH,
                    [ENTRY],
                    {},
                    set(),
                    "hardlink",
                    LOG_FILE,
                )

            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertEqual(RESULT.skipped_missing_source, 2)
            self.assertEqual(RESULT.created, 0)
            self.assertIn("Album view skipped unverified canonical source", LOG_TEXT)

# --------------------------------------------------------------------------
# This test confirms reconciliation skips missing canonical files even when
# the path is marked valid.
# --------------------------------------------------------------------------
    def test_reconcile_album_views_skips_missing_canonical_source(self) -> None:
        ENTRY = RemoteEntry(
            path="library/2026/03/15/photo.jpg",
            is_dir=False,
            size=4,
            modified="2026-03-15T10:00:00+00:00",
            download_name="photo.jpg",
            album_paths=("albums/Trips",),
        )

        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"

            with patch.dict("os.environ", {"LOG_LEVEL": "debug"}, clear=False):
                RESULT = reconcile_album_views(
                    TMPDIR_PATH,
                    [ENTRY],
                    {},
                    {ENTRY.path},
                    "hardlink",
                    LOG_FILE,
                )

            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertEqual(RESULT.skipped_missing_source, 1)
            self.assertIn("Album view skipped missing canonical source", LOG_TEXT)

# --------------------------------------------------------------------------
# This test confirms reconciliation records created and reused album outputs
# and updates the manifest for successful refreshes.
# --------------------------------------------------------------------------
    def test_reconcile_album_views_tracks_created_and_reused_outputs(self) -> None:
        ENTRY = RemoteEntry(
            path="library/2026/03/15/photo.jpg",
            is_dir=False,
            size=4,
            modified="2026-03-15T10:00:00+00:00",
            download_name="photo.jpg",
            album_paths=("albums/Trips", "albums/Favourites"),
        )

        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            SOURCE_PATH = TMPDIR_PATH / ENTRY.path
            SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)
            SOURCE_PATH.write_bytes(b"data")
            NEW_MANIFEST: dict[str, dict[str, object]] = {}

            with patch(
                "app.album_reconcile.create_album_link",
                side_effect=[True, False],
            ):
                RESULT = reconcile_album_views(
                    TMPDIR_PATH,
                    [ENTRY],
                    NEW_MANIFEST,
                    {ENTRY.path},
                    "hardlink",
                    None,
                )

            self.assertEqual(RESULT.created, 1)
            self.assertEqual(RESULT.reused, 1)
            self.assertEqual(len(NEW_MANIFEST), 2)
            self.assertIn("albums/Trips/photo.jpg", NEW_MANIFEST)
            self.assertIn("albums/Favourites/photo.jpg", NEW_MANIFEST)

# --------------------------------------------------------------------------
# This test confirms reconciliation counts refresh errors and leaves the
# manifest untouched for failed album outputs.
# --------------------------------------------------------------------------
    def test_reconcile_album_views_counts_refresh_errors(self) -> None:
        ENTRY = RemoteEntry(
            path="library/2026/03/15/photo.jpg",
            is_dir=False,
            size=4,
            modified="2026-03-15T10:00:00+00:00",
            download_name="photo.jpg",
            album_paths=("albums/Trips",),
        )

        with tempfile.TemporaryDirectory() as TMPDIR:
            TMPDIR_PATH = Path(TMPDIR)
            LOG_FILE = TMPDIR_PATH / "worker.log"
            SOURCE_PATH = TMPDIR_PATH / ENTRY.path
            SOURCE_PATH.parent.mkdir(parents=True, exist_ok=True)
            SOURCE_PATH.write_bytes(b"data")
            NEW_MANIFEST: dict[str, dict[str, object]] = {}

            with patch(
                "app.album_reconcile.create_album_link",
                side_effect=OSError("permission denied"),
            ):
                with patch.dict("os.environ", {"LOG_LEVEL": "debug"}, clear=False):
                    RESULT = reconcile_album_views(
                        TMPDIR_PATH,
                        [ENTRY],
                        NEW_MANIFEST,
                        {ENTRY.path},
                        "hardlink",
                        LOG_FILE,
                    )

            LOG_TEXT = LOG_FILE.read_text(encoding="utf-8")
            self.assertEqual(RESULT.errors, 1)
            self.assertEqual(NEW_MANIFEST, {})
            self.assertIn("Album view refresh failed", LOG_TEXT)
