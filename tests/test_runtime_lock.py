# ------------------------------------------------------------------------------
# This test module verifies the single-writer runtime lock behaviour.
# ------------------------------------------------------------------------------

from pathlib import Path
import tempfile
import unittest

from app.runtime_lock import (
    RuntimeLockError,
    acquire_runtime_lock,
    get_runtime_lock_path,
    release_runtime_lock,
)


# ------------------------------------------------------------------------------
# These tests verify runtime lock acquisition and release behaviour.
# ------------------------------------------------------------------------------
class TestRuntimeLock(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms a second acquisition attempt fails while the first handle
# still holds the shared lock.
# --------------------------------------------------------------------------
    def test_acquire_runtime_lock_blocks_second_writer(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG_DIR = Path(TMPDIR)
            FIRST_HANDLE = acquire_runtime_lock(CONFIG_DIR, "alice")

            with self.assertRaises(RuntimeLockError):
                acquire_runtime_lock(CONFIG_DIR, "bob")

            release_runtime_lock(FIRST_HANDLE)

# --------------------------------------------------------------------------
# This test confirms releasing a held lock allows a later acquisition attempt.
# --------------------------------------------------------------------------
    def test_release_runtime_lock_allows_future_acquisition(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG_DIR = Path(TMPDIR)
            FIRST_HANDLE = acquire_runtime_lock(CONFIG_DIR, "alice")
            release_runtime_lock(FIRST_HANDLE)

            SECOND_HANDLE = acquire_runtime_lock(CONFIG_DIR, "bob")
            LOCK_TEXT = get_runtime_lock_path(CONFIG_DIR).read_text(encoding="utf-8")

            self.assertIn("container_username=bob", LOCK_TEXT)
            release_runtime_lock(SECOND_HANDLE)
