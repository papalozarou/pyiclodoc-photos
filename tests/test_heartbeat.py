# ------------------------------------------------------------------------------
# This test module verifies heartbeat file update and thread bootstrap logic.
# ------------------------------------------------------------------------------

from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.heartbeat import start_heartbeat_updater, update_heartbeat


# ------------------------------------------------------------------------------
# These tests verify heartbeat file lifecycle helpers.
# ------------------------------------------------------------------------------
class TestHeartbeat(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms update_heartbeat creates the parent directory and file.
# --------------------------------------------------------------------------
    def test_update_heartbeat_creates_file(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            HEARTBEAT_PATH = Path(TMPDIR) / "logs" / "heartbeat.txt"

            update_heartbeat(HEARTBEAT_PATH)

            self.assertTrue(HEARTBEAT_PATH.exists())

# --------------------------------------------------------------------------
# This test confirms update_heartbeat suppresses filesystem errors.
# --------------------------------------------------------------------------
    def test_update_heartbeat_ignores_oserror(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            HEARTBEAT_PATH = Path(TMPDIR) / "logs" / "heartbeat.txt"

            with patch("pathlib.Path.mkdir", side_effect=OSError("permission denied")):
                update_heartbeat(HEARTBEAT_PATH)

            self.assertFalse(HEARTBEAT_PATH.exists())

# --------------------------------------------------------------------------
# This test confirms start_heartbeat_updater starts a daemon thread and
# performs the initial heartbeat update via the loop target.
# --------------------------------------------------------------------------
    def test_start_heartbeat_updater_starts_daemon_thread(self) -> None:
        HEARTBEAT_PATH = Path("/tmp/heartbeat.txt")
        CREATED_THREAD = MagicMock()
        STOP_EVENT = MagicMock()
        STOP_EVENT.is_set.return_value = False
        STOP_EVENT.wait.side_effect = [True]

        def create_thread(*, target, daemon):
            self.assertTrue(daemon)
            target()
            return CREATED_THREAD

        with patch("app.heartbeat.update_heartbeat") as UPDATE_HEARTBEAT:
            with patch("app.heartbeat.threading.Event", return_value=STOP_EVENT):
                with patch("app.heartbeat.threading.Thread", side_effect=create_thread) as THREAD:
                    RESULT = start_heartbeat_updater(HEARTBEAT_PATH)

        THREAD.assert_called_once()
        CREATED_THREAD.start.assert_called_once()
        UPDATE_HEARTBEAT.assert_called_once_with(HEARTBEAT_PATH)
        self.assertIs(RESULT, STOP_EVENT)
