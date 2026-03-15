# ------------------------------------------------------------------------------
# This test module verifies manifest and auth-state recovery behaviour.
# ------------------------------------------------------------------------------

from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import tempfile
import unittest

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.state import load_auth_state, load_manifest


# ------------------------------------------------------------------------------
# These tests verify corrupt JSON state is quarantined and tolerated.
# ------------------------------------------------------------------------------
class TestState(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms corrupt auth-state JSON falls back to defaults and is
# quarantined for later inspection.
# --------------------------------------------------------------------------
    def test_load_auth_state_quarantines_corrupt_json(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "auth_state.json"
            STATE_PATH.write_text("{broken", encoding="utf-8")
            OUTPUT = StringIO()

            with redirect_stdout(OUTPUT):
                AUTH_STATE = load_auth_state(STATE_PATH)

            self.assertEqual(AUTH_STATE.last_auth_utc, "1970-01-01T00:00:00+00:00")
            self.assertFalse(AUTH_STATE.auth_pending)
            self.assertFalse(STATE_PATH.exists())
            self.assertTrue(STATE_PATH.with_suffix(".json.corrupt").exists())
            self.assertIn("Corrupt JSON state ignored", OUTPUT.getvalue())

# --------------------------------------------------------------------------
# This test confirms corrupt manifest JSON returns an empty manifest and is
# quarantined instead of crashing the worker.
# --------------------------------------------------------------------------
    def test_load_manifest_quarantines_corrupt_json(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            MANIFEST_PATH = Path(TMPDIR) / "manifest.json"
            MANIFEST_PATH.write_text("{broken", encoding="utf-8")
            OUTPUT = StringIO()

            with redirect_stdout(OUTPUT):
                MANIFEST = load_manifest(MANIFEST_PATH)

            self.assertEqual(MANIFEST, {})
            self.assertFalse(MANIFEST_PATH.exists())
            self.assertTrue(MANIFEST_PATH.with_suffix(".json.corrupt").exists())
            self.assertIn("Corrupt JSON state ignored", OUTPUT.getvalue())
