# ------------------------------------------------------------------------------
# This test module verifies manifest and auth-state recovery behaviour.
# ------------------------------------------------------------------------------

from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.state import load_auth_state, load_manifest, save_auth_state, save_manifest, write_json


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

# --------------------------------------------------------------------------
# This test confirms atomic JSON writes return False and emit a warning when
# the temporary file path cannot be written.
# --------------------------------------------------------------------------
    def test_write_json_returns_false_on_oserror(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "state.json"
            OUTPUT = StringIO()

            with redirect_stdout(OUTPUT):
                with patch("pathlib.Path.open", side_effect=OSError("disk full")):
                    RESULT = write_json(STATE_PATH, {"ok": True})

            self.assertFalse(RESULT)
            self.assertIn("State write failed", OUTPUT.getvalue())

# --------------------------------------------------------------------------
# This test confirms a timezone-less persisted auth timestamp is normalized
# to offset-aware UTC and logged once during load.
# --------------------------------------------------------------------------
    def test_load_auth_state_normalizes_timezone_less_timestamp(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "auth_state.json"
            OUTPUT = StringIO()
            STATE_PATH.write_text(
                (
                    '{'
                    '"last_auth_utc":"2026-03-10T12:00:00",'
                    '"auth_pending":false,'
                    '"reauth_pending":false,'
                    '"reminder_stage":"none"'
                    '}'
                ),
                encoding="utf-8",
            )

            with redirect_stdout(OUTPUT):
                AUTH_STATE = load_auth_state(STATE_PATH)

            self.assertEqual(AUTH_STATE.last_auth_utc, "2026-03-10T12:00:00+00:00")
            self.assertIn("timestamp had no timezone offset", OUTPUT.getvalue())

# --------------------------------------------------------------------------
# This test confirms invalid auth-state field types are reset to defaults and
# logged rather than being coerced loosely.
# --------------------------------------------------------------------------
    def test_load_auth_state_resets_invalid_field_types(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "auth_state.json"
            OUTPUT = StringIO()
            STATE_PATH.write_text(
                (
                    '{'
                    '"last_auth_utc":123,'
                    '"auth_pending":"false",'
                    '"reauth_pending":"true",'
                    '"reminder_stage":"later",'
                    '"last_reminder_utc":7,'
                    '"manual_reauth_pending":"no"'
                    '}'
                ),
                encoding="utf-8",
            )

            with redirect_stdout(OUTPUT):
                AUTH_STATE = load_auth_state(STATE_PATH)

            self.assertEqual(AUTH_STATE.last_auth_utc, "1970-01-01T00:00:00+00:00")
            self.assertFalse(AUTH_STATE.auth_pending)
            self.assertFalse(AUTH_STATE.reauth_pending)
            self.assertEqual(AUTH_STATE.reminder_stage, "none")
            self.assertEqual(AUTH_STATE.last_reminder_utc, "")
            self.assertFalse(AUTH_STATE.manual_reauth_pending)
            self.assertIn('Invalid auth state field "auth_pending"', OUTPUT.getvalue())
            self.assertIn('Invalid auth state field "reminder_stage"', OUTPUT.getvalue())

# --------------------------------------------------------------------------
# This test confirms valid JSON manifest payloads with the wrong top-level
# shape are ignored with an explicit warning.
# --------------------------------------------------------------------------
    def test_load_manifest_warns_on_non_object_payload(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            MANIFEST_PATH = Path(TMPDIR) / "manifest.json"
            OUTPUT = StringIO()
            MANIFEST_PATH.write_text("[]", encoding="utf-8")

            with redirect_stdout(OUTPUT):
                MANIFEST = load_manifest(MANIFEST_PATH)

            self.assertEqual(MANIFEST, {})
            self.assertIn("Invalid manifest state ignored", OUTPUT.getvalue())

# --------------------------------------------------------------------------
# This test confirms the auth-state and manifest save helpers surface the
# boolean write contract from the shared state layer.
# --------------------------------------------------------------------------
    def test_save_helpers_return_boolean_write_results(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            ROOT_DIR = Path(TMPDIR)
            AUTH_STATE_PATH = ROOT_DIR / "auth.json"
            MANIFEST_PATH = ROOT_DIR / "manifest.json"

            self.assertTrue(
                save_auth_state(
                    AUTH_STATE_PATH,
                    load_auth_state(AUTH_STATE_PATH),
                ),
            )
            self.assertTrue(save_manifest(MANIFEST_PATH, {"library/a.jpg": {"size": 1}}))
