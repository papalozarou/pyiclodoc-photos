# ------------------------------------------------------------------------------
# This test module verifies extracted authentication state transitions and
# reminder handling.
# ------------------------------------------------------------------------------

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.auth_flow import (
    attempt_auth,
    get_reauth_days_left,
    parse_iso,
    process_reauth_reminders,
)
from app.state import AuthState, load_auth_state


# ------------------------------------------------------------------------------
# This client stub returns predefined authentication results for auth-flow
# tests.
# ------------------------------------------------------------------------------
class FakeClient:
    def __init__(self, START_RESULT: tuple[bool, str], COMPLETE_RESULT: tuple[bool, str]):
        self.start_result = START_RESULT
        self.complete_result = COMPLETE_RESULT

    def start_authentication(self) -> tuple[bool, str]:
        return self.start_result

    def complete_authentication(self, CODE: str) -> tuple[bool, str]:
        _ = CODE
        return self.complete_result


# ------------------------------------------------------------------------------
# These tests verify extracted authentication state and reminder behaviour.
# ------------------------------------------------------------------------------
class TestAuthFlow(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms invalid ISO values fall back to the Unix epoch.
# --------------------------------------------------------------------------
    def test_parse_iso_falls_back_to_epoch_for_invalid_value(self) -> None:
        RESULT = parse_iso("not-an-iso-value")

        self.assertEqual(RESULT.isoformat(), "1970-01-01T00:00:00+00:00")

# --------------------------------------------------------------------------
# This test confirms reauth day calculation uses whole elapsed days.
# --------------------------------------------------------------------------
    def test_get_reauth_days_left_uses_elapsed_whole_days(self) -> None:
        with patch(
            "app.auth_flow.now_local",
            return_value=parse_iso("2026-03-15T12:00:00+00:00"),
        ):
            DAYS_LEFT = get_reauth_days_left("2026-03-10T13:00:00+00:00", 10)

        self.assertEqual(DAYS_LEFT, 6)

# --------------------------------------------------------------------------
# This test confirms successful startup auth clears pending flags, persists
# state, and emits the completion message.
# --------------------------------------------------------------------------
    def test_attempt_auth_saves_complete_state_on_success(self) -> None:
        STATE = AuthState("1970-01-01T00:00:00+00:00", True, True, "prompt2")
        CLIENT = FakeClient((True, "Signed in."), (True, "ok"))
        SENT_MESSAGES: list[str] = []

        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "auth.json"

            with patch("app.auth_flow.now_iso", return_value="2026-03-15T12:00:00+00:00"):
                NEW_STATE, IS_AUTHENTICATED, DETAILS = attempt_auth(
                    CLIENT,
                    STATE,
                    STATE_PATH,
                    SENT_MESSAGES.append,
                    "alice",
                    "alice@example.com",
                    "",
                )

            SAVED_STATE = load_auth_state(STATE_PATH)

        self.assertTrue(IS_AUTHENTICATED)
        self.assertEqual(DETAILS, "Signed in.")
        self.assertFalse(NEW_STATE.auth_pending)
        self.assertFalse(NEW_STATE.reauth_pending)
        self.assertEqual(NEW_STATE.reminder_stage, "none")
        self.assertEqual(NEW_STATE.last_auth_utc, "2026-03-15T12:00:00+00:00")
        self.assertEqual(SAVED_STATE, NEW_STATE)
        self.assertIn("Authentication complete", SENT_MESSAGES[0])

# --------------------------------------------------------------------------
# This test confirms a two-factor prompt marks auth pending and persists the
# updated state.
# --------------------------------------------------------------------------
    def test_attempt_auth_marks_pending_for_two_factor_prompt(self) -> None:
        STATE = AuthState("1970-01-01T00:00:00+00:00", False, False, "none")
        CLIENT = FakeClient((False, "Two-factor code is required."), (True, "ok"))
        SENT_MESSAGES: list[str] = []

        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "auth.json"
            NEW_STATE, IS_AUTHENTICATED, _ = attempt_auth(
                CLIENT,
                STATE,
                STATE_PATH,
                SENT_MESSAGES.append,
                "alice",
                "alice@example.com",
                "",
            )

            SAVED_STATE = load_auth_state(STATE_PATH)

        self.assertFalse(IS_AUTHENTICATED)
        self.assertTrue(NEW_STATE.auth_pending)
        self.assertTrue(SAVED_STATE.auth_pending)
        self.assertIn("Authentication required", SENT_MESSAGES[0])

# --------------------------------------------------------------------------
# This test confirms a generic auth failure does not enter MFA-pending state
# and emits the failure message.
# --------------------------------------------------------------------------
    def test_attempt_auth_emits_failure_message_for_non_two_factor_error(self) -> None:
        STATE = AuthState("1970-01-01T00:00:00+00:00", False, False, "none")
        CLIENT = FakeClient((False, "Bad password."), (True, "ok"))
        SENT_MESSAGES: list[str] = []

        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "auth.json"
            NEW_STATE, IS_AUTHENTICATED, DETAILS = attempt_auth(
                CLIENT,
                STATE,
                STATE_PATH,
                SENT_MESSAGES.append,
                "alice",
                "alice@example.com",
                "",
            )

            SAVED_STATE = load_auth_state(STATE_PATH)

        self.assertFalse(IS_AUTHENTICATED)
        self.assertEqual(DETAILS, "Bad password.")
        self.assertFalse(NEW_STATE.auth_pending)
        self.assertEqual(SAVED_STATE, NEW_STATE)
        self.assertIn("Authentication failed", SENT_MESSAGES[0])

# --------------------------------------------------------------------------
# This test confirms a failed MFA-code submission keeps the existing pending
# code-entry state so the operator can retry with a new code.
# --------------------------------------------------------------------------
    def test_attempt_auth_keeps_pending_state_for_failed_code_submission(self) -> None:
        STATE = AuthState("1970-01-01T00:00:00+00:00", True, False, "none")
        CLIENT = FakeClient((False, "unused"), (False, "Bad verification code."))
        SENT_MESSAGES: list[str] = []

        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "auth.json"
            NEW_STATE, IS_AUTHENTICATED, DETAILS = attempt_auth(
                CLIENT,
                STATE,
                STATE_PATH,
                SENT_MESSAGES.append,
                "alice",
                "alice@example.com",
                "123456",
            )

            SAVED_STATE = load_auth_state(STATE_PATH)

        self.assertFalse(IS_AUTHENTICATED)
        self.assertEqual(DETAILS, "Bad verification code.")
        self.assertTrue(NEW_STATE.auth_pending)
        self.assertEqual(SAVED_STATE, NEW_STATE)
        self.assertIn("Authentication failed", SENT_MESSAGES[0])

# --------------------------------------------------------------------------
# This test confirms a provided MFA code uses the completion path rather than
# the startup-auth path.
# --------------------------------------------------------------------------
    def test_attempt_auth_uses_complete_authentication_when_code_is_supplied(self) -> None:
        STATE = AuthState("1970-01-01T00:00:00+00:00", True, False, "none")
        CLIENT = FakeClient((False, "unused"), (True, "MFA accepted."))
        SENT_MESSAGES: list[str] = []

        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "auth.json"

            with patch("app.auth_flow.now_iso", return_value="2026-03-15T12:00:00+00:00"):
                NEW_STATE, IS_AUTHENTICATED, DETAILS = attempt_auth(
                    CLIENT,
                    STATE,
                    STATE_PATH,
                    SENT_MESSAGES.append,
                    "alice",
                    "alice@example.com",
                    "123456",
                )

        self.assertTrue(IS_AUTHENTICATED)
        self.assertEqual(DETAILS, "MFA accepted.")
        self.assertFalse(NEW_STATE.auth_pending)
        self.assertIn("Authentication complete", SENT_MESSAGES[0])

# --------------------------------------------------------------------------
# This test confirms auth details surface auth-state persistence failure when
# the save path cannot be written.
# --------------------------------------------------------------------------
    def test_attempt_auth_surfaces_persistence_failure_in_details(self) -> None:
        STATE = AuthState("1970-01-01T00:00:00+00:00", False, False, "none")
        CLIENT = FakeClient((True, "Signed in."), (True, "ok"))
        SENT_MESSAGES: list[str] = []

        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "auth.json"

            with patch("app.auth_flow.now_iso", return_value="2026-03-15T12:00:00+00:00"):
                with patch("app.auth_flow.save_auth_state", return_value=False):
                    _, _, DETAILS = attempt_auth(
                        CLIENT,
                        STATE,
                        STATE_PATH,
                        SENT_MESSAGES.append,
                        "alice",
                        "alice@example.com",
                        "",
                    )

        self.assertIn("Auth state persistence failed.", DETAILS)

# --------------------------------------------------------------------------
# This test confirms the two-day reminder stage marks reauth pending and
# emits the manual reauth prompt.
# --------------------------------------------------------------------------
    def test_process_reauth_reminders_marks_reauth_pending(self) -> None:
        STATE = AuthState("1970-01-02T00:00:00+00:00", False, False, "none")
        SENT_MESSAGES: list[str] = []

        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "auth.json"
            with patch("app.auth_flow.now_iso", return_value="2026-03-15T12:00:00+00:00"):
                NEW_STATE = process_reauth_reminders(
                    STATE,
                    STATE_PATH,
                    SENT_MESSAGES.append,
                    "alice",
                    2,
                )

            SAVED_STATE = load_auth_state(STATE_PATH)

        self.assertTrue(NEW_STATE.reauth_pending)
        self.assertEqual(NEW_STATE.reminder_stage, "prompt2")
        self.assertEqual(NEW_STATE.last_reminder_utc, "2026-03-15T12:00:00+00:00")
        self.assertTrue(SAVED_STATE.reauth_pending)
        self.assertIn("Reauthentication required", SENT_MESSAGES[0])

# --------------------------------------------------------------------------
# This test confirms the five-day reminder stage sends the reminder message
# without marking reauth pending yet.
# --------------------------------------------------------------------------
    def test_process_reauth_reminders_sends_five_day_alert(self) -> None:
        STATE = AuthState("2026-03-10T00:00:00+00:00", False, False, "none")
        SENT_MESSAGES: list[str] = []

        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "auth.json"

            with patch("app.auth_flow.get_reauth_days_left", return_value=5):
                with patch("app.auth_flow.now_iso", return_value="2026-03-15T12:00:00+00:00"):
                    NEW_STATE = process_reauth_reminders(
                        STATE,
                        STATE_PATH,
                        SENT_MESSAGES.append,
                        "alice",
                        30,
                    )

            SAVED_STATE = load_auth_state(STATE_PATH)

        self.assertFalse(NEW_STATE.reauth_pending)
        self.assertEqual(NEW_STATE.reminder_stage, "alert5")
        self.assertEqual(NEW_STATE.last_reminder_utc, "2026-03-15T12:00:00+00:00")
        self.assertEqual(SAVED_STATE, NEW_STATE)
        self.assertIn("Reauth reminder", SENT_MESSAGES[0])

# --------------------------------------------------------------------------
# This test confirms days greater than five clear reminder state and reauth
# pending flags.
# --------------------------------------------------------------------------
    def test_process_reauth_reminders_clears_state_when_not_due(self) -> None:
        STATE = AuthState("2026-03-10T00:00:00+00:00", False, True, "prompt2")
        SENT_MESSAGES: list[str] = []

        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "auth.json"

            with patch("app.auth_flow.get_reauth_days_left", return_value=6):
                NEW_STATE = process_reauth_reminders(
                    STATE,
                    STATE_PATH,
                    SENT_MESSAGES.append,
                    "alice",
                    30,
                )

            SAVED_STATE = load_auth_state(STATE_PATH)

        self.assertFalse(NEW_STATE.reauth_pending)
        self.assertEqual(NEW_STATE.reminder_stage, "none")
        self.assertEqual(SAVED_STATE, NEW_STATE)

# --------------------------------------------------------------------------
# This test confirms the clear-state branch does not rewrite auth state when
# the current reminder state already matches the desired reset state.
# --------------------------------------------------------------------------
    def test_process_reauth_reminders_skips_save_when_state_is_already_clear(self) -> None:
        STATE = AuthState("2026-03-10T00:00:00+00:00", False, False, "none")

        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "auth.json"

            with patch("app.auth_flow.get_reauth_days_left", return_value=6):
                with patch("app.auth_flow.save_auth_state") as SAVE_AUTH_STATE:
                    NEW_STATE = process_reauth_reminders(
                        STATE,
                        STATE_PATH,
                        lambda MESSAGE: None,
                        "alice",
                        30,
                    )

        self.assertEqual(NEW_STATE, STATE)
        SAVE_AUTH_STATE.assert_not_called()

# --------------------------------------------------------------------------
# This test confirms no-op reminder paths leave state unchanged and avoid
# unnecessary persistence.
# --------------------------------------------------------------------------
    def test_process_reauth_reminders_returns_existing_state_when_no_transition_applies(self) -> None:
        STATE = AuthState("2026-03-10T00:00:00+00:00", False, True, "prompt2")
        SENT_MESSAGES: list[str] = []

        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "auth.json"

            with patch("app.auth_flow.get_reauth_days_left", return_value=2):
                with patch("app.auth_flow.save_auth_state") as SAVE_AUTH_STATE:
                    NEW_STATE = process_reauth_reminders(
                        STATE,
                        STATE_PATH,
                        SENT_MESSAGES.append,
                        "alice",
                        30,
                    )

        self.assertEqual(NEW_STATE, STATE)
        self.assertEqual(SENT_MESSAGES, [])
        SAVE_AUTH_STATE.assert_not_called()

# --------------------------------------------------------------------------
# This test confirms restart processing does not resend the reauth prompt when
# reauth is already pending from an earlier prompt or manual command.
# --------------------------------------------------------------------------
    def test_process_reauth_reminders_skips_duplicate_prompt_when_reauth_is_pending(self) -> None:
        STATE = AuthState(
            "2026-03-10T00:00:00+00:00",
            False,
            True,
            "none",
            "2026-03-15T12:00:00+00:00",
        )
        SENT_MESSAGES: list[str] = []

        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "auth.json"

            with patch("app.auth_flow.get_reauth_days_left", return_value=2):
                with patch("app.auth_flow.save_auth_state") as SAVE_AUTH_STATE:
                    NEW_STATE = process_reauth_reminders(
                        STATE,
                        STATE_PATH,
                        SENT_MESSAGES.append,
                        "alice",
                        30,
                    )

        self.assertEqual(NEW_STATE, STATE)
        self.assertEqual(SENT_MESSAGES, [])
        SAVE_AUTH_STATE.assert_not_called()
