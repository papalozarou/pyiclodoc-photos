# ------------------------------------------------------------------------------
# This test module verifies extracted authentication state transitions and
# reminder handling.
# ------------------------------------------------------------------------------

from pathlib import Path
import tempfile
import unittest

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.auth_flow import attempt_auth, process_reauth_reminders
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
# This test confirms the two-day reminder stage marks reauth pending and
# emits the manual reauth prompt.
# --------------------------------------------------------------------------
    def test_process_reauth_reminders_marks_reauth_pending(self) -> None:
        STATE = AuthState("1970-01-02T00:00:00+00:00", False, False, "none")
        SENT_MESSAGES: list[str] = []

        with tempfile.TemporaryDirectory() as TMPDIR:
            STATE_PATH = Path(TMPDIR) / "auth.json"
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
        self.assertTrue(SAVED_STATE.reauth_pending)
        self.assertIn("Reauthentication required", SENT_MESSAGES[0])
