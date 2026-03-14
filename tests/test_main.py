# ------------------------------------------------------------------------------
# This test module verifies Telegram message branding.
# ------------------------------------------------------------------------------

import unittest

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.telegram_messages import format_telegram_event


# ------------------------------------------------------------------------------
# These tests verify Photos-specific Telegram headings.
# ------------------------------------------------------------------------------
class TestTelegramMessages(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms Telegram event headings use the Photos title prefix.
# --------------------------------------------------------------------------
    def test_format_telegram_event_uses_photos_branding(self) -> None:
        MESSAGE = format_telegram_event("🟢", "Container started", "Hello.")
        self.assertIn("PCD Photos - Container started", MESSAGE)

