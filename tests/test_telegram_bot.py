# ------------------------------------------------------------------------------
# This test module verifies Telegram transport and command parsing behaviour.
# ------------------------------------------------------------------------------

import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.telegram_bot import TelegramConfig, fetch_updates, get_endpoint, parse_command, send_message


# ------------------------------------------------------------------------------
# These tests verify Telegram API helpers and strict command parsing.
# ------------------------------------------------------------------------------
class TestTelegramBot(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms endpoint generation matches the Bot API URL shape.
# --------------------------------------------------------------------------
    def test_get_endpoint_formats_expected_url(self) -> None:
        self.assertEqual(
            get_endpoint("abc", "sendMessage"),
            "https://api.telegram.org/botabc/sendMessage",
        )

# --------------------------------------------------------------------------
# This test confirms send_message returns false when Telegram is disabled.
# --------------------------------------------------------------------------
    def test_send_message_returns_false_without_required_config(self) -> None:
        self.assertFalse(send_message(TelegramConfig(bot_token="", chat_id="1"), "hello"))
        self.assertFalse(send_message(TelegramConfig(bot_token="token", chat_id=""), "hello"))

# --------------------------------------------------------------------------
# This test confirms send_message posts Markdown payload and returns the HTTP
# success state.
# --------------------------------------------------------------------------
    def test_send_message_posts_expected_payload(self) -> None:
        CONFIG = TelegramConfig(bot_token="token", chat_id="1")
        RESPONSE = MagicMock(ok=True)

        with patch("app.telegram_bot.requests.post", return_value=RESPONSE) as POST:
            RESULT = send_message(CONFIG, "hello", TIMEOUT=10)

        self.assertTrue(RESULT)
        POST.assert_called_once()
        self.assertEqual(
            POST.call_args.kwargs["json"],
            {
                "chat_id": "1",
                "text": "hello",
                "parse_mode": "Markdown",
            },
        )
        self.assertEqual(POST.call_args.kwargs["timeout"], 10)

# --------------------------------------------------------------------------
# This test confirms send_message collapses request exceptions to false.
# --------------------------------------------------------------------------
    def test_send_message_returns_false_on_request_exception(self) -> None:
        CONFIG = TelegramConfig(bot_token="token", chat_id="1")

        with patch(
            "app.telegram_bot.requests.post",
            side_effect=Exception("boom"),
        ):
            with patch("app.telegram_bot.requests.RequestException", Exception):
                RESULT = send_message(CONFIG, "hello")

        self.assertFalse(RESULT)

# --------------------------------------------------------------------------
# This test confirms fetch_updates returns an empty list without a bot token.
# --------------------------------------------------------------------------
    def test_fetch_updates_returns_empty_list_without_token(self) -> None:
        self.assertEqual(fetch_updates(TelegramConfig(bot_token="", chat_id="1"), None), [])

# --------------------------------------------------------------------------
# This test confirms fetch_updates calls the Bot API with timeout and offset.
# --------------------------------------------------------------------------
    def test_fetch_updates_returns_result_list(self) -> None:
        CONFIG = TelegramConfig(bot_token="token", chat_id="1")
        RESPONSE = MagicMock(ok=True)
        RESPONSE.json.return_value = {"ok": True, "result": [{"update_id": 1}]}

        with patch("app.telegram_bot.requests.get", return_value=RESPONSE) as GET:
            RESULT = fetch_updates(CONFIG, 10, TIMEOUT=30)

        self.assertEqual(RESULT, [{"update_id": 1}])
        self.assertEqual(GET.call_args.kwargs["params"], {"timeout": 30, "offset": 10})
        self.assertEqual(GET.call_args.kwargs["timeout"], 35)

# --------------------------------------------------------------------------
# This test confirms fetch_updates returns empty lists for request and API
# failure cases.
# --------------------------------------------------------------------------
    def test_fetch_updates_returns_empty_list_for_failure_cases(self) -> None:
        CONFIG = TelegramConfig(bot_token="token", chat_id="1")
        BAD_HTTP = MagicMock(ok=False)
        BAD_API = MagicMock(ok=True)
        BAD_API.json.return_value = {"ok": False}
        BAD_RESULT = MagicMock(ok=True)
        BAD_RESULT.json.return_value = {"ok": True, "result": "not-a-list"}

        with patch(
            "app.telegram_bot.requests.get",
            side_effect=Exception("boom"),
        ):
            with patch("app.telegram_bot.requests.RequestException", Exception):
                self.assertEqual(fetch_updates(CONFIG, None), [])

        with patch("app.telegram_bot.requests.get", return_value=BAD_HTTP):
            self.assertEqual(fetch_updates(CONFIG, None), [])

        with patch("app.telegram_bot.requests.get", return_value=BAD_API):
            self.assertEqual(fetch_updates(CONFIG, None), [])

        with patch("app.telegram_bot.requests.get", return_value=BAD_RESULT):
            self.assertEqual(fetch_updates(CONFIG, None), [])

# --------------------------------------------------------------------------
# This test confirms parse_command returns none for invalid update shapes and
# chat mismatches.
# --------------------------------------------------------------------------
    def test_parse_command_rejects_invalid_updates(self) -> None:
        self.assertIsNone(parse_command({}, "alice", "1"))
        self.assertIsNone(
            parse_command(
                {"update_id": 1, "message": {"chat": {"id": 2}, "text": "alice backup"}},
                "alice",
                "1",
            ),
        )
        self.assertIsNone(
            parse_command(
                {"update_id": 1, "message": {"chat": {"id": 1}, "text": ""}},
                "alice",
                "1",
            ),
        )
        self.assertIsNone(
            parse_command(
                {"update_id": 1, "message": {"chat": {"id": 1}, "text": "bob backup"}},
                "alice",
                "1",
            ),
        )
        self.assertIsNone(
            parse_command(
                {"update_id": 1, "message": {"chat": {"id": 1}, "text": "alice "}},
                "alice",
                "1",
            ),
        )
        self.assertIsNone(
            parse_command(
                {"update_id": 1, "message": {"chat": {"id": 1}, "text": "alice unknown"}},
                "alice",
                "1",
            ),
        )

# --------------------------------------------------------------------------
# This test confirms parse_command accepts recognised commands and normalises
# their casing.
# --------------------------------------------------------------------------
    def test_parse_command_accepts_expected_command_shapes(self) -> None:
        EVENT = parse_command(
            {
                "update_id": 1,
                "message": {"chat": {"id": 1}, "text": "Alice AUTH 123456"},
            },
            "alice",
            "1",
        )

        self.assertIsNotNone(EVENT)
        self.assertEqual(EVENT.command, "auth")
        self.assertEqual(EVENT.args, "123456")
        self.assertEqual(EVENT.update_id, 1)
