# ------------------------------------------------------------------------------
# This test module verifies file-backed keyring configuration and credential
# read-write behaviour.
# ------------------------------------------------------------------------------

from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

from tests._stubs import install_dependency_stubs

install_dependency_stubs()

from app.credential_store import configure_keyring, load_credentials, save_credentials


# ------------------------------------------------------------------------------
# These tests verify persistent credential-store helpers.
# ------------------------------------------------------------------------------
class TestCredentialStore(unittest.TestCase):
# --------------------------------------------------------------------------
# This test confirms configure_keyring sets the expected backend path and
# supporting environment variables.
# --------------------------------------------------------------------------
    def test_configure_keyring_sets_expected_backend_and_env(self) -> None:
        with tempfile.TemporaryDirectory() as TMPDIR:
            CONFIG_DIR = Path(TMPDIR)
            KEYRING_BACKEND = MagicMock()

            with patch("app.credential_store.PlaintextKeyring", return_value=KEYRING_BACKEND):
                with patch("app.credential_store.keyring.set_keyring") as SET_KEYRING:
                    with patch.dict("os.environ", {}, clear=True):
                        configure_keyring(CONFIG_DIR)

            EXPECTED_FILE = CONFIG_DIR / "keyring" / "keyring_pass.cfg"
            self.assertEqual(KEYRING_BACKEND.file_path, str(EXPECTED_FILE))
            self.assertEqual(SET_KEYRING.call_args.args[0], KEYRING_BACKEND)

# --------------------------------------------------------------------------
# This test confirms load_credentials returns empty-string fallbacks when the
# keyring has no stored values.
# --------------------------------------------------------------------------
    def test_load_credentials_returns_empty_defaults(self) -> None:
        with patch("app.credential_store.keyring.get_password", return_value=None):
            EMAIL, PASSWORD = load_credentials("service", "alice")

        self.assertEqual(EMAIL, "")
        self.assertEqual(PASSWORD, "")

# --------------------------------------------------------------------------
# This test confirms load_credentials returns both stored values with the
# expected key names.
# --------------------------------------------------------------------------
    def test_load_credentials_reads_expected_keys(self) -> None:
        with patch(
            "app.credential_store.keyring.get_password",
            side_effect=["alice@example.com", "secret"],
        ) as GET_PASSWORD:
            EMAIL, PASSWORD = load_credentials("service", "alice")

        self.assertEqual(EMAIL, "alice@example.com")
        self.assertEqual(PASSWORD, "secret")
        self.assertEqual(GET_PASSWORD.call_args_list[0].args, ("service", "alice:email"))
        self.assertEqual(GET_PASSWORD.call_args_list[1].args, ("service", "alice:password"))

# --------------------------------------------------------------------------
# This test confirms save_credentials writes only the non-empty values.
# --------------------------------------------------------------------------
    def test_save_credentials_ignores_empty_values(self) -> None:
        with patch("app.credential_store.keyring.set_password") as SET_PASSWORD:
            save_credentials("service", "alice", "alice@example.com", "")

        SET_PASSWORD.assert_called_once_with("service", "alice:email", "alice@example.com")

# --------------------------------------------------------------------------
# This test confirms save_credentials writes both keys when both values are
# supplied.
# --------------------------------------------------------------------------
    def test_save_credentials_writes_both_values(self) -> None:
        with patch("app.credential_store.keyring.set_password") as SET_PASSWORD:
            save_credentials("service", "alice", "alice@example.com", "secret")

        self.assertEqual(SET_PASSWORD.call_count, 2)
