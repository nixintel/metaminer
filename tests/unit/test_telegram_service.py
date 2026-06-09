"""
Unit tests for app/services/telegram_service.py

Covers:
  - get_credentials: env-var priority, DB fallback, missing creds
  - check_session: session file present / absent
  - make_client: correct session path construction, correct credentials passed

No database, network, or Telegram API access required.
"""
import asyncio
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from app.services.telegram_service import check_session, make_client, get_credentials

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# get_credentials
# ---------------------------------------------------------------------------

class TestGetCredentials:
    def _run(self, coro):
        return asyncio.run(coro)

    def _mock_settings(self, api_id=0, api_hash=""):
        cfg = MagicMock()
        cfg.TELEGRAM_API_ID = api_id
        cfg.TELEGRAM_API_HASH = api_hash
        return cfg

    def test_returns_env_creds_when_both_set(self):
        cfg = self._mock_settings(api_id=12345, api_hash="abc123hash")
        db = MagicMock()  # never consulted when env creds present
        with patch("app.services.telegram_service.settings", cfg):
            result = self._run(get_credentials(db))
        assert result == (12345, "abc123hash")

    def test_db_not_queried_when_env_creds_present(self):
        cfg = self._mock_settings(api_id=1, api_hash="hash")
        db = AsyncMock()
        with patch("app.services.telegram_service.settings", cfg):
            self._run(get_credentials(db))
        db.execute.assert_not_called()

    def test_falls_back_to_db_when_api_id_is_zero(self):
        # api_id of 0 is falsy — should query DB
        cfg = self._mock_settings(api_id=0, api_hash="hash")
        mock_cred = MagicMock()
        mock_cred.api_id = 99999
        mock_cred.api_hash = "db_hash"
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_cred
        db = AsyncMock()
        db.execute = AsyncMock(return_value=mock_result)
        with patch("app.services.telegram_service.settings", cfg):
            result = self._run(get_credentials(db))
        assert result == (99999, "db_hash")

    def test_falls_back_to_db_when_api_hash_is_empty(self):
        cfg = self._mock_settings(api_id=12345, api_hash="")
        mock_cred = MagicMock()
        mock_cred.api_id = 42
        mock_cred.api_hash = "from_db"
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_cred
        db = AsyncMock()
        db.execute = AsyncMock(return_value=mock_result)
        with patch("app.services.telegram_service.settings", cfg):
            result = self._run(get_credentials(db))
        assert result == (42, "from_db")

    def test_returns_none_when_no_env_and_no_db_row(self):
        cfg = self._mock_settings(api_id=0, api_hash="")
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        db = AsyncMock()
        db.execute = AsyncMock(return_value=mock_result)
        with patch("app.services.telegram_service.settings", cfg):
            result = self._run(get_credentials(db))
        assert result is None

    def test_env_creds_take_priority_over_db(self):
        # Even if DB has a row, env vars should win
        cfg = self._mock_settings(api_id=111, api_hash="env_hash")
        db = AsyncMock()  # should never be called
        with patch("app.services.telegram_service.settings", cfg):
            result = self._run(get_credentials(db))
        assert result == (111, "env_hash")
        db.execute.assert_not_called()


# ---------------------------------------------------------------------------
# check_session
# ---------------------------------------------------------------------------

class TestCheckSession:
    def test_returns_true_when_session_file_exists(self, tmp_path):
        session_file = tmp_path / "anon.session"
        session_file.write_bytes(b"fake_session_data")
        cfg = MagicMock()
        cfg.TELEGRAM_SESSION_PATH = session_file
        with patch("app.services.telegram_service.settings", cfg):
            assert check_session() is True

    def test_returns_false_when_session_file_absent(self, tmp_path):
        cfg = MagicMock()
        cfg.TELEGRAM_SESSION_PATH = tmp_path / "anon.session"
        with patch("app.services.telegram_service.settings", cfg):
            assert check_session() is False

    def test_returns_false_when_parent_dir_missing(self, tmp_path):
        cfg = MagicMock()
        cfg.TELEGRAM_SESSION_PATH = tmp_path / "subdir" / "anon.session"
        with patch("app.services.telegram_service.settings", cfg):
            assert check_session() is False

    def test_returns_true_for_empty_session_file(self, tmp_path):
        # An empty file still counts as "exists"
        session_file = tmp_path / "anon.session"
        session_file.write_bytes(b"")
        cfg = MagicMock()
        cfg.TELEGRAM_SESSION_PATH = session_file
        with patch("app.services.telegram_service.settings", cfg):
            assert check_session() is True


# ---------------------------------------------------------------------------
# make_client
# ---------------------------------------------------------------------------

class TestMakeClient:
    def _make(self, tmp_path, api_id=12345, api_hash="testhash"):
        session_path = tmp_path / "anon.session"
        cfg = MagicMock()
        cfg.TELEGRAM_SESSION_PATH = session_path
        mock_client_cls = MagicMock()
        mock_telethon = MagicMock()
        mock_telethon.TelegramClient = mock_client_cls
        with (
            patch("app.services.telegram_service.settings", cfg),
            patch.dict("sys.modules", {"telethon": mock_telethon}),
        ):
            make_client(api_id, api_hash)
        return mock_client_cls, session_path

    def test_creates_telegram_client(self, tmp_path):
        mock_cls, _ = self._make(tmp_path)
        assert mock_cls.called

    def test_session_path_has_no_dot_session_suffix(self, tmp_path):
        mock_cls, session_path = self._make(tmp_path)
        call_args = mock_cls.call_args[0]
        session_arg = call_args[0]
        assert not session_arg.endswith(".session"), (
            "Telethon appends .session itself — pass path without the suffix"
        )
        assert session_arg == str(session_path.with_suffix(""))

    def test_api_id_passed_correctly(self, tmp_path):
        mock_cls, _ = self._make(tmp_path, api_id=55555)
        call_args = mock_cls.call_args[0]
        assert call_args[1] == 55555

    def test_api_hash_passed_correctly(self, tmp_path):
        mock_cls, _ = self._make(tmp_path, api_hash="myhash99")
        call_args = mock_cls.call_args[0]
        assert call_args[2] == "myhash99"
