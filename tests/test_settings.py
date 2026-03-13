"""Tests for ``app.settings``."""

import pytest

from app.settings import Settings


class TestSettings:
    def test_loads_from_env(self, env_vars: None) -> None:
        s = Settings()
        assert str(s.CLOUD_API_URL) == "https://api.test.com/"
        assert s.X_API_KEY.get_secret_value() == "test-key-123"
        assert str(s.AMQP_URL) == "amqp://guest:guest@localhost:5672/"
        assert s.AMQP_EXCHANGE == "test_exchange"

    def test_defaults(self, env_vars: None) -> None:
        s = Settings()
        assert s.QUEUE_MAX_SIZE == 10_000
        assert s.BATCH_SIZE == 100
        assert s.BATCH_TIMEOUT_S == 1.0
        assert s.HEALTH_PORT == 8080
        assert s.OPCUA_CERT_PATH is None

    def test_missing_required_var_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CLOUD_API_URL", raising=False)
        monkeypatch.delenv("X_API_KEY", raising=False)
        monkeypatch.delenv("AMQP_URL", raising=False)
        monkeypatch.delenv("AMQP_EXCHANGE", raising=False)
        with pytest.raises(Exception):
            Settings()
