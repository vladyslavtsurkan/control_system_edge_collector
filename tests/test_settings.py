"""Tests for ``app.settings``."""

from types import SimpleNamespace

import pytest

from app.settings import Settings


class TestSettings:
    def test_loads_from_env(self, env_vars: None) -> None:
        s = Settings(_env_file=None)
        assert str(s.CLOUD_API_URL) == "https://api.test.com/"
        assert s.X_API_KEY.get_secret_value() == "test-key-123"
        assert str(s.AMQP_URL) == "amqp://guest:guest@localhost:5672/"
        assert s.AMQP_EXCHANGE == "test_exchange"
        assert s.AMQP_CONTROL_EXCHANGE == "test_control_exchange"

    def test_defaults(self, env_vars: None, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("AMQP_CONTROL_EXCHANGE", raising=False)
        s = Settings(_env_file=None)
        assert s.QUEUE_MAX_SIZE == 10_000
        assert str(s.EDGE_BUFFER_DB_PATH) == "edge_buffer.db"
        assert s.BATCH_SIZE == 100
        assert s.BATCH_TIMEOUT_S == 1.0
        assert s.AMQP_HEARTBEAT_S == 15
        assert s.CONFIG_REFRESH_INTERVAL_S == 300.0
        assert s.CONFIG_REFRESH_JITTER_S == 10.0
        assert s.HEALTH_PORT == 8080
        assert s.OPCUA_CERT_PATH is None
        assert s.AMQP_USE_TLS is True
        assert s.TLS_CA_CERT_PATH == "/app/certs/ca_certificate.pem"
        assert s.TLS_CLIENT_CERT_PATH == "/app/certs/collector_certificate.pem"
        assert s.TLS_CLIENT_KEY_PATH == "/app/certs/collector_key.pem"
        assert s.TLS_CHECK_HOSTNAME is True
        assert s.AMQP_CONTROL_EXCHANGE == "iiot_control_command"

    def test_organization_id_returns_dummy_when_tls_disabled(
        self, env_vars: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AMQP_USE_TLS", "false")
        s = Settings(_env_file=None)
        assert s.organization_id == "00000000-0000-0000-0000-000000000000"

    def test_organization_id_reads_cn_from_certificate(
        self,
        env_vars: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ) -> None:
        cert_path = tmp_path / "collector_certificate.pem"
        cert_path.write_text("dummy")
        monkeypatch.setenv("AMQP_USE_TLS", "true")
        monkeypatch.setenv("TLS_CLIENT_CERT_PATH", str(cert_path))

        certificate = SimpleNamespace(
            subject=SimpleNamespace(
                get_attributes_for_oid=lambda _oid: [SimpleNamespace(value="org-uuid")]
            )
        )
        monkeypatch.setattr(
            "app.settings.x509.load_pem_x509_certificate",
            lambda _bytes, _backend: certificate,
        )

        s = Settings(_env_file=None)
        assert s.organization_id == "org-uuid"

    def test_organization_id_raises_when_cn_missing(
        self,
        env_vars: None,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path,
    ) -> None:
        cert_path = tmp_path / "collector_certificate.pem"
        cert_path.write_text("dummy")
        monkeypatch.setenv("AMQP_USE_TLS", "true")
        monkeypatch.setenv("TLS_CLIENT_CERT_PATH", str(cert_path))

        certificate = SimpleNamespace(
            subject=SimpleNamespace(get_attributes_for_oid=lambda _oid: [])
        )
        monkeypatch.setattr(
            "app.settings.x509.load_pem_x509_certificate",
            lambda _bytes, _backend: certificate,
        )

        s = Settings(_env_file=None)
        with pytest.raises(ValueError, match="COMMON_NAME not found"):
            _ = s.organization_id

    def test_missing_required_var_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CLOUD_API_URL", raising=False)
        monkeypatch.delenv("X_API_KEY", raising=False)
        monkeypatch.delenv("AMQP_URL", raising=False)
        monkeypatch.delenv("AMQP_EXCHANGE", raising=False)
        with pytest.raises(Exception):
            Settings(_env_file=None)

    def test_amqp_heartbeat_can_be_overridden(
        self, env_vars: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AMQP_HEARTBEAT_S", "5")
        s = Settings(_env_file=None)
        assert s.AMQP_HEARTBEAT_S == 5
