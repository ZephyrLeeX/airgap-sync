from __future__ import annotations

from datetime import UTC, datetime, timedelta
from importlib.resources import files
from pathlib import Path

from click.testing import CliRunner
from fastapi.testclient import TestClient

from airgap_sync.cli import cli
from airgap_sync.common.models import AppConfig, DestinationConfig, MySQLConfig, Role
from airgap_sync.destination.mysql import (
    DestinationMySQLConnection,
    MonitoringRunRecord,
    TableVersion,
)
from airgap_sync.monitor.app import create_app
from airgap_sync.monitor.dashboard import snapshot
from airgap_sync.monitor.system import system_snapshot, worker_status


def config(tmp_path):
    return AppConfig(
        role=Role.DESTINATION,
        mysql=MySQLConfig(
            host="localhost", database="target", user="sync", password_env="SECRET_PASSWORD_ENV"
        ),
        destination=DestinationConfig(incoming_dir=tmp_path),
    )


class FakeConnection:
    def __init__(self, *args):
        self.sql = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def close(self):
        pass

    def ping(self):
        return "5.6.46"

    def metadata_schema_version(self):
        return 3

    def initialize_metadata(self):
        raise AssertionError("web must never initialize metadata")

    def monitoring_runs(self):
        now = datetime.now(UTC).replace(tzinfo=None)
        base = [
            "db",
            "table",
            "VERIFIED",
            20,
            20,
            2,
            now,
            now,
            now,
            now,
            now,
            now,
            now,
            now,
            None,
            None,
            None,
            now,
            now,
        ]
        return [
            MonitoringRunRecord("verified", *base),
            MonitoringRunRecord(
                "failed",
                "db",
                "table",
                "FAILED",
                20,
                None,
                2,
                now,
                now,
                None,
                None,
                None,
                None,
                None,
                now,
                "failed import",
                None,
                None,
                None,
                None,
            ),
        ]

    def monitoring_counts(self):
        return {"VERIFIED": 1, "FAILED": 1, "MISMATCH": 1, "cleanup_pending": 2}

    def monitoring_active_runs(self):
        return []

    def all_versions(self):
        now = datetime.now(UTC)
        return [
            TableVersion(
                "verified", "db", "table", now - timedelta(hours=3), 20, None, None, 5, now, now
            )
        ]


def test_pages_and_api_show_metadata_without_initialization(tmp_path, monkeypatch):
    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", FakeConnection)
    client = TestClient(create_app(config(tmp_path)))
    for path in ("/", "/tables", "/runs", "/system", "/problems"):
        response = client.get(path)
        assert response.status_code == 200, (path, response.text)
    data = client.get("/api/dashboard").json()
    assert data["rds"] == "CONNECTED"
    assert data["counts"]["MISMATCH"] == 1
    assert data["counts"]["cleanup_pending"] == 2
    assert data["latest_verified"]["run_id"] == "verified"
    assert data["tables"][0]["row_count"] == 20
    assert data["tables"][0]["this_run_net"] == 5
    assert data["tables"][0]["data_age_seconds"] >= 3 * 3600 - 2
    assert data["runs"][0]["timestamps"]["validated_at"].endswith("+00:00")


def test_disconnected_and_secrets_are_not_rendered(tmp_path, monkeypatch):
    class Failing(FakeConnection):
        def __enter__(self):
            raise RuntimeError("password=topsecret token=relay-secret")

    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", Failing)
    client = TestClient(create_app(config(tmp_path)))
    for path in ("/", "/system", "/api/dashboard"):
        response = client.get(path)
        assert response.status_code == 200
        assert "DISCONNECTED" in response.text
        assert "topsecret" not in response.text
        assert "relay-secret" not in response.text
        assert "SECRET_PASSWORD_ENV" not in response.text
        if path == "/api/dashboard":
            assert response.json()["system"]["hostname"]
        else:
            assert "hostname" in response.text.lower()


def test_schema_mismatch_never_queries_or_migrates(tmp_path, monkeypatch):
    class OldSchema(FakeConnection):
        def metadata_schema_version(self):
            return 2

        def monitoring_runs(self):
            raise AssertionError("old schema must not be queried")

    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", OldSchema)
    data = snapshot(config(tmp_path))
    assert data["metadata_status"] == "schema mismatch"
    assert data["runs"] == []


def test_active_and_cleanup_pending_are_metadata_facts(tmp_path, monkeypatch):
    now = datetime.now(UTC).replace(tzinfo=None)

    class Active(FakeConnection):
        def monitoring_active_runs(self):
            return [
                MonitoringRunRecord(
                    "active",
                    "db",
                    "table",
                    "IMPORTING",
                    10,
                    None,
                    1,
                    now,
                    now,
                    now,
                    now,
                    None,
                    None,
                    None,
                    now,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
            ]

    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", Active)
    data = snapshot(config(tmp_path))
    assert data["active_run"]["run_id"] == "active"
    run = Active().monitoring_active_runs()[0]
    assert not run.cleanup_pending
    verified = FakeConnection().monitoring_runs()[0]
    assert not verified.cleanup_pending
    pending = MonitoringRunRecord(
        "pending",
        "db",
        "table",
        "VERIFIED",
        1,
        1,
        1,
        now,
        now,
        now,
        now,
        now,
        now,
        now,
        now,
        None,
        None,
        None,
        None,
        now,
    )
    assert pending.cleanup_pending


def test_persisted_error_redacts_configured_secret(tmp_path, monkeypatch):
    monkeypatch.setenv("SECRET_PASSWORD_ENV", "private-password")

    class SecretError(FakeConnection):
        def monitoring_runs(self):
            now = datetime.now(UTC).replace(tzinfo=None)
            return [
                MonitoringRunRecord(
                    "failed",
                    "db",
                    "table",
                    "FAILED",
                    1,
                    None,
                    1,
                    now,
                    now,
                    None,
                    None,
                    None,
                    None,
                    None,
                    now,
                    "private-password",
                    None,
                    None,
                    None,
                    None,
                )
            ]

    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", SecretError)
    response = TestClient(create_app(config(tmp_path))).get("/runs")
    assert "private-password" not in response.text
    assert "[REDACTED]" in response.text


def test_monitoring_mysql_queries_are_select_only(tmp_path):
    connection = DestinationMySQLConnection(config(tmp_path).mysql, config(tmp_path).destination)
    statements = []
    connection._fetchall = lambda sql, params=(): statements.append(sql) or []
    connection._fetchone = lambda sql, params=(): statements.append(sql) or (0,)
    connection.monitoring_runs()
    connection.monitoring_active_runs()
    connection.monitoring_counts()
    connection.metadata_schema_version()
    assert statements and all(sql.lstrip().upper().startswith("SELECT ") for sql in statements)
    assert not any(
        word in sql.upper()
        for sql in statements
        for word in ("CREATE ", "ALTER ", "INSERT ", "UPDATE ", "DELETE ")
    )


def test_system_degrades_and_deduplicates(tmp_path, monkeypatch):
    monkeypatch.setattr("airgap_sync.monitor.system.shutil.which", lambda _: None)
    assert worker_status()["status"] == "UNKNOWN"
    data = system_snapshot(tmp_path / "missing")
    assert data["incoming_candidates"] is None
    assert data["incoming_error"]
    devices = [Path(item["path"]).stat().st_dev for item in data["filesystems"]]
    assert len(devices) == len(set(devices))


def test_malformed_proc_does_not_break_system_page(tmp_path, monkeypatch):
    original_read = Path.read_text

    def broken_proc(path, *args, **kwargs):
        if str(path).startswith("/proc/"):
            raise OSError("proc unavailable")
        return original_read(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", broken_proc)
    data = system_snapshot(tmp_path)
    assert data["hostname"]
    assert data["uptime"] is None
    assert data["memory_total"] is None


def test_monitor_assets_are_package_relative():
    root = files("airgap_sync.monitor")
    assert root.joinpath("templates/overview.html").is_file()
    assert root.joinpath("static/monitor.css").is_file()


def test_monitor_cli_has_loopback_defaults():
    result = CliRunner().invoke(cli, ["destination", "monitor-web", "--help"])
    assert result.exit_code == 0
    assert "127.0.0.1" in result.output
    assert "8080" in result.output
