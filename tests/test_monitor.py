from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from pathlib import Path
from sqlite3 import connect as sqlite_connect

import pymysql
import pytest
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
    def __init__(self, *args, **kwargs):
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

    def monitoring_latest_runs(self):
        return self.monitoring_runs()[1:]

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
    monkeypatch.setattr(
        "airgap_sync.monitor.system.worker_status",
        lambda: {"status": "RUNNING", "detail": "running", "pid": "1"},
    )
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
            assert response.json()["status"] == "DEGRADED"
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
    connection.monitoring_latest_runs()
    connection.monitoring_active_runs()
    connection.monitoring_counts()
    connection.metadata_schema_version()
    assert statements and all(sql.lstrip().upper().startswith("SELECT ") for sql in statements)
    assert not any(
        word in sql.upper()
        for sql in statements
        for word in ("CREATE ", "ALTER ", "INSERT ", "UPDATE ", "DELETE ")
    )


def test_monitor_timeout_is_connection_only_and_worker_defaults_unchanged(tmp_path, monkeypatch):
    calls = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql):
            assert sql.startswith("SET SESSION time_zone")

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    monkeypatch.setattr("airgap_sync.destination.mysql.resolve_password", lambda _: "secret")
    monkeypatch.setattr(pymysql, "connect", lambda **kwargs: calls.append(kwargs) or Connection())
    cfg = config(tmp_path)
    DestinationMySQLConnection(cfg.mysql, cfg.destination, monitor_timeout=3).connect()
    DestinationMySQLConnection(cfg.mysql, cfg.destination).connect()
    assert calls[0]["connect_timeout"] == calls[0]["read_timeout"] == calls[0]["write_timeout"] == 3
    assert calls[1]["connect_timeout"] == cfg.mysql.connect_timeout
    assert "read_timeout" not in calls[1] and "write_timeout" not in calls[1]


def test_read_timeout_stops_queries_and_returns_local_facts(tmp_path, monkeypatch):
    calls = []

    class Timeout(FakeConnection):
        def monitoring_latest_runs(self):
            calls.append("latest")
            raise pymysql.err.OperationalError(2013, "password=private")

        def monitoring_runs(self):
            calls.append("runs")
            raise AssertionError("failed connection reused")

    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", Timeout)
    client = TestClient(create_app(config(tmp_path)))
    for path in ("/", "/api/dashboard"):
        response = client.get(path)
        assert response.status_code == 200
        assert "private" not in response.text
        if path == "/":
            assert "DEGRADED" in response.text or "CRITICAL" in response.text
            assert "Incoming candidates" in response.text
        else:
            data = response.json()
            assert data["query_error"] == "Metadata query unavailable"
            assert data["status"] == "DEGRADED" or data["status"] == "CRITICAL"
            assert data["system"]["hostname"]
    assert calls == ["latest", "latest"]


@pytest.mark.parametrize("failure_status", ["FAILED", "MISMATCH"])
def test_known_latest_failure_survives_runs_timeout(tmp_path, monkeypatch, failure_status):
    calls = []
    failed = replace(FakeConnection().monitoring_runs()[1], status=failure_status)

    class Timeout(FakeConnection):
        def monitoring_latest_runs(self):
            calls.append("latest")
            return [failed]

        def monitoring_runs(self):
            calls.append("runs")
            raise pymysql.err.OperationalError(2013, "password=private")

        def monitoring_active_runs(self):
            raise AssertionError("failed connection reused")

    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", Timeout)
    monkeypatch.setattr(
        "airgap_sync.monitor.system.worker_status",
        lambda: {"status": "RUNNING", "detail": "running", "pid": "1"},
    )
    client = TestClient(create_app(config(tmp_path)))
    api_response = client.get("/api/dashboard")
    assert api_response.status_code == 200
    assert "private" not in api_response.text
    data = api_response.json()
    assert data["status"] == "CRITICAL"
    assert data["counts"] is None
    assert data["query_error"] == "Metadata query unavailable"
    assert data["problems"] == [
        "Metadata query unavailable",
        f"Known {failure_status} run: failed (db.table)",
    ]
    assert data["system"]["hostname"]
    assert data["tables"][0]["latest_status"] == failure_status
    for path in ("/", "/tables", "/problems"):
        response = client.get(path)
        assert response.status_code == 200
        assert "private" not in response.text
        if path == "/":
            assert "CRITICAL" in response.text
            assert "Incoming candidates" in response.text
            assert "FAILED / MISMATCH</label><strong>—" in response.text
        if path == "/problems":
            assert f"Known {failure_status} run: failed" in response.text
            assert "Metadata query unavailable" in response.text
    assert calls == ["latest", "runs"] * 4


def test_known_failures_from_both_queries_are_deduplicated(tmp_path, monkeypatch):
    calls = []
    failed = FakeConnection().monitoring_runs()[1]
    mismatched = replace(failed, run_id="mismatched", table_name="other", status="MISMATCH")

    class Timeout(FakeConnection):
        def monitoring_latest_runs(self):
            calls.append("latest")
            return [failed]

        def monitoring_runs(self):
            calls.append("runs")
            return [failed, mismatched]

        def monitoring_active_runs(self):
            calls.append("active")
            raise pymysql.err.OperationalError(2013, "password=private")

        def monitoring_counts(self):
            raise AssertionError("failed connection reused")

    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", Timeout)
    data = snapshot(config(tmp_path))
    assert data["counts"] is None
    assert data["status"] == "CRITICAL"
    assert data["query_error"] == "Metadata query unavailable"
    assert data["problems"].count("Known FAILED run: failed (db.table)") == 1
    assert data["problems"].count("Known MISMATCH run: mismatched (db.other)") == 1
    assert calls == ["latest", "runs", "active"]


def test_timeout_without_observed_failure_is_degraded(tmp_path, monkeypatch):
    class Timeout(FakeConnection):
        def monitoring_latest_runs(self):
            return [FakeConnection().monitoring_runs()[0]]

        def monitoring_runs(self):
            raise pymysql.err.OperationalError(2013, "password=private")

    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", Timeout)
    monkeypatch.setattr(
        "airgap_sync.monitor.system.worker_status",
        lambda: {"status": "RUNNING", "detail": "running", "pid": "1"},
    )
    data = snapshot(config(tmp_path))
    assert data["status"] == "DEGRADED"
    assert data["counts"] is None
    assert data["problems"] == ["Metadata query unavailable"]


def test_available_counts_keep_global_failure_summary(tmp_path, monkeypatch):
    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", FakeConnection)
    data = snapshot(config(tmp_path))
    assert data["status"] == "CRITICAL"
    assert data["counts"]["FAILED"] == 1
    assert data["counts"]["MISMATCH"] == 1
    assert "Historical FAILED runs: 1" in data["problems"]
    assert "Historical MISMATCH runs: 1" in data["problems"]
    assert not any(problem.startswith("Known ") for problem in data["problems"])


def test_latest_run_sql_ignores_cleanup_updates_and_global_window(tmp_path):
    connection = DestinationMySQLConnection(config(tmp_path).mysql, config(tmp_path).destination)
    db = sqlite_connect(":memory:")
    db.execute("ATTACH DATABASE ':memory:' AS airgap_sync_meta")
    columns = [
        "run_id",
        "source_database",
        "table_name",
        "status",
        "row_count",
        "actual_row_count",
        "chunk_count",
        "source_created_at",
        "manifest_received_at",
        "validated_at",
        "import_started_at",
        "import_completed_at",
        "digest_verified_at",
        "applied_at",
        "updated_at",
        "last_error",
        "cleanup_error",
        "backup_cleanup_error",
        "incoming_cleanup_completed_at",
        "backup_cleanup_completed_at",
    ]
    db.execute(
        "CREATE TABLE airgap_sync_meta.runs (" + ",".join(f"{name} TEXT" for name in columns) + ")"
    )

    def add(run_id, table, status, source_time, received_time, updated_time):
        values = dict.fromkeys(columns)
        values.update(
            run_id=run_id,
            source_database="db",
            table_name=table,
            status=status,
            row_count=1,
            chunk_count=1,
            source_created_at=source_time,
            manifest_received_at=received_time,
            updated_at=updated_time,
        )
        db.execute(
            "INSERT INTO airgap_sync_meta.runs VALUES (" + ",".join("?" for _ in columns) + ")",
            [values[name] for name in columns],
        )

    add("old", "target", "VERIFIED", "2025-01-01", "2025-01-02", "2026-09-27")
    add("new", "target", "FAILED", "2025-02-01", "2025-02-02", "2025-02-02")
    add("tie-a", "tie", "FAILED", "2025-03-01", "2025-03-02", "2025-03-03")
    add("tie-b", "tie", "IMPORTING", "2025-03-01", "2025-03-02", "2025-03-02")
    add("outside", "outside", "IMPORTING", "2024-01-01", "2024-01-02", "2024-01-02")
    for number in range(201):
        add(f"noise{number}", "noise", "VERIFIED", "2026-01-01", "2026-01-02", "2026-01-02")
    connection._fetchall = lambda sql, params=(): db.execute(sql, params).fetchall()
    latest = {run.table_name: run for run in connection.monitoring_latest_runs()}
    assert latest["target"].run_id == "new"
    assert latest["target"].status == "FAILED"
    assert latest["outside"].run_id == "outside"
    assert latest["tie"].run_id == "tie-b"


def test_unverified_tables_show_status_and_unknown_statistics(tmp_path, monkeypatch):
    now = datetime.now(UTC).replace(tzinfo=None)

    class FirstRuns(FakeConnection):
        def monitoring_latest_runs(self):
            result = []
            for status, name in (("IMPORTING", "first"), ("FAILED", "broken")):
                result.append(
                    MonitoringRunRecord(
                        name,
                        "db",
                        name,
                        status,
                        5,
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
                        "private-password" if status == "FAILED" else None,
                        None,
                        None,
                        None,
                        None,
                    )
                )
            return result

        def all_versions(self):
            return []

    monkeypatch.setenv("SECRET_PASSWORD_ENV", "private-password")
    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", FirstRuns)
    client = TestClient(create_app(config(tmp_path)))
    data = client.get("/api/dashboard").json()
    assert {table["latest_status"] for table in data["tables"]} == {"IMPORTING", "FAILED"}
    assert all(
        table["row_count"] is None and table["data_age_seconds"] is None for table in data["tables"]
    )
    assert data["max_data_age"] is None
    page = client.get("/tables").text
    assert "[REDACTED]" in page and "private-password" not in page
    assert "IMPORTING" in page and "FAILED" in page


def test_system_degrades_and_deduplicates(tmp_path, monkeypatch):
    monkeypatch.setattr("airgap_sync.monitor.system.shutil.which", lambda _: None)
    assert worker_status()["status"] == "UNKNOWN"
    data = system_snapshot(tmp_path / "missing")
    assert data["incoming_candidates"] is None
    assert data["incoming_error"]
    devices = [
        Path(item["path"]).stat().st_dev for item in data["filesystems"] if item["free"] is not None
    ]
    assert len(devices) == len(set(devices))


@pytest.mark.parametrize("shared_device", [False, True])
def test_overview_filesystems_include_incoming_and_keep_uses(tmp_path, monkeypatch, shared_device):
    import airgap_sync.monitor.system as monitor_system

    real_stat = monitor_system.os.stat
    real_disk_usage = monitor_system.shutil.disk_usage

    def stat(path, *args, **kwargs):
        result = real_stat(path, *args, **kwargs)
        if str(path) == str(tmp_path):
            from os import stat_result

            fields = list(result)
            fields[2] = real_stat("/").st_dev if shared_device else -123
            return stat_result(fields)
        return result

    def disk_usage(path):
        if str(path) == str(tmp_path):
            return monitor_system.shutil._ntuple_diskusage(total=1000, used=950, free=50)
        return real_disk_usage(path)

    monkeypatch.setattr(monitor_system.os, "stat", stat)
    monkeypatch.setattr(monitor_system.shutil, "disk_usage", disk_usage)
    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", FakeConnection)
    response = TestClient(create_app(config(tmp_path))).get("/")
    assert response.status_code == 200
    assert str(tmp_path) in response.text
    assert "incoming" in response.text
    assert "root" in response.text
    data = system_snapshot(tmp_path)
    if shared_device:
        assert any(
            {location["label"] for location in fs["locations"]} >= {"root", "incoming"}
            for fs in data["filesystems"]
        )
    else:
        assert any(fs["free"] == 50 for fs in data["filesystems"])


def test_filesystem_collection_failure_is_explicit(tmp_path, monkeypatch):
    import airgap_sync.monitor.system as monitor_system

    real_usage = monitor_system.shutil.disk_usage
    monkeypatch.setattr(
        monitor_system.shutil,
        "disk_usage",
        lambda path: (
            (_ for _ in ()).throw(OSError("disk unavailable"))
            if str(path) == str(tmp_path)
            else real_usage(path)
        ),
    )
    data = system_snapshot(tmp_path)
    incoming = [fs for fs in data["filesystems"] if fs["label"] == "incoming"]
    assert incoming and incoming[0]["free"] is None


def test_system_page_shows_collected_worker_pid(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "airgap_sync.monitor.system.worker_status",
        lambda: {"status": "RUNNING", "detail": "running", "pid": "12345"},
    )
    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", FakeConnection)
    response = TestClient(create_app(config(tmp_path))).get("/system")
    assert response.status_code == 200
    assert "PID 12345" in response.text


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
