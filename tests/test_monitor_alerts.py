"""M4 alert state, evidence and v1 migration with controlled time."""

import json
import os
import re
import sqlite3
import subprocess
import threading
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from airgap_sync.common.models import (
    AppConfig,
    DestinationConfig,
    MonitorAlertsConfig,
    MonitorIngestConfig,
    MySQLConfig,
    Role,
)
from airgap_sync.common.transport import transport_filename
from airgap_sync.monitor import store
from airgap_sync.monitor.alerts import (
    Decision,
    IncomingScanner,
    _incoming_decisions,
    _manifest_bytes,
    _rds_decisions,
    _source_decisions,
    apply,
    disk_decision,
)


def test_state_lifecycle_and_unknown(tmp_path):
    path = tmp_path / "monitor.db"
    store.initialize(path)
    with store.connect(path, write=True) as db:
        issue = Decision("SOURCE_WORKER", "source-01", "TRUE", "CRITICAL", "Stopped", 100)
        apply(db, issue, 100)
        apply(db, issue, 110)
        apply(db, Decision("SOURCE_WORKER", "source-01", "UNKNOWN"), 120)
        assert db.execute("SELECT count(*) FROM alerts").fetchone()[0] == 1
        row = db.execute("SELECT * FROM alerts").fetchone()
        assert (row["status"], row["evaluation_state"], row["last_seen_at"]) == (
            "OPEN",
            "UNKNOWN",
            110,
        )
        apply(db, Decision("SOURCE_WORKER", "source-01", "TRUE", "WARNING", "Degraded", 130), 130)
        assert (
            db.execute("SELECT severity FROM alerts WHERE status='OPEN'").fetchone()[0] == "WARNING"
        )
        apply(db, Decision("SOURCE_WORKER", "source-01", "FALSE", observed_at=140), 140)
        apply(db, issue, 150)  # old evidence cannot reopen after recovery
        statuses = [row[0] for row in db.execute("SELECT status FROM alerts ORDER BY id")]
        assert statuses == ["RECOVERED"]
        apply(db, Decision("SOURCE_WORKER", "source-01", "TRUE", "CRITICAL", "Stopped", 151), 151)
        assert db.execute("SELECT count(*) FROM alerts WHERE status='OPEN'").fetchone()[0] == 1
        row = db.execute("SELECT * FROM alerts WHERE status='OPEN'").fetchone()
        names = row.keys()
        columns = [column for column in names if column != "id"]
        try:
            placeholders = ",".join("?" for _ in columns)
            db.execute(
                f"INSERT INTO alerts ({','.join(columns)}) VALUES ({placeholders})",
                tuple(row[column] for column in columns),
            )
        except sqlite3.IntegrityError:
            db.rollback()
        else:
            raise AssertionError("duplicate OPEN accepted")


def test_disk_or_boundary_and_partial_unknown():
    cfg = MonitorAlertsConfig()
    gib = 1024**3
    assert disk_decision("DISK", "v", 51 * gib, 4.9, cfg, 1).severity == "EMERGENCY"
    assert disk_decision("DISK", "v", 9 * gib, 50, cfg, 1).severity == "EMERGENCY"
    assert disk_decision("DISK", "v", 50 * gib, 20, cfg, 1).state == "FALSE"
    assert disk_decision("DISK", "v", 50 * gib, None, cfg, 1).state == "UNKNOWN"
    assert disk_decision("DISK", "v", 19 * gib, None, cfg, 1).state == "TRUE"


def test_expected_heartbeat_persists_and_newer_sample_recovers(tmp_path):
    path = tmp_path / "monitor.db"
    store.initialize(path)
    cfg = MonitorAlertsConfig(expected_sources=["source-01"])
    with store.connect(path, write=True) as db:
        assert list(_source_decisions(db, cfg, 100))[0].state == "UNKNOWN"
        assert list(_source_decisions(db, cfg, 801))[0].state == "TRUE"
        assert (
            db.execute(
                "SELECT value FROM alert_state WHERE key='expected-source:source-01'"
            ).fetchone()[0]
            == "100"
        )
        payload = {
            "worker": {"status": "RUNNING", "task_status": "Ready"},
            "filesystems": [],
            "agent_version": "0.1.0",
            "cycle": None,
            "current_run": None,
            "last_run": None,
        }
        db.execute(
            "INSERT INTO node_samples VALUES (?,?,?,?,?)",
            ("a", "source-01", 810, 811, json.dumps(payload)),
        )
        db.execute("INSERT INTO nodes VALUES (?,?,?)", ("source-01", 811, "a"))
        db.commit()
        decisions = list(_source_decisions(db, cfg, 812))
        assert decisions[0].state == "FALSE"
        assert decisions[1].state == "FALSE"


def test_migrate_v1_retains_data_and_rejects_unknown(tmp_path):
    path = tmp_path / "monitor.db"
    # Build a true v1 database using the original M3 DDL prefix.
    with sqlite3.connect(path) as db:
        db.executescript(
            store.SCHEMA[: store.SCHEMA.index("CREATE TABLE alerts (")]
            + "PRAGMA user_version=1; PRAGMA application_id=1095191859;"
        )
        db.execute("INSERT INTO content_receipts VALUES ('digest',123)")
        db.execute("INSERT INTO node_samples VALUES ('digest','source-01',100,123,'{}')")
        db.execute("INSERT INTO nodes VALUES ('source-01',123,'digest')")
        db.execute("INSERT INTO filesystem_samples VALUES ('digest',0,'{}')")
        db.execute("INSERT INTO managed_storage_samples VALUES ('digest',100,'{}')")
        db.execute("INSERT INTO transfer_receipts VALUES ('file','digest',123)")
        db.execute("INSERT INTO observations VALUES ('file','stat',100,123)")
    store.initialize(path)
    with store.connect(path) as db:
        assert db.execute("SELECT received FROM content_receipts").fetchone()[0] == 123
        assert db.execute("SELECT latest_hash FROM nodes").fetchone()[0] == "digest"
        for table in (
            "node_samples",
            "filesystem_samples",
            "managed_storage_samples",
            "transfer_receipts",
            "observations",
        ):
            assert db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 1
        assert db.execute("PRAGMA user_version").fetchone()[0] == 3
    with sqlite3.connect(path) as db:
        db.execute("PRAGMA user_version=999")
    try:
        store.initialize(path)
    except ValueError:
        pass
    else:
        raise AssertionError("unknown schema accepted")


def test_migration_failure_rolls_back_and_retries(tmp_path):
    path = tmp_path / "monitor.db"
    with sqlite3.connect(path) as db:
        db.executescript(
            store.SCHEMA[: store.SCHEMA.index("CREATE TABLE alerts (")]
            + "PRAGMA user_version=1; PRAGMA application_id=1095191859;"
        )
        db.execute("INSERT INTO content_receipts VALUES ('keep',7)")
        db.execute("CREATE TABLE alerts (collision INTEGER)")
    try:
        store.initialize(path)
    except sqlite3.OperationalError:
        pass
    else:
        raise AssertionError("migration collision accepted")
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1
        assert db.execute("SELECT received FROM content_receipts").fetchone()[0] == 7
        assert (
            db.execute("SELECT name FROM sqlite_master WHERE name='alert_state'").fetchone() is None
        )
        db.execute("DROP TABLE alerts")
    store.initialize(path)
    with store.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 3
        assert db.execute("SELECT received FROM content_receipts").fetchone()[0] == 7


def test_alert_api_is_read_only(tmp_path):
    from airgap_sync.monitor.alerts import read_alerts

    cfg = MonitorIngestConfig(incoming=tmp_path / "incoming", db_path=tmp_path / "monitor.db")
    store.initialize(cfg.db_path)
    with store.connect(cfg.db_path, write=True) as db:
        apply(db, Decision("TEST", "object", "TRUE", "WARNING", "Reason", 1), 1)
        apply(db, Decision("TEST", "object-2", "TRUE", "CRITICAL", "Second", 2), 2)
    first = read_alerts(cfg, status="OPEN", limit=1)
    assert first["alerts"][0]["message"] == "Second"
    assert first["next_cursor"] is not None
    second = read_alerts(cfg, status="OPEN", limit=1, before=first["next_cursor"])
    assert second["alerts"][0]["message"] == "Reason"
    assert read_alerts(cfg, status="RECOVERED")["alerts"] == []


def test_incoming_complete_stable_and_restart(tmp_path, monkeypatch):
    class Metadata:
        known = {}

        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def close(self):
            pass

        def ping(self):
            return "5.6"

        def metadata_schema_version(self):
            from airgap_sync.destination.mysql import METADATA_SCHEMA_VERSION

            return METADATA_SCHEMA_VERSION

        def monitoring_known_run_ids(self, ids):
            return {item: self.known[item] for item in ids if item in self.known}

    monkeypatch.setattr("airgap_sync.monitor.alerts.DestinationMySQLConnection", Metadata)
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    run_id = "20260928T080000Z-11111111"
    base = f"airgap-v1--{run_id}--"
    manifest = {
        "protocol_version": 1,
        "run_id": run_id,
        "run_type": "FULL_SNAPSHOT",
        "source": {"database": "db", "table": "t"},
        "schema": {"file": "schema.sql", "sha256": "0" * 64},
        "columns": ["id"],
        "row_count": 1,
        "chunks": [
            {
                "sequence": 1,
                "file": "chunk-000001.jsonl.zst",
                "rows": 1,
                "uncompressed_bytes": 1,
                "compressed_bytes": 3,
                "sha256": "0" * 64,
            }
        ],
        "verification": {
            "algorithm": "test",
            "row_count": 1,
            "digest_a": "0" * 64,
            "digest_b": "0" * 64,
        },
        "created_at": "2026-09-28T08:00:00Z",
    }
    (incoming / f"{base}manifest.json").write_text(json.dumps(manifest))
    (incoming / f"{base}schema.sql").write_text("ddl")
    path = tmp_path / "monitor.db"
    store.initialize(path)
    config = SimpleNamespace(destination=SimpleNamespace(incoming_dir=incoming), mysql=None)
    cfg = MonitorAlertsConfig(incoming_settle_seconds=60)
    with store.connect(path, write=True) as db:
        with _scanner(incoming) as scanner:
            assert list(_incoming_decisions(config, db, cfg, 0, scanner)) == []
        (incoming / f"{base}chunk-000001.jsonl.zst").write_bytes(b"abc")
        with _scanner(incoming) as scanner:
            assert list(_incoming_decisions(config, db, cfg, 1, scanner)) == []
        with _scanner(incoming) as scanner:
            assert list(_incoming_decisions(config, db, cfg, 62, scanner))[0].state == "UNKNOWN"
        with _scanner(incoming) as scanner:
            decisions = list(_incoming_decisions(config, db, cfg, 7302, scanner))
        assert len(decisions) == 1 and decisions[0].state == "TRUE"
        apply(db, decisions[0], 7302)
        (incoming / f"{base}manifest.json").unlink()
        with _scanner(incoming) as scanner:
            assert list(_incoming_decisions(config, db, cfg, 7400, scanner)) == []
        assert db.execute("SELECT status FROM alerts").fetchone()[0] == "OPEN"
        (incoming / f"{base}manifest.json").write_text(json.dumps(manifest))
        Metadata.known[run_id] = 10  # RDS evidence can predate first local file observation.
        with _scanner(incoming) as scanner:
            assert list(_incoming_decisions(config, db, cfg, 7401, scanner)) == []
        with _scanner(incoming) as scanner:
            resolved = list(_incoming_decisions(config, db, cfg, 7470, scanner))
        assert resolved[0].state == "FALSE"
        apply(db, resolved[0], 7470)
        assert db.execute("SELECT status FROM alerts").fetchone()[0] == "RECOVERED"


def test_incoming_old_rds_evidence_can_recover(tmp_path):
    path = tmp_path / "monitor.db"
    store.initialize(path)
    # A processed Run may leave a complete file set during cleanup. The RDS
    # record is positive evidence that processing has already begun.
    candidate = Decision("INCOMING_STALE", "run", "TRUE", observed_at=100, run_id="run")
    with store.connect(path, write=True) as db:
        apply(db, candidate, 100)
        apply(
            db,
            Decision(
                "INCOMING_STALE", "run", "FALSE", observed_at=10, evidence_key="run", run_id="run"
            ),
            101,
        )
        assert db.execute("SELECT status FROM alerts").fetchone()[0] == "RECOVERED"


def test_alert_routes_escape_and_keep_get_read_only(tmp_path, monkeypatch):
    from airgap_sync.monitor.app import create_app

    cfg = MonitorIngestConfig(incoming=tmp_path, db_path=tmp_path / "monitor.db")
    store.initialize(cfg.db_path)
    config = AppConfig(
        role=Role.DESTINATION,
        mysql=MySQLConfig(
            host="localhost", database="target", user="sync", password_env="TEST_PASSWORD"
        ),
        destination=DestinationConfig(incoming_dir=tmp_path, report_timezone="Asia/Shanghai"),
        monitor_ingest=cfg,
        monitor_alerts=MonitorAlertsConfig(),
    )
    with store.connect(cfg.db_path, write=True) as db:
        apply(db, Decision("TEST", "<script>", "TRUE", "WARNING", "<script>", 1), 1)
    monkeypatch.setattr(
        "airgap_sync.monitor.app.snapshot",
        lambda _: {
            "status": "HEALTHY",
            "timezone": "Asia/Shanghai",
            "generated_at": "2026-09-28T00:00:00+00:00",
            "query_error": None,
        },
    )
    client = TestClient(create_app(config))
    assert (
        client.get("/api/alerts?status=OPEN&limit=1").json()["alerts"][0]["object_id"] == "<script>"
    )
    html = client.get("/alerts?status=OPEN").text
    assert "&lt;script&gt;" in html and "<script>" not in html
    assert "1970-01-01 08:00:01" in html
    assert client.get("/api/alerts?limit=201").status_code == 422
    assert client.post("/api/alerts").status_code == 405
    with store.connect(cfg.db_path) as db:
        assert db.execute("SELECT count(*) FROM alerts").fetchone()[0] == 1


def test_periodic_evaluation_without_new_telemetry(tmp_path, monkeypatch):
    from airgap_sync.monitor.alerts import evaluate

    cfg = MonitorIngestConfig(incoming=tmp_path, db_path=tmp_path / "monitor.db")
    store.initialize(cfg.db_path)
    config = AppConfig(
        role=Role.DESTINATION,
        mysql=MySQLConfig(
            host="localhost", database="target", user="sync", password_env="TEST_PASSWORD"
        ),
        destination=DestinationConfig(incoming_dir=tmp_path),
        monitor_ingest=cfg,
        monitor_alerts=MonitorAlertsConfig(expected_sources=["source-01"]),
    )
    monkeypatch.setattr("airgap_sync.monitor.alerts._destination_decisions", lambda *_: ())
    monkeypatch.setattr("airgap_sync.monitor.alerts._rds_decisions", lambda *_: ())
    with store.connect(cfg.db_path, write=True) as db:
        evaluate(config, db, 100)
        assert db.execute("SELECT count(*) FROM alerts").fetchone()[0] == 0
        evaluate(config, db, 801)
        evaluate(config, db, 802)
        assert db.execute("SELECT count(*) FROM alerts").fetchone()[0] == 1
        sample = {
            "worker": {"status": "RUNNING", "task_status": "Ready"},
            "filesystems": [],
            "agent_version": "0.1.0",
            "cycle": None,
            "current_run": None,
            "last_run": None,
        }
        db.execute(
            "INSERT INTO node_samples VALUES (?,?,?,?,?)",
            ("hash", "source-01", 810, 811, json.dumps(sample)),
        )
        db.execute("INSERT INTO nodes VALUES (?,?,?)", ("source-01", 811, "hash"))
        db.commit()
        evaluate(config, db, 812)
        assert db.execute("SELECT status FROM alerts").fetchone()[0] == "RECOVERED"


def test_config_change_is_disabled_not_recovered_and_open_retained(tmp_path, monkeypatch):
    from airgap_sync.monitor.alerts import evaluate

    cfg = MonitorIngestConfig(incoming=tmp_path, db_path=tmp_path / "monitor.db")
    store.initialize(cfg.db_path)
    base = AppConfig(
        role=Role.DESTINATION,
        mysql=MySQLConfig(
            host="localhost", database="target", user="sync", password_env="TEST_PASSWORD"
        ),
        destination=DestinationConfig(incoming_dir=tmp_path),
        monitor_ingest=cfg,
        monitor_alerts=MonitorAlertsConfig(
            expected_sources=["source-01"], recovered_retention_days=1
        ),
    )
    monkeypatch.setattr("airgap_sync.monitor.alerts._destination_decisions", lambda *_: ())
    monkeypatch.setattr("airgap_sync.monitor.alerts._rds_decisions", lambda *_: ())
    with store.connect(cfg.db_path, write=True) as db:
        evaluate(base, db, 1)
        evaluate(base, db, 2000)
        assert db.execute("SELECT status FROM alerts").fetchone()[0] == "OPEN"
        changed = base.model_copy(
            update={
                "monitor_alerts": base.monitor_alerts.model_copy(
                    update={"heartbeat_warning_seconds": 700}
                )
            }
        )
        evaluate(changed, db, 2100)
        assert [row[0] for row in db.execute("SELECT status FROM alerts ORDER BY id")] == [
            "DISABLED",
            "OPEN",
        ]
        evaluate(changed, db, 90000)
        assert db.execute("SELECT count(*) FROM alerts WHERE status='DISABLED'").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM alerts WHERE status='OPEN'").fetchone()[0] == 1


def test_disabled_event_can_reopen_from_same_still_abnormal_evidence(tmp_path):
    from airgap_sync.monitor.alerts import disable_all

    path = tmp_path / "monitor.db"
    store.initialize(path)
    decision = Decision("SOURCE_DISK", "source-01:D", "TRUE", "WARNING", "Low", 100)
    with store.connect(path, write=True) as db:
        apply(db, decision, 100)
        disable_all(db, 101)
        apply(db, decision, 102)
        assert [row[0] for row in db.execute("SELECT status FROM alerts ORDER BY id")] == [
            "DISABLED",
            "OPEN",
        ]


def test_background_evaluates_without_web_or_telemetry(tmp_path, monkeypatch):
    from airgap_sync.monitor.ingest import BackgroundIngest

    cfg = MonitorIngestConfig(
        incoming=tmp_path / "incoming", db_path=tmp_path / "monitor.db", poll_seconds=1
    )
    store.initialize(cfg.db_path)
    config = AppConfig(
        role=Role.DESTINATION,
        mysql=MySQLConfig(
            host="localhost", database="target", user="sync", password_env="TEST_PASSWORD"
        ),
        destination=DestinationConfig(incoming_dir=tmp_path),
        monitor_ingest=cfg,
        monitor_alerts=MonitorAlertsConfig(expected_sources=["source-01"]),
    )
    called = threading.Event()

    class FakeIngestor:
        def __init__(self, *_):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def tick(self):
            return {"error": 0}

    monkeypatch.setattr("airgap_sync.monitor.ingest.Ingestor", FakeIngestor)
    monkeypatch.setattr(
        "airgap_sync.monitor.alerts.evaluate", lambda *_args, **_kwargs: called.set()
    )
    background = BackgroundIngest(cfg, config)
    background.start()
    try:
        assert called.wait(3)
    finally:
        background.stop()


def test_destination_worker_requires_running_process(monkeypatch):
    from airgap_sync.monitor.system import worker_status

    monkeypatch.setattr("airgap_sync.monitor.system.sys.platform", "linux")
    monkeypatch.setattr("airgap_sync.monitor.system.shutil.which", lambda _: "/usr/bin/systemctl")
    monkeypatch.setattr(
        "airgap_sync.monitor.system.subprocess.run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=[], returncode=0, stdout="ActiveState=active\nSubState=exited\nMainPID=0\n"
        ),
    )
    assert worker_status()["status"] == "NOT RUNNING"


def test_source_run_failure_replaced_by_later_delivery(tmp_path):
    path = tmp_path / "monitor.db"
    store.initialize(path)
    cfg = MonitorAlertsConfig()
    base = {
        "worker": {"status": "RUNNING", "task_status": "Running"},
        "filesystems": [],
        "agent_version": "0.1.0",
        "cycle": None,
        "current_run": None,
        "last_run": None,
    }
    failed = {
        "run_id": "old",
        "table": "t",
        "status": "FAILED",
        "created_at": "2026-09-28T08:00:00Z",
    }
    success = {
        "run_id": "new",
        "table": "t",
        "status": "DELIVERED",
        "created_at": "2026-09-28T09:00:00Z",
    }
    with store.connect(path, write=True) as db:
        db.execute(
            "INSERT INTO node_samples VALUES (?,?,?,?,?)",
            ("old", "source-01", 100, 100, json.dumps({**base, "last_run": failed})),
        )
        db.execute("INSERT INTO nodes VALUES (?,?,?)", ("source-01", 100, "old"))
        db.commit()
        first = [d for d in _source_decisions(db, cfg, 101) if d.kind == "SOURCE_RUN"][0]
        assert first.state == "TRUE"
        apply(db, first, 101)
        db.execute(
            "INSERT INTO node_samples VALUES (?,?,?,?,?)",
            ("new", "source-01", 200, 200, json.dumps({**base, "last_run": success})),
        )
        db.execute("UPDATE nodes SET latest_hash='new' WHERE node_id='source-01'")
        db.commit()
        second = [d for d in _source_decisions(db, cfg, 201) if d.kind == "SOURCE_RUN"][0]
        assert second.state == "FALSE" and second.fingerprint == first.fingerprint
        apply(db, second, 201)
        assert db.execute("SELECT status FROM alerts").fetchone()[0] == "RECOVERED"


def test_stale_and_future_source_evidence_do_not_falsely_recover(tmp_path):
    path = tmp_path / "monitor.db"
    store.initialize(path)
    cfg = MonitorAlertsConfig()
    payload = {
        "worker": {"status": "STOPPED", "task_status": "Ready"},
        "filesystems": [{"mount": "D:", "free_bytes": 1, "free_percent": 1}],
        "agent_version": "0.1.0",
        "cycle": None,
        "current_run": None,
        "last_run": None,
    }
    with store.connect(path, write=True) as db:
        db.execute(
            "INSERT INTO node_samples VALUES (?,?,?,?,?)",
            ("first", "source-01", 100, 100, json.dumps(payload)),
        )
        db.execute("INSERT INTO nodes VALUES (?,?,?)", ("source-01", 100, "first"))
        db.commit()
        first = list(_source_decisions(db, cfg, 101))
        for decision in first:
            if decision.state == "TRUE":
                apply(db, decision, 101)
        payload["worker"]["status"] = "RUNNING"
        payload["filesystems"][0].update(free_bytes=100 * 1024**3, free_percent=90)
        # A delayed sample with older captured_at is not the M3 latest sample.
        db.execute(
            "INSERT INTO node_samples VALUES (?,?,?,?,?)",
            ("older", "source-01", 99, 2000, json.dumps(payload)),
        )
        db.commit()
        stale = list(_source_decisions(db, cfg, 2000))
        assert next(d for d in stale if d.kind == "SOURCE_WORKER").state == "UNKNOWN"
        assert not any(d.state == "FALSE" and d.kind == "SOURCE_DISK" for d in stale)
        for decision in stale:
            apply(db, decision, 2000)
        assert db.execute("SELECT count(*) FROM alerts WHERE status='OPEN'").fetchone()[0] >= 2
        # M3 allows five minutes of future skew; beyond that is not fresh evidence.
        db.execute(
            "INSERT INTO node_samples VALUES (?,?,?,?,?)",
            ("future", "source-01", 2500, 2001, json.dumps(payload)),
        )
        db.execute("UPDATE nodes SET latest_hash='future' WHERE node_id='source-01'")
        db.commit()
        future = list(_source_decisions(db, cfg, 2002))
        assert next(d for d in future if d.kind == "SOURCE_WORKER").state == "UNKNOWN"


class _scanner:
    def __init__(self, path):
        self.scanner = IncomingScanner(path)

    def __enter__(self):
        return self.scanner

    def __exit__(self, *_):
        self.scanner.close()


def test_heartbeat_reopens_after_recovered_sample_ages_and_restart(tmp_path):
    path = tmp_path / "monitor.db"
    store.initialize(path)
    cfg = MonitorAlertsConfig()
    payload = {
        "worker": {"status": "RUNNING", "task_status": "Ready"},
        "filesystems": [],
        "agent_version": "0.1.0",
        "cycle": None,
        "current_run": None,
        "last_run": None,
    }
    with store.connect(path, write=True) as db:
        db.execute(
            "INSERT INTO node_samples VALUES (?,?,?,?,?)",
            ("old", "node", 100, 100, json.dumps(payload)),
        )
        db.execute("INSERT INTO nodes VALUES (?,?,?)", ("node", 100, "old"))
        apply(
            db,
            next(d for d in _source_decisions(db, cfg, 701) if d.kind == "SOURCE_HEARTBEAT"),
            701,
        )
        db.execute(
            "INSERT INTO node_samples VALUES (?,?,?,?,?)",
            ("fresh", "node", 800, 800, json.dumps(payload)),
        )
        db.execute("UPDATE nodes SET latest_hash='fresh' WHERE node_id='node'")
        recovered = next(d for d in _source_decisions(db, cfg, 801) if d.kind == "SOURCE_HEARTBEAT")
        assert recovered.state == "FALSE"
        apply(db, recovered, 801)
        apply(
            db,
            Decision("SOURCE_HEARTBEAT", "node", "TRUE", observed_at=100, evidence_key="old"),
            802,
        )
        assert db.execute("SELECT count(*) FROM alerts").fetchone()[0] == 1
    with store.connect(path, write=True) as db:
        for now in (1401, 1402, 2001):
            decision = next(
                d for d in _source_decisions(db, cfg, now) if d.kind == "SOURCE_HEARTBEAT"
            )
            apply(db, decision, now)
        rows = db.execute("SELECT status,severity,observed_at FROM alerts ORDER BY id").fetchall()
        assert [tuple(row) for row in rows] == [
            ("RECOVERED", "WARNING", 800),
            ("OPEN", "CRITICAL", 800),
        ]


def test_freshness_reopens_after_same_verified_version_ages(tmp_path, monkeypatch):
    path = tmp_path / "monitor.db"
    store.initialize(path)
    cfg = MonitorAlertsConfig()
    version = SimpleNamespace(
        source_database="db",
        table_name="table",
        source_created_at=0,
        verified_at=10,
        run_id="old",
    )

    class Metadata:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def close(self):
            pass

        def ping(self):
            pass

        def metadata_schema_version(self):
            from airgap_sync.destination.mysql import METADATA_SCHEMA_VERSION

            return METADATA_SCHEMA_VERSION

        def monitoring_latest_versions(self, *_args):
            return [version]

        def monitoring_latest_runs(self, *_args):
            return []

        def monitoring_known_run_ids(self, _ids):
            return {}

    monkeypatch.setattr("airgap_sync.monitor.alerts.DestinationMySQLConnection", Metadata)
    config = SimpleNamespace(
        mysql=SimpleNamespace(database="db"),
        enabled_tables=[],
        destination=None,
        monitor_ingest=SimpleNamespace(db_path=path),
    )
    with store.connect(path, write=True) as db:
        for now in (cfg.freshness_warning_seconds + 1, cfg.freshness_warning_seconds + 2):
            apply(
                db,
                next(d for d in _rds_decisions(config, cfg, now, db) if d.kind == "FRESHNESS"),
                now,
            )
        version.source_created_at = cfg.freshness_warning_seconds + 3
        version.verified_at = cfg.freshness_warning_seconds + 4
        version.run_id = "fresh"
        now = cfg.freshness_warning_seconds + 5
        apply(
            db, next(d for d in _rds_decisions(config, cfg, now, db) if d.kind == "FRESHNESS"), now
        )
    with store.connect(path, write=True) as db:
        for now in (
            version.source_created_at + cfg.freshness_warning_seconds + 1,
            version.source_created_at + cfg.freshness_warning_seconds + 2,
            version.source_created_at + cfg.freshness_critical_seconds + 1,
        ):
            apply(
                db,
                next(d for d in _rds_decisions(config, cfg, now, db) if d.kind == "FRESHNESS"),
                now,
            )
        assert [
            tuple(row)
            for row in db.execute("SELECT status,severity,observed_at FROM alerts ORDER BY id")
        ] == [
            ("RECOVERED", "WARNING", version.verified_at),
            ("OPEN", "CRITICAL", version.verified_at),
        ]


def test_observed_rules_reject_old_duplicate_and_out_of_order_evidence(tmp_path):
    path = tmp_path / "monitor.db"
    store.initialize(path)
    with store.connect(path, write=True) as db:
        for kind in ("SOURCE_WORKER", "SOURCE_DISK", "DESTINATION_RUN"):
            apply(db, Decision(kind, kind, "TRUE", observed_at=100, evidence_key="old"), 100)
            apply(db, Decision(kind, kind, "FALSE", observed_at=200, evidence_key="new"), 200)
            for stamp, key in ((100, "old"), (200, "new"), (199, "late")):
                apply(db, Decision(kind, kind, "TRUE", observed_at=stamp, evidence_key=key), 300)
        assert db.execute("SELECT count(*) FROM alerts WHERE status='OPEN'").fetchone()[0] == 0


def test_incoming_recovery_cursor_covers_old_removed_files_and_wraps(tmp_path, monkeypatch):
    path = tmp_path / "monitor.db"
    store.initialize(path)
    seen = []
    known = {"run-0": 1}
    fail = False

    class Metadata:
        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def close(self):
            pass

        def ping(self):
            pass

        def metadata_schema_version(self):
            from airgap_sync.destination.mysql import METADATA_SCHEMA_VERSION

            return METADATA_SCHEMA_VERSION

        def monitoring_latest_versions(self, *_args):
            return []

        def monitoring_latest_runs(self, *_args):
            return []

        def monitoring_known_run_ids(self, ids):
            seen.append(tuple(ids))
            if fail:
                raise OSError("RDS down")
            return {item: known[item] for item in ids if item in known}

    monkeypatch.setattr("airgap_sync.monitor.alerts.DestinationMySQLConnection", Metadata)
    config = SimpleNamespace(
        mysql=SimpleNamespace(database="db"),
        enabled_tables=[],
        destination=None,
        monitor_ingest=SimpleNamespace(db_path=path),
    )
    with store.connect(path, write=True) as db:
        for number in range(101):
            run = f"run-{number}"
            apply(db, Decision("INCOMING_STALE", run, "TRUE", run_id=run), number)
        for now in (200, 201):
            for decision in _rds_decisions(config, MonitorAlertsConfig(), now, db):
                apply(db, decision, now)
        assert (
            db.execute("SELECT status FROM alerts WHERE run_id='run-0'").fetchone()[0]
            == "RECOVERED"
        )
        assert [len(batch) for batch in seen] == [100, 1]
        apply(db, Decision("INCOMING_STALE", "new", "TRUE", run_id="new"), 202)
        known["new"] = 1
        fail = True
        cursor = db.execute(
            "SELECT value FROM alert_state WHERE key='incoming-recovery-cursor'"
        ).fetchone()[0]
        with pytest.raises(OSError):
            list(_rds_decisions(config, MonitorAlertsConfig(), 203, db))
        assert (
            db.execute(
                "SELECT value FROM alert_state WHERE key='incoming-recovery-cursor'"
            ).fetchone()[0]
            == cursor
        )
    fail = False
    with store.connect(path, write=True) as db:
        for now in (204, 205):
            for decision in _rds_decisions(config, MonitorAlertsConfig(), now, db):
                apply(db, decision, now)
        assert (
            db.execute("SELECT status FROM alerts WHERE run_id='new'").fetchone()[0] == "RECOVERED"
        )
        assert all(len(batch) <= 100 for batch in seen)


def test_manifest_descriptor_read_is_bounded_and_checks_stability(tmp_path, monkeypatch):
    import airgap_sync.monitor.alerts as alerts_module

    path = tmp_path / "manifest.json"
    path.write_bytes(b"a" * (1024 * 1024))
    assert len(_manifest_bytes(path)) == 1024 * 1024
    path.write_bytes(b"a" * (1024 * 1024 + 1))
    with pytest.raises(ValueError):
        _manifest_bytes(path)
    path.write_bytes(b"{}")
    original_read = os.read
    requests = []

    def growing_read(fd, length):
        requests.append(length)
        with open(path, "ab") as stream:
            stream.write(b"x" * (1024 * 1024 + 1))
        return original_read(fd, length)

    monkeypatch.setattr(alerts_module.os, "read", growing_read)
    with pytest.raises(ValueError):
        _manifest_bytes(path)
    assert requests and max(requests) <= 1024 * 1024 + 1
    monkeypatch.setattr(alerts_module.os, "read", original_read)
    path.write_bytes(b"{}")
    original_open = os.open

    def replace_open(name, flags):
        path.unlink()
        path.symlink_to(tmp_path / "target")
        return original_open(name, flags)

    monkeypatch.setattr(alerts_module.os, "open", replace_open)
    with pytest.raises((OSError, ValueError)):
        _manifest_bytes(path)
    monkeypatch.setattr(alerts_module.os, "open", original_open)
    path.unlink()
    path.write_bytes(b"{}")

    def replace_with_fifo(name, flags):
        path.unlink()
        os.mkfifo(path)
        return original_open(name, flags)

    monkeypatch.setattr(alerts_module.os, "open", replace_with_fifo)
    with pytest.raises((OSError, ValueError)):
        _manifest_bytes(path)
    monkeypatch.setattr(alerts_module.os, "open", original_open)
    path.unlink()
    path.write_bytes(b"{}")

    def changing_read(fd, length):
        result = original_read(fd, length)
        path.write_bytes(b"changed")
        return result

    monkeypatch.setattr(alerts_module.os, "read", changing_read)
    with pytest.raises(ValueError):
        _manifest_bytes(path)


def test_bad_manifest_does_not_block_next_candidate_or_reach_parser(tmp_path, monkeypatch):
    import airgap_sync.monitor.alerts as alerts_module

    bad, good = "20260928T080000Z-aaaaaaaa", "20260928T080001Z-bbbbbbbb"
    (tmp_path / transport_filename(bad, "manifest.json")).write_bytes(b"x" * (1024 * 1024 + 1))
    (tmp_path / transport_filename(good, "manifest.json")).write_bytes(b"{}")
    (tmp_path / transport_filename(good, "schema.sql")).write_bytes(b"ddl")
    parsed = []

    def parse(data):
        parsed.append(data)
        return SimpleNamespace(
            run_id=good, chunks=[], schema_file=SimpleNamespace(file="schema.sql")
        )

    monkeypatch.setattr(alerts_module.Manifest, "model_validate_json", parse)
    config = SimpleNamespace(destination=SimpleNamespace(incoming_dir=tmp_path))
    scanner = SimpleNamespace(names=lambda: iter((bad, good)))
    path = tmp_path / "monitor.db"
    store.initialize(path)
    with store.connect(path, write=True) as db:
        assert list(_incoming_decisions(config, db, MonitorAlertsConfig(), 0, scanner)) == []
        assert (
            db.execute(
                "SELECT count(*) FROM alert_state WHERE key=?", (f"incoming:{good}",)
            ).fetchone()[0]
            == 1
        )
    assert parsed == [b"{}"]


def test_alert_browser_form_empty_filters_and_pagination(tmp_path, monkeypatch):
    from airgap_sync.monitor.app import create_app

    cfg = MonitorIngestConfig(incoming=tmp_path, db_path=tmp_path / "monitor.db")
    store.initialize(cfg.db_path)
    config = AppConfig(
        role=Role.DESTINATION,
        mysql=MySQLConfig(
            host="localhost", database="target", user="sync", password_env="TEST_PASSWORD"
        ),
        destination=DestinationConfig(incoming_dir=tmp_path),
        monitor_ingest=cfg,
        monitor_alerts=MonitorAlertsConfig(),
    )
    monkeypatch.setattr(
        "airgap_sync.monitor.app.snapshot",
        lambda _: {
            "status": "HEALTHY",
            "timezone": "UTC",
            "generated_at": "2026-09-28T00:00:00+00:00",
            "query_error": None,
        },
    )
    with store.connect(cfg.db_path, write=True) as db:
        for number, (kind, node, severity) in enumerate(
            (
                ("SOURCE_WORKER", "node-a", "WARNING"),
                ("SOURCE_DISK", "node-b", "CRITICAL"),
                ("SOURCE_WORKER", "node-a", "CRITICAL"),
            ),
            1,
        ):
            apply(
                db,
                Decision(
                    kind, str(number), "TRUE", severity, f"alert-{number}", number, node_id=node
                ),
                number,
            )
    client = TestClient(create_app(config))
    blank = {"status": "", "severity": "", "node": "", "kind": ""}
    assert "alert-3" in client.get("/alerts", params=blank).text
    assert len(client.get("/api/alerts", params=blank).json()["alerts"]) == 3
    cases = (
        ({**blank, "status": "OPEN"}, 3),
        ({**blank, "severity": "CRITICAL"}, 2),
        ({**blank, "status": "OPEN", "severity": "CRITICAL"}, 2),
        ({**blank, "node": "node-a", "kind": "SOURCE_WORKER"}, 2),
    )
    for params, count in cases:
        assert len(client.get("/api/alerts", params=params).json()["alerts"]) == count
        assert client.get("/alerts", params=params).status_code == 200
    first = client.get("/api/alerts", params={**blank, "status": "OPEN", "limit": 1}).json()
    second = client.get(
        "/api/alerts",
        params={**blank, "status": "OPEN", "limit": 1, "before": first["next_cursor"]},
    ).json()
    assert first["alerts"][0]["id"] != second["alerts"][0]["id"]
    with store.connect(cfg.db_path, write=True) as db:
        for number in range(4, 105):
            apply(
                db,
                Decision("SOURCE_WORKER", str(number), "TRUE", "CRITICAL", "more", number),
                number,
            )
    html = client.get("/alerts", params={**blank, "status": "OPEN", "severity": "CRITICAL"}).text
    assert "status=OPEN" in html and "severity=CRITICAL" in html
    next_page = re.search(r'href="(/alerts\?before=[^"]+)"', html)
    assert next_page is not None
    assert client.get(next_page.group(1).replace("&amp;", "&")).status_code == 200
    for route in ("/alerts", "/api/alerts"):
        assert client.get(route, params={"status": "BROKEN"}).status_code == 422
        assert client.get(route, params={"severity": "BROKEN"}).status_code == 422
        assert client.get(route, params={"node": "x" * 65}).status_code == 422
