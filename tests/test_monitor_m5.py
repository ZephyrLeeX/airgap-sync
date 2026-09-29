"""M5 persisted Source facts, strict wire contract and timeline arithmetic."""

import copy
import json
import sqlite3
from datetime import UTC, datetime

import pytest

from airgap_sync.common.models import AppConfig, MonitorIngestConfig
from airgap_sync.monitor import store, timeline
from airgap_sync.monitor.protocol import decode
from airgap_sync.source import monitor_report
from airgap_sync.source.state import SourceState, state_db_path

NOW = datetime(2026, 9, 29, 1, tzinfo=UTC)


def source_fixture(config_data, tmp_path):
    config_data["monitoring"] = {"node_id": "source-01"}
    config = AppConfig.model_validate(config_data)
    db_path = state_db_path(config.paths.data_dir)
    with SourceState(db_path) as state:
        state.initialize()
    with sqlite3.connect(db_path) as db:
        db.executemany(
            "INSERT INTO sync_runs(run_id,table_name,status,created_at,snapshot_completed_at,"
            "delivered_at,row_count,chunk_count,raw_bytes,compressed_bytes) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            [
                (
                    f"run-{i}",
                    "t_snapshot",
                    "DELIVERED",
                    "2026-09-29T00:00:00+00:00",
                    "2026-09-29T00:10:00+00:00",
                    "2026-09-29T00:15:00+00:00",
                    i,
                    1,
                    1024,
                    256,
                )
                for i in range(45)
            ],
        )
    return config


def payload(config, tmp_path, facts):
    result = monitor_report.build_payload(config, tmp_path / "source.yaml", NOW)
    result.update(
        schema_version=2,
        source_database=config.mysql.database,
        run_facts_status="OK",
        run_facts=facts,
    )
    return result


def wire(value, version=2):
    name = f"airgap-monitor-v{version}--source-01--20260929T010000Z--11111111.json"
    return decode(json.dumps(value).encode(), name)


def test_reporter_rotation_wire_ingest_and_timeline(config_data, tmp_path):
    config = source_fixture(config_data, tmp_path)
    first, cursor = monitor_report.run_facts(config, NOW)
    assert len(first) == 20 and cursor == "run-26"
    monitor_report.save_run_cursor(config, cursor)
    second, cursor = monitor_report.run_facts(config, NOW)
    assert len(second) == 20
    monitor_report.save_run_cursor(config, cursor)
    third, cursor = monitor_report.run_facts(config, NOW)
    assert len(third) == 5
    monitor_report.save_run_cursor(config, cursor)
    assert monitor_report.run_facts(config, NOW)[0] == first
    changed = config.model_copy(
        update={"mysql": config.mysql.model_copy(update={"database": "other"})}
    )
    assert monitor_report.run_facts(changed, NOW) == ([], None)
    cfg = MonitorIngestConfig(incoming=tmp_path / "incoming", db_path=tmp_path / "monitor.db")
    store.initialize(cfg.db_path)
    for index, facts in enumerate((first, second, third)):
        value = payload(config, tmp_path, facts)
        value["captured_at"] = f"2026-09-29T01:00:0{index}Z"
        name = f"airgap-monitor-v2--source-01--20260929T01000{index}Z--11111111.json"
        wire_payload, canonical, digest = decode(json.dumps(value).encode(), name)
        with store.connect(cfg.db_path, write=True) as db:
            assert store.ingest(
                db, cfg, name, wire_payload, canonical, digest, NOW.timestamp() + index
            )
    with store.connect(cfg.db_path) as db:
        assert db.execute("SELECT count(*) FROM source_runs").fetchone()[0] == 45
    source = timeline._source_rows(cfg, run_id="run-0")[1][0]
    item = timeline.assemble(source, None)
    assert item["metrics"]["compressed_over_raw"] == 0.25
    assert item["intervals"][0]["seconds"] == 600
    assert item["metrics"]["snapshot_raw_mib_per_second"] == 1024 / 600 / 1048576


def test_v1_and_v2_strict_version_and_invalid_fields(config_data, tmp_path):
    config = source_fixture(config_data, tmp_path)
    v1 = monitor_report.build_payload(config, tmp_path / "source.yaml", NOW)
    assert wire(v1, 1)[0]["schema_version"] == 1
    fact = monitor_report.run_facts(config, NOW)[0][0]
    v2 = payload(config, tmp_path, [fact])
    assert wire(v2)[0]["run_facts"] == [fact]
    with pytest.raises(ValueError):
        wire(v2, 1)
    bad = copy.deepcopy(v2)
    bad["run_facts"][0]["row_count"] = True
    with pytest.raises(ValueError):
        wire(bad)
    bad = copy.deepcopy(v2)
    bad["run_facts"][0]["created_at"] = "2026-09-29 00:00:00"
    with pytest.raises(ValueError):
        wire(bad)
    bad = copy.deepcopy(v2)
    bad["run_facts"] = [fact] * 21
    with pytest.raises(ValueError):
        wire(bad)


def test_conflict_merge_old_observation_and_retention(tmp_path):
    cfg = MonitorIngestConfig(incoming=tmp_path / "incoming", db_path=tmp_path / "monitor.db")
    store.initialize(cfg.db_path)
    original = {
        "run_id": "r",
        "table_name": "t",
        "status": "DELIVERED",
        "created_at": "2026-09-29T00:00:00Z",
        "snapshot_completed_at": "2026-09-29T00:10:00Z",
        "delivered_at": "2026-09-29T00:20:00Z",
        "row_count": 10,
        "chunk_count": 1,
        "raw_bytes": 100,
        "compressed_bytes": 20,
    }
    old = {
        **original,
        "status": "UPLOADING",
        "delivered_at": None,
        "snapshot_completed_at": "2026-09-29T00:11:00Z",
        "row_count": 9,
    }
    with store.connect(cfg.db_path, write=True) as db:
        store.merge_run(db, "n", "d", original, 20, 30)
        store.merge_run(db, "n", "d", old, 10, 31)
        row = db.execute("SELECT facts,conflicts FROM source_runs").fetchone()
        assert json.loads(row["facts"])["delivered_at"] == original["delivered_at"]
        assert json.loads(row["facts"])["status"] == "DELIVERED"
        assert set(json.loads(row["conflicts"])) == {"snapshot_completed_at", "row_count"}
        store.retention(db, cfg, 31)
        assert db.execute("SELECT count(*) FROM source_runs").fetchone()[0] == 1


def test_v2_migration_preserves_existing_tables(tmp_path):
    path = tmp_path / "monitor.db"
    with sqlite3.connect(path) as db:
        db.executescript(store.SCHEMA.replace("user_version=3", "user_version=2"))
        db.execute("INSERT INTO alert_state VALUES ('marker','retained',1)")
    store.initialize(path)
    with store.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 3
        assert (
            db.execute("SELECT value FROM alert_state WHERE key='marker'").fetchone()[0]
            == "retained"
        )
        assert db.execute("SELECT count(*) FROM source_runs").fetchone()[0] == 0


def test_size_boundary_and_source_identity(config_data, tmp_path):
    config = source_fixture(config_data, tmp_path)
    fact = monitor_report.run_facts(config, NOW)[0][0]
    value = payload(config, tmp_path, [fact])
    name = "airgap-monitor-v2--source-01--20260929T010000Z--11111111.json"
    body = json.dumps(value).encode()
    assert len(body) < 65536
    assert decode(body + b" " * (65536 - len(body)), name)[0] == value
    with pytest.raises(ValueError):
        decode(body + b" " * (65537 - len(body)), name)
    cfg = MonitorIngestConfig(incoming=tmp_path / "incoming", db_path=tmp_path / "monitor.db")
    store.initialize(cfg.db_path)
    with store.connect(cfg.db_path, write=True) as db, db:
        store.merge_run(db, "node-a", "database-a", fact, NOW.timestamp(), NOW.timestamp())
        store.merge_run(db, "node-b", "database-a", fact, NOW.timestamp(), NOW.timestamp())
        store.merge_run(db, "node-a", "database-b", fact, NOW.timestamp(), NOW.timestamp())
    assert len(timeline._source_rows(cfg, run_id=fact["run_id"])[1]) == 3
    assert len(timeline._source_rows(cfg, run_id=fact["run_id"], node="node-a")[1]) == 2


def test_negative_clock_zero_denominator_and_missing(config_data, tmp_path):
    config = source_fixture(config_data, tmp_path)
    fact = monitor_report.run_facts(config, NOW)[0][0]
    source = {
        "node_id": "n",
        "source_database": "d",
        "facts": fact,
        "conflicts": [],
        "last_capture": NOW.timestamp(),
    }
    item = timeline.assemble(source, None)
    assert item["intervals"][0]["seconds"] == 600
    source["facts"] = {
        **fact,
        "snapshot_completed_at": "2026-09-28T23:00:00Z",
        "raw_bytes": 0,
        "compressed_bytes": 0,
    }
    item = timeline.assemble(source, None)
    assert "snapshot" in item["anomalies"]
    assert item["metrics"]["compressed_over_raw"] is None
    assert item["metrics"]["snapshot_rows_per_second"] is None
    source["facts"] = {**fact, "snapshot_completed_at": None}
    item = timeline.assemble(source, None)
    assert item["intervals"][0]["seconds"] is None
    assert "snapshot_completed" in item["missing"]


def test_ambiguous_association_keeps_each_source(tmp_path, config_data, monkeypatch):
    from airgap_sync.destination.mysql import MonitoringRunRecord

    config = source_fixture(config_data, tmp_path)
    fact = monitor_report.run_facts(config, NOW)[0][0]
    cfg = MonitorIngestConfig(incoming=tmp_path / "incoming", db_path=tmp_path / "monitor.db")
    store.initialize(cfg.db_path)
    with store.connect(cfg.db_path, write=True) as db, db:
        for node in ("node-a", "node-b"):
            store.merge_run(db, node, config.mysql.database, fact, NOW.timestamp(), NOW.timestamp())
    config_data.update(
        role="destination",
        destination={"incoming_dir": str(tmp_path)},
        monitor_ingest=cfg.model_dump(),
    )
    dest_config = AppConfig.model_validate(config_data)
    stamp = NOW.replace(tzinfo=None)
    dest = MonitoringRunRecord(
        fact["run_id"],
        config.mysql.database,
        fact["table_name"],
        "VERIFIED",
        fact["row_count"],
        fact["row_count"],
        fact["chunk_count"],
        stamp,
        stamp,
        stamp,
        stamp,
        stamp,
        stamp,
        stamp,
        stamp,
        None,
        None,
        None,
        stamp,
        stamp,
    )
    monkeypatch.setattr(timeline, "_destination_rows", lambda *a, **k: ("OK", [dest]))
    result = timeline.read_runs(dest_config)
    assert len(result["items"]) == 3
    assert all(item["association"] == "ambiguous" for item in result["items"])
    assert sum(item["destination_status"] == "VERIFIED" for item in result["items"]) == 1


def test_readonly_api_timezone_and_escape(config_data, tmp_path):
    from fastapi.testclient import TestClient

    from airgap_sync.monitor.app import create_app

    config = source_fixture(config_data, tmp_path)
    fact = monitor_report.run_facts(config, NOW)[0][0]
    fact["table_name"] = "<script>alert(1)</script>"
    cfg = MonitorIngestConfig(incoming=tmp_path / "incoming", db_path=tmp_path / "monitor.db")
    store.initialize(cfg.db_path)
    with store.connect(cfg.db_path, write=True) as db, db:
        store.merge_run(db, "node-a", config.mysql.database, fact, NOW.timestamp(), NOW.timestamp())
    config_data.update(
        role="destination",
        destination={"incoming_dir": str(tmp_path)},
        monitor_ingest=cfg.model_dump(),
    )
    app = create_app(AppConfig.model_validate(config_data))
    client = TestClient(app)
    api = client.get("/api/runs", params={"node": "node-a"})
    assert api.status_code == 200
    assert api.json()["items"][0]["timeline"][0]["at"].endswith("Z")
    page = client.get("/runs", params={"node": "node-a"})
    assert page.status_code == 200
    assert "&lt;script&gt;" in page.text
    assert "<script>alert(1)</script>" not in page.text
    assert client.get("/api/runs", params={"before": "bad!"}).status_code == 422


def test_migration_failure_rolls_back_and_retries(tmp_path, monkeypatch):
    path = tmp_path / "monitor.db"
    with sqlite3.connect(path) as db:
        db.executescript(store.SCHEMA.replace("user_version=3", "user_version=2"))
        db.execute("INSERT INTO alert_state VALUES ('marker','retained',1)")
    original = store.RUN_SCHEMA
    monkeypatch.setattr(store, "RUN_SCHEMA", "CREATE TABLE bad (; PRAGMA user_version=3;")
    with pytest.raises(sqlite3.Error):
        store.initialize(path)
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 2
        assert (
            db.execute("SELECT value FROM alert_state WHERE key='marker'").fetchone()[0]
            == "retained"
        )
    monkeypatch.setattr(store, "RUN_SCHEMA", original)
    store.initialize(path)
    with store.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 3


def test_source_only_keyset_pages_and_monitor_failure(config_data, tmp_path, monkeypatch):
    config = source_fixture(config_data, tmp_path)
    cfg = MonitorIngestConfig(incoming=tmp_path / "incoming", db_path=tmp_path / "monitor.db")
    store.initialize(cfg.db_path)
    facts = monitor_report.run_facts(config, NOW)[0]
    with store.connect(cfg.db_path, write=True) as db, db:
        for fact in facts:
            store.merge_run(
                db, "node-a", config.mysql.database, fact, NOW.timestamp(), NOW.timestamp()
            )
    config_data.update(
        role="destination",
        destination={"incoming_dir": str(tmp_path)},
        monitor_ingest=cfg.model_dump(),
    )
    dest_config = AppConfig.model_validate(config_data)
    monkeypatch.setattr(timeline, "_destination_rows", lambda *a, **k: ("UNAVAILABLE", []))
    seen = []
    before = ""
    while True:
        result = timeline.read_runs(dest_config, before=before, limit=7)
        assert result["source_status"] == "OK"
        assert result["destination_status"] == "UNAVAILABLE"
        seen.extend(item["run_id"] for item in result["items"])
        before = result["next_cursor"]
        if before is None:
            break
    assert len(seen) == len(set(seen)) == 20
    assert timeline.read_runs(dest_config, node="missing")["items"] == []
    dest_config.monitor_ingest.db_path.unlink()
    assert timeline.read_runs(dest_config)["source_status"] == "UNAVAILABLE"


def test_real_report_to_safe_ingest_contract(config_data, tmp_path, monkeypatch):
    import shutil
    import time

    from airgap_sync.common.models import RelayConfig
    from airgap_sync.monitor.ingest import Ingestor

    config = source_fixture(config_data, tmp_path)
    config = config.model_copy(
        update={
            "relay": RelayConfig(base_url="https://relay.invalid", token_env="TEST_M5_RELAY_TOKEN")
        }
    )
    monkeypatch.setenv("TEST_M5_RELAY_TOKEN", "secret")
    cfg = MonitorIngestConfig(
        incoming=tmp_path / "incoming", db_path=tmp_path / "monitor.db", settle_seconds=1
    )
    cfg.incoming.mkdir()
    sent = []

    def upload(_self, path, name, _digest):
        sent.append(name)
        shutil.copy2(path, cfg.incoming / name)

    monkeypatch.setattr(monitor_report.RelayUploader, "upload", upload)
    assert monitor_report.report(config, tmp_path / "source.yaml") == (True, "RELAY_ACCEPTED")
    assert sent[0].startswith("airgap-monitor-v2--")
    clock = [time.time()]
    with Ingestor(cfg, lambda: clock[0]) as ing:
        assert ing.tick()["waiting"] == 1
        clock[0] += 2
        assert ing.tick()["inserted"] == 1
    with store.connect(cfg.db_path) as db:
        assert db.execute("SELECT count(*) FROM source_runs").fetchone()[0] == 20


def test_destination_cleanup_update_does_not_change_stage_time():
    from dataclasses import replace
    from datetime import timedelta

    from airgap_sync.destination.mysql import MonitoringRunRecord

    at = NOW.replace(tzinfo=None)
    dest = MonitoringRunRecord(
        "r",
        "d",
        "t",
        "VERIFIED",
        100,
        100,
        1,
        at,
        at,
        at,
        at,
        at + timedelta(minutes=10),
        at + timedelta(minutes=15),
        at + timedelta(minutes=16),
        at + timedelta(minutes=16),
        None,
        None,
        None,
        at,
        at,
    )
    first = timeline.assemble(None, dest)
    later = timeline.assemble(None, replace(dest, updated_at=at + timedelta(days=5)))
    assert first["duration_seconds"] == later["duration_seconds"]
    assert first["duration_seconds"]["import"] == 600
    assert first["duration_seconds"]["verification_window"] == 300
    assert first["metrics"]["destination_rows_per_second"] == 100 / 600


def test_destination_only_when_monitor_db_unavailable(config_data, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    from airgap_sync.destination.mysql import MonitoringRunRecord
    from airgap_sync.monitor.app import create_app

    config_data.update(
        role="destination",
        destination={"incoming_dir": str(tmp_path)},
        monitor_ingest={
            "incoming": str(tmp_path / "incoming"),
            "db_path": str(tmp_path / "missing" / "monitor.db"),
        },
    )
    config = AppConfig.model_validate(config_data)
    at = NOW.replace(tzinfo=None)
    dest = MonitoringRunRecord(
        "only-dest",
        "d",
        "t",
        "FAILED",
        0,
        None,
        0,
        at,
        at,
        at,
        None,
        None,
        None,
        None,
        at,
        None,
        None,
        None,
        None,
        None,
    )
    monkeypatch.setattr(timeline, "_destination_rows", lambda *a, **k: ("OK", [dest]))
    result = timeline.read_runs(config)
    assert result["source_status"] == "UNAVAILABLE"
    assert result["destination_status"] == "OK"
    assert result["items"][0]["destination_status"] == "FAILED"
    assert result["items"][0]["metrics"]["source_rows"] is None
    response = TestClient(create_app(config)).get("/runs/only-dest?node=&database=d&table=t")
    assert response.status_code == 200
    assert "only-dest" in response.text


def _destination_record(run_id, database="d", table="t"):
    from airgap_sync.destination.mysql import MonitoringRunRecord

    at = NOW.replace(tzinfo=None)
    return MonitoringRunRecord(
        run_id,
        database,
        table,
        "VERIFIED",
        1,
        1,
        1,
        at,
        at,
        at,
        at,
        at,
        at,
        at,
        at,
        None,
        None,
        None,
        at,
        at,
    )


def _timeline_case(config_data, tmp_path, monkeypatch, source_ids, destination_ids):
    cfg = MonitorIngestConfig(incoming=tmp_path / "incoming", db_path=tmp_path / "monitor.db")
    store.initialize(cfg.db_path)
    fact = {
        "run_id": "",
        "table_name": "t",
        "status": "DELIVERED",
        "created_at": "2026-09-29T00:00:00Z",
        "snapshot_completed_at": None,
        "delivered_at": None,
        "row_count": 1,
        "chunk_count": 1,
        "raw_bytes": 1,
        "compressed_bytes": 1,
    }
    with store.connect(cfg.db_path, write=True) as db, db:
        for run_id, node in source_ids:
            store.merge_run(
                db, node, "d", {**fact, "run_id": run_id}, NOW.timestamp(), NOW.timestamp()
            )
    destinations = [_destination_record(run_id) for run_id in destination_ids]

    def destination_page(_config, *, after, limit, database=None, table=None, run_id=None):
        if after[1] is None:
            eligible = [d for d in destinations if d.run_id > after[0]]
        else:
            eligible = [
                d for d in destinations if (d.run_id, d.source_database, d.table_name) > after
            ]
        eligible = [
            d
            for d in eligible
            if (database is None or d.source_database == database)
            and (table is None or d.table_name == table)
            and (run_id is None or d.run_id == run_id)
        ]
        return "OK", eligible[:limit]

    def exact_destinations(_config, sources, known=()):
        by_key = {(d.run_id, d.source_database, d.table_name): d for d in destinations}
        return "OK", {
            (s["run_id"], s["source_database"], s["table_name"]): by_key.get(
                (s["run_id"], s["source_database"], s["table_name"])
            )
            for s in sources
        }

    monkeypatch.setattr(timeline, "_destination_rows", destination_page)
    monkeypatch.setattr(timeline, "_exact_destinations", exact_destinations)
    config_data.update(
        role="destination",
        destination={"incoming_dir": str(tmp_path)},
        monitor_ingest=cfg.model_dump(),
    )
    return AppConfig.model_validate(config_data)


@pytest.mark.parametrize(
    ("sources", "destinations", "expected"),
    [
        ([], [f"r{i}" for i in range(1, 6)], 5),
        ([(f"r{i}", "node-a") for i in range(1, 6)], [], 5),
        (
            [("r2", "node-a"), ("r3", "node-a"), ("r4", "node-a"), ("r4", "node-b")],
            ["r1", "r3", "r4", "r5"],
            7,
        ),
    ],
)
def test_timeline_continuous_pages(
    config_data, tmp_path, monkeypatch, sources, destinations, expected
):
    config = _timeline_case(config_data, tmp_path, monkeypatch, sources, destinations)
    seen = []
    before = ""
    for _ in range(10):
        result = timeline.read_runs(config, before=before, limit=2)
        seen.extend(result["items"])
        before = result["next_cursor"]
        if before is None:
            break
    else:
        pytest.fail("pagination did not end")
    assert len(seen) == expected
    assert (
        len({(item["run_id"], item["node_id"], item["destination_status"]) for item in seen})
        == expected
    )
    assert [item["run_id"] for item in seen] == sorted(item["run_id"] for item in seen)
    if destinations and sources:
        ambiguous = [item for item in seen if item["run_id"] == "r4"]
        assert len(ambiguous) == 3
        assert all(item["association"] == "ambiguous" for item in ambiguous)


def test_node_page_exact_destination_join_and_ambiguity(config_data, tmp_path, monkeypatch):
    config = _timeline_case(
        config_data,
        tmp_path,
        monkeypatch,
        [("r4", "node-a"), ("r4", "node-b"), ("r5", "node-a")],
        ["r1", "r2", "r3", "r4", "r5"],
    )
    first = timeline.read_runs(config, node="node-a", limit=1)
    second = timeline.read_runs(config, node="node-a", limit=1, before=first["next_cursor"])
    assert first["items"][0]["run_id"] == "r4"
    assert first["items"][0]["association"] == "ambiguous"
    assert second["items"][0]["run_id"] == "r5"
    assert second["items"][0]["destination_status"] == "VERIFIED"
    assert second["next_cursor"] is None
    only = timeline.read_runs(config, node="node-a", run_id="r5", limit=2)
    assert only["items"][0]["destination_status"] == "VERIFIED"


def test_run_facts_advances_across_old_history_and_wraps(config_data, tmp_path):
    config = source_fixture(config_data, tmp_path)
    db_path = state_db_path(config.paths.data_dir)
    with sqlite3.connect(db_path) as db:
        db.execute("DELETE FROM sync_runs")
        db.executemany(
            "INSERT INTO sync_runs(run_id,table_name,status,created_at) VALUES (?,?,?,?)",
            ((f"old-{i:05}", "t", "DELIVERED", "2020-01-01T00:00:00+00:00") for i in range(60000)),
        )
        db.execute(
            "INSERT INTO sync_runs(run_id,table_name,status,created_at) VALUES (?,?,?,?)",
            ("recent", "t", "DELIVERED", "2026-09-29T00:00:00+00:00"),
        )
    cursor = ""
    for _ in range(240):
        facts, next_cursor = monitor_report.run_facts(config, NOW)
        assert next_cursor is not None
        assert len(facts) <= 20
        if facts:
            assert [fact["run_id"] for fact in facts] == ["recent"]
            break
        assert next_cursor > cursor
        cursor = next_cursor
        monitor_report.save_run_cursor(config, cursor)
    else:
        pytest.fail("recent Run was not reached")
    monitor_report.save_run_cursor(config, next_cursor)
    restarted = AppConfig.model_validate(config.model_dump(mode="json"))
    assert monitor_report.run_facts(restarted, NOW)[1] == "old-00255"
    # The saved cursor survives a new invocation and keeps its database binding.
    assert json.loads((config.paths.data_dir / "monitor" / "run-cursor.json").read_text()) == {
        "after": "recent",
        "database": config.mysql.database,
    }


def test_destination_keyset_sql_uses_full_identity(monkeypatch):
    from dataclasses import astuple

    from airgap_sync.destination.mysql import DestinationMySQLConnection

    db = object.__new__(DestinationMySQLConnection)
    db._metadata = "`monitor`"
    queries = []
    monkeypatch.setattr(db, "_fetchall", lambda sql, args: queries.append((sql, args)) or [])
    db.monitoring_run_page(after=("r3", "d", "t"), limit=3)
    sql, args = queries.pop()
    assert "(run_id,source_database,table_name)>(%s,%s,%s)" in sql
    assert "ORDER BY run_id,source_database,table_name LIMIT %s" in sql
    assert args == ("r3", "d", "t", 3)
    db.monitoring_run_page(after=("r3", None, None), limit=3)
    sql, args = queries.pop()
    assert "run_id>%s" in sql
    assert args == ("r3", 3)
    monkeypatch.setattr(
        db,
        "_fetchall",
        lambda sql, args: (
            queries.append((sql, args))
            or [astuple(_destination_record("r1")), astuple(_destination_record("r2", "other"))]
        ),
    )
    records = db.monitoring_runs_for_identities([("r1", "d", "t"), ("r2", "d", "t")])
    assert [record.run_id for record in records] == ["r1"]
    sql, args = queries.pop()
    assert "WHERE run_id IN (%s,%s) LIMIT %s" in sql
    assert args == ("r1", "r2", 2)


def test_run_facts_budget_interrupt_saves_partial_progress(config_data, tmp_path, monkeypatch):
    config = source_fixture(config_data, tmp_path)
    db_path = state_db_path(config.paths.data_dir)
    with sqlite3.connect(db_path) as db:
        db.execute("DELETE FROM sync_runs")
        db.executemany(
            "INSERT INTO sync_runs(run_id,table_name,status,created_at) VALUES (?,?,?,?)",
            ((f"old-{i:04}", "t", "DELIVERED", "2020-01-01T00:00:00+00:00") for i in range(1000)),
        )
        db.execute(
            "INSERT INTO sync_runs(run_id,table_name,status,created_at) VALUES (?,?,?,?)",
            ("recent", "t", "DELIVERED", "2026-09-29T00:00:00+00:00"),
        )
    original_connect = sqlite3.connect

    class InterruptedConnection(sqlite3.Connection):
        def set_progress_handler(self, callback, steps):
            calls = 0

            def interrupt():
                nonlocal calls
                calls += 1
                return 1 if calls == 2 else callback()

            return super().set_progress_handler(interrupt, steps)

    monkeypatch.setattr(
        monitor_report.sqlite3,
        "connect",
        lambda *args, **kwargs: original_connect(*args, factory=InterruptedConnection, **kwargs),
    )
    facts, cursor = monitor_report.run_facts(config, NOW)
    assert facts == []
    assert cursor is not None and cursor.startswith("old-")
    monitor_report.save_run_cursor(config, cursor)
    monkeypatch.setattr(monitor_report.sqlite3, "connect", original_connect)
    for _ in range(5):
        facts, cursor = monitor_report.run_facts(config, NOW)
        monitor_report.save_run_cursor(config, cursor)
        if facts:
            break
    assert [fact["run_id"] for fact in facts] == ["recent"]


def test_merged_page_does_not_jump_over_unscanned_source(config_data, tmp_path, monkeypatch):
    config = _timeline_case(
        config_data,
        tmp_path,
        monkeypatch,
        [("a", "node-a"), ("b", "node-a"), ("c", "node-a"), ("d", "node-a")],
        ["a", "b", "c", "z"],
    )
    seen = []
    before = ""
    for _ in range(8):
        result = timeline.read_runs(config, before=before, limit=2)
        seen.extend(item["run_id"] for item in result["items"])
        before = result["next_cursor"]
        if before is None:
            break
    assert seen == ["a", "b", "c", "d", "z"]


def test_same_run_different_source_identity_is_not_joined(config_data, tmp_path, monkeypatch):
    config = _timeline_case(config_data, tmp_path, monkeypatch, [("r1", "node-a")], ["r1"])
    cfg = config.monitor_ingest
    fact = {
        "run_id": "r1",
        "table_name": "other",
        "status": "DELIVERED",
        "created_at": "2026-09-29T00:00:00Z",
        "snapshot_completed_at": None,
        "delivered_at": None,
        "row_count": 1,
        "chunk_count": 1,
        "raw_bytes": 1,
        "compressed_bytes": 1,
    }
    with store.connect(cfg.db_path, write=True) as db, db:
        store.merge_run(db, "node-b", "other-db", fact, NOW.timestamp(), NOW.timestamp())
    result = timeline.read_runs(config, limit=2)
    assert len(result["items"]) == 2
    joined, separate = result["items"]
    assert joined["source_database"] == "d"
    assert joined["destination_status"] == "VERIFIED"
    assert separate["source_database"] == "other-db"
    assert separate["destination_status"] is None
    assert separate["association"] == "unresolved"
