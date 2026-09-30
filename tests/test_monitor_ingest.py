"""M3 contract, filesystem races, transaction recovery and independent service tests."""

import copy
import json
import os
import sqlite3
import threading
from contextlib import closing
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from click.testing import CliRunner
from fastapi.testclient import TestClient

from airgap_sync.cli import cli
from airgap_sync.common.models import AppConfig, MonitorIngestConfig
from airgap_sync.monitor import store
from airgap_sync.monitor.app import create_app
from airgap_sync.monitor.ingest import BackgroundIngest, Ingestor
from airgap_sync.monitor.protocol import decode
from airgap_sync.source import monitor_report
from airgap_sync.source.state import SourceState, state_db_path

NOW = datetime(2026, 9, 28, 8, tzinfo=UTC)


@pytest.fixture
def payload(config_data, tmp_path):
    config_data["monitoring"] = {"node_id": "source-01"}
    config = AppConfig.model_validate(config_data)
    with SourceState(state_db_path(config.paths.data_dir)) as state:
        state.initialize()
        state.create_cycle("cycle-test", ["t_snapshot"], NOW.isoformat())
        state.register_table("t_snapshot")
        state.begin_run("t_snapshot", "run-test")
    # Use actual M2 collectors/serialization, including metadata and unconfigured logs.
    return monitor_report.build_payload(config, tmp_path / "source.yaml", NOW)


@pytest.fixture
def cfg(tmp_path):
    return MonitorIngestConfig(
        incoming=tmp_path / "incoming",
        db_path=tmp_path / "db/monitor.db",
        settle_seconds=1,
        invalid_grace_seconds=30,
    )


def put(cfg, payload, nonce="11111111", body=None):
    stamp = datetime.fromisoformat(payload["captured_at"].replace("Z", "+00:00"))
    name = f"airgap-monitor-v1--{payload['node_id']}--{stamp:%Y%m%dT%H%M%SZ}--{nonce}.json"
    cfg.incoming.mkdir(exist_ok=True, parents=True)
    path = cfg.incoming / name
    path.write_bytes(body if body is not None else json.dumps(payload).encode())
    return path


def counts(cfg, table="node_samples"):
    with store.connect(cfg.db_path) as db:
        return db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def rounds(ing, clock, n=5, step=2):
    results = []
    for _ in range(n):
        results.append(ing.tick())
        clock[0] += step
    return results


def test_reporter_roundtrip_and_shared_business_files(cfg, payload):
    clock = [NOW.timestamp()]
    original = put(cfg, payload)
    for name in ("manifest.json", "chunk.zst", "airgap-monitor-v1--bad.json", "x.part"):
        (cfg.incoming / name).write_bytes(b"business")
    with Ingestor(cfg, lambda: clock[0]) as ing:
        rounds(ing, clock)
    assert not original.exists()
    assert counts(cfg) == 1
    assert counts(cfg, "filesystem_samples") == len(payload["filesystems"])
    data = store.read_sources(cfg, now=clock[0])["samples"][0]
    assert data["payload"] == payload
    assert payload["managed_storage"]["logs_bytes"] is None
    assert payload["connectivity"] == {"source_mysql": "UNKNOWN", "relay": "UNKNOWN"}
    assert payload["cycle"]["tables_delivered"] == 0
    assert payload["agent_version"] == "0.1.0"
    assert payload["current_run"]["status"] == "GENERATING"
    assert payload["last_run"]["rows_scanned"] is None
    for path in cfg.incoming.iterdir():
        if path.is_file() and not path.name.startswith("."):
            assert path.read_bytes() == b"business"


def test_duplicate_rename_order_tie_and_conflict(cfg, payload):
    clock = [NOW.timestamp()]
    with Ingestor(cfg, lambda: clock[0]) as ing:
        first = put(cfg, payload)
        rounds(ing, clock)
        received = store.read_sources(cfg)["samples"][0]["received_at"]
        put(cfg, payload)
        put(cfg, payload, "22222222", json.dumps(payload, indent=2).encode())
        rounds(ing, clock)
        assert counts(cfg) == 1
        assert store.read_sources(cfg)["samples"][0]["received_at"] == received
        older = {**payload, "captured_at": "2026-09-28T07:00:00Z"}
        put(cfg, older)
        rounds(ing, clock)
        assert (
            store.read_sources(cfg)["samples"][0]["payload"]["captured_at"]
            == payload["captured_at"]
        )
        tie = copy.deepcopy(payload)
        tie["host"]["hostname"] = "other"
        tie_path = put(cfg, tie, "33333333")
        expected_hash = max(
            decode(json.dumps(payload).encode(), first.name)[2],
            decode(json.dumps(tie).encode(), tie_path.name)[2],
        )
        rounds(ing, clock)
        assert store.read_sources(cfg)["samples"][0]["sample_id"] == expected_hash
        put(cfg, tie)  # same transfer identity, different content
        rounds(ing, clock, 6, 31)
        assert counts(cfg) == 3
        assert len(list((cfg.incoming / ".airgap-monitor-quarantine").glob("*.json"))) == 1


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p.update(schema_version=True),
        lambda p: p.update(schema_version=2),
        lambda p: p.update(role="DESTINATION"),
        lambda p: p.update(node_id="different"),
        lambda p: p.update(captured_at="2026-09-28T09:00:00Z"),
        lambda p: p.update(captured_at="2026-09-28T08:00:00"),
        lambda p: p.update(extra=0),
        lambda p: p.pop("host"),
        lambda p: p["system"].update(cpu_percent=101),
        lambda p: p["system"].update(cpu_percent=float("nan")),
        lambda p: p["system"].update(cpu_percent=float("inf")),
        lambda p: p["worker"].update(pid=True),
        lambda p: p["worker"].update(pid=-1),
        lambda p: p["worker"].update(pid=2**63),
        lambda p: p["worker"].update(status="ALIVE"),
        lambda p: p["host"].update(hostname="x" * 513),
        lambda p: p["host"].update(hostname="\ud800"),
        lambda p: p.update(filesystems=[{}] * 6),
        lambda p: p["connectivity"].update(relay="CONNECTED"),
        lambda p: p.update(collection_errors=["secret"]),
    ],
)
def test_strict_payload_validation(payload, cfg, mutate):
    name = put(cfg, payload).name
    mutate(payload)
    with pytest.raises(ValueError):
        decode(json.dumps(payload).encode(), name)


@pytest.mark.parametrize(
    "body", [b"{", b"\xff", b'{"x":1,"x":2}', b" " * 65537, b'{"schema_version":NaN}', b"[" * 2000]
)
def test_invalid_delayed_quarantine_and_good_batch(cfg, payload, body):
    clock = [NOW.timestamp()]
    bad = put(cfg, payload, body=body)
    good = put(cfg, payload, "22222222")
    with Ingestor(cfg, lambda: clock[0]) as ing:
        rounds(ing, clock, 5)
        assert counts(cfg) == 1
        assert not good.exists()
        assert not list((cfg.incoming / ".airgap-monitor-quarantine").glob("*.json"))
        rounds(ing, clock, 4, 31)
        assert not (cfg.incoming / ".airgap-monitor-processing" / bad.name).exists()
        records = list((cfg.incoming / ".airgap-monitor-quarantine").glob("*.json"))
        assert len(records) == 1 and records[0].stat().st_size < 512


def test_partial_replacement_symlink_and_atomic_rename(cfg, payload, monkeypatch):
    clock = [NOW.timestamp()]
    path = put(cfg, payload, body=b"{")
    with Ingestor(cfg, lambda: clock[0]) as ing:
        ing.tick()
        path.write_bytes(json.dumps(payload).encode())
        rounds(ing, clock)
        assert counts(cfg) == 1
        target = cfg.incoming / "business"
        target.write_text("do not touch")
        link = put(cfg, payload, "22222222")
        link.unlink()
        link.symlink_to(target)
        rounds(ing, clock)
        assert link.is_symlink() and target.read_text() == "do not touch"
        other = copy.deepcopy(payload)
        other["host"]["hostname"] = "replacement"
        candidate = put(cfg, other, "33333333")
        ing.tick()
        original_rename = os.rename

        def replace_during_claim(src, dst, **kwargs):
            replacement = cfg.incoming / "upload.part"
            replacement.write_bytes(json.dumps(payload).encode())
            os.replace(replacement, candidate)
            original_rename(src, dst, **kwargs)

        monkeypatch.setattr(os, "rename", replace_during_claim)
        clock[0] += 2
        ing.tick()
        assert counts(cfg) == 1
        assert (cfg.incoming / ".airgap-monitor-processing" / candidate.name).exists()
        monkeypatch.setattr(os, "rename", original_rename)
        rounds(ing, clock)
        assert counts(cfg) == 1  # replacement validated and deduplicated later
        temp = cfg.incoming / "another.part"
        temp.write_bytes(json.dumps(other).encode())
        os.rename(temp, candidate)
        rounds(ing, clock)
        # same transport identity now conflicts; cannot silently replace prior receipt
        assert counts(cfg) == 1


def test_delete_failure_restart_and_write_rollback(cfg, payload, monkeypatch):
    clock = [NOW.timestamp()]
    path = put(cfg, payload)
    with Ingestor(cfg, lambda: clock[0]) as ing:
        original = ing.remove
        monkeypatch.setattr(ing, "remove", lambda *a: (_ for _ in ()).throw(OSError("secret")))
        rounds(ing, clock)
        assert counts(cfg) == 1
        assert (cfg.incoming / ".airgap-monitor-processing" / path.name).exists()
        monkeypatch.setattr(ing, "remove", original)
    with Ingestor(cfg, lambda: clock[0]) as ing:
        rounds(ing, clock)
        assert counts(cfg) == 1
        assert not (cfg.incoming / ".airgap-monitor-processing" / path.name).exists()
        other = {**payload, "captured_at": "2026-09-28T07:00:00Z"}
        path = put(cfg, other)
        with store.connect(cfg.db_path, write=True) as db:
            db.execute("""CREATE TRIGGER fail_child BEFORE INSERT ON managed_storage_samples
                BEGIN SELECT RAISE(ABORT, 'secret'); END""")
        rounds(ing, clock)
        assert counts(cfg) == 1
        assert counts(cfg, "content_receipts") == 1
        assert counts(cfg, "filesystem_samples") == len(payload["filesystems"])
        assert (cfg.incoming / ".airgap-monitor-processing" / path.name).exists()
        with store.connect(cfg.db_path, write=True) as db:
            db.execute("DROP TRIGGER fail_child")
        rounds(ing, clock)
        assert counts(cfg) == 2


def test_sqlite_busy_retains_and_recovers(cfg, payload):
    clock = [NOW.timestamp()]
    path = put(cfg, payload)
    with Ingestor(cfg, lambda: clock[0]) as ing:
        ing.tick()
        clock[0] += 2
        with store.connect(cfg.db_path, write=True) as locked:
            locked.execute("BEGIN IMMEDIATE")
            with pytest.raises(sqlite3.OperationalError):
                ing.tick()
            assert path.exists()
            locked.rollback()
        rounds(ing, clock)
        assert counts(cfg) == 1


def test_future_old_retention_children_and_replay(cfg, payload):
    clock = [NOW.timestamp()]
    with Ingestor(cfg, lambda: clock[0]) as ing:
        put(cfg, payload)
        older = {**payload, "captured_at": "2026-09-27T08:00:00Z"}
        put(cfg, older)
        for offset in (-36, 2):
            bad = {
                **payload,
                "captured_at": (NOW + timedelta(days=offset)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            put(cfg, bad)
        rounds(ing, clock, 6, 31)
        assert counts(cfg) == 2
        assert len(list((cfg.incoming / ".airgap-monitor-quarantine").glob("*.json"))) == 2
        clock[0] += 36 * 86400
        ing.tick()
        assert counts(cfg) == 1
        assert counts(cfg, "filesystem_samples") == len(payload["filesystems"])
        assert counts(cfg, "managed_storage_samples") == 1
        assert counts(cfg, "content_receipts") == 2
        assert not list((cfg.incoming / ".airgap-monitor-quarantine").glob("*.json"))
        put(cfg, older, "22222222")
        rounds(ing, clock, 5, 31)
        assert counts(cfg) == 1
        clock[0] += 100 * 86400
        rounds(ing, clock)
        assert counts(cfg, "content_receipts") == 1
        put(cfg, older, "33333333")
        rounds(ing, clock, 5, 31)
        assert counts(cfg) == 1
        assert store.read_sources(cfg)["samples"][0]["payload"] == payload


def test_lock_and_corrupt_unknown_db_are_not_rebuilt(cfg):
    with Ingestor(cfg):
        with pytest.raises(BlockingIOError), Ingestor(cfg):
            pass
        alternate = cfg.model_copy(update={"db_path": cfg.db_path.parent / "other/monitor.db"})
        with pytest.raises(BlockingIOError), Ingestor(alternate):
            pass
    with closing(sqlite3.connect(cfg.db_path)) as db:
        db.execute("PRAGMA user_version=99")
    with pytest.raises(ValueError), Ingestor(cfg):
        pass
    with closing(sqlite3.connect(cfg.db_path)) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 99
    cfg.db_path.write_bytes(b"corrupt database")
    with pytest.raises(sqlite3.DatabaseError), Ingestor(cfg):
        pass
    assert cfg.db_path.read_bytes() == b"corrupt database"


def test_same_directory_lock_ingest_contention_and_release(cfg, payload, monkeypatch):
    cfg = cfg.model_copy(update={"db_path": cfg.incoming / "monitor.db"})
    clock = [NOW.timestamp()]
    delivered = put(cfg, payload)
    with Ingestor(cfg, lambda: clock[0]) as ing:
        assert os.fstat(ing.root).st_ino == os.fstat(ing.db_dir).st_ino
        with pytest.raises(BlockingIOError), Ingestor(cfg):
            pass
        rounds(ing, clock)
        assert not delivered.exists()
        assert counts(cfg) == 1
        root_fd, db_fd = ing.root, ing.db_dir
    with pytest.raises(OSError):
        os.fstat(root_fd)
    with pytest.raises(OSError):
        os.fstat(db_fd)
    with Ingestor(cfg):
        pass

    original_initialize = store.initialize

    def fail_initialize(path):
        raise RuntimeError("initialization failed")

    monkeypatch.setattr(store, "initialize", fail_initialize)
    failed = Ingestor(cfg)
    with pytest.raises(RuntimeError, match="initialization failed"):
        failed.__enter__()
    with pytest.raises(OSError):
        os.fstat(failed.root)
    with pytest.raises(OSError):
        os.fstat(failed.db_dir)
    monkeypatch.setattr(store, "initialize", original_initialize)
    with Ingestor(cfg):
        pass


def test_distinct_lock_namespaces_remain_mutually_exclusive(cfg):
    other_inbox = cfg.model_copy(update={"incoming": cfg.incoming.parent / "other-inbox"})
    other_db = cfg.model_copy(update={"db_path": cfg.incoming.parent / "other-db/monitor.db"})
    independent = cfg.model_copy(
        update={
            "incoming": cfg.incoming.parent / "independent-inbox",
            "db_path": cfg.incoming.parent / "independent-db/monitor.db",
        }
    )
    with Ingestor(cfg):
        with pytest.raises(BlockingIOError), Ingestor(other_inbox):
            pass
        with pytest.raises(BlockingIOError), Ingestor(other_db):
            pass
        with Ingestor(independent):
            pass
    with Ingestor(other_inbox), Ingestor(other_db):
        pass


def test_bounded_scan_history_and_quarantine(cfg, payload):
    cfg = cfg.model_copy(update={"scan_limit": 3, "batch_size": 2, "quarantine_max_files": 2})
    clock = [NOW.timestamp()]
    cfg.incoming.mkdir()
    for i in range(20):
        (cfg.incoming / f"business-{i}").write_text("data")
    for i in range(8):
        put(cfg, payload, f"{i:08x}", b"bad")
    with Ingestor(cfg, lambda: clock[0]) as ing:
        for result in rounds(ing, clock, 100, 31):
            assert result["candidates"] <= 4
        assert len(list((cfg.incoming / ".airgap-monitor-quarantine").glob("*.json"))) <= 2
        with store.connect(cfg.db_path, write=True) as db:
            for i in range(7):
                p = {
                    **payload,
                    "captured_at": (NOW - timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                name = put(cfg, p, f"{i + 100:08x}").name
                p, canonical, digest = decode(json.dumps(p).encode(), name)
                store.ingest(db, cfg, name, p, canonical, digest, clock[0])
        first = store.read_sources(cfg, node="source-01", limit=3, now=clock[0])
        second = store.read_sources(
            cfg, node="source-01", limit=3, now=clock[0], before=first["next_cursor"]
        )
        assert len(first["samples"]) == len(second["samples"]) == 3
        assert {s["sample_id"] for s in first["samples"]}.isdisjoint(
            s["sample_id"] for s in second["samples"]
        )
        with store.connect(cfg.db_path) as db:
            plan = db.execute(
                "EXPLAIN QUERY PLAN SELECT * FROM node_samples WHERE node_id=? AND captured>=? "
                "ORDER BY captured DESC,hash DESC LIMIT 100",
                ("source-01", 0),
            ).fetchall()
            assert any("sample_history" in row[3] for row in plan)


def test_background_recovers_without_real_wait(cfg, monkeypatch):
    background = BackgroundIngest(cfg)
    calls = []

    def tick(self):
        calls.append(1)
        if len(calls) == 1:
            raise sqlite3.OperationalError("secret")
        background.stop_event.set()
        return {"error": 0}

    monkeypatch.setattr(Ingestor, "tick", tick)

    class ImmediateEvent:
        stopped = False

        def is_set(self):
            return self.stopped

        def set(self):
            self.stopped = True

        def wait(self, _):
            return self.stopped

    background.stop_event = ImmediateEvent()
    background.run()
    assert len(calls) == 2 and background.status == "STOPPED"


def test_background_start_once_and_lifespan(cfg, monkeypatch, config_data):
    started = threading.Event()
    release = threading.Event()
    background = BackgroundIngest(cfg)

    def run():
        started.set()
        release.wait(5)

    monkeypatch.setattr(background, "run", run)
    background.start()
    assert started.wait(5)
    first = background.thread
    background.start()
    assert background.thread is first
    release.set()
    background.stop()
    assert not first.is_alive()
    events = []
    monkeypatch.setattr(BackgroundIngest, "start", lambda self: events.append("start"))
    monkeypatch.setattr(BackgroundIngest, "stop", lambda self: events.append("stop"))
    config_data.update(
        role="destination",
        destination={"incoming_dir": str(cfg.incoming)},
        monitor_ingest=cfg.model_dump(),
    )
    with TestClient(create_app(AppConfig.model_validate(config_data))):
        assert events == ["start"]
    assert events == ["start", "stop"]


def test_web_independent_degradation_escaping_and_bounds(cfg, payload, config_data, monkeypatch):
    clock = [NOW.timestamp()]
    payload["host"]["hostname"] = '<script>alert("x")</script>'
    with Ingestor(cfg, lambda: clock[0]) as ing:
        put(cfg, payload)
        rounds(ing, clock)
    config_data.update(
        role="destination",
        destination={"incoming_dir": str(cfg.incoming)},
        monitor_ingest=cfg.model_dump(),
    )
    app = create_app(AppConfig.model_validate(config_data))

    class FailedRDS:
        def __init__(self, *a, **kw):
            pass

        def __enter__(self):
            raise RuntimeError("secret")

        def __exit__(self, *a):
            pass

        def close(self):
            pass

    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", FailedRDS)
    client = TestClient(app)
    response = client.get("/api/dashboard").json()
    assert response["rds"] == "DISCONNECTED"
    assert response["sources"]["status"] == "OK"
    assert "<script>alert" not in client.get("/system").text
    assert "&lt;script&gt;" in client.get("/system").text
    assert client.get("/source-history?node=source-01&window=30d").status_code == 200
    assert client.get("/api/sources/source-01/history?limit=201").status_code == 422
    assert client.get("/api/sources/source-01/history?window=365d").status_code == 422
    assert client.post("/api/sources").status_code == 405
    cfg.db_path.write_bytes(b"corrupt")
    response = client.get("/api/dashboard").json()
    assert response["sources"]["status"] == "UNAVAILABLE"
    assert response["system"]["hostname"]


@pytest.mark.parametrize(
    ("zone", "capture", "storage", "boot", "next_action"),
    [
        (
            "Asia/Shanghai",
            "2026-09-28 04:30:00 CST",
            "2026-09-28 03:00:00 CST",
            "2026-09-28 02:00:00 CST",
            "2026-09-28 05:00:00 CST",
        ),
        (
            "UTC",
            "2026-09-27 20:30:00 UTC",
            "2026-09-27 19:00:00 UTC",
            "2026-09-27 18:00:00 UTC",
            "2026-09-27 21:00:00 UTC",
        ),
        (
            "America/New_York",
            "2026-09-27 16:30:00 EDT",
            "2026-09-27 15:00:00 EDT",
            "2026-09-27 14:00:00 EDT",
            "2026-09-27 17:00:00 EDT",
        ),
    ],
)
def test_source_html_timezone_keeps_raw_api_and_cursors(
    cfg, payload, config_data, monkeypatch, zone, capture, storage, boot, next_action
):
    from test_monitor import FakeConnection

    clock = [NOW.timestamp()]
    payload["captured_at"] = "2026-09-27T20:30:00Z"
    payload["host"]["boot_time"] = "2026-09-27T18:00:00Z"
    payload["managed_storage"]["captured_at"] = "2026-09-27T19:00:00Z"
    payload["next_action_at"] = "2026-09-27T21:00:00Z"
    payload["cycle"]["started_at"] = "2026-09-27T17:00:00Z"
    payload["cycle"]["completed_at"] = None
    payload["current_run"]["created_at"] = "2026-09-27T18:30:00Z"
    older = copy.deepcopy(payload)
    older["captured_at"] = "2026-09-27T20:00:00Z"
    with Ingestor(cfg, lambda: clock[0]) as ing:
        put(cfg, payload)
        put(cfg, older, "22222222")
        rounds(ing, clock)
    original = store.read_sources(cfg, node="source-01", limit=1, now=clock[0])
    assert original["next_cursor"]
    original = copy.deepcopy(original)
    config_data.update(
        role="destination",
        destination={"incoming_dir": str(cfg.incoming), "report_timezone": zone},
        monitor_ingest=cfg.model_dump(),
    )
    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", FakeConnection)
    from airgap_sync.monitor import dashboard as monitor_dashboard

    original_system_snapshot = monitor_dashboard.system_snapshot
    monkeypatch.setattr(
        monitor_dashboard,
        "system_snapshot",
        lambda path: {
            **original_system_snapshot(path),
            "boot_time": "2026-09-27T18:00:00+00:00",
        },
    )
    client = TestClient(create_app(AppConfig.model_validate(config_data)))
    api_before = client.get("/api/sources?limit=1").json()
    dashboard_before = client.get("/api/dashboard").json()
    received = datetime.fromisoformat(original["samples"][0]["received_at"])
    received_display = received.astimezone(ZoneInfo(zone)).strftime("%Y-%m-%d %H:%M:%S %Z")
    cycle_display = (
        datetime(2026, 9, 27, 17, tzinfo=UTC)
        .astimezone(ZoneInfo(zone))
        .strftime("%Y-%m-%d %H:%M:%S %Z")
    )
    for route in ("/", "/system", "/source-history?node=source-01&window=30d"):
        html = client.get(route).text
        assert zone in html
        assert capture in html
        assert storage in html
        assert boot in html
        assert next_action in html
        assert f"目标端接收时间： {received_display}" in html
        assert f"开始时间</dt><dd>{cycle_display}" in html
        assert "完成时间</dt><dd>未知" in html
        assert "2026-09-27T20:30:00Z" not in html or "原始负载（UTC 时间戳）" in html
    history = client.get("/source-history?node=source-01&window=30d").text
    assert "原始负载（UTC 时间戳）" in history
    assert 'window=7d">7 天</a>' in history
    assert "<title>Airgap Sync · 源端历史</title>" in history
    assert '"captured_at": "2026-09-27T20:30:00Z"' in history
    assert dashboard_before["system"]["boot_time"] == "2026-09-27T18:00:00+00:00"
    assert client.get("/api/dashboard").json()["sources"]["samples"][0]["payload"] == payload
    assert client.get("/api/sources?limit=1").json()["next_cursor"] == api_before["next_cursor"]
    assert client.get("/api/sources?limit=1").json()["samples"][0]["payload"] == payload
    assert (
        client.get("/api/sources?limit=1").json()["samples"][0]["sample_id"]
        == (original["samples"][0]["sample_id"])
    )
    assert store.read_sources(cfg, node="source-01", limit=1, now=clock[0]) == original


def test_source_history_dst_transition_uses_zoneinfo(cfg, payload, config_data, monkeypatch):
    from test_monitor import FakeConnection

    samples = []
    for stamp in ("2026-11-01T05:30:00Z", "2026-11-01T06:30:00Z"):
        observed = copy.deepcopy(payload)
        observed["captured_at"] = stamp
        samples.append({"payload": observed, "received_at": stamp, "age_seconds": 0})
    monkeypatch.setattr(
        "airgap_sync.monitor.app.read_sources",
        lambda *args, **kwargs: {"status": "OK", "samples": samples, "next_cursor": None},
    )
    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", FakeConnection)
    config_data.update(
        role="destination",
        destination={"incoming_dir": str(cfg.incoming), "report_timezone": "America/New_York"},
        monitor_ingest=cfg.model_dump(),
    )
    html = (
        TestClient(create_app(AppConfig.model_validate(config_data)))
        .get("/source-history?node=source-01")
        .text
    )
    assert "2026-11-01 01:30:00 EDT" in html
    assert "2026-11-01 01:30:00 EST" in html


def test_one_shot_cli(cfg, config_data, write_config):
    config_data.update(
        role="destination",
        destination={"incoming_dir": str(cfg.incoming)},
        monitor_ingest=cfg.model_dump(mode="json"),
    )
    path = write_config(config_data)
    result = CliRunner().invoke(cli, ["destination", "monitor-ingest", "--config", str(path)])
    assert result.exit_code == 0, result.output
    assert "candidates=0" in result.output
    with Ingestor(cfg):
        result = CliRunner().invoke(cli, ["destination", "monitor-ingest", "--config", str(path)])
        assert result.exit_code != 0 and "already owned" in result.output


def test_unknown_version_envelope_and_illegal_names(cfg, payload):
    clock = [NOW.timestamp()]
    valid = put(cfg, payload)
    unknown = valid.with_name(valid.name.replace("-v1--", "-v2--"))
    valid.rename(unknown)
    bad_stamp = unknown.with_name(unknown.name.replace("20260928T080000Z", "20269999T080000Z"))
    bad_stamp.write_bytes(json.dumps(payload).encode())
    untouched = cfg.incoming / "airgap-monitor-v1--../manifest.json"
    with pytest.raises(ValueError):
        decode(json.dumps(payload).encode(), str(untouched))
    with Ingestor(cfg, lambda: clock[0]) as ing:
        rounds(ing, clock, 6, 31)
        assert counts(cfg) == 0
        assert len(list((cfg.incoming / ".airgap-monitor-quarantine").glob("*.json"))) == 2


def test_post_commit_replacement_is_not_deleted(cfg, payload, monkeypatch):
    clock = [NOW.timestamp()]
    path = put(cfg, payload)
    original = store.ingest
    replacement = {**payload, "agent_version": "replacement"}
    changed = False

    def replace_after_commit(*args):
        nonlocal changed
        result = original(*args)
        if not changed:
            changed = True
            pending = cfg.incoming / ".airgap-monitor-processing" / path.name
            temp = cfg.incoming / "replacement.part"
            temp.write_bytes(json.dumps(replacement).encode())
            os.replace(temp, pending)
        return result

    monkeypatch.setattr(store, "ingest", replace_after_commit)
    with Ingestor(cfg, lambda: clock[0]) as ing:
        ing.tick()
        clock[0] += 2
        ing.tick()
        pending = cfg.incoming / ".airgap-monitor-processing" / path.name
        assert json.loads(pending.read_bytes()) == replacement
        assert counts(cfg) == 1
        rounds(ing, clock, 6, 31)
        assert not pending.exists()  # independently validated, then conflict quarantine
        assert counts(cfg) == 1


def test_linked_directory_and_database_refused(cfg, tmp_path):
    actual = tmp_path / "actual"
    actual.mkdir()
    cfg.incoming.symlink_to(actual, target_is_directory=True)
    with pytest.raises(OSError), Ingestor(cfg):
        pass
    cfg.incoming.unlink()
    cfg.db_path.parent.mkdir()
    target = actual / "business.db"
    target.write_bytes(b"business")
    cfg.db_path.symlink_to(target)
    with pytest.raises(OSError), Ingestor(cfg):
        pass
    assert target.read_bytes() == b"business"


def test_read_limit_is_real_not_stat_size(cfg, payload, monkeypatch):
    clock = [NOW.timestamp()]
    put(cfg, payload, body=b"x" * 1_000_000)
    requested = []
    fdopen = os.fdopen

    class Reader:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.handle.close()

        def fileno(self):
            return self.handle.fileno()

        def read(self, size):
            requested.append(size)
            assert size == 65537
            return self.handle.read(size)

    def wrap(fd, mode):
        handle = fdopen(fd, mode)
        return Reader(handle) if mode == "rb" else handle

    monkeypatch.setattr(os, "fdopen", wrap)
    with Ingestor(cfg, lambda: clock[0]) as ing:
        rounds(ing, clock, 5, 31)
    assert requested and counts(cfg) == 0


def test_null_current_never_falls_back_and_storage_time_is_preserved(cfg, payload):
    clock = [NOW.timestamp()]
    older = copy.deepcopy(payload)
    older["system"]["cpu_percent"] = 50
    older["captured_at"] = "2026-09-28T07:00:00Z"
    payload["system"]["cpu_percent"] = None
    payload["system"]["memory_available_bytes"] = 0
    payload["filesystems"] = [
        {
            "mount": None,
            "total_bytes": None,
            "free_bytes": None,
            "free_percent": None,
            "locations": ["data"],
        }
    ]
    payload["managed_storage"]["captured_at"] = "2026-09-28T06:00:00Z"
    with Ingestor(cfg, lambda: clock[0]) as ing:
        put(cfg, older)
        put(cfg, payload)
        rounds(ing, clock)
        latest = store.read_sources(cfg)["samples"][0]["payload"]
        assert latest["system"]["cpu_percent"] is None
        assert latest["system"]["memory_available_bytes"] == 0
        assert latest["filesystems"][0]["total_bytes"] is None
        assert latest["managed_storage"]["captured_at"] == "2026-09-28T06:00:00Z"


def test_missing_monitor_db_preserves_successful_destination(cfg, config_data, monkeypatch):
    from test_monitor import FakeConnection

    config_data.update(
        role="destination",
        destination={"incoming_dir": str(cfg.incoming)},
        monitor_ingest=cfg.model_dump(),
    )
    monkeypatch.setattr("airgap_sync.monitor.dashboard.DestinationMySQLConnection", FakeConnection)
    client = TestClient(create_app(AppConfig.model_validate(config_data)))
    data = client.get("/api/dashboard").json()
    assert data["sources"]["status"] == "UNAVAILABLE"
    assert data["rds"] == "CONNECTED"
    assert data["latest_verified"]["run_id"] == "verified"
    assert not cfg.db_path.exists()  # GET never initializes database


def test_new_config_remains_opt_in_and_bounds(cfg):
    from pydantic import ValidationError

    for fields in (
        {"dedup_days": 35},
        {"db_path": cfg.db_path.with_name("meta.db")},
        {"incoming": "relative"},
        {"batch_size": 1001},
        {"history_days": 29},
        {"poll_seconds": 0},
        {"settle_seconds": 0},
    ):
        with pytest.raises(ValidationError):
            MonitorIngestConfig.model_validate({**cfg.model_dump(), **fields})


def test_commit_failure_rolls_back_and_retry_recovers(cfg, payload, monkeypatch):
    clock = [NOW.timestamp()]
    path = put(cfg, payload)
    original = store.ingest

    def fail_commit(db, *args):
        def authorizer(action, arg1, *rest):
            if action == sqlite3.SQLITE_TRANSACTION and arg1 == "COMMIT":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        db.set_authorizer(authorizer)
        try:
            return original(db, *args)
        finally:
            db.set_authorizer(None)

    with Ingestor(cfg, lambda: clock[0]) as ing:
        monkeypatch.setattr(store, "ingest", fail_commit)
        rounds(ing, clock)
        assert counts(cfg) == counts(cfg, "content_receipts") == 0
        assert (cfg.incoming / ".airgap-monitor-processing" / path.name).exists()
        monkeypatch.setattr(store, "ingest", original)
        rounds(ing, clock)
        assert counts(cfg) == 1


def test_readers_can_query_during_writer_and_history_windows(cfg, payload):
    clock = [NOW.timestamp()]
    with Ingestor(cfg, lambda: clock[0]), store.connect(cfg.db_path, write=True) as db:
        for i in (0, 2, 8, 31):
            sample = {
                **payload,
                "captured_at": (NOW - timedelta(days=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            name = put(cfg, sample).name
            sample, canonical, digest = decode(json.dumps(sample).encode(), name)
            store.ingest(db, cfg, name, sample, canonical, digest, clock[0])
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE node_samples SET payload='{}'")
        for window, expected in (("24h", 1), ("7d", 2), ("30d", 3)):
            data = store.read_sources(cfg, node="source-01", window=window, now=clock[0])
            assert data["status"] == "OK" and len(data["samples"]) == expected
            assert data["samples"][0]["payload"]["node_id"] == "source-01"
        db.rollback()


def test_ingest_does_not_log_input_or_raw_errors(cfg, payload, monkeypatch, caplog):
    background = BackgroundIngest(cfg)

    def fail(self):
        background.stop_event.set()
        raise RuntimeError("password=super-secret token=private arbitrary-payload")

    monkeypatch.setattr(Ingestor, "tick", fail)
    background.run()
    assert "Monitor ingest round unavailable" in caplog.text
    assert "super-secret" not in caplog.text and "arbitrary-payload" not in caplog.text


def test_corrupt_sample_degrades_without_touching_database(cfg, payload):
    clock = [NOW.timestamp()]
    with Ingestor(cfg, lambda: clock[0]) as ing:
        put(cfg, payload)
        rounds(ing, clock)
        with store.connect(cfg.db_path, write=True) as db, db:
            db.execute("UPDATE node_samples SET payload='{}'")
        assert store.read_sources(cfg)["status"] == "UNAVAILABLE"
        assert counts(cfg) == 1
