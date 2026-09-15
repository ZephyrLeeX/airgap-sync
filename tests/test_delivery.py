from __future__ import annotations

import threading
from pathlib import Path

from airgap_sync.common.models import AppConfig
from airgap_sync.source.delivery import DeliveryRunner
from airgap_sync.source.state import SourceState, state_db_path
from airgap_sync.source.uploader import UploadConfirmation, UploadError


class Stream:
    columns = ["id"]

    def __init__(self, batches, produced=None):
        self.batches = batches
        self.produced = produced

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def __iter__(self):
        for index, batch in enumerate(self.batches):
            yield batch
            if index == 1 and self.produced is not None:
                self.produced.set()


class Source:
    def __init__(self, produced=None, ddl_after=None):
        self.produced = produced
        self.ddl_after = ddl_after
        self.ddl_calls = 0

    def fetch_all(self, sql, params=()):
        return [("BASE TABLE",)]

    def get_create_table(self, table):
        self.ddl_calls += 1
        if self.ddl_calls == 2 and self.ddl_after:
            return self.ddl_after
        return "CREATE TABLE `t` (`id` int) ENGINE=InnoDB"

    def stream_table(self, table, fetch_size):
        return Stream([[(1,)], [(2,)], [(3,)]], self.produced)


class Uploader:
    def __init__(self, started=None, release=None, fail_on=None):
        self.started = started
        self.release = release
        self.fail_on = fail_on
        self.names = []

    def upload(self, path, name, sha256, *, on_attempt=None):
        logical = name.split("--")[-1]
        self.names.append(logical)
        if on_attempt:
            on_attempt(1)
        if logical == "chunk-000001.jsonl.zst" and self.started:
            self.started.set()
            assert self.release.wait(5)
        if logical == self.fail_on:
            raise UploadError("FAIL", "fake", attempts=1)
        return UploadConfirmation(name, path.stat().st_size, sha256, f"req-{len(self.names)}", 1)


def config(tmp_path):
    return AppConfig.model_validate(
        {
            "role": "source",
            "mysql": {
                "host": "x",
                "database": "db",
                "user": "u",
                "password_env": "P",
            },
            "paths": {"data_dir": tmp_path / "data"},
            "snapshot": {"fetch_size": 1},
            "chunk": {"max_rows": 1, "max_uncompressed_bytes": 1000},
            "spool": {"max_pending_bytes": 1000000, "min_free_bytes": 0},
            "relay": {"base_url": "http://relay", "token_env": "T"},
            "tables": [{"name": "t"}],
        }
    )


def run(tmp_path, source, uploader):
    state = SourceState(state_db_path(tmp_path / "data"))
    state.initialize()
    try:
        result = DeliveryRunner(source, state, config(tmp_path), uploader).sync("t")
        artifacts = state.get_artifacts(result.run_id)
        table = state.get_table_state("t")
        return result, artifacts, table
    finally:
        state.close()


def test_producer_continues_while_first_upload_is_blocked(tmp_path):
    started = threading.Event()
    release = threading.Event()
    produced = threading.Event()
    output = []

    thread = threading.Thread(
        target=lambda: output.append(run(tmp_path, Source(produced), Uploader(started, release))[0])
    )
    thread.start()
    assert started.wait(5)
    assert produced.wait(5), "producer did not close later chunks while PUT 1 was blocked"
    release.set()
    thread.join(5)
    assert output[0].status == "DELIVERED"


def test_manifest_is_last_and_success_cleans_local_run(tmp_path):
    uploader = Uploader()
    result, artifacts, table = run(tmp_path, Source(), uploader)
    assert uploader.names == [
        "chunk-000001.jsonl.zst",
        "chunk-000002.jsonl.zst",
        "chunk-000003.jsonl.zst",
        "schema.sql",
        "manifest.json",
    ]
    assert result.status == "DELIVERED"
    assert all(a.upload_status == "UPLOADED" for a in artifacts)
    assert table.last_delivered_run_id == result.run_id
    assert not result.run_dir.exists()


def test_confirmed_chunk_cleanup_failure_is_not_reuploaded(tmp_path, monkeypatch):
    original_unlink = Path.unlink

    def fail_one_chunk(self, *args, **kwargs):
        if self.name == "chunk-000001.jsonl.zst":
            raise PermissionError("locked by test")
        return original_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_one_chunk)
    uploader = Uploader()
    result, artifacts, _ = run(tmp_path, Source(), uploader)
    assert result.status == "DELIVERED"
    assert uploader.names.count("chunk-000001.jsonl.zst") == 1
    chunk = next(a for a in artifacts if a.logical_name == "chunk-000001.jsonl.zst")
    assert chunk.upload_status == "UPLOADED"
    assert "locked by test" in chunk.cleanup_error


def test_chunk_failure_prevents_manifest(tmp_path):
    uploader = Uploader(fail_on="chunk-000001.jsonl.zst")
    result, artifacts, table = run(tmp_path, Source(), uploader)
    assert result.status == "FAILED"
    assert "manifest.json" not in uploader.names
    assert table.last_delivered_run_id is None
    assert any(a.upload_status == "FAILED" for a in artifacts)


def test_manifest_failure_advances_snapshot_but_not_delivered(tmp_path):
    uploader = Uploader(fail_on="manifest.json")
    result, _, table = run(tmp_path, Source(), uploader)
    assert result.status == "FAILED"
    assert uploader.names[-1] == "manifest.json"
    assert table.last_snapshot_run_id == result.run_id
    assert table.last_delivered_run_id is None


def test_ddl_change_prevents_schema_and_manifest(tmp_path):
    uploader = Uploader()
    result, _, table = run(tmp_path, Source(ddl_after="CREATE TABLE `t` (`x` int)"), uploader)
    assert result.status == "FAILED"
    assert "schema.sql" not in uploader.names
    assert "manifest.json" not in uploader.names
    assert table.last_delivered_run_id is None


def test_pending_spool_limit_fails_without_manifest(tmp_path):
    cfg = config(tmp_path)
    cfg.spool.max_pending_bytes = 1
    state = SourceState(state_db_path(tmp_path / "data"))
    state.initialize()
    uploader = Uploader()
    try:
        result = DeliveryRunner(Source(), state, cfg, uploader).sync("t")
        assert result.status == "DISK_PRESSURE"
        assert "manifest.json" not in uploader.names
        assert state.get_table_state("t").last_delivered_run_id is None
    finally:
        state.close()


def test_low_free_space_fails_without_manifest(tmp_path, monkeypatch):
    import airgap_sync.source.delivery as delivery_module

    cfg = config(tmp_path)
    cfg.spool.min_free_bytes = 100
    monkeypatch.setattr(
        delivery_module.shutil,
        "disk_usage",
        lambda path: type("Usage", (), {"total": 1000, "used": 999, "free": 1})(),
    )
    state = SourceState(state_db_path(tmp_path / "data"))
    state.initialize()
    uploader = Uploader()
    try:
        result = DeliveryRunner(Source(), state, cfg, uploader).sync("t")
        assert result.status == "DISK_PRESSURE"
        assert "manifest.json" not in uploader.names
        assert list(result.run_dir.glob("*.part")) == []
        assert state.get_table_state("t").last_delivered_run_id is None
    finally:
        state.close()
