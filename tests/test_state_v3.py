from __future__ import annotations

import sqlite3

from airgap_sync.source.state import SourceState, state_db_path


def test_v2_migrates_and_preserves_snapshot_pointer(tmp_path):
    path = state_db_path(tmp_path / "data")
    path.parent.mkdir(parents=True)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
    conn.execute("INSERT INTO schema_version VALUES (2)")
    conn.execute(
        "CREATE TABLE table_state (table_name TEXT PRIMARY KEY,current_run_id TEXT,"
        "status TEXT NOT NULL,last_run_id TEXT,last_error TEXT,created_at TEXT NOT NULL,"
        "updated_at TEXT NOT NULL)"
    )
    conn.execute("INSERT INTO table_state VALUES ('t','old','COMPLETED','old',NULL,'a','b')")
    conn.commit()
    conn.close()
    with SourceState(path) as state:
        state.initialize()
        assert state.schema_version() == 4
        row = state.get_table_state("t")
        assert row.last_snapshot_run_id == "old"
        assert row.last_delivered_run_id is None
        assert row.current_run_id == "old"


def test_run_artifact_and_delivered_lifecycle(tmp_path):
    with SourceState(state_db_path(tmp_path / "data")) as state:
        state.initialize()
        state.register_table("t")
        state.begin_run("t", "run")
        state.register_artifact("run", "chunk", 1, "chunk", "remote", 3, "a" * 64)
        state.mark_upload_attempt("run", "chunk", 1)
        state.mark_uploaded("run", "chunk", 1, "req")
        state.deliver_run("t", "run", row_count=1, chunk_count=1, raw_bytes=4, compressed_bytes=3)
        artifact = state.get_artifacts("run")[0]
        assert artifact.upload_status == "UPLOADED"
        assert artifact.request_id == "req"
        assert artifact.uploaded_at
        row = state.get_table_state("t")
        assert row.last_snapshot_run_id == "run"
        assert row.last_delivered_run_id == "run"


def test_failed_run_does_not_advance_delivered(tmp_path):
    with SourceState(state_db_path(tmp_path / "data")) as state:
        state.initialize()
        state.register_table("t")
        state.begin_run("t", "run")
        state.fail_run("t", "run", "boom")
        assert state.get_table_state("t").last_delivered_run_id is None


def test_v3_migrates_to_v4_with_empty_cycle_history(tmp_path):
    path = state_db_path(tmp_path / "data")
    with SourceState(path) as state:
        state.initialize()
    conn = sqlite3.connect(path)
    conn.execute("DROP TABLE cycle_tables")
    conn.execute("DROP TABLE sync_cycles")
    conn.execute("UPDATE schema_version SET version=3")
    conn.commit()
    conn.close()
    with SourceState(path) as state:
        state.initialize()
        assert state.schema_version() == 4
        assert state.latest_cycle() is None
