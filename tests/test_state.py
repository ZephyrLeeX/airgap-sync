"""SQLite 状态库测试 (schema v2)。"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from airgap_sync.source.state import (
    SCHEMA_VERSION,
    SourceState,
    StateError,
    TableStatus,
    state_db_path,
)


@pytest.fixture
def state(tmp_path: Path) -> SourceState:
    state = SourceState(state_db_path(tmp_path / "data"))
    state.initialize()
    return state


class TestInitialization:
    def test_first_initialization(self, tmp_path):
        db_path = state_db_path(tmp_path / "data")
        state = SourceState(db_path)
        try:
            state.initialize()
            assert db_path.exists()
            assert state.schema_version() == SCHEMA_VERSION
        finally:
            state.close()

    def test_repeated_initialization_same_handle(self, state):
        state.initialize()  # 不报错
        state.initialize()
        assert state.schema_version() == SCHEMA_VERSION

    def test_reopen_existing_database(self, tmp_path):
        db_path = state_db_path(tmp_path / "data")
        first = SourceState(db_path)
        first.initialize()
        first.register_table("t_a")
        first.close()

        second = SourceState(db_path)
        try:
            second.initialize()  # 已有库再次初始化, 不报错
            assert second.schema_version() == SCHEMA_VERSION
            assert second.get_table_state("t_a") is not None
        finally:
            second.close()

    def test_creates_nested_directories(self, tmp_path):
        db_path = state_db_path(tmp_path / "a" / "b" / "data")
        state = SourceState(db_path)
        try:
            state.initialize()
            assert (tmp_path / "a" / "b" / "data" / "state").is_dir()
        finally:
            state.close()

    def test_newer_schema_version_rejected(self, tmp_path):
        db_path = state_db_path(tmp_path / "data")
        state = SourceState(db_path)
        state.initialize()
        state._conn.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION + 1,))
        state.close()

        with pytest.raises(StateError, match="schema version"):
            reopen = SourceState(db_path)
            try:
                reopen.initialize()
            finally:
                reopen.close()

    def test_state_db_path_layout(self, tmp_path):
        assert state_db_path(tmp_path / "data") == tmp_path / "data" / "state" / "meta.db"

    def test_v2_schema_has_no_mode_column(self, state):
        columns = {row["name"] for row in state._conn.execute("PRAGMA table_info(table_state)")}
        assert "mode" not in columns
        assert {
            "table_name",
            "last_snapshot_run_id",
            "last_delivered_run_id",
            "status",
            "last_run_id",
            "last_error",
            "created_at",
            "updated_at",
        } <= columns

    def test_empty_sqlite_file_initializes(self, tmp_path):
        db_path = tmp_path / "empty.db"
        sqlite3.connect(db_path).close()
        state = SourceState(db_path)
        try:
            state.initialize()
            assert state.schema_version() == SCHEMA_VERSION
        finally:
            state.close()


class TestV1Migration:
    """v1 (table_state 含 mode 列) → v2 的简单迁移。"""

    @staticmethod
    def _create_v1_database(db_path: Path) -> None:
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
            conn.execute("INSERT INTO schema_version (version) VALUES (1)")
            conn.execute(
                "CREATE TABLE table_state ("
                "    table_name      TEXT PRIMARY KEY,"
                "    mode            TEXT NOT NULL,"
                "    current_run_id  TEXT,"
                "    status          TEXT NOT NULL,"
                "    created_at      TEXT NOT NULL,"
                "    updated_at      TEXT NOT NULL"
                ")"
            )
            conn.execute(
                "INSERT INTO table_state"
                " (table_name, mode, current_run_id, status, created_at, updated_at)"
                " VALUES ('t_old', 'keyed', 'run-0001', 'COMPLETED', '2026-01-01', '2026-01-01')"
            )
            conn.commit()
        finally:
            conn.close()

    def test_v1_database_migrated_to_v2(self, tmp_path):
        db_path = state_db_path(tmp_path / "data")
        db_path.parent.mkdir(parents=True)
        self._create_v1_database(db_path)

        state = SourceState(db_path)
        try:
            state.initialize()
            assert state.schema_version() == SCHEMA_VERSION
            columns = {row["name"] for row in state._conn.execute("PRAGMA table_info(table_state)")}
            assert "mode" not in columns
            # 旧开发期状态不保留 (mode 语义已废弃, 控制信息重新开始)
            assert state.get_table_state("t_old") is None
            # 新登记正常工作
            state.register_table("t_new")
            assert state.get_table_state("t_new") is not None
        finally:
            state.close()

    def test_migrated_database_reopens(self, tmp_path):
        db_path = state_db_path(tmp_path / "data")
        db_path.parent.mkdir(parents=True)
        self._create_v1_database(db_path)
        state = SourceState(db_path)
        state.initialize()
        state.register_table("t_a")
        state.close()

        reopen = SourceState(db_path)
        try:
            reopen.initialize()  # 幂等: v2 不再触发迁移
            assert reopen.schema_version() == SCHEMA_VERSION
            assert reopen.get_table_state("t_a") is not None
        finally:
            reopen.close()


class TestRegisterTable:
    def test_register_and_read(self, state):
        state.register_table("t_demo")
        row = state.get_table_state("t_demo")
        assert row is not None
        assert row.table_name == "t_demo"
        assert row.current_run_id is None
        assert row.status == TableStatus.IDLE.value
        assert row.last_run_id is None
        assert row.last_error is None
        assert row.created_at
        assert row.updated_at

    def test_register_is_idempotent(self, state):
        state.register_table("t_demo")
        state.register_table("t_demo")
        count = state._conn.execute("SELECT COUNT(*) FROM table_state").fetchone()[0]
        assert count == 1

    def test_reregister_does_not_reset_run_state(self, state):
        state.register_table("t_demo")
        state.begin_run("t_demo", "run-0001")
        state.register_table("t_demo")  # 再次登记
        row = state.get_table_state("t_demo")
        assert row is not None
        assert row.status == TableStatus.RUNNING.value
        assert row.last_run_id == "run-0001"

    def test_get_missing_table_returns_none(self, state):
        assert state.get_table_state("nope") is None

    def test_multiple_tables(self, state):
        state.register_table("t_a")
        state.register_table("t_b")
        assert state.get_table_state("t_a").table_name == "t_a"
        assert state.get_table_state("t_b").table_name == "t_b"


class TestRunLifecycle:
    def test_begin_run(self, state):
        state.register_table("t_demo")
        state.begin_run("t_demo", "run-0001")
        row = state.get_table_state("t_demo")
        assert row.status == TableStatus.RUNNING.value
        assert row.last_run_id == "run-0001"
        assert row.last_error is None
        assert row.current_run_id is None  # 未完成不推进

    def test_complete_run_advances_current_run_id(self, state):
        state.register_table("t_demo")
        state.begin_run("t_demo", "run-0001")
        state.complete_run("t_demo", "run-0001")
        row = state.get_table_state("t_demo")
        assert row.status == TableStatus.COMPLETED.value
        assert row.current_run_id == "run-0001"
        assert row.last_run_id == "run-0001"

    def test_fail_run_keeps_current_run_id(self, state):
        """失败的 Run 不能成为 current。"""
        state.register_table("t_demo")
        state.begin_run("t_demo", "run-0001")
        state.complete_run("t_demo", "run-0001")
        state.begin_run("t_demo", "run-0002")
        state.fail_run("t_demo", "run-0002", "boom")
        row = state.get_table_state("t_demo")
        assert row.status == TableStatus.FAILED.value
        assert row.current_run_id == "run-0001"  # 保持上一个成功 Run
        assert row.last_run_id == "run-0002"
        assert row.last_error == "boom"

    def test_successful_run_after_failure_advances(self, state):
        state.register_table("t_demo")
        state.begin_run("t_demo", "run-0001")
        state.fail_run("t_demo", "run-0001", "boom")
        state.begin_run("t_demo", "run-0002")  # 开始时清除上次错误
        row = state.get_table_state("t_demo")
        assert row.last_error is None
        state.complete_run("t_demo", "run-0002")
        row = state.get_table_state("t_demo")
        assert row.current_run_id == "run-0002"
        assert row.status == TableStatus.COMPLETED.value

    def test_run_on_unregistered_table_rejected(self, state):
        with pytest.raises(StateError, match="not registered"):
            state.begin_run("ghost", "run-0001")
        with pytest.raises(StateError, match="not registered"):
            state.complete_run("ghost", "run-0001")
        with pytest.raises(StateError, match="not registered"):
            state.fail_run("ghost", "run-0001", "boom")


class TestUninitialized:
    def test_schema_version_before_initialize(self, tmp_path):
        state = SourceState(state_db_path(tmp_path / "data"))
        try:
            with pytest.raises(StateError, match="not initialized"):
                state.schema_version()
        finally:
            state.close()
