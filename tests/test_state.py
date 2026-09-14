"""SQLite 状态库测试。"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from airgap_sync.source.state import (
    SCHEMA_VERSION,
    SourceState,
    StateError,
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
        first.close()

        second = SourceState(db_path)
        try:
            second.initialize()  # 已有库再次初始化, 不报错
            assert second.schema_version() == SCHEMA_VERSION
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


class TestTableState:
    def test_register_and_read(self, state):
        state.register_table("t_keyed", "keyed")
        row = state.get_table_state("t_keyed")
        assert row is not None
        assert row.table_name == "t_keyed"
        assert row.mode == "keyed"
        assert row.current_run_id is None
        assert row.status == "IDLE"
        assert row.created_at
        assert row.updated_at

    def test_register_is_idempotent(self, state):
        state.register_table("t_keyed", "keyed")
        state.register_table("t_keyed", "keyed")
        rows = state._conn.execute("SELECT COUNT(*) FROM table_state").fetchone()
        assert rows[0] == 1

    def test_reregister_updates_mode_not_status(self, state):
        state.register_table("t_keyed", "keyed")
        state._conn.execute(
            "UPDATE table_state SET status = 'SCANNING' WHERE table_name = 't_keyed'"
        )
        state.register_table("t_keyed", "row_multiset")
        row = state.get_table_state("t_keyed")
        assert row is not None
        assert row.mode == "row_multiset"
        assert row.status == "SCANNING"  # 运行状态不被登记操作重置

    def test_get_missing_table_returns_none(self, state):
        assert state.get_table_state("nope") is None

    def test_multiple_tables(self, state):
        state.register_table("t_a", "keyed")
        state.register_table("t_b", "row_multiset")
        assert state.get_table_state("t_a").mode == "keyed"
        assert state.get_table_state("t_b").mode == "row_multiset"


class TestUninitialized:
    def test_schema_version_before_initialize(self, tmp_path):
        state = SourceState(state_db_path(tmp_path / "data"))
        try:
            with pytest.raises(StateError, match="not initialized"):
                state.schema_version()
        finally:
            state.close()

    def test_direct_sqlite_file_rejected(self, tmp_path):
        """空 SQLite 文件 (无 schema) 应报错而不是崩溃。"""
        db_path = tmp_path / "empty.db"
        sqlite3.connect(db_path).close()
        state = SourceState(db_path)
        try:
            state.initialize()  # 空 db 应能正常初始化
            assert state.schema_version() == SCHEMA_VERSION
        finally:
            state.close()
