"""Source 本地 SQLite 状态库。

状态库位于 <data_dir>/state/meta.db。首次使用时自动创建目录、
数据库和 schema; 初始化是幂等的, 重复运行不会报错。

Phase 1 只包含基础元数据表 (schema_version / table_state)。
行级 Current/Next hash 状态属于后续任务 (T004-T006)。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

SCHEMA_VERSION = 1

STATE_DIR_NAME = "state"
STATE_DB_NAME = "meta.db"

_TABLE_STATE_COLUMNS = "table_name, mode, current_run_id, status, created_at, updated_at"


class StateError(Exception):
    """状态库初始化或访问失败。"""


def state_db_path(data_dir: Path) -> Path:
    """状态库文件路径: <data_dir>/state/meta.db。"""
    return data_dir / STATE_DIR_NAME / STATE_DB_NAME


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


@dataclass(frozen=True)
class TableState:
    """table_state 表中的一行。"""

    table_name: str
    mode: str
    current_run_id: str | None
    status: str
    created_at: str
    updated_at: str


class SourceState:
    """Source 状态库句柄。"""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        try:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(db_path, isolation_level=None)
            self._conn.row_factory = sqlite3.Row
        except (OSError, sqlite3.Error) as exc:
            raise StateError(f"cannot open state database at {db_path}: {exc}") from exc

    def initialize(self) -> None:
        """创建 schema (在单个事务内执行, 幂等)。"""
        try:
            with self._transaction():
                self._conn.execute(
                    "CREATE TABLE IF NOT EXISTS schema_version (    version INTEGER NOT NULL)"
                )
                self._conn.execute(
                    "CREATE TABLE IF NOT EXISTS table_state ("
                    "    table_name      TEXT PRIMARY KEY,"
                    "    mode            TEXT NOT NULL,"
                    "    current_run_id  TEXT,"
                    "    status          TEXT NOT NULL,"
                    "    created_at      TEXT NOT NULL,"
                    "    updated_at      TEXT NOT NULL"
                    ")"
                )
                row = self._conn.execute("SELECT version FROM schema_version").fetchone()
                if row is None:
                    self._conn.execute(
                        "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
                    )
                else:
                    self._check_version(int(row["version"]))
        except sqlite3.Error as exc:
            raise StateError(f"failed to initialize state database {self.db_path}: {exc}") from exc

    def _check_version(self, version: int) -> None:
        if version != SCHEMA_VERSION:
            raise StateError(
                f"state database {self.db_path} has schema version {version}, "
                f"but this program expects {SCHEMA_VERSION}"
            )

    def schema_version(self) -> int:
        """返回当前状态库 schema 版本。"""
        try:
            row = self._conn.execute("SELECT version FROM schema_version").fetchone()
        except sqlite3.Error as exc:
            raise StateError(f"state database {self.db_path} is not initialized: {exc}") from exc
        if row is None:
            raise StateError(
                f"state database {self.db_path} is not initialized; call initialize() first"
            )
        return int(row["version"])

    def register_table(self, table_name: str, mode: str) -> None:
        """登记同步表 (幂等)。

        - 未登记: 插入, current_run_id = NULL, status = 'IDLE';
        - 已登记且 current_run_id 为 NULL (尚无 committed baseline):
          允许调整 mode;
        - 已登记且 current_run_id 非空 (已产生 baseline): 禁止修改 mode,
          报错并要求通过显式 reset / snapshot 机制处理 (本阶段未实现)。

        任何情况下都不会重置 current_run_id / status 等运行状态。
        """
        now = _utcnow_iso()
        try:
            with self._transaction():
                existing = self._conn.execute(
                    "SELECT mode, current_run_id FROM table_state WHERE table_name = ?",
                    (table_name,),
                ).fetchone()
                if (
                    existing is not None
                    and existing["current_run_id"] is not None
                    and existing["mode"] != mode
                ):
                    raise StateError(
                        f"cannot change sync mode of table '{table_name}' from "
                        f"'{existing['mode']}' to '{mode}': table already has a committed "
                        f"baseline (current_run_id={existing['current_run_id']}); "
                        "use an explicit reset or snapshot mechanism instead"
                    )
                self._conn.execute(
                    f"INSERT INTO table_state ({_TABLE_STATE_COLUMNS})"
                    " VALUES (?, ?, NULL, 'IDLE', ?, ?)"
                    " ON CONFLICT(table_name) DO UPDATE SET"
                    "     mode = excluded.mode,"
                    "     updated_at = excluded.updated_at",
                    (table_name, mode, now, now),
                )
        except sqlite3.Error as exc:
            raise StateError(
                f"failed to register table '{table_name}' in {self.db_path}: {exc}"
            ) from exc

    def get_table_state(self, table_name: str) -> TableState | None:
        """读取指定表的同步状态; 未登记时返回 None。"""
        try:
            row = self._conn.execute(
                f"SELECT {_TABLE_STATE_COLUMNS} FROM table_state WHERE table_name = ?",
                (table_name,),
            ).fetchone()
        except sqlite3.Error as exc:
            raise StateError(
                f"failed to read table_state for '{table_name}' from {self.db_path}: {exc}"
            ) from exc
        if row is None:
            return None
        return TableState(
            table_name=row["table_name"],
            mode=row["mode"],
            current_run_id=row["current_run_id"],
            status=row["status"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self._conn.execute("BEGIN")
        try:
            yield
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> SourceState:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
