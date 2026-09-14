"""Source 本地 SQLite 状态库。

状态库位于 <data_dir>/state/meta.db。首次使用时自动创建目录、
数据库和 schema; 初始化是幂等的, 重复运行不会报错。

状态库只保存控制信息: 表状态 / Run 状态 (后续阶段还有上传状态)。
不保存业务行数据或行 Hash 状态 —— V1 的 Full Snapshot 不依赖
任何业务数据指纹, 这部分设计已随增量方案一并删除。

schema v2 (相对 v1 删除了失去意义的 table_state.mode 列):

    CREATE TABLE table_state (
        table_name      TEXT PRIMARY KEY,
        current_run_id  TEXT,     -- 最近成功生成的完整 Snapshot Run
        status          TEXT NOT NULL,
        last_run_id     TEXT,     -- 最近一次尝试的 Run (含失败)
        last_error      TEXT,
        created_at      TEXT NOT NULL,
        updated_at      TEXT NOT NULL
    );

current_run_id 的语义是 "Source 最近成功生成的完整 Snapshot Run",
不代表目标端已经同步 —— 严格单向网络下 Source 永远无法知道
目标 Apply 状态。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

SCHEMA_VERSION = 2

STATE_DIR_NAME = "state"
STATE_DB_NAME = "meta.db"

_TABLE_STATE_COLUMNS = (
    "table_name, current_run_id, status, last_run_id, last_error, created_at, updated_at"
)

_CREATE_TABLE_STATE_SQL = (
    "CREATE TABLE table_state ("
    "    table_name      TEXT PRIMARY KEY,"
    "    current_run_id  TEXT,"
    "    status          TEXT NOT NULL,"
    "    last_run_id     TEXT,"
    "    last_error      TEXT,"
    "    created_at      TEXT NOT NULL,"
    "    updated_at      TEXT NOT NULL"
    ")"
)


class StateError(Exception):
    """状态库初始化或访问失败。"""


class TableStatus(StrEnum):
    """table_state.status 的取值。"""

    IDLE = "IDLE"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


def state_db_path(data_dir: Path) -> Path:
    """状态库文件路径: <data_dir>/state/meta.db。"""
    return data_dir / STATE_DIR_NAME / STATE_DB_NAME


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


@dataclass(frozen=True)
class TableState:
    """table_state 表中的一行。"""

    table_name: str
    current_run_id: str | None
    status: str
    last_run_id: str | None
    last_error: str | None
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
        """创建或迁移 schema (在单个事务内执行, 幂等)。

        - 全新数据库: 创建 v2 schema;
        - v1 开发期状态库 (table_state 含 mode 列): 简单迁移到 v2 ——
          重建 table_state (mode 列删除, 控制信息重新开始)。
          项目尚未生产运行, 不做通用 migration framework;
        - 其他版本: 明确报错, 不静默产生错误 schema。
        """
        try:
            with self._transaction():
                self._conn.execute(
                    "CREATE TABLE IF NOT EXISTS schema_version (    version INTEGER NOT NULL)"
                )
                row = self._conn.execute("SELECT version FROM schema_version").fetchone()
                if row is None:
                    self._conn.execute(_CREATE_TABLE_STATE_SQL)
                    self._conn.execute(
                        "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
                    )
                else:
                    version = int(row["version"])
                    self._check_version(version)
                    if version == 1:
                        self._migrate_v1_to_v2()
        except sqlite3.Error as exc:
            raise StateError(f"failed to initialize state database {self.db_path}: {exc}") from exc

    def _migrate_v1_to_v2(self) -> None:
        """v1 → v2: 丢弃含 mode 列的旧 table_state, 重建 v2 结构。

        旧状态只含开发期的 mode / run 标记, 直接重建;
        迁移与版本号更新在同一个事务内完成。
        """
        self._conn.execute("DROP TABLE IF EXISTS table_state")
        self._conn.execute(_CREATE_TABLE_STATE_SQL)
        self._conn.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION,))

    def _check_version(self, version: int) -> None:
        if version != SCHEMA_VERSION and version != 1:
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

    def register_table(self, table_name: str) -> None:
        """登记同步表 (幂等)。

        新表: current_run_id = NULL, status = 'IDLE';
        已登记: 只更新 updated_at, 不重置任何运行状态。
        """
        now = _utcnow_iso()
        try:
            with self._transaction():
                self._conn.execute(
                    f"INSERT INTO table_state ({_TABLE_STATE_COLUMNS})"
                    " VALUES (?, NULL, 'IDLE', NULL, NULL, ?, ?)"
                    " ON CONFLICT(table_name) DO UPDATE SET"
                    "     updated_at = excluded.updated_at",
                    (table_name, now, now),
                )
        except sqlite3.Error as exc:
            raise StateError(
                f"failed to register table '{table_name}' in {self.db_path}: {exc}"
            ) from exc

    def _update_run(self, table_name: str, run_id: str, assignments: dict[str, str | None]) -> None:
        """通用 Run 状态更新; 表未登记时明确报错。"""
        set_columns = ", ".join(f"{column} = ?" for column in assignments)
        sql = (
            "UPDATE table_state SET"
            f"    {set_columns},"
            "     last_run_id = ?,"
            "     updated_at = ?"
            " WHERE table_name = ?"
        )
        params = (*assignments.values(), run_id, _utcnow_iso(), table_name)
        try:
            with self._transaction():
                cursor = self._conn.execute(sql, params)
                if cursor.rowcount == 0:
                    raise StateError(
                        f"table '{table_name}' is not registered in {self.db_path}; "
                        "call register_table() first"
                    )
        except sqlite3.Error as exc:
            raise StateError(
                f"failed to update run state for '{table_name}' in {self.db_path}: {exc}"
            ) from exc

    def begin_run(self, table_name: str, run_id: str) -> None:
        """Run 开始: status = RUNNING, 清除上次错误。

        current_run_id 不变 —— 只有 complete_run 才能推进它。
        """
        self._update_run(table_name, run_id, {"status": TableStatus.RUNNING, "last_error": None})

    def complete_run(self, table_name: str, run_id: str) -> None:
        """Run 成功: status = COMPLETED, current_run_id = run_id。"""
        self._update_run(
            table_name,
            run_id,
            {"status": TableStatus.COMPLETED, "current_run_id": run_id, "last_error": None},
        )

    def fail_run(self, table_name: str, run_id: str, error: str) -> None:
        """Run 失败: status = FAILED, 记录错误。

        current_run_id 不变 —— 失败的 Run 不能成为 current。
        """
        self._update_run(table_name, run_id, {"status": TableStatus.FAILED, "last_error": error})

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
            current_run_id=row["current_run_id"],
            status=row["status"],
            last_run_id=row["last_run_id"],
            last_error=row["last_error"],
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
