"""Source MySQL 只读连接与元数据检查。

本模块只执行 SELECT / information_schema 查询,
不会对源业务数据库执行 CREATE / ALTER / UPDATE / DELETE。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import pymysql

from airgap_sync.common.config import resolve_password
from airgap_sync.common.models import MySQLConfig, TableConfig, TableMode


class SourceMySQLError(Exception):
    """连接或查询 Source MySQL 失败。"""


class SourceMySQLConnection:
    """Source MySQL 连接的简单封装。

    密码通过 mysql.password_env 指定的环境变量读取,
    不会出现在日志或异常信息中。
    """

    def __init__(self, config: MySQLConfig) -> None:
        self._config = config
        self._conn: pymysql.Connection | None = None

    def connect(self) -> None:
        if self._conn is not None:
            return
        password = resolve_password(self._config)
        try:
            self._conn = pymysql.connect(
                host=self._config.host,
                port=self._config.port,
                user=self._config.user,
                password=password,
                database=self._config.database,
                connect_timeout=self._config.connect_timeout,
                charset="utf8mb4",
            )
        except pymysql.Error as exc:
            raise SourceMySQLError(
                f"cannot connect to MySQL {self._config.host}:{self._config.port} "
                f"database '{self._config.database}' as user '{self._config.user}': {exc}"
            ) from exc

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> SourceMySQLConnection:
        self.connect()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        """执行只读查询并返回全部行。"""
        if self._conn is None:
            raise SourceMySQLError("connection is not open; call connect() first")
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
        except pymysql.Error as exc:
            raise SourceMySQLError(f"MySQL query failed: {exc}") from exc
        return [tuple(row) for row in rows]

    def ping(self) -> str:
        """执行 SELECT 1 验证连接, 返回服务器版本号。"""
        rows = self.fetch_all("SELECT 1, VERSION()")
        return str(rows[0][1])


class QueryExecutor(Protocol):
    """能执行 fetch_all 的连接 (SourceMySQLConnection 或测试替身)。"""

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ...) -> list[tuple[Any, ...]]: ...


@dataclass(frozen=True)
class TableCheckResult:
    """单张表的元数据检查结果。"""

    table: TableConfig
    ok: bool
    error_code: str | None = None
    missing_key_columns: tuple[str, ...] = ()


def fetch_table_columns(
    executor: QueryExecutor, database: str, table: str
) -> frozenset[str] | None:
    """返回指定表的所有列名; 表不存在时返回 None。"""
    rows = executor.fetch_all(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (database, table),
    )
    if not rows:
        return None
    return frozenset(str(row[0]) for row in rows)


def check_table(table: TableConfig, columns: frozenset[str] | None) -> TableCheckResult:
    """检查单张表: 表是否存在; keyed 模式的 key 字段是否存在。

    本阶段不检查 key 是否为 NULL、是否唯一 (属于后续真实数据检查任务)。
    """
    if columns is None:
        return TableCheckResult(table=table, ok=False, error_code="TABLE_NOT_FOUND")
    if table.mode is TableMode.KEYED:
        missing = tuple(column for column in table.key or () if column not in columns)
        if missing:
            return TableCheckResult(
                table=table,
                ok=False,
                error_code="KEY_COLUMN_NOT_FOUND",
                missing_key_columns=missing,
            )
    return TableCheckResult(table=table, ok=True)


def check_tables(
    executor: QueryExecutor, database: str, tables: list[TableConfig]
) -> list[TableCheckResult]:
    """依次检查一组同步表。"""
    return [
        check_table(table, fetch_table_columns(executor, database, table.name)) for table in tables
    ]
