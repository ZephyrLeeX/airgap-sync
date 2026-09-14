"""Source MySQL 只读连接与元数据检查。

本模块实现"双层只读保护", 保证 Airgap Sync 不修改源业务数据库:

1. 应用层: 对外只提供 SELECT 查询能力 (fetch_all / ping),
   非 SELECT SQL 在发送给 MySQL 之前直接拒绝;
2. MySQL 层: 连接建立后立即执行
   SET SESSION TRANSACTION READ ONLY
   将当前 Session 设为只读。即使数据库账号具有
   INSERT / UPDATE / DELETE / ALTER / DROP 权限, 写操作也会被
   MySQL 以错误 1792 (Cannot execute statement in a read-only
   transaction) 拒绝。

服务端只读设置只影响当前 Session: 不修改 GLOBAL 变量, 不需要 SUPER 权限;
连接每次重新建立 (connect) 后都会重新设置, 设置或验证失败时连接初始化
直接失败, 不会静默降级为可写连接。

本模块不提供任何执行 DML/DDL 的公共入口, 也没有暴露原始连接的 API。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import pymysql

from airgap_sync.common.config import resolve_password
from airgap_sync.common.models import MySQLConfig, TableConfig, TableMode

_SET_SESSION_READ_ONLY_SQL = "SET SESSION TRANSACTION READ ONLY"

_CHECK_SESSION_READ_ONLY_SQL = "SELECT @@session.transaction_read_only"


class SourceMySQLError(Exception):
    """连接或查询 Source MySQL 失败。"""


def _ensure_select_only(sql: str) -> None:
    """应用层只读保护: 只允许 SELECT 语句。

    刻意不引入 SQL parser, 只检查第一个关键字是否为 SELECT;
    Source 端需要执行的查询只有 SELECT (ping / information_schema /
    后续 Scanner 的流式 SELECT), 拒绝时只报告关键字, 不回显 SQL 内容。
    """
    stripped = sql.lstrip()
    first_word = stripped.split(None, 1)[0].lower() if stripped else ""
    if first_word != "select":
        detail = (
            f"statement starts with '{first_word.upper()}'" if first_word else "empty statement"
        )
        raise SourceMySQLError(
            "non-read-only SQL is not allowed on Source MySQL connection: "
            f"only SELECT queries are permitted ({detail})"
        )


class SourceMySQLConnection:
    """Source MySQL 连接的只读封装。

    密码通过 mysql.password_env 指定的环境变量读取,
    不会出现在日志或异常信息中。
    """

    def __init__(self, config: MySQLConfig) -> None:
        self._config = config
        self._conn: pymysql.Connection | None = None

    def connect(self) -> None:
        """建立连接并启用服务端只读保护。

        - autocommit 打开: 每条查询是独立事务, 连接不会隐式持有长事务;
        - 连接成功后立即将 Session 设为 READ ONLY 并验证生效;
        - 设置失败时关闭连接并抛出 SourceMySQLError, 不允许静默忽略。
        """
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
                autocommit=True,
            )
        except pymysql.Error as exc:
            raise SourceMySQLError(
                f"cannot connect to MySQL {self._config.host}:{self._config.port} "
                f"database '{self._config.database}' as user '{self._config.user}': {exc}"
            ) from exc
        try:
            self._enable_session_read_only()
        except BaseException:
            self.close()
            raise

    def _enable_session_read_only(self) -> None:
        """将当前 Session 设置为 READ ONLY 并验证生效。

        整个模块中唯一执行的固定非 SELECT 语句就是这里的
        SET SESSION TRANSACTION READ ONLY, 它只在连接初始化时执行一次,
        不存在对外的执行入口。MySQL 5.7 支持 @@session.transaction_read_only,
        用于验证设置确实生效。
        """
        assert self._conn is not None
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(_SET_SESSION_READ_ONLY_SQL)
        except pymysql.Error as exc:
            raise SourceMySQLError(f"failed to set READ ONLY session: {exc}") from exc

        rows = self.fetch_all(_CHECK_SESSION_READ_ONLY_SQL)
        if not rows or int(rows[0][0]) != 1:
            got = rows[0][0] if rows else "no value"
            raise SourceMySQLError(
                "failed to set READ ONLY session: "
                f"@@session.transaction_read_only = {got}, expected 1"
            )

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
        """执行只读 SELECT 查询并返回全部行。

        非 SELECT SQL 在发送给 MySQL 之前被拒绝 (应用层只读保护);
        连接的 Session 本身也已是 READ ONLY (服务端只读保护)。
        """
        _ensure_select_only(sql)
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
    """能执行只读查询的连接 (SourceMySQLConnection 或测试替身)。"""

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
