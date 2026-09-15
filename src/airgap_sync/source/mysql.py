"""Source MySQL 只读连接、安全 identifier 引用与流式扫描。

本模块实现"双层只读保护", 保证 Airgap Sync 不修改源业务数据库:

1. 应用层: 通用查询入口 fetch_all 只允许 SELECT,
   非 SELECT SQL 在发送给 MySQL 之前直接拒绝;
2. MySQL 层: 连接建立后立即执行
   SET SESSION TRANSACTION READ ONLY
   将当前 Session 设为只读。即使数据库账号具有
   INSERT / UPDATE / DELETE / ALTER / DROP 权限, 写操作也会被
   MySQL 以错误 1792 (Cannot execute statement in a read-only
   transaction) 拒绝。

Full Snapshot 需要的额外能力不放宽上述边界, 全部通过专用方法实现:

* get_create_table(table)  —— 只构造 SHOW CREATE TABLE `安全引用表名`;
* stream_table(table, ...) —— 只构造 SELECT * FROM `安全引用表名`,
  使用 SSCursor (server-side cursor) 流式读取。

不提供任何执行任意 SQL 的入口, 也不暴露原始连接。
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Protocol

import pymysql
import pymysql.cursors

from airgap_sync.common.config import resolve_password
from airgap_sync.common.models import MySQLConfig, TableConfig

_SET_SESSION_READ_ONLY_SQL = "SET SESSION TRANSACTION READ ONLY"
_SET_SESSION_TIME_ZONE_SQL = "SET SESSION time_zone = '+00:00'"

_CHECK_SESSION_READ_ONLY_SQL = "SELECT @@session.transaction_read_only"


class SourceMySQLError(Exception):
    """连接或查询 Source MySQL 失败。"""


def _ensure_select_only(sql: str) -> None:
    """应用层只读保护: 只允许 SELECT 语句。

    刻意不引入 SQL parser, 只检查第一个关键字是否为 SELECT;
    Source 端需要执行的通用查询只有 SELECT (ping / information_schema),
    拒绝时只报告关键字, 不回显 SQL 内容。
    SHOW CREATE TABLE 等专用语句不经过本入口, 由各自的专用方法
    以固定模板构造。
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


def quote_identifier(name: str) -> str:
    """安全引用 MySQL identifier: 反引号包裹, 名称内反引号翻倍。

    这是所有把表名拼进 SQL 的唯一途径; 任何 f"SELECT * FROM {table}"
    式的直接拼接都被禁止。
    """
    if not name:
        raise SourceMySQLError("identifier must not be empty")
    return "`" + name.replace("`", "``") + "`"


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
            self._set_session_time_zone()
            self._enable_session_read_only()
        except BaseException:
            self.close()
            raise

    def _set_session_time_zone(self) -> None:
        """固定 TIMESTAMP 的会话解释时区；这是连接初始化的内部 SQL。"""
        assert self._conn is not None
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(_SET_SESSION_TIME_ZONE_SQL)
        except pymysql.Error as exc:
            raise SourceMySQLError(f"failed to set UTC session time zone: {exc}") from exc

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
        只用于小结果集 (ping / information_schema), 不用于业务表数据。
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

    def get_create_table(self, table_name: str) -> str:
        """获取指定表的 CREATE TABLE DDL (专用安全 API)。

        内部只构造固定模板:

            SHOW CREATE TABLE `安全引用后的表名`

        这是获取 DDL 的唯一入口, 不是通用 SQL 执行接口;
        通用入口 fetch_all 依旧拒绝一切非 SELECT 语句。
        """
        sql = f"SHOW CREATE TABLE {quote_identifier(table_name)}"
        if self._conn is None:
            raise SourceMySQLError("connection is not open; call connect() first")
        try:
            with self._conn.cursor() as cursor:
                cursor.execute(sql)
                row = cursor.fetchone()
        except pymysql.Error as exc:
            raise SourceMySQLError(
                f"SHOW CREATE TABLE failed for table '{table_name}': {exc}"
            ) from exc
        if row is None:
            raise SourceMySQLError(f"table '{table_name}' does not exist")
        # SHOW CREATE TABLE 对 BASE TABLE 返回 (表名, DDL)
        return str(row[1])

    def stream_table(self, table_name: str, fetch_size: int) -> TableStream:
        """流式扫描一张完整表的专用安全 API。

        内部只构造固定模板:

            SELECT * FROM `安全引用后的表名`

        使用 SSCursor (server-side / unbuffered cursor), MySQL 逐行推送,
        客户端 fetchmany 分批读取。不支持 WHERE / ORDER BY / JOIN /
        用户 SQL —— V1 只同步完整物理表, 不加 ORDER BY (见技术设计)。
        """
        sql = f"SELECT * FROM {quote_identifier(table_name)}"
        _ensure_select_only(sql)
        if self._conn is None:
            raise SourceMySQLError("connection is not open; call connect() first")
        try:
            cursor = self._conn.cursor(pymysql.cursors.SSCursor)
        except pymysql.Error as exc:
            raise SourceMySQLError(f"cannot open streaming cursor: {exc}") from exc
        try:
            cursor.execute(sql)
        except pymysql.Error as exc:
            cursor.close()
            raise SourceMySQLError(f"cannot start table scan for '{table_name}': {exc}") from exc
        columns = [str(desc[0]) for desc in cursor.description or ()]
        return TableStream(connection=self, cursor=cursor, columns=columns, fetch_size=fetch_size)


class TableStream:
    """单表流式扫描句柄。

    用法:

        with connection.stream_table(table, fetch_size) as stream:
            columns = stream.columns
            for batch in stream:      # 每批 list[tuple], 最多 fetch_size 行
                ...

    一张表一个 SELECT: Snapshot 的全部数据来自同一个查询,
    内存占用只与单批行数相关。

    迭代中途出错时 (网络断开 / 查询被杀), server-side cursor 的
    协议状态可能已不可恢复, __exit__ 会直接关闭底层连接;
    调用方按 Run 失败处理即可。
    """

    def __init__(
        self,
        connection: SourceMySQLConnection,
        cursor: Any,
        columns: list[str],
        fetch_size: int,
    ) -> None:
        self._connection = connection
        self._cursor = cursor
        self.columns = columns
        self._fetch_size = fetch_size
        self._failed = False

    def __enter__(self) -> TableStream:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        if exc_type is not None or self._failed:
            # 流出错: 连接协议状态不可信, 整个连接作废
            self._connection.close()
            return
        self._cursor.close()

    def __iter__(self) -> Iterator[list[tuple[Any, ...]]]:
        while True:
            try:
                batch = self._cursor.fetchmany(self._fetch_size)
            except pymysql.Error as exc:
                self._failed = True
                raise SourceMySQLError(f"table scan failed: {exc}") from exc
            if not batch:
                return
            yield [tuple(row) for row in batch]


class QueryExecutor(Protocol):
    """能执行只读查询的连接 (SourceMySQLConnection 或测试替身)。"""

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ...) -> list[tuple[Any, ...]]: ...


@dataclass(frozen=True)
class TableInfo:
    """information_schema 中的表类型信息。"""

    table_type: str  # 例如 'BASE TABLE' / 'VIEW' / 'SYSTEM VIEW'


@dataclass(frozen=True)
class TableCheckResult:
    """单张表的元数据检查结果。"""

    table: TableConfig
    ok: bool
    error_code: str | None = None
    table_type: str | None = None


BASE_TABLE_TYPE = "BASE TABLE"
TABLE_NOT_FOUND = "TABLE_NOT_FOUND"
UNSUPPORTED_TABLE_TYPE = "UNSUPPORTED_TABLE_TYPE"


def fetch_table_info(executor: QueryExecutor, database: str, table: str) -> TableInfo | None:
    """返回指定表的类型信息; 表不存在时返回 None。"""
    rows = executor.fetch_all(
        "SELECT TABLE_TYPE FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (database, table),
    )
    if not rows:
        return None
    return TableInfo(table_type=str(rows[0][0]))


def check_table(table: TableConfig, info: TableInfo | None) -> TableCheckResult:
    """检查单张表: 表是否存在; 是否为 BASE TABLE。

    V1 只同步真实表; 配置了 VIEW 等对象时明确失败
    (UNSUPPORTED_TABLE_TYPE), 不静默处理。
    """
    if info is None:
        return TableCheckResult(table=table, ok=False, error_code=TABLE_NOT_FOUND)
    if info.table_type.upper() != BASE_TABLE_TYPE:
        return TableCheckResult(
            table=table,
            ok=False,
            error_code=UNSUPPORTED_TABLE_TYPE,
            table_type=info.table_type,
        )
    return TableCheckResult(table=table, ok=True, table_type=info.table_type)


def check_tables(
    executor: QueryExecutor, database: str, tables: list[TableConfig]
) -> list[TableCheckResult]:
    """依次检查一组同步表。"""
    return [
        check_table(table, fetch_table_info(executor, database, table.name)) for table in tables
    ]
