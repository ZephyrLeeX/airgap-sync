"""MySQL 元数据检查、identifier quoting 与专用安全 API 测试 (不需要真实 MySQL)。

使用替身 PyMySQL 连接验证:
- 表类型检查只接受 BASE TABLE;
- get_create_table / stream_table 构造的 SQL 中表名总是被安全引用;
- 流式扫描按 fetchmany 分批消费。

真实数据库测试见 test_mysql_integration.py;
fetch_all 的只读守卫测试见 test_mysql_readonly.py。
"""

from __future__ import annotations

from typing import Any

import pymysql
import pymysql.cursors
import pytest

from airgap_sync.common.models import MySQLConfig, TableConfig
from airgap_sync.source.mysql import (
    SourceMySQLConnection,
    SourceMySQLError,
    check_table,
    check_tables,
    fetch_table_info,
    quote_identifier,
)

BASE = "BASE TABLE"


class FakeExecutor:
    """模拟 QueryExecutor: 只维护 表名 -> TABLE_TYPE。"""

    def __init__(self, tables: dict[str, str]):
        self.tables = tables
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        self.queries.append((sql, params))
        table_type = self.tables.get(params[1])
        if table_type is None:
            return []
        return [(table_type,)]


class TestFetchTableInfo:
    def test_returns_base_table(self):
        executor = FakeExecutor({"t_demo": BASE})
        info = fetch_table_info(executor, "sgaj_data", "t_demo")
        assert info is not None
        assert info.table_type == BASE

    def test_view_type_reported(self):
        executor = FakeExecutor({"v_demo": "VIEW"})
        info = fetch_table_info(executor, "sgaj_data", "v_demo")
        assert info is not None
        assert info.table_type == "VIEW"

    def test_missing_table_returns_none(self):
        executor = FakeExecutor({})
        assert fetch_table_info(executor, "sgaj_data", "t_demo") is None

    def test_uses_information_schema_with_parameters(self):
        executor = FakeExecutor({"t_demo": BASE})
        fetch_table_info(executor, "sgaj_data", "t_demo")
        sql, params = executor.queries[0]
        assert "information_schema.TABLES" in sql
        assert params == ("sgaj_data", "t_demo")  # 参数化查询, 不拼接表名


class TestCheckTable:
    def test_base_table_ok(self):
        table = TableConfig(name="t_demo", enabled=True)
        info = fetch_table_info(FakeExecutor({"t_demo": BASE}), "db", "t_demo")
        result = check_table(table, info)
        assert result.ok
        assert result.error_code is None

    def test_view_rejected(self):
        table = TableConfig(name="v_demo", enabled=True)
        info = fetch_table_info(FakeExecutor({"v_demo": "VIEW"}), "db", "v_demo")
        result = check_table(table, info)
        assert not result.ok
        assert result.error_code == "UNSUPPORTED_TABLE_TYPE"
        assert result.table_type == "VIEW"

    def test_system_view_rejected(self):
        table = TableConfig(name="sys_x", enabled=True)
        result = check_table(
            table, fetch_table_info(FakeExecutor({"sys_x": "SYSTEM VIEW"}), "db", "sys_x")
        )
        assert not result.ok
        assert result.error_code == "UNSUPPORTED_TABLE_TYPE"

    def test_table_not_found(self):
        table = TableConfig(name="ghost", enabled=True)
        result = check_table(table, None)
        assert not result.ok
        assert result.error_code == "TABLE_NOT_FOUND"

    def test_check_tables_iterates(self):
        executor = FakeExecutor({"t_a": BASE, "v_b": "VIEW"})
        tables = [TableConfig(name="t_a"), TableConfig(name="v_b"), TableConfig(name="ghost")]
        results = check_tables(executor, "db", tables)
        assert [r.ok for r in results] == [True, False, False]
        assert [r.error_code for r in results] == [
            None,
            "UNSUPPORTED_TABLE_TYPE",
            "TABLE_NOT_FOUND",
        ]


class TestQuoteIdentifier:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("normal_table", "`normal_table`"),
            ("table-name", "`table-name`"),
            ("table name with spaces", "`table name with spaces`"),
            ("t.name", "`t.name`"),
            ("中文表名", "`中文表名`"),
            ("has`tick", "`has``tick`"),
            ("a`b`c", "`a``b``c`"),
            ("'; DROP TABLE users; --", "`'; DROP TABLE users; --`"),
            ("_underscore123", "`_underscore123`"),
        ],
    )
    def test_quoting(self, name: str, expected: str):
        assert quote_identifier(name) == expected

    def test_injection_becomes_single_identifier(self):
        """任何表名输入都只会成为一个被引用的 identifier, 不能逃逸反引号。"""
        evil = "users`; DROP TABLE users; --"
        quoted = quote_identifier(evil)
        assert quoted == "`users``; DROP TABLE users; --`"
        # 内部反引号全部翻倍: 不存在未转义的反引号
        assert quoted.count("`") == 4

    def test_empty_identifier_rejected(self):
        with pytest.raises(SourceMySQLError, match="must not be empty"):
            quote_identifier("")


# ---------------------------------------------------------------------------
# 专用安全 API 的替身连接
# ---------------------------------------------------------------------------


class FakeRowCursor:
    """普通游标替身: 记录 execute, fetchone 返回预设行。"""

    def __init__(self, connection: FakePyMySQL) -> None:
        self._connection = connection

    def __enter__(self) -> FakeRowCursor:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        if self._connection.fail_on_sql == sql:
            raise pymysql.err.OperationalError(1046, "fake failure")
        self._connection.executed.append((sql, params))

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._connection.show_create_row


class FakeSSCursor:
    """SSCursor 替身: description + fetchmany 批次。"""

    def __init__(self, connection: FakePyMySQL) -> None:
        self._connection = connection
        self.description = self._connection.stream_description
        self.closed = False

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        if self._connection.fail_on_sql == sql:
            raise pymysql.err.OperationalError(1046, "fake failure")
        self._connection.executed.append((sql, params))

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        if self._connection.fail_on_fetchmany:
            raise pymysql.err.OperationalError(2013, "Lost connection to MySQL server")
        batches = self._connection.stream_batches
        return batches.pop(0) if batches else []

    def close(self) -> None:
        self.closed = True


class FakePyMySQL:
    """替身 PyMySQL 连接: 支持专用 API 需要的 cursor 形态。"""

    def __init__(
        self,
        show_create_row: tuple[Any, ...] | None = None,
        stream_description: list[tuple[Any, ...]] | None = None,
        stream_batches: list[list[tuple[Any, ...]]] | None = None,
        fail_on_sql: str | None = None,
        fail_on_fetchmany: bool = False,
    ) -> None:
        self.show_create_row = show_create_row
        self.stream_description = stream_description or []
        self.stream_batches = list(stream_batches or [])
        self.fail_on_sql = fail_on_sql
        self.fail_on_fetchmany = fail_on_fetchmany
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.closed = False
        self.ss_cursor: FakeSSCursor | None = None

    def cursor(self, cursor_class: type | None = None) -> Any:
        if cursor_class is pymysql.cursors.SSCursor:
            self.ss_cursor = FakeSSCursor(self)
            return self.ss_cursor
        return FakeRowCursor(self)

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def mysql_config(password_env: str) -> MySQLConfig:
    return MySQLConfig.model_validate(
        {
            "host": "127.0.0.1",
            "port": 3306,
            "database": "sgaj_data",
            "user": "sgaj_sync",
            "password_env": password_env,
        }
    )


def connected(config: MySQLConfig, fake: FakePyMySQL) -> SourceMySQLConnection:
    connection = SourceMySQLConnection(config)
    connection._conn = fake
    return connection


class TestGetCreateTable:
    def test_builds_safely_quoted_show_create_table(self, mysql_config):
        fake = FakePyMySQL(show_create_row=("t_demo", "CREATE TABLE `t_demo` (id INT)"))
        connection = connected(mysql_config, fake)
        ddl = connection.get_create_table("t_demo")
        assert ddl == "CREATE TABLE `t_demo` (id INT)"
        assert fake.executed == [("SHOW CREATE TABLE `t_demo`", ())]

    def test_backtick_in_table_name_is_escaped(self, mysql_config):
        fake = FakePyMySQL(show_create_row=("we`ird", "CREATE TABLE `we``ird` (id INT)"))
        connection = connected(mysql_config, fake)
        connection.get_create_table("we`ird")
        assert fake.executed == [("SHOW CREATE TABLE `we``ird`", ())]

    def test_query_failure_wrapped(self, mysql_config):
        fake = FakePyMySQL(show_create_row=("t", "x"))
        fake.fail_on_sql = "SHOW CREATE TABLE `t`"
        connection = connected(mysql_config, fake)
        with pytest.raises(SourceMySQLError, match="SHOW CREATE TABLE failed"):
            connection.get_create_table("t")

    def test_requires_open_connection(self, mysql_config):
        connection = SourceMySQLConnection(mysql_config)
        with pytest.raises(SourceMySQLError, match="connection is not open"):
            connection.get_create_table("t")


class TestStreamTable:
    def test_builds_safely_quoted_select_star(self, mysql_config):
        fake = FakePyMySQL(
            stream_description=[("id",), ("name",)],
            stream_batches=[[(1, "a")]],
        )
        connection = connected(mysql_config, fake)
        with connection.stream_table("t_demo", 100) as stream:
            assert stream.columns == ["id", "name"]
            batches = list(stream)
        assert batches == [[(1, "a")]]
        assert fake.executed == [("SELECT * FROM `t_demo`", ())]

    def test_weird_table_name_is_escaped(self, mysql_config):
        fake = FakePyMySQL(stream_description=[("id",)], stream_batches=[[]])
        connection = connected(mysql_config, fake)
        with connection.stream_table("we`ird", 10):
            pass
        assert fake.executed == [("SELECT * FROM `we``ird`", ())]

    def test_fetchmany_batches(self, mysql_config):
        fake = FakePyMySQL(
            stream_description=[("id",)],
            stream_batches=[[(1,), (2,)], [(3,)], []],
        )
        connection = connected(mysql_config, fake)
        with connection.stream_table("t", 2) as stream:
            assert list(stream) == [[(1,), (2,)], [(3,)]]

    def test_cursor_closed_after_full_consumption(self, mysql_config):
        fake = FakePyMySQL(stream_description=[("id",)], stream_batches=[[(1,)]])
        connection = connected(mysql_config, fake)
        with connection.stream_table("t", 1) as stream:
            list(stream)
        assert fake.ss_cursor is not None
        assert fake.ss_cursor.closed
        assert not fake.closed  # 连接仍可继续用 (例如第二次 DDL 检查)

    def test_fetchmany_error_closes_whole_connection(self, mysql_config):
        """流中途出错: 协议状态不可信, 连接整体作废。"""
        fake = FakePyMySQL(
            stream_description=[("id",)],
            stream_batches=[[(1,)]],
            fail_on_fetchmany=True,
        )
        connection = connected(mysql_config, fake)
        with (
            connection.stream_table("t", 1) as stream,
            pytest.raises(SourceMySQLError, match="table scan failed"),
        ):
            list(stream)
        assert fake.closed

    def test_execute_failure_raises(self, mysql_config):
        fake = FakePyMySQL(stream_description=[("id",)])
        fake.fail_on_sql = "SELECT * FROM `t`"
        connection = connected(mysql_config, fake)
        with pytest.raises(SourceMySQLError, match="cannot start table scan"):
            connection.stream_table("t", 10)

    def test_requires_open_connection(self, mysql_config):
        connection = SourceMySQLConnection(mysql_config)
        with pytest.raises(SourceMySQLError, match="connection is not open"):
            connection.stream_table("t", 10)
