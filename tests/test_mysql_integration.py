"""需要真实 MySQL 的集成测试。

默认被跳过 (pyproject addopts: -m 'not integration')。
运行示例:

    export AIRGAP_TEST_MYSQL_HOST=127.0.0.1
    export AIRGAP_TEST_MYSQL_PORT=3306
    export AIRGAP_TEST_MYSQL_DATABASE=airgap_sync_it   # 一次性测试库, 会被写入
    export AIRGAP_TEST_MYSQL_USER=root
    export AIRGAP_TEST_MYSQL_PASSWORD=...
    uv run pytest -m integration

测试边界:
- 测试表的 CREATE / INSERT / DROP 全部由独立的 admin/setup 连接
  (直接使用 PyMySQL, 只存在于本测试文件) 完成;
- SourceMySQLConnection 只负责读取和检查, 与生产代码行为完全一致,
  不承担测试 fixture 的 DDL 职责。

注意: admin 连接会在该库中创建并删除临时表, 请使用可丢弃的数据库。
"""

from __future__ import annotations

import os

import pymysql
import pytest

from airgap_sync.common.models import MySQLConfig, TableConfig, TableMode
from airgap_sync.source.mysql import (
    SourceMySQLConnection,
    SourceMySQLError,
    check_tables,
    fetch_table_columns,
)

# MySQL 5.7 起 READ ONLY Session 中执行修改语句的错误码。
ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION = 1792

DEMO_TABLE = "airgap_it_demo"

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("AIRGAP_TEST_MYSQL_HOST")
        or not os.environ.get("AIRGAP_TEST_MYSQL_PASSWORD"),
        reason="AIRGAP_TEST_MYSQL_HOST / AIRGAP_TEST_MYSQL_PASSWORD not set",
    ),
]


def admin_exec(admin: pymysql.Connection, sql: str) -> None:
    """在 admin 连接上执行固定 DDL/DML (SQL 均为本文件内的常量)。"""
    with admin.cursor() as cursor:
        cursor.execute(sql)


@pytest.fixture
def mysql_config(monkeypatch: pytest.MonkeyPatch) -> MySQLConfig:
    monkeypatch.setenv("AIRGAP_TEST_MYSQL_PASSWORD", os.environ["AIRGAP_TEST_MYSQL_PASSWORD"])
    return MySQLConfig.model_validate(
        {
            "host": os.environ["AIRGAP_TEST_MYSQL_HOST"],
            "port": int(os.environ.get("AIRGAP_TEST_MYSQL_PORT", "3306")),
            "database": os.environ["AIRGAP_TEST_MYSQL_DATABASE"],
            "user": os.environ["AIRGAP_TEST_MYSQL_USER"],
            "password_env": "AIRGAP_TEST_MYSQL_PASSWORD",
        }
    )


@pytest.fixture
def admin_connection():
    """集成测试专用的管理连接 (直接 PyMySQL), 不进入生产代码。

    负责 CREATE / INSERT / DROP 测试表; SourceMySQLConnection 永远不做 DDL。
    """
    conn = pymysql.connect(
        host=os.environ["AIRGAP_TEST_MYSQL_HOST"],
        port=int(os.environ.get("AIRGAP_TEST_MYSQL_PORT", "3306")),
        user=os.environ["AIRGAP_TEST_MYSQL_USER"],
        password=os.environ["AIRGAP_TEST_MYSQL_PASSWORD"],
        database=os.environ["AIRGAP_TEST_MYSQL_DATABASE"],
        connect_timeout=10,
        charset="utf8mb4",
        autocommit=True,
    )
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def connection(mysql_config: MySQLConfig):
    with SourceMySQLConnection(mysql_config) as conn:
        yield conn


@pytest.fixture
def demo_table(admin_connection: pymysql.Connection) -> str:
    """通过 admin 连接创建/清理临时测试表, 返回表名。"""
    admin_exec(admin_connection, f"DROP TABLE IF EXISTS {DEMO_TABLE}")
    admin_exec(
        admin_connection,
        f"CREATE TABLE {DEMO_TABLE} (id BIGINT PRIMARY KEY, name VARCHAR(32), fyrq DATE)",
    )
    admin_exec(
        admin_connection,
        f"INSERT INTO {DEMO_TABLE} (id, name, fyrq) VALUES (1, 'demo', '2026-01-01'),"
        " (2, 'demo2', NULL)",
    )
    try:
        yield DEMO_TABLE
    finally:
        admin_exec(admin_connection, f"DROP TABLE IF EXISTS {DEMO_TABLE}")


def test_ping_returns_version(connection: SourceMySQLConnection):
    version = connection.ping()
    assert version  # 非空即视为连通, MySQL 5.7 也应返回版本串


def test_metadata_checks(connection: SourceMySQLConnection, mysql_config, demo_table: str):
    columns = fetch_table_columns(connection, mysql_config.database, demo_table)
    assert columns == {"id", "name", "fyrq"}
    assert fetch_table_columns(connection, mysql_config.database, "airgap_no_such") is None

    results = check_tables(
        connection,
        mysql_config.database,
        [
            TableConfig(name=demo_table, mode=TableMode.KEYED, key=["id", "name"]),
            TableConfig(name=demo_table, mode=TableMode.KEYED, key=["id", "ghost"]),
            TableConfig(name=demo_table, mode=TableMode.ROW_MULTISET),
        ],
    )
    assert [r.ok for r in results] == [True, False, True]
    assert results[1].missing_key_columns == ("ghost",)


def test_source_can_read_table_rows(connection: SourceMySQLConnection, demo_table: str):
    rows = connection.fetch_all(f"SELECT id, name FROM {demo_table} ORDER BY id")
    assert rows == [(1, "demo"), (2, "demo2")]


@pytest.mark.parametrize(
    "sql",
    [
        f"UPDATE {DEMO_TABLE} SET name = 'hacked' WHERE id = 1",
        f"DELETE FROM {DEMO_TABLE} WHERE id = 1",
        f"INSERT INTO {DEMO_TABLE} (id, name) VALUES (99, 'hacked')",
        f"DROP TABLE {DEMO_TABLE}",
    ],
)
def test_source_rejects_non_select_before_sending(
    connection: SourceMySQLConnection, demo_table: str, sql: str
):
    """应用层只读保护: 修改语句在发送给 MySQL 之前就被拒绝。"""
    with pytest.raises(SourceMySQLError, match="non-read-only SQL"):
        connection.fetch_all(sql)


def test_source_session_is_read_only(
    connection: SourceMySQLConnection, admin_connection: pymysql.Connection, demo_table: str
):
    """服务端只读保护: Source 连接的 MySQL Session 确实是 READ ONLY。

    应用层 fetch_all 已拒绝非 SELECT, 因此这里通过查询
    @@session.transaction_read_only 确认设置, 并用底层连接 (仅测试白盒)
    直接验证 MySQL 本身也会拒绝写语句 (错误 1792)。
    """
    # 1. Session 只读标志已生效
    assert connection.fetch_all("SELECT @@session.transaction_read_only") == [(1,)]

    # 2. MySQL 拒绝 DML (绕过应用层守卫, 直接在底层连接上尝试;
    #    生产代码不暴露任何类似入口)
    with (
        pytest.raises(pymysql.err.OperationalError) as excinfo,
        connection._conn.cursor() as cursor,  # 测试专用白盒访问
    ):
        cursor.execute(f"UPDATE {demo_table} SET name = 'hacked' WHERE id = 1")
    assert excinfo.value.args[0] == ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION

    # 3. MySQL 拒绝 DDL
    with (
        pytest.raises(pymysql.err.OperationalError) as excinfo,
        connection._conn.cursor() as cursor,  # 测试专用白盒访问
    ):
        cursor.execute("CREATE TABLE airgap_it_should_fail (id INT)")
    assert excinfo.value.args[0] == ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION

    # 4. admin 视角确认: 数据未被修改, 表也未被创建
    with admin_connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {demo_table} WHERE name = 'hacked'")
        assert cursor.fetchone()[0] == 0
        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'airgap_it_should_fail'"
        )
        assert cursor.fetchone()[0] == 0
