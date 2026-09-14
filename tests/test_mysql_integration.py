"""需要真实 MySQL 的集成测试。

默认被跳过 (pyproject addopts: -m 'not integration')。
运行示例:

    export AIRGAP_TEST_MYSQL_HOST=127.0.0.1
    export AIRGAP_TEST_MYSQL_PORT=3306
    export AIRGAP_TEST_MYSQL_DATABASE=airgap_sync_it   # 一次性测试库, 会被写入
    export AIRGAP_TEST_MYSQL_USER=root
    export AIRGAP_TEST_MYSQL_PASSWORD=...
    uv run pytest -m integration

注意: 测试会在该库中创建并删除临时表, 请使用可丢弃的数据库。
"""

from __future__ import annotations

import os

import pytest

from airgap_sync.common.models import MySQLConfig, TableConfig, TableMode
from airgap_sync.source.mysql import (
    SourceMySQLConnection,
    check_tables,
    fetch_table_columns,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("AIRGAP_TEST_MYSQL_HOST"),
        reason="AIRGAP_TEST_MYSQL_HOST not set",
    ),
]


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
def connection(mysql_config: MySQLConfig):
    with SourceMySQLConnection(mysql_config) as conn:
        yield conn


def test_ping_returns_version(connection: SourceMySQLConnection):
    version = connection.ping()
    assert version  # 非空即视为连通, MySQL 5.7 也应返回版本串


def test_fetch_table_columns(connection: SourceMySQLConnection, mysql_config):
    ddl = "CREATE TABLE airgap_it_demo (id BIGINT PRIMARY KEY, name VARCHAR(32), fyrq DATE)"
    connection.fetch_all("DROP TABLE IF EXISTS airgap_it_demo")
    try:
        connection.fetch_all(ddl)
        columns = fetch_table_columns(connection, mysql_config.database, "airgap_it_demo")
        assert columns == {"id", "name", "fyrq"}
        assert fetch_table_columns(connection, mysql_config.database, "airgap_no_such") is None

        results = check_tables(
            connection,
            mysql_config.database,
            [
                TableConfig(name="airgap_it_demo", mode=TableMode.KEYED, key=["id", "name"]),
                TableConfig(name="airgap_it_demo", mode=TableMode.KEYED, key=["id", "ghost"]),
                TableConfig(name="airgap_it_demo", mode=TableMode.ROW_MULTISET),
            ],
        )
        assert [r.ok for r in results] == [True, False, True]
        assert results[1].missing_key_columns == ("ghost",)
    finally:
        connection.fetch_all("DROP TABLE IF EXISTS airgap_it_demo")
