"""SourceMySQLConnection 双层只读保护的单元测试 (不需要真实 MySQL)。

- 应用层: fetch_all 只允许 SELECT, 非 SELECT SQL 在发送给 MySQL 之前被拒绝;
- 服务端初始化: connect() 必须先执行 SET SESSION TRANSACTION READ ONLY
  并验证生效, 失败时连接初始化失败且连接被关闭。

通过替身 PyMySQL 连接验证 "发送前拒绝": 被拒绝的 SQL 不会出现在
替身记录的执行列表中。
"""

from __future__ import annotations

from typing import Any

import pymysql
import pytest

from airgap_sync.common.config import ConfigError
from airgap_sync.common.models import MySQLConfig
from airgap_sync.source.mysql import SourceMySQLConnection, SourceMySQLError

REJECTED_SQL = [
    "DROP TABLE some_table",
    "DROP DATABASE sgaj_data",
    "INSERT INTO t (id) VALUES (1)",
    "UPDATE t SET name = 'x'",
    "DELETE FROM t",
    "REPLACE INTO t (id) VALUES (1)",
    "CREATE TABLE t (id INT)",
    "ALTER TABLE t ADD COLUMN c INT",
    "TRUNCATE TABLE t",
    "RENAME TABLE t TO t2",
    "GRANT ALL ON *.* TO someone",
    "REVOKE ALL ON *.* FROM someone",
    "SET GLOBAL read_only = 1",
    "SET SESSION TRANSACTION READ WRITE",
    "CALL some_procedure()",
    "LOAD DATA INFILE 'x' INTO TABLE t",
    "LOCK TABLES t WRITE",
    "START TRANSACTION",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SHOW TABLES",
    "EXPLAIN SELECT 1",
    "DESC t",
    "selection is not select",
    "selecting data from somewhere",
]


class FakeCursor:
    """替身游标: 记录 execute, 返回预设行。"""

    def __init__(self, connection: FakePyMySQLConnection):
        self._connection = connection

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        if self._connection.fail_on_sql == sql:
            raise pymysql.err.OperationalError(
                1792, "Cannot execute statement in a read-only transaction"
            )
        self._connection.executed.append((sql, params))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._connection.next_rows


class FakePyMySQLConnection:
    """替身 PyMySQL 连接: 不接触网络, 记录执行过的 SQL。"""

    def __init__(
        self,
        next_rows: list[tuple[Any, ...]] | None = None,
        fail_on_sql: str | None = None,
    ) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.next_rows = next_rows or []
        self.fail_on_sql = fail_on_sql
        self.closed = False
        self.connect_kwargs: dict[str, Any] | None = None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

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


@pytest.fixture
def patched_connect(monkeypatch: pytest.MonkeyPatch):
    """把 pymysql.connect 替换为返回 FakePyMySQLConnection, 记录连接参数。"""

    def _patch(fake: FakePyMySQLConnection) -> None:
        def fake_connect(**kwargs: Any) -> FakePyMySQLConnection:
            fake.connect_kwargs = kwargs
            return fake

        monkeypatch.setattr("airgap_sync.source.mysql.pymysql.connect", fake_connect)

    return _patch


class TestFetchAllSelectOnly:
    @pytest.mark.parametrize("sql", REJECTED_SQL)
    def test_non_select_rejected_before_sending(self, mysql_config, sql):
        """非 SELECT SQL 必须在发送给 MySQL 之前被拒绝。"""
        connection = SourceMySQLConnection(mysql_config)
        fake = FakePyMySQLConnection()
        connection._conn = fake  # 模拟已连接, 验证 SQL 根本不会被发送

        with pytest.raises(SourceMySQLError, match="non-read-only SQL"):
            connection.fetch_all(sql)

        assert fake.executed == []  # 没有任何语句被发送

    def test_empty_sql_rejected(self, mysql_config):
        connection = SourceMySQLConnection(mysql_config)
        with pytest.raises(SourceMySQLError, match="non-read-only SQL"):
            connection.fetch_all("   ")

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1, VERSION()",
            "select 1",
            "SELECT @@session.transaction_read_only",
            "\n  SELECT COLUMN_NAME FROM information_schema.COLUMNS",
            "SELECT * FROM t WHERE id = %s",
        ],
    )
    def test_select_allowed(self, mysql_config, sql):
        connection = SourceMySQLConnection(mysql_config)
        fake = FakePyMySQLConnection(next_rows=[(1,)])
        connection._conn = fake

        connection.fetch_all(sql, (1,) if "%s" in sql else ())

        assert fake.executed == [(sql, (1,) if "%s" in sql else ())]

    def test_rejection_does_not_require_open_connection(self, mysql_config):
        """即使连接尚未建立, 拒绝也发生在 'connection is not open' 检查之前。"""
        connection = SourceMySQLConnection(mysql_config)
        with pytest.raises(SourceMySQLError, match="non-read-only SQL"):
            connection.fetch_all("DROP TABLE some_table")

    def test_select_on_closed_connection_reports_not_open(self, mysql_config):
        """合法 SELECT 在未连接时仍报告未打开 (而不是被只读检查掩盖)。"""
        connection = SourceMySQLConnection(mysql_config)
        with pytest.raises(SourceMySQLError, match="connection is not open"):
            connection.fetch_all("SELECT 1")

    def test_error_message_does_not_echo_sql_body(self, mysql_config):
        """错误信息只报告关键字, 不回显 SQL 内容 (避免泄漏语句中的字面量)。"""
        connection = SourceMySQLConnection(mysql_config)
        connection._conn = FakePyMySQLConnection()
        with pytest.raises(SourceMySQLError) as excinfo:
            connection.fetch_all("UPDATE t SET password = 'secret-value'")
        assert "secret-value" not in str(excinfo.value)
        assert "UPDATE" in str(excinfo.value)


class TestConnectEnablesSessionReadOnly:
    def test_connect_sets_session_read_only_and_verifies(self, mysql_config, patched_connect):
        fake = FakePyMySQLConnection(next_rows=[(1,)])
        patched_connect(fake)

        connection = SourceMySQLConnection(mysql_config)
        try:
            connection.connect()
        finally:
            connection.close()

        assert fake.connect_kwargs is not None
        assert fake.connect_kwargs["autocommit"] is True  # 不隐式持有长事务
        assert fake.executed[0] == ("SET SESSION time_zone = '+00:00'", ())
        assert fake.executed[1] == ("SET SESSION TRANSACTION READ ONLY", ())
        assert ("SELECT @@session.transaction_read_only", ()) in fake.executed
        assert fake.closed

    def test_connect_fails_when_set_read_only_fails(self, mysql_config, patched_connect):
        fake = FakePyMySQLConnection(
            next_rows=[(1,)], fail_on_sql="SET SESSION TRANSACTION READ ONLY"
        )
        patched_connect(fake)

        connection = SourceMySQLConnection(mysql_config)
        with pytest.raises(SourceMySQLError, match="failed to set READ ONLY session"):
            connection.connect()

        assert fake.closed  # 初始化失败的连接必须被关闭
        assert fake.executed == [("SET SESSION time_zone = '+00:00'", ())]

    def test_connect_fails_when_session_not_read_only(self, mysql_config, patched_connect):
        """验证查询返回 0 (未生效) 时必须失败, 不允许静默降级为可写连接。"""
        fake = FakePyMySQLConnection(next_rows=[(0,)])
        patched_connect(fake)

        connection = SourceMySQLConnection(mysql_config)
        with pytest.raises(SourceMySQLError, match="expected 1"):
            connection.connect()

        assert fake.closed
        assert fake.executed == [
            ("SET SESSION time_zone = '+00:00'", ()),
            ("SET SESSION TRANSACTION READ ONLY", ()),
            ("SELECT @@session.transaction_read_only", ()),
        ]

    def test_missing_password_fails_before_connect(
        self, mysql_config, patched_connect, monkeypatch
    ):
        monkeypatch.delenv("AIRGAP_TEST_PASSWORD", raising=False)
        fake = FakePyMySQLConnection()
        patched_connect(fake)

        connection = SourceMySQLConnection(mysql_config)
        with pytest.raises(ConfigError):
            connection.connect()
        assert fake.connect_kwargs is None  # 没有建立任何连接
