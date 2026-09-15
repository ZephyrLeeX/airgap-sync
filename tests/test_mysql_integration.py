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
- SourceMySQLConnection 只负责读取、检查和 Snapshot 生成,
  与生产代码行为完全一致, 不承担测试 fixture 的 DDL 职责。

注意: admin 连接会在该库中创建并删除临时表, 请使用可丢弃的数据库。
"""

from __future__ import annotations

import os
import shutil
from datetime import datetime
from pathlib import Path

import pymysql
import pytest

from airgap_sync.common.manifest import read_manifest
from airgap_sync.common.models import AppConfig, MySQLConfig, TableConfig
from airgap_sync.common.row_codec import decode_row, encode_row
from airgap_sync.common.transport import transport_filename
from airgap_sync.destination.mysql import DestinationMySQLConnection
from airgap_sync.destination.processor import DestinationProcessor
from airgap_sync.source.mysql import (
    SourceMySQLConnection,
    SourceMySQLError,
    check_tables,
    fetch_table_info,
)
from airgap_sync.source.snapshot import SnapshotRunner
from airgap_sync.source.state import SourceState, state_db_path

# MySQL 5.7 起 READ ONLY Session 中执行修改语句的错误码。
ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION = 1792

DEMO_TABLE = "airgap_it_demo"
SNAP_TABLE = "airgap_it_snap"
SNAP_VIEW = "airgap_it_snap_view"

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
    with conn.cursor() as cursor:
        cursor.execute("SET SESSION time_zone = '+00:00'")
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


@pytest.fixture
def snapshot_table(admin_connection: pymysql.Connection) -> str:
    """无 PRIMARY KEY 的快照测试表: 重复行 / NULL / Decimal / datetime / text / binary。"""
    admin_exec(admin_connection, f"DROP VIEW IF EXISTS {SNAP_VIEW}")
    admin_exec(admin_connection, f"DROP TABLE IF EXISTS {SNAP_TABLE}")
    admin_exec(
        admin_connection,
        f"CREATE TABLE {SNAP_TABLE} ("
        "  sid INT DEFAULT NULL,"
        "  name VARCHAR(64) DEFAULT NULL,"
        "  amount DECIMAL(12,4) DEFAULT NULL,"
        "  ts DATETIME(6) DEFAULT NULL,"
        "  event_ts TIMESTAMP NULL DEFAULT NULL,"
        "  event_ts6 TIMESTAMP(6) NULL DEFAULT NULL,"
        "  note TEXT,"
        "  payload VARBINARY(32) DEFAULT NULL"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    )
    admin_exec(
        admin_connection,
        f"INSERT INTO {SNAP_TABLE} "
        "(sid,name,amount,ts,event_ts,event_ts6,note,payload) VALUES"
        " (1,'a',12.3400,'2026-09-14 20:00:00.123456','2026-09-14 20:00:00',"
        "'2026-09-14 20:00:00.123456','first',0x0001FF),"
        " (1,'a',12.3400,'2026-09-14 20:00:00.123456','2026-09-14 20:00:00',"
        "'2026-09-14 20:00:00.123456','first',0x0001FF),"  # 完全重复
        " (2,NULL,NULL,NULL,NULL,NULL,NULL,NULL),"  # 全 NULL 行
        " (3,'中文长文本测试',-0.5000,'2026-01-01 00:00:00.000001',"
        "'2026-01-01 00:00:00','2026-01-01 00:00:00.000001','',0x00),"
        " (4,'',99999999.9999,'2026-12-31 23:59:59.999999',"
        "'2026-12-31 23:59:59','2026-12-31 23:59:59.999999',"
        "'中文身份证字段',0xDeadBeef)",
    )
    admin_exec(admin_connection, f"CREATE VIEW {SNAP_VIEW} AS SELECT sid, name FROM {SNAP_TABLE}")
    try:
        yield SNAP_TABLE
    finally:
        admin_exec(admin_connection, f"DROP VIEW IF EXISTS {SNAP_VIEW}")
        admin_exec(admin_connection, f"DROP TABLE IF EXISTS {SNAP_TABLE}")


def test_ping_returns_version(connection: SourceMySQLConnection):
    version = connection.ping()
    assert version  # 非空即视为连通, MySQL 5.7 也应返回版本串


def test_table_type_checks(connection: SourceMySQLConnection, mysql_config, snapshot_table: str):
    info = fetch_table_info(connection, mysql_config.database, snapshot_table)
    assert info is not None
    assert info.table_type.upper() == "BASE TABLE"

    view_info = fetch_table_info(connection, mysql_config.database, SNAP_VIEW)
    assert view_info is not None
    assert view_info.table_type.upper() == "VIEW"

    results = check_tables(
        connection,
        mysql_config.database,
        [
            TableConfig(name=snapshot_table),
            TableConfig(name=SNAP_VIEW),
            TableConfig(name="airgap_no_such"),
        ],
    )
    assert [r.ok for r in results] == [True, False, False]
    assert results[1].error_code == "UNSUPPORTED_TABLE_TYPE"
    assert results[2].error_code == "TABLE_NOT_FOUND"


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
        "SHOW TABLES",
    ],
)
def test_source_rejects_non_select_before_sending(
    connection: SourceMySQLConnection, demo_table: str, sql: str
):
    """应用层只读保护: 修改语句与非 SELECT 语句在发送给 MySQL 之前就被拒绝。"""
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


def test_get_create_table_returns_real_ddl(connection: SourceMySQLConnection, snapshot_table: str):
    ddl = connection.get_create_table(snapshot_table)
    assert ddl.startswith(f"CREATE TABLE `{snapshot_table}`")
    assert "`amount` decimal(12,4)" in ddl
    assert "ENGINE=InnoDB" in ddl


def test_snapshot_run_on_real_mysql(
    mysql_config: MySQLConfig,
    admin_connection: pymysql.Connection,
    snapshot_table: str,
    tmp_path: Path,
):
    """端到端: 只读连接对无主键表生成完整 Snapshot Run。

    故意使用: 无 PRIMARY KEY、重复行、NULL、Decimal、datetime(6)、
    text、binary; 小 fetch_size / 小 chunk 阈值强制多批多 Chunk。
    """
    import zstandard

    config = AppConfig.model_validate(
        {
            "role": "source",
            "mysql": mysql_config.model_dump(),
            "paths": {"data_dir": str(tmp_path / "data")},
            "snapshot": {"fetch_size": 2},
            "chunk": {"max_rows": 2, "max_uncompressed_bytes": 4096, "compression_level": 3},
            "tables": [{"name": snapshot_table, "enabled": True}],
        }
    )

    with SourceMySQLConnection(mysql_config) as conn:
        state = SourceState(state_db_path(config.paths.data_dir))
        try:
            state.initialize()
            result = SnapshotRunner(conn, state, config).snapshot(snapshot_table)
        finally:
            state.close()

    assert result.status == "COMPLETED", result.error
    assert result.row_count == 5
    assert result.chunk_count == 3  # max_rows=2 → 2+2+1

    # DDL 成功携带
    schema_text = (result.run_dir / "schema.sql").read_text(encoding="utf-8")
    assert schema_text.startswith(f"CREATE TABLE `{snapshot_table}`")

    # Manifest 完整且与文件一致
    manifest = read_manifest(result.run_dir / "manifest.json")
    assert manifest.row_count == 5
    assert [c.sequence for c in manifest.chunks] == [1, 2, 3]
    assert manifest.columns == [
        "sid",
        "name",
        "amount",
        "ts",
        "event_ts",
        "event_ts6",
        "note",
        "payload",
    ]

    # 解压解码全部行, 与 admin 直读的行 multiset 比较 (顺序无关)
    dctx = zstandard.ZstdDecompressor()
    recovered: list[tuple] = []
    for chunk in manifest.chunks:
        with open(result.run_dir / chunk.file, "rb") as fh:
            for line in dctx.stream_reader(fh).read().splitlines():
                recovered.append(tuple(decode_row(line)))
    assert len(recovered) == 5

    with admin_connection.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {SNAP_TABLE}")
        expected = [tuple(row) for row in cursor.fetchall()]
    assert sorted(recovered, key=lambda r: encode_row(r)) == sorted(
        expected, key=lambda r: encode_row(r)
    )
    # 重复行没有丢失: (1, 'a', ...) 出现两次
    duplicates = [row for row in recovered if row[0] == 1]
    assert len(duplicates) == 2

    # binary / Decimal / datetime 无损
    binary_row = next(row for row in recovered if row[7] == b"\x00\x01\xff")
    assert binary_row[2] is not None  # Decimal
    assert binary_row[4] == datetime(2026, 9, 14, 20, 0)
    assert binary_row[5] == datetime(2026, 9, 14, 20, 0, 0, 123456)
    decimal_row = next(row for row in recovered if row[0] == 3)
    from decimal import Decimal

    assert decimal_row[2] == Decimal("-0.5000")

    # 状态: current_run_id 已推进
    state = SourceState(state_db_path(config.paths.data_dir))
    try:
        row = state.get_table_state(snapshot_table)
        assert row is not None
        assert row.status == "COMPLETED"
        assert row.current_run_id == result.run_id
    finally:
        state.close()


def test_destination_verify_and_multi_table_promotion_on_real_mysql(
    mysql_config: MySQLConfig,
    admin_connection: pymysql.Connection,
    snapshot_table: str,
    tmp_path: Path,
):
    """完整真实路径：多 Chunk staging、DB 回读 digest、已有 live 原子替换。"""
    source_config = AppConfig.model_validate(
        {
            "role": "source",
            "mysql": mysql_config.model_dump(),
            "paths": {"data_dir": str(tmp_path / "source")},
            "snapshot": {"fetch_size": 2},
            "chunk": {"max_rows": 2, "max_uncompressed_bytes": 4096},
            "tables": [{"name": snapshot_table}],
        }
    )
    with SourceMySQLConnection(mysql_config) as source:
        state = SourceState(state_db_path(source_config.paths.data_dir))
        try:
            state.initialize()
            snapshot = SnapshotRunner(source, state, source_config).snapshot(snapshot_table)
        finally:
            state.close()
    assert snapshot.status == "COMPLETED"
    manifest = read_manifest(snapshot.run_dir / "manifest.json")

    incoming = tmp_path / "incoming"
    incoming.mkdir()
    logical_names = [manifest.schema_file.file, *(chunk.file for chunk in manifest.chunks)]
    for logical in logical_names:
        shutil.copyfile(
            snapshot.run_dir / logical,
            incoming / transport_filename(manifest.run_id, logical),
        )
    shutil.copyfile(
        snapshot.run_dir / "manifest.json",
        incoming / transport_filename(manifest.run_id, "manifest.json"),
    )
    destination_config = AppConfig.model_validate(
        {
            "role": "destination",
            "mysql": mysql_config.model_dump(),
            "destination": {"incoming_dir": str(incoming), "settle_seconds": 0},
        }
    )
    assert destination_config.destination is not None
    with DestinationMySQLConnection(mysql_config, destination_config.destination) as destination:
        destination.initialize_metadata()
        result = DestinationProcessor(destination, destination_config).process(manifest.run_id)

    assert result.status == "VERIFIED", result.error
    assert not list(incoming.iterdir())
    with admin_connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {SNAP_TABLE}")
        assert cursor.fetchone()[0] == 5
