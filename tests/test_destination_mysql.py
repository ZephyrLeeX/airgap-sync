from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

import pymysql.cursors
import pytest

from airgap_sync.common.models import DestinationConfig, MySQLConfig
from airgap_sync.destination.mysql import (
    DestinationMySQLConnection,
    DestinationMySQLError,
    RunRecord,
    TableVersion,
)


def configs(tmp_path):
    mysql = MySQLConfig(
        host="127.0.0.1", database="target_db", user="sync", password_env="DEST_PASSWORD"
    )
    destination = DestinationConfig(incoming_dir=tmp_path)
    return mysql, destination


class RecordingConnection(DestinationMySQLConnection):
    def __init__(self, mysql, destination):
        super().__init__(mysql, destination)
        self.sql = []

    def _execute(self, sql, params=()):
        self.sql.append((sql, params))
        return 1


def test_existing_target_promotion_is_one_multi_table_rename(tmp_path):
    connection = RecordingConnection(*configs(tmp_path))
    connection.rename_for_promotion("target_db", "staging", "live", "backup")
    assert connection.sql == [
        (
            "RENAME TABLE `target_db`.`live` TO `target_db`.`backup`, "
            "`target_db`.`staging` TO `target_db`.`live`",
            (),
        )
    ]


def test_missing_target_promotion_is_one_rename(tmp_path):
    connection = RecordingConnection(*configs(tmp_path))
    connection.rename_for_promotion("target_db", "staging", "live", None)
    assert connection.sql[0][0] == ("RENAME TABLE `target_db`.`staging` TO `target_db`.`live`")


class FinalizeConnection(RecordingConnection):
    def __init__(self, mysql, destination, run, latest):
        super().__init__(mysql, destination)
        self.run = run
        self.latest = latest

    @contextmanager
    def transaction(self):
        yield

    def get_run(self, run_id):
        return self.run

    def latest_version(self, source_database, table_name):
        return self.latest


def test_finalize_rejects_run_older_than_transaction_latest(tmp_path):
    run_at = datetime(2026, 9, 1)
    latest_at = datetime(2026, 9, 8, tzinfo=UTC)
    run = RunRecord("run-a", "source", "table", "SWAPPING", 5, 1, "staging", run_at)
    latest = TableVersion(
        "run-b", "source", "table", latest_at, 7, None, None, None, latest_at, latest_at
    )
    connection = FinalizeConnection(*configs(tmp_path), run, latest)

    with pytest.raises(DestinationMySQLError, match="run is older"):
        connection.finalize_verified("run-a", "run-b")

    assert connection.sql == []


def test_finalize_is_idempotent_when_current_run_is_already_latest(tmp_path):
    run_at = datetime(2026, 9, 1)
    version_at = run_at.replace(tzinfo=UTC)
    run = RunRecord("run-a", "source", "table", "SWAPPING", 5, 1, "staging", run_at)
    latest = TableVersion(
        "run-a", "source", "table", version_at, 5, None, None, None, version_at, version_at
    )
    connection = FinalizeConnection(*configs(tmp_path), run, latest)

    connection.finalize_verified("run-a", "run-a")

    assert len(connection.sql) == 1
    assert "UPDATE `airgap_sync_meta`.runs SET status='VERIFIED'" in connection.sql[0][0]


class StreamCursor:
    def __init__(self):
        self.sql = None
        self.fetch_sizes = []
        self.batches = [[(2, "b"), (1, "a")], [(1, "a")], []]
        self.closed = False

    def execute(self, sql):
        self.sql = sql

    def fetchmany(self, size):
        self.fetch_sizes.append(size)
        return self.batches.pop(0)

    def close(self):
        self.closed = True


class StreamConnection:
    def __init__(self, cursor):
        self.stream_cursor = cursor
        self.cursor_class = None

    def cursor(self, cursor_class=None):
        self.cursor_class = cursor_class
        return self.stream_cursor


def test_stream_verify_uses_sscursor_fetchmany_explicit_columns_without_order(tmp_path):
    connection = DestinationMySQLConnection(*configs(tmp_path))
    cursor = StreamCursor()
    raw = StreamConnection(cursor)
    connection._conn = raw
    with connection.stream_table_columns("staging", ["id", "name"], 2) as rows:
        assert list(rows) == [(2, "b"), (1, "a"), (1, "a")]
    assert raw.cursor_class is pymysql.cursors.SSCursor
    assert cursor.sql == "SELECT `id`,`name` FROM `staging`"
    assert "ORDER BY" not in cursor.sql
    assert cursor.fetch_sizes == [2, 2, 2]
    assert cursor.closed


class InitCursor:
    def __init__(self, owner):
        self.owner = owner

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def execute(self, sql, params=()):
        self.owner.executed.append((sql, params))


class InitConnection:
    def __init__(self):
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.closed = False

    def cursor(self):
        return InitCursor(self)

    def close(self):
        self.closed = True


def test_destination_connect_sets_utc_session(tmp_path, monkeypatch, password_env):
    fake = InitConnection()
    monkeypatch.setattr("airgap_sync.destination.mysql.pymysql.connect", lambda **kwargs: fake)
    mysql, destination = configs(tmp_path)
    mysql = mysql.model_copy(update={"password_env": password_env})
    connection = DestinationMySQLConnection(mysql, destination)
    connection.connect()
    assert fake.executed == [("SET SESSION time_zone = '+00:00'", ())]


class StagingColumnsConnection(DestinationMySQLConnection):
    def __init__(self, mysql, destination, *, probe, rows):
        super().__init__(mysql, destination)
        self.probe = probe
        self.rows = rows
        self.probe_calls = 0
        self.queries = []

    def _fetchone(self, sql, params=()):
        self.probe_calls += 1
        self.queries.append((sql, params))
        return self.probe

    def _fetchall(self, sql, params=()):
        self.queries.append((sql, params))
        return self.rows


def test_mysql_56_staging_columns_omit_generation_expression_and_cache_probe(tmp_path):
    connection = StagingColumnsConnection(
        *configs(tmp_path),
        probe=None,
        rows=[("id", 1, ""), ("name", 2, "")],
    )

    first = connection.staging_columns("staging")
    second = connection.staging_columns("staging")

    assert first == second
    assert [column.generation_expression for column in first] == ["", ""]
    assert connection.probe_calls == 1
    staging_queries = [sql for sql, _ in connection.queries if "ORDER BY" in sql]
    assert len(staging_queries) == 2
    assert all("GENERATION_EXPRESSION" not in sql for sql in staging_queries)


def test_generation_expression_capability_preserves_generated_column(tmp_path):
    connection = StagingColumnsConnection(
        *configs(tmp_path),
        probe=(1,),
        rows=[("id", 1, "", ""), ("computed", 2, "", "(`id` + 1)")],
    )

    columns = connection.staging_columns("staging")

    assert columns[1].generation_expression == "(`id` + 1)"
    assert columns[1].generated
    assert "COALESCE(GENERATION_EXPRESSION,'')" in connection.queries[-1][0]


def test_generation_expression_capability_probe_failure_is_not_fallback(tmp_path):
    connection = DestinationMySQLConnection(*configs(tmp_path))

    def fail_probe(sql, params=()):
        raise DestinationMySQLError("Destination MySQL query failed: connection lost")

    connection._fetchone = fail_probe
    connection._fetchall = lambda sql, params=(): pytest.fail("fallback query must not run")

    with pytest.raises(DestinationMySQLError, match="connection lost"):
        connection.staging_columns("staging")
    assert connection._supports_generation_expression is None


def test_generation_expression_capability_does_not_use_server_version(tmp_path):
    connection = StagingColumnsConnection(*configs(tmp_path), probe=None, rows=[("id", 1, "")])
    connection.ping = lambda: pytest.fail("VERSION() must not be used for capability detection")

    assert connection.staging_columns("staging")[0].generation_expression == ""


def test_v2_to_v3_adds_explicit_cleanup_completion_columns(tmp_path):
    connection = RecordingConnection(*configs(tmp_path))
    connection._fetchall = lambda sql, params=(): []
    connection._migrate_v2_to_v3()
    sql = [statement for statement, _ in connection.sql]
    assert any("ADD COLUMN incoming_cleanup_completed_at" in statement for statement in sql)
    assert any("ADD COLUMN backup_cleanup_completed_at" in statement for statement in sql)
    assert sql[-1].endswith("SET version=3 WHERE singleton=1")
