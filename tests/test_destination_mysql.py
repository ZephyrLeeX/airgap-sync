from __future__ import annotations

from typing import Any

import pymysql.cursors

from airgap_sync.common.models import DestinationConfig, MySQLConfig
from airgap_sync.destination.mysql import DestinationMySQLConnection


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
