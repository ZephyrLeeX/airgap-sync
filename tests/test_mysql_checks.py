"""MySQL 元数据检查逻辑测试 (不需要真实 MySQL)。

使用 FakeExecutor 模拟 information_schema 查询;
真实数据库测试见 test_mysql_integration.py。
"""

from __future__ import annotations

from typing import Any

from airgap_sync.common.models import TableConfig, TableMode
from airgap_sync.source.mysql import check_table, check_tables, fetch_table_columns


class FakeExecutor:
    """模拟 QueryExecutor: 只维护 表名 -> 列名集合。"""

    def __init__(self, tables: dict[str, set[str]]):
        self.tables = tables
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        self.queries.append((sql, params))
        columns = self.tables.get(params[1])
        if columns is None:
            return []
        return [(column,) for column in sorted(columns)]


def keyed_table(key: list[str]) -> TableConfig:
    return TableConfig(name="t_demo", mode=TableMode.KEYED, key=key)


class TestFetchTableColumns:
    def test_returns_all_columns(self):
        executor = FakeExecutor({"t_demo": {"id", "name"}})
        columns = fetch_table_columns(executor, "sgaj_data", "t_demo")
        assert columns == {"id", "name"}

    def test_missing_table_returns_none(self):
        executor = FakeExecutor({})
        assert fetch_table_columns(executor, "sgaj_data", "t_demo") is None

    def test_uses_information_schema_with_parameters(self):
        executor = FakeExecutor({"t_demo": {"id"}})
        fetch_table_columns(executor, "sgaj_data", "t_demo")
        sql, params = executor.queries[0]
        assert "information_schema.COLUMNS" in sql
        assert params == ("sgaj_data", "t_demo")  # 参数化查询, 不拼接表名


class TestCheckTable:
    def test_keyed_all_columns_present(self):
        result = check_table(keyed_table(["id"]), {"id", "name"})
        assert result.ok
        assert result.error_code is None

    def test_keyed_single_missing_column(self):
        result = check_table(keyed_table(["nope"]), {"id"})
        assert not result.ok
        assert result.error_code == "KEY_COLUMN_NOT_FOUND"
        assert result.missing_key_columns == ("nope",)

    def test_keyed_composite_key_partially_missing(self):
        result = check_table(keyed_table(["hh", "fyrq", "extra"]), {"hh"})
        assert not result.ok
        assert result.missing_key_columns == ("fyrq", "extra")

    def test_table_not_found(self):
        result = check_table(keyed_table(["id"]), None)
        assert not result.ok
        assert result.error_code == "TABLE_NOT_FOUND"

    def test_row_multiset_only_needs_table(self):
        table = TableConfig(name="t_demo", mode=TableMode.ROW_MULTISET)
        result = check_table(table, {"a"})
        assert result.ok

    def test_row_multiset_ignores_key(self):
        table = TableConfig(name="t_demo", mode=TableMode.ROW_MULTISET, key=["whatever"])
        result = check_table(table, {"a"})
        assert result.ok


class TestCheckTables:
    def test_checks_each_enabled_table(self):
        executor = FakeExecutor({"t_a": {"id"}, "t_b": {"x"}, "t_c": {"hh", "fyrq"}})
        tables = [
            TableConfig(name="t_a", mode=TableMode.KEYED, key=["id"]),
            TableConfig(name="t_b", mode=TableMode.KEYED, key=["id"]),  # key 不存在
            TableConfig(name="t_c", mode=TableMode.ROW_MULTISET),
        ]
        results = check_tables(executor, "sgaj_data", tables)
        assert [r.ok for r in results] == [True, False, True]
        assert results[1].error_code == "KEY_COLUMN_NOT_FOUND"
        assert len(executor.queries) == 3
