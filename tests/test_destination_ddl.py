from __future__ import annotations

import pytest

from airgap_sync.destination.ddl import SchemaError, rewrite_create_table_target


@pytest.mark.parametrize(
    ("source", "ddl"),
    [
        ("plain", "CREATE TABLE plain (`id` int) ENGINE=InnoDB;"),
        ("中文表", "CREATE TABLE `中文表` (`值` text) COMMENT='中文表';"),
        ("odd`name", "CREATE TABLE `odd``name` (`id` int) COMMENT='odd`name';"),
    ],
)
def test_rewrite_only_first_table_identifier(source, ddl):
    rewritten = rewrite_create_table_target(ddl, source, "__airgap_stg_x")
    assert rewritten.startswith("CREATE TABLE `__airgap_stg_x`")
    assert rewritten.count("__airgap_stg_x") == 1
    assert rewritten.split("`__airgap_stg_x`", 1)[1] == ddl[ddl.index("(") - 1 :]


def test_comment_occurrence_and_semicolon_are_unchanged():
    ddl = "CREATE TABLE `orders` (`note` text COMMENT 'orders; still') COMMENT='orders';"
    rewritten = rewrite_create_table_target(ddl, "orders", "__airgap_stg_x")
    assert "'orders; still'" in rewritten
    assert "COMMENT='orders'" in rewritten


def test_schema_table_mismatch():
    with pytest.raises(SchemaError, match="SCHEMA_TABLE_MISMATCH"):
        rewrite_create_table_target("CREATE TABLE `a` (`id` int)", "b", "stg")


@pytest.mark.parametrize(
    "ddl",
    [
        "CREATE TABLE `a` (`id` int, FOREIGN KEY (`id`) REFERENCES `x` (`id`))",
        "CREATE TABLE `a` (`id` int, CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `x` (`id`))",
    ],
)
def test_foreign_keys_and_constraints_are_rejected(ddl):
    with pytest.raises(SchemaError, match="UNSUPPORTED_SCHEMA_FEATURE"):
        rewrite_create_table_target(ddl, "a", "stg")


def test_multiple_statements_rejected():
    with pytest.raises(SchemaError, match="multiple SQL statements"):
        rewrite_create_table_target("CREATE TABLE `a` (`id` int); DROP TABLE `x`;", "a", "stg")
