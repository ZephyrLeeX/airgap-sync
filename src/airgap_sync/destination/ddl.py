"""SHOW CREATE TABLE DDL 的安全检查与 staging 目标重写。"""

from __future__ import annotations

import re


class SchemaError(Exception):
    """Schema 不安全、不受支持或与 Manifest 不一致。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


_CREATE = re.compile(r"\A\s*CREATE\s+TABLE\s+", re.IGNORECASE)
_UNQUOTED = re.compile(r"[A-Za-z0-9_$\u0080-\uffff]+")


def _identifier_at(ddl: str, start: int) -> tuple[str, int]:
    if start >= len(ddl):
        raise SchemaError("INVALID_SCHEMA_DDL", "CREATE TABLE has no target identifier")
    if ddl[start] == "`":
        chars: list[str] = []
        pos = start + 1
        while pos < len(ddl):
            if ddl[pos] == "`":
                if pos + 1 < len(ddl) and ddl[pos + 1] == "`":
                    chars.append("`")
                    pos += 2
                    continue
                return "".join(chars), pos + 1
            chars.append(ddl[pos])
            pos += 1
        raise SchemaError("INVALID_SCHEMA_DDL", "unterminated quoted table identifier")
    match = _UNQUOTED.match(ddl, start)
    if match is None:
        raise SchemaError("INVALID_SCHEMA_DDL", "invalid CREATE TABLE target identifier")
    return match.group(), match.end()


def _code_text(ddl: str) -> str:
    """移除字符串/identifier/comment 内容，保留 SQL 结构字符。"""
    cleaned: list[str] = []
    pos = 0
    quote: str | None = None
    while pos < len(ddl):
        char = ddl[pos]
        if quote is not None:
            if char == quote:
                if pos + 1 < len(ddl) and ddl[pos + 1] == quote:
                    pos += 2
                    continue
                quote = None
            elif char == "\\" and quote in {"'", '"'}:
                pos += 2
                continue
            pos += 1
            continue
        if ddl.startswith("--", pos) or char == "#":
            newline = ddl.find("\n", pos)
            pos = len(ddl) if newline < 0 else newline + 1
            cleaned.append(" ")
            continue
        if ddl.startswith("/*", pos):
            end = ddl.find("*/", pos + 2)
            if end < 0:
                raise SchemaError("INVALID_SCHEMA_DDL", "unterminated SQL comment")
            pos = end + 2
            cleaned.append(" ")
            continue
        if char in {"'", '"', "`"}:
            quote = char
            cleaned.append(" ")
        else:
            cleaned.append(char)
        pos += 1
    if quote is not None:
        raise SchemaError("INVALID_SCHEMA_DDL", "unterminated quoted value")
    return "".join(cleaned)


def rewrite_create_table_target(ddl: str, expected_source_table: str, staging_table: str) -> str:
    """只重写 CREATE TABLE 后的第一个 identifier，并拒绝危险结构。"""
    match = _CREATE.match(ddl)
    if match is None:
        raise SchemaError("INVALID_SCHEMA_DDL", "schema must contain one CREATE TABLE statement")
    source_table, end = _identifier_at(ddl, match.end())
    if source_table != expected_source_table:
        raise SchemaError(
            "SCHEMA_TABLE_MISMATCH",
            f"schema table {source_table!r} != manifest source table {expected_source_table!r}",
        )
    rest = ddl[end:]
    # SHOW CREATE TABLE 不会带 schema qualifier；拒绝它，避免重写后残留 `.table`。
    if rest.lstrip().startswith("."):
        raise SchemaError("INVALID_SCHEMA_DDL", "schema-qualified CREATE TABLE is unsupported")
    if not rest.lstrip().startswith("("):
        raise SchemaError("INVALID_SCHEMA_DDL", "CREATE TABLE must contain a column definition")
    code = _code_text(rest)
    words = re.findall(r"[A-Za-z_]+", code.upper())
    if "FOREIGN" in words or "CONSTRAINT" in words:
        raise SchemaError(
            "UNSUPPORTED_SCHEMA_FEATURE", "FOREIGN KEY / CONSTRAINT is unsupported in staging"
        )
    # 只允许尾部一个可选分号，避免执行任意附加 statement。
    semicolons = code.count(";")
    if semicolons > 1 or (semicolons == 1 and not code.rstrip().endswith(";")):
        raise SchemaError("INVALID_SCHEMA_DDL", "multiple SQL statements are not allowed")
    quoted = "`" + staging_table.replace("`", "``") + "`"
    return ddl[: match.end()] + quoted + ddl[end:]
