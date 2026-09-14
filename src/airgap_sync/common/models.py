"""强类型配置模型。

密码本身不属于配置:配置只记录提供密码的环境变量名
(mysql.password_env), 密码在连接时从环境变量读取。
"""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Role(StrEnum):
    """运行角色。Source 和 Destination 共用同一套代码。"""

    SOURCE = "source"
    DESTINATION = "destination"


class TableMode(StrEnum):
    """表同步模式。"""

    KEYED = "keyed"
    ROW_MULTISET = "row_multiset"


class MySQLConfig(BaseModel):
    """Source MySQL 连接配置。"""

    model_config = ConfigDict(extra="forbid")

    host: str = Field(min_length=1)
    port: int = Field(default=3306, ge=1, le=65535)
    database: str = Field(min_length=1)
    user: str = Field(min_length=1)
    password_env: str = Field(min_length=1)
    connect_timeout: int = Field(default=10, ge=1, le=600)


class PathsConfig(BaseModel):
    """本地路径配置。"""

    model_config = ConfigDict(extra="forbid")

    data_dir: Path


class TableConfig(BaseModel):
    """单张同步表的配置。"""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    mode: TableMode
    key: list[str] | None = None
    enabled: bool = True

    @model_validator(mode="after")
    def validate_key(self) -> Self:
        if self.mode is not TableMode.KEYED:
            return self
        if not self.key:
            raise ValueError(f"table '{self.name}': mode 'keyed' requires at least one key column")
        empty = [column for column in self.key if not column.strip()]
        if empty:
            raise ValueError(f"table '{self.name}': key column names must not be empty")
        duplicates = sorted({column for column in self.key if self.key.count(column) > 1})
        if duplicates:
            raise ValueError(f"table '{self.name}': duplicate key columns: {', '.join(duplicates)}")
        return self


class AppConfig(BaseModel):
    """顶层应用配置。"""

    model_config = ConfigDict(extra="forbid")

    role: Role
    mysql: MySQLConfig
    paths: PathsConfig
    tables: list[TableConfig] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_table_names(self) -> Self:
        names = [table.name for table in self.tables]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"duplicate table names in config: {', '.join(duplicates)}")
        return self

    @property
    def enabled_tables(self) -> list[TableConfig]:
        return [table for table in self.tables if table.enabled]
