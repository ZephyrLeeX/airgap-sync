"""强类型配置模型。

密码本身不属于配置:配置只记录提供密码的环境变量名
(mysql.password_env), 密码在连接时从环境变量读取。

V1 统一 Full Snapshot 同步: 表配置只有 name / enabled,
不存在同步模式与 key 概念。
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


class SnapshotConfig(BaseModel):
    """全表扫描参数。

    fetch_size 是每次从 server-side cursor 读取的行数,
    不是协议限制, 只是流式读取的批量大小。
    """

    model_config = ConfigDict(extra="forbid")

    fetch_size: int = Field(default=2000, ge=1, le=1_000_000)


class ChunkConfig(BaseModel):
    """Chunk 切分与压缩参数。

    max_rows / max_uncompressed_bytes 是可配置默认值, 不是协议硬限制;
    单行本身超过字节阈值时允许生成单行超限 Chunk (不丢数据)。
    """

    model_config = ConfigDict(extra="forbid")

    max_rows: int = Field(default=50_000, ge=1)
    max_uncompressed_bytes: int = Field(default=64 * 1024 * 1024, ge=1)
    compression_level: int = Field(default=3, ge=1, le=19)


class RelayConfig(BaseModel):
    """HTTP Relay 配置；Bearer token 本身永不进入配置模型。"""

    model_config = ConfigDict(extra="forbid")

    base_url: str = Field(min_length=1)
    token_env: str = Field(min_length=1, repr=False)
    ca_file: Path | None = None
    connect_timeout_seconds: float = Field(default=10, gt=0, le=600)
    read_timeout_seconds: float = Field(default=600, gt=0, le=86_400)
    max_attempts: int = Field(default=5, ge=1, le=100)
    retry_base_seconds: float = Field(default=5, ge=0, le=3600)
    retry_max_seconds: float = Field(default=60, ge=0, le=3600)

    @model_validator(mode="after")
    def validate_relay(self) -> Self:
        from urllib.parse import urlsplit

        parsed = urlsplit(self.base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("relay.base_url must be an absolute http:// or https:// URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("relay.base_url must not contain credentials, query, or fragment")
        if self.ca_file is not None and parsed.scheme != "https":
            raise ValueError("relay.ca_file is only valid with an https:// base_url")
        return self


class SpoolConfig(BaseModel):
    """Source 临时磁盘占用保护阈值。"""

    model_config = ConfigDict(extra="forbid")

    max_pending_bytes: int = Field(default=10 * 1024**3, ge=1)
    min_free_bytes: int = Field(default=10 * 1024**3, ge=0)


class TableConfig(BaseModel):
    """单张同步表的配置。

    所有表统一 FULL_SNAPSHOT, 不需要主键 / 逻辑键 / 同步模式;
    旧的 mode / key 字段因 extra=forbid 会明确校验失败。
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    enabled: bool = True

    @model_validator(mode="after")
    def validate_name(self) -> Self:
        # 表名会进入 MySQL 引用与文件路径 (outbox/<table>), 不允许空白名
        if not self.name.strip():
            raise ValueError("table name must not be blank")
        return self


class AppConfig(BaseModel):
    """顶层应用配置。"""

    model_config = ConfigDict(extra="forbid")

    role: Role
    mysql: MySQLConfig
    paths: PathsConfig
    snapshot: SnapshotConfig = SnapshotConfig()
    chunk: ChunkConfig = ChunkConfig()
    relay: RelayConfig | None = None
    spool: SpoolConfig = SpoolConfig()
    tables: list[TableConfig] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_table_names(self) -> Self:
        names = [table.name for table in self.tables]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"duplicate table names in config: {', '.join(duplicates)}")
        return self

    def table(self, name: str) -> TableConfig | None:
        """按名称查找表配置; 不存在时返回 None。"""
        return next((table for table in self.tables if table.name == name), None)

    @property
    def enabled_tables(self) -> list[TableConfig]:
        return [table for table in self.tables if table.enabled]
