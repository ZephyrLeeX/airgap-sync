"""强类型配置模型。

密码本身不属于配置:配置只记录提供密码的环境变量名
(mysql.password_env), 密码在连接时从环境变量读取。

V1 统一 Full Snapshot 同步: 表配置只有 name / enabled,
不存在同步模式与 key 概念。
"""

from __future__ import annotations

import os
import re
from enum import StrEnum
from pathlib import Path
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from airgap_sync.common.runtime import parse_duration


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


class ScheduleConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    delay_after_success: str = "7d"
    retry_after_failure: str = "6h"

    @model_validator(mode="after")
    def validate_durations(self) -> Self:
        parse_duration(self.delay_after_success)
        parse_duration(self.retry_after_failure)
        return self


class DestinationWorkerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    poll_interval: str = "30s"

    @model_validator(mode="after")
    def validate_duration(self) -> Self:
        parse_duration(self.poll_interval)
        return self


class MaintenanceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    failed_run_retention: str = "30d"
    destination_orphan_retention: str = "30d"

    @model_validator(mode="after")
    def validate_durations(self) -> Self:
        parse_duration(self.failed_run_retention)
        parse_duration(self.destination_orphan_retention)
        return self


class DestinationConfig(BaseModel):
    """Destination 接收目录与导入参数。"""

    model_config = ConfigDict(extra="forbid")

    incoming_dir: Path
    metadata_database: str = Field(default="airgap_sync_meta", min_length=1, max_length=64)
    insert_batch_rows: int = Field(default=1000, ge=1, le=1_000_000)
    verify_fetch_size: int = Field(default=2000, ge=1, le=1_000_000)
    report_timezone: str = Field(default="Asia/Shanghai", min_length=1)
    settle_seconds: float = Field(default=2, ge=0, le=3600)

    @model_validator(mode="after")
    def validate_report_timezone(self) -> Self:
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

        try:
            ZoneInfo(self.report_timezone)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError(
                f"destination.report_timezone is not a valid IANA timezone: "
                f"{self.report_timezone!r}"
            ) from exc
        return self


class SourceMonitoringConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
    worker_task_name: str = "Airgap Sync Source Worker"
    collect_cpu: bool = True
    collect_memory: bool = True
    managed_storage_interval: str = "1h"
    log_dirs: list[Path] = Field(default_factory=list, max_length=8)
    upload_connect_timeout_seconds: float = Field(default=3, gt=0, le=30)
    upload_read_timeout_seconds: float = Field(default=10, gt=0, le=60)
    upload_max_attempts: int = Field(default=2, ge=1, le=3)

    @model_validator(mode="after")
    def validate_monitoring(self) -> Self:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", self.node_id):
            raise ValueError("monitoring.node_id must be a safe filename component")
        if not self.worker_task_name.strip():
            raise ValueError("monitoring.worker_task_name must not be blank")
        for path in self.log_dirs:
            if not path.is_absolute() or path == Path(path.anchor):
                raise ValueError(
                    "monitoring.log_dirs must contain absolute project log directories"
                )
        normalized = [Path(os.path.abspath(path)) for path in self.log_dirs]
        for index, path in enumerate(normalized):
            if any(
                path == other or path in other.parents or other in path.parents
                for other in normalized[:index]
            ):
                raise ValueError("monitoring.log_dirs must not overlap")
        if parse_duration(self.managed_storage_interval) < 1800:
            raise ValueError("monitoring.managed_storage_interval must be at least 30m")
        return self


class MonitorIngestConfig(BaseModel):
    """Opt-in Destination telemetry; independent of Source monitoring config."""

    model_config = ConfigDict(extra="forbid")

    incoming: Path
    db_path: Path = Path("/var/lib/airgap-sync-monitor/monitor.db")
    poll_seconds: float = Field(default=10, ge=1, le=3600)
    batch_size: int = Field(default=100, ge=1, le=1000)
    scan_limit: int = Field(default=2000, ge=1, le=10000)
    settle_seconds: float = Field(default=30, ge=1, le=3600)
    invalid_grace_seconds: float = Field(default=300, ge=30, le=86400)
    history_days: int = Field(default=35, ge=30, le=365)
    dedup_days: int = Field(default=90, ge=31, le=730)
    quarantine_days: int = Field(default=7, ge=1, le=30)
    quarantine_max_files: int = Field(default=1000, ge=1, le=10000)
    future_seconds: int = Field(default=300, ge=0, le=3600)

    @model_validator(mode="after")
    def validate_ingest(self) -> Self:
        if not self.incoming.is_absolute() or not self.db_path.is_absolute():
            raise ValueError("monitor_ingest paths must be absolute")
        if self.db_path.name != "monitor.db":
            raise ValueError("monitor_ingest.db_path must name a dedicated monitor.db")
        if self.dedup_days <= self.history_days:
            raise ValueError("dedup_days must exceed history_days")
        if self.invalid_grace_seconds < self.settle_seconds:
            raise ValueError("invalid grace must be at least settle_seconds")
        return self


class AlertThreshold(BaseModel):
    model_config = ConfigDict(extra="forbid")
    free_percent: float = Field(ge=0, le=100)
    free_bytes: int = Field(ge=0)


class MonitorAlertsConfig(BaseModel):
    """Opt-in alert evaluation; requires monitor_ingest ownership."""

    model_config = ConfigDict(extra="forbid")
    expected_sources: list[str] = Field(default_factory=list, max_length=1000)
    interval_seconds: int = Field(default=60, ge=5, le=3600)
    future_seconds: int = Field(default=300, ge=0, le=3600)
    first_heartbeat_grace_seconds: int = Field(default=600, ge=0)
    heartbeat_warning_seconds: int = Field(default=600, ge=1)
    heartbeat_critical_seconds: int = Field(default=1200, ge=2)
    freshness_warning_seconds: int = Field(default=8 * 86400, ge=1)
    freshness_critical_seconds: int = Field(default=10 * 86400, ge=2)
    incoming_stale_seconds: int = Field(default=7200, ge=1)
    incoming_settle_seconds: int = Field(default=60, ge=1)
    recovered_retention_days: int = Field(default=90, ge=1, le=3650)
    disk_warning: AlertThreshold = AlertThreshold(free_percent=20, free_bytes=50 * 1024**3)
    disk_critical: AlertThreshold = AlertThreshold(free_percent=10, free_bytes=20 * 1024**3)
    disk_emergency: AlertThreshold = AlertThreshold(free_percent=5, free_bytes=10 * 1024**3)

    @model_validator(mode="after")
    def validate_alerts(self) -> Self:
        import re

        if len(self.expected_sources) != len(set(self.expected_sources)) or any(
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", node) is None
            for node in self.expected_sources
        ):
            raise ValueError("expected_sources must contain unique valid node IDs")
        if not self.heartbeat_warning_seconds < self.heartbeat_critical_seconds:
            raise ValueError("heartbeat warning must precede critical")
        if not self.freshness_warning_seconds < self.freshness_critical_seconds:
            raise ValueError("freshness warning must precede critical")
        if not (
            self.disk_emergency.free_percent
            < self.disk_critical.free_percent
            < self.disk_warning.free_percent
            and self.disk_emergency.free_bytes
            < self.disk_critical.free_bytes
            < self.disk_warning.free_bytes
        ):
            raise ValueError("disk thresholds must rise from emergency to warning")
        return self


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
    paths: PathsConfig | None = None
    snapshot: SnapshotConfig = SnapshotConfig()
    chunk: ChunkConfig = ChunkConfig()
    relay: RelayConfig | None = None
    spool: SpoolConfig = SpoolConfig()
    schedule: ScheduleConfig = ScheduleConfig()
    destination_worker: DestinationWorkerConfig = DestinationWorkerConfig()
    maintenance: MaintenanceConfig = MaintenanceConfig()
    destination: DestinationConfig | None = None
    monitoring: SourceMonitoringConfig | None = None
    monitor_ingest: MonitorIngestConfig | None = None
    monitor_alerts: MonitorAlertsConfig | None = None
    tables: list[TableConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_table_names(self) -> Self:
        if self.monitor_alerts is not None and self.monitor_ingest is None:
            raise ValueError("monitor_alerts requires monitor_ingest")
        if self.role is Role.SOURCE:
            if self.paths is None:
                raise ValueError("paths is required when role=source")
            if not self.tables:
                raise ValueError("at least one table is required when role=source")
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
