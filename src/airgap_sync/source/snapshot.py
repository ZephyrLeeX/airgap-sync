"""Snapshot Run 生命周期编排。

一个 Run = 一次完整快照生成:

    表已启用检查
        ↓
    DDL 第 1 次读取 (SHOW CREATE TABLE)
        ↓
    流式扫描 (scanner): Chunk + Multiset Digest
        ↓
    DDL 第 2 次读取 —— 与第 1 次不一致 → Run FAILED
    (SCHEMA_CHANGED_DURING_SNAPSHOT, 不生成 manifest)
        ↓
    schema.sql + manifest.json 原子写入
        ↓
    状态 COMPLETED, 推进 current_run_id

失败原则 (简单可靠):

* 任何中途失败 → Run FAILED, 不生成 manifest;
* 失败 Run 不推进 current_run_id;
* V1 不做 Chunk 级断点续扫 —— 下次运行生成全新 Run;
* 残留 .part / chunk 文件不视为有效数据 (manifest 不存在即 Run 不完整)。
"""

from __future__ import annotations

import hashlib
import logging
import re
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from airgap_sync.common.fsutil import atomic_write_bytes
from airgap_sync.common.manifest import (
    MANIFEST_FILENAME,
    PROTOCOL_VERSION,
    RUN_TYPE_FULL_SNAPSHOT,
    Manifest,
    SchemaFileMeta,
    SourceMeta,
    VerificationMeta,
    write_manifest,
)
from airgap_sync.common.models import AppConfig
from airgap_sync.common.verification import DIGEST_ALGORITHM
from airgap_sync.source.mysql import check_table, fetch_table_info
from airgap_sync.source.scanner import ScanResult, SnapshotSource, scan_table
from airgap_sync.source.state import SourceState, TableStatus

logger = logging.getLogger(__name__)

OUTBOX_DIR_NAME = "outbox"
SCHEMA_FILENAME = "schema.sql"

# 错误码
TABLE_NOT_CONFIGURED = "TABLE_NOT_CONFIGURED"
TABLE_NOT_ENABLED = "TABLE_NOT_ENABLED"
SCHEMA_CHANGED_DURING_SNAPSHOT = "SCHEMA_CHANGED_DURING_SNAPSHOT"
ROW_COUNT_MISMATCH = "ROW_COUNT_MISMATCH"


class SnapshotError(Exception):
    """Snapshot Run 无法开始或未通过一致性检查。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


# SHOW CREATE TABLE 的表选项 AUTO_INCREMENT=<n> 是自增计数器,
# 会随并发 INSERT 变化, 不代表结构变化; 比较两次 DDL 时忽略其数值,
# 其余任何差异 (列 / 索引 / charset / comment...) 都判定为结构变化。
_AUTO_INCREMENT_OPTION = re.compile(r"AUTO_INCREMENT=\d+", re.IGNORECASE)


def generate_run_id() -> str:
    """生成 Run ID: UTC timestamp + random suffix。

    形如 20260914T213500Z-a1b2c3d4 —— 唯一、文件名安全、
    不依赖数据库主键, 不需要分布式 ID 服务。
    """
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{secrets.token_hex(4)}"


def _normalize_ddl(ddl: str) -> str:
    """DDL 比较用的规范化: 统一换行 + 屏蔽 AUTO_INCREMENT 计数器数值。"""
    unix_newlines = ddl.replace("\r\n", "\n").replace("\r", "\n")
    return _AUTO_INCREMENT_OPTION.sub("AUTO_INCREMENT=#", unix_newlines)


def _schema_file_bytes(ddl: str) -> bytes:
    """schema.sql 内容: 原始 DDL, UTF-8, \\n 换行, 末尾统一 ;\\n。

    不重新构造 CREATE TABLE —— SHOW CREATE TABLE 的输出原样保留
    (字段类型 / DEFAULT / NULL / INDEX / PRIMARY KEY / charset /
    collation / comment 一个不丢), 目标端据此建表。
    """
    text = ddl.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text.endswith(";"):
        text += ";"
    return (text + "\n").encode("utf-8")


@dataclass(frozen=True)
class SnapshotResult:
    """一次 Snapshot Run 的结果摘要 (不含任何业务数据)。"""

    table: str
    run_id: str
    status: str
    row_count: int
    chunk_count: int
    raw_bytes: int
    compressed_bytes: int
    run_dir: Path
    error: str | None = None


class SnapshotRunner:
    """编排单表 Snapshot Run。"""

    def __init__(self, source: SnapshotSource, state: SourceState, config: AppConfig) -> None:
        self._source = source
        self._state = state
        self._config = config

    def snapshot(self, table_name: str) -> SnapshotResult:
        """生成一张表的完整 Snapshot Run。

        开始前的配置问题 (表未配置 / 未启用) 抛出 SnapshotError;
        开始后的任何失败记录为 FAILED Run 并返回 FAILED 结果,
        由调用方决定退出码。
        """
        table = self._config.table(table_name)
        if table is None:
            raise SnapshotError(TABLE_NOT_CONFIGURED, f"table '{table_name}' is not in config")
        if not table.enabled:
            raise SnapshotError(TABLE_NOT_ENABLED, f"table '{table_name}' is disabled in config")

        table_check = check_table(
            table,
            fetch_table_info(self._source, self._config.mysql.database, table_name),
        )
        if not table_check.ok:
            detail = (
                f"table '{table_name}' has unsupported type {table_check.table_type!r}"
                if table_check.table_type is not None
                else f"table '{table_name}' does not exist"
            )
            raise SnapshotError(table_check.error_code or "TABLE_CHECK_FAILED", detail)

        run_id = generate_run_id()
        run_dir = outbox_run_dir(self._config.paths.data_dir, table_name, run_id)
        self._state.register_table(table_name)
        self._state.begin_run(table_name, run_id)
        started = datetime.now(UTC)
        logger.info("snapshot started: table=%s run_id=%s", table_name, run_id)
        try:
            scan = self._generate(table_name, run_id, run_dir)
        except Exception as exc:
            self._state.fail_run(table_name, run_id, str(exc))
            elapsed = (datetime.now(UTC) - started).total_seconds()
            logger.error(
                "snapshot failed: table=%s run_id=%s elapsed=%.1fs error=%s",
                table_name,
                run_id,
                elapsed,
                exc,
            )
            return SnapshotResult(
                table=table_name,
                run_id=run_id,
                status=TableStatus.FAILED.value,
                row_count=0,
                chunk_count=0,
                raw_bytes=0,
                compressed_bytes=0,
                run_dir=run_dir,
                error=str(exc),
            )
        self._state.complete_run(
            table_name,
            run_id,
            row_count=scan.verification.row_count,
            chunk_count=len(scan.chunks),
            raw_bytes=sum(chunk.uncompressed_bytes for chunk in scan.chunks),
            compressed_bytes=sum(chunk.compressed_bytes for chunk in scan.chunks),
        )
        elapsed = (datetime.now(UTC) - started).total_seconds()
        logger.info(
            "snapshot completed: table=%s run_id=%s rows=%d chunks=%d "
            "raw_bytes=%d compressed_bytes=%d elapsed=%.1fs",
            table_name,
            run_id,
            scan.verification.row_count,
            len(scan.chunks),
            sum(chunk.uncompressed_bytes for chunk in scan.chunks),
            sum(chunk.compressed_bytes for chunk in scan.chunks),
            elapsed,
        )
        return SnapshotResult(
            table=table_name,
            run_id=run_id,
            status=TableStatus.COMPLETED.value,
            row_count=scan.verification.row_count,
            chunk_count=len(scan.chunks),
            raw_bytes=sum(chunk.uncompressed_bytes for chunk in scan.chunks),
            compressed_bytes=sum(chunk.compressed_bytes for chunk in scan.chunks),
            run_dir=run_dir,
        )

    def _generate(self, table_name: str, run_id: str, run_dir: Path) -> ScanResult:
        """Run 主体: DDL → 扫描 → DDL → schema.sql + manifest。"""
        run_dir.mkdir(parents=True, exist_ok=False)

        ddl_before = self._source.get_create_table(table_name)

        scan = scan_table(
            self._source,
            table_name,
            run_dir,
            self._config.snapshot,
            self._config.chunk,
        )

        ddl_after = self._source.get_create_table(table_name)
        if _normalize_ddl(ddl_before) != _normalize_ddl(ddl_after):
            raise SnapshotError(
                SCHEMA_CHANGED_DURING_SNAPSHOT,
                f"table '{table_name}' DDL changed during snapshot scan; "
                "run discarded, retry will create a new run",
            )

        schema_bytes = _schema_file_bytes(ddl_before)
        schema_path = run_dir / SCHEMA_FILENAME
        atomic_write_bytes(schema_path, schema_bytes)

        chunk_rows = sum(chunk.rows for chunk in scan.chunks)
        if chunk_rows != scan.verification.row_count:
            raise SnapshotError(
                ROW_COUNT_MISMATCH,
                f"chunk rows ({chunk_rows}) != digest row_count "
                f"({scan.verification.row_count}) for run {run_id}",
            )

        manifest = Manifest(
            protocol_version=PROTOCOL_VERSION,
            run_id=run_id,
            run_type=RUN_TYPE_FULL_SNAPSHOT,
            source=SourceMeta(database=self._config.mysql.database, table=table_name),
            schema_file=SchemaFileMeta(
                file=SCHEMA_FILENAME,
                sha256=hashlib.sha256(schema_bytes).hexdigest(),
            ),
            columns=scan.columns,
            row_count=scan.verification.row_count,
            chunks=scan.chunks,
            verification=VerificationMeta(
                algorithm=DIGEST_ALGORITHM,
                row_count=scan.verification.row_count,
                digest_a=scan.verification.digest_a,
                digest_b=scan.verification.digest_b,
            ),
            created_at=datetime.now(UTC).isoformat(timespec="seconds"),
        )
        write_manifest(run_dir / MANIFEST_FILENAME, manifest)
        return scan


def outbox_run_dir(data_dir: Path, table_name: str, run_id: str) -> Path:
    """Run 目录: <data_dir>/outbox/<table_name>/<run_id>。"""
    return data_dir / OUTBOX_DIR_NAME / table_name / run_id
