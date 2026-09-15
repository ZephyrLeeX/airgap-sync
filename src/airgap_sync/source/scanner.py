"""单表流式扫描: server-side cursor → Row Codec → Chunk + Digest。

一次扫描 = 一条 SELECT * 的完整消费:

    stream_table(table, fetch_size)
        ↓ fetchmany 批
    每行 encode_row (canonical bytes)
        ├→ ChunkWriter.write_row   (zstd 分块落盘)
        └→ MultisetDigest.update   (验证摘要)

全程没有 fetchall / list(all_rows): 内存只与 fetch_size 单批
和当前 Chunk 相关, 与总行数无关。
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from airgap_sync.common.manifest import ChunkMeta
from airgap_sync.common.models import ChunkConfig, SnapshotConfig
from airgap_sync.common.row_codec import encode_row
from airgap_sync.common.verification import MultisetDigest, VerificationSummary
from airgap_sync.source.chunk_writer import ChunkWriter

logger = logging.getLogger(__name__)


class TableStreamProtocol(Protocol):
    """流式扫描句柄 (SourceMySQLConnection.stream_table 的返回值)。"""

    columns: list[str]

    def __iter__(self) -> Any: ...


class SnapshotSource(Protocol):
    """Snapshot 需要的 Source MySQL 能力 (SourceMySQLConnection 或测试替身)。"""

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ...) -> list[tuple[Any, ...]]: ...

    def get_create_table(self, table_name: str) -> str: ...

    def stream_table(self, table_name: str, fetch_size: int) -> TableStreamProtocol: ...


@dataclass(frozen=True)
class ScanResult:
    """一次扫描的产物: 列顺序 + Chunk metadata + 验证摘要。"""

    columns: list[str]
    chunks: list[ChunkMeta]
    verification: VerificationSummary


def scan_table(
    source: SnapshotSource,
    table_name: str,
    run_dir: Path,
    snapshot_config: SnapshotConfig,
    chunk_config: ChunkConfig,
    *,
    on_chunk_closed: Callable[[ChunkMeta], None] | None = None,
    cancel_check: Callable[[], None] | None = None,
) -> ScanResult:
    """流式扫描一张表, 生成 Chunk 文件并计算验证摘要。

    行按 SELECT * 返回顺序编码; 不加 ORDER BY (见技术设计),
    摘要与行顺序无关。
    """
    digest = MultisetDigest()
    with ChunkWriter(
        run_dir,
        chunk_config,
        log_context=f" table={table_name}",
        on_chunk_closed=on_chunk_closed,
    ) as writer:
        with source.stream_table(table_name, snapshot_config.fetch_size) as stream:
            columns = list(stream.columns)
            for batch in stream:
                if cancel_check is not None:
                    cancel_check()
                for row in batch:
                    encoded = encode_row(row)
                    writer.write_row(encoded)
                    digest.update(encoded)
        chunks = writer.finish()
    logger.info(
        "scan finished: table=%s rows=%d chunks=%d", table_name, digest.row_count, len(chunks)
    )
    return ScanResult(columns=columns, chunks=chunks, verification=digest.summary())
