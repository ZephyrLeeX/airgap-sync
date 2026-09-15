"""Validated incoming Run → staging 的可重入编排。"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import zstandard

from airgap_sync.common.models import AppConfig
from airgap_sync.common.row_codec import decode_row
from airgap_sync.destination.ddl import SchemaError, rewrite_create_table_target
from airgap_sync.destination.incoming import DestinationError, discover_runs, validate_run
from airgap_sync.destination.mysql import (
    DestinationMySQLConnection,
    DestinationMySQLError,
    staging_table_name,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProcessResult:
    run_id: str
    status: str
    table: str | None = None
    staging_table: str | None = None
    rows: int = 0
    chunks: int = 0
    error: str | None = None


class DestinationProcessor:
    """单次处理能力；Phase 6 的 worker 可重复调用同一 API。"""

    def __init__(self, connection: DestinationMySQLConnection, config: AppConfig) -> None:
        if config.destination is None:
            raise DestinationError("INVALID_ROLE", "destination configuration is required")
        self._db = connection
        self._config = config
        self._destination = config.destination

    def process(self, run_id: str) -> ProcessResult:
        started = datetime.now(UTC)
        try:
            validated = validate_run(
                self._destination.incoming_dir,
                run_id,
                self._destination.settle_seconds,
            )
        except DestinationError as exc:
            return ProcessResult(
                run_id,
                "FAILED" if exc.permanent else "INCOMPLETE",
                error=str(exc),
            )

        manifest = validated.manifest
        staging = staging_table_name(run_id)
        if not self._db.acquire_run_lock(run_id):
            return ProcessResult(
                run_id,
                "BUSY",
                manifest.source.table,
                staging,
                error="RUN_BUSY: another processor holds this run lock",
            )
        if not self._db.acquire_table_lock(manifest.source.database, manifest.source.table):
            self._db.release_run_lock(run_id)
            return ProcessResult(
                run_id,
                "BUSY",
                manifest.source.table,
                staging,
                error="RUN_BUSY: another run for this source table is being imported",
            )
        try:
            return self._process_locked(
                validated.schema_path, validated.chunk_paths, manifest, staging
            )
        except (DestinationError, DestinationMySQLError, SchemaError) as exc:
            # register 前的矛盾可能没有 runs 行，UPDATE 0 行是安全的。
            try:
                self._db.fail_run(run_id, str(exc))
            except DestinationMySQLError:
                logger.exception("cannot persist failed run status: run_id=%s", run_id)
            return ProcessResult(
                run_id,
                "FAILED",
                manifest.source.table,
                staging,
                error=str(exc),
            )
        finally:
            self._db.release_table_lock(manifest.source.database, manifest.source.table)
            self._db.release_run_lock(run_id)
            elapsed = (datetime.now(UTC) - started).total_seconds()
            logger.info(
                "destination process finished: run_id=%s table=%s staging=%s elapsed=%.1fs",
                run_id,
                manifest.source.table,
                staging,
                elapsed,
            )

    def _process_locked(
        self, schema_path: Path, chunk_paths: tuple[Path, ...], manifest: Any, staging: str
    ) -> ProcessResult:
        existing_run = self._db.get_run(manifest.run_id)
        self._db.register_validated_run(manifest, staging)
        current = self._db.get_run(manifest.run_id)
        assert current is not None
        if current[2] == "STAGED":
            if not self._db.table_exists(self._config.mysql.database, staging):
                raise DestinationError(
                    "STAGING_STATE_MISMATCH", "run is STAGED but its staging table is missing"
                )
            return ProcessResult(
                manifest.run_id,
                "STAGED",
                manifest.source.table,
                staging,
                manifest.row_count,
                len(manifest.chunks),
            )

        try:
            ddl = schema_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise DestinationError("INVALID_SCHEMA_DDL", f"cannot read schema: {exc}") from exc
        rewritten = rewrite_create_table_target(ddl, manifest.source.table, staging)
        staging_exists = self._db.table_exists(self._config.mysql.database, staging)
        if existing_run is None and staging_exists:
            raise DestinationError(
                "STAGING_STATE_MISMATCH", "staging exists without metadata for this run"
            )
        if (
            existing_run is not None
            and not staging_exists
            and self._db.has_imported_chunks(manifest.run_id)
        ):
            raise DestinationError(
                "STAGING_STATE_MISMATCH", "staging is missing but imported chunk metadata exists"
            )
        if not staging_exists:
            self._db.create_staging_table(rewritten)

        columns = self._db.staging_columns(staging)
        actual_names = [column.name for column in columns]
        if actual_names != manifest.columns:
            raise DestinationError(
                "STAGING_COLUMN_MISMATCH",
                f"staging columns {actual_names!r} != manifest columns {manifest.columns!r}",
            )
        insert_indices = [index for index, column in enumerate(columns) if not column.generated]
        insert_columns = [columns[index].name for index in insert_indices]

        self._db.begin_import(manifest.run_id)
        for chunk, path in zip(manifest.chunks, chunk_paths, strict=True):
            if self._db.chunk_status(manifest.run_id, chunk.sequence) == "IMPORTED":
                continue
            self._import_chunk(
                manifest.run_id,
                chunk.sequence,
                path,
                chunk.rows,
                len(manifest.columns),
                staging,
                insert_columns,
                insert_indices,
            )
        self._db.complete_staged(manifest.run_id, manifest.row_count)
        return ProcessResult(
            manifest.run_id,
            "STAGED",
            manifest.source.table,
            staging,
            manifest.row_count,
            len(manifest.chunks),
        )

    def _import_chunk(
        self,
        run_id: str,
        sequence: int,
        path: Path,
        expected_rows: int,
        expected_width: int,
        staging: str,
        insert_columns: list[str],
        insert_indices: list[int],
    ) -> None:
        decoded_rows = 0
        batch: list[tuple[Any, ...]] = []
        try:
            with self._db.transaction():
                self._db.set_chunk_importing(run_id, sequence)
                with (
                    path.open("rb") as compressed,
                    zstandard.ZstdDecompressor().stream_reader(compressed) as reader,
                ):
                    buffered = io.BufferedReader(reader)
                    for raw_line in buffered:
                        line = raw_line[:-1] if raw_line.endswith(b"\n") else raw_line
                        if line.endswith(b"\r"):
                            line = line[:-1]
                        row = decode_row(line)
                        if len(row) != expected_width:
                            raise DestinationError(
                                "ROW_WIDTH_MISMATCH",
                                f"chunk {sequence} row width {len(row)} != {expected_width}",
                            )
                        batch.append(tuple(row[index] for index in insert_indices))
                        decoded_rows += 1
                        if len(batch) >= self._destination.insert_batch_rows:
                            self._db.insert_rows(staging, insert_columns, batch)
                            batch.clear()
                    if batch:
                        self._db.insert_rows(staging, insert_columns, batch)
                        batch.clear()
                if decoded_rows != expected_rows:
                    raise DestinationError(
                        "CHUNK_ROW_COUNT_MISMATCH",
                        f"chunk {sequence} decoded {decoded_rows} rows, expected {expected_rows}",
                    )
                self._db.set_chunk_imported(run_id, sequence, decoded_rows)
        except Exception as exc:
            # transaction 已回滚；FAILED 标记另行提交，不会与部分业务数据共存。
            self._db.fail_chunk(run_id, sequence, str(exc))
            if isinstance(exc, (DestinationError, DestinationMySQLError, SchemaError)):
                raise
            raise DestinationError("CHUNK_IMPORT_FAILED", f"chunk {sequence}: {exc}") from exc


def process_once(connection: DestinationMySQLConnection, config: AppConfig) -> list[ProcessResult]:
    """按 run_id（时间前缀）扫描所有 manifest，一次处理后退出。"""
    assert config.destination is not None
    processor = DestinationProcessor(connection, config)
    return [processor.process(run_id) for run_id in discover_runs(config.destination.incoming_dir)]
