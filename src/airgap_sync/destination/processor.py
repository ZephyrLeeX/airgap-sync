"""Incoming Run 到 VERIFIED live table 的可重入完整编排。"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

import zstandard

from airgap_sync.common.models import AppConfig
from airgap_sync.common.row_codec import decode_row, encode_row
from airgap_sync.common.transport import parse_transport_filename, transport_filename
from airgap_sync.common.verification import MultisetDigest, VerificationSummary
from airgap_sync.destination.ddl import SchemaError, rewrite_create_table_target
from airgap_sync.destination.incoming import (
    DestinationError,
    ValidatedRun,
    discover_runs,
    parse_source_created_at,
    validate_run,
)
from airgap_sync.destination.mysql import (
    DestinationMySQLConnection,
    DestinationMySQLError,
    RunRecord,
    TableVersion,
    backup_table_name,
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


class VersionRelation(Enum):
    NO_PREVIOUS = "NO_PREVIOUS"
    SAME_RUN = "SAME_RUN"
    NEWER = "NEWER"
    OLDER = "OLDER"
    SAME_TIMESTAMP_DIFFERENT_RUN = "SAME_TIMESTAMP_DIFFERENT_RUN"


@dataclass(frozen=True)
class VersionOrdering:
    relation: VersionRelation
    latest: TableVersion | None


class DestinationVerifier:
    """只从 MySQL 表流式回读并复用公共 Row Codec / Multiset Digest。"""

    def __init__(self, connection: DestinationMySQLConnection, fetch_size: int) -> None:
        self._db = connection
        self._fetch_size = fetch_size

    def verify(self, table: str, columns: list[str]) -> VerificationSummary:
        digest = MultisetDigest()
        with self._db.stream_table_columns(table, columns, self._fetch_size) as rows:
            for row in rows:
                digest.update(encode_row(row))
        return digest.summary()


class DestinationProcessor:
    """一次 process 完成 validate、stage、verify、promotion 和 cleanup。"""

    def __init__(self, connection: DestinationMySQLConnection, config: AppConfig) -> None:
        if config.destination is None:
            raise DestinationError("INVALID_ROLE", "destination configuration is required")
        self._db = connection
        self._config = config
        self._destination = config.destination
        self._verifier = DestinationVerifier(connection, self._destination.verify_fetch_size)

    def process(self, run_id: str) -> ProcessResult:
        started = datetime.now(UTC)
        existing = self._db.get_run(run_id)
        if existing is not None and existing.status == "VERIFIED":
            self._retry_cleanup(existing)
            return self._result(existing)
        if existing is not None and existing.status == "SUPERSEDED":
            self._cleanup_superseded(existing)
            return self._result(existing)

        try:
            validated = validate_run(
                self._destination.incoming_dir, run_id, self._destination.settle_seconds
            )
        except DestinationError as exc:
            return ProcessResult(
                run_id, "FAILED" if exc.permanent else "INCOMPLETE", error=str(exc)
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
                error="RUN_BUSY: another run holds this table lock",
            )
        try:
            return self._process_locked(validated, staging)
        except (DestinationError, DestinationMySQLError, SchemaError) as exc:
            try:
                self._db.fail_run(run_id, str(exc))
            except DestinationMySQLError:
                logger.exception("cannot persist failed run status: run_id=%s", run_id)
            return ProcessResult(run_id, "FAILED", manifest.source.table, staging, error=str(exc))
        finally:
            self._db.release_table_lock(manifest.source.database, manifest.source.table)
            self._db.release_run_lock(run_id)
            logger.info(
                "destination process finished: run_id=%s table=%s elapsed=%.1fs",
                run_id,
                manifest.source.table,
                (datetime.now(UTC) - started).total_seconds(),
            )

    def _process_locked(self, validated: ValidatedRun, staging: str) -> ProcessResult:
        manifest = validated.manifest
        source_time = parse_source_created_at(manifest.created_at)
        existing = self._db.get_run(manifest.run_id)
        self._db.register_validated_run(manifest, staging, source_time)
        run = self._require_run(manifest.run_id)

        if run.status == "VERIFIED":
            self._retry_cleanup(run)
            return self._result(run)
        if run.status in {"MISMATCH", "SUPERSEDED"}:
            return self._result(run)
        if run.status == "SWAPPING":
            return self._recover_swap(validated, run)

        if run.status not in {"STAGED", "VERIFYING"}:
            self._stage(validated, staging, existing)
        elif not self._db.table_exists(self._config.mysql.database, staging):
            raise DestinationError(
                "STAGING_STATE_MISMATCH", "run requires verification but staging is missing"
            )

        summary = self._verify(manifest.run_id, staging, manifest.columns)
        if not self._matches(manifest, summary):
            return ProcessResult(
                manifest.run_id,
                "MISMATCH",
                manifest.source.table,
                staging,
                summary.row_count,
                len(manifest.chunks),
                "DIGEST_MISMATCH: staging database content differs from manifest",
            )

        ordering = self._ordering(
            manifest.run_id, manifest.source.database, manifest.source.table, source_time
        )
        if ordering.relation is VersionRelation.OLDER:
            run = self._require_run(manifest.run_id)
            self._cleanup_superseded(run)
            return self._result(run)
        self._promote(manifest.run_id, manifest.source.table, staging, ordering.latest)
        run = self._require_run(manifest.run_id)
        self._retry_cleanup(run, validated)
        return self._result(run)

    def _stage(self, validated: ValidatedRun, staging: str, existing: RunRecord | None) -> None:
        manifest = validated.manifest
        try:
            ddl = validated.schema_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise DestinationError("INVALID_SCHEMA_DDL", f"cannot read schema: {exc}") from exc
        rewritten = rewrite_create_table_target(ddl, manifest.source.table, staging)
        staging_exists = self._db.table_exists(self._config.mysql.database, staging)
        if existing is None and staging_exists:
            raise DestinationError(
                "STAGING_STATE_MISMATCH", "staging exists without metadata for this run"
            )
        if (
            existing is not None
            and not staging_exists
            and self._db.has_imported_chunks(manifest.run_id)
        ):
            raise DestinationError(
                "STAGING_STATE_MISMATCH", "staging is missing but imported chunk metadata exists"
            )
        if not staging_exists:
            self._db.create_staging_table(rewritten)

        engine = self._db.storage_engine(self._config.mysql.database, staging)
        if engine is None or engine.upper() != "INNODB":
            raise DestinationError(
                "UNSUPPORTED_STORAGE_ENGINE",
                f"staging engine is {engine or 'unknown'}, expected InnoDB",
            )
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
        for chunk, path in zip(manifest.chunks, validated.chunk_paths, strict=True):
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

    def _verify(self, run_id: str, table: str, columns: list[str]) -> VerificationSummary:
        self._db.set_verifying(run_id)
        summary = self._verifier.verify(table, columns)
        # Expected fields stay in metadata; comparison is made by the caller against Manifest.
        return summary

    def _matches(self, manifest: Any, summary: VerificationSummary) -> bool:
        expected = manifest.verification
        matched = (
            summary.row_count == expected.row_count
            and summary.digest_a == expected.digest_a
            and summary.digest_b == expected.digest_b
        )
        self._db.record_verification(manifest.run_id, summary, matched)
        return matched

    def _ordering(
        self, run_id: str, source_database: str, table: str, source_time: datetime
    ) -> VersionOrdering:
        ordering = self._compare_run_to_latest_version(run_id, source_database, table, source_time)
        if ordering.relation is VersionRelation.OLDER:
            self._db.set_superseded(run_id)
        elif ordering.relation is VersionRelation.SAME_TIMESTAMP_DIFFERENT_RUN:
            raise DestinationError(
                "RUN_ORDER_AMBIGUOUS", "another run has the same source_created_at"
            )
        return ordering

    def _compare_run_to_latest_version(
        self, run_id: str, source_database: str, table: str, source_time: datetime
    ) -> VersionOrdering:
        latest = self._db.latest_version(source_database, table)
        if latest is None:
            return VersionOrdering(VersionRelation.NO_PREVIOUS, None)
        if latest.run_id == run_id:
            return VersionOrdering(VersionRelation.SAME_RUN, latest)
        latest_time = _as_utc(latest.source_created_at)
        if source_time < latest_time:
            return VersionOrdering(VersionRelation.OLDER, latest)
        if source_time == latest_time:
            return VersionOrdering(VersionRelation.SAME_TIMESTAMP_DIFFERENT_RUN, latest)
        return VersionOrdering(VersionRelation.NEWER, latest)

    def _promote(
        self, run_id: str, target: str, staging: str, previous: TableVersion | None
    ) -> None:
        database = self._config.mysql.database
        object_type = self._db.object_type(database, target)
        if object_type is not None and object_type != "BASE TABLE":
            raise DestinationError(
                "TARGET_OBJECT_TYPE_UNSUPPORTED", f"target object type is {object_type}"
            )
        target_existed = object_type == "BASE TABLE"
        backup = backup_table_name(run_id) if target_existed else None
        if target_existed:
            fk, trigger = self._db.target_dependencies(database, target)
            if fk:
                raise DestinationError(
                    "TARGET_FOREIGN_KEY_DEPENDENCY", "target has inbound or outbound foreign keys"
                )
            if trigger:
                raise DestinationError("TARGET_TRIGGER_DEPENDENCY", "target has triggers")
            if backup is not None and self._db.object_type(database, backup) is not None:
                raise DestinationError("SWAP_STATE_MISMATCH", "deterministic backup already exists")
        self._db.prepare_swapping(run_id, target_existed, backup)
        self._db.rename_for_promotion(database, staging, target, backup)
        self._db.finalize_verified(run_id, None if previous is None else previous.run_id)

    def _recover_swap(self, validated: ValidatedRun, run: RunRecord) -> ProcessResult:
        database = self._config.mysql.database
        target_type = self._db.object_type(database, run.table_name)
        staging_type = self._db.object_type(database, run.staging_table)
        backup_type = (
            None if run.backup_table is None else self._db.object_type(database, run.backup_table)
        )
        before = staging_type == "BASE TABLE" and (
            (run.target_existed is False and target_type is None)
            or (run.target_existed is True and target_type == "BASE TABLE" and backup_type is None)
        )
        after = (
            staging_type is None
            and target_type == "BASE TABLE"
            and (
                (run.target_existed is False and run.backup_table is None)
                or (run.target_existed is True and backup_type == "BASE TABLE")
            )
        )
        if not before and not after:
            raise DestinationError("SWAP_STATE_MISMATCH", "unexpected target/staging/backup state")
        if run.source_created_at is None:
            raise DestinationError("SWAP_STATE_MISMATCH", "run source_created_at is missing")
        ordering = self._ordering(
            run.run_id,
            run.source_database,
            run.table_name,
            _as_utc(run.source_created_at),
        )
        if ordering.relation is VersionRelation.OLDER:
            current = self._require_run(run.run_id)
            self._cleanup_superseded(current)
            return self._result(self._require_run(run.run_id))
        if before:
            self._db.rename_for_promotion(
                database, run.staging_table, run.table_name, run.backup_table
            )
            self._db.finalize_verified(
                run.run_id, None if ordering.latest is None else ordering.latest.run_id
            )
        elif after:
            summary = self._verifier.verify(run.table_name, validated.manifest.columns)
            expected = validated.manifest.verification
            matched = (
                summary.row_count == expected.row_count
                and summary.digest_a == expected.digest_a
                and summary.digest_b == expected.digest_b
            )
            if not matched:
                raise DestinationError(
                    "SWAP_STATE_MISMATCH", "live target digest differs from manifest"
                )
            self._db.record_verification(run.run_id, summary, True)
            # Verification persistence returns STAGED; restore swap intent.
            self._db.prepare_swapping(run.run_id, bool(run.target_existed), run.backup_table)
            self._db.finalize_verified(
                run.run_id, None if ordering.latest is None else ordering.latest.run_id
            )
        current = self._require_run(run.run_id)
        self._retry_cleanup(current, validated)
        return self._result(self._require_run(run.run_id))

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
                    for raw_line in io.BufferedReader(reader):
                        line = raw_line.rstrip(b"\r\n")
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
                if decoded_rows != expected_rows:
                    raise DestinationError(
                        "CHUNK_ROW_COUNT_MISMATCH",
                        f"chunk {sequence} decoded {decoded_rows} rows, expected {expected_rows}",
                    )
                self._db.set_chunk_imported(run_id, sequence, decoded_rows)
        except Exception as exc:
            self._db.fail_chunk(run_id, sequence, str(exc))
            if isinstance(exc, (DestinationError, DestinationMySQLError, SchemaError)):
                raise
            raise DestinationError("CHUNK_IMPORT_FAILED", f"chunk {sequence}: {exc}") from exc

    def _cleanup_superseded(self, run: RunRecord) -> None:
        try:
            if self._db.table_exists(self._config.mysql.database, run.staging_table):
                self._db.drop_table(self._config.mysql.database, run.staging_table)
            _cleanup_incoming(self._destination.incoming_dir, run.run_id)
            self._db.record_cleanup_error(run.run_id, None)
        except (OSError, DestinationMySQLError) as exc:
            self._db.record_cleanup_error(run.run_id, str(exc))

    def _retry_cleanup(self, run: RunRecord, validated: ValidatedRun | None = None) -> None:
        if run.status != "VERIFIED":
            return
        try:
            if run.backup_table and self._db.table_exists(
                self._config.mysql.database, run.backup_table
            ):
                self._db.drop_table(self._config.mysql.database, run.backup_table)
            self._db.record_backup_cleanup_error(run.run_id, None)
        except DestinationMySQLError as exc:
            self._db.record_backup_cleanup_error(run.run_id, str(exc))
        try:
            _cleanup_incoming(self._destination.incoming_dir, run.run_id, validated)
            self._db.record_cleanup_error(run.run_id, None)
        except OSError as exc:
            self._db.record_cleanup_error(run.run_id, str(exc))

    def _require_run(self, run_id: str) -> RunRecord:
        run = self._db.get_run(run_id)
        if run is None:
            raise DestinationMySQLError("run metadata is missing")
        return run

    @staticmethod
    def _result(run: RunRecord) -> ProcessResult:
        return ProcessResult(
            run.run_id,
            run.status,
            run.table_name,
            run.staging_table,
            run.row_count,
            run.chunk_count,
        )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _cleanup_incoming(
    incoming_dir: Path, run_id: str, validated: ValidatedRun | None = None
) -> None:
    """Manifest discovery marker first, then all other formal artifacts."""
    manifest = (
        validated.manifest_path
        if validated is not None
        else incoming_dir / transport_filename(run_id, "manifest.json")
    )
    manifest.unlink(missing_ok=True)
    if validated is not None:
        rest = [validated.schema_path, *validated.chunk_paths]
    else:
        rest = []
        for path in incoming_dir.iterdir() if incoming_dir.exists() else ():
            try:
                candidate_run, logical = parse_transport_filename(path.name)
            except ValueError:
                continue
            if candidate_run == run_id and logical != "manifest.json":
                rest.append(path)
    for path in rest:
        path.unlink(missing_ok=True)


def process_once(connection: DestinationMySQLConnection, config: AppConfig) -> list[ProcessResult]:
    assert config.destination is not None
    processor = DestinationProcessor(connection, config)
    return [processor.process(run_id) for run_id in discover_runs(config.destination.incoming_dir)]
