"""Full Snapshot 边生成、边上传的 Source 交付流水线。"""

from __future__ import annotations

import hashlib
import logging
import queue
import shutil
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from airgap_sync.common.fsutil import atomic_write_bytes, retry_windows_file_lock
from airgap_sync.common.manifest import (
    MANIFEST_FILENAME,
    PROTOCOL_VERSION,
    RUN_TYPE_FULL_SNAPSHOT,
    ChunkMeta,
    Manifest,
    SchemaFileMeta,
    SourceMeta,
    VerificationMeta,
    manifest_payload,
)
from airgap_sync.common.models import AppConfig
from airgap_sync.common.transport import transport_filename
from airgap_sync.common.verification import DIGEST_ALGORITHM
from airgap_sync.source.mysql import check_table, fetch_table_info
from airgap_sync.source.scanner import SnapshotSource, scan_table
from airgap_sync.source.snapshot import (
    ROW_COUNT_MISMATCH,
    SCHEMA_CHANGED_DURING_SNAPSHOT,
    SCHEMA_FILENAME,
    SnapshotError,
    SnapshotResult,
    _normalize_ddl,
    _schema_file_bytes,
    generate_run_id,
    outbox_run_dir,
)
from airgap_sync.source.state import RunStatus, SourceState, TableStatus
from airgap_sync.source.uploader import RelayUploader, UploadError

logger = logging.getLogger(__name__)

DISK_PRESSURE = "DISK_PRESSURE"
UPLOAD_BACKLOG_LIMIT = "UPLOAD_BACKLOG_LIMIT"


class DeliveryError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


@dataclass(frozen=True)
class _UploadItem:
    path: Path
    logical_name: str
    transport_name: str
    sha256: str
    size: int


class DeliveryRunner:
    """一个 scanner producer + 一个 HTTP worker；无多 PUT 并发。"""

    def __init__(
        self,
        source: SnapshotSource,
        state: SourceState,
        config: AppConfig,
        uploader: RelayUploader,
    ) -> None:
        self._source = source
        self._state = state
        self._config = config
        self._uploader = uploader

    def sync(self, table_name: str) -> SnapshotResult:
        started = time.monotonic()
        table = self._config.table(table_name)
        if table is None:
            raise SnapshotError("TABLE_NOT_CONFIGURED", f"table '{table_name}' is not in config")
        if not table.enabled:
            raise SnapshotError("TABLE_NOT_ENABLED", f"table '{table_name}' is disabled in config")
        table_check = check_table(
            table, fetch_table_info(self._source, self._config.mysql.database, table_name)
        )
        if not table_check.ok:
            raise SnapshotError(
                table_check.error_code or "TABLE_CHECK_FAILED",
                f"table '{table_name}' is not a supported BASE TABLE",
            )

        run_id = generate_run_id()
        run_dir = outbox_run_dir(self._config.paths.data_dir, table_name, run_id)
        self._state.register_table(table_name)
        self._state.begin_run(table_name, run_id)
        run_dir.mkdir(parents=True, exist_ok=False)

        upload_queue: queue.Queue[_UploadItem | None] = queue.Queue()
        fatal_lock = threading.Lock()
        fatal: list[Exception] = []
        pending_lock = threading.Lock()
        pending_bytes = 0
        peak_pending_bytes = 0

        def set_fatal(exc: Exception) -> None:
            with fatal_lock:
                if not fatal:
                    fatal.append(exc)

        def check_fatal() -> None:
            with fatal_lock:
                if fatal:
                    raise fatal[0]

        def worker() -> None:
            nonlocal pending_bytes
            worker_state = SourceState(self._state.db_path)
            try:
                worker_state.initialize()
                while True:
                    item = upload_queue.get()
                    if item is None:
                        return
                    with fatal_lock:
                        already_failed = bool(fatal)
                    if already_failed:
                        continue
                    try:
                        confirmation = self._uploader.upload(
                            item.path,
                            item.transport_name,
                            item.sha256,
                            on_attempt=lambda attempt, name=item.logical_name: (
                                worker_state.mark_upload_attempt(run_id, name, attempt)
                            ),
                        )
                        # 远端确认先持久化；本地删除失败绝不能触发重新上传。
                        worker_state.mark_uploaded(
                            run_id,
                            item.logical_name,
                            confirmation.attempts,
                            confirmation.request_id,
                        )
                        try:
                            retry_windows_file_lock(
                                "unlink",
                                item.path,
                                lambda path=item.path: path.unlink(missing_ok=True),
                            )
                            worker_state.mark_cleanup_error(run_id, item.logical_name, None)
                            with pending_lock:
                                pending_bytes -= item.size
                        except OSError as exc:
                            worker_state.mark_cleanup_error(run_id, item.logical_name, str(exc))
                            logger.warning(
                                "local cleanup pending: run_id=%s artifact=%s error=%s",
                                run_id,
                                item.logical_name,
                                exc,
                            )
                        logger.info(
                            "upload completed: table=%s run_id=%s artifact=%s bytes=%d "
                            "request_id=%s attempt=%d",
                            table_name,
                            run_id,
                            item.logical_name,
                            item.size,
                            confirmation.request_id,
                            confirmation.attempts,
                        )
                    except UploadError as exc:
                        worker_state.mark_artifact_failed(
                            run_id, item.logical_name, exc.attempts, str(exc)
                        )
                        set_fatal(exc)
                    except Exception as exc:
                        set_fatal(exc)
            finally:
                worker_state.close()

        thread = threading.Thread(target=worker, name=f"relay-{run_id}", daemon=True)
        thread.start()
        self._state.set_run_status(run_id, RunStatus.UPLOADING)

        def on_chunk_closed(chunk: ChunkMeta) -> None:
            nonlocal peak_pending_bytes, pending_bytes
            check_fatal()
            path = run_dir / chunk.file
            transport_name = transport_filename(run_id, chunk.file)
            # metadata 必须在 enqueue 与任何删除之前提交。
            self._state.register_artifact(
                run_id,
                "chunk",
                chunk.sequence,
                chunk.file,
                transport_name,
                chunk.compressed_bytes,
                chunk.sha256,
            )
            with pending_lock:
                pending_bytes += chunk.compressed_bytes
                peak_pending_bytes = max(peak_pending_bytes, pending_bytes)
                current_pending = pending_bytes
            free = shutil.disk_usage(self._config.paths.data_dir).free
            if current_pending > self._config.spool.max_pending_bytes:
                raise DeliveryError(
                    UPLOAD_BACKLOG_LIMIT,
                    f"pending bytes {current_pending} exceed configured limit "
                    f"{self._config.spool.max_pending_bytes}",
                )
            if free < self._config.spool.min_free_bytes:
                raise DeliveryError(
                    DISK_PRESSURE,
                    f"free bytes {free} below configured minimum "
                    f"{self._config.spool.min_free_bytes}",
                )
            upload_queue.put_nowait(
                _UploadItem(path, chunk.file, transport_name, chunk.sha256, chunk.compressed_bytes)
            )

        scan = None
        scan_completed_at: float | None = None
        error: Exception | None = None
        try:
            ddl_before = self._source.get_create_table(table_name)
            scan = scan_table(
                self._source,
                table_name,
                run_dir,
                self._config.snapshot,
                self._config.chunk,
                on_chunk_closed=on_chunk_closed,
                cancel_check=check_fatal,
            )
            ddl_after = self._source.get_create_table(table_name)
            if _normalize_ddl(ddl_before) != _normalize_ddl(ddl_after):
                raise SnapshotError(
                    SCHEMA_CHANGED_DURING_SNAPSHOT,
                    f"table '{table_name}' DDL changed during snapshot scan",
                )
            scan_completed_at = time.monotonic()
        except Exception as exc:
            error = exc
            set_fatal(exc)
        finally:
            upload_queue.put_nowait(None)
            thread.join()

        try:
            check_fatal()
            assert scan is not None and error is None
            chunk_rows = sum(chunk.rows for chunk in scan.chunks)
            if chunk_rows != scan.verification.row_count:
                raise SnapshotError(
                    ROW_COUNT_MISMATCH,
                    f"chunk rows ({chunk_rows}) != row_count ({scan.verification.row_count})",
                )

            self._state.set_run_status(run_id, RunStatus.FINALIZING)
            schema_bytes = _schema_file_bytes(ddl_before)
            schema_path = run_dir / SCHEMA_FILENAME
            atomic_write_bytes(schema_path, schema_bytes)
            self._upload_final_artifact(
                run_id,
                "schema",
                SCHEMA_FILENAME,
                schema_path,
                hashlib.sha256(schema_bytes).hexdigest(),
            )

            manifest = Manifest(
                protocol_version=PROTOCOL_VERSION,
                run_id=run_id,
                run_type=RUN_TYPE_FULL_SNAPSHOT,
                source=SourceMeta(database=self._config.mysql.database, table=table_name),
                schema_file=SchemaFileMeta(
                    file=SCHEMA_FILENAME, sha256=hashlib.sha256(schema_bytes).hexdigest()
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
            manifest_bytes = manifest_payload(manifest)
            manifest_path = run_dir / MANIFEST_FILENAME
            atomic_write_bytes(manifest_path, manifest_bytes)
            raw_bytes = sum(chunk.uncompressed_bytes for chunk in scan.chunks)
            compressed_bytes = sum(chunk.compressed_bytes for chunk in scan.chunks)
            # Manifest 已完整构建：先推进生成指针；远端提交仍须等待 manifest 确认。
            self._state.complete_run(
                table_name,
                run_id,
                row_count=scan.verification.row_count,
                chunk_count=len(scan.chunks),
                raw_bytes=raw_bytes,
                compressed_bytes=compressed_bytes,
            )
            self._state.set_run_status(run_id, RunStatus.FINALIZING)
            self._upload_final_artifact(
                run_id,
                "manifest",
                MANIFEST_FILENAME,
                manifest_path,
                hashlib.sha256(manifest_bytes).hexdigest(),
            )
            self._state.deliver_run(
                table_name,
                run_id,
                row_count=scan.verification.row_count,
                chunk_count=len(scan.chunks),
                raw_bytes=raw_bytes,
                compressed_bytes=compressed_bytes,
            )
            self._cleanup_delivered_run(run_id, run_dir)
            finished = time.monotonic()
            snapshot_seconds = max((scan_completed_at or finished) - started, 0.000001)
            upload_seconds = max(finished - (scan_completed_at or finished), 0.0)
            ratio = compressed_bytes / raw_bytes if raw_bytes else 0.0
            logger.info(
                "source benchmark metrics: table=%s run_id=%s rows_per_second=%.1f "
                "raw_mib_per_second=%.2f compressed_mib_per_second=%.2f "
                "compression_ratio=%.4f snapshot_seconds=%.3f upload_finalize_seconds=%.3f "
                "peak_spool_bytes=%d",
                table_name,
                run_id,
                scan.verification.row_count / snapshot_seconds,
                raw_bytes / 1024**2 / snapshot_seconds,
                compressed_bytes / 1024**2 / snapshot_seconds,
                ratio,
                snapshot_seconds,
                upload_seconds,
                peak_pending_bytes,
            )
            return SnapshotResult(
                table_name,
                run_id,
                TableStatus.DELIVERED.value,
                scan.verification.row_count,
                len(scan.chunks),
                raw_bytes,
                compressed_bytes,
                run_dir,
            )
        except Exception as exc:
            is_disk = isinstance(exc, DeliveryError) and exc.code in {
                DISK_PRESSURE,
                UPLOAD_BACKLOG_LIMIT,
            }
            self._state.fail_run(table_name, run_id, str(exc), disk_pressure=is_disk)
            return SnapshotResult(
                table_name,
                run_id,
                TableStatus.DISK_PRESSURE.value if is_disk else TableStatus.FAILED.value,
                0,
                len(scan.chunks) if scan else 0,
                0,
                0,
                run_dir,
                str(exc),
            )

    def _upload_final_artifact(
        self, run_id: str, kind: str, logical_name: str, path: Path, sha256: str
    ) -> None:
        transport_name = transport_filename(run_id, logical_name)
        size = path.stat().st_size
        self._state.register_artifact(
            run_id, kind, None, logical_name, transport_name, size, sha256
        )
        try:
            confirmation = self._uploader.upload(
                path,
                transport_name,
                sha256,
                on_attempt=lambda attempt: self._state.mark_upload_attempt(
                    run_id, logical_name, attempt
                ),
            )
        except UploadError as exc:
            self._state.mark_artifact_failed(run_id, logical_name, exc.attempts, str(exc))
            raise
        self._state.mark_uploaded(
            run_id, logical_name, confirmation.attempts, confirmation.request_id
        )
        try:
            retry_windows_file_lock("unlink", path, lambda: path.unlink(missing_ok=True))
        except OSError as exc:
            self._state.mark_cleanup_error(run_id, logical_name, str(exc))

    def _cleanup_delivered_run(self, run_id: str, run_dir: Path) -> None:
        for artifact in self._state.get_artifacts(run_id):
            if artifact.upload_status != "UPLOADED":
                continue
            path = run_dir / artifact.logical_name
            try:
                retry_windows_file_lock(
                    "unlink", path, lambda cleanup_path=path: cleanup_path.unlink(missing_ok=True)
                )
                self._state.mark_cleanup_error(run_id, artifact.logical_name, None)
            except OSError as exc:
                self._state.mark_cleanup_error(run_id, artifact.logical_name, str(exc))
        try:
            retry_windows_file_lock("rmdir", run_dir, run_dir.rmdir)
        except OSError:
            logger.warning("delivered run directory cleanup pending: %s", run_dir)
