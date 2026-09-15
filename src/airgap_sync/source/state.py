"""Source SQLite schema v3：表、Run 与 artifact 传输状态。"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

SCHEMA_VERSION = 3
STATE_DIR_NAME = "state"
STATE_DB_NAME = "meta.db"


class StateError(Exception):
    """状态库初始化或访问失败。"""


class TableStatus(StrEnum):
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"
    DISK_PRESSURE = "DISK_PRESSURE"


class RunStatus(StrEnum):
    GENERATING = "GENERATING"
    UPLOADING = "UPLOADING"
    FINALIZING = "FINALIZING"
    SNAPSHOT_READY = "SNAPSHOT_READY"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"
    DISK_PRESSURE = "DISK_PRESSURE"


class UploadStatus(StrEnum):
    PENDING = "PENDING"
    UPLOADING = "UPLOADING"
    UPLOADED = "UPLOADED"
    FAILED = "FAILED"


_ACTIVE_RUN_STATUSES = (
    RunStatus.GENERATING.value,
    RunStatus.UPLOADING.value,
    RunStatus.FINALIZING.value,
)


def state_db_path(data_dir: Path) -> Path:
    return data_dir / STATE_DIR_NAME / STATE_DB_NAME


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


@dataclass(frozen=True)
class TableState:
    table_name: str
    last_snapshot_run_id: str | None
    last_delivered_run_id: str | None
    status: str
    last_run_id: str | None
    last_error: str | None
    created_at: str
    updated_at: str

    @property
    def current_run_id(self) -> str | None:
        """Python API 兼容别名；持久化 schema 只保留明确的新字段。"""
        return self.last_snapshot_run_id


@dataclass(frozen=True)
class SyncRun:
    run_id: str
    table_name: str
    status: str
    created_at: str
    snapshot_completed_at: str | None
    delivered_at: str | None
    row_count: int | None
    chunk_count: int | None
    raw_bytes: int | None
    compressed_bytes: int | None
    last_error: str | None


@dataclass(frozen=True)
class RunArtifact:
    run_id: str
    kind: str
    sequence: int | None
    logical_name: str
    transport_name: str
    size: int
    sha256: str
    upload_status: str
    attempts: int
    request_id: str | None
    last_error: str | None
    uploaded_at: str | None
    cleanup_error: str | None
    created_at: str


_CREATE_TABLE_STATE = """
CREATE TABLE table_state (
    table_name TEXT PRIMARY KEY,
    last_snapshot_run_id TEXT,
    last_delivered_run_id TEXT,
    status TEXT NOT NULL,
    last_run_id TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)"""
_CREATE_SYNC_RUNS = """
CREATE TABLE sync_runs (
    run_id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    snapshot_completed_at TEXT,
    delivered_at TEXT,
    row_count INTEGER,
    chunk_count INTEGER,
    raw_bytes INTEGER,
    compressed_bytes INTEGER,
    last_error TEXT
)"""
_CREATE_ARTIFACTS = """
CREATE TABLE run_artifacts (
    run_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    sequence INTEGER,
    logical_name TEXT NOT NULL,
    transport_name TEXT NOT NULL,
    size INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    upload_status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    request_id TEXT,
    last_error TEXT,
    uploaded_at TEXT,
    cleanup_error TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, logical_name),
    UNIQUE (transport_name)
)"""


class SourceState:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        try:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(db_path, isolation_level=None, timeout=30)
            self._conn.row_factory = sqlite3.Row
        except (OSError, sqlite3.Error) as exc:
            raise StateError(f"cannot open state database at {db_path}: {exc}") from exc

    def initialize(self) -> None:
        try:
            with self._transaction():
                self._conn.execute(
                    "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)"
                )
                row = self._conn.execute("SELECT version FROM schema_version").fetchone()
                if row is None:
                    self._create_v3()
                    self._conn.execute("INSERT INTO schema_version VALUES (?)", (SCHEMA_VERSION,))
                    return
                version = int(row["version"])
                if version == 2:
                    self._migrate_v2_to_v3()
                elif version == 1:
                    self._conn.execute("DROP TABLE IF EXISTS table_state")
                    self._create_v3()
                    self._conn.execute("UPDATE schema_version SET version = 3")
                elif version != SCHEMA_VERSION:
                    raise StateError(
                        f"state database {self.db_path} has schema version {version}, "
                        f"but this program expects {SCHEMA_VERSION}"
                    )
                else:
                    self._create_v3(if_not_exists=True)
        except sqlite3.Error as exc:
            raise StateError(f"failed to initialize state database {self.db_path}: {exc}") from exc

    def _create_v3(self, *, if_not_exists: bool = False) -> None:
        suffix = " IF NOT EXISTS" if if_not_exists else ""
        for sql in (_CREATE_TABLE_STATE, _CREATE_SYNC_RUNS, _CREATE_ARTIFACTS):
            self._conn.execute(sql.replace("CREATE TABLE ", f"CREATE TABLE{suffix} ", 1))
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sync_runs_table_status ON sync_runs(table_name, status)"
        )

    def _migrate_v2_to_v3(self) -> None:
        self._conn.execute("ALTER TABLE table_state RENAME TO table_state_v2")
        self._create_v3()
        self._conn.execute(
            "INSERT INTO table_state "
            "(table_name,last_snapshot_run_id,last_delivered_run_id,status,last_run_id,"
            "last_error,created_at,updated_at) "
            "SELECT table_name,current_run_id,NULL,status,last_run_id,last_error,"
            "created_at,updated_at "
            "FROM table_state_v2"
        )
        self._conn.execute("DROP TABLE table_state_v2")
        self._conn.execute("UPDATE schema_version SET version = 3")

    def schema_version(self) -> int:
        try:
            row = self._conn.execute("SELECT version FROM schema_version").fetchone()
        except sqlite3.Error as exc:
            raise StateError(f"state database {self.db_path} is not initialized: {exc}") from exc
        if row is None:
            raise StateError(f"state database {self.db_path} is not initialized")
        return int(row["version"])

    def register_table(self, table_name: str) -> None:
        now = _utcnow_iso()
        self._execute(
            "INSERT INTO table_state VALUES (?,NULL,NULL,'IDLE',NULL,NULL,?,?) "
            "ON CONFLICT(table_name) DO UPDATE SET updated_at=excluded.updated_at",
            (table_name, now, now),
        )

    def begin_run(self, table_name: str, run_id: str) -> None:
        now = _utcnow_iso()
        with self._transaction():
            active = self._conn.execute(
                "SELECT run_id FROM sync_runs WHERE table_name=? AND status IN (?,?,?) LIMIT 1",
                (table_name, *_ACTIVE_RUN_STATUSES),
            ).fetchone()
            if active is not None:
                raise StateError(f"table '{table_name}' already has active run {active['run_id']}")
            cursor = self._conn.execute(
                "UPDATE table_state SET status=?,last_run_id=?,last_error=NULL,updated_at=? "
                "WHERE table_name=?",
                (TableStatus.RUNNING.value, run_id, now, table_name),
            )
            if cursor.rowcount == 0:
                raise StateError(f"table '{table_name}' is not registered")
            self._conn.execute(
                "INSERT INTO sync_runs (run_id,table_name,status,created_at) VALUES (?,?,?,?)",
                (run_id, table_name, RunStatus.GENERATING.value, now),
            )

    def set_run_status(self, run_id: str, status: RunStatus) -> None:
        self._execute("UPDATE sync_runs SET status=? WHERE run_id=?", (status.value, run_id))

    def complete_run(
        self,
        table_name: str,
        run_id: str,
        *,
        row_count: int | None = None,
        chunk_count: int | None = None,
        raw_bytes: int | None = None,
        compressed_bytes: int | None = None,
    ) -> None:
        now = _utcnow_iso()
        with self._transaction():
            cursor = self._conn.execute(
                "UPDATE sync_runs SET status=?,snapshot_completed_at=?,row_count=?,chunk_count=?,"
                "raw_bytes=?,compressed_bytes=?,last_error=NULL WHERE run_id=?",
                (
                    RunStatus.SNAPSHOT_READY.value,
                    now,
                    row_count,
                    chunk_count,
                    raw_bytes,
                    compressed_bytes,
                    run_id,
                ),
            )
            table_cursor = self._conn.execute(
                "UPDATE table_state SET status=?,last_snapshot_run_id=?,last_run_id=?,"
                "last_error=NULL,updated_at=? WHERE table_name=?",
                (TableStatus.COMPLETED.value, run_id, run_id, now, table_name),
            )
            if cursor.rowcount == 0 or table_cursor.rowcount == 0:
                raise StateError(f"table '{table_name}' is not registered")

    def deliver_run(
        self,
        table_name: str,
        run_id: str,
        *,
        row_count: int,
        chunk_count: int,
        raw_bytes: int,
        compressed_bytes: int,
    ) -> None:
        now = _utcnow_iso()
        with self._transaction():
            self._conn.execute(
                "UPDATE sync_runs SET status=?,"
                "snapshot_completed_at=COALESCE(snapshot_completed_at,?),"
                "delivered_at=?,row_count=?,chunk_count=?,raw_bytes=?,compressed_bytes=?,"
                "last_error=NULL WHERE run_id=?",
                (
                    RunStatus.DELIVERED.value,
                    now,
                    now,
                    row_count,
                    chunk_count,
                    raw_bytes,
                    compressed_bytes,
                    run_id,
                ),
            )
            self._conn.execute(
                "UPDATE table_state SET status=?,last_snapshot_run_id=?,last_delivered_run_id=?,"
                "last_run_id=?,last_error=NULL,updated_at=? WHERE table_name=?",
                (TableStatus.DELIVERED.value, run_id, run_id, run_id, now, table_name),
            )

    def fail_run(self, table_name: str, run_id: str, error: str, *, disk_pressure=False) -> None:
        run_status = RunStatus.DISK_PRESSURE if disk_pressure else RunStatus.FAILED
        table_status = TableStatus.DISK_PRESSURE if disk_pressure else TableStatus.FAILED
        now = _utcnow_iso()
        with self._transaction():
            self._conn.execute(
                "UPDATE sync_runs SET status=?,last_error=? WHERE run_id=?",
                (run_status.value, error, run_id),
            )
            cursor = self._conn.execute(
                "UPDATE table_state SET status=?,last_run_id=?,last_error=?,updated_at=? "
                "WHERE table_name=?",
                (table_status.value, run_id, error, now, table_name),
            )
            if cursor.rowcount == 0:
                raise StateError(f"table '{table_name}' is not registered")

    def register_artifact(
        self,
        run_id: str,
        kind: str,
        sequence: int | None,
        logical_name: str,
        transport_name: str,
        size: int,
        sha256: str,
    ) -> None:
        self._execute(
            "INSERT INTO run_artifacts "
            "(run_id,kind,sequence,logical_name,transport_name,size,sha256,"
            "upload_status,created_at) "
            "VALUES (?,?,?,?,?,?,?,'PENDING',?)",
            (run_id, kind, sequence, logical_name, transport_name, size, sha256, _utcnow_iso()),
        )

    def mark_upload_attempt(self, run_id: str, logical_name: str, attempt: int) -> None:
        self._execute(
            "UPDATE run_artifacts SET upload_status='UPLOADING',attempts=? "
            "WHERE run_id=? AND logical_name=?",
            (attempt, run_id, logical_name),
        )

    def mark_uploaded(self, run_id: str, logical_name: str, attempts: int, request_id: str) -> None:
        self._execute(
            "UPDATE run_artifacts SET upload_status='UPLOADED',attempts=?,request_id=?,"
            "last_error=NULL,uploaded_at=? WHERE run_id=? AND logical_name=?",
            (attempts, request_id, _utcnow_iso(), run_id, logical_name),
        )

    def mark_artifact_failed(
        self, run_id: str, logical_name: str, attempts: int, error: str
    ) -> None:
        self._execute(
            "UPDATE run_artifacts SET upload_status='FAILED',attempts=?,last_error=? "
            "WHERE run_id=? AND logical_name=?",
            (attempts, error, run_id, logical_name),
        )

    def mark_cleanup_error(self, run_id: str, logical_name: str, error: str | None) -> None:
        self._execute(
            "UPDATE run_artifacts SET cleanup_error=? WHERE run_id=? AND logical_name=?",
            (error, run_id, logical_name),
        )

    def get_table_state(self, table_name: str) -> TableState | None:
        row = self._fetchone("SELECT * FROM table_state WHERE table_name=?", (table_name,))
        return TableState(**dict(row)) if row is not None else None

    def get_run(self, run_id: str) -> SyncRun | None:
        row = self._fetchone("SELECT * FROM sync_runs WHERE run_id=?", (run_id,))
        return SyncRun(**dict(row)) if row is not None else None

    def get_artifacts(self, run_id: str) -> list[RunArtifact]:
        try:
            rows = self._conn.execute(
                "SELECT * FROM run_artifacts WHERE run_id=? ORDER BY "
                "CASE kind WHEN 'chunk' THEN 1 WHEN 'schema' THEN 2 ELSE 3 END, sequence",
                (run_id,),
            ).fetchall()
        except sqlite3.Error as exc:
            raise StateError(f"failed to read artifacts: {exc}") from exc
        return [RunArtifact(**dict(row)) for row in rows]

    def _execute(self, sql: str, params: tuple[object, ...]) -> None:
        try:
            with self._transaction():
                self._conn.execute(sql, params)
        except sqlite3.Error as exc:
            raise StateError(f"state database operation failed: {exc}") from exc

    def _fetchone(self, sql: str, params: tuple[object, ...]) -> sqlite3.Row | None:
        try:
            return self._conn.execute(sql, params).fetchone()
        except sqlite3.Error as exc:
            raise StateError(f"state database query failed: {exc}") from exc

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        # 先取得写锁，使跨进程同表 begin_run 串行检查，而不是两个 DEFERRED
        # transaction 同时观察到“无 active run”。
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        self._conn.execute("COMMIT")

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> SourceState:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
