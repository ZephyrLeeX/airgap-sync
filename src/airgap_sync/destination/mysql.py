"""Destination MySQL 受控写入、验证、切换与 metadata API。"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pymysql
import pymysql.cursors

from airgap_sync.common.config import resolve_password
from airgap_sync.common.manifest import Manifest
from airgap_sync.common.models import DestinationConfig, MySQLConfig

METADATA_SCHEMA_VERSION = 2
_SET_SESSION_TIME_ZONE_SQL = "SET SESSION time_zone = '+00:00'"


class DestinationMySQLError(Exception):
    """Destination MySQL 连接或受控操作失败。"""


def quote_identifier(name: str) -> str:
    if not name:
        raise DestinationMySQLError("identifier must not be empty")
    if len(name) > 64:
        raise DestinationMySQLError(f"identifier exceeds MySQL 64-character limit: {name!r}")
    return "`" + name.replace("`", "``") + "`"


def _derived_name(prefix: str, run_id: str) -> str:
    safe = "".join(char if char.isalnum() else "_" for char in run_id).lower()
    digest = hashlib.sha256(run_id.encode()).hexdigest()[:8]
    return f"{prefix}{safe[:40]}_{digest}"[:64]


def staging_table_name(run_id: str) -> str:
    return _derived_name("__airgap_stg_", run_id)


def backup_table_name(run_id: str) -> str:
    return _derived_name("__airgap_old_", run_id)


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


@dataclass(frozen=True)
class DestinationColumn:
    name: str
    ordinal_position: int
    extra: str
    generation_expression: str

    @property
    def generated(self) -> bool:
        return bool(self.generation_expression) or "GENERATED" in self.extra.upper()


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    source_database: str
    table_name: str
    status: str
    row_count: int
    chunk_count: int
    staging_table: str
    source_created_at: datetime | None
    target_existed: bool | None = None
    backup_table: str | None = None
    cleanup_error: str | None = None
    backup_cleanup_error: str | None = None


@dataclass(frozen=True)
class TableVersion:
    run_id: str
    source_database: str
    table_name: str
    source_created_at: datetime
    row_count: int
    previous_run_id: str | None
    previous_row_count: int | None
    net_change: int | None
    verified_at: datetime
    applied_at: datetime


class DestinationMySQLConnection:
    """不公开任意 SQL，只暴露同步协议所需的固定操作。"""

    def __init__(self, mysql: MySQLConfig, destination: DestinationConfig) -> None:
        self._config = mysql
        self._destination = destination
        self._metadata = quote_identifier(destination.metadata_database)
        self._conn: pymysql.Connection | None = None

    def connect(self) -> None:
        if self._conn is not None:
            return
        password = resolve_password(self._config)
        try:
            self._conn = pymysql.connect(
                host=self._config.host,
                port=self._config.port,
                user=self._config.user,
                password=password,
                database=self._config.database,
                connect_timeout=self._config.connect_timeout,
                charset="utf8mb4",
                autocommit=True,
            )
            with self._conn.cursor() as cursor:
                cursor.execute(_SET_SESSION_TIME_ZONE_SQL)
        except pymysql.Error as exc:
            self.close()
            raise DestinationMySQLError(
                f"cannot initialize Destination MySQL {self._config.host}:{self._config.port} "
                f"database '{self._config.database}' as user '{self._config.user}': {exc}"
            ) from exc

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> DestinationMySQLConnection:
        self.connect()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _cursor(self) -> Any:
        if self._conn is None:
            raise DestinationMySQLError("connection is not open; call connect() first")
        return self._conn.cursor()

    def _execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        try:
            with self._cursor() as cursor:
                return int(cursor.execute(sql, params))
        except pymysql.Error as exc:
            raise DestinationMySQLError(f"Destination MySQL operation failed: {exc}") from exc

    def _fetchone(self, sql: str, params: tuple[Any, ...] = ()) -> tuple[Any, ...] | None:
        try:
            with self._cursor() as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone()
                return None if row is None else tuple(row)
        except pymysql.Error as exc:
            raise DestinationMySQLError(f"Destination MySQL query failed: {exc}") from exc

    def _fetchall(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        try:
            with self._cursor() as cursor:
                cursor.execute(sql, params)
                return [tuple(row) for row in cursor.fetchall()]
        except pymysql.Error as exc:
            raise DestinationMySQLError(f"Destination MySQL query failed: {exc}") from exc

    def ping(self) -> str:
        row = self._fetchone("SELECT VERSION()")
        assert row is not None
        return str(row[0])

    def initialize_metadata(self) -> None:
        self._execute(f"CREATE DATABASE IF NOT EXISTS {self._metadata} CHARACTER SET utf8mb4")
        self._execute(
            f"CREATE TABLE IF NOT EXISTS {self._metadata}.schema_version ("
            "singleton TINYINT NOT NULL PRIMARY KEY, version INT NOT NULL) ENGINE=InnoDB"
        )
        self._execute(
            f"CREATE TABLE IF NOT EXISTS {self._metadata}.runs ("
            "run_id VARCHAR(64) NOT NULL PRIMARY KEY, source_database VARCHAR(64) NOT NULL,"
            "table_name VARCHAR(64) NOT NULL, status VARCHAR(24) NOT NULL,"
            "protocol_version INT NOT NULL, row_count BIGINT UNSIGNED NOT NULL,"
            "chunk_count INT UNSIGNED NOT NULL, staging_table VARCHAR(64) NOT NULL,"
            "source_created_at DATETIME(6) NOT NULL, expected_digest_a CHAR(64) NOT NULL,"
            "expected_digest_b CHAR(64) NOT NULL, actual_row_count BIGINT UNSIGNED NULL,"
            "actual_digest_a CHAR(64) NULL, actual_digest_b CHAR(64) NULL,"
            "digest_verified_at DATETIME(6) NULL, target_existed BOOLEAN NULL,"
            "backup_table VARCHAR(64) NULL, applied_at DATETIME(6) NULL,"
            "cleanup_error TEXT NULL, backup_cleanup_error TEXT NULL,"
            "manifest_received_at DATETIME(6) NOT NULL, validated_at DATETIME(6) NULL,"
            "import_started_at DATETIME(6) NULL, import_completed_at DATETIME(6) NULL,"
            "last_error TEXT NULL, created_at DATETIME(6) NOT NULL, "
            "updated_at DATETIME(6) NOT NULL,"
            "KEY idx_runs_table_created "
            "(source_database,table_name,source_created_at)) ENGINE=InnoDB"
        )
        self._execute(
            f"CREATE TABLE IF NOT EXISTS {self._metadata}.chunks ("
            "run_id VARCHAR(64) NOT NULL, sequence INT UNSIGNED NOT NULL,"
            "logical_name VARCHAR(96) NOT NULL, expected_rows BIGINT UNSIGNED NOT NULL,"
            "compressed_bytes BIGINT UNSIGNED NOT NULL, sha256 CHAR(64) NOT NULL,"
            "status VARCHAR(16) NOT NULL, imported_rows BIGINT UNSIGNED NULL,"
            "imported_at DATETIME NULL, last_error TEXT NULL, PRIMARY KEY (run_id,sequence),"
            "CONSTRAINT fk_chunks_run FOREIGN KEY (run_id) "
            f"REFERENCES {self._metadata}.runs(run_id)) ENGINE=InnoDB"
        )
        self._execute(
            f"CREATE TABLE IF NOT EXISTS {self._metadata}.table_versions ("
            "run_id VARCHAR(64) NOT NULL PRIMARY KEY, source_database VARCHAR(64) NOT NULL,"
            "table_name VARCHAR(64) NOT NULL, source_created_at DATETIME(6) NOT NULL,"
            "row_count BIGINT UNSIGNED NOT NULL, previous_run_id VARCHAR(64) NULL,"
            "previous_row_count BIGINT UNSIGNED NULL, net_change BIGINT NULL,"
            "verified_at DATETIME(6) NOT NULL, applied_at DATETIME(6) NOT NULL,"
            "KEY idx_versions_table_time "
            "(source_database,table_name,source_created_at)) ENGINE=InnoDB"
        )
        self._execute(f"INSERT IGNORE INTO {self._metadata}.schema_version VALUES (1,2)")
        row = self._fetchone(
            f"SELECT version FROM {self._metadata}.schema_version WHERE singleton=1"
        )
        if row is not None and int(row[0]) == 1:
            self._migrate_v1_to_v2()
            row = (2,)
        if row is None or int(row[0]) != METADATA_SCHEMA_VERSION:
            got = "missing" if row is None else str(row[0])
            raise DestinationMySQLError(
                f"metadata schema version is {got}, expected {METADATA_SCHEMA_VERSION}"
            )

    def _migrate_v1_to_v2(self) -> None:
        additions = [
            "source_created_at DATETIME(6) NULL",
            "expected_digest_a CHAR(64) NULL",
            "expected_digest_b CHAR(64) NULL",
            "actual_row_count BIGINT UNSIGNED NULL",
            "actual_digest_a CHAR(64) NULL",
            "actual_digest_b CHAR(64) NULL",
            "digest_verified_at DATETIME(6) NULL",
            "target_existed BOOLEAN NULL",
            "backup_table VARCHAR(64) NULL",
            "applied_at DATETIME(6) NULL",
            "cleanup_error TEXT NULL",
            "backup_cleanup_error TEXT NULL",
        ]
        existing = {
            str(row[0]).lower()
            for row in self._fetchall(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME='runs'",
                (self._destination.metadata_database,),
            )
        }
        for definition in additions:
            if definition.split(None, 1)[0].lower() not in existing:
                self._execute(f"ALTER TABLE {self._metadata}.runs ADD COLUMN {definition}")
        self._execute(f"ALTER TABLE {self._metadata}.runs MODIFY status VARCHAR(24) NOT NULL")
        self._execute(f"UPDATE {self._metadata}.schema_version SET version=2 WHERE singleton=1")

    def metadata_schema_version(self) -> int:
        row = self._fetchone(
            f"SELECT version FROM {self._metadata}.schema_version WHERE singleton=1"
        )
        if row is None:
            raise DestinationMySQLError("metadata schema is not initialized")
        return int(row[0])

    def _lock(self, prefix: str, identity: str) -> bool:
        name = prefix + hashlib.sha256(identity.encode()).hexdigest()[:40]
        row = self._fetchone("SELECT GET_LOCK(%s,0)", (name,))
        return row is not None and int(row[0] or 0) == 1

    def _unlock(self, prefix: str, identity: str) -> None:
        name = prefix + hashlib.sha256(identity.encode()).hexdigest()[:40]
        self._fetchone("SELECT RELEASE_LOCK(%s)", (name,))

    def acquire_run_lock(self, run_id: str) -> bool:
        return self._lock("airgap:", run_id)

    def release_run_lock(self, run_id: str) -> None:
        self._unlock("airgap:", run_id)

    def acquire_table_lock(self, source_database: str, table_name: str) -> bool:
        return self._lock("airgap-table:", f"{source_database}\0{table_name}")

    def release_table_lock(self, source_database: str, table_name: str) -> None:
        self._unlock("airgap-table:", f"{source_database}\0{table_name}")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        if self._conn is None:
            raise DestinationMySQLError("connection is not open; call connect() first")
        try:
            self._conn.begin()
            yield
            self._conn.commit()
        except BaseException:
            self._conn.rollback()
            raise

    def get_run(self, run_id: str) -> RunRecord | None:
        row = self._fetchone(
            f"SELECT run_id,source_database,table_name,status,row_count,chunk_count,staging_table,"
            "source_created_at,target_existed,backup_table,cleanup_error,backup_cleanup_error "
            f"FROM {self._metadata}.runs WHERE run_id=%s",
            (run_id,),
        )
        if row is None:
            return None
        return RunRecord(
            str(row[0]),
            str(row[1]),
            str(row[2]),
            str(row[3]),
            int(row[4]),
            int(row[5]),
            str(row[6]),
            row[7],
            None if row[8] is None else bool(row[8]),
            None if row[9] is None else str(row[9]),
            None if row[10] is None else str(row[10]),
            None if row[11] is None else str(row[11]),
        )

    def register_validated_run(
        self, manifest: Manifest, staging_table: str, source_created_at: datetime
    ) -> None:
        existing = self.get_run(manifest.run_id)
        identity = (
            manifest.source.database,
            manifest.source.table,
            manifest.row_count,
            len(manifest.chunks),
            staging_table,
        )
        if existing is not None:
            stored = (
                existing.source_database,
                existing.table_name,
                existing.row_count,
                existing.chunk_count,
                existing.staging_table,
            )
            if stored != identity:
                raise DestinationMySQLError(
                    "STAGING_STATE_MISMATCH: run metadata differs from manifest"
                )
            contract = self._fetchone(
                f"SELECT source_created_at,expected_digest_a,expected_digest_b "
                f"FROM {self._metadata}.runs WHERE run_id=%s",
                (manifest.run_id,),
            )
            if contract is None:
                raise DestinationMySQLError("STAGING_STATE_MISMATCH: run metadata is missing")
            created = source_created_at.astimezone(UTC).replace(tzinfo=None)
            expected = (manifest.verification.digest_a, manifest.verification.digest_b)
            if contract[0] is not None and contract[0] != created:
                raise DestinationMySQLError(
                    "STAGING_STATE_MISMATCH: source_created_at differs from manifest"
                )
            if contract[1] is not None and (str(contract[1]), str(contract[2])) != expected:
                raise DestinationMySQLError(
                    "STAGING_STATE_MISMATCH: expected digest differs from manifest"
                )
            stored_chunks = self._fetchall(
                f"SELECT sequence,logical_name,expected_rows,compressed_bytes,sha256 "
                f"FROM {self._metadata}.chunks WHERE run_id=%s ORDER BY sequence",
                (manifest.run_id,),
            )
            expected_chunks = [
                (chunk.sequence, chunk.file, chunk.rows, chunk.compressed_bytes, chunk.sha256)
                for chunk in manifest.chunks
            ]
            if stored_chunks != expected_chunks:
                raise DestinationMySQLError(
                    "STAGING_STATE_MISMATCH: chunk metadata differs from manifest"
                )
            # schema v1 的既有 Run 在首次 Phase 5 重试时补齐不可推导字段。
            self._execute(
                f"UPDATE {self._metadata}.runs SET source_created_at=COALESCE("
                "source_created_at,%s),expected_digest_a=COALESCE(expected_digest_a,%s),"
                "expected_digest_b=COALESCE(expected_digest_b,%s),updated_at=%s "
                "WHERE run_id=%s",
                (created, expected[0], expected[1], _utcnow(), manifest.run_id),
            )
            return
        now = _utcnow()
        created = source_created_at.astimezone(UTC).replace(tzinfo=None)
        with self.transaction():
            self._execute(
                f"INSERT INTO {self._metadata}.runs (run_id,source_database,table_name,status,"
                "protocol_version,row_count,chunk_count,staging_table,source_created_at,"
                "expected_digest_a,expected_digest_b,manifest_received_at,validated_at,created_at,"
                "updated_at) VALUES (%s,%s,%s,'VALIDATED',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    manifest.run_id,
                    manifest.source.database,
                    manifest.source.table,
                    manifest.protocol_version,
                    manifest.row_count,
                    len(manifest.chunks),
                    staging_table,
                    created,
                    manifest.verification.digest_a,
                    manifest.verification.digest_b,
                    now,
                    now,
                    now,
                    now,
                ),
            )
            for chunk in manifest.chunks:
                self._execute(
                    f"INSERT INTO {self._metadata}.chunks (run_id,sequence,logical_name,"
                    "expected_rows,compressed_bytes,sha256,status) "
                    "VALUES (%s,%s,%s,%s,%s,%s,'PENDING')",
                    (
                        manifest.run_id,
                        chunk.sequence,
                        chunk.file,
                        chunk.rows,
                        chunk.compressed_bytes,
                        chunk.sha256,
                    ),
                )

    def object_type(self, database: str, table: str) -> str | None:
        row = self._fetchone(
            "SELECT TABLE_TYPE FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
            (database, table),
        )
        return None if row is None else str(row[0]).upper()

    def table_exists(self, database: str, table: str) -> bool:
        return self.object_type(database, table) == "BASE TABLE"

    def storage_engine(self, database: str, table: str) -> str | None:
        row = self._fetchone(
            "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s "
            "AND TABLE_NAME=%s AND TABLE_TYPE='BASE TABLE'",
            (database, table),
        )
        return None if row is None else str(row[0])

    def create_staging_table(self, rewritten_ddl: str) -> None:
        self._execute(rewritten_ddl)

    def staging_columns(self, staging_table: str) -> list[DestinationColumn]:
        rows = self._fetchall(
            "SELECT COLUMN_NAME,ORDINAL_POSITION,EXTRA,COALESCE(GENERATION_EXPRESSION,'') "
            "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s "
            "ORDER BY ORDINAL_POSITION",
            (self._config.database, staging_table),
        )
        return [DestinationColumn(str(a), int(b), str(c), str(d)) for a, b, c, d in rows]

    def chunk_status(self, run_id: str, sequence: int) -> str:
        row = self._fetchone(
            f"SELECT status FROM {self._metadata}.chunks WHERE run_id=%s AND sequence=%s",
            (run_id, sequence),
        )
        if row is None:
            raise DestinationMySQLError("STAGING_STATE_MISMATCH: chunk metadata is missing")
        return str(row[0])

    def has_imported_chunks(self, run_id: str) -> bool:
        return (
            self._fetchone(
                f"SELECT 1 FROM {self._metadata}.chunks WHERE run_id=%s "
                "AND status='IMPORTED' LIMIT 1",
                (run_id,),
            )
            is not None
        )

    def set_chunk_importing(self, run_id: str, sequence: int) -> None:
        self._execute(
            f"UPDATE {self._metadata}.chunks SET status='IMPORTING',last_error=NULL "
            "WHERE run_id=%s AND sequence=%s",
            (run_id, sequence),
        )

    def insert_rows(self, table: str, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return
        target = quote_identifier(table)
        if columns:
            column_sql = ",".join(quote_identifier(column) for column in columns)
            placeholders = ",".join(["%s"] * len(columns))
            sql = f"INSERT INTO {target} ({column_sql}) VALUES ({placeholders})"
        else:
            sql = f"INSERT INTO {target} () VALUES ()"
        try:
            with self._cursor() as cursor:
                cursor.executemany(sql, rows)
        except pymysql.Error as exc:
            raise DestinationMySQLError(f"staging batch insert failed: {exc}") from exc

    def set_chunk_imported(self, run_id: str, sequence: int, rows: int) -> None:
        self._execute(
            f"UPDATE {self._metadata}.chunks SET status='IMPORTED',imported_rows=%s,"
            "imported_at=%s,last_error=NULL WHERE run_id=%s AND sequence=%s",
            (rows, _utcnow(), run_id, sequence),
        )

    def fail_chunk(self, run_id: str, sequence: int, error: str) -> None:
        self._execute(
            f"UPDATE {self._metadata}.chunks SET status='FAILED',last_error=%s "
            "WHERE run_id=%s AND sequence=%s",
            (error, run_id, sequence),
        )

    def begin_import(self, run_id: str) -> None:
        now = _utcnow()
        self._execute(
            f"UPDATE {self._metadata}.runs SET status='IMPORTING',"
            "import_started_at=COALESCE(import_started_at,%s),last_error=NULL,updated_at=%s "
            "WHERE run_id=%s",
            (now, now, run_id),
        )

    def complete_staged(self, run_id: str, expected_rows: int) -> None:
        row = self._fetchone(
            f"SELECT COUNT(*),COALESCE(SUM(imported_rows),0) FROM {self._metadata}.chunks "
            "WHERE run_id=%s AND status='IMPORTED'",
            (run_id,),
        )
        run = self.get_run(run_id)
        if (
            row is None
            or run is None
            or int(row[0]) != run.chunk_count
            or int(row[1]) != expected_rows
        ):
            raise DestinationMySQLError(
                "STAGING_STATE_MISMATCH: imported chunk totals are inconsistent"
            )
        now = _utcnow()
        self._execute(
            f"UPDATE {self._metadata}.runs SET status='STAGED',import_completed_at=%s,"
            "last_error=NULL,updated_at=%s WHERE run_id=%s",
            (now, now, run_id),
        )

    def set_verifying(self, run_id: str) -> None:
        self._execute(
            f"UPDATE {self._metadata}.runs SET status='VERIFYING',last_error=NULL,updated_at=%s "
            "WHERE run_id=%s",
            (_utcnow(), run_id),
        )

    @contextmanager
    def stream_table_columns(
        self, table: str, columns: list[str], fetch_size: int
    ) -> Iterator[Iterator[tuple[Any, ...]]]:
        if self._conn is None:
            raise DestinationMySQLError("connection is not open; call connect() first")
        column_sql = ",".join(quote_identifier(column) for column in columns)
        sql = f"SELECT {column_sql} FROM {quote_identifier(table)}"
        cursor = self._conn.cursor(pymysql.cursors.SSCursor)
        try:
            cursor.execute(sql)

            def rows() -> Iterator[tuple[Any, ...]]:
                while batch := cursor.fetchmany(fetch_size):
                    yield from (tuple(row) for row in batch)

            yield rows()
        except pymysql.Error as exc:
            raise DestinationMySQLError(f"streaming verification query failed: {exc}") from exc
        finally:
            cursor.close()

    def record_verification(self, run_id: str, summary: Any, matched: bool) -> None:
        status = "STAGED" if matched else "MISMATCH"
        now = _utcnow()
        self._execute(
            f"UPDATE {self._metadata}.runs SET status=%s,actual_row_count=%s,actual_digest_a=%s,"
            "actual_digest_b=%s,digest_verified_at=%s,last_error=NULL,updated_at=%s "
            "WHERE run_id=%s",
            (status, summary.row_count, summary.digest_a, summary.digest_b, now, now, run_id),
        )

    def latest_version(self, source_database: str, table_name: str) -> TableVersion | None:
        rows = self._fetchall(
            f"SELECT run_id,source_database,table_name,source_created_at,row_count,"
            "previous_run_id,previous_row_count,net_change,verified_at,applied_at "
            f"FROM {self._metadata}.table_versions WHERE source_database=%s AND table_name=%s "
            "ORDER BY source_created_at DESC,applied_at DESC LIMIT 1",
            (source_database, table_name),
        )
        return None if not rows else self._version(rows[0])

    @staticmethod
    def _version(row: tuple[Any, ...]) -> TableVersion:
        return TableVersion(
            str(row[0]),
            str(row[1]),
            str(row[2]),
            row[3],
            int(row[4]),
            None if row[5] is None else str(row[5]),
            None if row[6] is None else int(row[6]),
            None if row[7] is None else int(row[7]),
            row[8],
            row[9],
        )

    def set_superseded(self, run_id: str) -> None:
        self._execute(
            f"UPDATE {self._metadata}.runs SET status='SUPERSEDED',last_error=NULL,updated_at=%s "
            "WHERE run_id=%s",
            (_utcnow(), run_id),
        )

    def target_dependencies(self, database: str, table: str) -> tuple[bool, bool]:
        fk = self._fetchone(
            "SELECT 1 FROM information_schema.KEY_COLUMN_USAGE WHERE "
            "(TABLE_SCHEMA=%s AND TABLE_NAME=%s AND REFERENCED_TABLE_NAME IS NOT NULL) OR "
            "(REFERENCED_TABLE_SCHEMA=%s AND REFERENCED_TABLE_NAME=%s) LIMIT 1",
            (database, table, database, table),
        )
        trigger = self._fetchone(
            "SELECT 1 FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA=%s "
            "AND EVENT_OBJECT_TABLE=%s LIMIT 1",
            (database, table),
        )
        return fk is not None, trigger is not None

    def prepare_swapping(self, run_id: str, target_existed: bool, backup_table: str | None) -> None:
        self._execute(
            f"UPDATE {self._metadata}.runs SET status='SWAPPING',target_existed=%s,"
            "backup_table=%s,last_error=NULL,updated_at=%s WHERE run_id=%s",
            (target_existed, backup_table, _utcnow(), run_id),
        )

    def rename_for_promotion(
        self, database: str, staging: str, target: str, backup: str | None
    ) -> None:
        db = quote_identifier(database)
        stg = f"{db}.{quote_identifier(staging)}"
        live = f"{db}.{quote_identifier(target)}"
        if backup is None:
            sql = f"RENAME TABLE {stg} TO {live}"
        else:
            old = f"{db}.{quote_identifier(backup)}"
            sql = f"RENAME TABLE {live} TO {old}, {stg} TO {live}"
        self._execute(sql)

    def finalize_verified(self, run_id: str, expected_previous_run_id: str | None) -> None:
        now = _utcnow()
        with self.transaction():
            run = self.get_run(run_id)
            if run is None:
                raise DestinationMySQLError("run metadata is missing")
            if run.source_created_at is None:
                raise DestinationMySQLError("run source_created_at is missing")
            previous = self.latest_version(run.source_database, run.table_name)
            if previous is not None and previous.run_id == run_id:
                self._execute(
                    f"UPDATE {self._metadata}.runs SET status='VERIFIED',applied_at=%s,"
                    "last_error=NULL,updated_at=%s WHERE run_id=%s",
                    (now, now, run_id),
                )
                return
            actual_previous_run_id = None if previous is None else previous.run_id
            if actual_previous_run_id != expected_previous_run_id:
                raise DestinationMySQLError(
                    "FINALIZE_VERSION_CONFLICT: latest table version changed before finalize"
                )
            if previous is not None:
                run_time = _utc_naive(run.source_created_at)
                previous_time = _utc_naive(previous.source_created_at)
                if run_time < previous_time:
                    raise DestinationMySQLError(
                        "FINALIZE_VERSION_CONFLICT: run is older than latest table version"
                    )
                if run_time == previous_time:
                    raise DestinationMySQLError(
                        "RUN_ORDER_AMBIGUOUS: another run has the same source_created_at"
                    )
            previous_rows = None if previous is None else previous.row_count
            net_change = None if previous_rows is None else run.row_count - previous_rows
            self._execute(
                f"UPDATE {self._metadata}.runs SET status='VERIFIED',applied_at=%s,"
                "last_error=NULL,updated_at=%s WHERE run_id=%s",
                (now, now, run_id),
            )
            self._execute(
                f"INSERT INTO {self._metadata}.table_versions (run_id,source_database,table_name,"
                "source_created_at,row_count,previous_run_id,previous_row_count,net_change,"
                "verified_at,applied_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    run.run_id,
                    run.source_database,
                    run.table_name,
                    run.source_created_at,
                    run.row_count,
                    None if previous is None else previous.run_id,
                    previous_rows,
                    net_change,
                    now,
                    now,
                ),
            )

    def drop_table(self, database: str, table: str) -> None:
        self._execute(f"DROP TABLE {quote_identifier(database)}.{quote_identifier(table)}")

    def record_cleanup_error(self, run_id: str, error: str | None) -> None:
        self._execute(
            f"UPDATE {self._metadata}.runs SET cleanup_error=%s,updated_at=%s WHERE run_id=%s",
            (error, _utcnow(), run_id),
        )

    def record_backup_cleanup_error(self, run_id: str, error: str | None) -> None:
        self._execute(
            f"UPDATE {self._metadata}.runs SET backup_cleanup_error=%s,updated_at=%s "
            "WHERE run_id=%s",
            (error, _utcnow(), run_id),
        )

    def fail_run(self, run_id: str, error: str) -> None:
        self._execute(
            f"UPDATE {self._metadata}.runs SET status='FAILED',last_error=%s,updated_at=%s "
            "WHERE run_id=%s AND status<>'SWAPPING'",
            (error, _utcnow(), run_id),
        )

    def all_versions(self) -> list[TableVersion]:
        rows = self._fetchall(
            f"SELECT run_id,source_database,table_name,source_created_at,row_count,"
            "previous_run_id,previous_row_count,net_change,verified_at,applied_at "
            f"FROM {self._metadata}.table_versions ORDER BY source_database,table_name,"
            "source_created_at"
        )
        return [self._version(row) for row in rows]
