"""Destination MySQL 写连接与受控 metadata/staging 操作。"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pymysql

from airgap_sync.common.config import resolve_password
from airgap_sync.common.manifest import Manifest
from airgap_sync.common.models import DestinationConfig, MySQLConfig

METADATA_SCHEMA_VERSION = 1


class DestinationMySQLError(Exception):
    """Destination MySQL 连接或受控操作失败。"""


def quote_identifier(name: str) -> str:
    if not name:
        raise DestinationMySQLError("identifier must not be empty")
    if len(name) > 64:
        raise DestinationMySQLError(f"identifier exceeds MySQL 64-character limit: {name!r}")
    return "`" + name.replace("`", "``") + "`"


def staging_table_name(run_id: str) -> str:
    """生成与源表名无关、确定且不超过 64 字符的 staging 名称。"""
    safe = "".join(char if char.isalnum() else "_" for char in run_id).lower()
    digest = hashlib.sha256(run_id.encode()).hexdigest()[:8]
    return f"__airgap_stg_{safe[:40]}_{digest}"[:64]


def _utcnow() -> str:
    return datetime.now(UTC).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")


@dataclass(frozen=True)
class DestinationColumn:
    name: str
    ordinal_position: int
    extra: str
    generation_expression: str

    @property
    def generated(self) -> bool:
        return bool(self.generation_expression) or "GENERATED" in self.extra.upper()


class DestinationMySQLConnection:
    """独立的 Destination 写连接；只暴露应用所需的受控操作。"""

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
        except pymysql.Error as exc:
            raise DestinationMySQLError(
                f"cannot connect to Destination MySQL {self._config.host}:{self._config.port} "
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
        """创建 metadata schema v1；不引入 ORM 或通用用户 SQL API。"""
        self._execute(f"CREATE DATABASE IF NOT EXISTS {self._metadata} CHARACTER SET utf8mb4")
        self._execute(
            f"CREATE TABLE IF NOT EXISTS {self._metadata}.schema_version ("
            "singleton TINYINT NOT NULL PRIMARY KEY, version INT NOT NULL) ENGINE=InnoDB"
        )
        self._execute(
            f"CREATE TABLE IF NOT EXISTS {self._metadata}.runs ("
            "run_id VARCHAR(64) NOT NULL PRIMARY KEY, source_database VARCHAR(64) NOT NULL,"
            "table_name VARCHAR(64) NOT NULL, status VARCHAR(16) NOT NULL,"
            "protocol_version INT NOT NULL, row_count BIGINT UNSIGNED NOT NULL,"
            "chunk_count INT UNSIGNED NOT NULL, staging_table VARCHAR(64) NOT NULL,"
            "manifest_received_at DATETIME NOT NULL, validated_at DATETIME NULL,"
            "import_started_at DATETIME NULL, import_completed_at DATETIME NULL,"
            "last_error TEXT NULL, created_at DATETIME NOT NULL, updated_at DATETIME NOT NULL,"
            "KEY idx_runs_table_created (source_database,table_name,created_at)) ENGINE=InnoDB"
        )
        self._execute(
            f"CREATE TABLE IF NOT EXISTS {self._metadata}.chunks ("
            "run_id VARCHAR(64) NOT NULL, sequence INT UNSIGNED NOT NULL,"
            "logical_name VARCHAR(96) NOT NULL, expected_rows BIGINT UNSIGNED NOT NULL,"
            "compressed_bytes BIGINT UNSIGNED NOT NULL, sha256 CHAR(64) NOT NULL,"
            "status VARCHAR(16) NOT NULL, imported_rows BIGINT UNSIGNED NULL,"
            "imported_at DATETIME NULL, last_error TEXT NULL,"
            "PRIMARY KEY (run_id,sequence), CONSTRAINT fk_chunks_run FOREIGN KEY (run_id) "
            f"REFERENCES {self._metadata}.runs(run_id)) ENGINE=InnoDB"
        )
        self._execute(
            f"INSERT IGNORE INTO {self._metadata}.schema_version VALUES (1,%s)",
            (METADATA_SCHEMA_VERSION,),
        )
        row = self._fetchone(
            f"SELECT version FROM {self._metadata}.schema_version WHERE singleton=1"
        )
        if row is None or int(row[0]) != METADATA_SCHEMA_VERSION:
            got = "missing" if row is None else str(row[0])
            raise DestinationMySQLError(
                f"metadata schema version is {got}, expected {METADATA_SCHEMA_VERSION}"
            )

    def metadata_schema_version(self) -> int:
        row = self._fetchone(
            f"SELECT version FROM {self._metadata}.schema_version WHERE singleton=1"
        )
        if row is None:
            raise DestinationMySQLError("metadata schema is not initialized")
        return int(row[0])

    def acquire_run_lock(self, run_id: str) -> bool:
        lock = "airgap:" + hashlib.sha256(run_id.encode()).hexdigest()[:48]
        row = self._fetchone("SELECT GET_LOCK(%s,0)", (lock,))
        return row is not None and int(row[0] or 0) == 1

    def release_run_lock(self, run_id: str) -> None:
        lock = "airgap:" + hashlib.sha256(run_id.encode()).hexdigest()[:48]
        self._fetchone("SELECT RELEASE_LOCK(%s)", (lock,))

    def acquire_table_lock(self, source_database: str, table_name: str) -> bool:
        identity = f"{source_database}\0{table_name}"
        lock = "airgap-table:" + hashlib.sha256(identity.encode()).hexdigest()[:40]
        row = self._fetchone("SELECT GET_LOCK(%s,0)", (lock,))
        return row is not None and int(row[0] or 0) == 1

    def release_table_lock(self, source_database: str, table_name: str) -> None:
        identity = f"{source_database}\0{table_name}"
        lock = "airgap-table:" + hashlib.sha256(identity.encode()).hexdigest()[:40]
        self._fetchone("SELECT RELEASE_LOCK(%s)", (lock,))

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

    def get_run(self, run_id: str) -> tuple[str, str, str, int, int, str] | None:
        row = self._fetchone(
            f"SELECT source_database,table_name,status,row_count,chunk_count,staging_table "
            f"FROM {self._metadata}.runs WHERE run_id=%s",
            (run_id,),
        )
        return row  # type: ignore[return-value]

    def register_validated_run(self, manifest: Manifest, staging_table: str) -> None:
        existing = self.get_run(manifest.run_id)
        identity = (
            manifest.source.database,
            manifest.source.table,
            manifest.row_count,
            len(manifest.chunks),
            staging_table,
        )
        if existing is not None:
            stored = (existing[0], existing[1], int(existing[3]), int(existing[4]), existing[5])
            if stored != identity:
                raise DestinationMySQLError(
                    "STAGING_STATE_MISMATCH: run metadata differs from manifest"
                )
            stored_chunks = self._fetchall(
                f"SELECT sequence,logical_name,expected_rows,compressed_bytes,sha256 "
                f"FROM {self._metadata}.chunks WHERE run_id=%s ORDER BY sequence",
                (manifest.run_id,),
            )
            expected_chunks = [
                (
                    chunk.sequence,
                    chunk.file,
                    chunk.rows,
                    chunk.compressed_bytes,
                    chunk.sha256,
                )
                for chunk in manifest.chunks
            ]
            if stored_chunks != expected_chunks:
                raise DestinationMySQLError(
                    "STAGING_STATE_MISMATCH: chunk metadata differs from manifest"
                )
            return
        now = _utcnow()
        with self.transaction():
            self._execute(
                f"INSERT INTO {self._metadata}.runs "
                "(run_id,source_database,table_name,status,protocol_version,row_count,chunk_count,"
                "staging_table,manifest_received_at,validated_at,created_at,updated_at) "
                "VALUES (%s,%s,%s,'VALIDATED',%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    manifest.run_id,
                    manifest.source.database,
                    manifest.source.table,
                    manifest.protocol_version,
                    manifest.row_count,
                    len(manifest.chunks),
                    staging_table,
                    now,
                    now,
                    now,
                    now,
                ),
            )
            for chunk in manifest.chunks:
                self._execute(
                    f"INSERT INTO {self._metadata}.chunks "
                    "(run_id,sequence,logical_name,expected_rows,compressed_bytes,sha256,status) "
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

    def table_exists(self, database: str, table: str) -> bool:
        row = self._fetchone(
            "SELECT 1 FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND TABLE_TYPE='BASE TABLE'",
            (database, table),
        )
        return row is not None

    def create_staging_table(self, rewritten_ddl: str) -> None:
        # DDL 只来自 ddl.rewrite_create_table_target 的单 CREATE TABLE 输出。
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
        row = self._fetchone(
            f"SELECT 1 FROM {self._metadata}.chunks WHERE run_id=%s AND status='IMPORTED' LIMIT 1",
            (run_id,),
        )
        return row is not None

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
        self._execute(
            f"UPDATE {self._metadata}.runs SET status='IMPORTING',import_started_at=COALESCE("
            "import_started_at,%s),last_error=NULL,updated_at=%s WHERE run_id=%s",
            (_utcnow(), _utcnow(), run_id),
        )

    def complete_staged(self, run_id: str, expected_rows: int) -> None:
        row = self._fetchone(
            f"SELECT COUNT(*),COALESCE(SUM(imported_rows),0) FROM {self._metadata}.chunks "
            "WHERE run_id=%s AND status='IMPORTED'",
            (run_id,),
        )
        run = self.get_run(run_id)
        if row is None or run is None or int(row[0]) != int(run[4]) or int(row[1]) != expected_rows:
            raise DestinationMySQLError(
                "STAGING_STATE_MISMATCH: imported chunk totals are inconsistent"
            )
        now = _utcnow()
        self._execute(
            f"UPDATE {self._metadata}.runs SET status='STAGED',import_completed_at=%s,"
            "last_error=NULL,updated_at=%s WHERE run_id=%s",
            (now, now, run_id),
        )

    def fail_run(self, run_id: str, error: str) -> None:
        self._execute(
            f"UPDATE {self._metadata}.runs SET status='FAILED',last_error=%s,updated_at=%s "
            "WHERE run_id=%s",
            (error, _utcnow(), run_id),
        )
