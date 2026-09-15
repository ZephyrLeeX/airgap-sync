"""Snapshot Runner 测试 (使用 Fake Source, 不需要真实 MySQL)。

覆盖: 正常表 / 空表 / 多 Chunk / DDL 一致 / DDL 变化 / 扫描中途异常 /
current_run_id 只在成功后推进 / manifest 与文件内容端到端一致。
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import zstandard

from airgap_sync.common.manifest import read_manifest
from airgap_sync.common.models import AppConfig
from airgap_sync.common.row_codec import decode_row
from airgap_sync.common.verification import MultisetDigest
from airgap_sync.source.chunk_writer import ChunkWriter, ChunkWriterError
from airgap_sync.source.snapshot import (
    SCHEMA_CHANGED_DURING_SNAPSHOT,
    TABLE_NOT_CONFIGURED,
    TABLE_NOT_ENABLED,
    SnapshotError,
    SnapshotRunner,
    generate_run_id,
)
from airgap_sync.source.state import SourceState, state_db_path

DDL = (
    "CREATE TABLE `t_demo` (\n"
    "  `id` int(11) DEFAULT NULL,\n"
    "  `name` varchar(64) DEFAULT NULL COMMENT '姓名',\n"
    "  KEY `idx_name` (`name`)\n"
    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 AUTO_INCREMENT=101"
)

# 故意包含: 重复行、NULL、空串、Decimal、bytes、datetime (无主键表语义)
ROWS: list[tuple[Any, ...]] = [
    (1, "a", None),
    (1, "a", None),  # 完全重复的行
    (2, "", Decimal("12.3400")),
    (3, None, b"\x00\x01\xff"),
    (4, "中文", datetime(2026, 9, 14, 20, 0, 0, 123456)),
]


class FakeTableStream:
    def __init__(
        self,
        columns: list[str],
        batches: list[list[tuple[Any, ...]]],
        fail_after_batches: int | None = None,
    ) -> None:
        self.columns = columns
        self._batches = batches
        self._fail_after = fail_after_batches

    def __enter__(self) -> FakeTableStream:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def __iter__(self):
        for consumed, batch in enumerate(self._batches):
            if self._fail_after is not None and consumed >= self._fail_after:
                raise RuntimeError("fake scanner failure midway")
            yield batch


class FakeSnapshotSource:
    """SnapshotSource 替身: 固定 DDL + 可配置批次。"""

    def __init__(
        self,
        ddl: str = DDL,
        columns: list[str] | None = None,
        rows: list[tuple[Any, ...]] | None = None,
        ddl_change: str | None = None,
        stream_error_after_batches: int | None = None,
        missing_table: bool = False,
        table_type: str = "BASE TABLE",
    ) -> None:
        self.ddl = ddl
        self.ddl_change = ddl_change
        self.columns = columns if columns is not None else ["id", "name", "extra"]
        self.rows = rows if rows is not None else ROWS
        self.stream_error_after = stream_error_after_batches
        self.missing_table = missing_table
        self.table_type = table_type
        self.ddl_read_count = 0
        self.stream_count = 0

    def fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        assert "information_schema.TABLES" in sql
        return [] if self.missing_table else [(self.table_type,)]

    def get_create_table(self, table_name: str) -> str:
        if self.missing_table:
            raise RuntimeError(f"table '{table_name}' does not exist")
        self.ddl_read_count += 1
        if self.ddl_read_count == 2 and self.ddl_change is not None:
            return self.ddl_change
        return self.ddl

    def stream_table(self, table_name: str, fetch_size: int) -> FakeTableStream:
        self.stream_count += 1
        batches = [self.rows[i : i + fetch_size] for i in range(0, len(self.rows), fetch_size)]
        return FakeTableStream(
            columns=list(self.columns),
            batches=batches,
            fail_after_batches=self.stream_error_after,
        )


def make_config(
    tmp_path: Path,
    password_env: str,
    tables: dict[str, bool] | None = None,
    chunk: dict[str, Any] | None = None,
    snapshot: dict[str, Any] | None = None,
) -> AppConfig:
    return AppConfig.model_validate(
        {
            "role": "source",
            "mysql": {
                "host": "127.0.0.1",
                "port": 3306,
                "database": "sgaj_data",
                "user": "sgaj_sync",
                "password_env": password_env,
            },
            "paths": {"data_dir": str(tmp_path / "data")},
            "snapshot": snapshot or {"fetch_size": 2},
            "chunk": chunk or {"max_rows": 3, "max_uncompressed_bytes": 10_000_000},
            "tables": [
                {"name": name, "enabled": enabled}
                for name, enabled in (tables or {"t_demo": True}).items()
            ],
        }
    )


@pytest.fixture
def state(tmp_path: Path) -> SourceState:
    state = SourceState(state_db_path(tmp_path / "data"))
    state.initialize()
    return state


def run_snapshot(
    tmp_path: Path,
    password_env: str,
    source: FakeSnapshotSource,
    state: SourceState,
    config: AppConfig | None = None,
    table: str = "t_demo",
):
    config = config or make_config(tmp_path, password_env)
    return SnapshotRunner(source, state, config).snapshot(table)


def decompressed_rows(chunk_path: Path) -> list[bytes]:
    dctx = zstandard.ZstdDecompressor()
    with open(chunk_path, "rb") as fh:
        return dctx.stream_reader(fh).read().splitlines()


class TestRunId:
    def test_format(self):
        run_id = generate_run_id()
        prefix, suffix = run_id.split("-")
        datetime.strptime(prefix, "%Y%m%dT%H%M%SZ")  # 合法 UTC 时间戳
        assert len(suffix) == 8
        int(suffix, 16)  # hex

    def test_filename_safe(self):
        allowed = set("0123456789abcdefghijklmnopqrstuvwxyzTZ-")
        for _ in range(10):
            assert set(generate_run_id()) <= allowed

    def test_unique(self):
        ids = {generate_run_id() for _ in range(50)}
        assert len(ids) == 50


class TestSuccess:
    def test_small_table_completed(self, tmp_path, password_env, state):
        result = run_snapshot(tmp_path, password_env, FakeSnapshotSource(), state)
        assert result.status == "COMPLETED"
        assert result.error is None
        assert result.row_count == len(ROWS)
        assert result.chunk_count == 2  # fetch_size=2 → 3 批, max_rows=3 → 2 chunk
        assert result.table == "t_demo"
        assert result.run_dir == tmp_path / "data" / "outbox" / "t_demo" / result.run_id

        # 目录结构: schema.sql + chunks + manifest.json, 无 .part 残留
        names = sorted(p.name for p in result.run_dir.iterdir())
        assert names == [
            "chunk-000001.jsonl.zst",
            "chunk-000002.jsonl.zst",
            "manifest.json",
            "schema.sql",
        ]

    def test_manifest_content(self, tmp_path, password_env, state):
        result = run_snapshot(tmp_path, password_env, FakeSnapshotSource(), state)
        manifest = read_manifest(result.run_dir / "manifest.json")
        assert manifest.protocol_version == 1
        assert manifest.run_id == result.run_id
        assert manifest.run_type == "FULL_SNAPSHOT"
        assert manifest.source.database == "sgaj_data"
        assert manifest.source.table == "t_demo"
        assert manifest.columns == ["id", "name", "extra"]
        assert manifest.row_count == len(ROWS)
        assert [c.sequence for c in manifest.chunks] == [1, 2]
        assert manifest.schema_file.file == "schema.sql"
        assert manifest.verification.algorithm == "multiset_digest_v1"

    def test_manifest_is_digest_of_files(self, tmp_path, password_env, state):
        """端到端: 从最终文件反算摘要与 manifest 一致 (Destination 视角)。"""
        result = run_snapshot(tmp_path, password_env, FakeSnapshotSource(), state)
        manifest = read_manifest(result.run_dir / "manifest.json")

        digest = MultisetDigest()
        for chunk in manifest.chunks:
            content = (result.run_dir / chunk.file).read_bytes()
            assert hashlib.sha256(content).hexdigest() == chunk.sha256
            assert len(content) == chunk.compressed_bytes
            for line in decompressed_rows(result.run_dir / chunk.file):
                digest.update(line.rstrip(b"\n"))
        assert digest.summary().row_count == manifest.verification.row_count
        assert digest.summary().digest_a == manifest.verification.digest_a
        assert digest.summary().digest_b == manifest.verification.digest_b

    def test_rows_round_trip_through_files(self, tmp_path, password_env, state):
        """解码后的行与源数据完全一致 (含重复行 / NULL / Decimal / bytes)。"""
        result = run_snapshot(tmp_path, password_env, FakeSnapshotSource(), state)
        manifest = read_manifest(result.run_dir / "manifest.json")
        recovered = [
            tuple(decode_row(line))
            for chunk in manifest.chunks
            for line in decompressed_rows(result.run_dir / chunk.file)
        ]
        assert recovered == ROWS

    def test_schema_sql_keeps_original_ddl(self, tmp_path, password_env, state):
        result = run_snapshot(tmp_path, password_env, FakeSnapshotSource(), state)
        content = (result.run_dir / "schema.sql").read_bytes()
        assert content.endswith(b";\n")
        text = content.decode("utf-8")
        assert text.startswith("CREATE TABLE `t_demo`")
        assert "COMMENT '姓名'" in text
        assert "ENGINE=InnoDB" in text

        manifest = read_manifest(result.run_dir / "manifest.json")
        assert manifest.schema_file.sha256 == hashlib.sha256(content).hexdigest()

    def test_current_run_id_advanced(self, tmp_path, password_env, state):
        run_snapshot(tmp_path, password_env, FakeSnapshotSource(), state)
        row = state.get_table_state("t_demo")
        assert row.status == "COMPLETED"
        assert row.current_run_id is not None

    def test_empty_table_completed_zero_chunks(self, tmp_path, password_env, state):
        result = run_snapshot(tmp_path, password_env, FakeSnapshotSource(rows=[]), state)
        assert result.status == "COMPLETED"
        assert result.row_count == 0
        assert result.chunk_count == 0
        manifest = read_manifest(result.run_dir / "manifest.json")
        assert manifest.chunks == []
        assert manifest.verification.row_count == 0
        assert manifest.verification.digest_a == "0" * 64

    def test_many_chunks(self, tmp_path, password_env, state):
        config = make_config(
            tmp_path,
            password_env,
            chunk={"max_rows": 2, "max_uncompressed_bytes": 10_000_000, "compression_level": 3},
        )
        result = run_snapshot(tmp_path, password_env, FakeSnapshotSource(), state, config=config)
        assert result.chunk_count == 3
        manifest = read_manifest(result.run_dir / "manifest.json")
        assert [c.sequence for c in manifest.chunks] == [1, 2, 3]
        assert [c.file for c in manifest.chunks] == [
            "chunk-000001.jsonl.zst",
            "chunk-000002.jsonl.zst",
            "chunk-000003.jsonl.zst",
        ]

    def test_second_run_new_directory(self, tmp_path, password_env, state):
        first = run_snapshot(tmp_path, password_env, FakeSnapshotSource(), state)
        second = run_snapshot(tmp_path, password_env, FakeSnapshotSource(), state)
        assert first.run_id != second.run_id
        assert first.run_dir != second.run_dir
        assert first.run_dir.exists() and second.run_dir.exists()
        row = state.get_table_state("t_demo")
        assert row.current_run_id == second.run_id  # 指向最新成功 Run


class TestDdlConsistency:
    def test_auto_increment_counter_change_is_not_schema_change(
        self, tmp_path, password_env, state
    ):
        """并发插入只改自增计数器, 不算结构变化。"""
        source = FakeSnapshotSource(
            ddl_change=DDL.replace("AUTO_INCREMENT=101", "AUTO_INCREMENT=9182736")
        )
        result = run_snapshot(tmp_path, password_env, source, state)
        assert result.status == "COMPLETED"
        assert (result.run_dir / "manifest.json").exists()

    def test_real_ddl_change_fails_run(self, tmp_path, password_env, state):
        source = FakeSnapshotSource(
            ddl_change=DDL.replace("`name` varchar(64)", "`name` varchar(128)")
        )
        result = run_snapshot(tmp_path, password_env, source, state)
        assert result.status == "FAILED"
        assert SCHEMA_CHANGED_DURING_SNAPSHOT in str(result.error)

    def test_ddl_change_produces_no_manifest_and_no_schema(self, tmp_path, password_env, state):
        source = FakeSnapshotSource(ddl_change=DDL + "\n-- altered")
        result = run_snapshot(tmp_path, password_env, source, state)
        assert not (result.run_dir / "manifest.json").exists()
        assert not (result.run_dir / "schema.sql").exists()
        # Chunk 可能已写出, 但 Run 不完整 (manifest 不存在)
        assert list(result.run_dir.glob("*.part")) == []

    def test_ddl_change_does_not_advance_current_run_id(self, tmp_path, password_env, state):
        ok = run_snapshot(tmp_path, password_env, FakeSnapshotSource(), state)
        assert state.get_table_state("t_demo").current_run_id == ok.run_id

        source = FakeSnapshotSource(ddl_change=DDL.replace("int(11)", "bigint(20)"))
        failed = run_snapshot(tmp_path, password_env, source, state)
        row = state.get_table_state("t_demo")
        assert row.status == "FAILED"
        assert row.current_run_id == ok.run_id  # 保持上一个成功 Run
        assert row.last_run_id == failed.run_id
        assert row.last_error

    def test_index_added_counts_as_schema_change(self, tmp_path, password_env, state):
        source = FakeSnapshotSource(
            ddl_change=DDL.replace(
                "KEY `idx_name` (`name`)", "KEY `idx_name` (`name`),\n  KEY `k2` (`id`)"
            )
        )
        result = run_snapshot(tmp_path, password_env, source, state)
        assert result.status == "FAILED"


class TestScannerFailure:
    def test_midway_stream_error_fails_run(self, tmp_path, password_env, state):
        source = FakeSnapshotSource(stream_error_after_batches=1)
        result = run_snapshot(tmp_path, password_env, source, state)
        assert result.status == "FAILED"
        assert "fake scanner failure" in str(result.error)

    def test_midway_error_no_manifest(self, tmp_path, password_env, state):
        source = FakeSnapshotSource(stream_error_after_batches=1)
        result = run_snapshot(tmp_path, password_env, source, state)
        assert not (result.run_dir / "manifest.json").exists()

    def test_midway_stream_error_aborts_current_chunk(self, tmp_path, password_env, state):
        source = FakeSnapshotSource(stream_error_after_batches=1)
        result = run_snapshot(tmp_path, password_env, source, state)
        assert result.status == "FAILED"
        assert not (result.run_dir / "manifest.json").exists()
        assert list(result.run_dir.glob("*.part")) == []
        assert list(result.run_dir.glob("chunk-*.jsonl.zst")) == []

    def test_row_codec_error_aborts_current_chunk(self, tmp_path, password_env, state):
        source = FakeSnapshotSource(rows=[(1, "valid", None), (2, object(), None)])
        result = run_snapshot(tmp_path, password_env, source, state)
        assert result.status == "FAILED"
        assert "unsupported value type" in str(result.error)
        assert not (result.run_dir / "manifest.json").exists()
        assert list(result.run_dir.glob("*.part")) == []
        assert list(result.run_dir.glob("chunk-*.jsonl.zst")) == []

    def test_writer_error_aborts_current_chunk(self, tmp_path, password_env, state, monkeypatch):
        original_write_row = ChunkWriter.write_row
        calls = 0

        def fail_second_write(writer, encoded):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise ChunkWriterError("fake chunk write failure")
            original_write_row(writer, encoded)

        monkeypatch.setattr(ChunkWriter, "write_row", fail_second_write)
        result = run_snapshot(tmp_path, password_env, FakeSnapshotSource(), state)
        assert result.status == "FAILED"
        assert "fake chunk write failure" in str(result.error)
        assert not (result.run_dir / "manifest.json").exists()
        assert list(result.run_dir.glob("*.part")) == []
        assert list(result.run_dir.glob("chunk-*.jsonl.zst")) == []

    def test_missing_table_fails_run(self, tmp_path, password_env, state):
        with pytest.raises(SnapshotError, match="TABLE_NOT_FOUND"):
            run_snapshot(tmp_path, password_env, FakeSnapshotSource(missing_table=True), state)
        assert not (tmp_path / "data" / "outbox").exists()
        assert state.get_table_state("t_demo") is None

    def test_failure_keeps_previous_current_run_id(self, tmp_path, password_env, state):
        ok = run_snapshot(tmp_path, password_env, FakeSnapshotSource(), state)
        before = state.get_table_state("t_demo").current_run_id
        assert before == ok.run_id

        failed = run_snapshot(
            tmp_path, password_env, FakeSnapshotSource(stream_error_after_batches=0), state
        )
        assert failed.status == "FAILED"
        assert state.get_table_state("t_demo").current_run_id == before  # 未推进


class TestConfigGuards:
    def test_table_not_configured(self, tmp_path, password_env, state):
        with pytest.raises(SnapshotError) as excinfo:
            run_snapshot(tmp_path, password_env, FakeSnapshotSource(), state, table="ghost")
        assert excinfo.value.code == TABLE_NOT_CONFIGURED

    def test_table_disabled(self, tmp_path, password_env, state):
        config = make_config(tmp_path, password_env, tables={"t_demo": False})
        with pytest.raises(SnapshotError) as excinfo:
            run_snapshot(tmp_path, password_env, FakeSnapshotSource(), state, config=config)
        assert excinfo.value.code == TABLE_NOT_ENABLED

    def test_disabled_table_creates_no_run_directory(self, tmp_path, password_env, state):
        config = make_config(tmp_path, password_env, tables={"t_demo": False})
        with pytest.raises(SnapshotError):
            run_snapshot(tmp_path, password_env, FakeSnapshotSource(), state, config=config)
        assert not (tmp_path / "data" / "outbox").exists()
