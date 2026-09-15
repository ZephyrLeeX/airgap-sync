from __future__ import annotations

import hashlib
from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import zstandard

from airgap_sync.common.manifest import ChunkMeta, manifest_payload
from airgap_sync.common.models import AppConfig
from airgap_sync.common.row_codec import encode_row
from airgap_sync.common.transport import transport_filename
from airgap_sync.destination.mysql import DestinationColumn, staging_table_name
from airgap_sync.destination.processor import DestinationProcessor
from test_destination_incoming import RUN, make_run


class FakeDestinationDB:
    def __init__(self, *, fail_batch: int | None = None, generated_name: bool = False):
        self.run = None
        self.staging_exists = False
        self.chunk_statuses = {}
        self.rows = []
        self.target_rows = [(99, "old production data")]
        self.batch_calls = 0
        self.fail_batch = fail_batch
        self.generated_name = generated_name
        self.locked = False
        self.table_locked = False

    def acquire_run_lock(self, run_id):
        if self.locked:
            return False
        self.locked = True
        return True

    def release_run_lock(self, run_id):
        self.locked = False

    def acquire_table_lock(self, source_database, table_name):
        if self.table_locked:
            return False
        self.table_locked = True
        return True

    def release_table_lock(self, source_database, table_name):
        self.table_locked = False

    def get_run(self, run_id):
        return self.run

    def register_validated_run(self, manifest, staging):
        if self.run is None:
            self.run = (
                manifest.source.database,
                manifest.source.table,
                "VALIDATED",
                manifest.row_count,
                len(manifest.chunks),
                staging,
            )
            self.chunk_statuses = {chunk.sequence: "PENDING" for chunk in manifest.chunks}

    def table_exists(self, database, table):
        return self.staging_exists

    def has_imported_chunks(self, run_id):
        return "IMPORTED" in self.chunk_statuses.values()

    def create_staging_table(self, ddl):
        assert "CREATE TABLE `__airgap_stg_" in ddl
        self.staging_exists = True

    def staging_columns(self, staging):
        return [
            DestinationColumn("id", 1, "", ""),
            DestinationColumn(
                "name",
                2,
                "VIRTUAL GENERATED" if self.generated_name else "",
                "concat(`id`,'x')" if self.generated_name else "",
            ),
        ]

    def begin_import(self, run_id):
        assert self.run is not None
        self.run = (*self.run[:2], "IMPORTING", *self.run[3:])

    def chunk_status(self, run_id, sequence):
        return self.chunk_statuses[sequence]

    @contextmanager
    def transaction(self):
        rows_before = deepcopy(self.rows)
        statuses_before = dict(self.chunk_statuses)
        try:
            yield
        except BaseException:
            self.rows = rows_before
            self.chunk_statuses = statuses_before
            raise

    def set_chunk_importing(self, run_id, sequence):
        self.chunk_statuses[sequence] = "IMPORTING"

    def insert_rows(self, table, columns, rows):
        self.batch_calls += 1
        if self.batch_calls == self.fail_batch:
            raise RuntimeError("simulated third batch failure")
        self.rows.extend(rows)

    def set_chunk_imported(self, run_id, sequence, rows):
        self.chunk_statuses[sequence] = "IMPORTED"

    def fail_chunk(self, run_id, sequence, error):
        self.chunk_statuses[sequence] = "FAILED"

    def complete_staged(self, run_id, expected_rows):
        assert len(self.rows) == expected_rows
        assert set(self.chunk_statuses.values()) <= {"IMPORTED"}
        self.run = (*self.run[:2], "STAGED", *self.run[3:])

    def fail_run(self, run_id, error):
        if self.run is not None:
            self.run = (*self.run[:2], "FAILED", *self.run[3:])


def config(tmp_path: Path, *, batch=1000) -> AppConfig:
    return AppConfig.model_validate(
        {
            "role": "destination",
            "mysql": {
                "host": "127.0.0.1",
                "database": "target_db",
                "user": "sync",
                "password_env": "DEST_PASSWORD",
            },
            "destination": {
                "incoming_dir": str(tmp_path / "incoming"),
                "insert_batch_rows": batch,
                "settle_seconds": 0,
            },
        }
    )


def test_new_unconfigured_table_stages_without_touching_target(tmp_path):
    app_config = config(tmp_path, batch=2)
    assert app_config.destination is not None
    rows = (
        (1, None),
        (1, ""),
        (2, "duplicate"),
        (2, "duplicate"),
        (3, Decimal("123.4500")),
        (4, b"\x00\xff"),
        (5, datetime(2026, 9, 15, 3, 0, 0, 123456)),
        (6, date(2026, 9, 15)),
        (7, time(3, 4, 5, 6)),
        (8, timedelta(days=-1, seconds=2, microseconds=3)),
        (9, "长文本" * 100),
    )
    make_run(app_config.destination.incoming_dir, rows=rows)
    database = FakeDestinationDB()

    result = DestinationProcessor(database, app_config).process(RUN)

    assert result.status == "STAGED"
    assert result.table == "new_table"
    assert database.rows == list(rows)
    assert database.batch_calls == 6
    assert database.target_rows == [(99, "old production data")]
    assert app_config.tables == []
    assert list(app_config.destination.incoming_dir.iterdir())  # incoming 未清理


def test_generated_column_is_excluded_from_insert_list(tmp_path):
    app_config = config(tmp_path)
    assert app_config.destination is not None
    make_run(app_config.destination.incoming_dir, rows=((1, "source-generated-value"),))
    database = FakeDestinationDB(generated_name=True)

    assert DestinationProcessor(database, app_config).process(RUN).status == "STAGED"
    assert database.rows == [(1,)]


def test_third_batch_failure_rolls_back_entire_chunk_and_metadata(tmp_path):
    app_config = config(tmp_path, batch=1000)
    assert app_config.destination is not None
    make_run(
        app_config.destination.incoming_dir,
        rows=tuple((index, str(index)) for index in range(3000)),
    )
    database = FakeDestinationDB(fail_batch=3)

    result = DestinationProcessor(database, app_config).process(RUN)

    assert result.status == "FAILED"
    assert database.rows == []
    assert database.chunk_statuses[1] == "FAILED"
    assert database.target_rows == [(99, "old production data")]


def test_second_processing_of_staged_run_does_not_duplicate_rows(tmp_path):
    app_config = config(tmp_path)
    assert app_config.destination is not None
    make_run(app_config.destination.incoming_dir, rows=((1, "a"), (1, "a")))
    database = FakeDestinationDB()
    processor = DestinationProcessor(database, app_config)

    assert processor.process(RUN).status == "STAGED"
    first_rows = list(database.rows)
    assert processor.process(RUN).status == "STAGED"
    assert database.rows == first_rows
    assert database.batch_calls == 1


def test_multiple_chunks_are_imported_in_sequence(tmp_path):
    app_config = config(tmp_path, batch=2)
    assert app_config.destination is not None
    incoming = app_config.destination.incoming_dir
    manifest = make_run(incoming, rows=((1, "a"), (2, "b")))
    second_rows = ((3, "c"), (4, "d"))
    raw = b"".join(encode_row(row) + b"\n" for row in second_rows)
    compressed = zstandard.ZstdCompressor().compress(raw)
    (incoming / transport_filename(RUN, "chunk-000002.jsonl.zst")).write_bytes(compressed)
    second = ChunkMeta(
        sequence=2,
        file="chunk-000002.jsonl.zst",
        rows=2,
        uncompressed_bytes=len(raw),
        compressed_bytes=len(compressed),
        sha256=hashlib.sha256(compressed).hexdigest(),
    )
    manifest = manifest.model_copy(
        update={
            "row_count": 4,
            "chunks": [*manifest.chunks, second],
            "verification": manifest.verification.model_copy(update={"row_count": 4}),
        }
    )
    (incoming / transport_filename(RUN, "manifest.json")).write_bytes(manifest_payload(manifest))
    database = FakeDestinationDB()

    result = DestinationProcessor(database, app_config).process(RUN)

    assert result.status == "STAGED"
    assert database.rows == [(1, "a"), (2, "b"), (3, "c"), (4, "d")]
    assert database.chunk_statuses == {1: "IMPORTED", 2: "IMPORTED"}


def test_staging_name_is_deterministic_bounded_and_source_independent():
    first = staging_table_name(RUN)
    assert first == staging_table_name(RUN)
    assert first.startswith("__airgap_stg_")
    assert len(first) <= 64
    assert "new_table" not in first
