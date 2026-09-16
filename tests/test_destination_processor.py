from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

from airgap_sync.common.models import AppConfig
from airgap_sync.destination.mysql import (
    DestinationColumn,
    DestinationMySQLError,
    RunRecord,
    TableVersion,
    staging_table_name,
)
from airgap_sync.destination.processor import DestinationProcessor
from test_destination_incoming import RUN, make_run


class FakeDestinationDB:
    def __init__(
        self,
        *,
        fail_batch: int | None = None,
        generated_name: bool = False,
        verify_rows=None,
        target_exists: bool = False,
        engine: str = "InnoDB",
    ):
        self.run: RunRecord | None = None
        self.objects = {"new_table": "BASE TABLE"} if target_exists else {}
        self.chunk_statuses = {}
        self.rows = []
        self.target_rows = [(99, "old production data")] if target_exists else []
        self.batch_calls = 0
        self.fail_batch = fail_batch
        self.generated_name = generated_name
        self.verify_rows = verify_rows
        self.engine = engine
        self.locked = False
        self.table_locked = False
        self.versions = []
        self.renames = []

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

    def register_validated_run(self, manifest, staging, source_created_at):
        if self.run is None:
            self.run = RunRecord(
                manifest.run_id,
                manifest.source.database,
                manifest.source.table,
                "VALIDATED",
                manifest.row_count,
                len(manifest.chunks),
                staging,
                source_created_at.replace(tzinfo=None),
            )
            self.chunk_statuses = {chunk.sequence: "PENDING" for chunk in manifest.chunks}

    def object_type(self, database, table):
        return self.objects.get(table)

    def table_exists(self, database, table):
        return self.object_type(database, table) == "BASE TABLE"

    def storage_engine(self, database, table):
        return self.engine

    def has_imported_chunks(self, run_id):
        return "IMPORTED" in self.chunk_statuses.values()

    def create_staging_table(self, ddl):
        assert "CREATE TABLE `__airgap_stg_" in ddl
        assert self.run is not None
        self.objects[self.run.staging_table] = "BASE TABLE"

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
        self.run = replace(self.run, status="IMPORTING")

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
            raise RuntimeError("simulated batch failure")
        self.rows.extend(rows)

    def set_chunk_imported(self, run_id, sequence, rows):
        self.chunk_statuses[sequence] = "IMPORTED"

    def fail_chunk(self, run_id, sequence, error):
        self.chunk_statuses[sequence] = "FAILED"

    def complete_staged(self, run_id, expected_rows):
        assert len(self.rows) == expected_rows
        self.run = replace(self.run, status="STAGED")

    def set_verifying(self, run_id):
        self.run = replace(self.run, status="VERIFYING")

    @contextmanager
    def stream_table_columns(self, table, columns, fetch_size):
        if table == "new_table":
            rows = self.target_rows
        elif self.verify_rows is not None:
            rows = self.verify_rows
        elif self.generated_name:
            rows = [(row[0], "source-generated-value") for row in self.rows]
        else:
            rows = self.rows
        yield iter(rows)

    def record_verification(self, run_id, summary, matched):
        self.run = replace(self.run, status="STAGED" if matched else "MISMATCH")

    def latest_version(self, source_database, table_name):
        return self.versions[-1] if self.versions else None

    def set_superseded(self, run_id):
        self.run = replace(self.run, status="SUPERSEDED")

    def target_dependencies(self, database, table):
        return False, False

    def prepare_swapping(self, run_id, target_existed, backup_table):
        self.run = replace(
            self.run,
            status="SWAPPING",
            target_existed=target_existed,
            backup_table=backup_table,
        )

    def rename_for_promotion(self, database, staging, target, backup):
        self.renames.append((staging, target, backup))
        if backup:
            self.objects[backup] = self.objects.pop(target)
        self.objects[target] = self.objects.pop(staging)
        self.target_rows = list(self.verify_rows if self.verify_rows is not None else self.rows)

    def finalize_verified(self, run_id, expected_previous_run_id):
        self.run = replace(self.run, status="VERIFIED")

    def drop_table(self, database, table):
        self.objects.pop(table)

    def record_cleanup_error(self, run_id, error):
        self.run = replace(self.run, cleanup_error=error)

    def record_backup_cleanup_error(self, run_id, error):
        self.run = replace(self.run, backup_cleanup_error=error)

    def fail_run(self, run_id, error):
        if self.run is not None and self.run.status != "SWAPPING":
            self.run = replace(self.run, status="FAILED")


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


def test_full_pipeline_verifies_promotes_and_cleans_incoming(tmp_path):
    app_config = config(tmp_path, batch=2)
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
    )
    make_run(app_config.destination.incoming_dir, rows=rows)
    database = FakeDestinationDB()

    result = DestinationProcessor(database, app_config).process(RUN)

    assert result.status == "VERIFIED"
    assert database.target_rows == list(rows)
    assert database.batch_calls == 5
    assert not list(app_config.destination.incoming_dir.iterdir())


def test_verification_reads_database_and_mismatch_keeps_incoming(tmp_path):
    app_config = config(tmp_path)
    make_run(app_config.destination.incoming_dir, rows=((1, "correct file"),))
    database = FakeDestinationDB(verify_rows=[(1, "changed in database")])

    result = DestinationProcessor(database, app_config).process(RUN)

    assert result.status == "MISMATCH"
    assert database.renames == []
    assert list(app_config.destination.incoming_dir.iterdir())


def test_empty_table_verifies_and_promotes(tmp_path):
    app_config = config(tmp_path)
    make_run(app_config.destination.incoming_dir, rows=())
    database = FakeDestinationDB()
    result = DestinationProcessor(database, app_config).process(RUN)
    assert result.status == "VERIFIED"
    assert result.rows == 0
    assert database.target_rows == []


def test_generated_column_is_read_during_verification(tmp_path):
    app_config = config(tmp_path)
    make_run(app_config.destination.incoming_dir, rows=((1, "source-generated-value"),))
    database = FakeDestinationDB(generated_name=True)
    assert DestinationProcessor(database, app_config).process(RUN).status == "VERIFIED"
    assert database.rows == [(1,)]


def test_chunk_failure_rolls_back_before_verification(tmp_path):
    app_config = config(tmp_path, batch=1000)
    make_run(
        app_config.destination.incoming_dir,
        rows=tuple((index, str(index)) for index in range(3000)),
    )
    database = FakeDestinationDB(fail_batch=3)
    result = DestinationProcessor(database, app_config).process(RUN)
    assert result.status == "FAILED"
    assert database.rows == []
    assert database.renames == []


def test_verified_retry_does_not_reinsert_or_rename(tmp_path):
    app_config = config(tmp_path)
    make_run(app_config.destination.incoming_dir, rows=((1, "a"), (1, "a")))
    database = FakeDestinationDB()
    processor = DestinationProcessor(database, app_config)
    assert processor.process(RUN).status == "VERIFIED"
    assert processor.process(RUN).status == "VERIFIED"
    assert database.batch_calls == 1
    assert len(database.renames) == 1


def test_failed_capability_query_retries_same_run_with_existing_empty_staging(tmp_path):
    class CapabilityQueryOnceDB(FakeDestinationDB):
        def __init__(self):
            super().__init__()
            self.fail_capability_query = True
            self.create_calls = 0

        def create_staging_table(self, ddl):
            self.create_calls += 1
            super().create_staging_table(ddl)

        def staging_columns(self, staging):
            if self.fail_capability_query:
                self.fail_capability_query = False
                raise DestinationMySQLError("Destination MySQL query failed: unknown capability")
            return super().staging_columns(staging)

    app_config = config(tmp_path)
    make_run(app_config.destination.incoming_dir, rows=((1, "retry"),))
    database = CapabilityQueryOnceDB()
    processor = DestinationProcessor(database, app_config)

    failed = processor.process(RUN)
    assert failed.status == "FAILED"
    assert database.run.status == "FAILED"
    assert database.create_calls == 1
    assert database.rows == []
    assert database.chunk_statuses == {1: "PENDING"}
    assert list(app_config.destination.incoming_dir.iterdir())

    retried = processor.process(RUN)
    assert retried.status == "VERIFIED"
    assert database.create_calls == 1
    assert database.batch_calls == 1
    assert database.target_rows == [(1, "retry")]


def test_non_innodb_fails_before_any_insert(tmp_path):
    app_config = config(tmp_path)
    make_run(app_config.destination.incoming_dir)
    database = FakeDestinationDB(engine="MyISAM")
    result = DestinationProcessor(database, app_config).process(RUN)
    assert result.status == "FAILED"
    assert "UNSUPPORTED_STORAGE_ENGINE" in result.error
    assert database.batch_calls == 0


def test_existing_target_uses_one_backup_swap(tmp_path):
    app_config = config(tmp_path)
    make_run(app_config.destination.incoming_dir, rows=((1, "new"),))
    database = FakeDestinationDB(target_exists=True)
    result = DestinationProcessor(database, app_config).process(RUN)
    assert result.status == "VERIFIED"
    assert len(database.renames) == 1
    assert database.renames[0][2].startswith("__airgap_old_")
    assert database.target_rows == [(1, "new")]


def test_foreign_key_and_trigger_dependencies_refuse_swap(tmp_path):
    class DependencyDB(FakeDestinationDB):
        def __init__(self, dependencies):
            super().__init__(target_exists=True)
            self.dependencies = dependencies

        def target_dependencies(self, database, table):
            return self.dependencies

    for dependencies, code in [
        ((True, False), "TARGET_FOREIGN_KEY_DEPENDENCY"),
        ((False, True), "TARGET_TRIGGER_DEPENDENCY"),
    ]:
        case = tmp_path / code
        case.mkdir()
        app_config = config(case)
        make_run(app_config.destination.incoming_dir, rows=((1, "new"),))
        database = DependencyDB(dependencies)
        result = DestinationProcessor(database, app_config).process(RUN)
        assert result.status == "FAILED"
        assert code in result.error
        assert database.target_rows == [(99, "old production data")]
        assert database.renames == []


def test_view_at_target_name_is_never_replaced(tmp_path):
    app_config = config(tmp_path)
    make_run(app_config.destination.incoming_dir, rows=((1, "new"),))
    database = FakeDestinationDB()
    database.objects["new_table"] = "VIEW"
    result = DestinationProcessor(database, app_config).process(RUN)
    assert result.status == "FAILED"
    assert "TARGET_OBJECT_TYPE_UNSUPPORTED" in result.error
    assert database.objects["new_table"] == "VIEW"


def test_crash_after_rename_recovers_by_verifying_live_without_second_rename(tmp_path):
    class CrashOnceDB(FakeDestinationDB):
        def __init__(self):
            super().__init__(target_exists=True)
            self.crash = True

        def finalize_verified(self, run_id, expected_previous_run_id):
            if self.crash:
                self.crash = False
                raise DestinationMySQLError("simulated crash after rename")
            super().finalize_verified(run_id, expected_previous_run_id)

    app_config = config(tmp_path)
    make_run(app_config.destination.incoming_dir, rows=((1, "new"),))
    database = CrashOnceDB()
    processor = DestinationProcessor(database, app_config)
    assert processor.process(RUN).status == "FAILED"
    assert database.run.status == "SWAPPING"
    assert processor.process(RUN).status == "VERIFIED"
    assert len(database.renames) == 1


def test_rename_failure_keeps_swapping_and_incoming_then_recovers(tmp_path):
    class RenameOnceDB(FakeDestinationDB):
        def __init__(self):
            super().__init__(target_exists=True)
            self.fail_rename = True

        def rename_for_promotion(self, database, staging, target, backup):
            if self.fail_rename:
                self.fail_rename = False
                raise DestinationMySQLError("simulated rename failure")
            super().rename_for_promotion(database, staging, target, backup)

    app_config = config(tmp_path)
    make_run(app_config.destination.incoming_dir, rows=((1, "new"),))
    database = RenameOnceDB()
    processor = DestinationProcessor(database, app_config)
    assert processor.process(RUN).status == "FAILED"
    assert database.run.status == "SWAPPING"
    assert list(app_config.destination.incoming_dir.iterdir())
    assert processor.process(RUN).status == "VERIFIED"


def test_crash_before_rename_old_run_cannot_replace_newer_verified_run(tmp_path):
    class RenameOnceDB(FakeDestinationDB):
        def __init__(self):
            super().__init__(target_exists=True)
            self.fail_rename = True

        def rename_for_promotion(self, database, staging, target, backup):
            if self.fail_rename:
                self.fail_rename = False
                raise DestinationMySQLError("simulated crash before rename")
            super().rename_for_promotion(database, staging, target, backup)

    app_config = config(tmp_path)
    make_run(app_config.destination.incoming_dir, rows=((1, "run A"),))
    database = RenameOnceDB()
    processor = DestinationProcessor(database, app_config)
    assert processor.process(RUN).status == "FAILED"
    assert database.run.status == "SWAPPING"
    assert database.renames == []

    database.run = replace(database.run, source_created_at=datetime(2026, 9, 1))
    newer_at = datetime(2026, 9, 8, tzinfo=UTC)
    database.versions = [
        TableVersion(
            "run-b", "source_db", "new_table", newer_at, 1, None, None, None, newer_at, newer_at
        )
    ]
    database.target_rows = [(2, "run B")]

    assert processor.process(RUN).status == "SUPERSEDED"
    assert database.renames == []
    assert database.target_rows == [(2, "run B")]
    assert database.run.staging_table not in database.objects
    assert not list(app_config.destination.incoming_dir.iterdir())


def test_crash_before_rename_same_timestamp_is_ambiguous(tmp_path):
    class RenameOnceDB(FakeDestinationDB):
        def rename_for_promotion(self, database, staging, target, backup):
            raise DestinationMySQLError("simulated crash before rename")

    app_config = config(tmp_path)
    make_run(app_config.destination.incoming_dir, rows=((1, "run A"),))
    database = RenameOnceDB(target_exists=True)
    processor = DestinationProcessor(database, app_config)
    assert processor.process(RUN).status == "FAILED"

    same_at = datetime(2026, 9, 1, tzinfo=UTC)
    database.run = replace(database.run, source_created_at=same_at.replace(tzinfo=None))
    database.versions = [
        TableVersion(
            "run-b", "source_db", "new_table", same_at, 1, None, None, None, same_at, same_at
        )
    ]
    result = processor.process(RUN)

    assert result.status == "FAILED"
    assert "RUN_ORDER_AMBIGUOUS" in result.error
    assert database.run.status == "SWAPPING"
    assert database.renames == []
    assert database.run.staging_table in database.objects
    assert list(app_config.destination.incoming_dir.iterdir())


def test_crash_after_rename_old_run_does_not_touch_newer_live_or_own_backup(tmp_path):
    class CrashOnceDB(FakeDestinationDB):
        def __init__(self):
            super().__init__(target_exists=True)
            self.crash = True

        def finalize_verified(self, run_id, expected_previous_run_id):
            if self.crash:
                self.crash = False
                raise DestinationMySQLError("simulated crash after rename")
            super().finalize_verified(run_id, expected_previous_run_id)

    app_config = config(tmp_path)
    make_run(app_config.destination.incoming_dir, rows=((1, "run A"),))
    database = CrashOnceDB()
    processor = DestinationProcessor(database, app_config)
    assert processor.process(RUN).status == "FAILED"
    backup = database.run.backup_table
    assert backup in database.objects

    database.run = replace(database.run, source_created_at=datetime(2026, 9, 1))
    newer_at = datetime(2026, 9, 8, tzinfo=UTC)
    database.versions = [
        TableVersion(
            "run-b", "source_db", "new_table", newer_at, 1, None, None, None, newer_at, newer_at
        )
    ]
    database.target_rows = [(2, "run B")]

    assert processor.process(RUN).status == "SUPERSEDED"
    assert len(database.renames) == 1
    assert database.target_rows == [(2, "run B")]
    assert backup in database.objects


def test_backup_cleanup_failure_does_not_revoke_verified(tmp_path):
    class DropBackupDB(FakeDestinationDB):
        def drop_table(self, database, table):
            if table.startswith("__airgap_old_"):
                raise DestinationMySQLError("backup locked")
            super().drop_table(database, table)

    app_config = config(tmp_path)
    make_run(app_config.destination.incoming_dir, rows=((1, "new"),))
    database = DropBackupDB(target_exists=True)
    result = DestinationProcessor(database, app_config).process(RUN)
    assert result.status == "VERIFIED"
    assert database.run.backup_cleanup_error == "backup locked"


def test_incoming_cleanup_failure_does_not_revoke_verified(tmp_path, monkeypatch):
    app_config = config(tmp_path)
    make_run(app_config.destination.incoming_dir, rows=((1, "new"),))
    database = FakeDestinationDB()
    calls = []
    original_unlink = Path.unlink

    def fail_chunk(path, *args, **kwargs):
        calls.append(path.name)
        if "chunk-" in path.name:
            raise PermissionError("chunk locked")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_chunk)
    result = DestinationProcessor(database, app_config).process(RUN)
    assert result.status == "VERIFIED"
    assert calls[0].endswith("manifest.json")
    assert database.run.cleanup_error == "chunk locked"


def test_older_run_is_superseded_and_same_timestamp_is_ambiguous(tmp_path):
    def existing_version(created):
        stamp = datetime.fromisoformat(created).astimezone(UTC)
        return TableVersion(
            "other", "source_db", "new_table", stamp, 10, None, None, None, stamp, stamp
        )

    (tmp_path / "older").mkdir()
    older_config = config(tmp_path / "older")
    make_run(older_config.destination.incoming_dir, rows=((1, "a"),))
    older_db = FakeDestinationDB()
    older_db.versions = [existing_version("2026-09-16T00:00:00+00:00")]
    assert DestinationProcessor(older_db, older_config).process(RUN).status == "SUPERSEDED"
    assert older_db.renames == []

    (tmp_path / "same").mkdir()
    same_config = config(tmp_path / "same")
    make_run(same_config.destination.incoming_dir, rows=((1, "a"),))
    same_db = FakeDestinationDB()
    same_db.versions = [existing_version("2026-09-15T03:00:00+00:00")]
    result = DestinationProcessor(same_db, same_config).process(RUN)
    assert result.status == "FAILED"
    assert "RUN_ORDER_AMBIGUOUS" in result.error


def test_staging_name_is_deterministic_bounded_and_source_independent():
    first = staging_table_name(RUN)
    assert first == staging_table_name(RUN)
    assert first.startswith("__airgap_stg_")
    assert len(first) <= 64
    assert "new_table" not in first
