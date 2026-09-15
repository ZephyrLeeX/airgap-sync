from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from airgap_sync.common.models import AppConfig
from airgap_sync.common.transport import transport_filename
from airgap_sync.destination.processor import ProcessResult
from airgap_sync.destination.worker import DestinationWorker, cleanup_orphan_artifacts


def config(tmp_path):
    return AppConfig.model_validate(
        {
            "role": "destination",
            "mysql": {"host": "x", "database": "db", "user": "u", "password_env": "P"},
            "destination": {"incoming_dir": tmp_path},
            "destination_worker": {"poll_interval": "30s"},
            "maintenance": {"destination_orphan_retention": "30d"},
        }
    )


class Database:
    def cleanup_pending_runs(self):
        return []

    def active_run_ids(self):
        return set()


def test_worker_processes_bad_and_good_runs_in_same_poll(tmp_path, monkeypatch):
    import airgap_sync.destination.worker as module

    results = [
        ProcessResult("bad", "FAILED", error="broken"),
        ProcessResult("good", "VERIFIED", table="t"),
    ]
    monkeypatch.setattr(module, "process_once", lambda connection, cfg: results)
    monkeypatch.setattr(module, "retry_pending_cleanup", lambda connection, cfg: [])
    monkeypatch.setattr(module, "cleanup_orphan_artifacts", lambda connection, cfg: 0)
    assert DestinationWorker(Database(), config(tmp_path)).run_once() == results


def test_worker_retries_cleanup_without_manifest(tmp_path, monkeypatch):
    import airgap_sync.destination.worker as module

    cleanup = [ProcessResult("old", "VERIFIED", table="t")]
    monkeypatch.setattr(module, "process_once", lambda connection, cfg: [])
    monkeypatch.setattr(module, "retry_pending_cleanup", lambda connection, cfg: cleanup)
    monkeypatch.setattr(module, "cleanup_orphan_artifacts", lambda connection, cfg: 0)
    assert DestinationWorker(Database(), config(tmp_path)).run_once() == cleanup


def test_orphan_cleanup_is_old_formal_manifestless_and_conservative(tmp_path):
    cfg = config(tmp_path)
    old_run = "20260901T000000Z-12345678"
    new_run = "20260914T000000Z-12345678"
    old = tmp_path / transport_filename(old_run, "schema.sql")
    fresh = tmp_path / transport_filename(new_run, "schema.sql")
    temporary = tmp_path / "foreign.filepart"
    old.write_text("old")
    fresh.write_text("fresh")
    temporary.write_text("external")
    now = datetime(2026, 10, 2, tzinfo=UTC)
    timestamp = (now - timedelta(days=31)).timestamp()
    os.utime(old, (timestamp, timestamp))
    assert cleanup_orphan_artifacts(Database(), cfg, now=now) == 1
    assert not old.exists()
    assert fresh.exists()
    assert temporary.exists()


def test_orphan_cleanup_continues_after_permission_error(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    locked_run = "20260831T000000Z-12345678"
    removable_run = "20260901T000000Z-12345678"
    locked = tmp_path / transport_filename(locked_run, "schema.sql")
    removable = tmp_path / transport_filename(removable_run, "schema.sql")
    locked.write_text("locked")
    removable.write_text("removable")
    now = datetime(2026, 10, 2, tzinfo=UTC)
    timestamp = (now - timedelta(days=31)).timestamp()
    os.utime(locked, (timestamp, timestamp))
    os.utime(removable, (timestamp, timestamp))
    path_type = type(locked)
    original_unlink = path_type.unlink

    def unlink(path, *args, **kwargs):
        if path == locked:
            raise PermissionError("locked")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(path_type, "unlink", unlink)

    assert cleanup_orphan_artifacts(Database(), cfg, now=now) == 1
    assert locked.exists()
    assert not removable.exists()
