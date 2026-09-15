from __future__ import annotations

from datetime import UTC, datetime, timedelta
from threading import Event

from airgap_sync.common.models import AppConfig, RelayConfig
from airgap_sync.source.cycle import CycleRunner, SourceWorker, cleanup_failed_runs, next_action_at
from airgap_sync.source.snapshot import SnapshotResult
from airgap_sync.source.state import SourceState, state_db_path


def config(tmp_path, tables=("a", "b", "c")):
    return AppConfig.model_validate(
        {
            "role": "source",
            "mysql": {"host": "x", "database": "db", "user": "u", "password_env": "P"},
            "paths": {"data_dir": tmp_path / "data"},
            "schedule": {"delay_after_success": "7d", "retry_after_failure": "6h"},
            "tables": [{"name": name} for name in tables],
        }
    )


class Clock:
    def __init__(self, value):
        self.value = value

    def __call__(self):
        return self.value


def result(tmp_path, table, run_id, status="DELIVERED", error=None):
    return SnapshotResult(table, run_id, status, 1, 1, 2, 1, tmp_path, error)


def test_first_cycle_runs_immediately_and_success_uses_completion_delay(tmp_path):
    now = datetime(2026, 9, 15, tzinfo=UTC)
    clock = Clock(now)
    calls = []
    with SourceState(state_db_path(tmp_path / "data")) as state:
        state.initialize()

        def sync(table):
            calls.append(table)
            return result(tmp_path, table, f"run-{table}")

        cycle = CycleRunner(state, config(tmp_path), sync, clock=clock).run()
        assert cycle.status == "COMPLETED"
        assert calls == ["a", "b", "c"]
        assert next_action_at(state, config(tmp_path), now) == now + timedelta(days=7)


def test_failure_waits_and_retry_skips_delivered_table(tmp_path):
    now = datetime(2026, 9, 15, tzinfo=UTC)
    clock = Clock(now)
    calls = []
    fail_b = True
    with SourceState(state_db_path(tmp_path / "data")) as state:
        state.initialize()

        def sync(table):
            nonlocal fail_b
            calls.append(table)
            if table == "b" and fail_b:
                fail_b = False
                return result(tmp_path, table, "run-b1", "FAILED", "network down")
            return result(tmp_path, table, f"run-{table}-ok")

        first = CycleRunner(state, config(tmp_path), sync, clock=clock).run()
        assert first.status == "RETRY_WAIT"
        assert calls == ["a", "b"]
        assert next_action_at(state, config(tmp_path), now) == now + timedelta(hours=6)
        clock.value += timedelta(hours=1)
        second = CycleRunner(state, config(tmp_path), sync, clock=clock).run()
        assert second.status == "COMPLETED"  # manual run ignores retry time
        assert calls == ["a", "b", "b", "c"]


def test_active_cycle_keeps_captured_tables_after_config_change(tmp_path):
    now = datetime(2026, 9, 15, tzinfo=UTC)
    with SourceState(state_db_path(tmp_path / "data")) as state:
        state.initialize()
        state.create_cycle("cycle-fixed", ["a", "b", "c"], now.isoformat())
        changed = config(tmp_path, ("a", "b", "d"))
        calls = []

        def sync(table):
            calls.append(table)
            return result(tmp_path, table, f"run-{table}")

        CycleRunner(state, changed, sync, clock=lambda: now).run()
        assert calls == ["a", "b", "c"]


def test_interrupted_table_gets_new_run_and_prior_delivery_is_skipped(tmp_path):
    now = datetime(2026, 9, 15, tzinfo=UTC)
    with SourceState(state_db_path(tmp_path / "data")) as state:
        state.initialize()
        state.create_cycle("cycle-fixed", ["a", "b"], now.isoformat())
        state.deliver_cycle_table("cycle-fixed", "a", "run-a", now.isoformat())
        state.start_cycle_table("cycle-fixed", "b")
        state.register_table("b")
        state.begin_run("b", "stale-run")
        calls = []

        def sync(table):
            calls.append(table)
            return result(tmp_path, table, "fresh-run")

        cycle = CycleRunner(state, config(tmp_path, ("a", "b")), sync, clock=lambda: now).run()
        assert cycle.status == "COMPLETED"
        assert calls == ["b"]
        assert state.get_run("stale-run").status == "FAILED"


def test_worker_restart_before_due_waits_without_running(tmp_path):
    now = datetime(2026, 9, 15, tzinfo=UTC)
    cfg = config(tmp_path)
    with SourceState(state_db_path(tmp_path / "data")) as state:
        state.initialize()
        state.create_cycle("done", ["a"], now.isoformat())
        state.deliver_cycle_table("done", "a", "run-a", now.isoformat())
        state.complete_cycle("done", now.isoformat())
        delays = []
        worker = SourceWorker(
            state,
            cfg,
            lambda: (_ for _ in ()).throw(AssertionError("must not run")),
            clock=lambda: now,
            waiter=lambda seconds: delays.append(seconds) or True,
        )
        assert worker.tick() is None
        assert delays == [7 * 86400]


def test_worker_restart_after_due_runs_immediately(tmp_path):
    completed = datetime(2026, 9, 1, tzinfo=UTC)
    now = datetime(2026, 9, 15, tzinfo=UTC)
    cfg = config(tmp_path)
    with SourceState(state_db_path(tmp_path / "data")) as state:
        state.initialize()
        state.create_cycle("done", ["a"], completed.isoformat())
        state.deliver_cycle_table("done", "a", "run-a", completed.isoformat())
        state.complete_cycle("done", completed.isoformat())
        expected = state.latest_cycle()
        worker = SourceWorker(state, cfg, lambda: expected, clock=lambda: now)
        assert worker.tick() == expected


def test_shutdown_during_table_finishes_current_and_starts_no_next(tmp_path):
    now = datetime(2026, 9, 15, tzinfo=UTC)
    stop = Event()
    calls = []
    with SourceState(state_db_path(tmp_path / "data")) as state:
        state.initialize()

        def sync(table):
            calls.append(table)
            stop.set()
            return result(tmp_path, table, "run-a")

        cycle = CycleRunner(
            state, config(tmp_path, ("a", "b")), sync, clock=lambda: now, stop_event=stop
        ).run()
        assert calls == ["a"]
        assert cycle.status == "RUNNING"
        assert [table.status for table in state.cycle_tables(cycle.cycle_id)] == [
            "DELIVERED",
            "PENDING",
        ]


def test_failed_run_retention_never_removes_active_cycle_run(tmp_path):
    old = datetime(2026, 8, 1, tzinfo=UTC)
    now = datetime(2026, 10, 1, tzinfo=UTC)
    cfg = config(tmp_path, ("a",))
    run_dir = cfg.paths.data_dir / "outbox" / "a" / "old-run"
    run_dir.mkdir(parents=True)
    (run_dir / "chunk").write_text("data")
    with SourceState(state_db_path(tmp_path / "data")) as state:
        state.initialize()
        state.register_table("a")
        state.begin_run("a", "old-run")
        state.fail_run("a", "old-run", "failed")
        state._conn.execute(
            "UPDATE sync_runs SET created_at=? WHERE run_id='old-run'", (old.isoformat(),)
        )
        state.create_cycle("active", ["a"], old.isoformat())
        state.fail_cycle_table("active", "a", "old-run", "failed", now.isoformat())
        assert cleanup_failed_runs(state, cfg, clock=lambda: now) == 0
        assert run_dir.exists()
        state.abandon_active_cycle()
        assert cleanup_failed_runs(state, cfg, clock=lambda: now) == 1
        assert not run_dir.exists()


def test_cli_cycle_delivery_honors_removed_captured_table(tmp_path, monkeypatch):
    import airgap_sync.cli as cli_module

    cfg = config(tmp_path, ("a",))
    cfg = cfg.model_copy(update={"relay": RelayConfig(base_url="http://relay", token_env="TOKEN")})
    monkeypatch.setenv("TOKEN", "secret")
    seen = []

    class Connection:
        def __init__(self, mysql):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

    class Delivery:
        def __init__(self, connection, state, effective, uploader):
            self.effective = effective

        def sync(self, table):
            seen.append(self.effective.table(table).enabled)
            return result(tmp_path, table, "run-c")

    monkeypatch.setattr(cli_module, "SourceMySQLConnection", Connection)
    monkeypatch.setattr(cli_module, "DeliveryRunner", Delivery)
    monkeypatch.setattr(cli_module, "RelayUploader", lambda relay, token: object())
    runner = cli_module._cycle_runner(cfg, object())
    runner.sync_table("captured-but-removed")
    assert seen == [True]
