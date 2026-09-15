"""Persisted completion-driven Source cycles and fixed-delay worker."""

from __future__ import annotations

import logging
import shutil
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from threading import Event

from airgap_sync.common.models import AppConfig
from airgap_sync.common.runtime import parse_duration
from airgap_sync.source.snapshot import SnapshotResult
from airgap_sync.source.state import SourceState, SyncCycle

logger = logging.getLogger(__name__)
Clock = Callable[[], datetime]


def utcnow() -> datetime:
    return datetime.now(UTC)


def generate_cycle_id(now: datetime | None = None) -> str:
    value = (now or utcnow()).astimezone(UTC)
    return f"cycle-{value:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds")


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)


class CycleRunner:
    def __init__(
        self,
        state: SourceState,
        config: AppConfig,
        sync_table: Callable[[str], SnapshotResult],
        *,
        clock: Clock = utcnow,
        stop_event: Event | None = None,
    ) -> None:
        self.state = state
        self.config = config
        self.sync_table = sync_table
        self.clock = clock
        self.stop_event = stop_event or Event()

    def run(self) -> SyncCycle:
        self.state.recover_interrupted_cycle()
        cycle = self.state.active_cycle()
        if cycle is None:
            now = self.clock()
            cycle_id = generate_cycle_id(now)
            self.state.create_cycle(
                cycle_id, [table.name for table in self.config.enabled_tables], _iso(now)
            )
            cycle = self.state.active_cycle()
            assert cycle is not None
        logger.info("source cycle running: cycle_id=%s", cycle.cycle_id)
        for table in self.state.cycle_tables(cycle.cycle_id):
            if table.status == "DELIVERED":
                continue
            if self.stop_event.is_set():
                break
            self.state.start_cycle_table(cycle.cycle_id, table.table_name)
            logger.info(
                "source cycle table starting: cycle_id=%s table=%s",
                cycle.cycle_id,
                table.table_name,
            )
            try:
                result = self.sync_table(table.table_name)
            except Exception as exc:
                self._failed(cycle.cycle_id, table.table_name, None, str(exc))
                break
            if result.status != "DELIVERED":
                self._failed(
                    cycle.cycle_id,
                    table.table_name,
                    result.run_id,
                    result.error or f"sync ended with {result.status}",
                )
                break
            self.state.deliver_cycle_table(
                cycle.cycle_id, table.table_name, result.run_id, _iso(self.clock())
            )
            logger.info(
                "source cycle table delivered: cycle_id=%s table=%s run_id=%s",
                cycle.cycle_id,
                table.table_name,
                result.run_id,
            )
        tables = self.state.cycle_tables(cycle.cycle_id)
        if not tables or all(table.status == "DELIVERED" for table in tables):
            self.state.complete_cycle(cycle.cycle_id, _iso(self.clock()))
        latest = self.state.latest_cycle()
        assert latest is not None
        return latest

    def _failed(self, cycle_id: str, table_name: str, run_id: str | None, error: str) -> None:
        retry = self.clock() + timedelta(
            seconds=parse_duration(self.config.schedule.retry_after_failure)
        )
        self.state.fail_cycle_table(cycle_id, table_name, run_id, error, _iso(retry))
        logger.warning(
            "source cycle retry wait: cycle_id=%s table=%s run_id=%s next_action=%s error=%s",
            cycle_id,
            table_name,
            run_id,
            _iso(retry),
            error,
        )


def next_action_at(state: SourceState, config: AppConfig, now: datetime) -> datetime:
    active = state.active_cycle()
    if active is not None:
        if active.status == "RUNNING" or active.next_attempt_at is None:
            return now
        return _parse(active.next_attempt_at)
    completed = state.latest_completed_cycle()
    if completed is None or completed.completed_at is None:
        return now
    return _parse(completed.completed_at) + timedelta(
        seconds=parse_duration(config.schedule.delay_after_success)
    )


class SourceWorker:
    """Injectable scheduler loop; ``tick`` performs at most one Cycle."""

    def __init__(
        self,
        state: SourceState,
        config: AppConfig,
        run_cycle: Callable[[], SyncCycle],
        *,
        stop_event: Event | None = None,
        clock: Clock = utcnow,
        waiter: Callable[[float], bool] | None = None,
        maintenance: Callable[[], object] | None = None,
    ) -> None:
        self.state = state
        self.config = config
        self.run_cycle = run_cycle
        self.stop_event = stop_event or Event()
        self.clock = clock
        self.waiter = waiter or self.stop_event.wait
        self.maintenance = maintenance or (lambda: None)

    def _run_maintenance(self) -> None:
        try:
            self.maintenance()
        except OSError as exc:
            logger.warning("source worker maintenance deferred: error=%s", exc)

    def tick(self) -> SyncCycle | None:
        now = self.clock()
        due = next_action_at(self.state, self.config, now)
        delay = max(0.0, (due - now).total_seconds())
        if delay and self.waiter(delay):
            return None
        if self.stop_event.is_set():
            return None
        cycle = self.run_cycle()
        self._run_maintenance()
        logger.info(
            "source worker cycle result: cycle_id=%s cycle_status=%s next_action=%s",
            cycle.cycle_id,
            cycle.status,
            next_action_at(self.state, self.config, self.clock()).isoformat(),
        )
        return cycle

    def run(self) -> None:
        self._run_maintenance()
        while not self.stop_event.is_set():
            self.tick()


def cleanup_failed_runs(state: SourceState, config: AppConfig, *, clock: Clock = utcnow) -> int:
    cutoff = clock() - timedelta(seconds=parse_duration(config.maintenance.failed_run_retention))
    removed = 0
    protected = state.active_cycle_run_ids()
    for run in state.failed_runs_before(_iso(cutoff)):
        if run.run_id in protected:
            continue
        run_dir = config.paths.data_dir / "outbox" / run.table_name / run.run_id
        if run_dir.exists():
            try:
                shutil.rmtree(run_dir)
            except OSError as exc:
                logger.warning(
                    "source failed-run cleanup deferred: table=%s run_id=%s path=%s error=%s",
                    run.table_name,
                    run.run_id,
                    run_dir,
                    exc,
                )
                continue
            removed += 1
    return removed
