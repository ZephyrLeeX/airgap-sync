"""Destination polling worker and conservative incoming maintenance."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from threading import Event

from airgap_sync.common.models import AppConfig
from airgap_sync.common.runtime import parse_duration
from airgap_sync.common.transport import parse_transport_filename
from airgap_sync.destination.incoming import discover_runs
from airgap_sync.destination.mysql import DestinationMySQLConnection
from airgap_sync.destination.processor import ProcessResult, process_once, retry_pending_cleanup

logger = logging.getLogger(__name__)


def cleanup_orphan_artifacts(
    connection: DestinationMySQLConnection,
    config: AppConfig,
    *,
    now: datetime | None = None,
) -> int:
    """Delete only old, formal artifacts without a manifest or active metadata run."""
    assert config.destination is not None
    incoming = config.destination.incoming_dir
    if not incoming.exists():
        return 0
    cutoff = (now or datetime.now(UTC)) - timedelta(
        seconds=parse_duration(config.maintenance.destination_orphan_retention)
    )
    manifests = set(discover_runs(incoming))
    active = connection.active_run_ids()
    removed = 0
    for path in incoming.iterdir():
        if not path.is_file():
            continue
        try:
            run_id, logical = parse_transport_filename(path.name)
        except ValueError:
            continue
        if logical == "manifest.json" or run_id in manifests or run_id in active:
            continue
        modified = datetime.fromtimestamp(path.stat().st_mtime, UTC)
        if modified >= cutoff:
            continue
        path.unlink()
        removed += 1
        logger.info("destination orphan removed: run_id=%s artifact=%s", run_id, logical)
    return removed


class DestinationWorker:
    def __init__(
        self,
        connection: DestinationMySQLConnection,
        config: AppConfig,
        *,
        stop_event: Event | None = None,
        waiter: Callable[[float], bool] | None = None,
    ) -> None:
        self.connection = connection
        self.config = config
        self.stop_event = stop_event or Event()
        self.waiter = waiter or self.stop_event.wait
        self._last_results: dict[str, tuple[str, str | None]] = {}

    def run_once(self) -> list[ProcessResult]:
        results = process_once(self.connection, self.config)
        for result in results:
            current = (result.status, result.error)
            previous = self._last_results.get(result.run_id)
            level = logging.DEBUG if previous == current else logging.WARNING
            if result.status in {"FAILED", "MISMATCH"}:
                logger.log(
                    level,
                    "destination run result: run_id=%s table=%s result=%s error=%s",
                    result.run_id,
                    result.table,
                    result.status,
                    result.error,
                )
            else:
                logger.info(
                    "destination run result: run_id=%s table=%s result=%s",
                    result.run_id,
                    result.table,
                    result.status,
                )
            self._last_results[result.run_id] = current
        cleanup = retry_pending_cleanup(self.connection, self.config)
        cleanup_orphan_artifacts(self.connection, self.config)
        return [*results, *cleanup]

    def run(self) -> None:
        poll = parse_duration(self.config.destination_worker.poll_interval)
        while not self.stop_event.is_set():
            self.run_once()
            if self.waiter(poll):
                break
