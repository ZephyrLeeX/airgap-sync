from __future__ import annotations

import pytest

from airgap_sync.common.runtime import ProcessLock, WorkerAlreadyRunning, parse_duration


@pytest.mark.parametrize(
    ("value", "seconds"), [("30s", 30), ("10m", 600), ("6h", 21600), ("7d", 604800)]
)
def test_parse_duration(value, seconds):
    assert parse_duration(value) == seconds


@pytest.mark.parametrize("value", ["0s", "1.5h", "7 days", "P1D", " 1h", "1w"])
def test_parse_duration_rejects_extended_syntax(value):
    with pytest.raises(ValueError):
        parse_duration(value)


def test_process_lock_is_exclusive_and_reusable(tmp_path):
    first = ProcessLock(tmp_path / "worker.lock")
    second = ProcessLock(tmp_path / "worker.lock")
    first.acquire()
    with pytest.raises(WorkerAlreadyRunning, match="WORKER_ALREADY_RUNNING"):
        second.acquire()
    first.release()
    second.acquire()
    second.release()
