"""Windows transient filesystem lock retry tests."""

from __future__ import annotations

import errno
from pathlib import Path

import pytest

from airgap_sync.common.fsutil import retry_windows_file_lock


def windows_error(winerror: int) -> OSError:
    error = OSError(f"winerror {winerror}")
    error.winerror = winerror  # type: ignore[attr-defined]
    return error


@pytest.mark.parametrize("winerror", [32, 33])
def test_windows_lock_is_retried(winerror: int) -> None:
    attempts = 0

    def action() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise windows_error(winerror)
        return "ok"

    sleeps: list[float] = []
    assert (
        retry_windows_file_lock("replace", Path("chunk.part"), action, sleep=sleeps.append) == "ok"
    )
    assert attempts == 2
    assert sleeps == [0.05]


def test_all_windows_lock_attempts_fail_with_last_original_exception() -> None:
    errors = [windows_error(32) for _ in range(3)]
    last_error = errors[-1]

    def action() -> None:
        raise errors.pop(0)

    with pytest.raises(OSError) as caught:
        retry_windows_file_lock(
            "unlink", Path("chunk.part"), action, delays=(0.1, 0.2), sleep=lambda _: None
        )
    assert caught.value is last_error


@pytest.mark.parametrize(
    "error",
    [OSError(errno.ENOSPC, "disk full"), PermissionError(errno.EACCES, "denied")],
)
def test_non_transient_error_fails_immediately(error: OSError) -> None:
    attempts = 0

    def action() -> None:
        nonlocal attempts
        attempts += 1
        raise error

    with pytest.raises(OSError) as caught:
        retry_windows_file_lock("replace", Path("chunk.part"), action, sleep=lambda _: None)
    assert caught.value is error
    assert attempts == 1
