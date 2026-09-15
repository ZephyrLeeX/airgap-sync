"""Long-running process primitives: strict durations, interruptible waits and locks."""

from __future__ import annotations

import os
import re
from pathlib import Path
from threading import Event
from typing import BinaryIO

_DURATION_RE = re.compile(r"^([1-9][0-9]*)([smhd])$")
_MULTIPLIERS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(value: str) -> int:
    match = _DURATION_RE.fullmatch(value)
    if match is None:
        raise ValueError("duration must match ^[1-9][0-9]*[smhd]$")
    return int(match.group(1)) * _MULTIPLIERS[match.group(2)]


class WorkerAlreadyRunning(RuntimeError):
    pass


class ProcessLock:
    """Small non-blocking process lock using the platform's native file locking."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._handle: BinaryIO | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            if os.name == "nt":  # pragma: no cover - exercised on Windows
                import msvcrt

                handle.seek(0)
                if handle.tell() == handle.seek(0, 2) == 0:
                    handle.write(b"0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, PermissionError) as exc:
            handle.close()
            raise WorkerAlreadyRunning(f"WORKER_ALREADY_RUNNING: {self.path}") from exc
        self._handle = handle

    def release(self) -> None:
        if self._handle is None:
            return
        try:
            if os.name == "nt":  # pragma: no cover
                import msvcrt

                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None

    def __enter__(self) -> ProcessLock:
        self.acquire()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.release()


def wait(stop_event: Event, seconds: float) -> bool:
    """Return True when shutdown interrupted the timeout."""
    return stop_event.wait(max(0.0, seconds))
