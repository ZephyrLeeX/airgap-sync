"""Best-effort local observations; no worker lock or state mutation."""

from __future__ import annotations

import os
import platform
import shutil
import socket
import subprocess
import sys
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airgap_sync import __version__
from airgap_sync.destination.incoming import discover_runs


def _proc_fields(path: str) -> dict[str, int]:
    result = {}
    for line in Path(path).read_text().splitlines():
        key, _, value = line.partition(":")
        if value:
            try:
                result[key] = int(value.strip().split()[0]) * 1024
            except (ValueError, IndexError):
                continue
    return result


def worker_status() -> dict:
    if sys.platform != "linux" or shutil.which("systemctl") is None:
        return {"status": "UNKNOWN", "detail": "systemd unavailable"}
    try:
        result = subprocess.run(
            [
                "systemctl",
                "show",
                "airgap-sync-destination.service",
                "--property=ActiveState,SubState,MainPID",
            ],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        if result.returncode:
            return {"status": "UNKNOWN", "detail": "service query unavailable"}
        fields = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
        active = fields.get("ActiveState")
        if active not in ("active", "inactive", "failed"):
            return {"status": "UNKNOWN", "detail": "service state unavailable"}
        return {
            "status": "RUNNING" if active == "active" else "NOT RUNNING",
            "detail": fields.get("SubState", "—"),
            "pid": fields.get("MainPID", "—"),
        }
    except (OSError, subprocess.TimeoutExpired):
        return {"status": "UNKNOWN", "detail": "service query unavailable"}


def system_snapshot(incoming_dir: Path) -> dict:
    result = {
        "hostname": socket.gethostname(),
        "os": platform.platform(),
        "version": __version__,
        "worker": worker_status(),
        "monitor": "RUNNING",
        "incoming_dir": str(incoming_dir),
        "incoming_candidates": None,
        "incoming_bytes": None,
        "incoming_error": None,
        "filesystems": [],
        "uptime": None,
        "boot_time": None,
        "cpu_load": None,
        "memory_total": None,
        "memory_available": None,
        "memory_used_percent": None,
        "swap_total": None,
        "swap_free": None,
    }
    try:
        uptime = float(Path("/proc/uptime").read_text().split()[0])
        result["uptime"] = str(timedelta(seconds=int(uptime)))
        result["boot_time"] = (datetime.now(UTC) - timedelta(seconds=uptime)).isoformat()
    except (OSError, ValueError, IndexError):
        pass
    with suppress(OSError, AttributeError):
        result["cpu_load"] = os.getloadavg()[0]
    try:
        mem = _proc_fields("/proc/meminfo")
        total, available = mem.get("MemTotal"), mem.get("MemAvailable")
        result.update(
            memory_total=total,
            memory_available=available,
            memory_used_percent=round(100 * (total - available) / total, 1)
            if total and available is not None
            else None,
            swap_total=mem.get("SwapTotal"),
            swap_free=mem.get("SwapFree"),
        )
    except OSError:
        pass
    seen = set()
    for label, path in (
        ("root", Path("/")),
        ("incoming", incoming_dir),
        ("install", Path(__file__).resolve()),
    ):
        try:
            resolved = path if path.exists() else path.parent
            device = os.stat(resolved).st_dev
            if device in seen:
                continue
            seen.add(device)
            usage = shutil.disk_usage(resolved)
            result["filesystems"].append(
                {
                    "label": label,
                    "path": str(resolved),
                    "total": usage.total,
                    "free": usage.free,
                    "used_percent": round(100 * usage.used / usage.total, 1),
                }
            )
        except (OSError, ZeroDivisionError):
            continue
    try:
        if not incoming_dir.is_dir():
            raise OSError("incoming directory unavailable")
        result["incoming_candidates"] = len(discover_runs(incoming_dir))
        result["incoming_bytes"] = sum(
            entry.stat().st_size for entry in incoming_dir.iterdir() if entry.is_file()
        )
    except OSError:
        result["incoming_error"] = "incoming directory unavailable"
    return result
