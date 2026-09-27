"""Independent, bounded Source telemetry sample and best-effort delivery."""

from __future__ import annotations

import hashlib
import json
import logging
import ntpath
import os
import platform
import shutil
import socket
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
from contextlib import closing, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from secrets import token_hex

from airgap_sync import __version__
from airgap_sync.common.config import resolve_relay_token
from airgap_sync.common.models import AppConfig
from airgap_sync.common.runtime import parse_duration
from airgap_sync.source.state import SCHEMA_VERSION, state_db_path
from airgap_sync.source.uploader import RelayUploader, UploadError

logger = logging.getLogger(__name__)
MAX_PAYLOAD = 64 * 1024
_STORAGE_MAX_ENTRIES = 100_000
_STORAGE_MAX_SECONDS = 15
_COLLECTION_CODES = frozenset(
    {
        "SOURCE_METADATA_MISSING",
        "SOURCE_METADATA_SCHEMA",
        "SOURCE_METADATA_UNAVAILABLE",
        "CYCLE_TABLE_LIMIT",
        "RUN_ARTIFACT_LIMIT",
        "SYSTEM_UNAVAILABLE",
        "WORKER_QUERY_UNAVAILABLE",
        "UPTIME_UNAVAILABLE",
        "CPU_UNAVAILABLE",
        "MEMORY_UNAVAILABLE",
        "FILESYSTEM_UNAVAILABLE",
        "STORAGE_UNAVAILABLE",
        "COLLECTION_ERROR",
    }
)
_UPLOAD_CODES = frozenset(
    {
        "UPLOAD_RESPONSE_MISMATCH",
        "REMOTE_FILE_EXISTS_AMBIGUOUS",
        "UPLOAD_RETRY_EXHAUSTED",
        "UPLOAD_HTTP_ERROR",
    }
)
_STORAGE_FIELDS = (
    "outbox_bytes",
    "failed_runs_bytes",
    "backups_bytes",
    "logs_bytes",
    "pending_spool_bytes",
)


def _utc(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return (
            parsed.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
            if parsed.tzinfo
            else None
        )
    except ValueError:
        return None


def _query_one(db: sqlite3.Connection, sql: str, args: tuple = ()) -> dict | None:
    row = db.execute(sql, args).fetchone()
    return dict(row) if row else None


def metadata(config: AppConfig, now: datetime) -> tuple[dict, list[str]]:
    """Open SQLite read-only; never create or migrate the business database."""
    result: dict = {"cycle": None, "current_run": None, "last_run": None, "next_action_at": None}
    errors = []
    assert config.paths is not None
    path = state_db_path(config.paths.data_dir)
    try:
        if not path.is_file():
            return result, ["SOURCE_METADATA_MISSING"]
        with closing(
            sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=1)
        ) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            db.execute("PRAGMA busy_timeout=1000")
            version = _query_one(db, "SELECT version FROM schema_version LIMIT 1")
            if not version or version["version"] != SCHEMA_VERSION:
                return result, ["SOURCE_METADATA_SCHEMA"]
            cycle = _query_one(
                db,
                (
                    "SELECT cycle_id,status,created_at,started_at,completed_at,next_attempt_at "
                    "FROM sync_cycles ORDER BY created_at DESC LIMIT 1"
                ),
            )
            if cycle:
                statuses = db.execute(
                    "SELECT status FROM cycle_tables WHERE cycle_id=? LIMIT 1001",
                    (cycle["cycle_id"],),
                ).fetchall()
                bounded = len(statuses) <= 1000
                if not bounded:
                    errors.append("CYCLE_TABLE_LIMIT")
                result["cycle"] = {
                    **{
                        key: value
                        for key, value in cycle.items()
                        if key
                        not in {"created_at", "started_at", "completed_at", "next_attempt_at"}
                    },
                    "started_at": _utc(cycle["started_at"]),
                    "completed_at": _utc(cycle["completed_at"]),
                    "tables_total": len(statuses) if bounded else None,
                    "tables_delivered": (
                        sum(row["status"] == "DELIVERED" for row in statuses) if bounded else None
                    ),
                    "tables_failed": (
                        sum(row["status"] == "FAILED" for row in statuses) if bounded else None
                    ),
                }
                if cycle["status"] == "RETRY_WAIT":
                    result["next_action_at"] = _utc(cycle["next_attempt_at"])
                elif cycle["status"] == "RUNNING":
                    result["next_action_at"] = now.isoformat(timespec="seconds").replace(
                        "+00:00", "Z"
                    )
            if result["next_action_at"] is None:
                completed = _query_one(
                    db,
                    (
                        "SELECT completed_at FROM sync_cycles WHERE status='COMPLETED' "
                        "ORDER BY completed_at DESC LIMIT 1"
                    ),
                )
                if completed and _utc(completed["completed_at"]):
                    due = datetime.fromisoformat(
                        _utc(completed["completed_at"]).replace("Z", "+00:00")
                    ) + timedelta(seconds=parse_duration(config.schedule.delay_after_success))
                    result["next_action_at"] = due.isoformat(timespec="seconds").replace(
                        "+00:00", "Z"
                    )
            for name, sql in (
                (
                    "current_run",
                    (
                        "SELECT run_id,table_name,status,created_at,row_count,chunk_count, "
                        "raw_bytes, "
                        "compressed_bytes FROM sync_runs WHERE status IN "
                        "('GENERATING','UPLOADING','FINALIZING') ORDER BY created_at DESC LIMIT 1"
                    ),
                ),
                (
                    "last_run",
                    (
                        "SELECT run_id,table_name,status,created_at,row_count,chunk_count, "
                        "raw_bytes, "
                        "compressed_bytes FROM sync_runs ORDER BY created_at DESC LIMIT 1"
                    ),
                ),
            ):
                run = _query_one(db, sql)
                if run:
                    artifacts = db.execute(
                        "SELECT kind,upload_status,size FROM run_artifacts "
                        "WHERE run_id=? LIMIT 10001",
                        (run["run_id"],),
                    ).fetchall()
                    bounded_artifacts = len(artifacts) <= 10000
                    if not bounded_artifacts:
                        errors.append("RUN_ARTIFACT_LIMIT")
                    result[name] = {
                        "run_id": run["run_id"],
                        "table": run["table_name"],
                        "status": run["status"],
                        "created_at": _utc(run["created_at"]),
                        "rows_scanned": run["row_count"],
                        "chunks_created": run["chunk_count"],
                        "raw_bytes": run["raw_bytes"],
                        "compressed_bytes": run["compressed_bytes"],
                        "chunks_uploaded": (
                            sum(
                                row["kind"] == "chunk" and row["upload_status"] == "UPLOADED"
                                for row in artifacts
                            )
                            if bounded_artifacts
                            else None
                        ),
                        "pending_bytes": (
                            sum(
                                row["size"]
                                for row in artifacts
                                if row["upload_status"] != "UPLOADED"
                            )
                            if bounded_artifacts
                            else None
                        ),
                    }
    except (sqlite3.Error, OSError, ValueError):
        errors.append("SOURCE_METADATA_UNAVAILABLE")
    return result, errors


def _powershell(script: str) -> str:
    proc = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
        capture_output=True,
        text=True,
        timeout=3,
        check=False,
    )
    if proc.returncode:
        raise OSError("PowerShell query failed")
    return proc.stdout.strip()


def _windows_argv(command: str) -> list[str]:
    """Parse CRT quotes/backslashes, not POSIX shell quoting.

    See Microsoft Learn: cpp/c-language/parsing-c-command-line-arguments.
    Reject unterminated quotes conservatively instead of guessing identity.
    """
    args = []
    i = 0
    while i < len(command):
        while i < len(command) and command[i] in " \t":
            i += 1
        if i == len(command):
            break
        value = []
        quoted = False
        while i < len(command) and (quoted or command[i] not in " \t"):
            slashes = 0
            while i < len(command) and command[i] == "\\":
                slashes += 1
                i += 1
            if i < len(command) and command[i] == '"':
                value.append("\\" * (slashes // 2))
                if slashes % 2:
                    value.append('"')
                elif quoted and i + 1 < len(command) and command[i + 1] == '"':
                    value.append('"')
                    i += 1
                else:
                    quoted = not quoted
                i += 1
            else:
                value.append("\\" * slashes)
                if i < len(command) and (quoted or command[i] not in " \t"):
                    value.append(command[i])
                    i += 1
        if quoted:
            raise ValueError("unclosed command line quote")
        args.append("".join(value))
    return args


def _windows_absolute(path: str) -> str | None:
    # Rooted (\foo) and drive-relative (C:foo) paths need the *worker's* cwd.
    drive, tail = ntpath.splitdrive(path)
    if not drive or not tail.startswith(("\\", "/")):
        return None
    # Device namespaces have different normalization semantics; do not guess.
    if path.startswith(("\\\\?", "\\\\.")):
        return None
    return ntpath.normcase(ntpath.normpath(path))


def _worker_arguments(args: list[str]) -> tuple[str, str | None, list[str]] | None:
    """Recognize the installed console launcher and its Python child only."""
    if not args:
        return None
    executable = ntpath.basename(args[0]).lower()
    if executable == "airgap-sync.exe":
        kind, entrypoint, rest = "launcher", _windows_absolute(args[0]), args[1:]
    elif executable in {"python.exe", "pythonw.exe"}:
        if len(args) > 1 and ntpath.basename(args[1]).lower() == "airgap-sync.exe":
            kind, entrypoint, rest = "python", _windows_absolute(args[1]), args[2:]
        elif args[1:3] == ["-m", "airgap_sync.cli"]:
            kind, entrypoint, rest = "python", "module:airgap_sync.cli", args[3:]
        else:
            return None
    else:
        return None
    return (kind, entrypoint, rest[2:]) if rest[:2] == ["source", "worker"] else None


def worker(config_path: Path, task_name: str) -> dict:
    result = {"status": "UNKNOWN", "pid": None, "task_status": "UNKNOWN"}
    if sys.platform != "win32":
        return result
    try:
        escaped = task_name.replace("'", "''")
        result["task_status"] = (
            _powershell(f"(Get-ScheduledTask -TaskName '{escaped}' -ErrorAction Stop).State")
            or "UNKNOWN"
        )
    except (OSError, subprocess.TimeoutExpired):
        pass
    try:
        raw = _powershell(
            "Get-CimInstance Win32_Process -ErrorAction Stop -Filter \"Name='python.exe' OR "
            "Name='pythonw.exe' OR Name='airgap-sync.exe'\" | "
            "Select-Object ProcessId,ParentProcessId,Name,CommandLine | ConvertTo-Json -Compress"
        )
        processes = json.loads(raw) if raw else []
        if isinstance(processes, dict):
            processes = [processes]
        # Only our own CLI path may be made absolute using our cwd. Do not resolve
        # junctions on one side only; lexical normalization handles wrapper '..'.
        target = _windows_absolute(str(config_path)) or _windows_absolute(
            os.path.abspath(config_path)
        )
        if target is None:
            return result
        matched = {}
        uncertain = False
        for process in processes or []:
            pid = int(process["ProcessId"])
            if pid == os.getpid():
                continue
            command = process.get("CommandLine")
            if not isinstance(command, str) or not command.strip():
                uncertain = True
                continue
            try:
                candidate = _worker_arguments(_windows_argv(command))
            except ValueError:
                uncertain = True
                continue
            if candidate is None:
                continue
            kind, entrypoint, args = candidate
            selected = []
            for index, arg in enumerate(args):
                if arg == "--config":
                    selected.append(args[index + 1] if index + 1 < len(args) else "")
                elif arg.startswith("--config="):
                    selected.append(arg.partition("=")[2])
            identity = _windows_absolute(selected[0]) if len(selected) == 1 else None
            if identity is None:
                uncertain = True
            elif identity == target:
                matched[pid] = (kind, int(process.get("ParentProcessId") or 0), entrypoint)
        # distlib waits for Python; Windows venv Python can itself redirect to
        # another Python process. Collapse direct edges only for the same known
        # entrypoint and config, and report the final child PID. Never merge peers.
        parents = {
            parent
            for kind, parent, entrypoint in matched.values()
            if kind == "python"
            and entrypoint is not None
            and parent in matched
            and matched[parent][2] == entrypoint
        }
        independent = set(matched) - parents
        if not uncertain and (len(independent) == 1 or not matched):
            result["status"] = "RUNNING" if independent else "STOPPED"
            result["pid"] = next(iter(independent), None)
    except (OSError, subprocess.TimeoutExpired, ValueError, KeyError, TypeError):
        pass
    return result


def host_system(config: AppConfig) -> tuple[dict, dict]:
    assert config.monitoring is not None
    host = {
        "hostname": socket.gethostname(),
        "os": platform.platform(),
        "uptime_seconds": None,
        "boot_time": None,
    }
    system = {
        "cpu_percent": None,
        "memory_total_bytes": None,
        "memory_available_bytes": None,
        "memory_used_percent": None,
    }
    if sys.platform == "win32":
        try:
            data = json.loads(
                _powershell(
                    "$o=Get-CimInstance Win32_OperatingSystem; "
                    "@{boot=$o.LastBootUpTime.ToUniversalTime().ToString('o');"
                    "total=$o.TotalVisibleMemorySize;free=$o.FreePhysicalMemory}"
                    "|ConvertTo-Json -Compress"
                )
            )
            host["boot_time"] = _utc(data["boot"])
            host["uptime_seconds"] = max(
                0,
                int(
                    (
                        datetime.now(UTC)
                        - datetime.fromisoformat(data["boot"].replace("Z", "+00:00"))
                    ).total_seconds()
                ),
            )
            if config.monitoring.collect_memory:
                total, free = int(data["total"]) * 1024, int(data["free"]) * 1024
                system.update(
                    memory_total_bytes=total,
                    memory_available_bytes=free,
                    memory_used_percent=round(100 * (total - free) / total, 1) if total else None,
                )
        except (OSError, subprocess.TimeoutExpired, ValueError, KeyError, TypeError):
            pass
        if config.monitoring.collect_cpu:
            with suppress(OSError, subprocess.TimeoutExpired, ValueError):
                system["cpu_percent"] = float(
                    _powershell(
                        "(Get-CimInstance Win32_Processor | Measure-Object "
                        "-Property LoadPercentage -Average).Average"
                    )
                )
    elif sys.platform == "linux":
        try:
            uptime = float(Path("/proc/uptime").read_text().split()[0])
            host["uptime_seconds"] = int(uptime)
            host["boot_time"] = (
                (datetime.now(UTC) - timedelta(seconds=uptime))
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z")
            )
        except (OSError, ValueError, IndexError):
            pass
        if config.monitoring.collect_memory:
            try:
                fields = {
                    line.split(":", 1)[0]: int(line.split()[1]) * 1024
                    for line in Path("/proc/meminfo").read_text().splitlines()
                    if ":" in line
                }
                total, free = fields.get("MemTotal"), fields.get("MemAvailable")
                system.update(
                    memory_total_bytes=total,
                    memory_available_bytes=free,
                    memory_used_percent=round(100 * (total - free) / total, 1)
                    if total and free is not None
                    else None,
                )
            except (OSError, ValueError, IndexError):
                pass
    return host, system


def filesystems(config: AppConfig, config_path: Path) -> list[dict]:
    assert config.paths is not None
    locations = {
        "system": Path(os.environ.get("SYSTEMROOT", "/")),
        "install": Path(sys.executable),
        "config": config_path.parent,
        "data": config.paths.data_dir,
        "temp": Path(tempfile.gettempdir()),
    }
    volumes: dict[str, dict] = {}
    for label, path in locations.items():
        try:
            existing = path.resolve()
            while not existing.exists() and existing != existing.parent:
                existing = existing.parent
            if sys.platform == "win32":
                import ctypes

                kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
                mount_buffer = ctypes.create_unicode_buffer(32768)
                if not kernel32.GetVolumePathNameW(str(existing), mount_buffer, len(mount_buffer)):
                    raise OSError("volume path unavailable")
                mount = mount_buffer.value
                volume_buffer = ctypes.create_unicode_buffer(32768)
                if kernel32.GetVolumeNameForVolumeMountPointW(
                    mount, volume_buffer, len(volume_buffer)
                ):
                    key = volume_buffer.value.casefold()
                else:
                    key = mount.casefold()
            else:
                key = str(os.stat(existing).st_dev)
                mount_path = existing if existing.is_dir() else existing.parent
                while mount_path != mount_path.parent and not os.path.ismount(mount_path):
                    mount_path = mount_path.parent
                mount = str(mount_path)
            if key not in volumes:
                usage = shutil.disk_usage(existing)
                volumes[key] = {
                    "mount": mount,
                    "total_bytes": usage.total,
                    "free_bytes": usage.free,
                    "free_percent": round(100 * usage.free / usage.total, 1)
                    if usage.total
                    else None,
                    "locations": [],
                }
            volumes[key]["locations"].append(label)
        except OSError:
            volumes[f"error:{label}"] = {
                "mount": None,
                "total_bytes": None,
                "free_bytes": None,
                "free_percent": None,
                "locations": [label],
            }
    return list(volumes.values())


def _is_storage_link(info: os.stat_result) -> bool:
    return stat.S_ISLNK(info.st_mode) or bool(
        getattr(info, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT
    )


def _directory_bytes(path: Path) -> int:
    """Cooperative budgets between I/O calls; cannot interrupt a blocked syscall."""
    deadline = time.monotonic() + _STORAGE_MAX_SECONDS
    scanned = 0

    def check_budget() -> None:
        if time.monotonic() >= deadline or scanned > _STORAGE_MAX_ENTRIES:
            raise OSError("managed storage scan budget exceeded")

    # Reject links/reparse points in the root and its ancestors, including dangling
    # links. exists() would misreport a dangling root link as an empty directory.
    for component in (*reversed(path.absolute().parents), path.absolute()):
        check_budget()
        try:
            info = component.lstat()
        except FileNotFoundError:
            check_budget()
            return 0
        check_budget()
        if _is_storage_link(info):
            raise OSError("managed storage root traverses a link")
    total = 0
    stack = [path]
    while stack:
        check_budget()
        current = stack.pop()
        # Recheck queued directories before opening them (best effort against changes).
        if _is_storage_link(current.lstat()):
            raise OSError("managed storage directory became a link")
        check_budget()
        with os.scandir(current) as entries:
            iterator = iter(entries)
            while True:
                check_budget()
                entry = next(iterator, None)
                check_budget()
                if entry is None:
                    break
                scanned += 1
                check_budget()
                info = entry.stat(follow_symlinks=False)
                check_budget()
                if _is_storage_link(info):
                    continue
                if stat.S_ISDIR(info.st_mode):
                    stack.append(Path(entry.path))
                elif stat.S_ISREG(info.st_mode):
                    total += info.st_size
        check_budget()
    return total


def managed_storage(config: AppConfig, now: datetime) -> dict:
    assert config.paths is not None and config.monitoring is not None
    cache = config.paths.data_dir / "monitor" / "storage-cache.json"
    interval = parse_duration(config.monitoring.managed_storage_interval)
    roots = {
        "outbox_bytes": [config.paths.data_dir / "outbox"],
        "backups_bytes": [config.paths.data_dir / "backups"],
        "logs_bytes": config.monitoring.log_dirs,
        "pending_spool_bytes": [config.paths.data_dir / "spool"],
    }
    # Keep local path identities out of telemetry; version also invalidates old caches.
    paths_key = hashlib.sha256(
        json.dumps(
            {
                key: sorted(os.path.normcase(os.path.abspath(path)) for path in paths)
                for key, paths in roots.items()
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    try:
        with cache.open("rb") as handle:
            cached_bytes = handle.read(4097)
        if len(cached_bytes) > 4096:
            raise ValueError("oversized cache")
        old = json.loads(cached_bytes)
        if not isinstance(old, dict) or set(old) != {"captured_at", "paths_key", *_STORAGE_FIELDS}:
            raise ValueError("invalid cache fields")
        if (
            any(
                value is not None and (type(value) is not int or value < 0)
                for key, value in old.items()
                if key in _STORAGE_FIELDS
            )
            or old["failed_runs_bytes"] is not None
        ):
            raise ValueError("invalid cache values")
        captured = _utc(old.get("captured_at"))
        if (
            old["paths_key"] == paths_key
            and captured
            and 0
            <= (now - datetime.fromisoformat(captured.replace("Z", "+00:00"))).total_seconds()
            < interval
        ):
            return {key: value for key, value in old.items() if key != "paths_key"}
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    result = {
        "captured_at": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "outbox_bytes": None,
        "failed_runs_bytes": None,
        "backups_bytes": None,
        "logs_bytes": None,
        "pending_spool_bytes": None,
    }
    try:
        cache.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=cache.parent, prefix="storage-probe-", delete=True):
            pass
    except OSError:
        result["captured_at"] = None
        return result
    for key, paths in roots.items():
        if paths:
            with suppress(OSError):
                result[key] = sum(_directory_bytes(path) for path in paths)
    temp = cache.with_name(cache.name + f".{token_hex(4)}.tmp")
    try:
        cache.parent.mkdir(parents=True, exist_ok=True)
        temp.write_text(json.dumps({**result, "paths_key": paths_key}), encoding="utf-8")
        os.replace(temp, cache)
    except OSError:
        pass
    finally:
        with suppress(OSError):
            temp.unlink(missing_ok=True)
    return result


def build_payload(config: AppConfig, config_path: Path, now: datetime | None = None) -> dict:
    assert config.monitoring is not None
    now = now or datetime.now(UTC)
    errors: list[str] = []
    try:
        data, metadata_errors = metadata(config, now)
        errors.extend(metadata_errors)
    except Exception:
        data = {"cycle": None, "current_run": None, "last_run": None, "next_action_at": None}
        errors.append("SOURCE_METADATA_UNAVAILABLE")
    try:
        host, system = host_system(config)
    except Exception:
        host = {"hostname": None, "os": None, "uptime_seconds": None, "boot_time": None}
        system = {
            "cpu_percent": None,
            "memory_total_bytes": None,
            "memory_available_bytes": None,
            "memory_used_percent": None,
        }
        errors.append("SYSTEM_UNAVAILABLE")
    try:
        worker_sample = worker(config_path, config.monitoring.worker_task_name)
    except Exception:
        worker_sample = {"status": "UNKNOWN", "pid": None, "task_status": "UNKNOWN"}
        errors.append("WORKER_QUERY_UNAVAILABLE")
    try:
        disk_samples = filesystems(config, config_path)
    except Exception:
        disk_samples = []
        errors.append("FILESYSTEM_UNAVAILABLE")
    try:
        storage = managed_storage(config, now)
    except Exception:
        storage = {"captured_at": None, **dict.fromkeys(_STORAGE_FIELDS)}
        errors.append("STORAGE_UNAVAILABLE")
    if sys.platform == "win32" and worker_sample["status"] == "UNKNOWN":
        errors.append("WORKER_QUERY_UNAVAILABLE")
    if host.get("uptime_seconds") is None:
        errors.append("UPTIME_UNAVAILABLE")
    if (
        config.monitoring.collect_cpu
        and system.get("cpu_percent") is None
        and sys.platform == "win32"
    ):
        errors.append("CPU_UNAVAILABLE")
    if config.monitoring.collect_memory and system.get("memory_total_bytes") is None:
        errors.append("MEMORY_UNAVAILABLE")
    if any(item["total_bytes"] is None for item in disk_samples):
        errors.append("FILESYSTEM_UNAVAILABLE")
    if any(
        value is None
        for key, value in storage.items()
        if key.endswith("_bytes") and key != "failed_runs_bytes"
    ):
        errors.append("STORAGE_UNAVAILABLE")
    payload = {
        "schema_version": 1,
        "node_id": config.monitoring.node_id,
        "role": "SOURCE",
        "captured_at": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "agent_version": __version__,
        "host": host,
        "worker": worker_sample,
        "cycle": data["cycle"],
        "current_run": data["current_run"],
        "last_run": data["last_run"],
        "next_action_at": data["next_action_at"],
        "filesystems": disk_samples,
        "managed_storage": storage,
        "system": system,
        "connectivity": {"source_mysql": "UNKNOWN", "relay": "UNKNOWN"},
        "collection_errors": _collection_codes(errors),
    }
    return payload


def filename(node_id: str, now: datetime) -> str:
    return (
        f"airgap-monitor-v1--{node_id}--{now.astimezone(UTC):%Y%m%dT%H%M%SZ}--{token_hex(4)}.json"
    )


def _collection_codes(errors: object) -> list[str]:
    if not isinstance(errors, (list, tuple)):
        return ["COLLECTION_ERROR"]
    return sorted(
        {
            code if isinstance(code, str) and code in _COLLECTION_CODES else "COLLECTION_ERROR"
            for code in errors
        }
    )[:16]


def _warn_collection(errors: object) -> None:
    codes = _collection_codes(errors)
    if codes:
        logger.warning("Source telemetry collection incomplete: %s", ", ".join(codes))


def report(config: AppConfig, config_path: Path) -> tuple[bool, str]:
    """Exit-worthy config validation precedes sampling; runtime failure is best effort."""
    assert config.monitoring is not None and config.relay is not None
    token = resolve_relay_token(config.relay)
    now = datetime.now(UTC)
    try:
        payload = build_payload(config, config_path, now)
    except (OSError, ValueError, TypeError):
        _warn_collection(["COLLECTION_ERROR"])
        return False, "UPLOAD_SKIPPED"
    try:
        body = json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (OSError, ValueError, TypeError):
        _warn_collection(
            [*_collection_codes(payload.get("collection_errors", [])), "COLLECTION_ERROR"]
        )
        return False, "UPLOAD_SKIPPED"
    _warn_collection(payload.get("collection_errors", []))
    if len(body) > MAX_PAYLOAD:
        return False, "PAYLOAD_TOO_LARGE"
    name = filename(config.monitoring.node_id, now)
    relay = config.relay.model_copy(
        update={
            "connect_timeout_seconds": config.monitoring.upload_connect_timeout_seconds,
            "read_timeout_seconds": config.monitoring.upload_read_timeout_seconds,
            "max_attempts": config.monitoring.upload_max_attempts,
            "retry_base_seconds": 0.5,
            "retry_max_seconds": 1.0,
        }
    )
    temp_root = config.paths.data_dir / "monitor" / "tmp"
    try:
        temp_root.mkdir(parents=True, exist_ok=True)
        for old in temp_root.glob("airgap-monitor-*"):
            with suppress(OSError):
                if (
                    old.is_dir()
                    and not old.is_symlink()
                    and time.time() - old.stat().st_mtime > 86400
                ):
                    shutil.rmtree(old)
        with tempfile.TemporaryDirectory(prefix="airgap-monitor-", dir=temp_root) as directory:
            path = Path(directory) / name
            part = path.with_suffix(".part")
            part.write_bytes(body)
            os.replace(part, path)
            RelayUploader(relay, token).upload(path, name, hashlib.sha256(body).hexdigest())
        return True, "RELAY_ACCEPTED"
    except (OSError, UploadError) as exc:
        if isinstance(exc, UploadError):
            return False, exc.code if exc.code in _UPLOAD_CODES else "UPLOAD_ERROR"
        return False, "TEMP_FILE_ERROR"
