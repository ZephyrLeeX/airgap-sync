"""Source M2 reporter contract and isolation tests."""

from __future__ import annotations

import ctypes
import hashlib
import json
import sqlite3
import stat
import subprocess
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from airgap_sync.cli import cli
from airgap_sync.common.models import AppConfig
from airgap_sync.common.runtime import ProcessLock
from airgap_sync.source import monitor_report as monitor
from airgap_sync.source.state import SourceState, state_db_path
from airgap_sync.source.uploader import RelayUploader, UploadError


@pytest.fixture
def config(config_data):
    config_data["relay"] = {"base_url": "https://relay.example", "token_env": "TEST_RELAY_TOKEN"}
    config_data["monitoring"] = {"node_id": "source-01"}
    return AppConfig.model_validate(config_data)


def test_cli_one_shot_and_role(config_data, write_config, monkeypatch):
    config_data["relay"] = {"base_url": "https://relay.example", "token_env": "TEST_RELAY_TOKEN"}
    config_data["monitoring"] = {"node_id": "source-01"}
    path = write_config(config_data)
    calls = []
    monkeypatch.setattr(
        monitor, "report", lambda config, path: calls.append(path) or (False, "NETWORK")
    )
    result = CliRunner().invoke(cli, ["source", "monitor-report", "--config", str(path)])
    assert result.exit_code == 0
    assert calls == [path]
    config_data["role"] = "destination"
    result = CliRunner().invoke(
        cli, ["source", "monitor-report", "--config", str(write_config(config_data))]
    )
    assert result.exit_code != 0


def test_metadata_read_only_missing_and_schema(config, tmp_path):
    path = state_db_path(config.paths.data_dir)
    now = datetime.now(UTC)
    assert monitor.metadata(config, now)[1] == ["SOURCE_METADATA_MISSING"]
    assert not path.exists()
    with SourceState(path) as state:
        state.initialize()
        state.create_cycle("cycle-test", ["t_snapshot"], now.isoformat())
    before = path.stat().st_mtime_ns
    sample, errors = monitor.metadata(config, now)
    assert not errors
    assert sample["cycle"]["cycle_id"] == "cycle-test"
    assert sample["cycle"]["tables_total"] == 1
    assert path.stat().st_mtime_ns == before
    assert "COUNT(*)" not in Path(monitor.__file__).read_text()
    with sqlite3.connect(path) as db:
        db.execute("UPDATE schema_version SET version=999")
    assert monitor.metadata(config, now)[1] == ["SOURCE_METADATA_SCHEMA"]


def test_payload_protocol_and_unknown(config, tmp_path, monkeypatch):
    monkeypatch.setattr(monitor, "host_system", lambda config: ({"hostname": "host"}, {}))
    monkeypatch.setattr(monitor, "filesystems", lambda config, path: [])
    monkeypatch.setattr(monitor, "managed_storage", lambda config, now: {})
    now = datetime(2026, 9, 27, 1, 2, 3, tzinfo=UTC)
    payload = monitor.build_payload(config, tmp_path / "source.yaml", now)
    assert payload["schema_version"] == 1
    assert payload["captured_at"] == "2026-09-27T01:02:03Z"
    assert payload["worker"]["status"] == "UNKNOWN"
    assert payload["connectivity"]["source_mysql"] == "UNKNOWN"
    assert payload["current_run"] is None
    assert monitor.filename("source-01", now).startswith(
        "airgap-monitor-v1--source-01--20260927T010203Z--"
    )
    with pytest.raises(ValueError):
        AppConfig.model_validate({**config.model_dump(), "monitoring": {"node_id": "../bad"}})


def test_managed_storage_cache_and_failure(config, monkeypatch):
    now = datetime.now(UTC)
    outbox = config.paths.data_dir / "outbox"
    outbox.mkdir(parents=True)
    (outbox / "a").write_bytes(b"hello")
    first = monitor.managed_storage(config, now)
    assert first["outbox_bytes"] == 5
    (outbox / "a").write_bytes(b"longer text")
    assert monitor.managed_storage(config, now + timedelta(minutes=1))["outbox_bytes"] == 5
    assert monitor.managed_storage(config, now + timedelta(hours=2))["outbox_bytes"] == 11
    monkeypatch.setattr(
        monitor, "_directory_bytes", lambda path, **kwargs: (_ for _ in ()).throw(OSError())
    )
    assert monitor.managed_storage(config, now + timedelta(hours=4))["outbox_bytes"] is None


def test_managed_storage_rejects_injected_cache(config):
    now = datetime.now(UTC)
    cache = config.paths.data_dir / "monitor" / "storage-cache.json"
    cache.parent.mkdir(parents=True)
    cache.write_text(json.dumps({"captured_at": now.isoformat(), "token": "secret"}))
    sample = monitor.managed_storage(config, now)
    assert "token" not in sample
    assert sample["outbox_bytes"] == 0
    sample["failed_runs_bytes"] = 123
    cache.write_text(json.dumps(sample))
    assert monitor.managed_storage(config, now)["failed_runs_bytes"] is None


def test_managed_storage_cache_unwritable_skips_scan(config, monkeypatch):
    monkeypatch.setattr(
        monitor.tempfile,
        "NamedTemporaryFile",
        lambda **kwargs: (_ for _ in ()).throw(OSError()),
    )
    monkeypatch.setattr(
        monitor, "_directory_bytes", lambda path, **kwargs: pytest.fail("scan must be skipped")
    )
    sample = monitor.managed_storage(config, datetime.now(UTC))
    assert sample["captured_at"] is None
    assert sample["outbox_bytes"] is None


def test_collector_failure_keeps_other_sections(config, tmp_path, monkeypatch):
    monkeypatch.setattr(monitor, "host_system", lambda config: (_ for _ in ()).throw(OSError()))
    monkeypatch.setattr(monitor, "filesystems", lambda config, path: [])
    monkeypatch.setattr(monitor, "managed_storage", lambda config, now: {})
    payload = monitor.build_payload(config, tmp_path / "source.yaml")
    assert payload["host"]["hostname"] is None
    assert payload["worker"]["status"] == "UNKNOWN"
    assert "SYSTEM_UNAVAILABLE" in payload["collection_errors"]


@pytest.mark.parametrize("reason", ["HTTP 429", "ConnectTimeout", "ConnectionError"])
def test_upload_failure_best_effort(config, tmp_path, monkeypatch, reason):
    monkeypatch.setenv("TEST_RELAY_TOKEN", "secret")
    monkeypatch.setattr(monitor, "build_payload", lambda config, path, now: {"schema_version": 1})

    def fail(*args):
        raise UploadError("UPLOAD_RETRY_EXHAUSTED", reason, attempts=2)

    monkeypatch.setattr(monitor.RelayUploader, "upload", fail)
    ok, code = monitor.report(config, tmp_path / "source.yaml")
    assert not ok and code == "UPLOAD_RETRY_EXHAUSTED"
    assert not list((config.paths.data_dir / "monitor" / "tmp").iterdir())


def test_upload_success_and_size_limit(config, tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_RELAY_TOKEN", "secret")
    monkeypatch.setattr(monitor, "build_payload", lambda config, path, now: {"schema_version": 1})
    names = []

    def uploaded(self, path, name, sha):
        names.append(name)
        assert json.loads(path.read_text()) == {
            "schema_version": 4,
            "source_database": config.mysql.database,
            "run_facts_status": "UNAVAILABLE",
            "run_facts": [],
            "run_progress": [],
        }
        assert len(sha) == 64

    monkeypatch.setattr(monitor.RelayUploader, "upload", uploaded)
    assert monitor.report(config, tmp_path / "source.yaml")[0]
    assert len(names) == 1
    monkeypatch.setattr(
        monitor, "build_payload", lambda config, path, now: {"x": "a" * monitor.MAX_PAYLOAD}
    )
    assert monitor.report(config, tmp_path / "source.yaml") == (False, "PAYLOAD_TOO_LARGE")
    assert len(names) == 1


def test_worker_windows_process_matching(tmp_path, monkeypatch):
    monkeypatch.setattr(monitor.sys, "platform", "win32")
    path = Path(r"C:\Program Data\Airgap\source.yaml")
    target = str(path)
    entries = [
        {"ProcessId": 12, "CommandLine": f'python.exe source monitor-report --config "{target}"'},
        {"ProcessId": 13, "CommandLine": f'python.exe source worker --config "{target}2"'},
        {"ProcessId": 14, "CommandLine": f'airgap-sync.exe source worker --config "{target}"'},
    ]
    monkeypatch.setattr(
        monitor,
        "_powershell",
        lambda script: "Running" if "Get-ScheduledTask" in script else json.dumps(entries),
    )
    assert monitor.worker(path, "Airgap Sync Source Worker") == {
        "status": "RUNNING",
        "pid": 14,
        "task_status": "Running",
    }
    entries.pop()
    assert monitor.worker(path, "Airgap Sync Source Worker")["status"] == "STOPPED"
    monkeypatch.setattr(monitor, "_powershell", lambda script: (_ for _ in ()).throw(OSError()))
    assert monitor.worker(path, "Airgap Sync Source Worker")["status"] == "UNKNOWN"


def test_metadata_busy_degrades(config, monkeypatch):
    path = state_db_path(config.paths.data_dir)
    with SourceState(path) as state:
        state.initialize()
    monkeypatch.setattr(
        monitor.sqlite3,
        "connect",
        lambda *a, **kw: (_ for _ in ()).throw(sqlite3.OperationalError("secret")),
    )
    data, errors = monitor.metadata(config, datetime.now(UTC))
    assert data["cycle"] is None
    assert errors == ["SOURCE_METADATA_UNAVAILABLE"]


def test_reporter_never_initializes_or_locks_business_state(config, monkeypatch):
    path = state_db_path(config.paths.data_dir)
    with SourceState(path) as state:
        state.initialize()
    monkeypatch.setattr(SourceState, "initialize", lambda self: pytest.fail("unexpected migration"))
    monkeypatch.setattr(
        ProcessLock, "__enter__", lambda self: pytest.fail("unexpected business lock")
    )
    assert monitor.metadata(config, datetime.now(UTC))[1] == []


def test_filesystems_deduplicate_and_degrade(config, tmp_path, monkeypatch):
    entries = monitor.filesystems(config, tmp_path / "source.yaml")
    labels = [label for item in entries for label in item["locations"]]
    assert set(labels) == {"system", "install", "config", "data", "temp"}
    assert len({item["mount"] for item in entries}) == len(entries)
    real_usage = monitor.shutil.disk_usage
    monkeypatch.setattr(monitor.shutil, "disk_usage", lambda path: (_ for _ in ()).throw(OSError()))
    assert all(
        item["free_bytes"] is None for item in monitor.filesystems(config, tmp_path / "source.yaml")
    )
    monkeypatch.setattr(monitor.shutil, "disk_usage", real_usage)


def test_windows_filesystems_use_volume_identity(config, tmp_path, monkeypatch):
    class Kernel:
        def GetVolumePathNameW(self, path, buffer, length):
            buffer.value = "C:\\"
            return 1

        def GetVolumeNameForVolumeMountPointW(self, mount, buffer, length):
            buffer.value = "volume-guid"
            return 1

    monkeypatch.setattr(ctypes, "WinDLL", lambda *args, **kwargs: Kernel(), raising=False)
    monkeypatch.setattr(monitor.sys, "platform", "win32")
    volumes = monitor.filesystems(config, tmp_path / "source.yaml")
    assert len(volumes) == 1
    assert volumes[0]["mount"] == "C:\\"
    assert set(volumes[0]["locations"]) == {"system", "install", "config", "data", "temp"}


def test_offline_task_example_is_independent():
    root = Path(__file__).resolve().parents[1]
    service = root / "scripts" / "offline" / "service-examples"
    installer = (service / "install-source-monitor-task.ps1").read_text()
    wrapper = (service / "run-source-monitor.ps1").read_text()
    assert "PT5M" in installer and "IgnoreNew" in installer
    assert "<ExecutionTimeLimit>PT2M</ExecutionTimeLimit>" in installer
    assert "[string] $LogFile = ''" in installer
    assert "if ($LogFile)" in installer and ' -LogFile "' in installer
    assert "monitoring.log_dirs" in wrapper
    assert "Airgap Sync Source Monitor" in installer
    assert "source monitor-report --config $Config" in wrapper
    assert "source worker" not in wrapper
    builder = (root / "scripts" / "offline" / "build_release.py").read_text()
    assert "SERVICE_EXAMPLES_DIR.iterdir()" in builder
    assert '"MONITORING-M2.md"' in builder


def test_telemetry_uses_existing_relay_put_contract(config, tmp_path):
    now = datetime(2026, 9, 27, tzinfo=UTC)
    name = monitor.filename(config.monitoring.node_id, now)
    path = tmp_path / name
    path.write_bytes(b'{"schema_version":1}')
    digest = hashlib.sha256(path.read_bytes()).hexdigest()

    class Response:
        status_code = 201

        def json(self):
            return {
                "success": True,
                "filename": name,
                "size": path.stat().st_size,
                "sha256": digest,
                "request_id": "request-1",
            }

    class Session:
        def put(self, url, *, data, headers, timeout, verify):
            assert url.endswith("/api/v1/upload/" + name)
            assert data.read() == path.read_bytes()
            assert headers["Content-Type"] == "application/octet-stream"
            assert headers["X-File-SHA256"] == digest
            assert headers["Authorization"] == "Bearer secret"
            assert timeout == (10, 600)
            return Response()

    confirmation = RelayUploader(config.relay, "secret", session=Session()).upload(
        path, name, digest
    )
    assert confirmation.filename == name


@pytest.fixture
def windows_processes(monkeypatch):
    entries = []
    monkeypatch.setattr(monitor.sys, "platform", "win32")
    monkeypatch.setattr(
        monitor,
        "_powershell",
        lambda script: "Running" if "Get-ScheduledTask" in script else json.dumps(entries),
    )
    return entries


@pytest.mark.parametrize(
    "option,status",
    [
        (r'--config "c:/Program Data/Airgap/app/.././source.yaml"', "RUNNING"),
        (r'--config="C:\Program Data\Airgap\source.yaml"', "RUNNING"),
        (r'"--config=C:\Program Data\Airgap\source.yaml"', "RUNNING"),
        (r'--config "C:\Program Data\Airgap\other.yaml"', "STOPPED"),
        (r"--config ..\config\source.yaml", "UNKNOWN"),
        (r"--config C:source.yaml", "UNKNOWN"),
        (r"--config \config\source.yaml", "UNKNOWN"),
        ("--config", "UNKNOWN"),
        ("", "UNKNOWN"),
        ('--config "unclosed', "UNKNOWN"),
        (r"--config C:\one.yaml --config C:\two.yaml", "UNKNOWN"),
    ],
)
def test_windows_config_identity(windows_processes, option, status):
    windows_processes.append(
        {
            "ProcessId": 42,
            "CommandLine": rf'"D:\Custom Install\airgap-sync.exe" source worker {option}',
        }
    )
    result = monitor.worker(Path(r"C:\Program Data\Airgap\source.yaml"), "worker")
    assert result["status"] == status
    assert result["pid"] == (42 if status == "RUNNING" else None)


@pytest.mark.parametrize("command", [None, "", "   "])
def test_windows_unreadable_candidate_is_unknown(windows_processes, command):
    windows_processes.append({"ProcessId": 42, "CommandLine": command})
    assert monitor.worker(Path(r"C:\source.yaml"), "worker")["status"] == "UNKNOWN"
    windows_processes.append(
        {
            "ProcessId": 43,
            "CommandLine": r"airgap-sync.exe source worker --config C:\source.yaml",
        }
    )
    assert monitor.worker(Path(r"C:\source.yaml"), "worker")["status"] == "UNKNOWN"


def test_windows_excludes_self_reporter_and_unrelated(windows_processes):
    windows_processes.extend(
        [
            {"ProcessId": monitor.os.getpid(), "CommandLine": None},
            {
                "ProcessId": 42,
                "CommandLine": r"airgap-sync.exe source monitor-report --config C:\source.yaml",
            },
            {
                "ProcessId": 43,
                "CommandLine": r"python.exe other.py source worker --config C:\source.yaml",
            },
            {
                "ProcessId": 44,
                "CommandLine": r'python.exe -c "source worker --config C:\source.yaml"',
            },
        ]
    )
    assert monitor.worker(Path(r"C:\source.yaml"), "worker")["status"] == "STOPPED"


def test_windows_launcher_and_python_are_one_worker(windows_processes):
    windows_processes.extend(
        [
            {
                "ProcessId": 42,
                "ParentProcessId": 10,
                "CommandLine": r'"D:\App\airgap-sync.exe" source worker --config C:\source.yaml',
            },
            {
                "ProcessId": 43,
                "ParentProcessId": 42,
                "CommandLine": (
                    r'"D:\App\python.exe" "D:\App\airgap-sync.exe" '
                    r"source worker --config=C:\source.yaml"
                ),
            },
        ]
    )
    assert monitor.worker(Path(r"C:\source.yaml"), "worker")["pid"] == 43
    windows_processes[1]["ParentProcessId"] = 99
    assert monitor.worker(Path(r"C:\source.yaml"), "worker")["status"] == "UNKNOWN"
    windows_processes.pop(0)
    assert monitor.worker(Path(r"C:\source.yaml"), "worker")["status"] == "RUNNING"


def test_windows_module_and_unc_path(windows_processes):
    windows_processes.append(
        {
            "ProcessId": 42,
            "CommandLine": (
                r"python.exe -m airgap_sync.cli source worker "
                r'--config "\\SERVER\share\dir\..\source.yaml"'
            ),
        }
    )
    assert monitor.worker(Path(r"\\server\share\source.yaml"), "worker")["pid"] == 42


@pytest.mark.parametrize(
    "args",
    [
        [
            r"C:\Program Files\Airgap\airgap-sync.exe",
            "source",
            "worker",
            "--config",
            r"C:\Config Dir\source.yaml",
        ],
        ["python.exe", "", "trailing space " + chr(92), 'embedded"quote', r"C:\normal\path"],
    ],
)
def test_windows_argv_roundtrip(args):
    assert monitor._windows_argv(subprocess.list2cmdline(args)) == args


class FakeEntry:
    def __init__(self, path, mode=stat.S_IFREG, size=7, attributes=0):
        self.path = str(path)
        self.info = SimpleNamespace(st_mode=mode, st_size=size, st_file_attributes=attributes)
        self.stat_calls = 0

    def stat(self, *, follow_symlinks):
        assert follow_symlinks is False
        self.stat_calls += 1
        return self.info


@contextmanager
def fake_entries(entries):
    yield iter(entries)


def test_directory_single_directory_entry_budget(tmp_path, monkeypatch):
    entries = [FakeEntry(tmp_path / str(i)) for i in range(5)]
    monkeypatch.setattr(monitor, "_STORAGE_MAX_ENTRIES", 3)
    monkeypatch.setattr(monitor.time, "monotonic", lambda: 0)
    monkeypatch.setattr(monitor.os, "scandir", lambda path: fake_entries(entries))
    with pytest.raises(OSError, match="budget"):
        monitor._directory_bytes(tmp_path)
    assert [entry.stat_calls for entry in entries] == [1, 1, 1, 0, 0]
    monkeypatch.setattr(monitor.os, "scandir", lambda path: fake_entries(entries[:3]))
    assert monitor._directory_bytes(tmp_path) == 21


@pytest.mark.parametrize("phase", ["enumerate", "stat", "exhaust"])
def test_directory_time_budget_in_single_directory(tmp_path, monkeypatch, phase):
    clock = [0.0]
    entry = FakeEntry(tmp_path / "file")
    real_stat = entry.stat

    def delayed_stat(**kwargs):
        if phase == "stat":
            clock[0] = 15.0
        return real_stat(**kwargs)

    def entries():
        if phase == "enumerate":
            clock[0] = 15.0
        yield entry
        if phase == "exhaust":
            clock[0] = 15.0

    entry.stat = delayed_stat
    monkeypatch.setattr(monitor.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(monitor.os, "scandir", lambda path: fake_entries(entries()))
    with pytest.raises(OSError, match="budget"):
        monitor._directory_bytes(tmp_path)
    assert entry.stat_calls == (0 if phase == "enumerate" else 1)


def test_directory_nested_budget_and_links(tmp_path, monkeypatch):
    nested = tmp_path / "nested"
    nested.mkdir()
    root_entries = [
        FakeEntry(nested, stat.S_IFDIR),
        FakeEntry(tmp_path / "symlink", stat.S_IFLNK),
        FakeEntry(
            tmp_path / "junction", stat.S_IFDIR, attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT
        ),
        FakeEntry(tmp_path / "file-link", attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT),
    ]
    opened = []

    def scan(path):
        opened.append(path)
        assert path in {tmp_path, nested}
        return fake_entries(root_entries if path == tmp_path else [FakeEntry(nested / "file")])

    monkeypatch.setattr(monitor.os, "scandir", scan)
    monkeypatch.setattr(monitor.time, "monotonic", lambda: 0)
    assert monitor._directory_bytes(tmp_path) == 7
    assert opened == [tmp_path, nested]
    monkeypatch.setattr(monitor, "_STORAGE_MAX_ENTRIES", 4)
    with pytest.raises(OSError, match="budget"):
        monitor._directory_bytes(tmp_path)


@pytest.mark.parametrize("root_kind", ["symlink", "dangling", "junction", "ancestor"])
def test_directory_rejects_root_links(tmp_path, monkeypatch, root_kind):
    root = tmp_path / "logs"
    if root_kind == "junction":
        original = Path.lstat
        monkeypatch.setattr(
            Path,
            "lstat",
            lambda path: (
                SimpleNamespace(
                    st_mode=stat.S_IFDIR, st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT
                )
                if path == root
                else original(path)
            ),
        )
    else:
        target = tmp_path / "target"
        if root_kind != "dangling":
            target.mkdir()
        root.symlink_to(target, target_is_directory=True)
        if root_kind == "ancestor":
            root = root / "missing"
    monkeypatch.setattr(monitor.os, "scandir", lambda path: pytest.fail("must not follow link"))
    with pytest.raises(OSError, match="link"):
        monitor._directory_bytes(root)


@pytest.mark.parametrize("failure", ["budget", "permission"])
def test_storage_failed_category_cached_without_partial_total(config, monkeypatch, failure):
    now = datetime.now(UTC)
    outbox = config.paths.data_dir / "outbox"
    outbox.mkdir(parents=True)
    monkeypatch.setattr(monitor, "_STORAGE_MAX_ENTRIES", 1)
    calls = []

    def scan(path):
        calls.append(path)
        if failure == "permission":
            raise PermissionError("secret path")
        return fake_entries([FakeEntry(path / "a"), FakeEntry(path / "b")])

    monkeypatch.setattr(monitor.os, "scandir", scan)
    first = monitor.managed_storage(config, now)
    assert first["outbox_bytes"] is None
    assert first["backups_bytes"] == 0
    assert first["captured_at"] is not None
    assert monitor.managed_storage(config, now + timedelta(minutes=5)) == first
    assert calls == [outbox]
    monitor.managed_storage(config, now + timedelta(hours=1))
    assert calls == [outbox, outbox]


def test_logs_explicit_paths_capacity_and_cache_invalidation(config, tmp_path):
    now = datetime.now(UTC)
    install_logs = tmp_path / "custom-install" / "logs"
    custom_logs = tmp_path / "separate-project-logs"
    install_logs.mkdir(parents=True)
    custom_logs.mkdir()
    (install_logs / "worker.log").write_bytes(b"worker")
    (custom_logs / "monitor.log").write_bytes(b"monitor")
    # data_dir is intentionally unrelated to either installation or log directory.
    assert monitor.managed_storage(config, now)["logs_bytes"] is None
    config.monitoring.log_dirs = [install_logs]
    assert monitor.managed_storage(config, now)["logs_bytes"] == 6
    config.monitoring.log_dirs = [custom_logs]
    assert monitor.managed_storage(config, now)["logs_bytes"] == 7
    config.monitoring.log_dirs = [install_logs, custom_logs]
    sample = monitor.managed_storage(config, now)
    assert sample["logs_bytes"] == 13
    assert "paths_key" not in sample
    config.monitoring.log_dirs = [tmp_path / "explicit-missing"]
    assert monitor.managed_storage(config, now)["logs_bytes"] == 0
    config.monitoring.log_dirs = []
    assert monitor.managed_storage(config, now)["logs_bytes"] is None


def test_logs_failure_invalidates_whole_category(config, tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "a").write_bytes(b"hello")
    link = tmp_path / "link"
    link.symlink_to(logs, target_is_directory=True)
    config.monitoring.log_dirs = [logs, link]
    assert monitor.managed_storage(config, datetime.now(UTC))["logs_bytes"] is None


@pytest.mark.parametrize("phase", ["enumerate", "stat", "exhaust"])
def test_storage_shared_budget_cached_and_heartbeat_uploaded(
    config, tmp_path, monkeypatch, caplog, phase
):
    clock = [0.0]
    config.monitoring.log_dirs = [tmp_path / f"logs-{i}" for i in range(4)]
    roots = [
        config.paths.data_dir / "outbox",
        config.paths.data_dir / "backups",
        *config.monitoring.log_dirs,
        config.paths.data_dir / "spool",
    ]
    for root in roots:
        root.mkdir(parents=True)
    opened = []
    real_scan = monitor.os.scandir
    started = []
    real_directory_bytes = monitor._directory_bytes

    def directory_bytes(path, **kwargs):
        started.append(path)
        return real_directory_bytes(path, **kwargs)

    def scan(path):
        # TemporaryDirectory cleanup also uses scandir (with an fd on POSIX).
        if path not in roots:
            return real_scan(path)
        opened.append(path)
        entry = FakeEntry(path / "file")
        original_stat = entry.stat

        def delayed_stat(**kwargs):
            if phase == "stat":
                clock[0] += 10
            return original_stat(**kwargs)

        def entries():
            if phase == "enumerate":
                clock[0] += 10
            yield entry
            if phase == "exhaust":
                clock[0] += 10

        entry.stat = delayed_stat
        return fake_entries(entries())

    monkeypatch.setattr(monitor.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(monitor.os, "scandir", scan)
    monkeypatch.setattr(monitor, "_directory_bytes", directory_bytes)
    monkeypatch.setenv("TEST_RELAY_TOKEN", "SECRET")
    uploaded = []
    monkeypatch.setattr(
        monitor.RelayUploader,
        "upload",
        lambda self, path, *args: uploaded.append(json.loads(path.read_text())),
    )
    # Every tree takes only 10 seconds (<15), but the fifth crosses 45 total.
    # Two logs completed; the category must still be null. The last log and
    # spool must never start. Use build_payload/report's real cache/upload path.
    with SourceState(state_db_path(config.paths.data_dir)) as state:
        state.initialize()
    assert monitor.report(config, tmp_path / "source.yaml") == (True, "RELAY_ACCEPTED")
    first = uploaded[0]["managed_storage"]
    assert first["outbox_bytes"] == first["backups_bytes"] == 7
    assert first["logs_bytes"] is None
    assert first["pending_spool_bytes"] is None
    assert opened == roots[:5]
    assert started == roots[:5]
    assert clock[0] == 50
    assert uploaded[0]["collection_errors"] == ["STORAGE_UNAVAILABLE"]
    assert "SECRET" not in caplog.text
    cache = config.paths.data_dir / "monitor" / "storage-cache.json"
    assert json.loads(cache.read_text())["logs_bytes"] is None
    captured = datetime.fromisoformat(first["captured_at"].replace("Z", "+00:00"))
    assert monitor.managed_storage(config, captured + timedelta(minutes=5)) == first
    assert monitor.report(config, tmp_path / "source.yaml")[0]
    assert uploaded[1]["managed_storage"] == first
    assert uploaded[1]["collection_errors"] == ["STORAGE_UNAVAILABLE"]
    assert opened == roots[:5]
    assert started == roots[:5]


@pytest.mark.parametrize("failed_category", [None, "logs", "outbox", "backups", "spool"])
def test_storage_unconfigured_logs_are_not_failure(config, tmp_path, monkeypatch, failed_category):
    if failed_category:
        root = config.paths.data_dir / failed_category
        root.mkdir(parents=True)
        if failed_category == "logs":
            config.monitoring.log_dirs = [root]

        def denied(path):
            assert path == root
            raise PermissionError("SECRET path")

        monkeypatch.setattr(monitor.os, "scandir", denied)
    now = datetime.now(UTC)
    for captured in (now, now + timedelta(minutes=5)):
        payload = monitor.build_payload(config, tmp_path / "source.yaml", captured)
        assert payload["managed_storage"]["logs_bytes"] is None
        assert ("STORAGE_UNAVAILABLE" in payload["collection_errors"]) == bool(failed_category)
        assert "SECRET" not in json.dumps(payload)


@pytest.mark.parametrize("paths", [["relative"], ["/"], ["/logs", "/logs"], ["/logs", "/logs/sub"]])
def test_log_dirs_configuration_rejects_ambiguous_paths(config, paths):
    raw = config.model_dump()
    raw["monitoring"]["log_dirs"] = paths
    with pytest.raises(ValueError, match="log_dirs"):
        AppConfig.model_validate(raw)


@pytest.mark.parametrize("collection", ["missing", "item", "complete"])
@pytest.mark.parametrize("upload_ok", [True, False])
def test_cli_collection_and_upload_outcomes(
    config,
    write_config,
    monkeypatch,
    caplog,
    collection,
    upload_ok,
):
    path = write_config(config.model_dump(mode="json"))
    monkeypatch.setenv("TEST_RELAY_TOKEN", "TOKEN-SECRET")
    if collection != "missing":
        with SourceState(state_db_path(config.paths.data_dir)) as state:
            state.initialize()
    monkeypatch.setattr(
        monitor,
        "host_system",
        lambda config: (
            {"uptime_seconds": 1},
            {"memory_total_bytes": 10},
        ),
    )
    monkeypatch.setattr(monitor, "filesystems", lambda config, path: [])
    monkeypatch.setattr(monitor, "managed_storage", lambda config, now: {})
    if collection == "item":
        monkeypatch.setattr(
            monitor,
            "managed_storage",
            lambda *args: (_ for _ in ()).throw(OSError("PASSWORD-SECRET")),
        )
    uploaded = []

    def upload(self, path, *args):
        uploaded.append(json.loads(path.read_text()))
        if not upload_ok:
            raise UploadError("UPLOAD_RETRY_EXHAUSTED", "TOKEN-SECRET PASSWORD-SECRET", attempts=2)

    monkeypatch.setattr(monitor.RelayUploader, "upload", upload)
    result = CliRunner().invoke(cli, ["source", "monitor-report", "--config", str(path)])
    assert result.exit_code == 0
    assert len(uploaded) == 1
    messages = result.output.splitlines()
    warnings = [message for message in messages if "collection incomplete" in message]
    assert len(warnings) == (0 if collection == "complete" else 1)
    if collection != "complete":
        code = "SOURCE_METADATA_MISSING" if collection == "missing" else "STORAGE_UNAVAILABLE"
        assert warnings[0].count(code) == 1
        assert uploaded[0]["collection_errors"].count(code) == 1
    assert ("accepted by Relay" in result.output) == upload_ok
    assert any("not accepted: UPLOAD_RETRY_EXHAUSTED" in msg for msg in messages) != upload_ok
    for secret in ("TOKEN-SECRET", "PASSWORD-SECRET", "schema_version"):
        assert secret not in caplog.text + result.output


def test_warning_codes_allowlisted_bounded_and_deduplicated(config, tmp_path, monkeypatch, caplog):
    monkeypatch.setenv("TEST_RELAY_TOKEN", "SECRET")
    errors = ["SOURCE_METADATA_MISSING"] * 100 + ["password=SECRET", {"token": "SECRET"}]
    monkeypatch.setattr(monitor, "build_payload", lambda *args: {"collection_errors": errors})
    monkeypatch.setattr(monitor.RelayUploader, "upload", lambda *args: None)
    assert monitor.report(config, tmp_path / "source.yaml")[0]
    messages = [record.getMessage() for record in caplog.records]
    assert len(messages) == 1 and len(messages[0]) < 512
    assert messages[0].count("SOURCE_METADATA_MISSING") == 1
    assert messages[0].count("COLLECTION_ERROR") == 1
    assert "SECRET" not in caplog.text


def test_unknown_upload_error_code_is_not_logged(config, tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_RELAY_TOKEN", "SECRET")
    monkeypatch.setattr(monitor, "build_payload", lambda *args: {})
    monkeypatch.setattr(
        monitor.RelayUploader,
        "upload",
        lambda *args: (_ for _ in ()).throw(
            UploadError("token=SECRET", "password=SECRET", attempts=1)
        ),
    )
    assert monitor.report(config, tmp_path / "source.yaml") == (False, "UPLOAD_ERROR")


def test_windows_venv_redirector_chain_and_distinct_entrypoints(windows_processes):
    command = r'"D:\App\airgap-sync.exe" source worker --config C:\source.yaml'
    windows_processes.extend(
        [
            {"ProcessId": 42, "ParentProcessId": 10, "CommandLine": command},
            {
                "ProcessId": 43,
                "ParentProcessId": 42,
                "CommandLine": r'"D:\App\python.exe" ' + command,
            },
            {
                "ProcessId": 44,
                "ParentProcessId": 43,
                "CommandLine": r'"C:\Python312\python.exe" ' + command,
            },
        ]
    )
    result = monitor.worker(Path(r"C:\source.yaml"), "worker")
    assert result["status"] == "RUNNING" and result["pid"] == 44
    windows_processes[2]["CommandLine"] = windows_processes[2]["CommandLine"].replace(
        r"D:\App\airgap-sync.exe", r"D:\Other\airgap-sync.exe"
    )
    assert monitor.worker(Path(r"C:\source.yaml"), "worker")["status"] == "UNKNOWN"


@pytest.mark.parametrize("serialization", [False, True])
def test_cli_fatal_collection_reports_once_and_skips_upload(
    config,
    write_config,
    monkeypatch,
    serialization,
):
    monkeypatch.setenv("TEST_RELAY_TOKEN", "SECRET")
    path = write_config(config.model_dump(mode="json"))
    if serialization:
        monkeypatch.setattr(monitor, "build_payload", lambda *args: {"invalid": object()})
    else:
        monkeypatch.setattr(
            monitor, "build_payload", lambda *args: (_ for _ in ()).throw(ValueError("SECRET"))
        )
    monkeypatch.setattr(monitor.RelayUploader, "upload", lambda *args: pytest.fail("no payload"))
    result = CliRunner().invoke(cli, ["source", "monitor-report", "--config", str(path)])
    assert result.exit_code == 0
    assert result.output.count("COLLECTION_ERROR") == 1
    assert "Source telemetry upload skipped" in result.output
    assert "accepted" not in result.output
    assert "SECRET" not in result.output
