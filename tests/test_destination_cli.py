from __future__ import annotations

import sys
from dataclasses import dataclass

import pytest

from airgap_sync.cli import main
from airgap_sync.destination.processor import ProcessResult
from test_destination_config import destination_config


@dataclass(frozen=True)
class CliResult:
    exit_code: int
    output: str


@pytest.fixture
def run_cli(monkeypatch, capsys):
    def invoke(argv):
        monkeypatch.setattr(sys, "argv", ["airgap-sync", *argv])
        exit_code = 0
        try:
            main()
        except SystemExit as exc:
            exit_code = exc.code if isinstance(exc.code, int) else 1
        captured = capsys.readouterr()
        return CliResult(exit_code, captured.out + captured.err)

    return invoke


class FakeConnection:
    def __init__(self, *args):
        self.initialized = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def ping(self):
        return "8.0-test"

    def initialize_metadata(self):
        self.initialized = True

    def metadata_schema_version(self):
        return 1


def _config(tmp_path, password_env):
    data = destination_config(tmp_path)
    data["mysql"]["password_env"] = password_env
    return data


def test_destination_check_cli(tmp_path, write_config, password_env, run_cli, monkeypatch):
    import airgap_sync.cli as cli_module

    path = write_config(_config(tmp_path, password_env))
    monkeypatch.setattr(cli_module, "DestinationMySQLConnection", FakeConnection)

    result = run_cli(["destination", "check", "--config", str(path)])

    assert result.exit_code == 0, result.output
    assert "Incoming            OK" in result.output
    assert "Metadata schema     OK (airgap_sync_meta, v1)" in result.output


def test_destination_process_cli_prints_staged(
    tmp_path, write_config, password_env, run_cli, monkeypatch
):
    import airgap_sync.cli as cli_module

    path = write_config(_config(tmp_path, password_env))
    monkeypatch.setattr(cli_module, "DestinationMySQLConnection", FakeConnection)

    class FakeProcessor:
        def __init__(self, connection, config):
            pass

        def process(self, run_id):
            return ProcessResult(run_id, "STAGED", "new_table", "__airgap_stg_x", 2, 1)

    monkeypatch.setattr(cli_module, "DestinationProcessor", FakeProcessor)
    result = run_cli(["destination", "process", "--config", str(path), "--run", "test-run"])

    assert result.exit_code == 0, result.output
    assert "Table           new_table" in result.output
    assert "Status          STAGED" in result.output


def test_process_once_incomplete_does_not_fail_process(
    tmp_path, write_config, password_env, run_cli, monkeypatch
):
    import airgap_sync.cli as cli_module

    path = write_config(_config(tmp_path, password_env))
    monkeypatch.setattr(cli_module, "DestinationMySQLConnection", FakeConnection)
    monkeypatch.setattr(
        cli_module,
        "process_once",
        lambda connection, config: [
            ProcessResult("run-incomplete", "INCOMPLETE", error="RUN_INCOMPLETE: still copying"),
            ProcessResult("run-ready", "STAGED", "new_table", "stg", 2, 1),
        ],
    )

    result = run_cli(["destination", "process-once", "--config", str(path)])

    assert result.exit_code == 0, result.output
    assert "run-incomplete    INCOMPLETE" in result.output
    assert "run-ready    STAGED" in result.output
