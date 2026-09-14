"""CLI 测试 (直接调用 main(), 不需要真实 MySQL)。"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

import airgap_sync
from airgap_sync.cli import main
from test_mysql_checks import FakeExecutor

EXAMPLE_CONFIG = Path(__file__).parent.parent / "config" / "config.example.yaml"


@dataclass(frozen=True)
class CliResult:
    exit_code: int
    output: str


@pytest.fixture
def run_cli(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture):
    """在进程内执行 CLI 入口, 返回退出码和全部输出 (stdout + stderr)。"""

    def _run(argv: list[str]) -> CliResult:
        monkeypatch.setattr(sys, "argv", ["airgap-sync", *argv])
        exit_code = 0
        try:
            main()
        except SystemExit as exc:
            exit_code = exc.code if isinstance(exc.code, int) else 1
        captured = capsys.readouterr()
        return CliResult(exit_code, captured.out + captured.err)

    return _run


class TestVersion:
    def test_version(self, run_cli):
        result = run_cli(["--version"])
        assert result.exit_code == 0
        assert airgap_sync.__version__ in result.output
        assert "airgap-sync" in result.output


class TestConfigValidate:
    def test_valid_config(self, run_cli, config_data, write_config, password_env):
        path = write_config(config_data)
        result = run_cli(["config", "validate", "--config", str(path)])
        assert result.exit_code == 0
        assert "Configuration OK" in result.output
        assert "role=source tables=1 enabled=1" in result.output

    def test_missing_password_env(self, run_cli, config_data, write_config, monkeypatch):
        monkeypatch.delenv("AIRGAP_TEST_PASSWORD", raising=False)
        path = write_config(config_data)
        result = run_cli(["config", "validate", "--config", str(path)])
        assert result.exit_code == 1
        assert "AIRGAP_TEST_PASSWORD" in result.output

    def test_invalid_config(self, run_cli, config_data, write_config, password_env):
        config_data["tables"] = [{"name": "t_bad", "mode": "keyed"}]
        path = write_config(config_data)
        result = run_cli(["config", "validate", "--config", str(path)])
        assert result.exit_code == 1
        assert "requires at least one key column" in result.output

    def test_missing_config_file(self, run_cli, tmp_path):
        result = run_cli(["config", "validate", "--config", str(tmp_path / "no.yaml")])
        assert result.exit_code != 0  # click 用法错误, 退出码 2
        assert "does not exist" in result.output

    def test_example_config_file(self, run_cli, monkeypatch):
        """仓库中的示例配置在设置密码环境变量后应能通过校验。"""
        monkeypatch.setenv("AIRGAP_SYNC_MYSQL_PASSWORD", "example-password")
        result = run_cli(["config", "validate", "--config", str(EXAMPLE_CONFIG)])
        assert result.exit_code == 0, result.output
        assert "role=source tables=3 enabled=2" in result.output


class TestSourceCheck:
    def test_requires_source_role(self, run_cli, config_data, write_config, password_env):
        config_data["role"] = "destination"
        path = write_config(config_data)
        result = run_cli(["source", "check", "--config", str(path)])
        assert result.exit_code == 1
        assert "role=source" in result.output

    def test_missing_password_env(self, run_cli, config_data, write_config, monkeypatch):
        monkeypatch.delenv("AIRGAP_TEST_PASSWORD", raising=False)
        path = write_config(config_data)
        result = run_cli(["source", "check", "--config", str(path)])
        assert result.exit_code == 1
        assert "AIRGAP_TEST_PASSWORD" in result.output


class FakeSourceMySQL(FakeExecutor):
    """替身: 模拟 SourceMySQLConnection 的 with 语句行为和 ping。"""

    def __init__(self, config, tables):
        super().__init__(tables)
        self._config = config

    def __enter__(self):
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def ping(self) -> str:
        return "5.7.35-log"


class TestSourceCheckFullFlow:
    def test_success(self, run_cli, config_data, write_config, password_env, monkeypatch):
        import airgap_sync.cli as cli_module

        config_data["tables"].append({"name": "t_legacy", "mode": "row_multiset", "enabled": False})
        path = write_config(config_data)

        def fake_connect(config):
            return FakeSourceMySQL(config, {"t_keyed": {"id", "name"}})

        monkeypatch.setattr(cli_module, "SourceMySQLConnection", fake_connect)

        result = run_cli(["source", "check", "--config", str(path)])
        assert result.exit_code == 0, result.output
        assert "Configuration       OK" in result.output
        assert "MySQL connection    OK" in result.output
        assert "MySQL server        5.7.35-log" in result.output
        assert "Database            sgaj_data" in result.output
        assert "SQLite state        OK" in result.output
        assert "t_keyed                 OK   mode=keyed key=id" in result.output
        assert "t_legacy                SKIP mode=row_multiset (disabled)" in result.output

        # 状态库已创建, 且通过的 enabled 表被登记
        state_db = path.parent / "data" / "state" / "meta.db"
        assert state_db.exists()
        from airgap_sync.source.state import SourceState

        state = SourceState(state_db)
        try:
            row = state.get_table_state("t_keyed")
            assert row is not None
            assert row.mode == "keyed"
            assert row.status == "IDLE"
            assert state.get_table_state("t_legacy") is None  # disabled 表不登记
        finally:
            state.close()

    def test_table_not_found_fails(
        self, run_cli, config_data, write_config, password_env, monkeypatch
    ):
        import airgap_sync.cli as cli_module

        path = write_config(config_data)
        monkeypatch.setattr(
            cli_module, "SourceMySQLConnection", lambda config: FakeSourceMySQL(config, {})
        )

        result = run_cli(["source", "check", "--config", str(path)])
        assert result.exit_code == 1
        assert "TABLE_NOT_FOUND" in result.output
        assert "1 enabled table(s) failed check" in result.output

    def test_key_column_missing_fails(
        self, run_cli, config_data, write_config, password_env, monkeypatch
    ):
        import airgap_sync.cli as cli_module

        path = write_config(config_data)
        monkeypatch.setattr(
            cli_module,
            "SourceMySQLConnection",
            lambda config: FakeSourceMySQL(config, {"t_keyed": {"name"}}),
        )

        result = run_cli(["source", "check", "--config", str(path)])
        assert result.exit_code == 1
        assert "KEY_COLUMN_NOT_FOUND" in result.output
        assert "missing key column(s): id" in result.output

    def test_mode_change_with_existing_baseline_fails(
        self, run_cli, config_data, write_config, password_env, monkeypatch, tmp_path
    ):
        """状态库中已有 baseline 的表, 配置改成不同 mode 时 CLI 必须报错退出。"""
        import airgap_sync.cli as cli_module
        from airgap_sync.source.state import SourceState, state_db_path

        monkeypatch.setattr(
            cli_module,
            "SourceMySQLConnection",
            lambda config: FakeSourceMySQL(config, {"t_keyed": {"id"}}),
        )

        # 预置状态: t_keyed (keyed) 已有 committed baseline
        state = SourceState(state_db_path(tmp_path / "data"))
        try:
            state.initialize()
            state.register_table("t_keyed", "keyed")
            state._conn.execute(
                "UPDATE table_state SET current_run_id = 'run-0001' WHERE table_name = 't_keyed'"
            )
        finally:
            state.close()

        # 同一 data_dir, 表改成 row_multiset
        config_data["tables"] = [{"name": "t_keyed", "mode": "row_multiset"}]
        path = write_config(config_data)

        result = run_cli(["source", "check", "--config", str(path)])
        assert result.exit_code == 1
        assert "cannot change sync mode of table 't_keyed'" in result.output
        assert "row_multiset" in result.output
