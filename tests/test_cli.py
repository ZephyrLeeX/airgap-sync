"""CLI 测试 (直接调用 main(), 不需要真实 MySQL)。"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

import airgap_sync
from airgap_sync.cli import main

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

    def test_legacy_mode_config_rejected(self, run_cli, config_data, write_config, password_env):
        """旧 keyed 配置必须明确失败。"""
        config_data["tables"] = [{"name": "t_old", "mode": "keyed", "key": ["id"]}]
        path = write_config(config_data)
        result = run_cli(["config", "validate", "--config", str(path)])
        assert result.exit_code == 1
        assert "tables.0" in result.output

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


class FakeTableStream:
    def __init__(self, columns: list[str], batches: list[list[tuple]]) -> None:
        self.columns = columns
        self._batches = batches

    def __enter__(self) -> FakeTableStream:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def __iter__(self):
        yield from self._batches


class FakeSourceMySQL:
    """替身: 模拟 SourceMySQLConnection 的 ping / 元数据检查 / Snapshot 能力。"""

    def __init__(self, config, table_types: dict[str, str], rows: list[tuple] | None = None):
        self._config = config
        self.table_types = table_types
        self.rows = rows if rows is not None else [(1, "a"), (2, None)]
        self.queries: list[tuple[str, tuple]] = []

    def __enter__(self) -> FakeSourceMySQL:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def ping(self) -> str:
        return "5.7.35-log"

    def fetch_all(self, sql: str, params: tuple = ()) -> list[tuple]:
        self.queries.append((sql, params))
        if "information_schema.TABLES" in sql:
            table_type = self.table_types.get(params[1])
            return [(table_type,)] if table_type else []
        raise AssertionError(f"unexpected query: {sql}")

    def get_create_table(self, table_name: str) -> str:
        return (
            f"CREATE TABLE `{table_name}` (\n  `id` int(11) DEFAULT NULL,\n"
            "  `name` varchar(32) DEFAULT NULL\n) ENGINE=InnoDB"
        )

    def stream_table(self, table_name: str, fetch_size: int) -> FakeTableStream:
        batches = [self.rows[i : i + fetch_size] for i in range(0, len(self.rows), fetch_size)]
        return FakeTableStream(columns=["id", "name"], batches=batches)


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


class TestSourceCheckFullFlow:
    def test_success(self, run_cli, config_data, write_config, password_env, monkeypatch):
        import airgap_sync.cli as cli_module

        config_data["tables"].append({"name": "t_legacy", "enabled": False})
        path = write_config(config_data)

        def fake_connect(config):
            return FakeSourceMySQL(config, {"t_snapshot": "BASE TABLE"})

        monkeypatch.setattr(cli_module, "SourceMySQLConnection", fake_connect)

        result = run_cli(["source", "check", "--config", str(path)])
        assert result.exit_code == 0, result.output
        assert "Configuration       OK" in result.output
        assert "MySQL connection    OK" in result.output
        assert "MySQL server        5.7.35-log" in result.output
        assert "Database            sgaj_data" in result.output
        assert "SQLite state        OK" in result.output
        assert "schema v2" in result.output
        assert "t_snapshot" in result.output and "OK" in result.output
        assert "t_legacy" in result.output and "SKIP (disabled)" in result.output

        # 状态库已创建, 且通过的 enabled 表被登记
        state_db = path.parent / "data" / "state" / "meta.db"
        assert state_db.exists()
        from airgap_sync.source.state import SourceState

        state = SourceState(state_db)
        try:
            row = state.get_table_state("t_snapshot")
            assert row is not None
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
            cli_module,
            "SourceMySQLConnection",
            lambda config: FakeSourceMySQL(config, {}),
        )

        result = run_cli(["source", "check", "--config", str(path)])
        assert result.exit_code == 1
        assert "TABLE_NOT_FOUND" in result.output
        assert "1 enabled table(s) failed check" in result.output

    def test_view_rejected(self, run_cli, config_data, write_config, password_env, monkeypatch):
        import airgap_sync.cli as cli_module

        config_data["tables"] = [
            {"name": "t_snapshot", "enabled": True},
            {"name": "v_report", "enabled": True},
        ]
        path = write_config(config_data)
        monkeypatch.setattr(
            cli_module,
            "SourceMySQLConnection",
            lambda config: FakeSourceMySQL(
                config, {"t_snapshot": "BASE TABLE", "v_report": "VIEW"}
            ),
        )

        result = run_cli(["source", "check", "--config", str(path)])
        assert result.exit_code == 1
        assert "UNSUPPORTED_TABLE_TYPE (VIEW)" in result.output


class TestSourceSnapshot:
    def _patch(self, monkeypatch, rows=None):
        import airgap_sync.cli as cli_module

        def fake_connect(config):
            return FakeSourceMySQL(config, {"t_snapshot": "BASE TABLE"}, rows=rows)

        monkeypatch.setattr(cli_module, "SourceMySQLConnection", fake_connect)

    def test_success(self, run_cli, config_data, write_config, password_env, monkeypatch):
        self._patch(monkeypatch, rows=[(1, "a"), (2, None), (3, "中文")])
        path = write_config(config_data)
        config_data["snapshot"] = {"fetch_size": 2}
        path = write_config(config_data)

        result = run_cli(["source", "snapshot", "--config", str(path), "--table", "t_snapshot"])
        assert result.exit_code == 0, result.output
        assert "Table           t_snapshot" in result.output
        assert "Run             " in result.output
        assert "Rows            3" in result.output
        assert "Chunks          1" in result.output
        assert "Status          COMPLETED" in result.output
        assert "Output          " in result.output
        # 输出不含业务数据
        assert "中文" not in result.output

        run_dir = Path(result.output.split("Output          ")[1].strip().splitlines()[0])
        manifest_path = run_dir / "manifest.json"
        assert manifest_path.exists()
        assert (run_dir / "schema.sql").exists()
        chunks = sorted(run_dir.glob("chunk-*.jsonl.zst"))
        assert len(chunks) == 1

    def test_current_run_id_recorded(
        self, run_cli, config_data, write_config, password_env, monkeypatch
    ):
        self._patch(monkeypatch)
        path = write_config(config_data)
        result = run_cli(["source", "snapshot", "--config", str(path), "--table", "t_snapshot"])
        assert result.exit_code == 0, result.output

        from airgap_sync.source.state import SourceState, state_db_path

        state = SourceState(state_db_path(path.parent / "data"))
        try:
            row = state.get_table_state("t_snapshot")
            assert row is not None
            assert row.status == "COMPLETED"
            assert row.current_run_id in result.output
        finally:
            state.close()

    def test_table_not_in_config(
        self, run_cli, config_data, write_config, password_env, monkeypatch
    ):
        self._patch(monkeypatch)
        path = write_config(config_data)
        result = run_cli(["source", "snapshot", "--config", str(path), "--table", "ghost"])
        assert result.exit_code == 1
        assert "TABLE_NOT_CONFIGURED" in result.output

    def test_disabled_table(self, run_cli, config_data, write_config, password_env, monkeypatch):
        self._patch(monkeypatch)
        config_data["tables"] = [{"name": "t_snapshot", "enabled": False}]
        path = write_config(config_data)
        result = run_cli(["source", "snapshot", "--config", str(path), "--table", "t_snapshot"])
        assert result.exit_code == 1
        assert "TABLE_NOT_ENABLED" in result.output

    def test_scan_failure_reports_failed(
        self, run_cli, config_data, write_config, password_env, monkeypatch
    ):
        import airgap_sync.cli as cli_module

        class ExplodingSource(FakeSourceMySQL):
            def stream_table(self, table_name, fetch_size):
                raise RuntimeError("boom during scan")

        monkeypatch.setattr(
            cli_module,
            "SourceMySQLConnection",
            lambda config: ExplodingSource(config, {"t_snapshot": "BASE TABLE"}),
        )
        path = write_config(config_data)
        result = run_cli(["source", "snapshot", "--config", str(path), "--table", "t_snapshot"])
        assert result.exit_code == 1
        assert "Status          FAILED" in result.output
        assert "boom during scan" in result.output
        assert "ERROR: snapshot failed" in result.output

    def test_ddl_change_fails_without_manifest(
        self, run_cli, config_data, write_config, password_env, monkeypatch
    ):
        import airgap_sync.cli as cli_module

        class DdlChangingSource(FakeSourceMySQL):
            def __init__(self, config, table_types):
                super().__init__(config, table_types)
                self.calls = 0

            def get_create_table(self, table_name):
                self.calls += 1
                if self.calls == 2:
                    return super().get_create_table(table_name).replace("int(11)", "bigint(20)")
                return super().get_create_table(table_name)

        monkeypatch.setattr(
            cli_module,
            "SourceMySQLConnection",
            lambda config: DdlChangingSource(config, {"t_snapshot": "BASE TABLE"}),
        )
        path = write_config(config_data)
        result = run_cli(["source", "snapshot", "--config", str(path), "--table", "t_snapshot"])
        assert result.exit_code == 1
        assert "SCHEMA_CHANGED_DURING_SNAPSHOT" in result.output

        run_dir = Path(result.output.split("Output          ")[1].strip().splitlines()[0])
        assert not (run_dir / "manifest.json").exists()
        assert not (run_dir / "schema.sql").exists()
