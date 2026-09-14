"""airgap-sync 命令行入口。"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import click
from click.exceptions import Exit as ClickExit

from airgap_sync import __version__
from airgap_sync.common.config import ConfigError, load_config, resolve_password
from airgap_sync.common.logging import setup_logging
from airgap_sync.common.models import AppConfig, Role
from airgap_sync.source.mysql import (
    SourceMySQLConnection,
    SourceMySQLError,
    TableCheckResult,
    check_tables,
)
from airgap_sync.source.snapshot import SnapshotError, SnapshotResult, SnapshotRunner
from airgap_sync.source.state import SourceState, StateError, state_db_path

logger = logging.getLogger(__name__)

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")

_SIZE_UNITS = ("B", "KiB", "MiB", "GiB", "TiB")


@click.group()
@click.version_option(version=__version__, prog_name="airgap-sync")
@click.option(
    "--log-level",
    type=click.Choice(LOG_LEVELS),
    default=None,
    envvar="AIRGAP_SYNC_LOG_LEVEL",
    help="日志级别 (默认 INFO, 也可通过环境变量 AIRGAP_SYNC_LOG_LEVEL 设置)。",
)
def cli(log_level: str | None) -> None:
    """Airgap Sync - 单向隔离网络 MySQL 数据同步工具。"""
    setup_logging(log_level)


@cli.group("config")
def config_group() -> None:
    """配置相关命令。"""


@config_group.command("validate")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="YAML 配置文件路径。",
)
def config_validate(config_path: Path) -> None:
    """加载并校验 YAML 配置, 确认密码环境变量已设置。"""
    config = load_config(config_path)
    resolve_password(config.mysql)
    logger.debug("config validated: %s", config_path)
    click.echo("Configuration OK")
    click.echo(
        f"role={config.role.value} tables={len(config.tables)} enabled={len(config.enabled_tables)}"
    )
    click.echo(
        f"mysql={config.mysql.user}@{config.mysql.host}:{config.mysql.port}/{config.mysql.database}"
    )


@cli.group("source")
def source_group() -> None:
    """Source 端命令。"""


@source_group.command("check")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="YAML 配置文件路径。",
)
def source_check(config_path: Path) -> None:
    """检查配置、MySQL 连接、SQLite 状态库和同步表元数据。

    表级检查只验证表存在且为 BASE TABLE, 不做全表扫描。
    """
    config = load_config(config_path)
    _require_source_role(config)
    resolve_password(config.mysql)
    click.echo("Configuration       OK")

    with SourceMySQLConnection(config.mysql) as connection:
        server_version = connection.ping()
        click.echo("MySQL connection    OK")
        click.echo(f"MySQL server        {server_version}")
        click.echo(f"Database            {config.mysql.database}")
        results = check_tables(connection, config.mysql.database, config.enabled_tables)

    state = SourceState(state_db_path(config.paths.data_dir))
    try:
        state.initialize()
        click.echo(f"SQLite state        OK ({state.db_path}, schema v{state.schema_version()})")
        _print_table_report(config, results, state)
    finally:
        state.close()

    failed = [result for result in results if not result.ok]
    if failed:
        click.echo(f"ERROR: {len(failed)} enabled table(s) failed check", err=True)
        sys.exit(1)


@source_group.command("snapshot")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="YAML 配置文件路径。",
)
@click.option("--table", "table_name", required=True, help="要生成 Snapshot 的表名。")
def source_snapshot(config_path: Path, table_name: str) -> None:
    """生成一张表的完整 Full Snapshot Run。

    单条流式 SELECT * 全表扫描 → JSONL + zstd 分 Chunk →
    schema.sql + manifest.json, 输出到 <data_dir>/outbox/<table>/<run_id>/。
    """
    config = load_config(config_path)
    _require_source_role(config)
    resolve_password(config.mysql)

    with SourceMySQLConnection(config.mysql) as connection:
        state = SourceState(state_db_path(config.paths.data_dir))
        try:
            state.initialize()
            runner = SnapshotRunner(connection, state, config)
            result = runner.snapshot(table_name)
        finally:
            state.close()

    _print_snapshot_result(result)
    if result.status != "COMPLETED":
        click.echo(f"ERROR: snapshot failed: {result.error}", err=True)
        sys.exit(1)


def _require_source_role(config: AppConfig) -> None:
    if config.role is not Role.SOURCE:
        raise ConfigError(
            f"'source' commands require role=source, but config has role={config.role.value}"
        )


def _print_table_report(
    config: AppConfig, results: list[TableCheckResult], state: SourceState
) -> None:
    click.echo()
    click.echo("Tables:")
    result_by_name = {result.table.name: result for result in results}
    for table in config.tables:
        label = f"{table.name:<24}"
        if not table.enabled:
            click.echo(f"{label}SKIP (disabled)")
            continue
        result = result_by_name[table.name]
        if result.ok:
            state.register_table(table.name)
            click.echo(f"{label}OK")
        else:
            click.echo(f"{label}FAIL {_failure_detail(result)}")


def _failure_detail(result: TableCheckResult) -> str:
    if result.error_code == "UNSUPPORTED_TABLE_TYPE":
        return f"UNSUPPORTED_TABLE_TYPE ({result.table_type})"
    return result.error_code or "UNKNOWN"


def _print_snapshot_result(result: SnapshotResult) -> None:
    """输出 Run 摘要 (不输出任何业务数据)。"""
    click.echo(f"Table           {result.table}")
    click.echo(f"Run             {result.run_id}")
    click.echo(f"Rows            {result.row_count:,}")
    click.echo(f"Chunks          {result.chunk_count}")
    click.echo(f"Raw size        {_format_size(result.raw_bytes)}")
    click.echo(f"Compressed      {_format_size(result.compressed_bytes)}")
    click.echo(f"Status          {result.status}")
    click.echo(f"Output          {result.run_dir}")


def _format_size(size: int) -> str:
    """字节数的人类可读表达 (二进制单位)。"""
    value = float(size)
    for unit in _SIZE_UNITS:
        if value < 1024 or unit == _SIZE_UNITS[-1]:
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{size} B"  # pragma: no cover - 循环必在 TiB 内返回


def main() -> None:
    """airgap-sync 命令入口 (pyproject 脚本指向这里)。"""
    try:
        cli(standalone_mode=False)
    except ClickExit as exc:
        sys.exit(exc.exit_code)
    except click.ClickException as exc:
        exc.show()
        sys.exit(exc.exit_code)
    except click.Abort:
        click.echo("Aborted.", err=True)
        sys.exit(130)
    except (ConfigError, SourceMySQLError, StateError, SnapshotError) as exc:
        click.echo(f"ERROR: {exc}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
