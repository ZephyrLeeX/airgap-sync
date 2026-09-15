"""airgap-sync 命令行入口。"""

from __future__ import annotations

import logging
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path
from threading import Event

import click
from click.exceptions import Exit as ClickExit

from airgap_sync import __version__
from airgap_sync.common.config import (
    ConfigError,
    load_config,
    resolve_password,
    resolve_relay_token,
)
from airgap_sync.common.logging import setup_logging
from airgap_sync.common.models import AppConfig, Role, TableConfig
from airgap_sync.common.runtime import ProcessLock, WorkerAlreadyRunning
from airgap_sync.destination.incoming import DestinationError, discover_runs
from airgap_sync.destination.mysql import DestinationMySQLConnection, DestinationMySQLError
from airgap_sync.destination.processor import DestinationProcessor, ProcessResult, process_once
from airgap_sync.destination.statistics import TableStatistics, calculate_statistics
from airgap_sync.destination.worker import DestinationWorker, cleanup_orphan_artifacts
from airgap_sync.source.cycle import CycleRunner, SourceWorker, cleanup_failed_runs, next_action_at
from airgap_sync.source.delivery import DeliveryRunner
from airgap_sync.source.mysql import (
    SourceMySQLConnection,
    SourceMySQLError,
    TableCheckResult,
    check_tables,
)
from airgap_sync.source.snapshot import SnapshotError, SnapshotResult, SnapshotRunner
from airgap_sync.source.state import SourceState, StateError, state_db_path
from airgap_sync.source.uploader import RelayUploader, UploadError

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
    if config.role is Role.DESTINATION:
        _require_destination_role(config)
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


@cli.group("destination")
def destination_group() -> None:
    """Destination 端命令。"""


@destination_group.command("check")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
def destination_check(config_path: Path) -> None:
    """检查 Destination 配置、incoming、目标 MySQL 与 metadata schema。"""
    config = load_config(config_path)
    _require_destination_role(config)
    assert config.destination is not None
    resolve_password(config.mysql)
    _ensure_incoming(config.destination.incoming_dir)
    click.echo("Configuration       OK")
    click.echo(f"Incoming            OK ({config.destination.incoming_dir})")
    with DestinationMySQLConnection(config.mysql, config.destination) as connection:
        version = connection.ping()
        connection.initialize_metadata()
        metadata_version = connection.metadata_schema_version()
    click.echo("MySQL connection    OK")
    click.echo(f"MySQL server        {version}")
    click.echo(f"Target database     {config.mysql.database}")
    click.echo(
        f"Metadata schema     OK ({config.destination.metadata_database}, v{metadata_version})"
    )


@destination_group.command("process")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
@click.option("--run", "run_id", required=True, help="要处理的 transport Run ID。")
def destination_process(config_path: Path, run_id: str) -> None:
    """完整处理 Run：staging、数据库回读验证、安全切换与清理。"""
    config = load_config(config_path)
    _require_destination_role(config)
    assert config.destination is not None
    _ensure_incoming(config.destination.incoming_dir)
    with DestinationMySQLConnection(config.mysql, config.destination) as connection:
        connection.initialize_metadata()
        result = DestinationProcessor(connection, config).process(run_id)
    _print_destination_result(result)
    if result.status == "FAILED":
        raise click.ClickException(result.error or "destination processing failed")


@destination_group.command("process-once")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
def destination_process_once(config_path: Path) -> None:
    """扫描当前所有 manifest，各尝试一次，然后退出。"""
    config = load_config(config_path)
    _require_destination_role(config)
    assert config.destination is not None
    _ensure_incoming(config.destination.incoming_dir)
    with DestinationMySQLConnection(config.mysql, config.destination) as connection:
        connection.initialize_metadata()
        results = process_once(connection, config)
    for result in results:
        click.echo(f"{result.run_id}    {result.status}")
        if result.error:
            click.echo(f"  {result.error}")
    if any(result.status == "FAILED" for result in results):
        raise click.ClickException("one or more runs failed permanently")


@destination_group.command("worker")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
def destination_worker(config_path: Path) -> None:
    """前台轮询 incoming，处理 ready Run 并重试维护任务。"""
    config = load_config(config_path)
    _require_destination_role(config)
    assert config.destination is not None
    _ensure_incoming(config.destination.incoming_dir)
    stop = Event()
    _install_stop_handlers(stop)
    lock_path = config.destination.incoming_dir / ".airgap-sync-destination-worker.lock"
    with (
        ProcessLock(lock_path),
        DestinationMySQLConnection(config.mysql, config.destination) as connection,
    ):
        connection.initialize_metadata()
        cleanup_orphan_artifacts(connection, config)
        DestinationWorker(connection, config, stop_event=stop).run()


@destination_group.command("status")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
def destination_status(config_path: Path) -> None:
    """显示 incoming、cleanup pending、最新 VERIFIED 与异常 Run。"""
    config = load_config(config_path)
    _require_destination_role(config)
    assert config.destination is not None
    candidates = discover_runs(config.destination.incoming_dir)
    with DestinationMySQLConnection(config.mysql, config.destination) as connection:
        connection.initialize_metadata()
        pending = connection.cleanup_pending_runs()
        verified = connection.latest_verified_runs(20)
        problems = connection.recent_problem_runs(20)
    click.echo(f"Incoming candidate runs  {len(candidates)}")
    for run_id in candidates[:20]:
        click.echo(f"  {run_id}")
    click.echo(f"Cleanup pending          {len(pending)}")
    for run in pending[:20]:
        click.echo(f"  {run.run_id}  {run.table_name}  {run.status}")
    click.echo("Latest VERIFIED tables")
    for run in verified:
        click.echo(f"  {run.table_name:<24} {run.run_id}")
    click.echo("MISMATCH/FAILED runs")
    for run in problems:
        click.echo(f"  {run.status:<10} {run.table_name:<24} {run.run_id}")


@destination_group.command("stats")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
@click.option("--table", "table_name", help="只显示该表。")
@click.option("--source-database", help="同名表存在时指定 Source database。")
def destination_stats(
    config_path: Path, table_name: str | None, source_database: str | None
) -> None:
    """显示 VERIFIED 正式版本的总行数、本次净增和月度净增。"""
    config = load_config(config_path)
    _require_destination_role(config)
    assert config.destination is not None
    with DestinationMySQLConnection(config.mysql, config.destination) as connection:
        connection.initialize_metadata()
        stats = calculate_statistics(connection.all_versions(), config.destination.report_timezone)
    if table_name is not None:
        stats = [item for item in stats if item.table_name == table_name]
    if source_database is not None:
        stats = [item for item in stats if item.source_database == source_database]
    databases = {item.source_database for item in stats}
    if table_name and source_database is None and len(databases) > 1:
        raise click.ClickException(
            "multiple source databases contain this table; specify --source-database"
        )
    for index, item in enumerate(stats):
        if index:
            click.echo()
        _print_statistics(item, config.destination.report_timezone)


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


@source_group.command("relay-check")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
def source_relay_check(config_path: Path) -> None:
    """检查 Relay GET /health（不会上传文件）。"""
    config = load_config(config_path)
    _require_source_role(config)
    if config.relay is None:
        raise ConfigError("relay configuration is required for source relay-check")
    RelayUploader(config.relay, "").check_health()
    click.echo("Relay HTTP    OK")


@source_group.command("sync")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
@click.option("--table", "table_name", required=True, help="要可靠交付的表名。")
def source_sync(config_path: Path, table_name: str) -> None:
    """生成 Full Snapshot 并通过 HTTP Relay 可靠交付。"""
    config = load_config(config_path)
    _require_source_role(config)
    if config.relay is None:
        raise ConfigError("relay configuration is required for source sync")
    resolve_password(config.mysql)
    token = resolve_relay_token(config.relay)
    uploader = RelayUploader(config.relay, token)
    with SourceMySQLConnection(config.mysql) as connection:
        state = SourceState(state_db_path(config.paths.data_dir))
        try:
            state.initialize()
            result = DeliveryRunner(connection, state, config, uploader).sync(table_name)
        finally:
            state.close()
    _print_snapshot_result(result, relay=config.relay.base_url)
    if result.status != "DELIVERED":
        click.echo(f"ERROR: sync failed: {result.error}", err=True)
        sys.exit(1)


def _cycle_runner(config: AppConfig, state: SourceState, stop: Event | None = None) -> CycleRunner:
    if config.relay is None:
        raise ConfigError("relay configuration is required for source cycle")
    token = resolve_relay_token(config.relay)

    def sync_table(table_name: str) -> SnapshotResult:
        captured = config.table(table_name)
        if captured is None:
            tables = [*config.tables, TableConfig(name=table_name, enabled=True)]
            effective_config = config.model_copy(update={"tables": tables})
        elif not captured.enabled:
            tables = [
                table.model_copy(update={"enabled": True}) if table.name == table_name else table
                for table in config.tables
            ]
            effective_config = config.model_copy(update={"tables": tables})
        else:
            effective_config = config
        with SourceMySQLConnection(config.mysql) as connection:
            uploader = RelayUploader(config.relay, token)
            return DeliveryRunner(connection, state, effective_config, uploader).sync(table_name)

    return CycleRunner(state, config, sync_table, stop_event=stop)


@source_group.group("cycle")
def source_cycle_group() -> None:
    """Source persisted Cycle 管理。"""


@source_cycle_group.command("run")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
def source_cycle_run(config_path: Path) -> None:
    """立即创建或恢复一个完整 Cycle。"""
    config = load_config(config_path)
    _require_source_role(config)
    resolve_password(config.mysql)
    assert config.paths is not None
    lock_path = config.paths.data_dir / "state" / "source-worker.lock"
    with ProcessLock(lock_path), SourceState(state_db_path(config.paths.data_dir)) as state:
        state.initialize()
        cycle = _cycle_runner(config, state).run()
    click.echo(f"Cycle       {cycle.cycle_id}")
    click.echo(f"Status      {cycle.status}")
    if cycle.next_attempt_at:
        click.echo(f"Next retry  {cycle.next_attempt_at}")


@source_cycle_group.command("abandon")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
def source_cycle_abandon(config_path: Path) -> None:
    """显式放弃当前 RUNNING/RETRY_WAIT Cycle。"""
    config = load_config(config_path)
    _require_source_role(config)
    assert config.paths is not None
    lock_path = config.paths.data_dir / "state" / "source-worker.lock"
    with ProcessLock(lock_path), SourceState(state_db_path(config.paths.data_dir)) as state:
        state.initialize()
        cycle = state.abandon_active_cycle()
    click.echo(f"Abandoned   {cycle.cycle_id}")


@source_group.command("worker")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
def source_worker(config_path: Path) -> None:
    """前台运行 completion-driven fixed-delay Source worker。"""
    config = load_config(config_path)
    _require_source_role(config)
    if not config.schedule.enabled:
        raise ConfigError("schedule.enabled must be true for source worker")
    resolve_password(config.mysql)
    assert config.paths is not None
    stop = Event()
    _install_stop_handlers(stop)
    lock_path = config.paths.data_dir / "state" / "source-worker.lock"
    with ProcessLock(lock_path), SourceState(state_db_path(config.paths.data_dir)) as state:
        state.initialize()
        SourceWorker(
            state,
            config,
            lambda: _cycle_runner(config, state, stop).run(),
            stop_event=stop,
            maintenance=lambda: cleanup_failed_runs(state, config),
        ).run()


@source_group.command("status")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
def source_status(config_path: Path) -> None:
    """显示最新 Cycle、下一动作和捕获的表状态。"""
    config = load_config(config_path)
    _require_source_role(config)
    assert config.paths is not None
    with SourceState(state_db_path(config.paths.data_dir)) as state:
        state.initialize()
        cycle = state.latest_cycle()
        if cycle is None:
            click.echo("Latest Cycle  none")
            click.echo("Next action   now")
            return
        tables = state.cycle_tables(cycle.cycle_id)
        due = next_action_at(state, config, datetime.now(UTC))
    click.echo(f"Latest Cycle  {cycle.cycle_id}")
    click.echo(f"Status        {cycle.status}")
    click.echo(f"Started       {cycle.started_at or '-'}")
    click.echo(f"Completed     {cycle.completed_at or '-'}")
    click.echo(f"Next action   {due.isoformat(timespec='seconds')}")
    click.echo(
        "Table                    Status       Attempts  Last run                    Delivered"
    )
    for table in tables:
        click.echo(
            f"{table.table_name:<24} {table.status:<12} {table.attempts:<9} "
            f"{table.last_run_id or '-':<27} {table.delivered_at or '-'}"
        )
        if table.last_error:
            click.echo(f"  Last error: {table.last_error}")


def _require_source_role(config: AppConfig) -> None:
    if config.role is not Role.SOURCE:
        raise ConfigError(
            f"'source' commands require role=source, but config has role={config.role.value}"
        )


def _require_destination_role(config: AppConfig) -> None:
    if config.role is not Role.DESTINATION:
        raise ConfigError(
            "'destination' commands require role=destination, "
            f"but config has role={config.role.value}"
        )
    if config.destination is None:
        raise ConfigError("destination configuration is required for destination commands")


def _ensure_incoming(path: Path) -> None:
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise DestinationError("INCOMING_DIR_ERROR", str(exc)) from exc


def _print_destination_result(result: ProcessResult) -> None:
    click.echo(f"Run             {result.run_id}")
    if result.table is not None:
        click.echo(f"Table           {result.table}")
    if result.staging_table is not None:
        click.echo(f"Staging         {result.staging_table}")
    click.echo(f"Rows            {result.rows:,}")
    click.echo(f"Chunks          {result.chunks}")
    click.echo(f"Status          {result.status}")
    if result.error:
        click.echo(f"Detail          {result.error}")


def _print_statistics(item: TableStatistics, timezone_name: str) -> None:
    from zoneinfo import ZoneInfo

    timezone = ZoneInfo(timezone_name)
    snapshot = item.source_created_at.astimezone(timezone)
    verified = item.verified_at.astimezone(timezone)
    click.echo(f"Source database          {item.source_database}")
    click.echo(f"Table                    {item.table_name}")
    click.echo(f"Current rows             {item.current_rows:,}")
    click.echo(f"This run net             {_format_delta(item.this_run_net)}")
    click.echo(f"Monthly net              {_format_delta(item.monthly_net)}")
    click.echo(f"Snapshot                 {snapshot:%Y-%m-%d %H:%M %z}")
    click.echo(f"Verified                 {verified:%Y-%m-%d %H:%M %z}")
    click.echo(f"Run                      {item.run_id}")


def _format_delta(value: int | None) -> str:
    return "N/A" if value is None else f"{value:+,}"


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


def _print_snapshot_result(result: SnapshotResult, *, relay: str | None = None) -> None:
    """输出 Run 摘要 (不输出任何业务数据)。"""
    click.echo(f"Table           {result.table}")
    click.echo(f"Run             {result.run_id}")
    click.echo(f"Rows            {result.row_count:,}")
    click.echo(f"Chunks          {result.chunk_count}")
    click.echo(f"Raw size        {_format_size(result.raw_bytes)}")
    click.echo(f"Compressed      {_format_size(result.compressed_bytes)}")
    click.echo(f"Status          {result.status}")
    if relay is None:
        click.echo(f"Output          {result.run_dir}")
    else:
        click.echo(f"Uploaded        {result.chunk_count}/{result.chunk_count}")
        click.echo(f"Relay           {relay}")


def _format_size(size: int) -> str:
    """字节数的人类可读表达 (二进制单位)。"""
    value = float(size)
    for unit in _SIZE_UNITS:
        if value < 1024 or unit == _SIZE_UNITS[-1]:
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{size} B"  # pragma: no cover - 循环必在 TiB 内返回


def _install_stop_handlers(stop: Event) -> None:
    def request_shutdown(signum: int, _frame: object) -> None:
        logger.info("shutdown requested: signal=%s", signum)
        stop.set()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)


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
    except (
        ConfigError,
        DestinationError,
        DestinationMySQLError,
        SourceMySQLError,
        StateError,
        SnapshotError,
        UploadError,
        WorkerAlreadyRunning,
    ) as exc:
        click.echo(f"ERROR: {exc}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
