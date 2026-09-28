"""A per-request read-only snapshot assembled from destination-owned facts."""

from __future__ import annotations

import os
from contextlib import suppress
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from airgap_sync.common.models import AppConfig
from airgap_sync.destination.mysql import (
    METADATA_SCHEMA_VERSION,
    DestinationMySQLConnection,
    MonitoringRunRecord,
)
from airgap_sync.destination.statistics import calculate_statistics
from airgap_sync.monitor.system import system_snapshot

MONITOR_DB_TIMEOUT_SECONDS = 3


def utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def age(value: datetime | None, now: datetime) -> str:
    if value is None:
        return "—"
    seconds = max(0, int((now - utc(value)).total_seconds()))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes = seconds // 60
    return f"{days}d {hours}h" if days else f"{hours}h {minutes}m"


def _run(run: MonitoringRunRecord, now: datetime, zone: ZoneInfo, secrets: tuple[str, ...]) -> dict:
    dates = {
        key: utc(getattr(run, key))
        for key in (
            "source_created_at",
            "manifest_received_at",
            "validated_at",
            "import_started_at",
            "import_completed_at",
            "digest_verified_at",
            "applied_at",
        )
    }
    start = dates["manifest_received_at"]
    end = dates["applied_at"] or utc(run.updated_at)
    return {
        "run_id": run.run_id,
        "source_database": run.source_database,
        "table_name": run.table_name,
        "status": run.status,
        "expected_rows": run.expected_rows,
        "actual_rows": run.actual_rows,
        "chunk_count": run.chunk_count,
        "timestamps": {key: value.isoformat() if value else None for key, value in dates.items()},
        "display_timestamps": {
            key: value.astimezone(zone).strftime("%Y-%m-%d %H:%M:%S %Z") if value else "—"
            for key, value in dates.items()
        },
        "duration": str(end - start).split(".")[0] if start and end >= start else "—",
        "last_error": _redact(run.last_error, secrets),
        "cleanup_pending": run.cleanup_pending,
        "cleanup_error": _redact(run.cleanup_error or run.backup_cleanup_error, secrets),
        "updated_at": utc(run.updated_at).isoformat(),
    }


def _redact(value: str | None, secrets: tuple[str, ...]) -> str | None:
    if value is None:
        return None
    for secret in secrets:
        value = value.replace(secret, "[REDACTED]")
    return value


def snapshot(config: AppConfig) -> dict:
    assert config.destination is not None
    now = datetime.now(UTC)
    zone = ZoneInfo(config.destination.report_timezone)
    secret_names = [config.mysql.password_env]
    if config.relay is not None:
        secret_names.append(config.relay.token_env)
    secrets = tuple(value for name in secret_names if (value := os.environ.get(name)))
    system = system_snapshot(config.destination.incoming_dir)
    data = {
        "generated_at": now.isoformat(),
        "timezone": str(zone),
        "system": system,
        "status": "UNKNOWN",
        "rds": "DISCONNECTED",
        "mysql_version": None,
        "target_database": config.mysql.database,
        "metadata_version": None,
        "metadata_status": "unavailable",
        "query_error": None,
        "tables": [],
        "runs": [],
        "problems": [],
        "counts": None,
        "latest_verified": None,
        "active_run": None,
        "max_data_age": None,
    }
    if system["worker"]["status"] == "NOT RUNNING":
        data["problems"].append("Destination worker is not running")
    elif system["worker"]["status"] == "UNKNOWN":
        data["problems"].append("Destination worker status unknown")
    if system["incoming_error"]:
        data["problems"].append("Incoming discovery unavailable")
    connection = DestinationMySQLConnection(
        config.mysql, config.destination, monitor_timeout=MONITOR_DB_TIMEOUT_SECONDS
    )
    latest_runs: list[dict] = []
    try:
        with connection:
            data["mysql_version"] = connection.ping()
            data["rds"] = "CONNECTED"
            version = connection.metadata_schema_version()
            data["metadata_version"] = version
            if version != METADATA_SCHEMA_VERSION:
                data["metadata_status"] = "schema mismatch"
                data["problems"].append("Metadata schema mismatch")
            else:
                data["metadata_status"] = "OK"
                # Keep completed sections, then stop on the first failed query.
                for section, query in (
                    ("latest_runs", connection.monitoring_latest_runs),
                    ("runs", lambda: connection.monitoring_runs()),
                    ("active", lambda: connection.monitoring_active_runs()),
                    ("counts", lambda: connection.monitoring_counts()),
                    ("tables", lambda: connection.all_versions()),
                ):
                    try:
                        rows = query()
                        if section == "latest_runs":
                            latest_runs = [_run(row, now, zone, secrets) for row in rows]
                        elif section == "runs":
                            data["runs"] = [_run(row, now, zone, secrets) for row in rows]
                        elif section == "active":
                            data["active_run"] = _run(rows[0], now, zone, secrets) if rows else None
                        elif section == "counts":
                            data["counts"] = rows
                        else:
                            latest = max(rows, key=lambda row: utc(row.applied_at), default=None)
                            if latest is not None:
                                applied = utc(latest.applied_at)
                                data["latest_verified"] = {
                                    "run_id": latest.run_id,
                                    "display_timestamps": {
                                        "applied_at": applied.astimezone(zone).strftime(
                                            "%Y-%m-%d %H:%M:%S %Z"
                                        )
                                    },
                                }
                            stats = calculate_statistics(rows, str(zone))
                            data["tables"] = [
                                {
                                    "source_database": item.source_database,
                                    "table_name": item.table_name,
                                    "row_count": item.current_rows,
                                    "this_run_net": item.this_run_net,
                                    "monthly_net": item.monthly_net,
                                    "verified_at": utc(item.verified_at).isoformat(),
                                    "verified_display": utc(item.verified_at)
                                    .astimezone(zone)
                                    .strftime("%Y-%m-%d %H:%M:%S %Z"),
                                    "data_age": age(item.source_created_at, now),
                                    "data_age_seconds": max(
                                        0, int((now - utc(item.source_created_at)).total_seconds())
                                    ),
                                    "run_id": item.run_id,
                                }
                                for item in stats
                            ]
                    except Exception:
                        data["query_error"] = "Metadata query unavailable"
                        # A timed-out socket is unusable. Do not pay the read
                        # timeout again for each remaining section.
                        break
    except Exception:
        # MySQL and config errors may include credentials, host details, or environment names.
        data["query_error"] = (
            "Destination MySQL connection unavailable"
            if data["rds"] == "DISCONNECTED"
            else "Metadata query unavailable"
        )
    finally:
        with suppress(Exception):
            connection.close()
    if data["rds"] == "DISCONNECTED":
        data["problems"].append("RDS unavailable")
    elif data["metadata_status"] != "OK":
        data["problems"].append("Metadata unavailable")
    if data["query_error"]:
        data["problems"].append(data["query_error"])
    known_failures = {}
    if data["counts"] is not None:
        for state in ("FAILED", "MISMATCH"):
            if data["counts"].get(state, 0):
                data["problems"].append(f"Historical {state} runs: {data['counts'][state]}")
        if data["counts"].get("cleanup_pending", 0):
            data["problems"].append(f"Cleanup pending: {data['counts']['cleanup_pending']}")
    else:
        # These are observed runs, not a substitute for the unavailable global counts.
        for run in (*latest_runs, *data["runs"]):
            if run["status"] in ("FAILED", "MISMATCH"):
                known_failures.setdefault(run["run_id"], run)
        for run in known_failures.values():
            data["problems"].append(
                f"Known {run['status']} run: {run['run_id']} "
                f"({run['source_database']}.{run['table_name']})"
            )
    latest_by_table = {(run["source_database"], run["table_name"]): run for run in latest_runs}
    table_by_identity = {
        (table["source_database"], table["table_name"]): table for table in data["tables"]
    }
    for identity in latest_by_table:
        if identity not in table_by_identity:
            table = {
                "source_database": identity[0],
                "table_name": identity[1],
                "row_count": None,
                "this_run_net": None,
                "monthly_net": None,
                "verified_at": None,
                "verified_display": "—",
                "data_age": "—",
                "data_age_seconds": None,
                "run_id": None,
            }
            data["tables"].append(table)
            table_by_identity[identity] = table
    for table in data["tables"]:
        latest = latest_by_table.get((table["source_database"], table["table_name"]))
        table["latest_status"] = latest["status"] if latest else "—"
        table["latest_error"] = latest["last_error"] if latest else None
    data["tables"].sort(
        key=lambda row: (
            row["latest_status"] not in ("FAILED", "MISMATCH"),
            -(row["data_age_seconds"] or 0),
            row["source_database"],
            row["table_name"],
        )
    )
    known_age = [table for table in data["tables"] if table["data_age_seconds"] is not None]
    oldest = max(known_age, key=lambda row: row["data_age_seconds"], default=None)
    data["max_data_age"] = oldest["data_age"] if oldest else None
    if (
        any(run["status"] in ("FAILED", "MISMATCH") for run in latest_by_table.values())
        or system["worker"]["status"] == "NOT RUNNING"
    ):
        data["status"] = "CRITICAL"
    elif (
        data["rds"] == "DISCONNECTED"
        or data["query_error"]
        or data["metadata_status"] != "OK"
        or system["incoming_error"]
    ):
        data["status"] = "DEGRADED"
    elif system["worker"]["status"] == "UNKNOWN":
        data["status"] = "UNKNOWN"
    else:
        data["status"] = "HEALTHY"
    return data
