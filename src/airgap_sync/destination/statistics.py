"""基于 table_versions 的轻量目标端统计（不扫描 live table）。"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from airgap_sync.destination.mysql import TableVersion


@dataclass(frozen=True)
class TableStatistics:
    source_database: str
    table_name: str
    current_rows: int
    this_run_net: int | None
    monthly_net: int | None
    source_created_at: datetime
    verified_at: datetime
    run_id: str


def calculate_statistics(
    versions: list[TableVersion], report_timezone: str
) -> list[TableStatistics]:
    timezone = ZoneInfo(report_timezone)
    grouped: dict[tuple[str, str], list[TableVersion]] = defaultdict(list)
    for version in versions:
        grouped[(version.source_database, version.table_name)].append(version)

    result: list[TableStatistics] = []
    for identity, history in sorted(grouped.items()):
        history.sort(key=lambda item: (_aware(item.source_created_at), item.applied_at))
        current = history[-1]
        current_local = _aware(current.source_created_at).astimezone(timezone)
        previous_year, previous_month = _previous_month(current_local.year, current_local.month)
        prior_month = [
            version
            for version in history
            if _month(version.source_created_at, timezone) == (previous_year, previous_month)
        ]
        previous_month_last = max(
            prior_month, key=lambda item: _aware(item.source_created_at), default=None
        )
        monthly_net = (
            None
            if previous_month_last is None
            else current.row_count - previous_month_last.row_count
        )
        result.append(
            TableStatistics(
                identity[0],
                identity[1],
                current.row_count,
                current.net_change,
                monthly_net,
                _aware(current.source_created_at),
                _aware(current.verified_at),
                current.run_id,
            )
        )
    return result


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _month(value: datetime, timezone: ZoneInfo) -> tuple[int, int]:
    local = _aware(value).astimezone(timezone)
    return local.year, local.month


def _previous_month(year: int, month: int) -> tuple[int, int]:
    return (year - 1, 12) if month == 1 else (year, month - 1)
