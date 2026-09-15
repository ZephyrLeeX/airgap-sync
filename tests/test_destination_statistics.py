from datetime import UTC, datetime

from airgap_sync.destination.mysql import TableVersion
from airgap_sync.destination.statistics import calculate_statistics


def version(run, created, rows, net=None, database="db", table="t"):
    stamp = datetime.fromisoformat(created).astimezone(UTC)
    return TableVersion(run, database, table, stamp, rows, None, None, net, stamp, stamp)


def test_current_this_run_and_monthly_net_positive_and_negative():
    stats = calculate_statistics(
        [
            version("aug", "2026-08-31T10:00:00+00:00", 100),
            version("sep-1", "2026-09-03T10:00:00+00:00", 110, 10),
            version("sep-2", "2026-09-27T10:00:00+00:00", 90, -20),
        ],
        "Asia/Shanghai",
    )[0]
    assert stats.current_rows == 90
    assert stats.this_run_net == -20
    assert stats.monthly_net == -10


def test_first_version_and_missing_previous_calendar_month_are_na():
    first = calculate_statistics([version("first", "2026-07-03T00:00:00+00:00", 100)], "UTC")[0]
    assert first.this_run_net is None
    assert first.monthly_net is None
    missing = calculate_statistics(
        [
            version("jul", "2026-07-03T00:00:00+00:00", 100),
            version("sep", "2026-09-03T00:00:00+00:00", 140, 40),
        ],
        "UTC",
    )[0]
    assert missing.monthly_net is None


def test_report_timezone_controls_month_boundary():
    stats = calculate_statistics(
        [
            version("aug", "2026-08-01T00:00:00+00:00", 100),
            version("boundary", "2026-08-31T16:30:00+00:00", 140, 40),
        ],
        "Asia/Shanghai",
    )[0]
    assert stats.source_created_at == datetime(2026, 8, 31, 16, 30, tzinfo=UTC)
    assert stats.monthly_net == 40
