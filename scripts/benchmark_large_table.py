#!/usr/bin/env python3
"""Generate a disposable MySQL large-table fixture and print generation throughput.

This intentionally refuses to run unless both an explicit flag and environment
guard are present. It never derives credentials or targets from production config.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from decimal import Decimal

import pymysql


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--database", required=True, help="Disposable test database only")
    parser.add_argument("--user", required=True)
    parser.add_argument("--password-env", required=True)
    parser.add_argument("--table", default="airgap_benchmark_large")
    parser.add_argument("--rows", type=int, default=7_000_000)
    parser.add_argument("--batch", type=int, default=5_000)
    parser.add_argument("--confirm-destructive", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = arguments()
    if not args.confirm_destructive or os.environ.get("AIRGAP_BENCHMARK_DISPOSABLE") != "YES":
        print(
            "REFUSED: use a disposable database, set AIRGAP_BENCHMARK_DISPOSABLE=YES, "
            "and pass --confirm-destructive",
            file=sys.stderr,
        )
        return 2
    password = os.environ.get(args.password_env)
    if not password:
        print(
            f"REFUSED: password environment variable {args.password_env!r} is empty",
            file=sys.stderr,
        )
        return 2
    if args.rows < 1 or args.batch < 1:
        print("--rows and --batch must be positive", file=sys.stderr)
        return 2
    quoted_table = "`" + args.table.replace("`", "``") + "`"
    connection = pymysql.connect(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=password,
        charset="utf8mb4",
        autocommit=False,
    )
    started = time.monotonic()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {quoted_table}")
            cursor.execute(
                f"CREATE TABLE {quoted_table} ("
                "id BIGINT NOT NULL, label VARCHAR(100) NULL, amount DECIMAL(20,6) NULL,"
                "event_time DATETIME(6) NULL, observed_at TIMESTAMP(6) NULL,"
                "payload VARBINARY(64) NULL"
                ") ENGINE=InnoDB"
            )
            sql = f"INSERT INTO {quoted_table} VALUES (%s,%s,%s,%s,%s,%s)"
            for offset in range(0, args.rows, args.batch):
                stop = min(args.rows, offset + args.batch)
                rows = []
                for index in range(offset, stop):
                    duplicate = index // 10
                    rows.append(
                        (
                            duplicate,
                            None if index % 17 == 0 else f"fixture-{duplicate % 10000}",
                            None if index % 19 == 0 else Decimal(duplicate) / Decimal("1000"),
                            None if index % 23 == 0 else "2026-09-15 08:00:00.123456",
                            "2026-09-15 00:00:00.654321",
                            None if index % 29 == 0 else bytes((index % 256, 0, 255)),
                        )
                    )
                cursor.executemany(sql, rows)
                connection.commit()
        elapsed = time.monotonic() - started
        print(
            f"rows={args.rows} duration_seconds={elapsed:.3f} "
            f"rows_per_second={args.rows / elapsed:.1f}"
        )
        print(f"database={args.database} table={args.table} (DISPOSABLE FIXTURE)")
        return 0
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
