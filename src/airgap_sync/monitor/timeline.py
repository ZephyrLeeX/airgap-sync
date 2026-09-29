"""Read-only Run timeline assembled from independently available metadata stores."""

from __future__ import annotations

import base64
import json
import sqlite3
from datetime import UTC, datetime

from airgap_sync.destination.mysql import METADATA_SCHEMA_VERSION, DestinationMySQLConnection
from airgap_sync.monitor import store

EVENTS = (
    ("source_created", "Source Run created", "created_at", "source"),
    ("snapshot_completed", "Snapshot completed", "snapshot_completed_at", "source"),
    ("source_delivered", "Source delivered", "delivered_at", "source"),
    (
        "manifest_created",
        "Source manifest created (Destination metadata)",
        "source_created_at",
        "destination",
    ),
    ("manifest_recorded", "Manifest receipt recorded", "manifest_received_at", "destination"),
    ("validated", "Validation recorded", "validated_at", "destination"),
    ("import_started", "Import started", "import_started_at", "destination"),
    ("import_completed", "Import completed", "import_completed_at", "destination"),
    ("digest_verified", "Digest verification completed", "digest_verified_at", "destination"),
    ("applied", "Promotion applied", "applied_at", "destination"),
)
INTERVALS = (
    (
        "snapshot",
        "Source snapshot incl. scan/compression",
        "source_created",
        "snapshot_completed",
        "source",
    ),
    (
        "source_delivery",
        "Source delivery incl. waiting/retry",
        "snapshot_completed",
        "source_delivered",
        "source",
    ),
    (
        "transport_wait",
        "Delivery to manifest record",
        "source_delivered",
        "manifest_recorded",
        "cross",
    ),
    (
        "import",
        "Destination import incl. waiting/retry",
        "import_started",
        "import_completed",
        "destination",
    ),
    (
        "verification_window",
        "Import completion to digest verification incl. waiting",
        "import_completed",
        "digest_verified",
        "destination",
    ),
    ("end_to_end", "Run creation to promotion", "source_created", "applied", "cross"),
)


def _iso(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        return value.isoformat().replace("+00:00", "Z")
    return value


def _seconds(start, end):
    if not start or not end:
        return None
    first = datetime.fromisoformat(start.replace("Z", "+00:00"))
    last = datetime.fromisoformat(end.replace("Z", "+00:00"))
    return (last - first).total_seconds()


def assemble(source: dict | None, destination, *, association="unique") -> dict:
    facts = source["facts"] if source else {}
    dest = destination
    dates = {}
    for code, _label, field, side in EVENTS:
        value = facts.get(field) if side == "source" else getattr(dest, field, None)
        dates[code] = _iso(value)
    timeline = [
        {"code": code, "label": label, "at": dates[code], "origin": side}
        for code, label, _, side in EVENTS
    ]
    intervals = []
    anomalies = []
    for code, label, start, end, clock in INTERVALS:
        seconds = _seconds(dates[start], dates[end])
        state = (
            "not_recorded"
            if seconds is None
            else ("clock_or_order_anomaly" if seconds < 0 else "recorded")
        )
        if state == "clock_or_order_anomaly":
            anomalies.append(code)
        intervals.append(
            {
                "code": code,
                "label": label,
                "start": start,
                "end": end,
                "seconds": seconds,
                "state": state,
                "clock": clock,
                "includes_wait_retry": code in ("source_delivery", "import", "verification_window"),
            }
        )
    durations = {
        item["code"]: item["seconds"] if item["state"] == "recorded" else None for item in intervals
    }
    rows = facts.get("row_count") if source else None
    raw = facts.get("raw_bytes") if source else None
    compressed = facts.get("compressed_bytes") if source else None

    def rate(count, duration):
        return (
            count / duration
            if count is not None and duration is not None and duration > 0
            else None
        )

    metrics = {
        "source_rows": rows,
        "source_chunks": facts.get("chunk_count") if source else None,
        "destination_expected_rows": dest.expected_rows if dest else None,
        "destination_actual_rows": dest.actual_rows if dest else None,
        "destination_chunks": dest.chunk_count if dest else None,
        "raw_bytes": raw,
        "compressed_bytes": compressed,
        "compressed_over_raw": compressed / raw
        if raw is not None and raw > 0 and compressed is not None
        else None,
        "snapshot_rows_per_second": rate(rows, durations["snapshot"]),
        "snapshot_raw_mib_per_second": rate(raw, durations["snapshot"]) / 1048576
        if rate(raw, durations["snapshot"]) is not None
        else None,
        "destination_rows_per_second": rate(dest.expected_rows, durations["import"])
        if dest
        else None,
    }
    missing = [item["code"] for item in timeline if item["at"] is None]
    ongoing = []
    source_status = facts.get("status") if source else None
    if source_status in ("GENERATING", "UPLOADING", "FINALIZING", "SNAPSHOT_READY"):
        start = "snapshot_completed" if dates["snapshot_completed"] else "source_created"
        ongoing.append(
            {
                "side": "source",
                "status": source_status,
                "start": start,
                "elapsed_seconds": _seconds(
                    dates[start], _iso(datetime.fromtimestamp(source["last_capture"], UTC))
                ),
                "as_of": _iso(datetime.fromtimestamp(source["last_capture"], UTC)),
                "estimated": True,
            }
        )
    if dest and dest.status in ("VALIDATED", "IMPORTING", "STAGED", "VERIFYING", "SWAPPING"):
        start = "import_started" if dates["import_started"] else "manifest_recorded"
        ongoing.append(
            {
                "side": "destination",
                "status": dest.status,
                "start": start,
                "elapsed_seconds": _seconds(dates[start], _iso(datetime.now(UTC))),
                "as_of": _iso(datetime.now(UTC)),
                "estimated": True,
            }
        )
    if source:
        anomalies.extend(source["conflicts"])
    if dest and source:
        if dest.expected_rows != rows and rows is not None:
            anomalies.append("row_count_mismatch")
        if dest.chunk_count != facts.get("chunk_count") and facts.get("chunk_count") is not None:
            anomalies.append("chunk_count_mismatch")
    return {
        "run_id": facts.get("run_id") if source else dest.run_id,
        "node_id": source["node_id"] if source else None,
        "source_database": source["source_database"] if source else dest.source_database,
        "table_name": facts.get("table_name") if source else dest.table_name,
        "source_status": source_status,
        "destination_status": dest.status if dest else None,
        "association": association if source and dest else "single_side",
        "timeline": timeline,
        "intervals": intervals,
        "duration_seconds": durations,
        "metrics": metrics,
        "missing": missing,
        "ongoing": ongoing,
        "anomalies": sorted(set(anomalies)),
        "source_last_observed_at": _iso(datetime.fromtimestamp(source["last_capture"], UTC))
        if source
        else None,
        "source_last_received_at": _iso(datetime.fromtimestamp(source["last_received"], UTC))
        if source and source.get("last_received") is not None
        else None,
        "destination_updated_at": _iso(dest.updated_at) if dest else None,
    }


def _source_rows(
    cfg, *, after=("", "", "", ""), limit=50, node=None, database=None, table=None, run_id=None
):
    if cfg is None:
        return "DISABLED", []
    try:
        with store.connect(cfg.db_path) as db:
            where = ["(run_id,node_id,source_database,table_name)>(?,?,?,?)"]
            args = list(after)
            for column, value in (
                ("node_id", node),
                ("source_database", database),
                ("table_name", table),
                ("run_id", run_id),
            ):
                if value is not None:
                    where.append(f"{column}=?")
                    args.append(value)
            rows = db.execute(
                "SELECT * FROM source_runs WHERE "
                + " AND ".join(where)
                + " ORDER BY run_id,node_id,source_database,table_name LIMIT ?",
                (*args, limit),
            ).fetchall()
            return "OK", [
                {
                    **dict(row),
                    "facts": json.loads(row["facts"]),
                    "conflicts": json.loads(row["conflicts"]),
                }
                for row in rows
            ]
    except (OSError, sqlite3.Error, ValueError, TypeError):
        return "UNAVAILABLE", []


def _destination_rows(
    config, *, after=("", "", ""), limit=50, database=None, table=None, run_id=None
):
    try:
        with DestinationMySQLConnection(config.mysql, config.destination, monitor_timeout=3) as db:
            if db.metadata_schema_version() != METADATA_SCHEMA_VERSION:
                return "UNAVAILABLE", []
            rows = db.monitoring_run_page(
                after=after, limit=limit, source_database=database, table_name=table, run_id=run_id
            )
            return "OK", rows
    except Exception:
        return "UNAVAILABLE", []


def _exact_destinations(config, sources, known=()):
    """Look up only identities in the bounded Source page."""
    result = {(dest.run_id, dest.source_database, dest.table_name): dest for dest in known}
    missing = [
        source
        for source in sources
        if (source["run_id"], source["source_database"], source["table_name"]) not in result
    ]
    if not missing:
        return "OK", result
    try:
        with DestinationMySQLConnection(config.mysql, config.destination, monitor_timeout=3) as db:
            if db.metadata_schema_version() != METADATA_SCHEMA_VERSION:
                return "UNAVAILABLE", {}
            keys = {
                (source["run_id"], source["source_database"], source["table_name"])
                for source in missing
            }
            result.update(dict.fromkeys(keys))
            for dest in db.monitoring_runs_for_identities(list(keys)):
                result[(dest.run_id, dest.source_database, dest.table_name)] = dest
            return "OK", result
    except Exception:
        return "UNAVAILABLE", {}


def _exact_sources(cfg, destinations):
    if cfg is None:
        return {}
    try:
        with store.connect(cfg.db_path) as db:
            result = {}
            for dest in destinations:
                rows = db.execute(
                    "SELECT * FROM source_runs WHERE source_database=? AND table_name=? "
                    "AND run_id=? LIMIT 2",
                    (dest.source_database, dest.table_name, dest.run_id),
                ).fetchall()
                result[(dest.source_database, dest.table_name, dest.run_id)] = [
                    {
                        **dict(row),
                        "facts": json.loads(row["facts"]),
                        "conflicts": json.loads(row["conflicts"]),
                    }
                    for row in rows
                ]
            return result
    except (OSError, sqlite3.Error, ValueError, TypeError):
        return {}


def _cursor_tuple(value):
    if not value:
        return ("", "", "", "")
    try:
        data = json.loads(base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)))
        if (
            not isinstance(data, list)
            or len(data) != 4
            or any(not isinstance(part, str) or len(part) > 512 for part in data)
        ):
            raise ValueError("cursor")
        return tuple(data)
    except (ValueError, UnicodeError, TypeError) as exc:
        raise ValueError("invalid cursor") from exc


def _item_key(item):
    return (item["run_id"], item["node_id"] or "", item["source_database"], item["table_name"])


def _destination_after(after):
    # Destination has no node. Once a node key is passed, every Destination
    # identity under that run_id is already behind the cursor.
    return (after[0], None, None) if after[1] else (after[0], after[2], after[3])


def read_runs(config, *, before="", limit=50, node=None, database=None, table=None, run_id=None):
    """Merge bounded keysets; exact identity lookups keep page joins complete."""
    after = _cursor_tuple(before)
    source_status, sources = _source_rows(
        config.monitor_ingest,
        after=after,
        limit=limit + 1,
        node=node,
        database=database,
        table=table,
        run_id=run_id,
    )
    if node is None:
        dest_status, destinations = _destination_rows(
            config,
            after=_destination_after(after),
            limit=limit + 1,
            database=database,
            table=table,
            run_id=run_id,
        )
    else:
        # Destination has no node field. Only Source identities can select it.
        dest_status, destinations = "OK", []
    match_status, source_destinations = _exact_destinations(config, sources, destinations)
    if match_status != "OK":
        dest_status = "UNAVAILABLE"
    destination_sources = _exact_sources(config.monitor_ingest, destinations)
    source_matches = _exact_sources(
        config.monitor_ingest, [dest for dest in source_destinations.values() if dest is not None]
    )
    entries = []
    for dest in destinations:
        key = (dest.run_id, "", dest.source_database, dest.table_name)
        matches = destination_sources.get((dest.source_database, dest.table_name, dest.run_id), [])
        item = assemble(matches[0], dest) if len(matches) == 1 else assemble(None, dest)
        if len(matches) > 1:
            item["association"] = "ambiguous"
        elif not matches:
            item["association"] = "unresolved"
        entries.append((key, item))
    for source in sources:
        identity = (source["run_id"], source["source_database"], source["table_name"])
        dest = source_destinations.get(identity)
        matches = source_matches.get((identity[1], identity[2], identity[0]), []) if dest else []
        if node is None and dest and len(matches) == 1:
            # The joined item lives at its Destination key, on this or a later page.
            continue
        if node is not None and dest and len(matches) == 1:
            item = assemble(source, dest)
        else:
            item = assemble(source, None)
            item["association"] = "ambiguous" if dest else "unresolved"
        entries.append((_item_key(item), item))
    entries = sorted((key, item) for key, item in entries if key > after)
    frontier = None
    if len(sources) == limit + 1:
        last = sources[-1]
        frontier = (last["run_id"], last["node_id"], last["source_database"], last["table_name"])
    if len(destinations) == limit + 1:
        last = destinations[-1]
        dest_frontier = (last.run_id, "", last.source_database, last.table_name)
        frontier = min(frontier, dest_frontier) if frontier else dest_frontier
    if frontier is not None:
        entries = [(key, item) for key, item in entries if key <= frontier]
    more = len(entries) > limit or frontier is not None
    if len(entries) > limit:
        boundary = entries[limit - 1][0]
    elif frontier is not None:
        boundary = frontier
    elif entries:
        boundary = entries[-1][0]
    else:
        boundary = after
    next_cursor = (
        base64.urlsafe_b64encode(json.dumps(boundary).encode()).decode().rstrip("=")
        if more and boundary > after
        else None
    )
    return {
        "source_status": source_status,
        "destination_status": dest_status,
        "items": [item for _, item in entries[:limit]],
        "next_cursor": next_cursor,
    }
