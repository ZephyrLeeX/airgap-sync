"""Monitor-owned, best-effort alert evaluation. All times are UTC epoch seconds."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import stat
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from airgap_sync import __version__
from airgap_sync.common.manifest import Manifest
from airgap_sync.common.models import AppConfig
from airgap_sync.common.transport import parse_transport_filename, transport_filename
from airgap_sync.destination.mysql import METADATA_SCHEMA_VERSION, DestinationMySQLConnection
from airgap_sync.monitor import store
from airgap_sync.monitor.system import worker_status


@dataclass(frozen=True)
class Decision:
    kind: str
    object_id: str
    state: str  # TRUE / FALSE / UNKNOWN
    severity: str = "WARNING"
    message: str = ""
    observed_at: float | None = None
    evidence_key: str | None = None
    node_id: str | None = None
    table_name: str | None = None
    run_id: str | None = None
    volume: str | None = None
    details: dict | None = None

    @property
    def fingerprint(self):
        return json.dumps([self.kind, self.object_id], separators=(",", ":"))


def _epoch(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.timestamp()


def disk_decision(
    kind, object_id, free_bytes, free_percent, cfg, observed_at, *, details=None, **identity
):
    if free_bytes is None and free_percent is None:
        state, severity = "UNKNOWN", "WARNING"
    else:
        severity = "WARNING"
        state = "FALSE"
        for level, threshold in (
            ("EMERGENCY", cfg.disk_emergency),
            ("CRITICAL", cfg.disk_critical),
            ("WARNING", cfg.disk_warning),
        ):
            if (free_bytes is not None and free_bytes < threshold.free_bytes) or (
                free_percent is not None and free_percent < threshold.free_percent
            ):
                state, severity = "TRUE", level
                break
        if state == "FALSE" and (free_bytes is None or free_percent is None):
            state = "UNKNOWN"
    return Decision(
        kind,
        object_id,
        state,
        severity,
        "Local disk free space below threshold" if state == "TRUE" else "",
        observed_at,
        **identity,
        details={"free_bytes": free_bytes, "free_percent": free_percent, **(details or {})},
    )


def apply(db, decision: Decision, now):
    """One logical OPEN per fingerprint; recovered recurrences get new IDs."""
    details = json.dumps(decision.details or {}, ensure_ascii=True, sort_keys=True)
    if len(details) > 2048:
        details = '{"truncated":true}'
    with db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            "SELECT id,observed_at,evidence_key FROM alerts WHERE fingerprint=? AND status='OPEN'",
            (decision.fingerprint,),
        ).fetchone()
        if decision.state == "UNKNOWN":
            if row:
                db.execute(
                    "UPDATE alerts SET evaluated_at=?,evaluation_state='UNKNOWN' WHERE id=?",
                    (now, row["id"]),
                )
            return
        if decision.state == "TRUE":
            if row is None and decision.observed_at is not None:
                previous = db.execute(
                    """SELECT observed_at,status,evidence_key,recovered_at FROM alerts
                    WHERE fingerprint=? ORDER BY id DESC LIMIT 1""",
                    (decision.fingerprint,),
                ).fetchone()
                if (
                    previous
                    and previous["status"] == "RECOVERED"
                    and previous["observed_at"] is not None
                    and decision.observed_at <= previous["observed_at"]
                    and not (
                        decision.kind in ("SOURCE_HEARTBEAT", "FRESHNESS")
                        and decision.observed_at == previous["observed_at"]
                        and decision.evidence_key == previous["evidence_key"]
                        and now > previous["recovered_at"]
                    )
                ):
                    return
            # A late, duplicate observation cannot reverse an event established by newer evidence.
            if (
                row
                and decision.observed_at is not None
                and row["observed_at"] is not None
                and decision.observed_at < row["observed_at"]
            ):
                return
            if row:
                db.execute(
                    """UPDATE alerts SET severity=?,message=?,details_json=?,last_seen_at=?,
                    evaluated_at=?,observed_at=?,evaluation_state='TRUE',
                    evidence_key=? WHERE id=?""",
                    (
                        decision.severity,
                        decision.message[:256],
                        details,
                        now,
                        now,
                        decision.observed_at,
                        decision.evidence_key,
                        row["id"],
                    ),
                )
            else:
                db.execute(
                    """INSERT INTO alerts (fingerprint,alert_type,object_id,node_id,table_name,
                    run_id,volume,severity,status,message,details_json,opened_at,last_seen_at,
                    recovered_at,evaluated_at,observed_at,evaluation_state,evidence_key)
                    VALUES (?,?,?,?,?,?,?,?, 'OPEN',?,?,?,?,NULL,?,?, 'TRUE',?)""",
                    (
                        decision.fingerprint,
                        decision.kind,
                        decision.object_id,
                        decision.node_id,
                        decision.table_name,
                        decision.run_id,
                        decision.volume,
                        decision.severity,
                        decision.message[:256],
                        details,
                        now,
                        now,
                        now,
                        decision.observed_at,
                        decision.evidence_key,
                    ),
                )
        elif row:
            if (
                decision.kind != "INCOMING_STALE"
                and decision.observed_at is not None
                and row["observed_at"] is not None
                and decision.observed_at <= row["observed_at"]
                and decision.evidence_key != row["evidence_key"]
            ):
                return
            db.execute(
                """UPDATE alerts SET status='RECOVERED',recovered_at=?,evaluated_at=?,
                evaluation_state='FALSE',observed_at=?,evidence_key=? WHERE id=?""",
                (now, now, decision.observed_at, decision.evidence_key, row["id"]),
            )


def _source_decisions(db, cfg, now):
    cursor_row = db.execute("SELECT value FROM alert_state WHERE key='source-cursor'").fetchone()
    cursor = cursor_row["value"] if cursor_row else ""
    rows = db.execute(
        """SELECT n.node_id,n.first_received,s.captured,s.received,s.hash,s.payload
        FROM nodes n JOIN node_samples s ON s.hash=n.latest_hash
        WHERE n.node_id>? ORDER BY n.node_id LIMIT 1000""",
        (cursor,),
    ).fetchall()
    if not rows and cursor:
        rows = db.execute(
            """SELECT n.node_id,n.first_received,s.captured,s.received,s.hash,s.payload
            FROM nodes n JOIN node_samples s ON s.hash=n.latest_hash
            ORDER BY n.node_id LIMIT 1000"""
        ).fetchall()
    if rows:
        with db:
            db.execute(
                "INSERT OR REPLACE INTO alert_state VALUES ('source-cursor',?,?)",
                (rows[-1]["node_id"], now),
            )
    for row in rows:
        node = row["node_id"]
        payload = json.loads(row["payload"])
        age = now - row["captured"]
        # Small future skew is allowed by M3; it does not prove an even newer heartbeat.
        heartbeat = (
            "CRITICAL"
            if age > cfg.heartbeat_critical_seconds
            else "WARNING"
            if age > cfg.heartbeat_warning_seconds
            else None
        )
        yield Decision(
            "SOURCE_HEARTBEAT",
            node,
            "TRUE" if heartbeat else "FALSE",
            heartbeat or "WARNING",
            "Source heartbeat missing or overdue" if heartbeat else "",
            row["captured"],
            row["hash"],
            node_id=node,
            details={"age_seconds": round(age), "received_at": row["received"]},
        )
        fresh = -cfg.future_seconds <= age <= cfg.heartbeat_warning_seconds
        worker = payload["worker"]["status"] if fresh else "UNKNOWN"
        yield Decision(
            "SOURCE_WORKER",
            node,
            "TRUE" if worker == "STOPPED" else "FALSE" if worker == "RUNNING" else "UNKNOWN",
            "CRITICAL",
            "Source worker process stopped" if worker == "STOPPED" else "",
            row["captured"],
            row["hash"],
            node_id=node,
            details={"worker_status": worker, "task_status": payload["worker"]["task_status"]},
        )
        seen_volumes = set()
        if fresh:
            for fs in payload["filesystems"]:
                volume = fs["mount"]
                if volume in seen_volumes:
                    continue
                seen_volumes.add(volume)
                yield disk_decision(
                    "SOURCE_DISK",
                    json.dumps([node, volume]),
                    fs["free_bytes"],
                    fs["free_percent"],
                    cfg,
                    row["captured"],
                    node_id=node,
                    volume=volume,
                    evidence_key=row["hash"],
                )
        # Missing disks and stale payloads update availability without recovering the event.
        opened_disks = db.execute(
            """SELECT object_id,volume FROM alerts WHERE status='OPEN'
            AND alert_type='SOURCE_DISK' AND node_id=? LIMIT 100""",
            (node,),
        ).fetchall()
        for disk in opened_disks:
            if not fresh or disk["volume"] not in seen_volumes:
                yield Decision(
                    "SOURCE_DISK",
                    disk["object_id"],
                    "UNKNOWN",
                    node_id=node,
                    volume=disk["volume"],
                )
        version = payload["agent_version"]
        comparable = bool(version and __version__ and "+" not in version and "+" not in __version__)
        yield Decision(
            "VERSION",
            node,
            "UNKNOWN"
            if not fresh or not comparable
            else "TRUE"
            if version != __version__
            else "FALSE",
            "WARNING",
            "Known Source and Destination agent versions differ",
            row["captured"],
            row["hash"],
            node_id=node,
            details={"source_version": version, "destination_version": __version__},
        )
        runs_by_table = {}
        for run in (payload.get("last_run"), payload.get("current_run")):
            if run is None:
                continue
            table = run["table"]
            previous = runs_by_table.get(table)
            if previous is None or (run["created_at"] or "", run["run_id"]) > (
                previous["created_at"] or "",
                previous["run_id"],
            ):
                runs_by_table[table] = run
        for name, item in (
            ("cycle", payload.get("cycle")),
            *(("run", item) for item in runs_by_table.values()),
        ):
            if item is None:
                continue
            key = "cycle_id" if name == "cycle" else "run_id"
            identity = item[key]
            status = item["status"] if fresh else "UNKNOWN"
            abnormal = status in ("FAILED", "MISMATCH", "DISK_PRESSURE", "RETRY_WAIT")
            clear = status in ("COMPLETED", "DELIVERED", "VERIFIED")
            yield Decision(
                "SOURCE_CYCLE" if name == "cycle" else "SOURCE_RUN",
                json.dumps([node, identity])
                if name == "cycle"
                else json.dumps([node, item["table"]]),
                "TRUE" if abnormal else "FALSE" if clear else "UNKNOWN",
                "WARNING" if status == "RETRY_WAIT" else "CRITICAL",
                f"Source {name} reports {status}" if abnormal else "",
                row["captured"],
                row["hash"],
                node_id=node,
                run_id=identity if name != "cycle" else None,
                details={"status": status},
            )
    for node in cfg.expected_sources:
        if db.execute("SELECT 1 FROM nodes WHERE node_id=?", (node,)).fetchone():
            continue
        key = f"expected-source:{node}"
        with db:
            db.execute("INSERT OR IGNORE INTO alert_state VALUES (?,?,?)", (key, str(now), now))
        since = float(db.execute("SELECT value FROM alert_state WHERE key=?", (key,)).fetchone()[0])
        age = now - since
        severity = "CRITICAL" if age > cfg.heartbeat_critical_seconds else "WARNING"
        yield Decision(
            "SOURCE_HEARTBEAT",
            node,
            "TRUE" if age > cfg.first_heartbeat_grace_seconds else "UNKNOWN",
            severity,
            "Expected Source heartbeat has not arrived",
            None,
            node_id=node,
            details={"expected_since": since, "age_seconds": round(age)},
        )


def _destination_decisions(config, cfg, now):
    worker = worker_status()["status"]
    yield Decision(
        "DESTINATION_WORKER",
        "destination",
        "TRUE" if worker == "NOT RUNNING" else "FALSE" if worker == "RUNNING" else "UNKNOWN",
        "CRITICAL",
        "Destination worker service not running",
        now,
        details={"status": worker},
    )
    seen = set()
    for path in (Path("/"), config.destination.incoming_dir, Path(__file__).resolve()):
        try:
            volume = str(os.stat(path).st_dev)
            if volume in seen:
                continue
            seen.add(volume)
            usage = shutil.disk_usage(path)
        except OSError:
            continue
        total, free = usage.total, usage.free
        percent = 100 * free / total if total and free is not None else None
        yield disk_decision(
            "DESTINATION_DISK",
            volume,
            free,
            percent,
            cfg,
            now,
            volume=volume,
            details={"path": str(path)},
        )


def _rds_decisions(config, cfg, now, db=None):
    conn = DestinationMySQLConnection(config.mysql, config.destination, monitor_timeout=3)
    try:
        with conn:
            conn.ping()
            if conn.metadata_schema_version() != METADATA_SCHEMA_VERSION:
                raise ValueError("metadata schema unavailable")
            # Bounded latest-version metadata read; uncovered tables stay unknown.
            version_cursor = _cursor(db, "version-cursor") if db is not None else None
            versions = conn.monitoring_latest_versions(1000, version_cursor)
            if not versions and version_cursor:
                versions = conn.monitoring_latest_versions(1000)
            _save_cursor(db, "version-cursor", versions, now)
            latest = {(v.source_database, v.table_name): v for v in versions}
            expected = {
                (config.mysql.database, table.name) for table in config.enabled_tables[:1000]
            }
            for key in sorted(set(latest) | expected):
                version = latest.get(key)
                if version is None:
                    yield Decision("FRESHNESS", json.dumps(key), "UNKNOWN", table_name=key[1])
                    continue
                source_time = _epoch(version.source_created_at)
                age = now - source_time
                severity = "CRITICAL" if age > cfg.freshness_critical_seconds else "WARNING"
                yield Decision(
                    "FRESHNESS",
                    json.dumps(key),
                    "TRUE" if age > cfg.freshness_warning_seconds else "FALSE",
                    severity,
                    "Latest VERIFIED snapshot is overdue",
                    _epoch(version.verified_at),
                    version.run_id,
                    table_name=key[1],
                    run_id=version.run_id,
                    details={"source_created_at": source_time, "age_seconds": round(age)},
                )
            # Latest run per table is complete for current failures. Historical failure counts
            # remain on Problems, not in this current-state rule.
            run_cursor = _cursor(db, "run-cursor") if db is not None else None
            runs = conn.monitoring_latest_runs(1000, run_cursor)
            if not runs and run_cursor:
                runs = conn.monitoring_latest_runs(1000)
            _save_cursor(db, "run-cursor", runs, now)
            for run in runs:
                table_key = (run.source_database, run.table_name)
                if table_key not in latest:
                    yield Decision(
                        "FRESHNESS",
                        json.dumps(table_key),
                        "UNKNOWN",
                        table_name=run.table_name,
                        details={"reason": "No VERIFIED version observed"},
                    )
                status = run.status
                abnormal = status in ("FAILED", "MISMATCH", "DISK_PRESSURE", "RETRY_WAIT")
                yield Decision(
                    "DESTINATION_RUN",
                    json.dumps([run.source_database, run.table_name]),
                    "TRUE" if abnormal else "FALSE" if status == "VERIFIED" else "UNKNOWN",
                    "WARNING" if status == "RETRY_WAIT" else "CRITICAL",
                    f"Latest Destination run is {status}" if abnormal else "",
                    _epoch(run.updated_at),
                    run.run_id,
                    table_name=run.table_name,
                    run_id=run.run_id,
                    details={"status": status, "source_database": run.source_database},
                )
            cursor_row = db.execute(
                "SELECT value FROM alert_state WHERE key='incoming-recovery-cursor'"
            ).fetchone()
            cursor = int(cursor_row["value"]) if cursor_row else 0
            pending = db.execute(
                """SELECT id,run_id FROM alerts WHERE alert_type='INCOMING_STALE'
                AND status='OPEN' AND id>? ORDER BY id LIMIT 100""",
                (cursor,),
            ).fetchall()
            if not pending and cursor:
                pending = db.execute(
                    """SELECT id,run_id FROM alerts WHERE alert_type='INCOMING_STALE'
                    AND status='OPEN' ORDER BY id LIMIT 100"""
                ).fetchall()
            known = conn.monitoring_known_run_ids([row["run_id"] for row in pending])
            if pending:
                with db:
                    db.execute(
                        """INSERT OR REPLACE INTO alert_state
                        VALUES ('incoming-recovery-cursor',?,?)""",
                        (str(pending[-1]["id"]), now),
                    )
            for row in pending:
                if row["run_id"] in known:
                    yield Decision(
                        "INCOMING_STALE",
                        row["run_id"],
                        "FALSE",
                        observed_at=_epoch(known[row["run_id"]]),
                        evidence_key=row["run_id"],
                        run_id=row["run_id"],
                    )
    finally:
        conn.close()


def _cursor(db, key):
    row = db.execute("SELECT value FROM alert_state WHERE key=?", (key,)).fetchone()
    return tuple(json.loads(row["value"])) if row else None


def _save_cursor(db, key, rows, now):
    if db is not None and rows:
        last = rows[-1]
        with db:
            db.execute(
                "INSERT OR REPLACE INTO alert_state VALUES (?,?,?)",
                (key, json.dumps([last.source_database, last.table_name]), now),
            )


class IncomingScanner:
    """Persistent bounded directory cursor; no business file mutations."""

    def __init__(self, directory: Path):
        self.directory = directory
        self.iterator = None

    def close(self):
        if self.iterator is not None:
            self.iterator.close()
            self.iterator = None

    def names(self, limit=2000, candidates=20):
        if self.iterator is None:
            self.iterator = os.scandir(self.directory)
        yielded = 0
        for _ in range(limit):
            entry = next(self.iterator, None)
            if entry is None:
                self.close()
                break
            try:
                run_id, logical = parse_transport_filename(entry.name)
            except ValueError:
                continue
            if logical == "manifest.json" and entry.is_file(follow_symlinks=False):
                yield run_id
                yielded += 1
                if yielded >= candidates:
                    break


_MANIFEST_LIMIT = 1024 * 1024


def _manifest_bytes(path):
    """Read one stable regular file through its descriptor with a hard byte bound."""
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_size > _MANIFEST_LIMIT:
        raise ValueError("manifest is not a bounded regular file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    fd = os.open(path, flags)
    try:
        opened = os.fstat(fd)
        if not stat.S_ISREG(opened.st_mode) or _file_identity(opened) != _file_identity(before):
            raise ValueError("manifest changed before reading")
        chunks = []
        remaining = _MANIFEST_LIMIT + 1
        while remaining:
            chunk = os.read(fd, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        if len(data) > _MANIFEST_LIMIT:
            raise ValueError("manifest exceeds size limit")
        if len(data) != opened.st_size or _file_identity(os.fstat(fd)) != _file_identity(opened):
            raise ValueError("manifest changed while reading")
        if _file_identity(path.lstat()) != _file_identity(opened):
            raise ValueError("manifest was replaced while reading")
        return data
    finally:
        os.close(fd)


def _file_identity(info):
    return (
        info.st_dev,
        info.st_ino,
        info.st_mode,
        info.st_size,
        info.st_mtime_ns,
        info.st_ctime_ns,
    )


def _incoming_decisions(config, db, cfg, now, scanner):
    directory = config.destination.incoming_dir
    candidates = []
    for run_id in scanner.names():
        try:
            manifest_path = directory / transport_filename(run_id, "manifest.json")
            manifest = Manifest.model_validate_json(_manifest_bytes(manifest_path))
            if manifest.run_id != run_id or len(manifest.chunks) > 1000:
                continue
            paths = [
                manifest_path,
                directory / transport_filename(run_id, manifest.schema_file.file),
            ]
            paths.extend(
                directory / transport_filename(run_id, chunk.file) for chunk in manifest.chunks
            )
            expected = [None, None, *(chunk.compressed_bytes for chunk in manifest.chunks)]
            fingerprints = []
            complete = True
            for path, size in zip(paths, expected, strict=True):
                item = path.lstat()
                if not stat.S_ISREG(item.st_mode) or (size is not None and item.st_size != size):
                    complete = False
                    break
                fingerprints.append((item.st_ino, item.st_size, item.st_mtime_ns, item.st_ctime_ns))
            if not complete:
                continue
            mark = hashlib.sha256(json.dumps(fingerprints).encode()).hexdigest()
            key = f"incoming:{run_id}"
            old = db.execute(
                "SELECT value,updated_at FROM alert_state WHERE key=?", (key,)
            ).fetchone()
            if old is None or old["value"] != mark or old["updated_at"] > now:
                with db:
                    db.execute(
                        "INSERT OR REPLACE INTO alert_state VALUES (?,?,?)", (key, mark, now)
                    )
                continue
            since = old["updated_at"]
            if now - since >= cfg.incoming_settle_seconds:
                candidates.append(
                    Decision(
                        "INCOMING_STALE",
                        run_id,
                        "TRUE" if now - since > cfg.incoming_stale_seconds else "UNKNOWN",
                        "WARNING",
                        "Complete incoming run has not started processing",
                        since,
                        mark,
                        run_id=run_id,
                        details={"first_complete_at": since, "age_seconds": round(now - since)},
                    )
                )
        except (OSError, ValueError, TypeError):
            # Incomplete, changing or unreadable sets provide no recovery evidence.
            continue
    if not candidates:
        return
    connection = DestinationMySQLConnection(config.mysql, config.destination, monitor_timeout=3)
    try:
        with connection:
            connection.ping()
            if connection.metadata_schema_version() != METADATA_SCHEMA_VERSION:
                raise ValueError("metadata schema unavailable")
            known = connection.monitoring_known_run_ids([item.run_id for item in candidates])
    except Exception:
        known = None
    finally:
        connection.close()
    for item in candidates:
        if known is None:
            yield Decision("INCOMING_STALE", item.run_id, "UNKNOWN", run_id=item.run_id)
        elif item.run_id in known:
            yield Decision(
                "INCOMING_STALE",
                item.run_id,
                "FALSE",
                observed_at=_epoch(known[item.run_id]),
                evidence_key=item.run_id,
                run_id=item.run_id,
            )
        else:
            yield item


def evaluate(config: AppConfig, db, now=None, incoming_scanner=None):
    cfg = config.monitor_alerts
    if cfg is None:
        return
    now = time.time() if now is None else now
    signature = hashlib.sha256(json.dumps(cfg.model_dump(), sort_keys=True).encode()).hexdigest()
    with db:
        db.execute("BEGIN IMMEDIATE")
        prior = db.execute("SELECT value FROM alert_state WHERE key='rule-config'").fetchone()
        if prior and prior[0] != signature:
            db.execute(
                """UPDATE alerts SET status='DISABLED',recovered_at=?,evaluated_at=?,
                evaluation_state='UNKNOWN' WHERE status='OPEN'""",
                (now, now),
            )
        db.execute(
            "INSERT OR REPLACE INTO alert_state VALUES ('rule-config',?,?)", (signature, now)
        )
    # Each source is isolated; one failure does not block the other families.
    failures = 0
    for families, source in (
        (
            (
                "SOURCE_HEARTBEAT",
                "SOURCE_WORKER",
                "SOURCE_DISK",
                "SOURCE_RUN",
                "SOURCE_CYCLE",
                "VERSION",
            ),
            lambda: _source_decisions(db, cfg, now),
        ),
        (
            ("DESTINATION_WORKER", "DESTINATION_DISK"),
            lambda: _destination_decisions(config, cfg, now),
        ),
        (("FRESHNESS", "DESTINATION_RUN"), lambda: _rds_decisions(config, cfg, now, db)),
        (
            ("INCOMING_STALE",),
            lambda: (
                _incoming_decisions(config, db, cfg, now, incoming_scanner)
                if incoming_scanner is not None
                else ()
            ),
        ),
    ):
        try:
            visited = set()
            for decision in source():
                apply(db, decision, now)
                visited.add(decision.fingerprint)
            if families != ("INCOMING_STALE",):
                opened = db.execute(
                    """SELECT id,fingerprint FROM alerts WHERE status='OPEN'
                    AND alert_type IN ("""
                    + ",".join("?" for _ in families)
                    + ") ORDER BY id DESC LIMIT 2000",
                    families,
                ).fetchall()
                with db:
                    db.executemany(
                        """UPDATE alerts SET evaluated_at=?,evaluation_state='UNKNOWN'
                        WHERE id=?""",
                        [(now, row["id"]) for row in opened if row["fingerprint"] not in visited],
                    )
        except Exception:
            failures += 1
            with db:
                db.execute(
                    """UPDATE alerts SET evaluated_at=?,evaluation_state='UNKNOWN'
                    WHERE status='OPEN' AND alert_type IN ("""
                    + ",".join("?" for _ in families)
                    + ")",
                    (now, *families),
                )
                if "FRESHNESS" in families:
                    db.execute(
                        """UPDATE alerts SET evaluated_at=?,evaluation_state='UNKNOWN'
                        WHERE status='OPEN' AND alert_type='INCOMING_STALE'""",
                        (now,),
                    )
    with db:
        db.execute(
            """DELETE FROM alerts WHERE id IN (SELECT id FROM alerts
            WHERE status IN ('RECOVERED','DISABLED') AND recovered_at < ?
            ORDER BY recovered_at LIMIT 100)""",
            (now - cfg.recovered_retention_days * 86400,),
        )
        # Auxiliary state is bounded independently; active issues and configured
        # never-seen nodes keep their original first-observed time.
        db.execute(
            """DELETE FROM alert_state WHERE key IN (
            SELECT s.key FROM alert_state s WHERE s.key LIKE 'incoming:%'
            AND s.updated_at < ? AND NOT EXISTS (
              SELECT 1 FROM alerts a WHERE a.status='OPEN'
              AND a.alert_type='INCOMING_STALE' AND a.run_id=substr(s.key,10))
            ORDER BY s.updated_at LIMIT 100)""",
            (now - cfg.recovered_retention_days * 86400,),
        )
        old_expected = db.execute(
            """SELECT key FROM alert_state WHERE key LIKE 'expected-source:%'
            AND updated_at < ? ORDER BY updated_at LIMIT 100""",
            (now - cfg.recovered_retention_days * 86400,),
        ).fetchall()
        db.executemany(
            "DELETE FROM alert_state WHERE key=?",
            [(row["key"],) for row in old_expected if row["key"][16:] not in cfg.expected_sources],
        )
    return failures


def disable_all(db, now):
    with db:
        db.execute("BEGIN IMMEDIATE")
        db.execute(
            """UPDATE alerts SET status='DISABLED',recovered_at=?,evaluated_at=?,
            evaluation_state='UNKNOWN' WHERE status='OPEN'""",
            (now, now),
        )


def read_alerts(
    ingest_cfg, *, status=None, severity=None, node=None, kind=None, limit=100, before=None
):
    if ingest_cfg is None:
        return {"status": "DISABLED", "alerts": [], "next_cursor": None}
    limit = max(1, min(limit, 200))
    clauses, values = [], []
    for column, value in (
        ("status", status),
        ("severity", severity),
        ("node_id", node),
        ("alert_type", kind),
    ):
        if value not in (None, ""):
            clauses.append(f"{column}=?")
            values.append(value)
    if before is not None:
        clauses.append("id<?")
        values.append(before)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    try:
        with store.connect(ingest_cfg.db_path) as db:
            rows = db.execute(
                "SELECT * FROM alerts" + where + " ORDER BY id DESC LIMIT ?", (*values, limit + 1)
            ).fetchall()
            alerts = []
            for row in rows[:limit]:
                item = dict(row)
                for field in (
                    "opened_at",
                    "last_seen_at",
                    "recovered_at",
                    "evaluated_at",
                    "observed_at",
                ):
                    item[field] = (
                        datetime.fromtimestamp(item[field], UTC).isoformat()
                        if item[field] is not None
                        else None
                    )
                item["details"] = json.loads(item.pop("details_json"))
                alerts.append(item)
            return {
                "status": "OK",
                "alerts": alerts,
                "next_cursor": rows[limit - 1]["id"] if len(rows) > limit else None,
            }
    except (OSError, sqlite3.Error, ValueError):
        return {"status": "UNAVAILABLE", "alerts": [], "next_cursor": None}


def summary(ingest_cfg):
    if ingest_cfg is None:
        return {"status": "DISABLED", "count": None, "highest": None, "important": []}
    try:
        with store.connect(ingest_cfg.db_path) as db:
            rows = db.execute(
                """SELECT id,severity,message,alert_type,object_id,evaluation_state
                FROM alerts WHERE status='OPEN' ORDER BY CASE severity
                WHEN 'EMERGENCY' THEN 0 WHEN 'CRITICAL' THEN 1 ELSE 2 END,id DESC LIMIT 5"""
            ).fetchall()
            count = db.execute("SELECT count(*) FROM alerts WHERE status='OPEN'").fetchone()[0]
            return {
                "status": "OK",
                "count": count,
                "highest": rows[0]["severity"] if rows else None,
                "important": [dict(row) for row in rows],
            }
    except (OSError, sqlite3.Error, ValueError):
        return {"status": "UNAVAILABLE", "count": None, "highest": None, "important": []}
