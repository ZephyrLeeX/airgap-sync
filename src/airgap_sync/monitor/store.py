"""Monitor-only SQLite storage. No business database dependencies."""

import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from airgap_sync.common.models import MonitorIngestConfig
from airgap_sync.monitor.protocol import VALIDATE, timestamp

SCHEMA = """
CREATE TABLE node_samples (
 hash TEXT PRIMARY KEY, node_id TEXT NOT NULL, captured REAL NOT NULL,
 received REAL NOT NULL, payload TEXT NOT NULL);
CREATE INDEX sample_history ON node_samples(node_id, captured DESC, hash DESC);
CREATE INDEX sample_retention ON node_samples(received, hash);
CREATE TABLE nodes (
 node_id TEXT PRIMARY KEY, first_received REAL NOT NULL,
 latest_hash TEXT REFERENCES node_samples(hash));
CREATE INDEX latest_samples ON nodes(latest_hash);
CREATE TABLE filesystem_samples (
 sample_hash TEXT NOT NULL REFERENCES node_samples(hash) ON DELETE CASCADE,
 ordinal INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(sample_hash, ordinal));
CREATE TABLE managed_storage_samples (
 sample_hash TEXT PRIMARY KEY REFERENCES node_samples(hash) ON DELETE CASCADE,
 captured REAL, payload TEXT NOT NULL);
CREATE TABLE content_receipts (hash TEXT PRIMARY KEY, received REAL NOT NULL);
CREATE INDEX content_expiry ON content_receipts(received);
CREATE TABLE transfer_receipts (
 identity TEXT PRIMARY KEY, hash TEXT NOT NULL, received REAL NOT NULL);
CREATE INDEX transfer_expiry ON transfer_receipts(received);
CREATE TABLE observations (identity TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
 since REAL NOT NULL, seen REAL NOT NULL);
CREATE INDEX observation_expiry ON observations(seen);
CREATE TABLE alerts (
 id INTEGER PRIMARY KEY, fingerprint TEXT NOT NULL, alert_type TEXT NOT NULL,
 object_id TEXT NOT NULL, node_id TEXT, table_name TEXT, run_id TEXT, volume TEXT,
 severity TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('OPEN','RECOVERED','DISABLED')),
 message TEXT NOT NULL, details_json TEXT NOT NULL,
 opened_at REAL NOT NULL, last_seen_at REAL NOT NULL, recovered_at REAL,
 evaluated_at REAL NOT NULL, observed_at REAL, evaluation_state TEXT NOT NULL,
 evidence_key TEXT);
CREATE UNIQUE INDEX alerts_one_open ON alerts(fingerprint) WHERE status='OPEN';
CREATE INDEX alerts_page ON alerts(status,severity,id DESC);
CREATE INDEX alerts_recovered ON alerts(recovered_at,id);
CREATE TABLE alert_state (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at REAL NOT NULL);
PRAGMA user_version=2;
PRAGMA application_id=1095191859;
"""
MIGRATION_V2 = SCHEMA[SCHEMA.index("CREATE TABLE alerts (") : SCHEMA.index("PRAGMA application_id")]


class Conflict(ValueError):
    pass


def check_schema(db, *, allow_v1=False):
    version = db.execute("PRAGMA user_version").fetchone()[0]
    if (
        version not in ((1, 2) if allow_v1 else (2,))
        or db.execute("PRAGMA application_id").fetchone()[0] != 1095191859
    ):
        raise ValueError("monitor schema unavailable")


@contextmanager
def connect(path: Path, *, write=False):
    # URI ro/rw prevents accidental initialization by Web or on an unexpected path.
    db = sqlite3.connect(f"{path.as_uri()}?mode={'rw' if write else 'ro'}", uri=True, timeout=1)
    try:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        if not write:
            db.execute("PRAGMA query_only=ON")
        check_schema(db)
        # Even unexpected query plans have a finite VM instruction budget.
        remaining = 2000

        def progress():
            nonlocal remaining
            remaining -= 1
            return int(remaining <= 0)

        db.set_progress_handler(progress, 1000)
        yield db
    finally:
        db.close()


def initialize(path: Path):
    # Only a genuinely new file is initialized. Empty, corrupt and unknown existing
    # databases are never repaired/replaced automatically.
    import os

    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        db = sqlite3.connect(path, timeout=1)
        try:
            db.execute("BEGIN IMMEDIATE")
            check_schema(db, allow_v1=True)
            if db.execute("PRAGMA user_version").fetchone()[0] == 1:
                for statement in MIGRATION_V2.split(";"):
                    if statement.strip():
                        db.execute(statement)
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()
        return
    else:
        os.close(fd)
    db = sqlite3.connect(path, timeout=1)
    try:
        db.execute("PRAGMA journal_mode=WAL")
        db.executescript("BEGIN IMMEDIATE;" + SCHEMA + "COMMIT;")
    finally:
        db.close()


def ingest(db, cfg: MonitorIngestConfig, name, payload, canonical, digest, now):
    captured = timestamp(payload["captured_at"]).timestamp()
    if captured > now + cfg.future_seconds or captured < now - cfg.history_days * 86400:
        raise ValueError("capture outside acceptance window")
    with db:
        db.execute("BEGIN IMMEDIATE")
        old = db.execute("SELECT hash FROM transfer_receipts WHERE identity=?", (name,)).fetchone()
        if old and old[0] != digest:
            raise Conflict("transfer conflict")
        duplicate = db.execute("SELECT 1 FROM content_receipts WHERE hash=?", (digest,)).fetchone()
        db.execute("INSERT OR IGNORE INTO transfer_receipts VALUES (?,?,?)", (name, digest, now))
        if duplicate:
            return False
        db.execute("INSERT INTO content_receipts VALUES (?,?)", (digest, now))
        db.execute(
            "INSERT INTO node_samples VALUES (?,?,?,?,?)",
            (digest, payload["node_id"], captured, now, canonical),
        )
        db.executemany(
            "INSERT INTO filesystem_samples VALUES (?,?,?)",
            [(digest, i, json.dumps(fs)) for i, fs in enumerate(payload["filesystems"])],
        )
        storage = payload["managed_storage"]
        db.execute(
            "INSERT INTO managed_storage_samples VALUES (?,?,?)",
            (
                digest,
                timestamp(storage["captured_at"]).timestamp() if storage["captured_at"] else None,
                json.dumps(storage),
            ),
        )
        db.execute("INSERT OR IGNORE INTO nodes VALUES (?,?,NULL)", (payload["node_id"], now))
        db.execute(
            """UPDATE nodes SET latest_hash=? WHERE node_id=? AND
            (latest_hash IS NULL OR (SELECT (captured,hash) < (?,?)
             FROM node_samples WHERE hash=latest_hash))""",
            (digest, payload["node_id"], captured, digest),
        )
    return True


def retention(db, cfg, now):
    with db:
        db.execute(
            """DELETE FROM node_samples WHERE hash IN
          (SELECT s.hash FROM node_samples s WHERE received < ?
           AND NOT EXISTS (SELECT 1 FROM nodes n WHERE n.latest_hash=s.hash)
           ORDER BY received LIMIT ?)""",
            (now - cfg.history_days * 86400, cfg.batch_size),
        )
        # Content receipts remain while a sample exists. Transfer identities expire
        # even for a pinned sample, bounding repeated rename deliveries.
        for table, key in (("content_receipts", "hash"), ("transfer_receipts", "identity")):
            keep = (
                "AND NOT EXISTS (SELECT 1 FROM node_samples s WHERE s.hash=r.hash)"
                if table == "content_receipts"
                else ""
            )
            db.execute(
                f"""DELETE FROM {table} WHERE {key} IN
              (SELECT r.{key} FROM {table} r WHERE received < ?
               {keep}
               ORDER BY received LIMIT ?)""",
                (now - cfg.dedup_days * 86400, cfg.batch_size),
            )
        db.execute(
            """DELETE FROM observations WHERE identity IN
            (SELECT identity FROM observations WHERE seen < ? ORDER BY seen LIMIT ?)""",
            (now - 86400, cfg.batch_size),
        )
    db.execute("PRAGMA wal_checkpoint(PASSIVE)")


def _sample(row, now):
    payload = json.loads(row["payload"])
    VALIDATE(payload)
    return {
        "sample_id": row["hash"],
        "received_at": datetime.fromtimestamp(row["received"], UTC).isoformat(),
        "age_seconds": now - row["captured"],
        "payload": payload,
    }


def read_sources(cfg, *, node=None, window="24h", limit=100, before=None, now=None):
    if cfg is None:
        return {"status": "DISABLED", "samples": [], "next_cursor": None}
    now = now if now is not None else datetime.now(UTC).timestamp()
    limit = max(1, min(limit, 200))
    try:
        with connect(cfg.db_path) as db:
            if node is None:
                rows = db.execute(
                    """SELECT s.* FROM nodes n JOIN node_samples s
                    ON s.hash=n.latest_hash WHERE n.node_id > ? ORDER BY n.node_id LIMIT ?""",
                    (before or "", limit + 1),
                ).fetchall()
                cursor = rows[limit - 1]["node_id"] if len(rows) > limit else None
            else:
                days = {"24h": 1, "7d": 7, "30d": 30}[window]
                upper, digest = (now + cfg.future_seconds, "z")
                if before:
                    raw_time, digest = before.split(":", 1)
                    upper = float(raw_time)
                    if not 0 <= upper <= now + cfg.future_seconds or len(digest) != 64:
                        raise ValueError("cursor")
                rows = db.execute(
                    """SELECT * FROM node_samples WHERE node_id=? AND captured>=?
                    AND (captured,hash)<(?,?) ORDER BY captured DESC,hash DESC LIMIT ?""",
                    (node, now - days * 86400, upper, digest, limit + 1),
                ).fetchall()
                cursor = (
                    f"{rows[limit - 1]['captured']}:{rows[limit - 1]['hash']}"
                    if len(rows) > limit
                    else None
                )
            return {
                "status": "OK",
                "samples": [_sample(row, now) for row in rows[:limit]],
                "next_cursor": cursor,
            }
    except (OSError, sqlite3.Error, ValueError, KeyError, TypeError):
        return {"status": "UNAVAILABLE", "samples": [], "next_cursor": None}
