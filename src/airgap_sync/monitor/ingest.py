"""Bounded Linux Destination ingestion, isolated from the business worker."""

import json
import logging
import os
import re
import sqlite3
import stat
import threading
import time
from contextlib import ExitStack, suppress
from itertools import islice
from pathlib import Path
from uuid import uuid4

from airgap_sync.monitor import store
from airgap_sync.monitor.protocol import MAX_BYTES, NAME, decode

logger = logging.getLogger(__name__)


def safe_directory(path: Path):
    """Refuse linked ancestors. Monitor directories must be operator-owned."""
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for component in path.parts[1:]:
            if component in (".", ".."):
                raise OSError("unsafe directory component")
            with suppress(FileExistsError):
                os.mkdir(component, mode=0o700, dir_fd=fd)
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def fingerprint(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


class Ingestor:
    def __init__(self, config, clock=time.time):
        self.config = config
        self.clock = clock
        self.stack = ExitStack()
        self.iterators = {}

    def __enter__(self):
        import fcntl

        try:
            cfg = self.config
            self.root = safe_directory(cfg.incoming)
            self.stack.callback(os.close, self.root)
            self.db_dir = safe_directory(cfg.db_path.parent)
            self.stack.callback(os.close, self.db_dir)
            # Lock both namespaces: different DBs must not race on the same inbox;
            # different inboxes must not race on the same database.
            locked_directories = set()
            for directory in (self.db_dir, self.root):
                info = os.fstat(directory)
                identity = (info.st_dev, info.st_ino)
                if identity in locked_directories:
                    continue
                fd = os.open(
                    ".monitor-ingest.lock",
                    os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=directory,
                )
                self.stack.callback(os.close, fd)
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked_directories.add(identity)
            for name in ("processing", "quarantine"):
                path = cfg.incoming / f".airgap-monitor-{name}"
                fd = safe_directory(path)
                self.stack.callback(os.close, fd)
                if os.fstat(fd).st_mode & 0o022:
                    raise OSError("private monitor directory is writable by others")
                setattr(self, name, fd)
            if cfg.db_path.is_symlink():
                raise OSError("linked database")
            store.initialize(cfg.db_path)
            return self
        except BaseException:
            self.close()
            raise

    def close(self):
        for iterator in self.iterators.values():
            iterator.close()
        self.iterators.clear()
        self.stack.close()

    def __exit__(self, *_):
        self.close()

    def entries(self, directory, count):
        # A persistent scandir cursor bounds enumeration without starving files
        # behind a large number of unrelated business files.
        iterator = self.iterators.get(directory)
        if iterator is None:
            fd = os.open(".", os.O_RDONLY | os.O_DIRECTORY, dir_fd=directory)
            try:
                iterator = self.iterators[directory] = os.scandir(fd)
            finally:
                os.close(fd)
        for _ in range(count):
            entry = next(iterator, None)
            if entry is None:
                iterator.close()
                del self.iterators[directory]
                break
            yield entry.name

    def stable(self, db, name, info, now):
        mark = json.dumps(fingerprint(info))
        row = db.execute(
            "SELECT fingerprint,since FROM observations WHERE identity=?", (name,)
        ).fetchone()
        since = row[1] if row and row[0] == mark and row[1] <= now else now
        with db:
            db.execute(
                "INSERT OR REPLACE INTO observations VALUES (?,?,?,?)", (name, mark, since, now)
            )
        return now - since

    def remove(self, directory, name, expected):
        # Private processing directory prevents producer path replacement. An FTP
        # writer retaining an open inode is detected by size/mtime/ctime checks.
        current = os.stat(name, dir_fd=directory, follow_symlinks=False)
        if fingerprint(current) != fingerprint(expected):
            return False
        os.unlink(name, dir_fd=directory)
        return True

    def reject(self, name, info, reason, now):
        # Bounded metadata-only quarantine: do not retain arbitrary/oversized bytes.
        record = json.dumps({"identity": name, "reason": reason, "rejected_at": now}).encode()
        key = f"{int(now)}-{uuid4().hex}.json"
        fd = os.open(
            key, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=self.quarantine
        )
        with os.fdopen(fd, "wb") as handle:
            handle.write(record)
        return self.remove(self.processing, name, info)

    def quarantine_retention(self, now):
        # Only our metadata records exist here. Bound memory/enumeration and cap
        # Cleanup precedes admissions, allowing two per-directory batches of slack.
        records = []
        fd = os.open(".", os.O_RDONLY | os.O_DIRECTORY, dir_fd=self.quarantine)
        try:
            with os.scandir(fd) as entries:
                for entry in islice(
                    entries, self.config.quarantine_max_files + 2 * self.config.batch_size + 1
                ):
                    name = entry.name
                    if not re.fullmatch(r"[0-9]+-[0-9a-f]{32}\.json", name):
                        continue
                    info = entry.stat(follow_symlinks=False)
                    if stat.S_ISREG(info.st_mode):
                        records.append((int(name.split("-", 1)[0]), name, info))
        finally:
            os.close(fd)
        records.sort()
        excess = max(0, len(records) - self.config.quarantine_max_files)
        for index, (created, name, info) in enumerate(records):
            if index < excess or created < now - self.config.quarantine_days * 86400:
                self.remove(self.quarantine, name, info)

    def process(self, db, directory, name, now):
        info = os.stat(name, dir_fd=directory, follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode):
            return "ignored"
        observation = ("incoming:" if directory == self.root else "processing:") + name
        elapsed = self.stable(db, observation, info, now)
        if elapsed < self.config.settle_seconds:
            return "waiting"
        if directory == self.root:
            # Never replace a pending claimed file with a fresh delivery.
            try:
                os.stat(name, dir_fd=self.processing, follow_symlinks=False)
                return "waiting"
            except FileNotFoundError:
                pass
            os.rename(name, name, src_dir_fd=self.root, dst_dir_fd=self.processing)
            # Rename changes ctime. Replaced inode must be re-observed, never deleted.
            claimed = os.stat(name, dir_fd=self.processing, follow_symlinks=False)
            if fingerprint(claimed)[:4] != fingerprint(info)[:4]:
                return "waiting"
            info = claimed
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=self.processing)
        with os.fdopen(fd, "rb") as handle:
            if fingerprint(os.fstat(handle.fileno())) != fingerprint(info):
                return "waiting"
            body = handle.read(MAX_BYTES + 1)
            if fingerprint(os.fstat(handle.fileno())) != fingerprint(info):
                return "waiting"
        try:
            payload, canonical, digest = decode(body, name)
            inserted = store.ingest(db, self.config, name, payload, canonical, digest, now)
        except (ValueError, UnicodeError, RecursionError) as exc:
            if elapsed < self.config.invalid_grace_seconds:
                return "waiting"
            reason = "TRANSFER_CONFLICT" if isinstance(exc, store.Conflict) else "INVALID_TELEMETRY"
            removed = self.reject(name, info, reason, now)
            result = "rejected" if removed else "waiting"
        else:
            removed = self.remove(self.processing, name, info)
            result = "inserted" if inserted else "duplicate"
        if removed:
            with db:
                db.execute("DELETE FROM observations WHERE identity=?", (observation,))
        return result

    def tick(self):
        now = self.clock()
        counts = {
            key: 0 for key in ("inserted", "duplicate", "rejected", "waiting", "ignored", "error")
        }
        with store.connect(self.config.db_path, write=True) as db:
            store.retention(db, self.config, now)
            self.quarantine_retention(now)
            candidates = 0
            # Separate quotas ensure a bad/retrying processing file cannot block inbox.
            for directory in (self.processing, self.root):
                used = 0
                for name in self.entries(directory, self.config.scan_limit):
                    if not NAME.fullmatch(name):
                        continue
                    try:
                        counts[self.process(db, directory, name, now)] += 1
                    except (OSError, sqlite3.Error):
                        counts["error"] += 1
                    used += 1
                    candidates += 1
                    if used >= self.config.batch_size:
                        break
        return {**counts, "candidates": candidates}


class BackgroundIngest:
    """One background thread, cancellable between bounded rounds; lock retry on failure."""

    def __init__(self, config, app_config=None):
        self.config = config
        self.app_config = app_config
        self.stop_event = threading.Event()
        self.thread = None
        self.status = "STARTING"
        self.alert_status = "STARTING" if app_config and app_config.monitor_alerts else "DISABLED"

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self.run, name="monitor-ingest", daemon=True)
        self.thread.start()

    def run(self):
        next_alert = 0.0
        next_ingest = 0.0
        from airgap_sync.monitor.alerts import IncomingScanner

        scanner = (
            IncomingScanner(self.app_config.destination.incoming_dir)
            if self.app_config and self.app_config.monitor_alerts
            else None
        )
        while not self.stop_event.is_set():
            try:
                with Ingestor(self.config) as ingestor:
                    while not self.stop_event.is_set():
                        if time.time() >= next_ingest:
                            try:
                                counts = ingestor.tick()
                                self.status = "DEGRADED" if counts["error"] else "RUNNING"
                            except Exception:
                                self.status = "UNAVAILABLE"
                                logger.warning("Monitor ingest round unavailable")
                            next_ingest = time.time() + self.config.poll_seconds
                        if self.app_config and time.time() >= next_alert:
                            try:
                                from airgap_sync.monitor.alerts import disable_all, evaluate

                                with store.connect(self.config.db_path, write=True) as db:
                                    if self.app_config.monitor_alerts:
                                        failures = evaluate(
                                            self.app_config, db, incoming_scanner=scanner
                                        )
                                        self.alert_status = "DEGRADED" if failures else "RUNNING"
                                    else:
                                        disable_all(db, time.time())
                            except Exception:
                                self.alert_status = "UNAVAILABLE"
                                logger.warning("Monitor alert evaluation unavailable")
                            interval = (
                                self.app_config.monitor_alerts.interval_seconds
                                if self.app_config.monitor_alerts
                                else 60
                            )
                            next_alert = time.time() + interval
                        self.stop_event.wait(
                            max(
                                0.1,
                                min(next_ingest, next_alert if self.app_config else next_ingest)
                                - time.time(),
                            )
                        )
            except Exception:
                self.status = "UNAVAILABLE_OR_LOCKED"
                if self.app_config and self.app_config.monitor_alerts:
                    self.alert_status = "UNAVAILABLE_OR_LOCKED"
                logger.warning("Monitor ingest unavailable or already owned")
                self.stop_event.wait(self.config.poll_seconds)
        self.status = "STOPPED"
        if self.app_config and self.app_config.monitor_alerts:
            self.alert_status = "STOPPED"
        if scanner is not None:
            scanner.close()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join()
