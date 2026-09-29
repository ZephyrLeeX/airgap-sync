"""Best-effort, bounded stage observations kept outside business metadata."""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

STAGES = frozenset(
    {
        "source_read_wait",
        "source_encode_write",
        "source_upload_call",
        "destination_import",
        "destination_verify",
    }
)
STATES = frozenset({"RUNNING", "COMPLETE", "INTERRUPTED"})
MAX_FILE = 16384


def identity_name(node: str, database: str, table: str, run_id: str) -> str:
    body = json.dumps([node, database, table, run_id], separators=(",", ":")).encode()
    return hashlib.sha256(body).hexdigest() + ".json"


def path_for(root: Path, node: str, database: str, table: str, run_id: str) -> Path:
    return root / identity_name(node, database, table, run_id)


def read(path: Path) -> dict | None:
    try:
        with path.open("rb") as stream:
            raw = stream.read(MAX_FILE + 1)
        if len(raw) > MAX_FILE:
            return None
        value = json.loads(raw)
        if not isinstance(value, dict) or not isinstance(value.get("stages"), dict):
            return None
        return value
    except (OSError, ValueError, TypeError):
        return None


class Recorder:
    """One recorder per Worker Run; writes at most once a second plus transitions."""

    def __init__(self, root: Path, node: str, database: str, table: str, run_id: str):
        self.path = path_for(root, node, database, table, run_id)
        self.identity = [node, database, table, run_id]
        self.attempt = uuid4().hex
        self._lock = threading.Lock()
        old = read(self.path)
        self._stages = old.get("stages", {}) if old and old.get("identity") == self.identity else {}
        self.generation = (
            max(
                (
                    item["generation"]
                    for item in self._stages.values()
                    if isinstance(item, dict)
                    and type(item.get("generation")) is int
                    and item["generation"] >= 0
                ),
                default=0,
            )
            + 1
        )
        self._last_write = 0.0
        self._last_sample: dict[str, tuple[float, int, int, int]] = {}

    def update(
        self,
        stage: str,
        *,
        rows: int = 0,
        chunks: int = 0,
        bytes_: int = 0,
        seconds: float = 0.0,
        attempts: int = 0,
        retry_bytes: int = 0,
        total_rows: int | None = None,
        total_chunks: int | None = None,
        state: str = "RUNNING",
        force: bool = False,
        recovered: bool = False,
    ) -> None:
        if stage not in STAGES or state not in STATES:
            return
        try:
            with self._lock:
                now = time.monotonic()
                item = self._stages.get(stage)
                # INTERRUPTED is meaningful only for a stage this attempt
                # started and left RUNNING. It is decided before any stage
                # object is created or replaced, so an inapplicable interrupt
                # changes no memory, snapshot, counter, or sampling baseline;
                # otherwise a later write of another stage would persist the
                # replacement over the previous attempt's snapshot.
                if state == "INTERRUPTED" and (
                    not isinstance(item, dict)
                    or item.get("attempt") != self.attempt
                    or item.get("sequence", 0) == 0
                    or item.get("state") != "RUNNING"
                ):
                    return
                if not isinstance(item, dict) or item.get("attempt") != self.attempt:
                    item = {
                        "attempt": self.attempt,
                        "generation": self.generation,
                        "sequence": 0,
                        "state": "RUNNING",
                        "rows": 0,
                        "chunks": 0,
                        "bytes": 0,
                        "seconds": 0.0,
                        "attempts": 0,
                        "retry_bytes": 0,
                        "total_rows": None,
                        "total_chunks": None,
                        "observed_at": None,
                        "window_seconds": None,
                        "window_rows": None,
                        "window_bytes": None,
                        "processed_rows": 0,
                        "processed_bytes": 0,
                    }
                    self._stages[stage] = item
                    force = True
                # A final state belongs to this attempt. Later cleanup errors
                # must not regress COMPLETE.
                if item.get("state") in {"COMPLETE", "INTERRUPTED"} and state != item["state"]:
                    return
                for key, delta in (
                    ("rows", rows),
                    ("chunks", chunks),
                    ("bytes", bytes_),
                    ("attempts", attempts),
                    ("retry_bytes", retry_bytes),
                ):
                    item[key] += max(int(delta), 0)
                item["seconds"] += max(float(seconds), 0.0)
                if not recovered:
                    item["processed_rows"] += max(int(rows), 0)
                    item["processed_bytes"] += max(int(bytes_), 0)
                if total_rows is not None:
                    item["total_rows"] = max(int(total_rows), 0)
                if total_chunks is not None:
                    item["total_chunks"] = max(int(total_chunks), 0)
                changed = item["state"] != state
                item["state"] = state
                if not (force or changed or now - self._last_write >= 1.0):
                    return
                observed_at = datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")
                for name, current in self._stages.items():
                    if current.get("attempt") != self.attempt:
                        continue
                    if name != stage and current.get("state") != "RUNNING":
                        continue
                    prior = self._last_sample.get(name)
                    if (
                        prior
                        and now > prior[0]
                        and (
                            current["processed_rows"] > prior[1]
                            or current["processed_bytes"] > prior[2]
                        )
                    ):
                        current["window_seconds"] = now - prior[0]
                        current["window_rows"] = current["processed_rows"] - prior[1]
                        current["window_bytes"] = current["processed_bytes"] - prior[2]
                    else:
                        current["window_seconds"] = None
                        current["window_rows"] = None
                        current["window_bytes"] = None
                    current["observed_at"] = observed_at
                    current["sequence"] += 1
                payload = {"identity": self.identity, "stages": self._stages}
                body = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode()
                if len(body) > MAX_FILE:
                    return
                self.path.parent.mkdir(parents=True, exist_ok=True)
                part = self.path.with_name(self.path.name + "." + self.attempt + ".tmp")
                try:
                    part.write_bytes(body)
                    os.replace(part, self.path)
                finally:
                    part.unlink(missing_ok=True)
                self._last_write = now
                for name, current in self._stages.items():
                    if current.get("attempt") == self.attempt and (
                        name == stage or current.get("state") == "RUNNING"
                    ):
                        self._last_sample[name] = (
                            now,
                            current["processed_rows"],
                            current["processed_bytes"],
                            current["chunks"],
                        )
        except Exception:
            # Monitoring cannot alter Worker success, retry, or consistency.
            return


def display(item: dict | None, *, now: datetime | None = None, remote: bool = False) -> dict:
    if not item:
        return {"state": "UNKNOWN"}
    result = dict(item)
    try:
        observed = datetime.fromisoformat(result["observed_at"].replace("Z", "+00:00"))
        age = ((now or datetime.now(UTC)) - observed).total_seconds()
    except (TypeError, ValueError, KeyError, AttributeError):
        age = None
    result["age_seconds"] = age
    if result.get("state") == "RUNNING":
        result["state"] = (
            "STALE"
            if age is None or age > 900
            else "LAST_OBSERVED"
            if remote or age > 5
            else "RUNNING"
        )
    window = result.get("window_seconds")
    result["rows_per_second"] = (
        result.get("window_rows", 0) / window
        if isinstance(window, (int, float))
        and window > 0
        and isinstance(result.get("window_rows"), int)
        and result["window_rows"] > 0
        else None
    )
    result["bytes_per_second"] = (
        result.get("window_bytes", 0) / window
        if isinstance(window, (int, float))
        and window > 0
        and isinstance(result.get("window_bytes"), int)
        and result["window_bytes"] > 0
        else None
    )
    total = result.get("total_rows")
    result["percent"] = (
        100 * result["rows"] / total
        if isinstance(total, int)
        and total > 0
        and isinstance(result.get("rows"), int)
        and result["rows"] <= total
        else None
    )
    chunk_total = result.get("total_chunks")
    result["chunk_percent"] = (
        100 * result["chunks"] / chunk_total
        if isinstance(chunk_total, int)
        and chunk_total > 0
        and isinstance(result.get("chunks"), int)
        and result["chunks"] <= chunk_total
        else None
    )
    return result


def merge_received(root: Path, node: str, database: str, fact: dict) -> None:
    """Merge one validated v3 observation; older or reset counters cannot regress it."""
    identity = [node, database, fact["table_name"], fact["run_id"]]
    path = path_for(root, *identity)
    old = read(path)
    if old and old.get("identity") != identity:
        return
    stages = old.get("stages", {}) if old and old.get("identity") == identity else {}
    for incoming in fact["stages"]:
        stage = incoming["stage"]
        prior = stages.get(stage)
        if not isinstance(prior, dict) or not all(
            key in prior
            for key in (
                "attempt",
                "observed_at",
                "rows",
                "chunks",
                "bytes",
                "attempts",
                "retry_bytes",
                "seconds",
            )
        ):
            prior = None
        if (
            prior
            and "generation" in prior
            and (type(prior["generation"]) is not int or type(prior.get("sequence")) is not int)
        ):
            prior = None
        if prior:
            if "generation" in prior:
                if "generation" not in incoming or incoming["generation"] < prior["generation"]:
                    continue
                if incoming["generation"] == prior["generation"]:
                    if incoming["attempt"] != prior["attempt"]:
                        continue
                    if incoming["sequence"] <= prior["sequence"]:
                        continue
            elif "generation" in incoming:
                pass  # A sequenced sample supersedes a legacy local or v3 observation.
            else:
                if incoming["observed_at"] <= prior["observed_at"]:
                    continue
            if incoming["attempt"] == prior["attempt"] and any(
                incoming[key] < prior[key]
                for key in ("rows", "chunks", "bytes", "attempts", "retry_bytes", "seconds")
            ):
                continue
        stages[stage] = {key: value for key, value in incoming.items() if key != "stage"}
    body = json.dumps(
        {"identity": identity, "stages": stages}, separators=(",", ":"), allow_nan=False
    ).encode()
    if len(body) > MAX_FILE:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        part = path.with_name(path.name + "." + uuid4().hex + ".tmp")
        try:
            part.write_bytes(body)
            os.replace(part, path)
        finally:
            part.unlink(missing_ok=True)
    except OSError:
        pass
