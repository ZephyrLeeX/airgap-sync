"""Strict M2 wire validation. Unknown fields/enums require a protocol revision."""

import hashlib
import json
import re
from datetime import UTC, datetime

NAME = re.compile(
    r"airgap-monitor-v(?P<version>[0-9]{1,3})--"
    r"(?P<node>[A-Za-z0-9][A-Za-z0-9_-]{0,63})--"
    r"(?P<stamp>[0-9]{8}T[0-9]{6}Z)--[0-9a-f]{8}\.json\Z"
)
MAX_BYTES = 65536


def timestamp(value):
    if not isinstance(value, str) or not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z", value
    ):
        raise ValueError("timestamp")
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def text(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 512:
        raise ValueError("string")
    if any(ord(c) < 32 or 0xD800 <= ord(c) <= 0xDFFF for c in value):
        raise ValueError("control character")


def integer(value):
    if type(value) is not int or not 0 <= value <= 2**63 - 1:
        raise ValueError("integer")


def percent(value):
    if type(value) not in (int, float) or not 0 <= value <= 100:
        raise ValueError("percent")


def enum(*allowed):
    def check(value):
        if not isinstance(value, str) or value not in allowed:
            raise ValueError("enum")

    return check


def nullable(check):
    def validate(value):
        if value is not None:
            check(value)

    return validate


def array(check, limit):
    def validate(value):
        if not isinstance(value, list) or len(value) > limit:
            raise ValueError("array")
        for item in value:
            check(item)

    return validate


def obj(**fields):
    def validate(value):
        if not isinstance(value, dict) or value.keys() != fields.keys():
            raise ValueError("fields")
        for key, check in fields.items():
            check(value[key])

    return validate


N = nullable(integer)
T = nullable(timestamp)
S = nullable(text)
P = nullable(percent)
RUN = nullable(
    obj(
        run_id=text,
        table=text,
        status=enum(
            "GENERATING",
            "UPLOADING",
            "FINALIZING",
            "SNAPSHOT_READY",
            "DELIVERED",
            "FAILED",
            "DISK_PRESSURE",
        ),
        created_at=T,
        rows_scanned=N,
        chunks_created=N,
        raw_bytes=N,
        compressed_bytes=N,
        chunks_uploaded=N,
        pending_bytes=N,
    )
)
VALIDATE = obj(
    schema_version=integer,
    node_id=text,
    role=enum("SOURCE"),
    captured_at=timestamp,
    agent_version=text,
    host=obj(hostname=S, os=S, uptime_seconds=N, boot_time=T),
    worker=obj(
        status=enum("RUNNING", "STOPPED", "UNKNOWN"),
        pid=N,
        task_status=enum("UNKNOWN", "Unknown", "Ready", "Running", "Disabled", "Queued"),
    ),
    cycle=nullable(
        obj(
            cycle_id=text,
            status=enum("RUNNING", "RETRY_WAIT", "COMPLETED", "ABANDONED"),
            started_at=T,
            completed_at=T,
            tables_total=N,
            tables_delivered=N,
            tables_failed=N,
        )
    ),
    current_run=RUN,
    last_run=RUN,
    next_action_at=T,
    filesystems=array(
        obj(
            mount=S,
            total_bytes=N,
            free_bytes=N,
            free_percent=P,
            locations=array(enum("system", "install", "config", "data", "temp"), 5),
        ),
        5,
    ),
    managed_storage=obj(
        captured_at=T,
        outbox_bytes=N,
        failed_runs_bytes=N,
        backups_bytes=N,
        logs_bytes=N,
        pending_spool_bytes=N,
    ),
    system=obj(
        cpu_percent=P, memory_total_bytes=N, memory_available_bytes=N, memory_used_percent=P
    ),
    connectivity=obj(source_mysql=enum("UNKNOWN"), relay=enum("UNKNOWN")),
    collection_errors=array(
        enum(
            "SOURCE_METADATA_MISSING",
            "SOURCE_METADATA_SCHEMA",
            "SOURCE_METADATA_UNAVAILABLE",
            "CYCLE_TABLE_LIMIT",
            "RUN_ARTIFACT_LIMIT",
            "SYSTEM_UNAVAILABLE",
            "WORKER_QUERY_UNAVAILABLE",
            "UPTIME_UNAVAILABLE",
            "CPU_UNAVAILABLE",
            "MEMORY_UNAVAILABLE",
            "FILESYSTEM_UNAVAILABLE",
            "STORAGE_UNAVAILABLE",
            "COLLECTION_ERROR",
        ),
        16,
    ),
)


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _constant(_):
    raise ValueError("nonfinite")


def decode(body: bytes, name: str) -> tuple[dict, str, str]:
    match = NAME.fullmatch(name)
    if not match or len(body) > MAX_BYTES:
        raise ValueError("envelope")
    payload = json.loads(body.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant)
    VALIDATE(payload)
    if payload["schema_version"] != 1 or match["version"] != "1":
        raise ValueError("version")
    if payload["node_id"] != match["node"]:
        raise ValueError("node")
    if timestamp(payload["captured_at"]).strftime("%Y%m%dT%H%M%SZ") != match["stamp"]:
        raise ValueError("capture")
    for fs in payload["filesystems"]:
        if (
            fs["free_bytes"] is not None
            and fs["total_bytes"] is not None
            and fs["free_bytes"] > fs["total_bytes"]
        ):
            raise ValueError("capacity")
    system = payload["system"]
    if (
        system["memory_available_bytes"] is not None
        and system["memory_total_bytes"] is not None
        and system["memory_available_bytes"] > system["memory_total_bytes"]
    ):
        raise ValueError("memory")
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"))
    return payload, canonical, hashlib.sha256(canonical.encode()).hexdigest()
