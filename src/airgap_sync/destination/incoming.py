"""Destination incoming 中 Run 的发现、稳定性与完整性验证。"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from airgap_sync.common.manifest import (
    PROTOCOL_VERSION,
    RUN_TYPE_FULL_SNAPSHOT,
    Manifest,
    read_manifest,
)
from airgap_sync.common.transport import parse_transport_filename, transport_filename
from airgap_sync.common.verification import DIGEST_ALGORITHM
from airgap_sync.source.chunk_writer import chunk_filename


class DestinationError(Exception):
    """Destination 可分类错误。permanent=False 表示稍后可重试。"""

    def __init__(self, code: str, message: str, *, permanent: bool = True) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.permanent = permanent


@dataclass(frozen=True)
class ValidatedRun:
    manifest: Manifest
    manifest_path: Path
    schema_path: Path
    chunk_paths: tuple[Path, ...]


def discover_runs(incoming_dir: Path) -> list[str]:
    """只把合法 transport manifest 当作候选 Run，按 run_id 顺序返回。"""
    if not incoming_dir.exists():
        return []
    run_ids: list[str] = []
    for path in incoming_dir.iterdir():
        if not path.is_file():
            continue
        try:
            run_id, logical = parse_transport_filename(path.name)
        except ValueError:
            continue
        if logical == "manifest.json":
            run_ids.append(run_id)
    return sorted(set(run_ids))


def _sha256(path: Path, block_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while block := handle.read(block_size):
                digest.update(block)
    except OSError as exc:
        raise DestinationError(
            "RUN_INCOMPLETE", f"cannot read {path.name}: {exc}", permanent=False
        ) from exc
    return digest.hexdigest()


def _stat_set(paths: list[Path]) -> dict[Path, tuple[int, int]]:
    stats: dict[Path, tuple[int, int]] = {}
    for path in paths:
        try:
            stat = path.stat()
        except FileNotFoundError as exc:
            raise DestinationError(
                "RUN_INCOMPLETE", f"expected artifact is missing: {path.name}", permanent=False
            ) from exc
        except OSError as exc:
            raise DestinationError(
                "RUN_INCOMPLETE", f"cannot stat artifact {path.name}: {exc}", permanent=False
            ) from exc
        if not path.is_file():
            raise DestinationError(
                "RUN_INCOMPLETE", f"expected artifact is not a file: {path.name}", permanent=False
            )
        stats[path] = (stat.st_size, stat.st_mtime_ns)
    return stats


def _validate_manifest(manifest: Manifest, filename_run_id: str) -> None:
    if manifest.run_id != filename_run_id:
        raise DestinationError(
            "MANIFEST_RUN_ID_MISMATCH",
            f"transport run_id {filename_run_id!r} != manifest run_id {manifest.run_id!r}",
        )
    if manifest.protocol_version != PROTOCOL_VERSION:
        raise DestinationError(
            "UNSUPPORTED_PROTOCOL", f"protocol_version={manifest.protocol_version}"
        )
    if manifest.run_type != RUN_TYPE_FULL_SNAPSHOT:
        raise DestinationError("UNSUPPORTED_RUN_TYPE", f"run_type={manifest.run_type!r}")
    if manifest.verification.algorithm != DIGEST_ALGORITHM:
        raise DestinationError(
            "UNSUPPORTED_VERIFICATION_ALGORITHM",
            f"algorithm={manifest.verification.algorithm!r}",
        )
    if manifest.row_count != manifest.verification.row_count:
        raise DestinationError(
            "MANIFEST_ROW_COUNT_MISMATCH",
            "manifest row_count differs from verification row_count",
        )
    if manifest.schema_file.file != "schema.sql":
        raise DestinationError("INVALID_SCHEMA_FILENAME", f"file={manifest.schema_file.file!r}")
    for expected, chunk in enumerate(manifest.chunks, 1):
        if chunk.sequence != expected:
            raise DestinationError(
                "CHUNK_SEQUENCE_GAP", f"expected sequence {expected}, got {chunk.sequence}"
            )
        expected_name = chunk_filename(expected)
        if chunk.file != expected_name:
            raise DestinationError(
                "CHUNK_LOGICAL_NAME_MISMATCH",
                f"sequence {expected} requires {expected_name!r}, got {chunk.file!r}",
            )
    rows = sum(chunk.rows for chunk in manifest.chunks)
    if rows != manifest.row_count:
        raise DestinationError(
            "MANIFEST_ROW_COUNT_MISMATCH",
            f"sum(chunk.rows)={rows} != row_count={manifest.row_count}",
        )


def validate_run(
    incoming_dir: Path,
    run_id: str,
    settle_seconds: float,
    *,
    sleeper: Callable[[float], None] = time.sleep,
) -> ValidatedRun:
    """读取 manifest，等待整个文件集稳定，再流式校验 size/SHA256。"""
    try:
        manifest_path = incoming_dir / transport_filename(run_id, "manifest.json")
    except ValueError as exc:
        raise DestinationError("INVALID_RUN_ID", str(exc)) from exc
    try:
        manifest = read_manifest(manifest_path)
    except Exception as exc:
        raise DestinationError("INVALID_MANIFEST", str(exc)) from exc
    _validate_manifest(manifest, run_id)
    schema_path = incoming_dir / transport_filename(run_id, manifest.schema_file.file)
    chunk_paths = tuple(
        incoming_dir / transport_filename(run_id, chunk.file) for chunk in manifest.chunks
    )
    all_paths = [manifest_path, schema_path, *chunk_paths]
    before = _stat_set(all_paths)
    sleeper(settle_seconds)
    after = _stat_set(all_paths)
    if before != after:
        raise DestinationError(
            "RUN_INCOMPLETE",
            "artifact file set changed during stability observation",
            permanent=False,
        )

    if _sha256(schema_path) != manifest.schema_file.sha256:
        raise DestinationError("ARTIFACT_HASH_MISMATCH", f"SHA256 mismatch: {schema_path.name}")
    for path, chunk in zip(chunk_paths, manifest.chunks, strict=True):
        size = after[path][0]
        if size < chunk.compressed_bytes:
            raise DestinationError(
                "RUN_INCOMPLETE",
                f"artifact is shorter than declared size: {path.name}",
                permanent=False,
            )
        if size > chunk.compressed_bytes:
            raise DestinationError("ARTIFACT_SIZE_MISMATCH", f"size mismatch: {path.name}")
        if _sha256(path) != chunk.sha256:
            raise DestinationError("ARTIFACT_HASH_MISMATCH", f"SHA256 mismatch: {path.name}")
    return ValidatedRun(manifest, manifest_path, schema_path, chunk_paths)
