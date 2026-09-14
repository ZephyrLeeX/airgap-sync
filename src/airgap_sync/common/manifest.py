"""Manifest 数据模型与原子读写。

Manifest 是一个 Run 的完整性凭证:

    manifest.json 存在 = Run 已完整生成

因此 Manifest 必须最后通过 .part → 原子 rename 写入,
且 Chunk metadata 在生成过程中独立记录 (不要求所有 Chunk
同时存在于磁盘, 为 Phase 3 边生成边上传预留)。
"""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator

from airgap_sync.common.fsutil import atomic_write_bytes

PROTOCOL_VERSION = 1

RUN_TYPE_FULL_SNAPSHOT = "FULL_SNAPSHOT"

MANIFEST_FILENAME = "manifest.json"

_SHA256_PATTERN = r"^[0-9a-f]{64}$"


class ChunkMeta(BaseModel):
    """单个 Chunk 文件的元数据 (在 Chunk 封闭时独立记录)。"""

    model_config = ConfigDict(extra="forbid")

    sequence: int = Field(ge=1)
    file: str = Field(min_length=1)
    rows: int = Field(ge=1)
    uncompressed_bytes: int = Field(ge=1)
    compressed_bytes: int = Field(ge=1)
    sha256: str = Field(pattern=_SHA256_PATTERN)


class SchemaFileMeta(BaseModel):
    """随 Run 携带的 DDL 文件。"""

    model_config = ConfigDict(extra="forbid")

    file: str = Field(min_length=1)
    sha256: str = Field(pattern=_SHA256_PATTERN)


class SourceMeta(BaseModel):
    """快照来源标识。"""

    model_config = ConfigDict(extra="forbid")

    database: str = Field(min_length=1)
    table: str = Field(min_length=1)


class VerificationMeta(BaseModel):
    """Snapshot Multiset Digest 摘要。"""

    model_config = ConfigDict(extra="forbid")

    algorithm: str = Field(min_length=1)
    row_count: int = Field(ge=0)
    digest_a: str = Field(pattern=_SHA256_PATTERN)
    digest_b: str = Field(pattern=_SHA256_PATTERN)


class Manifest(BaseModel):
    """一个 Run 的完整描述。

    Python 侧字段名 schema_file 避免遮蔽 BaseModel.schema;
    JSON 中的键名保持为 "schema" (通过 alias)。
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    protocol_version: int = Field(ge=1)
    run_id: str = Field(min_length=1)
    run_type: str = Field(min_length=1)
    source: SourceMeta
    schema_file: SchemaFileMeta = Field(alias="schema")
    columns: list[str] = Field(min_length=1)
    row_count: int = Field(ge=0)
    chunks: list[ChunkMeta]
    verification: VerificationMeta
    created_at: str = Field(min_length=1)

    @field_validator("columns")
    @classmethod
    def columns_not_empty_and_unique(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("columns must not be empty")
        duplicates = sorted({name for name in value if value.count(name) > 1})
        if duplicates:
            raise ValueError(f"duplicate column names: {', '.join(duplicates)}")
        return value


def manifest_payload(manifest: Manifest) -> bytes:
    """Manifest 的确定性序列化 (排序 key, 固定缩进, 末尾换行)。"""
    data = manifest.model_dump(mode="json", by_alias=True)
    return json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8") + b"\n"


def write_manifest(path: Path, manifest: Manifest) -> None:
    """原子写入 manifest.json (.part → fsync → rename)。"""
    atomic_write_bytes(path, manifest_payload(manifest))


def read_manifest(path: Path) -> Manifest:
    """读取并严格校验 Manifest (字段缺失/多余/格式错误都会报错)。"""
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"cannot read manifest {path}: {exc}") from exc
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON in manifest {path}: {exc}") from exc
    return Manifest.model_validate(data)
