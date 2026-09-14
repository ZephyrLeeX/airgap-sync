"""Manifest 模型与原子读写测试。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from airgap_sync.common.manifest import (
    PROTOCOL_VERSION,
    RUN_TYPE_FULL_SNAPSHOT,
    ChunkMeta,
    Manifest,
    SchemaFileMeta,
    SourceMeta,
    VerificationMeta,
    manifest_payload,
    read_manifest,
    write_manifest,
)

SHA = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64


def sample_manifest(chunks: list[ChunkMeta] | None = None, row_count: int = 2) -> Manifest:
    if chunks is None:
        chunks = [
            ChunkMeta(
                sequence=1,
                file="chunk-000001.jsonl.zst",
                rows=1,
                uncompressed_bytes=10,
                compressed_bytes=5,
                sha256=SHA_B,
            )
        ]
    return Manifest(
        protocol_version=PROTOCOL_VERSION,
        run_id="20260914T213500Z-a1b2c3d4",
        run_type=RUN_TYPE_FULL_SNAPSHOT,
        source=SourceMeta(database="sgaj_data", table="std_xxx"),
        schema_file=SchemaFileMeta(file="schema.sql", sha256=SHA_C),
        columns=["id", "name"],
        row_count=row_count,
        chunks=chunks,
        verification=VerificationMeta(
            algorithm="multiset_digest_v1",
            row_count=row_count,
            digest_a=SHA,
            digest_b="0" * 64,
        ),
        created_at="2026-09-14T21:35:00+00:00",
    )


class TestModel:
    def test_valid_manifest(self):
        manifest = sample_manifest()
        assert manifest.protocol_version == 1
        assert manifest.run_type == "FULL_SNAPSHOT"
        assert manifest.chunks[0].sequence == 1

    def test_json_uses_schema_alias(self):
        """JSON 键名必须是 "schema" (与协议一致), 不是 schema_file。"""
        data = json.loads(manifest_payload(sample_manifest()))
        assert "schema" in data
        assert "schema_file" not in data

    def test_rejects_bad_sha256(self):
        with pytest.raises(ValidationError):
            ChunkMeta(
                sequence=1,
                file="f",
                rows=1,
                uncompressed_bytes=1,
                compressed_bytes=1,
                sha256="not-hex",
            )

    def test_rejects_zero_row_chunk(self):
        with pytest.raises(ValidationError):
            ChunkMeta(
                sequence=1, file="f", rows=0, uncompressed_bytes=1, compressed_bytes=1, sha256=SHA
            )

    def test_rejects_duplicate_columns(self):
        with pytest.raises(ValidationError, match="duplicate column"):
            sample_manifest().__class__(**{**sample_manifest().model_dump(), "columns": ["a", "a"]})

    def test_rejects_empty_columns(self):
        data = sample_manifest().model_dump()
        data["columns"] = []
        with pytest.raises(ValidationError):
            Manifest.model_validate(data)

    def test_rejects_unknown_fields(self):
        data = sample_manifest().model_dump()
        data["extra"] = 1
        with pytest.raises(ValidationError):
            Manifest.model_validate(data)


class TestAtomicWrite:
    def test_write_and_read_round_trip(self, tmp_path: Path):
        path = tmp_path / "manifest.json"
        manifest = sample_manifest()
        write_manifest(path, manifest)
        assert read_manifest(path) == manifest

    def test_no_part_file_left_behind(self, tmp_path: Path):
        write_manifest(tmp_path / "manifest.json", sample_manifest())
        assert list(tmp_path.glob("*.part")) == []

    def test_payload_is_deterministic(self):
        assert manifest_payload(sample_manifest()) == manifest_payload(sample_manifest())

    def test_payload_ends_with_newline(self):
        assert manifest_payload(sample_manifest()).endswith(b"\n")

    def test_overwrite_replaces_atomically(self, tmp_path: Path):
        path = tmp_path / "manifest.json"
        write_manifest(path, sample_manifest())
        other = sample_manifest().model_copy(
            update={
                "row_count": 5,
                "verification": VerificationMeta(
                    algorithm="multiset_digest_v1", row_count=5, digest_a=SHA, digest_b="0" * 64
                ),
            }
        )
        write_manifest(path, other)
        assert read_manifest(path) == other


class TestReadValidation:
    def test_missing_file(self, tmp_path: Path):
        with pytest.raises(ValueError, match="cannot read manifest"):
            read_manifest(tmp_path / "nope.json")

    def test_invalid_json(self, tmp_path: Path):
        path = tmp_path / "manifest.json"
        path.write_text("{broken", encoding="utf-8")
        with pytest.raises(ValueError, match="invalid JSON"):
            read_manifest(path)

    def test_missing_required_field(self, tmp_path: Path):
        path = tmp_path / "manifest.json"
        data = sample_manifest().model_dump()
        del data["run_id"]
        path.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(ValidationError):
            read_manifest(path)
