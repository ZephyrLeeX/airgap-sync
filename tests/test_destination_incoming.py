from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import zstandard

from airgap_sync.common.manifest import (
    ChunkMeta,
    Manifest,
    SchemaFileMeta,
    SourceMeta,
    VerificationMeta,
    manifest_payload,
)
from airgap_sync.common.row_codec import encode_row
from airgap_sync.common.transport import transport_filename
from airgap_sync.destination.incoming import DestinationError, discover_runs, validate_run

RUN = "20260915T030000Z-a1b2c3d4"
SHA_ZERO = "0" * 64


def _put(incoming: Path, logical: str, payload: bytes) -> Path:
    path = incoming / transport_filename(RUN, logical)
    path.write_bytes(payload)
    return path


def make_run(incoming: Path, rows=((1, "a"), (2, None))) -> Manifest:
    incoming.mkdir()
    schema = b"CREATE TABLE `new_table` (`id` int, `name` text);\n"
    raw = b"".join(encode_row(row) + b"\n" for row in rows)
    compressed = zstandard.ZstdCompressor().compress(raw)
    _put(incoming, "schema.sql", schema)
    _put(incoming, "chunk-000001.jsonl.zst", compressed)
    manifest = Manifest(
        protocol_version=1,
        run_id=RUN,
        run_type="FULL_SNAPSHOT",
        source=SourceMeta(database="source_db", table="new_table"),
        schema_file=SchemaFileMeta(file="schema.sql", sha256=hashlib.sha256(schema).hexdigest()),
        columns=["id", "name"],
        row_count=len(rows),
        chunks=[
            ChunkMeta(
                sequence=1,
                file="chunk-000001.jsonl.zst",
                rows=len(rows),
                uncompressed_bytes=len(raw),
                compressed_bytes=len(compressed),
                sha256=hashlib.sha256(compressed).hexdigest(),
            )
        ]
        if rows
        else [],
        verification=VerificationMeta(
            algorithm="multiset_digest_v1",
            row_count=len(rows),
            digest_a=SHA_ZERO,
            digest_b=SHA_ZERO,
        ),
        created_at="2026-09-15T03:00:00+00:00",
    )
    if not rows:
        (incoming / transport_filename(RUN, "chunk-000001.jsonl.zst")).unlink()
    _put(incoming, "manifest.json", manifest_payload(manifest))
    return manifest


def test_no_manifest_is_ignored(tmp_path):
    tmp_path.joinpath("airgap-v1--20260915T030000Z-a1b2c3d4--chunk-000001.jsonl.zst").touch()
    assert discover_runs(tmp_path) == []


def test_valid_and_empty_runs(tmp_path):
    incoming = tmp_path / "incoming"
    make_run(incoming)
    assert validate_run(incoming, RUN, 0, sleeper=lambda _: None).manifest.row_count == 2

    other = tmp_path / "empty"
    make_run(other, rows=())
    validated = validate_run(other, RUN, 0, sleeper=lambda _: None)
    assert validated.chunk_paths == ()


def test_missing_chunk_is_incomplete(tmp_path):
    incoming = tmp_path / "incoming"
    make_run(incoming)
    (incoming / transport_filename(RUN, "chunk-000001.jsonl.zst")).unlink()
    with pytest.raises(DestinationError) as error:
        validate_run(incoming, RUN, 0, sleeper=lambda _: None)
    assert error.value.code == "RUN_INCOMPLETE"
    assert not error.value.permanent


def test_file_change_during_settle_is_incomplete(tmp_path):
    incoming = tmp_path / "incoming"
    make_run(incoming)
    chunk = incoming / transport_filename(RUN, "chunk-000001.jsonl.zst")

    def mutate(_):
        chunk.write_bytes(chunk.read_bytes() + b"x")

    with pytest.raises(DestinationError) as error:
        validate_run(incoming, RUN, 1, sleeper=mutate)
    assert error.value.code == "RUN_INCOMPLETE"


@pytest.mark.parametrize(
    ("field", "value", "code"),
    [
        ("protocol_version", 2, "UNSUPPORTED_PROTOCOL"),
        ("run_type", "DIFF", "UNSUPPORTED_RUN_TYPE"),
    ],
)
def test_unsupported_manifest_contract(tmp_path, field, value, code):
    incoming = tmp_path / "incoming"
    manifest = make_run(incoming)
    data = manifest.model_dump(mode="json", by_alias=True)
    data[field] = value
    _put(incoming, "manifest.json", json.dumps(data).encode())
    with pytest.raises(DestinationError) as error:
        validate_run(incoming, RUN, 0, sleeper=lambda _: None)
    assert error.value.code == code


def test_unsupported_digest_algorithm(tmp_path):
    incoming = tmp_path / "incoming"
    manifest = make_run(incoming)
    data = manifest.model_dump(mode="json", by_alias=True)
    data["verification"]["algorithm"] = "future_digest"
    _put(incoming, "manifest.json", json.dumps(data).encode())
    with pytest.raises(DestinationError) as error:
        validate_run(incoming, RUN, 0, sleeper=lambda _: None)
    assert error.value.code == "UNSUPPORTED_VERIFICATION_ALGORITHM"


def test_filename_manifest_run_mismatch(tmp_path):
    incoming = tmp_path / "incoming"
    manifest = make_run(incoming)
    data = manifest.model_dump(mode="json", by_alias=True)
    data["run_id"] = "20260915T030001Z-ffffffff"
    _put(incoming, "manifest.json", json.dumps(data).encode())
    with pytest.raises(DestinationError) as error:
        validate_run(incoming, RUN, 0, sleeper=lambda _: None)
    assert error.value.code == "MANIFEST_RUN_ID_MISMATCH"


@pytest.mark.parametrize("kind", ["size-short", "size-long", "hash", "schema-hash"])
def test_artifact_integrity_failures(tmp_path, kind):
    incoming = tmp_path / "incoming"
    manifest = make_run(incoming)
    data = manifest.model_dump(mode="json", by_alias=True)
    if kind == "size-short":
        data["chunks"][0]["compressed_bytes"] += 1
        expected = "RUN_INCOMPLETE"
    elif kind == "size-long":
        data["chunks"][0]["compressed_bytes"] -= 1
        expected = "ARTIFACT_SIZE_MISMATCH"
    elif kind == "hash":
        data["chunks"][0]["sha256"] = "f" * 64
        expected = "ARTIFACT_HASH_MISMATCH"
    else:
        data["schema"]["sha256"] = "f" * 64
        expected = "ARTIFACT_HASH_MISMATCH"
    _put(incoming, "manifest.json", json.dumps(data).encode())
    with pytest.raises(DestinationError) as error:
        validate_run(incoming, RUN, 0, sleeper=lambda _: None)
    assert error.value.code == expected


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (lambda data: data["chunks"][0].update(sequence=2), "CHUNK_SEQUENCE_GAP"),
        (
            lambda data: data["chunks"][0].update(file="chunk-000002.jsonl.zst"),
            "CHUNK_LOGICAL_NAME_MISMATCH",
        ),
        (lambda data: data.update(row_count=3), "MANIFEST_ROW_COUNT_MISMATCH"),
    ],
)
def test_manifest_internal_consistency(tmp_path, mutation, code):
    incoming = tmp_path / "incoming"
    manifest = make_run(incoming)
    data = manifest.model_dump(mode="json", by_alias=True)
    mutation(data)
    _put(incoming, "manifest.json", json.dumps(data).encode())
    with pytest.raises(DestinationError) as error:
        validate_run(incoming, RUN, 0, sleeper=lambda _: None)
    assert error.value.code == code
