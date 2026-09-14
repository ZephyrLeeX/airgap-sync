"""Chunk Writer 测试: 阈值、原子性、SHA256、zstd 解压。"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
import zstandard

from airgap_sync.common.models import ChunkConfig
from airgap_sync.source.chunk_writer import ChunkWriter, ChunkWriterError, chunk_filename


def make_writer(tmp_path: Path, **overrides) -> ChunkWriter:
    config = ChunkConfig(
        max_rows=overrides.pop("max_rows", 3),
        max_uncompressed_bytes=overrides.pop("max_uncompressed_bytes", 10_000_000),
        compression_level=overrides.pop("compression_level", 3),
    )
    assert not overrides
    return ChunkWriter(tmp_path / "run", config)


def write_rows(writer: ChunkWriter, rows: int, payload: bytes = b'["x"]') -> None:
    for _ in range(rows):
        writer.write_row(payload)


def decompress_lines(path: Path) -> list[bytes]:
    dctx = zstandard.ZstdDecompressor()
    with open(path, "rb") as fh:
        text = dctx.stream_reader(fh).read()
    return text.splitlines()


class TestThresholds:
    def test_row_threshold_splits_chunks(self, tmp_path):
        writer = make_writer(tmp_path, max_rows=3)
        write_rows(writer, 7)
        chunks = writer.finish()
        assert [c.rows for c in chunks] == [3, 3, 1]
        assert [c.sequence for c in chunks] == [1, 2, 3]
        assert [c.file for c in chunks] == [
            "chunk-000001.jsonl.zst",
            "chunk-000002.jsonl.zst",
            "chunk-000003.jsonl.zst",
        ]

    def test_byte_threshold_splits_chunks(self, tmp_path):
        # 每行 7 字节 (["ab"] 6 字节 + 换行), 12 字节阈值:
        # 写入后检查, 第 2 行后达到 14 >= 12 → 封闭 (超出至多一行, 单行超限同理)
        writer = make_writer(tmp_path, max_rows=10_000, max_uncompressed_bytes=12)
        write_rows(writer, 5, b'["ab"]')
        chunks = writer.finish()
        assert [c.rows for c in chunks] == [2, 2, 1]
        assert [c.uncompressed_bytes for c in chunks] == [14, 14, 7]

    def test_single_row_exceeding_byte_threshold(self, tmp_path):
        """单行超过字节阈值: 允许单行超限 Chunk, 不死循环不丢数据。"""
        writer = make_writer(tmp_path, max_uncompressed_bytes=5)
        writer.write_row(b'["very-long-row-payload"]')
        chunks = writer.finish()
        assert len(chunks) == 1
        assert chunks[0].rows == 1
        assert chunks[0].uncompressed_bytes > 5

    def test_row_after_oversized_row_starts_new_chunk(self, tmp_path):
        writer = make_writer(tmp_path, max_uncompressed_bytes=5)
        writer.write_row(b'["very-long-row-payload"]')  # 超限 → 立即封闭
        writer.write_row(b'["s"]')
        chunks = writer.finish()
        assert [c.rows for c in chunks] == [1, 1]

    def test_zero_rows_produce_zero_chunks(self, tmp_path):
        writer = make_writer(tmp_path)
        assert writer.finish() == []
        assert list((tmp_path / "run").glob("chunk-*")) == []

    def test_finish_is_idempotent(self, tmp_path):
        writer = make_writer(tmp_path, max_rows=2)
        write_rows(writer, 3)
        first = writer.finish()
        assert writer.finish() == first


class TestAtomicity:
    def test_only_final_names_after_close(self, tmp_path):
        writer = make_writer(tmp_path, max_rows=2)
        write_rows(writer, 4)
        writer.finish()
        names = sorted(p.name for p in (tmp_path / "run").iterdir())
        assert names == ["chunk-000001.jsonl.zst", "chunk-000002.jsonl.zst"]

    def test_part_file_exists_while_open(self, tmp_path):
        """写入中的 Chunk 是 .part 文件, 不是有效数据。"""
        writer = make_writer(tmp_path, max_rows=100)
        writer.write_row(b"[1]")
        names = [p.name for p in (tmp_path / "run").iterdir()]
        assert names == ["chunk-000001.jsonl.zst.part"]
        assert not (tmp_path / "run" / chunk_filename(1)).exists()

    def test_close_without_open_chunk_raises(self, tmp_path):
        writer = make_writer(tmp_path)
        with pytest.raises(ChunkWriterError, match="no open chunk"):
            writer.close_current_chunk()


class TestContentIntegrity:
    def test_sha256_matches_final_file(self, tmp_path):
        writer = make_writer(tmp_path, max_rows=3)
        write_rows(writer, 4)
        chunks = writer.finish()
        for chunk in chunks:
            content = (tmp_path / "run" / chunk.file).read_bytes()
            assert chunk.sha256 == hashlib.sha256(content).hexdigest()
            assert chunk.compressed_bytes == len(content)

    def test_decompressed_content_is_correct_jsonl(self, tmp_path):
        writer = make_writer(tmp_path, max_rows=2)
        rows = [b"[1]", b'["two"]', b"[3, null]", '["中文"]'.encode()]
        for row in rows:
            writer.write_row(row)
        chunks = writer.finish()
        assert sum(c.rows for c in chunks) == 4

        recovered: list[bytes] = []
        for chunk in chunks:
            recovered.extend(decompress_lines(tmp_path / "run" / chunk.file))
        assert recovered == [row + b"" for row in rows]

    def test_uncompressed_bytes_count_includes_newlines(self, tmp_path):
        writer = make_writer(tmp_path, max_rows=10)
        payload = b'["abc"]'  # 7 字节 + 1 换行
        write_rows(writer, 2, payload)
        (chunk,) = writer.finish()
        assert chunk.uncompressed_bytes == 16

    def test_sequence_continuous_across_many_chunks(self, tmp_path):
        writer = make_writer(tmp_path, max_rows=1)
        write_rows(writer, 5)
        chunks = writer.finish()
        assert [c.sequence for c in chunks] == [1, 2, 3, 4, 5]

    def test_compression_level_configurable(self, tmp_path):
        """不同压缩级别都产生可解压的合法 zstd 文件。"""
        config = ChunkConfig(max_rows=100, max_uncompressed_bytes=10**6, compression_level=9)
        writer = ChunkWriter(tmp_path / "run", config)
        write_rows(writer, 10)
        (chunk,) = writer.finish()
        assert len(decompress_lines(tmp_path / "run" / chunk.file)) == 10
