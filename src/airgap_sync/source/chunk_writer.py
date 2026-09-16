"""Snapshot Chunk 写入器。

把流式编码后的 JSONL 行写入 zstd 压缩的 Chunk 文件:

* Chunk 同时受 max_rows / max_uncompressed_bytes 双阈值限制,
  任一达到阈值即封闭;
* 单行本身超过字节阈值时允许生成单行超限 Chunk (先写入再封闭),
  不丢数据也不死循环;
* 文件先写 <name>.part, 写完 (关闭压缩器 → flush → fsync → 关闭)
  后原子 rename 为最终文件名 —— 只有最终文件名代表完整 Chunk;
* SHA256 与字节数针对最终压缩文件字节流边写边计算, 不回读文件;
* 每个 Chunk 的 metadata 在封闭时立即返回, 供 Manifest 独立记录
  (Phase 3 起 Chunk 生成后即可上传并删除本地文件)。
"""

from __future__ import annotations

import hashlib
import logging
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import zstandard

from airgap_sync.common.fsutil import PART_SUFFIX, fsync_directory, retry_windows_file_lock
from airgap_sync.common.manifest import ChunkMeta
from airgap_sync.common.models import ChunkConfig

logger = logging.getLogger(__name__)

CHUNK_FILE_TEMPLATE = "chunk-{sequence:06d}.jsonl.zst"


class ChunkWriterError(Exception):
    """Chunk 写入失败。"""


def chunk_filename(sequence: int) -> str:
    """Chunk 最终文件名, 例如 chunk-000001.jsonl.zst。"""
    return CHUNK_FILE_TEMPLATE.format(sequence=sequence)


class _HashingFile:
    """写透文件对象: 转发写入并计算 SHA256 / 累计字节数。

    zstd 压缩器把最终压缩字节写到这里, 因此摘要统计的就是
    最终传输的压缩文件字节。
    """

    def __init__(self, fh: Any) -> None:
        self._fh = fh
        self._hash = hashlib.sha256()
        self.size = 0

    def write(self, data: bytes) -> int:  # type: ignore[override]
        self._hash.update(data)
        self.size += len(data)
        return self._fh.write(data)

    def flush(self) -> None:
        self._fh.flush()

    def fileno(self) -> int:
        return self._fh.fileno()

    def hexdigest(self) -> str:
        return self._hash.hexdigest()


class ChunkWriter:
    """把行流式切分成多个 zstd Chunk 文件。

    内存中只保留当前 Chunk 的统计值和已封闭 Chunk 的 metadata
    (每个数十字节), 与总行数无关。
    """

    def __init__(
        self,
        run_dir: Path,
        config: ChunkConfig,
        log_context: str = "",
        on_chunk_closed: Callable[[ChunkMeta], None] | None = None,
    ) -> None:
        self._run_dir = run_dir
        run_dir.mkdir(parents=True, exist_ok=True)
        self._config = config
        self._log_context = log_context
        self._on_chunk_closed = on_chunk_closed
        self._compressor_context = zstandard.ZstdCompressor(level=config.compression_level)
        self.chunks: list[ChunkMeta] = []
        self._current: _OpenChunk | None = None
        self._finished = False
        self._aborted = False

    def __enter__(self) -> ChunkWriter:
        return self

    def __exit__(self, *exc_info: object) -> None:
        # finish() 后没有打开的 Chunk, abort() 是安全空操作。异常时不吞掉原异常。
        self.abort()

    def write_row(self, encoded: bytes) -> None:
        """写入一行 (canonical JSON bytes, 不含换行)。

        写入后检查阈值: 达到任一阈值即封闭当前 Chunk。
        行本身超过字节阈值时仍会先完整写入, 封闭后形成单行超限 Chunk。
        """
        if self._finished:
            raise ChunkWriterError("cannot write after finish")
        if self._aborted:
            raise ChunkWriterError("cannot write after abort")
        if self._current is None:
            self._current = self._open_chunk()
        self._current.write_row(encoded)
        if (
            self._current.rows >= self._config.max_rows
            or self._current.uncompressed_bytes >= self._config.max_uncompressed_bytes
        ):
            self.close_current_chunk()

    def close_current_chunk(self) -> ChunkMeta:
        """封闭当前 Chunk 并原子落盘; 没有打开的 Chunk 时报错。"""
        if self._current is None:
            raise ChunkWriterError("no open chunk to close")
        meta = self._current.close()
        self._current = None
        self.chunks.append(meta)
        if self._on_chunk_closed is not None:
            self._on_chunk_closed(meta)
        return meta

    def finish(self) -> list[ChunkMeta]:
        """封闭最后一个 Chunk (如果还有), 返回全部 Chunk metadata。"""
        if self._aborted:
            raise ChunkWriterError("cannot finish an aborted chunk writer")
        if self._current is not None:
            self.close_current_chunk()
        self._finished = True
        return list(self.chunks)

    def abort(self) -> None:
        """关闭并丢弃当前未完成 Chunk; 已封闭 Chunk 保留。可重复调用。"""
        if self._aborted or self._finished:
            return
        self._aborted = True
        current, self._current = self._current, None
        if current is not None:
            current.abort()

    def _open_chunk(self) -> _OpenChunk:
        # 只在 _current 为 None 时调用, 序号 = 已封闭 Chunk 数 + 1
        return _OpenChunk(
            run_dir=self._run_dir,
            sequence=len(self.chunks) + 1,
            compressor_context=self._compressor_context,
            log_context=self._log_context,
        )


class _OpenChunk:
    """一个正在写入的 Chunk (.part 文件)。"""

    def __init__(
        self,
        run_dir: Path,
        sequence: int,
        compressor_context: zstandard.ZstdCompressor,
        log_context: str = "",
    ) -> None:
        self.run_dir = run_dir
        self.sequence = sequence
        self.rows = 0
        self.uncompressed_bytes = 0
        self._log_context = log_context
        self._final_path = run_dir / chunk_filename(sequence)
        self._part_path = run_dir / (self._final_path.name + PART_SUFFIX)
        # 文件在 close() (写完 → fsync → rename) 前保持打开,
        # 生命周期跨多个 write_row 调用, 不能用 with 包裹单次写入
        self._raw_fh = open(self._part_path, "wb")  # noqa: SIM115
        self._hashing = _HashingFile(self._raw_fh)
        try:
            self._compressor = compressor_context.stream_writer(self._hashing, closefd=False)
        except Exception:
            self._raw_fh.close()
            try:
                retry_windows_file_lock(
                    "unlink",
                    self._part_path,
                    lambda: self._part_path.unlink(missing_ok=True),
                )
            except OSError as exc:
                logger.warning("cannot remove failed chunk %s: %s", self._part_path, exc)
            raise
        self._aborted = False

    def write_row(self, encoded: bytes) -> None:
        line = encoded + b"\n"
        try:
            self._compressor.write(line)
        except (OSError, zstandard.ZstdError) as exc:
            raise ChunkWriterError(f"cannot write chunk {self._final_path.name}: {exc}") from exc
        self.rows += 1
        self.uncompressed_bytes += len(line)

    def close(self) -> ChunkMeta:
        """关闭压缩器与文件, 原子 rename, 返回 Chunk metadata。"""
        try:
            self._compressor.close()  # 结束 zstd frame, flush 全部压缩字节
            self._hashing.flush()
            os.fsync(self._raw_fh.fileno())
        except (OSError, zstandard.ZstdError) as exc:
            raise ChunkWriterError(f"cannot finalize chunk {self._final_path.name}: {exc}") from exc
        finally:
            self._raw_fh.close()
        try:
            retry_windows_file_lock(
                "replace",
                self._part_path,
                lambda: os.replace(self._part_path, self._final_path),
            )
        except OSError as exc:
            raise ChunkWriterError(f"cannot finalize chunk {self._final_path.name}: {exc}") from exc
        fsync_directory(self.run_dir)
        meta = ChunkMeta(
            sequence=self.sequence,
            file=self._final_path.name,
            rows=self.rows,
            uncompressed_bytes=self.uncompressed_bytes,
            compressed_bytes=self._hashing.size,
            sha256=self._hashing.hexdigest(),
        )
        logger.info(
            "chunk closed:%s sequence=%d rows=%d uncompressed_bytes=%d compressed_bytes=%d",
            self._log_context,
            meta.sequence,
            meta.rows,
            meta.uncompressed_bytes,
            meta.compressed_bytes,
        )
        return meta

    def abort(self) -> None:
        """Best-effort 关闭所有句柄并删除 .part; 从不 rename。"""
        if self._aborted:
            return
        self._aborted = True
        try:
            self._compressor.close()
        except Exception as exc:
            logger.warning("cannot close aborted chunk %s: %s", self._part_path, exc)
        finally:
            try:
                self._raw_fh.close()
            except Exception as exc:
                logger.warning(
                    "cannot close raw file for aborted chunk %s: %s", self._part_path, exc
                )
        try:
            retry_windows_file_lock(
                "unlink",
                self._part_path,
                lambda: self._part_path.unlink(missing_ok=True),
            )
        except OSError as exc:
            logger.warning("cannot remove aborted chunk %s: %s", self._part_path, exc)
