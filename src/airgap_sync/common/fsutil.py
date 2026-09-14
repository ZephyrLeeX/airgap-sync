"""文件原子写入辅助。

Snapshot 的所有最终文件 (chunk / schema.sql / manifest.json) 都必须:
写入 .part 临时文件 → flush → fsync → 原子 rename 到最终文件名。

只有最终文件名代表完整数据; .part 不是有效数据。
"""

from __future__ import annotations

import os
from pathlib import Path

PART_SUFFIX = ".part"


def fsync_directory(path: Path) -> None:
    """尽力持久化目录项 (rename 的持久性依赖目录 fsync)。

    Windows 不支持以 O_RDONLY 打开目录, 打开失败时静默跳过;
    这属于 best-effort 增强, 不影响正确性 (manifest 的存在性
    由 rename 的原子性保证)。
    """
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """把字节原子写入 path: 先写 <name>.part, fsync 后 rename。

    适用于一次性生成的小文件 (schema.sql / manifest.json)。
    """
    part = path.with_name(path.name + PART_SUFFIX)
    with open(part, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(part, path)
    fsync_directory(path.parent)
