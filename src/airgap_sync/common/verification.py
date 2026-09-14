"""Snapshot Multiset Digest: 与行顺序无关、对重复行敏感的验证摘要。

为什么需要这样的摘要:

* 表可能没有主键、有重复行、行顺序不稳定;
* 简单的 SHA256(row1 + row2 + ...) 依赖行顺序, Destination 按不同
  顺序读取同样数据会得到不同结果;
* 简单 XOR 会被偶数个相同行互相抵消 (A ⊕ A = 0), 无法区分
  "出现 2 次" 和 "不出现"。

算法 (multiset_digest_v1):

    row_bytes = Row Codec 的 canonical 编码 (不含换行)

    h1 = SHA256(domain_a || row_bytes)
    h2 = SHA256(domain_b || row_bytes)

    digest_a = Σ int(h1) mod 2^256
    digest_b = Σ int(h2) mod 2^256

    summary = (row_count, hex(digest_a), hex(digest_b))

性质:

* 模 2^256 加法满足交换律 → 行顺序无关;
* 每行贡献一个固定的加法项 → 两条相同行贡献两份 (multiplicity-sensitive);
* 两个独立的 domain 分隔 → 独立累加两条摘要, 显著提高构造碰撞的难度;
* 空表得到确定结果 (全零 digest + row_count = 0);
* 每行只需两次 SHA256 与一个大整数加法 → 流式处理, 内存 O(1),
  与总行数无关。

注意: 这不是数据库主键 Hash, 只是 Snapshot Multiset Digest。
Source 生成 Snapshot 时计算, Destination 导入后用完全相同实现复算比对。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

DIGEST_ALGORITHM = "multiset_digest_v1"

# 固定的域分隔常量 (domain separation): 让两条摘要相互独立。
_DOMAIN_A = b"airgap-sync/multiset-digest/a/v1"
_DOMAIN_B = b"airgap-sync/multiset-digest/b/v1"

_MODULUS = 1 << 256

_HEX_WIDTH = 64


@dataclass(frozen=True)
class VerificationSummary:
    """一次 Snapshot 的验证摘要。"""

    algorithm: str
    row_count: int
    digest_a: str
    digest_b: str


class MultisetDigest:
    """流式累加器: 逐行 update, 最后 summary。

    保存的全量状态只有两个 256-bit 累加值和行计数, 与总行数无关。
    """

    def __init__(self) -> None:
        self._sum_a = 0
        self._sum_b = 0
        self.row_count = 0

    def update(self, row_bytes: bytes) -> None:
        """纳入一行的 canonical 编码字节 (encode_row 的输出)。"""
        digest_a = hashlib.sha256(_DOMAIN_A + row_bytes).digest()
        digest_b = hashlib.sha256(_DOMAIN_B + row_bytes).digest()
        self._sum_a = (self._sum_a + int.from_bytes(digest_a, "big")) % _MODULUS
        self._sum_b = (self._sum_b + int.from_bytes(digest_b, "big")) % _MODULUS
        self.row_count += 1

    def update_rows(self, rows_bytes: list[bytes]) -> None:
        """批量纳入多行 (等价于逐行 update)。"""
        for row_bytes in rows_bytes:
            self.update(row_bytes)

    def summary(self) -> VerificationSummary:
        """返回当前摘要 (固定 64 位 hex, 空表为全零)。"""
        return VerificationSummary(
            algorithm=DIGEST_ALGORITHM,
            row_count=self.row_count,
            digest_a=f"{self._sum_a:0{_HEX_WIDTH}x}",
            digest_b=f"{self._sum_b:0{_HEX_WIDTH}x}",
        )
