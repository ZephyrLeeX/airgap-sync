"""Snapshot Multiset Digest 测试。"""

from __future__ import annotations

import pytest

from airgap_sync.common.verification import (
    DIGEST_ALGORITHM,
    MultisetDigest,
    VerificationSummary,
)

ROW_A = b'{"a": 1}'
ROW_B = b'["b", 2]'
ROW_C = b'[null, "c"]'
ROW_D = b'["d", {"$bytes": "AA=="}]'


def digest_of(*rows: bytes) -> VerificationSummary:
    digest = MultisetDigest()
    digest.update_rows(list(rows))
    return digest.summary()


class TestOrderIndependence:
    def test_same_data_different_order_same_digest(self):
        assert digest_of(ROW_A, ROW_B, ROW_C) == digest_of(ROW_C, ROW_A, ROW_B)
        assert digest_of(ROW_A, ROW_B, ROW_C) == digest_of(ROW_B, ROW_C, ROW_A)

    def test_permutation_of_many_rows(self):
        rows = [f"row-{i}".encode() for i in range(100)]
        shuffled = list(reversed(rows))
        assert digest_of(*rows) == digest_of(*shuffled)

    def test_row_count_constant(self):
        assert digest_of(ROW_A, ROW_B, ROW_C).row_count == 3


class TestMultiplicity:
    def test_duplicate_count_matters(self):
        assert digest_of(ROW_A, ROW_A, ROW_B) != digest_of(ROW_A, ROW_B)

    def test_even_duplicates_do_not_cancel(self):
        """XOR 摘要会让 A⊕A = 0 抵消; 模加摘要必须区分。"""
        assert digest_of(ROW_A, ROW_A) != digest_of()
        assert digest_of(ROW_A, ROW_A) != digest_of(ROW_A, ROW_A, ROW_A, ROW_A)

    def test_two_identical_rows_differ_from_one(self):
        assert digest_of(ROW_A) != digest_of(ROW_A, ROW_A)


class TestSensitivity:
    def test_any_row_change_detected(self):
        base = [ROW_A, ROW_B, ROW_C]
        for i in range(len(base)):
            changed = list(base)
            changed[i] = ROW_D
            assert digest_of(*base) != digest_of(*changed)

    def test_single_bit_change_detected(self):
        assert digest_of(b"[1]") != digest_of(b"[2]")
        assert digest_of(b'["a"]') != digest_of(b'["b"]')

    def test_two_digests_are_independent(self):
        """digest_a 与 digest_b 来自不同 domain, 不相等。"""
        summary = digest_of(ROW_A)
        assert summary.digest_a != summary.digest_b


class TestEmpty:
    def test_empty_table_has_deterministic_result(self):
        empty = digest_of()
        assert empty.row_count == 0
        assert empty.digest_a == "0" * 64
        assert empty.digest_b == "0" * 64
        assert digest_of() == empty  # 确定性


class TestFormat:
    def test_digest_is_64_hex_chars(self):
        summary = digest_of(ROW_A, ROW_B)
        assert len(summary.digest_a) == 64
        assert len(summary.digest_b) == 64
        int(summary.digest_a, 16)  # 合法 hex
        int(summary.digest_b, 16)

    def test_algorithm_tag(self):
        assert digest_of().algorithm == DIGEST_ALGORITHM

    def test_incremental_equals_batch(self):
        """逐行 update 与 update_rows 等价。"""
        incremental = MultisetDigest()
        for row in (ROW_A, ROW_B, ROW_C):
            incremental.update(row)
        assert incremental.summary() == digest_of(ROW_A, ROW_B, ROW_C)


class TestIndependenceFromCodecNewlines:
    def test_row_bytes_with_and_without_trailing_newline_differ(self):
        """摘要是精确字节级比较: 换行差异必须可检测。"""
        assert digest_of(b"[1]") != digest_of(b"[1]\n")


@pytest.mark.parametrize(
    "rows_a,rows_b",
    [
        ([ROW_A, ROW_B], [ROW_B, ROW_A]),  # 顺序无关 → 相同
        ([ROW_A, ROW_A, ROW_B], [ROW_A, ROW_B]),  # 多重度敏感 → 不同
        ([ROW_A, ROW_B], [ROW_A, ROW_C]),  # 内容敏感 → 不同
        ([], [ROW_A]),  # 空集与非空集 → 不同
    ],
    ids=["order", "multiplicity", "content", "empty-vs-nonempty"],
)
def test_multiset_semantics(rows_a, rows_b):
    """组合性质: multiset 相等 ⟺ 摘要相等 (在测试样本范围内)。"""
    expected_equal = sorted(rows_a) == sorted(rows_b)
    assert (digest_of(*rows_a) == digest_of(*rows_b)) is expected_equal
