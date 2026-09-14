"""Row Codec 测试: 类型映射、round-trip、determinism。"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

import pytest

from airgap_sync.common.row_codec import RowCodecError, decode_row, encode_row

# 覆盖 Source MySQL 常见类型的 round-trip 样本。
ROUND_TRIP_ROWS = [
    # 纯 NULL / 空串 / 数字边界
    [None, None, None],
    ["", "", ""],
    [0, -1, 2**63 - 1, -(2**63)],
    [1, -1, 0],
    # 字符串
    ["abc", "中文身份证字段", "line\nbreak\ttab", "\x00\x01binary-ish str", "a" * 100_000],
    # float
    [3.14, -2.5, 0.0, -0.0, 1.5e300, 1e-10, 2.0],
    # Decimal (不转 float, 保留精度与标度)
    [Decimal("123.4500"), Decimal("-0.000001"), Decimal("99999999999999999999.99")],
    # bytes / binary
    [b"", b"\x00\x01\x02\xff", bytes(range(256))],
    # 日期时间
    [date(2026, 9, 14), date(1, 1, 1), date(9999, 12, 31)],
    [
        datetime(2026, 9, 14, 20, 0, 0, 123456),
        datetime(2026, 9, 14, 0, 0, 0),
        datetime(1, 1, 1, 0, 0, 0),
        datetime(2026, 12, 31, 23, 59, 59, 999999),
    ],
    [time(12, 34, 56, 1), time(0, 0, 0), time(23, 59, 59, 999999)],
    [timedelta(0), timedelta(days=1), timedelta(hours=-1), timedelta(microseconds=-5)],
    # 混合一行
    [
        1,
        None,
        "",
        "0",
        Decimal("3.14"),
        b"\x00",
        datetime(2026, 9, 14, 20, 0, 0),
        "混合 mixed",
    ],
]

# 非有限 float (JSON 数字不能表达)
NON_FINITE_FLOATS = [float("inf"), float("-inf"), float("nan")]


class TestRoundTrip:
    @pytest.mark.parametrize(
        "row",
        ROUND_TRIP_ROWS,
        ids=lambda row: f"row[{type(row[0]).__name__}]",
    )
    def test_round_trip(self, row):
        decoded = decode_row(encode_row(row))
        assert decoded == row
        assert [type(a) for a in decoded] == [type(b) for b in row]

    @pytest.mark.parametrize("value", NON_FINITE_FLOATS, ids=["inf", "-inf", "nan"])
    def test_non_finite_float_round_trip(self, value):
        row = [value]
        decoded = decode_row(encode_row(row))
        assert len(decoded) == 1
        result = decoded[0]
        assert isinstance(result, float)
        if value != value:  # NaN
            assert result != result
        else:
            assert result == value

    def test_decimal_scale_preserved(self):
        """Decimal('123.4500') 不能变成 123.45 或 123.45(float)。"""
        decoded = decode_row(encode_row([Decimal("123.4500")]))
        value = decoded[0]
        assert isinstance(value, Decimal)
        assert str(value) == "123.4500"

    def test_bytes_decodes_to_bytes(self):
        decoded = decode_row(encode_row([b"\x00\x01"]))
        assert decoded[0] == b"\x00\x01"
        assert isinstance(decoded[0], bytes)

    def test_bytearray_decodes_to_bytes(self):
        """bytearray 可编码 (等价于同内容 bytes), 解码统一返回 bytes。"""
        assert decode_row(encode_row([bytearray(b"\xde\xad")])) == [b"\xde\xad"]
        assert encode_row([bytearray(b"\xde\xad")]) == encode_row([b"\xde\xad"])

    def test_timedelta_negative_round_trip(self):
        """负 timedelta 的规范化三分量必须精确还原。"""
        value = timedelta(hours=-1)  # days=-1, seconds=82800
        decoded = decode_row(encode_row([value]))
        assert decoded[0] == value
        assert decoded[0].days == -1
        assert decoded[0].seconds == 82800

    def test_datetime_with_timezone(self):
        """带时区的 datetime (MySQL 不产生, 但编码必须无损)。"""
        value = datetime(2026, 9, 14, 20, 0, 0, tzinfo=UTC)
        decoded = decode_row(encode_row([value]))
        assert decoded[0] == value


class TestDistinctness:
    """语义区分: NULL != "", 0 != "0", int != float 等。"""

    def test_null_and_empty_string(self):
        assert encode_row([None]) != encode_row([""])

    def test_zero_int_and_zero_string(self):
        assert encode_row([0]) != encode_row(["0"])

    def test_int_and_float_encoding_differ(self):
        assert encode_row([1]) != encode_row([1.0])

    def test_empty_string_and_string_null_text(self):
        assert encode_row([""]) != encode_row(["null"])

    def test_bytes_and_similar_string(self):
        assert encode_row([b"abc"]) != encode_row(["abc"])

    def test_empty_row_allowed(self):
        assert decode_row(encode_row([])) == []


class TestDeterminism:
    def test_same_data_same_bytes(self):
        row = [1, "a", None, Decimal("1.5"), b"\x00", datetime(2026, 1, 1)]
        assert encode_row(row) == encode_row(list(row))

    def test_unicode_not_escaped(self):
        """ensure_ascii=False: 中文原样 UTF-8 输出 (而不是 \\uXXXX)。"""
        encoded = encode_row(["中文"])
        assert "中文".encode() in encoded

    def test_key_order_fixed(self):
        assert encode_row(["x"]) == encode_row(["x"])


class TestTagFormat:
    def test_tag_documented_in_payload(self):
        """tag 对象是单键 dict, 键以 $ 开头。"""
        payload = encode_row([Decimal("1"), b"a", date(2026, 1, 1)])
        assert b'"$decimal"' in payload
        assert b'"$bytes"' in payload
        assert b'"$date"' in payload

    def test_line_is_compact_json_array(self):
        encoded = encode_row([1, "a", None])
        assert encoded.startswith(b"[")
        assert encoded.endswith(b"]")
        assert b'": ' not in encoded  # separators 无空格
        assert not encoded.endswith(b"\n")  # 换行由 ChunkWriter 追加


class TestErrors:
    def test_unsupported_type_rejected(self):
        with pytest.raises(RowCodecError, match="unsupported value type"):
            encode_row([object()])

    def test_decode_invalid_json(self):
        with pytest.raises(RowCodecError, match="invalid encoded row"):
            decode_row(b"{not json")

    def test_decode_non_array(self):
        with pytest.raises(RowCodecError, match="JSON array"):
            decode_row(b'{"a": 1}')

    def test_decode_unknown_tag(self):
        with pytest.raises(RowCodecError, match="unknown row codec tag"):
            decode_row(b'[{"$nope": 1}]')

    def test_decode_multi_key_tag_object(self):
        with pytest.raises(RowCodecError, match="exactly one key"):
            decode_row(b'[{"$date": "2026-01-01", "$bytes": "AA=="}]')

    def test_decode_invalid_tag_payload(self):
        with pytest.raises(RowCodecError):
            decode_row(b'[{"$date": "not-a-date"}]')
        with pytest.raises(RowCodecError):
            decode_row(b'[{"$timedelta": "1:2"}]')
        with pytest.raises(RowCodecError):
            decode_row(b'[{"$float": "weird"}]')
        with pytest.raises(RowCodecError):
            decode_row(b'[{"$bytes": "!!not base64!!"}]')

    def test_decode_non_iterable_row(self):
        with pytest.raises(RowCodecError, match="not iterable"):
            encode_row(42)
