"""Snapshot 行编码 / 解码 (Row Codec)。

V1 数据文件格式是 JSON Lines: 数据库一行 → JSON array 一行
(列顺序 = SELECT * 返回顺序, 列名记录在 manifest, 不在每行重复)。

MySQL 值不能直接 json.dumps, 因为需要:

* NULL 与空字符串严格区分;
* bytes / binary 使用 Base64 安全表达;
* Decimal 不转成 float (不丢精度);
* datetime / date / time / timedelta 不丢精度;
* 编码 deterministic: 同一份数据始终编码为完全相同的 bytes。

类型映射 (tag 以 "$" 开头, 只出现在对象位置, 与普通数据不混淆):

    MySQL/PyMySQL 值        JSON 表达
    ---------------------  --------------------------------
    None                   null
    bool                   true / false
    int                    数字 (任意精度)
    float (有限)           数字
    float (inf/nan)        {"$float": "inf" | "-inf" | "nan"}
    str                    字符串 (Unicode 原样)
    Decimal                {"$decimal": "123.4500"}
    bytes / bytearray      {"$bytes": "<base64>"}
    date                   {"$date": "2026-09-14"}
    datetime               {"$datetime": "2026-09-14T20:00:00.123456"}
    time                   {"$time": "12:34:56.000001"}
    timedelta              {"$timedelta": "<days>:<seconds>:<microseconds>"}

timedelta 是 PyMySQL 对 TIME 列的默认返回类型; 三分量编码的是
timedelta 规范化后的 days (可为负) / seconds (0-86399) /
microseconds (0-999999), 解码按相同分量重建, 精确无损。

禁止使用 pickle。Source 编码与 Destination 解码共用本模块。
"""

from __future__ import annotations

import base64
import binascii
import json
import math
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

TAG_BYTES = "$bytes"
TAG_DECIMAL = "$decimal"
TAG_FLOAT = "$float"
TAG_DATE = "$date"
TAG_DATETIME = "$datetime"
TAG_TIME = "$time"
TAG_TIMEDELTA = "$timedelta"

# 非有限 float 的确定性命名 (JSON 数字不能表达 inf/nan)。
# 注意不能以 float 值作 dict key: NaN != NaN, 不同 NaN 对象会查找失败。


class RowCodecError(Exception):
    """行编码或解码失败。"""


def encode_row(values: Any) -> bytes:
    """把一行数据库值编码为 canonical JSON bytes (不含换行符)。

    deterministic: 相同的值列表始终产生完全相同的 bytes。
    不支持的类型抛出 RowCodecError, 不做静默转换。
    """
    try:
        encoded = [_encode_value(value) for value in values]
    except TypeError as exc:
        raise RowCodecError(f"row is not iterable: {type(values).__name__}") from exc
    return json.dumps(
        encoded,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def decode_row(data: bytes) -> list[Any]:
    """把 encode_row 的输出还原为 Python 值列表 (无损 round-trip)。"""
    try:
        values = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RowCodecError(f"invalid encoded row: {exc}") from exc
    if not isinstance(values, list):
        raise RowCodecError(f"encoded row must be a JSON array, got {type(values).__name__}")
    return [_decode_value(value) for value in values]


def _encode_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        # bool 是 int 的子类已在前面的分支处理; 非有限 float 不能作为 JSON 数字
        if math.isnan(value):
            return {TAG_FLOAT: "nan"}
        if value == math.inf:
            return {TAG_FLOAT: "inf"}
        if value == -math.inf:
            return {TAG_FLOAT: "-inf"}
        return value
    if isinstance(value, Decimal):
        return {TAG_DECIMAL: str(value)}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {TAG_BYTES: base64.b64encode(bytes(value)).decode("ascii")}
    # datetime 是 date 的子类, 必须先判断
    if isinstance(value, datetime):
        return {TAG_DATETIME: value.isoformat()}
    if isinstance(value, date):
        return {TAG_DATE: value.isoformat()}
    if isinstance(value, time):
        return {TAG_TIME: value.isoformat()}
    if isinstance(value, timedelta):
        return {TAG_TIMEDELTA: f"{value.days}:{value.seconds}:{value.microseconds}"}
    raise RowCodecError(f"unsupported value type for row codec: {type(value).__name__}")


def _decode_value(value: Any) -> Any:
    if not isinstance(value, dict):
        # None / bool / int / float / str 原样返回
        return value
    if len(value) != 1:
        raise RowCodecError(f"encoded value object must have exactly one key, got {sorted(value)}")
    tag, raw = next(iter(value.items()))
    if tag == TAG_BYTES:
        try:
            return base64.b64decode(str(raw), validate=True)
        except (binascii.Error, ValueError) as exc:
            raise RowCodecError(f"invalid $bytes payload: {exc}") from exc
    if tag == TAG_DECIMAL:
        return Decimal(str(raw))
    if tag == TAG_FLOAT:
        mapping = {"inf": math.inf, "-inf": -math.inf, "nan": math.nan}
        if raw not in mapping:
            raise RowCodecError(f"invalid $float payload: {raw!r}")
        return mapping[raw]
    if tag == TAG_DATETIME:
        return _parse_iso(raw, TAG_DATETIME, datetime.fromisoformat)
    if tag == TAG_DATE:
        return _parse_iso(raw, TAG_DATE, date.fromisoformat)
    if tag == TAG_TIME:
        return _parse_iso(raw, TAG_TIME, time.fromisoformat)
    if tag == TAG_TIMEDELTA:
        parts = str(raw).split(":")
        if len(parts) != 3:
            raise RowCodecError(f"invalid $timedelta payload: {raw!r}")
        try:
            days, seconds, microseconds = (int(part) for part in parts)
        except ValueError as exc:
            raise RowCodecError(f"invalid $timedelta payload: {raw!r}") from exc
        return timedelta(days=days, seconds=seconds, microseconds=microseconds)
    raise RowCodecError(f"unknown row codec tag: {tag!r}")


def _parse_iso(raw: Any, tag: str, parser: Any) -> Any:
    try:
        return parser(str(raw))
    except ValueError as exc:
        raise RowCodecError(f"invalid {tag} payload: {raw!r}") from exc
