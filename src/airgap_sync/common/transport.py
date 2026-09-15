"""Relay 扁平命名空间中的安全 transport filename。"""

from __future__ import annotations

import re

PREFIX = "airgap-v1"
_RUN_ID_RE = re.compile(r"^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}$")
_LOGICAL_RE = re.compile(r"^(?:chunk-[0-9]{6}\.jsonl\.zst|schema\.sql|manifest\.json)$")
_TRANSPORT_RE = re.compile(
    rf"^{PREFIX}--(?P<run_id>[0-9]{{8}}T[0-9]{{6}}Z-[0-9a-f]{{8}})--"
    r"(?P<logical_name>chunk-[0-9]{6}\.jsonl\.zst|schema\.sql|manifest\.json)$"
)


class TransportFilenameError(ValueError):
    """Run ID、逻辑名或 transport filename 不符合协议。"""


def transport_filename(run_id: str, logical_name: str) -> str:
    if not _RUN_ID_RE.fullmatch(run_id):
        raise TransportFilenameError(f"invalid run_id: {run_id!r}")
    if not _LOGICAL_RE.fullmatch(logical_name):
        raise TransportFilenameError(f"invalid logical artifact name: {logical_name!r}")
    return f"{PREFIX}--{run_id}--{logical_name}"


def parse_transport_filename(filename: str) -> tuple[str, str]:
    match = _TRANSPORT_RE.fullmatch(filename)
    if match is None:
        raise TransportFilenameError(f"invalid transport filename: {filename!r}")
    return match.group("run_id"), match.group("logical_name")
