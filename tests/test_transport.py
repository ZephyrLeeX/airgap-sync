from __future__ import annotations

import pytest

from airgap_sync.common.transport import (
    TransportFilenameError,
    parse_transport_filename,
    transport_filename,
)

RUN = "20260915T030000Z-a1b2c3d4"


@pytest.mark.parametrize("logical", ["chunk-000001.jsonl.zst", "schema.sql", "manifest.json"])
def test_transport_round_trip(logical):
    name = transport_filename(RUN, logical)
    assert name == f"airgap-v1--{RUN}--{logical}"
    assert parse_transport_filename(name) == (RUN, logical)
    assert "/" not in name and "\\" not in name


def test_different_runs_are_different():
    other = "20260915T030001Z-ffffffff"
    assert transport_filename(RUN, "schema.sql") != transport_filename(other, "schema.sql")


@pytest.mark.parametrize(
    "logical", ["../schema.sql", "x/schema.sql", "chunk-1.jsonl.zst", "anything"]
)
def test_rejects_arbitrary_logical_names(logical):
    with pytest.raises(TransportFilenameError):
        transport_filename(RUN, logical)


@pytest.mark.parametrize("name", ["x", "airgap-v1--bad--schema.sql", "../manifest.json"])
def test_parser_rejects_invalid_names(name):
    with pytest.raises(TransportFilenameError):
        parse_transport_filename(name)
