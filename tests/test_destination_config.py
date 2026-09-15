from __future__ import annotations

from pathlib import Path

import pytest

from airgap_sync.common.config import ConfigError, load_config


def destination_config(tmp_path: Path) -> dict:
    return {
        "role": "destination",
        "mysql": {
            "host": "127.0.0.1",
            "database": "target_db",
            "user": "sync",
            "password_env": "DEST_PASSWORD",
        },
        "destination": {"incoming_dir": str(tmp_path / "incoming")},
    }


def test_destination_allows_omitted_tables_and_paths(tmp_path, write_config):
    config = load_config(write_config(destination_config(tmp_path)))
    assert config.tables == []
    assert config.paths is None
    assert config.destination is not None
    assert config.destination.metadata_database == "airgap_sync_meta"
    assert config.destination.insert_batch_rows == 1000
    assert config.destination.settle_seconds == 2


def test_destination_allows_explicit_empty_tables(tmp_path, write_config):
    data = destination_config(tmp_path)
    data["tables"] = []
    assert load_config(write_config(data)).tables == []


@pytest.mark.parametrize("tables", [None, []], ids=["omitted", "empty"])
def test_source_still_requires_tables(config_data, write_config, tables):
    if tables is None:
        config_data.pop("tables")
    else:
        config_data["tables"] = tables
    with pytest.raises(ConfigError, match="at least one table"):
        load_config(write_config(config_data))


def test_destination_parameters_are_validated(tmp_path, write_config):
    data = destination_config(tmp_path)
    data["destination"]["insert_batch_rows"] = 0
    with pytest.raises(ConfigError, match="insert_batch_rows"):
        load_config(write_config(data))
