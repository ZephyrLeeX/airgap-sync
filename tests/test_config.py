"""配置加载与校验测试。"""

from __future__ import annotations

from pathlib import Path

import pytest

from airgap_sync.common.config import ConfigError, load_config, resolve_password
from airgap_sync.common.models import AppConfig, MySQLConfig
from conftest import PASSWORD_VALUE


class TestValidConfigs:
    def test_minimal_table_config(self, config_data, write_config, password_env):
        config = load_config(write_config(config_data))
        assert isinstance(config, AppConfig)
        table = config.tables[0]
        assert table.name == "t_snapshot"
        assert table.enabled is True

    def test_disabled_table(self, config_data, write_config, password_env):
        config_data["tables"].append({"name": "t_off", "enabled": False})
        config = load_config(write_config(config_data))
        assert [t.name for t in config.enabled_tables] == ["t_snapshot"]

    def test_defaults(self, config_data, write_config, password_env):
        del config_data["mysql"]["port"]
        del config_data["mysql"]["connect_timeout"]
        config = load_config(write_config(config_data))
        assert config.mysql.port == 3306
        assert config.mysql.connect_timeout == 10
        assert config.snapshot.fetch_size == 2000
        assert config.chunk.max_rows == 50_000
        assert config.chunk.max_uncompressed_bytes == 64 * 1024 * 1024
        assert config.chunk.compression_level == 3

    def test_snapshot_and_chunk_overrides(self, config_data, write_config, password_env):
        config_data["snapshot"] = {"fetch_size": 500}
        config_data["chunk"] = {
            "max_rows": 1000,
            "max_uncompressed_bytes": 1048576,
            "compression_level": 7,
        }
        config = load_config(write_config(config_data))
        assert config.snapshot.fetch_size == 500
        assert config.chunk.max_rows == 1000
        assert config.chunk.max_uncompressed_bytes == 1048576
        assert config.chunk.compression_level == 7

    def test_windows_style_data_dir(self, config_data, write_config, password_env):
        config_data["paths"]["data_dir"] = "D:/airgap-sync/data"
        config = load_config(write_config(config_data))
        assert config.paths.data_dir == Path("D:/airgap-sync/data")

    def test_lookup_table(self, config_data, write_config, password_env):
        config = load_config(write_config(config_data))
        assert config.table("t_snapshot") is not None
        assert config.table("nope") is None


class TestLegacyDiffConfigRejected:
    """旧的 keyed / row_multiset 增量配置必须明确失败, 避免用户误以为仍然有效。"""

    def test_mode_field_rejected(self, config_data, write_config, password_env):
        config_data["tables"] = [{"name": "t_old", "mode": "keyed"}]
        with pytest.raises(ConfigError, match="tables.0.mode"):
            load_config(write_config(config_data))

    def test_key_field_rejected(self, config_data, write_config, password_env):
        config_data["tables"] = [{"name": "t_old", "key": ["id"]}]
        with pytest.raises(ConfigError, match="tables.0.key"):
            load_config(write_config(config_data))

    def test_row_multiset_mode_rejected(self, config_data, write_config, password_env):
        config_data["tables"] = [{"name": "t_old", "mode": "row_multiset"}]
        with pytest.raises(ConfigError, match="tables.0.mode"):
            load_config(write_config(config_data))

    def test_error_mentions_extra_field_not_allowed(self, config_data, write_config, password_env):
        config_data["tables"] = [{"name": "t_old", "mode": "keyed", "key": ["id"]}]
        with pytest.raises(ConfigError) as excinfo:
            load_config(write_config(config_data))
        assert "Extra inputs" in str(excinfo.value) or "extra" in str(excinfo.value).lower()


class TestInvalidConfigs:
    @pytest.mark.parametrize(
        "mutation",
        [
            lambda d: d["mysql"].pop("host"),
            lambda d: d["mysql"].pop("password_env"),
            lambda d: d.pop("mysql"),
            lambda d: d.pop("paths"),
            lambda d: d.pop("tables"),
            lambda d: d.update(tables=[]),
            lambda d: d.update(role="agent"),
            lambda d: d["mysql"].update(port="not-a-port"),
        ],
        ids=[
            "no-host",
            "no-password-env",
            "no-mysql",
            "no-paths",
            "no-tables",
            "empty-tables",
            "bad-role",
            "bad-port",
        ],
    )
    def test_missing_or_invalid_required_fields(
        self, config_data, write_config, password_env, mutation
    ):
        mutation(config_data)
        with pytest.raises(ConfigError):
            load_config(write_config(config_data))

    def test_duplicate_table_names(self, config_data, write_config, password_env):
        config_data["tables"].append({"name": "t_snapshot"})
        with pytest.raises(ConfigError, match="duplicate table names"):
            load_config(write_config(config_data))

    def test_unknown_top_level_field(self, config_data, write_config, password_env):
        config_data["redis"] = {"url": "redis://localhost"}
        with pytest.raises(ConfigError, match="redis"):
            load_config(write_config(config_data))

    def test_blank_table_name(self, config_data, write_config, password_env):
        config_data["tables"] = [{"name": "  "}]
        with pytest.raises(ConfigError, match="tables.0.*blank"):
            load_config(write_config(config_data))

    @pytest.mark.parametrize(
        "snapshot_mutation",
        [
            lambda d: d.update(snapshot={"fetch_size": 0}),
            lambda d: d.update(snapshot={"fetch_size": -1}),
            lambda d: d.update(snapshot={"batch": 100}),
        ],
        ids=["fetch-size-zero", "fetch-size-negative", "snapshot-unknown-field"],
    )
    def test_invalid_snapshot_config(
        self, config_data, write_config, password_env, snapshot_mutation
    ):
        snapshot_mutation(config_data)
        with pytest.raises(ConfigError):
            load_config(write_config(config_data))

    @pytest.mark.parametrize(
        "chunk_mutation",
        [
            lambda d: d.update(chunk={"max_rows": 0}),
            lambda d: d.update(chunk={"max_uncompressed_bytes": 0}),
            lambda d: d.update(chunk={"compression_level": 0}),
            lambda d: d.update(chunk={"compression_level": 20}),
            lambda d: d.update(chunk={"level": 3}),
        ],
        ids=[
            "max-rows-zero",
            "max-bytes-zero",
            "level-too-low",
            "level-too-high",
            "chunk-unknown-field",
        ],
    )
    def test_invalid_chunk_config(self, config_data, write_config, password_env, chunk_mutation):
        chunk_mutation(config_data)
        with pytest.raises(ConfigError):
            load_config(write_config(config_data))


class TestConfigFileErrors:
    def test_file_not_found(self, tmp_path):
        with pytest.raises(ConfigError, match="not found"):
            load_config(tmp_path / "missing.yaml")

    def test_invalid_yaml_syntax(self, tmp_path):
        path = tmp_path / "broken.yaml"
        path.write_text("role: [unclosed", encoding="utf-8")
        with pytest.raises(ConfigError, match="invalid YAML"):
            load_config(path)

    def test_root_not_mapping(self, tmp_path):
        path = tmp_path / "list.yaml"
        path.write_text("- a\n- b\n", encoding="utf-8")
        with pytest.raises(ConfigError, match="mapping"):
            load_config(path)


class TestPassword:
    def test_resolve_password(self, config_data, password_env):
        assert resolve_password(load_mysql(config_data, password_env)) == PASSWORD_VALUE

    def test_missing_env_var_fails(self, config_data, monkeypatch):
        monkeypatch.delenv("AIRGAP_TEST_PASSWORD", raising=False)
        with pytest.raises(ConfigError, match="AIRGAP_TEST_PASSWORD"):
            resolve_password(load_mysql(config_data, "AIRGAP_TEST_PASSWORD"))

    def test_empty_env_var_fails(self, config_data, monkeypatch):
        monkeypatch.setenv("AIRGAP_TEST_PASSWORD", "")
        with pytest.raises(ConfigError):
            resolve_password(load_mysql(config_data, "AIRGAP_TEST_PASSWORD"))

    def test_password_never_in_config_error_output(self, config_data, write_config, password_env):
        """校验失败的错误信息中不得出现密码值。"""
        config_data["tables"] = [{"name": "t_bad", "mode": "keyed"}]
        with pytest.raises(ConfigError) as excinfo:
            load_config(write_config(config_data))
        assert PASSWORD_VALUE not in str(excinfo.value)

    def test_password_not_in_config_repr(self, config_data, write_config, password_env):
        """配置对象本身不保存密码。"""
        config = load_config(write_config(config_data))
        assert PASSWORD_VALUE not in repr(config)
        assert PASSWORD_VALUE not in repr(config.mysql)

    def test_resolve_password_error_names_env_var_only(self, config_data, monkeypatch):
        monkeypatch.delenv("AIRGAP_TEST_PASSWORD", raising=False)
        with pytest.raises(ConfigError) as excinfo:
            resolve_password(load_mysql(config_data, "AIRGAP_TEST_PASSWORD"))
        # 错误信息只包含变量名, 不可能有密码值 (变量未设置)
        assert "AIRGAP_TEST_PASSWORD" in str(excinfo.value)


def load_mysql(config_data: dict, env_name: str):
    """从 config_data 构造 MySQLConfig (供密码相关测试使用)。"""
    data = dict(config_data["mysql"])
    data["password_env"] = env_name
    return MySQLConfig.model_validate(data)
