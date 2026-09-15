"""pytest 公共 fixture。"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

# scripts/offline 下的共享 helper (release_manifest / build_release) 按顶层模块导入。
OFFLINE_SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts" / "offline"
if str(OFFLINE_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(OFFLINE_SCRIPTS_DIR))

PASSWORD_ENV = "AIRGAP_TEST_PASSWORD"
PASSWORD_VALUE = "test-secret-password"


@pytest.fixture
def password_env(monkeypatch: pytest.MonkeyPatch) -> str:
    """设置测试密码环境变量, 返回变量名。"""
    monkeypatch.setenv(PASSWORD_ENV, PASSWORD_VALUE)
    return PASSWORD_ENV


@pytest.fixture
def config_data(tmp_path: Path) -> dict:
    """一份完整合法的 source 配置 (dict 副本, 测试可自由修改)。"""
    return {
        "role": "source",
        "mysql": {
            "host": "127.0.0.1",
            "port": 3306,
            "database": "sgaj_data",
            "user": "sgaj_sync",
            "password_env": PASSWORD_ENV,
            "connect_timeout": 10,
        },
        "paths": {"data_dir": str(tmp_path / "data")},
        "tables": [
            {"name": "t_snapshot", "enabled": True},
        ],
    }


@pytest.fixture
def write_config(tmp_path: Path):
    """把 dict 写成 YAML 配置文件, 返回路径。"""

    def _write(data: dict, name: str = "config.yaml") -> Path:
        path = tmp_path / name
        path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")
        return path

    return _write
