"""YAML 配置文件加载与校验。"""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import ValidationError

from airgap_sync.common.models import AppConfig, MySQLConfig


class ConfigError(Exception):
    """配置文件读取、解析或校验失败。"""


def load_config(path: Path) -> AppConfig:
    """读取 YAML 配置文件并校验。

    校验失败时抛出 ConfigError, 错误信息包含具体位置;
    配置中不含密码, 因此错误信息不会泄漏密码。
    """
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {path}") from exc
    except OSError as exc:
        raise ConfigError(f"cannot read config file {path}: {exc}") from exc

    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigError(f"config root must be a mapping, got {type(data).__name__} ({path})")

    try:
        return AppConfig.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(f"invalid config {path}:\n{_format_validation_error(exc)}") from exc


def _format_validation_error(exc: ValidationError) -> str:
    lines = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"]) or "(root)"
        lines.append(f"  {location}: {error['msg']}")
    return "\n".join(lines)


def resolve_password(mysql: MySQLConfig) -> str:
    """从环境变量读取数据库密码。

    环境变量未设置 (或为空) 时抛出 ConfigError。
    返回值只用于建立连接, 不得写入日志。
    """
    password = os.environ.get(mysql.password_env)
    if not password:
        raise ConfigError(
            f"environment variable '{mysql.password_env}' "
            "(mysql.password_env) is not set; provide the database "
            "password through this variable"
        )
    return password
