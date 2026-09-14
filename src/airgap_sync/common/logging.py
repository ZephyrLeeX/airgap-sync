"""统一日志初始化。"""

from __future__ import annotations

import logging
import os
import sys

LOG_LEVEL_ENV = "AIRGAP_SYNC_LOG_LEVEL"

VALID_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")

_LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s %(message)s"


def resolve_log_level(level: str | None = None) -> int:
    """确定日志级别: 显式参数 > 环境变量 > INFO。"""
    value = (level or os.environ.get(LOG_LEVEL_ENV) or "INFO").upper()
    if value not in VALID_LOG_LEVELS:
        raise ValueError(
            f"invalid log level '{value}', expected one of: {', '.join(VALID_LOG_LEVELS)}"
        )
    return logging.getLevelName(value)


def setup_logging(level: str | None = None) -> None:
    """初始化控制台日志, 可重复调用。"""
    root = logging.getLogger()
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(resolve_log_level(level))
