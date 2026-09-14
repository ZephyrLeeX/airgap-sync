"""日志初始化测试。"""

from __future__ import annotations

import logging

import pytest

from airgap_sync.common.logging import (
    LOG_LEVEL_ENV,
    resolve_log_level,
    setup_logging,
)


class TestResolveLogLevel:
    def test_default_is_info(self, monkeypatch):
        monkeypatch.delenv(LOG_LEVEL_ENV, raising=False)
        assert resolve_log_level() == logging.INFO

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("DEBUG", logging.DEBUG),
            ("INFO", logging.INFO),
            ("WARNING", logging.WARNING),
            ("ERROR", logging.ERROR),
        ],
    )
    def test_explicit_level(self, value, expected):
        assert resolve_log_level(value) == expected

    def test_env_var(self, monkeypatch):
        monkeypatch.setenv(LOG_LEVEL_ENV, "debug")  # 大小写不敏感
        assert resolve_log_level() == logging.DEBUG

    def test_invalid_level_raises(self):
        with pytest.raises(ValueError, match="invalid log level"):
            resolve_log_level("VERBOSE")

    def test_invalid_env_var_raises(self, monkeypatch):
        monkeypatch.setenv(LOG_LEVEL_ENV, "nope")
        with pytest.raises(ValueError, match="invalid log level"):
            resolve_log_level()


class TestSetupLogging:
    def test_setup_is_repeatable(self):
        setup_logging("INFO")
        setup_logging("DEBUG")  # 不抛异常
        root = logging.getLogger()
        assert root.level == logging.DEBUG
        assert len(root.handlers) == 1

    def test_output_contains_level_name_and_module(self, capsys):
        setup_logging("INFO")
        logging.getLogger("airgap_sync.test").info("hello-log-message")
        err = capsys.readouterr().err
        assert "INFO" in err
        assert "airgap_sync.test" in err
        assert "hello-log-message" in err
