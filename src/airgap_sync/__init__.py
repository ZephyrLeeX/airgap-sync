"""Airgap Sync - 单向隔离网络 MySQL 数据同步工具。"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("airgap-sync")
except PackageNotFoundError as exc:
    raise RuntimeError(
        "airgap-sync distribution metadata is unavailable; install the package before running it"
    ) from exc
