"""HTTP Relay 单文件流式上传客户端。"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from airgap_sync.common.models import RelayConfig

logger = logging.getLogger(__name__)

UPLOAD_CONFIRMED = "UPLOAD_CONFIRMED"
UPLOAD_RESPONSE_MISMATCH = "UPLOAD_RESPONSE_MISMATCH"
REMOTE_FILE_EXISTS_AMBIGUOUS = "REMOTE_FILE_EXISTS_AMBIGUOUS"
UPLOAD_RETRY_EXHAUSTED = "UPLOAD_RETRY_EXHAUSTED"
UPLOAD_HTTP_ERROR = "UPLOAD_HTTP_ERROR"

_RETRYABLE_STATUSES = {429, 500, 507}


class UploadError(Exception):
    """Relay 未能可靠确认接收；消息永不包含 Authorization token。"""

    def __init__(self, code: str, message: str, *, attempts: int, request_id: str | None = None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.attempts = attempts
        self.request_id = request_id


@dataclass(frozen=True)
class UploadConfirmation:
    filename: str
    size: int
    sha256: str
    request_id: str
    attempts: int


class RelayUploader:
    """固定单请求客户端；并发策略由上层单 worker 保证。"""

    def __init__(
        self,
        config: RelayConfig,
        token: str,
        *,
        session: Any | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self._token = token
        self._session = session or requests.Session()
        self._sleep = sleeper

    def __repr__(self) -> str:
        return f"RelayUploader(base_url={self.config.base_url!r}, token=<redacted>)"

    def check_health(self) -> None:
        url = f"{self.config.base_url.rstrip('/')}/health"
        try:
            response = self._session.get(
                url,
                timeout=(
                    self.config.connect_timeout_seconds,
                    self.config.read_timeout_seconds,
                ),
                verify=self._tls_verify(),
            )
        except requests.RequestException as exc:
            raise UploadError("RELAY_HEALTH_FAILED", type(exc).__name__, attempts=1) from exc
        if response.status_code != 200:
            raise UploadError("RELAY_HEALTH_FAILED", f"HTTP {response.status_code}", attempts=1)
        try:
            payload = response.json()
        except (ValueError, TypeError) as exc:
            raise UploadError("RELAY_HEALTH_FAILED", "invalid JSON response", attempts=1) from exc
        if not isinstance(payload, dict) or payload.get("status") != "ok":
            raise UploadError("RELAY_HEALTH_FAILED", "response status is not ok", attempts=1)

    def upload(
        self,
        path: Path,
        transport_name: str,
        sha256: str,
        *,
        on_attempt: Callable[[int], None] | None = None,
    ) -> UploadConfirmation:
        """用文件对象作为请求体流式 PUT，并严格验证 201 JSON。"""
        size = path.stat().st_size
        url = f"{self.config.base_url.rstrip('/')}/api/v1/upload/{quote(transport_name, safe='')}"
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/octet-stream",
            "Content-Length": str(size),
            "X-File-SHA256": sha256,
        }
        last_reason = "request failed"
        for attempt in range(1, self.config.max_attempts + 1):
            if on_attempt is not None:
                on_attempt(attempt)
            try:
                # 每次重试重新打开，确保请求体从 offset 0 开始；不把文件读入内存。
                with path.open("rb") as body:
                    response = self._session.put(
                        url,
                        data=body,
                        headers=headers,
                        timeout=(
                            self.config.connect_timeout_seconds,
                            self.config.read_timeout_seconds,
                        ),
                        verify=self._tls_verify(),
                    )
            except requests.RequestException as exc:
                last_reason = type(exc).__name__
                if attempt < self.config.max_attempts:
                    self._backoff(attempt)
                    continue
                raise UploadError(UPLOAD_RETRY_EXHAUSTED, last_reason, attempts=attempt) from exc

            request_id = self._safe_request_id(response)
            if response.status_code == 201:
                return self._validate_confirmation(response, transport_name, size, sha256, attempt)
            if response.status_code == 409:
                raise UploadError(
                    REMOTE_FILE_EXISTS_AMBIGUOUS,
                    "HTTP 409; existing remote file cannot be verified",
                    attempts=attempt,
                    request_id=request_id,
                )
            if self._is_retryable(response.status_code):
                last_reason = f"HTTP {response.status_code}"
                if attempt < self.config.max_attempts:
                    self._backoff(attempt)
                    continue
                raise UploadError(
                    UPLOAD_RETRY_EXHAUSTED,
                    last_reason,
                    attempts=attempt,
                    request_id=request_id,
                )
            raise UploadError(
                UPLOAD_HTTP_ERROR,
                f"HTTP {response.status_code}",
                attempts=attempt,
                request_id=request_id,
            )
        raise AssertionError(f"unreachable: {last_reason}")  # pragma: no cover

    def _validate_confirmation(
        self, response: Any, filename: str, size: int, sha256: str, attempt: int
    ) -> UploadConfirmation:
        try:
            payload = response.json()
        except (ValueError, TypeError) as exc:
            raise UploadError(
                UPLOAD_RESPONSE_MISMATCH,
                "HTTP 201 response is not valid JSON",
                attempts=attempt,
            ) from exc
        request_id = payload.get("request_id") if isinstance(payload, dict) else None
        valid = (
            isinstance(payload, dict)
            and payload.get("success") is True
            and payload.get("filename") == filename
            and type(payload.get("size")) is int
            and payload.get("size") == size
            and isinstance(payload.get("sha256"), str)
            and payload["sha256"].lower() == sha256.lower()
            and isinstance(request_id, str)
            and bool(request_id.strip())
        )
        if not valid:
            raise UploadError(
                UPLOAD_RESPONSE_MISMATCH,
                "HTTP 201 confirmation fields do not match local artifact",
                attempts=attempt,
                request_id=request_id if isinstance(request_id, str) else None,
            )
        return UploadConfirmation(filename, size, sha256.lower(), request_id, attempt)

    @staticmethod
    def _safe_request_id(response: Any) -> str | None:
        try:
            payload = response.json()
        except (ValueError, TypeError):
            return None
        value = payload.get("request_id") if isinstance(payload, dict) else None
        return value[:200] if isinstance(value, str) and value.strip() else None

    @staticmethod
    def _is_retryable(status: int) -> bool:
        return status in _RETRYABLE_STATUSES or 500 <= status <= 599

    def _backoff(self, attempt: int) -> None:
        delay = min(
            self.config.retry_base_seconds * (2 ** (attempt - 1)),
            self.config.retry_max_seconds,
        )
        self._sleep(delay)

    def _tls_verify(self) -> bool | str:
        return str(self.config.ca_file) if self.config.ca_file is not None else True
