from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
import requests

from airgap_sync.common.models import RelayConfig
from airgap_sync.source.uploader import (
    REMOTE_FILE_EXISTS_AMBIGUOUS,
    UPLOAD_HTTP_ERROR,
    UPLOAD_RESPONSE_MISMATCH,
    UPLOAD_RETRY_EXHAUSTED,
    RelayUploader,
    UploadError,
)


class Response:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class Session:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def put(self, url, *, data, headers, timeout, verify):
        assert not isinstance(data, bytes)
        body = b"".join(iter(lambda: data.read(3), b""))
        self.calls.append((url, body, headers, timeout, verify))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def relay(**overrides):
    return RelayConfig(
        base_url="https://relay.example/root/",
        token_env="TOKEN",
        max_attempts=overrides.pop("max_attempts", 3),
        retry_base_seconds=0,
        retry_max_seconds=0,
        **overrides,
    )


def good(path: Path, filename: str):
    content = path.read_bytes()
    return Response(
        201,
        {
            "success": True,
            "filename": filename,
            "size": len(content),
            "sha256": hashlib.sha256(content).hexdigest().upper(),
            "request_id": "req-1",
        },
    )


def test_streaming_put_headers_and_confirmation(tmp_path):
    path = tmp_path / "chunk"
    path.write_bytes(b"abcdefghij")
    filename = "airgap-v1--20260915T030000Z-a1b2c3d4--chunk-000001.jsonl.zst"
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    session = Session([good(path, filename)])
    result = RelayUploader(relay(ca_file=tmp_path / "ca.pem"), "secret", session=session).upload(
        path, filename, digest
    )
    assert result.request_id == "req-1"
    url, body, headers, timeout, verify = session.calls[0]
    assert url.endswith("/api/v1/upload/" + filename)
    assert body == b"abcdefghij"
    assert headers == {
        "Authorization": "Bearer secret",
        "Content-Type": "application/octet-stream",
        "Content-Length": "10",
        "X-File-SHA256": digest,
    }
    assert timeout == (10, 600)
    assert verify == str(tmp_path / "ca.pem")


@pytest.mark.parametrize(
    "change",
    [
        {"success": False},
        {"filename": "wrong"},
        {"size": 999},
        {"sha256": "0" * 64},
        {"request_id": ""},
    ],
)
def test_201_mismatch_fails_without_retry(tmp_path, change):
    path = tmp_path / "x"
    path.write_bytes(b"abc")
    filename = "file"
    payload = good(path, filename)._payload | change
    session = Session([Response(201, payload)])
    with pytest.raises(UploadError) as caught:
        RelayUploader(relay(), "secret", session=session).upload(
            path, filename, hashlib.sha256(b"abc").hexdigest()
        )
    assert caught.value.code == UPLOAD_RESPONSE_MISMATCH
    assert len(session.calls) == 1
    assert path.exists()


@pytest.mark.parametrize("status", [401, 411, 413, 415, 422, 404])
def test_deterministic_4xx_not_retried(tmp_path, status):
    path = tmp_path / "x"
    path.write_bytes(b"abc")
    session = Session([Response(status, {})])
    with pytest.raises(UploadError) as caught:
        RelayUploader(relay(), "secret", session=session).upload(path, "file", "0" * 64)
    assert caught.value.code == UPLOAD_HTTP_ERROR
    assert len(session.calls) == 1


@pytest.mark.parametrize("status", [429, 500, 503, 507])
def test_retryable_http_then_success(tmp_path, status):
    path = tmp_path / "x"
    path.write_bytes(b"abc")
    filename = "file"
    session = Session([Response(status, {}), good(path, filename)])
    result = RelayUploader(relay(), "secret", session=session).upload(
        path, filename, hashlib.sha256(b"abc").hexdigest()
    )
    assert result.attempts == 2
    assert len(session.calls) == 2


def test_network_timeout_retried_and_exhausted(tmp_path):
    path = tmp_path / "x"
    path.write_bytes(b"abc")
    session = Session([requests.ReadTimeout("secret"), requests.ReadTimeout("secret")])
    with pytest.raises(UploadError) as caught:
        RelayUploader(relay(max_attempts=2), "super-secret-token", session=session).upload(
            path, "file", "0" * 64
        )
    assert caught.value.code == UPLOAD_RETRY_EXHAUSTED
    assert caught.value.attempts == 2
    assert "super-secret-token" not in repr(caught.value)


def test_409_is_ambiguous_and_not_success(tmp_path):
    path = tmp_path / "x"
    path.write_bytes(b"abc")
    with pytest.raises(UploadError) as caught:
        RelayUploader(relay(), "secret", session=Session([Response(409, {})])).upload(
            path, "file", "0" * 64
        )
    assert caught.value.code == REMOTE_FILE_EXISTS_AMBIGUOUS


def test_token_not_in_repr():
    assert "super-secret-token" not in repr(RelayUploader(relay(), "super-secret-token"))
