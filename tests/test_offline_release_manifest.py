"""scripts/offline/release_manifest.py 单元测试。

覆盖: release id、manifest 校验、release.env、SHA256SUMS 写读校验、
bundle 完整性 (含 checksum mismatch)、schema 兼容规则、安装状态判定。
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

import release_manifest as rm
from airgap_sync.destination.mysql import METADATA_SCHEMA_VERSION
from airgap_sync.source.state import SCHEMA_VERSION

GIT_COMMIT = "cc8e17a" + "1" * 33


def make_manifest(**overrides) -> rm.ReleaseManifest:
    values: dict = dict(
        release_id=f"0.1.0-{GIT_COMMIT[:7]}",
        app_version="0.1.0",
        git_commit=GIT_COMMIT,
        created_at="2026-09-15T00:00:00+00:00",
        python_version="3.13.15",
        platform_os="linux",
        platform_arch="x86_64",
        minimum_glibc="2.17",
        source_state_schema=4,
        destination_metadata_schema=3,
        runtime_artifact="cpython-3.13.15+20260901-x86_64-unknown-linux-gnu-install_only.tar.gz",
        app_wheel="airgap_sync-0.1.0-py3-none-any.whl",
        wheel_count=15,
        include_tests=False,
    )
    values.update(overrides)
    return rm.ReleaseManifest(**values)


def write_fake_bundle(root: Path, *, platform: str = "linux") -> rm.ReleaseManifest:
    """Assemble a minimal but structurally complete fake bundle."""
    manifest = make_manifest(
        platform_os=platform,
        platform_arch="amd64" if platform == "windows" else "x86_64",
        minimum_glibc=None if platform == "windows" else "2.17",
        runtime_artifact=(
            "python-3.13.15-amd64.exe"
            if platform == "windows"
            else "cpython-3.13.15+20260901-x86_64-unknown-linux-gnu-install_only.tar.gz"
        ),
    )
    for name in ("runtime", "app", "wheelhouse", "config"):
        (root / name).mkdir(parents=True)
    (root / "runtime" / manifest.runtime_artifact).write_bytes(b"runtime-bytes")
    (root / "app" / manifest.app_wheel).write_bytes(b"app-wheel-bytes")
    (root / "wheelhouse" / "click-8.5.0-py3-none-any.whl").write_bytes(b"click-wheel")
    (root / "config" / "source.example.yaml").write_text("role: source\n")
    (root / "config" / "destination.example.yaml").write_text("role: destination\n")
    (root / rm.DEPLOY_SCRIPT_NAMES[platform]).write_text("#!/bin/sh\nexit 0\n")
    (root / rm.HELPER_SCRIPT_NAME).write_text("# shared helper\n")
    manifest.dump(root / rm.MANIFEST_NAME)
    rm.write_release_env(manifest, root / rm.RELEASE_ENV_NAME)
    rm.write_sha256sums(root, rm.bundle_payload_files(root))
    return manifest


class TestReleaseId:
    def test_short_sha_suffix(self) -> None:
        assert rm.make_release_id("0.1.0", GIT_COMMIT) == "0.1.0-cc8e17a"

    def test_rejects_non_hex_commit(self) -> None:
        with pytest.raises(rm.ReleaseManifestError):
            rm.make_release_id("0.1.0", "zzzzzzz")


class TestManifest:
    def test_roundtrip(self, tmp_path: Path) -> None:
        manifest = make_manifest()
        path = tmp_path / rm.MANIFEST_NAME
        manifest.dump(path)
        assert rm.ReleaseManifest.load(path) == manifest

    def test_platform_block(self, tmp_path: Path) -> None:
        data = make_manifest().to_dict()
        assert data["platform"] == {"os": "linux", "arch": "x86_64", "minimum_glibc": "2.17"}

    def test_validation_rejects_wrong_platform(self) -> None:
        with pytest.raises(rm.ReleaseManifestError, match="unsupported platform"):
            make_manifest(platform_os="darwin", platform_arch="arm64")

    def test_validation_rejects_bad_release_id(self) -> None:
        with pytest.raises(rm.ReleaseManifestError, match="release_id"):
            make_manifest(release_id="latest")

    def test_validation_rejects_mismatched_commit(self) -> None:
        with pytest.raises(rm.ReleaseManifestError, match="git commit"):
            make_manifest(release_id="0.1.0-deadbee")

    @pytest.mark.parametrize(
        "overrides",
        [
            {"minimum_glibc": None},
            {"source_state_schema": 0},
            {"app_wheel": "airgap_sync-0.1.0.tar.gz"},
            {"runtime_artifact": "runtime/python.tar.gz"},
        ],
    )
    def test_validation_rejects_bad_fields(self, overrides: dict) -> None:
        with pytest.raises(rm.ReleaseManifestError):
            make_manifest(**overrides)

    def test_missing_key_is_malformed(self) -> None:
        with pytest.raises(rm.ReleaseManifestError, match="malformed"):
            rm.ReleaseManifest.from_dict({"release_id": "0.1.0-cc8e17a"})

    def test_schema_versions_match_application(self) -> None:
        """manifest schema 值必须来自应用单一常量, 防止发布元数据漂移。"""
        assert rm.schema_versions() == {
            "source_state_schema": SCHEMA_VERSION,
            "destination_metadata_schema": METADATA_SCHEMA_VERSION,
        }


class TestReleaseEnv:
    def test_render_contains_all_keys(self) -> None:
        env = rm.render_release_env(make_manifest())
        assert 'AIRGAP_RELEASE_ID="0.1.0-cc8e17a"' in env
        assert "AIRGAP_SOURCE_STATE_SCHEMA=4" in env
        assert "AIRGAP_DESTINATION_METADATA_SCHEMA=3" in env
        assert 'AIRGAP_PLATFORM_OS="linux"' in env
        assert 'AIRGAP_MINIMUM_GLIBC="2.17"' in env

    def test_sourceable_by_bash(self, tmp_path: Path) -> None:
        if shutil.which("bash") is None:
            pytest.skip("bash not available")
        path = tmp_path / rm.RELEASE_ENV_NAME
        rm.write_release_env(make_manifest(), path)
        result = subprocess.run(
            [
                "bash",
                "-c",
                f'set -eu; . {path}; printf \'%s|%s\' "$AIRGAP_RELEASE_ID" "$AIRGAP_WHEEL_COUNT"',
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        assert result.stdout == "0.1.0-cc8e17a|15"


class TestSha256sums:
    def test_roundtrip(self, tmp_path: Path) -> None:
        (tmp_path / "a.bin").write_bytes(b"aaa")
        (tmp_path / "b.bin").write_bytes(b"bbb")
        rm.write_sha256sums(tmp_path, ["b.bin", "a.bin"])
        entries = rm.read_sha256sums(tmp_path / rm.SHA256SUMS_NAME)
        assert [name for _, name in entries] == ["a.bin", "b.bin"]
        assert rm.verify_sha256sums(tmp_path, entries) == []

    def test_tampering_detected(self, tmp_path: Path) -> None:
        (tmp_path / "a.bin").write_bytes(b"aaa")
        rm.write_sha256sums(tmp_path, ["a.bin"])
        entries = rm.read_sha256sums(tmp_path / rm.SHA256SUMS_NAME)
        (tmp_path / "a.bin").write_bytes(b"evil")
        problems = rm.verify_sha256sums(tmp_path, entries)
        assert any("checksum mismatch" in problem for problem in problems)

    def test_missing_file_detected(self, tmp_path: Path) -> None:
        (tmp_path / "a.bin").write_bytes(b"aaa")
        rm.write_sha256sums(tmp_path, ["a.bin"])
        (tmp_path / "a.bin").unlink()
        entries = rm.read_sha256sums(tmp_path / rm.SHA256SUMS_NAME)
        assert rm.verify_sha256sums(tmp_path, entries) == [
            "missing file listed in SHA256SUMS: a.bin"
        ]

    def test_empty_sums_rejected(self, tmp_path: Path) -> None:
        assert rm.verify_sha256sums(tmp_path, []) == ["SHA256SUMS lists no files"]

    @pytest.mark.parametrize(
        "evil",
        [
            "../outside.file",
            "/etc/passwd",
            "C:\\Windows\\evil.dll",
            "foo/../../bar.whl",
            "..\\outside.whl",
        ],
    )
    def test_path_traversal_rejected(self, tmp_path: Path, evil: str) -> None:
        digest = "a" * 64
        sums = tmp_path / rm.SHA256SUMS_NAME
        sums.write_text(f"{digest}  {evil}\n", encoding="utf-8")
        with pytest.raises(rm.ReleaseManifestError, match="not bundle-relative"):
            rm.read_sha256sums(sums)

    def test_bundle_relative_paths_accepted(self, tmp_path: Path) -> None:
        (tmp_path / "wheelhouse").mkdir()
        (tmp_path / "wheelhouse" / "a.whl").write_bytes(b"a")
        sums = tmp_path / rm.SHA256SUMS_NAME
        sums.write_text(f"{rm.sha256_file(tmp_path / 'wheelhouse' / 'a.whl')}  wheelhouse/a.whl\n")
        entries = rm.read_sha256sums(sums)
        assert [name for _, name in entries] == ["wheelhouse/a.whl"]
        assert rm.verify_sha256sums(tmp_path, entries) == []


class TestVerifyBundle:
    def test_valid_bundle(self, tmp_path: Path) -> None:
        write_fake_bundle(tmp_path)
        assert rm.verify_bundle(tmp_path) == []
        assert rm.verify_bundle(tmp_path, expect_os="linux", expect_arch="x86_64") == []

    def test_wheel_tampering_fails_verification(self, tmp_path: Path) -> None:
        write_fake_bundle(tmp_path)
        wheel = tmp_path / "wheelhouse" / "click-8.5.0-py3-none-any.whl"
        wheel.write_bytes(b"corrupted-wheel")
        problems = rm.verify_bundle(tmp_path)
        assert any("checksum mismatch" in problem for problem in problems)

    def test_wrong_platform_rejected(self, tmp_path: Path) -> None:
        write_fake_bundle(tmp_path)
        problems = rm.verify_bundle(tmp_path, expect_os="windows", expect_arch="amd64")
        assert any("targets os=linux" in problem for problem in problems)

    def test_unlisted_extra_file_rejected(self, tmp_path: Path) -> None:
        write_fake_bundle(tmp_path)
        (tmp_path / "wheelhouse" / "extra-1.0-py3-none-any.whl").write_bytes(b"extra")
        problems = rm.verify_bundle(tmp_path)
        assert any("not covered by SHA256SUMS" in problem for problem in problems)

    def test_missing_required_file_rejected(self, tmp_path: Path) -> None:
        write_fake_bundle(tmp_path)
        (tmp_path / rm.RELEASE_ENV_NAME).unlink()
        problems = rm.verify_bundle(tmp_path)
        assert any("release.env" in problem for problem in problems)


class TestReleaseEnvConsistency:
    def test_parse_render_roundtrip(self) -> None:
        manifest = make_manifest()
        parsed = rm.parse_release_env_text(rm.render_release_env(manifest))
        assert parsed["AIRGAP_RELEASE_ID"] == "0.1.0-cc8e17a"
        assert parsed["AIRGAP_PYTHON_VERSION"] == "3.13.15"
        assert parsed["AIRGAP_SOURCE_STATE_SCHEMA"] == "4"
        assert parsed["AIRGAP_WHEEL_COUNT"] == "15"
        assert parsed["AIRGAP_INCLUDE_TESTS"] == "0"
        assert parsed["AIRGAP_MINIMUM_GLIBC"] == "2.17"

    def test_parse_unescapes_quoted_values(self) -> None:
        parsed = rm.parse_release_env_text('AIRGAP_RELEASE_ID="0.1.0-a\\$b"\nAIRGAP_X="q\\"uote"\n')
        assert parsed["AIRGAP_RELEASE_ID"] == "0.1.0-a$b"
        assert parsed["AIRGAP_X"] == 'q"uote'

    def test_agreeing_env_has_no_problems(self, tmp_path: Path) -> None:
        manifest = make_manifest()
        rm.write_release_env(manifest, tmp_path / rm.RELEASE_ENV_NAME)
        assert rm.release_env_problems(manifest, tmp_path / rm.RELEASE_ENV_NAME) == []

    def test_disagreeing_env_reported(self, tmp_path: Path) -> None:
        manifest = make_manifest()
        text = rm.render_release_env(manifest).replace(
            'AIRGAP_PYTHON_VERSION="3.13.15"', 'AIRGAP_PYTHON_VERSION="3.13.16"'
        )
        (tmp_path / rm.RELEASE_ENV_NAME).write_text(text, encoding="utf-8")
        problems = rm.release_env_problems(manifest, tmp_path / rm.RELEASE_ENV_NAME)
        assert any("AIRGAP_PYTHON_VERSION disagrees" in p for p in problems)

    def test_missing_env_key_reported(self, tmp_path: Path) -> None:
        manifest = make_manifest()
        text = rm.render_release_env(manifest).replace("AIRGAP_APP_WHEEL", "AIRGAP_REMOVED_KEY")
        (tmp_path / rm.RELEASE_ENV_NAME).write_text(text, encoding="utf-8")
        problems = rm.release_env_problems(manifest, tmp_path / rm.RELEASE_ENV_NAME)
        assert any("missing AIRGAP_APP_WHEEL" in p for p in problems)

    def test_cli_check_env_exit_codes(self, tmp_path: Path) -> None:
        write_fake_bundle(tmp_path)
        helper = Path(rm.__file__)
        ok = subprocess.run(
            [sys.executable, str(helper), "check-env", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert ok.returncode == 0, ok.stdout + ok.stderr
        env_path = tmp_path / rm.RELEASE_ENV_NAME
        tampered = env_path.read_text(encoding="utf-8").replace(
            'AIRGAP_APP_VERSION="0.1.0"', 'AIRGAP_APP_VERSION="9.9.9"'
        )
        env_path.write_text(tampered, encoding="utf-8")
        bad = subprocess.run(
            [sys.executable, str(helper), "check-env", str(tmp_path)],
            capture_output=True,
            text=True,
        )
        assert bad.returncode == 1
        assert "AIRGAP_APP_VERSION disagrees" in bad.stdout


class TestSchemaCompatibility:
    @staticmethod
    def payload(source: int, destination: int) -> dict:
        return {
            "source_state_schema": source,
            "destination_metadata_schema": destination,
        }

    def test_same_schema_allowed(self) -> None:
        assert rm.check_schema_compatibility(self.payload(4, 3), self.payload(4, 3)) == []

    def test_newer_schema_allowed(self) -> None:
        assert rm.check_schema_compatibility(self.payload(4, 3), self.payload(5, 3)) == []

    def test_source_schema_downgrade_blocked(self) -> None:
        reasons = rm.check_schema_compatibility(self.payload(5, 3), self.payload(4, 3))
        assert any("source_state_schema downgrade 5 -> 4" in reason for reason in reasons)

    def test_destination_schema_downgrade_blocked(self) -> None:
        reasons = rm.check_schema_compatibility(self.payload(4, 3), self.payload(4, 2))
        assert any("destination_metadata_schema downgrade 3 -> 2" in reason for reason in reasons)

    def test_cli_schema_check_exit_codes(self, tmp_path: Path) -> None:
        current = tmp_path / "current.json"
        target = tmp_path / "target.json"
        current.write_text(json.dumps(self.payload(4, 3)))
        target.write_text(json.dumps(self.payload(4, 3)))
        helper = Path(rm.__file__)
        ok = subprocess.run(
            [sys.executable, str(helper), "schema-check", str(current), str(target)],
            capture_output=True,
            text=True,
        )
        assert ok.returncode == 0
        target.write_text(json.dumps(self.payload(3, 3)))
        blocked = subprocess.run(
            [sys.executable, str(helper), "schema-check", str(current), str(target)],
            capture_output=True,
            text=True,
        )
        assert blocked.returncode == 2
        assert "BLOCKED" in blocked.stdout


class TestInstallState:
    def test_missing(self, tmp_path: Path) -> None:
        assert rm.install_state(tmp_path / "nope") == rm.INSTALL_STATE_MISSING

    def test_incomplete(self, tmp_path: Path) -> None:
        release_dir = tmp_path / "0.1.0-aaaaaaa"
        (release_dir / "venv").mkdir(parents=True)
        assert rm.install_state(release_dir) == rm.INSTALL_STATE_INCOMPLETE

    def test_complete(self, tmp_path: Path) -> None:
        release_dir = tmp_path / "0.1.0-aaaaaaa"
        release_dir.mkdir()
        rm.write_installed_marker(release_dir, make_manifest().to_dict())
        assert rm.install_state(release_dir) == rm.INSTALL_STATE_COMPLETE
        data = rm.load_installed_marker(release_dir)
        assert data["release_id"] == "0.1.0-cc8e17a"
        assert data["installed_at"]
