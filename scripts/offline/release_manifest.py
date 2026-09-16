#!/usr/bin/env python3
"""Shared release-manifest helpers for offline bundles and deployments.

This module is copied verbatim into every release bundle. The build machine,
the unit tests and the deploy scripts all use the same functions so that
manifest format, SHA256SUMS handling, schema-compatibility rules and install
state checks stay consistent everywhere.

It deliberately has no third-party imports: on the deploy side it must run
under the bundled portable Python before/without the application venv.

Schema version values are NOT defined here. They are read from the application
package (``airgap_sync.source.state.SCHEMA_VERSION`` and
``airgap_sync.destination.mysql.METADATA_SCHEMA_VERSION``) so the release
metadata can never drift from the runtime migrations. That import only happens
on the build machine via :func:`schema_versions`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MANIFEST_FORMAT_VERSION = 2
MANIFEST_NAME = "release.json"
RELEASE_ENV_NAME = "release.env"
SHA256SUMS_NAME = "SHA256SUMS"
INSTALLED_MARKER_NAME = "installed.json"
RUNTIME_DIR_NAME = "runtime"
APP_DIR_NAME = "app"
WHEELHOUSE_DIR_NAME = "wheelhouse"
LINUX_MINIMUM_GLIBC = "2.17"
DEPLOY_SCRIPT_NAMES = {"windows": "airgap-sync-deploy.ps1", "linux": "airgap-sync-deploy.sh"}
HELPER_SCRIPT_NAME = "release_manifest.py"
CONFIG_EXAMPLES = ("source.example.yaml", "destination.example.yaml")
SUPPORTED_PLATFORMS = {("windows", "amd64"), ("linux", "x86_64")}
RUNTIME_POLICY_BUNDLED = "bundled"
RUNTIME_POLICY_EXTERNAL_PYTHON_PATH = "external-python-path"
SCHEMA_KEYS = ("source_state_schema", "destination_metadata_schema")

RELEASE_ID_PATTERN = re.compile(r"^\d+\.\d+\.\d+-[0-9a-f]{7,40}$")
HEX_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")

INSTALL_STATE_MISSING = "missing"
INSTALL_STATE_COMPLETE = "complete"
INSTALL_STATE_INCOMPLETE = "incomplete"


class ReleaseManifestError(Exception):
    """Release manifest is missing, malformed or inconsistent."""


def make_release_id(app_version: str, git_commit: str, sha_length: int = 7) -> str:
    """Build ``<app_version>-<short git sha>`` release identifiers."""
    if not re.fullmatch(r"[0-9a-f]+", git_commit):
        raise ReleaseManifestError(f"git commit must be a hex sha, got {git_commit!r}")
    return f"{app_version}-{git_commit[:sha_length]}"


@dataclass(frozen=True)
class ReleaseManifest:
    """Validated contents of a bundle's release.json."""

    release_id: str
    app_version: str
    git_commit: str
    created_at: str
    python_version: str
    platform_os: str
    platform_arch: str
    minimum_glibc: str | None
    source_state_schema: int
    destination_metadata_schema: int
    runtime_policy: str
    runtime_artifact: str | None
    app_wheel: str
    wheel_count: int
    include_tests: bool

    def __post_init__(self) -> None:
        problems = self.validation_problems()
        if problems:
            raise ReleaseManifestError("; ".join(problems))

    def to_dict(self) -> dict[str, Any]:
        platform: dict[str, str] = {"os": self.platform_os, "arch": self.platform_arch}
        if self.minimum_glibc is not None:
            platform["minimum_glibc"] = self.minimum_glibc
        result = {
            "format_version": MANIFEST_FORMAT_VERSION,
            "release_id": self.release_id,
            "app_version": self.app_version,
            "git_commit": self.git_commit,
            "created_at": self.created_at,
            "python_version": self.python_version,
            "runtime_policy": self.runtime_policy,
            "platform": platform,
            "source_state_schema": self.source_state_schema,
            "destination_metadata_schema": self.destination_metadata_schema,
            "app_wheel": self.app_wheel,
            "wheel_count": self.wheel_count,
            "include_tests": self.include_tests,
        }
        if self.runtime_artifact is not None:
            result["runtime_artifact"] = self.runtime_artifact
        return result

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReleaseManifest:
        try:
            if int(data["format_version"]) != MANIFEST_FORMAT_VERSION:
                raise ReleaseManifestError(
                    f"unsupported manifest format_version {data['format_version']!r} "
                    f"(expected {MANIFEST_FORMAT_VERSION})"
                )
            platform = data["platform"]
            manifest = cls(
                release_id=str(data["release_id"]),
                app_version=str(data["app_version"]),
                git_commit=str(data["git_commit"]),
                created_at=str(data["created_at"]),
                python_version=str(data["python_version"]),
                platform_os=str(platform["os"]),
                platform_arch=str(platform["arch"]),
                minimum_glibc=str(platform["minimum_glibc"])
                if platform.get("minimum_glibc") is not None
                else None,
                source_state_schema=int(data["source_state_schema"]),
                destination_metadata_schema=int(data["destination_metadata_schema"]),
                runtime_policy=str(data["runtime_policy"]),
                runtime_artifact=(
                    str(data["runtime_artifact"])
                    if data.get("runtime_artifact") is not None
                    else None
                ),
                app_wheel=str(data["app_wheel"]),
                wheel_count=int(data["wheel_count"]),
                include_tests=bool(data["include_tests"]),
            )
        except (KeyError, TypeError, ValueError, ReleaseManifestError) as exc:
            if isinstance(exc, ReleaseManifestError):
                raise
            raise ReleaseManifestError(f"release.json is malformed: {exc}") from exc
        return manifest

    @classmethod
    def load(cls, path: Path) -> ReleaseManifest:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ReleaseManifestError(f"cannot read {path}: {exc}") from exc
        return cls.from_dict(data)

    def dump(self, path: Path) -> None:
        path.write_text(
            json.dumps(self.to_dict(), indent=2, sort_keys=False) + "\n", encoding="utf-8"
        )

    def validation_problems(self) -> list[str]:
        problems: list[str] = []
        if not RELEASE_ID_PATTERN.fullmatch(self.release_id):
            problems.append(f"release_id {self.release_id!r} is not <version>-<gitsha>")
        if not self.release_id.endswith(self.git_commit[: self._release_sha_length()]):
            problems.append("release_id does not end with the declared git commit")
        if not re.fullmatch(r"[0-9a-f]{7,40}", self.git_commit):
            problems.append(f"git_commit {self.git_commit!r} is not a hex sha")
        if not re.fullmatch(r"\d+\.\d+\.\d+", self.python_version):
            problems.append(f"python_version {self.python_version!r} is not X.Y.Z")
        if not self.created_at:
            problems.append("created_at must not be empty")
        if not re.fullmatch(r"\d+\.\d+\.\d+.*", self.app_version):
            problems.append(f"app_version {self.app_version!r} is not a semver-ish version")
        if (self.platform_os, self.platform_arch) not in SUPPORTED_PLATFORMS:
            problems.append(f"unsupported platform {self.platform_os}/{self.platform_arch}")
        if self.platform_os == "linux" and not self.minimum_glibc:
            problems.append("linux bundles must declare minimum_glibc")
        if self.platform_os == "windows" and self.minimum_glibc is not None:
            problems.append("windows bundles must not declare minimum_glibc")
        for key in SCHEMA_KEYS:
            value = getattr(self, key)
            if value < 1:
                problems.append(f"{key} must be a positive integer")
        expected_policy = (
            RUNTIME_POLICY_EXTERNAL_PYTHON_PATH
            if self.platform_os == "windows"
            else RUNTIME_POLICY_BUNDLED
        )
        if self.runtime_policy != expected_policy:
            problems.append(
                f"{self.platform_os} bundles must use runtime_policy {expected_policy!r}"
            )
        if self.runtime_policy == RUNTIME_POLICY_BUNDLED:
            if self.runtime_artifact is None or "/" in self.runtime_artifact:
                problems.append("bundled runtime_artifact must be a bare filename")
        elif self.runtime_artifact is not None:
            problems.append("external Python bundles must not declare runtime_artifact")
        if "/" in self.app_wheel or not self.app_wheel:
            problems.append("app_wheel must be a bare filename")
        if not self.app_wheel.endswith(".whl"):
            problems.append("app_wheel must be a wheel filename")
        if self.wheel_count < 1:
            problems.append("wheel_count must be at least 1")
        return problems

    def _release_sha_length(self) -> int:
        suffix = self.release_id.rsplit("-", 1)[-1]
        return len(suffix)


def schema_versions() -> dict[str, int]:
    """Return the application's current schema versions (single source of truth).

    Imports the application package lazily: only build machines (and tests)
    have ``airgap_sync`` importable, while deploy scripts compare manifest
    values as plain data via :func:`check_schema_compatibility`.
    """
    from airgap_sync.destination.mysql import METADATA_SCHEMA_VERSION
    from airgap_sync.source.state import SCHEMA_VERSION

    return {
        "source_state_schema": SCHEMA_VERSION,
        "destination_metadata_schema": METADATA_SCHEMA_VERSION,
    }


def render_release_env(manifest: ReleaseManifest) -> str:
    """Render a shell-sourceable key/value dump of the manifest.

    Builder writes both release.json and release.env from the same in-memory
    manifest so their contents cannot diverge. Values are double-quoted with
    defensive escaping; integers stay unquoted for arithmetic use.
    """

    def quote(value: str) -> str:
        escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$")
        return f'"{escaped}"'

    lines = [
        f"AIRGAP_RELEASE_ID={quote(manifest.release_id)}",
        f"AIRGAP_APP_VERSION={quote(manifest.app_version)}",
        f"AIRGAP_GIT_COMMIT={quote(manifest.git_commit)}",
        f"AIRGAP_CREATED_AT={quote(manifest.created_at)}",
        f"AIRGAP_PYTHON_VERSION={quote(manifest.python_version)}",
        f"AIRGAP_RUNTIME_POLICY={quote(manifest.runtime_policy)}",
        f"AIRGAP_PLATFORM_OS={quote(manifest.platform_os)}",
        f"AIRGAP_PLATFORM_ARCH={quote(manifest.platform_arch)}",
        f"AIRGAP_SOURCE_STATE_SCHEMA={manifest.source_state_schema}",
        f"AIRGAP_DESTINATION_METADATA_SCHEMA={manifest.destination_metadata_schema}",
        f"AIRGAP_APP_WHEEL={quote(manifest.app_wheel)}",
        f"AIRGAP_WHEEL_COUNT={manifest.wheel_count}",
        f"AIRGAP_INCLUDE_TESTS={'1' if manifest.include_tests else '0'}",
    ]
    if manifest.runtime_artifact is not None:
        lines.append(f"AIRGAP_RUNTIME_ARTIFACT={quote(manifest.runtime_artifact)}")
    if manifest.minimum_glibc is not None:
        lines.append(f"AIRGAP_MINIMUM_GLIBC={quote(manifest.minimum_glibc)}")
    return "\n".join(lines) + "\n"


def write_release_env(manifest: ReleaseManifest, path: Path) -> None:
    path.write_text(render_release_env(manifest), encoding="utf-8")


def parse_release_env_text(text: str) -> dict[str, str]:
    """Parse a release.env into ``AIRGAP_* -> string value`` (unquoted).

    Only understands the exact shape :func:`render_release_env` produces
    (``KEY="escaped"`` or bare ``KEY=123``); anything else is reported as a
    parse problem by the caller via a missing key, so no lenient fallback.
    """
    values: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, raw = line.partition("=")
        if not separator or not key.startswith("AIRGAP_"):
            continue
        raw = raw.strip()
        if len(raw) >= 2 and raw.startswith('"') and raw.endswith('"'):
            # single-pass inverse of render_release_env's escaping
            raw = re.sub(r"\\(.)", r"\1", raw[1:-1])
        values[key.strip()] = raw
    return values


def release_env_problems(manifest: ReleaseManifest, env_path: Path) -> list[str]:
    """Compare release.env against release.json (must agree on every field).

    The builder writes both from one in-memory manifest; this deploy-side
    check makes sure a hand-edited or mixed-pair bundle is rejected instead
    of half-trusted.
    """
    try:
        actual = parse_release_env_text(env_path.read_text(encoding="utf-8"))
    except OSError as exc:
        return [f"cannot read {env_path}: {exc}"]
    expected = parse_release_env_text(render_release_env(manifest))
    problems = []
    for key in sorted(expected):
        if key not in actual:
            problems.append(f"release.env is missing {key}")
        elif actual[key] != expected[key]:
            problems.append(
                f"{key} disagrees: release.env={actual[key]!r} vs release.json={expected[key]!r}"
            )
    return problems


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_sha256sums(root: Path, relative_paths: Sequence[str]) -> None:
    """Write SHA256SUMS covering exactly the given POSIX relative paths."""
    entries = []
    for relative in sorted(set(relative_paths)):
        digest = sha256_file(root / relative)
        entries.append(f"{digest}  {relative}")
    (root / SHA256SUMS_NAME).write_text("\n".join(entries) + "\n", encoding="utf-8")


def validate_sums_relative_path(relative: str) -> str | None:
    """Return an error message when a SHA256SUMS path may escape the bundle.

    Entries must be relative POSIX paths that stay inside the bundle root:
    absolute paths, Windows drive letters, and any ``..`` segment are
    rejected. Builder output is always safe; this guards against a tampered
    checksum file pointing verification at files outside the bundle.
    """
    if not relative:
        return "empty path"
    normalized = relative.replace("\\", "/")
    if normalized.startswith("/"):
        return f"absolute paths are not allowed: {relative!r}"
    if re.fullmatch(r"[A-Za-z]:.*", normalized):
        return f"Windows drive paths are not allowed: {relative!r}"
    segments = normalized.split("/")
    if ".." in segments:
        return f"path traversal (..) is not allowed: {relative!r}"
    return None


def read_sha256sums(path: Path) -> list[tuple[str, str]]:
    """Parse SHA256SUMS into ``(sha256, posix_relative_path)`` pairs."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ReleaseManifestError(f"cannot read {path}: {exc}") from exc
    entries: list[tuple[str, str]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            raise ReleaseManifestError(f"malformed SHA256SUMS line: {line!r}")
        digest, relative = parts
        relative = relative.strip().lstrip("*")
        if not HEX_SHA256_PATTERN.fullmatch(digest):
            raise ReleaseManifestError(f"malformed sha256 in SHA256SUMS: {digest!r}")
        posix_relative = relative.replace("\\", "/")
        problem = validate_sums_relative_path(posix_relative)
        if problem:
            raise ReleaseManifestError(f"SHA256SUMS entry is not bundle-relative: {problem}")
        entries.append((digest, posix_relative))
    return entries


def verify_sha256sums(root: Path, entries: Sequence[tuple[str, str]]) -> list[str]:
    """Verify every listed file; returns a list of problems (empty when valid)."""
    problems: list[str] = []
    if not entries:
        return [f"{SHA256SUMS_NAME} lists no files"]
    for digest, relative in entries:
        target = root / relative
        if not target.is_file():
            problems.append(f"missing file listed in {SHA256SUMS_NAME}: {relative}")
            continue
        actual = sha256_file(target)
        if actual != digest:
            problems.append(f"checksum mismatch for {relative}: expected {digest}, got {actual}")
    return problems


def bundle_payload_files(root: Path) -> list[str]:
    """All bundle files (POSIX relative paths) that SHA256SUMS must cover."""
    files = []
    for path in sorted(root.rglob("*")):
        if path.is_file() and path.name != SHA256SUMS_NAME:
            files.append(path.relative_to(root).as_posix())
    return files


def verify_bundle(
    root: Path, *, expect_os: str | None = None, expect_arch: str | None = None
) -> list[str]:
    """Full structural + integrity verification of an extracted bundle.

    Returns a list of problems; an empty list means the bundle is valid.
    Checks: manifest parses and validates, platform expectations, required
    payload presence, SHA256SUMS covers every payload file, and every listed
    checksum matches.
    """
    problems: list[str] = []
    manifest_path = root / MANIFEST_NAME
    try:
        manifest = ReleaseManifest.load(manifest_path)
    except ReleaseManifestError as exc:
        return [str(exc)]

    if expect_os is not None and manifest.platform_os != expect_os:
        problems.append(f"bundle targets os={manifest.platform_os}, expected {expect_os}")
    if expect_arch is not None and manifest.platform_arch != expect_arch:
        problems.append(f"bundle targets arch={manifest.platform_arch}, expected {expect_arch}")

    sums_path = root / SHA256SUMS_NAME
    if not sums_path.is_file():
        problems.append(f"{SHA256SUMS_NAME} is missing")
        return problems
    try:
        entries = read_sha256sums(sums_path)
    except ReleaseManifestError as exc:
        problems.append(str(exc))
        return problems
    problems.extend(verify_sha256sums(root, entries))

    listed = {relative for _, relative in entries}
    required_files = [
        MANIFEST_NAME,
        RELEASE_ENV_NAME,
        HELPER_SCRIPT_NAME,
        DEPLOY_SCRIPT_NAMES[manifest.platform_os],
        f"{APP_DIR_NAME}/{manifest.app_wheel}",
        *(f"config/{name}" for name in CONFIG_EXAMPLES),
    ]
    if manifest.runtime_artifact is not None:
        required_files.append(f"{RUNTIME_DIR_NAME}/{manifest.runtime_artifact}")
    for required in required_files:
        if required not in listed:
            problems.append(f"{SHA256SUMS_NAME} does not cover required file: {required}")

    unlisted = [name for name in bundle_payload_files(root) if name not in listed]
    for name in unlisted:
        problems.append(f"bundle file not covered by {SHA256SUMS_NAME}: {name}")

    wheelhouse = root / WHEELHOUSE_DIR_NAME
    wheels = sorted(wheelhouse.glob("*.whl")) if wheelhouse.is_dir() else []
    if not wheels:
        problems.append("wheelhouse contains no wheels")
    return problems


def check_schema_compatibility(current: Mapping[str, Any], target: Mapping[str, Any]) -> list[str]:
    """Return blocking reasons for switching ``current`` to ``target``.

    A target whose schema is OLDER than the current one may read metadata that
    a newer release already migrated, so such switches (rollbacks or
    downgrades) must be blocked. Same or newer schema versions are allowed.
    """
    reasons: list[str] = []
    for key in SCHEMA_KEYS:
        current_version = int(current[key])
        target_version = int(target[key])
        if target_version < current_version:
            reasons.append(
                f"{key} downgrade {current_version} -> {target_version} is not allowed: "
                "the newer release may already have migrated the metadata"
            )
    return reasons


def install_state(release_dir: Path) -> str:
    """Classify a release directory as missing / incomplete / complete.

    A release only counts as installed once its ``installed.json`` completion
    marker exists; a leftover directory without the marker is a partial
    install that may be safely removed and rebuilt.
    """
    if not release_dir.is_dir():
        return INSTALL_STATE_MISSING
    if (release_dir / INSTALLED_MARKER_NAME).is_file():
        return INSTALL_STATE_COMPLETE
    return INSTALL_STATE_INCOMPLETE


def write_installed_marker(release_dir: Path, manifest: Mapping[str, Any]) -> Path:
    """Write the completion marker (release metadata + install timestamp)."""
    payload = dict(manifest)
    payload["installed_at"] = datetime.now(UTC).isoformat(timespec="seconds")
    path = release_dir / INSTALLED_MARKER_NAME
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def load_installed_marker(release_dir: Path) -> dict[str, Any]:
    path = release_dir / INSTALLED_MARKER_NAME
    try:
        data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReleaseManifestError(f"cannot read {path}: {exc}") from exc
    for key in ("release_id", *SCHEMA_KEYS):
        if key not in data:
            raise ReleaseManifestError(f"{path} is missing required key {key!r}")
    return data


SMOKE_APP_IMPORTS = (
    "airgap_sync",
    "click",
    "pydantic",
    "pymysql",
    "yaml",
    "requests",
    "zstandard",
)
SMOKE_RUNTIME_IMPORTS = ("ssl", "sqlite3", "ctypes", "zlib", "venv")


def smoke_test_imports(module_names: Sequence[str]) -> None:
    """Import each module or raise ImportError with the failing name."""
    for name in module_names:
        __import__(name)


def smoke_test_runtime_report() -> str:
    """One-line runtime capability report (python/openssl/sqlite versions)."""
    import sqlite3
    import ssl

    return (
        f"python={sys.version.split()[0]} "
        f"openssl={ssl.OPENSSL_VERSION} "
        f"sqlite={sqlite3.sqlite_version}"
    )


def _cli() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    verify_parser = sub.add_parser("verify-bundle", help="verify an extracted bundle")
    verify_parser.add_argument("root", type=Path)
    verify_parser.add_argument("--expect-os")
    verify_parser.add_argument("--expect-arch")

    schema_parser = sub.add_parser(
        "schema-check", help="check whether current may switch to target"
    )
    schema_parser.add_argument("current", type=Path, help="current installed.json")
    schema_parser.add_argument("target", type=Path, help="target installed.json or release.json")

    state_parser = sub.add_parser("install-state", help="print release directory state")
    state_parser.add_argument("release_dir", type=Path)

    env_parser = sub.add_parser("check-env", help="verify release.env agrees with release.json")
    env_parser.add_argument("root", type=Path, help="extracted bundle root")

    marker_parser = sub.add_parser(
        "mark-installed", help="write the installed.json completion marker"
    )
    marker_parser.add_argument("release_dir", type=Path)
    marker_parser.add_argument("release_json", type=Path)

    sub.add_parser("smoke-app", help="import application dependencies")
    sub.add_parser("smoke-runtime", help="import runtime stdlib modules and report versions")

    args = parser.parse_args()
    if args.command == "verify-bundle":
        problems = verify_bundle(args.root, expect_os=args.expect_os, expect_arch=args.expect_arch)
        for problem in problems:
            print(f"ERROR: {problem}")
        if problems:
            return 1
        print("Bundle verification OK")
        return 0
    if args.command == "schema-check":
        current = json.loads(args.current.read_text(encoding="utf-8"))
        target = json.loads(args.target.read_text(encoding="utf-8"))
        reasons = check_schema_compatibility(current, target)
        for reason in reasons:
            print(f"BLOCKED: {reason}")
        if reasons:
            return 2
        print("Schema compatibility OK")
        return 0
    if args.command == "install-state":
        print(install_state(args.release_dir))
        return 0
    if args.command == "check-env":
        try:
            manifest = ReleaseManifest.load(args.root / MANIFEST_NAME)
        except ReleaseManifestError as exc:
            print(f"ERROR: {exc}")
            return 1
        problems = release_env_problems(manifest, args.root / RELEASE_ENV_NAME)
        for problem in problems:
            print(f"ERROR: {problem}")
        if problems:
            return 1
        print("release.env and release.json agree")
        return 0
    if args.command == "mark-installed":
        manifest = ReleaseManifest.load(args.release_json)
        write_installed_marker(args.release_dir, manifest.to_dict())
        return 0
    if args.command == "smoke-app":
        smoke_test_imports(SMOKE_APP_IMPORTS)
        print("Application imports OK")
        return 0
    smoke_test_imports(SMOKE_RUNTIME_IMPORTS)
    print(smoke_test_runtime_report())
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
