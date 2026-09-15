#!/usr/bin/env python3
"""Build self-contained offline release bundles for Windows and CentOS 7.

Run on a networked development machine from the repository root:

    uv run python scripts/offline/build_release.py [--allow-dirty] [--include-tests]

Produces under dist/offline/:
    airgap-sync-<version>-<gitsha>-windows-amd64.zip
    airgap-sync-<version>-<gitsha>-centos7-x86_64.tar.gz
    release-summary.json

Determinism: runtime dependencies come exclusively from the current uv.lock
(``uv export --locked`` cross-checked against the lock's dependency closure),
the application wheel is built from the current checkout, and the pinned
Python runtimes come from scripts/offline/runtime-versions.json.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tarfile
import tomllib
import urllib.request
import zipfile
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import release_manifest as rm

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
RUNTIME_VERSIONS_PATH = SCRIPT_DIR / "runtime-versions.json"
BUILD_DIR_NAME = ".offline-build"
BUNDLE_ROOT_NAME = "airgap-sync-release"
DOCS_FILE = REPO_ROOT / "docs" / "offline-deployment.md"
SERVICE_EXAMPLES_DIR = SCRIPT_DIR / "service-examples"

TRANSIENT_BUILD_ENTRIES = (
    "app",
    "dl-venv",
    "bundle-windows",
    "bundle-linux",
    "wheelhouse-windows",
    "wheelhouse-linux",
    "wheelhouse-tests",
    "wheelhouse-tests-merged",
    "requirements-runtime.txt",
)

# pip --platform expansion accepts these and resolves every manylinux wheel
# compatible with glibc <= 2.17 (manylinux2014 == manylinux_2_17).
# Generic linux_x86_64 is deliberately absent: such wheels carry no glibc ABI
# promise and may be built against glibc > 2.17 (they would fail to load on
# CentOS 7). The post-download ABI guard below rejects any wheel that slips
# through with no glibc <= 2.17 tag at all.
LINUX_DOWNLOAD_PLATFORMS = (
    "manylinux2014_x86_64",
    "manylinux_2_17_x86_64",
    "manylinux1_x86_64",
    "manylinux_2_5_x86_64",
)
WINDOWS_DOWNLOAD_PLATFORMS = ("win_amd64",)

# Platform tags CentOS 7 (glibc 2.17) can load. manylinux_2_N with
# (2, N) <= (2, 17) is accepted via MANYLINUX_TAG_PATTERN.
CENTOS7_COMPATIBLE_PLATFORM_TAGS = frozenset(
    {
        "manylinux1_x86_64",
        "manylinux2010_x86_64",
        "manylinux2014_x86_64",
        "manylinux_2_5_x86_64",
        "manylinux_2_12_x86_64",
        "manylinux_2_17_x86_64",
    }
)
MANYLINUX_TAG_PATTERN = re.compile(r"^manylinux_2_(\d+)_(\d+)_x86_64$")

PLATFORM_SPECS = {
    "windows": {
        "archive_suffix": "windows-amd64.zip",
        "arch": "amd64",
        "deploy_script": "airgap-sync-deploy.ps1",
        "pip_platforms": WINDOWS_DOWNLOAD_PLATFORMS,
        "minimum_glibc": None,
    },
    "linux": {
        "archive_suffix": "centos7-x86_64.tar.gz",
        "arch": "x86_64",
        "deploy_script": "airgap-sync-deploy.sh",
        "pip_platforms": LINUX_DOWNLOAD_PLATFORMS,
        "minimum_glibc": rm.LINUX_MINIMUM_GLIBC,
    },
}


class BuildError(Exception):
    """Release build failed."""


def normalize_name(name: str) -> str:
    """PEP 503 normalized distribution name."""
    return re.sub(r"[-_.]+", "-", name).lower()


def stage(message: str) -> None:
    print(message, flush=True)


def run(command: Sequence[str], *, cwd: Path | None = None) -> str:
    try:
        result = subprocess.run(command, cwd=cwd, check=True, capture_output=True, text=True)
    except FileNotFoundError as exc:
        raise BuildError(f"required executable not found: {command[0]}") from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stdout or "") + (exc.stderr or "")
        raise BuildError(f"command failed: {' '.join(command)}\n{detail.strip()}") from exc
    return result.stdout.strip()


def preflight(allow_dirty: bool) -> tuple[str, str]:
    """Verify repository/toolchain state; returns (full git sha, app version)."""
    stage("[1/8] Preflight checks")
    if shutil.which("git") is None:
        raise BuildError("git executable not found on PATH")
    if shutil.which("uv") is None:
        raise BuildError("uv executable not found on PATH")
    if run(["git", "rev-parse", "--is-inside-work-tree"], cwd=REPO_ROOT) != "true":
        raise BuildError(f"{REPO_ROOT} is not inside a git work tree")
    pyproject = REPO_ROOT / "pyproject.toml"
    if not pyproject.is_file():
        raise BuildError("pyproject.toml not found")
    if not (REPO_ROOT / "uv.lock").is_file():
        raise BuildError("uv.lock not found")

    status = run(["git", "status", "--porcelain"], cwd=REPO_ROOT)
    if status:
        if not allow_dirty:
            raise BuildError(
                f"working tree is dirty; commit first or pass --allow-dirty:\n{status}"
            )
        print("WARNING: building from a dirty work tree (--allow-dirty)")

    git_commit = run(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT)
    with pyproject.open("rb") as handle:
        app_version = tomllib.load(handle)["project"]["version"]
    return git_commit, str(app_version)


def load_runtime_versions() -> dict:
    stage("[2/8] Load pinned Python runtime versions")
    data = json.loads(RUNTIME_VERSIONS_PATH.read_text(encoding="utf-8"))
    python_version = data["python"]
    if python_version.count(".") != 2:
        raise BuildError(f"runtime python version must be X.Y.Z, got {python_version!r}")
    for key, entry in data.items():
        if not isinstance(entry, dict) or "url" not in entry:
            continue
        reference = entry.get("artifact") or entry.get("installer") or ""
        if python_version not in entry["url"] or python_version not in reference:
            raise BuildError(
                f"{key} runtime URL/artifact does not match pinned python {python_version}"
            )
    return data


def lock_closures() -> tuple[dict[str, str], dict[str, str]]:
    """Runtime and dev-only dependency closures from uv.lock (name -> version)."""
    lock = tomllib.loads((REPO_ROOT / "uv.lock").read_text(encoding="utf-8"))
    packages = {package["name"]: package for package in lock["package"]}
    with (REPO_ROOT / "pyproject.toml").open("rb") as handle:
        project_name = tomllib.load(handle)["project"]["name"]
    root = packages[project_name]

    def closure(seed_names: set[str]) -> dict[str, str]:
        found: dict[str, str] = {}
        stack = sorted(seed_names)
        while stack:
            name = stack.pop()
            if name in found or name == project_name:
                continue
            package = packages[name]
            found[name] = str(package["version"])
            for dependency in package.get("dependencies") or []:
                stack.append(dependency["name"])
        return found

    runtime = closure({dependency["name"] for dependency in root["dependencies"]})
    dev_seed = {
        dependency["name"]
        for group in (root.get("dev-dependencies") or {}).values()
        for dependency in group
    }
    dev_only = {name: version for name, version in closure(dev_seed).items() if name not in runtime}
    return runtime, dev_only


def parse_requirement_pins(text: str) -> dict[str, str]:
    """Parse `name==version` lines from a uv export requirements file."""
    pins: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        base = line.split(";", 1)[0].strip()
        name, separator, version = base.partition("==")
        if not separator:
            raise BuildError(f"unexpected requirements line (expected name==version): {line}")
        pins[normalize_name(name.strip())] = version.strip()
    return pins


def export_runtime_requirements(build_dir: Path, lock_runtime: dict[str, str]) -> dict[str, str]:
    """uv export the locked runtime set, completed with marker-only lock entries.

    uv export evaluates environment markers for the build machine's platform,
    which can drop dependencies only needed on the other platform. The uv.lock
    closure is authoritative: any closure package missing from the export is
    added at its locked version, and any version disagreement fails the build.
    """
    requirements_path = build_dir / "requirements-runtime.txt"
    run(
        [
            "uv",
            "export",
            "--format",
            "requirements-txt",
            "--no-dev",
            "--no-hashes",
            "--no-annotate",
            "--no-emit-project",
            "--locked",
            "--output-file",
            str(requirements_path),
        ],
        cwd=REPO_ROOT,
    )
    exported = parse_requirement_pins(requirements_path.read_text(encoding="utf-8"))

    pins: dict[str, str] = {}
    for name, version in sorted(lock_runtime.items()):
        exported_version = exported.get(name)
        if exported_version is None:
            print(
                f"  note: {name}=={version} is marker-only on this platform; "
                "added from uv.lock closure"
            )
            pins[name] = version
        elif exported_version != version:
            raise BuildError(
                f"uv export disagrees with uv.lock for {name}: {exported_version} vs {version}"
            )
        else:
            pins[name] = version
    for name in sorted(set(exported) - set(lock_runtime)):
        raise BuildError(f"uv export contains {name} which is not in the lock runtime closure")
    return pins


def test_requirements(lock_runtime: dict[str, str], dev_only: dict[str, str]) -> dict[str, str]:
    """pytest and its dependencies (dev closure minus ruff, minus runtime)."""
    excluded = {"ruff"}
    return {
        name: version
        for name, version in sorted(dev_only.items())
        if name not in excluded and name not in lock_runtime
    }


def build_app_wheel(build_dir: Path) -> Path:
    app_dir = build_dir / "app"
    app_dir.mkdir(parents=True, exist_ok=True)
    run(["uv", "build", "--wheel", "--out-dir", str(app_dir)], cwd=REPO_ROOT)
    wheels = sorted(app_dir.glob("*.whl"))
    if len(wheels) != 1:
        raise BuildError(f"expected exactly one app wheel, found: {[w.name for w in wheels]}")
    return wheels[0]


def seed_download_venv(build_dir: Path, python_version: str) -> Path:
    venv_dir = build_dir / "dl-venv"
    if venv_dir.exists():
        shutil.rmtree(venv_dir)
    major_minor = ".".join(python_version.split(".")[:2])
    run(["uv", "venv", "--seed", "--python", major_minor, str(venv_dir)], cwd=REPO_ROOT)
    python = venv_dir / "bin" / "python"
    if not python.exists():  # pragma: no cover - Windows build hosts are unsupported
        raise BuildError("seeded download venv has no bin/python; build from Linux/macOS")
    return python


def download_wheels(
    pip_python: Path,
    pins: dict[str, str],
    destination: Path,
    platforms: Sequence[str],
    python_version: str,
    label: str,
) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    pins_file = destination.parent / f"pins-{destination.name}.txt"
    pins_file.write_text(
        "".join(f"{name}=={version}\n" for name, version in sorted(pins.items())),
        encoding="utf-8",
    )
    major_minor = ".".join(python_version.split(".")[:2])
    command = [
        str(pip_python),
        "-m",
        "pip",
        "download",
        "--quiet",
        "-r",
        str(pins_file),
        "--dest",
        str(destination),
        "--only-binary=:all:",
        "--implementation",
        "cp",
        "--python-version",
        major_minor,
    ]
    for platform in platforms:
        command += ["--platform", platform]
    try:
        run(command)
    except BuildError as exc:
        raise BuildError(f"wheel download failed for {label}: {exc}") from exc


def validate_wheelhouse(directory: Path, pins: dict[str, str]) -> tuple[int, list[str], list[str]]:
    """Return (wheel count, unresolved dependency names, non-wheel files)."""
    if not directory.is_dir():
        return 0, sorted(pins), []
    files = sorted(path.name for path in directory.iterdir() if path.is_file())
    sdists = [name for name in files if not name.endswith(".whl")]
    wheel_names = {normalize_name(name.split("-")[0]) for name in files if name.endswith(".whl")}
    unresolved = [name for name in sorted(pins) if name not in wheel_names]
    return len(wheel_names), unresolved, sdists


def is_centos7_compatible_platform_tag(tag: str) -> bool:
    """True when a single wheel platform tag promises glibc <= 2.17 x86_64."""
    if tag in CENTOS7_COMPATIBLE_PLATFORM_TAGS:
        return True
    match = MANYLINUX_TAG_PATTERN.fullmatch(tag)
    return bool(match) and (int(match.group(1)), int(match.group(2))) <= (2, 17)


def wheel_platform_tags(wheel_name: str) -> list[str]:
    """Platform tags from a wheel filename (``{...}-{py}-{abi}-{platform}.whl``)."""
    stem = wheel_name[:-4] if wheel_name.endswith(".whl") else wheel_name
    parts = stem.split("-")
    if len(parts) < 5:
        raise BuildError(f"malformed wheel filename: {wheel_name!r}")
    return parts[-1].split(".")


def is_universal_wheel(wheel_name: str) -> bool:
    """py3-none-any style wheels run anywhere."""
    return wheel_platform_tags(wheel_name) == ["any"]


def linux_abi_problems(wheel_names: Sequence[str]) -> list[str]:
    """Reject Linux wheels whose platform offers no glibc <= 2.17 tag.

    A wheel with multiple platform tags is fine as long as ONE tag is
    CentOS 7 compatible (pip picks the best tag at install time). Universal
    ``*-any.whl`` wheels are always allowed. Generic ``linux_x86_64``,
    ``musllinux_*`` and ``manylinux_2_18+``-only wheels must not enter the
    CentOS 7 wheelhouse.
    """
    problems: list[str] = []
    for name in wheel_names:
        try:
            tags = wheel_platform_tags(name)
        except BuildError as exc:
            problems.append(str(exc))
            continue
        if is_universal_wheel(name):
            continue
        if not any(is_centos7_compatible_platform_tag(tag) for tag in tags):
            problems.append(
                f"{name}: no CentOS 7 (glibc <= 2.17) compatible platform tag "
                f"(tags: {', '.join(tags)})"
            )
    return problems


def non_universal_platform_tags(wheel_names: Sequence[str]) -> list[str]:
    """Sorted distinct platform tags of all non-universal wheels (reporting)."""
    tags: set[str] = set()
    for name in wheel_names:
        if not is_universal_wheel(name):
            tags.update(wheel_platform_tags(name))
    return sorted(tags)


def fetch_url(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + ".part")
    with urllib.request.urlopen(url, timeout=300) as response, partial.open("wb") as handle:
        shutil.copyfileobj(response, handle, 1024 * 1024)
    partial.replace(destination)


def parse_sums_text(text: str) -> dict[str, str]:
    sums: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or not line[0].isalnum():
            continue
        digest, _, name = line.partition(" ")
        name = name.strip().lstrip("*")
        if name:
            sums[name] = digest.lower()
    return sums


def fetch_linux_runtime(build_dir: Path, spec: dict) -> tuple[Path, str, bool]:
    """Download the python-build-standalone runtime and verify it upstream.

    Returns (artifact path, sha256, upstream_verified). The pinned sha256 in
    runtime-versions.json is cross-checked against the live SHA256SUMS from
    the same PBS release; any disagreement fails the build. The downloaded
    artifact is cached across builds under .offline-build/downloads/.
    """
    downloads = build_dir / "downloads"
    downloads.mkdir(parents=True, exist_ok=True)
    artifact = downloads / spec["artifact"]
    if artifact.exists():
        print(f"  runtime cached: {artifact.name}")
    else:
        print(f"  downloading {spec['url']}")
        fetch_url(spec["url"], artifact)

    print(f"  verifying against upstream {spec['sha256sums_url'].rsplit('/', 1)[-1]}")
    with urllib.request.urlopen(spec["sha256sums_url"], timeout=120) as response:
        sums_text = response.read().decode("utf-8")
    upstream = parse_sums_text(sums_text)
    upstream_sha = upstream.get(spec["artifact"])
    if upstream_sha is None:
        raise BuildError(f"artifact {spec['artifact']} missing from upstream SHA256SUMS")
    digest = rm.sha256_file(artifact)
    if digest != upstream_sha:
        raise BuildError(f"runtime sha256 mismatch: upstream {upstream_sha} vs downloaded {digest}")
    pinned = spec.get("sha256")
    if pinned and pinned != upstream_sha:
        raise BuildError(
            f"runtime-versions.json pinned sha256 is stale: pinned {pinned} "
            f"vs upstream {upstream_sha}"
        )
    return artifact, digest, True


def fetch_windows_runtime(build_dir: Path, spec: dict) -> tuple[Path, str, bool]:
    """Download the official CPython installer.

    python.org publishes no machine-readable checksum file next to the
    installer, so there is no automated upstream verification: a WARNING is
    recorded and the downloaded artifact's own sha256 is written into the
    bundle's SHA256SUMS so deploy-side verification is still exact.
    """
    downloads = build_dir / "downloads"
    downloads.mkdir(parents=True, exist_ok=True)
    artifact = downloads / spec["installer"]
    if artifact.exists():
        print(f"  runtime cached: {artifact.name}")
    else:
        print(f"  downloading {spec['url']}")
        fetch_url(spec["url"], artifact)
    print(
        "  WARNING: python.org installers have no auto-verifiable upstream checksum; "
        "recording downloaded artifact sha256 into SHA256SUMS"
    )
    return artifact, rm.sha256_file(artifact), False


def copy_tree_contents(source: Path, destination: Path) -> None:
    for path in sorted(source.rglob("*")):
        if path.is_dir() or "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        target = destination / path.relative_to(source)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)


def assemble_bundle(
    build_dir: Path,
    platform: str,
    manifest: rm.ReleaseManifest,
    app_wheel: Path,
    runtime_artifact: Path,
    wheelhouse: Path,
    test_wheelhouse: Path | None,
) -> Path:
    spec = PLATFORM_SPECS[platform]
    bundle_root = build_dir / f"bundle-{platform}" / BUNDLE_ROOT_NAME
    if bundle_root.parent.exists():
        shutil.rmtree(bundle_root.parent)
    (bundle_root / rm.RUNTIME_DIR_NAME).mkdir(parents=True)
    (bundle_root / rm.APP_DIR_NAME).mkdir(parents=True)
    (bundle_root / rm.WHEELHOUSE_DIR_NAME).mkdir(parents=True)

    shutil.copy2(runtime_artifact, bundle_root / rm.RUNTIME_DIR_NAME / runtime_artifact.name)
    shutil.copy2(app_wheel, bundle_root / rm.APP_DIR_NAME / app_wheel.name)
    for wheel in sorted(wheelhouse.glob("*.whl")):
        shutil.copy2(wheel, bundle_root / rm.WHEELHOUSE_DIR_NAME / wheel.name)
    if test_wheelhouse is not None:
        test_dir = bundle_root / "test-wheelhouse"
        test_dir.mkdir()
        for wheel in sorted(test_wheelhouse.glob("*.whl")):
            shutil.copy2(wheel, test_dir / wheel.name)
        copy_tree_contents(REPO_ROOT / "tests", bundle_root / "tests")

    config_dir = bundle_root / "config"
    config_dir.mkdir()
    shutil.copy2(REPO_ROOT / "config" / "config.example.yaml", config_dir / "source.example.yaml")
    shutil.copy2(
        REPO_ROOT / "config" / "config.destination.example.yaml",
        config_dir / "destination.example.yaml",
    )
    service_dir = bundle_root / "service"
    service_dir.mkdir()
    for example in sorted(SERVICE_EXAMPLES_DIR.iterdir()):
        shutil.copy2(example, service_dir / example.name)

    shutil.copy2(SCRIPT_DIR / spec["deploy_script"], bundle_root / spec["deploy_script"])
    shutil.copy2(SCRIPT_DIR / rm.HELPER_SCRIPT_NAME, bundle_root / rm.HELPER_SCRIPT_NAME)
    if DOCS_FILE.is_file():
        shutil.copy2(DOCS_FILE, bundle_root / "OFFLINE-DEPLOYMENT.md")

    manifest.dump(bundle_root / rm.MANIFEST_NAME)
    rm.write_release_env(manifest, bundle_root / rm.RELEASE_ENV_NAME)
    rm.write_sha256sums(bundle_root, rm.bundle_payload_files(bundle_root))

    problems = rm.verify_bundle(bundle_root, expect_os=platform, expect_arch=spec["arch"])
    if problems:
        raise BuildError(f"bundle self-check failed for {platform}: " + "; ".join(problems))
    return bundle_root


def archive_bundle(bundle_root: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        destination.unlink()
    files = sorted(path for path in bundle_root.rglob("*") if path.is_file())
    if destination.suffix == ".zip":
        with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in files:
                archive.write(path, f"{BUNDLE_ROOT_NAME}/{path.relative_to(bundle_root)}")
    else:
        with tarfile.open(destination, "w:gz") as archive:
            for path in files:
                archive.add(
                    path,
                    arcname=f"{BUNDLE_ROOT_NAME}/{path.relative_to(bundle_root)}",
                    recursive=False,
                )


def clean_build_dir(build_dir: Path) -> None:
    """Remove transient outputs but keep the runtime download cache."""
    build_dir.mkdir(parents=True, exist_ok=True)
    for name in TRANSIENT_BUILD_ENTRIES:
        target = build_dir / name
        if target.is_dir():
            shutil.rmtree(target)
        elif target.exists():
            target.unlink()
    for pins in build_dir.glob("pins-*.txt"):
        pins.unlink()


def build(include_tests: bool, allow_dirty: bool, output_dir: Path, keep_build: bool) -> dict:
    started_at = datetime.now(UTC)
    git_commit, app_version = preflight(allow_dirty)
    runtime_versions = load_runtime_versions()
    python_version = runtime_versions["python"]
    release_id = rm.make_release_id(app_version, git_commit)

    build_dir = REPO_ROOT / BUILD_DIR_NAME
    clean_build_dir(build_dir)

    stage("[3/8] Resolve locked runtime dependencies from uv.lock")
    lock_runtime, lock_dev_only = lock_closures()
    runtime_pins = export_runtime_requirements(build_dir, lock_runtime)
    print(f"  runtime dependencies: {len(runtime_pins)}")

    stage("[4/8] Build application wheel")
    app_wheel = build_app_wheel(build_dir)
    print(f"  {app_wheel.name}")

    stage("[5/8] Download binary wheels for both platforms")
    pip_python = seed_download_venv(build_dir, python_version)
    wheelhouses: dict[str, Path] = {}
    for platform, spec in PLATFORM_SPECS.items():
        destination = build_dir / f"wheelhouse-{platform}"
        download_wheels(
            pip_python, runtime_pins, destination, spec["pip_platforms"], python_version, platform
        )
        wheelhouses[platform] = destination
    test_pins: dict[str, str] = {}
    test_wheelhouse: Path | None = None
    if include_tests:
        test_pins = test_requirements(lock_runtime, lock_dev_only)
        per_platform_dir = build_dir / "wheelhouse-tests"
        for platform, spec in PLATFORM_SPECS.items():
            download_wheels(
                pip_python,
                test_pins,
                per_platform_dir / platform,
                spec["pip_platforms"],
                python_version,
                f"tests-{platform}",
            )
        merged = build_dir / "wheelhouse-tests-merged"
        merged.mkdir()
        seen: set[str] = set()
        for platform in PLATFORM_SPECS:
            for wheel in sorted((per_platform_dir / platform).glob("*.whl")):
                if wheel.name in seen:
                    continue
                seen.add(wheel.name)
                shutil.copy2(wheel, merged / wheel.name)
        test_wheelhouse = merged

    stage("[6/8] Verify wheelhouse completeness (no sdists, no unresolved)")
    summary_platforms: dict[str, dict] = {}
    for platform in PLATFORM_SPECS:
        wheels, unresolved, sdists = validate_wheelhouse(wheelhouses[platform], runtime_pins)
        if sdists:
            raise BuildError(
                f"{platform} wheelhouse contains non-wheel files "
                f"(offline machines must not build from source): {sdists}"
            )
        if unresolved:
            raise BuildError(f"{platform} wheelhouse is missing binary wheels for: {unresolved}")
        wheel_names = [path.name for path in sorted(wheelhouses[platform].glob("*.whl"))]
        abi_note = ""
        if platform == "linux":
            problems = linux_abi_problems(wheel_names)
            if problems:
                raise BuildError(
                    "linux wheelhouse contains wheels CentOS 7 cannot load "
                    "(no glibc <= 2.17 compatible tag): " + "; ".join(problems)
                )
            abi_note = "\n  non-universal wheel tags all glibc <= 2.17 compatible"
        print(
            f"{platform}:\n"
            f"  runtime dependencies: {len(runtime_pins)}\n"
            f"  wheels: {wheels}\n"
            f"  unresolved: 0{abi_note}"
        )
        summary_platforms[platform] = {
            "wheels": wheels,
            "platform_tags": non_universal_platform_tags(wheel_names),
        }
    if include_tests:
        linux_test_problems = linux_abi_problems(
            [path.name for path in sorted((build_dir / "wheelhouse-tests" / "linux").glob("*.whl"))]
        )
        if linux_test_problems:
            raise BuildError(
                "linux test wheelhouse contains wheels CentOS 7 cannot load: "
                + "; ".join(linux_test_problems)
            )
        wheels, unresolved, sdists = validate_wheelhouse(test_wheelhouse, test_pins)
        if sdists or unresolved:
            raise BuildError(f"test wheelhouse invalid: sdists={sdists} unresolved={unresolved}")
        print(f"tests:\n  wheels: {wheels}\n  unresolved: 0")
        summary_platforms["tests"] = {"wheels": wheels}

    stage("[7/8] Fetch pinned Python runtimes")
    linux_artifact, linux_sha, linux_verified = fetch_linux_runtime(
        build_dir, runtime_versions["linux_x86_64"]
    )
    windows_artifact, windows_sha, windows_verified = fetch_windows_runtime(
        build_dir, runtime_versions["windows"]
    )
    print(f"  linux   {linux_artifact.name} (upstream checksum verified: {linux_verified})")
    print(f"  windows {windows_artifact.name} (upstream checksum verified: {windows_verified})")

    stage("[8/8] Assemble and package release bundles")
    schemas = rm.schema_versions()
    output_dir.mkdir(parents=True, exist_ok=True)
    archives: dict[str, Path] = {}
    runtime_by_platform = {"windows": windows_artifact, "linux": linux_artifact}
    for platform, spec in PLATFORM_SPECS.items():
        manifest = rm.ReleaseManifest(
            release_id=release_id,
            app_version=app_version,
            git_commit=git_commit,
            created_at=started_at.isoformat(timespec="seconds"),
            python_version=python_version,
            platform_os=platform,
            platform_arch=spec["arch"],
            minimum_glibc=spec["minimum_glibc"],
            source_state_schema=schemas["source_state_schema"],
            destination_metadata_schema=schemas["destination_metadata_schema"],
            runtime_artifact=runtime_by_platform[platform].name,
            app_wheel=app_wheel.name,
            wheel_count=len(runtime_pins),
            include_tests=include_tests,
        )
        bundle_root = assemble_bundle(
            build_dir,
            platform,
            manifest,
            app_wheel,
            runtime_by_platform[platform],
            wheelhouses[platform],
            test_wheelhouse if include_tests else None,
        )
        archive_path = output_dir / (
            f"airgap-sync-{app_version}-{git_commit[:7]}-{spec['archive_suffix']}"
        )
        archive_bundle(bundle_root, archive_path)
        archives[platform] = archive_path
        print(f"  {archive_path.name} ({archive_path.stat().st_size:,} bytes)")

    summary = {
        "release_id": release_id,
        "git_commit": git_commit,
        "app_version": app_version,
        "python_version": python_version,
        "created_at": started_at.isoformat(timespec="seconds"),
        "include_tests": include_tests,
        "source_state_schema": schemas["source_state_schema"],
        "destination_metadata_schema": schemas["destination_metadata_schema"],
        "runtime_dependencies": len(runtime_pins),
        "platforms": {},
    }
    for platform, archive_path in archives.items():
        spec = PLATFORM_SPECS[platform]
        try:
            archive_location = str(archive_path.relative_to(REPO_ROOT))
        except ValueError:
            archive_location = str(archive_path)
        summary["platforms"][spec["archive_suffix"]] = {
            "path": archive_location,
            "size": archive_path.stat().st_size,
            "sha256": rm.sha256_file(archive_path),
            "runtime_artifact": runtime_by_platform[platform].name,
            "runtime_sha256": (windows_sha if platform == "windows" else linux_sha),
            "runtime_upstream_checksum_verified": (
                windows_verified if platform == "windows" else linux_verified
            ),
            "runtime_wheel_count": summary_platforms[platform]["wheels"],
            "non_universal_platform_tags": summary_platforms[platform]["platform_tags"],
        }
    if include_tests:
        summary["test_wheel_count"] = summary_platforms["tests"]["wheels"]
    summary_path = output_dir / "release-summary.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    print()
    print(f"Release ID:      {release_id}")
    print(f"Git commit:      {git_commit}")
    print(f"Python:          {python_version}")
    print(f"App version:     {app_version}")
    for platform, archive_path in archives.items():
        spec = PLATFORM_SPECS[platform]
        print(f"{spec['archive_suffix']} package:")
        print(f"  path:   {archive_path}")
        print(f"  size:   {archive_path.stat().st_size:,}")
        print(f"  sha256: {rm.sha256_file(archive_path)}")
    print("Runtime wheel count:")
    for platform in PLATFORM_SPECS:
        spec = PLATFORM_SPECS[platform]
        count = summary["platforms"][spec["archive_suffix"]]["runtime_wheel_count"]
        print(f"  {platform}: {count}")
    print(f"Tests included:  {'yes' if include_tests else 'no'}")
    if not keep_build:
        clean_build_dir(build_dir)
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--allow-dirty", action="store_true", help="allow building from a dirty work tree"
    )
    parser.add_argument(
        "--include-tests",
        action="store_true",
        help="bundle tests/ plus pytest wheels for offline integration testing",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "dist" / "offline",
        help="output directory (default: dist/offline)",
    )
    parser.add_argument(
        "--keep-build", action="store_true", help="keep the .offline-build scratch directory"
    )
    args = parser.parse_args(argv)
    try:
        build(args.include_tests, args.allow_dirty, args.output, args.keep_build)
    except BuildError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
