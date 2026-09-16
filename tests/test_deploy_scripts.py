"""部署脚本检查: bash 语法、Linux 真实功能测试、PowerShell 静态约束、service 示例约束。"""

from __future__ import annotations

import io
import os
import platform as python_platform
import re
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest

import release_manifest as rm

OFFLINE_DIR = Path(__file__).resolve().parent.parent / "scripts" / "offline"
DEPLOY_SH = OFFLINE_DIR / "airgap-sync-deploy.sh"
DEPLOY_PS1 = OFFLINE_DIR / "airgap-sync-deploy.ps1"
SERVICE_DIR = OFFLINE_DIR / "service-examples"

APP_VERSION = "0.1.0"
TEST_DIST = "airgapsynctestapp"
COMMIT_A = "aa" * 20
COMMIT_C = "bb" * 20
RELEASE_A = f"{APP_VERSION}-{COMMIT_A[:7]}"
RELEASE_C = f"{APP_VERSION}-{COMMIT_C[:7]}"
PYTHON_15 = "3.13.15"
PYTHON_16 = "3.13.16"


class TestShellScript:
    def test_bash_syntax(self) -> None:
        if shutil.which("bash") is None:
            pytest.skip("bash not available")
        result = subprocess.run(["bash", "-n", str(DEPLOY_SH)], capture_output=True, text=True)
        assert result.returncode == 0, result.stderr

    def test_script_is_executable(self) -> None:
        assert os.access(DEPLOY_SH, os.X_OK), f"{DEPLOY_SH} must carry the +x bit"

    def test_uses_only_centos7_base_tools(self) -> None:
        text = DEPLOY_SH.read_text(encoding="utf-8")
        for forbidden in ("jq ", "yq ", "realpath -f", "parallel"):
            assert forbidden not in text, forbidden


class TestPowerShellScript:
    def test_syntax(self) -> None:
        pwsh = shutil.which("pwsh")
        if pwsh is None:
            pytest.skip(
                "pwsh not available; must be validated on the first Windows Server "
                "2019 deployment (see docs/offline-deployment.md)"
            )
        command = (
            "$errs = $null; "
            "$null = [System.Management.Automation.Language.Parser]::ParseFile("
            f"'{DEPLOY_PS1}', [ref]$null, [ref]$errs); "
            "if ($errs.Count -gt 0) { $errs | ForEach-Object { Write-Host $_.Message }; exit 1 }"
        )
        result = subprocess.run(
            [pwsh, "-NoProfile", "-NonInteractive", "-Command", command],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr

    def test_external_python_payload_and_temporary_venv_execute(self, tmp_path: Path) -> None:
        if os.name != "nt":
            pytest.skip("requires Windows PowerShell/pwsh and Windows python.exe")
        powershell = shutil.which("powershell") or shutil.which("pwsh")
        if powershell is None:
            pytest.skip("PowerShell unavailable")
        python = Path(sys.executable)
        if python.name.lower() != "python.exe":
            pytest.skip("test interpreter is not python.exe")
        version = python_platform.python_version()
        command = (
            f". '{DEPLOY_PS1}' -Action Status -InstallRoot '{tmp_path}'; "
            f"$manifest = [pscustomobject]@{{python_version = '{version}'}}; "
            "Test-ExternalPython $manifest"
        )
        env = os.environ.copy()
        env["PATH"] = str(python.parent) + os.pathsep + env.get("PATH", "")
        result = subprocess.run(
            [powershell, "-NoProfile", "-NonInteractive", "-Command", command],
            capture_output=True,
            text=True,
            env=env,
            timeout=180,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "Python architecture: 64-bit" in result.stdout
        assert "temporary venv" not in result.stderr.lower()

    @pytest.mark.parametrize("mode", ["normal", "cleanup-failure", "rename-failure"])
    def test_switch_current_real_junction_and_recovery(self, tmp_path: Path, mode: str) -> None:
        if os.name != "nt":
            pytest.skip("requires Windows junction semantics")
        powershell = shutil.which("powershell") or shutil.which("pwsh")
        if powershell is None:
            pytest.skip("PowerShell unavailable")
        root = tmp_path / mode
        quoted_script = str(DEPLOY_PS1).replace("'", "''")
        quoted_root = str(root).replace("'", "''")
        setup = rf"""
. '{quoted_script}' -Action Status -InstallRoot '{quoted_root}'
$releases = Join-Path $InstallRoot 'releases'
$a = Join-Path $releases 'release-A'
$b = Join-Path $releases 'release-B'
New-Item -ItemType Directory -Path (Join-Path $a 'nested') -Force | Out-Null
New-Item -ItemType Directory -Path (Join-Path $b 'nested') -Force | Out-Null
Set-Content -LiteralPath (Join-Path $a 'nested\a.txt') -Value 'A'
Set-Content -LiteralPath (Join-Path $b 'nested\b.txt') -Value 'B'
New-Item -ItemType Junction -Path (Join-Path $InstallRoot 'current') -Value $a | Out-Null
"""
        if mode == "cleanup-failure":
            action = """
function Remove-JunctionSafely([string] $Path) { throw 'simulated cleanup failure' }
Switch-Current 'release-B'
if ((Get-CurrentReleaseId) -ne 'release-B') { exit 20 }
"""
        elif mode == "rename-failure":
            action = r"""
function Rename-Item {
    param([string] $LiteralPath, [string] $NewName)
    if ($LiteralPath -like '*.current.new.*') { throw 'simulated staged rename failure' }
    Microsoft.PowerShell.Management\Rename-Item -LiteralPath $LiteralPath -NewName $NewName
}
try { Switch-Current 'release-B'; exit 21 } catch { }
if ((Get-CurrentReleaseId) -ne 'release-A') { exit 22 }
"""
        else:
            action = """
Switch-Current 'release-B'
if ((Get-CurrentReleaseId) -ne 'release-B') { exit 23 }
if (Get-ChildItem -LiteralPath $InstallRoot -Filter '.current.old.*') { exit 24 }
"""
        assertions = r"""
if (-not (Test-Path -LiteralPath (Join-Path $a 'nested\a.txt'))) { exit 25 }
if (-not (Test-Path -LiteralPath (Join-Path $b 'nested\b.txt'))) { exit 26 }
"""
        result = subprocess.run(
            [
                powershell,
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                setup + action + assertions,
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr


def ps1_function_body(name: str) -> str:
    """Extract one top-level function definition from the deploy script."""
    text = DEPLOY_PS1.read_text(encoding="utf-8")
    start = text.index(f"function {name}")
    following = text.find("\nfunction ", start + 1)
    return text[start:] if following == -1 else text[start:following]


class TestPowerShellStaticConstraints:
    """无 Windows CI 时的静态约束: install guard / 升级顺序 / junction 切换可恢复。"""

    def test_install_guard_blocks_existing_deployment(self) -> None:
        body = ps1_function_body("Do-Install")
        assert "Get-CurrentReleaseId" in body
        assert "INSTALL_BLOCKED_EXISTING_DEPLOYMENT" in body
        assert "Use upgrade instead of install." in body
        assert "already installed and current; nothing to do" in body
        # guard 必须发生在 external Python 检查 / release 创建之前
        assert body.index("Get-CurrentReleaseId") < body.index("Test-ExternalPython $Manifest")

    def test_upgrade_checks_path_python_before_sqlite_backup(self) -> None:
        body = ps1_function_body("Do-Upgrade")
        assert body.index("Test-ExternalPython $Manifest") < body.index("Backup-Sqlite")

    def test_external_python_preflight_is_strict_and_creates_real_venv(self) -> None:
        body = ps1_function_body("Test-ExternalPython")
        assert "Get-Command python" in body
        assert "--version" in body
        assert "-ne $Manifest.python_version" in body
        assert "struct.calcsize('P') * 8" in body
        assert "Windows Python must be 64-bit" in body
        for module in ("ssl", "sqlite3", "ctypes", "zlib", "venv"):
            assert module in body
        assert "airgap-sync-python-smoke-" in body
        assert "'-m', 'venv'" in body
        assert "Scripts\\python.exe" in body

    def test_python_c_payloads_do_not_backslash_escape_quotes_in_single_quoted_ps(self) -> None:
        for path in [DEPLOY_PS1, *SERVICE_DIR.glob("*.ps1")]:
            text = path.read_text(encoding="utf-8")
            dangerous = re.compile(r"(?:-c|'-c'\s*,)\s*'[^'\r\n]*\\\"")
            assert dangerous.search(text) is None, path

    def test_windows_script_contains_no_python_installer_logic(self) -> None:
        text = DEPLOY_PS1.read_text(encoding="utf-8")
        for forbidden in (
            "PythonExe",
            "InstallAllUsers",
            "TargetDir=",
            "AssociateFiles",
            "Include_launcher",
            "Start-Process -FilePath $installer",
            "C:\\Python313",
            "D:\\Python313",
        ):
            assert forbidden not in text

    def test_rollback_does_not_check_path_python(self) -> None:
        body = ps1_function_body("Do-Rollback")
        assert "Test-ExternalPython" not in body

    def test_switch_current_stages_junction_before_retiring_old(self) -> None:
        body = ps1_function_body("Switch-Current")
        assert body.index("New-Item -ItemType Junction") < body.index(
            "Rename-Item -LiteralPath $current"
        )
        # 确认临时 junction 创建成功后才允许动旧 current
        assert "staged junction was not created" in body
        assert "current junction is missing after switch" in body

    def test_switch_current_uses_guid_unique_names(self) -> None:
        body = ps1_function_body("Switch-Current")
        assert "[Guid]::NewGuid()" in body
        assert ".current.new." in body and ".current.old." in body
        assert "yyyyMMddHHmmss" not in body  # 时间戳命名有同秒冲突风险

    def test_switch_current_restores_old_pointer_on_failure(self) -> None:
        body = ps1_function_body("Switch-Current")
        assert "Rename-Item -LiteralPath $retired -NewName 'current'" in body
        assert "could not be restored" in body

    def test_switch_current_cleanup_failure_is_warning_only(self) -> None:
        body = ps1_function_body("Switch-Current")
        assert "retired junction cleanup deferred" in body
        # retired 清理在 try/catch 中, 不能让已成功的切换判回失败
        cleanup_at = body.index("retired junction cleanup deferred")
        remove_at = body.rindex("Remove-JunctionSafely $retired")
        assert remove_at < cleanup_at

    def test_junction_removal_is_non_recursive_and_type_checked(self) -> None:
        helper = ps1_function_body("Remove-JunctionSafely")
        assert "FileAttributes]::ReparsePoint" in helper
        assert "Directory]::Delete($item.FullName, $false)" in helper
        assert "-Recurse" not in helper
        assert "refusing junction-only removal for non-junction path" in helper

    def test_checksum_verifier_rejects_paths_outside_bundle(self) -> None:
        body = ps1_function_body("Test-BundleChecksums")
        assert "-contains '..'" in body
        assert "^[A-Za-z]:" in body


# ---------------------------------------------------------------------------
# Linux 真实功能测试: 在临时目录组装 checksum-valid 的假 bundle, 真跑部署脚本。
# runtime 是一个指向测试解释器的 shim, app wheel 是带 console script 的真 wheel,
# 因此 install/upgrade/rollback 走完整真实流程 (venv / pip / smoke / current)。
# ---------------------------------------------------------------------------


def _require_linux_x86_64_glibc() -> None:
    if sys.platform != "linux" or shutil.which("bash") is None:
        pytest.skip("requires Linux with bash")
    if python_platform.machine() != "x86_64":
        pytest.skip("deploy script requires an x86_64 host")
    glibc = subprocess.run(["getconf", "GNU_LIBC_VERSION"], capture_output=True, text=True)
    if glibc.returncode != 0 or "glibc" not in glibc.stdout:
        pytest.skip("requires glibc (deploy script platform check)")


SMOKE_APP_STUB_MODULES = (
    "airgap_sync",
    "click",
    "pydantic",
    "pymysql",
    "yaml",
    "requests",
    "zstandard",
)


def make_app_wheel(directory: Path, app_version: str) -> str:
    """A real, dependency-free wheel providing the smoke-test imports + CLI."""
    wheel_name = f"{TEST_DIST}-{app_version}-py3-none-any.whl"
    directory.mkdir(parents=True, exist_ok=True)
    payload: list[tuple[str, str]] = [
        (
            f"{TEST_DIST}/__init__.py",
            f"def main():\n    print('airgap-sync {app_version}')\n",
        ),
        *(
            (f"{module}.py", "# test stub satisfying smoke-app imports\n")
            for module in SMOKE_APP_STUB_MODULES
        ),
        (
            f"{TEST_DIST}-{app_version}.dist-info/METADATA",
            f"Metadata-Version: 2.1\nName: {TEST_DIST}\nVersion: {app_version}\n",
        ),
        (
            f"{TEST_DIST}-{app_version}.dist-info/WHEEL",
            "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        ),
        (
            f"{TEST_DIST}-{app_version}.dist-info/entry_points.txt",
            f"[console_scripts]\nairgap-sync = {TEST_DIST}:main\n",
        ),
    ]
    record_name = f"{TEST_DIST}-{app_version}.dist-info/RECORD"
    record = "".join(f"{name},,\n" for name, _ in payload) + f"{record_name},,\n"
    with zipfile.ZipFile(directory / wheel_name, "w") as archive:
        for name, data in payload:
            archive.writestr(name, data)
        archive.writestr(record_name, record)
    return wheel_name


def make_runtime_tarball(path: Path) -> None:
    """A tarball whose python/bin/python3 execs the test interpreter."""
    shim = f'#!/bin/sh\nexec "{sys.executable}" "$@"\n'
    with tarfile.open(path, "w:gz") as archive:
        info = tarfile.TarInfo("python/bin/python3")
        info.size = len(shim.encode("utf-8"))
        info.mode = 0o755
        archive.addfile(info, io.BytesIO(shim.encode("utf-8")))


def build_test_bundle(
    bundle: Path,
    *,
    release_id: str,
    git_commit: str,
    python_version: str = PYTHON_15,
    env_extra: str = "",
) -> Path:
    """Assemble a checksum-valid bundle that the real deploy script can install."""
    for name in ("runtime", "app", "wheelhouse", "config"):
        (bundle / name).mkdir(parents=True)
    runtime_artifact = (
        f"cpython-{python_version}+20260901-x86_64-unknown-linux-gnu-install_only.tar.gz"
    )
    make_runtime_tarball(bundle / "runtime" / runtime_artifact)
    app_wheel = make_app_wheel(bundle / "app", APP_VERSION)
    shutil.copy(bundle / "app" / app_wheel, bundle / "wheelhouse" / app_wheel)
    (bundle / "config" / "source.example.yaml").write_text("role: source\n")
    (bundle / "config" / "destination.example.yaml").write_text("role: destination\n")
    shutil.copy(OFFLINE_DIR / "airgap-sync-deploy.sh", bundle / "airgap-sync-deploy.sh")
    shutil.copy(OFFLINE_DIR / rm.HELPER_SCRIPT_NAME, bundle / rm.HELPER_SCRIPT_NAME)
    manifest = rm.ReleaseManifest(
        release_id=release_id,
        app_version=APP_VERSION,
        git_commit=git_commit,
        created_at="2026-09-15T00:00:00+00:00",
        python_version=python_version,
        platform_os="linux",
        platform_arch="x86_64",
        minimum_glibc="2.17",
        source_state_schema=4,
        destination_metadata_schema=3,
        runtime_policy=rm.RUNTIME_POLICY_BUNDLED,
        runtime_artifact=runtime_artifact,
        app_wheel=app_wheel,
        wheel_count=1,
        include_tests=False,
    )
    manifest.dump(bundle / rm.MANIFEST_NAME)
    rm.write_release_env(manifest, bundle / rm.RELEASE_ENV_NAME)
    if env_extra:
        env_path = bundle / rm.RELEASE_ENV_NAME
        env_path.write_text(env_path.read_text(encoding="utf-8") + env_extra, encoding="utf-8")
    rm.write_sha256sums(bundle, rm.bundle_payload_files(bundle))
    return bundle


def run_deploy(bundle: Path, *args: str, timeout: int = 600) -> subprocess.CompletedProcess:
    script = bundle / "airgap-sync-deploy.sh"
    return subprocess.run(
        ["bash", str(script), *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


@pytest.fixture()
def deploy_env(tmp_path: Path):
    """Install/config/data roots + a bundle for release A (python 3.13.15)."""
    _require_linux_x86_64_glibc()
    return {
        "install_root": tmp_path / "opt" / "airgap-sync",
        "config_root": tmp_path / "etc" / "airgap-sync",
        "data_root": tmp_path / "var" / "lib" / "airgap-sync",
        "bundle_a": build_test_bundle(
            tmp_path / "bundle-a", release_id=RELEASE_A, git_commit=COMMIT_A
        ),
    }


def deploy_args(env: dict, *args: str) -> tuple[str, ...]:
    return (
        *args,
        "--install-root",
        str(env["install_root"]),
        "--config-root",
        str(env["config_root"]),
        "--data-root",
        str(env["data_root"]),
    )


def current_release(env: dict) -> str | None:
    current = env["install_root"] / "current"
    if not current.is_symlink():
        return None
    return current.resolve().name


def write_state_db(env: dict, rows: list[int]) -> None:
    db = env["data_root"] / "state" / "meta.db"
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE IF NOT EXISTS t (x INTEGER)")
    conn.executemany("INSERT INTO t VALUES (?)", [(x,) for x in rows])
    conn.commit()
    conn.close()


class TestLinuxInstallGuard:
    def test_first_install_allowed_and_switches_current(self, deploy_env: dict) -> None:
        result = run_deploy(deploy_env["bundle_a"], *deploy_args(deploy_env, "install"))
        assert result.returncode == 0, result.stdout + result.stderr
        assert current_release(deploy_env) == RELEASE_A
        release_dir = deploy_env["install_root"] / "releases" / RELEASE_A
        assert (release_dir / "installed.json").is_file()
        assert (release_dir / "venv" / "bin" / "airgap-sync").is_file()
        assert (deploy_env["install_root"] / "runtimes" / f"python-{PYTHON_15}").is_dir()

    def test_install_same_release_is_idempotent_noop(self, deploy_env: dict) -> None:
        assert (
            run_deploy(deploy_env["bundle_a"], *deploy_args(deploy_env, "install")).returncode == 0
        )
        result = run_deploy(deploy_env["bundle_a"], *deploy_args(deploy_env, "install"))
        assert result.returncode == 0, result.stdout + result.stderr
        assert "already installed and current" in result.stdout
        assert current_release(deploy_env) == RELEASE_A

    def test_install_different_release_blocked_and_current_unchanged(
        self, deploy_env: dict, tmp_path: Path
    ) -> None:
        assert (
            run_deploy(deploy_env["bundle_a"], *deploy_args(deploy_env, "install")).returncode == 0
        )
        bundle_c = build_test_bundle(
            tmp_path / "bundle-c", release_id=RELEASE_C, git_commit=COMMIT_C
        )
        result = run_deploy(bundle_c, *deploy_args(deploy_env, "install"))
        assert result.returncode != 0
        assert "INSTALL_BLOCKED_EXISTING_DEPLOYMENT" in result.stdout + result.stderr
        assert "Use upgrade instead of install." in result.stdout + result.stderr
        # current 不变, 新 release 未安装
        assert current_release(deploy_env) == RELEASE_A
        assert not (deploy_env["install_root"] / "releases" / RELEASE_C).exists()


class TestLinuxUpgradeRuntimeOrdering:
    def test_python_patch_upgrade_installs_runtime_before_sqlite_backup(
        self, deploy_env: dict, tmp_path: Path
    ) -> None:
        assert (
            run_deploy(deploy_env["bundle_a"], *deploy_args(deploy_env, "install")).returncode == 0
        )
        write_state_db(deploy_env, [42])
        new_runtime = deploy_env["install_root"] / "runtimes" / f"python-{PYTHON_16}"
        assert not new_runtime.exists()  # 3.13.16 尚未安装

        bundle_c = build_test_bundle(
            tmp_path / "bundle-c",
            release_id=RELEASE_C,
            git_commit=COMMIT_C,
            python_version=PYTHON_16,
        )
        result = run_deploy(
            bundle_c, *deploy_args(deploy_env, "upgrade", "--assume-worker-stopped")
        )
        assert result.returncode == 0, result.stdout + result.stderr

        # 顺序: 目标 runtime 先装 → SQLite backup → 新 release → 切 current
        order = [
            result.stdout.index("Install target Python runtime"),
            result.stdout.index("Backup Source SQLite state"),
            result.stdout.index("Install new release"),
            result.stdout.index("Switch current"),
        ]
        assert order == sorted(order), result.stdout

        assert (new_runtime / ".runtime-installed").is_file()
        backups = sorted((deploy_env["data_root"] / "backups").glob("*/meta.db"))
        assert backups, "SQLite backup missing"
        conn = sqlite3.connect(backups[-1])
        assert [row[0] for row in conn.execute("SELECT x FROM t")] == [42]
        conn.close()
        assert current_release(deploy_env) == RELEASE_C

    def test_sqlite_backup_failure_leaves_current_unchanged(
        self, deploy_env: dict, tmp_path: Path
    ) -> None:
        assert (
            run_deploy(deploy_env["bundle_a"], *deploy_args(deploy_env, "install")).returncode == 0
        )
        corrupt = deploy_env["data_root"] / "state" / "meta.db"
        corrupt.parent.mkdir(parents=True, exist_ok=True)
        corrupt.write_bytes(b"definitely not a sqlite database")
        bundle_c = build_test_bundle(
            tmp_path / "bundle-c",
            release_id=RELEASE_C,
            git_commit=COMMIT_C,
            python_version=PYTHON_16,
        )
        result = run_deploy(
            bundle_c, *deploy_args(deploy_env, "upgrade", "--assume-worker-stopped")
        )
        assert result.returncode != 0
        assert "SQLite backup failed" in result.stdout + result.stderr
        assert current_release(deploy_env) == RELEASE_A
        assert not (deploy_env["install_root"] / "releases" / RELEASE_C).exists()

    def test_rollback_repoints_current(self, deploy_env: dict, tmp_path: Path) -> None:
        first = run_deploy(deploy_env["bundle_a"], *deploy_args(deploy_env, "install"))
        assert first.returncode == 0, first.stdout + first.stderr
        bundle_c = build_test_bundle(
            tmp_path / "bundle-c",
            release_id=RELEASE_C,
            git_commit=COMMIT_C,
            python_version=PYTHON_16,
        )
        upgrade = run_deploy(
            bundle_c, *deploy_args(deploy_env, "upgrade", "--assume-worker-stopped")
        )
        assert upgrade.returncode == 0, upgrade.stdout + upgrade.stderr
        rollback = run_deploy(
            bundle_c,
            *deploy_args(
                deploy_env, "rollback", "--to-release", RELEASE_A, "--assume-worker-stopped"
            ),
        )
        assert rollback.returncode == 0, rollback.stdout + rollback.stderr
        assert current_release(deploy_env) == RELEASE_A


class TestLinuxIntegrityGate:
    def test_release_env_not_sourced_when_gate_fails(
        self, deploy_env: dict, tmp_path: Path
    ) -> None:
        marker = tmp_path / "poison-marker"
        poison = f'AIRGAP_POISON="x"; touch {marker}\n'
        bundle = build_test_bundle(
            tmp_path / "bundle-poison",
            release_id=RELEASE_A,
            git_commit=COMMIT_A,
            env_extra=poison,
        )
        # 破坏一个 wheel → checksum gate 必须失败; release.env 不得被 source
        wheel = next((bundle / "wheelhouse").glob("*.whl"))
        wheel.write_bytes(b"corrupted")
        result = run_deploy(bundle, *deploy_args(deploy_env, "verify"))
        assert result.returncode != 0
        assert "bundle integrity check failed" in result.stdout + result.stderr
        assert not marker.exists()

    def test_release_env_sourced_after_gate_passes(self, deploy_env: dict, tmp_path: Path) -> None:
        marker = tmp_path / "poison-marker"
        poison = f'AIRGAP_POISON="x"; touch {marker}\n'
        bundle = build_test_bundle(
            tmp_path / "bundle-poison-ok",
            release_id=RELEASE_A,
            git_commit=COMMIT_A,
            env_extra=poison,
        )
        result = run_deploy(bundle, *deploy_args(deploy_env, "verify"))
        assert result.returncode == 0, result.stdout + result.stderr
        assert marker.exists()


class TestServiceExamples:
    def test_units_call_current_not_release_ids(self) -> None:
        for name in (
            "airgap-sync-source.service.example",
            "airgap-sync-destination.service.example",
        ):
            text = (SERVICE_DIR / name).read_text(encoding="utf-8")
            assert "/opt/airgap-sync/current/venv/bin/airgap-sync" in text
            assert "releases/" not in text  # 不写死具体 release 路径

    def test_windows_wrapper_calls_current(self) -> None:
        cmd_text = (SERVICE_DIR / "run-source-worker.cmd").read_text(encoding="utf-8")
        ps1_text = (SERVICE_DIR / "run-source-worker.ps1").read_text(encoding="utf-8")
        assert "current\\venv\\Scripts\\airgap-sync.exe" in cmd_text
        assert "current" in ps1_text and "venv\\Scripts\\airgap-sync.exe" in ps1_text
        assert "0.1.0" not in cmd_text and "0.1.0" not in ps1_text

    def test_windows_wrapper_combines_output_and_preserves_native_exit_code(self) -> None:
        text = (SERVICE_DIR / "run-source-worker.ps1").read_text(encoding="utf-8")
        assert "$ErrorActionPreference = 'Continue'" in text
        assert "2>&1 |" in text
        assert "Out-File -FilePath $LogFile -Append -Encoding utf8" in text
        assert "$workerExitCode = $LASTEXITCODE" in text
        assert "exit $workerExitCode" in text
        assert "Test-Path -LiteralPath $Config -PathType Leaf" in text
        assert "current\\venv\\Scripts\\airgap-sync.exe" in text

    @pytest.mark.parametrize("native_exit", [0, 1])
    def test_windows_wrapper_real_native_streams_and_exit_code(
        self, tmp_path: Path, native_exit: int
    ) -> None:
        if os.name != "nt":
            pytest.skip("requires Windows native stderr behavior")
        powershell = shutil.which("powershell") or shutil.which("pwsh")
        if powershell is None:
            pytest.skip("PowerShell unavailable")
        root = tmp_path / f"worker-{native_exit}"
        scripts_dir = root / "current" / "venv" / "Scripts"
        scripts_dir.mkdir(parents=True)
        wrapper = root / "run-source-worker.ps1"
        shutil.copy(SERVICE_DIR / "run-source-worker.ps1", wrapper)
        config = root / "source.yaml"
        config.write_text("role: source\n", encoding="utf-8")
        log = root / "logs" / "worker.log"
        exe = scripts_dir / "airgap-sync.exe"
        compile_script = root / "compile-fake-worker.ps1"
        compile_script.write_text(
            """
$source = @'
using System;
public class Worker {
    public static int Main(string[] args) {
        Console.Out.WriteLine("STDOUT test");
        Console.Error.WriteLine("INFO test");
        return Int32.Parse(Environment.GetEnvironmentVariable("FAKE_WORKER_EXIT"));
    }
}
'@
Add-Type -TypeDefinition $source -Language CSharp -OutputType ConsoleApplication `
    -OutputAssembly $args[0]
""",
            encoding="utf-8",
        )
        compiled = subprocess.run(
            [powershell, "-NoProfile", "-NonInteractive", "-File", compile_script, exe],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert compiled.returncode == 0, compiled.stdout + compiled.stderr
        env = os.environ.copy()
        env["FAKE_WORKER_EXIT"] = str(native_exit)
        result = subprocess.run(
            [
                powershell,
                "-NoProfile",
                "-NonInteractive",
                "-File",
                wrapper,
                "-Config",
                config,
                "-LogFile",
                log,
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=60,
        )
        assert result.returncode == native_exit, result.stdout + result.stderr
        logged = log.read_text(encoding="utf-8-sig")
        assert "STDOUT test" in logged
        assert "INFO test" in logged

    def test_windows_wrapper_missing_executable_fails(self, tmp_path: Path) -> None:
        if os.name != "nt":
            pytest.skip("requires Windows PowerShell")
        powershell = shutil.which("powershell") or shutil.which("pwsh")
        if powershell is None:
            pytest.skip("PowerShell unavailable")
        root = tmp_path / "missing-worker"
        root.mkdir()
        wrapper = root / "run-source-worker.ps1"
        shutil.copy(SERVICE_DIR / "run-source-worker.ps1", wrapper)
        result = subprocess.run(
            [powershell, "-NoProfile", "-NonInteractive", "-File", wrapper],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode != 0
        assert "airgap-sync not found" in result.stdout + result.stderr
