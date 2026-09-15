"""部署脚本静态检查: bash 语法、(可选) PowerShell 语法、service 示例约束。"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

OFFLINE_DIR = Path(__file__).resolve().parent.parent / "scripts" / "offline"
DEPLOY_SH = OFFLINE_DIR / "airgap-sync-deploy.sh"
DEPLOY_PS1 = OFFLINE_DIR / "airgap-sync-deploy.ps1"
SERVICE_DIR = OFFLINE_DIR / "service-examples"


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
