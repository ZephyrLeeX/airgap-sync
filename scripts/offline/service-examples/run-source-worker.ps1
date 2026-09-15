<#
.SYNOPSIS
Example wrapper for running the Airgap Sync Source worker on Windows.

.DESCRIPTION
Copy this file into the install root (the folder that contains "current"),
e.g. C:\Program Files\AirgapSync\run-source-worker.ps1

It always invokes the CURRENT release, so upgrades only switch the "current"
junction; this wrapper never needs editing. Secrets stay in environment
variables named by the YAML config.
#>
param(
    [string] $Config = ''
)

$ErrorActionPreference = 'Stop'

$installRoot = $PSScriptRoot
if (-not $Config) {
    $Config = Join-Path $installRoot '..\config\source.yaml'
}

$cli = Join-Path $installRoot 'current\venv\Scripts\airgap-sync.exe'
if (-not (Test-Path -LiteralPath $cli)) {
    Write-Error "airgap-sync not found under the current release: $cli"
    exit 1
}

& $cli source worker --config $Config
exit $LASTEXITCODE
