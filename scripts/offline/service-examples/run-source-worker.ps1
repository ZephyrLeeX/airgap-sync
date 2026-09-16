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
    [string] $Config = '',
    [string] $LogFile = ''
)

$ErrorActionPreference = 'Stop'

$installRoot = $PSScriptRoot
if (-not $Config) {
    $Config = Join-Path $installRoot '..\config\source.yaml'
}
if (-not $LogFile) {
    $LogFile = Join-Path $installRoot 'logs\source-worker.log'
}

$cli = Join-Path $installRoot 'current\venv\Scripts\airgap-sync.exe'
if (-not (Test-Path -LiteralPath $cli)) {
    Write-Error "airgap-sync not found under the current release: $cli"
    exit 1
}
if (-not (Test-Path -LiteralPath $Config -PathType Leaf)) {
    Write-Error "Source config not found: $Config"
    exit 1
}

$logDirectory = Split-Path -Parent $LogFile
if ($logDirectory) {
    New-Item -ItemType Directory -Path $logDirectory -Force -ErrorAction Stop | Out-Null
}

$oldPreference = $ErrorActionPreference
try {
    # Windows PowerShell 5.1 wraps native stderr as NativeCommandError. It is
    # worker output, not a PowerShell failure; preserve it in the combined log.
    $ErrorActionPreference = 'Continue'
    & $cli source worker --config $Config 2>&1 |
        Out-File -FilePath $LogFile -Append -Encoding utf8 -ErrorAction Stop
    $workerExitCode = $LASTEXITCODE
} finally {
    $ErrorActionPreference = $oldPreference
}

exit $workerExitCode
