<# One-shot Source telemetry wrapper. Copy to the install root.
Set monitoring.log_dirs in source.yaml to the absolute parent directory of
this LogFile and the worker LogFile (one entry when they share a directory).
The default is <InstallRoot>\logs; Reporter does not infer this location.
#>
param(
    [string] $Config = 'C:\ProgramData\AirgapSync\config\source.yaml',
    [string] $LogFile = ''
)
$ErrorActionPreference = 'Stop'
$installRoot = $PSScriptRoot
if (-not $LogFile) { $LogFile = Join-Path $installRoot 'logs\source-monitor.log' }
$cli = Join-Path $installRoot 'current\venv\Scripts\airgap-sync.exe'
if (-not (Test-Path -LiteralPath $cli -PathType Leaf)) { throw 'airgap-sync executable missing' }
if (-not (Test-Path -LiteralPath $Config -PathType Leaf)) { throw 'Source config missing' }
New-Item -ItemType Directory -Path (Split-Path -Parent $LogFile) -Force | Out-Null
Push-Location $installRoot
try {
    $ErrorActionPreference = 'Continue'
    & $cli source monitor-report --config $Config 2>&1 |
        Out-File -FilePath $LogFile -Append -Encoding utf8 -ErrorAction Stop
    $result = $LASTEXITCODE
} finally {
    Pop-Location
}
exit $result
