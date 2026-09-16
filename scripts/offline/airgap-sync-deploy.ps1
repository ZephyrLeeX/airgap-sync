<#
.SYNOPSIS
Airgap Sync offline deploy script for Windows Server 2019 x64.

.DESCRIPTION
Works with a self-contained release bundle produced by build_release.py.
Compatible with Windows PowerShell 5.1 (no PowerShell 7-only features).
Run from an elevated prompt when installing into the default locations.

Actions:
  Verify           verify bundle integrity, platform and architecture
  Install          FIRST INSTALL ONLY; refuses when a different release is
                   already current (use Upgrade for that)
  Upgrade          install a new release side-by-side and switch current
  Rollback         re-point current to an earlier installed release
  Status           show install root, current release and installed releases
  VerifyInstalled  smoke-check the current release (no database access)
  InitConfig       copy an example config (never overwrites)

Examples:
  .\airgap-sync-deploy.ps1 -Action Verify
  .\airgap-sync-deploy.ps1 -Action Install
  .\airgap-sync-deploy.ps1 -Action Upgrade -AssumeWorkerStopped
  .\airgap-sync-deploy.ps1 -Action Rollback -ToRelease 0.1.0-cc8e17a -AssumeWorkerStopped
  .\airgap-sync-deploy.ps1 -Action InitConfig -Role source
#>
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('Verify', 'Install', 'Upgrade', 'Rollback', 'Status', 'VerifyInstalled', 'InitConfig')]
    [string] $Action,

    [string] $InstallRoot = 'C:\Program Files\AirgapSync',
    [string] $ConfigRoot = 'C:\ProgramData\AirgapSync\config',
    [string] $DataRoot = '',
    [string] $ToRelease = '',
    [string] $ServiceName = '',
    [string] $ScheduledTaskName = '',
    [ValidateSet('', 'source', 'destination')]
    [string] $Role = '',
    [switch] $AssumeWorkerStopped
)

$ErrorActionPreference = 'Stop'
$script:BundleRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$script:StoppedService = $null
$script:StoppedTask = $null
$script:PathPython = $null

function Write-Stage([string] $Message) {
    Write-Host "== $Message" -ForegroundColor Cyan
}

function Fail([string] $Message) {
    # throw (not exit) so upgrade/rollback catch blocks can restart a stopped
    # worker before the process terminates; the top-level handler prints it.
    throw [InvalidOperationException]::new($Message)
}

function Invoke-Checked([string] $FilePath, [string[]] $Arguments, [string] $Label) {
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        Fail "$Label failed with exit code $LASTEXITCODE"
    }
}

function Read-Manifest {
    $path = Join-Path $script:BundleRoot 'release.json'
    if (-not (Test-Path -LiteralPath $path)) {
        Fail "release.json not found next to this script: $path"
    }
    try {
        return (Get-Content -LiteralPath $path -Raw | ConvertFrom-Json)
    } catch {
        Fail "release.json is not valid JSON: $_"
    }
}

function Test-Platform([object] $Manifest) {
    if ($Manifest.platform.os -ne 'windows') {
        Fail ("this bundle targets os={0}; it cannot be installed on windows" -f $Manifest.platform.os)
    }
    if ($Manifest.platform.arch -ne 'amd64') {
        Fail ("this bundle targets arch={0}, not amd64" -f $Manifest.platform.arch)
    }
    if ($env:PROCESSOR_ARCHITECTURE -ne 'AMD64') {
        Fail "this machine reports $env:PROCESSOR_ARCHITECTURE; an amd64 bundle requires AMD64"
    }
    if ($Manifest.runtime_policy -ne 'external-python-path') {
        Fail "Windows bundle runtime_policy must be external-python-path"
    }
}

function Test-BundleLayout([object] $Manifest) {
    $paths = @(
        (Join-Path $script:BundleRoot 'SHA256SUMS'),
        (Join-Path $script:BundleRoot 'release_manifest.py'),
        (Join-Path $script:BundleRoot 'airgap-sync-deploy.ps1'),
        (Join-Path $script:BundleRoot ('app\' + $Manifest.app_wheel)),
        (Join-Path $script:BundleRoot 'config\source.example.yaml'),
        (Join-Path $script:BundleRoot 'config\destination.example.yaml')
    )
    foreach ($path in $paths) {
        if (-not (Test-Path -LiteralPath $path)) {
            Fail "bundle file missing: $path"
        }
    }
    $wheelhouse = Join-Path $script:BundleRoot 'wheelhouse'
    if (-not (Get-ChildItem -LiteralPath $wheelhouse -Filter '*.whl' -ErrorAction SilentlyContinue)) {
        Fail "wheelhouse contains no wheels: $wheelhouse"
    }
}

function Test-BundleChecksums {
    $sumsPath = Join-Path $script:BundleRoot 'SHA256SUMS'
    $checked = 0
    foreach ($line in (Get-Content -LiteralPath $sumsPath)) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith('#')) { continue }
        $parts = $trimmed -split '\s+', 2
        if ($parts.Count -ne 2) { Fail "malformed SHA256SUMS line: $trimmed" }
        $expected = $parts[0].ToLower()
        $relative = $parts[1].Trim().TrimStart('*') -replace '/', '\'
        # Entries must stay inside the bundle root: no absolute paths, no
        # drive letters, no ".." segments.
        if ($relative -eq '' -or $relative.StartsWith('\') -or $relative.StartsWith('/') -or
            ($relative -match '^[A-Za-z]:') -or ($relative.Split('\') -contains '..')) {
            Fail "SHA256SUMS entry is not bundle-relative: $relative"
        }
        $target = Join-Path $script:BundleRoot $relative
        if (-not (Test-Path -LiteralPath $target)) {
            Fail "missing file listed in SHA256SUMS: $relative"
        }
        $actual = (Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash.ToLower()
        if ($actual -ne $expected) {
            Fail ("checksum mismatch for {0}: expected {1}, got {2}" -f $relative, $expected, $actual)
        }
        $checked += 1
    }
    if ($checked -eq 0) { Fail "SHA256SUMS lists no files" }
    Write-Host "  $checked files checked"
}

function Invoke-VerifyBundle([object] $Manifest) {
    Write-Stage 'Verify release metadata'
    Test-BundleLayout $Manifest
    Write-Host ("  release={0} app={1} python={2}" -f $Manifest.release_id, $Manifest.app_version, $Manifest.python_version)
    Test-Platform $Manifest
    Write-Host "  arch=amd64 (Windows)"
    Write-Stage 'Verify SHA256SUMS'
    Test-BundleChecksums
    Write-Host 'Bundle verification OK'
}

function Test-ExternalPython([object] $Manifest) {
    Write-Stage ("Check Python {0} x64 from PATH" -f $Manifest.python_version)
    $command = Get-Command python -CommandType Application -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($null -eq $command -or [System.IO.Path]::GetFileName($command.Source).ToLowerInvariant() -ne 'python.exe') {
        Fail ("Python {0} x64 is required on PATH.`nInstall Python first and ensure 'python' is available in this shell." -f $Manifest.python_version)
    }
    $python = $command.Source
    $versionOutput = (& $python --version 2>&1) -join ' '
    if ($LASTEXITCODE -ne 0) {
        Fail "Python on PATH could not report its version: $python"
    }
    $foundVersion = $versionOutput -replace '^Python\s+', ''
    if ($foundVersion -ne $Manifest.python_version) {
        Fail ("Python version mismatch.`nRequired: {0}`nFound:    {1}`nPath:     {2}" -f $Manifest.python_version, $foundVersion, $python)
    }

    $architecture = (& $python -c "import struct; print(struct.calcsize('P') * 8)" 2>&1) -join ' '
    if ($LASTEXITCODE -ne 0 -or $architecture.Trim() -ne '64') {
        Fail 'Windows Python must be 64-bit.'
    }
    $smoke = "import ctypes, ssl, sqlite3, struct, sys, venv, zlib; " +
        "print('Python version: ' + sys.version.replace('\n', ' ')); " +
        "print('Python executable: ' + sys.executable); " +
        "print('Python architecture: ' + str(struct.calcsize('P') * 8) + '-bit'); " +
        "print('OpenSSL version: ' + ssl.OPENSSL_VERSION); " +
        "print('SQLite version: ' + sqlite3.sqlite_version)"
    Invoke-Checked $python @('-c', $smoke) 'Python runtime smoke test'

    $smokeVenv = Join-Path ([System.IO.Path]::GetTempPath()) `
        ('airgap-sync-python-smoke-' + [Guid]::NewGuid().ToString('N'))
    try {
        Invoke-Checked $python @('-m', 'venv', $smokeVenv) 'temporary venv creation'
        $smokePython = Join-Path $smokeVenv 'Scripts\python.exe'
        Invoke-Checked $smokePython @('-c', 'import ssl,sqlite3,ctypes,zlib') `
            'temporary venv runtime smoke test'
    } finally {
        if (Test-Path -LiteralPath $smokeVenv) {
            Remove-Item -LiteralPath $smokeVenv -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
    Write-Host "  Python requirement satisfied: $($Manifest.python_version) x64 via PATH"
    Write-Host "  Python executable: $python"
    $script:PathPython = $python
}

function Get-ReleasesDir {
    return (Join-Path $InstallRoot 'releases')
}

function Get-ReleaseState([string] $ReleaseDir) {
    if (-not (Test-Path -LiteralPath $ReleaseDir)) { return 'missing' }
    if (Test-Path -LiteralPath (Join-Path $ReleaseDir 'installed.json')) { return 'complete' }
    return 'incomplete'
}

function Get-CurrentReleaseId {
    $current = Join-Path $InstallRoot 'current'
    if (-not (Test-Path -LiteralPath $current)) { return $null }
    $item = Get-Item -LiteralPath $current
    if ($null -eq $item.Target) { return $null }
    return (Split-Path -Leaf ($item.Target | Select-Object -First 1))
}

function Remove-JunctionSafely([string] $Path) {
    if (-not (Test-Path -LiteralPath $Path)) { return }
    $item = Get-Item -LiteralPath $Path -Force
    if (-not $item.PSIsContainer -or
        -not ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint)) {
        Fail "refusing junction-only removal for non-junction path: $Path"
    }
    # Directory.Delete(path, false) removes the directory reparse point itself.
    # It neither enumerates nor recursively deletes anything in its target.
    [System.IO.Directory]::Delete($item.FullName, $false)
}

function Switch-Current([string] $ReleaseId) {
    # Failure-recoverable current switch:
    #   1. create a staged junction under a GUID name and confirm it resolves
    #   2. retire the old current (rename, also under a GUID name)
    #   3. rename staged -> current and confirm current exists
    #   4. delete the retired junction (cleanup only; a failure here is a
    #      WARNING, not a failed switch)
    # If step 3 fails after step 2 retired the old current, the old junction
    # is renamed back so `current` always points at a real release. GUID
    # names avoid same-second collisions between consecutive switches.
    $target = Join-Path (Get-ReleasesDir) $ReleaseId
    if (-not (Test-Path -LiteralPath $target)) {
        Fail "cannot switch current: $target does not exist"
    }
    $current = Join-Path $InstallRoot 'current'
    $unique = [Guid]::NewGuid().ToString('N')
    $staged = Join-Path $InstallRoot ('.current.new.' + $unique)
    $retired = $null

    New-Item -ItemType Junction -Path $staged -Value $target | Out-Null
    if (-not (Test-Path -LiteralPath $staged)) {
        Fail "staged junction was not created: $staged"
    }
    try {
        if (Test-Path -LiteralPath $current) {
            $retired = Join-Path $InstallRoot ('.current.old.' + $unique)
            Rename-Item -LiteralPath $current -NewName (Split-Path -Leaf $retired)
        }
        Rename-Item -LiteralPath $staged -NewName 'current'
        if (-not (Test-Path -LiteralPath $current)) {
            Fail "current junction is missing after switch: $current"
        }
    } catch {
        $restored = $false
        if ($null -ne $retired) {
            if (Test-Path -LiteralPath $current) {
                $restored = $true
            } else {
                try {
                    Rename-Item -LiteralPath $retired -NewName 'current'
                    $restored = $true
                } catch {
                    $message = "CRITICAL: current switch failed and the previous current " +
                        "could not be restored; repoint current to $retired manually"
                    Write-Warning $message
                }
            }
        }
        if (Test-Path -LiteralPath $staged) {
            try { Remove-JunctionSafely $staged } catch { }
        }
        if ($null -ne $retired -and -not $restored) {
            Fail ("current switch failed and the previous current junction was left at {0}" -f $retired)
        }
        throw
    }
    if ($null -ne $retired) {
        try {
            Remove-JunctionSafely $retired
        } catch {
            # The retired junction is only a leftover link; the active current
            # already points at the new release. Do not fail a successful
            # switch over cleanup.
            Write-Warning "retired junction cleanup deferred: $retired"
        }
    }
}

function Ensure-ReleaseInstalled([object] $Manifest, [string] $PathPython) {
    $releaseDir = Join-Path (Get-ReleasesDir) $Manifest.release_id
    $venvDir = Join-Path $releaseDir 'venv'
    $venvPython = Join-Path $venvDir 'Scripts\python.exe'
    $appCli = Join-Path $venvDir 'Scripts\airgap-sync.exe'
    $state = Get-ReleaseState $releaseDir
    if ($state -eq 'complete') {
        Write-Host "  release $($Manifest.release_id) already installed, reusing venv"
        return
    }
    if ($state -eq 'incomplete') {
        Write-Host "  incomplete install detected, rebuilding $releaseDir"
        Remove-Item -LiteralPath $releaseDir -Recurse -Force
    }
    New-Item -ItemType Directory -Path $releaseDir -Force | Out-Null

    Write-Stage "Create venv for $($Manifest.release_id)"
    Invoke-Checked $PathPython @('-m', 'venv', $venvDir) 'venv creation'

    Write-Stage 'Offline pip install (app wheel + locked dependencies)'
    $oldNoIndex = $env:PIP_NO_INDEX
    $oldVersionCheck = $env:PIP_DISABLE_PIP_VERSION_CHECK
    $env:PIP_NO_INDEX = '1'
    $env:PIP_DISABLE_PIP_VERSION_CHECK = '1'
    try {
        $wheelhouse = Join-Path $script:BundleRoot 'wheelhouse'
        $appWheel = Join-Path $script:BundleRoot ('app\' + $Manifest.app_wheel)
        Invoke-Checked $venvPython @('-m', 'pip', 'install', '--no-index', '--find-links', $wheelhouse, $appWheel) 'offline pip install'
    } finally {
        $env:PIP_NO_INDEX = $oldNoIndex
        $env:PIP_DISABLE_PIP_VERSION_CHECK = $oldVersionCheck
    }

    Write-Stage 'Smoke test application imports'
    Invoke-Checked $venvPython @((Join-Path $script:BundleRoot 'release_manifest.py'), 'smoke-app') 'application smoke test'
    $versionOutput = (& $appCli --version) -join ' '
    if ($LASTEXITCODE -ne 0) { Fail 'airgap-sync --version failed' }
    if ($versionOutput -notlike ('*' + $Manifest.app_version + '*')) {
        Fail "unexpected --version output: $versionOutput"
    }
    Write-Host "  $versionOutput"

    Write-Stage 'Write release metadata'
    $installed = [ordered]@{
        format_version               = $Manifest.format_version
        release_id                   = $Manifest.release_id
        app_version                  = $Manifest.app_version
        git_commit                   = $Manifest.git_commit
        created_at                   = $Manifest.created_at
        python_version               = $Manifest.python_version
        runtime_policy              = $Manifest.runtime_policy
        platform                     = $Manifest.platform
        source_state_schema          = $Manifest.source_state_schema
        destination_metadata_schema  = $Manifest.destination_metadata_schema
        app_wheel                    = $Manifest.app_wheel
        wheel_count                  = $Manifest.wheel_count
        include_tests                = [bool] $Manifest.include_tests
        installed_at                 = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
    }
    $markerPath = Join-Path $releaseDir 'installed.json'
    [System.IO.File]::WriteAllText($markerPath, (($installed | ConvertTo-Json -Depth 5) + "`n"))
    Copy-Item -LiteralPath (Join-Path $script:BundleRoot 'release_manifest.py') `
        -Destination (Join-Path $releaseDir 'release_manifest.py')
}

function Stop-Worker {
    if ($ServiceName -ne '') {
        Write-Stage "Stop worker service $ServiceName"
        Stop-Service -Name $ServiceName -ErrorAction Stop
        $script:StoppedService = $ServiceName
        return
    }
    if ($ScheduledTaskName -ne '') {
        Write-Stage "Stop scheduled task $ScheduledTaskName"
        Stop-ScheduledTask -TaskName $ScheduledTaskName -ErrorAction Stop
        $script:StoppedTask = $ScheduledTaskName
        return
    }
    if (-not $AssumeWorkerStopped) {
        Fail 'refusing to continue: the worker may still be running. Pass -ServiceName/-ScheduledTaskName or -AssumeWorkerStopped.'
    }
    Write-Host '  assuming worker already stopped (-AssumeWorkerStopped)'
}

function Start-Worker {
    if ($null -ne $script:StoppedService) {
        Write-Stage "Start worker service $($script:StoppedService)"
        Start-Service -Name $script:StoppedService -ErrorAction Stop
    }
    if ($null -ne $script:StoppedTask) {
        Write-Stage "Start scheduled task $($script:StoppedTask)"
        Start-ScheduledTask -TaskName $script:StoppedTask -ErrorAction Stop
    }
}

function Backup-Configs {
    if (-not (Test-Path -LiteralPath $ConfigRoot)) { return }
    $yamlFiles = Get-ChildItem -LiteralPath $ConfigRoot -Filter '*.yaml' -ErrorAction SilentlyContinue
    if (-not $yamlFiles) { return }
    $stamp = Get-Date -Format 'yyyyMMddHHmmss'
    $destination = Join-Path $ConfigRoot "backups\$stamp"
    New-Item -ItemType Directory -Path $destination -Force | Out-Null
    foreach ($file in $yamlFiles) {
        Copy-Item -LiteralPath $file.FullName -Destination $destination
    }
    Write-Host "  config backup: $destination"
}

function Backup-Sqlite([string] $PathPython) {
    if ($DataRoot -eq '') {
        Write-Host '  no -DataRoot given, skipping SQLite backup'
        return
    }
    $database = Join-Path $DataRoot 'state\meta.db'
    if (-not (Test-Path -LiteralPath $database)) {
        Write-Host "  no SQLite state at $database, skipping backup"
        return
    }
    $stamp = Get-Date -Format 'yyyyMMddHHmmss'
    $destination = Join-Path $DataRoot "backups\$stamp"
    New-Item -ItemType Directory -Path $destination -Force | Out-Null
    $scriptBlock = 'import sqlite3, sys; ' +
        'source = sqlite3.connect(sys.argv[1]); target = sqlite3.connect(sys.argv[2]); ' +
        'source.backup(target); source.close(); target.close()'
    Invoke-Checked $PathPython @('-c', $scriptBlock, $database, (Join-Path $destination 'meta.db')) 'SQLite backup'
    Write-Host "  SQLite backup: $destination\meta.db"
}

function Test-SchemaCompatibility([object] $Current, [object] $Target, [string] $Label) {
    $pairs = @(
        @('source_state_schema', 'source state schema'),
        @('destination_metadata_schema', 'destination metadata schema')
    )
    foreach ($pair in $pairs) {
        $key = $pair[0]
        $currentVersion = [int] $Current.$key
        $targetVersion = [int] $Target.$key
        if ($targetVersion -lt $currentVersion) {
            Fail ("{0}: {1} downgrade {2} -> {3} is not allowed; the newer release may already have migrated the metadata" -f $Label, $pair[1], $currentVersion, $targetVersion)
        }
    }
}

function Write-NextSteps {
    Write-Host ''
    Write-Host 'Next steps (worker NOT started automatically):'
    Write-Host '  1. .\airgap-sync-deploy.ps1 -Action InitConfig -Role source   # or destination'
    Write-Host "  2. Edit $ConfigRoot\<role>.yaml (secrets stay in env vars)"
    Write-Host '  3. Set the password/token environment variables'
    Write-Host '  4. airgap-sync config validate --config ...'
    Write-Host '  5. airgap-sync source check | source relay-check | source sync --table SMALL_TABLE'
    Write-Host '     airgap-sync destination check | destination process --run RUN_ID'
}

function Do-Install([object] $Manifest) {
    Write-Stage '[1/8] Verify release bundle'
    Invoke-VerifyBundle $Manifest
    Write-Stage '[2/8] Check platform'
    Test-Platform $Manifest
    Write-Stage '[3/8] Check for existing deployment'
    $current = Get-CurrentReleaseId
    if ($null -ne $current) {
        if ($current -eq $Manifest.release_id -and
            (Get-ReleaseState (Join-Path (Get-ReleasesDir) $Manifest.release_id)) -eq 'complete') {
            Write-Host "  release $($Manifest.release_id) is already installed and current; nothing to do"
            Write-Host "Install complete (no-op): current -> $($Manifest.release_id)"
            return
        }
        # Install is for first installs only: switching to a different
        # release here would silently bypass worker stop, config/SQLite
        # backups, the schema guard and upgrade failure recovery.
        Write-Host 'Existing deployment detected.'
        Write-Host 'Use upgrade instead of install.'
        Fail ("INSTALL_BLOCKED_EXISTING_DEPLOYMENT: current -> {0}, this bundle installs {1}" -f $current, $Manifest.release_id)
    }
    Write-Host '  no existing deployment; proceeding with first install'
    Write-Stage '[4/8] Validate external Python and temporary venv'
    Test-ExternalPython $Manifest
    Write-Stage ('[5/8] Create release directory {0}' -f (Join-Path (Get-ReleasesDir) $Manifest.release_id))
    Ensure-ReleaseInstalled $Manifest $script:PathPython
    Write-Stage '[6/8] Application smoke completed'
    Write-Host '  release venv and application are ready'
    Write-Stage '[7/8] Switch current'
    Switch-Current $Manifest.release_id
    Write-Stage '[8/8] Done'
    Write-Host "Install complete: current -> $($Manifest.release_id)"
    Write-NextSteps
}

function Do-Upgrade([object] $Manifest) {
    Write-Stage '[1/9] Verify release bundle'
    Invoke-VerifyBundle $Manifest
    Write-Stage '[2/9] Check platform'
    Test-Platform $Manifest
    Write-Stage '[3/9] Validate external Python and temporary venv'
    Test-ExternalPython $Manifest
    Write-Stage '[4/9] Stop worker'
    Stop-Worker
    try {
        $current = Get-CurrentReleaseId
        Write-Host ("  current release: {0}" -f ($(if ($null -ne $current) { $current } else { 'none' })))
        if ($current -eq $Manifest.release_id) {
            Write-Host "  $($Manifest.release_id) is already current; nothing to do"
            Start-Worker
            return
        }
        Write-Stage '[5/9] Backup config'
        Backup-Configs
        Write-Stage '[6/9] Backup Source SQLite state (when present)'
        Backup-Sqlite $script:PathPython
        Write-Stage '[7/9] Install new release from external PATH Python'
        Ensure-ReleaseInstalled $Manifest $script:PathPython
        Write-Stage '[8/9] Schema compatibility check'
        if ($null -ne $current) {
            $currentMarker = Join-Path (Get-ReleasesDir) ($current + '\installed.json')
            if (Test-Path -LiteralPath $currentMarker) {
                $currentData = Get-Content -LiteralPath $currentMarker -Raw | ConvertFrom-Json
                Test-SchemaCompatibility $currentData $Manifest 'upgrade blocked'
            }
        }
        Write-Stage '[9/9] Switch current'
        Switch-Current $Manifest.release_id
    } catch {
        Write-Host "ERROR: upgrade failed; any partial current switch was rolled back so the current release is unchanged: $_" -ForegroundColor Red
        if ($null -ne $script:StoppedService) {
            Write-Host "NOTE: attempting to restart $($script:StoppedService)"
            try { Start-Service -Name $script:StoppedService -ErrorAction Stop } catch { }
        }
        if ($null -ne $script:StoppedTask) {
            Write-Host "NOTE: attempting to restart task $($script:StoppedTask)"
            try { Start-ScheduledTask -TaskName $script:StoppedTask -ErrorAction Stop } catch { }
        }
        exit 1
    }
    Start-Worker
    Write-Host ("Upgrade complete: current -> {0} (previous: {1})" -f $Manifest.release_id, ($(if ($null -ne $current) { $current } else { 'none' })))
}

function Do-Rollback([object] $Manifest) {
    if ($ToRelease -eq '') {
        Fail 'rollback requires -ToRelease <release-id>'
    }
    $releasesDir = Get-ReleasesDir
    $targetDir = Join-Path $releasesDir $ToRelease
    Write-Stage '[1/5] Stop worker'
    Stop-Worker
    try {
        Write-Stage '[2/5] Check target release'
        if (-not (Test-Path -LiteralPath $targetDir)) {
            Fail "release $ToRelease is not installed under $releasesDir"
        }
        if ((Get-ReleaseState $targetDir) -ne 'complete') {
            Fail "release $ToRelease has no completion marker (incomplete install)"
        }
        $current = Get-CurrentReleaseId
        Write-Host ("  rollback: {0} -> {1}" -f ($(if ($null -ne $current) { $current } else { 'none' })), $ToRelease)
        Write-Stage '[3/5] Schema compatibility check'
        if ($null -ne $current) {
            $currentMarker = Join-Path $releasesDir ($current + '\installed.json')
            $targetMarker = Join-Path $targetDir 'installed.json'
            if (Test-Path -LiteralPath $currentMarker) {
                $currentData = Get-Content -LiteralPath $currentMarker -Raw | ConvertFrom-Json
                $targetData = Get-Content -LiteralPath $targetMarker -Raw | ConvertFrom-Json
                Test-SchemaCompatibility $currentData $targetData 'ROLLBACK BLOCKED'
            }
        }
        Write-Stage '[4/5] Switch current (new release directory is kept)'
        Switch-Current $ToRelease
    } catch {
        Write-Host "ERROR: rollback failed; any partial current switch was rolled back so the current release is unchanged: $_" -ForegroundColor Red
        if ($null -ne $script:StoppedService) {
            try { Start-Service -Name $script:StoppedService -ErrorAction Stop } catch { }
        }
        if ($null -ne $script:StoppedTask) {
            try { Start-ScheduledTask -TaskName $script:StoppedTask -ErrorAction Stop } catch { }
        }
        exit 1
    }
    Write-Stage '[5/5] Start worker'
    Start-Worker
    Write-Host "Rollback complete: current -> $ToRelease"
}

function Do-Status {
    Write-Host "Install root      $InstallRoot"
    $current = Get-CurrentReleaseId
    Write-Host ("Current release   {0}" -f ($(if ($null -ne $current) { $current } else { '(none)' })))
    if ($null -ne $current) {
        $marker = Join-Path (Get-ReleasesDir) ($current + '\installed.json')
        if (Test-Path -LiteralPath $marker) {
            $data = Get-Content -LiteralPath $marker -Raw | ConvertFrom-Json
            Write-Host ("App version       {0}" -f $data.app_version)
            Write-Host ("Git commit        {0}" -f $data.git_commit)
            Write-Host ("Required Python      {0}" -f $data.python_version)
            $venvPython = Join-Path (Get-ReleasesDir) ($current + '\venv\Scripts\python.exe')
            if (Test-Path -LiteralPath $venvPython) {
                $venvVersion = (& $venvPython -c 'import platform; print(platform.python_version())' 2>&1) -join ' '
                if ($LASTEXITCODE -eq 0) {
                    Write-Host ("Current venv Python  {0}" -f $venvVersion.Trim())
                    Write-Host ("Venv interpreter     {0}" -f $venvPython)
                } else {
                    Write-Host 'Current venv Python  (unable to query)'
                }
            } else {
                Write-Host 'Current venv Python  (missing)'
            }
            Write-Host ("Source schema     {0}" -f $data.source_state_schema)
            Write-Host ("Destination schema {0}" -f $data.destination_metadata_schema)
        }
    }
    Write-Host 'Installed releases:'
    $releasesDir = Get-ReleasesDir
    if (Test-Path -LiteralPath $releasesDir) {
        $found = $false
        foreach ($entry in (Get-ChildItem -LiteralPath $releasesDir -Directory)) {
            $found = $true
            $state = Get-ReleaseState $entry.FullName
            $marker = if ($state -eq 'complete') { 'complete' } else { 'INCOMPLETE' }
            $arrow = if ($entry.Name -eq $current) { '  <- current' } else { '' }
            Write-Host ("  {0}  {1}{2}" -f $entry.Name, $marker, $arrow)
        }
        if (-not $found) { Write-Host '  (none)' }
    } else {
        Write-Host '  (none)'
    }
}

function Do-VerifyInstalled {
    Write-Stage 'Verify installed release'
    $current = Get-CurrentReleaseId
    if ($null -eq $current) { Fail 'current pointer is missing; run install first' }
    $releaseDir = Join-Path (Get-ReleasesDir) $current
    if (-not (Test-Path -LiteralPath (Join-Path $releaseDir 'installed.json'))) {
        Fail 'current release has no completion marker'
    }
    $venvPython = Join-Path $releaseDir 'venv\Scripts\python.exe'
    $appCli = Join-Path $releaseDir 'venv\Scripts\airgap-sync.exe'
    if (-not (Test-Path -LiteralPath $venvPython)) { Fail "venv python missing: $venvPython" }
    if (-not (Test-Path -LiteralPath $appCli)) { Fail "airgap-sync CLI missing: $appCli" }
    Invoke-Checked $venvPython @((Join-Path $releaseDir 'release_manifest.py'), 'smoke-app') 'application imports'
    $versionOutput = (& $appCli --version) -join ' '
    if ($LASTEXITCODE -ne 0) { Fail 'airgap-sync --version failed' }
    Write-Host "  $versionOutput"
    Write-Host "  current -> $current"
    Write-Host 'Installed release verification OK (no database connection was made)'
}

function Do-InitConfig {
    if ($Role -eq '') { Fail 'InitConfig requires -Role source|destination' }
    $example = Join-Path $script:BundleRoot ("config\" + $Role + ".example.yaml")
    if (-not (Test-Path -LiteralPath $example)) { Fail "example config missing in bundle: $example" }
    $target = Join-Path $ConfigRoot ($Role + '.yaml')
    if (Test-Path -LiteralPath $target) {
        Fail "refusing to overwrite existing config: $target (edits are never touched by upgrades)"
    }
    New-Item -ItemType Directory -Path $ConfigRoot -Force | Out-Null
    Copy-Item -LiteralPath $example -Destination $target
    Write-Host "Config initialized: $target"
    $current = Join-Path $InstallRoot 'current\venv\Scripts\airgap-sync.exe'
    Write-Host 'Edit it, set the password/token environment variables, then run:'
    Write-Host "  $current config validate --config $target"
}

try {
    switch ($Action) {
        'Verify' { Invoke-VerifyBundle (Read-Manifest) }
        'Install' { Do-Install (Read-Manifest) }
        'Upgrade' { Do-Upgrade (Read-Manifest) }
        'Rollback' { Do-Rollback (Read-Manifest) }
        'Status' { Do-Status }
        'VerifyInstalled' { Do-VerifyInstalled }
        'InitConfig' { Do-InitConfig }
        default { Fail "unhandled action: $Action" }
    }
} catch {
    if ($Action -eq 'Install') {
        Write-Host "INSTALL FAILED: $_" -ForegroundColor Red
    } else {
        Write-Host "ERROR: $_" -ForegroundColor Red
    }
    exit 1
}
