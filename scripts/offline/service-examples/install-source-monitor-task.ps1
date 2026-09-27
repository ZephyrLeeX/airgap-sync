<# Register an independent five-minute one-shot task. Run elevated. #>
param(
    [string] $InstallRoot = 'C:\Program Files\AirgapSync',
    [string] $Config = 'C:\ProgramData\AirgapSync\config\source.yaml',
    [string] $LogFile = ''
)
$ErrorActionPreference = 'Stop'
$wrapper = Join-Path $InstallRoot 'run-source-monitor.ps1'
if (-not (Test-Path -LiteralPath $wrapper -PathType Leaf)) { throw 'Monitor wrapper missing' }
if (-not (Test-Path -LiteralPath $Config -PathType Leaf)) { throw 'Source config missing' }
$arguments = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "' + $wrapper + '" -Config "' + $Config + '"'
if ($LogFile) { $arguments += ' -LogFile "' + $LogFile + '"' }
$argumentsXml = [System.Security.SecurityElement]::Escape($arguments)
$rootXml = [System.Security.SecurityElement]::Escape($InstallRoot)
$start = (Get-Date).Date.ToString('yyyy-MM-ddTHH:mm:ss')
$taskXml = @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <Triggers><CalendarTrigger>
    <Repetition><Interval>PT5M</Interval><Duration>P1D</Duration><StopAtDurationEnd>false</StopAtDurationEnd></Repetition>
    <StartBoundary>$start</StartBoundary><Enabled>true</Enabled>
    <ScheduleByDay><DaysInterval>1</DaysInterval></ScheduleByDay>
  </CalendarTrigger></Triggers>
  <Principals><Principal id="Author"><UserId>S-1-5-18</UserId><LogonType>ServiceAccount</LogonType><RunLevel>HighestAvailable</RunLevel></Principal></Principals>
  <Settings><MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy><ExecutionTimeLimit>PT2M</ExecutionTimeLimit><Enabled>true</Enabled></Settings>
  <Actions Context="Author"><Exec><Command>powershell.exe</Command><Arguments>$argumentsXml</Arguments><WorkingDirectory>$rootXml</WorkingDirectory></Exec></Actions>
</Task>
"@
Register-ScheduledTask -TaskName 'Airgap Sync Source Monitor' -Xml $taskXml -ErrorAction Stop | Out-Null
