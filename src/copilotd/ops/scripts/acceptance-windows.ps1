param()
$ErrorActionPreference = 'Stop'

function Fail-Prerequisite([string]$Message) {
  [Console]::Error.WriteLine("Windows acceptance prerequisite failed: $Message")
  exit 2
}

if ($env:OS -ne 'Windows_NT') { Fail-Prerequisite 'requires a real Windows host' }
if ([string]::IsNullOrWhiteSpace($env:COPILOTD_DISCORD_TOKEN)) {
  Fail-Prerequisite 'COPILOTD_DISCORD_TOKEN is required'
}
foreach ($Command in @('copilotd', 'Get-ScheduledTask', 'Export-ScheduledTask')) {
  if ($null -eq (Get-Command $Command -ErrorAction SilentlyContinue)) {
    Fail-Prerequisite "missing command: $Command"
  }
}

$setup = copilotd setup | ConvertFrom-Json
if (-not $setup.ok -or -not $setup.result.status.ready) {
  throw 'copilotD setup did not reach ready state'
}
$status = copilotd service status | ConvertFrom-Json
if (-not $status.result.process_identity_matches) {
  throw 'Task Scheduler process PID does not match heartbeat PID'
}
$expected = @('copilotD Bot', 'copilotD Watchdog')
if ($status.result.topology -eq 'sidecar') { $expected += 'copilotD Runtime' }
foreach ($TaskName in $expected) {
  $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
  $xml = Export-ScheduledTask -TaskName $TaskName -ErrorAction Stop
  if ($xml -notmatch '<WakeToRun>false</WakeToRun>') {
    throw "$TaskName has WakeToRun drift"
  }
  if ($xml -notmatch '<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>') {
    throw "$TaskName has an execution time limit"
  }
  if ($TaskName -eq 'copilotD Watchdog' -and $xml -notmatch '<Interval>PT5M</Interval>') {
    throw 'watchdog repetition is not PT5M'
  }
  if ($task.State -eq 'Disabled') { throw "$TaskName is disabled" }
}
$resume = Get-WinEvent -FilterHashtable @{
  LogName='Microsoft-Windows-Power-Troubleshooter/Operational'
  Id=1
} -MaxEvents 1 -ErrorAction SilentlyContinue
if ($null -eq $resume) {
  $resume = Get-WinEvent -FilterHashtable @{
    LogName='System'
    ProviderName='Microsoft-Windows-Power-Troubleshooter'
    Id=1
  } -MaxEvents 1 -ErrorAction SilentlyContinue
}
if ($null -eq $resume) {
  Fail-Prerequisite 'no Power-Troubleshooter resume event is available'
}
