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
if ($env:COPILOTD_ACCEPTANCE_ALLOW_SLEEP -ne '1') {
  Fail-Prerequisite 'COPILOTD_ACCEPTANCE_ALLOW_SLEEP=1 is required'
}
foreach ($Command in @(
  'copilotd',
  'Get-ScheduledTask',
  'Export-ScheduledTask',
  'Register-ScheduledTask',
  'New-ScheduledTaskSettingsSet'
)) {
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
  if ($xml -notmatch '<Interval>PT1M</Interval>') {
    throw "$TaskName has an invalid restart interval"
  }
  if ($xml -notmatch '<Count>255</Count>') {
    throw "$TaskName has an invalid restart count"
  }
  if ($TaskName -eq 'copilotD Watchdog' -and $xml -notmatch '<Interval>PT5M</Interval>') {
    throw 'watchdog repetition is not PT5M'
  }
  if ($task.State -eq 'Disabled') { throw "$TaskName is disabled" }
}

$wakeTask = "copilotD Acceptance Wake $([Guid]::NewGuid())"
$wakeMarker = Join-Path $env:TEMP "$wakeTask.txt"
$sleepRequestedAt = [DateTime]::UtcNow
$wakeDeadline = $sleepRequestedAt.AddMinutes(3)
$wakeAction = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument (
  "-NoProfile -NonInteractive -Command " +
  "`"Set-Content -LiteralPath '$($wakeMarker.Replace("'", "''"))' " +
  "-Value ([DateTime]::UtcNow.ToString('o')) -Encoding UTF8`""
)
$wakeTrigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1)
$wakeSettings = New-ScheduledTaskSettingsSet -WakeToRun -StartWhenAvailable
$wakePrincipal = New-ScheduledTaskPrincipal -UserId (
  [Security.Principal.WindowsIdentity]::GetCurrent().Name
) -LogonType Interactive -RunLevel Limited
try {
  Register-ScheduledTask -TaskName $wakeTask -Action $wakeAction `
    -Trigger $wakeTrigger -Settings $wakeSettings -Principal $wakePrincipal `
    -Force | Out-Null
  Add-Type -AssemblyName System.Windows.Forms
  $suspended = [Windows.Forms.Application]::SetSuspendState(
    [Windows.Forms.PowerState]::Suspend,
    $false,
    $false
  )
  if (-not $suspended) { throw 'Windows refused the requested suspend transition' }
  do {
    $resume = Get-WinEvent -FilterHashtable @{
      LogName='Microsoft-Windows-Power-Troubleshooter/Operational'
      Id=1
      StartTime=$sleepRequestedAt
    } -MaxEvents 1 -ErrorAction SilentlyContinue
    if ($null -eq $resume) {
      $resume = Get-WinEvent -FilterHashtable @{
        LogName='System'
        ProviderName='Microsoft-Windows-Power-Troubleshooter'
        Id=1
        StartTime=$sleepRequestedAt
      } -MaxEvents 1 -ErrorAction SilentlyContinue
    }
    if (
      $null -ne $resume -and
      $resume.TimeCreated.ToUniversalTime() -ge $sleepRequestedAt -and
      $resume.TimeCreated.ToUniversalTime() -le $wakeDeadline
    ) {
      if (Test-Path -LiteralPath $wakeMarker) { break }
    }
    Start-Sleep -Seconds 1
  } while ([DateTime]::UtcNow -lt $wakeDeadline)
  if ($null -eq $resume) {
    throw 'no new Windows resume event occurred after the test started'
  }
  if (-not (Test-Path -LiteralPath $wakeMarker)) {
    throw 'the WakeToRun acceptance task did not execute after resume'
  }
  $markerTime = [DateTime]::Parse(
    (Get-Content -LiteralPath $wakeMarker -Raw).Trim()
  ).ToUniversalTime()
  if (
    $markerTime -lt $sleepRequestedAt -or
    $markerTime -gt $wakeDeadline
  ) {
    throw 'the wake marker is outside the intended wake interval'
  }

  $watchdog = copilotd service watchdog | ConvertFrom-Json
  if ($watchdog.result.watchdog -ne 'recent-wake') {
    throw "wake suppression did not trigger: $($watchdog | ConvertTo-Json -Compress)"
  }
  $afterWake = copilotd service status | ConvertFrom-Json
  if (-not $afterWake.result.ready -or -not $afterWake.result.process_identity_matches) {
    throw 'service is not ready after resume'
  }
} finally {
  Unregister-ScheduledTask -TaskName $wakeTask -Confirm:$false `
    -ErrorAction SilentlyContinue
  Remove-Item -LiteralPath $wakeMarker -Force -ErrorAction SilentlyContinue
}
