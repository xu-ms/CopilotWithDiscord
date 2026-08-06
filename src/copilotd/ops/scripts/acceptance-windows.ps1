param()
$ErrorActionPreference = 'Stop'

function Fail-Prerequisite([string]$Message) {
  [Console]::Error.WriteLine("Windows acceptance prerequisite failed: $Message")
  exit 2
}

function Get-AcceptanceProcessTreeIds([object[]]$Roots) {
  $all = @(Get-CimInstance Win32_Process)
  $pending = [Collections.Generic.Queue[int]]::new()
  $ids = [Collections.Generic.HashSet[int]]::new()
  foreach ($root in $Roots) { $pending.Enqueue([int]$root.ProcessId) }
  while ($pending.Count -gt 0) {
    $id = $pending.Dequeue()
    if (-not $ids.Add($id)) { continue }
    foreach ($child in $all | Where-Object { $_.ParentProcessId -eq $id }) {
      $pending.Enqueue([int]$child.ProcessId)
    }
  }
  return @($ids)
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
  'Get-CimInstance',
  'Export-ScheduledTask',
  'Register-ScheduledTask',
  'New-ScheduledTaskSettingsSet',
  'Disable-ScheduledTask',
  'Stop-ScheduledTask',
  'Unregister-ScheduledTask',
  'taskkill.exe'
)) {
  if ($null -eq (Get-Command $Command -ErrorAction SilentlyContinue)) {
    Fail-Prerequisite "missing command: $Command"
  }
}

$cleanupTasks = @('copilotD Runtime', 'copilotD Bot', 'copilotD Watchdog')
foreach ($TaskName in $cleanupTasks) {
  if ($null -ne (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue)) {
    Fail-Prerequisite "existing scheduled task would be replaced: $TaskName"
  }
}
$expected = @('copilotD Bot', 'copilotD Watchdog')
$acceptanceRoot = Join-Path $env:TEMP "copilotd-windows-acceptance-$([Guid]::NewGuid())"
$env:COPILOTD_DATA_DIR = Join-Path $acceptanceRoot 'state'
$env:COPILOTD_CACHE_DIR = Join-Path $acceptanceRoot 'cache'
$env:COPILOTD_LOG_DIR = Join-Path $acceptanceRoot 'logs'
$serviceAttempted = $false
try {
  $serviceAttempted = $true
  $setup = copilotd setup | ConvertFrom-Json
  if (-not $setup.ok -or -not $setup.result.status.ready) {
    throw 'copilotD setup did not reach ready state'
  }
  $status = copilotd service status | ConvertFrom-Json
  if (-not $status.result.process_identity_matches) {
    throw 'Task Scheduler process PID does not match heartbeat PID'
  }
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
  $wakeAt = (Get-Date).AddMinutes(3)
  $wakeAction = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument (
    "-NoProfile -NonInteractive -Command " +
    "`"Set-Content -LiteralPath '$($wakeMarker.Replace("'", "''"))' " +
    "-Value ([DateTime]::UtcNow.ToString('o')) -Encoding UTF8`""
  )
  $wakeTrigger = New-ScheduledTaskTrigger -Once -At $wakeAt
  $wakeSettings = New-ScheduledTaskSettingsSet -WakeToRun -StartWhenAvailable
  $wakePrincipal = New-ScheduledTaskPrincipal -UserId (
    [Security.Principal.WindowsIdentity]::GetCurrent().Name
  ) -LogonType Interactive -RunLevel Limited
  try {
  Register-ScheduledTask -TaskName $wakeTask -Action $wakeAction `
    -Trigger $wakeTrigger -Settings $wakeSettings -Principal $wakePrincipal `
    -Force | Out-Null
  $wakeInfo = Get-ScheduledTaskInfo -TaskName $wakeTask -ErrorAction Stop
  if ($wakeInfo.NextRunTime -lt (Get-Date).AddMinutes(2)) {
    throw 'wake trigger does not have sufficient suspend margin'
  }
  $sleepRequestedAt = [DateTime]::UtcNow
  $wakeDeadline = $wakeAt.ToUniversalTime().AddMinutes(2)
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
} finally {
  $cleanupFailure = $null
  if ($serviceAttempted) {
    try {
      $uninstall = copilotd service uninstall | ConvertFrom-Json
      if (-not $uninstall.ok) { throw 'copilotD uninstall reported failure' }
    } catch {
      $cleanupFailure = $_
    }
  }
  foreach ($TaskName in $cleanupTasks) {
    try {
      Disable-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue `
        | Out-Null
      Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    } catch {
      if ($null -eq $cleanupFailure) { $cleanupFailure = $_ }
    }
  }
  foreach ($TaskName in $cleanupTasks) {
    try {
      Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false `
        -ErrorAction SilentlyContinue
    } catch {
      if ($null -eq $cleanupFailure) { $cleanupFailure = $_ }
    }
  }
  try {
    $acceptanceProcesses = @(Get-CimInstance Win32_Process | Where-Object {
      ([string]$_.CommandLine).IndexOf(
        $acceptanceRoot,
        [StringComparison]::OrdinalIgnoreCase
      ) -ge 0
    })
    $trackedProcesses = @(Get-AcceptanceProcessTreeIds $acceptanceProcesses)
    foreach ($process in $acceptanceProcesses) {
      & taskkill.exe /PID $process.ProcessId /T /F | Out-Null
    }
    $processDeadline = [DateTime]::UtcNow.AddSeconds(15)
    do {
      $remainingTracked = @($trackedProcesses | Where-Object {
        $null -ne (Get-Process -Id $_ -ErrorAction SilentlyContinue)
      })
      $remainingAcceptance = @(Get-CimInstance Win32_Process | Where-Object {
        ([string]$_.CommandLine).IndexOf(
          $acceptanceRoot,
          [StringComparison]::OrdinalIgnoreCase
        ) -ge 0
      })
      if (
        $remainingTracked.Count -eq 0 -and
        $remainingAcceptance.Count -eq 0
      ) {
        break
      }
      Start-Sleep -Milliseconds 200
    } while ([DateTime]::UtcNow -lt $processDeadline)
    if (
      $remainingTracked.Count -ne 0 -or
      $remainingAcceptance.Count -ne 0
    ) {
      throw 'acceptance service process tree remains after cleanup'
    }
  } catch {
    if ($null -eq $cleanupFailure) { $cleanupFailure = $_ }
  }
  foreach ($TaskName in $cleanupTasks) {
    if ($null -ne (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue)) {
      if ($null -eq $cleanupFailure) {
        $cleanupFailure = "scheduled task remains after cleanup: $TaskName"
      }
    }
  }
  if (Test-Path -LiteralPath $acceptanceRoot) {
    $secretPath = Join-Path $env:COPILOTD_DATA_DIR 'config\service-secrets.json'
    $token = [Text.Encoding]::UTF8.GetBytes($env:COPILOTD_DISCORD_TOKEN)
    $leakFailure = $null
    try {
      Get-ChildItem -LiteralPath $acceptanceRoot -File -Recurse | ForEach-Object {
        if ($_.FullName -ne $secretPath) {
          $bytes = [IO.File]::ReadAllBytes($_.FullName)
          if ($bytes.Length -ge $token.Length) {
            for ($index = 0; $index -le $bytes.Length - $token.Length; $index++) {
              $matches = $true
              for ($tokenIndex = 0; $tokenIndex -lt $token.Length; $tokenIndex++) {
                if ($bytes[$index + $tokenIndex] -ne $token[$tokenIndex]) {
                  $matches = $false
                  break
                }
              }
              if ($matches) {
                throw "credential leaked into acceptance artifact: $($_.Name)"
              }
            }
          }
        }
      }
    } catch {
      $leakFailure = $_
    } finally {
      if (Test-Path -LiteralPath $secretPath) {
        $length = [int](Get-Item -LiteralPath $secretPath).Length
        [IO.File]::WriteAllBytes($secretPath, [byte[]]::new($length))
      }
      Remove-Item -LiteralPath $acceptanceRoot -Recurse -Force
    }
    if ($null -eq $cleanupFailure -and $null -ne $leakFailure) {
      $cleanupFailure = $leakFailure
    }
  }
  if ($null -ne $cleanupFailure) { throw $cleanupFailure }
}
