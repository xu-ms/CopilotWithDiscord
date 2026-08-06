$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
& (Join-Path $root 'src/copilotd/ops/scripts/acceptance-windows.ps1') @args
exit $LASTEXITCODE
