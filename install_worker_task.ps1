$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

$runScript = Join-Path $PSScriptRoot "run_worker.ps1"
if (-not (Test-Path $runScript)) {
    throw "No existe $runScript"
}

$startupDir = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\Startup"
$startupCmd = Join-Path $startupDir "Iniciar QuiniAI Worker.cmd"

if (-not (Test-Path $startupDir)) {
    New-Item -ItemType Directory -Path $startupDir -Force | Out-Null
}

$cmdContent = @(
    "@echo off"
    ('start "" powershell.exe -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File "{0}"' -f $runScript)
)

Set-Content -LiteralPath $startupCmd -Value $cmdContent -Encoding ASCII

Write-Output "Autoarranque instalado en:"
Write-Output $startupCmd
