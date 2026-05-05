$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

$host.UI.RawUI.BackgroundColor = "Black"
$host.UI.RawUI.ForegroundColor = "Green"
Clear-Host

$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$worker = Join-Path $PSScriptRoot "snapshot_worker.py"
$supervisor = Join-Path $PSScriptRoot "run_worker.ps1"
$statusFile = Join-Path $PSScriptRoot "output\\ULTIMO_ESTADO_QUINIAI.txt"
$statusJson = Join-Path $PSScriptRoot "output\\ULTIMO_ESTADO_QUINIAI.json"
$workerLog = Join-Path $PSScriptRoot "logs\\worker_events.log"
$supervisorLog = Join-Path $PSScriptRoot "logs\\worker_supervisor.log"
$manualRefreshFlag = Join-Path $PSScriptRoot "cache\\manual_refresh.flag"
$workerLock = Join-Path $PSScriptRoot "cache\\snapshot_worker.lock"

function Get-WorkerFromLock {
    if (-not (Test-Path $workerLock)) {
        return $null
    }
    try {
        $lock = Get-Content $workerLock -Raw | ConvertFrom-Json
        if (-not $lock.pid) {
            return $null
        }
        return Get-Process -Id ([int]$lock.pid) -ErrorAction SilentlyContinue
    } catch {
        return $null
    }
}

Write-Host "=============================================================="
Write-Host "  QUINIAI WORKER :: BOOT SEQUENCE"
Write-Host "=============================================================="
Write-Host "  Cargando fuentes externas, contexto quiniela y estado local..."
Write-Host ""

$bootLines = @(
    "[feed] sync odds snapshot repository",
    "[feed] connect open-meteo forecast layer",
    "[feed] connect wikipedia team profiles",
    "[feed] connect google news signals",
    "[feed] connect football-data historical csv",
    "[feed] connect thesportsdb event metadata",
    "[scan] resolving team identities",
    "[scan] loading official quiniela jornadas",
    "[scan] refreshing structured referee layer",
    "[scan] refreshing structured injury layer",
    "[scan] calculating pressure, fatigue and elo metrics",
    "[scan] pruning expired matches",
    "[push] preparing backend payload"
)

foreach ($line in $bootLines) {
    Write-Host ("  " + $line) -ForegroundColor DarkGreen
    Start-Sleep -Milliseconds 140
}

Write-Host ""

if (-not (Test-Path $python)) {
    Write-Host "  ERROR: no existe el entorno virtual." -ForegroundColor Red
    Start-Sleep -Seconds 8
    exit 1
}

$alreadyRunning = $null
try {
    $alreadyRunning = Get-CimInstance Win32_Process |
        Where-Object {
            $_.Name -match 'python' -and
            $_.CommandLine -match 'snapshot_worker\.py'
        } |
        Select-Object -First 1
} catch {
    Write-Host "  Aviso: no pude consultar procesos por WMI; el worker validara el lock." -ForegroundColor Yellow
}

if (-not $alreadyRunning) {
    $alreadyRunning = Get-WorkerFromLock
}

Write-Host ""
if ($alreadyRunning) {
    Set-Content -Path $manualRefreshFlag -Value (Get-Date).ToString("o") -Encoding UTF8
    Write-Host "  Worker en segundo plano: ya estaba activo." -ForegroundColor Green
    Write-Host "  He dejado una orden de refresco manual para el siguiente ciclo inmediato." -ForegroundColor Green
} else {
    & $python $worker --once --pretty
    $exitCode = $LASTEXITCODE

    if ($exitCode -ne 0) {
        Write-Host ""
        Write-Host "  ERROR: la pasada manual ha fallado." -ForegroundColor Red
        Write-Host "  Revisa Estado QuiniAI.txt o worker_events.log" -ForegroundColor Yellow
        Start-Sleep -Seconds 10
        exit $exitCode
    }

    Start-Process -FilePath "powershell.exe" -ArgumentList @(
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        "`"$supervisor`""
    ) -WindowStyle Minimized
    Write-Host "  Worker en segundo plano: supervisor iniciado correctamente." -ForegroundColor Green
}

if (Test-Path $statusFile) {
    Write-Host ""
    Write-Host "  Estado guardado en:" -ForegroundColor Green
    Write-Host "  $statusFile"
    if (Test-Path $statusJson) {
        try {
            $status = Get-Content $statusJson -Raw | ConvertFrom-Json
            if ($status.snapshot_generated_at) {
                $lastOk = [DateTimeOffset]::Parse($status.snapshot_generated_at).ToLocalTime().ToString("dd/MM/yyyy HH:mm:ss")
                Write-Host "  Ultimo snapshot: $lastOk (Madrid)" -ForegroundColor Green
            }
        } catch {
        }
    }
    Write-Host "  Logs:" -ForegroundColor Green
    Write-Host "  $workerLog"
    Write-Host "  $supervisorLog"
}

Write-Host ""
Write-Host "  Ventana cerrandose en 6 segundos..." -ForegroundColor DarkGreen
Start-Sleep -Seconds 6
