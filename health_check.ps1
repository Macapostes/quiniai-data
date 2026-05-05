$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

$host.UI.RawUI.BackgroundColor = "Black"
$host.UI.RawUI.ForegroundColor = "Green"
Clear-Host

$statusJson = Join-Path $PSScriptRoot "output\\ULTIMO_ESTADO_QUINIAI.json"
$statusTxt = Join-Path $PSScriptRoot "output\\ULTIMO_ESTADO_QUINIAI.txt"
$workerLog = Join-Path $PSScriptRoot "logs\\worker_events.log"
$supervisorLog = Join-Path $PSScriptRoot "logs\\worker_supervisor.log"
$workerLock = Join-Path $PSScriptRoot "cache\\snapshot_worker.lock"

function Write-Line($text, $color = "Green") {
    Write-Host $text -ForegroundColor $color
}

function Minutes-Old($timestamp) {
    if (-not $timestamp) { return $null }
    try {
        $dt = [DateTimeOffset]::Parse($timestamp)
        return [math]::Round(((Get-Date).ToUniversalTime() - $dt.UtcDateTime).TotalMinutes, 1)
    } catch {
        return $null
    }
}

function Format-LocalTime($timestamp) {
    if (-not $timestamp) { return "-" }
    try {
        $dt = [DateTimeOffset]::Parse($timestamp).ToLocalTime()
        return $dt.ToString("dd/MM/yyyy HH:mm:ss") + " (Madrid)"
    } catch {
        return $timestamp
    }
}

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

$workerProcess = $null
$processCheckError = $null
try {
    $workerProcess = Get-CimInstance Win32_Process |
        Where-Object {
            $_.Name -match 'python' -and
            $_.CommandLine -match 'snapshot_worker\.py'
        } |
        Select-Object -First 1
} catch {
    $processCheckError = $_.Exception.Message
}

if (-not $workerProcess) {
    $workerProcess = Get-WorkerFromLock
}

$status = $null
if (Test-Path $statusJson) {
    try {
        $status = Get-Content $statusJson -Raw | ConvertFrom-Json
    } catch {
        $status = $null
    }
}

Write-Line "=============================================================="
Write-Line "  QUINIAI WORKER :: HEALTH PANEL"
Write-Line "=============================================================="

if ($workerProcess) {
    Write-Line "  Process running: YES" "Green"
} elseif ($processCheckError) {
    Write-Line "  Process running: UNKNOWN ($processCheckError)" "Yellow"
} else {
    Write-Line "  Process running: NO" "Red"
}

if (-not $status) {
    Write-Line ""
    Write-Line "  No se pudo leer el estado estructurado." "Yellow"
    if (Test-Path $statusTxt) {
        Write-Line "  Existe el estado en texto, pero no el JSON." "Yellow"
    }
    Write-Line ""
    Write-Line "  Ventana cerrandose en 10 segundos..." "DarkGreen"
    Start-Sleep -Seconds 10
    exit
}

$snapshotAge = Minutes-Old $status.snapshot_generated_at
$pollSeconds = [int]($status.poll_seconds)
$staleThresholdMinutes = [math]::Round(($pollSeconds * 2) / 60, 1)
$okColor = if ($status.ok) { "Green" } else { "Red" }

Write-Line "  Last successful snapshot: $(Format-LocalTime $status.snapshot_generated_at)" $okColor
Write-Line "  Raw UTC snapshot: $($status.snapshot_generated_at)" "DarkGreen"
if ($snapshotAge -ne $null) {
    $ageColor = if ($snapshotAge -le $staleThresholdMinutes) { "Green" } else { "Yellow" }
    Write-Line "  Snapshot age: $snapshotAge minutes" $ageColor
}
Write-Line "  Poll interval: $pollSeconds seconds" "Green"

$coverage = $status.coverage
$structured = $status.structured_db_summary
$integrity = $status.quiniela_integrity

Write-Line ""
Write-Line "  Coverage"
Write-Line "  - Monitored matches: $($coverage.monitored_matches)"
Write-Line "  - Jornada actual: $($coverage.quiniela_current_jornada)"
Write-Line "  - Ultima oficial publicada: $($coverage.quiniela_latest_available_jornada)"
Write-Line "  - Partidos jornada actual: $($coverage.focus_matches)"
Write-Line "  - Partidos quiniela rastreados: $($coverage.tracked_quiniela_matches)"
Write-Line "  - Jornadas oficiales: $($coverage.quiniela_jornadas)"
Write-Line "  - Weather matches: $($coverage.weather_matches)"
Write-Line "  - Travel matches: $($coverage.travel_matches)"
Write-Line "  - History matches: $($coverage.history_matches)"
Write-Line "  - Healthy sources: $($coverage.sources_ok)/$($coverage.sources_total)"
Write-Line "  - Fresh headlines: $($coverage.fresh_headlines)"
Write-Line "  - Structured tracked matches: $($coverage.structured_focus_matches)"
Write-Line "  - Structured referees: $($coverage.structured_referees)"
if ($integrity) {
    $integrityColor = if ($integrity.ok) { "Green" } else { "Red" }
    Write-Line "  - Quiniela integrity: $(if ($integrity.ok) { 'OK' } else { 'ERROR' }) | slots=$($integrity.checked_slots) | fallos=$($integrity.mismatch_count)" $integrityColor
}

if ($structured) {
    Write-Line ""
    Write-Line "  Structured DB"
    Write-Line "  - Matches: $($structured.matches)"
    Write-Line "  - Teams: $($structured.teams)"
    Write-Line "  - Referees: $($structured.referees)"
    Write-Line "  - Last pruned: $(Format-LocalTime $structured.last_pruned_at)"
}

if ($status.last_error) {
    Write-Line ""
    Write-Line "  Last error: $($status.last_error)" "Red"
}

if ($integrity -and -not $integrity.ok -and @($integrity.mismatches).Count -gt 0) {
    Write-Line ""
    Write-Line "  Fallos de integridad detectados" "Red"
    foreach ($mismatch in @($integrity.mismatches) | Select-Object -First 6) {
        Write-Line "  - J$($mismatch.jornada) slot $($mismatch.position): oficial=$($mismatch.official_local) vs $($mismatch.official_visitante) | resuelto=$($mismatch.resolved_local) vs $($mismatch.resolved_visitante)" "Red"
    }
}

$lastRuns = @($status.last_runs)
if ($lastRuns.Count -gt 0) {
    Write-Line ""
    Write-Line "  Ultimas ejecuciones"
    foreach ($run in ($lastRuns | Select-Object -Last 5)) {
        $label = if ($run.ok) { "OK" } else { "ERROR" }
        $color = if ($run.ok) { "Green" } else { "Red" }
        Write-Line "  - [$label] $(Format-LocalTime $run.finished_at) | $($run.duration_seconds)s | jornada=$($run.current_jornada) | partidos=$($run.tracked_matches) | $($run.error)" $color
    }
}

Write-Line ""
Write-Line "  Estado en texto: $statusTxt" "DarkGreen"
Write-Line "  Estado en JSON:  $statusJson" "DarkGreen"
Write-Line "  Log worker:      $workerLog" "DarkGreen"
Write-Line "  Log supervisor:  $supervisorLog" "DarkGreen"
Write-Line ""
Write-Line "  Ventana cerrandose en 12 segundos..." "DarkGreen"
Start-Sleep -Seconds 12
