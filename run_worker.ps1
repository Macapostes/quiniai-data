$ErrorActionPreference = "Continue"

Set-Location $PSScriptRoot

$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$script = Join-Path $PSScriptRoot "snapshot_worker.py"
$workerStdoutLog = Join-Path $PSScriptRoot "logs\\worker_stdout.log"
$supervisorLog = Join-Path $PSScriptRoot "logs\\worker_supervisor.log"
$healthMonitorPython = "C:\Users\mario\Desktop\Bot Trading\.venv\Scripts\python.exe"
$healthMonitorScript = "C:\Users\mario\Desktop\Bot Trading\tools\desktop_launchers\watch_quiniai_worker.py"
$restartDelaySeconds = 20
# El worker se relanza SIEMPRE, tambien tras una salida limpia (codigo 0). No
# hay bucle en caliente: tras un codigo 0 se espera un poco, y si la ultima
# sincronizacion es reciente el worker nuevo duerme hasta el siguiente ciclo
# (startup_skipped_recent_sync) en vez de volver a descargar nada.
$cleanExitDelaySeconds = 60
if ($env:QUINIAI_WORKER_RESTART_DELAY) { $restartDelaySeconds = [int]$env:QUINIAI_WORKER_RESTART_DELAY }
if ($env:QUINIAI_WORKER_CLEAN_EXIT_DELAY) { $cleanExitDelaySeconds = [int]$env:QUINIAI_WORKER_CLEAN_EXIT_DELAY }
# Unica forma de pararlo a proposito (ver DETENER_WORKER.md). El worker tambien
# lo mira y sale; "Iniciar QuiniAI Worker.cmd" lo borra al arrancar.
$stopFlag = Join-Path $PSScriptRoot "cache\stop_worker.flag"

function Write-SupervisorLog([string]$message, [string]$level = "INFO") {
    $line = "{0} | {1} | {2}" -f ([DateTimeOffset]::UtcNow.ToString("o")), $level, $message
    $line | Out-File -FilePath $supervisorLog -Encoding utf8 -Append
}

function Test-StopRequested {
    return (Test-Path -LiteralPath $stopFlag)
}

# Duerme en pasos de 5 s. Devuelve $true si mientras tanto se ha pedido parar.
function Wait-OrStop([int]$seconds) {
    $remaining = $seconds
    while ($remaining -gt 0) {
        if (Test-StopRequested) {
            return $true
        }
        $step = [Math]::Min(5, $remaining)
        Start-Sleep -Seconds $step
        $remaining -= $step
    }
    return (Test-StopRequested)
}

if (-not (Test-Path $python)) {
    Write-SupervisorLog "No existe el entorno virtual en $python" "ERROR"
    throw "No existe el entorno virtual en $python"
}

# Un solo supervisor a la vez: si ya hay otro vigilando, este no hace falta.
$supervisorMutex = New-Object System.Threading.Mutex($false, "Local\QuiniAIWorkerSupervisor")
$ownsMutex = $false
try {
    $ownsMutex = $supervisorMutex.WaitOne(0)
} catch {
    # AbandonedMutexException: el supervisor anterior murio sin soltarlo y el
    # mutex pasa a este. Ante cualquier otro fallo, mejor vigilar que no.
    $ownsMutex = $true
}
if (-not $ownsMutex) {
    Write-SupervisorLog "Ya hay otro supervisor activo. Este sale sin duplicar."
    exit 0
}

Write-SupervisorLog "Supervisor arrancado. Worker path=$script"

function Start-WorkerHealthMonitor {
    if (-not (Test-Path -LiteralPath $healthMonitorPython) -or -not (Test-Path -LiteralPath $healthMonitorScript)) {
        Write-SupervisorLog "Monitor de salud no disponible; se mantiene el supervisor básico." "WARN"
        return
    }
    $runningMonitor = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -match 'python' -and ([string]$_.CommandLine) -match 'watch_quiniai_worker\.py' } |
        Select-Object -First 1
    if ($runningMonitor) {
        return
    }
    Start-Process -FilePath $healthMonitorPython -WorkingDirectory "C:\Users\mario\Desktop\Bot Trading" -ArgumentList ("-u `"$healthMonitorScript`"") -WindowStyle Hidden
    Write-SupervisorLog "Monitor de salud de Datos jornada iniciado (umbral: 6 horas)."
}

Start-WorkerHealthMonitor

$lastBusyPid = $null
try {
    while ($true) {
        if (Test-StopRequested) {
            Write-SupervisorLog "Parada solicitada ($stopFlag). El supervisor sale sin relanzar el worker."
            break
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
            Write-SupervisorLog ("No pude consultar procesos por WMI; delego el lock al worker: " + $_.Exception.Message) "WARN"
        }

        if ($alreadyRunning) {
            # Otro worker vivo (una pasada manual --once, o uno que quedo de un
            # supervisor anterior): no se duplica, pero tampoco se abandona la
            # vigilancia. Cuando termine, se lanza el nuestro.
            if ($lastBusyPid -ne $alreadyRunning.ProcessId) {
                Write-SupervisorLog ("Detectado worker ya vivo con PID " + $alreadyRunning.ProcessId + ". No lanzo otro; sigo vigilando cada " + $cleanExitDelaySeconds + " s.")
                $lastBusyPid = $alreadyRunning.ProcessId
            }
            [void](Wait-OrStop $cleanExitDelaySeconds)
            continue
        }
        $lastBusyPid = $null

        $exitCode = $null
        try {
            Write-SupervisorLog "Lanzando proceso Python persistente del worker"
            # CreateNoWindow=true evita cualquier ventana visible aunque el padre sea hidden
            $psi = New-Object System.Diagnostics.ProcessStartInfo
            $psi.FileName        = $python
            $psi.Arguments       = "-u `"$script`""
            $psi.CreateNoWindow  = $true
            $psi.UseShellExecute = $false
            $psi.RedirectStandardOutput = $true
            $psi.RedirectStandardError  = $true
            $p = [System.Diagnostics.Process]::Start($psi)
            $outTask = $p.StandardOutput.ReadToEndAsync()
            $errTask = $p.StandardError.ReadToEndAsync()
            $p.WaitForExit()
            $outTask.Wait()
            $errTask.Wait()
            if ($outTask.Result) {
                $outTask.Result | Out-File -FilePath $workerStdoutLog -Encoding utf8 -Append
            }
            if ($errTask.Result) {
                $errTask.Result | Out-File -FilePath $workerStdoutLog -Encoding utf8 -Append
            }
            $exitCode = $p.ExitCode
            Write-SupervisorLog "El proceso Python termino con codigo $exitCode" "WARN"
        } catch {
            Write-SupervisorLog ("Supervisor capturo error: " + $_.Exception.Message) "ERROR"
        }

        if (Test-StopRequested) {
            continue
        }
        if ($exitCode -eq 0) {
            Write-SupervisorLog ("Salida limpia (codigo 0): el worker se relanza en " + $cleanExitDelaySeconds + " s. Para pararlo a proposito, crea " + $stopFlag) "WARN"
            [void](Wait-OrStop $cleanExitDelaySeconds)
        } else {
            Write-SupervisorLog ("Reintento en " + $restartDelaySeconds + " segundos") "WARN"
            [void](Wait-OrStop $restartDelaySeconds)
        }
    }
} finally {
    Write-SupervisorLog "Supervisor detenido."
    try { $supervisorMutex.ReleaseMutex() } catch { }
}
exit 0
