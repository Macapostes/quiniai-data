# Supervisor del worker de QuiniAI. Compatible con Windows PowerShell 5.1.
#
# - Se desacopla del proceso que lo lance (consola, asistente, monitor): se
#   relanza a si mismo con Win32_Process.Create, fuera del arbol y del job de
#   quien lo llamo, para que cerrar esa consola no mate supervisor y worker.
#   -NoDetach lo ejecuta aqui mismo (depuracion a mano).
# - Relanza SIEMPRE el worker (60 s tras codigo 0, 20 s tras error), salvo que
#   exista cache\stop_worker.flag (ver DETENER_WORKER.md).
# - La salida del worker va directa a archivos por redireccion del sistema
#   (cmd.exe ... 1>> 2>>): el supervisor no tiene tuberias ni guarda nada en
#   memoria. logs\worker_stdout.log y logs\worker_stderr.log, rotados a .1 al
#   pasar de 10 MB. Cada salida queda en worker_supervisor.log y
#   worker_last_exit.json; cada caida (codigo distinto de 0) en
#   worker_crashes.log con el final de stderr.
param([switch]$NoDetach)

$ErrorActionPreference = "Continue"

Set-Location $PSScriptRoot

$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$script = Join-Path $PSScriptRoot "snapshot_worker.py"
$logDir = Join-Path $PSScriptRoot "logs"
$supervisorLog = Join-Path $logDir "worker_supervisor.log"
$workerStdoutLog = Join-Path $logDir "worker_stdout.log"
$workerStderrLog = Join-Path $logDir "worker_stderr.log"
$crashLog = Join-Path $logDir "worker_crashes.log"
$lastExitFile = Join-Path $logDir "worker_last_exit.json"
$heartbeatFile = Join-Path $logDir "worker_supervisor_heartbeat.txt"
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
$isWindows5 = ($env:OS -eq "Windows_NT")

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

function Write-SupervisorLog([string]$message, [string]$level = "INFO") {
    $line = "{0} | {1} | sup={2} | {3}" -f ([DateTimeOffset]::UtcNow.ToString("o")), $level, $PID, $message
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

# Solo se rota antes de lanzar, cuando nadie esta escribiendo en los archivos.
function Invoke-LogRotation {
    foreach ($file in @($workerStdoutLog, $workerStderrLog, $crashLog)) {
        $item = Get-Item -LiteralPath $file -ErrorAction SilentlyContinue
        if ($item -and $item.Length -gt 10MB) {
            Move-Item -LiteralPath $file -Destination ($file + ".1") -Force -ErrorAction SilentlyContinue
        }
    }
}

if (Test-StopRequested) {
    Write-SupervisorLog "Parada solicitada ($stopFlag). El supervisor no arranca."
    exit 0
}

# Desacoplarse de quien lo lanza (ver cabecera).
if ($isWindows5 -and -not $NoDetach) {
    $selfCommand = 'powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "' + $PSCommandPath + '" -NoDetach'
    try {
        $startup = New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly -Property @{ ShowWindow = [uint16]0 }
        $created = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{
            CommandLine = $selfCommand
            CurrentDirectory = $PSScriptRoot
            ProcessStartupInformation = $startup
        }
        if ($created.ReturnValue -eq 0) {
            Write-SupervisorLog ("Supervisor relanzado fuera del arbol del llamador: pid " + $created.ProcessId + ". Este sale.")
            exit 0
        }
        Write-SupervisorLog ("No pude desacoplar el supervisor (Win32_Process.Create=" + $created.ReturnValue + "); sigo en este proceso.") "WARN"
    } catch {
        Write-SupervisorLog ("No pude desacoplar el supervisor (" + $_.Exception.Message + "); sigo en este proceso.") "WARN"
    }
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

$parentPid = ""
try {
    $parentPid = (Get-CimInstance Win32_Process -Filter "ProcessId=$PID" -ErrorAction Stop).ParentProcessId
} catch { }
Write-SupervisorLog "Supervisor arrancado. Worker path=$script padre=$parentPid"

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

# Sin buffer en Python: lo que imprime el worker llega al archivo al momento.
$env:PYTHONUNBUFFERED = "1"
# stdout a archivo en UTF-8 (en Windows seria cp1252 y un emoji tumbaria el print).
$env:PYTHONIOENCODING = "utf-8"

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
        $workerPid = $null
        $started = Get-Date
        try {
            Invoke-LogRotation
            $marca = "==== " + [DateTimeOffset]::UtcNow.ToString("o") + " | supervisor " + $PID + " lanza el worker ===="
            Add-Content -LiteralPath $workerStdoutLog -Value $marca -Encoding UTF8
            Add-Content -LiteralPath $workerStderrLog -Value $marca -Encoding UTF8
            # La redireccion la hace el sistema, no PowerShell: nada pasa por el
            # supervisor y el archivo se escribe en vivo (python -u).
            if ($isWindows5) {
                $shellArgs = '/d /s /c ""' + $python + '" -u "' + $script + '" 1>>"' + $workerStdoutLog + '" 2>>"' + $workerStderrLog + '""'
                $p = Start-Process -FilePath "cmd.exe" -ArgumentList $shellArgs -WorkingDirectory $PSScriptRoot -WindowStyle Hidden -PassThru
            } else {
                # Solo para las pruebas en Linux/macOS con pwsh.
                $shellArgs = "-c `"exec '" + $python + "' -u '" + $script + "' 1>>'" + $workerStdoutLog + "' 2>>'" + $workerStderrLog + "'`""
                $p = Start-Process -FilePath "/bin/sh" -ArgumentList $shellArgs -WorkingDirectory $PSScriptRoot -PassThru
            }
            # Windows PowerShell 5.1: sin tocar Handle antes de que salga, ExitCode queda vacio.
            $null = $p.Handle
            $workerPid = $p.Id
            Write-SupervisorLog ("Worker lanzado pid=" + $workerPid + " (cmd) salida en " + $workerStdoutLog + " y " + $workerStderrLog)
            while (-not $p.WaitForExit(60000)) {
                ("{0} supervisor={1} worker={2}" -f ([DateTimeOffset]::UtcNow.ToString("o")), $PID, $workerPid) |
                    Out-File -FilePath $heartbeatFile -Encoding utf8
            }
            $p.WaitForExit()
            $exitCode = $p.ExitCode
        } catch {
            Write-SupervisorLog ("Supervisor capturo error lanzando o esperando al worker: " + $_.Exception.Message) "ERROR"
        }

        $runtime = [int]((Get-Date) - $started).TotalSeconds
        $exitText = "desconocido"
        if ($null -ne $exitCode) { $exitText = [string]$exitCode }
        Write-SupervisorLog ("Worker pid=" + $workerPid + " termino con codigo " + $exitText + " tras " + $runtime + " s") "WARN"
        try {
            [ordered]@{
                time = [DateTimeOffset]::UtcNow.ToString("o")
                worker_pid = $workerPid
                exit_code = $exitCode
                runtime_seconds = $runtime
                stdout = $workerStdoutLog
                stderr = $workerStderrLog
            } | ConvertTo-Json | Out-File -FilePath $lastExitFile -Encoding utf8
            if ($exitCode -ne 0) {
                # Solo lo que escribio este lanzamiento (desde su marca), ultimas 40 lineas.
                $lines = @(Get-Content -LiteralPath $workerStderrLog -Tail 400 -ErrorAction SilentlyContinue)
                $from = 0
                for ($i = $lines.Count - 1; $i -ge 0; $i--) {
                    if ([string]$lines[$i] -like "*lanza el worker ====") { $from = $i + 1; break }
                }
                $tail = @($lines | Select-Object -Skip $from | Select-Object -Last 40)
                $block = @("==== " + [DateTimeOffset]::UtcNow.ToString("o") + " | CAIDA worker pid=" + $workerPid + " codigo=" + $exitText + " tras " + $runtime + " s | ultimas lineas de " + $workerStderrLog) + $tail + @("")
                $block | Out-File -FilePath $crashLog -Encoding utf8 -Append
            }
        } catch {
            Write-SupervisorLog ("No pude escribir el registro de salida: " + $_.Exception.Message) "WARN"
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
} catch {
    Write-SupervisorLog ("Supervisor: error inesperado, sale: " + $_.Exception.Message + " " + $_.ScriptStackTrace) "ERROR"
} finally {
    Write-SupervisorLog "Supervisor detenido."
    try { $supervisorMutex.ReleaseMutex() } catch { }
}
exit 0
