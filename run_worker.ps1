$ErrorActionPreference = "Continue"

Set-Location $PSScriptRoot

$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$script = Join-Path $PSScriptRoot "snapshot_worker.py"
$workerStdoutLog = Join-Path $PSScriptRoot "logs\\worker_stdout.log"
$supervisorLog = Join-Path $PSScriptRoot "logs\\worker_supervisor.log"
$healthMonitorPython = "C:\Users\mario\Desktop\Bot Trading\.venv\Scripts\python.exe"
$healthMonitorScript = "C:\Users\mario\Desktop\Bot Trading\tools\desktop_launchers\watch_quiniai_worker.py"
$restartDelaySeconds = 20

function Write-SupervisorLog([string]$message, [string]$level = "INFO") {
    $line = "{0} | {1} | {2}" -f ([DateTimeOffset]::UtcNow.ToString("o")), $level, $message
    $line | Out-File -FilePath $supervisorLog -Encoding utf8 -Append
}

if (-not (Test-Path $python)) {
    Write-SupervisorLog "No existe el entorno virtual en $python" "ERROR"
    throw "No existe el entorno virtual en $python"
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

while ($true) {
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
        Write-SupervisorLog ("Detectado worker ya vivo con PID " + $alreadyRunning.ProcessId + ". El supervisor sale sin duplicar proceso.")
        exit 0
    }

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
        if ($exitCode -eq 0) {
            exit 0
        }
    } catch {
        Write-SupervisorLog ("Supervisor capturo error: " + $_.Exception.Message) "ERROR"
    }

    Write-SupervisorLog ("Reintento en " + $restartDelaySeconds + " segundos") "WARN"
    Start-Sleep -Seconds $restartDelaySeconds
}
