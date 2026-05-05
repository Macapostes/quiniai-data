$ErrorActionPreference = "Continue"

Set-Location $PSScriptRoot

$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$script = Join-Path $PSScriptRoot "snapshot_worker.py"
$workerStdoutLog = Join-Path $PSScriptRoot "logs\\worker_stdout.log"
$supervisorLog = Join-Path $PSScriptRoot "logs\\worker_supervisor.log"
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
        & $python -u $script *>> $workerStdoutLog
        $exitCode = $LASTEXITCODE
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
