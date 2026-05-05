@echo off
set "SCRIPT_PATH=%~dp0install_worker_task.ps1"
if not exist "%SCRIPT_PATH%" set "SCRIPT_PATH=C:\Users\mario\Documents\Codex\quiniai-data\install_worker_task.ps1"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_PATH%"
pause
