@echo off
set "SCRIPT_PATH=%~dp0launch_worker.ps1"
if not exist "%SCRIPT_PATH%" set "SCRIPT_PATH=C:\Users\mario\Documents\Codex\quiniai-data\launch_worker.ps1"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_PATH%"
