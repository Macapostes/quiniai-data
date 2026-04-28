@echo off
setlocal
cd /d "%~dp0"

echo Lanzando pasada manual de QuiniAI...
if exist "%~dp0.venv\Scripts\python.exe" (
  "%~dp0.venv\Scripts\python.exe" snapshot_worker.py --once --pretty
) else (
  where py >nul 2>nul
  if %errorlevel%==0 (
    py -3 snapshot_worker.py --once --pretty
  ) else (
    python snapshot_worker.py --once --pretty
  )
)

set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" (
  echo.
  echo La pasada manual termino con error %EXIT_CODE%.
  echo Revisa logs\worker_events.log o docs\monitor\status.json
  pause
)

endlocal
