@echo off
setlocal
cd /d "%~dp0"

echo Iniciando QuiniAI Worker...
if exist "%~dp0.venv\Scripts\python.exe" (
  "%~dp0.venv\Scripts\python.exe" snapshot_worker.py --pretty
) else (
  where py >nul 2>nul
  if %errorlevel%==0 (
    py -3 snapshot_worker.py --pretty
  ) else (
    python snapshot_worker.py --pretty
  )
)

set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" (
  echo.
  echo El worker termino con error %EXIT_CODE%.
  echo Revisa requirements, .env y logs\worker_events.log
  pause
)

endlocal
