@echo off
setlocal
cd /d "%~dp0"

set "STATUS_JSON=%~dp0docs\monitor\status.json"
set "STATUS_TXT=%~dp0output\ULTIMO_ESTADO_QUINIAI.txt"

if exist "%STATUS_JSON%" (
  start "" "%STATUS_JSON%"
  endlocal
  exit /b 0
)

if exist "%STATUS_TXT%" (
  start "" "%STATUS_TXT%"
  endlocal
  exit /b 0
)

echo Todavia no hay ficheros de estado generados por el worker.
pause
endlocal
