@echo off
setlocal
cd /d "%~dp0"

set "PANEL=%~dp0docs\monitor\index.html"
if not exist "%PANEL%" (
  echo No se encontro %PANEL%
  pause
  exit /b 1
)

start "" "%PANEL%"
endlocal
