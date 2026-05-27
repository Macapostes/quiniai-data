@echo off
set "APP_DIR=%~dp0"
set "STATUS_PATH=%APP_DIR%Estado\Estado QuiniAI.txt"
if not exist "%STATUS_PATH%" set "STATUS_PATH=%APP_DIR%output\ULTIMO_ESTADO_QUINIAI.txt"
notepad "%STATUS_PATH%"
