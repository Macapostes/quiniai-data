@echo off
set "APP_DIR=%~dp0"
set "PANEL_PATH=%APP_DIR%Estado\Panel QuiniAI.html"
if not exist "%PANEL_PATH%" set "PANEL_PATH=%APP_DIR%output\PANEL_QUINIAI.html"
start "" "%PANEL_PATH%"
