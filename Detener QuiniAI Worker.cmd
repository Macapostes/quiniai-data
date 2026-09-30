@echo off
cd /d "%~dp0"
if not exist cache mkdir cache
echo %date% %time%> cache\stop_worker.flag
echo Orden de parada creada: %~dp0cache\stop_worker.flag
echo El worker se detiene en su siguiente espera (unos segundos; si esta en mitad de un ciclo, al acabarlo).
echo El supervisor no lo relanzara mientras exista ese archivo.
echo Para volver a arrancarlo: Iniciar QuiniAI Worker.cmd
pause
