@echo off
set "STATUS_PATH=C:\Users\mario\Desktop\Kinii\Estado\Estado QuiniAI.txt"
if not exist "%STATUS_PATH%" set "STATUS_PATH=C:\Users\mario\Documents\Codex\quiniai-data\output\ULTIMO_ESTADO_QUINIAI.txt"
notepad "%STATUS_PATH%"
