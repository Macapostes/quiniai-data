# Detener el worker a propósito

El supervisor (`run_worker.ps1`) relanza **siempre** `snapshot_worker.py`,
también cuando termina con código 0. Por eso, cerrar o matar el proceso de
Python ya no sirve para pararlo: a los 20–60 s vuelve a arrancar. La única forma
de pararlo a propósito es la **orden de parada**: el archivo
`cache\stop_worker.flag`.

## Parar

Opción A: doble clic en `Detener QuiniAI Worker.cmd`.

Opción B: en PowerShell, desde la carpeta del worker:

```powershell
New-Item -ItemType File -Force cache\stop_worker.flag | Out-Null
```

Qué pasa después:

- El worker lo mira cada 5 s mientras espera al siguiente ciclo y sale limpio
  (evento `worker_stop_requested` en `logs\worker_events.log`). Si está en mitad
  de un ciclo, lo termina (sube su snapshot) y sale al acabar.
- El supervisor ve la orden, no relanza nada y sale (`Parada solicitada` y
  `Supervisor detenido.` en `logs\worker_supervisor.log`).
- Mientras el archivo exista, cualquier supervisor que se arranque sale sin
  lanzar el worker.

Para pararlo **ya**, sin esperar a que acabe el ciclo en curso, crea primero la
orden y después cierra el proceso:

```powershell
New-Item -ItemType File -Force cache\stop_worker.flag | Out-Null
Get-CimInstance Win32_Process |
    Where-Object { $_.CommandLine -match 'snapshot_worker\.py' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
```

El monitor de salud (`watch_quiniai_worker.py`, en la carpeta
`C:\Users\mario\Desktop\Bot Trading`) no forma parte de este repositorio. Lo
arranca `run_worker.ps1` al iniciarse. Si también quieres pararlo:

```powershell
Get-CimInstance Win32_Process |
    Where-Object { $_.CommandLine -match 'watch_quiniai_worker\.py' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
```

`Ctrl+C` solo sirve si has lanzado `run_worker.ps1` a mano en una consola. Para
el supervisor, pero el Python que estuviera corriendo sigue vivo. Usa la orden
de parada.

## Volver a arrancar

Doble clic en `Iniciar QuiniAI Worker.cmd`. Borra `cache\stop_worker.flag`, hace
una pasada inmediata (`--once`) y deja el supervisor en segundo plano. Si la
pasada falla, el supervisor se arranca igual y el worker lo reintenta.

A mano, en PowerShell:

```powershell
Remove-Item cache\stop_worker.flag -ErrorAction SilentlyContinue
powershell -NoProfile -ExecutionPolicy Bypass -File .\run_worker.ps1
```

`run_worker.ps1` se relanza a sí mismo fuera del proceso que lo llama (con
`Win32_Process.Create`) y sale al momento. Así, cerrar la consola, el asistente
o el programa que lo lanzó no se lleva por delante al supervisor ni al worker.
Para depurar en primer plano, en esa misma consola: `.\run_worker.ps1 -NoDetach`.

## Comprobar el estado

- `logs\worker_supervisor.log`: arranques (con el PID del padre), cada
  lanzamiento del worker, **el código de cada salida** y la duración, los
  relanzamientos y la parada.
- `logs\worker_stdout.log` y `logs\worker_stderr.log`: la salida del worker, en
  vivo, con una marca `==== ... lanza el worker ====` por lanzamiento. Se rotan
  a `.1` al pasar de 10 MB.
- `logs\worker_crashes.log`: una entrada por cada salida con código distinto de 0,
  con las últimas líneas de stderr de ese lanzamiento.
- `logs\worker_last_exit.json`: la última salida (PID, código, duración).
- `logs\worker_supervisor_heartbeat.txt`: se reescribe cada minuto mientras el
  worker corre. Si es viejo y no hay supervisor, alguien lo mató desde fuera.
- `logs\worker_events.log`: ciclos (`cycle_started`, `cycle_completed`,
  `cycle_failed`), esperas (`startup_skipped_recent_sync`, `cycle_sleep`), la
  parada, y también `worker_process_started` y `worker_process_exit` por proceso,
  más `worker_crashed` o `thread_exception` con traceback. Si hay un
  `worker_process_started` sin su `worker_process_exit`, al proceso lo
  terminaron desde fuera.
- `logs\worker_faulthandler.log`: volcado si Python cae por un fallo nativo.
- ¿Hay supervisor vivo?

```powershell
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'run_worker\.ps1|snapshot_worker\.py' } |
    Select-Object ProcessId, ParentProcessId, CommandLine
```

Solo puede haber un supervisor a la vez: uno nuevo sale sin duplicar si ya hay
otro vivo.
