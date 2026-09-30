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

## Comprobar el estado

- `logs\worker_supervisor.log`: arranques, códigos de salida, relanzamientos.
- `logs\worker_events.log`: ciclos (`cycle_completed`), esperas
  (`startup_skipped_recent_sync`, `cycle_sleep`) y la parada.
- ¿Hay supervisor vivo?

```powershell
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'run_worker\.ps1|snapshot_worker\.py' } |
    Select-Object ProcessId, CommandLine
```

Solo puede haber un supervisor a la vez: uno nuevo sale sin duplicar si ya hay
otro vivo.
