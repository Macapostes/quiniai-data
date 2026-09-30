"""El supervisor no guarda nada en memoria y deja rastro de cada salida y caída.

- run_worker.ps1 manda stdout/stderr del worker directo a archivos, registra el
  código de cada salida y apunta las caídas con el final de stderr.
- snapshot_worker.py registra en worker_events.log su arranque, su salida y
  cualquier excepción que lo tumbe (también en hilos).
"""

import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import snapshot_worker as worker

AQUI = Path(__file__).resolve().parent


class SupervisorEstaticoTests(unittest.TestCase):
    def setUp(self):
        self.ps1 = (AQUI / "run_worker.ps1").read_text(encoding="utf-8")

    def test_nada_de_salida_en_memoria(self):
        # Ni tuberias leidas por PowerShell ni Start-Process -Redirect* (en pwsh
        # de Linux pasa por la memoria del supervisor): redireccion del sistema.
        self.assertNotIn("ReadToEndAsync", self.ps1)
        self.assertNotIn("RedirectStandard", self.ps1)
        self.assertIn('1>>"', self.ps1)
        self.assertIn('2>>"', self.ps1)
        self.assertIn('$env:PYTHONUNBUFFERED = "1"', self.ps1)
        self.assertIn('" -u "', self.ps1)

    def test_codigo_de_salida_fiable_en_powershell_51(self):
        self.assertIn("$null = $p.Handle", self.ps1)

    def test_cada_salida_y_cada_caida_quedan_escritas(self):
        self.assertIn("termino con codigo", self.ps1)
        self.assertIn("worker_last_exit.json", self.ps1)
        self.assertIn("worker_crashes.log", self.ps1)

    def test_se_desacopla_del_llamador(self):
        self.assertTrue(self.ps1.lstrip().startswith("#"))
        self.assertIn("param([switch]$NoDetach)", self.ps1)
        self.assertIn("Win32_Process -MethodName Create", self.ps1)
        self.assertIn('-NoDetach', self.ps1)

    def test_sin_sintaxis_exclusiva_de_powershell_7(self):
        codigo = "\n".join(l for l in self.ps1.splitlines() if not l.lstrip().startswith("#"))
        for patron in (r"\?\?", r"&&", r"\|\|", r"-Parallel\b", r"\$IsWindows\b", r"\$IsLinux\b", r"\?\s*\$\w+\s*:"):
            self.assertIsNone(re.search(patron, codigo), f"sintaxis de PowerShell 7: {patron}")


@unittest.skipUnless(shutil.which("pwsh") and os.name != "nt", "necesita pwsh en Linux/macOS")
class SupervisorRealConPwshTests(unittest.TestCase):
    """Ejecuta el run_worker.ps1 real con un worker falso (script de bash)."""

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, True)
        shutil.copy(AQUI / "run_worker.ps1", self.dir / "run_worker.ps1")
        (self.dir / "cache").mkdir()
        falso = self.dir / ".venv" / "Scripts" / "python.exe"
        falso.parent.mkdir(parents=True)
        falso.write_text(
            "#!/bin/bash\n"
            f'D="{self.dir}"\n'
            'n=$(cat "$D/cuenta" 2>/dev/null || echo 0); n=$((n+1)); echo $n > "$D/cuenta"\n'
            'for i in $(seq 1 2000); do echo "salida $n linea $i"; done\n'
            'echo "Traceback falso de la vuelta $n" >&2\n'
            'case $n in 1) exit 0;; 2) exit 3;; *) touch "$D/cache/stop_worker.flag"; exit 0;; esac\n',
            encoding="utf-8",
        )
        falso.chmod(0o755)

    def test_relanza_registra_y_para_con_la_orden(self):
        entorno = dict(os.environ, QUINIAI_WORKER_CLEAN_EXIT_DELAY="1", QUINIAI_WORKER_RESTART_DELAY="1")
        r = subprocess.run(
            ["pwsh", "-NoProfile", "-File", str(self.dir / "run_worker.ps1")],
            cwd=self.dir, env=entorno, capture_output=True, text=True, timeout=120,
        )
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        logs = self.dir / "logs"
        sup = (logs / "worker_supervisor.log").read_text(encoding="utf-8-sig")
        self.assertIn("termino con codigo 0", sup)
        self.assertIn("termino con codigo 3", sup)
        self.assertIn("Parada solicitada", sup)
        self.assertIn("Supervisor detenido.", sup)
        salida = (logs / "worker_stdout.log").read_text(encoding="utf-8-sig")
        self.assertEqual(salida.count("lanza el worker"), 3, "una marca por lanzamiento")
        self.assertIn("salida 1 linea 2000", salida)
        self.assertIn("salida 3 linea 2000", salida)
        caidas = (logs / "worker_crashes.log").read_text(encoding="utf-8-sig")
        self.assertIn("codigo=3", caidas)
        self.assertIn("Traceback falso de la vuelta 2", caidas)
        self.assertNotIn("vuelta 1", caidas, "solo el stderr del lanzamiento que cayo")
        ultima = json.loads((logs / "worker_last_exit.json").read_text(encoding="utf-8-sig"))
        self.assertEqual(ultima["exit_code"], 0)


class RastroDelProcesoTests(unittest.TestCase):
    def setUp(self):
        self.eventos = []
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, True)
        hook_original = threading.excepthook
        self.addCleanup(setattr, threading, "excepthook", hook_original)
        self.addCleanup(lambda: worker._FAULTHANDLER_FILE and worker._FAULTHANDLER_FILE.close())
        self.addCleanup(worker.faulthandler.disable)
        for p in (
            patch.object(worker, "_log_cycle_event", lambda nivel, msg, **ctx: self.eventos.append((nivel, msg, ctx))),
            patch.object(worker, "LOG_DIR", self.tmp),
            patch.object(worker.atexit, "register", lambda fn: self.eventos.append(("atexit", fn.__name__, {}))),
            patch.object(worker, "_acquire_worker_lock", lambda: None),
            patch.object(worker.sys, "argv", ["snapshot_worker.py"]),
        ):
            p.start()
            self.addCleanup(p.stop)

    def _mensajes(self):
        return [m for _, m, _ in self.eventos]

    def test_una_excepcion_que_tumba_el_worker_queda_en_el_log(self):
        with patch.object(worker, "run_forever", side_effect=MemoryError("sin memoria")):
            with self.assertRaises(MemoryError):
                worker.main()
        self.assertIn("worker_process_started", self._mensajes())
        self.assertIn("_registrar_salida_del_proceso", self._mensajes())
        nivel, _, ctx = next(e for e in self.eventos if e[1] == "worker_crashed")
        self.assertEqual(nivel, "error")
        self.assertIn("MemoryError", ctx["traceback"])
        self.assertTrue((self.tmp / "worker_faulthandler.log").exists())

    def test_una_salida_pedida_queda_con_su_codigo(self):
        with patch.object(worker, "run_forever", side_effect=SystemExit(0)):
            with self.assertRaises(SystemExit):
                worker.main()
        _, _, ctx = next(e for e in self.eventos if e[1] == "worker_process_exit_requested")
        self.assertEqual(ctx["code"], "0")

    def test_una_excepcion_en_un_hilo_queda_en_el_log(self):
        with patch.object(worker, "run_forever", lambda: None):
            worker.main()
        hilo = threading.Thread(target=lambda: 1 / 0, name="hilo-test")
        hilo.start()
        hilo.join()
        _, _, ctx = next(e for e in self.eventos if e[1] == "thread_exception")
        self.assertEqual(ctx["thread"], "hilo-test")
        self.assertIn("ZeroDivisionError", ctx["traceback"])


if __name__ == "__main__":
    unittest.main()
