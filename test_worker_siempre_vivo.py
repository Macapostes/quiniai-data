"""El worker no se queda muerto: la sincronizacion reciente le hace dormir (no
salir), solo una orden de parada a proposito lo detiene, y el supervisor de
Windows lo relanza tambien tras una salida limpia (codigo 0)."""

import re
import shutil
import subprocess
import tempfile
import time as _time
import unittest
from pathlib import Path
from unittest.mock import patch

import snapshot_worker as worker

AQUI = Path(__file__).resolve().parent
_SNAPSHOT = {"coverage": {"monitored_matches": 3, "quiniela_current_jornada": 11}, "generated_at": "x"}


class _Reloj:
    """time.time/time.sleep falsos: dormir avanza el reloj sin esperar de verdad."""

    def __init__(self):
        self.ahora = 1_800_000_000.0
        self.dormido = 0.0
        self.al_dormir = None

    def time(self):
        return self.ahora

    def sleep(self, segundos):
        self.ahora += segundos
        self.dormido += segundos
        if self.al_dormir:
            self.al_dormir(self)


class RunForeverTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.stop = self.tmp / "stop_worker.flag"
        self.manual = self.tmp / "manual_refresh.flag"
        self.reloj = _Reloj()
        self.eventos = []
        self.ciclos = []
        parches = [
            patch.object(worker, "STOP_FLAG_PATH", self.stop),
            patch.object(worker, "MANUAL_REFRESH_FLAG_PATH", self.manual),
            patch.object(worker, "POLL_SECONDS", 21600),
            patch.object(worker.time, "time", self.reloj.time),
            patch.object(worker.time, "sleep", self.reloj.sleep),
            patch.object(worker, "_log_cycle_event", lambda nivel, msg, **ctx: self.eventos.append(msg)),
            patch.object(worker, "run_once", self._ciclo),
        ]
        for p in parches:
            p.start()
            self.addCleanup(p.stop)
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def _ciclo(self, print_summary=False):
        self.ciclos.append(self.reloj.dormido)
        # Tras el ciclo se pide parar, para que run_forever termine el test.
        self.stop.write_text("test", encoding="utf-8")
        return _SNAPSHOT

    def _ultima_sync_hace(self, segundos):
        return patch.object(worker, "_load_last_sync_ts", lambda: self.reloj.ahora - segundos)

    def test_sincronizacion_reciente_duerme_hasta_el_ciclo_debido_en_vez_de_salir(self):
        with self._ultima_sync_hace(3600):
            worker.run_forever()  # no lanza SystemExit ni vuelve antes de tiempo
        self.assertIn("startup_skipped_recent_sync", self.eventos)
        self.assertEqual(len(self.ciclos), 1, "tras la espera hace su ciclo")
        self.assertGreaterEqual(self.ciclos[0], 21600 - 3600 - 5)
        self.assertLessEqual(self.ciclos[0], 21600 - 3600 + 5)
        self.assertEqual(self.eventos[-1], "worker_stop_requested")

    def test_sin_sincronizacion_reciente_hace_el_ciclo_enseguida(self):
        with self._ultima_sync_hace(30000):
            worker.run_forever()
        self.assertNotIn("startup_skipped_recent_sync", self.eventos)
        self.assertEqual(self.ciclos, [0.0])

    def test_la_orden_de_parada_corta_la_espera_inicial_sin_ciclo(self):
        def _parar_a_la_hora(reloj):
            if reloj.dormido >= 3600:
                self.stop.write_text("x", encoding="utf-8")

        self.reloj.al_dormir = _parar_a_la_hora
        with self._ultima_sync_hace(600):
            worker.run_forever()
        self.assertEqual(self.ciclos, [])
        self.assertLess(self.reloj.dormido, 3600 + 10)
        self.assertTrue(self.stop.exists(), "la orden se queda para que el supervisor tampoco relance")

    def test_la_orden_de_parada_corta_la_espera_entre_ciclos(self):
        with self._ultima_sync_hace(30000):
            worker.run_forever()
        self.assertEqual(len(self.ciclos), 1)
        self.assertLessEqual(self.reloj.dormido, 5, "sale en el primer paso de la espera")

    def test_el_refresco_manual_sigue_adelantando_el_ciclo(self):
        def _refresco(reloj):
            if reloj.dormido >= 60 and not self.ciclos:
                self.manual.write_text("x", encoding="utf-8")

        self.reloj.al_dormir = _refresco
        with self._ultima_sync_hace(600):
            worker.run_forever()
        self.assertIn("manual_refresh_triggered", self.eventos)
        self.assertEqual(len(self.ciclos), 1)
        self.assertLess(self.ciclos[0], 120)

    def test_sin_orden_no_hay_parada(self):
        self.assertFalse(worker._stop_requested())
        self.stop.write_text("x", encoding="utf-8")
        self.assertTrue(worker._stop_requested())


class SupervisorWindowsTests(unittest.TestCase):
    """run_worker.ps1 / launch_worker.ps1: regresiones del supervisor."""

    def setUp(self):
        self.run_worker = (AQUI / "run_worker.ps1").read_text(encoding="utf-8")
        self.launch = (AQUI / "launch_worker.ps1").read_text(encoding="utf-8")

    def test_una_salida_limpia_no_mata_la_supervision(self):
        self.assertIsNone(
            re.search(r"\$exitCode\s+-eq\s+0\)\s*\{\s*exit\s+0", self.run_worker),
            "el codigo 0 del worker no puede terminar el supervisor",
        )
        self.assertIn("$cleanExitDelaySeconds", self.run_worker)

    def test_solo_la_orden_de_parada_termina_el_bucle(self):
        self.assertIn('cache\\stop_worker.flag', self.run_worker)
        bucle = self.run_worker.split("while ($true) {", 1)[1]
        salidas = re.findall(r"^\s*(exit\b.*|break\b.*)$", bucle.split("} finally {", 1)[0], re.M)
        self.assertEqual(salidas, ["break"], f"unica salida del bucle: la orden de parada ({salidas})")

    def test_un_worker_ajeno_vivo_no_hace_salir_al_supervisor(self):
        tramo = self.run_worker.split("if ($alreadyRunning) {", 1)[1].split("$lastBusyPid = $null", 1)[0]
        self.assertNotIn("exit", tramo)
        self.assertIn("continue", tramo)

    def test_el_lanzador_retira_la_parada_y_arranca_el_supervisor_siempre(self):
        self.assertIn("Remove-Item -Path $stopFlag", self.launch)
        self.assertNotIn("exit $exitCode", self.launch)
        self.assertEqual(self.launch.count("Start-WorkerSupervisor"), 3, "definicion + las dos ramas")

    @unittest.skipUnless(shutil.which("pwsh"), "pwsh no instalado")
    def test_sintaxis_powershell(self):
        for nombre in ("run_worker.ps1", "launch_worker.ps1", "health_check.ps1"):
            ruta = str(AQUI / nombre).replace("'", "''")
            orden = (
                "$e=$null; [void][System.Management.Automation.Language.Parser]::ParseFile("
                f"'{ruta}', [ref]$null, [ref]$e); if ($e) {{ $e | % {{ $_.ToString() }}; exit 1 }}"
            )
            r = subprocess.run(["pwsh", "-NoProfile", "-Command", orden], capture_output=True, text=True)
            self.assertEqual(r.returncode, 0, f"{nombre}: {r.stdout}{r.stderr}")


if __name__ == "__main__":
    unittest.main()
