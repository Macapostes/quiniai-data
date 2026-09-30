"""TheSportsDB con educacion: ritmo, memoria, cache, 429 y circuito.

El 30-sep-2026 la primera pasada en Windows tardo 42 minutos: cientos de 429 y
la misma consulta repetida decenas de veces. Todo se prueba con HTTP falso y
reloj falso: ni red ni esperas de verdad.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import requests  # noqa: E402

import snapshot_worker as sw  # noqa: E402
from sportsdb_cliente import (  # noqa: E402
    ClienteSportsDB,
    SportsDBCircuitoAbierto,
    SportsDBError,
    SportsDBNoJSON,
)

BASE = "https://www.thesportsdb.com/api/v1/json/123"


class Respuesta:
    def __init__(self, estado=200, cuerpo=None, texto=None, cabeceras=None):
        self.status_code = estado
        self.text = texto if texto is not None else json.dumps(cuerpo if cuerpo is not None else {})
        self.headers = cabeceras or {}
        self.encoding = "utf-8"


class Entorno:
    """Reloj falso + HTTP falso que responde lo que se le programe."""

    def __init__(self, respuestas=None):
        self.t = 1000.0
        self.pared = 1_800_000_000.0
        self.esperas = []
        self.peticiones = []  # (monotonic, url, params)
        self.respuestas = list(respuestas or [])
        self.por_defecto = Respuesta(200, {"teams": [{"idTeam": "1"}]})
        self.fallos = 0

    def reloj(self):
        return self.t

    def ahora(self):
        return self.pared + self.t

    def dormir(self, s):
        self.esperas.append(s)
        self.t += s

    def get(self, url, params=None, headers=None, timeout=None):
        self.peticiones.append((self.t, url, dict(params or {})))
        if self.respuestas:
            r = self.respuestas.pop(0)
            return r(url, params) if callable(r) else r
        return self.por_defecto

    def marcar(self):
        self.fallos += 1

    def cliente(self, **kw):
        base = dict(
            intervalo=2.2,
            http_get=self.get,
            dormir=self.dormir,
            reloj=self.reloj,
            ahora=self.ahora,
            al_fallar=self.marcar,
            avisar=lambda _t: None,
        )
        base.update(kw)
        return ClienteSportsDB(**base)


class RitmoTests(unittest.TestCase):
    def test_nunca_mas_de_una_peticion_cada_intervalo(self):
        e = Entorno()
        c = e.cliente()
        for i in range(10):
            c.get_json(f"{BASE}/searchteams.php", {"t": f"Equipo {i}"})
        tiempos = [p[0] for p in e.peticiones]
        huecos = [b - a for a, b in zip(tiempos, tiempos[1:])]
        self.assertEqual(len(e.peticiones), 10)
        self.assertTrue(all(h >= 2.2 - 1e-9 for h in huecos), huecos)

    def test_por_debajo_del_limite_documentado(self):
        """30/min por IP con la clave gratuita: el worker no puede pasar de ahi."""
        self.assertGreaterEqual(sw._SPORTSDB_PAUSA_SEGUNDOS, 2.0)
        self.assertLessEqual(60.0 / sw._SPORTSDB_PAUSA_SEGUNDOS, 30)

    def test_el_ritmo_se_comparte_entre_hilos(self):
        import threading

        e = Entorno()
        lock = threading.Lock()
        reloj_real = {"t": 0.0}

        def dormir(s):
            with lock:
                e.esperas.append(s)

        c = e.cliente(dormir=dormir, reloj=lambda: reloj_real["t"])
        hilos = [
            threading.Thread(target=c.get_json, args=(f"{BASE}/eventslast.php", {"id": str(i)}))
            for i in range(5)
        ]
        for h in hilos:
            h.start()
        for h in hilos:
            h.join()
        # Con el reloj parado, cada hilo reserva su turno: 0, 2.2, 4.4, 6.6, 8.8
        self.assertEqual(sorted(round(x, 1) for x in e.esperas), [2.2, 4.4, 6.6, 8.8])


class MemoriaDeLaPasadaTests(unittest.TestCase):
    def test_la_misma_url_se_pide_una_vez(self):
        e = Entorno()
        c = e.cliente()
        for _ in range(12):
            c.get_json(f"{BASE}/searchteams.php", {"t": "Alaves"})
        self.assertEqual(len(e.peticiones), 1)
        self.assertEqual(c.stats["memo"], 11)

    def test_devuelve_copias_que_no_se_contaminan(self):
        e = Entorno()
        c = e.cliente()
        a = c.get_json(f"{BASE}/searchteams.php", {"t": "X"})
        a["teams"].clear()
        b = c.get_json(f"{BASE}/searchteams.php", {"t": "X"})
        self.assertEqual(b["teams"], [{"idTeam": "1"}])

    def test_el_orden_de_los_parametros_no_importa(self):
        e = Entorno()
        c = e.cliente()
        c.get_json(f"{BASE}/lookuptable.php", {"l": "4335", "s": "2026-2027"})
        c.get_json(f"{BASE}/lookuptable.php", {"s": "2026-2027", "l": "4335"})
        self.assertEqual(len(e.peticiones), 1)

    def test_un_fallo_tampoco_se_repite(self):
        """Moldova, Faroe Islands... un 429 se repetia decenas de veces."""
        e = Entorno([Respuesta(503)])
        c = e.cliente()
        for _ in range(8):
            with self.assertRaises(SportsDBError):
                c.get_json(f"{BASE}/searchteams.php", {"t": "Moldova"})
        self.assertEqual(len(e.peticiones), 1)
        self.assertEqual(e.fallos, 1, "el 5xx cuenta para la auditoria, una vez")

    def test_el_fallo_repetido_va_marcado_para_no_llenar_el_log(self):
        e = Entorno([Respuesta(503)])
        c = e.cliente()
        with self.assertRaises(SportsDBError) as primero:
            c.get_json(f"{BASE}/searchteams.php", {"t": "Moldova"})
        with self.assertRaises(SportsDBError) as segundo:
            c.get_json(f"{BASE}/searchteams.php", {"t": "Moldova"})
        self.assertFalse(primero.exception.repetido)
        self.assertTrue(segundo.exception.repetido)

    def test_nueva_pasada_vuelve_a_intentar(self):
        e = Entorno([Respuesta(503)])
        c = e.cliente()
        with self.assertRaises(SportsDBError):
            c.get_json(f"{BASE}/searchteams.php", {"t": "Moldova"})
        c.nueva_pasada()
        self.assertTrue(c.get_json(f"{BASE}/searchteams.php", {"t": "Moldova"}))
        self.assertEqual(len(e.peticiones), 2)


class CacheEnDiscoTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.ruta = Path(self.dir.name) / "http.json"

    def tearDown(self):
        self.dir.cleanup()

    def _pasada(self, e, **kw):
        c = e.cliente(ruta_cache=self.ruta, **kw)
        return c

    def test_se_reutiliza_entre_pasadas(self):
        e = Entorno([Respuesta(200, {"table": [{"strTeam": "A"}]})])
        c = self._pasada(e)
        c.get_json(f"{BASE}/lookuptable.php", {"l": "4335", "s": "2026-2027"})
        c.guardar()
        e.t += 3600  # una hora despues, proceso nuevo
        c2 = self._pasada(e)
        datos = c2.get_json(f"{BASE}/lookuptable.php", {"l": "4335", "s": "2026-2027"})
        self.assertEqual(datos["table"][0]["strTeam"], "A")
        self.assertEqual(len(e.peticiones), 1)
        self.assertEqual(c2.stats["disco"], 1)

    def test_la_tabla_caduca_en_horas(self):
        e = Entorno()
        c = self._pasada(e)
        c.get_json(f"{BASE}/lookuptable.php", {"l": "4335", "s": "2026-2027"})
        c.guardar()
        e.t += 3 * 3600 + 1
        c2 = self._pasada(e)
        c2.get_json(f"{BASE}/lookuptable.php", {"l": "4335", "s": "2026-2027"})
        self.assertEqual(len(e.peticiones), 2)

    def test_el_equipo_dura_dias(self):
        e = Entorno()
        c = self._pasada(e)
        c.get_json(f"{BASE}/searchteams.php", {"t": "Alaves"})
        c.guardar()
        e.t += 2 * 24 * 3600
        c2 = self._pasada(e)
        c2.get_json(f"{BASE}/searchteams.php", {"t": "Alaves"})
        self.assertEqual(len(e.peticiones), 1)
        e.t += 2 * 24 * 3600
        c3 = self._pasada(e)
        c3.get_json(f"{BASE}/searchteams.php", {"t": "Alaves"})
        self.assertEqual(len(e.peticiones), 2)

    def test_la_respuesta_vacia_dura_menos(self):
        e = Entorno([Respuesta(200, {"teams": None})])
        c = self._pasada(e)
        c.get_json(f"{BASE}/searchteams.php", {"t": "Nadie"})
        c.guardar()
        e.t += 25 * 3600  # un equipo con ficha duraria 3 dias
        c2 = self._pasada(e)
        c2.get_json(f"{BASE}/searchteams.php", {"t": "Nadie"})
        self.assertEqual(len(e.peticiones), 2)

    def test_una_tabla_vacia_se_vuelve_a_pedir_en_horas(self):
        e = Entorno([Respuesta(200, {"table": None})])
        c = self._pasada(e)
        c.get_json(f"{BASE}/lookuptable.php", {"l": "4358", "s": "2026-2027"})
        c.guardar()
        e.t += 3 * 3600 + 1
        c2 = self._pasada(e)
        c2.get_json(f"{BASE}/lookuptable.php", {"l": "4358", "s": "2026-2027"})
        self.assertEqual(len(e.peticiones), 2)

    def test_el_h2h_vacio_no_se_repite_en_la_siguiente_pasada(self):
        """Pasadas cada 6 h: 8 consultas por partido casi siempre vacias."""
        e = Entorno([Respuesta(200, {"event": None})])
        c = self._pasada(e)
        c.get_json(f"{BASE}/searchevents.php", {"e": "Moldova vs Italy"})
        c.guardar()
        e.t += 6 * 3600
        c2 = self._pasada(e)
        c2.get_json(f"{BASE}/searchevents.php", {"e": "Moldova vs Italy"})
        self.assertEqual(len(e.peticiones), 1)

    def test_los_fallos_no_se_guardan_en_disco(self):
        e = Entorno([Respuesta(429), Respuesta(429), Respuesta(429), Respuesta(200, texto="<html>")])
        c = self._pasada(e)
        with self.assertRaises(SportsDBError):
            c.get_json(f"{BASE}/eventslast.php", {"id": "1"})
        c.nueva_pasada()
        with self.assertRaises(SportsDBNoJSON):
            c.get_json(f"{BASE}/eventslast.php", {"id": "2"})
        c.guardar()
        self.assertFalse(self.ruta.exists() and json.loads(self.ruta.read_text()))

    def test_la_clave_de_api_no_se_escribe(self):
        e = Entorno()
        c = self._pasada(e)
        c.get_json("https://www.thesportsdb.com/api/v1/json/SECRETA/searchteams.php", {"t": "X"})
        c.guardar()
        self.assertNotIn("SECRETA", self.ruta.read_text())

    def test_las_caducidades_no_empeoran_la_frescura(self):
        """Nunca mas largas que la cache del llamador: el dato no llega mas viejo."""
        from sportsdb_cliente import TTL_POR_ENDPOINT as T

        self.assertLessEqual(T["lookuptable.php"], sw.HISTORY_CACHE_TTL_SECONDS)
        self.assertLessEqual(T["eventslast.php"], sw.HISTORY_CACHE_TTL_SECONDS)
        self.assertLessEqual(T["eventsseason.php"], sw.HISTORY_CACHE_TTL_SECONDS)
        self.assertLessEqual(T["searchevents.php"], sw.HISTORY_CACHE_TTL_SECONDS)
        self.assertLessEqual(T["eventsnext.php"], 3 * 3600)
        self.assertLessEqual(T["eventsround.php"], 12 * 3600)
        self.assertLessEqual(T["lookup_all_players.php"], sw._PLANTILLA_TTL_SEGUNDOS)
        self.assertLessEqual(T["lookuptable.php"], 6 * 3600, "tabla: unas pocas horas")
        self.assertLessEqual(T["eventslast.php"], 6 * 3600, "racha: unas pocas horas")


class LimiteYCircuitoTests(unittest.TestCase):
    def test_respeta_retry_after(self):
        e = Entorno([Respuesta(429, cabeceras={"Retry-After": "7"}), Respuesta(200, {"table": []})])
        c = e.cliente(intervalo=0)
        c.get_json(f"{BASE}/lookuptable.php", {"l": "1", "s": "2026"})
        self.assertEqual(len(e.peticiones), 2)
        self.assertAlmostEqual(e.peticiones[1][0] - e.peticiones[0][0], 7.0)

    def test_retry_after_desorbitado_tiene_tope(self):
        e = Entorno([Respuesta(429, cabeceras={"Retry-After": "3600"}), Respuesta(200, {})])
        c = e.cliente(intervalo=0)
        c.get_json(f"{BASE}/lookuptable.php", {"l": "1", "s": "2026"})
        self.assertLessEqual(e.peticiones[1][0] - e.peticiones[0][0], 60.0)

    def test_sin_retry_after_espera_exponencial_acotada(self):
        e = Entorno([Respuesta(429), Respuesta(429), Respuesta(200, {})])
        c = e.cliente(intervalo=0, umbral_circuito=5)
        c.get_json(f"{BASE}/eventslast.php", {"id": "1"})
        t = [p[0] for p in e.peticiones]
        self.assertEqual([round(b - a) for a, b in zip(t, t[1:])], [20, 40])

    def test_tras_n_429_seguidos_se_abre_el_circuito(self):
        e = Entorno([Respuesta(429)] * 10)
        c = e.cliente(intervalo=0)
        with self.assertRaises(requests.exceptions.HTTPError):
            c.get_json(f"{BASE}/searchteams.php", {"t": "Alaves"})
        self.assertTrue(c.circuito_abierto)
        self.assertEqual(len(e.peticiones), 3)
        # Todo lo demas de la pasada ya no llega al proveedor.
        for i in range(20):
            with self.assertRaises(SportsDBCircuitoAbierto):
                c.get_json(f"{BASE}/eventslast.php", {"id": str(i)})
        self.assertEqual(len(e.peticiones), 3)
        self.assertEqual(c.stats["saltadas_circuito"], 20)
        # Y la auditoria sabe que el proveedor no atiende.
        self.assertGreaterEqual(e.fallos, sw.SPORTSDB_FALLOS_PARA_DEGRADADO)

    def test_con_circuito_abierto_se_sirve_el_disco(self):
        with tempfile.TemporaryDirectory() as d:
            ruta = Path(d) / "c.json"
            e = Entorno([Respuesta(200, {"results": [{"idEvent": "9"}]})])
            c = e.cliente(ruta_cache=ruta, intervalo=0)
            c.get_json(f"{BASE}/eventslast.php", {"id": "7"})
            c.circuito_abierto = True
            c.nueva_pasada()
            c.circuito_abierto = True
            self.assertEqual(c.get_json(f"{BASE}/eventslast.php", {"id": "7"})["results"][0]["idEvent"], "9")

    def test_un_acierto_reinicia_la_cuenta(self):
        e = Entorno([Respuesta(429), Respuesta(200, {}), Respuesta(429), Respuesta(200, {}), Respuesta(429), Respuesta(200, {})])
        c = e.cliente(intervalo=0)
        for i in range(3):
            c.get_json(f"{BASE}/eventslast.php", {"id": str(i)})
        self.assertFalse(c.circuito_abierto)

    def test_el_resumen_cuenta_todo(self):
        e = Entorno([Respuesta(200, {}), Respuesta(200, texto="")])
        c = e.cliente(intervalo=0)
        c.get_json(f"{BASE}/eventslast.php", {"id": "1"})
        c.get_json(f"{BASE}/eventslast.php", {"id": "1"})
        with self.assertRaises(SportsDBNoJSON):
            c.get_json(f"{BASE}/eventslast.php", {"id": "2"})
        texto = c.resumen()
        self.assertIn("3 consultas", texto)
        self.assertIn("2 a la red", texto)
        self.assertIn("1 desde cache", texto)
        self.assertIn("0 bloqueos 429", texto)
        self.assertIn("1 sin JSON", texto)


class SinJSONTests(unittest.TestCase):
    def test_html_o_vacio_es_fallo_blando(self):
        for cuerpo in ("", "<html>Too many</html>"):
            e = Entorno([Respuesta(200, texto=cuerpo)])
            c = e.cliente(intervalo=0)
            with self.assertRaises(ValueError):
                c.get_json(f"{BASE}/lookuptable.php", {"l": "4490", "s": "2026"})
            with self.assertRaises(SportsDBNoJSON):
                c.get_json(f"{BASE}/lookuptable.php", {"l": "4490", "s": "2026"})
            self.assertEqual(len(e.peticiones), 1, "ni se reintenta ni se repite")
            self.assertEqual(e.fallos, 0, "no es una averia del proveedor")
            self.assertFalse(c.circuito_abierto)


class IntegracionWorkerTests(unittest.TestCase):
    """El worker de verdad, con requests.get falso."""

    def setUp(self):
        self.e = Entorno()
        self.cliente_original = sw._SPORTSDB_CLIENTE
        sw._SPORTSDB_CLIENTE = self.e.cliente(
            intervalo=0, al_fallar=sw._marcar_fallo_sportsdb
        )
        sw._reiniciar_fallos_sportsdb()
        sw._reiniciar_cupo_sportsdb()
        self.patch = mock.patch.object(requests, "get", side_effect=self.e.get)
        self.patch.start()
        # Las pruebas no deben tocar la cache HTTP del worker.
        sw._SPORTSDB_CLIENTE.ruta_cache = None
        self.e.respuestas = []

    def tearDown(self):
        self.patch.stop()
        sw._SPORTSDB_CLIENTE = self.cliente_original
        sw._reiniciar_fallos_sportsdb()
        sw._reiniciar_cupo_sportsdb()

    def test_request_json_pasa_por_el_cliente(self):
        sw._request_json(sw.THESPORTSDB_SEARCH_TEAM_URL, params={"t": "Alaves"})
        sw._request_json(sw.THESPORTSDB_SEARCH_TEAM_URL, params={"t": "Alaves"})
        self.assertEqual(sw._SPORTSDB_CLIENTE.stats["llamadas"], 2)
        self.assertEqual(len(self.e.peticiones), 1)

    def test_lo_servido_de_cache_no_gasta_cupo(self):
        for _ in range(5):
            sw._frenar_sportsdb()
            sw._request_json(sw.THESPORTSDB_EVENTS_LAST_URL, params={"id": "133604"})
        self.assertEqual(sw._SPORTSDB_PETICIONES_CICLO, 1)

    def test_la_tabla_no_se_pide_con_las_dos_etiquetas_si_nos_limitan(self):
        self.e.respuestas = [Respuesta(429)] * 3
        clave = "sportsdb_table:v1:4490:"
        for k in [k for k in sw.HISTORY_CACHE if k.startswith(clave)]:
            sw.HISTORY_CACHE.pop(k, None)
        with mock.patch.object(sw, "fetch_the_sportsdb_last_events", return_value=[]):
            sw._sportsdb_domestic_fallback("Moldova", {"idTeam": "1", "idLeague": "4490"}, None)
        tablas = [p for p in self.e.peticiones if p[1].endswith("lookuptable.php")]
        etiquetas = {p[2]["s"] for p in tablas}
        self.assertEqual(len(etiquetas), 1, etiquetas)

    def test_la_segunda_etiqueta_si_se_prueba_cuando_la_primera_no_trae_tabla(self):
        """Las ligas nordicas van por ano natural: eso no se puede perder."""
        self.e.respuestas = [Respuesta(200, {"table": None}), Respuesta(200, {"table": [{"strTeam": "Molde", "intRank": "1"}]})]
        for k in [k for k in sw.HISTORY_CACHE if k.startswith("sportsdb_table:v1:4358:")]:
            sw.HISTORY_CACHE.pop(k, None)
        with mock.patch.object(sw, "fetch_the_sportsdb_last_events", return_value=[]):
            _, tabla = sw._sportsdb_domestic_fallback("Molde", {"idTeam": "1", "idLeague": "4358"}, None)
        for k in [k for k in sw.HISTORY_CACHE if k.startswith("sportsdb_table:v1:4358:")]:
            sw.HISTORY_CACHE.pop(k, None)
        self.assertIn("Molde", tabla)

    def test_misma_tabla_para_todas_las_selecciones_se_pide_una_vez(self):
        self.e.respuestas = []
        self.e.por_defecto = Respuesta(200, texto="")  # lo que devolvia lookuptable 4490
        for k in [k for k in sw.HISTORY_CACHE if k.startswith("sportsdb_table:v1:4490:")]:
            sw.HISTORY_CACHE.pop(k, None)
        with mock.patch.object(sw, "fetch_the_sportsdb_last_events", return_value=[]):
            for equipo in ("Moldova", "Faroe Islands", "Spain", "Italy", "Norway"):
                sw._sportsdb_domestic_fallback(equipo, {"idTeam": "1", "idLeague": "4490"}, None)
        tablas = [p for p in self.e.peticiones if p[1].endswith("lookuptable.php")]
        self.assertEqual(len(tablas), 2, "una por etiqueta, no dos por seleccion")

    def test_los_avisos_repetidos_no_se_imprimen(self):
        self.e.por_defecto = Respuesta(503)
        sw.HISTORY_CACHE.pop("sportsdb_table:v1:5518:2026-2027", None)
        with mock.patch("builtins.print") as impreso:
            for _ in range(6):
                sw.fetch_the_sportsdb_lookup_table("5518", "2026-2027")
        avisos = [c for c in impreso.call_args_list if "lookuptable" in str(c)]
        self.assertEqual(len(avisos), 1)

    def test_con_el_circuito_abierto_la_auditoria_sabe_que_es_el_proveedor(self):
        """Si no, los lados vacios por culpa del 429 frenarian la publicacion."""
        self.e.por_defecto = Respuesta(429)
        with self.assertRaises(SportsDBError):
            sw._request_json(sw.THESPORTSDB_EVENTS_LAST_URL, params={"id": "1"})
        self.assertTrue(sw._SPORTSDB_CLIENTE.circuito_abierto)
        self.assertTrue(sw._sportsdb_degradado())
        sw._reiniciar_fallos_sportsdb()
        self.assertFalse(sw._sportsdb_degradado())

    def test_run_once_resume_la_pasada(self):
        import inspect

        fuente = inspect.getsource(sw.run_once)
        self.assertIn("_resumir_sportsdb()", fuente)
        self.assertLess(fuente.index("_reiniciar_fallos_sportsdb()"), fuente.index("fetch_snapshot()"))


if __name__ == "__main__":
    unittest.main()
