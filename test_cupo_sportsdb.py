"""El worker no puede vaciar el presupuesto compartido de TheSportsDB.

La clave publica permite unas 30 peticiones por minuto y **la comparte todo el
que usa la API gratis**: no es un cupo nuestro. El 6 de septiembre de 2026 un
solo ciclo se comio 1.063 peticiones rechazadas y dejo la jornada sin publicar.

Lo que se pide ahi -clasificacion, racha, enfrentamientos, plantillas- cambia
como mucho una vez por jornada. Pedirlo cada dos horas era desperdicio, y el
desperdicio se pagaba quedandose sin datos.
"""

import os
import unittest

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import snapshot_worker as sw  # noqa: E402


def _gastar(n):
    """Gasta n peticiones de cupo sin dormir.

    _frenar_sportsdb() espacia de verdad las llamadas (2 s cada una), asi
    que usarlo tal cual en un bucle de 40 dejaba la suite en cinco minutos.
    Aqui se prueba la contabilidad del cupo, no el temporizador.
    """
    original = sw._SPORTSDB_PAUSA_SEGUNDOS
    sw._SPORTSDB_PAUSA_SEGUNDOS = 0.0
    try:
        for _ in range(n):
            sw._frenar_sportsdb()
    finally:
        sw._SPORTSDB_PAUSA_SEGUNDOS = original


class CupoPorCicloTests(unittest.TestCase):
    def setUp(self):
        sw._reiniciar_cupo_sportsdb()

    def tearDown(self):
        sw._reiniciar_cupo_sportsdb()

    def test_al_empezar_hay_cupo(self):
        self.assertTrue(sw._sportsdb_hay_cupo())

    def test_cada_peticion_gasta_cupo(self):
        antes = sw._SPORTSDB_PETICIONES_CICLO
        _gastar(1)
        self.assertEqual(sw._SPORTSDB_PETICIONES_CICLO, antes + 1)

    def test_el_cupo_se_agota(self):
        _gastar(sw.SPORTSDB_MAX_PETICIONES_CICLO)
        self.assertFalse(sw._sportsdb_hay_cupo())

    def test_el_cupo_se_reinicia_en_cada_ciclo(self):
        """Sin reinicio, el worker gastaria el cupo el primer ciclo del dia y no
        volveria a pedir nada nunca mas."""
        _gastar(sw.SPORTSDB_MAX_PETICIONES_CICLO)
        self.assertFalse(sw._sportsdb_hay_cupo())
        sw._reiniciar_cupo_sportsdb()
        self.assertTrue(sw._sportsdb_hay_cupo())

    def test_run_once_reinicia_el_cupo_antes_de_pedir_datos(self):
        import inspect
        fuente = inspect.getsource(sw.run_once)
        self.assertIn("_reiniciar_cupo_sportsdb()", fuente)
        self.assertLess(
            fuente.index("_reiniciar_cupo_sportsdb()"),
            fuente.index("fetch_snapshot()"),
        )


class ReservaParaLigasTests(unittest.TestCase):
    """Una peticion de liga vale mucho mas que una de equipo.

    `_eventos_de_temporada_completa` desbloquea con UNA peticion la
    clasificacion, la racha y el H2H de una liga entera. Antes el ciclo se
    gastaba el cupo resolviendo nombres de equipo y la liga se quedaba muda.
    """

    def setUp(self):
        sw._reiniciar_cupo_sportsdb()

    def tearDown(self):
        sw._reiniciar_cupo_sportsdb()

    def test_los_equipos_paran_antes_que_las_ligas(self):
        gastadas = sw.SPORTSDB_MAX_PETICIONES_CICLO - sw.SPORTSDB_RESERVA_LIGAS
        _gastar(gastadas)
        self.assertFalse(
            sw._sportsdb_hay_cupo(sw.SPORTSDB_RESERVA_LIGAS),
            "los equipos ya no deberian pedir",
        )
        self.assertTrue(
            sw._sportsdb_hay_cupo(),
            "pero la liga si, que para eso se le reserva",
        )

    def test_la_reserva_tambien_tiene_tope(self):
        """Reservado no es ilimitado: con muchas ligas tampoco se puede vaciar
        el presupuesto de todo el mundo."""
        _gastar(sw.SPORTSDB_MAX_PETICIONES_CICLO)
        self.assertFalse(sw._sportsdb_hay_cupo())


class VentanasDeCacheTests(unittest.TestCase):
    def test_la_ventana_maxima_cabe_dentro_de_la_purga(self):
        """Si la ventana larga superase a la purga generica, seria mentira: el
        dato ya no estaria guardado para servirlo."""
        self.assertLessEqual(sw.SPORTSDB_TTL_MAXIMA, sw.GENERIC_CACHE_MAX_AGE_SECONDS)

    def test_la_fresca_es_mas_corta_que_la_maxima(self):
        self.assertLess(sw.SPORTSDB_TTL_FRESCA, sw.SPORTSDB_TTL_MAXIMA)

    def test_la_cache_tiene_sitio_de_sobra(self):
        """Estaba en 500 y habia exactamente 500 entradas: llena y expulsando
        fichas que luego habia que volver a pedir."""
        self.assertGreaterEqual(sw.GENERIC_CACHE_MAX_ENTRIES, 1000)

    def test_sin_cupo_se_sirve_la_copia_vieja_en_vez_de_nada(self):
        """El equipo no puede desaparecer por no tener cupo: la ficha de hace
        ocho dias sigue siendo mejor que un hueco."""
        import inspect
        fuente = inspect.getsource(sw.fetch_the_sportsdb_team)
        self.assertIn("SPORTSDB_TTL_MAXIMA", fuente)
        self.assertIn("return vieja", fuente)


class NoCachearElVacioTests(unittest.TestCase):
    def test_sin_cupo_la_plantilla_no_se_guarda_vacia(self):
        """Guardar [] al quedarnos sin cupo dejaria al equipo sin jugadores
        hasta que caducase la entrada. Hay que devolver vacio SIN guardar."""
        import inspect
        fuente = inspect.getsource(sw._plantilla_de_equipo)
        corte = fuente.index("_sportsdb_hay_cupo")
        tras_el_corte = fuente[corte : corte + 400]
        self.assertIn("return []", tras_el_corte)
        self.assertNotIn("_cache_set", tras_el_corte.split("return []")[0])


class NingunaLlamadaSeSaltaElCupoTests(unittest.TestCase):
    """El cupo no vale de nada si hay puertas traseras.

    Al medir el primer ciclo con cupo salieron 519 peticiones rechazadas cuando
    el tope era 40: tres funciones -eventsnext dos veces y eventsround- pedian
    sin pasar por el freno. Este test recorre el fuente y no deja que vuelva a
    colarse ninguna.
    """

    def test_toda_llamada_a_thesportsdb_pasa_por_el_freno(self):
        from pathlib import Path

        lineas = Path(sw.__file__).read_text(encoding="utf-8").splitlines()
        marcadores = (
            "THESPORTSDB_EVENTS_NEXT_URL,",
            "THESPORTSDB_EVENTS_ROUND_URL,",
            "THESPORTSDB_EVENTS_SEASON_URL,",
            "THESPORTSDB_SEARCH_TEAM_URL,",
            "lookup_all_players",
        )
        sin_freno = []
        for i, linea in enumerate(lineas):
            if not any(m in linea for m in marcadores):
                continue
            # El bloque de constantes de arriba no son llamadas.
            if linea.strip().startswith(("THESPORTSDB_", "#")):
                continue
            previas = lineas[max(0, i - 10) : i]
            if not any("_frenar_sportsdb()" in p for p in previas):
                sin_freno.append(f"linea {i + 1}: {linea.strip()[:70]}")
        self.assertEqual(
            sin_freno,
            [],
            "Estas llamadas piden a TheSportsDB sin gastar cupo: " + str(sin_freno),
        )

    def test_toda_llamada_mira_el_cupo_antes_de_gastarlo(self):
        """Descontar no es parar.

        El test de arriba exige que cada llamada pase por el freno, y el freno
        descuenta del cupo; pero una funcion que descuenta sin mirar lo que
        queda se salta el tope igual. El 17/10/2026 un ciclo gasto 173
        peticiones con el tope en 40 y se paso 33 de sus 58 minutos esperando
        turno: fetch_the_sportsdb_team miraba el cupo una sola vez, y luego el
        bucle de variantes del nombre lanzaba hasta seis seguidas sin volver a
        mirar; eventslast y lookuptable no lo miraban nunca.

        Dos funciones estan exentas a proposito, y no por descuido: ver 33ade7d
        (9/9/2026). last_events y lookup_table son las pocas llamadas que de
        verdad valen, y se piden aunque la busqueda de nombres haya vaciado el
        cupo; si no, un 429 dejaba a Bayern y al Shakhtar sin racha ni puesto, y
        a Bodo con el puesto 22 de la Champions. Gastan cupo pero no lo
        consultan. Lo que si miran es si el proveedor esta caido.

        La ventana de 12 lineas da de sobra: hoy la comprobacion mas lejana
        esta a 6 lineas de su llamada.
        """
        from pathlib import Path

        EXENTAS = {
            "fetch_the_sportsdb_last_events",
            "fetch_the_sportsdb_lookup_table",
        }

        lineas = Path(sw.__file__).read_text(encoding="utf-8").splitlines()

        def _funcion_de(indice):
            for j in range(indice, -1, -1):
                if lineas[j].startswith("def "):
                    return lineas[j][4:].split("(")[0]
            return ""

        sin_mirar = []
        for i, linea in enumerate(lineas):
            if "_frenar_sportsdb()" not in linea:
                continue
            # Ni la definicion de la funcion ni los comentarios que la nombran.
            if linea.strip().startswith(("#", "def ")):
                continue
            if _funcion_de(i) in EXENTAS:
                continue
            previas = lineas[max(0, i - 12) : i]
            if not any("_sportsdb_hay_cupo" in p for p in previas):
                sin_mirar.append(f"linea {i + 1}: {linea.strip()[:70]}")
        self.assertEqual(
            sin_mirar,
            [],
            "Estas gastan cupo sin comprobar si queda: " + str(sin_mirar),
        )

    def test_las_exentas_del_cupo_si_paran_con_el_proveedor_caido(self):
        """Exenta del cupo no es exenta de todo.

        Pedir con el circuito abierto no trae el dato: solo hace cola. El
        17/10/2026 el ciclo se paso 33 minutos esperando turno con 30 respuestas
        429 encima.
        """
        import inspect

        for funcion in (sw.fetch_the_sportsdb_last_events, sw.fetch_the_sportsdb_lookup_table):
            fuente = inspect.getsource(funcion)
            self.assertIn(
                "_sportsdb_degradado()",
                fuente,
                f"{funcion.__name__} sigue pidiendo con el proveedor caido",
            )
            # La exencion del cupo es deliberada (33ade7d): que siga siendolo.
            self.assertNotIn("_sportsdb_hay_cupo", fuente)


if __name__ == "__main__":
    unittest.main()
