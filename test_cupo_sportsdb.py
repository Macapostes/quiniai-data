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


if __name__ == "__main__":
    unittest.main()
