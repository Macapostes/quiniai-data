"""Un fallo de un momento no puede dejar una liga sin datos 24 horas.

Asi es como la Liga F llevaba semanas sin clasificacion ni H2H: el ciclo pedia
el historico despues de gastarse el cupo del proveedor en las consultas de
equipo, le respondian 429, y el vacio resultante se guardaba en la cache. Los
ciclos siguientes leian el vacio y ni lo intentaban.
"""

import inspect
import re
import unittest

import snapshot_worker as w


class LaCacheNoSeEnvenenaTests(unittest.TestCase):
    def test_ninguna_rama_cachea_una_lista_vacia(self):
        fuente = inspect.getsource(w.fetch_league_history)
        # Cada `_cache_set` del historico tiene que ir precedido de la
        # comprobacion de que hay algo que guardar.
        for trozo in fuente.split("_cache_set(HISTORY_CACHE")[:-1]:
            cola = trozo[-260:]
            self.assertTrue(
                "if parsed_rows:" in cola or "if rows:" in cola,
                "hay un _cache_set del historico sin comprobar que trae filas:\n" + cola,
            )

    def test_si_falla_se_sirve_la_copia_anterior(self):
        """Basta con acertar UNA vez para que la liga no vuelva a quedarse muda."""
        fuente = inspect.getsource(w.fetch_league_history)
        self.assertIn("_cache_get(HISTORY_CACHE, cache_key) or []", fuente)

    def test_la_rama_de_sportsdb_tambien(self):
        fuente = inspect.getsource(w._fetch_sportsdb_league_history)
        self.assertIn("if rows:\n            _cache_set", fuente)
        self.assertIn("_cache_get(HISTORY_CACHE, cache_key) or []", fuente)


class ReintentoDeLaTemporadaTests(unittest.TestCase):
    """Una peticion desbloquea la liga entera: merece la pena insistir."""

    def test_reintenta_cuando_le_limitan(self):
        fuente = inspect.getsource(w._eventos_de_temporada_completa)
        self.assertIn("_ESPERAS_REINTENTO_TEMPORADA", fuente)
        self.assertIn('"429" in str(exc)', fuente)

    def test_no_reintenta_otros_errores(self):
        """Un 404 o un fallo de red no se arreglan esperando; insistir ahi solo
        alarga el ciclo."""
        fuente = inspect.getsource(w._eventos_de_temporada_completa)
        self.assertIn("if not limitado or ultimo:", fuente)
        self.assertLess(fuente.index("if not limitado"), fuente.index("time.sleep"))

    def test_el_numero_de_intentos_esta_acotado(self):
        self.assertLessEqual(len(w._ESPERAS_REINTENTO_TEMPORADA), 4)
        # Y la espera total no puede colgar el ciclo.
        self.assertLessEqual(sum(w._ESPERAS_REINTENTO_TEMPORADA), 120)


class LigaFResuelveTests(unittest.TestCase):
    def test_la_liga_femenina_espanola_tiene_identidad(self):
        """Sin esto el partido se queda en `league_unresolved` y no hay de donde
        sacar tabla ni H2H."""
        self.assertEqual(w._sportsdb_league_id_for_key("sportsdb_5106"), "5106")

    def test_la_tabla_se_calcula_de_los_resultados(self):
        """TheSportsDB devuelve la clasificacion de la Liga F con `intPlayed: 0`
        -no la calcula-, asi que sale de los partidos jugados."""
        filas = [
            {"Date": "2026-08-29", "HomeTeam": "Madrid CFF", "AwayTeam": "Granada Femenino",
             "FTHG": 3, "FTAG": 1, "FTR": "H", "SeasonCode": "2627"},
            {"Date": "2026-08-29", "HomeTeam": "Eibar Women", "AwayTeam": "Espanyol Femení",
             "FTHG": 1, "FTAG": 0, "FTR": "H", "SeasonCode": "2627"},
            {"Date": "2026-08-30", "HomeTeam": "Granada Femenino", "AwayTeam": "Eibar Women",
             "FTHG": 0, "FTAG": 0, "FTR": "D", "SeasonCode": "2627"},
        ]
        tabla = w._table_snapshot(filas)
        self.assertIn("Madrid CFF", tabla)
        # Eibar suma 4 (una victoria y un empate) y Madrid CFF 3 en un partido.
        self.assertEqual(tabla["Eibar Women"]["points"], 4)
        self.assertEqual(tabla["Eibar Women"]["position"], 1)
        self.assertEqual(tabla["Madrid CFF"]["points"], 3)
        self.assertEqual(tabla["Madrid CFF"]["position"], 2)
        self.assertEqual(tabla["Granada Femenino"]["played"], 2)
        # Y con tabla, la calidad deja de ser "tabla vacia".
        calidad = w._table_quality_snapshot(tabla, "Madrid CFF", "Eibar Women")
        self.assertNotEqual(calidad.get("reason"), "tabla vacia")


class LaPodaNoBorraLoQueAcabaDeGuardarTests(unittest.TestCase):
    """La causa de raiz de toda la historia de la Liga F.

    La poda mira el ultimo trozo de la clave y lo compara con los codigos de
    temporada ("2627"). Pero las claves de TheSportsDB acaban en como lo llama
    el proveedor: el ano de inicio ("2026") o la etiqueta ("2026-2027"). Ninguno
    coincidia, asi que el historico se borraba entero en CADA guardado: no
    llegaba a persistir nunca, cada ciclo lo volvia a pedir desde cero, y el
    proveedor respondia 429. De ahi que el partido femenino no tuviera ni
    clasificacion ni H2H por mucho que se arreglara todo lo demas.
    """

    def _podar(self, claves):
        for k in claves:
            w.HISTORY_CACHE[k] = {"fetched_at": w._now_iso(), "data": [1]}
        w._prune_history_cache()
        return {k for k in claves if k in w.HISTORY_CACHE}

    def test_sobreviven_las_tres_formas_de_nombrar_la_temporada(self):
        temporada = sorted(w._sportsdb_recent_seasons())[-1]      # "2026"
        etiqueta = w._etiquetas_de_temporada(temporada)[0]        # "2026-2027"
        codigo = sorted(w._recent_season_codes(w.HISTORY_SEASONS_BACK))[-1]  # "2627"
        claves = [
            f"sportsdb_history:v5:sportsdb_5106:5106:{temporada}",
            f"sportsdb_season_events:v1:5106:{etiqueta}",
            f"soccer_spain_la_liga:{codigo}",
        ]
        self.assertEqual(self._podar(claves), set(claves))

    def test_las_temporadas_viejas_si_se_borran(self):
        """La poda tiene que seguir sirviendo para algo."""
        claves = [
            "sportsdb_season_events:v1:5106:2019-2020",
            "soccer_spain_la_liga:2021",
        ]
        self.assertEqual(self._podar(claves), set())


class UnaClaveNumericaSigueSiendoLaLigaTests(unittest.TestCase):
    """Tres partidos de Liga F traian clasificacion y el cuarto no.

    La diferencia estaba en como le habia quedado escrita la liga: a tres les
    llego "sportsdb_5106" y al cuarto "5106" a secas, puesta por el camino que
    infiere la liga del boleto. Sin el prefijo no se resolvia el id, asi que se
    quedaba sin historico -y con el, sin tabla ni H2H- mientras los de al lado,
    identicos, si lo tenian.
    """

    def test_con_prefijo_y_sin_el_dan_la_misma_liga(self):
        self.assertEqual(w._sportsdb_league_id_for_key("sportsdb_5106"), "5106")
        self.assertEqual(w._sportsdb_league_id_for_key("5106"), "5106")

    def test_las_dos_formas_son_la_misma_liga(self):
        """Si no se unifican, cada una se trae su propio historico y lo cachea
        aparte: el doble de peticiones al proveedor -lo que dejaba la liga sin
        datos- y dos copias que pueden estar en distinto estado."""
        self.assertEqual(
            w._canonical_league_key("5106"), w._canonical_league_key("sportsdb_5106")
        )

    def test_las_claves_normales_no_se_confunden_con_un_id(self):
        for clave in ("soccer_spain_la_liga", "league_unresolved", "", "sportsdb_"):
            with self.subTest(clave):
                self.assertEqual(w._sportsdb_league_id_for_key(clave), "")


if __name__ == "__main__":
    unittest.main()
