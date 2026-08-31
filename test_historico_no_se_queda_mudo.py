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


if __name__ == "__main__":
    unittest.main()
