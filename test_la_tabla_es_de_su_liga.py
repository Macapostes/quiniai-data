"""La tabla de un equipo tiene que ser la de la competición del partido.

En la jornada 9 el Andorra CF llegó al informe como "15º con 1 partido jugado"
en un Segunda donde el resto llevaba seis. Su ficha de TheSportsDB lo tiene en
la liga andorrana (5554), así que el relleno de huecos le montó la tabla de esa
competición y la etiquetó como doméstica.

No es un número feo y ya está: el informe escribió "Granada y Andorra están 14º
y 15º, separados por nada" y montó encima la apuesta más grande del boleto —
dejar fuera el 1 del Granada, que llevaba el 75% del público—. La tabla real de
Segunda tenía al Andorra 20º.
"""

import inspect
import unittest

import snapshot_worker as w


class ElRellenoNoTraeTablasDeOtraLigaTests(unittest.TestCase):
    def test_el_guardian_va_antes_de_buscar_nada(self):
        fuente = inspect.getsource(w._fill_side_from_sportsdb_if_empty)
        self.assertLess(
            fuente.index("liga_del_equipo"), fuente.index("_sportsdb_domestic_fallback(")
        )

    def test_una_ficha_de_otra_liga_no_rellena(self):
        historia = {"recent_all": {}, "table": {}}
        salida = w._fill_side_from_sportsdb_if_empty(
            historia,
            "Andorra CF",
            {"idTeam": "1", "idLeague": "5554"},   # liga andorrana
            None,
            "soccer_spain_segunda_division",       # el partido es de Segunda (4400)
            "domestic",
            None,
        )
        self.assertEqual(salida, historia)
        self.assertFalse((salida.get("table") or {}).get("position"))

    def test_sin_ficha_de_liga_no_se_bloquea(self):
        """Si TheSportsDB no dice liga, se sigue como hasta ahora."""
        fuente = inspect.getsource(w._fill_side_from_sportsdb_if_empty)
        self.assertIn("if liga_del_equipo and liga_del_partido and", fuente)

    def test_segunda_tiene_su_id(self):
        self.assertEqual(str(w._sportsdb_league_id_for_key("soccer_spain_segunda_division")), "4400")


if __name__ == "__main__":
    unittest.main()


class ALoGuardadoTambienSeLeQuitaTests(unittest.TestCase):
    """Arreglar la causa no le llega a un partido que ya estaba guardado.

    Es la misma trampa que con las noticias: un partido que no vuelve a pasar
    por el enriquecimiento se sirve con lo que tuviera dentro. Tras arreglar el
    relleno, el Andorra seguía saliendo 15º con la tabla andorrana.
    """

    def _partido(self, liga="soccer_spain_segunda_division", liga_ficha="sportsdb_5554"):
        return {
            "local": "Granada CF",
            "visitante": "Andorra CF",
            "league": liga,
            "history_context": {
                "home": {
                    "league_key": "soccer_spain_segunda_division",
                    "table": {"position": 14, "played": 6, "points": 8},
                },
                "away": {
                    "league_key": liga_ficha,
                    "table": {"position": 15, "played": 1, "points": 0},
                },
            },
        }

    def test_se_retira_la_de_otra_liga(self):
        partido = self._partido()
        self.assertEqual(w._quitar_tablas_de_otra_liga(partido), 1)
        self.assertEqual(partido["history_context"]["away"]["table"], {})
        self.assertEqual(partido["history_context"]["home"]["table"]["position"], 14)

    def test_la_de_su_liga_se_queda(self):
        partido = self._partido(liga_ficha="soccer_spain_segunda_division")
        self.assertEqual(w._quitar_tablas_de_otra_liga(partido), 0)

    def test_en_una_jornada_europea_no_se_toca_nada(self):
        """Ahí las fichas de los dos equipos son de sus ligas, y está bien."""
        partido = self._partido(liga="soccer_uefa_champs_league", liga_ficha="soccer_spain_la_liga")
        partido["history_context"]["home"]["league_key"] = "soccer_germany_bundesliga"
        self.assertEqual(w._quitar_tablas_de_otra_liga(partido), 0)
        self.assertEqual(partido["history_context"]["away"]["table"]["position"], 15)

    def test_el_ciclo_lo_hace_antes_de_publicar(self):
        fuente = inspect.getsource(w.build_snapshot)
        self.assertIn("_quitar_tablas_de_otra_liga(match)", fuente)
