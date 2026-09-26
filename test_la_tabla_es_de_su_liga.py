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
