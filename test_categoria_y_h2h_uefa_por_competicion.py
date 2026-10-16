# -*- coding: utf-8 -*-
"""16-oct-2026: categoria por competicion y H2H europeo de todas las competiciones UEFA.

- J12: la LAE mando "RAYO", "BARCELONA"... sin (M)/(F) y el historico se
  publicaba con gender "": el backend no podia verificar la categoria.
- J13: GALATASARAY-BARCELONA (Champions) se vio en la Europa League 2021-22,
  pero solo se miraba el archivo de la Champions.
"""
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import snapshot_worker as w


class CategoriaPorCompeticionTests(unittest.TestCase):
    def test_laliga_segunda_y_uefa_son_masculinas_sin_sufijo(self):
        for liga in ("soccer_spain_la_liga", "soccer_spain_segunda_division", "soccer_uefa_champs_league", "sportsdb_4335"):
            with self.subTest(liga):
                self.assertEqual(w._categoria_publicable({"league": liga, "local": "RAYO", "visitante": "ATH.CLUB"}), "male")

    def test_liga_f_es_femenina(self):
        self.assertEqual(w._categoria_publicable({"league": "sportsdb_5106", "local": "R.MADRID", "visitante": "ESPANYOL"}), "female")
        self.assertEqual(w._categoria_publicable({"league": "x", "league_name": "Liga F", "local": "A", "visitante": "B"}), "female")

    def test_el_sufijo_del_boleto_sigue_mandando(self):
        self.assertEqual(w._categoria_publicable({"league": "soccer_spain_la_liga", "local_lae": "ALAVÉS (F)", "visitante_lae": "VALENCIA (F)"}), "female")

    def test_competicion_desconocida_no_inventa(self):
        self.assertEqual(w._categoria_publicable({"league": "sportsdb_999999", "local": "A", "visitante": "B"}), "")


class H2HEuropeoDeTodasLasCompeticionesTests(unittest.TestCase):
    def test_un_cruce_de_champions_mira_tambien_europa_league_y_conference(self):
        pedidas = []

        def historia(clave, seasons_back=None):
            pedidas.append(clave)
            if clave == "soccer_uefa_europa_league":
                return [
                    {"Date": "2022-03-10", "HomeTeam": "Barcelona", "AwayTeam": "Galatasaray", "FTHG": 0, "FTAG": 0, "FTR": "D", "League": "UEFA Europa League"},
                    {"Date": "2022-03-17", "HomeTeam": "Galatasaray", "AwayTeam": "Barcelona", "FTHG": 1, "FTAG": 2, "FTR": "A", "League": "UEFA Europa League"},
                ]
            return []

        with patch.object(w, "fetch_league_history", side_effect=historia), \
             patch.object(w, "fetch_the_sportsdb_h2h_events", return_value=[]), \
             patch.object(w, "_club_api_for_history", side_effect=lambda team, api: api), \
             patch.object(w, "_domestic_league_key_from_team_api", return_value=""), \
             patch.object(w, "_ensure_league_history", return_value=[]), \
             patch.object(w, "_team_history_with_scope", return_value={}), \
             patch.object(w, "_fill_side_from_sportsdb_if_empty", side_effect=lambda h, *a, **k: h):
            _, _, h2h = w._resolve_domestic_histories_and_h2h(
                home_team="Galatasaray", away_team="Barcelona", league_key="soccer_uefa_champs_league",
                histories={}, home_team_api=None, away_team_api=None,
                kickoff_dt=datetime(2026, 10, 13, 19, 0, tzinfo=timezone.utc),
            )
        self.assertIn("soccer_uefa_europa_league", pedidas)
        self.assertIn("soccer_uefa_europa_conference_league", pedidas)
        self.assertEqual(h2h.get("meetings"), 2)


if __name__ == "__main__":
    unittest.main()
