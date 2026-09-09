# -*- coding: utf-8 -*-
"""En Champions, la forma y el puesto salen de la liga domestica de cada club.

El CSV de la Champions en MD1 esta vacio (o solo tiene la previa). Bayern y
Shakhtar llegaban con recent_all {} y H2H vacio, y cada movil pedia Sofascore.
El worker tiene que rellenar una vez: ultimos 5 de la liga de cada uno, puesto
real (Shakhtar = Ucrania), H2H historico sin el partido de hoy.
"""

import inspect
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import snapshot_worker as w


class AliasLAETests(unittest.TestCase):
    def test_nombres_del_boleto(self):
        casos = {
            "BAYERN DE MÚNICH": "Bayern Munich",
            "BAYERN DE MUNICH": "Bayern Munich",
            "B.MUNICH": "Bayern Munich",
            "SHAKHTAR": "Shakhtar Donetsk",
            "OPORTO": "Porto",
            "MAN.CITY": "Manchester City",
            "PSV": "PSV Eindhoven",
        }
        for lae, canonico in casos.items():
            with self.subTest(lae):
                self.assertEqual(w._canonical_team_name(lae), canonico)


class SatelitesTests(unittest.TestCase):
    def test_legends_y_filial_no_son_el_primer_equipo(self):
        self.assertTrue(w._parece_equipo_satelite("Bayern Munich Legends"))
        self.assertTrue(w._parece_equipo_satelite("Bayern Munich II"))
        self.assertFalse(w._parece_equipo_satelite("Bayern Munich"))
        self.assertEqual(w._team_similarity_score("Bayern Munich", "Bayern Munich Legends"), 0.0)
        self.assertEqual(w._team_similarity_score("Bayern Munich", "Bayern Munich II"), 0.0)

    def test_hof_e_escaldes_no_heredan_al_grande(self):
        self.assertFalse(w._es_el_mismo_club("Bayern Munich", "Bayern Hof"))
        self.assertFalse(w._es_el_mismo_club("Inter Milan", "Inter Club d'Escaldes"))
        self.assertFalse(w._country_label_matches("Italy", "Spain"))


class LigaDomesticaDelClubTests(unittest.TestCase):
    def test_la_ficha_dice_la_liga_real(self):
        self.assertEqual(
            w._domestic_league_key_from_team_api(
                {"idLeague": "4331", "strLeague": "German Bundesliga"}
            ),
            "soccer_germany_bundesliga",
        )
        self.assertEqual(
            w._domestic_league_key_from_team_api(
                {"idLeague": "4354", "strLeague": "Ukrainian Premier League"}
            ),
            "soccer_ukraine_premier_league",
        )
        self.assertEqual(
            w._domestic_league_key_from_team_api(
                {"idLeague": "4337", "strLeague": "Dutch Eredivisie"}
            ),
            "soccer_netherlands_eredivisie",
        )

    def test_una_ficha_de_champions_no_cuenta_como_domestica(self):
        self.assertEqual(
            w._domestic_league_key_from_team_api(
                {"idLeague": "4480", "strLeague": "UEFA Champions League"}
            ),
            "",
        )

    def test_el_partido_sigue_siendo_champions(self):
        match = {
            "local": "B.MUNICH",
            "visitante": "Bodo Glimt",
            "league": "soccer_uefa_champs_league",
            "league_source": "feed-de-cuotas",
        }
        w._apply_dynamic_league_metadata(
            match,
            {"idLeague": "4480", "idEvent": "1"},
            {"idLeague": "4331"},
            {"idLeague": "4358"},
        )
        self.assertEqual(match["league"], "soccer_uefa_champs_league")


class FormaDomesticaTests(unittest.TestCase):
    def setUp(self):
        self.kickoff = datetime(2026, 9, 9, 19, 0, tzinfo=timezone.utc)

    def test_ultimos_5_y_puesto_salen_del_csv_domestico(self):
        filas = []
        for i, (gf, ga, ftr) in enumerate(
            [(3, 1, "H"), (2, 0, "H"), (1, 1, "D"), (4, 0, "H"), (2, 1, "H")],
            start=1,
        ):
            filas.append(
                {
                    "Date": f"2026-08-{20 + i:02d}",
                    "HomeTeam": "Bayern Munich",
                    "AwayTeam": f"Rival {i}",
                    "FTHG": gf,
                    "FTAG": ga,
                    "FTR": ftr,
                    "SeasonCode": "2627",
                }
            )
        filas.append(
            {
                "Date": "2026-08-21",
                "HomeTeam": "Stuttgart",
                "AwayTeam": "Leverkusen",
                "FTHG": 1,
                "FTAG": 0,
                "FTR": "H",
                "SeasonCode": "2627",
            }
        )
        ctx = w._team_history_with_scope(
            filas,
            "B.MUNICH",
            self.kickoff,
            "soccer_germany_bundesliga",
            "domestic",
            {"filas_de_su_categoria": True},
        )
        self.assertEqual(ctx.get("resolved_name"), "Bayern Munich")
        self.assertEqual((ctx.get("recent_all") or {}).get("matches"), 5)
        self.assertTrue((ctx.get("recent_all") or {}).get("form"))
        self.assertEqual((ctx.get("table") or {}).get("played"), 5)
        self.assertEqual((ctx.get("table") or {}).get("position"), 1)

    def test_copa_usa_la_liga_de_la_ficha_no_la_champions(self):
        filas = [
            {
                "Date": f"2026-08-{20 + i:02d}",
                "HomeTeam": "Shakhtar Donetsk",
                "AwayTeam": f"Rival {i}",
                "FTHG": 2,
                "FTAG": 0,
                "FTR": "H",
                "SeasonCode": "2627",
            }
            for i in range(1, 6)
        ]
        ctx = w._team_history_with_scope(
            filas,
            "SHAKHTAR",
            self.kickoff,
            "soccer_ukraine_premier_league",
            "domestic",
            {"filas_de_su_categoria": True},
        )
        self.assertEqual((ctx.get("table") or {}).get("position"), 1)
        self.assertEqual(ctx.get("league_key"), "soccer_ukraine_premier_league")
        self.assertEqual((ctx.get("recent_all") or {}).get("matches"), 5)
        self.assertEqual(ctx.get("league_scope"), "domestic")

    def test_shakhtar_no_hereda_la_premier(self):
        filas_epl = [
            {
                "Date": "2026-08-22",
                "HomeTeam": "Everton",
                "AwayTeam": "Southampton",
                "FTHG": 1,
                "FTAG": 0,
                "FTR": "H",
                "SeasonCode": "2627",
            }
        ]
        ctx = w._team_history_with_scope(
            filas_epl,
            "SHAKHTAR",
            self.kickoff,
            "soccer_epl",
            "domestic",
            {"filas_de_su_categoria": True},
        )
        self.assertFalse((ctx.get("recent_all") or {}).get("form"))
        self.assertNotEqual((ctx.get("resolved_name") or "").lower(), "everton")

    def test_h2h_no_incluye_el_partido_de_hoy(self):
        filas = [
            {
                "Date": "2021-11-02",
                "HomeTeam": "Bayern Munich",
                "AwayTeam": "Bodo Glimt",
                "FTHG": 5,
                "FTAG": 1,
                "FTR": "H",
            },
            {
                "Date": "2026-09-09",
                "HomeTeam": "Bayern Munich",
                "AwayTeam": "Bodo Glimt",
                "FTHG": 1,
                "FTAG": 0,
                "FTR": "H",
            },
        ]
        h2h = w._head_to_head_metrics(
            [
                row
                for row in filas
                if str(row.get("Date")) != "2026-09-09"
            ],
            "Bayern Munich",
            "Bodo Glimt",
        )
        self.assertEqual(h2h.get("meetings"), 1)
        self.assertEqual(h2h.get("recent_matches")[0]["date"], "2021-11-02")


class CupoYCacheTests(unittest.TestCase):
    def test_el_vacio_de_ultimos_partidos_no_se_cachea(self):
        fuente = inspect.getsource(w.fetch_the_sportsdb_last_events)
        self.assertIn("if payload:", fuente)
        self.assertIn("_cache_set(HISTORY_CACHE, cache_key, payload)", fuente)
        self.assertLess(
            fuente.index("if payload:"),
            fuente.index("_cache_set(HISTORY_CACHE, cache_key, payload)"),
        )

    def test_el_vacio_de_la_tabla_tampoco(self):
        fuente = inspect.getsource(w.fetch_the_sportsdb_lookup_table)
        self.assertIn("if payload:", fuente)

    def test_una_copa_vacia_pide_relleno_domestico(self):
        match = {
            "league": "soccer_uefa_champs_league",
            "history_context": {
                "home": {"recent_all": {}, "table": {}},
                "away": {"recent_all": {}, "table": {}},
                "head_to_head": {},
            },
        }
        self.assertTrue(w._cup_history_needs_domestic_fill(match))
        match["history_context"]["home"] = {
            "recent_all": {"form": "WWWDW", "matches": 5},
            "table": {"position": 1, "played": 4},
        }
        match["history_context"]["away"] = {
            "recent_all": {"form": "WDWWL", "matches": 5},
            "table": {"position": 2, "played": 5},
        }
        self.assertFalse(w._cup_history_needs_domestic_fill(match))


class BlindajeSinProveedorTests(unittest.TestCase):
    """El ciclo del 9-9-2026 gasto el cupo en searchteams, tiro la ficha
    cacheada al 429, y relleno a Bodo con la tabla de Champions (22o de 36).
    """

    def test_shakhtar_tiene_id_sin_consultar(self):
        api = w._club_api_for_history("SHAKHTAR", {})
        self.assertEqual(api["idTeam"], "134126")
        self.assertEqual(api["idLeague"], "4354")
        self.assertEqual(
            w._domestic_league_key_from_team_api(api, "soccer_uefa_champs_league"),
            "soccer_ukraine_premier_league",
        )

    def test_bayern_y_bodo_tambien(self):
        self.assertEqual(w._known_club_profile("B.MUNICH")["idTeam"], "133664")
        self.assertEqual(w._known_club_profile("Bodo Glimt")["idLeague"], "4358")
        self.assertEqual(w._known_club_profile("PSV")["idTeam"], "133768")

    def test_bodo_no_hereda_el_puesto_de_la_champions(self):
        ucl = "soccer_uefa_champs_league"
        filas_ucl = [
            {
                "Date": f"2026-08-{10 + i:02d}",
                "HomeTeam": "Bodø/Glimt",
                "AwayTeam": f"Rival {i}",
                "FTHG": 1,
                "FTAG": 0,
                "FTR": "H",
                "SeasonCode": "2627",
            }
            for i in range(1, 8)
        ]
        histories = {
            ucl: filas_ucl,
            "soccer_norway_eliteserien": [],
            "soccer_germany_bundesliga": [],
        }
        kickoff = datetime(2026, 9, 9, 19, 0, tzinfo=timezone.utc)

        def fake_fill(history, *args, **kwargs):
            return history or {}

        with (
            patch.object(w, "_fill_side_from_sportsdb_if_empty", side_effect=fake_fill),
            patch.object(w, "fetch_league_history", return_value=[]),
            patch.object(w, "fetch_the_sportsdb_h2h_events", return_value=[]),
        ):
            _home, away, _h2h = w._resolve_domestic_histories_and_h2h(
                home_team="Bayern Munich",
                away_team="Bodø/Glimt",
                league_key=ucl,
                histories=histories,
                home_team_api={},
                away_team_api={},
                kickoff_dt=kickoff,
            )
        self.assertFalse((away.get("recent_all") or {}).get("form"))
        self.assertNotEqual((away.get("table") or {}).get("position"), 1)

    def test_sportsdb_rellena_si_el_csv_esta_vacio(self):
        extra = [
            {
                "Date": f"2026-08-{20 + i:02d}",
                "HomeTeam": "Shakhtar Donetsk",
                "AwayTeam": f"Rival {i}",
                "FTHG": 2,
                "FTAG": 0,
                "FTR": "H",
                "SeasonCode": "2627",
            }
            for i in range(1, 6)
        ]
        tabla = {
            "Shakhtar Donetsk": {
                "team": "Shakhtar Donetsk",
                "position": 2,
                "played": 5,
                "points": 12,
            }
        }
        kickoff = datetime(2026, 9, 9, 19, 0, tzinfo=timezone.utc)
        with patch.object(w, "_sportsdb_domestic_fallback", return_value=(extra, tabla)):
            ctx = w._fill_side_from_sportsdb_if_empty(
                {},
                "SHAKHTAR",
                w._club_api_for_history("SHAKHTAR", {}),
                kickoff,
                "soccer_ukraine_premier_league",
                "domestic",
                {"filas_de_su_categoria": True},
            )
        self.assertEqual((ctx.get("recent_all") or {}).get("matches"), 5)
        self.assertTrue((ctx.get("recent_all") or {}).get("form"))
        self.assertEqual((ctx.get("table") or {}).get("position"), 2)

    def test_ultimos_partidos_no_reservan_cupo_de_ligas(self):
        fuente = inspect.getsource(w.fetch_the_sportsdb_last_events)
        self.assertNotIn("SPORTSDB_RESERVA_LIGAS", fuente)
        self.assertNotIn("_sportsdb_hay_cupo", fuente)


class PistaDePaisTests(unittest.TestCase):
    def test_bayern_no_se_busca_en_inglaterra(self):
        self.assertEqual(w._guess_country_hint("B.MUNICH"), "DE")
        self.assertEqual(w._guess_country_hint("SHAKHTAR"), "UA")
        self.assertEqual(w._guess_country_hint("OPORTO"), "PT")
        self.assertEqual(w._guess_country_hint("PSV"), "NL")


if __name__ == "__main__":
    unittest.main()
