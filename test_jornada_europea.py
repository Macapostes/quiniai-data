# -*- coding: utf-8 -*-
"""Contrato de una jornada europea: la competicion es del partido, no del club.

La jornada 5 2026/27 es toda Champions, fase liga MD1. El worker no puede:
- etiquetar Bayern-Bodo como Eliteserien porque Bodo es noruego
- etiquetar Dortmund-Villarreal como LaLiga porque el Villarreal es espanol
- fabricar una tabla de 48 equipos con previa
- hablar de descenso o puestos europeos
- tratar el propio partido como el proximo partido europeo que obliga a rotar
- publicar viajes de 10.000 km o 0.1 km Espana-Espana
"""

import unittest
from datetime import datetime, timezone

import snapshot_worker as w


UCL = "soccer_uefa_champs_league"
KICKOFF_MD1 = datetime(2026, 9, 8, 19, 0, tzinfo=timezone.utc)


class ClasificacionEuropeaTests(unittest.TestCase):
    def test_sportsdb_4480_es_champions(self):
        self.assertEqual(
            w._infer_league_key_from_sportsdb({"idLeague": "4480", "strLeague": "UEFA Champions League"}),
            UCL,
        )

    def test_fichas_de_ligas_distintas_no_eligen_ninguna(self):
        self.assertEqual(
            w._shared_team_league_key(
                {"idLeague": "4331", "strLeague": "German Bundesliga"},
                {"idLeague": "4358", "strLeague": "Norwegian Eliteserien"},
            ),
            "",
        )
        self.assertEqual(
            w._shared_team_league_key(
                {"idLeague": "4335"},
                {"idLeague": "4331"},
            ),
            "",
        )

    def test_cuotas_champions_no_se_bajan_a_eliteserien(self):
        match = {
            "local": "B.MUNICH",
            "visitante": "Bodo Glimt",
            "league": UCL,
            "league_source": "feed-de-cuotas",
        }
        w._apply_dynamic_league_metadata(
            match,
            {"idLeague": "4358", "strLeague": "Norwegian Eliteserien"},
            {"idLeague": "4331"},
            {"idLeague": "4358"},
        )
        self.assertEqual(match["league"], UCL)

    def test_evento_europeo_real_corrige_una_liga_domestica(self):
        match = {"local": "B.MUNICH", "visitante": "Bodo Glimt", "league": "soccer_norway_eliteserien"}
        w._apply_dynamic_league_metadata(
            match,
            {
                "idEvent": "123456",
                "idLeague": "4480",
                "strLeague": "UEFA Champions League",
            },
            {"idLeague": "4331"},
            {"idLeague": "4358"},
        )
        self.assertEqual(match["league"], UCL)

    def test_evento_fabricado_con_liga_de_un_equipo_se_ignora(self):
        match = {"local": "B.DORTMUND", "visitante": "VILLARREAL"}
        w._apply_dynamic_league_metadata(
            match,
            {"idLeague": "4335", "strLeague": "Spanish La Liga"},
            {"idLeague": "4331"},
            {"idLeague": "4335"},
        )
        self.assertNotEqual(match.get("league"), "soccer_spain_la_liga")
        self.assertFalse(str(match.get("league") or "").startswith("soccer_spain"))

    def test_b_munich_casa_con_bayern_en_cuotas_con_hora_placeholder(self):
        cuotas = [
            {
                "home_team": "Bayern Munich",
                "away_team": "Bodø/Glimt",
                "commence_time": "2026-09-10T19:00:00Z",
                "sport_key": UCL,
            }
        ]
        match = {
            "local": "B.MUNICH",
            "visitante": "Bodo Glimt",
            "kickoff": "2026-09-08T14:00:00Z",
        }
        self.assertEqual(w._competicion_desde_las_cuotas(match, cuotas), UCL)
        self.assertEqual(match.get("kickoff"), "2026-09-10T19:00:00Z")

    def test_find_match_by_teams_acepta_abreviaturas_lae(self):
        matches = [
            {"local": "Bayern Munich", "visitante": "Bodo Glimt", "league": UCL},
            {"local": "Borussia Dortmund", "visitante": "Villarreal", "league": UCL},
        ]
        found = w._find_match_by_teams(matches, "B.MUNICH", "Bodo Glimt")
        self.assertIsNotNone(found)
        self.assertEqual(found["local"], "Bayern Munich")
        found2 = w._find_match_by_teams(matches, "B.DORTMUND", "VILLARREAL")
        self.assertIsNotNone(found2)


class TablaYFormatoChampionsTests(unittest.TestCase):
    def test_36_equipos_y_8_jornadas(self):
        self.assertEqual(w.LEAGUE_EXPECTED_TEAMS[UCL], 36)
        self.assertEqual(w.LEAGUE_TOTAL_ROUNDS[UCL], 8)
        self.assertEqual(w._league_total_rounds({}, 48, UCL), 8)

    def test_previa_no_entra_en_la_tabla(self):
        rows = [
            {
                "Date": "2026-08-12",
                "HomeTeam": "Qarabag",
                "AwayTeam": "Ferencvaros",
                "FTHG": 1,
                "FTAG": 0,
                "FTR": "H",
                "SeasonCode": "2627",
                "Stage": "3rd Qualifying",
            },
            {
                "Date": "2026-08-19",
                "HomeTeam": "Qarabag",
                "AwayTeam": "Ferencvaros",
                "FTHG": 2,
                "FTAG": 1,
                "FTR": "H",
                "SeasonCode": "2627",
                "Round": "Play-off",
            },
        ]
        table = w._table_snapshot(rows, league_key=UCL)
        self.assertEqual(table, {})

    def test_tabla_vacia_es_fase_liga_no_iniciada(self):
        reliability = w._table_reliability({}, UCL, expected_teams=36)
        self.assertEqual(reliability["regime"], "preseason")
        self.assertIn("fase liga no iniciada", reliability["reason"])

    def test_stakes_no_hablan_de_descenso_ni_puestos_europeos(self):
        context = w._season_competitive_context(UCL, {}, "Bayern Munich", "Bodo Glimt", KICKOFF_MD1)
        label = context["competitive_stakes_label"].lower()
        self.assertNotIn("descenso", label)
        self.assertNotIn("puestos europeos", label)
        self.assertIn("fase liga", label)
        self.assertTrue(context.get("european_matchday", {}).get("active"))
        self.assertEqual(context["european_matchday"]["matchday"], 1)

    def test_no_hay_descenso_en_champions(self):
        relegation = w._relegation_context(UCL, {"Bayern Munich": {"position": 1, "points": 0}}, "Bayern Munich")
        self.assertFalse(relegation.get("available", True))

    def test_preview_de_villarreal_usa_laliga_como_domestica(self):
        preview = w._team_season_preview(UCL, "VILLARREAL", {}, "2526")
        self.assertEqual(preview.get("scope"), "domestic")
        self.assertIn("LaLiga", preview.get("summary", ""))
        self.assertIn("Champions", preview.get("summary", ""))


class ViajeYRotacionEuropeaTests(unittest.TestCase):
    def test_viaje_de_10000_km_se_descarta(self):
        roma_indonesia = {
            "latitude": -6.2,
            "longitude": 106.8,
            "country_code": "ID",
            "country": "Indonesia",
        }
        fener = {
            "latitude": 41.0,
            "longitude": 29.0,
            "country_code": "TR",
            "country": "Türkiye",
        }
        ctx = w._build_travel_context(fener, roma_indonesia, UCL)
        self.assertIsNone(ctx["distance_km"])
        self.assertIn("implausible", ctx.get("distance_rejected_reason", "").lower() + ctx.get("distance_rejected_reason", ""))

    def test_viaje_0km_mismo_pais_en_champions_se_descarta(self):
        fake_spain = {
            "latitude": 39.5,
            "longitude": -0.4,
            "country_code": "ES",
            "country": "España",
        }
        ctx = w._build_travel_context(fake_spain, dict(fake_spain), UCL)
        self.assertIsNone(ctx["distance_km"])

    def test_viaje_real_lille_betis_se_publica(self):
        lille = {
            "latitude": 50.61,
            "longitude": 3.13,
            "country_code": "FR",
            "country": "France",
        }
        betis = {
            "latitude": 37.36,
            "longitude": -5.98,
            "country_code": "ES",
            "country": "España",
        }
        ctx = w._build_travel_context(lille, betis, UCL)
        self.assertTrue(ctx["international_trip"])
        self.assertIsNotNone(ctx["distance_km"])
        self.assertGreater(ctx["distance_km"], 1000)

    def test_el_propio_partido_no_es_rotacion(self):
        fixtures = [
            {
                "kickoff": "2026-09-08T19:00:00Z",
                "opponent": "Borussia Dortmund",
                "league": UCL,
            }
        ]
        rotation = w._rotation_context_from_upcoming(
            "VILLARREAL",
            fixtures,
            "2026-09-08T14:00:00Z",
            {},
            opponent="B.DORTMUND",
        )
        self.assertNotIn("Borussia Dortmund", rotation.get("reason") or "")
        self.assertEqual(rotation.get("risk"), "low")


class HistoricoNoEtiquetaCopasTests(unittest.TestCase):
    def test_historico_domestico_sigue_valiendo(self):
        filas = [
            {"HomeTeam": "Kristiansund", "AwayTeam": "Bodo Glimt"},
            {"HomeTeam": "Bodo Glimt", "AwayTeam": "Rosenborg"},
        ]
        self.assertEqual(
            w._infer_league_from_histories(
                "Bodo Glimt",
                "Kristiansund",
                {"soccer_norway_eliteserien": filas},
            ),
            "soccer_norway_eliteserien",
        )

    def test_historico_de_champions_no_etiqueta(self):
        filas = [
            {"HomeTeam": "Real Betis", "AwayTeam": "Lille"},
            {"HomeTeam": "Deportivo", "AwayTeam": "Arsenal"},
        ]
        self.assertEqual(
            w._infer_league_from_histories(
                "DEPORTIVO",
                "BETIS",
                {"soccer_uefa_champs_league": filas},
            ),
            "",
        )

    def test_una_champions_colgada_por_historico_se_olvida(self):
        match = {
            "local": "DEPORTIVO",
            "visitante": "BETIS",
            "league": "soccer_uefa_champs_league",
            "league_source": "history-team-membership",
            "league_id": "4480",
        }
        self.assertTrue(w._needs_dynamic_league_revalidation(match))
        self.assertTrue(w._forget_cup_league_inferred_from_history(match))
        self.assertEqual(match.get("league"), "")
        self.assertEqual(match.get("league_source"), "")

    def test_una_ficha_sola_no_cuelga_colombia_a_deportivo_betis(self):
        match = {"local": "DEPORTIVO", "visitante": "BETIS"}
        w._apply_dynamic_league_metadata(
            match,
            {},
            {"idLeague": "4497", "strLeague": "Colombia Categoría Primera A"},
            {},
        )
        self.assertNotEqual(match.get("league"), "sportsdb_4497")
        self.assertFalse(str(match.get("league") or "").startswith("sportsdb_4497"))

    def test_sportsdb_desconocido_no_es_liga_de_confianza(self):
        self.assertFalse(w._is_trusted_resolved_league("sportsdb_4497"))
        self.assertTrue(w._is_trusted_resolved_league("soccer_spain_la_liga"))
        self.assertTrue(w._is_trusted_resolved_league("soccer_uefa_champs_league"))
        self.assertFalse(w._is_trusted_resolved_league("sportsdb_5106"))

    def test_una_champions_de_cuotas_no_se_olvida(self):
        match = {
            "local": "B.DORTMUND",
            "visitante": "VILLARREAL",
            "league": "soccer_uefa_champs_league",
            "league_source": "odds-snapshot",
        }
        self.assertFalse(w._needs_dynamic_league_revalidation(match))
        self.assertFalse(w._forget_cup_league_inferred_from_history(match))
        self.assertEqual(match["league"], "soccer_uefa_champs_league")

    def test_liga_domestica_gana_aunque_tambien_esten_en_champions(self):
        self.assertEqual(
            w._infer_league_from_histories(
                "SEVILLA",
                "BARCELONA",
                {
                    "soccer_uefa_champs_league": [
                        {"HomeTeam": "Sevilla", "AwayTeam": "Dortmund"},
                        {"HomeTeam": "Barcelona", "AwayTeam": "Bayern"},
                    ],
                    "soccer_spain_la_liga": [
                        {"HomeTeam": "Sevilla", "AwayTeam": "Barcelona"},
                    ],
                },
            ),
            "soccer_spain_la_liga",
        )


class RelevanciaDeNoticiasEuropeasTests(unittest.TestCase):
    def test_zira_fk_no_es_sabah_fk(self):
        self.assertEqual(
            w._team_relevance_score("Zira FK - Transfermarkt", "Sabah FK"),
            0.0,
        )
        self.assertFalse(
            w._passes_season_transition_quality(
                {"title": "Zira FK - Transfermarkt", "source": "Transfermarkt"},
                "Sabah FK",
            )
        )


class BriefingEuropeoTests(unittest.TestCase):
    def test_briefing_de_md1_no_habla_de_puestos_europeos(self):
        match = {
            "local": "B.DORTMUND",
            "visitante": "VILLARREAL",
            "league": UCL,
            "league_name": "UEFA Champions League",
            "market_context": {"normalized_percent": {"1": 54, "X": 25, "2": 21}, "source": "odds"},
            "history_context": {"home": {"recent_all": {}}, "away": {"recent_all": {}}, "head_to_head": {}},
            "competition_context": {
                "competitive_stakes_label": (
                    "fase liga de UEFA Champions League: fase liga no iniciada. "
                    "No hay octavos, play-off ni eliminacion que perseguir."
                ),
                "european_matchday": {
                    "active": True,
                    "competition": "UEFA Champions League",
                    "format": "fase liga: 36 equipos, 8 jornadas; 1-8 octavos, 9-24 play-off, 25-36 eliminados",
                    "matchday": 1,
                    "table_available": False,
                    "guidance": "La clasificacion de esta copa aun no existe.",
                },
                "home_rotation_context": {},
                "away_rotation_context": {},
                "season_transition": {},
            },
            "home_team_context": {},
            "away_team_context": {},
            "structured_context": {},
        }
        briefing = w._focus_match_ai_briefing(match)
        texto = str(briefing.get("contexto_deportivo", {}).get("contexto_competitivo", "")).lower()
        self.assertNotIn("descenso", texto)
        self.assertNotIn("puestos europeos", texto)
        self.assertIn("champions", texto)


if __name__ == "__main__":
    unittest.main()
