"""Régimen de muestra de la clasificación al arrancar la temporada.

En jornada 1 todos los equipos están a 0-3 puntos de cualquier línea
competitiva y el orden de la tabla lo decide el desempate alfabético de
_table_snapshot. Sin estas barreras el feed publicaba cosas como
"persigue Europa League a 1 pts" con must_win_index 82/100, que es ruido
con tono de hecho verificado.

Esto vuelve cada agosto: si algún día estos tests fallan, el arreglo es
mirar por qué la tabla se ha vuelto a considerar utilizable, no relajarlos.
"""

import unittest
from datetime import datetime, timezone

import snapshot_worker as worker


LALIGA = "soccer_spain_la_liga"
KICKOFF_J1 = datetime(2026, 8, 16, tzinfo=timezone.utc)
KICKOFF_MEDIA = datetime(2027, 1, 17, tzinfo=timezone.utc)


def _table(played: int, teams: int = 20, points_step: int = 0) -> dict:
    """Tabla sintética con `teams` equipos y `played` jornadas disputadas."""
    rows = {}
    for index in range(teams):
        name = f"Equipo {index:02d}"
        rows[name] = {
            "team": name,
            "position": index + 1,
            "played": played,
            "points": max(0, (teams - index) * points_step),
            "goal_diff": 0,
            "goals_for": 0,
        }
    return rows


class SeasonStartGuardTests(unittest.TestCase):
    def test_matchday_one_table_is_not_usable_for_anything(self):
        reliability = worker._table_reliability(_table(played=1), LALIGA)
        self.assertEqual(reliability["regime"], "preseason")
        self.assertFalse(reliability["positions_usable"])
        self.assertFalse(reliability["objectives_usable"])

    def test_partial_matchday_is_caught_even_if_points_already_differ(self):
        # Solo han jugado los partidos del viernes: el que ganó figura líder
        # y el resto de la liga ni siquiera aparece en la tabla.
        reliability = worker._table_reliability(
            _table(played=1, teams=6, points_step=3), LALIGA
        )
        self.assertEqual(reliability["regime"], "preseason")
        self.assertIn("6 de 20", reliability["reason"])

    def test_short_sample_allows_positions_but_never_objectives(self):
        reliability = worker._table_reliability(
            _table(played=6, points_step=1), LALIGA
        )
        self.assertEqual(reliability["regime"], "early_sample")
        self.assertTrue(reliability["positions_usable"])
        self.assertFalse(reliability["objectives_usable"])

    def test_flat_table_is_rejected_however_many_matchdays_it_claims(self):
        reliability = worker._table_reliability(_table(played=20), LALIGA)
        self.assertEqual(reliability["regime"], "preseason")
        self.assertIn("no separa a nadie", reliability["reason"])

    def test_mid_season_table_keeps_the_full_regime(self):
        reliability = worker._table_reliability(
            _table(played=20, points_step=3), LALIGA
        )
        self.assertEqual(reliability["regime"], "normal")
        self.assertTrue(reliability["objectives_usable"])

    def test_no_objective_is_published_on_matchday_one(self):
        context = worker._season_competitive_context(
            LALIGA, _table(played=1), "Equipo 00", "Equipo 04", KICKOFF_J1
        )
        self.assertEqual(context["home_objective"], {})
        self.assertEqual(context["away_objective"], {})
        self.assertFalse(context["direct_rivalry"]["is_direct_rivalry"])

    def test_stakes_label_says_there_is_nothing_to_chase(self):
        context = worker._season_competitive_context(
            LALIGA, _table(played=1), "Equipo 00", "Equipo 04", KICKOFF_J1
        )
        label = context["competitive_stakes_label"].lower()
        self.assertIn("arranque de temporada", label)
        for prohibido in ("persigue", "defiende", "a 0 pts", "a 1 pts"):
            self.assertNotIn(prohibido, label)

    def test_objectives_come_back_in_mid_season(self):
        context = worker._season_competitive_context(
            LALIGA,
            _table(played=20, points_step=3),
            "Equipo 00",
            "Equipo 04",
            KICKOFF_MEDIA,
        )
        self.assertTrue(context["home_objective"].get("summary"))
        self.assertEqual(context["table_reliability"]["regime"], "normal")

    def test_team_objective_context_refuses_a_matchday_one_table(self):
        # Segunda barrera: llamadas directas que no pasan por
        # _season_competitive_context.
        objective = worker._team_objective_context(
            LALIGA, _table(played=1), "Equipo 00", KICKOFF_J1
        )
        self.assertEqual(objective.get("objective_candidates"), [])
        self.assertNotIn("summary", objective)
        self.assertNotIn("must_win_index", objective)

    def test_relegation_and_pressure_are_suppressed_without_sample(self):
        table = _table(played=1)
        relegation = worker._relegation_context(LALIGA, table, "Equipo 00")
        self.assertIs(relegation["available"], False)
        pressure = worker._pressure_index(table["Equipo 00"], relegation, {})
        self.assertIs(pressure["available"], False)

    def test_season_preview_replaces_the_standings_narrative(self):
        preview = worker._season_preview_context(
            LALIGA,
            "Equipo 00",
            "Equipo 04",
            KICKOFF_J1,
            worker._table_reliability(_table(played=1), LALIGA),
        )
        self.assertTrue(preview["active"])
        self.assertEqual(preview["reference_season_label"], "25/26")
        self.assertTrue(preview["transfer_window"]["open"])

    def test_previous_season_code_points_to_the_closed_season(self):
        self.assertEqual(worker._previous_season_code(LALIGA, KICKOFF_J1), "2526")
        self.assertEqual(worker._previous_season_code(LALIGA, KICKOFF_MEDIA), "2526")

    def test_season_context_phase_uses_the_real_league_size(self):
        # Con la tabla partida, len(table) daba total_rounds=30 en una liga de
        # 38 jornadas y el porcentaje de temporada salía inflado.
        phase = worker._season_context_phase(
            KICKOFF_J1, _table(played=1, teams=16), {"played": 1}, expected_teams=20
        )
        self.assertEqual(phase["total_rounds"], 38)

    def test_sportsdb_can_replace_an_unresolved_spanish_league(self):
        match = {"league": "league_unresolved", "league_source": "quiniela-placeholder"}
        worker._apply_dynamic_league_metadata(
            match,
            {"idLeague": "4335", "strLeague": "Spanish La Liga"},
        )
        self.assertEqual(match["league"], LALIGA)
        self.assertEqual(match["league_name"], "LaLiga")
        self.assertEqual(match["league_source"], "TheSportsDB")


class FormSampleTests(unittest.TestCase):
    def test_a_single_match_is_not_a_streak(self):
        self.assertIn("muestra insuficiente", worker._describe_form("W"))
        self.assertIn("muestra insuficiente", worker._describe_form("WD"))

    def test_five_matches_are_still_described_normally(self):
        self.assertEqual(worker._describe_form("WWWDL"), "Buena dinámica reciente")
        self.assertEqual(worker._describe_form("LLLDW"), "Mala racha reciente")


class LeagueHistoryDivisionTests(unittest.TestCase):
    """football-data ha servido P1 (Portugal) bajo la URL de SP1 26/27.

    Fiarse de la URL hacía que _resolve_csv_team_name emparejara LEVANTE con
    Gil Vicente y BETIS con Benfica, atribuyendo a un equipo el historial de
    otro. Los rangos de similitud se solapan (RAYO->Vallecano puntúa 0.31 y
    BETIS->Benfica 0.50), así que subir el umbral no separa los casos: hay
    que impedir que el pool de candidatos mezcle ligas.
    """

    def test_rows_from_another_division_are_dropped(self):
        rows = [
            {"Div": "SP1", "HomeTeam": "Levante", "AwayTeam": "Betis"},
            {"Div": "P1", "HomeTeam": "Gil Vicente", "AwayTeam": "Benfica"},
        ]
        kept = worker._rows_matching_division(rows, "SP1")
        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0]["HomeTeam"], "Levante")

    def test_division_column_is_read_through_any_bom_encoding(self):
        self.assertEqual(worker._row_division_code({"﻿Div": "P1"}), "P1")
        # BOM leído en latin-1: tres caracteres sueltos delante del nombre.
        self.assertEqual(worker._row_division_code({"\xef\xbb\xbfDiv": "P1"}), "P1")
        self.assertEqual(worker._row_division_code({"Div": "sp1"}), "SP1")

    def test_rows_without_division_column_are_kept(self):
        rows = [{"HomeTeam": "Kristiansund", "AwayTeam": "Start"}]
        self.assertEqual(len(worker._rows_matching_division(rows, "SP1")), 1)


class TeamGeolocationTests(unittest.TestCase):
    """Coordenadas incompatibles con el país declarado del equipo.

    El perfil tomaba las coordenadas de Wikipedia y el país del
    geocodificador sin comprobar que concordasen: "BETIS" resolvió al
    artículo "Betis Church" (Guagua, Pampanga, Filipinas) y se quedó con
    sus coordenadas junto a country_code "ES". El viaje Valencia-Sevilla
    salió a 11.421 km. "ALAVÉS (F)" hizo lo mismo con "Alaverdi, Armenia".
    """

    def test_foreign_coordinates_are_rejected_for_a_spanish_club(self):
        self.assertFalse(worker._coordinates_match_country(14.97558, 120.64289, "ES"))
        self.assertFalse(worker._coordinates_match_country(41.13333, 44.65, "ES"))

    def test_spanish_islands_and_enclaves_are_still_spain(self):
        for nombre, lat, lon in [
            ("Las Palmas", 28.1235, -15.4363),
            ("Tenerife", 28.4636, -16.2518),
            ("Mallorca", 39.5696, 2.6502),
            ("Menorca", 39.8885, 4.2658),
            ("Ceuta", 35.8894, -5.3213),
            ("Melilla", 35.2923, -2.9381),
            ("Sevilla", 37.3564, -5.9819),
        ]:
            with self.subTest(nombre=nombre):
                self.assertTrue(worker._coordinates_match_country(lat, lon, "ES"))

    def test_unknown_country_cannot_be_checked(self):
        self.assertIsNone(worker._coordinates_match_country(37.3, -5.9, "ZZ"))
        self.assertIsNone(worker._coordinates_match_country(None, None, "ES"))

    def test_inconsistent_coordinates_are_dropped_with_a_reason(self):
        perfil = {
            "team": "BETIS",
            "latitude": 14.97558,
            "longitude": 120.64289,
            "country_code": "ES",
            "city": "Tarifa",
        }
        limpio = worker._drop_inconsistent_coordinates(perfil, "BETIS")
        self.assertIsNone(limpio.get("latitude"))
        self.assertIsNone(limpio.get("longitude"))
        self.assertTrue(limpio.get("coordinates_rejected"))
        self.assertIn("fuera de ES", limpio["coordinates_rejected_reason"])

    def test_a_good_profile_is_left_untouched(self):
        perfil = {"latitude": 37.3564, "longitude": -5.9819, "country_code": "ES"}
        self.assertEqual(worker._drop_inconsistent_coordinates(perfil, "Betis"), perfil)

    def test_a_church_is_not_a_football_club(self):
        self.assertFalse(
            worker._wikipedia_page_is_a_football_entity(
                {"title": "Betis Church", "summary": "A Roman Catholic church in Guagua, Pampanga."}
            )
        )
        self.assertFalse(
            worker._wikipedia_page_is_a_football_entity(
                {"title": "Alaverdi, Armenia", "summary": "A town in the Lori Province of Armenia."}
            )
        )
        self.assertTrue(
            worker._wikipedia_page_is_a_football_entity(
                {"title": "Real Betis", "summary": "A Spanish professional football club in Seville."}
            )
        )

    def test_gender_suffix_does_not_break_the_location_override(self):
        # El equipo femenino del Alavés juega en Vitoria igual que el masculino.
        self.assertEqual(worker._strip_gender_suffix("ALAVÉS (F)"), "ALAVÉS")
        self.assertEqual(worker._strip_gender_suffix("VALENCIA (F)"), "VALENCIA")
        self.assertEqual(worker._strip_gender_suffix("LEVANTE"), "LEVANTE")
        for nombre, ciudad in [
            ("ALAVÉS (F)", "Vitoria-Gasteiz"),
            ("VALENCIA (F)", "Valencia"),
            ("BETIS", "Sevilla"),
        ]:
            with self.subTest(nombre=nombre):
                self.assertEqual(
                    worker._team_location_override(nombre).get("city"), ciudad
                )


class TravelDistanceTests(unittest.TestCase):
    LEVANTE = {"latitude": 39.4699, "longitude": -0.3763, "country_code": "ES", "country": "Spain"}
    BETIS_OK = {"latitude": 37.3564, "longitude": -5.9819, "country_code": "ES", "country": "España"}
    BETIS_FILIPINAS = {
        "latitude": 14.97558, "longitude": 120.64289, "country_code": "ES", "country": "España",
    }
    LAS_PALMAS = {"latitude": 28.1235, "longitude": -15.4363, "country_code": "ES", "country": "España"}
    OSLO = {"latitude": 59.91, "longitude": 10.75, "country_code": "NO", "country": "Norway"}

    def test_impossible_domestic_distance_is_not_published(self):
        contexto = worker._build_travel_context(
            self.LEVANTE, self.BETIS_FILIPINAS, "soccer_spain_la_liga"
        )
        self.assertIsNone(contexto["distance_km"])
        self.assertEqual(contexto["distance_bucket"], "unknown")
        self.assertIn("imposible", contexto["distance_rejected_reason"])

    def test_the_real_valencia_seville_trip_is_published(self):
        contexto = worker._build_travel_context(
            self.LEVANTE, self.BETIS_OK, "soccer_spain_la_liga"
        )
        self.assertAlmostEqual(contexto["distance_km"], 542, delta=25)
        self.assertNotIn("distance_rejected_reason", contexto)

    def test_a_canary_islands_trip_is_long_but_legitimate(self):
        contexto = worker._build_travel_context(
            self.LAS_PALMAS, self.LEVANTE, "soccer_spain_la_liga"
        )
        self.assertIsNotNone(contexto["distance_km"])
        self.assertGreater(contexto["distance_km"], 1500)

    def test_international_trips_are_never_capped(self):
        contexto = worker._build_travel_context(self.LEVANTE, self.OSLO, "")
        self.assertTrue(contexto["international_trip"])
        self.assertIsNotNone(contexto["distance_km"])

    def test_missing_coordinates_give_no_distance_instead_of_zero(self):
        contexto = worker._build_travel_context(self.LEVANTE, {}, "soccer_spain_la_liga")
        self.assertIsNone(contexto["distance_km"])
        self.assertEqual(contexto["distance_bucket"], "unknown")


class SeasonTransitionNewsTests(unittest.TestCase):
    def test_transfer_and_preseason_are_predictive_signals(self):
        self.assertEqual(
            worker._season_transition_category(
                "El Racing de Santander confirma el fichaje de un delantero"
            ),
            "signing",
        )
        self.assertEqual(
            worker._season_transition_category(
                "El Villarreal completa su pretemporada con un amistoso exigente"
            ),
            "preseason",
        )

    def test_departure_is_not_misclassified_as_a_signing(self):
        self.assertEqual(
            worker._season_transition_category(
                "El Racing confirma la salida de su capitan tras ser traspasado"
            ),
            "departure",
        )

    def test_confirmed_transfer_is_separated_from_a_rumour(self):
        self.assertEqual(
            worker._season_transition_fact_status(
                "El CE Sabadell anuncia el fichaje de Yanis Rahmani"
            ),
            "confirmed",
        )
        self.assertEqual(
            worker._season_transition_fact_status(
                "El Barcelona quiere el fichaje de un delantero del Villarreal"
            ),
            "rumour",
        )

    def test_racing_abbreviation_uses_the_full_news_identity(self):
        score = worker._team_relevance_score(
            "El Racing de Santander anuncia un nuevo refuerzo", "RACING S."
        )
        self.assertGreaterEqual(score, 0.9)

    def test_cadiz_transfer_is_not_attributed_to_racing(self):
        score = worker._team_relevance_score(
            "El Cadiz CF anuncia cuatro fichajes para la nueva temporada",
            "Real Racing Club de Santander",
        )
        self.assertEqual(score, 0.0)

    def test_ambiguous_racing_name_needs_santander(self):
        score = worker._team_relevance_score(
            "Racing Club confirma el fichaje de un delantero argentino",
            "Real Racing Club de Santander",
        )
        self.assertEqual(score, 0.0)

    def test_reserve_team_is_not_confused_with_first_team(self):
        self.assertEqual(worker._team_similarity_score("Celta Fortuna", "Celta"), 0.0)
        self.assertEqual(worker._lookup_table_row({"Celta": {"position": 6}}, "Celta Fortuna"), {})

    def test_opponent_coach_news_is_not_assigned_to_the_team(self):
        cadiz_item = {
            "title": "El Oviedo, proximo rival del Cadiz CF, tiene nuevo entrenador",
            "source": "Diario de Cadiz",
            "link": "https://example.test/oviedo",
        }
        celta_item = {
            "title": "Celades, nuevo entrenador del Cadiz antes de medirse al Celta Fortuna",
            "source": "Diario AS",
            "link": "https://example.test/cadiz",
        }
        self.assertFalse(worker._passes_season_transition_quality(cadiz_item, "CADIZ"))
        self.assertFalse(
            worker._passes_season_transition_quality(celta_item, "CELTA FORTUNA")
        )

    def test_social_hashtag_and_live_broadcast_are_not_squad_evidence(self):
        social_item = {
            "title": "Sacando los prohibidos #laliga #futbol #fichajes",
            "source": "Cadiz Club de Futbol",
            "link": "https://example.test/social",
        }
        live_item = {
            "title": "DIRECTO | Amistoso de pretemporada: Tenerife-Cadiz CF",
            "source": "Cadiz Club de Futbol",
            "link": "https://example.test/live",
        }
        self.assertFalse(worker._passes_season_transition_quality(social_item, "CADIZ"))
        self.assertFalse(worker._passes_season_transition_quality(live_item, "CADIZ"))

    def test_snapshot_audit_rejects_cross_team_evidence(self):
        snapshot = {
            "quiniela_focus_matches": [
                {
                    "local": "Real Racing Club de Santander",
                    "visitante": "Villarreal",
                    "competition_context": {
                        "season_transition": {
                            "home": {
                                "previous_season": {"summary": "ascendido"},
                                "all_evidence": [
                                    {"title": "El Cadiz anuncia cuatro fichajes"}
                                ],
                            },
                            "away": {
                                "previous_season": {"summary": "3o en Primera"},
                                "all_evidence": [],
                            },
                        }
                    },
                }
            ]
        }
        audit = worker._audit_season_transition_snapshot(snapshot)
        self.assertFalse(audit["ok"])
        self.assertEqual(audit["invalid_evidence_count"], 1)

    def test_snapshot_audit_accepts_previous_season_or_relevant_evidence(self):
        snapshot = {
            "quiniela_focus_matches": [
                {
                    "local": "Real Racing Club de Santander",
                    "visitante": "Villarreal",
                    "competition_context": {
                        "season_transition": {
                            "home": {
                                "previous_season": {"summary": "ascendido"},
                                "all_evidence": [],
                            },
                            "away": {
                                "previous_season": {},
                                "all_evidence": [
                                    {"title": "El Villarreal anuncia un nuevo fichaje"}
                                ],
                            },
                        }
                    },
                }
            ]
        }
        audit = worker._audit_season_transition_snapshot(snapshot)
        self.assertTrue(audit["ok"])
        self.assertEqual(audit["empty_side_count"], 0)

    def test_transition_context_keeps_previous_season_and_sourced_facts(self):
        item = {
            "title": "El Racing de Santander confirma un fichaje internacional",
            "source": "Cadena SER",
            "published_at": "2026-08-10T10:00:00+00:00",
            "link": "https://example.test/racing",
            "category": "signing",
            "fact_status": "confirmed",
            "evidence_quality": "high",
        }
        context = worker._build_team_season_transition(
            "Racing de Santander",
            {
                "status": "ascendido",
                "last_season_league": "Segunda Division",
                "last_season_position": 2,
                "last_season_points": 77,
                "summary": "ascendido: 2o con 77 pts en Segunda Division 25/26",
            },
            {"items": [item], "coverage": "partial", "lookback_days": 120},
        )
        briefing = worker._transition_briefing_side(context)
        self.assertIn("temporada anterior", context["summary"])
        self.assertEqual(briefing["temporada_anterior"]["situacion"], "ascendido")
        self.assertEqual(briefing["altas_y_refuerzos"][0]["fuente"], "Cadena SER")

    def test_no_news_does_not_claim_the_squad_is_unchanged(self):
        context = worker._build_team_season_transition("Equipo", {}, {"items": []})
        self.assertIn("no significa", context["summary"])


if __name__ == "__main__":
    unittest.main()
