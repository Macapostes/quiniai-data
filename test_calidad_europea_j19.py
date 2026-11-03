"""Errores de la auditoria de la J19 (Champions, 3-5 nov 2026)."""
import unittest
from datetime import datetime, timezone
from unittest import mock

import calidad_europea as ce
import snapshot_worker as w


class ArbitroTests(unittest.TestCase):
    def test_un_titular_no_es_una_designacion(self):
        noticias = [{"title": "Lamine Yamal va por un récord de Mbappé en Champions League - Soy Referee",
                     "source": "Soy Referee", "link": ""}]
        with mock.patch.object(w, "_fetch_article_referee_candidate", return_value=""), \
             mock.patch.object(w, "_extract_rfef_officials", return_value={}):
            ctx = w._extract_referee_assignment("soccer_uefa_champs_league", "2026-11-03T20:00:00Z",
                                                "Barcelona", "Aston Villa", noticias, {})
        self.assertEqual(ctx["assigned_referee"], "")

    def test_thesportsdb_y_rfef_si_valen(self):
        with mock.patch.object(w, "_extract_rfef_officials", return_value={}):
            ctx = w._extract_referee_assignment("soccer_spain_la_liga", "2026-11-08T20:00:00Z",
                                                "Sevilla", "Betis", [], {"strOfficial": "Jose Sanchez Martinez"})
        self.assertEqual(ctx["assigned_referee"], "Jose Sanchez Martinez")
        rfef = {"assigned_referee": "Gil Manzano", "source": "rfef"}
        self.assertFalse(ce.sanear_arbitro(rfef))
        self.assertEqual(rfef["assigned_referee"], "Gil Manzano")

    def test_lo_guardado_de_prensa_se_retira(self):
        m = {"league": "soccer_uefa_champs_league", "structured_context": {
            "referee_context": {"assigned_referee": "Lamine Yamal", "source": "news"}}}
        w._sanear_partido(m)
        self.assertEqual(m["structured_context"]["referee_context"]["assigned_referee"], "")


class JugadorasTests(unittest.TestCase):
    def setUp(self):
        self._prev = w._JUGADORAS_LIGA_F
        w._JUGADORAS_LIGA_F = {"barcelona": {"kika nazareth", "alexia putellas"}}

    def tearDown(self):
        w._JUGADORAS_LIGA_F = self._prev

    def test_lesion_de_una_jugadora_no_va_al_masculino(self):
        t = "Lesión Kika Nazareth: comunicado médico del Barcelona, qué tiene y cuántos partidos se perderá - SPORT"
        self.assertTrue(w._titular_de_otra_categoria(t, "Barcelona"))

    def test_lesion_del_masculino_se_queda(self):
        self.assertFalse(w._titular_de_otra_categoria("Lesión de Raphinha: parte médico del Barcelona", "Barcelona"))
        self.assertFalse(w._titular_de_otra_categoria("Kika Nazareth: parte médico", "Sevilla"))


class FaseLigaTests(unittest.TestCase):
    TABLA = {
        "Atlético Madrid": {"team": "Atlético Madrid", "played": 3, "points": 4, "position": 16, "espn_team_id": "1068"},
        "Bayern Munich": {"team": "Bayern Munich", "played": 3, "points": 5, "position": 14, "espn_team_id": "132"},
        "Bodo/Glimt": {"team": "Bodo/Glimt", "played": 3, "points": 3, "position": 28},
    }

    def test_jornada_y_disputadas_de_la_tabla(self):
        b = ce.fase_liga_desde_tabla(self.TABLA, self.TABLA["Atlético Madrid"], self.TABLA["Bayern Munich"],
                                     "soccer_uefa_champs_league")
        self.assertEqual((b["matchday"], b["matchdays_played"]), (4, 3))
        self.assertIn("16º con 4 pts", b["guidance"])

    def test_el_worker_corrige_la_jornada_12(self):
        m = {"local": "Atlético Madrid", "visitante": "Bayern Munich", "league": "soccer_uefa_champs_league",
             "kickoff": "2026-11-03T20:00:00Z", "structured_context": {"event_context": {"round": "12"}},
             "competition_context": {"season_context_phase": {"played": 7, "total_rounds": 158}},
             "history_context": {"home": {}, "away": {}}}
        forma = {"matches": 5, "form": "WWDWW", "points": 13}
        with mock.patch.object(w, "_espn_clasificacion", return_value=self.TABLA), \
             mock.patch.object(w, "_forma_todas_las_competiciones", return_value=forma):
            w._fase_liga_uefa_con_espn(m, "uefa.champions", datetime(2026, 11, 3, 12, tzinfo=timezone.utc))
        self.assertEqual(m["structured_context"]["event_context"]["round"], "4")
        self.assertEqual(m["competition_context"]["season_context_phase"]["played"], 3)
        self.assertEqual(m["competition_context"]["european_matchday"]["matchday"], 4)
        self.assertEqual(m["history_context"]["home"]["recent_all"]["points"], 13)

    def test_forma_cuenta_bien_los_puntos(self):
        filas = [
            {"HomeTeam": "Atlético Madrid", "AwayTeam": "X", "FTR": "H", "FTHG": 2, "FTAG": 0, "KickoffUTC": "2026-10-13T19:00:00Z"},
            {"HomeTeam": "Y", "AwayTeam": "Atlético Madrid", "FTR": "A", "FTHG": 0, "FTAG": 1, "KickoffUTC": "2026-10-17T19:00:00Z"},
            {"HomeTeam": "Z", "AwayTeam": "Atlético Madrid", "FTR": "D", "FTHG": 1, "FTAG": 1, "KickoffUTC": "2026-10-20T19:00:00Z"},
            {"HomeTeam": "Atlético Madrid", "AwayTeam": "D", "FTR": "H", "FTHG": 1, "FTAG": 0, "KickoffUTC": "2026-10-24T19:00:00Z"},
            {"HomeTeam": "L", "AwayTeam": "Atlético Madrid", "FTR": "A", "FTHG": 0, "FTAG": 2, "KickoffUTC": "2026-10-31T19:00:00Z"},
        ]
        with mock.patch.object(w, "_espn_calendario_todas", return_value=filas):
            f = w._forma_todas_las_competiciones({"espn_team_id": "1068", "team": "Atlético Madrid"},
                                                 "uefa.champions", w._parse_iso_datetime("2026-11-03T20:00:00Z"))
        self.assertEqual((f["form"], f["points"]), ("WWDWW", 13))


class ProximosTests(unittest.TestCase):
    def test_otra_copa_uefa_es_de_un_homonimo(self):
        self.assertTrue(ce.proximo_de_otra_copa_uefa("soccer_uefa_europa_league", "soccer_uefa_champs_league"))
        self.assertFalse(ce.proximo_de_otra_copa_uefa("soccer_uefa_champs_league", "soccer_uefa_champs_league"))
        # En LaLiga no se toca: el club puede jugar cualquier copa.
        self.assertFalse(ce.proximo_de_otra_copa_uefa("soccer_uefa_europa_league", "soccer_spain_la_liga"))

    def test_lille_no_juega_la_europa_league(self):
        m = {"local": "Bodø/Glimt", "visitante": "Lille", "league": "soccer_uefa_champs_league",
             "kickoff": "2026-11-03T20:00:00Z", "competition_context": {
                 "home_upcoming": [],
                 "away_upcoming": [
                     {"opponent": "Viktoria Plzeň", "league": "soccer_uefa_europa_league", "venue": "home",
                      "kickoff": "2026-11-05T17:45:00Z", "date": "2026-11-05"},
                     {"opponent": "Aalesund", "league": "soccer_france_ligue_one", "venue": "home",
                      "kickoff": "2026-11-08T16:00:00Z", "date": "2026-11-08"},
                     {"opponent": "Viejo", "league": "soccer_france_ligue_one", "venue": "home",
                      "kickoff": "2026-10-20T16:00:00Z", "date": "2026-10-20"},
                 ],
                 "away_rotation_context": {"risk": "high", "next_high_importance_fixture": {
                     "opponent": "Viktoria Plzeň", "league": "soccer_uefa_europa_league",
                     "kickoff": "2026-11-05T17:45:00Z", "date": "2026-11-05", "venue": "home"}}}}
        w._proximos_con_su_competicion(m)
        rivales = [f["opponent"] for f in m["competition_context"]["away_upcoming"]]
        self.assertEqual(rivales, ["Aalesund"])
        self.assertNotIn("Plze", str(m["competition_context"]["away_rotation_context"]))


class ViajeYMeteoTests(unittest.TestCase):
    def test_union_sg_no_esta_en_eeuu(self):
        self.assertTrue(ce.viaje_inverosimil_uefa(6959.5, "Spain", "United States"))
        self.assertTrue(ce.viaje_inverosimil_uefa(7708, "France", "Italy"))
        self.assertEqual(ce.viaje_inverosimil_uefa(1036, "Czechia", "England"), "")
        self.assertEqual(ce.viaje_inverosimil_uefa(4800, "Portugal", "Azerbaijan"), "")

    def test_el_saneado_quita_el_viaje_imposible(self):
        m = {"league": "soccer_uefa_europa_league", "kickoff": "2026-11-05T20:00:00Z",
             "travel_context": {"distance_km": 6959.5, "home_country": "Spain", "away_country": "United States",
                                "international_trip": True}}
        w._sanear_partido(m)
        self.assertIsNone(m["travel_context"]["distance_km"])

    def test_laliga_no_se_toca(self):
        m = {"league": "soccer_spain_la_liga", "kickoff": "2026-11-08T20:00:00Z",
             "travel_context": {"distance_km": 530.0, "home_country": "Spain", "away_country": "Spain"},
             "weather_context": {"timezone": "Europe/Madrid", "temperature_c": 14.0},
             "structured_context": {"referee_context": {"assigned_referee": "Gil Manzano", "source": "rfef"}}}
        self.assertEqual(w._sanear_partido(m), [])
        self.assertEqual(m["travel_context"]["distance_km"], 530.0)

    def test_meteo_de_otro_continente(self):
        self.assertTrue(ce.meteo_inverosimil({"timezone": "America/Puerto_Rico", "temperature_c": 23.6}, "Spain", 11))
        self.assertTrue(ce.meteo_inverosimil({"timezone": "Europe/Madrid", "temperature_c": 23.6}, "Spain", 11))
        self.assertEqual(ce.meteo_inverosimil({"timezone": "Europe/Madrid", "temperature_c": 11.0}, "Spain", 11), "")
        self.assertEqual(ce.meteo_inverosimil({"timezone": "Europe/Madrid", "temperature_c": 30.0}, "Spain", 8), "")


if __name__ == "__main__":
    unittest.main()
