"""UEFA oficial (arbitro, jornada, ids, once), plantillas ESPN y onces ESPN."""
import os
import unittest
from datetime import datetime, timezone, timedelta
from unittest import mock

os.environ.setdefault("OPENAI_API_KEY", "test")
import calidad_europea as ce  # noqa: E402
import fuentes_espn as fe  # noqa: E402
import fuentes_uefa as fu  # noqa: E402
import snapshot_worker as w  # noqa: E402


def _eq(id_, nombre, pais, oficial=None, tipo="DOMESTIC_MEN_TEAM_A"):
    return {"id": id_, "internationalName": nombre, "countryCode": pais, "teamTypeDetail": tipo,
            "translations": {"displayName": {"EN": nombre}, "displayOfficialName": {"EN": oficial or nombre}}}


def _partido(id_, local, visitante, arbitro="Clément Turpin", md="MD4", lineup="NOT_AVAILABLE"):
    return {"id": id_, "homeTeam": local, "awayTeam": visitante, "status": "UPCOMING", "lineupStatus": lineup,
            "kickOffTime": {"dateTime": "2026-11-04T20:00:00Z"}, "matchday": {"name": md},
            "round": {"phase": "TOURNAMENT"},
            "referees": [{"role": "FOURTH_OFFICIAL", "person": {"translations": {"name": {"EN": "Otro Senor"}}}},
                         {"role": "REFEREE", "person": {"translations": {"name": {"EN": arbitro}}}}],
            "stadium": {"city": {"translations": {"name": {"EN": "Istanbul"}}}, "geolocation": {"latitude": 41.0, "longitude": 29.0}}}


DIA = [
    _partido("1", _eq("50", "Fenerbahçe", "TUR"), _eq("7889", "Liverpool", "ENG")),
    _partido("2", _eq("59", "Bodø/Glimt", "NOR"), _eq("52", "Lille", "FRA", "LOSC Lille"), arbitro="João Pinheiro"),
    _partido("3", _eq("60", "Lillestrøm", "NOR"), _eq("61", "Viktoria Plzeň", "CZE")),
    _partido("4", _eq("62", "Slavia Praha", "CZE"), _eq("63", "Arsenal", "ENG"), arbitro="Tobias Stieler"),
]


class ParseoUefaTests(unittest.TestCase):
    def setUp(self):
        self.lista = fu.partidos(DIA)

    def test_solo_el_arbitro_principal(self):
        self.assertEqual(self.lista[0]["referee"], "Clément Turpin")
        self.assertEqual(self.lista[0]["matchday"], 4)
        self.assertEqual(self.lista[0]["home"]["country"], "TR")

    def test_lille_nunca_es_lillestrom(self):
        p = fu.buscar_partido(self.lista, ["LILLE"], ["PLZEN"])
        self.assertEqual(p, {})
        p = fu.buscar_partido(self.lista, ["Bodo/Glimt", "BODO GLIMT (M)"], ["LILLE"])
        self.assertEqual(p["uefa_match_id"], "2")
        self.assertEqual(p["away"]["country"], "FR")

    def test_slavia_praga_y_prague(self):
        self.assertEqual(fu.buscar_partido(self.lista, ["Slavia Prague"], ["Arsenal"])["uefa_match_id"], "4")
        self.assertEqual(fu.buscar_partido(self.lista, ["SLAVIA PRAGA"], ["ARSENAL"])["uefa_match_id"], "4")

    def test_temporada(self):
        self.assertEqual(fu.temporada_uefa(2026, 11), 2027)
        self.assertEqual(fu.temporada_uefa(2027, 2), 2027)

    def test_once_oficial_solo_con_11(self):
        jug = lambda n: {"player": {"translations": {"name": {"EN": f"J{n}"}}, "fieldPosition": "MIDFIELDER"}}
        ok = {"lineupStatus": "TACTICAL_AVAILABLE",
              "homeTeam": {"field": [jug(i) for i in range(11)], "coaches": [{"person": {"translations": {"name": {"EN": "Hansi Flick"}}}}]},
              "awayTeam": {"field": [jug(i) for i in range(11)]}}
        a = fu.alineacion(ok)
        self.assertEqual(a["status"], "confirmed")
        self.assertEqual(len(a["home"]["starters"]), 11)
        self.assertEqual(a["home"]["coach"], "Hansi Flick")
        self.assertEqual(fu.alineacion({**ok, "lineupStatus": "NOT_AVAILABLE"}), {})
        corto = {**ok, "awayTeam": {"field": [jug(i) for i in range(10)]}}
        self.assertEqual(fu.alineacion(corto), {})


class AplicarUefaTests(unittest.TestCase):
    def _m(self, local="FENERBAHÇE (M)", visitante="LIVERPOOL (M)", liga="soccer_uefa_champs_league"):
        return {"local": local, "visitante": visitante, "league": liga, "kickoff": "2026-11-04T20:00:00Z",
                "competition_context": {"european_matchday": {"matchday": 5}},
                "structured_context": {"referee_context": {"assigned_referee": "", "source": ""}}}

    def test_arbitro_jornada_y_pais(self):
        m = self._m()
        with mock.patch.object(w, "_uefa_partidos_del_dia", return_value=fu.partidos(DIA)):
            cambios = w._aplicar_fuente_uefa(m, datetime(2026, 11, 4, 9, tzinfo=timezone.utc))
        self.assertTrue(cambios)
        ref = m["structured_context"]["referee_context"]
        self.assertEqual((ref["assigned_referee"], ref["source"]), ("Clément Turpin", "uefa"))
        self.assertFalse(ce.sanear_arbitro(ref))
        self.assertEqual(m["competition_context"]["european_matchday"]["matchday"], 4)
        self.assertEqual(w._pais_del_lado(m, "away"), "GB")
        self.assertNotIn("lineup_context", m["structured_context"])  # lejos del inicio

    def test_once_cerca_del_inicio(self):
        dia = fu.partidos([_partido("1", _eq("50", "Fenerbahçe", "TUR"), _eq("7889", "Liverpool", "ENG"), lineup="TACTICAL_AVAILABLE")])
        m = self._m()
        once = {"status": "confirmed", "source": "uefa", "home": {"starters": []}, "away": {"starters": []}}
        with mock.patch.object(w, "_uefa_partidos_del_dia", return_value=dia), \
             mock.patch.object(w, "_uefa_alineacion", return_value=once):
            w._aplicar_fuente_uefa(m, datetime(2026, 11, 4, 19, tzinfo=timezone.utc))
        self.assertEqual(m["structured_context"]["lineup_context"]["status"], "confirmed")

    def test_laliga_y_femenino_no_se_tocan(self):
        with mock.patch.object(w, "_uefa_partidos_del_dia") as pedir:
            self.assertEqual(w._aplicar_fuente_uefa(self._m("BETIS", "SEVILLA", "soccer_spain_la_liga")), [])
            self.assertEqual(w._aplicar_fuente_uefa(self._m("BARCELONA (F)", "CHELSEA (F)", "soccer_uefa_womens_champions_league")), [])
            pedir.assert_not_called()

    def test_sin_partido_unico_no_hace_nada(self):
        m = self._m("LILLE (M)", "PLZEN (M)")
        with mock.patch.object(w, "_uefa_partidos_del_dia", return_value=fu.partidos(DIA)):
            self.assertEqual(w._aplicar_fuente_uefa(m), [])
        self.assertEqual(m["structured_context"]["referee_context"]["assigned_referee"], "")


class GolesEnLaTablaTests(unittest.TestCase):
    def test_guia_con_goles(self):
        t = {f"T{i}": {"team": f"T{i}", "played": 3, "position": i + 1, "points": 4, "goals_for": 5, "goals_against": 2} for i in range(36)}
        b = ce.fase_liga_desde_tabla(t, t["T0"], t["T1"], "soccer_uefa_champs_league")
        self.assertIn("(goles 5-2)", b["guidance"])


class OnceEspnTests(unittest.TestCase):
    def _roster(self, lado, n):
        return {"homeAway": lado, "team": {"displayName": lado}, "roster": [
            {"starter": i < n, "athlete": {"displayName": f"{lado}{i}"}, "position": {"abbreviation": "M"}} for i in range(18)]}

    def test_once_confirmado(self):
        a = fe.alineacion_de_resumen({"rosters": [self._roster("home", 11), self._roster("away", 11)]})
        self.assertEqual(a["status"], "confirmed")
        self.assertEqual(len(a["away"]["starters"]), 11)

    def test_sin_once_publicado_nada(self):
        self.assertEqual(fe.alineacion_de_resumen({"rosters": [self._roster("home", 0), self._roster("away", 0)]}), {})
        self.assertEqual(fe.alineacion_de_resumen({"rosters": [self._roster("home", 11), self._roster("away", 9)]}), {})

    def test_ventana(self):
        ko = datetime(2026, 11, 8, 20, tzinfo=timezone.utc)
        self.assertTrue(w._en_ventana_de_alineacion(ko, ko - timedelta(hours=1)))
        self.assertFalse(w._en_ventana_de_alineacion(ko, ko - timedelta(hours=8)))


class PlantillaEspnTests(unittest.TestCase):
    INDICE = {"villarreal": ["102", "esp.1"], "athletic club": ["93", "esp.1"], "lille": ["166", "uefa.champions"],
              "real valladolid": ["95", "esp.2"]}

    def test_identidad_por_igualdad(self):
        with mock.patch.object(w, "_espn_indice_de_equipos", return_value=self.INDICE):
            self.assertEqual(w._espn_equipo_y_liga("Lille"), ["166", "uefa.champions"])
            self.assertEqual(w._espn_equipo_y_liga("Lillestrom"), [])
            self.assertEqual(w._espn_equipo_y_liga("Athletic Bilbao"), ["93", "esp.1"])
            self.assertEqual(w._espn_equipo_y_liga("Real Valladolid CF"), ["95", "esp.2"])

    def test_femenino_no_pide_plantilla(self):
        with mock.patch.object(w, "_request_json") as pedir:
            self.assertEqual(w._plantilla_espn("VALENCIA (F)"), [])
            pedir.assert_not_called()

    def test_bajas_guardadas_se_verifican_o_se_quitan(self):
        w._INDICE_DE_JUGADORES.clear()
        w._PLANTILLAS_PEDIDAS.clear()
        plantillas = {"VILLARREAL": ["Gerard Moreno", "Pape Gueye"], "ESPANYOL": ["Javi Puado"]}
        with mock.patch.object(w, "_plantilla_de_equipo", side_effect=lambda t: plantillas.get(t, [])):
            w._registrar_plantilla("ESPANYOL")
            m = {"local": "VILLARREAL", "visitante": "PSG", "structured_context": {"injury_context": {
                "home_team": {"items": [{"player_name": "Gerard Moreno", "verificado": False},
                                         {"player_name": "Javi Puado", "verificado": False},
                                         {"player_name": "Desconocido Uno", "verificado": False}]}}}}
            self.assertEqual(w._reverificar_bajas(m), 1)
        items = m["structured_context"]["injury_context"]["home_team"]["items"]
        self.assertEqual([i["player_name"] for i in items], ["Gerard Moreno", "Desconocido Uno"])
        self.assertTrue(items[0]["verificado"])
        self.assertFalse(items[1]["verificado"])
        w._INDICE_DE_JUGADORES.clear()
        w._PLANTILLAS_PEDIDAS.clear()


if __name__ == "__main__":
    unittest.main()
