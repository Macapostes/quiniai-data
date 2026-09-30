"""La fase y "que se juega" de una competicion por grupos salen de SU grupo.

J11 (30-sep-2026), partido 15 España - Chequia (Nations League), en produccion:
  "fase liga europea (1 jornada disputada): la clasificación de esta copa aún no
   informa. No hay octavos, play-off ni eliminación que perseguir"
Salia de una tabla unica con las 54 selecciones y del texto de la Champions. En
el grupo A3 de ESPN ese dia, tras 2 jornadas: España 1ª (6), Inglaterra 2ª (3),
Croacia 3ª (3), Chequia 4ª (0).

Formato 2026-27 verificado con UEFA ("Promotion and relegation between the
2026/27 and 2028/29 editions", 15-09-2026): en la Liga A 1º y 2º van a cuartos,
los 2 peores 3º y los 2 mejores 4º al play-off A/B y los 2 peores 4º bajan.
"""

import copy
import os
import unittest
from unittest import mock

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import snapshot_worker as w  # noqa: E402
from test_proximos_de_su_competicion import ESPN_NATIONS, cadiz_leganes, espana_chequia  # noqa: E402

ESPERADO_J11_15 = (
    "Nations League 2026-27, Liga A, grupo A3 (4 selecciones, 6 partidos cada una) tras 2 jornadas: "
    "local Spain 1º (6 pts, 2 jugados, le quedan 4, 3 pts sobre el 3º): puesto de cuartos; "
    "visitante Czech Republic 4º (0 pts, 2 jugados, le quedan 4, a 3 pts del 2º): "
    "play-off A/B o descenso directo, segun la comparacion entre cuartos de los 4 grupos. "
    "Formato: en la Liga A, 1º y 2º de cada grupo van a cuartos (marzo 2027); los 2 mejores 3º "
    "de los cuatro grupos siguen en la A y los 2 peores juegan el play-off A/B; los 2 mejores 4º "
    "juegan el play-off A/B y los 2 peores descienden a la Liga B"
)


def con_lo_de_la_tabla_de_54(m):
    """Lo que el ciclo habia calculado con la tabla unica antes de este paso."""
    cc = m["competition_context"]
    cc.update(
        {
            "season_context_phase": {"key": "early", "label": "tramo temprano", "played": 1, "total_rounds": 106, "source": "played_matches"},
            "competitive_stakes_label": (
                "sin clasificacion util: arranque de temporada: 2 jornadas disputadas. "
                "No hay ni lider, ni descenso, ni puestos europeos que perseguir"
            ),
            "home_objective": {"summary": "persigue titulo"},
            "away_objective": {"summary": "evita descenso"},
            "home_relegation": {"available": True, "gap": 3},
            "table_reliability": {"regime": "preseason", "teams_ranked": 54},
            "season_preview": {
                "active": True,
                "home": {"team": "Spain", "summary": "sin registro en 25/26"},
                "away": {"team": "Czech Republic", "summary": "sin registro en 25/26"},
            },
        }
    )
    m["match_signals"] = {"competitive_stakes_label": cc["competitive_stakes_label"], "season_context_phase": "early", "home_must_win_index": 40}
    m["focus_ai_briefing"] = {
        "contexto_deportivo": {"fase_temporada": "tramo temprano", "contexto_competitivo": "x", "objetivo_local": "persigue titulo"},
        "contexto_competitivo_avanzado": {"competitive_stakes_label": "x", "home_objective": {"summary": "persigue titulo"}},
        "plantillas_y_transicion_de_temporada": {"local": {"a": 1}},
    }
    return m


class _ConEspn(unittest.TestCase):
    SLUGS = ("uefa.nations", "fifa.worldq.uefa")

    def setUp(self):
        for slug in self.SLUGS:
            w.EXTERNAL_FEEDS_CACHE.pop(f"espn:grupos:v1:{slug}", None)
        self.datos = copy.deepcopy(ESPN_NATIONS)

        def falso(url, params=None, timeout=30):
            if any(f"{slug}/standings" in url for slug in self.SLUGS):
                return copy.deepcopy(self.datos)
            raise AssertionError(url)

        self.patches = [
            mock.patch.object(w, "_request_json", side_effect=falso),
            mock.patch.object(w, "ESPN_ENABLED", True),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()
        for slug in self.SLUGS:
            w.EXTERNAL_FEEDS_CACHE.pop(f"espn:grupos:v1:{slug}", None)


class NationsLeagueTests(_ConEspn):
    def partido(self):
        m = con_lo_de_la_tabla_de_54(espana_chequia())
        w._proximos_con_su_competicion(m)
        return m

    def test_j11_15_texto_completo(self):
        m = self.partido()
        self.assertEqual(m["competition_context"]["competitive_stakes_label"], ESPERADO_J11_15)

    def test_nada_del_texto_de_champions_ni_de_liga(self):
        m = self.partido()
        texto = m["competition_context"]["competitive_stakes_label"].lower()
        for malo in ("octavos", "fase liga europea", "1 jornada", "puestos europeos", "lider", "titulo"):
            self.assertNotIn(malo, texto)

    def test_bloque_estructurado(self):
        fase = self.partido()["competition_context"]["group_phase"]
        self.assertTrue(fase["format_verified"])
        self.assertEqual((fase["league"], fase["group"], fase["group_size"], fase["games_per_team"]), ("A", "A3", 4, 6))
        self.assertEqual(fase["matchdays_played"], 2)
        self.assertEqual(fase["source"], "espn-standings")
        self.assertEqual(
            {k: fase["home"][k] for k in ("position", "points", "played", "remaining", "points_over_next")},
            {"position": 1, "points": 6, "played": 2, "remaining": 4, "points_over_next": 3},
        )
        self.assertEqual(
            {k: fase["away"][k] for k in ("position", "points", "played", "remaining", "points_to_cut")},
            {"position": 4, "points": 0, "played": 2, "remaining": 4, "points_to_cut": 3},
        )

    def test_fase_con_las_jornadas_del_grupo(self):
        fase = self.partido()["competition_context"]["season_context_phase"]
        self.assertEqual(fase["key"], "group_stage")
        self.assertEqual(fase["played"], 2)
        self.assertEqual(fase["total_rounds"], 6)
        self.assertEqual(fase["source"], "espn-standings")

    def test_muestra_contada_en_el_grupo(self):
        fiab = self.partido()["competition_context"]["table_reliability"]
        self.assertEqual((fiab["teams_ranked"], fiab["expected_teams"], fiab["min_played"], fiab["median_played"]), (4, 4, 2, 2.0))
        self.assertEqual(fiab["regime"], "preseason", "el regimen no se toca")

    def test_fuera_los_objetivos_de_la_tabla_de_54(self):
        cc = self.partido()["competition_context"]
        self.assertEqual(cc["home_objective"], {})
        self.assertEqual(cc["away_objective"], {})
        self.assertFalse(cc["home_relegation"]["available"])
        self.assertFalse(cc["season_preview"]["active"], "una seleccion no tiene 'temporada pasada'")

    def test_senales_y_briefing_dicen_lo_mismo(self):
        m = self.partido()
        self.assertEqual(m["match_signals"]["competitive_stakes_label"], ESPERADO_J11_15)
        self.assertEqual(m["match_signals"]["season_context_phase"], "group_stage")
        self.assertEqual(m["match_signals"]["home_must_win_index"], 0)
        dep = m["focus_ai_briefing"]["contexto_deportivo"]
        self.assertEqual(dep["contexto_competitivo"], ESPERADO_J11_15)
        self.assertEqual(dep["objetivo_local"], "")
        self.assertEqual(m["focus_ai_briefing"]["contexto_competitivo_avanzado"]["home_objective"], {})
        self.assertEqual(m["focus_ai_briefing"]["plantillas_y_transicion_de_temporada"], {"local": {"a": 1}})

    def test_es_idempotente(self):
        m = self.partido()
        primera = copy.deepcopy(m)
        w._proximos_con_su_competicion(m)
        self.assertEqual(m, primera)

    def test_fuera_de_la_fase_de_liga_no_hay_formato(self):
        """Cuartos o play-offs (marzo 2027): el grupo ya no es lo que se juega."""
        m = con_lo_de_la_tabla_de_54(espana_chequia())
        m["kickoff"] = "2027-03-25T19:45:00Z"
        w._proximos_con_su_competicion(m)
        cc = m["competition_context"]
        self.assertFalse(cc["group_phase"]["format_verified"])
        texto = cc["competitive_stakes_label"]
        self.assertIn("Formato de la competicion sin verificar", texto)
        for malo in ("cuartos", "descenso", "play-off", "le quedan"):
            self.assertNotIn(malo, texto)

    def test_grupo_con_otro_tamano_no_usa_el_formato(self):
        self.datos["children"][1]["standings"]["entries"].pop()  # A3 con 3 selecciones
        m = con_lo_de_la_tabla_de_54(espana_chequia())
        w._proximos_con_su_competicion(m)
        fase = m["competition_context"]["group_phase"]
        self.assertFalse(fase["format_verified"])
        self.assertNotIn("remaining", fase["home"])

    def test_sin_grupo_verificado_no_hay_stakes(self):
        self.patches[0].stop()
        with mock.patch.object(w, "_request_json", side_effect=RuntimeError("ESPN caido")):
            m = con_lo_de_la_tabla_de_54(espana_chequia())
            w._proximos_con_su_competicion(m)
        self.patches[0].start()
        cc = m["competition_context"]
        self.assertNotIn("group_phase", cc)
        self.assertEqual(cc["competitive_stakes_label"], "")
        self.assertEqual(cc["home_objective"], {})
        self.assertEqual(m["match_signals"]["competitive_stakes_label"], "")


class FormatoTests(unittest.TestCase):
    def test_ligas(self):
        self.assertEqual(w._formato_del_grupo("soccer_uefa_nations_league", "B2", 4, "2026-10-03")["positions"][1], "puesto de ascenso a la Liga A")
        self.assertEqual(w._formato_del_grupo("soccer_uefa_nations_league", "C4", 4, "2026-11-17")["positions"][4], "sigue en la Liga C")
        d = w._formato_del_grupo("soccer_uefa_nations_league", "D1", 3, "2026-09-25")
        self.assertEqual((d["games"], d["group_size"]), (4, 3))
        self.assertEqual(w._formato_del_grupo("soccer_uefa_nations_league", "D1", 4, "2026-09-25"), {})

    def test_sin_fecha_o_fuera_de_la_edicion(self):
        self.assertEqual(w._formato_del_grupo("soccer_uefa_nations_league", "A3", 4, ""), {})
        self.assertEqual(w._formato_del_grupo("soccer_uefa_nations_league", "A3", 4, "2028-09-26"), {})

    def test_clasificatorios_sin_formato_verificado(self):
        self.assertEqual(w._formato_del_grupo("soccer_fifa_world_cup_qualifiers_europe", "A", 4, "2026-10-03"), {})
        self.assertEqual(w._formato_del_grupo("soccer_uefa_euro_qualification", "A3", 4, "2026-10-03"), {})


class ClasificatorioTests(_ConEspn):
    def test_solo_hechos_del_grupo(self):
        m = con_lo_de_la_tabla_de_54(espana_chequia())
        m["league"] = "soccer_fifa_world_cup_qualifiers_europe"
        w._proximos_con_su_competicion(m)
        cc = m["competition_context"]
        texto = cc["competitive_stakes_label"]
        self.assertTrue(texto.endswith("Formato de la competicion sin verificar: sin objetivos por puesto"), texto)
        self.assertIn("local Spain 1º (6 pts, 2 jugados)", texto)
        for malo in ("cuartos", "descenso", "play-off", "le quedan", "sobre el", "del 2º"):
            self.assertNotIn(malo, texto)
        self.assertEqual(cc["home_objective"], {})


class ClubesIgualTests(_ConEspn):
    def test_club_no_cambia_fase_ni_stakes(self):
        m = cadiz_leganes()
        cc = m["competition_context"]
        cc.update(
            {
                "season_context_phase": {"key": "early", "played": 7},
                "competitive_stakes_label": "muestra corta",
                "home_objective": {"summary": "persigue ascenso"},
                "season_preview": {"active": True},
                "direct_rivalry": {"is_direct_rivalry": True, "direct_rivalry_index": 60},
            }
        )
        antes = {k: copy.deepcopy(v) for k, v in cc.items() if not k.endswith(("_upcoming", "_future_difficulty"))}
        w._proximos_con_su_competicion(m)
        despues = {k: v for k, v in m["competition_context"].items() if not k.endswith(("_upcoming", "_future_difficulty"))}
        self.assertEqual(despues, antes)
        self.assertNotIn("group_phase", m["competition_context"])

    def test_la_funcion_no_toca_ligas_de_clubes(self):
        m = cadiz_leganes()
        m["competition_context"]["competitive_stakes_label"] = "x"
        antes = copy.deepcopy(m)
        w._fase_de_grupos_del_partido(m, "soccer_spain_segunda_division", {}, {})
        self.assertEqual(m, antes)


if __name__ == "__main__":
    unittest.main()
