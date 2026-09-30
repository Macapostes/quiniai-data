"""Cada proximo partido con SU competicion, y un puesto solo si es de ESA tabla.

J11 (30-sep-2026), España - Chequia de Nations League, en produccion:
  "proximos local: fuera vs Croatia (22º) | fuera vs Czech Republic (44º) |
   casa vs England (42º)"
Los puestos salian de una tabla unica con las 54 selecciones (ligas A-D
mezcladas). En el grupo A3 de ESPN ese dia: España 1ª, Inglaterra 2ª,
Croacia 3ª, Chequia 4ª.

Y en Liga F los proximos de BARCELONA (F) eran Getafe, Galatasaray y Betis: los
del Barcelona masculino, sacados del feed de cuotas.
"""

import copy
import json
import os
import unittest
from pathlib import Path
from unittest import mock

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import fuentes_espn  # noqa: E402
import snapshot_worker as w  # noqa: E402

# Clasificacion real de ESPN (uefa.nations/standings, 30-sep-2026), recortada a
# dos grupos.
ESPN_NATIONS = json.loads(
    '{"name": "UEFA Nations League", "children": [{"name": "Group A2", "abbreviation": "Group A2", "standings": {"entries": [{"team": {"id": "449", "displayName": "Netherlands"}, "stats": [{"name": "gamesPlayed", "value": 2.0}, {"name": "points", "value": 4.0}, {"name": "pointsAgainst", "value": 2.0}, {"name": "pointsFor", "value": 3.0}, {"name": "rank", "value": 2.0}]}, {"team": {"id": "455", "displayName": "Greece"}, "stats": [{"name": "gamesPlayed", "value": 2.0}, {"name": "points", "value": 6.0}, {"name": "pointsAgainst", "value": 1.0}, {"name": "pointsFor", "value": 3.0}, {"name": "rank", "value": 1.0}]}, {"team": {"id": "481", "displayName": "Germany"}, "stats": [{"name": "gamesPlayed", "value": 2.0}, {"name": "points", "value": 1.0}, {"name": "pointsAgainst", "value": 2.0}, {"name": "pointsFor", "value": 1.0}, {"name": "rank", "value": 3.0}]}, {"team": {"id": "6757", "displayName": "Serbia"}, "stats": [{"name": "gamesPlayed", "value": 2.0}, {"name": "points", "value": 0.0}, {"name": "pointsAgainst", "value": 4.0}, {"name": "pointsFor", "value": 2.0}, {"name": "rank", "value": 4.0}]}]}}, {"name": "Group A3", "abbreviation": "Group A3", "standings": {"entries": [{"team": {"id": "164", "displayName": "Spain"}, "stats": [{"name": "gamesPlayed", "value": 2.0}, {"name": "points", "value": 6.0}, {"name": "pointsAgainst", "value": 3.0}, {"name": "pointsFor", "value": 7.0}, {"name": "rank", "value": 1.0}]}, {"team": {"id": "448", "displayName": "England"}, "stats": [{"name": "gamesPlayed", "value": 2.0}, {"name": "points", "value": 3.0}, {"name": "pointsAgainst", "value": 3.0}, {"name": "pointsFor", "value": 4.0}, {"name": "rank", "value": 2.0}]}, {"team": {"id": "450", "displayName": "Czechia"}, "stats": [{"name": "gamesPlayed", "value": 2.0}, {"name": "points", "value": 0.0}, {"name": "pointsAgainst", "value": 4.0}, {"name": "pointsFor", "value": 1.0}, {"name": "rank", "value": 4.0}]}, {"team": {"id": "477", "displayName": "Croatia"}, "stats": [{"name": "gamesPlayed", "value": 2.0}, {"name": "points", "value": 3.0}, {"name": "pointsAgainst", "value": 5.0}, {"name": "pointsFor", "value": 3.0}, {"name": "rank", "value": 3.0}]}]}}]}'
)


def _fx(opponent, venue, pos, pts, kickoff, source="football-data", league=None):
    f = {"date": kickoff[:10], "kickoff": kickoff, "venue": venue, "opponent": opponent,
         "opponent_position": pos, "opponent_points": pts, "source": source}
    if league is not None:
        f["league"] = league
    return f


def espana_chequia():
    """El partido 15 de la J11 tal como llego al backend (datos de produccion)."""
    return {
        "local": "ESPAÑA",
        "visitante": "REP.CHECA",
        "league": "soccer_uefa_nations_league",
        "kickoff": "2026-10-03T18:45:00Z",
        "history_context": {
            "league": "UEFA Nations League",
            "table_quality": {"valid": True, "teams": 54, "median_played": 2.0, "sample_regime": "preseason"},
            "table_caveat": "la tabla del proveedor no incluye el ultimo resultado",
            "home": {"resolved_name": "Spain", "table": {"team": "Spain", "played": 1, "points": 3, "position": None}},
            "away": {"resolved_name": "Czech Republic", "table": {"team": "Czech Republic", "played": 1, "points": 0, "position": None}},
        },
        "competition_context": {
            "home_upcoming": [
                _fx("Croatia", "away", 22, 3, "2026-10-06T00:00:00+00:00"),
                _fx("Czech Republic", "away", 44, 0, "2026-11-12T00:00:00+00:00"),
                _fx("England", "home", 42, 0, "2026-11-15T00:00:00+00:00"),
            ],
            "away_upcoming": [
                _fx("England", "away", 42, 0, "2026-10-06T00:00:00+00:00"),
                _fx("Spain", "home", 20, 3, "2026-11-12T00:00:00+00:00"),
                _fx("Croatia", "away", 22, 3, "2026-11-15T00:00:00+00:00"),
            ],
        },
    }


def barcelona_f():
    """Partido 11 de la J11: los proximos eran los del Barcelona masculino."""
    return {
        "local": "BARCELONA (F)",
        "visitante": "R.MADRID (F)",
        "league": "sportsdb_5106",
        "kickoff": "2026-10-04T15:00:00Z",
        "history_context": {"home": {"resolved_name": "BARCELONA"}, "away": {"resolved_name": "Real Madrid"}},
        "competition_context": {
            "home_upcoming": [
                _fx("Getafe", "home", None, None, "2026-10-10T16:30:00Z", "odds-feed", "soccer_spain_la_liga"),
                _fx("Galatasaray", "away", None, None, "2026-10-13T19:00:00Z", "odds-feed", "soccer_uefa_champs_league"),
                _fx("Levante Women", "away", 9, 7, "2026-10-18T00:00:00+00:00"),
            ],
            "away_upcoming": [
                _fx("Real Sociedad", "home", 15, 4, "2026-10-11T14:15:00Z", "odds-feed", "soccer_spain_la_liga"),
            ],
            "home_rotation_context": {
                "risk": "high", "score": 88,
                "reason": "BARCELONA tiene UEFA Champions League vs Galatasaray en 3.2 dias",
                "next_high_importance_fixture": _fx("Galatasaray", "away", None, None, "2026-10-13T19:00:00Z", "odds-feed", "soccer_uefa_champs_league"),
            },
        },
    }


def cadiz_leganes():
    return {
        "local": "CÁDIZ",
        "visitante": "LEGANÉS",
        "league": "soccer_spain_segunda_division",
        "kickoff": "2026-10-03T16:30:00Z",
        "history_context": {"home": {"resolved_name": "Cadiz"}, "away": {"resolved_name": "Leganes"}},
        "competition_context": {
            "home_upcoming": [
                _fx("Sporting Gijón", "home", 12, 9, "2026-10-11T13:00:00Z", "espn-fixtures", "Spanish LALIGA 2"),
                _fx("Real Sociedad II", "away", None, None, "2026-10-18T08:15:00Z", "espn-fixtures", "Spanish LALIGA 2"),
                _fx("Getafe", "home", 13, 8, "2026-10-29T19:00:00Z", "sportsdb-next", "Spanish Copa del Rey"),
                _fx("Rival X", "away", 5, 12, "2026-11-02T19:00:00Z", "espn-summary"),
            ],
            "away_upcoming": [],
        },
    }


class _ConEspn(unittest.TestCase):
    def setUp(self):
        w.EXTERNAL_FEEDS_CACHE.pop("espn:grupos:v1:uefa.nations", None)
        self.pedidas = []

        def falso(url, params=None, timeout=30):
            self.pedidas.append(url)
            if "uefa.nations/standings" in url:
                return copy.deepcopy(ESPN_NATIONS)
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
        w.EXTERNAL_FEEDS_CACHE.pop("espn:grupos:v1:uefa.nations", None)


class ClasificacionPorGruposTests(unittest.TestCase):
    def test_el_puesto_es_el_del_grupo(self):
        tabla = fuentes_espn.clasificacion_por_grupos(ESPN_NATIONS)
        self.assertEqual(tabla["England"]["position"], 2)
        self.assertEqual(tabla["England"]["group"], "A3")
        self.assertEqual(tabla["Greece"]["group"], "A2")
        self.assertEqual(tabla["Spain"]["group_size"], 4)
        self.assertEqual(tabla["Spain"]["points"], 6)

    def test_la_tabla_unica_sigue_negandose_a_mezclar_grupos(self):
        self.assertEqual(fuentes_espn.clasificacion(ESPN_NATIONS), {})


class SeleccionesTests(_ConEspn):
    def test_espana_chequia_con_su_grupo(self):
        m = espana_chequia()
        w._proximos_con_su_competicion(m)
        self.assertEqual(
            m["future_home"],
            "fuera vs Croatia (Nations League, 3º grupo A3) | "
            "fuera vs Czech Republic (Nations League, 4º grupo A3) | "
            "casa vs England (Nations League, 2º grupo A3)",
        )
        self.assertEqual(
            m["future_away"],
            "fuera vs England (Nations League, 2º grupo A3) | "
            "casa vs Spain (Nations League, 1º grupo A3) | "
            "fuera vs Croatia (Nations League, 3º grupo A3)",
        )

    def test_ningun_numero_de_la_tabla_de_54(self):
        m = espana_chequia()
        w._proximos_con_su_competicion(m)
        for lado in ("home_upcoming", "away_upcoming"):
            for fx in m["competition_context"][lado]:
                self.assertIsNone(fx["opponent_position"], fx)
                self.assertIsNone(fx["opponent_points"], fx)
                self.assertLessEqual(fx["opponent_group_position"], 4)
        for texto in (m["future_home"], m["future_away"]):
            for malo in ("42º", "44º", "22º", "20º"):
                self.assertNotIn(malo, texto)

    def test_la_tabla_del_partido_es_la_del_grupo(self):
        m = espana_chequia()
        w._proximos_con_su_competicion(m)
        h = m["history_context"]
        self.assertEqual(h["home"]["table"]["position"], 1)
        self.assertEqual(h["home"]["table"]["played"], 2)
        self.assertEqual(h["home"]["table"]["points"], 6)
        self.assertEqual(h["away"]["table"]["position"], 4)
        self.assertEqual(h["home"]["table"]["position_label"], "Nations League, 1º grupo A3")
        self.assertEqual(h["table_scope"]["group"], "A3")
        self.assertIn("grupo A3", h["table_caveat"])
        self.assertEqual(h["table_quality"]["teams"], 4)

    def test_sin_clasificacion_de_grupo_no_hay_puesto(self):
        self.patches[0].stop()
        with mock.patch.object(w, "_request_json", side_effect=RuntimeError("ESPN caido")):
            m = espana_chequia()
            m["history_context"]["home"]["table"]["position"] = 20
            w._proximos_con_su_competicion(m)
        self.patches[0].start()
        self.assertEqual(
            m["future_home"],
            "fuera vs Croatia (Nations League) | fuera vs Czech Republic (Nations League) | "
            "casa vs England (Nations League)",
        )
        self.assertIsNone(m["history_context"]["home"]["table"]["position"])
        self.assertNotIn("table_scope", m["history_context"])

    def test_rival_de_otro_grupo_no_recibe_puesto(self):
        """Un play-off o un cruce entre grupos no tiene "su" puesto de grupo."""
        m = espana_chequia()
        m["competition_context"]["home_upcoming"] = [_fx("Greece", "home", 3, 6, "2027-03-20T00:00:00+00:00")]
        w._proximos_con_su_competicion(m)
        self.assertEqual(m["future_home"], "casa vs Greece (Nations League)")

    def test_amistoso_sin_puesto(self):
        m = espana_chequia()
        m["competition_context"]["home_upcoming"] = [
            _fx("Argentina", "home", 7, 9, "2026-11-18T19:00:00Z", "sportsdb-next", "International Friendly")
        ]
        w._proximos_con_su_competicion(m)
        self.assertEqual(m["future_home"], "casa vs Argentina (amistoso)")

    def test_es_idempotente(self):
        m = espana_chequia()
        w._proximos_con_su_competicion(m)
        primera = copy.deepcopy(m)
        w._proximos_con_su_competicion(m)
        self.assertEqual(m["future_home"], primera["future_home"])
        self.assertEqual(m["competition_context"], primera["competition_context"])
        self.assertEqual(m["history_context"], primera["history_context"])

    def test_la_dificultad_no_usa_puestos_de_grupo_como_de_liga(self):
        m = espana_chequia()
        w._proximos_con_su_competicion(m)
        dif = m["competition_context"]["home_future_difficulty"]
        self.assertEqual(dif["top4_matches"], 0)
        self.assertIsNone(dif["avg_opponent_position"])


class ClubesTests(_ConEspn):
    def test_liga_f_sin_los_partidos_del_masculino(self):
        m = barcelona_f()
        w._proximos_con_su_competicion(m)
        self.assertEqual(m["future_home"], "fuera vs Levante Women (Liga F, 9º)")
        self.assertEqual(
            [f["opponent"] for f in m["competition_context"]["home_upcoming"]], ["Levante Women"]
        )
        self.assertNotIn("future_away", m, "Real Sociedad con el 15º de Liga F era del masculino")
        self.assertEqual(m["competition_context"]["away_upcoming"], [])

    def test_la_rotacion_no_depende_de_la_champions_del_masculino(self):
        m = barcelona_f()
        w._proximos_con_su_competicion(m)
        rot = m["competition_context"]["home_rotation_context"]
        self.assertNotIn("Galatasaray", rot.get("reason", ""))
        self.assertFalse(rot.get("next_high_importance_fixture"))

    def test_misma_liga_puesto_con_su_nombre(self):
        m = cadiz_leganes()
        w._proximos_con_su_competicion(m)
        partes = m["future_home"].split(" | ")
        self.assertEqual(partes[0], "casa vs Sporting Gijón (Segunda División, 12º)")
        self.assertEqual(partes[1], "fuera vs Real Sociedad II (Segunda División)")

    def test_copa_sin_el_puesto_de_la_liga(self):
        m = cadiz_leganes()
        w._proximos_con_su_competicion(m)
        partes = m["future_home"].split(" | ")
        self.assertEqual(partes[2], "casa vs Getafe (Copa del Rey)")
        copa = m["competition_context"]["home_upcoming"][2]
        self.assertIsNone(copa["opponent_position"])
        self.assertEqual(copa["position_omitted"], "otra competicion")

    def test_competicion_desconocida_sin_numero(self):
        m = cadiz_leganes()
        w._proximos_con_su_competicion(m)
        partes = m["future_home"].split(" | ")
        self.assertEqual(partes[3], "fuera vs Rival X")
        self.assertIsNone(m["competition_context"]["home_upcoming"][3]["opponent_position"])

    def test_no_pide_grupos_si_no_hace_falta(self):
        w._proximos_con_su_competicion(cadiz_leganes())
        self.assertEqual(self.pedidas, [])


class CableadoTests(unittest.TestCase):
    def test_se_aplica_a_todos_los_partidos_guardados(self):
        todo = Path(w.__file__).read_text(encoding="utf-8")
        self.assertIn("_proximos_con_su_competicion(match)", todo)
        # Despues del contraste con ESPN, dentro del bucle de partidos guardados.
        self.assertLess(todo.index("cambios_espn = _aplicar_fuentes_espn(match)"), todo.index("proximos = _proximos_con_su_competicion(match)"))

    def test_el_monitor_usa_el_resumen_etiquetado(self):
        m = {"future_home": "casa vs England (Nations League, 2º grupo A3)"}
        self.assertEqual(w._monitor_future_summary(m, "home"), "casa vs England (Nations League, 2º grupo A3)")


if __name__ == "__main__":
    unittest.main()
