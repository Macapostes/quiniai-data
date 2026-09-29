"""Segunda tanda del feed: tablas desfasadas, sedes, competicion y desempates.

- Tabla de Segunda con retraso de football-data y de Liga F (TheSportsDB) con
  2 jugados cuando la liga iba por 5: se contrasta con la de ESPN.
- Sede de selecciones y del Barcelona - Real Madrid femenino (Camp Nou, no
  Johan Cruyff): la del partido en el calendario, no la de la ficha.
- J11 #15 sin liga: la competicion sale del partido confirmado.
- "Mallorca descendido 17o": en LaLiga el empate a puntos se resuelve por los
  partidos entre los empatados.
"""

import copy
import os
import unittest
from datetime import datetime, timezone
from unittest import mock

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import fuentes_espn as fe  # noqa: E402
import snapshot_worker as w  # noqa: E402

AHORA = datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc)


def _fila(div, local, visit, gl, gv):
    ftr = "H" if gl > gv else ("A" if gl < gv else "D")
    return {"\u00ef\u00bb\u00bfDiv": div, "HomeTeam": local, "AwayTeam": visit, "FTHG": str(gl), "FTAG": str(gv), "FTR": ftr}


def _marcador(liga, eventos):
    return {
        "leagues": [{"name": liga}],
        "events": [
            {
                "id": eid,
                "date": fecha,
                "status": {"type": {"name": "STATUS_SCHEDULED", "completed": False}},
                "competitions": [
                    {
                        "venue": {"fullName": sede, "address": {"city": ciudad, "country": pais}},
                        "competitors": [
                            {"homeAway": "home", "team": {"displayName": local}},
                            {"homeAway": "away", "team": {"displayName": visit}},
                        ],
                    }
                ],
            }
            for eid, fecha, local, visit, sede, ciudad, pais in eventos
        ],
    }


def _tabla(nombre_liga, filas):
    return {
        "name": nombre_liga,
        "children": [
            {
                "abbreviation": "2026-27",
                "standings": {
                    "entries": [
                        {
                            "team": {"id": str(i + 100), "displayName": equipo},
                            "stats": [
                                {"name": "gamesPlayed", "value": pj},
                                {"name": "points", "value": pts},
                                {"name": "pointsFor", "value": gf},
                                {"name": "pointsAgainst", "value": gc},
                                {"name": "rank", "value": i + 1},
                            ],
                        }
                        for i, (equipo, pj, pts, gf, gc) in enumerate(filas)
                    ]
                },
            }
        ],
    }


LIGA_F = fe.clasificacion(_tabla("Spanish Liga F", [
    ("Barcelona", 5, 15, 21, 2), ("Real Madrid", 5, 13, 13, 6), ("Madrid CFF", 5, 12, 9, 7),
    ("Athletic Club", 5, 10, 8, 5), ("Atlético Madrid", 5, 6, 8, 10), ("Deportivo", 5, 5, 4, 4),
]))
SEGUNDA = fe.clasificacion(_tabla("Spanish LALIGA 2", [
    ("Castellón", 7, 19, 20, 8), ("Eibar", 7, 18, 15, 6), ("CD Sabadell", 7, 12, 8, 6),
    ("FC Andorra", 7, 6, 12, 15), ("Real Sociedad II", 7, 8, 9, 11), ("RC Celta Fortuna", 7, 7, 8, 10),
]))
EV_BARSA = fe.eventos_del_marcador(_marcador("Spanish Liga F", [
    ("401882497", "2026-10-04T15:00Z", "Barcelona", "Real Madrid", "Spotify Camp Nou", "Barcelona", "Spain"),
]))
EV_NATIONS = fe.eventos_del_marcador(_marcador("UEFA Nations League", [
    ("401861118", "2026-10-03T18:45Z", "Spain", "Czechia", "Estadio Carlos Tartiere", "Oviedo", "Spain"),
    ("401861119", "2026-10-03T18:45Z", "North Macedonia", "Scotland", "Toše Proeski Arena", "Skopje", "North Macedonia"),
]))


class DesempateDirectoTests(unittest.TestCase):
    """Tres empatados a 10 puntos: la diferencia general dice C, B, A; la
    liguilla entre ellos (A 6, B 5, C 3) dice A, B, C. Es el caso de LaLiga
    25/26 con Levante, Osasuna y Mallorca a 42: por diferencia salia Mallorca
    17o y el feed lo daba por "descendido: 17o"."""

    @staticmethod
    def _temporada(div="SP1", completa=True):
        filas = [
            _fila(div, "A", "B", 2, 0), _fila(div, "B", "A", 1, 1),
            _fila(div, "A", "C", 1, 1), _fila(div, "C", "A", 1, 1),
            _fila(div, "B", "C", 1, 0),
            _fila(div, "C", "B", 0, 0) if completa else _fila(div, "C", "X9", 0, 0),
            # Relleno contra otros hasta 10 puntos, con diferencias C 9, B 4, A 3.
            _fila(div, "A", "X1", 1, 0), _fila(div, "A", "X2", 0, 0),
            _fila(div, "B", "X3", 5, 0), _fila(div, "B", "X4", 0, 0), _fila(div, "B", "X5", 0, 0),
            _fila(div, "C", "X6", 5, 0), _fila(div, "C", "X7", 5, 0), _fila(div, "C", "X8", 0, 0),
        ]
        if not completa:
            filas.append(_fila(div, "B", "X10", 0, 0))
        return filas

    def _orden(self, tabla):
        return sorted(("A", "B", "C"), key=lambda e: tabla[e]["position"])

    def test_liguilla_entre_empatados_en_laliga(self):
        tabla = w._table_snapshot(self._temporada("SP1"))
        self.assertEqual({tabla[e]["points"] for e in "ABC"}, {10})
        self.assertEqual(self._orden(tabla), ["A", "B", "C"])

    def test_segunda_tambien(self):
        self.assertEqual(self._orden(w._table_snapshot(self._temporada("SP2"))), ["A", "B", "C"])

    def test_premier_sigue_por_diferencia_general(self):
        self.assertEqual(self._orden(w._table_snapshot(self._temporada("E0"))), ["C", "B", "A"])

    def test_sin_todos_los_cruces_jugados_manda_la_diferencia(self):
        tabla = w._table_snapshot(self._temporada("SP1", completa=False))
        self.assertEqual({tabla[e]["points"] for e in "ABC"}, {10})
        self.assertEqual(self._orden(tabla), ["C", "B", "A"])

    def test_la_cache_de_tabla_final_cambia_de_version(self):
        import inspect
        self.assertIn("final-table:v2:", inspect.getsource(w._final_table_for_season))


class FuentesEspnParseoTests(unittest.TestCase):
    def test_clasificacion(self):
        self.assertEqual(LIGA_F["Barcelona"]["played"], 5)
        self.assertEqual(LIGA_F["Barcelona"]["position"], 1)
        self.assertEqual(LIGA_F["Barcelona"]["goal_diff"], 19)
        self.assertEqual(fe.mediana_jugados(LIGA_F), 5)

    def test_tabla_con_varios_grupos_no_se_usa(self):
        payload = _tabla("UEFA Nations League", [("Spain", 2, 6, 5, 1)])
        payload["children"].append(copy.deepcopy(payload["children"][0]))
        self.assertEqual(fe.clasificacion(payload), {})

    def test_elige_el_partido_por_equipos_y_fecha(self):
        ko = datetime(2026, 10, 3, 18, 45, tzinfo=timezone.utc)
        ev = fe.elegir_partido(EV_NATIONS, "ESPAÑA", "REP.CHECA", ko, w._similitud_espn)
        self.assertEqual(ev["venue"], "Estadio Carlos Tartiere")
        # Otra fecha: no es este partido.
        lejos = datetime(2026, 10, 12, 18, 45, tzinfo=timezone.utc)
        self.assertEqual(fe.elegir_partido(EV_NATIONS, "ESPAÑA", "REP.CHECA", lejos, w._similitud_espn), {})
        # Local y visitante cambiados: tampoco.
        self.assertEqual(fe.elegir_partido(EV_NATIONS, "REP.CHECA", "ESPAÑA", ko, w._similitud_espn), {})

    def test_nombres_del_boleto_contra_espn(self):
        for nombres, esperado, tabla in (
            (["R.SOCIEDAD B", "Sociedad B"], "Real Sociedad II", SEGUNDA),
            (["CELTA FORTUNA", "Celta B"], "RC Celta Fortuna", SEGUNDA),
            (["ANDORRA FC", "Andorra CF"], "FC Andorra", SEGUNDA),
            (["SABADELL"], "CD Sabadell", SEGUNDA),
            (["ATH.CLUB (F)", "Athletic Club Women"], "Athletic Club", LIGA_F),
            (["AT.MADRID (F)", "Atlético Madrid Femenino"], "Atlético Madrid", LIGA_F),
            (["R.MADRID (F)", "Real Madrid Femenino"], "Real Madrid", LIGA_F),
        ):
            self.assertEqual(fe.fila_de(tabla, nombres, w._similitud_espn).get("team"), esperado, nombres)

    def test_calendario_de_equipo(self):
        payload = {"events": [
            {"date": "2026-09-26T14:30Z", "competitions": [{"status": {"type": {"completed": True}}, "competitors": [
                {"homeAway": "home", "team": {"displayName": "Dux Logroño"}, "score": {"value": 0.0}},
                {"homeAway": "away", "team": {"displayName": "Barcelona"}, "score": {"value": 2.0}}]}]},
            {"date": "2026-10-04T15:00Z", "competitions": [{"status": {"type": {"completed": False}}, "competitors": [
                {"homeAway": "home", "team": {"displayName": "Barcelona"}},
                {"homeAway": "away", "team": {"displayName": "Real Madrid"}}]}]},
        ]}
        filas = fe.filas_de_calendario(payload)
        self.assertEqual(len(filas), 1)
        self.assertEqual((filas[0]["AwayTeam"], filas[0]["FTR"]), ("Barcelona", "A"))


def _partido_liga_f():
    return {
        "local": "Barcelona", "visitante": "Real Madrid",
        "local_lae": "BARCELONA (F)", "visitante_lae": "R.MADRID (F)",
        "league": "sportsdb_5106", "kickoff": "2026-10-04T15:00:00Z",
        "structured_context": {"event_context": {"venue": "Estadi Johan Cruyff", "stadium_city": "Barcelona, Spain"}},
        "weather_context": {"timezone": "Europe/Madrid", "temperature_c": 24},
        "history_context": {
            "table_quality": {"median_played": 2.0},
            "home": {"resolved_name": "Barcelona Femení", "recent_all": {"matches": 2},
                     "table": {"team": "Barcelona Femení", "played": 2, "points": 6, "position": 1}},
            "away": {"resolved_name": "Real Madrid Femenino", "recent_all": {"matches": 2},
                     "table": {"team": "Real Madrid Femenino", "played": 2, "points": 4, "position": 4}},
        },
    }


class AplicarEspnTests(unittest.TestCase):
    def setUp(self):
        self.patches = [
            mock.patch.object(w, "_espn_eventos_del_dia", side_effect=self._eventos),
            mock.patch.object(w, "_espn_clasificacion", side_effect=lambda slug: {"esp.w.1": LIGA_F, "esp.2": SEGUNDA}.get(slug, {})),
            mock.patch.object(w, "_espn_calendario_equipo", return_value=[]),
            # Sin red en los tests: el descanso con ESPN tiene sus propios tests.
            mock.patch.object(w, "_espn_forma_del_partido", return_value={}),
            mock.patch.object(w, "_geocode_location", return_value={"latitude": 43.36, "longitude": -5.85}),
            mock.patch.object(w, "fetch_weather_context", return_value={"timezone": "Europe/Madrid", "temperature_c": 16.0}),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()

    @staticmethod
    def _eventos(slug, dia):
        if slug == "esp.w.1" and dia == "20261004":
            return EV_BARSA
        if slug == "uefa.nations" and dia == "20261003":
            return EV_NATIONS
        return []

    def test_barsa_femenino_juega_en_el_camp_nou_y_la_tabla_se_pone_al_dia(self):
        m = _partido_liga_f()
        cambios = w._aplicar_fuentes_espn(m, AHORA)
        ev = m["structured_context"]["event_context"]
        self.assertEqual(ev["venue"], "Spotify Camp Nou")
        self.assertEqual(ev["venue_source"], "espn-fixture")
        casa = m["history_context"]["home"]["table"]
        self.assertEqual((casa["played"], casa["points"], casa["position"]), (5, 15, 1))
        self.assertEqual(casa["provider_played_before_refresh"], 2)
        self.assertEqual(m["history_context"]["away"]["table"]["position"], 2)
        self.assertEqual(m["history_context"]["table_quality"]["median_played"], 5)
        self.assertTrue(any("Camp Nou" in c for c in cambios))

    def test_espana_rep_checa_sede_competicion_y_meteo_de_la_sede(self):
        m = {
            "local": "Spain", "visitante": "REP.CHECA", "league": "", "kickoff": "2026-10-03T18:45:00Z",
            "structured_context": {"event_context": {"venue": "Estadio Santiago Bernabéu", "stadium_city": "Madrid, Spain", "league": "FIFA World Cup"}},
            "weather_context": {"timezone": "Europe/Madrid", "temperature_c": 20.3},
            "history_context": {},
        }
        w._aplicar_fuentes_espn(m, AHORA)
        ev = m["structured_context"]["event_context"]
        self.assertEqual((ev["venue"], ev["stadium_city"]), ("Estadio Carlos Tartiere", "Oviedo, Spain"))
        self.assertEqual(ev["espn_event_id"], "401861118")
        self.assertEqual(m["league"], "soccer_uefa_nations_league")
        self.assertEqual(m["league_name"], "UEFA Nations League")
        self.assertEqual(m["weather_context"]["location_basis"], "venue")
        self.assertEqual(m["weather_context"]["temperature_c"], 16.0)

    def test_seleccion_sin_partido_confirmado_meteo_aproximada(self):
        m = {
            "local": "Spain", "visitante": "Croatia", "league": "soccer_uefa_nations_league",
            "kickoff": "2026-10-03T18:45:00Z", "structured_context": {"event_context": {}},
            "weather_context": {"timezone": "Europe/Madrid", "temperature_c": 25}, "history_context": {},
        }
        w._aplicar_fuentes_espn(m, AHORA)
        self.assertTrue(m["weather_context"]["approximate"])
        self.assertEqual(m["weather_context"]["location_basis"], "capital_aproximada")

    def test_segunda_rezagada_y_andorra_sin_tabla(self):
        m = {
            "local": "Sabadell", "visitante": "FC Andorra", "league": "soccer_spain_segunda_division",
            "kickoff": "2026-10-03T16:30:00Z", "structured_context": {"event_context": {}},
            "history_context": {
                "home": {"resolved_name": "Sabadell", "table": {"team": "Sabadell", "played": 6, "points": 9, "position": 9}},
                "away": {"resolved_name": "Andorra CF", "league_key": "sportsdb_5554", "table": {}},
            },
        }
        w._aplicar_fuentes_espn(m, AHORA)
        casa = m["history_context"]["home"]["table"]
        fuera = m["history_context"]["away"]["table"]
        self.assertEqual((casa["played"], casa["points"], casa["position"]), (7, 12, 3))
        self.assertEqual((fuera["played"], fuera["position"]), (7, 4))
        self.assertEqual(fuera["league_key"], "soccer_spain_segunda_division")

    def test_espn_con_menos_partidos_no_pisa(self):
        m = _partido_liga_f()
        m["history_context"]["home"]["table"]["played"] = 6
        w._aplicar_fuentes_espn(m, AHORA)
        self.assertEqual(m["history_context"]["home"]["table"]["played"], 6)

    def test_equipo_que_no_aparece_y_va_por_detras_se_marca(self):
        m = _partido_liga_f()
        m["visitante"], m["visitante_lae"] = "Granada", "GRANADA (F)"
        m["history_context"]["away"] = {"resolved_name": "Granada CF Femenino", "table": {"played": 2, "position": 9}}
        w._aplicar_fuentes_espn(m, AHORA)
        self.assertEqual(m["history_context"]["table_freshness"]["stale_sides"], ["away"])

    def test_partido_ya_jugado_no_se_toca(self):
        m = _partido_liga_f()
        m["kickoff"] = "2026-09-20T15:00:00Z"
        self.assertEqual(w._aplicar_fuentes_espn(m, AHORA), [])
        self.assertEqual(m["structured_context"]["event_context"]["venue"], "Estadi Johan Cruyff")

    def test_racha_rehecha_con_el_calendario(self):
        filas = [
            {"HomeTeam": "Barcelona", "AwayTeam": f"R{i}", "FTHG": "2", "FTAG": "0", "FTR": "H",
             "KickoffUTC": f"2026-09-0{i + 1}T18:00:00+00:00"}
            for i in range(5)
        ]
        with mock.patch.object(w, "_espn_calendario_equipo", return_value=filas):
            m = _partido_liga_f()
            w._aplicar_fuentes_espn(m, AHORA)
        casa = m["history_context"]["home"]
        self.assertEqual(casa["recent_all"]["matches"], 5)
        self.assertEqual(casa["recent_all"]["form"], "WWWWW")
        self.assertEqual(casa["form_source"], "espn-schedule")


class FormaDeSeleccionesTests(unittest.TestCase):
    RESUMEN = {"lastFiveGames": [
        {"team": {"displayName": "Spain"}, "events": [
            {"gameDate": "2026-07-19T19:00Z", "gameResult": "W", "score": "1-0", "atVs": "vs",
             "opponent": {"displayName": "Argentina"}, "leagueName": "FIFA World Cup"},
            {"gameDate": "2026-09-26T18:45Z", "gameResult": "W", "score": "3-2", "atVs": "@",
             "opponent": {"displayName": "England"}, "leagueName": "UEFA Nations League"},
        ]},
        {"team": {"displayName": "Czechia"}, "events": [
            {"gameDate": "2026-06-25T01:00Z", "gameResult": "L", "score": "3-0", "atVs": "vs",
             "opponent": {"displayName": "Mexico"}, "leagueName": "FIFA World Cup"},
            {"gameDate": "2026-09-26T18:45Z", "gameResult": "L", "score": "2-1", "atVs": "vs",
             "opponent": {"displayName": "Croatia"}, "leagueName": "UEFA Nations League"},
        ]},
    ]}

    def test_goles_con_el_goleador_delante(self):
        forma = fe.forma_de_resumen(self.RESUMEN)
        derrota = forma["Czechia"][0]
        self.assertEqual((derrota["goals_for"], derrota["goals_against"], derrota["home"]), (0, 3, True))
        m = fe.metricas_de_forma(forma["Spain"])
        self.assertEqual((m["form"], m["points"], m["goals_for"], m["goals_against"]), ("WW", 6, 4, 2))

    def test_seleccion_sin_racha_la_recibe_de_espn(self):
        m = {
            "local": "Spain", "visitante": "REP.CHECA", "league": "soccer_uefa_nations_league",
            "kickoff": "2026-10-03T18:45:00Z", "structured_context": {"event_context": {}},
            "history_context": {"supported": False},
        }
        evento = {"slug": "uefa.nations", "espn_event_id": "401861118", "local": "Spain", "visitante": "Czechia"}
        with mock.patch.object(w, "_espn_forma_del_partido", return_value=fe.forma_de_resumen(self.RESUMEN)):
            cambios = w._forma_de_selecciones_con_espn(m, evento)
        h = m["history_context"]
        self.assertTrue(h["supported"])
        self.assertEqual(h["home"]["recent_all"]["form"], "WW")
        self.assertEqual(h["away"]["recent_all"]["form"], "LL")
        self.assertEqual(h["away"]["form_source"], "espn-summary")
        self.assertEqual(len(cambios), 2)


class SlugsTests(unittest.TestCase):
    def test_slugs(self):
        self.assertEqual(fe.slugs_para_partido("soccer_spain_segunda_division", femenino=False, selecciones=False), ["esp.2"])
        self.assertEqual(fe.slugs_para_partido("sportsdb_5106", femenino=True, selecciones=False), ["esp.w.1"])
        self.assertEqual(fe.slugs_para_partido("", femenino=False, selecciones=True)[0], "uefa.nations")
        self.assertEqual(fe.slugs_para_partido("soccer_norway_eliteserien", femenino=False, selecciones=False), [])


if __name__ == "__main__":
    unittest.main()
