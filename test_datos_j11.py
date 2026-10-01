"""Datos de la J11 2026-27 que salian mal en el contexto (revision de Mario).

1. Liga F: el propio partido de la J11 salia como "proximo" (football-data lo
   apunta un dia despues, a las 00:00).
2. Liga F: el puesto de los proximos rivales no era el de la tabla de ESPN
   (Deportivo 13º en la tabla y "11º" en el calendario).
3. Mercado: Guardia Civil / Mini Desertica en Almeria, la salida de Aguirre
   del Mallorca en 2024 y "Suso se va del Cadiz" como fichaje.
4. FC Andorra: "proximo: casa vs Ejea (Tercera Federacion Grupo 17)", que es
   el Andorra CF de Teruel.
5. Cadiz: ESPN situa el Nuevo Mirandilla en La Linea de la Concepcion.
6. Barcelona F: "2 dias" de descanso con 3,9 reales.
7. Espana: 1 partido de forma con 2 jugados en el grupo.

Se ejecuta como los demas: python -m unittest test_datos_j11
"""

import copy
import os
import unittest
from datetime import datetime, timezone
from unittest import mock

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import filtros_feed as ff  # noqa: E402
import snapshot_worker as w  # noqa: E402


def _fila(nombre, puesto, jugados, puntos, team_id=""):
    return {
        "team": nombre, "played": jugados, "points": puntos, "goals_for": 0, "goals_against": 0,
        "goal_diff": 0, "position": puesto, "scope": "domestic", "league_name": "Spanish Liga F",
        "source": "espn-standings", "season_label": "2026-27", "espn_team_id": team_id,
    }


# Clasificacion de ESPN (esp.w.1) del 1-oct-2026, la que ya usa la tabla del partido.
TABLA_LIGA_F_ESPN = {
    n: _fila(n, p, 5, pts)
    for n, p, pts in [
        ("Barcelona", 1, 15), ("Real Madrid", 2, 13), ("Madrid CFF", 3, 12), ("Athletic Club", 4, 10),
        ("Eibar", 5, 10), ("FC Badalona", 6, 7), ("Dux Logroño", 7, 6), ("Atlético Madrid", 8, 6),
        ("Sevilla", 9, 6), ("Granada", 10, 6), ("Alavés", 11, 6), ("Espanyol", 12, 5),
        ("Deportivo", 13, 5), ("CD Tenerife", 14, 5), ("Real Sociedad", 15, 2), ("Valencia", 16, 0),
    ]
}


def _fx(kickoff, venue, rival, puesto, liga="sportsdb_5106", fuente="football-data", nombre="Liga F"):
    return {
        "date": kickoff[:10], "kickoff": kickoff, "venue": venue, "opponent": rival,
        "opponent_position": puesto, "opponent_points": None, "league": liga,
        "competition": nombre, "competition_key": liga, "source": fuente,
    }


def _deportivo_atletico():
    """P12 tal como llego en el snapshot de las 12:28 (hora de Madrid)."""
    return {
        "league": "sportsdb_5106",
        "local": "DEPORTIVO (F)", "visitante": "AT.MADRID (F)",
        "local_lae": "DEPORTIVO (F)", "visitante_lae": "AT.MADRID (F)",
        "kickoff": "2026-10-03T14:00:00Z",
        "history_context": {
            "home": {"resolved_name": "Deportivo de La Coruña Women",
                     "table": {**_fila("Deportivo", 13, 5, 5), "team": "DEPORTIVO (F)"}},
            "away": {"resolved_name": "Atlético Madrid Femenino",
                     "table": {**_fila("Atlético Madrid", 8, 5, 6), "team": "AT.MADRID (F)"}},
        },
        "competition_context": {
            "home_upcoming": [
                _fx("2026-10-04T00:00:00+00:00", "home", "Atlético Madrid Femenino", 8),
                _fx("2026-10-18T00:00:00+00:00", "away", "Eibar Women", 5),
            ],
            "away_upcoming": [
                _fx("2026-10-04T00:00:00+00:00", "away", "Deportivo de La Coruña Women", 11),
                _fx("2026-10-18T00:00:00+00:00", "home", "Madrid CFF", 2),
            ],
        },
    }


def _proximos(match, tabla=TABLA_LIGA_F_ESPN):
    with mock.patch.object(w, "_espn_clasificacion", return_value=tabla), \
            mock.patch.object(w, "ESPN_ENABLED", True):
        return w._proximos_con_su_competicion(match)


class ElPartidoDeLaJornadaNoEsUnProximoTests(unittest.TestCase):
    def test_deportivo_atletico_no_aparece_como_su_propio_proximo(self):
        m = _deportivo_atletico()
        resumen = _proximos(m)
        self.assertEqual(resumen["este_partido"], 2)
        rivales_local = [f["opponent"] for f in m["competition_context"]["home_upcoming"]]
        rivales_visit = [f["opponent"] for f in m["competition_context"]["away_upcoming"]]
        self.assertEqual(rivales_local, ["Eibar Women"])
        self.assertEqual(rivales_visit, ["Madrid CFF"])
        self.assertNotIn("Atlético", m["future_home"])
        self.assertNotIn("Deportivo", m["future_away"])

    def test_un_dia_y_pico_de_margen_tambien_cuenta(self):
        # Mismo rival, fecha con hora y 40 h despues: sigue siendo este partido.
        m = _deportivo_atletico()
        m["competition_context"]["home_upcoming"][0]["kickoff"] = "2026-10-05T06:00:00Z"
        m["competition_context"]["home_upcoming"][0]["date"] = "2026-10-05"
        _proximos(m)
        self.assertEqual([f["opponent"] for f in m["competition_context"]["home_upcoming"]], ["Eibar Women"])

    def test_el_mismo_rival_en_la_vuelta_se_queda(self):
        m = _deportivo_atletico()
        m["competition_context"]["home_upcoming"].append(
            _fx("2027-02-14T00:00:00+00:00", "away", "Atlético Madrid Femenino", 8)
        )
        _proximos(m)
        self.assertIn("Atlético Madrid Femenino", [f["opponent"] for f in m["competition_context"]["home_upcoming"]])

    def test_la_dificultad_se_recalcula_sin_el_partido(self):
        m = _deportivo_atletico()
        _proximos(m)
        dificultad = m["competition_context"]["home_future_difficulty"]
        self.assertEqual(dificultad, w._future_schedule_difficulty(m["competition_context"]["home_upcoming"]))


class PuestosDeLosProximosConLaTablaDeEspnTests(unittest.TestCase):
    def test_el_puesto_es_el_de_la_misma_tabla_que_el_partido(self):
        m = _deportivo_atletico()
        m["competition_context"]["home_upcoming"].append(
            _fx("2026-10-25T00:00:00+00:00", "home", "Deportivo de La Coruña Women", 11)
        )  # absurdo, pero sirve para comprobar el nombre de football-data
        m["competition_context"]["away_upcoming"].append(
            _fx("2026-10-25T00:00:00+00:00", "away", "Tenerife Femenino", 12)
        )
        _proximos(m)
        local = {f["opponent"]: f for f in m["competition_context"]["home_upcoming"]}
        visit = {f["opponent"]: f for f in m["competition_context"]["away_upcoming"]}
        self.assertEqual(visit["Madrid CFF"]["opponent_position"], 3)  # football-data decia 2
        self.assertEqual(visit["Madrid CFF"]["provider_position"], 2)
        self.assertEqual(visit["Madrid CFF"]["position_source"], "espn-standings")
        self.assertEqual(visit["Tenerife Femenino"]["opponent_position"], 14)  # decia 12
        self.assertEqual(local["Deportivo de La Coruña Women"]["opponent_position"], 13)  # decia 11
        self.assertEqual(local["Eibar Women"]["opponent_position"], 5)
        self.assertIn("Madrid CFF (Liga F, 3º)", m["future_away"])

    def test_si_el_rival_no_esta_claro_en_espn_no_hay_puesto(self):
        m = _deportivo_atletico()
        m["competition_context"]["away_upcoming"][1]["opponent"] = "Equipo Desconocido Women"
        _proximos(m)
        fx = m["competition_context"]["away_upcoming"][0]
        self.assertIsNone(fx["opponent_position"])
        self.assertIn("ESPN", fx["position_omitted"])
        self.assertNotIn("º", m["future_away"])

    def test_sin_tabla_de_espn_en_el_partido_se_queda_como_estaba(self):
        m = _deportivo_atletico()
        for lado in ("home", "away"):
            m["history_context"][lado]["table"]["source"] = "thesportsdb"
        with mock.patch.object(w, "_espn_clasificacion", side_effect=AssertionError("no se pide")):
            w._proximos_con_su_competicion(m)
        self.assertEqual(m["competition_context"]["away_upcoming"][0]["opponent_position"], 2)


class MercadoDudosoTests(unittest.TestCase):
    AHORA = datetime(2026, 10, 1, 10, 0, tzinfo=timezone.utc)

    def test_titulares_reales_que_sobraban(self):
        casos = [
            ('PP reclama un refuerzo de Guardia Civil en el Almanzora de Almería por sus "problemas de '
             'seguridad, especialmente robos" - Europa Press', "signing", "Almería"),
            ("La Mini Desértica reúne a cientos de niños con salida desde el Puerto de Almería",
             "departure", "Almería"),
            ("Javier Aguirre y Toni Amor vuelven a la liga tras su salida del RCD Mallorca en 2024 "
             "para hacerse cargo del Valencia CF - cope.es", "departure", "Mallorca"),
            ("Suso confirma su fracaso y se va del Cádiz CF un año después de llegar como fichaje "
             "estrella - El Desmarque", "signing", "Cádiz"),
            ("El Girona hace oficial el traspaso de Tsygankov al Ajax - MARCA", "signing", "Girona"),
            ("Oficial: Karrikaburu traspasado al Burgos - Diario AS", "departure", "Burgos"),
        ]
        for titulo, categoria, equipo in casos:
            with self.subTest(titulo=titulo[:50]):
                self.assertTrue(ff.motivo_mercado_dudoso(titulo, categoria, equipo, 2026))

    def test_los_buenos_se_quedan(self):
        casos = [
            ("La salida de Nelson Monte del Almería, al límite: “Si hubiera estado solo, habría cogido "
             "la mochila” - La Voz de Almería", "departure", "Almería"),
            ("Javi Muñoz, de fichaje del Almería a candidato al mejor jugador de septiembre de LaLiga",
             "signing", "Almería"),
            ("El Leganés hace oficial el fichaje de Álvaro Morata - EL PAÍS", "signing", "Leganés"),
            ("El C.D. Leganés cierra la cesión de Álex Sancris - C.D. Leganés - Web Oficial", "signing", "Leganés"),
            ("El Andorra anuncia el fichaje de Nacho Quintana - Diario AS", "signing", "FC Andorra"),
            ("Manuel Vizcaíno anuncia su salida del Cádiz CF - Onda Cádiz", "departure", "Cádiz"),
        ]
        for titulo, categoria, equipo in casos:
            with self.subTest(titulo=titulo[:50]):
                self.assertEqual(ff.motivo_mercado_dudoso(titulo, categoria, equipo, 2026), "")

    def test_lo_que_no_es_mercado_no_se_toca(self):
        self.assertEqual(ff.motivo_mercado_dudoso("Sergio Francisco, nuevo entrenador", "coach", "Burgos"), "")

    def test_la_mini_desertica_tampoco_es_futbol(self):
        self.assertEqual(ff.motivo_titular_ajeno("La Mini Desértica sale del Puerto de Almería"), "no es futbol")

    def test_partido_guardado(self):
        aguirre = {"title": "Javier Aguirre y Toni Amor vuelven a la liga tras su salida del RCD Mallorca "
                            "en 2024 para hacerse cargo del Valencia CF - cope.es",
                   "category": "departure", "fact_status": "reported"}
        suso = {"title": "Suso confirma su fracaso y se va del Cádiz CF un año después de llegar como "
                         "fichaje estrella - El Desmarque", "category": "signing", "fact_status": "confirmed"}
        guardia = {"title": "PP reclama un refuerzo de Guardia Civil en el Almanzora de Almería por sus "
                            "problemas de seguridad - Europa Press", "category": "signing",
                   "fact_status": "reported"}
        bueno = {"title": "El último fichaje serbio del Almería tarda solo dos minutos en marcar su primer gol",
                 "category": "signing", "fact_status": "reported"}
        m = {
            "local": "Almería", "visitante": "Mallorca",
            "competition_context": {"season_transition": {
                "home": {"transfer_reports": [guardia, bueno], "signings": [suso | {"title": suso["title"].replace("Cádiz CF", "Almería")}],
                         "all_evidence": [guardia, bueno], "summary": "2 posibles altas u operaciones"},
                "away": {"departure_reports": [aguirre], "all_evidence": [aguirre], "summary": "1 posibles salidas"},
            }},
            "focus_ai_briefing": {"x": 1},
        }
        quitados = w._quitar_mercado_dudoso(m, self.AHORA)
        self.assertEqual(quitados, 3)
        casa = m["competition_context"]["season_transition"]["home"]
        fuera = m["competition_context"]["season_transition"]["away"]
        self.assertEqual(casa["transfer_reports"], [bueno])
        self.assertEqual(casa["signings"], [])
        self.assertEqual(casa["all_evidence"], [bueno])
        self.assertEqual(casa["summary"], "1 posibles altas u operaciones")
        self.assertEqual(fuera["departure_reports"], [])
        self.assertIn("sin hechos recientes", fuera["summary"])
        briefing = m["focus_ai_briefing"]["plantillas_y_transicion_de_temporada"]
        self.assertEqual(briefing["visitante"]["posibles_salidas_no_confirmadas"], [])

    def test_el_filtro_de_calidad_tambien_los_para(self):
        item = {"title": "Suso confirma su fracaso y se va del Cádiz CF un año después de llegar como "
                         "fichaje estrella", "source": "El Desmarque", "link": "https://eldesmarque.com/x"}
        self.assertFalse(w._passes_season_transition_quality(item, "Cádiz"))


class AndorraNoEsElDeTeruelTests(unittest.TestCase):
    def _sabadell_andorra(self):
        return {
            "league": "soccer_spain_segunda_division",
            "local": "Sabadell", "visitante": "Andorra CF", "visitante_lae": "ANDORRA FC",
            "kickoff": "2026-10-03T16:30:00Z",
            "history_context": {"home": {}, "away": {}},
            "competition_context": {
                "home_upcoming": [],
                "away_upcoming": [
                    _fx("2026-10-04T15:00:00Z", "home", "Ejea", None, liga="Spanish Tercera Federación Group 17",
                        fuente="sportsdb-next", nombre="Spanish Tercera Federación Group 17"),
                    _fx("2026-10-11T14:15:00Z", "home", "Real Zaragoza", None,
                        liga="soccer_spain_segunda_division", fuente="sportsdb-next", nombre="Segunda"),
                ],
            },
        }

    def test_el_partido_de_ejea_no_es_suyo(self):
        m = self._sabadell_andorra()
        resumen = w._proximos_con_su_competicion(m)
        self.assertEqual([f["opponent"] for f in m["competition_context"]["away_upcoming"]], ["Real Zaragoza"])
        self.assertNotIn("Ejea", m.get("future_away", ""))
        self.assertEqual(resumen["imposibles"], 1)

    def test_una_liga_inferior_sobra_aunque_no_sea_al_dia_siguiente(self):
        m = self._sabadell_andorra()
        m["competition_context"]["away_upcoming"][0]["kickoff"] = "2026-10-18T15:00:00Z"
        resumen = w._proximos_con_su_competicion(m)
        self.assertEqual(resumen["de_liga_inferior"], 1)
        self.assertEqual([f["opponent"] for f in m["competition_context"]["away_upcoming"]], ["Real Zaragoza"])

    def test_la_copa_si_se_queda(self):
        self.assertFalse(w._proximo_de_liga_inferior("nombre:copa del rey", "Copa del Rey", "soccer_spain_segunda_division"))
        self.assertTrue(w._proximo_de_liga_inferior(
            "nombre:spanish tercera federacion group 17", "Spanish Tercera Federación Group 17",
            "soccer_spain_segunda_division"))
        # En una liga que no es la profesional espanola no se decide nada.
        self.assertFalse(w._proximo_de_liga_inferior("x", "Tercera Federación", "soccer_epl"))

    def test_thesportsdb_se_pregunta_por_el_fc_andorra(self):
        consultas = []
        teruel = {"idTeam": "150482", "strTeam": "Andorra CF", "strSport": "Soccer", "strCountry": "Spain",
                  "strLeague": "Spanish Tercera Federación Group 17"}
        bueno = {"idTeam": "138280", "strTeam": "FC Andorra", "strSport": "Soccer", "strCountry": "Andorra",
                 "strLeague": "Spanish La Liga 2"}

        def responder(url, params=None, timeout=None):
            consultas.append(params.get("t"))
            return {"teams": [bueno] if params.get("t") == "FC Andorra" else [teruel]}

        claves = []
        with mock.patch.object(w, "_request_json", side_effect=responder), \
                mock.patch.object(w, "_cache_get", side_effect=lambda cache, key, *a: claves.append(key) or None), \
                mock.patch.object(w, "_cache_set"), \
                mock.patch.object(w, "_frenar_sportsdb"), \
                mock.patch.object(w, "_sportsdb_hay_cupo", return_value=True), \
                mock.patch.object(w, "_known_club_profile", return_value={}):
            ficha = w.fetch_the_sportsdb_team("Andorra CF", "ES")
        self.assertEqual(ficha.get("idTeam"), "138280")
        self.assertEqual(consultas[0], "FC Andorra")
        self.assertTrue(claves and all(k.startswith("team:v3c:") for k in claves))

    def test_la_seleccion_de_andorra_no_se_toca(self):
        with mock.patch.object(w, "_cache_get", return_value={"idTeam": "x"}):
            self.assertEqual(w.fetch_the_sportsdb_team("Andorra")["idTeam"], "x")


class CiudadDelNuevoMirandillaTests(unittest.TestCase):
    EVENTO = {
        "espn_event_id": "401883194", "league_name": "Spanish LALIGA 2", "slug": "esp.2",
        "local": "Cádiz", "visitante": "Leganés", "kickoff": "2026-10-03T16:30Z",
        "venue": "Nuevo Mirandilla", "city": "La Línea de la Concepción", "country": "Spain",
    }

    def test_la_sede_queda_en_cadiz_y_la_meteo_se_pide_alli(self):
        m = {"local": "Cádiz", "visitante": "Leganés", "league": "soccer_spain_segunda_division",
             "kickoff": "2026-10-03T16:30:00Z",
             "structured_context": {"event_context": {"stadium_city": "La Línea de la Concepción, Spain",
                                                      "venue": "Nuevo Mirandilla"}},
             "weather_context": {"location_city": "La Línea de la Concepción", "location_basis": "venue"}}
        pedidas = []
        with mock.patch.object(w, "_geocode_location", side_effect=lambda c, p: pedidas.append((c, p)) or {"latitude": 36.5, "longitude": -6.27}), \
                mock.patch.object(w, "fetch_weather_context", return_value={"temperature_c": 22}), \
                mock.patch.object(w, "_cuotas_de_respaldo_espn", return_value=[]):
            w._aplicar_evento_espn(m, dict(self.EVENTO), selecciones=False)
        self.assertEqual(m["structured_context"]["event_context"]["stadium_city"], "Cádiz, Spain")
        self.assertEqual(pedidas, [("Cádiz", "ES")])
        self.assertEqual(m["weather_context"]["location_city"], "Cádiz")

    def test_otra_sede_no_cambia(self):
        ev = {**self.EVENTO, "venue": "Estadio Municipal de La Línea", "city": "La Línea de la Concepción"}
        self.assertEqual(w._corregir_ciudad_de_sede(ev)["city"], "La Línea de la Concepción")
        self.assertEqual(w._corregir_ciudad_de_sede({**self.EVENTO, "venue": "JP Financial Estadio"})["city"], "Cádiz")


class DescansoDelBarcelonaTests(unittest.TestCase):
    EVENTO = {"slug": "esp.w.1", "espn_event_id": "1", "local": "Barcelona", "visitante": "Real Madrid"}
    FORMA = {
        "Barcelona": [{"date": "2026-09-20T12:00Z"}, {"date": "2026-09-27T10:00Z"}, {"date": "2026-09-30T16:45Z"}],
        "Real Madrid": [{"date": "2026-09-20T12:00Z"}, {"date": "2026-09-23T17:00Z"}, {"date": "2026-09-26T10:00Z"}],
    }

    def _partido(self):
        # Lo que estaba guardado: dias contados contra un kickoff anterior.
        return {
            "local": "BARCELONA (F)", "visitante": "R.MADRID (F)", "kickoff": "2026-10-04T15:00:00Z",
            "schedule_context": {
                "home": {"days_since_last_match": 2, "matches_last_14_days": 3,
                         "last_match_date": "2026-09-30T16:45:00+00:00", "rest_source": "espn-summary"},
                "away": {"days_since_last_match": 7, "matches_last_14_days": 3,
                         "last_match_date": "2026-09-26T10:00:00+00:00", "rest_source": "espn-summary"},
            },
        }

    def test_el_descanso_se_rehace_con_el_kickoff_de_ahora(self):
        m = self._partido()
        with mock.patch.object(w, "_espn_forma_del_partido", return_value=self.FORMA):
            w._descanso_con_espn(m, self.EVENTO)
        casa = m["schedule_context"]["home"]
        self.assertEqual(casa["days_since_last_match"], 3)
        self.assertEqual(casa["rest_days_exact"], 3.9)
        self.assertEqual(m["schedule_context"]["away"]["days_since_last_match"], 8)

    def test_un_dato_de_otra_fuente_que_cuadra_se_respeta(self):
        m = self._partido()
        m["schedule_context"]["home"] = {"days_since_last_match": 3, "last_match_date": "2026-09-30T16:45:00+00:00",
                                         "rest_source": "historico"}
        antes = copy.deepcopy(m["schedule_context"]["home"])
        with mock.patch.object(w, "_espn_forma_del_partido", return_value=self.FORMA):
            w._descanso_con_espn(m, self.EVENTO)
        self.assertEqual(m["schedule_context"]["home"], antes)

    def test_otra_fuente_desfasada_tambien_se_rehace(self):
        m = self._partido()
        m["schedule_context"]["home"] = {"days_since_last_match": 2, "last_match_date": "2026-09-30T16:45:00+00:00",
                                         "rest_source": "historico"}
        with mock.patch.object(w, "_espn_forma_del_partido", return_value=self.FORMA):
            w._descanso_con_espn(m, self.EVENTO)
        self.assertEqual(m["schedule_context"]["home"]["days_since_last_match"], 3)


class FormaDeEspanaTests(unittest.TestCase):
    EVENTO = {"slug": "uefa.nations", "espn_event_id": "401861118", "local": "Spain", "visitante": "Czechia"}

    def _p(self, fecha, gf, gc, home=True, comp="UEFA Nations League"):
        return {"date": fecha, "home": home, "goals_for": gf, "goals_against": gc,
                "result": "W" if gf > gc else ("D" if gf == gc else "L"), "league": comp}

    def _forma(self):
        return {
            "Spain": [self._p("2026-06-20T19:00Z", 2, 0, comp="FIFA World Cup"),
                      self._p("2026-09-05T18:45Z", 3, 2), self._p("2026-09-08T18:45Z", 4, 1, home=False)],
            "Czechia": [],
        }

    def _partido(self):
        return {
            "local": "ESPAÑA", "visitante": "REP.CHECA", "kickoff": "2026-10-03T18:45:00Z",
            "history_context": {"home": {"resolved_name": "Spain", "recent_all": {
                "matches": 1, "form": "W", "points": 3}}, "away": {}},
        }

    def test_espn_con_mas_partidos_sustituye_a_la_forma_corta(self):
        m = self._partido()
        metricas = w._espn_metricas_de_forma(self._forma()["Spain"])
        with mock.patch.object(w, "_espn_forma_del_partido", return_value=self._forma()):
            cambios = w._forma_de_selecciones_con_espn(m, self.EVENTO)
        casa = m["history_context"]["home"]
        self.assertTrue(cambios)
        self.assertEqual(casa["form_source"], "espn-summary")
        self.assertEqual(casa["recent_all"]["matches"], metricas["matches"])
        self.assertGreater(casa["recent_all"]["matches"], 1)

    def test_una_forma_completa_de_otra_fuente_no_se_toca(self):
        m = self._partido()
        m["history_context"]["home"]["recent_all"] = {"matches": 5, "form": "WWWDW"}
        with mock.patch.object(w, "_espn_forma_del_partido", return_value=self._forma()):
            w._forma_de_selecciones_con_espn(m, self.EVENTO)
        self.assertEqual(m["history_context"]["home"]["recent_all"]["form"], "WWWDW")


if __name__ == "__main__":
    unittest.main()
