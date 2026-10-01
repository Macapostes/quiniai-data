"""La hora del partido la corrige ESPN cuando el cruce es inequivoco (J11 2026-27).

El worker llevaba los cuatro partidos de Liga F de la J11 el sabado 3 a las
14:00 UTC, la hora de relleno del boleto. ESPN los tiene el 3 a las 14:00 y
16:00 y el 4 a las 10:00 y 15:00 UTC. Con la hora mala:
- el Barcelona F salia con "2 dias" de descanso (3,9 reales) y el Real Madrid F
  con 7 (8,2);
- la meteo del Madrid CFF F era la de las 14:00 de Madrid y no la de las 18:00;
- el Tenerife F, la del dia antes.
Ademas la meteo tomaba las horas locales de Open-Meteo como si fueran UTC.

Se ejecuta como los demas: python -m unittest test_hora_espn
"""

import os
import unittest
from datetime import datetime, timezone
from unittest import mock

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import fuentes_espn as fe  # noqa: E402
import snapshot_worker as w  # noqa: E402
from test_feed_limpio_2 import _marcador  # noqa: E402

AHORA = datetime(2026, 10, 1, 15, 30, tzinfo=timezone.utc)
RELLENO = "2026-10-03T14:00:00Z"

# Marcador de ESPN (esp.w.1) del 3 y 4 de octubre, tal como estaba el 1-oct.
EVENTOS_LIGA_F = {
    "20261003": fe.eventos_del_marcador(_marcador("Spanish Liga F", [
        ("401882498", "2026-10-03T10:00Z", "Espanyol", "Alavés", "Ciutat Esportiva Dani Jarque", "Barcelona", "Spain"),
        ("401882494", "2026-10-03T10:00Z", "Sevilla", "FC Badalona", "Estadio Jesús Navas", "Sevilla", "Spain"),
        ("401882499", "2026-10-03T14:00Z", "Deportivo", "Atlético Madrid", "Riazor", "La Coruña", "Spain"),
        ("401882495", "2026-10-03T16:00Z", "Madrid CFF", "Athletic Club", "Estadio Fernando Torres", "Fuenlabrada", "Spain"),
    ])),
    "20261004": fe.eventos_del_marcador(_marcador("Spanish Liga F", [
        ("401882500", "2026-10-04T10:00Z", "CD Tenerife", "Dux Logroño", "Estadio Heliodoro Rodríguez López",
         "Santa Cruz de Tenerife", "Spain"),
        ("401882496", "2026-10-04T10:00Z", "Granada", "Eibar", "Ciudad Deportiva del Granada CF", "Granada", "Spain"),
        ("401882497", "2026-10-04T15:00Z", "Barcelona", "Real Madrid", "Spotify Camp Nou", "Barcelona", "Spain"),
        ("401882493", "2026-10-04T17:30Z", "Valencia", "Real Sociedad", "Estadio Antonio Puchades", "Paterna", "Spain"),
    ])),
}

# Ultimos partidos de /summary (los que dan el descanso).
FORMA = {
    "Barcelona": [{"date": "2026-09-20T12:00Z"}, {"date": "2026-09-27T10:00Z"}, {"date": "2026-09-30T16:45Z"}],
    "Real Madrid": [{"date": "2026-09-20T12:00Z"}, {"date": "2026-09-23T17:00Z"}, {"date": "2026-09-26T10:00Z"}],
    "Madrid CFF": [{"date": "2026-09-26T16:30Z"}],
    "Athletic Club": [{"date": "2026-09-27T10:00Z"}],
    "CD Tenerife": [{"date": "2026-09-27T10:00Z"}],
    "Dux Logroño": [{"date": "2026-09-26T14:30Z"}],
}


def _liga_f(local, visitante, lae_l, lae_v, nombre_l, nombre_v):
    return {
        "local": local, "visitante": visitante, "local_lae": lae_l, "visitante_lae": lae_v,
        "league": "sportsdb_5106", "kickoff": RELLENO, "kickoff_source": "quiniela_oficial",
        "structured_context": {"event_context": {}},
        "weather_context": {"timezone": "Europe/Madrid", "forecast_time": "2026-10-03T14:00", "temperature_c": 21.1},
        "schedule_context": {},
        "history_context": {
            "home": {"resolved_name": nombre_l, "table": {"team": nombre_l, "source": "thesportsdb"}},
            "away": {"resolved_name": nombre_v, "table": {"team": nombre_v, "source": "thesportsdb"}},
        },
    }


class _ConEspn(unittest.TestCase):
    def setUp(self):
        self.meteo_pedida = []

        def meteo(punto, kickoff):
            self.meteo_pedida.append(kickoff)
            return {"timezone": "Europe/Madrid", "forecast_time": kickoff[:13], "temperature_c": 20.0}

        self.patches = [
            mock.patch.object(w, "_espn_eventos_del_dia", side_effect=lambda slug, dia: EVENTOS_LIGA_F.get(dia, [])),
            mock.patch.object(w, "_espn_clasificacion", return_value={}),
            mock.patch.object(w, "_espn_forma_del_partido", return_value=FORMA),
            mock.patch.object(w, "_espn_proximos_equipo", return_value=[]),
            mock.patch.object(w, "_geocode_location", return_value={"latitude": 41.38, "longitude": 2.12}),
            mock.patch.object(w, "fetch_weather_context", side_effect=meteo),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()


class HoraDeEspnTests(_ConEspn):
    def test_barcelona_madrid_al_domingo_con_su_descanso(self):
        m = _liga_f("Barcelona", "Real Madrid", "BARCELONA (F)", "R.MADRID (F)", "Barcelona Femení", "Real Madrid Femenino")
        cambios = w._aplicar_fuentes_espn(m, AHORA)
        self.assertEqual(m["kickoff"], "2026-10-04T15:00:00Z")
        self.assertEqual(m["kickoff_original"], RELLENO)
        self.assertEqual(m["kickoff_source"], "espn-fixture")
        self.assertEqual(m["kickoff_source_original"], "quiniela_oficial")
        casa, fuera = m["schedule_context"]["home"], m["schedule_context"]["away"]
        self.assertEqual((casa["days_since_last_match"], casa["rest_days_exact"]), (3, 3.9))
        self.assertEqual((fuera["days_since_last_match"], fuera["rest_days_exact"]), (8, 8.2))
        # La meteo se vuelve a pedir, a la hora nueva y en la sede de ESPN.
        self.assertEqual(self.meteo_pedida, ["2026-10-04T15:00:00Z"])
        self.assertEqual(m["weather_context"]["location_city"], "Barcelona")
        self.assertTrue(any("hora" in c and "ESPN" in c for c in cambios))

    def test_madrid_cff_y_tenerife_a_su_hora(self):
        p13 = _liga_f("Madrid CFF", "Athletic Club", "MADRID CFF (F)", "ATH.CLUB (F)", "Madrid CFF", "Athletic Club Women")
        p14 = _liga_f("Tenerife", "Logroño", "TENERIFE (F)", "LOGROÑO (F)", "Tenerife Femenino", "Logroño United")
        w._aplicar_fuentes_espn(p13, AHORA)
        w._aplicar_fuentes_espn(p14, AHORA)
        self.assertEqual(p13["kickoff"], "2026-10-03T16:00:00Z")
        self.assertEqual(p14["kickoff"], "2026-10-04T10:00:00Z")
        self.assertEqual(p14["schedule_context"]["home"]["rest_days_exact"], 7.0)
        self.assertEqual(p14["schedule_context"]["away"]["rest_days_exact"], 7.8)
        self.assertEqual(self.meteo_pedida, ["2026-10-03T16:00:00Z", "2026-10-04T10:00:00Z"])

    def test_la_hora_buena_no_se_toca(self):
        p12 = _liga_f("Deportivo", "Atlético Madrid", "DEPORTIVO (F)", "AT.MADRID (F)",
                      "Deportivo de La Coruña Women", "Atlético Madrid Femenino")
        p12["structured_context"]["event_context"]["stadium_city"] = "La Coruña, Spain"
        w._aplicar_fuentes_espn(p12, AHORA)
        self.assertEqual(p12["kickoff"], RELLENO)
        self.assertNotIn("kickoff_original", p12)
        self.assertEqual(p12["kickoff_source"], "quiniela_oficial")
        self.assertEqual(self.meteo_pedida, [])  # ni sede nueva ni hora nueva

    def test_la_original_es_la_primera(self):
        m = _liga_f("Barcelona", "Real Madrid", "BARCELONA (F)", "R.MADRID (F)", "Barcelona Femení", "Real Madrid Femenino")
        m["kickoff_original"] = "2026-10-03T14:00:00Z"
        m["kickoff"] = "2026-10-04T12:00:00Z"
        w._aplicar_fuentes_espn(m, AHORA)
        self.assertEqual(m["kickoff"], "2026-10-04T15:00:00Z")
        self.assertEqual(m["kickoff_original"], "2026-10-03T14:00:00Z")


class SoloSiElCruceEsInequivocoTests(unittest.TestCase):
    EV = {"espn_event_id": "401882497", "kickoff": "2026-10-04T15:00Z", "local": "Barcelona",
          "visitante": "Real Madrid", "status": "STATUS_SCHEDULED", "time_valid": True, "completed": False}

    def _m(self):
        return {"kickoff": RELLENO, "kickoff_source": "quiniela_oficial"}

    def test_hora_sin_fijar_en_espn(self):
        m = self._m()
        self.assertEqual(w._corregir_kickoff_con_espn(m, {**self.EV, "time_valid": False}, [self.EV]), [])
        self.assertEqual(m["kickoff"], RELLENO)

    def test_aplazado_o_jugado(self):
        for cambio in ({"status": "STATUS_POSTPONED"}, {"status": "STATUS_FULL_TIME", "completed": True}):
            m = self._m()
            self.assertEqual(w._corregir_kickoff_con_espn(m, {**self.EV, **cambio}, [self.EV]), [])
            self.assertEqual(m["kickoff"], RELLENO)

    def test_dos_eventos_del_mismo_cruce(self):
        otro = {**self.EV, "espn_event_id": "999", "kickoff": "2026-10-03T15:00Z"}
        m = self._m()
        self.assertEqual(w._corregir_kickoff_con_espn(m, self.EV, [self.EV, otro]), [])
        self.assertEqual(m["kickoff"], RELLENO)

    def test_quince_minutos_no_es_una_correccion(self):
        m = {"kickoff": "2026-10-04T15:10:00Z"}
        self.assertEqual(w._corregir_kickoff_con_espn(m, self.EV, [self.EV]), [])

    def test_sin_evento_de_espn_no_se_cambia_nada(self):
        m = _liga_f("Sevilla", "Valencia", "SEVILLA (F)", "VALENCIA (F)", "Sevilla Femenino", "Valencia Femenino")
        with mock.patch.object(w, "_espn_eventos_del_dia", side_effect=lambda s, d: EVENTOS_LIGA_F.get(d, [])), \
                mock.patch.object(w, "_espn_clasificacion", return_value={}), \
                mock.patch.object(w, "_espn_forma_del_partido", return_value={}):
            w._aplicar_fuentes_espn(m, AHORA)
        self.assertEqual(m["kickoff"], RELLENO)


class ElBoletoNoLaDevuelveAtrasTests(unittest.TestCase):
    def test_la_misma_hora_de_relleno_no_pisa_a_espn(self):
        m = {"kickoff": "2026-10-04T15:00:00Z", "kickoff_source": "espn-fixture", "kickoff_original": RELLENO,
             "weather_context": {"temperature_c": 22}}
        w._aplicar_horario_oficial(m, {"kickoff": RELLENO})
        self.assertEqual(m["kickoff"], "2026-10-04T15:00:00Z")
        self.assertEqual(m["weather_context"], {"temperature_c": 22})

    def test_una_hora_oficial_nueva_si_manda(self):
        m = {"kickoff": "2026-10-04T15:00:00Z", "kickoff_source": "espn-fixture", "kickoff_original": RELLENO}
        w._aplicar_horario_oficial(m, {"kickoff": "2026-10-04T17:00:00Z"})
        self.assertEqual(m["kickoff"], "2026-10-04T17:00:00Z")
        self.assertEqual(m["kickoff_source"], "quiniela_oficial")


class MeteoALaHoraLocalTests(unittest.TestCase):
    def test_las_horas_de_open_meteo_son_locales(self):
        horas = [f"2026-10-03T{h:02d}:00" for h in range(24)]
        zona = w._zona_de_open_meteo({"timezone": "Europe/Madrid", "utc_offset_seconds": 7200})
        kickoff = datetime(2026, 10, 3, 16, 0, tzinfo=timezone.utc)  # Madrid CFF F: 18:00 en Madrid
        self.assertEqual(horas[w._nearest_index(kickoff, horas, zona)], "2026-10-03T18:00")
        canarias = w._zona_de_open_meteo({"timezone": "Atlantic/Canary"})
        tenerife = datetime(2026, 10, 3, 10, 0, tzinfo=timezone.utc)
        self.assertEqual(horas[w._nearest_index(tenerife, horas, canarias)], "2026-10-03T11:00")

    def test_sin_zona_se_usa_el_desfase(self):
        zona = w._zona_de_open_meteo({"timezone": "GMT", "utc_offset_seconds": 3600})
        self.assertEqual(zona.utcoffset(None).total_seconds(), 3600)

    def test_la_cache_distingue_la_hora(self):
        claves = []
        horas = ["2026-10-03T16:00", "2026-10-03T18:00"]
        campos = ("temperature_2m", "precipitation_probability", "precipitation", "wind_speed_10m",
                  "wind_gusts_10m", "relative_humidity_2m", "weather_code", "cloud_cover", "apparent_temperature")
        respuesta = {"timezone": "Europe/Madrid", "hourly": {"time": horas, **{c: [20, 18] for c in campos}}}
        with mock.patch.object(w, "_cache_get", side_effect=lambda c, k, *a: claves.append(k) or None), \
                mock.patch.object(w, "_cache_set"), \
                mock.patch.object(w, "_request_json", return_value=respuesta):
            a = w.fetch_weather_context({"latitude": 40.28, "longitude": -3.79}, "2026-10-03T14:00:00Z")
            b = w.fetch_weather_context({"latitude": 40.28, "longitude": -3.79}, "2026-10-03T16:00:00Z")
        self.assertNotEqual(claves[0], claves[1])
        self.assertEqual((a["forecast_time"], b["forecast_time"]), ("2026-10-03T16:00", "2026-10-03T18:00"))


def _calendario_andorra():
    """/teams/20179/schedule?fixture=true (esp.2) del 1-oct-2026, recortado."""
    def ev(eid, fecha, local, local_id, visit, visit_id, valida=True):
        return {"id": eid, "date": fecha, "league": {"name": "Spanish LALIGA 2"},
                "competitions": [{"timeValid": valida, "status": {"type": {"completed": False}}, "competitors": [
                    {"homeAway": "home", "team": {"id": local_id, "displayName": local}},
                    {"homeAway": "away", "team": {"id": visit_id, "displayName": visit}}]}]}
    return {"events": [
        ev("401883190", "2026-10-03T16:30Z", "CD Sabadell", "3842", "FC Andorra", "20179"),
        ev("401883188", "2026-10-11T12:00Z", "FC Andorra", "20179", "Castellón", "3771"),
        ev("401883177", "2026-10-18T16:30Z", "Girona", "9812", "FC Andorra", "20179"),
        ev("401883131", "2026-10-26T19:30Z", "FC Andorra", "20179", "Burgos", "3858"),
        ev("401883144", "2026-11-01T20:00Z", "Tenerife", "3843", "FC Andorra", "20179", valida=False),
    ]}


class ProximosDelAndorraTests(unittest.TestCase):
    def _p4(self, proximos=None):
        return {
            "local": "Sabadell FC", "visitante": "Andorra CF", "visitante_lae": "ANDORRA FC",
            "league": "soccer_spain_segunda_division", "kickoff": "2026-10-03T16:30:00Z",
            "history_context": {"home": {"table": {}}, "away": {"table": {
                "team": "Andorra CF", "position": 17, "source": "espn-standings", "espn_team_id": "20179",
                "league_name": "Spanish LALIGA 2"}}},
            "competition_context": {"home_upcoming": [], "away_upcoming": proximos if proximos is not None else []},
        }

    def test_parser(self):
        filas = fe.proximos_de_calendario(_calendario_andorra(), "20179")
        self.assertEqual([f["opponent"] for f in filas], ["CD Sabadell", "Castellón", "Girona", "Burgos", "Tenerife"])
        self.assertEqual(filas[1]["venue"], "home")
        self.assertFalse(filas[4]["time_valid"])

    def test_se_rellena_sin_el_partido_de_la_jornada(self):
        m = self._p4()
        filas = fe.proximos_de_calendario(_calendario_andorra(), "20179")
        with mock.patch.object(w, "_espn_proximos_equipo", return_value=filas) as pedido:
            cambios = w._rellenar_proximos_con_espn(m, "esp.2")
        pedido.assert_called_once_with("esp.2", "20179")
        proximos = m["competition_context"]["away_upcoming"]
        self.assertEqual([f["opponent"] for f in proximos], ["Castellón", "Girona", "Burgos", "Tenerife"])
        self.assertEqual(proximos[0]["league"], "soccer_spain_segunda_division")
        self.assertEqual(proximos[0]["source"], "espn-schedule")
        self.assertEqual(proximos[3]["kickoff"], "2026-11-01T00:00:00Z")  # hora sin fijar: solo el dia
        self.assertTrue(cambios)
        # El local no tiene fila de ESPN: no se le inventa nada.
        self.assertEqual(m["competition_context"]["home_upcoming"], [])

    def test_un_calendario_que_ya_hay_no_se_toca(self):
        ya = [{"kickoff": "2026-10-11T12:00:00Z", "venue": "home", "opponent": "Castellón", "source": "sportsdb-next"}]
        m = self._p4(list(ya))
        with mock.patch.object(w, "_espn_proximos_equipo", side_effect=AssertionError("no se pide")):
            self.assertEqual(w._rellenar_proximos_con_espn(m, "esp.2"), [])
        self.assertEqual(m["competition_context"]["away_upcoming"], ya)

    def test_sin_lista_no_se_inventa(self):
        m = self._p4()
        del m["competition_context"]["away_upcoming"]
        with mock.patch.object(w, "_espn_proximos_equipo", side_effect=AssertionError("no se pide")):
            self.assertEqual(w._rellenar_proximos_con_espn(m, "esp.2"), [])


if __name__ == "__main__":
    unittest.main()
