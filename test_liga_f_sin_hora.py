"""J11 2026-27: la Liga F se quedo sin descanso, meteo, proximos ni tabla fresca.

Al pasar a ser la jornada en curso, la J11 dejo de salir en "proximas" de
Eduardo (de ahi venia la hora de los cuatro de Liga F) y la pagina de la
jornada no trae horas. La de las 12:14 (ventana de jornadas rota) la habia
podado del historico, asi que al reconstruirla no habia hora guardada: kickoff
"" en P11-P14 y _aplicar_fuentes_espn se salia en la primera linea.

Tambien: J12 P12 Mallorca-Las Palmas salia como LaLiga por el historico,
cuando en 26/27 los dos juegan en Segunda.
"""

import copy
import os
import unittest
from datetime import datetime, timezone
from unittest import mock

os.environ.setdefault("OPENAI_API_KEY", "x")
os.environ.setdefault("ANTHROPIC_API_KEY", "x")

import snapshot_worker as w

AHORA = datetime(2026, 10, 2, 15, 8, tzinfo=timezone.utc)


def _ev(eid, local, visitante, kickoff, **extra):
    evento = {
        "espn_event_id": eid,
        "local": local,
        "visitante": visitante,
        "kickoff": kickoff,
        "time_valid": True,
        "status": "STATUS_SCHEDULED",
        "completed": False,
        "venue": "",
        "city": "",
        "country": "Spain",
        "league_name": "Spanish Liga F",
        "odds": {},
    }
    evento.update(extra)
    return evento


# Lo que ESPN (esp.w.1) publicaba para el 3 y el 4 de octubre.
ESPN_LIGA_F = {
    "20261003": [
        _ev("401882498", "Espanyol", "Alavés", "2026-10-03T10:00Z"),
        _ev("401882494", "Sevilla", "FC Badalona", "2026-10-03T10:00Z"),
        _ev("401882499", "Deportivo", "Atlético Madrid", "2026-10-03T14:00Z"),
        _ev("401882495", "Madrid CFF", "Athletic Club", "2026-10-03T16:00Z"),
    ],
    "20261004": [
        _ev("401882500", "CD Tenerife", "Dux Logroño", "2026-10-04T10:00Z"),
        _ev("401882496", "Granada", "Eibar", "2026-10-04T10:00Z"),
        _ev("401882497", "Barcelona", "Real Madrid", "2026-10-04T15:00Z"),
        _ev("401882493", "Valencia", "Real Sociedad", "2026-10-04T17:30Z"),
    ],
}


def _liga_f(local, visitante, local_lae, visitante_lae):
    return {
        "local": local,
        "visitante": visitante,
        "local_lae": local_lae,
        "visitante_lae": visitante_lae,
        "league": "sportsdb_5106",
        "league_name": "Liga F",
        "kickoff": "",
        "gender": "female",
    }


def _jornada_11():
    hombres = [
        {"local": "Albacete", "visitante": "SD Eibar", "league": "soccer_spain_segunda_division",
         "kickoff": "2026-10-03T12:00:00Z"},
        {"local": "Córdoba", "visitante": "Tenerife", "league": "soccer_spain_segunda_division",
         "kickoff": "2026-10-05T18:30:00Z"},
    ]
    mujeres = [
        _liga_f("BARCELONA", "Real Madrid", "BARCELONA (F)", "R.MADRID (F)"),
        _liga_f("DEPORTIVO", "Atlético Madrid", "DEPORTIVO (F)", "AT.MADRID (F)"),
        _liga_f("MADRID CFF", "Athletic Bilbao", "MADRID CFF (F)", "ATH.CLUB (F)"),
        _liga_f("TENERIFE", "LOGROÑO", "TENERIFE (F)", "LOGROÑO (F)"),
    ]
    return {
        "jornada": 11,
        "kickoff_from": "2026-10-03T12:00:00Z",
        "kickoff_to": "2026-10-05T18:30:00Z",
        "matches": hombres + mujeres,
    }


def _marcador(eventos_por_dia):
    def eventos(slug, dia):
        if slug != "esp.w.1":
            return []
        return copy.deepcopy(eventos_por_dia.get(dia, []))
    return eventos


class HoraDeLaLigaFDesdeESPN(unittest.TestCase):
    def _rellenar(self, jornada, eventos_por_dia=ESPN_LIGA_F):
        ventana = w._ventana_de_la_jornada(jornada)
        cambios = {}
        with mock.patch.object(w, "_espn_eventos_del_dia", side_effect=_marcador(eventos_por_dia)):
            for match in jornada["matches"]:
                cambios[match["local"]] = w._kickoff_de_espn_si_falta(match, ventana, ahora=AHORA)
        return cambios

    def test_sin_hora_el_contraste_con_espn_no_hace_nada(self):
        # Lo que pasaba en Windows: ni descanso, ni meteo, ni tabla de ESPN.
        match = _liga_f("BARCELONA", "Real Madrid", "BARCELONA (F)", "R.MADRID (F)")
        with mock.patch.object(w, "_espn_eventos_del_dia", side_effect=AssertionError("no deberia llegar")):
            self.assertEqual(w._aplicar_fuentes_espn(match, ahora=AHORA), [])

    def test_ventana_de_la_jornada(self):
        desde, hasta = w._ventana_de_la_jornada(_jornada_11())
        self.assertEqual(desde, datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc))
        self.assertEqual(hasta, datetime(2026, 10, 5, 18, 30, tzinfo=timezone.utc))
        self.assertIsNone(w._ventana_de_la_jornada({"matches": [{"kickoff": ""}]}))

    def test_los_cuatro_de_liga_f_recuperan_la_hora_de_espn(self):
        jornada = _jornada_11()
        self._rellenar(jornada)
        horas = {m["local"]: m["kickoff"] for m in jornada["matches"]}
        self.assertEqual(horas["BARCELONA"], "2026-10-04T15:00:00Z")
        self.assertEqual(horas["DEPORTIVO"], "2026-10-03T14:00:00Z")
        self.assertEqual(horas["MADRID CFF"], "2026-10-03T16:00:00Z")
        self.assertEqual(horas["TENERIFE"], "2026-10-04T10:00:00Z")
        for m in jornada["matches"][2:]:
            self.assertEqual(m["kickoff_source"], "espn-fixture")
            self.assertEqual(m["kickoff_source_original"], "sin-hora")

    def test_la_hora_que_ya_hay_no_se_toca(self):
        jornada = _jornada_11()
        cambios = self._rellenar(jornada)
        self.assertEqual(cambios["Albacete"], [])
        self.assertEqual(jornada["matches"][0]["kickoff"], "2026-10-03T12:00:00Z")
        self.assertNotIn("kickoff_source", jornada["matches"][0])

    def test_sin_ventana_no_se_inventa_nada(self):
        match = _liga_f("BARCELONA", "Real Madrid", "BARCELONA (F)", "R.MADRID (F)")
        with mock.patch.object(w, "_espn_eventos_del_dia", side_effect=_marcador(ESPN_LIGA_F)):
            self.assertEqual(w._kickoff_de_espn_si_falta(match, None, ahora=AHORA), [])
        self.assertEqual(match["kickoff"], "")

    def test_cruce_repetido_en_la_ventana_no_se_elige(self):
        dias = copy.deepcopy(ESPN_LIGA_F)
        dias["20261005"] = [_ev("999", "Barcelona", "Real Madrid", "2026-10-05T18:00Z")]
        jornada = _jornada_11()
        self._rellenar(jornada, dias)
        self.assertEqual(jornada["matches"][2]["kickoff"], "")

    def test_hora_sin_confirmar_o_partido_aplazado_no_vale(self):
        for extra in ({"time_valid": False}, {"status": "STATUS_POSTPONED"}, {"completed": True}):
            dias = copy.deepcopy(ESPN_LIGA_F)
            dias["20261004"] = [
                _ev("401882497", "Barcelona", "Real Madrid", "2026-10-04T15:00Z", **extra)
            ]
            jornada = _jornada_11()
            self._rellenar(jornada, dias)
            self.assertEqual(jornada["matches"][2]["kickoff"], "", extra)

    def test_fuera_de_la_ventana_no_vale(self):
        # El mismo cruce de la segunda vuelta no es este partido.
        dias = {"20261004": [], "20270214": [_ev("1", "Barcelona", "Real Madrid", "2027-02-14T15:00Z")]}
        jornada = _jornada_11()
        self._rellenar(jornada, dias)
        self.assertEqual(jornada["matches"][2]["kickoff"], "")

    def test_jornada_ya_jugada_no_se_consulta(self):
        jornada = _jornada_11()
        ventana = w._ventana_de_la_jornada(jornada)
        tarde = datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)
        with mock.patch.object(w, "_espn_eventos_del_dia", side_effect=AssertionError("no deberia consultar")):
            self.assertEqual(w._kickoff_de_espn_si_falta(jornada["matches"][2], ventana, ahora=tarde), [])

    def test_con_hora_el_contraste_ya_entra(self):
        jornada = _jornada_11()
        self._rellenar(jornada)
        barcelona = jornada["matches"][2]
        llamado = {}

        def descanso(match, evento):
            llamado["evento"] = evento.get("espn_event_id")
            return []

        with mock.patch.object(w, "_espn_eventos_del_dia", side_effect=_marcador(ESPN_LIGA_F)), \
                mock.patch.object(w, "_aplicar_evento_espn", return_value=[]), \
                mock.patch.object(w, "_descanso_con_espn", side_effect=descanso), \
                mock.patch.object(w, "_refrescar_tabla_con_espn", return_value=["tabla"]) as tabla, \
                mock.patch.object(w, "_rellenar_proximos_con_espn", return_value=[]):
            w._aplicar_fuentes_espn(barcelona, ahora=AHORA)
        self.assertEqual(llamado.get("evento"), "401882497")
        # Y con ella la tabla de ESPN (Real Madrid 2o con 5 jugados, no 4o con 2).
        tabla.assert_called_once()


def _fila(local, visitante, fecha, season):
    return {"Date": fecha, "HomeTeam": local, "AwayTeam": visitante, "FTHG": 1, "FTAG": 0,
            "FTR": "H", "Season": season}


class LaLigaDeHoyPesaMasQueElHistorico(unittest.TestCase):
    def _historicos(self, con_temporada_en_curso=True):
        laliga = [
            _fila("Mallorca", "Las Palmas", "2024-11-03", "2425"),
            _fila("Las Palmas", "Mallorca", "2025-04-12", "2425"),
            _fila("Real Madrid", "Barcelona", "2026-04-20", "2526"),
        ]
        segunda = [
            _fila("Mallorca", "Las Palmas", "2019-02-10", "1819"),
        ]
        if con_temporada_en_curso:
            laliga.append(_fila("Real Madrid", "Barcelona", "2026-09-20", "2627"))
            segunda += [
                _fila("Mallorca", "Burgos", "2026-09-13", "2627"),
                _fila("Las Palmas", "Granada", "2026-09-14", "2627"),
            ]
        return {"soccer_spain_la_liga": laliga, "soccer_spain_segunda_division": segunda}

    def test_mallorca_las_palmas_es_segunda_en_26_27(self):
        with mock.patch.object(w, "_league_season_code_for", return_value="2627"):
            self.assertEqual(
                w._infer_league_from_histories("Mallorca", "Las Palmas", self._historicos()),
                "soccer_spain_segunda_division",
            )

    def test_sin_temporada_en_curso_se_queda_lo_de_siempre(self):
        with mock.patch.object(w, "_league_season_code_for", return_value="2627"):
            self.assertEqual(
                w._infer_league_from_histories(
                    "Mallorca", "Las Palmas", self._historicos(con_temporada_en_curso=False)
                ),
                "soccer_spain_la_liga",
            )

    def test_los_de_primera_siguen_en_primera(self):
        with mock.patch.object(w, "_league_season_code_for", return_value="2627"):
            self.assertEqual(
                w._infer_league_from_histories("Real Madrid", "Barcelona", self._historicos()),
                "soccer_spain_la_liga",
            )


class LaDeduccionDeOtroCicloSeRehace(unittest.TestCase):
    def _bootstrap(self, match, histories):
        vacio = {"profile": {}, "news": {"items": [], "signals": {}}}
        with mock.patch.object(w, "fetch_league_history", side_effect=lambda clave, *_a, **_k: list(histories.get(w._canonical_league_key(clave), []))), \
                mock.patch.object(w, "fetch_the_sportsdb_team", return_value={}), \
                mock.patch.object(w, "fetch_the_sportsdb_h2h_events", return_value=[]), \
                mock.patch.object(w, "_resolve_sportsdb_event", return_value={}), \
                mock.patch.object(w, "_enrich_team", return_value=vacio), \
                mock.patch.object(w, "fetch_team_profile", return_value={}), \
                mock.patch.object(w, "fetch_weather_context", return_value={}), \
                mock.patch.object(w, "_request_json", side_effect=RuntimeError("sin red en tests")), \
                mock.patch.object(w, "_cache_get", return_value=None), \
                mock.patch.object(w, "_cache_set", return_value=None), \
                mock.patch.object(w, "_league_season_code_for", return_value="2627"):
            try:
                w._bootstrap_quiniela_placeholder(match, [], {}, histories)
            except Exception as exc:  # la liga se decide al principio
                match["_error_test"] = repr(exc)

    def test_mallorca_guardado_como_laliga_pasa_a_segunda(self):
        # Asi estaba en el historico de jornadas publicado a las 17:08.
        match = {
            "local": "MALLORCA",
            "visitante": "LAS PALMAS",
            "local_lae": "MALLORCA",
            "visitante_lae": "LAS PALMAS",
            "league": "soccer_spain_la_liga",
            "league_name": "LaLiga",
            "league_id": "4335",
            "league_source": "history-team-membership",
            "kickoff": "2026-10-11T16:30:00Z",
            "market_context": {"normalized_percent": {}},
        }
        self._bootstrap(match, LaLigaDeHoyPesaMasQueElHistorico()._historicos())
        self.assertEqual(match["league"], "soccer_spain_segunda_division")
        self.assertEqual(match["league_source"], "history-team-membership")

    def test_la_liga_del_feed_de_cuotas_no_se_toca(self):
        match = {
            "local": "MALLORCA",
            "visitante": "LAS PALMAS",
            "league": "soccer_spain_la_liga",
            "league_source": "feed-de-cuotas",
            "kickoff": "2026-10-11T16:30:00Z",
            "market_context": {"normalized_percent": {}},
        }
        self._bootstrap(match, LaLigaDeHoyPesaMasQueElHistorico()._historicos())
        self.assertEqual(match["league"], "soccer_spain_la_liga")


if __name__ == "__main__":
    unittest.main()
