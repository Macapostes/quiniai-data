"""Calidad del feed: cuotas de consenso, cuotas de respaldo de ESPN, regimen
de tabla de Liga F, descanso real, rumores caducados y el id interno de liga.

Se ejecuta como los demas: python test_worker_calidad.py
"""

import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import fuentes_espn as fe  # noqa: E402
import snapshot_worker as w  # noqa: E402
from test_feed_limpio_2 import LIGA_F, _partido_liga_f, _tabla  # noqa: E402

AHORA = datetime(2026, 9, 29, 16, 0, tzinfo=timezone.utc)


def _book(key, title, uno, x, dos, home="Albacete", away="Eibar"):
    return {"key": key, "title": title, "markets": [{"key": "h2h", "outcomes": [
        {"name": home, "price": uno}, {"name": "Draw", "price": x}, {"name": away, "price": dos}]}]}


class CuotasDeConsensoTests(unittest.TestCase):
    def test_pinnacle_aunque_no_sea_la_primera(self):
        odds, casa = w._best_h2h([_book("pmu_fr", "PMU (FR)", 2.35, 2.95, 2.48),
                                  _book("pinnacle", "Pinnacle", 2.52, 3.1, 2.8)], "Albacete", "Eibar")
        self.assertEqual(casa, "Pinnacle")
        self.assertEqual((odds["Albacete"], odds["Draw"], odds["Eibar"]), (2.52, 3.1, 2.8))

    def test_sin_pinnacle_mediana(self):
        odds, casa = w._best_h2h([_book("a", "A", 2.4, 3.0, 2.9), _book("b", "B", 2.5, 3.1, 2.8),
                                  _book("c", "C", 4.0, 5.0, 1.5)], "Albacete", "Eibar")
        self.assertEqual(casa, "mediana de 3 casas")
        self.assertEqual((odds["Albacete"], odds["Draw"], odds["Eibar"]), (2.5, 3.1, 2.8))

    def test_una_sola_casa_y_casas_rotas(self):
        rota = {"key": "x", "title": "X", "markets": [{"key": "h2h", "outcomes": [{"name": "Albacete", "price": "n/a"}]}]}
        odds, casa = w._best_h2h([rota, _book("z", "Z", 2.2, 3.0, 3.1)], "Albacete", "Eibar")
        self.assertEqual(casa, "Z")
        self.assertEqual(w._best_h2h([], "Albacete", "Eibar"), ({}, ""))
        self.assertEqual(w._best_h2h([rota], "Albacete", "Eibar"), ({}, ""))


def _comp_con_cuotas(home="+165", draw="+225", away="+155"):
    return {"odds": [{"provider": {"name": "DraftKings"}, "moneyline": {
        "home": {"close": {"odds": home}}, "draw": {"close": {"odds": draw}}, "away": {"close": {"odds": away}}}}]}


class CuotasDeEspnTests(unittest.TestCase):
    def test_americana_a_decimal(self):
        self.assertEqual(fe.americana_a_decimal("+165"), 2.65)
        self.assertEqual(fe.americana_a_decimal("-215"), 1.47)
        self.assertEqual(fe.americana_a_decimal("EVEN"), 2.0)
        self.assertIsNone(fe.americana_a_decimal("+50"))
        self.assertIsNone(fe.americana_a_decimal(None))

    def test_cuotas_del_marcador(self):
        self.assertEqual(fe.cuotas_de_competicion(_comp_con_cuotas()),
                         {"1": 2.65, "X": 3.25, "2": 2.55, "bookmaker": "DraftKings"})

    def test_liga_f_sin_cuotas_en_espn(self):
        self.assertEqual(fe.cuotas_de_competicion({"odds": [None]}), {})
        self.assertEqual(fe.cuotas_de_competicion({}), {})

    def test_respaldo_solo_si_el_partido_no_tiene_cuotas(self):
        m = {"local": "Albacete", "visitante": "Eibar", "market_context": {"official_percent": {"1": 40}}}
        cambios = w._cuotas_de_respaldo_espn(m, {"odds": {"1": 2.65, "X": 3.25, "2": 2.55, "bookmaker": "DraftKings"},
                                                 "slug": "esp.2"})
        self.assertTrue(cambios)
        self.assertEqual(m["odds"], {"1": 2.65, "X": 3.25, "2": 2.55})
        self.assertEqual(m["odds_source"], "espn-scoreboard")
        self.assertFalse(m.get("odds_femenino"))
        self.assertEqual(m["market_context"]["official_percent"], {"1": 40})
        self.assertAlmostEqual(sum(m["market_context"]["normalized_percent"].values()), 100, delta=0.1)
        # Con cuotas de The Odds API no se toca nada.
        m2 = {"odds": {"1": 2.1, "X": 3.2, "2": 3.6}}
        self.assertEqual(w._cuotas_de_respaldo_espn(m2, {"odds": {"1": 9, "X": 9, "2": 9}}), [])
        self.assertEqual(m2["odds"], {"1": 2.1, "X": 3.2, "2": 3.6})

    def test_si_espn_publica_liga_f_van_marcadas_como_femeninas(self):
        m = {}
        w._cuotas_de_respaldo_espn(m, {"odds": {"1": 1.3, "X": 5.0, "2": 8.0, "bookmaker": "DraftKings"},
                                       "slug": fe.ESPN_SLUG_LIGA_F})
        self.assertTrue(m["odds_femenino"])
        self.assertTrue(w._monitor_match_payload(m)["odds_femenino"])


class RegimenDeTablaLigaFTests(unittest.TestCase):
    def _liga_f_completa(self):
        filas = [("Barcelona", 5, 15, 21, 2), ("Real Madrid", 5, 13, 13, 6), ("Madrid CFF", 5, 12, 9, 7),
                 ("Athletic Club", 5, 10, 8, 5), ("Atlético Madrid", 5, 6, 8, 10), ("Deportivo", 5, 5, 4, 4),
                 ("Dux Logroño", 5, 6, 5, 6), ("Tenerife", 5, 5, 4, 10), ("Sevilla", 5, 7, 6, 6),
                 ("Real Sociedad", 5, 8, 7, 6), ("Levante", 5, 3, 3, 9), ("Granada", 5, 4, 4, 8),
                 ("Espanyol", 5, 4, 5, 9), ("Eibar", 5, 2, 2, 11), ("Badalona", 5, 5, 5, 7), ("Alhama", 5, 1, 2, 12)]
        return fe.clasificacion(_tabla("Spanish Liga F", filas))

    def test_con_la_tabla_de_espn_deja_de_ser_arranque(self):
        m = _partido_liga_f()
        m["competition_context"] = {
            "table_reliability": {"regime": "preseason", "median_played": 2.0, "positions_usable": False,
                                  "reason": "arranque de temporada: 2 jornadas disputadas"},
            "season_preview": {"active": True, "regime": "preseason", "matchdays_played": 2.0},
        }
        tabla = self._liga_f_completa()
        cambios = w._recalcular_regimen_con_espn(m, tabla, "sportsdb_5106")
        fiab = m["competition_context"]["table_reliability"]
        self.assertEqual(fiab["median_played"], 5.0)
        self.assertNotEqual(fiab["regime"], "preseason")
        self.assertTrue(fiab["positions_usable"])
        self.assertEqual(fiab["source"], "espn-standings")
        self.assertEqual(m["competition_context"]["season_preview"]["matchdays_played"], 5.0)
        self.assertTrue(any("regimen de tabla preseason" in c for c in cambios))
        self.assertTrue(w._positions_publishable(m["competition_context"]))

    def test_nunca_baja_de_jornadas(self):
        m = {"competition_context": {"table_reliability": {"regime": "normal", "median_played": 9.0}}}
        self.assertEqual(w._recalcular_regimen_con_espn(m, self._liga_f_completa(), "sportsdb_5106"), [])
        self.assertEqual(m["competition_context"]["table_reliability"]["regime"], "normal")


class DescansoConEspnTests(unittest.TestCase):
    RESUMEN = {"lastFiveGames": [
        {"team": {"displayName": "Barcelona"}, "events": [
            {"gameDate": "2026-09-26T14:30Z", "gameResult": "W", "score": "2-0", "atVs": "@", "leagueName": "Liga F"},
            {"gameDate": "2026-10-01T19:00Z", "gameResult": "W", "score": "3-1", "atVs": "vs", "leagueName": "UWCL"},
            {"gameDate": "2026-09-20T10:00Z", "gameResult": "D", "score": "1-1", "atVs": "vs", "leagueName": "Liga F"},
        ]},
        {"team": {"displayName": "Real Madrid"}, "events": [
            {"gameDate": "2026-09-27T10:00Z", "gameResult": "W", "score": "1-0", "atVs": "vs", "leagueName": "Liga F"},
        ]},
    ]}

    def test_cuenta_todas_las_competiciones(self):
        ko = datetime(2026, 10, 4, 15, 0, tzinfo=timezone.utc)
        forma = fe.forma_de_resumen(self.RESUMEN)
        d = fe.descanso_desde_forma(forma["Barcelona"], ko)
        self.assertEqual(d["days_since_last_match"], 2)  # UWCL del 1 de octubre
        self.assertEqual(d["matches_last_14_days"], 2)  # el del 20-9 queda a 14 dias y 5 horas
        self.assertEqual(fe.descanso_desde_forma([], ko), {})
        self.assertEqual(fe.descanso_desde_forma(forma["Barcelona"], None), {})

    def test_rellena_el_calendario_y_la_fatiga(self):
        m = _partido_liga_f()
        m["schedule_context"] = {"home": {"days_since_last_match": None, "matches_last_14_days": 0, "fatigue": "unknown"}}
        evento = {"slug": "esp.w.1", "espn_event_id": "401882497", "local": "Barcelona", "visitante": "Real Madrid"}
        with mock.patch.object(w, "_espn_forma_del_partido", return_value=fe.forma_de_resumen(self.RESUMEN)):
            cambios = w._descanso_con_espn(m, evento)
        casa = m["schedule_context"]["home"]
        self.assertEqual((casa["days_since_last_match"], casa["matches_last_14_days"]), (2, 2))
        self.assertEqual(casa["fatigue"], "high")
        self.assertEqual(casa["rest_source"], "espn-summary")
        self.assertEqual(m["schedule_context"]["away"]["days_since_last_match"], 7)
        self.assertEqual(len(cambios), 2)
        monitor = w._monitor_match_payload(m)
        self.assertEqual(monitor["fatigue"]["home"]["days_since_last_match"], 2)
        self.assertEqual(monitor["fatigue"]["home"]["matches_last_14_days"], 2)

    def test_no_pisa_un_descanso_mas_reciente(self):
        m = _partido_liga_f()
        m["schedule_context"] = {"home": {"days_since_last_match": 1, "matches_last_14_days": 4}}
        evento = {"slug": "esp.w.1", "espn_event_id": "1", "local": "Barcelona", "visitante": "Real Madrid"}
        with mock.patch.object(w, "_espn_forma_del_partido", return_value=fe.forma_de_resumen(self.RESUMEN)):
            w._descanso_con_espn(m, evento)
        self.assertEqual(m["schedule_context"]["home"]["days_since_last_match"], 1)

    def test_si_espn_falla_el_partido_sigue(self):
        m = _partido_liga_f()
        with mock.patch.object(w, "_espn_eventos_del_dia", side_effect=RuntimeError("503")), \
                mock.patch.object(w, "_espn_clasificacion", side_effect=RuntimeError("timeout")):
            try:
                w._aplicar_fuentes_espn(m, AHORA)
            except RuntimeError:
                # _espn_eventos_del_dia ya captura sus errores de red; si algo
                # se escapa, el ciclo lo captura (test_feed_limpio_2). Aqui lo
                # importante es que la tabla no deja el partido a medias.
                pass
        with mock.patch.object(w, "_espn_eventos_del_dia", return_value=[]), \
                mock.patch.object(w, "_espn_clasificacion", side_effect=RuntimeError("timeout")):
            self.assertEqual(w._aplicar_fuentes_espn(m, AHORA), [])

    def test_fallo_en_el_descanso_no_tumba_el_resto(self):
        from test_feed_limpio_2 import EV_BARSA
        m = _partido_liga_f()
        with mock.patch.object(w, "_espn_eventos_del_dia", side_effect=lambda s, d: EV_BARSA if (s, d) == ("esp.w.1", "20261004") else []), \
                mock.patch.object(w, "_espn_clasificacion", return_value=LIGA_F), \
                mock.patch.object(w, "_espn_calendario_equipo", return_value=[]), \
                mock.patch.object(w, "_espn_forma_del_partido", side_effect=RuntimeError("boom")), \
                mock.patch.object(w, "_geocode_location", return_value={}):
            cambios = w._aplicar_fuentes_espn(m, AHORA)
        self.assertEqual(m["structured_context"]["event_context"]["venue"], "Spotify Camp Nou")
        self.assertTrue(any(c.startswith("tabla ESPN") for c in cambios))


class RumoresCaducadosTests(unittest.TestCase):
    def _item(self, dias, categoria="signing", estado="reported"):
        fecha = (AHORA - timedelta(days=dias)).strftime("%a, %d %b %Y %H:%M:%S GMT")
        return {"title": f"rumor {dias}", "category": categoria, "fact_status": estado, "published_at": fecha}

    def test_criterio(self):
        self.assertIn("dias", w._rumor_caducado(self._item(30), AHORA))
        self.assertEqual(w._rumor_caducado(self._item(5), AHORA), "")
        # Publicado en agosto con la ventana abierta; en octubre ya cerró.
        agosto = {"title": "fichaje inminente", "category": "departure", "fact_status": "reported",
                  "published_at": "Mon, 24 Aug 2026 10:00:00 GMT"}
        self.assertIn("ventana", w._rumor_caducado(agosto, datetime(2026, 9, 5, tzinfo=timezone.utc)))
        # Un fichaje confirmado es un hecho y no caduca.
        self.assertEqual(w._rumor_caducado(self._item(60, estado="confirmed"), AHORA), "")
        # Lo que no es mercado no se toca.
        self.assertEqual(w._rumor_caducado(self._item(60, categoria="coach"), AHORA), "")
        self.assertEqual(w._rumor_caducado({"category": "signing"}, AHORA), "")

    def test_la_transicion_nueva_ya_no_los_lleva(self):
        with mock.patch.object(w, "datetime", wraps=datetime) as fake:
            fake.now.return_value = AHORA
            t = w._build_team_season_transition("Albacete", {}, {"items": [self._item(40), self._item(3)]})
        self.assertEqual(t["stale_rumours_dropped"], 1)
        self.assertEqual(len(t["transfer_reports"]), 1)

    def test_partido_guardado(self):
        m = {"competition_context": {"season_transition": {"home": {
            "transfer_reports": [self._item(40), self._item(2)], "all_evidence": [self._item(40), self._item(2)],
        }, "away": {}}}}
        self.assertEqual(w._quitar_rumores_caducados(m, AHORA), 1)
        lado = m["competition_context"]["season_transition"]["home"]
        self.assertEqual(len(lado["transfer_reports"]), 1)
        self.assertEqual(lado["evidence_count"], 1)


class IdInternoDeLigaTests(unittest.TestCase):
    def test_liga_f_tiene_nombre(self):
        self.assertEqual(w._league_display_name("sportsdb_5106"), "Liga F")
        self.assertEqual(w._league_display_name("5106"), "Liga F")

    def test_un_id_desconocido_no_se_enseña(self):
        self.assertEqual(w._league_display_name("sportsdb_9999"), "Liga no resuelta")
        self.assertEqual(w._league_display_name("sportsdb_9999", "Liga Nórdica"), "Liga Nórdica")
        self.assertEqual(w._league_display_name("sportsdb_9999", "sportsdb_9999"), "Liga no resuelta")


if __name__ == "__main__":
    unittest.main()
