"""J11, P11: BARCELONA (F) - R.MADRID (F) se quedaba en "league_unresolved".

El partido viaja con el nombre canonizado ("BARCELONA", "Real Madrid") y la
marca (F) solo en el del boleto (local_lae / visitante_lae). Con esos nombres:

1. el historico de LaLiga masculina tiene a los dos clubes, asi que la liga se
   deducia como soccer_spain_la_liga (id 4335);
2. la guarda de categoria la descartaba (league_descartada);
3. el ultimo recurso a Liga F miraba "(f)" solo en el nombre canonizado y no
   lo encontraba: "Liga no resuelta", tabla vacia, "sin registro en 25/26".

P12-P14 no lo sufrian porque sus clubes masculinos no comparten liga.
"""

import os
import unittest
from unittest import mock

os.environ.setdefault("OPENAI_API_KEY", "x")
os.environ.setdefault("ANTHROPIC_API_KEY", "x")

import snapshot_worker as w


def _fila(local, visitante, fecha, season="2526", gl=1, gv=0):
    return {
        "Date": fecha,
        "HomeTeam": local,
        "AwayTeam": visitante,
        "FTHG": gl,
        "FTAG": gv,
        "FTR": "H" if gl > gv else ("A" if gv > gl else "D"),
        "Season": season,
    }


HISTORICO_LALIGA = [
    _fila("Barcelona", "Real Madrid", "2026-05-10"),
    _fila("Real Madrid", "Barcelona", "2025-10-26"),
]
HISTORICO_LIGA_F = [
    # 26/27: dos jornadas, como la J11 real.
    _fila("Barcelona Femení", "Granada Femenino", "2026-09-06", season="2627", gl=4, gv=0),
    _fila("Real Madrid Femenino", "Sevilla Femenino", "2026-09-07", season="2627", gl=2, gv=1),
    _fila("Levante Femenino", "Barcelona Femení", "2026-09-13", season="2627", gl=0, gv=3),
    _fila("Granada Femenino", "Real Madrid Femenino", "2026-09-14", season="2627", gl=1, gv=1),
    _fila("Sevilla Femenino", "Levante Femenino", "2026-09-06", season="2627", gl=1, gv=1),
    _fila("Granada Femenino", "Sevilla Femenino", "2026-09-13", season="2627", gl=0, gv=0),
    # 25/26: las dos jugaron Liga F.
    _fila("Barcelona Femení", "Real Madrid Femenino", "2026-03-01", season="2526", gl=3, gv=1),
    _fila("Real Madrid Femenino", "Barcelona Femení", "2025-11-09", season="2526", gl=0, gv=2),
]
# Lo que TheSportsDB devuelve si se le pregunta con el nombre del primer equipo.
H2H_MASCULINO = [_fila("Barcelona", "Real Madrid", "2025-05-11", gl=4, gv=3)]

FICHAS_MASCULINAS = {
    "BARCELONA (F)": {"strTeam": "Barcelona", "idLeague": "4335", "strLeague": "Spanish La Liga", "strGender": "Male"},
    "R.MADRID (F)": {"strTeam": "Real Madrid", "idLeague": "4335", "strLeague": "Spanish La Liga", "strGender": "Male"},
    "BARCELONA": {"strTeam": "Barcelona", "idLeague": "4335", "strLeague": "Spanish La Liga", "strGender": "Male"},
    "Real Madrid": {"strTeam": "Real Madrid", "idLeague": "4335", "strLeague": "Spanish La Liga", "strGender": "Male"},
}


def _partido_p11(**extra):
    match = {
        "local": "BARCELONA",
        "visitante": "Real Madrid",
        "local_lae": "BARCELONA (F)",
        "visitante_lae": "R.MADRID (F)",
        "league": "",
        "kickoff": "2026-10-04T15:00:00Z",
        "quiniela_slots": [{"jornada": 11, "position": 11}],
        "market_context": {"normalized_percent": {}},
    }
    match.update(extra)
    return match


class LigaFSinMarcaEnElNombre(unittest.TestCase):
    def setUp(self):
        self.h2h_pedidos = []

    def _h2h(self, local, visitante, *_a, **_k):
        self.h2h_pedidos.append((local, visitante))
        if "(F)" in f"{local}{visitante}":
            return []
        return list(H2H_MASCULINO)

    def _bootstrap(self, match, histories=None):
        pedidas = []

        def historico(clave, *_a, **_k):
            pedidas.append(clave)
            if w._canonical_league_key(clave) == "sportsdb_5106":
                return list(HISTORICO_LIGA_F)
            if w._canonical_league_key(clave) == "soccer_spain_la_liga":
                return list(HISTORICO_LALIGA)
            return []

        histories = {"soccer_spain_la_liga": list(HISTORICO_LALIGA)} if histories is None else histories
        vacio = {"profile": {}, "news": {"items": [], "signals": {}}}
        with mock.patch.object(w, "fetch_league_history", side_effect=historico), \
                mock.patch.object(w, "fetch_the_sportsdb_team", side_effect=lambda n, *_a, **_k: dict(FICHAS_MASCULINAS.get(n, {}))), \
                mock.patch.object(w, "_resolve_sportsdb_event", return_value={}), \
                mock.patch.object(w, "fetch_the_sportsdb_h2h_events", side_effect=self._h2h), \
                mock.patch.object(w, "_enrich_team", return_value=vacio), \
                mock.patch.object(w, "fetch_team_profile", return_value={}), \
                mock.patch.object(w, "fetch_weather_context", return_value={}), \
                mock.patch.object(w, "_request_json", side_effect=RuntimeError("sin red en tests")), \
                mock.patch.object(w, "_cache_get", return_value=None), \
                mock.patch.object(w, "_cache_set", return_value=None):
            try:
                w._bootstrap_quiniela_placeholder(match, [], {}, histories)
            except Exception as exc:  # lo que interesa es la liga, que se decide antes
                match.setdefault("_error_test", repr(exc))
        return pedidas

    def test_p11_resuelve_liga_f_y_no_laliga(self):
        match = _partido_p11()
        pedidas = self._bootstrap(match)
        self.assertEqual(match["league"], "sportsdb_5106")
        self.assertEqual(match["league_id"], "5106")
        self.assertEqual(match["league_name"], "Liga F")
        self.assertEqual(match["league_source"], "quiniela-placeholder-inferred")
        # Queda constancia de lo descartado, pero no se usa.
        self.assertEqual(match.get("league_descartada"), "soccer_spain_la_liga")
        self.assertIn("sportsdb_5106", [w._canonical_league_key(c) for c in pedidas])
        self.assertNotIn("league_unresolved", pedidas)
        self.assertNotIn("_error_test", match)

    def test_p11_forma_tabla_y_h2h_salen_de_liga_f(self):
        match = _partido_p11()
        self._bootstrap(match)
        self.assertNotIn("_error_test", match)
        historia = match["history_context"]
        self.assertEqual(historia.get("gender"), "female")
        for lado, nombre in (("home", "Barcelona Femení"), ("away", "Real Madrid Femenino")):
            self.assertEqual(historia[lado]["resolved_name"], nombre)
            self.assertEqual(historia[lado]["table"]["played"], 2)
        # Los puntos son los de Liga F (Barcelona 6, Madrid 4), no los del Clasico.
        self.assertEqual(historia["home"]["table"]["points"], 6)
        self.assertEqual(historia["away"]["table"]["points"], 4)
        fechas_h2h = {p["date"] for p in historia["head_to_head"].get("recent_matches", [])}
        self.assertEqual(fechas_h2h, {"2026-03-01", "2025-11-09"})
        # Las dos jugaron Liga F 25/26: nada de "sin registro en 25/26".
        previa = match["competition_context"]["season_preview"]
        for lado in ("home", "away"):
            self.assertEqual(previa[lado]["last_season_code"], "2526")
            self.assertNotIn("sin registro", previa[lado]["summary"])
        # TheSportsDB se consulta con los nombres del boleto, no con los del masculino.
        self.assertTrue(self.h2h_pedidos)
        for local, visitante in self.h2h_pedidos:
            self.assertIn("(F)", local)
            self.assertIn("(F)", visitante)

    def test_p11_aunque_venga_marcado_como_no_resuelto_de_otro_ciclo(self):
        # Asi estaba guardado en el historico de jornadas (347efe1).
        match = _partido_p11(
            league="league_unresolved",
            league_name="Liga no resuelta",
            league_id="4335",
            league_source="quiniela-placeholder",
            league_descartada="soccer_spain_la_liga",
            gender="female",
        )
        self._bootstrap(match)
        self.assertEqual(match["league"], "sportsdb_5106")
        self.assertEqual(match["league_id"], "5106")
        self.assertEqual(match["league_name"], "Liga F")

    def test_masculino_con_los_mismos_clubes_sigue_en_laliga(self):
        match = {
            "local": "BARCELONA",
            "visitante": "Real Madrid",
            "local_lae": "BARCELONA",
            "visitante_lae": "R.MADRID",
            "league": "",
            "kickoff": "2026-10-25T15:00:00Z",
            "market_context": {"normalized_percent": {}},
        }
        self._bootstrap(match)
        self.assertEqual(match["league"], "soccer_spain_la_liga")
        self.assertNotIn("league_descartada", match)

    def test_nombre_con_marca_sigue_funcionando(self):
        match = _partido_p11(local="BARCELONA (F)", visitante="R.MADRID (F)")
        self._bootstrap(match, histories={})
        self.assertEqual(match["league"], "sportsdb_5106")
        self.assertEqual(match["league_id"], "5106")

    def test_selecciones_femeninas_no_van_a_liga_f(self):
        match = {
            "local": "España",
            "visitante": "Suecia",
            "local_lae": "ESPAÑA (F)",
            "visitante_lae": "SUECIA (F)",
            "league": "",
            "kickoff": "2026-10-24T18:00:00Z",
            "market_context": {"normalized_percent": {}},
        }
        if not w._es_partido_de_selecciones(match):
            self.skipTest("el detector de selecciones no reconoce este cruce")
        self._bootstrap(match, histories={})
        self.assertNotEqual(match["league"], "sportsdb_5106")


if __name__ == "__main__":
    unittest.main()
