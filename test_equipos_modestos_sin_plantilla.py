"""Un equipo modesto sin plantilla no puede dejar la jornada sin publicar.

27/10/2026, ciclo de las 14:00: AT.CALATAYUD - CA Osasuna (Copa del Rey). El
Calatayud no tiene plantilla ni noticias en ninguna fuente, TheSportsDB
respondia, y la auditoria rechazo el snapshot entero dos veces. Paso al tercer
intento solo porque el proveedor acabo cortando y se publico "degradado".

La tolerancia es para huecos de equipos modestos o muy pocos huecos. Lo masivo
y la evidencia invalida siguen frenando.
"""

import os
import unittest

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import snapshot_worker as sw  # noqa: E402

_CON_CONTEXTO = {"previous_season": {"summary": "3o el ano pasado"}}


def _partido(local, visitante, liga, home_ctx=None, away_ctx=None, historia=None):
    return {
        "local": local,
        "visitante": visitante,
        "league": liga,
        "history_context": historia or {},
        "competition_context": {
            "season_transition": {
                "home": _CON_CONTEXTO if home_ctx is None else home_ctx,
                "away": _CON_CONTEXTO if away_ctx is None else away_ctx,
            }
        },
    }


def _copa_calatayud():
    return _partido(
        "AT.CALATAYUD",
        "CA Osasuna",
        "soccer_spain_copa_del_rey",
        home_ctx={},
        historia={
            "home": {},
            "away": {"league_key": "soccer_spain_la_liga", "league_scope": "domestic"},
        },
    )


def _liga(n, **kw):
    return _partido(f"Local {n}", f"Visitante {n}", "soccer_spain_la_liga", **kw)


class BaseAuditoria(unittest.TestCase):
    def setUp(self):
        sw._reiniciar_fallos_sportsdb()

    def tearDown(self):
        sw._reiniciar_fallos_sportsdb()

    def auditar(self, partidos):
        return sw._audit_season_transition_snapshot({"quiniela_focus_matches": partidos})


class EquipoModestoTests(BaseAuditoria):
    def test_el_calatayud_solo_no_frena_la_jornada(self):
        """El caso exacto del 27/10, con el proveedor respondiendo bien."""
        partidos = [_copa_calatayud()] + [_liga(i) for i in range(14)]
        audit = self.auditar(partidos)
        self.assertTrue(audit["ok"])
        self.assertFalse(audit["degraded"])
        self.assertEqual(audit["empty_side_count"], 1)
        self.assertEqual(audit["tolerated_empty_sides"][0]["team"], "AT.CALATAYUD")
        self.assertTrue(audit["tolerated_empty_sides"][0]["modest"])

    def test_el_modesto_se_tolera_aunque_el_partido_vaya_solo(self):
        """Aunque sea 1 de 2 lados: es de un equipo modesto en copa."""
        audit = self.auditar([_copa_calatayud()])
        self.assertTrue(audit["ok"])

    def test_osasuna_en_copa_no_es_modesto(self):
        partido = _copa_calatayud()
        self.assertFalse(sw._lado_de_equipo_modesto(partido, "away"))
        self.assertTrue(sw._lado_de_equipo_modesto(partido, "home"))

    def test_en_un_cruce_europeo_nadie_es_modesto(self):
        partido = _partido("B.MUNICH", "Bodo Glimt", "soccer_uefa_champs_league", home_ctx={})
        self.assertFalse(sw._lado_de_equipo_modesto(partido, "home"))


class MuchosHuecosTests(BaseAuditoria):
    def test_muchos_lados_vacios_frenan(self):
        """Mas huecos que el maximo, aunque sean todos de copa: eso es una averia."""
        partidos = [
            _partido(f"Modesto {i}", f"Otro {i}", "soccer_spain_copa_del_rey", home_ctx={})
            for i in range(sw.AUDIT_MAX_LADOS_SIN_CONTEXTO + 1)
        ] + [_liga(i) for i in range(10)]
        audit = self.auditar(partidos)
        self.assertFalse(audit["ok"])
        self.assertEqual(audit["tolerated_empty_sides"], [])

    def test_un_grande_vacio_en_partido_suelto_sigue_frenando(self):
        """1 de 2 lados de un equipo de liga principal no es "pocos"."""
        audit = self.auditar([_liga(1, home_ctx={})])
        self.assertFalse(audit["ok"])

    def test_un_hueco_de_primera_entre_treinta_lados_se_tolera(self):
        """Un hueco suelto en toda la quiniela (< 5% de los lados)."""
        partidos = [_liga(0, home_ctx={})] + [_liga(i) for i in range(1, 15)]
        audit = self.auditar(partidos)
        self.assertTrue(audit["ok"])

    def test_dos_huecos_de_primera_en_quince_partidos_frenan(self):
        """2/30 = 6,7%: ya no es un hueco suelto si no son modestos."""
        partidos = [_liga(0, home_ctx={}, away_ctx={})] + [_liga(i) for i in range(1, 15)]
        self.assertFalse(self.auditar(partidos)["ok"])


class EvidenciaInvalidaTests(BaseAuditoria):
    def test_la_evidencia_invalida_frena_aunque_el_hueco_sea_tolerable(self):
        partidos = [_copa_calatayud()] + [_liga(i) for i in range(14)]
        partidos[1] = _partido(
            "B.MUNICH",
            "Bodo Glimt",
            "soccer_uefa_champs_league",
            home_ctx={
                "previous_season": {"summary": "3o el ano pasado"},
                "all_evidence": [{"title": "El Bodo Glimt ficha a un delantero"}],
            },
        )
        audit = self.auditar(partidos)
        self.assertGreater(audit["invalid_evidence_count"], 0)
        self.assertFalse(audit["ok"])


if __name__ == "__main__":
    unittest.main()
