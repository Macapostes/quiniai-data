"""La jornada actual no es la ultima anunciada (2-oct-2026).

Eduardo publico de golpe las proximas J12-J15. El worker seguia las 4 ultimas
publicadas (12-15): la J11, la del fin de semana, se quedo fuera; como ninguna
quedaba marcada como actual, la "actual" paso a ser la primera de la lista
(ordenada de mayor a menor): la J15, con el Oporto-PSV del 20-oct. Los partidos
de foco, el monitor ("jornada actual 15, ultima oficial 15") y el historico
pasaron a la J15, y la J11 dejo de enriquecerse: el backend se quedo sin
contexto para los partidos de Liga F de la J11 y emparejo el Deportivo F-Atletico F
con el Alaves-Atletico masculino de la J12.

Se ejecuta como los demas: python -m unittest test_jornada_actual
"""

import os
import unittest
from unittest import mock

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import snapshot_worker as w  # noqa: E402


def _slots(prefijo, fecha):
    return [
        {"position": i, "local": f"{prefijo} L{i}", "visitante": f"{prefijo} V{i}", "kickoff": fecha}
        for i in range(1, 15)
    ]


# Lo que habia el 2-oct a las 12:45: porcentajes con la J11 (activo=si), LAE con
# la J12 (activo=no) y la J10; las proximas de Eduardo, 12-15; la 16 no existe.
OFICIALES = {
    10: "2026-09-27T12:00:00Z",
    11: "2026-10-03T12:00:00Z",
    12: "2026-10-10T12:00:00Z",
}
PROXIMAS = [
    {"jornada": j, "date_label": d, "matches": _slots(f"P{j}", f), "pleno15": {}}
    for j, d, f in [
        (12, "10/10/2026", "2026-10-10T12:00:00Z"),
        (13, "13/10/2026", "2026-10-13T19:00:00Z"),
        (14, "17/10/2026", "2026-10-17T12:00:00Z"),
        (15, "20/10/2026", "2026-10-20T19:00:00Z"),
    ]
]


def _pagina(jornada, temporada=None):
    if jornada in OFICIALES:
        return {"ok": True, "source": "Eduardo Losilla LAE", "url": "", "jornada": jornada,
                "season": 2027, "matches": _slots(f"J{jornada}", OFICIALES[jornada]), "pleno15": {}}
    return {"ok": False, "jornada": jornada, "matches": [], "pleno15": {}}


class VentanaTests(unittest.TestCase):
    def test_una_semana_normal_no_cambia(self):
        # Lo de siempre: las 4 ultimas publicadas, con la actual dentro.
        self.assertEqual(w._ventana_de_jornadas(11, 13, 4), [10, 11, 12, 13])
        self.assertEqual(w._ventana_de_jornadas(11, 12, 4), [9, 10, 11, 12])
        self.assertEqual(w._ventana_de_jornadas(11, 14, 4), [11, 12, 13, 14])
        self.assertEqual(w._ventana_de_jornadas(11, 11, 4), [8, 9, 10, 11])

    def test_con_cuatro_anunciadas_la_actual_no_se_cae(self):
        self.assertEqual(w._ventana_de_jornadas(11, 15, 4), [11, 12, 13, 14])
        self.assertEqual(w._ventana_de_jornadas(11, 15, 3), [11, 12, 13])
        self.assertEqual(w._ventana_de_jornadas(11, 15, 2), [11, 12])

    def test_bordes(self):
        self.assertEqual(w._ventana_de_jornadas(1, 3, 4), [1, 2, 3])
        self.assertEqual(w._ventana_de_jornadas(11, None, 4), [8, 9, 10, 11])


class JornadaActualDeTests(unittest.TestCase):
    def test_la_marcada(self):
        js = [{"jornada": 15}, {"jornada": 14}, {"jornada": 11, "is_current": True}]
        self.assertEqual(w._jornada_actual_de(js), 11)

    def test_sin_marcada_la_mas_baja_y_no_la_primera(self):
        # El estado del 2-oct: 15, 14, 13, 12 sin ninguna marcada.
        js = [{"jornada": 15}, {"jornada": 14}, {"jornada": 13}, {"jornada": 12}]
        self.assertEqual(w._jornada_actual_de(js), 12)

    def test_el_historico_no_cuenta_si_hay_otra(self):
        js = [{"jornada": 12}, {"jornada": 10, "history_only": True}]
        self.assertEqual(w._jornada_actual_de(js), 12)
        self.assertEqual(w._jornada_actual_de([{"jornada": 10, "history_only": True}]), 10)

    def test_vacia(self):
        self.assertIsNone(w._jornada_actual_de([]))
        self.assertIsNone(w._jornada_actual_de(None))


class ConstruirJornadasTests(unittest.TestCase):
    def setUp(self):
        self.historia = {"season": 2027, "current_jornada": 11, "jornadas": {}}
        self.patches = [
            mock.patch.object(w, "QUINIELA_HISTORY", self.historia),
            mock.patch.object(w, "QUINIELA_HISTORY_JORNADAS", 4),
            mock.patch.object(w, "_eduardo_current_context",
                              return_value={"ok": True, "jornada": 11, "temporada": 2027}),
            mock.patch.object(w, "fetch_eduardo_upcoming_jornadas", return_value=PROXIMAS),
            mock.patch.object(w, "fetch_quiniela_jornada_page", side_effect=_pagina),
            mock.patch.object(w, "_find_cached_quiniela_match", return_value=None),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()

    def test_el_2_de_octubre(self):
        jornadas, _, _ = w.build_quiniela_jornadas([])
        self.assertEqual([j["jornada"] for j in jornadas], [14, 13, 12, 11])
        self.assertEqual([j["jornada"] for j in jornadas if j["is_current"]], [11])
        self.assertEqual(w._jornada_actual_de(jornadas), 11)
        self.assertEqual(self.historia["latest_announced_jornada"], 15)
        # La J11 y la J12 vienen de la LAE, la 13 y la 14 de las proximas.
        fuentes = {j["jornada"]: j["source"] for j in jornadas}
        self.assertEqual(fuentes[11], "Eduardo Losilla LAE")
        self.assertEqual(fuentes[12], "Eduardo Losilla LAE")
        self.assertEqual(fuentes[14], "Eduardo Losilla Proximas")
        w._persist_quiniela_history(jornadas)
        self.assertEqual(self.historia["current_jornada"], 11)
        self.assertEqual(sorted(self.historia["jornadas"]), ["11", "12", "13", "14"])
        publicas = w._select_monitor_jornadas(jornadas)
        self.assertEqual([j["jornada"] for j in publicas], [11, 12, 13])

    def test_una_semana_normal_sigue_igual(self):
        with mock.patch.object(w, "fetch_eduardo_upcoming_jornadas", return_value=PROXIMAS[:1]):
            jornadas, _, _ = w.build_quiniela_jornadas([])
        # Ultima anunciada 12 (+ la sonda de la 13, que no existe): 9-12, con
        # la 9 fuera porque no hay de donde sacarla.
        self.assertEqual([j["jornada"] for j in jornadas], [12, 11, 10])
        self.assertEqual(w._jornada_actual_de(jornadas), 11)


class SinPaginaDePorcentajesTests(unittest.TestCase):
    def _contexto(self, historia):
        with mock.patch.object(w, "QUINIELA_HISTORY", historia), \
                mock.patch.object(w, "_cache_get", return_value=None), \
                mock.patch.object(w, "_cache_set"), \
                mock.patch.object(w, "_fetch_cached_html", side_effect=RuntimeError("caida")), \
                mock.patch.object(w, "fetch_eduardo_upcoming_jornadas", return_value=PROXIMAS):
            return w._eduardo_current_context()

    def test_lo_guardado_si_es_coherente(self):
        self.assertEqual(self._contexto({"season": 2027, "current_jornada": 11})["jornada"], 11)

    def test_lo_guardado_no_va_por_delante_de_las_anunciadas(self):
        # El historico de Windows se quedo con la 15.
        ctx = self._contexto({"season": 2027, "current_jornada": 15})
        self.assertEqual(ctx["jornada"], 12)
        self.assertTrue(ctx["ok"])

    def test_sin_nada_guardado_la_primera_anunciada(self):
        ctx = self._contexto({"season": 2027})
        self.assertEqual(ctx["jornada"], 12)


if __name__ == "__main__":
    unittest.main()
