"""El lunes del boleto cae DESPUES del sabado de la jornada, no la semana anterior.

J12 2026-27: Eduardo anuncio la jornada con fecha 10/10/2026 (sabado). Burgos-
Granada y Oviedo-Eibar iban como LUN 16:15 / 18:30. El calculo del dia los
mando al 05/10 (lunes anterior). El backend / LAE del app tenian el 12/10. Sin
cuotas en esos dos cruces, la hora mala se quedo para siempre y contamino
kickoff_from de toda la J12.
"""
from __future__ import annotations

import unittest
from datetime import datetime

import snapshot_worker as w


HTML = """
<div class="c-ayudas-proximas__tabla-partidos__titulo">JORNADA 12 - 10/10/2026</div>
<p title="BURGOS - GRANADA">
  <span class="c-equipos__number">13</span>
  <div class="c-marcador-horario__time__day">LUN</div>
  <div class="c-marcador-horario__time__hour">16:15</div>
</p>
<p title="R.OVIEDO - EIBAR">
  <span class="c-equipos__number">14</span>
  <div class="c-marcador-horario__time__day">LUN</div>
  <div class="c-marcador-horario__time__hour">18:30</div>
</p>
<p title="RAYO - ATH.CLUB">
  <span class="c-equipos__number">1</span>
  <div class="c-marcador-horario__time__day">SAB</div>
  <div class="c-marcador-horario__time__hour">14:00</div>
</p>
<p title="ELCHE - CELTA">
  <span class="c-equipos__number">4</span>
  <div class="c-marcador-horario__time__day">DOM</div>
  <div class="c-marcador-horario__time__hour">14:00</div>
</p>
"""


class LunesDespuesDelSabadoTests(unittest.TestCase):
    def test_burgos_y_oviedo_caen_en_el_12(self):
        jornadas = w._parse_eduardo_upcoming_jornadas(HTML)
        self.assertEqual(len(jornadas), 1)
        by_pos = {m["position"]: m for m in jornadas[0]["matches"]}
        # 16:15 Madrid = 14:15 UTC (CEST); 18:30 Madrid = 16:30 UTC
        self.assertEqual(by_pos[13]["kickoff"], "2026-10-12T14:15:00Z")
        self.assertEqual(by_pos[14]["kickoff"], "2026-10-12T16:30:00Z")
        self.assertEqual(by_pos[1]["kickoff"], "2026-10-10T12:00:00Z")
        self.assertEqual(by_pos[4]["kickoff"], "2026-10-11T12:00:00Z")

    def test_viernes_previo_sigue_antes_del_sabado(self):
        html = HTML.replace(
            'title="RAYO - ATH.CLUB"',
            'title="RAYO - ATH.CLUB"',
            1,
        ).replace(
            '<div class="c-marcador-horario__time__day">SAB</div>\n  <div class="c-marcador-horario__time__hour">14:00</div>',
            '<div class="c-marcador-horario__time__day">VIE</div>\n  <div class="c-marcador-horario__time__hour">21:00</div>',
            1,
        )
        jornadas = w._parse_eduardo_upcoming_jornadas(html)
        by_pos = {m["position"]: m for m in jornadas[0]["matches"]}
        self.assertEqual(by_pos[1]["kickoff"], "2026-10-09T19:00:00Z")


if __name__ == "__main__":
    unittest.main()
