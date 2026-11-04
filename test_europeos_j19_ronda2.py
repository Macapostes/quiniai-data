"""J19, segunda ronda de la auditoria (4 nov 2026)."""
import os
import unittest
from unittest import mock

os.environ.setdefault("OPENAI_API_KEY", "test")
import calidad_europea as ce  # noqa: E402
import snapshot_worker as w  # noqa: E402


def _tabla(n_martes=18, n_miercoles=18):
    t = {f"M{i}": {"team": f"M{i}", "played": 4, "position": i + 1, "points": 6} for i in range(n_martes)}
    t.update({f"X{i}": {"team": f"X{i}", "played": 3, "position": 20 + i, "points": 4} for i in range(n_miercoles)})
    return t


class JornadaDelPropioPartidoTests(unittest.TestCase):
    def test_miercoles_con_el_martes_ya_jugado_es_jornada_4(self):
        t = _tabla(20, 16)  # mediana 4 tras el martes
        b = ce.fase_liga_desde_tabla(t, t["X0"], t["X1"], "soccer_uefa_champs_league", ya_empezado=False)
        self.assertEqual(b["matchday"], 4)
        self.assertIn("Clasificacion tras 3 jornadas", b["guidance"])
        self.assertIn("Quedan 5 jornadas", b["guidance"])

    def test_martes_ya_jugado_sigue_siendo_jornada_4(self):
        t = _tabla(20, 16)
        b = ce.fase_liga_desde_tabla(t, t["M0"], t["M1"], "soccer_uefa_champs_league", ya_empezado=True)
        self.assertEqual(b["matchday"], 4)
        self.assertIn("Clasificacion tras 4 jornadas", b["guidance"])

    def test_conference_tiene_6_jornadas(self):
        t = {f"C{i}": {"team": f"C{i}", "played": 2, "position": i + 1, "points": 3} for i in range(36)}
        b = ce.fase_liga_desde_tabla(t, t["C0"], t["C1"], "soccer_uefa_europa_conference_league")
        self.assertEqual(b["matchday"], 3)
        self.assertEqual(b["total_matchdays"], 6)
        self.assertIn("Quedan 4 jornadas", b["guidance"])
        self.assertIn("6 jornadas", b["format"])
        self.assertEqual(ce.jornadas_fase_liga("soccer_uefa_champs_league"), 8)


class IdentidadPorPaisTests(unittest.TestCase):
    def test_lille_no_juega_la_eliteserien(self):
        self.assertTrue(ce.proximo_de_otro_pais("soccer_norway_eliteserien", "Norwegian Eliteserien", "France"))
        self.assertFalse(ce.proximo_de_otro_pais("soccer_france_ligue_one", "Ligue 1", "France"))
        self.assertFalse(ce.proximo_de_otro_pais("soccer_uefa_europa_league", "UEFA Europa League", "France"))
        self.assertFalse(ce.proximo_de_otro_pais("soccer_norway_eliteserien", "Eliteserien", ""))
        self.assertTrue(ce.proximo_de_otro_pais("soccer_epl", "English Premier League", "Spain"))

    def test_pais_del_lado(self):
        m = {"travel_context": {"home_country": "Norway", "away_country": "France"}}
        self.assertEqual(w._pais_del_lado(m, "away"), "FR")


class H2HEspnTests(unittest.TestCase):
    RESUMEN = {"seasonseries": [{"type": "head-to-head", "events": [
        {"id": "64104", "date": "2002-10-22T18:45:00Z", "statusType": {"completed": True}, "competitors": [
            {"homeAway": "home", "team": {"id": "86", "displayName": "Real Madrid"}, "score": "2"},
            {"homeAway": "away", "team": {"id": "887", "displayName": "AEK Athens"}, "score": "2"}]},
        {"id": "64200", "date": "2002-11-13T19:45:00Z", "statusType": {"completed": True}, "competitors": [
            {"homeAway": "home", "team": {"id": "887", "displayName": "AEK Athens"}, "score": "3"},
            {"homeAway": "away", "team": {"id": "86", "displayName": "Real Madrid"}, "score": "3"}]},
        {"id": "401915383", "date": "2026-11-04T20:00:00Z", "statusType": {"completed": False}, "competitors": []},
    ]}]}

    def test_aek_real_madrid_sale_de_la_serie_de_espn(self):
        filas = w._espn_filas_de_serie(self.RESUMEN, "401915383")
        self.assertEqual(len(filas), 2)
        m = {"local": "AEK ATENAS", "visitante": "REAL MADRID", "kickoff": "2026-11-04T20:00:00Z"}
        ev = {"local": "AEK Athens", "visitante": "Real Madrid", "espn_event_id": "401915383", "slug": "uefa.champions"}
        with mock.patch.object(w, "_espn_serie_h2h", return_value=filas):
            self.assertTrue(w._h2h_uefa_con_espn(m, ev))
        h2h = m["history_context"]["head_to_head"]
        self.assertEqual(h2h["meetings"], 2)
        self.assertEqual(h2h["draws"], 2)
        self.assertEqual(h2h["source"], "espn-seasonseries")

    def test_no_pisa_un_h2h_mas_largo(self):
        m = {"local": "A", "visitante": "B", "kickoff": "2026-11-04T20:00:00Z",
             "history_context": {"head_to_head": {"meetings": 7}}}
        ev = {"local": "AEK Athens", "visitante": "Real Madrid", "espn_event_id": "401915383"}
        with mock.patch.object(w, "_espn_serie_h2h", return_value=w._espn_filas_de_serie(self.RESUMEN)):
            self.assertEqual(w._h2h_uefa_con_espn(m, ev), [])
        self.assertEqual(m["history_context"]["head_to_head"]["meetings"], 7)


class NoticiasDeOtroClubTests(unittest.TestCase):
    ESPANYOL = ("Así llega el Espanyol a La Cerámica: nuevo entrenador, dos bajas y una crisis que urge "
                "frenar contra el Villarreal - El Periódico Mediterráneo")
    FLICK = "La reacción de Hansi Flick tras el 3-0 al Aston Villa - Fichajes.com"

    def test_el_sujeto_es_otro_club(self):
        self.assertEqual(w._otro_club_es_el_sujeto(self.ESPANYOL, "Villarreal"), "espanyol")
        self.assertEqual(w._otro_club_es_el_sujeto("El Villarreal recupera a Gerard Moreno ante el Espanyol", "Villarreal"), "")
        self.assertEqual(w._otro_club_es_el_sujeto("Bellingham, baja del Real Madrid ante el AEK", "Real Madrid"), "")

    def test_la_previa_de_los_dos_no_se_quita(self):
        self.assertEqual(w._otro_club_es_el_sujeto("Getafe - Villarreal: bajas y alineaciones", "Villarreal"), "")
        self.assertEqual(w._otro_club_es_el_sujeto("Previa Real Madrid vs Barcelona: Pedri, baja", "Barcelona"), "")

    def test_una_cronica_no_es_mercado_ni_la_web_fichajes(self):
        self.assertNotEqual(w._season_transition_category(self.FLICK, "Fichajes.com"), "signing")
        self.assertEqual(w._season_transition_category("El Real Betis encarrila el fichaje de un lateral", "Fichajes.com"), "signing")

    def test_se_limpia_lo_guardado(self):
        m = {"local": "VILLARREAL", "visitante": "PSG", "league": "soccer_uefa_champs_league",
             "competition_context": {"season_transition": {
                 "home": {"availability": [{"title": self.ESPANYOL, "category": "availability"}],
                          "all_evidence": [{"title": self.ESPANYOL}], "summary": "1 bajas/disponibilidad"},
                 "away": {"transfer_reports": [{"title": self.FLICK, "category": "signing"}],
                          "all_evidence": [{"title": self.FLICK}], "summary": "1 posibles altas u operaciones"}}}}
        m["visitante"] = "ASTON VILLA"
        self.assertEqual(w._quitar_noticias_de_otro_club(m), 2)
        tr = m["competition_context"]["season_transition"]
        self.assertEqual(tr["home"]["availability"], [])
        self.assertEqual(tr["away"]["transfer_reports"], [])
        self.assertNotIn("bajas", tr["home"]["summary"])


if __name__ == "__main__":
    unittest.main()
