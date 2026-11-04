"""J19 (3-4 nov 2026): el worker de Windows rechazaba TODOS los ciclos porque
Athletic Bilbao (Osasuna-Athletic) y Sporting Gijon (Sporting-Valladolid) no
tenian contexto de plantilla: con la liga en marcha no hay "temporada anterior"
y sus rumores de verano caducan. Ahora se omite ese lado y se publica; solo la
evidencia invalida rechaza. Ademas, nombres con mojibake de Windows."""
import os
import unittest

os.environ.setdefault("OPENAI_API_KEY", "test")
import snapshot_worker as sw  # noqa: E402

CON = {"previous_season": {"summary": "3o el ano pasado"}}


def _p(local, visitante, home=None, away=None):
    return {"local": local, "visitante": visitante, "league": "soccer_spain_la_liga",
            "competition_context": {"season_transition": {"active": False,
                "home": CON if home is None else home, "away": CON if away is None else away}}}


class J19Tests(unittest.TestCase):
    def setUp(self):
        sw._reiniciar_fallos_sportsdb()

    def test_el_ciclo_del_3_de_noviembre_se_publica(self):
        partidos = [_p("CA Osasuna", "Athletic Bilbao", away={"all_evidence": []}),
                    _p("Sporting Gijón", "Real Valladolid CF", home={})] + [_p(f"L{i}", f"V{i}") for i in range(13)]
        audit = sw._audit_season_transition_snapshot({"quiniela_focus_matches": partidos})
        self.assertTrue(audit["ok"])
        self.assertEqual(audit["empty_side_count"], 2)
        self.assertTrue(all(i["season_underway"] for i in audit["tolerated_empty_sides"]))
        lado = partidos[0]["competition_context"]["season_transition"]["away"]
        self.assertTrue(lado["context_omitted"])
        self.assertIn("sin contexto", lado["summary"])

    def test_evidencia_invalida_sigue_rechazando(self):
        partido = _p("CA Osasuna", "Athletic Bilbao",
                     away={"all_evidence": [{"title": "El Osasuna ficha a un central"}]})
        audit = sw._audit_season_transition_snapshot({"quiniela_focus_matches": [partido]})
        self.assertFalse(audit["ok"])

    def test_nombres_con_tilde_en_codificaciones_de_windows(self):
        bueno = "Sporting Gijón"
        for raro in (bueno.encode("utf-8").decode("cp1252"), bueno.encode("utf-8").decode("latin-1"),
                     "Sporting Gij\ufffdn", "Sporting Gijo\u0301n"):
            self.assertEqual(sw._normalize_team_name(raro) in ("sporting gijon", "sporting gijn"), True, raro)
        self.assertEqual(sw._canonical_team_name(bueno.encode("utf-8").decode("cp1252")), "Sporting de Gijon")
        self.assertEqual(sw._canonical_team_name("Athletic Bilbao"), "Athletic Bilbao")

    def test_texto_sin_mojibake_no_cambia(self):
        for t in ("Atlético de Madrid", "Bodø/Glimt", "Fenerbahçe", "Real Sociedad"):
            self.assertEqual(sw._reparar_mojibake(t), t)


if __name__ == "__main__":
    unittest.main()
