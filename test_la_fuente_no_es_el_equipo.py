"""El nombre del periódico no cuenta como el del equipo.

En la jornada 9, el informe del Mallorca decía que el Mallorca tenía
entrenador nuevo: Javier Aguirre. El titular era "Javier Aguirre, nuevo
entrenador del Valencia CF - Diario de Mallorca". El equipo aparecía en la
cabecera del periódico, no en la noticia.

La fuente no se puede descontar siempre: cuando quien publica es el propio
club —"Acuerdo para el traspaso de Hugo San - Real Valladolid CF"— es la mejor
señal que hay. La regla distingue una cosa de otra: la fuente es el club si,
quitándole el nombre del equipo, solo quedan palabras de club.

Medido sobre la jornada entera: de 697 titulares se caen 3 (0,4%). Dos son
errores de verdad; el tercero, del Almería, era suyo y se pierde. Se acepta.
"""

import unittest

import snapshot_worker as w

AGUIRRE = "Javier Aguirre, nuevo entrenador del Valencia CF - Diario de Mallorca"
CLUB = "Acuerdo para el traspaso de Hugo San - Real Valladolid CF"


class LaFuenteEsElClubTests(unittest.TestCase):
    def test_la_web_del_club_lo_es(self):
        self.assertTrue(w._la_fuente_es_el_club("Real Valladolid CF", "Real Valladolid CF"))
        self.assertTrue(w._la_fuente_es_el_club("Cadiz Club de Futbol", "Cádiz CF"))
        self.assertTrue(w._la_fuente_es_el_club("Córdoba CF", "Córdoba"))

    def test_el_periodico_de_la_ciudad_no(self):
        self.assertFalse(w._la_fuente_es_el_club("Diario de Mallorca", "Mallorca"))
        self.assertFalse(w._la_fuente_es_el_club("Diario de Almería", "Almería"))
        self.assertFalse(w._la_fuente_es_el_club("Granada Hoy", "Granada CF"))

    def test_una_fuente_de_otro_sitio_tampoco(self):
        self.assertFalse(w._la_fuente_es_el_club("Diario AS", "Andorra CF"))


class ElTitularSeMideSinLaCabeceraTests(unittest.TestCase):
    def test_el_fichaje_de_otro_club_no_es_suyo(self):
        self.assertEqual(w._team_relevance_score(AGUIRRE, "Mallorca"), 0.0)

    def test_lo_que_publica_el_club_si(self):
        self.assertGreater(w._team_relevance_score(CLUB, "Real Valladolid CF"), 0)

    def test_si_el_equipo_esta_en_la_noticia_da_igual_el_periodico(self):
        self.assertGreater(
            w._team_relevance_score(
                "El Granada CF presenta a sus ocho fichajes - Granada Hoy", "Granada CF"
            ),
            0,
        )

    def test_un_resultado_con_guion_no_se_corta(self):
        """"Liverpool 2-1 Atlético" no lleva fuente: el guión va sin espacios."""
        titular = "Liverpool 2-1 Atletico de Madrid: la cronica"
        self.assertEqual(w._titulo_sin_el_periodico(titular, "Atletico Madrid"), titular)


class LaLimpiezaBorraLoQueLaAuditoriaRechazaTests(unittest.TestCase):
    """Si no coinciden, el snapshot se queda sin publicar por algo ya sabido."""

    def _partido(self):
        return {
            "local": "Mallorca",
            "visitante": "Almería",
            "local_lae": "MALLORCA",
            "visitante_lae": "ALMERIA",
            "competition_context": {
                "season_transition": {
                    "home": {
                        "previous_season": {"summary": "17º en LaLiga"},
                        "all_evidence": [
                            {"title": AGUIRRE},
                            {"title": "El Mallorca cierra el fichaje de un central - Diario AS"},
                        ],
                        "evidence_count": 2,
                    },
                    "away": {
                        "previous_season": {"summary": "3º en Segunda"},
                        "all_evidence": [
                            {"title": "El Almeria presenta a su nuevo delantero - Diario AS"}
                        ],
                        "evidence_count": 1,
                    },
                }
            },
        }

    def test_sin_limpiar_la_auditoria_frena(self):
        snapshot = {"quiniela_focus_matches": [self._partido()]}
        self.assertFalse(w._audit_season_transition_snapshot(snapshot)["ok"])

    def test_limpiando_sale(self):
        partido = self._partido()
        snapshot = {"quiniela_focus_matches": [partido]}
        self.assertEqual(w._limpiar_noticias_de_otra_categoria(partido), 1)
        informe = w._audit_season_transition_snapshot(snapshot)
        self.assertTrue(informe["ok"])
        self.assertEqual(informe["invalid_evidence_count"], 0)
        lado = partido["competition_context"]["season_transition"]["home"]
        self.assertEqual(lado["evidence_count"], 1)


if __name__ == "__main__":
    unittest.main()


class ElBriefingSeRehaceAlLimpiarTests(unittest.TestCase):
    """Limpiar el contexto no bastaba: el informe imprime una copia redactada.

    Con el contexto ya limpio y la auditoría en verde, el feed publicado seguía
    diciendo "local Mallorca: entrenador: Javier Aguirre, nuevo entrenador del
    Valencia CF". La frase salía de focus_ai_briefing, que se redacta una vez y
    se queda como está.
    """

    def _partido(self):
        coach = {"title": AGUIRRE, "source": "Diario de Mallorca", "fact_status": "reported"}
        lado = {
            "previous_season": {"summary": "17º en LaLiga"},
            "coach_changes": [coach],
            "all_evidence": [coach],
            "evidence_count": 1,
        }
        return {
            "local": "Mallorca",
            "visitante": "Almería",
            "local_lae": "MALLORCA",
            "visitante_lae": "ALMERIA",
            "competition_context": {
                "season_transition": {
                    "home": lado,
                    "away": {"previous_season": {"summary": "3º en Segunda"}, "all_evidence": []},
                }
            },
            "focus_ai_briefing": {
                "plantillas_y_transicion_de_temporada": {
                    "local": {"entrenador": [{"titular": AGUIRRE}]},
                    "visitante": {},
                }
            },
        }

    def test_la_frase_redactada_desaparece(self):
        partido = self._partido()
        w._limpiar_noticias_de_otra_categoria(partido)
        briefing = partido["focus_ai_briefing"]["plantillas_y_transicion_de_temporada"]
        self.assertEqual(briefing["local"]["entrenador"], [])

    def test_un_partido_sin_briefing_no_revienta(self):
        partido = self._partido()
        partido.pop("focus_ai_briefing")
        self.assertEqual(w._limpiar_noticias_de_otra_categoria(partido), 2)


class CadaBloqueConSuFiltroTests(unittest.TestCase):
    """La web del club casi nunca se nombra a sí misma."""

    def _partido(self, bloque, titulares):
        return {
            "local": "Real Sociedad B",
            "visitante": "Almería",
            "local_lae": "SANSE",
            "visitante_lae": "ALMERIA",
            "home_team_context": {bloque: {"items": [{"title": t} for t in titulares]}},
        }

    def test_la_web_oficial_conserva_lo_suyo(self):
        partido = self._partido("official_site", ["Promotion dream fulfilled"])
        self.assertEqual(w._limpiar_noticias_de_otra_categoria(partido), 0)
        self.assertEqual(len(partido["home_team_context"]["official_site"]["items"]), 1)

    def test_las_noticias_buscadas_pasan_el_filtro_con_el_que_se_eligieron(self):
        partido = self._partido("media_news", ["Promotion dream fulfilled"])
        self.assertEqual(w._limpiar_noticias_de_otra_categoria(partido), 1)
        self.assertEqual(partido["home_team_context"]["media_news"]["items"], [])
