"""La auditoría de plantillas juzga cada evidencia con el nombre con que se buscó.

Este fue el freno de dos ciclos seguidos. Un cruce femenino trae noticias
femeninas —correctas—, pero la auditoría las comparaba con el nombre sin marca
("AT.MADRID"), las daba por invalidas y rechazaba el snapshot entero. A la vez,
el contexto guardado de un ciclo anterior no se volvía a mirar nunca, así que
las evidencias contaminadas de antes del filtro se quedaban dentro para
siempre. Dos arreglos, y el snapshot vuelve a publicarse.
"""

import inspect
import unittest

import snapshot_worker as w

# Titulares reales de la jornada 9, cada uno del equipo que le toca.
GRANADA = "El Granada CF presenta a sus ocho fichajes cargados de ilusion - VAVEL"
ANDORRA = "El Andorra anuncia el fichaje de Nacho Quintana - Diario AS"
ATHLETIC_FEM = "El Athletic Club femenino de Bilbao renueva a su capitana - El Correo"
ATLETICO_FEM = "Gabi Nunes regresa a Liga F para reforzar el ataque del Atletico de Madrid"
# Esta es la que colgaba del Andorra masculino y frenaba la publicacion.
CONTAMINADA = "El Barca Femeni realizara un stage de pretemporada en Andorra la Vella"


def _partido(local, visitante, local_lae, visitante_lae, evidencia_local, evidencia_visitante):
    def lado(titulares):
        return {
            "previous_season": {"summary": "8º la temporada pasada"},
            "all_evidence": [{"title": t} for t in titulares],
        }

    return {
        "local": local,
        "visitante": visitante,
        "local_lae": local_lae,
        "visitante_lae": visitante_lae,
        "competition_context": {
            "season_transition": {
                "home": lado(evidencia_local),
                "away": lado(evidencia_visitante),
            }
        },
    }


class LaAuditoriaUsaElNombreDelBoletoTests(unittest.TestCase):
    def test_una_noticia_femenina_vale_en_un_cruce_femenino(self):
        snapshot = {
            "quiniela_focus_matches": [
                _partido(
                    "Athletic Club", "Atletico Madrid",
                    "ATH.CLUB (F)", "AT.MADRID (F)",
                    [ATHLETIC_FEM], [ATLETICO_FEM],
                )
            ]
        }
        informe = w._audit_season_transition_snapshot(snapshot)
        self.assertEqual(informe["invalid_evidence_count"], 0)
        self.assertTrue(informe["ok"])

    def test_esa_misma_noticia_en_el_masculino_frena_la_publicacion(self):
        snapshot = {
            "quiniela_focus_matches": [
                _partido(
                    "Granada CF", "Andorra CF", "GRANADA", "ANDORRA FC",
                    [GRANADA], [ANDORRA, CONTAMINADA],
                )
            ]
        }
        informe = w._audit_season_transition_snapshot(snapshot)
        self.assertEqual(informe["invalid_evidence_count"], 1)
        self.assertFalse(informe["ok"])


class LoGuardadoSeVuelveAMirarTests(unittest.TestCase):
    """Sin esto el snapshot se rechazaba solo, ciclo tras ciclo, sin rehacerse."""

    def test_la_transicion_contaminada_se_rehace(self):
        partido = _partido(
            "Granada CF", "Andorra CF", "GRANADA", "ANDORRA FC",
            [GRANADA], [ANDORRA, CONTAMINADA],
        )
        transicion = partido["competition_context"]["season_transition"]
        self.assertFalse(w._transicion_guardada_sigue_valiendo(partido, transicion))

    def test_la_buena_se_reutiliza(self):
        partido = _partido(
            "Granada CF", "Andorra CF", "GRANADA", "ANDORRA FC",
            [GRANADA], [ANDORRA],
        )
        transicion = partido["competition_context"]["season_transition"]
        self.assertTrue(w._transicion_guardada_sigue_valiendo(partido, transicion))

    def test_y_la_femenina_correcta_tambien(self):
        partido = _partido(
            "Athletic Club", "Atletico Madrid", "ATH.CLUB (F)", "AT.MADRID (F)",
            [ATHLETIC_FEM], [ATLETICO_FEM],
        )
        transicion = partido["competition_context"]["season_transition"]
        self.assertTrue(w._transicion_guardada_sigue_valiendo(partido, transicion))


class ElContextoSeBuscaEnSuCategoriaTests(unittest.TestCase):
    def test_un_equipo_femenino_usa_la_busqueda_femenina(self):
        llamadas = []
        original_fem = w.fetch_season_transition_news_femenino
        original = w.fetch_season_transition_news
        w.fetch_season_transition_news_femenino = lambda n: llamadas.append(("fem", n)) or {}
        w.fetch_season_transition_news = lambda n: llamadas.append(("masc", n)) or {}
        try:
            w._transicion_por_categoria("AT.MADRID (F)")
            w._transicion_por_categoria("Andorra CF")
        finally:
            w.fetch_season_transition_news_femenino = original_fem
            w.fetch_season_transition_news = original
        self.assertEqual(llamadas, [("fem", "AT.MADRID (F)"), ("masc", "Andorra CF")])

    def test_se_rehace_con_el_nombre_marcado(self):
        fuente = inspect.getsource(w._ensure_season_transition_context)
        self.assertIn('_nombre_para_el_proveedor(match, "local")', fuente)
        self.assertIn("_transicion_por_categoria(", fuente)


if __name__ == "__main__":
    unittest.main()
