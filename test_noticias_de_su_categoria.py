"""A un partido femenino no le entran noticias del masculino.

En la jornada 9, al VALENCIA (F) - TENERIFE (F) le llegaron dieciséis titulares
sobre Javier Aguirre como nuevo entrenador del Valencia. Aguirre entrena al
masculino: el informe de pago decía que el Valencia femenino había fichado
entrenador. Las noticias se buscan por nombre de equipo y al partido femenino
le llegaba "VALENCIA" a secas, sin marca de categoría, así que la búsqueda
traía al primer equipo y nada lo filtraba después.

Son dos arreglos: el nombre que se usa para buscar ya lleva la marca del
boleto, y un titular tiene que ser de la misma categoría que el equipo. Y como
las consultas normales llevan pegadas palabras clave que en Liga F no encuentra
casi nada, los equipos femeninos tienen su propia búsqueda, más ancha.
"""

import inspect
import unittest

import snapshot_worker as w

AGUIRRE = "Javier Aguirre es nuevo entrenador del Valencia - ESPN"
FEMENINA = "El Valencia CF inicia la pretemporada del regreso a la Liga F - Superdeporte"


class UnTitularEsDeSuCategoriaTests(unittest.TestCase):
    def test_el_fichaje_del_masculino_no_es_del_femenino(self):
        self.assertTrue(w._titular_de_otra_categoria(AGUIRRE, "VALENCIA (F)"))

    def test_y_al_masculino_no_le_entra_lo_femenino(self):
        self.assertTrue(w._titular_de_otra_categoria(FEMENINA, "VALENCIA"))

    def test_lo_que_corresponde_si_pasa(self):
        self.assertFalse(w._titular_de_otra_categoria(AGUIRRE, "VALENCIA"))
        self.assertFalse(w._titular_de_otra_categoria(FEMENINA, "VALENCIA (F)"))

    def test_no_puntua_relevancia(self):
        self.assertEqual(w._team_relevance_score(AGUIRRE, "VALENCIA (F)"), 0.0)
        self.assertGreater(w._team_relevance_score(AGUIRRE, "VALENCIA"), 0.0)

    def test_no_pasa_la_criba_de_calidad(self):
        item = {"title": AGUIRRE, "source": "ESPN", "link": "https://espn.com/x"}
        self.assertFalse(w._passes_team_news_quality(item, "VALENCIA (F)"))


class LaBusquedaFemeninaBuscaEnFemeninoTests(unittest.TestCase):
    def test_la_consulta_lleva_la_categoria(self):
        consulta = w._team_query_terms("VALENCIA (F)")
        self.assertIn("femenino", consulta)
        self.assertIn("Liga F", consulta)

    def test_y_no_va_en_mayusculas(self):
        """El boleto escribe en mayúsculas y los buscadores responden peor."""
        self.assertIn('"Valencia femenino"', w._team_query_terms("VALENCIA (F)"))

    def test_el_masculino_busca_como_siempre(self):
        consulta = w._team_query_terms("VALENCIA")
        self.assertNotIn("femenino", consulta)


class ElPartidoBuscaConElNombreDelBoletoTests(unittest.TestCase):
    def test_se_usa_el_nombre_con_marca(self):
        fuente = inspect.getsource(w._enrich_quiniela_match)
        self.assertIn('_nombre_para_el_proveedor(match, "local")', fuente)
        self.assertNotIn('fetch_focus_team_news(match["local"])', fuente)

    def test_un_equipo_femenino_usa_su_propia_busqueda(self):
        fuente = inspect.getsource(w._enrich_quiniela_match)
        self.assertIn("fetch_focus_team_news_femenino", fuente)
        self.assertIn("fetch_season_transition_news_femenino", fuente)

    def test_la_busqueda_femenina_no_arrastra_palabras_clave(self):
        """Medido: la frase sola da 20 titulares; con las palabras clave, 1."""
        fuente = inspect.getsource(w._noticias_femeninas)
        # Solo el código: el comentario de la función cita esas palabras como
        # ejemplo de lo que se quitó.
        codigo = fuente.split('"""')[-1]
        self.assertIn("femenino", codigo)
        for palabra in ("lesion OR", "convocatoria", "rueda de prensa"):
            self.assertNotIn(palabra, codigo)


if __name__ == "__main__":
    unittest.main()
