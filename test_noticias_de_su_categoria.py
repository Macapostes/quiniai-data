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



class LaMarcaFemeninaSeBuscaPorPalabrasTests(unittest.TestCase):
    """Buscarla por trozos de texto tumbó un snapshot entero.

    "liga f" aparece dentro de "liga francesa" y de "LaLiga Fantasy", así que
    titulares masculinos normales contaban como femeninos, la auditoría de
    contexto los daba por evidencia inválida y rechazaba la publicación.
    """

    def test_liga_francesa_no_es_liga_f(self):
        self.assertFalse(w._titular_femenino("El Andorra jugara en la liga francesa"))
        self.assertFalse(w._titular_femenino("LaLiga Fantasy: los fichajes del Andorra"))

    def test_liga_f_de_verdad_si(self):
        self.assertTrue(w._titular_femenino("La Liga F arranca este fin de semana"))

    def test_las_formas_que_usa_la_prensa(self):
        for titular in (
            "El Barca Femeni golea",
            "Athletic Club Women gana en San Mames",
            "El Atletico femenino ficha a una delantera",
            "Frauen-Bundesliga: resultados",
        ):
            self.assertTrue(w._titular_femenino(titular), titular)


class LaContaminacionVaEnLosDosSentidosTests(unittest.TestCase):
    """Los nueve titulares que rechazó la auditoría del 23/09, todos reales.

    Eran noticias femeninas guardadas en el contexto de equipos MASCULINOS: el
    espejo del caso de Aguirre. El filtro las caza, y por eso hay que tirar la
    caché de noticias anterior: se eligieron cuando no existía.
    """

    CASOS = [
        ("ANDORRA", "El Barca Femeni realizara un stage de pretemporada en Andorra la Vella"),
        ("TENERIFE", "Asi afronta el Costa Adeje Tenerife su nueva temporada en la Liga F"),
        ("AT.MADRID", "Gabi Nunes regresa a Liga F para reforzar el ataque del Atletico de Madrid"),
        ("VALENCIA", "El Valencia CF inicia la pretemporada del regreso a la Liga F"),
    ]

    def test_una_noticia_femenina_no_es_del_equipo_masculino(self):
        for equipo, titular in self.CASOS:
            self.assertTrue(w._titular_de_otra_categoria(titular, equipo), titular)

    def test_y_esa_misma_noticia_si_es_del_femenino(self):
        for equipo, titular in self.CASOS:
            self.assertFalse(w._titular_de_otra_categoria(titular, f"{equipo} (F)"), titular)

    def test_la_cache_de_noticias_se_abandona(self):
        """Lo guardado antes se eligió sin filtro y la auditoría lo rechaza."""
        import inspect

        for funcion, version in (
            (w.fetch_team_news, "v12:team:"),
            (w.fetch_focus_team_news, "v13:focus:"),
            (w.fetch_season_transition_news, "v6:season-transition:"),
            (w.fetch_local_media_news, "v13:media:"),
        ):
            self.assertIn(version, inspect.getsource(funcion))

if __name__ == "__main__":
    unittest.main()


class LaCacheSeVuelveAComprobarTests(unittest.TestCase):
    """Lo guardado se eligió con el filtro de aquel día, no con el de hoy.

    Esto tumbó el snapshot dos ciclos seguidos. La noche del 23 la marca de
    categoría estaba rota y la búsqueda guardó noticias femeninas para equipos
    masculinos. Arreglada la expresión, la caché seguía devolviendo aquello: la
    auditoría lo rechazaba y nadie volvía a buscar nunca.
    """

    GUARDADO_SUCIO = {
        "items": [
            {"title": "El Andorra anuncia el fichaje de Nacho Quintana - AS"},
            {"title": "El Barca Femeni realizara un stage de pretemporada en Andorra la Vella"},
        ]
    }
    GUARDADO_LIMPIO = {"items": [{"title": "El Andorra anuncia el fichaje de Nacho Quintana - AS"}]}

    def test_lo_contaminado_no_se_reutiliza(self):
        self.assertFalse(w._lo_guardado_sigue_pasando_el_filtro(self.GUARDADO_SUCIO, "Andorra CF"))

    def test_lo_bueno_se_sigue_reutilizando(self):
        self.assertTrue(w._lo_guardado_sigue_pasando_el_filtro(self.GUARDADO_LIMPIO, "Andorra CF"))
        self.assertTrue(w._lo_guardado_sigue_pasando_el_filtro({"items": []}, "Andorra CF"))

    def test_y_al_femenino_le_vale_lo_femenino(self):
        self.assertTrue(
            w._lo_guardado_sigue_pasando_el_filtro(
                {"items": [{"title": "Gabi Nunes regresa a Liga F para reforzar al Atletico"}]},
                "AT.MADRID (F)",
            )
        )

    def test_todos_los_buscadores_lo_comprueban(self):
        for funcion in (
            w.fetch_team_news,
            w.fetch_focus_team_news,
            w.fetch_focus_team_news_femenino,
            w.fetch_season_transition_news,
            w.fetch_season_transition_news_femenino,
            w.fetch_local_media_news,
        ):
            fuente = inspect.getsource(funcion)
            self.assertIn(
                "if cached and _lo_guardado_sigue_pasando_el_filtro(cached, team_name):",
                fuente,
                funcion.__name__,
            )
