"""Un cruce femenino no puede recopilar los datos del primer equipo masculino.

La quiniela incorpora partidos de Liga F y ningun proveedor distingue por
categoria: buscar "R.MADRID (F)" en TheSportsDB devuelve el Real Madrid
masculino, con su liga, su clasificacion y su H2H detras. El unico dato fiable
por partido es el sufijo que pone la propia LAE en el nombre del equipo.

La regla es fallar cerrado: antes un partido sin contexto que un partido con el
contexto de otro equipo.
"""

import unittest

import snapshot_worker as worker


class CategoriaPorNombreTests(unittest.TestCase):
    def test_el_sufijo_del_boleto_identifica_la_categoria(self):
        self.assertEqual(worker._categoria_por_nombre("R.MADRID (F)"), "female")
        self.assertEqual(worker._categoria_por_nombre("EIBAR (F)"), "female")
        self.assertEqual(worker._categoria_por_nombre("BADALONA W."), "female")
        self.assertEqual(worker._categoria_por_nombre("ALAVES (M)"), "male")

    def test_sin_sufijo_no_se_inventa_categoria(self):
        self.assertIsNone(worker._categoria_por_nombre("R.MADRID"))
        self.assertIsNone(worker._categoria_por_nombre("BARCELONA"))
        self.assertIsNone(worker._categoria_por_nombre(""))

    def test_la_categoria_del_cruce_mira_los_dos_equipos(self):
        self.assertEqual(
            worker._categoria_del_partido({"local": "EIBAR", "visitante": "ESPANYOL (F)"}),
            "female",
        )


class FichaDeProveedorTests(unittest.TestCase):
    def test_la_ficha_masculina_no_pasa_por_femenina(self):
        self.assertFalse(
            worker._ficha_es_femenina({"strTeam": "Real Madrid", "strLeague": "Spanish La Liga"})
        )

    def test_la_ficha_femenina_si_pasa(self):
        self.assertTrue(
            worker._ficha_es_femenina({"strTeam": "Real Madrid Femenino", "strLeague": "Spanish Liga F"})
        )
        self.assertTrue(
            worker._ficha_es_femenina({"strTeam": "Atletico Madrid Women", "strLeague": "Liga F"})
        )


class HistoricoTests(unittest.TestCase):
    FILAS = [
        {"HomeTeam": "Real Madrid", "AwayTeam": "Barcelona"},
        {"HomeTeam": "Atletico Madrid", "AwayTeam": "Sevilla"},
    ]

    def test_un_nombre_femenino_no_resuelve_contra_el_historico_masculino(self):
        # Sin este filtro, el parecido de 0.33 bastaba para que "R.MADRID (F)"
        # se llevase la tabla, la racha y el H2H del Real Madrid masculino.
        self.assertEqual(
            worker._resolve_csv_team_name("R.MADRID (F)", self.FILAS), "R.MADRID (F)"
        )
        self.assertEqual(
            worker._resolve_csv_team_name("AT.MADRID (F)", self.FILAS), "AT.MADRID (F)"
        )

    def test_un_nombre_masculino_sigue_resolviendo_igual(self):
        self.assertEqual(
            worker._resolve_csv_team_name("R.MADRID", self.FILAS), "Real Madrid"
        )


class LigaTests(unittest.TestCase):
    def test_una_liga_masculina_se_descarta_en_un_cruce_femenino(self):
        match = {"local": "R.MADRID (F)", "visitante": "AT.MADRID (F)"}
        worker._apply_dynamic_league_metadata(
            match, {"strLeague": "Spanish La Liga", "idLeague": "4335"}
        )
        self.assertEqual(match["league"], "league_unresolved")
        self.assertEqual(match["league_descartada"], "soccer_spain_la_liga")

    def test_la_liga_f_si_se_conserva(self):
        # El candado no es "los femeninos se quedan sin datos": cuando el
        # proveedor resuelve la competicion correcta, los datos pasan.
        match = {"local": "EIBAR (F)", "visitante": "ESPANYOL (F)"}
        worker._apply_dynamic_league_metadata(
            match, {"strLeague": "Spanish Liga F", "idLeague": "5214"}
        )
        self.assertNotEqual(match["league"], "league_unresolved")
        self.assertNotIn("league_descartada", match)

    def test_un_cruce_masculino_no_se_toca(self):
        match = {"local": "CELTA", "visitante": "ATH.CLUB"}
        worker._apply_dynamic_league_metadata(
            match, {"strLeague": "Spanish La Liga", "idLeague": "4335"}
        )
        self.assertEqual(match["league"], "soccer_spain_la_liga")


class NoticiasTests(unittest.TestCase):
    MASCULINA = {"title": "Lesion de Vinicius en el Real Madrid", "link": "https://as.com/rm"}
    FEMENINA = {"title": "Baja en el Real Madrid Femenino", "link": "https://as.com/rmfem"}
    CANTERA = {"title": "El juvenil U19 se lesiona", "link": "https://as.com/u19"}

    def test_en_un_cruce_femenino_el_ruido_es_la_noticia_masculina(self):
        self.assertTrue(worker._is_non_first_team_news(self.MASCULINA, "female"))
        self.assertFalse(worker._is_non_first_team_news(self.FEMENINA, "female"))

    def test_en_un_cruce_masculino_se_mantiene_el_criterio_de_siempre(self):
        self.assertFalse(worker._is_non_first_team_news(self.MASCULINA))
        self.assertTrue(worker._is_non_first_team_news(self.FEMENINA))

    def test_la_cantera_se_descarta_en_las_dos_categorias(self):
        self.assertTrue(worker._is_non_first_team_news(self.CANTERA))
        self.assertTrue(worker._is_non_first_team_news(self.CANTERA, "female"))


if __name__ == "__main__":
    unittest.main()


class ResolucionDeEquipoFemeninoTests(unittest.TestCase):
    """Encontrar al equipo femenino, no solo rechazar al masculino.

    Antes se resolvia 1 de 8: el sufijo "(F)" rompia la busqueda en el
    diccionario de alias, y el filtro de categoria del proveedor solo entendia
    " women"/" ladies"/" dam ", que el nombre de la quiniela nunca lleva.
    """

    def test_el_sufijo_no_rompe_el_diccionario_de_alias(self):
        # "R.MADRID" esta en la tabla; "R.MADRID (F)" normalizaba a
        # "r madrid f", no encontraba nada y devolvia el nombre crudo, asi que
        # se consultaba "R.MADRID Femenino", que no existe en ningun proveedor.
        self.assertEqual(worker._canonical_team_name("R.MADRID (F)"), "Real Madrid")
        self.assertEqual(worker._canonical_team_name("AT.MADRID (F)"), "Atlético Madrid")
        self.assertEqual(worker._canonical_team_name("ATH.CLUB (F)"), "Athletic Bilbao")
        # Y el masculino sigue igual.
        self.assertEqual(worker._canonical_team_name("R.MADRID"), "Real Madrid")

    def test_la_marca_de_categoria_se_quita_para_comparar(self):
        # Sin esto, "Barcelona Femeni" no se parecia lo bastante a "Barcelona".
        self.assertEqual(worker._sin_marca_femenina("Real Madrid Femenino"), "Real Madrid")
        self.assertEqual(worker._sin_marca_femenina("Sevilla Women"), "Sevilla")
        self.assertEqual(worker._sin_marca_femenina("Barcelona Femení"), "Barcelona")
        self.assertEqual(worker._sin_marca_femenina("Espanyol Femení"), "Espanyol")

    def test_se_reconocen_las_tres_formas_del_sufijo(self):
        # El proveedor usa las tres y hacen falta todas: Femenino (Real Madrid),
        # Femeni en catalan (Barcelona, Espanyol) y Women (Sevilla, Eibar).
        for nombre in ("Real Madrid Femenino", "Barcelona Femení", "Sevilla Women"):
            self.assertTrue(worker._parece_femenino(nombre), nombre)

    def test_un_fallo_de_red_no_se_cachea_como_equipo_inexistente(self):
        # Un 429 dejaba al equipo ilocalizable una semana entera.
        import inspect

        fuente = inspect.getsource(worker.fetch_the_sportsdb_team)
        self.assertIn("alguna_respuesta", fuente)
        self.assertLess(fuente.index("if not alguna_respuesta"), fuente.index("_cache_set"))

    def test_las_consultas_paran_en_cuanto_hay_candidatos(self):
        import inspect

        fuente = inspect.getsource(worker.fetch_the_sportsdb_team)
        self.assertIn("if teams:", fuente)
        self.assertIn("break", fuente)


class HistoricoFemeninoTests(unittest.TestCase):
    """El historico de la Liga F tiene que llegar, y ser del equipo correcto."""

    FILAS = [
        {"HomeTeam": "Real Madrid Femenino", "AwayTeam": "Eibar Women"},
        {"HomeTeam": "Real Sociedad Femenino", "AwayTeam": "Granada Femenino"},
        {"HomeTeam": "Madrid CFF", "AwayTeam": "Barcelona Femení"},
    ]

    def test_se_prueban_las_dos_formas_de_nombrar_la_temporada(self):
        # El worker nacio con ligas nordicas, de ano natural, y solo probaba
        # "2026". Las europeas usan "2026-2027" y por eso la Liga F devolvia
        # cero eventos siempre.
        self.assertEqual(worker._etiquetas_de_temporada("2026"), ["2026-2027", "2026"])

    def test_con_nombre_exacto_no_se_acepta_un_club_parecido(self):
        # "Real Madrid" y "Real Sociedad" comparten palabra: el comparador por
        # solapamiento los daba por el mismo equipo y el informe recibia la
        # clasificacion de otro club.
        self.assertEqual(
            worker._resolve_csv_team_name(
                "Atlético Madrid Femenino", self.FILAS,
                filas_de_su_categoria=True, exacto=True,
            ),
            "Atlético Madrid Femenino",  # no esta: se devuelve tal cual, sin historico
        )
        self.assertEqual(
            worker._resolve_csv_team_name(
                "Real Madrid Femenino", self.FILAS,
                filas_de_su_categoria=True, exacto=True,
            ),
            "Real Madrid Femenino",
        )

    def test_en_una_tabla_de_su_categoria_no_se_filtra_por_nombre(self):
        # En la Liga F juegan el Madrid CFF o el Logrono United, que no llevan
        # marca femenina en el nombre: filtrarlos los dejaria fuera de su tabla.
        resuelto = worker._resolve_csv_team_name(
            "Madrid CFF", self.FILAS, filas_de_su_categoria=True, exacto=True
        )
        self.assertEqual(resuelto, "Madrid CFF")

    def test_un_historico_vacio_no_se_cachea(self):
        import inspect

        fuente = inspect.getsource(worker._fetch_sportsdb_league_history)
        self.assertIn("if rows:", fuente)
        self.assertIn("if not etiqueta_buena", fuente)
