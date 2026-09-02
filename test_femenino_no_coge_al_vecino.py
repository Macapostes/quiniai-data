# -*- coding: utf-8 -*-
"""Parecerse no basta para colgarle a un equipo la clasificacion de otro.

En femenino el nombre del boleto ("SEVILLA (F)") no coincide con el del
proveedor ("Sevilla Women"), asi que hay que comparar a ojo. Con solo medir
parecido, cuando el club correcto falta del historico -recien ascendido,
arranque de temporada- el comparador se quedaba con el vecino: "BARCELONA (F)"
acababa con los datos del Badalona y "R.MADRID (F)" con los del Atletico. Ese
dato falso llegaba al informe de la IA como si fuera suyo.
"""

import unittest

import snapshot_worker as w

# Los 16 clubes de Liga F, como los nombra TheSportsDB.
CLUBES = [
    "Sevilla Women", "Barcelona Femení", "Logroño United", "Athletic Club Women",
    "Atlético Madrid Femenino", "Real Madrid Femenino", "Eibar Women", "Badalona Women",
    "Espanyol Femení", "Alavés Gloriosas", "Valencia Femenino", "Real Sociedad Femenino",
    "Granada Femenino", "Madrid CFF", "Deportivo de La Coruña Women", "Levante Femenino",
]

# Como los nombra la LAE en el boleto.
EQUIVALENCIAS = {
    "SEVILLA (F)": "Sevilla Women",
    "BARCELONA (F)": "Barcelona Femení",
    "LOGROÑO (F)": "Logroño United",
    "ATH.CLUB (F)": "Athletic Club Women",
    "AT.MADRID (F)": "Atlético Madrid Femenino",
    "R.MADRID (F)": "Real Madrid Femenino",
    "EIBAR (F)": "Eibar Women",
    "BADALONA (F)": "Badalona Women",
    "ESPANYOL (F)": "Espanyol Femení",
    "ALAVÉS (F)": "Alavés Gloriosas",
    "VALENCIA (F)": "Valencia Femenino",
    "R.SOCIEDAD (F)": "Real Sociedad Femenino",
    "GRANADA (F)": "Granada Femenino",
    "MADRID CFF (F)": "Madrid CFF",
    "DEPORTIVO (F)": "Deportivo de La Coruña Women",
    "LEVANTE (F)": "Levante Femenino",
}


def _filas(clubes):
    return [
        {"HomeTeam": c, "AwayTeam": clubes[(i + 1) % len(clubes)]}
        for i, c in enumerate(clubes)
    ]


def _resolver(nombre, clubes):
    return w._resolve_csv_team_name(
        nombre, _filas(clubes), filas_de_su_categoria=True,
        umbral=0.6, exigir_mismo_club=True,
    )


class EncuentraASuClubTests(unittest.TestCase):
    """Lo que hay que conservar: el boleto no usa los nombres del proveedor."""

    def test_los_dieciseis_clubes_de_liga_f(self):
        for lae, proveedor in EQUIVALENCIAS.items():
            with self.subTest(lae):
                self.assertEqual(_resolver(lae, CLUBES), proveedor)


class NoCuelgaLosDatosDelVecinoTests(unittest.TestCase):
    """Lo que hay que impedir: quedarse sin datos es correcto; inventarlos no."""

    def test_si_falta_su_club_se_queda_sin_historico(self):
        for lae, proveedor in EQUIVALENCIAS.items():
            with self.subTest(lae):
                sin_el_suyo = [c for c in CLUBES if c != proveedor]
                self.assertEqual(
                    _resolver(lae, sin_el_suyo), lae,
                    f"{lae} deberia quedarse sin datos, no coger otro club",
                )

    def test_los_cinco_que_se_confundian(self):
        """Los que de verdad fallaban, uno a uno, por si el resto tapa alguno."""
        peligrosos = [
            ("BARCELONA (F)", "Badalona Women"),
            ("BADALONA (F)", "Barcelona Femení"),
            ("R.MADRID (F)", "Atlético Madrid Femenino"),
            ("AT.MADRID (F)", "Real Madrid Femenino"),
            ("ATH.CLUB (F)", "Atlético Madrid Femenino"),
        ]
        for lae, vecino in peligrosos:
            with self.subTest(f"{lae} != {vecino}"):
                self.assertFalse(w._es_el_mismo_club(lae, vecino))
                # Y con el vecino como unica opcion, no lo coge.
                self.assertEqual(_resolver(lae, [vecino]), lae)


class LaReglaDeIdentidadTests(unittest.TestCase):
    def test_acepta_las_formas_en_que_la_lae_abrevia(self):
        for lae, proveedor in EQUIVALENCIAS.items():
            with self.subTest(lae):
                self.assertTrue(w._es_el_mismo_club(lae, proveedor))

    def test_una_sigla_corta_casa_con_las_iniciales(self):
        # "LP" de "Las Planas" no es prefijo de ninguna palabra.
        self.assertTrue(w._es_el_mismo_club("LEVANTE LP (F)", "Levante Las Planas"))

    def test_dos_levantes_distintos_no_son_el_mismo_club(self):
        # Levante Las Planas y Levante UD Femenino son clubes diferentes.
        self.assertFalse(w._es_el_mismo_club("LEVANTE LP (F)", "Levante Femenino"))

    def test_el_camino_masculino_no_lo_usa(self):
        """Ahi los nombres vienen abreviados por football-data ("Sociedad" por
        "Real Sociedad") y esta regla los rechazaria. Se queda apagada."""
        import inspect
        firma = inspect.signature(w._resolve_csv_team_name)
        self.assertIs(firma.parameters["exigir_mismo_club"].default, False)


if __name__ == "__main__":
    unittest.main()
