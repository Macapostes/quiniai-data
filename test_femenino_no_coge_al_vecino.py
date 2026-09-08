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

    def test_dos_levantes_distintos_se_separan_al_elegir(self):
        """Levante Las Planas y Levante UD son clubes distintos, pero eso no se
        puede ver comparando dos nombres: "Levante" encaja en los dos, igual que
        "Celta" encaja en "Celta Vigo" y ahi si es el mismo. Se resuelve donde
        se elige: si encajan varios, no se resuelve ninguno."""
        filas = [
            {"HomeTeam": "Levante UD", "AwayTeam": "Levante Las Planas"},
            {"HomeTeam": "Levante Las Planas", "AwayTeam": "Levante UD"},
        ]
        self.assertEqual(w._resolve_csv_team_name("LEVANTE", filas), "LEVANTE")

    def test_un_nombre_corto_si_resuelve_cuando_no_hay_duda(self):
        """Lo contrario tambien tiene que valer: si solo encaja uno, se coge."""
        filas = [{"HomeTeam": "Celta", "AwayTeam": "Sevilla"}]
        self.assertEqual(w._resolve_csv_team_name("Celta Vigo", filas), "Celta")

    def test_ahora_lo_usan_todos_los_caminos(self):
        """Al principio se dejo apagada en masculino porque la regla iba en un
        solo sentido y rechazaba "Sociedad" por "Real Sociedad". Ya es
        bidireccional, y hacia falta: en el feed del 08-09-2026 habia once
        equipos resueltos a otro club -OPORTO a Everton, PSG a KuPS, Athletic
        Bilbao a Almeria- y de ahi salian la clasificacion y el H2H del informe."""
        import inspect

        for fn in (w._resolve_csv_team_name, w._team_history_context):
            with self.subTest(fn.__name__):
                firma = inspect.signature(fn)
                self.assertIs(firma.parameters["exigir_mismo_club"].default, True)

    def test_los_equipos_europeos_que_se_confundian(self):
        casos = [
            ("OPORTO", "Everton"), ("PSG", "KuPS"), ("ANDORRA FC", "Riga FC"),
            ("LOGROÑO", "La Coruna"), ("Athletic Bilbao", "Almeria"),
            ("SEVILLA", "Sabadell"), ("Alavés", "Albacete"),
            ("MADRID CFF", "Ath Madrid"),
        ]
        for pedido, otro in casos:
            with self.subTest(f"{pedido} != {otro}"):
                self.assertFalse(w._es_el_mismo_club(pedido, otro))

    def test_las_abreviaturas_legitimas_siguen_pasando(self):
        casos = [
            ("Real Sociedad", "Sociedad"), ("Real Betis", "Betis"),
            ("Celta Vigo", "Celta"), ("Rayo Vallecano", "Vallecano"),
            ("Atlético Madrid", "Ath Madrid"), ("Athletic Bilbao", "Ath Bilbao"),
            ("Espanyol", "Espanol"), ("Lillestrom", "Lillestrøm"),
            ("HamKam", "Hamarkameratene"), ("Sarpsborg FK", "Sarpsborg 08"),
        ]
        for pedido, escrito in casos:
            with self.subTest(f"{pedido} == {escrito}"):
                self.assertTrue(w._es_el_mismo_club(pedido, escrito))


class NingunaLlamadaLoApagaTests(unittest.TestCase):
    """Cambiar el valor por defecto no basta: dos llamadas lo pasaban explicito.

    Con el candado activado por defecto pero apagado en esas dos, el feed seguia
    resolviendo OPORTO a Everton, PSG a KuPS y Athletic Bilbao a Almeria. Solo
    bajo de 11 equipos mal a 9, y ahi se vio.
    """

    def test_nadie_lo_pasa_apagado(self):
        import re

        fuente = inspect_getsource()
        for linea in fuente.splitlines():
            limpia = linea.strip()
            if not limpia.startswith("exigir_mismo_club="):
                continue
            valor = limpia.split("=", 1)[1].rstrip(",")
            with self.subTest(limpia):
                self.assertIn(
                    valor, {"True", "exigir_mismo_club"},
                    f"esta llamada apaga el candado: {limpia}",
                )


def inspect_getsource():
    import os

    ruta = os.path.join(os.path.dirname(os.path.abspath(__file__)), "snapshot_worker.py")
    with open(ruta, encoding="utf-8") as fh:
        return fh.read()


if __name__ == "__main__":
    unittest.main()
