# -*- coding: utf-8 -*-
"""Las bajas tienen que ser de ESE equipo, y de esa categoría.

En el informe de la jornada 6, el VALENCIA (F) llegaba con tres bajas: Diakhaby
—del Valencia masculino—, Corberán —que es el ENTRENADOR del masculino— y
Lamine Yamal, que juega en el Barcelona. El GRANADA (F) traía a Diallo, del
Granada CF, y el SEVILLA (F) a Rubén Vargas. Todo eso salió impreso en un PDF
de pago, y el propio modelo escribió en el informe que sospechaba que eran del
equipo masculino.

Dos fallos encadenados:

1. `_build_injury_entities` ya sabe descartar noticias del primer equipo en un
   cruce femenino, pero deduce la categoría del nombre, y "VALENCIA (F)" le
   llegaba canonizado como "VALENCIA".

2. Lo de Lamine Yamal es peor porque también afecta al masculino: el titular
   "Lamine Yamal, baja en el último entrenamiento previo al Valencia CF" habla
   del Barcelona y nombra al Valencia como RIVAL. Nombrarlo lo nombra, así que
   pasaba la comprobación de procedencia. La guarda que había mira si otro club
   aparece ANTES; aquí el Barcelona aparece después, al final, como fuente.
"""

import unittest

import snapshot_worker as w

VALENCIA = [
    "Lamine Yamal, baja en el último entrenamiento previo al Valencia CF - FC Barcelona",
    "Diakhaby, baja en el Valencia CF por una lesión muscular - Europa Press",
]


def _items(titulares, fuente="Europa Press"):
    return [
        {"title": t, "source": fuente, "link": "http://x", "published_at": ""}
        for t in titulares
    ]


class NoSomosElEquipoDeLaNoticiaTests(unittest.TestCase):
    def test_nombrarnos_como_rival_no_nos_hace_protagonistas(self):
        casos = [
            ("Lamine Yamal, baja en el último entrenamiento previo al Valencia CF", "VALENCIA", True),
            ("Mbappé no viajará a Pamplona para el duelo ante Osasuna", "OSASUNA", True),
            ("El Betis recibe al Villarreal sin Isco", "VILLARREAL", True),
            ("Diakhaby, baja en el Valencia CF por una lesión muscular", "VALENCIA", False),
            ("Osasuna pierde a Budimir por lesión", "OSASUNA", False),
            ("Isco, baja en el Betis", "BETIS", False),
        ]
        for titular, equipo, esperado in casos:
            with self.subTest(titular[:40]):
                self.assertEqual(
                    w._somos_el_rival_en_el_titular(titular, equipo), esperado
                )

    def test_si_nos_nombra_como_sujeto_aunque_luego_haya_rival_es_nuestra(self):
        """"Baja del Valencia CF contra el Barcelona" sí es del Valencia."""
        self.assertFalse(
            w._somos_el_rival_en_el_titular(
                "Baja confirmada del Valencia CF contra el FC Barcelona: problemas para Corberán",
                "VALENCIA",
            )
        )

    def test_lamine_yamal_no_es_baja_del_valencia_ni_en_el_masculino(self):
        nombres = [e.get("player_name") for e in w._build_injury_entities("VALENCIA", _items(VALENCIA))]
        self.assertNotIn("Lamine Yamal", nombres)
        # Y la baja de verdad sigue estando.
        self.assertIn("Diakhaby", nombres)


class LaCategoriaDecideDeQuienSonLasBajasTests(unittest.TestCase):
    def test_un_cruce_femenino_no_hereda_las_bajas_del_primer_equipo(self):
        self.assertEqual(w._build_injury_entities("VALENCIA (F)", _items(VALENCIA)), [])

    def test_al_extractor_se_le_pasa_el_nombre_del_boleto(self):
        import inspect

        fuente = inspect.getsource(w)
        trozo = fuente[fuente.index("home_injuries = _build_injury_entities"):][:400]
        self.assertIn('_nombre_para_el_proveedor(match, "local")', trozo)
        self.assertIn('_nombre_para_el_proveedor(match, "visitante")', trozo)

    def test_un_partido_masculino_conserva_sus_bajas(self):
        nombres = [
            e.get("player_name")
            for e in w._build_injury_entities(
                "GRANADA", _items(["Diallo, baja en el último entrenamiento del Granada CF"], "Ideal")
            )
        ]
        self.assertIn("Diallo", nombres)


if __name__ == "__main__":
    unittest.main()


class SinPoderConfirmarNoSeNombraANadieTests(unittest.TestCase):
    """Si no se puede comprobar que el jugador es de ese equipo, no se publica
    su nombre: se dice que hay bajas y cuántas.

    No es prudencia genérica. El índice de plantillas se llena con
    `lookup_all_players` de TheSportsDB y la clave pública lo bloquea: hoy hay
    CERO plantillas en caché, así que ningún nombre que imprimamos está
    verificado. Un nombre equivocado se detecta de un vistazo y tira abajo la
    credibilidad del informe entero; "3 bajas" sigue sirviendo para el signo.

    Con una clave propia el índice se llena y los nombres vuelven solos.
    """

    def test_hoy_no_hay_plantillas_con_las_que_confirmar(self):
        self.assertFalse(
            w._jugador_confirmado_del_equipo("Diakhaby", "VALENCIA"),
            "sin plantilla no se puede confirmar a nadie",
        )

    def test_con_plantilla_si_se_confirma(self):
        w._INDICE_DE_JUGADORES.setdefault(w._norm_persona("Diakhaby"), set()).add(
            w._norm_persona("VALENCIA")
        )
        try:
            self.assertTrue(w._jugador_confirmado_del_equipo("Diakhaby", "VALENCIA"))
            self.assertFalse(w._jugador_confirmado_del_equipo("Diakhaby", "SEVILLA"))
        finally:
            w._INDICE_DE_JUGADORES.pop(w._norm_persona("Diakhaby"), None)

    def test_cada_baja_dice_si_esta_verificada(self):
        entidades = w._build_injury_entities(
            "VALENCIA",
            _items(["Diakhaby, baja en el Valencia CF por una lesión muscular"]),
        )
        self.assertTrue(entidades)
        for e in entidades:
            self.assertIn("verificado", e)
            self.assertFalse(e["verificado"], "sin plantilla no puede estar verificada")

    def test_el_texto_del_worker_no_nombra_a_los_no_verificados(self):
        import inspect

        fuente = inspect.getsource(w)
        trozo = fuente[fuente.index("home_injury_names = ["):][:500]
        self.assertIn('i.get("verificado")', trozo)
