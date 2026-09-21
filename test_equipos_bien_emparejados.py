"""Cada equipo del boleto tiene que acabar siendo ESE equipo, no uno parecido.

La auditoría de la jornada 9 encontró tres emparejados rotos, y los tres
llegaban al modelo como datos buenos:

- DEPORTIVO (F) era Always Ready, un club boliviano cuyo nombre oficial es
  "Club Deportivo Always Ready". Con él, el partido quedó etiquetado como Copa
  Libertadores Femenina, con la clasificación y el histórico de otro club. El
  buscador del proveedor devuelve un solo equipo por consulta y, con los
  nombres de la quiniela, casi siempre el que no es.
- R.MADRID (F) salía como el Real Madrid masculino por el atajo de clubes
  conocidos: de ahí los treinta partidos de LaLiga en el histórico de un cruce
  de Liga F.
- Burgos, Sabadell y R.Sociedad B quedaban "ambiguos" y sin clasificación,
  porque la "B" de "Celta B" hacía de principio de "Burgos" y el "CE" de
  "CE Sabadell" de principio de "Ceuta".
"""

import unittest
from unittest import mock

import snapshot_worker as w

PLANTILLA_LIGA_F = [
    "Alavés Gloriosas", "Athletic Club Women", "Atlético Madrid Femenino",
    "Badalona Women", "Barcelona Femení", "DUX Logroño",
    "Deportivo de La Coruña Women", "Eibar Women", "Espanyol Femení",
    "Granada Femenino", "Madrid CFF", "Real Madrid Femenino",
    "Real Sociedad Femenino", "Sevilla Women", "Tenerife Femenino",
    "Valencia Femenino",
]


def _filas_liga_f():
    filas = []
    for i, local in enumerate(PLANTILLA_LIGA_F):
        visitante = PLANTILLA_LIGA_F[(i + 1) % len(PLANTILLA_LIGA_F)]
        filas.append({"HomeTeam": local, "AwayTeam": visitante, "SeasonCode": "2627"})
    return filas


def _ficha(nombre, pais="Spain", liga="Spanish Liga F", genero="Female", alt=""):
    return {
        "idTeam": str(abs(hash(nombre)) % 100000),
        "strTeam": nombre,
        "strTeamAlternate": alt,
        "strCountry": pais,
        "strLeague": liga,
        "strGender": genero,
        "strSport": "Soccer",
    }


class _SinRed(unittest.TestCase):
    """Caché vacía, plantilla fija y un proveedor que contesta lo que se le diga."""

    respuestas: dict = {}

    def setUp(self):
        self.parches = [
            mock.patch.object(w, "THESPORTSDB_CACHE", {}),
            mock.patch.object(w, "fetch_league_history", lambda *a, **k: _filas_liga_f()),
            mock.patch.object(w, "_frenar_sportsdb", lambda *a, **k: None),
            mock.patch.object(w, "_sportsdb_hay_cupo", lambda *a, **k: True),
            mock.patch.object(w, "_request_json", self._responder),
        ]
        for p in self.parches:
            p.start()
        w._PLANTILLA_LIGA_F_MEMO.update({"cuando": 0.0, "filas": []})
        self.consultas = []

    def tearDown(self):
        for p in self.parches:
            p.stop()
        w._PLANTILLA_LIGA_F_MEMO.update({"cuando": 0.0, "filas": []})

    def _responder(self, url, params=None, timeout=None):
        consulta = (params or {}).get("t", "")
        self.consultas.append(consulta)
        return {"teams": self.respuestas.get(consulta, [])}


class LosFemeninosSeBuscanEnSuPlantillaTests(_SinRed):
    respuestas = {
        "Deportivo de La Coruña Women": [_ficha("Deportivo de La Coruña Women")],
        "Real Madrid Femenino": [_ficha("Real Madrid Femenino")],
        # Lo que devolvía el buscador libre, que es por donde se colaba.
        "Deportivo Femenino": [
            _ficha("Always Ready Femenino", pais="Bolivia", liga="_No League Soccer",
                   alt="Club Deportivo Always Ready")
        ],
    }

    def test_el_deportivo_es_el_deportivo(self):
        ficha = w.fetch_the_sportsdb_team("DEPORTIVO (F)")
        self.assertEqual(ficha.get("strTeam"), "Deportivo de La Coruña Women")
        self.assertEqual(ficha.get("strCountry"), "Spain")

    def test_se_pregunta_por_el_nombre_exacto_de_la_plantilla(self):
        w.fetch_the_sportsdb_team("DEPORTIVO (F)")
        self.assertEqual(self.consultas[0], "Deportivo de La Coruña Women")

    def test_el_madrid_femenino_no_es_el_masculino(self):
        ficha = w.fetch_the_sportsdb_team("R.MADRID (F)")
        self.assertEqual(ficha.get("strTeam"), "Real Madrid Femenino")
        self.assertEqual(ficha.get("strGender"), "Female")

    def test_la_plantilla_se_pide_una_vez_por_ciclo(self):
        llamadas = []
        with mock.patch.object(
            w, "fetch_league_history", lambda *a, **k: llamadas.append(1) or _filas_liga_f()
        ):
            for nombre in ("DEPORTIVO (F)", "R.MADRID (F)", "EIBAR (F)"):
                w._nombre_en_plantilla_liga_f(nombre)
        self.assertEqual(len(llamadas), 1)


class LaPlantillaEmparejaBienTests(_SinRed):
    def test_los_de_la_jornada_9(self):
        esperado = {
            "DEPORTIVO (F)": "Deportivo de La Coruña Women",
            "ESPANYOL (F)": "Espanyol Femení",
            "EIBAR (F)": "Eibar Women",
            "SEVILLA (F)": "Sevilla Women",
            "TENERIFE (F)": "Tenerife Femenino",
            "VALENCIA (F)": "Valencia Femenino",
            "ATH.CLUB (F)": "Athletic Club Women",
            "AT.MADRID (F)": "Atlético Madrid Femenino",
            "R.MADRID (F)": "Real Madrid Femenino",
            "MADRID CFF (F)": "Madrid CFF",
        }
        for lae, plantilla in esperado.items():
            self.assertEqual(w._nombre_en_plantilla_liga_f(lae), plantilla, lae)

    def test_quien_no_esta_en_la_categoria_no_se_empareja_con_otro(self):
        """Levante, Alhama o Betis no están en la Liga F 26/27: sin plantilla,
        nada, en vez de cogerle el nombre al vecino."""
        for lae in ("LEVANTE (F)", "ALHAMA (F)", "BETIS (F)"):
            self.assertEqual(w._nombre_en_plantilla_liga_f(lae), "", lae)


class ParecerseEnElNombreAlternativoNoBastaTests(_SinRed):
    """Fuera de la plantilla se sigue buscando libre, pero con más cuidado."""

    respuestas = {
        "Chelsea Femenino": [_ficha("Chelsea FC Women", pais="England", liga="English WSL")],
        "Quilmes Femenino": [
            _ficha("Always Ready Femenino", pais="Bolivia", alt="Club Quilmes Always Ready")
        ],
    }

    def test_un_equipo_de_fuera_que_se_llama_asi_si_vale(self):
        ficha = w.fetch_the_sportsdb_team("CHELSEA (F)")
        self.assertEqual(ficha.get("strTeam"), "Chelsea FC Women")

    def test_uno_que_solo_se_parece_por_el_alternativo_no(self):
        ficha = w.fetch_the_sportsdb_team("QUILMES (F)")
        self.assertNotEqual(ficha.get("strTeam"), "Always Ready Femenino")


class ElMismoClubNoPorUnaLetraTests(unittest.TestCase):
    def test_una_b_de_filial_no_es_el_principio_de_burgos(self):
        self.assertFalse(w._es_el_mismo_club("Burgos CF", "Celta B"))
        self.assertFalse(w._es_el_mismo_club("Real Sociedad B", "Burgos"))

    def test_el_ce_de_sabadell_no_es_el_principio_de_ceuta(self):
        self.assertFalse(w._es_el_mismo_club("Sabadell FC", "Ceuta"))

    def test_las_abreviaturas_del_boleto_siguen_valiendo(self):
        self.assertTrue(w._es_el_mismo_club("R.MADRID", "Real Madrid"))
        self.assertTrue(w._es_el_mismo_club("AT.MADRID", "Atletico Madrid"))
        self.assertTrue(w._es_el_mismo_club("R.SOCIEDAD B", "Sociedad B"))

    def test_el_sporting_es_el_de_gijon_y_nadie_mas(self):
        self.assertTrue(w._es_el_mismo_club("SPORTING", "Sp Gijon"))
        self.assertFalse(w._es_el_mismo_club("ESPAÑA", "Sp Gijon"))


class LaClasificacionDeSegundaEncuentraATodosTests(unittest.TestCase):
    EQUIPOS = [
        "Albacete", "Almeria", "Andorra", "Burgos", "Cadiz", "Castellon",
        "Celta B", "Ceuta", "Cordoba", "Eibar", "Eldense", "Girona",
        "Granada", "Las Palmas", "Leganes", "Mallorca", "Oviedo", "Sabadell",
        "Sociedad B", "Sp Gijon", "Tenerife", "Valladolid",
    ]

    def _filas(self):
        return [
            {"HomeTeam": a, "AwayTeam": self.EQUIPOS[(i + 1) % len(self.EQUIPOS)]}
            for i, a in enumerate(self.EQUIPOS)
        ]

    def test_los_que_salian_sin_clasificacion(self):
        esperado = {
            "Burgos CF": "Burgos",
            "BURGOS": "Burgos",
            "Sabadell FC": "Sabadell",
            "Real Sociedad B": "Sociedad B",
            "R.SOCIEDAD B": "Sociedad B",
            "Celta Fortuna": "Celta B",
            "CELTA FORTUNA": "Celta B",
            "SPORTING": "Sp Gijon",
        }
        for pedido, fila in esperado.items():
            self.assertEqual(
                w._resolve_csv_team_name(pedido, self._filas(), False, 0.33, False),
                fila,
                pedido,
            )

    def test_una_seleccion_no_cae_en_un_club(self):
        self.assertEqual(
            w._resolve_csv_team_name("ESPAÑA", self._filas(), False, 0.33, False), "ESPAÑA"
        )


class LaNationsLeagueNoEsUnMundialTests(unittest.TestCase):
    def test_se_llama_por_su_nombre(self):
        self.assertEqual(
            w._league_display_name("soccer_uefa_nations_league", "FIFA World Cup"),
            "UEFA Nations League",
        )

    def test_el_proveedor_la_conoce_por_su_id(self):
        self.assertEqual(w.LEAGUE_THESPORTSDB_IDS.get("soccer_uefa_nations_league"), "4490")

    def test_un_historico_de_torneo_no_etiqueta_el_partido(self):
        self.assertEqual(
            w._infer_league_from_histories(
                "INGLATERRA",
                "ESPAÑA",
                {"soccer_fifa_world_cup": [{"HomeTeam": "England", "AwayTeam": "Spain"}]},
            ),
            "",
        )


if __name__ == "__main__":
    unittest.main()
