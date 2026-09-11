# -*- coding: utf-8 -*-
"""Dos agujeros de la jornada 6, que no tienen nada que ver entre si.

1. RACING S. - ALAVÉS salia con el H2H vacio, y se han visto cuatro veces:
   2014, 2015, 2022 y 2023, las cuatro en Segunda. El buscador solo preguntaba
   por las ligas en las que los equipos estan HOY, y como el Racing subio, los
   dos figuran en Primera, donde no se han cruzado nunca. Le pasa a cualquier
   recien ascendido o descendido.

2. Los cuatro partidos de Liga F se resolvian contra ligas MASCULINAS, y en la
   ficha de ALAVÉS (F) - GRANADA (F) acababa el Granada masculino, noveno de
   Segunda. El boleto sella la categoria -"ALAVÉS (F)"- y el XML de origen la
   trae, pero el worker se la quitaba el solo: `_canonical_team_name` borra el
   sufijo a proposito y el parser tiraba el nombre original, asi que aguas
   abajo no quedaba ni rastro y la guarda de categoria no llegaba a saltar.

   El unico que se salvaba era EIBAR - BADALONA W., y por casualidad: el
   proveedor escribe "W." y eso si es una marca que el worker reconoce.
"""

import unittest

import snapshot_worker as w


class CrucesEnOtraDivisionTests(unittest.TestCase):
    def test_el_racing_y_el_alaves_se_vieron_en_segunda(self):
        """El caso real, con los nombres que usa football-data."""
        filas = [
            {"Date": "01/11/2022", "HomeTeam": "Santander", "AwayTeam": "Alaves",
             "FTHG": 1, "FTAG": 1, "FTR": "D", "Div": "SP2"},
            {"Date": "21/01/2023", "HomeTeam": "Alaves", "AwayTeam": "Santander",
             "FTHG": 3, "FTAG": 0, "FTR": "H", "Div": "SP2"},
        ]
        h2h = w._head_to_head_metrics(
            w._completed_rows_before_kickoff(filas, None), "Santander", "Alaves", last_n=30
        )
        self.assertEqual(h2h.get("meetings"), 2)

    def test_la_liga_de_hoy_no_es_la_unica_que_se_pregunta(self):
        self.assertIn(
            "soccer_spain_segunda_division",
            w._divisiones_hermanas("soccer_spain_la_liga"),
            "un cruce de Primera tiene que mirar tambien en Segunda",
        )
        self.assertIn(
            "soccer_spain_la_liga",
            w._divisiones_hermanas("soccer_spain_segunda_division"),
        )
        self.assertIn("soccer_efl_champ", w._divisiones_hermanas("soccer_epl"))

    def test_no_se_mezclan_paises(self):
        hermanas = w._divisiones_hermanas("soccer_spain_la_liga")
        self.assertNotIn("soccer_epl", hermanas)
        self.assertNotIn("soccer_italy_serie_a", hermanas)

    def test_solo_ligas_con_historico_propio(self):
        """Cada clave que se anada son peticiones. Las que solo conoce
        TheSportsDB se quedan fuera: ahi el cupo se acaba enseguida."""
        for clave in w._divisiones_hermanas("soccer_spain_la_liga"):
            self.assertIn(clave, w.LEAGUE_FOOTBALL_DATA_CODES)
        self.assertEqual(w._divisiones_hermanas("soccer_uefa_champs_league"), [])
        self.assertEqual(w._divisiones_hermanas("sportsdb_5106"), [])
        self.assertEqual(w._divisiones_hermanas(""), [])

    def test_el_h2h_pregunta_por_las_hermanas(self):
        """De punta a punta: con el partido en Primera, el historico de Segunda
        tiene que llegar igual."""
        segunda = [
            {"Date": "01/11/2022", "HomeTeam": "Santander", "AwayTeam": "Alaves",
             "FTHG": 1, "FTAG": 1, "FTR": "D"},
            {"Date": "21/01/2023", "HomeTeam": "Alaves", "AwayTeam": "Santander",
             "FTHG": 3, "FTAG": 0, "FTR": "H"},
        ]
        pedidas = []

        def falso_historico(clave, seasons_back=None):
            pedidas.append(clave)
            return list(segunda) if clave == "soccer_spain_segunda_division" else []

        from unittest.mock import patch

        with (
            patch.object(w, "fetch_league_history", side_effect=falso_historico),
            patch.object(w, "fetch_the_sportsdb_h2h_events", return_value=[]),
            patch.object(w, "_fill_side_from_sportsdb_if_empty", side_effect=lambda h, *a, **k: h or {}),
        ):
            _home, _away, h2h = w._resolve_domestic_histories_and_h2h(
                home_team="Santander",
                away_team="Alaves",
                league_key="soccer_spain_la_liga",
                histories={},
                home_team_api={},
                away_team_api={},
                kickoff_dt=None,
            )
        self.assertIn("soccer_spain_segunda_division", pedidas)
        self.assertEqual(h2h.get("meetings"), 2, "los cruces de Segunda tienen que contar")


class LaCategoriaDelBoletoLlegaTests(unittest.TestCase):
    """El sufijo "(F)" tiene que sobrevivir desde el XML hasta la guarda."""

    XML = """<?xml version="1.0" encoding="UTF-8"?>
<root><porcentajes jornada="6" temporada="2027" activo="si">
  <partido num="5" local="R.MADRID" visitante="RAYO" porc_1="89" porc_X="7" porc_2="4"/>
  <partido num="11" local="ALAV&#201;S (F)" visitante="GRANADA (F)" porc_1="60" porc_X="24" porc_2="16"/>
  <partido num="14" local="MADRID CFF (F)" visitante="SEVILLA (F)" porc_1="78" porc_X="13" porc_2="9"/>
</porcentajes></root>"""

    def _slots(self):
        payload = w._eduardo_parse_percentages_xml(self.XML, "Eduardo Losilla LAE", "http://x")
        return {s["position"]: s for s in payload["matches"]}

    def test_el_parser_guarda_el_nombre_del_boleto(self):
        slots = self._slots()
        self.assertEqual(slots[11]["local_lae"], "ALAVÉS (F)")
        self.assertEqual(slots[11]["visitante_lae"], "GRANADA (F)")
        # Y el canonico sigue sin sufijo, que es para lo que sirve.
        self.assertNotIn("(F)", slots[11]["local"])

    def test_el_partido_sabe_que_es_femenino(self):
        slots = self._slots()
        partido = {"local": slots[11]["local"], "visitante": slots[11]["visitante"]}
        self.assertIsNone(
            w._categoria_del_partido(partido),
            "sin el boleto no hay forma de saberlo: ese era el fallo",
        )
        w._apply_quiniela_slot(partido, 6, slots[11])
        self.assertEqual(w._categoria_del_partido(partido), "female")

    def test_no_se_queda_una_liga_masculina_puesta(self):
        """La consecuencia: el Granada masculino colgado de un partido de Liga F."""
        slots = self._slots()
        partido = {
            "local": slots[11]["local"],
            "visitante": slots[11]["visitante"],
            "league": "soccer_spain_segunda_division",
        }
        w._apply_quiniela_slot(partido, 6, slots[11])
        w._apply_dynamic_league_metadata(partido, {}, {}, {})
        self.assertEqual(partido["league"], "league_unresolved")
        self.assertEqual(partido.get("league_descartada"), "soccer_spain_segunda_division")

    def test_al_proveedor_se_le_pide_el_equipo_femenino(self):
        """Buscando "Alavés" a secas vuelve el masculino; con la marca, no."""
        slots = self._slots()
        partido = {"local": slots[11]["local"], "visitante": slots[11]["visitante"]}
        w._apply_quiniela_slot(partido, 6, slots[11])
        self.assertEqual(w._nombre_para_el_proveedor(partido, "local"), "ALAVÉS (F)")
        self.assertEqual(w._categoria_por_nombre(w._nombre_para_el_proveedor(partido, "local")), "female")

    def test_un_partido_masculino_no_cambia(self):
        slots = self._slots()
        partido = {
            "local": slots[5]["local"],
            "visitante": slots[5]["visitante"],
            "league": "soccer_spain_la_liga",
        }
        w._apply_quiniela_slot(partido, 6, slots[5])
        w._apply_dynamic_league_metadata(partido, {}, {}, {})
        self.assertEqual(partido["league"], "soccer_spain_la_liga")
        self.assertIsNone(w._categoria_del_partido(partido))

    def test_el_sufijo_masculino_no_se_le_pasa_al_proveedor(self):
        """En las jornadas europeas el boleto sella "(M)". Ningun proveedor
        llama "REAL MADRID (M)" a nadie: ahi se busca como siempre."""
        partido = {
            "local": "Real Madrid",
            "visitante": "Inter Milan",
            "local_lae": "REAL MADRID (M)",
            "visitante_lae": "INTER DE MILÁN (M)",
        }
        self.assertEqual(w._nombre_para_el_proveedor(partido, "local"), "Real Madrid")
        self.assertEqual(w._categoria_del_partido(partido), "male")


if __name__ == "__main__":
    unittest.main()


class LaCacheNoSirveFichasViejasTests(unittest.TestCase):
    """El ensayo en seco no vio el arreglo, y no era el arreglo: era la cache.

    Lo que se guarda ahi lo produce el parser. Al anadirle el nombre con sufijo
    de categoria, las fichas guardadas por el parser anterior se quedaron sin
    ese campo, y el codigo nuevo las seguia leyendo tan contento. Peor aun: cada
    ciclo las vuelve a guardar, asi que el TTL de seis horas no las caduca nunca.
    """

    def test_la_clave_lleva_version(self):
        import inspect

        for funcion in (w._fetch_eduardo_percentages_source, w.fetch_quiniela_jornada_page):
            with self.subTest(funcion.__name__):
                fuente = inspect.getsource(funcion)
                self.assertRegex(
                    fuente,
                    r'cache_key = f"eduardo:[^"]*:v\d+:',
                    "la clave tiene que llevar version para invalidar lo viejo",
                )

    def test_una_ficha_del_parser_viejo_no_se_usa(self):
        """Comprobado de verdad: se mete a mano una ficha sin el campo nuevo con
        la clave vieja y no puede colarse."""
        ctx_temp = 2027
        vieja = {
            "ok": True, "matches": [{"position": 11, "local": "Alavés", "visitante": "GRANADA"}],
            "pleno15": {},
        }
        w.EXTERNAL_FEEDS_CACHE[f"eduardo:merged:{ctx_temp}:99"] = {
            "fetched_at": w._now_iso(), "data": vieja,
        }
        leido = w._cache_get(w.EXTERNAL_FEEDS_CACHE, f"eduardo:merged:v2:{ctx_temp}:99", 6 * 3600)
        self.assertIsNone(leido, "la clave nueva no puede leer lo guardado por la vieja")
