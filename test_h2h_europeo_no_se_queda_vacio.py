# -*- coding: utf-8 -*-
"""La pestana H2H de un cruce europeo no puede salir vacia.

En la jornada 5 -toda de Champions- doce de los quince partidos llegaban a la
app con `head_to_head: {}` y la tarjeta decia "Sin histerico de enfrentamientos".
Entre ellos, cruces con historial de sobra: REAL MADRID - INTER DE MILAN, que se
vieron cuatro veces en las fases de grupos de 2020/21 y 2021/22.

La causa no era el emparejamiento de nombres: era que no habia con que
emparejar. El historico de una competicion europea solo podia salir de
TheSportsDB, y con la clave publica eso son tres temporadas como mucho -las
demas se quedan sin cupo- y ninguna eliminatoria, porque el respaldo ronda a
ronda se para a las cuatro rondas vacias y los cuartos van numerados 125.

Ahora hay archivo propio (openfootball), que empieza en 2011-12 y trae la
competicion escrita en cada fila. Esto vigila las dos mitades: que el archivo se
lea bien y que el H2H que sale de el cumpla el contrato que pintan las apps.
"""

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import snapshot_worker as w


# Trozo literal del archivo, con las dos fases de grupos en las que se cruzaron
# el Madrid y el Inter. Se conservan las trampas del formato: el ano solo esta
# escrito en la primera fecha, cada grupo vuelve a empezar en septiembre, y la
# eliminatoria trae prorroga y penaltis.
ARCHIVO_2021_22 = """= UEFA Champions League 2021/22

# Date       Tue Sep 14 2021 - Sat May 28 2022 (256d)
# Teams      32
# Matches    125


▪ Group D
  Wed Sep 15 2021
    18:45  FC Sheriff (MDA)        v Shakhtar Donetsk (UKR)   2-0 (1-0)
    21:00  Inter (ITA)             v Real Madrid (ESP)        0-1 (0-0)
  Tue Dec 7
    21:00  Real Madrid (ESP)       v Inter (ITA)              2-0 (1-0)
           Shakhtar Donetsk (UKR)  v FC Sheriff (MDA)         1-1 (1-0)


▪ Group E
  Wed Sep 15
    21:00  Bayern München (GER)    v FC Barcelona (ESP)       3-0 (1-0)
  Wed Dec 8
    21:00  FC Barcelona (ESP)      v Bayern München (GER)     0-3 (0-2)


▪ Quarterfinals
  Wed Apr 6
    21:00  Chelsea FC (ENG)        v Real Madrid (ESP)        1-3 (1-2)
  Tue Apr 12
    21:00  Real Madrid (ESP)       v Chelsea FC (ENG)         2-3 a.e.t. (1-3, 0-1)


▪ Final
  Sat May 28
    21:35  Liverpool FC (ENG)      v Real Madrid (ESP)        0-1 (0-0)
"""

ARCHIVO_2020_21 = """= UEFA Champions League 2020/21

# Date       Tue Oct 20 2020 - Sat May 29 2021 (221d)
# Teams      32
# Matches    125


▪ Group B
  Wed Oct 21 2020
    18:55  Real Madrid (ESP)       v Shakhtar Donetsk (UKR)   2-3 (0-3)
  Tue Nov 3
    21:00  Real Madrid (ESP)       v Inter (ITA)              3-2 (2-1)
  Wed Nov 25
    21:00  Inter (ITA)             v Real Madrid (ESP)        0-2 (0-1)


▪ Final
  Sat May 29
    21:00  Manchester City (ENG)   v Chelsea FC (ENG)         0-1 (0-1)
"""

# Una final decidida en los penaltis: para el H2H el partido acabo 1-1.
ARCHIVO_CON_PENALTIS = """= UEFA Champions League 2025/26

▪ Finals, Final
  Sat May 30 2026
    18:00  Paris Saint-Germain FC (FRA) v Arsenal FC (ENG)         4-3 pen. 1-1 a.e.t. (1-1, 0-1)
"""


def _filas_del_archivo():
    return (
        w._parse_openfootball_uefa(ARCHIVO_2020_21, 2020)
        + w._parse_openfootball_uefa(ARCHIVO_2021_22, 2021)
    )


class ElArchivoEuropeoSeLeeBienTests(unittest.TestCase):
    def test_el_ano_no_se_dispara_al_cambiar_de_grupo(self):
        """Cada grupo vuelve a empezar en septiembre.

        Arrastrando el mes de un grupo al siguiente, el mes "retrocede" ocho
        veces por temporada y la fase de grupos de 2011-12 acababa en 2018.
        """
        filas = w._parse_openfootball_uefa(ARCHIVO_2021_22, 2021)
        fechas = sorted(fila["Date"] for fila in filas)
        self.assertEqual(fechas[0], "2021-09-15")
        self.assertEqual(fechas[-1], "2022-05-28")
        grupo_e = [f for f in filas if f["Round"] == "Group E"]
        self.assertEqual(sorted(f["Date"] for f in grupo_e), ["2021-09-15", "2021-12-08"])

    def test_la_prorroga_cuenta_y_los_penaltis_no(self):
        cuartos = [
            f
            for f in w._parse_openfootball_uefa(ARCHIVO_2021_22, 2021)
            if f["Date"] == "2022-04-12"
        ]
        self.assertEqual(len(cuartos), 1)
        # "2-3 a.e.t. (1-3, 0-1)": el partido acabo 2-3, no 1-3.
        self.assertEqual((cuartos[0]["FTHG"], cuartos[0]["FTAG"]), (2, 3))
        self.assertEqual(cuartos[0]["FTR"], "A")

        final = w._parse_openfootball_uefa(ARCHIVO_CON_PENALTIS, 2025)
        self.assertEqual(len(final), 1)
        # "4-3 pen. 1-1 a.e.t.": para el futbol -y para el H2H- fue un empate.
        self.assertEqual((final[0]["FTHG"], final[0]["FTAG"]), (1, 1))
        self.assertEqual(final[0]["FTR"], "D")

    def test_cada_fila_sabe_de_que_competicion_es(self):
        """Si `liga` llega vacia, la app pinta la tarjeta sin competicion."""
        for fila in _filas_del_archivo():
            self.assertEqual(fila["League"], "UEFA Champions League")
            self.assertTrue(fila["Date"])
            self.assertEqual(fila["Source"], w.OPENFOOTBALL_SOURCE)

    def test_la_eliminatoria_es_historial_pero_no_jornada(self):
        """Sumar los cruces a una clasificacion le da nueve jornadas al que
        llego a la final y ocho al que no."""
        filas = w._parse_openfootball_uefa(ARCHIVO_2021_22, 2021)
        fases = {f["Round"]: f["Stage"] for f in filas}
        self.assertEqual(fases["Group D"], "group")
        self.assertEqual(fases["Quarterfinals"], "knockout")
        self.assertEqual(fases["Final"], "knockout")
        for seccion, esperada in (
            ("League phase", "group"),
            ("League, Matchday 7", "group"),
            ("Gruppe G", "group"),
            ("Playoffs, Matchday 1", "knockout"),
            ("Finals, Round of 16", "knockout"),
            ("Sechzehntelfinale", "knockout"),
        ):
            with self.subTest(seccion):
                self.assertEqual(w._openfootball_fase(seccion), esperada)

    def test_la_tabla_de_una_temporada_cerrada_no_cuenta_eliminatorias(self):
        with patch.object(
            w, "fetch_league_history", return_value=_filas_del_archivo()
        ):
            w.HISTORY_CACHE.pop("final-table:soccer_uefa_champs_league:2122", None)
            tabla = w._final_table_for_season("soccer_uefa_champs_league", "2122")
        # En el trozo de archivo, la liguilla son seis partidos y cada equipo
        # aparece en dos: nadie puede salir con mas de dos jugados.
        self.assertTrue(tabla)
        self.assertLessEqual(max(fila["played"] for fila in tabla.values()), 2)
        self.assertNotIn("Chelsea FC", tabla, "los cuartos no son una jornada")

    def test_el_pais_no_se_queda_pegado_al_nombre(self):
        nombres = {f["HomeTeam"] for f in _filas_del_archivo()}
        self.assertIn("Real Madrid", nombres)
        self.assertNotIn("Real Madrid (ESP)", nombres)


class UnCruceEuropeoConocidoTraeSuH2HTests(unittest.TestCase):
    """El caso que se veia vacio en produccion."""

    def _h2h(self, local, visitante, filas=None):
        filas = _filas_del_archivo() if filas is None else filas
        return w._head_to_head_metrics(
            w._completed_rows_before_kickoff(filas, None),
            local,
            visitante,
            last_n=w.H2H_LAST_N,
        )

    def test_real_madrid_inter_trae_sus_cuatro_partidos(self):
        h2h = self._h2h("Real Madrid", "Inter Milan")
        self.assertEqual(h2h.get("meetings"), 4, "el Madrid y el Inter se vieron cuatro veces")
        self.assertEqual(len(h2h.get("recent_matches") or []), 4)
        # Las cuatro las gano el Madrid, dos en casa y dos en San Siro: el
        # contador tiene que ir con el equipo, no con el lado del acta.
        self.assertEqual(h2h.get("home_team_wins"), 4)
        self.assertEqual(h2h.get("away_team_wins"), 0)
        self.assertEqual(h2h.get("draws"), 0)
        self.assertEqual(h2h.get("wins_local"), h2h.get("home_team_wins"))
        self.assertEqual(h2h.get("wins_visit"), h2h.get("away_team_wins"))

    def test_el_contrato_que_pintan_las_apps(self):
        recientes = self._h2h("Real Madrid", "Inter Milan")["recent_matches"]
        for fila in recientes:
            self.assertTrue(
                (fila.get("liga") or "").strip(),
                "un H2H europeo sin competicion es media entrega: " + repr(fila),
            )
            self.assertRegex(fila["date"], r"^\d{4}-\d{2}-\d{2}$")
            self.assertRegex(fila["score"], r"^\d+-\d+$")
            self.assertTrue(fila["home"] and fila["away"])
        # Del mas antiguo al mas reciente, y el marcador en el orden de su fila.
        self.assertEqual([f["date"] for f in recientes], sorted(f["date"] for f in recientes))
        primero = recientes[0]
        self.assertEqual((primero["home"], primero["away"], primero["score"]),
                         ("Real Madrid", "Inter", "3-2"))

    def test_los_nombres_largos_del_archivo_tambien_encuentran_al_equipo(self):
        """Cada fuente escribe el club a su manera y ninguna manda.

        El boleto trae "PSG" y "BAYERN MUNICH"; el archivo, "Paris Saint-Germain
        FC" y "Bayern München". Sin resolverlo, el cruce existe y el H2H sale
        vacio igual.
        """
        for boleto, archivo in (
            ("PSG", "Paris Saint-Germain FC"),
            ("Bayern Munich", "Bayern München"),
            ("Inter Milan", "FC Internazionale Milano"),
            ("Sporting Lisbon", "Sporting Clube de Portugal"),
            ("Ath Madrid", "Club Atlético de Madrid"),
        ):
            with self.subTest(boleto):
                self.assertTrue(
                    w._row_is_h2h(
                        {"HomeTeam": archivo, "AwayTeam": "Rival"}, boleto, "Rival"
                    ),
                    f"{boleto} tiene que encontrarse en {archivo}",
                )

    def test_barcelona_bayern_sale_del_mismo_archivo(self):
        h2h = self._h2h("Barcelona", "Bayern Munich")
        self.assertEqual(h2h.get("meetings"), 2)
        self.assertEqual(
            {f["liga"] for f in h2h["recent_matches"]}, {"UEFA Champions League"}
        )


class ElH2HNoSeLlenaDeVecinosTests(unittest.TestCase):
    """Con toda Europa en el pozo, parecerse por letras no basta.

    "Manchester City" y "Manchester United" puntuan 0.81 de parecido, y
    "Leicester City" 0.76: por encima del umbral. Asi es como el H2H de
    OPORTO - MAN.CITY se llenaba de partidos contra el Leicester.
    """

    def test_no_confunde_a_dos_clubes_distintos(self):
        for fila_local, fila_visitante in (
            ("Manchester United FC", "FC Porto"),
            ("Leicester City", "FC Porto"),
            ("Rangers FC", "FC Porto"),
            ("Sparta Praha", "FC Porto"),
        ):
            with self.subTest(fila_local):
                self.assertFalse(
                    w._row_is_h2h(
                        {"HomeTeam": fila_local, "AwayTeam": fila_visitante},
                        "Man City",
                        "Porto",
                    ),
                    f"{fila_local} no es el Manchester City",
                )

    def test_el_braga_no_es_un_sporting(self):
        self.assertFalse(
            w._row_is_h2h(
                {"HomeTeam": "Sporting Braga", "AwayTeam": "Galatasaray"},
                "Sporting Lisbon",
                "Galatasaray",
            ),
            "el Sporting CP y el Braga son dos clubes",
        )

    def test_el_mismo_partido_por_dos_fuentes_cuenta_una_vez(self):
        """El archivo y TheSportsDB traen el mismo 2-0 con nombres distintos.

        Sin unificarlos, el marcador global dice el doble de enfrentamientos de
        los que hubo.
        """
        filas = _filas_del_archivo() + [
            {
                "Date": "2021-12-07",
                "HomeTeam": "Real Madrid",
                "AwayTeam": "Inter Milan",
                "FTHG": 2,
                "FTAG": 0,
                "FTR": "H",
                "League": "",
                "Source": "TheSportsDB-H2H",
            }
        ]
        h2h = w._head_to_head_metrics(
            w._completed_rows_before_kickoff(filas, None),
            "Real Madrid",
            "Inter Milan",
            last_n=w.H2H_LAST_N,
        )
        self.assertEqual(h2h.get("meetings"), 4)
        # Y gana la fila que sabe de que competicion es.
        del_dia = [f for f in h2h["recent_matches"] if f["date"] == "2021-12-07"]
        self.assertEqual(len(del_dia), 1)
        self.assertEqual(del_dia[0]["liga"], "UEFA Champions League")


class ElArchivoEuropeoEsMasculinoTests(unittest.TestCase):
    """El worker ya tiene guardas de categoria; esta fuente no las puede saltar.

    "Real Madrid" y "Real Madrid Femenino" se parecen de sobra para colarse.
    """

    def test_un_cruce_femenino_no_hereda_el_h2h_del_primer_equipo(self):
        histories = {"soccer_spain_la_liga": []}
        with (
            patch.object(w, "fetch_league_history", return_value=list(_filas_del_archivo())),
            patch.object(w, "fetch_the_sportsdb_h2h_events", return_value=[]),
            patch.object(w, "_fill_side_from_sportsdb_if_empty", side_effect=lambda h, *a, **k: h or {}),
        ):
            _home, _away, h2h = w._resolve_domestic_histories_and_h2h(
                home_team="Real Madrid Femenino",
                away_team="Inter Milan Women",
                league_key="soccer_uefa_champs_league",
                histories=histories,
                home_team_api={},
                away_team_api={},
                kickoff_dt=datetime(2026, 9, 9, 19, 0, tzinfo=timezone.utc),
            )
        self.assertFalse(h2h.get("recent_matches"), "el archivo europeo es masculino")


class ElArchivoNoSeVuelveAPedirTests(unittest.TestCase):
    """Un partido de 2020 no cambia: pedirlo cada ciclo es gastar por gastar."""

    def test_una_temporada_cerrada_se_guarda_para_mucho(self):
        self.assertGreaterEqual(w.UEFA_ARCHIVE_CACHE_TTL_SECONDS, 7 * 24 * 3600)

    def test_la_temporada_que_no_existe_se_recuerda_ausente(self):
        """Sin esto, cada ciclo repite el mismo 404 por cada temporada que el
        repositorio aun no ha publicado."""
        w.HISTORY_CACHE.pop("openfootball:v1:cl.txt:1999", None)
        w.HISTORY_CACHE.pop("openfootball_ausente:v1:cl.txt:1999", None)
        with patch.object(w, "_request_text", side_effect=Exception("404 Client Error")) as pedir:
            self.assertEqual(w._fetch_openfootball_uefa_temporada("cl.txt", 1999), [])
            self.assertEqual(w._fetch_openfootball_uefa_temporada("cl.txt", 1999), [])
        self.assertEqual(pedir.call_count, 1, "la segunda vez ya no se pide")

    def test_el_memo_se_vacia_al_empezar_el_ciclo(self):
        """El worker vive dias: si el memo no se vacia, una temporada nueva no
        entra hasta reiniciarlo, y da igual que la cache haya caducado."""
        w._OPENFOOTBALL_MEMO[("cl.txt", (2020,))] = [{"Date": "2020-11-03"}]
        w._reiniciar_memo_openfootball()
        self.assertFalse(w._OPENFOOTBALL_MEMO)
        import inspect

        self.assertIn("_reiniciar_memo_openfootball()", inspect.getsource(w.run_once))

    def test_la_poda_del_historico_no_se_lleva_el_archivo(self):
        """La poda mira el ultimo trozo de la clave; si el ano no le suena,
        borra la temporada entera en cada guardado y no persiste nunca."""
        conservadas = w._temporadas_que_se_conservan()
        for anio in w._openfootball_temporadas(w.H2H_SEASONS_BACK):
            with self.subTest(anio):
                self.assertIn(str(anio), conservadas)

    def test_lo_que_trae_el_archivo_no_se_le_pide_al_proveedor(self):
        """Cada temporada europea que resuelve el archivo son peticiones que le
        quedan a TheSportsDB para la temporada en curso, que es la unica que el
        archivo no puede tener."""
        archivo = [
            {
                "Date": "2021-12-07",
                "HomeTeam": "Real Madrid",
                "AwayTeam": "Inter",
                "FTHG": 2,
                "FTAG": 0,
                "FTR": "H",
                "SeasonCode": "2122",
                "League": "UEFA Champions League",
                "Source": w.OPENFOOTBALL_SOURCE,
            }
        ]
        pedidas = []

        def falso_sportsdb(league_key, league_id, seasons_back=None, saltar_temporadas=None):
            pedidas.append(set(saltar_temporadas or ()))
            return []

        with (
            patch.object(w, "_fetch_openfootball_uefa_history", return_value=archivo),
            patch.object(w, "_fetch_sportsdb_league_history", side_effect=falso_sportsdb),
        ):
            filas = w.fetch_league_history("soccer_uefa_champs_league", seasons_back=20)
        self.assertEqual(filas, archivo)
        self.assertEqual(pedidas, [{"2122"}])


if __name__ == "__main__":
    unittest.main()
