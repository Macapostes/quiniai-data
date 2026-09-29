"""Los fallos del feed de las jornadas 10 y 11 (2026-27), uno por uno.

Nations League con meteo de Honduras e Iran, "Mundial" y "ronda 1" donde no
tocaba, el Bernabeu de sede por defecto, horarios de otro partido, MotoGP y
bolsa como fichajes, y "Biglietteria", "Cuatro" o Klopp como bajas.
"""

import os
import unittest
from unittest import mock

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import filtros_feed  # noqa: E402
import snapshot_worker as w  # noqa: E402


class SeleccionesTests(unittest.TestCase):
    def test_nombres_del_boleto(self):
        for lae, canonico in (
            ("LUXEMBURGO", "Luxembourg"), ("ISLANDIA", "Iceland"), ("REP.CHECA", "Czech Republic"),
            ("MACEDONIA N.", "North Macedonia"), ("KAZAJISTÁN", "Kazakhstan"), ("GRECIA", "Greece"),
            ("PAÍSES BAJOS", "Netherlands"), ("REP.IRLANDA", "Republic of Ireland"),
            ("AZERBAIYAN", "Azerbaijan"), ("DINAMARCA", "Denmark"), ("ESCOCIA", "Scotland"),
        ):
            self.assertEqual(w._canonical_team_name(lae), canonico, lae)
            self.assertTrue(w._es_seleccion(lae), lae)

    def test_andorra_fc_sigue_siendo_un_club(self):
        self.assertFalse(w._es_seleccion("ANDORRA FC"))
        self.assertEqual(w._canonical_team_name("ANDORRA FC"), "FC Andorra")

    def test_la_tabla_pisa_un_perfil_envenenado(self):
        envenenado = {
            "city": "Luxemburgo", "country": "Honduras", "country_code": "HN",
            "timezone": "America/Tegucigalpa", "latitude": 14.9, "longitude": -88.2,
        }
        perfil = w._apply_location_override_fields(envenenado, "LUXEMBURGO")
        self.assertEqual(perfil["country_code"], "LU")
        self.assertEqual(perfil["timezone"], "Europe/Luxembourg")
        self.assertAlmostEqual(perfil["latitude"], 49.6116)

    def test_un_club_no_se_fuerza(self):
        perfil = w._apply_location_override_fields({"city": "Sabadell", "timezone": "Europe/Madrid"}, "SABADELL")
        self.assertEqual(perfil["city"], "Sabadell")


class EventoTests(unittest.TestCase):
    def test_sin_evento_propio_no_se_hereda_el_proximo_de_otro(self):
        ajeno = {
            "idEvent": "999", "strHomeTeam": "Scotland", "strAwayTeam": "Brazil",
            "strLeague": "FIFA World Cup", "strVenue": "MetLife Stadium", "strCity": "New Jersey",
            "dateEvent": "2026-06-24", "strTime": "22:00:00",
        }
        with mock.patch.object(w, "fetch_the_sportsdb_next_event", return_value=ajeno), \
                mock.patch.object(w, "fetch_the_sportsdb_round_events", return_value=[]):
            evento = w._resolve_sportsdb_event(
                "Scotland", "Switzerland", "2026-09-29T18:45:00Z",
                {"idTeam": "1", "idLeague": "4429"}, {"idTeam": "2", "idLeague": "4429"},
            )
        self.assertEqual(evento, {})

    def test_el_mismo_cruce_en_otra_fecha_es_otro_partido(self):
        evento = {"strHomeTeam": "Greece", "strAwayTeam": "Netherlands", "dateEvent": "2026-09-24", "strTime": "18:45:00"}
        self.assertEqual(w._sportsdb_event_match_score(evento, "Greece", "Netherlands", "2026-10-01T18:45:00Z"), 0.0)
        evento["dateEvent"] = "2026-10-01"
        self.assertGreater(w._sportsdb_event_match_score(evento, "Greece", "Netherlands", "2026-10-01T18:45:00Z"), 0.0)


class HorarioTests(unittest.TestCase):
    def test_la_hora_del_boleto_manda(self):
        partido = {"kickoff": "2026-10-03T14:00:00Z", "weather_context": {"temperature_c": 20}}
        w._aplicar_horario_oficial(partido, {"kickoff": "2026-10-04T15:00:00Z"})
        self.assertEqual(partido["kickoff"], "2026-10-04T15:00:00Z")
        self.assertEqual(partido["kickoff_previo"], "2026-10-03T14:00:00Z")
        self.assertNotIn("weather_context", partido)

    def test_sin_hora_oficial_no_se_toca(self):
        partido = {"kickoff": "2026-10-03T14:00:00Z"}
        w._aplicar_horario_oficial(partido, {"kickoff": ""})
        self.assertEqual(partido["kickoff"], "2026-10-03T14:00:00Z")

    def test_emparejar_mira_la_fecha(self):
        viejo = {"local": "Greece", "visitante": "Netherlands", "kickoff": "2026-09-10T18:45:00Z"}
        with mock.patch.object(w, "_match_similarity_breakdown", return_value=(1.0, 1.0, 2.0)):
            self.assertIsNone(w._find_match_by_teams([viejo], "GRECIA", "PAÍSES BAJOS", "2026-10-01T18:45:00Z"))
            self.assertIs(w._find_match_by_teams([viejo], "GRECIA", "PAÍSES BAJOS", ""), viejo)


class NoticiasTests(unittest.TestCase):
    def test_titulares_ajenos(self):
        for titulo in (
            "La parrilla de salida del GP de San Marino de MotoGP 2026 en Misano",
            "Digi Spain cede un 8,04% en su salida a Bolsa y reduce su valor",
            "Islandia reanuda la caza de ballenas tras dos años de suspensión",
            "NK Croatia Bietigheim - Transfermarkt",
            "Sebastián Domínguez es el nuevo entrenador de Central Córdoba",
            "El Covirán Granada, ante una 'extraña' pretemporada",
        ):
            self.assertTrue(filtros_feed.motivo_titular_ajeno(titulo), titulo)
        self.assertEqual(filtros_feed.motivo_titular_ajeno("El Leganés hace oficial el fichaje de Álvaro Morata"), "")
        self.assertTrue(filtros_feed.motivo_titular_ajeno("Ángel Donato, nuevo entrenador del Girona B", team_name="GIRONA"))

    def test_transicion_de_seleccion_sin_mercado_de_clubes(self):
        item = {"title": "Barcelona confirms signing of Spain captain Rodri from Manchester City", "source": "lanacion.com.ar", "link": ""}
        self.assertFalse(w._passes_season_transition_quality(item, "Spain"))
        motogp = {"title": "La parrilla de salida del GP de San Marino de MotoGP 2026", "source": "Motorsport.com", "link": ""}
        self.assertFalse(w._passes_season_transition_quality(motogp, "San Marino"))


class BajasTests(unittest.TestCase):
    def setUp(self):
        # Sin red: las plantillas no se piden en los tests.
        self._p = [
            mock.patch.object(w, "_jugador_confirmado_del_equipo", return_value=False),
            mock.patch.object(w, "_es_jugador_de_otro_equipo", return_value=False),
        ]
        for p in self._p:
            p.start()

    def tearDown(self):
        for p in self._p:
            p.stop()

    def _bajas(self, equipo, titular):
        return [e["player_name"] for e in w._build_injury_entities(equipo, [{"title": titular, "source": "FotMob"}])]

    def test_no_son_bajas(self):
        self.assertEqual(self._bajas("San Marino", "Biglietteria: settore ospiti sold-out per San Marino-Albania"), [])
        self.assertEqual(self._bajas("Almería", "Cuatro bajas y dos regresos marcan la convocatoria del Almería ante el Mallorca"), [])
        self.assertEqual(self._bajas("Sporting Gijón", "Los daños colaterales para el Sporting de Gijón: Otra baja más"), [])
        self.assertEqual(self._bajas("Netherlands", "Klopp provides injury updates on Havertz and Musiala after Netherlands draw"), [])
        self.assertEqual(self._bajas("Norway", "Jesus has 'zero' fear of Norway despite Fernandes injury issue"), [])
        self.assertEqual(self._bajas("England", "Branthwaite hopes injury issues are behind him following England recall"), [])

    def test_bajas_reales(self):
        self.assertEqual(self._bajas("Netherlands", "Malen ruled out of Netherlands’ squad with injury"), ["Malen"])
        self.assertIn(
            "Lutsharel Geertruida",
            self._bajas("Netherlands", "PSV Dealt Another Injury Blow as Lutsharel Geertruida Leaves Netherlands Camp"),
        )

    def test_deduplicar(self):
        entidades = [{"player_name": "Havertz"}, {"player_name": "Kai Havertz"}, {"player_name": "Musiala"}]
        self.assertEqual([e["player_name"] for e in filtros_feed.deduplicar_por_apellido(entidades)], ["Havertz", "Musiala"])
        hermanos = [{"player_name": "Nico Williams"}, {"player_name": "Iñaki Williams"}]
        self.assertEqual(len(filtros_feed.deduplicar_por_apellido(hermanos)), 2)


if __name__ == "__main__":
    unittest.main()
