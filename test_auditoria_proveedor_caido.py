"""Una averia de TheSportsDB no puede dejar la jornada sin publicar.

El 6 de septiembre de 2026 el worker estuvo horas sin actualizar. La causa: la
clave publica "123" de TheSportsDB devolvia 429 a todo, el Bayern (partido 12 de
la quiniela de Champions) se quedaba sin contexto de plantilla, y la auditoria
rechazaba el snapshot ENTERO por ese unico lado vacio.

La distincion que faltaba: una evidencia contaminada es un dato malo y tiene que
frenar siempre; un lado sin contexto porque el proveedor esta caido es una
averia ajena, y frenar por ella deja a todos sin jornada.
"""

import os
import unittest

os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("ANTHROPIC_API_KEY", "test")

import snapshot_worker as sw  # noqa: E402


def _partido(local="B.MUNICH", visitante="Bodo Glimt", home_ctx=None, away_ctx=None):
    """Un partido del boleto; sin contexto en un lado salvo que se le pase."""
    return {
        "local": local,
        "visitante": visitante,
        "competition_context": {
            "season_transition": {
                "home": home_ctx or {},
                "away": away_ctx or {"previous_season": {"summary": "8o el ano pasado"}},
            }
        },
    }


def _con_contexto(resumen="3o el ano pasado"):
    return {"previous_season": {"summary": resumen}}


class BaseAuditoria(unittest.TestCase):
    def setUp(self):
        sw._reiniciar_fallos_sportsdb()

    def tearDown(self):
        sw._reiniciar_fallos_sportsdb()

    def auditar(self, partidos):
        return sw._audit_season_transition_snapshot({"quiniela_focus_matches": partidos})


class ProveedorCaidoTests(BaseAuditoria):
    def test_el_caso_del_6_de_septiembre_ya_no_frena(self):
        """El escenario exacto que dejo el worker parado horas."""
        for _ in range(sw.SPORTSDB_FALLOS_PARA_DEGRADADO):
            sw._marcar_fallo_sportsdb()
        audit = self.auditar([_partido()])
        self.assertTrue(audit["ok"], "la jornada tiene que publicarse igualmente")
        self.assertTrue(audit["degraded"])
        self.assertEqual(audit["empty_side_count"], 1)

    def test_publicar_degradado_deja_constancia(self):
        """Si no se dice por que va a medias, nadie se entera de que hay averia."""
        for _ in range(sw.SPORTSDB_FALLOS_PARA_DEGRADADO):
            sw._marcar_fallo_sportsdb()
        audit = self.auditar([_partido()])
        self.assertIn("TheSportsDB", audit["degraded_reason"])
        self.assertGreaterEqual(audit["provider_failures"], sw.SPORTSDB_FALLOS_PARA_DEGRADADO)

    def test_un_fallo_suelto_no_es_una_averia(self):
        """Un 429 aislado es ruido normal; si eso bastara para tolerar huecos,
        la auditoria no protegeria de nada."""
        sw._marcar_fallo_sportsdb()
        audit = self.auditar([_partido()])
        self.assertFalse(audit["ok"])
        self.assertFalse(audit["degraded"])


class ProveedorSanoTests(BaseAuditoria):
    def test_sin_averia_un_lado_vacio_sigue_frenando(self):
        """Esta es la garantia original y no se toca: si el proveedor responde y
        aun asi falta contexto, el dato es nuestro y esta mal."""
        audit = self.auditar([_partido()])
        self.assertFalse(audit["ok"])
        self.assertFalse(audit["degraded"])

    def test_con_contexto_en_los_dos_lados_pasa(self):
        audit = self.auditar([_partido(home_ctx=_con_contexto())])
        self.assertTrue(audit["ok"])
        self.assertFalse(audit["degraded"])

    def test_sin_partidos_no_se_publica(self):
        """Un snapshot sin jornada nunca es valido, haya averia o no."""
        for _ in range(sw.SPORTSDB_FALLOS_PARA_DEGRADADO * 3):
            sw._marcar_fallo_sportsdb()
        self.assertFalse(self.auditar([])["ok"])


class EvidenciaContaminadaTests(BaseAuditoria):
    def test_la_evidencia_cruzada_frena_aunque_el_proveedor_este_caido(self):
        """Lo mas importante del arreglo: la tolerancia es SOLO para huecos.

        Publicar contexto del equipo de al lado es peor que no publicar, y una
        averia del proveedor no lo vuelve aceptable.
        """
        for _ in range(sw.SPORTSDB_FALLOS_PARA_DEGRADADO * 5):
            sw._marcar_fallo_sportsdb()
        partido = _partido(
            home_ctx={
                "previous_season": {"summary": "3o el ano pasado"},
                "all_evidence": [{"title": "El Bodo Glimt ficha a un delantero"}],
            }
        )
        audit = self.auditar([partido])
        self.assertGreater(audit["invalid_evidence_count"], 0)
        self.assertFalse(audit["ok"], "una evidencia del rival nunca puede publicarse")


class ContadorTests(unittest.TestCase):
    def test_el_contador_se_reinicia_entre_ciclos(self):
        """Sin reinicio, la averia de un ciclo tolera huecos en todos los
        siguientes aunque el proveedor ya haya vuelto."""
        for _ in range(sw.SPORTSDB_FALLOS_PARA_DEGRADADO * 2):
            sw._marcar_fallo_sportsdb()
        self.assertTrue(sw._sportsdb_degradado())
        sw._reiniciar_fallos_sportsdb()
        self.assertFalse(sw._sportsdb_degradado())

    def test_run_once_reinicia_el_contador(self):
        import inspect
        fuente = inspect.getsource(sw.run_once)
        self.assertIn("_reiniciar_fallos_sportsdb()", fuente)
        self.assertLess(
            fuente.index("_reiniciar_fallos_sportsdb()"),
            fuente.index("fetch_snapshot()"),
            "hay que reiniciar ANTES de recoger datos, no despues",
        )


class QueCuentaComoAveriaTests(unittest.TestCase):
    """Se comprueba el comportamiento, no el texto del codigo.

    La primera version de este test buscaba "404" en el fuente y lo encontraba
    en un comentario: pasaba y fallaba por motivos que no eran el que importa.
    """

    def setUp(self):
        sw._reiniciar_fallos_sportsdb()

    def tearDown(self):
        sw._reiniciar_fallos_sportsdb()

    def _pedir(self, url, codigo):
        import requests
        from unittest.mock import patch

        class RespuestaFalsa:
            status_code = codigo
            encoding = "utf-8"
            text = "{}"

            def raise_for_status(self):
                if self.status_code >= 400:
                    raise requests.exceptions.HTTPError(f"{self.status_code}")

        with patch.object(requests, "get", return_value=RespuestaFalsa()):
            try:
                sw._request_json(url)
            except requests.exceptions.HTTPError:
                pass
        return sw._SPORTSDB_FALLOS_CICLO

    def test_el_429_del_proveedor_cuenta(self):
        self.assertEqual(self._pedir(sw.THESPORTSDB_SEARCH_TEAM_URL, 429), 1)

    def test_los_5xx_del_proveedor_cuentan(self):
        self.assertEqual(self._pedir(sw.THESPORTSDB_SEARCH_TEAM_URL, 503), 1)

    def test_el_404_del_proveedor_no_cuenta(self):
        """404 es "ese equipo no existe", no una averia. Si contara, un nombre
        mal escrito bastaria para relajar la auditoria."""
        self.assertEqual(self._pedir(sw.THESPORTSDB_SEARCH_TEAM_URL, 404), 0)

    def test_el_429_de_otro_sitio_no_cuenta(self):
        """El worker llama a mas sitios; que se caiga otro no justifica publicar
        la quiniela sin contexto de plantillas."""
        self.assertEqual(self._pedir("https://feeds.bbci.co.uk/algo.rss", 429), 0)

    def test_una_respuesta_buena_no_cuenta(self):
        self.assertEqual(self._pedir(sw.THESPORTSDB_SEARCH_TEAM_URL, 200), 0)


class SalidaDeTextoTests(unittest.TestCase):
    """El worker murio el 2 de septiembre imprimiendo "Jagiellonia Bialystok"."""

    def test_stdout_y_stderr_aguantan_cualquier_caracter(self):
        import sys
        for flujo, nombre in ((sys.stdout, "stdout"), (sys.stderr, "stderr")):
            enc = (getattr(flujo, "encoding", "") or "").lower().replace("-", "")
            if enc:
                self.assertEqual(enc, "utf8", f"{nombre} deberia ir en UTF-8")


if __name__ == "__main__":
    unittest.main()


class PublicacionSinCredencialTests(unittest.TestCase):
    """Sin credencial no hay averia, hay una via sin configurar.

    Escribir por la API de GitHub exige autenticacion: sin ella devuelve 404
    siempre. Antes eso soltaba un WARNING en cada ciclo y parecia que algo iba
    mal, cuando publicar por git push es una via de pleno derecho.

    Ojo con el matiz que se me escapo al principio: la credencial no sale solo
    de QUINIAI_GITHUB_TOKEN, tambien puede venir del gestor de credenciales de
    git. Por eso se mira la cabecera Authorization ya construida, y no la
    variable de entorno: mirar la variable desactivaba una via que si funciona.
    """

    def setUp(self):
        self._disabled = sw.MONITOR_GITHUB_API_DISABLED
        sw.MONITOR_GITHUB_API_DISABLED = False

    def tearDown(self):
        sw.MONITOR_GITHUB_API_DISABLED = self._disabled

    def _sin_credencial(self):
        from unittest.mock import patch
        return patch.object(
            sw, "_monitor_github_headers", return_value={"Accept": "application/vnd.github+json"}
        )

    def test_sin_credencial_ni_se_intenta_la_api(self):
        """Gastar peticiones que solo pueden devolver 404 es ruido puro."""
        from unittest.mock import patch
        import requests

        with self._sin_credencial(), patch.object(requests, "get") as get, patch.object(
            requests, "patch"
        ) as patch_req:
            resultado = sw._github_monitor_upsert_many([("docs/monitor/x.json", "{}")])
        self.assertFalse(resultado)
        get.assert_not_called()
        patch_req.assert_not_called()

    def test_se_avisa_una_sola_vez(self):
        with self._sin_credencial():
            sw._github_monitor_upsert_many([("docs/monitor/x.json", "{}")])
        self.assertTrue(
            sw.MONITOR_GITHUB_API_DISABLED,
            "hay que marcarlo para no repetir el aviso en cada ciclo",
        )

    def test_con_credencial_del_gestor_de_git_si_se_intenta(self):
        """El fallo que casi cuelo: si la credencial viene del gestor de git y
        no del .env, la API sigue siendo valida y no hay que desactivarla."""
        from unittest.mock import patch, Mock
        import requests

        respuesta = Mock(status_code=404)
        with patch.object(
            sw, "_monitor_github_headers", return_value={"Authorization": "Basic x"}
        ), patch.object(requests, "get", return_value=respuesta) as get:
            sw._github_monitor_upsert_many([("docs/monitor/y.json", "{}")])
        get.assert_called()

    def test_el_aviso_sin_credencial_no_es_un_warning(self):
        """Un WARNING recurrente por algo normal tapa los problemas de verdad."""
        import inspect
        fuente = inspect.getsource(sw._github_monitor_upsert_many)
        trozo = fuente[: fuente.index("ref_path")]
        self.assertIn("LOGGER.info", trozo)
        self.assertNotIn("LOGGER.warning", trozo)
