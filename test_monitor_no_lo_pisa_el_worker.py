"""La pagina del monitor ya se borro sola dos veces. Que no haya una tercera.

`_build_monitor_web_html()` regenera `docs/monitor/index.html` entera en cada
ciclo. Mientras el worker fue el dueno de ese fichero, cualquier cosa anadida a
mano -las pestanas, el panel de admin- duraba hasta el siguiente ciclo y
desaparecia sin que nadie tocara nada. Y como el worker es un proceso largo,
ademas se llevaba en memoria la plantilla vieja.

La solucion fue quitarle la propiedad del fichero. Esto lo vigila.
"""

import os
import re
import unittest

RAIZ = os.path.dirname(os.path.abspath(__file__))
PAGINA = os.path.join(RAIZ, "docs", "monitor", "index.html")


def _worker() -> str:
    with open(os.path.join(RAIZ, "snapshot_worker.py"), encoding="utf-8") as fh:
        return fh.read()


def _pagina() -> str:
    with open(PAGINA, encoding="utf-8") as fh:
        return fh.read()


class ElWorkerNoEsDuenoDeLaPaginaTests(unittest.TestCase):
    def test_publicar_el_index_viene_apagado(self):
        fuente = _worker()
        self.assertIn('os.getenv("QUINIAI_MONITOR_PUBLISH_INDEX", "0")', fuente)

    def test_toda_escritura_del_index_esta_detras_del_interruptor(self):
        """Si alguien anade un `write` suelto, el fichero vuelve a tener dos
        duenos y la pagina se pisa igual que antes."""
        fuente = _worker().splitlines()
        for n, linea in enumerate(fuente, 1):
            if "monitor" in linea and "index.html" in linea and "write" in linea.lower():
                contexto = "\n".join(fuente[max(0, n - 12):n])
                self.assertIn(
                    "MONITOR_PUBLISH_INDEX", contexto,
                    f"linea {n}: escribe el index sin comprobar el interruptor",
                )


class LaPaginaPublicadaSigueEnteraTests(unittest.TestCase):
    def test_existe(self):
        self.assertTrue(os.path.exists(PAGINA), "falta docs/monitor/index.html")

    def test_conserva_el_panel_de_admin(self):
        """Las dos veces que se rompio, el sintoma fue este: volvia la pagina
        de siempre y la pestana nueva ya no estaba."""
        html = _pagina()
        self.assertIn("<iframe", html)
        self.assertIn("/admin/panel", html)

    def test_no_lleva_marcas_de_conflicto(self):
        """Una vez se subieron con el fichero, y el navegador reventaba con un
        SyntaxError: las pestanas se pintaban pero no respondian."""
        html = _pagina()
        for marca in ("<<<<<<<", "=======" + "=", ">>>>>>>"):
            self.assertNotIn(marca, html, f"quedo un {marca!r} dentro")

    def test_la_plantilla_del_worker_no_la_contradice(self):
        """Aunque hoy no escriba, la plantilla que lleva dentro tiene que seguir
        siendo la buena: el dia que alguien encienda el interruptor para
        depurar, que no publique la version sin panel."""
        fuente = _worker()
        plantilla = fuente[fuente.index("def _build_monitor_web_html"):]
        plantilla = plantilla[:plantilla.index("\ndef ", 10)]
        self.assertIn("/admin/panel", plantilla)
        self.assertRegex(plantilla, re.compile(r"<iframe", re.I))


if __name__ == "__main__":
    unittest.main()
