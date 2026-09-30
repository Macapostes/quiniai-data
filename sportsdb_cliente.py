"""Cliente unico para TheSportsDB: ritmo, memoria por pasada y cache en disco.

Por que existe
--------------
La clave gratuita ("123") admite 30 peticiones por minuto POR IP
(https://www.thesportsdb.com/docs_api_guide); pasado eso responde 429 y hay que
esperar un minuto. El 30-sep-2026 la primera pasada en el PC de Windows tardo
42 minutos: cientos de 429 en searchteams/lookuptable/eventslast y la misma
consulta ("Alaves", "Moldova", lookuptable 4490 con dos temporadas...) repetida
decenas de veces en la misma pasada, porque cada funcion pedia por su cuenta,
sin ritmo comun y sin acordarse de lo que ya habia fallado.

Todas las llamadas a TheSportsDB pasan por aqui (via `_request_json`), asi que
aqui se garantiza para todas a la vez:

* Ritmo global y compartido entre hilos: como mucho una peticion cada
  `intervalo` segundos (2,2 s por defecto = ~27/min, por debajo de 30/min).
* Memoria por pasada: la misma URL se pide UNA vez por pasada. Tambien los
  fallos: un 429 o una respuesta vacia no se vuelven a pedir en la misma pasada;
  se relanza el mismo error y cada llamador sirve su copia anterior como hacia.
* Cache en disco con caducidad por endpoint, reutilizada entre pasadas. Las
  caducidades son iguales o mas cortas que las de los llamadores, asi que el
  dato nunca llega mas viejo que antes.
* 429: respeta Retry-After (con tope), si no espera 20 s y luego 40 s, y tras
  `umbral_circuito` 429 seguidos abre el circuito: el resto de la pasada no se
  llama al proveedor y los llamadores sirven su cache.
* Un cuerpo que no es JSON (HTML de limite, vacio) es un fallo blando: no se
  reintenta, no se guarda en disco y cuenta en el resumen.

Solo usa la biblioteca estandar y `requests`.
"""

from __future__ import annotations

import copy
import email.utils
import json
import os
import threading
import time
import urllib.parse
from datetime import timezone
from pathlib import Path
from typing import Callable

import requests


# Caducidad de la copia en disco por endpoint. Nunca mas larga que la de la
# cache propia del llamador (tabla/racha/temporada: 24 h; proximos: 3-6 h;
# ronda: 12 h; ficha de equipo: 24 h fresca / 13 dias maxima; plantilla: 7 d),
# asi que no puede empeorar la frescura: solo evita repetir lo que el llamador
# no guarda (respuestas vacias, fallos) y lo que piden varios llamadores.
HORA = 3600
TTL_POR_ENDPOINT = {
    "searchteams.php": 3 * 24 * HORA,  # identidad del equipo: id, liga, estadio
    "lookup_all_players.php": 3 * 24 * HORA,
    "searchevents.php": 12 * HORA,  # H2H: partidos ya jugados
    "lookuptable.php": 3 * HORA,
    "eventslast.php": 3 * HORA,
    "eventsseason.php": 3 * HORA,
    "eventsround.php": 3 * HORA,
    "eventsnext.php": 2 * HORA,
}
# Una respuesta valida pero vacia ("teams": null) se guarda menos tiempo: si
# el proveedor la dio por un mal momento, a las pocas horas se vuelve a pedir.
# Salvo donde vacio es la respuesta normal y estable: la mayoria de cruces H2H
# por nombre no existen (8 consultas por partido, casi todas vacias, que antes
# se repetian en cada pasada) y un nombre de equipo que no aparece hoy tampoco
# aparece esta tarde.
TTL_MAXIMA_VACIA = 3 * HORA
TTL_VACIA_POR_ENDPOINT = {
    "searchevents.php": 12 * HORA,
    "searchteams.php": 24 * HORA,
}


class SportsDBError(requests.exceptions.RequestException):
    """Fallo de TheSportsDB. `motivo`: 429, http, nojson, red o circuito.

    `repetido` es True cuando se relanza desde la memoria de la pasada: el
    llamador ya lo aviso la primera vez y no hace falta volver a imprimirlo.
    """

    def __init__(self, mensaje: str, motivo: str, estado: int | None = None, repetido: bool = False):
        super().__init__(mensaje)
        self.motivo = motivo
        self.estado = estado
        self.repetido = repetido

    def repetir(self) -> "SportsDBError":
        return type(self)(str(self), self.motivo, self.estado, repetido=True)


class SportsDBHTTPError(SportsDBError, requests.exceptions.HTTPError):
    """429, 4xx o 5xx. Sigue siendo un HTTPError para quien lo capture asi."""


class SportsDBNoJSON(SportsDBError, ValueError):
    """Respondio 200 pero sin JSON: vacio o una pagina HTML."""


class SportsDBCircuitoAbierto(SportsDBError):
    """El proveedor nos esta limitando: no se le llama mas en esta pasada."""


class _Entrada:
    __slots__ = ("listo", "datos", "error")

    def __init__(self) -> None:
        self.listo = threading.Event()
        self.datos = None
        self.error: SportsDBError | None = None


def _es_vacia(datos) -> bool:
    if not datos:
        return True
    if isinstance(datos, dict):
        return all(not valor for valor in datos.values())
    return False


def _endpoint(url: str) -> str:
    return urllib.parse.urlsplit(url).path.rsplit("/", 1)[-1]


class ClienteSportsDB:
    def __init__(
        self,
        *,
        intervalo: float = 2.2,
        ruta_cache: Path | str | None = None,
        cabeceras: dict | None = None,
        max_reintentos_429: int = 2,
        espera_base_429: float = 20.0,
        espera_max_429: float = 60.0,
        umbral_circuito: int = 3,
        ttl_por_endpoint: dict | None = None,
        http_get: Callable | None = None,
        dormir: Callable[[float], None] | None = None,
        reloj: Callable[[], float] | None = None,
        ahora: Callable[[], float] | None = None,
        al_fallar: Callable[[], None] | None = None,
        avisar: Callable[[str], None] | None = None,
    ) -> None:
        self.intervalo = max(0.0, float(intervalo))
        self.ruta_cache = Path(ruta_cache) if ruta_cache else None
        self.cabeceras = dict(cabeceras or {})
        self.max_reintentos_429 = max(0, int(max_reintentos_429))
        self.espera_base_429 = float(espera_base_429)
        self.espera_max_429 = float(espera_max_429)
        self.umbral_circuito = max(1, int(umbral_circuito))
        self.ttl_por_endpoint = dict(TTL_POR_ENDPOINT if ttl_por_endpoint is None else ttl_por_endpoint)
        # requests.get se busca en cada llamada para que los tests que lo
        # parchean sigan funcionando.
        self._http_get = http_get or (lambda *a, **k: requests.get(*a, **k))
        self.dormir = dormir or time.sleep
        self.reloj = reloj or time.monotonic
        self.ahora = ahora or time.time
        self.al_fallar = al_fallar or (lambda: None)
        self.avisar = avisar or print
        self._lock = threading.Lock()
        self._lock_ritmo = threading.Lock()
        self._proximo_turno = 0.0
        self._disco: dict | None = None
        self._disco_sucio = False
        self.nueva_pasada()

    # ------------------------------------------------------------------ pasada
    def nueva_pasada(self) -> None:
        """El estado del proveedor no se hereda: memoria, circuito y contadores."""
        with self._lock:
            self._memo: dict[str, _Entrada] = {}
            self._seguidos_429 = 0
            self.circuito_abierto = False
            self.stats = {
                "llamadas": 0,
                "red": 0,
                "memo": 0,
                "disco": 0,
                "e429": 0,
                "reintentos": 0,
                "nojson": 0,
                "errores": 0,
                "saltadas_circuito": 0,
                "espera_segundos": 0.0,
            }

    def resumen(self) -> str:
        s = self.stats
        cache = s["memo"] + s["disco"]
        return (
            f"[sportsdb] {s['llamadas']} consultas: {s['red']} a la red, "
            f"{cache} desde cache (pasada {s['memo']}, disco {s['disco']}), "
            f"{s['e429']} bloqueos 429, {s['nojson']} sin JSON, {s['errores']} errores, "
            f"circuito {'ABIERTO' if self.circuito_abierto else 'cerrado'}"
            + (f" ({s['saltadas_circuito']} saltadas)" if s["saltadas_circuito"] else "")
            + f", {s['espera_segundos']:.0f} s de espera"
        )

    def _sumar(self, campo: str, cantidad: float = 1) -> None:
        with self._lock:
            self.stats[campo] += cantidad

    # ------------------------------------------------------------------- ritmo
    def _esperar_turno(self) -> None:
        with self._lock_ritmo:
            ahora = self.reloj()
            turno = max(ahora, self._proximo_turno)
            self._proximo_turno = turno + self.intervalo
        espera = turno - ahora
        if espera > 0:
            self._sumar("espera_segundos", espera)
            self.dormir(espera)

    def _aplazar_todos(self, segundos: float) -> None:
        """Tras un 429 esperan TODOS los hilos, no solo el que lo recibio."""
        with self._lock_ritmo:
            self._proximo_turno = max(self._proximo_turno, self.reloj() + segundos)

    def _espera_429(self, respuesta, intento: int) -> float:
        cabeceras = getattr(respuesta, "headers", None) or {}
        valor = cabeceras.get("Retry-After") if hasattr(cabeceras, "get") else None
        segundos = None
        if valor not in (None, ""):
            try:
                segundos = float(str(valor).strip())
            except ValueError:
                try:
                    fecha = email.utils.parsedate_to_datetime(str(valor))
                    if fecha.tzinfo is None:
                        fecha = fecha.replace(tzinfo=timezone.utc)
                    segundos = fecha.timestamp() - self.ahora()
                except Exception:
                    segundos = None
        if segundos is None:
            segundos = self.espera_base_429 * (2 ** intento)
        return max(1.0, min(self.espera_max_429, segundos))

    # ------------------------------------------------------------------- disco
    def _clave(self, url: str, params: dict | None) -> str:
        # Sin la clave de API: la copia vale igual con otra clave y no se
        # escribe en disco.
        consulta = urllib.parse.urlencode(sorted((str(k), str(v)) for k, v in (params or {}).items()))
        return f"{_endpoint(url)}?{consulta}"

    def _cargar_disco(self) -> dict:
        if self._disco is None:
            datos = {}
            if self.ruta_cache and self.ruta_cache.exists():
                try:
                    datos = json.loads(self.ruta_cache.read_text(encoding="utf-8")) or {}
                except Exception:
                    datos = {}
            self._disco = datos if isinstance(datos, dict) else {}
        return self._disco

    def _ttl_de(self, clave: str, entrada: dict) -> float:
        ttl = float(self.ttl_por_endpoint.get(clave.split("?", 1)[0], 0) or 0)
        if entrada.get("vacia"):
            endpoint = clave.split("?", 1)[0]
            ttl = min(ttl, TTL_VACIA_POR_ENDPOINT.get(endpoint, TTL_MAXIMA_VACIA))
        return ttl

    def _disco_get(self, clave: str):
        with self._lock:
            entrada = self._cargar_disco().get(clave)
            if not isinstance(entrada, dict):
                return False, None
            edad = self.ahora() - float(entrada.get("t") or 0)
            if edad < 0 or edad > self._ttl_de(clave, entrada):
                return False, None
            return True, entrada.get("data")

    def _disco_set(self, clave: str, datos) -> None:
        if not self.ttl_por_endpoint.get(clave.split("?", 1)[0]):
            return
        with self._lock:
            self._cargar_disco()[clave] = {"t": self.ahora(), "vacia": _es_vacia(datos), "data": datos}
            self._disco_sucio = True

    def guardar(self) -> None:
        """Poda lo caducado y escribe la cache en disco (escritura atomica)."""
        if not self.ruta_cache:
            return
        with self._lock:
            if self._disco is None:
                return
            ahora = self.ahora()
            for clave, entrada in list(self._disco.items()):
                if not isinstance(entrada, dict) or ahora - float(entrada.get("t") or 0) > self._ttl_de(clave, entrada):
                    self._disco.pop(clave, None)
                    self._disco_sucio = True
            if not self._disco_sucio:
                return
            texto = json.dumps(self._disco, ensure_ascii=False, separators=(",", ":"))
            self._disco_sucio = False
        self.ruta_cache.parent.mkdir(parents=True, exist_ok=True)
        temporal = self.ruta_cache.with_name(self.ruta_cache.name + ".tmp")
        temporal.write_text(texto, encoding="utf-8")
        os.replace(temporal, self.ruta_cache)

    # ------------------------------------------------------------------ peticion
    def get_json(self, url: str, params: dict | None = None, timeout: float = 30, sin_red: Callable[[], None] | None = None):
        """Como `_request_json`, pero educado. `sin_red` se llama cuando la
        respuesta sale de la memoria, del disco o del circuito abierto (para
        devolver el cupo que el llamador ya habia apartado)."""
        clave = self._clave(url, params)
        with self._lock:
            self.stats["llamadas"] += 1
            entrada = self._memo.get(clave)
            propietario = entrada is None
            if propietario:
                entrada = _Entrada()
                self._memo[clave] = entrada
        if not propietario:
            entrada.listo.wait()
            self._sumar("memo")
            if sin_red:
                sin_red()
            if entrada.error is not None:
                raise entrada.error.repetir()
            if entrada.datos is None:
                raise SportsDBError(f"sin respuesta para {clave}", "red", repetido=True)
            return copy.deepcopy(entrada.datos)
        try:
            datos = self._resolver(url, params, timeout, clave, sin_red)
            entrada.datos = datos
            return copy.deepcopy(datos)
        except SportsDBError as exc:
            entrada.error = exc
            raise
        except Exception as exc:  # timeout, conexion...
            self._sumar("errores")
            envuelto = SportsDBError(f"{type(exc).__name__}: {exc}", "red")
            entrada.error = envuelto
            raise envuelto from exc
        finally:
            entrada.listo.set()

    def _resolver(self, url, params, timeout, clave, sin_red):
        hay, datos = self._disco_get(clave)
        if hay:
            self._sumar("disco")
            if sin_red:
                sin_red()
            return datos
        if self.circuito_abierto:
            self._sumar("saltadas_circuito")
            if sin_red:
                sin_red()
            # Cuenta como fallo del proveedor para la auditoria de contexto:
            # no hay dato porque el proveedor no nos atiende, no porque no exista.
            self.al_fallar()
            raise SportsDBCircuitoAbierto(
                f"circuito abierto: TheSportsDB nos limita, {_endpoint(url)} no se pide en esta pasada",
                "circuito",
            )
        respuesta = None
        estado = 0
        for intento in range(self.max_reintentos_429 + 1):
            self._esperar_turno()
            self._sumar("red")
            respuesta = self._http_get(url, params=params, headers=self.cabeceras, timeout=timeout)
            estado = int(getattr(respuesta, "status_code", 0) or 0)
            if estado != 429:
                with self._lock:
                    self._seguidos_429 = 0
                break
            self._sumar("e429")
            with self._lock:
                self._seguidos_429 += 1
                abrir = self._seguidos_429 >= self.umbral_circuito and not self.circuito_abierto
                if abrir:
                    self.circuito_abierto = True
            if abrir:
                self.avisar(
                    f"[sportsdb] {self._seguidos_429} respuestas 429 seguidas: circuito abierto, "
                    "el resto de la pasada se sirve de la cache"
                )
            if self.circuito_abierto or intento >= self.max_reintentos_429:
                break
            espera = self._espera_429(respuesta, intento)
            self._sumar("reintentos")
            self._aplazar_todos(espera)
        if estado == 429 or estado >= 500:
            self.al_fallar()
        if estado >= 400:
            if estado != 429:
                self._sumar("errores")
            texto = "429 Too Many Requests" if estado == 429 else f"{estado} Error"
            raise SportsDBHTTPError(f"{texto} en {_endpoint(url)}", "429" if estado == 429 else "http", estado)
        if not getattr(respuesta, "encoding", None) or str(respuesta.encoding).lower() in {"iso-8859-1", "latin-1"}:
            try:
                respuesta.encoding = "utf-8"
            except Exception:
                pass
        cuerpo = getattr(respuesta, "text", "") or ""
        try:
            datos = json.loads(cuerpo)
        except ValueError:
            self._sumar("nojson")
            raise SportsDBNoJSON(
                f"respuesta sin JSON en {_endpoint(url)} ({len(cuerpo)} bytes)", "nojson", estado
            ) from None
        self._disco_set(clave, datos)
        return datos
