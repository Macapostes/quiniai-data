"""Barreras de calidad para los cruces europeos (y comunes a todas las ligas).

Auditoria de la J19 (Champions/Europa/Conference, 3-5 nov 2026). El contexto
que leia la IA traia:

- "arbitro Lamine Yamal": el nombre salio de un titular de la web "Soy
  Referee". Un arbitro solo vale si viene de una fuente estructurada.
- "jornada 12 de Champions" y "(5/7 jornadas disputadas)": la numeracion de
  TheSportsDB cuenta las previas. La fase liga tiene 8 jornadas.
- "Lille tiene Europa League vs Viktoria Plzen": era el Lillestrom. Un club
  solo juega una copa UEFA por temporada.
- "viaje 6960 km" (Union SG situado en EEUU), "7708 km" (Como).
- 23,6 C en San Sebastian en noviembre: la ciudad geolocalizada en otro
  continente.

Todo lo de aqui es puro (sin red) para poder probarlo.
"""
from __future__ import annotations

from datetime import datetime
from statistics import median

COPAS_UEFA_CLUBES = (
    "soccer_uefa_champs_league",
    "soccer_uefa_europa_league",
    "soccer_uefa_europa_conference_league",
)
NOMBRE_COPA_UEFA = {
    "soccer_uefa_champs_league": "UEFA Champions League",
    "soccer_uefa_europa_league": "UEFA Europa League",
    "soccer_uefa_europa_conference_league": "UEFA Conference League",
}
# Fase liga 2024+: 36 equipos. Champions y Europa League 8 jornadas; la
# Conference League solo 6 (UEFA, formato 2024-27).
JORNADAS_FASE_LIGA = 8
JORNADAS_POR_COPA = {"soccer_uefa_europa_conference_league": 6}
FORMATO_FASE_LIGA = "36 equipos, 8 jornadas: 1-8 octavos, 9-24 play-off, 25-36 eliminados"
FORMATO_POR_COPA = {
    "soccer_uefa_europa_conference_league": "36 equipos, 6 jornadas: 1-8 octavos, 9-24 play-off, 25-36 eliminados",
}


def jornadas_fase_liga(liga: str) -> int:
    return JORNADAS_POR_COPA.get(liga, JORNADAS_FASE_LIGA)

FUENTES_ARBITRO_FIABLES = {"rfef", "thesportsdb", "espn", "uefa"}

# Paises (ISO2) de federaciones UEFA. Los de los extremos pueden pasar de
# 3.500 km de viaje de verdad (Bakú, Almaty, Tel Aviv, Nicosia...).
PAISES_UEFA = set(
    "AD AL AM AT AZ BA BE BG BY CH CY CZ DE DK EE ES FI FO FR GB GE GI GR HR HU "
    "IE IL IS IT KZ LI LT LU LV MC MD ME MK MT NL NO PL PT RO RS RU SE SI SK SM "
    "TR UA XK".split()
)
PAISES_UEFA_EXTREMOS = {"AZ", "KZ", "IL", "CY", "AM", "GE", "TR", "IS", "RU"}
MAX_KM_VIAJE_EUROPA = 3500.0
MAX_KM_VIAJE_EXTREMOS = 6000.0

_NOMBRE_PAIS_A_ISO = {
    "spain": "ES", "espana": "ES", "england": "GB", "scotland": "GB", "wales": "GB",
    "united kingdom": "GB", "france": "FR", "germany": "DE", "italy": "IT",
    "portugal": "PT", "netherlands": "NL", "belgium": "BE", "norway": "NO",
    "sweden": "SE", "denmark": "DK", "czechia": "CZ", "czech republic": "CZ",
    "greece": "GR", "turkey": "TR", "turkiye": "TR", "türkiye": "TR",
    "austria": "AT", "switzerland": "CH", "poland": "PL", "croatia": "HR",
    "serbia": "RS", "ukraine": "UA", "scotland ": "GB", "ireland": "IE",
    "cyprus": "CY", "israel": "IL", "azerbaijan": "AZ", "kazakhstan": "KZ",
    "hungary": "HU", "romania": "RO", "bulgaria": "BG", "slovakia": "SK",
    "slovenia": "SI", "finland": "FI", "iceland": "IS", "moldova": "MD",
    "luxembourg": "LU", "lithuania": "LT", "latvia": "LV", "estonia": "EE",
    "belarus": "BY", "albania": "AL", "north macedonia": "MK", "montenegro": "ME",
    "bosnia and herzegovina": "BA", "kosovo": "XK", "georgia": "GE", "armenia": "AM",
    "malta": "MT", "faroe islands": "FO", "gibraltar": "GI", "andorra": "AD",
    "san marino": "SM", "liechtenstein": "LI", "northern ireland": "GB", "russia": "RU",
}


def es_copa_uefa(liga: object) -> bool:
    return str(liga or "").strip() in COPAS_UEFA_CLUBES


def iso_de_pais(valor: object) -> str:
    texto = str(valor or "").strip()
    if len(texto) == 2 and texto.isalpha():
        return texto.upper()
    return _NOMBRE_PAIS_A_ISO.get(texto.lower(), "")


# --- Pais de una competicion domestica (identidad estricta del equipo) -------

_PAIS_POR_TOKEN = [
    (("spain", "laliga", "la liga", "segunda", "copa del rey"), "ES"),
    (("epl", "england", "english", "premier league", "efl", "fa cup", "championship"), "GB"),
    (("scotland", "scottish"), "GB"),
    (("france", "french", "ligue 1", "ligue 2", "coupe de france"), "FR"),
    (("germany", "german", "bundesliga", "dfb"), "DE"),
    (("italy", "italian", "serie a", "coppa italia"), "IT"),
    (("portugal", "portuguese", "primeira", "liga portugal"), "PT"),
    (("netherlands", "dutch", "eredivisie"), "NL"),
    (("belgium", "belgian", "jupiler"), "BE"),
    (("norway", "norwegian", "eliteserien"), "NO"),
    (("sweden", "swedish", "allsvenskan"), "SE"),
    (("denmark", "danish", "superliga"), "DK"),
    (("turkey", "turkish", "super lig", "süper lig"), "TR"),
    (("greece", "greek", "super league greece"), "GR"),
    (("czech", "chance liga", "fortuna liga"), "CZ"),
    (("austria", "austrian"), "AT"),
    (("switzerland", "swiss"), "CH"),
    (("poland", "polish", "ekstraklasa"), "PL"),
    (("croatia", "croatian", "hnl"), "HR"),
    (("serbia", "serbian"), "RS"),
    (("ukraine", "ukrainian"), "UA"),
    (("cyprus", "cypriot"), "CY"),
    (("israel", "israeli"), "IL"),
]


def pais_de_competicion(clave: object, nombre: object) -> str:
    """ISO2 de una liga/copa domestica por su clave o nombre; "" si no se sabe
    o si es internacional (UEFA, amistosos)."""
    texto = f" {str(clave or '').lower().replace('_', ' ')} {str(nombre or '').lower()} "
    if any(t in texto for t in ("uefa", "champions", "europa", "conference", "friendl", "amistos", "world", "nations")):
        return ""
    for tokens, iso in _PAIS_POR_TOKEN:
        if any(f" {t} " in texto or f" {t}" in texto for t in tokens):
            return iso
    return ""


def proximo_de_otro_pais(clave: object, nombre: object, pais_equipo: object) -> bool:
    """J19: "Lille: casa vs Aalesund (Eliteserien)" era el Lillestrom. Un club
    no juega la liga de otro pais: identidad por pais, no por nombre parecido."""
    pais_equipo = iso_de_pais(pais_equipo)
    pais_comp = pais_de_competicion(clave, nombre)
    return bool(pais_equipo and pais_comp and pais_equipo != pais_comp)


# --- Arbitro -----------------------------------------------------------------

def _norm_nombre(texto: object) -> str:
    import unicodedata
    t = unicodedata.normalize("NFKD", str(texto or "")).encode("ascii", "ignore").decode().lower()
    return " ".join("".join(c if c.isalnum() else " " for c in t).split())


def arbitro_parece_titular(nombre: str) -> bool:
    """Un nombre de arbitro no tiene mas de 5 palabras ni signos de titular."""
    if len(nombre.split()) > 5:
        return True
    return any(ch in nombre for ch in ":;!?\"«»|") or len(nombre) > 45


def sanear_arbitro(referee_context: object, jugadores: object = None) -> bool:
    """Deja el arbitro solo si viene de una fuente estructurada, no es un titular
    y no es un jugador de ninguno de los dos equipos. True si cambia."""
    if not isinstance(referee_context, dict):
        return False
    nombre = str(referee_context.get("assigned_referee") or "").strip()
    fuente = str(referee_context.get("source") or "").strip().lower()
    if not nombre:
        return False
    plantilla = {_norm_nombre(j) for j in (jugadores or []) if _norm_nombre(j)}
    es_jugador = _norm_nombre(nombre) in plantilla
    if fuente in FUENTES_ARBITRO_FIABLES and not es_jugador and not arbitro_parece_titular(nombre):
        return False
    referee_context["unverified_referee"] = nombre
    referee_context["assigned_referee"] = ""
    referee_context["source"] = "news_unverified"
    return True


# --- Copa UEFA de cada club ----------------------------------------------------

def proximo_de_otra_copa_uefa(clave_proximo: str, liga_partido: str) -> bool:
    """En un cruce europeo, un 'proximo' de OTRA copa UEFA es de un homonimo."""
    return (
        es_copa_uefa(liga_partido)
        and es_copa_uefa(clave_proximo)
        and clave_proximo != liga_partido
    )


def proximo_ya_jugado(momento: datetime | None, kickoff: datetime | None) -> bool:
    return bool(momento and kickoff and momento < kickoff)


# --- Viaje ---------------------------------------------------------------------

def viaje_inverosimil_uefa(distancia_km: object, pais_local: object, pais_visitante: object) -> str:
    """Motivo para descartar la distancia de un cruce europeo; "" si vale."""
    try:
        km = float(distancia_km)
    except (TypeError, ValueError):
        return ""
    loc, vis = iso_de_pais(pais_local), iso_de_pais(pais_visitante)
    for codigo, nombre in ((loc, pais_local), (vis, pais_visitante)):
        if nombre and codigo and codigo not in PAISES_UEFA:
            return f"{nombre} no es un pais UEFA: coordenadas de otro equipo"
        if nombre and not codigo:
            # Pais que no reconocemos (p. ej. "United States"): no es UEFA.
            return f"{nombre} no es un pais UEFA: coordenadas de otro equipo"
    techo = MAX_KM_VIAJE_EXTREMOS if (loc in PAISES_UEFA_EXTREMOS or vis in PAISES_UEFA_EXTREMOS) else MAX_KM_VIAJE_EUROPA
    if km > techo:
        return f"{km:.0f} km es imposible entre {loc or '?'} y {vis or '?'} (maximo {techo:.0f} km)"
    return ""


# --- Meteo ---------------------------------------------------------------------

_ZONAS_EUROPEAS = ("Europe/", "Atlantic/Canary", "Atlantic/Madeira", "Atlantic/Azores",
                   "Atlantic/Reykjavik", "Atlantic/Faroe", "Asia/Nicosia", "Asia/Famagusta",
                   "Asia/Baku", "Asia/Almaty", "Asia/Tbilisi", "Asia/Yerevan", "Asia/Jerusalem",
                   "Asia/Tel_Aviv", "Asia/Qostanay", "Asia/Aqtobe", "Asia/Oral")


def meteo_inverosimil(meteo: object, pais_local: object, mes: int | None) -> str:
    """Motivo para descartar la meteo de un partido en Europa; "" si vale."""
    if not isinstance(meteo, dict) or not meteo:
        return ""
    codigo = iso_de_pais(pais_local)
    if codigo and codigo not in PAISES_UEFA:
        return ""
    zona = str(meteo.get("timezone") or "").strip()
    if zona and zona.upper() not in {"GMT", "UTC"} and not zona.startswith(_ZONAS_EUROPEAS):
        return f"prevision de {zona}: la sede se geolocalizo fuera de Europa"
    try:
        temp = float(meteo.get("temperature_c"))
    except (TypeError, ValueError):
        return ""
    if mes in {11, 12, 1, 2, 3} and temp > 22 and codigo not in {"CY", "IL"}:
        return f"{temp:.1f} C en Europa en el mes {mes}: prevision de otra ciudad"
    return ""


# --- Fase liga -------------------------------------------------------------------

def fase_liga_desde_tabla(tabla: dict, fila_local: dict, fila_visitante: dict, liga: str, ya_empezado: bool = False) -> dict:
    """Bloque `european_matchday` a partir de la clasificacion de ESPN."""
    jugados = [int(f.get("played") or 0) for f in (tabla or {}).values() if isinstance(f, dict)]
    if not jugados:
        return {}
    total = jornadas_fase_liga(liga)
    # La jornada sale del propio partido: lo que llevan jugado SUS dos equipos.
    # La mediana de la tabla mezclaba los resultados del martes con los
    # partidos del miercoles (J19: "jornada 5" con 3 jugados).
    propios = [int(f.get("played") or 0) for f in (fila_local, fila_visitante)
               if isinstance(f, dict) and f.get("played") is not None]
    if propios:
        previos = min(propios) - 1 if ya_empezado else max(propios)
        previos = max(0, previos)
    else:
        previos = int(round(median(jugados)))
        previos = max(0, previos - 1) if ya_empezado else previos
    jornada = max(1, min(total, previos + 1))
    disputadas = jornada - 1
    # Lo que refleja la fila de la tabla (si el partido ya se jugo, lo incluye).
    en_tabla = min(propios) if propios else disputadas
    nombre = NOMBRE_COPA_UEFA.get(liga, "competicion europea")
    partes = []
    for etiqueta, fila in (("local", fila_local), ("visitante", fila_visitante)):
        if isinstance(fila, dict) and fila.get("position"):
            partes.append(
                f"{etiqueta} {fila.get('team')} {fila.get('position')}º con {fila.get('points')} pts "
                f"en {fila.get('played')} partidos"
                + (f" (goles {fila.get('goals_for')}-{fila.get('goals_against')})"
                   if fila.get('played') and fila.get('goals_for') is not None and fila.get('goals_against') is not None else "")
            )
    if en_tabla <= 0:
        guia = "Primera jornada: aun no hay clasificacion."
    else:
        guia = (
            f"Clasificacion tras {en_tabla} jornada{'s' if en_tabla != 1 else ''}: "
            + ("; ".join(partes) if partes else "sin fila de los equipos")
            + ". Quedan " + str(total - en_tabla) + " jornadas; los puestos aun se mueven mucho."
        )
    return {
        "active": True,
        "competition": nombre,
        "matchday": jornada,
        "matchdays_played": disputadas,
        "format": FORMATO_POR_COPA.get(liga, FORMATO_FASE_LIGA),
        "total_matchdays": total,
        "guidance": guia,
        "source": "espn-standings",
    }


# --- Jugadoras -----------------------------------------------------------------

def titular_nombra_jugadora(titulo: str, jugadoras: set[str], normalizar) -> bool:
    """True si el titular nombra (nombre completo) a una jugadora del club."""
    if not titulo or not jugadoras:
        return False
    texto = f" {normalizar(titulo)} "
    return any(f" {j} " in texto for j in jugadoras if j and " " in j)
