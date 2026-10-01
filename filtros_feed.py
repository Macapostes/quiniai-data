"""Filtros de titulares que faltaban tras la jornada 10/11 de 2026-27.

Lo que se colo en el feed publicado:
- noticias que no son de futbol pero comparten nombre con el equipo: MotoGP
  ("parrilla de SALIDA del GP de San Marino"), bolsa ("SALIDA a bolsa de Digi
  Spain"), la caza de ballenas en Islandia, la sucesion al trono de
  Liechtenstein, futbol sala y baloncesto del mismo club...
- homonimos y categorias inferiores: NK Croatia, Victor San Marino, Racing y
  Central Cordoba, Austria Lustenau, juveniles, Sub-19, filiales B/C.
- paginas de listado ("UD Almeria - Transfermarkt", "Plantilla Amistoso").
- "bajas" que no son nombres: Biglietteria ("sold-out"), Cuatro, Otra, Blow;
  quien habla (Klopp provides..., Jesus has..., Branthwaite hopes...), y
  jugadores del rival ("... after Netherlands draw", "fear of Norway").

Van aparte para poder probarlos sin cargar el worker entero.
"""

from __future__ import annotations

import re
import unicodedata


def _norm(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(c for c in text if not unicodedata.combining(c)).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


NO_FUTBOL_RE = re.compile(
    r"\b(motogp|moto ?gp|moto2|moto3|formula 1|formula uno|f1|nascar|rally|dakar|grand prix|gran premio|"
    r"gp de|parrilla de salida|bolsa|acciones|cotiza\w*|ibex|nasdaq|dividend\w*|ballenas?|whales?|"
    r"trono|sucesion|monarqu\w*|princesa|gobierno|coalicion|elecciones|parlamento|ministr\w*|"
    r"aviones?|aerolinea\w*|paraiso fiscal|incendio\w*|hermandad|cofradia|autovia|carretera|"
    r"futbol sala|futsal|baloncesto|basket\w*|nba|euroliga|euroleague|acb|feb|balonmano|handball|"
    r"ciclismo|cycling|tenis|tennis|atp|wta|padel|golf|voleibol|volleyball|rugby|hockey|waterpolo|"
    r"atletismo|natacion|ciudadanos|turismo|turistas|hotel\w*|inmobiliari\w*|vivienda\w*|"
    r"concierto\w*|festival\w*|pelicula|serie de tv|esports?|procesion\w*|virgen|semana santa|"
    r"manto|coviran|cajasol|unicaja|baskonia|"
    # J11: "La Mini Desertica ... salida desde el Puerto de Almeria" entro
    # como posible salida del Almeria. Carreras populares y pruebas de
    # resistencia comparten la palabra "salida" con el mercado.
    r"mini desertica|desertica|carrera popular|maraton|media maraton|triatlon|duatlon|"
    r"trail|senderismo|mtb|btt|travesia)\b"
)
HOMONIMOS_RE = re.compile(
    r"\b(racing de cordoba|central cordoba|austria lustenau|austria wien|austria viena|austria klagenfurt|"
    r"nk croatia|victor san marino|juvenil\w*|cadete\w*|giovanili|primavera|sub ?1[5-9]|sub ?2[0-3]|"
    r"u ?1[5-9]|u ?2[0-3]|under ?1[5-9]|under ?2[0-3])\b"
)
LISTADOS_RE = re.compile(
    r"(plantillas? y estadisticas|plantilla amistoso|squad and statistics|\bgaleria\b|\bpagina \d+\b)"
)
ACCION_DE_MERCADO_RE = re.compile(
    r"\b(ficha\w*|firma\w*|sign\w*|joins?|traspas\w*|transfer\w*|cedid\w*|cesion|loan\w*|"
    r"renueva\w*|renov\w*|deja|leaves?|salida|incorpora\w*|refuerzo\w*|contrat\w*|acuerdo|deal)\b"
)
CONTEXTO_SELECCION_RE = re.compile(
    r"\b(seleccion\w*|convocatoria|convocad\w*|lista|national team|squad|call ?up|called up|"
    r"nations league|liga de naciones|uefa|fifa|mundial|world cup|eurocopa|euro 20\d\d|amistoso|"
    r"friendly|qualif\w*|clasificacion|partido|match|futbol|football|soccer|goles?|goals?|"
    r"seleccionador|coach|manager|jugador\w*|players?|lesion\w*|injur\w*|baja\w*|"
    r"withdraw\w*|camp|concentracion|stadium|estadio)\b"
)
# Categorias de _season_transition_category que son mercado de clubes.
CATEGORIAS_DE_MERCADO = {"signing", "departure", "preseason", "promotion_history"}


def motivo_titular_ajeno(title: object, source: object = "", team_name: object = "") -> str:
    """Motivo para descartar un titular que no es de ESTE equipo de futbol."""
    titulo = _norm(title)
    if not titulo:
        return "sin titular"
    completo = f"{titulo} {_norm(source)}"
    if NO_FUTBOL_RE.search(completo):
        return "no es futbol"
    if HOMONIMOS_RE.search(titulo):
        return "homonimo o categoria inferior"
    if LISTADOS_RE.search(titulo):
        return "pagina de listado"
    if "transfermarkt" in completo and not ACCION_DE_MERCADO_RE.search(titulo.replace("transfermarkt", "")):
        return "pagina de listado"
    base = _norm(team_name)
    if base and not re.search(r"\b(b|c)$", base):
        principal = base.split()[-1] if len(base.split()[-1]) >= 4 else base
        if re.search(rf"\b{re.escape(principal)} (b|c)\b", titulo):
            return "filial"
    return ""


def noticia_valida_para_seleccion(title: object, category: object = "") -> bool:
    """Para una seleccion: nada de mercado de clubes y siempre contexto de futbol."""
    if str(category or "").strip().lower() in CATEGORIAS_DE_MERCADO:
        return False
    return bool(CONTEXTO_SELECCION_RE.search(_norm(title)))


# --- Bajas -----------------------------------------------------------------
NO_SON_NOMBRES = {
    "uno", "una", "dos", "tres", "cuatro", "cinco", "seis", "siete", "ocho", "nueve",
    "diez", "otra", "otro", "otras", "otros", "nueva", "nuevo", "doble", "triple",
    "one", "two", "three", "four", "five", "another", "double", "blow", "dealt",
    "biglietteria", "biglietti", "tickets", "entradas", "abonos", "alarma", "golpe",
    "susto", "mazazo", "sold",
}
VERBOS_FINALES = {
    "leaves", "leave", "withdraws", "withdraw", "exits", "misses", "suffers", "picks",
    "returns", "deja", "abandona", "sufre", "cae", "vuelve",
}
VERBOS_DE_QUIEN_HABLA = {
    "provides", "provide", "says", "said", "hopes", "hope", "explains", "admits",
    "reveals", "insists", "claims", "warns", "praises", "talks", "speaks", "gives",
    "offers", "discusses", "responds", "hails", "backs", "urges", "defends",
    "dice", "explica", "habla", "asegura", "admite", "reconoce", "avisa", "advierte",
    "valora", "analiza", "afirma", "comenta", "opina", "lamenta", "espera", "cree",
    "elogia", "defiende", "responde", "atiende", "confiesa", "desvela",
}
# Las marcas castellanas ya estan en snapshot_worker._MARCAS_DE_RIVAL.
MARCAS_DE_RIVAL_EN = (
    "after", "against", "versus", "v", "facing", "face", "faces", "fear of",
    "win over", "victory over", "defeat to", "loss to", "draw with", "draw against",
    "beat", "beating", "tras el", "tras la", "tras",
)


def limpiar_candidato(candidate: object) -> str:
    """"Lutsharel Geertruida Leaves" -> "Lutsharel Geertruida"."""
    palabras = str(candidate or "").strip().split()
    while palabras and _norm(palabras[-1]) in VERBOS_FINALES:
        palabras.pop()
    return " ".join(palabras)


def no_es_un_nombre(candidate: object) -> bool:
    return any(p in NO_SON_NOMBRES for p in _norm(candidate).split())


def es_quien_habla(title: object, candidate: object) -> bool:
    """El nombre va seguido de un verbo de declaracion: es quien habla."""
    titulo = _norm(title)
    nombre = _norm(candidate)
    if not titulo or not nombre:
        return False
    m = re.search(rf"\b{re.escape(nombre)}\b(.*)$", titulo)
    if not m:
        return False
    resto = m.group(1).split()
    siguiente = resto[0] if resto else ""
    if siguiente in VERBOS_DE_QUIEN_HABLA:
        return True
    return siguiente == "has" and (len(resto) < 2 or resto[1] != "been")


def misma_persona(a: object, b: object) -> bool:
    """"Malen"="Malen", "Havertz"="Kai Havertz"; "Nico Williams"!="Inaki Williams"."""
    pa, pb = _norm(a).split(), _norm(b).split()
    if not pa or not pb:
        return False
    if pa == pb:
        return True
    if len(pa) == 1 or len(pb) == 1:
        return pa[-1] == pb[-1]
    return False


def deduplicar_por_apellido(entidades: list[dict]) -> list[dict]:
    """"Malen" dos veces o "Havertz" y "Kai Havertz" son la misma baja."""
    out: list[dict] = []
    for entidad in entidades:
        nombre = entidad.get("player_name")
        if not _norm(nombre):
            continue
        if any(misma_persona(nombre, previa.get("player_name")) for previa in out):
            continue
        out.append(entidad)
    return out


# --- Mercado (altas y salidas) -------------------------------------------
# J11 2026-27: tres titulares del mercado que estaban mal.
# - "PP reclama un refuerzo de Guardia Civil en el Almanzora de Almeria": el
#   "refuerzo" no es un fichaje y "de Almeria" es la provincia, no el club.
# - "Suso ... se va del Cadiz CF ..." salia como FICHAJE confirmado del Cadiz.
# - "Javier Aguirre ... tras su salida del RCD Mallorca en 2024 para hacerse
#   cargo del Valencia CF" salia como posible salida del Mallorca hoy.
# Ante la duda el titular se quita: mejor nada que un dato al reves.
TERMINOS_DE_FUTBOL_RE = re.compile(
    r"\b(futbol|football|soccer|futbolist\w*|jugador\w*|player\w*|fichaj\w*|fichar|ficha por|"
    r"traspas\w*|cesion|cedid\w*|entrenador\w*|tecnico|delanter\w*|defensa|central|"
    r"centrocampista|mediocentro|portero|guardameta|extremo|lateral|plantilla|vestuario|cantera|"
    r"filial|laliga|liga|segunda|primera|division|temporada|contrato|renov\w*|rescind\w*|"
    r"goles?|goleador\w*|partidos?|aficion|estadio|mercado|signs?|signing|transfer\w*|"
    r"loan\w*|manager|coach)\b"
)
_PARTIDO_POLITICO_RE = re.compile(r"\bpartido (popular|socialista|politico)\b")
_PREFIJOS_DE_CLUB = r"(?:ud|cf|fc|cd|sd|rcd|rc|ad|sad|club|real|sporting|deportivo|atletico|racing)"
_PALABRAS_NO_NUCLEO = {
    "ud", "cf", "fc", "cd", "sd", "rcd", "rc", "ad", "sad", "club", "de", "del", "la", "el",
    "f", "fem", "femenino", "femenina", "femeni", "women", "futbol", "real", "b", "c",
}
_SALIDA_RE = (
    r"(?:se va|se marcha|se despide|deja|dejara|sale|saldra|abandona|salida|adios|despedida|"
    r"desvinculacion|rescinde con|rompe con|traspasa|vende|cede)"
)
_LLEGADA_RE = (
    r"(?:traspasad[oa]s?|cedid[oa]s?|vendid[oa]s?|llega|llegan|aterriza|ficha por|firma por|"
    r"se une|nuevo jugador|nueva jugadora|nuevo fichaje|refuerza|signs for|joins)"
)
_ARTICULOS = r"(?:del|de la|de|el|la|al|a la|por el|por la|para el|para la|con el|con la)"


def nucleo_del_equipo(team_name: object) -> str:
    """"CADIZ CF" -> "cadiz", "RCD Mallorca" -> "mallorca", "R.MADRID (F)" -> "madrid"."""
    palabras = [p for p in _norm(team_name).split() if p not in _PALABRAS_NO_NUCLEO]
    largas = [p for p in palabras if len(p) >= 4]
    return (largas or palabras or [""])[-1]


def _nombra_al_club(titulo: str, nucleo: str) -> bool:
    """El equipo aparece como club ("el Almeria", "UD Almeria"), no como lugar."""
    if not nucleo:
        return False
    n = re.escape(nucleo)
    return bool(
        re.search(rf"\b(?:el|del|al)\s+{n}\b", titulo)
        or re.search(rf"\b{_PREFIJOS_DE_CLUB}\s+(?:de\s+)?{n}\b", titulo)
        or re.search(rf"\b{n}\s+(?:cf|fc|ud|sd|cd|club)\b", titulo)
    )


def direccion_de_mercado(title: object, team_name: object) -> str:
    """"sale" si el titular dice que alguien se va del equipo, "entra" si llega,
    "" si no se sabe. Solo mira frases donde el verbo va pegado al club."""
    titulo = _norm(title)
    nucleo = nucleo_del_equipo(team_name)
    if not titulo or not nucleo:
        return ""
    n = re.escape(nucleo)
    club = rf"(?:{_PREFIJOS_DE_CLUB}\s+)?{n}"
    sale = bool(re.search(rf"\b{_SALIDA_RE}\s+(?:{_ARTICULOS}\s+)?{club}\b", titulo))
    # "El Girona hace oficial el traspaso de Tsygankov al Ajax": el club vende.
    venta = re.search(
        rf"\b{n}\b.{{0,40}}\b(?:traspaso|venta|cesion|salida)\s+de\s+.{{1,40}}?\s+(?:al|a la|a)\s+(\w+)",
        titulo,
    )
    if venta and venta.group(1) != nucleo:
        sale = True
    entra = bool(re.search(rf"\b{_LLEGADA_RE}\s+(?:{_ARTICULOS}\s+)?{club}\b", titulo))
    if sale and not entra:
        return "sale"
    if entra and not sale:
        return "entra"
    return ""


def motivo_mercado_dudoso(
    title: object, category: object, team_name: object, anio_actual: int | None = None
) -> str:
    """Motivo para no mostrar un alta/salida de este equipo, o "" si vale.

    `category` es la de _season_transition_category ("signing"/"departure").
    """
    categoria = str(category or "").strip().lower()
    if categoria not in {"signing", "departure"}:
        return ""
    titulo = _norm(title)
    if not titulo:
        return "sin titular"
    nucleo = nucleo_del_equipo(team_name)
    sin_politica = _PARTIDO_POLITICO_RE.sub(" ", titulo)
    if not TERMINOS_DE_FUTBOL_RE.search(sin_politica) and not _nombra_al_club(titulo, nucleo):
        return "sin contexto de futbol"
    direccion = direccion_de_mercado(title, team_name)
    if categoria == "signing" and direccion == "sale":
        return "es una salida, no un fichaje"
    if categoria == "departure" and direccion == "entra":
        return "es una llegada, no una salida"
    if categoria == "departure":
        if re.search(r"\btras (?:su |la )?(?:salida|marcha|adios)\b", titulo):
            return "salida antigua que se cita de pasada"
        if anio_actual:
            for anio in re.findall(r"\b(?:salida|marcha|se fue|dejo)\b.{0,60}?\ben (20\d\d)\b", titulo):
                if int(anio) < int(anio_actual) - 1 or (int(anio) < int(anio_actual) and "tras" in titulo):
                    return f"salida de {anio}"
        destino = re.search(r"\bhacerse cargo (?:del|de la|de)\s+(?:\w+\s+)?(\w+)", titulo)
        if destino and nucleo and nucleo not in titulo[destino.start():]:
            return "habla de su llegada a otro club"
    return ""
