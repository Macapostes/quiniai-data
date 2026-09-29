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
    r"manto|coviran|cajasol|unicaja|baskonia)\b"
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
