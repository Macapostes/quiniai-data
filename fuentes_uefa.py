"""API publica de UEFA (match.uefa.com) para las copas de clubes.

Fuente oficial y estructurada, sin clave: designacion arbitral, jornada,
alineaciones oficiales, ids de equipo y pais de cada club. Solo parseo puro
aqui; las peticiones y la cache las hace el worker.
"""
from __future__ import annotations

import re
import unicodedata

UEFA_MATCHES_URL = "https://match.uefa.com/v5/matches"
UEFA_LINEUPS_URL = "https://match.uefa.com/v5/matches/{match_id}/lineups"

COMPETICION_UEFA_POR_LIGA = {
    "soccer_uefa_champs_league": "1",
    "soccer_uefa_europa_league": "14",
    "soccer_uefa_europa_conference_league": "2019",
}

# FIFA/UEFA 3 letras -> ISO2 (lo que usa calidad_europea).
ISO3_A_ISO2 = {
    "ESP": "ES", "ENG": "GB", "SCO": "GB", "WAL": "GB", "NIR": "GB", "FRA": "FR", "GER": "DE",
    "ITA": "IT", "POR": "PT", "NED": "NL", "BEL": "BE", "NOR": "NO", "SWE": "SE", "DEN": "DK",
    "CZE": "CZ", "GRE": "GR", "TUR": "TR", "AUT": "AT", "SUI": "CH", "POL": "PL", "CRO": "HR",
    "SRB": "RS", "UKR": "UA", "IRL": "IE", "CYP": "CY", "ISR": "IL", "AZE": "AZ", "KAZ": "KZ",
    "HUN": "HU", "ROU": "RO", "BUL": "BG", "SVK": "SK", "SVN": "SI", "FIN": "FI", "ISL": "IS",
    "MDA": "MD", "LUX": "LU", "LTU": "LT", "LVA": "LV", "EST": "EE", "BLR": "BY", "ALB": "AL",
    "MKD": "MK", "MNE": "ME", "BIH": "BA", "ARM": "AM", "GEO": "GE", "AND": "AD", "MLT": "MT",
    "FRO": "FO", "GIB": "GI", "KOS": "XK", "SMR": "SM", "LIE": "LI",
}

LINEUP_CONFIRMADA = {"TACTICAL_AVAILABLE", "LINEUP_AVAILABLE", "CONFIRMED"}


def temporada_uefa(anio: int, mes: int) -> int:
    """seasonYear de UEFA = ano en que acaba la temporada (2026-27 -> 2027)."""
    return anio + 1 if mes >= 7 else anio


def _txt(nodo: object, *ruta: str) -> str:
    for clave in ruta:
        if not isinstance(nodo, dict):
            return ""
        nodo = nodo.get(clave)
    return str(nodo or "").strip()


_LETRAS = str.maketrans({"ø": "o", "Ø": "o", "æ": "ae", "Æ": "ae", "ß": "ss", "ł": "l", "Ł": "l", "đ": "d", "Đ": "d", "ı": "i"})


def norm(texto: object) -> str:
    texto = str(texto or "").translate(_LETRAS)
    t = unicodedata.normalize("NFKD", str(texto or "")).encode("ascii", "ignore").decode().lower()
    t = re.sub(r"[^a-z0-9 ]+", " ", t)
    t = re.sub(r"\b(fc|cf|ac|as|sc|afc|ssc|club|de|the|cd|sk|fk|rb)\b", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def equipo(nodo: dict) -> dict:
    nodo = nodo if isinstance(nodo, dict) else {}
    iso3 = str(nodo.get("countryCode") or "").upper()
    nombres = {
        nodo.get("internationalName"),
        _txt(nodo, "translations", "displayName", "EN"),
        _txt(nodo, "translations", "displayOfficialName", "EN"),
        _txt(nodo, "translations", "displayName", "ES"),
        _txt(nodo, "translations", "shortName", "EN"),
    }
    return {
        "uefa_team_id": str(nodo.get("id") or ""),
        "name": str(nodo.get("internationalName") or ""),
        "names": sorted({norm(n) for n in nombres if n and norm(n)}),
        "country_iso3": iso3,
        "country": ISO3_A_ISO2.get(iso3, ""),
        "men_first_team": str(nodo.get("teamTypeDetail") or "") in ("", "DOMESTIC_MEN_TEAM_A"),
    }


def partidos(payload: object) -> list[dict]:
    """Lista de partidos de /v5/matches con lo que nos interesa."""
    salida = []
    for m in payload if isinstance(payload, list) else []:
        if not isinstance(m, dict):
            continue
        arbitro = ""
        for r in m.get("referees") or []:
            if isinstance(r, dict) and r.get("role") == "REFEREE":
                arbitro = _txt(r, "person", "translations", "name", "EN") or _txt(r, "person", "translations", "name", "ES")
                break
        jornada = _txt(m, "matchday", "name")
        num = re.search(r"(\d+)", jornada)
        estadio = m.get("stadium") if isinstance(m.get("stadium"), dict) else {}
        geo = estadio.get("geolocation") if isinstance(estadio.get("geolocation"), dict) else {}
        salida.append({
            "uefa_match_id": str(m.get("id") or ""),
            "kickoff": _txt(m, "kickOffTime", "dateTime"),
            "status": str(m.get("status") or ""),
            "lineup_status": str(m.get("lineupStatus") or ""),
            "home": equipo(m.get("homeTeam")),
            "away": equipo(m.get("awayTeam")),
            "referee": arbitro,
            "matchday": int(num.group(1)) if num and _txt(m, "round", "phase") in ("", "LEAGUE", "TOURNAMENT") else None,
            "matchday_label": jornada,
            "phase": _txt(m, "round", "phase"),
            "city": _txt(estadio, "city", "translations", "name", "EN"),
            "stadium_country_iso3": str(estadio.get("countryCode") or "").upper(),
            "lat": geo.get("latitude"),
            "lon": geo.get("longitude"),
        })
    return salida


def _coincide(nombres_uefa: list[str], pedidos: list[str]) -> bool:
    """Igualdad (o inclusion de palabra completa) de nombres normalizados. Nunca
    parecido: "lille" no es "lillestrom"."""
    for p in pedidos:
        p = norm(p)
        if not p:
            continue
        for u in nombres_uefa:
            if p == u or f" {p} " in f" {u} " or f" {u} " in f" {p} ":
                return True
            # Una palabra significativa comun ("slavia" en Slavia Praha /
            # Slavia Prague). Palabra completa: "lille" no casa "lillestrom".
            comunes = {w for w in p.split() if len(w) >= 4} & {w for w in u.split() if len(w) >= 4}
            if comunes - _GENERICAS:
                return True
    return False


_GENERICAS = {"real", "united", "city", "sporting", "athletic", "atletico", "club", "olympique", "dynamo",
              "dinamo", "racing", "union", "inter", "borussia", "sport", "football", "calcio", "town", "rovers"}


def buscar_partido(lista: list[dict], locales: list[str], visitantes: list[str]) -> dict:
    """El partido cuyo local Y visitante coinciden. Exigir los dos lados el
    mismo dia en la misma competicion hace imposible confundir clubes."""
    encontrados = [
        p for p in lista
        if _coincide(p["home"]["names"], locales) and _coincide(p["away"]["names"], visitantes)
    ]
    return encontrados[0] if len(encontrados) == 1 else {}


def alineacion(payload: object) -> dict:
    """Once titular oficial; {} si no esta publicado o no son 11."""
    if not isinstance(payload, dict) or str(payload.get("lineupStatus") or "") not in LINEUP_CONFIRMADA:
        return {}
    salida = {}
    for lado, clave in (("home", "homeTeam"), ("away", "awayTeam")):
        bloque = payload.get(clave) if isinstance(payload.get(clave), dict) else {}
        once = []
        for j in bloque.get("field") or []:
            jugador = (j or {}).get("player") or {}
            nombre = _txt(jugador, "translations", "name", "EN") or str(jugador.get("clubShirtName") or "")
            if nombre:
                once.append({"name": nombre, "position": str(jugador.get("fieldPosition") or "")})
        if len(once) != 11:
            return {}
        entrenador = ""
        for c in bloque.get("coaches") or []:
            entrenador = _txt(c, "person", "translations", "name", "EN")
            if entrenador:
                break
        salida[lado] = {"starters": once, "coach": entrenador}
    return {"status": "confirmed", "source": "uefa", **salida}
