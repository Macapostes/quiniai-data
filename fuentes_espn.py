"""Calendario y clasificacion de ESPN (API publica, sin clave).

Por que ESPN y no solo football-data / TheSportsDB:

- La clasificacion de Segunda sale del CSV de football-data, que se actualiza
  con retraso: tras los partidos del lunes varios equipos seguian con una
  jornada menos. La de Liga F sale de TheSportsDB, y en la jornada 11 llevaba
  2 partidos jugados para todo el mundo cuando ya se habian jugado 5.
- Las selecciones no tienen estadio fijo. La ficha del equipo dice "Santiago
  Bernabeu" para Espana y el partido era en Oviedo (J11) o Sevilla (J10); el
  partido Barcelona - Real Madrid femenino era en el Camp Nou, no en el Johan
  Cruyff de la ficha. El calendario de ESPN trae la sede de cada partido.
- La competicion de un partido de selecciones (J11 #15, unico de la jornada)
  se queda sin etiqueta si no hay otros de los que sacar la mayoritaria; el
  calendario la trae ("UEFA Nations League").

Este modulo solo interpreta respuestas: la peticion HTTP (y su cache) la hace
quien lo llama, con `pedir_json(url)`. Asi se prueba sin red.
"""
from __future__ import annotations

import re

from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable

ESPN_BASE = "https://site.api.espn.com/apis"
ESPN_SCOREBOARD_URL = ESPN_BASE + "/site/v2/sports/soccer/{slug}/scoreboard"
ESPN_STANDINGS_URL = ESPN_BASE + "/v2/sports/soccer/{slug}/standings"
ESPN_SUMMARY_URL = ESPN_BASE + "/site/v2/sports/soccer/{slug}/summary"
ESPN_TEAM_SCHEDULE_URL = ESPN_BASE + "/site/v2/sports/soccer/{slug}/teams/{team_id}/schedule"

# Clave de liga del worker -> competicion de ESPN.
ESPN_SLUG_POR_LIGA = {
    "soccer_spain_la_liga": "esp.1",
    "soccer_spain_segunda_division": "esp.2",
    "soccer_epl": "eng.1",
    "soccer_efl_champ": "eng.2",
    "soccer_italy_serie_a": "ita.1",
    "soccer_germany_bundesliga": "ger.1",
    "soccer_france_ligue_one": "fra.1",
    "soccer_portugal_primeira_liga": "por.1",
    "soccer_netherlands_eredivisie": "ned.1",
    "soccer_uefa_nations_league": "uefa.nations",
    "soccer_uefa_champs_league": "uefa.champions",
    "soccer_uefa_europa_league": "uefa.europa",
}
ESPN_SLUG_LIGA_F = "esp.w.1"
# Ligas cuya clasificacion se puede contrastar con la de ESPN.
SLUGS_CON_CLASIFICACION = {"esp.1", "esp.2", "esp.w.1", "eng.1", "eng.2", "ita.1", "ger.1", "fra.1", "por.1", "ned.1"}
# Un partido de selecciones europeas en una ventana FIFA: se prueba en este orden.
SLUGS_SELECCIONES = ("uefa.nations", "fifa.worldq.uefa", "uefa.euroq", "fifa.friendly")

Similitud = Callable[[str, str], float]


def slugs_para_partido(league_key: str, *, femenino: bool, selecciones: bool) -> list[str]:
    if selecciones:
        propia = ESPN_SLUG_POR_LIGA.get(league_key or "")
        orden = [propia] if propia in SLUGS_SELECCIONES else []
        return orden + [s for s in SLUGS_SELECCIONES if s not in orden]
    if femenino:
        # Solo Liga F: en la quiniela no entra otra liga femenina.
        return [ESPN_SLUG_LIGA_F]
    slug = ESPN_SLUG_POR_LIGA.get(league_key or "")
    return [slug] if slug else []


def fechas_a_consultar(kickoff: datetime) -> list[str]:
    """El dia del partido y los dos de al lado, en UTC (YYYYMMDD).

    El endpoint no acepta rangos (devuelve 400), asi que es una consulta por dia.
    """
    base = kickoff.astimezone(timezone.utc).date()
    return [(base + timedelta(days=d)).strftime("%Y%m%d") for d in (0, -1, 1)]


def _parse_fecha(value: object) -> datetime | None:
    texto = str(value or "").strip()
    if not texto:
        return None
    if texto.endswith("Z"):
        texto = texto[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(texto)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def eventos_del_marcador(payload: dict) -> list[dict]:
    """Partidos de una respuesta de /scoreboard, ya aplanados."""
    if not isinstance(payload, dict):
        return []
    liga = ((payload.get("leagues") or [{}])[0] or {}).get("name") or ""
    out = []
    for evento in payload.get("events") or []:
        try:
            comp = (evento.get("competitions") or [])[0]
            equipos = comp.get("competitors") or []
            local = next(c for c in equipos if c.get("homeAway") == "home")
            visitante = next(c for c in equipos if c.get("homeAway") == "away")
        except (IndexError, StopIteration, AttributeError):
            continue
        sede = comp.get("venue") or {}
        direccion = sede.get("address") or {}
        estado = ((evento.get("status") or {}).get("type") or {})
        out.append(
            {
                "espn_event_id": str(evento.get("id") or ""),
                "kickoff": str(evento.get("date") or ""),
                "local": ((local.get("team") or {}).get("displayName") or "").strip(),
                "visitante": ((visitante.get("team") or {}).get("displayName") or "").strip(),
                "venue": str(sede.get("fullName") or "").strip(),
                "city": str(direccion.get("city") or "").strip(),
                "country": str(direccion.get("country") or "").strip(),
                "neutral_site": bool(comp.get("neutralSite")),
                "league_name": liga,
                "status": str(estado.get("name") or ""),
                "completed": bool(estado.get("completed")),
                # ESPN marca con timeValid=false los partidos con la hora aun
                # sin fijar: esa hora no vale para corregir la de nadie.
                "time_valid": bool(comp.get("timeValid", evento.get("timeValid", True))),
                "odds": cuotas_de_competicion(comp),
            }
        )
    return out


def americana_a_decimal(valor: object) -> float | None:
    """"+165" -> 2.65, "-215" -> 1.47. None si no es una cuota americana valida."""
    texto = str(valor or "").strip().replace("−", "-")
    if texto.upper() in {"", "EVEN", "EV"}:
        return 2.0 if texto.upper() in {"EVEN", "EV"} else None
    try:
        n = float(texto)
    except ValueError:
        return None
    if n >= 100:
        dec = 1.0 + n / 100.0
    elif n <= -100:
        dec = 1.0 + 100.0 / abs(n)
    else:
        return None
    return round(dec, 2) if 1.01 <= dec <= 100 else None


def cuotas_de_competicion(comp: dict) -> dict:
    """1X2 de la casa que publique ESPN en el marcador (DraftKings), en decimal.

    Se usa la linea de cierre (la vigente) y, si no la hay, la de apertura.
    ESPN no publica cuotas de Liga F (odds: [null]); ahi devuelve {}.
    """
    for bloque in (comp or {}).get("odds") or []:
        if not isinstance(bloque, dict):
            continue
        ml = bloque.get("moneyline") or {}
        precios = {}
        for signo, lado in (("1", "home"), ("X", "draw"), ("2", "away")):
            datos = ml.get(lado) or {}
            precio = None
            for momento in ("close", "current", "open"):
                precio = americana_a_decimal((datos.get(momento) or {}).get("odds"))
                if precio:
                    break
            if precio is None and signo == "X":
                precio = americana_a_decimal((bloque.get("drawOdds") or {}).get("moneyLine"))
            if precio is None:
                break
            precios[signo] = precio
        if len(precios) == 3:
            casa = ((bloque.get("provider") or {}).get("name") or "ESPN").strip()
            return {**precios, "bookmaker": casa}
    return {}


def descanso_desde_forma(partidos: list[dict], kickoff: datetime | None) -> dict:
    """Dias desde el ultimo partido y partidos en los 14 dias previos.

    `partidos` son los de forma_de_resumen: ultimos cinco en TODAS las
    competiciones, que es lo que cuenta para el cansancio (un partido de
    Champions a mitad de semana no sale en el historico de liga).
    """
    if kickoff is None:
        return {}
    fechas = sorted(
        f for f in (_parse_fecha(p.get("date")) for p in partidos or []) if f is not None and f < kickoff
    )
    if not fechas:
        return {}
    dias = max(0, int((kickoff - fechas[-1]).total_seconds() // 86400))
    en14 = sum(1 for f in fechas if (kickoff - f).total_seconds() <= 14 * 86400)
    return {"days_since_last_match": dias, "matches_last_14_days": en14, "last_match_date": fechas[-1].isoformat()}


def elegir_partido(
    eventos: Iterable[dict],
    local: str,
    visitante: str,
    kickoff: datetime | None,
    similitud: Similitud,
    *,
    umbral: float = 0.8,
    margen_horas: float = 36.0,
) -> dict:
    """El evento de ESPN que es ESTE partido, o {} si no hay uno claro.

    Los dos equipos tienen que parecerse (y en su orden: el local de ESPN es el
    local del boleto) y la fecha tiene que caer a menos de `margen_horas` del
    kickoff conocido. Si dos candidatos empatan, no se elige ninguno.
    """
    candidatos = []
    for evento in eventos or []:
        s_local = similitud(local, evento.get("local", ""))
        s_visit = similitud(visitante, evento.get("visitante", ""))
        if s_local < umbral or s_visit < umbral:
            continue
        if kickoff is not None:
            ko = _parse_fecha(evento.get("kickoff"))
            if ko is None or abs((ko - kickoff).total_seconds()) > margen_horas * 3600:
                continue
        candidatos.append((s_local + s_visit, evento))
    if not candidatos:
        return {}
    candidatos.sort(key=lambda par: -par[0])
    if len(candidatos) > 1 and abs(candidatos[0][0] - candidatos[1][0]) < 1e-9:
        return {}
    return dict(candidatos[0][1])


def clasificacion(payload: dict) -> dict:
    """Clasificacion de /standings como {nombre: fila} con el formato del worker."""
    if not isinstance(payload, dict):
        return {}
    grupos = payload.get("children") or []
    if len(grupos) != 1:
        # Una liga con varios grupos (Champions antigua, Nations League) no es
        # una tabla unica: mejor nada que mezclar grupos.
        return {}
    grupo = grupos[0] or {}
    entradas = ((grupo.get("standings") or {}).get("entries")) or []
    tabla = {}
    for entrada in entradas:
        equipo = (entrada.get("team") or {})
        nombre = str(equipo.get("displayName") or "").strip()
        if not nombre:
            continue
        stats = {s.get("name"): s.get("value") for s in entrada.get("stats") or [] if isinstance(s, dict)}

        def _n(clave):
            try:
                return int(round(float(stats.get(clave))))
            except (TypeError, ValueError):
                return None

        jugados, puntos, gf, gc, puesto = (_n("gamesPlayed"), _n("points"), _n("pointsFor"), _n("pointsAgainst"), _n("rank"))
        if jugados is None or puntos is None or puesto is None:
            continue
        tabla[nombre] = {
            "team": nombre,
            "played": jugados,
            "points": puntos,
            "goals_for": gf or 0,
            "goals_against": gc or 0,
            "goal_diff": (gf or 0) - (gc or 0),
            "position": puesto,
            "scope": "domestic",
            "league_name": str(payload.get("name") or "").strip(),
            "source": "espn-standings",
            "season_label": str(grupo.get("abbreviation") or grupo.get("name") or ""),
            "espn_team_id": str(equipo.get("id") or ""),
        }
    return tabla


def clasificacion_por_grupos(payload: dict) -> dict:
    """Clasificacion de una competicion por grupos como {nombre: fila}.

    Cada fila lleva su grupo ("A3") y el tamano del grupo. `position` es el
    puesto DENTRO del grupo: es lo unico que significa algo. Una tabla unica con
    las 54 selecciones de la Nations League mezcla ligas A-D y daba cosas como
    "Inglaterra 42a".
    """
    if not isinstance(payload, dict):
        return {}
    tabla = {}
    for grupo in payload.get("children") or []:
        if not isinstance(grupo, dict):
            continue
        nombre_grupo = str(grupo.get("name") or grupo.get("abbreviation") or "").strip()
        codigo = re.sub(r"^(group|grupo)\s+", "", nombre_grupo, flags=re.IGNORECASE).strip()
        filas = clasificacion({"name": payload.get("name"), "children": [grupo]})
        for nombre, fila in filas.items():
            if nombre in tabla:
                # La misma seleccion en dos grupos: la tabla no es fiable.
                return {}
            fila = dict(fila)
            fila["scope"] = "group"
            fila["group"] = codigo
            fila["group_name"] = nombre_grupo
            fila["group_size"] = len(filas)
            tabla[nombre] = fila
    return tabla


def fila_de(tabla: dict, nombres: Iterable[str], similitud: Similitud, *, umbral: float = 0.85) -> dict:
    """La fila del equipo, solo si hay una que encaje claramente."""
    mejor, segundo, fila = 0.0, 0.0, {}
    for candidato, valores in (tabla or {}).items():
        s = max((similitud(n, candidato) for n in nombres if n), default=0.0)
        if s > mejor:
            mejor, segundo, fila = s, mejor, valores
        elif s > segundo:
            segundo = s
    if mejor < umbral or mejor - segundo < 0.05:
        return {}
    return dict(fila)


def mediana_jugados(tabla: dict) -> int | None:
    valores = sorted(int(f.get("played") or 0) for f in (tabla or {}).values())
    if not valores:
        return None
    return valores[len(valores) // 2]


def filas_de_calendario(payload: dict) -> list[dict]:
    """Partidos terminados de /teams/{id}/schedule como filas de football-data.

    Ordenadas por fecha, que es lo que esperan _recent_form_metrics y compania.
    """
    if not isinstance(payload, dict):
        return []
    filas = []
    for evento in payload.get("events") or []:
        try:
            comp = (evento.get("competitions") or [])[0]
        except (IndexError, TypeError):
            continue
        estado = ((comp.get("status") or evento.get("status") or {}).get("type") or {})
        if not estado.get("completed"):
            continue
        equipos = comp.get("competitors") or []
        try:
            local = next(c for c in equipos if c.get("homeAway") == "home")
            visitante = next(c for c in equipos if c.get("homeAway") == "away")
        except StopIteration:
            continue

        def _goles(c):
            marcador = c.get("score")
            if isinstance(marcador, dict):
                marcador = marcador.get("value")
            try:
                return int(round(float(marcador)))
            except (TypeError, ValueError):
                return None

        gl, gv = _goles(local), _goles(visitante)
        fecha = _parse_fecha(evento.get("date"))
        if gl is None or gv is None or fecha is None:
            continue
        filas.append(
            {
                "Date": fecha.strftime("%d/%m/%Y"),
                "KickoffUTC": fecha.isoformat(),
                "HomeTeam": ((local.get("team") or {}).get("displayName") or "").strip(),
                "AwayTeam": ((visitante.get("team") or {}).get("displayName") or "").strip(),
                "FTHG": str(gl),
                "FTAG": str(gv),
                "FTR": "H" if gl > gv else ("A" if gl < gv else "D"),
                "Source": "espn-schedule",
            }
        )
    filas.sort(key=lambda f: f["KickoffUTC"])
    return filas


def proximos_de_calendario(payload: dict, team_id: str) -> list[dict]:
    """Partidos por jugar de /teams/{id}/schedule?fixture=true del equipo `team_id`.

    Cada uno: kickoff (ISO), time_valid, venue ("home"/"away"), opponent y
    league_name. Solo los que traen al equipo pedido en uno de los dos lados.
    """
    if not isinstance(payload, dict) or not str(team_id or "").strip():
        return []
    equipo_id = str(team_id).strip()
    out = []
    for evento in payload.get("events") or []:
        try:
            comp = (evento.get("competitions") or [])[0]
        except (IndexError, TypeError):
            continue
        estado = ((comp.get("status") or evento.get("status") or {}).get("type") or {})
        if estado.get("completed"):
            continue
        equipos = comp.get("competitors") or []
        propio = next((c for c in equipos if str((c.get("team") or {}).get("id") or c.get("id") or "") == equipo_id), None)
        rival = next((c for c in equipos if c is not propio), None)
        fecha = _parse_fecha(evento.get("date"))
        if propio is None or rival is None or fecha is None or propio.get("homeAway") not in {"home", "away"}:
            continue
        out.append(
            {
                "kickoff": fecha.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "time_valid": bool(comp.get("timeValid", evento.get("timeValid", True))),
                "venue": propio.get("homeAway"),
                "opponent": ((rival.get("team") or {}).get("displayName") or "").strip(),
                "league_name": str(((evento.get("league") or {}).get("name")) or "").strip(),
                "espn_event_id": str(evento.get("id") or ""),
            }
        )
    out.sort(key=lambda f: f["kickoff"])
    return out


def forma_de_resumen(payload: dict) -> dict:
    """Ultimos cinco partidos de cada equipo segun /summary (todas las competiciones).

    Es la unica forma que hay para una seleccion: el historico de Nations
    League no tiene partidos de esta temporada, y un amistoso o un Mundial no
    estan en ninguna liga. El marcador de ESPN va con el goleador delante
    ("W 1-0", "L 2-1"), asi que los goles se ordenan con el resultado.
    """
    out = {}
    for bloque in (payload or {}).get("lastFiveGames") or []:
        equipo = ((bloque.get("team") or {}).get("displayName") or "").strip()
        partidos = []
        for ev in bloque.get("events") or []:
            resultado = str(ev.get("gameResult") or "").upper()[:1]
            try:
                a, b = (int(x) for x in str(ev.get("score") or "").split("-")[:2])
            except ValueError:
                continue
            if resultado not in {"W", "D", "L"}:
                continue
            alto, bajo = max(a, b), min(a, b)
            gf, gc = (alto, bajo) if resultado == "W" else ((bajo, alto) if resultado == "L" else (a, b))
            partidos.append({
                "date": str(ev.get("gameDate") or ""),
                "result": resultado,
                "goals_for": gf,
                "goals_against": gc,
                "home": str(ev.get("atVs") or "").strip().lower() == "vs",
                "opponent": ((ev.get("opponent") or {}).get("displayName") or "").strip(),
                "league": str(ev.get("leagueName") or ""),
            })
        partidos.sort(key=lambda p: p["date"])
        if equipo and partidos:
            out[equipo] = partidos
    return out


def metricas_de_forma(partidos: list[dict]) -> dict:
    """Mismo formato que _recent_form_metrics del worker."""
    if not partidos:
        return {}
    puntos = sum(3 if p["result"] == "W" else (1 if p["result"] == "D" else 0) for p in partidos)
    gf = sum(p["goals_for"] for p in partidos)
    gc = sum(p["goals_against"] for p in partidos)
    n = len(partidos)
    return {
        "matches": n,
        "form": "".join(p["result"] for p in partidos),
        "points": puntos,
        "points_per_game": round(puntos / n, 2),
        "goals_for": gf,
        "goals_against": gc,
        "avg_goals_for": round(gf / n, 2),
        "avg_goals_against": round(gc / n, 2),
        "clean_sheets": sum(1 for p in partidos if p["goals_against"] == 0),
        "competitions": sorted({p["league"] for p in partidos if p["league"]}),
    }
