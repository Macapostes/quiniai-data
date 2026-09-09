# -*- coding: utf-8 -*-
"""La liga de un partido no se deduce de la liga domestica de un equipo.

En la jornada 5 -toda de Champions- el informe de IA Avanzada dijo que el Oporto
"acabo 13o en la Premier con 49 puntos". Eso es el Everton, y no se lo invento el
modelo: venia asi en el feed.

La cadena era: la liga se tomaba de la primera ficha de equipo que la trajera, y
como el Man City es de la Premier, OPORTO - MAN.CITY quedo etiquetado como
Premier League. Despues se busco "OPORTO" en la tabla inglesa y, con un umbral de
parecido de 0.33, Everton (0.46) valia. De ahi salian la clasificacion, la racha
y el H2H que el informe presenta como del equipo.
"""

import sys

import snapshot_worker as w

FALLOS = []


def comprobar(cond, msg):
    if not cond:
        FALLOS.append(msg)


def _liga(local, visitante, evento, ficha_local, ficha_visitante):
    match = {"local": local, "visitante": visitante}
    w._apply_dynamic_league_metadata(match, evento, ficha_local, ficha_visitante)
    return match.get("league")


LALIGA, PREMIER, BUNDES, PORTUGAL, CHAMPIONS = "4335", "4328", "4331", "4344", "4480"

# Equipos de ligas distintas: es competicion europea, ninguna liga domestica vale.
comprobar(
    _liga("B.DORTMUND", "VILLARREAL", {}, {"idLeague": BUNDES}, {"idLeague": LALIGA}) is None,
    "Dortmund-Villarreal no puede quedarse con LaLiga",
)
comprobar(
    _liga("OPORTO", "MAN.CITY", {}, {"idLeague": PORTUGAL}, {"idLeague": PREMIER}) is None,
    "Oporto-Man City no puede quedarse con la Premier",
)

# Si el propio partido dice en que competicion se juega, manda el partido.
comprobar(
    _liga("PSG", "SLOVAN", {"idLeague": CHAMPIONS}, {"idLeague": "4334"}, {"idLeague": "4353"})
    == "sportsdb_" + CHAMPIONS,
    "con evento, la competicion la dice el evento",
)

# Y un partido domestico de verdad sigue resolviendose.
comprobar(
    _liga("SEVILLA", "BARCELONA", {}, {"idLeague": LALIGA}, {"idLeague": LALIGA})
    == "soccer_spain_la_liga",
    "un partido de LaLiga tiene que seguir saliendo LaLiga",
)

# Sin datos no se inventa nada.
comprobar(_liga("A", "B", {}, {}, {}) is None, "sin fichas no se asigna liga")

# El boleto recorta. El histórico no. Tienen que ser el mismo club.
comprobar(
    w._es_el_mismo_club("OPORTO", "Porto"),
    "OPORTO y Porto son el mismo club",
)
comprobar(
    w._es_el_mismo_club("MAN.CITY", "Manchester City"),
    "MAN.CITY y Manchester City son el mismo club",
)
comprobar(
    w._es_el_mismo_club("SPORTING PORT.", "Sporting CP"),
    "SPORTING PORT. y Sporting CP son el mismo club",
)
comprobar(
    w._canonical_team_name("MAN.CITY") == "Manchester City",
    "MAN.CITY canónico es Manchester City",
)
comprobar(
    w._canonical_team_name("OPORTO") == "Porto",
    "OPORTO canónico es Porto",
)
comprobar(
    "FC Porto" in w._h2h_search_names("OPORTO"),
    "TheSportsDB tiene que buscar FC Porto, no solo OPORTO",
)
comprobar(
    any("manchester city" in n.lower() for n in w._h2h_search_names("MAN.CITY")),
    "TheSportsDB tiene que buscar Manchester City, no solo MAN.CITY",
)


# La deduccion por historicos exige que los dos equipos sean de esa liga.
filas_premier = [
    {"HomeTeam": "Everton", "AwayTeam": "Southampton"},
    {"HomeTeam": "Man City", "AwayTeam": "Everton"},
]
comprobar(
    w._infer_league_from_histories("OPORTO", "MAN.CITY", {"soccer_epl": filas_premier}) == "",
    "OPORTO no esta en la Premier: no puede deducirse esa liga",
)
filas_liga = [
    {"HomeTeam": "Sevilla", "AwayTeam": "Barcelona"},
    {"HomeTeam": "Barcelona", "AwayTeam": "Sevilla"},
]
comprobar(
    w._infer_league_from_histories("SEVILLA", "BARCELONA", {"soccer_spain_la_liga": filas_liga})
    == "soccer_spain_la_liga",
    "con los dos equipos dentro, si se deduce",
)


# --- La competicion real la dice el feed de cuotas ---------------------------
#
# El boleto solo trae "OPORTO - MAN.CITY". Quien sabe que eso es Champions es el
# proveedor de cuotas, que lo marca como soccer_uefa_champs_league. Y hace falta
# saberlo para lo que de verdad importa: en un partido de Champions la
# clasificacion que cuenta es la de la Champions -si el equipo ya esta
# clasificado o se juega el pase-, no la de su liga domestica.

CUOTAS = [
    {"home_team": "Porto", "away_team": "Manchester City",
     "commence_time": "2026-09-08T21:00:00Z", "sport_key": "soccer_uefa_champs_league"},
    {"home_team": "Borussia Dortmund", "away_team": "Villarreal",
     "commence_time": "2026-09-08T21:00:00Z", "sport_key": "soccer_uefa_champs_league"},
    {"home_team": "Paris Saint Germain", "away_team": "ŠK Slovan Bratislava",
     "commence_time": "2026-09-09T21:00:00Z", "sport_key": "soccer_uefa_champs_league"},
    {"home_team": "Sevilla", "away_team": "Barcelona",
     "commence_time": "2026-09-12T19:00:00Z", "sport_key": "soccer_spain_la_liga"},
]


def _competicion(local, visitante, kickoff):
    return w._competicion_desde_las_cuotas(
        {"local": local, "visitante": visitante, "kickoff": kickoff}, CUOTAS
    )


comprobar(
    _competicion("OPORTO", "MAN.CITY", "2026-09-08T21:00:00Z") == "soccer_uefa_champs_league",
    "OPORTO - MAN.CITY tiene que heredar la Champions del feed de cuotas",
)
comprobar(
    _competicion("B.DORTMUND", "VILLARREAL", "2026-09-08T21:00:00Z") == "soccer_uefa_champs_league",
    "B.DORTMUND tiene que emparejar con Borussia Dortmund",
)
comprobar(
    _competicion("PSG", "SLOVAN BRATISLAVA", "2026-09-09T21:00:00Z") == "soccer_uefa_champs_league",
    "PSG son las iniciales de Paris Saint Germain",
)
comprobar(
    _competicion("SEVILLA", "BARCELONA", "2026-09-12T19:00:00Z") == "soccer_spain_la_liga",
    "un partido domestico sigue siendo de su liga",
)

# La hora forma parte del emparejamiento: los mismos equipos en otra fecha son
# otro partido, y puede ser de otra competicion.
comprobar(
    _competicion("OPORTO", "MAN.CITY", "2026-11-20T21:00:00Z") == "",
    "con la hora lejos no se empareja",
)

# Y las competiciones europeas tienen que resolver a un historico propio, que es
# de donde saldra su clasificacion segun avance la fase de liga.
for clave, id_esperada in (
    ("soccer_uefa_champs_league", "4480"),
    ("soccer_uefa_europa_league", "4481"),
    ("soccer_uefa_europa_conference_league", "5071"),
):
    comprobar(
        w._sportsdb_league_id_for_key(clave) == id_esperada,
        f"{clave} deberia resolver a {id_esperada}",
    )

# Las siglas no pueden abrir la mano: solo valen si son EXACTAMENTE las iniciales.
comprobar(w._es_el_mismo_club("PSG", "Paris Saint Germain"), "PSG es Paris Saint Germain")
comprobar(
    not w._es_el_mismo_club("PSG", "Sporting Portugal Guimaraes"),
    "tres letras que no son las iniciales no valen",
)


if FALLOS:
    print("FALLOS:")
    for f in FALLOS:
        print("  -", f)
    sys.exit(1)
print("OK: la liga sale del partido, no de la liga domestica de un equipo")
