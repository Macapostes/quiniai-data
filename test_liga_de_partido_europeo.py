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


if FALLOS:
    print("FALLOS:")
    for f in FALLOS:
        print("  -", f)
    sys.exit(1)
print("OK: la liga sale del partido, no de la liga domestica de un equipo")
