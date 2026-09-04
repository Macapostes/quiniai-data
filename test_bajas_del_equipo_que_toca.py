# -*- coding: utf-8 -*-
"""Las bajas del informe tienen que ser personas, y del equipo al que se atribuyen.

El 4 de septiembre de 2026 el PDF de IA Avanzada de la jornada 4 -100 creditos-
publico estas dos lineas:

    "Malaga-Levante: bajas verificables relevantes incluyendo a Mourinho y
     Lamine Yamal Injury"
    "La Real Sociedad B tiene bajas verificables (Oskarsson, Celta, Matarazzo)"

Cuatro entradas, cuatro problemas distintos: un entrenador ajeno al partido, un
jugador de otro equipo con una palabra del titular pegada, un club, y otro
entrenador. Ninguno era una baja del equipo al que se le colgo.

Estos cuatro son el criterio de aceptacion. Y el ultimo test es igual de
importante: si el arreglo deja el bloque de bajas siempre vacio, no es un
arreglo.
"""

import sys

import snapshot_worker as w


FALLOS = []


def comprobar(condicion, mensaje):
    if condicion:
        return
    FALLOS.append(mensaje)


def _bajas(equipo, titular, rival=""):
    item = {
        "title": titular,
        "source": "Prueba",
        "link": "https://example.invalid/x",
        "published_at": "",
    }
    return [e["player_name"] for e in w._build_injury_entities(equipo, [item], rival=rival)]


# --- Los cuatro casos que se publicaron -------------------------------------

# 1. Mourinho: entrenador, y de un equipo que no juega ese partido. El titular
#    ni siquiera nombra al Malaga.
comprobar(
    "Mourinho" not in _bajas("Málaga", "Mourinho, baja sensible en el Fenerbahce por lesion"),
    "se sigue atribuyendo Mourinho al Malaga",
)

# 2. Lamine Yamal: jugador del Barcelona, con la palabra del titular pegada.
comprobar(
    not _bajas("Málaga", "Lamine Yamal Injury: Barcelona confirm star is out"),
    "se sigue atribuyendo Lamine Yamal al Malaga",
)

# 3. Celta: un club, no una persona.
comprobar(
    "Celta" not in _bajas(
        "Real Sociedad B", "El Celta pierde a un jugador por lesion ante la Real Sociedad B"
    ),
    "Celta sigue saliendo como persona",
)

# 4. Matarazzo: entrenador.
comprobar(
    "Matarazzo" not in _bajas(
        "Real Sociedad B", "El entrenador Matarazzo, baja en el banquillo de la Real Sociedad B"
    ),
    "Matarazzo sigue saliendo como jugador",
)


# 5. El mismo error por otra puerta, encontrado al medir sobre el feed real:
#    un titular que SI nombra al equipo, pero que trata de otro. "Convocatoria
#    del Real Madrid contra el Malaga" pasaba la comprobacion de procedencia
#    -nombra al Malaga- y le colgaba al Malaga los nombres del Real Madrid.
comprobar(
    not _bajas("Málaga", "Convocatoria del Real Madrid contra el Málaga: Mourinho deja fuera a Ceballos"),
    "un titular del Real Madrid sigue dando bajas del Malaga",
)
comprobar(
    not _bajas(
        "Real Sociedad",
        "Convocatoria del Real Madrid contra la Real Sociedad: Mourinho mantiene a Asencio",
    ),
    "dos equipos que comparten la palabra 'Real' vuelven a confundirse",
)
comprobar(
    w._el_titular_es_de_otro_equipo(
        "Convocatoria del Real Madrid contra el Málaga: Mourinho", "Málaga"
    ),
    "ese titular es del Real Madrid, no del Malaga",
)
comprobar(
    not w._el_titular_es_de_otro_equipo(
        "El Málaga pierde a Ramon Enriquez por lesion", "Málaga"
    ),
    "ese titular si es del Malaga",
)


# --- Y una baja legitima tiene que seguir pasando ---------------------------

legitimas = _bajas("Real Sociedad B", "El Real Sociedad B pierde a Oskarsson por lesion muscular")
comprobar(
    "Oskarsson" in legitimas,
    f"se ha perdido la baja legitima: el filtro devolvio {legitimas!r}",
)

otra = _bajas("Málaga", "El Málaga pierde a Ramon Enriquez por lesion hasta diciembre")
comprobar(
    any("Enriquez" in nombre for nombre in otra),
    f"se ha perdido otra baja legitima: {otra!r}",
)


# --- Las piezas, por separado ------------------------------------------------

comprobar(w._es_nombre_de_club("Celta"), "Celta deberia detectarse como club")
comprobar(w._es_nombre_de_club("Barcelona"), "Barcelona deberia detectarse como club")
comprobar(not w._es_nombre_de_club("Oskarsson"), "Oskarsson no es un club")
comprobar(not w._es_nombre_de_club("Mourinho"), "Mourinho no es un club")

# El catalogo de alias no tiene ni Malaga ni Eibar; los equipos de la jornada
# se registran al construir el snapshot y tapan ese hueco.
comprobar(not w._es_nombre_de_club("Malaga"), "de partida Malaga no esta en el catalogo")
w._recordar_clubes_de_la_jornada("Málaga", "Eibar")
comprobar(w._es_nombre_de_club("Malaga"), "tras registrarlo, Malaga deberia ser club")
comprobar(w._es_nombre_de_club("Eibar"), "tras registrarlo, Eibar deberia ser club")

comprobar(
    w._titular_menciona_equipo("El Malaga pierde a su delantero", "Málaga"),
    "el titular si nombra al Malaga",
)
comprobar(
    not w._titular_menciona_equipo("Lamine Yamal Injury: Barcelona confirm star is out", "Málaga"),
    "ese titular no nombra al Malaga",
)

comprobar(w._parece_resto_de_titular("Lamine Yamal Injury"), "'Injury' es resto de titular")
comprobar(w._parece_resto_de_titular("El Malaga"), "'El Malaga' empieza por determinante")
comprobar(not w._parece_resto_de_titular("Ramon Enriquez"), "un nombre normal no es resto")

comprobar(
    w._parece_entrenador("El entrenador Matarazzo deja el banquillo", "Matarazzo"),
    "Matarazzo aparece junto a 'entrenador'",
)
comprobar(
    not w._parece_entrenador("El Malaga pierde a Ramon Enriquez por lesion", "Ramon Enriquez"),
    "ahi no hay ninguna palabra de entrenador",
)


# --- Plantillas: lo que ninguna heuristica de texto resuelve ----------------
#
# Tras las cinco capas de texto seguian saliendo "Bernardo Silva" y "Camavinga"
# como bajas del Inter de Milan. Son del Real Madrid, y no hay forma de verlo
# mirando el nombre: son nombres de persona validos, en un titular que habla
# del partido. Solo la plantilla lo dice.
#
# Se indexa a mano para no depender de la red en el test.
w._INDICE_DE_JUGADORES.clear()
w._INDICE_DE_JUGADORES.update({
    "bernardo silva": {"real madrid"},
    "tchouameni": {"real madrid"},
    "gavi": {"barcelona"},
})

comprobar(
    w._es_jugador_de_otro_equipo("Bernardo Silva", "Inter Milan"),
    "Bernardo Silva es del Real Madrid, no baja del Inter",
)
comprobar(
    not w._es_jugador_de_otro_equipo("Bernardo Silva", "Real Madrid"),
    "para el Real Madrid, Bernardo Silva si es suyo",
)
comprobar(
    not _bajas("Inter Milan", "Bernardo Silva, baja por sancion para recibir al Inter Milan"),
    "sigue atribuyendose Bernardo Silva al Inter",
)

# Y lo que NO puede pasar: un nombre desconocido no se descarta nunca. Las
# plantillas del proveedor vienen incompletas -diez jugadores de veinticinco-,
# asi que usarlas como lista blanca tiraria las bajas de los que faltan.
comprobar(
    not w._es_jugador_de_otro_equipo("Oskarsson", "Real Sociedad B"),
    "un nombre que no esta en ninguna plantilla no se puede descartar",
)
comprobar(
    "Oskarsson" in _bajas("Real Sociedad B", "El Real Sociedad B pierde a Oskarsson por lesion"),
    "la baja legitima tiene que seguir pasando con el indice cargado",
)
w._INDICE_DE_JUGADORES.clear()


if FALLOS:
    print("FALLOS:")
    for f in FALLOS:
        print("  -", f)
    sys.exit(1)
print("OK: los cuatro casos del informe se descartan y las bajas legitimas pasan")
