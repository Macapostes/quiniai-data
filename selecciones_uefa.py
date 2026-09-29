"""Selecciones europeas tal como las escribe el boleto (LAE) y donde estan.

En la jornada 10 (Nations League) el worker solo conocia ocho selecciones. Las
demas se buscaban por su nombre en castellano y el geocodificador devolvia el
pueblo homonimo mas "importante": LUXEMBURGO acabo en Honduras, REP.CHECA en
Matamoros (Mexico), DINAMARCA en Iran, ESCOCIA en Nueva York y GRECIA en el
club de Grecia (Costa Rica). Con la meteo, el viaje y la sede de alli.

Cada entrada: nombre canonico (el que usan TheSportsDB/Wikipedia), codigo ISO
del pais (GB para las cuatro britanicas, como COUNTRY_BOUNDING_BOXES), ciudad
de referencia, huso, coordenadas y los alias del boleto ya normalizados
(_normalize_team_name: minusculas, sin tildes ni puntos).

ANDORRA no esta a proposito: en Segunda "ANDORRA" es el FC Andorra, y
convertirlo en seleccion le quitaria su tabla y su H2H.
"""

SELECCIONES_UEFA = [
    ("Spain", "ES", "Madrid", "Europe/Madrid", 40.4168, -3.7038, ["espana"]),
    ("Portugal", "PT", "Lisbon", "Europe/Lisbon", 38.7223, -9.1393, []),
    ("France", "FR", "Paris", "Europe/Paris", 48.8566, 2.3522, ["francia"]),
    ("Italy", "IT", "Rome", "Europe/Rome", 41.9028, 12.4964, ["italia"]),
    ("Germany", "DE", "Berlin", "Europe/Berlin", 52.5200, 13.4050, ["alemania"]),
    ("Netherlands", "NL", "Amsterdam", "Europe/Amsterdam", 52.3676, 4.9041, ["paises bajos", "holanda"]),
    ("Belgium", "BE", "Brussels", "Europe/Brussels", 50.8503, 4.3517, ["belgica"]),
    ("Luxembourg", "LU", "Luxembourg", "Europe/Luxembourg", 49.6116, 6.1319, ["luxemburgo"]),
    ("Switzerland", "CH", "Bern", "Europe/Zurich", 46.9480, 7.4474, ["suiza"]),
    ("Austria", "AT", "Vienna", "Europe/Vienna", 48.2082, 16.3738, []),
    ("England", "GB", "London", "Europe/London", 51.5072, -0.1276, ["inglaterra"]),
    ("Scotland", "GB", "Glasgow", "Europe/London", 55.8642, -4.2518, ["escocia"]),
    ("Wales", "GB", "Cardiff", "Europe/London", 51.4816, -3.1791, ["gales"]),
    ("Northern Ireland", "GB", "Belfast", "Europe/London", 54.5973, -5.9301,
     ["irlanda n", "irlanda del norte", "irl del norte", "n irlanda", "irlanda norte"]),
    ("Republic of Ireland", "IE", "Dublin", "Europe/Dublin", 53.3498, -6.2603,
     ["rep irlanda", "irlanda", "republica de irlanda", "ireland"]),
    ("Denmark", "DK", "Copenhagen", "Europe/Copenhagen", 55.6761, 12.5683, ["dinamarca"]),
    ("Norway", "NO", "Oslo", "Europe/Oslo", 59.9139, 10.7522, ["noruega"]),
    ("Sweden", "SE", "Stockholm", "Europe/Stockholm", 59.3293, 18.0686, ["suecia"]),
    ("Finland", "FI", "Helsinki", "Europe/Helsinki", 60.1699, 24.9384, ["finlandia"]),
    ("Iceland", "IS", "Reykjavik", "Atlantic/Reykjavik", 64.1466, -21.9426, ["islandia"]),
    ("Faroe Islands", "FO", "Torshavn", "Atlantic/Faroe", 62.0079, -6.7909, ["islas feroe", "feroe"]),
    ("Poland", "PL", "Warsaw", "Europe/Warsaw", 52.2297, 21.0122, ["polonia"]),
    ("Czech Republic", "CZ", "Prague", "Europe/Prague", 50.0755, 14.4378,
     ["rep checa", "republica checa", "chequia", "czechia"]),
    ("Slovakia", "SK", "Bratislava", "Europe/Bratislava", 48.1486, 17.1077, ["eslovaquia"]),
    ("Hungary", "HU", "Budapest", "Europe/Budapest", 47.4979, 19.0402, ["hungria"]),
    ("Slovenia", "SI", "Ljubljana", "Europe/Ljubljana", 46.0569, 14.5058, ["eslovenia"]),
    ("Croatia", "HR", "Zagreb", "Europe/Zagreb", 45.8150, 15.9819, ["croacia"]),
    ("Bosnia and Herzegovina", "BA", "Sarajevo", "Europe/Sarajevo", 43.8563, 18.4131,
     ["bosnia", "bosnia herzegovina", "bosnia y herzegovina", "bosnia herzeg"]),
    ("Serbia", "RS", "Belgrade", "Europe/Belgrade", 44.7866, 20.4489, []),
    ("Montenegro", "ME", "Podgorica", "Europe/Podgorica", 42.4304, 19.2594, []),
    ("North Macedonia", "MK", "Skopje", "Europe/Skopje", 41.9981, 21.4254,
     ["macedonia n", "macedonia del norte", "macedonia", "macedonia norte"]),
    ("Albania", "AL", "Tirana", "Europe/Tirane", 41.3275, 19.8187, []),
    ("Kosovo", "XK", "Pristina", "Europe/Belgrade", 42.6629, 21.1655, []),
    ("Greece", "GR", "Athens", "Europe/Athens", 37.9838, 23.7275, ["grecia"]),
    ("Bulgaria", "BG", "Sofia", "Europe/Sofia", 42.6977, 23.3219, []),
    ("Romania", "RO", "Bucharest", "Europe/Bucharest", 44.4268, 26.1025, ["rumania"]),
    ("Moldova", "MD", "Chisinau", "Europe/Chisinau", 47.0105, 28.8638, ["moldavia"]),
    ("Ukraine", "UA", "Kyiv", "Europe/Kiev", 50.4501, 30.5234, ["ucrania"]),
    ("Belarus", "BY", "Minsk", "Europe/Minsk", 53.9006, 27.5590, ["bielorrusia"]),
    ("Lithuania", "LT", "Vilnius", "Europe/Vilnius", 54.6872, 25.2797, ["lituania"]),
    ("Latvia", "LV", "Riga", "Europe/Riga", 56.9496, 24.1052, ["letonia"]),
    ("Estonia", "EE", "Tallinn", "Europe/Tallinn", 59.4370, 24.7536, []),
    ("Turkey", "TR", "Istanbul", "Europe/Istanbul", 41.0082, 28.9784, ["turquia", "turkiye"]),
    ("Cyprus", "CY", "Nicosia", "Asia/Nicosia", 35.1856, 33.3823, ["chipre"]),
    ("Malta", "MT", "Ta' Qali", "Europe/Malta", 35.8946, 14.4153, []),
    ("Gibraltar", "GI", "Gibraltar", "Europe/Gibraltar", 36.1408, -5.3536, []),
    ("San Marino", "SM", "Serravalle", "Europe/San_Marino", 43.9706, 12.4786, []),
    ("Liechtenstein", "LI", "Vaduz", "Europe/Vaduz", 47.1410, 9.5209, []),
    ("Georgia", "GE", "Tbilisi", "Asia/Tbilisi", 41.7151, 44.8271, []),
    ("Armenia", "AM", "Yerevan", "Asia/Yerevan", 40.1792, 44.4991, []),
    ("Azerbaijan", "AZ", "Baku", "Asia/Baku", 40.4093, 49.8671, ["azerbaiyan"]),
    ("Kazakhstan", "KZ", "Astana", "Asia/Almaty", 51.1694, 71.4491, ["kazajistan", "kazajstan"]),
    ("Israel", "IL", "Tel Aviv", "Asia/Jerusalem", 32.0853, 34.7818, []),
]
