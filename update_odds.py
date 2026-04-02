import requests
import json
import os

API_KEY = os.getenv('ODDS_API_KEY')
# Ligas: Primera y Segunda de España
# Ligas españolas, competiciones europeas, selecciones y ligas top europeas (por si hay parón)
LEAGUES = [
    'soccer_spain_la_liga',                 # Primera División
    'soccer_spain_la_liga_2',               # Segunda División
    'soccer_uefa_champs_league',            # Champions League
    'soccer_uefa_europa_league',            # Europa League
    'soccer_uefa_europa_conference_league', # Conference League
    'soccer_uefa_nations_league',           # Selecciones (Nations League)
    'soccer_fifa_world_cup_qualification',  # Selecciones (Clasificación Mundial)
    'soccer_uefa_euro_qualification',       # Selecciones (Clasificación Eurocopa)
]

def get_odds():
    all_odds = []
    for league in LEAGUES:
        url = f'https://api.the-odds-api.com/v4/sports/{league}/odds/?apiKey={API_KEY}&regions=eu&markets=h2h'
        r = requests.get(url)
        if r.status_code == 200:
            all_odds.extend(r.json())
    
    # Guardamos el resultado en un JSON limpio
    with open('cuotas.json', 'w', encoding='utf-8') as f:
        json.dump(all_odds, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    get_odds()
