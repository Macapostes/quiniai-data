import requests
import json
import os

API_KEY = os.getenv('ODDS_API_KEY')
# Ligas: Primera y Segunda de España
LEAGUES = ['soccer_spain_la_liga', 'soccer_spain_la_liga_2']

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