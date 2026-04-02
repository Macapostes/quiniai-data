import requests
import json
import os

API_KEY = os.getenv('ODDS_API_KEY')
# Lista refinada: Solo España (1 y 2) y las 3 grandes de Europa
LEAGUES = [
    'soccer_spain_la_liga',
    'soccer_spain_segunda_division',
    'soccer_uefa_champs_league',
    'soccer_uefa_europa_league',
    'soccer_uefa_europa_conference_league'
]

def get_odds():
    all_odds = []
    print(f"Iniciando descarga de cuotas...")
    
    for league in LEAGUES:
        # Petición a la API
        url = f'https://api.the-odds-api.com/v4/sports/{league}/odds/?apiKey={API_KEY}&regions=eu&markets=h2h'
        try:
            r = requests.get(url)
            if r.status_code == 200:
                data = r.json()
                print(f"✅ {league}: Encontrados {len(data)} partidos.")
                all_odds.extend(data)
            else:
                print(f"❌ {league}: Error {r.status_code} - {r.text}")
        except Exception as e:
            print(f"💥 Error crítico en {league}: {e}")
    
    # Guardar el JSON final
    with open('cuotas.json', 'w', encoding='utf-8') as f:
        json.dump(all_odds, f, ensure_ascii=False, indent=2)
    print(f"PROCESO TERMINADO. Total partidos guardados: {len(all_odds)}")

if __name__ == "__main__":
    get_odds()
