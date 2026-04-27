import atexit
import base64
import csv
import ctypes
import difflib
import email.utils
import hashlib
import html
import io
import json
import logging
import math
import os
import re
import shutil
import threading
import time
import traceback
import unicodedata
import urllib.parse
import xml.etree.ElementTree as ET
import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

RUNTIME_SITE_PACKAGES = (
    Path.home()
    / ".cache"
    / "codex-runtimes"
    / "codex-primary-runtime"
    / "dependencies"
    / "python"
    / "Lib"
    / "site-packages"
)
if RUNTIME_SITE_PACKAGES.exists() and str(RUNTIME_SITE_PACKAGES) not in sys.path:
    sys.path.append(str(RUNTIME_SITE_PACKAGES))

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None


load_dotenv()

BACKEND_URL = os.getenv(
    "QUINIAI_BACKEND_URL",
    "https://quiniela-backend-production-cb1a.up.railway.app",
).rstrip("/")
ADMIN_KEY = os.getenv("QUINIAI_ADMIN_KEY", "").strip()
POLL_SECONDS = int(os.getenv("SNAPSHOT_POLL_SECONDS", "7200"))
DATA_URL = os.getenv(
    "QUINIAI_DATA_URL",
    "https://raw.githubusercontent.com/Macapostes/quiniai-data/main/cuotas.json",
).strip()
NEWS_LANGUAGE = os.getenv("QUINIAI_NEWS_LANGUAGE", "es").strip() or "es"
NEWS_COUNTRY = os.getenv("QUINIAI_NEWS_COUNTRY", "ES").strip() or "ES"
TEAM_NEWS_ITEMS = int(os.getenv("QUINIAI_TEAM_NEWS_ITEMS", "6"))
MATCH_NEWS_ITEMS = int(os.getenv("QUINIAI_MATCH_NEWS_ITEMS", "8"))
FOCUS_MATCH_COUNT = int(os.getenv("QUINIAI_FOCUS_MATCH_COUNT", "15"))
FOCUS_TEAM_NEWS_ITEMS = int(os.getenv("QUINIAI_FOCUS_TEAM_NEWS_ITEMS", "10"))
LOCAL_MEDIA_NEWS_ITEMS = int(os.getenv("QUINIAI_LOCAL_MEDIA_NEWS_ITEMS", "8"))
MAX_WORKERS = max(2, int(os.getenv("QUINIAI_MAX_WORKERS", "6")))
HISTORY_SEASONS_BACK = max(6, int(os.getenv("QUINIAI_HISTORY_SEASONS_BACK", "10")))
UPCOMING_FIXTURE_WINDOW = max(5, int(os.getenv("QUINIAI_UPCOMING_FIXTURE_WINDOW", "5")))
NEWS_CACHE_TTL_SECONDS = int(os.getenv("QUINIAI_NEWS_CACHE_TTL_SECONDS", "21600"))
MATCH_NEWS_CACHE_TTL_SECONDS = int(
    os.getenv("QUINIAI_MATCH_NEWS_CACHE_TTL_SECONDS", "21600")
)
MONITOR_PUBLISH_MIN_SECONDS = int(os.getenv("QUINIAI_MONITOR_PUBLISH_MIN_SECONDS", "7200"))
WEATHER_CACHE_TTL_SECONDS = int(
    os.getenv("QUINIAI_WEATHER_CACHE_TTL_SECONDS", "21600")
)
HISTORY_CACHE_TTL_SECONDS = int(
    os.getenv("QUINIAI_HISTORY_CACHE_TTL_SECONDS", "43200")
)
TEAM_NEWS_MAX_AGE_DAYS = int(os.getenv("QUINIAI_TEAM_NEWS_MAX_AGE_DAYS", "10"))
MATCH_NEWS_MAX_AGE_DAYS = int(os.getenv("QUINIAI_MATCH_NEWS_MAX_AGE_DAYS", "7"))
COMPETITION_NEWS_MAX_AGE_DAYS = int(
    os.getenv("QUINIAI_COMPETITION_NEWS_MAX_AGE_DAYS", "14")
)

WIKI_API_URL = "https://en.wikipedia.org/w/api.php"
GOOGLE_NEWS_RSS_URL = "https://news.google.com/rss/search"
OPEN_METEO_GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
FOOTBALL_DATA_BASE_URL = "https://www.football-data.co.uk/mmz4281"
THESPORTSDB_SEARCH_TEAM_URL = "https://www.thesportsdb.com/api/v1/json/123/searchteams.php"
THESPORTSDB_EVENTS_NEXT_URL = "https://www.thesportsdb.com/api/v1/json/123/eventsnext.php"
THESPORTSDB_EVENTS_ROUND_URL = "https://www.thesportsdb.com/api/v1/json/123/eventsround.php"
BBC_FOOTBALL_RSS_URL = "https://feeds.bbci.co.uk/sport/football/rss.xml"
GUARDIAN_FOOTBALL_RSS_URL = "https://feeds.theguardian.com/theguardian/football/rss"
EDUARDO_QUINIELA_PORCENTAJES_URL = "https://www.eduardolosilla.es/quiniela/ayudas/porcentajes"
EDUARDO_QUINIELA_PROXIMAS_URL = "https://www.eduardolosilla.es/quiniela/ayudas/proximas"
EDUARDO_API_QUINIELISTA_URL = "https://api.eduardolosilla.es/servicios/v1/porcentajes_quinielista"
EDUARDO_API_LAE_URL = "https://api.eduardolosilla.es/servicios/v1/porcentajes_lae"
QUINIELA_ROOT_URL = EDUARDO_QUINIELA_PORCENTAJES_URL
QUINIELA_HISTORY_JORNADAS = max(5, int(os.getenv("QUINIAI_QUINIELA_HISTORY_JORNADAS", "5")))
MONITOR_PUBLIC_JORNADAS = max(2, min(5, int(os.getenv("QUINIAI_MONITOR_PUBLIC_JORNADAS", "5"))))
MONITOR_REPO = os.getenv("QUINIAI_MONITOR_REPO", "Macapostes/quiniai-data").strip()
MONITOR_BRANCH = os.getenv("QUINIAI_MONITOR_BRANCH", "main").strip() or "main"
MONITOR_PUBLISH_ENABLED = (
    os.getenv("QUINIAI_MONITOR_PUBLISH", "1").strip().lower() not in {"0", "false", "no"}
)
MONITOR_PUBLISH_INDEX = (
    os.getenv("QUINIAI_MONITOR_PUBLISH_INDEX", "0").strip().lower() in {"1", "true", "yes"}
)
MONITOR_GITHUB_TOKEN = os.getenv("QUINIAI_GITHUB_TOKEN", "").strip()
TEAM_PROFILE_CACHE_VERSION = "v4"

CACHE_DIR = Path(__file__).with_name("cache")
OUTPUT_DIR = Path(__file__).with_name("output")
LOG_DIR = Path(__file__).with_name("logs")
MONITOR_WEB_DIR = Path(__file__).with_name("docs") / "monitor"
TEAM_PROFILE_CACHE_PATH = CACHE_DIR / "team_profiles.json"
TEAM_NEWS_CACHE_PATH = CACHE_DIR / "team_news_cache.json"
MATCH_NEWS_CACHE_PATH = CACHE_DIR / "match_news_cache.json"
WEATHER_CACHE_PATH = CACHE_DIR / "weather_cache.json"
HISTORY_CACHE_PATH = CACHE_DIR / "history_cache.json"
THESPORTSDB_CACHE_PATH = CACHE_DIR / "thesportsdb_cache.json"
STRUCTURED_DB_PATH = CACHE_DIR / "structured_context_db.json"
EXTERNAL_FEEDS_CACHE_PATH = CACHE_DIR / "external_feeds_cache.json"
OFFICIAL_SITE_CACHE_PATH = CACHE_DIR / "official_site_cache.json"
RFEF_CACHE_PATH = CACHE_DIR / "rfef_cache.json"
RUN_HISTORY_PATH = CACHE_DIR / "run_history.json"
QUINIELA_HISTORY_PATH = CACHE_DIR / "quiniela_jornadas_history.json"
MONITOR_PUBLISH_STATE_PATH = CACHE_DIR / "monitor_publish_state.json"
LEGACY_SNAPSHOT_PATH = (
    Path(__file__).with_name("archive") / "pre_reorg_root_20260422" / "ia_feed_snapshot.json"
)
DESKTOP_KINII_DIR = Path.home() / "Desktop" / "Kinii"
DESKTOP_KINII_STATE_DIR = DESKTOP_KINII_DIR / "Estado"
SNAPSHOT_OUTPUT_PATH = OUTPUT_DIR / "ia_feed_snapshot.json"
STATUS_FILE_PATH = OUTPUT_DIR / "ULTIMO_ESTADO_QUINIAI.txt"
DESKTOP_STATUS_FILE_PATH = DESKTOP_KINII_STATE_DIR / "Estado QuiniAI.txt"
STATUS_JSON_PATH = OUTPUT_DIR / "ULTIMO_ESTADO_QUINIAI.json"
DESKTOP_STATUS_JSON_PATH = DESKTOP_KINII_STATE_DIR / "Estado QuiniAI.json"
STATUS_HTML_PATH = OUTPUT_DIR / "PANEL_QUINIAI.html"
DESKTOP_STATUS_HTML_PATH = DESKTOP_KINII_STATE_DIR / "Panel QuiniAI.html"
MONITOR_STATUS_JSON_PATH = MONITOR_WEB_DIR / "status.json"
MONITOR_INDEX_PATH = MONITOR_WEB_DIR / "index.html"
WORKER_LOG_PATH = LOG_DIR / "worker_events.log"
SUPERVISOR_LOG_PATH = LOG_DIR / "worker_supervisor.log"
WORKER_LOCK_PATH = CACHE_DIR / "snapshot_worker.lock"
MANUAL_REFRESH_FLAG_PATH = CACHE_DIR / "manual_refresh.flag"

DEFAULT_HEADERS = {
    "User-Agent": "QuiniAI-Context-Worker/3.0 (+https://github.com/Macapostes/quiniai-data)"
}

MADRID_TZ = ZoneInfo("Europe/Madrid")

HIGH_TRUST_NEWS_DOMAINS = {
    # ES
    "marca.com",
    "as.com",
    "relevo.com",
    "eldesmarque.com",
    "mundodeportivo.com",
    "sport.es",
    "estadiodeportivo.com",
    "superdeporte.es",
    "cope.es",
    "cadenaser.com",
    # EN/UK
    "bbc.co.uk",
    "theguardian.com",
    "skysports.com",
    "premierleague.com",
    "uefa.com",
    "fifa.com",
}

LOW_TRUST_NEWS_DOMAINS = {
    "youtube.com",
    "youtu.be",
    "tiktok.com",
    "instagram.com",
    "reddit.com",
    "twitter.com",
    "x.com",
    "facebook.com",
    "pinterest.com",
    "telegram.org",
    "whatsapp.com",
}

LEAGUE_COUNTRY_HINTS = {
    "soccer_spain_la_liga": "ES",
    "soccer_spain_segunda_division": "ES",
    "soccer_epl": "GB",
    "soccer_efl_champ": "GB",
}

LEAGUE_KEY_ALIASES = {
    # Algunas fuentes etiquetan Segunda como "LaLiga2".
    "soccer_spain_la_liga2": "soccer_spain_segunda_division",
    "soccer_spain_la_liga_2": "soccer_spain_segunda_division",
    "soccer_spain_segunda": "soccer_spain_segunda_division",
}


def _canonical_league_key(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return LEAGUE_KEY_ALIASES.get(raw, raw)


LEAGUE_FOOTBALL_DATA_CODES = {
    "soccer_spain_la_liga": "SP1",
    "soccer_spain_segunda_division": "SP2",
    "soccer_epl": "E0",
    "soccer_efl_champ": "E1",
}

LEAGUE_PRIORITY = {
    "soccer_spain_la_liga": 0,
    "soccer_spain_segunda_division": 1,
    "soccer_uefa_champs_league": 2,
    "soccer_uefa_europa_league": 3,
    "soccer_uefa_europa_conference_league": 4,
    "soccer_epl": 5,
    "soccer_efl_champ": 6,
    # Competiciones internacionales (a veces salen en quiniela)
    "soccer_fifa_world_cup": 7,
    "soccer_uefa_european_championship": 8,
    "soccer_conmebol_copa_america": 9,
    "soccer_international_friendlies": 10,
}

LEAGUE_RELEGATION_START = {
    "soccer_spain_la_liga": 18,
    "soccer_epl": 18,
    "soccer_efl_champ": 22,
    "soccer_spain_segunda_division": 19,
}

LEAGUE_COMPETITIVE_LINES = {
    "soccer_spain_la_liga": [
        {"key": "title", "label": "titulo", "line_position": 1, "direction": "top"},
        {"key": "champions", "label": "Champions", "line_position": 4, "direction": "top"},
        {"key": "europa_league", "label": "Europa League", "line_position": 5, "direction": "top"},
        {"key": "conference", "label": "Conference", "line_position": 6, "direction": "top"},
        {"key": "survival", "label": "salvacion", "line_position": 17, "direction": "survival"},
    ],
    "soccer_spain_segunda_division": [
        {"key": "direct_promotion", "label": "ascenso directo", "line_position": 2, "direction": "top"},
        {"key": "playoff", "label": "play-off", "line_position": 6, "direction": "top"},
        {"key": "survival", "label": "salvacion", "line_position": 18, "direction": "survival"},
    ],
    "soccer_efl_champ": [
        {"key": "direct_promotion", "label": "ascenso directo", "line_position": 2, "direction": "top"},
        {"key": "playoff", "label": "play-off", "line_position": 6, "direction": "top"},
        {"key": "survival", "label": "salvacion", "line_position": 21, "direction": "survival"},
    ],
}

LEAGUE_RFEF_PDF_PREFIX = {
    "soccer_spain_la_liga": "1a_division_masculina",
    "soccer_spain_segunda_division": "2a_division_masculina",
}

TEAM_NAME_ALIASES = {
    "athletic de bilbao": "Athletic Bilbao",
    "athletic club": "Athletic Bilbao",
    "athletic bilbao": "Athletic Bilbao",
    "club atletico osasuna": "CA Osasuna",
    "osasuna": "CA Osasuna",
    "real madrid": "Real Madrid",
    "alaves": "Alavés",
    "deportivo alaves": "Alavés",
    "atletico de madrid": "Atlético Madrid",
    "real sociedad": "Real Sociedad",
    "queens park": "Queens Park Rangers",
    "queens park rangers": "Queens Park Rangers",
    "swansea": "Swansea City",
    "southamton": "Southampton",
    "southampton": "Southampton",
    "leicestein": "Leicester City",
    "leicester": "Leicester City",
    "hull": "Hull City",
    "r madrid": "Real Madrid",
    "r sociedad": "Real Sociedad",
    "at madrid": "Atlético Madrid",
    "ath club": "Athletic Bilbao",
    "ath bilbao": "Athletic Bilbao",
    "r betis": "Real Betis",
    "real betis": "Real Betis",
    "r zaragoza": "Real Zaragoza",
    "r oviedo": "Real Oviedo",
    "oviedo": "Real Oviedo",
    "real oviedo": "Real Oviedo",
    "elche": "Elche CF",
    "elche cf": "Elche CF",
    "dep coruna": "Deportivo La Coruña",
    "dep la coruna": "Deportivo La Coruña",
    "qpr": "Queens Park Rangers",
    "swans": "Swansea City",
    "coventry": "Coventry City",
    "portsmouth": "Portsmouth",
    "mirandes": "Mirandés",
    "castellon": "Castellón",
    "rayo v": "Rayo Vallecano",
    "athletic de bilbao": "Athletic Bilbao",
}

LEAGUE_EXTERNAL_FEEDS = {
    "soccer_spain_la_liga": [
        {"name": "AS Primera", "url": "https://futbol.as.com/rss/futbol/primera.xml"},
        {"name": "AS Futbol", "url": "https://as.com/rss/futbol/portada.xml"},
        {"name": "Google News La Liga", "url": ""},
    ],
    "soccer_spain_segunda_division": [
        {"name": "AS Segunda", "url": "https://futbol.as.com/rss/futbol/segunda.xml"},
        {"name": "AS Futbol", "url": "https://as.com/rss/futbol/portada.xml"},
        {"name": "Google News Segunda", "url": ""},
    ],
    "soccer_uefa_champs_league": [
        {"name": "AS Champions", "url": "https://as.com/rss/futbol/champions.xml"},
        {"name": "BBC Football", "url": BBC_FOOTBALL_RSS_URL},
        {"name": "Guardian Football", "url": GUARDIAN_FOOTBALL_RSS_URL},
        {"name": "Google News Champions", "url": ""},
    ],
    "soccer_uefa_europa_league": [
        {"name": "AS UEFA", "url": "https://as.com/rss/futbol/uefa.xml"},
        {"name": "BBC Football", "url": BBC_FOOTBALL_RSS_URL},
        {"name": "Guardian Football", "url": GUARDIAN_FOOTBALL_RSS_URL},
        {"name": "Google News Europa", "url": ""},
    ],
    "soccer_uefa_europa_conference_league": [
        {"name": "AS UEFA", "url": "https://as.com/rss/futbol/uefa.xml"},
        {"name": "BBC Football", "url": BBC_FOOTBALL_RSS_URL},
        {"name": "Guardian Football", "url": GUARDIAN_FOOTBALL_RSS_URL},
        {"name": "Google News Conference", "url": ""},
    ],
    "soccer_epl": [
        {"name": "BBC Football", "url": BBC_FOOTBALL_RSS_URL},
        {"name": "Guardian Football", "url": GUARDIAN_FOOTBALL_RSS_URL},
        {"name": "Google News Premier League", "url": ""},
    ],
    "soccer_efl_champ": [
        {"name": "BBC Football", "url": BBC_FOOTBALL_RSS_URL},
        {"name": "Guardian Football", "url": GUARDIAN_FOOTBALL_RSS_URL},
        {"name": "Google News Championship", "url": ""},
    ],
    "soccer_fifa_world_cup": [
        {"name": "BBC Football", "url": BBC_FOOTBALL_RSS_URL},
        {"name": "Guardian Football", "url": GUARDIAN_FOOTBALL_RSS_URL},
        {"name": "Google News World Cup", "url": ""},
    ],
    "soccer_uefa_european_championship": [
        {"name": "BBC Football", "url": BBC_FOOTBALL_RSS_URL},
        {"name": "Guardian Football", "url": GUARDIAN_FOOTBALL_RSS_URL},
        {"name": "Google News EURO", "url": ""},
    ],
    "soccer_conmebol_copa_america": [
        {"name": "BBC Football", "url": BBC_FOOTBALL_RSS_URL},
        {"name": "Guardian Football", "url": GUARDIAN_FOOTBALL_RSS_URL},
        {"name": "Google News Copa America", "url": ""},
    ],
    "soccer_international_friendlies": [
        {"name": "BBC Football", "url": BBC_FOOTBALL_RSS_URL},
        {"name": "Guardian Football", "url": GUARDIAN_FOOTBALL_RSS_URL},
        {"name": "Google News International friendlies", "url": ""},
    ],
}

LEAGUE_NEWS_SEARCH_TERMS = {
    "soccer_spain_la_liga": "La Liga Spain football",
    "soccer_spain_segunda_division": "Segunda Division Spain football",
    "soccer_uefa_champs_league": "UEFA Champions League football",
    "soccer_uefa_europa_league": "UEFA Europa League football",
    "soccer_uefa_europa_conference_league": "UEFA Conference League football",
    "soccer_epl": "Premier League football",
    "soccer_efl_champ": "EFL Championship football",
    "soccer_fifa_world_cup": "FIFA World Cup football",
    "soccer_uefa_european_championship": "UEFA European Championship football",
    "soccer_conmebol_copa_america": "Copa America football",
    "soccer_international_friendlies": "International friendlies football",
}

COUNTRY_LABELS = {
    "ES": "Spain",
    "GB": "England",
}

LOCAL_MEDIA_SOURCE_TOKENS = [
    "marca",
    "as",
    "diario as",
    "relevo",
    "eldesmarque",
    "mundodeportivo",
    "sport",
    "estadio deportivo",
    "estadiodeportivo",
    "superdeporte",
    "tribuna deportiva",
    "plaza deportiva",
    "cope",
    "cadena ser",
    "abc",
    "diario de sevilla",
    "faro de vigo",
    "ideal",
    "heraldo",
    "bbc",
    "guardian",
    "coventry live",
    "portsmouth news",
    "hampshire live",
    "wales online",
    "south wales evening post",
    "west london sport",
    "daily echo",
    "la nueva espana",
    "la nueva españa",
    "el comercio",
    "radio marca",
    "cope",
]

TEAM_NEWS_QUERY_HINTS = {
    "valencia": ['"Valencia CF"', '"Valencia Club de Futbol"', '"Valencia" futbol'],
    "mallorca": ['"RCD Mallorca"', '"Mallorca" futbol', '"Mallorca" Laliga'],
    "barcelona": ['"FC Barcelona"', '"Barcelona" futbol'],
    "girona": ['"Girona FC"', '"Girona" futbol'],
    "sevilla": ['"Sevilla FC"', '"Sevilla" futbol'],
    "betis": ['"Real Betis"', '"Betis" futbol'],
    "espanyol": ['"RCD Espanyol"', '"Espanyol" futbol'],
    "levante": ['"Levante UD"', '"Levante" futbol'],
    "oviedo": ['"Real Oviedo"', '"Oviedo" futbol'],
    "zaragoza": ['"Real Zaragoza"', '"Zaragoza" futbol'],
    "coventry city": ['"Coventry City"', '"Coventry City FC"', '"Coventry City" football'],
    "portsmouth": ['"Portsmouth FC"', '"Portsmouth" football', '"Pompey" football'],
    "queens park rangers": ['"Queens Park Rangers"', '"QPR" football'],
    "swansea city": ['"Swansea City"', '"Swans" football'],
    "southampton": ['"Southampton FC"', '"Southampton" football'],
    "bristol city": ['"Bristol City"', '"Bristol City" football'],
    "charlton athletic": ['"Charlton Athletic"', '"Charlton" football'],
    "hull city": ['"Hull City"', '"Hull City" football'],
    "watford": ['"Watford FC"', '"Watford" football'],
    "wrexham": ['"Wrexham AFC"', '"Wrexham" football'],
    "real oviedo": ['"Real Oviedo"', '"Oviedo" futbol'],
    "elche cf": ['"Elche CF"', '"Elche" futbol'],
}

TEAM_LOCAL_MEDIA_HINTS = {
    "coventry city": ["coventry live", "bbc sport", "bbc coventry"],
    "portsmouth": ["portsmouth news", "hampshire live", "bbc sport"],
    "queens park rangers": ["west london sport", "bbc sport london", "qpr"],
    "swansea city": ["wales online", "bbc sport wales", "south wales evening post"],
    "southampton": ["daily echo", "hampshire live", "bbc sport"],
    "bristol city": ["bristol live", "bbc sport"],
    "charlton athletic": ["south london press", "bbc sport"],
    "real oviedo": ["la nueva espana", "la nueva españa", "el comercio"],
    "real zaragoza": ["heraldo", "el periodico de aragon"],
    "levante": ["superdeporte", "plaza deportiva"],
    "elche cf": ["informacion", "marca", "as"],
}

TEAM_LOCATION_OVERRIDES = {
    "mallorca": {"query": "Palma de Mallorca, Spain", "city": "Palma", "country": "Spain", "country_code": "ES", "timezone": "Europe/Madrid", "latitude": 39.5696, "longitude": 2.6502},
    "valencia": {"query": "Valencia, Spain", "city": "Valencia", "country": "Spain", "country_code": "ES", "timezone": "Europe/Madrid", "latitude": 39.4699, "longitude": -0.3763},
    "girona": {"query": "Girona, Spain", "city": "Girona", "country": "Spain", "country_code": "ES", "timezone": "Europe/Madrid", "latitude": 41.9794, "longitude": 2.8214},
    "real oviedo": {"query": "Oviedo, Asturias, Spain", "city": "Oviedo", "country": "Spain", "country_code": "ES", "timezone": "Europe/Madrid", "latitude": 43.3614, "longitude": -5.8494},
    "real zaragoza": {"query": "Zaragoza, Spain", "city": "Zaragoza", "country": "Spain", "country_code": "ES", "timezone": "Europe/Madrid", "latitude": 41.6488, "longitude": -0.8891},
    "levante": {"query": "Valencia, Spain", "city": "Valencia", "country": "Spain", "country_code": "ES", "timezone": "Europe/Madrid", "latitude": 39.4699, "longitude": -0.3763},
    "elche cf": {"query": "Elche, Alicante, Spain", "city": "Elche", "country": "Spain", "country_code": "ES", "timezone": "Europe/Madrid", "latitude": 38.2699, "longitude": -0.7126},
    "rayo vallecano": {"query": "Vallecas, Madrid, Spain", "city": "Madrid", "country": "Spain", "country_code": "ES", "timezone": "Europe/Madrid", "latitude": 40.3919, "longitude": -3.6588},
    "real sociedad": {"query": "San Sebastian, Gipuzkoa, Spain", "city": "San Sebastian", "country": "Spain", "country_code": "ES", "timezone": "Europe/Madrid", "latitude": 43.3183, "longitude": -1.9812},
    "villarreal": {"query": "Villarreal, Castellon, Spain", "city": "Villarreal", "country": "Spain", "country_code": "ES", "timezone": "Europe/Madrid", "latitude": 39.9383, "longitude": -0.1009},
    "mirandes": {"query": "Miranda de Ebro, Burgos, Spain"},
    "mirandes": {"query": "Miranda de Ebro, Burgos, Spain"},
    "castellon": {"query": "Castellon de la Plana, Spain"},
    "castellón": {"query": "Castellon de la Plana, Spain"},
    "alaves": {"query": "Vitoria-Gasteiz, Spain"},
    "alavés": {"query": "Vitoria-Gasteiz, Spain"},
    "espanyol": {"query": "Cornella de Llobregat, Barcelona, Spain"},
    "coventry city": {"query": "Coventry, England"},
    "portsmouth": {"query": "Portsmouth, Hampshire, England"},
    "queens park rangers": {"query": "Shepherds Bush, London, England"},
    "swansea city": {"query": "Swansea, Wales"},
    "stoke city": {"query": "Stoke-on-Trent, England"},
    "southampton": {"query": "Southampton, England"},
    "bristol city": {"query": "Bristol, England"},
    "charlton athletic": {"query": "Charlton, London, England"},
    "hull city": {"query": "Kingston upon Hull, England"},
    "watford": {"query": "Watford, Hertfordshire, England"},
    "wrexham": {"query": "Wrexham, Wales"},
    "oxford united": {"query": "Oxford, England"},
    "sheffield wednesday": {"query": "Sheffield, England"},
    "middlesbrough": {"query": "Middlesbrough, England"},
    "burnley": {"query": "Burnley, Lancashire, England"},
    "derby county": {"query": "Derby, England"},
    "plymouth argyle": {"query": "Plymouth, England"},
    "cardiff city": {"query": "Cardiff, Wales"},
    "birmingham city": {"query": "Birmingham, England"},
}

AMBIGUOUS_GEO_TEAM_TOKENS = {
    "valencia",
    "mallorca",
    "barcelona",
    "girona",
    "sevilla",
    "zaragoza",
    "oviedo",
    "levante",
}

INJURY_KEYWORDS = [
    "injury",
    "injured",
    "lesion",
    "lesionado",
    "baja",
    "out",
    "doubt",
    "duda",
    "suspension",
    "sancion",
    "absence",
]
ROTATION_KEYWORDS = [
    "rotation",
    "rotacion",
    "rotación",
    "rotar",
    "rotaciones",
    "rested",
    "rest",
    "descanso",
    "fatigue",
    "fatiga",
    "congestion",
    "fixture",
    "schedule",
    "semifinal",
    "semi-final",
    "champions",
    "europa league",
    "conference league",
]
DISCIPLINE_KEYWORDS = [
    "referee",
    "arbitro",
    "penalty",
    "penalti",
    "red card",
    "tarjeta roja",
    "suspension",
    "sancion",
]
PRESS_CONFERENCE_KEYWORDS = [
    "rueda de prensa",
    "press conference",
    "comparecencia",
    "declara",
    "declaraciones",
    "dijo",
    "coach",
    "entrenador",
    "míster",
]
SQUAD_KEYWORDS = [
    "convocatoria",
    "squad",
    "lista",
    "called up",
    "once probable",
    "alineacion",
    "lineup",
    "line-up",
]
MORALE_KEYWORDS = [
    "crisis",
    "vestuario",
    "moral",
    "mal clima",
    "tension",
    "tensión",
    "racha",
    "presion",
    "presión",
    "presión",
    "salvacion",
    "salvación",
    "salvación",
    "descenso",
    "playoff",
    "ascenso",
    "title race",
    "objetivo",
]
NOISE_FORMAT_KEYWORDS = [
    "foto",
    "fotos",
    "galeria",
    "galería",
    "imagenes",
    "imágenes",
    "resumen",
    "highlights",
    "resultado",
    "crónica",
    "cronica",
    "uno por uno",
    "player ratings",
    "alineaciones",
    "where to watch",
    "donde ver",
    "horario",
    "canal tv",
    "tv",
    "streaming",
    "live stream",
    "live streaming",
    "watch live",
    "directo",
    "ver en vivo",
    "a que hora",
    "a qué hora",
    "canal",
    "minuto a minuto",
    "live blog",
]
NON_PREDICTIVE_NOISE_KEYWORDS = [
    "fantasy",
    "apuestas",
    "betting",
    "cuotas",
    "odds",
    "pronostico",
    "pronóstico",
    "prediccion",
    "predicción",
    "women",
    "femenino",
    "femenina",
    "basket",
    "basketball",
    "valencia basket",
    "earthquake",
    "terremoto",
    "terremotos",
    "travel",
    "turismo",
]
LOW_INFORMATION_SOURCE_TOKENS = [
    "onefootball",
    "fotmob",
    "futbol24",
    "ysscores",
    "transfermarkt",
    "365scores",
    "besoccer",
    "soccerway",
    "sportmole",
]
HIGH_TRUST_SOURCE_TOKENS = [
    "bbc",
    "guardian",
    "marca",
    "as",
    "relevo",
    "eldesmarque",
    "cope",
    "cadena ser",
    "superdeporte",
    "coventry live",
    "portsmouth news",
    "hampshire live",
    "wales online",
    "south wales evening post",
    "daily echo",
    "west london sport",
    "la nueva espana",
    "la nueva españa",
    "heraldo",
]
EUROPE_KEYWORDS = [
    "champions league",
    "europa league",
    "conference league",
    "ucl",
    "uefa",
]
WEATHER_KEYWORDS = [
    "weather",
    "rain",
    "wind",
    "storm",
    "snow",
    "clima",
    "lluvia",
    "viento",
    "tormenta",
    "nieve",
]

CACHE_LOCK = threading.Lock()


def _ensure_cache_dir() -> None:
    CACHE_DIR.mkdir(exist_ok=True)
    OUTPUT_DIR.mkdir(exist_ok=True)
    LOG_DIR.mkdir(exist_ok=True)
    DESKTOP_KINII_DIR.mkdir(parents=True, exist_ok=True)
    DESKTOP_KINII_STATE_DIR.mkdir(parents=True, exist_ok=True)


def _load_cache(path: Path) -> dict:
    _ensure_cache_dir()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


TEAM_PROFILE_CACHE = _load_cache(TEAM_PROFILE_CACHE_PATH)
TEAM_NEWS_CACHE = _load_cache(TEAM_NEWS_CACHE_PATH)
MATCH_NEWS_CACHE = _load_cache(MATCH_NEWS_CACHE_PATH)
WEATHER_CACHE = _load_cache(WEATHER_CACHE_PATH)
HISTORY_CACHE = _load_cache(HISTORY_CACHE_PATH)
THESPORTSDB_CACHE = _load_cache(THESPORTSDB_CACHE_PATH)
EXTERNAL_FEEDS_CACHE = _load_cache(EXTERNAL_FEEDS_CACHE_PATH)
OFFICIAL_SITE_CACHE = _load_cache(OFFICIAL_SITE_CACHE_PATH)
RFEF_CACHE = _load_cache(RFEF_CACHE_PATH)
STRUCTURED_DB = _load_cache(STRUCTURED_DB_PATH) or {
    "teams": {},
    "matches": {},
    "referees": {},
    "meta": {},
}
RUN_HISTORY = _load_cache(RUN_HISTORY_PATH) or {"runs": []}
QUINIELA_HISTORY = _load_cache(QUINIELA_HISTORY_PATH) or {"season": None, "current_jornada": None, "jornadas": {}}
MONITOR_PUBLISH_STATE = _load_cache(MONITOR_PUBLISH_STATE_PATH) or {"files": {}}
LEGACY_SNAPSHOT = _load_cache(LEGACY_SNAPSHOT_PATH) if LEGACY_SNAPSHOT_PATH.exists() else {}


def _save_cache(path: Path, payload: dict) -> None:
    _ensure_cache_dir()
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _flush_caches() -> None:
    with CACHE_LOCK:
        _save_cache(TEAM_PROFILE_CACHE_PATH, TEAM_PROFILE_CACHE)
        _save_cache(TEAM_NEWS_CACHE_PATH, TEAM_NEWS_CACHE)
        _save_cache(MATCH_NEWS_CACHE_PATH, MATCH_NEWS_CACHE)
        _save_cache(WEATHER_CACHE_PATH, WEATHER_CACHE)
        _save_cache(HISTORY_CACHE_PATH, HISTORY_CACHE)
        _save_cache(THESPORTSDB_CACHE_PATH, THESPORTSDB_CACHE)
        _save_cache(EXTERNAL_FEEDS_CACHE_PATH, EXTERNAL_FEEDS_CACHE)
        _save_cache(OFFICIAL_SITE_CACHE_PATH, OFFICIAL_SITE_CACHE)
        _save_cache(RFEF_CACHE_PATH, RFEF_CACHE)
        _save_cache(STRUCTURED_DB_PATH, STRUCTURED_DB)
        _save_cache(RUN_HISTORY_PATH, RUN_HISTORY)
        _save_cache(QUINIELA_HISTORY_PATH, QUINIELA_HISTORY)
        _save_cache(MONITOR_PUBLISH_STATE_PATH, MONITOR_PUBLISH_STATE)


def _build_logger() -> logging.Logger:
    logger = logging.getLogger("quiniai-worker")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(
        WORKER_LOG_PATH,
        maxBytes=2_000_000,
        backupCount=4,
        encoding="utf-8",
    )
    handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    )
    logger.addHandler(handler)
    logger.propagate = False
    return logger


LOGGER = _build_logger()
LOCK_FD: int | None = None


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(  # type: ignore[attr-defined]
            PROCESS_QUERY_LIMITED_INFORMATION,
            False,
            pid,
        )
        if not handle:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)  # type: ignore[attr-defined]
        return True
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _release_worker_lock() -> None:
    global LOCK_FD
    if LOCK_FD is not None:
        try:
            os.close(LOCK_FD)
        except OSError:
            pass
        LOCK_FD = None
    try:
        if WORKER_LOCK_PATH.exists():
            lock_payload = json.loads(WORKER_LOCK_PATH.read_text(encoding="utf-8"))
            if _safe_int(lock_payload.get("pid")) == os.getpid():
                WORKER_LOCK_PATH.unlink(missing_ok=True)
    except Exception:
        pass


def _acquire_worker_lock() -> None:
    global LOCK_FD
    _ensure_cache_dir()
    if LOCK_FD is not None:
        return
    try:
        LOCK_FD = os.open(str(WORKER_LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        try:
            existing = json.loads(WORKER_LOCK_PATH.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
        existing_pid = _safe_int(existing.get("pid"), 0) or 0
        if existing_pid and _pid_is_alive(existing_pid):
            raise SystemExit(
                f"Otro snapshot_worker.py ya esta en ejecucion (pid={existing_pid})."
            )
        started_at = _parse_iso_datetime(str(existing.get("started_at", "")).strip())
        if existing_pid and started_at:
            lock_age = (datetime.now(timezone.utc) - started_at).total_seconds()
            if lock_age < max(POLL_SECONDS * 2, 6 * 3600):
                raise SystemExit(
                    f"Existe un lock reciente de snapshot_worker.py (pid={existing_pid}); "
                    "no se arranca otra instancia para evitar duplicados."
                )
        try:
            WORKER_LOCK_PATH.unlink(missing_ok=True)
        except Exception:
            pass
        LOCK_FD = os.open(str(WORKER_LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    os.write(
        LOCK_FD,
        json.dumps(
            {"pid": os.getpid(), "started_at": _now_iso(), "poll_seconds": POLL_SECONDS},
            ensure_ascii=False,
        ).encode("utf-8"),
    )
    os.fsync(LOCK_FD)
    atexit.register(_release_worker_lock)


def _consume_manual_refresh_flag() -> bool:
    if not MANUAL_REFRESH_FLAG_PATH.exists():
        return False
    try:
        MANUAL_REFRESH_FLAG_PATH.unlink(missing_ok=True)
    except Exception:
        pass
    return True


def _append_run_history(entry: dict) -> None:
    runs = list((RUN_HISTORY or {}).get("runs", []))
    runs.append(entry)
    RUN_HISTORY["runs"] = runs[-40:]


def _persist_run_history() -> None:
    _save_cache(RUN_HISTORY_PATH, RUN_HISTORY)


def _log_cycle_event(level: str, message: str, **context) -> None:
    payload = {"message": message}
    if context:
        payload.update(context)
    line = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    if level == "error":
        LOGGER.error(line)
    elif level == "warning":
        LOGGER.warning(line)
    else:
        LOGGER.info(line)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_datetime(value: str) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _parse_match_date(value: str) -> datetime | None:
    if not value:
        return None
    for fmt in ["%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d/%m/%Y %H:%M", "%d/%m/%y %H:%M"]:
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _parse_published_at(value: str) -> datetime | None:
    if not value:
        return None
    normalized = str(value).strip()
    try:
        parsed = email.utils.parsedate_to_datetime(normalized)
    except (TypeError, ValueError, IndexError):
        parsed = _parse_iso_datetime(normalized)
    if not parsed:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _weekday_token_es(date_value: datetime) -> str:
    mapping = {
        0: "lunes",
        1: "martes",
        2: "miercoles",
        3: "jueves",
        4: "viernes",
        5: "sabado",
        6: "domingo",
    }
    return mapping.get(date_value.weekday(), "")


def _season_tag_for(date_value: datetime | None = None) -> str:
    current = date_value or datetime.now(timezone.utc)
    start_year = current.year if current.month >= 7 else current.year - 1
    return f"{start_year}-{(start_year + 1) % 100:02d}"


def _request_json(url: str, params: dict | None = None, timeout: int = 30) -> dict | list:
    response = requests.get(url, params=params, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _request_text(url: str, params: dict | None = None, timeout: int = 30) -> str:
    response = requests.get(url, params=params, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def _format_madrid_datetime(value: object, include_tz: bool = True) -> str:
    text = str(value or "").strip()
    if not text:
        return "-"
    parsed = _parse_iso_datetime(text)
    if not parsed:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    local_dt = parsed.astimezone(MADRID_TZ)
    suffix = " (Madrid)" if include_tz else ""
    return local_dt.strftime("%d/%m/%Y %H:%M:%S") + suffix


def _slugify_team_name(value: object) -> str:
    normalized = _normalize_ascii(str(value or "")).lower()
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")
    return normalized or "team"


def _cache_get(cache: dict, key: str, ttl_seconds: int | None = None):
    with CACHE_LOCK:
        entry = cache.get(key)
    if not entry:
        return None
    if ttl_seconds is None:
        return entry.get("data")
    fetched_at = _parse_iso_datetime(entry.get("fetched_at", ""))
    if not fetched_at:
        return None
    age_seconds = (datetime.now(timezone.utc) - fetched_at).total_seconds()
    if age_seconds > ttl_seconds:
        return None
    return entry.get("data")


def _cache_set(cache: dict, key: str, data) -> None:
    with CACHE_LOCK:
        cache[key] = {"fetched_at": _now_iso(), "data": data}


def _safe_int(value: object, default: int | None = None) -> int | None:
    try:
        text = str(value).strip()
        if not text:
            return default
        return int(float(text))
    except (TypeError, ValueError):
        return default


def _safe_float(value: object, default: float | None = None) -> float | None:
    try:
        text = str(value).strip().replace(",", ".")
        if not text:
            return default
        return float(text)
    except (TypeError, ValueError):
        return default


def _json_clone(value):
    try:
        return json.loads(json.dumps(value, ensure_ascii=False))
    except Exception:
        return value


def _match_key(league: str, home_team: str, away_team: str, kickoff: str) -> str:
    return "|".join(
        [
            league.strip().lower(),
            _normalize_team_name(home_team),
            _normalize_team_name(away_team),
            kickoff.strip(),
        ]
    )


def _extract_person_candidates(text: str) -> list[str]:
    pattern = r"\b[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){0,2}\b"
    candidates = []
    for candidate in re.findall(pattern, text):
        cleaned = candidate.strip()
        if len(cleaned) < 4:
            continue
        candidates.append(cleaned)
    deduped = []
    for candidate in candidates:
        if candidate not in deduped:
            deduped.append(candidate)
    return deduped


def _news_age_days(value: str) -> float | None:
    published_dt = _parse_published_at(value)
    if not published_dt:
        return None
    return max(0.0, (datetime.now(timezone.utc) - published_dt).total_seconds() / 86400.0)


def _headline_recent_enough(item: dict, max_age_days: int) -> bool:
    published_at = str(item.get("published_at", "")).strip()
    if not published_at:
        return True
    age_days = _news_age_days(published_at)
    if age_days is None:
        return True
    return age_days <= max_age_days


def _team_relevance_score(title: str, team_name: str) -> float:
    title_norm = _normalize_team_name(title)
    team_norm = _normalize_team_name(team_name)
    if not title_norm or not team_norm:
        return 0.0
    title_tokens = set(title_norm.split())
    team_tokens = set(team_norm.split())
    if not title_tokens or not team_tokens:
        return 0.0
    overlap = len(title_tokens & team_tokens)
    if overlap == 0:
        return 0.0
    return max(overlap / len(team_tokens), _team_similarity_score(title, team_name))


def _match_relevance_score(title: str, home_team: str, away_team: str) -> float:
    home_score = _team_relevance_score(title, home_team)
    away_score = _team_relevance_score(title, away_team)
    if home_score <= 0 or away_score <= 0:
        return 0.0
    return round((home_score + away_score) / 2, 4)


def _dedupe_news_items(items: list[dict]) -> list[dict]:
    deduped = []
    seen = set()
    for item in items:
        key = (
            _normalize_ascii(str(item.get("title", "")).lower()),
            str(item.get("link", "")).strip(),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _sort_news_items(items: list[dict]) -> list[dict]:
    def _key(item: dict):
        age_days = _news_age_days(str(item.get("published_at", "")).strip())
        freshness = age_days if age_days is not None else 9999.0
        return (
            -float(item.get("_signal", 0.0) or 0.0),
            -float(item.get("_relevance", 0.0) or 0.0),
            freshness,
            str(item.get("title", "")),
        )

    return sorted(items, key=_key)


def _clean_news_items(items: list[dict], max_age_days: int, limit: int) -> list[dict]:
    filtered = [_enrich_news_item(item) for item in items if _headline_recent_enough(item, max_age_days)]
    ordered = _sort_news_items(_dedupe_news_items(filtered))
    cleaned = []
    for item in ordered[:limit]:
        entry = dict(item)
        entry.pop("_relevance", None)
        entry.pop("_domain", None)
        cleaned.append(entry)
    return cleaned


def _competition_relevance_score(item: dict, league_key: str, league_teams: list[str] | None = None) -> float:
    title = str(item.get("title", "")).strip()
    source = str(item.get("source", "")).strip().lower()
    domain = _safe_url_host(str(item.get("link", "")).strip())
    stop_tokens = {"football", "soccer", "league", "spain"}
    league_terms = [
        token
        for token in _normalize_team_name(LEAGUE_NEWS_SEARCH_TERMS.get(league_key, "")).split()
        if token not in stop_tokens
    ]
    title_norm = _normalize_team_name(title)
    token_bonus = sum(1 for token in league_terms if token and token in title_norm)
    team_bonus = 0.0
    for team_name in league_teams or []:
        team_bonus = max(team_bonus, _team_relevance_score(title, team_name))
    source_bonus = 0.0
    if source in {"uefa.com", "bbc football", "guardian football"} or source.startswith("as "):
        source_bonus = 0.2
    if domain and domain in HIGH_TRUST_NEWS_DOMAINS:
        source_bonus = max(source_bonus, 0.25)
    if domain and domain in LOW_TRUST_NEWS_DOMAINS:
        return 0.0
    base_score = token_bonus + team_bonus
    if base_score <= 0:
        return 0.0
    return round(base_score + source_bonus, 4)


def _is_low_signal_source(source_name: str) -> bool:
    normalized = str(source_name).strip().lower()
    return any(
        token in normalized
        for token in [
            "oddschecker",
            "sofascore",
            "flashscore",
            "bet",
            "apuestas",
            "wincomparator",
            "apwin",
            "sportytrader",
            "futbolfantasy",
            "youtube",
            "tiktok",
            "instagram",
            "reddit",
            "twitter",
            "x.com",
            "pinterest",
            "facebook",
            "telegram",
            "whatsapp",
        ]
    )


def _is_low_information_source(source_name: str) -> bool:
    normalized = _normalize_ascii(str(source_name).strip()).lower()
    return any(token in normalized for token in LOW_INFORMATION_SOURCE_TOKENS)


def _is_high_trust_source(source_name: str) -> bool:
    normalized = _normalize_ascii(str(source_name).strip()).lower()
    return any(token in normalized for token in HIGH_TRUST_SOURCE_TOKENS)


def _looks_like_hard_signal_news(title: str, source: str = "") -> bool:
    haystack = f"{title} {source}"
    lowered = _normalize_ascii(haystack).lower()
    soft_false_positives = [
        "predicted line-up",
        "predicted lineup",
        "probable lineup",
        "highlights",
        "resumen",
        "result",
        "resultado",
    ]
    if any(token in lowered for token in soft_false_positives):
        return False
    hard_tokens = [
        "medical update",
        "parte medico",
        "convocatoria",
        "called up",
        "press conference",
        "rueda de prensa",
        "suspension",
        "sancion",
        "injury",
        "lesion",
        "ruled out",
        "will miss",
        "miss remainder of the season",
        "season-ending",
        "fitness test",
        "aggravated injury",
        "doubt for",
        "duda para",
        "banned",
        "sanctioned",
    ]
    return _contains_any(lowered, hard_tokens)


def _is_generic_preview_title(title: str) -> bool:
    lowered = _normalize_ascii(title).lower()
    preview_tokens = [
        "horario y donde ver",
        "horario",
        "where to watch",
        "pronostico",
        "predictions",
        "cuotas",
        "odds",
        "alineaciones probables",
        "probable lineup",
        "previa",
        "preview",
        "en directo",
        "live",
        "como ver",
        "donde ver",
        "canal",
        "tv",
    ]
    important_tokens = [
        "lesion",
        "injury",
        "suspension",
        "sancion",
        "baja",
        "duda",
        "rueda de prensa",
        "coach",
        "entrenador",
        "crisis",
        "problem",
        "moral",
        "banquillo",
        "convocatoria",
        "referee",
        "arbitro",
    ]
    return any(token in lowered for token in preview_tokens) and not any(
        token in lowered for token in important_tokens
    )


def _is_non_match_noise_title(title: str) -> bool:
    lowered = _normalize_ascii(title).lower()
    noise_tokens = [
        "baloncesto",
        "basket",
        "basquet",
        "euroliga",
        "futsal",
        "campus",
        "ciclistas",
        "ciclismo",
        "rutas guiadas",
        "turismo",
        "inmigrantes",
        "consejo de ministros",
        "mediodia cope",
        "fundacion",
        "foundation",
        "museo",
        "patrocin",
    ]
    signal_tokens = [
        "football",
        "futbol",
        "partido",
        "match",
        "lesion",
        "injury",
        "baja",
        "convocatoria",
        "rueda de prensa",
        "arbitro",
        "referee",
        "alineacion",
        "descenso",
    ]
    return any(token in lowered for token in noise_tokens) and not any(
        token in lowered for token in signal_tokens
    )


def _contains_any(text: str, keywords: list[str]) -> bool:
    lowered = _normalize_ascii(text).lower()
    return any(keyword in lowered for keyword in keywords)


def _signal_strength_score(title: str, source: str = "") -> float:
    haystack = f"{title} {source}"
    score = 0.0
    if _contains_any(haystack, INJURY_KEYWORDS):
        score += 3.0
    if _contains_any(haystack, DISCIPLINE_KEYWORDS):
        score += 2.5
    if _contains_any(haystack, ROTATION_KEYWORDS):
        score += 2.0
    if _contains_any(haystack, PRESS_CONFERENCE_KEYWORDS):
        score += 1.7
    if _contains_any(haystack, SQUAD_KEYWORDS):
        score += 1.7
    if _contains_any(haystack, MORALE_KEYWORDS):
        score += 1.8
    return round(score, 2)


def _team_query_terms(team_name: str) -> str:
    normalized = _normalize_team_name(team_name)
    hints = TEAM_NEWS_QUERY_HINTS.get(normalized) or TEAM_NEWS_QUERY_HINTS.get(normalized.split()[0] if normalized else "")
    if hints:
        return " OR ".join(hints)
    return f'"{team_name}"'


def _requires_football_context(team_name: str) -> bool:
    normalized = _normalize_team_name(team_name)
    return normalized in AMBIGUOUS_GEO_TEAM_TOKENS


def _has_football_context(title: str, source: str = "") -> bool:
    haystack = f"{title} {source}"
    football_tokens = [
        "football",
        "futbol",
        "fútbol",
        "laliga",
        "segunda",
        "championship",
        "liga",
        "partido",
        "match",
        "cf",
        "fc",
        "club",
        "entrenador",
        "rueda de prensa",
        "convocatoria",
    ]
    return _contains_any(haystack, football_tokens) or _signal_strength_score(title, source) > 0


def _is_low_value_result_story(title: str) -> bool:
    lowered = _normalize_ascii(title).lower()
    result_tokens = [
        "empate",
        "victoria",
        "derrota",
        "goles",
        "resumen",
        "resultado",
        "cronica",
        "crónica",
        "uno por uno",
        "player ratings",
        "al descanso",
        "final del partido",
        "1-0",
        "1-1",
        "2-0",
        "0-0",
    ]
    return any(token in lowered for token in result_tokens) and _signal_strength_score(title, "") < 2.5


def _passes_team_news_quality(item: dict, team_name: str, require_signal: bool = False) -> bool:
    title = str(item.get("title", "")).strip()
    source = str(item.get("source", "")).strip()
    domain = _safe_url_host(str(item.get("link", "")).strip())
    if not title:
        return False
    if _is_low_signal_source(source):
        return False
    if domain and domain in LOW_TRUST_NEWS_DOMAINS:
        return False
    if _is_generic_preview_title(title) or _is_non_match_noise_title(title):
        return False
    if _contains_any(f"{title} {source}", NON_PREDICTIVE_NOISE_KEYWORDS):
        return False
    if _contains_any(title, NOISE_FORMAT_KEYWORDS) and _signal_strength_score(title, source) < 2.5:
        return False
    if _is_low_information_source(source) and not _looks_like_hard_signal_news(title, source):
        return False
    if _requires_football_context(team_name) and not _has_football_context(title, source):
        return False
    if require_signal and _signal_strength_score(title, source) <= 0:
        return False
    if not _is_high_trust_source(source) and not _looks_like_hard_signal_news(title, source):
        return False
    return _team_relevance_score(title, team_name) > 0


def _passes_match_news_quality(item: dict, home_team: str, away_team: str) -> bool:
    title = str(item.get("title", "")).strip()
    source = str(item.get("source", "")).strip()
    domain = _safe_url_host(str(item.get("link", "")).strip())
    if not title:
        return False
    if _is_low_signal_source(source):
        return False
    if domain and domain in LOW_TRUST_NEWS_DOMAINS:
        return False
    # Evita falsos positivos de equipos con nombre geográfico/ambiguo.
    if (_requires_football_context(home_team) or _requires_football_context(away_team)) and not _has_football_context(
        title, source
    ):
        return False
    if _is_generic_preview_title(title) or _is_non_match_noise_title(title):
        return False
    if _contains_any(f"{title} {source}", NON_PREDICTIVE_NOISE_KEYWORDS):
        return False
    if _is_low_value_result_story(title):
        return False
    if _contains_any(title, NOISE_FORMAT_KEYWORDS) and _signal_strength_score(title, source) < 2.5:
        return False
    if _is_low_information_source(source) and not _looks_like_hard_signal_news(title, source):
        return False
    if not _is_high_trust_source(source) and not _looks_like_hard_signal_news(title, source):
        return False
    return _match_relevance_score(title, home_team, away_team) > 0


def _predictive_news_items(items: list[dict]) -> list[dict]:
    filtered = []
    for item in items:
        title = str(item.get("title", "")).strip()
        source = str(item.get("source", "")).strip()
        if not title or _is_low_signal_source(source):
            continue
        if _is_generic_preview_title(title) or _is_non_match_noise_title(title):
            continue
        enriched = dict(item)
        enriched["_signal"] = _signal_strength_score(title, source)
        filtered.append(enriched)
    return filtered


def _is_official_noise_title(title: str) -> bool:
    lowered = _normalize_ascii(title).lower()
    noise_tokens = [
        "store",
        "tienda",
        "shop",
        "sponsor",
        "patrocin",
        "ticketing",
        "abonos",
        "campus",
        "academy",
        "fundacion",
        "foundation",
        "presentacion",
        "youtube",
        "live!",
        "play red live",
    ]
    signal_tokens = [
        "lesion",
        "injury",
        "baja",
        "convocatoria",
        "rueda de prensa",
        "entrenador",
        "alineacion",
        "match",
        "partido",
        "previa",
        "cronica",
        "referee",
        "arbitro",
    ]
    return any(token in lowered for token in noise_tokens) and not any(
        token in lowered for token in signal_tokens
    )


def _official_predictive_items(items: list[dict]) -> list[dict]:
    filtered = []
    for item in _predictive_news_items(items):
        title = str(item.get("title", "")).strip()
        if not title or _is_official_noise_title(title):
            continue
        filtered.append(item)
    return filtered


def _is_local_media_source(source: str) -> bool:
    lowered = _normalize_ascii(source).lower()
    return any(token in lowered for token in LOCAL_MEDIA_SOURCE_TOKENS)


def _local_media_items(items: list[dict]) -> list[dict]:
    filtered = []
    for item in _predictive_news_items(items):
        source = str(item.get("source", "")).strip()
        if source and not _is_local_media_source(source):
            continue
        filtered.append(item)
    return filtered


def _infer_injury_status(title: str) -> str:
    lowered = title.lower()
    if any(token in lowered for token in ["out", "baja", "injured", "lesionado", "ruled out"]):
        return "out"
    if any(token in lowered for token in ["doubt", "duda", "questionable"]):
        return "doubtful"
    if any(token in lowered for token in ["suspension", "sancion", "banned"]):
        return "suspended"
    return "watch"


def _build_injury_entities(team_name: str, items: list[dict]) -> list[dict]:
    entities = []
    ignored_title_tokens = [
        "clasificacion",
        "partidos y marcadores",
        "standings, matches and scores",
        "u21",
        "u-21",
        "sub-21",
        "highlights",
        "lineups of",
        "predicted line-up",
        "predicted lineup",
        "probable lineup",
        "relegation",
        "resultado",
        "result",
    ]
    ignored_people = {"predicted", "relegation", "foxes", "saints", "pompey", "swans"}
    for item in items:
        title = str(item.get("title", "")).strip()
        source_name = str(item.get("source", "")).strip()
        haystack = f"{title} {item.get('source', '')}".lower()
        if not any(keyword in haystack for keyword in INJURY_KEYWORDS):
            continue
        normalized_title = _normalize_ascii(title).lower()
        if any(token in normalized_title for token in ignored_title_tokens):
            continue
        source_tokens = {
            token
            for token in re.findall(r"[a-z]+", _normalize_ascii(source_name).lower())
            if len(token) > 3
        }
        people = []
        for candidate in _extract_person_candidates(title):
            candidate_tokens = [
                token
                for token in re.findall(r"[a-z]+", _normalize_ascii(candidate).lower())
                if len(token) > 3
            ]
            if _team_similarity_score(candidate, team_name) >= 0.6:
                continue
            if _looks_like_known_team_entity(candidate):
                continue
            if _normalize_ascii(candidate).lower().strip() in ignored_people:
                continue
            if candidate_tokens and source_tokens and all(token in source_tokens for token in candidate_tokens):
                continue
            people.append(candidate)
        if not people and not _looks_like_hard_signal_news(title, source_name):
            continue
        if not people:
            people = [""]
        for person in people[:3]:
            entities.append(
                {
                    "player_name": person,
                    "status": _infer_injury_status(title),
                    "headline": title,
                    "source": item.get("source", ""),
                    "link": item.get("link", ""),
                    "published_at": item.get("published_at", ""),
                }
            )
    deduped = []
    seen = set()
    for entity in entities:
        key = (entity["player_name"], entity["headline"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entity)
    return deduped


def _build_referee_candidates(items: list[dict]) -> list[dict]:
    candidates = []
    for item in items:
        title = str(item.get("title", "")).strip()
        lowered = title.lower()
        if not any(keyword in lowered for keyword in ["referee", "arbitro", "árbitro"]):
            continue
        for person in _extract_person_candidates(title):
            if person.lower() in {"google news", "laliga"}:
                continue
            candidates.append(
                {
                    "name": person,
                    "headline": title,
                    "source": item.get("source", ""),
                    "link": item.get("link", ""),
                    "published_at": item.get("published_at", ""),
                }
            )
    deduped = []
    seen = set()
    for candidate in candidates:
        key = (candidate["name"], candidate["headline"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _looks_like_referee_name(candidate: str, home_team: str = "", away_team: str = "") -> bool:
    raw_candidate = str(candidate or "").strip()
    if not raw_candidate:
        return False
    if raw_candidate == raw_candidate.lower():
        return False
    normalized = _normalize_team_name(candidate)
    if not normalized:
        return False
    if any(
        token in normalized.split()
        for token in {
            "lashes",
            "against",
            "about",
            "after",
            "before",
            "with",
            "without",
            "says",
            "claim",
            "claims",
            "report",
            "reports",
            "preview",
            "lineup",
        }
    ):
        return False
    if normalized in {
        "laliga",
        "premier league",
        "champions league",
        "europa league",
        "conference league",
        "google news",
        "bbc football",
        "guardian",
    }:
        return False
    if home_team and _team_similarity_score(candidate, home_team) >= 0.55:
        return False
    if away_team and _team_similarity_score(candidate, away_team) >= 0.55:
        return False
    return len(normalized.split()) >= 2


def _extract_keyword_name(title: str, keywords: list[str]) -> list[str]:
    extracted = []
    escaped = "|".join(re.escape(keyword) for keyword in keywords)
    patterns = [
        rf"(?:{escaped})[^A-ZÃÃ‰ÃÃ“ÃšÃ‘]{{0,20}}([A-ZÃÃ‰ÃÃ“ÃšÃ‘][a-zÃ¡Ã©Ã­Ã³ÃºÃ±]+(?:\s+[A-ZÃÃ‰ÃÃ“ÃšÃ‘][a-zÃ¡Ã©Ã­Ã³ÃºÃ±]+){{1,3}})",
        rf"([A-ZÃÃ‰ÃÃ“ÃšÃ‘][a-zÃ¡Ã©Ã­Ã³ÃºÃ±]+(?:\s+[A-ZÃÃ‰ÃÃ“ÃšÃ‘][a-zÃ¡Ã©Ã­Ã³ÃºÃ±]+){{1,3}})[^A-ZÃÃ‰ÃÃ“ÃšÃ‘]{{0,20}}(?:{escaped})",
    ]
    for pattern in patterns:
        for match in re.findall(pattern, title, flags=re.IGNORECASE):
            if isinstance(match, tuple):
                extracted.extend([value for value in match if value])
            elif match:
                extracted.append(match)
    deduped = []
    for candidate in extracted:
        cleaned = str(candidate).strip()
        if cleaned and cleaned not in deduped:
            deduped.append(cleaned)
    return deduped


def _build_referee_candidates_strict(
    items: list[dict], home_team: str = "", away_team: str = ""
) -> list[dict]:
    candidates = []
    for item in items:
        title = str(item.get("title", "")).strip()
        lowered = title.lower()
        if not any(keyword in lowered for keyword in ["referee", "arbitro", "Ã¡rbitro"]):
            continue
        names = _extract_keyword_name(title, ["referee", "arbitro", "árbitro"]) or _extract_person_candidates(
            title
        )
        for person in names:
            if not _looks_like_referee_name(person, home_team, away_team):
                continue
            candidates.append(
                {
                    "name": person,
                    "headline": title,
                    "source": item.get("source", ""),
                    "link": item.get("link", ""),
                    "published_at": item.get("published_at", ""),
                }
            )
    deduped = []
    seen = set()
    for candidate in candidates:
        key = (candidate["name"], candidate["headline"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _extract_referee_name_from_text(text: str, home_team: str = "", away_team: str = "") -> str:
    if not text:
        return ""
    compact = re.sub(r"\s+", " ", str(text or "")).strip()
    patterns = [
        r"(?:Árbitro|Arbitro|Referee|Colegiado)(?: principal)?\s*[:\-]\s*([A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ]+){1,4})",
        r"(?:dirigirá|dirigira|pitará|pitara|arbitrará|arbitrara)\s+(?:el partido|el encuentro|la contienda)?\s*([A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ]+){1,4})",
        r"(?:designación arbitral|designacion arbitral).*?([A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ]+){1,4})",
    ]
    for pattern in patterns:
        match = re.search(pattern, compact, flags=re.IGNORECASE)
        if not match:
            continue
        candidate = match.group(1).strip(" .,:;-")
        if _looks_like_referee_name(candidate, home_team, away_team):
            return candidate
    return ""


def _fetch_article_referee_candidate(item: dict, home_team: str = "", away_team: str = "") -> str:
    link = str(item.get("link", "")).strip()
    if not link:
        return ""
    cache_key = f"article-referee:{hashlib.sha1(link.encode('utf-8')).hexdigest()}"
    cached = _cache_get(MATCH_NEWS_CACHE, cache_key, 12 * 3600)
    if cached is not None:
        return str(cached)
    try:
        article_text = _request_text(link, timeout=18)
    except Exception:
        article_text = ""
    candidate = _extract_referee_name_from_text(article_text, home_team, away_team)
    _cache_set(MATCH_NEWS_CACHE, cache_key, candidate)
    return candidate


def _prune_structured_db(active_match_keys: set[str], reference_time: datetime | None = None) -> None:
    now_dt = reference_time or datetime.now(timezone.utc)
    matches = STRUCTURED_DB.setdefault("matches", {})
    for match_key in list(matches.keys()):
        entry = matches.get(match_key) or {}
        kickoff_dt = _parse_iso_datetime(str(entry.get("kickoff", "")))
        keep = False
        if match_key in active_match_keys:
            keep = True
        elif kickoff_dt and (now_dt - kickoff_dt).total_seconds() <= 14 * 24 * 3600:
            keep = True
        if not keep:
            matches.pop(match_key, None)

    active_teams = {
        team_name
        for entry in matches.values()
        for team_name in [entry.get("local", ""), entry.get("visitante", "")]
        if team_name
    }
    teams = STRUCTURED_DB.setdefault("teams", {})
    for team_name in list(teams.keys()):
        if team_name not in active_teams:
            teams.pop(team_name, None)

    active_referees = {
        str(entry.get("referee_context", {}).get("assigned_referee", "")).strip()
        for entry in matches.values()
        if str(entry.get("referee_context", {}).get("assigned_referee", "")).strip()
    }
    referees = STRUCTURED_DB.setdefault("referees", {})
    for referee_name in list(referees.keys()):
        if referee_name not in active_referees:
            referees.pop(referee_name, None)

    STRUCTURED_DB.setdefault("meta", {})["last_pruned_at"] = _now_iso()


def _briefing_excerpt_from_dict(briefing: dict) -> str:
    if not briefing or not isinstance(briefing, dict):
        return ""
    parts = []
    insight = (briefing.get("mercado_y_probabilidades") or {}).get("insight_mercado", "")
    if insight:
        parts.append(insight)
    stakes = (briefing.get("contexto_deportivo") or {}).get("contexto_competitivo", "")
    if stakes:
        parts.append(stakes)
    rotation = (briefing.get("contexto_deportivo") or {}).get("riesgo_rotacion_competitiva", "")
    if rotation and "Sin senal fuerte" not in rotation:
        parts.append(rotation)
    cal = (briefing.get("contexto_deportivo") or {}).get("analisis_calendario_local", "")
    if cal:
        parts.append(cal)
    fatiga = (briefing.get("factores_externos") or {}).get("fatiga_y_descanso", "")
    if fatiga:
        parts.append(fatiga)
    return " | ".join(p for p in parts if p)


def _snapshot_summary_lines(snapshot: dict) -> list[str]:
    coverage = snapshot.get("coverage", {})
    jornadas = snapshot.get("quiniela_jornadas") or []

    lines = [
        "QUINIAI WORKER STATUS",
        f"Generated at: {_format_madrid_datetime(snapshot.get('generated_at', ''))} | UTC {snapshot.get('generated_at', '')}",
        f"Monitored matches: {coverage.get('monitored_matches', 0)}",
        f"Quiniela current jornada: {coverage.get('quiniela_current_jornada', '-')}",
        f"Ultima jornada oficial publicada: {coverage.get('quiniela_latest_available_jornada', '-')}",
        f"Quiniela jornadas tracked: {coverage.get('quiniela_jornadas', len(jornadas))}",
        f"Quiniela current matches: {coverage.get('focus_matches', 0)}",
        f"Quiniela tracked matches: {coverage.get('tracked_quiniela_matches', 0)}",
        f"Teams covered: {coverage.get('teams', 0)}",
        f"Weather coverage: {coverage.get('weather_matches', 0)} matches",
        f"Travel coverage: {coverage.get('travel_matches', 0)} matches",
        f"History coverage: {coverage.get('history_matches', 0)} matches",
        f"Structured DB: {coverage.get('structured_focus_matches', 0)} tracked matches, {coverage.get('structured_teams', 0)} teams, {coverage.get('structured_referees', 0)} referees",
        f"Source health: {coverage.get('sources_ok', 0)}/{coverage.get('sources_total', 0)} healthy, {coverage.get('fresh_headlines', 0)} fresh headlines",
    ]
    if coverage.get("quiniela_unmatched_slots"):
        lines.append(f"Official quiniela slots pending resolution: {coverage.get('quiniela_unmatched_slots', 0)}")
    if jornadas:
        lines.append(f"Tracked jornadas: {', '.join(str(j.get('jornada')) for j in jornadas if j.get('jornada') is not None)}")
    integrity = snapshot.get("quiniela_integrity") or {}
    if integrity:
        status = "OK" if integrity.get("ok") else "ERROR"
        lines.append(
            f"Quiniela integrity: {status} | jornadas={integrity.get('checked_jornadas', 0)} | "
            f"slots={integrity.get('checked_slots', 0)} | mismatches={integrity.get('mismatch_count', 0)}"
        )
    return lines


def _write_text_file(path: Path, text: str) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    except Exception:
        pass


def _write_json_file(path: Path, payload: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _monitor_slot_label(match: dict) -> str:
    slot = next(
        (
            item
            for item in (match.get("quiniela_slots") or [])
            if _safe_int(item.get("position")) is not None
        ),
        {},
    )
    position = _safe_int(slot.get("position"))
    if not position:
        return "-"
    return "Pleno al 15" if position == 15 else f"{position}."


def _monitor_extract_upcoming(match: dict, side: str) -> list[dict]:
    schedule_inputs = match.get("_schedule_inputs") or {}
    for key in (
        f"{side}_sportsdb_next_upcoming",
        f"{side}_round_upcoming",
        f"{side}_espn_upcoming",
        f"{side}_schedule_upcoming",
        f"{side}_feed_upcoming",
    ):
        items = schedule_inputs.get(key) or []
        if items:
            return items[:UPCOMING_FIXTURE_WINDOW]
    return []


def _monitor_future_summary(match: dict, side: str) -> str:
    upcoming = _monitor_extract_upcoming(match, side)
    if not upcoming:
        return "sin calendario detectado"
    parts = []
    for item in upcoming[:5]:
        venue = "casa" if str(item.get("venue", "")).strip().lower() == "home" else "fuera"
        opponent = str(item.get("opponent", "")).strip() or "rival por confirmar"
        position = _safe_int(item.get("opponent_position"))
        position_text = f"{position}º" if position else "-"
        competition = _fixture_competition_label(item)
        suffix = position_text
        if competition and _is_high_importance_nonleague_fixture(item):
            suffix += f", {competition}"
        parts.append(f"{venue} vs {opponent} ({suffix})")
    return " | ".join(parts)


def _monitor_match_payload(match: dict) -> dict:
    history = match.get("history_context") or {}
    analytics = match.get("analytics_context") or {}
    competition = match.get("competition_context") or {}
    structured = match.get("structured_context") or {}
    home_table = ((history.get("home") or {}).get("table") or {})
    away_table = ((history.get("away") or {}).get("table") or {})
    home_recent = ((history.get("home") or {}).get("recent_all") or {})
    away_recent = ((history.get("away") or {}).get("recent_all") or {})
    market = match.get("market_context") or {}
    referee = structured.get("referee_context") or {}
    referee_bias = _referee_analysis_summary(referee.get("season_analysis") or {})
    if not referee_bias:
        referee_bias = "sin historico arbitral fiable"
    return {
        "slot": _monitor_slot_label(match),
        "local": match.get("local", ""),
        "visitante": match.get("visitante", ""),
        "league": match.get("league", ""),
        "kickoff": match.get("kickoff", ""),
        "bookmaker": match.get("bookmaker", ""),
        "odds": match.get("odds", {}),
        "normalized_percent": market.get("normalized_percent", {}),
        "official_percent": (
            market.get("official_percent")
            or match.get("official_quiniela_percentages")
            or {}
        ),
        "home_table": {
            "position": home_table.get("position"),
            "points": home_table.get("points"),
            "form": home_recent.get("form", ""),
        },
        "away_table": {
            "position": away_table.get("position"),
            "points": away_table.get("points"),
            "form": away_recent.get("form", ""),
        },
        "pressure": {
            "home": analytics.get("home_pressure_index", {}),
            "away": analytics.get("away_pressure_index", {}),
        },
        "fatigue": {
            "home": analytics.get("home_fatigue_index", {}),
            "away": analytics.get("away_fatigue_index", {}),
        },
        "competitive_context": {
            "season_context_phase": competition.get("season_context_phase", {}),
            "competitive_stakes_label": competition.get("competitive_stakes_label", ""),
            "direct_rivalry": competition.get("direct_rivalry", {}),
            "home_objective": competition.get("home_objective", {}),
            "away_objective": competition.get("away_objective", {}),
            "home_must_win_index": analytics.get("home_must_win_index", 0),
            "away_must_win_index": analytics.get("away_must_win_index", 0),
            "home_must_not_lose_index": analytics.get("home_must_not_lose_index", 0),
            "away_must_not_lose_index": analytics.get("away_must_not_lose_index", 0),
            "direct_rivalry_index": analytics.get("direct_rivalry_index", 0),
            "home_rotation_context": competition.get("home_rotation_context", {}),
            "away_rotation_context": competition.get("away_rotation_context", {}),
        },
        "travel_km": _safe_float((match.get("travel_context") or {}).get("distance_km")),
        "weather": {
            "temperature_c": _safe_float((match.get("weather_context") or {}).get("temperature_c")),
            "precipitation_probability": _safe_int(
                (match.get("weather_context") or {}).get("precipitation_probability")
            ),
            "wind_speed_kmh": _safe_float((match.get("weather_context") or {}).get("wind_speed_kmh")),
        },
        "referee": {
            "name": referee.get("assigned_referee", "") or "no confirmado",
            "bias_summary": referee_bias,
        },
        "future_home": _monitor_future_summary(match, "home"),
        "future_away": _monitor_future_summary(match, "away"),
        "briefing_excerpt": _briefing_excerpt_from_dict(match.get("focus_ai_briefing") or {}),
        "analysis_ready": bool(match.get("focus_ai_briefing")),
    }


def _select_monitor_jornadas(quiniela_jornadas: list[dict]) -> list[dict]:
    if not quiniela_jornadas:
        return []
    current = next((j for j in quiniela_jornadas if j.get("is_current")), None)
    selected = []
    if current:
        selected.append(current)
        current_num = _safe_int(current.get("jornada"), 0) or 0
        future_jornadas = sorted(
            [
                jornada
                for jornada in quiniela_jornadas
                if (_safe_int(jornada.get("jornada"), 0) or 0) > current_num
            ],
            key=lambda item: _safe_int(item.get("jornada"), 9999) or 9999,
        )
        previous_jornadas = sorted(
            [
                jornada
                for jornada in quiniela_jornadas
                if (_safe_int(jornada.get("jornada"), 0) or 0) < current_num
            ],
            key=lambda item: _safe_int(item.get("jornada"), 0) or 0,
            reverse=True,
        )
        selected.extend(future_jornadas)
        selected.extend(previous_jornadas)
    if not selected:
        selected = sorted(
            quiniela_jornadas,
            key=lambda item: _safe_int(item.get("jornada"), 0) or 0,
            reverse=True,
        )
    seen = set()
    deduped = []
    for jornada in selected:
        jornada_num = _safe_int(jornada.get("jornada"))
        if not jornada_num or jornada_num in seen:
            continue
        seen.add(jornada_num)
        deduped.append(jornada)
    return deduped[:MONITOR_PUBLIC_JORNADAS]


def _build_monitor_public_jornadas(status_payload: dict) -> list[dict]:
    jornadas = _select_monitor_jornadas(status_payload.get("quiniela_jornadas") or [])
    public_jornadas = []
    for jornada in jornadas:
        public_jornadas.append(
            {
                "jornada": jornada.get("jornada"),
                "label": jornada.get("label") or f"Jornada {jornada.get('jornada')}",
                "is_current": bool(jornada.get("is_current")),
                "source": jornada.get("source", ""),
                "kickoff_from": jornada.get("kickoff_from", ""),
                "kickoff_to": jornada.get("kickoff_to", ""),
                "history_only": bool(jornada.get("history_only")),
                "matches": [_monitor_match_payload(match) for match in (jornada.get("matches") or [])],
            }
        )
    return public_jornadas


def _html_escape(value: object) -> str:
    text = str(value if value is not None else "")
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _bullet_list_html(items: list[str]) -> str:
    valid_items = [item for item in items if str(item).strip()]
    if not valid_items:
        return "<li>Sin datos relevantes.</li>"
    return "".join(f"<li>{_html_escape(item)}</li>" for item in valid_items)


def _render_match_news_html(items: list[dict], limit: int = 5) -> str:
    rows = []
    for item in items[:limit]:
        title = str(item.get("title", "")).strip()
        if not title:
            continue
        source = str(item.get("source", "")).strip()
        rows.append(f"{title} [{source}]".strip())
    return _bullet_list_html(rows)


def _render_focus_match_detail(match: dict) -> str:
    market = (match.get("market_context") or {}).get("normalized_percent", {})
    odds = match.get("odds") or {}
    weather = match.get("weather_context") or {}
    travel = match.get("travel_context") or {}
    schedule = match.get("schedule_context") or {}
    history = match.get("history_context") or {}
    competition = match.get("competition_context") or {}
    analytics = match.get("analytics_context") or {}
    structured = match.get("structured_context") or {}
    injury_context = structured.get("injury_context") or {}
    referee_context = structured.get("referee_context") or {}
    referee_analysis = referee_context.get("season_analysis") or {}
    event_context = structured.get("event_context") or {}
    home_focus_news = (match.get("home_team_context") or {}).get("focus_news", {}).get("items", [])
    away_focus_news = (match.get("away_team_context") or {}).get("focus_news", {}).get("items", [])
    home_focus_signals = (match.get("home_team_context") or {}).get("focus_news", {}).get("signals", {})
    away_focus_signals = (match.get("away_team_context") or {}).get("focus_news", {}).get("signals", {})
    home_media_news = (match.get("home_team_context") or {}).get("media_news", {}).get("items", [])
    away_media_news = (match.get("away_team_context") or {}).get("media_news", {}).get("items", [])
    home_focus_signals = (match.get("home_team_context") or {}).get("focus_news", {}).get("signals", {})
    away_focus_signals = (match.get("away_team_context") or {}).get("focus_news", {}).get("signals", {})
    home_official = (match.get("home_team_context") or {}).get("official_site", {}).get("items", [])
    away_official = (match.get("away_team_context") or {}).get("official_site", {}).get("items", [])
    match_news = (match.get("match_news_context") or {}).get("items", [])
    referee = referee_context.get("assigned_referee", "") or "No confirmado"
    home_table = ((history.get("home") or {}).get("table") or {})
    away_table = ((history.get("away") or {}).get("table") or {})
    home_recent = ((history.get("home") or {}).get("recent_all") or {})
    away_recent = ((history.get("away") or {}).get("recent_all") or {})
    h2h = history.get("head_to_head") or {}
    home_relegation = competition.get("home_relegation") or {}
    away_relegation = competition.get("away_relegation") or {}
    home_upcoming = competition.get("home_upcoming") or []
    away_upcoming = competition.get("away_upcoming") or []
    home_pressure = analytics.get("home_pressure_index") or {}
    away_pressure = analytics.get("away_pressure_index") or {}
    home_fatigue = analytics.get("home_fatigue_index") or {}
    away_fatigue = analytics.get("away_fatigue_index") or {}
    home_rolling = analytics.get("home_rolling") or {}
    away_rolling = analytics.get("away_rolling") or {}
    digest = ", ".join(match.get("focus_digest") or []) or "sin alertas fuertes"
    slot_chips = "".join(
        f"<span class='chip chip-soft'>{_html_escape(label)}</span>"
        for label in _quiniela_slot_labels(match)
    )
    home_injuries = [
        f"{item.get('player_name') or 'Jugador sin identificar'} ({item.get('status', 'watch')})"
        for item in (injury_context.get("home_team") or {}).get("items", [])[:8]
    ]
    away_injuries = [
        f"{item.get('player_name') or 'Jugador sin identificar'} ({item.get('status', 'watch')})"
        for item in (injury_context.get("away_team") or {}).get("items", [])[:8]
    ]
    return f"""
    <details class="match-detail">
      <summary>
        <span class="match-title">{_html_escape(match.get('local', ''))} vs {_html_escape(match.get('visitante', ''))}</span>
        <span class="match-sub">{_html_escape(match.get('league', ''))} | {_html_escape(digest)}</span>
        <span class="chips">{slot_chips}</span>
      </summary>
      <div class="match-detail-body">
        <div class="detail-grid">
          <div class="detail-card">
            <h3>Mercado y cuotas</h3>
            <ul>
              <li>Cuotas: 1={_html_escape(odds.get('1', '-'))}, X={_html_escape(odds.get('X', '-'))}, 2={_html_escape(odds.get('2', '-'))}</li>
              <li>Probabilidad base: 1={_html_escape(market.get('1', '-'))}%, X={_html_escape(market.get('X', '-'))}%, 2={_html_escape(market.get('2', '-'))}%</li>
              <li>Porcentaje oficial quiniela: {_html_escape(_official_quiniela_percentages_line(match))}</li>
              <li>Bookmaker: {_html_escape(match.get('bookmaker', '-') or '-')}</li>
              <li>Round/evento: {_html_escape(event_context.get('round', '-') or '-')} | sede {_html_escape(event_context.get('venue', '-') or '-')}</li>
            </ul>
          </div>
          <div class="detail-card">
            <h3>Tabla y objetivo</h3>
            <ul>
              <li>{_html_escape(_competitive_context_line(match.get('local', ''), home_table, home_relegation, competition.get('home_objective') or {}))}</li>
              <li>{_html_escape(_competitive_context_line(match.get('visitante', ''), away_table, away_relegation, competition.get('away_objective') or {}))}</li>
              <li>Forma ultimos 5: {_html_escape(home_recent.get('form', '-'))} ({_html_escape(home_recent.get('points', '-'))} pts) / {_html_escape(away_recent.get('form', '-'))} ({_html_escape(away_recent.get('points', '-'))} pts)</li>
              <li>Indice de presion: {_html_escape(home_pressure.get('score', '-'))} / {_html_escape(away_pressure.get('score', '-'))}</li>
              <li>ELO: {_html_escape(analytics.get('home_elo', '-'))} / {_html_escape(analytics.get('away_elo', '-'))}</li>
              <li>H2H: {_html_escape(h2h.get('meetings', 0))} cruces, local {_html_escape(h2h.get('home_team_wins', 0))}, visitante {_html_escape(h2h.get('away_team_wins', 0))}, empates {_html_escape(h2h.get('draws', 0))}</li>
            </ul>
          </div>
          <div class="detail-card">
            <h3>Clima, viaje y carga</h3>
            <ul>
              <li>Clima: {_html_escape(weather.get('temperature_c', '-'))} C, lluvia {_html_escape(weather.get('precipitation_probability', '-'))}%, viento {_html_escape(weather.get('wind_speed_kmh', '-'))} km/h</li>
              <li>Riesgo clima: {_html_escape(match.get('match_signals', {}).get('weather_risk', 'unknown'))}</li>
              <li>Viaje visitante: {_html_escape(travel.get('distance_km', '-'))} km ({_html_escape(travel.get('distance_bucket', 'unknown'))})</li>
              <li>Descanso local/visitante: {_html_escape(schedule.get('home', {}).get('days_since_last_match', '-'))} / {_html_escape(schedule.get('away', {}).get('days_since_last_match', '-'))} dias</li>
              <li>Partidos ultimos 14 dias: {_html_escape(schedule.get('home', {}).get('matches_last_14_days', '-'))} / {_html_escape(schedule.get('away', {}).get('matches_last_14_days', '-'))}</li>
              <li>Indice de fatiga: {_html_escape(home_fatigue.get('score', '-'))} / {_html_escape(away_fatigue.get('score', '-'))}</li>
            </ul>
          </div>
          <div class="detail-card">
            <h3>Arbitro y contexto oficial</h3>
            <ul>
              <li>Arbitro: {_html_escape(referee)}</li>
              <li>Cuarto arbitro: {_html_escape(referee_context.get('fourth_official', '-') or '-')}</li>
              <li>VAR / AVAR: {_html_escape(referee_context.get('var_referee', '-') or '-')} / {_html_escape(referee_context.get('avar_referee', '-') or '-')}</li>
              <li>Fuente arbitral: {_html_escape(referee_context.get('source', '-') or '-')}</li>
              <li>Historico arbitral: {_html_escape(_referee_analysis_summary(referee_analysis))}</li>
              <li>Bajas estructuradas: {_html_escape((injury_context.get('home_team') or {}).get('count', 0))} / {_html_escape((injury_context.get('away_team') or {}).get('count', 0))}</li>
            </ul>
          </div>
        </div>
        <div class="detail-grid three">
          <div class="detail-card">
            <h3>Proximos {UPCOMING_FIXTURE_WINDOW} partidos { _html_escape(match.get('local', '')) }</h3>
            <ul>{_render_fixture_list_html_deep(home_upcoming, UPCOMING_FIXTURE_WINDOW)}</ul>
            <p class="mini-title">{_html_escape(_future_window_summary(competition.get('home_future_difficulty') or {}))}</p>
          </div>
          <div class="detail-card">
            <h3>Proximos {UPCOMING_FIXTURE_WINDOW} partidos { _html_escape(match.get('visitante', '')) }</h3>
            <ul>{_render_fixture_list_html_deep(away_upcoming, UPCOMING_FIXTURE_WINDOW)}</ul>
            <p class="mini-title">{_html_escape(_future_window_summary(competition.get('away_future_difficulty') or {}))}</p>
          </div>
          <div class="detail-card">
            <h3>Noticias del cruce</h3>
            <ul>{_render_match_news_html(match_news, 8)}</ul>
          </div>
        </div>
        <div class="detail-grid three">
          <div class="detail-card">
            <h3>Medias moviles { _html_escape(match.get('local', '')) }</h3>
            <ul>
              <li>Goles 5/10/15: {_html_escape((home_rolling.get('5') or {}).get('avg_goals_for', '-'))} / {_html_escape((home_rolling.get('10') or {}).get('avg_goals_for', '-'))} / {_html_escape((home_rolling.get('15') or {}).get('avg_goals_for', '-'))}</li>
              <li>Encajados 5/10/15: {_html_escape((home_rolling.get('5') or {}).get('avg_goals_against', '-'))} / {_html_escape((home_rolling.get('10') or {}).get('avg_goals_against', '-'))} / {_html_escape((home_rolling.get('15') or {}).get('avg_goals_against', '-'))}</li>
              <li>Tiros a puerta 5/10/15: {_html_escape((home_rolling.get('5') or {}).get('avg_shots_on_target_for', '-'))} / {_html_escape((home_rolling.get('10') or {}).get('avg_shots_on_target_for', '-'))} / {_html_escape((home_rolling.get('15') or {}).get('avg_shots_on_target_for', '-'))}</li>
            </ul>
          </div>
          <div class="detail-card">
            <h3>Medias moviles { _html_escape(match.get('visitante', '')) }</h3>
            <ul>
              <li>Goles 5/10/15: {_html_escape((away_rolling.get('5') or {}).get('avg_goals_for', '-'))} / {_html_escape((away_rolling.get('10') or {}).get('avg_goals_for', '-'))} / {_html_escape((away_rolling.get('15') or {}).get('avg_goals_for', '-'))}</li>
              <li>Encajados 5/10/15: {_html_escape((away_rolling.get('5') or {}).get('avg_goals_against', '-'))} / {_html_escape((away_rolling.get('10') or {}).get('avg_goals_against', '-'))} / {_html_escape((away_rolling.get('15') or {}).get('avg_goals_against', '-'))}</li>
              <li>Tiros a puerta 5/10/15: {_html_escape((away_rolling.get('5') or {}).get('avg_shots_on_target_for', '-'))} / {_html_escape((away_rolling.get('10') or {}).get('avg_shots_on_target_for', '-'))} / {_html_escape((away_rolling.get('15') or {}).get('avg_shots_on_target_for', '-'))}</li>
            </ul>
          </div>
          <div class="detail-card">
            <h3>Lectura predictiva</h3>
            <ul>
              <li>Presion local/visitante: {_html_escape(home_pressure.get('label', '-'))} / {_html_escape(away_pressure.get('label', '-'))}</li>
              <li>Fatiga local/visitante: {_html_escape(home_fatigue.get('label', '-'))} / {_html_escape(away_fatigue.get('label', '-'))}</li>
              <li>Dificultad calendario: {_html_escape((competition.get('home_future_difficulty') or {}).get('difficulty_index', '-'))} / {_html_escape((competition.get('away_future_difficulty') or {}).get('difficulty_index', '-'))}</li>
            </ul>
          </div>
        </div>
        <div class="detail-grid three">
          <div class="detail-card">
            <h3>Web oficial { _html_escape(match.get('local', '')) }</h3>
            <ul>{_render_match_news_html(home_official, 6)}</ul>
          </div>
          <div class="detail-card">
            <h3>Web oficial { _html_escape(match.get('visitante', '')) }</h3>
            <ul>{_render_match_news_html(away_official, 6)}</ul>
          </div>
          <div class="detail-card">
            <h3>Noticias personalizadas</h3>
            <div class="mini-two">
              <div>
                <div class="mini-title">{_html_escape(match.get('local', ''))}</div>
                <ul>{_render_match_news_html(home_focus_news, 6)}</ul>
              </div>
              <div>
                <div class="mini-title">{_html_escape(match.get('visitante', ''))}</div>
                <ul>{_render_match_news_html(away_focus_news, 6)}</ul>
              </div>
            </div>
          </div>
        </div>
        <div class="detail-grid three">
          <div class="detail-card">
            <h3>Prensa local { _html_escape(match.get('local', '')) }</h3>
            <ul>{_render_match_news_html(home_media_news, 6)}</ul>
          </div>
          <div class="detail-card">
            <h3>Prensa local { _html_escape(match.get('visitante', '')) }</h3>
            <ul>{_render_match_news_html(away_media_news, 6)}</ul>
          </div>
          <div class="detail-card">
            <h3>Lectura de necesidad</h3>
            <ul>
              <li>Presion numerica: {_html_escape(home_pressure.get('score', '-'))} / {_html_escape(away_pressure.get('score', '-'))}</li>
              <li>Fatiga numerica: {_html_escape(home_fatigue.get('score', '-'))} / {_html_escape(away_fatigue.get('score', '-'))}</li>
              <li>Dificultad futura: {_html_escape((competition.get('home_future_difficulty') or {}).get('difficulty_index', '-'))} / {_html_escape((competition.get('away_future_difficulty') or {}).get('difficulty_index', '-'))}</li>
            </ul>
          </div>
        </div>
        <div class="detail-grid two-strong">
          <div class="detail-card">
            <h3>Bajas detectadas</h3>
            <div class="mini-two">
              <div>
                <div class="mini-title">{_html_escape(match.get('local', ''))}</div>
                <ul>{_bullet_list_html(home_injuries)}</ul>
              </div>
              <div>
                <div class="mini-title">{_html_escape(match.get('visitante', ''))}</div>
                <ul>{_bullet_list_html(away_injuries)}</ul>
              </div>
            </div>
          </div>
          <div class="detail-card">
            <h3>Bloque que recibe la IA</h3>
            <pre class="briefing">{_html_escape(json.dumps(match.get('focus_ai_briefing') or {}, ensure_ascii=False, indent=2))}</pre>
          </div>
        </div>
      </div>
    </details>
    """


def _render_jornada_block(jornada: dict) -> str:
    rows = ""
    for match in jornada.get("matches", []):
        market = (match.get("market_context") or {}).get("normalized_percent", {})
        history = match.get("history_context") or {}
        competition = match.get("competition_context") or {}
        structured = match.get("structured_context") or {}
        home_table = ((history.get("home") or {}).get("table") or {})
        away_table = ((history.get("away") or {}).get("table") or {})
        home_relegation = competition.get("home_relegation") or {}
        away_relegation = competition.get("away_relegation") or {}
        referee_name = (structured.get("referee_context") or {}).get("assigned_referee", "") or "-"
        slot = next(
            (
                current
                for current in (match.get("quiniela_slots") or [])
                if current.get("jornada") == jornada.get("jornada")
            ),
            {},
        )
        slot_label = f"{slot.get('position', '-')}"
        if slot.get("pleno15"):
            slot_label += " P15"
        rows += (
            "<tr>"
            f"<td>{_html_escape(slot_label)}</td>"
            f"<td>{_html_escape(match.get('local', ''))} vs {_html_escape(match.get('visitante', ''))}</td>"
            f"<td>{_html_escape(match.get('league', ''))}</td>"
            f"<td>1:{_html_escape(market.get('1', '-'))} X:{_html_escape(market.get('X', '-'))} 2:{_html_escape(market.get('2', '-'))}</td>"
            f"<td>{_html_escape(home_table.get('position', '-'))}º / {_html_escape(home_table.get('points', '-'))} pts · gap {_html_escape(home_relegation.get('gap_to_drop_zone', '-'))}</td>"
            f"<td>{_html_escape(away_table.get('position', '-'))}º / {_html_escape(away_table.get('points', '-'))} pts · gap {_html_escape(away_relegation.get('gap_to_drop_zone', '-'))}</td>"
            f"<td>{_html_escape(referee_name)}</td>"
            "</tr>"
        )
    unmatched = jornada.get("unmatched_slots") or []
    unmatched_html = ""
    if unmatched:
        unmatched_rows = "".join(
            f"<li>{_html_escape(slot.get('position', '-'))}. {_html_escape(slot.get('local', ''))} vs {_html_escape(slot.get('visitante', ''))}</li>"
            for slot in unmatched
        )
        unmatched_html = f"""
        <div class="detail-card">
          <h3>Slots oficiales pendientes de casar con el feed</h3>
          <ul>{unmatched_rows}</ul>
        </div>
        """
    detail_blocks = "".join(_render_focus_match_detail(match) for match in jornada.get("matches", []))
    jornada_anchor = f"jornada-{_html_escape(jornada.get('jornada', 'sin-numero'))}"
    return f"""
    <div class="section jornada-section" id="{jornada_anchor}">
      <div class="jornada-header">
        <div>
          <h2>{_html_escape(jornada.get('label', 'Jornada'))}</h2>
          <div class="meta">
            Fuente: <a href="{_html_escape(jornada.get('source_url', ''))}">{_html_escape(jornada.get('source', ''))}</a> |
            Partidos resueltos: {_html_escape(len(jornada.get('matches', [])))} |
            Slots pendientes: {_html_escape(len(unmatched))}
          </div>
        </div>
        <div class="chips">
          <span class="chip chip-soft">Desde {_html_escape(jornada.get('kickoff_from', '-') or '-')}</span>
          <span class="chip chip-soft">Hasta {_html_escape(jornada.get('kickoff_to', '-') or '-')}</span>
        </div>
      </div>
      <table>
        <thead>
          <tr><th>Slot</th><th>Partido</th><th>Liga</th><th>Mercado</th><th>Local</th><th>Visitante</th><th>Arbitro</th></tr>
        </thead>
        <tbody>
          {rows}
        </tbody>
      </table>
      {unmatched_html}
      <div class="detail-stack">
        {detail_blocks}
      </div>
    </div>
    """


def _build_status_html(status_payload: dict) -> str:
    coverage = status_payload.get("coverage") or {}
    structured = status_payload.get("structured_db_summary") or {}
    competition_headlines = status_payload.get("competition_headlines") or {}
    context_sources = status_payload.get("context_sources") or []
    focus_matches = status_payload.get("focus_matches") or []
    quiniela_jornadas = status_payload.get("quiniela_jornadas") or []
    last_runs = status_payload.get("last_runs") or []
    ok = bool(status_payload.get("ok"))
    status_label = "OK" if ok else "ERROR"
    status_color = "#16a34a" if ok else "#dc2626"

    cards = [
        ("Partidos monitorizados", coverage.get("monitored_matches", 0)),
        ("Jornada actual", coverage.get("quiniela_current_jornada", "-")),
        ("Ultima oficial publicada", coverage.get("quiniela_latest_available_jornada", "-")),
        ("Partidos jornada actual", coverage.get("focus_matches", 0)),
        ("Partidos quiniela rastreados", coverage.get("tracked_quiniela_matches", 0)),
        ("Jornadas oficiales", coverage.get("quiniela_jornadas", 0)),
        ("Clima", coverage.get("weather_matches", 0)),
        ("Viajes", coverage.get("travel_matches", 0)),
        ("Historicos", coverage.get("history_matches", 0)),
        ("Arbitros estructurados", coverage.get("structured_referees", 0)),
        ("Fuentes sanas", f"{coverage.get('sources_ok', 0)}/{coverage.get('sources_total', 0)}"),
        ("Titulares frescos", coverage.get("fresh_headlines", 0)),
    ]
    cards_html = "".join(
        f"<div class='card'><div class='k'>{_html_escape(label)}</div><div class='v'>{_html_escape(value)}</div></div>"
        for label, value in cards
    )

    current_jornada_label = "Sin jornada oficial detectada"
    current_jornada_number = coverage.get("quiniela_current_jornada")
    latest_available_number = coverage.get("quiniela_latest_available_jornada")
    if current_jornada_number:
        current_jornada_label = f"Jornada actual {current_jornada_number}"
        if latest_available_number and latest_available_number != current_jornada_number:
            current_jornada_label += f" · siguiente publicada {latest_available_number}"
    elif latest_available_number:
        current_jornada_label = f"Ultima oficial publicada {latest_available_number}"
    jornada_nav_parts = []
    for jornada in quiniela_jornadas:
        jornada_number = jornada.get("jornada", "sin-numero")
        jornada_label = jornada.get("label") or f"Jornada {jornada_number}"
        jornada_nav_parts.append(
            f"<a class='chip chip-soft' href='#jornada-{_html_escape(jornada_number)}'>{_html_escape(jornada_label)}</a>"
        )
    jornada_nav = "".join(jornada_nav_parts) or "<span class='chip'>Sin jornadas detectadas</span>"

    source_rows = ""
    for source in context_sources:
        source_rows += (
            "<tr>"
            f"<td>{_html_escape(source.get('name', ''))}</td>"
            f"<td><a href='{_html_escape(source.get('url', ''))}'>{_html_escape(source.get('url', ''))}</a></td>"
            "</tr>"
        )

    headline_blocks = ""
    for league_key, payload in competition_headlines.items():
        items = payload.get("items", [])[:4]
        health = payload.get("source_health", [])
        items_html = "".join(
            f"<li><a href='{_html_escape(item.get('link', ''))}'>{_html_escape(item.get('title', ''))}</a>"
            f" <span>{_html_escape(item.get('source', ''))}</span></li>"
            for item in items
        ) or "<li>Sin titulares disponibles.</li>"
        health_html = "".join(
            f"<span class='chip {'chip-ok' if source.get('ok') else 'chip-bad'}'>{_html_escape(source.get('name', ''))}: {_html_escape(source.get('items', 0))}</span>"
            for source in health
        )
        headline_blocks += f"""
        <div class="section headline-block">
          <h2>{_html_escape(league_key)}</h2>
          <div class="chips">{health_html}</div>
          <ul class="headline-list">{items_html}</ul>
        </div>
        """

    focus_rows = ""
    for match in focus_matches[:15]:
        market = (match.get("market_context") or {}).get("normalized_percent", {})
        schedule = match.get("schedule_context") or {}
        competition = match.get("competition_context") or {}
        analytics = match.get("analytics_context") or {}
        home_relegation = competition.get("home_relegation") or {}
        away_relegation = competition.get("away_relegation") or {}
        referee_name = (match.get("structured_context") or {}).get("referee_context", {}).get(
            "assigned_referee", ""
        ) or "-"
        pressure = analytics.get("home_pressure_index", {}).get("score", "-")
        away_pressure = analytics.get("away_pressure_index", {}).get("score", "-")
        focus_rows += (
            "<tr>"
            f"<td>{_html_escape(match.get('local', ''))} vs {_html_escape(match.get('visitante', ''))}</td>"
            f"<td>{_html_escape(match.get('league', ''))}</td>"
            f"<td>1:{_html_escape(market.get('1', '-'))} X:{_html_escape(market.get('X', '-'))} 2:{_html_escape(market.get('2', '-'))}</td>"
            f"<td>{_html_escape(home_relegation.get('gap_to_drop_zone', '-'))} / {_html_escape(away_relegation.get('gap_to_drop_zone', '-'))}</td>"
            f"<td>L {_html_escape(schedule.get('home', {}).get('days_since_last_match', '-'))}d / "
            f"V {_html_escape(schedule.get('away', {}).get('days_since_last_match', '-'))}d</td>"
            f"<td>{_html_escape(referee_name)}</td>"
            f"<td>{_html_escape(pressure)} / {_html_escape(away_pressure)}</td>"
            "</tr>"
        )
    jornada_blocks = "".join(_render_jornada_block(jornada) for jornada in quiniela_jornadas)
    run_rows = "".join(
        "<tr>"
        f"<td>{_html_escape('OK' if run.get('ok') else 'ERROR')}</td>"
        f"<td>{_html_escape(_format_madrid_datetime(run.get('finished_at', '')))}</td>"
        f"<td>{_html_escape(run.get('duration_seconds', '-'))}</td>"
        f"<td>{_html_escape(run.get('current_jornada', '-'))}</td>"
        f"<td>{_html_escape(run.get('tracked_matches', '-'))}</td>"
        f"<td>{_html_escape(run.get('error', ''))}</td>"
        "</tr>"
        for run in last_runs[-8:]
    ) or "<tr><td colspan='6'>Sin historial reciente.</td></tr>"

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="{max(300, MONITOR_PUBLISH_MIN_SECONDS)}">
  <title>Panel QuiniAI</title>
  <style>
    :root {{
      --bg: #08111f;
      --panel: #0f1b2d;
      --panel-2: #13233a;
      --text: #e6f1ff;
      --muted: #99adc7;
      --accent: #22c55e;
      --warn: #f59e0b;
      --border: #20324d;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Consolas, "Cascadia Code", monospace;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(34,197,94,.10), transparent 30%),
        radial-gradient(circle at bottom right, rgba(56,189,248,.10), transparent 28%),
        var(--bg);
      min-height: 100vh;
    }}
    .wrap {{ max-width: 1100px; margin: 0 auto; padding: 32px 20px 48px; }}
    .hero {{
      display: grid;
      gap: 16px;
      padding: 24px;
      border: 1px solid var(--border);
      background: linear-gradient(135deg, rgba(15,27,45,.96), rgba(19,35,58,.96));
      border-radius: 18px;
      box-shadow: 0 24px 80px rgba(0,0,0,.35);
    }}
    .hero-top {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      flex-wrap: wrap;
    }}
    h1 {{
      margin: 0;
      font-size: 32px;
      letter-spacing: 1px;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      padding: 10px 14px;
      border-radius: 999px;
      border: 1px solid rgba(255,255,255,.08);
      background: rgba(255,255,255,.04);
      color: white;
      font-weight: 700;
    }}
    .dot {{
      width: 12px;
      height: 12px;
      border-radius: 50%;
      background: {status_color};
      box-shadow: 0 0 16px {status_color};
    }}
    .sub {{
      color: var(--muted);
      line-height: 1.6;
      font-size: 15px;
    }}
    .grid {{
      margin-top: 24px;
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 14px;
    }}
    .card {{
      padding: 16px;
      border: 1px solid var(--border);
      border-radius: 16px;
      background: rgba(255,255,255,.03);
    }}
    .k {{ color: var(--muted); font-size: 13px; margin-bottom: 8px; }}
    .v {{ font-size: 28px; font-weight: 800; }}
    .section {{
      margin-top: 24px;
      padding: 22px;
      border: 1px solid var(--border);
      border-radius: 18px;
      background: rgba(255,255,255,.025);
    }}
    .headline-block {{ margin-top: 0; }}
    h2 {{ margin: 0 0 14px; font-size: 20px; }}
    ul {{ margin: 0; padding-left: 18px; color: var(--muted); line-height: 1.7; }}
    .headline-list li {{ margin-bottom: 8px; }}
    .headline-list a {{ color: #dbeafe; text-decoration: none; }}
    .headline-list span {{ color: var(--warn); font-size: 12px; margin-left: 8px; }}
    .focus {{
      font-size: 18px;
      font-weight: 700;
      color: #f8fafc;
    }}
    .focus span {{
      display: inline-block;
      margin-left: 10px;
      color: var(--warn);
      font-size: 13px;
      font-weight: 600;
    }}
    .meta {{
      margin-top: 10px;
      color: var(--muted);
      font-size: 13px;
    }}
    .two {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 18px;
      margin-top: 24px;
    }}
    code {{
      color: #86efac;
      background: rgba(255,255,255,.03);
      padding: 2px 6px;
      border-radius: 6px;
    }}
    .chips {{ display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 12px; }}
    .chip {{
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 12px;
      border: 1px solid var(--border);
      background: rgba(255,255,255,.04);
      color: var(--muted);
    }}
    .chip-ok {{ color: #bbf7d0; }}
    .chip-bad {{ color: #fecaca; }}
    .chip-soft {{ color: #bfdbfe; }}
    .match-detail {{
      margin-top: 14px;
      border: 1px solid rgba(255,255,255,.08);
      border-radius: 16px;
      background: rgba(255,255,255,.025);
      overflow: hidden;
    }}
    .match-detail summary {{
      list-style: none;
      cursor: pointer;
      padding: 18px 20px;
      display: grid;
      gap: 6px;
      background: linear-gradient(180deg, rgba(255,255,255,.03), rgba(255,255,255,.01));
    }}
    .match-detail summary::-webkit-details-marker {{ display: none; }}
    .match-title {{
      font-size: 18px;
      font-weight: 800;
      color: #f8fafc;
    }}
    .match-sub {{
      color: var(--muted);
      font-size: 13px;
    }}
    .match-detail-body {{
      padding: 0 20px 20px;
    }}
    .detail-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 14px;
      margin-top: 14px;
    }}
    .detail-grid.three {{
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
    }}
    .detail-grid.two-strong {{
      grid-template-columns: minmax(260px, 1fr) minmax(360px, 1.4fr);
    }}
    .detail-card {{
      padding: 16px;
      border: 1px solid rgba(255,255,255,.06);
      border-radius: 14px;
      background: rgba(255,255,255,.03);
    }}
    .detail-card h3 {{
      margin: 0 0 10px;
      font-size: 15px;
      color: #dbeafe;
    }}
    .jornada-header {{
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 16px;
      flex-wrap: wrap;
      margin-bottom: 14px;
    }}
    .detail-stack {{ margin-top: 18px; }}
    .mini-two {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
    }}
    .mini-title {{
      color: #f8fafc;
      font-size: 13px;
      font-weight: 700;
      margin-bottom: 8px;
    }}
    .briefing {{
      white-space: pre-wrap;
      margin: 0;
      font-family: Consolas, "Cascadia Code", monospace;
      font-size: 12px;
      line-height: 1.6;
      color: #dbeafe;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    td, th {{
      text-align: left;
      padding: 10px 12px;
      border-top: 1px solid rgba(255,255,255,.06);
      vertical-align: top;
    }}
    th {{ color: var(--muted); font-weight: 600; }}
    a {{ color: #93c5fd; text-decoration: none; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div class="hero-top">
        <h1>QUINIAI WORKER</h1>
        <div class="badge"><span class="dot"></span> Estado: {status_label}</div>
      </div>
      <div class="sub">
        Ultimo snapshot correcto: <strong>{_html_escape(_format_madrid_datetime(status_payload.get("snapshot_generated_at", "")))}</strong><br>
        Hora tecnica UTC: <strong>{_html_escape(status_payload.get("snapshot_generated_at", ""))}</strong><br>
        Intervalo de actualizacion: <strong>{_html_escape(status_payload.get("poll_seconds", ""))}</strong> segundos<br>
        Este worker sigue la <strong>quiniela oficial via Eduardo Losilla</strong>, agrupa partidos por jornadas, enriquece cada cruce con contexto externo y guarda una ventana rodante de jornadas para que no desaparezcan al terminar los partidos.
      </div>
      <div class="grid">{cards_html}</div>
    </div>

    <div class="two">
      <div class="section">
        <h2>Que hace exactamente</h2>
        <ul>
          <li>Lee cuotas y feed de partidos actuales.</li>
          <li>Sigue jornadas oficiales de quiniela y mete tambien partidos ingleses o de Segunda cuando entren en esa jornada.</li>
          <li>Busca noticias, clima, viajes, historicos, lesiones, web oficial y arbitros cuando hay fuente gratuita fiable.</li>
          <li>Guarda una ventana rodante de jornadas en <code>quiniela_jornadas</code> para no perder partidos ya jugados.</li>
          <li>Actualiza una base estructurada local y borra partidos viejos que ya no interesan.</li>
          <li>Sube todo el contexto a <code>/admin/ia-feed</code>.</li>
        </ul>
      </div>

      <div class="section">
        <h2>Como usarlo</h2>
        <ul>
          <li>Para verlo todo bonito: doble clic en <code>Abrir Panel QuiniAI.cmd</code>.</li>
          <li>Para arrancarlo manualmente con consola visual: doble clic en <code>Iniciar QuiniAI Worker.cmd</code>.</li>
          <li>Para revisar si va bien: doble clic en <code>Ver Salud QuiniAI.cmd</code>.</li>
          <li>El autoarranque ya queda en Windows y se relanza al iniciar sesion.</li>
        </ul>
      </div>
    </div>

    <div class="section">
      <h2>Jornada actual</h2>
      <div class="focus">{_html_escape(current_jornada_label)}</div>
      <div class="meta">
        Equipos en base estructurada: {_html_escape(structured.get("teams", 0))} |
        Partidos vivos en base estructurada: {_html_escape(structured.get("matches", 0))} |
        Arbitros detectados: {_html_escape(structured.get("referees", 0))} |
        Slots oficiales sin casar: {_html_escape(coverage.get("quiniela_unmatched_slots", 0))}
      </div>
    </div>

    <div class="section">
      <h2>Ir Directo A Jornadas</h2>
      <div class="chips">{jornada_nav}</div>
    </div>

    <div class="section">
      <h2>Fuentes integradas</h2>
      <table>
        <thead>
          <tr><th>Fuente</th><th>URL</th></tr>
        </thead>
        <tbody>
          {source_rows}
        </tbody>
      </table>
    </div>

    <div class="section">
      <h2>Historial de ejecuciones</h2>
      <table>
        <thead>
          <tr><th>Estado</th><th>Terminó</th><th>Segundos</th><th>Jornada</th><th>Partidos</th><th>Error</th></tr>
        </thead>
        <tbody>
          {run_rows}
        </tbody>
      </table>
      <div class="meta">
        Log worker: {_html_escape(str(WORKER_LOG_PATH))} |
        Log supervisor: {_html_escape(str(SUPERVISOR_LOG_PATH))}
      </div>
    </div>

    <div class="section">
      <h2>Resumen de la jornada actual</h2>
      <table>
        <thead>
          <tr><th>Partido</th><th>Liga</th><th>Mercado</th><th>Gap descenso</th><th>Descanso</th><th>Arbitro</th><th>Indice presion</th></tr>
        </thead>
        <tbody>
          {focus_rows}
        </tbody>
      </table>
    </div>

    {jornada_blocks}

    <div class="section">
      <h2>Titulares por competicion</h2>
      <div class="two">
        {headline_blocks}
      </div>
    </div>
  </div>
</body>
</html>"""


def _build_monitor_web_html() -> str:
    return """<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>QuiniAI Monitor</title>
  <style>
    :root{--bg:#07101d;--panel:#122033;--panel2:#182b44;--text:#eef6ff;--muted:#96aac4;--ok:#22c55e;--warn:#f59e0b;--bad:#ef4444;--border:#243754;--accent:#38bdf8}
    *{box-sizing:border-box} body{margin:0;font-family:Consolas,\"Cascadia Code\",monospace;background:radial-gradient(circle at top right,rgba(34,197,94,.12),transparent 28%),radial-gradient(circle at bottom left,rgba(56,189,248,.12),transparent 32%),var(--bg);color:var(--text)}
    .wrap{max-width:980px;margin:0 auto;padding:20px}
    .hero,.panel{background:linear-gradient(180deg,var(--panel),var(--panel2));border:1px solid var(--border);border-radius:18px;box-shadow:0 20px 60px rgba(0,0,0,.28)}
    .hero{padding:22px}.top{display:flex;justify-content:space-between;align-items:center;gap:16px;flex-wrap:wrap}
    h1,h2{margin:0} h1{font-size:28px} h2{font-size:18px;margin-bottom:12px}
    .muted{color:var(--muted);line-height:1.5}
    .badge{display:inline-flex;align-items:center;gap:10px;padding:10px 14px;border-radius:999px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08);font-weight:700}
    .dot{width:12px;height:12px;border-radius:50%;background:var(--ok);box-shadow:0 0 14px var(--ok)}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-top:18px}
    .card{padding:14px;border:1px solid var(--border);border-radius:14px;background:rgba(255,255,255,.03)}
    .k{font-size:12px;color:var(--muted);text-transform:uppercase}.v{font-size:28px;font-weight:800;margin-top:8px}
    .stack{display:grid;gap:16px;margin-top:18px}.panel{padding:18px}
    table{width:100%;border-collapse:collapse;font-size:14px} th,td{padding:10px 8px;border-bottom:1px solid rgba(255,255,255,.08);text-align:left;vertical-align:top}
    .chips{display:flex;gap:8px;flex-wrap:wrap}.chip{padding:7px 10px;border-radius:999px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08);font-size:12px}
    .ok{color:var(--ok)} .warn{color:var(--warn)} .bad{color:var(--bad)}
    .jornada{border:1px solid var(--border);border-radius:16px;background:rgba(255,255,255,.03);margin-top:14px;overflow:hidden}
    .jornada summary{cursor:pointer;list-style:none;padding:16px 18px;font-weight:700;display:flex;justify-content:space-between;align-items:center;gap:12px}
    .jornada summary::-webkit-details-marker{display:none}
    .jornada-body{padding:0 18px 18px}
    .match{border:1px solid rgba(255,255,255,.08);border-radius:14px;padding:14px;background:rgba(0,0,0,.12);margin-top:12px}
    .match-head{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap}
    .match-title{font-size:16px;font-weight:700}
    .mini{font-size:12px;color:var(--muted);line-height:1.5}
    .line{margin-top:8px;font-size:13px;line-height:1.5}
    .line strong{color:var(--accent)}
    .brief{white-space:pre-wrap;font-size:12px;line-height:1.45;margin-top:10px;padding:10px;border-radius:12px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.06)}
    @media (max-width:640px){.v{font-size:24px}table{font-size:12px}.match-title{font-size:15px}}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div class="top">
        <div>
          <h1>QuiniAI Monitor</h1>
          <div class="muted">Panel ligero para comprobar desde movil si el worker sigue vivo, cuando subio el ultimo snapshot y si la integridad de las jornadas sigue correcta.</div>
        </div>
        <div class="badge"><span class="dot" id="dot"></span><span id="alive">Cargando...</span></div>
      </div>
      <div class="grid" id="cards"></div>
    </div>
    <div class="stack">
      <div class="panel">
        <h2>Estado actual</h2>
        <div class="chips" id="status-chips"></div>
      </div>
      <div class="panel" id="audit-panel" style="display:none">
        <h2>Auditoría avanzada</h2>
        <div class="muted" id="audit-meta"></div>
        <div class="chips" id="audit-chips" style="margin-top:12px"></div>
      </div>
      <div class="panel">
        <h2>Ultimas ejecuciones</h2>
        <table>
          <thead><tr><th>Estado</th><th>Hora Madrid</th><th>Duracion</th><th>Jornada</th><th>Partidos</th></tr></thead>
          <tbody id="runs"></tbody>
        </table>
      </div>
      <div class="panel">
        <h2>Analisis publico: jornada actual y siguiente</h2>
        <div class="muted">Aqui puedes abrir la jornada actual y la siguiente publicada para ver si el monitor realmente esta recogiendo contexto util, no solo horas.</div>
        <div id="jornadas"></div>
      </div>
    </div>
  </div>
  <script>
    const fmtMadrid = (iso) => {
      if (!iso) return "-";
      try {
        return new Intl.DateTimeFormat("es-ES",{timeZone:"Europe/Madrid",dateStyle:"short",timeStyle:"medium"}).format(new Date(iso));
      } catch { return iso; }
    };
    const ageMinutes = (iso) => {
      if (!iso) return "-";
      const ms = Date.now() - new Date(iso).getTime();
      return (ms / 60000).toFixed(1);
    };
    const fmtPct = (value) => value === null || value === undefined || value === "" ? "-" : `${Number(value).toFixed(2)}%`;
    const fmtNum = (value, digits=1) => value === null || value === undefined || value === "" || Number.isNaN(Number(value)) ? "-" : Number(value).toFixed(digits);
    const chip = (text, cls="") => `<span class="chip ${cls}">${text}</span>`;
    const escapeHtml = (value) => String(value ?? "").replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;");
    const renderMatch = (match) => {
      const homeTable = match.home_table || {};
      const awayTable = match.away_table || {};
      const homePressure = (match.pressure || {}).home || {};
      const awayPressure = (match.pressure || {}).away || {};
      const homeFatigue = (match.fatigue || {}).home || {};
      const awayFatigue = (match.fatigue || {}).away || {};
      const odds = match.odds || {};
      const market = match.normalized_percent || {};
      const official = match.official_percent || {};
      const weather = match.weather || {};
      const referee = match.referee || {};
      const competitive = match.competitive_context || {};
      const homeObjective = competitive.home_objective || {};
      const awayObjective = competitive.away_objective || {};
      const homeRotation = competitive.home_rotation_context || {};
      const awayRotation = competitive.away_rotation_context || {};
      const rotationLine = [homeRotation.reason ? `local ${homeRotation.reason} (${homeRotation.risk})` : "", awayRotation.reason ? `visitante ${awayRotation.reason} (${awayRotation.risk})` : ""].filter(Boolean).join(" | ");
      return `
        <div class="match">
          <div class="match-head">
            <div>
              <div class="match-title">${escapeHtml(match.slot)} ${escapeHtml(match.local)} vs ${escapeHtml(match.visitante)}</div>
              <div class="mini">${escapeHtml(match.league || "-")} · ${fmtMadrid(match.kickoff)} · ${escapeHtml(match.bookmaker || "sin bookmaker")}</div>
            </div>
            <div class="chips">
              ${chip(`1 ${fmtNum(odds["1"],2)}`)}
              ${chip(`X ${fmtNum(odds["X"],2)}`)}
              ${chip(`2 ${fmtNum(odds["2"],2)}`)}
            </div>
          </div>
          <div class="line"><strong>Mercado base:</strong> 1=${fmtPct(market["1"])} · X=${fmtPct(market["X"])} · 2=${fmtPct(market["2"])} | <strong>Quinielista:</strong> 1=${fmtPct(official["1"])} · X=${fmtPct(official["X"])} · 2=${fmtPct(official["2"])}</div>
          <div class="line"><strong>Tabla:</strong> ${escapeHtml(match.local)} ${homeTable.position ?? "-"}º (${homeTable.points ?? "-"} pts, ${escapeHtml(homeTable.form || "-")}) | ${escapeHtml(match.visitante)} ${awayTable.position ?? "-"}º (${awayTable.points ?? "-"} pts, ${escapeHtml(awayTable.form || "-")})</div>
          <div class="line"><strong>Objetivo:</strong> ${escapeHtml(homeObjective.summary || "-")} [MW ${competitive.home_must_win_index ?? 0}, NP ${competitive.home_must_not_lose_index ?? 0}] | ${escapeHtml(awayObjective.summary || "-")} [MW ${competitive.away_must_win_index ?? 0}, NP ${competitive.away_must_not_lose_index ?? 0}]</div>
          <div class="line"><strong>Contexto competitivo:</strong> ${escapeHtml(competitive.competitive_stakes_label || "-")} | rivalidad ${competitive.direct_rivalry_index ?? 0}/100</div>
          ${rotationLine ? `<div class="line"><strong>Rotacion probable:</strong> ${escapeHtml(rotationLine)}</div>` : ""}
          <div class="line"><strong>Presion/Fatiga:</strong> local ${fmtNum(homePressure.score,2)} (${escapeHtml(homePressure.label || "-")}) y visitante ${fmtNum(awayPressure.score,2)} (${escapeHtml(awayPressure.label || "-")}) | fatiga ${fmtNum(homeFatigue.score,2)} / ${fmtNum(awayFatigue.score,2)}</div>
          <div class="line"><strong>Arbitro:</strong> ${escapeHtml(referee.name || "no confirmado")} | <strong>Viaje visitante:</strong> ${fmtNum(match.travel_km,1)} km | <strong>Clima:</strong> ${fmtNum(weather.temperature_c,1)} C, lluvia ${weather.precipitation_probability ?? "-"}%, viento ${fmtNum(weather.wind_speed_kmh,1)} km/h</div>
          <div class="line"><strong>Proximos 5 ${escapeHtml(match.local)}:</strong> ${escapeHtml(match.future_home || "sin calendario detectado")}</div>
          <div class="line"><strong>Proximos 5 ${escapeHtml(match.visitante)}:</strong> ${escapeHtml(match.future_away || "sin calendario detectado")}</div>
          <div class="line"><strong>Sesgo arbitral:</strong> ${escapeHtml(referee.bias_summary || "sin historico arbitral fiable")}</div>
          ${match.briefing_excerpt ? `<div class="brief">${escapeHtml(match.briefing_excerpt)}</div>` : `<div class="brief">Esta jornada ya esta publicada pero este partido todavia no tiene briefing enriquecido en el monitor publico.</div>`}
        </div>`;
    };
    async function render() {
      const response = await fetch(`status.json?t=${Date.now()}`, {cache:"no-store"});
      const status = await response.json();
      const coverage = status.coverage || {};
      const integrity = status.quiniela_integrity || {};
      document.getElementById("alive").textContent = status.ok ? "Worker sano" : "Worker con error";
      document.getElementById("dot").style.background = status.ok ? "var(--ok)" : "var(--bad)";
      document.getElementById("dot").style.boxShadow = status.ok ? "0 0 14px var(--ok)" : "0 0 14px var(--bad)";
      document.getElementById("cards").innerHTML = [
        ["Ultimo snapshot", fmtMadrid(status.snapshot_generated_at)],
        ["Antiguedad", `${ageMinutes(status.snapshot_generated_at)} min`],
        ["Jornada actual", coverage.quiniela_current_jornada ?? "-"],
        ["Ultima oficial", coverage.quiniela_latest_available_jornada ?? "-"],
        ["Partidos rastreados", coverage.tracked_quiniela_matches ?? "-"],
        ["Integridad", integrity.ok ? "OK" : "ERROR"],
      ].map(([k,v]) => `<div class="card"><div class="k">${k}</div><div class="v">${v}</div></div>`).join("");
      document.getElementById("status-chips").innerHTML = [
        chip(`Poll ${status.poll_seconds || "-"}s`, "ok"),
        chip(`Fuentes ${coverage.sources_ok || 0}/${coverage.sources_total || 0}`, "ok"),
        chip(`Weather ${coverage.weather_matches || 0}`),
        chip(`Travel ${coverage.travel_matches || 0}`),
        chip(`History ${coverage.history_matches || 0}`),
        chip(`Referees ${coverage.structured_referees || 0}`),
        chip(`Slots ${integrity.checked_slots || 0}`),
        chip(`Fallos ${integrity.mismatch_count || 0}`, integrity.ok ? "ok" : "bad"),
      ].join("");
      const runs = (status.last_runs || []).slice(-8).reverse();
      document.getElementById("runs").innerHTML = runs.map((run) => `
        <tr>
          <td class="${run.ok ? "ok" : "bad"}">${run.ok ? "OK" : "ERROR"}</td>
          <td>${fmtMadrid(run.finished_at)}</td>
          <td>${run.duration_seconds ?? "-" }s</td>
          <td>${run.current_jornada ?? "-"}</td>
          <td>${run.tracked_matches ?? "-"}</td>
        </tr>`).join("") || "<tr><td colspan='5'>Sin historial.</td></tr>";
      const jornadas = status.public_jornadas || [];
      document.getElementById("jornadas").innerHTML = jornadas.map((jornada, index) => `
        <details class="jornada" ${index === 0 ? "open" : ""}>
          <summary>
            <span>${escapeHtml(jornada.label || "Jornada")} ${jornada.is_current ? "· actual" : "· siguiente/publicada"}</span>
            <span class="mini">${fmtMadrid(jornada.kickoff_from)} ${jornada.history_only ? "· historico/cache" : ""}</span>
          </summary>
          <div class="jornada-body">
            ${(jornada.matches || []).map(renderMatch).join("") || "<div class='match'>Sin partidos publicados.</div>"}
          </div>
        </details>`).join("") || "<div class='match'>Todavia no hay jornadas publicas preparadas.</div>";

      const audit = status.audit_news_quality || status.auditoria_avanzada || status.audit || null;
      const auditPanel = document.getElementById("audit-panel");
      if (auditPanel && audit) {
        auditPanel.style.display = "block";
        const lang = audit.news_language || (coverage.news_language ?? "-");
        const country = audit.news_country || (coverage.news_country ?? "-");
        document.getElementById("audit-meta").textContent =
          `Curación de noticias activa · hl=${lang} · gl=${country}`;
        const hi = (audit.high_trust_domains || []).length;
        const lo = (audit.low_trust_domains || []).length;
        document.getElementById("audit-chips").innerHTML = [
          chip(`High-trust domains ${hi}`, "ok"),
          chip(`Blocked domains ${lo}`, "warn"),
          chip(`Fuentes sanas ${coverage.sources_ok || 0}/${coverage.sources_total || 0}`),
          chip(`Titulares frescos ${coverage.fresh_headlines || 0}`),
        ].join("");
      }
    }
    render();
    setInterval(render, 60000);
  </script>
</body>
</html>"""


def _build_monitor_status_payload(status_payload: dict) -> dict:
    coverage = status_payload.get("coverage") or {}
    structured = status_payload.get("structured_db_summary") or {}
    integrity = status_payload.get("quiniela_integrity") or {}
    payload = {
        "generated_at": status_payload.get("generated_at", ""),
        "snapshot_generated_at": status_payload.get("snapshot_generated_at", ""),
        "ok": bool(status_payload.get("ok")),
        "last_error": status_payload.get("last_error", ""),
        "poll_seconds": status_payload.get("poll_seconds"),
        "coverage": {
            "monitored_matches": coverage.get("monitored_matches"),
            "focus_matches": coverage.get("focus_matches"),
            "tracked_quiniela_matches": coverage.get("tracked_quiniela_matches"),
            "quiniela_jornadas": coverage.get("quiniela_jornadas"),
            "quiniela_current_jornada": coverage.get("quiniela_current_jornada"),
            "quiniela_latest_available_jornada": coverage.get("quiniela_latest_available_jornada"),
            "weather_matches": coverage.get("weather_matches"),
            "travel_matches": coverage.get("travel_matches"),
            "history_matches": coverage.get("history_matches"),
            "sources_total": coverage.get("sources_total"),
            "sources_ok": coverage.get("sources_ok"),
            "fresh_headlines": coverage.get("fresh_headlines"),
            "structured_focus_matches": coverage.get("structured_focus_matches"),
            "structured_referees": coverage.get("structured_referees"),
        },
        "structured_db_summary": {
            "teams": structured.get("teams"),
            "matches": structured.get("matches"),
            "referees": structured.get("referees"),
            "last_pruned_at": structured.get("last_pruned_at"),
        },
        "quiniela_integrity": {
            "ok": integrity.get("ok"),
            "checked_slots": integrity.get("checked_slots"),
            "mismatch_count": integrity.get("mismatch_count"),
        },
        "last_runs": (status_payload.get("last_runs") or [])[-12:],
        "public_jornadas": _build_monitor_public_jornadas(status_payload),
    }
    for key, value in status_payload.items():
        normalized_key = _normalize_ascii(str(key)).lower()
        if any(
            token in normalized_key
            for token in ["audit", "auditoria", "price", "precio", "quality", "calidad"]
        ):
            payload[key] = value
    return payload


def _monitor_github_headers() -> dict | None:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "QuiniAI-Monitor-Publisher/1.0",
    }
    if MONITOR_GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {MONITOR_GITHUB_TOKEN}"
        return headers
    git_candidates = [
        os.getenv("QUINIAI_GIT_EXE", "").strip(),
        shutil.which("git") or "",
        r"C:\Program Files\Git\cmd\git.exe",
        r"C:\Program Files\Git\bin\git.exe",
    ]
    for candidate in git_candidates:
        if not candidate:
            continue
        git_path = Path(candidate)
        if not git_path.exists():
            continue
        try:
            result = subprocess.run(
                [str(git_path), "credential", "fill"],
                input="protocol=https\nhost=github.com\n\n",
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=12,
                check=True,
            )
            parsed = {}
            for line in result.stdout.splitlines():
                if "=" not in line:
                    continue
                key, value = line.split("=", 1)
                parsed[key.strip()] = value.strip()
            username = parsed.get("username", "")
            password = parsed.get("password", "")
            if username and password:
                auth = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
                headers["Authorization"] = f"Basic {auth}"
                return headers
        except Exception:
            continue
    return None


def _github_monitor_upsert(repo_path: str, content: str) -> bool:
    headers = _monitor_github_headers()
    if not headers or not MONITOR_REPO:
        return False
    local_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    state_entry = ((MONITOR_PUBLISH_STATE or {}).setdefault("files", {})).get(repo_path) or {}
    if (
        state_entry.get("local_hash") == local_hash
        and state_entry.get("repo") == MONITOR_REPO
        and state_entry.get("branch") == MONITOR_BRANCH
    ):
        return False
    quoted_path = urllib.parse.quote(repo_path, safe="/")
    url = f"https://api.github.com/repos/{MONITOR_REPO}/contents/{quoted_path}"
    existing_sha = None
    try:
        response = requests.get(
            url,
            headers=headers,
            params={"ref": MONITOR_BRANCH},
            timeout=25,
        )
        if response.status_code == 200:
            existing_sha = (response.json() or {}).get("sha")
        elif response.status_code != 404:
            response.raise_for_status()
    except Exception as exc:
        LOGGER.warning("monitor_publish_metadata_failed path=%s error=%s", repo_path, exc)
        return False
    payload = {
        "message": f"Update public monitor: {repo_path}",
        "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
        "branch": MONITOR_BRANCH,
    }
    if existing_sha:
        payload["sha"] = existing_sha
    try:
        response = requests.put(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        response_payload = response.json() or {}
        content_payload = response_payload.get("content") or {}
        MONITOR_PUBLISH_STATE.setdefault("files", {})[repo_path] = {
            "local_hash": local_hash,
            "repo": MONITOR_REPO,
            "branch": MONITOR_BRANCH,
            "sha": content_payload.get("sha", existing_sha or ""),
            "published_at": _now_iso(),
        }
        _save_cache(MONITOR_PUBLISH_STATE_PATH, MONITOR_PUBLISH_STATE)
        return True
    except Exception as exc:
        LOGGER.warning("monitor_publish_failed path=%s error=%s", repo_path, exc)
        return False


def publish_monitor_site() -> None:
    if not MONITOR_PUBLISH_ENABLED:
        return
    last_published = _parse_iso_datetime(
        str((MONITOR_PUBLISH_STATE.get("meta") or {}).get("last_publish_attempt_at", "")).strip()
    )
    if last_published:
        age_seconds = (datetime.now(timezone.utc) - last_published).total_seconds()
        if age_seconds < MONITOR_PUBLISH_MIN_SECONDS:
            LOGGER.info(
                "monitor_publish_throttled age_seconds=%s min_seconds=%s",
                round(age_seconds, 1),
                MONITOR_PUBLISH_MIN_SECONDS,
            )
            return
    files_to_publish = [("docs/monitor/status.json", MONITOR_STATUS_JSON_PATH)]
    if MONITOR_PUBLISH_INDEX:
        files_to_publish.insert(0, ("docs/monitor/index.html", MONITOR_INDEX_PATH))
    updated_any = False
    for repo_path, local_path in files_to_publish:
        try:
            content = local_path.read_text(encoding="utf-8")
        except Exception as exc:
            LOGGER.warning("monitor_publish_read_failed path=%s error=%s", local_path, exc)
            continue
        updated_any = _github_monitor_upsert(repo_path, content) or updated_any
    MONITOR_PUBLISH_STATE.setdefault("meta", {})["last_publish_attempt_at"] = _now_iso()
    _save_cache(MONITOR_PUBLISH_STATE_PATH, MONITOR_PUBLISH_STATE)
    if updated_any:
        LOGGER.info("monitor_publish_ok repo=%s branch=%s", MONITOR_REPO, MONITOR_BRANCH)
        return

    # Sin token (`QUINIAI_GITHUB_TOKEN`) ni credenciales API disponibles:
    # intentamos publicar via git (usara el Git Credential Manager del equipo).
    try:
        if not _monitor_github_headers():
            _git_publish_monitor([repo_path for repo_path, _ in files_to_publish])
    except Exception as exc:
        LOGGER.warning("monitor_publish_git_fallback_failed error=%s", exc)


def _git_publish_monitor(repo_paths: list[str]) -> bool:
    repo_root = Path(__file__).resolve().parent
    git_candidates = [
        os.getenv("QUINIAI_GIT_EXE", "").strip(),
        shutil.which("git") or "",
        r"C:\Program Files\Git\cmd\git.exe",
        r"C:\Program Files\Git\bin\git.exe",
    ]
    git_exe = next((c for c in git_candidates if c and Path(c).exists()), "")
    if not git_exe:
        LOGGER.warning("monitor_git_publish_no_git")
        return False
    try:
        ok = subprocess.run(
            [git_exe, "-C", str(repo_root), "rev-parse", "--is-inside-work-tree"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=12,
            check=True,
        )
        if "true" not in (ok.stdout or "").strip().lower():
            return False
    except Exception:
        return False
    for rel_path in repo_paths:
        subprocess.run(
            [git_exe, "-C", str(repo_root), "add", "--", rel_path],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=12,
        )
    porcelain = subprocess.run(
        [git_exe, "-C", str(repo_root), "status", "--porcelain"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=12,
    )
    if not (porcelain.stdout or "").strip():
        return False
    subprocess.run(
        [
            git_exe,
            "-C",
            str(repo_root),
            "-c",
            "user.name=QuiniAIWorker",
            "-c",
            "user.email=worker@quiniai.local",
            "commit",
            "-m",
            "Update monitor snapshot",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=20,
    )
    pushed = subprocess.run(
        [git_exe, "-C", str(repo_root), "push", "origin", f"HEAD:{MONITOR_BRANCH}"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=60,
    )
    if pushed.returncode != 0:
        LOGGER.warning("monitor_git_publish_push_failed stderr=%s", (pushed.stderr or "").strip())
        return False
    LOGGER.info("monitor_git_publish_ok branch=%s", MONITOR_BRANCH)
    return True


def write_status_files(snapshot: dict | None = None, error: str = "") -> None:
    timestamp = _now_iso()
    status_payload = {
        "generated_at": timestamp,
        "last_error": error,
        "ok": bool(snapshot) and not error,
        "poll_seconds": POLL_SECONDS,
        "last_runs": list((RUN_HISTORY or {}).get("runs", []))[-12:],
    }
    if snapshot:
        lines = _snapshot_summary_lines(snapshot)
        status_text = "\n".join(lines) + "\n"
        status_payload.update(
            {
                "snapshot_generated_at": snapshot.get("generated_at", ""),
                "coverage": snapshot.get("coverage", {}),
                "structured_db_summary": snapshot.get("structured_db_summary", {}),
                "competition_headlines": snapshot.get("competition_headlines", {}),
                "context_sources": snapshot.get("context_sources", []),
                "source_health_summary": snapshot.get("source_health_summary", {}),
                "quiniela_jornadas": snapshot.get("quiniela_jornadas", []),
                "focus_matches": snapshot.get("quiniela_focus_matches", []),
                "quiniela_integrity": snapshot.get("quiniela_integrity", {}),
                "last_runs": list((RUN_HISTORY or {}).get("runs", []))[-12:],
            }
        )
        # Exponer auditorías/costes/calidad en el monitor público (GitHub Pages).
        for key, value in snapshot.items():
            normalized_key = _normalize_ascii(str(key)).lower()
            if any(token in normalized_key for token in ["audit", "auditoria", "price", "precio", "quality", "calidad", "cost", "coste"]):
                status_payload[key] = value
    else:
        status_text = (
            "QUINIAI WORKER STATUS\n"
            f"Generated at: {timestamp}\n"
            f"Last error: {error}\n"
        )
    _write_text_file(STATUS_FILE_PATH, status_text)
    _write_text_file(DESKTOP_STATUS_FILE_PATH, status_text)
    _write_json_file(STATUS_JSON_PATH, status_payload)
    _write_json_file(DESKTOP_STATUS_JSON_PATH, status_payload)
    _write_json_file(MONITOR_STATUS_JSON_PATH, _build_monitor_status_payload(status_payload))
    html = _build_status_html(status_payload)
    _write_text_file(STATUS_HTML_PATH, html)
    _write_text_file(DESKTOP_STATUS_HTML_PATH, html)
    _write_text_file(MONITOR_INDEX_PATH, _build_monitor_web_html())
    publish_monitor_site()


def print_pretty_summary(snapshot: dict) -> None:
    lines = _snapshot_summary_lines(snapshot)
    print("=" * 64)
    print("QUINIAI WORKER :: LIVE SNAPSHOT OK")
    print("=" * 64)
    for line in lines[1:]:
        print(f"> {line}")
    print("=" * 64)


def _strip_google_suffix(title: str) -> str:
    cleaned = title.strip()
    if cleaned.endswith(" - Google News"):
        cleaned = cleaned[: -len(" - Google News")].strip()
    return cleaned


def _safe_url_host(value: str) -> str:
    url = str(value or "").strip()
    if not url:
        return ""
    try:
        parsed = urllib.parse.urlparse(url)
        host = (parsed.netloc or "").strip().lower()
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def _resolve_google_news_link(value: str) -> str:
    """Google News RSS suele traer URLs intermedias; intentamos extraer el destino real.

    - /rss/articles/... a veces incluye ?url=<destino>
    - algunos enlaces usan /articles/.. con query ?url=
    """
    url = str(value or "").strip()
    if not url:
        return ""
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return url
    if "news.google.com" not in (parsed.netloc or ""):
        return url
    params = urllib.parse.parse_qs(parsed.query or "")
    # RSS suele traer `url` o `q` apuntando al destino.
    candidate = (params.get("url") or params.get("q") or [""])[0]
    candidate = str(candidate or "").strip()
    if candidate.startswith("http"):
        return candidate
    return url


def _enrich_news_item(item: dict) -> dict:
    enriched = dict(item or {})
    link = str(enriched.get("link", "")).strip()
    resolved = _resolve_google_news_link(link)
    if resolved and resolved != link:
        enriched["link"] = resolved
    enriched["_domain"] = _safe_url_host(enriched.get("link", ""))
    return enriched


def _normalize_ascii(value: str) -> str:
    return unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")


def _normalize_team_name(value: str) -> str:
    lowered = _normalize_ascii(value).lower()
    lowered = lowered.replace("&", " and ")
    for token in [" football club ", " club de futbol ", " fc ", " cf ", " afc ", " sc ", " cd ", " sd ", " ud ", " rcde ", " rcd ", " ca "]:
        lowered = lowered.replace(token, " ")
    lowered = f" {lowered} "
    lowered = re.sub(r"[^a-z0-9 ]+", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered


def _canonical_team_name(value: str) -> str:
    normalized = _normalize_team_name(value)
    return TEAM_NAME_ALIASES.get(normalized, value)


def _team_similarity_score(left: str, right: str) -> float:
    left_norm = _normalize_team_name(_canonical_team_name(left))
    right_norm = _normalize_team_name(_canonical_team_name(right))
    if not left_norm or not right_norm:
        return 0.0
    if left_norm == right_norm:
        return 1.0
    if left_norm in right_norm or right_norm in left_norm:
        return 0.93
    left_tokens = set(left_norm.split())
    right_tokens = set(right_norm.split())
    if not left_tokens or not right_tokens:
        return 0.0
    if left_tokens <= right_tokens or right_tokens <= left_tokens:
        return 0.92
    token_score = len(left_tokens & right_tokens) / len(left_tokens | right_tokens)
    ratio_score = difflib.SequenceMatcher(a=left_norm, b=right_norm).ratio()
    return max(token_score, ratio_score)


def _looks_like_known_team_entity(candidate: str) -> bool:
    normalized = _normalize_team_name(candidate)
    if not normalized:
        return False
    for known in set(TEAM_NAME_ALIASES.keys()) | set(TEAM_NAME_ALIASES.values()):
        if difflib.SequenceMatcher(a=normalized, b=_normalize_team_name(known)).ratio() >= 0.92:
            return True
    return False


def _guess_country_hint(team_name: str, fallback: str | None = None) -> str | None:
    if fallback:
        return fallback
    lower_name = team_name.lower()
    if any(
        token in lower_name
        for token in [" madrid", " bilbao", " barcelona", " sevilla", " osasuna", " gijon", " malaga"]
    ):
        return "ES"
    if any(token in lower_name for token in [" united", " city", " town", " rovers", " albion", " wednesday"]):
        return "GB"
    return None


def _team_location_override(team_name: str) -> dict:
    normalized = _normalize_ascii(str(team_name or "").strip()).lower()
    if not normalized:
        return {}
    return TEAM_LOCATION_OVERRIDES.get(normalized, {})


def _apply_location_override_fields(profile: dict, team_name: str) -> dict:
    enriched = dict(profile or {})
    override = _team_location_override(team_name)
    if not override:
        return enriched
    for field in ["city", "country", "country_code", "timezone", "latitude", "longitude"]:
        if override.get(field) not in {None, ""} and (
            field in {"latitude", "longitude"} or enriched.get(field) in {None, ""}
        ):
            enriched[field] = override.get(field)
    if not str(enriched.get("location_hint", "")).strip() and override.get("query"):
        enriched["location_hint"] = override.get("query", "")
    return enriched


def _extract_location_hint(summary: str) -> str:
    patterns = [
        r"based in ([^.;]+)",
        r"from ([^.;]+)",
        r"located in ([^.;]+)",
        r"plays in ([^.;]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, summary, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""


def _clean_location_hint(location_hint: str) -> str:
    if not location_hint:
        return ""
    cleaned = re.split(r"\bthat\b|\bwhich\b|\bwhere\b|\bcompetes\b", location_hint, maxsplit=1)[0]
    cleaned = cleaned.replace("\n", " ").strip(" ,.;")
    return cleaned


def _search_wikipedia_title(team_name: str, country_hint: str | None = None) -> str:
    country_label = COUNTRY_LABELS.get(country_hint or "", "")
    queries = [
        f"{team_name} {country_label} football club".strip(),
        f"{team_name} football club",
        f"{team_name} {country_label} soccer club".strip(),
        f"{team_name} soccer club",
        team_name,
    ]
    for query in queries:
        try:
            data = _request_json(
                WIKI_API_URL,
                params={
                    "action": "opensearch",
                    "search": query,
                    "limit": 5,
                    "namespace": 0,
                    "format": "json",
                },
                timeout=20,
            )
        except Exception:
            continue
        if isinstance(data, list) and len(data) > 1 and data[1]:
            return str(data[1][0]).strip()
    return team_name


def _fetch_wikipedia_page_data(title: str) -> dict:
    try:
        data = _request_json(
            WIKI_API_URL,
            params={
                "action": "query",
                "prop": "extracts|coordinates",
                "titles": title,
                "redirects": 1,
                "exintro": 1,
                "explaintext": 1,
                "format": "json",
            },
            timeout=20,
        )
    except Exception:
        return {}
    pages = (((data or {}).get("query") or {}).get("pages") or {})
    if not pages:
        return {}
    page = next(iter(pages.values()))
    coords = (page.get("coordinates") or [{}])[0]
    final_title = str(page.get("title", title)).strip()
    return {
        "title": final_title,
        "summary": str(page.get("extract", "")).strip(),
        "latitude": coords.get("lat"),
        "longitude": coords.get("lon"),
        "wikipedia_url": "https://en.wikipedia.org/wiki/"
        + urllib.parse.quote(final_title.replace(" ", "_")),
    }


def _geocode_location(name: str, country_hint: str | None = None) -> dict:
    if not name:
        return {}
    params = {"name": name, "count": 1, "language": "en", "format": "json"}
    if country_hint:
        params["countryCode"] = country_hint
    try:
        data = _request_json(OPEN_METEO_GEOCODING_URL, params=params, timeout=20)
    except Exception:
        data = {}
    results = (data or {}).get("results") or []
    if results:
        result = results[0]
        return {
            "latitude": result.get("latitude"),
            "longitude": result.get("longitude"),
            "city": result.get("name", ""),
            "country": result.get("country", ""),
            "country_code": result.get("country_code", ""),
            "timezone": result.get("timezone", ""),
        }
    try:
        fallback_results = _request_json(
            NOMINATIM_SEARCH_URL,
            params={
                "q": name,
                "format": "jsonv2",
                "limit": 1,
                "addressdetails": 1,
                **({"countrycodes": str(country_hint).lower()} if country_hint else {}),
            },
            timeout=20,
        )
    except Exception:
        fallback_results = []
    if not fallback_results:
        return {}
    result = fallback_results[0]
    address = result.get("address") or {}
    return {
        "latitude": _safe_float(result.get("lat")),
        "longitude": _safe_float(result.get("lon")),
        "city": address.get("city")
        or address.get("town")
        or address.get("village")
        or address.get("municipality")
        or result.get("name", ""),
        "country": address.get("country", ""),
        "country_code": str(address.get("country_code", "")).upper(),
        "timezone": "",
    }


def _geocode_team_profile_candidates(
    team_name: str,
    country_hint: str | None,
    *extra_hints: str,
) -> tuple[dict, str]:
    override = _team_location_override(team_name)
    candidates = []
    for candidate in [override.get("query", ""), *extra_hints, team_name]:
        cleaned = _clean_location_hint(str(candidate or "").strip())
        if cleaned and cleaned not in candidates:
            candidates.append(cleaned)
    for candidate in candidates:
        geocoded = _geocode_location(candidate, country_hint)
        if geocoded.get("latitude") is not None and geocoded.get("longitude") is not None:
            return geocoded, candidate
    return {}, candidates[0] if candidates else ""


def _repair_profile_location(
    team_name: str,
    profile: dict,
    country_hint: str | None = None,
    *extra_hints: str,
) -> dict:
    repaired = _apply_location_override_fields(profile or {}, team_name)
    resolved_country_hint = _guess_country_hint(
        team_name,
        country_hint or repaired.get("country_code") or repaired.get("country_hint"),
    )
    geocoded, best_hint = _geocode_team_profile_candidates(
        team_name,
        resolved_country_hint,
        repaired.get("location_hint", ""),
        repaired.get("city", ""),
        *extra_hints,
    )
    override = _team_location_override(team_name)
    repaired["team"] = team_name
    repaired["country_hint"] = resolved_country_hint
    repaired["cache_version"] = TEAM_PROFILE_CACHE_VERSION
    if best_hint:
        repaired["location_hint"] = override.get("query") or best_hint
    if repaired.get("latitude") is None and geocoded.get("latitude") is not None:
        repaired["latitude"] = geocoded.get("latitude")
    if repaired.get("longitude") is None and geocoded.get("longitude") is not None:
        repaired["longitude"] = geocoded.get("longitude")
    if not str(repaired.get("city", "")).strip() and geocoded.get("city"):
        repaired["city"] = geocoded.get("city", "")
    if not str(repaired.get("country", "")).strip() and geocoded.get("country"):
        repaired["country"] = geocoded.get("country", "")
    if not str(repaired.get("country_code", "")).strip() and geocoded.get("country_code"):
        repaired["country_code"] = geocoded.get("country_code", "")
    if not str(repaired.get("timezone", "")).strip() and geocoded.get("timezone"):
        repaired["timezone"] = geocoded.get("timezone", "")
    return _apply_location_override_fields(repaired, team_name)


def fetch_team_profile(team_name: str, country_hint: str | None = None) -> dict:
    cached = _cache_get(TEAM_PROFILE_CACHE, team_name)
    if (
        cached
        and cached.get("cache_version") == TEAM_PROFILE_CACHE_VERSION
        and cached.get("latitude") is not None
        and cached.get("longitude") is not None
    ):
        return cached

    resolved_country_hint = _guess_country_hint(team_name, country_hint)
    wiki_title = _search_wikipedia_title(team_name, resolved_country_hint)
    wiki_data = _fetch_wikipedia_page_data(wiki_title)
    location_hint = _clean_location_hint(_extract_location_hint(wiki_data.get("summary", "")))
    geocoded, best_hint = _geocode_team_profile_candidates(
        team_name,
        resolved_country_hint,
        location_hint,
        wiki_data.get("title", ""),
    )

    latitude = wiki_data.get("latitude")
    longitude = wiki_data.get("longitude")
    if latitude is None or longitude is None:
        latitude = geocoded.get("latitude")
        longitude = geocoded.get("longitude")

    profile = {
        "team": team_name,
        "country_hint": resolved_country_hint,
        "wikipedia_title": wiki_data.get("title", wiki_title),
        "summary": wiki_data.get("summary", ""),
        "wikipedia_url": wiki_data.get("wikipedia_url", ""),
        "location_hint": best_hint or location_hint,
        "city": geocoded.get("city", ""),
        "country": geocoded.get("country", ""),
        "country_code": geocoded.get("country_code", resolved_country_hint or ""),
        "timezone": geocoded.get("timezone", ""),
        "latitude": latitude,
        "longitude": longitude,
        "cache_version": TEAM_PROFILE_CACHE_VERSION,
    }
    profile = _apply_location_override_fields(profile, team_name)
    _cache_set(TEAM_PROFILE_CACHE, team_name, profile)
    return profile


def _parse_google_news_rss(xml_text: str) -> list:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    items = []
    for item in root.findall("./channel/item"):
        source = item.find("source")
        items.append(
            {
                "title": _strip_google_suffix(item.findtext("title", "")),
                "link": item.findtext("link", ""),
                "published_at": item.findtext("pubDate", ""),
                "source": source.text.strip() if source is not None and source.text else "",
            }
        )
    return items


def _parse_generic_rss(xml_text: str, default_source: str = "") -> list:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    items = []
    for item in root.findall(".//item"):
        source = item.find("source")
        title = (item.findtext("title", "") or "").strip()
        if not title:
            continue
        items.append(
            {
                "title": _strip_google_suffix(title),
                "link": item.findtext("link", ""),
                "published_at": item.findtext("pubDate", "") or item.findtext("published", ""),
                "source": (source.text.strip() if source is not None and source.text else default_source),
            }
        )
    if items:
        return items
    atom_ns = {"atom": "http://www.w3.org/2005/Atom"}
    for entry in root.findall(".//atom:entry", atom_ns):
        title = (entry.findtext("atom:title", "", atom_ns) or "").strip()
        if not title:
            continue
        link_node = entry.find("atom:link", atom_ns)
        source_node = entry.find("atom:source/atom:title", atom_ns)
        items.append(
            {
                "title": _strip_google_suffix(title),
                "link": (link_node.get("href", "") if link_node is not None else ""),
                "published_at": entry.findtext("atom:updated", "", atom_ns)
                or entry.findtext("atom:published", "", atom_ns),
                "source": (source_node.text.strip() if source_node is not None and source_node.text else default_source),
            }
        )
    return items


def _discover_feed_urls(html_text: str, base_url: str) -> list[str]:
    feeds = []
    pattern = re.compile(
        r'<link[^>]+type=["\'](?:application|text)/(?:rss|atom)\+xml["\'][^>]+href=["\']([^"\']+)["\']',
        flags=re.IGNORECASE,
    )
    for href in pattern.findall(html_text):
        resolved = href if href.startswith("http") else urllib.parse.urljoin(base_url, href)
        if resolved not in feeds:
            feeds.append(resolved)
    return feeds[:4]


def _fetch_cached_html(url: str, cache_key: str, ttl_seconds: int = 12 * 3600) -> str:
    cached = _cache_get(EXTERNAL_FEEDS_CACHE, cache_key, ttl_seconds)
    if cached:
        return str(cached)
    html_text = _request_text(url, timeout=20)
    _cache_set(EXTERNAL_FEEDS_CACHE, cache_key, html_text)
    return html_text


def _parse_eduardo_upcoming_jornadas(html_text: str) -> list[dict]:
    jornadas = []
    block_pattern = re.compile(
        r'<div[^>]*c-ayudas-proximas__tabla-partidos__titulo[^>]*>\s*JORNADA\s+(\d+)\s*-\s*([^<]+?)\s*</div>(.*?)(?=<div[^>]*c-ayudas-proximas__tabla-partidos__titulo|</app-ayudas-proximas>|$)',
        flags=re.IGNORECASE | re.DOTALL,
    )
    slot_pattern = re.compile(
        r'<p[^>]*title="([^"]+?)"[^>]*>.*?<span[^>]*c-equipos__number[^>]*>\s*(\d+)\s*</span>.*?<div[^>]*c-marcador-horario__time__hour[^>]*>\s*([^<]*)\s*</div>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    for jornada_match in block_pattern.finditer(html_text):
        jornada_num = _safe_int(jornada_match.group(1))
        jornada_date = html.unescape(jornada_match.group(2)).strip()
        block = jornada_match.group(3)
        slots = []
        for slot_match in slot_pattern.finditer(block):
            title = html.unescape(slot_match.group(1)).strip()
            position = _safe_int(slot_match.group(2))
            hour = html.unescape(slot_match.group(3)).strip()
            if not position or " - " not in title:
                continue
            local_team, away_team = [part.strip() for part in title.split(" - ", 1)]
            kickoff = ""
            if jornada_date and hour:
                try:
                    local_dt = datetime.strptime(
                        f"{jornada_date} {hour}",
                        "%d/%m/%Y %H:%M",
                    ).replace(tzinfo=MADRID_TZ)
                    kickoff = local_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
                except Exception:
                    kickoff = ""
            slots.append(
                {
                    "position": position,
                    "local": _canonical_team_name(local_team),
                    "visitante": _canonical_team_name(away_team),
                    "percentages": {},
                    "kickoff": kickoff,
                    "date_label": jornada_date,
                }
            )
        if slots:
            jornadas.append(
                {
                    "jornada": jornada_num,
                    "date_label": jornada_date,
                    "matches": [slot for slot in slots if slot.get("position", 0) < 15],
                    "pleno15": next((slot for slot in slots if slot.get("position") == 15), {}),
                }
            )
    return jornadas


def fetch_eduardo_upcoming_jornadas() -> list[dict]:
    cache_key = "eduardo:upcoming-jornadas"
    cached = _cache_get(EXTERNAL_FEEDS_CACHE, cache_key, 30 * 60)
    if cached:
        return list(cached)
    try:
        html_text = _fetch_cached_html(
            EDUARDO_QUINIELA_PROXIMAS_URL,
            "eduardo:proximas:html",
            ttl_seconds=30 * 60,
        )
        jornadas = _parse_eduardo_upcoming_jornadas(html_text)
    except Exception:
        jornadas = []
    _cache_set(EXTERNAL_FEEDS_CACHE, cache_key, jornadas)
    return jornadas


def _eduardo_current_context() -> dict:
    cache_key = "eduardo:current-context"
    cached = _cache_get(EXTERNAL_FEEDS_CACHE, cache_key, 15 * 60)
    if cached:
        return cached
    payload = {
        "ok": False,
        "source": "Eduardo Losilla",
        "source_url": EDUARDO_QUINIELA_PORCENTAJES_URL,
        "jornada": None,
        "temporada": None,
    }
    try:
        html_text = _fetch_cached_html(
            EDUARDO_QUINIELA_PORCENTAJES_URL,
            "eduardo:porcentajes:html",
            ttl_seconds=3 * 3600,
        )
        match = re.search(
            r"porcentajes_quinielista\?jornada=(\d+)&amp;temporada=(\d+)",
            html_text,
            flags=re.IGNORECASE,
        )
        if match:
            payload.update(
                {
                    "ok": True,
                    "jornada": int(match.group(1)),
                    "temporada": int(match.group(2)),
                }
            )
    except Exception as exc:
        payload["error"] = str(exc)
    if not payload.get("ok"):
        upcoming_jornadas = fetch_eduardo_upcoming_jornadas()
        if upcoming_jornadas:
            latest = max(
                (jornada for jornada in upcoming_jornadas if _safe_int(jornada.get("jornada"))),
                key=lambda jornada: _safe_int(jornada.get("jornada")),
                default={},
            )
            latest_jornada = _safe_int(latest.get("jornada"))
            if latest_jornada:
                payload["jornada"] = latest_jornada
    if not payload.get("ok"):
        payload["jornada"] = _safe_int((QUINIELA_HISTORY or {}).get("current_jornada"))
        payload["temporada"] = _safe_int((QUINIELA_HISTORY or {}).get("season"))
        payload["ok"] = bool(payload.get("jornada") and payload.get("temporada"))
    _cache_set(EXTERNAL_FEEDS_CACHE, cache_key, payload)
    return payload


def fetch_current_quiniela_jornada_number() -> int | None:
    return _safe_int(_eduardo_current_context().get("jornada"))


def _eduardo_parse_percentages_xml(xml_text: str, source_name: str, source_url: str) -> dict:
    root = ET.fromstring(xml_text)
    percentages = root.find(".//porcentajes")
    if percentages is None:
        raise ValueError("XML de Eduardo sin nodo porcentajes")
    slots = []
    for partido in percentages.findall(".//partido"):
        position = _safe_int(partido.attrib.get("num"))
        if not position:
            continue
        slots.append(
            {
                "position": position,
                "local": _canonical_team_name(html.unescape(partido.attrib.get("local", "")).strip()),
                "visitante": _canonical_team_name(
                    html.unescape(partido.attrib.get("visitante", "")).strip()
                ),
                "percentages": {
                    "1": _safe_float(partido.attrib.get("porc_1")),
                    "X": _safe_float(partido.attrib.get("porc_X")),
                    "2": _safe_float(partido.attrib.get("porc_2")),
                },
            }
        )
    return {
        "ok": bool(slots),
        "source": source_name,
        "url": source_url,
        "jornada": _safe_int(percentages.attrib.get("jornada")),
        "season": _safe_int(percentages.attrib.get("temporada")),
        "active": str(percentages.attrib.get("activo", "")).strip().lower() == "si",
        "matches": [slot for slot in slots if slot.get("position", 0) < 15],
        "pleno15": next((slot for slot in slots if slot.get("position") == 15), {}),
    }


def _fetch_eduardo_percentages_source(jornada: int, temporada: int, source: str) -> dict:
    base_url = EDUARDO_API_QUINIELISTA_URL if source == "quinielista" else EDUARDO_API_LAE_URL
    cache_key = f"eduardo:{source}:{temporada}:{jornada}"
    cached = _cache_get(EXTERNAL_FEEDS_CACHE, cache_key, 6 * 3600)
    if cached:
        return cached
    source_name = (
        "Eduardo Losilla Quinielista" if source == "quinielista" else "Eduardo Losilla LAE"
    )
    params = {"jornada": jornada, "temporada": temporada}
    try:
        xml_text = _request_text(base_url, params=params, timeout=20)
        payload = _eduardo_parse_percentages_xml(
            xml_text,
            source_name=source_name,
            source_url=f"{base_url}?jornada={jornada}&temporada={temporada}",
        )
    except Exception as exc:
        payload = {
            "ok": False,
            "source": source_name,
            "url": f"{base_url}?jornada={jornada}&temporada={temporada}",
            "jornada": jornada,
            "season": temporada,
            "matches": [],
            "pleno15": {},
            "error": str(exc),
        }
    _cache_set(EXTERNAL_FEEDS_CACHE, cache_key, payload)
    return payload


def fetch_quiniela_jornada_page(jornada: int, temporada: int | None = None) -> dict:
    context = _eduardo_current_context()
    season_value = _safe_int(temporada or context.get("temporada"))
    if not season_value:
        return {
            "ok": False,
            "source": "Eduardo Losilla Quinielista",
            "url": EDUARDO_QUINIELA_PORCENTAJES_URL,
            "jornada": jornada,
            "season": None,
            "matches": [],
            "pleno15": {},
            "error": "Temporada no disponible",
        }

    cache_key = f"eduardo:merged:{season_value}:{jornada}"
    cached = _cache_get(EXTERNAL_FEEDS_CACHE, cache_key, 6 * 3600)
    if cached:
        return cached

    quinielista_payload = _fetch_eduardo_percentages_source(jornada, season_value, "quinielista")
    lae_payload = _fetch_eduardo_percentages_source(jornada, season_value, "lae")
    base_payload = quinielista_payload if quinielista_payload.get("ok") else lae_payload
    if not base_payload.get("ok"):
        payload = {
            "ok": False,
            "source": "Eduardo Losilla Quinielista",
            "url": EDUARDO_QUINIELA_PORCENTAJES_URL,
            "jornada": jornada,
            "season": season_value,
            "matches": [],
            "pleno15": {},
            "error": quinielista_payload.get("error") or lae_payload.get("error") or "Sin datos",
        }
        _cache_set(EXTERNAL_FEEDS_CACHE, cache_key, payload)
        return payload

    slots_by_position: dict[int, dict] = {}
    for source_name, payload in (("quinielista", quinielista_payload), ("lae", lae_payload)):
        for slot in list(payload.get("matches", [])) + ([payload.get("pleno15")] if payload.get("pleno15") else []):
            position = _safe_int(slot.get("position"))
            if not position:
                continue
            current = slots_by_position.setdefault(
                position,
                {
                    "position": position,
                    "pleno15": position == 15,
                    "local": slot.get("local", ""),
                    "visitante": slot.get("visitante", ""),
                    "percentages": {},
                },
            )
            if not current.get("local"):
                current["local"] = slot.get("local", "")
            if not current.get("visitante"):
                current["visitante"] = slot.get("visitante", "")
            current.setdefault("percentages", {})[source_name] = slot.get("percentages", {})

    ordered_slots = [slots_by_position[position] for position in sorted(slots_by_position)]
    payload = {
        "ok": bool(ordered_slots),
        "source": "Eduardo Losilla Quinielista",
        "url": EDUARDO_QUINIELA_PORCENTAJES_URL,
        "jornada": jornada,
        "season": season_value,
        "active": bool(quinielista_payload.get("active")),
        "matches": [slot for slot in ordered_slots if slot.get("position", 0) < 15],
        "pleno15": next((slot for slot in ordered_slots if slot.get("position") == 15), {}),
    }
    _cache_set(EXTERNAL_FEEDS_CACHE, cache_key, payload)
    return payload


def fetch_external_feed(url: str, source_name: str, limit: int = 8) -> dict:
    cache_key = f"external:{url}"
    cached = _cache_get(EXTERNAL_FEEDS_CACHE, cache_key, NEWS_CACHE_TTL_SECONDS)
    if cached:
        return cached
    try:
        xml_text = _request_text(url, timeout=20)
        items = _clean_news_items(
            _parse_generic_rss(xml_text, default_source=source_name),
            COMPETITION_NEWS_MAX_AGE_DAYS,
            limit,
        )
        payload = {"ok": True, "items": items, "source_name": source_name, "url": url}
    except Exception as exc:
        payload = {
            "ok": False,
            "items": [],
            "source_name": source_name,
            "url": url,
            "error": str(exc),
        }
    _cache_set(EXTERNAL_FEEDS_CACHE, cache_key, payload)
    return payload


def fetch_google_news_search(query: str, limit: int = 8, max_age_days: int = 14) -> dict:
    cache_key = f"google:{query}"
    cached = _cache_get(EXTERNAL_FEEDS_CACHE, cache_key, NEWS_CACHE_TTL_SECONDS)
    if cached:
        return cached
    try:
        xml_text = _request_text(
            GOOGLE_NEWS_RSS_URL,
            params={
                "q": query,
                "hl": NEWS_LANGUAGE,
                "gl": NEWS_COUNTRY,
                "ceid": f"{NEWS_COUNTRY}:{NEWS_LANGUAGE}",
            },
            timeout=20,
        )
        items = _clean_news_items(_parse_google_news_rss(xml_text), max_age_days, limit)
        payload = {
            "ok": True,
            "items": items,
            "source_name": "Google News",
            "url": GOOGLE_NEWS_RSS_URL,
            "query": query,
        }
    except Exception as exc:
        payload = {
            "ok": False,
            "items": [],
            "source_name": "Google News",
            "url": GOOGLE_NEWS_RSS_URL,
            "query": query,
            "error": str(exc),
        }
    _cache_set(EXTERNAL_FEEDS_CACHE, cache_key, payload)
    return payload


def fetch_competition_headlines(
    league_key: str, league_teams: list[str] | None = None, limit: int = 8
) -> dict:
    feeds = LEAGUE_EXTERNAL_FEEDS.get(league_key, [])
    all_items = []
    source_health = []
    for feed in feeds:
        if not feed.get("url"):
            continue
        payload = fetch_external_feed(feed["url"], feed["name"], limit=limit)
        source_health.append(
            {
                "name": payload.get("source_name", ""),
                "url": payload.get("url", ""),
                "ok": payload.get("ok", False),
                "items": len(payload.get("items", [])),
                "error": payload.get("error", ""),
            }
        )
        all_items.extend(payload.get("items", []))
    google_query = LEAGUE_NEWS_SEARCH_TERMS.get(league_key, "")
    if google_query:
        google_payload = fetch_google_news_search(
            google_query,
            limit=limit,
            max_age_days=COMPETITION_NEWS_MAX_AGE_DAYS,
        )
        source_health.append(
            {
                "name": google_payload.get("source_name", ""),
                "url": google_payload.get("url", ""),
                "ok": google_payload.get("ok", False),
                "items": len(google_payload.get("items", [])),
                "error": google_payload.get("error", ""),
            }
        )
        for item in google_payload.get("items", []):
            enriched = dict(item)
            enriched["_relevance"] = _competition_relevance_score(enriched, league_key, league_teams)
            if enriched["_relevance"] <= 0:
                continue
            all_items.append(enriched)
    filtered_items = []
    for item in all_items:
        enriched = dict(item)
        if _is_low_signal_source(enriched.get("source", "")):
            continue
        enriched["_relevance"] = max(
            float(enriched.get("_relevance", 0.0) or 0.0),
            _competition_relevance_score(enriched, league_key, league_teams),
        )
        if enriched["_relevance"] <= 0:
            continue
        filtered_items.append(enriched)
    deduped = []
    seen = set()
    for item in filtered_items:
        key = (item.get("title", ""), item.get("link", ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return {
        "league": league_key,
        "items": _clean_news_items(deduped, COMPETITION_NEWS_MAX_AGE_DAYS, limit),
        "source_health": source_health,
    }


def _summarize_news_signals(items: list) -> dict:
    signals = {
        "injury_count": 0,
        "rotation_count": 0,
        "discipline_count": 0,
        "europe_count": 0,
        "weather_count": 0,
        "press_count": 0,
        "squad_count": 0,
        "morale_count": 0,
    }
    for item in items:
        haystack = f"{item.get('title', '')} {item.get('source', '')}".lower()
        if any(keyword in haystack for keyword in INJURY_KEYWORDS):
            signals["injury_count"] += 1
        if any(keyword in haystack for keyword in ROTATION_KEYWORDS):
            signals["rotation_count"] += 1
        if any(keyword in haystack for keyword in DISCIPLINE_KEYWORDS):
            signals["discipline_count"] += 1
        if any(keyword in haystack for keyword in EUROPE_KEYWORDS):
            signals["europe_count"] += 1
        if any(keyword in haystack for keyword in WEATHER_KEYWORDS):
            signals["weather_count"] += 1
        if any(keyword in haystack for keyword in PRESS_CONFERENCE_KEYWORDS):
            signals["press_count"] += 1
        if any(keyword in haystack for keyword in SQUAD_KEYWORDS):
            signals["squad_count"] += 1
        if any(keyword in haystack for keyword in MORALE_KEYWORDS):
            signals["morale_count"] += 1
    return signals


def _fetch_google_news_items(query: str, limit: int, max_age_days: int) -> list[dict]:
    xml_text = _request_text(
        GOOGLE_NEWS_RSS_URL,
        params={
            "q": query,
            "hl": NEWS_LANGUAGE,
            "gl": NEWS_COUNTRY,
            "ceid": f"{NEWS_COUNTRY}:{NEWS_LANGUAGE}",
        },
        timeout=20,
    )
    return _clean_news_items(_parse_google_news_rss(xml_text), max_age_days, limit)


def _query_news_with_relevance(
    query: str,
    relevance_fn,
    limit: int,
    max_age_days: int,
) -> list[dict]:
    items = []
    for item in _fetch_google_news_items(query, max(limit * 2, limit), max_age_days):
        if _is_low_signal_source(item.get("source", "")):
            continue
        enriched = dict(item)
        enriched["_relevance"] = relevance_fn(enriched.get("title", ""))
        if enriched["_relevance"] <= 0:
            continue
        items.append(enriched)
    return items


def fetch_team_news(team_name: str) -> dict:
    cache_key = f"v8:team:{team_name}"
    cached = _cache_get(TEAM_NEWS_CACHE, cache_key, NEWS_CACHE_TTL_SECONDS)
    if cached:
        return cached
    try:
        team_query = _team_query_terms(team_name)
        items = _query_news_with_relevance(
            f'{team_query} football OR futbol OR soccer OR lesion OR injury OR arbitro OR referee OR rotation OR convocatoria OR rueda de prensa OR sancion OR descenso',
            lambda title: _team_relevance_score(title, team_name),
            TEAM_NEWS_ITEMS * 2,
            TEAM_NEWS_MAX_AGE_DAYS,
        )
        filtered = [item for item in _predictive_news_items(items) if _passes_team_news_quality(item, team_name, require_signal=False)]
        items = _clean_news_items(filtered, TEAM_NEWS_MAX_AGE_DAYS, TEAM_NEWS_ITEMS)
    except Exception:
        items = []
    payload = {"items": items, "signals": _summarize_news_signals(items)}
    _cache_set(TEAM_NEWS_CACHE, cache_key, payload)
    return payload


def fetch_focus_team_news(team_name: str) -> dict:
    cache_key = f"v9:focus:{team_name}"
    cached = _cache_get(TEAM_NEWS_CACHE, cache_key, NEWS_CACHE_TTL_SECONDS)
    if cached:
        return cached
    team_query = _team_query_terms(team_name)
    queries = [
        f'{team_query} lesion OR injury OR baja OR suspension OR sancion OR doubt OR convocatoria',
        f'{team_query} entrenador OR coach OR rueda de prensa OR alineacion OR rotation OR descanso',
        f'{team_query} descenso OR permanencia OR playoff OR ascenso OR title race OR crisis OR moral OR presion',
        f'{team_query} Champions OR Europa League OR Conference League OR semifinal OR rotaciones OR descanso',
        f'{team_query} convocatoria OR once probable OR probable lineup OR parte medico OR medical update',
    ]
    items = []
    try:
        for query in queries:
            items.extend(
                _query_news_with_relevance(
                    query,
                    lambda title, current_team=team_name: _team_relevance_score(title, current_team),
                    FOCUS_TEAM_NEWS_ITEMS * 2,
                    TEAM_NEWS_MAX_AGE_DAYS,
                )
            )
        filtered = [item for item in _predictive_news_items(items) if _passes_team_news_quality(item, team_name, require_signal=True)]
        items = _clean_news_items(filtered, TEAM_NEWS_MAX_AGE_DAYS, FOCUS_TEAM_NEWS_ITEMS)
    except Exception:
        items = []
    payload = {"items": items, "signals": _summarize_news_signals(items), "query_count": len(queries)}
    _cache_set(TEAM_NEWS_CACHE, cache_key, payload)
    return payload


def fetch_local_media_news(team_name: str) -> dict:
    cache_key = f"v8:media:{team_name}"
    cached = _cache_get(TEAM_NEWS_CACHE, cache_key, NEWS_CACHE_TTL_SECONDS)
    if cached:
        return cached
    team_query = _team_query_terms(team_name)
    team_hint_key = _normalize_team_name(team_name)
    local_media_hints = TEAM_LOCAL_MEDIA_HINTS.get(team_hint_key) or TEAM_LOCAL_MEDIA_HINTS.get(
        TEAM_NAME_ALIASES.get(team_hint_key, "").lower(),
        [],
    )
    source_hint_clause = " OR ".join(f'"{hint}"' for hint in local_media_hints) if local_media_hints else (
        'marca OR as OR relevo OR eldesmarque OR mundodeportivo OR sport OR superdeporte '
        'OR estadiodeportivo OR cope OR "cadena ser" OR bbc OR guardian'
    )
    queries = [
        (
            f'{team_query} '
            f"{source_hint_clause} "
            "football OR futbol OR soccer OR laliga OR segunda OR championship"
        ),
        (
            f'{team_query} '
            "lesion OR injury OR baja OR suspension OR sancion OR rueda de prensa OR convocatoria "
            "OR alineacion OR crisis OR vestuario OR moral OR football OR futbol OR soccer"
        ),
    ]
    items = []
    try:
        for query in queries:
            items.extend(
                _query_news_with_relevance(
                    query,
                    lambda title, current_team=team_name: _team_relevance_score(title, current_team),
                    LOCAL_MEDIA_NEWS_ITEMS * 2,
                    TEAM_NEWS_MAX_AGE_DAYS,
                )
            )
        filtered = [
            item
            for item in _local_media_items(items)
            if _passes_team_news_quality(item, team_name, require_signal=True)
        ]
        items = _clean_news_items(filtered, TEAM_NEWS_MAX_AGE_DAYS, LOCAL_MEDIA_NEWS_ITEMS)
    except Exception:
        items = []
    payload = {"items": items, "signals": _summarize_news_signals(items), "query_count": len(queries)}
    _cache_set(TEAM_NEWS_CACHE, cache_key, payload)
    return payload


def fetch_match_news(home_team: str, away_team: str) -> dict:
    cache_key = f"v8:{home_team}__{away_team}"
    cached = _cache_get(MATCH_NEWS_CACHE, cache_key, MATCH_NEWS_CACHE_TTL_SECONDS)
    if cached:
        return cached
    try:
        xml_text = _request_text(
            GOOGLE_NEWS_RSS_URL,
            params={
                "q": (
                    f'{_team_query_terms(home_team)} {_team_query_terms(away_team)} '
                    "referee OR arbitro OR injury OR lesion OR rotation OR descanso OR weather "
                    "OR convocatoria OR rueda de prensa OR sancion OR crisis OR moral"
                ),
                "hl": NEWS_LANGUAGE,
                "gl": NEWS_COUNTRY,
                "ceid": f"{NEWS_COUNTRY}:{NEWS_LANGUAGE}",
            },
            timeout=20,
        )
        items = []
        for item in _parse_google_news_rss(xml_text):
            if _is_low_signal_source(item.get("source", "")):
                continue
            enriched = dict(item)
            enriched["_relevance"] = _match_relevance_score(
                enriched.get("title", ""),
                home_team,
                away_team,
            )
            if enriched["_relevance"] <= 0:
                continue
            items.append(enriched)
        filtered = [
            item
            for item in _predictive_news_items(items)
            if _passes_match_news_quality(item, home_team, away_team)
        ]
        items = _clean_news_items(filtered, MATCH_NEWS_MAX_AGE_DAYS, MATCH_NEWS_ITEMS)
    except Exception:
        items = []
    signals = {
        "referee_count": 0,
        "injury_count": 0,
        "rotation_count": 0,
        "weather_count": 0,
    }
    for item in items:
        haystack = f"{item.get('title', '')} {item.get('source', '')}".lower()
        if any(keyword in haystack for keyword in DISCIPLINE_KEYWORDS):
            signals["referee_count"] += 1
        if any(keyword in haystack for keyword in INJURY_KEYWORDS):
            signals["injury_count"] += 1
        if any(keyword in haystack for keyword in ROTATION_KEYWORDS):
            signals["rotation_count"] += 1
        if any(keyword in haystack for keyword in WEATHER_KEYWORDS):
            signals["weather_count"] += 1
    payload = {"items": items, "signals": signals}
    _cache_set(MATCH_NEWS_CACHE, cache_key, payload)
    return payload


def fetch_match_referee_news(home_team: str, away_team: str) -> list[dict]:
    cache_key = f"v8:referee:{home_team}__{away_team}"
    cached = _cache_get(MATCH_NEWS_CACHE, cache_key, MATCH_NEWS_CACHE_TTL_SECONDS)
    if cached:
        return cached
    try:
        xml_text = _request_text(
            GOOGLE_NEWS_RSS_URL,
            params={
                "q": (
                    f'{_team_query_terms(home_team)} {_team_query_terms(away_team)} '
                    '"referee" OR "arbitro" OR "árbitro" OR "colegiado"'
                ),
                "hl": NEWS_LANGUAGE,
                "gl": NEWS_COUNTRY,
                "ceid": f"{NEWS_COUNTRY}:{NEWS_LANGUAGE}",
            },
            timeout=20,
        )
        items = []
        for item in _parse_google_news_rss(xml_text):
            if _is_low_signal_source(item.get("source", "")):
                continue
            enriched = dict(item)
            enriched["_relevance"] = _match_relevance_score(
                enriched.get("title", ""),
                home_team,
                away_team,
            )
            if enriched["_relevance"] <= 0:
                continue
            items.append(enriched)
        filtered = [
            item
            for item in _predictive_news_items(items)
            if _passes_match_news_quality(item, home_team, away_team)
            and _contains_any(f"{item.get('title', '')} {item.get('source', '')}", DISCIPLINE_KEYWORDS)
        ]
        items = _clean_news_items(filtered, MATCH_NEWS_MAX_AGE_DAYS, 4)
    except Exception:
        items = []
    _cache_set(MATCH_NEWS_CACHE, cache_key, items)
    return items


def fetch_match_referee_news(home_team: str, away_team: str) -> list[dict]:
    cache_key = f"v9:referee:{home_team}__{away_team}"
    cached = _cache_get(MATCH_NEWS_CACHE, cache_key, MATCH_NEWS_CACHE_TTL_SECONDS)
    if cached:
        return cached
    try:
        items = []
        queries = [
            (
                f'{_team_query_terms(home_team)} {_team_query_terms(away_team)} '
                '"referee" OR "arbitro" OR "árbitro" OR "colegiado"'
            ),
            f'"{home_team}" "{away_team}" arbitro OR árbitro OR colegiado OR designacion arbitral',
            f'"{home_team}" "{away_team}" arbitros OR árbitros OR designaciones',
        ]
        for query in queries:
            xml_text = _request_text(
                GOOGLE_NEWS_RSS_URL,
                params={
                    "q": query,
                    "hl": NEWS_LANGUAGE,
                    "gl": NEWS_COUNTRY,
                    "ceid": f"{NEWS_COUNTRY}:{NEWS_LANGUAGE}",
                },
                timeout=20,
            )
            for item in _parse_google_news_rss(xml_text):
                if _is_low_signal_source(item.get("source", "")):
                    continue
                enriched = dict(item)
                enriched["_relevance"] = _match_relevance_score(
                    enriched.get("title", ""),
                    home_team,
                    away_team,
                )
                if enriched["_relevance"] <= 0 and not _contains_any(
                    enriched.get("title", ""),
                    ["referee", "arbitro", "árbitro", "colegiado"],
                ):
                    continue
                items.append(enriched)
        filtered = [
            item
            for item in _predictive_news_items(items)
            if _contains_any(f"{item.get('title', '')} {item.get('source', '')}", DISCIPLINE_KEYWORDS)
            and (
                _passes_match_news_quality(item, home_team, away_team)
                or _contains_any(item.get("title", ""), ["referee", "arbitro", "árbitro", "colegiado"])
            )
        ]
        items = _clean_news_items(filtered, MATCH_NEWS_MAX_AGE_DAYS, 6)
    except Exception:
        items = []
    _cache_set(MATCH_NEWS_CACHE, cache_key, items)
    return items


def fetch_match_referee_news(home_team: str, away_team: str) -> list[dict]:
    cache_key = f"v10:referee:{home_team}__{away_team}"
    cached = _cache_get(MATCH_NEWS_CACHE, cache_key, MATCH_NEWS_CACHE_TTL_SECONDS)
    if cached:
        return cached
    try:
        items = []
        queries = [
            f'"{home_team}" "{away_team}" arbitro OR árbitro OR colegiado OR designacion arbitral OR designaciones arbitrales',
            f'{_team_query_terms(home_team)} {_team_query_terms(away_team)} "referee" OR "arbitro" OR "colegiado"',
            (
                f'"{home_team}" "{away_team}" '
                '(site:vavel.com OR site:as.com OR site:marca.com OR site:mundodeportivo.com OR site:eldesmarque.com) '
                'arbitro OR árbitro OR colegiado'
            ),
        ]
        for query in queries:
            xml_text = _request_text(
                GOOGLE_NEWS_RSS_URL,
                params={
                    "q": query,
                    "hl": NEWS_LANGUAGE,
                    "gl": NEWS_COUNTRY,
                    "ceid": f"{NEWS_COUNTRY}:{NEWS_LANGUAGE}",
                },
                timeout=20,
            )
            for item in _parse_google_news_rss(xml_text):
                if _is_low_signal_source(item.get("source", "")):
                    continue
                enriched = dict(item)
                enriched["_relevance"] = _match_relevance_score(
                    enriched.get("title", ""),
                    home_team,
                    away_team,
                )
                if enriched["_relevance"] <= 0 and not _contains_any(
                    enriched.get("title", ""),
                    ["referee", "arbitro", "árbitro", "colegiado", "designacion"],
                ):
                    continue
                items.append(enriched)
        filtered = [
            item
            for item in _predictive_news_items(items)
            if _contains_any(
                f"{item.get('title', '')} {item.get('source', '')}",
                DISCIPLINE_KEYWORDS + ["designacion", "designación"],
            )
            and (
                _passes_match_news_quality(item, home_team, away_team)
                or _contains_any(
                    item.get("title", ""),
                    ["referee", "arbitro", "árbitro", "colegiado", "designacion", "designación"],
                )
            )
        ]
        items = _clean_news_items(filtered, MATCH_NEWS_MAX_AGE_DAYS, 8)
    except Exception:
        items = []
    _cache_set(MATCH_NEWS_CACHE, cache_key, items)
    return items


def fetch_the_sportsdb_team(team_name: str) -> dict:
    cached = _cache_get(THESPORTSDB_CACHE, f"team:{team_name}", 7 * 24 * 3600)
    if cached:
        return cached
    try:
        data = _request_json(
            THESPORTSDB_SEARCH_TEAM_URL,
            params={"t": team_name},
            timeout=20,
        )
    except Exception:
        data = {}
    teams = (data or {}).get("teams") or []
    payload = {}
    if teams:
        country_hint = _guess_country_hint(team_name)
        best_score = -1.0
        for candidate in teams:
            if str(candidate.get("strSport", "")).strip().lower() != "soccer":
                continue
            score = max(
                _team_similarity_score(team_name, str(candidate.get("strTeam", "")).strip()),
                _team_similarity_score(team_name, str(candidate.get("strTeamAlternate", "")).strip()),
            )
            if country_hint and str(candidate.get("strCountry", "")).strip():
                if str(candidate.get("strCountry", "")).strip().lower() == COUNTRY_LABELS.get(country_hint, "").lower():
                    score += 0.08
            if str(candidate.get("idESPN", "")).strip():
                score += 0.04
            if score > best_score:
                best_score = score
                payload = candidate
    _cache_set(THESPORTSDB_CACHE, f"team:{team_name}", payload)
    return payload


def fetch_official_site_headlines(team_name: str, team_api: dict, limit: int = 4) -> dict:
    website = str(team_api.get("strWebsite", "")).strip()
    if not website:
        return {"website": "", "items": []}
    if not website.startswith("http"):
        website = "https://" + website.lstrip("/")
    cache_key = f"official:v3:{website}"
    cached = _cache_get(OFFICIAL_SITE_CACHE, cache_key, 12 * 3600)
    if cached:
        return cached
    try:
        html_text = _request_text(website, timeout=20)
        items = []
        for feed_url in _discover_feed_urls(html_text, website):
            try:
                feed_text = _request_text(feed_url, timeout=20)
                items.extend(_parse_generic_rss(feed_text, default_source="Web oficial"))
            except Exception:
                continue
        matches = []
        if not items:
            matches = re.findall(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html_text, flags=re.IGNORECASE | re.DOTALL)
        for href, body in matches:
            text = html.unescape(re.sub(r"<[^>]+>", " ", body))
            text = re.sub(r"\s+", " ", text).strip(" -\t\r\n")
            if len(text) < 25 or len(text) > 140:
                continue
            if _team_relevance_score(text, team_name) <= 0 and not any(
                token in text.lower() for token in ["coach", "match", "previa", "crónica", "convocatoria", "lesión", "injury"]
            ):
                continue
            link = href if href.startswith("http") else urllib.parse.urljoin(website, href)
            items.append({"title": text, "link": link, "source": "Web oficial"})
        filtered = [
            item
            for item in _official_predictive_items(items)
            if _passes_team_news_quality(item, team_name, require_signal=True)
        ]
        items = _clean_news_items(filtered, TEAM_NEWS_MAX_AGE_DAYS, limit)
        deduped = []
        seen = set()
        for item in items:
            key = (item["title"], item["link"])
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        payload = {"website": website, "items": deduped[:limit]}
    except Exception:
        payload = {"website": website, "items": []}
    _cache_set(OFFICIAL_SITE_CACHE, cache_key, payload)
    return payload


def fetch_the_sportsdb_next_event(team_id: str) -> dict:
    if not team_id:
        return {}
    cached = _cache_get(THESPORTSDB_CACHE, f"next_event:{team_id}", 6 * 3600)
    if cached:
        return cached
    try:
        data = _request_json(
            THESPORTSDB_EVENTS_NEXT_URL,
            params={"id": team_id},
            timeout=20,
        )
    except Exception:
        data = {}
    events = (data or {}).get("events") or []
    payload = events[0] if events else {}
    _cache_set(THESPORTSDB_CACHE, f"next_event:{team_id}", payload)
    return payload


def fetch_the_sportsdb_next_events(team_id: str) -> list[dict]:
    if not team_id:
        return []
    cached = _cache_get(THESPORTSDB_CACHE, f"next_events:v2:{team_id}", 3 * 3600)
    if cached:
        return list(cached)
    try:
        data = _request_json(
            THESPORTSDB_EVENTS_NEXT_URL,
            params={"id": team_id},
            timeout=20,
        )
    except Exception:
        data = {}
    events = (data or {}).get("events") or []
    payload = events if isinstance(events, list) else []
    _cache_set(THESPORTSDB_CACHE, f"next_events:v2:{team_id}", payload)
    return payload


def fetch_the_sportsdb_round_events(league_id: str, season: str, round_value: object) -> list[dict]:
    if not league_id or not season or round_value in {None, ""}:
        return []
    round_text = str(round_value).strip()
    cache_key = f"round_events:{league_id}:{season}:{round_text}"
    cached = _cache_get(THESPORTSDB_CACHE, cache_key, 12 * 3600)
    if cached:
        return list(cached)
    try:
        data = _request_json(
            THESPORTSDB_EVENTS_ROUND_URL,
            params={"id": league_id, "r": round_text, "s": season},
            timeout=25,
        )
    except Exception:
        data = {}
    events = (data or {}).get("events") or []
    _cache_set(THESPORTSDB_CACHE, cache_key, events)
    return events


def _infer_league_key_from_sportsdb(*payloads: dict) -> str:
    league_id_map = {
        "4335": "soccer_spain_la_liga",
        "4396": "soccer_spain_segunda_division",
        "4328": "soccer_epl",
        "4329": "soccer_efl_champ",
    }
    league_name_aliases = {
        "spanish laliga": "soccer_spain_la_liga",
        "laliga": "soccer_spain_la_liga",
        "la liga": "soccer_spain_la_liga",
        "spanish segunda division": "soccer_spain_segunda_division",
        "segunda division": "soccer_spain_segunda_division",
        "laliga2": "soccer_spain_segunda_division",
        "english premier league": "soccer_epl",
        "premier league": "soccer_epl",
        "english league championship": "soccer_efl_champ",
        "efl championship": "soccer_efl_champ",
        "championship": "soccer_efl_champ",
    }
    for payload in payloads:
        league_id = str((payload or {}).get("idLeague", "")).strip()
        if league_id and league_id in league_id_map:
            return league_id_map[league_id]
        league_name = _normalize_team_name(str((payload or {}).get("strLeague", "")).strip())
        if league_name in league_name_aliases:
            return league_name_aliases[league_name]
    return ""


def _sportsdb_event_kickoff(event: dict) -> str:
    timestamp = str(event.get("strTimestamp", "")).strip()
    if timestamp:
        return timestamp if timestamp.endswith("Z") else f"{timestamp}Z"
    date_value = str(event.get("dateEvent", "")).strip()
    time_value = str(event.get("strTime", "")).strip() or "00:00:00"
    if date_value:
        return f"{date_value}T{time_value}Z"
    return ""


def _sportsdb_event_match_score(event: dict, home_team: str, away_team: str, kickoff: str) -> float:
    event_home = str(event.get("strHomeTeam", "")).strip()
    event_away = str(event.get("strAwayTeam", "")).strip()
    if not event_home or not event_away:
        return 0.0
    home_score = _team_similarity_score(home_team, event_home)
    away_score = _team_similarity_score(away_team, event_away)
    if home_score < 0.9 or away_score < 0.9:
        return 0.0
    score = home_score + away_score
    kickoff_dt = _parse_iso_datetime(kickoff)
    event_dt = _parse_iso_datetime(_sportsdb_event_kickoff(event))
    if kickoff_dt and event_dt:
        delta_hours = abs((event_dt - kickoff_dt).total_seconds()) / 3600.0
        if delta_hours <= 3:
            score += 1.1
        elif delta_hours <= 30:
            score += 0.7
        elif delta_hours <= 72:
            score += 0.3
    return score


def _resolve_sportsdb_event(
    home_team: str,
    away_team: str,
    kickoff: str,
    home_team_api: dict,
    away_team_api: dict,
    inferred_round: int | None = None,
) -> dict:
    candidates = []
    home_next = fetch_the_sportsdb_next_event(str(home_team_api.get("idTeam", "")).strip())
    away_next = fetch_the_sportsdb_next_event(str(away_team_api.get("idTeam", "")).strip())
    for event in [home_next, away_next]:
        if event:
            candidates.append(dict(event))
    league_id = str(home_team_api.get("idLeague", "") or away_team_api.get("idLeague", "")).strip()
    season = (
        str(home_next.get("strSeason", "")).strip()
        or str(away_next.get("strSeason", "")).strip()
        or _season_tag_for(_parse_iso_datetime(kickoff))
    )
    round_candidates = []
    if inferred_round:
        round_candidates.extend([inferred_round - 1, inferred_round, inferred_round + 1, inferred_round + 2])
    for raw_round in round_candidates:
        if not league_id or not season or raw_round is None or raw_round <= 0:
            continue
        for event in fetch_the_sportsdb_round_events(league_id, season, raw_round):
            candidates.append(dict(event))
    best_event = {}
    best_score = 0.0
    seen = set()
    for event in candidates:
        event_id = str(event.get("idEvent", "")).strip()
        if event_id and event_id in seen:
            continue
        if event_id:
            seen.add(event_id)
        score = _sportsdb_event_match_score(event, home_team, away_team, kickoff)
        if score > best_score:
            best_score = score
            best_event = event
    if not best_event:
        best_event = dict(away_next or home_next or {})
    if best_event:
        best_event.setdefault("strHomeTeam", home_team)
        best_event.setdefault("strAwayTeam", away_team)
        best_event.setdefault("idLeague", league_id)
        best_event.setdefault("strLeague", home_team_api.get("strLeague", "") or away_team_api.get("strLeague", ""))
        best_event.setdefault("strSeason", season)
        if inferred_round and not str(best_event.get("intRound", "")).strip():
            best_event["intRound"] = str(inferred_round)
    return best_event


def fetch_espn_team_fixtures(
    team_name: str,
    espn_id: str,
    kickoff_dt: datetime | None,
    table_snapshot: dict,
    history_rows: list[dict],
    next_n: int = UPCOMING_FIXTURE_WINDOW,
) -> list[dict]:
    if not espn_id:
        return []
    cache_key = f"espn:fixtures:{espn_id}"
    cached = _cache_get(EXTERNAL_FEEDS_CACHE, cache_key, 6 * 3600)
    if cached:
        fixture_rows = list(cached)
    else:
        slug = _slugify_team_name(team_name)
        url = f"https://www.espn.com/soccer/team/fixtures/_/id/{espn_id}/{slug}"
        try:
            html_text = _request_text(url, timeout=25)
        except Exception:
            html_text = ""
        row_pattern = re.compile(
            r'<tr[^>]*Table__TR[^>]*>.*?<div[^>]*data-testid="date"[^>]*>([^<]+)</div>.*?'
            r'<div[^>]*data-testid="localTeam"[^>]*>.*?data-testid="formattedTeam"[^>]*>([^<]+)</a>.*?'
            r'<div[^>]*data-testid="awayTeam"[^>]*>.*?data-testid="formattedTeam"[^>]*>([^<]+)</a>.*?'
            r'<td class="Table__TD"><a[^>]*>([^<]+)</a>.*?<td class="Table__TD"><span>([^<]+)</span>',
            flags=re.IGNORECASE | re.DOTALL,
        )
        fixture_rows = []
        for match in row_pattern.finditer(html_text):
            date_text, home_team, away_team, time_text, competition = [
                html.unescape(part).strip() for part in match.groups()
            ]
            fixture_rows.append(
                {
                    "date_text": date_text,
                    "home_team": home_team,
                    "away_team": away_team,
                    "time_text": time_text,
                    "competition": competition,
                }
            )
        _cache_set(EXTERNAL_FEEDS_CACHE, cache_key, fixture_rows)
    if not kickoff_dt:
        return []
    fixtures = []
    current_year = kickoff_dt.astimezone(MADRID_TZ).year
    current_month = kickoff_dt.astimezone(MADRID_TZ).month
    for row in fixture_rows:
        home_team = str(row.get("home_team", "")).strip()
        away_team = str(row.get("away_team", "")).strip()
        if not home_team or not away_team:
            continue
        home_score = _team_similarity_score(team_name, home_team)
        away_score = _team_similarity_score(team_name, away_team)
        if max(home_score, away_score) < 0.9:
            continue
        date_text = str(row.get("date_text", "")).strip()
        time_text = str(row.get("time_text", "")).strip()
        try:
            partial_dt = datetime.strptime(
                f"{date_text}, {current_year} {time_text}",
                "%a, %b %d, %Y %I:%M %p",
            )
        except Exception:
            try:
                partial_dt = datetime.strptime(
                    f"{date_text}, {current_year}",
                    "%a, %b %d, %Y",
                )
            except Exception:
                partial_dt = None
        if partial_dt is None:
            continue
        if partial_dt.month < current_month - 3:
            partial_dt = partial_dt.replace(year=current_year + 1)
        local_dt = partial_dt.replace(tzinfo=MADRID_TZ)
        event_dt = local_dt.astimezone(timezone.utc)
        if event_dt <= kickoff_dt:
            continue
        is_home = home_score >= away_score
        opponent = away_team if is_home else home_team
        resolved_opponent = _resolve_csv_team_name(opponent, history_rows) if history_rows else opponent
        fixtures.append(
            {
                "date": event_dt.strftime("%Y-%m-%d"),
                "kickoff": event_dt.isoformat().replace("+00:00", "Z"),
                "venue": "home" if is_home else "away",
                "opponent": opponent,
                "opponent_position": (table_snapshot.get(resolved_opponent) or {}).get("position"),
                "opponent_points": (table_snapshot.get(resolved_opponent) or {}).get("points"),
                "league": str(row.get("competition", "")).strip(),
                "source": "espn-fixtures",
            }
        )
    fixtures.sort(key=lambda item: item.get("kickoff", ""))
    return fixtures[:next_n]


def fetch_rfef_designation_text(league_key: str, round_value: object, kickoff: str) -> str:
    prefix = LEAGUE_RFEF_PDF_PREFIX.get(league_key)
    kickoff_dt = _parse_iso_datetime(kickoff)
    if not prefix or not kickoff_dt or not round_value:
        return ""
    round_text = str(round_value).strip()
    if not round_text.isdigit():
        return ""
    pdf_url = (
        "https://rfef.es/sites/default/files/"
        f"designaciones_{prefix}_-_temp_{_season_tag_for(kickoff_dt)}_-_jornada_{round_text}_{_weekday_token_es(kickoff_dt)}.pdf"
    )
    cache_key = f"rfef:{pdf_url}"
    cached = _cache_get(RFEF_CACHE, cache_key, 12 * 3600)
    if cached:
        return str(cached)
    try:
        response = requests.get(pdf_url, headers=DEFAULT_HEADERS, timeout=25)
        response.raise_for_status()
        text = ""
        if PdfReader is not None:
            try:
                reader = PdfReader(io.BytesIO(response.content))
                text = "\n".join(page.extract_text() or "" for page in reader.pages)
            except Exception:
                text = ""
        if not text:
            text = response.content.decode("latin-1", errors="ignore")
        _cache_set(RFEF_CACHE, cache_key, text)
        return text
    except Exception:
        _cache_set(RFEF_CACHE, cache_key, "")
        return ""


def _extract_rfef_officials(
    league_key: str, sportsdb_event: dict, home_team: str, away_team: str, kickoff: str
) -> dict:
    round_value = sportsdb_event.get("intRound", "")
    text = fetch_rfef_designation_text(league_key, round_value, kickoff)
    if not text:
        return {}
    home_candidates = [home_team, str(sportsdb_event.get("strHomeTeam", "")).strip(), _canonical_team_name(home_team)]
    away_candidates = [away_team, str(sportsdb_event.get("strAwayTeam", "")).strip(), _canonical_team_name(away_team)]
    normalized_text = _normalize_ascii(text).lower()
    blocks = re.split(r"(?=\d{2}-\d{2}-\d{4}\s)", text)
    for block in blocks:
        normalized_block = _normalize_ascii(block).lower()
        home_ok = any(candidate and _normalize_team_name(candidate) in normalized_block for candidate in home_candidates)
        away_ok = any(candidate and _normalize_team_name(candidate) in normalized_block for candidate in away_candidates)
        if not (home_ok and away_ok):
            continue
        referee = re.search(r"Árbitro:\s*([^\n\r]+?)(?:\s+4º Árbitro:|\n|$)", block)
        fourth = re.search(r"4º Árbitro:\s*([^\n\r]+)", block)
        var_match = re.search(r"VAR:\s*([^\n\r]+)", block)
        avar_match = re.search(r"AVAR:\s*([^\n\r]+)", block)
        return {
            "assigned_referee": referee.group(1).strip() if referee else "",
            "fourth_official": fourth.group(1).strip() if fourth else "",
            "var_referee": var_match.group(1).strip() if var_match else "",
            "avar_referee": avar_match.group(1).strip() if avar_match else "",
            "source": "rfef",
        }
    return {}


def _extract_referee_assignment(
    league_key: str,
    kickoff: str,
    home_team: str,
    away_team: str,
    match_news_items: list[dict],
    sportsdb_event: dict,
) -> dict:
    rfef_context = _extract_rfef_officials(league_key, sportsdb_event, home_team, away_team, kickoff)
    if rfef_context.get("assigned_referee"):
        return {
            "assigned_referee": rfef_context.get("assigned_referee", ""),
            "fourth_official": rfef_context.get("fourth_official", ""),
            "var_referee": rfef_context.get("var_referee", ""),
            "avar_referee": rfef_context.get("avar_referee", ""),
            "source": "rfef",
            "candidate_articles": [],
        }
    official_name = str(sportsdb_event.get("strOfficial", "")).strip()
    sportsdb_home_team = str(sportsdb_event.get("strHomeTeam", "")).strip() or home_team
    sportsdb_away_team = str(sportsdb_event.get("strAwayTeam", "")).strip() or away_team
    if official_name and not _looks_like_referee_name(official_name, sportsdb_home_team, sportsdb_away_team):
        official_name = ""
    candidates = _build_referee_candidates_strict(
        match_news_items,
        sportsdb_home_team,
        sportsdb_away_team,
    )
    assigned_referee = official_name
    if not assigned_referee and candidates:
        assigned_referee = candidates[0].get("name", "")
    return {
        "assigned_referee": assigned_referee,
        "fourth_official": "",
        "var_referee": "",
        "avar_referee": "",
        "source": "thesportsdb" if official_name else ("news" if candidates else ""),
        "candidate_articles": candidates,
    }


def _extract_referee_assignment(
    league_key: str,
    kickoff: str,
    home_team: str,
    away_team: str,
    match_news_items: list[dict],
    sportsdb_event: dict,
) -> dict:
    rfef_context = _extract_rfef_officials(league_key, sportsdb_event, home_team, away_team, kickoff)
    if rfef_context.get("assigned_referee"):
        return {
            "assigned_referee": rfef_context.get("assigned_referee", ""),
            "fourth_official": rfef_context.get("fourth_official", ""),
            "var_referee": rfef_context.get("var_referee", ""),
            "avar_referee": rfef_context.get("avar_referee", ""),
            "source": "rfef",
            "candidate_articles": [],
        }
    official_name = str(sportsdb_event.get("strOfficial", "")).strip()
    sportsdb_home_team = str(sportsdb_event.get("strHomeTeam", "")).strip() or home_team
    sportsdb_away_team = str(sportsdb_event.get("strAwayTeam", "")).strip() or away_team
    if official_name and not _looks_like_referee_name(official_name, sportsdb_home_team, sportsdb_away_team):
        official_name = ""
    candidates = _build_referee_candidates_strict(
        match_news_items,
        sportsdb_home_team,
        sportsdb_away_team,
    )
    assigned_referee = official_name
    if not assigned_referee:
        for item in match_news_items:
            assigned_referee = _fetch_article_referee_candidate(
                item,
                sportsdb_home_team,
                sportsdb_away_team,
            )
            if assigned_referee:
                break
    if not assigned_referee and candidates:
        assigned_referee = candidates[0].get("name", "")
    return {
        "assigned_referee": assigned_referee,
        "fourth_official": "",
        "var_referee": "",
        "avar_referee": "",
        "source": "thesportsdb" if official_name else ("news" if (assigned_referee or candidates) else ""),
        "candidate_articles": candidates,
    }


def _structured_referee_record(referee_context: dict, match_entry: dict) -> dict:
    assigned_referee = str(referee_context.get("assigned_referee", "")).strip()
    if not assigned_referee:
        return {}
    return {
        "name": assigned_referee,
        "last_seen_match": {
            "league": match_entry.get("league", ""),
            "local": match_entry.get("local", ""),
            "visitante": match_entry.get("visitante", ""),
            "kickoff": match_entry.get("kickoff", ""),
        },
        "source": referee_context.get("source", ""),
        "season_analysis": referee_context.get("season_analysis", {}),
        "candidate_articles": referee_context.get("candidate_articles", []),
        "updated_at": _now_iso(),
    }


def _normalize_referee_name(value: object) -> str:
    return re.sub(r"\s+", " ", _normalize_ascii(str(value or "")).strip().lower())


def _average(values: list[float]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return None
    return round(sum(clean) / len(clean), 2)


def _find_history_row_for_fixture(
    rows: list[dict],
    home_team: str,
    away_team: str,
    kickoff: str,
) -> dict:
    kickoff_dt = _parse_iso_datetime(kickoff)
    if not kickoff_dt:
        return {}
    best_match = {}
    best_delta = None
    for row in rows:
        if str(row.get("HomeTeam", "")).strip() != home_team:
            continue
        if str(row.get("AwayTeam", "")).strip() != away_team:
            continue
        row_dt = _parse_iso_datetime(str(row.get("_parsed_date", "")).strip())
        if not row_dt:
            continue
        delta = abs((row_dt - kickoff_dt).total_seconds())
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_match = row
    return best_match if best_delta is not None and best_delta <= 3 * 86400 else {}


def _referee_team_stats_payload(record: dict) -> dict:
    matches = int(record.get("matches", 0) or 0)
    if matches <= 0:
        return {}
    points = int(record.get("points", 0) or 0)
    return {
        "matches": matches,
        "wins": int(record.get("wins", 0) or 0),
        "draws": int(record.get("draws", 0) or 0),
        "losses": int(record.get("losses", 0) or 0),
        "points_per_match": round(points / matches, 2),
        "avg_yellows_for": _average(record.get("yellows_for", [])) or 0.0,
        "avg_yellows_against": _average(record.get("yellows_against", [])) or 0.0,
        "avg_reds_for": _average(record.get("reds_for", [])) or 0.0,
        "avg_reds_against": _average(record.get("reds_against", [])) or 0.0,
    }


def _referee_season_analysis(
    league_key: str,
    sportsdb_event: dict,
    assigned_referee: str,
    home_team: str,
    away_team: str,
    history_rows: list[dict],
) -> dict:
    if league_key not in LEAGUE_RFEF_PDF_PREFIX:
        return {}
    normalized_referee = _normalize_referee_name(assigned_referee)
    if not normalized_referee:
        return {}
    league_id = str(sportsdb_event.get("idLeague", "")).strip()
    season = str(sportsdb_event.get("strSeason", "")).strip()
    round_value = str(sportsdb_event.get("intRound", "")).strip()
    if not league_id or not season or not round_value.isdigit():
        return {}
    cache_key = (
        f"referee-analysis:{league_key}:{season}:{round_value}:"
        f"{_normalize_team_name(home_team)}:{_normalize_team_name(away_team)}:{normalized_referee}"
    )
    cached = _cache_get(RFEF_CACHE, cache_key, 12 * 3600)
    if cached:
        return dict(cached)

    current_round = int(round_value)
    season_history = _season_rows(history_rows, _season_code_for(_parse_iso_datetime(_sportsdb_event_kickoff(sportsdb_event))))
    completed_history = _completed_rows_before_kickoff(
        season_history,
        _parse_iso_datetime(_sportsdb_event_kickoff(sportsdb_event)),
    )
    total_matches = 0
    home_wins = 0
    draws = 0
    away_wins = 0
    home_yellows = []
    away_yellows = []
    home_reds = []
    away_reds = []
    team_records = {
        "home": {"team": home_team, "matches": 0, "wins": 0, "draws": 0, "losses": 0, "points": 0, "yellows_for": [], "yellows_against": [], "reds_for": [], "reds_against": []},
        "away": {"team": away_team, "matches": 0, "wins": 0, "draws": 0, "losses": 0, "points": 0, "yellows_for": [], "yellows_against": [], "reds_for": [], "reds_against": []},
    }

    for past_round in range(1, current_round):
        round_events = fetch_the_sportsdb_round_events(league_id, season, past_round)
        for event in round_events:
            event_kickoff = _sportsdb_event_kickoff(event)
            if not event_kickoff:
                continue
            event_home = str(event.get("strHomeTeam", "")).strip()
            event_away = str(event.get("strAwayTeam", "")).strip()
            if not event_home or not event_away:
                continue
            officials = _extract_rfef_officials(league_key, event, event_home, event_away, event_kickoff)
            if _normalize_referee_name(officials.get("assigned_referee", "")) != normalized_referee:
                continue
            home_score = event.get("intHomeScore")
            away_score = event.get("intAwayScore")
            if home_score in {None, ""} or away_score in {None, ""}:
                continue
            home_score = int(home_score)
            away_score = int(away_score)
            total_matches += 1
            if home_score > away_score:
                home_wins += 1
            elif home_score < away_score:
                away_wins += 1
            else:
                draws += 1

            resolved_home = _resolve_csv_team_name(event_home, completed_history) if completed_history else event_home
            resolved_away = _resolve_csv_team_name(event_away, completed_history) if completed_history else event_away
            history_row = _find_history_row_for_fixture(completed_history, resolved_home, resolved_away, event_kickoff)
            if history_row:
                hy = float(history_row.get("HY", 0) or 0)
                ay = float(history_row.get("AY", 0) or 0)
                hr = float(history_row.get("HR", 0) or 0)
                ar = float(history_row.get("AR", 0) or 0)
                home_yellows.append(hy)
                away_yellows.append(ay)
                home_reds.append(hr)
                away_reds.append(ar)
            else:
                hy = ay = hr = ar = 0.0

            for side, tracked_team in (("home", home_team), ("away", away_team)):
                similarity_home = _team_similarity_score(tracked_team, event_home)
                similarity_away = _team_similarity_score(tracked_team, event_away)
                if max(similarity_home, similarity_away) < 0.9:
                    continue
                is_home_team = similarity_home >= similarity_away
                record = team_records[side]
                record["matches"] += 1
                if is_home_team:
                    goals_for, goals_against = home_score, away_score
                    record["yellows_for"].append(hy)
                    record["yellows_against"].append(ay)
                    record["reds_for"].append(hr)
                    record["reds_against"].append(ar)
                else:
                    goals_for, goals_against = away_score, home_score
                    record["yellows_for"].append(ay)
                    record["yellows_against"].append(hy)
                    record["reds_for"].append(ar)
                    record["reds_against"].append(hr)
                if goals_for > goals_against:
                    record["wins"] += 1
                    record["points"] += 3
                elif goals_for < goals_against:
                    record["losses"] += 1
                else:
                    record["draws"] += 1
                    record["points"] += 1

    if total_matches <= 0:
        return {}

    total_completed = max(1, len(completed_history))
    baseline_home_win_pct = round(
        sum(1 for row in completed_history if row.get("FTR") == "H") / total_completed * 100.0,
        2,
    )
    baseline_away_win_pct = round(
        sum(1 for row in completed_history if row.get("FTR") == "A") / total_completed * 100.0,
        2,
    )
    referee_home_win_pct = round(home_wins / total_matches * 100.0, 2)
    referee_away_win_pct = round(away_wins / total_matches * 100.0, 2)
    home_bias_delta = round(referee_home_win_pct - baseline_home_win_pct, 2)
    away_bias_delta = round(referee_away_win_pct - baseline_away_win_pct, 2)
    bias_label = "neutral"
    if home_bias_delta >= 8:
        bias_label = "home-lean"
    elif away_bias_delta >= 8:
        bias_label = "away-lean"

    payload = {
        "sample_matches": total_matches,
        "season": season,
        "overall": {
            "home_win_pct": referee_home_win_pct,
            "draw_pct": round(draws / total_matches * 100.0, 2),
            "away_win_pct": referee_away_win_pct,
            "baseline_home_win_pct": baseline_home_win_pct,
            "baseline_away_win_pct": baseline_away_win_pct,
            "home_bias_delta": home_bias_delta,
            "away_bias_delta": away_bias_delta,
            "avg_home_yellows": _average(home_yellows) or 0.0,
            "avg_away_yellows": _average(away_yellows) or 0.0,
            "avg_home_reds": _average(home_reds) or 0.0,
            "avg_away_reds": _average(away_reds) or 0.0,
            "bias_label": bias_label,
        },
        "home_team": _referee_team_stats_payload(team_records["home"]),
        "away_team": _referee_team_stats_payload(team_records["away"]),
    }
    _cache_set(RFEF_CACHE, cache_key, payload)
    return payload


def _season_code_for(date_value: datetime | None = None) -> str:
    current = date_value or datetime.now(timezone.utc)
    start_year = current.year if current.month >= 7 else current.year - 1
    return f"{start_year % 100:02d}{(start_year + 1) % 100:02d}"


def _recent_season_codes(limit: int | None = None) -> list[str]:
    current = datetime.now(timezone.utc)
    start_year = current.year if current.month >= 7 else current.year - 1
    total = max(1, limit or HISTORY_SEASONS_BACK)
    return [
        f"{(start_year - offset) % 100:02d}{(start_year - offset + 1) % 100:02d}"
        for offset in range(total)
    ]


def _football_data_url(league_code: str, season_code: str) -> str:
    return f"{FOOTBALL_DATA_BASE_URL}/{season_code}/{league_code}.csv"


def fetch_league_history(league_key: str) -> list[dict]:
    league_code = LEAGUE_FOOTBALL_DATA_CODES.get(league_key)
    if not league_code:
        return []
    combined_rows = []
    for season_code in _recent_season_codes():
        cache_key = f"{league_key}:{season_code}"
        cached = _cache_get(HISTORY_CACHE, cache_key, HISTORY_CACHE_TTL_SECONDS)
        if cached:
            combined_rows.extend(cached)
            continue
        try:
            csv_text = _request_text(_football_data_url(league_code, season_code), timeout=30)
            if "<html" in csv_text.lower():
                parsed_rows = []
            else:
                parsed_rows = list(csv.DictReader(io.StringIO(csv_text)))
        except Exception:
            parsed_rows = []
        _cache_set(HISTORY_CACHE, cache_key, parsed_rows)
        combined_rows.extend(parsed_rows)
    return combined_rows


def _completed_rows_before_kickoff(rows: list[dict], kickoff_dt: datetime | None) -> list[dict]:
    completed = []
    for row in rows:
        parsed_date = _parse_match_date(str(row.get("Date", "")).strip())
        if not parsed_date:
            continue
        if kickoff_dt and parsed_date >= kickoff_dt:
            continue
        if row.get("FTR") not in {"H", "D", "A"}:
            continue
        enriched = dict(row)
        enriched["_parsed_date"] = parsed_date.isoformat()
        completed.append(enriched)
    completed.sort(key=lambda item: item["_parsed_date"])
    return completed


def _season_rows(rows: list[dict], season_code: str) -> list[dict]:
    seasonal = []
    for row in rows:
        parsed_date = _parse_match_date(str(row.get("Date", "")).strip())
        if not parsed_date:
            continue
        if _season_code_for(parsed_date) != season_code:
            continue
        enriched = dict(row)
        enriched["_parsed_date"] = parsed_date.isoformat()
        seasonal.append(enriched)
    seasonal.sort(key=lambda item: item["_parsed_date"])
    return seasonal


def _resolve_csv_team_name(team_name: str, rows: list[dict]) -> str:
    options = sorted(
        {
            str(row.get("HomeTeam", "")).strip()
            for row in rows
            if str(row.get("HomeTeam", "")).strip()
        }
        | {
            str(row.get("AwayTeam", "")).strip()
            for row in rows
            if str(row.get("AwayTeam", "")).strip()
        }
    )
    if not options:
        return team_name
    best = max(options, key=lambda candidate: _team_similarity_score(team_name, candidate))
    return best if _team_similarity_score(team_name, best) >= 0.33 else team_name


def _points_from_result(result: str, home: bool) -> int:
    if result == "D":
        return 1
    if result == "H":
        return 3 if home else 0
    if result == "A":
        return 0 if home else 3
    return 0


def _recent_form_metrics(rows: list[dict], team_name: str, last_n: int = 5) -> dict:
    recent = [row for row in rows if row.get("HomeTeam") == team_name or row.get("AwayTeam") == team_name]
    recent = recent[-last_n:]
    if not recent:
        return {}
    form = []
    points = goals_for = goals_against = clean_sheets = 0
    for row in recent:
        home = row.get("HomeTeam") == team_name
        result = row.get("FTR", "")
        goals_scored = int(row.get("FTHG", 0) if home else row.get("FTAG", 0) or 0)
        goals_allowed = int(row.get("FTAG", 0) if home else row.get("FTHG", 0) or 0)
        points += _points_from_result(result, home)
        goals_for += goals_scored
        goals_against += goals_allowed
        clean_sheets += 1 if goals_allowed == 0 else 0
        if result == "D":
            form.append("D")
        elif (result == "H" and home) or (result == "A" and not home):
            form.append("W")
        else:
            form.append("L")
    return {
        "matches": len(recent),
        "form": "".join(form),
        "points": points,
        "points_per_game": round(points / len(recent), 2),
        "goals_for": goals_for,
        "goals_against": goals_against,
        "avg_goals_for": round(goals_for / len(recent), 2),
        "avg_goals_against": round(goals_against / len(recent), 2),
        "clean_sheets": clean_sheets,
    }


def _rolling_team_metrics(rows: list[dict], team_name: str, windows: tuple[int, ...] = (5, 10, 15)) -> dict:
    relevant = [row for row in rows if row.get("HomeTeam") == team_name or row.get("AwayTeam") == team_name]
    metrics = {}
    for window in windows:
        sample = relevant[-window:]
        if not sample:
            metrics[str(window)] = {}
            continue
        goals_for = goals_against = shots_for = shots_against = shots_on_target_for = shots_on_target_against = 0
        valid_shots = valid_sot = 0
        for row in sample:
            is_home = row.get("HomeTeam") == team_name
            gf = int(row.get("FTHG", 0) if is_home else row.get("FTAG", 0) or 0)
            ga = int(row.get("FTAG", 0) if is_home else row.get("FTHG", 0) or 0)
            goals_for += gf
            goals_against += ga
            sf = row.get("HS" if is_home else "AS")
            sa = row.get("AS" if is_home else "HS")
            sotf = row.get("HST" if is_home else "AST")
            sota = row.get("AST" if is_home else "HST")
            if str(sf or "").strip() and str(sa or "").strip():
                shots_for += int(float(sf or 0))
                shots_against += int(float(sa or 0))
                valid_shots += 1
            if str(sotf or "").strip() and str(sota or "").strip():
                shots_on_target_for += int(float(sotf or 0))
                shots_on_target_against += int(float(sota or 0))
                valid_sot += 1
        metrics[str(window)] = {
            "matches": len(sample),
            "avg_goals_for": round(goals_for / len(sample), 2),
            "avg_goals_against": round(goals_against / len(sample), 2),
            "avg_shots_for": round(shots_for / valid_shots, 2) if valid_shots else None,
            "avg_shots_against": round(shots_against / valid_shots, 2) if valid_shots else None,
            "avg_shots_on_target_for": round(shots_on_target_for / valid_sot, 2) if valid_sot else None,
            "avg_shots_on_target_against": round(shots_on_target_against / valid_sot, 2) if valid_sot else None,
        }
    return metrics


def _result_streak(rows: list[dict], team_name: str, last_n: int = 5) -> dict:
    recent = [row for row in rows if row.get("HomeTeam") == team_name or row.get("AwayTeam") == team_name][-last_n:]
    if not recent:
        return {}
    sequence = []
    for row in recent:
        is_home = row.get("HomeTeam") == team_name
        result = row.get("FTR", "")
        if result == "D":
            sequence.append("D")
        elif (result == "H" and is_home) or (result == "A" and not is_home):
            sequence.append("W")
        else:
            sequence.append("L")
    tail = "".join(sequence[-3:])
    if tail.endswith("LLL") or tail.endswith("LL"):
        morale = "low"
    elif tail.endswith("WWW") or tail.endswith("WW"):
        morale = "high"
    else:
        morale = "neutral"
    return {"sequence": "".join(sequence), "morale": morale}


def _elo_ratings(rows: list[dict], base_rating: float = 1500.0, k_factor: float = 22.0) -> dict:
    ratings: dict[str, float] = {}
    ordered = sorted(
        [row for row in rows if row.get("FTR") in {"H", "D", "A"}],
        key=lambda item: item.get("_parsed_date", ""),
    )
    for row in ordered:
        home_team = str(row.get("HomeTeam", "")).strip()
        away_team = str(row.get("AwayTeam", "")).strip()
        if not home_team or not away_team:
            continue
        home_rating = ratings.get(home_team, base_rating)
        away_rating = ratings.get(away_team, base_rating)
        expected_home = 1.0 / (1.0 + 10 ** ((away_rating - home_rating) / 400))
        expected_away = 1.0 - expected_home
        result = row.get("FTR")
        actual_home = 1.0 if result == "H" else (0.5 if result == "D" else 0.0)
        actual_away = 1.0 - actual_home
        ratings[home_team] = round(home_rating + k_factor * (actual_home - expected_home), 2)
        ratings[away_team] = round(away_rating + k_factor * (actual_away - expected_away), 2)
    return ratings


def _future_schedule_difficulty(fixtures: list[dict]) -> dict:
    if not fixtures:
        return {
            "matches": 0,
            "difficulty_index": 0.0,
            "avg_opponent_position": None,
            "top4_matches": 0,
            "top6_matches": 0,
            "top8_matches": 0,
            "hard_opponents": [],
            "label": "low",
        }
    total = 0.0
    positions = []
    top4_matches = 0
    top6_matches = 0
    top8_matches = 0
    hard_opponents = []
    for index, fixture in enumerate(fixtures):
        opponent_position = fixture.get("opponent_position")
        weight = max(0.7, 1.25 - index * 0.12)
        if opponent_position:
            opponent_position = int(opponent_position)
            positions.append(opponent_position)
            total += max(0.0, 22 - float(opponent_position)) * weight
            if opponent_position <= 4:
                top4_matches += 1
            if opponent_position <= 6:
                top6_matches += 1
            if opponent_position <= 8:
                top8_matches += 1
                hard_opponents.append(str(fixture.get("opponent", "")).strip())
        else:
            total += 8.0 * weight
    difficulty = round(min(100.0, (total / max(1, len(fixtures))) * 5.8), 2)
    avg_position = round(sum(positions) / len(positions), 2) if positions else None
    label = "critical" if difficulty >= 72 or top8_matches >= 4 else (
        "high" if difficulty >= 58 or top6_matches >= 3 else (
            "medium" if difficulty >= 40 or top8_matches >= 2 else "low"
        )
    )
    return {
        "matches": len(fixtures),
        "difficulty_index": difficulty,
        "avg_opponent_position": avg_position,
        "top4_matches": top4_matches,
        "top6_matches": top6_matches,
        "top8_matches": top8_matches,
        "hard_opponents": hard_opponents[:6],
        "label": label,
    }


def _fixture_richness_score(fixture: dict) -> int:
    score = 0
    if fixture.get("opponent_position") is not None:
        score += 3
    if fixture.get("opponent_points") is not None:
        score += 2
    if fixture.get("round"):
        score += 2
    if fixture.get("league"):
        score += 1
    source = str(fixture.get("source", "")).strip()
    if source == "odds-feed":
        score += 3
    elif source == "sportsdb-rounds":
        score += 2
    elif source == "espn-fixtures":
        score += 1
    return score


def _same_future_fixture(left: dict, right: dict) -> bool:
    if str(left.get("venue", "")).strip() != str(right.get("venue", "")).strip():
        return False
    if _team_similarity_score(str(left.get("opponent", "")), str(right.get("opponent", ""))) < 0.9:
        return False
    left_dt = _parse_iso_datetime(str(left.get("kickoff", "")).strip())
    right_dt = _parse_iso_datetime(str(right.get("kickoff", "")).strip())
    if left_dt and right_dt:
        if abs((left_dt - right_dt).total_seconds()) <= 36 * 3600:
            return True
    return str(left.get("date", "")).strip() and str(left.get("date", "")).strip() == str(
        right.get("date", "")
    ).strip()


def _pressure_index(team_row: dict, relegation: dict, future_difficulty: dict) -> dict:
    if not team_row:
        return {}
    position = int(team_row.get("position", 0) or 0)
    points = int(team_row.get("points", 0) or 0)
    gap_to_drop = relegation.get("gap_to_drop_zone")
    difficulty = float(future_difficulty.get("difficulty_index", 0.0) or 0.0)
    hard_window = int(future_difficulty.get("top8_matches", 0) or 0)
    position_pressure = max(0.0, 100.0 - min(100.0, position * 4.0))
    points_pressure = max(0.0, 40.0 - points * 0.6)
    if gap_to_drop is None:
        relegation_pressure = 25.0
    else:
        relegation_pressure = max(0.0, 55.0 - (float(gap_to_drop) * 12.0))
    schedule_pressure = difficulty * 0.24 + hard_window * 4.5
    score = round(
        min(
            100.0,
            position_pressure * 0.26
            + points_pressure * 0.12
            + relegation_pressure * 0.34
            + schedule_pressure,
        ),
        2,
    )
    label = "critical" if score >= 75 else ("high" if score >= 55 else ("medium" if score >= 35 else "low"))
    return {
        "score": score,
        "label": label,
        "future_difficulty": difficulty,
        "hard_window_matches": hard_window,
        "gap_to_drop_zone": gap_to_drop,
    }


def _fatigue_index(days_since_last_match: int | None, recent_match_count: int, distance_km: float | None) -> dict:
    rest_component = 45.0 if days_since_last_match is None else max(0.0, 50.0 - (days_since_last_match * 8.0))
    density_component = min(35.0, float(recent_match_count) * 9.0)
    travel_component = min(25.0, (float(distance_km or 0.0) / 1000.0) * 25.0)
    score = round(min(100.0, rest_component + density_component + travel_component), 2)
    label = "high" if score >= 70 else ("medium" if score >= 40 else "low")
    return {"score": score, "label": label}


def _table_snapshot(rows: list[dict]) -> dict:
    table = {}
    for row in rows:
        home_team = str(row.get("HomeTeam", "")).strip()
        away_team = str(row.get("AwayTeam", "")).strip()
        if not home_team or not away_team:
            continue
        home_goals = int(row.get("FTHG", 0) or 0)
        away_goals = int(row.get("FTAG", 0) or 0)
        result = row.get("FTR", "")
        for team in [home_team, away_team]:
            table.setdefault(
                team,
                {
                    "team": team,
                    "played": 0,
                    "points": 0,
                    "goals_for": 0,
                    "goals_against": 0,
                    "goal_diff": 0,
                },
            )
        table[home_team]["played"] += 1
        table[away_team]["played"] += 1
        table[home_team]["goals_for"] += home_goals
        table[home_team]["goals_against"] += away_goals
        table[away_team]["goals_for"] += away_goals
        table[away_team]["goals_against"] += home_goals
        table[home_team]["goal_diff"] = table[home_team]["goals_for"] - table[home_team]["goals_against"]
        table[away_team]["goal_diff"] = table[away_team]["goals_for"] - table[away_team]["goals_against"]
        table[home_team]["points"] += _points_from_result(result, True)
        table[away_team]["points"] += _points_from_result(result, False)

    ordered = sorted(
        table.values(),
        key=lambda row: (-row["points"], -row["goal_diff"], -row["goals_for"], row["team"]),
    )
    positions = {}
    for position, row in enumerate(ordered, start=1):
        enriched = dict(row)
        enriched["position"] = position
        positions[row["team"]] = enriched
    return positions


def _head_to_head_metrics(rows: list[dict], home_team: str, away_team: str, last_n: int = 10) -> dict:
    meetings = []
    for row in rows:
        teams = {row.get("HomeTeam"), row.get("AwayTeam")}
        if home_team in teams and away_team in teams:
            meetings.append(row)
    meetings = meetings[-last_n:]
    if not meetings:
        return {}
    home_team_wins = away_team_wins = draws = 0
    recent_matches = []
    for row in meetings:
        result = row.get("FTR", "")
        if result == "D":
            draws += 1
        elif (result == "H" and row.get("HomeTeam") == home_team) or (
            result == "A" and row.get("AwayTeam") == home_team
        ):
            home_team_wins += 1
        else:
            away_team_wins += 1
        recent_matches.append(
            {
                "date": row.get("Date", ""),
                "home": row.get("HomeTeam", ""),
                "away": row.get("AwayTeam", ""),
                "score": f"{row.get('FTHG', '')}-{row.get('FTAG', '')}",
            }
        )
    return {
        "meetings": len(meetings),
        "home_team_wins": home_team_wins,
        "away_team_wins": away_team_wins,
        "draws": draws,
        "years_span": (
            (
                (_parse_match_date(str(meetings[-1].get("Date", "")).strip()) or datetime.now(timezone.utc)).year
                - (_parse_match_date(str(meetings[0].get("Date", "")).strip()) or datetime.now(timezone.utc)).year
            )
            if meetings
            else 0
        ),
        "recent_matches": recent_matches,
    }


def _team_history_context(rows: list[dict], team_name: str, kickoff_dt: datetime | None) -> dict:
    if not rows:
        return {}
    season_code = _season_code_for(kickoff_dt or datetime.now(timezone.utc))
    filtered = _completed_rows_before_kickoff(_season_rows(rows, season_code), kickoff_dt)
    if not filtered:
        return {}
    resolved = _resolve_csv_team_name(team_name, filtered)
    table = _table_snapshot(filtered)
    recent_all = _recent_form_metrics(filtered, resolved, 5)
    recent_home = _recent_form_metrics([row for row in filtered if row.get("HomeTeam") == resolved], resolved, 5)
    recent_away = _recent_form_metrics([row for row in filtered if row.get("AwayTeam") == resolved], resolved, 5)
    rolling = _rolling_team_metrics(filtered, resolved, (5, 10, 15))
    streak = _result_streak(filtered, resolved, 5)
    elo = _elo_ratings(filtered).get(resolved)
    return {
        "resolved_name": resolved,
        "table": table.get(resolved, {}),
        "recent_all": recent_all,
        "recent_home": recent_home,
        "recent_away": recent_away,
        "rolling": rolling,
        "streak": streak,
        "elo_rating": elo,
    }


def _days_since_last_match(rows: list[dict], team_name: str, kickoff_dt: datetime | None) -> int | None:
    if not kickoff_dt:
        return None
    filtered = _completed_rows_before_kickoff(_season_rows(rows, _season_code_for(kickoff_dt)), kickoff_dt)
    relevant = [row for row in filtered if row.get("HomeTeam") == team_name or row.get("AwayTeam") == team_name]
    if not relevant:
        return None
    last_played = _parse_iso_datetime(relevant[-1].get("_parsed_date", ""))
    if not last_played:
        return None
    return max(0, int((kickoff_dt - last_played).total_seconds() // 86400))


def _matches_in_recent_days(
    rows: list[dict], team_name: str, kickoff_dt: datetime | None, days: int = 14
) -> int:
    if not kickoff_dt:
        return 0
    filtered = _completed_rows_before_kickoff(_season_rows(rows, _season_code_for(kickoff_dt)), kickoff_dt)
    window_start = kickoff_dt.timestamp() - days * 86400
    count = 0
    for row in filtered:
        parsed_dt = _parse_iso_datetime(row.get("_parsed_date", ""))
        if not parsed_dt:
            continue
        if parsed_dt.timestamp() < window_start:
            continue
        if row.get("HomeTeam") == team_name or row.get("AwayTeam") == team_name:
            count += 1
    return count


def _upcoming_team_fixtures(
    rows: list[dict],
    team_name: str,
    kickoff_dt: datetime | None,
    table_snapshot: dict,
    next_n: int = UPCOMING_FIXTURE_WINDOW,
) -> list[dict]:
    if not kickoff_dt:
        return []
    season_rows = _season_rows(rows, _season_code_for(kickoff_dt))
    fixtures = []
    for row in season_rows:
        parsed_dt = _parse_iso_datetime(row.get("_parsed_date", ""))
        if not parsed_dt or parsed_dt < kickoff_dt:
            continue
        home_team = str(row.get("HomeTeam", "")).strip()
        away_team = str(row.get("AwayTeam", "")).strip()
        if team_name not in {home_team, away_team}:
            continue
        is_home = home_team == team_name
        opponent = away_team if is_home else home_team
        fixtures.append(
            {
                "date": row.get("Date", ""),
                "kickoff": row.get("_parsed_date", ""),
                "venue": "home" if is_home else "away",
                "opponent": opponent,
                "opponent_position": (table_snapshot.get(opponent) or {}).get("position"),
                "opponent_points": (table_snapshot.get(opponent) or {}).get("points"),
                "source": "football-data",
            }
        )
    fixtures.sort(key=lambda item: item.get("kickoff", ""))
    return fixtures[:next_n]


def _upcoming_feed_fixtures(
    raw_matches: list[dict],
    team_name: str,
    kickoff_dt: datetime | None,
    league_key: str,
    table_snapshot: dict,
    history_rows: list[dict],
    next_n: int = UPCOMING_FIXTURE_WINDOW,
) -> list[dict]:
    if not kickoff_dt:
        return []
    fixtures = []
    for item in raw_matches:
        candidate_kickoff = _parse_iso_datetime(str(item.get("commence_time", "")).strip())
        if not candidate_kickoff or candidate_kickoff <= kickoff_dt:
            continue
        home_team = str(item.get("home_team", "")).strip()
        away_team = str(item.get("away_team", "")).strip()
        home_score = _team_similarity_score(team_name, home_team)
        away_score = _team_similarity_score(team_name, away_team)
        if max(home_score, away_score) < 0.9:
            continue
        is_home = home_score >= away_score
        opponent = away_team if is_home else home_team
        resolved_opponent = _resolve_csv_team_name(opponent, history_rows) if history_rows else opponent
        fixtures.append(
            {
                "date": str(item.get("commence_time", "")).strip()[:10],
                "kickoff": str(item.get("commence_time", "")).strip(),
                "venue": "home" if is_home else "away",
                "opponent": opponent,
                "opponent_position": (table_snapshot.get(resolved_opponent) or {}).get("position"),
                "opponent_points": (table_snapshot.get(resolved_opponent) or {}).get("points"),
                "league": str(item.get("sport_key", "")).strip() or league_key,
                "source": "odds-feed",
            }
        )
    fixtures.sort(key=lambda item: item.get("kickoff", ""))
    return fixtures[:next_n]


def _upcoming_round_fixtures(
    team_name: str,
    kickoff_dt: datetime | None,
    sportsdb_event: dict,
    table_snapshot: dict,
    history_rows: list[dict],
    next_n: int = UPCOMING_FIXTURE_WINDOW,
    rounds_ahead: int = 8,
) -> list[dict]:
    if not kickoff_dt:
        return []
    league_id = str(sportsdb_event.get("idLeague", "")).strip()
    season = str(sportsdb_event.get("strSeason", "")).strip()
    round_value = str(sportsdb_event.get("intRound", "")).strip()
    if not league_id or not season or not round_value.isdigit():
        return []
    fixtures = []
    seen = set()
    current_round = int(round_value)
    for future_round in range(current_round + 1, current_round + rounds_ahead + 1):
        round_events = fetch_the_sportsdb_round_events(league_id, season, future_round)
        for event in round_events:
            event_kickoff = _sportsdb_event_kickoff(event)
            event_dt = _parse_iso_datetime(event_kickoff)
            if not event_kickoff or not event_dt or event_dt <= kickoff_dt:
                continue
            home_team = str(event.get("strHomeTeam", "")).strip()
            away_team = str(event.get("strAwayTeam", "")).strip()
            if not home_team or not away_team:
                continue
            home_score = _team_similarity_score(team_name, home_team)
            away_score = _team_similarity_score(team_name, away_team)
            if max(home_score, away_score) < 0.9:
                continue
            is_home = home_score >= away_score
            opponent = away_team if is_home else home_team
            resolved_opponent = _resolve_csv_team_name(opponent, history_rows) if history_rows else opponent
            fixture_key = (event_kickoff, _normalize_team_name(opponent), "home" if is_home else "away")
            if fixture_key in seen:
                continue
            seen.add(fixture_key)
            fixtures.append(
                {
                    "date": str(event.get("dateEvent", "")).strip(),
                    "kickoff": event_kickoff,
                    "venue": "home" if is_home else "away",
                    "opponent": opponent,
                    "opponent_position": (table_snapshot.get(resolved_opponent) or {}).get("position"),
                    "opponent_points": (table_snapshot.get(resolved_opponent) or {}).get("points"),
                    "round": event.get("intRound", ""),
                    "source": "sportsdb-rounds",
                }
            )
        if len(fixtures) >= next_n:
            break
    fixtures.sort(key=lambda item: item.get("kickoff", ""))
    return fixtures[:next_n]


def _upcoming_sportsdb_next_fixtures(
    team_name: str,
    team_id: str,
    kickoff_dt: datetime | None,
    table_snapshot: dict,
    history_rows: list[dict],
    next_n: int = UPCOMING_FIXTURE_WINDOW,
) -> list[dict]:
    if not kickoff_dt or not team_id:
        return []
    fixtures = []
    for event in fetch_the_sportsdb_next_events(team_id):
        event_kickoff = _sportsdb_event_kickoff(event)
        event_dt = _parse_iso_datetime(event_kickoff)
        if not event_dt or event_dt <= kickoff_dt:
            continue
        home_team = str(event.get("strHomeTeam", "")).strip()
        away_team = str(event.get("strAwayTeam", "")).strip()
        if not home_team or not away_team:
            continue
        home_score = _team_similarity_score(team_name, home_team)
        away_score = _team_similarity_score(team_name, away_team)
        if max(home_score, away_score) < 0.86:
            continue
        is_home = home_score >= away_score
        opponent = away_team if is_home else home_team
        resolved_opponent = _resolve_csv_team_name(opponent, history_rows) if history_rows else opponent
        fixtures.append(
            {
                "date": str(event.get("dateEvent", "")).strip() or event_kickoff[:10],
                "kickoff": event_kickoff,
                "venue": "home" if is_home else "away",
                "opponent": opponent,
                "opponent_position": (table_snapshot.get(resolved_opponent) or {}).get("position"),
                "opponent_points": (table_snapshot.get(resolved_opponent) or {}).get("points"),
                "league": str(event.get("strLeague", "")).strip(),
                "round": str(event.get("intRound", "")).strip(),
                "stage": str(event.get("strRound", "")).strip(),
                "source": "sportsdb-next",
            }
        )
    fixtures.sort(key=lambda item: item.get("kickoff", ""))
    return fixtures[:next_n]


def _merge_upcoming_fixtures(*fixture_lists: list[dict], next_n: int = UPCOMING_FIXTURE_WINDOW) -> list[dict]:
    merged = []
    for fixture_list in fixture_lists:
        for fixture in fixture_list or []:
            opponent = str(fixture.get("opponent", "")).strip()
            kickoff = str(fixture.get("kickoff", "")).strip()
            venue = str(fixture.get("venue", "")).strip()
            if not opponent or not kickoff:
                continue
            candidate = dict(fixture)
            existing_index = next(
                (idx for idx, current in enumerate(merged) if _same_future_fixture(current, candidate)),
                None,
            )
            if existing_index is None:
                merged.append(candidate)
                continue
            current = merged[existing_index]
            if _fixture_richness_score(candidate) > _fixture_richness_score(current):
                preferred = candidate
                secondary = current
            else:
                preferred = current
                secondary = candidate
            for field in [
                "date",
                "kickoff",
                "venue",
                "opponent",
                "opponent_position",
                "opponent_points",
                "round",
                "league",
            ]:
                if preferred.get(field) in {None, ""} and secondary.get(field) not in {None, ""}:
                    preferred[field] = secondary.get(field)
            if secondary.get("source") and secondary.get("source") not in {
                preferred.get("source"),
                "",
                None,
            }:
                preferred["source"] = f"{preferred.get('source', '')}+{secondary.get('source', '')}".strip("+")
            merged[existing_index] = preferred
    merged.sort(key=lambda item: item.get("kickoff", ""))
    return merged[:next_n]


def _relegation_context(league_key: str, table_snapshot: dict, team_name: str) -> dict:
    team_row = table_snapshot.get(team_name) or {}
    if not team_row:
        return {}
    start_position = LEAGUE_RELEGATION_START.get(league_key)
    ordered = sorted(table_snapshot.values(), key=lambda item: item.get("position", 999))
    drop_row = next((row for row in ordered if row.get("position") == start_position), {})
    safe_row = next((row for row in ordered if row.get("position") == max(1, (start_position or 1) - 1)), {})
    team_points = int(team_row.get("points", 0) or 0)
    drop_points = int(drop_row.get("points", team_points) or team_points)
    safe_points = int(safe_row.get("points", team_points) or team_points)
    gap_to_drop = team_points - drop_points
    gap_to_safe = safe_points - team_points
    urgency = "high" if gap_to_drop <= 2 else ("medium" if gap_to_drop <= 5 else "low")
    return {
        "position": team_row.get("position"),
        "points": team_points,
        "drop_zone_starts_at": start_position,
        "drop_zone_points": drop_points,
        "safe_line_points": safe_points,
        "gap_to_drop_zone": gap_to_drop,
        "gap_to_safe_line": gap_to_safe,
        "urgency": urgency,
    }


def _ordered_table_rows(table_snapshot: dict) -> list[dict]:
    return sorted(
        [row for row in table_snapshot.values() if row.get("position")],
        key=lambda item: int(item.get("position", 999) or 999),
    )


def _league_total_rounds(table_snapshot: dict) -> int | None:
    teams = len(table_snapshot or {})
    if teams < 2:
        return None
    return max(1, (teams - 1) * 2)


def _season_context_phase(kickoff_dt: datetime | None, table_snapshot: dict, team_row: dict) -> dict:
    played = _safe_int(team_row.get("played"), None) if team_row else None
    total_rounds = _league_total_rounds(table_snapshot)
    progress = None
    source = ""
    if played is not None and total_rounds:
        progress = min(1.0, max(0.0, played / float(total_rounds)))
        source = "played_matches"
    elif kickoff_dt:
        month = kickoff_dt.month
        if month in {8, 9, 10}:
            progress = 0.2
        elif month in {11, 12, 1, 2}:
            progress = 0.5
        elif month in {3, 4}:
            progress = 0.78
        elif month in {5, 6}:
            progress = 0.92
        source = "calendar_date"
    if progress is None:
        return {"key": "", "label": "", "played": played, "total_rounds": total_rounds, "source": ""}
    if progress < 0.28:
        key = "early"
        label = "tramo temprano"
    elif progress < 0.65:
        key = "middle"
        label = "tramo medio"
    elif progress < 0.86:
        key = "decisive"
        label = "tramo decisivo"
    else:
        key = "final"
        label = "final de temporada"
    return {
        "key": key,
        "label": label,
        "played": played,
        "total_rounds": total_rounds,
        "progress": round(progress, 3),
        "source": source,
    }


def _urgency_from_margin(margin: int | None, phase_key: str) -> str:
    if margin is None:
        return "unknown"
    phase_boost = 1 if phase_key in {"decisive", "final"} else 0
    if margin <= 1:
        return "critical" if phase_boost else "high"
    if margin <= 3:
        return "high" if phase_boost else "medium"
    if margin <= 6:
        return "medium"
    return "low"


def _objective_candidate(
    line: dict,
    team_row: dict,
    ordered_rows: list[dict],
    phase_key: str,
) -> dict:
    position = _safe_int(team_row.get("position"), None)
    points = _safe_int(team_row.get("points"), None)
    line_position = _safe_int(line.get("line_position"), None)
    if position is None or points is None or line_position is None:
        return {}
    line_row = next((row for row in ordered_rows if _safe_int(row.get("position"), 0) == line_position), {})
    line_points = _safe_int(line_row.get("points"), None)
    if line_points is None:
        return {}
    direction = str(line.get("direction", "top"))
    objective_key = str(line.get("key", ""))
    objective_label = str(line.get("label", ""))
    gap_points = None
    cushion_points = None
    status = "unknown"
    margin = None
    relevance = 0.0

    if direction == "survival":
        drop_position = line_position + 1
        drop_row = next((row for row in ordered_rows if _safe_int(row.get("position"), 0) == drop_position), {})
        drop_points = _safe_int(drop_row.get("points"), line_points)
        if position <= line_position:
            status = "defending"
            cushion_points = points - int(drop_points or line_points)
            margin = cushion_points
            relevance = 72.0 - max(0, cushion_points or 0) * 7.0
        else:
            status = "chasing"
            gap_points = line_points - points
            margin = gap_points
            relevance = 86.0 - max(0, gap_points or 0) * 7.0
        if position >= line_position - 4 or (margin is not None and margin <= 8):
            relevance += 22.0
    else:
        outside_row = next(
            (row for row in ordered_rows if _safe_int(row.get("position"), 0) == line_position + 1),
            {},
        )
        outside_points = _safe_int(outside_row.get("points"), line_points)
        if position <= line_position:
            status = "defending"
            cushion_points = points - int(outside_points or line_points)
            margin = cushion_points
            relevance = 74.0 - max(0, cushion_points or 0) * 8.0
        else:
            status = "chasing"
            gap_points = line_points - points
            margin = gap_points
            if objective_key == "title" and (gap_points is None or gap_points > 8):
                return {}
            if objective_key != "title" and gap_points is not None and gap_points > 12 and phase_key in {"decisive", "final"}:
                return {}
            relevance = 80.0 - max(0, gap_points or 0) * 8.0
        if abs(position - line_position) <= 3 or (margin is not None and margin <= 6):
            relevance += 20.0
        if objective_key == "title" and position > 4:
            relevance -= 35.0

    if phase_key == "final":
        relevance += 12.0
    elif phase_key == "decisive":
        relevance += 8.0
    urgency = _urgency_from_margin(margin, phase_key)
    if objective_key == "survival":
        if status == "chasing":
            urgency = "critical" if phase_key in {"decisive", "final"} else "high"
        elif status == "defending" and position >= line_position:
            urgency = "critical" if phase_key == "final" else "high"
        elif status == "defending" and margin is not None and margin <= 6 and phase_key in {"decisive", "final"}:
            urgency = "high"
    if urgency == "critical":
        relevance += 16.0
    elif urgency == "high":
        relevance += 10.0
    elif urgency == "medium":
        relevance += 4.0

    if status == "defending":
        if objective_key == "survival":
            summary = f"defiende salvacion con {cushion_points} pts de colchon"
        else:
            summary = f"defiende {objective_label} con {cushion_points} pts de colchon"
    elif objective_key == "survival":
        summary = f"persigue salvacion a {gap_points} pts"
    else:
        summary = f"persigue {objective_label} a {gap_points} pts"

    return {
        "objective_key": objective_key,
        "objective_label": objective_label,
        "status": status,
        "urgency": urgency,
        "summary": summary,
        "gap_points": gap_points,
        "cushion_points": cushion_points,
        "line_position": line_position,
        "line_points": line_points,
        "relevance_score": round(max(0.0, min(100.0, relevance)), 2),
    }


def _material_swing(team_row: dict, objective: dict, points_delta: int) -> str:
    if not team_row or not objective:
        return ""
    points = _safe_int(team_row.get("points"), None)
    if points is None:
        return ""
    projected = points + points_delta
    label = objective.get("objective_label") or objective.get("objective_key") or "objetivo"
    status = objective.get("status")
    line_points = _safe_int(objective.get("line_points"), None)
    gap = _safe_int(objective.get("gap_points"), None)
    cushion = _safe_int(objective.get("cushion_points"), None)
    if line_points is None:
        return ""
    if points_delta > 0:
        if status == "chasing" and gap is not None and projected >= line_points:
            return f"victoria le mete de lleno en {label}" if projected == line_points else f"victoria supera linea de {label}"
        if status == "defending" and cushion is not None:
            return f"victoria abre colchon en {label}"
    else:
        line_position = _safe_int(objective.get("line_position"), 0)
        position = _safe_int(team_row.get("position"), 99)
        if status == "defending" and cushion is not None and (
            cushion <= 3 or (objective.get("objective_key") == "survival" and line_position and position >= line_position)
        ):
            if objective.get("objective_key") == "survival":
                return "derrota puede meterle en descenso o dejarle al borde"
            return f"derrota deja en riesgo {label}"
        if status == "chasing" and gap is not None and gap <= 3:
            return f"derrota aleja {label}"
    return "impacto material bajo"


def _objective_swing_index(objective: dict, outcome: str, phase_key: str) -> int:
    if not objective:
        return 0
    margin = objective.get("gap_points")
    if margin is None:
        margin = objective.get("cushion_points")
    margin = _safe_int(margin, 99)
    margin_value = int(margin if margin is not None else 99)
    base = max(0, 75 - margin_value * 12)
    if phase_key == "final":
        base += 18
    elif phase_key == "decisive":
        base += 12
    if outcome == "lose" and objective.get("status") == "defending":
        base += 8
    if outcome == "win" and objective.get("status") == "chasing":
        base += 8
    return int(max(0, min(100, base)))


def _team_objective_context(
    league_key: str,
    table_snapshot: dict,
    team_name: str,
    kickoff_dt: datetime | None,
) -> dict:
    team_row = table_snapshot.get(team_name) or {}
    if not team_row:
        return {}
    ordered_rows = _ordered_table_rows(table_snapshot)
    phase = _season_context_phase(kickoff_dt, table_snapshot, team_row)
    phase_key = phase.get("key", "")
    candidates = [
        candidate
        for line in LEAGUE_COMPETITIVE_LINES.get(league_key, [])
        for candidate in [_objective_candidate(line, team_row, ordered_rows, phase_key)]
        if candidate
    ]
    candidates.sort(key=lambda item: item.get("relevance_score", 0), reverse=True)
    primary = dict(candidates[0]) if candidates else {}
    if not primary:
        return {"season_context_phase": phase, "objective_candidates": []}
    margin = primary.get("gap_points")
    if margin is None:
        margin = primary.get("cushion_points")
    margin = _safe_int(margin, 99)
    margin_value = int(margin if margin is not None else 99)
    phase_bonus = 18 if phase_key == "final" else (12 if phase_key == "decisive" else 0)
    must_win = 0
    must_not_lose = 0
    if primary.get("status") == "chasing":
        must_win = 82 - margin_value * 13 + phase_bonus
        must_not_lose = 58 - margin_value * 7 + phase_bonus
    elif primary.get("status") == "defending":
        must_win = 48 - margin_value * 5 + phase_bonus
        must_not_lose = 78 - margin_value * 12 + phase_bonus
    if primary.get("objective_key") == "survival":
        must_not_lose += 10
        if primary.get("status") == "chasing":
            must_win += 8
        if primary.get("status") == "defending":
            position = _safe_int(team_row.get("position"), 99)
            line_position = _safe_int(primary.get("line_position"), 0)
            if line_position and position >= line_position:
                must_not_lose += 22
                must_win += 8
            elif margin_value <= 6 and phase_key in {"decisive", "final"}:
                must_not_lose += 14
    primary.update(
        {
            "must_win_index": int(max(0, min(100, must_win))),
            "must_not_lose_index": int(max(0, min(100, must_not_lose))),
            "objective_swing_if_win": _objective_swing_index(primary, "win", phase_key),
            "objective_swing_if_lose": _objective_swing_index(primary, "lose", phase_key),
            "swing_summary_if_win": _material_swing(team_row, primary, 3),
            "swing_summary_if_lose": _material_swing(team_row, primary, 0),
            "season_context_phase": phase,
        }
    )
    return {
        **primary,
        "objective_candidates": candidates[:4],
    }


def _direct_rivalry_context(home_objective: dict, away_objective: dict, home_row: dict, away_row: dict) -> dict:
    if not home_objective or not away_objective:
        return {"is_direct_rivalry": False, "direct_rivalry_index": 0}
    same_objective = home_objective.get("objective_key") == away_objective.get("objective_key")
    home_points = _safe_int(home_row.get("points"), None)
    away_points = _safe_int(away_row.get("points"), None)
    home_position = _safe_int(home_row.get("position"), None)
    away_position = _safe_int(away_row.get("position"), None)
    if not same_objective or home_points is None or away_points is None or home_position is None or away_position is None:
        return {"is_direct_rivalry": False, "direct_rivalry_index": 0}
    points_delta = abs(home_points - away_points)
    position_delta = abs(home_position - away_position)
    close = points_delta <= 6 and position_delta <= 5
    score = 0
    if close:
        score = 70
        score += max(0, 18 - points_delta * 3)
        score += max(0, 12 - position_delta * 2)
    score = int(max(0, min(100, score)))
    label = ""
    if score:
        objective_label = home_objective.get("objective_label") or home_objective.get("objective_key")
        if home_objective.get("objective_key") == "direct_promotion":
            label = "duelo directo por ascenso"
        elif home_objective.get("objective_key") == "playoff":
            label = "duelo directo por play-off"
        elif home_objective.get("objective_key") == "survival":
            label = "duelo directo por salvacion"
        else:
            label = f"duelo directo por {objective_label}"
    return {
        "is_direct_rivalry": bool(score),
        "direct_rivalry_index": score,
        "objective_key": home_objective.get("objective_key") if score else "",
        "objective_label": home_objective.get("objective_label") if score else "",
        "points_delta": points_delta,
        "position_delta": position_delta,
        "label": label,
    }


def _competitive_stakes_label(home_objective: dict, away_objective: dict, rivalry: dict) -> str:
    if rivalry.get("is_direct_rivalry"):
        return str(rivalry.get("label", "")).strip()
    pieces = []
    for side, objective in [("local", home_objective), ("visitante", away_objective)]:
        if not objective:
            continue
        summary = str(objective.get("summary", "")).strip()
        urgency = str(objective.get("urgency", "")).strip()
        if summary and urgency in {"critical", "high", "medium"}:
            pieces.append(f"{side} {summary}")
    return "; ".join(pieces[:2]) or "contexto competitivo bajo o no calculable"


def _season_competitive_context(
    league_key: str,
    table_snapshot: dict,
    home_team: str,
    away_team: str,
    kickoff_dt: datetime | None,
) -> dict:
    home_objective = _team_objective_context(league_key, table_snapshot, home_team, kickoff_dt)
    away_objective = _team_objective_context(league_key, table_snapshot, away_team, kickoff_dt)
    home_row = table_snapshot.get(home_team) or {}
    away_row = table_snapshot.get(away_team) or {}
    rivalry = _direct_rivalry_context(home_objective, away_objective, home_row, away_row)
    phase = home_objective.get("season_context_phase") or away_objective.get("season_context_phase") or {}
    label = _competitive_stakes_label(home_objective, away_objective, rivalry)
    return {
        "season_context_phase": phase,
        "home_objective": home_objective,
        "away_objective": away_objective,
        "direct_rivalry": rivalry,
        "competitive_stakes_label": label,
    }


def _fatigue_rating(days_since_last_match: int | None, recent_match_count: int) -> str:
    if days_since_last_match is None:
        return "unknown"
    if days_since_last_match <= 2 or recent_match_count >= 4:
        return "high"
    if days_since_last_match <= 4 or recent_match_count >= 3:
        return "medium"
    return "low"


def _nearest_index(target: datetime, candidates: list[str]) -> int | None:
    best_index = None
    best_delta = None
    for idx, candidate in enumerate(candidates):
        candidate_dt = _parse_iso_datetime(candidate)
        if not candidate_dt:
            continue
        if candidate_dt.tzinfo is None and target.tzinfo is not None:
            candidate_dt = candidate_dt.replace(tzinfo=target.tzinfo)
        elif candidate_dt.tzinfo is not None and target.tzinfo is None:
            target = target.replace(tzinfo=candidate_dt.tzinfo)
        delta = abs((candidate_dt - target).total_seconds())
        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_index = idx
    return best_index


def fetch_weather_context(profile: dict, kickoff: str) -> dict:
    latitude = profile.get("latitude")
    longitude = profile.get("longitude")
    kickoff_dt = _parse_iso_datetime(kickoff)
    if latitude is None or longitude is None or kickoff_dt is None:
        return {}
    cache_key = f"{round(float(latitude), 3)}|{round(float(longitude), 3)}|{kickoff_dt.date().isoformat()}"
    cached = _cache_get(WEATHER_CACHE, cache_key, WEATHER_CACHE_TTL_SECONDS)
    if cached:
        return cached
    try:
        data = _request_json(
            OPEN_METEO_FORECAST_URL,
            params={
                "latitude": latitude,
                "longitude": longitude,
                "hourly": ",".join(
                    [
                        "temperature_2m",
                        "precipitation_probability",
                        "precipitation",
                        "wind_speed_10m",
                        "wind_gusts_10m",
                        "weather_code",
                    ]
                ),
                "timezone": "auto",
                "forecast_days": 7,
            },
            timeout=20,
        )
    except Exception:
        return {}
    hourly = data.get("hourly") or {}
    times = hourly.get("time") or []
    idx = _nearest_index(kickoff_dt, times)
    if idx is None:
        return {}
    weather = {
        "timezone": data.get("timezone", ""),
        "forecast_time": times[idx],
        "temperature_c": (hourly.get("temperature_2m") or [None])[idx],
        "precipitation_probability": (hourly.get("precipitation_probability") or [None])[idx],
        "precipitation_mm": (hourly.get("precipitation") or [None])[idx],
        "wind_speed_kmh": (hourly.get("wind_speed_10m") or [None])[idx],
        "wind_gusts_kmh": (hourly.get("wind_gusts_10m") or [None])[idx],
        "weather_code": (hourly.get("weather_code") or [None])[idx],
    }
    _cache_set(WEATHER_CACHE, cache_key, weather)
    return weather


def _weather_risk(weather: dict) -> str:
    if not weather:
        return "unknown"
    precipitation_probability = weather.get("precipitation_probability") or 0
    wind_gusts = weather.get("wind_gusts_kmh") or 0
    precipitation_mm = weather.get("precipitation_mm") or 0
    if precipitation_probability >= 70 or precipitation_mm >= 2 or wind_gusts >= 45:
        return "high"
    if precipitation_probability >= 40 or wind_gusts >= 30:
        return "medium"
    return "low"


def _haversine_km(lat1, lon1, lat2, lon2) -> float | None:
    if None in {lat1, lon1, lat2, lon2}:
        return None
    earth_radius_km = 6371.0
    lat1_rad = math.radians(float(lat1))
    lon1_rad = math.radians(float(lon1))
    lat2_rad = math.radians(float(lat2))
    lon2_rad = math.radians(float(lon2))
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(earth_radius_km * c, 1)


def _distance_bucket(distance_km: float | None) -> str:
    if distance_km is None:
        return "unknown"
    if distance_km < 100:
        return "local"
    if distance_km < 300:
        return "short"
    if distance_km < 800:
        return "medium"
    if distance_km < 1500:
        return "long"
    return "very_long"


def _odds_probabilities(odds: dict) -> dict:
    implied = {}
    total = 0.0
    for key, value in odds.items():
        if value:
            prob = 1.0 / float(value)
            implied[key] = prob
            total += prob
    normalized = {}
    for key, value in implied.items():
        normalized[key] = round((value / total) * 100, 2) if total else None
    return {
        "implied_percent": {key: round(value * 100, 2) for key, value in implied.items()},
        "normalized_percent": normalized,
        "overround_percent": round(max(0.0, (total - 1.0) * 100), 2),
    }


def _rotation_risk(signals: dict) -> str:
    injury = signals.get("injury_count", 0)
    rotation = signals.get("rotation_count", 0)
    europe = signals.get("europe_count", 0)
    if injury >= 2 or (rotation >= 1 and europe >= 1):
        return "high"
    if injury >= 1 or rotation >= 1 or europe >= 1:
        return "medium"
    return "low"


def _source_health_summary(competition_headlines: dict) -> dict:
    sources_total = 0
    sources_ok = 0
    fresh_headlines = 0
    stale_headlines = 0
    for payload in competition_headlines.values():
        for source in payload.get("source_health", []):
            sources_total += 1
            if source.get("ok"):
                sources_ok += 1
        for item in payload.get("items", []):
            age_days = _news_age_days(str(item.get("published_at", "")).strip())
            if age_days is None or age_days <= COMPETITION_NEWS_MAX_AGE_DAYS:
                fresh_headlines += 1
            else:
                stale_headlines += 1
    return {
        "sources_total": sources_total,
        "sources_ok": sources_ok,
        "fresh_headlines": fresh_headlines,
        "stale_headlines": stale_headlines,
    }


def _focus_match_digest(match: dict) -> list[str]:
    digest = []
    weather_risk = (match.get("match_signals") or {}).get("weather_risk", "")
    if weather_risk == "high":
        digest.append("clima duro")
    travel_bucket = (match.get("travel_context") or {}).get("distance_bucket", "")
    if travel_bucket in {"long", "very_long"}:
        digest.append("viaje largo")
    home_inj = (match.get("structured_context") or {}).get("injury_context", {}).get("home_team", {}).get("count", 0)
    away_inj = (match.get("structured_context") or {}).get("injury_context", {}).get("away_team", {}).get("count", 0)
    if home_inj:
        digest.append(f"bajas local {home_inj}")
    if away_inj:
        digest.append(f"bajas visitante {away_inj}")
    home_news_signals = (match.get("home_team_context") or {}).get("focus_news", {}).get("signals", {})
    away_news_signals = (match.get("away_team_context") or {}).get("focus_news", {}).get("signals", {})
    if (home_news_signals.get("morale_count", 0) or away_news_signals.get("morale_count", 0)):
        digest.append("contexto mental")
    if (home_news_signals.get("squad_count", 0) or away_news_signals.get("squad_count", 0)):
        digest.append("convocatoria")
    referee_name = (match.get("structured_context") or {}).get("referee_context", {}).get("assigned_referee", "")
    if referee_name:
        digest.append("arbitro identificado")
    home_fatigue = (match.get("schedule_context") or {}).get("home", {}).get("fatigue", "")
    away_fatigue = (match.get("schedule_context") or {}).get("away", {}).get("fatigue", "")
    if home_fatigue == "high" or away_fatigue == "high":
        digest.append("calendario apretado")
    future_home = (match.get("competition_context") or {}).get("home_future_difficulty", {})
    future_away = (match.get("competition_context") or {}).get("away_future_difficulty", {})
    if future_home.get("label") in {"high", "critical"} or future_away.get("label") in {"high", "critical"}:
        digest.append("ventana dura")
    competition = match.get("competition_context") or {}
    if (competition.get("direct_rivalry") or {}).get("is_direct_rivalry"):
        digest.append("duelo directo")
    elif competition.get("competitive_stakes_label"):
        digest.append("objetivo competitivo")
    if (competition.get("home_rotation_context") or {}).get("risk") == "high" or (
        competition.get("away_rotation_context") or {}
    ).get("risk") == "high":
        digest.append("rotacion probable")
    return digest[:4]


def _brief_headlines(items: list[dict], limit: int = 4) -> list[str]:
    headlines = []
    for item in items[:limit]:
        title = str(item.get("title", "")).strip()
        source = str(item.get("source", "")).strip()
        if not title:
            continue
        headlines.append(f"{title} [{source}]".strip())
    return headlines


def _quiniela_slot_labels(match: dict) -> list[str]:
    labels = []
    for slot in match.get("quiniela_slots") or []:
        jornada = slot.get("jornada")
        position = slot.get("position")
        if not jornada or not position:
            continue
        label = f"J{jornada} · {position}"
        if slot.get("pleno15"):
            label += " · pleno al 15"
        labels.append(label)
    return labels


def _official_quiniela_percentages_line(match: dict) -> str:
    percentages = match.get("official_quiniela_percentages") or {}
    if not percentages:
        for slot in match.get("quiniela_slots") or []:
            percentages = (
                (slot.get("percentages") or {}).get("quinielista")
                or (slot.get("percentages") or {}).get("lae")
                or {}
            )
            if percentages:
                break
    if not percentages:
        return "Sin porcentaje oficial Quinielista disponible"
    return (
        f"Quinielista 1={percentages.get('1', '-')}, "
        f"X={percentages.get('X', '-')}, 2={percentages.get('2', '-')}"
    )


def _fixture_summary(fixtures: list[dict], limit: int = UPCOMING_FIXTURE_WINDOW) -> str:
    parts = []
    for fixture in fixtures[:limit]:
        opponent = str(fixture.get("opponent", "")).strip()
        if not opponent:
            continue
        venue = "casa" if fixture.get("venue") == "home" else "fuera"
        opponent_position = fixture.get("opponent_position")
        if opponent_position:
            parts.append(f"{venue} vs {opponent} ({opponent_position}º)")
        else:
            parts.append(f"{venue} vs {opponent}")
    return "; ".join(parts) or "sin calendario cercano detectado"


def _render_fixture_list_html(fixtures: list[dict], limit: int = 4) -> str:
    rows = []
    for fixture in fixtures[:limit]:
        opponent = str(fixture.get("opponent", "")).strip()
        if not opponent:
            continue
        venue = "casa" if fixture.get("venue") == "home" else "fuera"
        kickoff = str(fixture.get("date", "")).strip() or str(fixture.get("kickoff", "")).strip()
        opponent_position = fixture.get("opponent_position")
        opponent_points = fixture.get("opponent_points")
        tail = []
        if opponent_position:
            tail.append(f"{opponent_position}º")
        if opponent_points is not None:
            tail.append(f"{opponent_points} pts")
        suffix = f" [{' · '.join(tail)}]" if tail else ""
        rows.append(f"{kickoff} · {venue} vs {opponent}{suffix}".strip())
    return _bullet_list_html(rows)


def _fixture_summary_deep(fixtures: list[dict], limit: int = UPCOMING_FIXTURE_WINDOW) -> str:
    parts = []
    for fixture in fixtures[:limit]:
        opponent = str(fixture.get("opponent", "")).strip()
        if not opponent:
            continue
        venue = "casa" if fixture.get("venue") == "home" else "fuera"
        opponent_position = fixture.get("opponent_position")
        round_value = fixture.get("round")
        suffix = f" ({opponent_position}Âº)" if opponent_position else ""
        if round_value:
            suffix += f" J{round_value}"
        parts.append(f"{venue} vs {opponent}{suffix}")
    return "; ".join(parts) or "sin calendario cercano detectado"


def _render_fixture_list_html_deep(fixtures: list[dict], limit: int = UPCOMING_FIXTURE_WINDOW) -> str:
    rows = []
    for fixture in fixtures[:limit]:
        opponent = str(fixture.get("opponent", "")).strip()
        if not opponent:
            continue
        venue = "casa" if fixture.get("venue") == "home" else "fuera"
        kickoff = str(fixture.get("date", "")).strip() or str(fixture.get("kickoff", "")).strip()
        opponent_position = fixture.get("opponent_position")
        opponent_points = fixture.get("opponent_points")
        round_value = fixture.get("round")
        tail = []
        if opponent_position:
            tail.append(f"{opponent_position}Âº")
        if opponent_points is not None:
            tail.append(f"{opponent_points} pts")
        if round_value:
            tail.append(f"J{round_value}")
        suffix = f" [{' Â· '.join(tail)}]" if tail else ""
        rows.append(f"{kickoff} Â· {venue} vs {opponent}{suffix}".strip())
    return _bullet_list_html(rows)


def _future_window_summary(future_difficulty: dict) -> str:
    if not future_difficulty:
        return "sin ventana futura calculada"
    hard = future_difficulty.get("hard_opponents") or []
    hard_text = ", ".join(hard[:4]) if hard else "sin rivales top-8 detectados"
    coverage_note = ""
    if _safe_int(future_difficulty.get("matches"), 0) < UPCOMING_FIXTURE_WINDOW:
        coverage_note = " (resto de temporada detectado)"
    return (
        f"{future_difficulty.get('matches', 0)} partidos, "
        f"indice {future_difficulty.get('difficulty_index', '-')}, "
        f"top-6={future_difficulty.get('top6_matches', 0)}, "
        f"top-8={future_difficulty.get('top8_matches', 0)}, "
        f"nivel {future_difficulty.get('label', '-')}, rivales duros: {hard_text}{coverage_note}"
    )


def _is_high_importance_nonleague_fixture(fixture: dict) -> bool:
    text = " ".join(
        str(fixture.get(key, "")).lower()
        for key in ["league", "stage", "round", "opponent", "source"]
    )
    return any(
        token in text
        for token in [
            "champions league",
            "uefa champions",
            "champs_league",
            "uefa_champs",
            "soccer_uefa_champs_league",
            "europa league",
            "europa_league",
            "conference league",
            "conference_league",
            "copa del rey",
            "fa cup",
            "efl cup",
            "semifinal",
            "semi-final",
            "final",
        ]
    )


def _fixture_competition_label(fixture: dict) -> str:
    raw = str(fixture.get("league", "")).strip()
    normalized = raw.lower()
    labels = {
        "soccer_uefa_champs_league": "UEFA Champions League",
        "soccer_uefa_europa_league": "UEFA Europa League",
        "soccer_uefa_europa_conference_league": "UEFA Conference League",
    }
    return labels.get(normalized, raw or "competicion europea/copera")


def _rotation_context_from_upcoming(team_name: str, fixtures: list[dict], kickoff: str, news_signals: dict) -> dict:
    kickoff_dt = _parse_iso_datetime(kickoff)
    news_rotation = _safe_int((news_signals or {}).get("rotation_count"), 0) or 0
    news_morale = _safe_int((news_signals or {}).get("morale_count"), 0) or 0
    if not kickoff_dt:
        return {
            "risk": "medium" if news_rotation else "low",
            "score": min(100, news_rotation * 18 + news_morale * 8),
            "reason": "senales de prensa" if news_rotation else "",
            "next_high_importance_fixture": {},
        }
    best_fixture = {}
    best_days = None
    for fixture in fixtures or []:
        fixture_dt = _parse_iso_datetime(str(fixture.get("kickoff", "")).strip())
        if not fixture_dt:
            continue
        days = (fixture_dt - kickoff_dt).total_seconds() / 86400.0
        if days < 0 or days > 5.0:
            continue
        if not _is_high_importance_nonleague_fixture(fixture):
            continue
        if best_days is None or days < best_days:
            best_days = days
            best_fixture = fixture
    score = min(100, news_rotation * 18 + news_morale * 8)
    reason = ""
    if best_fixture:
        if best_days is not None and best_days <= 3.5:
            score = max(score, 88)
        else:
            score = max(score, 68)
        competition = _fixture_competition_label(best_fixture)
        opponent = str(best_fixture.get("opponent", "")).strip() or "rival por confirmar"
        reason = f"{team_name} tiene {competition} vs {opponent} en {best_days:.1f} dias"
    elif news_rotation:
        reason = "la prensa reciente apunta a rotaciones/descanso"
    risk = "high" if score >= 75 else ("medium" if score >= 35 else "low")
    return {
        "risk": risk,
        "score": int(score),
        "reason": reason,
        "next_high_importance_fixture": best_fixture,
    }


def _referee_analysis_summary(referee_analysis: dict) -> str:
    if not referee_analysis:
        return "sin historico arbitral fiable"
    overall = referee_analysis.get("overall") or {}
    return (
        f"muestra {referee_analysis.get('sample_matches', 0)} partidos, "
        f"local {overall.get('home_win_pct', '-')}% vs base {overall.get('baseline_home_win_pct', '-')}"
        f"%, visitante {overall.get('away_win_pct', '-')}% vs base {overall.get('baseline_away_win_pct', '-')}"
        f"%, sesgo {overall.get('bias_label', 'neutral')}"
    )


def _competitive_context_line(team_name: str, table: dict, relegation: dict, objective: dict | None = None) -> str:
    position = table.get("position", "-")
    points = table.get("points", "-")
    gap_to_drop = relegation.get("gap_to_drop_zone")
    gap_to_safe = relegation.get("gap_to_safe_line")
    urgency = relegation.get("urgency", "")
    extras = []
    objective = objective or {}
    if objective.get("summary"):
        extras.append(str(objective.get("summary")))
    if objective.get("must_win_index"):
        extras.append(f"must-win {objective.get('must_win_index')}/100")
    if objective.get("must_not_lose_index"):
        extras.append(f"no perder {objective.get('must_not_lose_index')}/100")
    if gap_to_drop is not None:
        extras.append(f"gap descenso {gap_to_drop:+}")
    if gap_to_safe is not None and gap_to_safe > 0:
        extras.append(f"a {gap_to_safe} pts de la salvacion")
    if urgency:
        extras.append(f"urgencia {urgency}")
    suffix = ", ".join(extras) if extras else "sin alerta clasificatoria clara"
    return f"{team_name}: puesto {position}, {points} puntos, {suffix}."


def _enrich_quiniela_match(match: dict) -> None:
    match_news = fetch_match_news(match["local"], match["visitante"])
    referee_news_items = fetch_match_referee_news(match["local"], match["visitante"])
    home_focus_news = fetch_focus_team_news(match["local"])
    away_focus_news = fetch_focus_team_news(match["visitante"])
    home_media_news = fetch_local_media_news(match["local"])
    away_media_news = fetch_local_media_news(match["visitante"])
    match["home_team_context"]["focus_news"] = home_focus_news
    match["away_team_context"]["focus_news"] = away_focus_news
    match["home_team_context"]["media_news"] = home_media_news
    match["away_team_context"]["media_news"] = away_media_news

    merged_match_news_items = _clean_news_items(
        _predictive_news_items(list(match_news.get("items", [])) + list(referee_news_items)),
        MATCH_NEWS_MAX_AGE_DAYS,
        max(MATCH_NEWS_ITEMS, 8),
    )
    merged_signals = {
        "referee_count": 0,
        "injury_count": 0,
        "rotation_count": 0,
        "weather_count": 0,
    }
    for merged_item in merged_match_news_items:
        haystack = f"{merged_item.get('title', '')} {merged_item.get('source', '')}".lower()
        if any(keyword in haystack for keyword in DISCIPLINE_KEYWORDS):
            merged_signals["referee_count"] += 1
        if any(keyword in haystack for keyword in INJURY_KEYWORDS):
            merged_signals["injury_count"] += 1
        if any(keyword in haystack for keyword in ROTATION_KEYWORDS):
            merged_signals["rotation_count"] += 1
        if any(keyword in haystack for keyword in WEATHER_KEYWORDS):
            merged_signals["weather_count"] += 1
    match["match_news_context"] = {
        "items": merged_match_news_items,
        "signals": merged_signals,
    }
    match["match_signals"]["match_referee_attention"] = merged_signals.get("referee_count", 0)
    match["match_signals"]["match_injury_attention"] = merged_signals.get("injury_count", 0)
    match["match_signals"]["match_rotation_attention"] = merged_signals.get("rotation_count", 0)
    match["match_signals"]["match_weather_attention"] = merged_signals.get("weather_count", 0)

    home_team_api = fetch_the_sportsdb_team(match["local"])
    away_team_api = fetch_the_sportsdb_team(match["visitante"])
    inferred_round = max(
        int(((match.get("history_context") or {}).get("home") or {}).get("table", {}).get("played", 0) or 0),
        int(((match.get("history_context") or {}).get("away") or {}).get("table", {}).get("played", 0) or 0),
    ) + 1
    sportsdb_event = _resolve_sportsdb_event(
        match["local"],
        match["visitante"],
        match.get("kickoff", ""),
        home_team_api,
        away_team_api,
        inferred_round=inferred_round,
    ) or {
        "strHomeTeam": match["local"],
        "strAwayTeam": match["visitante"],
        "idLeague": home_team_api.get("idLeague", "") or away_team_api.get("idLeague", ""),
        "strLeague": home_team_api.get("strLeague", "") or away_team_api.get("strLeague", ""),
        "strSeason": _season_tag_for(_parse_iso_datetime(match.get("kickoff", ""))),
        "intRound": str(inferred_round),
    }

    home_profile = _repair_profile_location(
        match["local"],
        (match.get("home_team_context") or {}).get("profile", {}),
        LEAGUE_COUNTRY_HINTS.get(match.get("league", "")),
        sportsdb_event.get("strCity", ""),
        home_team_api.get("strLocation", ""),
        home_team_api.get("strStadiumLocation", ""),
        home_team_api.get("strStadium", ""),
    )
    away_profile = _repair_profile_location(
        match["visitante"],
        (match.get("away_team_context") or {}).get("profile", {}),
        LEAGUE_COUNTRY_HINTS.get(match.get("league", "")),
        away_team_api.get("strLocation", ""),
        away_team_api.get("strStadiumLocation", ""),
        away_team_api.get("strStadium", ""),
    )
    venue_profile = _repair_profile_location(
        match["local"],
        dict(home_profile),
        LEAGUE_COUNTRY_HINTS.get(match.get("league", "")),
        sportsdb_event.get("strCity", ""),
        sportsdb_event.get("strVenue", ""),
        home_team_api.get("strStadiumLocation", ""),
        home_team_api.get("strStadium", ""),
    )
    match["home_team_context"]["profile"] = home_profile
    match["away_team_context"]["profile"] = away_profile
    _cache_set(TEAM_PROFILE_CACHE, match["local"], home_profile)
    _cache_set(TEAM_PROFILE_CACHE, match["visitante"], away_profile)

    travel_distance_km = _haversine_km(
        home_profile.get("latitude"),
        home_profile.get("longitude"),
        away_profile.get("latitude"),
        away_profile.get("longitude"),
    )
    match["travel_context"] = {
        "distance_km": travel_distance_km,
        "distance_bucket": _distance_bucket(travel_distance_km),
        "home_country": home_profile.get("country", ""),
        "away_country": away_profile.get("country", ""),
        "international_trip": bool(
            home_profile.get("country_code")
            and away_profile.get("country_code")
            and home_profile.get("country_code") != away_profile.get("country_code")
        ),
    }
    weather = fetch_weather_context(venue_profile, match.get("kickoff", ""))
    match["weather_context"] = weather
    match["match_signals"]["weather_risk"] = _weather_risk(weather)
    match["match_signals"]["travel_burden_away"] = _distance_bucket(travel_distance_km)

    competition_context = match.get("competition_context") or {}
    history_context = match.get("history_context") or {}
    league_history_for_schedule = fetch_league_history(match.get("league", ""))
    kickoff_dt = _parse_iso_datetime(match.get("kickoff", ""))
    season_history_for_schedule = _season_rows(
        league_history_for_schedule,
        _season_code_for(kickoff_dt),
    )
    current_table_snapshot = _table_snapshot(
        _completed_rows_before_kickoff(season_history_for_schedule, kickoff_dt)
    )
    home_resolved_name = ((history_context.get("home") or {}).get("resolved_name")) or match["local"]
    away_resolved_name = ((history_context.get("away") or {}).get("resolved_name")) or match["visitante"]
    season_competitive_context = _season_competitive_context(
        match.get("league", ""),
        current_table_snapshot,
        home_resolved_name,
        away_resolved_name,
        kickoff_dt,
    )
    home_espn_upcoming = fetch_espn_team_fixtures(
        match["local"],
        str(home_team_api.get("idESPN", "")).strip(),
        kickoff_dt,
        current_table_snapshot,
        season_history_for_schedule,
    )
    away_espn_upcoming = fetch_espn_team_fixtures(
        match["visitante"],
        str(away_team_api.get("idESPN", "")).strip(),
        kickoff_dt,
        current_table_snapshot,
        season_history_for_schedule,
    )
    home_round_upcoming = _upcoming_round_fixtures(
        match.get("local", ""),
        kickoff_dt,
        sportsdb_event,
        current_table_snapshot,
        season_history_for_schedule,
    )
    away_round_upcoming = _upcoming_round_fixtures(
        match.get("visitante", ""),
        kickoff_dt,
        sportsdb_event,
        current_table_snapshot,
        season_history_for_schedule,
    )
    home_sportsdb_next_upcoming = _upcoming_sportsdb_next_fixtures(
        match.get("local", ""),
        str(home_team_api.get("idTeam", "")).strip(),
        kickoff_dt,
        current_table_snapshot,
        season_history_for_schedule,
    )
    away_sportsdb_next_upcoming = _upcoming_sportsdb_next_fixtures(
        match.get("visitante", ""),
        str(away_team_api.get("idTeam", "")).strip(),
        kickoff_dt,
        current_table_snapshot,
        season_history_for_schedule,
    )
    schedule_inputs = match.get("_schedule_inputs") or {}
    home_upcoming = _merge_upcoming_fixtures(
        home_sportsdb_next_upcoming,
        home_round_upcoming,
        home_espn_upcoming,
        schedule_inputs.get("home_feed_upcoming") or [],
        schedule_inputs.get("home_schedule_upcoming") or [],
        schedule_inputs.get("home_espn_upcoming") or [],
    )
    away_upcoming = _merge_upcoming_fixtures(
        away_sportsdb_next_upcoming,
        away_round_upcoming,
        away_espn_upcoming,
        schedule_inputs.get("away_feed_upcoming") or [],
        schedule_inputs.get("away_schedule_upcoming") or [],
        schedule_inputs.get("away_espn_upcoming") or [],
    )
    competition_context["home_upcoming"] = home_upcoming
    competition_context["away_upcoming"] = away_upcoming
    competition_context["home_future_difficulty"] = _future_schedule_difficulty(home_upcoming)
    competition_context["away_future_difficulty"] = _future_schedule_difficulty(away_upcoming)
    competition_context["season_context_phase"] = season_competitive_context.get("season_context_phase", {})
    competition_context["home_objective"] = season_competitive_context.get("home_objective", {})
    competition_context["away_objective"] = season_competitive_context.get("away_objective", {})
    competition_context["direct_rivalry"] = season_competitive_context.get("direct_rivalry", {})
    competition_context["competitive_stakes_label"] = season_competitive_context.get(
        "competitive_stakes_label", ""
    )
    competition_context["home_rotation_context"] = _rotation_context_from_upcoming(
        match["local"],
        home_upcoming,
        match.get("kickoff", ""),
        home_focus_news.get("signals", {}),
    )
    competition_context["away_rotation_context"] = _rotation_context_from_upcoming(
        match["visitante"],
        away_upcoming,
        match.get("kickoff", ""),
        away_focus_news.get("signals", {}),
    )
    match["competition_context"] = competition_context
    match["analytics_context"]["home_pressure_index"] = _pressure_index(
        (history_context.get("home") or {}).get("table", {}),
        competition_context.get("home_relegation") or {},
        competition_context.get("home_future_difficulty") or {},
    )
    match["analytics_context"]["away_pressure_index"] = _pressure_index(
        (history_context.get("away") or {}).get("table", {}),
        competition_context.get("away_relegation") or {},
        competition_context.get("away_future_difficulty") or {},
    )
    match["analytics_context"]["home_fatigue_index"] = _fatigue_index(
        (match.get("schedule_context") or {}).get("home", {}).get("days_since_last_match"),
        (match.get("schedule_context") or {}).get("home", {}).get("matches_last_14_days"),
        0.0,
    )
    match["analytics_context"]["away_fatigue_index"] = _fatigue_index(
        (match.get("schedule_context") or {}).get("away", {}).get("days_since_last_match"),
        (match.get("schedule_context") or {}).get("away", {}).get("matches_last_14_days"),
        travel_distance_km,
    )
    match["match_signals"]["home_pressure_index"] = (match["analytics_context"]["home_pressure_index"] or {}).get("score")
    match["match_signals"]["away_pressure_index"] = (match["analytics_context"]["away_pressure_index"] or {}).get("score")
    match["analytics_context"]["home_must_win_index"] = (
        season_competitive_context.get("home_objective", {}) or {}
    ).get("must_win_index", 0)
    match["analytics_context"]["away_must_win_index"] = (
        season_competitive_context.get("away_objective", {}) or {}
    ).get("must_win_index", 0)
    match["analytics_context"]["home_must_not_lose_index"] = (
        season_competitive_context.get("home_objective", {}) or {}
    ).get("must_not_lose_index", 0)
    match["analytics_context"]["away_must_not_lose_index"] = (
        season_competitive_context.get("away_objective", {}) or {}
    ).get("must_not_lose_index", 0)
    match["analytics_context"]["direct_rivalry_index"] = (
        season_competitive_context.get("direct_rivalry", {}) or {}
    ).get("direct_rivalry_index", 0)
    match["analytics_context"]["home_objective_swing_if_win"] = (
        season_competitive_context.get("home_objective", {}) or {}
    ).get("objective_swing_if_win", 0)
    match["analytics_context"]["home_objective_swing_if_lose"] = (
        season_competitive_context.get("home_objective", {}) or {}
    ).get("objective_swing_if_lose", 0)
    match["analytics_context"]["away_objective_swing_if_win"] = (
        season_competitive_context.get("away_objective", {}) or {}
    ).get("objective_swing_if_win", 0)
    match["analytics_context"]["away_objective_swing_if_lose"] = (
        season_competitive_context.get("away_objective", {}) or {}
    ).get("objective_swing_if_lose", 0)
    match["analytics_context"]["home_rotation_risk_index"] = (
        competition_context.get("home_rotation_context") or {}
    ).get("score", 0)
    match["analytics_context"]["away_rotation_risk_index"] = (
        competition_context.get("away_rotation_context") or {}
    ).get("score", 0)
    match["match_signals"]["home_must_win_index"] = match["analytics_context"]["home_must_win_index"]
    match["match_signals"]["away_must_win_index"] = match["analytics_context"]["away_must_win_index"]
    match["match_signals"]["home_must_not_lose_index"] = match["analytics_context"]["home_must_not_lose_index"]
    match["match_signals"]["away_must_not_lose_index"] = match["analytics_context"]["away_must_not_lose_index"]
    match["match_signals"]["direct_rivalry_index"] = match["analytics_context"]["direct_rivalry_index"]
    match["match_signals"]["season_context_phase"] = (
        season_competitive_context.get("season_context_phase", {}) or {}
    ).get("key", "")
    match["match_signals"]["competitive_stakes_label"] = season_competitive_context.get(
        "competitive_stakes_label", ""
    )
    match["match_signals"]["home_rotation_risk_index"] = match["analytics_context"]["home_rotation_risk_index"]
    match["match_signals"]["away_rotation_risk_index"] = match["analytics_context"]["away_rotation_risk_index"]
    match["match_signals"]["home_fatigue_index"] = (match["analytics_context"]["home_fatigue_index"] or {}).get("score")
    match["match_signals"]["away_fatigue_index"] = (match["analytics_context"]["away_fatigue_index"] or {}).get("score")
    match["_schedule_inputs"] = {
        **schedule_inputs,
        "home_round_upcoming": home_round_upcoming,
        "away_round_upcoming": away_round_upcoming,
        "home_sportsdb_next_upcoming": home_sportsdb_next_upcoming,
        "away_sportsdb_next_upcoming": away_sportsdb_next_upcoming,
        "home_espn_upcoming": home_espn_upcoming,
        "away_espn_upcoming": away_espn_upcoming,
    }

    home_official = fetch_official_site_headlines(match["local"], home_team_api)
    away_official = fetch_official_site_headlines(match["visitante"], away_team_api)
    match["home_team_context"]["official_site"] = home_official
    match["away_team_context"]["official_site"] = away_official

    referee_context = _extract_referee_assignment(
        match.get("league", ""),
        match.get("kickoff", ""),
        match.get("local", ""),
        match.get("visitante", ""),
        merged_match_news_items,
        sportsdb_event,
    )
    referee_context["season_analysis"] = _referee_season_analysis(
        match.get("league", ""),
        sportsdb_event,
        referee_context.get("assigned_referee", ""),
        match.get("local", ""),
        match.get("visitante", ""),
        league_history_for_schedule,
    )
    home_injuries = _build_injury_entities(match["local"], match["home_team_context"].get("news", []))
    away_injuries = _build_injury_entities(match["visitante"], match["away_team_context"].get("news", []))
    structured_context = {
        "match_key": match.get("match_key")
        or _match_key(
            match.get("league", ""),
            match.get("local", ""),
            match.get("visitante", ""),
            match.get("kickoff", ""),
        ),
        "event_context": {
            "sportsdb_event_id": sportsdb_event.get("idEvent", ""),
            "sportsdb_home_team_id": home_team_api.get("idTeam", ""),
            "sportsdb_away_team_id": away_team_api.get("idTeam", ""),
            "venue": sportsdb_event.get("strVenue", "") or home_team_api.get("strStadium", ""),
            "stadium_city": sportsdb_event.get("strCity", "") or home_team_api.get("strLocation", ""),
            "league": sportsdb_event.get("strLeague", ""),
            "round": sportsdb_event.get("intRound", ""),
            "status": sportsdb_event.get("strStatus", ""),
        },
        "referee_context": referee_context,
        "injury_context": {
            "home_team": {
                "team": match["local"],
                "items": home_injuries,
                "count": len(home_injuries),
            },
            "away_team": {
                "team": match["visitante"],
                "items": away_injuries,
                "count": len(away_injuries),
            },
        },
        "updated_at": _now_iso(),
    }
    match["structured_context"] = structured_context
    match["match_signals"]["structured_home_injuries"] = len(home_injuries)
    match["match_signals"]["structured_away_injuries"] = len(away_injuries)
    match["match_signals"]["structured_referee_known"] = bool(
        referee_context.get("assigned_referee")
    )
    match["focus_digest"] = _focus_match_digest(match)
    match["focus_ai_briefing"] = _focus_match_ai_briefing(match)


def _describe_form(form: str) -> str:
    if not form or form == "-":
        return "sin datos de forma reciente"
    wins = form.count("W")
    losses = form.count("L")
    total = wins + form.count("D") + losses
    if total == 0:
        return "sin datos de forma reciente"
    if wins >= 3:
        return "Buena dinámica reciente"
    if losses >= 3:
        return "Mala racha reciente"
    if wins >= 2 and losses <= 1:
        return "Dinámica positiva"
    if losses >= 2 and wins <= 1:
        return "Dinámica negativa"
    return "Trayectoria irregular"


def _insight_mercado(market: dict, quiniela_pct: dict, home: str, away: str) -> str:
    m1 = _safe_float(market.get("1"))
    mX = _safe_float(market.get("X"))
    m2 = _safe_float(market.get("2"))
    q1 = _safe_float(quiniela_pct.get("1"))
    q2 = _safe_float(quiniela_pct.get("2"))
    if m1 is None or m2 is None:
        return "Sin suficientes datos de mercado para generar insight."
    if m1 > m2 and m1 > (mX or 0):
        fav_label, fav_market, fav_quiniela = "local", m1, q1
    elif m2 > m1 and m2 > (mX or 0):
        fav_label, fav_market, fav_quiniela = "visitante", m2, q2
    else:
        return (
            f"El mercado apunta a un partido muy equilibrado "
            f"(1={m1:.1f}%, X={mX:.1f}%, 2={m2:.1f}%). El empate tiene valor quinielístico."
        )
    if fav_quiniela is None:
        return f"El {fav_label} es favorito para las casas de apuestas ({fav_market:.1f}%)."
    gap = round(fav_quiniela - fav_market, 1)
    if gap > 8:
        return (
            f"El {fav_label} es favorito para las casas de apuestas ({fav_market:.1f}%), "
            f"pero el público de la quiniela lo sobrevalora aún más ({fav_quiniela:.1f}%). "
            "Hay valor matemático en buscar la sorpresa o el empate."
        )
    if gap < -8:
        return (
            f"El {fav_label} es favorito para las casas de apuestas ({fav_market:.1f}%), "
            f"pero el público quinielista lo infravalora ({fav_quiniela:.1f}%). "
            "El favorito podría estar infravalorado en la quiniela."
        )
    return (
        f"El {fav_label} es favorito para las casas de apuestas ({fav_market:.1f}%) "
        f"y el público quinielista lo refleja de forma similar ({fav_quiniela:.1f}%). "
        "Sin gran divergencia entre mercado y quiniela."
    )


def _insight_calendar(
    team_name: str, future_difficulty: dict, relegation: dict, table: dict, objective: dict | None = None
) -> str:
    parts = []
    objective = objective or {}
    if objective.get("summary"):
        phase_label = ((objective.get("season_context_phase") or {}).get("label") or "").strip()
        swing_win = str(objective.get("swing_summary_if_win", "")).strip()
        swing_lose = str(objective.get("swing_summary_if_lose", "")).strip()
        objective_text = f"{team_name} {objective.get('summary')}"
        if phase_label:
            objective_text += f" en {phase_label}"
        objective_text += (
            f" (must-win {objective.get('must_win_index', 0)}/100, "
            f"no perder {objective.get('must_not_lose_index', 0)}/100)."
        )
        parts.append(objective_text)
        if swing_win and swing_win != "impacto material bajo":
            parts.append(swing_win.capitalize() + ".")
        if swing_lose and swing_lose != "impacto material bajo":
            parts.append(swing_lose.capitalize() + ".")
    label = str(future_difficulty.get("label", "")).strip().lower()
    top6 = _safe_int(future_difficulty.get("top6_matches"), 0)
    hard_opponents = future_difficulty.get("hard_opponents") or []
    urgency = str(relegation.get("urgency", "")).strip().lower()
    gap_to_drop = relegation.get("gap_to_drop_zone")
    gap_to_safe = relegation.get("gap_to_safe_line")
    if objective and objective.get("objective_key") != "survival":
        urgency = ""
        gap_to_drop = None
        gap_to_safe = None
    if label in ("hard", "very hard", "alta", "muy alta"):
        opponents_text = f" ({', '.join(hard_opponents[:3])})" if hard_opponents else ""
        parts.append(
            f"Calendario futuro exigente con {top6} rivales del top-6{opponents_text}. "
            "Posibles rotaciones hoy para reservar jugadores clave."
        )
    elif label in ("easy", "low", "baja"):
        parts.append("Calendario próximo equilibrado, sin presión de conservar fuerzas.")
    else:
        if top6 > 0:
            parts.append(f"Calendario futuro con {top6} rivales del top-6.")
        else:
            parts.append("Calendario próximo sin rivales especialmente exigentes.")
    if urgency in ("high", "critical", "alta", "crítica"):
        if gap_to_safe is not None and gap_to_safe > 0:
            parts.append(
                f"Urgencia clasificatoria alta, necesitan {gap_to_safe} puntos para la salvación. "
                "Juegan con la presión del descenso."
            )
        else:
            parts.append("Situación clasificatoria muy delicada, partido de alto voltaje emocional.")
    elif urgency in ("medium", "media"):
        parts.append("Cierta tensión clasificatoria, aunque no en zona crítica.")
    elif gap_to_drop is not None:
        parts.append(
            f"Nivel de urgencia bajo ({gap_to_drop:+} puntos sobre el descenso), "
            "juegan sin presión extrema."
        )
    return " ".join(parts) if parts else "Sin datos clasificatorios suficientes."


def _insight_fatigue_and_rest(
    home_fatigue: dict, away_fatigue: dict,
    home_schedule: dict, away_schedule: dict,
) -> str:
    home_label = str(home_fatigue.get("label", "")).strip().lower()
    away_label = str(away_fatigue.get("label", "")).strip().lower()
    home_days = home_schedule.get("days_since_last_match")
    away_days = away_schedule.get("days_since_last_match")
    parts = []
    if home_label and away_label:
        if home_label == away_label:
            parts.append(f"Ambos equipos llegan con una carga física similar ({home_label.capitalize()}).")
        elif home_label in ("high", "alta") and away_label not in ("high", "alta"):
            parts.append(
                f"El local llega con mayor carga física ({home_label}) "
                f"frente al visitante ({away_label}). Podría notar el cansancio."
            )
        elif away_label in ("high", "alta") and home_label not in ("high", "alta"):
            parts.append(
                f"El visitante llega con mayor carga física ({away_label}) "
                f"frente al local ({home_label}). Ventaja física para el equipo local."
            )
        else:
            parts.append(f"Carga física: local {home_label}, visitante {away_label}.")
    if home_days is not None and away_days is not None:
        try:
            h, a = int(home_days), int(away_days)
            if abs(h - a) >= 2:
                if h > a:
                    parts.append(f"El local descansó {h} días vs {a} días del visitante. Ligera ventaja física para el local.")
                else:
                    parts.append(f"El visitante descansó {a} días vs {h} días del local. Ligera ventaja física para el visitante.")
            else:
                parts.append(f"Tiempo de descanso similar ({h} vs {a} días).")
        except (TypeError, ValueError):
            pass
    return " ".join(parts) if parts else "Sin datos de fatiga y descanso disponibles."


def _insight_weather(weather: dict) -> str:
    temp = _safe_float(weather.get("temperature_c"))
    precip = _safe_float(weather.get("precipitation_probability"))
    wind = _safe_float(weather.get("wind_speed_kmh"))
    if temp is None:
        return "Sin datos meteorológicos disponibles."
    conditions = []
    if 15 <= temp <= 25:
        conditions.append(f"Temperatura ideal ({temp:.0f}ºC)")
    elif temp < 8:
        conditions.append(f"Frío notable ({temp:.0f}ºC), posible impacto en el ritmo de juego")
    elif temp > 30:
        conditions.append(f"Calor intenso ({temp:.0f}ºC), favorece a equipos con mayor fondo físico")
    else:
        conditions.append(f"Temperatura {temp:.0f}ºC")
    if precip is not None:
        if precip >= 60:
            conditions.append("alta probabilidad de lluvia que puede alterar el juego técnico")
        elif precip >= 30:
            conditions.append("posibilidad de lluvia moderada")
        else:
            conditions.append("sin riesgo de lluvia")
    if wind is not None:
        if wind >= 40:
            conditions.append(f"viento fuerte ({wind:.0f} km/h) que perjudica el juego combinativo")
        elif wind >= 20:
            conditions.append(f"viento moderado ({wind:.0f} km/h)")
        else:
            conditions.append("sin viento significativo")
    base = ", ".join(conditions[:2]) if conditions else "condiciones normales"
    wind_part = conditions[2] if len(conditions) > 2 else ""
    if wind_part:
        return f"{base}, {wind_part}."
    if "ideal" in base and "lluvia" not in base:
        return f"{base}. Condiciones perfectas para el juego técnico y los goles."
    return f"{base}."


def _insight_travel(travel: dict) -> str:
    km = _safe_float(travel.get("distance_km"))
    international = travel.get("international_trip", False)
    if km is None:
        return "Sin datos de desplazamiento disponibles."
    if international:
        return f"Desplazamiento internacional ({km:.0f} km). Impacto significativo en la preparación del visitante."
    if km < 100:
        return f"Desplazamiento corto para el visitante ({km:.0f} km). Impacto nulo."
    if km < 300:
        return f"Desplazamiento moderado para el visitante ({km:.0f} km). Impacto mínimo."
    if km < 600:
        return f"Viaje notable para el visitante ({km:.0f} km). Leve impacto físico."
    return f"Viaje largo para el visitante ({km:.0f} km). Desgaste adicional a considerar."


def _focus_match_ai_briefing(match: dict) -> dict:
    market = (match.get("market_context") or {}).get("normalized_percent", {})
    weather = match.get("weather_context") or {}
    travel = match.get("travel_context") or {}
    history = match.get("history_context") or {}
    schedule = match.get("schedule_context") or {}
    competition = match.get("competition_context") or {}
    analytics = match.get("analytics_context") or {}
    structured = match.get("structured_context") or {}
    referee_context = structured.get("referee_context") or {}
    referee_analysis = referee_context.get("season_analysis") or {}
    injury_context = structured.get("injury_context") or {}
    home_injuries = (injury_context.get("home_team") or {}).get("items", [])
    away_injuries = (injury_context.get("away_team") or {}).get("items", [])

    home = match.get("local", "")
    away = match.get("visitante", "")

    home_recent = (history.get("home") or {}).get("recent_all") or {}
    away_recent = (history.get("away") or {}).get("recent_all") or {}
    home_table = (history.get("home") or {}).get("table") or {}
    away_table = (history.get("away") or {}).get("table") or {}
    h2h = history.get("head_to_head") or {}

    home_relegation = competition.get("home_relegation") or {}
    away_relegation = competition.get("away_relegation") or {}
    home_objective = competition.get("home_objective") or {}
    away_objective = competition.get("away_objective") or {}
    direct_rivalry = competition.get("direct_rivalry") or {}
    season_phase = competition.get("season_context_phase") or {}
    stakes_label = competition.get("competitive_stakes_label", "")
    home_rotation_context = competition.get("home_rotation_context") or {}
    away_rotation_context = competition.get("away_rotation_context") or {}
    home_future_difficulty = competition.get("home_future_difficulty") or {}
    away_future_difficulty = competition.get("away_future_difficulty") or {}
    home_fatigue = analytics.get("home_fatigue_index") or {}
    away_fatigue = analytics.get("away_fatigue_index") or {}

    quiniela_pct: dict = match.get("official_quiniela_percentages") or {}
    if not quiniela_pct:
        for slot in match.get("quiniela_slots") or []:
            quiniela_pct = (
                (slot.get("percentages") or {}).get("quinielista")
                or (slot.get("percentages") or {}).get("lae")
                or {}
            )
            if quiniela_pct:
                break

    def _fmt_pct(val: object) -> str:
        if val is None:
            return "-"
        try:
            return f"{float(val):.2f}%"
        except (TypeError, ValueError):
            return str(val)

    cuotas_str = (
        f"1={_fmt_pct(market.get('1'))}, "
        f"X={_fmt_pct(market.get('X'))}, "
        f"2={_fmt_pct(market.get('2'))}"
    )
    quiniela_str = (
        f"1={_fmt_pct(quiniela_pct.get('1'))}, "
        f"X={_fmt_pct(quiniela_pct.get('X'))}, "
        f"2={_fmt_pct(quiniela_pct.get('2'))}"
    ) if quiniela_pct else "Sin datos oficiales disponibles"

    referee_name = referee_context.get("assigned_referee", "") or "No confirmado"
    referee_bias = _referee_analysis_summary(referee_analysis) if referee_analysis else "Sin histórico arbitral fiable"

    home_injury_names = [i.get("player", "") for i in home_injuries[:4] if i.get("player")]
    away_injury_names = [i.get("player", "") for i in away_injuries[:4] if i.get("player")]
    home_injury_text = (
        f"{len(home_injuries)} baja(s) detectada(s)"
        + (f": {', '.join(home_injury_names)}" if home_injury_names else "")
        + "."
    ) if home_injuries else "Sin bajas confirmadas."
    away_injury_text = (
        f"{len(away_injuries)} baja(s) detectada(s)"
        + (f": {', '.join(away_injury_names)}" if away_injury_names else "")
        + "."
    ) if away_injuries else "Sin bajas confirmadas."

    h2h_text = (
        f"{h2h.get('meetings', 0)} encuentros históricos: "
        f"gana {home} en {h2h.get('home_team_wins', 0)}, "
        f"empates {h2h.get('draws', 0)}, "
        f"gana {away} en {h2h.get('away_team_wins', 0)} "
        f"(ventana de {h2h.get('years_span', 0)} años)."
    ) if h2h.get("meetings") else "Sin datos de enfrentamientos directos disponibles."

    home_form = str(home_recent.get("form", "-"))
    away_form = str(away_recent.get("form", "-"))
    competitive_summary = stakes_label or "Sin contexto competitivo material calculable."
    if direct_rivalry.get("is_direct_rivalry"):
        competitive_summary = (
            f"{direct_rivalry.get('label')}: equipos separados por "
            f"{direct_rivalry.get('points_delta')} pts y "
            f"{direct_rivalry.get('position_delta')} puestos."
        )
    rotation_parts = []
    for side, rotation in [("local", home_rotation_context), ("visitante", away_rotation_context)]:
        if rotation.get("risk") in {"high", "medium"} and rotation.get("reason"):
            rotation_parts.append(f"{side}: {rotation.get('reason')} (riesgo {rotation.get('risk')})")
    rotation_summary = " | ".join(rotation_parts) or "Sin senal fuerte de rotacion por calendario externo."

    return {
        "partido": f"{home} vs {away}",
        "mercado_y_probabilidades": {
            "cuotas_1X2": cuotas_str,
            "tendencia_quinielista": quiniela_str,
            "insight_mercado": _insight_mercado(market, quiniela_pct, home, away),
        },
        "contexto_deportivo": {
            "racha_local": f"{home_form} ({_describe_form(home_form)})",
            "racha_visitante": f"{away_form} ({_describe_form(away_form)})",
            "fase_temporada": season_phase.get("label", ""),
            "contexto_competitivo": competitive_summary,
            "objetivo_local": home_objective.get("summary", ""),
            "objetivo_visitante": away_objective.get("summary", ""),
            "riesgo_rotacion_competitiva": rotation_summary,
            "analisis_calendario_local": _insight_calendar(
                home, home_future_difficulty, home_relegation, home_table, home_objective
            ),
            "analisis_calendario_visitante": _insight_calendar(
                away, away_future_difficulty, away_relegation, away_table, away_objective
            ),
            "h2h_resumen": h2h_text,
        },
        "contexto_competitivo_avanzado": {
            "season_context_phase": season_phase,
            "competitive_stakes_label": stakes_label,
            "direct_rivalry": direct_rivalry,
            "home_objective": home_objective,
            "away_objective": away_objective,
            "home_rotation_context": home_rotation_context,
            "away_rotation_context": away_rotation_context,
        },
        "factores_externos": {
            "fatiga_y_descanso": _insight_fatigue_and_rest(
                home_fatigue, away_fatigue,
                schedule.get("home") or {},
                schedule.get("away") or {},
            ),
            "clima_e_impacto": _insight_weather(weather),
            "viaje": _insight_travel(travel),
            "arbitro": f"{referee_name}. {referee_bias}",
            "lesiones": {
                "local": home_injury_text,
                "visitante": away_injury_text,
            },
        },
    }


def _best_h2h(bookmakers: list, home_team: str, away_team: str) -> tuple[dict, str]:
    best = {}
    book_name = ""
    for book in bookmakers or []:
        markets = book.get("markets") or []
        for market in markets:
            if market.get("key") != "h2h":
                continue
            outcomes = market.get("outcomes") or []
            current = {}
            for outcome in outcomes:
                name = str(outcome.get("name", "")).strip()
                price = outcome.get("price")
                if name and price is not None:
                    current[name] = price
            if home_team in current and away_team in current:
                best = current
                book_name = str(book.get("title", "")).strip()
                return best, book_name
    return best, book_name


def fetch_repo_odds() -> list:
    if not DATA_URL:
        raise RuntimeError("QUINIAI_DATA_URL no configurada")
    data = _request_json(DATA_URL, timeout=30)
    if not isinstance(data, list):
        raise RuntimeError("El origen de cuotas no devolvio una lista valida")
    return data


def _team_country_hints(raw_matches: list) -> dict:
    hints = {}
    for item in raw_matches:
        country_hint = LEAGUE_COUNTRY_HINTS.get(_canonical_league_key(item.get("sport_key", "")))
        for team_name in [item.get("home_team", ""), item.get("away_team", "")]:
            team_name = str(team_name).strip()
            if team_name and country_hint and team_name not in hints:
                hints[team_name] = country_hint
    return hints


def _enrich_team(team_name: str, country_hint: str | None) -> dict:
    return {
        "profile": fetch_team_profile(team_name, country_hint),
        "news": fetch_team_news(team_name),
    }


def _focus_sort_key(match: dict) -> tuple:
    kickoff_dt = _parse_iso_datetime(match.get("kickoff", "")) or datetime.max.replace(
        tzinfo=timezone.utc
    )
    return (
        LEAGUE_PRIORITY.get(match.get("league", ""), 99),
        kickoff_dt,
        match.get("local", ""),
        match.get("visitante", ""),
    )


def _match_similarity(home_a: str, away_a: str, match: dict) -> float:
    return _team_similarity_score(home_a, match.get("local", "")) + _team_similarity_score(
        away_a, match.get("visitante", "")
    )


def _match_similarity_breakdown(home_team: str, away_team: str, match: dict) -> tuple[float, float, float]:
    home_score = _team_similarity_score(home_team, match.get("local", ""))
    away_score = _team_similarity_score(away_team, match.get("visitante", ""))
    return home_score, away_score, home_score + away_score


def _is_confident_slot_match(home_team: str, away_team: str, match: dict) -> bool:
    home_score, away_score, total_score = _match_similarity_breakdown(home_team, away_team, match)
    return home_score >= 0.7 and away_score >= 0.7 and total_score >= 1.55


def _find_match_by_teams(matches: list[dict], home_team: str, away_team: str) -> dict | None:
    best_match = None
    best_score = 0.0
    for match in matches:
        home_score, away_score, score = _match_similarity_breakdown(home_team, away_team, match)
        if home_score < 0.7 or away_score < 0.7:
            continue
        if score > best_score:
            best_score = score
            best_match = match
    return best_match if best_match and best_score >= 1.55 else None


def _preferred_quiniela_percentages(slot: dict) -> dict:
    percentages = slot.get("percentages") or {}
    return (percentages.get("quinielista") or percentages.get("lae") or {}).copy()


def _match_richness_score(match: dict) -> int:
    score = 0
    if match.get("league"):
        score += 4
    if match.get("kickoff"):
        score += 2
    if match.get("focus_ai_briefing"):
        score += 2
    if ((match.get("structured_context") or {}).get("referee_context") or {}).get("assigned_referee"):
        score += 2
    if (((match.get("history_context") or {}).get("home") or {}).get("table") or {}).get("position"):
        score += 2
    if (match.get("competition_context") or {}).get("home_upcoming"):
        score += 1
    if match.get("odds"):
        score += 1
    return score


def _apply_quiniela_slot(match: dict, jornada: int, slot: dict) -> None:
    slot_entry = {
        "jornada": jornada,
        "position": slot.get("position"),
        "pleno15": bool(slot.get("pleno15")),
        "source": "Eduardo Losilla",
        "percentages": slot.get("percentages", {}),
    }
    slots = match.setdefault("quiniela_slots", [])
    if not any(
        current.get("jornada") == slot_entry.get("jornada")
        and current.get("position") == slot_entry.get("position")
        for current in slots
    ):
        slots.append(slot_entry)
    preferred = _preferred_quiniela_percentages(slot)
    if preferred:
        match["official_quiniela_percentages"] = preferred
        match.setdefault("market_context", {}).setdefault(
            "official_percent", preferred.copy()
        )


def _find_cached_quiniela_match(
    jornada: int,
    position: int,
    slot_local: str = "",
    slot_visitante: str = "",
) -> dict | None:
    candidates = []
    jornada_record = ((QUINIELA_HISTORY or {}).get("jornadas") or {}).get(str(jornada)) or {}
    for cached_match in jornada_record.get("matches", []):
        for slot in cached_match.get("quiniela_slots") or []:
            if slot.get("jornada") == jornada and slot.get("position") == position:
                candidates.append(cached_match)
    for legacy_jornada in (LEGACY_SNAPSHOT or {}).get("quiniela_jornadas", []):
        if legacy_jornada.get("jornada") != jornada:
            continue
        for cached_match in legacy_jornada.get("matches", []):
            for slot in cached_match.get("quiniela_slots") or []:
                if slot.get("jornada") == jornada and slot.get("position") == position:
                    candidates.append(cached_match)
    if not candidates:
        return None
    ranked = []
    for candidate in candidates:
        home_score, away_score, total_score = _match_similarity_breakdown(
            slot_local or candidate.get("local", ""),
            slot_visitante or candidate.get("visitante", ""),
            candidate,
        )
        ranked.append(
            (
                1 if _is_confident_slot_match(slot_local or candidate.get("local", ""), slot_visitante or candidate.get("visitante", ""), candidate) else 0,
                total_score,
                _match_richness_score(candidate),
                candidate,
            )
        )
    ranked.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    best = ranked[0][3]
    if slot_local and slot_visitante and not _is_confident_slot_match(slot_local, slot_visitante, best):
        return None
    return _json_clone(best)


def _build_quiniela_placeholder(slot: dict, jornada: int, cached_match: dict | None = None) -> dict:
    if cached_match:
        placeholder = cached_match
    else:
        placeholder = {
            "local": slot.get("local", ""),
            "visitante": slot.get("visitante", ""),
            "league": "",
            "kickoff": "",
            "bookmaker": "",
            "odds": {},
            "market_context": {"normalized_percent": {}},
            "weather_context": {},
            "travel_context": {},
            "schedule_context": {},
            "history_context": {},
            "competition_context": {},
            "analytics_context": {},
            "structured_context": {},
            "home_team_context": {},
            "away_team_context": {},
            "match_news_context": {"items": [], "signals": {}},
            "match_signals": {},
            "focus_digest": [],
            "focus_ai_briefing": {},
            "notes": [],
        }
    placeholder.setdefault("local", slot.get("local", ""))
    placeholder.setdefault("visitante", slot.get("visitante", ""))
    placeholder["match_key"] = placeholder.get("match_key") or _match_key(
        "quiniela_cache",
        placeholder.get("local", ""),
        placeholder.get("visitante", ""),
        placeholder.get("kickoff", "") or f"j{jornada}-{slot.get('position')}",
    )
    placeholder["quiniela_tracked"] = True
    placeholder["quiniela_focus"] = False
    _apply_quiniela_slot(placeholder, jornada, slot)
    preferred = _preferred_quiniela_percentages(slot)
    if preferred:
        placeholder.setdefault("market_context", {})["normalized_percent"] = preferred.copy()
    placeholder.setdefault("notes", []).append("source=eduardo-history")
    return placeholder


def _merge_upcoming_slot_metadata(slots: list[dict], upcoming_payload: dict | None = None) -> list[dict]:
    if not slots:
        return []
    upcoming_by_position = {
        _safe_int(item.get("position")): item
        for item in list((upcoming_payload or {}).get("matches", []))
        + ([((upcoming_payload or {}).get("pleno15") or {})] if (upcoming_payload or {}).get("pleno15") else [])
        if _safe_int(item.get("position"))
    }
    merged = []
    for slot in slots:
        position = _safe_int(slot.get("position"))
        enriched = dict(slot)
        extra = upcoming_by_position.get(position) or {}
        for field in ["kickoff", "date_label", "local", "visitante"]:
            if enriched.get(field) in {None, ""} and extra.get(field) not in {None, ""}:
                enriched[field] = extra.get(field)
        merged.append(enriched)
    return merged


def _persist_quiniela_history(quiniela_jornadas: list[dict]) -> None:
    jornadas_store = QUINIELA_HISTORY.setdefault("jornadas", {})
    QUINIELA_HISTORY["updated_at"] = _now_iso()
    if quiniela_jornadas:
        QUINIELA_HISTORY["current_jornada"] = next(
            (jornada.get("jornada") for jornada in quiniela_jornadas if jornada.get("is_current")),
            quiniela_jornadas[0].get("jornada"),
        )
    keep_jornadas = set()
    for jornada in quiniela_jornadas:
        jornada_num = _safe_int(jornada.get("jornada"))
        if not jornada_num:
            continue
        keep_jornadas.add(jornada_num)
        jornadas_store[str(jornada_num)] = {
            "jornada": jornada_num,
            "label": jornada.get("label") or f"Jornada {jornada_num}",
            "source": jornada.get("source", ""),
            "source_url": jornada.get("source_url", ""),
            "kickoff_from": jornada.get("kickoff_from", ""),
            "kickoff_to": jornada.get("kickoff_to", ""),
            "updated_at": _now_iso(),
            "matches": [_json_clone(match) for match in jornada.get("matches", [])],
            "unmatched_slots": _json_clone(jornada.get("unmatched_slots", [])),
        }
    for jornada_key in list(jornadas_store.keys()):
        if _safe_int(jornada_key, 0) not in keep_jornadas:
            jornadas_store.pop(jornada_key, None)


def _audit_quiniela_integrity(
    quiniela_jornadas: list[dict], season_value: int | None = None
) -> dict:
    report = {
        "ok": True,
        "checked_jornadas": 0,
        "checked_slots": 0,
        "exact_matches": 0,
        "mismatches": [],
    }
    for jornada in quiniela_jornadas:
        jornada_num = _safe_int(jornada.get("jornada"))
        if not jornada_num:
            continue
        official_payload = fetch_quiniela_jornada_page(jornada_num, temporada=season_value)
        if not official_payload.get("ok"):
            continue
        report["checked_jornadas"] += 1
        official_slots = list(official_payload.get("matches", []))
        pleno15 = official_payload.get("pleno15") or {}
        if pleno15:
            official_slots.append(pleno15)
        resolved_by_position = {}
        for match in jornada.get("matches", []):
            for slot in match.get("quiniela_slots") or []:
                if slot.get("jornada") != jornada_num:
                    continue
                resolved_by_position[_safe_int(slot.get("position"))] = match
        for slot in official_slots:
            position = _safe_int(slot.get("position"))
            if not position:
                continue
            report["checked_slots"] += 1
            resolved_match = resolved_by_position.get(position)
            if resolved_match and _is_confident_slot_match(
                slot.get("local", ""),
                slot.get("visitante", ""),
                resolved_match,
            ):
                report["exact_matches"] += 1
                continue
            mismatch = {
                "jornada": jornada_num,
                "position": position,
                "official_local": slot.get("local", ""),
                "official_visitante": slot.get("visitante", ""),
                "resolved_local": resolved_match.get("local", "") if resolved_match else "",
                "resolved_visitante": resolved_match.get("visitante", "") if resolved_match else "",
                "resolved_kickoff": resolved_match.get("kickoff", "") if resolved_match else "",
                "resolved_league": resolved_match.get("league", "") if resolved_match else "",
            }
            report["mismatches"].append(mismatch)
        if report["mismatches"]:
            report["ok"] = False
    report["mismatch_count"] = len(report["mismatches"])
    return report


def build_quiniela_jornadas(matches: list[dict]) -> tuple[list[dict], set[str], set[str]]:
    current_context = _eduardo_current_context()
    current_jornada = _safe_int(current_context.get("jornada"))
    current_season = _safe_int(current_context.get("temporada"))
    upcoming_jornadas = fetch_eduardo_upcoming_jornadas()
    upcoming_map = {
        _safe_int(jornada.get("jornada")): jornada
        for jornada in upcoming_jornadas
        if _safe_int(jornada.get("jornada"))
    }
    if not current_jornada or not current_season:
        return [], set(), set()
    QUINIELA_HISTORY["current_jornada"] = current_jornada
    QUINIELA_HISTORY["season"] = current_season
    jornadas = []
    all_keys = set()
    current_keys = set()
    latest_available_jornada = max([current_jornada] + list(upcoming_map.keys()))
    first_jornada = max(1, latest_available_jornada - QUINIELA_HISTORY_JORNADAS + 1)
    target_jornadas = list(range(first_jornada, latest_available_jornada + 1))
    for jornada_num in reversed(target_jornadas):
        payload = fetch_quiniela_jornada_page(jornada_num, temporada=current_season)
        upcoming_payload = upcoming_map.get(jornada_num) or {}
        history_only = False
        if not payload.get("ok"):
            if upcoming_payload.get("matches"):
                payload = {
                    "ok": True,
                    "source": "Eduardo Losilla Proximas",
                    "url": EDUARDO_QUINIELA_PROXIMAS_URL,
                    "jornada": jornada_num,
                    "season": current_season,
                    "matches": list(upcoming_payload.get("matches", [])),
                    "pleno15": dict(upcoming_payload.get("pleno15") or {}),
                }
        history_record = ((QUINIELA_HISTORY or {}).get("jornadas") or {}).get(str(jornada_num)) or {}
        if not payload.get("ok") and not history_record.get("matches"):
            continue
        if not payload.get("ok"):
            payload = {
                "ok": True,
                "source": history_record.get("source") or "Eduardo Losilla Quinielista",
                "url": history_record.get("source_url") or EDUARDO_QUINIELA_PORCENTAJES_URL,
                "jornada": jornada_num,
                "season": current_season,
                "matches": history_record.get("unmatched_slots", [])[:14],
                "pleno15": next(
                    (slot for slot in history_record.get("unmatched_slots", []) if slot.get("position") == 15),
                    {},
                ),
            }
            history_only = True
        if not payload.get("ok"):
            continue
        jornada_matches = []
        unmatched_slots = []
        slots = _merge_upcoming_slot_metadata(list(payload.get("matches", [])), upcoming_payload)
        pleno_slot = dict(payload.get("pleno15") or {})
        upcoming_pleno = dict(upcoming_payload.get("pleno15") or {})
        for field in ["kickoff", "date_label", "local", "visitante"]:
            if pleno_slot.get(field) in {None, ""} and upcoming_pleno.get(field) not in {None, ""}:
                pleno_slot[field] = upcoming_pleno.get(field)
        if pleno_slot:
            slots.append(pleno_slot)
        for slot in slots:
            position = _safe_int(slot.get("position"))
            if not position:
                continue
            match = _find_match_by_teams(matches, slot.get("local", ""), slot.get("visitante", ""))
            if not match:
                cached_match = _find_cached_quiniela_match(
                    jornada_num,
                    position,
                    slot_local=slot.get("local", ""),
                    slot_visitante=slot.get("visitante", ""),
                )
                placeholder = _build_quiniela_placeholder(slot, jornada_num, cached_match=cached_match)
                if slot.get("kickoff") and not placeholder.get("kickoff"):
                    placeholder["kickoff"] = slot.get("kickoff", "")
                jornada_matches.append(placeholder)
                if not cached_match:
                    unmatched_slots.append(dict(slot))
                continue
            slot["pleno15"] = position == 15
            _apply_quiniela_slot(match, jornada_num, slot)
            match_key = _match_key(
                match.get("league", ""),
                match.get("local", ""),
                match.get("visitante", ""),
                match.get("kickoff", ""),
            )
            jornada_matches.append(match)
            all_keys.add(match_key)
            if jornada_num == current_jornada:
                current_keys.add(match_key)
        if jornada_matches:
            kickoff_dates = sorted(
                [match.get("kickoff", "") for match in jornada_matches if match.get("kickoff", "")]
            )
            jornadas.append(
                {
                    "jornada": jornada_num,
                    "label": f"Jornada {jornada_num}",
                    "is_current": jornada_num == current_jornada,
                    "source": payload.get("source", ""),
                    "source_url": payload.get("url", ""),
                    "kickoff_from": kickoff_dates[0] if kickoff_dates else "",
                    "kickoff_to": kickoff_dates[-1] if kickoff_dates else "",
                    "matches": jornada_matches,
                    "unmatched_slots": unmatched_slots,
                    "history_only": history_only,
                }
            )
    jornadas.sort(key=lambda item: item.get("jornada", 0), reverse=True)
    return jornadas, all_keys, current_keys


def _bootstrap_quiniela_placeholder(
    match: dict,
    raw_matches: list[dict],
    team_contexts: dict,
    histories: dict,
) -> None:
    home_team = str(match.get("local", "")).strip()
    away_team = str(match.get("visitante", "")).strip()
    if not home_team or not away_team:
        return

    home_context = team_contexts.get(home_team)
    if not home_context:
        home_context = _enrich_team(home_team, _guess_country_hint(home_team))
        team_contexts[home_team] = home_context
    away_context = team_contexts.get(away_team)
    if not away_context:
        away_context = _enrich_team(away_team, _guess_country_hint(away_team))
        team_contexts[away_team] = away_context

    home_profile = _repair_profile_location(
        home_team,
        (home_context or {}).get("profile", {}),
        _guess_country_hint(home_team),
    )
    away_profile = _repair_profile_location(
        away_team,
        (away_context or {}).get("profile", {}),
        _guess_country_hint(away_team),
    )
    match.setdefault("home_team_context", {})["profile"] = home_profile
    match.setdefault("away_team_context", {})["profile"] = away_profile
    match["home_team_context"].setdefault("news", (home_context or {}).get("news", {}).get("items", []))
    match["away_team_context"].setdefault("news", (away_context or {}).get("news", {}).get("items", []))
    match["home_team_context"].setdefault("signals", (home_context or {}).get("news", {}).get("signals", {}))
    match["away_team_context"].setdefault("signals", (away_context or {}).get("news", {}).get("signals", {}))
    match["home_team_context"].setdefault(
        "rotation_risk",
        _rotation_risk((home_context or {}).get("news", {}).get("signals", {})),
    )
    match["away_team_context"].setdefault(
        "rotation_risk",
        _rotation_risk((away_context or {}).get("news", {}).get("signals", {})),
    )
    match["home_team_context"].setdefault("focus_news", {"items": [], "signals": {}})
    match["away_team_context"].setdefault("focus_news", {"items": [], "signals": {}})
    match["home_team_context"].setdefault("media_news", {"items": [], "signals": {}})
    match["away_team_context"].setdefault("media_news", {"items": [], "signals": {}})
    match["home_team_context"].setdefault("official_site", {"website": "", "items": []})
    match["away_team_context"].setdefault("official_site", {"website": "", "items": []})

    home_team_api = fetch_the_sportsdb_team(home_team)
    away_team_api = fetch_the_sportsdb_team(away_team)
    inferred_league = match.get("league", "") or _infer_league_key_from_sportsdb(home_team_api, away_team_api)
    kickoff = str(match.get("kickoff", "")).strip()
    sportsdb_event = _resolve_sportsdb_event(home_team, away_team, kickoff, home_team_api, away_team_api) or {}
    if not kickoff:
        kickoff = _sportsdb_event_kickoff(sportsdb_event)
        if kickoff:
            match["kickoff"] = kickoff
    if not match.get("league") and inferred_league:
        match["league"] = inferred_league
    if not match.get("league"):
        match["league"] = _infer_league_key_from_sportsdb(sportsdb_event, home_team_api, away_team_api)

    league_key = str(match.get("league", "")).strip()
    if not league_key:
        return
    if league_key not in histories:
        histories[league_key] = fetch_league_history(league_key)
    league_history = histories.get(league_key, [])
    kickoff_dt = _parse_iso_datetime(match.get("kickoff", ""))
    season_code = _season_code_for(kickoff_dt or datetime.now(timezone.utc))
    season_history = _season_rows(league_history, season_code)
    completed_history = _completed_rows_before_kickoff(season_history, kickoff_dt)
    all_completed_history = _completed_rows_before_kickoff(league_history, kickoff_dt)
    current_table_snapshot = _table_snapshot(completed_history)
    home_history = _team_history_context(league_history, home_team, kickoff_dt)
    away_history = _team_history_context(league_history, away_team, kickoff_dt)
    home_resolved_name = home_history.get("resolved_name", home_team)
    away_resolved_name = away_history.get("resolved_name", away_team)
    h2h_history = _head_to_head_metrics(
        all_completed_history,
        home_resolved_name,
        away_resolved_name,
    )
    home_rest_days = _days_since_last_match(league_history, home_resolved_name, kickoff_dt)
    away_rest_days = _days_since_last_match(league_history, away_resolved_name, kickoff_dt)
    home_recent_matches = _matches_in_recent_days(league_history, home_resolved_name, kickoff_dt, 14)
    away_recent_matches = _matches_in_recent_days(league_history, away_resolved_name, kickoff_dt, 14)

    home_feed_upcoming = _upcoming_feed_fixtures(
        raw_matches,
        home_team,
        kickoff_dt,
        league_key,
        current_table_snapshot,
        season_history,
    )
    away_feed_upcoming = _upcoming_feed_fixtures(
        raw_matches,
        away_team,
        kickoff_dt,
        league_key,
        current_table_snapshot,
        season_history,
    )
    home_schedule_upcoming = _upcoming_team_fixtures(
        season_history,
        home_resolved_name,
        kickoff_dt,
        current_table_snapshot,
    )
    away_schedule_upcoming = _upcoming_team_fixtures(
        season_history,
        away_resolved_name,
        kickoff_dt,
        current_table_snapshot,
    )
    home_espn_upcoming = fetch_espn_team_fixtures(
        home_team,
        str(home_team_api.get("idESPN", "")).strip(),
        kickoff_dt,
        current_table_snapshot,
        season_history,
    )
    away_espn_upcoming = fetch_espn_team_fixtures(
        away_team,
        str(away_team_api.get("idESPN", "")).strip(),
        kickoff_dt,
        current_table_snapshot,
        season_history,
    )
    home_sportsdb_next_upcoming = _upcoming_sportsdb_next_fixtures(
        home_team,
        str(home_team_api.get("idTeam", "")).strip(),
        kickoff_dt,
        current_table_snapshot,
        season_history,
    )
    away_sportsdb_next_upcoming = _upcoming_sportsdb_next_fixtures(
        away_team,
        str(away_team_api.get("idTeam", "")).strip(),
        kickoff_dt,
        current_table_snapshot,
        season_history,
    )
    if sportsdb_event:
        home_round_upcoming = _upcoming_round_fixtures(
            home_team,
            kickoff_dt,
            sportsdb_event,
            current_table_snapshot,
            season_history,
        )
        away_round_upcoming = _upcoming_round_fixtures(
            away_team,
            kickoff_dt,
            sportsdb_event,
            current_table_snapshot,
            season_history,
        )
    else:
        home_round_upcoming = []
        away_round_upcoming = []

    home_upcoming = _merge_upcoming_fixtures(
        home_sportsdb_next_upcoming,
        home_round_upcoming,
        home_feed_upcoming,
        home_schedule_upcoming,
        home_espn_upcoming,
    )
    away_upcoming = _merge_upcoming_fixtures(
        away_sportsdb_next_upcoming,
        away_round_upcoming,
        away_feed_upcoming,
        away_schedule_upcoming,
        away_espn_upcoming,
    )
    home_relegation = _relegation_context(league_key, current_table_snapshot, home_resolved_name)
    away_relegation = _relegation_context(league_key, current_table_snapshot, away_resolved_name)
    season_competitive_context = _season_competitive_context(
        league_key,
        current_table_snapshot,
        home_resolved_name,
        away_resolved_name,
        kickoff_dt,
    )
    home_future_difficulty = _future_schedule_difficulty(home_upcoming)
    away_future_difficulty = _future_schedule_difficulty(away_upcoming)
    home_rotation_context = _rotation_context_from_upcoming(
        home_team,
        home_upcoming,
        match.get("kickoff", ""),
        ((match.get("home_team_context") or {}).get("focus_news") or {}).get("signals", {}),
    )
    away_rotation_context = _rotation_context_from_upcoming(
        away_team,
        away_upcoming,
        match.get("kickoff", ""),
        ((match.get("away_team_context") or {}).get("focus_news") or {}).get("signals", {}),
    )

    travel_distance_km = _haversine_km(
        home_profile.get("latitude"),
        home_profile.get("longitude"),
        away_profile.get("latitude"),
        away_profile.get("longitude"),
    )
    weather = fetch_weather_context(home_profile, match.get("kickoff", ""))
    home_fatigue_index = _fatigue_index(home_rest_days, home_recent_matches, 0.0)
    away_fatigue_index = _fatigue_index(away_rest_days, away_recent_matches, travel_distance_km)
    home_pressure_index = _pressure_index(home_history.get("table", {}), home_relegation, home_future_difficulty)
    away_pressure_index = _pressure_index(away_history.get("table", {}), away_relegation, away_future_difficulty)

    match.setdefault("market_context", {}).setdefault(
        "official_percent",
        (match.get("official_quiniela_percentages") or {}).copy(),
    )
    if not (match.get("market_context") or {}).get("normalized_percent"):
        match.setdefault("market_context", {})["normalized_percent"] = (
            match.get("official_quiniela_percentages") or {}
        ).copy()
    match["travel_context"] = {
        "distance_km": travel_distance_km,
        "distance_bucket": _distance_bucket(travel_distance_km),
        "home_country": home_profile.get("country", ""),
        "away_country": away_profile.get("country", ""),
        "international_trip": bool(
            home_profile.get("country_code")
            and away_profile.get("country_code")
            and home_profile.get("country_code") != away_profile.get("country_code")
        ),
    }
    match["weather_context"] = weather
    match["history_context"] = {
        "supported": bool(league_history),
        "home": home_history,
        "away": away_history,
        "head_to_head": h2h_history,
    }
    match["competition_context"] = {
        "season_code": season_code,
        "home_relegation": home_relegation,
        "away_relegation": away_relegation,
        "season_context_phase": season_competitive_context.get("season_context_phase", {}),
        "home_objective": season_competitive_context.get("home_objective", {}),
        "away_objective": season_competitive_context.get("away_objective", {}),
        "direct_rivalry": season_competitive_context.get("direct_rivalry", {}),
        "competitive_stakes_label": season_competitive_context.get("competitive_stakes_label", ""),
        "home_rotation_context": home_rotation_context,
        "away_rotation_context": away_rotation_context,
        "home_upcoming": home_upcoming,
        "away_upcoming": away_upcoming,
        "home_future_difficulty": home_future_difficulty,
        "away_future_difficulty": away_future_difficulty,
    }
    match["schedule_context"] = {
        "home": {
            "days_since_last_match": home_rest_days,
            "matches_last_14_days": home_recent_matches,
            "fatigue": _fatigue_rating(home_rest_days, home_recent_matches),
            "fatigue_index": home_fatigue_index,
        },
        "away": {
            "days_since_last_match": away_rest_days,
            "matches_last_14_days": away_recent_matches,
            "fatigue": _fatigue_rating(away_rest_days, away_recent_matches),
            "fatigue_index": away_fatigue_index,
        },
    }
    match["analytics_context"] = {
        "home_pressure_index": home_pressure_index,
        "away_pressure_index": away_pressure_index,
        "home_fatigue_index": home_fatigue_index,
        "away_fatigue_index": away_fatigue_index,
        "home_elo": home_history.get("elo_rating"),
        "away_elo": away_history.get("elo_rating"),
        "home_trend": home_history.get("streak", {}),
        "away_trend": away_history.get("streak", {}),
        "home_rolling": home_history.get("rolling", {}),
        "away_rolling": away_history.get("rolling", {}),
        "home_must_win_index": (
            season_competitive_context.get("home_objective", {}) or {}
        ).get("must_win_index", 0),
        "away_must_win_index": (
            season_competitive_context.get("away_objective", {}) or {}
        ).get("must_win_index", 0),
        "home_must_not_lose_index": (
            season_competitive_context.get("home_objective", {}) or {}
        ).get("must_not_lose_index", 0),
        "away_must_not_lose_index": (
            season_competitive_context.get("away_objective", {}) or {}
        ).get("must_not_lose_index", 0),
        "direct_rivalry_index": (
            season_competitive_context.get("direct_rivalry", {}) or {}
        ).get("direct_rivalry_index", 0),
        "home_objective_swing_if_win": (
            season_competitive_context.get("home_objective", {}) or {}
        ).get("objective_swing_if_win", 0),
        "home_objective_swing_if_lose": (
            season_competitive_context.get("home_objective", {}) or {}
        ).get("objective_swing_if_lose", 0),
        "away_objective_swing_if_win": (
            season_competitive_context.get("away_objective", {}) or {}
        ).get("objective_swing_if_win", 0),
        "away_objective_swing_if_lose": (
            season_competitive_context.get("away_objective", {}) or {}
        ).get("objective_swing_if_lose", 0),
    }
    match["_schedule_inputs"] = {
        "home_feed_upcoming": home_feed_upcoming,
        "away_feed_upcoming": away_feed_upcoming,
        "home_schedule_upcoming": home_schedule_upcoming,
        "away_schedule_upcoming": away_schedule_upcoming,
        "home_espn_upcoming": home_espn_upcoming,
        "away_espn_upcoming": away_espn_upcoming,
        "home_sportsdb_next_upcoming": home_sportsdb_next_upcoming,
        "away_sportsdb_next_upcoming": away_sportsdb_next_upcoming,
        "home_round_upcoming": home_round_upcoming,
        "away_round_upcoming": away_round_upcoming,
    }
    match.setdefault("match_news_context", {"items": [], "signals": {}})
    match.setdefault("structured_context", {})
    match.setdefault("match_signals", {})
    match["match_signals"].update(
        {
            "weather_risk": _weather_risk(weather),
            "travel_burden_away": _distance_bucket(travel_distance_km),
            "home_form_points_last_5": home_history.get("recent_all", {}).get("points"),
            "away_form_points_last_5": away_history.get("recent_all", {}).get("points"),
            "home_league_position": home_history.get("table", {}).get("position"),
            "away_league_position": away_history.get("table", {}).get("position"),
            "home_league_points": home_history.get("table", {}).get("points"),
            "away_league_points": away_history.get("table", {}).get("points"),
            "home_gap_to_drop": home_relegation.get("gap_to_drop_zone"),
            "away_gap_to_drop": away_relegation.get("gap_to_drop_zone"),
            "home_rest_days": home_rest_days,
            "away_rest_days": away_rest_days,
            "home_matches_last_14_days": home_recent_matches,
            "away_matches_last_14_days": away_recent_matches,
            "home_pressure_index": home_pressure_index.get("score"),
            "away_pressure_index": away_pressure_index.get("score"),
            "home_must_win_index": (season_competitive_context.get("home_objective", {}) or {}).get("must_win_index", 0),
            "away_must_win_index": (season_competitive_context.get("away_objective", {}) or {}).get("must_win_index", 0),
            "home_must_not_lose_index": (season_competitive_context.get("home_objective", {}) or {}).get("must_not_lose_index", 0),
            "away_must_not_lose_index": (season_competitive_context.get("away_objective", {}) or {}).get("must_not_lose_index", 0),
            "direct_rivalry_index": (season_competitive_context.get("direct_rivalry", {}) or {}).get("direct_rivalry_index", 0),
            "season_context_phase": (season_competitive_context.get("season_context_phase", {}) or {}).get("key", ""),
            "competitive_stakes_label": season_competitive_context.get("competitive_stakes_label", ""),
            "home_fatigue_index": home_fatigue_index.get("score"),
            "away_fatigue_index": away_fatigue_index.get("score"),
            "home_elo": home_history.get("elo_rating"),
            "away_elo": away_history.get("elo_rating"),
        }
    )


def _select_focus_match_indexes(matches: list[dict]) -> set[int]:
    ordered = sorted(range(len(matches)), key=lambda idx: _focus_sort_key(matches[idx]))
    return set(ordered[: max(0, FOCUS_MATCH_COUNT)])


def build_snapshot(raw_matches: list) -> dict:
    # Normaliza claves de liga (p.ej. Segunda -> LaLiga2 en algunas fuentes)
    normalized_raw = []
    for item in raw_matches or []:
        if not isinstance(item, dict):
            continue
        cloned = dict(item)
        raw_key = str(cloned.get("sport_key", "")).strip()
        canonical = _canonical_league_key(raw_key)
        if canonical and canonical != raw_key:
            cloned["sport_key_raw"] = raw_key
            cloned["sport_key"] = canonical
        normalized_raw.append(cloned)
    raw_matches = normalized_raw

    country_hints = _team_country_hints(raw_matches)
    unique_teams = sorted(
        {
            str(item.get("home_team", "")).strip()
            for item in raw_matches
            if str(item.get("home_team", "")).strip()
        }
        | {
            str(item.get("away_team", "")).strip()
            for item in raw_matches
            if str(item.get("away_team", "")).strip()
        }
    )

    team_contexts = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_enrich_team, team_name, country_hints.get(team_name)): team_name
            for team_name in unique_teams
        }
        for future, team_name in futures.items():
            try:
                team_contexts[team_name] = future.result()
            except Exception:
                team_contexts[team_name] = {
                    "profile": {"team": team_name},
                    "news": {"items": [], "signals": {}},
                }

    league_keys = sorted({str(item.get("sport_key", "")).strip() for item in raw_matches})
    league_team_names = {
        league_key: sorted(
            {
                str(item.get("home_team", "")).strip()
                for item in raw_matches
                if str(item.get("sport_key", "")).strip() == league_key
            }
            | {
                str(item.get("away_team", "")).strip()
                for item in raw_matches
                if str(item.get("sport_key", "")).strip() == league_key
            }
        )
        for league_key in league_keys
    }
    histories = {league_key: fetch_league_history(league_key) for league_key in league_keys}
    competition_headlines = {
        league_key: fetch_competition_headlines(
            league_key,
            league_teams=league_team_names.get(league_key, []),
            limit=6,
        )
        for league_key in league_keys
    }

    matches = []
    for item in raw_matches:
        league = str(item.get("sport_key", "")).strip()
        home_team = str(item.get("home_team", "")).strip()
        away_team = str(item.get("away_team", "")).strip()
        kickoff = str(item.get("commence_time", "")).strip()
        kickoff_dt = _parse_iso_datetime(kickoff)
        odds, bookmaker = _best_h2h(item.get("bookmakers") or [], home_team, away_team)
        odds_block = {
            "1": odds.get(home_team),
            "X": odds.get("Draw"),
            "2": odds.get(away_team),
        }

        home_context = team_contexts.get(
            home_team,
            {"profile": {"team": home_team}, "news": {"items": [], "signals": {}}},
        )
        away_context = team_contexts.get(
            away_team,
            {"profile": {"team": away_team}, "news": {"items": [], "signals": {}}},
        )
        home_profile = home_context.get("profile", {})
        away_profile = away_context.get("profile", {})
        home_news = home_context.get("news", {})
        away_news = away_context.get("news", {})

        league_history = histories.get(league, [])
        season_code = _season_code_for(kickoff_dt or datetime.now(timezone.utc))
        season_history = _season_rows(league_history, season_code)
        home_history = _team_history_context(league_history, home_team, kickoff_dt)
        away_history = _team_history_context(league_history, away_team, kickoff_dt)
        completed_history = _completed_rows_before_kickoff(season_history, kickoff_dt)
        all_completed_history = _completed_rows_before_kickoff(league_history, kickoff_dt)
        current_table_snapshot = _table_snapshot(completed_history)
        home_resolved_name = home_history.get("resolved_name", home_team)
        away_resolved_name = away_history.get("resolved_name", away_team)
        h2h_history = _head_to_head_metrics(
            all_completed_history,
            home_resolved_name,
            away_resolved_name,
        )
        home_rest_days = _days_since_last_match(league_history, home_resolved_name, kickoff_dt)
        away_rest_days = _days_since_last_match(league_history, away_resolved_name, kickoff_dt)
        home_recent_matches = _matches_in_recent_days(
            league_history,
            home_resolved_name,
            kickoff_dt,
            14,
        )
        away_recent_matches = _matches_in_recent_days(
            league_history,
            away_resolved_name,
            kickoff_dt,
            14,
        )
        home_feed_upcoming = _upcoming_feed_fixtures(
            raw_matches,
            home_team,
            kickoff_dt,
            league,
            current_table_snapshot,
            season_history,
        )
        away_feed_upcoming = _upcoming_feed_fixtures(
            raw_matches,
            away_team,
            kickoff_dt,
            league,
            current_table_snapshot,
            season_history,
        )
        home_schedule_upcoming = _upcoming_team_fixtures(
            season_history,
            home_resolved_name,
            kickoff_dt,
            current_table_snapshot,
        )
        away_schedule_upcoming = _upcoming_team_fixtures(
            season_history,
            away_resolved_name,
            kickoff_dt,
            current_table_snapshot,
        )
        home_team_api = fetch_the_sportsdb_team(home_team)
        away_team_api = fetch_the_sportsdb_team(away_team)
        home_espn_upcoming = fetch_espn_team_fixtures(
            home_team,
            str(home_team_api.get("idESPN", "")).strip(),
            kickoff_dt,
            current_table_snapshot,
            season_history,
        )
        away_espn_upcoming = fetch_espn_team_fixtures(
            away_team,
            str(away_team_api.get("idESPN", "")).strip(),
            kickoff_dt,
            current_table_snapshot,
            season_history,
        )
        home_sportsdb_next_upcoming = _upcoming_sportsdb_next_fixtures(
            home_team,
            str(home_team_api.get("idTeam", "")).strip(),
            kickoff_dt,
            current_table_snapshot,
            season_history,
        )
        away_sportsdb_next_upcoming = _upcoming_sportsdb_next_fixtures(
            away_team,
            str(away_team_api.get("idTeam", "")).strip(),
            kickoff_dt,
            current_table_snapshot,
            season_history,
        )
        home_upcoming = _merge_upcoming_fixtures(
            home_sportsdb_next_upcoming,
            home_feed_upcoming,
            home_schedule_upcoming,
            home_espn_upcoming,
        )
        away_upcoming = _merge_upcoming_fixtures(
            away_sportsdb_next_upcoming,
            away_feed_upcoming,
            away_schedule_upcoming,
            away_espn_upcoming,
        )
        home_relegation = _relegation_context(league, current_table_snapshot, home_resolved_name)
        away_relegation = _relegation_context(league, current_table_snapshot, away_resolved_name)
        season_competitive_context = _season_competitive_context(
            league,
            current_table_snapshot,
            home_resolved_name,
            away_resolved_name,
            kickoff_dt,
        )
        home_future_difficulty = _future_schedule_difficulty(home_upcoming)
        away_future_difficulty = _future_schedule_difficulty(away_upcoming)
        home_rotation_context = _rotation_context_from_upcoming(
            home_team,
            home_upcoming,
            kickoff,
            home_news.get("signals", {}),
        )
        away_rotation_context = _rotation_context_from_upcoming(
            away_team,
            away_upcoming,
            kickoff,
            away_news.get("signals", {}),
        )

        travel_distance_km = _haversine_km(
            home_profile.get("latitude"),
            home_profile.get("longitude"),
            away_profile.get("latitude"),
            away_profile.get("longitude"),
        )
        weather = fetch_weather_context(home_profile, kickoff)
        home_fatigue_index = _fatigue_index(home_rest_days, home_recent_matches, 0.0)
        away_fatigue_index = _fatigue_index(away_rest_days, away_recent_matches, travel_distance_km)
        home_pressure_index = _pressure_index(home_history.get("table", {}), home_relegation, home_future_difficulty)
        away_pressure_index = _pressure_index(away_history.get("table", {}), away_relegation, away_future_difficulty)
        match_key = _match_key(league, home_team, away_team, kickoff)

        matches.append(
            {
                "match_key": match_key,
                "league": league,
                "local": home_team,
                "visitante": away_team,
                "kickoff": kickoff,
                "bookmaker": bookmaker,
                "odds": odds_block,
                "market_context": _odds_probabilities(odds_block),
                "travel_context": {
                    "distance_km": travel_distance_km,
                    "distance_bucket": _distance_bucket(travel_distance_km),
                    "home_country": home_profile.get("country", ""),
                    "away_country": away_profile.get("country", ""),
                    "international_trip": bool(
                        home_profile.get("country_code")
                        and away_profile.get("country_code")
                        and home_profile.get("country_code") != away_profile.get("country_code")
                    ),
                },
                "weather_context": weather,
                "history_context": {
                    "supported": bool(league_history),
                    "home": home_history,
                    "away": away_history,
                    "head_to_head": h2h_history,
                },
                "competition_context": {
                    "season_code": season_code,
                    "home_relegation": home_relegation,
                    "away_relegation": away_relegation,
                    "season_context_phase": season_competitive_context.get("season_context_phase", {}),
                    "home_objective": season_competitive_context.get("home_objective", {}),
                    "away_objective": season_competitive_context.get("away_objective", {}),
                    "direct_rivalry": season_competitive_context.get("direct_rivalry", {}),
                    "competitive_stakes_label": season_competitive_context.get("competitive_stakes_label", ""),
                    "home_rotation_context": home_rotation_context,
                    "away_rotation_context": away_rotation_context,
                    "home_upcoming": home_upcoming,
                    "away_upcoming": away_upcoming,
                    "home_future_difficulty": home_future_difficulty,
                    "away_future_difficulty": away_future_difficulty,
                },
                "schedule_context": {
                    "home": {
                        "days_since_last_match": home_rest_days,
                        "matches_last_14_days": home_recent_matches,
                        "fatigue": _fatigue_rating(home_rest_days, home_recent_matches),
                        "fatigue_index": home_fatigue_index,
                    },
                    "away": {
                        "days_since_last_match": away_rest_days,
                        "matches_last_14_days": away_recent_matches,
                        "fatigue": _fatigue_rating(away_rest_days, away_recent_matches),
                        "fatigue_index": away_fatigue_index,
                    },
                },
                "analytics_context": {
                    "home_pressure_index": home_pressure_index,
                    "away_pressure_index": away_pressure_index,
                    "home_fatigue_index": home_fatigue_index,
                    "away_fatigue_index": away_fatigue_index,
                    "home_elo": home_history.get("elo_rating"),
                    "away_elo": away_history.get("elo_rating"),
                    "home_trend": home_history.get("streak", {}),
                    "away_trend": away_history.get("streak", {}),
                    "home_rolling": home_history.get("rolling", {}),
                    "away_rolling": away_history.get("rolling", {}),
                    "home_must_win_index": (season_competitive_context.get("home_objective", {}) or {}).get("must_win_index", 0),
                    "away_must_win_index": (season_competitive_context.get("away_objective", {}) or {}).get("must_win_index", 0),
                    "home_must_not_lose_index": (season_competitive_context.get("home_objective", {}) or {}).get("must_not_lose_index", 0),
                    "away_must_not_lose_index": (season_competitive_context.get("away_objective", {}) or {}).get("must_not_lose_index", 0),
                    "direct_rivalry_index": (season_competitive_context.get("direct_rivalry", {}) or {}).get("direct_rivalry_index", 0),
                    "home_rotation_risk_index": home_rotation_context.get("score", 0),
                    "away_rotation_risk_index": away_rotation_context.get("score", 0),
                },
                "home_team_context": {
                    "profile": home_profile,
                    "news": home_news.get("items", []),
                    "signals": home_news.get("signals", {}),
                    "rotation_risk": _rotation_risk(home_news.get("signals", {})),
                    "focus_news": {"items": [], "signals": {}},
                    "media_news": {"items": [], "signals": {}},
                    "official_site": {"website": "", "items": []},
                },
                "away_team_context": {
                    "profile": away_profile,
                    "news": away_news.get("items", []),
                    "signals": away_news.get("signals", {}),
                    "rotation_risk": _rotation_risk(away_news.get("signals", {})),
                    "focus_news": {"items": [], "signals": {}},
                    "media_news": {"items": [], "signals": {}},
                    "official_site": {"website": "", "items": []},
                },
                "match_news_context": {"items": [], "signals": {}},
                "competition_headlines": competition_headlines.get(
                    league,
                    {"items": [], "source_health": []},
                ),
                "match_signals": {
                    "weather_risk": _weather_risk(weather),
                    "travel_burden_away": _distance_bucket(travel_distance_km),
                    "injury_attention_home": home_news.get("signals", {}).get("injury_count", 0),
                    "injury_attention_away": away_news.get("signals", {}).get("injury_count", 0),
                    "europe_attention_home": home_news.get("signals", {}).get("europe_count", 0),
                    "europe_attention_away": away_news.get("signals", {}).get("europe_count", 0),
                    "discipline_attention_home": home_news.get("signals", {}).get("discipline_count", 0),
                    "discipline_attention_away": away_news.get("signals", {}).get("discipline_count", 0),
                    "home_form_points_last_5": home_history.get("recent_all", {}).get("points"),
                    "away_form_points_last_5": away_history.get("recent_all", {}).get("points"),
                    "home_league_position": home_history.get("table", {}).get("position"),
                    "away_league_position": away_history.get("table", {}).get("position"),
                    "home_league_points": home_history.get("table", {}).get("points"),
                    "away_league_points": away_history.get("table", {}).get("points"),
                    "home_gap_to_drop": home_relegation.get("gap_to_drop_zone"),
                    "away_gap_to_drop": away_relegation.get("gap_to_drop_zone"),
                    "home_rest_days": home_rest_days,
                    "away_rest_days": away_rest_days,
                    "home_matches_last_14_days": home_recent_matches,
                    "away_matches_last_14_days": away_recent_matches,
                    "home_pressure_index": home_pressure_index.get("score"),
                    "away_pressure_index": away_pressure_index.get("score"),
                    "home_must_win_index": (season_competitive_context.get("home_objective", {}) or {}).get("must_win_index", 0),
                    "away_must_win_index": (season_competitive_context.get("away_objective", {}) or {}).get("must_win_index", 0),
                    "home_must_not_lose_index": (season_competitive_context.get("home_objective", {}) or {}).get("must_not_lose_index", 0),
                    "away_must_not_lose_index": (season_competitive_context.get("away_objective", {}) or {}).get("must_not_lose_index", 0),
                    "direct_rivalry_index": (season_competitive_context.get("direct_rivalry", {}) or {}).get("direct_rivalry_index", 0),
                    "season_context_phase": (season_competitive_context.get("season_context_phase", {}) or {}).get("key", ""),
                    "competitive_stakes_label": season_competitive_context.get("competitive_stakes_label", ""),
                    "home_rotation_risk_index": home_rotation_context.get("score", 0),
                    "away_rotation_risk_index": away_rotation_context.get("score", 0),
                    "home_fatigue_index": home_fatigue_index.get("score"),
                    "away_fatigue_index": away_fatigue_index.get("score"),
                    "home_elo": home_history.get("elo_rating"),
                    "away_elo": away_history.get("elo_rating"),
                },
                "quiniela_slots": [],
                "quiniela_focus": False,
                "quiniela_tracked": False,
                "structured_context": {},
                "focus_digest": [],
                "focus_ai_briefing": {},
                "_schedule_inputs": {
                    "home_feed_upcoming": home_feed_upcoming,
                    "away_feed_upcoming": away_feed_upcoming,
                    "home_schedule_upcoming": home_schedule_upcoming,
                    "away_schedule_upcoming": away_schedule_upcoming,
                    "home_espn_upcoming": home_espn_upcoming,
                    "away_espn_upcoming": away_espn_upcoming,
                    "home_sportsdb_next_upcoming": home_sportsdb_next_upcoming,
                    "away_sportsdb_next_upcoming": away_sportsdb_next_upcoming,
                },
                "notes": [
                    f"bookmaker={bookmaker}" if bookmaker else "",
                    f"id={item.get('id', '')}",
                    f"source={DATA_URL}",
                ],
            }
        )

    quiniela_jornadas, quiniela_all_keys, quiniela_current_keys = build_quiniela_jornadas(matches)
    if not quiniela_jornadas:
        focus_indexes = _select_focus_match_indexes(matches)
        fallback_matches = []
        quiniela_all_keys = set()
        quiniela_current_keys = set()
        for position, idx in enumerate(focus_indexes, start=1):
            match = matches[idx]
            match.setdefault("quiniela_slots", []).append(
                {"jornada": 0, "position": position, "pleno15": position == 15}
            )
            match_key = match.get("match_key", "")
            if match_key:
                quiniela_all_keys.add(match_key)
                quiniela_current_keys.add(match_key)
            fallback_matches.append(match)
        if fallback_matches:
            quiniela_jornadas = [
                {
                    "jornada": 0,
                    "label": "Seleccion automatica",
                    "source": "Fallback interno",
                    "source_url": "",
                    "kickoff_from": fallback_matches[0].get("kickoff", ""),
                    "kickoff_to": fallback_matches[-1].get("kickoff", ""),
                    "matches": fallback_matches,
                    "unmatched_slots": [],
                }
            ]

    tracked_matches = []
    quiniela_focus_matches = []
    for match in matches:
        match_key = match.get("match_key", "")
        is_tracked = match_key in quiniela_all_keys
        is_focus = match_key in quiniela_current_keys
        match["quiniela_tracked"] = is_tracked
        match["quiniela_focus"] = is_focus
        if is_tracked:
            _enrich_quiniela_match(match)
            tracked_matches.append(match)
            if is_focus:
                quiniela_focus_matches.append(match)

    if quiniela_jornadas:
        for jornada in quiniela_jornadas:
            for match in jornada.get("matches", []):
                competition_context = match.get("competition_context") or {}
                needs_bootstrap = (
                    not match.get("league")
                    or not match.get("kickoff")
                    or not competition_context.get("home_upcoming")
                    or not competition_context.get("away_upcoming")
                )
                if needs_bootstrap:
                    _bootstrap_quiniela_placeholder(match, raw_matches, team_contexts, histories)
        for jornada in quiniela_jornadas:
            for match in jornada.get("matches", []):
                competition_context = match.get("competition_context") or {}
                home_upcoming = competition_context.get("home_upcoming") or []
                away_upcoming = competition_context.get("away_upcoming") or []
                should_refresh = (
                    not match.get("focus_ai_briefing")
                    or (
                        match.get("league")
                        and match.get("kickoff")
                        and (
                            len(home_upcoming) < UPCOMING_FIXTURE_WINDOW
                            or len(away_upcoming) < UPCOMING_FIXTURE_WINDOW
                        )
                    )
                )
                if should_refresh and match.get("league") and match.get("kickoff"):
                    _enrich_quiniela_match(match)
        ordered_tracked = []
        seen_keys = set()
        for jornada in quiniela_jornadas:
            resolved_matches = []
            for match in jornada.get("matches", []):
                match_key = match.get("match_key", "")
                if match_key and match_key not in seen_keys:
                    ordered_tracked.append(match)
                    seen_keys.add(match_key)
                resolved_matches.append(match)
            jornada["matches"] = resolved_matches
        tracked_matches = ordered_tracked
        current_jornada_num = next(
            (jornada.get("jornada") for jornada in quiniela_jornadas if jornada.get("is_current")),
            quiniela_jornadas[0].get("jornada"),
        )
        quiniela_focus_matches = []
        for jornada in quiniela_jornadas:
            if jornada.get("jornada") != current_jornada_num:
                continue
            quiniela_focus_matches = list(jornada.get("matches", []))
            break
        _persist_quiniela_history(quiniela_jornadas)
        for jornada in quiniela_jornadas:
            for match in jornada.get("matches", []):
                competition_context = match.get("competition_context") or {}
                if (
                    not match.get("league")
                    or not match.get("kickoff")
                    or not competition_context.get("home_upcoming")
                    or not competition_context.get("away_upcoming")
                ):
                    _bootstrap_quiniela_placeholder(match, raw_matches, team_contexts, histories)
                if match.get("league") and match.get("kickoff") and not match.get("focus_ai_briefing"):
                    _enrich_quiniela_match(match)
    quiniela_integrity = _audit_quiniela_integrity(
        quiniela_jornadas,
        _safe_int(_eduardo_current_context().get("temporada")),
    )

    active_match_keys = set()
    for match in tracked_matches:
        structured_context = match.get("structured_context", {})
        match_key = structured_context.get("match_key") or match.get("match_key")
        if not match_key:
            continue
        active_match_keys.add(match_key)
        STRUCTURED_DB.setdefault("matches", {})[match_key] = {
            "league": match.get("league", ""),
            "local": match.get("local", ""),
            "visitante": match.get("visitante", ""),
            "kickoff": match.get("kickoff", ""),
            "quiniela_focus": match.get("quiniela_focus", False),
            "quiniela_tracked": match.get("quiniela_tracked", False),
            "quiniela_slots": match.get("quiniela_slots", []),
            "structured_context": structured_context,
            "referee_context": structured_context.get("referee_context", {}),
            "injury_context": structured_context.get("injury_context", {}),
            "event_context": structured_context.get("event_context", {}),
            "updated_at": structured_context.get("updated_at", _now_iso()),
        }
        STRUCTURED_DB.setdefault("teams", {})[match.get("local", "")] = {
            "team": match.get("local", ""),
            "injuries": structured_context.get("injury_context", {}).get("home_team", {}),
            "profile": match.get("home_team_context", {}).get("profile", {}),
            "updated_at": _now_iso(),
        }
        STRUCTURED_DB.setdefault("teams", {})[match.get("visitante", "")] = {
            "team": match.get("visitante", ""),
            "injuries": structured_context.get("injury_context", {}).get("away_team", {}),
            "profile": match.get("away_team_context", {}).get("profile", {}),
            "updated_at": _now_iso(),
        }
        referee_record = _structured_referee_record(
            structured_context.get("referee_context", {}),
            match,
        )
        if referee_record:
            STRUCTURED_DB.setdefault("referees", {})[referee_record["name"]] = referee_record

    _prune_structured_db(active_match_keys)
    STRUCTURED_DB.setdefault("meta", {})["last_snapshot_generated_at"] = _now_iso()
    STRUCTURED_DB.setdefault("meta", {})["active_focus_matches"] = len(active_match_keys)

    coverage = {
        "monitored_matches": len(matches),
        "focus_matches": len(quiniela_focus_matches),
        "tracked_quiniela_matches": len(tracked_matches),
        "quiniela_jornadas": len(quiniela_jornadas),
        "quiniela_current_jornada": next(
            (jornada.get("jornada") for jornada in quiniela_jornadas if jornada.get("is_current")),
            quiniela_jornadas[0].get("jornada") if quiniela_jornadas else None,
        ),
        "quiniela_latest_available_jornada": max(
            [jornada.get("jornada") for jornada in quiniela_jornadas if jornada.get("jornada") is not None],
            default=None,
        ),
        "quiniela_unmatched_slots": sum(
            len(jornada.get("unmatched_slots", [])) for jornada in quiniela_jornadas
        ),
        "quiniela_integrity_ok": bool(quiniela_integrity.get("ok")),
        "quiniela_integrity_mismatches": quiniela_integrity.get("mismatch_count", 0),
        "quiniela_integrity_slots": quiniela_integrity.get("checked_slots", 0),
        "teams": len(unique_teams),
        "news_language": NEWS_LANGUAGE,
        "news_country": NEWS_COUNTRY,
        "poll_seconds": POLL_SECONDS,
        "historical_leagues": len([key for key, rows in histories.items() if rows]),
        "weather_matches": sum(1 for match in matches if match.get("weather_context")),
        "travel_matches": sum(
            1
            for match in matches
            if (match.get("travel_context") or {}).get("distance_km") is not None
        ),
        "history_matches": sum(
            1
            for match in matches
            if (match.get("history_context") or {}).get("supported")
        ),
        "structured_focus_matches": len(active_match_keys),
        "structured_teams": len(STRUCTURED_DB.get("teams", {})),
        "structured_referees": len(STRUCTURED_DB.get("referees", {})),
    }
    source_health = _source_health_summary(competition_headlines)
    coverage.update(source_health)

    snapshot = {
        "source": "quiniai-external-context-worker",
        "generated_at": _now_iso(),
        "context_sources": [
            {"name": "Odds snapshot repo", "url": DATA_URL},
            {"name": "Eduardo Losilla Quinielista", "url": QUINIELA_ROOT_URL},
            {"name": "Eduardo Losilla Proximas", "url": EDUARDO_QUINIELA_PROXIMAS_URL},
            {"name": "RFEF designaciones", "url": "https://rfef.es"},
            {"name": "Wikipedia Action API", "url": WIKI_API_URL},
            {"name": "Open-Meteo Geocoding", "url": OPEN_METEO_GEOCODING_URL},
            {"name": "Open-Meteo Forecast", "url": OPEN_METEO_FORECAST_URL},
            {"name": "Google News RSS", "url": GOOGLE_NEWS_RSS_URL},
            {"name": "Football-Data historical CSV", "url": FOOTBALL_DATA_BASE_URL},
            {"name": "BBC Football RSS", "url": BBC_FOOTBALL_RSS_URL},
            {"name": "Guardian Football RSS", "url": GUARDIAN_FOOTBALL_RSS_URL},
            {"name": "TheSportsDB", "url": THESPORTSDB_SEARCH_TEAM_URL},
        ],
        "coverage": coverage,
        "structured_db_summary": {
            "teams": len(STRUCTURED_DB.get("teams", {})),
            "matches": len(STRUCTURED_DB.get("matches", {})),
            "referees": len(STRUCTURED_DB.get("referees", {})),
            "last_pruned_at": STRUCTURED_DB.get("meta", {}).get("last_pruned_at", ""),
        },
        "source_health_summary": source_health,
        "competition_headlines": competition_headlines,
        "quiniela_integrity": quiniela_integrity,
        "quiniela_jornadas": quiniela_jornadas,
        "quiniela_focus_matches": quiniela_focus_matches,
        "quiniela_tracked_matches": tracked_matches,
        "matches": matches,
        "audit_news_quality": {
            "news_language": NEWS_LANGUAGE,
            "news_country": NEWS_COUNTRY,
            "high_trust_domains": sorted(HIGH_TRUST_NEWS_DOMAINS),
            "low_trust_domains": sorted(LOW_TRUST_NEWS_DOMAINS),
            "notes": "Se resuelven enlaces de Google News RSS cuando incluyen url/q, se filtra ruido y se prioriza prensa fiable.",
        },
    }
    _flush_caches()
    return snapshot


def fetch_snapshot() -> dict:
    raw_matches = fetch_repo_odds()
    return build_snapshot(raw_matches)


def save_local_snapshot(snapshot: dict) -> None:
    _ensure_cache_dir()
    with open(SNAPSHOT_OUTPUT_PATH, "w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, ensure_ascii=False, indent=2)


def upload_snapshot(snapshot: dict) -> None:
    if not ADMIN_KEY:
        raise RuntimeError("QUINIAI_ADMIN_KEY no configurada")
    response = requests.post(
        f"{BACKEND_URL}/admin/ia-feed",
        headers={
            "x-admin-key": ADMIN_KEY,
            "Content-Type": "application/json",
        },
        data=json.dumps(snapshot, ensure_ascii=False),
        timeout=60,
    )
    response.raise_for_status()
    print(response.json())


def run_once(print_summary: bool = False) -> dict:
    started_at = time.time()
    _log_cycle_event("info", "cycle_started", poll_seconds=POLL_SECONDS)
    snapshot = fetch_snapshot()
    save_local_snapshot(snapshot)
    upload_snapshot(snapshot)
    duration_seconds = round(time.time() - started_at, 2)
    _append_run_history(
        {
            "started_at": datetime.fromtimestamp(started_at, tz=timezone.utc).isoformat(),
            "finished_at": _now_iso(),
            "ok": True,
            "duration_seconds": duration_seconds,
            "monitored_matches": snapshot.get("coverage", {}).get("monitored_matches", 0),
            "tracked_matches": snapshot.get("coverage", {}).get("tracked_quiniela_matches", 0),
            "current_jornada": snapshot.get("coverage", {}).get("quiniela_current_jornada"),
        }
    )
    _persist_run_history()
    write_status_files(snapshot=snapshot)
    _log_cycle_event(
        "info",
        "cycle_completed",
        duration_seconds=duration_seconds,
        monitored_matches=snapshot.get("coverage", {}).get("monitored_matches", 0),
        current_jornada=snapshot.get("coverage", {}).get("quiniela_current_jornada"),
    )
    integrity = snapshot.get("quiniela_integrity") or {}
    if integrity and not integrity.get("ok"):
        _log_cycle_event(
            "warning",
            "quiniela_integrity_failed",
            mismatch_count=integrity.get("mismatch_count", 0),
            mismatches=integrity.get("mismatches", [])[:6],
        )
    if print_summary:
        print_pretty_summary(snapshot)
    return snapshot


def run_forever() -> None:
    while True:
        started = time.time()
        try:
            snapshot = run_once(print_summary=False)
            print(
                f"[snapshot-worker] ok monitored={snapshot['coverage']['monitored_matches']} "
                f"jornada={snapshot['coverage']['quiniela_current_jornada']} "
                f"generated_at={snapshot['generated_at']}"
            )
        except KeyboardInterrupt:
            _log_cycle_event("warning", "worker_interrupted")
            raise
        except Exception as exc:
            duration_seconds = round(time.time() - started, 2)
            _append_run_history(
                {
                    "started_at": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
                    "finished_at": _now_iso(),
                    "ok": False,
                    "duration_seconds": duration_seconds,
                    "error": str(exc),
                }
            )
            _persist_run_history()
            write_status_files(snapshot=None, error=str(exc))
            _log_cycle_event(
                "error",
                "cycle_failed",
                duration_seconds=duration_seconds,
                error=str(exc),
                traceback=traceback.format_exc(limit=8),
            )
            print(f"[snapshot-worker] error: {exc}")
        elapsed = time.time() - started
        sleep_for = max(30, POLL_SECONDS - int(elapsed))
        _log_cycle_event("info", "cycle_sleep", sleep_for_seconds=sleep_for)
        remaining = sleep_for
        while remaining > 0:
            step = min(5, remaining)
            time.sleep(step)
            remaining -= step
            if _consume_manual_refresh_flag():
                _log_cycle_event("info", "manual_refresh_triggered")
                break


if __name__ == "__main__":
    _acquire_worker_lock()
    if "--once" in sys.argv:
        snapshot = run_once(print_summary="--pretty" in sys.argv)
        if "--pretty" not in sys.argv:
            print(
                f"[snapshot-worker] ok monitored={snapshot['coverage']['monitored_matches']} "
                f"jornada={snapshot['coverage']['quiniela_current_jornada']} "
                f"generated_at={snapshot['generated_at']}"
            )
    else:
        run_forever()
