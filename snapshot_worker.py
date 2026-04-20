import json
import os
import time
from datetime import datetime, timezone

import requests


ODDS_API_KEY = os.getenv("ODDS_API_KEY", "").strip()
BACKEND_URL = os.getenv(
    "QUINIAI_BACKEND_URL",
    "https://quiniela-backend-production-cb1a.up.railway.app",
).rstrip("/")
ADMIN_KEY = os.getenv("QUINIAI_ADMIN_KEY", "").strip()
POLL_SECONDS = int(os.getenv("SNAPSHOT_POLL_SECONDS", "900"))

LEAGUES = [
    "soccer_spain_la_liga",
    "soccer_spain_segunda_division",
    "soccer_uefa_champs_league",
    "soccer_uefa_europa_league",
    "soccer_uefa_europa_conference_league",
    "soccer_epl",
    "soccer_efl_champ",
]


def _best_h2h(bookmakers: list) -> tuple[dict, str]:
    best = {}
    book_name = ""
    for book in bookmakers or []:
        markets = book.get("markets") or []
        if not markets:
            continue
        outcomes = markets[0].get("outcomes") or []
        current = {}
        for out in outcomes:
            name = str(out.get("name", "")).strip()
            price = out.get("price")
            if name and price:
                current[name] = price
        if {"Home", "Away"} & set(current.keys()):
            best = current
            book_name = str(book.get("title", "")).strip()
            break
    return best, book_name


def fetch_snapshot() -> dict:
    if not ODDS_API_KEY:
        raise RuntimeError("ODDS_API_KEY no configurada")

    matches = []
    for league in LEAGUES:
        url = (
            f"https://api.the-odds-api.com/v4/sports/{league}/odds/"
            f"?apiKey={ODDS_API_KEY}&regions=eu&markets=h2h&oddsFormat=decimal"
        )
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        for item in resp.json():
            odds, bookmaker = _best_h2h(item.get("bookmakers") or [])
            matches.append(
                {
                    "league": league,
                    "local": item.get("home_team", ""),
                    "visitante": next(
                        (
                            team
                            for team in item.get("away_team", [])
                            if team != item.get("home_team")
                        ),
                        item.get("away_team", ""),
                    )
                    if isinstance(item.get("away_team"), list)
                    else item.get("away_team", ""),
                    "kickoff": item.get("commence_time", ""),
                    "bookmaker": bookmaker,
                    "odds": {
                        "1": odds.get("Home"),
                        "X": odds.get("Draw"),
                        "2": odds.get("Away"),
                    },
                    "notes": [
                        f"bookmaker={bookmaker}" if bookmaker else "",
                        f"id={item.get('id', '')}",
                    ],
                }
            )

    return {
        "source": "second-pc-odds-worker",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "matches": matches,
    }


def save_local_snapshot(snapshot: dict) -> None:
    with open("ia_feed_snapshot.json", "w", encoding="utf-8") as fh:
        json.dump(snapshot, fh, ensure_ascii=False, indent=2)


def upload_snapshot(snapshot: dict) -> None:
    if not ADMIN_KEY:
        raise RuntimeError("QUINIAI_ADMIN_KEY no configurada")
    resp = requests.post(
        f"{BACKEND_URL}/admin/ia-feed",
        headers={
            "x-admin-key": ADMIN_KEY,
            "Content-Type": "application/json",
        },
        data=json.dumps(snapshot, ensure_ascii=False),
        timeout=30,
    )
    resp.raise_for_status()
    print(resp.json())


def run_forever() -> None:
    while True:
        started = time.time()
        try:
            snapshot = fetch_snapshot()
            save_local_snapshot(snapshot)
            upload_snapshot(snapshot)
            print(
                f"[snapshot-worker] ok matches={len(snapshot['matches'])} "
                f"generated_at={snapshot['generated_at']}"
            )
        except Exception as exc:
            print(f"[snapshot-worker] error: {exc}")

        elapsed = time.time() - started
        sleep_for = max(30, POLL_SECONDS - int(elapsed))
        time.sleep(sleep_for)


if __name__ == "__main__":
    run_forever()
