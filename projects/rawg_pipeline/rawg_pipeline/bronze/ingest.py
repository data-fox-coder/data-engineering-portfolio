"""
rawg_pipeline/bronze/ingest.py
-------------------------------
Fetches raw game, genre, and platform data from the RAWG API
and stores append-only JSON strings in the bronze DuckDB schema.
"""
import json
import logging
import os
from dotenv import load_dotenv

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy.orm import Session

from rawg_pipeline.db import init_db, SessionLocal
from rawg_pipeline.bronze.models import BronzeGame, BronzeGenre, BronzePlatform

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Retrieve and validate API key immediately
API_KEY = os.getenv("RAWG_API_KEY")
if not API_KEY:
    raise ValueError(
        "RAWG_API_KEY is missing! "
        "Make sure you have a .env file in your root directory with: "
        "RAWG_API_KEY=your_actual_key_here"
    )

BASE_URL = "https://api.rawg.io/api"


def build_session() -> requests.Session:
    """
    Returns a requests Session with a retry adapter mounted.
    Retries up to 3 times on transient errors (429, 500, 502, 503, 504)
    with exponential backoff.
    """
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_games(http: requests.Session, page_size: int = 40) -> list[dict]:
    """Fetch a single page of games from the RAWG API."""
    url = f"{BASE_URL}/games"
    params = {"key": API_KEY, "page_size": page_size}
    response = http.get(url, params=params)
    response.raise_for_status()
    return response.json().get("results", [])


def fetch_genres(http: requests.Session) -> list[dict]:
    """Fetch all genres from the RAWG API."""
    url = f"{BASE_URL}/genres"
    params = {"key": API_KEY}
    response = http.get(url, params=params)
    response.raise_for_status()
    return response.json().get("results", [])


def fetch_platforms(http: requests.Session) -> list[dict]:
    """Fetch all platforms from the RAWG API."""
    url = f"{BASE_URL}/platforms"
    params = {"key": API_KEY}
    response = http.get(url, params=params)
    response.raise_for_status()
    return response.json().get("results", [])


def load_bronze(session: Session, games: list, genres: list, platforms: list) -> None:
    """Write raw API results to the bronze DuckDB schema as append-only JSON."""
    for game in games:
        session.add(BronzeGame(rawg_id=game["id"], raw_json=json.dumps(game)))
    for genre in genres:
        session.add(BronzeGenre(rawg_id=genre["id"], raw_json=json.dumps(genre)))
    for platform in platforms:
        session.add(BronzePlatform(rawg_id=platform["id"], raw_json=json.dumps(platform)))
    session.commit()


if __name__ == "__main__":
    init_db()
    http = build_session()
    with SessionLocal() as session:
        logger.info("Fetching from RAWG API...")
        games = fetch_games(http)
        genres = fetch_genres(http)
        platforms = fetch_platforms(http)
        load_bronze(session, games, genres, platforms)
        logger.info(
            "Loaded %d games, %d genres, %d platforms.",
            len(games), len(genres), len(platforms),
        )