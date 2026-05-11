"""
rawg_pipeline/bronze/ingest.py
-------------------------------
Fetches raw game, genre, and platform data from the RAWG API
and stores append-only JSON strings in the bronze DuckDB schema.
"""
import json
import os
from dotenv import load_dotenv

import requests
from sqlalchemy.orm import Session

from rawg_pipeline.db import engine, Base, init_db, SessionLocal  # Add SessionLocal here
from rawg_pipeline.bronze.models import BronzeGame, BronzeGenre, BronzePlatform

# Load environment variable from .env file
load_dotenv()

# Retrieve the key from the environment variable
API_KEY = os.getenv("RAWG_API_KEY")

# Validate the key immediately
if not API_KEY:
    raise ValueError(
        "RAWG_API_KEY is missing! "
        "Make sure you have a .env file in your root directory with: "
        "RAWG_API_KEY=your_actual_key_here"
    )

BASE_URL = "https://api.rawg.io/api"


def fetch_games(page_size: int = 40) -> list[dict]:
    url = f"{BASE_URL}/games"
    params = {"key": API_KEY, "page_size": page_size}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get("results", [])


def fetch_genres() -> list[dict]:
    url = f"{BASE_URL}/genres"
    params = {"key": API_KEY}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get("results", [])


def fetch_platforms() -> list[dict]:
    url = f"{BASE_URL}/platforms"
    params = {"key": API_KEY}
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json().get("results", [])


def load_bronze(session: Session, games: list, genres: list, platforms: list) -> None:
    for game in games:
        session.add(BronzeGame(rawg_id=game["id"], raw_json=json.dumps(game)))
    for genre in genres:
        session.add(BronzeGenre(rawg_id=genre["id"], raw_json=json.dumps(genre)))
    for platform in platforms:
        session.add(BronzePlatform(rawg_id=platform["id"], raw_json=json.dumps(platform)))
    session.commit()


if __name__ == "__main__":
    init_db()
    with SessionLocal() as session:
        print("Fetching from RAWG API...")
        games = fetch_games()
        genres = fetch_genres()
        platforms = fetch_platforms()
        load_bronze(session, games, genres, platforms)
        print(f"Loaded {len(games)} games, {len(genres)} genres, {len(platforms)} platforms.")
