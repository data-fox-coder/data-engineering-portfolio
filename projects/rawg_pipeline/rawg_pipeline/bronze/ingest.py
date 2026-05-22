"""
rawg_pipeline/bronze/ingest.py
-------------------------------
Fetches raw game, genre, and platform data from the RAWG API
and stores append-only JSON strings in the bronze DuckDB schema.
"""
import json
import logging
import os
from datetime import datetime, timezone

import duckdb
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()

API_KEY = os.getenv("RAWG_API_KEY")
if not API_KEY:
    raise ValueError(
        "RAWG_API_KEY is missing! "
        "Make sure you have a .env file with: RAWG_API_KEY=your_key"
    )

BASE_URL = "https://api.rawg.io/api"
DB_PATH = os.getenv("DB_PATH", "rawg_data.duckdb")


def get_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH)


def init_bronze(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.bronze_games (
            id          INTEGER PRIMARY KEY,
            rawg_id     INTEGER NOT NULL,
            raw_json    TEXT NOT NULL,
            ingested_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.bronze_genres (
            id          INTEGER PRIMARY KEY,
            rawg_id     INTEGER NOT NULL,
            raw_json    TEXT NOT NULL,
            ingested_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.bronze_platforms (
            id          INTEGER PRIMARY KEY,
            rawg_id     INTEGER NOT NULL,
            raw_json    TEXT NOT NULL,
            ingested_at TIMESTAMP DEFAULT current_timestamp
        )
    """)


def build_session() -> requests.Session:
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
    url = f"{BASE_URL}/games"
    params = {"key": API_KEY, "page_size": page_size}
    response = http.get(url, params=params)
    response.raise_for_status()
    return response.json().get("results", [])


def fetch_genres(http: requests.Session) -> list[dict]:
    url = f"{BASE_URL}/genres"
    params = {"key": API_KEY}
    response = http.get(url, params=params)
    response.raise_for_status()
    return response.json().get("results", [])


def fetch_platforms(http: requests.Session) -> list[dict]:
    url = f"{BASE_URL}/platforms"
    params = {"key": API_KEY}
    response = http.get(url, params=params)
    response.raise_for_status()
    return response.json().get("results", [])


def load_bronze(
    conn: duckdb.DuckDBPyConnection,
    games: list[dict],
    genres: list[dict],
    platforms: list[dict],
) -> None:
    now = datetime.now(timezone.utc)
    conn.executemany(
        "INSERT INTO bronze.bronze_games (id, rawg_id, raw_json, ingested_at) VALUES (nextval('bronze_games_seq'), ?, ?, ?)",
        [(g["id"], json.dumps(g), now) for g in games],
    )
    conn.executemany(
        "INSERT INTO bronze.bronze_genres (id, rawg_id, raw_json, ingested_at) VALUES (nextval('bronze_genres_seq'), ?, ?, ?)",
        [(g["id"], json.dumps(g), now) for g in genres],
    )
    conn.executemany(
        "INSERT INTO bronze.bronze_platforms (id, rawg_id, raw_json, ingested_at) VALUES (nextval('bronze_platforms_seq'), ?, ?, ?)",
        [(p["id"], json.dumps(p), now) for p in platforms],
    )


if __name__ == "__main__":
    conn = get_conn()
    init_bronze(conn)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS bronze_games_seq")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS bronze_genres_seq")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS bronze_platforms_seq")
    http = build_session()
    logger.info("Fetching from RAWG API...")
    games = fetch_games(http)
    genres = fetch_genres(http)
    platforms = fetch_platforms(http)
    load_bronze(conn, games, genres, platforms)
    conn.close()
    logger.info(
        "Loaded %d games, %d genres, %d platforms.",
        len(games), len(genres), len(platforms),
    )