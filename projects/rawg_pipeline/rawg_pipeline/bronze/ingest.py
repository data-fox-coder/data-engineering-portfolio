"""
rawg_pipeline/bronze/ingest.py
-------------------------------
Fetches raw game, genre, and platform data from the RAWG API
and stores append-only JSON strings in the bronze DuckDB schema.
"""
import json
import logging
import os
import time
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

# ------------------------------------------------------------------ #
# MAX_RECORDS controls how many games are ingested per run.           #
# Change this one number to scale up or down, no other edits needed.  #
# At page_size=40 per request, 1000 games = 25 API calls.            #
# RAWG free tier allows 20,000 requests/month, so this is well within #
# safe limits.                                                         #
# ------------------------------------------------------------------ #
MAX_RECORDS = 1000

# Half-second pause between paginated requests. This keeps the pipeline
# well within RAWG's rate limits and avoids 429 (Too Many Requests)
# errors. 1000 games takes ~12 seconds of sleep total, which is fine.
REQUEST_SLEEP_SECONDS = 0.5


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


def fetch_games(
    http: requests.Session,
    page_size: int = 40,
    max_records: int = MAX_RECORDS,
) -> list[dict]:
    """
    Fetch up to max_records games from the RAWG API using pagination.

    RAWG returns a 'next' URL in each response pointing to the next page.
    We follow those URLs until either we've collected max_records games,
    or the API returns null for 'next' (no more pages left).

    The params dict is cleared after the first request because RAWG bakes
    all query parameters (including the API key) into the 'next' URL.
    Passing params again would duplicate them and could break the request.
    """
    url = f"{BASE_URL}/games"
    params = {"key": API_KEY, "page_size": page_size}
    all_games: list[dict] = []

    while url and len(all_games) < max_records:
        response = http.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        all_games.extend(data.get("results", []))
        url = data.get("next")  # None on the last page; stops the loop
        params = {}             # Next URL already has all params baked in

        logger.info("Fetched %d games so far...", len(all_games))

        # Pause between requests to respect RAWG's rate limits.
        # Only sleep if there are more pages to fetch.
        if url and len(all_games) < max_records:
            time.sleep(REQUEST_SLEEP_SECONDS)

    # Trim any overshoot: the final page may push us slightly over max_records
    # e.g. fetching 25 pages of 40 gives 1000, but we guard against edge cases.
    return all_games[:max_records]


def fetch_genres(http: requests.Session) -> list[dict]:
    """
    Fetch all genres from the RAWG API using pagination.

    Genres are a small, bounded list (typically ~20 entries) but we
    paginate correctly in case RAWG adds more over time.
    """
    url = f"{BASE_URL}/genres"
    params = {"key": API_KEY}
    all_genres: list[dict] = []

    while url:
        response = http.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        all_genres.extend(data.get("results", []))
        url = data.get("next")
        params = {}

        if url:
            time.sleep(REQUEST_SLEEP_SECONDS)

    return all_genres


def fetch_platforms(http: requests.Session) -> list[dict]:
    """
    Fetch all platforms from the RAWG API using pagination.

    Platforms typically run to several pages, so pagination is needed
    to capture the full list (consoles, PC, mobile, etc.).
    """
    url = f"{BASE_URL}/platforms"
    params = {"key": API_KEY}
    all_platforms: list[dict] = []

    while url:
        response = http.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        all_platforms.extend(data.get("results", []))
        url = data.get("next")
        params = {}

        if url:
            time.sleep(REQUEST_SLEEP_SECONDS)

    return all_platforms


def load_bronze(
    conn: duckdb.DuckDBPyConnection,
    games: list[dict],
    genres: list[dict],
    platforms: list[dict],
) -> None:
    """
    Insert new records into bronze tables, skipping any rawg_id already present.

    We use INSERT OR IGNORE (via a WHERE NOT EXISTS guard) to make the
    ingestion idempotent: re-running the pipeline won't create duplicate rows
    for records already stored. This is important now that we're ingesting
    large volumes, since accidentally doubling 1000 rows would silently corrupt
    downstream silver/gold counts.
    """
    now = datetime.now(timezone.utc)

    # Filter out any rawg_ids already in the table before inserting.
    # This is more efficient than INSERT OR IGNORE on a large batch because
    # DuckDB handles the deduplication in a single set operation.
    existing_game_ids = {
        row[0]
        for row in conn.execute(
            "SELECT rawg_id FROM bronze.bronze_games"
        ).fetchall()
    }
    new_games = [g for g in games if g["id"] not in existing_game_ids]

    existing_genre_ids = {
        row[0]
        for row in conn.execute(
            "SELECT rawg_id FROM bronze.bronze_genres"
        ).fetchall()
    }
    new_genres = [g for g in genres if g["id"] not in existing_genre_ids]

    existing_platform_ids = {
        row[0]
        for row in conn.execute(
            "SELECT rawg_id FROM bronze.bronze_platforms"
        ).fetchall()
    }
    new_platforms = [p for p in platforms if p["id"] not in existing_platform_ids]

    if new_games:
        conn.executemany(
            "INSERT INTO bronze.bronze_games (id, rawg_id, raw_json, ingested_at) "
            "VALUES (nextval('bronze_games_seq'), ?, ?, ?)",
            [(g["id"], json.dumps(g), now) for g in new_games],
        )

    if new_genres:
        conn.executemany(
            "INSERT INTO bronze.bronze_genres (id, rawg_id, raw_json, ingested_at) "
            "VALUES (nextval('bronze_genres_seq'), ?, ?, ?)",
            [(g["id"], json.dumps(g), now) for g in new_genres],
        )

    if new_platforms:
        conn.executemany(
            "INSERT INTO bronze.bronze_platforms (id, rawg_id, raw_json, ingested_at) "
            "VALUES (nextval('bronze_platforms_seq'), ?, ?, ?)",
            [(p["id"], json.dumps(p), now) for p in new_platforms],
        )

    logger.info(
        "Inserted %d new games, %d new genres, %d new platforms "
        "(%d games / %d genres / %d platforms already existed, skipped).",
        len(new_games), len(new_genres), len(new_platforms),
        len(games) - len(new_games),
        len(genres) - len(new_genres),
        len(platforms) - len(new_platforms),
    )


if __name__ == "__main__":
    conn = get_conn()
    init_bronze(conn)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS bronze_games_seq")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS bronze_genres_seq")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS bronze_platforms_seq")
    http = build_session()

    logger.info("Fetching up to %d games from RAWG API...", MAX_RECORDS)
    games = fetch_games(http)
    genres = fetch_genres(http)
    platforms = fetch_platforms(http)

    load_bronze(conn, games, genres, platforms)
    conn.close()

    logger.info(
        "Ingestion complete: %d games, %d genres, %d platforms fetched from API.",
        len(games), len(genres), len(platforms),
    )