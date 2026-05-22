"""
rawg_pipeline/silver/transform.py
----------------------------------
Reads raw JSON from bronze layer, cleans and types the data,
and upserts into the silver DuckDB schema.
"""
import json
import logging
import os
from datetime import date, datetime, timezone

import duckdb
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "rawg_data.duckdb")


def get_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(DB_PATH)


def init_silver(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS silver.silver_games (
            id           INTEGER PRIMARY KEY,
            rawg_id      INTEGER NOT NULL UNIQUE,
            name         TEXT NOT NULL,
            rating       DOUBLE,
            ratings_count INTEGER,
            released     DATE,
            updated_at   TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS silver.silver_genres (
            id      INTEGER PRIMARY KEY,
            rawg_id INTEGER NOT NULL UNIQUE,
            name    TEXT NOT NULL,
            slug    TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS silver.silver_platforms (
            id      INTEGER PRIMARY KEY,
            rawg_id INTEGER NOT NULL UNIQUE,
            name    TEXT NOT NULL,
            slug    TEXT
        )
    """)


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def transform_games(conn: duckdb.DuckDBPyConnection) -> None:
    rows = conn.execute("SELECT raw_json FROM bronze.bronze_games").fetchall()
    seen = set()
    records = []
    for (raw,) in rows:
        data = json.loads(raw)
        rawg_id = data["id"]
        if rawg_id in seen:
            continue
        seen.add(rawg_id)
        records.append((
            rawg_id,
            data.get("name", ""),
            data.get("rating"),
            data.get("ratings_count"),
            parse_date(data.get("released")),
            datetime.now(timezone.utc),
        ))
    conn.executemany("""
        INSERT INTO silver.silver_games (id, rawg_id, name, rating, ratings_count, released, updated_at)
        VALUES (nextval('silver_games_seq'), ?, ?, ?, ?, ?, ?)
        ON CONFLICT (rawg_id) DO UPDATE SET
            name = excluded.name,
            rating = excluded.rating,
            ratings_count = excluded.ratings_count,
            released = excluded.released,
            updated_at = excluded.updated_at
    """, records)


def transform_genres(conn: duckdb.DuckDBPyConnection) -> None:
    rows = conn.execute("SELECT raw_json FROM bronze.bronze_genres").fetchall()
    seen = set()
    records = []
    for (raw,) in rows:
        data = json.loads(raw)
        rawg_id = data["id"]
        if rawg_id in seen:
            continue
        seen.add(rawg_id)
        records.append((rawg_id, data.get("name", ""), data.get("slug")))
    conn.executemany("""
        INSERT INTO silver.silver_genres (id, rawg_id, name, slug)
        VALUES (nextval('silver_genres_seq'), ?, ?, ?)
        ON CONFLICT (rawg_id) DO UPDATE SET
            name = excluded.name,
            slug = excluded.slug
    """, records)


def transform_platforms(conn: duckdb.DuckDBPyConnection) -> None:
    rows = conn.execute("SELECT raw_json FROM bronze.bronze_platforms").fetchall()
    seen = set()
    records = []
    for (raw,) in rows:
        data = json.loads(raw)
        rawg_id = data["id"]
        if rawg_id in seen:
            continue
        seen.add(rawg_id)
        records.append((rawg_id, data.get("name", ""), data.get("slug")))
    conn.executemany("""
        INSERT INTO silver.silver_platforms (id, rawg_id, name, slug)
        VALUES (nextval('silver_platforms_seq'), ?, ?, ?)
        ON CONFLICT (rawg_id) DO UPDATE SET
            name = excluded.name,
            slug = excluded.slug
    """, records)


if __name__ == "__main__":
    conn = get_conn()
    init_silver(conn)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS silver_games_seq")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS silver_genres_seq")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS silver_platforms_seq")
    logger.info("Transforming bronze -> silver...")
    transform_games(conn)
    transform_genres(conn)
    transform_platforms(conn)
    conn.close()
    logger.info("Silver layer complete.")