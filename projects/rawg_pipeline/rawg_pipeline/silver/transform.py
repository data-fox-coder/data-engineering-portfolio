"""
rawg_pipeline/silver/transform.py
----------------------------------
Reads raw JSON from bronze layer, cleans and types the data,
and upserts into the silver DuckDB schema.
"""
import json
from datetime import date

from sqlalchemy.orm import Session

from rawg_pipeline.bronze.models import BronzeGame, BronzeGenre, BronzePlatform
from rawg_pipeline.db import SessionLocal, init_db
from rawg_pipeline.silver.models import SilverGame, SilverGenre, SilverPlatform


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def transform_games(session: Session) -> None:
    rows = session.query(BronzeGame).all()
    seen = set()
    for row in rows:
        data = json.loads(row.raw_json)
        rawg_id = data["id"]
        if rawg_id in seen:
            continue
        seen.add(rawg_id)
        existing = session.query(SilverGame).filter_by(rawg_id=rawg_id).first()
        if existing:
            continue
        session.add(SilverGame(
            rawg_id=rawg_id,
            name=data.get("name", ""),
            rating=data.get("rating"),
            ratings_count=data.get("ratings_count"),
            released=parse_date(data.get("released")),
        ))
    session.commit()


def transform_genres(session: Session) -> None:
    rows = session.query(BronzeGenre).all()
    seen = set()
    for row in rows:
        data = json.loads(row.raw_json)
        rawg_id = data["id"]
        if rawg_id in seen:
            continue
        seen.add(rawg_id)
        existing = session.query(SilverGenre).filter_by(rawg_id=rawg_id).first()
        if existing:
            continue
        session.add(SilverGenre(
            rawg_id=rawg_id,
            name=data.get("name", ""),
            slug=data.get("slug"),
        ))
    session.commit()


def transform_platforms(session: Session) -> None:
    rows = session.query(BronzePlatform).all()
    seen = set()
    for row in rows:
        data = json.loads(row.raw_json)
        rawg_id = data["id"]
        if rawg_id in seen:
            continue
        seen.add(rawg_id)
        existing = session.query(SilverPlatform).filter_by(rawg_id=rawg_id).first()
        if existing:
            continue
        session.add(SilverPlatform(
            rawg_id=rawg_id,
            name=data.get("name", ""),
            slug=data.get("slug"),
        ))
    session.commit()


if __name__ == "__main__":
    init_db()
    with SessionLocal() as session:
        print("Transforming bronze -> silver...")
        transform_games(session)
        transform_genres(session)
        transform_platforms(session)
        print("Silver layer complete.")
