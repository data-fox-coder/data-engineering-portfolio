"""
rawg_pipeline/silver/transform.py
----------------------------------
Reads raw JSON from bronze layer, cleans and types the data,
and upserts into the silver DuckDB schema.
"""
import json
import logging
from datetime import date
from typing import Callable, Type

from sqlalchemy.orm import Session

from rawg_pipeline.bronze.models import BronzeGame, BronzeGenre, BronzePlatform
from rawg_pipeline.db import SessionLocal, init_db
from rawg_pipeline.silver.models import SilverBase, SilverGame, SilverGenre, SilverPlatform

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def parse_date(value: str | None) -> date | None:
    """Parse an ISO date string, returning None if missing or invalid."""
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _transform(
    session: Session,
    bronze_model: Type,
    silver_model: Type[SilverBase],
    build_record: Callable[[dict], SilverBase],
) -> None:
    """
    Generic transform helper.
    Reads all bronze rows, deduplicates by rawg_id,
    skips existing silver records, and inserts new ones.
    """
    rows = session.query(bronze_model).all()
    seen = set()
    for row in rows:
        data = json.loads(row.raw_json)
        rawg_id = data["id"]
        if rawg_id in seen:
            continue
        seen.add(rawg_id)
        existing = session.query(silver_model).filter_by(rawg_id=rawg_id).first()
        if existing:
            continue
        session.add(build_record(data))
    session.commit()


def transform_games(session: Session) -> None:
    """Transform bronze games into typed silver records."""
    _transform(
        session,
        bronze_model=BronzeGame,
        silver_model=SilverGame,
        build_record=lambda data: SilverGame(
            rawg_id=data["id"],
            name=data.get("name", ""),
            rating=data.get("rating"),
            ratings_count=data.get("ratings_count"),
            released=parse_date(data.get("released")),
        ),
    )


def transform_genres(session: Session) -> None:
    """Transform bronze genres into typed silver records."""
    _transform(
        session,
        bronze_model=BronzeGenre,
        silver_model=SilverGenre,
        build_record=lambda data: SilverGenre(
            rawg_id=data["id"],
            name=data.get("name", ""),
            slug=data.get("slug"),
        ),
    )


def transform_platforms(session: Session) -> None:
    """Transform bronze platforms into typed silver records."""
    _transform(
        session,
        bronze_model=BronzePlatform,
        silver_model=SilverPlatform,
        build_record=lambda data: SilverPlatform(
            rawg_id=data["id"],
            name=data.get("name", ""),
            slug=data.get("slug"),
        ),
    )


if __name__ == "__main__":
    init_db()
    with SessionLocal() as session:
        logger.info("Transforming bronze -> silver...")
        transform_games(session)
        transform_genres(session)
        transform_platforms(session)
        logger.info("Silver layer complete.")