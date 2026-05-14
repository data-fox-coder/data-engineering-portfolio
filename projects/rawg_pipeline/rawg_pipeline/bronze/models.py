"""
rawg_pipeline/bronze/models.py
-------------------------------
SQLAlchemy ORM models for the bronze DuckDB schema.
Stores append-only raw JSON strings from the RAWG API.
"""
from datetime import datetime
from sqlalchemy import Integer, Text, DateTime, func, text
from sqlalchemy.orm import Mapped, mapped_column
from rawg_pipeline.db import Base


class BronzeBase(Base):
    """
    Abstract base for all bronze tables.
    DuckDB does not support SERIAL/AUTOINCREMENT natively,
    so we use explicit sequences for primary key generation.
    """
    __abstract__ = True

    rawg_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    raw_json: Mapped[str] = mapped_column(Text, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )


class BronzeGame(BronzeBase):
    __tablename__ = "bronze_games"
    __table_args__ = {"schema": "bronze", "extend_existing": True}
    id: Mapped[int] = mapped_column(
        Integer, primary_key=True,
        server_default=text("nextval('bronze_games_id_seq')"),
    )


class BronzeGenre(BronzeBase):
    __tablename__ = "bronze_genres"
    __table_args__ = {"schema": "bronze", "extend_existing": True}
    id: Mapped[int] = mapped_column(
        Integer, primary_key=True,
        server_default=text("nextval('bronze_genres_id_seq')"),
    )


class BronzePlatform(BronzeBase):
    __tablename__ = "bronze_platforms"
    __table_args__ = {"schema": "bronze", "extend_existing": True}
    id: Mapped[int] = mapped_column(
        Integer, primary_key=True,
        server_default=text("nextval('bronze_platforms_id_seq')"),
    )