"""
rawg_pipeline/silver/models.py
-------------------------------
Silver layer: cleaned, typed, deduplicated records.
"""
from datetime import date, datetime

from sqlalchemy import Date, DateTime, Float, Integer, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column

from rawg_pipeline.db import Base


class SilverGame(Base):
    __tablename__ = "silver_games"
    __table_args__ = {"schema": "silver"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, server_default=text("nextval('silver_games_id_seq')"))
    rawg_id: Mapped[int] = mapped_column(Integer, nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    rating: Mapped[float | None] = mapped_column(Float, nullable=True)
    ratings_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    released: Mapped[date | None] = mapped_column(Date, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )


class SilverGenre(Base):
    __tablename__ = "silver_genres"
    __table_args__ = {"schema": "silver"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, server_default=text("nextval('silver_genres_id_seq')"))
    rawg_id: Mapped[int] = mapped_column(Integer, nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    slug: Mapped[str | None] = mapped_column(Text, nullable=True)


class SilverPlatform(Base):
    __tablename__ = "silver_platforms"
    __table_args__ = {"schema": "silver"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, server_default=text("nextval('silver_platforms_id_seq')"))
    rawg_id: Mapped[int] = mapped_column(Integer, nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    slug: Mapped[str | None] = mapped_column(Text, nullable=True)
