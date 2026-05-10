"""
rawg_pipeline/bronze/models.py
-------------------------------
Bronze layer: append-only raw API responses stored as JSON strings.
"""
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Integer, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column

from rawg_pipeline.db import Base


class BronzeGame(Base):
    __tablename__ = "bronze_games"
    __table_args__ = {"schema": "bronze"}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, server_default=text("nextval('bronze_games_id_seq')"))
    rawg_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    raw_json: Mapped[str] = mapped_column(Text, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )


class BronzeGenre(Base):
    __tablename__ = "bronze_genres"
    __table_args__ = {"schema": "bronze"}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, server_default=text("nextval('bronze_genres_id_seq')"))
    rawg_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    raw_json: Mapped[str] = mapped_column(Text, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )


class BronzePlatform(Base):
    __tablename__ = "bronze_platforms"
    __table_args__ = {"schema": "bronze"}

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, server_default=text("nextval('bronze_platforms_id_seq')"))
    rawg_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    raw_json: Mapped[str] = mapped_column(Text, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )
