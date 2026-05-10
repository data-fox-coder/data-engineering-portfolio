"""
rawg_pipeline/db.py
-------------------
SQLAlchemy engine and session factory.
Uses DuckDB as the storage backend — bronze and silver tables are written here,
and dbt reads from the same file to build gold models.
"""
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker

load_dotenv()

DB_PATH = Path(os.getenv("DB_PATH", "data/rawg.duckdb"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

engine = create_engine(
    f"duckdb:///{DB_PATH}",
    echo=False,
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


def init_db() -> None:
    """
    Create schemas and tables for the Python-managed layers (bronze, silver).
    Gold is owned by dbt — do not create gold tables here.
    """
    from rawg_pipeline.bronze import models as bronze_models  # noqa: F401
    from rawg_pipeline.silver import models as silver_models  # noqa: F401

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        conn.commit()

    Base.metadata.create_all(bind=engine)
