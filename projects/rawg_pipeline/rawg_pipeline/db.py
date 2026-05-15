import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from rawg_pipeline.bronze.models import BronzeGame, BronzeGenre, BronzePlatform  # noqa: F401
from rawg_pipeline.silver.models import SilverGame, SilverGenre, SilverPlatform  # noqa: F401

load_dotenv()

# DuckDB connection string
DATABASE_URL = os.getenv("DATABASE_URL", "duckdb:///rawg_data.duckdb")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def init_db():
    """Initializes the database, creating schemas, sequences, and tables."""
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))

        # Prepare DuckDB sequences used by SQLAlchemy server defaults.
        conn.execute(text("CREATE SEQUENCE IF NOT EXISTS bronze_games_id_seq START 1;"))
        conn.execute(text("CREATE SEQUENCE IF NOT EXISTS bronze_genres_id_seq START 1;"))
        conn.execute(text("CREATE SEQUENCE IF NOT EXISTS bronze_platforms_id_seq START 1;"))
        conn.execute(text("CREATE SEQUENCE IF NOT EXISTS silver_games_id_seq START 1;"))
        conn.execute(text("CREATE SEQUENCE IF NOT EXISTS silver_genres_id_seq START 1;"))
        conn.execute(text("CREATE SEQUENCE IF NOT EXISTS silver_platforms_id_seq START 1;"))
        conn.commit()

    Base.metadata.create_all(bind=engine)


def tear_down_db():
    """Tears down the database by removing the file."""
    db_path = DATABASE_URL.replace("duckdb:///", "")
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Removed database file: {db_path}")
    else:
        print(f"Database file not found: {db_path}")


def clean_db_file():
    """Removes the DuckDB database file if it exists."""
    db_path = DATABASE_URL.replace("duckdb:///", "")
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Removed database file: {db_path}")
    else:
        print(f"Database file not found: {db_path}")