"""
tests/test_rawg_pipeline.py
----------------------------
Pytest test suite for the RAWG gaming data pipeline.
Covers bronze ingestion, silver transformation, and db initialisation.

Run from projects/rawg_pipeline/:
    pytest tests/test_rawg_pipeline.py -v
"""

import json
import os
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def in_memory_engine():
    """Create a fresh in-memory DuckDB engine for each test."""
    # Removed ?prefers_ansi=true as it's not a valid DuckDB config
    engine = create_engine(
        "duckdb:///:memory:",
        echo=False,
    )
    yield engine
    
    # Clean up the engine after the test is done
    engine.dispose()

@pytest.fixture(scope="function")
def db_session(in_memory_engine):
    """
    Bootstrap schemas and tables, then return a session.
    Tears down after each test.
    """
    from rawg_pipeline.db import Base

    # Import models so SQLAlchemy registers them before create_all
    from rawg_pipeline.bronze import models as bronze_models  # noqa: F401
    from rawg_pipeline.silver import models as silver_models  # noqa: F401

    with in_memory_engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        
        # THE FIX: Tell DuckDB that 'SERIAL' is just an 'INTEGER'
        # This stops the "Type with name SERIAL does not exist" error.
        conn.execute(text("CREATE TYPE SERIAL AS INTEGER"))
        conn.execute(text("CREATE TYPE BIGSERIAL AS BIGINT"))
        
        # Create sequences for auto-incrementing IDs
        conn.execute(text("CREATE SEQUENCE IF NOT EXISTS bronze_games_id_seq START 1"))
        conn.execute(text("CREATE SEQUENCE IF NOT EXISTS bronze_genres_id_seq START 1"))
        conn.execute(text("CREATE SEQUENCE IF NOT EXISTS bronze_platforms_id_seq START 1"))
        conn.execute(text("CREATE SEQUENCE IF NOT EXISTS silver_games_id_seq START 1"))
        conn.execute(text("CREATE SEQUENCE IF NOT EXISTS silver_genres_id_seq START 1"))
        conn.execute(text("CREATE SEQUENCE IF NOT EXISTS silver_platforms_id_seq START 1"))
        
        conn.commit()

    Base.metadata.create_all(bind=in_memory_engine)

    Session = sessionmaker(bind=in_memory_engine)
    session = Session()
    yield session
    session.close()
    Base.metadata.drop_all(bind=in_memory_engine)


@pytest.fixture
def sample_games():
    """Sample raw game dicts as returned by the RAWG API."""
    return [
        {
            "id": 1,
            "name": "The Witcher 3",
            "rating": 4.66,
            "ratings_count": 5000,
            "released": "2015-05-19",
        },
        {
            "id": 2,
            "name": "Cyberpunk 2077",
            "rating": 3.91,
            "ratings_count": 3200,
            "released": "2020-12-10",
        },
    ]


@pytest.fixture
def sample_genres():
    return [
        {"id": 1, "name": "Action", "slug": "action"},
        {"id": 2, "name": "RPG", "slug": "role-playing-games-rpg"},
    ]


@pytest.fixture
def sample_platforms():
    return [
        {"id": 1, "name": "PC", "slug": "pc"},
        {"id": 2, "name": "PlayStation 5", "slug": "playstation5"},
    ]


# ---------------------------------------------------------------------------
# Bronze layer tests
# ---------------------------------------------------------------------------

class TestBronzeIngest:
    def test_load_bronze_inserts_all_records(self, db_session, sample_games, sample_genres, sample_platforms):
        """load_bronze inserts games, genres, and platforms."""
        from rawg_pipeline.bronze.ingest import load_bronze
        from rawg_pipeline.bronze.models import BronzeGame, BronzeGenre, BronzePlatform

        load_bronze(db_session, sample_games, sample_genres, sample_platforms)

        assert db_session.query(BronzeGame).count() == 2
        assert db_session.query(BronzeGenre).count() == 2
        assert db_session.query(BronzePlatform).count() == 2

    def test_fetch_games_returns_results(self):
        """fetch_games calls RAWG API and returns results list."""
        from rawg_pipeline.bronze.ingest import fetch_games

        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [{"id": 1, "name": "Test Game"}]}
        mock_response.raise_for_status = MagicMock()

        mock_http = MagicMock()
        mock_http.get.return_value = mock_response

        results = fetch_games(mock_http)

        assert len(results) == 1
        assert results[0]["name"] == "Test Game"

    def test_fetch_genres_returns_results(self):
        """fetch_genres calls RAWG API and returns results list."""
        from rawg_pipeline.bronze.ingest import fetch_genres

        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [{"id": 1, "name": "Action"}]}
        mock_response.raise_for_status = MagicMock()

        mock_http = MagicMock()
        mock_http.get.return_value = mock_response

        results = fetch_genres(mock_http)

        assert len(results) == 1
        assert results[0]["name"] == "Action"

    def test_fetch_platforms_returns_results(self):
        """fetch_platforms calls RAWG API and returns results list."""
        from rawg_pipeline.bronze.ingest import fetch_platforms

        mock_response = MagicMock()
        mock_response.json.return_value = {"results": [{"id": 1, "name": "PC"}]}
        mock_response.raise_for_status = MagicMock()

        mock_http = MagicMock()
        mock_http.get.return_value = mock_response

        results = fetch_platforms(mock_http)

        assert len(results) == 1
        
# ---------------------------------------------------------------------------
# Silver transformation tests
# ---------------------------------------------------------------------------

class TestSilverTransform:
    def _seed_bronze(self, db_session, sample_games, sample_genres, sample_platforms):
        """Helper to seed bronze layer before silver tests."""
        from rawg_pipeline.bronze.ingest import load_bronze
        load_bronze(db_session, sample_games, sample_genres, sample_platforms)

    def test_transform_games_creates_silver_records(self, db_session, sample_games, sample_genres, sample_platforms):
        """transform_games creates SilverGame records from bronze."""
        from rawg_pipeline.silver.models import SilverGame
        from rawg_pipeline.silver.transform import transform_games

        self._seed_bronze(db_session, sample_games, sample_genres, sample_platforms)
        transform_games(db_session)

        count = db_session.query(SilverGame).count()
        assert count == 2

    def test_transform_games_correct_fields(self, db_session, sample_games, sample_genres, sample_platforms):
        """Silver games have correctly typed and mapped fields."""
        from rawg_pipeline.silver.models import SilverGame
        from rawg_pipeline.silver.transform import transform_games

        self._seed_bronze(db_session, sample_games, sample_genres, sample_platforms)
        transform_games(db_session)

        game = db_session.query(SilverGame).filter_by(rawg_id=1).first()
        assert game.name == "The Witcher 3"
        assert abs(game.rating - 4.66) < 0.001  # Use approximate equality for floats
        assert game.released == date(2015, 5, 19)

    def test_transform_games_deduplication(self, db_session, sample_games, sample_genres, sample_platforms):
        """Running transform_games twice does not create duplicate silver records."""
        from rawg_pipeline.silver.models import SilverGame
        from rawg_pipeline.silver.transform import transform_games

        self._seed_bronze(db_session, sample_games, sample_genres, sample_platforms)
        transform_games(db_session)
        transform_games(db_session)

        count = db_session.query(SilverGame).count()
        assert count == 2

    def test_transform_genres_creates_silver_records(self, db_session, sample_games, sample_genres, sample_platforms):
        """transform_genres creates SilverGenre records from bronze."""
        from rawg_pipeline.silver.models import SilverGenre
        from rawg_pipeline.silver.transform import transform_genres

        self._seed_bronze(db_session, sample_games, sample_genres, sample_platforms)
        transform_genres(db_session)

        count = db_session.query(SilverGenre).count()
        assert count == 2

    def test_transform_genres_correct_fields(self, db_session, sample_games, sample_genres, sample_platforms):
        """Silver genres have correctly mapped slug and name fields."""
        from rawg_pipeline.silver.models import SilverGenre
        from rawg_pipeline.silver.transform import transform_genres

        self._seed_bronze(db_session, sample_games, sample_genres, sample_platforms)
        transform_genres(db_session)

        genre = db_session.query(SilverGenre).filter_by(rawg_id=1).first()
        assert genre.name == "Action"
        assert genre.slug == "action"

    def test_transform_platforms_creates_silver_records(self, db_session, sample_games, sample_genres, sample_platforms):
        """transform_platforms creates SilverPlatform records from bronze."""
        from rawg_pipeline.silver.models import SilverPlatform
        from rawg_pipeline.silver.transform import transform_platforms

        self._seed_bronze(db_session, sample_games, sample_genres, sample_platforms)
        transform_platforms(db_session)

        count = db_session.query(SilverPlatform).count()
        assert count == 2

    def test_transform_platforms_correct_fields(self, db_session, sample_games, sample_genres, sample_platforms):
        """Silver platforms have correctly mapped slug and name fields."""
        from rawg_pipeline.silver.models import SilverPlatform
        from rawg_pipeline.silver.transform import transform_platforms

        self._seed_bronze(db_session, sample_games, sample_genres, sample_platforms)
        transform_platforms(db_session)

        platform = db_session.query(SilverPlatform).filter_by(rawg_id=1).first()
        assert platform.name == "PC"
        assert platform.slug == "pc"

    def test_transform_games_handles_null_release_date(self, db_session, sample_genres, sample_platforms):
        """Games with no released date are handled gracefully."""
        from rawg_pipeline.bronze.ingest import load_bronze
        from rawg_pipeline.silver.models import SilverGame
        from rawg_pipeline.silver.transform import transform_games

        games = [{"id": 99, "name": "Unreleased Game", "rating": None, "ratings_count": 0, "released": None}]
        load_bronze(db_session, games, sample_genres, sample_platforms)
        transform_games(db_session)

        game = db_session.query(SilverGame).filter_by(rawg_id=99).first()
        assert game is not None
        assert game.released is None

    def test_transform_games_handles_null_rating(self, db_session, sample_genres, sample_platforms):
        """Games with no rating are handled gracefully."""
        from rawg_pipeline.bronze.ingest import load_bronze
        from rawg_pipeline.silver.models import SilverGame
        from rawg_pipeline.silver.transform import transform_games

        games = [{"id": 100, "name": "Unrated Game", "rating": None, "ratings_count": 0, "released": "2024-01-01"}]
        load_bronze(db_session, games, sample_genres, sample_platforms)
        transform_games(db_session)

        game = db_session.query(SilverGame).filter_by(rawg_id=100).first()
        assert game is not None
        assert game.rating is None


# ---------------------------------------------------------------------------
# DB initialisation tests
# ---------------------------------------------------------------------------

class TestDbInit:
    def test_init_db_creates_schemas(self, in_memory_engine):
        """init_db creates bronze and silver schemas."""

        with patch("rawg_pipeline.db.engine", in_memory_engine):
            with in_memory_engine.connect() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
                conn.commit()

            result = in_memory_engine.connect().execute(
                text("SELECT schema_name FROM information_schema.schemata")
            ).fetchall()
            schema_names = [r[0] for r in result]
            assert "bronze" in schema_names
            assert "silver" in schema_names