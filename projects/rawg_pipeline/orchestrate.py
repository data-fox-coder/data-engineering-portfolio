import logging

import duckdb
from config import DB_PATH
from rawg_pipeline.bronze.ingest import (
    build_session,
    fetch_games,
    fetch_genres,
    fetch_platforms,
    init_bronze,
    load_bronze,
)
from rawg_pipeline.silver.transform import (
    init_silver,
    transform_games,
    transform_genres,
    transform_platforms,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _run_bronze(conn: duckdb.DuckDBPyConnection) -> None:
    logger.info("Executing Bronze layer initialization...")
    init_bronze(conn)
    session = build_session()
    games = fetch_games(session)
    genres = fetch_genres(session)
    platforms = fetch_platforms(session)
    load_bronze(conn, games, genres, platforms)


def _run_silver(conn: duckdb.DuckDBPyConnection) -> None:
    logger.info("Executing Silver layer transformation...")
    init_silver(conn)
    transform_games(conn)
    transform_genres(conn)
    transform_platforms(conn)


def main() -> None:
    conn = duckdb.connect(str(DB_PATH))

    try:
        _run_bronze(conn)
        _run_silver(conn)
    except Exception as e:  # noqa: BLE001
        logger.error(f"Pipeline execution failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()