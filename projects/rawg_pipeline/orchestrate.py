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

# Removed the redundant path definition since they are now centralized in config.py
# DB_PATH = "rawg_data.duckdb"


def run():
    logger.info("Initializing Medallion pipeline execution.")
    conn = duckdb.connect(DB_PATH)

    try:
        # Phase 1: Bronze Layer Ingestion
        logger.info("Executing Bronze layer initialization...")
        init_bronze(conn)
        conn.commit()  # Persist schema and sequences DDL

        # Core fix: Instantiate the network session and fetch live API data
        logger.info("Connecting to RAWG API and fetching raw datasets...")
        http_session = build_session()

        raw_games = fetch_games(http_session)
        raw_genres = fetch_genres(http_session)
        raw_platforms = fetch_platforms(http_session)

        # Load data into the database
        load_bronze(conn, raw_games, raw_genres, raw_platforms)
        conn.commit()  # Flush raw data to disk

        # Phase 2: Silver Layer Transformation
        logger.info("Executing Silver layer transformations...")
        init_silver(conn)
        conn.commit()  # Ensure schema registration

        transform_games(conn)
        transform_genres(conn)
        transform_platforms(conn)

        conn.commit()  # Final structural commit
        logger.info("Pipeline execution completed successfully.")

    except Exception as e:
        logger.error(f"Pipeline execution failure: {e}")
        raise
    finally:
        conn.close()
        logger.info("Database connection terminated.")


if __name__ == "__main__":
    run()
