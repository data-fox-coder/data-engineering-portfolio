import logging
import os
import subprocess
from pathlib import Path

import duckdb
from config import DB_PATH
from dotenv import load_dotenv
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

load_dotenv()

# Paths for dbt execution
PROJECT_DIR = Path(__file__).parent
DBT_DIR = PROJECT_DIR / "rawg_dbt"


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
    logger.info("Silver layer transformation complete.")


def _run_gold() -> None:
    logger.info(f"Executing dbt with target path: {DB_PATH}")
    logger.info(f"Targeting dbt project directory path: {DBT_DIR}")

    # Pass --profiles-dir alongside --project-dir so dbt finds profiles.yml,
    # and pass env=os.environ so dbt reads DBT_DUCKDB_PATH from .env
    result = subprocess.run(
        [
            "dbt",
            "run",
            "--project-dir",
            str(DBT_DIR),
            "--profiles-dir",
            str(DBT_DIR),
        ],
        env=os.environ,
        check=False,
    )

    if result.returncode == 0:
        logger.info("dbt model execution successful.")
    else:
        logger.error("dbt model execution failed.")
        raise RuntimeError("dbt execution failed.")


def main() -> None:
    conn = duckdb.connect(str(DB_PATH))

    try:
        _run_bronze(conn)
        _run_silver(conn)
    except Exception as e:  # noqa: BLE001
        logger.error(f"Pipeline execution failed in Bronze/Silver: {e}")
        raise
    finally:
        # Crucial: Closes the DuckDB connection before dbt runs so dbt can acquire the file lock
        conn.close()

    # Runs dbt Gold layer after DuckDB connection is safely closed
    _run_gold()
    logger.info("Pipeline execution completed successfully!")


# Alias for backward compatibility with run_pipeline.py
run = main


if __name__ == "__main__":
    main()