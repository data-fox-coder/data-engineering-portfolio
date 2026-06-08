"""
orchestrate.py
----------------------------------
The master control script that imports modular functions from the 
Bronze and Silver layers and executes the full pipeline end-to-end.
"""
import logging
import os
import duckdb
from dotenv import load_dotenv

# Import our modular components
from rawg_pipeline.bronze.ingest import build_session, fetch_games, fetch_genres, fetch_platforms, load_bronze, init_bronze
from rawg_pipeline.silver.transform import init_silver, transform_games, transform_genres, transform_platforms

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

load_dotenv()
DB_PATH = os.getenv("DB_PATH", "rawg_data.duckdb")

def run_pipeline():
    logger.info("=============================================")
    logger.info("   STARTING END-TO-END RAWG PIPELINE        ")
    logger.info("=============================================")
    
    # Open a single, shared connection for the entire pipeline run
    conn = duckdb.connect(DB_PATH)
    
    try:
        # ------------------------------------------------------------------
        # PHASE 1: BRONZE INGESTION
        # ------------------------------------------------------------------
        logger.info("--- PHASE 1: Executing Bronze Ingestion ---")
        init_bronze(conn)
        
        # Ensure our sequences exist for Bronze
        conn.execute("CREATE SEQUENCE IF NOT EXISTS bronze_games_seq")
        conn.execute("CREATE SEQUENCE IF NOT EXISTS bronze_genres_seq")
        conn.execute("CREATE SEQUENCE IF NOT EXISTS bronze_platforms_seq")
        
        # Fetch data via API session
        http_session = build_session()
        games_data = fetch_games(http_session)
        genres_data = fetch_genres(http_session)
        platforms_data = fetch_platforms(http_session)
        
        # Load raw data to Bronze
        load_bronze(conn, games_data, genres_data, platforms_data)
        
        # ------------------------------------------------------------------
        # PHASE 2: SILVER TRANSFORMATION
        # ------------------------------------------------------------------
        logger.info("--- PHASE 2: Executing Silver Transformation ---")
        init_silver(conn)
        
        # Ensure our sequences exist for Silver
        conn.execute("CREATE SEQUENCE IF NOT EXISTS silver_games_seq")
        conn.execute("CREATE SEQUENCE IF NOT EXISTS silver_genres_seq")
        conn.execute("CREATE SEQUENCE IF NOT EXISTS silver_platforms_seq")
        
        # Run upserts/transformations using the same connection
        transform_games(conn)
        transform_genres(conn)
        transform_platforms(conn)
        
        logger.info("=============================================")
        logger.info("   PIPELINE EXECUTED SUCCESSFULLY 🎉        ")
        logger.info("=============================================")

    except Exception as e:
        logger.error("❌ Pipeline execution failed!", exc_info=True)
        raise e
        
    finally:
        conn.close()
        logger.info("Database connection closed safely.")

if __name__ == "__main__":
    run_pipeline()