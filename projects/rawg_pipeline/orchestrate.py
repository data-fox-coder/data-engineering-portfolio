import duckdb
import logging
from rawg_pipeline.bronze.ingest import init_bronze, load_bronze
from rawg_pipeline.silver.transform import init_silver, transform_games, transform_genres, transform_platforms

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = "rawg_data.duckdb"

def run():
    logger.info("Initializing Medallion pipeline execution.")
    conn = duckdb.connect(DB_PATH)
    
    try:
        # Audit: Log database file location for diagnostic purposes
        path_check = conn.execute("SELECT current_setting('database_path')").fetchone()[0]
        logger.info(f"Database connection established at: {path_check}")

        # Phase 1: Bronze Layer Ingestion
        init_bronze(conn)
        conn.commit()  # Persist schema DDL to disk
        
        # Ingestion logic should follow here
        conn.commit()  # Flush ingested records to storage
        
        # Phase 2: Silver Layer Transformation
        init_silver(conn)
        conn.commit()  # Ensure schema registration prior to dbt model execution
        
        transform_games(conn)
        transform_genres(conn)
        transform_platforms(conn)
        
        conn.commit()  # Commit structural transformations
        logger.info("Pipeline execution completed successfully.")
        
    except Exception as e:
        logger.error(f"Pipeline execution failure: {e}")
        raise
    finally:
        conn.close()
        logger.info("Database connection terminated.")

if __name__ == "__main__":
    run()