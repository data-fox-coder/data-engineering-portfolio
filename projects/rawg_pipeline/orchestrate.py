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
        # Phase 1: Bronze Layer Ingestion
        logger.info("Executing Bronze layer initialization...")
        init_bronze(conn)
        conn.commit()  # Persist schema DDL
        
        # Add your ingestion call here if needed
        # load_bronze(conn, ...)
        conn.commit()  # Flush raw data
        
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