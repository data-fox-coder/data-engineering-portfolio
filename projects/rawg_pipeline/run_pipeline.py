import os
import subprocess
import time
import logging
import gc  # Core addition for explicit memory/file handle flush
import orchestrate

# 1. Importing the centralized paths from config.py
from config import DB_PATH

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run():
    """
    Orchestrates the ETL execution followed by dbt transformation.
    Ensures process isolation and file-system synchronization.
    """
    logger.info("Starting ETL process...")
    orchestrate.run() 
    
    # Core Fix: Force Python to run garbage collection and release DuckDB file handles
    gc.collect()
    time.sleep(3) 
    
    # 2. Fix: Direct os.environ to the absolute, imported DB_PATH instead of guessing with abspath()
    os.environ["DBT_DUCKDB_PATH"] = DB_PATH
    
    logger.info(f"Executing dbt with target path: {DB_PATH}")
    
    # Locate rawg_dbt directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dbt_project_dir = os.path.join(current_dir, "rawg_dbt")
    
    logger.info(f"Targeting dbt project directory path: {dbt_project_dir}")
    
    cmd = [
        "dbt", "run", 
        "--project-dir", dbt_project_dir,
        "--profiles-dir", dbt_project_dir
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error(f"dbt runtime error: {result.stderr}")
        raise RuntimeError("dbt model execution failed.")
    
    logger.info("dbt model execution successful.")

if __name__ == "__main__":
    run()