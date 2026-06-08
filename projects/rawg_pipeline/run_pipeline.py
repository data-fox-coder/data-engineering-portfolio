import os
import subprocess
import time
import logging
import orchestrate

logger = logging.getLogger(__name__)

def run():
    """
    Orchestrates the ETL execution followed by dbt transformation.
    Ensures process isolation and file-system synchronization.
    """
    logger.info("Starting ETL process...")
    orchestrate.run() 
    
    # Synchronization: Wait to ensure file handle release by Python process
    time.sleep(2) 
    
    # Path configuration: Force absolute path for dbt connection
    abs_db_path = os.path.abspath("rawg_data.duckdb")
    os.environ["DBT_DUCKDB_PATH"] = abs_db_path
    
    logger.info(f"Executing dbt with target path: {abs_db_path}")
    
    # Execution: Invoke dbt via subprocess for isolated model processing
    cmd = ["dbt", "run", "--project-dir", "rawg_dbt"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error(f"dbt runtime error: {result.stderr}")
        raise RuntimeError("dbt model execution failed.")
    
    logger.info("dbt model execution successful.")