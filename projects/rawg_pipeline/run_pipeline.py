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
    
    # Locate rawg_dbt directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dbt_project_dir = os.path.join(current_dir, "rawg_dbt")
    
    logger.info(f"Targeting dbt project directory path: {dbt_project_dir}")
    
    # Core Fix: Point dbt to the project directory for BOTH the models and the profiles.yml file
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