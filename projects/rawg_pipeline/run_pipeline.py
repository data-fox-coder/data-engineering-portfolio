"""
run_pipeline.py
---------------
Bootstraps the full pipeline on Streamlit Cloud startup:
Runs Bronze & Silver natively, then triggers dbt for Gold.
"""

import os
import sys
import subprocess
import logging

# Import your orchestrator logic directly to avoid multi-process file locks
from orchestrate import run_pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Locate the base directory of the repository
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Adjust paths to match your folder structure exactly
DBT_DIR = os.path.join(BASE_DIR, "rawg_dbt") 
DB_PATH = os.path.join(BASE_DIR, "rawg_data.duckdb")

def run():
    # 1. RUN BRONZE & SILVER NATIVELY
    logger.info("⚡ Executing Python pipeline (Bronze Ingest -> Silver Transform)...")
    try:
        run_pipeline()
    except Exception as e:
        logger.error(f"❌ Python pipeline execution failed: {e}")
        raise RuntimeError("Pipeline failed during Python execution phase.") from e

    # 2. RUN DBT FOR THE GOLD LAYER
    logger.info("🚀 Running dbt gold layer via subprocess...")
    
    # Setup environment variables so profiles.yml reads the exact DB path
    env = os.environ.copy()
    env["DBT_DUCKDB_PATH"] = DB_PATH
    
    # Let the system path locate the 'dbt' executable inside the container runtime
    dbt_command = ["dbt", "run", "--profiles-dir", ".", "--target", "dev"]
    
    result = subprocess.run(
        dbt_command,
        cwd=DBT_DIR, 
        env=env, 
        capture_output=True, 
        text=True
    )
    
    # Log compilation logs so they appear in your Streamlit Cloud log dashboard
    logger.info(result.stdout)
    
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt run failed:\n{result.stderr}")

if __name__ == "__main__":
    run()