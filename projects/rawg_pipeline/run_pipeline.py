"""
run_pipeline.py
---------------
Bootstraps the full pipeline on Streamlit Cloud startup:
bronze ingest -> silver transform -> dbt gold layer.
"""

import os
import sys
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(__file__)
DBT_DIR = os.path.join(BASE_DIR, "rawg_dbt")
DB_PATH = os.path.join(BASE_DIR, "rawg_data.duckdb")

def run():
    logger.info("Running bronze ingest...")
    subprocess.run([sys.executable, "-m", "rawg_pipeline.bronze.ingest"], cwd=BASE_DIR, check=True)

    logger.info("Running silver transform...")
    subprocess.run([sys.executable, "-m", "rawg_pipeline.silver.transform"], cwd=BASE_DIR, check=True)

    logger.info("Running dbt gold layer...")
    env = os.environ.copy()
    env["DBT_DUCKDB_PATH"] = DB_PATH
    subprocess.run(
        [sys.executable, "-m", "dbt", "run", "--profiles-dir", DBT_DIR],
        cwd=DBT_DIR,
        env=env,
        check=True
    )

if __name__ == "__main__":
    run()
