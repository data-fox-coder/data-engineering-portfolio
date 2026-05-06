"""
Cat Shelter Pipeline — RescueGroups.org v5 API
================================================
Extracts available cat listings from the RescueGroups.org API,
transforms them through a bronze / silver / gold medallion architecture,
and loads into a local SQLite database with upsert semantics.

Requires:
    RESCUEGROUPS_API_KEY set as a Codespaces secret (or in a .env file)

Usage:
    python pipeline.py
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, List
import pandas as pd
import requests
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

load_dotenv()

# Resolve all paths relative to this file, not the working directory
PROJECT_ROOT = Path(__file__).parent


def load_config() -> Dict:
    """Load configuration from config.yml relative to this file."""
    config_path = PROJECT_ROOT / "config.yml"
    with config_path.open("r") as fh:
        return yaml.safe_load(fh)


def setup_logging(config: Dict) -> None:
    """Configure logging to both console and file."""
    log_path = PROJECT_ROOT / config["logging"]["log_path"]
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=config["logging"]["level"],
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path),
        ],
    )


# ---------------------------------------------------------------------------
# Extract → Bronze
# ---------------------------------------------------------------------------

def extract_cat_data(config: Dict) -> List[Dict]:
    """
    Fetch available cat listings from the RescueGroups v5 API.
    Returns the raw list of animal dicts from the API response.
    """
    api_key = os.getenv("RESCUEGROUPS_API_KEY")
    if not api_key:
        logging.error("RESCUEGROUPS_API_KEY is not set. Aborting extraction.")
        return []

    base_url = config["source"]["base_url"]
    api_url = f"{base_url}/public/animals/search/available/cats"

    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json",
    }

    body = {
        "data": {
            "filterProcessing": "1",
            "filterRadius": {
                "miles": config["source"].get("radius_miles", 50),
                "postalcode": config["source"].get("postalcode", "10001"),
            },
        }
    }

    logging.info(f"Extracting data from: {api_url}")
    try:
        response = requests.post(api_url, headers=headers, json=body, timeout=30)
        response.raise_for_status()
        raw_data = response.json().get("data", [])
        logging.info(f"Extracted {len(raw_data)} records from API.")
        return raw_data
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        return []


def save_bronze(raw_data: List[Dict], config: Dict) -> None:
    """Persist raw API response as JSON to the bronze layer."""
    bronze_path = PROJECT_ROOT / config["layers"]["bronze"]["path"]
    bronze_path.mkdir(parents=True, exist_ok=True)
    output_file = bronze_path / "cats_raw.json"

    with output_file.open("w") as fh:
        json.dump(raw_data, fh, indent=2)

    logging.info(f"Bronze: saved {len(raw_data)} raw records to {output_file}")


# ---------------------------------------------------------------------------
# Transform → Silver
# ---------------------------------------------------------------------------

def transform_cat_data(raw_data: List[Dict], config: Dict) -> pd.DataFrame:
    """
    Normalise raw API data and apply column selection and deduplication
    as configured in config.yml (layers.silver).
    Returns a clean DataFrame ready for the gold layer.
    """
    if not raw_data:
        logging.warning("No records to transform.")
        return pd.DataFrame()

    logging.info(f"Transforming {len(raw_data)} records...")

    df = pd.json_normalize(raw_data)

    # Standardise column names: lowercase, dots/spaces → underscores
    df.columns = [
        col.lower().replace(".", "_").replace(" ", "_") for col in df.columns
    ]

    # Apply column selection from config if specified
    fields_to_keep: List[str] = config["layers"]["silver"].get("fields_to_keep", [])
    if fields_to_keep:
        available = [f for f in fields_to_keep if f in df.columns]
        missing = [f for f in fields_to_keep if f not in df.columns]
        if missing:
            logging.warning(f"Configured fields not found in API response: {missing}")
        df = df[available]

    # Deduplicate on primary key if configured
    if config["layers"]["silver"].get("deduplicate") and "id" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["id"])
        dropped = before - len(df)
        if dropped:
            logging.info(f"Silver: dropped {dropped} duplicate records.")

    logging.info(f"Transformation complete. {len(df)} records, {len(df.columns)} columns.")
    return df


def save_silver(df: pd.DataFrame, config: Dict) -> None:
    """Persist transformed DataFrame as Parquet to the silver layer."""
    if df.empty:
        logging.warning("Silver: DataFrame is empty, skipping save.")
        return

    silver_path = PROJECT_ROOT / config["layers"]["silver"]["path"]
    silver_path.mkdir(parents=True, exist_ok=True)
    output_file = silver_path / "cats_clean.parquet"

    df.to_parquet(output_file, index=False)
    logging.info(f"Silver: saved {len(df)} records to {output_file}")


# ---------------------------------------------------------------------------
# Load → Gold (SQLite upsert)
# ---------------------------------------------------------------------------

def load_cat_data(df: pd.DataFrame, config: Dict) -> None:
    """
    Upsert the silver DataFrame into the gold SQLite database.
    Uses INSERT OR REPLACE semantics keyed on the 'id' column.
    """
    if df.empty:
        logging.warning("Gold: DataFrame is empty, skipping load.")
        return

    if "id" not in df.columns:
        logging.error("Gold: 'id' column not found — cannot upsert without a primary key.")
        return

    db_path = PROJECT_ROOT / config["layers"]["gold"]["path"]
    db_path.parent.mkdir(parents=True, exist_ok=True)

    engine = create_engine(f"sqlite:///{db_path}")
    table_name = "cats"

    # SQLite cannot bind Python lists or dicts — serialise any such columns
    # to JSON strings so they survive the round-trip and remain readable.
    df = df.copy()
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, (list, dict))).any():
            df[col] = df[col].apply(
                lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v
            )

    with engine.begin() as conn:
        # Ensure the table exists with the correct schema by doing an initial
        # to_sql on an empty slice — this is a no-op if the table already exists.
        df.head(0).to_sql(table_name, conn, if_exists="append", index=False)

        # Upsert row by row using SQLite's INSERT OR REPLACE.
        # This replaces any existing row with the same primary key and
        # inserts new rows, leaving unmatched rows untouched.
        placeholders = ", ".join([f":{col}" for col in df.columns])
        columns = ", ".join(df.columns)
        upsert_sql = text(
            f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"
        )
        records = df.to_dict(orient="records")
        conn.execute(upsert_sql, records)

    logging.info(f"Gold: upserted {len(df)} records into '{table_name}' at {db_path}")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def main() -> None:
    config = load_config()
    setup_logging(config)

    logging.info("=== Cat Shelter ETL Pipeline starting ===")

    # Extract
    raw_data = extract_cat_data(config)
    if not raw_data:
        logging.error("Pipeline aborted: extraction returned no data.")
        return
    save_bronze(raw_data, config)

    # Transform
    df = transform_cat_data(raw_data, config)
    if df.empty:
        logging.error("Pipeline aborted: transformation produced an empty DataFrame.")
        return
    save_silver(df, config)

    # Load
    load_cat_data(df, config)

    logging.info("=== Cat Shelter ETL Pipeline completed successfully ===")


if __name__ == "__main__":
    main()