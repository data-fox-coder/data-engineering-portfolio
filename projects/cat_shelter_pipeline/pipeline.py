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
from typing import Dict, List, Any
import pandas as pd
import requests
import time
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

load_dotenv()

# Resolve all paths relative to this file, not the working directory
PROJECT_ROOT = Path(__file__).parent


class ExtractionError(Exception):
    """Raised when extraction fails in a non-recoverable way."""
    pass


def load_config() -> Dict:
    """Load configuration from config.yml relative to this file."""
    config_path = PROJECT_ROOT / "config.yml"
    with config_path.open("r") as fh:
        return yaml.safe_load(fh)


def validate_config(config: Dict) -> None:
    """Basic validation to ensure required config sections exist."""
    required_sections = ["source", "layers", "logging"]
    missing = [key for key in required_sections if key not in config]
    if missing:
        raise ValueError(f"Missing required config sections: {missing}")

    # Minimal nested checks
    if "base_url" not in config["source"]:
        raise ValueError("Config 'source.base_url' is required.")
    for layer in ["bronze", "silver", "gold"]:
        if layer not in config["layers"] or "path" not in config["layers"][layer]:
            raise ValueError(f"Config 'layers.{layer}.path' is required.")


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

def extract_cat_data(config: Dict) -> List[Dict[str, Any]]:
    """
    Fetch available cat listings from the RescueGroups v5 API.
    Falls back gracefully to local mock data if the API is unreachable (e.g., cloud IP blocks).
    """
    api_key = os.getenv("RESCUEGROUPS_API_KEY")
    if not api_key:
        logging.error("RESCUEGROUPS_API_KEY is not set. Aborting extraction.")
        raise ExtractionError("Missing RESCUEGROUPS_API_KEY environment variable.")

    base_url = config["source"]["base_url"]
    api_url: str | None = f"{base_url}/public/animals/search/available/cats"

    headers = {
        "Authorization": api_key,
        "Content-Type": "application/vnd.api+json",
        "Accept": "application/vnd.api+json",
    }

    page_size = config["source"].get("page_size", 25)
    body = {
        "data": {
            "filterProcessing": "1",
            "limit": page_size,
        }
    }

    all_records: List[Dict[str, Any]] = []
    page_count = 1

    logging.info(f"Extracting data starting at: {api_url}")

    while api_url:
        try:
            # The initial search requires a POST to send the body payload.
            # Subsequent pages use standard GET requests because the API's 'links.next' URL
            # handles the state and offsets automatically.
            if page_count == 1:
                response = requests.post(
                    api_url,
                    headers=headers,
                    json=body,
                    timeout=15,
                )
            else:
                response = requests.get(api_url, headers=headers, timeout=15)

            response.raise_for_status()
            data = response.json()

            records = data.get("data", [])
            all_records.extend(records)
            logging.info(
                f"Page {page_count}: Extracted {len(records)} records from live API."
            )

            # Fetch the next page URL provided by the API, guarding against empty strings
            next_url = data.get("links", {}).get("next")
            api_url = next_url or None

            if api_url:
                page_count += 1
                time.sleep(0.3)  # Polite pause between API calls

        except requests.exceptions.RequestException as e:
            # If it drops on the very first request, trigger the full mock fallback
            if page_count == 1:
                logging.warning(
                    f"⚠️ Live API connection dropped ({e}). "
                    "Switching to local mock dataset for development."
                )

                mock_file_path = PROJECT_ROOT / "mock_rescuegroups_raw.json"

                if mock_file_path.exists():
                    with mock_file_path.open("r") as fh:
                        mock_data = json.load(fh)
                    logging.info(
                        f"Successfully loaded {len(mock_data)} mock records "
                        f"from local Bronze backup."
                    )
                    return mock_data
                else:
                    logging.error(
                        f"Mock file not found at {mock_file_path}. Cannot proceed."
                    )
                    raise ExtractionError(
                        "API unreachable and mock file missing; extraction failed."
                    )
            # If it drops midway through pagination, save whatever records we already successfully downloaded
            else:
                logging.warning(
                    f"⚠️ Live API connection dropped on page {page_count} ({e}). "
                    f"Returning {len(all_records)} records collected so far."
                )
                break

    return all_records


def _atomic_json_write(output_file: Path, data: Any) -> None:
    """Write JSON atomically via a temporary file."""
    tmp = output_file.with_suffix(output_file.suffix + ".tmp")
    with tmp.open("w") as fh:
        json.dump(data, fh, indent=2)
    tmp.replace(output_file)


def save_bronze(raw_data: List[Dict[str, Any]], config: Dict) -> None:
    """Persist raw API response as JSON to the bronze layer."""
    bronze_path = PROJECT_ROOT / config["layers"]["bronze"]["path"]
    bronze_path.mkdir(parents=True, exist_ok=True)
    output_file = bronze_path / "cats_raw.json"

    _atomic_json_write(output_file, raw_data)

    logging.info(f"Bronze: saved {len(raw_data)} raw records to {output_file}")


# ---------------------------------------------------------------------------
# Transform → Silver
# ---------------------------------------------------------------------------

def transform_cat_data(raw_data: List[Dict[str, Any]], config: Dict) -> pd.DataFrame:
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


def _atomic_parquet_write(df: pd.DataFrame, output_file: Path) -> None:
    """Write Parquet atomically via a temporary file."""
    tmp = output_file.with_suffix(output_file.suffix + ".tmp")
    df.to_parquet(tmp, index=False)
    tmp.replace(output_file)


def save_silver(df: pd.DataFrame, config: Dict) -> None:
    """Persist transformed DataFrame as Parquet to the silver layer."""
    if df.empty:
        logging.warning("Silver: DataFrame is empty, skipping save.")
        return

    silver_path = PROJECT_ROOT / config["layers"]["silver"]["path"]
    silver_path.mkdir(parents=True, exist_ok=True)
    output_file = silver_path / "cats_clean.parquet"

    _atomic_parquet_write(df, output_file)
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
        # Kept as 'append' to avoid dropping existing data; schema changes should
        # be managed explicitly if needed.
        df.head(0).to_sql(table_name, conn, if_exists="append", index=False)

        # Upsert row by row using SQLite's INSERT OR REPLACE.
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
    try:
        config = load_config()
        validate_config(config)
    except Exception as e:
        logging.basicConfig(level=logging.ERROR)
        logging.error(f"Failed to load/validate config: {e}")
        return

    setup_logging(config)

    logging.info("=== Cat Shelter ETL Pipeline starting ===")

    # Extract
    try:
        raw_data = extract_cat_data(config)
    except ExtractionError as e:
        logging.error(f"Pipeline aborted: extraction failed — {e}")
        return

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
