"""
Cat Shelter Pipeline — RescueGroups.org v5 API
================================================
Extracts available cat listings from the RescueGroups.org API,
transforms them with pandas, and loads into a local SQLite database.

Requires:
    RESCUEGROUPS_API_KEY set as a Codespaces secret (or in a .env file)

Usage:
    python pipeline.py
"""

import requests
import yaml
import os
import pandas as pd
from dotenv import load_dotenv
import logging
from typing import List, Dict

# --- Setup Logging (The Audit Trail) ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables
load_dotenv()

def load_config():
    """Loads configuration parameters from config.yml."""
    config_path = os.path.join(os.path.dirname(__file__), 'config.yml')
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def extract_cat_data() -> list:
    """Fetches cat data from the RescueGroups API."""
    config = load_config()
    
    # Dynamically build the full search endpoint using your YAML structure
    base_url = config['source']['base_url']
    api_url = f"{base_url}/public/animals/search/available/cats"
    
    # Retrieve API key from environment variables
    api_key = os.getenv("RESCUEGROUPS_API_KEY")
    if not api_key:
        logging.error("API Key missing! Please set RESCUEGROUPS_API_KEY in your environment.")
        return []

    headers = {
        "Authorization": api_key,
        "Content-Type": "application/json"
    }

    # Example filter body required by RescueGroups API
    body = {
        "data": {
            "filterProcessing": "1",
            "filterRadius": {
                "miles": 50,
                "postalcode": "90210"
            }
        }
    }

    try:
        logging.info(f"Fetching data from API: {api_url}")
        response = requests.post(api_url, headers=headers, json=body)
        response.raise_for_status()
        
        # Access the 'data' key from the JSON response
        result = response.json()
        return result.get('data', [])
        
    except requests.exceptions.RequestException as e:
        logging.error(f"API Error: {e}")
        return []

def transform_cat_data(raw_data: List[Dict]) -> pd.DataFrame:
    """
    Standardizes the cat data.
    Takes the raw JSON (List of Dicts) and returns a clean Pandas DataFrame.
    """
    if not raw_data:
        logging.warning("No data found to transform.")
        return pd.DataFrame()

    logging.info(f"Transforming {len(raw_data)} records...")
    
    # Use your Analyst skills to flatten the nested API data
    df = pd.json_normalize(raw_data)
    
    # Clean up column names for SQL compatibility: 
    # lower_case_with_underscores, no dots or spaces
    df.columns = [
        col.lower().replace(".", "_").replace(" ", "_") 
        for col in df.columns
    ]
    
    logging.info(f"Transformation complete. Columns: {list(df.columns[:5])}...")
    return df

def load_cat_data(df: pd.DataFrame):
    """Saves the transformed data to a CSV file (placeholder for database loading)."""
    if df.empty:
        logging.warning("DataFrame is empty. Skipping load stage.")
        return

    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    output_path = os.path.join(output_dir, 'cats_cleaned.csv')
    df.to_csv(output_path, index=False)
    logging.info(f"Successfully loaded data to {output_path}")

def main():
    logging.info("Starting Cat Shelter ETL Pipeline...")
    raw_data = extract_cat_data()
    if raw_data:
        df = transform_cat_data(raw_data)
        load_cat_data(df)
        logging.info("Pipeline completed successfully!")
    else:
        logging.error("Pipeline failed at extraction stage.")

if __name__ == "__main__":
    main()