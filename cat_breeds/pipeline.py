"""
Cat Breeds Pipeline
====================
Extracts cat breed data from TheCatAPI (no API key required),
transforms it with pandas, and loads it into a local SQLite database.

"""

# ── IMPORT LIBARIES AND SET UP CONFIGURATION ─────────────────────────────────────────────────────────────

# %%

import requests
import pandas as pd
import sqlite3
import os
from datetime import datetime

BASE_URL = "https://api.thecatapi.com/v1"
DB_PATH  = "data/breeds.db"

# %%

# ── EXTRACT ───────────────────────────────────────────────────────────────────

# %%

def fetch_breeds() -> list[dict]:
    """Fetch all cat breeds from TheCatAPI. No authentication required."""
    resp = requests.get(f"{BASE_URL}/breeds")
    resp.raise_for_status()
    breeds = resp.json()
    print(f"  Extracted {len(breeds)} breeds from TheCatAPI")
    return breeds

# Run function and print first record to verify it works
raw_data = fetch_breeds()
print(raw_data[0]) # Look at the first cat breed

# %%

# ── TRANSFORM ─────────────────────────────────────────────────────────────────

def transform(breeds: list[dict]) -> pd.DataFrame:
    """
    Flatten the API response and clean the data.
    Mirrors the kind of logic I'd write in Power Query M or SSIS.
    """
    rows = []
    for b in breeds:
        # Parse life_span: "12 - 15" -> take the midpoint as a number
        life_span_raw = b.get("life_span", "")
        life_span_avg = _parse_range_midpoint(life_span_raw)

        # Parse weight: {"metric": "3 - 5"} -> midpoint in kg
        weight_raw = b.get("weight", {}).get("metric", "")
        weight_avg = _parse_range_midpoint(weight_raw)

        rows.append({
            "id":               b.get("id"),
            "name":             b.get("name"),
            "origin":           b.get("origin"),
            "country_code":     b.get("country_code"),
            "life_span_raw":    life_span_raw,
            "life_span_avg":    life_span_avg,
            "weight_kg_avg":    weight_avg,
            "temperament":      b.get("temperament"),
            "indoor":           b.get("indoor", 0),
            "lap":              b.get("lap", 0),
            "hypoallergenic":   b.get("hypoallergenic", 0),
            "hairless":         b.get("hairless", 0),
            "rare":             b.get("rare", 0),
            "natural":          b.get("natural", 0),
            "short_legs":       b.get("short_legs", 0),
            # Trait scores (all 1-5)
            "adaptability":     b.get("adaptability"),
            "affection_level":  b.get("affection_level"),
            "child_friendly":   b.get("child_friendly"),
            "dog_friendly":     b.get("dog_friendly"),
            "energy_level":     b.get("energy_level"),
            "grooming":         b.get("grooming"),
            "health_issues":    b.get("health_issues"),
            "intelligence":     b.get("intelligence"),
            "shedding_level":   b.get("shedding_level"),
            "social_needs":     b.get("social_needs"),
            "stranger_friendly":b.get("stranger_friendly"),
            "vocalisation":     b.get("vocalisation"),
            "wikipedia_url":    b.get("wikipedia_url"),
            "extracted_at":     datetime.utcnow().isoformat(),
        })

    df = pd.DataFrame(rows)

    # Clean up text columns
    df["name"]        = df["name"].str.strip().str.title()
    df["origin"]      = df["origin"].str.strip()
    df["temperament"] = df["temperament"].str.strip()

    print(f"  Transformed into {len(df)} rows with {len(df.columns)} columns")
    return df


def _parse_range_midpoint(value: str) -> float | None:
    """Convert a '12 - 15' string into the numeric midpoint 13.5."""
    try:
        parts = [float(x.strip()) for x in value.split("-")]
        return sum(parts) / len(parts)
    except (ValueError, AttributeError):
        return None


# ── LOAD ──────────────────────────────────────────────────────────────────────

def setup_db(db_path: str = DB_PATH) -> None:
    """Create the breeds table if it doesn't already exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS breeds (
                id                TEXT PRIMARY KEY,
                name              TEXT,
                origin            TEXT,
                country_code      TEXT,
                life_span_raw     TEXT,
                life_span_avg     REAL,
                weight_kg_avg     REAL,
                temperament       TEXT,
                indoor            INTEGER,
                lap               INTEGER,
                hypoallergenic    INTEGER,
                hairless          INTEGER,
                rare              INTEGER,
                natural           INTEGER,
                short_legs        INTEGER,
                adaptability      INTEGER,
                affection_level   INTEGER,
                child_friendly    INTEGER,
                dog_friendly      INTEGER,
                energy_level      INTEGER,
                grooming          INTEGER,
                health_issues     INTEGER,
                intelligence      INTEGER,
                shedding_level    INTEGER,
                social_needs      INTEGER,
                stranger_friendly INTEGER,
                vocalisation      INTEGER,
                wikipedia_url     TEXT,
                extracted_at      TEXT
            )
        """)
    print(f"  Database ready at {db_path}")


def load(df: pd.DataFrame, db_path: str = DB_PATH) -> None:
    """Upsert breed records into SQLite."""
    with sqlite3.connect(db_path) as conn:
        df.to_sql("breeds", conn, if_exists="append", index=False,
                  method=_upsert_sqlite)
    print(f"  Loaded {len(df)} rows into {db_path}")


def _upsert_sqlite(table, conn, keys, data_iter):
    """INSERT OR REPLACE to handle re-runs cleanly."""
    rows = list(data_iter)
    placeholders = ", ".join(["?"] * len(keys))
    sql = f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({placeholders})"
    conn.executemany(sql, rows)


# ── ORCHESTRATE ───────────────────────────────────────────────────────────────

def run_pipeline() -> pd.DataFrame:
    """Extract -> Transform -> Load. Returns the DataFrame."""
    print("\n=== Cat Breeds Pipeline ===")

    print("\n[1/3] Extracting from TheCatAPI...")
    breeds = fetch_breeds()

    print("\n[2/3] Transforming data...")
    df = transform(breeds)

    print("\n[3/3] Loading into SQLite...")
    setup_db()
    load(df)

    print("\n✅ Pipeline complete.\n")
    return df


if __name__ == "__main__":
    run_pipeline()
