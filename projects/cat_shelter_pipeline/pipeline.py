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
import pandas as pd
import sqlite3
import os
from datetime import datetime
from dotenv import load_dotenv
import yaml

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

load_dotenv()  # Load environment variables from .env file
with open("config.yml") as f:
    config = yaml.safe_load(f)

API_KEY  = os.getenv("RESCUEGROUPS_API_KEY")
if not API_KEY:
    raise ValueError("RESCUEGROUPS_API_KEY not found. Check your .env file or Codespaces secrets.")

BASE_URL = config["source"]["base_url"]
DB_PATH  = config["layers"]["gold"]["path"]

HEADERS = {
    "Authorization": API_KEY,
    "Content-Type": "application/vnd.api+json",
}

# ── EXTRACT ───────────────────────────────────────────────────────────────────

def fetch_cats(max_pages: int = 5) -> list[dict]:
    """
    Fetch available cat listings from RescueGroups using the
    /public/animals/search/available/ endpoint with a POST body
    filter for cats.

    The API returns a max of 250 records per page. We paginate
    until there are no more results or we hit max_pages.
    """
    if not API_KEY:
        raise ValueError(
            "RESCUEGROUPS_API_KEY is not set. "
            "Add it as a Codespaces secret or in a .env file."
        )

    animals = []
    page = 1

    while page <= max_pages:
        resp = requests.post(
            f"{BASE_URL}/public/animals/search/available/",
            headers=HEADERS,
            params={
                "limit": 250,
                "page":  page,
                "include": "breeds,orgs,locations",
            },
            json={
                "data": {
                    "filters": [
                        {
                            "fieldName": "species.plural",
                            "operation": "equal",
                            "criteria": "cats"
                        }
                    ]
                }
            },
        )
        resp.raise_for_status()
        payload = resp.json()

        batch = payload.get("data", [])
        if not batch:
            break

        # Build a lookup of included objects (breeds, orgs, locations)
        included = _index_included(payload.get("included", []))

        for animal in batch:
            animals.append({"animal": animal, "included": included})

        meta = payload.get("meta", {})
        print(f"  Page {page}: {len(batch)} cats "
              f"(total available: {meta.get('count', '?')})")

        if page >= meta.get("pages", 1):
            break
        page += 1

    print(f"Total records extracted: {len(animals)}")
    return animals

def _index_included(included: list) -> dict:
    """Build a {(type, id): attributes} lookup from the included array."""
    index = {}
    for item in included:
        key = (item["type"], item["id"])
        index[key] = item.get("attributes", {})
    return index


# ── TRANSFORM ─────────────────────────────────────────────────────────────────

def transform(records: list[dict]) -> pd.DataFrame:
    """
    Flatten animal + related data into a clean tabular structure.
    Mirrors the kind of logic you'd write in Power Query M or SSIS.
    """
    rows = []

    for record in records:
        animal   = record["animal"]
        included = record["included"]
        attrs    = animal.get("attributes", {})
        rels     = animal.get("relationships", {})

        # Resolve primary breed name from relationships
        breed_name = _resolve_relationship(rels, "breeds", included, "name")

        # Resolve organisation name
        org_name = _resolve_relationship(rels, "orgs", included, "name")

        # Resolve location city/state
        loc_attrs  = _resolve_relationship_attrs(rels, "locations", included)
        city       = loc_attrs.get("city")  if loc_attrs else None
        state      = loc_attrs.get("state") if loc_attrs else None
        postalcode = loc_attrs.get("postalcode") if loc_attrs else None

        rows.append({
            "id":                  animal.get("id"),
            "name":                attrs.get("name"),
            "age_group":           attrs.get("ageGroup"),
            "age_string":          attrs.get("ageString"),
            "sex":                 attrs.get("sex"),
            "size_group":          attrs.get("sizeGroup"),
            "coat_length":         attrs.get("coatLength"),
            "energy_level":        attrs.get("energyLevel"),
            "activity_level":      attrs.get("activityLevel"),
            "shedding_level":      attrs.get("sheddingLevel"),
            "grooming_needs":      attrs.get("groomingNeeds"),
            "indoor_outdoor":      attrs.get("indoorOutdoor"),
            "is_kids_ok":          attrs.get("isKidsOk"),
            "is_cats_ok":          attrs.get("isCatsOk"),
            "is_dogs_ok":          attrs.get("isDogsOk"),
            "is_housetrained":     attrs.get("isHousetrained"),
            "is_declawed":         attrs.get("isDeclawed"),
            "is_microchipped":     attrs.get("isMicrochipped"),
            "is_special_needs":    attrs.get("isSpecialNeeds"),
            "is_altered":          attrs.get("isAltered"),
            "breed_primary":       breed_name,
            "org_name":            org_name,
            "city":                city,
            "state":               state,
            "postalcode":          postalcode,
            "picture_count":       attrs.get("pictureCount", 0),
            "available_date":      attrs.get("availableDate"),
            "updated_date":        attrs.get("updatedDate"),
            "url":                 attrs.get("url"),
            "extracted_at":        datetime.utcnow().isoformat(),
        })

    df = pd.DataFrame(rows)

    # --- cleaning ---
    df["name"]          = df["name"].str.strip().str.title()
    df["breed_primary"] = df["breed_primary"].fillna("Unknown")
    df["available_date"]= pd.to_datetime(df["available_date"], errors="coerce", utc=True)
    df["updated_date"]  = pd.to_datetime(df["updated_date"],   errors="coerce", utc=True)
    df = df.drop_duplicates(subset="id")

    print(f"Rows after transform: {len(df)}")
    return df


def _resolve_relationship(rels: dict, rel_name: str,
                           included: dict, field: str) -> str | None:
    """Return the first matching included attribute value for a relationship."""
    attrs = _resolve_relationship_attrs(rels, rel_name, included)
    return attrs.get(field) if attrs else None


def _resolve_relationship_attrs(rels: dict, rel_name: str,
                                 included: dict) -> dict | None:
    """Return the attributes dict for the first item in a relationship."""
    rel = rels.get(rel_name, {})
    data = rel.get("data")
    if not data:
        return None
    # data may be a list or a single object
    first = data[0] if isinstance(data, list) else data
    key = (first["type"], str(first["id"]))
    return included.get(key)


# ── LOAD ──────────────────────────────────────────────────────────────────────

def setup_db(db_path: str = DB_PATH) -> None:
    """Create the cats table if it doesn't already exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cats (
                id               TEXT PRIMARY KEY,
                name             TEXT,
                age_group        TEXT,
                age_string       TEXT,
                sex              TEXT,
                size_group       TEXT,
                coat_length      TEXT,
                energy_level     TEXT,
                activity_level   TEXT,
                shedding_level   TEXT,
                grooming_needs   TEXT,
                indoor_outdoor   TEXT,
                is_kids_ok       INTEGER,
                is_cats_ok       INTEGER,
                is_dogs_ok       INTEGER,
                is_housetrained  INTEGER,
                is_declawed      INTEGER,
                is_microchipped  INTEGER,
                is_special_needs INTEGER,
                is_altered       INTEGER,
                breed_primary    TEXT,
                org_name         TEXT,
                city             TEXT,
                state            TEXT,
                postalcode       TEXT,
                picture_count    INTEGER,
                available_date   TEXT,
                updated_date     TEXT,
                url              TEXT,
                extracted_at     TEXT
            )
        """)
    print(f"  Database ready at {db_path}")


def load(df: pd.DataFrame, db_path: str = DB_PATH) -> None:
    """Upsert records into SQLite — safe to re-run."""
    with sqlite3.connect(db_path) as conn:
        df.to_sql("cats", conn, if_exists="append", index=False,
                  method=_upsert_sqlite)
    print(f"  Loaded {len(df)} rows into {db_path}")


def _upsert_sqlite(table, conn, keys, data_iter):
    rows = list(data_iter)
    placeholders = ", ".join(["?"] * len(keys))
    sql = f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({placeholders})"
    conn.executemany(sql, rows)


# ── ORCHESTRATE ───────────────────────────────────────────────────────────────

def run_pipeline(max_pages: int = 5) -> pd.DataFrame:
    """Extract -> Transform -> Load. Returns the DataFrame."""
    print("\n=== Cat Shelter Pipeline (RescueGroups.org) ===")

    print("\n[1/3] Extracting from RescueGroups API...")
    records = fetch_cats(max_pages=max_pages)

    print("\n[2/3] Transforming data...")
    df = transform(records)

    print("\n[3/3] Loading into SQLite...")
    setup_db()
    load(df)

    print("\n✅ Pipeline complete.\n")
    return df


if __name__ == "__main__":
    run_pipeline()