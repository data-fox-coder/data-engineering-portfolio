# """
# Cat Shelter Pipeline
# ====================
# Extracts cat listings from the Petfinder API, transforms the data
# using pandas, and loads it into a local SQLite database.
# """

# import requests
# import pandas as pd
# import sqlite3
# import os
# from datetime import datetime
# from dotenv import load_dotenv

# load_dotenv()

# # ── 1. CONFIGURATION ──────────────────────────────────────────────────────────

# API_KEY    = os.getenv("PETFINDER_API_KEY")
# API_SECRET = os.getenv("PETFINDER_API_SECRET")
# BASE_URL   = "https://api.petfinder.com/v2"
# DB_PATH    = "data/cats.db"


# # ── 2. EXTRACT ────────────────────────────────────────────────────────────────

# def get_token() -> str:
#     """Fetch a short-lived OAuth token from the Petfinder API."""
#     resp = requests.post(
#         f"{BASE_URL}/oauth2/token",
#         data={
#             "grant_type":    "client_credentials",
#             "client_id":     API_KEY,
#             "client_secret": API_SECRET,
#         },
#     )
#     resp.raise_for_status()
#     return resp.json()["access_token"]


# def fetch_cats(token: str, pages: int = 3) -> list[dict]:
#     """
#     Pull cat listings from the API.
#     `pages` controls how many pages of 100 results to fetch.
#     """
#     headers = {"Authorization": f"Bearer {token}"}
#     animals = []

#     for page in range(1, pages + 1):
#         resp = requests.get(
#             f"{BASE_URL}/animals",
#             headers=headers,
#             params={"type": "cat", "limit": 100, "page": page},
#         )
#         resp.raise_for_status()
#         batch = resp.json().get("animals", [])
#         if not batch:
#             break
#         animals.extend(batch)
#         print(f"  Fetched page {page} — {len(batch)} cats")

#     print(f"Total cats extracted: {len(animals)}")
#     return animals


# # ── 3. TRANSFORM ──────────────────────────────────────────────────────────────

# def transform(animals: list[dict]) -> pd.DataFrame:
#     """
#     Flatten the nested API response and apply cleaning rules.
#     Mirrors the kind of logic you'd write in Power Query M or SSIS.
#     """
#     rows = []
#     for a in animals:
#         rows.append({
#             "id":           a.get("id"),
#             "name":         a.get("name"),
#             "age":          a.get("age"),          # Baby / Young / Adult / Senior
#             "gender":       a.get("gender"),
#             "size":         a.get("size"),
#             "breed_primary":a["breeds"].get("primary") if a.get("breeds") else None,
#             "coat":         a.get("coat"),
#             "status":       a.get("status"),       # adoptable / adopted / found
#             "city":         a["contact"]["address"].get("city")  if a.get("contact") else None,
#             "state":        a["contact"]["address"].get("state") if a.get("contact") else None,
#             "postcode":     a["contact"]["address"].get("postcode") if a.get("contact") else None,
#             "published_at": a.get("published_at"),
#             "extracted_at": datetime.utcnow().isoformat(),
#         })

#     df = pd.DataFrame(rows)

#     # --- cleaning ---
#     df["name"]         = df["name"].str.strip().str.title()
#     df["breed_primary"]= df["breed_primary"].fillna("Unknown")
#     df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)

#     # Drop exact duplicate IDs (re-runs of the pipeline)
#     df = df.drop_duplicates(subset="id")

#     print(f"Rows after transform: {len(df)}")
#     return df


# # ── 4. LOAD ───────────────────────────────────────────────────────────────────

# def load(df: pd.DataFrame, db_path: str = DB_PATH) -> None:
#     """
#     Upsert records into SQLite.
#     Uses INSERT OR REPLACE so re-running the pipeline won't create duplicates.
#     """
#     os.makedirs(os.path.dirname(db_path), exist_ok=True)

#     with sqlite3.connect(db_path) as conn:
#         # to_sql with if_exists="append" is fine here because the table
#         # definition (and UNIQUE constraint on id) is set up in setup_db().
#         df.to_sql("cats", conn, if_exists="append", index=False,
#                   method=_upsert_sqlite)

#     print(f"Loaded {len(df)} rows into {db_path}")


# def _upsert_sqlite(table, conn, keys, data_iter):
#     """Custom to_sql method that issues INSERT OR REPLACE statements."""
#     rows = list(data_iter)
#     placeholders = ", ".join(["?"] * len(keys))
#     sql = f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({placeholders})"
#     conn.executemany(sql, rows)


# def setup_db(db_path: str = DB_PATH) -> None:
#     """Create the cats table if it doesn't already exist."""
#     os.makedirs(os.path.dirname(db_path), exist_ok=True)
#     with sqlite3.connect(db_path) as conn:
#         conn.execute("""
#             CREATE TABLE IF NOT EXISTS cats (
#                 id            INTEGER PRIMARY KEY,
#                 name          TEXT,
#                 age           TEXT,
#                 gender        TEXT,
#                 size          TEXT,
#                 breed_primary TEXT,
#                 coat          TEXT,
#                 status        TEXT,
#                 city          TEXT,
#                 state         TEXT,
#                 postcode      TEXT,
#                 published_at  TEXT,
#                 extracted_at  TEXT
#             )
#         """)
#     print(f"Database ready at {db_path}")


# # ── 5. ORCHESTRATE ────────────────────────────────────────────────────────────

# def run_pipeline(pages: int = 3) -> pd.DataFrame:
#     """End-to-end pipeline: Extract → Transform → Load. Returns the DataFrame."""
#     print("\n=== Cat Shelter Pipeline ===")

#     print("\n[1/3] Extracting from Petfinder API...")
#     token   = get_token()
#     animals = fetch_cats(token, pages=pages)

#     print("\n[2/3] Transforming data...")
#     df = transform(animals)

#     print("\n[3/3] Loading into SQLite...")
#     setup_db()
#     load(df)

#     print("\n✅ Pipeline complete.\n")
#     return df


# if __name__ == "__main__":
#     run_pipeline()