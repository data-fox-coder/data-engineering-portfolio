# 🐱 Cat Shelter Pipeline — RescueGroups.org

A Python ETL pipeline that pulls available cat listings from the
[RescueGroups.org v5 API](https://api.rescuegroups.org/v5/public/docs),
transforms the data with **pandas**, stores it in **SQLite**, and generates
visualisations with **matplotlib**.

---

## Project Structure

```
cat-shelter-pipeline/
├── pipeline.py      # Extract -> Transform -> Load
├── analyse.py       # Queries the DB and produces charts
├── requirements.txt
├── data/            # Created automatically — SQLite DB lives here
└── output/          # Created automatically — charts saved here
```

---

## Setup in GitHub Codespaces

### 1. Add your API key as a Codespaces secret

Go to **github.com → Settings → Codespaces → Secrets** and add:

| Name | Value |
|---|---|
| `RESCUEGROUPS_API_KEY` | Your RescueGroups API key |

This makes it available automatically as an environment variable inside your Codespace
without ever touching your code or repo.

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the pipeline

```bash
python pipeline.py   # Extract, transform, load
python analyse.py    # Generate charts
```

---

## What the Pipeline Does

### `pipeline.py` — Extract → Transform → Load

| Stage | Detail |
|---|---|
| **Extract** | Calls `/public/animals/search/available/cats/` with pagination (250 per page) |
| **Transform** | Flattens nested JSON, resolves breed/org/location relationships, cleans text |
| **Load** | Upserts into SQLite — safe to re-run without creating duplicates |

The API uses an `included` array for related data (breeds, organisations, locations).
The transform step resolves these relationships — similar to a SQL JOIN.

### `analyse.py` — Visualisations

Six charts are produced in the `output/` folder:

- Age group distribution
- Top 10 breeds in shelters
- Gender split
- Top 10 states by available cats
- Energy level breakdown
- Compatibility & characteristics (% OK with kids/cats/dogs, housetrained, microchipped)

---

## Stretch Goals

- [ ] Add logging with Python's `logging` module
- [ ] Export a summary report to Excel with `openpyxl`
- [ ] Add a map of shelter locations using `folium`
- [ ] Filter by location using the API's radius search feature
- [ ] Connect Tableau to the SQLite file for interactive dashboards
- [ ] Write tests for the transform logic using `pytest`