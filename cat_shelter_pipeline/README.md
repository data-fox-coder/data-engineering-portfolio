# 🐱 Cat Shelter Data Pipeline

A Python ETL pipeline that pulls cat adoption listings from the [Petfinder API](https://www.petfinder.com/developers/), cleans the data with **pandas**, stores it in **SQLite**, and generates visualisations with **matplotlib**.

---

## Project Structure

```
cat-pipeline/
├── pipeline.py      # Extract → Transform → Load
├── analyse.py       # Queries the DB and produces charts
├── requirements.txt
├── .env.example     # Copy this to .env and add your credentials
├── .gitignore
├── data/            # Created automatically — SQLite DB lives here
└── output/          # Created automatically — charts saved here
```

---

## Getting Started in GitHub Codespaces

### 1. Get a free Petfinder API key

Register at [https://www.petfinder.com/developers/](https://www.petfinder.com/developers/) to get your `API_KEY` and `API_SECRET`.

### 2. Set up your environment

```bash
# Install dependencies
pip install -r requirements.txt

# Create your secrets file
cp .env.example .env
```

Open `.env` and replace the placeholder values with your real API credentials.

> ⚠️ The `.env` file is in `.gitignore` — it will never be committed to GitHub.

### 3. Run the pipeline

```bash
# Step 1: Extract, transform, and load data into SQLite
python pipeline.py

# Step 2: Generate charts from the database
python analyse.py
```

Charts are saved to the `output/` folder.

---

## What Each Script Does

### `pipeline.py`

| Stage | What happens | Python equivalent of... |
|---|---|---|
| **Extract** | Calls the Petfinder API with OAuth | HTTP request (like a data source connection) |
| **Transform** | Cleans and flattens the JSON with pandas | Power Query M / SSIS |
| **Load** | Upserts rows into SQLite | A SQL Server `MERGE` statement |

### `analyse.py`

Reads from the database and produces five charts:

- Age band distribution
- Top 10 breeds
- Gender split
- New listings per week (time series)
- Top states by listing count

---

## Stretch Goals

Once the basics are working, try extending the project:

- [ ] **Schedule the pipeline** to run daily using the `schedule` library
- [ ] **Export to Excel** with formatted sheets using `openpyxl`
- [ ] **Add a map** of shelter locations using `folium`
- [ ] **Connect Tableau** by pointing it at the SQLite file or a CSV export
- [ ] **Add logging** with Python's `logging` module instead of `print()`
- [ ] **Write tests** for your transform logic using `pytest`
