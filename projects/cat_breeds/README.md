# 🐱 Cat Breeds Data Pipeline

A Python ETL pipeline that pulls cat breed data from [TheCatAPI](https://thecatapi.com),
cleans it with **pandas**, stores it in **SQLite**, and generates visualisations with **matplotlib**.

No API key required to get started.

---

## Project Structure

```
cat-pipeline/
├── pipeline.py      # Extract -> Transform -> Load
├── analyse.py       # Queries the DB and produces charts
├── requirements.txt
├── .gitignore
├── data/            # Created automatically — SQLite DB lives here
└── output/          # Created automatically — charts saved here
```

---

## Getting Started in GitHub Codespaces

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the pipeline

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
| **Extract** | Calls TheCatAPI breeds endpoint | HTTP request (like a data source connection) |
| **Transform** | Cleans and flattens the JSON with pandas | Power Query M / SSIS |
| **Load** | Upserts rows into SQLite | A SQL Server `MERGE` statement |

The transform step also parses range strings like `"12 - 15"` into numeric midpoints,
making the data ready for analysis.

### `analyse.py`

Reads from the database and produces six charts:

- Top countries by number of breeds
- Life span distribution (histogram)
- Average trait scores across all breeds
- Intelligence vs energy level (scatter plot)
- Hypoallergenic breed breakdown (pie chart)
- Top 10 longest-lived breeds

---

## Data Fields

Each breed record includes:

| Field | Description |
|---|---|
| `life_span_avg` | Midpoint of the breed's life span range (years) |
| `weight_kg_avg` | Midpoint of the weight range (kg) |
| `intelligence` | Score 1–5 |
| `affection_level` | Score 1–5 |
| `energy_level` | Score 1–5 |
| `hypoallergenic` | 0 or 1 |
| `origin` | Country of origin |
| ...and more | 12 trait scores in total |

---

## Stretch Goals

Once the basics are working, try extending the project:

- [ ] **Add logging** with Python's `logging` module instead of `print()`
- [ ] **Export to Excel** with formatted sheets using `openpyxl`
- [ ] **Write tests** for your transform logic using `pytest`
- [ ] **Schedule the pipeline** to run daily using the `schedule` library
- [ ] **Connect Tableau** by pointing it at the SQLite file or a CSV export
- [ ] **Fetch breed images** using the `/images/search?breed_ids=` endpoint
