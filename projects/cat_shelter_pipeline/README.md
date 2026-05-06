# Cat Shelter Pipeline 🐱

An end-to-end ETL data pipeline that extracts real-world cat adoption data from the [RescueGroups.org v5 API](https://rescuegroups.org/services/adoptable-pet-data-api/), transforms it using pandas, and loads it into a local SQLite database for analysis.

Built to demonstrate practical data engineering skills in Python, using production-oriented patterns from a background in SSIS, Power Query M, and SQL Server.

---

## What It Does

| Stage | Tool | What it does |
|---|---|---|
| **Extract** | `requests` | Authenticates with the RescueGroups v5 API, paginates through available cat listings, resolves related data (breeds, orgs, locations) from the JSON `included` array |
| **Transform** | `pandas` | Flattens nested JSON into a clean tabular structure, cleans and standardises fields, deduplicates on animal ID |
| **Load** | `sqlite3` | Upserts records into a local SQLite database — safe to re-run without creating duplicates |

---

## Project Structure

```
cat_shelter_pipeline/
├── pipeline.py         # Full ETL pipeline: extract → transform → load
├── config.yml          # Config-driven settings (API URL, DB path, page size)
├── .env.example        # Template for API credentials
└── README.md
```

---

## Skills Demonstrated

- Authenticating with and paginating a REST API (`requests`)
- Parsing and normalising nested, relational JSON into a flat DataFrame
- Data cleaning and transformation with `pandas`
- Config-driven pipeline design with `yaml`
- Secret management with `python-dotenv`
- Upsert logic (`INSERT OR REPLACE`) for idempotent loads
- Structured, modular Python code (not just a notebook)

---

## Getting Started

### 1. Get a RescueGroups API key

Sign up at [rescuegroups.org](https://rescuegroups.org) to request access to the v5 API.

### 2. Configure your environment

```bash
cp .env.example .env
```

Edit `.env` and add your key:

```
RESCUEGROUPS_API_KEY=your_api_key_here
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the pipeline

```bash
python pipeline.py
```

The pipeline will print progress by page and confirm the row count loaded into SQLite.

---

## Configuration

Key settings are managed in `config.yml`:

```yaml
source:
  base_url: https://api.rescuegroups.org/v5
  page_size: 100       # max 250

layers:
  gold:
    path: data/cats_shelter.db
```

---

## Planned Enhancements

- Medallion architecture (Bronze → Silver → Gold layers with JSON and Parquet intermediate storage)
- GitHub Actions scheduled run
- RAWG gaming pipeline as a second project

---

## Dependencies

See `requirements.txt` in the repo root.

Key packages: `requests`, `pandas`, `python-dotenv`, `pyyaml`