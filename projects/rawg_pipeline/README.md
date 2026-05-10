# RAWG Gaming Data Pipeline

A data engineering portfolio project implementing a medallion architecture pipeline
ingesting gaming data from the [RAWG API](https://rawg.io/apidocs).

## Architecture

```
RAWG API
   │
   ▼
Bronze (Python + SQLAlchemy)
   │  Raw JSON stored in DuckDB bronze schema
   ▼
Silver (Python + SQLAlchemy)
   │  Cleaned, typed, deduplicated records in DuckDB silver schema
   ▼
Gold (dbt + DuckDB)
      Aggregated, analytics-ready models built and tested by dbt
```

| Layer  | Managed by          | Storage              | Description                        |
|--------|---------------------|----------------------|------------------------------------|
| Bronze | Python / SQLAlchemy | DuckDB `bronze.*`    | Raw API responses, append-only     |
| Silver | Python / SQLAlchemy | DuckDB `silver.*`    | Cleaned, typed, deduplicated       |
| Gold   | dbt                 | DuckDB `gold.*`      | Aggregated, analytics-ready        |

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env          # add your RAWG_API_KEY

# Run the Python ingestion layers
python -m rawg_pipeline.bronze.ingest
python -m rawg_pipeline.silver.transform

# Run dbt gold models
cd dbt_rawg
dbt build --profiles-dir .
```

## dbt Models

| Model                  | Layer  | Description                                      |
|------------------------|--------|--------------------------------------------------|
| `stg_games`            | Silver | Cleaned game records with typed fields           |
| `stg_genres`           | Silver | Cleaned genre reference data                     |
| `stg_platforms`        | Silver | Cleaned platform reference data                  |
| `top_rated_games`      | Gold   | Top 100 games by rating (min 10 ratings filter)  |
| `yearly_release_summary` | Gold | Aggregated stats per release year               |

## Testing

```bash
# Python unit tests
pytest tests/ -v

# dbt model tests
cd dbt_rawg
dbt test --profiles-dir .
```

## Environment Variables

| Variable       | Description                                        |
|----------------|----------------------------------------------------|
| `RAWG_API_KEY` | Your RAWG API key                                  |
| `DB_PATH`      | DuckDB file path (default: `data/rawg.duckdb`)     |
