# RAWG Gaming Data Pipeline

A data engineering portfolio project implementing a medallion architecture pipeline
ingesting gaming data from the [RAWG API](https://rawg.io/apidocs).

[![Live Dashboard](https://img.shields.io/badge/Live%20Dashboard-Streamlit-ff4b4b?logo=streamlit)](https://data-engineering-portfolio-mxxbvanhcjuvkrgtjhemzr.streamlit.app)

## Architecture

```
RAWG API
   │
   ▼
Bronze (Python + DuckDB)
   │  Raw JSON stored in DuckDB bronze schema
   ▼
Silver (Python + DuckDB)        PySpark (optional scale-out layer)
   │  Cleaned, typed,               │  Same bronze source, transforms
   │  deduplicated records          │  via Spark DataFrames, writes Parquet
   ▼                               ▼
Gold (dbt + DuckDB)          data/spark/ (Parquet)
   │  Aggregated, analytics-ready models built and tested by dbt
   ▼
Dashboard (Streamlit + Plotly)
      Live visualisations of gold layer data
```

| Layer     | Managed by      | Storage                 | Description                           |
|-----------|-----------------|-------------------------|---------------------------------------|
| Bronze    | Python / DuckDB | DuckDB `bronze.*`       | Raw API responses, append-only        |
| Silver    | Python / DuckDB | DuckDB `silver.*`       | Cleaned, typed, deduplicated          |
| PySpark   | PySpark         | Parquet (`data/spark/`) | Scale-out alternative to silver       |
| Gold      | dbt             | DuckDB `gold.*`         | Aggregated, analytics-ready           |
| Dashboard | Streamlit       | Reads from Gold         | Interactive Plotly visualisations     |

## Setup

### Prerequisites

PySpark requires Java 17 or above. Install it before running the PySpark layer:

```bash
sudo apt-get install -y default-jdk
```

This is a system-level dependency and is not included in `requirements.txt`.

### Installation

```bash
pip install -r requirements.txt
cp .env.example .env          # add your RAWG_API_KEY

# Run the Python ingestion layers
python -m rawg_pipeline.bronze.ingest
python -m rawg_pipeline.silver.transform

# Optional: run the PySpark transformation layer (requires Java)
python -m rawg_pipeline.spark.transform

# Run dbt gold models
cd rawg_dbt
dbt build --profiles-dir .

# Run the dashboard
streamlit run app.py
```

## PySpark Layer

The PySpark layer is a modular, scale-out alternative to the SQLAlchemy silver transform.
It reads the same bronze DuckDB tables, applies identical cleaning and typing logic using
Spark DataFrames, and writes Parquet output to `data/spark/`.

In production, PySpark would replace the SQLAlchemy silver layer when dataset size
outgrows what DuckDB can handle locally. Both approaches are included here to demonstrate
proficiency with both paradigms.

| Output                  | Description                       |
|-------------------------|-----------------------------------|
| `data/spark/games/`     | Typed game records (Parquet)      |
| `data/spark/genres/`    | Genre reference data (Parquet)    |
| `data/spark/platforms/` | Platform reference data (Parquet) |

## dbt Models

| Model                   | Layer | Description                           |
|-------------------------|-------|---------------------------------------|
| `gold_top_rated_games`  | Gold  | Top rated games ranked by user rating |
| `gold_genre_summary`    | Gold  | Genre reference data                  |
| `gold_platform_summary` | Gold  | Platform reference data               |

## Testing

```bash
# Python unit tests
pytest tests/ -v

# dbt model tests
cd rawg_dbt
dbt test --profiles-dir .
```

## Environment Variables

| Variable       | Description                                    |
|----------------|------------------------------------------------|
| `RAWG_API_KEY` | Your RAWG API key                              |
| `DB_PATH`      | DuckDB file path (default: `rawg_data.duckdb`) |