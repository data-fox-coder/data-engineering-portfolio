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
Silver (Python + DuckDB)
   │  Cleaned, typed, deduplicated records in DuckDB silver schema
   ▼
Gold (dbt + DuckDB)
   │  Aggregated, analytics-ready models built and tested by dbt
   ▼
Dashboard (Streamlit + Plotly)
      Live visualisations of gold layer data
```

| Layer     | Managed by      | Storage           | Description                       |
|-----------|-----------------|-------------------|-----------------------------------|
| Bronze    | Python / DuckDB | DuckDB `bronze.*` | Raw API responses, append-only    |
| Silver    | Python / DuckDB | DuckDB `silver.*` | Cleaned, typed, deduplicated      |
| Gold      | dbt             | DuckDB `gold.*`   | Aggregated, analytics-ready       |
| Dashboard | Streamlit       | Reads from Gold   | Interactive Plotly visualisations |

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env          # add your RAWG_API_KEY

# Run the Python ingestion layers
python -m rawg_pipeline.bronze.ingest
python -m rawg_pipeline.silver.transform

# Run dbt gold models
cd rawg_dbt
dbt build --profiles-dir .

# Run the dashboard
streamlit run app.py
```

## dbt Models

| Model                  | Layer | Description                           |
|------------------------|-------|---------------------------------------|
| `gold_top_rated_games` | Gold  | Top rated games ranked by user rating |
| `gold_genre_summary`   | Gold  | Genre reference data                  |

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