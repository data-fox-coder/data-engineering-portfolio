# RAWG Gaming Data Pipeline

A data engineering portfolio project implementing a medallion architecture pipeline ingesting gaming data from the [RAWG API](https://rawg.io/apidocs).

[![Live Dashboard](https://img.shields.io/badge/Live%20Dashboard-Streamlit-ff4b4b?logo=streamlit)](https://data-engineering-portfolio-mxxbvanhcjuvkrgtjhemzr.streamlit.app)

## Architecture

```
RAWG API
   │
   ▼
Bronze (Python + DuckDB)
   │  Raw JSON stored in DuckDB bronze schema via orchestrator
   ▼
Silver (Python + DuckDB)        PySpark (optional scale-out layer)
   │  Cleaned, typed,               │  Same bronze source, transforms
   │  deduplicated records          │  via Spark DataFrames, writes Parquet
   ▼                                ▼
Gold (dbt + DuckDB)          data/spark/ (Parquet)
   │  Aggregated, analytics-ready models built by dbt in main_gold schema
   ▼
Dashboard (Streamlit + Plotly)
      Live, self-bootstrapping visualisations of gold layer data
```

| Layer     | Managed by      | Storage                 | Description                                       |
|-----------|-----------------|-------------------------|---------------------------------------------------|
| Bronze    | Python / DuckDB | DuckDB `bronze.*`       | Raw API responses, append-only                    |
| Silver    | Python / DuckDB | DuckDB `silver.*`       | Cleaned, typed, deduplicated                      |
| PySpark   | PySpark         | Parquet (`data/spark/`) | Scale-out alternative to silver                   |
| Gold      | dbt             | DuckDB `main_gold.*`    | Aggregated, analytics-ready reporting layer       |
| Dashboard | Streamlit       | Reads from Gold         | Full-width interactive Plotly analytical panels   |

---

## Setup

### Prerequisites

PySpark requires Java 17 or above. Install it before running the PySpark layer:

```bash
sudo apt-get install -y default-jdk
```

*Note: This is a system-level dependency and is not included in `requirements.txt`.*

### Installation & Execution

```bash
# Clone the repository and navigate to the project root
pip install -r requirements.txt
cp .env.example .env          # Add your RAWG_API_KEY
```

#### Option A: Run the End-to-End Orchestrated Pipeline
You can trigger the entire operational back-end data pipeline sequentially (Bronze Ingest -> Silver Transform -> dbt Gold compilation) with a single script execution:

```bash
python orchestrate.py
```

#### Option B: Run the Self-Bootstrapping App Directly
The web application features lazy-initialization logic optimized for ephemeral container hosting. If the target `rawg_data.duckdb` storage file is missing on startup, it will automatically bootstrap all back-end steps natively before launching the front-end interface:

```bash
streamlit run app.py
```

---

## PySpark Layer

The PySpark layer is a modular, scale-out alternative to the core Python silver transform. It reads the same bronze DuckDB tables, applies identical cleaning and typing logic using Spark DataFrames, and writes Parquet output to `data/spark/`.

In production, PySpark would replace the local DuckDB silver layer when dataset scale outgrows local compute limits. Both approaches are included here to demonstrate proficiency with both data lakehouse paradigms.

| Output                  | Description                               |
|-------------------------|-------------------------------------------|
| `data/spark/games/`     | Typed game records (Parquet)              |
| `data/spark/genres/`    | Genre reference data (Parquet)            |
| `data/spark/platforms/` | Platform reference data (Parquet)        |

---

## dbt Core Transformation Layer

The semantic layer is managed using dbt core, targeting the compiled `main_gold` schema context inside DuckDB. The compilation profiles utilize dynamic environment parsing via `{{ env_var() }}` to maintain identical structural configuration across both development environments and container runtimes.

| Model                       | Layer | Description                                         |
|-----------------------------|-------|-----------------------------------------------------|
| `main_gold.gold_top_rated`  | Gold  | Top-tier game assets ranked by global user ratings  |
| `main_gold.gold_genre`      | Gold  | Aggregated genre analytical metric summaries        |
| `main_gold.gold_platform`   | Gold  | Aggregated platform footprint analytical summaries  |

To manually compile or inspect the dbt models from the repository root:

```bash
dbt run --project-dir rawg_dbt --profiles-dir rawg_dbt
```

---

## Testing

```bash
# Python unit tests
pytest tests/ -v

# Run dbt data quality assertions and schema tests
dbt test --project-dir rawg_dbt --profiles-dir rawg_dbt
```

---

## Environment Variables

| Variable           | Description                                                        |
|--------------------|--------------------------------------------------------------------|
| `RAWG_API_KEY`     | Your private RAWG API endpoint developer credential token          |
| `DBT_DUCKDB_PATH`  | Target runtime environment variable path fallback for dbt profile  |