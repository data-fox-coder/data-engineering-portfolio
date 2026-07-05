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

- Python 3.11+
- Java 17+ (required only for the optional PySpark layer):

```bash
sudo apt-get install -y default-jdk
```

*Java is a system-level dependency and is not included in `requirements.txt`.*

### Installation & Execution

```bash
# Clone the repository and navigate to the project root
git clone https://github.com/data-fox-coder/data-engineering-portfolio.git
cd data-engineering-portfolio

# Install project dependencies
pip install -r requirements.txt

# Add your RAWG API key
cp .env.example .env  # then edit .env and set RAWG_API_KEY=your_key_here
```

> **Note:** `config.py` contains only path resolution logic (no secrets) and is committed as-is. `.env` holds `RAWG_API_KEY` and is gitignored, never commit it.

#### Step 1: Run the Pipeline

Trigger the full pipeline sequentially (Bronze ingest → Silver transform → dbt Gold compilation) with a single script. This generates `rawg_data.duckdb`:

```bash
python run_pipeline.py
```

#### Step 2: Launch the Dashboard

The Streamlit app reads `rawg_data.duckdb` in read-only mode. The pipeline and serving layer are fully decoupled (see Design Note below), so the database must already exist before launching:

```bash
streamlit run app.py
```

If `rawg_data.duckdb` is missing, the app will display an error directing you to run `run_pipeline.py` first.

> **Note for GitHub Codespaces:** Streamlit keeps running in the background even after you close the forwarded port tab. Stop it with `Ctrl+C` in the terminal before ending your Codespaces session, otherwise it will keep consuming compute hours on your Codespaces usage quota.

---

## PySpark Layer

The PySpark layer is a modular, scale-out alternative to the core Python silver transform. It reads the same bronze DuckDB tables, applies identical cleaning and typing logic using Spark DataFrames, and writes Parquet output to `data/spark/`.

In production, PySpark would replace the local DuckDB silver layer when dataset scale outgrows local compute limits. Both approaches are included here to demonstrate proficiency with both data lakehouse paradigms.

| Output                  | Description                          |
|-------------------------|---------------------------------------|
| `data/spark/games/`     | Typed game records (Parquet)         |
| `data/spark/genres/`    | Genre reference data (Parquet)       |
| `data/spark/platforms/` | Platform reference data (Parquet)    |

---

## dbt Core Transformation Layer

The semantic layer is managed using dbt core, targeting the compiled `main_gold` schema inside DuckDB. Compilation profiles use dynamic environment parsing via `{{ env_var() }}` to maintain identical configuration across development environments and container runtimes.

| Model | Layer | Description |
|---|---|---|
| `main_gold.gold_top_rated_games` | Gold | Top-tier game assets ranked by global user ratings |
| `main_gold.gold_genre_summary` | Gold | Aggregated genre analytical metric summaries |
| `main_gold.gold_platform_summary` | Gold | Aggregated platform footprint analytical summaries |

To manually compile or inspect the dbt models from the repository root:

```bash
dbt run --project-dir rawg_dbt
```

---

## Testing

```bash
# Python unit tests
pytest tests/ -v

# Run dbt data quality assertions and schema tests
dbt test --project-dir rawg_dbt
```

---

## Environment & Configuration Management

| Variable           | Description                                              |
|--------------------|-----------------------------------------------------------|
| `config.py`        | Committed module resolving local paths (e.g. `DB_PATH`), no secrets |
| `.env`             | Local file holding `RAWG_API_KEY` (gitignored)           |
| `profiles.yml`     | Isolated globally in `~/.dbt/`, maps dbt target schemas  |

**Design note:** Earlier versions of this app triggered the pipeline on cold-start within the Streamlit process itself. This caused a DuckDB connection conflict between the pipeline's read-write connection and the dashboard's read-only connection. The pipeline and serving layer are now fully decoupled, `rawg_data.duckdb` is a committed build artifact, regenerated manually via `run_pipeline.py` and refreshed in the repo as needed.