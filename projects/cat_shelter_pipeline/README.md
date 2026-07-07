# Cat Shelter Pipeline 🐱

An end-to-end data pipeline that extracts real-world cat adoption data from the [RescueGroups.org v5 API](https://rescuegroups.org/services/adoptable-pet-data-api/), transforms it through a medallion architecture, and serves it via an interactive Streamlit dashboard.

Built to demonstrate practical data engineering skills in Python, using production-oriented patterns from a background in SSIS, Power Query M, and SQL Server.

**[🐱 Live Dashboard →](https://data-engineering-portfolio-wcn4sfvy8fvfuuyz4emqli.streamlit.app/)**

---

## What It Does

| Stage | Tool | What it does |
|---|---|---|
| **Extract** | `requests` | Authenticates with the RescueGroups v5 API and fetches available cat listings |
| **Bronze** | JSON | Raw API response persisted as-is for auditability |
| **Silver** | `pandas` + Parquet | Flattens nested JSON, standardises column names, applies config-driven field selection and deduplication |
| **Gold** | `SQLAlchemy` + SQLite | Upserts clean records into SQLite — safe to re-run without duplicates |
| **Dashboard** | `Streamlit` | Interactive dashboard with filters, metrics and charts served from the gold layer |

---

## 🛠️ Resilient Architecture: Hybrid Bronze Extraction Layer

During development in cloud-hosted environments (like GitHub Codespaces), upstream API firewalls may intermittently flag data center outbound IP ranges, leading to connection resets (`RemoteDisconnected` or `Connection reset by peer` errors). 

To ensure continuous integration and pipeline reliability, the **Bronze Extraction Layer** implements a resilient hybrid design pattern:

1. **Live Ingestion (Primary):** The pipeline attempts an authenticated `POST` request to the RescueGroups v5 API using the strict `application/vnd.api+json` specification.
2. **Graceful Local Fallback (Secondary):** If a network boundary error or firewall block is encountered, the extraction layer intercepts the exception, issues a system warning, and automatically hot-swaps to a local structured dataset (`mock_rescuegroups_raw.json`).

This design decouples downstream data engineering logic (Silver deduplication/Parquet serialization and Gold SQLite upserts) from external network volatility, allowing for seamless end-to-end development and robust error handling in production.

---

## Project Structure

```
cat_shelter_pipeline/
├── pipeline.py            # ETL pipeline: extract → bronze → silver → gold
├── dashboard.py           # Streamlit dashboard with pipeline bootstrap
├── config.yml             # Config-driven settings (API URL, field selection, paths)
├── requirements.in        # Direct dependencies
├── requirements.txt       # Pinned dependencies (compiled with pip-tools)
├── .env.example           # Template for API credentials
├── data/
│   ├── bronze/cats/       # Raw JSON from API
│   ├── silver/cats/       # Cleaned Parquet
│   └── cats_shelter.db    # Gold SQLite database
└── README.md
```

---

## Dashboard

The Streamlit dashboard reads from the gold SQLite layer and includes:

- **Summary metrics** — total cats, unique breeds, special needs count, cats with pictures
- **Age distribution** — bar chart across Baby / Young / Adult / Senior
- **Gender split** — pie chart
- **Top 10 breeds** — horizontal bar chart
- **Activity levels** — bar chart
- **Compatibility & characteristics** — % of cats OK with kids, cats, dogs, housetrained, special needs
- **Sidebar filters** — filter all charts by age group, gender, and activity level

The dashboard triggers the pipeline automatically on first load and refreshes data every 24 hours, using the `RESCUEGROUPS_API_KEY` secret configured in Streamlit Community Cloud.

> **Note for GitHub Codespaces:** Streamlit keeps running in the background even after you close the forwarded port tab. Stop it with `Ctrl+C` in the terminal before ending your Codespaces session, otherwise it will keep consuming compute hours on your Codespaces usage quota.

---

## Skills Demonstrated

- Medallion architecture (Bronze → Silver → Gold) with JSON and Parquet intermediate layers
- Resilient pipeline design (network exception handling and automated local data fallback logic)
- REST API authentication and data extraction with `requests`
- Nested JSON normalisation and transformation with `pandas`
- Config-driven pipeline design with `yaml`
- Upsert semantics (`INSERT OR REPLACE`) for idempotent loads via `SQLAlchemy`
- Secret management with `python-dotenv`
- Streamlit dashboard with pipeline bootstrap and staleness guard
- Dependency management with `pip-tools` (`requirements.in` → `requirements.txt`)
- Modular, production-oriented Python (not just a notebook)

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

Or launch the dashboard — it will trigger the pipeline automatically if no data is found:

```bash
streamlit run dashboard.py
```

---

## Configuration

Key settings are managed in `config.yml`:

```yaml
source:
  base_url: https://api.rescuegroups.org/v5
  page_size: 100       # max 250

layers:
  silver:
    fields_to_keep:    # config-driven field selection
      - id
      - attributes_name
      - attributes_agegroup
      # ... (23 fields total)
  gold:
    path: data/cats_shelter.db
```

---

## Dependencies

Managed with `pip-tools`. To update:

```bash
pip-compile requirements.in
pip install -r requirements.txt
```

Key packages: `requests`, `pandas`, `sqlalchemy`, `python-dotenv`, `pyyaml`, `streamlit`, `matplotlib`, `pyarrow`