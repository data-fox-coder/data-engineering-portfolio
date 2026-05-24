# 🐍 data-engineering-portfolio

A portfolio of Python data engineering pipelines and learning projects. Projects are designed to map existing enterprise skills onto programmatic equivalents — pandas ↔ Power Query, SQLite/DuckDB ↔ SQL Server, dbt/ETL structure ↔ SSIS packages.

![Python](https://img.shields.io/badge/python-3670A0?style=flat-square&logo=python&logoColor=ffdd54)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=flat-square&logo=pandas&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-%23FFF000.svg?style=flat-square&logo=duckdb&logoColor=black)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat-square&logo=dbt&logoColor=white)

---

## Projects

### 🕹️ [rawg_pipeline](./projects/rawg_pipeline)

An automated Medallion Architecture (Bronze/Silver/Gold) ETL pipeline that extracts rich video game metadata from the [RAWG.io API](https://rawg.io/apidocs), processes transformations via **dbt** and **DuckDB**, and serves insights through an interactive **Streamlit** dashboard.

[![Live Dashboard](https://img.shields.io/badge/Live%20Dashboard-Streamlit-ff4b4b?logo=streamlit)](https://data-engineering-portfolio-mxxbvanhcjuvkrgtjhemzr.streamlit.app)

**Skills demonstrated:** REST API extraction (`requests`), JSON flattening, pipeline orchestration, analytics engineering with dbt, local data warehouse optimisation with DuckDB, interactive data presentation.  
**BI equivalent:** Power Query API Connection ➔ SQL Server Staging ➔ dbt Core Transformations ➔ Power BI / Tableau Dashboard

---

### 🐱 [cat_shelter_pipeline](./projects/cat_shelter_pipeline)

An end-to-end Medallion Architecture (Bronze/Silver/Gold) ETL pipeline that extracts real-world cat adoption data from the [RescueGroups.org v5 API](https://rescuegroups.org/services/adoptable-pet-data-api/), transforms it through pandas and Parquet intermediate layers, and serves it via an interactive Streamlit dashboard.

[![Live Dashboard](https://img.shields.io/badge/Live%20Dashboard-Streamlit-ff4b4b?logo=streamlit)](https://data-engineering-portfolio-wcn4sfvy8fvfuuyz4emqli.streamlit.app/)

**Skills demonstrated:** REST API integration, medallion architecture, `pandas` transformation, config-driven field selection, SQLAlchemy upserts, Streamlit dashboard with pipeline bootstrap and staleness guard.  
**BI equivalent:** SSIS package ➔ SQL Server staging ➔ Power BI Dashboard

---

### 📘 [python_intermediate_d2i](/learning/python_intermediate_d2i)

Exercises and worked examples based on the Data to Insight ERN intermediate Python sessions — a structured course covering core Python and pandas concepts for data practitioners.

**Topics covered:** pandas Series & DataFrames, filtering & aggregation, merging datasets, handling nulls, data visualization.  
**BI equivalent:** Moving from Excel/SQL-based analysis to programmatic pandas-based data wrangling.

---

## Tech Stack

| Tool / Framework | Purpose |
|---|---|
| **Python 3.x** | Core language for ingestion, scripting, and application serving |
| **pandas / PyArrow** | High-performance data manipulation and evaluation |
| **dbt-duckdb** | Modern Data Stack modeling, transformation logic, and testing |
| **DuckDB** | In-process analytical database for lightning-fast local queries |
| **Streamlit / Plotly** | Interactive visualization and user-facing presentation layer |
| **pip-tools** | Explicit dependency management (`requirements.in` / `requirements.txt` compilation) |

---

## Background

I am a data professional with a deep background in SQL Server, local government data analysis, and Excel automation (Power Query M / DAX), expanding into programmatic data engineering. This repository documents that journey — each project is carefully engineered to handle messy, real-world API data rather than generic toy datasets.

Currently tracking towards the **Databricks Certified Data Analyst Associate** certification in parallel with these builds.

---

## Setup & Project Isolation

This repository uses a **monorepo structure** where each independent project contains its own isolated virtual environment (`.venv`) and pinned dependencies via `pip-tools`. This ensures individual pipelines never suffer from package version conflicts.

### 1. Clone the repository
```bash
git clone https://github.com/data-fox-coder/data-engineering-portfolio.git
cd data-engineering-portfolio
```