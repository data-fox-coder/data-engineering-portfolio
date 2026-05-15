# 🐍 data-engineering-portfolio

A portfolio of Python learning projects. Projects are designed to map existing skills onto Python equivalents — pandas ↔ Power Query, SQLite ↔ SQL Server, ETL structure ↔ SSIS packages.

![Python](https://img.shields.io/badge/python-3670A0?style=flat-square&logo=python&logoColor=ffdd54)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=flat-square&logo=pandas&logoColor=white)
![SQLite](https://img.shields.io/badge/sqlite-%2307405e.svg?style=flat-square&logo=sqlite&logoColor=white)

---

## Projects

### 🕹️ [rawg_pipeline](./projects/rawg_pipeline)

An automated ETL pipeline that extracts rich video game metadata from the [RAWG.io API](https://rawg.io/apidocs) to create a structured database, demonstrating the transition from GUI-based data tools to programmatic ingestion.

**Skills demonstrated:** REST API calls (`requests`), handling API pagination and rate limits, JSON flattening, `pandas` data cleaning, SQLite storage, modular Python project structure.
**BI equivalent:** Power Query API Connection → SQL Server Dimension Table

---

### 🐱 [cat_shelter_pipeline](./projects/cat_shelter_pipeline)

An end-to-end ETL pipeline built using the [RescueGroups.org v5 API](https://rescuegroups.org/services/adoptable-pet-data-api/) to extract, transform, and load real-world cat adoption data into a local SQLite database.

**Skills demonstrated:** REST API calls (`requests`), JSON parsing, `pandas` data transformation, SQLite via `sqlite3`, ETL pipeline structure, `.env` secret management  
**BI equivalent:** SSIS package → SQL Server staging table

---

### 📘 [python_intermediate_d2i](/learning/python_intermediate_d2i)

Exercises and worked examples based on the [Data to Insight ERN intermediate Python sessions](https://github.com/data-to-insight/ERN-sessions/tree/main/intermediate_python) — a structured course covering core Python and pandas concepts for data practitioners.

**Topics covered:** pandas Series & DataFrames, filtering & aggregation, merging datasets, handling nulls, applying functions, data visualisation  
**BI equivalent:** Moving from Excel/SQL-based analysis to pandas-based workflows

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.x | Core language |
| pandas | Data transformation & analysis |
| requests | API calls |
| sqlite3 | Local data storage |
| python-dotenv | Secret/config management |
| Jupyter Notebooks | Exploration & documentation |

---

## Background

I'm a data professional with a background in SQL Server and Excel automation (Power Query M), currently learning Python to expand into programmatic data engineering and analytics. This repo documents that journey — each project is chosen to apply Python to something meaningful rather than toy examples.

Currently pursuing the **Databricks Data Analyst certification** alongside this learning.

---

## Setup

```bash
# Clone the repo
git clone https://github.com/data-fox-coder/data-engineering-portfolio.git
cd python-learning-repo

# Install dependencies
pip install -r requirements.txt
```

For projects that use API keys, copy `.env.example` to `.env` and add your credentials (see individual project READMEs).
