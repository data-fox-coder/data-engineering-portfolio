import os

# 1. This evaluates directly to /workspaces/data-engineering-portfolio/projects/rawg_pipeline
PIPELINE_ROOT = os.path.dirname(os.path.abspath(__file__))

# 2. Anchor the DuckDB file cleanly right here
DB_PATH = os.path.join(PIPELINE_ROOT, "rawg_data.duckdb")