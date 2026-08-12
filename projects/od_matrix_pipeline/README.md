# Origin-Destination Matrix Demo

A small pipeline that builds an origin-destination (OD) matrix from raw trip-level records, using synthetic East Sussex commuting data as a stand-in for a licensed transport, footfall, or logistics dataset.

## Problem statement

An OD matrix summarises how many trips (or goods, calls, deliveries, etc.) moved between every pair of locations in a dataset. It is a standard reporting pattern in transport planning, retail footfall analysis, and logistics, where the question is not just "how much activity happened" but "where did it happen, and where did it go."

This project demonstrates the pipeline pattern behind that kind of reporting, from raw trip records through to a matrix ready for a BI tool.

## Data source

The current version uses synthetic data, generated with a fixed random seed for reproducibility, weighted so that Brighton behaves as a commuter hub, in line with real East Sussex travel-to-work patterns. This was a deliberate choice to prove out the pipeline logic without needing a licensed dataset upfront.

A planned next step is to swap in a real dataset, most likely ONS travel-to-work flow data, once licence terms have been checked.

## Architecture

The pipeline follows the same bronze, silver, gold pattern used elsewhere in this portfolio (see the RAWG pipeline project):

- **Bronze**: raw trip-level records, one row per journey (origin, destination)
- **Silver**: cleaned and validated records, currently just removing intra-zone trips where origin equals destination
- **Gold**: the OD matrix itself, an origin-by-destination grid of trip counts, ready to be loaded into a reporting layer such as Power BI

## Key design decisions

- **Zero-trip pairs are kept, not dropped.** Every possible origin/destination combination is built first using `pandas.MultiIndex.from_product`, then actual trip counts are reindexed onto that full grid. This means a zone pair with no recorded trips shows as `0` rather than being silently missing, which matters for reporting completeness.
- **Intra-zone trips are excluded at the silver layer.** Trips where origin and destination are the same are treated as noise for OD reporting purposes, though this assumption would need revisiting depending on what the real dataset represents.
- **The gold table is a plain matrix, not a long-format table**, so it can be dropped straight into a BI tool's matrix visual without further reshaping.

## How to run it

```bash
pip install pandas numpy matplotlib
python src/od_matrix_demo.py
```

This will print the bronze, silver, and gold record counts to the console, save the finished matrix to `data/processed/od_matrix.csv`, and save a heatmap visualisation as a PNG.

## Output

The pipeline produces:
- `od_matrix.csv`, the gold-layer OD matrix
- `od_matrix_heatmap.png`, a heatmap visualisation with annotated trip counts

## What I'd do next

- Swap synthetic data for a real dataset (ONS travel-to-work flows or a public transport dataset with origin/destination fields)
- Add a normalised view (percentage of total, or location quotient) alongside raw counts
- Plot the matrix as a flow map rather than just a heatmap, since spatial visualisation is usually what OD reporting roles are actually asking for
- Bucket trips by time of day, if the source data supports it
- Rebuild the silver and gold transforms as dbt models with tests, consistent with the rest of this portfolio's analytics engineering direction
