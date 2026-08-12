"""
Origin-Destination Matrix Demo
--------------------------------
Demonstrates building an OD matrix from raw "trip" records, following the
same bronze -> silver -> gold pattern used in the RAWG pipeline:

    bronze: raw trip-level records (one row per journey)
    silver: cleaned, validated trip records
    gold:   the OD matrix itself, ready for reporting/BI

Uses synthetic commuter data between East Sussex towns, since real OD data
usually comes from ticketing systems, mobile location data, or survey data
that requires licensing agreements.
"""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# BRONZE LAYER: simulate raw trip records
# ---------------------------------------------------------------------------
# In a real pipeline this would be an extract from a ticketing system,
# ANPR camera data, or a travel survey. Here we generate synthetic data so
# the demo is fully reproducible without needing a licensed dataset.

np.random.seed(42)  # fixes the random seed so the output is identical every run

zones = ["Eastbourne", "Brighton", "Lewes", "Hastings", "Seaford", "Uckfield"]

# Build a raw, trip-level table: one row per journey.
# In reality this table could have millions of rows; we simulate ~2,500.
n_trips = 2500
raw_trips = pd.DataFrame({
    "origin": np.random.choice(zones, size=n_trips),
    "destination": np.random.choice(zones, size=n_trips),
})

# Weight the data so it isn't perfectly random - Brighton acts as a commuter
# hub, receiving more inbound trips than other towns, which mirrors real
# East Sussex travel-to-work patterns.
brighton_boost = raw_trips["destination"] == "Brighton"
extra_brighton_trips = raw_trips[brighton_boost].sample(frac=0.6, random_state=1)
raw_trips = pd.concat([raw_trips, extra_brighton_trips], ignore_index=True)

print(f"Bronze layer: {len(raw_trips):,} raw trip records")

# ---------------------------------------------------------------------------
# SILVER LAYER: clean and validate
# ---------------------------------------------------------------------------
# Real-world validation would include: dropping nulls, checking zone codes
# against a reference list, removing duplicate ticket scans, etc.
# Here we just remove intra-zone "trips" (same origin and destination),
# since these usually aren't meaningful for OD reporting.

silver_trips = raw_trips[raw_trips["origin"] != raw_trips["destination"]].copy()
print(f"Silver layer: {len(silver_trips):,} records after removing intra-zone trips")

# ---------------------------------------------------------------------------
# GOLD LAYER: build the OD matrix
# ---------------------------------------------------------------------------
# Step 1 - build every possible origin/destination pairing first, so zones
# with zero recorded trips still appear in the matrix as 0 rather than being
# silently missing. This matters for OD reporting because "no trips" is a
# meaningful result, not an absence of data.

full_index = pd.MultiIndex.from_product([zones, zones], names=["origin", "destination"])

# Step 2 - count actual trips per origin/destination pair
trip_counts = (
    silver_trips
    .groupby(["origin", "destination"])
    .size()
    .rename("trips")
)

# Step 3 - reindex onto the full grid, filling any missing pairs with 0
od_long = trip_counts.reindex(full_index, fill_value=0).reset_index()

# Step 4 - pivot from long format (one row per pair) into a proper matrix
# (origins as rows, destinations as columns) - this is the "gold" table
# that would be exposed to a BI tool or reporting layer.
od_matrix = od_long.pivot(index="origin", columns="destination", values="trips")

# Zero out the diagonal explicitly for readability (origin == destination).
# .to_numpy(copy=True) forces a writable array, since pandas sometimes
# returns a read-only view from .values after a pivot.
diag_safe = od_matrix.to_numpy(copy=True)
np.fill_diagonal(diag_safe, 0)
od_matrix = pd.DataFrame(diag_safe, index=od_matrix.index, columns=od_matrix.columns)

print("\nGold layer: OD matrix")
print(od_matrix)

# Save the gold table as a CSV, as you would before loading into Power BI
od_matrix.to_csv("/mnt/user-data/outputs/od_matrix.csv")

# ---------------------------------------------------------------------------
# VISUALISATION: heatmap of the OD matrix
# ---------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(8, 6))
im = ax.imshow(od_matrix.values, cmap="viridis")

ax.set_xticks(range(len(zones)))
ax.set_yticks(range(len(zones)))
ax.set_xticklabels(od_matrix.columns, rotation=45, ha="right")
ax.set_yticklabels(od_matrix.index)
ax.set_xlabel("Destination")
ax.set_ylabel("Origin")
ax.set_title("East Sussex Commuter Flows: Origin-Destination Matrix (Demo Data)")

# Annotate each cell with its trip count
for i in range(len(zones)):
    for j in range(len(zones)):
        value = od_matrix.values[i, j]
        text_colour = "white" if value < od_matrix.values.max() / 2 else "black"
        ax.text(j, i, int(value), ha="center", va="center", color=text_colour, fontsize=9)

fig.colorbar(im, ax=ax, label="Trip count")
fig.tight_layout()
fig.savefig("/mnt/user-data/outputs/od_matrix_heatmap.png", dpi=150)

print("\nSaved od_matrix.csv and od_matrix_heatmap.png to outputs")
