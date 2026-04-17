# """
# Cat Shelter Analysis & Visualisations
# ======================================
# Connects to the SQLite database produced by pipeline.py and generates
# charts — the Python equivalent of your Tableau dashboards.

# Run after pipeline.py:
#     python analyse.py
# """

# import sqlite3
# import pandas as pd
# import matplotlib.pyplot as plt
# import matplotlib.ticker as mticker
# import os

# DB_PATH    = "data/cats.db"
# OUTPUT_DIR = "output"


# # ── HELPERS ───────────────────────────────────────────────────────────────────

# def load_data(db_path: str = DB_PATH) -> pd.DataFrame:
#     """Read the cats table into a DataFrame."""
#     with sqlite3.connect(db_path) as conn:
#         df = pd.read_sql("SELECT * FROM cats", conn, parse_dates=["published_at"])
#     print(f"Loaded {len(df):,} rows from {db_path}")
#     return df


# def save(fig: plt.Figure, filename: str) -> None:
#     os.makedirs(OUTPUT_DIR, exist_ok=True)
#     path = os.path.join(OUTPUT_DIR, filename)
#     fig.savefig(path, bbox_inches="tight", dpi=150)
#     print(f"  Saved → {path}")
#     plt.close(fig)


# # ── CHARTS ────────────────────────────────────────────────────────────────────

# def chart_age_distribution(df: pd.DataFrame) -> None:
#     """Bar chart: how many cats fall into each age band?"""
#     order = ["Baby", "Young", "Adult", "Senior"]
#     counts = df["age"].value_counts().reindex(order, fill_value=0)

#     fig, ax = plt.subplots(figsize=(7, 4))
#     bars = ax.bar(counts.index, counts.values, color="#E07B54", edgecolor="white", linewidth=0.8)
#     ax.bar_label(bars, padding=4, fontsize=9)
#     ax.set_title("Cats by Age Band", fontweight="bold", pad=12)
#     ax.set_ylabel("Number of cats")
#     ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
#     ax.spines[["top", "right"]].set_visible(False)
#     save(fig, "age_distribution.png")


# def chart_top_breeds(df: pd.DataFrame, top_n: int = 10) -> None:
#     """Horizontal bar chart: most common breeds."""
#     counts = (
#         df["breed_primary"]
#         .value_counts()
#         .head(top_n)
#         .sort_values()          # ascending so longest bar is at the top
#     )

#     fig, ax = plt.subplots(figsize=(8, 5))
#     bars = ax.barh(counts.index, counts.values, color="#5B8DB8", edgecolor="white")
#     ax.bar_label(bars, padding=4, fontsize=9)
#     ax.set_title(f"Top {top_n} Breeds", fontweight="bold", pad=12)
#     ax.set_xlabel("Number of cats")
#     ax.spines[["top", "right"]].set_visible(False)
#     save(fig, "top_breeds.png")


# def chart_gender_split(df: pd.DataFrame) -> None:
#     """Pie chart: male vs female vs unknown."""
#     counts = df["gender"].fillna("Unknown").value_counts()

#     fig, ax = plt.subplots(figsize=(5, 5))
#     ax.pie(
#         counts.values,
#         labels=counts.index,
#         autopct="%1.1f%%",
#         colors=["#5B8DB8", "#E07B54", "#A8C5A0"],
#         startangle=90,
#         wedgeprops={"edgecolor": "white", "linewidth": 1.5},
#     )
#     ax.set_title("Gender Split", fontweight="bold")
#     save(fig, "gender_split.png")


# def chart_listings_over_time(df: pd.DataFrame) -> None:
#     """Line chart: new listings published per week."""
#     if df["published_at"].isna().all():
#         print("  Skipping time chart — no published_at data available.")
#         return

#     weekly = (
#         df.set_index("published_at")
#         .resample("W")["id"]
#         .count()
#         .rename("listings")
#     )

#     fig, ax = plt.subplots(figsize=(10, 4))
#     ax.plot(weekly.index, weekly.values, color="#5B8DB8", linewidth=2)
#     ax.fill_between(weekly.index, weekly.values, alpha=0.15, color="#5B8DB8")
#     ax.set_title("New Listings per Week", fontweight="bold", pad=12)
#     ax.set_ylabel("Number of cats")
#     ax.spines[["top", "right"]].set_visible(False)
#     fig.autofmt_xdate()
#     save(fig, "listings_over_time.png")


# def chart_top_states(df: pd.DataFrame, top_n: int = 10) -> None:
#     """Bar chart: which US states have the most listings?"""
#     if df["state"].isna().all():
#         print("  Skipping states chart — no state data available.")
#         return

#     counts = df["state"].value_counts().head(top_n)

#     fig, ax = plt.subplots(figsize=(8, 4))
#     bars = ax.bar(counts.index, counts.values, color="#A8C5A0", edgecolor="white")
#     ax.bar_label(bars, padding=4, fontsize=9)
#     ax.set_title(f"Top {top_n} States by Listings", fontweight="bold", pad=12)
#     ax.set_ylabel("Number of cats")
#     ax.spines[["top", "right"]].set_visible(False)
#     save(fig, "top_states.png")


# # ── SUMMARY STATS ─────────────────────────────────────────────────────────────

# def print_summary(df: pd.DataFrame) -> None:
#     print("\n── Summary ──────────────────────────────")
#     print(f"  Total cats:        {len(df):,}")
#     print(f"  Unique breeds:     {df['breed_primary'].nunique():,}")
#     print(f"  States covered:    {df['state'].nunique():,}")
#     print(f"  Status breakdown:\n{df['status'].value_counts().to_string()}")
#     print("─────────────────────────────────────────\n")


# # ── MAIN ──────────────────────────────────────────────────────────────────────

# if __name__ == "__main__":
#     df = load_data()
#     print_summary(df)

#     print("\nGenerating charts...")
#     chart_age_distribution(df)
#     chart_top_breeds(df)
#     chart_gender_split(df)
#     chart_listings_over_time(df)
#     chart_top_states(df)

#     print("\n✅ All charts saved to /output")
