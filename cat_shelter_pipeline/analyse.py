"""
Cat Shelter Analysis & Visualisations
======================================
Connects to the SQLite database produced by pipeline.py and generates
charts exploring available cats in shelters.

Run after pipeline.py:
    python analyse.py
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import os

DB_PATH    = "data/cats.db"
OUTPUT_DIR = "output"


# ── HELPERS ───────────────────────────────────────────────────────────────────

def load_data(db_path: str = DB_PATH) -> pd.DataFrame:
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql("SELECT * FROM cats", conn,
                         parse_dates=["available_date", "updated_date"])
    print(f"Loaded {len(df):,} cats from {db_path}")
    return df


def save(fig: plt.Figure, filename: str) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, filename)
    fig.savefig(path, bbox_inches="tight", dpi=150)
    print(f"  Saved -> {path}")
    plt.close(fig)


# ── CHARTS ────────────────────────────────────────────────────────────────────

def chart_age_distribution(df: pd.DataFrame) -> None:
    """Bar chart: cats by age group."""
    order  = ["Baby", "Young", "Adult", "Senior"]
    counts = df["age_group"].value_counts().reindex(order, fill_value=0)

    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.bar(counts.index, counts.values, color="#E07B54", edgecolor="white")
    ax.bar_label(bars, padding=4, fontsize=9)
    ax.set_title("Available Cats by Age Group", fontweight="bold", pad=12)
    ax.set_ylabel("Number of cats")
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.spines[["top", "right"]].set_visible(False)
    save(fig, "age_distribution.png")


def chart_top_breeds(df: pd.DataFrame, top_n: int = 10) -> None:
    """Horizontal bar: most common breeds in shelters."""
    counts = (
        df["breed_primary"]
        .value_counts()
        .head(top_n)
        .sort_values()
    )

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.barh(counts.index, counts.values, color="#5B8DB8", edgecolor="white")
    ax.bar_label(bars, padding=4, fontsize=9)
    ax.set_title(f"Top {top_n} Breeds in Shelters", fontweight="bold", pad=12)
    ax.set_xlabel("Number of cats")
    ax.spines[["top", "right"]].set_visible(False)
    save(fig, "top_breeds.png")


def chart_gender_split(df: pd.DataFrame) -> None:
    """Pie chart: male vs female."""
    counts = df["sex"].fillna("Unknown").value_counts()

    fig, ax = plt.subplots(figsize=(5, 5))
    ax.pie(
        counts.values,
        labels=counts.index,
        autopct="%1.1f%%",
        colors=["#5B8DB8", "#E07B54", "#A8C5A0"],
        startangle=90,
        wedgeprops={"edgecolor": "white", "linewidth": 1.5},
    )
    ax.set_title("Gender Split", fontweight="bold")
    save(fig, "gender_split.png")


def chart_top_states(df: pd.DataFrame, top_n: int = 10) -> None:
    """Bar chart: states with the most available cats."""
    if df["state"].isna().all():
        print("  Skipping states chart — no state data available.")
        return

    counts = df["state"].value_counts().head(top_n)

    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.bar(counts.index, counts.values, color="#A8C5A0", edgecolor="white")
    ax.bar_label(bars, padding=4, fontsize=9)
    ax.set_title(f"Top {top_n} States by Available Cats", fontweight="bold", pad=12)
    ax.set_ylabel("Number of cats")
    ax.spines[["top", "right"]].set_visible(False)
    save(fig, "top_states.png")


def chart_energy_levels(df: pd.DataFrame) -> None:
    """Bar chart: breakdown of energy levels."""
    counts = df["energy_level"].value_counts()

    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(counts.index, counts.values, color="#5B8DB8", edgecolor="white")
    ax.bar_label(bars, padding=4, fontsize=9)
    ax.set_title("Cats by Energy Level", fontweight="bold", pad=12)
    ax.set_ylabel("Number of cats")
    ax.spines[["top", "right"]].set_visible(False)
    save(fig, "energy_levels.png")


def chart_compatibility(df: pd.DataFrame) -> None:
    """Horizontal bar: what % of cats are compatible with kids, cats, dogs."""
    total = len(df)
    labels = ["OK with kids", "OK with cats", "OK with dogs",
              "Housetrained", "Microchipped"]
    cols   = ["is_kids_ok", "is_cats_ok", "is_dogs_ok",
              "is_housetrained", "is_microchipped"]
    pcts   = [df[c].sum() / total * 100 for c in cols]

    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.barh(labels, pcts, color="#A8C5A0", edgecolor="white")
    ax.bar_label(bars, fmt="%.1f%%", padding=4, fontsize=9)
    ax.set_xlim(0, 110)
    ax.set_title("Compatibility & Characteristics (% of all cats)",
                 fontweight="bold", pad=12)
    ax.spines[["top", "right"]].set_visible(False)
    save(fig, "compatibility.png")


# ── SUMMARY ───────────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame) -> None:
    print("\n── Summary ──────────────────────────────────────────")
    print(f"  Total cats available:  {len(df):,}")
    print(f"  Unique breeds:         {df['breed_primary'].nunique():,}")
    print(f"  States covered:        {df['state'].nunique():,}")
    print(f"  With pictures:         {(df['picture_count'] > 0).sum():,}")
    print(f"  Special needs:         {df['is_special_needs'].sum():,}")
    print(f"  Gender breakdown:\n{df['sex'].value_counts().to_string()}")
    print("─────────────────────────────────────────────────────\n")


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    df = load_data()
    print_summary(df)

    print("Generating charts...")
    chart_age_distribution(df)
    chart_top_breeds(df)
    chart_gender_split(df)
    chart_top_states(df)
    chart_energy_levels(df)
    chart_compatibility(df)

    print("\n✅ All charts saved to /output")