"""
Cat Breeds Analysis & Visualisations
======================================
Connects to the SQLite database produced by pipeline.py and generates
charts exploring breed characteristics.

Run after pipeline.py:
    python analyse.py
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import os
import numpy as np

DB_PATH    = "data/breeds.db"
OUTPUT_DIR = "output"


# ── HELPERS ───────────────────────────────────────────────────────────────────

def load_data(db_path: str = DB_PATH) -> pd.DataFrame:
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql("SELECT * FROM breeds", conn)
    print(f"Loaded {len(df):,} breeds from {db_path}")
    return df


def save(fig: plt.Figure, filename: str) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, filename)
    fig.savefig(path, bbox_inches="tight", dpi=150)
    print(f"  Saved -> {path}")
    plt.close(fig)


# ── CHARTS ────────────────────────────────────────────────────────────────────

def chart_top_countries(df: pd.DataFrame, top_n: int = 10) -> None:
    """Bar chart: which countries have produced the most breeds?"""
    counts = df["origin"].value_counts().head(top_n)

    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(counts.index, counts.values, color="#5B8DB8", edgecolor="white")
    ax.bar_label(bars, padding=4, fontsize=9)
    ax.set_title(f"Top {top_n} Countries by Number of Breeds", fontweight="bold", pad=12)
    ax.set_ylabel("Number of breeds")
    ax.tick_params(axis="x", rotation=30)
    ax.spines[["top", "right"]].set_visible(False)
    save(fig, "top_countries.png")


def chart_life_span_distribution(df: pd.DataFrame) -> None:
    """Histogram: distribution of average life spans."""
    data = df["life_span_avg"].dropna()

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.hist(data, bins=12, color="#E07B54", edgecolor="white", linewidth=0.8)
    ax.axvline(data.mean(), color="#333", linestyle="--", linewidth=1.2,
               label=f"Mean: {data.mean():.1f} yrs")
    ax.set_title("Distribution of Average Life Span", fontweight="bold", pad=12)
    ax.set_xlabel("Average life span (years)")
    ax.set_ylabel("Number of breeds")
    ax.legend()
    ax.spines[["top", "right"]].set_visible(False)
    save(fig, "life_span_distribution.png")


def chart_trait_averages(df: pd.DataFrame) -> None:
    """Horizontal bar chart: average score across all trait categories."""
    traits = [
        "adaptability", "affection_level", "child_friendly", "dog_friendly",
        "energy_level", "grooming", "health_issues", "intelligence",
        "shedding_level", "social_needs", "stranger_friendly", "vocalisation",
    ]
    labels = [t.replace("_", " ").title() for t in traits]
    averages = df[traits].mean().values

    fig, ax = plt.subplots(figsize=(8, 6))
    bars = ax.barh(labels, averages, color="#A8C5A0", edgecolor="white")
    ax.bar_label(bars, fmt="%.2f", padding=4, fontsize=9)
    ax.set_xlim(0, 5.5)
    ax.set_title("Average Trait Scores Across All Breeds", fontweight="bold", pad=12)
    ax.set_xlabel("Score (1 = low, 5 = high)")
    ax.spines[["top", "right"]].set_visible(False)
    save(fig, "trait_averages.png")


def chart_intelligence_vs_energy(df: pd.DataFrame) -> None:
    """Scatter plot: intelligence vs energy level, coloured by affection."""

    # Add jitter to prevent overplotting
    jitter = 0.15
    x = df["energy_level"] + np.random.uniform(-jitter, jitter, len(df))
    y = df["intelligence"] + np.random.uniform(-jitter, jitter, len(df))

    fig, ax = plt.subplots(figsize=(9, 6))
    scatter = ax.scatter(
        x, y,
        c=df["affection_level"], cmap="YlOrRd",
        s=60, edgecolors="white", linewidth=0.5, alpha=0.85,
    )
    cbar = plt.colorbar(scatter, ax=ax, label="Affection Level")
    cbar.ax.yaxis.label.set_size(10)

# Label only the most affectionate breed at intelligence=5, energy=5
    top = df[(df["intelligence"] == 5) & (df["energy_level"] == 5)].nlargest(1, "affection_level")
    for _, row in top.iterrows():
        ax.annotate(
            row["name"],
            (row["energy_level"], row["intelligence"]),
            fontsize=8,
            fontweight="bold",
            xytext=(8, 4),
            textcoords="offset points",
            arrowprops=dict(arrowstyle="-", color="grey", lw=0.8),
        )

    ax.set_title("Intelligence vs Energy Level\n(colour = affection level)",
                 fontweight="bold", pad=12)
    ax.set_xlabel("Energy Level (1-5)")
    ax.set_ylabel("Intelligence (1-5)")
    ax.set_xlim(0.5, 5.5)
    ax.set_ylim(0.5, 5.5)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.spines[["top", "right"]].set_visible(False)
    save(fig, "intelligence_vs_energy.png")

def chart_hypoallergenic_breakdown(df: pd.DataFrame) -> None:
    """Pie chart: hypoallergenic vs non-hypoallergenic breeds."""
    counts = df["hypoallergenic"].map({1: "Hypoallergenic", 0: "Not hypoallergenic"}).value_counts()

    fig, ax = plt.subplots(figsize=(5, 5))
    ax.pie(
        counts.values,
        labels=counts.index,
        autopct="%1.1f%%",
        colors=["#A8C5A0", "#E07B54"],
        startangle=90,
        wedgeprops={"edgecolor": "white", "linewidth": 1.5},
    )
    ax.set_title("Hypoallergenic Breeds", fontweight="bold")
    save(fig, "hypoallergenic_breakdown.png")


def chart_top_longest_lived(df: pd.DataFrame, top_n: int = 10) -> None:
    """Horizontal bar chart: breeds with the longest average life spans."""
    top = df.nlargest(top_n, "life_span_avg")[["name", "life_span_avg"]].sort_values("life_span_avg")

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.barh(top["name"], top["life_span_avg"], color="#5B8DB8", edgecolor="white")
    ax.bar_label(bars, fmt="%.1f yrs", padding=4, fontsize=9)
    ax.set_title(f"Top {top_n} Longest-Lived Breeds", fontweight="bold", pad=12)
    ax.set_xlabel("Average life span (years)")
    ax.spines[["top", "right"]].set_visible(False)
    save(fig, "longest_lived.png")


# ── SUMMARY ───────────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame) -> None:
    print("\n── Summary ──────────────────────────────────────")
    print(f"  Total breeds:          {len(df):,}")
    print(f"  Countries represented: {df['origin'].nunique():,}")
    print(f"  Hypoallergenic breeds: {df['hypoallergenic'].sum():,}")
    print(f"  Hairless breeds:       {df['hairless'].sum():,}")
    print(f"  Avg life span:         {df['life_span_avg'].mean():.1f} years")
    print(f"  Most intelligent:      {df.nlargest(1, 'intelligence')['name'].values[0]}")
    print(f"  Most affectionate:     {df.nlargest(1, 'affection_level')['name'].values[0]}")
    print("─────────────────────────────────────────────────\n")


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    df = load_data()
    print_summary(df)

    print("Generating charts...")
    chart_top_countries(df)
    chart_life_span_distribution(df)
    chart_trait_averages(df)
    chart_intelligence_vs_energy(df)
    chart_hypoallergenic_breakdown(df)
    chart_top_longest_lived(df)

    print("\n✅ All charts saved to /output")
