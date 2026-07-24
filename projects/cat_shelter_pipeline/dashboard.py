"""
Cat Shelter Dashboard
=====================
Interactive Streamlit dashboard visualising available cats in shelters.
Reads from the SQLite database produced by pipeline.py.
Includes startup logic to trigger the pipeline if data is missing or stale.
"""

import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_HERE = Path(__file__).parent
DB_PATH = _HERE / "data" / "gold" / "cats" / "cats_shelter.db"

REFRESH_HOURS = 24


# ---------------------------------------------------------------------------
# Pipeline bootstrap
# ---------------------------------------------------------------------------

def _db_age_hours() -> float | None:
    """Return how many hours ago the DB was last modified, or None if missing."""
    if not DB_PATH.exists():
        return None
    age_seconds = time.time() - DB_PATH.stat().st_mtime
    return age_seconds / 3600


def _run_pipeline() -> bool:
    try:
        from pipeline import (
            extract_cat_data,
            load_cat_data,
            load_config,
            save_bronze,
            save_silver,
            setup_logging,
            transform_cat_data,
        )
    except Exception as exc:  # noqa: BLE001
        st.error(f"Pipeline import error: {exc}")
        return False

    try:
        config = load_config()
        setup_logging(config)

        raw_data = extract_cat_data(config)
        if not raw_data:
            return False

        save_bronze(raw_data, config)

        df = transform_cat_data(raw_data, config)
        if df.empty:
            return False

        save_silver(df, config)
        load_cat_data(df, config)
        return True

    except Exception as exc:  # noqa: BLE001
        st.error(f"Pipeline error: {exc}")
        return False


def ensure_fresh_data() -> None:
    """
    Call once at app startup before load_data().
    Triggers the pipeline if the DB is missing or older than REFRESH_HOURS.
    """
    age = _db_age_hours()

    if age is None:
        with st.spinner("🐱 No local data found — fetching cats from RescueGroups..."):
            success = _run_pipeline()
        if success:
            last_run = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")
            st.success(f"Pipeline complete. Data loaded as of {last_run}.")
        else:
            st.error(
                "Pipeline failed to fetch data. "
                "Check your RESCUEGROUPS_API_KEY secret and try refreshing."
            )
            st.stop()

    elif age > REFRESH_HOURS:
        with st.spinner(f"🔄 Data is {age:.0f}h old — refreshing from RescueGroups..."):
            success = _run_pipeline()
        if success:
            last_run = datetime.now(timezone.utc).strftime("%d %b %Y %H:%M UTC")
            st.toast(f"✅ Data refreshed at {last_run}")
        else:
            st.warning(
                f"⚠️ Refresh failed — showing data from {age:.0f}h ago. "
                "Check your API key or try again later."
            )


# ---------------------------------------------------------------------------
# Data loader
# ---------------------------------------------------------------------------

@st.cache_data(ttl=REFRESH_HOURS * 3600)
def load_data() -> pd.DataFrame:
    """Load cats from the gold SQLite layer. Cached for REFRESH_HOURS."""
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql(
            "SELECT * FROM cats",
            conn,
            parse_dates=["attributes_updateddate"],
        )

    # ---- Missing-column guard ----
    expected_cols = [
        "attributes_agegroup",
        "attributes_sex",
        "attributes_activitylevel",
        "attributes_breedprimary",
        "attributes_isspecialneeds",
        "attributes_picturecount",
        "attributes_iscatsok",
        "attributes_isdogsok",
        "attributes_iskidsok",
        "attributes_ishousetrained",
    ]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        st.warning(f"Missing expected columns: {missing}")

    return df


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filters")

    age_groups = ["All"] + sorted(df["attributes_agegroup"].dropna().unique().tolist())
    selected_age = st.sidebar.selectbox("Age Group", age_groups)

    genders = ["All"] + sorted(df["attributes_sex"].dropna().unique().tolist())
    selected_gender = st.sidebar.selectbox("Gender", genders)

    activity_levels = ["All"] + sorted(df["attributes_activitylevel"].dropna().unique().tolist())
    selected_activity = st.sidebar.selectbox("Activity Level", activity_levels)

    if selected_age != "All":
        df = df[df["attributes_agegroup"] == selected_age]
    if selected_gender != "All":
        df = df[df["attributes_sex"] == selected_gender]
    if selected_activity != "All":
        df = df[df["attributes_activitylevel"] == selected_activity]

    return df


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def show_metrics(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("No cats match the selected filters.")
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Cats", f"{len(df):,}")
    col2.metric("Unique Breeds", f"{df['attributes_breedprimary'].nunique():,}")
    col3.metric("Special Needs", f"{pd.to_numeric(df['attributes_isspecialneeds'], errors='coerce').fillna(0).sum():,.0f}")
    col4.metric("With Pictures", f"{pd.to_numeric(df['attributes_picturecount'], errors='coerce').fillna(0).gt(0).sum():,}")


# ---------------------------------------------------------------------------
# Charts (with empty-data protection)
# ---------------------------------------------------------------------------

def chart_age_distribution(df: pd.DataFrame) -> plt.Figure:
    if df.empty:
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, "No data available", ha="center")
        return fig

    order = ["Baby", "Young", "Adult", "Senior"]
    counts = df["attributes_agegroup"].value_counts().reindex(order, fill_value=0)
    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.bar(counts.index, counts.values, color="#E07B54", edgecolor="white")
    ax.bar_label(bars, padding=4, fontsize=9)
    ax.set_title("Available Cats by Age Group", fontweight="bold", pad=12)
    ax.set_ylabel("Number of cats")
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.spines[["top", "right"]].set_visible(False)
    return fig


def chart_top_breeds(df: pd.DataFrame, top_n: int = 10) -> plt.Figure:
    if df.empty:
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, "No data available", ha="center")
        return fig

    counts = df["attributes_breedprimary"].value_counts().head(top_n).sort_values()
    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.barh(counts.index, counts.values, color="#5B8DB8", edgecolor="white")
    ax.bar_label(bars, padding=4, fontsize=9)
    ax.set_title(f"Top {top_n} Breeds in Shelters", fontweight="bold", pad=12)
    ax.set_xlabel("Number of cats")
    ax.spines[["top", "right"]].set_visible(False)
    return fig


def chart_gender_split(df: pd.DataFrame) -> plt.Figure:
    if df.empty:
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, "No data available", ha="center")
        return fig

    counts = df["attributes_sex"].fillna("Unknown").value_counts()
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
    return fig


def chart_activity_levels(df: pd.DataFrame) -> plt.Figure:
    if df.empty:
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, "No data available", ha="center")
        return fig

    counts = df["attributes_activitylevel"].value_counts()
    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(counts.index, counts.values, color="#5B8DB8", edgecolor="white")
    ax.bar_label(bars, padding=4, fontsize=9)
    ax.set_title("Cats by Activity Level", fontweight="bold", pad=12)
    ax.set_ylabel("Number of cats")
    ax.spines[["top", "right"]].set_visible(False)
    return fig


def chart_compatibility(df: pd.DataFrame) -> plt.Figure:
    if df.empty:
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, "No data available", ha="center")
        return fig

    total = len(df)
    labels = ["OK with kids", "OK with cats", "OK with dogs",
              "Housetrained", "Special Needs"]
    cols = ["attributes_iskidsok", "attributes_iscatsok", "attributes_isdogsok",
            "attributes_ishousetrained", "attributes_isspecialneeds"]
    pcts = [
        pd.to_numeric(df[c], errors="coerce").fillna(0).sum() / total * 100
        for c in cols
    ]
    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.barh(labels, pcts, color="#A8C5A0", edgecolor="white")
    ax.bar_label(bars, fmt="%.1f%%", padding=4, fontsize=9)
    ax.set_xlim(0, 110)
    ax.set_title("Compatibility & Characteristics (% of all cats)",
                 fontweight="bold", pad=12)
    ax.spines[["top", "right"]].set_visible(False)
    return fig


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Cat Shelter Dashboard", page_icon="🐱", layout="wide")
st.title("🐱 Cat Shelter Dashboard")
st.caption("Data sourced from RescueGroups v5 API")

# ---- Last updated timestamp ----
if DB_PATH.exists():
    ts = datetime.fromtimestamp(DB_PATH.stat().st_mtime).strftime("%d %b %Y %H:%M")
    st.caption(f"Last updated: {ts}")

# ---- Manual refresh button ----
if st.button("🔄 Refresh Data"):
    ensure_fresh_data()
    st.rerun()

# ---- Automatic call removed ----
# ensure_fresh_data() 

df_raw = load_data()
df = apply_filters(df_raw)

# ---- Raw data viewer ----
with st.expander("View Raw Data"):
    st.dataframe(df_raw)

st.subheader("Summary")
show_metrics(df)

st.divider()

col1, col2 = st.columns(2)
with col1:
    st.pyplot(chart_age_distribution(df))
with col2:
    st.pyplot(chart_gender_split(df))

col3, col4 = st.columns(2)
with col3:
    st.pyplot(chart_top_breeds(df))
with col4:
    st.pyplot(chart_activity_levels(df))

col5, _ = st.columns(2)
with col5:
    st.pyplot(chart_compatibility(df))