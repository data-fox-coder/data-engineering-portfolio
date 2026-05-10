"""
Cat Shelter Dashboard
=====================
Interactive Streamlit dashboard visualising available cats in shelters.
Reads from the SQLite database produced by pipeline.py.
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import streamlit as st

DB_PATH = "data/cats_shelter.db"


@st.cache_data
def load_data() -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql("SELECT * FROM cats", conn,
                         parse_dates=["available_date", "updated_date"])
    return df


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filters")

    age_groups = ["All"] + sorted(df["attributes_agegroup"].dropna().unique().tolist())
    selected_age = st.sidebar.selectbox("Age Group", age_groups)

    genders = ["All"] + sorted(df["attributes_sex"].dropna().unique().tolist())
    selected_gender = st.sidebar.selectbox("Gender", genders)

    energy_levels = ["All"] + sorted(df["attributes_energylevel"].dropna().unique().tolist())
    selected_energy = st.sidebar.selectbox("Energy Level", energy_levels)

    if selected_age != "All":
        df = df[df["attributes_agegroup"] == selected_age]
    if selected_gender != "All":
        df = df[df["attributes_sex"] == selected_gender]
    if selected_energy != "All":
        df = df[df["attributes_energylevel"] == selected_energy]

    return df


def show_metrics(df: pd.DataFrame) -> None:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Cats", f"{len(df):,}")
    col2.metric("Unique Breeds", f"{df['attributes_breedprimary'].nunique():,}")
    col3.metric("States Covered", "N/A")
    col4.metric("With Pictures", f"{(df['attributes_picturecount'] > 0).sum():,}")


def chart_age_distribution(df: pd.DataFrame) -> plt.Figure:
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
    counts = df["attributes_breedprimary"].value_counts().head(top_n).sort_values()
    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.barh(counts.index, counts.values, color="#5B8DB8", edgecolor="white")
    ax.bar_label(bars, padding=4, fontsize=9)
    ax.set_title(f"Top {top_n} Breeds in Shelters", fontweight="bold", pad=12)
    ax.set_xlabel("Number of cats")
    ax.spines[["top", "right"]].set_visible(False)
    return fig


def chart_gender_split(df: pd.DataFrame) -> plt.Figure:
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


def chart_energy_levels(df: pd.DataFrame) -> plt.Figure:
    counts = df["attributes_energylevel"].value_counts()
    fig, ax = plt.subplots(figsize=(6, 4))
    bars = ax.bar(counts.index, counts.values, color="#5B8DB8", edgecolor="white")
    ax.bar_label(bars, padding=4, fontsize=9)
    ax.set_title("Cats by Energy Level", fontweight="bold", pad=12)
    ax.set_ylabel("Number of cats")
    ax.spines[["top", "right"]].set_visible(False)
    return fig


def chart_compatibility(df: pd.DataFrame) -> plt.Figure:
    total = len(df)
    labels = ["OK with kids", "OK with cats", "OK with dogs",
              "Housetrained", "Microchipped"]
    cols = ["attributes_iskidsok", "attributes_iscatsok", "attributes_isdogsok",
            "attributes_ishousetrained", "attributes_iscurrentvaccinations"]
    pcts = [pd.to_numeric(df[c], errors='coerce').fillna(0).sum() / total * 100 for c in cols]
    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.barh(labels, pcts, color="#A8C5A0", edgecolor="white")
    ax.bar_label(bars, fmt="%.1f%%", padding=4, fontsize=9)
    ax.set_xlim(0, 110)
    ax.set_title("Compatibility & Characteristics (% of all cats)",
                 fontweight="bold", pad=12)
    ax.spines[["top", "right"]].set_visible(False)
    return fig


def chart_top_states(df: pd.DataFrame, top_n: int = 10) -> plt.Figure:
    # State data not currently available in this dataset
    # TODO: extract state from relationships_locations_data when pipeline is extended
    return None

# ── MAIN ──────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="Cat Shelter Dashboard", page_icon="🐱", layout="wide")
st.title("🐱 Cat Shelter Dashboard")
st.caption("Data sourced from RescueGroups v5 API")

df_raw = load_data()
df = apply_filters(df_raw)

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
    st.pyplot(chart_energy_levels(df))

col5, col6 = st.columns(2)
with col5:
    st.pyplot(chart_compatibility(df))
with col6:
    fig = chart_top_states(df)
    if fig:
        st.pyplot(fig)
    else:
        st.info("No state data available.")
