"""
app.py
------
The primary Streamlit web application dashboard.
Opens the committed DuckDB database file (read-only) and renders
Gold layer analytical views.
"""

import os

# Importing the centralized paths from config.py
import config
import duckdb
import plotly.express as px
import streamlit as st

# 1. PAGE CONFIGURATION (Must be the absolute first Streamlit command executed)
st.set_page_config(
    page_title="RAWG Gaming Insights",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded",
)


# 2. DATABASE CONNECTION (Using cache_resource for the connection asset)
@st.cache_resource
def get_db_connection(path):
    """Creates a persistent, read-only connection to the DuckDB file."""
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return duckdb.connect(path, read_only=True)
    return None


conn = get_db_connection(config.DB_PATH)

# Safety check if the database file isn't present
if conn is None:
    st.title("🕹️ RAWG Pipeline: Gold Layer Insights")
    st.markdown("---")
    st.error(
        "📦 Data platform database not found. The pipeline must be run to generate rawg_data.duckdb."
    )
    st.stop()

# Fetch data frames cleanly from the Gold schema views compiled by dbt
try:
    df_games = conn.execute(
        "SELECT * FROM main_gold.gold_top_rated_games ORDER BY rating_rank"
    ).df()
    df_genres = conn.execute(
        "SELECT * FROM main_gold.gold_genre_summary ORDER BY name"
    ).df()
    df_platforms = conn.execute(
        "SELECT * FROM main_gold.gold_platform_summary ORDER BY name"
    ).df()
except Exception as e:  # noqa: BLE001
    st.error("⚠️ Could not read Gold layer views from DuckDB.")
    st.sidebar.error(f"Error compilation logs: {e}")

# 3. SIDEBAR FILTERS
st.sidebar.title("🎮 RAWG Dashboard Controls")
st.sidebar.markdown("Explore your transformed Gold layer data platform.")

min_rating = float(df_games["rating"].min()) if not df_games.empty else 0.0
max_rating = float(df_games["rating"].max()) if not df_games.empty else 5.0

rating_range = st.sidebar.slider(
    "Filter by Minimum Game Rating",
    min_value=min_rating,
    max_value=max_rating,
    value=max(4.0, min_rating),
    step=0.05,
)

filtered_games = df_games[df_games["rating"] >= rating_range]

# 4. MAIN DASHBOARD VISUALIZATIONS
st.title("🕹️ RAWG Pipeline: Gold Layer Insights")
st.markdown("---")

# Row 1: High-Level Core Metrics
col1, col2, col3 = st.columns(3)
with col1:
    st.metric(label="Total Tracked High-Rated Games", value=len(df_games))
with col2:
    st.metric(label="Unique Genres Analyzed", value=len(df_genres))
with col3:
    st.metric(label="Platforms Tracked", value=len(df_platforms))

st.markdown("---")

# Row 2: Interactive Plotly Charts
st.subheader("🏆 Top Games by User Engagement")
if not filtered_games.empty:
    fig_scatter = px.scatter(
        filtered_games,
        x="rating",
        y="ratings_count",
        text="name",
        size="ratings_count",
        color="rating",
        labels={"rating": "User Rating", "ratings_count": "Total Review Count"},
        color_continuous_scale=px.colors.sequential.Viridis,
    )

    fig_scatter.update_traces(
        textposition="top center",
        marker={"line": {"width": 1, "color": "DarkSlateGrey"}},
    )
    fig_scatter.update_layout(
        margin={"l": 40, "r": 40, "t": 20, "b": 40},
        height=650,
        xaxis={"showgrid": True, "gridcolor": "rgba(255,255,255,0.1)"},
        yaxis={"showgrid": True, "gridcolor": "rgba(255,255,255,0.1)"},
    )

    st.plotly_chart(fig_scatter, width="stretch")
else:
    st.info("No games match the selected rating threshold.")

st.subheader("📊 Rating Distribution")
if not df_games.empty:
    fig_hist = px.histogram(
        df_games,
        x="rating",
        nbins=20,
        labels={"rating": "User Rating", "count": "Number of Games"},
        color_discrete_sequence=["#b44fff"],
    )

    fig_hist.update_layout(margin={"l": 40, "r": 40, "t": 20, "b": 40}, height=400, bargap=0.05)

    st.plotly_chart(fig_hist, width="stretch")

st.subheader("🕹️ Platforms Tracked")
if not df_platforms.empty:
    fig_platforms = px.bar(
        df_platforms.sort_values("name"),
        x="name",
        y="platform_rank",
        labels={"name": "Platform", "platform_rank": "Rank"},
        color_discrete_sequence=["#4fd1ff"],
    )
    fig_platforms.update_layout(
        margin={"l": 40, "r": 40, "t": 20, "b": 40}, height=400, xaxis_tickangle=-45
    )
    st.plotly_chart(fig_platforms, width="stretch")

st.markdown("---")

# Row 3: Tabbed Data Table Explorers
st.subheader("📋 Gold Layer Raw Explorer")
tab1, tab2, tab3 = st.tabs(
    ["⭐ Top Rated Games", "🏷️ Genre Summary", "🕹️ Platform Summary"]
)

with tab1:
    st.dataframe(
        filtered_games[["rating_rank", "name", "rating", "ratings_count", "released"]]
        if not filtered_games.empty
        else filtered_games,
        width="stretch",
        hide_index=True,
    )

with tab2:
    st.dataframe(df_genres, width="stretch", hide_index=True)

with tab3:
    st.dataframe(df_platforms, width="stretch", hide_index=True)
