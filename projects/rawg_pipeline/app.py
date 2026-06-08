"""
app.py
------
The primary Streamlit web application dashboard. 
Checks for the local DuckDB database file, lazily triggers 
the bootstrap pipeline if missing, and renders Gold layer analytical views.
"""

import os
import time
import streamlit as st
import duckdb
import plotly.express as px

# 1. PAGE CONFIGURATION (Must be the absolute first Streamlit command executed)
st.set_page_config(
    page_title="RAWG Gaming Insights",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Define paths relative to this file's root location
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "rawg_data.duckdb")

# 2. LAZY-BOOTSTRAP PIPELINE DATA IF COLD STARTING
# Added a file size check to ensure it's not a ghost empty file
if not os.path.exists(DB_PATH) or os.path.getsize(DB_PATH) == 0:
    with st.spinner("📦 First-time initialization: Executing full Medallion pipeline layers (API -> DuckDB -> dbt)..."):
        import run_pipeline
        run_pipeline.run()
        time.sleep(2) # Give the OS a moment to register the closed file handle
    st.success("🎉 Pipeline execution successful!")

# 3. DATABASE CONNECTION INTERACTION (Using cache_resource for the connection asset)
@st.cache_resource
def get_db_connection(path):
    """Creates a persistent, read-only connection pool to the DuckDB file."""
    return duckdb.connect(path, read_only=True)

# Attempt to safely query data using a volatile layout wrapper (No cache_data on the query block)
try:
    conn = get_db_connection(DB_PATH)
    # Fetch data frames cleanly
    df_games = conn.execute("SELECT * FROM main_gold.gold_top_rated_games ORDER BY rating_rank").df()
    df_genres = conn.execute("SELECT * FROM main_gold.gold_genre_summary ORDER BY name").df()
except duckdb.OperationalError as db_err:
    # Fail-safe backup if dbt is writing checkpoints mid-click
    st.warning("🔄 Finalizing database structural sync. Refreshing UI...")
    time.sleep(2)
    st.rerun()
except Exception as e:
    st.error(f"⚠️ Could not read Gold layer views from DuckDB at `{DB_PATH}`.")
    st.sidebar.error(f"Error compilation logs: {e}")
    st.stop()

# 4. SIDEBAR FILTERS
st.sidebar.title("🎮 RAWG Dashboard Controls")
st.sidebar.markdown("Explore your transformed Gold layer data platform.")

min_rating = float(df_games['rating'].min()) if not df_games.empty else 0.0
max_rating = float(df_games['rating'].max()) if not df_games.empty else 5.0

rating_range = st.sidebar.slider(
    "Filter by Minimum Game Rating",
    min_value=min_rating,
    max_value=max_rating,
    value=4.0 if min_rating <= 4.0 else min_rating,
    step=0.05
)

filtered_games = df_games[df_games['rating'] >= rating_range]

# 5. MAIN DASHBOARD VISUALIZATIONS
st.title("🕹️ RAWG Pipeline: Gold Layer Insights")
st.markdown("---")

# Row 1: High-Level Core Metrics
col1, col2 = st.columns(2)
with col1:
    st.metric(label="Total Tracked High-Rated Games", value=len(df_games))
with col2:
    st.metric(label="Unique Genres Analyzed", value=len(df_genres))

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
        color_continuous_scale=px.colors.sequential.Viridis
    )
    
    fig_scatter.update_traces(
        textposition='top center',
        marker=dict(line=dict(width=1, color='DarkSlateGrey')) 
    )
    fig_scatter.update_layout(
        margin=dict(l=40, r=40, t=20, b=40), 
        height=650, 
        xaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)'),
        yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    )
    
    st.plotly_chart(fig_scatter, use_container_width=True)
else:
    st.info("No games match the selected rating threshold.")

st.subheader("📊 Rating Distribution")
if not df_games.empty:
    fig_hist = px.histogram(
        df_games,
        x="rating",
        nbins=20,
        labels={"rating": "User Rating", "count": "Number of Games"},
        color_discrete_sequence=["#b44fff"]
    )
    
    fig_hist.update_layout(
        margin=dict(l=40, r=40, t=20, b=40),
        height=400,
        bargap=0.05 
    )
    
    st.plotly_chart(fig_hist, use_container_width=True)

st.markdown("---")

# Row 3: Tabbed Data Table Explorers
st.subheader("📋 Gold Layer Raw Explorer")
tab1, tab2 = st.tabs(["⭐ Top Rated Games", "🏷️ Genre Summary"])

with tab1:
    st.dataframe(
        filtered_games[['rating_rank', 'name', 'rating', 'ratings_count', 'released']] if not filtered_games.empty else filtered_games,
        use_container_width=True,
        hide_index=True
    )

with tab2:
    st.dataframe(df_genres, use_container_width=True, hide_index=True)