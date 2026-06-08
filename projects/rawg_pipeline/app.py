"""
app.py
------
The primary Streamlit web application dashboard. 
Checks for the local DuckDB database file at the repository root, lazily triggers 
the bootstrap pipeline if missing via a session state guard, 
and renders Gold layer analytical views.
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

# Define paths relative to the repository root location
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(REPO_ROOT, "rawg_data.duckdb")

# 2. STATE-GUARDED LAZY BOOTSTRAP
if "pipeline_executed" not in st.session_state:
    st.session_state.pipeline_executed = False

# Only execute if the file is missing/empty AND we haven't already run it in this container session
if (not os.path.exists(DB_PATH) or os.path.getsize(DB_PATH) == 0) and not st.session_state.pipeline_executed:
    with st.spinner("📦 Cold-start initialization: Executing full Medallion pipeline (API -> DuckDB -> dbt)..."):
        try:
            import run_pipeline
            run_pipeline.run()
            st.session_state.pipeline_executed = True
            time.sleep(2)  # Give the file system a brief window to clear handles
            st.success("🎉 Pipeline execution successful! Loading database structures...")
            st.rerun()  
        except Exception as pipeline_err:
            st.error("❌ Critical failure during cold-start pipeline execution.")
            st.exception(pipeline_err)
            st.stop()

# 3. DATABASE CONNECTION INTERACTION (Using cache_resource for the connection asset)
@st.cache_resource
def get_db_connection(path):
    """Creates a persistent, read-only connection pool to the DuckDB file."""
    if os.path.exists(path) and os.path.getsize(path) > 0:
        return duckdb.connect(path, read_only=True)
    return None

conn = get_db_connection(DB_PATH)

# Safety check if the database layout isn't ready
if conn is None:
    st.title("🕹️ RAWG Pipeline: Gold Layer Insights")
    st.markdown("---")
    st.info("📦 Data platform database is initializing or unavailable. Please refresh the page in a moment.")
    st.stop()

# Fetch data frames cleanly from the Gold schema views compiled by dbt
try:
    df_games = conn.execute("SELECT * FROM main_gold.gold_top_rated_games ORDER BY rating_rank").df()
    df_genres = conn.execute("SELECT * FROM main_gold.gold_genre_summary ORDER BY name").df()
except Exception as e:
    st.error(f"⚠️ Could not read Gold layer views from DuckDB.")
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