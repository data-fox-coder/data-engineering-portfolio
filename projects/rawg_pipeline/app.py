"""
app.py
------
The primary Streamlit web application dashboard. 
Checks for the local DuckDB database file, lazily triggers 
the bootstrap pipeline if missing, and renders Gold layer analytical views.
"""

import os
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
if not os.path.exists(DB_PATH):
    with st.spinner("📦 First-time initialization: Executing full Medallion pipeline layers..."):
        import run_pipeline
        run_pipeline.run()

# 3. DATABASE CONNECTION WITH STREAMLIT CACHING
@st.cache_data(ttl=3600)
def load_gold_data():
    """Connects to DuckDB and fetches the compiled Gold layer summaries."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    # Using the main_gold schema names compiled by dbt
    top_games = conn.execute("SELECT * FROM main_gold.gold_top_rated_games ORDER BY rating_rank").df()
    genres = conn.execute("SELECT * FROM main_gold.gold_genre_summary ORDER BY name").df()
    conn.close()
    return top_games, genres

# Attempt to load data assets safely
try:
    df_games, df_genres = load_gold_data()
except Exception as e:
    st.error(f"⚠️ Could not connect to DuckDB at `{DB_PATH}`. Check your schema path or ensure a file lock isn't active.")
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

# Row 2: Interactive Plotly Charts (De-cluttered Full-Width Layout)

# CHART 1: ENGAGEMENT SCATTER
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
    
    # Clean up margins, shift text labels, and customize chart appearance
    fig_scatter.update_traces(
        textposition='top center',
        marker=dict(line=dict(width=1, color='DarkSlateGrey')) # Adds a sharp border to markers
    )
    fig_scatter.update_layout(
        margin=dict(l=40, r=40, t=20, b=40), # Strips out wasted border padding
        height=650, # Boosts height to give text room to spread out
        xaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)'),
        yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
    )
    
    st.plotly_chart(fig_scatter, use_container_width=True)
else:
    st.info("No games match the selected rating threshold.")

# CHART 2: RATING HISTOGRAM
st.subheader("📊 Rating Distribution")
if not df_games.empty:
    fig_hist = px.histogram(
        df_games,
        x="rating",
        nbins=20,
        labels={"rating": "User Rating", "count": "Number of Games"},
        color_discrete_sequence=["#b44fff"]
    )
    
    # Strip padding margins out of the histogram as well
    fig_hist.update_layout(
        margin=dict(l=40, r=40, t=20, b=40),
        height=400,
        bargap=0.05 # Adds a tiny, crisp gap between bars for a clean look
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