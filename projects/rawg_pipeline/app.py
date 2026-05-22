import os
import streamlit as st
import duckdb
import plotly.express as px

DB_PATH = os.path.join(os.path.dirname(__file__), "rawg_data.duckdb")

if not os.path.exists(DB_PATH):
    import run_pipeline
    run_pipeline.run()
    
# 1. Page Configuration
st.set_page_config(
    page_title="RAWG Gaming Insights",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 2. Database Connection with Caching
DB_PATH = os.path.join(os.path.dirname(__file__), "rawg_data.duckdb")

@st.cache_data(ttl=3600)
def load_gold_data():
    """Connects to DuckDB and fetches the Gold layer summaries."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    top_games = conn.execute("SELECT * FROM main_gold.gold_top_rated_games ORDER BY rating_rank").df()
    genres = conn.execute("SELECT * FROM main_gold.gold_genre_summary ORDER BY name").df()
    conn.close()
    return top_games, genres

# Load the data
try:
    df_games, df_genres = load_gold_data()
except Exception:
    st.error(f"⚠️ Could not connect to DuckDB at `{DB_PATH}`. Check your path or ensure another process hasn't locked the file.")
    st.stop()

# 3. Sidebar Filters
st.sidebar.title("🎮 RAWG Dashboard Controls")
st.sidebar.markdown("Explore your transformed Gold layer data.")

min_rating = float(df_games['rating'].min())
max_rating = float(df_games['rating'].max())
rating_range = st.sidebar.slider(
    "Filter by Minimum Game Rating",
    min_value=min_rating,
    max_value=max_rating,
    value=4.0,
    step=0.05
)

filtered_games = df_games[df_games['rating'] >= rating_range]

# 4. Main Dashboard Layout
st.title("🕹️ RAWG Pipeline: Gold Layer Insights")
st.markdown("---")

# Row 1: Key Metrics
col1, col2 = st.columns(2)
with col1:
    st.metric(label="Total Tracked High-Rated Games", value=len(df_games))
with col2:
    st.metric(label="Unique Genres Analyzed", value=len(df_genres))

st.markdown("---")

# Row 2: Charts
left_chart_col, right_chart_col = st.columns(2)

with left_chart_col:
    st.subheader("🏆 Top Games by User Engagement")
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
    fig_scatter.update_traces(textposition='top center')
    st.plotly_chart(fig_scatter, use_container_width=True)

with right_chart_col:
    st.subheader("📊 Rating Distribution")
    fig_hist = px.histogram(
        df_games,
        x="rating",
        nbins=20,
        labels={"rating": "User Rating", "count": "Number of Games"},
        color_discrete_sequence=["#b44fff"]
    )
    st.plotly_chart(fig_hist, use_container_width=True)

st.markdown("---")

# Row 3: Data Tables
st.subheader("📋 Gold Layer Raw Explorer")
tab1, tab2 = st.tabs(["⭐ Top Rated Games", "🏷️ Genre Summary"])

with tab1:
    st.dataframe(
        filtered_games[['rating_rank', 'name', 'rating', 'ratings_count', 'released']],
        use_container_width=True,
        hide_index=True
    )

with tab2:
    st.dataframe(df_genres, use_container_width=True, hide_index=True)