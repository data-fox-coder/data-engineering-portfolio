import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# 1. Page Configuration
st.set_page_config(
    page_title="RAWG Gaming Insights",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 2. Database Connection with Caching
DB_PATH = "rawg_data.duckdb"

@st.cache_data(ttl=3600)  # Cache data for 1 hour to keep the app snappy
def load_gold_data():
    """Connects to DuckDB and fetches the Gold layer summaries."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    # Load dataframes
    top_games = conn.execute("SELECT * FROM main_gold.gold_top_rated_games ORDER BY rating_rank").df()
    genres = conn.execute("SELECT * FROM main_gold.gold_genre_summary ORDER BY genre_rank").df()
    platforms = conn.execute("SELECT * FROM main_gold.gold_platform_summary ORDER BY platform_rank").df()
    
    conn.close()
    return top_games, genres, platforms

# Load the data
try:
    df_games, df_genres, df_platforms = load_gold_data()
except Exception as e:
    st.error(f"⚠️ Could not connect to DuckDB at `{DB_PATH}`. Check your path or ensure another process hasn't locked the file.")
    st.stop()

# 3. Sidebar Filters
st.sidebar.title("🎮 RAWG Dashboard Controls")
st.sidebar.markdown("Explore your transformed Gold layer data.")

# Rating Filter for Top Games
min_rating = float(df_games['rating'].min())
max_rating = float(df_games['rating'].max())
rating_range = st.sidebar.slider(
    "Filter by Minimum Game Rating",
    min_value=min_rating,
    max_value=max_rating,
    value=4.0,
    step=0.05
)

# Filter Data based on sidebar selection
filtered_games = df_games[df_games['rating'] >= rating_range]

# 4. Main Dashboard Layout
st.title("🕹️ RAWG Pipeline: Gold Layer Insights")
st.markdown("---")

# Row 1: Key Metrics
col1, col2, col3 = st.columns(3)
with col1:
    st.metric(label="Total Tracked High-Rated Games", value=len(df_games))
with col2:
    st.metric(label="Unique Genres Analyzed", value=len(df_genres))
with col3:
    st.metric(label="Unique Platforms Analyzed", value=len(df_platforms))

st.markdown("---")

# Row 2: Charts & Visualizations
left_chart_col, right_chart_col = st.columns(2)

with left_chart_col:
    st.subheader("🏆 Top Games by User Engagement")
    # Plotly Scatter: Rating vs Ratings Count
    fig_scatter = px.scatter(
        filtered_games.head(15), 
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
    st.subheader("📊 Top 10 Genres (By Rank)")
    # Bar Chart for Genres
    fig_genre = px.bar(
        df_genres.head(10),
        x="name",
        y="genre_rank",
        labels={"name": "Genre", "genre_rank": "Internal Rank Index"},
        color="genre_rank",
        color_continuous_scale=px.colors.sequential.Plasma_r
    )
    # FIX: Change 'reverse' to 'reversed'
    fig_genre.update_layout(yaxis_autorange="reversed")
    st.plotly_chart(fig_genre, use_container_width=True)
st.markdown("---")

# Row 3: Deep Dive Data Tables
st.subheader("📋 Gold Layer Raw Explorer")
tab1, tab2, tab3 = st.tabs(["⭐ Top Rated Games", "🏷️ Genre Summary", "💻 Platform Summary"])

with tab1:
    st.dataframe(
        filtered_games[['rating_rank', 'name', 'rating', 'ratings_count', 'released']], 
        use_container_width=True,
        hide_index=True
    )

with tab2:
    st.dataframe(df_genres, use_container_width=True, hide_index=True)

with tab3:
    st.dataframe(df_platforms, use_container_width=True, hide_index=True)