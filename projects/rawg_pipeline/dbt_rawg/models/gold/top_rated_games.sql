-- models/gold/top_rated_games.sql
-- Top 100 games by RAWG rating with rank, release year, and quality filters.
-- Requires a minimum number of ratings to exclude obscure/unrated titles.

with silver as (
    select * from {{ ref('stg_games') }}
),

filtered as (
    select *
    from silver
    where
        rating is not null
        and ratings_count >= 10          -- minimum credibility threshold
),

ranked as (
    select
        row_number() over (
            order by rating desc, ratings_count desc
        )                               as rank,
        rawg_id,
        name,
        slug,
        rating,
        ratings_count,
        metacritic,
        playtime_hours,
        year(released)                  as release_year,
        background_image
    from filtered
)

select *
from ranked
where rank <= 100
