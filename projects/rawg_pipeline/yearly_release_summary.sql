-- models/gold/yearly_release_summary.sql
-- Aggregated stats per release year — useful for trend analysis and dashboarding.

with silver as (
    select * from {{ ref('stg_games') }}
),

yearly as (
    select
        year(released)                          as release_year,
        count(*)                                as total_games,
        round(avg(rating), 2)                   as avg_rating,
        round(avg(metacritic), 1)               as avg_metacritic,
        sum(ratings_count)                      as total_ratings,
        round(avg(playtime_hours), 1)           as avg_playtime_hours,
        max(rating)                             as highest_rating,
        min(rating)                             as lowest_rating
    from silver
    where
        released is not null
        and year(released) between 1980 and year(current_date)
    group by year(released)
)

select *
from yearly
order by release_year desc
