-- Top games ranked by RAWG community rating
select
    rawg_id,
    name,
    rating,
    ratings_count,
    released,
    rank() over (order by rating desc) as rating_rank
from {{ source('silver', 'silver_games') }}
where name is not null 
    and name != ''                    -- Filters out empty string placeholders
    and rating is not null
    and rating > 0                    -- Filters out games with no rating data
    and ratings_count > 100           -- Ensures the game has a reliable sample size of reviews
order by rating_rank