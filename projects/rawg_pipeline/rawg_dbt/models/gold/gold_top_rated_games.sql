-- Top games ranked by RAWG community rating
select
    rawg_id,
    name,
    rating,
    ratings_count,
    released,
    rank() over (order by rating desc) as rating_rank
from {{ source('silver', 'silver_games') }}
where rating is not null
order by rating_rank