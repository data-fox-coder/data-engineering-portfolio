-- Genre reference data with ranking
select
    rawg_id,
    name,
    slug,
    rank() over (order by name) as genre_rank
from {{ source('silver', 'silver_genres') }}
order by name