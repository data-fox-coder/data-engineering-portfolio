-- Platform reference data with ranking
select
    rawg_id,
    name,
    slug,
    rank() over (order by name) as platform_rank
from {{ source('silver', 'silver_platforms') }}
order by name