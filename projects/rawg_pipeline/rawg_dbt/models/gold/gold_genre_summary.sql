-- Genre reference data
select
    rawg_id,
    name,
    slug
from {{ source('silver', 'silver_genres') }}
order by name