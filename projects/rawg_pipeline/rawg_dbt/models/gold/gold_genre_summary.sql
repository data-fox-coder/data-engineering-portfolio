-- Genre reference data with sequential ranking
select
    rawg_id,
    name,
    slug,
    row_number() over (order by name) as genre_rank
from {{ source('silver', 'silver_genres') }}
where name is not null 
  and name != ''
  and slug is not null
order by genre_rank