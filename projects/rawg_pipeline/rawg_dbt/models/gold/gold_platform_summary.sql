-- Platform reference data with sequential ranking
select
    rawg_id,
    name,
    slug,
    row_number() over (order by name) as platform_rank
from {{ source('silver', 'silver_platforms') }}
where name is not null 
  and name != ''
  and slug is not null
order by platform_rank