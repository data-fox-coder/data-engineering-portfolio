-- Game count per platform, ranked by popularity
select
    p.rawg_id,
    p.name,
    p.slug,
    count(gp.game_rawg_id) as game_count,
    rank() over (order by count(gp.game_rawg_id) desc) as popularity_rank
from {{ source('silver', 'silver_platforms') }} as p
left join {{ source('silver', 'silver_game_platforms') }} as gp
    on p.rawg_id = gp.platform_rawg_id
group by p.rawg_id, p.name, p.slug
order by game_count desc