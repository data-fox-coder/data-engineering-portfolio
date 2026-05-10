-- models/silver/stg_genres.sql
-- Cleans raw genre JSON from bronze into a typed silver table.

with raw as (
    select
        rawg_id,
        raw_json,
        ingested_at,
        row_number() over (
            partition by rawg_id
            order by ingested_at desc
        ) as row_num
    from {{ source('bronze', 'bronze_genres') }}
),

deduplicated as (
    select rawg_id, raw_json
    from raw
    where row_num = 1
)

select
    rawg_id,
    json_extract_string(raw_json, '$.name')                         as name,
    json_extract_string(raw_json, '$.slug')                         as slug,
    try_cast(
        json_extract_string(raw_json, '$.games_count') as integer
    )                                                               as games_count
from deduplicated
