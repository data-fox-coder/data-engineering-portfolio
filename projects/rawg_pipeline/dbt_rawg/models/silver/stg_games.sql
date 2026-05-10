-- models/silver/stg_games.sql
-- Cleans and types raw game JSON from bronze into structured silver rows.
-- Deduplicates by rawg_id, keeping the most recently ingested record.

with raw as (
    select
        rawg_id,
        raw_json,
        ingested_at,
        row_number() over (
            partition by rawg_id
            order by ingested_at desc
        ) as row_num
    from {{ source('bronze', 'bronze_games') }}
),

deduplicated as (
    select
        rawg_id,
        raw_json,
        ingested_at
    from raw
    where row_num = 1
),

typed as (
    select
        rawg_id,
        json_extract_string(raw_json, '$.name')                         as name,
        json_extract_string(raw_json, '$.slug')                         as slug,
        try_cast(
            json_extract_string(raw_json, '$.released') as date
        )                                                               as released,
        try_cast(
            json_extract_string(raw_json, '$.rating') as double
        )                                                               as rating,
        try_cast(
            json_extract_string(raw_json, '$.ratings_count') as integer
        )                                                               as ratings_count,
        try_cast(
            json_extract_string(raw_json, '$.metacritic') as integer
        )                                                               as metacritic,
        try_cast(
            json_extract_string(raw_json, '$.playtime') as integer
        )                                                               as playtime_hours,
        json_extract_string(raw_json, '$.background_image')             as background_image,
        ingested_at
    from deduplicated
)

select * from typed
