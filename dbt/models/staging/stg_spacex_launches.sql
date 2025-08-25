{{ config(materialized='view') }}

with src as (
    select
        launch_id,
        name,
        date_utc::timestamptz as date_utc,
        success::boolean as success,
        rocket,
        details,
        load_ts
    from raw.spacex_launches
)

select
    launch_id,
    name,
    date_utc,
    date_part('year', date_utc)::int as year,
    success,
    rocket,
    details,
    load_ts
from src
