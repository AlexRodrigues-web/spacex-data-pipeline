{{ config(materialized='table') }}

select
    year,
    count(*) as launches,
    sum(case when success then 1 else 0 end) as successes,
    sum(case when success then 0 else 1 end) as failures,
    round(100.0 * sum(case when success then 1 else 0 end) / nullif(count(*),0), 2) as success_rate_pct
from {{ ref('stg_spacex_launches') }}
group by 1
order by 1
