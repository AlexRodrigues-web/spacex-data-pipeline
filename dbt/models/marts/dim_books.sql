-- dim_books: tabela final para análise (dedup por dia/título)
{{ config(materialized='table') }}

with base as (
    select
        title,
        price_gbp,
        rating,
        load_ts,
        date_trunc('day', load_ts)::date as load_date
    from {{ ref('stg_books') }}
    where rating between 1 and 5
),
dedup as (
    select
        *,
        row_number() over (
            partition by title, load_date
            order by load_ts desc, price_gbp desc nulls last
        ) as rn
    from base
)
select
    title,
    price_gbp,
    rating,
    load_date
from dedup
where rn = 1
