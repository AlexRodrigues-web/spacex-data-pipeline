-- stg_books: limpeza e tipagem dos dados brutos
{{ config(materialized='view') }}

with src as (
    select
        title,
        price_gbp::text as price_raw,
        rating_txt,
        load_ts
    from raw.books
)

select
    title,
    /* 1) troca vírgula por ponto
       2) remove tudo que não for dígito ou ponto
       3) NULL se sobrar string vazia
       4) cast para numeric
    */
    nullif(
        regexp_replace(
            replace(price_raw, ',', '.'),
            '[^0-9\.]+',
            '',
            'g'
        ),
        ''
    )::numeric(10,2) as price_gbp,
    case rating_txt
        when 'One'   then 1
        when 'Two'   then 2
        when 'Three' then 3
        when 'Four'  then 4
        when 'Five'  then 5
        else null
    end as rating,
    load_ts
from src
